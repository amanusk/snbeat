//! Reducer-level tests for the address-view refresh behaviour:
//!
//!  - The header nonce is derived from streamed tx summaries, so WS-arriving
//!    txs (or any future streaming source) keep it in sync instead of drifting
//!    behind the initial RPC-loaded value.
//!  - `Action::PeriodicAddressPollTick` only dispatches an RPC refresh when
//!    the user is on the address view *and* WS is not `Live` — it must be
//!    a no-op in any other state.

use starknet::core::types::Felt;
use tokio::sync::mpsc;

use snbeat::app::App;
use snbeat::app::actions::{Action, Source};
use snbeat::app::state::{SourceStatus, View};
use snbeat::data::types::{AddressTxSummary, SnAddressInfo};

fn make_app() -> (App, mpsc::UnboundedReceiver<Action>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (App::new(tx), rx)
}

fn tx_summary(hash: u64, nonce: u64, sender: Felt) -> AddressTxSummary {
    AddressTxSummary {
        hash: Felt::from(hash),
        nonce,
        block_number: 1_000 + nonce,
        timestamp: 0,
        endpoint_names: String::new(),
        total_fee_fri: 0,
        tip: 0,
        tx_type: "INVOKE".to_string(),
        status: "OK".to_string(),
        sender: Some(sender),
        called_contracts: Vec::new(),
    }
}

fn seed_account_view(app: &mut App, address: Felt, starting_nonce: u64) {
    app.push_view(View::AddressInfo);
    app.address.context = Some(address);
    app.address.is_contract = false;
    app.address.info = Some(SnAddressInfo {
        address,
        nonce: Felt::from(starting_nonce),
        class_hash: None,
        recent_events: Vec::new(),
        token_balances: Vec::new(),
    });
}

#[test]
fn streamed_ws_tx_bumps_header_nonce() {
    let (mut app, _rx) = make_app();
    let addr = Felt::from(0xa11ceu64);
    seed_account_view(&mut app, addr, 42);

    // A WS notification arrives with a new tx at nonce 42.
    app.handle_action(Action::AddressTxsStreamed {
        address: addr,
        source: Source::Ws,
        tx_summaries: vec![tx_summary(0x1111, 42, addr)],
        complete: false,
    });

    let info = app
        .address
        .info
        .as_ref()
        .expect("info must still be set after streamed tx");
    // After a tx with nonce N lands, the account's next nonce is N+1.
    assert_eq!(info.nonce, Felt::from(43u64));
}

#[test]
fn streamed_tx_with_lower_nonce_does_not_downgrade_header() {
    let (mut app, _rx) = make_app();
    let addr = Felt::from(0xa11ceu64);
    seed_account_view(&mut app, addr, 100);

    // A "catch-up" tx streams in for an older nonce — the header must not
    // regress to a lower value.
    app.handle_action(Action::AddressTxsStreamed {
        address: addr,
        source: Source::Rpc,
        tx_summaries: vec![tx_summary(0x2222, 5, addr)],
        complete: false,
    });

    assert_eq!(app.address.info.as_ref().unwrap().nonce, Felt::from(100u64));
}

#[test]
fn ws_tx_before_address_info_loaded_is_not_clobbered() {
    // Race: a WS tx can arrive between `NavigateToAddress` (which fires the
    // subscribe) and `AddressInfoLoaded` (which lands the initial RPC-read
    // nonce). If the WS tx bumps the header to N+1, the later-arriving RPC
    // nonce must not overwrite it with a smaller value.
    let (mut app, _rx) = make_app();
    let addr = Felt::from(0xa11ceu64);
    // Simulate the pre-AddressInfoLoaded state: view pushed + context set by
    // `NavigateToAddress`, but `info` still None.
    app.push_view(View::AddressInfo);
    app.address.context = Some(addr);
    app.address.is_contract = false;
    assert!(app.address.info.is_none());

    // WS arrives first with nonce 7 → header should be seeded to 8.
    app.handle_action(Action::AddressTxsStreamed {
        address: addr,
        source: Source::Ws,
        tx_summaries: vec![tx_summary(0x7777, 7, addr)],
        complete: false,
    });
    assert_eq!(app.address.info.as_ref().unwrap().nonce, Felt::from(8u64));

    // Now `AddressInfoLoaded` lands with the stale RPC read (nonce 5). The
    // clamp must keep the header at 8, not regress to 5.
    app.handle_action(Action::AddressInfoLoaded {
        info: SnAddressInfo {
            address: addr,
            nonce: Felt::from(5u64),
            class_hash: None,
            recent_events: Vec::new(),
            token_balances: Vec::new(),
        },
        decoded_events: Vec::new(),
        tx_summaries: Vec::new(),
        contract_calls: Vec::new(),
    });
    assert_eq!(app.address.info.as_ref().unwrap().nonce, Felt::from(8u64));
}

#[test]
fn streamed_tx_on_contract_does_not_touch_nonce() {
    let (mut app, _rx) = make_app();
    let addr = Felt::from(0xc0deu64);
    seed_account_view(&mut app, addr, 0);
    app.address.is_contract = true;

    app.handle_action(Action::AddressTxsStreamed {
        address: addr,
        source: Source::Ws,
        tx_summaries: vec![tx_summary(0x3333, 99, addr)],
        complete: false,
    });

    // Contracts: nonce stays at 0 regardless of incoming tx nonces.
    assert_eq!(app.address.info.as_ref().unwrap().nonce, Felt::ZERO);
}

#[test]
fn periodic_tick_dispatches_refresh_when_ws_is_not_live() {
    let (mut app, mut rx) = make_app();
    let addr = Felt::from(0xbeefu64);
    seed_account_view(&mut app, addr, 5);
    app.data_sources.ws = SourceStatus::ConnectError("unreachable".into());

    app.handle_action(Action::PeriodicAddressPollTick);

    let dispatched = rx
        .try_recv()
        .expect("expected a RefreshAddressRpc dispatch");
    match dispatched {
        Action::RefreshAddressRpc { address } => assert_eq!(address, addr),
        other => panic!("unexpected dispatched action: {other:?}"),
    }
}

#[test]
fn periodic_tick_is_noop_when_ws_is_live() {
    let (mut app, mut rx) = make_app();
    let addr = Felt::from(0xbeefu64);
    seed_account_view(&mut app, addr, 5);
    app.data_sources.ws = SourceStatus::Live;

    app.handle_action(Action::PeriodicAddressPollTick);

    assert!(
        rx.try_recv().is_err(),
        "no action should be dispatched while WS is Live"
    );
}

#[test]
fn periodic_tick_is_noop_when_not_on_address_view() {
    let (mut app, mut rx) = make_app();
    // Default view is Blocks; never push AddressInfo.
    app.data_sources.ws = SourceStatus::Off;

    app.handle_action(Action::PeriodicAddressPollTick);

    assert!(
        rx.try_recv().is_err(),
        "no action should be dispatched when not on the address view"
    );
}

#[test]
fn periodic_tick_is_noop_for_contract_addresses() {
    let (mut app, mut rx) = make_app();
    let addr = Felt::from(0xc0deu64);
    seed_account_view(&mut app, addr, 0);
    app.address.is_contract = true;
    app.data_sources.ws = SourceStatus::Off;

    app.handle_action(Action::PeriodicAddressPollTick);

    assert!(
        rx.try_recv().is_err(),
        "contracts don't need the periodic nonce refresh"
    );
}
