//! E2E tests for WebSocket address event/transaction streaming.
//!
//! These tests exercise the full `spawn_ws_subscriber` → `WsSubscriptionManager` pipeline,
//! verifying that:
//!   - Events for subscribed addresses arrive as `Action::AddressTxsStreamed`
//!   - `AddressTxSummary` values have valid hashes and block numbers
//!   - Unsubscribing stops delivery of new summaries
//!   - `merge_tx_summaries` correctly deduplicates repeated events for the same tx
//!
//! Run with:
//!   APP_WS_URL=ws://... APP_RPC_URL=http://... cargo test --test ws_address_stream_test -- --include-ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use starknet::core::types::Felt;
use tokio::sync::mpsc;
use tokio::time::timeout;

use snbeat::app::actions::Action;
use snbeat::app::views::address_info::AddressInfoState;
use snbeat::data::rpc::RpcDataSource;
use snbeat::network::ws::spawn_ws_subscriber;

// ---------------------------------------------------------------------------
// Known high-volume addresses
// ---------------------------------------------------------------------------
const ETH_TOKEN: &str = "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";
const STRK_TOKEN: &str = "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d";
const AVNU: &str = "0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f";

fn ws_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("APP_WS_URL").expect("APP_WS_URL must be set")
}

fn rpc_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("APP_RPC_URL").expect("APP_RPC_URL must be set")
}

fn felt(hex: &str) -> Felt {
    Felt::from_hex(hex).expect("valid hex felt")
}

/// Drain `Action::AddressTxsStreamed` messages for `address` from the channel,
/// waiting up to `timeout_secs` seconds for the first one.
/// Returns all summaries collected across all matching actions received
/// in a burst (non-blocking drain after the first one).
async fn collect_streamed_summaries(
    rx: &mut mpsc::UnboundedReceiver<Action>,
    address: Felt,
    min_count: usize,
    timeout_secs: u64,
) -> Vec<snbeat::data::types::AddressTxSummary> {
    let mut summaries = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(Action::AddressTxsStreamed {
                address: addr,
                tx_summaries,
                ..
            })) if addr == address => {
                summaries.extend(tx_summaries);
                if summaries.len() >= min_count {
                    break;
                }
            }
            Ok(Some(_)) => {
                // Other actions (e.g. NewBlock, SourceUpdate) — ignore
            }
            Ok(None) | Err(_) => break,
        }
    }

    summaries
}

// ---------------------------------------------------------------------------
// Test 1: ETH token events arrive as valid AddressTxSummaries
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL and APP_RPC_URL"]
async fn test_eth_token_events_to_tx_summaries() {
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();
    let data_source = Arc::new(RpcDataSource::new(&rpc_url()));
    let address = felt(ETH_TOKEN);

    let (_handle, manager) = spawn_ws_subscriber(ws_url(), data_source, response_tx);

    // Give WS time to connect
    tokio::time::sleep(Duration::from_secs(2)).await;

    manager.subscribe_address(address);

    // ETH token is very high volume — expect multiple events within 60s
    let summaries = collect_streamed_summaries(&mut response_rx, address, 3, 60).await;

    assert!(
        !summaries.is_empty(),
        "Expected at least one AddressTxSummary from ETH token events"
    );

    for s in &summaries {
        assert_ne!(s.hash, Felt::ZERO, "tx hash must be non-zero");
        println!("ETH token tx: hash={:#x} block={}", s.hash, s.block_number);
    }

    println!(
        "Received {} tx summaries from ETH token subscription",
        summaries.len()
    );
}

// ---------------------------------------------------------------------------
// Test 2: STRK token events — verify event structure
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL and APP_RPC_URL"]
async fn test_strk_token_events_stream() {
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();
    let data_source = Arc::new(RpcDataSource::new(&rpc_url()));
    let address = felt(STRK_TOKEN);

    let (_handle, manager) = spawn_ws_subscriber(ws_url(), data_source, response_tx);

    tokio::time::sleep(Duration::from_secs(2)).await;
    manager.subscribe_address(address);

    let summaries = collect_streamed_summaries(&mut response_rx, address, 1, 60).await;

    assert!(
        !summaries.is_empty(),
        "Expected STRK token events within 60s"
    );

    let s = &summaries[0];
    assert_ne!(s.hash, Felt::ZERO);
    assert_eq!(s.tx_type, "INVOKE", "Events from token should be INVOKE");
    assert_eq!(s.status, "OK", "Events are only emitted for successful txs");
    println!("STRK token event: hash={:#x}", s.hash);
}

// ---------------------------------------------------------------------------
// Test 3: AVNU events — verify we receive valid summaries
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL and APP_RPC_URL"]
async fn test_avnu_events_to_tx_summaries() {
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();
    let data_source = Arc::new(RpcDataSource::new(&rpc_url()));
    let address = felt(AVNU);

    let (_handle, manager) = spawn_ws_subscriber(ws_url(), data_source, response_tx);

    tokio::time::sleep(Duration::from_secs(2)).await;
    manager.subscribe_address(address);

    // AVNU is high volume but not as fast as token contracts — give 300s
    let summaries = collect_streamed_summaries(&mut response_rx, address, 1, 300).await;

    assert!(
        !summaries.is_empty(),
        "Expected at least one AVNU event within 300s"
    );
    assert_ne!(summaries[0].hash, Felt::ZERO);
    println!(
        "AVNU: received {} summaries, first hash={:#x}",
        summaries.len(),
        summaries[0].hash
    );
}

// ---------------------------------------------------------------------------
// Test 4: Unsubscribe stops delivery of new summaries
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL and APP_RPC_URL"]
async fn test_subscribe_unsubscribe_lifecycle() {
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();
    let data_source = Arc::new(RpcDataSource::new(&rpc_url()));
    let address = felt(ETH_TOKEN);

    let (_handle, manager) = spawn_ws_subscriber(ws_url(), data_source, response_tx);

    tokio::time::sleep(Duration::from_secs(2)).await;
    manager.subscribe_address(address);

    // Wait for at least one event to confirm subscription is live
    let initial = collect_streamed_summaries(&mut response_rx, address, 1, 60).await;
    assert!(
        !initial.is_empty(),
        "Should receive events before unsubscribe"
    );
    println!("Got {} events before unsubscribe", initial.len());

    // Unsubscribe
    manager.unsubscribe_address(address);

    // Drain the channel of any already-buffered events (brief window)
    tokio::time::sleep(Duration::from_secs(2)).await;
    while response_rx.try_recv().is_ok() {}

    // Now check that no new events arrive within 10s
    let after = timeout(Duration::from_secs(10), async {
        loop {
            match response_rx.recv().await {
                Some(Action::AddressTxsStreamed { address: addr, .. }) if addr == address => {
                    return true; // Got one — bad!
                }
                Some(_) => continue,
                None => return false,
            }
        }
    })
    .await;

    // timeout means no event arrived — which is what we want
    assert!(
        after.is_err(),
        "Should not receive events for address after unsubscribe"
    );
    println!("Confirmed: no events after unsubscribe");
}

// ---------------------------------------------------------------------------
// Test 5: Event deduplication via merge_tx_summaries
// ---------------------------------------------------------------------------
//
// A single tx may emit multiple events (e.g. Transfer + Approval).
// The WS subscription sends one notification per event, so the same tx hash
// may appear multiple times. `merge_tx_summaries` must deduplicate by hash.

#[tokio::test]
#[ignore = "requires APP_WS_URL and APP_RPC_URL"]
async fn test_event_deduplication_via_merge() {
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();
    let data_source = Arc::new(RpcDataSource::new(&rpc_url()));
    let address = felt(ETH_TOKEN);

    let (_handle, manager) = spawn_ws_subscriber(ws_url(), data_source, response_tx);

    tokio::time::sleep(Duration::from_secs(2)).await;
    manager.subscribe_address(address);

    // Collect many summaries — likely to contain duplicates for multi-event txs
    // ETH token emits multiple events per tx (Transfer, Approval, etc.)
    let raw = collect_streamed_summaries(&mut response_rx, address, 5, 120).await;
    assert!(
        raw.len() >= 2,
        "Need at least 2 summaries for dedup test (got {}, increase timeout?)",
        raw.len()
    );

    println!("Raw summaries before merge: {}", raw.len());

    // Feed all through merge_tx_summaries — simulates what the address view does
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(raw.clone());

    // After merge: should have ≤ raw count (duplicates collapsed)
    let merged_count = state.txs.items.len();
    println!(
        "After merge: {} unique txs (from {} raw summaries)",
        merged_count,
        raw.len()
    );

    assert!(
        merged_count <= raw.len(),
        "merge should not increase count (dedup is required)"
    );
    assert!(merged_count > 0, "at least one tx after merge");

    // Verify all hashes are unique in the merged list
    let hashes: std::collections::HashSet<Felt> = state.txs.items.iter().map(|t| t.hash).collect();
    assert_eq!(
        hashes.len(),
        state.txs.items.len(),
        "merged list must have unique hashes"
    );

    // Idempotency: feeding the same data twice should not grow the list
    state.merge_tx_summaries(raw.clone());
    assert_eq!(
        state.txs.items.len(),
        merged_count,
        "re-merging same data must be idempotent"
    );

    println!("Deduplication confirmed: {} unique hashes", hashes.len());
}

// ---------------------------------------------------------------------------
// Test 6: Multiple addresses subscribed on one connection
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL and APP_RPC_URL"]
async fn test_multiple_addresses_one_connection() {
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();
    let data_source = Arc::new(RpcDataSource::new(&rpc_url()));
    let eth = felt(ETH_TOKEN);
    let strk = felt(STRK_TOKEN);

    let (_handle, manager) = spawn_ws_subscriber(ws_url(), data_source, response_tx);

    tokio::time::sleep(Duration::from_secs(2)).await;
    manager.subscribe_address(eth);
    manager.subscribe_address(strk);

    let mut got_eth = false;
    let mut got_strk = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, response_rx.recv()).await {
            Ok(Some(Action::AddressTxsStreamed { address, .. })) => {
                if address == eth {
                    got_eth = true;
                    println!("Got ETH token event");
                } else if address == strk {
                    got_strk = true;
                    println!("Got STRK token event");
                }
                if got_eth && got_strk {
                    break;
                }
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }

    assert!(got_eth, "Should receive ETH token events");
    assert!(got_strk, "Should receive STRK token events");
    println!("Both addresses received events on one WS connection");
}
