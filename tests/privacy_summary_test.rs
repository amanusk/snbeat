//! End-to-end probe of the Privacy Pool summarizer against real mainnet txs.
//!
//! Two fixtures, both offline (receipt JSON condensed into helper calls):
//!
//!   * `0x90304ef5...` (block 9216979) — direct (non-sponsored) privacy tx
//!     with deposit/withdrawal/InvokeExternal mix.
//!
//!   * `0x4b9cde1b...` (block 9515174) — AVNU-sponsored privacy tx where
//!     the pool call is wrapped in an outside-execution intent and the
//!     pool's withdrawal recipient is the AVNU forwarder itself (gasless
//!     fee-in-private-STRK pattern). Verifies the OE-aware detection plus
//!     the fix that doesn't double-count a withdrawal payout as a pool fee.

use snbeat::data::types::{ExecutionStatus, InvokeTx, SnEvent, SnTransaction};
use snbeat::decode::events::DecodedEvent;
use snbeat::decode::functions::RawCall;
use snbeat::decode::outside_execution::{OutsideExecutionInfo, OutsideExecutionVersion};
use snbeat::decode::privacy::{self, POOL_ADDRESS, PaymasterMode};
use starknet::core::types::Felt;
use starknet::core::utils::get_selector_from_name;

fn felt(hex: &str) -> Felt {
    Felt::from_hex(hex).unwrap()
}

fn pool_event(selector: Felt, keys_after_selector: Vec<Felt>, data: Vec<Felt>) -> DecodedEvent {
    let mut keys = vec![selector];
    keys.extend(keys_after_selector);
    DecodedEvent {
        contract_address: *POOL_ADDRESS,
        event_name: None,
        decoded_keys: Vec::new(),
        decoded_data: Vec::new(),
        raw: SnEvent {
            from_address: *POOL_ADDRESS,
            keys,
            data,
            transaction_hash: Felt::ZERO,
            block_number: 9_216_979,
            event_index: 0,
        },
    }
}

fn pool_call() -> RawCall {
    RawCall {
        contract_address: *POOL_ADDRESS,
        selector: get_selector_from_name("apply_actions").unwrap(),
        data: Vec::new(),
        function_name: Some("apply_actions".to_string()),
        function_def: None,
        contract_abi: None,
    }
}

fn invoke_tx(sender: Felt) -> SnTransaction {
    SnTransaction::Invoke(InvokeTx {
        hash: Felt::ZERO,
        sender_address: sender,
        calldata: Vec::new(),
        nonce: None,
        version: Felt::from(3u64),
        actual_fee: None,
        execution_status: ExecutionStatus::Succeeded,
        block_number: 9_216_979,
        index: 0,
        tip: 0,
        resource_bounds: None,
    })
}

#[test]
fn matches_mainnet_example_tx() {
    let note_used = felt("0x0247fc60d782e0094e7f98c47f277d92a3345d07a436f1f56b27a9b62be2322e");
    let enc_note_created =
        felt("0x023c20207be8b1ef4430c25eef8ce779c9745ebe04139555ae81bd4f8fdd6ec5");
    let open_note_created =
        felt("0x022330482fd296a27cf9096807b4a3622cd619d31cce42c1e55655914e8459ee");
    let withdrawal = felt("0x002eed7e29b3502a726faf503ac4316b7101f3da813654e8df02c13449e03da8");
    let open_note_deposited =
        felt("0x025b6da03c4858d11cb0708d5cb6be79b190fb32eb7a7ce83804e07cbbb9bead");

    // Tokens (STRK, USDC) and addresses observed in the example tx.
    let strk = felt("0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d");
    let usdc = felt("0x033068f6539f8e6e6b131e6b2b814e6c34a5224bc66947c47dab9dfee93b35fb");
    let to_addr = felt("0x389e0930f2bcf14221adefb71007ca71cec4cd4f64f321836392d2748a14c6");
    let depositor = felt("0x389e0930f2bcf14221adefb71007ca71cec4cd4f64f321836392d2748a14c6");
    let one_strk: u128 = 0xde0b6b3a7640000; // 1.0 STRK in fri.
    let nullifier_a = felt("0x30cc93d82386c9f64ffdf15899f42169a07d38c043c2dfa4727e2a65034de6d");
    let nullifier_b = felt("0x6a0c5a0e0ab2a02934da15f7d761cd484cdb8c704ef55bf8345ecb8160338cc");
    let note_id_open = felt("0x7dd4dc2cee342d94cbf47505a4bd368bf07c41826ac248ce05830201d34c82");
    let note_id_enc_a = felt("0x1eed60b8d483b3bede62d1cc0f32874aea30747e6943437c858359b41801bf7");

    let events = vec![
        pool_event(note_used, vec![nullifier_a], vec![]),
        pool_event(note_used, vec![nullifier_b], vec![]),
        pool_event(
            open_note_created,
            vec![usdc, note_id_open],
            vec![Felt::ZERO; 3],
        ),
        pool_event(
            enc_note_created,
            vec![note_id_enc_a],
            vec![felt(
                "0x53f6f5e87c1aa2c7329d4e060260075db650f2a68d11b6e4316215b9333fe86",
            )],
        ),
        pool_event(
            withdrawal,
            vec![to_addr, strk],
            // 3 felts of EncUserAddr blob, then amount as u128 low.
            vec![Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::from(one_strk)],
        ),
        pool_event(
            open_note_deposited,
            vec![depositor, strk, note_id_open],
            // amount = 1 STRK (u128 low + high).
            vec![Felt::from(one_strk), Felt::ZERO],
        ),
    ];

    let sender = felt("0x51258c78dba24ee15c3aed60fb6fbbfb91a7cb2f739c82e39871847823aa410");
    let tx = invoke_tx(sender);
    let summary =
        privacy::summarize(&tx, &[pool_call()], &events, &[]).expect("tx is a privacy-pool tx");

    // Action-mix counts mirror the real tx's pool-side events.
    assert_eq!(summary.actions.notes_used, 2);
    assert_eq!(summary.actions.enc_notes_created, 1);
    assert_eq!(summary.actions.open_notes_created, 1);
    assert_eq!(summary.actions.withdrawals, 1);
    assert_eq!(summary.actions.open_notes_deposited, 1);
    assert_eq!(summary.actions.viewing_keys_set, 0);
    assert_eq!(summary.actions.deposits, 0);

    // Public-side fields.
    assert_eq!(summary.nullifiers, vec![nullifier_a, nullifier_b]);
    assert_eq!(summary.enc_notes_created, vec![note_id_enc_a]);

    let w = &summary.withdrawals[0];
    assert_eq!(w.to_addr, to_addr);
    assert_eq!(w.token, strk);
    assert_eq!(w.amount, one_strk);

    let n = &summary.open_notes_created[0];
    assert_eq!(n.token, usdc);
    assert_eq!(n.note_id, note_id_open);

    let d = &summary.open_notes_deposited[0];
    assert_eq!(d.depositor, depositor);
    assert_eq!(d.token, strk);
    assert_eq!(d.note_id, note_id_open);
    assert_eq!(d.amount, one_strk);

    // Sender is the user's own account, not a known paymaster.
    assert_eq!(summary.paymaster, PaymasterMode::None);
    assert!(summary.intender.is_none());
}

#[test]
fn non_privacy_tx_returns_none() {
    let other_call = RawCall {
        contract_address: felt(
            "0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f",
        ),
        selector: Felt::ZERO,
        data: Vec::new(),
        function_name: None,
        function_def: None,
        contract_abi: None,
    };
    let tx = invoke_tx(Felt::from(1u64));
    assert!(privacy::summarize(&tx, &[other_call], &[], &[]).is_none());
}

/// Mirrors mainnet tx
/// `0x4b9cde1b731130096209a2af5f947e3a57c290dcfa99c358bc958521ff3c35f` —
/// an AVNU-sponsored privacy tx. Top-level multicall is `[STRK.transfer,
/// AVNU_Forwarder.execute_from_outside_v2(intent)]`; the pool call lives
/// only inside the OE intent's inner_calls. The pool's `Withdrawal` event
/// pays 4 STRK to the AVNU forwarder (the gasless service fee in private
/// STRK). The summarizer must:
///   1. Recognize this as a privacy tx (via the pool-emitted Withdrawal).
///   2. Surface `paymaster = OutsideExecution` and the OE intender.
///   3. NOT report the 4 STRK pool→forwarder transfer as a pool fee
///      (that would be double-counting the withdrawal payout).
#[test]
fn matches_mainnet_sponsored_tx() {
    // AVNU-known relayer account (sender of the tx).
    let relayer = felt("0x22d287c1251406e5e75e2777599ff0aed256fc838c354cb2fbf1f95775ecbdb");
    // The actual user (intender) signed the OE intent.
    let user = felt("0x383d01739484177556480d629b2dc75661dc3afa7d1a19c30d64be2999cb501");
    let avnu_forwarder = felt("0x0127021a1b5a52d3174c2ab077c2b043c80369250d29428cee956d76ee51584f");
    let strk = felt("0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d");
    let four_strk: u128 = 0x3782dace9d900000;
    let withdrawal_selector =
        felt("0x002eed7e29b3502a726faf503ac4316b7101f3da813654e8df02c13449e03da8");
    let enc_note_selector =
        felt("0x023c20207be8b1ef4430c25eef8ce779c9745ebe04139555ae81bd4f8fdd6ec5");
    let erc20_transfer = felt("0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9");

    let tx = invoke_tx(relayer);

    // Top-level multicall: STRK.transfer (the relayer's pre-fee top-up) +
    // AVNU forwarder execute_from_outside_v2.
    let top_calls = vec![
        RawCall {
            contract_address: strk,
            selector: Felt::ZERO,
            data: Vec::new(),
            function_name: Some("transfer".to_string()),
            function_def: None,
            contract_abi: None,
        },
        RawCall {
            contract_address: avnu_forwarder,
            selector: Felt::ZERO,
            data: Vec::new(),
            function_name: Some("execute_from_outside_v2".to_string()),
            function_def: None,
            contract_abi: None,
        },
    ];

    let oe = OutsideExecutionInfo {
        intender: user,
        caller: avnu_forwarder,
        nonce: Felt::ZERO,
        execute_after: 0,
        execute_before: u64::MAX,
        inner_calls: vec![
            // approve(forwarder, fee) — pre-pool plumbing.
            RawCall {
                contract_address: strk,
                selector: Felt::ZERO,
                data: Vec::new(),
                function_name: Some("approve".to_string()),
                function_def: None,
                contract_abi: None,
            },
            // The actual pool call.
            RawCall {
                contract_address: *POOL_ADDRESS,
                selector: get_selector_from_name("apply_actions").unwrap(),
                data: Vec::new(),
                function_name: Some("apply_actions".to_string()),
                function_def: None,
                contract_abi: None,
            },
        ],
        signature: Vec::new(),
        version: OutsideExecutionVersion::V2,
    };

    let events = vec![
        // Pool-emitted EncNoteCreated (a private receive-side note for the user).
        DecodedEvent {
            contract_address: *POOL_ADDRESS,
            event_name: None,
            decoded_keys: Vec::new(),
            decoded_data: Vec::new(),
            raw: SnEvent {
                from_address: *POOL_ADDRESS,
                keys: vec![enc_note_selector, Felt::from(0xAAu64)],
                data: vec![Felt::from(0xBBu64)],
                transaction_hash: Felt::ZERO,
                block_number: 9_515_174,
                event_index: 0,
            },
        },
        // Pool-emitted Withdrawal: to_addr = AVNU forwarder, token = STRK,
        // amount = 4 STRK (the AVNU service fee paid in private funds).
        DecodedEvent {
            contract_address: *POOL_ADDRESS,
            event_name: None,
            decoded_keys: Vec::new(),
            decoded_data: Vec::new(),
            raw: SnEvent {
                from_address: *POOL_ADDRESS,
                keys: vec![withdrawal_selector, avnu_forwarder, strk],
                data: vec![Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::from(four_strk)],
                transaction_hash: Felt::ZERO,
                block_number: 9_515_174,
                event_index: 0,
            },
        },
        // Matching ERC-20 Transfer pool→forwarder for the withdrawal payout.
        DecodedEvent {
            contract_address: strk,
            event_name: None,
            decoded_keys: Vec::new(),
            decoded_data: Vec::new(),
            raw: SnEvent {
                from_address: strk,
                keys: vec![erc20_transfer, *POOL_ADDRESS, avnu_forwarder],
                data: vec![Felt::from(four_strk), Felt::ZERO],
                transaction_hash: Felt::ZERO,
                block_number: 9_515_174,
                event_index: 0,
            },
        },
    ];

    let summary = privacy::summarize(&tx, &top_calls, &events, &[oe])
        .expect("OE-wrapped sponsored privacy tx must still be recognized");

    // The pool only shows up in events (no top-level call, no top-level OE
    // call landed at the contract address at the multicall layer).
    assert_eq!(summary.actions.withdrawals, 1);
    assert_eq!(summary.actions.enc_notes_created, 1);

    // The 4 STRK pool→forwarder transfer matches the Withdrawal event, so
    // it's classified as a withdrawal payout, not a pool fee.
    assert!(
        summary.pool_fee_fri.is_none(),
        "withdrawal payout to AVNU forwarder must not surface as a pool fee"
    );

    // OE-based sponsorship + intender surfaced.
    assert_eq!(summary.paymaster, PaymasterMode::OutsideExecution);
    assert_eq!(summary.intender, Some(user));

    // The withdrawal-recipient is correctly the forwarder, in the clear.
    assert_eq!(summary.withdrawals[0].to_addr, avnu_forwarder);
    assert_eq!(summary.withdrawals[0].amount, four_strk);
}
