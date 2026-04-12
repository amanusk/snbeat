use starknet::core::types::Felt;

use snbeat::app::views::AddressInfoState;
use snbeat::data::types::AddressTxSummary;

fn make_summary(
    hash: u64,
    nonce: u64,
    status: &str,
    fee: u128,
    endpoint: &str,
) -> AddressTxSummary {
    AddressTxSummary {
        hash: Felt::from(hash),
        nonce,
        block_number: 100 + nonce,
        timestamp: 0,
        endpoint_names: endpoint.to_string(),
        total_fee_fri: fee,
        tip: 0,
        tx_type: "INVOKE".to_string(),
        status: status.to_string(),
        sender: Some(Felt::from(0x1u64)),
    }
}

#[test]
fn merge_empty_into_empty_is_noop() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(Vec::new());
    assert!(state.txs.items.is_empty());
}

#[test]
fn merge_into_empty_list() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(vec![
        make_summary(0xaaa, 5, "OK", 100, "transfer"),
        make_summary(0xbbb, 3, "OK", 200, "approve"),
    ]);
    assert_eq!(state.txs.items.len(), 2);
    // Sorted by nonce descending
    assert_eq!(state.txs.items[0].nonce, 5);
    assert_eq!(state.txs.items[1].nonce, 3);
}

#[test]
fn merge_upgrades_existing_entries() {
    let mut state = AddressInfoState::default();
    // Initial load with poor data
    state.merge_tx_summaries(vec![make_summary(0xaaa, 5, "?", 0, "")]);
    assert_eq!(state.txs.items[0].status, "?");
    assert_eq!(state.txs.items[0].total_fee_fri, 0);
    assert!(state.txs.items[0].endpoint_names.is_empty());

    // Merge with better data
    state.merge_tx_summaries(vec![make_summary(0xaaa, 5, "OK", 100, "transfer")]);

    // Should upgrade, not duplicate
    assert_eq!(state.txs.items.len(), 1);
    assert_eq!(state.txs.items[0].status, "OK");
    assert_eq!(state.txs.items[0].total_fee_fri, 100);
    assert_eq!(state.txs.items[0].endpoint_names, "transfer");
}

#[test]
fn merge_does_not_downgrade_existing_entries() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(vec![make_summary(0xaaa, 5, "OK", 100, "transfer")]);

    // Merge with worse data — should not overwrite
    state.merge_tx_summaries(vec![make_summary(0xaaa, 5, "?", 0, "")]);

    assert_eq!(state.txs.items.len(), 1);
    assert_eq!(state.txs.items[0].status, "OK");
    assert_eq!(state.txs.items[0].total_fee_fri, 100);
    assert_eq!(state.txs.items[0].endpoint_names, "transfer");
}

#[test]
fn merge_appends_new_and_upgrades_existing() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(vec![make_summary(0xaaa, 5, "?", 0, "")]);

    state.merge_tx_summaries(vec![
        make_summary(0xaaa, 5, "OK", 100, "transfer"), // upgrade
        make_summary(0xbbb, 3, "OK", 200, "approve"),  // new
    ]);

    assert_eq!(state.txs.items.len(), 2);
    assert_eq!(state.txs.items[0].status, "OK"); // upgraded
    assert_eq!(state.txs.items[1].nonce, 3); // appended
}

#[test]
fn merge_sorts_by_nonce_descending() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(vec![
        make_summary(0xaaa, 1, "OK", 100, "a"),
        make_summary(0xbbb, 5, "OK", 200, "b"),
        make_summary(0xccc, 3, "OK", 300, "c"),
    ]);

    let nonces: Vec<u64> = state.txs.items.iter().map(|t| t.nonce).collect();
    assert_eq!(nonces, vec![5, 3, 1]);
}

#[test]
fn merge_updates_oldest_event_block() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(vec![
        make_summary(0xaaa, 5, "OK", 100, "a"),
        make_summary(0xbbb, 3, "OK", 200, "b"),
    ]);

    // Oldest block should be min(block_number) = 103
    assert_eq!(state.oldest_event_block, Some(103));
}

#[test]
fn merge_preserves_selection() {
    let mut state = AddressInfoState::default();
    state.merge_tx_summaries(vec![
        make_summary(0xaaa, 5, "OK", 100, "a"),
        make_summary(0xbbb, 3, "OK", 200, "b"),
    ]);
    // Select second item
    state.txs.state.select(Some(1));

    // Merge new items
    state.merge_tx_summaries(vec![make_summary(0xccc, 4, "OK", 300, "c")]);

    // Selection should be preserved at index 1
    assert_eq!(state.txs.state.selected(), Some(1));
}

#[test]
fn filter_deployment_txs_separates_deploy() {
    let mut state = AddressInfoState::default();
    let addr = Felt::from(0x1u64);

    let txs = vec![
        AddressTxSummary {
            hash: Felt::from(0xdu64),
            nonce: 0,
            block_number: 100,
            timestamp: 0,
            endpoint_names: String::new(),
            total_fee_fri: 0,
            tip: 0,
            tx_type: "DEPLOY_ACCOUNT".to_string(),
            status: "OK".to_string(),
            sender: Some(addr),
        },
        make_summary(0xaaa, 1, "OK", 100, "transfer"),
    ];

    let regular = state.filter_deployment_txs(addr, txs);
    assert_eq!(regular.len(), 1);
    assert_eq!(regular[0].nonce, 1);
    assert!(state.deployment.is_some());
    assert_eq!(state.deployment.as_ref().unwrap().tx_type, "DEPLOY_ACCOUNT");
}
