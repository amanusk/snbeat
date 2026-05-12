use starknet::core::types::Felt;

use snbeat::data::types::{ContractCallSummary, deduplicate_contract_calls};

fn make_call(tx_hash: u64, sender: u64, function_name: &str, block: u64) -> ContractCallSummary {
    ContractCallSummary {
        tx_hash: Felt::from(tx_hash),
        sender: Felt::from(sender),
        function_name: function_name.to_string(),
        block_number: block,
        timestamp: block * 10,
        total_fee_fri: 0,
        status: "OK".into(),
        nonce: None,
        tip: 0,
        inner_targets: Vec::new(),
    }
}

#[test]
fn test_no_duplicates_unchanged() {
    let calls = vec![
        make_call(0xaaa, 0x1, "transfer", 100),
        make_call(0xbbb, 0x2, "approve", 101),
        make_call(0xccc, 0x3, "swap", 102),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].function_name, "transfer");
    assert_eq!(result[1].function_name, "approve");
    assert_eq!(result[2].function_name, "swap");
}

#[test]
fn test_duplicate_tx_hash_merged() {
    let calls = vec![
        make_call(0xaaa, 0x1, "transfer", 100),
        make_call(0xaaa, 0x1, "approve", 100),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 1, "duplicate tx_hash should merge into one");
    assert_eq!(result[0].function_name, "transfer, approve");
}

#[test]
fn test_duplicate_same_function_not_repeated() {
    let calls = vec![
        make_call(0xaaa, 0x1, "transfer", 100),
        make_call(0xaaa, 0x1, "transfer", 100),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].function_name, "transfer",
        "same function name should not be duplicated"
    );
}

#[test]
fn test_three_calls_same_tx() {
    let calls = vec![
        make_call(0xaaa, 0x1, "transfer", 100),
        make_call(0xaaa, 0x2, "approve", 100),
        make_call(0xaaa, 0x3, "swap", 100),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].function_name, "transfer, approve, swap");
    // First occurrence's sender is kept
    assert_eq!(result[0].sender, Felt::from(0x1u64));
}

#[test]
fn test_fee_filled_from_later_entry() {
    let mut call1 = make_call(0xaaa, 0x1, "transfer", 100);
    call1.total_fee_fri = 0;
    let mut call2 = make_call(0xaaa, 0x1, "approve", 100);
    call2.total_fee_fri = 5000;

    let result = deduplicate_contract_calls(vec![call1, call2]);
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].total_fee_fri, 5000,
        "fee should be filled from second entry"
    );
}

#[test]
fn test_timestamp_filled_from_later_entry() {
    let mut call1 = make_call(0xaaa, 0x1, "transfer", 100);
    call1.timestamp = 0;
    let mut call2 = make_call(0xaaa, 0x1, "approve", 100);
    call2.timestamp = 99999;

    let result = deduplicate_contract_calls(vec![call1, call2]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].timestamp, 99999);
}

#[test]
fn test_mixed_duplicate_and_unique() {
    let calls = vec![
        make_call(0xaaa, 0x1, "transfer", 100),
        make_call(0xbbb, 0x2, "swap", 101),
        make_call(0xaaa, 0x1, "approve", 100),
        make_call(0xccc, 0x3, "mint", 102),
        make_call(0xbbb, 0x2, "burn", 101),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].function_name, "transfer, approve");
    assert_eq!(result[1].function_name, "swap, burn");
    assert_eq!(result[2].function_name, "mint");
}

#[test]
fn test_empty_function_names_handled() {
    let calls = vec![
        make_call(0xaaa, 0x1, "", 100),
        make_call(0xaaa, 0x1, "transfer", 100),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].function_name, "transfer");
}

#[test]
fn test_empty_input() {
    let result = deduplicate_contract_calls(vec![]);
    assert!(result.is_empty());
}

#[test]
fn test_single_entry() {
    let calls = vec![make_call(0xaaa, 0x1, "transfer", 100)];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].function_name, "transfer");
}

#[test]
fn test_preserves_insertion_order() {
    let calls = vec![
        make_call(0xccc, 0x3, "c_fn", 300),
        make_call(0xaaa, 0x1, "a_fn", 100),
        make_call(0xbbb, 0x2, "b_fn", 200),
    ];
    let result = deduplicate_contract_calls(calls);
    assert_eq!(result.len(), 3);
    // Order should be preserved (not sorted by hash or block)
    assert_eq!(result[0].tx_hash, Felt::from(0xcccu64));
    assert_eq!(result[1].tx_hash, Felt::from(0xaaau64));
    assert_eq!(result[2].tx_hash, Felt::from(0xbbbu64));
}

#[test]
fn test_fee_not_overwritten_when_already_set() {
    let mut call1 = make_call(0xaaa, 0x1, "transfer", 100);
    call1.total_fee_fri = 3000;
    let mut call2 = make_call(0xaaa, 0x1, "approve", 100);
    call2.total_fee_fri = 5000;

    let result = deduplicate_contract_calls(vec![call1, call2]);
    assert_eq!(
        result[0].total_fee_fri, 3000,
        "existing non-zero fee should not be overwritten"
    );
}
