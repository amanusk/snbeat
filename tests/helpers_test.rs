use starknet::core::types::Felt;

use snbeat::data::types::{
    DeployAccountTx, ExecutionStatus, InvokeTx, SnExecutionResources, SnReceipt, SnTransaction,
};
use snbeat::network::helpers;

fn make_receipt(fee: u64, status: ExecutionStatus) -> SnReceipt {
    SnReceipt {
        transaction_hash: Felt::ZERO,
        actual_fee: Felt::from(fee),
        block_number: 100,
        block_hash: None,
        fee_unit: "FRI".into(),
        execution_status: status,
        execution_resources: SnExecutionResources::default(),
        events: Vec::new(),
        revert_reason: None,
        finality: "ACCEPTED_ON_L2".into(),
    }
}

fn make_invoke_tx(nonce: u64, tip: u64) -> SnTransaction {
    SnTransaction::Invoke(InvokeTx {
        hash: Felt::from(0xaaau64),
        sender_address: Felt::from(0x1u64),
        calldata: Vec::new(),
        nonce: Some(Felt::from(nonce)),
        version: Felt::from(3u64),
        actual_fee: None,
        execution_status: ExecutionStatus::Succeeded,
        block_number: 100,
        index: 0,
        tip,
        resource_bounds: None,
    })
}

#[test]
fn receipt_status_succeeded() {
    let receipt = make_receipt(100, ExecutionStatus::Succeeded);
    assert_eq!(helpers::receipt_status(Some(&receipt)), "OK");
}

#[test]
fn receipt_status_reverted() {
    let receipt = make_receipt(100, ExecutionStatus::Reverted("reason".into()));
    assert_eq!(helpers::receipt_status(Some(&receipt)), "REV");
}

#[test]
fn receipt_status_unknown() {
    let receipt = make_receipt(100, ExecutionStatus::Unknown);
    assert_eq!(helpers::receipt_status(Some(&receipt)), "?");
}

#[test]
fn receipt_status_none() {
    assert_eq!(helpers::receipt_status(None), "?");
}

#[test]
fn extract_nonce_tip_invoke() {
    let tx = make_invoke_tx(42, 100);
    let (nonce, tip) = helpers::extract_nonce_tip(&tx);
    assert_eq!(nonce, 42);
    assert_eq!(tip, 100);
}

#[test]
fn extract_nonce_tip_invoke_no_nonce() {
    let tx = SnTransaction::Invoke(InvokeTx {
        hash: Felt::ZERO,
        sender_address: Felt::ZERO,
        calldata: Vec::new(),
        nonce: None,
        version: Felt::from(1u64),
        actual_fee: None,
        execution_status: ExecutionStatus::Succeeded,
        block_number: 0,
        index: 0,
        tip: 0,
        resource_bounds: None,
    });
    let (nonce, tip) = helpers::extract_nonce_tip(&tx);
    assert_eq!(nonce, 0);
    assert_eq!(tip, 0);
}

#[test]
fn extract_nonce_tip_deploy_account() {
    let tx = SnTransaction::DeployAccount(DeployAccountTx {
        hash: Felt::ZERO,
        contract_address: Felt::ZERO,
        class_hash: Felt::ZERO,
        constructor_calldata: Vec::new(),
        version: Felt::from(3u64),
        actual_fee: None,
        execution_status: ExecutionStatus::Succeeded,
        block_number: 100,
        index: 0,
        tip: 55,
        resource_bounds: None,
    });
    let (nonce, tip) = helpers::extract_nonce_tip(&tx);
    assert_eq!(nonce, 0);
    assert_eq!(tip, 55);
}
