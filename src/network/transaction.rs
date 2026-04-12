//! Transaction-related network functions: fetching, decoding, and resolving call ABIs.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::data::types::SnTransaction;
use crate::decode::AbiRegistry;
use crate::decode::events::decode_event;
use crate::decode::functions::parse_multicall;

/// Resolve selector names, function definitions, and contract ABIs for a list of calls.
/// Shared by all code paths that produce a `TransactionLoaded` action.
pub(super) async fn resolve_call_abis(
    calls: &mut Vec<crate::decode::functions::RawCall>,
    abi_reg: &Arc<AbiRegistry>,
) {
    for call in calls.iter_mut() {
        if let Some(name) = abi_reg.get_selector_name(&call.selector) {
            call.function_name = Some(name);
        }
        if let Some(abi) = abi_reg.get_abi_for_address(&call.contract_address).await {
            if let Some(func) = abi.get_function(&call.selector) {
                if call.function_name.is_none() {
                    call.function_name = Some(func.name.clone());
                }
                call.function_def = Some(func.clone());
            }
            call.contract_abi = Some(abi);
        }
    }
}

/// Decode already-fetched tx+receipt and send `TransactionLoaded`. No IO.
pub(super) async fn decode_and_send_transaction(
    transaction: SnTransaction,
    receipt: crate::data::types::SnReceipt,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    let mut decoded_events = Vec::with_capacity(receipt.events.len());
    for event in &receipt.events {
        let abi = abi_reg.get_abi_for_address(&event.from_address).await;
        decoded_events.push(decode_event(event, abi.as_deref()));
    }
    let mut decoded_calls = match &transaction {
        SnTransaction::Invoke(invoke) => parse_multicall(&invoke.calldata),
        _ => Vec::new(),
    };
    resolve_call_abis(&mut decoded_calls, abi_reg).await;
    let _ = action_tx.send(Action::TransactionLoaded {
        transaction,
        receipt,
        decoded_events,
        decoded_calls,
    });
}

/// Fetch tx + receipt in parallel, decode, and send `TransactionLoaded`.
/// Used when neither the transaction nor receipt has been fetched yet.
pub(super) async fn fetch_and_send_transaction(
    hash: starknet::core::types::Felt,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    let start = std::time::Instant::now();
    let hash_short = format!("{:#x}", hash);
    debug!(tx_hash = %hash_short, "Fetching transaction + receipt");

    let ds2 = Arc::clone(ds);
    let (tx_result, receipt_result) = tokio::join!(ds.get_transaction(hash), ds2.get_receipt(hash));

    match (tx_result, receipt_result) {
        (Ok(transaction), Ok(receipt)) => {
            info!(tx_hash = %hash_short, elapsed_ms = start.elapsed().as_millis(), "Transaction fetched, decoding");
            decode_and_send_transaction(transaction, receipt, abi_reg, tx).await;
        }
        (Err(e), _) | (_, Err(e)) => {
            error!(tx_hash = %hash_short, elapsed_ms = start.elapsed().as_millis(), error = %e, "Failed to fetch transaction");
            let _ = tx.send(Action::Error(format!("Fetch tx: {e}")));
        }
    }
}
