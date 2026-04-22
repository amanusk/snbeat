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
use crate::decode::outside_execution::{
    OutsideExecutionInfo, OutsideExecutionVersion, is_avnu_forwarder, is_outside_execution,
    looks_like_outside_execution, parse_forwarder_call, parse_outside_execution,
};

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

/// Decode already-fetched tx+receipt and send `TransactionLoaded`.
pub(super) async fn decode_and_send_transaction(
    transaction: SnTransaction,
    receipt: crate::data::types::SnReceipt,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    // Parse multicall up front so we can prewarm the ABI cache for every
    // unique address referenced by this tx (event sources + call targets)
    // in a single parallel round-trip. Every subsequent per-event /
    // per-call `get_abi_for_address` call then hits warm cache.
    let mut decoded_calls = match &transaction {
        SnTransaction::Invoke(invoke) => parse_multicall(&invoke.calldata),
        _ => Vec::new(),
    };
    let mut prewarm_targets: std::collections::HashSet<starknet::core::types::Felt> =
        std::collections::HashSet::with_capacity(receipt.events.len() + decoded_calls.len());
    for event in &receipt.events {
        prewarm_targets.insert(event.from_address);
    }
    for call in &decoded_calls {
        prewarm_targets.insert(call.contract_address);
    }
    super::helpers::prewarm_abis(prewarm_targets, abi_reg).await;

    let mut decoded_events = Vec::with_capacity(receipt.events.len());
    for event in &receipt.events {
        let abi = abi_reg.get_abi_for_address(&event.from_address).await;
        decoded_events.push(decode_event(event, abi.as_deref()));
    }
    resolve_call_abis(&mut decoded_calls, abi_reg).await;
    let outside_executions = detect_and_resolve_outside_executions(&decoded_calls, abi_reg).await;

    // Fetch block timestamp (used for age display and price lookups on tracked tokens).
    // Block fetches are cached, so repeat calls for the same block are cheap.
    let block_timestamp = ds
        .get_block(receipt.block_number)
        .await
        .ok()
        .map(|b| b.timestamp);

    let _ = action_tx.send(Action::TransactionLoaded {
        transaction,
        receipt,
        decoded_events,
        decoded_calls,
        outside_executions,
        block_timestamp,
    });
}

/// Detect outside execution calls, parse their inner calls, and resolve inner call ABIs.
///
/// Detection uses three methods:
/// 1. By function name (when ABI selector resolution works)
/// 2. By calldata heuristic (ANY_CALLER + valid struct, for component-based selectors)
/// 3. By known forwarder address (AVNU paymaster wraps outside execution in execute/execute_sponsored)
async fn detect_and_resolve_outside_executions(
    calls: &[crate::decode::functions::RawCall],
    abi_reg: &Arc<AbiRegistry>,
) -> Vec<(usize, OutsideExecutionInfo)> {
    let mut results = Vec::new();
    for (i, call) in calls.iter().enumerate() {
        let fname = call.function_name.as_deref().unwrap_or("");

        let mut oe = None;

        // Method 1: detect by resolved function name
        if let Some(version) = is_outside_execution(fname) {
            oe = parse_outside_execution(call, version);
        }
        // Method 2: detect by calldata pattern (ANY_CALLER + valid struct)
        if oe.is_none() && fname.is_empty() && looks_like_outside_execution(call) {
            oe = parse_outside_execution(call, OutsideExecutionVersion::V2);
        }
        // Method 3: detect by known AVNU forwarder address
        if oe.is_none() && is_avnu_forwarder(&call.contract_address) {
            oe = parse_forwarder_call(call);
        }

        if let Some(mut oe) = oe {
            resolve_call_abis(&mut oe.inner_calls, abi_reg).await;
            results.push((i, oe));
        }
    }
    results
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
            decode_and_send_transaction(transaction, receipt, ds, abi_reg, tx).await;
        }
        (Err(e), _) | (_, Err(e)) => {
            error!(tx_hash = %hash_short, elapsed_ms = start.elapsed().as_millis(), error = %e, "Failed to fetch transaction");
            let _ = tx.send(Action::Error(format!("Fetch tx: {e}")));
        }
    }
}
