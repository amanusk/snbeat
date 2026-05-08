//! Transaction-related network functions: fetching, decoding, and resolving call ABIs.

use std::collections::HashMap;
use std::sync::Arc;

use starknet::core::types::Felt;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::data::pathfinder::PathfinderClient;
use crate::data::types::SnTransaction;
use crate::decode::AbiRegistry;
use crate::decode::abi::ParsedAbi;
use crate::decode::events::decode_event;
use crate::decode::functions::parse_multicall;
use crate::decode::outside_execution::{
    OutsideExecutionInfo, OutsideExecutionVersion, is_avnu_forwarder, is_outside_execution,
    looks_like_outside_execution, looks_like_private_sponsored, parse_forwarder_call,
    parse_outside_execution, parse_private_sponsored,
};

/// Resolve the ABI for `addr` as of `block`. Prefers the prewarmed
/// `addr_to_class` map; on miss, falls back to a synchronous resolve via
/// `class_history` (cached or fetched). If that also fails, falls through
/// to the latest-ABI path (same behaviour as pre-issue-#24 code) so any
/// degradation is graceful.
async fn abi_at_block(
    addr: &Felt,
    block: u64,
    addr_to_class: &HashMap<Felt, Felt>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
    abi_reg: &AbiRegistry,
) -> Option<Arc<ParsedAbi>> {
    if let Some(class_hash) = addr_to_class.get(addr) {
        return abi_reg.get_abi_for_class(class_hash).await;
    }
    if block > 0
        && let Some(class_hash) = super::helpers::resolve_class_hash_at(*addr, block, ds, pf).await
    {
        return abi_reg.get_abi_for_class(&class_hash).await;
    }
    abi_reg.get_abi_for_address(addr).await
}

/// Resolve selector names, function definitions, and contract ABIs for a list of calls.
/// Shared by all code paths that produce a `TransactionLoaded` action.
pub(super) async fn resolve_call_abis(
    calls: &mut [crate::decode::functions::RawCall],
    block: u64,
    addr_to_class: &HashMap<Felt, Felt>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
) {
    // Resolve ABIs concurrently — each `abi_at_block` is mostly cache-hit
    // after the prewarm pass, but the long-tail miss (an address that wasn't
    // in `prewarm_targets`) used to serialize the whole multicall behind a
    // single RPC. `buffered(8)` caps fan-out so a multicall with hundreds of
    // unique cold targets can't burst into hundreds of concurrent RPCs.
    use futures::stream::StreamExt;
    let abi_futs: Vec<_> = calls
        .iter()
        .map(|call| {
            abi_at_block(
                &call.contract_address,
                block,
                addr_to_class,
                ds,
                pf,
                abi_reg,
            )
        })
        .collect();
    let abis: Vec<Option<Arc<ParsedAbi>>> =
        futures::stream::iter(abi_futs).buffered(8).collect().await;

    for (call, abi_opt) in calls.iter_mut().zip(abis.into_iter()) {
        if let Some(name) = abi_reg.get_selector_name(&call.selector) {
            call.function_name = Some(name);
        }
        if let Some(abi) = abi_opt {
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
    pf: Option<&Arc<PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    let block = receipt.block_number;

    // Parse multicall up front so we can prewarm the ABI cache for every
    // unique address referenced by this tx (event sources + call targets)
    // in a single parallel round-trip — resolving each address's class
    // hash *as of `block`* so post-upgrade contracts decode pre-upgrade
    // events correctly (issue #24). Subsequent per-event / per-call ABI
    // lookups consult the resolved (address → class_hash) map first.
    let mut decoded_calls = match &transaction {
        SnTransaction::Invoke(invoke) => parse_multicall(&invoke.calldata),
        _ => Vec::new(),
    };
    let mut prewarm_targets: std::collections::HashSet<Felt> =
        std::collections::HashSet::with_capacity(receipt.events.len() + decoded_calls.len());
    for event in &receipt.events {
        prewarm_targets.insert(event.from_address);
    }
    for call in &decoded_calls {
        prewarm_targets.insert(call.contract_address);
    }
    let addr_to_class = if block > 0 {
        super::helpers::prewarm_abis_at(prewarm_targets, block, ds, pf, abi_reg).await
    } else {
        // Pending tx (no block yet). Use latest-ABI prewarm.
        super::helpers::prewarm_abis(prewarm_targets, abi_reg).await;
        HashMap::new()
    };

    // Decode events concurrently with bounded fan-out. After prewarm, most
    // ABI lookups are cache hits and complete synchronously; cache-miss tails
    // (synthetic events whose `from_address` wasn't in `prewarm_targets`)
    // overlap rather than serialize. `buffered(8)` caps in-flight fetches so
    // huge receipts can't burst hundreds of concurrent RPCs.
    use futures::stream::StreamExt;
    let decoded_events: Vec<_> = {
        let addr_to_class = &addr_to_class;
        let event_futs: Vec<_> = receipt
            .events
            .iter()
            .map(|event| async move {
                let abi =
                    abi_at_block(&event.from_address, block, addr_to_class, ds, pf, abi_reg).await;
                decode_event(event, abi.as_deref())
            })
            .collect();
        futures::stream::iter(event_futs)
            .buffered(8)
            .collect()
            .await
    };
    resolve_call_abis(&mut decoded_calls, block, &addr_to_class, ds, pf, abi_reg).await;
    let outside_executions = detect_and_resolve_outside_executions(
        &decoded_calls,
        block,
        &addr_to_class,
        ds,
        pf,
        abi_reg,
    )
    .await;

    // Fetch block timestamp (used for age display and price lookups on tracked tokens).
    // Block fetches are cached, so repeat calls for the same block are cheap.
    let block_timestamp = ds.get_block(block).await.ok().map(|b| b.timestamp);

    let tx_hash = transaction.hash();
    let _ = action_tx.send(Action::TransactionLoaded {
        transaction,
        receipt,
        decoded_events,
        decoded_calls,
        outside_executions,
        block_timestamp,
    });

    // Fire-and-forget trace fetch. Sent as a separate Action so the rest of
    // the tx view paints immediately while the recursive trace decodes.
    spawn_trace_fetch(tx_hash, block, ds, abi_reg, action_tx);
}

/// Detect outside execution calls, parse their inner calls, and resolve inner call ABIs.
///
/// Detection uses three methods:
/// 1. By function name (when ABI selector resolution works)
/// 2. By calldata heuristic (ANY_CALLER + valid struct, for component-based selectors)
/// 3. By known forwarder address (AVNU paymaster wraps outside execution in execute/execute_sponsored)
async fn detect_and_resolve_outside_executions(
    calls: &[crate::decode::functions::RawCall],
    block: u64,
    addr_to_class: &HashMap<Felt, Felt>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
) -> Vec<(usize, OutsideExecutionInfo)> {
    // Pass 1 (sync): detect OEs.
    let mut detected: Vec<(usize, OutsideExecutionInfo)> = Vec::new();
    for (i, call) in calls.iter().enumerate() {
        let fname = call.function_name.as_deref().unwrap_or("");

        let mut oe = None;

        // Method 1: detect by resolved function name. `is_outside_execution`
        // covers both classic SNIP-9 (v1/v2/v3) and the privacy-aware
        // `execute_private_sponsored` AVNU entrypoint.
        if let Some(version) = is_outside_execution(fname) {
            oe = if matches!(version, OutsideExecutionVersion::PrivateSponsored) {
                parse_private_sponsored(call)
            } else {
                parse_outside_execution(call, version)
            };
        }
        // Method 1b: detect `execute_private_sponsored` by selector when the
        // ABI hasn't resolved a name yet (e.g. cold cache on first run).
        if oe.is_none() && looks_like_private_sponsored(call) {
            oe = parse_private_sponsored(call);
        }
        // Method 2: detect by calldata pattern (ANY_CALLER + valid struct)
        if oe.is_none() && fname.is_empty() && looks_like_outside_execution(call) {
            oe = parse_outside_execution(call, OutsideExecutionVersion::V2);
        }
        // Method 3: detect by known AVNU forwarder address
        if oe.is_none() && is_avnu_forwarder(&call.contract_address) {
            oe = parse_forwarder_call(call);
        }

        if let Some(oe) = oe {
            detected.push((i, oe));
        }
    }

    // Pass 2 (concurrent): resolve every OE's inner_calls in parallel. Each
    // future holds a disjoint `&mut Vec<RawCall>` so the borrows don't alias.
    // Concurrency is transitively bounded — `resolve_call_abis` itself caps
    // in-flight ABI lookups at 8 per OE, so the global ceiling is
    // `OE_count × 8` (typically ≤ 16 in practice).
    let resolve_futs = detected.iter_mut().map(|(_, oe)| {
        resolve_call_abis(&mut oe.inner_calls, block, addr_to_class, ds, pf, abi_reg)
    });
    futures::future::join_all(resolve_futs).await;

    detected
}

/// Fetch tx + receipt in parallel, decode, and send `TransactionLoaded`.
/// Used when neither the transaction nor receipt has been fetched yet.
pub(super) async fn fetch_and_send_transaction(
    hash: starknet::core::types::Felt,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
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
            decode_and_send_transaction(transaction, receipt, ds, pf, abi_reg, tx).await;
        }
        (Err(e), _) | (_, Err(e)) => {
            error!(tx_hash = %hash_short, elapsed_ms = start.elapsed().as_millis(), error = %e, "Failed to fetch transaction");
            let _ = tx.send(Action::Error(format!("Fetch tx: {e}")));
        }
    }
}

/// Fetch + decode the trace for `hash` and send `TransactionTraceLoaded`.
pub(super) async fn fetch_and_send_trace(
    hash: starknet::core::types::Felt,
    block: u64,
    ds: Arc<dyn DataSource>,
    abi_reg: Arc<AbiRegistry>,
    tx: mpsc::UnboundedSender<Action>,
) {
    let start = std::time::Instant::now();
    let hash_short = format!("{:#x}", hash);
    debug!(tx_hash = %hash_short, "Fetching transaction trace");

    match ds.get_trace(hash).await {
        Ok(trace) => {
            let decoded = crate::decode::trace::decode_trace(&trace, hash, block, &abi_reg).await;
            info!(
                tx_hash = %hash_short,
                elapsed_ms = start.elapsed().as_millis(),
                "Trace fetched + decoded"
            );
            let _ = tx.send(Action::TransactionTraceLoaded {
                tx_hash: hash,
                trace: Some(decoded),
            });
        }
        Err(e) => {
            error!(tx_hash = %hash_short, error = %e, "Failed to fetch trace");
            // Non-fatal: the rest of the tx view is already populated. Send
            // a TransactionTraceLoaded with `trace: None` so the UI can clear
            // the "loading…" state and render an "unavailable" message,
            // and surface the error itself separately.
            let _ = tx.send(Action::TransactionTraceLoaded {
                tx_hash: hash,
                trace: None,
            });
            let _ = tx.send(Action::Error(format!("Fetch trace: {e}")));
        }
    }
}

/// Helper that spawns `fetch_and_send_trace` as a detached tokio task.
pub(super) fn spawn_trace_fetch(
    hash: starknet::core::types::Felt,
    block: u64,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    let ds = Arc::clone(ds);
    let abi_reg = Arc::clone(abi_reg);
    let tx = tx.clone();
    tokio::spawn(async move {
        fetch_and_send_trace(hash, block, ds, abi_reg, tx).await;
    });
}
