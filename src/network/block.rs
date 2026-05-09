//! Block-related network functions: fetching block details, resolving endpoint names,
//! and prefetching Voyager labels for visible addresses.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use starknet::core::types::Felt;

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::decode::AbiRegistry;
use crate::decode::functions::parse_multicall;
use crate::decode::outside_execution::detect_outside_execution;

use super::helpers;
use super::rpc_source_update;
use super::voyager;

/// Fetch block detail: txs, endpoint names, and execution statuses (from receipts).
pub(super) async fn fetch_and_send_block_detail(
    number: u64,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    voyager_c: &Option<Arc<voyager::VoyagerClient>>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    let start = std::time::Instant::now();
    debug!(number, "Fetching block detail");
    match ds.get_block_with_txs(number).await {
        Ok((block, mut transactions)) => {
            let _ = tx.send(rpc_source_update(crate::app::state::SourceStatus::Live));
            let (endpoint_names, meta_tx_info, is_privacy_tx) =
                resolve_endpoint_names(&transactions, abi_reg).await;

            // Batch-fetch receipts for execution status + actual fee. `buffered`
            // keeps up to 20 in flight while letting later receipts start as soon
            // as earlier ones land — the previous `step_by(20) + join_all` form
            // gated each chunk's start on its predecessor's slowest receipt.
            use futures::stream::StreamExt;
            let mut tx_statuses = vec!["?".to_string(); transactions.len()];
            let receipt_results: Vec<_> = futures::stream::iter(0..transactions.len())
                .map(|idx| {
                    let ds_r = Arc::clone(ds);
                    let hash = transactions[idx].hash();
                    async move { (idx, ds_r.get_receipt(hash).await) }
                })
                .buffered(20)
                .collect()
                .await;
            for (idx, result) in receipt_results {
                if let Ok(receipt) = result {
                    tx_statuses[idx] = match receipt.execution_status {
                        crate::data::types::ExecutionStatus::Succeeded => "OK".into(),
                        crate::data::types::ExecutionStatus::Reverted(_) => "REV".into(),
                        _ => "?".into(),
                    };
                    transactions[idx].set_actual_fee(receipt.actual_fee);
                }
            }

            let resolved = endpoint_names.iter().filter(|n| n.is_some()).count();
            info!(
                number,
                tx_count = transactions.len(),
                resolved,
                elapsed_ms = start.elapsed().as_millis(),
                "Block detail fetched"
            );
            // Prefetch Voyager labels for all senders visible in this block
            let senders = transactions.iter().map(|t| t.sender());
            spawn_voyager_prefetch(senders, voyager_c, tx);

            let _ = tx.send(Action::BlockDetailLoaded {
                block,
                transactions,
                endpoint_names,
                tx_statuses,
                meta_tx_info,
                is_privacy_tx,
            });
        }
        Err(e) => {
            error!(number, elapsed_ms = start.elapsed().as_millis(), error = %e, "Failed to fetch block detail");
            let _ = tx.send(rpc_source_update(
                crate::app::state::SourceStatus::FetchError(e.to_string()),
            ));
            let _ = tx.send(Action::Error(format!("Fetch block: {e}")));
        }
    }
}

/// Spawn background Voyager label fetches for a batch of addresses.
/// Deduplicates the list; each fetch checks the local cache first so
/// already-known addresses return immediately without an API call.
pub(super) fn spawn_voyager_prefetch(
    addresses: impl IntoIterator<Item = starknet::core::types::Felt>,
    voyager_c: &Option<Arc<voyager::VoyagerClient>>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    let Some(vc) = voyager_c else { return };
    let unique: std::collections::HashSet<starknet::core::types::Felt> =
        addresses.into_iter().collect();
    for addr in unique {
        let vc = Arc::clone(vc);
        let tx = tx.clone();
        tokio::spawn(async move {
            voyager::fetch_and_send_label(addr, &vc, &tx).await;
        });
    }
}

/// Resolve function endpoint names and detect outside execution (meta tx) for a list of transactions.
/// Returns (endpoint_names, meta_tx_info).
///
/// Endpoint names are formatted as:
///   "transfer, approve" or "transfer, approve, swap, ... +2 more"
/// Uses the persistent selector DB for instant lookups, then batch-fetches
/// class ABIs for unknown selectors.
///
/// Also returns `is_privacy_tx`: per-tx flag set when any top-level call
/// (or any OE-inner call inside a top-level OE wrapper) targets the
/// privacy pool. Catches user-direct pool calls + standard SNIP-9 + AVNU
/// classic-forwarder patterns. Misses `execute_private_sponsored` (see
/// issue #41) — that path doesn't surface the user as intender at all.
async fn resolve_endpoint_names(
    transactions: &[crate::data::types::SnTransaction],
    abi_registry: &AbiRegistry,
) -> (
    Vec<Option<String>>,
    Vec<Option<crate::app::views::block_detail::MetaTxSummary>>,
    Vec<bool>,
) {
    use std::collections::HashSet;

    use crate::decode::functions::RawCall;

    // Step 1: Parse all multicall selectors and collect unknown targets.
    // Also store the parsed RawCalls for outside execution detection.
    let mut tx_calls: Vec<Vec<(Felt, Felt)>> = Vec::with_capacity(transactions.len());
    let mut tx_raw_calls: Vec<Vec<RawCall>> = Vec::with_capacity(transactions.len());
    let mut unknown_targets: HashSet<Felt> = HashSet::new();

    for tx in transactions {
        match tx {
            crate::data::types::SnTransaction::Invoke(invoke) => {
                let calls = parse_multicall(&invoke.calldata);
                for call in &calls {
                    if abi_registry.get_selector_name(&call.selector).is_none() {
                        unknown_targets.insert(call.contract_address);
                    }
                }
                tx_calls.push(
                    calls
                        .iter()
                        .map(|c| (c.contract_address, c.selector))
                        .collect(),
                );
                tx_raw_calls.push(calls);
            }
            crate::data::types::SnTransaction::L1Handler(l1) => {
                tx_calls.push(vec![(l1.contract_address, l1.entry_point_selector)]);
                tx_raw_calls.push(Vec::new());
                if abi_registry
                    .get_selector_name(&l1.entry_point_selector)
                    .is_none()
                {
                    unknown_targets.insert(l1.contract_address);
                }
            }
            _ => {
                tx_calls.push(Vec::new());
                tx_raw_calls.push(Vec::new());
            }
        }
    }

    // Step 2: Batch fetch ABIs for unknown targets
    if !unknown_targets.is_empty() {
        debug!(
            count = unknown_targets.len(),
            "Batch resolving class ABIs for endpoint names"
        );
        let targets_vec: Vec<_> = unknown_targets.into_iter().collect();
        for chunk in targets_vec.chunks(10) {
            let futures: Vec<_> = chunk
                .iter()
                .map(|addr| abi_registry.get_abi_for_address(addr))
                .collect();
            let _results = futures::future::join_all(futures).await;
        }
    }

    use crate::app::views::block_detail::MetaTxSummary;

    let pool_addr = *crate::decode::privacy::POOL_ADDRESS;

    // Step 3: Format endpoint names per tx + detect outside execution + flag privacy
    let mut endpoint_names = Vec::with_capacity(tx_calls.len());
    let mut meta_tx_info: Vec<Option<MetaTxSummary>> = vec![None; tx_calls.len()];
    let mut is_privacy_tx: Vec<bool> = vec![false; tx_calls.len()];

    for (i, (calls, raw_calls)) in tx_calls.iter().zip(tx_raw_calls.iter()).enumerate() {
        // Endpoint names (shared helper: "foo, bar, baz, … +N").
        if calls.is_empty() {
            endpoint_names.push(None);
        } else {
            let names = helpers::format_selector_names(
                calls.iter().map(|(_addr, selector)| *selector),
                abi_registry,
            );
            endpoint_names.push(Some(names));
        }

        // Top-level pool call detection (the user-direct case).
        if calls.iter().any(|(addr, _)| *addr == pool_addr) {
            is_privacy_tx[i] = true;
        }

        // Outside execution detection (lightweight — no inner call ABI resolution).
        // Shared with address-view classify; see `detect_outside_execution`. Same
        // pass also catches OE-inner calls that target the pool, so a single
        // sponsored privacy tx (standard SNIP-9 / AVNU classic forwarder) gets
        // its `is_privacy_tx` flag set without re-walking the calls.
        for call in raw_calls {
            let resolved_name = call
                .function_name
                .clone()
                .or_else(|| abi_registry.get_selector_name(&call.selector));
            if let Some((oe, _method)) = detect_outside_execution(call, resolved_name.as_deref()) {
                if meta_tx_info[i].is_none() {
                    meta_tx_info[i] = Some(MetaTxSummary {
                        intender: oe.intender,
                        version: oe.version,
                    });
                }
                if oe
                    .inner_calls
                    .iter()
                    .any(|ic| ic.contract_address == pool_addr)
                {
                    is_privacy_tx[i] = true;
                }
            }
        }
    }

    (endpoint_names, meta_tx_info, is_privacy_tx)
}
