//! Block-related network functions: fetching block details, resolving endpoint names,
//! and prefetching Voyager labels for visible addresses.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::decode::AbiRegistry;
use crate::decode::functions::parse_multicall;

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
        Ok((block, transactions)) => {
            let _ = tx.send(rpc_source_update(crate::app::state::SourceStatus::Live));
            let endpoint_names = resolve_endpoint_names(&transactions, abi_reg).await;

            // Batch-fetch receipts for execution status (chunks of 20)
            let mut tx_statuses = vec!["?".to_string(); transactions.len()];
            for chunk_start in (0..transactions.len()).step_by(20) {
                let chunk_end = (chunk_start + 20).min(transactions.len());
                let futs: Vec<_> = (chunk_start..chunk_end)
                    .map(|idx| {
                        let ds_r = Arc::clone(ds);
                        let hash = transactions[idx].hash();
                        async move { (idx, ds_r.get_receipt(hash).await) }
                    })
                    .collect();
                let results = futures::future::join_all(futs).await;
                for (idx, result) in results {
                    if let Ok(receipt) = result {
                        tx_statuses[idx] = match receipt.execution_status {
                            crate::data::types::ExecutionStatus::Succeeded => "OK".into(),
                            crate::data::types::ExecutionStatus::Reverted(_) => "REV".into(),
                            _ => "?".into(),
                        };
                    }
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

/// Resolve function endpoint names for a list of transactions.
/// Parses multicall calldata to extract ALL calls per tx, formats as:
///   "transfer, approve" or "transfer, approve, swap, ... +2 more"
/// Uses the persistent selector DB for instant lookups, then batch-fetches
/// class ABIs for unknown selectors.
async fn resolve_endpoint_names(
    transactions: &[crate::data::types::SnTransaction],
    abi_registry: &AbiRegistry,
) -> Vec<Option<String>> {
    use std::collections::HashSet;

    // Step 1: Parse all multicall selectors and collect unknown targets
    let mut tx_calls: Vec<Vec<(starknet::core::types::Felt, starknet::core::types::Felt)>> =
        Vec::with_capacity(transactions.len()); // per tx: vec of (target_addr, selector)
    let mut unknown_targets: HashSet<starknet::core::types::Felt> = HashSet::new();

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
            }
            crate::data::types::SnTransaction::L1Handler(l1) => {
                tx_calls.push(vec![(l1.contract_address, l1.entry_point_selector)]);
                if abi_registry
                    .get_selector_name(&l1.entry_point_selector)
                    .is_none()
                {
                    unknown_targets.insert(l1.contract_address);
                }
            }
            _ => {
                tx_calls.push(Vec::new());
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

    // Step 3: Format endpoint names per tx
    tx_calls
        .iter()
        .map(|calls| {
            if calls.is_empty() {
                return None;
            }
            let resolved: Vec<String> = calls
                .iter()
                .map(|(_addr, selector)| {
                    abi_registry.get_selector_name(selector).unwrap_or_else(|| {
                        // Show short hex for unknown selectors
                        let hex = format!("{:#x}", selector);
                        if hex.len() > 10 {
                            format!("{}…", &hex[..10])
                        } else {
                            hex
                        }
                    })
                })
                .collect();

            if resolved.len() <= 3 {
                Some(resolved.join(", "))
            } else {
                let shown: Vec<_> = resolved[..3].to_vec();
                Some(format!("{}, … +{}", shown.join(", "), resolved.len() - 3))
            }
        })
        .collect()
}
