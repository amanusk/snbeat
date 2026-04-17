//! Async network task: receives `Action` requests from the UI over an mpsc
//! channel, dispatches to the appropriate data source, and sends results back.
//!
//! Split into focused sub-modules:
//! - `block` — fetch block detail, resolve endpoint names
//! - `transaction` — fetch tx + receipt, decode events/calldata
//! - `address` — multi-source parallel address loading (PF/Dune/RPC)
//! - `class` — fetch class ABI, declaration info, deployed contracts
//! - `search` — resolve search queries (block number / address / tx hash / class hash)
//! - `helpers` — shared utilities (build_tx_summary, backfill_timestamps, etc.)

mod address;
mod block;
mod class;
pub mod dune;
pub mod helpers;
mod search;
mod transaction;
pub mod voyager;
pub mod ws;

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::decode::AbiRegistry;

/// Runs the network task: receives actions from the UI, dispatches to data source,
/// sends results back.
pub async fn run_network_task(
    data_source: Arc<dyn DataSource>,
    abi_registry: Arc<AbiRegistry>,
    dune_client: Option<Arc<dune::DuneClient>>,
    pf_client: Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    voyager_client: Option<Arc<voyager::VoyagerClient>>,
    mut action_rx: mpsc::UnboundedReceiver<Action>,
    response_tx: mpsc::UnboundedSender<Action>,
) {
    info!("Network task started");

    while let Some(action) = action_rx.recv().await {
        let ds = Arc::clone(&data_source);
        let abi_reg = Arc::clone(&abi_registry);
        let dune = dune_client.clone();
        let pf = pf_client.clone();
        let voyager = voyager_client.clone();
        let tx = response_tx.clone();

        // Spawn each request as a separate task for concurrency
        tokio::spawn(async move {
            match action {
                Action::FetchRecentBlocks { count } => {
                    let start = std::time::Instant::now();
                    debug!(count, "Fetching recent blocks");
                    match ds.get_recent_blocks(count).await {
                        Ok(blocks) => {
                            info!(
                                count = blocks.len(),
                                elapsed_ms = start.elapsed().as_millis(),
                                "Recent blocks fetched"
                            );
                            let _ =
                                tx.send(rpc_source_update(crate::app::state::SourceStatus::Live));
                            let _ = tx.send(Action::BlocksLoaded(blocks));
                        }
                        Err(e) => {
                            error!(elapsed_ms = start.elapsed().as_millis(), error = %e, "Failed to fetch recent blocks");
                            let _ = tx.send(rpc_source_update(
                                crate::app::state::SourceStatus::ConnectError(e.to_string()),
                            ));
                            let _ = tx.send(Action::Error(format!("Fetch blocks: {e}")));
                        }
                    }
                }
                Action::FetchOlderBlocks { before, count } => {
                    debug!(before, count, "Fetching older blocks");
                    let end = before.saturating_sub(1);
                    let start = end.saturating_sub(count as u64 - 1);
                    let futs: Vec<_> = (start..=end)
                        .map(|num| {
                            let ds_c = Arc::clone(&ds);
                            async move { (num, ds_c.get_block(num).await) }
                        })
                        .collect();
                    let results = futures::future::join_all(futs).await;
                    let mut blocks: Vec<_> = results
                        .into_iter()
                        .filter_map(|(num, r)| match r {
                            Ok(block) => Some(block),
                            Err(e) => {
                                warn!(block = num, error = %e, "Failed to fetch older block");
                                None
                            }
                        })
                        .collect();
                    // Sort descending (newest first) to match the list order
                    blocks.sort_by(|a, b| b.number.cmp(&a.number));
                    let _ = tx.send(Action::OlderBlocksLoaded(blocks));
                }
                Action::FetchBlockDetail { number } => {
                    block::fetch_and_send_block_detail(number, &ds, &abi_reg, &voyager, &tx).await;
                }
                Action::FetchTransaction { hash } => {
                    transaction::fetch_and_send_transaction(hash, &ds, &abi_reg, &tx).await;
                }
                Action::FetchAddressInfo { address } => {
                    let _ = tx.send(Action::NavigateToAddress { address });
                    address::fetch_and_send_address_info(
                        address, &ds, &abi_reg, &dune, &pf, &voyager, &tx,
                    )
                    .await;
                }
                Action::FetchTxByNonce {
                    sender,
                    current_nonce,
                    direction,
                } => {
                    // Find the tx with the target nonce by scanning events for the sender.
                    // We look at the sender's events to find tx hashes, then check their nonces.
                    let target_nonce = (current_nonce as i64 + direction).max(0) as u64;
                    debug!(sender = %format!("{:#x}", sender), current_nonce, target_nonce, "Searching for tx by nonce");

                    // Strategy: fetch events for the sender, find tx with target nonce
                    match ds.get_events_for_address(sender, None, None, 200).await {
                        Ok(events) => {
                            // Collect unique tx hashes
                            let mut seen = std::collections::HashSet::new();
                            let tx_hashes: Vec<_> = events
                                .iter()
                                .filter_map(|e| {
                                    let h = e.transaction_hash;
                                    if h != starknet::core::types::Felt::ZERO && seen.insert(h) {
                                        Some(h)
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            // Find the tx with the target nonce
                            // Check a batch of txs to find the right nonce
                            let target_felt = starknet::core::types::Felt::from(target_nonce);
                            for hash in &tx_hashes {
                                if let Ok(fetched_tx) = ds.get_transaction(*hash).await {
                                    if let crate::data::types::SnTransaction::Invoke(ref inv) =
                                        fetched_tx
                                    {
                                        if inv.nonce == Some(target_felt)
                                            && inv.sender_address == sender
                                        {
                                            // Found it — reuse fetched_tx, only fetch receipt
                                            match ds.get_receipt(*hash).await {
                                                Ok(receipt) => {
                                                    transaction::decode_and_send_transaction(
                                                        fetched_tx, receipt, &abi_reg, &tx,
                                                    )
                                                    .await;
                                                }
                                                Err(e) => {
                                                    let _ = tx.send(Action::Error(format!(
                                                        "Fetch receipt: {e}"
                                                    )));
                                                }
                                            }
                                            return;
                                        }
                                    }
                                }
                            }
                            let _ = tx.send(Action::Error(format!(
                                "No tx found with nonce {target_nonce} for this sender"
                            )));
                        }
                        Err(e) => {
                            let _ =
                                tx.send(Action::Error(format!("Failed to search by nonce: {e}")));
                        }
                    }
                }
                Action::ResolveSearch { query } => {
                    search::resolve_search(query, &ds, &abi_reg, &dune, &pf, &voyager, &tx).await;
                }
                Action::EnrichAddressTxs { address, hashes } => {
                    address::enrich_address_txs(address, hashes, &ds, pf.as_ref(), &abi_reg, &tx)
                        .await;
                }
                Action::EnrichAddressEndpoints {
                    address,
                    current_nonce,
                    known_txs,
                } => {
                    address::run_endpoint_enrichment(
                        address,
                        current_nonce,
                        known_txs,
                        &ds,
                        &dune,
                        &pf,
                        &abi_reg,
                        &tx,
                    )
                    .await;
                }
                Action::FillAddressNonceGaps {
                    address,
                    known_txs,
                    gap,
                } => {
                    address::run_nonce_gap_fill(
                        address, known_txs, gap, &ds, &dune, &pf, &abi_reg, &tx,
                    )
                    .await;
                }
                Action::EnrichAddressCalls {
                    address,
                    hashes_with_blocks,
                } => {
                    let calls = address::build_contract_calls_from_hashes(
                        address,
                        &hashes_with_blocks,
                        &ds,
                        pf.as_ref(),
                        &abi_reg,
                    )
                    .await;
                    if !calls.is_empty() {
                        let _ = tx.send(Action::AddressCallsEnriched { address, calls });
                    }
                }
                Action::FetchMoreAddressTxs {
                    address,
                    before_block,
                    is_contract,
                } => {
                    address::fetch_more_address_txs(
                        address,
                        before_block,
                        is_contract,
                        &ds,
                        &dune,
                        &pf,
                        &abi_reg,
                        &tx,
                    )
                    .await;
                }
                Action::FetchClassInfo { class_hash } => {
                    class::fetch_class_info(class_hash, &ds, &abi_reg, &dune, &pf, &tx).await;
                }
                Action::PersistAddressTxs { address, txs } => {
                    ds.save_address_txs(&address, &txs);
                }
                Action::PersistAddressCalls { address, calls } => {
                    ds.save_address_calls(&address, &calls);
                }
                // Response actions are not handled here
                _ => {}
            }
        });
    }

    info!("Network task ended");
}

/// Helper to build an RPC source update action.
fn rpc_source_update(status: crate::app::state::SourceStatus) -> Action {
    Action::SourceUpdate {
        source: crate::app::actions::Source::Rpc,
        status,
    }
}

/// Helper to build a Dune source update action.
fn dune_source_update(status: crate::app::state::SourceStatus) -> Action {
    Action::SourceUpdate {
        source: crate::app::actions::Source::Dune,
        status,
    }
}

/// Spawns a polling task that checks for new blocks periodically.
pub fn spawn_block_poller(
    data_source: Arc<dyn DataSource>,
    response_tx: mpsc::UnboundedSender<Action>,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_block = 0u64;
        loop {
            tokio::time::sleep(interval).await;

            match data_source.get_latest_block_number().await {
                Ok(latest) => {
                    if latest > last_block && last_block > 0 {
                        // Fetch new blocks
                        for num in (last_block + 1)..=latest {
                            match data_source.get_block(num).await {
                                Ok(block) => {
                                    let _ = response_tx.send(Action::NewBlock(block));
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to fetch new block {num}: {e}");
                                }
                            }
                        }
                    }
                    last_block = latest;
                }
                Err(e) => {
                    tracing::warn!("Failed to poll latest block: {e}");
                }
            }
        }
    })
}
