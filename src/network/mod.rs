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
#![allow(clippy::too_many_arguments)]

mod address;
mod block;
mod class;
pub mod dune;
pub mod event_window;
pub mod helpers;
pub mod prices;
mod search;
mod transaction;
pub mod voyager;
pub mod ws;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
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
    price_client: Option<Arc<prices::PriceClient>>,
    mut action_rx: mpsc::UnboundedReceiver<Action>,
    response_tx: mpsc::UnboundedSender<Action>,
) {
    info!("Network task started");

    // Per-session cancellation: foreground navigation (opening a new address,
    // block, tx, class, or submitting a search) cancels this token so stale
    // background enrichment/pagination work for the previous view stops piling
    // up on pf-query. New work spawned after navigation captures the fresh
    // token. See `action_is_cancellable` / `action_is_fg_nav` below.
    let mut session_token = CancellationToken::new();

    while let Some(action) = action_rx.recv().await {
        // Rotate the session token on any foreground navigation. The old token
        // is cancelled, signalling in-flight bg tasks to unwind.
        if action_is_fg_nav(&action) {
            session_token.cancel();
            session_token = CancellationToken::new();
        }

        let ds = Arc::clone(&data_source);
        let abi_reg = Arc::clone(&abi_registry);
        let dune = dune_client.clone();
        let pf = pf_client.clone();
        let voyager = voyager_client.clone();
        let prices = price_client.clone();
        let tx = response_tx.clone();
        let cancel = session_token.clone();
        let cancellable = action_is_cancellable(&action);

        // Spawn each request as a separate task for concurrency
        tokio::spawn(async move {
            let cancel_inner = cancel.clone();
            let task = async move {
                let cancel = cancel_inner;
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
                                let _ = tx
                                    .send(rpc_source_update(crate::app::state::SourceStatus::Live));
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
                        block::fetch_and_send_block_detail(number, &ds, &abi_reg, &voyager, &tx)
                            .await;
                    }
                    Action::FetchTransaction { hash } => {
                        transaction::fetch_and_send_transaction(
                            hash,
                            &ds,
                            pf.as_ref(),
                            &abi_reg,
                            &tx,
                        )
                        .await;
                    }
                    Action::FetchAddressInfo { address } => {
                        let _ = tx.send(Action::NavigateToAddress { address });
                        address::fetch_and_send_address_info(
                            address, &ds, &abi_reg, &dune, &pf, &voyager, &tx, &cancel,
                        )
                        .await;
                    }
                    Action::RefreshAddressRpc { address } => {
                        address::refresh_address_rpc(address, &ds, &pf, &abi_reg, &tx).await;
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
                                        if h != starknet::core::types::Felt::ZERO && seen.insert(h)
                                        {
                                            Some(h)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();

                                // Find the tx with the target nonce. Fetch candidates in
                                // parallel (buffer_unordered) so worst-case latency is ~1
                                // RTT instead of N. Nonces are unique per sender on a
                                // healthy chain, so unordered traversal cannot pick a
                                // different "winner".
                                //
                                // We drain the stream to completion even after finding a
                                // match: the underlying `CachingDataSource::get_transaction`
                                // uses `Shared`-future dedup and removes its `pending_txs`
                                // entry only after the future is awaited to completion.
                                // Dropping the stream early would leave those entries in
                                // the pending map until some later caller observes them,
                                // so we let in-flight fetches finish (their results are
                                // cached for free) and just keep the first match.
                                use futures::stream::StreamExt;
                                let target_felt = starknet::core::types::Felt::from(target_nonce);
                                let ds_for_stream = ds.clone();
                                let stream = futures::stream::iter(tx_hashes.into_iter())
                                    .map(|h| {
                                        let ds = ds_for_stream.clone();
                                        async move { (h, ds.get_transaction(h).await) }
                                    })
                                    .buffer_unordered(8);

                                let mut found: Option<(
                                    starknet::core::types::Felt,
                                    crate::data::types::SnTransaction,
                                )> = None;
                                {
                                    tokio::pin!(stream);
                                    while let Some((hash, res)) = stream.next().await {
                                        if found.is_some() {
                                            continue;
                                        }
                                        if let Ok(fetched_tx) = res
                                            && let crate::data::types::SnTransaction::Invoke(
                                                ref inv,
                                            ) = fetched_tx
                                            && inv.nonce == Some(target_felt)
                                            && inv.sender_address == sender
                                        {
                                            found = Some((hash, fetched_tx));
                                        }
                                    }
                                }

                                if let Some((hash, fetched_tx)) = found {
                                    match ds.get_receipt(hash).await {
                                        Ok(receipt) => {
                                            transaction::decode_and_send_transaction(
                                                fetched_tx,
                                                receipt,
                                                &ds,
                                                pf.as_ref(),
                                                &abi_reg,
                                                &tx,
                                            )
                                            .await;
                                        }
                                        Err(e) => {
                                            let _ = tx
                                                .send(Action::Error(format!("Fetch receipt: {e}")));
                                        }
                                    }
                                    return;
                                }
                                let _ = tx.send(Action::Error(format!(
                                    "No tx found with nonce {target_nonce} for this sender"
                                )));
                            }
                            Err(e) => {
                                let _ = tx
                                    .send(Action::Error(format!("Failed to search by nonce: {e}")));
                            }
                        }
                    }
                    Action::ResolveSearch { query } => {
                        search::resolve_search(
                            query, &ds, &abi_reg, &dune, &pf, &voyager, &tx, &cancel,
                        )
                        .await;
                    }
                    Action::EnrichAddressTxs { address, hashes } => {
                        address::enrich_address_txs(
                            address,
                            hashes,
                            &ds,
                            pf.as_ref(),
                            &abi_reg,
                            &tx,
                        )
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
                    Action::FetchAddressMetaTxs {
                        address,
                        from_block,
                        continuation_token,
                        window_size,
                        limit,
                    } => {
                        // On first page, emit cached rows immediately so the UI
                        // renders instantly. Uses a separate CacheLoaded action so
                        // it merges without clearing the in-flight loading flag.
                        if continuation_token.is_none() {
                            let cached = ds.load_cached_meta_txs(&address);
                            if !cached.is_empty() {
                                let _ = tx.send(Action::AddressMetaTxsCacheLoaded {
                                    address,
                                    summaries: cached,
                                });
                            }
                        }
                        if let Some(pf) = pf.as_ref() {
                            address::fetch_address_meta_txs(
                                address,
                                from_block,
                                continuation_token,
                                window_size,
                                limit,
                                &ds,
                                pf,
                                &abi_reg,
                                &tx,
                            )
                            .await;
                        } else {
                            // pf-query required for this feature. Send empty result so
                            // the UI clears its loading state instead of hanging.
                            let _ = tx.send(Action::AddressMetaTxsLoaded {
                                address,
                                summaries: Vec::new(),
                                next_token: None,
                                next_window_size: None,
                            });
                        }
                    }
                    Action::ClassifyPotentialMetaTx { address, tx_hash } => {
                        // Streaming path from WS: a TRANSACTION_EXECUTED event for
                        // `address` arrived. Fetch the single tx via pf-query, run
                        // it through the shared classifier, and — if it's actually
                        // a meta-tx where `address` is the intender — emit it to
                        // merge into the MetaTxs tab. Silent no-op otherwise
                        // (including when pf-query is unavailable).
                        let Some(pf) = pf.as_ref() else {
                            return;
                        };
                        match pf.get_txs_by_hash(&[tx_hash]).await {
                            Ok(rows) => {
                                let Some(row) = rows.first() else {
                                    return;
                                };
                                if let Some(summary) =
                                    address::classify_meta_tx_candidate(address, row, &abi_reg)
                                        .await
                                {
                                    // Persist immediately so the row survives
                                    // restart even if the user never scrolls past
                                    // the bulk fetch range. INSERT OR REPLACE
                                    // handles dedup at the DB layer.
                                    ds.save_meta_txs(&address, std::slice::from_ref(&summary));
                                    let _ = tx.send(Action::AddressMetaTxsStreamed {
                                        address,
                                        summaries: vec![summary],
                                    });
                                }
                            }
                            Err(e) => {
                                debug!(tx = %format!("{:#x}", tx_hash), error = %e, "WS meta-tx classify: get_txs_by_hash failed");
                            }
                        }
                    }
                    Action::DecodeAddressWsEvent { address, event } => {
                        // Streaming path from WS: a raw event for the
                        // currently-viewed `address` arrived. Resolve its
                        // emitter ABI, decode, and forward so the Events tab
                        // merges the decoded row in real time. ABI miss falls
                        // back to an undecoded `DecodedEvent` (raw keys/data
                        // preserved) — the user still sees live activity, it
                        // just renders without names.
                        let abi = abi_reg.get_abi_for_address(&event.from_address).await;
                        let decoded = crate::decode::events::decode_event(&event, abi.as_deref());
                        let _ = tx.send(Action::AddressEventStreamed {
                            address,
                            decoded_event: decoded,
                        });
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
                    Action::FetchTokenPricesToday { tokens } => {
                        if let Some(pc) = &prices
                            && pc.ensure_today(&tokens).await
                        {
                            let _ = tx.send(Action::PricesUpdated);
                        }
                    }
                    Action::FetchTokenPricesHistoric { requests } => {
                        if let Some(pc) = &prices
                            && pc.ensure_historic(&requests).await
                        {
                            let _ = tx.send(Action::PricesUpdated);
                        }
                    }
                    Action::FetchTokenMetadata { token } => {
                        let meta =
                            crate::data::token_metadata::fetch_token_metadata(token, &*ds).await;
                        if let Some(m) = &meta {
                            ds.save_token_metadata(&token, m);
                            tracing::debug!(
                                token = %format!("{:#x}", token),
                                symbol = %m.symbol,
                                decimals = m.decimals,
                                "Token metadata fetched"
                            );
                        } else {
                            tracing::debug!(
                                token = %format!("{:#x}", token),
                                "Token metadata fetch failed (not ERC-20-shaped or RPC error)"
                            );
                        }
                        let _ = tx.send(Action::TokenMetadataLoaded { token, meta });
                    }
                    Action::FetchPrivateNotes { user, viewing_key } => {
                        // Re-wrap the raw felt as a SecretFelt so it gets
                        // zeroize-on-drop after the sync completes. The
                        // value already passed through the channel as a
                        // bare Felt — the wrapper is just for in-memory
                        // hygiene, not for cryptographic confidentiality
                        // beyond what the channel already provides.
                        let key =
                            crate::decode::privacy_crypto::types::SecretFelt::new(viewing_key);
                        let backend = crate::decode::privacy_sync::StorageBackend::new(
                            pf.clone(),
                            Arc::clone(&ds),
                        );
                        match crate::decode::privacy_sync::sync_user_notes(user, &key, &backend)
                            .await
                        {
                            Ok((index, _block_number)) => {
                                let nullifiers: Vec<_> =
                                    index.by_nullifier.iter().map(|(n, id)| (*n, *id)).collect();
                                let notes: Vec<_> = index.notes.into_values().collect();
                                let _ = tx.send(Action::PrivateNotesIndexed {
                                    user,
                                    notes,
                                    nullifiers,
                                });
                            }
                            Err(e) => {
                                tracing::warn!(
                                    user = %format!("{:#x}", user),
                                    error = %e,
                                    "Privacy sync failed"
                                );
                            }
                        }
                    }
                    // Response actions are not handled here
                    _ => {}
                }
            };

            if cancellable {
                tokio::select! {
                    _ = task => {}
                    _ = cancel.cancelled() => {
                        debug!("Background task cancelled on navigation");
                    }
                }
            } else {
                task.await;
            }
        });
    }

    info!("Network task ended");
}

/// Actions that represent foreground navigation. Receiving one of these
/// cancels the current session token, which in turn stops any in-flight
/// cancellable background work (enrichment, MetaTx pagination, etc.) for the
/// previous view.
fn action_is_fg_nav(action: &Action) -> bool {
    matches!(
        action,
        Action::FetchAddressInfo { .. }
            | Action::ResolveSearch { .. }
            | Action::FetchBlockDetail { .. }
            | Action::FetchTransaction { .. }
            | Action::FetchClassInfo { .. }
            | Action::FetchTxByNonce { .. }
    )
}

/// Actions that should abort when the session token is cancelled. These are
/// address-view-scoped background operations — stale ones just waste
/// pf-query / RPC cycles and delay the new view. Foreground fetches (block,
/// tx, class detail, the initial address info) are intentionally excluded:
/// aborting them leaves the UI half-loaded.
fn action_is_cancellable(action: &Action) -> bool {
    matches!(
        action,
        Action::EnrichAddressTxs { .. }
            | Action::EnrichAddressEndpoints { .. }
            | Action::FillAddressNonceGaps { .. }
            | Action::EnrichAddressCalls { .. }
            | Action::FetchAddressMetaTxs { .. }
            | Action::ClassifyPotentialMetaTx { .. }
            | Action::DecodeAddressWsEvent { .. }
            | Action::FetchMoreAddressTxs { .. }
            | Action::RefreshAddressRpc { .. }
    )
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
