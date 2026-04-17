//! Address-related network functions: fetching address info, enriching txs,
//! filling nonce gaps, fetching token balances, and deploy tx lookups.

use std::sync::Arc;

use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info, warn};

/// Caps concurrent RPC `get_transaction` / `get_receipt` calls fired from the
/// background enrichment path so that a user-initiated `FetchTransaction`
/// never queues behind dozens of enrichment round trips. Only applies when
/// pf-query is unavailable (or didn't return the requested hash). User clicks
/// bypass this semaphore entirely.
static ENRICH_RPC_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(8));

use crate::app::actions::{Action, Source};
use crate::data::DataSource;
use crate::data::types::SnTransaction;
use crate::decode::AbiRegistry;
use crate::decode::events::decode_event;
use crate::decode::functions::parse_multicall;
use crate::utils::felt_to_u128;

use super::block::spawn_voyager_prefetch;
use super::dune;
use super::dune_source_update;
use super::helpers;
use super::voyager;

/// UDC ContractDeployed event selector.
const UDC_CONTRACT_DEPLOYED: &str =
    "0x26b160f10156dea0639bec90696772c640b9706a47f5b8c52ea1abe5858b34d";

/// Whether we're fetching events for a contract or a user account.
///
/// Determines both the event filter (accounts use the `transaction_executed`
/// selector key filter; contracts fetch everything the contract emitted) and
/// the preferred primary data source — see [`fetch_events_routed`].
#[derive(Debug, Clone, Copy)]
pub(super) enum EventQueryKind {
    /// Account address — we want one event per invoke (`transaction_executed`).
    Account,
    /// Contract address — we want all events emitted by the contract.
    Contract,
}

/// Fetch address-scoped events, routing between pathfinder and RPC per benchmark.
///
/// Routing (from `bench_events_rpc_vs_pathfinder`):
///  - **Contracts** → pathfinder primary, RPC fallback.
///    PF's bloom-filter index on `from_address` makes it 1-2ms TTFE vs ~1s
///    cold RPC probe; page fetches are 36-138ms vs ~230ms on RPC.
///  - **Accounts** → RPC primary, PF fallback.
///    PF's bloom indexes `from_address` but not keys, so the
///    `transaction_executed` filter doesn't narrow its scan — RPC with the
///    key filter wins on cold windows (20-30ms vs 165-634ms) and on stale
///    accounts (11-17ms vs 118-131ms).
///
/// `to_block` is an inclusive upper bound (useful for pagination into older
/// history); `None` means "up to latest".
pub(super) async fn fetch_events_routed(
    kind: EventQueryKind,
    pf: Option<&Arc<crate::data::pathfinder::PathfinderClient>>,
    ds: &Arc<dyn DataSource>,
    address: starknet::core::types::Felt,
    from_block: Option<u64>,
    to_block: Option<u64>,
    limit: usize,
) -> crate::error::Result<Vec<crate::data::types::SnEvent>> {
    // Local helpers capture the `kind`-dependent calls in one place so the
    // primary/fallback flow below stays agnostic.
    async fn from_pf(
        kind: EventQueryKind,
        pf: &Arc<crate::data::pathfinder::PathfinderClient>,
        address: starknet::core::types::Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> crate::error::Result<Vec<crate::data::types::SnEvent>> {
        let res = match kind {
            EventQueryKind::Contract => {
                pf.get_contract_events(
                    address,
                    from_block.unwrap_or(0),
                    to_block,
                    &[],
                    limit as u32,
                    None,
                )
                .await
            }
            EventQueryKind::Account => {
                pf.get_events_for_address(
                    address,
                    from_block.unwrap_or(0),
                    to_block,
                    limit as u32,
                    None,
                )
                .await
            }
        };
        res.map(|(events, _token)| events)
            .map_err(|e| crate::error::SnbeatError::Provider(e.to_string()))
    }

    async fn from_ds(
        kind: EventQueryKind,
        ds: &Arc<dyn DataSource>,
        address: starknet::core::types::Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> crate::error::Result<Vec<crate::data::types::SnEvent>> {
        match kind {
            EventQueryKind::Contract => {
                ds.get_contract_events(address, from_block, to_block, limit)
                    .await
            }
            EventQueryKind::Account => {
                ds.get_events_for_address(address, from_block, to_block, limit)
                    .await
            }
        }
    }

    match kind {
        EventQueryKind::Contract => {
            // PF primary, RPC fallback.
            if let Some(pf_client) = pf {
                match from_pf(kind, pf_client, address, from_block, to_block, limit).await {
                    Ok(events) => return Ok(events),
                    Err(e) => {
                        warn!(error = %e, "PF contract events failed, falling back to RPC");
                    }
                }
            }
            from_ds(kind, ds, address, from_block, to_block, limit).await
        }
        EventQueryKind::Account => {
            // RPC primary, PF fallback.
            match from_ds(kind, ds, address, from_block, to_block, limit).await {
                Ok(events) => Ok(events),
                Err(rpc_err) => {
                    if let Some(pf_client) = pf {
                        warn!(error = %rpc_err, "RPC account events failed, falling back to PF");
                        match from_pf(kind, pf_client, address, from_block, to_block, limit).await {
                            Ok(events) => Ok(events),
                            Err(pf_err) => {
                                warn!(error = %pf_err, "PF account events fallback also failed");
                                Err(rpc_err)
                            }
                        }
                    } else {
                        Err(rpc_err)
                    }
                }
            }
        }
    }
}

/// Fetch token balances for all known tokens for an address.
async fn fetch_token_balances(
    address: starknet::core::types::Felt,
    ds: &Arc<dyn DataSource>,
) -> Vec<crate::data::types::TokenBalance> {
    let balance_selector = starknet::core::utils::get_selector_from_name("balance_of").unwrap();
    let known_tokens: &[(&str, &str, u8)] = &[
        (
            "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
            "STRK",
            18,
        ),
        (
            "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
            "ETH",
            18,
        ),
        (
            "0x0124aeb495b947201f5fac96fd1138e326ad86195b98df6dec9009158a533b49",
            "wBTC",
            8,
        ),
        (
            "0x033068f6539f8e6e6b131e6b2b814e6c34a5224bc66947c47dab9dfee93b35fb",
            "USDC",
            6,
        ),
        (
            "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
            "USDC.e",
            6,
        ),
        (
            "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
            "USDT",
            6,
        ),
    ];

    let balance_futures: Vec<_> = known_tokens
        .iter()
        .map(|(hex, name, decimals)| {
            let ds_bal = Arc::clone(ds);
            let token = starknet::core::types::Felt::from_hex(hex).unwrap();
            let name = name.to_string();
            let decimals = *decimals;
            async move {
                let result = ds_bal
                    .call_contract(token, balance_selector, vec![address])
                    .await;
                (token, name, decimals, result)
            }
        })
        .collect();

    let balance_results = futures::future::join_all(balance_futures).await;
    let mut token_balances = Vec::new();
    for (token, name, decimals, result) in balance_results {
        let balance_felt = result
            .ok()
            .and_then(|v| v.first().copied())
            .unwrap_or(starknet::core::types::Felt::ZERO);
        token_balances.push(crate::data::types::TokenBalance {
            token_address: token,
            token_name: name,
            balance_raw: balance_felt,
            decimals,
        });
    }
    token_balances
}

/// Convert Pathfinder SenderTxEntry records to AddressTxSummary.
fn pf_txs_to_summaries(
    pf_txs: Vec<crate::data::pathfinder::SenderTxEntry>,
) -> Vec<crate::data::types::AddressTxSummary> {
    let mut summaries = Vec::new();
    for pt in pf_txs {
        if pt.hash.is_empty() {
            continue; // stub entry from pf-query
        }
        let hash = match starknet::core::types::Felt::from_hex(&pt.hash) {
            Ok(h) => h,
            Err(_) => continue,
        };
        let fee = u128::from_str_radix(pt.actual_fee.trim_start_matches("0x"), 16).unwrap_or(0);
        let tx_type = match pt.tx_type.as_str() {
            t if t.starts_with("INVOKE") => "INVOKE".to_string(),
            t if t.starts_with("DECLARE") => "DECLARE".to_string(),
            t if t.starts_with("DEPLOY_ACCOUNT") => "DEPLOY_ACCOUNT".to_string(),
            t if t.starts_with("DEPLOY") => "DEPLOY".to_string(),
            t if t.starts_with("L1_HANDLER") => "L1_HANDLER".to_string(),
            other => other.to_string(),
        };
        let sender = pt
            .sender_address
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(|s| starknet::core::types::Felt::from_hex(s).ok());
        summaries.push(crate::data::types::AddressTxSummary {
            hash,
            nonce: pt.nonce.unwrap_or(0),
            block_number: pt.block_number,
            timestamp: pt.timestamp,
            endpoint_names: String::new(), // decoded later via enrich
            total_fee_fri: fee,
            tip: pt.tip,
            tx_type,
            status: pt.status,
            sender,
        });
    }
    summaries
}

/// Fetch tx+receipt for a batch of hashes, build AddressTxSummary for each.
/// Also backfills timestamps from block data.
async fn fetch_tx_summaries_from_hashes(
    hashes: &[starknet::core::types::Felt],
    block_map: &std::collections::HashMap<starknet::core::types::Felt, u64>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    tx: &mpsc::UnboundedSender<Action>,
    progress_prefix: &str,
) -> Vec<crate::data::types::AddressTxSummary> {
    let total = hashes.len();
    let mut summaries = Vec::new();
    for (chunk_idx, chunk) in hashes.chunks(20).enumerate() {
        let fetched_so_far = chunk_idx * 20;
        if !progress_prefix.is_empty() {
            let _ = tx.send(Action::LoadingStatus(format!(
                "{progress_prefix} {fetched_so_far}/{total}..."
            )));
        }
        let futs: Vec<_> = chunk
            .iter()
            .map(|hash| {
                let ds_tx = Arc::clone(ds);
                let ds_rx = Arc::clone(ds);
                let h = *hash;
                async move {
                    let (tx_r, rx_r) = tokio::join!(ds_tx.get_transaction(h), ds_rx.get_receipt(h));
                    (h, tx_r, rx_r)
                }
            })
            .collect();
        let results = futures::future::join_all(futs).await;

        for (hash, tx_result, rx_result) in results {
            if let Ok(fetched_tx) = tx_result {
                let receipt = rx_result.ok();
                let block_num = receipt
                    .as_ref()
                    .map(|r| r.block_number)
                    .unwrap_or_else(|| block_map.get(&hash).copied().unwrap_or(0));
                summaries.push(helpers::build_tx_summary(
                    hash,
                    &fetched_tx,
                    receipt.as_ref(),
                    block_num,
                    0,
                    abi_reg,
                ));
            }
        }
    }

    helpers::backfill_timestamps(&mut summaries, ds, pf).await;
    summaries
}

/// Build ContractCallSummary entries from event tx hashes by fetching tx+receipt
/// and extracting calls to the target contract. Also backfills timestamps.
pub(super) async fn build_contract_calls_from_hashes(
    address: starknet::core::types::Felt,
    hashes_with_blocks: &[(starknet::core::types::Felt, u64)],
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
) -> Vec<crate::data::types::ContractCallSummary> {
    // Pre-fetch ABI for the target contract so selector→name lookups succeed.
    let _ = abi_reg.get_abi_for_address(&address).await;

    let mut calls_list = Vec::new();
    for chunk in hashes_with_blocks.chunks(20) {
        let futs: Vec<_> = chunk
            .iter()
            .map(|(hash, block_num)| {
                let ds_t = Arc::clone(ds);
                let ds_r = Arc::clone(ds);
                let h = *hash;
                let bn = *block_num;
                async move {
                    let (tx_r, rx_r) = tokio::join!(ds_t.get_transaction(h), ds_r.get_receipt(h));
                    (h, bn, tx_r, rx_r)
                }
            })
            .collect();
        let results = futures::future::join_all(futs).await;

        for (hash, block_num, tx_r, rx_r) in results {
            if let Ok(fetched_tx) = tx_r {
                let receipt = rx_r.ok();
                let fee_fri = receipt
                    .as_ref()
                    .map(|r| felt_to_u128(&r.actual_fee))
                    .unwrap_or(0);
                let status = receipt
                    .as_ref()
                    .map(|r| match &r.execution_status {
                        crate::data::types::ExecutionStatus::Succeeded => "OK",
                        crate::data::types::ExecutionStatus::Reverted(_) => "REV",
                        _ => "?",
                    })
                    .unwrap_or("?")
                    .to_string();
                let function_name = match &fetched_tx {
                    crate::data::types::SnTransaction::Invoke(i) => {
                        let calls = parse_multicall(&i.calldata);
                        calls
                            .iter()
                            .filter(|c| c.contract_address == address)
                            .map(|c| {
                                abi_reg.get_selector_name(&c.selector).unwrap_or_else(|| {
                                    let hex = format!("{:#x}", c.selector);
                                    if hex.len() > 10 {
                                        format!("{}…", &hex[..10])
                                    } else {
                                        hex
                                    }
                                })
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                    _ => String::new(),
                };
                calls_list.push(crate::data::types::ContractCallSummary {
                    tx_hash: hash,
                    sender: fetched_tx.sender(),
                    function_name,
                    block_number: block_num,
                    timestamp: 0,
                    total_fee_fri: fee_fri,
                    status,
                });
            }
        }
    }

    helpers::backfill_call_timestamps(&mut calls_list, ds, pf).await;
    crate::data::types::deduplicate_contract_calls(calls_list)
}

/// Fetch address info with parallel, opportunistic data loading.
///
/// Fires all data sources (PF, Dune, RPC) in parallel and streams results to the
/// UI as each source completes. The UI merges incrementally via AddressTxsStreamed.
pub(super) async fn fetch_and_send_address_info(
    address: starknet::core::types::Felt,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    dune: &Option<Arc<dune::DuneClient>>,
    pf: &Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    voyager_c: &Option<Arc<voyager::VoyagerClient>>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    let start = std::time::Instant::now();
    debug!(address = %format!("{:#x}", address), "Fetching address info");

    // Kick off Voyager label fetch immediately — runs fully in parallel with all other IO.
    if let Some(vc) = voyager_c {
        let vc = Arc::clone(vc);
        let tx_v = tx.clone();
        tokio::spawn(async move {
            voyager::fetch_and_send_label(address, &vc, &tx_v).await;
        });
    }

    // Step 1: Fetch nonce + class_hash fast, send to UI immediately
    let ds2 = Arc::clone(ds);
    let (nonce_r, class_r) = tokio::join!(ds.get_nonce(address), ds2.get_class_hash(address));
    let nonce = nonce_r.unwrap_or(starknet::core::types::Felt::ZERO);
    let class_hash = class_r.ok();

    // Detect contract type early: nonce == 0 + has class_hash = likely a contract, not an account
    let is_contract = nonce == starknet::core::types::Felt::ZERO && class_hash.is_some();

    // Compute nonce delta: how many new txs since last visit?
    // This tells us whether we can skip fetching entirely (delta=0) or narrow the search.
    let cached_nonce_info = ds.load_cached_nonce(&address);
    let nonce_delta = if let Some((prev_nonce, _prev_block)) = &cached_nonce_info {
        let prev = crate::utils::felt_to_u64(prev_nonce);
        let curr = crate::utils::felt_to_u64(&nonce);
        curr.saturating_sub(prev)
    } else {
        u64::MAX // unknown — do full search
    };
    let cached_nonce_block = cached_nonce_info.map(|(_, b)| b).unwrap_or(0);
    // Save current nonce for next visit — but only if non-zero.
    // A nonce=0 contract might gain account functionality later, so we must
    // always re-check rather than locking in "no txs" from a cached zero.
    if nonce != starknet::core::types::Felt::ZERO {
        let latest = ds.get_latest_block_number().await.unwrap_or(0);
        ds.save_cached_nonce(&address, &nonce, latest);
    }

    // Fire-and-forget: fetch class history from PF if available
    if let Some(pf_client) = pf {
        let pf_c = Arc::clone(pf_client);
        let ds_c = Arc::clone(ds);
        let tx_c = tx.clone();
        let addr = address;
        tokio::spawn(async move {
            match pf_c.get_class_history(addr).await {
                Ok(entries) => {
                    // Use earliest entry (deployment block) to find the deploy tx
                    if let Some(deploy_entry) = entries.last() {
                        let deploy_block = deploy_entry.block_number;
                        let tx_c2 = tx_c.clone();
                        let ds_c2 = Arc::clone(&ds_c);
                        tokio::spawn(async move {
                            find_deploy_tx(addr, deploy_block, &ds_c2, &tx_c2).await;
                        });
                    }
                    let _ = tx_c.send(Action::ClassHistoryLoaded {
                        address: addr,
                        entries,
                    });
                }
                Err(e) => {
                    warn!(error = %e, "PF class history fetch failed");
                    let _ = tx_c.send(Action::SourceUpdate {
                        source: Source::Pathfinder,
                        status: crate::app::state::SourceStatus::FetchError(e.to_string()),
                    });
                }
            }
        });
    }

    // --- Determine which sources to fire and tell the UI ---
    // Must be sent BEFORE AddressInfoLoaded so the handler sees sources_pending
    // is non-empty and doesn't prematurely clear the loading state.
    let mut sources = vec![Source::Rpc];
    if !is_contract && pf.is_some() {
        sources.push(Source::Pathfinder);
    }
    if dune.is_some() {
        sources.push(Source::Dune);
    }
    let _ = tx.send(Action::AddressSourcesPending {
        address,
        sources: sources.clone(),
    });

    // Send partial info immediately (cached txs seed the UI)
    let _ = tx.send(Action::AddressInfoLoaded {
        info: crate::data::types::SnAddressInfo {
            address,
            nonce,
            class_hash,
            recent_events: Vec::new(),
            token_balances: Vec::new(),
        },
        decoded_events: Vec::new(),
        tx_summaries: ds.load_cached_address_txs(&address),
        contract_calls: ds.load_cached_address_calls(&address),
    });

    // Fetch balances in background
    {
        let ds_bal = Arc::clone(ds);
        let tx_bal = tx.clone();
        tokio::spawn(async move {
            let balances = fetch_token_balances(address, &ds_bal).await;
            let _ = tx_bal.send(Action::AddressBalancesLoaded { address, balances });
        });
    }

    // Check cached activity range — if fresh, skip Dune probe entirely.
    let cached_range = ds.load_cached_activity_range(&address);
    if let Some((min_b, max_b)) = cached_range {
        debug!(
            address = %format!("{:#x}", address),
            cached_range = format!("{}..{}", min_b, max_b),
            "Using cached activity range"
        );
        let _ = tx.send(Action::LoadingStatus(format!(
            "Cached range: blocks {}..{}",
            min_b, max_b
        )));
    }

    // Spawn Dune activity probe — used by TASK B and TASK C to target the right block range.
    // Uses starknet.events (3-4x cheaper than the old UNION ALL approach).
    // watch channel so both TASK B (Dune) and TASK C (RPC) can observe the probe result.
    let (probe_watch_tx, probe_watch_rx) =
        tokio::sync::watch::channel::<Option<dune::AddressActivityProbe>>(None);
    // Load cached range WITH event count for accurate density calculation
    let cached_range_with_count = ds.load_cached_activity_range_with_count(&address);
    if cached_range.is_some() {
        // Build probe from cache — no Dune query needed.
        let (min_b, max_b) = cached_range.unwrap();
        let event_count = cached_range_with_count
            .map(|(_, _, c)| c)
            .filter(|&c| c > 1) // Ignore stale rows with placeholder count
            .unwrap_or_else(|| {
                // No real count cached — estimate conservatively from block span.
                // Assume moderate activity (~1 event per 100 blocks) as a safe default.
                let span = max_b.saturating_sub(min_b).max(1);
                (span / 100).max(100)
            });
        let mut probe = dune::AddressActivityProbe::default();
        probe.sender_min_block = min_b;
        probe.sender_max_block = max_b;
        probe.callee_min_block = min_b;
        probe.callee_max_block = max_b;
        probe.sender_tx_count = event_count;
        probe.callee_call_count = event_count;
        let _ = probe_watch_tx.send(Some(probe));
    } else if let Some(dune_client) = dune {
        let dune_p = Arc::clone(dune_client);
        let ds_probe = Arc::clone(ds);
        let tx_probe = tx.clone();
        tokio::spawn(async move {
            let _ = tx_probe.send(Action::LoadingStatus(
                "Dune: probing activity range (events)...".into(),
            ));
            match dune_p.probe_address_activity(address).await {
                Ok(probe) => {
                    if probe.has_activity() {
                        let _ = tx_probe.send(Action::LoadingStatus(format!(
                            "Dune probe: {} events, blocks {}..{}",
                            probe.sender_tx_count,
                            probe.min_block(),
                            probe.max_block(),
                        )));
                        // Cache the discovered range + event count for next time.
                        let total_events = probe.sender_tx_count.max(probe.callee_call_count);
                        ds_probe.save_activity_range_with_count(
                            &address,
                            probe.min_block(),
                            probe.max_block(),
                            total_events,
                        );
                    } else {
                        let _ = tx_probe.send(Action::LoadingStatus(
                            "Dune probe: no activity found".into(),
                        ));
                    }
                    // Send probe to UI for pagination window sizing.
                    let _ = tx_probe.send(Action::AddressProbeLoaded {
                        address,
                        probe: probe.clone(),
                    });
                    let _ = probe_watch_tx.send(Some(probe));
                }
                Err(e) => {
                    warn!(error = %e, "Dune activity probe failed");
                    let _ =
                        tx_probe.send(Action::LoadingStatus(format!("Dune probe failed: {}", e)));
                    // Leave watch at None — tasks will use default windows.
                }
            }
        });
    }

    // Shared flag: set by PF task on success so RPC task can skip deep search
    let pf_succeeded = Arc::new(AtomicBool::new(false));

    // =====================================================================
    // TASK A: Pathfinder get_sender_txs() — fastest for accounts (1-3s)
    // =====================================================================
    if !is_contract {
        if let Some(pf_client) = pf {
            let pf_c = Arc::clone(pf_client);
            let tx_a = tx.clone();
            let ds_a = Arc::clone(ds);
            let pf_ok = Arc::clone(&pf_succeeded);
            tokio::spawn(async move {
                const PF_LIMIT: u32 = 200;
                let _ = tx_a.send(Action::LoadingStatus("PF: fetching tx history...".into()));
                match pf_c.get_sender_txs(address, PF_LIMIT).await {
                    Ok(pf_txs) => {
                        let _ = tx_a.send(Action::SourceUpdate {
                            source: Source::Pathfinder,
                            status: crate::app::state::SourceStatus::Live,
                        });
                        let real_count = pf_txs.iter().filter(|t| !t.hash.is_empty()).count();
                        info!(
                            pf_txs = pf_txs.len(),
                            real_txs = real_count,
                            "PF sender-txs returned"
                        );
                        let summaries = pf_txs_to_summaries(pf_txs);
                        // Save to cache
                        if !summaries.is_empty() {
                            let min_b = summaries.iter().map(|s| s.block_number).min().unwrap_or(0);
                            let max_b = summaries.iter().map(|s| s.block_number).max().unwrap_or(0);
                            let _ = tx_a.send(Action::LoadingStatus(format!(
                                "PF: {} txs, blocks {}..{}",
                                summaries.len(),
                                min_b,
                                max_b
                            )));
                            ds_a.save_address_txs(&address, &summaries);
                            // Cache activity range from PF results
                            if min_b > 0 {
                                ds_a.save_activity_range(&address, min_b, max_b);
                            }
                            // Only signal success when PF actually returned txs —
                            // otherwise RPC should still do its deep search.
                            pf_ok.store(true, Ordering::Release);
                        }
                        let _ = tx_a.send(Action::AddressTxsStreamed {
                            address,
                            source: Source::Pathfinder,
                            tx_summaries: summaries,
                            complete: true,
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "PF sender-txs failed");
                        let _ = tx_a.send(Action::SourceUpdate {
                            source: Source::Pathfinder,
                            status: crate::app::state::SourceStatus::FetchError(e.to_string()),
                        });
                        let _ = tx_a.send(Action::AddressTxsStreamed {
                            address,
                            source: Source::Pathfinder,
                            tx_summaries: Vec::new(),
                            complete: true,
                        });
                    }
                }
            });
        }
    }

    // =====================================================================
    // TASK B: Dune query — narrow windowed fetch for fast initial display
    // =====================================================================
    if let Some(dune_client) = dune {
        let dune_c = Arc::clone(dune_client);
        let tx_b = tx.clone();
        let abi_b = Arc::clone(abi_reg);
        let ds_b = Arc::clone(ds);
        let pf_b = pf.as_ref().map(Arc::clone);
        let mut probe_rx_b = probe_watch_rx.clone();
        const DUNE_PAGE_LIMIT: u32 = 100;
        const INITIAL_WINDOW: u64 = 5_000;

        if is_contract {
            tokio::spawn(async move {
                let _ = tx_b.send(Action::LoadingStatus(
                    "Dune: fetching recent contract calls...".into(),
                ));
                let latest_block = ds_b.get_latest_block_number().await.unwrap_or(0);
                let from = latest_block.saturating_sub(INITIAL_WINDOW);

                let result = dune_c
                    .query_contract_calls_windowed(address, from, latest_block, DUNE_PAGE_LIMIT)
                    .await;

                match result {
                    Ok(calls) if !calls.is_empty() => {
                        let _ =
                            tx_b.send(dune_source_update(crate::app::state::SourceStatus::Live));
                        let count = calls.len();
                        info!(calls = count, "Dune windowed contract calls complete");

                        let dune_calls =
                            enrich_dune_calls(address, calls, &abi_b, &ds_b, pf_b.as_ref(), &tx_b)
                                .await;

                        if !dune_calls.is_empty() {
                            let min_b = dune_calls.last().map(|c| c.block_number).unwrap_or(0);
                            let max_b = dune_calls.first().map(|c| c.block_number).unwrap_or(0);
                            let _ = tx_b.send(Action::LoadingStatus(format!(
                                "Dune: {} calls, blocks {}..{}",
                                dune_calls.len(),
                                min_b,
                                max_b
                            )));
                            // Cache calls for next visit
                            ds_b.save_address_calls(&address, &dune_calls);
                        }

                        let _ = tx_b.send(Action::AddressInfoLoaded {
                            info: crate::data::types::SnAddressInfo {
                                address,
                                nonce,
                                class_hash,
                                recent_events: Vec::new(),
                                token_balances: Vec::new(),
                            },
                            decoded_events: Vec::new(),
                            tx_summaries: Vec::new(),
                            contract_calls: dune_calls,
                        });

                        let _ = tx_b.send(Action::AddressTxsStreamed {
                            address,
                            source: Source::Dune,
                            tx_summaries: Vec::new(),
                            complete: true,
                        });
                    }
                    Ok(_) | Err(_) => {
                        if let Err(ref e) = result {
                            warn!(error = %e, "Dune windowed contract calls failed");
                        }
                        // Initial window empty — wait for probe, retry with probe-guided window
                        let probe = tokio::time::timeout(
                            tokio::time::Duration::from_secs(10),
                            probe_rx_b.wait_for(|p| p.is_some()),
                        )
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .and_then(|p| p.clone());

                        if let Some(p) = probe {
                            if p.has_activity() {
                                let window = p.recommended_window();
                                let probe_from = p.max_block().saturating_sub(window);
                                let probe_to = p.max_block();
                                let _ = tx_b.send(Action::LoadingStatus(format!(
                                    "Dune: retrying blocks {}..{} (probe-guided)...",
                                    probe_from, probe_to
                                )));
                                match dune_c
                                    .query_contract_calls_windowed(
                                        address,
                                        probe_from,
                                        probe_to,
                                        DUNE_PAGE_LIMIT,
                                    )
                                    .await
                                {
                                    Ok(calls) if !calls.is_empty() => {
                                        let _ = tx_b.send(dune_source_update(
                                            crate::app::state::SourceStatus::Live,
                                        ));
                                        let dune_calls = enrich_dune_calls(
                                            address,
                                            calls,
                                            &abi_b,
                                            &ds_b,
                                            pf_b.as_ref(),
                                            &tx_b,
                                        )
                                        .await;
                                        if !dune_calls.is_empty() {
                                            let min_b = dune_calls
                                                .last()
                                                .map(|c| c.block_number)
                                                .unwrap_or(0);
                                            let max_b = dune_calls
                                                .first()
                                                .map(|c| c.block_number)
                                                .unwrap_or(0);
                                            let _ = tx_b.send(Action::LoadingStatus(format!(
                                                "Dune: {} calls, blocks {}..{}",
                                                dune_calls.len(),
                                                min_b,
                                                max_b
                                            )));
                                            ds_b.save_address_calls(&address, &dune_calls);
                                        }
                                        let _ = tx_b.send(Action::AddressInfoLoaded {
                                            info: crate::data::types::SnAddressInfo {
                                                address,
                                                nonce,
                                                class_hash,
                                                recent_events: Vec::new(),
                                                token_balances: Vec::new(),
                                            },
                                            decoded_events: Vec::new(),
                                            tx_summaries: Vec::new(),
                                            contract_calls: dune_calls,
                                        });
                                    }
                                    _ => {}
                                }
                            }
                        }

                        let _ = tx_b.send(Action::AddressTxsStreamed {
                            address,
                            source: Source::Dune,
                            tx_summaries: Vec::new(),
                            complete: true,
                        });
                    }
                }
            });
        } else {
            // Account: windowed tx fetch
            tokio::spawn(async move {
                let _ = tx_b.send(Action::LoadingStatus(
                    "Dune: fetching recent account txs...".into(),
                ));
                let latest_block = ds_b.get_latest_block_number().await.unwrap_or(0);
                let from = latest_block.saturating_sub(INITIAL_WINDOW);

                let result = dune_c
                    .query_account_txs_windowed(address, from, latest_block, DUNE_PAGE_LIMIT)
                    .await;

                match result {
                    Ok(dune_txs) if !dune_txs.is_empty() => {
                        let _ =
                            tx_b.send(dune_source_update(crate::app::state::SourceStatus::Live));
                        info!(
                            dune_txs = dune_txs.len(),
                            "Dune windowed account txs complete"
                        );
                        let min_b = dune_txs.iter().map(|t| t.block_number).min().unwrap_or(0);
                        let max_b = dune_txs.iter().map(|t| t.block_number).max().unwrap_or(0);
                        let _ = tx_b.send(Action::LoadingStatus(format!(
                            "Dune: {} txs, blocks {}..{}",
                            dune_txs.len(),
                            min_b,
                            max_b
                        )));
                        let _ = tx_b.send(Action::AddressTxsStreamed {
                            address,
                            source: Source::Dune,
                            tx_summaries: dune_txs,
                            complete: true,
                        });
                    }
                    Ok(_) | Err(_) => {
                        if let Err(ref e) = result {
                            warn!(error = %e, "Dune windowed account txs failed");
                        }
                        // Initial window empty — wait for probe, retry with probe-guided window
                        let probe = tokio::time::timeout(
                            tokio::time::Duration::from_secs(10),
                            probe_rx_b.wait_for(|p| p.is_some()),
                        )
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .and_then(|p| p.clone());

                        if let Some(p) = probe {
                            if p.has_activity() {
                                let window = p.recommended_window();
                                let probe_from = p.max_block().saturating_sub(window);
                                let probe_to = p.max_block();
                                let _ = tx_b.send(Action::LoadingStatus(format!(
                                    "Dune: retrying blocks {}..{} (probe-guided)...",
                                    probe_from, probe_to
                                )));
                                match dune_c
                                    .query_account_txs_windowed(
                                        address,
                                        probe_from,
                                        probe_to,
                                        DUNE_PAGE_LIMIT,
                                    )
                                    .await
                                {
                                    Ok(txs) if !txs.is_empty() => {
                                        let _ = tx_b.send(dune_source_update(
                                            crate::app::state::SourceStatus::Live,
                                        ));
                                        let min_b =
                                            txs.iter().map(|t| t.block_number).min().unwrap_or(0);
                                        let max_b =
                                            txs.iter().map(|t| t.block_number).max().unwrap_or(0);
                                        let _ = tx_b.send(Action::LoadingStatus(format!(
                                            "Dune: {} txs, blocks {}..{}",
                                            txs.len(),
                                            min_b,
                                            max_b
                                        )));
                                        let _ = tx_b.send(Action::AddressTxsStreamed {
                                            address,
                                            source: Source::Dune,
                                            tx_summaries: txs,
                                            complete: true,
                                        });
                                    }
                                    _ => {
                                        let _ = tx_b.send(Action::AddressTxsStreamed {
                                            address,
                                            source: Source::Dune,
                                            tx_summaries: Vec::new(),
                                            complete: true,
                                        });
                                    }
                                }
                            } else {
                                let _ = tx_b.send(Action::AddressTxsStreamed {
                                    address,
                                    source: Source::Dune,
                                    tx_summaries: Vec::new(),
                                    complete: true,
                                });
                            }
                        } else {
                            // No probe available — fall back to unwindowed query with tight limit
                            match dune_c.query_account_txs(address, DUNE_PAGE_LIMIT).await {
                                Ok(txs) => {
                                    let _ = tx_b.send(dune_source_update(
                                        crate::app::state::SourceStatus::Live,
                                    ));
                                    let _ = tx_b.send(Action::AddressTxsStreamed {
                                        address,
                                        source: Source::Dune,
                                        tx_summaries: txs,
                                        complete: true,
                                    });
                                }
                                Err(e) => {
                                    warn!(error = %e, "Dune account txs fallback failed");
                                    let _ = tx_b.send(dune_source_update(
                                        crate::app::state::SourceStatus::FetchError(e.to_string()),
                                    ));
                                    let _ = tx_b.send(Action::AddressTxsStreamed {
                                        address,
                                        source: Source::Dune,
                                        tx_summaries: Vec::new(),
                                        complete: true,
                                    });
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    // =====================================================================
    // TASK C: Events — pathfinder-first for contracts, RPC-first for accounts.
    // Probe-guided fallback for sparse addresses; pagination for deeper history.
    // =====================================================================
    {
        let ds_c = Arc::clone(ds);
        let abi_c = Arc::clone(abi_reg);
        let tx_c = tx.clone();
        let voyager_c2 = voyager_c.clone();
        let _pf_ok = Arc::clone(&pf_succeeded);
        let pf_c = pf.as_ref().map(Arc::clone);
        let mut probe_rx_c = probe_watch_rx.clone();

        tokio::spawn(async move {
            let latest_block = ds_c.get_latest_block_number().await.unwrap_or(0);

            // --- Use cached search progress + nonce delta to narrow the window ---
            let search_progress = ds_c.load_search_progress(&address);

            // If nonce hasn't changed and we've already searched up to near the tip,
            // skip the full search — only check the small delta.
            let from_block = if let Some((_min_searched, max_searched)) = search_progress {
                if !is_contract && nonce_delta == 0 && max_searched + 100 >= latest_block {
                    // No new txs — skip TASK C entirely
                    debug!(
                        address = %format!("{:#x}", address),
                        "Nonce unchanged & search progress up-to-date, skipping RPC event scan"
                    );
                    let _ = tx_c.send(Action::AddressTxsStreamed {
                        address,
                        source: Source::Rpc,
                        tx_summaries: Vec::new(),
                        complete: true,
                    });
                    return;
                }
                // Search only from where we left off
                max_searched + 1
            } else if !is_contract && cached_nonce_block > 0 && nonce_delta < 50 {
                // We know the nonce block — start from there
                cached_nonce_block
            } else {
                let initial_window = if is_contract { 10_000u64 } else { 5_000u64 };
                latest_block.saturating_sub(initial_window)
            };

            let window_size = latest_block.saturating_sub(from_block);
            let _ = tx_c.send(Action::LoadingStatus(format!(
                "RPC: scanning {}k blocks for events...",
                (window_size + 999) / 1000
            )));
            let phase1_limit = if is_contract { 500 } else { 100 };

            let kind = if is_contract {
                EventQueryKind::Contract
            } else {
                EventQueryKind::Account
            };
            let events_result = fetch_events_routed(
                kind,
                pf_c.as_ref(),
                &ds_c,
                address,
                Some(from_block),
                None,
                phase1_limit,
            )
            .await;
            let events = match events_result {
                Ok(evts) => evts,
                Err(e) => {
                    warn!(error = %e, address = %format!("{:#x}", address), "RPC event fetch failed");
                    let _ = tx_c.send(Action::LoadingStatus(format!(
                        "RPC: event fetch failed: {}",
                        e
                    )));
                    let _ = tx_c.send(Action::SourceUpdate {
                        source: Source::Rpc,
                        status: crate::app::state::SourceStatus::FetchError(e.to_string()),
                    });
                    Vec::new()
                }
            };

            let events_count = events.len();
            debug!(
                address = %format!("{:#x}", address),
                events = events_count,
                "RPC phase 1 events fetched"
            );

            if events_count > 0 {
                let min_b = events.iter().map(|e| e.block_number).min().unwrap_or(0);
                let max_b = events.iter().map(|e| e.block_number).max().unwrap_or(0);
                let _ = tx_c.send(Action::LoadingStatus(format!(
                    "RPC: found {} events in blocks {}..{}",
                    events_count, min_b, max_b
                )));
            } else {
                let _ = tx_c.send(Action::LoadingStatus(
                    "RPC: no recent events, waiting for probe...".into(),
                ));
            }

            // Extract unique tx hashes
            let mut seen = std::collections::HashSet::new();
            let unique_hashes: Vec<starknet::core::types::Felt> = events
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

            let tx_block_map: std::collections::HashMap<_, _> = events
                .iter()
                .map(|e| (e.transaction_hash, e.block_number))
                .collect();

            // Decode events for the events tab
            let mut decoded_events = Vec::new();
            for event in &events {
                let abi = abi_c.get_abi_for_address(&event.from_address).await;
                decoded_events.push(decode_event(event, abi.as_deref()));
            }

            // Cache discovered range from phase 1 events
            if !events.is_empty() {
                let min_b = events.iter().map(|e| e.block_number).min().unwrap_or(0);
                let max_b = events.iter().map(|e| e.block_number).max().unwrap_or(0);
                if min_b > 0 {
                    ds_c.save_activity_range(&address, min_b, max_b);
                }
            }

            // Stream phase 1 results immediately (contracts: calls, accounts: events)
            if is_contract && !unique_hashes.is_empty() {
                let call_hashes: Vec<_> = unique_hashes
                    .iter()
                    .map(|h| (*h, *tx_block_map.get(h).unwrap_or(&0)))
                    .collect();

                let mut contract_calls_list = build_contract_calls_from_hashes(
                    address,
                    &call_hashes,
                    &ds_c,
                    pf_c.as_ref(),
                    &abi_c,
                )
                .await;
                contract_calls_list.sort_by(|a, b| b.block_number.cmp(&a.block_number));

                {
                    let callers = contract_calls_list.iter().map(|c| c.sender);
                    spawn_voyager_prefetch(callers, &voyager_c2, &tx_c);
                }

                let _ = tx_c.send(Action::AddressInfoLoaded {
                    info: crate::data::types::SnAddressInfo {
                        address,
                        nonce,
                        class_hash,
                        recent_events: events,
                        token_balances: Vec::new(),
                    },
                    decoded_events,
                    tx_summaries: Vec::new(),
                    contract_calls: contract_calls_list,
                });
            } else if !decoded_events.is_empty() {
                let _ = tx_c.send(Action::AddressInfoLoaded {
                    info: crate::data::types::SnAddressInfo {
                        address,
                        nonce,
                        class_hash,
                        recent_events: events.clone(),
                        token_balances: Vec::new(),
                    },
                    decoded_events,
                    tx_summaries: Vec::new(),
                    contract_calls: Vec::new(),
                });
            }

            // For accounts: stream phase 1 tx summaries from event hashes
            if !is_contract {
                let cached_txs = ds_c.load_cached_address_txs(&address);
                let cached_hashes: std::collections::HashSet<_> =
                    cached_txs.iter().map(|t| t.hash).collect();
                let hashes_to_fetch: Vec<_> = unique_hashes
                    .iter()
                    .filter(|h| !cached_hashes.contains(h))
                    .copied()
                    .collect();

                if !hashes_to_fetch.is_empty() {
                    let summaries = fetch_tx_summaries_from_hashes(
                        &hashes_to_fetch,
                        &tx_block_map,
                        &ds_c,
                        pf_c.as_ref(),
                        &abi_c,
                        &tx_c,
                        "RPC: fetching txs",
                    )
                    .await;

                    if !summaries.is_empty() {
                        let mut all_txs = cached_txs;
                        for s in &summaries {
                            if !all_txs.iter().any(|t| t.hash == s.hash) {
                                all_txs.push(s.clone());
                            }
                        }
                        all_txs.sort_by(|a, b| b.nonce.cmp(&a.nonce));
                        ds_c.save_address_txs(&address, &all_txs);

                        let _ = tx_c.send(Action::AddressTxsStreamed {
                            address,
                            source: Source::Rpc,
                            tx_summaries: summaries,
                            complete: false,
                        });
                    }
                }
            }

            // --- Phase 2: If phase 1 found nothing, do ONE probe-guided search ---
            // No more eager progressive window expansion — pagination handles deeper history.
            let phase1_found = !unique_hashes.is_empty();
            let mut any_events_found = phase1_found;
            if !phase1_found {
                let probe_timeout = if cached_range.is_some() { 0 } else { 5 };
                let probe = tokio::time::timeout(
                    tokio::time::Duration::from_secs(probe_timeout),
                    probe_rx_c.wait_for(|p| p.is_some()),
                )
                .await
                .ok()
                .and_then(|r| r.ok())
                .and_then(|p| p.clone());

                if let Some(p) = probe {
                    let has_activity = if is_contract {
                        p.callee_call_count > 0
                    } else {
                        p.sender_tx_count > 0
                    };

                    if has_activity {
                        let window = p.recommended_window();
                        let search_from = p.max_block().saturating_sub(window);
                        let search_to = p.max_block();
                        let _ = tx_c.send(Action::LoadingStatus(format!(
                            "RPC: probe-guided search blocks {}..{}...",
                            search_from, search_to
                        )));

                        let kind = if is_contract {
                            EventQueryKind::Contract
                        } else {
                            EventQueryKind::Account
                        };
                        // `search_to` is inclusive to match the probe range.
                        let deeper_events = fetch_events_routed(
                            kind,
                            pf_c.as_ref(),
                            &ds_c,
                            address,
                            Some(search_from),
                            Some(search_to),
                            500,
                        )
                        .await;

                        if let Ok(deeper_events) = deeper_events {
                            if !deeper_events.is_empty() {
                                any_events_found = true;
                                // Cache discovered range
                                let min_b = deeper_events
                                    .iter()
                                    .map(|e| e.block_number)
                                    .min()
                                    .unwrap_or(0);
                                let max_b = deeper_events
                                    .iter()
                                    .map(|e| e.block_number)
                                    .max()
                                    .unwrap_or(0);
                                if min_b > 0 {
                                    ds_c.save_activity_range(&address, min_b, max_b);
                                }

                                let mut deep_seen = std::collections::HashSet::new();
                                let deep_hashes: Vec<_> = deeper_events
                                    .iter()
                                    .filter_map(|e| {
                                        let h = e.transaction_hash;
                                        if h != starknet::core::types::Felt::ZERO
                                            && deep_seen.insert(h)
                                        {
                                            Some(h)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();

                                let deep_block_map: std::collections::HashMap<_, _> = deeper_events
                                    .iter()
                                    .map(|e| (e.transaction_hash, e.block_number))
                                    .collect();

                                let mut deep_decoded = Vec::new();
                                for event in &deeper_events {
                                    let abi = abi_c.get_abi_for_address(&event.from_address).await;
                                    deep_decoded.push(decode_event(event, abi.as_deref()));
                                }

                                if is_contract && !deep_hashes.is_empty() {
                                    let call_hashes: Vec<_> = deep_hashes
                                        .iter()
                                        .map(|h| (*h, deep_block_map.get(h).copied().unwrap_or(0)))
                                        .collect();
                                    let mut contract_calls_list = build_contract_calls_from_hashes(
                                        address,
                                        &call_hashes,
                                        &ds_c,
                                        pf_c.as_ref(),
                                        &abi_c,
                                    )
                                    .await;
                                    contract_calls_list
                                        .sort_by(|a, b| b.block_number.cmp(&a.block_number));

                                    {
                                        let callers = contract_calls_list.iter().map(|c| c.sender);
                                        spawn_voyager_prefetch(callers, &voyager_c2, &tx_c);
                                    }

                                    let _ = tx_c.send(Action::AddressInfoLoaded {
                                        info: crate::data::types::SnAddressInfo {
                                            address,
                                            nonce,
                                            class_hash,
                                            recent_events: deeper_events,
                                            token_balances: Vec::new(),
                                        },
                                        decoded_events: deep_decoded,
                                        tx_summaries: Vec::new(),
                                        contract_calls: contract_calls_list,
                                    });
                                } else if !is_contract && !deep_hashes.is_empty() {
                                    let to_fetch: Vec<_> = {
                                        let cached_txs = ds_c.load_cached_address_txs(&address);
                                        let cached_set: std::collections::HashSet<_> =
                                            cached_txs.iter().map(|t| t.hash).collect();
                                        deep_hashes
                                            .into_iter()
                                            .filter(|h| !cached_set.contains(h))
                                            .collect()
                                    };
                                    if !to_fetch.is_empty() {
                                        let summaries = fetch_tx_summaries_from_hashes(
                                            &to_fetch,
                                            &deep_block_map,
                                            &ds_c,
                                            pf_c.as_ref(),
                                            &abi_c,
                                            &tx_c,
                                            "RPC: fetching probe-guided txs",
                                        )
                                        .await;
                                        if !summaries.is_empty() {
                                            let _ = tx_c.send(Action::AddressTxsStreamed {
                                                address,
                                                source: Source::Rpc,
                                                tx_summaries: summaries,
                                                complete: false,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        let _ = tx_c.send(Action::LoadingStatus(
                            "No activity found for this address".into(),
                        ));
                    }
                }
            }

            // Save search progress only when events were found — otherwise the initial
            // window may have been too small and we need to retry with probe guidance.
            if any_events_found {
                ds_c.save_search_progress(&address, from_block, latest_block);
            }

            // RPC task complete
            let _ = tx_c.send(Action::AddressTxsStreamed {
                address,
                source: Source::Rpc,
                tx_summaries: Vec::new(),
                complete: true,
            });

            info!(
                address = %format!("{:#x}", address),
                elapsed_ms = start.elapsed().as_millis(),
                "RPC address info task complete"
            );
        });
    }
}

/// Enrich a set of address tx summaries that are missing endpoint/timestamp data.
/// Called lazily after the initial view load for visible transactions.
///
/// Prefers pf-query (`/txs-by-hash`) when available: one round trip to the
/// local Pathfinder DB returns calldata + fee + status + timestamp for every
/// requested hash. This removes enrichment traffic from the shared RPC pool,
/// which was saturating on large accounts and making user-initiated
/// `FetchTransaction` clicks block behind enrichment.
///
/// Falls back to RPC (per-hash `get_transaction` + `get_receipt`) for any
/// hashes pf doesn't return, guarded by `ENRICH_RPC_SEMAPHORE` so that even
/// RPC-only users never have more than a handful of concurrent enrichment
/// requests competing with a click.
pub(super) async fn enrich_address_txs(
    address: starknet::core::types::Felt,
    hashes: Vec<starknet::core::types::Felt>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    if hashes.is_empty() {
        return;
    }
    debug!(
        count = hashes.len(),
        "Enriching address txs (endpoints/timestamps)"
    );

    let mut updates: Vec<crate::data::types::AddressTxSummary> = Vec::new();
    let mut missing: Vec<starknet::core::types::Felt> = hashes.clone();

    // --- Fast path: pf-query ------------------------------------------------
    if let Some(pf) = pf {
        match pf.get_txs_by_hash(&hashes).await {
            Ok(pf_rows) => {
                debug!(
                    requested = hashes.len(),
                    returned = pf_rows.len(),
                    "enrich_address_txs: pf-query returned"
                );

                // Pre-warm the ABI registry for every target contract seen in
                // the multicall calldata so `build_tx_summary_from_pf_data`
                // can resolve selector names.
                let mut target_addresses: std::collections::HashSet<starknet::core::types::Felt> =
                    std::collections::HashSet::new();
                for row in &pf_rows {
                    for addr in helpers::pf_tx_target_addresses(row) {
                        target_addresses.insert(addr);
                    }
                }
                for addr in target_addresses {
                    let _ = abi_reg.get_abi_for_address(&addr).await;
                }

                let mut returned_hashes: std::collections::HashSet<starknet::core::types::Felt> =
                    std::collections::HashSet::new();
                for row in &pf_rows {
                    if let Some(summary) = helpers::build_tx_summary_from_pf_data(row, abi_reg) {
                        returned_hashes.insert(summary.hash);
                        updates.push(summary);
                    }
                }

                missing.retain(|h| !returned_hashes.contains(h));
            }
            Err(e) => {
                warn!(error = %e, "enrich_address_txs: pf-query failed, falling back to RPC for all hashes");
            }
        }
    }

    // --- Fallback path: RPC (bounded concurrency) --------------------------
    if !missing.is_empty() {
        let futs: Vec<_> = missing
            .iter()
            .map(|hash| {
                let ds_t = Arc::clone(ds);
                let ds_r = Arc::clone(ds);
                let h = *hash;
                async move {
                    // One permit per tx — keeps enrichment from saturating the
                    // HTTP pool while a user click is in flight.
                    let _permit = ENRICH_RPC_SEMAPHORE.acquire().await.ok();
                    let (tx_r, rx_r) = tokio::join!(ds_t.get_transaction(h), ds_r.get_receipt(h));
                    (h, tx_r, rx_r)
                }
            })
            .collect();
        let results = futures::future::join_all(futs).await;

        // Pre-fetch ABIs for every contract address referenced in multicall
        // calldata (same warmup path as the pf branch).
        let mut target_addresses = std::collections::HashSet::new();
        for (_hash, tx_result, _rx_result) in &results {
            if let Ok(SnTransaction::Invoke(invoke)) = tx_result {
                for call in parse_multicall(&invoke.calldata) {
                    target_addresses.insert(call.contract_address);
                }
            }
        }
        for addr in target_addresses {
            let _ = abi_reg.get_abi_for_address(&addr).await;
        }

        for (hash, tx_result, rx_result) in results {
            if let Ok(fetched_tx) = tx_result {
                let receipt = rx_result.ok();
                let block_num = receipt.as_ref().map(|r| r.block_number).unwrap_or(0);
                updates.push(helpers::build_tx_summary(
                    hash,
                    &fetched_tx,
                    receipt.as_ref(),
                    block_num,
                    0,
                    abi_reg,
                ));
            }
        }
    }

    helpers::backfill_timestamps(&mut updates, ds, pf).await;

    if !updates.is_empty() {
        let _ =
            action_tx.send(crate::app::actions::Action::AddressTxsEnriched { address, updates });
    }
}

/// Post-display enrichment: fill only *small* nonce gaps and enrich all txs
/// with missing endpoint names.
///
/// Large gaps (the pathological case from issue #10) are left untouched here;
/// they are deferred to `run_nonce_gap_fill` which fires only when the user
/// scrolls toward the gap.
pub(super) async fn run_endpoint_enrichment(
    address: starknet::core::types::Felt,
    current_nonce: u64,
    known_txs: Vec<crate::data::types::AddressTxSummary>,
    ds: &Arc<dyn DataSource>,
    dune: &Option<Arc<dune::DuneClient>>,
    pf: &Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    if known_txs.is_empty() || current_nonce == 0 {
        info!(
            address = %format!("{:#x}", address),
            txs = known_txs.len(),
            current_nonce,
            "Endpoint enrich: skipping nonce gaps (empty txs or nonce=0), running endpoint enrichment only"
        );
        enrich_all_empty_endpoints(address, &known_txs, ds, pf.as_ref(), abi_reg, action_tx).await;
        return;
    }

    let min_nonce = known_txs.iter().map(|t| t.nonce).min().unwrap_or(0);
    let max_nonce = known_txs.iter().map(|t| t.nonce).max().unwrap_or(0);
    let empty_endpoints = known_txs
        .iter()
        .filter(|t| t.endpoint_names.is_empty())
        .count();
    info!(
        address = %format!("{:#x}", address),
        txs = known_txs.len(),
        current_nonce,
        min_nonce,
        max_nonce,
        empty_endpoints,
        "Endpoint enrich: starting (nonce range {}..{}, {} txs, {} missing endpoints)",
        min_nonce, max_nonce, known_txs.len(), empty_endpoints
    );

    // --- Phase 1: Fill only small nonce gaps ---
    // Large gaps are left for on-demand fill via `run_nonce_gap_fill`.
    let gap_txs = fill_small_nonce_gaps_phase(
        address,
        current_nonce,
        &known_txs,
        ds,
        dune,
        abi_reg,
        action_tx,
    )
    .await;

    if !gap_txs.is_empty() {
        let gap_nonces: Vec<u64> = gap_txs.iter().map(|t| t.nonce).collect();
        info!(
            found = gap_txs.len(),
            nonces = ?gap_nonces,
            "Endpoint enrich: filled {} small nonce gaps, sending to UI",
            gap_txs.len()
        );
        let _ = action_tx.send(Action::AddressTxsEnriched {
            address,
            updates: gap_txs.clone(),
        });
    } else {
        info!("Endpoint enrich: no small nonce gaps to fill");
    }

    // --- Phase 2: Enrich all txs with missing endpoint names ---
    let mut all_txs = known_txs;
    for gt in &gap_txs {
        if !all_txs.iter().any(|t| t.hash == gt.hash) {
            all_txs.push(gt.clone());
        }
    }
    enrich_all_empty_endpoints(address, &all_txs, ds, pf.as_ref(), abi_reg, action_tx).await;

    debug!(
        address = %format!("{:#x}", address),
        "Endpoint enrich complete"
    );
}

/// On-demand fill of a single large nonce gap (issue #10).
///
/// Queries Dune for the gap's block range (if available) to locate missing
/// txs, then enriches missing endpoints on the returned set. Progressive
/// results flow through `AddressTxsEnriched` like the auto path.
pub(super) async fn run_nonce_gap_fill(
    address: starknet::core::types::Felt,
    known_txs: Vec<crate::data::types::AddressTxSummary>,
    gap: crate::app::views::address_info::UnfilledGap,
    ds: &Arc<dyn DataSource>,
    dune: &Option<Arc<dune::DuneClient>>,
    pf: &Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    info!(
        address = %format!("{:#x}", address),
        lo_nonce = gap.lo_nonce,
        hi_nonce = gap.hi_nonce,
        lo_block = gap.lo_block,
        hi_block = gap.hi_block,
        missing = gap.missing_count,
        "Gap fill: filling large nonce gap on demand"
    );

    let _ = action_tx.send(Action::LoadingStatus(format!(
        "Filling gap of {} txs...",
        gap.missing_count
    )));

    let found = fill_specific_large_gap(address, &known_txs, &gap, dune, action_tx).await;

    if !found.is_empty() {
        info!(
            found = found.len(),
            "Gap fill: Dune returned {} new txs, enriching endpoints",
            found.len()
        );
        let _ = action_tx.send(Action::AddressTxsEnriched {
            address,
            updates: found.clone(),
        });

        // Enrich endpoints for the newly discovered txs (they usually arrive from
        // Dune without endpoint names decoded).
        let mut combined = known_txs;
        for t in &found {
            if !combined.iter().any(|k| k.hash == t.hash) {
                combined.push(t.clone());
            }
        }
        enrich_all_empty_endpoints(address, &combined, ds, pf.as_ref(), abi_reg, action_tx).await;
    } else {
        info!("Gap fill: no txs returned from Dune for this range");
        let _ = action_tx.send(Action::LoadingStatus(String::new()));
    }

    debug!(
        address = %format!("{:#x}", address),
        "Gap fill complete"
    );
}

/// Fill only the *small* nonce gaps (≤50 blocks each) via RPC block scans.
///
/// Large gaps are skipped here and deferred to on-demand fill via
/// `run_nonce_gap_fill` (issue #10). This function retains the same gap
/// classification the original code used; the only behavior change is the
/// removal of the Dune-driven large-gap path.
async fn fill_small_nonce_gaps_phase(
    address: starknet::core::types::Felt,
    current_nonce: u64,
    known_txs: &[crate::data::types::AddressTxSummary],
    ds: &Arc<dyn DataSource>,
    _dune: &Option<Arc<dune::DuneClient>>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) -> Vec<crate::data::types::AddressTxSummary> {
    // Build a set of known nonces and their block numbers
    let known_nonces: std::collections::HashMap<u64, u64> = known_txs
        .iter()
        .filter(|t| t.block_number > 0)
        .map(|t| (t.nonce, t.block_number))
        .collect();

    let min_known = known_txs.iter().map(|t| t.nonce).min().unwrap_or(0);
    let max_known = known_txs.iter().map(|t| t.nonce).max().unwrap_or(0);
    // Check up to current_nonce but cap how far past max_known we look
    let check_up_to = current_nonce.min(max_known + 20);

    info!(
        min_known,
        max_known,
        current_nonce,
        check_up_to,
        known_nonce_count = known_nonces.len(),
        "Sanity gap check: scanning nonces {}..{} (current_nonce={}, known={})",
        min_known,
        check_up_to,
        current_nonce,
        known_nonces.len()
    );

    // Only small-span gaps are handled here; wider gaps are picked up by
    // `detect_unfilled_gap` and filled on-demand via `run_nonce_gap_fill`.
    // Both paths share `SMALL_GAP_SPAN_BLOCKS` so they stay aligned.
    use crate::app::views::address_info::SMALL_GAP_SPAN_BLOCKS;
    let mut small_gaps: Vec<(u64, u64, u64)> = Vec::new(); // (nonce, from_block, to_block)

    for nonce in min_known..check_up_to {
        if known_nonces.contains_key(&nonce) {
            continue;
        }
        let block_before = known_nonces
            .iter()
            .filter(|(n, _)| **n < nonce)
            .max_by_key(|(n, _)| *n)
            .map(|(_, b)| *b)
            .unwrap_or(0);
        let block_after = known_nonces
            .iter()
            .filter(|(n, _)| **n > nonce)
            .min_by_key(|(n, _)| *n)
            .map(|(_, b)| *b)
            .unwrap_or(0);

        if block_before == 0 && block_after == 0 {
            continue;
        }

        let scan_from = if block_before > 0 {
            block_before
        } else {
            block_after.saturating_sub(10)
        };
        let scan_to = if block_after > 0 {
            block_after
        } else {
            block_before + 10
        };

        if scan_to.saturating_sub(scan_from) <= SMALL_GAP_SPAN_BLOCKS {
            small_gaps.push((nonce, scan_from, scan_to));
        }
        // else: caught by detect_unfilled_gap → run_nonce_gap_fill.
    }

    let small_count = small_gaps.len();
    if small_count == 0 {
        info!(
            "Sanity gap check: no small nonce gaps in range {}..{}",
            min_known, check_up_to
        );
        return Vec::new();
    }

    let small_nonces: Vec<u64> = small_gaps.iter().map(|(n, _, _)| *n).take(10).collect();
    info!(
        small = small_count,
        first_small_nonces = ?small_nonces,
        "Sanity check: filling {} small nonce gaps (wider gaps deferred to on-demand fill)",
        small_count
    );

    let _ = action_tx.send(Action::LoadingStatus(format!(
        "Filling {} small nonce gaps...",
        small_count
    )));

    let mut found_txs = Vec::new();

    // Small gaps only: RPC block scan. Large gaps are left for on-demand fill.
    if !small_gaps.is_empty() {
        let mut blocks_to_scan: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for (_, from, to) in &small_gaps {
            for b in *from..=*to {
                blocks_to_scan.insert(b);
            }
        }
        // Cap RPC block scan to 200 blocks
        if blocks_to_scan.len() <= 200 {
            let blocks_vec: Vec<u64> = blocks_to_scan.into_iter().collect();
            info!(
                blocks = blocks_vec.len(),
                "Sanity gap-fill: scanning {} blocks via RPC for small gaps",
                blocks_vec.len()
            );
            let rpc_found =
                fetch_txs_from_blocks(address, &blocks_vec, known_txs, ds, abi_reg, action_tx)
                    .await;
            info!(
                found = rpc_found.len(),
                "Sanity gap-fill: RPC scan found {} txs",
                rpc_found.len()
            );
            found_txs.extend(rpc_found);
        } else {
            info!(
                blocks = blocks_to_scan.len(),
                "Sanity gap-fill: too many small-gap blocks ({}), skipping RPC scan",
                blocks_to_scan.len()
            );
        }
    }

    found_txs
}

/// On-demand Dune query for a single large nonce gap (issue #10).
///
/// Caller supplies the known lo/hi block bounds of the gap; we query Dune's
/// windowed account-tx endpoint for that range, deduplicate against known
/// hashes, and return the new entries.
async fn fill_specific_large_gap(
    address: starknet::core::types::Felt,
    known_txs: &[crate::data::types::AddressTxSummary],
    gap: &crate::app::views::address_info::UnfilledGap,
    dune: &Option<Arc<dune::DuneClient>>,
    action_tx: &mpsc::UnboundedSender<Action>,
) -> Vec<crate::data::types::AddressTxSummary> {
    let Some(dune_c) = dune.as_ref() else {
        warn!(
            "Gap fill: Dune client unavailable, cannot fill gap {}..{}",
            gap.lo_block, gap.hi_block
        );
        let _ = action_tx.send(Action::LoadingStatus(
            "Gap fill unavailable (Dune not configured)".to_string(),
        ));
        return Vec::new();
    };

    let from = gap.lo_block;
    let to = gap.hi_block;
    info!(
        from,
        to,
        span = to.saturating_sub(from),
        "Gap fill: querying Dune for blocks {}..{}",
        from,
        to
    );

    let known_hashes: std::collections::HashSet<_> = known_txs.iter().map(|t| t.hash).collect();

    match dune_c
        .query_account_txs_windowed(address, from, to, 1000)
        .await
    {
        Ok(dune_txs) => {
            let total_returned = dune_txs.len();
            let new: Vec<_> = dune_txs
                .into_iter()
                .filter(|t| !known_hashes.contains(&t.hash))
                .collect();
            info!(
                returned = total_returned,
                new = new.len(),
                from,
                to,
                "Gap fill: Dune returned {} txs, {} new for blocks {}..{}",
                total_returned,
                new.len(),
                from,
                to
            );
            new
        }
        Err(e) => {
            warn!(error = %e, from, to, "Gap fill: Dune query failed for blocks {}..{}", from, to);
            Vec::new()
        }
    }
}

/// Phase 2 of sanity check: enrich ALL txs that have empty endpoint names.
/// Batches in chunks of 20 and sends progressive updates.
async fn enrich_all_empty_endpoints(
    address: starknet::core::types::Felt,
    all_txs: &[crate::data::types::AddressTxSummary],
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    action_tx: &mpsc::UnboundedSender<Action>,
) {
    let total_invoke = all_txs.iter().filter(|t| t.tx_type == "INVOKE").count();
    let missing: Vec<starknet::core::types::Felt> = all_txs
        .iter()
        .filter(|t| t.endpoint_names.is_empty() && t.tx_type == "INVOKE")
        .map(|t| t.hash)
        .collect();

    if missing.is_empty() {
        info!(
            total_invoke,
            "Sanity check endpoints: all {} INVOKE txs already have endpoints", total_invoke
        );
        return;
    }

    info!(
        missing = missing.len(),
        total_invoke,
        "Sanity check endpoints: {} of {} INVOKE txs missing endpoints, enriching in batches",
        missing.len(),
        total_invoke
    );

    // Process in batches of 20
    for (i, chunk) in missing.chunks(20).enumerate() {
        info!(
            batch = i + 1,
            size = chunk.len(),
            "Sanity check endpoints: enriching batch {}/{}",
            i + 1,
            (missing.len() + 19) / 20
        );
        enrich_address_txs(address, chunk.to_vec(), ds, pf, abi_reg, action_tx).await;
    }
}

/// Fetch all txs sent by `address` from specific blocks, skipping any already in `known_txs`.
/// Fetches receipts for fee/status and decodes endpoint names from calldata.
/// Shared by `fill_nonce_gaps` (range-scan) and the Pathfinder nonce-history path (exact blocks).
pub(super) async fn fetch_txs_from_blocks(
    address: starknet::core::types::Felt,
    blocks: &[u64],
    known_txs: &[crate::data::types::AddressTxSummary],
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    status_tx: &mpsc::UnboundedSender<Action>,
) -> Vec<crate::data::types::AddressTxSummary> {
    let mut found_txs: Vec<crate::data::types::AddressTxSummary> = Vec::new();
    let total_chunks = blocks.chunks(10).count();

    for (chunk_idx, chunk) in blocks.chunks(10).enumerate() {
        if total_chunks > 1 {
            let fetched = chunk_idx * 10 + 1;
            let _ = status_tx.send(Action::LoadingStatus(format!(
                "Fetching blocks from RPC {}/{}...",
                fetched.min(blocks.len()),
                blocks.len()
            )));
        }
        let futs: Vec<_> = chunk
            .iter()
            .map(|bn| {
                let ds_b = Arc::clone(ds);
                let b = *bn;
                async move { (b, ds_b.get_block_with_txs(b).await) }
            })
            .collect();
        let results = futures::future::join_all(futs).await;

        for (block_num, result) in results {
            if let Ok((block, txs)) = result {
                for btx in txs.iter() {
                    if btx.sender() != address {
                        continue;
                    }
                    if known_txs.iter().any(|t| t.hash == btx.hash()) {
                        continue;
                    }

                    let receipt = ds.get_receipt(btx.hash()).await.ok();
                    found_txs.push(helpers::build_tx_summary(
                        btx.hash(),
                        btx,
                        receipt.as_ref(),
                        block_num,
                        block.timestamp,
                        abi_reg,
                    ));
                }
            }
        }
    }

    found_txs
}

/// Fetch more address transactions from before a given block (pagination).
pub(super) async fn fetch_more_address_txs(
    address: starknet::core::types::Felt,
    before_block: u64,
    is_contract: bool,
    ds: &Arc<dyn crate::data::DataSource>,
    dune: &Option<Arc<dune::DuneClient>>,
    pf: &Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    abi_reg: &Arc<AbiRegistry>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    use crate::data::types::ContractCallSummary;

    let start = std::time::Instant::now();
    debug!(address = %format!("{:#x}", address), before_block, is_contract, "Fetching more address txs");

    // Use cached activity range for density-aware window sizing
    let cached = ds.load_cached_activity_range(&address);
    let window_size = if let Some((min_b, max_b)) = cached {
        // Build a lightweight probe to compute recommended_window
        let mut p = dune::AddressActivityProbe::default();
        p.sender_min_block = min_b;
        p.sender_max_block = max_b;
        p.callee_min_block = min_b;
        p.callee_max_block = max_b;
        p.sender_tx_count = 1;
        p.callee_call_count = 1;
        p.recommended_window()
    } else {
        50_000u64
    };

    // Don't fetch before the deploy block — no txs can exist before contract creation
    let deploy_block = ds
        .load_cached_deploy_info(&address)
        .map(|(_, block, _)| block);
    if let Some(db) = deploy_block {
        if before_block <= db {
            debug!(address = %format!("{:#x}", address), deploy_block = db, "Already at deploy block, no more txs to fetch");
            let _ = tx.send(Action::MoreAddressTxsLoaded {
                address,
                tx_summaries: Vec::new(),
                contract_calls: Vec::new(),
                oldest_block: db,
                has_more: false,
            });
            return;
        }
    }

    let mut from_block = before_block.saturating_sub(window_size);
    // Clamp to deploy block — no point scanning before contract existed
    if let Some(db) = deploy_block {
        from_block = from_block.max(db);
    }
    if from_block == 0 && before_block <= 1 {
        let _ = tx.send(Action::MoreAddressTxsLoaded {
            address,
            tx_summaries: Vec::new(),
            contract_calls: Vec::new(),
            oldest_block: 0,
            has_more: false,
        });
        return;
    }

    let _ = tx.send(Action::LoadingStatus(format!(
        "Loading more: blocks {}..{}...",
        from_block,
        before_block.saturating_sub(1)
    )));

    // Fire events-source + Dune in parallel.
    //
    // Events source routing (matches initial fetch):
    //  - Contracts: pathfinder primary (bloom-indexed, fast for dense contracts),
    //    RPC fallback on PF error. Used to be skipped entirely because RPC was
    //    too slow for dense contracts at scroll depth — PF resolves that.
    //  - Accounts: RPC primary (narrow window + key filter is fast), PF fallback.
    let rpc_ds = Arc::clone(ds);
    let pf_c = pf.as_ref().map(Arc::clone);
    let rpc_addr = address;
    let rpc_is_contract = is_contract;
    // Window size per source:
    //  - Contracts use pathfinder (bloom-indexed), which can scan the full
    //    `window_size` cheaply.
    //  - Accounts use RPC which is slow on wide ranges, so clamp to 10k.
    let (events_from, events_to) = if rpc_is_contract {
        (
            before_block.saturating_sub(window_size),
            before_block.saturating_sub(1),
        )
    } else {
        (
            before_block.saturating_sub(window_size.min(10_000)),
            before_block.saturating_sub(1),
        )
    };

    let rpc_fut = async move {
        let kind = if rpc_is_contract {
            EventQueryKind::Contract
        } else {
            EventQueryKind::Account
        };
        // `to_block = before_block - 1` keeps the scan below the already-known
        // range, so we don't refetch newer events only to filter them out.
        fetch_events_routed(
            kind,
            pf_c.as_ref(),
            &rpc_ds,
            rpc_addr,
            Some(events_from),
            Some(events_to),
            500,
        )
        .await
        .unwrap_or_default()
    };

    let dune_c = dune.as_ref().map(Arc::clone);
    let dune_fut = async move {
        let Some(dune_client) = dune_c else {
            return (Vec::new(), Vec::new());
        };
        let dune_to = before_block.saturating_sub(1);
        if is_contract {
            match dune_client
                .query_contract_calls_windowed(address, from_block, dune_to, 100)
                .await
            {
                Ok(calls) => (Vec::new(), calls),
                Err(e) => {
                    warn!(error = %e, "Dune pagination contract calls failed");
                    (Vec::new(), Vec::new())
                }
            }
        } else {
            match dune_client
                .query_account_txs_windowed(address, from_block, dune_to, 100)
                .await
            {
                Ok(txs) => (txs, Vec::new()),
                Err(e) => {
                    warn!(error = %e, "Dune pagination account txs failed");
                    (Vec::new(), Vec::new())
                }
            }
        }
    };

    let (events, (dune_txs, dune_calls)) = tokio::join!(rpc_fut, dune_fut);

    // Build tx summaries from RPC events
    let mut seen = std::collections::HashSet::new();
    let unique_hashes: Vec<starknet::core::types::Felt> = events
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

    let tx_block_map: std::collections::HashMap<_, _> = events
        .iter()
        .map(|e| (e.transaction_hash, e.block_number))
        .collect();

    let mut summaries = Vec::new();
    // Build contract calls from RPC events if contract
    let mut rpc_calls: Vec<ContractCallSummary> = Vec::new();
    if is_contract && !unique_hashes.is_empty() {
        let call_hashes: Vec<_> = unique_hashes
            .iter()
            .map(|h| (*h, *tx_block_map.get(h).unwrap_or(&0)))
            .collect();
        rpc_calls =
            build_contract_calls_from_hashes(address, &call_hashes, ds, pf.as_ref(), abi_reg).await;
    } else if !unique_hashes.is_empty() {
        // Build tx summaries for accounts
        for chunk in unique_hashes.chunks(20) {
            let futs: Vec<_> = chunk
                .iter()
                .map(|hash| {
                    let ds_tx = Arc::clone(ds);
                    let ds_rx = Arc::clone(ds);
                    let h = *hash;
                    async move {
                        let (tx_r, rx_r) =
                            tokio::join!(ds_tx.get_transaction(h), ds_rx.get_receipt(h));
                        (h, tx_r, rx_r)
                    }
                })
                .collect();
            let results = futures::future::join_all(futs).await;

            for (hash, tx_result, rx_result) in results {
                if let Ok(fetched_tx) = tx_result {
                    let receipt = rx_result.ok();
                    let block_num = receipt
                        .as_ref()
                        .map(|r| r.block_number)
                        .unwrap_or_else(|| tx_block_map.get(&hash).copied().unwrap_or(0));
                    summaries.push(helpers::build_tx_summary(
                        hash,
                        &fetched_tx,
                        receipt.as_ref(),
                        block_num,
                        0,
                        abi_reg,
                    ));
                }
            }
        }
        helpers::backfill_timestamps(&mut summaries, ds, pf.as_ref()).await;
    }

    // Merge Dune account txs into summaries (dedup by hash)
    if !dune_txs.is_empty() {
        let existing: std::collections::HashSet<_> = summaries.iter().map(|s| s.hash).collect();
        for dtx in dune_txs {
            if !existing.contains(&dtx.hash) {
                summaries.push(dtx);
            }
        }
    }

    summaries.sort_by(|a, b| b.nonce.cmp(&a.nonce));

    // Enrich + deduplicate Dune contract calls (same as initial fetch)
    let dune_calls = if !dune_calls.is_empty() {
        enrich_dune_calls(address, dune_calls, abi_reg, ds, pf.as_ref(), tx).await
    } else {
        Vec::new()
    };

    // Merge Dune contract calls with RPC calls (dedup by tx_hash)
    let mut all_calls = rpc_calls;
    if !dune_calls.is_empty() {
        let existing: std::collections::HashSet<_> = all_calls.iter().map(|c| c.tx_hash).collect();
        for dc in dune_calls {
            if !existing.contains(&dc.tx_hash) {
                all_calls.push(dc);
            }
        }
    }
    all_calls.sort_by(|a, b| b.block_number.cmp(&a.block_number));

    // Determine oldest block and whether there's likely more data
    let oldest_block = summaries
        .iter()
        .filter(|s| s.block_number > 0)
        .map(|s| s.block_number)
        .chain(all_calls.iter().map(|c| c.block_number))
        .min()
        .unwrap_or(from_block);

    let at_deploy_floor = deploy_block.is_some_and(|db| from_block <= db);
    let has_more = from_block > 0
        && !at_deploy_floor
        && (summaries.len() >= 50
            || all_calls.len() >= 50
            || cached.is_some_and(|(min_b, _)| min_b < from_block));

    info!(
        address = %format!("{:#x}", address),
        new_txs = summaries.len(),
        new_calls = all_calls.len(),
        oldest_block,
        has_more,
        elapsed_ms = start.elapsed().as_millis(),
        "More address txs fetched"
    );

    // Save to cache
    if !summaries.is_empty() {
        ds.save_address_txs(&address, &summaries);
    }
    if !all_calls.is_empty() {
        ds.save_address_calls(&address, &all_calls);
    }

    let _ = tx.send(Action::MoreAddressTxsLoaded {
        address,
        tx_summaries: summaries,
        contract_calls: all_calls,
        oldest_block,
        has_more,
    });
}

/// Resolve selectors, dedup, backfill fees + timestamps on a batch of Dune contract calls.
/// Returns the enriched, sorted calls.
pub(super) async fn enrich_dune_calls(
    address: starknet::core::types::Felt,
    mut dune_calls: Vec<crate::data::types::ContractCallSummary>,
    abi_reg: &Arc<AbiRegistry>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<crate::data::pathfinder::PathfinderClient>>,
    tx: &mpsc::UnboundedSender<Action>,
) -> Vec<crate::data::types::ContractCallSummary> {
    // Resolve selectors
    let mut unresolved = false;
    for call in &mut dune_calls {
        if call.function_name.starts_with("0x") {
            if let Ok(sel) = starknet::core::types::Felt::from_hex(&call.function_name) {
                if let Some(name) = abi_reg.get_selector_name(&sel) {
                    call.function_name = name;
                } else {
                    unresolved = true;
                }
            }
        }
    }
    if unresolved {
        if let Some(abi) = abi_reg.get_abi_for_address(&address).await {
            for call in &mut dune_calls {
                if call.function_name.starts_with("0x") {
                    if let Ok(sel) = starknet::core::types::Felt::from_hex(&call.function_name) {
                        if let Some(func) = abi.get_function(&sel) {
                            call.function_name = func.name.clone();
                        }
                    }
                }
            }
        }
    }
    dune_calls = crate::data::types::deduplicate_contract_calls(dune_calls);

    // Batch-fetch fees + real senders from RPC
    let _ = tx.send(Action::LoadingStatus(format!(
        "Fetching fees for {} calls...",
        dune_calls.len()
    )));
    for chunk in dune_calls.chunks_mut(20) {
        let futs: Vec<_> = chunk
            .iter()
            .map(|c| {
                let ds_t = Arc::clone(ds);
                let ds_r = Arc::clone(ds);
                let h = c.tx_hash;
                async move {
                    let (tx_r, rx_r) = tokio::join!(ds_t.get_transaction(h), ds_r.get_receipt(h));
                    (h, tx_r, rx_r)
                }
            })
            .collect();
        let results = futures::future::join_all(futs).await;
        for (hash, tx_result, rx_result) in results {
            if let Some(call) = chunk.iter_mut().find(|c| c.tx_hash == hash) {
                if let Ok(ref fetched_tx) = tx_result {
                    call.sender = fetched_tx.sender();
                }
                if let Ok(receipt) = rx_result {
                    call.total_fee_fri = felt_to_u128(&receipt.actual_fee);
                    call.status = match &receipt.execution_status {
                        crate::data::types::ExecutionStatus::Succeeded => "OK".into(),
                        crate::data::types::ExecutionStatus::Reverted(_) => "REV".into(),
                        _ => call.status.clone(),
                    };
                }
            }
        }
    }

    helpers::backfill_call_timestamps(&mut dune_calls, ds, pf).await;

    dune_calls.sort_by(|a, b| b.block_number.cmp(&a.block_number));
    dune_calls
}

/// Find the deploy tx for an address in a given block.
///
/// Checks for DEPLOY_ACCOUNT / DEPLOY tx types first, then falls back to
/// scanning receipts for the UDC `ContractDeployed` event.
pub(super) async fn find_deploy_tx(
    addr: starknet::core::types::Felt,
    deploy_block: u64,
    ds: &Arc<dyn DataSource>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    // Check cache first — deploy tx is immutable
    if let Some((cached_hash, cached_block, cached_deployer)) = ds.load_cached_deploy_info(&addr) {
        debug!(%addr, cached_block, "Deploy tx found in cache");
        let summary = crate::data::types::AddressTxSummary {
            hash: cached_hash,
            nonce: 0,
            block_number: cached_block,
            timestamp: 0,
            endpoint_names: String::new(),
            total_fee_fri: 0,
            tip: 0,
            tx_type: "DEPLOY".into(),
            status: "OK".into(),
            sender: cached_deployer,
        };
        let _ = tx.send(Action::AddressTxsStreamed {
            address: addr,
            source: Source::Pathfinder,
            tx_summaries: vec![summary],
            complete: false,
        });
        return;
    }

    info!(%addr, deploy_block, "Looking for deploy tx");
    let txs = match ds.get_block_with_txs(deploy_block).await {
        Ok((_block, txs)) => txs,
        Err(e) => {
            warn!(error = %e, "Failed to fetch block for deploy tx lookup");
            return;
        }
    };
    info!(tx_count = txs.len(), "Block fetched for deploy tx lookup");

    // Step 1: Check for native DEPLOY_ACCOUNT / DEPLOY tx types
    for t in &txs {
        let (is_deploy, sender) = match t {
            SnTransaction::DeployAccount(da) if da.contract_address == addr => {
                (true, Some(da.contract_address))
            }
            SnTransaction::Deploy(d) if d.contract_address == addr => {
                (true, Some(d.contract_address))
            }
            _ => (false, None),
        };
        if is_deploy {
            ds.save_deploy_info(&addr, &t.hash(), deploy_block, sender.as_ref());
            let summary = crate::data::types::AddressTxSummary {
                hash: t.hash(),
                nonce: 0,
                block_number: deploy_block,
                timestamp: 0,
                endpoint_names: String::new(),
                total_fee_fri: t.actual_fee().map(|f| felt_to_u128(&f)).unwrap_or(0),
                tip: 0,
                tx_type: t.type_name().to_string(),
                status: "OK".into(),
                sender,
            };
            let _ = tx.send(Action::AddressTxsStreamed {
                address: addr,
                source: Source::Pathfinder,
                tx_summaries: vec![summary],
                complete: false,
            });
            return;
        }
    }

    // Step 2: Scan receipts for UDC ContractDeployed event
    // data[0] = deployed_address, data[1] = deployer
    let udc_selector = starknet::core::types::Felt::from_hex(UDC_CONTRACT_DEPLOYED).unwrap();
    let invoke_count = txs
        .iter()
        .filter(|t| matches!(t, SnTransaction::Invoke(_)))
        .count();
    info!(invoke_count, "Scanning INVOKE txs for UDC event");
    for t in &txs {
        // Only check INVOKE txs (UDC deploys are invocations)
        if !matches!(t, SnTransaction::Invoke(_)) {
            continue;
        }
        let receipt = match ds.get_receipt(t.hash()).await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, tx_hash = %t.hash(), "Failed to fetch receipt");
                continue;
            }
        };
        for event in &receipt.events {
            if event.keys.first() == Some(&udc_selector) {
                let deployed = event.data.first().copied().unwrap_or_default();
                info!(
                    tx_hash = %t.hash(),
                    deployed_addr = %format!("{:#x}", deployed),
                    looking_for = %format!("{:#x}", addr),
                    match_result = (deployed == addr),
                    "Found UDC ContractDeployed event"
                );
            }
            if event.keys.first() == Some(&udc_selector) && event.data.first() == Some(&addr) {
                let deployer = event.data.get(1).copied();
                ds.save_deploy_info(&addr, &t.hash(), deploy_block, deployer.as_ref());
                let summary = crate::data::types::AddressTxSummary {
                    hash: t.hash(),
                    nonce: 0,
                    block_number: deploy_block,
                    timestamp: 0,
                    endpoint_names: String::new(),
                    total_fee_fri: felt_to_u128(&receipt.actual_fee),
                    tip: 0,
                    tx_type: "DEPLOY (UDC)".into(),
                    status: "OK".into(),
                    sender: deployer,
                };
                let _ = tx.send(Action::AddressTxsStreamed {
                    address: addr,
                    source: Source::Pathfinder,
                    tx_summaries: vec![summary],
                    complete: false,
                });
                return;
            }
        }
    }

    debug!(%addr, deploy_block, "Deploy tx not found in block (neither native nor UDC)");
}
