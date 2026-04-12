//! Address-related network functions: fetching address info, enriching txs,
//! filling nonce gaps, fetching token balances, and deploy tx lookups.

use std::sync::Arc;

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

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

    helpers::backfill_timestamps(&mut summaries, ds).await;
    summaries
}

/// Build ContractCallSummary entries from event tx hashes by fetching tx+receipt
/// and extracting calls to the target contract. Also backfills timestamps.
pub(super) async fn build_contract_calls_from_hashes(
    address: starknet::core::types::Felt,
    hashes_with_blocks: &[(starknet::core::types::Felt, u64)],
    ds: &Arc<dyn DataSource>,
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

    helpers::backfill_call_timestamps(&mut calls_list, ds).await;
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
                            enrich_dune_calls(address, calls, &abi_b, &ds_b, &tx_b).await;

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
                                        let dune_calls =
                                            enrich_dune_calls(address, calls, &abi_b, &ds_b, &tx_b)
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
    // TASK C: RPC Events — small window first, probe-guided fallback, pagination for deeper
    // =====================================================================
    {
        let ds_c = Arc::clone(ds);
        let abi_c = Arc::clone(abi_reg);
        let tx_c = tx.clone();
        let voyager_c2 = voyager_c.clone();
        let _pf_ok = Arc::clone(&pf_succeeded);
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

            let events_result = if is_contract {
                ds_c.get_contract_events(address, Some(from_block), phase1_limit)
                    .await
            } else {
                ds_c.get_events_for_address(address, Some(from_block), phase1_limit)
                    .await
            };
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

                let mut contract_calls_list =
                    build_contract_calls_from_hashes(address, &call_hashes, &ds_c, &abi_c).await;
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

                        let deeper_events = if is_contract {
                            ds_c.get_contract_events(address, Some(search_from), 500)
                                .await
                        } else {
                            ds_c.get_events_for_address(address, Some(search_from), 500)
                                .await
                        };

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
pub(super) async fn enrich_address_txs(
    address: starknet::core::types::Felt,
    hashes: Vec<starknet::core::types::Felt>,
    ds: &Arc<dyn DataSource>,
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

    // Fetch tx + receipt in parallel
    let futs: Vec<_> = hashes
        .iter()
        .map(|hash| {
            let ds_t = Arc::clone(ds);
            let ds_r = Arc::clone(ds);
            let h = *hash;
            async move {
                let (tx_r, rx_r) = tokio::join!(ds_t.get_transaction(h), ds_r.get_receipt(h));
                (h, tx_r, rx_r)
            }
        })
        .collect();
    let results = futures::future::join_all(futs).await;

    // Pre-fetch ABIs for all contract addresses referenced in multicall calldata.
    // This populates the selector→name cache so that format_endpoint_names can
    // resolve function names instead of showing raw hex selectors.
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

    let mut updates = Vec::new();
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

    helpers::backfill_timestamps(&mut updates, ds).await;

    if !updates.is_empty() {
        let _ =
            action_tx.send(crate::app::actions::Action::AddressTxsEnriched { address, updates });
    }
}

/// Fill nonce gaps by scanning blocks between known nonces.
/// Reverted txs don't emit events, so they appear as gaps in the nonce sequence.
/// We scan blocks in the gap range to find them.
pub(super) async fn fill_nonce_gaps(
    address: starknet::core::types::Felt,
    current_nonce: u64,
    known_txs: &[crate::data::types::AddressTxSummary],
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    status_tx: &mpsc::UnboundedSender<Action>,
) -> Vec<crate::data::types::AddressTxSummary> {
    if known_txs.is_empty() || current_nonce == 0 {
        return Vec::new();
    }

    // Build a set of known nonces and their block numbers
    let known_nonces: std::collections::HashMap<u64, u64> = known_txs
        .iter()
        .filter(|t| t.block_number > 0)
        .map(|t| (t.nonce, t.block_number))
        .collect();

    // Find gaps in the nonce sequence (only in the range we have data for)
    let min_known = known_txs.iter().map(|t| t.nonce).min().unwrap_or(0);
    let max_known = known_txs.iter().map(|t| t.nonce).max().unwrap_or(0);

    // Also check gap between max_known and current_nonce
    let check_up_to = current_nonce.min(max_known + 20); // don't scan too far ahead

    let mut gaps: Vec<(u64, u64, u64)> = Vec::new(); // (missing_nonce, scan_from_block, scan_to_block)

    for nonce in min_known..check_up_to {
        if known_nonces.contains_key(&nonce) {
            continue;
        }
        // Find the block range to scan: between the nearest known nonces
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

        // Skip if the range is too large (> 50 blocks) — Dune will handle these
        if scan_to - scan_from > 50 {
            continue;
        }

        gaps.push((nonce, scan_from, scan_to));
    }

    if gaps.is_empty() {
        return Vec::new();
    }

    debug!(gaps = gaps.len(), "Scanning blocks to fill nonce gaps");

    // Collect unique blocks to scan
    let mut blocks_to_scan: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for (_, from, to) in &gaps {
        for b in *from..=*to {
            blocks_to_scan.insert(b);
        }
    }

    // Cap total blocks to scan
    if blocks_to_scan.len() > 200 {
        debug!(
            blocks = blocks_to_scan.len(),
            "Too many blocks to scan for gaps, deferring to Dune"
        );
        return Vec::new();
    }

    // Batch fetch blocks and filter for our sender
    let blocks_vec: Vec<u64> = blocks_to_scan.into_iter().collect();
    fetch_txs_from_blocks(address, &blocks_vec, known_txs, ds, abi_reg, status_tx).await
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

    // Fire RPC + Dune in parallel
    let rpc_ds = Arc::clone(ds);
    let rpc_addr = address;
    let rpc_before = before_block;
    let rpc_is_contract = is_contract;
    // Cap RPC window at 10k blocks — RPC is slow for large ranges.
    // For contracts, skip RPC entirely — Dune is the primary source for contract calls.
    let rpc_window = window_size.min(10_000);
    let rpc_from = before_block.saturating_sub(rpc_window);

    let rpc_fut = async move {
        if rpc_is_contract {
            // Skip RPC for contract pagination — events are rarely found via RPC
            // for high-traffic contracts, and the scan blocks the response.
            return Vec::new();
        }
        let events = rpc_ds
            .get_events_for_address(rpc_addr, Some(rpc_from), 500)
            .await
            .unwrap_or_default();
        events
            .into_iter()
            .filter(|e| e.block_number < rpc_before)
            .collect::<Vec<_>>()
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
        rpc_calls = build_contract_calls_from_hashes(address, &call_hashes, ds, abi_reg).await;
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
        helpers::backfill_timestamps(&mut summaries, ds).await;
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
        enrich_dune_calls(address, dune_calls, abi_reg, ds, tx).await
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

    helpers::backfill_call_timestamps(&mut dune_calls, ds).await;

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
