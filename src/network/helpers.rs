//! Shared helper functions for building tx summaries, formatting endpoint names,
//! and backfilling timestamps. Extracted from duplicated patterns across the
//! network module to ensure consistent behavior.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::data::DataSource;
use crate::data::pathfinder::{PathfinderClient, TxByHashData};
use crate::data::types::{
    AddressTxSummary, ContractCallSummary, ExecutionStatus, SnReceipt, SnTransaction,
};
use crate::decode::AbiRegistry;
use crate::decode::functions::{RawCall, parse_multicall};
use crate::decode::outside_execution::detect_outside_execution;
use crate::utils::{felt_to_u64, felt_to_u128};

/// Format endpoint/function names from a transaction's multicall calldata.
///
/// Parses the calldata to extract all calls, resolves each selector to a human-readable
/// name via the ABI registry, and formats them as:
///   - `"transfer, approve"` (up to 3 calls)
///   - `"transfer, approve, swap, … +2"` (more than 3 calls)
///   - `""` for non-Invoke transactions
pub fn format_endpoint_names(tx: &SnTransaction, abi_reg: &AbiRegistry) -> String {
    let calls = match tx {
        SnTransaction::Invoke(i) => parse_multicall(&i.calldata),
        _ => return String::new(),
    };
    format_selector_names(calls.iter().map(|c| c.selector), abi_reg)
}

/// Top-level multicall targets only, in first-seen order with duplicates
/// removed. Empty for non-Invoke txs.
///
/// Unlike the broader `AddressTxSummary::called_contracts` field (which
/// `build_tx_summary` extends with OE inner targets), this helper stops
/// at the top level — callers that need OE-inner addresses should walk
/// `oe_inner_targets` themselves.
pub fn tx_called_contracts(tx: &SnTransaction) -> Vec<Felt> {
    let calls = match tx {
        SnTransaction::Invoke(i) => parse_multicall(&i.calldata),
        _ => return Vec::new(),
    };
    dedupe_preserve_order(calls.into_iter().map(|c| c.contract_address))
}

/// Deduplicate while preserving first-seen order. Cheap O(n²) is fine for the
/// handful of calls a typical multicall carries.
fn dedupe_preserve_order(items: impl IntoIterator<Item = Felt>) -> Vec<Felt> {
    let mut out: Vec<Felt> = Vec::new();
    for item in items {
        if !out.contains(&item) {
            out.push(item);
        }
    }
    out
}

/// Walk a multicall's raw calls and collect target addresses of any OE inner
/// calls (deduplicated, first-seen order). Surfaces privacy-pool / anonymizer
/// interactions that only appear inside `execute_from_outside*` or
/// `execute_private_sponsored` wrappers — the top-level call list alone would
/// miss them.
pub fn oe_inner_targets(calls: &[RawCall], abi_reg: &AbiRegistry) -> Vec<Felt> {
    let mut out: Vec<Felt> = Vec::new();
    for call in calls {
        let resolved_name = call
            .function_name
            .clone()
            .or_else(|| abi_reg.get_selector_name(&call.selector));
        if let Some((oe, _method)) = detect_outside_execution(call, resolved_name.as_deref()) {
            for ic in &oe.inner_calls {
                if !out.contains(&ic.contract_address) {
                    out.push(ic.contract_address);
                }
            }
        }
    }
    out
}

/// Resolve a selector to its ABI name, or a short hex fallback if unknown.
/// Shared across per-call formatters so the fallback shape stays consistent.
pub fn format_selector_name_or_hex(selector: &Felt, abi_reg: &AbiRegistry) -> String {
    abi_reg.get_selector_name(selector).unwrap_or_else(|| {
        let hex = format!("{:#x}", selector);
        if hex.len() > 10 {
            format!("{}…", &hex[..10])
        } else {
            hex
        }
    })
}

/// Format a list of selectors into a human-readable endpoint name string.
/// Truncates to 3 + "+N" once more than three selectors are present.
pub fn format_selector_names(
    selectors: impl Iterator<Item = Felt>,
    abi_reg: &AbiRegistry,
) -> String {
    let names: Vec<String> = selectors
        .map(|sel| format_selector_name_or_hex(&sel, abi_reg))
        .collect();
    if names.is_empty() {
        return String::new();
    }
    if names.len() <= 3 {
        names.join(", ")
    } else {
        format!("{}, … +{}", names[..3].join(", "), names.len() - 3)
    }
}

/// Pre-warm the ABI registry for a set of addresses in parallel.
///
/// Each `get_abi_for_address` call populates the LRU + SQLite cache so that
/// subsequent synchronous `get_selector_name` lookups in the same task hit
/// warm data. No-op for addresses that fail to fetch (errors are already
/// surfaced inside `AbiRegistry`).
pub async fn prewarm_abis(addresses: impl IntoIterator<Item = Felt>, abi_reg: &AbiRegistry) {
    let futs: Vec<_> = addresses
        .into_iter()
        .map(|a| async move {
            let _ = abi_reg.get_abi_for_address(&a).await;
        })
        .collect();
    futures::future::join_all(futs).await;
}

/// Resolve the class hash that was active for `address` at `block`.
///
/// Tries in order:
/// 1. Cached class_history (populated by address-view visits or earlier
///    tx-decode calls). Pick the latest entry with `block_number <= block`.
/// 2. pf-query `/class-history/{address}`, then save full history to cache.
/// 3. RPC `starknet_getClassHashAt(BlockId::Number(block), address)`.
///    Result is intentionally NOT written into class_history — that table
///    is a list of class-hash *changes*, and a single point lookup would
///    falsely indicate "this is the only class this address ever had".
///
/// Returns `None` if all three fail; callers should fall back to the
/// latest-ABI path.
pub async fn resolve_class_hash_at(
    address: Felt,
    block: u64,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
) -> Option<Felt> {
    // 1. cached class_history (desc-ordered).
    let mut history = ds.load_cached_class_history(&address);

    // The cache "covers" the target block only if BOTH:
    //   * the oldest cached entry is at/before `block` (cache reaches back
    //     far enough to find the right class), AND
    //   * pf-query has validated the cache forward through at least `block`
    //     (no unobserved `replace_class` could sit between the newest cached
    //     entry and `block`).
    // Skipping the forward check would let a stale cache satisfy a newer
    // target block and return the wrong class hash.
    let reaches_back = history
        .last()
        .map(|e| e.block_number <= block)
        .unwrap_or(false);
    let validated_through_target = ds
        .load_class_history_max_block(&address)
        .map(|max_block| max_block >= block)
        .unwrap_or(false);
    let cache_covers = reaches_back && validated_through_target;
    if !cache_covers && let Some(pf) = pf {
        match pf.get_class_history(address).await {
            Ok(entries) => {
                if !entries.is_empty() {
                    let latest = ds.get_latest_block_number().await.unwrap_or(0);
                    ds.save_class_history(&address, &entries);
                    if latest > 0 {
                        ds.save_class_history_max_block(&address, latest);
                    }
                    history = entries;
                }
            }
            Err(e) => {
                debug!(address = %format!("{:#x}", address), error = %e, "pf-query class-history fetch failed");
            }
        }
    }

    // 2. scan desc-ordered history for first entry with block_number <= target
    for entry in &history {
        if entry.block_number <= block {
            if let Ok(felt) = Felt::from_hex(&entry.class_hash) {
                return Some(felt);
            }
            warn!(class_hash = %entry.class_hash, "Bad class_hash hex in class_history");
            break;
        }
    }

    // 3. RPC fallback (don't cache — see doc comment).
    match ds.get_class_hash_at(address, block).await {
        Ok(ch) => Some(ch),
        Err(e) => {
            debug!(address = %format!("{:#x}", address), block, error = %e, "RPC get_class_hash_at failed");
            None
        }
    }
}

/// Pre-warm ABIs for every address used in a tx, resolving each one's
/// class hash *as of `block`* (not latest). Returns the address → class_hash
/// map so callers can decode events / calls against the correct ABI by
/// passing the resolved class to `AbiRegistry::get_abi_for_class` (which
/// will be a cache hit after this prewarm).
///
/// Addresses that fail to resolve are simply omitted from the returned
/// map; callers should fall back to `AbiRegistry::get_abi_for_address`
/// (latest) for those.
pub async fn prewarm_abis_at(
    addresses: impl IntoIterator<Item = Felt>,
    block: u64,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
    abi_reg: &AbiRegistry,
) -> HashMap<Felt, Felt> {
    let addrs: Vec<Felt> = addresses.into_iter().collect();

    // Resolve all (address → class_hash @ block) in parallel.
    let resolutions = futures::future::join_all(
        addrs
            .iter()
            .copied()
            .map(|a| async move { (a, resolve_class_hash_at(a, block, ds, pf).await) }),
    )
    .await;

    let mut addr_to_class: HashMap<Felt, Felt> = HashMap::new();
    let mut unique_classes: HashSet<Felt> = HashSet::new();
    for (addr, class_opt) in resolutions {
        if let Some(ch) = class_opt {
            addr_to_class.insert(addr, ch);
            unique_classes.insert(ch);
        }
    }

    // Prewarm ABIs for each unique class_hash in parallel.
    let class_futs: Vec<_> = unique_classes
        .iter()
        .copied()
        .map(|ch| async move {
            let _ = abi_reg.get_abi_for_class(&ch).await;
        })
        .collect();
    futures::future::join_all(class_futs).await;

    addr_to_class
}

/// Extract execution status string from a receipt.
pub fn receipt_status(receipt: Option<&SnReceipt>) -> String {
    receipt
        .map(|r| match &r.execution_status {
            ExecutionStatus::Succeeded => "OK",
            ExecutionStatus::Reverted(_) => "REV",
            ExecutionStatus::Unknown => "?",
        })
        .unwrap_or("?")
        .to_string()
}

/// Extract nonce and tip from a transaction.
pub fn extract_nonce_tip(tx: &SnTransaction) -> (u64, u64) {
    match tx {
        SnTransaction::Invoke(i) => (i.nonce.map(|n| felt_to_u64(&n)).unwrap_or(0), i.tip),
        SnTransaction::Declare(d) => (0, d.tip),
        SnTransaction::DeployAccount(da) => {
            (da.nonce.map(|n| felt_to_u64(&n)).unwrap_or(0), da.tip)
        }
        _ => (0, 0),
    }
}

/// Build an `AddressTxSummary` from a fetched transaction, optional receipt,
/// and block number. Resolves endpoint names via the ABI registry.
pub fn build_tx_summary(
    hash: Felt,
    tx: &SnTransaction,
    receipt: Option<&SnReceipt>,
    block_number: u64,
    timestamp: u64,
    abi_reg: &AbiRegistry,
) -> AddressTxSummary {
    let fee_fri = receipt.map(|r| felt_to_u128(&r.actual_fee)).unwrap_or(0);
    let status = receipt_status(receipt);
    let (nonce, tip) = extract_nonce_tip(tx);
    // Parse calldata once and feed both endpoint formatting and called-contracts.
    let calls = match tx {
        SnTransaction::Invoke(i) => parse_multicall(&i.calldata),
        _ => Vec::new(),
    };
    let endpoint_names = format_selector_names(calls.iter().map(|c| c.selector), abi_reg);
    let mut called_contracts = dedupe_preserve_order(calls.iter().map(|c| c.contract_address));
    // Append OE inner targets so the privacy predicate (and the Calls column)
    // can see the pool when it's reached only via an outside-execution wrapper.
    for addr in oe_inner_targets(&calls, abi_reg) {
        if !called_contracts.contains(&addr) {
            called_contracts.push(addr);
        }
    }

    AddressTxSummary {
        hash,
        nonce,
        block_number,
        timestamp,
        endpoint_names,
        total_fee_fri: fee_fri,
        tip,
        tx_type: tx.type_name().to_string(),
        status,
        sender: Some(tx.sender()),
        called_contracts,
    }
}

/// Normalize a pf-query tx_type (e.g. "INVOKE_V3") to snbeat's canonical
/// form (e.g. "INVOKE"). pf returns a versioned string; `SnTransaction::type_name()`
/// strips the version. Leaves `L1_HANDLER` as-is.
pub fn normalize_pf_tx_type(pf_type: &str) -> String {
    // Strip trailing `_V<n>` if present.
    if let Some(idx) = pf_type.rfind("_V")
        && pf_type[idx + 2..].chars().all(|c| c.is_ascii_digit())
        && !pf_type[idx + 2..].is_empty()
    {
        return pf_type[..idx].to_string();
    }
    pf_type.to_string()
}

/// Build an `AddressTxSummary` directly from pf-query `TxByHashData`, without
/// going through an `SnTransaction`. Parses calldata and resolves selectors
/// via the ABI registry the same way `build_tx_summary` does for RPC-sourced
/// transactions.
///
/// Returns `None` if the response is malformed (bad hex). Callers should
/// fall through to the RPC path in that case.
pub fn build_tx_summary_from_pf_data(
    pf_tx: &TxByHashData,
    abi_reg: &AbiRegistry,
) -> Option<AddressTxSummary> {
    let hash = Felt::from_hex(&pf_tx.hash).ok()?;
    let sender = Felt::from_hex(&pf_tx.sender).ok()?;
    let fee_fri = u128::from_str_radix(pf_tx.actual_fee.trim_start_matches("0x"), 16).unwrap_or(0);
    let tx_type = normalize_pf_tx_type(&pf_tx.tx_type);

    // Only INVOKE transactions have multicall calldata to decode.
    let (endpoint_names, called_contracts) = if tx_type == "INVOKE" {
        let calldata: Vec<Felt> = pf_tx
            .calldata
            .iter()
            .filter_map(|h| Felt::from_hex(h).ok())
            .collect();
        let calls = parse_multicall(&calldata);
        let names = format_selector_names(calls.iter().map(|c| c.selector), abi_reg);
        let mut contracts = dedupe_preserve_order(calls.iter().map(|c| c.contract_address));
        for addr in oe_inner_targets(&calls, abi_reg) {
            if !contracts.contains(&addr) {
                contracts.push(addr);
            }
        }
        (names, contracts)
    } else {
        (String::new(), Vec::new())
    };

    Some(AddressTxSummary {
        hash,
        nonce: pf_tx.nonce.unwrap_or(0),
        block_number: pf_tx.block_number,
        timestamp: pf_tx.block_timestamp,
        endpoint_names,
        total_fee_fri: fee_fri,
        tip: pf_tx.tip,
        tx_type,
        status: pf_tx.status.clone(),
        sender: Some(sender),
        called_contracts,
    })
}

/// Collect the set of target contract addresses referenced by a pf tx's
/// multicall calldata. Used to pre-warm the ABI registry before building
/// summaries in bulk. Order-preserving and deduplicated.
pub fn pf_tx_target_addresses(pf_tx: &TxByHashData) -> Vec<Felt> {
    if normalize_pf_tx_type(&pf_tx.tx_type) != "INVOKE" {
        return Vec::new();
    }
    let calldata: Vec<Felt> = pf_tx
        .calldata
        .iter()
        .filter_map(|h| Felt::from_hex(h).ok())
        .collect();
    dedupe_preserve_order(
        parse_multicall(&calldata)
            .into_iter()
            .map(|c| c.contract_address),
    )
}

/// Backfill timestamps on address tx summaries by fetching block data.
///
/// Collects all unique block numbers where `timestamp == 0`, fetches them
/// via pf-query (range query, one round trip) when available, RPC per-block
/// for anything pf doesn't return.
pub async fn backfill_timestamps(
    summaries: &mut [AddressTxSummary],
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
) {
    let blocks_needing_ts: HashSet<u64> = summaries
        .iter()
        .filter(|s| s.timestamp == 0 && s.block_number > 0)
        .map(|s| s.block_number)
        .collect();
    if blocks_needing_ts.is_empty() {
        return;
    }
    let ts_map = fetch_block_timestamps(blocks_needing_ts, ds, pf).await;
    for s in summaries.iter_mut() {
        if s.timestamp == 0
            && let Some(&ts) = ts_map.get(&s.block_number)
        {
            s.timestamp = ts;
        }
    }
}

/// Backfill timestamps on contract call summaries by fetching block data.
pub async fn backfill_call_timestamps(
    calls: &mut [ContractCallSummary],
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
) {
    let blocks_needing_ts: HashSet<u64> = calls
        .iter()
        .filter(|c| c.timestamp == 0 && c.block_number > 0)
        .map(|c| c.block_number)
        .collect();
    if blocks_needing_ts.is_empty() {
        return;
    }
    let ts_map = fetch_block_timestamps(blocks_needing_ts, ds, pf).await;
    for c in calls.iter_mut() {
        if c.timestamp == 0
            && let Some(&ts) = ts_map.get(&c.block_number)
        {
            c.timestamp = ts;
        }
    }
}

/// Fetch timestamps for a set of block numbers. Prefers pf-query (single
/// range query over the min..=max span) when available; falls back to
/// per-block RPC for any gaps.
async fn fetch_block_timestamps(
    block_numbers: HashSet<u64>,
    ds: &Arc<dyn DataSource>,
    pf: Option<&Arc<PathfinderClient>>,
) -> HashMap<u64, u64> {
    use futures::stream::{self, StreamExt};

    let mut ts_map: HashMap<u64, u64> = HashMap::new();

    if let Some(pf) = pf {
        let min = *block_numbers.iter().min().unwrap();
        let max = *block_numbers.iter().max().unwrap();
        match pf.get_block_timestamps(min, max).await {
            Ok(entries) => {
                debug!(
                    requested = block_numbers.len(),
                    returned = entries.len(),
                    range = format!("{min}..={max}"),
                    "Fetched block timestamps from pf-query"
                );
                for e in entries {
                    if block_numbers.contains(&e.block_number) {
                        ts_map.insert(e.block_number, e.timestamp);
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "pf-query block-timestamps failed, falling back to RPC");
            }
        }
    }

    let missing: Vec<u64> = block_numbers
        .iter()
        .copied()
        .filter(|bn| !ts_map.contains_key(bn))
        .collect();
    if missing.is_empty() {
        return ts_map;
    }

    let rpc_map: HashMap<u64, u64> = stream::iter(missing)
        .map(|bn| {
            let ds_blk = Arc::clone(ds);
            async move { (bn, ds_blk.get_block(bn).await) }
        })
        .buffer_unordered(10)
        .filter_map(|(bn, r)| async move { r.ok().map(|b| (bn, b.timestamp)) })
        .collect()
        .await;
    ts_map.extend(rpc_map);
    ts_map
}
