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
use crate::decode::functions::parse_multicall;
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
    let endpoint_names = format_endpoint_names(tx, abi_reg);

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
    let endpoint_names = if tx_type == "INVOKE" {
        let calldata: Vec<Felt> = pf_tx
            .calldata
            .iter()
            .filter_map(|h| Felt::from_hex(h).ok())
            .collect();
        let calls = parse_multicall(&calldata);
        format_selector_names(calls.iter().map(|c| c.selector), abi_reg)
    } else {
        String::new()
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
    })
}

/// Collect the set of target contract addresses referenced by a pf tx's
/// multicall calldata. Used to pre-warm the ABI registry before building
/// summaries in bulk.
pub fn pf_tx_target_addresses(pf_tx: &TxByHashData) -> Vec<Felt> {
    if normalize_pf_tx_type(&pf_tx.tx_type) != "INVOKE" {
        return Vec::new();
    }
    let calldata: Vec<Felt> = pf_tx
        .calldata
        .iter()
        .filter_map(|h| Felt::from_hex(h).ok())
        .collect();
    parse_multicall(&calldata)
        .into_iter()
        .map(|c| c.contract_address)
        .collect()
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
        if s.timestamp == 0 {
            if let Some(&ts) = ts_map.get(&s.block_number) {
                s.timestamp = ts;
            }
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
        if c.timestamp == 0 {
            if let Some(&ts) = ts_map.get(&c.block_number) {
                c.timestamp = ts;
            }
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
