//! Shared helper functions for building tx summaries, formatting endpoint names,
//! and backfilling timestamps. Extracted from duplicated patterns across the
//! network module to ensure consistent behavior.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use starknet::core::types::Felt;

use crate::data::DataSource;
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

/// Format a list of selectors into a human-readable endpoint name string.
pub fn format_selector_names(
    selectors: impl Iterator<Item = Felt>,
    abi_reg: &AbiRegistry,
) -> String {
    let names: Vec<String> = selectors
        .map(|sel| {
            abi_reg.get_selector_name(&sel).unwrap_or_else(|| {
                let hex = format!("{:#x}", sel);
                if hex.len() > 10 {
                    format!("{}…", &hex[..10])
                } else {
                    hex
                }
            })
        })
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
        SnTransaction::DeployAccount(da) => (0, da.tip),
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

/// Backfill timestamps on address tx summaries by fetching block data.
///
/// Collects all unique block numbers where `timestamp == 0`, fetches them
/// in parallel, and applies the timestamps.
pub async fn backfill_timestamps(summaries: &mut [AddressTxSummary], ds: &Arc<dyn DataSource>) {
    let blocks_needing_ts: HashSet<u64> = summaries
        .iter()
        .filter(|s| s.timestamp == 0 && s.block_number > 0)
        .map(|s| s.block_number)
        .collect();
    if blocks_needing_ts.is_empty() {
        return;
    }
    let ts_map = fetch_block_timestamps(blocks_needing_ts, ds).await;
    for s in summaries.iter_mut() {
        if s.timestamp == 0 {
            if let Some(&ts) = ts_map.get(&s.block_number) {
                s.timestamp = ts;
            }
        }
    }
}

/// Backfill timestamps on contract call summaries by fetching block data.
pub async fn backfill_call_timestamps(calls: &mut [ContractCallSummary], ds: &Arc<dyn DataSource>) {
    let blocks_needing_ts: HashSet<u64> = calls
        .iter()
        .filter(|c| c.timestamp == 0 && c.block_number > 0)
        .map(|c| c.block_number)
        .collect();
    if blocks_needing_ts.is_empty() {
        return;
    }
    let ts_map = fetch_block_timestamps(blocks_needing_ts, ds).await;
    for c in calls.iter_mut() {
        if c.timestamp == 0 {
            if let Some(&ts) = ts_map.get(&c.block_number) {
                c.timestamp = ts;
            }
        }
    }
}

/// Fetch timestamps for a set of block numbers in parallel.
async fn fetch_block_timestamps(
    block_numbers: HashSet<u64>,
    ds: &Arc<dyn DataSource>,
) -> HashMap<u64, u64> {
    use futures::stream::{self, StreamExt};
    stream::iter(block_numbers)
        .map(|bn| {
            let ds_blk = Arc::clone(ds);
            async move { (bn, ds_blk.get_block(bn).await) }
        })
        .buffer_unordered(10)
        .filter_map(|(bn, r)| async move { r.ok().map(|b| (bn, b.timestamp)) })
        .collect()
        .await
}
