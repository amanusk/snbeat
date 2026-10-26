//! Address Transfers tab: ERC-20 transfers touching an address, fetched
//! newest-first over block windows. Every window that completes is recorded as
//! a scanned range so the App can render the unscanned remainder as gap rows.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use starknet::core::types::Felt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::address::{QueryGuard, query_addr_prefix};
use super::starkscan::StarkscanClient;
use crate::app::actions::Action;
use crate::data::DataSource;
use crate::data::pathfinder::PathfinderClient;
use crate::data::types::TokenTransfer;

/// Rows one fill collects before handing control back to the user.
pub const TRANSFER_FILL_TARGET: usize = 200;
const RPC_WINDOW_INITIAL: u64 = 10_000;
const RPC_WINDOW_MIN: u64 = 1_000;
const RPC_WINDOW_MAX: u64 = 40_000;
/// Keyed bloom scans on pathfinder run ~2s per 20k blocks on a sparse address.
const RPC_TIME_BUDGET: Duration = Duration::from_secs(12);

/// Clears the App's in-flight flag on every exit path, so a failed fill never
/// leaves a gap row stuck on "loading".
struct FillFinishedGuard {
    tx: mpsc::UnboundedSender<Action>,
    address: Felt,
    lo_block: Option<u64>,
}

impl Drop for FillFinishedGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(Action::AddressTransferFillFinished {
            address: self.address,
            lo_block: self.lo_block,
        });
    }
}

pub(super) struct TransferBackends<'a> {
    pub ds: &'a Arc<dyn DataSource>,
    pub starkscan: Option<&'a Arc<StarkscanClient>>,
    pub pf: Option<&'a Arc<PathfinderClient>>,
    pub tx: &'a mpsc::UnboundedSender<Action>,
}

/// Tab entry / poll: scan `(top scanned block, head]`, or from `floor_block`
/// on a cold cache.
pub(super) async fn fetch_top_delta(
    address: Felt,
    floor_block: u64,
    head: &Arc<super::HeadTracker>,
    b: TransferBackends<'_>,
) {
    let _finish = FillFinishedGuard {
        tx: b.tx.clone(),
        address,
        lo_block: None,
    };
    let head_block = match head.read().0 {
        0 => b.ds.latest_block_hint().await,
        n => Some(n),
    };
    let Some(head_block) = head_block else {
        warn!(address = %format!("{:#x}", address), "Transfers: no chain head available");
        return;
    };
    let ranges = b.ds.load_transfer_scanned_ranges(&address);
    let lo = match ranges.last() {
        Some(&(_, hi)) if hi >= head_block => {
            debug!(address = %format!("{:#x}", address), "Transfers: already scanned to head");
            return;
        }
        Some(&(_, hi)) => hi + 1,
        None => floor_block,
    };
    fill_window(address, lo, head_block, &b).await;
}

/// Enter on a gap row: fill `[lo, hi]` newest-first; the gap shrinks from its
/// newer edge if the fill stops at the row target.
pub(super) async fn fill_gap(address: Felt, lo: u64, hi: u64, b: TransferBackends<'_>) {
    let _finish = FillFinishedGuard {
        tx: b.tx.clone(),
        address,
        lo_block: Some(lo),
    };
    fill_window(address, lo, hi, &b).await;
}

fn publish(
    address: Felt,
    rows: Vec<TokenTransfer>,
    scanned: Option<(u64, u64)>,
    b: &TransferBackends<'_>,
) {
    if !rows.is_empty() {
        b.ds.save_transfers(&address, &rows);
    }
    if let Some((lo, hi)) = scanned {
        b.ds.add_transfer_scanned_range(&address, lo, hi);
    }
    let _ = b.tx.send(Action::AddressTransfersLoaded {
        address,
        rows,
        scanned,
    });
}

async fn fill_window(address: Felt, lo: u64, hi: u64, b: &TransferBackends<'_>) {
    if lo > hi {
        return;
    }
    let _guard = QueryGuard::new(
        b.tx,
        format!("transfers:{}", query_addr_prefix(&address)),
        "Transfers".to_string(),
    );
    let _ = b.tx.send(Action::LoadingStatus(format!(
        "Fetching transfers (blocks {lo}..{hi})..."
    )));
    let start = Instant::now();

    let mut cur_hi = hi;
    let mut collected = 0usize;
    let mut done = false;
    if let Some(ss) = b.starkscan.filter(|s| s.available()) {
        match fill_via_starkscan(address, lo, hi, ss, b).await {
            Ok(n) => {
                collected = n;
                done = true;
            }
            Err((e, resume_hi, n)) => {
                warn!(address = %format!("{:#x}", address), error = %e, "Transfers: Starkscan failed, falling back to RPC");
                cur_hi = resume_hi;
                collected = n;
            }
        }
    }
    if !done {
        collected = fill_via_rpc(address, lo, cur_hi, collected, b).await;
    }
    info!(
        address = %format!("{:#x}", address),
        lo,
        hi,
        rows = collected,
        elapsed_ms = start.elapsed().as_millis(),
        "Transfers window fill finished"
    );
    let _ = b.tx.send(Action::LoadingStatus(String::new()));
}

/// Pages of 100 newest-first inside `[lo, hi]`. A page ending mid-block
/// leaves that block's older rows on the next page, so its rows are held back
/// and coverage stops one block above it; they are published together once a
/// later page crosses the block, or re-fetched by the next fill if we stop
/// first. `Err` carries the block to resume from and the rows published so far.
async fn fill_via_starkscan(
    address: Felt,
    lo: u64,
    hi: u64,
    ss: &StarkscanClient,
    b: &TransferBackends<'_>,
) -> Result<usize, (anyhow::Error, u64, usize)> {
    let from_block = (lo > 0).then_some(lo);
    let mut cursor: Option<String> = None;
    let mut cur_hi = hi;
    let mut total = 0usize;
    let mut pending: Vec<TokenTransfer> = Vec::new();
    loop {
        let page = ss
            .fetch_transfers(address, from_block, Some(hi), cursor.as_deref())
            .await
            .map_err(|e| (e, cur_hi, total))?;
        let last_block = page.rows.last().map(|r| r.block_number);
        let mut rows = std::mem::take(&mut pending);
        rows.extend(page.rows);
        let covered_lo = match (&page.next_cursor, last_block) {
            (Some(_), Some(last)) => {
                pending = rows
                    .iter()
                    .filter(|r| r.block_number == last)
                    .cloned()
                    .collect();
                rows.retain(|r| r.block_number > last);
                last + 1
            }
            _ => lo,
        };
        total += rows.len();
        let scanned = (covered_lo <= cur_hi).then_some((covered_lo, cur_hi));
        if !rows.is_empty() || scanned.is_some() {
            publish(address, rows, scanned, b);
        }
        let (Some(next), Some(last)) = (page.next_cursor, last_block) else {
            return Ok(total);
        };
        // Only stop on a covered boundary; a block spanning whole pages keeps paging.
        if total >= TRANSFER_FILL_TARGET && scanned.is_some() {
            return Ok(total);
        }
        cursor = Some(next);
        cur_hi = last;
    }
}

/// Windowed `starknet_getEvents` walk, newest-first. Window size adapts to
/// the node's response time; stops at the row target, the time budget, or `lo`.
async fn fill_via_rpc(
    address: Felt,
    lo: u64,
    hi: u64,
    mut total: usize,
    b: &TransferBackends<'_>,
) -> usize {
    let start = Instant::now();
    let mut window = RPC_WINDOW_INITIAL;
    let mut cur_hi = hi;
    loop {
        let from = cur_hi.saturating_sub(window - 1).max(lo);
        let t0 = Instant::now();
        let mut rows = match b.ds.get_transfer_events(address, from, cur_hi).await {
            Ok(r) => r,
            Err(e) => {
                warn!(address = %format!("{:#x}", address), from, to = cur_hi, error = %e, "Transfers: RPC scan failed");
                let _ = b.tx.send(Action::Error(format!("Transfers scan: {e}")));
                return total;
            }
        };
        let took = t0.elapsed();
        let empty = rows.is_empty();
        total += rows.len();
        backfill_timestamps(&mut rows, b).await;
        publish(address, rows, Some((from, cur_hi)), b);
        if from <= lo || total >= TRANSFER_FILL_TARGET || start.elapsed() > RPC_TIME_BUDGET {
            return total;
        }
        if empty && took < Duration::from_secs(2) {
            window = (window * 2).min(RPC_WINDOW_MAX);
        } else if took > Duration::from_secs(6) {
            window = (window / 2).max(RPC_WINDOW_MIN);
        }
        cur_hi = from - 1;
    }
}

async fn backfill_timestamps(rows: &mut [TokenTransfer], b: &TransferBackends<'_>) {
    let blocks: HashSet<u64> = rows
        .iter()
        .filter(|r| r.timestamp == 0 && r.block_number > 0)
        .map(|r| r.block_number)
        .collect();
    if blocks.is_empty() {
        return;
    }
    let ts = super::helpers::fetch_block_timestamps(blocks, b.ds, b.pf).await;
    for r in rows.iter_mut() {
        if let Some(t) = ts.get(&r.block_number) {
            r.timestamp = *t;
        }
    }
}
