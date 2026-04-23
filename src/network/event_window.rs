//! Unified windowed event-fetch pipeline for an address.
//!
//! Backs the three "additive" address tabs (MetaTxs, Calls, Events) on a single
//! source: pf-query events persisted to `address_events` with a per-address
//! scanned-range cursor (`address_search_progress`). Each tab is a projection
//! over the same cache; fetching once updates all three.
//!
//! # Policies
//!
//! - [`EventWindowPolicy::TopDelta`] — the default on tab entry. Fetches the
//!   block range `(max_searched_block, latest_block]` so we only ever pay for
//!   what's new since the last visit. On a cold cache, falls back to a bounded
//!   first-paint window from the tip — we don't auto-walk to the deploy block.
//! - [`EventWindowPolicy::ExtendDown`] / [`EventWindowPolicy::FillGap`] are
//!   the scrolling primitives; wiring lands with the gap UI.
//!
//! # Invariants
//!
//! - Every returned event is also persisted (additively) to `address_events`.
//! - `address_search_progress` is advanced only over ranges we actually scanned
//!   contiguously; a deferred gap is reported to the caller instead of being
//!   silently bridged.

use std::sync::Arc;

use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::data::pathfinder::PathfinderClient;
use crate::data::types::SnEvent;
use crate::data::{DataSource, FilterKind};
use crate::error::Result;
use crate::network::address::{
    AddressActivityPage, EventQueryKind, fetch_address_activity, fetch_events_routed,
};

/// Map an [`EventQueryKind`] to the scan-coverage kind recorded in
/// `address_search_progress`. Accounts scan with a `TRANSACTION_EXECUTED`
/// key filter (keyed), contracts scan without a key filter (unkeyed).
fn filter_kind_for(kind: EventQueryKind) -> FilterKind {
    match kind {
        EventQueryKind::Account => FilterKind::Keyed,
        EventQueryKind::Contract => FilterKind::Unkeyed,
    }
}

/// Initial window size (events) when no prior scan exists. Matches the pre-
/// existing Phase-1 scan limits used by `fetch_and_send_address_info`.
pub const EVENT_FIRST_PAINT_TARGET: u32 = 200;

/// If the delta between `max_searched_block` and `latest_block` exceeds this
/// threshold, don't auto-fill the intervening range — record a deferred gap
/// and scan only the tip. Prevents "address wasn't visited in a month" from
/// triggering a multi-million-block walk on revisit.
///
/// ~100k blocks ≈ 3 days on Starknet mainnet at current block rate.
pub const EVENT_LARGE_GAP_THRESHOLD_BLOCKS: u64 = 100_000;

/// When the delta is larger than the threshold, this is the tip window we do
/// fetch (blocks from `latest_block - TIP_ONLY_WINDOW_BLOCKS` to `latest_block`).
pub const TIP_ONLY_WINDOW_BLOCKS: u64 = 5_000;

/// How many events a single `TopDelta` fetch is allowed to pull.
pub const EVENT_PAGE_LIMIT: u32 = 200;

/// Initial `ExtendDown` window size (blocks). Small enough to return results
/// quickly on dense addresses; grown adaptively when the backend returns
/// empty pages (sparse addresses).
pub const EXTEND_DOWN_INITIAL_WINDOW: u64 = 5_000;

/// Minimum `ExtendDown` window size (blocks). We halve toward this floor
/// when a scan hits the [`EVENT_PAGE_LIMIT`] cap (signal that the window
/// was too big and events may have been truncated).
pub const EXTEND_DOWN_MIN_WINDOW: u64 = 5_000;

/// Maximum `ExtendDown` window size (blocks). We double toward this cap
/// when a scan returns zero events on a sparse address. Kept well below
/// what a keyed pf-query bloom scan can complete within the 30 s client
/// deadline — start conservative, raise if measurements allow.
pub const EXTEND_DOWN_MAX_WINDOW: u64 = 500_000;

/// Fetch policy for [`ensure_address_events_window`].
#[derive(Debug, Clone, Copy)]
pub enum EventWindowPolicy {
    /// Fetch events newer than our cursor. Default on tab entry.
    ///
    /// Semantics:
    /// - Cold cache → fetch newest `EVENT_FIRST_PAINT_TARGET` events from tip.
    /// - Warm cache, small delta → fetch `(max_searched, latest]`.
    /// - Warm cache, large delta → fetch the last `TIP_ONLY_WINDOW_BLOCKS` and
    ///   record a deferred gap; caller decides when/if to fill.
    TopDelta,
    /// Fetch events strictly older than the current `min_searched_block`.
    /// The `window_size` is the number of blocks below the cached floor to
    /// scan in this call; the caller decides how to adapt it across
    /// successive pages (e.g. doubling on empty hits up to
    /// [`EXTEND_DOWN_MAX_WINDOW`]).
    ExtendDown { window_size: u64 },
    /// Fetch a specific block range — used to fill a previously deferred gap.
    #[allow(dead_code)] // wired with the gap UI task
    FillGap { from_block: u64, to_block: u64 },
}

/// Result of a single window fetch.
#[derive(Debug, Clone)]
pub(crate) struct EventWindowOutcome {
    /// Full merged event list for the address, newest-first. Includes both
    /// cached and freshly-fetched events.
    pub merged: Vec<SnEvent>,
    /// How many events the upstream returned for *this* call. Independent of
    /// cache contents — a revisit with nothing new yields 0.
    pub fetched: usize,
    /// pf-query continuation token. `Some` ⇒ the current page wasn't exhausted
    /// and calling again would return more within the same window.
    pub next_token: Option<u64>,
    /// Range `(lo, hi)` we *knowingly* skipped because the gap exceeded
    /// `EVENT_LARGE_GAP_THRESHOLD_BLOCKS`. `None` ⇒ the scanned range is
    /// contiguous. Meant to be surfaced as a gap marker in the UI.
    pub deferred_gap: Option<(u64, u64)>,
    /// New max scanned block after this call (reflects what we actually
    /// scanned, not what we deferred).
    pub max_searched: u64,
    /// Lowest block we've ever scanned for this address. `0` means "we've
    /// reached history" (or never scanned). Used as the "more older events
    /// to fetch" signal — if `min_searched > 0`, an `ExtendDown` call can
    /// pull more.
    pub min_searched: u64,
    /// Suggested window size (blocks) for the *next* `ExtendDown` call,
    /// derived from this call's hit density:
    /// - 0 events fetched → double (sparse address, amortize round-trips).
    /// - `>= EVENT_PAGE_LIMIT` events → halve (dense address; we may have
    ///   truncated at the page cap, narrow the next window).
    /// - otherwise → keep the current size.
    /// Clamped to `[EXTEND_DOWN_MIN_WINDOW, EXTEND_DOWN_MAX_WINDOW]`.
    /// `None` for non-`ExtendDown` policies and short-circuit paths.
    pub suggested_next_window: Option<u64>,
    /// The unfiltered pf-query page from the fetch, with `tx_rows` included.
    /// Callers that need the raw events + per-tx metadata (e.g. MetaTxs
    /// classification) can derive from this without re-querying.
    pub page: crate::network::address::AddressActivityPage,
}

/// Advance the per-address event window according to `policy`, merging new
/// events into the persistent cache and updating the scanned-range cursor.
///
/// pf-query is preferred — it returns tx_rows in bulk alongside events so
/// downstream classifiers (meta-tx, calls) don't need a second round trip.
/// When pf is unavailable we fall back to an RPC-only scan via
/// [`fetch_events_routed`]; `page.tx_rows` will be empty in that case and
/// callers that need rich tx data should re-fetch per-hash through the
/// [`DataSource`] (see `build_contract_calls_from_hashes`).
pub(crate) async fn ensure_address_events_window(
    address: Felt,
    kind: EventQueryKind,
    policy: EventWindowPolicy,
    pf: Option<&Arc<PathfinderClient>>,
    ds: &Arc<dyn DataSource>,
    latest_block: u64,
    floor_block: u64,
) -> Result<EventWindowOutcome> {
    // Current cursor (None ⇒ cold cache). Keyed/unkeyed coverage is tracked
    // separately so a narrower keyed scan doesn't lie about broader coverage.
    let filter_kind = filter_kind_for(kind);
    let progress = ds.load_search_progress(&address, filter_kind);

    // Pick a fetch window.
    let (from_block, deferred_gap) = match policy {
        EventWindowPolicy::TopDelta => resolve_top_delta(progress, latest_block),
        EventWindowPolicy::ExtendDown { window_size } => {
            // Scan below the cached floor. `fetch_address_activity` fetches
            // newest-first, so "older" here means a lower from_block paired
            // with to_block = current min - 1.
            match progress {
                Some((min, _)) if min > floor_block => {
                    // Clamp at floor so we never scan past e.g. the deploy
                    // block. `to_block` below is still `min - 1`, so the
                    // window we actually hit is `[from, min-1]` — no double
                    // coverage of already-scanned range above `min`.
                    let from = min.saturating_sub(window_size).max(floor_block);
                    (from, None)
                }
                Some(_) => {
                    // Already at floor (or below) — nothing older to fetch.
                    let (min, max) = progress.unwrap_or((0, 0));
                    return Ok(EventWindowOutcome {
                        merged: ds.load_address_events(&address),
                        fetched: 0,
                        next_token: None,
                        deferred_gap: None,
                        max_searched: max,
                        min_searched: min,
                        suggested_next_window: None,
                        page: empty_page(),
                    });
                }
                None => {
                    // Cold cache: no "down" to extend. Degrade to TopDelta.
                    resolve_top_delta(progress, latest_block)
                }
            }
        }
        EventWindowPolicy::FillGap { from_block, .. } => (from_block, None),
    };

    let to_block = match policy {
        EventWindowPolicy::FillGap { to_block, .. } => Some(to_block),
        EventWindowPolicy::ExtendDown { .. } => progress.and_then(|(min, _)| min.checked_sub(1)),
        EventWindowPolicy::TopDelta => None,
    };

    debug!(
        address = %format!("{:#x}", address),
        ?policy,
        from_block,
        ?to_block,
        ?deferred_gap,
        pf_available = pf.is_some(),
        "event_window: fetching"
    );

    let page = match pf {
        Some(pf_client) => {
            fetch_address_activity(address, kind, from_block, None, EVENT_PAGE_LIMIT, pf_client)
                .await
                .inspect_err(|e| {
                    warn!(
                        address = %format!("{:#x}", address),
                        error = %e,
                        "event_window: fetch_address_activity failed"
                    );
                })?
        }
        None => {
            // RPC fallback: just events, no bulk tx_rows. Callers that need
            // tx data build it per-hash via the DataSource.
            let events = fetch_events_routed(
                kind,
                None,
                ds,
                address,
                Some(from_block),
                to_block,
                EVENT_PAGE_LIMIT as usize,
            )
            .await
            .inspect_err(|e| {
                warn!(
                    address = %format!("{:#x}", address),
                    error = %e,
                    "event_window: RPC event scan failed"
                );
            })?;
            page_from_events(events)
        }
    };

    let fetched = page.events.len();
    let merged = ds.merge_address_events(&address, &page.events);

    // Update cursor. We only advance over ranges we *actually* scanned; if a
    // gap was deferred, the old min/max remain and we merge in the tip range.
    let scanned_min = match policy {
        EventWindowPolicy::TopDelta => from_block,
        EventWindowPolicy::ExtendDown { .. } => from_block,
        EventWindowPolicy::FillGap { from_block, .. } => from_block,
    };
    let scanned_max = to_block.unwrap_or(latest_block);
    if scanned_max >= scanned_min {
        ds.save_search_progress(&address, filter_kind, scanned_min, scanned_max);
    }

    let (min_searched, max_searched) = ds
        .load_search_progress(&address, filter_kind)
        .unwrap_or((scanned_min, scanned_max));

    // Adapt the next ExtendDown window based on hit density. Empty window →
    // widen (sparse address, minimise round-trips); full page (bloom cap
    // hit) → narrow (dense address; we may have truncated events). Only
    // meaningful for ExtendDown.
    let suggested_next_window = match policy {
        EventWindowPolicy::ExtendDown { window_size } => {
            Some(suggest_next_window(window_size, fetched))
        }
        _ => None,
    };

    Ok(EventWindowOutcome {
        merged,
        fetched,
        next_token: page.next_token,
        deferred_gap,
        max_searched,
        min_searched,
        suggested_next_window,
        page,
    })
}

/// Density-aware window-size adaptation for `ExtendDown`.
///
/// - `fetched == 0` → double, up to [`EXTEND_DOWN_MAX_WINDOW`]. On a sparse
///   address this amortises round-trips so we don't walk 6M blocks at 5K/step.
/// - `fetched >= EVENT_PAGE_LIMIT` → halve, down to [`EXTEND_DOWN_MIN_WINDOW`].
///   The bloom cap capped this page; a smaller next window is less likely to
///   truncate.
/// - otherwise → keep the current size.
fn suggest_next_window(current: u64, fetched: usize) -> u64 {
    let next = if fetched == 0 {
        current.saturating_mul(2)
    } else if fetched >= EVENT_PAGE_LIMIT as usize {
        current / 2
    } else {
        current
    };
    next.clamp(EXTEND_DOWN_MIN_WINDOW, EXTEND_DOWN_MAX_WINDOW)
}

/// Empty page for short-circuit paths (e.g. ExtendDown past genesis).
fn empty_page() -> AddressActivityPage {
    AddressActivityPage {
        events: Vec::new(),
        tx_rows: Vec::new(),
        unique_hashes: Vec::new(),
        tx_block_map: std::collections::HashMap::new(),
        next_token: None,
    }
}

/// Build an [`AddressActivityPage`] from a bare event list (RPC fallback).
///
/// `tx_rows` is empty because the RPC path doesn't bulk-fetch tx bodies;
/// `unique_hashes` and `tx_block_map` are derived so downstream code that
/// only needs (tx_hash, block) pairs works uniformly. The `next_token` is
/// always `None` — the RPC path doesn't produce pf-style continuation
/// tokens.
fn page_from_events(events: Vec<SnEvent>) -> AddressActivityPage {
    let mut unique_hashes: Vec<Felt> = Vec::with_capacity(events.len());
    let mut seen: std::collections::HashSet<Felt> = std::collections::HashSet::new();
    let mut tx_block_map: std::collections::HashMap<Felt, u64> = std::collections::HashMap::new();
    for e in &events {
        if e.transaction_hash != Felt::ZERO && seen.insert(e.transaction_hash) {
            unique_hashes.push(e.transaction_hash);
        }
        tx_block_map
            .entry(e.transaction_hash)
            .or_insert(e.block_number);
    }
    AddressActivityPage {
        events,
        tx_rows: Vec::new(),
        unique_hashes,
        tx_block_map,
        next_token: None,
    }
}

/// Compute `(from_block, deferred_gap)` for a TopDelta fetch.
///
/// Split out so the branching logic is unit-testable without a pf client.
fn resolve_top_delta(progress: Option<(u64, u64)>, latest_block: u64) -> (u64, Option<(u64, u64)>) {
    let Some((_min, max_searched)) = progress else {
        // Cold cache: fetch from a tip window only — don't walk history.
        let from = latest_block.saturating_sub(TIP_ONLY_WINDOW_BLOCKS);
        return (from, None);
    };

    if latest_block <= max_searched {
        // Nothing new.
        return (max_searched.saturating_add(1), None);
    }

    let delta = latest_block - max_searched;
    if delta <= EVENT_LARGE_GAP_THRESHOLD_BLOCKS {
        // Small enough — scan contiguously.
        (max_searched.saturating_add(1), None)
    } else {
        // Stale cache: only scan the tip, record the gap.
        let tip_start = latest_block.saturating_sub(TIP_ONLY_WINDOW_BLOCKS);
        let gap = (max_searched.saturating_add(1), tip_start.saturating_sub(1));
        (tip_start, Some(gap))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_delta_cold_cache_fetches_tip_only() {
        let (from, gap) = resolve_top_delta(None, 9_000_000);
        assert_eq!(from, 9_000_000 - TIP_ONLY_WINDOW_BLOCKS);
        assert_eq!(gap, None);
    }

    #[test]
    fn top_delta_small_gap_fetches_contiguous() {
        // max_searched = 8_900_000, latest = 8_950_000 → delta 50k < threshold.
        let (from, gap) = resolve_top_delta(Some((3_000_000, 8_900_000)), 8_950_000);
        assert_eq!(from, 8_900_001);
        assert_eq!(gap, None);
    }

    #[test]
    fn top_delta_large_gap_defers_backfill() {
        // max_searched = 5_000_000, latest = 9_000_000 → delta 4M >> threshold.
        let (from, gap) = resolve_top_delta(Some((3_000_000, 5_000_000)), 9_000_000);
        let tip_start = 9_000_000 - TIP_ONLY_WINDOW_BLOCKS;
        assert_eq!(from, tip_start);
        assert_eq!(gap, Some((5_000_001, tip_start - 1)));
    }

    #[test]
    fn top_delta_no_new_blocks_is_noop() {
        let (from, gap) = resolve_top_delta(Some((0, 9_000_000)), 9_000_000);
        assert_eq!(from, 9_000_001); // effectively no-op when fed to pf
        assert_eq!(gap, None);
    }

    #[test]
    fn suggest_next_window_doubles_on_empty_page() {
        assert_eq!(suggest_next_window(5_000, 0), 10_000);
        assert_eq!(suggest_next_window(50_000, 0), 100_000);
    }

    #[test]
    fn suggest_next_window_caps_at_max() {
        assert_eq!(
            suggest_next_window(EXTEND_DOWN_MAX_WINDOW, 0),
            EXTEND_DOWN_MAX_WINDOW
        );
        assert_eq!(
            suggest_next_window(EXTEND_DOWN_MAX_WINDOW / 2 + 1, 0),
            EXTEND_DOWN_MAX_WINDOW
        );
    }

    #[test]
    fn suggest_next_window_halves_on_full_page() {
        assert_eq!(
            suggest_next_window(100_000, EVENT_PAGE_LIMIT as usize),
            50_000
        );
    }

    #[test]
    fn suggest_next_window_floors_at_min() {
        assert_eq!(
            suggest_next_window(EXTEND_DOWN_MIN_WINDOW, EVENT_PAGE_LIMIT as usize),
            EXTEND_DOWN_MIN_WINDOW
        );
    }

    #[test]
    fn suggest_next_window_keeps_on_partial_page() {
        assert_eq!(suggest_next_window(20_000, 37), 20_000);
    }
}
