//! State for the address info view (tx history, balances, calls, events, class history).

use std::collections::HashSet;

use starknet::core::types::Felt;

use crate::app::AddressTab;
use crate::app::actions::Source;
use crate::app::state::TxNavItem;
use crate::data::types::{
    AddressTxSummary, ContractCallSummary, MetaTxIntenderSummary, SnAddressInfo,
};
use crate::decode::events::DecodedEvent;
use crate::network::dune::AddressActivityProbe;
use crate::ui::widgets::stateful_list::StatefulList;

/// Maximum block span a nonce gap can cover before we stop auto-filling it
/// via RPC block scans. Gaps wider than this are deferred to on-demand Dune
/// queries (`run_nonce_gap_fill`). Shared between the detector
/// (`detect_unfilled_gap`) and the filler (`fill_small_nonce_gaps_phase`) so
/// they stay aligned — drift between the two causes silent data loss.
pub const SMALL_GAP_SPAN_BLOCKS: u64 = 50;

/// Passive UI hint derived from the event-window helper's last fetch.
/// Shared across the Calls / Events / MetaTxs tabs because all three
/// project from the same `address_events` + `address_search_progress`
/// cache. Surfaces whatever the helper already computed — no fill logic
/// is wired to it yet (see task #7 scope: passive-only port).
///
/// - `deferred_gap = Some((lo, hi))` ⇒ helper deliberately skipped that
///   block range because the TopDelta delta exceeded
///   `EVENT_LARGE_GAP_THRESHOLD_BLOCKS`. Surfaced as a title suffix so
///   users know the cached event set isn't contiguous.
/// - `min_searched > 0` ⇒ older history exists below the cached floor
///   (useful future signal for an interactive ExtendDown trigger).
#[derive(Clone, Debug, Default)]
pub struct EventWindowHint {
    /// Lowest block ever scanned for this address. `0` ⇒ reached genesis
    /// or never scanned.
    pub min_searched: u64,
    /// Highest block ever scanned. Anchors the "is there more tip to fetch"
    /// question on the next TopDelta.
    pub max_searched: u64,
    /// Deliberately skipped block range from the last TopDelta fetch.
    /// `Some((lo, hi))` where `lo..=hi` is unscanned.
    pub deferred_gap: Option<(u64, u64)>,
}

/// A detected, unfilled nonce gap in the tx list that has been deferred
/// for on-demand filling (issue #10). We do NOT auto-fill large gaps on
/// address load; instead we wait until the user scrolls toward the gap
/// boundary before burning RPC/Dune budget on it.
#[derive(Clone, Debug)]
pub struct UnfilledGap {
    /// Last known nonce below the gap.
    pub lo_nonce: u64,
    /// First known nonce above the gap.
    pub hi_nonce: u64,
    /// Block number of the lo_nonce tx (lower bound for the scan).
    pub lo_block: u64,
    /// Block number of the hi_nonce tx (upper bound for the scan).
    pub hi_block: u64,
    /// Number of missing nonces between lo and hi (exclusive).
    pub missing_count: u64,
    /// Whether the fill action has already been dispatched for this gap.
    pub fill_dispatched: bool,
}

/// All state related to the address info view.
pub struct AddressInfoState {
    pub info: Option<SnAddressInfo>,
    pub events: StatefulList<DecodedEvent>,
    /// Enriched tx summaries for the Transactions tab.
    pub txs: StatefulList<AddressTxSummary>,
    /// The DEPLOY_ACCOUNT tx that created this address (filtered out of txs).
    pub deployment: Option<AddressTxSummary>,
    /// Incoming calls to this contract (for the Calls tab).
    pub calls: StatefulList<ContractCallSummary>,
    /// Whether this address is a contract (nonce == 0) vs an account.
    pub is_contract: bool,
    /// Whether header is in visual (item-selection) mode.
    pub visual_mode: bool,
    /// Navigable items in the address header (class hash, deploy tx, deploy block, deployer).
    pub nav_items: Vec<TxNavItem>,
    /// Cursor index into nav_items.
    pub nav_cursor: usize,
    /// Class hash upgrade history for the current address.
    pub class_history: Vec<crate::data::pathfinder::ClassHashEntry>,
    /// Selected index in the class history list.
    pub class_history_scroll: usize,
    pub tab: AddressTab,
    /// Track which address we're viewing (for nonce-based cycling).
    pub context: Option<Felt>,
    /// Pagination flag (prevent duplicate fetches).
    pub fetching_more_txs: bool,
    /// Oldest block number in the current tx list (for pagination).
    pub oldest_event_block: Option<u64>,
    /// Tracks which data sources are still in-flight for the current address load.
    pub sources_pending: HashSet<Source>,
    /// Stored probe result — used for density-aware pagination window sizing.
    pub activity_probe: Option<AddressActivityProbe>,
    /// Dune-specific cursor: lowest block from last Dune result.
    pub dune_cursor_block: Option<u64>,
    /// RPC-specific cursor: lowest block from last RPC event fetch.
    pub rpc_cursor_block: Option<u64>,
    /// Whether Dune signaled more data is available (last query returned full batch).
    pub dune_has_more: bool,
    /// Whether RPC signaled more data is available.
    pub rpc_has_more: bool,
    /// Whether a WS subscription is currently active for this address.
    pub ws_subscribed: bool,
    /// Whether the post-display sanity check has already been dispatched.
    pub sanity_check_dispatched: bool,
    /// Detected large nonce gap that has NOT been auto-filled (issue #10).
    /// Set after the initial load settles; filled on-demand when the user
    /// presses Enter on the gap row.
    pub unfilled_gap: Option<UnfilledGap>,
    /// True when the rendered gap row (rather than any tx) is currently selected.
    /// The gap row is a synthetic, standalone entry in the rendered list; the
    /// underlying `txs.state.selected()` does not move while this flag is set.
    pub gap_selected: bool,
    /// ListState fed to ratatui for the Transactions tab. Indexed against the
    /// rendered list (txs + optional gap row), so it diverges from `txs.state`
    /// whenever a gap is showing. Persisted across frames to keep the viewport
    /// offset stable.
    pub txs_render_state: ratatui::widgets::ListState,
    /// Meta-transactions (SNIP-9 outside executions) where this address is the intender.
    pub meta_txs: StatefulList<MetaTxIntenderSummary>,
    /// Pagination flag for MetaTxs tab (prevent duplicate fetches).
    pub fetching_meta_txs: bool,
    /// pf-query event cursor (oldest block - 1) for MetaTxs pagination.
    pub meta_tx_cursor_block: Option<u64>,
    /// Whether pf-query signaled more meta-tx data is available.
    pub meta_tx_has_more: bool,
    /// Whether the initial MetaTxs fetch has been dispatched for the current address.
    pub meta_txs_dispatched: bool,
    /// Lower bound for the MetaTxs bloom scan, preserved across pagination.
    /// Set to deploy block when known; bounds the scan range so pf-query
    /// doesn't time out walking chunks older than the account.
    pub meta_tx_from_block: u64,
    /// Last ExtendDown window size (blocks) used for this address's MetaTxs
    /// fetch. Adapted across pages: doubles on empty windows (sparse
    /// addresses), halves on full pages (dense addresses, bloom cap hit).
    /// Seeded with `EXTEND_DOWN_INITIAL_WINDOW` on tab entry.
    pub meta_tx_last_window: u64,
    /// Shared event-window hint for the event-backed tabs (Calls / Events /
    /// MetaTxs). Updated whenever `ensure_address_events_window` runs.
    /// `None` before any fetch completes.
    pub event_window: Option<EventWindowHint>,
}

/// Auto-fill row target for the event-backed tabs (currently MetaTxs;
/// extends to Calls / Events as those tabs migrate to the shared pipeline).
///
/// Stop conditions for the background auto-continue loop are exactly:
///
///   1. The visible list reaches `AUTO_FILL_TARGET` rows — stop.
///   2. `has_more` drops (history exhausted: pf-query ran out of blocks
///      to scan) — stop. The final count stands, no retry.
///   3. The user navigates away — the session-token cancellation in the
///      network task tears down any in-flight page fetch.
///
/// There is no page-count cap: a sparse address with zero classified
/// meta-txs is allowed to walk back to its deploy block, because the
/// alternative ("give up early") silently under-reports. Scrolling near
/// the bottom re-arms auto-fill (see `maybe_fetch_more_meta_txs`), so
/// history-exhaustion and user-initiated continuation share one path.
pub const AUTO_FILL_TARGET: usize = 50;

impl Default for AddressInfoState {
    fn default() -> Self {
        Self {
            info: None,
            events: StatefulList::new(),
            txs: StatefulList::new(),
            deployment: None,
            calls: StatefulList::new(),
            is_contract: false,
            visual_mode: false,
            nav_items: Vec::new(),
            nav_cursor: 0,
            class_history: Vec::new(),
            class_history_scroll: 0,
            tab: AddressTab::Transactions,
            context: None,
            fetching_more_txs: false,
            oldest_event_block: None,
            sources_pending: HashSet::new(),
            activity_probe: None,
            dune_cursor_block: None,
            rpc_cursor_block: None,
            dune_has_more: false,
            rpc_has_more: false,
            ws_subscribed: false,
            sanity_check_dispatched: false,
            unfilled_gap: None,
            gap_selected: false,
            txs_render_state: ratatui::widgets::ListState::default(),
            meta_txs: StatefulList::new(),
            fetching_meta_txs: false,
            meta_tx_cursor_block: None,
            meta_tx_has_more: false,
            meta_txs_dispatched: false,
            meta_tx_from_block: 0,
            meta_tx_last_window: crate::network::event_window::EXTEND_DOWN_INITIAL_WINDOW,
            event_window: None,
        }
    }
}

impl AddressInfoState {
    /// Clear all address info data. Called when navigating to a new address.
    pub fn clear(&mut self) {
        self.info = None;
        self.events = StatefulList::new();
        self.class_history.clear();
        self.class_history_scroll = 0;
        self.visual_mode = false;
        self.nav_items.clear();
        self.nav_cursor = 0;
        self.tab = AddressTab::Transactions;
        self.txs = StatefulList::new();
        self.deployment = None;
        self.calls = StatefulList::new();
        self.fetching_more_txs = false;
        self.oldest_event_block = None;
        self.sources_pending.clear();
        self.activity_probe = None;
        self.dune_cursor_block = None;
        self.rpc_cursor_block = None;
        self.dune_has_more = false;
        self.rpc_has_more = false;
        self.ws_subscribed = false;
        self.sanity_check_dispatched = false;
        self.unfilled_gap = None;
        self.gap_selected = false;
        self.txs_render_state = ratatui::widgets::ListState::default();
        self.meta_txs = StatefulList::new();
        self.fetching_meta_txs = false;
        self.meta_tx_cursor_block = None;
        self.meta_tx_has_more = false;
        self.meta_txs_dispatched = false;
        self.meta_tx_from_block = 0;
        self.meta_tx_last_window = crate::network::event_window::EXTEND_DOWN_INITIAL_WINDOW;
        self.event_window = None;
    }

    /// Whether any source thinks there is more data to fetch.
    pub fn has_more_data(&self) -> bool {
        self.dune_has_more || self.rpc_has_more
    }

    /// The pagination cursor: the minimum block across all source cursors.
    pub fn pagination_cursor(&self) -> Option<u64> {
        [
            self.dune_cursor_block,
            self.rpc_cursor_block,
            self.oldest_event_block,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Build the list of navigable items for the AddressInfo header.
    pub fn build_nav_items(&mut self) {
        let mut items: Vec<TxNavItem> = Vec::new();

        // Class hash -> ClassInfo view
        if let Some(info) = &self.info
            && let Some(ch) = info.class_hash
        {
            items.push(TxNavItem::ClassHash(ch));
        }

        // Deployment info
        if let Some(deploy) = &self.deployment {
            // Deploy tx (skip if unknown/zero)
            if deploy.hash != Felt::ZERO {
                items.push(TxNavItem::Transaction(deploy.hash));
            }
            // Deploy block
            if deploy.block_number > 0 {
                items.push(TxNavItem::Block(deploy.block_number));
            }
            // Deployer address
            if let Some(sender) = deploy.sender
                && let Some(info) = &self.info
                && sender != info.address
                && sender != Felt::ZERO
            {
                items.push(TxNavItem::Address(sender));
            }
        }

        self.nav_items = items;
        self.nav_cursor = 0;
    }

    /// Step the visual-mode cursor by `delta` (wrapping).
    pub fn nav_step(&mut self, delta: i64) {
        if self.nav_items.is_empty() {
            return;
        }
        let len = self.nav_items.len() as i64;
        let next = (self.nav_cursor as i64 + delta).rem_euclid(len) as usize;
        self.nav_cursor = next;
    }

    /// Split `txs` into regular txs and deployment tx.
    pub fn filter_deployment_txs(
        &mut self,
        address: Felt,
        txs: Vec<AddressTxSummary>,
    ) -> Vec<AddressTxSummary> {
        let mut regular = Vec::with_capacity(txs.len());
        for tx in txs {
            let is_deploy = tx.tx_type == "DEPLOY_ACCOUNT"
                || tx.tx_type.starts_with("DEPLOY")
                || tx.sender.is_some_and(|s| s != address);
            if is_deploy {
                let should_set = self.deployment.is_none()
                    || self
                        .deployment
                        .as_ref()
                        .is_some_and(|d| d.hash == Felt::ZERO);
                if should_set {
                    self.deployment = Some(tx);
                }
            } else {
                regular.push(tx);
            }
        }
        regular
    }

    /// Merge incoming tx summaries into the existing list.
    /// - Upgrades existing entries with better data (status, fee, timestamp, endpoint)
    /// - Appends new entries not yet seen
    /// - Re-sorts by nonce descending
    /// - Preserves the current selection index
    pub fn merge_tx_summaries(&mut self, incoming: Vec<AddressTxSummary>) {
        if incoming.is_empty() {
            return;
        }
        // `seen_hashes` tracks both pre-existing and newly added hashes so that
        // duplicates within the same incoming batch are also collapsed.
        let mut seen_hashes: HashSet<Felt> = self.txs.items.iter().map(|t| t.hash).collect();
        let had_selection = self.txs.state.selected();

        for item in incoming {
            if seen_hashes.contains(&item.hash) {
                if let Some(existing) = self.txs.items.iter_mut().find(|t| t.hash == item.hash) {
                    upgrade_tx_summary(existing, &item);
                }
            } else {
                seen_hashes.insert(item.hash);
                self.txs.items.push(item);
            }
        }

        self.txs.items.sort_by(|a, b| b.nonce.cmp(&a.nonce));

        // Preserve selection
        if let Some(sel) = had_selection {
            if sel < self.txs.items.len() {
                self.txs.state.select(Some(sel));
            }
        } else if !self.txs.items.is_empty() {
            self.txs.select_first();
        }

        // Update oldest block for pagination
        if let Some(oldest) = self
            .txs
            .items
            .iter()
            .filter(|t| t.block_number > 0)
            .map(|t| t.block_number)
            .min()
        {
            self.oldest_event_block = Some(oldest);
        }
    }

    /// Merge incoming contract calls into the existing list and sort by block
    /// number descending. `deduplicate_contract_calls` does the field-level
    /// merge so a later-arriving Dune row can still contribute richer
    /// function_name/fee data to a pf-query row with the same tx_hash (and
    /// vice-versa). Callers are responsible for follow-up work (selection,
    /// cursor updates, persistence).
    pub fn merge_calls(&mut self, incoming: Vec<ContractCallSummary>) {
        if incoming.is_empty() {
            return;
        }
        let mut merged = std::mem::take(&mut self.calls.items);
        merged.extend(incoming);
        self.calls.items = crate::data::types::deduplicate_contract_calls(merged);
        self.calls
            .items
            .sort_by(|a, b| b.block_number.cmp(&a.block_number));
    }

    /// Scan the current tx list for a "large" nonce gap that we want to defer
    /// filling until the user asks for it.
    ///
    /// Returns `Some(gap)` when the biggest contiguous missing-nonce run is large
    /// enough to justify deferring (either many missing entries, or a wide block
    /// span). Small gaps are left to the normal enrichment path and return `None`
    /// so the caller can auto-fill them.
    pub fn detect_unfilled_gap(&self) -> Option<UnfilledGap> {
        // Only meaningful for account txs (contracts don't have sequential nonces).
        if self.is_contract {
            return None;
        }
        // Build sorted (nonce, block) pairs for txs that have landed in a block.
        let mut pairs: Vec<(u64, u64)> = self
            .txs
            .items
            .iter()
            .filter(|t| t.block_number > 0)
            .map(|t| (t.nonce, t.block_number))
            .collect();
        if pairs.len() < 2 {
            return None;
        }
        pairs.sort_by_key(|(n, _)| *n);

        // Find the biggest missing-nonce run.
        let mut best: Option<UnfilledGap> = None;
        for w in pairs.windows(2) {
            let (lo_nonce, lo_block) = w[0];
            let (hi_nonce, hi_block) = w[1];
            let missing = hi_nonce.saturating_sub(lo_nonce).saturating_sub(1);
            if missing == 0 {
                continue;
            }
            let keep = best.as_ref().is_none_or(|b| missing > b.missing_count);
            if keep {
                best = Some(UnfilledGap {
                    lo_nonce,
                    hi_nonce,
                    lo_block,
                    hi_block,
                    missing_count: missing,
                    fill_dispatched: false,
                });
            }
        }
        let gap = best?;

        // Defer any gap the RPC small-gap path can't handle. `fill_small_nonce_gaps_phase`
        // only scans spans ≤ `SMALL_GAP_SPAN_BLOCKS` — anything wider must be
        // filled via Dune on-demand or it would otherwise be dropped silently.
        let span = gap.hi_block.saturating_sub(gap.lo_block);
        if span <= SMALL_GAP_SPAN_BLOCKS {
            return None;
        }
        Some(gap)
    }

    /// Index in `txs.items` of the tx that sits just below the rendered gap
    /// row (i.e. the one with `nonce == gap.lo_nonce`). Returns `None` when
    /// there is no detected gap or the lo-nonce tx isn't in the visible list.
    pub fn gap_pos(&self) -> Option<usize> {
        let gap = self.unfilled_gap.as_ref()?;
        self.txs.items.iter().position(|t| t.nonce == gap.lo_nonce)
    }

    /// Rendered (gap-aware) selection index for the Transactions list — the
    /// position the gap row + tx rows occupy in the rendered viewport.
    pub fn tx_list_rendered_selected(&self) -> Option<usize> {
        let gap = self.gap_pos();
        if self.gap_selected {
            return gap;
        }
        let tx_idx = self.txs.state.selected()?;
        Some(match gap {
            Some(g) if tx_idx >= g => tx_idx + 1,
            _ => tx_idx,
        })
    }

    /// Step the tx-list selection down. If the next step would cross the gap
    /// row, selection lands on the gap row instead. From the gap row, advances
    /// to the tx below it (no fill is dispatched — Enter is the only trigger).
    pub fn tx_list_next(&mut self) {
        if self.txs.items.is_empty() {
            return;
        }
        let gap = self.gap_pos();
        if self.gap_selected {
            self.gap_selected = false;
            if let Some(g) = gap {
                let target = g.min(self.txs.items.len() - 1);
                self.txs.state.select(Some(target));
            } else {
                self.txs.next();
            }
            return;
        }
        if let Some(g) = gap {
            let cur = self.txs.state.selected().unwrap_or(0);
            if cur + 1 == g {
                self.gap_selected = true;
                return;
            }
        }
        self.txs.next();
    }

    /// Step the tx-list selection up. Symmetric to `tx_list_next` w.r.t. the
    /// gap row.
    pub fn tx_list_previous(&mut self) {
        if self.txs.items.is_empty() {
            return;
        }
        let gap = self.gap_pos();
        if self.gap_selected {
            self.gap_selected = false;
            if let Some(g) = gap {
                let target = g.saturating_sub(1);
                self.txs.state.select(Some(target));
            } else {
                self.txs.previous();
            }
            return;
        }
        if let Some(g) = gap {
            let cur = self.txs.state.selected().unwrap_or(0);
            if cur == g {
                self.gap_selected = true;
                return;
            }
        }
        self.txs.previous();
    }

    pub fn tx_list_select_first(&mut self) {
        self.gap_selected = false;
        self.txs.select_first();
    }

    /// Jump to the last tx. If the cursor is currently above the gap row, the
    /// jump clamps on the gap so the user can decide whether to load it or
    /// step past with another keypress.
    pub fn tx_list_select_last(&mut self) {
        if self.txs.items.is_empty() {
            return;
        }
        if let Some(g) = self.gap_pos() {
            let above_gap =
                !self.gap_selected && self.txs.state.selected().is_none_or(|cur| cur < g);
            if above_gap {
                self.gap_selected = true;
                return;
            }
        }
        self.gap_selected = false;
        self.txs.select_last();
    }

    /// Scroll the tx-list selection by `delta`. If the gap row sits between
    /// the current selection and the target, selection clamps on the gap row.
    pub fn tx_list_scroll_by(&mut self, delta: i64) {
        if self.txs.items.is_empty() || delta == 0 {
            return;
        }
        let gap = self.gap_pos();
        let last_tx = self.txs.items.len() as i64 - 1;
        let cur_tx = self.txs.state.selected().unwrap_or(0) as i64;
        let cur_rendered = match (self.gap_selected, gap) {
            (true, Some(g)) => g as i64,
            (false, Some(g)) if cur_tx >= g as i64 => cur_tx + 1,
            _ => cur_tx,
        };
        let rendered_last = last_tx + if gap.is_some() { 1 } else { 0 };
        let target_rendered = (cur_rendered + delta).clamp(0, rendered_last);
        let final_rendered = match gap {
            Some(g) => {
                let g = g as i64;
                if cur_rendered != g && (cur_rendered < g) != (target_rendered < g) {
                    g
                } else {
                    target_rendered
                }
            }
            None => target_rendered,
        };
        match gap {
            Some(g) if final_rendered == g as i64 => {
                self.gap_selected = true;
            }
            Some(g) => {
                self.gap_selected = false;
                let tx_idx = if final_rendered < g as i64 {
                    final_rendered as usize
                } else {
                    (final_rendered - 1) as usize
                };
                self.txs.state.select(Some(tx_idx));
            }
            None => {
                self.gap_selected = false;
                self.txs.state.select(Some(final_rendered as usize));
            }
        }
    }
}

/// Upgrade an existing tx summary with better data from an incoming one.
pub fn upgrade_tx_summary(existing: &mut AddressTxSummary, incoming: &AddressTxSummary) {
    if existing.block_number == 0 && incoming.block_number > 0 {
        existing.block_number = incoming.block_number;
    }
    if existing.status == "?" && incoming.status != "?" {
        existing.status.clone_from(&incoming.status);
    }
    if existing.total_fee_fri == 0 && incoming.total_fee_fri > 0 {
        existing.total_fee_fri = incoming.total_fee_fri;
    }
    if existing.timestamp == 0 && incoming.timestamp > 0 {
        existing.timestamp = incoming.timestamp;
    }
    if existing.endpoint_names.is_empty() && !incoming.endpoint_names.is_empty() {
        existing.endpoint_names.clone_from(&incoming.endpoint_names);
    }
    if existing.called_contracts.is_empty() && !incoming.called_contracts.is_empty() {
        existing
            .called_contracts
            .clone_from(&incoming.called_contracts);
    }
    if existing.sender.is_none() && incoming.sender.is_some() {
        existing.sender = incoming.sender;
    }
    if existing.tip == 0 && incoming.tip > 0 {
        existing.tip = incoming.tip;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::AddressTxSummary;
    use starknet::core::types::Felt;

    fn summary(nonce: u64, block: u64) -> AddressTxSummary {
        AddressTxSummary {
            hash: Felt::from(nonce + 1),
            nonce,
            block_number: block,
            timestamp: 0,
            endpoint_names: String::new(),
            total_fee_fri: 0,
            tip: 0,
            tx_type: "INVOKE".into(),
            status: "OK".into(),
            sender: None,
            called_contracts: Vec::new(),
        }
    }

    fn state_with(txs: Vec<AddressTxSummary>) -> AddressInfoState {
        let mut s = AddressInfoState::default();
        s.txs.items = txs;
        s
    }

    #[test]
    fn no_gap_when_contiguous() {
        let state = state_with(vec![summary(10, 100), summary(11, 102), summary(12, 104)]);
        assert!(state.detect_unfilled_gap().is_none());
    }

    #[test]
    fn no_gap_for_small_span() {
        // Gap spans only 30 blocks — RPC small-gap path handles this.
        let state = state_with(vec![summary(10, 100), summary(16, 130)]);
        assert!(state.detect_unfilled_gap().is_none());
    }

    #[test]
    fn detects_wide_gap_even_with_few_missing() {
        // Only 2 missing nonces but span = 100 blocks > SMALL_GAP_SPAN_BLOCKS.
        // This is the medium-gap case that would otherwise be silently dropped.
        let state = state_with(vec![summary(10, 100), summary(13, 200)]);
        let g = state.detect_unfilled_gap().expect("wide-span gap expected");
        assert_eq!(g.lo_nonce, 10);
        assert_eq!(g.hi_nonce, 13);
        assert_eq!(g.missing_count, 2);
        assert!(!g.fill_dispatched);
    }

    #[test]
    fn detects_gap_by_block_span() {
        // Large block span well above the RPC small-gap limit.
        let state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        let g = state.detect_unfilled_gap().expect("wide-span gap expected");
        assert_eq!(g.lo_nonce, 10);
        assert_eq!(g.hi_nonce, 21);
        assert_eq!(g.missing_count, 10);
    }

    #[test]
    fn gap_threshold_boundary() {
        // Exactly at the threshold: span == SMALL_GAP_SPAN_BLOCKS → no defer.
        let state = state_with(vec![
            summary(10, 100),
            summary(12, 100 + SMALL_GAP_SPAN_BLOCKS),
        ]);
        assert!(state.detect_unfilled_gap().is_none());

        // One over: span == SMALL_GAP_SPAN_BLOCKS + 1 → deferred.
        let state = state_with(vec![
            summary(10, 100),
            summary(12, 100 + SMALL_GAP_SPAN_BLOCKS + 1),
        ]);
        assert!(state.detect_unfilled_gap().is_some());
    }

    #[test]
    fn contract_addresses_never_report_gap() {
        let mut state = state_with(vec![summary(10, 100), summary(200, 500)]);
        state.is_contract = true;
        assert!(state.detect_unfilled_gap().is_none());
    }

    #[test]
    fn reset_clears_unfilled_gap() {
        // Guards the 'r'-refresh path: `NavigateToAddress` calls `reset()`, and
        // the UI hint "retry with 'r'" only works if this wipes
        // `unfilled_gap` + `fill_dispatched` so the next load re-detects.
        let mut state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        state.unfilled_gap = Some(UnfilledGap {
            lo_nonce: 10,
            hi_nonce: 21,
            lo_block: 100,
            hi_block: 2000,
            missing_count: 10,
            fill_dispatched: true,
        });
        state.sanity_check_dispatched = true;
        state.clear();
        assert!(state.unfilled_gap.is_none());
        assert!(!state.sanity_check_dispatched);
    }

    #[test]
    fn biggest_gap_wins_when_multiple() {
        let state = state_with(vec![
            summary(10, 100),
            summary(20, 110),   // 9 missing
            summary(200, 5000), // 179 missing — winner
            summary(201, 5001),
        ]);
        let g = state
            .detect_unfilled_gap()
            .expect("should pick biggest gap");
        assert_eq!(g.lo_nonce, 20);
        assert_eq!(g.hi_nonce, 200);
        assert_eq!(g.missing_count, 179);
    }
}
