//! State for the address info view (tx history, balances, calls, events, class history).

use std::collections::{HashMap, HashSet};

use starknet::core::types::Felt;

use crate::app::AddressTab;
use crate::app::actions::Source;
use crate::app::state::TxNavItem;
use crate::data::types::{
    AddressTxSummary, ContractCallSummary, MetaTxIntenderSummary, SnAddressInfo,
};
use crate::decode::events::DecodedEvent;
use crate::network::dune::AddressActivityProbe;
use crate::ui::widgets::address_color::AddressColorMap;
use crate::ui::widgets::stateful_list::StatefulList;

/// Maximum block span a nonce gap can cover before we stop auto-filling it
/// via RPC block scans. Gaps wider than this are deferred to on-demand Dune
/// queries (`run_nonce_gap_fill`). Shared between the detector
/// (`detect_unfilled_gaps`) and the filler (`fill_small_nonce_gaps_phase`) so
/// they stay aligned — drift between the two causes silent data loss.
pub const SMALL_GAP_SPAN_BLOCKS: u64 = 50;

/// Maximum number of txs an on-demand fill pulls per Enter press. Keeping
/// this small (matching the chronological pagination chunk in `app/mod.rs`)
/// makes large-gap fills lazy: each Enter shrinks the gap from its newer
/// edge, leaving a residual gap row that the user can fill again. Without
/// this cap, a 33k-tx gap would fire one giant Dune/Pathfinder query per
/// Enter, which is exactly the behavior we want to avoid.
pub const LARGE_GAP_FILL_CHUNK_TXS: u32 = 50;

/// Detected gaps with no more than this many missing nonces are filled
/// automatically (no Enter required). They still go through the deferred-fill
/// machinery — we just dispatch the fill on the user's behalf so a 1-tx hole
/// in a sparse account never surfaces as a "press Enter" row. Larger gaps
/// stay deferred so we don't burn Dune/Pathfinder queries on multi-thousand
/// tx ranges the user might not care about.
pub const AUTO_FILL_MAX_MISSING: u64 = 1;

/// Cap on the number of tiny gaps auto-dispatched per refresh. A very-active
/// address can have dozens of 1-nonce holes scattered across its history;
/// firing every one at once would saturate Pathfinder and (on the Dune
/// fallback) blow through quota. Remaining tiny gaps still appear as
/// deferred rows, and the next refresh picks them up after the dispatched
/// chunk lands.
pub const MAX_AUTO_FILLS_PER_REFRESH: usize = 5;

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
    /// The deploy tx that created this address. For self-deploys
    /// (DEPLOY_ACCOUNT, sender == address) the same tx also stays in `txs`
    /// since it consumed nonce 0; for UDC-style deploys it's only here.
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
    /// Detected large nonce gaps that have NOT been auto-filled (issue #10).
    /// Each gap renders as its own row between the bordering txs; the user
    /// presses Enter on a gap row to dispatch its fill.
    pub unfilled_gaps: Vec<UnfilledGap>,
    /// `Some(lo_nonce)` when the rendered gap row whose `lo_nonce` matches is
    /// currently selected. The gap row is a synthetic, standalone entry in the
    /// rendered list; the underlying `txs.state.selected()` does not move
    /// while this is set. Identifying by `lo_nonce` (instead of vec index)
    /// keeps the selection stable across re-detection passes that reorder the
    /// gap list.
    pub gap_selected: Option<u64>,
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
    /// Per-sender occurrence counts across `calls.items`. Rebuilt only when
    /// the list length changes (i.e. on merge), so steady-state renders
    /// pay nothing.
    pub call_sender_counts: HashMap<Felt, usize>,
    /// Slot assignments for repeated, non-tagged senders in the Calls tab.
    /// Slots are sticky across rebuilds (HashMap insert is idempotent), so
    /// an address keeps its color once it crosses the 2-occurrence threshold.
    pub call_color_map: AddressColorMap,
    /// Cached `calls.items.len()` from the last color-map rebuild. When this
    /// matches the current length, the cache is up-to-date and renders skip
    /// the rescan entirely.
    pub call_color_processed_len: usize,
    /// Per-contract occurrence counts across `txs.items`'s `called_contracts`.
    /// Drives palette assignment in the Txs tab's Contracts column so repeated
    /// targets pop out, unlabeled one-offs stay neutral, and labeled or
    /// privacy contracts keep their tagged style.
    pub tx_contract_counts: HashMap<Felt, usize>,
    /// Slot assignments for repeated, non-tagged contracts called by this
    /// address's outbound txs. Same idempotent-slot guarantee as
    /// `call_color_map`.
    pub tx_color_map: AddressColorMap,
    /// Cached `txs.items.len()` from the last contract color-map rebuild.
    pub tx_color_processed_len: usize,
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
            unfilled_gaps: Vec::new(),
            gap_selected: None,
            txs_render_state: ratatui::widgets::ListState::default(),
            meta_txs: StatefulList::new(),
            fetching_meta_txs: false,
            meta_tx_cursor_block: None,
            meta_tx_has_more: false,
            meta_txs_dispatched: false,
            meta_tx_from_block: 0,
            meta_tx_last_window: crate::network::event_window::EXTEND_DOWN_INITIAL_WINDOW,
            event_window: None,
            call_sender_counts: HashMap::new(),
            call_color_map: AddressColorMap::new(),
            call_color_processed_len: 0,
            tx_contract_counts: HashMap::new(),
            tx_color_map: AddressColorMap::new(),
            tx_color_processed_len: 0,
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
        self.unfilled_gaps.clear();
        self.gap_selected = None;
        self.txs_render_state = ratatui::widgets::ListState::default();
        self.meta_txs = StatefulList::new();
        self.fetching_meta_txs = false;
        self.meta_tx_cursor_block = None;
        self.meta_tx_has_more = false;
        self.meta_txs_dispatched = false;
        self.meta_tx_from_block = 0;
        self.meta_tx_last_window = crate::network::event_window::EXTEND_DOWN_INITIAL_WINDOW;
        self.event_window = None;
        self.call_sender_counts.clear();
        self.call_color_map = AddressColorMap::new();
        self.call_color_processed_len = 0;
        self.tx_contract_counts.clear();
        self.tx_color_map = AddressColorMap::new();
        self.tx_color_processed_len = 0;
    }

    /// Drop the cached color map so the next render rebuilds it from scratch.
    /// Use this when a call's sender mutates in place (e.g. a `Felt::ZERO`
    /// stub being upgraded by enrichment) — `calls.items.len()` doesn't
    /// change in that case, so the length-keyed fast path in
    /// `update_call_color_map` would otherwise leave stale entries (a
    /// phantom `Felt::ZERO` slot lingering in the color map, real senders
    /// never counted).
    pub fn invalidate_call_color_cache(&mut self) {
        self.call_sender_counts.clear();
        self.call_color_map = AddressColorMap::new();
        self.call_color_processed_len = 0;
    }

    /// Refresh the per-sender count map and color slots for the Calls tab.
    ///
    /// Cheap fast path: when `calls.items.len()` matches the cached length,
    /// nothing has merged since the last rebuild and we return immediately.
    /// On a merge, we rescan the full list — sort interleaving makes index-
    /// based incremental work unsafe, but `AddressColorMap::register` is
    /// idempotent so existing senders keep their slots (and thus their
    /// colors) across rebuilds.
    ///
    /// `is_known` is the registry-tagged predicate; tagged addresses skip
    /// color registration so they keep their `LABEL_STYLE` rendering.
    pub fn update_call_color_map(&mut self, is_known: impl Fn(&Felt) -> bool) {
        if self.call_color_processed_len == self.calls.items.len() {
            return;
        }
        self.call_sender_counts.clear();
        for call in &self.calls.items {
            let count = self.call_sender_counts.entry(call.sender).or_insert(0);
            *count += 1;
            if *count == 2 && !is_known(&call.sender) {
                self.call_color_map.register(call.sender);
            }
        }
        self.call_color_processed_len = self.calls.items.len();
    }

    /// Refresh the per-contract count map and color slots for the Txs tab's
    /// Contracts column. Mirrors `update_call_color_map`: counts occurrences
    /// across every tx's `called_contracts`, and assigns palette slots only
    /// to repeats that aren't already registry-known (labeled contracts keep
    /// their `LABEL_STYLE`; privacy contracts keep `PRIVACY_STYLE`).
    pub fn update_tx_color_map(&mut self, is_known: impl Fn(&Felt) -> bool) {
        if self.tx_color_processed_len == self.txs.items.len() {
            return;
        }
        self.tx_contract_counts.clear();
        for tx in &self.txs.items {
            for contract in &tx.called_contracts {
                let count = self.tx_contract_counts.entry(*contract).or_insert(0);
                *count += 1;
                if *count == 2 && !is_known(contract) {
                    self.tx_color_map.register(*contract);
                }
            }
        }
        self.tx_color_processed_len = self.txs.items.len();
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
    ///
    /// DEPLOY_ACCOUNT txs (where the new account paid for its own deploy,
    /// `sender == address`) consume nonce 0 and are part of the account's
    /// history, so they populate `self.deployment` *and* stay in the regular
    /// list. UDC-style deploys (`sender != address`) are the deployer's tx,
    /// not the deployed contract's, so they're pulled out entirely.
    pub fn filter_deployment_txs(
        &mut self,
        address: Felt,
        txs: Vec<AddressTxSummary>,
    ) -> Vec<AddressTxSummary> {
        let mut regular = Vec::with_capacity(txs.len());
        for tx in txs {
            let self_deploy =
                tx.tx_type == "DEPLOY_ACCOUNT" && tx.sender.is_none_or(|s| s == address);
            let foreign_deploy =
                tx.tx_type.starts_with("DEPLOY") || tx.sender.is_some_and(|s| s != address);
            if self_deploy || foreign_deploy {
                let should_set = self.deployment.is_none()
                    || self
                        .deployment
                        .as_ref()
                        .is_some_and(|d| d.hash == Felt::ZERO);
                if should_set {
                    self.deployment = Some(tx.clone());
                }
                if self_deploy {
                    regular.push(tx);
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

    /// Scan the current tx list for *all* large nonce gaps that should be
    /// deferred until the user asks for them.
    ///
    /// Each returned gap has at least one missing nonce *and* a block span
    /// wider than `SMALL_GAP_SPAN_BLOCKS` (the RPC small-gap path's reach —
    /// narrower gaps stay on the auto-fill path). Sorted by `lo_nonce`
    /// ascending so renderers/tests have a stable order.
    pub fn detect_unfilled_gaps(&self) -> Vec<UnfilledGap> {
        if self.is_contract {
            return Vec::new();
        }
        let mut pairs: Vec<(u64, u64)> = self
            .txs
            .items
            .iter()
            .filter(|t| t.block_number > 0)
            .map(|t| (t.nonce, t.block_number))
            .collect();
        if pairs.len() < 2 {
            return Vec::new();
        }
        pairs.sort_by_key(|(n, _)| *n);

        let mut out = Vec::new();
        for w in pairs.windows(2) {
            let (lo_nonce, lo_block) = w[0];
            let (hi_nonce, hi_block) = w[1];
            let missing = hi_nonce.saturating_sub(lo_nonce).saturating_sub(1);
            if missing == 0 {
                continue;
            }
            // `fill_small_nonce_gaps_phase` only covers spans ≤ SMALL_GAP_SPAN_BLOCKS
            // via RPC scans; anything wider must be filled via Dune on demand
            // or it would be silently dropped.
            let span = hi_block.saturating_sub(lo_block);
            if span <= SMALL_GAP_SPAN_BLOCKS {
                continue;
            }
            out.push(UnfilledGap {
                lo_nonce,
                hi_nonce,
                lo_block,
                hi_block,
                missing_count: missing,
                fill_dispatched: false,
            });
        }
        out
    }

    /// Re-detect gaps and store them. `fill_dispatched` is preserved only for
    /// gaps whose `(lo_nonce, hi_nonce)` is unchanged from the previous list —
    /// a shifted `hi_nonce` is evidence the fill response just landed
    /// (lazy fills always shrink the gap from the newer edge), so we clear
    /// the flag and let the user press Enter again for the next chunk.
    /// Drops the gap selection if the previously-selected gap no longer exists.
    pub fn refresh_unfilled_gaps(&mut self) {
        let prev_inflight: std::collections::HashMap<u64, u64> = self
            .unfilled_gaps
            .iter()
            .filter(|g| g.fill_dispatched)
            .map(|g| (g.lo_nonce, g.hi_nonce))
            .collect();
        let mut next = self.detect_unfilled_gaps();
        for g in next.iter_mut() {
            if let Some(prev_hi) = prev_inflight.get(&g.lo_nonce)
                && *prev_hi == g.hi_nonce
            {
                g.fill_dispatched = true;
            }
        }
        self.unfilled_gaps = next;
        if let Some(sel) = self.gap_selected
            && !self.unfilled_gaps.iter().any(|g| g.lo_nonce == sel)
        {
            self.gap_selected = None;
        }
    }

    /// Render-order positions for the gap rows: pairs of `(tx_idx, lo_nonce)`
    /// sorted by `tx_idx` ascending. Each gap renders immediately above the
    /// tx whose nonce equals `lo_nonce`. Gaps whose `lo_nonce` tx isn't in
    /// `txs.items` are filtered out (defensive — usually all match).
    pub fn gap_render_positions(&self) -> Vec<(usize, u64)> {
        let mut out: Vec<(usize, u64)> = self
            .unfilled_gaps
            .iter()
            .filter_map(|g| {
                self.txs
                    .items
                    .iter()
                    .position(|t| t.nonce == g.lo_nonce)
                    .map(|p| (p, g.lo_nonce))
            })
            .collect();
        out.sort_by_key(|(p, _)| *p);
        out
    }

    fn rendered_len(&self, gaps: &[(usize, u64)]) -> usize {
        self.txs.items.len() + gaps.len()
    }

    fn tx_pos_to_rendered(tx_pos: usize, gaps: &[(usize, u64)]) -> usize {
        tx_pos + gaps.iter().filter(|(p, _)| *p <= tx_pos).count()
    }

    fn gap_rendered_idx(gaps: &[(usize, u64)], idx_in_gaps: usize) -> usize {
        gaps[idx_in_gaps].0 + idx_in_gaps
    }

    fn rendered_to_tx_pos(r: usize, gaps: &[(usize, u64)]) -> usize {
        let gaps_before = gaps
            .iter()
            .enumerate()
            .filter(|(g_idx, (p, _))| p + g_idx < r)
            .count();
        r - gaps_before
    }

    /// Rendered (gap-aware) selection index for the Transactions list.
    pub fn tx_list_rendered_selected(&self) -> Option<usize> {
        let gaps = self.gap_render_positions();
        if let Some(sel_lo) = self.gap_selected
            && let Some(g_idx) = gaps.iter().position(|(_, lo)| *lo == sel_lo)
        {
            return Some(Self::gap_rendered_idx(&gaps, g_idx));
        }
        let tx_idx = self.txs.state.selected()?;
        Some(Self::tx_pos_to_rendered(tx_idx, &gaps))
    }

    /// Currently-selected gap, if the gap row is the active selection.
    pub fn selected_gap(&self) -> Option<&UnfilledGap> {
        let lo = self.gap_selected?;
        self.unfilled_gaps.iter().find(|g| g.lo_nonce == lo)
    }

    fn current_rendered(&self, gaps: &[(usize, u64)]) -> usize {
        if let Some(sel_lo) = self.gap_selected
            && let Some(g_idx) = gaps.iter().position(|(_, lo)| *lo == sel_lo)
        {
            return Self::gap_rendered_idx(gaps, g_idx);
        }
        let t = self.txs.state.selected().unwrap_or(0);
        Self::tx_pos_to_rendered(t, gaps)
    }

    /// First gap rendered idx that lies strictly between `cur` and `target`
    /// (target inclusive). Returns `None` if no gap is crossed.
    fn first_gap_crossed(gaps: &[(usize, u64)], cur: usize, target: usize) -> Option<usize> {
        if cur == target {
            return None;
        }
        if target > cur {
            gaps.iter().enumerate().find_map(|(g_idx, (p, _))| {
                let r = p + g_idx;
                (r > cur && r <= target).then_some(r)
            })
        } else {
            gaps.iter().enumerate().rev().find_map(|(g_idx, (p, _))| {
                let r = p + g_idx;
                (r < cur && r >= target).then_some(r)
            })
        }
    }

    /// Apply a target rendered index to state — selects the gap row when the
    /// index lands on one, otherwise selects the corresponding tx.
    fn apply_rendered(&mut self, r: usize, gaps: &[(usize, u64)]) {
        for (g_idx, (p, lo)) in gaps.iter().enumerate() {
            if p + g_idx == r {
                self.gap_selected = Some(*lo);
                return;
            }
        }
        self.gap_selected = None;
        let t = Self::rendered_to_tx_pos(r, gaps);
        self.txs.state.select(Some(t));
    }

    /// Move selection by `delta` rows in the rendered list, clamping on the
    /// first gap row crossed.
    fn tx_list_step(&mut self, delta: i64) {
        if self.txs.items.is_empty() || delta == 0 {
            return;
        }
        let gaps = self.gap_render_positions();
        let rendered_max = self.rendered_len(&gaps).saturating_sub(1);
        let cur = self.current_rendered(&gaps);
        let target = ((cur as i64) + delta).clamp(0, rendered_max as i64) as usize;
        let final_r = Self::first_gap_crossed(&gaps, cur, target).unwrap_or(target);
        self.apply_rendered(final_r, &gaps);
    }

    pub fn tx_list_next(&mut self) {
        self.tx_list_step(1);
    }

    pub fn tx_list_previous(&mut self) {
        self.tx_list_step(-1);
    }

    /// Jump toward the first row, clamping on the first gap row crossed.
    pub fn tx_list_select_first(&mut self) {
        if self.txs.items.is_empty() {
            return;
        }
        let gaps = self.gap_render_positions();
        let cur = self.current_rendered(&gaps);
        let final_r = Self::first_gap_crossed(&gaps, cur, 0).unwrap_or(0);
        self.apply_rendered(final_r, &gaps);
    }

    /// Jump toward the last row, clamping on the first gap row crossed.
    pub fn tx_list_select_last(&mut self) {
        if self.txs.items.is_empty() {
            return;
        }
        let gaps = self.gap_render_positions();
        let rendered_max = self.rendered_len(&gaps).saturating_sub(1);
        let cur = self.current_rendered(&gaps);
        let final_r = Self::first_gap_crossed(&gaps, cur, rendered_max).unwrap_or(rendered_max);
        self.apply_rendered(final_r, &gaps);
    }

    pub fn tx_list_scroll_by(&mut self, delta: i64) {
        self.tx_list_step(delta);
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
        assert!(state.detect_unfilled_gaps().is_empty());
    }

    #[test]
    fn no_gap_for_small_span() {
        // Gap spans only 30 blocks — RPC small-gap path handles this.
        let state = state_with(vec![summary(10, 100), summary(16, 130)]);
        assert!(state.detect_unfilled_gaps().is_empty());
    }

    #[test]
    fn detects_wide_gap_even_with_few_missing() {
        // Only 2 missing nonces but span = 100 blocks > SMALL_GAP_SPAN_BLOCKS.
        // This is the medium-gap case that would otherwise be silently dropped.
        let state = state_with(vec![summary(10, 100), summary(13, 200)]);
        let gaps = state.detect_unfilled_gaps();
        assert_eq!(gaps.len(), 1);
        let g = &gaps[0];
        assert_eq!(g.lo_nonce, 10);
        assert_eq!(g.hi_nonce, 13);
        assert_eq!(g.missing_count, 2);
        assert!(!g.fill_dispatched);
    }

    #[test]
    fn detects_gap_by_block_span() {
        let state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        let gaps = state.detect_unfilled_gaps();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].lo_nonce, 10);
        assert_eq!(gaps[0].hi_nonce, 21);
        assert_eq!(gaps[0].missing_count, 10);
    }

    #[test]
    fn gap_threshold_boundary() {
        // Exactly at the threshold: span == SMALL_GAP_SPAN_BLOCKS → not deferred.
        let state = state_with(vec![
            summary(10, 100),
            summary(12, 100 + SMALL_GAP_SPAN_BLOCKS),
        ]);
        assert!(state.detect_unfilled_gaps().is_empty());

        // One over → deferred.
        let state = state_with(vec![
            summary(10, 100),
            summary(12, 100 + SMALL_GAP_SPAN_BLOCKS + 1),
        ]);
        assert_eq!(state.detect_unfilled_gaps().len(), 1);
    }

    #[test]
    fn contract_addresses_never_report_gap() {
        let mut state = state_with(vec![summary(10, 100), summary(200, 500)]);
        state.is_contract = true;
        assert!(state.detect_unfilled_gaps().is_empty());
    }

    #[test]
    fn reset_clears_unfilled_gaps() {
        // Guards the 'r'-refresh path: `NavigateToAddress` calls `reset()`,
        // and the next load re-detects from scratch.
        let mut state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        state.unfilled_gaps.push(UnfilledGap {
            lo_nonce: 10,
            hi_nonce: 21,
            lo_block: 100,
            hi_block: 2000,
            missing_count: 10,
            fill_dispatched: true,
        });
        state.sanity_check_dispatched = true;
        state.clear();
        assert!(state.unfilled_gaps.is_empty());
        assert!(!state.sanity_check_dispatched);
    }

    #[test]
    fn detects_every_qualifying_gap() {
        let state = state_with(vec![
            summary(10, 100),
            summary(20, 1100),  // 9 missing, span 1000 → deferred
            summary(22, 1110),  // 1 missing, span 10 → small (skipped)
            summary(200, 5000), // 177 missing, span 3890 → deferred
            summary(201, 5001),
        ]);
        let gaps = state.detect_unfilled_gaps();
        assert_eq!(gaps.len(), 2);
        let los: Vec<u64> = gaps.iter().map(|g| g.lo_nonce).collect();
        assert_eq!(los, vec![10, 22]);
    }

    #[test]
    fn refresh_preserves_dispatched_flag_when_gap_unchanged() {
        let mut state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        state.unfilled_gaps = state.detect_unfilled_gaps();
        state.unfilled_gaps[0].fill_dispatched = true;
        state.refresh_unfilled_gaps();
        assert_eq!(state.unfilled_gaps.len(), 1);
        assert!(state.unfilled_gaps[0].fill_dispatched);
    }

    #[test]
    fn refresh_clears_dispatched_flag_when_gap_shrinks() {
        // Mimics a lazy fill landing: the chunk arrived from the top of the
        // gap (nonces just below `hi_nonce`), shrinking the gap from its
        // newer edge. Same `lo_nonce`, lower `hi_nonce` → flag should clear
        // so the user can Enter on the residual to load the next chunk.
        let mut state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        state.unfilled_gaps = state.detect_unfilled_gaps();
        state.unfilled_gaps[0].fill_dispatched = true;

        // Add a contiguous chunk at the top of the gap (nonces 18, 19, 20).
        // Their blocks are close to 2000 (top edge) but the older boundary
        // gap (10..18) still has a wide block span → still deferred.
        state.txs.items.push(summary(18, 1980));
        state.txs.items.push(summary(19, 1990));
        state.txs.items.push(summary(20, 1995));
        state.refresh_unfilled_gaps();

        assert_eq!(state.unfilled_gaps.len(), 1);
        assert_eq!(state.unfilled_gaps[0].lo_nonce, 10);
        assert_eq!(state.unfilled_gaps[0].hi_nonce, 18);
        assert!(!state.unfilled_gaps[0].fill_dispatched);
    }

    #[test]
    fn refresh_drops_stale_gap_selection() {
        let mut state = state_with(vec![summary(10, 100), summary(21, 2000)]);
        state.unfilled_gaps = state.detect_unfilled_gaps();
        state.gap_selected = Some(10);
        // Replace tx data so the gap closes.
        state.txs.items = (10..=21).map(|n| summary(n, 100 + n)).collect();
        state.refresh_unfilled_gaps();
        assert!(state.unfilled_gaps.is_empty());
        assert!(state.gap_selected.is_none());
    }

    /// Build a state with a single deferred gap between hi/lo nonces. Tx list
    /// is sorted nonce-descending, matching the live-render layout.
    fn state_one_gap() -> AddressInfoState {
        // Nonces 100, 99, 30, 29 — gap between 30 and 99 (block span 4790).
        let mut state = state_with(vec![
            summary(100, 5000),
            summary(99, 4990),
            summary(30, 200),
            summary(29, 190),
        ]);
        state.refresh_unfilled_gaps();
        assert_eq!(state.unfilled_gaps.len(), 1);
        state.txs.state.select(Some(0));
        state
    }

    #[test]
    fn next_lands_on_gap_when_crossing_forward() {
        let mut state = state_one_gap();
        // Start at nonce 99 (tx idx 1), the tx just above the gap.
        state.txs.state.select(Some(1));
        state.tx_list_next();
        assert_eq!(state.gap_selected, Some(30));
        assert_eq!(state.txs.state.selected(), Some(1));
    }

    #[test]
    fn next_off_gap_lands_on_lo_nonce_tx_no_dispatch() {
        let mut state = state_one_gap();
        state.gap_selected = Some(30);
        state.tx_list_next();
        // Cleared gap selection, landed on the lo_nonce tx (nonce 30, idx 2).
        assert_eq!(state.gap_selected, None);
        assert_eq!(state.txs.state.selected(), Some(2));
        // Crucially, no fill_dispatched — Enter alone triggers fills.
        assert!(!state.unfilled_gaps[0].fill_dispatched);
    }

    #[test]
    fn previous_lands_on_gap_when_crossing_backward() {
        let mut state = state_one_gap();
        // Start at nonce 30 (tx idx 2), the tx just below the gap.
        state.txs.state.select(Some(2));
        state.tx_list_previous();
        assert_eq!(state.gap_selected, Some(30));
        assert_eq!(state.txs.state.selected(), Some(2));
    }

    #[test]
    fn previous_off_gap_lands_on_hi_nonce_tx() {
        let mut state = state_one_gap();
        state.gap_selected = Some(30);
        state.tx_list_previous();
        assert_eq!(state.gap_selected, None);
        assert_eq!(state.txs.state.selected(), Some(1));
    }

    #[test]
    fn scroll_by_clamps_on_gap_forward() {
        let mut state = state_one_gap();
        state.txs.state.select(Some(0));
        state.tx_list_scroll_by(10);
        // Should clamp on the gap row, not jump past it.
        assert_eq!(state.gap_selected, Some(30));
    }

    #[test]
    fn scroll_by_clamps_on_gap_backward() {
        let mut state = state_one_gap();
        state.txs.state.select(Some(3));
        state.tx_list_scroll_by(-10);
        assert_eq!(state.gap_selected, Some(30));
    }

    #[test]
    fn select_first_clamps_on_gap_from_below() {
        let mut state = state_one_gap();
        state.txs.state.select(Some(3));
        state.tx_list_select_first();
        assert_eq!(state.gap_selected, Some(30));
    }

    #[test]
    fn select_last_clamps_on_gap_from_above() {
        let mut state = state_one_gap();
        state.txs.state.select(Some(0));
        state.tx_list_select_last();
        assert_eq!(state.gap_selected, Some(30));
    }

    #[test]
    fn navigation_is_a_noop_with_no_gap() {
        let mut state = state_with(vec![summary(10, 100), summary(11, 102)]);
        state.refresh_unfilled_gaps();
        assert!(state.unfilled_gaps.is_empty());
        state.txs.state.select(Some(0));
        state.tx_list_next();
        assert_eq!(state.gap_selected, None);
        assert_eq!(state.txs.state.selected(), Some(1));
    }

    /// Two deferred gaps — single-step navigation should pause at each.
    fn state_two_gaps() -> AddressInfoState {
        // Nonces 200, 199, 100, 99, 30, 29 — gaps between 100..199 and 30..99.
        let mut state = state_with(vec![
            summary(200, 9010),
            summary(199, 9000),
            summary(100, 5000),
            summary(99, 4990),
            summary(30, 200),
            summary(29, 190),
        ]);
        state.refresh_unfilled_gaps();
        assert_eq!(state.unfilled_gaps.len(), 2);
        state.txs.state.select(Some(0));
        state
    }

    #[test]
    fn scroll_by_clamps_on_first_of_multiple_gaps_forward() {
        let mut state = state_two_gaps();
        state.tx_list_scroll_by(20);
        // First gap encountered going down from the top is the upper one.
        assert_eq!(state.gap_selected, Some(100));
    }

    #[test]
    fn scroll_by_from_first_gap_clamps_on_second() {
        let mut state = state_two_gaps();
        state.gap_selected = Some(100);
        state.tx_list_scroll_by(20);
        assert_eq!(state.gap_selected, Some(30));
    }

    #[test]
    fn step_walks_through_each_gap() {
        let mut state = state_two_gaps();
        // Start on the tx just above the upper gap (nonce 199, idx 1).
        state.txs.state.select(Some(1));
        state.tx_list_next(); // lands on upper gap
        assert_eq!(state.gap_selected, Some(100));
        state.tx_list_next(); // off gap → tx with nonce 100 (idx 2)
        assert_eq!(state.gap_selected, None);
        assert_eq!(state.txs.state.selected(), Some(2));
        state.tx_list_next(); // tx idx 3 (nonce 99) — just above lower gap
        assert_eq!(state.txs.state.selected(), Some(3));
        state.tx_list_next(); // lands on lower gap
        assert_eq!(state.gap_selected, Some(30));
    }

    #[test]
    fn rendered_selected_tracks_gap_and_tx_state() {
        let mut state = state_two_gaps();
        // Tx idx 0 (nonce 200) → rendered 0.
        state.txs.state.select(Some(0));
        assert_eq!(state.tx_list_rendered_selected(), Some(0));
        // Tx idx 3 (nonce 99) → rendered 4 (one upper-gap row in front of it).
        state.txs.state.select(Some(3));
        assert_eq!(state.tx_list_rendered_selected(), Some(4));
        // Selecting the upper gap → rendered 2.
        state.gap_selected = Some(100);
        assert_eq!(state.tx_list_rendered_selected(), Some(2));
        // Selecting the lower gap → rendered 5.
        state.gap_selected = Some(30);
        assert_eq!(state.tx_list_rendered_selected(), Some(5));
    }

    fn call_summary(sender: Felt, block: u64) -> ContractCallSummary {
        ContractCallSummary {
            tx_hash: Felt::from(block * 31 + 1),
            sender,
            function_name: String::new(),
            block_number: block,
            timestamp: 0,
            total_fee_fri: 0,
            status: "OK".into(),
            nonce: None,
            tip: 0,
            inner_targets: Vec::new(),
        }
    }

    /// Builds a synthetic call list with `n` rows. ~30% of senders are unique
    /// one-shots; the remaining 70% are drawn from a pool of 50 repeating
    /// senders (the "hot" set the color map should pick up). Hot-pool indices
    /// step by a coprime stride so every slot gets used as `n` grows.
    fn build_call_corpus(n: usize) -> Vec<ContractCallSummary> {
        const POOL_SIZE: usize = 50;
        let hot_pool: Vec<Felt> = (0..POOL_SIZE)
            .map(|i| Felt::from(0x1000_u64 + i as u64))
            .collect();
        let mut hot_cursor: usize = 0;
        (0..n)
            .map(|i| {
                let sender = if i % 10 < 3 {
                    Felt::from(0x9000_0000_u64 + i as u64)
                } else {
                    let s = hot_pool[hot_cursor % POOL_SIZE];
                    // 7 is coprime to 50 → cycles through every slot.
                    hot_cursor = hot_cursor.wrapping_add(7);
                    s
                };
                call_summary(sender, (n - i) as u64)
            })
            .collect()
    }

    #[test]
    #[ignore = "release-mode microbenchmark; run with `cargo test --release bench_update_call_color_map -- --ignored --nocapture`"]
    fn bench_update_call_color_map() {
        // Marked `#[ignore]` so the default `cargo test` run (typically debug)
        // doesn't flake on the 100ms ceiling. Run explicitly in release with
        // `--ignored` to print timings.
        let sizes = [1_000usize, 5_000, 10_000, 25_000];
        for &n in &sizes {
            let corpus = build_call_corpus(n);

            // Cold rebuild: full rescan from scratch (worst case — every merge).
            let mut state = AddressInfoState::default();
            state.calls.items = corpus.clone();
            let t0 = std::time::Instant::now();
            state.update_call_color_map(|_| false);
            let cold = t0.elapsed();

            // Warm path: items unchanged, length-equal early-out.
            let t1 = std::time::Instant::now();
            for _ in 0..1_000 {
                state.update_call_color_map(|_| false);
            }
            let warm_avg = t1.elapsed() / 1_000;

            // Sanity: the hot pool of 50 repeated senders should all have
            // crossed the count==2 threshold and registered.
            let registered = state.call_color_map.slots_count();
            assert!(
                registered >= 50,
                "expected ≥50 registered (hot pool), got {} at n={}",
                registered,
                n
            );

            println!(
                "n={:>5} cold_rebuild={:>8.3?} warm_per_call={:>8.3?} registered={}",
                n, cold, warm_avg, registered
            );

            // Hard ceiling: even 25k items should rebuild well under 100 ms
            // on any modern machine. Anything above this means the design
            // has regressed.
            assert!(
                cold < std::time::Duration::from_millis(100),
                "cold rebuild for n={} took {:?}, exceeds 100 ms budget",
                n,
                cold
            );
        }
    }
}
