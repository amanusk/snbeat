//! State for the transaction detail view (full tx inspection with event tree).

use std::collections::HashSet;

use super::super::state::TxNavItem;
use crate::data::types::{SnReceipt, SnTransaction};
use crate::decode::events::DecodedEvent;
use crate::decode::functions::RawCall;
use crate::decode::outside_execution::OutsideExecutionInfo;
use crate::decode::trace::DecodedTrace;

/// Which body tab is active in the tx detail view.
///
/// `Privacy` is the only tab that's conditionally rendered: it appears when
/// the tx interacts with the Starknet Privacy Pool (see
/// [`crate::decode::privacy::summarize`]). For non-privacy txs the cycle
/// skips it via [`TxTab::next_visible`]/[`TxTab::prev_visible`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxTab {
    #[default]
    Events,
    Calls,
    Transfers,
    Trace,
    Privacy,
}

impl TxTab {
    pub fn next(self) -> Self {
        match self {
            Self::Events => Self::Calls,
            Self::Calls => Self::Transfers,
            Self::Transfers => Self::Trace,
            Self::Trace => Self::Privacy,
            Self::Privacy => Self::Events,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Events => Self::Privacy,
            Self::Calls => Self::Events,
            Self::Transfers => Self::Calls,
            Self::Trace => Self::Transfers,
            Self::Privacy => Self::Trace,
        }
    }

    /// Tab cycling that skips the Privacy tab when the current tx isn't a
    /// privacy tx. `has_privacy` is the caller's view of whether
    /// `decode::privacy::summarize` produced a summary for this tx.
    pub fn next_visible(self, has_privacy: bool) -> Self {
        let n = self.next();
        if matches!(n, Self::Privacy) && !has_privacy {
            n.next()
        } else {
            n
        }
    }
    pub fn prev_visible(self, has_privacy: bool) -> Self {
        let n = self.prev();
        if matches!(n, Self::Privacy) && !has_privacy {
            n.prev()
        } else {
            n
        }
    }
}

/// Which screen region a nav item lives in. Drives auto-scroll + tab-switching
/// when visual-mode steps to an item outside the active tab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NavSection {
    Header,
    Events,
    Calls,
    Transfers,
    Trace,
    Privacy,
}

/// All state related to the transaction detail view.
#[derive(Default)]
pub struct TxDetailState {
    pub transaction: Option<SnTransaction>,
    pub receipt: Option<SnReceipt>,
    pub decoded_events: Vec<DecodedEvent>,
    pub decoded_calls: Vec<RawCall>,
    /// Which tab is currently active in the body (Events default, Calls, Trace).
    pub active_tab: TxTab,
    /// Per-tab scroll offsets so switching tabs preserves scroll position.
    pub events_scroll: u16,
    pub calls_scroll: u16,
    pub transfers_scroll: u16,
    pub trace_scroll: u16,
    pub privacy_scroll: u16,
    /// Decoded recursive call tree (populated lazily after the main fetch).
    pub trace: Option<DecodedTrace>,
    /// True from the moment we trigger the trace fetch until it lands.
    pub trace_loading: bool,
    /// Navigable items in the current tx (rebuilt on load).
    pub nav_items: Vec<TxNavItem>,
    /// Section each nav_item belongs to. Aligned with `nav_items` 1:1; used by
    /// `nav_step()` to switch tabs and pick the right per-tab scroll to update.
    pub nav_sections: Vec<NavSection>,
    /// Cursor index into nav_items (only meaningful when visual_mode is true).
    pub nav_cursor: usize,
    /// First line index for each nav_items entry (written by the renderer each
    /// frame). Line numbers are relative to the section the item is in: header
    /// items use absolute line within the header paragraph; tab-section items
    /// use the line within that tab's paragraph.
    pub nav_item_lines: Vec<u16>,
    /// Whether visual mode (item selection) is active.
    pub visual_mode: bool,
    /// Whether to expand raw calldata felts under each call.
    pub show_calldata: bool,
    /// Whether to show ABI-decoded calldata under each call.
    pub show_decoded_calldata: bool,
    /// Master "expand everything" toggle (`e`). When true, every truncated
    /// hash is shown in full, structs/arrays/tuples render inline-expanded,
    /// and the Calls tab behaves as if `d` (decoded calldata) and `o`
    /// (outside-exec intent) were also on. Default off — too much noise.
    pub expand_all: bool,
    /// Detected outside executions: (call_index in decoded_calls, parsed info).
    pub outside_executions: Vec<(usize, OutsideExecutionInfo)>,
    /// Whether to show the expanded outside execution intent view.
    pub show_outside_execution: bool,
    pub block_timestamp: Option<u64>,
}

impl TxDetailState {
    /// Clear all tx detail data. Called when navigating to a new transaction.
    pub fn clear(&mut self) {
        self.transaction = None;
        self.receipt = None;
        self.decoded_events = Vec::new();
        self.decoded_calls = Vec::new();
        self.outside_executions = Vec::new();
        self.active_tab = TxTab::default();
        self.events_scroll = 0;
        self.calls_scroll = 0;
        self.transfers_scroll = 0;
        self.trace_scroll = 0;
        self.privacy_scroll = 0;
        self.trace = None;
        self.trace_loading = false;
        self.visual_mode = false;
        self.show_outside_execution = false;
        self.expand_all = false;
        self.nav_items = Vec::new();
        self.nav_sections = Vec::new();
        self.nav_cursor = 0;
        self.nav_item_lines = Vec::new();
        self.block_timestamp = None;
    }

    /// Build the list of navigable items for the current transaction.
    /// Order: header (block, sender, declared class, deployed, call targets,
    /// outside-exec) → events → trace. Tagging each push with its section
    /// lets `nav_step()` switch tabs and scroll the right region as the
    /// cursor cycles.
    ///
    /// `private_notes` / `private_nullifiers` are passed in so the Privacy
    /// section can register the counterparties/users/tokens of decrypted +
    /// spent notes (which only exist for users we hold a viewing key for).
    pub fn build_nav_items<F>(
        &mut self,
        is_known: F,
        private_notes: &std::collections::HashMap<
            starknet::core::types::Felt,
            crate::decode::privacy_sync::DecryptedNote,
        >,
        private_nullifiers: &std::collections::HashMap<
            starknet::core::types::Felt,
            starknet::core::types::Felt,
        >,
    ) where
        F: Fn(&starknet::core::types::Felt) -> bool,
    {
        let mut items: Vec<TxNavItem> = Vec::new();
        let mut sections: Vec<NavSection> = Vec::new();
        // Dedup model: Header items dedupe globally (Header is visible from
        // every tab, so each header address should appear at most once and
        // doesn't need a per-tab duplicate). Every other section dedupes
        // within itself only, but skips addresses that are already in
        // Header — so e.g. a token contract that emits a Transfer event will
        // appear under Events AND Transfers (both tabs render it), but a
        // sender already in Header won't be duplicated everywhere.
        let mut in_header: HashSet<starknet::core::types::Felt> = HashSet::new();
        let mut in_calls: HashSet<starknet::core::types::Felt> = HashSet::new();
        let mut in_events: HashSet<starknet::core::types::Felt> = HashSet::new();
        let mut in_transfers: HashSet<starknet::core::types::Felt> = HashSet::new();
        let mut in_privacy: HashSet<starknet::core::types::Felt> = HashSet::new();
        let mut in_trace: HashSet<starknet::core::types::Felt> = HashSet::new();

        let push = |item: TxNavItem,
                    section: NavSection,
                    items: &mut Vec<TxNavItem>,
                    sections: &mut Vec<NavSection>| {
            items.push(item);
            sections.push(section);
        };

        // === Header ===
        // Block number
        let block_num = self
            .receipt
            .as_ref()
            .map(|r| r.block_number)
            .or_else(|| self.transaction.as_ref().map(|tx| tx.block_number()));
        if let Some(n) = block_num {
            push(
                TxNavItem::Block(n),
                NavSection::Header,
                &mut items,
                &mut sections,
            );
        }

        // Sender
        if let Some(tx) = &self.transaction {
            let sender = tx.sender();
            if in_header.insert(sender) {
                push(
                    TxNavItem::Address(sender),
                    NavSection::Header,
                    &mut items,
                    &mut sections,
                );
            }
            // Class hash for Declare txs
            if let crate::data::types::SnTransaction::Declare(decl) = tx {
                push(
                    TxNavItem::ClassHash(decl.class_hash),
                    NavSection::Header,
                    &mut items,
                    &mut sections,
                );
            }
        }

        // Deployed contract addresses (via UDC) — rendered in the header's
        // "Contracts Deployed" section, so they're navigable from there.
        for addr in crate::decode::events::extract_deployed_addresses(&self.decoded_events) {
            if in_header.insert(addr) {
                push(
                    TxNavItem::Address(addr),
                    NavSection::Header,
                    &mut items,
                    &mut sections,
                );
            }
        }

        // Outside execution intender is shown in the header's META line.
        for (_, oe) in &self.outside_executions {
            if in_header.insert(oe.intender) {
                push(
                    TxNavItem::Address(oe.intender),
                    NavSection::Header,
                    &mut items,
                    &mut sections,
                );
            }
        }

        // Helper for non-Header sections: dedupe within the section but
        // skip addresses already covered by Header (since Header is
        // visible from every tab, those would just be redundant stops).
        let push_section = |felt: starknet::core::types::Felt,
                            section: NavSection,
                            section_seen: &mut HashSet<starknet::core::types::Felt>,
                            in_header: &HashSet<starknet::core::types::Felt>,
                            items: &mut Vec<TxNavItem>,
                            sections: &mut Vec<NavSection>| {
            if felt == starknet::core::types::Felt::ZERO {
                return;
            }
            if in_header.contains(&felt) {
                return;
            }
            if section_seen.insert(felt) {
                push(TxNavItem::Address(felt), section, items, sections);
            }
        };

        // === Calls tab ===
        for call in &self.decoded_calls {
            push_section(
                call.contract_address,
                NavSection::Calls,
                &mut in_calls,
                &in_header,
                &mut items,
                &mut sections,
            );
        }
        // OE inner calls — only revealed when the Calls tab toggles `o`,
        // so they belong there.
        for (_, oe) in &self.outside_executions {
            for inner in &oe.inner_calls {
                push_section(
                    inner.contract_address,
                    NavSection::Calls,
                    &mut in_calls,
                    &in_header,
                    &mut items,
                    &mut sections,
                );
            }
        }

        // === Events tab ===
        for event in &self.decoded_events {
            push_section(
                event.contract_address,
                NavSection::Events,
                &mut in_events,
                &in_header,
                &mut items,
                &mut sections,
            );
        }
        // Address-typed event params + untyped params that resolve to known labels.
        for event in &self.decoded_events {
            for p in event.decoded_keys.iter().chain(event.decoded_data.iter()) {
                let type_name = p.type_name.as_deref().unwrap_or("");
                let is_address_type = type_name.contains("ContractAddress");
                let is_known_label = !is_address_type
                    && type_name.is_empty()
                    && p.value != starknet::core::types::Felt::ZERO
                    && is_known(&p.value);
                if is_address_type || is_known_label {
                    push_section(
                        p.value,
                        NavSection::Events,
                        &mut in_events,
                        &in_header,
                        &mut items,
                        &mut sections,
                    );
                }
            }
        }

        // === Transfers tab ===
        // Token / from / to addresses from Transfer events. These can
        // overlap with Events (token contracts emit the Transfer event)
        // and Calls (sender / fee_token are usually call targets too),
        // but since each tab dedupes independently they end up
        // selectable from the Transfers tab too.
        if let Some(trace) = &self.trace {
            let groups = trace.collect_transfers();
            for row in groups
                .validate
                .iter()
                .chain(groups.constructor.iter())
                .chain(groups.execute_top.iter())
                .chain(groups.execute_calls.iter().flat_map(|g| g.transfers.iter()))
                .chain(groups.l1_handler.iter())
                .chain(groups.fee.iter())
            {
                for felt in [row.token, row.from, row.to] {
                    push_section(
                        felt,
                        NavSection::Transfers,
                        &mut in_transfers,
                        &in_header,
                        &mut items,
                        &mut sections,
                    );
                }
            }
        }

        // === Privacy tab ===
        // Compute the privacy summary once and walk it for nav-able addresses
        // (deposit user_addr / withdrawal to_addr / open-note depositor /
        // viewing-key user_addr / invoke-external target / OE intender). The
        // pool address itself is already covered by the Calls section.
        let oe_clone: Vec<crate::decode::outside_execution::OutsideExecutionInfo> = self
            .outside_executions
            .iter()
            .map(|(_, oe)| oe.clone())
            .collect();
        if let Some(tx) = &self.transaction
            && let Some(summary) = crate::decode::privacy::summarize(
                tx,
                &self.decoded_calls,
                &self.decoded_events,
                &oe_clone,
            )
        {
            let mut p = |felt: starknet::core::types::Felt,
                         items: &mut Vec<TxNavItem>,
                         sections: &mut Vec<NavSection>| {
                push_section(
                    felt,
                    NavSection::Privacy,
                    &mut in_privacy,
                    &in_header,
                    items,
                    sections,
                );
            };
            for d in &summary.deposits {
                p(d.user_addr, &mut items, &mut sections);
                p(d.token, &mut items, &mut sections);
            }
            for w in &summary.withdrawals {
                p(w.to_addr, &mut items, &mut sections);
                p(w.token, &mut items, &mut sections);
            }
            for n in &summary.open_notes_created {
                p(n.token, &mut items, &mut sections);
            }
            for d in &summary.open_notes_deposited {
                p(d.depositor, &mut items, &mut sections);
                p(d.token, &mut items, &mut sections);
            }
            for v in &summary.viewing_keys_set {
                p(v.user_addr, &mut items, &mut sections);
            }
            if let Some(ie) = &summary.invoke_external {
                p(ie.target, &mut items, &mut sections);
            }
            if let Some(intender) = summary.intender {
                p(intender, &mut items, &mut sections);
            }

            // Decrypted notes (created in this tx, for users we hold a
            // viewing key for): expose the note's user, counterparty, and
            // token so v-mode can step onto each of them — without this,
            // the recipient label on the "Decrypted (viewing keys)" rows
            // (e.g. the receiver of an outgoing transfer) isn't selectable.
            for nid in &summary.enc_notes_created {
                if let Some(n) = private_notes.get(nid) {
                    p(n.user, &mut items, &mut sections);
                    p(n.counterparty, &mut items, &mut sections);
                    p(n.token, &mut items, &mut sections);
                }
            }
            // Spent notes (this tx's nullifiers that match notes we know):
            // same idea — the "Spent notes" rows render user + counterparty
            // + token, so all three need to be in nav_items.
            for nul in &summary.nullifiers {
                if let Some(nid) = private_nullifiers.get(nul)
                    && let Some(n) = private_notes.get(nid)
                {
                    p(n.user, &mut items, &mut sections);
                    p(n.counterparty, &mut items, &mut sections);
                    p(n.token, &mut items, &mut sections);
                }
            }
        }

        // === Trace tab ===
        if let Some(trace) = &self.trace {
            trace.for_each_call(|call| {
                push_section(
                    call.contract_address,
                    NavSection::Trace,
                    &mut in_trace,
                    &in_header,
                    &mut items,
                    &mut sections,
                );
                for ep in call
                    .events
                    .iter()
                    .flat_map(|e| e.decoded_keys.iter().chain(e.decoded_data.iter()))
                {
                    let type_name = ep.type_name.as_deref().unwrap_or("");
                    let is_address_type = type_name.contains("ContractAddress");
                    let is_known_label = !is_address_type
                        && type_name.is_empty()
                        && ep.value != starknet::core::types::Felt::ZERO
                        && is_known(&ep.value);
                    if is_address_type || is_known_label {
                        push_section(
                            ep.value,
                            NavSection::Trace,
                            &mut in_trace,
                            &in_header,
                            &mut items,
                            &mut sections,
                        );
                    }
                }
            });
        }

        self.nav_items = items;
        self.nav_sections = sections;
        self.nav_cursor = 0;
        self.nav_item_lines = Vec::new();
    }

    /// Read-only access to the active tab's scroll offset.
    pub fn active_scroll(&self) -> u16 {
        match self.active_tab {
            TxTab::Events => self.events_scroll,
            TxTab::Calls => self.calls_scroll,
            TxTab::Transfers => self.transfers_scroll,
            TxTab::Trace => self.trace_scroll,
            TxTab::Privacy => self.privacy_scroll,
        }
    }

    /// Mutable access to the active tab's scroll offset (so `select_next`,
    /// `select_first`, etc. don't have to know about the tab enum).
    pub fn active_scroll_mut(&mut self) -> &mut u16 {
        match self.active_tab {
            TxTab::Events => &mut self.events_scroll,
            TxTab::Calls => &mut self.calls_scroll,
            TxTab::Transfers => &mut self.transfers_scroll,
            TxTab::Trace => &mut self.trace_scroll,
            TxTab::Privacy => &mut self.privacy_scroll,
        }
    }

    /// `NavSection` that pairs with the currently active tab. Header items are
    /// always navigable in addition to the active tab's section.
    fn active_tab_section(&self) -> NavSection {
        match self.active_tab {
            TxTab::Events => NavSection::Events,
            TxTab::Calls => NavSection::Calls,
            TxTab::Transfers => NavSection::Transfers,
            TxTab::Trace => NavSection::Trace,
            TxTab::Privacy => NavSection::Privacy,
        }
    }

    /// Step the visual-mode cursor by `delta` (wrapping) within the items
    /// visible from the active tab — Header items plus items in the active
    /// tab's section. Tab switching is intentionally NOT done here: j/k
    /// keeps the user on the current tab so the highlight doesn't jump
    /// across tabs mid-cycle (use `Tab`/`Shift+Tab` to change tabs).
    pub fn nav_step(&mut self, delta: i64) {
        if self.nav_items.is_empty() {
            return;
        }
        let active_section = self.active_tab_section();
        let visible: Vec<usize> = self
            .nav_sections
            .iter()
            .enumerate()
            .filter(|(_, s)| matches!(**s, NavSection::Header) || **s == active_section)
            .map(|(i, _)| i)
            .collect();
        if visible.is_empty() {
            return;
        }
        let pos = visible
            .iter()
            .position(|&i| i == self.nav_cursor)
            .unwrap_or(0);
        let len = visible.len() as i64;
        let next_pos = (pos as i64 + delta).rem_euclid(len) as usize;
        self.nav_cursor = visible[next_pos];

        // Scroll the active tab so the item is visible. Header items don't
        // scroll anything (header is fixed-height).
        if let Some(&line) = self.nav_item_lines.get(self.nav_cursor) {
            let target = line.saturating_sub(2);
            match self.nav_sections.get(self.nav_cursor).copied() {
                Some(NavSection::Events) => self.events_scroll = target,
                Some(NavSection::Calls) => self.calls_scroll = target,
                Some(NavSection::Transfers) => self.transfers_scroll = target,
                Some(NavSection::Trace) => self.trace_scroll = target,
                Some(NavSection::Privacy) => self.privacy_scroll = target,
                Some(NavSection::Header) | None => {}
            }
        }
    }

    /// Reset visual-mode cursor to the first item visible from the active tab
    /// (typically a Header item, since Header items come first in
    /// `nav_items`). Used when entering visual mode or when the active tab
    /// changes so the cursor lands somewhere visible.
    pub fn reset_nav_cursor_for_active_tab(&mut self) {
        let active_section = self.active_tab_section();
        if let Some(idx) = self
            .nav_sections
            .iter()
            .position(|s| matches!(*s, NavSection::Header) || *s == active_section)
        {
            self.nav_cursor = idx;
        } else {
            self.nav_cursor = 0;
        }
    }
}
