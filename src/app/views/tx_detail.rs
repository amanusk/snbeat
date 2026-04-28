//! State for the transaction detail view (full tx inspection with event tree).

use std::collections::HashSet;

use super::super::state::TxNavItem;
use crate::data::types::{SnReceipt, SnTransaction};
use crate::decode::events::DecodedEvent;
use crate::decode::functions::RawCall;
use crate::decode::outside_execution::OutsideExecutionInfo;
use crate::decode::trace::DecodedTrace;

/// Which body tab is active in the tx detail view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxTab {
    #[default]
    Events,
    Calls,
    Transfers,
    Trace,
}

impl TxTab {
    pub fn next(self) -> Self {
        match self {
            Self::Events => Self::Calls,
            Self::Calls => Self::Transfers,
            Self::Transfers => Self::Trace,
            Self::Trace => Self::Events,
        }
    }
    pub fn prev(self) -> Self {
        match self {
            Self::Events => Self::Trace,
            Self::Calls => Self::Events,
            Self::Transfers => Self::Calls,
            Self::Trace => Self::Transfers,
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
    pub fn build_nav_items<F>(&mut self, is_known: F)
    where
        F: Fn(&starknet::core::types::Felt) -> bool,
    {
        let mut items: Vec<TxNavItem> = Vec::new();
        let mut sections: Vec<NavSection> = Vec::new();
        let mut seen: HashSet<starknet::core::types::Felt> = HashSet::new();

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
            if seen.insert(sender) {
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
            if seen.insert(addr) {
                push(
                    TxNavItem::Address(addr),
                    NavSection::Header,
                    &mut items,
                    &mut sections,
                );
            }
        }

        // === Calls tab ===
        // Call contract addresses
        for call in &self.decoded_calls {
            if seen.insert(call.contract_address) {
                push(
                    TxNavItem::Address(call.contract_address),
                    NavSection::Calls,
                    &mut items,
                    &mut sections,
                );
            }
        }

        // Outside execution: the intender is shown in the header's META line
        // (always visible), so it's a Header-section nav item. Inner calls are
        // only revealed when the Calls tab toggles `o`, so they belong there.
        for (_, oe) in &self.outside_executions {
            if seen.insert(oe.intender) {
                push(
                    TxNavItem::Address(oe.intender),
                    NavSection::Header,
                    &mut items,
                    &mut sections,
                );
            }
            for inner in &oe.inner_calls {
                if seen.insert(inner.contract_address) {
                    push(
                        TxNavItem::Address(inner.contract_address),
                        NavSection::Calls,
                        &mut items,
                        &mut sections,
                    );
                }
            }
        }

        // === Events tab ===
        for event in &self.decoded_events {
            if seen.insert(event.contract_address) {
                push(
                    TxNavItem::Address(event.contract_address),
                    NavSection::Events,
                    &mut items,
                    &mut sections,
                );
            }
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
                if (is_address_type || is_known_label) && seen.insert(p.value) {
                    push(
                        TxNavItem::Address(p.value),
                        NavSection::Events,
                        &mut items,
                        &mut sections,
                    );
                }
            }
        }

        // === Transfers tab ===
        // Add token / from / to addresses pulled from Transfer events. Most
        // are dedup'd against earlier sections (sender is in Header, token
        // contracts in Events), so this only adds genuinely transfer-unique
        // addresses (e.g. final recipients).
        if let Some(trace) = &self.trace {
            let groups = trace.collect_transfers();
            let add = |felt: starknet::core::types::Felt,
                       items: &mut Vec<TxNavItem>,
                       sections: &mut Vec<NavSection>,
                       seen: &mut HashSet<starknet::core::types::Felt>| {
                if seen.insert(felt) {
                    push(
                        TxNavItem::Address(felt),
                        NavSection::Transfers,
                        items,
                        sections,
                    );
                }
            };
            for row in groups
                .validate
                .iter()
                .chain(groups.constructor.iter())
                .chain(groups.execute_top.iter())
                .chain(groups.execute_calls.iter().flat_map(|g| g.transfers.iter()))
                .chain(groups.l1_handler.iter())
                .chain(groups.fee.iter())
            {
                add(row.token, &mut items, &mut sections, &mut seen);
                add(row.from, &mut items, &mut sections, &mut seen);
                add(row.to, &mut items, &mut sections, &mut seen);
            }
        }

        // === Trace tab ===
        if let Some(trace) = &self.trace {
            trace.for_each_call(|call| {
                if seen.insert(call.contract_address) {
                    push(
                        TxNavItem::Address(call.contract_address),
                        NavSection::Trace,
                        &mut items,
                        &mut sections,
                    );
                }
                for p in call
                    .events
                    .iter()
                    .flat_map(|e| e.decoded_keys.iter().chain(e.decoded_data.iter()))
                {
                    let type_name = p.type_name.as_deref().unwrap_or("");
                    let is_address_type = type_name.contains("ContractAddress");
                    let is_known_label = !is_address_type
                        && type_name.is_empty()
                        && p.value != starknet::core::types::Felt::ZERO
                        && is_known(&p.value);
                    if (is_address_type || is_known_label) && seen.insert(p.value) {
                        push(
                            TxNavItem::Address(p.value),
                            NavSection::Trace,
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
        }
    }

    /// Step the visual-mode cursor by `delta` (wrapping). If the next item is
    /// in a different tab, switch to that tab; then scroll the matching
    /// section so the item is visible (2 lines of context above).
    pub fn nav_step(&mut self, delta: i64) {
        if self.nav_items.is_empty() {
            return;
        }
        let len = self.nav_items.len() as i64;
        let next = (self.nav_cursor as i64 + delta).rem_euclid(len) as usize;
        self.nav_cursor = next;

        // Switch tab if needed so the selected item is in the active panel.
        if let Some(section) = self.nav_sections.get(next).copied() {
            match section {
                NavSection::Events => self.active_tab = TxTab::Events,
                NavSection::Calls => self.active_tab = TxTab::Calls,
                NavSection::Transfers => self.active_tab = TxTab::Transfers,
                NavSection::Trace => self.active_tab = TxTab::Trace,
                NavSection::Header => {} // header is always visible — no tab switch
            }
        }

        // Scroll the relevant tab so the item is visible. Header items don't
        // scroll anything (header is fixed-height).
        if let Some(&line) = self.nav_item_lines.get(next) {
            let target = line.saturating_sub(2);
            match self.nav_sections.get(next).copied() {
                Some(NavSection::Events) => self.events_scroll = target,
                Some(NavSection::Calls) => self.calls_scroll = target,
                Some(NavSection::Transfers) => self.transfers_scroll = target,
                Some(NavSection::Trace) => self.trace_scroll = target,
                Some(NavSection::Header) | None => {}
            }
        }
    }
}
