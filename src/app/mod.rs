//! Application state machine: view stack, navigation, and action dispatch.
//!
//! The `App` struct holds all UI state. Views are stacked (`view_stack`) and
//! navigated with `navigate_to(NavTarget)` — the single entry point for all
//! view transitions regardless of origin (search, visual mode, Enter, forward
//! history, etc.).
//!
//! Network responses arrive as `Action` variants and are handled by
//! `handle_action()`, which updates the appropriate view state struct.

pub mod actions;
pub mod input;
pub mod state;
pub mod views;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::debug;

use crate::data::types::{AddressTxSummary, SnBlock, VoyagerLabelInfo};
use crate::network::prices::PriceClient;
use crate::network::ws::WsSubscriptionManager;
use crate::registry::SearchResult;
use crate::search::SearchEngine;
use crate::ui::widgets::stateful_list::StatefulList;
use actions::{Action, Source};
use starknet::core::types::Felt;
use state::{
    ActiveQueries, ConnectionStatus, DataSources, Focus, InputMode, NavTarget, SourceStatus, View,
};
use views::{AddressInfoState, BlockDetailState, ClassInfoState, TxDetailState};

/// Maximum number of blocks retained in the main blocks list.
/// Applied consistently to both prepends (new blocks) and appends (older
/// blocks paginated in) to keep the list stable while the user navigates.
const MAX_BLOCKS: usize = 500;

/// A saved navigation location for the forward/back jump list (Ctrl+o / Ctrl+i).
#[derive(Debug, Clone)]
pub enum NavEntry {
    Block(u64),
    Transaction(starknet::core::types::Felt),
    Address(starknet::core::types::Felt),
    ClassHash(starknet::core::types::Felt),
}

/// Tabs in the address info view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressTab {
    Transactions,
    /// SNIP-9 outside executions where this address is the intender (issue #11).
    MetaTxs,
    Calls, // Incoming calls to this contract (for non-account contracts)
    Balances,
    Events,
    ClassHistory, // Class hash upgrades over time
}

pub struct App {
    // Navigation
    pub view_stack: Vec<View>,
    pub focus: Focus,
    pub input_mode: InputMode,
    pub should_quit: bool,

    // Block list (home view)
    pub blocks: StatefulList<SnBlock>,
    pub latest_block_number: u64,

    // View-specific state
    pub block_detail: BlockDetailState,
    pub tx_detail: TxDetailState,
    pub address: AddressInfoState,
    pub class: ClassInfoState,

    // Search
    pub search_input: String,
    pub search_cursor: usize,
    pub search_suggestions: Vec<SearchResult>,
    pub search_selected: usize,
    pub search_engine: Option<Arc<SearchEngine>>,

    // Status
    pub show_help: bool,
    pub is_loading: bool,
    pub loading_detail: Option<String>,
    pub connection_status: ConnectionStatus,
    pub error_message: Option<String>,
    pub data_sources: DataSources,
    pub active_queries: ActiveQueries,

    // Pagination flags (prevent duplicate fetches)
    pub fetching_older_blocks: bool,

    /// Set when `G` on the blocks view triggers an older-blocks fetch, so
    /// that once the fetch lands we re-snap the selection to the new last
    /// item (without this, `G` stops 30 blocks short of the true bottom).
    pub pending_bottom_jump: bool,

    /// Labels fetched from Voyager API at runtime (address → label info).
    /// Used as a fallback when neither user labels nor known addresses have an entry.
    pub voyager_labels: HashMap<starknet::core::types::Felt, VoyagerLabelInfo>,

    // Channel to network task
    pub action_tx: mpsc::UnboundedSender<Action>,

    // Navigation history
    /// Locations to revisit with Ctrl+i (populated by Ctrl+o / back navigation).
    pub forward_history: Vec<NavEntry>,

    /// Optional WebSocket subscription manager (present when APP_WS_URL is configured).
    pub ws_manager: Option<WsSubscriptionManager>,

    pub price_client: Option<Arc<PriceClient>>,
}

impl App {
    pub fn new(action_tx: mpsc::UnboundedSender<Action>) -> Self {
        Self {
            view_stack: vec![View::Blocks],
            focus: Focus::BlockList,
            input_mode: InputMode::Normal,
            should_quit: false,

            blocks: StatefulList::new(),
            latest_block_number: 0,

            block_detail: BlockDetailState::default(),
            tx_detail: TxDetailState::default(),
            address: AddressInfoState::default(),
            class: ClassInfoState::default(),

            search_input: String::new(),
            search_cursor: 0,
            search_suggestions: Vec::new(),
            search_selected: 0,
            search_engine: None,

            show_help: false,
            is_loading: false,
            loading_detail: None,
            connection_status: ConnectionStatus::default(),
            error_message: None,
            data_sources: DataSources::default(),
            active_queries: ActiveQueries::default(),

            fetching_older_blocks: false,
            pending_bottom_jump: false,

            voyager_labels: HashMap::new(),

            action_tx,

            forward_history: Vec::new(),

            ws_manager: None,
            price_client: None,
        }
    }

    fn filter_deployment_txs(
        &mut self,
        address: starknet::core::types::Felt,
        txs: Vec<AddressTxSummary>,
    ) -> Vec<AddressTxSummary> {
        self.address.filter_deployment_txs(address, txs)
    }

    fn dispatch_balance_price_fetch(&self) {
        if self.price_client.is_none() {
            return;
        }
        let tokens = crate::network::prices::TRACKED_TOKENS.clone();
        if tokens.is_empty() {
            return;
        }
        let _ = self
            .action_tx
            .send(Action::FetchTokenPricesToday { tokens });
    }

    fn dispatch_tx_price_fetch(&self) {
        if self.price_client.is_none() {
            return;
        }
        let mut event_tokens: Vec<starknet::core::types::Felt> = self
            .tx_detail
            .decoded_events
            .iter()
            .map(|e| e.contract_address)
            .filter(crate::network::prices::is_tracked)
            .collect();
        event_tokens.sort();
        event_tokens.dedup();
        if event_tokens.is_empty() {
            return;
        }

        let _ = self.action_tx.send(Action::FetchTokenPricesToday {
            tokens: event_tokens.clone(),
        });

        if let Some(ts) = self.tx_detail.block_timestamp {
            let requests: Vec<_> = event_tokens.into_iter().map(|t| (t, ts)).collect();
            let _ = self
                .action_tx
                .send(Action::FetchTokenPricesHistoric { requests });
        }
    }

    pub fn current_view(&self) -> View {
        *self.view_stack.last().unwrap_or(&View::Blocks)
    }

    pub fn push_view(&mut self, view: View) {
        self.view_stack.push(view);
        // Set focus based on view
        match view {
            View::Blocks => self.focus = Focus::BlockList,
            View::BlockDetail => {
                self.block_detail.txs.select_first();
                self.focus = Focus::TxList;
            }
            View::TxDetail => self.focus = Focus::TxDetail,
            View::AddressInfo => self.focus = Focus::AddressHistory,
            View::ClassInfo => self.focus = Focus::ClassDetail,
        }
    }

    pub fn pop_view(&mut self) {
        if self.view_stack.len() > 1 {
            // Unsubscribe from WS address subscriptions when leaving address view
            if self.current_view() == View::AddressInfo {
                self.unsubscribe_current_address();
            }
            self.view_stack.pop();
            // Restore focus
            match self.current_view() {
                View::Blocks => self.focus = Focus::BlockList,
                View::BlockDetail => self.focus = Focus::TxList,
                View::TxDetail => self.focus = Focus::TxDetail,
                View::AddressInfo => self.focus = Focus::AddressHistory,
                View::ClassInfo => self.focus = Focus::ClassDetail,
            }
        }
    }

    /// Pop one view, saving the current location to the forward history (Ctrl+i can revisit it).
    pub fn pop_view_saving_forward(&mut self) {
        if self.view_stack.len() <= 1 {
            return;
        }
        if let Some(entry) = self.current_nav_entry() {
            self.forward_history.push(entry);
        }
        self.pop_view();
    }

    /// Unsubscribe WS for the currently viewed address, if any.
    fn unsubscribe_current_address(&mut self) {
        if let (Some(addr), Some(mgr)) = (self.address.context, &self.ws_manager) {
            if self.address.ws_subscribed {
                mgr.unsubscribe_address(addr);
                self.address.ws_subscribed = false;
            }
        }
    }

    /// Go to the Blocks root view, clearing all history. Quits if already at root.
    pub fn go_to_root_or_quit(&mut self) {
        if self.view_stack.len() > 1 {
            // Unsubscribe from any active address WS subscription
            if self.view_stack.contains(&View::AddressInfo) {
                self.unsubscribe_current_address();
            }
            self.view_stack.truncate(1);
            self.forward_history.clear();
            self.focus = Focus::BlockList;
        } else {
            self.should_quit = true;
        }
    }

    /// Dispatch the next forward-history entry. Returns the Action to fetch it.
    pub fn navigate_forward(&mut self) -> Option<Action> {
        let entry = self.forward_history.pop()?;
        let target = match entry {
            NavEntry::Block(n) => NavTarget::Block(n),
            NavEntry::Transaction(h) => NavTarget::Transaction(h),
            NavEntry::Address(a) => NavTarget::Address(a),
            NavEntry::ClassHash(c) => NavTarget::ClassHash(c),
        };
        self.navigate_to(target)
    }

    /// Single entry point for all view navigation.
    ///
    /// Clears any active visual mode, clears stale view state, pushes the
    /// target view, sets loading state, and returns the fetch Action.
    /// All navigation paths (search, visual mode, Enter, forward history,
    /// nonce cycling, etc.) should go through this method.
    pub fn navigate_to(&mut self, target: NavTarget) -> Option<Action> {
        // Clear any active visual mode
        self.tx_detail.visual_mode = false;
        self.block_detail.visual_mode = false;
        self.class.visual_mode = false;
        self.address.visual_mode = false;

        self.is_loading = true;

        match target {
            NavTarget::Block(n) => {
                self.clear_block_detail();
                self.push_view(View::BlockDetail);
                Some(Action::FetchBlockDetail { number: n })
            }
            NavTarget::Transaction(h) => {
                self.clear_tx_detail();
                self.push_view(View::TxDetail);
                Some(Action::FetchTransaction { hash: h })
            }
            NavTarget::Address(a) => {
                // FetchAddressInfo handler sends NavigateToAddress internally,
                // which does the view push + state clear in handle_action.
                Some(Action::FetchAddressInfo { address: a })
            }
            NavTarget::ClassHash(c) => {
                self.clear_class_info();
                // FetchClassInfo handler sends NavigateToClassInfo internally.
                Some(Action::FetchClassInfo { class_hash: c })
            }
        }
    }

    fn current_nav_entry(&self) -> Option<NavEntry> {
        match self.current_view() {
            View::BlockDetail => self
                .block_detail
                .block
                .as_ref()
                .map(|b| NavEntry::Block(b.number)),
            View::TxDetail => self
                .tx_detail
                .transaction
                .as_ref()
                .map(|tx| NavEntry::Transaction(tx.hash())),
            View::AddressInfo => self.address.context.map(NavEntry::Address),
            View::ClassInfo => self.class.hash.map(NavEntry::ClassHash),
            View::Blocks => None,
        }
    }

    /// Send an `EnrichAddressTxs` for the rows currently visible in the
    /// address Transactions tab that are missing endpoint names or timestamps.
    ///
    /// Idempotent: the cache layer dedups in-flight requests and
    /// `enrich_address_txs` itself drops hashes that already have data.
    /// Safe to call on every scroll keystroke.
    pub fn maybe_enrich_visible_address_txs(&self) {
        if self.current_view() != View::AddressInfo {
            return;
        }
        if !matches!(self.address.tab, AddressTab::Transactions) {
            return;
        }
        let Some(address) = self.address.context else {
            return;
        };
        let offset = self.address.txs.state.offset();
        let hashes: Vec<_> = self
            .address
            .txs
            .items
            .iter()
            .skip(offset)
            .take(50)
            .filter(|t| t.endpoint_names.is_empty() || t.timestamp == 0)
            .map(|t| t.hash)
            .collect();
        if !hashes.is_empty() {
            let _ = self
                .action_tx
                .send(Action::EnrichAddressTxs { address, hashes });
        }
    }

    /// Scroll the active address list by a delta and trigger viewport
    /// enrichment for any rows newly exposed.
    pub fn address_list_scroll_by(&mut self, delta: i64) {
        match self.address.tab {
            AddressTab::Transactions => {
                self.address.txs.scroll_by(delta);
                self.maybe_enrich_visible_address_txs();
            }
            AddressTab::Calls => self.address.calls.scroll_by(delta),
            AddressTab::MetaTxs => {
                self.address.meta_txs.scroll_by(delta);
                self.maybe_fetch_more_meta_txs();
            }
            _ => {}
        }
    }

    /// Scroll the main blocks list by a delta, triggering older-block
    /// pagination when the new selection nears the tail. Used by Ctrl+D /
    /// Ctrl+U on the Blocks view.
    pub fn blocks_scroll_by(&mut self, delta: i64) {
        if self.blocks.items.is_empty() {
            return;
        }
        self.blocks.scroll_by(delta);
        self.maybe_fetch_older_blocks();
    }

    /// Dispatch the next MetaTxs page when the selection nears the end of the
    /// list and pf-query signaled more data. Idempotent: guarded by
    /// `fetching_meta_txs`.
    pub fn maybe_fetch_more_meta_txs(&mut self) {
        if !matches!(self.address.tab, AddressTab::MetaTxs) {
            return;
        }
        if self.address.fetching_meta_txs || !self.address.meta_tx_has_more {
            return;
        }
        if !self.address.meta_txs.is_near_bottom(5) {
            return;
        }
        let Some(addr) = self.address.context else {
            return;
        };
        let token = self.address.meta_tx_cursor_block;
        let from_block = self.address.meta_tx_from_block;
        let window_size = self.address.meta_tx_last_window;
        self.address.fetching_meta_txs = true;
        let _ = self.action_tx.send(Action::FetchAddressMetaTxs {
            address: addr,
            from_block,
            continuation_token: token,
            window_size,
            limit: 50,
        });
    }

    pub fn select_next(&mut self) {
        match self.current_view() {
            View::Blocks => {
                self.blocks.next();
                self.maybe_fetch_older_blocks();
            }
            View::BlockDetail => self.block_detail.txs.next(),
            View::TxDetail => {
                self.tx_detail.scroll = self.tx_detail.scroll.saturating_add(1);
            }
            View::ClassInfo => {
                self.class.scroll = self.class.scroll.saturating_add(1);
            }
            View::AddressInfo => match self.address.tab {
                AddressTab::Transactions => {
                    self.address.txs.next();
                    self.maybe_fetch_more_address_txs();
                    self.maybe_enrich_visible_address_txs();
                }
                AddressTab::Calls => {
                    self.address.calls.next();
                    self.maybe_fetch_more_address_txs();
                }
                AddressTab::MetaTxs => {
                    self.address.meta_txs.next();
                    self.maybe_fetch_more_meta_txs();
                }
                AddressTab::Events => self.address.events.next(),
                AddressTab::ClassHistory => {
                    self.address.class_history_scroll = self
                        .address
                        .class_history_scroll
                        .saturating_add(1)
                        .min(self.address.class_history.len().saturating_sub(1));
                }
                AddressTab::Balances => {}
            },
        }
    }

    pub fn select_previous(&mut self) {
        match self.current_view() {
            View::Blocks => self.blocks.previous(),
            View::BlockDetail => self.block_detail.txs.previous(),
            View::TxDetail => {
                self.tx_detail.scroll = self.tx_detail.scroll.saturating_sub(1);
            }
            View::ClassInfo => {
                self.class.scroll = self.class.scroll.saturating_sub(1);
            }
            View::AddressInfo => match self.address.tab {
                AddressTab::Transactions => {
                    self.address.txs.previous();
                    self.maybe_enrich_visible_address_txs();
                }
                AddressTab::Calls => self.address.calls.previous(),
                AddressTab::MetaTxs => self.address.meta_txs.previous(),
                AddressTab::Events => self.address.events.previous(),
                AddressTab::ClassHistory => {
                    self.address.class_history_scroll =
                        self.address.class_history_scroll.saturating_sub(1);
                }
                AddressTab::Balances => {}
            },
        }
    }

    pub fn select_first(&mut self) {
        match self.current_view() {
            View::Blocks => self.blocks.select_first(),
            View::BlockDetail => self.block_detail.txs.select_first(),
            View::TxDetail => {
                self.tx_detail.scroll = 0;
            }
            View::ClassInfo => {
                self.class.scroll = 0;
            }
            View::AddressInfo => match self.address.tab {
                AddressTab::Transactions => {
                    self.address.txs.select_first();
                    self.maybe_enrich_visible_address_txs();
                }
                AddressTab::Calls => self.address.calls.select_first(),
                AddressTab::MetaTxs => self.address.meta_txs.select_first(),
                AddressTab::Events => self.address.events.select_first(),
                AddressTab::ClassHistory => {
                    self.address.class_history_scroll = 0;
                }
                AddressTab::Balances => {}
            },
        }
    }

    pub fn select_last(&mut self) {
        match self.current_view() {
            View::Blocks => {
                self.blocks.select_last();
                let was_fetching = self.fetching_older_blocks;
                self.maybe_fetch_older_blocks();
                // If G just kicked off an older-blocks fetch, remember that
                // the user wanted the true bottom so we can re-snap once
                // the paginated batch lands.
                if !was_fetching && self.fetching_older_blocks {
                    self.pending_bottom_jump = true;
                }
            }
            View::BlockDetail => self.block_detail.txs.select_last(),
            View::TxDetail => {
                self.tx_detail.scroll = 999;
            } // will be clamped by Paragraph
            View::ClassInfo => {
                self.class.scroll = 999;
            }
            View::AddressInfo => match self.address.tab {
                AddressTab::Transactions => {
                    self.address.txs.select_last();
                    self.maybe_fetch_more_address_txs();
                    self.maybe_enrich_visible_address_txs();
                }
                AddressTab::Calls => {
                    self.address.calls.select_last();
                    self.maybe_fetch_more_address_txs();
                }
                AddressTab::MetaTxs => {
                    self.address.meta_txs.select_last();
                    self.maybe_fetch_more_meta_txs();
                }
                AddressTab::Events => self.address.events.select_last(),
                AddressTab::ClassHistory => {
                    self.address.class_history_scroll =
                        self.address.class_history.len().saturating_sub(1);
                }
                AddressTab::Balances => {}
            },
        }
    }

    /// If the selection is near the bottom of the block list, fetch the next batch of older blocks.
    fn maybe_fetch_older_blocks(&mut self) {
        if self.fetching_older_blocks || self.blocks.items.is_empty() {
            return;
        }
        // Trigger when within 5 items of the end
        if self.blocks.is_near_bottom(5) {
            if let Some(oldest) = self.blocks.items.last() {
                let before = oldest.number;
                if before > 0 {
                    self.fetching_older_blocks = true;
                    let _ = self
                        .action_tx
                        .send(Action::FetchOlderBlocks { before, count: 30 });
                }
            }
        }
    }

    /// If the selection is near the bottom of the address tx or calls list, fetch more.
    fn maybe_fetch_more_address_txs(&mut self) {
        if self.address.fetching_more_txs {
            return;
        }
        let near_bottom = match self.address.tab {
            AddressTab::Transactions => self.address.txs.is_near_bottom(5),
            AddressTab::Calls => self.address.calls.is_near_bottom(5),
            _ => false,
        };
        if !near_bottom {
            return;
        }

        // Priority 1: on-demand fill of a deferred large nonce gap (issue #10).
        // Only fires once per gap; the handler clears `unfilled_gap` on completion.
        if let Some(gap) = self.address.unfilled_gap.as_ref() {
            if !gap.fill_dispatched {
                if let Some(address) = self.address.context {
                    let gap_clone = gap.clone();
                    if let Some(g) = self.address.unfilled_gap.as_mut() {
                        g.fill_dispatched = true;
                    }
                    self.address.fetching_more_txs = true;
                    let _ = self.action_tx.send(Action::FillAddressNonceGaps {
                        address,
                        known_txs: self.address.txs.items.clone(),
                        gap: gap_clone,
                    });
                    return;
                }
            }
        }

        // Priority 2: chronological pagination (older than oldest known block).
        // Don't fetch if no source thinks there's more data.
        if !self.address.has_more_data()
            && self.address.oldest_event_block.is_some()
            && self.address.sources_pending.is_empty()
        {
            return;
        }
        let cursor = self.address.pagination_cursor();
        if let (Some(address), Some(before_block)) = (self.address.context, cursor) {
            if before_block > 0 {
                self.address.fetching_more_txs = true;
                let _ = self.action_tx.send(Action::FetchMoreAddressTxs {
                    address,
                    before_block,
                    is_contract: self.address.is_contract,
                });
            }
        }
    }

    pub fn clear_block_detail(&mut self) {
        self.block_detail.clear();
    }

    pub fn clear_tx_detail(&mut self) {
        self.tx_detail.clear();
    }

    pub fn build_tx_nav_items(&mut self) {
        let registry = self.search_engine.as_ref().map(|e| e.registry());
        self.tx_detail
            .build_nav_items(|felt| registry.as_ref().is_some_and(|r| r.resolve(felt).is_some()));
    }

    pub fn tx_nav_step(&mut self, delta: i64) {
        self.tx_detail.nav_step(delta);
    }

    pub fn clear_class_info(&mut self) {
        self.class.clear();
    }

    pub fn build_class_nav_items(&mut self) {
        self.class.build_nav_items();
    }

    pub fn class_nav_step(&mut self, delta: i64) {
        self.class.nav_step(delta);
    }

    pub fn build_address_nav_items(&mut self) {
        self.address.build_nav_items();
    }

    pub fn address_nav_step(&mut self, delta: i64) {
        self.address.nav_step(delta);
    }

    /// Get the currently selected block (in blocks list view).
    pub fn selected_block(&self) -> Option<&SnBlock> {
        self.blocks.selected_item()
    }

    /// Get the currently selected transaction (in block detail view).
    pub fn selected_transaction(&self) -> Option<&crate::data::types::SnTransaction> {
        self.block_detail.txs.selected_item()
    }

    /// Update autocomplete suggestions based on current search input.
    pub fn update_suggestions(&mut self) {
        if let Some(engine) = &self.search_engine {
            self.search_suggestions = engine.suggest(&self.search_input);
            self.search_selected = 0;
        }
    }

    /// Format an address using the registry (label if known, short hex otherwise).
    /// Falls back to a Voyager-sourced label when the registry has no entry.
    pub fn format_address(&self, address: &starknet::core::types::Felt) -> String {
        if let Some(engine) = &self.search_engine {
            let registry = engine.registry();
            if registry.resolve(address).is_some() {
                return registry.format_address(address);
            }
        }
        // Voyager fallback
        if let Some(label) = self.voyager_labels.get(address) {
            if let Some(name) = &label.name {
                return format!("[{}] \u{2B21}", name); // ⬡ = Voyager-sourced // ⬡ marker for Voyager-sourced
            }
        }
        if let Some(engine) = &self.search_engine {
            engine.registry().format_address(address)
        } else {
            crate::ui::widgets::hex_display::short_address(address)
        }
    }

    /// Format an address showing both user and global labels (for detail views).
    /// Falls back to a Voyager-sourced label when the registry has no entry.
    pub fn format_address_full(&self, address: &starknet::core::types::Felt) -> String {
        if let Some(engine) = &self.search_engine {
            let registry = engine.registry();
            if registry.resolve(address).is_some() {
                return registry.format_address_full(address);
            }
        }
        // Voyager fallback
        if let Some(label) = self.voyager_labels.get(address) {
            if let Some(name) = &label.name {
                return format!("[{}] \u{2B21}", name); // ⬡ = Voyager-sourced // ⬡
            }
        }
        if let Some(engine) = &self.search_engine {
            engine.registry().format_address_full(address)
        } else {
            crate::ui::widgets::hex_display::short_address(address)
        }
    }

    /// Handle a response action from the network task.
    pub fn handle_action(&mut self, action: Action) {
        match action {
            Action::BlocksLoaded(blocks) => {
                if let Some(first) = blocks.first() {
                    self.latest_block_number = first.number;
                }
                self.blocks = StatefulList::with_items(blocks);
                if !self.blocks.items.is_empty() {
                    self.blocks.select_first();
                }
                self.is_loading = false;
                self.fetching_older_blocks = false;
                self.pending_bottom_jump = false;
            }
            Action::OlderBlocksLoaded(blocks) => {
                // Preserve selection by block NUMBER across the append +
                // truncate — index-based tracking silently drifts the user
                // onto a different block when the list grows or shrinks.
                let selected_number = self.blocks.selected_item().map(|b| b.number);
                self.blocks.items.extend(blocks);
                if self.blocks.items.len() > MAX_BLOCKS {
                    self.blocks.items.truncate(MAX_BLOCKS);
                }
                self.fetching_older_blocks = false;
                if self.pending_bottom_jump {
                    // `G` asked for the true bottom; snap to the new last
                    // item now that pagination has landed.
                    self.blocks.select_last();
                    self.pending_bottom_jump = false;
                } else if let Some(num) = selected_number
                    && let Some(idx) = self.blocks.items.iter().position(|b| b.number == num)
                {
                    self.blocks.state.select(Some(idx));
                }
            }
            Action::NewBlock(block) => {
                self.latest_block_number = block.number;
                // Remember where the user was *before* mutating the list so
                // we can either follow the tip (sel==0) or pin to the same
                // block number (sel>0), robust against tail truncation.
                let prior_sel = self.blocks.state.selected();
                let selected_number = self.blocks.selected_item().map(|b| b.number);

                // Deduplicate: if the head already has this block number
                // (e.g. WS replay on reconnect), overwrite rather than
                // insert a duplicate row.
                if self.blocks.items.first().map(|b| b.number) == Some(block.number) {
                    self.blocks.items[0] = block;
                } else {
                    self.blocks.items.insert(0, block);
                }
                if self.blocks.items.len() > MAX_BLOCKS {
                    self.blocks.items.truncate(MAX_BLOCKS);
                }

                match prior_sel {
                    // Follow the tip when unselected or already at the top.
                    None | Some(0) => {
                        if !self.blocks.items.is_empty() {
                            self.blocks.state.select(Some(0));
                        }
                    }
                    // Otherwise pin to the same block number so the user's
                    // highlighted row does not silently change under them.
                    Some(_) => {
                        if let Some(num) = selected_number {
                            if let Some(idx) =
                                self.blocks.items.iter().position(|b| b.number == num)
                            {
                                self.blocks.state.select(Some(idx));
                            } else {
                                // Block fell off the tail during truncation;
                                // clamp to the new last item.
                                self.blocks.select_last();
                            }
                        }
                    }
                }
            }
            Action::BlockDetailLoaded {
                block,
                mut transactions,
                mut endpoint_names,
                mut tx_statuses,
                mut meta_tx_info,
            } => {
                self.block_detail.block = Some(block);
                // Reverse to show highest index first (descending order)
                transactions.reverse();
                endpoint_names.reverse();
                tx_statuses.reverse();
                meta_tx_info.reverse();
                self.block_detail.txs = StatefulList::with_items(transactions);
                self.block_detail.endpoint_names = endpoint_names;
                self.block_detail.tx_statuses = tx_statuses;
                self.block_detail.meta_tx_info = meta_tx_info;
                if !self.block_detail.txs.items.is_empty() {
                    self.block_detail.txs.select_first();
                }
                self.is_loading = false;
                self.loading_detail = None;
                // Push view if not already there (e.g., from search)
                if self.current_view() != View::BlockDetail {
                    self.push_view(View::BlockDetail);
                }
            }
            Action::TransactionLoaded {
                transaction,
                receipt,
                decoded_events,
                decoded_calls,
                outside_executions,
                block_timestamp,
            } => {
                self.tx_detail.transaction = Some(transaction);
                self.tx_detail.receipt = Some(receipt);
                self.tx_detail.decoded_events = decoded_events;
                self.tx_detail.decoded_calls = decoded_calls;
                self.tx_detail.outside_executions = outside_executions;
                self.tx_detail.block_timestamp = block_timestamp;
                self.tx_detail.scroll = 0;
                self.tx_detail.visual_mode = false;
                self.build_tx_nav_items();
                self.is_loading = false;
                self.loading_detail = None;
                if self.current_view() != View::TxDetail {
                    self.push_view(View::TxDetail);
                }
                self.dispatch_tx_price_fetch();
            }
            Action::NavigateToAddress { address } => {
                // Push view immediately — show cached data while fresh data loads
                self.address.clear();
                self.address.context = Some(address);
                self.is_loading = true;
                self.loading_detail = Some("Fetching address info...".into());

                if self.current_view() != View::AddressInfo {
                    self.push_view(View::AddressInfo);
                }

                // Start streaming new txs/events via WS if available
                if let Some(mgr) = &self.ws_manager {
                    mgr.subscribe_address(address);
                    self.address.ws_subscribed = true;
                }
            }
            Action::AddressInfoLoaded {
                info,
                decoded_events,
                tx_summaries,
                contract_calls,
            } => {
                // Guard: ignore stale results from a previously navigated address.
                if let Some(ctx) = self.address.context {
                    if ctx != info.address {
                        return; // stale result for a different address — discard
                    }
                }
                let address = info.address;
                self.address.context = Some(address);

                // Detect contract vs account
                let is_contract =
                    info.nonce == starknet::core::types::Felt::ZERO && info.class_hash.is_some();
                self.address.is_contract = is_contract;

                // Preserve any balances that may have already arrived — the
                // balance task runs in parallel with the nonce/class_hash
                // fetch and can land first, and every callsite constructs
                // `SnAddressInfo` with an empty `token_balances` by default.
                let preserved_balances = self
                    .address
                    .info
                    .as_ref()
                    .map(|i| i.token_balances.clone())
                    .unwrap_or_default();

                // Only update info if it has real data
                let mut info = info;
                if info.token_balances.is_empty() && !preserved_balances.is_empty() {
                    info.token_balances = preserved_balances;
                }
                if info.nonce != starknet::core::types::Felt::ZERO
                    || !info.token_balances.is_empty()
                    || info.class_hash.is_some()
                {
                    self.address.info = Some(info);
                } else if self.address.info.is_none() {
                    self.address.info = Some(info);
                }

                if !decoded_events.is_empty() {
                    let had_selection = self.address.events.state.selected().is_some();
                    self.address.events.items = decoded_events;
                    if !had_selection {
                        self.address.events.select_first();
                    }
                }

                // Merge txs (dedup + upgrade existing entries with better data)
                let tx_summaries = self.filter_deployment_txs(address, tx_summaries);
                self.address.merge_tx_summaries(tx_summaries);

                // Track oldest block for pagination (from both txs and calls)
                let oldest_from_txs = self
                    .address
                    .txs
                    .items
                    .iter()
                    .filter(|t| t.block_number > 0)
                    .map(|t| t.block_number)
                    .min();
                let oldest_from_calls = self
                    .address
                    .calls
                    .items
                    .iter()
                    .filter(|c| c.block_number > 0)
                    .map(|c| c.block_number)
                    .min();
                if let Some(oldest) = oldest_from_txs.into_iter().chain(oldest_from_calls).min() {
                    self.address.oldest_event_block = Some(
                        self.address
                            .oldest_event_block
                            .map_or(oldest, |old| old.min(oldest)),
                    );
                }

                // Update contract calls (merge across sources, not replace).
                //
                // Calls can arrive from two paths for the same tx_hash:
                //   - Dune `starknet.calls` — authoritative and richer (resolved
                //     function_name, accurate fee).
                //   - pf-query shared window scan — supplementary, may arrive
                //     first if Dune is slow/unavailable.
                //
                // Previously the reducer skipped any new row whose tx_hash was
                // already present, which silently dropped Dune's richer data
                // when pf-query arrived first. Route both sources through
                // `deduplicate_contract_calls` which merges function_name
                // (joined), fills missing fee/timestamp, and preserves the
                // superset — so neither source can hide the other's data.
                if !contract_calls.is_empty() {
                    self.address.merge_calls(contract_calls);
                    if self.address.calls.state.selected().is_none()
                        && !self.address.calls.items.is_empty()
                    {
                        self.address.calls.select_first();
                    }

                    // Update cursor from calls for contracts
                    if let Some(min_b) = self
                        .address
                        .calls
                        .items
                        .iter()
                        .filter(|c| c.block_number > 0)
                        .map(|c| c.block_number)
                        .min()
                    {
                        self.address.oldest_event_block = Some(
                            self.address
                                .oldest_event_block
                                .map_or(min_b, |old| old.min(min_b)),
                        );
                        self.address.dune_has_more = true;
                        self.address.rpc_has_more = true;
                    }
                }

                // Set default tab based on contract type
                if self.address.tab == AddressTab::Transactions
                    && is_contract
                    && self.address.txs.items.is_empty()
                {
                    self.address.tab = AddressTab::Calls;
                }

                // Don't clear loading here — let AddressTxsStreamed source
                // completion tracking handle it for the parallel flow.
                // Only clear if no sources are pending (e.g. cache-only load).
                if self.address.sources_pending.is_empty() {
                    self.is_loading = false;
                    self.loading_detail = None;
                }
                if self.current_view() != View::AddressInfo {
                    self.push_view(View::AddressInfo);
                }
                self.build_address_nav_items();

                // Lazily enrich visible txs that are missing endpoint/timestamp data
                if let Some(address) = self.address.context {
                    let offset = self.address.txs.state.offset();
                    let hashes: Vec<_> = self
                        .address
                        .txs
                        .items
                        .iter()
                        .skip(offset)
                        .take(50)
                        .filter(|t| t.endpoint_names.is_empty() || t.timestamp == 0)
                        .map(|t| t.hash)
                        .collect();
                    if !hashes.is_empty() {
                        let _ = self
                            .action_tx
                            .send(Action::EnrichAddressTxs { address, hashes });
                    }

                    // Detect any large nonce gap in cached data and defer it to
                    // on-demand fill (issue #10). Endpoint enrichment + small gap
                    // fill still run immediately from cache.
                    if !self.address.is_contract
                        && !self.address.txs.items.is_empty()
                        && !self.address.sanity_check_dispatched
                    {
                        if let Some(info) = &self.address.info {
                            let current_nonce = crate::utils::felt_to_u64(&info.nonce);
                            if current_nonce > 0 {
                                self.address.unfilled_gap = self.address.detect_unfilled_gap();
                                self.address.sanity_check_dispatched = true;
                                let _ = self.action_tx.send(Action::EnrichAddressEndpoints {
                                    address,
                                    current_nonce,
                                    known_txs: self.address.txs.items.clone(),
                                });
                            }
                        }
                    }
                }
            }
            Action::MoreAddressTxsLoaded {
                address,
                tx_summaries,
                contract_calls,
                oldest_block,
                has_more,
            } => {
                if self.address.context == Some(address) {
                    let tx_summaries = self.filter_deployment_txs(address, tx_summaries);
                    self.address.merge_tx_summaries(tx_summaries);
                    // Merge contract calls across sources (Dune + pf-query) —
                    // see `merge_calls` for why this is a field-level merge
                    // rather than a hash-based replace.
                    self.address.merge_calls(contract_calls);
                    self.address.oldest_event_block = Some(oldest_block);
                    self.address.dune_cursor_block = Some(oldest_block);
                    self.address.rpc_cursor_block = Some(oldest_block);
                    self.address.dune_has_more = has_more;
                    self.address.rpc_has_more = has_more;
                }
                self.address.fetching_more_txs = false;
            }
            Action::AddressMetaTxsLoaded {
                address,
                summaries,
                next_token,
                next_window_size,
            } => {
                if self.address.context == Some(address) {
                    // Merge by hash to avoid dupes across pages.
                    let mut seen: std::collections::HashSet<_> =
                        self.address.meta_txs.items.iter().map(|m| m.hash).collect();
                    for s in summaries {
                        if seen.insert(s.hash) {
                            self.address.meta_txs.items.push(s);
                        }
                    }
                    self.address.meta_txs.items.sort_by(|a, b| {
                        b.block_number
                            .cmp(&a.block_number)
                            .then(b.tx_index.cmp(&a.tx_index))
                    });
                    if !self.address.meta_txs.items.is_empty()
                        && self.address.meta_txs.state.selected().is_none()
                    {
                        self.address.meta_txs.select_first();
                    }
                    self.address.meta_tx_cursor_block = next_token;
                    self.address.meta_tx_has_more = next_token.is_some();
                    // Persist adapted window size so the next ExtendDown
                    // dispatch (auto-page or user-scroll) picks up where this
                    // page left off.
                    if let Some(w) = next_window_size {
                        self.address.meta_tx_last_window = w;
                    }
                }
                self.address.fetching_meta_txs = false;
                // Progressive fill: keep paging until the visible list has
                // `AUTO_FILL_TARGET` rows, or pf-query runs out of blocks to
                // scan (`meta_tx_has_more == false`), or the user navigates
                // away (session-token cancellation tears down the in-flight
                // fetch). No page-count cap — a sparse address with zero
                // classified meta-txs will walk back to deploy block rather
                // than silently under-report. Once auto-fill stops, further
                // pagination is strictly on-demand via
                // `maybe_fetch_more_meta_txs` on scroll-near-bottom, so the
                // history-exhausted and user-continuation paths converge.
                use crate::app::views::address_info::AUTO_FILL_TARGET;
                if self.address.context == Some(address)
                    && self.address.meta_tx_has_more
                    && self.address.meta_txs.items.len() < AUTO_FILL_TARGET
                {
                    let next = self.address.meta_tx_cursor_block;
                    let from_block = self.address.meta_tx_from_block;
                    let window_size = self.address.meta_tx_last_window;
                    self.address.fetching_meta_txs = true;
                    let _ = self.action_tx.send(Action::FetchAddressMetaTxs {
                        address,
                        from_block,
                        continuation_token: next,
                        window_size,
                        limit: 50,
                    });
                }
            }
            Action::AddressMetaTxsCacheLoaded { address, summaries } => {
                if self.address.context == Some(address) {
                    let mut seen: std::collections::HashSet<_> =
                        self.address.meta_txs.items.iter().map(|m| m.hash).collect();
                    for s in &summaries {
                        if seen.insert(s.hash) {
                            self.address.meta_txs.items.push(s.clone());
                        }
                    }
                    self.address.meta_txs.items.sort_by(|a, b| {
                        b.block_number
                            .cmp(&a.block_number)
                            .then(b.tx_index.cmp(&a.tx_index))
                    });
                    if !self.address.meta_txs.items.is_empty()
                        && self.address.meta_txs.state.selected().is_none()
                    {
                        self.address.meta_txs.select_first();
                    }
                    // Keep fetching_meta_txs / cursor as-is: a live fetch may
                    // still be in-flight behind this cache delivery.

                    // Plan §2 invariant: every cached MetaTx is also a Call on
                    // this address. The live meta-tx scan already emits
                    // `AddressCallsMerged` for freshly-seen pages, but on
                    // re-entry the cached rows never pass through that path —
                    // promote them here so the Calls list reflects the
                    // MetaTxs ⊆ Calls invariant immediately from cache.
                    let promoted: Vec<crate::data::types::ContractCallSummary> = summaries
                        .into_iter()
                        .map(|m| crate::data::types::ContractCallSummary {
                            tx_hash: m.hash,
                            sender: m.paymaster,
                            function_name: String::new(),
                            block_number: m.block_number,
                            timestamp: m.timestamp,
                            total_fee_fri: m.total_fee_fri,
                            status: m.status,
                            nonce: None,
                            tip: 0,
                        })
                        .collect();
                    if !promoted.is_empty() {
                        let _ = self.action_tx.send(Action::AddressCallsMerged {
                            address,
                            calls: promoted,
                        });
                    }
                }
            }
            Action::AddressMetaTxsStreamed { address, summaries } => {
                if self.address.context == Some(address) && !summaries.is_empty() {
                    let mut seen: std::collections::HashSet<_> =
                        self.address.meta_txs.items.iter().map(|m| m.hash).collect();
                    for s in summaries {
                        if seen.insert(s.hash) {
                            self.address.meta_txs.items.push(s);
                        }
                    }
                    self.address.meta_txs.items.sort_by(|a, b| {
                        b.block_number
                            .cmp(&a.block_number)
                            .then(b.tx_index.cmp(&a.tx_index))
                    });
                    if self.address.meta_txs.state.selected().is_none() {
                        self.address.meta_txs.select_first();
                    }
                    // Persistence of streamed rows happens in the network
                    // dispatcher (it holds the DataSource Arc). We never touch
                    // fetching_meta_txs / cursor / has_more / meta_txs_dispatched
                    // here: streaming merges run in parallel with bulk
                    // pagination and must not change the user's view or
                    // short-circuit in-flight fetches.
                }
            }
            Action::AddressProbeLoaded { address, probe } => {
                if self.address.context == Some(address) {
                    self.address.activity_probe = Some(probe);
                }
            }
            Action::ClassHistoryLoaded { address, entries } => {
                if self.address.context == Some(address) {
                    // Fallback: if no deploy info yet, use earliest class_history block
                    if self.address.deployment.is_none() {
                        if let Some(first) = entries.last() {
                            self.address.deployment = Some(AddressTxSummary {
                                hash: starknet::core::types::Felt::ZERO,
                                nonce: 0,
                                block_number: first.block_number,
                                timestamp: 0,
                                endpoint_names: String::new(),
                                total_fee_fri: 0,
                                tip: 0,
                                tx_type: "DEPLOY".into(),
                                status: "OK".into(),
                                sender: None,
                            });
                            self.build_address_nav_items();
                        }
                    }
                    self.address.class_history = entries;
                    self.address.class_history_scroll = 0;
                }
            }
            Action::AddressTxsEnriched { address, updates } => {
                if self.address.context != Some(address) {
                    return;
                }
                // If a deferred gap-fill was in-flight, unblock the pagination
                // trigger now that results have landed.
                let was_gap_fill = self
                    .address
                    .unfilled_gap
                    .as_ref()
                    .is_some_and(|g| g.fill_dispatched);
                // Merge enrichment data (upgrades existing entries)
                self.address.merge_tx_summaries(updates);
                // If a gap fill was in flight, re-run detection so we either
                // clear the gap (filled) or update it to the residual gap.
                if was_gap_fill {
                    self.address.unfilled_gap = self.address.detect_unfilled_gap().map(|mut g| {
                        // Preserve dispatched state so we don't re-fire for the
                        // same (or similar) gap; require refresh ('r') to retry.
                        g.fill_dispatched = true;
                        g
                    });
                    self.address.fetching_more_txs = false;
                }
                // Persist enriched txs to cache so they survive restarts
                if !self.address.txs.items.is_empty() {
                    let _ = self.action_tx.send(Action::PersistAddressTxs {
                        address,
                        txs: self.address.txs.items.clone(),
                    });
                }
            }
            Action::AddressSourcesPending { address, sources } => {
                if self.address.context == Some(address) {
                    self.address.sources_pending = sources.into_iter().collect();
                }
            }
            Action::AddressEventWindowUpdated {
                address,
                min_searched,
                max_searched,
                deferred_gap,
            } => {
                if self.address.context == Some(address) {
                    self.address.event_window =
                        Some(crate::app::views::address_info::EventWindowHint {
                            min_searched,
                            max_searched,
                            deferred_gap,
                        });
                }
            }
            Action::AddressTxsStreamed {
                address,
                source,
                tx_summaries,
                complete,
            } => {
                if self.address.context != Some(address) {
                    debug!(
                        address = %format!("{:#x}", address),
                        source = ?source,
                        tx_summaries = tx_summaries.len(),
                        complete,
                        current_context = ?self.address.context.map(|c| format!("{:#x}", c)),
                        "AddressTxsStreamed dropped: stale context"
                    );
                    return;
                }
                let old_deploy_hash = self.address.deployment.as_ref().map(|d| d.hash);
                let tx_summaries = self.filter_deployment_txs(address, tx_summaries);
                let new_deploy_hash = self.address.deployment.as_ref().map(|d| d.hash);
                // Rebuild nav items if deployment info arrived or hash was upgraded
                if old_deploy_hash != new_deploy_hash {
                    self.build_address_nav_items();
                }

                // Derive an up-to-date account nonce from the incoming
                // (post-deployment-filter) txs. After a tx with nonce N lands,
                // the account's next nonce is N+1. This keeps the header
                // `Nonce:` field in sync with newly streamed txs (WS today,
                // RPC fallback poll in the future) instead of drifting behind
                // the initial RPC load value.
                let max_incoming_nonce = if self.address.is_contract {
                    None
                } else {
                    tx_summaries.iter().map(|t| t.nonce).max()
                };

                self.address.merge_tx_summaries(tx_summaries);

                if let Some(max_n) = max_incoming_nonce {
                    if let Some(info) = self.address.info.as_mut() {
                        let new_nonce = max_n.saturating_add(1);
                        if new_nonce > crate::utils::felt_to_u64(&info.nonce) {
                            info.nonce = Felt::from(new_nonce);
                        }
                    }
                }

                // Trigger enrichment for visible window
                if !self.address.txs.items.is_empty() {
                    let offset = self.address.txs.state.offset();
                    let hashes: Vec<_> = self
                        .address
                        .txs
                        .items
                        .iter()
                        .skip(offset)
                        .take(50)
                        .filter(|t| t.endpoint_names.is_empty() || t.timestamp == 0)
                        .map(|t| t.hash)
                        .collect();
                    if !hashes.is_empty() {
                        let _ = self
                            .action_tx
                            .send(Action::EnrichAddressTxs { address, hashes });
                    }
                }

                // Update per-source cursors from streamed data
                if !self.address.txs.items.is_empty() {
                    let min_block = self
                        .address
                        .txs
                        .items
                        .iter()
                        .filter(|t| t.block_number > 0)
                        .map(|t| t.block_number)
                        .min();
                    if let Some(min_b) = min_block {
                        self.address.oldest_event_block = Some(
                            self.address
                                .oldest_event_block
                                .map_or(min_b, |old| old.min(min_b)),
                        );
                        match source {
                            Source::Dune => {
                                self.address.dune_cursor_block = Some(min_b);
                                self.address.dune_has_more = true; // Assume more until proven otherwise
                            }
                            Source::Rpc => {
                                self.address.rpc_cursor_block = Some(min_b);
                                self.address.rpc_has_more = true;
                            }
                            _ => {}
                        }
                    }
                }

                // Track source completion
                if complete {
                    self.address.sources_pending.remove(&source);
                    debug!(
                        address = %format!("{:#x}", address),
                        source = ?source,
                        sources_pending = ?self.address.sources_pending,
                        "AddressTxsStreamed source completed"
                    );
                    if self.address.sources_pending.is_empty() {
                        self.is_loading = false;
                        self.loading_detail = None;

                        // Post-display: detect any large nonce gap and defer it for
                        // on-demand fill (issue #10). Small gaps + endpoint
                        // enrichment still run automatically.
                        if !self.address.is_contract {
                            if let Some(info) = &self.address.info {
                                let current_nonce = crate::utils::felt_to_u64(&info.nonce);
                                self.address.unfilled_gap = self.address.detect_unfilled_gap();
                                self.address.sanity_check_dispatched = true;
                                let _ = self.action_tx.send(Action::EnrichAddressEndpoints {
                                    address,
                                    current_nonce,
                                    known_txs: self.address.txs.items.clone(),
                                });
                            }
                        }
                    }
                }

                // Save to cache after each source completes
                if complete && !self.address.txs.items.is_empty() {
                    let _ = self.action_tx.send(Action::LoadingStatus(format!(
                        "Loaded {} txs",
                        self.address.txs.items.len()
                    )));
                }
            }
            Action::AddressWsEvent { address, event } => {
                // Fan-out hub for WS-received events. The event has already
                // been persisted to the per-address event cache in ws.rs; this
                // reducer is purely about updating in-memory tab state and
                // dispatching enrichment / classification follow-ups.
                if self.address.context != Some(address) {
                    return;
                }

                // --- Events tab: delegate decode to the network task (which
                // owns the async ABI registry); the resulting
                // `AddressEventStreamed` arm below merges the decoded row.
                let _ = self.action_tx.send(Action::DecodeAddressWsEvent {
                    address,
                    event: event.clone(),
                });

                // --- Calls tab: derive a stub summary and merge (dedupe by tx_hash).
                let tx_hash = event.transaction_hash;
                let block_number = event.block_number;
                let already_present = self
                    .address
                    .calls
                    .items
                    .iter()
                    .any(|c| c.tx_hash == tx_hash);
                if !already_present {
                    let stub = crate::data::types::ContractCallSummary {
                        tx_hash,
                        sender: Felt::ZERO, // filled in by EnrichAddressCalls
                        function_name: String::new(),
                        block_number,
                        timestamp: 0,
                        total_fee_fri: 0,
                        status: "OK".to_string(), // events only fire for successful txs
                        nonce: None,              // filled in by EnrichAddressCalls
                        tip: 0,
                    };
                    let _ = self.action_tx.send(Action::EnrichAddressCalls {
                        address,
                        hashes_with_blocks: vec![(tx_hash, block_number)],
                    });
                    self.address.calls.items.push(stub);
                    self.address
                        .calls
                        .items
                        .sort_by(|a, b| b.block_number.cmp(&a.block_number));
                    if self.address.calls.state.selected().is_none() {
                        self.address.calls.select_first();
                    }
                    // Auto-switch to Calls only if the user is still on the
                    // default Transactions tab. Never yank them away from a
                    // tab they deliberately navigated to.
                    if self.address.tab == AddressTab::Transactions
                        && self.address.txs.items.is_empty()
                    {
                        self.address.tab = AddressTab::Calls;
                    }
                }

                // --- MetaTxs: for viewed accounts, any TRANSACTION_EXECUTED
                // event might be an execute_from_outside. Dispatch the
                // classifier to check and populate the MetaTxs tab live.
                if event
                    .keys
                    .first()
                    .map(|k| *k == crate::network::ws::tx_executed_selector())
                    .unwrap_or(false)
                {
                    let _ = self
                        .action_tx
                        .send(Action::ClassifyPotentialMetaTx { address, tx_hash });
                }
            }
            Action::AddressEventStreamed {
                address,
                decoded_event,
            } => {
                // Response arm for DecodeAddressWsEvent. Merge the decoded
                // event into the Events tab list newest-first, dedup by
                // `(tx_hash, event_index)` — the same key the cache uses. The
                // event is already persisted (the WS handler called
                // `merge_address_events` before broadcasting), so this is
                // purely an in-memory refresh; a future cache reload will
                // produce the same list via the cache path.
                if self.address.context != Some(address) {
                    return;
                }
                let key = (
                    decoded_event.raw.transaction_hash,
                    decoded_event.raw.event_index,
                );
                let already_present = self
                    .address
                    .events
                    .items
                    .iter()
                    .any(|e| (e.raw.transaction_hash, e.raw.event_index) == key);
                if already_present {
                    return;
                }
                self.address.events.items.insert(0, decoded_event);
                // `insert(0, …)` preserves newest-first ordering as long as
                // WS events arrive in block order, which they do. No sort.
                if self.address.events.state.selected().is_none() {
                    self.address.events.select_first();
                }
            }
            Action::AddressEventsCacheLoaded {
                address,
                decoded_events,
            } => {
                // Deferred decode pass for the events cached at entry time.
                // `fetch_and_send_address_info` now sends `AddressInfoLoaded`
                // immediately with empty `decoded_events` so the TUI can drop
                // the "Fetching address info…" spinner while this expensive
                // per-event ABI decode runs in a background task.
                if self.address.context != Some(address) {
                    return; // stale — user navigated away
                }
                if decoded_events.is_empty() {
                    return;
                }
                let had_selection = self.address.events.state.selected().is_some();
                self.address.events.items = decoded_events;
                if !had_selection {
                    self.address.events.select_first();
                }
            }
            Action::AddressCallsEnriched { address, calls } => {
                if self.address.context != Some(address) {
                    return;
                }
                for enriched in calls {
                    if let Some(existing) = self
                        .address
                        .calls
                        .items
                        .iter_mut()
                        .find(|c| c.tx_hash == enriched.tx_hash)
                    {
                        // Upgrade placeholder fields with real data from RPC
                        if existing.sender == Felt::ZERO && enriched.sender != Felt::ZERO {
                            existing.sender = enriched.sender;
                        }
                        if existing.function_name.is_empty() && !enriched.function_name.is_empty() {
                            existing.function_name = enriched.function_name;
                        }
                        if existing.total_fee_fri == 0 && enriched.total_fee_fri > 0 {
                            existing.total_fee_fri = enriched.total_fee_fri;
                        }
                        if existing.timestamp == 0 && enriched.timestamp > 0 {
                            existing.timestamp = enriched.timestamp;
                        }
                        if existing.status == "?" && enriched.status != "?" {
                            existing.status = enriched.status;
                        }
                        if existing.nonce.is_none() && enriched.nonce.is_some() {
                            existing.nonce = enriched.nonce;
                        }
                        if existing.tip == 0 && enriched.tip > 0 {
                            existing.tip = enriched.tip;
                        }
                    }
                }
                // Persist enriched calls to cache so they survive restarts
                if !self.address.calls.items.is_empty() {
                    let _ = self.action_tx.send(Action::PersistAddressCalls {
                        address,
                        calls: self.address.calls.items.clone(),
                    });
                }
            }
            Action::AddressCallsMerged { address, calls } => {
                // Plan §2: pf-query tx_rows supplement Dune's capped Calls set.
                // Dune's `starknet.calls` query is LIMIT 500 which collapses to
                // far fewer unique tx_hashes after dedup for addresses with
                // many multi-call txs (e.g. meta-tx-heavy accounts). The
                // meta-tx scan already walks the right tx_rows — merge those
                // rows so MetaTxs ⊆ Calls stays visible.
                if self.address.context != Some(address) || calls.is_empty() {
                    return;
                }
                self.address.merge_calls(calls);
                if self.address.calls.state.selected().is_none()
                    && !self.address.calls.items.is_empty()
                {
                    self.address.calls.select_first();
                }
                // Persist the merged set so it survives restarts.
                let _ = self.action_tx.send(Action::PersistAddressCalls {
                    address,
                    calls: self.address.calls.items.clone(),
                });
            }
            Action::AddressBalancesLoaded { address, balances } => {
                if self.address.context == Some(address) {
                    // Balances can beat the nonce/class_hash fetch to the UI —
                    // seed a minimal info so the result isn't dropped while
                    // `info` is still `None` waiting on `AddressInfoLoaded`.
                    if self.address.info.is_none() {
                        self.address.info = Some(crate::data::types::SnAddressInfo {
                            address,
                            nonce: starknet::core::types::Felt::ZERO,
                            class_hash: None,
                            recent_events: Vec::new(),
                            token_balances: Vec::new(),
                        });
                    }
                    if let Some(info) = &mut self.address.info {
                        info.token_balances = balances;
                    }
                    self.dispatch_balance_price_fetch();
                }
            }
            Action::PricesUpdated => {
                // Cache reads happen on next draw; no app state to update.
            }
            Action::VoyagerLabelLoaded { address, label } => {
                // Add to search index so Voyager labels are searchable
                if let Some(name) = &label.name {
                    if let Some(engine) = &self.search_engine {
                        engine.registry().add_voyager_label(address, name);
                    }
                }
                self.voyager_labels.insert(address, label);
            }
            Action::LoadingStatus(msg) => {
                self.is_loading = true;
                self.loading_detail = Some(msg);
            }
            Action::SetActiveQuery { key, label } => match label {
                Some(l) => self.active_queries.set(&key, l),
                None => self.active_queries.clear(&key),
            },
            Action::SourceUpdate { source, status } => {
                use crate::app::actions::Source;
                let target = match source {
                    Source::Rpc => &mut self.data_sources.rpc,
                    Source::Ws => &mut self.data_sources.ws,
                    Source::Pathfinder => &mut self.data_sources.pathfinder,
                    Source::Dune => &mut self.data_sources.dune,
                    Source::Voyager => &mut self.data_sources.voyager,
                };
                *target = status;
            }
            Action::LatestBlockNumber(n) => {
                self.latest_block_number = n;
            }
            Action::NavigateToClassInfo { class_hash } => {
                self.clear_class_info();
                self.class.hash = Some(class_hash);
                self.is_loading = true;
                self.loading_detail = Some("Fetching class info...".into());
                if self.current_view() != View::ClassInfo {
                    self.push_view(View::ClassInfo);
                }
            }
            Action::ClassAbiLoaded { class_hash, abi } => {
                if self.class.hash == Some(class_hash) {
                    self.class.abi = abi;
                    self.class.abi_loaded = true;
                    self.build_class_nav_items();
                    // If declare+contracts already arrived, clear loading
                    if self.class.declare.is_some() || self.class.decl_block.is_some() {
                        self.is_loading = false;
                        self.loading_detail = None;
                    }
                }
            }
            Action::ClassDeclareLoaded {
                class_hash,
                declare_info,
            } => {
                if self.class.hash == Some(class_hash) {
                    self.class.declare = declare_info;
                    self.build_class_nav_items();
                    // If all data has arrived, clear loading
                    if self.class.abi_loaded {
                        self.is_loading = false;
                        self.loading_detail = None;
                    }
                }
            }
            Action::ClassContractsLoaded {
                class_hash,
                contracts,
                declaration_block,
            } => {
                if self.class.hash == Some(class_hash) {
                    self.class.contracts = contracts;
                    self.class.decl_block = declaration_block;
                    self.build_class_nav_items();
                    // If all data has arrived, clear loading
                    if self.class.abi_loaded {
                        self.is_loading = false;
                        self.loading_detail = None;
                    }
                }
            }
            Action::Error(msg) => {
                self.error_message = Some(msg);
                self.is_loading = false;
            }
            Action::PeriodicAddressPollTick => {
                // Only refresh when the user is actually sitting on an address
                // view, WS isn't live (otherwise the WS stream covers this),
                // and the context is an account (contracts don't have a
                // meaningful account nonce and are topped up via other paths).
                if self.current_view() == View::AddressInfo
                    && self.data_sources.ws != SourceStatus::Live
                    && !self.address.is_contract
                {
                    if let Some(address) = self.address.context {
                        let _ = self.action_tx.send(Action::RefreshAddressRpc { address });
                    }
                }
            }
            // Request actions are not handled here (they go to the network task)
            _ => {}
        }
    }
}
