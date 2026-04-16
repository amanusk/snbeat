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

use crate::data::types::{AddressTxSummary, SnBlock, VoyagerLabelInfo};
use crate::network::ws::WsSubscriptionManager;
use crate::registry::SearchResult;
use crate::search::SearchEngine;
use crate::ui::widgets::stateful_list::StatefulList;
use actions::{Action, Source};
use starknet::core::types::Felt;
use state::{ConnectionStatus, DataSources, Focus, InputMode, NavTarget, View};
use views::{AddressInfoState, BlockDetailState, ClassInfoState, TxDetailState};

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

    // Pagination flags (prevent duplicate fetches)
    pub fetching_older_blocks: bool,

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

            fetching_older_blocks: false,

            voyager_labels: HashMap::new(),

            action_tx,

            forward_history: Vec::new(),

            ws_manager: None,
        }
    }

    fn filter_deployment_txs(
        &mut self,
        address: starknet::core::types::Felt,
        txs: Vec<AddressTxSummary>,
    ) -> Vec<AddressTxSummary> {
        self.address.filter_deployment_txs(address, txs)
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
            if self.view_stack.iter().any(|v| *v == View::AddressInfo) {
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
                }
                AddressTab::Calls => {
                    self.address.calls.next();
                    self.maybe_fetch_more_address_txs();
                }
                AddressTab::Events => {
                    if self.address.event_scroll < self.address.events.len().saturating_sub(1) {
                        self.address.event_scroll += 1;
                    }
                }
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
                AddressTab::Transactions => self.address.txs.previous(),
                AddressTab::Calls => self.address.calls.previous(),
                AddressTab::Events => {
                    self.address.event_scroll = self.address.event_scroll.saturating_sub(1);
                }
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
                AddressTab::Transactions => self.address.txs.select_first(),
                AddressTab::Calls => self.address.calls.select_first(),
                AddressTab::Events => self.address.event_scroll = 0,
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
                self.maybe_fetch_older_blocks();
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
                }
                AddressTab::Calls => {
                    self.address.calls.select_last();
                    self.maybe_fetch_more_address_txs();
                }
                AddressTab::Events => {
                    self.address.event_scroll = self.address.events.len().saturating_sub(1);
                }
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
        // Don't fetch if no source thinks there's more data
        if !self.address.has_more_data()
            && self.address.oldest_event_block.is_some()
            && self.address.sources_pending.is_empty()
        {
            return;
        }
        let near_bottom = match self.address.tab {
            AddressTab::Transactions => self.address.txs.is_near_bottom(5),
            AddressTab::Calls => self.address.calls.is_near_bottom(5),
            _ => false,
        };
        if near_bottom {
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
            }
            Action::OlderBlocksLoaded(blocks) => {
                // Append older blocks to the end of the list (they are already sorted newest-first)
                self.blocks.items.extend(blocks);
                // Keep list bounded to avoid unbounded memory growth
                if self.blocks.items.len() > 500 {
                    self.blocks.items.truncate(500);
                }
                self.fetching_older_blocks = false;
            }
            Action::NewBlock(block) => {
                self.latest_block_number = block.number;
                // If the user has scrolled away from the top, keep focus on the
                // same block by bumping the selection index.  When the selection
                // is at 0 (the newest block) we leave it there so the list
                // always tracks the latest block.
                if let Some(sel) = self.blocks.state.selected() {
                    if sel > 0 {
                        self.blocks.state.select(Some(sel + 1));
                    }
                }
                self.blocks.items.insert(0, block);
                // Keep list bounded
                if self.blocks.items.len() > 200 {
                    self.blocks.items.truncate(200);
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
            } => {
                self.tx_detail.transaction = Some(transaction);
                self.tx_detail.receipt = Some(receipt);
                self.tx_detail.decoded_events = decoded_events;
                self.tx_detail.decoded_calls = decoded_calls;
                self.tx_detail.outside_executions = outside_executions;
                self.tx_detail.scroll = 0;
                self.tx_detail.visual_mode = false;
                self.build_tx_nav_items();
                self.is_loading = false;
                self.loading_detail = None;
                if self.current_view() != View::TxDetail {
                    self.push_view(View::TxDetail);
                }
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

                // Only update info if it has real data
                if info.nonce != starknet::core::types::Felt::ZERO
                    || !info.token_balances.is_empty()
                    || info.class_hash.is_some()
                {
                    self.address.info = Some(info);
                } else if self.address.info.is_none() {
                    self.address.info = Some(info);
                }

                if !decoded_events.is_empty() {
                    self.address.events = decoded_events;
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

                // Update contract calls (merge, not replace)
                if !contract_calls.is_empty() {
                    let existing_hashes: std::collections::HashSet<_> =
                        self.address.calls.items.iter().map(|c| c.tx_hash).collect();
                    let new_calls: Vec<_> = contract_calls
                        .into_iter()
                        .filter(|c| !existing_hashes.contains(&c.tx_hash))
                        .collect();
                    if !new_calls.is_empty() {
                        self.address.calls.items.extend(new_calls);
                        self.address
                            .calls
                            .items
                            .sort_by(|a, b| b.block_number.cmp(&a.block_number));
                    }
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

                    // Sanity check cached data: fill nonce gaps + enrich endpoints
                    // from what we already have in the cache, even before sources complete.
                    if !self.address.is_contract
                        && !self.address.txs.items.is_empty()
                        && !self.address.sanity_check_dispatched
                    {
                        if let Some(info) = &self.address.info {
                            let current_nonce = crate::utils::felt_to_u64(&info.nonce);
                            if current_nonce > 0 {
                                self.address.sanity_check_dispatched = true;
                                let _ = self.action_tx.send(Action::SanityCheckAddress {
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
                    // Merge contract calls
                    if !contract_calls.is_empty() {
                        let existing_hashes: std::collections::HashSet<_> =
                            self.address.calls.items.iter().map(|c| c.tx_hash).collect();
                        let new_calls: Vec<_> = contract_calls
                            .into_iter()
                            .filter(|c| !existing_hashes.contains(&c.tx_hash))
                            .collect();
                        self.address.calls.items.extend(new_calls);
                        self.address
                            .calls
                            .items
                            .sort_by(|a, b| b.block_number.cmp(&a.block_number));
                    }
                    self.address.oldest_event_block = Some(oldest_block);
                    self.address.dune_cursor_block = Some(oldest_block);
                    self.address.rpc_cursor_block = Some(oldest_block);
                    self.address.dune_has_more = has_more;
                    self.address.rpc_has_more = has_more;
                }
                self.address.fetching_more_txs = false;
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
                // Merge enrichment data (upgrades existing entries)
                self.address.merge_tx_summaries(updates);
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
            Action::AddressTxsStreamed {
                address,
                source,
                tx_summaries,
                complete,
            } => {
                if self.address.context != Some(address) {
                    return;
                }
                let old_deploy_hash = self.address.deployment.as_ref().map(|d| d.hash);
                let tx_summaries = self.filter_deployment_txs(address, tx_summaries);
                let new_deploy_hash = self.address.deployment.as_ref().map(|d| d.hash);
                // Rebuild nav items if deployment info arrived or hash was upgraded
                if old_deploy_hash != new_deploy_hash {
                    self.build_address_nav_items();
                }
                self.address.merge_tx_summaries(tx_summaries);

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
                    if self.address.sources_pending.is_empty() {
                        self.is_loading = false;
                        self.loading_detail = None;

                        // Post-display sanity check: fill nonce gaps + enrich all endpoints.
                        // Reset the flag so we run again with the full merged dataset.
                        if !self.address.is_contract {
                            if let Some(info) = &self.address.info {
                                let current_nonce = crate::utils::felt_to_u64(&info.nonce);
                                self.address.sanity_check_dispatched = true;
                                let _ = self.action_tx.send(Action::SanityCheckAddress {
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
            Action::AddressCallsStreamed { address, calls } => {
                if self.address.context != Some(address) {
                    return;
                }
                if calls.is_empty() {
                    return;
                }
                let existing: std::collections::HashSet<_> =
                    self.address.calls.items.iter().map(|c| c.tx_hash).collect();
                let new_calls: Vec<_> = calls
                    .into_iter()
                    .filter(|c| !existing.contains(&c.tx_hash))
                    .collect();
                if !new_calls.is_empty() {
                    // Dispatch enrichment for the new stubs (fetch sender/function/fee/timestamp)
                    let hashes_with_blocks: Vec<_> = new_calls
                        .iter()
                        .map(|c| (c.tx_hash, c.block_number))
                        .collect();
                    let _ = self.action_tx.send(Action::EnrichAddressCalls {
                        address,
                        hashes_with_blocks,
                    });

                    self.address.calls.items.extend(new_calls);
                    self.address
                        .calls
                        .items
                        .sort_by(|a, b| b.block_number.cmp(&a.block_number));
                    if self.address.calls.state.selected().is_none() {
                        self.address.calls.select_first();
                    }
                    // Auto-switch to Calls tab if Txs tab is empty
                    if self.address.txs.items.is_empty() {
                        self.address.tab = AddressTab::Calls;
                    }
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
            Action::AddressBalancesLoaded { address, balances } => {
                if self.address.context == Some(address) {
                    if let Some(info) = &mut self.address.info {
                        info.token_balances = balances;
                    }
                }
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
            // Request actions are not handled here (they go to the network task)
            _ => {}
        }
    }
}
