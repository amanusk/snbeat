//! State for the address info view (tx history, balances, calls, events, class history).

use std::collections::HashSet;

use starknet::core::types::Felt;

use crate::app::AddressTab;
use crate::app::actions::Source;
use crate::app::state::TxNavItem;
use crate::data::types::{AddressTxSummary, ContractCallSummary, SnAddressInfo};
use crate::decode::events::DecodedEvent;
use crate::network::dune::AddressActivityProbe;
use crate::ui::widgets::stateful_list::StatefulList;

/// All state related to the address info view.
pub struct AddressInfoState {
    pub info: Option<SnAddressInfo>,
    pub events: Vec<DecodedEvent>,
    /// Enriched tx summaries for the Transactions tab.
    pub txs: StatefulList<AddressTxSummary>,
    /// The DEPLOY_ACCOUNT tx that created this address (filtered out of txs).
    pub deployment: Option<AddressTxSummary>,
    /// Incoming calls to this contract (for the Calls tab).
    pub calls: StatefulList<ContractCallSummary>,
    /// Whether this address is a contract (nonce == 0) vs an account.
    pub is_contract: bool,
    /// Scrollable index for events tab.
    pub event_scroll: usize,
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
}

impl Default for AddressInfoState {
    fn default() -> Self {
        Self {
            info: None,
            events: Vec::new(),
            txs: StatefulList::new(),
            deployment: None,
            calls: StatefulList::new(),
            is_contract: false,
            event_scroll: 0,
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
        }
    }
}

impl AddressInfoState {
    /// Clear all address info data. Called when navigating to a new address.
    pub fn clear(&mut self) {
        self.info = None;
        self.events.clear();
        self.event_scroll = 0;
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
        if let Some(info) = &self.info {
            if let Some(ch) = info.class_hash {
                items.push(TxNavItem::ClassHash(ch));
            }
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
            if let Some(sender) = deploy.sender {
                if let Some(info) = &self.info {
                    if sender != info.address && sender != Felt::ZERO {
                        items.push(TxNavItem::Address(sender));
                    }
                }
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
    if existing.sender.is_none() && incoming.sender.is_some() {
        existing.sender = incoming.sender;
    }
    if existing.tip == 0 && incoming.tip > 0 {
        existing.tip = incoming.tip;
    }
}
