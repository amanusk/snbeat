//! State for the transaction detail view (full tx inspection with event tree).

use std::collections::HashSet;

use super::super::state::TxNavItem;
use crate::data::types::{SnReceipt, SnTransaction};
use crate::decode::events::DecodedEvent;
use crate::decode::functions::RawCall;
use crate::decode::outside_execution::OutsideExecutionInfo;

/// All state related to the transaction detail view.
#[derive(Default)]
pub struct TxDetailState {
    pub transaction: Option<SnTransaction>,
    pub receipt: Option<SnReceipt>,
    pub decoded_events: Vec<DecodedEvent>,
    pub decoded_calls: Vec<RawCall>,
    pub scroll: u16,
    /// Navigable items in the current tx (rebuilt on load).
    pub nav_items: Vec<TxNavItem>,
    /// Cursor index into nav_items (only meaningful when visual_mode is true).
    pub nav_cursor: usize,
    /// First line index for each nav_items entry (written by the renderer each frame).
    pub nav_item_lines: Vec<u16>,
    /// Whether visual mode (item selection) is active.
    pub visual_mode: bool,
    /// Whether to expand raw calldata felts under each call.
    pub show_calldata: bool,
    /// Whether to show ABI-decoded calldata under each call.
    pub show_decoded_calldata: bool,
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
        self.scroll = 0;
        self.visual_mode = false;
        self.show_outside_execution = false;
        self.nav_items = Vec::new();
        self.nav_cursor = 0;
        self.nav_item_lines = Vec::new();
        self.block_timestamp = None;
    }

    /// Build the list of navigable items for the current transaction.
    /// Order: block -> sender -> call contracts -> event contracts -> address-typed params.
    pub fn build_nav_items<F>(&mut self, is_known: F)
    where
        F: Fn(&starknet::core::types::Felt) -> bool,
    {
        let mut items: Vec<TxNavItem> = Vec::new();
        let mut seen: HashSet<starknet::core::types::Felt> = HashSet::new();

        // Block number
        let block_num = self
            .receipt
            .as_ref()
            .map(|r| r.block_number)
            .or_else(|| self.transaction.as_ref().map(|tx| tx.block_number()));
        if let Some(n) = block_num {
            items.push(TxNavItem::Block(n));
        }

        // Sender
        if let Some(tx) = &self.transaction {
            let sender = tx.sender();
            if seen.insert(sender) {
                items.push(TxNavItem::Address(sender));
            }

            // Class hash for Declare txs
            if let crate::data::types::SnTransaction::Declare(decl) = tx {
                items.push(TxNavItem::ClassHash(decl.class_hash));
            }
        }

        // Deployed contract addresses (via UDC)
        for addr in crate::decode::events::extract_deployed_addresses(&self.decoded_events) {
            if seen.insert(addr) {
                items.push(TxNavItem::Address(addr));
            }
        }

        // Call contract addresses
        for call in &self.decoded_calls {
            if seen.insert(call.contract_address) {
                items.push(TxNavItem::Address(call.contract_address));
            }
        }

        // Outside execution intender and inner call addresses
        for (_, oe) in &self.outside_executions {
            if seen.insert(oe.intender) {
                items.push(TxNavItem::Address(oe.intender));
            }
            for inner in &oe.inner_calls {
                if seen.insert(inner.contract_address) {
                    items.push(TxNavItem::Address(inner.contract_address));
                }
            }
        }

        // Event contract addresses
        for event in &self.decoded_events {
            if seen.insert(event.contract_address) {
                items.push(TxNavItem::Address(event.contract_address));
            }
        }

        // Address-typed event params + untyped params that resolve to known labels
        for event in &self.decoded_events {
            for p in event.decoded_keys.iter().chain(event.decoded_data.iter()) {
                let type_name = p.type_name.as_deref().unwrap_or("");
                let is_address_type = type_name.contains("ContractAddress");
                let is_known_label = !is_address_type
                    && type_name.is_empty()
                    && p.value != starknet::core::types::Felt::ZERO
                    && is_known(&p.value);
                if (is_address_type || is_known_label) && seen.insert(p.value) {
                    items.push(TxNavItem::Address(p.value));
                }
            }
        }

        self.nav_items = items;
        self.nav_cursor = 0;
        self.nav_item_lines = Vec::new();
    }

    /// Step the visual-mode cursor by `delta` (wrapping), then scroll to keep it visible.
    pub fn nav_step(&mut self, delta: i64) {
        if self.nav_items.is_empty() {
            return;
        }
        let len = self.nav_items.len() as i64;
        let next = (self.nav_cursor as i64 + delta).rem_euclid(len) as usize;
        self.nav_cursor = next;
        // Scroll the view so the selected item is visible (2 lines of context above).
        if let Some(&line) = self.nav_item_lines.get(next) {
            self.scroll = line.saturating_sub(2);
        }
    }
}
