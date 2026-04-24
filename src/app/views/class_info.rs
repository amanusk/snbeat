//! State for the class info view (ABI, declaration info, deployed contracts).

use std::sync::Arc;

use starknet::core::types::Felt;

use crate::app::state::TxNavItem;
use crate::data::types::{ClassContractEntry, ClassDeclareInfo};
use crate::decode::abi::ParsedAbi;

/// All state related to the class info view.
#[derive(Default)]
pub struct ClassInfoState {
    pub hash: Option<Felt>,
    pub abi: Option<Arc<ParsedAbi>>,
    pub abi_loaded: bool,
    pub declare: Option<ClassDeclareInfo>,
    pub contracts: Vec<ClassContractEntry>,
    pub decl_block: Option<u64>,
    pub scroll: u16,
    pub nav_items: Vec<TxNavItem>,
    pub nav_cursor: usize,
    pub nav_item_lines: Vec<u16>,
    pub visual_mode: bool,
    /// Whether to expand the ABI section.
    pub show_abi: bool,
}

impl ClassInfoState {
    /// Clear all class info data. Called when navigating to a new class hash.
    pub fn clear(&mut self) {
        self.hash = None;
        self.abi = None;
        self.abi_loaded = false;
        self.declare = None;
        self.contracts = Vec::new();
        self.decl_block = None;
        self.scroll = 0;
        self.visual_mode = false;
        self.nav_items = Vec::new();
        self.nav_cursor = 0;
        self.nav_item_lines = Vec::new();
    }

    /// Build the list of navigable items for the ClassInfo view.
    pub fn build_nav_items(&mut self) {
        let mut items: Vec<TxNavItem> = Vec::new();
        let mut seen_addrs: std::collections::HashSet<Felt> = std::collections::HashSet::new();

        // Declaration block
        if let Some(n) = self.decl_block {
            items.push(TxNavItem::Block(n));
        }

        // Declare tx + sender
        if let Some(decl) = &self.declare {
            items.push(TxNavItem::Transaction(decl.tx_hash));
            if seen_addrs.insert(decl.sender) {
                items.push(TxNavItem::Address(decl.sender));
            }
        }

        // Deployed contracts
        for entry in &self.contracts {
            if seen_addrs.insert(entry.address) {
                items.push(TxNavItem::Address(entry.address));
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
        if let Some(&line) = self.nav_item_lines.get(next) {
            self.scroll = line.saturating_sub(2);
        }
    }
}
