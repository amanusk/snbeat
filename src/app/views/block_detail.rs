//! State for the block detail view (single block header + transaction list).

use crate::data::types::{SnBlock, SnTransaction};
use crate::ui::widgets::stateful_list::StatefulList;

/// All state related to the block detail view.
pub struct BlockDetailState {
    pub block: Option<SnBlock>,
    pub txs: StatefulList<SnTransaction>,
    /// Decoded endpoint/function name per tx (by index in txs).
    pub endpoint_names: Vec<Option<String>>,
    /// Execution status per tx: "OK", "REV", "?"
    pub tx_statuses: Vec<String>,
    /// Whether visual mode (sender selection) is active.
    pub visual_mode: bool,
    /// Cursor index into txs (only meaningful when visual_mode is true).
    pub nav_cursor: usize,
}

impl Default for BlockDetailState {
    fn default() -> Self {
        Self {
            block: None,
            txs: StatefulList::new(),
            endpoint_names: Vec::new(),
            tx_statuses: Vec::new(),
            visual_mode: false,
            nav_cursor: 0,
        }
    }
}

impl BlockDetailState {
    /// Clear all block detail data. Called when navigating to a new block.
    pub fn clear(&mut self) {
        self.block = None;
        self.txs = StatefulList::new();
        self.endpoint_names = Vec::new();
        self.tx_statuses = Vec::new();
        self.visual_mode = false;
        self.nav_cursor = 0;
    }
}
