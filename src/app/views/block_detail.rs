//! State for the block detail view (single block header + transaction list).

use starknet::core::types::Felt;

use crate::data::types::{SnBlock, SnTransaction};
use crate::decode::outside_execution::OutsideExecutionVersion;
use crate::ui::widgets::stateful_list::StatefulList;

/// Lightweight meta tx summary for the block view (no inner calls, just intender + version).
#[derive(Debug, Clone)]
pub struct MetaTxSummary {
    pub intender: Felt,
    pub version: OutsideExecutionVersion,
}

/// All state related to the block detail view.
#[derive(Default)]
pub struct BlockDetailState {
    pub block: Option<SnBlock>,
    pub txs: StatefulList<SnTransaction>,
    /// Decoded endpoint/function name per tx (by index in txs).
    pub endpoint_names: Vec<Option<String>>,
    /// Execution status per tx: "OK", "REV", "?"
    pub tx_statuses: Vec<String>,
    /// Outside execution summary per tx. Some for meta txs, None otherwise.
    pub meta_tx_info: Vec<Option<MetaTxSummary>>,
    /// True for any tx that touches the privacy pool (top-level call or
    /// OE-inner call). Drives the orange shield marker in the "Prv"
    /// column on the block tx list.
    pub is_privacy_tx: Vec<bool>,
    /// Whether visual mode (sender selection) is active.
    pub visual_mode: bool,
    /// Cursor index into txs (only meaningful when visual_mode is true).
    pub nav_cursor: usize,
}

impl BlockDetailState {
    /// Clear all block detail data. Called when navigating to a new block.
    pub fn clear(&mut self) {
        self.block = None;
        self.txs = StatefulList::new();
        self.endpoint_names = Vec::new();
        self.tx_statuses = Vec::new();
        self.meta_tx_info = Vec::new();
        self.is_privacy_tx = Vec::new();
        self.visual_mode = false;
        self.nav_cursor = 0;
    }
}
