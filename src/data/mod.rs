pub mod cache;
pub mod pathfinder;
pub mod rpc;
pub mod types;

use async_trait::async_trait;
use starknet::core::types::Felt;

use starknet::core::types::ContractClass;

use crate::error::Result;
use types::{AddressTxSummary, ContractCallSummary, SnBlock, SnEvent, SnReceipt, SnTransaction};

/// Abstraction over different Starknet data sources (RPC, Pathfinder DB).
#[async_trait]
pub trait DataSource: Send + Sync {
    async fn get_latest_block_number(&self) -> Result<u64>;
    async fn get_block(&self, number: u64) -> Result<SnBlock>;
    async fn get_block_by_hash(&self, hash: Felt) -> Result<u64>;
    async fn get_block_with_txs(&self, number: u64) -> Result<(SnBlock, Vec<SnTransaction>)>;
    async fn get_transaction(&self, hash: Felt) -> Result<SnTransaction>;
    async fn get_receipt(&self, hash: Felt) -> Result<SnReceipt>;
    async fn get_nonce(&self, address: Felt) -> Result<Felt>;
    async fn get_class_hash(&self, address: Felt) -> Result<Felt>;
    async fn get_class(&self, class_hash: Felt) -> Result<ContractClass>;
    async fn get_recent_blocks(&self, count: usize) -> Result<Vec<SnBlock>>;
    /// Fetch recent events emitted by or targeting an address.
    ///
    /// `to_block` is an inclusive upper bound; `None` means "up to latest".
    /// Used by pagination to fetch events strictly older than a cursor.
    async fn get_events_for_address(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SnEvent>>;
    /// Load cached tx summaries for an address (returns empty if none).
    fn load_cached_address_txs(&self, _address: &Felt) -> Vec<AddressTxSummary> {
        Vec::new() // Default: no cache. CachingDataSource overrides.
    }
    /// Save tx summaries for an address to persistent cache.
    fn save_address_txs(&self, _address: &Felt, _txs: &[AddressTxSummary]) {
        // Default: no-op. CachingDataSource overrides.
    }
    /// Load cached contract call summaries for an address.
    fn load_cached_address_calls(&self, _address: &Felt) -> Vec<ContractCallSummary> {
        Vec::new()
    }
    /// Save contract call summaries for an address to persistent cache.
    fn save_address_calls(&self, _address: &Felt, _calls: &[ContractCallSummary]) {
        // Default: no-op. CachingDataSource overrides.
    }
    /// Load cached activity range for an address (min_block, max_block, event_count).
    /// Returns None if not cached or stale (> 1 hour).
    fn load_cached_activity_range(&self, _address: &Felt) -> Option<(u64, u64)> {
        None // Default: no cache. CachingDataSource overrides.
    }
    /// Load cached activity range with event count.
    fn load_cached_activity_range_with_count(&self, _address: &Felt) -> Option<(u64, u64, u64)> {
        None
    }
    /// Save discovered activity range for an address.
    fn save_activity_range(&self, _address: &Felt, _min_block: u64, _max_block: u64) {
        // Default: no-op. CachingDataSource overrides.
    }
    /// Save discovered activity range with event count.
    fn save_activity_range_with_count(
        &self,
        _address: &Felt,
        _min_block: u64,
        _max_block: u64,
        _event_count: u64,
    ) {
        // Default: no-op. CachingDataSource overrides.
    }
    /// Fetch events emitted by a contract (all events, not just transaction_executed).
    /// Used for finding calls TO a contract.
    ///
    /// `to_block` is an inclusive upper bound; `None` means "up to latest".
    async fn get_contract_events(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SnEvent>> {
        // Default: same as get_events_for_address (overridden in RPC impl)
        self.get_events_for_address(address, from_block, to_block, limit)
            .await
    }
    /// Call a contract view function (e.g., balance_of).
    async fn call_contract(
        &self,
        contract_address: Felt,
        selector: Felt,
        calldata: Vec<Felt>,
    ) -> Result<Vec<Felt>>;

    // --- Deploy info cache ---
    /// Load cached deploy tx info for an address. Returns (tx_hash, block, deployer).
    fn load_cached_deploy_info(&self, _address: &Felt) -> Option<(Felt, u64, Option<Felt>)> {
        None
    }
    /// Save deploy tx info for an address.
    fn save_deploy_info(
        &self,
        _address: &Felt,
        _tx_hash: &Felt,
        _block: u64,
        _deployer: Option<&Felt>,
    ) {
    }

    // --- Nonce cache ---
    /// Load cached nonce + block number for an address.
    fn load_cached_nonce(&self, _address: &Felt) -> Option<(Felt, u64)> {
        None
    }
    /// Save nonce + block number for an address.
    fn save_cached_nonce(&self, _address: &Felt, _nonce: &Felt, _block: u64) {}

    // --- Search progress cache ---
    /// Load cached search progress (min_searched_block, max_searched_block).
    fn load_search_progress(&self, _address: &Felt) -> Option<(u64, u64)> {
        None
    }
    /// Save search progress for an address.
    fn save_search_progress(&self, _address: &Felt, _min_block: u64, _max_block: u64) {}
}
