use starknet::core::types::Felt;

use crate::app::state::SourceStatus;
use crate::data::pathfinder::ClassHashEntry;
use crate::data::types::{
    AddressTxSummary, ClassContractEntry, ClassDeclareInfo, ContractCallSummary, SnAddressInfo,
    SnBlock, SnReceipt, SnTransaction, TokenBalance, VoyagerLabelInfo,
};
use crate::decode::events::DecodedEvent;
use crate::decode::functions::RawCall;
use crate::decode::outside_execution::OutsideExecutionInfo;
use crate::network::dune::AddressActivityProbe;

/// Actions dispatched from the UI to the network task, or responses back.
#[derive(Debug)]
pub enum Action {
    // --- Requests (UI → network) ---
    /// Fetch the N most recent blocks.
    FetchRecentBlocks { count: usize },
    /// Fetch a block with all its transactions.
    FetchBlockDetail { number: u64 },
    /// Fetch a transaction and its receipt.
    FetchTransaction { hash: Felt },
    /// Fetch address info (nonce, class hash, etc.).
    FetchAddressInfo { address: Felt },
    /// Navigate to address view immediately (before data loads).
    NavigateToAddress { address: Felt },
    /// Resolve a search query (may need RPC to disambiguate tx vs address).
    ResolveSearch { query: String },
    /// Fetch the next/prev tx by nonce for a given sender address.
    FetchTxByNonce {
        sender: Felt,
        current_nonce: u64,
        direction: i64,
    },
    /// Enrich visible address txs that are missing endpoint/timestamp data.
    EnrichAddressTxs { address: Felt, hashes: Vec<Felt> },
    /// Post-load sanity check: fill nonce gaps + enrich all empty endpoints.
    SanityCheckAddress {
        address: Felt,
        current_nonce: u64,
        known_txs: Vec<AddressTxSummary>,
    },
    /// Enrich WS-streamed call stubs (missing sender/function/fee/timestamp).
    EnrichAddressCalls {
        address: Felt,
        hashes_with_blocks: Vec<(Felt, u64)>,
    },
    /// Fetch older blocks (pagination: blocks before `before` block number).
    FetchOlderBlocks { before: u64, count: usize },
    /// Fetch more address transactions from before a given block (pagination).
    FetchMoreAddressTxs {
        address: Felt,
        before_block: u64,
        is_contract: bool,
    },
    /// Fetch class hash info (ABI, declaration, deployed contracts).
    FetchClassInfo { class_hash: Felt },
    /// Persist enriched address txs to cache (sent after enrichment completes).
    PersistAddressTxs {
        address: Felt,
        txs: Vec<AddressTxSummary>,
    },
    /// Persist enriched address calls to cache (sent after enrichment completes).
    PersistAddressCalls {
        address: Felt,
        calls: Vec<ContractCallSummary>,
    },

    // --- Responses (network → UI) ---
    /// Recent blocks loaded.
    BlocksLoaded(Vec<SnBlock>),
    /// A single new block header arrived (from WebSocket or polling).
    NewBlock(SnBlock),
    /// Block detail with transactions loaded.
    BlockDetailLoaded {
        block: SnBlock,
        transactions: Vec<SnTransaction>,
        /// Decoded function name per tx (by index). None if not yet resolved.
        endpoint_names: Vec<Option<String>>,
        /// Execution status per tx: "OK", "REV", or "?"
        tx_statuses: Vec<String>,
        /// Outside execution summary per tx. Some for meta txs, None otherwise.
        meta_tx_info: Vec<Option<crate::app::views::block_detail::MetaTxSummary>>,
    },
    /// Transaction + receipt + decoded events loaded.
    TransactionLoaded {
        transaction: SnTransaction,
        receipt: SnReceipt,
        decoded_events: Vec<DecodedEvent>,
        decoded_calls: Vec<RawCall>,
        /// Detected outside executions: (call_index, parsed_info).
        outside_executions: Vec<(usize, OutsideExecutionInfo)>,
    },
    /// Address info loaded.
    AddressInfoLoaded {
        info: SnAddressInfo,
        decoded_events: Vec<DecodedEvent>,
        tx_summaries: Vec<AddressTxSummary>,
        contract_calls: Vec<ContractCallSummary>,
    },
    /// Class hash history loaded for an address.
    ClassHistoryLoaded {
        address: Felt,
        entries: Vec<ClassHashEntry>,
    },
    /// Enriched endpoint/timestamp data for a subset of address txs.
    AddressTxsEnriched {
        address: Felt,
        updates: Vec<AddressTxSummary>,
    },
    /// Enriched call data (sender/function/fee/timestamp) for WS-streamed calls.
    AddressCallsEnriched {
        address: Felt,
        calls: Vec<ContractCallSummary>,
    },
    /// Older blocks loaded (appended to block list).
    OlderBlocksLoaded(Vec<SnBlock>),
    /// More address transactions loaded (appended to address tx list).
    MoreAddressTxsLoaded {
        address: Felt,
        tx_summaries: Vec<AddressTxSummary>,
        contract_calls: Vec<ContractCallSummary>,
        /// The earliest block number in this batch (for further pagination).
        oldest_block: u64,
        /// Whether there is likely more data beyond this batch.
        has_more: bool,
    },
    /// Dune activity probe result delivered to UI for pagination window sizing.
    AddressProbeLoaded {
        address: Felt,
        probe: AddressActivityProbe,
    },
    /// Tells the UI which data sources will be streaming tx data for this address load.
    AddressSourcesPending { address: Felt, sources: Vec<Source> },
    /// Streaming partial tx results from a specific data source (merges, never replaces).
    AddressTxsStreamed {
        address: Felt,
        source: Source,
        tx_summaries: Vec<AddressTxSummary>,
        /// When true, this source has delivered all its data.
        complete: bool,
    },
    /// Streaming incoming calls (events emitted by the contract) from WS.
    /// Goes to the Calls tab — separate from AddressTxsStreamed which populates the Txs tab.
    AddressCallsStreamed {
        address: Felt,
        calls: Vec<ContractCallSummary>,
    },
    /// Token balances loaded for an address (sent early for accounts).
    AddressBalancesLoaded {
        address: Felt,
        balances: Vec<TokenBalance>,
    },
    /// Voyager label loaded for an address.
    VoyagerLabelLoaded {
        address: Felt,
        label: VoyagerLabelInfo,
    },
    /// Update the status of a named data source.
    SourceUpdate {
        source: Source,
        status: SourceStatus,
    },
    /// Latest block number updated.
    LatestBlockNumber(u64),
    /// Update the loading status message shown in the status bar.
    LoadingStatus(String),
    /// Navigate to class info view immediately (before data loads).
    NavigateToClassInfo { class_hash: Felt },
    /// Class ABI loaded from RPC/cache.
    ClassAbiLoaded {
        class_hash: Felt,
        abi: Option<std::sync::Arc<crate::decode::abi::ParsedAbi>>,
    },
    /// Class declaration info loaded (from PF+RPC or Dune).
    ClassDeclareLoaded {
        class_hash: Felt,
        declare_info: Option<ClassDeclareInfo>,
    },
    /// Contracts deployed with this class hash (from PF).
    ClassContractsLoaded {
        class_hash: Felt,
        contracts: Vec<ClassContractEntry>,
        declaration_block: Option<u64>,
    },
    /// An error occurred in the network task.
    Error(String),
}

/// Identifies a data source for status updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    Rpc,
    Ws,
    Pathfinder,
    Dune,
    Voyager,
}
