use starknet::core::types::Felt;

use crate::app::state::SourceStatus;
use crate::app::views::address_info::UnfilledGap;
use crate::data::pathfinder::ClassHashEntry;
use crate::data::types::{
    AddressTxSummary, ClassContractEntry, ClassDeclareInfo, ContractCallSummary,
    MetaTxIntenderSummary, SnAddressInfo, SnBlock, SnEvent, SnReceipt, SnTransaction, TokenBalance,
    VoyagerLabelInfo,
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
    FetchRecentBlocks {
        count: usize,
    },
    /// Fetch a block with all its transactions.
    FetchBlockDetail {
        number: u64,
    },
    /// Fetch a transaction and its receipt.
    FetchTransaction {
        hash: Felt,
    },
    /// Fetch address info (nonce, class hash, etc.).
    FetchAddressInfo {
        address: Felt,
    },
    /// Navigate to address view immediately (before data loads).
    NavigateToAddress {
        address: Felt,
    },
    /// Resolve a search query (may need RPC to disambiguate tx vs address).
    ResolveSearch {
        query: String,
    },
    /// Fetch the next/prev tx by nonce for a given sender address.
    FetchTxByNonce {
        sender: Felt,
        current_nonce: u64,
        direction: i64,
    },
    /// Enrich visible address txs that are missing endpoint/timestamp data.
    EnrichAddressTxs {
        address: Felt,
        hashes: Vec<Felt>,
    },
    /// Post-load enrichment: fill small nonce gaps + enrich missing endpoint names.
    /// Fires automatically once initial sources settle. Large gaps are handled by
    /// `FillAddressNonceGaps` instead (on-demand, issue #10).
    EnrichAddressEndpoints {
        address: Felt,
        current_nonce: u64,
        known_txs: Vec<AddressTxSummary>,
    },
    /// On-demand large-gap fill: triggered when the user scrolls toward the gap.
    FillAddressNonceGaps {
        address: Felt,
        known_txs: Vec<AddressTxSummary>,
        gap: UnfilledGap,
    },
    /// Enrich WS-streamed call stubs (missing sender/function/fee/timestamp).
    EnrichAddressCalls {
        address: Felt,
        hashes_with_blocks: Vec<(Felt, u64)>,
    },
    /// Fetch older blocks (pagination: blocks before `before` block number).
    FetchOlderBlocks {
        before: u64,
        count: usize,
    },
    /// Fetch more address transactions from before a given block (pagination).
    FetchMoreAddressTxs {
        address: Felt,
        before_block: u64,
        is_contract: bool,
    },
    /// Fetch meta-transactions (SNIP-9 outside executions) where `address` is
    /// the intender (issue #11). Paginates via pf-query's event continuation
    /// token — opaque u64 from the previous response, or `None` for first page.
    ///
    /// `from_block` is the absolute scan floor (typically deploy block); the
    /// backend stops paginating once `min_searched <= from_block` so we don't
    /// time out walking chunks older than the account. `window_size` is the
    /// per-page ExtendDown block range, adapted across calls (double on empty,
    /// halve on full) — see `event_window::suggest_next_window`.
    FetchAddressMetaTxs {
        address: Felt,
        from_block: u64,
        continuation_token: Option<u64>,
        window_size: u64,
        limit: u32,
    },
    /// Classify a single tx as a potential meta-tx where `address` is the
    /// intender. Dispatched from the WS event handler on every
    /// `TRANSACTION_EXECUTED` event for the currently-viewed account so the
    /// MetaTxs tab updates in real time. Requires pf-query (tx_type +
    /// calldata decoding); no-op when unavailable.
    ClassifyPotentialMetaTx {
        address: Felt,
        tx_hash: Felt,
    },
    /// Fetch class hash info (ABI, declaration, deployed contracts).
    FetchClassInfo {
        class_hash: Felt,
    },
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
    FetchTokenPricesToday {
        tokens: Vec<Felt>,
    },
    FetchTokenPricesHistoric {
        requests: Vec<(Felt, u64)>,
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
        block_timestamp: Option<u64>,
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
    /// Meta-transaction summaries loaded for an address (issue #11).
    AddressMetaTxsLoaded {
        address: Felt,
        summaries: Vec<MetaTxIntenderSummary>,
        /// Opaque continuation token from pf-query, if more pages remain.
        /// Pass back via `FetchAddressMetaTxs::continuation_token` to resume.
        next_token: Option<u64>,
        /// Adaptive next ExtendDown window size (blocks) suggested by the
        /// backend based on this page's hit density. `None` when no further
        /// paging is expected (we hit the floor) or on non-ExtendDown calls.
        /// The UI persists this as `meta_tx_last_window` and feeds it into
        /// the next `FetchAddressMetaTxs` dispatch.
        next_window_size: Option<u64>,
    },
    /// Cached meta-tx rows delivered synchronously at tab-entry time. Merges
    /// into the visible list but does NOT touch the loading flag / cursor — a
    /// live pf-query fetch may still be in-flight behind this.
    AddressMetaTxsCacheLoaded {
        address: Felt,
        summaries: Vec<MetaTxIntenderSummary>,
    },
    /// Streaming single-tx meta-tx classification result from the WS path.
    /// Merges into the visible list without touching pagination state (live
    /// pf-query fetches may still be in-flight in parallel).
    AddressMetaTxsStreamed {
        address: Felt,
        summaries: Vec<MetaTxIntenderSummary>,
    },
    /// Dune activity probe result delivered to UI for pagination window sizing.
    AddressProbeLoaded {
        address: Felt,
        probe: AddressActivityProbe,
    },
    /// Tells the UI which data sources will be streaming tx data for this address load.
    AddressSourcesPending {
        address: Felt,
        sources: Vec<Source>,
    },
    /// Passive update of the shared event-window hint (min/max scanned block,
    /// deferred gap) driven by `ensure_address_events_window`. Consumed by
    /// the Calls / Events / MetaTxs tab titles to surface gap state. Fires
    /// from every event-window fetch path so all three tabs stay aligned.
    AddressEventWindowUpdated {
        address: Felt,
        min_searched: u64,
        max_searched: u64,
        deferred_gap: Option<(u64, u64)>,
    },
    /// Streaming partial tx results from a specific data source (merges, never replaces).
    AddressTxsStreamed {
        address: Felt,
        source: Source,
        tx_summaries: Vec<AddressTxSummary>,
        /// When true, this source has delivered all its data.
        complete: bool,
    },
    /// Single broadcast of a WS-received event. Emitted once per event; the
    /// reducer fans it out to the Calls tab (builds a `ContractCallSummary`
    /// stub + dispatches `EnrichAddressCalls`) and, for `TRANSACTION_EXECUTED`
    /// events, dispatches `ClassifyPotentialMetaTx` so live meta-tx detection
    /// still reaches the MetaTxs tab. The event itself has already been
    /// persisted into the address event cache in the WS handler.
    AddressWsEvent {
        address: Felt,
        event: SnEvent,
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
    NavigateToClassInfo {
        class_hash: Felt,
    },
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
    PricesUpdated,
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
