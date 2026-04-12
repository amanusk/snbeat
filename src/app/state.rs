/// Which view is currently displayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// Default home: block list + search bar.
    Blocks,
    /// Single block header + transaction list.
    BlockDetail,
    /// Full transaction detail with event tree.
    TxDetail,
    /// Address information (tx history, balances, transfers).
    AddressInfo,
    /// Class hash detail (ABI, declaration info, deployed contracts).
    ClassInfo,
}

/// Whether the user is navigating or typing in the search bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    /// hjkl navigation, Enter to drill in, q to quit.
    Normal,
    /// Typing in the search bar with autocomplete.
    Search,
}

/// Which panel/widget currently has keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    BlockList,
    SearchBar,
    TxList,
    TxDetail,
    EventTree,
    AddressHistory,
    ClassDetail,
}

/// An item in a detail view that can be navigated to (visual mode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxNavItem {
    Block(u64),
    Address(starknet::core::types::Felt),
    ClassHash(starknet::core::types::Felt),
    Transaction(starknet::core::types::Felt),
}

/// A unified navigation target. All view transitions go through
/// `App::navigate_to(target)` to ensure consistent state clearing,
/// view pushing, and fetch action dispatch regardless of entry point
/// (search, visual mode, Enter, forward history, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NavTarget {
    Block(u64),
    Transaction(starknet::core::types::Felt),
    Address(starknet::core::types::Felt),
    ClassHash(starknet::core::types::Felt),
}

impl NavTarget {
    /// Convert a `TxNavItem` (visual mode selection) into a `NavTarget`.
    pub fn from_nav_item(item: &TxNavItem) -> Self {
        match item {
            TxNavItem::Block(n) => NavTarget::Block(*n),
            TxNavItem::Address(a) => NavTarget::Address(*a),
            TxNavItem::ClassHash(c) => NavTarget::ClassHash(*c),
            TxNavItem::Transaction(h) => NavTarget::Transaction(*h),
        }
    }
}

/// Connection status shown in the status bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStatus {
    Disconnected,
    Connecting,
    Connected { network: String },
    Error(String),
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Disconnected
    }
}

/// Health state of an external data source.
///
/// Colour mapping in the status bar:
/// - `Off`            → grey  (not configured)
/// - `Configured`     → grey  (configured, not yet probed)
/// - `Live`           → green (working)
/// - `ConnectError`   → red   (configured but cannot reach — shows error)
/// - `FetchError`     → yellow (reachable but a request failed — shows error)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SourceStatus {
    /// Not configured / not available.
    #[default]
    Off,
    /// Configured but not yet verified (just started).
    Configured,
    /// Configured and confirmed reachable.
    Live,
    /// Configured but cannot connect (e.g. wrong URL, host down).
    ConnectError(String),
    /// Was live, but a specific fetch/request failed.
    FetchError(String),
}

impl SourceStatus {
    /// Short error string for display in the status bar, if any.
    pub fn error_msg(&self) -> Option<&str> {
        match self {
            SourceStatus::ConnectError(msg) | SourceStatus::FetchError(msg) => Some(msg),
            _ => None,
        }
    }

    /// Whether the source is in an error state (connect or fetch).
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            SourceStatus::ConnectError(_) | SourceStatus::FetchError(_)
        )
    }
}

/// Which external data sources are configured and available.
#[derive(Debug, Clone)]
pub struct DataSources {
    pub rpc: SourceStatus,
    pub dune: SourceStatus,
    pub pathfinder: SourceStatus,
    pub voyager: SourceStatus,
    pub ws: SourceStatus,
}

impl Default for DataSources {
    fn default() -> Self {
        Self {
            rpc: SourceStatus::Configured,
            dune: SourceStatus::Off,
            pathfinder: SourceStatus::Off,
            voyager: SourceStatus::Off,
            ws: SourceStatus::Off,
        }
    }
}
