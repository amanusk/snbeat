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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ConnectionStatus {
    #[default]
    Disconnected,
    Connecting,
    Connected {
        network: String,
    },
    Error(String),
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

/// Per-query status surface. Each in-flight background query registers a
/// short, human-readable label keyed by (tab, address-prefix, ...). Labels
/// are rendered in the status bar so the user can see what's fetching in
/// each tab — a richer signal than the single-slot `loading_detail` string.
///
/// Keys are opaque strings (e.g. `"meta:41420f"`, `"calls:41420f"`), chosen
/// by the dispatcher; the registry just stores the most recent label per key
/// and drops the entry when the query finishes.
#[derive(Debug, Default, Clone)]
pub struct ActiveQueries {
    entries: Vec<(String, String)>,
}

impl ActiveQueries {
    pub fn set(&mut self, key: &str, label: String) {
        if let Some(slot) = self.entries.iter_mut().find(|(k, _)| k == key) {
            slot.1 = label;
        } else {
            self.entries.push((key.to_string(), label));
        }
    }

    pub fn clear(&mut self, key: &str) {
        self.entries.retain(|(k, _)| k != key);
    }

    /// Drop every entry whose key starts with `prefix`. Useful on address
    /// navigation: `clear_prefix("addr:41420f")` removes every per-tab
    /// registration tied to that address without listing each tab.
    pub fn clear_prefix(&mut self, prefix: &str) {
        self.entries.retain(|(k, _)| !k.starts_with(prefix));
    }

    pub fn labels(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(_, l)| l.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod active_queries_tests {
    use super::ActiveQueries;

    #[test]
    fn set_inserts_and_updates_same_key_in_place() {
        let mut q = ActiveQueries::default();
        q.set("meta:41420f", "MetaTxs scan".into());
        q.set("calls:41420f", "Calls fetch".into());
        q.set("meta:41420f", "MetaTxs scan 6M→5M".into());

        let labels: Vec<_> = q.labels().collect();
        assert_eq!(labels, vec!["MetaTxs scan 6M→5M", "Calls fetch"]);
    }

    #[test]
    fn clear_removes_only_matching_key() {
        let mut q = ActiveQueries::default();
        q.set("meta:41420f", "A".into());
        q.set("calls:41420f", "B".into());
        q.clear("meta:41420f");

        let labels: Vec<_> = q.labels().collect();
        assert_eq!(labels, vec!["B"]);
    }

    #[test]
    fn clear_prefix_drops_every_entry_for_an_address() {
        let mut q = ActiveQueries::default();
        q.set("meta:41420f", "A".into());
        q.set("calls:41420f", "B".into());
        q.set("meta:deadbe", "C".into());
        q.clear_prefix("meta:");

        let labels: Vec<_> = q.labels().collect();
        assert_eq!(labels, vec!["B"]);
    }

    #[test]
    fn is_empty_tracks_lifecycle() {
        let mut q = ActiveQueries::default();
        assert!(q.is_empty());
        q.set("k", "l".into());
        assert!(!q.is_empty());
        q.clear("k");
        assert!(q.is_empty());
    }
}
