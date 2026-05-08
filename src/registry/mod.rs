pub mod known_addresses;
pub mod user_labels;
pub mod viewing_keys;

use std::path::Path;
use std::sync::RwLock;

use starknet::core::types::Felt;
use tracing::info;

use crate::decode::privacy_crypto::types::SecretFelt;
use crate::error::Result;
use known_addresses::{KnownAddress, load_known_addresses};
use user_labels::{UserLabel, UserTxLabel, load_user_labels};
use viewing_keys::{ViewingKey, load_viewing_keys};

/// What kind of on-chain entity a search entry/result points at. Drives
/// downstream navigation (address view vs transaction detail view).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Address,
    Transaction,
}

/// Unified address + tx-hash registry over user labels + known addresses.
/// User labels take priority over known addresses.
/// Pre-builds a search index for fast prefix/substring matching.
pub struct AddressRegistry {
    /// User address labels (highest priority)
    user: Vec<UserLabel>,
    /// User transaction labels
    tx_labels: Vec<UserTxLabel>,
    /// Known addresses (curated)
    known: Vec<KnownAddress>,
    /// Privacy-pool viewing keys keyed by user address. Deliberately NOT
    /// in the search index — secrets shouldn't surface in autocomplete.
    viewing_keys: Vec<ViewingKey>,
    /// Search index: sorted entries for fast lookup.
    /// Behind RwLock so Voyager labels can be added at runtime.
    search_index: RwLock<Vec<SearchEntry>>,
}

#[derive(Debug, Clone)]
struct SearchEntry {
    name_lower: String,
    display_name: String,
    /// Address or tx hash, depending on `kind`.
    felt: Felt,
    hex_lower: String,
    is_user: bool,
    /// Lowercase tags for search matching (user labels only).
    tags_lower: Vec<String>,
    kind: EntryKind,
}

/// A search result returned to the UI.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub display: String,
    /// Address or tx hash, depending on `kind`.
    pub felt: Felt,
    pub is_user: bool,
    pub kind: EntryKind,
}

/// Metadata for a resolved address.
#[derive(Debug, Clone)]
pub struct AddressMeta {
    pub name: String,
    pub addr_type: String,
    pub decimals: Option<u8>,
    pub is_user: bool,
}

impl AddressRegistry {
    /// Load from user labels, viewing keys, and bundled known addresses.
    /// Returns `(registry, warnings)`. Warnings collect non-fatal issues
    /// from any of the input files (corrupt TOML etc.) so all of them
    /// can surface in the UI rather than the first one masking the rest.
    pub fn load(user_labels_path: &Path, viewing_keys_path: &Path) -> Result<(Self, Vec<String>)> {
        let (user, tx_labels, labels_warning) = load_user_labels(user_labels_path)?;
        let (viewing_keys, vk_warning) = load_viewing_keys(viewing_keys_path)?;
        let known = load_known_addresses()?;
        let mut warnings = Vec::new();
        if let Some(w) = labels_warning {
            warnings.push(w);
        }
        if let Some(w) = vk_warning {
            warnings.push(w);
        }

        let mut search_index = Vec::with_capacity(user.len() + tx_labels.len() + known.len());

        // User labels first (higher priority in search results)
        for label in &user {
            let hex = format!("{:#x}", label.address);
            search_index.push(SearchEntry {
                name_lower: label.name.to_lowercase(),
                display_name: label.name.clone(),
                felt: label.address,
                hex_lower: hex.to_lowercase(),
                is_user: true,
                tags_lower: label.tags.iter().map(|t| t.to_lowercase()).collect(),
                kind: EntryKind::Address,
            });
        }

        // User tx labels: same priority as address labels (the user added them)
        for label in &tx_labels {
            let hex = format!("{:#x}", label.hash);
            search_index.push(SearchEntry {
                name_lower: label.name.to_lowercase(),
                display_name: label.name.clone(),
                felt: label.hash,
                hex_lower: hex.to_lowercase(),
                is_user: true,
                tags_lower: Vec::new(),
                kind: EntryKind::Transaction,
            });
        }

        // Then known addresses
        for addr in &known {
            // Skip if user already has a label for this address
            if user.iter().any(|u| u.address == addr.address) {
                continue;
            }
            let hex = format!("{:#x}", addr.address);
            search_index.push(SearchEntry {
                name_lower: addr.name.to_lowercase(),
                display_name: addr.name.clone(),
                felt: addr.address,
                hex_lower: hex.to_lowercase(),
                is_user: false,
                tags_lower: Vec::new(),
                kind: EntryKind::Address,
            });
        }

        // Sort by name for consistent ordering
        search_index.sort_by(|a, b| a.name_lower.cmp(&b.name_lower));

        info!(
            user = user.len(),
            tx_labels = tx_labels.len(),
            viewing_keys = viewing_keys.len(),
            known = known.len(),
            index = search_index.len(),
            "Address registry loaded"
        );

        Ok((
            Self {
                user,
                tx_labels,
                known,
                viewing_keys,
                search_index: RwLock::new(search_index),
            },
            warnings,
        ))
    }

    /// Look up a user-supplied private viewing key for an address.
    /// Returns None if the user has no viewing key registered for it.
    pub fn viewing_key(&self, user: &Felt) -> Option<&SecretFelt> {
        self.viewing_keys
            .iter()
            .find(|vk| vk.user == *user)
            .map(|vk| &vk.private_key)
    }

    /// Iterate over all `(user_address, viewing_key)` pairs the user has
    /// configured. Order is the file's load order (HashMap iteration).
    pub fn iter_viewing_keys(&self) -> impl Iterator<Item = (Felt, &SecretFelt)> {
        self.viewing_keys
            .iter()
            .map(|vk| (vk.user, &vk.private_key))
    }

    /// Resolve an address to its display name. User labels take priority.
    pub fn resolve(&self, address: &Felt) -> Option<&str> {
        // Check user labels first
        if let Some(label) = self.user.iter().find(|u| u.address == *address) {
            return Some(&label.name);
        }
        // Check known addresses
        if let Some(addr) = self.known.iter().find(|k| k.address == *address) {
            return Some(&addr.name);
        }
        None
    }

    /// Resolve a transaction hash to its user-supplied display name.
    pub fn resolve_tx(&self, hash: &Felt) -> Option<&str> {
        self.tx_labels
            .iter()
            .find(|t| t.hash == *hash)
            .map(|t| t.name.as_str())
    }

    /// Get metadata for an address.
    pub fn get_metadata(&self, address: &Felt) -> Option<AddressMeta> {
        if let Some(label) = self.user.iter().find(|u| u.address == *address) {
            return Some(AddressMeta {
                name: label.name.clone(),
                addr_type: String::new(),
                decimals: None,
                is_user: true,
            });
        }
        if let Some(addr) = self.known.iter().find(|k| k.address == *address) {
            return Some(AddressMeta {
                name: addr.name.clone(),
                addr_type: addr.addr_type.clone(),
                decimals: addr.decimals,
                is_user: false,
            });
        }
        None
    }

    /// Get token decimals for an address (from known addresses).
    pub fn get_decimals(&self, address: &Felt) -> Option<u8> {
        self.known
            .iter()
            .find(|k| k.address == *address)
            .and_then(|k| k.decimals)
    }

    /// Search the registry by prefix or substring. Returns up to `limit` results.
    /// Must complete in <1ms for 10k entries.
    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        if query.is_empty() {
            return Vec::new();
        }

        let index = self.search_index.read().unwrap();
        let query_lower = query.to_lowercase();
        let mut results = Vec::new();

        // Prefix matches first (highest relevance)
        for entry in index.iter() {
            if entry.name_lower.starts_with(&query_lower) {
                results.push(make_result(entry));
            }
            if results.len() >= limit {
                return results;
            }
        }

        // Then substring matches (name or tags)
        for entry in index.iter() {
            if results
                .iter()
                .any(|r| r.felt == entry.felt && r.kind == entry.kind)
            {
                continue;
            }
            let name_match = entry.name_lower.contains(&query_lower);
            let tag_match = entry
                .tags_lower
                .iter()
                .any(|t| t.starts_with(&query_lower) || t.contains(&query_lower));
            if name_match || tag_match {
                results.push(make_result(entry));
            }
            if results.len() >= limit {
                return results;
            }
        }

        // Hex prefix matches
        if query_lower.starts_with("0x") {
            for entry in index.iter() {
                if entry.hex_lower.starts_with(&query_lower)
                    && !results
                        .iter()
                        .any(|r| r.felt == entry.felt && r.kind == entry.kind)
                {
                    results.push(make_result(entry));
                }
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Look up an entry by exact label name (case-insensitive). Returns the
    /// matched felt (address or tx hash). On a name collision between an
    /// address label and a tx label, the address wins (insertion order is
    /// preserved by the stable sort that builds the index).
    pub fn resolve_by_name(&self, name: &str) -> Option<Felt> {
        let lower = name.to_lowercase();
        let index = self.search_index.read().unwrap();
        index.iter().find(|e| e.name_lower == lower).map(|e| e.felt)
    }

    /// Add a Voyager-sourced label to the search index.
    /// Skips if the address already has an entry (user/known labels take priority).
    pub fn add_voyager_label(&self, address: Felt, name: &str) {
        let mut index = self.search_index.write().unwrap();
        // Don't duplicate if address already indexed (only check Address-kind
        // entries — a tx label sharing the same felt by accident is fine).
        if index
            .iter()
            .any(|e| e.kind == EntryKind::Address && e.felt == address)
        {
            return;
        }
        let hex = format!("{:#x}", address);
        let entry = SearchEntry {
            name_lower: name.to_lowercase(),
            display_name: name.to_string(),
            felt: address,
            hex_lower: hex.to_lowercase(),
            is_user: false,
            tags_lower: Vec::new(),
            kind: EntryKind::Address,
        };
        // Insert sorted by name
        let pos = index
            .binary_search_by(|e| e.name_lower.cmp(&entry.name_lower))
            .unwrap_or_else(|p| p);
        index.insert(pos, entry);
    }

    /// Check if an address is known (user or known).
    pub fn is_known(&self, address: &Felt) -> bool {
        self.resolve(address).is_some()
    }

    /// Format an address for display: label if known, truncated hex otherwise.
    pub fn format_address(&self, address: &Felt) -> String {
        if let Some(name) = self.resolve(address) {
            format!("[{}]", name)
        } else {
            let hex = format!("{:#x}", address);
            if hex.len() > 14 {
                format!("{}..{}", &hex[..6], &hex[hex.len() - 4..])
            } else {
                hex
            }
        }
    }

    /// Format an address showing both user and global labels when they differ.
    /// Returns e.g. "[My ETH] (ETH / ERC20)" or "[ETH]" or "0x49d..dc7"
    pub fn format_address_full(&self, address: &Felt) -> String {
        let user_name = self
            .user
            .iter()
            .find(|u| u.address == *address)
            .map(|u| &u.name);
        let known = self.known.iter().find(|k| k.address == *address);

        match (user_name, known) {
            (Some(uname), Some(k)) if uname != &k.name => {
                if k.addr_type.is_empty() {
                    format!("[{}] ({})", uname, k.name)
                } else {
                    format!("[{}] ({} / {})", uname, k.name, k.addr_type)
                }
            }
            (Some(uname), Some(k)) if !k.addr_type.is_empty() => {
                format!("[{}] ({})", uname, k.addr_type)
            }
            (Some(uname), _) => format!("[{}]", uname),
            (None, Some(k)) => {
                if k.addr_type.is_empty() {
                    format!("[{}]", k.name)
                } else {
                    format!("[{}] ({})", k.name, k.addr_type)
                }
            }
            (None, None) => self.format_address(address),
        }
    }
}

fn format_search_result(entry: &SearchEntry) -> String {
    let hex = &entry.hex_lower;
    let short = if hex.len() > 14 {
        format!("{}..{}", &hex[..6], &hex[hex.len() - 4..])
    } else {
        hex.clone()
    };
    let kind_tag = match entry.kind {
        EntryKind::Address => "",
        EntryKind::Transaction => " [tx]",
    };
    // The kind tag goes after the closing paren so `extract_name_from_display`
    // (which splits on " (") still recovers the bare label name for Tab-complete.
    format!("{} ({}){}", entry.display_name, short, kind_tag)
}

fn make_result(entry: &SearchEntry) -> SearchResult {
    SearchResult {
        display: format_search_result(entry),
        felt: entry.felt,
        is_user: entry.is_user,
        kind: entry.kind,
    }
}
