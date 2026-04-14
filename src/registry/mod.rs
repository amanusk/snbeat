pub mod known_addresses;
pub mod user_labels;

use std::path::Path;
use std::sync::RwLock;

use starknet::core::types::Felt;
use tracing::info;

use crate::error::Result;
use known_addresses::{KnownAddress, load_known_addresses};
use user_labels::{UserLabel, load_user_labels};

/// Unified address registry over user labels + known addresses.
/// User labels take priority over known addresses.
/// Pre-builds a search index for fast prefix/substring matching.
pub struct AddressRegistry {
    /// User labels (highest priority)
    user: Vec<UserLabel>,
    /// Known addresses (curated)
    known: Vec<KnownAddress>,
    /// Search index: sorted entries for fast lookup.
    /// Behind RwLock so Voyager labels can be added at runtime.
    search_index: RwLock<Vec<SearchEntry>>,
}

#[derive(Debug, Clone)]
struct SearchEntry {
    name_lower: String,
    display_name: String,
    address: Felt,
    hex_lower: String,
    is_user: bool,
}

/// A search result returned to the UI.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub display: String,
    pub address: Felt,
    pub is_user: bool,
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
    /// Load from user labels file and bundled known addresses.
    /// Returns (registry, optional_warning) — warning is set when labels file is corrupt.
    pub fn load(user_labels_path: &Path) -> Result<(Self, Option<String>)> {
        let (user, warning) = load_user_labels(user_labels_path)?;
        let known = load_known_addresses()?;

        let mut search_index = Vec::with_capacity(user.len() + known.len());

        // User labels first (higher priority in search results)
        for label in &user {
            let hex = format!("{:#x}", label.address);
            search_index.push(SearchEntry {
                name_lower: label.name.to_lowercase(),
                display_name: label.name.clone(),
                address: label.address,
                hex_lower: hex.to_lowercase(),
                is_user: true,
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
                address: addr.address,
                hex_lower: hex.to_lowercase(),
                is_user: false,
            });
        }

        // Sort by name for consistent ordering
        search_index.sort_by(|a, b| a.name_lower.cmp(&b.name_lower));

        info!(
            user = user.len(),
            known = known.len(),
            index = search_index.len(),
            "Address registry loaded"
        );

        Ok((
            Self {
                user,
                known,
                search_index: RwLock::new(search_index),
            },
            warning,
        ))
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
                results.push(SearchResult {
                    display: format_search_result(entry),
                    address: entry.address,
                    is_user: entry.is_user,
                });
            }
            if results.len() >= limit {
                return results;
            }
        }

        // Then substring matches
        for entry in index.iter() {
            if !entry.name_lower.starts_with(&query_lower)
                && entry.name_lower.contains(&query_lower)
            {
                results.push(SearchResult {
                    display: format_search_result(entry),
                    address: entry.address,
                    is_user: entry.is_user,
                });
            }
            if results.len() >= limit {
                return results;
            }
        }

        // Hex prefix matches
        if query_lower.starts_with("0x") {
            for entry in index.iter() {
                if entry.hex_lower.starts_with(&query_lower)
                    && !results.iter().any(|r| r.address == entry.address)
                {
                    results.push(SearchResult {
                        display: format_search_result(entry),
                        address: entry.address,
                        is_user: entry.is_user,
                    });
                }
                if results.len() >= limit {
                    return results;
                }
            }
        }

        results
    }

    /// Look up an address by exact label name (case-insensitive).
    pub fn resolve_by_name(&self, name: &str) -> Option<Felt> {
        let lower = name.to_lowercase();
        let index = self.search_index.read().unwrap();
        index
            .iter()
            .find(|e| e.name_lower == lower)
            .map(|e| e.address)
    }

    /// Add a Voyager-sourced label to the search index.
    /// Skips if the address already has an entry (user/known labels take priority).
    pub fn add_voyager_label(&self, address: Felt, name: &str) {
        let mut index = self.search_index.write().unwrap();
        // Don't duplicate if address already indexed
        if index.iter().any(|e| e.address == address) {
            return;
        }
        let hex = format!("{:#x}", address);
        let entry = SearchEntry {
            name_lower: name.to_lowercase(),
            display_name: name.to_string(),
            address,
            hex_lower: hex.to_lowercase(),
            is_user: false,
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
    format!("{} ({})", entry.display_name, short)
}
