pub mod abi;
pub mod calldata;
pub mod class_cache;
pub mod events;
pub mod functions;
pub mod outside_execution;

use std::sync::Arc;

use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::data::DataSource;
use abi::{ABI_SCHEMA_VERSION, ParsedAbi};
use class_cache::ClassCache;

/// ABI registry: resolves contract addresses to parsed ABIs.
/// Handles the chain: address → class_hash → fetch class → parse ABI → cache.
pub struct AbiRegistry {
    data_source: Arc<dyn DataSource>,
    cache: ClassCache,
}

impl AbiRegistry {
    pub fn new(data_source: Arc<dyn DataSource>, cache: ClassCache) -> Self {
        Self { data_source, cache }
    }

    /// Get the parsed ABI for a contract address.
    /// Returns None if the ABI cannot be fetched or parsed (does not error).
    pub async fn get_abi_for_address(&self, address: &Felt) -> Option<Arc<ParsedAbi>> {
        // Step 1: address → class_hash
        let class_hash = match self.data_source.get_class_hash(*address).await {
            Ok(h) => h,
            Err(e) => {
                debug!(address = %format!("{:#x}", address), error = %e, "Failed to get class hash");
                return None;
            }
        };

        self.get_abi_for_class(&class_hash).await
    }

    /// Get the parsed ABI for a class hash.
    pub async fn get_abi_for_class(&self, class_hash: &Felt) -> Option<Arc<ParsedAbi>> {
        // Step 2: check cache (may return a stale entry from before struct support was added)
        let cached = self.cache.get(class_hash);

        // Fresh entry: use it immediately without any RPC call
        if let Some(ref abi) = cached {
            if abi.schema_version >= ABI_SCHEMA_VERSION {
                return cached;
            }
            debug!(class_hash = %format!("{:#x}", class_hash), "Stale ABI cache entry, re-fetching");
        }

        // Step 3: fetch class from RPC (either missing or stale)
        let class = match self.data_source.get_class(*class_hash).await {
            Ok(c) => c,
            Err(e) => {
                warn!(class_hash = %format!("{:#x}", class_hash), error = %e, "Failed to fetch class");
                // Fall back to stale cache entry rather than returning None
                return cached;
            }
        };

        // Step 4: parse ABI
        let parsed = abi::parse_contract_class(&class);
        if parsed.is_empty() {
            debug!(class_hash = %format!("{:#x}", class_hash), "ABI is empty");
        }

        // Step 5: index selectors for fast lookup
        self.cache.index_abi_selectors(&parsed);

        // Step 6: cache and return
        let arc = Arc::new(parsed);
        self.cache.put(*class_hash, Arc::clone(&arc));
        Some(arc)
    }

    /// Fast selector→name lookup from the persistent DB. No RPC needed.
    pub fn get_selector_name(&self, selector: &Felt) -> Option<String> {
        self.cache.get_selector_name(selector)
    }
}
