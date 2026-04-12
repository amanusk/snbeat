pub mod parser;

use std::sync::Arc;

use crate::registry::{AddressRegistry, SearchResult};

/// Search engine: combines the address registry with autocomplete.
pub struct SearchEngine {
    registry: Arc<AddressRegistry>,
}

impl SearchEngine {
    pub fn new(registry: Arc<AddressRegistry>) -> Self {
        Self { registry }
    }

    /// Generate autocomplete suggestions for the current input.
    /// Called on every keystroke — must be < 1ms.
    pub fn suggest(&self, input: &str) -> Vec<SearchResult> {
        self.registry.search(input, 200)
    }

    /// Classify the final input into a SearchQuery.
    pub fn classify(&self, input: &str) -> Result<parser::SearchQuery, String> {
        parser::classify(input, &self.registry)
    }

    /// Get the underlying registry for label resolution.
    pub fn registry(&self) -> &AddressRegistry {
        &self.registry
    }
}
