use starknet::core::types::Felt;

use crate::registry::AddressRegistry;

/// Classified search query.
#[derive(Debug, Clone)]
pub enum SearchQuery {
    /// Pure decimal number → block number.
    BlockNumber(u64),
    /// Resolved from a known label → address.
    Label(String, Felt),
    /// Hex that could be a tx hash or address — needs RPC to disambiguate.
    Ambiguous(Felt),
    /// Empty input.
    Empty,
}

/// Classify a raw search input string into a SearchQuery.
pub fn classify(input: &str, registry: &AddressRegistry) -> Result<SearchQuery, String> {
    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Ok(SearchQuery::Empty);
    }

    // Pure decimal → block number
    if let Ok(num) = trimmed.parse::<u64>() {
        return Ok(SearchQuery::BlockNumber(num));
    }

    // Check registry by exact name first (case-insensitive)
    if let Some(address) = registry.resolve_by_name(trimmed) {
        return Ok(SearchQuery::Label(trimmed.to_string(), address));
    }

    // Hex input
    let hex = if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        trimmed.to_string()
    } else if trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        format!("0x{trimmed}")
    } else {
        // Not a number, not a known label, not hex
        // Try as a partial name match — if exactly one result, use it
        let results = registry.search(trimmed, 2);
        if results.len() == 1 {
            return Ok(SearchQuery::Label(trimmed.to_string(), results[0].felt));
        }
        return Err(format!("Cannot parse: {trimmed}"));
    };

    let felt = Felt::from_hex(&hex).map_err(|e| format!("Invalid hex: {e}"))?;

    // Check if this hex is a known address
    if registry.is_known(&felt) {
        let name = registry.resolve(&felt).unwrap_or("").to_string();
        return Ok(SearchQuery::Label(name, felt));
    }

    // Ambiguous — could be tx hash or address
    Ok(SearchQuery::Ambiguous(felt))
}
