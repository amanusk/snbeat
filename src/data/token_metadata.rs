//! On-chain ERC-20 metadata lookup (decimals + symbol).
//!
//! Used to render unknown tokens correctly in the Privacy tab and the
//! address Balances tab without committing addresses to a static
//! `labels.toml` first. We fetch via two `starknet_call`s, normalize the
//! Cairo 0 (single-felt short string) and Cairo 1 (`ByteArray` =
//! `[num_full_words, ...words, pending_word, pending_len]`) symbol
//! encodings, and surface the result to the UI through the App's
//! fetched-metadata cache.
//!
//! Fetched entries are persisted (see `src/data/cache.rs`) so the cost is
//! one-time per token across restarts.

use std::sync::LazyLock;

use starknet::core::utils::get_selector_from_name;
use starknet_types_core::felt::Felt;

use crate::data::DataSource;
use crate::utils::felt_to_u64;

/// Display metadata for one ERC-20 contract.
#[derive(Debug, Clone)]
pub struct TokenMeta {
    pub symbol: String,
    pub decimals: u8,
}

static DECIMALS_SELECTOR: LazyLock<Felt> =
    LazyLock::new(|| get_selector_from_name("decimals").expect("static selector"));
static SYMBOL_SELECTOR: LazyLock<Felt> =
    LazyLock::new(|| get_selector_from_name("symbol").expect("static selector"));

/// Fetch `decimals()` and `symbol()` for an ERC-20-shaped contract.
/// Returns `None` if either call fails or the response can't be parsed —
/// we don't try to half-populate, since rendering with one field but not
/// the other would be inconsistent across rows.
pub async fn fetch_token_metadata(token: Felt, ds: &dyn DataSource) -> Option<TokenMeta> {
    let dec_resp = ds
        .call_contract(token, *DECIMALS_SELECTOR, Vec::new())
        .await
        .ok()?;
    let sym_resp = ds
        .call_contract(token, *SYMBOL_SELECTOR, Vec::new())
        .await
        .ok()?;
    let decimals = parse_decimals(&dec_resp)?;
    let symbol = parse_symbol(&sym_resp)?;
    if symbol.is_empty() {
        return None;
    }
    Some(TokenMeta { symbol, decimals })
}

fn parse_decimals(felts: &[Felt]) -> Option<u8> {
    let first = felts.first()?;
    let v = felt_to_u64(first);
    if v > u8::MAX as u64 {
        None
    } else {
        Some(v as u8)
    }
}

/// Parse the response of `symbol()`. Handles both the Cairo 0 form
/// (single felt252, ASCII bytes packed big-endian, leading zeros) and
/// the Cairo 1 `ByteArray` form
/// (`[num_full_words, ...full_words(31 bytes each), pending_word,
/// pending_len]`).
fn parse_symbol(felts: &[Felt]) -> Option<String> {
    match felts.len() {
        0 => None,
        1 => Some(felt_to_short_string(felts[0])),
        2 => Some(felt_to_short_string(felts[0])), // unusual; best-effort
        _ => parse_byte_array(felts).or_else(|| Some(felt_to_short_string(felts[0]))),
    }
}

fn felt_to_short_string(felt: Felt) -> String {
    let bytes = felt.to_bytes_be();
    let trimmed: Vec<u8> = bytes.iter().copied().skip_while(|&b| b == 0).collect();
    String::from_utf8(trimmed).unwrap_or_default()
}

fn parse_byte_array(felts: &[Felt]) -> Option<String> {
    if felts.len() < 3 {
        return None;
    }
    let num_full_words = felt_to_u64(&felts[0]) as usize;
    let needed = 1 + num_full_words + 2;
    if felts.len() < needed {
        return None;
    }
    let pending_word = felts[1 + num_full_words];
    let pending_len = felt_to_u64(&felts[2 + num_full_words]) as usize;
    if pending_len > 31 {
        return None;
    }
    let mut bytes: Vec<u8> = Vec::with_capacity(num_full_words * 31 + pending_len);
    for word in &felts[1..1 + num_full_words] {
        let be = word.to_bytes_be();
        bytes.extend_from_slice(&be[1..32]); // each full word holds 31 bytes
    }
    if pending_len > 0 {
        let be = pending_word.to_bytes_be();
        bytes.extend_from_slice(&be[32 - pending_len..32]);
    }
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cairo0_short_string_symbol() {
        // "STRK" packed big-endian as a single felt.
        let felt = Felt::from_hex_unchecked("0x5354524b");
        assert_eq!(felt_to_short_string(felt), "STRK");
    }

    #[test]
    fn parses_cairo1_byte_array_short_symbol() {
        // ByteArray representing "USDC": 0 full words, pending = "USDC", len = 4.
        let usdc_pending = Felt::from_hex_unchecked("0x55534443");
        let felts = vec![Felt::from(0u64), usdc_pending, Felt::from(4u64)];
        assert_eq!(parse_byte_array(&felts).as_deref(), Some("USDC"));
    }

    #[test]
    fn parses_cairo1_byte_array_long_symbol() {
        // 1 full word = 31 chars "ABCDEFGHIJKLMNOPQRSTUVWXYZ01234"
        // (26 letters + 5 digits = 31 chars), pending = "5", len = 1.
        let mut full_word_bytes = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ01234".to_vec();
        let mut padded = vec![0u8]; // 32 bytes total, 1 leading zero
        padded.append(&mut full_word_bytes);
        let full_word = Felt::from_bytes_be_slice(&padded);
        let pending = Felt::from_hex_unchecked("0x35"); // "5"
        let felts = vec![Felt::from(1u64), full_word, pending, Felt::from(1u64)];
        assert_eq!(
            parse_byte_array(&felts).as_deref(),
            Some("ABCDEFGHIJKLMNOPQRSTUVWXYZ012345")
        );
    }

    #[test]
    fn parses_decimals_in_range() {
        assert_eq!(parse_decimals(&[Felt::from(18u64)]), Some(18));
        assert_eq!(parse_decimals(&[Felt::from(6u64)]), Some(6));
    }

    #[test]
    fn rejects_decimals_out_of_range() {
        assert_eq!(parse_decimals(&[Felt::from(256u64)]), None);
    }
}
