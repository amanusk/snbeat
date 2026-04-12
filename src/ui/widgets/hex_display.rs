use starknet::core::types::Felt;

use crate::utils::felt_to_u128;

/// Format a u64 with thousands separators: 1_234_567 → "1,234,567".
pub fn format_commas(n: u64) -> String {
    commas_str(&n.to_string())
}

fn commas_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// Format a value in fri (10^-18 STRK) as human-readable GFri or Fri.
/// GFri = 10^9 Fri (like Gwei for ETH). Numbers include thousands separators.
pub fn format_fri(fri: u128) -> String {
    if fri == 0 {
        return "0 Fri".to_string();
    }
    let gfri = fri / 1_000_000_000;
    if gfri > 0 {
        let frac = (fri % 1_000_000_000) / 1_000_000; // 3 decimals
        if frac > 0 {
            format!("{}.{frac:03} GFri", commas_str(&gfri.to_string()))
        } else {
            format!("{} GFri", commas_str(&gfri.to_string()))
        }
    } else {
        format!("{} Fri", commas_str(&fri.to_string()))
    }
}

/// Format an actual fee (Felt, in fri) as STRK amount.
/// 1 STRK = 10^18 fri. Shows 5 decimal places.
pub fn format_strk(felt: &Felt) -> String {
    format_strk_u128(felt_to_u128(felt))
}

/// Truncate a hex felt for display: "0x049d36..004dc7"
pub fn truncate_felt(felt: &Felt, prefix_len: usize, suffix_len: usize) -> String {
    let hex = format!("{felt:#x}");
    let needed = 2 + prefix_len + 2 + suffix_len; // "0x" + prefix + ".." + suffix
    if hex.len() <= needed {
        return hex;
    }
    let start = &hex[..2 + prefix_len]; // "0x" + prefix chars
    let end = &hex[hex.len() - suffix_len..];
    format!("{start}..{end}")
}

/// Short address display: "0x049d..dc7"
pub fn short_address(felt: &Felt) -> String {
    truncate_felt(felt, 4, 4)
}

/// Short hash display: "0x3a2f..8b1c"
pub fn short_hash(felt: &Felt) -> String {
    truncate_felt(felt, 4, 4)
}

/// Format a u128 fri value as STRK amount (5 decimal places).
pub fn format_strk_u128(fri: u128) -> String {
    if fri == 0 {
        return "0 STRK".to_string();
    }
    let whole = fri / 1_000_000_000_000_000_000;
    let frac = (fri % 1_000_000_000_000_000_000) / 10_000_000_000_000; // 5 decimals
    if whole > 0 {
        format!("{whole}.{frac:05} STRK")
    } else if frac > 0 {
        format!("0.{frac:05} STRK")
    } else {
        "< 0.00001 STRK".to_string()
    }
}

/// Format a fee in Felt as STRK. Alias for format_strk.
pub fn format_fee(felt: &Felt) -> String {
    format_strk(felt)
}
