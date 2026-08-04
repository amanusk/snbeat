use starknet::core::types::Felt;

/// Convert the lower 8 bytes of a felt to u64.
pub fn felt_to_u64(felt: &Felt) -> u64 {
    let bytes = felt.to_bytes_be();
    u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]))
}

/// Convert a felt to usize, returning `None` if it does not fit.
///
/// Unlike [`felt_to_u64`] this does not silently truncate. Use it when a felt
/// that *should* hold a small number (a length, an offset, a count) might
/// actually hold a 251-bit value: truncation can turn a contract address into
/// a plausible-looking small integer that passes a bounds check.
///
/// Returns `usize` rather than `u64` deliberately. Every caller wants an index
/// or a length, and a trailing `as usize` would re-open the same truncation
/// hole on a 32-bit target — the exact hazard this function exists to close.
pub fn felt_to_usize_checked(felt: &Felt) -> Option<usize> {
    let bytes = felt.to_bytes_be();
    if bytes[..24].iter().any(|&b| b != 0) {
        return None;
    }
    let value = u64::from_be_bytes(bytes[24..32].try_into().ok()?);
    usize::try_from(value).ok()
}

/// Convert the lower 16 bytes of a felt to u128.
pub fn felt_to_u128(felt: &Felt) -> u128 {
    let bytes = felt.to_bytes_be();
    u128::from_be_bytes(bytes[16..32].try_into().unwrap_or([0u8; 16]))
}

/// Insert `[lo, hi]` into a set of sorted, non-overlapping block intervals,
/// coalescing any that overlap or merely touch (adjacency merges, so
/// `[100,200]` + `[201,300]` becomes `[100,300]`). Keeps `ranges` sorted and
/// minimal. Tolerates `lo > hi` by swapping. Shared between the in-memory
/// scanned-range tracker (`AddressInfoState::note_scanned_call_range`) and the
/// SQLite-backed persistence (`cache`'s `add_call_scanned_range`).
pub fn merge_block_interval(ranges: &mut Vec<(u64, u64)>, lo: u64, hi: u64) {
    let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
    let mut merged = (lo, hi);
    let mut rest = Vec::with_capacity(ranges.len() + 1);
    for &(rlo, rhi) in ranges.iter() {
        if rlo <= merged.1.saturating_add(1) && merged.0.saturating_sub(1) <= rhi {
            merged = (merged.0.min(rlo), merged.1.max(rhi));
        } else {
            rest.push((rlo, rhi));
        }
    }
    rest.push(merged);
    rest.sort_unstable();
    *ranges = rest;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_felt_to_usize_checked_accepts_small_values() {
        assert_eq!(felt_to_usize_checked(&Felt::from(0u64)), Some(0));
        assert_eq!(felt_to_usize_checked(&Felt::from(42u64)), Some(42));
        // Fits exactly where `usize` is 64-bit wide; correctly rejected where
        // it is narrower — so compare against the same checked narrowing.
        assert_eq!(
            felt_to_usize_checked(&Felt::from_hex("0xFFFFFFFFFFFFFFFF").unwrap()),
            usize::try_from(u64::MAX).ok()
        );
    }

    /// A felt too large to be a real length must be rejected outright, not
    /// truncated into a plausible small number that passes a bounds check —
    /// which is exactly what [`felt_to_u64`] would do here.
    #[test]
    fn test_felt_to_usize_checked_rejects_oversized_felts() {
        // 2^64 + 3: the low 64 bits look like a harmless `3`.
        let wraps = Felt::from_hex("0x10000000000000003").unwrap();
        assert_eq!(felt_to_u64(&wraps), 3, "low-64 conversion truncates");
        assert_eq!(
            felt_to_usize_checked(&wraps),
            None,
            "checked conversion rejects"
        );

        // A full-width, address-shaped felt whose low 64 bits are all zero:
        // truncation would read it as `0`, a perfectly plausible length.
        let addr =
            Felt::from_hex("0x700000000000000000000000000000000000000000000000000000000000000")
                .unwrap();
        assert_eq!(felt_to_u64(&addr), 0, "low-64 conversion truncates to zero");
        assert_eq!(felt_to_usize_checked(&addr), None);
    }
}
