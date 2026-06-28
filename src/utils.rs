use starknet::core::types::Felt;

/// Convert the lower 8 bytes of a felt to u64.
pub fn felt_to_u64(felt: &Felt) -> u64 {
    let bytes = felt.to_bytes_be();
    u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]))
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
