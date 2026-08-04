use std::sync::Arc;

use starknet::core::types::Felt;

use super::abi::{FunctionDef, ParsedAbi};
use crate::utils::{felt_to_u64, felt_to_usize_checked};

/// A decoded function call with human-readable name.
#[derive(Debug, Clone)]
pub struct DecodedCall {
    pub contract_address: Felt,
    pub function_name: Option<String>,
    pub function_def: Option<FunctionDef>,
    pub selector: Felt,
}

/// Decode the selector of a function call using the parsed ABI.
pub fn decode_function_selector(selector: &Felt, abi: Option<&ParsedAbi>) -> Option<String> {
    abi.and_then(|a| a.get_function(selector))
        .map(|f| f.name.clone())
}

/// Parse an Invoke transaction's calldata to extract individual calls.
///
/// Two mutually incompatible `__execute__` calldata layouts exist on mainnet:
///
/// **Modern (Cairo 1, `Array<Call>`)** — the call data is inlined per call:
/// ```text
/// [n, (to, selector, calldata_len, calldata...) * n]
/// ```
///
/// **Legacy (Cairo 0, `CallArray*`)** — fixed-size 4-felt entries index into a
/// single flat blob appended after the array:
/// ```text
/// [n, (to, selector, data_offset, data_len) * n, calldata_len, calldata...]
/// ```
///
/// Deprecated (Cairo 0) accounts are still live and still relay INVOKE v3
/// transactions, so the layout is a property of the *account class*, not of the
/// transaction version — there is nothing in the transaction to switch on. We
/// pick whichever layout is internally self-consistent, trying the modern one
/// first: a false positive there mangles the rare legacy format, whereas a
/// false positive on the legacy reading would mangle the common one.
pub fn parse_multicall(calldata: &[Felt]) -> Vec<RawCall> {
    if calldata.is_empty() {
        return Vec::new();
    }

    if let Some(calls) = try_parse_inline_calls(calldata) {
        return calls;
    }
    if let Some(calls) = try_parse_legacy_call_array(calldata) {
        return calls;
    }

    // Neither layout validates — the calldata is truncated or otherwise
    // malformed (e.g. a partial record from an external indexer). Fall back to
    // a lenient inline walk so we still surface whatever prefix is readable.
    let num_calls = felt_to_u64(&calldata[0]) as usize;
    parse_call_array(calldata, 1, num_calls).0
}

/// Strictly parse the modern Cairo 1 `Array<Call>` layout.
///
/// Returns `None` unless all `n` calls are present and together they consume
/// the array exactly, with nothing trailing. That exact-consumption check is
/// what makes this safe to try first.
fn try_parse_inline_calls(calldata: &[Felt]) -> Option<Vec<RawCall>> {
    let num_calls = felt_to_usize_checked(&calldata[0])?;
    // Each call needs ≥3 felts, so cap the allocation regardless of the count.
    let mut calls = Vec::with_capacity(num_calls.min(calldata.len() / 3));

    let mut offset = 1usize;
    for _ in 0..num_calls {
        if offset + 3 > calldata.len() {
            return None;
        }
        let contract_address = calldata[offset];
        let selector = calldata[offset + 1];
        let data_len = felt_to_usize_checked(&calldata[offset + 2])?;
        offset += 3;

        let end = offset.checked_add(data_len)?;
        if end > calldata.len() {
            return None;
        }
        calls.push(RawCall::new(
            contract_address,
            selector,
            calldata[offset..end].to_vec(),
        ));
        offset = end;
    }

    (offset == calldata.len()).then_some(calls)
}

/// Strictly parse the legacy Cairo 0 `CallArray*` layout.
///
/// The discriminator is the exact length equation: the 4-felt entries are
/// followed by a `calldata_len` felt whose value must make the trailing blob
/// fill the array precisely. Combined with rejecting any offset/length felt too
/// large to be a real offset or length, that is where essentially all of the
/// discriminating power lives.
///
/// Per-entry offsets are only bounds-checked, not required to be cumulative:
/// the ABI lets entries point at arbitrary (even overlapping) slices of the
/// blob, and rejecting those would drop the transaction back to the lenient
/// inline walk — i.e. back to garbage.
fn try_parse_legacy_call_array(calldata: &[Felt]) -> Option<Vec<RawCall>> {
    let num_calls = felt_to_usize_checked(&calldata[0])?;
    // count felt + n × 4-felt entries → index of the blob's length felt.
    let blob_len_idx = num_calls.checked_mul(4)?.checked_add(1)?;
    let blob_start = blob_len_idx.checked_add(1)?;
    if blob_start > calldata.len() {
        return None;
    }

    let blob_len = felt_to_usize_checked(&calldata[blob_len_idx])?;
    if blob_start.checked_add(blob_len)? != calldata.len() {
        return None;
    }
    let blob = &calldata[blob_start..];

    let mut calls = Vec::with_capacity(num_calls);
    for i in 0..num_calls {
        let base = 1 + i * 4;
        let data_offset = felt_to_usize_checked(&calldata[base + 2])?;
        let data_len = felt_to_usize_checked(&calldata[base + 3])?;
        let end = data_offset.checked_add(data_len)?;
        if end > blob_len {
            return None;
        }
        calls.push(RawCall::new(
            calldata[base],
            calldata[base + 1],
            blob[data_offset..end].to_vec(),
        ));
    }

    Some(calls)
}

/// Parse a sequence of Call structs from a flat felt array starting at `offset`.
/// Each call is: contract_address, selector, data_length, data[0..data_length].
/// Returns (parsed_calls, new_offset_after_all_calls).
pub fn parse_call_array(
    calldata: &[Felt],
    mut offset: usize,
    num_calls: usize,
) -> (Vec<RawCall>, usize) {
    // Cap to the maximum possible calls given available data (each call needs ≥3 felts).
    let remaining = calldata.len().saturating_sub(offset);
    let max_possible = remaining / 3;
    let num_calls = num_calls.min(max_possible);
    let mut calls = Vec::with_capacity(num_calls);

    for _ in 0..num_calls {
        if offset + 2 >= calldata.len() {
            break;
        }

        let contract_address = calldata[offset];
        let selector = calldata[offset + 1];
        let data_len = felt_to_u64(&calldata[offset + 2]) as usize;
        offset += 3;

        // `data_len` comes from an untrusted felt, so it can be up to u64::MAX.
        // An unchecked `offset + data_len` wraps in release builds and can land
        // *below* `offset`, passing the bounds check and then panicking on a
        // slice whose end precedes its start.
        let data = match offset.checked_add(data_len) {
            Some(end) if end <= calldata.len() => calldata[offset..end].to_vec(),
            // Malformed, or a length felt too large to be a real length — take
            // what we can.
            _ => calldata[offset..].to_vec(),
        };
        // Saturate rather than wrap: callers treat an offset past the end as
        // "did not consume cleanly", which a wrapped small value would hide.
        offset = offset.saturating_add(data_len);

        calls.push(RawCall::new(contract_address, selector, data));
    }

    (calls, offset)
}

/// A raw call extracted from multicall calldata.
#[derive(Debug, Clone)]
pub struct RawCall {
    pub contract_address: Felt,
    pub selector: Felt,
    pub data: Vec<Felt>,
    /// Decoded function name (populated after ABI lookup).
    pub function_name: Option<String>,
    /// Full function definition from ABI (for calldata decoding).
    pub function_def: Option<FunctionDef>,
    /// Parsed ABI for the target contract (for resolving struct/enum types during decoding).
    pub contract_abi: Option<Arc<ParsedAbi>>,
}

impl RawCall {
    /// A freshly parsed call, before any ABI lookup has enriched it.
    fn new(contract_address: Felt, selector: Felt, data: Vec<Felt>) -> Self {
        Self {
            contract_address,
            selector,
            data,
            function_name: None,
            function_def: None,
            contract_abi: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: parse_call_array must not OOM when num_calls is absurdly large.
    #[test]
    fn test_parse_call_array_huge_num_calls() {
        // 6 felts of data but claiming billions of calls.
        let data = vec![
            Felt::from(1u64),
            Felt::from(2u64),
            Felt::from(0u64), // data_len = 0
            Felt::from(3u64),
            Felt::from(4u64),
            Felt::from(0u64), // data_len = 0
        ];
        // Request usize::MAX calls — must not panic or OOM.
        let (calls, _offset) = parse_call_array(&data, 0, usize::MAX);
        // Can parse at most 2 calls from 6 felts (each needs ≥3).
        assert!(calls.len() <= 2);
    }

    fn f(v: u64) -> Felt {
        Felt::from(v)
    }

    /// Regression: a `data_len` felt near `u64::MAX` must not panic. An
    /// unchecked `offset + data_len` wraps in release builds to a value *below*
    /// `offset`, which slips past the `<= calldata.len()` guard and then
    /// panics slicing backwards ("slice index starts at 4 but ends at 3").
    /// Debug builds panic earlier still, on the overflowing add.
    ///
    /// Reachable through `parse_multicall`: the strict parsers reject the felt
    /// outright, so control falls through to this lenient walk.
    #[test]
    fn test_parse_multicall_huge_data_len_does_not_panic() {
        let cd = vec![
            f(1),
            f(0xAA1),
            f(0xBB1),
            Felt::from_hex("0xFFFFFFFFFFFFFFFF").unwrap(), // data_len = u64::MAX
        ];
        assert!(try_parse_inline_calls(&cd).is_none());
        assert!(try_parse_legacy_call_array(&cd).is_none());

        let calls = parse_multicall(&cd);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].contract_address, f(0xAA1));
        // Nothing left after the header, so the salvaged data is empty.
        assert!(calls[0].data.is_empty());
    }

    /// The offset returned for such a call must saturate past the end rather
    /// than wrap to a small value — callers read "offset > len" as "did not
    /// consume cleanly", and a wrapped value would masquerade as valid.
    #[test]
    fn test_parse_call_array_huge_data_len_offset_saturates() {
        let cd = vec![
            f(0xAA1),
            f(0xBB1),
            Felt::from_hex("0xFFFFFFFFFFFFFFFF").unwrap(),
            f(0xD1),
        ];
        let (calls, offset) = parse_call_array(&cd, 0, 1);
        assert_eq!(calls.len(), 1);
        assert!(
            offset > cd.len(),
            "offset must not wrap below the input len"
        );
    }

    /// A synthetic legacy Cairo 0 `CallArray*` multicall:
    /// `[n, (to, selector, data_offset, data_len) * n, calldata_len, blob...]`.
    ///
    /// `entries` are `(to, selector, data_offset, data_len)`; the trailing blob
    /// is `blob_len` filler felts.
    fn legacy_calldata(entries: &[(u64, u64, u64, u64)], blob_len: u64) -> Vec<Felt> {
        let mut cd = vec![f(entries.len() as u64)];
        for &(to, sel, off, len) in entries {
            cd.extend([f(to), f(sel), f(off), f(len)]);
        }
        cd.push(f(blob_len));
        cd.extend((0..blob_len).map(|i| f(0xD00 + i)));
        cd
    }

    /// Regression: legacy Cairo 0 accounts index into a trailing calldata blob
    /// rather than inlining per-call data. Read as the modern inline layout, a
    /// leading call with `data_len = 0` makes the parser mistake the *next*
    /// felt — the first entry's `data_len` — for call 2's target, yielding
    /// absurd targets like `0x3`. Calls 2..n were garbage from there on.
    #[test]
    fn test_parse_multicall_legacy_cairo0_layout() {
        // Shape mirrors a real 5-call legacy multicall: first entry has
        // data_offset 0, so the felt at index 3 is 0 and index 4 holds a small
        // length that the inline reading would treat as a contract address.
        let entries = [
            (0xAA1, 0xBB1, 0, 3),
            (0xAA1, 0xBB2, 3, 3),
            (0xAA2, 0xBB3, 6, 11),
            (0xAA2, 0xBB4, 17, 1),
            (0xAA2, 0xBB5, 18, 3),
        ];
        let cd = legacy_calldata(&entries, 21);
        assert_eq!(cd.len(), 43, "1 + 5*4 + 1 + 21");

        let calls = parse_multicall(&cd);
        assert_eq!(calls.len(), 5);
        for (call, &(to, sel, off, len)) in calls.iter().zip(&entries) {
            assert_eq!(call.contract_address, f(to));
            assert_eq!(call.selector, f(sel));
            assert_eq!(call.data.len(), len as usize);
            // Data must come from the trailing blob at the entry's offset.
            assert_eq!(call.data[0], f(0xD00 + off));
        }
        // The symptom: no call may target the misread length felt.
        assert!(calls.iter().all(|c| c.contract_address != f(3)));
    }

    /// A well-formed modern multicall must keep parsing as inline, including
    /// when its first call takes zero arguments (the shape that triggers the
    /// legacy misread) — the modern layout always wins when it is consistent.
    #[test]
    fn test_parse_multicall_inline_wins_over_legacy() {
        let cd = vec![
            f(2),
            f(0xAA1),
            f(0xBB1),
            f(0), // zero-arg call
            f(0xAA2),
            f(0xBB2),
            f(2),
            f(0xD1),
            f(0xD2),
        ];
        let calls = parse_multicall(&cd);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].contract_address, f(0xAA1));
        assert!(calls[0].data.is_empty());
        assert_eq!(calls[1].contract_address, f(0xAA2));
        assert_eq!(calls[1].data, vec![f(0xD1), f(0xD2)]);
        assert!(try_parse_legacy_call_array(&cd).is_none());
    }

    /// The ABI permits entries pointing at overlapping slices of the blob;
    /// rejecting them would drop the tx back to the lenient inline walk.
    #[test]
    fn test_legacy_allows_overlapping_offsets() {
        let cd = legacy_calldata(&[(0xAA1, 0xBB1, 0, 3), (0xAA1, 0xBB2, 0, 3)], 3);
        let calls = parse_multicall(&cd);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].data, calls[1].data);
    }

    /// A length felt too large to be a real length must not sneak past the
    /// bounds check by truncating to a plausible small number.
    #[test]
    fn test_legacy_rejects_truncating_length_felt() {
        let mut cd = legacy_calldata(&[(0xAA1, 0xBB1, 0, 3)], 3);
        // 2^64 + 3 truncates to 3 under a low-64-bit conversion.
        cd[4] = Felt::from_hex("0x10000000000000003").unwrap();
        assert!(try_parse_legacy_call_array(&cd).is_none());
    }

    /// Truncated calldata matches neither layout and must still yield a
    /// best-effort inline parse rather than nothing.
    #[test]
    fn test_parse_multicall_truncated_falls_back() {
        // Claims 2 calls; the second call's data is cut short.
        let cd = vec![
            f(2),
            f(0xAA1),
            f(0xBB1),
            f(0),
            f(0xAA2),
            f(0xBB2),
            f(5),
            f(0xD1),
        ];
        assert!(try_parse_inline_calls(&cd).is_none());
        assert!(try_parse_legacy_call_array(&cd).is_none());
        let calls = parse_multicall(&cd);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].contract_address, f(0xAA1));
        assert_eq!(calls[1].contract_address, f(0xAA2));
    }

    #[test]
    fn test_parse_multicall_normal() {
        let data = vec![
            Felt::from(1u64),   // 1 call
            Felt::from(0xAu64), // to
            Felt::from(0xBu64), // selector
            Felt::from(2u64),   // data_len
            Felt::from(0xCu64),
            Felt::from(0xDu64),
        ];
        let calls = parse_multicall(&data);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].contract_address, Felt::from(0xAu64));
        assert_eq!(calls[0].selector, Felt::from(0xBu64));
        assert_eq!(calls[0].data.len(), 2);
    }
}
