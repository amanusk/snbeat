use std::sync::Arc;

use starknet::core::types::Felt;

use super::abi::{FunctionDef, ParsedAbi};
use crate::utils::felt_to_u64;

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
/// Starknet account contracts use a multicall pattern:
///   calldata[0] = number_of_calls
///   For each call:
///     calldata[i]   = contract_address
///     calldata[i+1] = selector
///     calldata[i+2] = data_length
///     calldata[i+3..i+3+data_length] = call data
pub fn parse_multicall(calldata: &[Felt]) -> Vec<RawCall> {
    if calldata.is_empty() {
        return Vec::new();
    }

    let num_calls = felt_to_u64(&calldata[0]) as usize;
    parse_call_array(calldata, 1, num_calls).0
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

        let data = if offset + data_len <= calldata.len() {
            calldata[offset..offset + data_len].to_vec()
        } else {
            // Malformed — take what we can
            calldata[offset..].to_vec()
        };
        offset += data_len;

        calls.push(RawCall {
            contract_address,
            selector,
            data,
            function_name: None,
            function_def: None,
            contract_abi: None,
        });
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
