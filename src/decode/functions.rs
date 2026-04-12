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
    let mut calls = Vec::with_capacity(num_calls);
    let mut offset = 1;

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

    calls
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
