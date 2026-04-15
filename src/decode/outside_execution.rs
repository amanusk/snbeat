//! Detection and parsing of SNIP-9 outside execution (meta transaction) calls.
//!
//! Outside execution allows a user ("intender") to sign a set of calls that a
//! relayer submits on-chain. The relayer pays gas; the intender's intent is
//! encoded in an `OutsideExecution` struct.
//!
//! Two nonce formats exist:
//!
//! **Standard SNIP-9 (v1/v2)** — nonce is 1 felt:
//!   [0] caller, [1] nonce, [2] execute_after, [3] execute_before, [4] num_calls, ...
//!
//! **Argent v3** — nonce is `(felt252, u128)` = 2 felts:
//!   [0] caller, [1] nonce.0, [2] nonce.1, [3] execute_after, [4] execute_before, [5] num_calls, ...
//!
//! The parser tries both layouts and validates structurally.

use starknet::core::types::Felt;

use super::functions::{RawCall, parse_call_array};
use crate::utils::felt_to_u64;

/// The SNIP-9 "ANY_CALLER" sentinel: ASCII encoding of "ANY_CALLER".
pub const ANY_CALLER: Felt = Felt::from_hex_unchecked("0x414e595f43414c4c4552");

/// Which version of the outside execution entrypoint was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutsideExecutionVersion {
    V1,
    V2,
    V3,
}

impl std::fmt::Display for OutsideExecutionVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1 => write!(f, "v1"),
            Self::V2 => write!(f, "v2"),
            Self::V3 => write!(f, "v3"),
        }
    }
}

/// Parsed outside execution with intender info and inner calls.
#[derive(Debug, Clone)]
pub struct OutsideExecutionInfo {
    /// The account contract that will execute the inner calls (the "intender").
    /// For direct calls this is the contract_address of the execute_from_outside call.
    pub intender: Felt,
    /// Who can submit this outside execution (ANY_CALLER or a specific address).
    pub caller: Felt,
    /// Non-sequential nonce for this outside execution.
    pub nonce: Felt,
    /// Timestamp lower bound (execute_after).
    pub execute_after: u64,
    /// Timestamp upper bound (execute_before).
    pub execute_before: u64,
    /// Inner calls extracted from the OutsideExecution struct.
    pub inner_calls: Vec<RawCall>,
    /// Raw signature felts.
    pub signature: Vec<Felt>,
    /// Which version of the entrypoint was used.
    pub version: OutsideExecutionVersion,
}

/// Check if a function name is an outside execution entrypoint.
pub fn is_outside_execution(function_name: &str) -> Option<OutsideExecutionVersion> {
    match function_name {
        "execute_from_outside" => Some(OutsideExecutionVersion::V1),
        "execute_from_outside_v2" => Some(OutsideExecutionVersion::V2),
        "execute_from_outside_v3" => Some(OutsideExecutionVersion::V3),
        _ => None,
    }
}

/// Heuristic: detect outside execution by calldata pattern when function name is
/// unresolved (e.g. Argent/Dojo contracts with component-based selectors where
/// entry point selectors don't match starknet_keccak(short_name)).
///
/// Returns `true` if data[0] is ANY_CALLER and the struct validates structurally.
pub fn looks_like_outside_execution(call: &RawCall) -> bool {
    if call.data.len() < 6 {
        return false;
    }
    // First felt must be ANY_CALLER (most outside executions use this)
    if call.data[0] != ANY_CALLER {
        return false;
    }
    // Try both nonce formats and see if either produces a valid parse
    try_parse_with_nonce_size(&call.data, 1).is_some()
        || try_parse_with_nonce_size(&call.data, 2).is_some()
}

/// Parse an outside execution call's raw calldata into structured info.
/// Tries both 1-felt nonce (standard SNIP-9) and 2-felt nonce (Argent v3)
/// layouts, returning the first that validates structurally.
///
/// The `call.contract_address` is the intender (the account being called).
/// Returns `None` if the calldata is too short or malformed.
pub fn parse_outside_execution(
    call: &RawCall,
    version: OutsideExecutionVersion,
) -> Option<OutsideExecutionInfo> {
    let data = &call.data;
    if data.len() < 5 {
        return None;
    }

    let caller = data[0];

    // Try 1-felt nonce (standard SNIP-9 v1/v2) first, then 2-felt (Argent v3).
    let (nonce, nonce_size) = if let Some(parsed) = try_parse_with_nonce_size(data, 1) {
        (data[1], parsed)
    } else if let Some(parsed) = try_parse_with_nonce_size(data, 2) {
        (data[1], parsed)
    } else {
        return None;
    };

    let header_size = 1 + nonce_size + 2 + 1; // caller + nonce + after/before + num_calls
    let execute_after = felt_to_u64(&data[1 + nonce_size]);
    let execute_before = felt_to_u64(&data[1 + nonce_size + 1]);
    let (inner_calls, signature) = (nonce, header_size); // reuse parsed result below

    // Re-parse with the validated nonce_size
    let _ = (inner_calls, signature); // discard shadowed bindings
    let num_inner_calls = felt_to_u64(&data[header_size - 1]) as usize;
    let (inner_calls, offset_after_calls) = parse_call_array(data, header_size, num_inner_calls);

    let signature = if offset_after_calls < data.len() {
        let sig_len = felt_to_u64(&data[offset_after_calls]) as usize;
        let sig_start = offset_after_calls + 1;
        if sig_start + sig_len <= data.len() {
            data[sig_start..sig_start + sig_len].to_vec()
        } else {
            data[sig_start..].to_vec()
        }
    } else {
        Vec::new()
    };

    Some(OutsideExecutionInfo {
        intender: call.contract_address,
        caller,
        nonce,
        execute_after,
        execute_before,
        inner_calls,
        signature,
        version,
    })
}

/// Try to parse the OutsideExecution struct with a given nonce size (1 or 2 felts).
/// Returns `Some(ValidatedParse)` with the nonce_size if the struct validates, else `None`.
///
/// Validation: num_inner_calls is reasonable, inner calls consume data correctly,
/// and the remaining data is a valid signature array (sig_len + sig_len felts = remaining).
fn try_parse_with_nonce_size(data: &[Felt], nonce_size: usize) -> Option<usize> {
    // Header: caller(1) + nonce(nonce_size) + execute_after(1) + execute_before(1) + num_calls(1)
    let header_size = 1 + nonce_size + 2 + 1;
    if data.len() < header_size {
        return None;
    }

    let num_inner_calls = felt_to_u64(&data[header_size - 1]) as usize;

    // Reject obviously invalid call counts
    if num_inner_calls > 100 {
        return None;
    }

    let remaining_after_header = data.len().saturating_sub(header_size);
    if num_inner_calls > 0 && remaining_after_header < 3 {
        return None;
    }

    // Parse inner calls
    let (inner_calls, offset_after_calls) = parse_call_array(data, header_size, num_inner_calls);
    if inner_calls.len() != num_inner_calls {
        return None;
    }

    // Validate signature: remaining data should be sig_len + exactly sig_len felts
    if offset_after_calls < data.len() {
        let sig_len = felt_to_u64(&data[offset_after_calls]) as usize;
        let expected_end = offset_after_calls + 1 + sig_len;
        if expected_end != data.len() {
            return None;
        }
    } else if offset_after_calls != data.len() {
        // No signature but data remaining — invalid
        return None;
    }

    Some(nonce_size)
}

/// Known AVNU paymaster forwarder contract addresses.
/// These call `execute(account, entrypoint, calldata, gas_token, gas_amount)` or
/// `execute_sponsored(account, entrypoint, calldata, sponsor_metadata)` which forwards
/// an `execute_from_outside` call to the account.
const AVNU_FORWARDER_ADDRESSES: &[Felt] = &[Felt::from_hex_unchecked(
    "0x127021a1b5a52d3174c2ab077c2b043c80369250d29428cee956d76ee51584f",
)];

/// Check if a contract address is a known AVNU forwarder.
pub fn is_avnu_forwarder(address: &Felt) -> bool {
    AVNU_FORWARDER_ADDRESSES.contains(address)
}

/// Parse an AVNU forwarder call to extract the embedded OutsideExecution.
///
/// AVNU forwarder functions have the signature:
///   execute(account_address, entrypoint, calldata, gas_token_address, gas_amount)
///   execute_sponsored(account_address, entrypoint, calldata, sponsor_metadata)
///
/// The `calldata` array parameter contains the OutsideExecution struct (same format as
/// a direct `execute_from_outside` call on the account).
/// The `account_address` parameter IS the intender.
pub fn parse_forwarder_call(call: &RawCall) -> Option<OutsideExecutionInfo> {
    let data = &call.data;
    // Minimum: account_address(1) + entrypoint(1) + calldata_len(1) + at least 5 felts inside = 8
    if data.len() < 8 {
        return None;
    }

    let account_address = data[0]; // the intender
    let _entrypoint = data[1]; // the execute_from_outside selector on the account
    let calldata_len = felt_to_u64(&data[2]) as usize;

    // Validate the calldata array fits in the remaining data
    if calldata_len == 0 || 3 + calldata_len > data.len() {
        return None;
    }

    // Extract the inner calldata (the OutsideExecution struct)
    let inner_data: Vec<Felt> = data[3..3 + calldata_len].to_vec();

    // Build a synthetic RawCall with the inner calldata so we can reuse parse_outside_execution
    let inner_call = RawCall {
        contract_address: account_address, // intender is the account
        selector: data[1],
        data: inner_data,
        function_name: None,
        function_def: None,
        contract_abi: None,
    };

    parse_outside_execution(&inner_call, OutsideExecutionVersion::V2)
}

/// Returns `true` if the caller field is the ANY_CALLER sentinel.
pub fn is_any_caller(caller: &Felt) -> bool {
    *caller == ANY_CALLER
}

/// Format the caller field for display.
pub fn format_caller(caller: &Felt) -> String {
    if is_any_caller(caller) {
        "ANY_CALLER".to_string()
    } else {
        format!("{:#x}", caller)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn felt(hex: &str) -> Felt {
        Felt::from_hex(hex).unwrap()
    }

    #[test]
    fn test_is_outside_execution() {
        assert_eq!(
            is_outside_execution("execute_from_outside"),
            Some(OutsideExecutionVersion::V1)
        );
        assert_eq!(
            is_outside_execution("execute_from_outside_v2"),
            Some(OutsideExecutionVersion::V2)
        );
        assert_eq!(
            is_outside_execution("execute_from_outside_v3"),
            Some(OutsideExecutionVersion::V3)
        );
        assert_eq!(is_outside_execution("transfer"), None);
        assert_eq!(is_outside_execution("execute"), None);
    }

    #[test]
    fn test_parse_outside_execution_basic() {
        // Build synthetic calldata for execute_from_outside_v2 with 1 inner call
        let intender = felt("0xabc");
        let caller = ANY_CALLER;
        let nonce = felt("0x42");
        let execute_after = Felt::from(1000u64);
        let execute_before = Felt::from(2000u64);

        // Inner call: to=0xdef, selector=0x123, data=[0x1, 0x2]
        let inner_to = felt("0xdef");
        let inner_selector = felt("0x123");
        let inner_data_len = Felt::from(2u64);
        let inner_data_0 = Felt::from(1u64);
        let inner_data_1 = Felt::from(2u64);

        // Signature: [0xsig1, 0xsig2]
        let sig_len = Felt::from(2u64);
        let sig_0 = felt("0xfeed");
        let sig_1 = felt("0xbeef");

        let data = vec![
            caller,
            nonce,
            execute_after,
            execute_before,
            Felt::from(1u64), // num_inner_calls
            inner_to,
            inner_selector,
            inner_data_len,
            inner_data_0,
            inner_data_1,
            sig_len,
            sig_0,
            sig_1,
        ];

        let call = RawCall {
            contract_address: intender,
            selector: felt("0x999"),
            data,
            function_name: Some("execute_from_outside_v2".into()),
            function_def: None,
            contract_abi: None,
        };

        let oe = parse_outside_execution(&call, OutsideExecutionVersion::V2).unwrap();

        assert_eq!(oe.intender, intender);
        assert_eq!(oe.caller, ANY_CALLER);
        assert_eq!(oe.nonce, nonce);
        assert_eq!(oe.execute_after, 1000);
        assert_eq!(oe.execute_before, 2000);
        assert_eq!(oe.version, OutsideExecutionVersion::V2);
        assert_eq!(oe.inner_calls.len(), 1);
        assert_eq!(oe.inner_calls[0].contract_address, inner_to);
        assert_eq!(oe.inner_calls[0].selector, inner_selector);
        assert_eq!(oe.inner_calls[0].data.len(), 2);
        assert_eq!(oe.signature.len(), 2);
    }

    #[test]
    fn test_parse_outside_execution_multiple_inner_calls() {
        let data = vec![
            ANY_CALLER,
            Felt::from(1u64),    // nonce
            Felt::from(0u64),    // execute_after
            Felt::from(9999u64), // execute_before
            Felt::from(2u64),    // num_inner_calls
            // Call 0: to=0x10, selector=0x20, data=[]
            felt("0x10"),
            felt("0x20"),
            Felt::from(0u64),
            // Call 1: to=0x30, selector=0x40, data=[0x50]
            felt("0x30"),
            felt("0x40"),
            Felt::from(1u64),
            felt("0x50"),
            // Signature: empty
            Felt::from(0u64),
        ];

        let call = RawCall {
            contract_address: felt("0xabc"),
            selector: felt("0x999"),
            data,
            function_name: None,
            function_def: None,
            contract_abi: None,
        };

        let oe = parse_outside_execution(&call, OutsideExecutionVersion::V3).unwrap();
        assert_eq!(oe.inner_calls.len(), 2);
        assert_eq!(oe.inner_calls[0].contract_address, felt("0x10"));
        assert_eq!(oe.inner_calls[0].data.len(), 0);
        assert_eq!(oe.inner_calls[1].contract_address, felt("0x30"));
        assert_eq!(oe.inner_calls[1].data, vec![felt("0x50")]);
        assert!(oe.signature.is_empty());
    }

    #[test]
    fn test_parse_outside_execution_too_short() {
        let call = RawCall {
            contract_address: felt("0xabc"),
            selector: felt("0x999"),
            data: vec![Felt::from(1u64), Felt::from(2u64)], // only 2 felts
            function_name: None,
            function_def: None,
            contract_abi: None,
        };
        assert!(parse_outside_execution(&call, OutsideExecutionVersion::V2).is_none());
    }

    #[test]
    fn test_any_caller() {
        assert!(is_any_caller(&ANY_CALLER));
        assert!(!is_any_caller(&Felt::from(123u64)));
    }

    /// Regression: if a function is named execute_from_outside_v2 but the calldata
    /// is from a forwarder contract (different layout), num_inner_calls parsed from
    /// data[4] can be a huge garbage value. This must not OOM or crash.
    #[test]
    fn test_parse_outside_execution_garbage_num_calls() {
        // Simulate a forwarder whose data[4] is an address (huge number, not a call count).
        let data = vec![
            felt("0x414e595f43414c4c4552"), // caller
            Felt::from(1u64),               // nonce
            Felt::from(1000u64),            // execute_after
            Felt::from(2000u64),            // execute_before
            // "num_calls" is actually a large address — would OOM if used as Vec capacity
            felt("0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"),
            // Only a few more felts — nowhere near enough for billions of calls
            Felt::from(42u64),
            Felt::from(43u64),
        ];

        let call = RawCall {
            contract_address: felt("0xabc"),
            selector: felt("0x999"),
            data,
            function_name: Some("execute_from_outside_v2".into()),
            function_def: None,
            contract_abi: None,
        };

        // Must not panic or OOM — should return None (validation rejects garbage).
        assert!(parse_outside_execution(&call, OutsideExecutionVersion::V2).is_none());
    }

    /// Regression: calldata has exactly 5 felts with non-zero num_calls but no room
    /// for any actual call data.
    #[test]
    fn test_parse_outside_execution_no_room_for_calls() {
        let data = vec![
            ANY_CALLER,
            Felt::from(1u64),    // nonce
            Felt::from(0u64),    // execute_after
            Felt::from(9999u64), // execute_before
            Felt::from(5u64),    // claims 5 inner calls but no data follows
        ];

        let call = RawCall {
            contract_address: felt("0xabc"),
            selector: felt("0x999"),
            data,
            function_name: None,
            function_def: None,
            contract_abi: None,
        };

        // Should return None — can't have 5 calls with 0 remaining felts.
        assert!(parse_outside_execution(&call, OutsideExecutionVersion::V2).is_none());
    }

    /// Argent v3 accounts use nonce: (felt252, u128) = 2 felts.
    /// The parser must detect this and parse correctly.
    #[test]
    fn test_parse_outside_execution_argent_v3_two_felt_nonce() {
        // Simplified version of the real tx 0xcbb78b... calldata
        let data = vec![
            ANY_CALLER,                                                                // caller
            felt("0x4874fad70d602bc5306439d34b1d8c54555f852d3366695ca45c7fdc1c553b6"), // nonce.0
            Felt::from(1u64),          // nonce.1 (u128)
            Felt::from(0u64),          // execute_after
            Felt::from(1776060058u64), // execute_before
            Felt::from(2u64),          // num_inner_calls
            // Inner call 0: to=0xaaa, selector=0xbbb, data=[0x1]
            felt("0xaaa"),
            felt("0xbbb"),
            Felt::from(1u64),
            Felt::from(1u64),
            // Inner call 1: to=0xccc, selector=0xddd, data=[]
            felt("0xccc"),
            felt("0xddd"),
            Felt::from(0u64),
            // Signature: 3 felts
            Felt::from(3u64),
            felt("0x111"),
            felt("0x222"),
            felt("0x333"),
        ];

        let call = RawCall {
            contract_address: felt(
                "0x4fc0fc0cc69761d6bc13e57f6e6839ea180ac27189e9f90466abf53c25dc327",
            ),
            selector: felt("0x3dbc508ba4afd040c8dc4ff8a61113a7bcaf5eae88a6ba27b3c50578b3587e3"),
            data,
            function_name: None, // Argent component selectors don't resolve
            function_def: None,
            contract_abi: None,
        };

        // Heuristic detection should find it
        assert!(looks_like_outside_execution(&call));

        // Parsing should auto-detect the 2-felt nonce
        let oe = parse_outside_execution(&call, OutsideExecutionVersion::V2).unwrap();
        assert_eq!(
            oe.intender,
            felt("0x4fc0fc0cc69761d6bc13e57f6e6839ea180ac27189e9f90466abf53c25dc327")
        );
        assert_eq!(oe.caller, ANY_CALLER);
        assert_eq!(oe.execute_after, 0);
        assert_eq!(oe.execute_before, 1776060058);
        assert_eq!(oe.inner_calls.len(), 2);
        assert_eq!(oe.inner_calls[0].contract_address, felt("0xaaa"));
        assert_eq!(oe.inner_calls[1].contract_address, felt("0xccc"));
        assert_eq!(oe.signature.len(), 3);
    }

    /// Heuristic should NOT trigger on random calldata that happens to start with ANY_CALLER.
    #[test]
    fn test_heuristic_rejects_non_outside_execution() {
        let call = RawCall {
            contract_address: felt("0xabc"),
            selector: felt("0x999"),
            data: vec![
                ANY_CALLER,
                Felt::from(42u64),
                Felt::from(100u64),
                Felt::from(200u64),
                Felt::from(1u64), // "1 call"
                felt("0xdef"),
                felt("0x123"),
                Felt::from(0u64),
                // No valid signature follows — just random data
                Felt::from(999u64),
                Felt::from(888u64),
            ],
            function_name: None,
            function_def: None,
            contract_abi: None,
        };
        // Signature validation should reject: data doesn't end with sig_len + sig_len felts
        assert!(!looks_like_outside_execution(&call));
    }

    /// AVNU forwarder wraps execute_from_outside in execute_sponsored(account, entrypoint, calldata, metadata).
    /// The calldata array IS the OutsideExecution struct. The account_address IS the intender.
    #[test]
    fn test_parse_avnu_forwarder_call() {
        let forwarder = felt("0x127021a1b5a52d3174c2ab077c2b043c80369250d29428cee956d76ee51584f");
        let intender = felt("0x643130c3b10cbd7a1e2ea3cda084606d3d03971376780d1ae5a65c06cd26604");
        let entrypoint = felt("0x34cc13b274446654ca3233ed2c1620d4c5d1d32fd20b47146a3371064bdc57d");

        // Build the inner OutsideExecution calldata (1-felt nonce, 1 inner call, 2-felt sig)
        // Total inner: 5 (header) + 6 (call with 3 data) + 1 + 2 (sig) = 14
        let inner_calldata = vec![
            forwarder,                 // caller = forwarder (not ANY_CALLER)
            felt("0x19337909"),        // nonce
            Felt::from(1u64),          // execute_after
            Felt::from(1776181022u64), // execute_before
            Felt::from(1u64),          // num_inner_calls
            // Inner call 0
            felt("0x377c2d65debb3978ea81904e7d59740da1f07412e30d01c5ded1c5d6f1ddc43"),
            felt("0x2f0b3c5710379609eb5495f1ecd348cb28167711b73609fe565a72734550354"),
            Felt::from(3u64), // calldata_len
            felt("0xaa"),
            felt("0xbb"),
            felt("0xcc"),
            // Signature
            Felt::from(2u64),
            felt("0xfeed"),
            felt("0xbeef"),
        ];

        // Build the forwarder call data: (account, entrypoint, calldata_array, sponsor_metadata)
        let mut data = vec![
            intender,                                // account_address
            entrypoint,                              // entrypoint
            Felt::from(inner_calldata.len() as u64), // calldata array length
        ];
        data.extend_from_slice(&inner_calldata);
        // sponsor_metadata: Array<felt252>
        data.push(Felt::from(1u64)); // metadata len
        data.push(felt("0xabcd")); // metadata

        let call = RawCall {
            contract_address: forwarder,
            selector: felt("0x3d82f059"), // execute_sponsored (component selector)
            data,
            function_name: None,
            function_def: None,
            contract_abi: None,
        };

        assert!(is_avnu_forwarder(&forwarder));

        let oe = parse_forwarder_call(&call).unwrap();
        assert_eq!(oe.intender, intender);
        assert_eq!(oe.caller, forwarder); // caller = forwarder, NOT ANY_CALLER
        assert_eq!(oe.execute_after, 1);
        assert_eq!(oe.execute_before, 1776181022);
        assert_eq!(oe.inner_calls.len(), 1);
        assert_eq!(
            oe.inner_calls[0].contract_address,
            felt("0x377c2d65debb3978ea81904e7d59740da1f07412e30d01c5ded1c5d6f1ddc43")
        );
        assert_eq!(oe.inner_calls[0].data.len(), 3);
        assert_eq!(oe.signature.len(), 2);
    }
}
