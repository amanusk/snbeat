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
