//! Hash functions and domain separation tags for the Privacy Pool.
//!
//! Vendored from `crates/discovery-core/src/privacy_pool/hashes.rs` of
//! `starkware-libs/starknet-privacy@009a94c`. Apache-2.0.

use std::sync::LazyLock;

use starknet_crypto::poseidon_hash_many;
use starknet_types_core::felt::Felt;

use super::types::SecretFelt;

static ENC_CHANNEL_KEY_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("ENC_CHANNEL_KEY_TAG:V1"));
static ENC_SENDER_ADDR_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("ENC_SENDER_ADDR_TAG:V1"));
static SUBCHANNEL_ID_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("SUBCHANNEL_ID_TAG:V1"));
static ENC_TOKEN_TAG: LazyLock<Felt> = LazyLock::new(|| short_string_to_felt("ENC_TOKEN_TAG:V1"));
static NOTE_ID_TAG: LazyLock<Felt> = LazyLock::new(|| short_string_to_felt("NOTE_ID_TAG:V1"));
static ENC_AMOUNT_TAG: LazyLock<Felt> = LazyLock::new(|| short_string_to_felt("ENC_AMOUNT_TAG:V1"));
static NULLIFIER_TAG: LazyLock<Felt> = LazyLock::new(|| short_string_to_felt("NULLIFIER_TAG:V1"));
static CHANNEL_KEY_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("CHANNEL_KEY_TAG:V1"));
static CHANNEL_MARKER_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("CHANNEL_MARKER_TAG:V1"));
static SUBCHANNEL_MARKER_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("SUBCHANNEL_MARKER_TAG:V1"));
static OUTGOING_CHANNEL_ID_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("OUTGOING_CHANNEL_ID_TAG:V1"));
static ENC_RECIPIENT_ADDR_TAG: LazyLock<Felt> =
    LazyLock::new(|| short_string_to_felt("ENC_RECIPIENT_ADDR_TAG:V1"));

fn short_string_to_felt(s: &str) -> Felt {
    assert!(
        s.len() <= 31,
        "short string must be at most 31 bytes, got {}",
        s.len()
    );
    Felt::from_bytes_be_slice(s.as_bytes())
}

pub fn hash(elements: &[Felt]) -> Felt {
    poseidon_hash_many(elements.iter())
}

pub fn compute_enc_channel_key_hash(shared_x: Felt) -> Felt {
    hash(&[*ENC_CHANNEL_KEY_TAG, shared_x])
}

pub fn compute_enc_sender_addr_hash(shared_x: Felt) -> Felt {
    hash(&[*ENC_SENDER_ADDR_TAG, shared_x])
}

pub fn compute_subchannel_id(channel_key: &SecretFelt, index: u64) -> Felt {
    hash(&[
        *SUBCHANNEL_ID_TAG,
        **channel_key,
        Felt::from(index),
        Felt::ZERO,
    ])
}

pub fn compute_enc_token_hash(channel_key: &SecretFelt, index: u64, salt: Felt) -> Felt {
    hash(&[
        *ENC_TOKEN_TAG,
        **channel_key,
        Felt::from(index),
        Felt::ZERO,
        salt,
    ])
}

pub fn compute_note_id(channel_key: &SecretFelt, token: Felt, index: u64) -> Felt {
    hash(&[
        *NOTE_ID_TAG,
        **channel_key,
        token,
        Felt::from(index),
        Felt::ZERO,
    ])
}

pub fn compute_enc_amount_hash(
    channel_key: &SecretFelt,
    token: Felt,
    index: u64,
    salt: u128,
) -> Felt {
    hash(&[
        *ENC_AMOUNT_TAG,
        **channel_key,
        token,
        Felt::from(index),
        Felt::ZERO,
        Felt::from(salt),
    ])
}

pub fn compute_nullifier(
    channel_key: &SecretFelt,
    token: Felt,
    index: u64,
    private_key: &SecretFelt,
) -> Felt {
    hash(&[
        *NULLIFIER_TAG,
        **channel_key,
        token,
        Felt::from(index),
        Felt::ZERO,
        **private_key,
    ])
}

pub fn compute_channel_key(
    sender_addr: Felt,
    private_key: &SecretFelt,
    recipient_addr: Felt,
    recipient_public_key: Felt,
) -> SecretFelt {
    SecretFelt::new(hash(&[
        *CHANNEL_KEY_TAG,
        sender_addr,
        **private_key,
        recipient_addr,
        recipient_public_key,
    ]))
}

pub fn compute_channel_marker(
    channel_key: &SecretFelt,
    sender_addr: Felt,
    recipient_addr: Felt,
    recipient_public_key: Felt,
) -> Felt {
    hash(&[
        *CHANNEL_MARKER_TAG,
        **channel_key,
        sender_addr,
        recipient_addr,
        recipient_public_key,
    ])
}

pub fn compute_subchannel_marker(
    channel_key: &SecretFelt,
    recipient_addr: Felt,
    recipient_public_key: Felt,
    token: Felt,
) -> Felt {
    hash(&[
        *SUBCHANNEL_MARKER_TAG,
        **channel_key,
        recipient_addr,
        recipient_public_key,
        token,
    ])
}

pub fn compute_outgoing_channel_id(
    sender_addr: Felt,
    private_key: &SecretFelt,
    index: u64,
) -> Felt {
    hash(&[
        *OUTGOING_CHANNEL_ID_TAG,
        sender_addr,
        **private_key,
        Felt::from(index),
        Felt::ZERO,
    ])
}

pub fn compute_enc_recipient_addr_hash(
    sender_addr: Felt,
    private_key: &SecretFelt,
    index: u64,
    salt: Felt,
) -> Felt {
    hash(&[
        *ENC_RECIPIENT_ADDR_TAG,
        sender_addr,
        **private_key,
        Felt::from(index),
        Felt::ZERO,
        salt,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_string_round_trip() {
        let felt = short_string_to_felt("hello");
        assert_eq!(felt, Felt::from_hex_unchecked("0x68656c6c6f"));
    }
}
