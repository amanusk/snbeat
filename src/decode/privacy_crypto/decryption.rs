//! ECDH-based decryption for the Privacy Pool.
//!
//! Vendored from `crates/discovery-core/src/privacy_pool/decryption.rs` of
//! `starkware-libs/starknet-privacy@009a94c`. Apache-2.0.

use starknet_types_core::{curve::AffinePoint, felt::Felt};
use thiserror::Error;

use super::hashes::{
    compute_enc_amount_hash, compute_enc_channel_key_hash, compute_enc_recipient_addr_hash,
    compute_enc_sender_addr_hash, compute_enc_token_hash,
};
use super::types::{
    ChannelInfo, EncChannelInfo, EncOutgoingChannelInfo, EncSubchannelInfo, SecretFelt,
    felt_low_u128,
};

/// Salt value indicating an open (plaintext) note.
/// Open notes store their amount unencrypted in the lower 128 bits.
pub const OPEN_NOTE_SALT: u128 = 1;

#[derive(Debug, Error)]
pub enum DecryptionError {
    #[error("invalid ephemeral pubkey: x-coordinate is not on the curve")]
    InvalidEphemeralPubkey,
}

/// Decrypts encrypted channel info using ECDH.
///
/// 1. Recover the ephemeral public key point from its x-coordinate.
/// 2. Compute ECDH shared secret: `shared_point = ephemeral_pubkey * private_key`.
/// 3. Decrypt: `plaintext = ciphertext - hash(tag, shared_x)`.
pub fn decrypt_channel_info(
    enc: &EncChannelInfo,
    private_key: &SecretFelt,
) -> Result<ChannelInfo, DecryptionError> {
    let ephemeral_point = AffinePoint::new_from_x(&enc.ephemeral_pubkey, false)
        .ok_or(DecryptionError::InvalidEphemeralPubkey)?;
    let shared_point = &ephemeral_point * **private_key;
    let shared_x = shared_point.x();

    let channel_key = enc.enc_channel_key - compute_enc_channel_key_hash(shared_x);
    let sender_addr = enc.enc_sender_addr - compute_enc_sender_addr_hash(shared_x);

    Ok(ChannelInfo {
        channel_key: SecretFelt::new(channel_key),
        sender_addr,
    })
}

/// Decrypts encrypted subchannel info to get the token address:
/// `token = enc_token - hash(ENC_TOKEN_TAG, channel_key, index, 0, salt)`.
pub fn decrypt_subchannel_token(
    enc: &EncSubchannelInfo,
    channel_key: &SecretFelt,
    index: u64,
) -> Felt {
    let enc_token_hash = compute_enc_token_hash(channel_key, index, enc.salt);
    enc.enc_token - enc_token_hash
}

/// Unpacks a packed note amount into salt and encrypted amount.
///
/// Packed format (big-endian): `packed = salt * 2^128 + enc_amount`.
pub fn unpack_note_amount(packed_amount: Felt) -> (u128, u128) {
    let d = packed_amount.to_le_digits();
    let enc_amount = d[0] as u128 | (d[1] as u128) << 64;
    let salt = d[2] as u128 | (d[3] as u128) << 64;
    (salt, enc_amount)
}

/// Decrypts an encrypted note amount.
///
/// `amount = (enc_amount - pad) % 2^128`, where
/// `pad = hash(ENC_AMOUNT_TAG, channel_key, token, index, 0, salt) % 2^128`.
pub fn decrypt_note_amount(
    enc_amount: u128,
    salt: u128,
    channel_key: &SecretFelt,
    token: Felt,
    index: u64,
) -> u128 {
    let enc_amount_hash = compute_enc_amount_hash(channel_key, token, index, salt);
    let pad = felt_low_u128(enc_amount_hash);
    enc_amount.wrapping_sub(pad)
}

/// Unpacks and decrypts a packed note value into `(amount, salt)`.
///
/// Open notes (salt == 1) store their amount in plaintext; encrypted notes
/// (salt >= 2) require ECDH-based decryption.
pub fn decrypt_packed_value(
    packed: Felt,
    channel_key: &SecretFelt,
    token: Felt,
    index: u64,
) -> (u128, u128) {
    let (salt, enc_amount) = unpack_note_amount(packed);
    let amount = if salt == OPEN_NOTE_SALT {
        enc_amount
    } else {
        decrypt_note_amount(enc_amount, salt, channel_key, token, index)
    };
    (amount, salt)
}

/// Decrypts an outgoing channel's encrypted recipient address:
/// `recipient_addr = enc_recipient_addr - hash(ENC_RECIPIENT_ADDR_TAG, sender_addr, private_key, index, 0, salt)`.
pub fn decrypt_outgoing_recipient_addr(
    enc: &EncOutgoingChannelInfo,
    sender_addr: Felt,
    private_key: &SecretFelt,
    index: u64,
) -> Felt {
    let mask = compute_enc_recipient_addr_hash(sender_addr, private_key, index, enc.salt);
    enc.enc_recipient_addr - mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_ephemeral_pubkey() {
        let enc = EncChannelInfo {
            ephemeral_pubkey: Felt::ZERO,
            enc_channel_key: Felt::ONE,
            enc_sender_addr: Felt::TWO,
        };
        let result = decrypt_channel_info(&enc, &SecretFelt::new(Felt::from(12345u64)));
        assert!(matches!(
            result,
            Err(DecryptionError::InvalidEphemeralPubkey)
        ));
    }

    #[test]
    fn unpacks_open_note_amount_as_plaintext() {
        let amount: u128 = 50_000_000_000_000_000_000;
        let salt = OPEN_NOTE_SALT;
        // Pack: salt in upper 128 bits, amount in lower 128 bits.
        let packed = Felt::from(salt) * Felt::from(1u128 << 64) * Felt::from(1u128 << 64)
            + Felt::from(amount);
        let (unpacked_salt, unpacked_amount) = unpack_note_amount(packed);
        assert_eq!(unpacked_salt, salt);
        assert_eq!(unpacked_amount, amount);
    }
}
