//! Storage-slot derivations for the Privacy Pool contract.
//!
//! Vendored from `crates/discovery-core/src/privacy_pool/storage_slots.rs` of
//! `starkware-libs/starknet-privacy@009a94c`. Apache-2.0.

use starknet::core::utils::get_storage_var_address;
use starknet_crypto::pedersen_hash;
use starknet_types_core::felt::Felt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncPrivateKeySlots {
    pub auditor_public_key: Felt,
    pub ephemeral_pubkey: Felt,
    pub enc_private_key: Felt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncChannelInfoSlots {
    pub ephemeral_pubkey: Felt,
    pub enc_channel_key: Felt,
    pub enc_sender_addr: Felt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncSubchannelInfoSlots {
    pub salt: Felt,
    pub enc_token: Felt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncOutgoingChannelInfoSlots {
    pub salt: Felt,
    pub enc_recipient_addr: Felt,
}

impl EncPrivateKeySlots {
    pub fn to_vec(self) -> Vec<Felt> {
        vec![
            self.auditor_public_key,
            self.ephemeral_pubkey,
            self.enc_private_key,
        ]
    }
}

impl EncChannelInfoSlots {
    pub fn to_vec(self) -> Vec<Felt> {
        vec![
            self.ephemeral_pubkey,
            self.enc_channel_key,
            self.enc_sender_addr,
        ]
    }
}

impl EncSubchannelInfoSlots {
    pub fn to_vec(self) -> Vec<Felt> {
        vec![self.salt, self.enc_token]
    }
}

impl EncOutgoingChannelInfoSlots {
    pub fn to_vec(self) -> Vec<Felt> {
        vec![self.salt, self.enc_recipient_addr]
    }
}

fn slot(name: &str, keys: &[Felt]) -> Felt {
    get_storage_var_address(name, keys).expect("storage var name exceeds 31 chars")
}

/// `auditor_public_key: PublicKey`
pub fn auditor_public_key() -> Felt {
    slot("auditor_public_key", &[])
}

/// `public_key: LegacyMap<ContractAddress, PublicKey>`
pub fn public_key(user_address: Felt) -> Felt {
    slot("public_key", &[user_address])
}

/// `enc_private_key: LegacyMap<ContractAddress, EncPrivateKey>` (3 fields).
pub fn enc_private_key(user_address: Felt) -> EncPrivateKeySlots {
    let base = slot("enc_private_key", &[user_address]);
    EncPrivateKeySlots {
        auditor_public_key: base,
        ephemeral_pubkey: base + Felt::ONE,
        enc_private_key: base + Felt::TWO,
    }
}

/// `channel_exists: LegacyMap<ChannelMarker, bool>`
pub fn channel_exists(channel_marker: Felt) -> Felt {
    slot("channel_exists", &[channel_marker])
}

/// Base address for `recipient_channels: LegacyMap<ContractAddress, Vec<EncChannelInfo>>`.
/// The base address stores the length of the Vec.
pub fn recipient_channels_base(recipient_address: Felt) -> Felt {
    slot("recipient_channels", &[recipient_address])
}

/// Slots for the i'th element of `recipient_channels[recipient_address]`. Each
/// `EncChannelInfo` occupies 3 consecutive slots starting at
/// `pedersen_hash(base, index)`.
pub fn recipient_channels_element(recipient_address: Felt, index: u64) -> EncChannelInfoSlots {
    let base = recipient_channels_base(recipient_address);
    let element_base = pedersen_hash(&base, &Felt::from(index));
    EncChannelInfoSlots {
        ephemeral_pubkey: element_base,
        enc_channel_key: element_base + Felt::ONE,
        enc_sender_addr: element_base + Felt::TWO,
    }
}

/// `subchannel_exists: LegacyMap<SubchannelMarker, bool>`
pub fn subchannel_exists(subchannel_marker: Felt) -> Felt {
    slot("subchannel_exists", &[subchannel_marker])
}

/// `subchannel_tokens: Map<felt252, EncSubchannelInfo>` (2 fields).
pub fn subchannel_tokens(subchannel_id: Felt) -> EncSubchannelInfoSlots {
    let base = slot("subchannel_tokens", &[subchannel_id]);
    EncSubchannelInfoSlots {
        salt: base,
        enc_token: base + Felt::ONE,
    }
}

/// `outgoing_channels: Map<felt252, EncOutgoingChannelInfo>` (2 fields).
pub fn outgoing_channels(outgoing_channel_id: Felt) -> EncOutgoingChannelInfoSlots {
    let base = slot("outgoing_channels", &[outgoing_channel_id]);
    EncOutgoingChannelInfoSlots {
        salt: base,
        enc_recipient_addr: base + Felt::ONE,
    }
}

/// `notes: LegacyMap<NoteId, bool>`
pub fn notes(note_id: Felt) -> Felt {
    slot("notes", &[note_id])
}

/// `nullifiers: LegacyMap<Nullifier, bool>`
pub fn nullifiers(nullifier: Felt) -> Felt {
    slot("nullifiers", &[nullifier])
}
