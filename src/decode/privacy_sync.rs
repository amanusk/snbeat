//! Privacy Pool note discovery + decryption for users we hold viewing keys for.
//!
//! Walks contract storage to enumerate a user's incoming notes:
//!   1. `recipient_channels[user]` length → N
//!   2. For each channel index: read `EncChannelInfo` (3 slots), ECDH-decrypt
//!      with the private viewing key → `(channel_key, sender_addr)`.
//!   3. For each channel: walk `subchannel_tokens[subchannel_id(channel_key, i)]`
//!      with `i = 0,1,2,…`, stopping on the first zero-salt sentinel → token list.
//!   4. For each `(channel_key, token)`: walk `notes[note_id(channel_key, token, j)]`
//!      with `j = 0,1,2,…`, stopping on the first zero. Decrypt each
//!      `packed_value` → `(amount, salt)`.
//!
//! The walk is deterministic — every slot key is computable from the user's
//! address + private viewing key. We never scan events. Result is a
//! `PrivateNotesIndex` mapping `note_id → DecryptedNote`, persisted to the
//! cache so subsequent tx opens hit the in-memory index without re-syncing.
//!
//! Storage backend prefers pf-query's batched `/storage-batch` endpoint
//! (one HTTP roundtrip per ~64 slots); falls back to parallel
//! `starknet_getStorageAt` RPC calls when pathfinder isn't configured.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use starknet::core::types::Felt;
use tracing::{debug, info, warn};

use crate::data::DataSource;
use crate::data::pathfinder::PathfinderClient;
use crate::decode::privacy::POOL_ADDRESS;
use crate::decode::privacy_crypto::decryption::{
    decrypt_channel_info, decrypt_outgoing_recipient_addr, decrypt_packed_value,
    decrypt_subchannel_token,
};
use crate::decode::privacy_crypto::hashes;
use crate::decode::privacy_crypto::storage_slots;
use crate::decode::privacy_crypto::types::{
    EncChannelInfo, EncOutgoingChannelInfo, EncSubchannelInfo, SecretFelt,
};
use crate::error::{Result, SnbeatError};
use crate::utils::felt_to_u64;

/// Cap on enumerated channels per user. 1024 is well above any realistic
/// usage; serves to prevent runaway loops if a sentinel logic bug ever
/// slips in. Bump if production users start hitting it.
const MAX_CHANNELS: u64 = 1024;

/// Cap on subchannels per channel. Same rationale as `MAX_CHANNELS`.
const MAX_SUBCHANNELS: u64 = 1024;

/// Cap on notes per subchannel. Linear walk from index 0 is the v1
/// strategy; users with hundreds of notes per subchannel will need the
/// upstream's exponential probe + bisect. Revisit if perf bites.
const MAX_NOTES_PER_SUBCHANNEL: u64 = 1024;

/// Slots probed per batch request when walking subchannels / notes. Each
/// subchannel is 2 slots, each note is 1 slot, so a 64-slot batch
/// probes 32 subchannels or 64 notes per pf-query roundtrip.
const PROBE_BATCH: usize = 64;

/// Concurrency cap for the RPC fallback. 8 in-flight `getStorageAt`
/// requests is well below typical RPC quota and keeps the sync
/// progressing without spamming a single endpoint.
const RPC_CONCURRENCY: usize = 8;

/// Whether a note was discovered via the user's incoming or outgoing
/// channel tree. Drives both UI direction (sender→user vs. user→recipient)
/// and whether we can compute a spend nullifier (incoming only — outgoing
/// notes are spent by the recipient with the recipient's key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteDirection {
    /// User is the recipient. Found via `recipient_channels[user]`.
    Incoming,
    /// User is the sender. Found via `outgoing_channels` walk.
    Outgoing,
}

/// One forward-decrypted note belonging to a user we hold a viewing key for.
#[derive(Debug, Clone)]
pub struct DecryptedNote {
    pub note_id: Felt,
    /// The user we synced for.
    pub user: Felt,
    /// Sender for incoming notes, recipient for outgoing notes.
    pub counterparty: Felt,
    pub direction: NoteDirection,
    pub token: Felt,
    pub amount: u128,
    /// Position within the user's discovery tree. Useful for cache
    /// dedup, debugging, and future incremental-sync bookkeeping.
    pub channel_idx: u64,
    pub subchannel_idx: u64,
    pub note_idx: u64,
    /// True iff the note's nullifier is set on-chain (= already spent).
    /// For Incoming notes: read from `nullifiers[nullifier]` during
    /// sync. For Outgoing notes: always `false` — we can't derive the
    /// nullifier without the recipient's private key, so we have no
    /// authoritative way to know if it's been spent.
    pub spent: bool,
    /// Block we synced at (provenance — lets a future reorg detector
    /// invalidate notes that came from a block that got rolled back).
    pub block_number: u64,
}

/// Lookup-by-note_id index that the Privacy tab consumes when annotating
/// `EncNoteCreated` events.
#[derive(Debug, Clone, Default)]
pub struct PrivateNotesIndex {
    pub notes: HashMap<Felt, DecryptedNote>,
    /// Spend-nullifier → note_id, populated for incoming notes only.
    /// Lets `NoteUsed` events in a tx be labelled "user spent note X".
    pub by_nullifier: HashMap<Felt, Felt>,
}

impl PrivateNotesIndex {
    pub fn get(&self, note_id: &Felt) -> Option<&DecryptedNote> {
        self.notes.get(note_id)
    }

    pub fn note_for_nullifier(&self, nullifier: &Felt) -> Option<&DecryptedNote> {
        self.by_nullifier
            .get(nullifier)
            .and_then(|nid| self.notes.get(nid))
    }

    pub fn len(&self) -> usize {
        self.notes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.notes.is_empty()
    }
}

/// Storage backend used by `sync_user_notes`. Encapsulates
/// pf-query batch reads with RPC fallback so the sync logic stays
/// transport-agnostic.
pub struct StorageBackend {
    pathfinder: Option<Arc<PathfinderClient>>,
    data_source: Arc<dyn DataSource>,
}

impl StorageBackend {
    pub fn new(
        pathfinder: Option<Arc<PathfinderClient>>,
        data_source: Arc<dyn DataSource>,
    ) -> Self {
        Self {
            pathfinder,
            data_source,
        }
    }

    /// Read N storage slots from the privacy pool contract at the latest
    /// block. Returns the values plus the block_number we read at, so
    /// callers can stamp the synced data with provenance.
    async fn read_slots(&self, keys: &[Felt]) -> Result<(Vec<Felt>, u64)> {
        if keys.is_empty() {
            // Still need to resolve the block; pathfinder returns it for
            // free on an empty request.
            if let Some(pf) = &self.pathfinder
                && let Ok((_, bn)) = pf.get_storage_batch(*POOL_ADDRESS, &[], "latest").await
            {
                return Ok((Vec::new(), bn));
            }
            let bn = self
                .data_source
                .get_latest_block_number()
                .await
                .map_err(|e| {
                    SnbeatError::Provider(format!("get_latest_block_number failed: {e}"))
                })?;
            return Ok((Vec::new(), bn));
        }

        // Try pathfinder batch first.
        if let Some(pf) = &self.pathfinder {
            match pf.get_storage_batch(*POOL_ADDRESS, keys, "latest").await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    warn!(error = %e, "pf-query /storage-batch failed, falling back to RPC");
                }
            }
        }

        // RPC fallback: parallel single-slot reads, capped concurrency.
        let bn = self
            .data_source
            .get_latest_block_number()
            .await
            .map_err(|e| SnbeatError::Provider(format!("get_latest_block_number failed: {e}")))?;
        let ds = Arc::clone(&self.data_source);
        let pool = *POOL_ADDRESS;
        let values: Vec<Result<Felt>> = stream::iter(keys.iter().copied())
            .map(|k| {
                let ds = Arc::clone(&ds);
                async move { ds.get_storage_at(pool, k, Some(bn)).await }
            })
            .buffered(RPC_CONCURRENCY)
            .collect()
            .await;
        let mut out = Vec::with_capacity(values.len());
        for v in values {
            out.push(v?);
        }
        Ok((out, bn))
    }
}

/// Sync all Privacy Pool notes for a user we hold a viewing key for —
/// both incoming (user as recipient) and outgoing (user as sender) — and
/// return the merged index plus the block we synced at.
///
/// Cost: incoming = `1 + 3·N + 2·K + M` slot reads (channel count +
/// channel infos + subchannel probes + note probes). Outgoing adds a
/// parallel walk: `2·O` slots to enumerate outgoing channels, `O` slots
/// for recipient public keys, plus the same subchannel + note probes
/// per non-self recipient. Self-channels are walked exactly once (via
/// the incoming side) so users with only a self-channel see no
/// duplicate work.
pub async fn sync_user_notes(
    user: Felt,
    viewing_key: &SecretFelt,
    backend: &StorageBackend,
) -> Result<(PrivateNotesIndex, u64)> {
    let mut index = PrivateNotesIndex::default();
    let bn_in = sync_incoming(user, viewing_key, backend, &mut index).await?;
    let bn_out = sync_outgoing(user, viewing_key, backend, &mut index).await?;
    Ok((index, bn_in.max(bn_out)))
}

/// Walk `recipient_channels[user]` and append every discovered note to
/// `index` with `direction = Incoming`. Also computes the spend
/// nullifier for each note and adds it to `index.by_nullifier` so
/// subsequent `NoteUsed` events in tx detail can be labelled.
async fn sync_incoming(
    user: Felt,
    viewing_key: &SecretFelt,
    backend: &StorageBackend,
    index: &mut PrivateNotesIndex,
) -> Result<u64> {
    // Step 1: read channel count.
    let count_slot = storage_slots::recipient_channels_base(user);
    let (count_values, block_number) = backend.read_slots(&[count_slot]).await?;
    let count_felt = count_values.first().copied().unwrap_or(Felt::ZERO);
    let n_channels: u64 = felt_to_u64(&count_felt);
    if n_channels == 0 {
        debug!(user = %format!("{:#x}", user), block = block_number, "User has no privacy-pool channels");
        return Ok(block_number);
    }
    let n_channels = n_channels.min(MAX_CHANNELS);

    info!(
        user = %format!("{:#x}", user),
        channels = n_channels,
        block = block_number,
        "Privacy sync: enumerating channels"
    );

    // Step 2: read all 3·N channel-info slots in one batch.
    let mut channel_keys_slots = Vec::with_capacity((n_channels * 3) as usize);
    for i in 0..n_channels {
        let s = storage_slots::recipient_channels_element(user, i);
        channel_keys_slots.push(s.ephemeral_pubkey);
        channel_keys_slots.push(s.enc_channel_key);
        channel_keys_slots.push(s.enc_sender_addr);
    }
    let (channel_values, _) = backend.read_slots(&channel_keys_slots).await?;
    if channel_values.len() != channel_keys_slots.len() {
        return Err(SnbeatError::Provider(format!(
            "Storage backend returned {} values for {} keys",
            channel_values.len(),
            channel_keys_slots.len()
        )));
    }

    // Step 3 + 4: per-channel decrypt + walk subchannels + walk notes.
    // Insertion is deferred until after a batched nullifier-presence
    // read so each note can be stamped with its on-chain `spent` state
    // in a single extra round-trip.
    struct PendingIncoming {
        note: DecryptedNote,
        nullifier: Felt,
    }
    let mut pending: Vec<PendingIncoming> = Vec::new();
    for ch_idx in 0..n_channels {
        let base = (ch_idx * 3) as usize;
        let enc = EncChannelInfo {
            ephemeral_pubkey: channel_values[base],
            enc_channel_key: channel_values[base + 1],
            enc_sender_addr: channel_values[base + 2],
        };
        let info = match decrypt_channel_info(&enc, viewing_key) {
            Ok(i) => i,
            Err(e) => {
                warn!(channel_idx = ch_idx, error = %e, "Failed to decrypt channel info, skipping");
                continue;
            }
        };

        // Walk subchannels for this channel, batched by `PROBE_BATCH`.
        let tokens = walk_subchannels(&info.channel_key, backend).await?;
        debug!(
            user = %format!("{:#x}", user),
            channel_idx = ch_idx,
            subchannels = tokens.len(),
            sender = %format!("{:#x}", info.sender_addr),
            "Channel decrypted"
        );

        // Per-subchannel: walk notes.
        for (sub_idx, token) in tokens.iter().enumerate() {
            let sub_idx = sub_idx as u64;
            let notes = walk_notes(&info.channel_key, *token, backend).await?;
            for (note_idx, (amount, _salt, packed)) in notes.into_iter().enumerate() {
                let note_idx = note_idx as u64;
                let note_id = hashes::compute_note_id(&info.channel_key, *token, note_idx);
                if packed == Felt::ZERO {
                    continue;
                }
                let nullifier =
                    hashes::compute_nullifier(&info.channel_key, *token, note_idx, viewing_key);
                pending.push(PendingIncoming {
                    note: DecryptedNote {
                        note_id,
                        user,
                        counterparty: info.sender_addr,
                        direction: NoteDirection::Incoming,
                        token: *token,
                        amount,
                        channel_idx: ch_idx,
                        subchannel_idx: sub_idx,
                        note_idx,
                        spent: false,
                        block_number,
                    },
                    nullifier,
                });
            }
        }
    }

    // One batched read of `nullifiers[nullifier]` for every incoming
    // note we just enumerated. Lets the UI distinguish live balance
    // from already-spent without scanning `NoteUsed` events.
    if !pending.is_empty() {
        let null_slots: Vec<Felt> = pending
            .iter()
            .map(|p| storage_slots::nullifiers(p.nullifier))
            .collect();
        let (null_values, _) = backend.read_slots(&null_slots).await?;
        let mut spent_count = 0usize;
        for (
            i,
            PendingIncoming {
                mut note,
                nullifier,
            },
        ) in pending.into_iter().enumerate()
        {
            let spent = null_values
                .get(i)
                .map(|v| *v != Felt::ZERO)
                .unwrap_or(false);
            note.spent = spent;
            if spent {
                spent_count += 1;
            }
            let note_id = note.note_id;
            index.notes.insert(note_id, note);
            index.by_nullifier.insert(nullifier, note_id);
        }
        debug!(
            user = %format!("{:#x}", user),
            spent = spent_count,
            unspent = index.notes.len() - spent_count,
            "Incoming spend-state stamped"
        );
    }

    info!(
        user = %format!("{:#x}", user),
        notes = index.notes.len(),
        block = block_number,
        "Privacy sync: incoming enumeration complete"
    );
    Ok(block_number)
}

/// Walk `outgoing_channels` for the user (where they were the sender).
/// For each non-self recipient, derive the channel key from `(user,
/// viewing_key, recipient, recipient_pubkey)`, then walk subchannels +
/// notes. Each discovered note is appended to `index` with `direction =
/// Outgoing` and `counterparty = recipient`. No nullifiers — outgoing
/// notes are spent by the recipient with the recipient's private key.
async fn sync_outgoing(
    user: Felt,
    viewing_key: &SecretFelt,
    backend: &StorageBackend,
    index: &mut PrivateNotesIndex,
) -> Result<u64> {
    let recipients = walk_outgoing_channels(user, viewing_key, backend).await?;
    if recipients.is_empty() {
        let (_, bn) = backend.read_slots(&[]).await?;
        debug!(user = %format!("{:#x}", user), block = bn, "User has no outgoing channels");
        return Ok(bn);
    }

    info!(
        user = %format!("{:#x}", user),
        outgoing_channels = recipients.len(),
        "Privacy sync: enumerating outgoing channels"
    );

    // Read public_key[recipient] for every recipient in one batch.
    let pk_slots: Vec<Felt> = recipients
        .iter()
        .map(|(_, r)| storage_slots::public_key(*r))
        .collect();
    let (pk_values, block_number) = backend.read_slots(&pk_slots).await?;
    if pk_values.len() != pk_slots.len() {
        return Err(SnbeatError::Provider(format!(
            "Storage backend returned {} pubkey values for {} recipients",
            pk_values.len(),
            pk_slots.len()
        )));
    }

    for ((ch_idx, recipient), recipient_pubkey) in recipients.iter().zip(pk_values.iter()) {
        // Self-channels are already covered by `sync_incoming` — both
        // walks would derive the same channel_key + note_ids.
        if *recipient == user {
            continue;
        }
        if *recipient_pubkey == Felt::ZERO {
            warn!(
                recipient = %format!("{:#x}", recipient),
                "Outgoing recipient has no on-chain public_key, skipping channel"
            );
            continue;
        }
        let channel_key =
            hashes::compute_channel_key(user, viewing_key, *recipient, *recipient_pubkey);
        let tokens = walk_subchannels(&channel_key, backend).await?;
        debug!(
            user = %format!("{:#x}", user),
            channel_idx = ch_idx,
            recipient = %format!("{:#x}", recipient),
            subchannels = tokens.len(),
            "Outgoing channel decrypted"
        );
        for (sub_idx, token) in tokens.iter().enumerate() {
            let sub_idx = sub_idx as u64;
            let notes = walk_notes(&channel_key, *token, backend).await?;
            for (note_idx, (amount, _salt, packed)) in notes.into_iter().enumerate() {
                let note_idx = note_idx as u64;
                if packed == Felt::ZERO {
                    continue;
                }
                let note_id = hashes::compute_note_id(&channel_key, *token, note_idx);
                index.notes.insert(
                    note_id,
                    DecryptedNote {
                        note_id,
                        user,
                        counterparty: *recipient,
                        direction: NoteDirection::Outgoing,
                        token: *token,
                        amount,
                        channel_idx: *ch_idx,
                        subchannel_idx: sub_idx,
                        note_idx,
                        // Outgoing nullifiers require the recipient's
                        // private key to compute, so we leave this as
                        // `false` — surface only what we can
                        // authoritatively prove.
                        spent: false,
                        block_number,
                    },
                );
            }
        }
    }
    Ok(block_number)
}

/// Walk `outgoing_channels[outgoing_channel_id(user, viewing_key, i)]`
/// for `i=0,1,…` in `PROBE_BATCH`-sized rounds, stopping on the first
/// zero-salt sentinel. Returns `(channel_idx, recipient_addr)` pairs.
async fn walk_outgoing_channels(
    user: Felt,
    viewing_key: &SecretFelt,
    backend: &StorageBackend,
) -> Result<Vec<(u64, Felt)>> {
    let mut out = Vec::new();
    let mut next_idx: u64 = 0;
    while next_idx < MAX_CHANNELS {
        let probe_count = (MAX_CHANNELS - next_idx).min(PROBE_BATCH as u64 / 2) as usize;
        let mut keys = Vec::with_capacity(probe_count * 2);
        for off in 0..probe_count {
            let id = hashes::compute_outgoing_channel_id(user, viewing_key, next_idx + off as u64);
            let s = storage_slots::outgoing_channels(id);
            keys.push(s.salt);
            keys.push(s.enc_recipient_addr);
        }
        let (values, _) = backend.read_slots(&keys).await?;
        let mut hit_sentinel = false;
        for off in 0..probe_count {
            let salt = values[off * 2];
            let enc_recipient_addr = values[off * 2 + 1];
            if salt == Felt::ZERO {
                hit_sentinel = true;
                break;
            }
            let enc = EncOutgoingChannelInfo {
                salt,
                enc_recipient_addr,
            };
            let recipient =
                decrypt_outgoing_recipient_addr(&enc, user, viewing_key, next_idx + off as u64);
            out.push((next_idx + off as u64, recipient));
        }
        if hit_sentinel {
            break;
        }
        next_idx += probe_count as u64;
    }
    Ok(out)
}

/// Walk `subchannel_tokens[subchannel_id(channel_key, i)]` for `i=0,1,…`
/// in `PROBE_BATCH`-sized rounds, stopping on the first zero-salt
/// sentinel. Returns the decrypted token list in subchannel order.
async fn walk_subchannels(channel_key: &SecretFelt, backend: &StorageBackend) -> Result<Vec<Felt>> {
    let mut tokens = Vec::new();
    let mut next_idx: u64 = 0;
    while next_idx < MAX_SUBCHANNELS {
        let probe_count = (MAX_SUBCHANNELS - next_idx).min(PROBE_BATCH as u64 / 2) as usize;
        let mut keys = Vec::with_capacity(probe_count * 2);
        for off in 0..probe_count {
            let id = crate::decode::privacy_crypto::hashes::compute_subchannel_id(
                channel_key,
                next_idx + off as u64,
            );
            let s = storage_slots::subchannel_tokens(id);
            keys.push(s.salt);
            keys.push(s.enc_token);
        }
        let (values, _) = backend.read_slots(&keys).await?;
        let mut hit_sentinel = false;
        for off in 0..probe_count {
            let salt = values[off * 2];
            let enc_token = values[off * 2 + 1];
            if salt == Felt::ZERO {
                hit_sentinel = true;
                break;
            }
            let enc = EncSubchannelInfo { salt, enc_token };
            let token = decrypt_subchannel_token(&enc, channel_key, next_idx + off as u64);
            tokens.push(token);
        }
        if hit_sentinel {
            break;
        }
        next_idx += probe_count as u64;
    }
    Ok(tokens)
}

/// Walk `notes[note_id(channel_key, token, j)]` for `j=0,1,…` in
/// `PROBE_BATCH`-sized rounds, stopping on the first zero packed_value
/// (note doesn't exist). Returns `(amount, salt, packed_value)` per
/// existing note in order.
async fn walk_notes(
    channel_key: &SecretFelt,
    token: Felt,
    backend: &StorageBackend,
) -> Result<Vec<(u128, u128, Felt)>> {
    let mut notes = Vec::new();
    let mut next_idx: u64 = 0;
    while next_idx < MAX_NOTES_PER_SUBCHANNEL {
        let probe_count = (MAX_NOTES_PER_SUBCHANNEL - next_idx).min(PROBE_BATCH as u64) as usize;
        let mut keys = Vec::with_capacity(probe_count);
        for off in 0..probe_count {
            let nid = crate::decode::privacy_crypto::hashes::compute_note_id(
                channel_key,
                token,
                next_idx + off as u64,
            );
            keys.push(storage_slots::notes(nid));
        }
        let (values, _) = backend.read_slots(&keys).await?;
        let mut hit_sentinel = false;
        for (off, packed) in values.iter().take(probe_count).enumerate() {
            let packed = *packed;
            if packed == Felt::ZERO {
                hit_sentinel = true;
                break;
            }
            let (amount, salt) =
                decrypt_packed_value(packed, channel_key, token, next_idx + off as u64);
            notes.push((amount, salt, packed));
        }
        if hit_sentinel {
            break;
        }
        next_idx += probe_count as u64;
    }
    Ok(notes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_index_lookup() {
        let idx = PrivateNotesIndex::default();
        assert!(idx.is_empty());
        assert!(idx.get(&Felt::from(1u64)).is_none());
    }

    /// Pins down the exact lookup pattern the Privacy tab does at render
    /// time: `summary.enc_notes_created.iter().filter_map(|nid|
    /// private_notes.get(nid))`. Felt equality + Hash must agree across
    /// the sync-side `compute_note_id` output and the event-side note_id
    /// parsed from receipts. Sanity check that ensures a regression in
    /// HashMap behavior over `Felt` doesn't silently break the UI.
    #[test]
    fn ui_lookup_matches_round_trip() {
        let note_id = Felt::from(0xc0ffeeu64);
        let user = Felt::from(0xabcdu64);
        let token = Felt::from(0x1234u64);
        let counterparty = Felt::from(0xdeadu64);

        let mut map: HashMap<Felt, DecryptedNote> = HashMap::new();
        map.insert(
            note_id,
            DecryptedNote {
                note_id,
                user,
                counterparty,
                direction: NoteDirection::Incoming,
                token,
                amount: 140_000_000_000_000_000_000u128,
                channel_idx: 0,
                subchannel_idx: 0,
                note_idx: 3,
                spent: false,
                block_number: 9579062,
            },
        );

        // Construct an "event side" Felt from a different code path than
        // the sync-side one, just to be thorough.
        let from_event = Felt::from_hex_unchecked("0xc0ffee");
        let enc_notes_created = [from_event];
        let hits: Vec<&DecryptedNote> = enc_notes_created
            .iter()
            .filter_map(|nid| map.get(nid))
            .collect();
        assert_eq!(hits.len(), 1, "lookup should hit exactly once");
        assert_eq!(hits[0].amount, 140_000_000_000_000_000_000u128);
    }
}
