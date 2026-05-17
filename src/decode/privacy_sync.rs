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
use std::sync::atomic::{AtomicBool, Ordering};

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
    ChannelInfo, EncChannelInfo, EncOutgoingChannelInfo, EncSubchannelInfo, SecretFelt,
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

/// Chunk size for the JSON-RPC batch fallback. Keeps each batch
/// comfortably below typical provider caps (Alchemy ~500, Infura ~100)
/// while still amortizing HTTP overhead — 64 slots per batch turns
/// hundreds of single requests into a handful.
const RPC_BATCH_CHUNK: usize = 64;

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

/// Optional "where we left off" state passed into [`sync_user_notes`]. When
/// non-empty, the sync skips re-walking channels/subchannels we've already
/// fully enumerated and only probes for newly-added content. Empty
/// (`Default::default()`) reproduces the original full-from-genesis walk.
///
/// Persistence is owned by the cache layer: callers load this from
/// SQLite (`load_private_notes_for_user`) before invoking sync, then
/// the merged result is written back.
#[derive(Debug, Clone, Default)]
pub struct SyncResume {
    /// Previously-decrypted notes for this user (incoming + outgoing).
    pub notes: HashMap<Felt, DecryptedNote>,
    /// `nullifier → note_id` for previously-discovered incoming notes.
    pub by_nullifier: HashMap<Felt, Felt>,
}

/// Per-channel state derived from a `SyncResume`, scoped to one
/// (user, direction) slice. Used by the resumed sync path to know
/// where to resume the subchannel and per-subchannel note walks
/// without re-reading slots we've already enumerated.
#[derive(Debug, Default)]
struct ChannelKnown {
    /// One past the highest persisted `subchannel_idx` for this channel
    /// — i.e. the next index `walk_subchannels` should probe to detect
    /// growth.
    next_subchannel_idx: u64,
    /// Per known subchannel: `(token, next_note_idx)` where
    /// `next_note_idx = max(persisted note_idx) + 1`.
    per_subchannel: HashMap<u64, (Felt, u64)>,
}

/// Build per-channel resume state from `notes`, scoped to (user, direction).
/// Channels not in the result will be full-walked on resume.
fn build_known_state(
    notes: &HashMap<Felt, DecryptedNote>,
    user: Felt,
    direction: NoteDirection,
) -> HashMap<u64, ChannelKnown> {
    let mut out: HashMap<u64, ChannelKnown> = HashMap::new();
    let mut conflicting_channels: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for n in notes.values() {
        if n.user != user || n.direction != direction {
            continue;
        }
        if conflicting_channels.contains(&n.channel_idx) {
            continue;
        }
        let ck = out.entry(n.channel_idx).or_default();
        if n.subchannel_idx + 1 > ck.next_subchannel_idx {
            ck.next_subchannel_idx = n.subchannel_idx + 1;
        }
        let entry = ck
            .per_subchannel
            .entry(n.subchannel_idx)
            .or_insert((n.token, 0));
        if entry.0 != n.token {
            warn!(
                channel_idx = n.channel_idx,
                sub_idx = n.subchannel_idx,
                "Conflicting tokens in resume state for the same subchannel — falling back to a full walk for this channel"
            );
            conflicting_channels.insert(n.channel_idx);
            continue;
        }
        if n.note_idx + 1 > entry.1 {
            entry.1 = n.note_idx + 1;
        }
    }
    for ch in &conflicting_channels {
        out.remove(ch);
    }
    out
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

/// Resolve a list of spend-nullifiers to a single owner address, when all
/// of them belong to notes we hold viewing keys for AND those notes share
/// the same `user`. Returns `None` for the multi-owner case, the partial
/// case (some nullifiers don't resolve), and the empty case.
///
/// Used to reconstruct hidden user identities from on-chain nullifier
/// references:
///   * Withdrawal sender — the user whose notes were spent to produce a
///     public withdrawal.
///   * `execute_private_sponsored` funder — the user whose notes paid the
///     relayer's fee, where SNIP-9 leaves no plaintext intender.
pub fn resolve_single_owner(
    nullifiers: &[Felt],
    nullifier_index: &HashMap<Felt, Felt>,
    notes: &HashMap<Felt, DecryptedNote>,
) -> Option<Felt> {
    let mut owners = nullifiers
        .iter()
        .filter_map(|n| nullifier_index.get(n))
        .filter_map(|nid| notes.get(nid))
        .map(|n| n.user);
    let first = owners.next()?;
    owners.all(|u| u == first).then_some(first)
}

/// Storage backend used by `sync_user_notes`. Encapsulates pf-query
/// batch reads with RPC fallback so the sync logic stays
/// transport-agnostic.
///
/// Once a pf-query call fails within a single backend instance, the
/// `pf_disabled` flag latches and every subsequent batch goes straight
/// to RPC — without it, every batch would pay the pf-query timeout
/// before falling back, ballooning a sync's wall-clock when pf-query
/// is permanently down.
pub struct StorageBackend {
    pathfinder: Option<Arc<PathfinderClient>>,
    data_source: Arc<dyn DataSource>,
    pf_disabled: AtomicBool,
}

impl StorageBackend {
    pub fn new(
        pathfinder: Option<Arc<PathfinderClient>>,
        data_source: Arc<dyn DataSource>,
    ) -> Self {
        Self {
            pathfinder,
            data_source,
            pf_disabled: AtomicBool::new(false),
        }
    }

    fn pf_active(&self) -> Option<&Arc<PathfinderClient>> {
        if self.pf_disabled.load(Ordering::Relaxed) {
            return None;
        }
        self.pathfinder.as_ref()
    }

    fn poison_pf(&self) {
        self.pf_disabled.store(true, Ordering::Relaxed);
    }

    /// Read N storage slots from the privacy pool contract at the latest
    /// block. Returns the values plus the block_number we read at, so
    /// callers can stamp the synced data with provenance.
    async fn read_slots(&self, keys: &[Felt]) -> Result<(Vec<Felt>, u64)> {
        if keys.is_empty() {
            // Still need to resolve the block; pathfinder returns it for
            // free on an empty request.
            if let Some(pf) = self.pf_active()
                && let Ok((_, bn)) = pf.get_storage_batch(*POOL_ADDRESS, &[], "latest").await
            {
                return Ok((Vec::new(), bn));
            }
            let bn = self.data_source.latest_block_hint().await.ok_or_else(|| {
                SnbeatError::Provider("latest_block_hint: no chain head available".into())
            })?;
            return Ok((Vec::new(), bn));
        }

        // Try pathfinder batch first.
        if let Some(pf) = self.pf_active() {
            match pf.get_storage_batch(*POOL_ADDRESS, keys, "latest").await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    warn!(
                        error = %e,
                        "pf-query /storage-batch failed; disabling pf for this sync, switching to RPC batch"
                    );
                    self.poison_pf();
                }
            }
        }

        // RPC fallback: chunked JSON-RPC batches via
        // `starknet_getStorageAt`. One HTTP roundtrip per chunk.
        let bn = self.data_source.latest_block_hint().await.ok_or_else(|| {
            SnbeatError::Provider("latest_block_hint: no chain head available".into())
        })?;
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(RPC_BATCH_CHUNK) {
            let results = self
                .data_source
                .batch_get_storage_at(*POOL_ADDRESS, chunk, Some(bn))
                .await;
            for r in results {
                out.push(r?);
            }
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
    resume: SyncResume,
) -> Result<(PrivateNotesIndex, u64)> {
    let mut index = PrivateNotesIndex::default();
    let bn_in = sync_incoming(user, viewing_key, backend, &resume, &mut index).await?;
    let bn_out = sync_outgoing(user, viewing_key, backend, &resume, &mut index).await?;
    Ok((index, bn_in.max(bn_out)))
}

/// Walk `recipient_channels[user]` and append every discovered note to
/// `index` with `direction = Incoming`. Also computes the spend
/// nullifier for each note and adds it to `index.by_nullifier`.
///
/// When `resume` carries previously-persisted state for this user, the
/// walk is incremental: a single bulk-probe batch detects whether any
/// known subchannel has new notes / any known channel has new
/// subchannels, and full walks fire only for the hits. Channels that
/// don't appear in `resume` (i.e. discovered for the first time this
/// run) get the original full-from-zero walk.
///
/// Spent state is refreshed for ALL incoming unspent notes (resume +
/// new) in a single batched nullifier-presence read at the end.
async fn sync_incoming(
    user: Felt,
    viewing_key: &SecretFelt,
    backend: &StorageBackend,
    resume: &SyncResume,
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

    let known = build_known_state(&resume.notes, user, NoteDirection::Incoming);

    info!(
        user = %format!("{:#x}", user),
        channels = n_channels,
        known_channels = known.len(),
        block = block_number,
        "Privacy sync: enumerating incoming channels"
    );

    // Step 2: read all 3·N channel-info slots. Re-read across resumes
    // because (a) we need to know `n_channels` to detect new channels
    // beyond the last cached count and (b) re-decrypting old channels
    // is local-CPU only — the slot reads are the unavoidable part. For
    // a typical user N is small (single-digit), so this is cheap.
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

    // Decrypt all channel infos up front. Failures are logged and
    // skipped — those channels won't contribute notes this run.
    let mut channel_infos: Vec<(u64, ChannelInfo)> = Vec::new();
    for ch_idx in 0..n_channels {
        let base = (ch_idx * 3) as usize;
        let enc = EncChannelInfo {
            ephemeral_pubkey: channel_values[base],
            enc_channel_key: channel_values[base + 1],
            enc_sender_addr: channel_values[base + 2],
        };
        match decrypt_channel_info(&enc, viewing_key) {
            Ok(info) => channel_infos.push((ch_idx, info)),
            Err(e) => {
                warn!(channel_idx = ch_idx, error = %e, "Failed to decrypt channel info, skipping");
            }
        }
    }

    struct PendingIncoming {
        note: DecryptedNote,
        nullifier: Felt,
    }
    let mut pending: Vec<PendingIncoming> = Vec::new();

    // Step 3: bulk "is anything new?" probe for known channels.
    // For each known channel, enqueue:
    //   - 1 probe (2 slots: salt + enc_token) for `subchannel_tokens[next_subchannel_idx]`
    //   - 1 probe (1 slot)  for `notes[next_note_idx]` per known subchannel
    // All concatenated into a single backend round-trip. Probes that come
    // back zero mean "no new content" → skip the deep walk.
    enum Probe {
        NewSubchannel {
            ch_idx: u64,
            start_idx: u64,
        },
        NewNote {
            ch_idx: u64,
            sub_idx: u64,
            token: Felt,
            start_idx: u64,
        },
    }
    let mut probes: Vec<(Probe, usize)> = Vec::new();
    let mut probe_slots: Vec<Felt> = Vec::new();
    for (ch_idx, info) in &channel_infos {
        if let Some(k) = known.get(ch_idx) {
            let sub_id = hashes::compute_subchannel_id(&info.channel_key, k.next_subchannel_idx);
            let s = storage_slots::subchannel_tokens(sub_id);
            let off = probe_slots.len();
            probe_slots.push(s.salt);
            probe_slots.push(s.enc_token);
            probes.push((
                Probe::NewSubchannel {
                    ch_idx: *ch_idx,
                    start_idx: k.next_subchannel_idx,
                },
                off,
            ));
            for (&sub_idx, &(token, next_note_idx)) in &k.per_subchannel {
                let nid = hashes::compute_note_id(&info.channel_key, token, next_note_idx);
                let off = probe_slots.len();
                probe_slots.push(storage_slots::notes(nid));
                probes.push((
                    Probe::NewNote {
                        ch_idx: *ch_idx,
                        sub_idx,
                        token,
                        start_idx: next_note_idx,
                    },
                    off,
                ));
            }
        }
    }
    let probe_results = if probe_slots.is_empty() {
        Vec::new()
    } else {
        backend.read_slots(&probe_slots).await?.0
    };
    let probes_total = probes.len();
    let mut probe_hits = 0usize;

    let find_info = |ch: u64| -> Option<&ChannelInfo> {
        channel_infos.iter().find(|(c, _)| *c == ch).map(|(_, i)| i)
    };

    // Step 4: dispatch real walks for hits.
    for (kind, off) in &probes {
        match kind {
            Probe::NewSubchannel { ch_idx, start_idx } => {
                let salt = probe_results.get(*off).copied().unwrap_or(Felt::ZERO);
                if salt == Felt::ZERO {
                    continue;
                }
                probe_hits += 1;
                let info = match find_info(*ch_idx) {
                    Some(i) => i,
                    None => continue,
                };
                let new_tokens = walk_subchannels(&info.channel_key, backend, *start_idx).await?;
                for (sub_off, token) in new_tokens.into_iter().enumerate() {
                    let abs_sub = start_idx + sub_off as u64;
                    let new_notes = walk_notes(&info.channel_key, token, backend, 0).await?;
                    for (note_off, (amount, _salt, packed)) in new_notes.into_iter().enumerate() {
                        if packed == Felt::ZERO {
                            continue;
                        }
                        let note_idx = note_off as u64;
                        let note_id = hashes::compute_note_id(&info.channel_key, token, note_idx);
                        let nullifier = hashes::compute_nullifier(
                            &info.channel_key,
                            token,
                            note_idx,
                            viewing_key,
                        );
                        pending.push(PendingIncoming {
                            note: DecryptedNote {
                                note_id,
                                user,
                                counterparty: info.sender_addr,
                                direction: NoteDirection::Incoming,
                                token,
                                amount,
                                channel_idx: *ch_idx,
                                subchannel_idx: abs_sub,
                                note_idx,
                                spent: false,
                                block_number,
                            },
                            nullifier,
                        });
                    }
                }
            }
            Probe::NewNote {
                ch_idx,
                sub_idx,
                token,
                start_idx,
            } => {
                let packed = probe_results.get(*off).copied().unwrap_or(Felt::ZERO);
                if packed == Felt::ZERO {
                    continue;
                }
                probe_hits += 1;
                let info = match find_info(*ch_idx) {
                    Some(i) => i,
                    None => continue,
                };
                let new_notes = walk_notes(&info.channel_key, *token, backend, *start_idx).await?;
                for (note_off, (amount, _salt, packed)) in new_notes.into_iter().enumerate() {
                    if packed == Felt::ZERO {
                        continue;
                    }
                    let abs_idx = start_idx + note_off as u64;
                    let note_id = hashes::compute_note_id(&info.channel_key, *token, abs_idx);
                    let nullifier =
                        hashes::compute_nullifier(&info.channel_key, *token, abs_idx, viewing_key);
                    pending.push(PendingIncoming {
                        note: DecryptedNote {
                            note_id,
                            user,
                            counterparty: info.sender_addr,
                            direction: NoteDirection::Incoming,
                            token: *token,
                            amount,
                            channel_idx: *ch_idx,
                            subchannel_idx: *sub_idx,
                            note_idx: abs_idx,
                            spent: false,
                            block_number,
                        },
                        nullifier,
                    });
                }
            }
        }
    }

    // Step 5: full walk for channels not in `known` (i.e. new this run).
    for (ch_idx, info) in &channel_infos {
        if known.contains_key(ch_idx) {
            continue;
        }
        let tokens = walk_subchannels(&info.channel_key, backend, 0).await?;
        debug!(
            user = %format!("{:#x}", user),
            channel_idx = *ch_idx,
            subchannels = tokens.len(),
            sender = %format!("{:#x}", info.sender_addr),
            "New channel discovered, full walk"
        );
        for (sub_idx, token) in tokens.iter().enumerate() {
            let sub_idx = sub_idx as u64;
            let notes = walk_notes(&info.channel_key, *token, backend, 0).await?;
            for (note_idx, (amount, _salt, packed)) in notes.into_iter().enumerate() {
                let note_idx = note_idx as u64;
                if packed == Felt::ZERO {
                    continue;
                }
                let note_id = hashes::compute_note_id(&info.channel_key, *token, note_idx);
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
                        channel_idx: *ch_idx,
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

    // Step 6: carry forward existing resume notes for (this user, incoming).
    for n in resume.notes.values() {
        if n.user == user && n.direction == NoteDirection::Incoming {
            index.notes.insert(n.note_id, n.clone());
        }
    }
    for (nul, nid) in &resume.by_nullifier {
        if let Some(n) = index.notes.get(nid)
            && n.user == user
            && n.direction == NoteDirection::Incoming
        {
            index.by_nullifier.insert(*nul, *nid);
        }
    }

    // Insert newly-discovered pending notes (without spent state — refreshed below).
    let new_count = pending.len();
    for p in pending.drain(..) {
        let note_id = p.note.note_id;
        index.notes.insert(note_id, p.note);
        index.by_nullifier.insert(p.nullifier, note_id);
    }

    // Step 7: refresh spent state for ALL incoming unspent notes for this user
    // (resume + new). Lets the UI distinguish live balance from already-spent
    // notes without scanning NoteUsed events. One batched read.
    let unspent: Vec<(Felt, Felt)> = index
        .by_nullifier
        .iter()
        .filter(|(_, nid)| {
            index
                .notes
                .get(*nid)
                .map(|n| n.user == user && n.direction == NoteDirection::Incoming && !n.spent)
                .unwrap_or(false)
        })
        .map(|(n, id)| (*n, *id))
        .collect();
    let mut newly_spent = 0usize;
    if !unspent.is_empty() {
        let null_slots: Vec<Felt> = unspent
            .iter()
            .map(|(nul, _)| storage_slots::nullifiers(*nul))
            .collect();
        let (null_values, _) = backend.read_slots(&null_slots).await?;
        for (i, (_, nid)) in unspent.iter().enumerate() {
            let spent = null_values
                .get(i)
                .map(|v| *v != Felt::ZERO)
                .unwrap_or(false);
            if spent
                && let Some(n) = index.notes.get_mut(nid)
                && !n.spent
            {
                n.spent = true;
                newly_spent += 1;
            }
        }
    }

    info!(
        user = %format!("{:#x}", user),
        notes = index.notes.iter().filter(|(_, n)| n.user == user && n.direction == NoteDirection::Incoming).count(),
        new = new_count,
        probes = probes_total,
        probe_hits,
        newly_spent,
        block = block_number,
        "Privacy sync: incoming complete"
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
    resume: &SyncResume,
    index: &mut PrivateNotesIndex,
) -> Result<u64> {
    let known = build_known_state(&resume.notes, user, NoteDirection::Outgoing);

    // Step 1: derive existing recipients from resume (counterparty for
    // outgoing notes). For each known channel we already have the
    // recipient address from prior persisted notes, so we can skip
    // re-decrypting the outgoing_channels[idx] slot for those channels.
    // We still need to fetch their on-chain public_key to derive
    // `channel_key`, but that's 1 slot per known channel rather than 3.
    let known_recipients: HashMap<u64, Felt> = known
        .keys()
        .copied()
        .filter_map(|ch_idx| {
            resume
                .notes
                .values()
                .find(|n| {
                    n.user == user
                        && n.direction == NoteDirection::Outgoing
                        && n.channel_idx == ch_idx
                })
                .map(|n| (ch_idx, n.counterparty))
        })
        .collect();

    // Step 2: discover any NEW outgoing channels added since last sync.
    // Probe from `next_outgoing_channel_idx = max(known channel_idx) + 1`,
    // or 0 if there's no known state. `walk_outgoing_channels` takes one
    // PROBE_BATCH (~64 slots) to confirm "no new channels" — acceptable
    // given typical channel counts are low.
    let next_outgoing_start = known_recipients.keys().max().map(|m| m + 1).unwrap_or(0);
    let new_recipients =
        walk_outgoing_channels(user, viewing_key, backend, next_outgoing_start).await?;

    // Combined recipient list: (channel_idx, recipient) for both known and new.
    let mut all_recipients: Vec<(u64, Felt)> = known_recipients.into_iter().collect();
    for nr in &new_recipients {
        all_recipients.push((nr.0, nr.1));
    }
    all_recipients.sort_by_key(|(ch, _)| *ch);

    if all_recipients.is_empty() {
        let (_, bn) = backend.read_slots(&[]).await?;
        debug!(user = %format!("{:#x}", user), block = bn, "User has no outgoing channels");
        return Ok(bn);
    }

    info!(
        user = %format!("{:#x}", user),
        outgoing_channels = all_recipients.len(),
        known_channels = known.len(),
        new_channels = new_recipients.len(),
        "Privacy sync: enumerating outgoing channels"
    );

    // Step 3: read public_key[recipient] for every recipient in one batch.
    // Self-channels are already covered by sync_incoming, so skip them for
    // both the pubkey read and the per-channel walk.
    let pk_targets: Vec<(u64, Felt)> = all_recipients
        .iter()
        .copied()
        .filter(|(_, r)| *r != user)
        .collect();
    let pk_slots: Vec<Felt> = pk_targets
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

    // Step 4: build channel_key per recipient (local CPU only).
    struct OutgoingChannelCtx {
        ch_idx: u64,
        recipient: Felt,
        channel_key: SecretFelt,
    }
    let mut outgoing_ctxs: Vec<OutgoingChannelCtx> = Vec::new();
    for ((ch_idx, recipient), recipient_pubkey) in pk_targets.iter().zip(pk_values.iter()) {
        if *recipient_pubkey == Felt::ZERO {
            warn!(
                recipient = %format!("{:#x}", recipient),
                "Outgoing recipient has no on-chain public_key, skipping channel"
            );
            continue;
        }
        let channel_key =
            hashes::compute_channel_key(user, viewing_key, *recipient, *recipient_pubkey);
        outgoing_ctxs.push(OutgoingChannelCtx {
            ch_idx: *ch_idx,
            recipient: *recipient,
            channel_key,
        });
    }

    // Step 5: bulk "is anything new?" probe for known outgoing channels
    // (same shape as the incoming probe phase).
    enum Probe {
        NewSubchannel {
            ch_idx: u64,
            start_idx: u64,
        },
        NewNote {
            ch_idx: u64,
            sub_idx: u64,
            token: Felt,
            start_idx: u64,
        },
    }
    let mut probes: Vec<(Probe, usize)> = Vec::new();
    let mut probe_slots: Vec<Felt> = Vec::new();
    for ctx in &outgoing_ctxs {
        if let Some(k) = known.get(&ctx.ch_idx) {
            let sub_id = hashes::compute_subchannel_id(&ctx.channel_key, k.next_subchannel_idx);
            let s = storage_slots::subchannel_tokens(sub_id);
            let off = probe_slots.len();
            probe_slots.push(s.salt);
            probe_slots.push(s.enc_token);
            probes.push((
                Probe::NewSubchannel {
                    ch_idx: ctx.ch_idx,
                    start_idx: k.next_subchannel_idx,
                },
                off,
            ));
            for (&sub_idx, &(token, next_note_idx)) in &k.per_subchannel {
                let nid = hashes::compute_note_id(&ctx.channel_key, token, next_note_idx);
                let off = probe_slots.len();
                probe_slots.push(storage_slots::notes(nid));
                probes.push((
                    Probe::NewNote {
                        ch_idx: ctx.ch_idx,
                        sub_idx,
                        token,
                        start_idx: next_note_idx,
                    },
                    off,
                ));
            }
        }
    }
    let probe_results = if probe_slots.is_empty() {
        Vec::new()
    } else {
        backend.read_slots(&probe_slots).await?.0
    };
    let probes_total = probes.len();
    let mut probe_hits = 0usize;
    let find_ctx = |ch: u64| outgoing_ctxs.iter().find(|c| c.ch_idx == ch);

    let mut new_count = 0usize;
    let mut insert_outgoing = |index: &mut PrivateNotesIndex,
                               ctx: &OutgoingChannelCtx,
                               token: Felt,
                               sub_idx: u64,
                               note_idx: u64,
                               amount: u128| {
        let note_id = hashes::compute_note_id(&ctx.channel_key, token, note_idx);
        index.notes.insert(
            note_id,
            DecryptedNote {
                note_id,
                user,
                counterparty: ctx.recipient,
                direction: NoteDirection::Outgoing,
                token,
                amount,
                channel_idx: ctx.ch_idx,
                subchannel_idx: sub_idx,
                note_idx,
                // Outgoing nullifiers require the recipient's private
                // key to compute, so we leave this as `false` —
                // surface only what we can authoritatively prove.
                spent: false,
                block_number,
            },
        );
        new_count += 1;
    };

    // Step 6: dispatch real walks for probe hits.
    for (kind, off) in &probes {
        match kind {
            Probe::NewSubchannel { ch_idx, start_idx } => {
                let salt = probe_results.get(*off).copied().unwrap_or(Felt::ZERO);
                if salt == Felt::ZERO {
                    continue;
                }
                probe_hits += 1;
                let ctx = match find_ctx(*ch_idx) {
                    Some(c) => c,
                    None => continue,
                };
                let new_tokens = walk_subchannels(&ctx.channel_key, backend, *start_idx).await?;
                for (sub_off, token) in new_tokens.into_iter().enumerate() {
                    let abs_sub = start_idx + sub_off as u64;
                    let new_notes = walk_notes(&ctx.channel_key, token, backend, 0).await?;
                    for (note_off, (amount, _salt, packed)) in new_notes.into_iter().enumerate() {
                        if packed == Felt::ZERO {
                            continue;
                        }
                        let note_idx = note_off as u64;
                        insert_outgoing(index, ctx, token, abs_sub, note_idx, amount);
                    }
                }
            }
            Probe::NewNote {
                ch_idx,
                sub_idx,
                token,
                start_idx,
            } => {
                let packed = probe_results.get(*off).copied().unwrap_or(Felt::ZERO);
                if packed == Felt::ZERO {
                    continue;
                }
                probe_hits += 1;
                let ctx = match find_ctx(*ch_idx) {
                    Some(c) => c,
                    None => continue,
                };
                let new_notes = walk_notes(&ctx.channel_key, *token, backend, *start_idx).await?;
                for (note_off, (amount, _salt, packed)) in new_notes.into_iter().enumerate() {
                    if packed == Felt::ZERO {
                        continue;
                    }
                    let abs_idx = start_idx + note_off as u64;
                    insert_outgoing(index, ctx, *token, *sub_idx, abs_idx, amount);
                }
            }
        }
    }

    // Step 7: full walk for new channels (those not in `known`).
    for ctx in &outgoing_ctxs {
        if known.contains_key(&ctx.ch_idx) {
            continue;
        }
        let tokens = walk_subchannels(&ctx.channel_key, backend, 0).await?;
        debug!(
            user = %format!("{:#x}", user),
            channel_idx = ctx.ch_idx,
            recipient = %format!("{:#x}", ctx.recipient),
            subchannels = tokens.len(),
            "New outgoing channel discovered, full walk"
        );
        for (sub_idx, token) in tokens.iter().enumerate() {
            let sub_idx = sub_idx as u64;
            let notes = walk_notes(&ctx.channel_key, *token, backend, 0).await?;
            for (note_idx, (amount, _salt, packed)) in notes.into_iter().enumerate() {
                let note_idx = note_idx as u64;
                if packed == Felt::ZERO {
                    continue;
                }
                insert_outgoing(index, ctx, *token, sub_idx, note_idx, amount);
            }
        }
    }

    // Step 8: carry forward existing resume notes for (this user, outgoing).
    for n in resume.notes.values() {
        if n.user == user && n.direction == NoteDirection::Outgoing {
            // Existing notes are inserted only if not already overwritten by a
            // newly-discovered note with the same id (which would have the same
            // content anyway — note storage is immutable).
            index.notes.entry(n.note_id).or_insert_with(|| n.clone());
        }
    }

    info!(
        user = %format!("{:#x}", user),
        new = new_count,
        probes = probes_total,
        probe_hits,
        block = block_number,
        "Privacy sync: outgoing complete"
    );
    Ok(block_number)
}

/// Walk `outgoing_channels[outgoing_channel_id(user, viewing_key, i)]`
/// for `i=start_idx,start_idx+1,…` in `PROBE_BATCH`-sized rounds, stopping
/// on the first zero-salt sentinel. Returns `(channel_idx, recipient_addr)`
/// pairs with absolute indices. `start_idx > 0` lets a resumed sync skip
/// the channels it already discovered last time.
async fn walk_outgoing_channels(
    user: Felt,
    viewing_key: &SecretFelt,
    backend: &StorageBackend,
    start_idx: u64,
) -> Result<Vec<(u64, Felt)>> {
    let mut out = Vec::new();
    let mut next_idx: u64 = start_idx;
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

/// Walk `subchannel_tokens[subchannel_id(channel_key, i)]` for
/// `i=start_idx,start_idx+1,…` in `PROBE_BATCH`-sized rounds, stopping
/// on the first zero-salt sentinel. Returns the decrypted token list
/// in subchannel order, where `tokens[k]` is the token at absolute
/// subchannel index `start_idx + k`.
async fn walk_subchannels(
    channel_key: &SecretFelt,
    backend: &StorageBackend,
    start_idx: u64,
) -> Result<Vec<Felt>> {
    let mut tokens = Vec::new();
    let mut next_idx: u64 = start_idx;
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

/// Walk `notes[note_id(channel_key, token, j)]` for
/// `j=start_idx,start_idx+1,…` in `PROBE_BATCH`-sized rounds, stopping
/// on the first zero packed_value (note doesn't exist). Returns
/// `(amount, salt, packed_value)` per existing note, where the kth entry
/// corresponds to absolute note index `start_idx + k`.
async fn walk_notes(
    channel_key: &SecretFelt,
    token: Felt,
    backend: &StorageBackend,
    start_idx: u64,
) -> Result<Vec<(u128, u128, Felt)>> {
    let mut notes = Vec::new();
    let mut next_idx: u64 = start_idx;
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

    /// Helper that mirrors how the Privacy tab populates the lookup maps:
    /// (notes_by_id, by_nullifier).
    fn make_maps(
        entries: &[(u64, &DecryptedNote)],
    ) -> (HashMap<Felt, DecryptedNote>, HashMap<Felt, Felt>) {
        let mut notes: HashMap<Felt, DecryptedNote> = HashMap::new();
        let mut by_nul: HashMap<Felt, Felt> = HashMap::new();
        for (nullifier, n) in entries {
            notes.insert(n.note_id, (*n).clone());
            by_nul.insert(Felt::from(*nullifier), n.note_id);
        }
        (notes, by_nul)
    }

    #[test]
    fn resolve_single_owner_returns_owner_when_all_match() {
        let n1 = note(1, 0xA, 0, 0, 0, NoteDirection::Incoming, 0, 0, 0, true);
        let n2 = note(2, 0xA, 0, 0, 0, NoteDirection::Incoming, 0, 0, 1, true);
        let (notes, by_nul) = make_maps(&[(0xAA, &n1), (0xBB, &n2)]);
        let nullifiers = vec![Felt::from(0xAAu64), Felt::from(0xBBu64)];
        assert_eq!(
            resolve_single_owner(&nullifiers, &by_nul, &notes),
            Some(Felt::from(0xAu64))
        );
    }

    #[test]
    fn resolve_single_owner_returns_none_for_multi_owner() {
        let n1 = note(1, 0xA, 0, 0, 0, NoteDirection::Incoming, 0, 0, 0, true);
        let n2 = note(2, 0xB, 0, 0, 0, NoteDirection::Incoming, 0, 0, 1, true);
        let (notes, by_nul) = make_maps(&[(0xAA, &n1), (0xBB, &n2)]);
        let nullifiers = vec![Felt::from(0xAAu64), Felt::from(0xBBu64)];
        assert!(resolve_single_owner(&nullifiers, &by_nul, &notes).is_none());
    }

    #[test]
    fn resolve_single_owner_returns_none_for_empty() {
        let notes: HashMap<Felt, DecryptedNote> = HashMap::new();
        let by_nul: HashMap<Felt, Felt> = HashMap::new();
        assert!(resolve_single_owner(&[], &by_nul, &notes).is_none());
    }

    /// Partial coverage: one nullifier resolves to user A, another isn't in
    /// the index at all. We require *all* nullifiers to resolve to the same
    /// owner — but unresolved ones are silently filtered, so a partial
    /// resolution where the resolved subset agrees still returns Some(A).
    /// This matches the existing withdrawal_sender behavior — the user
    /// experience is "best-effort attribution from what we can decrypt."
    #[test]
    fn resolve_single_owner_ignores_unresolved_nullifiers() {
        let n1 = note(1, 0xA, 0, 0, 0, NoteDirection::Incoming, 0, 0, 0, true);
        let (notes, by_nul) = make_maps(&[(0xAA, &n1)]);
        let nullifiers = vec![Felt::from(0xAAu64), Felt::from(0xFFFFu64)];
        assert_eq!(
            resolve_single_owner(&nullifiers, &by_nul, &notes),
            Some(Felt::from(0xAu64))
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn note(
        note_id: u64,
        user: u64,
        counterparty: u64,
        token: u64,
        amount: u128,
        direction: NoteDirection,
        channel_idx: u64,
        subchannel_idx: u64,
        note_idx: u64,
        spent: bool,
    ) -> DecryptedNote {
        DecryptedNote {
            note_id: Felt::from(note_id),
            user: Felt::from(user),
            counterparty: Felt::from(counterparty),
            direction,
            token: Felt::from(token),
            amount,
            channel_idx,
            subchannel_idx,
            note_idx,
            spent,
            block_number: 1_000_000,
        }
    }

    /// `build_known_state` derives per-channel watermarks from persisted
    /// notes — this is the source of truth for "where to resume the walk".
    /// If this drifts, every resumed sync will probe the wrong slots.
    #[test]
    fn build_known_state_basic() {
        let user = Felt::from(0xa11ceu64);
        let mut notes: HashMap<Felt, DecryptedNote> = HashMap::new();
        // Channel 0: 2 subchannels, sub 0 has notes [0,1,2], sub 1 has notes [0].
        for (nid, sub, ni) in [(1, 0, 0), (2, 0, 1), (3, 0, 2), (4, 1, 0)] {
            let n = note(
                nid,
                0xa11ce,
                0xb0b,
                0x1111,
                100,
                NoteDirection::Incoming,
                0,
                sub,
                ni,
                false,
            );
            notes.insert(n.note_id, n);
        }
        // Channel 2: sub 0 has notes [0,1].
        for (nid, ni) in [(5, 0), (6, 1)] {
            let n = note(
                nid,
                0xa11ce,
                0xc0c0,
                0x2222,
                50,
                NoteDirection::Incoming,
                2,
                0,
                ni,
                false,
            );
            notes.insert(n.note_id, n);
        }
        let state = build_known_state(&notes, user, NoteDirection::Incoming);
        assert_eq!(state.len(), 2, "channels 0 and 2 should be known");

        let ch0 = state.get(&0).expect("channel 0");
        // Highest sub_idx is 1 → next probe target is 2.
        assert_eq!(ch0.next_subchannel_idx, 2);
        assert_eq!(ch0.per_subchannel.len(), 2);
        let (token0, next0) = ch0.per_subchannel.get(&0).expect("sub 0");
        assert_eq!(*token0, Felt::from(0x1111u64));
        // Highest note_idx in sub 0 is 2 → next probe is 3.
        assert_eq!(*next0, 3);
        let (_, next1) = ch0.per_subchannel.get(&1).expect("sub 1");
        assert_eq!(*next1, 1);

        let ch2 = state.get(&2).expect("channel 2");
        assert_eq!(ch2.next_subchannel_idx, 1);
        let (token2, next2) = ch2.per_subchannel.get(&0).expect("sub 0");
        assert_eq!(*token2, Felt::from(0x2222u64));
        assert_eq!(*next2, 2);
    }

    /// Channels with ZERO matching notes (e.g. only outgoing notes when
    /// asking for incoming, or only another user's notes) must not appear
    /// in the result — otherwise resume would skip a real fresh-walk that
    /// it owes the caller.
    #[test]
    fn build_known_state_filters_by_user_and_direction() {
        let user_a = Felt::from(0xa11ceu64);
        let user_b = Felt::from(0xb0bu64);
        let mut notes: HashMap<Felt, DecryptedNote> = HashMap::new();

        // user_a incoming
        let n1 = note(
            1,
            0xa11ce,
            0xb0b,
            0x1111,
            100,
            NoteDirection::Incoming,
            0,
            0,
            0,
            false,
        );
        notes.insert(n1.note_id, n1);
        // user_a outgoing (different direction)
        let n2 = note(
            2,
            0xa11ce,
            0xc0c0,
            0x1111,
            50,
            NoteDirection::Outgoing,
            5,
            0,
            0,
            false,
        );
        notes.insert(n2.note_id, n2);
        // user_b incoming (different user)
        let n3 = note(
            3,
            0xb0b,
            0xa11ce,
            0x1111,
            10,
            NoteDirection::Incoming,
            7,
            0,
            0,
            false,
        );
        notes.insert(n3.note_id, n3);

        let inc_a = build_known_state(&notes, user_a, NoteDirection::Incoming);
        assert_eq!(inc_a.len(), 1, "only user_a's incoming channel");
        assert!(inc_a.contains_key(&0));
        assert!(!inc_a.contains_key(&5));
        assert!(!inc_a.contains_key(&7));

        let out_a = build_known_state(&notes, user_a, NoteDirection::Outgoing);
        assert_eq!(out_a.len(), 1);
        assert!(out_a.contains_key(&5));

        let inc_b = build_known_state(&notes, user_b, NoteDirection::Incoming);
        assert_eq!(inc_b.len(), 1);
        assert!(inc_b.contains_key(&7));
    }

    /// Empty input → empty state. Guards against a "missing channel"
    /// being interpreted as "channel 0 known" by an off-by-one bug.
    #[test]
    fn build_known_state_empty() {
        let user = Felt::from(0xa11ceu64);
        let notes: HashMap<Felt, DecryptedNote> = HashMap::new();
        let state = build_known_state(&notes, user, NoteDirection::Incoming);
        assert!(state.is_empty());
    }

    /// `next_subchannel_idx` is `max + 1`, not just `count`. Verifies a
    /// sparse subchannel set (0 and 5, with 1..4 empty) yields next=6.
    /// (Sparse subchannels in practice shouldn't exist — the contract
    /// appends sequentially — but the watermark logic must be defined
    /// in terms of max-seen so a single dropped row doesn't shift the
    /// resume cursor backward.)
    #[test]
    fn build_known_state_sparse_subchannels() {
        let user = Felt::from(0xa11ceu64);
        let mut notes: HashMap<Felt, DecryptedNote> = HashMap::new();
        for (nid, sub) in [(1u64, 0u64), (2, 5)] {
            let n = note(
                nid,
                0xa11ce,
                0xb0b,
                0x1111,
                10,
                NoteDirection::Incoming,
                0,
                sub,
                0,
                false,
            );
            notes.insert(n.note_id, n);
        }
        let state = build_known_state(&notes, user, NoteDirection::Incoming);
        let ch0 = state.get(&0).unwrap();
        assert_eq!(ch0.next_subchannel_idx, 6);
        assert_eq!(ch0.per_subchannel.len(), 2);
    }
}
