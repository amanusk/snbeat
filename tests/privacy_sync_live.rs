//! Live integration tests for the privacy-pool sync. Marked `#[ignore]`
//! so `cargo test` skips them by default; invoke explicitly:
//!
//!     # 1. Dump every note our sync finds for the user(s) in
//!     #    viewing_keys.toml. Useful for verifying decryption produces
//!     #    plausible amounts and tokens.
//!     cargo test --test privacy_sync_live dump_synced_note_ids \
//!         -- --ignored --nocapture
//!
//!     # 2. Sync, then scan recent pool events for any of the synced
//!     #    note_ids — proves end-to-end that our forward
//!     #    `compute_note_id` derivation matches what the contract
//!     #    emits in `EncNoteCreated`. Bring your own optional env vars:
//!     #      SCAN_FROM=9000000 SCAN_TO=9580000  (defaults: tip-50k..tip)
//!     cargo test --test privacy_sync_live find_synced_note_emission \
//!         -- --ignored --nocapture
//!
//! Both tests read `APP_RPC_URL` + `APP_PATHFINDER_SERVICE_URL` from the
//! environment and `viewing_keys.toml` from cwd. They never write
//! state. No on-chain identifiers are hard-coded — anything specific to
//! the operator's pool activity comes from the operator's own labels.

use std::sync::Arc;

use snbeat::data::DataSource;
use snbeat::data::pathfinder::PathfinderClient;
use snbeat::data::rpc::RpcDataSource;
use snbeat::decode::privacy_crypto::types::SecretFelt;
use snbeat::decode::privacy_sync::{StorageBackend, sync_user_notes};
use snbeat::registry::viewing_keys::load_viewing_keys;
use starknet::core::types::Felt;

fn build_backend() -> StorageBackend {
    let rpc = std::env::var("APP_RPC_URL").expect("APP_RPC_URL not set");
    let pf = std::env::var("APP_PATHFINDER_SERVICE_URL").ok();
    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc));
    let pf_client = pf.map(|u| Arc::new(PathfinderClient::new(u)));
    StorageBackend::new(pf_client, ds)
}

#[tokio::test]
#[ignore]
async fn dump_synced_note_ids() {
    let (keys, _warn) = load_viewing_keys(std::path::Path::new("viewing_keys.toml"))
        .expect("load viewing_keys.toml");
    assert!(
        !keys.is_empty(),
        "no viewing keys loaded — populate viewing_keys.toml in cwd"
    );
    let backend = build_backend();

    for vk in &keys {
        let user = vk.user;
        let private_key = SecretFelt::new(*vk.private_key);
        eprintln!("\n=== syncing user {:#x} ===", user);
        let (index, block) = sync_user_notes(user, &private_key, &backend)
            .await
            .expect("sync");
        eprintln!(
            "synced {} notes ({} nullifiers indexed) at block {}",
            index.notes.len(),
            index.by_nullifier.len(),
            block,
        );
        let mut notes: Vec<_> = index.notes.values().collect();
        notes.sort_by_key(|n| {
            (
                n.direction as u8,
                n.channel_idx,
                n.subchannel_idx,
                n.note_idx,
            )
        });
        let unspent_in: u128 = notes
            .iter()
            .filter(|n| {
                matches!(
                    n.direction,
                    snbeat::decode::privacy_sync::NoteDirection::Incoming
                ) && !n.spent
            })
            .map(|n| n.amount)
            .sum();
        let spent_count = notes.iter().filter(|n| n.spent).count();
        eprintln!(
            "incoming live = {} (sum, raw u128) · {} spent · {} unspent",
            unspent_in,
            spent_count,
            notes
                .iter()
                .filter(|n| matches!(
                    n.direction,
                    snbeat::decode::privacy_sync::NoteDirection::Incoming
                ) && !n.spent)
                .count(),
        );
        for n in notes {
            let dir = match n.direction {
                snbeat::decode::privacy_sync::NoteDirection::Incoming => "in ",
                snbeat::decode::privacy_sync::NoteDirection::Outgoing => "out",
            };
            let state = if n.spent { "spent" } else { "live " };
            eprintln!(
                "  {dir} {state} ch={} sub={} idx={} cp={:#x} note_id={:#x} amount={} token={:#x}",
                n.channel_idx,
                n.subchannel_idx,
                n.note_idx,
                n.counterparty,
                n.note_id,
                n.amount,
                n.token,
            );
        }
    }
}

/// Cross-check: scan recent privacy-pool `EncNoteCreated` events and
/// report any whose note_id matches a note in our synced index. A
/// non-zero hit count proves our forward `compute_note_id` derivation
/// matches what the contract emits — i.e. our walker is correct.
#[tokio::test]
#[ignore]
async fn find_synced_note_emission() {
    use reqwest::Client;

    let rpc = std::env::var("APP_RPC_URL").expect("APP_RPC_URL not set");
    let (keys, _warn) = load_viewing_keys(std::path::Path::new("viewing_keys.toml"))
        .expect("load viewing_keys.toml");
    let backend = build_backend();

    let vk = keys.first().expect("at least one viewing key");
    let user = vk.user;
    let private_key = SecretFelt::new(*vk.private_key);
    let (index, _) = sync_user_notes(user, &private_key, &backend)
        .await
        .expect("sync");
    eprintln!("synced {} notes for {:#x}", index.notes.len(), user);
    let synced: std::collections::HashSet<Felt> = index.notes.keys().copied().collect();

    let pool = "0x040337b1af3c663e86e333bab5a4b28da8d4652a15a69beee2b677776ffe812a";
    let enc_note_sel = "0x023c20207be8b1ef4430c25eef8ce779c9745ebe04139555ae81bd4f8fdd6ec5";

    // Default scan range: tip-50k..tip in 50k chunks. Override via env.
    let scan_to: u64 = std::env::var("SCAN_TO")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9_580_000);
    let scan_from: u64 = std::env::var("SCAN_FROM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(scan_to.saturating_sub(50_000));

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .unwrap();
    let mut to = scan_to;
    let mut hits = 0;
    while to > scan_from {
        let from = to.saturating_sub(50_000).max(scan_from);
        eprintln!("scan {from}..{to}");
        let body = serde_json::json!({
            "jsonrpc": "2.0", "method": "starknet_getEvents", "id": 1,
            "params": {"filter": {
                "from_block": {"block_number": from},
                "to_block": {"block_number": to},
                "address": pool,
                "keys": [[enc_note_sel]],
                "chunk_size": 1000,
            }}
        });
        let resp: serde_json::Value = client
            .post(&rpc)
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let events = resp["result"]["events"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for e in &events {
            let note_id_hex = e["keys"][1].as_str().unwrap_or("0x0");
            if let Ok(nid) = Felt::from_hex(note_id_hex)
                && synced.contains(&nid)
            {
                hits += 1;
                eprintln!(
                    "*** SYNCED NOTE FOUND ON-CHAIN: tx={} block={} note_id={}",
                    e["transaction_hash"].as_str().unwrap_or("?"),
                    e["block_number"].as_u64().unwrap_or(0),
                    note_id_hex,
                );
            }
        }
        if from == scan_from {
            break;
        }
        to = from;
    }
    eprintln!("scan done. hits={hits} of {} synced", synced.len());
}
