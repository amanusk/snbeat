use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::info;

mod bloom;
mod decode;
mod dto;

#[derive(Parser)]
#[command(name = "pf-query", about = "Pathfinder DB query service for snbeat")]
struct Config {
    /// Path to the Pathfinder SQLite database
    #[arg(long, env = "PF_DB_PATH")]
    db_path: String,

    /// Port to listen on
    #[arg(long, env = "PF_PORT", default_value = "8234")]
    port: u16,

    /// Host address to bind to
    #[arg(long, env = "PF_HOST", default_value = "127.0.0.1")]
    host: String,
}

#[derive(Clone)]
struct AppState {
    db_path: Arc<String>,
}

fn open_db(db_path: &str) -> Result<Connection, rusqlite::Error> {
    Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

/// Parse a hex Starknet address (with or without 0x prefix) into 32 big-endian bytes.
fn parse_address(addr: &str) -> Result<Vec<u8>> {
    let s = addr.trim_start_matches("0x");
    anyhow::ensure!(
        s.len() <= 64,
        "address too long: {} hex chars (max 64)",
        s.len()
    );
    let padded = format!("{:0>64}", s);
    Ok(hex::decode(&padded)?)
}

/// Decode a nonce stored in SQLite as either INTEGER (u64) or BLOB (big-endian bytes).
fn decode_nonce(row: &rusqlite::Row, col: usize) -> rusqlite::Result<u64> {
    row.get::<_, u64>(col).or_else(|_| {
        let bytes: Vec<u8> = row.get(col)?;
        let len = bytes.len();
        let mut arr = [0u8; 8];
        let src_start = len.saturating_sub(8);
        let dst_start = 8usize.saturating_sub(len - src_start);
        arr[dst_start..].copy_from_slice(&bytes[src_start..]);
        Ok(u64::from_be_bytes(arr))
    })
}

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

fn db_err(e: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::warn!(error = %e, "DB error");
    (StatusCode::INTERNAL_SERVER_ERROR, format!("DB error: {e}"))
}

// =========================================================================
// Response types
// =========================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct NonceEntry {
    pub block_number: u64,
    pub nonce: u64,
    pub timestamp: u64,
}

#[derive(Serialize)]
struct HealthResponse {
    latest_block: u64,
}

#[derive(Serialize)]
struct ClassHashEntry {
    block_number: u64,
    class_hash: String,
}

#[derive(Serialize)]
struct TxHashLookup {
    block_number: u64,
    tx_index: u64,
}

/// A contract deployed with a given class hash.
#[derive(Serialize)]
struct ContractByClassEntry {
    contract_address: String,
    block_number: u64,
}

/// Declaration info for a class hash.
#[derive(Serialize)]
struct ClassDeclarationInfo {
    block_number: u64,
}

/// Decoded transaction summary from a block blob.
#[derive(Serialize)]
struct DecodedTx {
    hash: String,
    sender: String,
    nonce: Option<u64>,
    tx_type: String,
    actual_fee: String,
    tip: u64,
    status: String,
    revert_reason: Option<String>,
}

/// Full transaction summary combining nonce_updates + blob decode.
#[derive(Serialize)]
struct SenderTxEntry {
    hash: String,
    sender_address: String,
    nonce: Option<u64>,
    block_number: u64,
    timestamp: u64,
    tx_type: String,
    actual_fee: String,
    tip: u64,
    status: String,
    revert_reason: Option<String>,
}

/// Event from a specific contract.
#[derive(Serialize)]
struct ContractEvent {
    tx_index: usize,
    event_index: usize,
    from_address: String,
    keys: Vec<String>,
    data: Vec<String>,
    block_number: u64,
    timestamp: u64,
}

// =========================================================================
// Query params
// =========================================================================

#[derive(Deserialize)]
struct NonceHistoryParams {
    limit: Option<u32>,
}

#[derive(Deserialize)]
struct SenderTxParams {
    limit: Option<u32>,
}

#[derive(Deserialize)]
struct ContractEventsParams {
    from_block: Option<u64>,
    limit: Option<u32>,
}

// =========================================================================
// Handlers
// =========================================================================

/// GET /health — latest block number
async fn handler_health(State(state): State<AppState>) -> ApiResult<HealthResponse> {
    let conn = open_db(&state.db_path).map_err(db_err)?;
    let latest_block: u64 = conn
        .query_row(
            "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .map_err(db_err)?;
    Ok(Json(HealthResponse { latest_block }))
}

/// GET /nonce-history/{address} — nonce updates with timestamps
async fn handler_nonce_history(
    Path(address): Path<String>,
    Query(params): Query<NonceHistoryParams>,
    State(state): State<AppState>,
) -> ApiResult<Vec<NonceEntry>> {
    let limit = params.limit.unwrap_or(500).min(2000);
    let addr_bytes = parse_address(&address)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid address: {e}")))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;
    let mut stmt = conn
        .prepare(
            "SELECT nu.block_number, nu.nonce, bh.timestamp \
             FROM nonce_updates nu \
             JOIN contract_addresses ca ON nu.contract_address_id = ca.id \
             JOIN block_headers bh ON nu.block_number = bh.number \
             WHERE ca.contract_address = ?1 \
             ORDER BY nu.block_number DESC LIMIT ?2",
        )
        .map_err(db_err)?;

    let entries: Vec<NonceEntry> = stmt
        .query_map(rusqlite::params![addr_bytes.as_slice(), limit], |row| {
            let block_number: u64 = row.get(0)?;
            let nonce = decode_nonce(row, 1)?;
            let timestamp: u64 = row.get(2)?;
            Ok(NonceEntry {
                block_number,
                nonce,
                timestamp,
            })
        })
        .map_err(db_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_err)?;

    Ok(Json(entries))
}

/// GET /class-history/{address} — class hash changes (deploys + upgrades)
async fn handler_class_history(
    Path(address): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Vec<ClassHashEntry>> {
    let addr_bytes = parse_address(&address)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid address: {e}")))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;
    let mut stmt = conn
        .prepare(
            "SELECT block_number, class_hash \
             FROM contract_updates \
             WHERE contract_address = ?1 \
             ORDER BY block_number DESC",
        )
        .map_err(db_err)?;

    let entries: Vec<ClassHashEntry> = stmt
        .query_map(rusqlite::params![addr_bytes.as_slice()], |row| {
            let block_number: u64 = row.get(0)?;
            let hash_blob: Vec<u8> = row.get(1)?;
            Ok(ClassHashEntry {
                block_number,
                class_hash: format!("0x{}", hex::encode(&hash_blob)),
            })
        })
        .map_err(db_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_err)?;

    Ok(Json(entries))
}

/// GET /contracts-by-class/{class_hash} — all contracts deployed with a given class hash
async fn handler_contracts_by_class(
    Path(class_hash): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Vec<ContractByClassEntry>> {
    let hash_bytes = parse_address(&class_hash)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid class hash: {e}")))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;
    let mut stmt = conn
        .prepare(
            "SELECT contract_address, block_number \
             FROM contract_updates \
             WHERE class_hash = ?1 \
             ORDER BY block_number DESC \
             LIMIT 500",
        )
        .map_err(db_err)?;

    let entries: Vec<ContractByClassEntry> = stmt
        .query_map(rusqlite::params![hash_bytes.as_slice()], |row| {
            let addr_blob: Vec<u8> = row.get(0)?;
            let block_number: u64 = row.get(1)?;
            Ok(ContractByClassEntry {
                contract_address: format!("0x{}", hex::encode(&addr_blob)),
                block_number,
            })
        })
        .map_err(db_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_err)?;

    Ok(Json(entries))
}

/// GET /class-declaration/{class_hash} — block where a class was first declared
async fn handler_class_declaration(
    Path(class_hash): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<ClassDeclarationInfo> {
    let hash_bytes = parse_address(&class_hash)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid class hash: {e}")))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;
    let result = conn
        .query_row(
            "SELECT block_number FROM class_definitions WHERE hash = ?1",
            rusqlite::params![hash_bytes.as_slice()],
            |row| {
                // block_number can be NULL (ON DELETE SET NULL in schema)
                let block_number: Option<u64> = row.get(0)?;
                Ok(block_number)
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                (StatusCode::NOT_FOUND, "Class hash not found".into())
            }
            other => db_err(other),
        })?;

    match result {
        Some(block_number) => Ok(Json(ClassDeclarationInfo { block_number })),
        None => Err((
            StatusCode::NOT_FOUND,
            "Class found but declaration block is unknown".into(),
        )),
    }
}

/// GET /tx-by-hash/{hash} — look up block_number + index for a tx hash
async fn handler_tx_by_hash(
    Path(hash): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<TxHashLookup> {
    let hash_bytes = parse_address(&hash)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid hash: {e}")))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;
    let result = conn
        .query_row(
            "SELECT block_number, idx FROM transaction_hashes WHERE hash = ?1",
            rusqlite::params![hash_bytes.as_slice()],
            |row| {
                Ok(TxHashLookup {
                    block_number: row.get(0)?,
                    tx_index: row.get(1)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                (StatusCode::NOT_FOUND, "Transaction not found".into())
            }
            other => db_err(other),
        })?;

    Ok(Json(result))
}

/// GET /block-txs/{block_number} — decode all transactions in a block
async fn handler_block_txs(
    Path(block_number): Path<u64>,
    State(state): State<AppState>,
) -> ApiResult<Vec<DecodedTx>> {
    let conn = open_db(&state.db_path).map_err(db_err)?;
    let blob: Vec<u8> = conn
        .query_row(
            "SELECT transactions FROM transactions WHERE block_number = ?1",
            rusqlite::params![block_number],
            |row| row.get(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                (StatusCode::NOT_FOUND, "Block not found".into())
            }
            other => db_err(other),
        })?;

    let txs = decode::decode_transactions(&blob).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Decode error: {e}"),
        )
    })?;

    let results: Vec<DecodedTx> = txs
        .into_iter()
        .map(|tr| {
            let (status, revert_reason) = match &tr.receipt.execution_status {
                dto::ExecutionStatus::Succeeded => ("OK".to_string(), None),
                dto::ExecutionStatus::Reverted { reason } => {
                    ("REV".to_string(), Some(reason.clone()))
                }
            };
            DecodedTx {
                hash: tr.transaction.hash.to_hex(),
                sender: tr
                    .transaction
                    .variant
                    .sender_address()
                    .map(|a| a.to_hex())
                    .unwrap_or_default(),
                nonce: tr.transaction.nonce(),
                tx_type: tr.transaction.tx_type().to_string(),
                actual_fee: tr.receipt.actual_fee.to_hex(),
                tip: tr.transaction.tip(),
                status,
                revert_reason,
            }
        })
        .collect();

    Ok(Json(results))
}

/// GET /sender-txs/{address} — full tx history for an account via nonce_updates + blob decode.
///
/// Combines nonce_updates (to find blocks) with transaction blob decoding
/// (to get hash, fee, status, type) in a single request.
async fn handler_sender_txs(
    Path(address): Path<String>,
    Query(params): Query<SenderTxParams>,
    State(state): State<AppState>,
) -> ApiResult<Vec<SenderTxEntry>> {
    let limit = params.limit.unwrap_or(500).min(2000);
    let addr_bytes = parse_address(&address)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid address: {e}")))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;

    // Step 1: Get block numbers from nonce_updates + timestamps
    let mut stmt = conn
        .prepare(
            "SELECT nu.block_number, nu.nonce, bh.timestamp \
             FROM nonce_updates nu \
             JOIN contract_addresses ca ON nu.contract_address_id = ca.id \
             JOIN block_headers bh ON nu.block_number = bh.number \
             WHERE ca.contract_address = ?1 \
             ORDER BY nu.block_number DESC LIMIT ?2",
        )
        .map_err(db_err)?;

    let nonce_entries: Vec<(u64, u64, u64)> = stmt
        .query_map(rusqlite::params![addr_bytes.as_slice(), limit], |row| {
            Ok((row.get(0)?, decode_nonce(row, 1)?, row.get(2)?))
        })
        .map_err(db_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_err)?;

    if nonce_entries.is_empty() {
        return Ok(Json(vec![]));
    }

    // Step 2: Decode transaction blobs for each block and match by sender.
    // Use a set to avoid decoding the same block twice when the sender has
    // multiple nonce updates in one block (which would cause duplicate results).
    let mut results = Vec::with_capacity(nonce_entries.len());
    let mut seen_blocks = std::collections::HashSet::new();

    // Prepare the statement once for re-use
    let mut tx_stmt = conn
        .prepare("SELECT transactions FROM transactions WHERE block_number = ?1")
        .map_err(db_err)?;

    for (block_number, expected_nonce, timestamp) in &nonce_entries {
        if !seen_blocks.insert(block_number) {
            continue; // already decoded this block
        }
        let blob: Vec<u8> =
            match tx_stmt.query_row(rusqlite::params![block_number], |row| row.get(0)) {
                Ok(b) => b,
                Err(_) => continue,
            };

        let txs = match decode::decode_transactions(&blob) {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Find the transaction from this sender in this block
        for tr in txs {
            let sender = match tr.transaction.sender_address() {
                Some(s) => s,
                None => continue,
            };

            if sender.0 != addr_bytes.as_slice() {
                continue;
            }

            let nonce = tr.transaction.nonce();

            // Match by nonce if possible (nonce_updates records the nonce AFTER the tx)
            // The tx that caused nonce N has nonce = N-1 in its body (for Invoke),
            // but nonce_updates stores the NEW nonce. So the tx nonce = expected_nonce - 1.
            // However, for deploy_account, nonce is 0 and nonce_update records 1.
            // We include all txs from this sender in this block.
            let (status, revert_reason) = match &tr.receipt.execution_status {
                dto::ExecutionStatus::Succeeded => ("OK".to_string(), None),
                dto::ExecutionStatus::Reverted { reason } => {
                    ("REV".to_string(), Some(reason.clone()))
                }
            };

            results.push(SenderTxEntry {
                hash: tr.transaction.hash.to_hex(),
                sender_address: sender.to_hex(),
                nonce,
                block_number: *block_number,
                timestamp: *timestamp,
                tx_type: tr.transaction.tx_type().to_string(),
                actual_fee: tr.receipt.actual_fee.to_hex(),
                tip: tr.transaction.tip(),
                status,
                revert_reason,
            });
        }

        // If we couldn't find any tx from this sender via blob decode,
        // still include a stub entry from nonce_updates data
        let block_has_match = results.iter().any(|r| r.block_number == *block_number);
        if !block_has_match {
            results.push(SenderTxEntry {
                hash: String::new(),
                sender_address: String::new(),
                nonce: Some(expected_nonce.saturating_sub(1)),
                block_number: *block_number,
                timestamp: *timestamp,
                tx_type: "UNKNOWN".to_string(),
                actual_fee: "0x0".to_string(),
                tip: 0,
                status: "OK".to_string(),
                revert_reason: None,
            });
        }
    }

    Ok(Json(results))
}

/// GET /contract-events/{address} — events emitted by a contract, accelerated by bloom filters.
///
/// Uses the `event_filters` bloom filter table to identify candidate blocks,
/// then decodes only those blocks' event blobs (instead of scanning every block).
async fn handler_contract_events(
    Path(address): Path<String>,
    Query(params): Query<ContractEventsParams>,
    State(state): State<AppState>,
) -> ApiResult<Vec<ContractEvent>> {
    let limit = params.limit.unwrap_or(200).min(1000) as usize;
    let addr_bytes = parse_address(&address)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid address: {e}")))?;

    let addr_array: [u8; 32] = addr_bytes
        .as_slice()
        .try_into()
        .map_err(|_| (StatusCode::BAD_REQUEST, "Address must be 32 bytes".into()))?;

    let conn = open_db(&state.db_path).map_err(db_err)?;

    // Get latest block
    let latest_block: u64 = conn
        .query_row(
            "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .map_err(db_err)?;

    let from_block = params
        .from_block
        .unwrap_or(latest_block.saturating_sub(100_000));

    // Step 1: Load bloom filter chunks that overlap our block range
    let mut bloom_stmt = conn
        .prepare(
            "SELECT from_block, to_block, bitmap \
             FROM event_filters \
             WHERE to_block >= ?1 AND from_block <= ?2 \
             ORDER BY from_block DESC",
        )
        .map_err(db_err)?;

    let mut candidate_blocks: Vec<u64> = Vec::new();

    let mut bloom_rows = bloom_stmt
        .query(rusqlite::params![from_block, latest_block])
        .map_err(db_err)?;

    while let Some(row) = bloom_rows.next().map_err(db_err)? {
        let bf_from: u64 = row.get(0).map_err(db_err)?;
        let bf_to: u64 = row.get(1).map_err(db_err)?;
        let compressed: Vec<u8> = row.get(2).map_err(db_err)?;

        let agg = bloom::AggregateBloom::from_compressed(bf_from, bf_to, &compressed);
        let mut blocks = agg.blocks_for_address(&addr_array);

        // Filter to our requested range
        blocks.retain(|&b| b >= from_block && b <= latest_block);
        candidate_blocks.extend(blocks);
    }

    // Also scan the most recent blocks not yet covered by event_filters
    // (the "running" filter may not be flushed yet)
    let max_bloom_block = candidate_blocks.iter().copied().max().unwrap_or(from_block);
    if max_bloom_block < latest_block {
        // Add all blocks from max_bloom_block+1 to latest_block (brute scan for recent)
        let recent_start = max_bloom_block + 1;
        for b in recent_start..=latest_block {
            candidate_blocks.push(b);
        }
    }

    // Sort descending (most recent first) and deduplicate
    candidate_blocks.sort_unstable_by(|a, b| b.cmp(a));
    candidate_blocks.dedup();

    // Cap candidate blocks to avoid runaway scans on very popular contracts.
    // Process most recent first; caller can paginate with from_block.
    const MAX_CANDIDATES: usize = 5000;
    candidate_blocks.truncate(MAX_CANDIDATES);

    // Step 2: Decode event blobs for candidate blocks in batches
    let mut results = Vec::new();

    // Process in batches of 500 blocks using an IN clause for efficient DB access
    for chunk in candidate_blocks.chunks(500) {
        if results.len() >= limit {
            break;
        }

        let placeholders: String = (0..chunk.len())
            .map(|i| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT t.block_number, t.events, bh.timestamp \
             FROM transactions t \
             JOIN block_headers bh ON t.block_number = bh.number \
             WHERE t.block_number IN ({placeholders}) AND t.events IS NOT NULL \
             ORDER BY t.block_number DESC"
        );

        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let params: Vec<Box<dyn rusqlite::types::ToSql>> = chunk
            .iter()
            .map(|b| Box::new(*b as i64) as Box<dyn rusqlite::types::ToSql>)
            .collect();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|b| b.as_ref()).collect();

        let mut rows = stmt.query(param_refs.as_slice()).map_err(db_err)?;

        while let Some(row) = rows.next().map_err(db_err)? {
            if results.len() >= limit {
                break;
            }

            let block_number: u64 = row.get(0).map_err(db_err)?;
            let events_blob: Vec<u8> = row.get(1).map_err(db_err)?;
            let timestamp: u64 = row.get(2).map_err(db_err)?;

            let tx_events = match decode::decode_events(&events_blob) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for (tx_idx, events) in tx_events.iter().enumerate() {
                for (ev_idx, event) in events.iter().enumerate() {
                    if event.from_address.0 == addr_bytes.as_slice() {
                        results.push(ContractEvent {
                            tx_index: tx_idx,
                            event_index: ev_idx,
                            from_address: event.from_address.to_hex(),
                            keys: event.keys.iter().map(|k| k.to_hex()).collect(),
                            data: event.data.iter().map(|d| d.to_hex()).collect(),
                            block_number,
                            timestamp,
                        });

                        if results.len() >= limit {
                            break;
                        }
                    }
                }
                if results.len() >= limit {
                    break;
                }
            }
        }
    }

    Ok(Json(results))
}

// =========================================================================
// Main
// =========================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pf_query=info,tower_http=debug".parse().unwrap()),
        )
        .init();

    let config = Config::parse();

    // Verify DB is accessible on startup
    let conn = open_db(&config.db_path)?;
    let latest: u64 = conn.query_row(
        "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    info!(db = %config.db_path, latest_block = latest, "Pathfinder DB opened successfully");

    // Quick decode smoke test on latest block
    let blob: Vec<u8> = conn.query_row(
        "SELECT transactions FROM transactions WHERE block_number = ?1",
        rusqlite::params![latest],
        |row| row.get(0),
    )?;
    let txs = decode::decode_transactions(&blob)?;
    info!(
        block = latest,
        tx_count = txs.len(),
        "Blob decode smoke test passed"
    );
    drop(conn);

    let state = AppState {
        db_path: Arc::new(config.db_path),
    };

    let app = Router::new()
        .route("/health", get(handler_health))
        .route("/nonce-history/{address}", get(handler_nonce_history))
        .route("/class-history/{address}", get(handler_class_history))
        .route(
            "/contracts-by-class/{class_hash}",
            get(handler_contracts_by_class),
        )
        .route(
            "/class-declaration/{class_hash}",
            get(handler_class_declaration),
        )
        .route("/tx-by-hash/{hash}", get(handler_tx_by_hash))
        .route("/block-txs/{block_number}", get(handler_block_txs))
        .route("/sender-txs/{address}", get(handler_sender_txs))
        .route("/contract-events/{address}", get(handler_contract_events))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    let addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "pf-query service listening");
    axum::serve(listener, app).await?;

    Ok(())
}
