use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
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

/// Per-tx response for `/txs-by-hash` — includes calldata so the client can
/// decode multicalls/endpoint names without a separate RPC round trip.
#[derive(Serialize)]
struct TxByHashEntry {
    hash: String,
    block_number: u64,
    block_timestamp: u64,
    tx_index: u64,
    sender: String,
    nonce: Option<u64>,
    tx_type: String,
    /// Calldata as `0x`-prefixed hex felts, matching RPC `InvokeTransaction.calldata`.
    calldata: Vec<String>,
    actual_fee: String,
    tip: u64,
    status: String,
    revert_reason: Option<String>,
}

/// Entry for `/block-timestamps`.
#[derive(Serialize)]
struct BlockTimestampEntry {
    block_number: u64,
    timestamp: u64,
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
    tx_hash: String,
    from_address: String,
    keys: Vec<String>,
    data: Vec<String>,
    block_number: u64,
    timestamp: u64,
}

/// Paginated response for /contract-events.
#[derive(Serialize)]
struct ContractEventsResponse {
    events: Vec<ContractEvent>,
    /// If set, the caller should pass this as `to_block` (inclusive upper bound)
    /// on the next request to continue newest-first pagination.
    continuation_token: Option<u64>,
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
struct TxsByHashRequest {
    hashes: Vec<String>,
}

#[derive(Deserialize)]
struct BlockTimestampsParams {
    from: u64,
    to: u64,
}

/// Server-side cap on how many hashes a single /txs-by-hash POST may include.
/// Generous enough that one enrichment run of ~1000 missing endpoints is a
/// single request, while bounding memory and decode CPU per request.
const TXS_BY_HASH_MAX: usize = 10_000;

/// Server-side cap on how many blocks a single /block-timestamps request may
/// span. 50_000 ≈ ~3 days at 5s block time; the underlying SQL is a single
/// indexed range scan so this is cheap.
const BLOCK_TIMESTAMPS_MAX_SPAN: u64 = 50_000;

#[derive(Deserialize)]
struct ContractEventsParams {
    /// Inclusive lower block bound. Default 0 (scan whole chain).
    from_block: Option<u64>,
    /// Inclusive upper block bound. Default = latest block.
    to_block: Option<u64>,
    /// Key filter: positional groups separated by `;`, OR-keys within a group by `,`.
    /// An empty group is a wildcard for that position. Example:
    /// `keys=0x3db...,0x0af...;;0xc2f...` means
    ///   (key[0] IN {0x3db, 0x0af}) AND (key[2] == 0xc2f).
    keys: Option<String>,
    /// Max events per page. Clamped to 5000.
    limit: Option<u32>,
    /// Pagination: inclusive upper bound for the next page (newest-first).
    /// Takes precedence over `to_block`.
    continuation_token: Option<u64>,
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

/// POST /txs-by-hash — bulk lookup of tx + receipt data by hash.
///
/// Groups input hashes by block_number (single SELECT on `transaction_hashes`),
/// decompresses each block's `transactions` blob at most once, and returns
/// calldata + sender + nonce + fee + status + timestamp per tx. This offloads
/// the address-view enrichment path from Starknet RPC.
///
/// Missing hashes are silently omitted from the response. Caller should
/// diff requested vs returned hashes to decide fallback behavior.
async fn handler_txs_by_hash(
    State(state): State<AppState>,
    Json(req): Json<TxsByHashRequest>,
) -> ApiResult<Vec<TxByHashEntry>> {
    if req.hashes.is_empty() {
        return Ok(Json(vec![]));
    }
    if req.hashes.len() > TXS_BY_HASH_MAX {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Too many hashes: {} (max {})",
                req.hashes.len(),
                TXS_BY_HASH_MAX
            ),
        ));
    }

    // Parse all hashes up front; reject the whole request on a bad one.
    let mut wanted: Vec<(Vec<u8>, String)> = Vec::with_capacity(req.hashes.len());
    for h in &req.hashes {
        let bytes = parse_address(h)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid hash {h}: {e}")))?;
        wanted.push((bytes, h.clone()));
    }

    let db_path = Arc::clone(&state.db_path);

    // Entire body runs on a blocking thread — rusqlite `Connection` is !Send,
    // so we keep it off the async state machine and use rayon::par_iter for
    // the CPU-bound blob decode.
    let results = tokio::task::spawn_blocking(move || {
        let conn = open_db(&db_path).map_err(db_err)?;

        // Step 1: map each hash → (block_number, idx). One SELECT per hash
        // (fast indexed lookup on `transaction_hashes`, no wildcard scan).
        let mut lookup_stmt = conn
            .prepare("SELECT block_number, idx FROM transaction_hashes WHERE hash = ?1")
            .map_err(db_err)?;

        // (block_number, tx_idx_in_block, original_hash_hex)
        let mut locations: Vec<(u64, u64, String)> = Vec::with_capacity(wanted.len());
        for (hash_bytes, hash_hex) in &wanted {
            match lookup_stmt.query_row(rusqlite::params![hash_bytes.as_slice()], |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))
            }) {
                Ok((block_number, idx)) => locations.push((block_number, idx, hash_hex.clone())),
                Err(rusqlite::Error::QueryReturnedNoRows) => continue,
                Err(e) => return Err(db_err(e)),
            }
        }

        if locations.is_empty() {
            return Ok::<Vec<TxByHashEntry>, (StatusCode, String)>(Vec::new());
        }

        // Step 2: gather unique block blobs + timestamps (cheap indexed reads).
        let mut tx_blob_stmt = conn
            .prepare("SELECT transactions FROM transactions WHERE block_number = ?1")
            .map_err(db_err)?;
        let mut timestamp_stmt = conn
            .prepare("SELECT timestamp FROM block_headers WHERE number = ?1")
            .map_err(db_err)?;

        let unique_blocks: std::collections::BTreeSet<u64> =
            locations.iter().map(|(b, _, _)| *b).collect();

        let mut blobs: Vec<(u64, Vec<u8>)> = Vec::with_capacity(unique_blocks.len());
        let mut timestamp_cache: std::collections::HashMap<u64, u64> =
            std::collections::HashMap::new();
        for block_number in &unique_blocks {
            if let Ok(blob) = tx_blob_stmt
                .query_row(rusqlite::params![block_number], |row| row.get::<_, Vec<u8>>(0))
            {
                blobs.push((*block_number, blob));
            }
            if let Ok(ts) =
                timestamp_stmt.query_row(rusqlite::params![block_number], |row| row.get(0))
            {
                timestamp_cache.insert(*block_number, ts);
            }
        }

        // Step 3: parallel zstd + bincode decode via rayon.
        let decoded_by_block: std::collections::HashMap<u64, Vec<dto::TransactionWithReceiptV4>> = {
            use rayon::prelude::*;
            blobs
                .into_par_iter()
                .filter_map(|(block_number, blob)| match decode::decode_transactions(&blob) {
                    Ok(d) => Some((block_number, d)),
                    Err(e) => {
                        tracing::warn!(block = block_number, error = %e, "Failed to decode tx blob");
                        None
                    }
                })
                .collect()
        };

        // Step 4: stitch results in the original request order.
        let mut results: Vec<TxByHashEntry> = Vec::with_capacity(locations.len());
        for (block_number, tx_idx, hash_hex) in &locations {
            let Some(txs) = decoded_by_block.get(block_number) else {
                continue;
            };
            let Some(timestamp) = timestamp_cache.get(block_number).copied() else {
                continue;
            };
            let tr = match txs.get(*tx_idx as usize) {
                Some(t) => t,
                None => continue,
            };

            let (status, revert_reason) = match &tr.receipt.execution_status {
                dto::ExecutionStatus::Succeeded => ("OK".to_string(), None),
                dto::ExecutionStatus::Reverted { reason } => {
                    ("REV".to_string(), Some(reason.clone()))
                }
            };

            let calldata: Vec<String> = tr
                .transaction
                .calldata()
                .iter()
                .map(|f| f.to_hex())
                .collect();

            results.push(TxByHashEntry {
                hash: hash_hex.clone(),
                block_number: *block_number,
                block_timestamp: timestamp,
                tx_index: *tx_idx,
                sender: tr
                    .transaction
                    .variant
                    .sender_address()
                    .map(|a| a.to_hex())
                    .unwrap_or_default(),
                nonce: tr.transaction.nonce(),
                tx_type: tr.transaction.tx_type().to_string(),
                calldata,
                actual_fee: tr.receipt.actual_fee.to_hex(),
                tip: tr.transaction.tip(),
                status,
                revert_reason,
            });
        }

        Ok(results)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Join error: {e}")))??;

    Ok(Json(results))
}

/// GET /block-timestamps?from=N&to=M — bulk fetch block timestamps in range.
///
/// Single indexed range scan on `block_headers.number`. Replaces the per-block
/// RPC round trip in snbeat's `backfill_timestamps`.
async fn handler_block_timestamps(
    Query(params): Query<BlockTimestampsParams>,
    State(state): State<AppState>,
) -> ApiResult<Vec<BlockTimestampEntry>> {
    if params.to < params.from {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("to ({}) must be >= from ({})", params.to, params.from),
        ));
    }
    let span = params.to - params.from + 1;
    if span > BLOCK_TIMESTAMPS_MAX_SPAN {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Range too large: {} blocks (max {})",
                span, BLOCK_TIMESTAMPS_MAX_SPAN
            ),
        ));
    }

    let conn = open_db(&state.db_path).map_err(db_err)?;
    let mut stmt = conn
        .prepare(
            "SELECT number, timestamp FROM block_headers \
             WHERE number BETWEEN ?1 AND ?2 ORDER BY number",
        )
        .map_err(db_err)?;
    let entries: Vec<BlockTimestampEntry> = stmt
        .query_map(rusqlite::params![params.from, params.to], |row| {
            Ok(BlockTimestampEntry {
                block_number: row.get(0)?,
                timestamp: row.get(1)?,
            })
        })
        .map_err(db_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_err)?;

    Ok(Json(entries))
}

/// GET /sender-txs/{address} — full tx history for an account via nonce_updates + blob decode.
///
/// Combines nonce_updates (to find blocks) with transaction blob decoding
/// (to get hash, fee, status, type) in a single request.
///
/// The zstd+bincode decode of each block blob is the CPU-bound hot path. We
/// gather blobs first (cheap, indexed SQLite reads), then decode them in
/// parallel via `rayon` inside `spawn_blocking` so we don't block the tokio
/// worker and we exploit the server's cores. Output ordering matches the
/// nonce_updates DESC order of the serial implementation it replaces.
async fn handler_sender_txs(
    Path(address): Path<String>,
    Query(params): Query<SenderTxParams>,
    State(state): State<AppState>,
) -> ApiResult<Vec<SenderTxEntry>> {
    let limit = params.limit.unwrap_or(500).min(2000);
    let addr_bytes = parse_address(&address)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid address: {e}")))?;
    let db_path = Arc::clone(&state.db_path);

    // Entire body runs on a blocking thread so the rusqlite `Connection`
    // (which is !Send) never crosses an async await point. Inside, we use
    // rayon::par_iter to decode block blobs across the server's cores.
    let results = tokio::task::spawn_blocking(move || {
        let conn = open_db(&db_path).map_err(db_err)?;

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
            return Ok::<Vec<SenderTxEntry>, (StatusCode, String)>(Vec::new());
        }

        // Step 2: Dedup blocks and fetch each blob once. Preserve
        // first-occurrence order so the final output matches nonce_updates
        // DESC ordering.
        let mut tx_stmt = conn
            .prepare("SELECT transactions FROM transactions WHERE block_number = ?1")
            .map_err(db_err)?;

        let mut block_order: Vec<(u64, u64, u64)> = Vec::with_capacity(nonce_entries.len());
        let mut blobs: Vec<(u64, Vec<u8>)> = Vec::with_capacity(nonce_entries.len());
        let mut seen_blocks: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (block_number, expected_nonce, timestamp) in &nonce_entries {
            if !seen_blocks.insert(*block_number) {
                continue;
            }
            block_order.push((*block_number, *expected_nonce, *timestamp));
            if let Ok(blob) = tx_stmt.query_row(rusqlite::params![block_number], |row| {
                row.get::<_, Vec<u8>>(0)
            }) {
                blobs.push((*block_number, blob));
            }
        }

        // Step 3: Parallel zstd + bincode decode of block blobs via rayon.
        let decoded_by_block: std::collections::HashMap<u64, Vec<dto::TransactionWithReceiptV4>> = {
            use rayon::prelude::*;
            blobs
                .into_par_iter()
                .filter_map(
                    |(block_number, blob)| match decode::decode_transactions(&blob) {
                        Ok(txs) => Some((block_number, txs)),
                        Err(e) => {
                            tracing::warn!(block = block_number, error = %e, "decode tx blob");
                            None
                        }
                    },
                )
                .collect()
        };

        // Step 4: Stitch results in block_order. For each unique block, emit
        // every matching sender tx; if none matched, emit a stub from
        // nonce_updates (nonce = expected_nonce - 1).
        let mut results: Vec<SenderTxEntry> = Vec::with_capacity(block_order.len());
        for (block_number, expected_nonce, timestamp) in &block_order {
            let mut matched_any = false;
            if let Some(txs) = decoded_by_block.get(block_number) {
                for tr in txs {
                    let sender = match tr.transaction.sender_address() {
                        Some(s) => s,
                        None => continue,
                    };
                    if sender.0 != addr_bytes.as_slice() {
                        continue;
                    }
                    let (status, revert_reason) = match &tr.receipt.execution_status {
                        dto::ExecutionStatus::Succeeded => ("OK".to_string(), None),
                        dto::ExecutionStatus::Reverted { reason } => {
                            ("REV".to_string(), Some(reason.clone()))
                        }
                    };
                    results.push(SenderTxEntry {
                        hash: tr.transaction.hash.to_hex(),
                        sender_address: sender.to_hex(),
                        nonce: tr.transaction.nonce(),
                        block_number: *block_number,
                        timestamp: *timestamp,
                        tx_type: tr.transaction.tx_type().to_string(),
                        actual_fee: tr.receipt.actual_fee.to_hex(),
                        tip: tr.transaction.tip(),
                        status,
                        revert_reason,
                    });
                    matched_any = true;
                }
            }
            if !matched_any {
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

        Ok(results)
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Join error: {e}"),
        )
    })??;

    Ok(Json(results))
}

/// Parse the `keys` query-param into positional filter groups.
///
/// Format: groups separated by `;`, OR-keys within a group by `,`.
/// An empty group means "no filter" for that position.
/// Each key is a hex string (0x-prefixed optional). Returns an error for
/// malformed hex; silently tolerates extra whitespace.
fn parse_keys_filter(raw: &str) -> Result<Vec<Vec<[u8; 32]>>, String> {
    let mut groups = Vec::new();
    for group_str in raw.split(';') {
        let group_str = group_str.trim();
        let mut group = Vec::new();
        if !group_str.is_empty() {
            for key_str in group_str.split(',') {
                let key_str = key_str.trim();
                if key_str.is_empty() {
                    continue;
                }
                let bytes =
                    parse_address(key_str).map_err(|e| format!("Invalid key in filter: {e}"))?;
                let arr: [u8; 32] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| "key must decode to 32 bytes".to_string())?;
                group.push(arr);
            }
        }
        groups.push(group);
    }
    Ok(groups)
}

/// Check an event's keys against a positional filter.
///
/// Each group in `filter` constrains the key at that position:
/// - empty group = no constraint
/// - non-empty group = event.keys[i] must be one of the listed values
///
/// Groups beyond the event's key count fail the match (can't satisfy a
/// required position that doesn't exist).
fn event_keys_match(event_keys: &[dto::MinimalFelt], filter: &[Vec<[u8; 32]>]) -> bool {
    for (i, group) in filter.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let ek = match event_keys.get(i) {
            Some(k) => &k.0,
            None => return false,
        };
        if !group.iter().any(|g| g == ek) {
            return false;
        }
    }
    true
}

/// Decode events for a single block and append matching ones to `results`.
///
/// Shared between the brute-scan phase and the bloom-walk phase. Silently
/// skips blocks with no blob or with decode errors — bloom filters have
/// false positives, so "no match in this block" is normal.
fn process_candidate_block(
    block_number: u64,
    addr_bytes: &[u8],
    keys_filter: &[Vec<[u8; 32]>],
    events_stmt: &mut rusqlite::Statement<'_>,
    txs_stmt: &mut rusqlite::Statement<'_>,
    ts_stmt: &mut rusqlite::Statement<'_>,
    results: &mut Vec<ContractEvent>,
) {
    let events_blob: Option<Vec<u8>> = events_stmt
        .query_row(rusqlite::params![block_number], |row| row.get(0))
        .ok();
    let Some(events_blob) = events_blob else {
        return;
    };

    let tx_events = match decode::decode_events(&events_blob) {
        Ok(e) => e,
        Err(_) => return,
    };

    // Fast rejection scan: bloom filters have false positives, so most
    // candidate blocks will have no match.
    let any_match = tx_events.iter().any(|evs| {
        evs.iter()
            .any(|e| e.from_address.0 == addr_bytes && event_keys_match(&e.keys, keys_filter))
    });
    if !any_match {
        return;
    }

    // Need tx hashes for this block.
    let txs_blob: Option<Vec<u8>> = txs_stmt
        .query_row(rusqlite::params![block_number], |row| row.get(0))
        .ok();
    let tx_hashes: Vec<String> = txs_blob
        .and_then(|b| decode::decode_transactions(&b).ok())
        .map(|txs| txs.iter().map(|t| t.transaction.hash.to_hex()).collect())
        .unwrap_or_default();

    let timestamp: u64 = ts_stmt
        .query_row(rusqlite::params![block_number], |row| row.get(0))
        .unwrap_or(0);

    for (tx_idx, events) in tx_events.iter().enumerate() {
        for (ev_idx, event) in events.iter().enumerate() {
            if event.from_address.0 != addr_bytes {
                continue;
            }
            if !event_keys_match(&event.keys, keys_filter) {
                continue;
            }
            let tx_hash = tx_hashes
                .get(tx_idx)
                .cloned()
                .unwrap_or_else(|| "0x0".to_string());
            results.push(ContractEvent {
                tx_index: tx_idx,
                event_index: ev_idx,
                tx_hash,
                from_address: event.from_address.to_hex(),
                keys: event.keys.iter().map(|k| k.to_hex()).collect(),
                data: event.data.iter().map(|d| d.to_hex()).collect(),
                block_number,
                timestamp,
            });
        }
    }
}

/// GET /contract-events/{address} — events emitted by a contract, accelerated by bloom filters.
///
/// Pagination: newest-first. When more events may exist below the oldest returned
/// block, the response includes `continuation_token = Some(next_to_block)`; the
/// caller passes that back on the next request to continue.
///
/// Key filter: see `ContractEventsParams::keys`.
async fn handler_contract_events(
    Path(address): Path<String>,
    Query(params): Query<ContractEventsParams>,
    State(state): State<AppState>,
) -> ApiResult<ContractEventsResponse> {
    // Cap per-page events. Bigger than before since pagination handles the rest.
    let limit = params.limit.unwrap_or(500).min(5000) as usize;

    // Per-request safety cap on candidate blocks, to bound latency on
    // very dense contracts. If the bloom-filter candidate set exceeds this,
    // we process the newest MAX_CANDIDATES and return a continuation token.
    const MAX_CANDIDATES: usize = 10_000;

    let addr_bytes = parse_address(&address)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid address: {e}")))?;

    let addr_array: [u8; 32] = addr_bytes
        .as_slice()
        .try_into()
        .map_err(|_| (StatusCode::BAD_REQUEST, "Address must be 32 bytes".into()))?;

    let keys_filter: Vec<Vec<[u8; 32]>> = match params.keys.as_deref() {
        Some(raw) => parse_keys_filter(raw).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => Vec::new(),
    };

    let conn = open_db(&state.db_path).map_err(db_err)?;

    // Resolve block range.
    let latest_block: u64 = conn
        .query_row(
            "SELECT number FROM block_headers ORDER BY number DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .map_err(db_err)?;

    let from_block = params.from_block.unwrap_or(0);
    // continuation_token takes precedence over to_block.
    let requested_to = params
        .continuation_token
        .or(params.to_block)
        .unwrap_or(latest_block);
    let effective_to = requested_to.min(latest_block);

    if from_block > effective_to {
        return Ok(Json(ContractEventsResponse {
            events: Vec::new(),
            continuation_token: None,
        }));
    }

    // Step 1: Discover max bloom coverage so we know where to brute-scan.
    // Blocks above `max_bloom_covered` are in `running_event_filter` (not yet
    // persisted into an `event_filters` row) and must be scanned directly.
    let max_bloom_covered: Option<u64> = conn
        .query_row(
            "SELECT MAX(to_block) FROM event_filters WHERE from_block <= ?1",
            rusqlite::params![effective_to],
            |row| row.get::<_, Option<u64>>(0),
        )
        .unwrap_or(None);

    let brute_scan_start = max_bloom_covered
        .map(|m| m.saturating_add(1).max(from_block))
        .unwrap_or(from_block);

    // Statements used during per-block processing.
    let mut events_stmt = conn
        .prepare("SELECT events FROM transactions WHERE block_number = ?1")
        .map_err(db_err)?;
    let mut txs_stmt = conn
        .prepare("SELECT transactions FROM transactions WHERE block_number = ?1")
        .map_err(db_err)?;
    let mut ts_stmt = conn
        .prepare("SELECT timestamp FROM block_headers WHERE number = ?1")
        .map_err(db_err)?;

    // Inline closure-like helper: given a block number, decode events and push
    // any matching ones into `results`. Returns true if the block contained at
    // least one match (for telemetry; not used directly).
    //
    // We expand it as a macro-style block below since it borrows `results`,
    // the addr bytes, the filter, and multiple prepared statements — inlining
    // avoids lifetime gymnastics.
    let mut results: Vec<ContractEvent> = Vec::new();
    let mut last_processed: Option<u64> = None;
    let mut candidates_processed: usize = 0;
    let mut hit_limit = false;
    let mut hit_candidate_cap = false;

    // Step 2a: Brute-scan blocks above the persisted bloom filter, newest-first.
    if brute_scan_start <= effective_to {
        let mut b = effective_to;
        loop {
            if b < brute_scan_start {
                break;
            }
            last_processed = Some(b);
            candidates_processed += 1;

            process_candidate_block(
                b,
                &addr_bytes,
                &keys_filter,
                &mut events_stmt,
                &mut txs_stmt,
                &mut ts_stmt,
                &mut results,
            );

            if results.len() >= limit {
                hit_limit = true;
                break;
            }
            if candidates_processed >= MAX_CANDIDATES {
                hit_candidate_cap = true;
                break;
            }
            if b == 0 {
                break;
            }
            b -= 1;
        }
    }

    // Step 2b: Walk bloom chunks newest-first, decompressing one at a time.
    // As soon as `limit` is reached we stop — the continuation token resumes
    // the scan from (last_processed - 1) on the next call.
    if !hit_limit && !hit_candidate_cap {
        let mut bloom_stmt = conn
            .prepare(
                "SELECT from_block, to_block, bitmap \
                 FROM event_filters \
                 WHERE to_block >= ?1 AND from_block <= ?2 \
                 ORDER BY from_block DESC",
            )
            .map_err(db_err)?;

        let mut bloom_rows = bloom_stmt
            .query(rusqlite::params![from_block, effective_to])
            .map_err(db_err)?;

        'chunks: while let Some(row) = bloom_rows.next().map_err(db_err)? {
            let bf_from: u64 = row.get(0).map_err(db_err)?;
            let bf_to: u64 = row.get(1).map_err(db_err)?;
            let compressed: Vec<u8> = row.get(2).map_err(db_err)?;

            let agg = bloom::AggregateBloom::from_compressed(bf_from, bf_to, &compressed);
            let mut blocks = agg.blocks_for_address(&addr_array);
            blocks.retain(|&b| b >= from_block && b <= effective_to);
            // Process within-chunk candidates newest-first.
            blocks.sort_unstable_by(|a, b| b.cmp(a));
            blocks.dedup();

            for block_number in blocks {
                last_processed = Some(block_number);
                candidates_processed += 1;

                process_candidate_block(
                    block_number,
                    &addr_bytes,
                    &keys_filter,
                    &mut events_stmt,
                    &mut txs_stmt,
                    &mut ts_stmt,
                    &mut results,
                );

                if results.len() >= limit {
                    hit_limit = true;
                    break 'chunks;
                }
                if candidates_processed >= MAX_CANDIDATES {
                    hit_candidate_cap = true;
                    break 'chunks;
                }
            }
        }
    }

    // Compute continuation token.
    let continuation_token = if hit_limit || hit_candidate_cap {
        // More to scan: next page should start at (last_processed - 1).
        last_processed
            .and_then(|b| b.checked_sub(1))
            .filter(|&b| b >= from_block)
    } else {
        None
    };

    Ok(Json(ContractEventsResponse {
        events: results,
        continuation_token,
    }))
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
        .route("/txs-by-hash", post(handler_txs_by_hash))
        .route("/block-txs/{block_number}", get(handler_block_txs))
        .route("/block-timestamps", get(handler_block_timestamps))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::MinimalFelt;

    fn felt(hex_last_byte: u8) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[31] = hex_last_byte;
        out
    }

    fn mfelt(hex_last_byte: u8) -> MinimalFelt {
        MinimalFelt(felt(hex_last_byte))
    }

    // ----- parse_keys_filter --------------------------------------------------

    #[test]
    fn parse_keys_filter_single_group_single_key() {
        let parsed = parse_keys_filter("0x1").unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0], vec![felt(1)]);
    }

    #[test]
    fn parse_keys_filter_or_within_group() {
        let parsed = parse_keys_filter("0x1,0x2").unwrap();
        assert_eq!(parsed, vec![vec![felt(1), felt(2)]]);
    }

    #[test]
    fn parse_keys_filter_positional_groups() {
        let parsed = parse_keys_filter("0x1;;0x3").unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], vec![felt(1)]);
        assert!(parsed[1].is_empty(), "empty group is a wildcard");
        assert_eq!(parsed[2], vec![felt(3)]);
    }

    #[test]
    fn parse_keys_filter_tolerates_whitespace_and_empty_tail() {
        // Trailing comma / empty slot is tolerated rather than producing a
        // phantom zero key — caller typos shouldn't silently match 0x0.
        let parsed = parse_keys_filter(" 0x1 , 0x2 ;0x3, ").unwrap();
        assert_eq!(parsed, vec![vec![felt(1), felt(2)], vec![felt(3)]]);
    }

    #[test]
    fn parse_keys_filter_rejects_bad_hex() {
        let err = parse_keys_filter("0xZZZ").unwrap_err();
        assert!(err.contains("Invalid key"), "err was: {err}");
    }

    // ----- event_keys_match ---------------------------------------------------

    #[test]
    fn event_keys_match_empty_filter_matches_anything() {
        let keys = vec![mfelt(1), mfelt(2)];
        assert!(event_keys_match(&keys, &[]));
    }

    #[test]
    fn event_keys_match_empty_group_is_wildcard() {
        let keys = vec![mfelt(1), mfelt(2), mfelt(3)];
        // [*, *, 0x3]
        let filter = vec![vec![], vec![], vec![felt(3)]];
        assert!(event_keys_match(&keys, &filter));
    }

    #[test]
    fn event_keys_match_exact_positional() {
        let keys = vec![mfelt(0xAA), mfelt(0xBB)];
        let filter = vec![vec![felt(0xAA)], vec![felt(0xBB)]];
        assert!(event_keys_match(&keys, &filter));
    }

    #[test]
    fn event_keys_match_or_within_group() {
        let keys = vec![mfelt(0xBB)];
        let filter = vec![vec![felt(0xAA), felt(0xBB), felt(0xCC)]];
        assert!(event_keys_match(&keys, &filter));
    }

    #[test]
    fn event_keys_match_mismatch_rejects() {
        let keys = vec![mfelt(0xAA), mfelt(0xBB)];
        let filter = vec![vec![felt(0xAA)], vec![felt(0xCC)]];
        assert!(!event_keys_match(&keys, &filter));
    }

    #[test]
    fn event_keys_match_longer_filter_than_event_is_reject() {
        // Filter requires a key at position 2, event only has 2 keys → no match.
        let keys = vec![mfelt(0xAA), mfelt(0xBB)];
        let filter = vec![vec![felt(0xAA)], vec![felt(0xBB)], vec![felt(0xCC)]];
        assert!(!event_keys_match(&keys, &filter));
    }

    #[test]
    fn event_keys_match_trailing_wildcard_ignored() {
        // Wildcard (empty) groups don't look at event_keys, so a trailing
        // wildcard beyond the event's key count is harmless.
        let keys = vec![mfelt(0xAA)];
        let filter = vec![vec![felt(0xAA)], vec![]];
        assert!(event_keys_match(&keys, &filter));
    }
}
