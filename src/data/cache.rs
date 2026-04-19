use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::Shared;
use rusqlite::{Connection, params};
use starknet::core::types::{ContractClass, Felt};
use tracing::{debug, trace, warn};

use super::DataSource;
#[allow(unused_imports)]
use super::types::*;
use crate::error::{Result, SnbeatError};

/// Shared future output: errors are stringified so the future is `Clone`-able
/// (SnbeatError is not Clone due to `std::io::Error`).
type SharedTxFut =
    Shared<Pin<Box<dyn Future<Output = std::result::Result<SnTransaction, String>> + Send>>>;
type SharedRxFut =
    Shared<Pin<Box<dyn Future<Output = std::result::Result<SnReceipt, String>> + Send>>>;

/// Persistent cache backed by SQLite + in-memory LRU.
/// Wraps any DataSource: checks cache first, fetches from upstream on miss,
/// writes through to cache on fetch. Persists across restarts.
///
/// Also deduplicates concurrent in-flight `get_transaction` / `get_receipt`
/// fetches so that N parallel callers for the same hash produce one RPC round
/// trip, not N. Prevents the user-click-races-background-enrichment storm.
pub struct CachingDataSource {
    upstream: Arc<dyn DataSource>,
    db: Mutex<Connection>,
    pending_txs: Mutex<HashMap<Felt, SharedTxFut>>,
    pending_receipts: Mutex<HashMap<Felt, SharedRxFut>>,
}

impl CachingDataSource {
    pub fn new(upstream: Arc<dyn DataSource>, cache_path: &Path) -> Result<Self> {
        let db = Connection::open(cache_path)
            .map_err(|e| SnbeatError::Config(format!("Failed to open cache db: {e}")))?;

        // Create tables
        db.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS blocks (
                number INTEGER PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS transactions (
                hash TEXT PRIMARY KEY,
                block_number INTEGER,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS receipts (
                tx_hash TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_number INTEGER,
                tx_index INTEGER,
                tx_hash TEXT NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (block_number, tx_index)
            );
            CREATE TABLE IF NOT EXISTS parsed_abis (
                class_hash TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS address_events (
                address TEXT NOT NULL,
                event_index INTEGER NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (address, event_index)
            );
            CREATE INDEX IF NOT EXISTS idx_addr_events ON address_events(address);
            CREATE TABLE IF NOT EXISTS address_txs (
                address TEXT NOT NULL,
                tx_index INTEGER NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (address, tx_index)
            );
            CREATE INDEX IF NOT EXISTS idx_addr_txs ON address_txs(address);
            CREATE INDEX IF NOT EXISTS idx_tx_block ON transactions(block_number);
            CREATE INDEX IF NOT EXISTS idx_block_tx ON block_transactions(block_number);
            CREATE TABLE IF NOT EXISTS address_calls (
                address TEXT NOT NULL,
                call_index INTEGER NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (address, call_index)
            );
            CREATE INDEX IF NOT EXISTS idx_addr_calls ON address_calls(address);
            CREATE TABLE IF NOT EXISTS address_activity (
                address TEXT PRIMARY KEY,
                min_block INTEGER NOT NULL,
                max_block INTEGER NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS class_hashes (
                address TEXT PRIMARY KEY,
                class_hash TEXT NOT NULL,
                fetched_at_block INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS block_hash_index (
                hash TEXT PRIMARY KEY,
                number INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deploy_info (
                address TEXT PRIMARY KEY,
                deploy_tx_hash TEXT NOT NULL,
                deploy_block INTEGER NOT NULL,
                deployer TEXT
            );
            CREATE TABLE IF NOT EXISTS cached_nonces (
                address TEXT PRIMARY KEY,
                nonce TEXT NOT NULL,
                block_number INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS address_search_progress (
                address TEXT PRIMARY KEY,
                max_searched_block INTEGER NOT NULL,
                min_searched_block INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS contract_events (
                address TEXT NOT NULL,
                event_index INTEGER NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (address, event_index)
            );
            CREATE INDEX IF NOT EXISTS idx_contract_events ON contract_events(address);
            CREATE TABLE IF NOT EXISTS address_meta_txs (
                address TEXT NOT NULL,
                tx_hash TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (address, tx_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_addr_meta_txs
                ON address_meta_txs(address, block_number DESC);
            CREATE TABLE IF NOT EXISTS class_declarations (
                class_hash TEXT PRIMARY KEY,
                tx_hash TEXT NOT NULL,
                sender TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                timestamp INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS class_contracts (
                class_hash TEXT NOT NULL,
                contract_address TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                PRIMARY KEY (class_hash, contract_address)
            );
            CREATE INDEX IF NOT EXISTS idx_class_contracts_class
                ON class_contracts(class_hash, block_number DESC);
            CREATE TABLE IF NOT EXISTS class_contracts_meta (
                class_hash TEXT PRIMARY KEY,
                fetched_at INTEGER NOT NULL
            );
            ",
        )
        .map_err(|e| SnbeatError::Config(format!("Failed to init cache schema: {e}")))?;

        // --- Migrations (keyed on PRAGMA user_version) ---
        let version: i64 = db
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap_or(0);

        if version < 4 {
            // v4: DEPLOY_ACCOUNT txs had wrong contract_address. Clear all
            // tx/block/address caches so they are re-fetched with correct data.
            db.execute_batch(
                "DELETE FROM transactions;
                 DELETE FROM block_transactions;
                 DELETE FROM blocks;
                 DELETE FROM address_txs;
                 DELETE FROM deploy_info;
                 PRAGMA user_version = 4;",
            )
            .map_err(|e| SnbeatError::Config(format!("Migration v4 failed: {e}")))?;
            debug!("Migration v4: cleared tx/block/address caches");
        }

        Ok(Self {
            upstream,
            db: Mutex::new(db),
            pending_txs: Mutex::new(HashMap::new()),
            pending_receipts: Mutex::new(HashMap::new()),
        })
    }

    fn get_cached_block(&self, number: u64) -> Option<SnBlock> {
        let db = self.db.lock().ok()?;
        let mut stmt = db
            .prepare("SELECT data FROM blocks WHERE number = ?1")
            .ok()?;
        let json: String = stmt.query_row(params![number], |row| row.get(0)).ok()?;
        serde_json::from_str(&json).ok()
    }

    fn cache_block(&self, block: &SnBlock) {
        if let Ok(json) = serde_json::to_string(block) {
            if let Ok(db) = self.db.lock() {
                let _ = db.execute(
                    "INSERT OR REPLACE INTO blocks (number, data) VALUES (?1, ?2)",
                    params![block.number, json],
                );
                // Also index hash→number for get_block_by_hash cache
                if block.hash != Felt::ZERO {
                    let hash_hex = format!("{:#x}", block.hash);
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO block_hash_index (hash, number) VALUES (?1, ?2)",
                        params![hash_hex, block.number as i64],
                    );
                }
            }
        }
    }

    fn get_cached_block_with_txs(&self, number: u64) -> Option<(SnBlock, Vec<SnTransaction>)> {
        let block = self.get_cached_block(number)?;
        let db = self.db.lock().ok()?;
        let mut stmt = db
            .prepare(
                "SELECT data FROM block_transactions WHERE block_number = ?1 ORDER BY tx_index",
            )
            .ok()?;
        let txs: Vec<SnTransaction> = stmt
            .query_map(params![number], |row| {
                let json: String = row.get(0)?;
                Ok(json)
            })
            .ok()?
            .filter_map(|r| r.ok())
            .filter_map(|json| serde_json::from_str(&json).ok())
            .collect();

        if txs.is_empty() {
            return None;
        }
        Some((block, txs))
    }

    fn cache_block_with_txs(&self, block: &SnBlock, txs: &[SnTransaction]) {
        self.cache_block(block);
        if let Ok(db) = self.db.lock() {
            for (i, tx) in txs.iter().enumerate() {
                if let Ok(json) = serde_json::to_string(tx) {
                    let hash_hex = format!("{:#x}", tx.hash());
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO block_transactions (block_number, tx_index, tx_hash, data) VALUES (?1, ?2, ?3, ?4)",
                        params![block.number, i as i64, hash_hex, json],
                    );
                    // Also cache in transactions table for hash lookup
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO transactions (hash, block_number, data) VALUES (?1, ?2, ?3)",
                        params![hash_hex, block.number, json],
                    );
                }
            }
        }
    }

    fn get_cached_transaction(&self, hash: Felt) -> Option<SnTransaction> {
        let db = self.db.lock().ok()?;
        let hash_hex = format!("{:#x}", hash);
        let mut stmt = db
            .prepare("SELECT data FROM transactions WHERE hash = ?1")
            .ok()?;
        let json: String = stmt.query_row(params![hash_hex], |row| row.get(0)).ok()?;
        serde_json::from_str(&json).ok()
    }

    fn cache_transaction(&self, tx: &SnTransaction) {
        if let Ok(json) = serde_json::to_string(tx) {
            if let Ok(db) = self.db.lock() {
                let hash_hex = format!("{:#x}", tx.hash());
                let _ = db.execute(
                    "INSERT OR REPLACE INTO transactions (hash, block_number, data) VALUES (?1, ?2, ?3)",
                    params![hash_hex, 0i64, json],
                );
            }
        }
    }

    fn load_address_events(&self, address: &Felt) -> Vec<SnEvent> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return Vec::new(),
        };
        let addr_hex = format!("{:#x}", address);
        let mut stmt = match db
            .prepare("SELECT data FROM address_events WHERE address = ?1 ORDER BY event_index")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map(params![addr_hex], |row| row.get::<_, String>(0)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .filter_map(|json| serde_json::from_str(&json).ok())
            .collect()
    }

    fn save_address_events(&self, address: &Felt, events: &[SnEvent]) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            // Clear old events for this address and rewrite
            let _ = db.execute(
                "DELETE FROM address_events WHERE address = ?1",
                params![addr_hex],
            );
            for (i, event) in events.iter().enumerate() {
                if let Ok(json) = serde_json::to_string(event) {
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO address_events (address, event_index, data) VALUES (?1, ?2, ?3)",
                        params![addr_hex, i as i64, json],
                    );
                }
            }
        }
    }

    fn get_cached_receipt(&self, hash: Felt) -> Option<SnReceipt> {
        let db = self.db.lock().ok()?;
        let hash_hex = format!("{:#x}", hash);
        let mut stmt = db
            .prepare("SELECT data FROM receipts WHERE tx_hash = ?1")
            .ok()?;
        let json: String = stmt.query_row(params![hash_hex], |row| row.get(0)).ok()?;
        serde_json::from_str(&json).ok()
    }

    fn cache_receipt(&self, receipt: &SnReceipt) {
        if let Ok(json) = serde_json::to_string(receipt) {
            if let Ok(db) = self.db.lock() {
                let hash_hex = format!("{:#x}", receipt.transaction_hash);
                let _ = db.execute(
                    "INSERT OR REPLACE INTO receipts (tx_hash, data) VALUES (?1, ?2)",
                    params![hash_hex, json],
                );
            }
        }
    }

    // --- class_hash cache ---

    fn get_cached_class_hash(&self, address: &Felt) -> Option<(Felt, u64)> {
        let db = self.db.lock().ok()?;
        let addr_hex = format!("{:#x}", address);
        let mut stmt = db
            .prepare("SELECT class_hash, fetched_at_block FROM class_hashes WHERE address = ?1")
            .ok()?;
        stmt.query_row(params![addr_hex], |row| {
            let ch_hex: String = row.get(0)?;
            let block: i64 = row.get(1)?;
            Ok((ch_hex, block as u64))
        })
        .ok()
        .and_then(|(ch_hex, block)| Felt::from_hex(&ch_hex).ok().map(|f| (f, block)))
    }

    fn cache_class_hash(&self, address: &Felt, class_hash: &Felt, block: u64) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let ch_hex = format!("{:#x}", class_hash);
            let _ = db.execute(
                "INSERT OR REPLACE INTO class_hashes (address, class_hash, fetched_at_block) VALUES (?1, ?2, ?3)",
                params![addr_hex, ch_hex, block as i64],
            );
        }
    }

    // --- block_by_hash cache ---

    fn get_cached_block_number_by_hash(&self, hash: &Felt) -> Option<u64> {
        let db = self.db.lock().ok()?;
        let hash_hex = format!("{:#x}", hash);
        let mut stmt = db
            .prepare("SELECT number FROM block_hash_index WHERE hash = ?1")
            .ok()?;
        stmt.query_row(params![hash_hex], |row| {
            let n: i64 = row.get(0)?;
            Ok(n as u64)
        })
        .ok()
    }

    fn cache_block_hash(&self, hash: &Felt, number: u64) {
        if let Ok(db) = self.db.lock() {
            let hash_hex = format!("{:#x}", hash);
            let _ = db.execute(
                "INSERT OR REPLACE INTO block_hash_index (hash, number) VALUES (?1, ?2)",
                params![hash_hex, number as i64],
            );
        }
    }

    // --- deploy info cache ---

    fn get_cached_deploy_info(&self, address: &Felt) -> Option<(Felt, u64, Option<Felt>)> {
        let db = self.db.lock().ok()?;
        let addr_hex = format!("{:#x}", address);
        let mut stmt = db
            .prepare(
                "SELECT deploy_tx_hash, deploy_block, deployer FROM deploy_info WHERE address = ?1",
            )
            .ok()?;
        stmt.query_row(params![addr_hex], |row| {
            let tx_hex: String = row.get(0)?;
            let block: i64 = row.get(1)?;
            let deployer_hex: Option<String> = row.get(2)?;
            Ok((tx_hex, block as u64, deployer_hex))
        })
        .ok()
        .and_then(|(tx_hex, block, deployer_hex)| {
            let tx_hash = Felt::from_hex(&tx_hex).ok()?;
            let deployer = deployer_hex.and_then(|h| Felt::from_hex(&h).ok());
            Some((tx_hash, block, deployer))
        })
    }

    fn cache_deploy_info(
        &self,
        address: &Felt,
        tx_hash: &Felt,
        block: u64,
        deployer: Option<&Felt>,
    ) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let tx_hex = format!("{:#x}", tx_hash);
            let deployer_hex = deployer.map(|d| format!("{:#x}", d));
            let _ = db.execute(
                "INSERT OR REPLACE INTO deploy_info (address, deploy_tx_hash, deploy_block, deployer) VALUES (?1, ?2, ?3, ?4)",
                params![addr_hex, tx_hex, block as i64, deployer_hex],
            );
        }
    }

    // --- nonce cache ---

    fn get_cached_nonce_info(&self, address: &Felt) -> Option<(Felt, u64)> {
        let db = self.db.lock().ok()?;
        let addr_hex = format!("{:#x}", address);
        let mut stmt = db
            .prepare("SELECT nonce, block_number FROM cached_nonces WHERE address = ?1")
            .ok()?;
        stmt.query_row(params![addr_hex], |row| {
            let nonce_hex: String = row.get(0)?;
            let block: i64 = row.get(1)?;
            Ok((nonce_hex, block as u64))
        })
        .ok()
        .and_then(|(nonce_hex, block)| Felt::from_hex(&nonce_hex).ok().map(|f| (f, block)))
    }

    fn cache_nonce_info(&self, address: &Felt, nonce: &Felt, block: u64) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let nonce_hex = format!("{:#x}", nonce);
            let _ = db.execute(
                "INSERT OR REPLACE INTO cached_nonces (address, nonce, block_number) VALUES (?1, ?2, ?3)",
                params![addr_hex, nonce_hex, block as i64],
            );
        }
    }

    // --- search progress cache ---

    fn get_cached_search_progress(&self, address: &Felt) -> Option<(u64, u64)> {
        let db = self.db.lock().ok()?;
        let addr_hex = format!("{:#x}", address);
        let mut stmt = db
            .prepare("SELECT min_searched_block, max_searched_block FROM address_search_progress WHERE address = ?1")
            .ok()?;
        stmt.query_row(params![addr_hex], |row| {
            let min: i64 = row.get(0)?;
            let max: i64 = row.get(1)?;
            Ok((min as u64, max as u64))
        })
        .ok()
    }

    fn cache_search_progress(&self, address: &Felt, min_block: u64, max_block: u64) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            // Merge: expand existing range
            let existing = db
                .prepare("SELECT min_searched_block, max_searched_block FROM address_search_progress WHERE address = ?1")
                .ok()
                .and_then(|mut s| {
                    s.query_row(params![addr_hex], |row| {
                        let min: i64 = row.get(0)?;
                        let max: i64 = row.get(1)?;
                        Ok((min as u64, max as u64))
                    })
                    .ok()
                });
            let (final_min, final_max) = if let Some((old_min, old_max)) = existing {
                (old_min.min(min_block), old_max.max(max_block))
            } else {
                (min_block, max_block)
            };
            let _ = db.execute(
                "INSERT OR REPLACE INTO address_search_progress (address, min_searched_block, max_searched_block) VALUES (?1, ?2, ?3)",
                params![addr_hex, final_min as i64, final_max as i64],
            );
        }
    }

    // --- contract events cache ---

    fn load_contract_events(&self, address: &Felt) -> Vec<SnEvent> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return Vec::new(),
        };
        let addr_hex = format!("{:#x}", address);
        let mut stmt = match db
            .prepare("SELECT data FROM contract_events WHERE address = ?1 ORDER BY event_index")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map(params![addr_hex], |row| row.get::<_, String>(0)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .filter_map(|json| serde_json::from_str(&json).ok())
            .collect()
    }

    fn save_contract_events(&self, address: &Felt, events: &[SnEvent]) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let _ = db.execute(
                "DELETE FROM contract_events WHERE address = ?1",
                params![addr_hex],
            );
            for (i, event) in events.iter().enumerate() {
                if let Ok(json) = serde_json::to_string(event) {
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO contract_events (address, event_index, data) VALUES (?1, ?2, ?3)",
                        params![addr_hex, i as i64, json],
                    );
                }
            }
        }
    }
}

#[async_trait]
impl DataSource for CachingDataSource {
    async fn get_latest_block_number(&self) -> Result<u64> {
        // Always go to upstream for latest block number (it changes constantly)
        self.upstream.get_latest_block_number().await
    }

    async fn get_block(&self, number: u64) -> Result<SnBlock> {
        if let Some(block) = self.get_cached_block(number) {
            trace!(number, "cache hit: block");
            return Ok(block);
        }
        debug!(number, "cache miss: block, fetching from RPC");
        let block = self.upstream.get_block(number).await?;
        self.cache_block(&block);
        Ok(block)
    }

    async fn get_block_by_hash(&self, hash: Felt) -> Result<u64> {
        if let Some(number) = self.get_cached_block_number_by_hash(&hash) {
            trace!(hash = %format!("{:#x}", hash), "cache hit: block_by_hash");
            return Ok(number);
        }
        let number = self.upstream.get_block_by_hash(hash).await?;
        self.cache_block_hash(&hash, number);
        Ok(number)
    }

    async fn get_block_with_txs(&self, number: u64) -> Result<(SnBlock, Vec<SnTransaction>)> {
        if let Some(result) = self.get_cached_block_with_txs(number) {
            trace!(number, tx_count = result.1.len(), "cache hit: block+txs");
            return Ok(result);
        }
        debug!(number, "cache miss: block+txs, fetching from RPC");
        let (block, txs) = self.upstream.get_block_with_txs(number).await?;
        self.cache_block_with_txs(&block, &txs);
        Ok((block, txs))
    }

    async fn get_transaction(&self, hash: Felt) -> Result<SnTransaction> {
        if let Some(tx) = self.get_cached_transaction(hash) {
            trace!(tx_hash = %format!("{:#x}", hash), "cache hit: transaction");
            return Ok(tx);
        }

        // In-flight dedup: if another caller is already fetching this hash,
        // await their future instead of firing a second RPC.
        let fut = {
            let mut pending = self.pending_txs.lock().unwrap();
            if let Some(existing) = pending.get(&hash) {
                trace!(tx_hash = %format!("{:#x}", hash), "dedup: joining in-flight transaction fetch");
                existing.clone()
            } else {
                debug!(tx_hash = %format!("{:#x}", hash), "cache miss: transaction, fetching from RPC");
                let upstream = Arc::clone(&self.upstream);
                let fut: Pin<
                    Box<dyn Future<Output = std::result::Result<SnTransaction, String>> + Send>,
                > = Box::pin(async move {
                    upstream
                        .get_transaction(hash)
                        .await
                        .map_err(|e| e.to_string())
                });
                let shared = fut.shared();
                pending.insert(hash, shared.clone());
                shared
            }
        };

        let result = fut.await;

        // Leader cleanup: whichever task observes it first removes the entry
        // so a later miss can start a fresh fetch.
        {
            let mut pending = self.pending_txs.lock().unwrap();
            pending.remove(&hash);
        }

        match result {
            Ok(tx) => {
                self.cache_transaction(&tx);
                Ok(tx)
            }
            Err(e) => Err(SnbeatError::Rpc(e)),
        }
    }

    async fn get_receipt(&self, hash: Felt) -> Result<SnReceipt> {
        if let Some(receipt) = self.get_cached_receipt(hash) {
            trace!(tx_hash = %format!("{:#x}", hash), "cache hit: receipt");
            return Ok(receipt);
        }

        let fut = {
            let mut pending = self.pending_receipts.lock().unwrap();
            if let Some(existing) = pending.get(&hash) {
                trace!(tx_hash = %format!("{:#x}", hash), "dedup: joining in-flight receipt fetch");
                existing.clone()
            } else {
                debug!(tx_hash = %format!("{:#x}", hash), "cache miss: receipt, fetching from RPC");
                let upstream = Arc::clone(&self.upstream);
                let fut: Pin<
                    Box<dyn Future<Output = std::result::Result<SnReceipt, String>> + Send>,
                > = Box::pin(
                    async move { upstream.get_receipt(hash).await.map_err(|e| e.to_string()) },
                );
                let shared = fut.shared();
                pending.insert(hash, shared.clone());
                shared
            }
        };

        let result = fut.await;

        {
            let mut pending = self.pending_receipts.lock().unwrap();
            pending.remove(&hash);
        }

        match result {
            Ok(receipt) => {
                self.cache_receipt(&receipt);
                Ok(receipt)
            }
            Err(e) => Err(SnbeatError::Rpc(e)),
        }
    }

    async fn get_nonce(&self, address: Felt) -> Result<Felt> {
        // Nonces change — always go upstream
        self.upstream.get_nonce(address).await
    }

    async fn get_class_hash(&self, address: Felt) -> Result<Felt> {
        // Class hash is mostly stable but CAN change via replace_class syscall.
        // Cache with the block at which we fetched it; refetch if stale (>1000 blocks).
        const CLASS_HASH_STALE_BLOCKS: u64 = 1000;
        if let Some((class_hash, fetched_at)) = self.get_cached_class_hash(&address) {
            let latest = self.upstream.get_latest_block_number().await.unwrap_or(0);
            if latest.saturating_sub(fetched_at) < CLASS_HASH_STALE_BLOCKS {
                trace!(address = %format!("{:#x}", address), "cache hit: class_hash");
                return Ok(class_hash);
            }
            debug!(address = %format!("{:#x}", address), age = latest - fetched_at, "class_hash cache stale, refetching");
        }
        let class_hash = self.upstream.get_class_hash(address).await?;
        let block = self.upstream.get_latest_block_number().await.unwrap_or(0);
        self.cache_class_hash(&address, &class_hash, block);
        Ok(class_hash)
    }

    async fn get_class(&self, class_hash: Felt) -> Result<ContractClass> {
        // Classes are large — pass through to upstream.
        // Parsed ABIs are cached separately via the decode layer's class_cache.
        debug!(class_hash = %format!("{:#x}", class_hash), "Fetching class from RPC (not cached — parsed ABI cached separately)");
        self.upstream.get_class(class_hash).await
    }

    async fn get_events_for_address(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SnEvent>> {
        // Bounded (paginating into history) — bypass cache merge and pass through.
        // The cache only accelerates the "newest events" path; old-window fetches
        // shouldn't poison the cache with partial windows.
        if to_block.is_some() {
            return self
                .upstream
                .get_events_for_address(address, from_block, to_block, limit)
                .await;
        }

        // Load cached events
        let cached = self.load_address_events(&address);

        // Incremental: fetch events newer than our newest cached event
        let fetch_from = if !cached.is_empty() && from_block.is_none() {
            // Start from the block AFTER our newest cached event to avoid duplicates
            let max_block = cached.iter().map(|e| e.block_number).max().unwrap_or(0);
            Some(max_block + 1)
        } else {
            from_block
        };

        // Fetch from upstream
        let new_events = self
            .upstream
            .get_events_for_address(address, fetch_from, None, limit)
            .await
            .unwrap_or_default();

        // Merge: new + cached (new events are newest-first, cached are newest-first)
        let mut merged = new_events;
        for event in cached {
            let exists = merged.iter().any(|e| {
                e.transaction_hash == event.transaction_hash
                    && e.block_number == event.block_number
                    && e.event_index == event.event_index
            });
            if !exists {
                merged.push(event);
            }
        }

        // Sort by block number descending (newest first)
        merged.sort_by(|a, b| b.block_number.cmp(&a.block_number));

        // Cache the merged result
        self.save_address_events(&address, &merged);

        Ok(merged)
    }

    fn load_cached_address_txs(&self, address: &Felt) -> Vec<AddressTxSummary> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return Vec::new(),
        };
        let addr_hex = format!("{:#x}", address);
        let mut stmt =
            match db.prepare("SELECT data FROM address_txs WHERE address = ?1 ORDER BY tx_index") {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
        let rows = match stmt.query_map(params![addr_hex], |row| row.get::<_, String>(0)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .filter_map(|json| serde_json::from_str(&json).ok())
            .collect()
    }

    fn save_address_txs(&self, address: &Felt, txs: &[AddressTxSummary]) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let _ = db.execute(
                "DELETE FROM address_txs WHERE address = ?1",
                params![addr_hex],
            );
            for (i, tx) in txs.iter().enumerate() {
                if let Ok(json) = serde_json::to_string(tx) {
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO address_txs (address, tx_index, data) VALUES (?1, ?2, ?3)",
                        params![addr_hex, i as i64, json],
                    );
                }
            }
        }
    }

    fn load_cached_address_calls(&self, address: &Felt) -> Vec<ContractCallSummary> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return Vec::new(),
        };
        let addr_hex = format!("{:#x}", address);
        let mut stmt = match db
            .prepare("SELECT data FROM address_calls WHERE address = ?1 ORDER BY call_index")
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map(params![addr_hex], |row| row.get::<_, String>(0)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .filter_map(|json| serde_json::from_str(&json).ok())
            .collect()
    }

    fn save_address_calls(&self, address: &Felt, calls: &[ContractCallSummary]) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let _ = db.execute(
                "DELETE FROM address_calls WHERE address = ?1",
                params![addr_hex],
            );
            for (i, call) in calls.iter().enumerate() {
                if let Ok(json) = serde_json::to_string(call) {
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO address_calls (address, call_index, data) VALUES (?1, ?2, ?3)",
                        params![addr_hex, i as i64, json],
                    );
                }
            }
        }
    }

    fn load_cached_meta_txs(&self, address: &Felt) -> Vec<MetaTxIntenderSummary> {
        let db = match self.db.lock() {
            Ok(db) => db,
            Err(_) => return Vec::new(),
        };
        let addr_hex = format!("{:#x}", address);
        let mut stmt = match db.prepare(
            "SELECT data FROM address_meta_txs WHERE address = ?1 ORDER BY block_number DESC",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = match stmt.query_map(params![addr_hex], |row| row.get::<_, String>(0)) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.filter_map(|r| r.ok())
            .filter_map(|json| serde_json::from_str(&json).ok())
            .collect()
    }

    fn save_meta_txs(&self, address: &Felt, txs: &[MetaTxIntenderSummary]) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            for tx in txs {
                if let Ok(json) = serde_json::to_string(tx) {
                    let hash_hex = format!("{:#x}", tx.hash);
                    let _ = db.execute(
                        "INSERT OR REPLACE INTO address_meta_txs \
                         (address, tx_hash, block_number, data) VALUES (?1, ?2, ?3, ?4)",
                        params![addr_hex, hash_hex, tx.block_number as i64, json],
                    );
                }
            }
        }
    }

    fn load_cached_activity_range(&self, address: &Felt) -> Option<(u64, u64)> {
        self.load_cached_activity_range_with_count(address)
            .map(|(min, max, _count)| (min, max))
    }

    fn load_cached_activity_range_with_count(&self, address: &Felt) -> Option<(u64, u64, u64)> {
        let db = self.db.lock().ok()?;
        let addr_hex = format!("{:#x}", address);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs() as i64;
        // Activity range is considered fresh for 1 hour
        let cutoff = now - 3600;
        let mut stmt = db
            .prepare(
                "SELECT min_block, max_block, event_count FROM address_activity \
                 WHERE address = ?1 AND updated_at > ?2",
            )
            .ok()?;
        stmt.query_row(params![addr_hex, cutoff], |row| {
            let min: i64 = row.get(0)?;
            let max: i64 = row.get(1)?;
            let count: i64 = row.get(2)?;
            Ok((min as u64, max as u64, count as u64))
        })
        .ok()
    }

    fn save_activity_range(&self, address: &Felt, min_block: u64, max_block: u64) {
        self.save_activity_range_with_count(address, min_block, max_block, 0);
    }

    fn save_activity_range_with_count(
        &self,
        address: &Felt,
        min_block: u64,
        max_block: u64,
        event_count: u64,
    ) {
        if let Ok(db) = self.db.lock() {
            let addr_hex = format!("{:#x}", address);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            // Merge: expand existing range, keep max event count
            let existing = db
                .prepare(
                    "SELECT min_block, max_block, event_count FROM address_activity WHERE address = ?1",
                )
                .ok()
                .and_then(|mut s| {
                    s.query_row(params![addr_hex], |row| {
                        let min: i64 = row.get(0)?;
                        let max: i64 = row.get(1)?;
                        let count: i64 = row.get(2)?;
                        Ok((min as u64, max as u64, count as u64))
                    })
                    .ok()
                });
            let (final_min, final_max, final_count) =
                if let Some((old_min, old_max, old_count)) = existing {
                    (
                        old_min.min(min_block),
                        old_max.max(max_block),
                        old_count.max(event_count),
                    )
                } else {
                    (min_block, max_block, event_count)
                };
            let _ = db.execute(
                "INSERT OR REPLACE INTO address_activity (address, min_block, max_block, event_count, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![addr_hex, final_min as i64, final_max as i64, final_count as i64, now],
            );
        }
    }

    async fn call_contract(
        &self,
        contract_address: Felt,
        selector: Felt,
        calldata: Vec<Felt>,
    ) -> Result<Vec<Felt>> {
        // Contract calls are dynamic (balances change) — pass through
        self.upstream
            .call_contract(contract_address, selector, calldata)
            .await
    }

    async fn batch_call_contracts(
        &self,
        calls: Vec<(Felt, Felt, Vec<Felt>)>,
    ) -> Vec<Result<Vec<Felt>>> {
        // Pass through so the upstream's batched implementation is used —
        // the default trait impl would silently run each call serially.
        self.upstream.batch_call_contracts(calls).await
    }

    async fn get_contract_events(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SnEvent>> {
        // Bounded pagination: skip cache merge (see `get_events_for_address`).
        if to_block.is_some() {
            return self
                .upstream
                .get_contract_events(address, from_block, to_block, limit)
                .await;
        }

        // Incremental caching for contract events (all events, no key filter)
        let cached = self.load_contract_events(&address);

        let fetch_from = if !cached.is_empty() && from_block.is_none() {
            let max_block = cached.iter().map(|e| e.block_number).max().unwrap_or(0);
            Some(max_block + 1)
        } else {
            from_block
        };

        let new_events = match self
            .upstream
            .get_contract_events(address, fetch_from, None, limit)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!(address = %format!("{:#x}", address), error = %e, "RPC error fetching events, using cache only");
                if cached.is_empty() {
                    return Err(e);
                }
                vec![]
            }
        };

        let mut merged = new_events;
        for event in cached {
            let exists = merged.iter().any(|e| {
                e.transaction_hash == event.transaction_hash
                    && e.block_number == event.block_number
                    && e.event_index == event.event_index
            });
            if !exists {
                merged.push(event);
            }
        }

        merged.sort_by(|a, b| b.block_number.cmp(&a.block_number));
        self.save_contract_events(&address, &merged);
        merged.truncate(limit);

        Ok(merged)
    }

    fn load_cached_deploy_info(&self, address: &Felt) -> Option<(Felt, u64, Option<Felt>)> {
        self.get_cached_deploy_info(address)
    }

    fn save_deploy_info(
        &self,
        address: &Felt,
        tx_hash: &Felt,
        block: u64,
        deployer: Option<&Felt>,
    ) {
        self.cache_deploy_info(address, tx_hash, block, deployer);
    }

    fn load_cached_nonce(&self, address: &Felt) -> Option<(Felt, u64)> {
        self.get_cached_nonce_info(address)
    }

    fn save_cached_nonce(&self, address: &Felt, nonce: &Felt, block: u64) {
        self.cache_nonce_info(address, nonce, block);
    }

    fn load_search_progress(&self, address: &Felt) -> Option<(u64, u64)> {
        self.get_cached_search_progress(address)
    }

    fn save_search_progress(&self, address: &Felt, min_block: u64, max_block: u64) {
        self.cache_search_progress(address, min_block, max_block);
    }

    async fn get_recent_blocks(&self, count: usize) -> Result<Vec<SnBlock>> {
        // Fetch from upstream (recent blocks change)
        let blocks = self.upstream.get_recent_blocks(count).await?;
        // Cache each block
        for block in &blocks {
            self.cache_block(block);
        }
        Ok(blocks)
    }

    fn load_cached_class_declaration(&self, class_hash: &Felt) -> Option<ClassDeclareInfo> {
        let db = self.db.lock().ok()?;
        let hash_hex = format!("{:#x}", class_hash);
        let mut stmt = db
            .prepare(
                "SELECT tx_hash, sender, block_number, timestamp \
                 FROM class_declarations WHERE class_hash = ?1",
            )
            .ok()?;
        stmt.query_row(params![hash_hex], |row| {
            let tx_hash_s: String = row.get(0)?;
            let sender_s: String = row.get(1)?;
            let block_number: i64 = row.get(2)?;
            let timestamp: i64 = row.get(3)?;
            Ok((tx_hash_s, sender_s, block_number as u64, timestamp as u64))
        })
        .ok()
        .and_then(|(tx_hash_s, sender_s, block_number, timestamp)| {
            let tx_hash = Felt::from_hex(&tx_hash_s).ok()?;
            let sender = Felt::from_hex(&sender_s).ok()?;
            Some(ClassDeclareInfo {
                tx_hash,
                sender,
                block_number,
                timestamp,
            })
        })
    }

    fn save_class_declaration(&self, class_hash: &Felt, info: &ClassDeclareInfo) {
        if let Ok(db) = self.db.lock() {
            let hash_hex = format!("{:#x}", class_hash);
            let tx_hex = format!("{:#x}", info.tx_hash);
            let sender_hex = format!("{:#x}", info.sender);
            let _ = db.execute(
                "INSERT OR REPLACE INTO class_declarations \
                 (class_hash, tx_hash, sender, block_number, timestamp) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    hash_hex,
                    tx_hex,
                    sender_hex,
                    info.block_number as i64,
                    info.timestamp as i64,
                ],
            );
        }
    }

    fn load_cached_class_contracts(&self, class_hash: &Felt) -> Option<Vec<ClassContractEntry>> {
        let db = self.db.lock().ok()?;
        let hash_hex = format!("{:#x}", class_hash);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs() as i64;
        // Contracts-by-class is fresh for 1 hour; stale entries return None so
        // the caller re-fetches. Without this TTL the list would miss new
        // deploys performed against the class after the first cache write.
        let cutoff = now - 3600;
        let fetched_at: i64 = db
            .prepare("SELECT fetched_at FROM class_contracts_meta WHERE class_hash = ?1")
            .ok()?
            .query_row(params![&hash_hex], |row| row.get(0))
            .ok()?;
        if fetched_at <= cutoff {
            return None;
        }
        let mut stmt = db
            .prepare(
                "SELECT contract_address, block_number FROM class_contracts \
                 WHERE class_hash = ?1 ORDER BY block_number DESC",
            )
            .ok()?;
        let rows = stmt
            .query_map(params![hash_hex], |row| {
                let addr_s: String = row.get(0)?;
                let block_number: i64 = row.get(1)?;
                Ok((addr_s, block_number as u64))
            })
            .ok()?;
        let mut out = Vec::new();
        for row in rows.flatten() {
            if let Ok(address) = Felt::from_hex(&row.0) {
                out.push(ClassContractEntry {
                    address,
                    block_number: row.1,
                });
            }
        }
        Some(out)
    }

    fn save_class_contracts(&self, class_hash: &Felt, contracts: &[ClassContractEntry]) {
        if let Ok(mut db) = self.db.lock() {
            let hash_hex = format!("{:#x}", class_hash);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let tx = match db.transaction() {
                Ok(t) => t,
                Err(e) => {
                    warn!(error = %e, "save_class_contracts: begin transaction failed");
                    return;
                }
            };
            // Replace: drop prior rows, insert the new list atomically.
            let _ = tx.execute(
                "DELETE FROM class_contracts WHERE class_hash = ?1",
                params![hash_hex],
            );
            for entry in contracts {
                let contract_hex = format!("{:#x}", entry.address);
                let _ = tx.execute(
                    "INSERT OR REPLACE INTO class_contracts \
                     (class_hash, contract_address, block_number) VALUES (?1, ?2, ?3)",
                    params![hash_hex, contract_hex, entry.block_number as i64],
                );
            }
            let _ = tx.execute(
                "INSERT OR REPLACE INTO class_contracts_meta (class_hash, fetched_at) \
                 VALUES (?1, ?2)",
                params![hash_hex, now],
            );
            if let Err(e) = tx.commit() {
                warn!(error = %e, "save_class_contracts: commit failed");
            }
        }
    }
}
