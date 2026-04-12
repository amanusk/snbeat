use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{Connection, params};
use starknet::core::types::{ContractClass, Felt};
use tracing::{debug, trace};

use super::DataSource;
#[allow(unused_imports)]
use super::types::*;
use crate::error::{Result, SnbeatError};

/// Persistent cache backed by SQLite + in-memory LRU.
/// Wraps any DataSource: checks cache first, fetches from upstream on miss,
/// writes through to cache on fetch. Persists across restarts.
pub struct CachingDataSource {
    upstream: Box<dyn DataSource>,
    db: Mutex<Connection>,
}

impl CachingDataSource {
    pub fn new(upstream: Box<dyn DataSource>, cache_path: &Path) -> Result<Self> {
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
            ",
        )
        .map_err(|e| SnbeatError::Config(format!("Failed to init cache schema: {e}")))?;

        Ok(Self {
            upstream,
            db: Mutex::new(db),
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
        self.upstream.get_block_by_hash(hash).await
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
        debug!(tx_hash = %format!("{:#x}", hash), "cache miss: transaction, fetching from RPC");
        let tx = self.upstream.get_transaction(hash).await?;
        self.cache_transaction(&tx);
        Ok(tx)
    }

    async fn get_receipt(&self, hash: Felt) -> Result<SnReceipt> {
        if let Some(receipt) = self.get_cached_receipt(hash) {
            trace!(tx_hash = %format!("{:#x}", hash), "cache hit: receipt");
            return Ok(receipt);
        }
        debug!(tx_hash = %format!("{:#x}", hash), "cache miss: receipt, fetching from RPC");
        let receipt = self.upstream.get_receipt(hash).await?;
        self.cache_receipt(&receipt);
        Ok(receipt)
    }

    async fn get_nonce(&self, address: Felt) -> Result<Felt> {
        // Nonces change — always go upstream
        self.upstream.get_nonce(address).await
    }

    async fn get_class_hash(&self, address: Felt) -> Result<Felt> {
        // Class hash is immutable once deployed — could cache, but for now upstream
        self.upstream.get_class_hash(address).await
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
        limit: usize,
    ) -> Result<Vec<SnEvent>> {
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
            .get_events_for_address(address, fetch_from, limit)
            .await
            .unwrap_or_default();

        // Merge: new + cached (new events are newest-first, cached are newest-first)
        let mut merged = new_events;
        for event in cached {
            let exists = merged.iter().any(|e| {
                e.transaction_hash == event.transaction_hash && e.block_number == event.block_number
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

    async fn get_recent_blocks(&self, count: usize) -> Result<Vec<SnBlock>> {
        // Fetch from upstream (recent blocks change)
        let blocks = self.upstream.get_recent_blocks(count).await?;
        // Cache each block
        for block in &blocks {
            self.cache_block(block);
        }
        Ok(blocks)
    }
}
