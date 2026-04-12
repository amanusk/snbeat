use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use rusqlite::{Connection, params};
use starknet::core::types::Felt;
use tracing::{debug, trace};

use super::abi::ParsedAbi;

/// Two-tier cache: in-memory LRU + persistent SQLite.
/// Parsed ABIs are expensive to recompute (requires RPC fetch + parsing),
/// so we persist them across restarts.
/// Also caches selector→name mappings (deterministic, never changes).
pub struct ClassCache {
    memory: Mutex<LruCache<Felt, Arc<ParsedAbi>>>,
    /// Persistent selector→name cache: selector hex → function/event name.
    /// Selectors are deterministic hashes — once resolved, always valid.
    selector_names: Mutex<HashMap<Felt, String>>,
    db: Mutex<Connection>,
}

impl ClassCache {
    pub fn new(db: Connection, memory_capacity: usize) -> Self {
        // Create selector_names table if not exists
        let _ = db.execute_batch(
            "CREATE TABLE IF NOT EXISTS selector_names (
                selector TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );",
        );

        // Preload selector names into memory
        let mut selector_names = HashMap::new();
        if let Ok(mut stmt) = db.prepare("SELECT selector, name FROM selector_names") {
            if let Ok(rows) = stmt.query_map([], |row| {
                let selector_hex: String = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((selector_hex, name))
            }) {
                for row in rows.flatten() {
                    if let Ok(felt) = Felt::from_hex(&row.0) {
                        selector_names.insert(felt, row.1);
                    }
                }
            }
        }
        debug!(
            count = selector_names.len(),
            "Loaded selector names from DB"
        );

        Self {
            memory: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(memory_capacity).unwrap(),
            )),
            selector_names: Mutex::new(selector_names),
            db: Mutex::new(db),
        }
    }

    /// Look up a parsed ABI by class hash.
    pub fn get(&self, class_hash: &Felt) -> Option<Arc<ParsedAbi>> {
        // Check memory first
        if let Ok(mut mem) = self.memory.lock() {
            if let Some(abi) = mem.get(class_hash) {
                trace!(class_hash = %format!("{:#x}", class_hash), "ABI cache hit (memory)");
                return Some(Arc::clone(abi));
            }
        }

        // Check SQLite
        let abi = self.load_from_db(class_hash)?;
        let arc = Arc::new(abi);

        // Promote to memory cache
        if let Ok(mut mem) = self.memory.lock() {
            mem.put(*class_hash, Arc::clone(&arc));
        }

        debug!(class_hash = %format!("{:#x}", class_hash), "ABI cache hit (disk)");
        Some(arc)
    }

    /// Store a parsed ABI in both memory and SQLite.
    pub fn put(&self, class_hash: Felt, abi: Arc<ParsedAbi>) {
        // Store in memory
        if let Ok(mut mem) = self.memory.lock() {
            mem.put(class_hash, Arc::clone(&abi));
        }

        // Store in SQLite
        self.save_to_db(&class_hash, &abi);
        debug!(class_hash = %format!("{:#x}", class_hash), "ABI cached (memory + disk)");
    }

    /// Look up a selector name. Returns instantly from memory.
    pub fn get_selector_name(&self, selector: &Felt) -> Option<String> {
        self.selector_names.lock().ok()?.get(selector).cloned()
    }

    /// Store a selector→name mapping (persistent + memory).
    pub fn put_selector_name(&self, selector: Felt, name: String) {
        if let Ok(mut map) = self.selector_names.lock() {
            if map.contains_key(&selector) {
                return; // Already known
            }
            map.insert(selector, name.clone());
        }
        if let Ok(db) = self.db.lock() {
            let hex = format!("{:#x}", selector);
            let _ = db.execute(
                "INSERT OR IGNORE INTO selector_names (selector, name) VALUES (?1, ?2)",
                params![hex, name],
            );
        }
    }

    /// Bulk insert selector names from a parsed ABI.
    pub fn index_abi_selectors(&self, abi: &ParsedAbi) {
        for (key, func) in &abi.functions {
            self.put_selector_name(key.0, func.name.clone());
        }
        for (key, event) in &abi.events {
            self.put_selector_name(key.0, event.name.clone());
        }
    }

    fn load_from_db(&self, class_hash: &Felt) -> Option<ParsedAbi> {
        let db = self.db.lock().ok()?;
        let hash_hex = format!("{:#x}", class_hash);
        let mut stmt = db
            .prepare("SELECT data FROM parsed_abis WHERE class_hash = ?1")
            .ok()?;
        let json: String = stmt.query_row(params![hash_hex], |row| row.get(0)).ok()?;
        serde_json::from_str(&json).ok()
    }

    fn save_to_db(&self, class_hash: &Felt, abi: &ParsedAbi) {
        if let Ok(json) = serde_json::to_string(abi) {
            if let Ok(db) = self.db.lock() {
                let hash_hex = format!("{:#x}", class_hash);
                let _ = db.execute(
                    "INSERT OR REPLACE INTO parsed_abis (class_hash, data) VALUES (?1, ?2)",
                    params![hash_hex, json],
                );
            }
        }
    }
}
