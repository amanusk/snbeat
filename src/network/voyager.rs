use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, info, warn};

use crate::data::types::VoyagerLabelInfo;

const VOYAGER_API_BASE: &str = "https://api.voyager.online/beta";
/// Cache TTL: 24 hours (labels rarely change)
const CACHE_TTL_SECS: u64 = 86_400;

/// Voyager API client for fetching contract metadata (labels, types).
/// Wraps a SQLite cache to avoid redundant API calls across restarts.
pub struct VoyagerClient {
    client: reqwest::Client,
    api_key: String,
    db: Mutex<Connection>,
}

/// Raw API response shape — all fields optional to be robust to schema changes.
/// Voyager uses:
/// - `contractAlias` for labelled accounts/contracts (e.g. "Binance: Hot Wallet")
/// - `classAlias`    for the contract class name (e.g. "Ready", "OpenZeppelin Account")
/// - `tokenName`     for ERC-20 tokens
/// - `type`          for the contract type (mirrors classAlias)
#[derive(Debug, Deserialize)]
struct VoyagerContractResponse {
    /// Primary display label for labelled contracts/accounts
    #[serde(rename = "contractAlias")]
    contract_alias: Option<String>,
    /// Token name for ERC-20 contracts
    #[serde(rename = "tokenName")]
    token_name: Option<String>,
    /// Human-readable class name (e.g. "OpenZeppelin Account")
    #[serde(rename = "classAlias")]
    class_alias: Option<String>,
    #[serde(rename = "type")]
    contract_type: Option<String>,
    /// Block number where the contract was deployed.
    #[serde(rename = "blockNumber")]
    block_number: Option<u64>,
}

impl VoyagerContractResponse {
    /// Return the best available display label, in priority order.
    fn best_label(&self) -> Option<String> {
        [self.contract_alias.as_deref(), self.token_name.as_deref()]
            .into_iter()
            .flatten()
            .find(|s| !s.is_empty())
            .map(str::to_owned)
    }

    /// Return the best available type description.
    /// Prefer classAlias (e.g. "OpenZeppelin Account") over the raw type field.
    fn best_type(&self) -> Option<String> {
        [self.class_alias.as_deref(), self.contract_type.as_deref()]
            .into_iter()
            .flatten()
            .find(|s| !s.is_empty())
            .map(str::to_owned)
    }
}

impl VoyagerClient {
    /// Create a new client. Opens (or creates) the voyager_labels table in `cache_db_path`.
    pub fn new(api_key: String, cache_db_path: &Path) -> Result<Self, String> {
        let db = Connection::open(cache_db_path)
            .map_err(|e| format!("Failed to open voyager cache db: {e}"))?;

        // Enable WAL for concurrent access with the main cache connection
        db.execute_batch("PRAGMA journal_mode=WAL;")
            .map_err(|e| format!("Failed to set WAL mode: {e}"))?;

        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS voyager_labels (
                address      TEXT PRIMARY KEY,
                name         TEXT,
                contract_type TEXT,
                fetched_at   INTEGER NOT NULL,
                deploy_block INTEGER
            );",
        )
        .map_err(|e| format!("Failed to init voyager_labels table: {e}"))?;

        Ok(Self {
            client: reqwest::Client::new(),
            api_key,
            db: Mutex::new(db),
        })
    }

    /// Get the label for an address, reading from cache when available.
    /// On cache miss, fetches from the Voyager API and persists the result.
    pub async fn get_label(&self, address: Felt) -> Result<VoyagerLabelInfo, String> {
        let addr_hex = format!("{:#066x}", address);

        if let Some(cached) = self.get_cached(&addr_hex) {
            debug!(address = %addr_hex, "Voyager label cache hit");
            return Ok(cached);
        }

        let label = self.fetch_from_api(&addr_hex).await?;
        self.store_cached(&addr_hex, &label);
        Ok(label)
    }

    /// Probe connectivity — fetches a known contract (ETH token) to verify the key works.
    pub async fn health_check(&self) -> Result<(), String> {
        // Use ETH token address as the canary — it's always deployed and labelled on Voyager
        let url = format!(
            "{}/contracts/0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
            VOYAGER_API_BASE
        );
        let resp = self
            .client
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Voyager unreachable: {e}"))?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("Voyager returned HTTP {}", resp.status()))
        }
    }

    /// Load all cached labels that have a name (for seeding the search index on startup).
    /// Returns (address, name) pairs — ignores entries without a name.
    pub fn load_all_cached_labels(&self) -> Vec<(Felt, String)> {
        let Ok(db) = self.db.lock() else {
            return Vec::new();
        };
        let mut stmt = match db.prepare(
            "SELECT address, name FROM voyager_labels WHERE name IS NOT NULL AND name != ''",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt
            .query_map([], |row| {
                let addr_hex: String = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((addr_hex, name))
            })
            .ok();
        let Some(rows) = rows else {
            return Vec::new();
        };
        rows.filter_map(|r| {
            let (addr_hex, name) = r.ok()?;
            let felt = Felt::from_hex(&addr_hex).ok()?;
            Some((felt, name))
        })
        .collect()
    }

    // --- private helpers ---

    fn get_cached(&self, addr_hex: &str) -> Option<VoyagerLabelInfo> {
        let db = self.db.lock().ok()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let cutoff = now.saturating_sub(CACHE_TTL_SECS) as i64;

        let mut stmt = db
            .prepare(
                "SELECT name, contract_type, deploy_block FROM voyager_labels \
                 WHERE address = ?1 AND fetched_at > ?2",
            )
            .ok()?;

        stmt.query_row(params![addr_hex, cutoff], |row| {
            let deploy_block: Option<i64> = row.get(2)?;
            Ok(VoyagerLabelInfo {
                name: row.get(0)?,
                class_alias: row.get(1)?,
                deploy_block: deploy_block.map(|b| b as u64),
            })
        })
        .ok()
    }

    fn store_cached(&self, addr_hex: &str, label: &VoyagerLabelInfo) {
        let Ok(db) = self.db.lock() else { return };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let deploy_block = label.deploy_block.map(|b| b as i64);
        let _ = db.execute(
            "INSERT OR REPLACE INTO voyager_labels (address, name, contract_type, fetched_at, deploy_block) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![addr_hex, label.name, label.class_alias, now, deploy_block],
        );
    }

    async fn fetch_from_api(&self, addr_hex: &str) -> Result<VoyagerLabelInfo, String> {
        let url = format!("{}/contracts/{}", VOYAGER_API_BASE, addr_hex);
        debug!(address = %addr_hex, "Fetching Voyager contract info");

        let resp = self
            .client
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Voyager request failed: {e}"))?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            // 404 means the address is not a known contract on Voyager — not an error.
            return Ok(VoyagerLabelInfo {
                name: None,
                class_alias: None,
                deploy_block: None,
            });
        }
        if !status.is_success() {
            return Err(format!("Voyager HTTP {status}"));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| format!("Voyager read body failed: {e}"))?;

        let contract: VoyagerContractResponse =
            serde_json::from_str(&body).map_err(|e| format!("Voyager parse failed: {e}"))?;

        let label_name = contract.best_label();
        let label_type = contract.best_type();
        let deploy_block = contract.block_number;

        info!(
            address = %addr_hex,
            name = ?label_name,
            class_alias = ?label_type,
            deploy_block = ?deploy_block,
            "Voyager label fetched"
        );

        Ok(VoyagerLabelInfo {
            name: label_name,
            class_alias: label_type,
            deploy_block,
        })
    }
}

/// Directly insert a label into the cache (used in tests to pre-populate).
#[cfg(test)]
pub fn insert_label_for_test(
    client: &VoyagerClient,
    addr_hex: &str,
    label: &VoyagerLabelInfo,
    fetched_at: i64,
) {
    let db = client.db.lock().unwrap();
    let deploy_block = label.deploy_block.map(|b| b as i64);
    db.execute(
        "INSERT OR REPLACE INTO voyager_labels (address, name, contract_type, fetched_at, deploy_block) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![addr_hex, label.name, label.class_alias, fetched_at, deploy_block],
    )
    .unwrap();
}

/// Fetch the Voyager label for `address` in a background task and send the result
/// as an `Action::VoyagerLabelLoaded` on `tx`.
pub async fn fetch_and_send_label(
    address: Felt,
    client: &VoyagerClient,
    tx: &tokio::sync::mpsc::UnboundedSender<crate::app::actions::Action>,
) {
    match client.get_label(address).await {
        Ok(label) => {
            // Only emit an action if Voyager actually has a label for this address
            if label.name.is_some() {
                let _ = tx.send(crate::app::actions::Action::VoyagerLabelLoaded { address, label });
            }
        }
        Err(e) => {
            warn!(error = %e, address = %format!("{:#x}", address), "Voyager label fetch failed");
            let _ = tx.send(crate::app::actions::Action::SourceUpdate {
                source: crate::app::actions::Source::Voyager,
                status: crate::app::state::SourceStatus::FetchError(e),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use starknet::core::types::Felt;
    use tempfile::NamedTempFile;
    use tokio::sync::mpsc;

    use super::*;
    use crate::data::types::VoyagerLabelInfo;

    fn tmp_client() -> (VoyagerClient, NamedTempFile) {
        let f = NamedTempFile::new().unwrap();
        let client = VoyagerClient::new("test-key".into(), f.path()).unwrap();
        (client, f)
    }

    fn now_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    // --- best_label() priority ---

    #[test]
    fn best_label_uses_contract_alias() {
        let r = VoyagerContractResponse {
            contract_alias: Some("Binance: Hot Wallet".into()),
            token_name: Some("Some Token".into()),
            class_alias: Some("OpenZeppelin Account".into()),
            contract_type: Some("Ready".into()),
            block_number: None,
        };
        assert_eq!(r.best_label().as_deref(), Some("Binance: Hot Wallet"));
    }

    #[test]
    fn best_label_falls_back_to_token_name() {
        let r = VoyagerContractResponse {
            contract_alias: None,
            token_name: Some("Ether".into()),
            class_alias: Some("ERC20".into()),
            contract_type: Some("ERC20".into()),
            block_number: None,
        };
        assert_eq!(r.best_label().as_deref(), Some("Ether"));
    }

    #[test]
    fn best_label_returns_none_when_all_empty() {
        let r = VoyagerContractResponse {
            contract_alias: None,
            token_name: None,
            class_alias: None,
            contract_type: None,
            block_number: None,
        };
        assert!(r.best_label().is_none());
    }

    #[test]
    fn best_label_skips_empty_strings() {
        let r = VoyagerContractResponse {
            contract_alias: Some("".into()),
            token_name: Some("Ether".into()),
            class_alias: None,
            contract_type: None,
            block_number: None,
        };
        // empty contractAlias skipped, tokenName used
        assert_eq!(r.best_label().as_deref(), Some("Ether"));
    }

    #[test]
    fn best_type_prefers_class_alias() {
        let r = VoyagerContractResponse {
            contract_alias: None,
            token_name: None,
            class_alias: Some("OpenZeppelin Account".into()),
            contract_type: Some("Ready".into()),
            block_number: None,
        };
        assert_eq!(r.best_type().as_deref(), Some("OpenZeppelin Account"));
    }

    #[test]
    fn best_type_falls_back_to_contract_type() {
        let r = VoyagerContractResponse {
            contract_alias: None,
            token_name: None,
            class_alias: None,
            contract_type: Some("ERC20".into()),
            block_number: None,
        };
        assert_eq!(r.best_type().as_deref(), Some("ERC20"));
    }

    // --- cache round-trip ---

    #[test]
    fn cache_hit_within_ttl() {
        let (client, _f) = tmp_client();
        let addr = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let label = VoyagerLabelInfo {
            name: Some("Test Label".into()),
            class_alias: Some("Account".into()),
            deploy_block: None,
        };
        insert_label_for_test(&client, addr, &label, now_secs());

        let cached = client.get_cached(addr).unwrap();
        assert_eq!(cached.name.as_deref(), Some("Test Label"));
        assert_eq!(cached.class_alias.as_deref(), Some("Account"));
    }

    #[test]
    fn cache_miss_after_ttl_expiry() {
        let (client, _f) = tmp_client();
        let addr = "0x0000000000000000000000000000000000000000000000000000000000000002";
        let label = VoyagerLabelInfo {
            name: Some("Old Label".into()),
            class_alias: None,
            deploy_block: None,
        };
        // Insert with a timestamp older than the 24 h TTL
        let stale_ts = now_secs() - (CACHE_TTL_SECS as i64 + 1);
        insert_label_for_test(&client, addr, &label, stale_ts);

        assert!(
            client.get_cached(addr).is_none(),
            "Stale entry should be a cache miss"
        );
    }

    #[test]
    fn cache_miss_for_unknown_address() {
        let (client, _f) = tmp_client();
        assert!(client.get_cached("0xdeadbeef").is_none());
    }

    // --- fetch_and_send_label action dispatch ---

    #[tokio::test]
    async fn fetch_and_send_label_emits_action_when_cached_name_present() {
        let (client, _f) = tmp_client();
        let address =
            Felt::from_hex("0x04164013f90b05d67f026779bf96e9c401c96f3485b645a786166e6935fba116")
                .unwrap();
        let addr_hex = format!("{:#066x}", address);
        let label = VoyagerLabelInfo {
            name: Some("Binance Hot Wallet".into()),
            class_alias: Some("OpenZeppelin Account".into()),
            deploy_block: None,
        };
        insert_label_for_test(&client, &addr_hex, &label, now_secs());

        let (tx, mut rx) = mpsc::unbounded_channel();
        fetch_and_send_label(address, &client, &tx).await;

        let action = rx.try_recv().expect("Should have received an action");
        match action {
            crate::app::actions::Action::VoyagerLabelLoaded {
                address: a,
                label: l,
            } => {
                assert_eq!(a, address);
                assert_eq!(l.name.as_deref(), Some("Binance Hot Wallet"));
                assert_eq!(l.class_alias.as_deref(), Some("OpenZeppelin Account"));
            }
            other => panic!("Expected VoyagerLabelLoaded, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn fetch_and_send_label_emits_nothing_when_name_is_null() {
        let (client, _f) = tmp_client();
        let address =
            Felt::from_hex("0x0000000000000000000000000000000000000000000000000000000000000003")
                .unwrap();
        let addr_hex = format!("{:#066x}", address);
        // Cached entry with null name (unknown address)
        let label = VoyagerLabelInfo {
            name: None,
            class_alias: Some("Account".into()),
            deploy_block: None,
        };
        insert_label_for_test(&client, &addr_hex, &label, now_secs());

        let (tx, mut rx) = mpsc::unbounded_channel();
        fetch_and_send_label(address, &client, &tx).await;

        assert!(
            rx.try_recv().is_err(),
            "Should emit nothing when name is null"
        );
    }
}
