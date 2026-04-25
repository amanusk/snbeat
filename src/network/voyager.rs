use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde::Deserialize;
use starknet::core::types::Felt;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::data::types::VoyagerLabelInfo;

const VOYAGER_API_BASE: &str = "https://api.voyager.online/beta";
/// Cache TTL: 24 hours (labels rarely change)
const CACHE_TTL_SECS: u64 = 86_400;
/// Max concurrent outbound Voyager API requests. Voyager is a nice-to-have
/// (labels for sender/contract tags) so we keep this small to avoid rate-limit
/// errors when a busy page (e.g. a block with hundreds of unique senders)
/// triggers a burst of prefetches. Cache hits bypass this limit entirely.
const MAX_CONCURRENT_FETCHES: usize = 4;
/// After a Voyager error, suppress new outbound calls for this window.
/// The error is usually a 429/5xx — burning more requests at it during the
/// window just compounds the problem and spams the UI status bar.
const ERROR_BACKOFF_SECS: u64 = 30;

/// Voyager API client for fetching contract metadata (labels, types).
/// Wraps a SQLite cache to avoid redundant API calls across restarts, plus
/// in-process throttling (concurrency cap, in-flight dedup, error backoff)
/// so a single busy page can't hammer the Voyager API.
pub struct VoyagerClient {
    client: reqwest::Client,
    api_key: String,
    db: Mutex<Connection>,
    /// Caps simultaneous outbound HTTP calls. Acquired only after a cache miss
    /// and after dedup, so cache-only paths stay free.
    sem: Semaphore,
    /// Addresses currently being fetched. A second concurrent caller for the
    /// same address returns an empty label immediately; the next render after
    /// the in-flight call lands will hit the cache.
    in_flight: Mutex<HashSet<Felt>>,
    /// Unix-secs gate. While `now < backoff_until`, outbound calls are skipped.
    /// Tripped on any non-cacheable Voyager error (429/5xx, transport error).
    backoff_until: AtomicU64,
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

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn empty_label() -> VoyagerLabelInfo {
    VoyagerLabelInfo {
        name: None,
        class_alias: None,
        deploy_block: None,
    }
}

/// RAII guard for an in-flight address slot. Removes the entry on drop so a
/// failure path can't wedge an address as "permanently in flight".
struct InFlightGuard<'a> {
    set: &'a Mutex<HashSet<Felt>>,
    addr: Felt,
}

impl<'a> InFlightGuard<'a> {
    fn try_acquire(set: &'a Mutex<HashSet<Felt>>, addr: Felt) -> Option<Self> {
        let mut g = set.lock().ok()?;
        if g.insert(addr) {
            Some(InFlightGuard { set, addr })
        } else {
            None
        }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut g) = self.set.lock() {
            g.remove(&self.addr);
        }
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
            sem: Semaphore::new(MAX_CONCURRENT_FETCHES),
            in_flight: Mutex::new(HashSet::new()),
            backoff_until: AtomicU64::new(0),
        })
    }

    /// Get the label for an address, reading from cache when available.
    ///
    /// Cache hits return immediately. On a cache miss this enforces three
    /// guards before issuing an API call:
    ///   1. Error backoff: if Voyager recently errored, return an empty label
    ///      without firing a request.
    ///   2. In-flight dedup: if another task is already fetching this address,
    ///      return an empty label and let the in-flight call populate the cache.
    ///   3. Concurrency cap: only `MAX_CONCURRENT_FETCHES` requests run at once.
    ///
    /// "Empty label" means `VoyagerLabelInfo` with all `None` fields; callers
    /// (e.g. `fetch_and_send_label`) treat it as "no tag available" and don't
    /// emit anything to the UI, so the throttle is invisible to the user.
    pub async fn get_label(&self, address: Felt) -> Result<VoyagerLabelInfo, String> {
        let addr_hex = format!("{:#066x}", address);

        if let Some(cached) = self.get_cached(&addr_hex) {
            debug!(address = %addr_hex, "Voyager label cache hit");
            return Ok(cached);
        }

        if self.in_backoff() {
            debug!(address = %addr_hex, "Voyager backoff active, skipping fetch");
            return Ok(empty_label());
        }

        // Reserve the in-flight slot. If another task is already fetching this
        // address, bail — we don't want N tasks racing to fetch the same key.
        let _guard = match InFlightGuard::try_acquire(&self.in_flight, address) {
            Some(g) => g,
            None => {
                debug!(address = %addr_hex, "Voyager fetch already in flight, skipping");
                return Ok(empty_label());
            }
        };

        // `acquire` only fails if the semaphore is closed, which we never do.
        let _permit = self.sem.acquire().await.map_err(|e| e.to_string())?;

        match self.fetch_from_api(&addr_hex).await {
            Ok(label) => {
                self.store_cached(&addr_hex, &label);
                Ok(label)
            }
            Err(e) => {
                self.trip_backoff();
                Err(e)
            }
        }
    }

    fn in_backoff(&self) -> bool {
        let now = now_secs();
        self.backoff_until.load(Ordering::Acquire) > now
    }

    fn trip_backoff(&self) {
        let until = now_secs().saturating_add(ERROR_BACKOFF_SECS);
        self.backoff_until.store(until, Ordering::Release);
        warn!(
            secs = ERROR_BACKOFF_SECS,
            "Voyager error — pausing outbound fetches"
        );
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
        let cutoff = now_secs().saturating_sub(CACHE_TTL_SECS) as i64;

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
        let now = now_secs() as i64;
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

    // --- throttle: backoff & in-flight dedup ---

    #[tokio::test]
    async fn get_label_skips_outbound_during_backoff() {
        let (client, _f) = tmp_client();
        // Trip the backoff manually to a future time. With a bogus api_key
        // and no cached entry, a real fetch would error against api.voyager.online;
        // backoff must short-circuit before any HTTP call.
        let until = (now_secs() as u64).saturating_add(60);
        client.backoff_until.store(until, Ordering::Release);

        let address =
            Felt::from_hex("0x0000000000000000000000000000000000000000000000000000000000000aaa")
                .unwrap();
        let label = client.get_label(address).await.expect("must not error");
        assert!(label.name.is_none(), "backoff must yield empty label");
        assert!(label.class_alias.is_none());
    }

    #[tokio::test]
    async fn in_flight_guard_dedups_concurrent_callers() {
        let (client, _f) = tmp_client();
        let address =
            Felt::from_hex("0x0000000000000000000000000000000000000000000000000000000000000bbb")
                .unwrap();
        // Manually claim the in-flight slot to simulate an ongoing fetch by another task.
        let _held = InFlightGuard::try_acquire(&client.in_flight, address)
            .expect("first acquire must succeed");

        // A second concurrent caller must NOT hit the network — short-circuit to empty.
        // (No cache entry, no backoff: the only way `get_label` can return Ok here is
        // via the in-flight short-circuit.)
        let label = client.get_label(address).await.expect("must not error");
        assert!(label.name.is_none(), "in-flight dedup must yield empty");
    }

    #[test]
    fn in_flight_guard_releases_on_drop() {
        let set: Mutex<HashSet<Felt>> = Mutex::new(HashSet::new());
        let addr = Felt::from_hex("0x1").unwrap();
        {
            let _g = InFlightGuard::try_acquire(&set, addr).unwrap();
            assert_eq!(set.lock().unwrap().len(), 1);
        }
        assert!(
            set.lock().unwrap().is_empty(),
            "guard must remove its entry on drop"
        );
    }
}
