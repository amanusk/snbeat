use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::{Connection, params};
use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, info, warn};

use crate::data::types::{AddressTxSummary, ContractCallSummary};

const DUNE_API_BASE: &str = "https://api.dune.com/api/v1";

/// Identifier of a SQL shape we want to reuse across calls as a Dune
/// persistent (parameterized) query. Each variant maps to one Dune
/// query_id stored in `dune_persistent_queries` after first creation.
#[derive(Debug, Clone, Copy)]
enum QueryShape {
    /// Events-from-address COUNT + block range over all of mainnet history.
    ProbeAddressActivity,
    /// Delta re-probe: events-from-address above a `from_block` floor.
    ProbeActivityDelta,
    /// Full-history sender txs — the non-windowed shape behind `query_account_txs`.
    AccountTxs,
    /// Sender txs scoped to a `[from_block, to_block]` range.
    AccountTxsWindowed,
    /// Calls TO a contract scoped to a range; also serves the non-windowed
    /// full-history case via an open-ended range (see `query_contract_calls`).
    ContractCallsWindowed,
    /// First DECLARE tx for a class hash.
    DeclareTx,
}

impl QueryShape {
    fn name(self) -> &'static str {
        match self {
            QueryShape::ProbeAddressActivity => "probe_address_activity",
            QueryShape::ProbeActivityDelta => "probe_address_activity_delta",
            QueryShape::AccountTxs => "account_txs",
            QueryShape::AccountTxsWindowed => "account_txs_windowed",
            QueryShape::ContractCallsWindowed => "contract_calls_windowed",
            QueryShape::DeclareTx => "declare_tx",
        }
    }

    /// Map a persisted shape name back to its variant — the single source of
    /// truth for the hydration (name -> variant) mapping. A new shape must be
    /// added here as well as in `name()`, `sql()`, and `display_name()`.
    fn from_name(name: &str) -> Option<QueryShape> {
        Some(match name {
            "probe_address_activity" => QueryShape::ProbeAddressActivity,
            "probe_address_activity_delta" => QueryShape::ProbeActivityDelta,
            "account_txs" => QueryShape::AccountTxs,
            "account_txs_windowed" => QueryShape::AccountTxsWindowed,
            "contract_calls_windowed" => QueryShape::ContractCallsWindowed,
            "declare_tx" => QueryShape::DeclareTx,
            _ => return None,
        })
    }

    /// Parameterized SQL body. Uses Dune's `{{key}}` placeholders so we
    /// can reuse the same query_id across calls (different params) and
    /// avoid the create + archive round trips that `execute_sql` pays.
    ///
    /// Substitution is textual: bare `{{x}}` for numeric/hex literals
    /// (`block_number BETWEEN {{from_block}} AND {{to_block}}`,
    /// `from_address = {{address}}`), quoted `date '{{min_date}}'` for the
    /// partition floor. Each `{{key}}` must be supplied by the matching
    /// `params` map in the calling function (and mirrored in its fallback
    /// SQL). To prune `block_date` partitions on dense tables, range
    /// queries carry a `{{min_date}}` floor rather than relying on
    /// `block_number` alone (which Dune cannot use for partition pruning).
    fn sql(self) -> &'static str {
        match self {
            QueryShape::ProbeAddressActivity => {
                "SELECT COUNT(*) AS cnt, \
                   MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                 FROM starknet.events \
                 WHERE from_address = {{address}} \
                 AND block_date >= date '{{min_date}}'"
            }
            QueryShape::ProbeActivityDelta => {
                "SELECT COUNT(*) AS cnt, \
                   MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                 FROM starknet.events \
                 WHERE from_address = {{address}} \
                 AND block_date >= date '{{min_date}}' \
                 AND block_number > {{from_block}}"
            }
            QueryShape::AccountTxs => {
                "SELECT hash, sender_address, nonce, execution_status, revert_reason, \
                   actual_fee_amount, tip, block_number, block_time, type \
                 FROM starknet.transactions \
                 WHERE sender_address = {{sender}} \
                 AND block_date >= date '2021-01-01' \
                 ORDER BY nonce DESC \
                 LIMIT {{limit}}"
            }
            QueryShape::AccountTxsWindowed => {
                "SELECT hash, sender_address, nonce, execution_status, revert_reason, \
                   actual_fee_amount, tip, block_number, block_time, type \
                 FROM starknet.transactions \
                 WHERE sender_address = {{sender}} \
                 AND block_date >= date '{{min_date}}' \
                 AND block_number BETWEEN {{from_block}} AND {{to_block}} \
                 ORDER BY nonce DESC \
                 LIMIT {{limit}}"
            }
            QueryShape::ContractCallsWindowed => {
                "SELECT transaction_hash, caller_address, entry_point_selector, \
                   block_number, block_time, revert_reason \
                 FROM starknet.calls \
                 WHERE contract_address = {{contract}} \
                 AND block_date >= date '{{min_date}}' \
                 AND block_number BETWEEN {{from_block}} AND {{to_block}} \
                 AND call_type = 'CALL' \
                 ORDER BY block_number DESC \
                 LIMIT {{limit}}"
            }
            QueryShape::DeclareTx => {
                "SELECT hash, sender_address, block_number, block_time \
                 FROM starknet.transactions \
                 WHERE type = 'DECLARE' AND class_hash = {{class_hash}} \
                 ORDER BY block_number ASC \
                 LIMIT 1"
            }
        }
    }

    /// Display name to register the query under in Dune.
    fn display_name(self) -> &'static str {
        match self {
            QueryShape::ProbeAddressActivity => "snbeat_probe_address_activity",
            QueryShape::ProbeActivityDelta => "snbeat_probe_address_activity_delta",
            QueryShape::AccountTxs => "snbeat_account_txs",
            QueryShape::AccountTxsWindowed => "snbeat_account_txs_windowed",
            QueryShape::ContractCallsWindowed => "snbeat_contract_calls_windowed",
            QueryShape::DeclareTx => "snbeat_declare_tx",
        }
    }
}

/// Resolve an optional `block_date` partition floor to its `YYYY-MM-DD`
/// literal, defaulting to Starknet mainnet's launch year when absent.
///
/// `starknet.events` / `starknet.transactions` / `starknet.calls` are
/// partitioned by `block_date`, so every windowed/probe shape carries a
/// `{{min_date}}` hint: a `block_number`-only predicate cannot prune date
/// partitions and forces a full-table scan. The 2021-01-01 default is
/// equivalent to "no floor" (mainnet launched late 2021) but still lets Dune
/// skip empty pre-mainnet partitions.
///
/// The caller must pass a floor that is a *lower* bound on the earliest row
/// it wants — a too-late floor silently prunes partitions that still hold
/// matching rows. Derive it from the `from_block`'s real timestamp minus a
/// 1-day UTC cushion, never from an estimate that could overshoot.
fn min_date_floor(min_block_date: Option<chrono::NaiveDate>) -> String {
    min_block_date
        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(2021, 1, 1).expect("valid date"))
        .format("%Y-%m-%d")
        .to_string()
}

/// Dune Analytics API client for querying Starknet transaction history.
pub struct DuneClient {
    client: reqwest::Client,
    api_key: String,
    /// Whether dynamic queries created via `execute_sql` are flagged
    /// `is_private`. Toggleable so callers can sidestep a per-account
    /// private-query quota when it's exhausted; archive-on-finish still
    /// runs either way, so the queries stay temporary.
    is_private: bool,
    /// SQLite-backed cache of Dune query IDs per `QueryShape` so we
    /// don't pay create + archive round trips on every call. Optional —
    /// when not configured, the persistent-query fast path falls back
    /// to `execute_sql`'s create-per-call shape.
    persistent_db: Option<Mutex<Connection>>,
    /// In-memory mirror of the persistent_db rows so the common case is
    /// a HashMap read, not a SQLite query. Updated on every successful
    /// `get_or_create_persistent_query` call.
    persistent_ids: Mutex<HashMap<&'static str, u64>>,
    /// Serializes the cold-path create in `get_or_create_persistent_query`
    /// so two concurrent cold callers (probes run in spawned tasks) can't
    /// both POST a create to Dune and leave a duplicate query registered.
    /// Held across the create await; the map is re-checked after acquiring.
    create_lock: tokio::sync::Mutex<()>,
}

#[derive(Debug, Deserialize)]
struct CreateQueryResponse {
    query_id: u64,
}

#[derive(Debug, Deserialize)]
struct ExecuteResponse {
    execution_id: String,
}

#[derive(Debug, Deserialize)]
struct ExecutionStatusResponse {
    state: String,
    result: Option<ExecutionResult>,
}

#[derive(Debug, Deserialize)]
struct ExecutionResult {
    rows: Vec<serde_json::Value>,
}

/// Outcome of executing a persistent (parameterized) query. Distinguishes a
/// permanently-dead `query_id` (deleted/archived externally) from an ordinary
/// failure so `run_shape` can evict + recreate the former rather than pinning
/// the shape to the slow path forever.
enum PersistentOutcome {
    Rows(Vec<serde_json::Value>),
    /// The persisted `query_id` is no longer usable on Dune (archived → 403,
    /// or deleted → 404/410) — evict it and recreate on the next call.
    Stale,
    /// Transient/other failure — fall back to `execute_sql` this call.
    Failed,
}

/// RAII archive trigger for ephemeral queries created by `execute_sql`.
/// Dropping this fires a detached archive POST so the query doesn't leak on
/// the Dune account when the awaiting future is cancelled mid-poll. Cleanup
/// is best-effort: if the runtime is shutting down the spawned task may not
/// run, but on regular cancellation it does.
struct ArchiveGuard {
    client: reqwest::Client,
    api_key: String,
    query_id: u64,
}

impl Drop for ArchiveGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let api_key = std::mem::take(&mut self.api_key);
        let query_id = self.query_id;
        tokio::spawn(async move {
            match client
                .post(format!("{}/query/{}/archive", DUNE_API_BASE, query_id))
                .header("X-Dune-API-Key", &api_key)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        debug!(query_id, "Dune archive succeeded");
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        tracing::warn!(query_id, %status, body = %body, "Dune archive returned non-success");
                    }
                }
                Err(e) => {
                    tracing::warn!(query_id, error = %e, "Dune archive request failed");
                }
            }
        });
    }
}

/// Result of a lightweight probe: block range + count of address activity.
#[derive(Debug, Default, Clone)]
pub struct AddressActivityProbe {
    pub sender_tx_count: u64,
    pub sender_min_block: u64,
    pub sender_max_block: u64,
    pub callee_call_count: u64,
    pub callee_min_block: u64,
    pub callee_max_block: u64,
}

impl AddressActivityProbe {
    /// Overall min block across both sender and callee activity.
    pub fn min_block(&self) -> u64 {
        match (self.sender_min_block, self.callee_min_block) {
            (0, b) => b,
            (a, 0) => a,
            (a, b) => a.min(b),
        }
    }

    /// Overall max block across both sender and callee activity.
    pub fn max_block(&self) -> u64 {
        self.sender_max_block.max(self.callee_max_block)
    }

    /// Whether any activity was found at all.
    pub fn has_activity(&self) -> bool {
        self.sender_tx_count > 0 || self.callee_call_count > 0
    }

    /// Recommended block window size based on activity density.
    /// Used by all sources to decide how many blocks to fetch per page.
    pub fn recommended_window(&self) -> u64 {
        let total_events = self.sender_tx_count.max(self.callee_call_count);
        let block_span = self.max_block().saturating_sub(self.min_block()).max(1);
        let events_per_block = total_events as f64 / block_span as f64;

        if events_per_block > 10.0 {
            500 // Super hot (>10 events/block)
        } else if events_per_block > 1.0 {
            5_000 // Hot
        } else if events_per_block > 0.01 {
            50_000 // Moderate
        } else {
            200_000 // Cold
        }
    }
}

impl DuneClient {
    pub fn new(api_key: String, is_private: bool) -> Self {
        // Without timeouts, a hung connection wedges the poll loop
        // indefinitely — the overall 120s deadline in `execute_sql` only
        // counts *completed* polls. Match PF's posture: cap each request
        // at 60s (enough for result fetches on large windows), 10s to
        // even open a connection.
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .expect("Failed to build Dune reqwest client");
        Self {
            client,
            api_key,
            is_private,
            persistent_db: None,
            persistent_ids: Mutex::new(HashMap::new()),
            create_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Enable the persistent-query fast path by opening (or creating) a
    /// `dune_persistent_queries` table in `cache_db_path`. Builder-style
    /// — call once before wrapping the client in `Arc`. If open or
    /// schema init fails, the client falls back to the ephemeral
    /// `execute_sql` path (same behaviour as before this feature
    /// existed), so this method never errors fatally.
    pub fn with_persistent_cache(mut self, cache_db_path: &Path) -> Self {
        match Connection::open(cache_db_path) {
            Ok(db) => {
                // WAL matches every other cache-backed client (cache.rs,
                // prices.rs, voyager.rs); it's sticky on the file anyway.
                // `busy_timeout` is per-connection and defaults to 0 — only
                // cache.rs's pool sets it — so without it here a write (first
                // query-id creation) racing the main cache pool returns
                // `database is locked` immediately instead of waiting.
                if let Err(e) =
                    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
                {
                    warn!(error = %e, "Dune persistent cache: pragma init failed; falling back to ephemeral");
                    return self;
                }
                if let Err(e) = db.execute_batch(
                    "CREATE TABLE IF NOT EXISTS dune_persistent_queries (
                        shape TEXT PRIMARY KEY,
                        query_id INTEGER NOT NULL,
                        created_at INTEGER NOT NULL,
                        sql_text TEXT
                     );",
                ) {
                    warn!(error = %e, "Dune persistent cache: schema init failed; falling back to ephemeral");
                    return self;
                }
                // Migrate tables created before `sql_text` existed so a cached
                // query_id can be checked against the SQL it was registered
                // with. Without this, editing a shape's SQL in-place would keep
                // hitting the old persistent query on Dune forever — the #78
                // eviction only fires on an external 403/404/410, not on our
                // own SQL changes. Duplicate-column is expected on
                // already-migrated DBs and ignored.
                let _ = db
                    .execute_batch("ALTER TABLE dune_persistent_queries ADD COLUMN sql_text TEXT;");
                // Hydrate the in-memory map so the common case never
                // touches SQLite.
                let mut hydrated: HashMap<&'static str, u64> = HashMap::new();
                let mut stale_shapes: Vec<String> = Vec::new();
                if let Ok(mut stmt) =
                    db.prepare("SELECT shape, query_id, sql_text FROM dune_persistent_queries")
                {
                    let rows = stmt.query_map([], |row| {
                        let shape: String = row.get(0)?;
                        let qid: i64 = row.get(1)?;
                        let sql_text: Option<String> = row.get(2)?;
                        Ok((shape, qid as u64, sql_text))
                    });
                    if let Ok(iter) = rows {
                        for (shape, qid, sql_text) in iter.flatten() {
                            // Only trust a cached query_id if the SQL it was
                            // registered with still matches the shape's current
                            // SQL. A mismatch (or a pre-migration NULL) means the
                            // query body changed since it was created, so the
                            // persisted query on Dune is stale — drop it and let
                            // get_or_create recreate a fresh one with the new SQL.
                            match QueryShape::from_name(&shape) {
                                Some(s) if sql_text.as_deref() == Some(s.sql()) => {
                                    hydrated.insert(s.name(), qid);
                                }
                                _ => stale_shapes.push(shape),
                            }
                        }
                    }
                }
                // Evict stale rows so a re-hydrate next launch doesn't resurrect
                // them; the fast path recreates each with current SQL on demand.
                for shape in &stale_shapes {
                    if let Err(e) = db.execute(
                        "DELETE FROM dune_persistent_queries WHERE shape = ?1",
                        params![shape],
                    ) {
                        warn!(shape = %shape, error = %e, "Dune persistent cache: failed to evict stale-SQL query_id");
                    }
                }
                self.persistent_ids = Mutex::new(hydrated);
                self.persistent_db = Some(Mutex::new(db));
            }
            Err(e) => {
                warn!(error = %e, "Dune persistent cache: open failed; falling back to ephemeral");
            }
        }
        self
    }

    /// Look up the Dune query_id for `shape`, creating and persisting it
    /// on first use. Returns None when the persistent cache isn't
    /// configured — callers then fall back to `execute_sql`.
    async fn get_or_create_persistent_query(&self, shape: QueryShape) -> Option<u64> {
        self.persistent_db.as_ref()?;
        // Fast path: in-memory hit.
        if let Ok(g) = self.persistent_ids.lock()
            && let Some(qid) = g.get(shape.name())
        {
            return Some(*qid);
        }
        // Cold path: serialize creates so two concurrent cold callers don't
        // both POST a create to Dune (which would leave a duplicate query
        // and burn quota). Hold the async lock across the create await.
        let _create_guard = self.create_lock.lock().await;
        // Re-check under the guard: a racing caller may have created the
        // query while we waited for the lock.
        if let Ok(g) = self.persistent_ids.lock()
            && let Some(qid) = g.get(shape.name())
        {
            return Some(*qid);
        }
        // Create the query on Dune and persist the id.
        let body = serde_json::json!({
            "name": shape.display_name(),
            "query_sql": shape.sql(),
            // Persistent queries are deliberately NOT private — they'd
            // churn the per-account private quota and the SQL has no
            // secrets (parameters carry the per-call values).
            "is_private": false,
        });
        let resp: CreateQueryResponse = match self
            .client
            .post(format!("{}/query", DUNE_API_BASE))
            .header("X-Dune-API-Key", &self.api_key)
            .json(&body)
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(shape = shape.name(), error = %e, "Dune persistent create failed");
                return None;
            }
        };
        let qid = resp.query_id;
        if let (Some(db_mutex), Ok(mut g)) =
            (self.persistent_db.as_ref(), self.persistent_ids.lock())
        {
            if let Ok(db) = db_mutex.lock() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                // Log persist failures: the id stays usable in-memory this
                // session, but a silently-failed write means a fresh create
                // (extra Dune churn) on next launch with no trace why.
                if let Err(e) = db.execute(
                    "INSERT OR REPLACE INTO dune_persistent_queries \
                     (shape, query_id, created_at, sql_text) VALUES (?1, ?2, ?3, ?4)",
                    params![shape.name(), qid as i64, now, shape.sql()],
                ) {
                    warn!(shape = shape.name(), query_id = qid, error = %e, "Dune persistent cache: failed to persist query_id; will recreate next launch");
                }
            }
            g.insert(shape.name(), qid);
        }
        debug!(
            shape = shape.name(),
            query_id = qid,
            "Dune persistent query created"
        );
        Some(qid)
    }

    /// Execute a persistent (parameterized) Dune query and return its rows.
    /// Cheap relative to `execute_sql`: no create, no archive, just an execute
    /// then poll. If the persisted `query_id` was archived/deleted externally
    /// the execute POST rejects it (`Stale`, so the caller can evict it); any
    /// other failure is `Failed` (fall back).
    async fn execute_persistent(
        &self,
        query_id: u64,
        params_map: &serde_json::Value,
    ) -> PersistentOutcome {
        let resp = match self
            .client
            .post(format!("{}/query/{}/execute", DUNE_API_BASE, query_id))
            .header("X-Dune-API-Key", &self.api_key)
            .json(&serde_json::json!({ "query_parameters": params_map }))
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return PersistentOutcome::Failed,
        };
        // An archived query_id returns 403 ("Query is archived or an unsaved
        // query"); a hard-deleted one would be 404/410. These are the signals
        // that the cached id is permanently stale (vs a transient 5xx/timeout,
        // which must NOT evict). Everything else falls back to execute_sql.
        if matches!(
            resp.status(),
            reqwest::StatusCode::FORBIDDEN
                | reqwest::StatusCode::NOT_FOUND
                | reqwest::StatusCode::GONE
        ) {
            return PersistentOutcome::Stale;
        }
        let exec_resp: ExecuteResponse = match resp.error_for_status() {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(_) => return PersistentOutcome::Failed,
            },
            Err(_) => return PersistentOutcome::Failed,
        };
        let execution_id = exec_resp.execution_id;

        // Same polling cadence as execute_sql — poll immediately, back
        // off 250ms → 2s cap, 120s overall deadline.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut delay = Duration::from_millis(250);
        loop {
            let status: ExecutionStatusResponse = match self
                .client
                .get(format!(
                    "{}/execution/{}/results",
                    DUNE_API_BASE, execution_id
                ))
                .header("X-Dune-API-Key", &self.api_key)
                .send()
                .await
                .and_then(|r| r.error_for_status())
            {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(_) => return PersistentOutcome::Failed,
                },
                Err(_) => return PersistentOutcome::Failed,
            };
            match status.state.as_str() {
                "QUERY_STATE_COMPLETED" => {
                    return PersistentOutcome::Rows(
                        status.result.map(|r| r.rows).unwrap_or_default(),
                    );
                }
                "QUERY_STATE_FAILED" | "QUERY_STATE_CANCELLED" | "QUERY_STATE_EXPIRED" => {
                    warn!(query_id, state = %status.state, "Dune persistent execute failed");
                    return PersistentOutcome::Failed;
                }
                _ => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        warn!(query_id, "Dune persistent execute timed out");
                        return PersistentOutcome::Failed;
                    }
                    // Cap the sleep to the remaining budget so a final
                    // pre-sleep deadline check just under the cap can't push
                    // wall-clock past 120s by up to a full `delay` (matches
                    // execute_sql).
                    let remaining = deadline.duration_since(now);
                    tokio::time::sleep(delay.min(remaining)).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
            }
        }
    }

    /// Drop a shape's cached `query_id` from both the in-memory map and the
    /// SQLite table, so the next `get_or_create_persistent_query` recreates
    /// it. Called when Dune reports the persisted id no longer exists.
    fn evict_persistent_query(&self, shape: QueryShape) {
        if let Ok(mut g) = self.persistent_ids.lock() {
            g.remove(shape.name());
        }
        if let Some(db_mutex) = self.persistent_db.as_ref()
            && let Ok(db) = db_mutex.lock()
            && let Err(e) = db.execute(
                "DELETE FROM dune_persistent_queries WHERE shape = ?1",
                params![shape.name()],
            )
        {
            warn!(shape = shape.name(), error = %e, "Dune persistent cache: failed to evict stale query_id");
        }
    }

    /// Run a query `shape`: try the persistent (parameterized) fast path,
    /// and on any failure (cache not configured, create/execute error) fall
    /// back to the ephemeral `execute_sql` create→archive path so semantics
    /// are unchanged. `fallback_sql` must encode the same query as `shape`
    /// with the `params` values substituted as literals. Returning the raw
    /// rows lets each caller parse once instead of duplicating the parse
    /// across both paths.
    async fn run_shape(
        &self,
        shape: QueryShape,
        params: serde_json::Value,
        fallback_sql: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
        if let Some(qid) = self.get_or_create_persistent_query(shape).await {
            match self.execute_persistent(qid, &params).await {
                PersistentOutcome::Rows(rows) => {
                    debug!(shape = shape.name(), "Dune persistent query: hit");
                    return Ok(rows);
                }
                PersistentOutcome::Stale => {
                    // The stored id was archived/deleted externally. Evict it
                    // so the next call recreates a fresh one; fall back to
                    // execute_sql for this call.
                    warn!(
                        shape = shape.name(),
                        query_id = qid,
                        "Dune persistent query_id archived/deleted externally; evicting so it is recreated next call"
                    );
                    self.evict_persistent_query(shape);
                }
                PersistentOutcome::Failed => {
                    debug!(
                        shape = shape.name(),
                        "Dune persistent query failed; falling back to execute_sql"
                    );
                }
            }
        }
        self.execute_sql(fallback_sql).await
    }

    /// Lightweight connectivity / API-key check.
    /// Fetches metadata for a well-known public query (id 1) which is fast
    /// and does not create or execute anything.
    pub async fn health(&self) -> Result<(), String> {
        self.client
            .get(format!("{}/query/4000922", DUNE_API_BASE))
            .header("X-Dune-API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| format!("Dune unreachable: {e}"))?
            .error_for_status()
            .map_err(|e| format!("Dune API error: {e}"))?;
        Ok(())
    }

    /// Query all transactions for an account (including reverted).
    pub async fn query_account_txs(
        &self,
        sender: Felt,
        limit: u32,
    ) -> Result<Vec<AddressTxSummary>, String> {
        let sender_hex = format!("{:#066x}", sender);
        // Partition-pruning floor: kept at the start of Starknet mainnet's
        // launch year (2021-01-01) so Dune skips empty partitions for sender
        // lookups, without silently dropping pre-2025 account history (the
        // previous 2025-01-01 floor hid legitimate older txs).
        let params = serde_json::json!({
            "sender": sender_hex,
            "limit": limit.to_string(),
        });
        let sql = format!(
            "SELECT hash, sender_address, nonce, execution_status, revert_reason, actual_fee_amount, \
             tip, block_number, block_time, type \
             FROM starknet.transactions \
             WHERE sender_address = {} \
             AND block_date >= date '2021-01-01' \
             ORDER BY nonce DESC \
             LIMIT {}",
            sender_hex, limit
        );

        debug!(sender = %sender_hex, "Querying Dune for account txs");
        let rows = self.run_shape(QueryShape::AccountTxs, params, &sql).await?;
        info!(rows = rows.len(), "Dune account txs query complete");
        Ok(parse_dune_rows(&rows))
    }

    /// Query calls TO a specific contract address using starknet.calls table.
    /// Returns ContractCallSummary objects sorted by block_number descending.
    ///
    /// Non-windowed: all calls from the 2024 `block_date` floor to the chain
    /// tip — the same scope as the original `query_contract_calls`, not
    /// literally full mainnet history. Delegates to the windowed shape with an
    /// open-ended block range so we don't register a second persistent query
    /// or duplicate the row-parsing logic.
    pub async fn query_contract_calls(
        &self,
        contract: Felt,
        limit: u32,
    ) -> Result<Vec<ContractCallSummary>, String> {
        // `expect` on a hardcoded valid date: panic loudly rather than let a
        // future typo silently degrade to the windowed helper's 2021 default
        // and lose the intended 2024 partition floor.
        let floor =
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).expect("2024-01-01 is a valid date");
        self.query_contract_calls_windowed(contract, 0, u64::MAX, limit, Some(floor))
            .await
    }

    /// Windowed variant of `query_account_txs` — scoped to a block range for fast completion.
    ///
    /// `min_block_date` is the `block_date` partition floor (see
    /// [`min_date_floor`]): `starknet.transactions` is partitioned by date, so
    /// a `block_number BETWEEN` predicate alone scans every partition and is
    /// slow for busy senders. Pass the `block_date` of `from_block` (minus a
    /// 1-day cushion); `None` falls back to the 2021 floor (correct, slower).
    pub async fn query_account_txs_windowed(
        &self,
        sender: Felt,
        from_block: u64,
        to_block: u64,
        limit: u32,
        min_block_date: Option<chrono::NaiveDate>,
    ) -> Result<Vec<AddressTxSummary>, String> {
        let sender_hex = format!("{:#066x}", sender);
        let min_date_str = min_date_floor(min_block_date);
        // DuneSQL (Trino) bigint rejects literals above i64::MAX at parse
        // time, so cap both bounds: an open-ended `to_block` of u64::MAX, and
        // any out-of-range `from_block` (the API is u64). The cap is far above
        // any real block number, so the range is unchanged in practice.
        let from_capped = from_block.min(i64::MAX as u64);
        let to_capped = to_block.min(i64::MAX as u64);
        let params = serde_json::json!({
            "sender": sender_hex,
            "min_date": min_date_str,
            "from_block": from_capped.to_string(),
            "to_block": to_capped.to_string(),
            "limit": limit.to_string(),
        });
        let sql = format!(
            "SELECT hash, sender_address, nonce, execution_status, revert_reason, actual_fee_amount, \
             tip, block_number, block_time, type \
             FROM starknet.transactions \
             WHERE sender_address = {} \
             AND block_date >= date '{}' \
             AND block_number BETWEEN {} AND {} \
             ORDER BY nonce DESC \
             LIMIT {}",
            sender_hex, min_date_str, from_capped, to_capped, limit
        );

        debug!(sender = %sender_hex, from_block, to_block, from_capped, to_capped, %min_date_str, "Querying Dune for account txs (windowed)");
        let rows = self
            .run_shape(QueryShape::AccountTxsWindowed, params, &sql)
            .await?;
        info!(
            rows = rows.len(),
            "Dune windowed account txs query complete"
        );
        Ok(parse_dune_rows(&rows))
    }

    /// Windowed variant of `query_contract_calls` — scoped to a block range for fast completion.
    ///
    /// `min_block_date` is an optional partition hint: `starknet.calls` is partitioned by
    /// `block_date`, and without a date predicate Dune has to scan every partition to find
    /// the requested `block_number` range, which times out as `QUERY_STATE_FAILED` on dense
    /// contracts. Pass the `block_date` of `from_block` (minus a 1-day UTC buffer) whenever
    /// the caller has a reasonable estimate — the SQL stays correct either way.
    pub async fn query_contract_calls_windowed(
        &self,
        contract: Felt,
        from_block: u64,
        to_block: u64,
        limit: u32,
        min_block_date: Option<chrono::NaiveDate>,
    ) -> Result<Vec<ContractCallSummary>, String> {
        let contract_hex = format!("{:#066x}", contract);
        // The persistent shape has fixed SQL, so the date floor is always
        // present; `min_date_floor` defaults a missing hint to mainnet's first
        // year (equivalent to "no floor", still prunes empty partitions).
        let min_date_str = min_date_floor(min_block_date);
        // DuneSQL (Trino) uses bigint for block_number; any literal above
        // i64::MAX (9_223_372_036_854_775_807) is rejected at parse time as
        // "Invalid numeric literal". Cap both bounds — an open-ended u64::MAX
        // `to_block` and any out-of-range `from_block` (the API is u64). The
        // cap is far above any real block, so `BETWEEN from AND cap` stays
        // equivalent to an open-ended `>= from`.
        let from_capped = from_block.min(i64::MAX as u64);
        let to_capped = to_block.min(i64::MAX as u64);
        let params = serde_json::json!({
            "contract": contract_hex,
            "min_date": min_date_str,
            "from_block": from_capped.to_string(),
            "to_block": to_capped.to_string(),
            "limit": limit.to_string(),
        });
        let sql = format!(
            "SELECT transaction_hash, caller_address, entry_point_selector, \
             block_number, block_time, revert_reason \
             FROM starknet.calls \
             WHERE contract_address = {} \
             AND block_date >= date '{}' \
             AND block_number BETWEEN {} AND {} \
             AND call_type = 'CALL' \
             ORDER BY block_number DESC \
             LIMIT {}",
            contract_hex, min_date_str, from_capped, to_capped, limit
        );

        debug!(contract = %contract_hex, from_block, to_block, from_capped, to_capped, ?min_block_date, "Querying Dune for contract calls (windowed)");
        let rows = self
            .run_shape(QueryShape::ContractCallsWindowed, params, &sql)
            .await?;
        info!(
            rows = rows.len(),
            "Dune windowed contract calls query complete"
        );

        Ok(rows
            .iter()
            .filter_map(|row| {
                let tx_hash = Felt::from_hex(row.get("transaction_hash")?.as_str()?).ok()?;
                let caller = Felt::from_hex(row.get("caller_address")?.as_str()?).ok()?;
                let selector_hex = row.get("entry_point_selector")?.as_str().unwrap_or("");
                let function_name = selector_hex.to_string();
                let block_number = row
                    .get("block_number")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let block_time = row
                    .get("block_time")
                    .and_then(|v| v.as_str())
                    .and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(s)
                            .or_else(|_| {
                                chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f %Z")
                            })
                            .ok()
                    })
                    .map(|dt| dt.timestamp() as u64)
                    .unwrap_or(0);
                let revert = row
                    .get("revert_reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let status = if revert.is_empty() { "OK" } else { "REV" }.to_string();

                Some(ContractCallSummary {
                    tx_hash,
                    sender: caller,
                    function_name,
                    block_number,
                    timestamp: block_time,
                    total_fee_fri: 0,
                    status,
                    nonce: None,
                    tip: 0,
                    inner_targets: Vec::new(),
                })
            })
            .collect())
    }

    /// Lightweight probe: returns the block range and count of activity for an address.
    ///
    /// Uses `starknet.events WHERE from_address = addr` (single cheap query).
    /// This count reflects **events emitted by** the address:
    /// - Accounts emit `transaction_executed` once per invoke (self-sent OR
    ///   relayed meta-tx) — so it's an upper bound on both sender tx count
    ///   and inbound-call count, not a clean measure of either.
    /// - Contracts emit whatever their entrypoints emit.
    ///
    /// The result is populated into `callee_call_count` only (a fair proxy
    /// for inbound activity on account-contracts, exact for contracts).
    /// `sender_tx_count` is left at 0 because Dune cannot answer it cheaply
    /// here; the caller should use the on-chain nonce as the authoritative
    /// sender count for accounts. The UI distinguishes these two roles and
    /// must not display the events count as "sender tx total" — see
    /// `src/ui/views/address_info.rs`.
    ///
    /// `min_block_date` is the `block_date` partition floor (see
    /// [`min_date_floor`]). Pass `None` for a full-history probe (mainnet
    /// launch onward); pass a recent date for a fast recent-window probe whose
    /// count/range is a *lower bound* on lifetime activity — the caller can
    /// publish that as an early hint, then refine with a `None` probe in the
    /// background (the count only grows, never regresses).
    pub async fn probe_address_activity(
        &self,
        address: Felt,
        min_block_date: Option<chrono::NaiveDate>,
    ) -> Result<AddressActivityProbe, String> {
        let addr_hex = format!("{:#066x}", address);
        // Persistent parameterized query (fast path) with the ephemeral
        // `execute_sql` create→archive path as fallback. With `None`, the
        // 2021-01-01 floor covers all of mainnet history in a single shot
        // (launch was late 2021, so empty pre-mainnet partitions are still
        // pruned); a recent floor scopes to a handful of partitions and
        // returns in a few seconds.
        let min_date_str = min_date_floor(min_block_date);
        let params = serde_json::json!({ "address": addr_hex, "min_date": min_date_str });
        let sql = format!(
            "SELECT COUNT(*) AS cnt, \
               MIN(block_number) AS min_block, MAX(block_number) AS max_block \
             FROM starknet.events \
             WHERE from_address = {addr} \
             AND block_date >= date '{min_date}'",
            addr = addr_hex,
            min_date = min_date_str,
        );

        debug!(address = %addr_hex, %min_date_str, "Dune events probe: checking address activity range");
        let rows = self
            .run_shape(QueryShape::ProbeAddressActivity, params, &sql)
            .await?;

        // Events-from-address counts inbound activity for account-contracts
        // and emitted events for pure contracts. Both populate callee fields;
        // sender_tx_count is left at 0 (unknown from this query — use nonce).
        let probe = probe_from_rows(&rows);

        info!(
            event_count = probe.callee_call_count,
            blocks = format!("{}..{}", probe.min_block(), probe.max_block()),
            "Dune events probe complete"
        );
        Ok(probe)
    }

    /// TopDelta variant of [`Self::probe_address_activity`].
    ///
    /// When a prior probe result is cached but stale, we only need to extend
    /// its `max_block` + event count toward the chain tip — `min_block`
    /// never regresses. This query scopes to `block_number > from_block`,
    /// making the re-probe cheap regardless of chain age.
    ///
    /// Returns a partial probe: `callee_call_count` counts the delta events,
    /// and `callee_max_block` is their upper bound. The caller is expected to
    /// merge this into the cached row (cache.rs `save_activity_range_with_count`
    /// handles the min-preserve / max-expand / count-max semantics).
    ///
    /// `min_block_date` is the `block_date` partition floor (see
    /// [`min_date_floor`]). `starknet.events` is partitioned by date, so a
    /// `block_number > from_block` predicate alone cannot prune partitions and
    /// scans the whole table. Pass the `block_date` of `from_block` (minus a
    /// 1-day cushion) — since the delta only wants rows above `from_block`,
    /// that date is a safe floor; `None` falls back to the 2021 floor (correct,
    /// just slower).
    pub async fn probe_address_activity_delta(
        &self,
        address: Felt,
        from_block: u64,
        min_block_date: Option<chrono::NaiveDate>,
    ) -> Result<AddressActivityProbe, String> {
        let addr_hex = format!("{:#066x}", address);
        let min_date_str = min_date_floor(min_block_date);
        let params = serde_json::json!({
            "address": addr_hex,
            "min_date": min_date_str,
            "from_block": from_block.to_string(),
        });
        let sql = format!(
            "SELECT COUNT(*) AS cnt, \
               MIN(block_number) AS min_block, MAX(block_number) AS max_block \
             FROM starknet.events \
             WHERE from_address = {addr} \
             AND block_date >= date '{min_date}' \
             AND block_number > {from}",
            addr = addr_hex,
            min_date = min_date_str,
            from = from_block,
        );

        debug!(address = %addr_hex, from_block, %min_date_str, "Dune events probe (delta): extending activity range");
        let rows = self
            .run_shape(QueryShape::ProbeActivityDelta, params, &sql)
            .await?;

        // Same COUNT/MIN/MAX-over-events shape as `probe_address_activity`,
        // so reuse its row-shaping — which parses DuneSQL's string-encoded
        // numerics via parse_json_u64 rather than as_u64 (the latter silently
        // reads a stringified count as 0, dropping a non-zero delta).
        let probe = probe_from_rows(&rows);

        info!(
            event_count = probe.callee_call_count,
            from_block,
            max_block = probe.callee_max_block,
            "Dune events probe (delta) complete"
        );
        Ok(probe)
    }

    /// Query the declare transaction for a class hash (fallback when PF is unavailable).
    pub async fn query_declare_tx(
        &self,
        class_hash: Felt,
    ) -> Result<Option<crate::data::types::ClassDeclareInfo>, String> {
        let hash_hex = format!("{:#066x}", class_hash);
        let params = serde_json::json!({ "class_hash": hash_hex });
        let sql = format!(
            "SELECT hash, sender_address, block_number, block_time \
             FROM starknet.transactions \
             WHERE type = 'DECLARE' AND class_hash = {} \
             ORDER BY block_number ASC \
             LIMIT 1",
            hash_hex
        );

        debug!(class_hash = %hash_hex, "Querying Dune for declare tx");
        let rows = self.run_shape(QueryShape::DeclareTx, params, &sql).await?;

        if let Some(row) = rows.first() {
            let tx_hash = row
                .get("hash")
                .and_then(|v| v.as_str())
                .and_then(|s| Felt::from_hex(s).ok());
            let sender = row
                .get("sender_address")
                .and_then(|v| v.as_str())
                .and_then(|s| Felt::from_hex(s).ok());
            let block_number = row
                .get("block_number")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let timestamp = row
                .get("block_time")
                .and_then(|v| v.as_str())
                .and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f %Z"))
                        .ok()
                })
                .map(|dt| dt.timestamp() as u64)
                .unwrap_or(0);

            if let (Some(tx_hash), Some(sender)) = (tx_hash, sender) {
                return Ok(Some(crate::data::types::ClassDeclareInfo {
                    tx_hash,
                    sender,
                    block_number,
                    timestamp,
                }));
            }
        }
        Ok(None)
    }

    /// Execute arbitrary SQL and return result rows.
    async fn execute_sql(&self, sql: &str) -> Result<Vec<serde_json::Value>, String> {
        let create_body = serde_json::json!({
            "name": format!("snbeat_{}", chrono::Utc::now().timestamp()),
            "query_sql": sql,
            "is_private": self.is_private
        });

        let resp: CreateQueryResponse = self
            .client
            .post(format!("{}/query", DUNE_API_BASE))
            .header("X-Dune-API-Key", &self.api_key)
            .json(&create_body)
            .send()
            .await
            .map_err(|e| format!("Dune create failed: {e}"))?
            .error_for_status()
            .map_err(|e| format!("Dune create failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("Dune create parse failed: {e}"))?;

        let query_id = resp.query_id;
        // Arm the archive guard immediately after create. If the calling task
        // is cancelled (or any later step short-circuits with `?`), the guard
        // spawns a fire-and-forget archive request from Drop so we don't leak
        // the query on the Dune account. Non-private queries make this matter
        // more — they're visible org-wide until archived.
        let _archive_guard = ArchiveGuard {
            client: self.client.clone(),
            api_key: self.api_key.clone(),
            query_id,
        };

        let exec_resp: ExecuteResponse = self
            .client
            .post(format!("{}/query/{}/execute", DUNE_API_BASE, query_id))
            .header("X-Dune-API-Key", &self.api_key)
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|e| format!("Dune execute failed: {e}"))?
            .error_for_status()
            .map_err(|e| format!("Dune execute failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("Dune execute parse failed: {e}"))?;

        let execution_id = &exec_resp.execution_id;
        // Poll immediately, then back off exponentially (250ms → 2s cap).
        // Light queries (COUNT probes) complete in well under a second, so a
        // fixed pre-poll sleep put a hard ~2s latency floor on every Dune
        // fetch. The 120s overall deadline is unchanged.
        // Archive runs from `_archive_guard`'s Drop impl so the query is
        // cleaned up on every exit path (success, polling failure, future
        // cancellation, error early-return).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut delay = Duration::from_millis(250);
        loop {
            let status: ExecutionStatusResponse = self
                .client
                .get(format!(
                    "{}/execution/{}/results",
                    DUNE_API_BASE, execution_id
                ))
                .header("X-Dune-API-Key", &self.api_key)
                .send()
                .await
                .map_err(|e| format!("Dune poll failed: {e}"))?
                .error_for_status()
                .map_err(|e| format!("Dune poll failed: {e}"))?
                .json()
                .await
                .map_err(|e| format!("Dune poll parse failed: {e}"))?;

            match status.state.as_str() {
                "QUERY_STATE_COMPLETED" => {
                    break Ok(status.result.map(|r| r.rows).unwrap_or_default());
                }
                "QUERY_STATE_FAILED" | "QUERY_STATE_CANCELLED" | "QUERY_STATE_EXPIRED" => {
                    break Err(format!("Dune query {} failed: {}", query_id, status.state));
                }
                _ => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        break Err("Dune query timed out (120s)".into());
                    }
                    // Cap the sleep to the remaining budget so a final
                    // pre-sleep deadline check just under the cap can't
                    // push wall-clock past 120s by up to a full `delay`
                    // window (~2s at steady state).
                    let remaining = deadline.duration_since(now);
                    tokio::time::sleep(delay.min(remaining)).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
            }
        }
    }
}

/// Parse a JSON value that may arrive as a quoted string ("123") or as a
/// JSON number (123) into a u128. DuneSQL serialises wide numerics as
/// strings but smaller fields can come back as numbers.
fn parse_json_u128(v: &serde_json::Value) -> Option<u128> {
    if let Some(s) = v.as_str() {
        return s.parse::<u128>().ok();
    }
    v.as_u64().map(|n| n as u128)
}

/// u64 variant of `parse_json_u128`.
fn parse_json_u64(v: &serde_json::Value) -> Option<u64> {
    if let Some(s) = v.as_str() {
        return s.parse::<u64>().ok();
    }
    v.as_u64()
}

/// Shared row-shaping for the `probe_address_activity` SQL — used by
/// both the persistent fast path and the legacy `execute_sql` fallback.
fn probe_from_rows(rows: &[serde_json::Value]) -> AddressActivityProbe {
    let mut probe = AddressActivityProbe::default();
    if let Some(row) = rows.first() {
        // DuneSQL can serialize numerics as quoted strings, so use
        // parse_json_u64 (handles both encodings) rather than as_u64,
        // which would silently read a stringified count as 0.
        let cnt = row.get("cnt").and_then(parse_json_u64).unwrap_or(0);
        let min_b = row.get("min_block").and_then(parse_json_u64).unwrap_or(0);
        let max_b = row.get("max_block").and_then(parse_json_u64).unwrap_or(0);
        probe.callee_call_count = cnt;
        probe.callee_min_block = min_b;
        probe.callee_max_block = max_b;
    }
    probe
}

fn parse_dune_rows(rows: &[serde_json::Value]) -> Vec<AddressTxSummary> {
    rows.iter()
        .filter_map(|row| {
            let hash_hex = row.get("hash")?.as_str()?;
            let hash = Felt::from_hex(hash_hex).ok()?;
            let nonce = row.get("nonce").and_then(|v| v.as_u64()).unwrap_or(0);
            let block_number = row
                .get("block_number")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let block_time = row
                .get("block_time")
                .and_then(|v| v.as_str())
                .and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f %Z"))
                        .ok()
                })
                .map(|dt| dt.timestamp() as u64)
                .unwrap_or(0);

            let execution_status = row
                .get("execution_status")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let status = match execution_status {
                "SUCCEEDED" => "OK",
                "REVERTED" => "REV",
                _ => "?",
            }
            .to_string();

            // actual_fee_amount typically arrives as a string (DuneSQL
            // serialises u128-sized numerics as strings), but smaller values
            // can come back as JSON numbers. Handle both; the old
            // `.or_else(|| as_u64().map(|_| "0"))` shape silently dropped
            // the numeric variant to zero.
            let total_fee_fri = row
                .get("actual_fee_amount")
                .and_then(parse_json_u128)
                .unwrap_or(0);

            let tip_val = row.get("tip").and_then(parse_json_u64).unwrap_or(0);

            let tx_type = row
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("INVOKE")
                .to_string();

            let sender = row
                .get("sender_address")
                .and_then(|v| v.as_str())
                .and_then(|s| Felt::from_hex(s).ok());

            Some(AddressTxSummary {
                hash,
                nonce,
                block_number,
                timestamp: block_time,
                endpoint_names: String::new(), // Decoded later from cached selectors
                total_fee_fri,
                tip: tip_val,
                tx_type,
                status,
                sender,
                called_contracts: Vec::new(), // Populated later from cached calldata
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both JSON forms must parse the same value: Dune serialises wide
    /// numerics (e.g. uint256 fees) as strings but smaller numerics as
    /// JSON numbers. The previous shape `as_str().or_else(|| as_u64()
    /// .map(|_| "0"))` silently dropped the numeric variant to zero — a
    /// regression-prone edge case.
    #[test]
    fn parse_json_u128_accepts_string_and_numeric_forms() {
        let s = serde_json::json!("12345678901234567890");
        assert_eq!(parse_json_u128(&s), Some(12345678901234567890u128));

        let n = serde_json::json!(42u64);
        assert_eq!(parse_json_u128(&n), Some(42u128));

        // Wide value that exceeds u64 must still round-trip via the
        // string path (Dune's uint256 serialisation).
        let huge_str = format!("{}", (u64::MAX as u128) + 1);
        let huge = serde_json::Value::String(huge_str);
        assert_eq!(parse_json_u128(&huge), Some((u64::MAX as u128) + 1));

        // Garbage and null both yield None — the caller falls back to 0.
        assert_eq!(parse_json_u128(&serde_json::json!("not-a-number")), None);
        assert_eq!(parse_json_u128(&serde_json::Value::Null), None);
    }

    #[test]
    fn parse_json_u64_accepts_string_and_numeric_forms() {
        let s = serde_json::json!("123456789");
        assert_eq!(parse_json_u64(&s), Some(123_456_789u64));

        let n = serde_json::json!(7u64);
        assert_eq!(parse_json_u64(&n), Some(7));

        // Above u64 in string form → None (would have parsed wrong as u64).
        let too_big = serde_json::Value::String(format!("{}", (u64::MAX as u128) + 1));
        assert_eq!(parse_json_u64(&too_big), None);

        assert_eq!(parse_json_u64(&serde_json::json!("nope")), None);
        assert_eq!(parse_json_u64(&serde_json::Value::Null), None);
    }

    /// `with_persistent_cache` must hydrate previously-persisted query_ids
    /// into the in-memory map so the fast path hits without a SQLite read.
    /// Guards the schema-init + hydration wiring as more `QueryShape`s land.
    #[test]
    fn with_persistent_cache_hydrates_in_memory_map() {
        // One (shape-name, id) per QueryShape variant — keep in sync with the
        // enum so every from_name mapping is exercised, not just a subset.
        const SEEDED_SHAPES: [(&str, u64); 6] = [
            ("probe_address_activity", 4242),
            ("probe_address_activity_delta", 4343),
            ("account_txs", 4444),
            ("account_txs_windowed", 4545),
            ("contract_calls_windowed", 5151),
            ("declare_tx", 6262),
        ];

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cache.db");
        // Seed persisted ids for several shapes — including ones added after
        // the original probe shape — so a regression in `QueryShape::from_name`
        // for the newer variants is caught, not just the probe mapping. Each
        // row carries the shape's *current* SQL so it survives the SQL-match
        // hydration check (a stale-SQL row would be evicted, not hydrated).
        {
            let db = Connection::open(&db_path).unwrap();
            db.execute_batch(
                "CREATE TABLE IF NOT EXISTS dune_persistent_queries (
                    shape TEXT PRIMARY KEY,
                    query_id INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    sql_text TEXT
                 );",
            )
            .unwrap();
            for (shape, qid) in SEEDED_SHAPES {
                let sql = QueryShape::from_name(shape).unwrap().sql();
                db.execute(
                    "INSERT INTO dune_persistent_queries (shape, query_id, created_at, sql_text) \
                     VALUES (?1, ?2, ?3, ?4)",
                    params![shape, qid as i64, 0i64, sql],
                )
                .unwrap();
            }
        }

        let client = DuneClient::new("test-key".to_string(), false).with_persistent_cache(&db_path);

        // Assert every seeded shape hydrated under its canonical name. Looking
        // up by the stored string (not via from_name) means a from_name
        // regression for any one variant surfaces as a missing key here.
        let map = client.persistent_ids.lock().unwrap();
        for (shape, qid) in SEEDED_SHAPES {
            assert_eq!(
                map.get(shape).copied(),
                Some(qid),
                "shape {shape} not hydrated"
            );
        }
    }

    /// A pre-`sql_text` table (created before this column existed) must migrate
    /// cleanly: `with_persistent_cache` adds the column via ALTER, and the
    /// rows — whose `sql_text` is now NULL — are treated as stale and evicted
    /// rather than hydrated, so the fast path recreates them with current SQL.
    #[test]
    fn with_persistent_cache_migrates_legacy_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cache.db");
        {
            // Legacy 3-column schema, no sql_text.
            let db = Connection::open(&db_path).unwrap();
            db.execute_batch(
                "CREATE TABLE IF NOT EXISTS dune_persistent_queries (
                    shape TEXT PRIMARY KEY,
                    query_id INTEGER NOT NULL,
                    created_at INTEGER NOT NULL
                 );",
            )
            .unwrap();
            db.execute(
                "INSERT INTO dune_persistent_queries (shape, query_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                params!["declare_tx", 6262i64, 0i64],
            )
            .unwrap();
        }

        let client = DuneClient::new("test-key".to_string(), false).with_persistent_cache(&db_path);

        // NULL sql_text → not hydrated...
        assert!(
            client
                .persistent_ids
                .lock()
                .unwrap()
                .get("declare_tx")
                .is_none(),
            "legacy NULL-sql row was hydrated instead of recreated"
        );
        // ...and the row was evicted (and the column now exists).
        let db = client.persistent_db.as_ref().unwrap().lock().unwrap();
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM dune_persistent_queries WHERE shape = 'declare_tx'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "legacy row not evicted after migration");
    }

    /// A persisted query_id whose stored SQL no longer matches the shape's
    /// current SQL (an in-place SQL edit, e.g. the issue #80 partition floor)
    /// must NOT be hydrated, and its row must be evicted so get_or_create
    /// recreates it with the new SQL. Matching rows still hydrate.
    #[test]
    fn with_persistent_cache_evicts_stale_sql() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cache.db");
        {
            let db = Connection::open(&db_path).unwrap();
            db.execute_batch(
                "CREATE TABLE IF NOT EXISTS dune_persistent_queries (
                    shape TEXT PRIMARY KEY,
                    query_id INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    sql_text TEXT
                 );",
            )
            .unwrap();
            // Matching SQL → hydrates.
            db.execute(
                "INSERT INTO dune_persistent_queries (shape, query_id, created_at, sql_text) \
                 VALUES (?1, ?2, ?3, ?4)",
                params!["declare_tx", 6262i64, 0i64, QueryShape::DeclareTx.sql()],
            )
            .unwrap();
            // Stale SQL → evicted.
            db.execute(
                "INSERT INTO dune_persistent_queries (shape, query_id, created_at, sql_text) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    "account_txs_windowed",
                    4545i64,
                    0i64,
                    "SELECT 1 -- pre-issue-80 SQL, no block_date floor"
                ],
            )
            .unwrap();
        }

        let client = DuneClient::new("test-key".to_string(), false).with_persistent_cache(&db_path);

        let map = client.persistent_ids.lock().unwrap();
        assert_eq!(
            map.get("declare_tx").copied(),
            Some(6262),
            "matching-SQL row should still hydrate"
        );
        assert!(
            map.get("account_txs_windowed").is_none(),
            "stale-SQL row was hydrated instead of recreated"
        );
        drop(map);

        let db = client.persistent_db.as_ref().unwrap().lock().unwrap();
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM dune_persistent_queries WHERE shape = 'account_txs_windowed'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "stale-SQL row not evicted from SQLite");
    }

    /// `evict_persistent_query` must clear a stale id from BOTH the in-memory
    /// map and the SQLite table — otherwise a restart re-hydrates the dead id
    /// and the fast path never recovers (the #78 regression).
    #[test]
    fn evict_persistent_query_clears_memory_and_db() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cache.db");
        {
            let db = Connection::open(&db_path).unwrap();
            db.execute_batch(
                "CREATE TABLE IF NOT EXISTS dune_persistent_queries (
                    shape TEXT PRIMARY KEY,
                    query_id INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    sql_text TEXT
                 );",
            )
            .unwrap();
            // Seed with the shape's current SQL so it survives the SQL-match
            // hydration check; this test exercises explicit eviction, not the
            // stale-SQL self-heal.
            db.execute(
                "INSERT INTO dune_persistent_queries (shape, query_id, created_at, sql_text) \
                 VALUES (?1, ?2, ?3, ?4)",
                params!["declare_tx", 6262i64, 0i64, QueryShape::DeclareTx.sql()],
            )
            .unwrap();
        }

        let client = DuneClient::new("test-key".to_string(), false).with_persistent_cache(&db_path);
        // Sanity: hydrated before eviction.
        assert_eq!(
            client
                .persistent_ids
                .lock()
                .unwrap()
                .get("declare_tx")
                .copied(),
            Some(6262)
        );

        client.evict_persistent_query(QueryShape::DeclareTx);

        // Gone from the in-memory map...
        assert!(
            client
                .persistent_ids
                .lock()
                .unwrap()
                .get("declare_tx")
                .is_none(),
            "stale id still in memory after evict"
        );
        // ...and from SQLite, so a re-hydrate won't bring it back.
        let db = client.persistent_db.as_ref().unwrap().lock().unwrap();
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM dune_persistent_queries WHERE shape = 'declare_tx'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "stale id still in SQLite after evict");
    }

    /// Public hybrid account (Cartridge Controller class) with both sender-side
    /// txs and heavy inbound traffic — see `src/network/address.rs` test.
    /// No ownership/activity implication.
    const HYBRID_TEST_ADDR: &str =
        "0x3a496b92d292386ad70dab94ae181a06d289440e3b632a2435721b4280874c4";

    /// Bug-guard (Bug 1): the probe must populate `sender_tx_count` and
    /// `callee_call_count` from DISTINCT queries, not the same number. For a
    /// hybrid account, sender txs are bounded by the on-chain nonce while
    /// callee calls are typically orders of magnitude larger — a probe that
    /// assigns `cnt -> both` (as the current implementation does) produces a
    /// wildly wrong Txs total label in the UI.
    ///
    /// Invariants asserted:
    ///   1. `sender_tx_count <= nonce + 1` (nonce = next-tx counter; all
    ///      prior nonces 0..=nonce-1 correspond to sender txs, at most).
    ///   2. `sender_tx_count != callee_call_count` for this address — they
    ///      reflect different roles and should not match. (If they happen to
    ///      match by pure coincidence, the test is still useful as a signal
    ///      to re-verify; the odds are negligible for a hot hybrid address.)
    #[tokio::test]
    #[ignore = "requires DUNE_API_KEY + APP_RPC_URL"]
    async fn probe_distinguishes_sender_from_callee() {
        use crate::data::DataSource;
        use crate::data::rpc::RpcDataSource;
        use starknet::core::types::Felt;

        dotenvy::dotenv().ok();
        let dune_key = std::env::var("DUNE_API_KEY").expect("DUNE_API_KEY");
        let rpc_url = std::env::var("APP_RPC_URL").expect("APP_RPC_URL");

        let dune = DuneClient::new(dune_key, true);
        let ds = RpcDataSource::new(&rpc_url);
        let address = Felt::from_hex(HYBRID_TEST_ADDR).unwrap();

        let probe = dune
            .probe_address_activity(address, None)
            .await
            .expect("probe_address_activity");
        let nonce_felt = ds.get_nonce(address).await.expect("get_nonce");
        let nonce = {
            // Felt -> u64 via little-endian bytes; nonces fit.
            let bytes = nonce_felt.to_bytes_be();
            u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]))
        };

        println!(
            "Probe: sender_tx_count={} callee_call_count={} (on-chain nonce={})",
            probe.sender_tx_count, probe.callee_call_count, nonce
        );

        // Invariant 1: sender_tx_count cannot exceed nonce (each sender tx
        // consumes exactly one nonce position; reverts also consume). Small
        // slack for edge cases (tx in flight, off-by-one at the boundary).
        assert!(
            probe.sender_tx_count <= nonce + 10,
            "sender_tx_count ({}) must not exceed nonce ({}+10): probe is counting the wrong thing",
            probe.sender_tx_count,
            nonce,
        );

        // Invariant 2: sender != callee for a hybrid address with known
        // heavy inbound traffic.
        assert_ne!(
            probe.sender_tx_count, probe.callee_call_count,
            "sender_tx_count ({}) must not equal callee_call_count ({}) \
             — the probe is populating both from a single query (Bug 1)",
            probe.sender_tx_count, probe.callee_call_count,
        );
    }
}
