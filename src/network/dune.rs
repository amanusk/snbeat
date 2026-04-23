use std::time::Duration;

use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, info};

use crate::data::types::{AddressTxSummary, ContractCallSummary};

const DUNE_API_BASE: &str = "https://api.dune.com/api/v1";

/// Dune Analytics API client for querying Starknet transaction history.
pub struct DuneClient {
    client: reqwest::Client,
    api_key: String,
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
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
        }
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
        let sql = format!(
            "SELECT hash, sender_address, nonce, execution_status, revert_reason, actual_fee_amount, \
             tip, block_number, block_time, type \
             FROM starknet.transactions \
             WHERE sender_address = {} \
             AND block_date >= date '2025-01-01' \
             ORDER BY nonce DESC \
             LIMIT {}",
            sender_hex, limit
        );

        debug!(sender = %sender_hex, "Querying Dune for account txs");
        let rows = self.execute_sql(&sql).await?;
        info!(rows = rows.len(), "Dune account txs query complete");
        Ok(parse_dune_rows(&rows))
    }

    /// Query calls TO a specific contract address using starknet.calls table.
    /// Returns ContractCallSummary objects sorted by block_number descending.
    pub async fn query_contract_calls(
        &self,
        contract: Felt,
        limit: u32,
    ) -> Result<Vec<ContractCallSummary>, String> {
        let contract_hex = format!("{:#066x}", contract);
        let sql = format!(
            "SELECT transaction_hash, caller_address, entry_point_selector, \
             block_number, block_time, revert_reason \
             FROM starknet.calls \
             WHERE contract_address = {} \
             AND block_date >= date '2024-01-01' \
             AND call_type = 'CALL' \
             ORDER BY block_number DESC \
             LIMIT {}",
            contract_hex, limit
        );

        debug!(contract = %contract_hex, "Creating Dune query for contract calls");

        let rows = self.execute_sql(&sql).await?;
        info!(rows = rows.len(), "Dune contract calls query complete");

        Ok(rows
            .iter()
            .filter_map(|row| {
                let tx_hash = Felt::from_hex(row.get("transaction_hash")?.as_str()?).ok()?;
                let caller = Felt::from_hex(row.get("caller_address")?.as_str()?).ok()?;
                let selector_hex = row.get("entry_point_selector")?.as_str().unwrap_or("");
                let function_name = selector_hex.to_string(); // Will be decoded by caller
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
                    total_fee_fri: 0, // Not in calls table
                    status,
                    nonce: None, // Not in calls table — merged in from RPC/pf path
                    tip: 0,
                })
            })
            .collect())
    }

    /// Windowed variant of `query_account_txs` — scoped to a block range for fast completion.
    pub async fn query_account_txs_windowed(
        &self,
        sender: Felt,
        from_block: u64,
        to_block: u64,
        limit: u32,
    ) -> Result<Vec<AddressTxSummary>, String> {
        let sender_hex = format!("{:#066x}", sender);
        let sql = format!(
            "SELECT hash, sender_address, nonce, execution_status, revert_reason, actual_fee_amount, \
             tip, block_number, block_time, type \
             FROM starknet.transactions \
             WHERE sender_address = {} \
             AND block_number BETWEEN {} AND {} \
             ORDER BY nonce DESC \
             LIMIT {}",
            sender_hex, from_block, to_block, limit
        );

        debug!(sender = %sender_hex, from_block, to_block, "Querying Dune for account txs (windowed)");
        let rows = self.execute_sql(&sql).await?;
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
        let date_clause = min_block_date
            .map(|d| format!("AND block_date >= date '{}' ", d.format("%Y-%m-%d")))
            .unwrap_or_default();
        // DuneSQL (Trino) uses bigint for block_number; any literal above i64::MAX
        // (9_223_372_036_854_775_807) is rejected at parse time as "Invalid numeric
        // literal" — so a caller passing u64::MAX as "no upper bound" would have
        // the whole query fail. Emit an open-ended predicate in that case.
        let range_clause = if to_block == u64::MAX {
            format!("AND block_number >= {}", from_block)
        } else {
            format!("AND block_number BETWEEN {} AND {}", from_block, to_block)
        };
        let sql = format!(
            "SELECT transaction_hash, caller_address, entry_point_selector, \
             block_number, block_time, revert_reason \
             FROM starknet.calls \
             WHERE contract_address = {} \
             {}\
             {} \
             AND call_type = 'CALL' \
             ORDER BY block_number DESC \
             LIMIT {}",
            contract_hex, date_clause, range_clause, limit
        );

        debug!(contract = %contract_hex, from_block, to_block, ?min_block_date, "Querying Dune for contract calls (windowed)");
        let rows = self.execute_sql(&sql).await?;
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
    pub async fn probe_address_activity(
        &self,
        address: Felt,
    ) -> Result<AddressActivityProbe, String> {
        let addr_hex = format!("{:#066x}", address);
        let sql = format!(
            "SELECT COUNT(*) AS cnt, \
               MIN(block_number) AS min_block, MAX(block_number) AS max_block \
             FROM starknet.events \
             WHERE from_address = {addr} \
             AND block_date >= date '2024-01-01'",
            addr = addr_hex
        );

        debug!(address = %addr_hex, "Dune events probe: checking address activity range");
        let rows = self.execute_sql(&sql).await?;

        let mut probe = AddressActivityProbe::default();
        if let Some(row) = rows.first() {
            let cnt = row.get("cnt").and_then(|v| v.as_u64()).unwrap_or(0);
            let min_b = row.get("min_block").and_then(|v| v.as_u64()).unwrap_or(0);
            let max_b = row.get("max_block").and_then(|v| v.as_u64()).unwrap_or(0);
            // Events-from-address counts inbound activity for account-contracts
            // and emitted events for pure contracts. Both populate callee fields;
            // sender_tx_count is left at 0 (unknown from this query — use nonce).
            probe.callee_call_count = cnt;
            probe.callee_min_block = min_b;
            probe.callee_max_block = max_b;
        }

        // If nothing found since 2024, try without date filter to catch pre-2024 activity.
        if !probe.has_activity() {
            debug!(address = %addr_hex, "Dune events probe: no activity since 2024, trying full range");
            let sql_full = format!(
                "SELECT COUNT(*) AS cnt, \
                   MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                 FROM starknet.events \
                 WHERE from_address = {addr}",
                addr = addr_hex
            );
            let rows_full = self.execute_sql(&sql_full).await?;
            if let Some(row) = rows_full.first() {
                let cnt = row.get("cnt").and_then(|v| v.as_u64()).unwrap_or(0);
                let min_b = row.get("min_block").and_then(|v| v.as_u64()).unwrap_or(0);
                let max_b = row.get("max_block").and_then(|v| v.as_u64()).unwrap_or(0);
                probe.callee_call_count = cnt;
                probe.callee_min_block = min_b;
                probe.callee_max_block = max_b;
            }
        }

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
    pub async fn probe_address_activity_delta(
        &self,
        address: Felt,
        from_block: u64,
    ) -> Result<AddressActivityProbe, String> {
        let addr_hex = format!("{:#066x}", address);
        let sql = format!(
            "SELECT COUNT(*) AS cnt, \
               MIN(block_number) AS min_block, MAX(block_number) AS max_block \
             FROM starknet.events \
             WHERE from_address = {addr} \
             AND block_number > {from}",
            addr = addr_hex,
            from = from_block,
        );

        debug!(address = %addr_hex, from_block, "Dune events probe (delta): extending activity range");
        let rows = self.execute_sql(&sql).await?;

        let mut probe = AddressActivityProbe::default();
        if let Some(row) = rows.first() {
            let cnt = row.get("cnt").and_then(|v| v.as_u64()).unwrap_or(0);
            let min_b = row.get("min_block").and_then(|v| v.as_u64()).unwrap_or(0);
            let max_b = row.get("max_block").and_then(|v| v.as_u64()).unwrap_or(0);
            probe.callee_call_count = cnt;
            probe.callee_min_block = min_b;
            probe.callee_max_block = max_b;
        }

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
        let sql = format!(
            "SELECT hash, sender_address, block_number, block_time \
             FROM starknet.transactions \
             WHERE type = 'DECLARE' AND class_hash = {} \
             ORDER BY block_number ASC \
             LIMIT 1",
            hash_hex
        );

        debug!(class_hash = %hash_hex, "Querying Dune for declare tx");
        let rows = self.execute_sql(&sql).await?;

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
            "is_private": true
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
        let mut attempts = 0;
        let result = loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            attempts += 1;

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
                    if attempts > 60 {
                        break Err("Dune query timed out (120s)".into());
                    }
                }
            }
        };

        // Archive the ephemeral query to avoid accumulating queries on the account.
        let _ = self
            .client
            .post(format!("{}/query/{}/archive", DUNE_API_BASE, query_id))
            .header("X-Dune-API-Key", &self.api_key)
            .send()
            .await;

        result
    }
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

            // actual_fee_amount comes as a string number
            let fee_str = row
                .get("actual_fee_amount")
                .and_then(|v| v.as_str().or_else(|| v.as_u64().map(|_| "0")))
                .unwrap_or("0");
            let total_fee_fri = fee_str.parse::<u128>().unwrap_or(0);

            let tip_val = row
                .get("tip")
                .and_then(|v| v.as_str().or_else(|| v.as_u64().map(|_| "0")))
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);

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
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let dune = DuneClient::new(dune_key);
        let ds = RpcDataSource::new(&rpc_url);
        let address = Felt::from_hex(HYBRID_TEST_ADDR).unwrap();

        let probe = dune
            .probe_address_activity(address)
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
