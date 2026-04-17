use crate::data::types::SnEvent;
use serde::Deserialize;
use starknet::core::types::Felt;
use std::time::Duration;
use tracing::debug;

/// `transaction_executed` event selector — emitted once per invoke by every account contract.
/// Shared with `src/data/rpc.rs` so both paths use the same filter.
pub const TRANSACTION_EXECUTED_SELECTOR: &str =
    "0x01dcde06aabdbca2f80aa51392b345d7549d7757aa855f7e37f5d335ac8243b1";

/// HTTP client for the pf-query service.
pub struct PathfinderClient {
    client: reqwest::Client,
    base_url: String,
}

/// A single nonce-update entry from the Pathfinder DB.
#[derive(Debug, Clone, Deserialize)]
pub struct NonceEntry {
    pub block_number: u64,
    pub nonce: u64,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub latest_block: u64,
}

/// Full transaction info decoded from PF block blobs.
#[derive(Debug, Clone, Deserialize)]
pub struct SenderTxEntry {
    pub hash: String,
    #[serde(default)]
    pub sender_address: Option<String>,
    pub nonce: Option<u64>,
    pub block_number: u64,
    pub timestamp: u64,
    pub tx_type: String,
    pub actual_fee: String,
    pub tip: u64,
    pub status: String,
    pub revert_reason: Option<String>,
}

/// Class hash update entry.
#[derive(Debug, Clone, Deserialize)]
pub struct ClassHashEntry {
    pub block_number: u64,
    pub class_hash: String,
}

/// Transaction hash lookup result.
#[derive(Debug, Clone, Deserialize)]
pub struct TxHashLookup {
    pub block_number: u64,
    pub tx_index: u64,
}

/// Per-tx data returned by `POST /txs-by-hash`.
///
/// Covers everything the address-view enrichment path needs without an RPC
/// round trip: multicall calldata (for endpoint name decoding), sender,
/// nonce, fee, status, and block timestamp.
#[derive(Debug, Clone, Deserialize)]
pub struct TxByHashData {
    pub hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    #[allow(dead_code)]
    pub tx_index: u64,
    pub sender: String,
    pub nonce: Option<u64>,
    pub tx_type: String,
    /// Calldata felts, `0x`-prefixed hex. For invoke txs this is the multicall
    /// payload (matches RPC `InvokeTransaction.calldata`); empty for declare.
    pub calldata: Vec<String>,
    pub actual_fee: String,
    #[allow(dead_code)]
    pub tip: u64,
    pub status: String,
    pub revert_reason: Option<String>,
}

/// Entry from `GET /block-timestamps`.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockTimestamp {
    pub block_number: u64,
    pub timestamp: u64,
}

/// A contract deployed with a given class hash.
#[derive(Debug, Clone, Deserialize)]
pub struct ContractByClassEntry {
    pub contract_address: String,
    pub block_number: u64,
}

/// Declaration block info for a class hash.
#[derive(Debug, Clone, Deserialize)]
pub struct ClassDeclarationInfo {
    pub block_number: u64,
}

/// Raw /contract-events event payload (hex strings, as pf-query returns them).
#[derive(Debug, Clone, Deserialize)]
struct PfContractEvent {
    #[allow(dead_code)]
    tx_index: usize,
    event_index: usize,
    tx_hash: String,
    from_address: String,
    keys: Vec<String>,
    data: Vec<String>,
    block_number: u64,
    #[allow(dead_code)]
    #[serde(default)]
    timestamp: u64,
}

/// Paginated /contract-events response.
#[derive(Debug, Clone, Deserialize)]
struct PfContractEventsResponse {
    events: Vec<PfContractEvent>,
    continuation_token: Option<u64>,
}

/// Serialize a positional key filter as pf-query expects:
/// groups separated by `;`, OR-keys within a group by `,`.
/// Empty groups => wildcard at that position.
fn encode_keys_filter(keys: &[Vec<Felt>]) -> Option<String> {
    if keys.is_empty() {
        return None;
    }
    let parts: Vec<String> = keys
        .iter()
        .map(|group| {
            group
                .iter()
                .map(|k| format!("{:#x}", k))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    Some(parts.join(";"))
}

fn parse_felt(s: &str) -> anyhow::Result<Felt> {
    Felt::from_hex(s).map_err(|e| anyhow::anyhow!("bad hex {s}: {e}"))
}

impl PfContractEvent {
    fn into_sn_event(self) -> anyhow::Result<SnEvent> {
        let from_address = parse_felt(&self.from_address)?;
        let transaction_hash = parse_felt(&self.tx_hash)?;
        let keys = self
            .keys
            .iter()
            .map(|k| parse_felt(k))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let data = self
            .data
            .iter()
            .map(|d| parse_felt(d))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(SnEvent {
            from_address,
            keys,
            data,
            transaction_hash,
            block_number: self.block_number,
            event_index: self.event_index as u64,
        })
    }
}

impl PathfinderClient {
    pub fn new(base_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build reqwest client");
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Fetch the nonce-update history for an address (with timestamps).
    /// Returns entries ordered by block_number DESC (most recent first).
    pub async fn get_nonce_history(
        &self,
        address: Felt,
        limit: u32,
    ) -> anyhow::Result<Vec<NonceEntry>> {
        let addr_hex = format!("{:#x}", address);
        let url = format!(
            "{}/nonce-history/{}?limit={}",
            self.base_url, addr_hex, limit
        );
        debug!(url = %url, "Fetching nonce history from pf-query");
        let entries = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<NonceEntry>>()
            .await?;
        Ok(entries)
    }

    /// Fetch full decoded transaction history for an account.
    /// Combines nonce_updates + block blob decoding server-side.
    pub async fn get_sender_txs(
        &self,
        address: Felt,
        limit: u32,
    ) -> anyhow::Result<Vec<SenderTxEntry>> {
        let addr_hex = format!("{:#x}", address);
        let url = format!("{}/sender-txs/{}?limit={}", self.base_url, addr_hex, limit);
        debug!(url = %url, "Fetching sender txs from pf-query");
        let entries = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<SenderTxEntry>>()
            .await?;
        Ok(entries)
    }

    /// Fetch class hash history for an address.
    pub async fn get_class_history(&self, address: Felt) -> anyhow::Result<Vec<ClassHashEntry>> {
        let addr_hex = format!("{:#x}", address);
        let url = format!("{}/class-history/{}", self.base_url, addr_hex);
        debug!(url = %url, "Fetching class history from pf-query");
        let entries = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<ClassHashEntry>>()
            .await?;
        Ok(entries)
    }

    /// Look up a tx hash to find its block number and index.
    pub async fn get_tx_block(&self, tx_hash: Felt) -> anyhow::Result<TxHashLookup> {
        let hash_hex = format!("{:#x}", tx_hash);
        let url = format!("{}/tx-by-hash/{}", self.base_url, hash_hex);
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<TxHashLookup>()
            .await?;
        Ok(resp)
    }

    /// Bulk-fetch tx + receipt data by hash in one round trip.
    ///
    /// Hashes not present in the Pathfinder DB are silently omitted from the
    /// response — callers should diff requested vs returned and fall back
    /// (e.g. to RPC) for missing ones.
    pub async fn get_txs_by_hash(&self, hashes: &[Felt]) -> anyhow::Result<Vec<TxByHashData>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let url = format!("{}/txs-by-hash", self.base_url);
        let hashes_hex: Vec<String> = hashes.iter().map(|h| format!("{:#x}", h)).collect();
        let body = serde_json::json!({ "hashes": hashes_hex });
        debug!(url = %url, count = hashes.len(), "Fetching txs by hash from pf-query");
        let entries = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<TxByHashData>>()
            .await?;
        Ok(entries)
    }

    /// Bulk-fetch block timestamps over an inclusive range.
    pub async fn get_block_timestamps(
        &self,
        from: u64,
        to: u64,
    ) -> anyhow::Result<Vec<BlockTimestamp>> {
        let url = format!("{}/block-timestamps?from={}&to={}", self.base_url, from, to);
        debug!(url = %url, "Fetching block timestamps from pf-query");
        let entries = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<BlockTimestamp>>()
            .await?;
        Ok(entries)
    }

    /// Fetch all contracts deployed with a given class hash.
    pub async fn get_contracts_by_class(
        &self,
        class_hash: Felt,
    ) -> anyhow::Result<Vec<ContractByClassEntry>> {
        let hash_hex = format!("{:#x}", class_hash);
        let url = format!("{}/contracts-by-class/{}", self.base_url, hash_hex);
        debug!(url = %url, "Fetching contracts by class from pf-query");
        let entries = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<ContractByClassEntry>>()
            .await?;
        Ok(entries)
    }

    /// Fetch the block number where a class was first declared.
    pub async fn get_class_declaration(&self, class_hash: Felt) -> anyhow::Result<u64> {
        let hash_hex = format!("{:#x}", class_hash);
        let url = format!("{}/class-declaration/{}", self.base_url, hash_hex);
        debug!(url = %url, "Fetching class declaration from pf-query");
        let resp: ClassDeclarationInfo = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp.block_number)
    }

    /// Fetch events emitted by a contract / account, newest-first.
    ///
    /// - `keys`: positional filter groups (empty = unfiltered).
    ///   For accounts, pass `&[vec![TRANSACTION_EXECUTED_SELECTOR.parse()?]]`
    ///   (or use [`PathfinderClient::get_events_for_address`]).
    /// - `continuation_token`: pass `None` for first page, or the token from
    ///   the previous response for subsequent pages.
    ///
    /// Returns `(events, next_continuation_token)`. When the token is `None`,
    /// the range `[from_block, to_block]` has been fully scanned.
    pub async fn get_contract_events(
        &self,
        address: Felt,
        from_block: u64,
        to_block: Option<u64>,
        keys: &[Vec<Felt>],
        limit: u32,
        continuation_token: Option<u64>,
    ) -> anyhow::Result<(Vec<SnEvent>, Option<u64>)> {
        let addr_hex = format!("{:#x}", address);
        let mut url = format!(
            "{}/contract-events/{}?from_block={}&limit={}",
            self.base_url, addr_hex, from_block, limit
        );
        if let Some(to) = to_block {
            url.push_str(&format!("&to_block={to}"));
        }
        if let Some(tok) = continuation_token {
            url.push_str(&format!("&continuation_token={tok}"));
        }
        if let Some(k) = encode_keys_filter(keys) {
            // `,` and `;` are sub-delimiters in RFC 3986 but valid in query values
            // per the generic syntax. Percent-encode `;` defensively since some
            // intermediaries treat it as a pair separator. `,` stays literal.
            let encoded = k.replace(';', "%3B");
            url.push_str(&format!("&keys={encoded}"));
        }

        debug!(url = %url, "Fetching contract events from pf-query");
        let resp: PfContractEventsResponse = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let events = resp
            .events
            .into_iter()
            .map(PfContractEvent::into_sn_event)
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((events, resp.continuation_token))
    }

    /// Convenience: fetch account-emitted `transaction_executed` events for an address.
    pub async fn get_events_for_address(
        &self,
        address: Felt,
        from_block: u64,
        to_block: Option<u64>,
        limit: u32,
        continuation_token: Option<u64>,
    ) -> anyhow::Result<(Vec<SnEvent>, Option<u64>)> {
        let selector = Felt::from_hex(TRANSACTION_EXECUTED_SELECTOR)
            .expect("TRANSACTION_EXECUTED_SELECTOR is a valid felt");
        let keys = vec![vec![selector]];
        self.get_contract_events(
            address,
            from_block,
            to_block,
            &keys,
            limit,
            continuation_token,
        )
        .await
    }

    pub async fn health(&self) -> anyhow::Result<HealthResponse> {
        let url = format!("{}/health", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()?
            .json::<HealthResponse>()
            .await?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    //! Benchmark & correctness tests that require live RPC + pf-query endpoints.
    //! Run with:
    //!   APP_RPC_URL=... APP_PATHFINDER_SERVICE_URL=... \
    //!   BENCH_EVENTS_ADDRS="0xabc,account,label1;0xdef,contract,label2" \
    //!   cargo test --release -- --ignored bench_events_rpc_vs_pathfinder --nocapture
    use super::*;
    use crate::data::DataSource;
    use crate::data::rpc::RpcDataSource;
    use std::collections::HashSet;
    use std::time::Instant;

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Role {
        Account,
        Contract,
    }

    impl Role {
        fn parse(s: &str) -> anyhow::Result<Self> {
            match s {
                "account" => Ok(Role::Account),
                "contract" => Ok(Role::Contract),
                other => anyhow::bail!("unknown role {other:?}, expected 'account' or 'contract'"),
            }
        }
    }

    struct Case {
        addr: Felt,
        role: Role,
        label: String,
    }

    fn parse_cases(raw: &str) -> anyhow::Result<Vec<Case>> {
        raw.split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|entry| {
                let parts: Vec<&str> = entry.split(',').map(str::trim).collect();
                anyhow::ensure!(
                    parts.len() == 3,
                    "bad case {entry:?}: expected 'addr,role,label'"
                );
                Ok(Case {
                    addr: Felt::from_hex(parts[0])
                        .map_err(|e| anyhow::anyhow!("bad addr {}: {e}", parts[0]))?,
                    role: Role::parse(parts[1])?,
                    label: parts[2].to_string(),
                })
            })
            .collect()
    }

    /// Identity key for cross-source event comparison. Uses
    /// `(tx_hash, block_number, keys, data)` — resilient to `event_index`
    /// differences between RPC and pathfinder paths.
    fn event_key(e: &SnEvent) -> (Felt, u64, Vec<Felt>, Vec<Felt>) {
        (
            e.transaction_hash,
            e.block_number,
            e.keys.clone(),
            e.data.clone(),
        )
    }

    fn event_set(events: &[SnEvent]) -> HashSet<(Felt, u64, Vec<Felt>, Vec<Felt>)> {
        events.iter().map(event_key).collect()
    }

    /// Compare RPC vs PF event sets within the overlap block range.
    /// Returns (matched, rpc_only, pf_only).
    fn diff_sets(rpc: &[SnEvent], pf: &[SnEvent]) -> (usize, usize, usize) {
        // Only compare events within the block range that BOTH sides cover.
        let rpc_min = rpc.iter().map(|e| e.block_number).min().unwrap_or(u64::MAX);
        let rpc_max = rpc.iter().map(|e| e.block_number).max().unwrap_or(0);
        let pf_min = pf.iter().map(|e| e.block_number).min().unwrap_or(u64::MAX);
        let pf_max = pf.iter().map(|e| e.block_number).max().unwrap_or(0);
        let lo = rpc_min.max(pf_min);
        let hi = rpc_max.min(pf_max);
        if lo > hi {
            return (0, rpc.len(), pf.len());
        }

        let rpc_in: Vec<_> = rpc
            .iter()
            .filter(|e| e.block_number >= lo && e.block_number <= hi)
            .cloned()
            .collect();
        let pf_in: Vec<_> = pf
            .iter()
            .filter(|e| e.block_number >= lo && e.block_number <= hi)
            .cloned()
            .collect();
        let rpc_set = event_set(&rpc_in);
        let pf_set = event_set(&pf_in);
        let matched = rpc_set.intersection(&pf_set).count();
        let rpc_only = rpc_set.difference(&pf_set).count();
        let pf_only = pf_set.difference(&rpc_set).count();
        (matched, rpc_only, pf_only)
    }

    #[tokio::test]
    #[ignore]
    async fn bench_events_rpc_vs_pathfinder() -> anyhow::Result<()> {
        let rpc_url = std::env::var("APP_RPC_URL")
            .map_err(|_| anyhow::anyhow!("APP_RPC_URL must be set for this benchmark"))?;
        let pf_url = std::env::var("APP_PATHFINDER_SERVICE_URL").map_err(|_| {
            anyhow::anyhow!("APP_PATHFINDER_SERVICE_URL must be set for this benchmark")
        })?;
        let cases_raw = std::env::var("BENCH_EVENTS_ADDRS").map_err(|_| {
            anyhow::anyhow!(
                "BENCH_EVENTS_ADDRS must be set, e.g. \
                 '0x213...,account,hi-acct;0x5dd...,contract,hi-ctr'"
            )
        })?;
        let cases = parse_cases(&cases_raw)?;

        let rpc = RpcDataSource::new(&rpc_url);
        let pf = PathfinderClient::new(pf_url);

        let latest = pf.health().await?.latest_block;

        // Two scenarios:
        //  - "cold"  : matches the app's initial fetch window in
        //              src/network/address.rs (10k blocks for contracts,
        //              5k for accounts).
        //  - "scroll": simulates scroll-to-bottom paging over a deeper range.
        const PAGE_LIMIT: u32 = 500;
        // Time-to-first-event probe size (lower = closer to "first byte").
        const TTFE_LIMIT: u32 = 1;

        let scenarios: [(&str, u64, u64); 2] = [
            // name, account_window, contract_window
            ("cold", 5_000, 10_000),
            ("scroll", 500_000, 500_000),
        ];

        println!();
        println!(
            "# events bench — latest={latest}, page_limit={PAGE_LIMIT}, ttfe_limit={TTFE_LIMIT}"
        );
        println!();
        println!(
            "| scenario | label | role | window | source | ttfe_ms | page_ms | events | matched | rpc_only | pf_only |"
        );
        println!(
            "|----------|-------|------|--------|--------|---------|---------|--------|---------|----------|---------|"
        );

        for (scenario, acct_win, ctr_win) in scenarios.iter() {
            for case in &cases {
                let Case { addr, role, label } = case;
                let window = match role {
                    Role::Account => *acct_win,
                    Role::Contract => *ctr_win,
                };
                let from_block = latest.saturating_sub(window);

                // --- TTFE: fetch the very first event only. ---
                let rpc_ttfe = {
                    let t = Instant::now();
                    let res = match role {
                        Role::Account => {
                            rpc.get_events_for_address(
                                *addr,
                                Some(from_block),
                                None,
                                TTFE_LIMIT as usize,
                            )
                            .await
                        }
                        Role::Contract => {
                            rpc.get_contract_events(
                                *addr,
                                Some(from_block),
                                None,
                                TTFE_LIMIT as usize,
                            )
                            .await
                        }
                    };
                    match res {
                        Ok(_) => Some(t.elapsed().as_millis()),
                        Err(e) => {
                            eprintln!("rpc ttfe error for {label}: {e}");
                            None
                        }
                    }
                };
                let pf_ttfe = {
                    let t = Instant::now();
                    let res = match role {
                        Role::Account => {
                            pf.get_events_for_address(
                                *addr,
                                from_block,
                                Some(latest),
                                TTFE_LIMIT,
                                None,
                            )
                            .await
                        }
                        Role::Contract => {
                            pf.get_contract_events(
                                *addr,
                                from_block,
                                Some(latest),
                                &[],
                                TTFE_LIMIT,
                                None,
                            )
                            .await
                        }
                    };
                    match res {
                        Ok(_) => Some(t.elapsed().as_millis()),
                        Err(e) => {
                            eprintln!("pf ttfe error for {label}: {e}");
                            None
                        }
                    }
                };

                // --- Full page: fetch up to PAGE_LIMIT events. ---
                let t = Instant::now();
                let rpc_events = match role {
                    Role::Account => {
                        rpc.get_events_for_address(
                            *addr,
                            Some(from_block),
                            None,
                            PAGE_LIMIT as usize,
                        )
                        .await
                    }
                    Role::Contract => {
                        rpc.get_contract_events(*addr, Some(from_block), None, PAGE_LIMIT as usize)
                            .await
                    }
                }
                .unwrap_or_else(|e| {
                    eprintln!("rpc page error for {label}: {e}");
                    Vec::new()
                });
                let rpc_page_ms = t.elapsed().as_millis();

                let t = Instant::now();
                let (pf_events, _tok) = match role {
                    Role::Account => pf
                        .get_events_for_address(*addr, from_block, Some(latest), PAGE_LIMIT, None)
                        .await
                        .unwrap_or_else(|e| {
                            eprintln!("pf page error for {label}: {e}");
                            (Vec::new(), None)
                        }),
                    Role::Contract => pf
                        .get_contract_events(*addr, from_block, Some(latest), &[], PAGE_LIMIT, None)
                        .await
                        .unwrap_or_else(|e| {
                            eprintln!("pf page error for {label}: {e}");
                            (Vec::new(), None)
                        }),
                };
                let pf_page_ms = t.elapsed().as_millis();

                let (matched, rpc_only, pf_only) = diff_sets(&rpc_events, &pf_events);
                let role_s = match role {
                    Role::Account => "account",
                    Role::Contract => "contract",
                };
                let ttfe_str = |v: Option<u128>| match v {
                    Some(ms) => ms.to_string(),
                    None => "ERR".to_string(),
                };

                println!(
                    "| {scenario} | {label} | {role_s} | {window} | rpc | {} | {rpc_page_ms} | {} | {matched} | {rpc_only} | {pf_only} |",
                    ttfe_str(rpc_ttfe),
                    rpc_events.len()
                );
                println!(
                    "| {scenario} | {label} | {role_s} | {window} | pf  | {} | {pf_page_ms} | {} |         |          |         |",
                    ttfe_str(pf_ttfe),
                    pf_events.len()
                );
            }
        }

        Ok(())
    }

    #[test]
    fn encode_keys_filter_shapes() {
        let f1 = Felt::from_hex("0x1").unwrap();
        let f2 = Felt::from_hex("0x2").unwrap();
        let f3 = Felt::from_hex("0x3").unwrap();

        assert_eq!(encode_keys_filter(&[]), None);
        assert_eq!(
            encode_keys_filter(&[vec![f1]]).unwrap(),
            "0x1",
            "single group, single key"
        );
        assert_eq!(
            encode_keys_filter(&[vec![f1, f2]]).unwrap(),
            "0x1,0x2",
            "single group, OR'd keys"
        );
        assert_eq!(
            encode_keys_filter(&[vec![f1], vec![], vec![f3]]).unwrap(),
            "0x1;;0x3",
            "empty middle group is a wildcard"
        );
    }

    /// Local re-implementation of `pf-query::parse_keys_filter` used as a
    /// contract check: anything `encode_keys_filter` produces must be parseable
    /// by the pf-query side with the same semantics. The two live in separate
    /// crates, so we mirror the parser here and assert round-trip equivalence.
    /// If this ever drifts, update both sides at once.
    fn parse_keys_filter_mirror(raw: &str) -> Vec<Vec<Felt>> {
        raw.split(';')
            .map(|group_str| {
                group_str
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|k| Felt::from_hex(k).expect("valid hex"))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn encode_keys_filter_round_trips_through_pf_query_parser() {
        let f1 = Felt::from_hex("0x1").unwrap();
        let f2 = Felt::from_hex("0x2").unwrap();
        let f3 = Felt::from_hex("0x3").unwrap();
        let cases: Vec<Vec<Vec<Felt>>> = vec![
            vec![vec![f1]],
            vec![vec![f1, f2]],
            vec![vec![f1], vec![], vec![f3]],
            vec![vec![f1, f2, f3]],
        ];
        for input in cases {
            let encoded = encode_keys_filter(&input).expect("non-empty filter encodes");
            let round_tripped = parse_keys_filter_mirror(&encoded);
            assert_eq!(
                round_tripped, input,
                "round-trip mismatch for {input:?} (encoded = {encoded:?})"
            );
        }
    }
}
