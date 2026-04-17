//! DefiLlama token price client with persistent SQLite cache.
//!
//! - Today's prices: refreshed at most once per `TODAY_REFRESH_SECS` (1 hour).
//! - Historical prices: cached forever, keyed on UTC date (`YYYY-MM-DD`).
//! - All `get_*` reads are sync cache lookups; `ensure_*` fetches and writes through.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{Connection, params};
use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, info, warn};

const DEFI_LLAMA_BASE: &str = "https://coins.llama.fi";
const TODAY_REFRESH_SECS: u64 = 3_600;
const TODAY_KEY: &str = "today";

const TRACKED_TOKEN_HEX: &[&str] = &[
    "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d", // STRK
    "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7", // ETH
    "0x0124aeb495b947201f5fac96fd1138e326ad86195b98df6dec9009158a533b49", // wBTC
    "0x033068f6539f8e6e6b131e6b2b814e6c34a5224bc66947c47dab9dfee93b35fb", // USDC (native)
    "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8", // USDC (bridged)
    "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8", // USDT
];

/// The set of tokens we surface USD prices for. Mirrors the tokens fetched
/// by `network::address::fetch_token_balances`.
pub static TRACKED_TOKENS: LazyLock<Vec<Felt>> = LazyLock::new(|| {
    TRACKED_TOKEN_HEX
        .iter()
        .filter_map(|h| Felt::from_hex(h).ok())
        .collect()
});

/// True when `token` is one of the addresses we display USD prices for.
pub fn is_tracked(token: &Felt) -> bool {
    TRACKED_TOKENS.contains(token)
}

pub struct PriceClient {
    client: reqwest::Client,
    db: Mutex<Connection>,
}

#[derive(Debug, Deserialize)]
struct PriceResponse {
    #[serde(default)]
    coins: HashMap<String, CoinPrice>,
}

#[derive(Debug, Deserialize)]
struct CoinPrice {
    price: f64,
}

impl PriceClient {
    pub fn new(cache_db_path: &Path) -> Result<Self, String> {
        let db = Connection::open(cache_db_path)
            .map_err(|e| format!("Failed to open price cache db: {e}"))?;

        db.execute_batch("PRAGMA journal_mode=WAL;")
            .map_err(|e| format!("Failed to set WAL mode: {e}"))?;

        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS token_prices (
                token_address TEXT NOT NULL,
                date          TEXT NOT NULL,
                price_usd     REAL NOT NULL,
                fetched_at    INTEGER NOT NULL,
                PRIMARY KEY (token_address, date)
            );",
        )
        .map_err(|e| format!("Failed to init token_prices table: {e}"))?;

        Ok(Self {
            client: reqwest::Client::new(),
            db: Mutex::new(db),
        })
    }

    pub fn get_today_price(&self, token: &Felt) -> Option<f64> {
        self.read_price(&token_key(token), TODAY_KEY)
    }

    pub fn get_historic_price(&self, token: &Felt, timestamp: u64) -> Option<f64> {
        let date = date_key_for_timestamp(timestamp)?;
        self.read_price(&token_key(token), &date)
    }

    /// Fetch missing/stale today prices. Returns true if the cache changed.
    pub async fn ensure_today(&self, tokens: &[Felt]) -> bool {
        let now = unix_now();
        let stale_cutoff = now.saturating_sub(TODAY_REFRESH_SECS);

        let to_fetch: Vec<Felt> = tokens
            .iter()
            .copied()
            .filter(|t| !self.is_today_fresh(t, stale_cutoff))
            .collect();
        if to_fetch.is_empty() {
            return false;
        }

        let url = format!(
            "{}/prices/current/{}",
            DEFI_LLAMA_BASE,
            join_tokens(&to_fetch)
        );

        match self.fetch(&url).await {
            Ok(prices) => {
                if prices.is_empty() {
                    debug!(
                        count = to_fetch.len(),
                        "DefiLlama returned no current prices"
                    );
                    return false;
                }
                for (addr, price) in &prices {
                    self.write_price(addr, TODAY_KEY, *price, now);
                }
                info!(
                    count = prices.len(),
                    "Fetched current token prices from DefiLlama"
                );
                true
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch current token prices");
                false
            }
        }
    }

    /// Fetch missing historic prices for `(token, timestamp)` pairs. Day granularity.
    /// Distinct days are fetched concurrently.
    pub async fn ensure_historic(&self, requests: &[(Felt, u64)]) -> bool {
        let mut wanted: HashMap<String, HashSet<Felt>> = HashMap::new();
        for (token, ts) in requests {
            let Some(date) = date_key_for_timestamp(*ts) else {
                continue;
            };
            if self.read_price(&token_key(token), &date).is_some() {
                continue;
            }
            wanted.entry(date).or_default().insert(*token);
        }
        if wanted.is_empty() {
            return false;
        }

        let now = unix_now();
        let futs = wanted.into_iter().filter_map(|(date, tokens)| {
            let day_ts = date_to_unix(&date)?;
            let token_list: Vec<Felt> = tokens.into_iter().collect();
            let url = format!(
                "{}/prices/historical/{}/{}",
                DEFI_LLAMA_BASE,
                day_ts,
                join_tokens(&token_list)
            );
            Some(async move { (date, self.fetch(&url).await) })
        });
        let results = futures::future::join_all(futs).await;

        let mut updated = false;
        for (date, res) in results {
            match res {
                Ok(prices) => {
                    if prices.is_empty() {
                        debug!(date = %date, "DefiLlama returned no historic prices");
                        continue;
                    }
                    for (addr, price) in &prices {
                        self.write_price(addr, &date, *price, now);
                    }
                    info!(
                        date = %date,
                        count = prices.len(),
                        "Fetched historic token prices from DefiLlama"
                    );
                    updated = true;
                }
                Err(e) => {
                    warn!(date = %date, error = %e, "Failed to fetch historic token prices");
                }
            }
        }
        updated
    }

    // --- private helpers ---

    fn is_today_fresh(&self, token: &Felt, cutoff: u64) -> bool {
        let Ok(db) = self.db.lock() else {
            return false;
        };
        db.query_row(
            "SELECT fetched_at FROM token_prices WHERE token_address = ?1 AND date = ?2",
            params![token_key(token), TODAY_KEY],
            |row| row.get::<_, i64>(0),
        )
        .ok()
        .map(|fa| (fa as u64) >= cutoff)
        .unwrap_or(false)
    }

    fn read_price(&self, token_key: &str, date: &str) -> Option<f64> {
        let db = self.db.lock().ok()?;
        db.query_row(
            "SELECT price_usd FROM token_prices WHERE token_address = ?1 AND date = ?2",
            params![token_key, date],
            |row| row.get::<_, f64>(0),
        )
        .ok()
    }

    fn write_price(&self, token_key: &str, date: &str, price: f64, now: u64) {
        let Ok(db) = self.db.lock() else { return };
        let _ = db.execute(
            "INSERT OR REPLACE INTO token_prices (token_address, date, price_usd, fetched_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![token_key, date, price, now as i64],
        );
    }

    async fn fetch(&self, url: &str) -> Result<Vec<(String, f64)>, String> {
        debug!(url = %url, "DefiLlama price request");
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("HTTP {status}"));
        }
        let body: PriceResponse = resp
            .json()
            .await
            .map_err(|e| format!("parse failed: {e}"))?;
        Ok(body
            .coins
            .into_iter()
            .filter_map(|(k, v)| {
                let addr = k.strip_prefix("starknet:")?.to_string();
                Some((addr, v.price))
            })
            .collect())
    }
}

/// Canonical token key — must match the address format DefiLlama returns.
fn token_key(token: &Felt) -> String {
    format!("{:#x}", token)
}

fn join_tokens(tokens: &[Felt]) -> String {
    tokens
        .iter()
        .map(|t| format!("starknet:{}", token_key(t)))
        .collect::<Vec<_>>()
        .join(",")
}

fn date_key_for_timestamp(ts: u64) -> Option<String> {
    if ts == 0 {
        return None;
    }
    let dt = DateTime::<Utc>::from_timestamp(ts as i64, 0)?;
    Some(dt.format("%Y-%m-%d").to_string())
}

fn date_to_unix(date: &str) -> Option<i64> {
    let nd = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    let dt = nd.and_hms_opt(0, 0, 0)?.and_utc();
    Some(dt.timestamp())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_key_round_trips() {
        let ts = 1_700_000_000u64;
        let key = date_key_for_timestamp(ts).unwrap();
        assert_eq!(key, "2023-11-14");
        let back = date_to_unix(&key).unwrap();
        assert!(back <= ts as i64);
    }

    #[test]
    fn zero_timestamp_returns_none() {
        assert!(date_key_for_timestamp(0).is_none());
    }

    #[test]
    fn token_key_is_lowercase_no_padding() {
        let f =
            Felt::from_hex("0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7")
                .unwrap();
        let k = token_key(&f);
        assert!(k.starts_with("0x"));
        assert_eq!(k, k.to_lowercase());
    }

    #[test]
    fn cache_read_and_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.db");
        let pc = PriceClient::new(&path).unwrap();
        let token =
            Felt::from_hex("0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7")
                .unwrap();
        assert!(pc.get_today_price(&token).is_none());
        pc.write_price(&token_key(&token), TODAY_KEY, 1234.5, unix_now());
        assert_eq!(pc.get_today_price(&token), Some(1234.5));

        let ts = 1_700_000_000u64;
        let date = date_key_for_timestamp(ts).unwrap();
        pc.write_price(&token_key(&token), &date, 999.0, unix_now());
        assert_eq!(pc.get_historic_price(&token, ts), Some(999.0));
    }

    #[test]
    fn tracked_tokens_includes_known_addresses() {
        assert!(is_tracked(
            &Felt::from_hex("0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7")
                .unwrap()
        ));
        assert!(!is_tracked(&Felt::from_hex("0x1").unwrap()));
    }
}
