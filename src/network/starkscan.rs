//! Starkscan REST client: indexed ERC-20 transfers touching an address.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::data::types::TokenTransfer;

pub const DEFAULT_API_BASE: &str = "https://api.starkscan.co/v1/SN_MAIN";
/// Server-side clamp on page size.
pub const PAGE_LIMIT: u32 = 100;
/// After a 429/5xx, skip Starkscan for this long so the RPC fallback takes over.
const ERROR_BACKOFF_SECS: u64 = 30;

pub struct StarkscanClient {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    backoff_until: AtomicU64,
    /// Set on 401/403: the key is rejected, so stop calling for the session.
    disabled: AtomicBool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransfersResponse {
    #[serde(default)]
    items: Vec<TransferItem>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransferItem {
    block_number: u64,
    timestamp_iso: Option<String>,
    #[serde(default)]
    tx_index: u64,
    #[serde(default)]
    log_index: u64,
    tx_hash: String,
    token_address: String,
    from_address: String,
    to_address: String,
    raw_value: Option<String>,
    amount: Option<String>,
    token_symbol: Option<String>,
    token_decimals: Option<u8>,
    #[serde(default)]
    historical_usd: Option<HistoricalUsd>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoricalUsd {
    value_usd: Option<String>,
}

pub struct TransfersPage {
    pub rows: Vec<TokenTransfer>,
    pub next_cursor: Option<String>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Decimal string → u256 as (low, high) limbs. `None` on overflow or junk.
pub fn parse_u256_decimal(s: &str) -> Option<(u128, u128)> {
    if s.is_empty() {
        return None;
    }
    let mut limbs = [0u64; 4];
    for c in s.bytes() {
        let mut carry = (c as char).to_digit(10)? as u128;
        for limb in limbs.iter_mut() {
            let v = (*limb as u128) * 10 + carry;
            *limb = v as u64;
            carry = v >> 64;
        }
        if carry != 0 {
            return None;
        }
    }
    Some((
        limbs[0] as u128 | ((limbs[1] as u128) << 64),
        limbs[2] as u128 | ((limbs[3] as u128) << 64),
    ))
}

impl TransferItem {
    fn into_transfer(self) -> Option<TokenTransfer> {
        let amount = self.raw_value.as_deref().or(self.amount.as_deref())?;
        let (amount_low, amount_high) = parse_u256_decimal(amount)?;
        let timestamp = self
            .timestamp_iso
            .as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.timestamp().max(0) as u64)
            .unwrap_or(0);
        Some(TokenTransfer {
            token: Felt::from_hex(&self.token_address).ok()?,
            from: Felt::from_hex(&self.from_address).ok()?,
            to: Felt::from_hex(&self.to_address).ok()?,
            amount_low,
            amount_high,
            tx_hash: Felt::from_hex(&self.tx_hash).ok()?,
            block_number: self.block_number,
            tx_index: self.tx_index,
            event_index: self.log_index,
            timestamp,
            symbol: self.token_symbol.filter(|s| !s.is_empty()),
            decimals: self.token_decimals,
            value_usd: self
                .historical_usd
                .and_then(|h| h.value_usd)
                .and_then(|v| v.parse::<f64>().ok())
                .filter(|v| v.is_finite()),
        })
    }
}

impl StarkscanClient {
    pub fn new(api_key: String, base_url: String) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(40))
                .build()
                .unwrap_or_default(),
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            backoff_until: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
        }
    }

    /// False while the key is rejected or an error backoff is in effect.
    pub fn available(&self) -> bool {
        !self.disabled.load(Ordering::Relaxed)
            && now_secs() >= self.backoff_until.load(Ordering::Relaxed)
    }

    fn trip_backoff(&self, secs: u64) {
        self.backoff_until
            .store(now_secs().saturating_add(secs), Ordering::Relaxed);
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> anyhow::Result<T> {
        let resp = self
            .client
            .get(url)
            .header("X-Starkscan-Api-Key", &self.api_key)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp.json::<T>().await?);
        }
        let request_id = resp
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        match status.as_u16() {
            401 | 403 => {
                self.disabled.store(true, Ordering::Relaxed);
                warn!(%status, request_id, "Starkscan rejected the API key; disabling for this session");
            }
            429 => {
                let retry = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(ERROR_BACKOFF_SECS);
                self.trip_backoff(retry);
            }
            _ => self.trip_backoff(ERROR_BACKOFF_SECS),
        }
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "Starkscan HTTP {status} (request {request_id}): {}",
            body.chars().take(160).collect::<String>()
        )
    }

    /// One newest-first page of ERC-20 transfers with `address` as `from` or
    /// `to`, bounded to `[from_block, to_block]`. Pass `next_cursor` back
    /// unchanged to continue; the bounds must stay the same across pages.
    pub async fn fetch_transfers(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        cursor: Option<&str>,
    ) -> anyhow::Result<TransfersPage> {
        let mut url = format!(
            "{}/address/{:#x}/transfers?type=erc20&limit={PAGE_LIMIT}",
            self.base_url, address
        );
        if let Some(lo) = from_block {
            url.push_str(&format!("&from_block={lo}"));
        }
        if let Some(hi) = to_block {
            url.push_str(&format!("&to_block={hi}"));
        }
        if let Some(c) = cursor {
            url.push_str(&format!("&cursor={c}"));
        }
        debug!(url = %url, "Fetching transfers from Starkscan");
        let resp: TransfersResponse = self.get_json(&url).await?;
        let rows = resp
            .items
            .into_iter()
            .filter_map(TransferItem::into_transfer)
            .collect();
        Ok(TransfersPage {
            rows,
            next_cursor: resp.next_cursor.filter(|c| !c.is_empty()),
        })
    }

    pub async fn health_check(&self) -> anyhow::Result<()> {
        let url = format!("{}/status", self.base_url);
        self.get_json::<serde_json::Value>(&url).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_small_and_huge_decimals() {
        assert_eq!(parse_u256_decimal("0"), Some((0, 0)));
        assert_eq!(parse_u256_decimal("56700000"), Some((56_700_000, 0)));
        let max_u128 = u128::MAX.to_string();
        assert_eq!(parse_u256_decimal(&max_u128), Some((u128::MAX, 0)));
        // 2^128 = high 1, low 0
        assert_eq!(
            parse_u256_decimal("340282366920938463463374607431768211456"),
            Some((0, 1))
        );
        assert_eq!(parse_u256_decimal(""), None);
        assert_eq!(parse_u256_decimal("12a"), None);
        // 2^256 overflows
        assert_eq!(
            parse_u256_decimal(
                "115792089237316195423570985008687907853269984665640564039457584007913129639936"
            ),
            None
        );
    }

    #[test]
    fn maps_item_to_transfer() {
        let item: TransferItem = serde_json::from_str(
            r#"{"blockNumber":500166,"timestampIso":"2024-01-09T06:55:31.000Z","txIndex":27,
                "logIndex":3,"txHash":"0x7294eee909df0a5343b7aa6a484e2aff23ac54778a51bbe00b421ec0bed95b9",
                "tokenAddress":"0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
                "fromAddress":"0x0","toAddress":"0x73314940630fd6dcda0d772d4c972c4e0a9946bef9dabf4ef84eda8ef542b82",
                "amount":"56700000","rawValue":"56700000","tokenSymbol":"ETH","tokenDecimals":18,
                "historicalUsd":{"priceUsd":"2500.5","valueUsd":"0.000141770","coverageStatus":"priced"}}"#,
        )
        .unwrap();
        let t = item.into_transfer().unwrap();
        assert_eq!(t.block_number, 500166);
        assert_eq!(t.tx_index, 27);
        assert_eq!(t.event_index, 3);
        assert_eq!(t.amount_low, 56_700_000);
        assert_eq!(t.timestamp, 1_704_783_331);
        assert_eq!(t.symbol.as_deref(), Some("ETH"));
        assert_eq!(t.decimals, Some(18));
        assert_eq!(t.from, Felt::ZERO);
        assert_eq!(t.value_usd, Some(0.000_141_77));
    }

    #[test]
    fn unpriced_item_has_no_value() {
        let item: TransferItem = serde_json::from_str(
            r#"{"blockNumber":1,"txIndex":0,"logIndex":0,"txHash":"0x1","tokenAddress":"0x2",
                "fromAddress":"0x3","toAddress":"0x4","rawValue":"5",
                "historicalUsd":{"priceUsd":null,"valueUsd":null,"coverageStatus":"unpriced"}}"#,
        )
        .unwrap();
        assert_eq!(item.into_transfer().unwrap().value_usd, None);
    }
}
