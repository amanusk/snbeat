use serde::Deserialize;
use starknet::core::types::Felt;
use std::time::Duration;
use tracing::debug;

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
