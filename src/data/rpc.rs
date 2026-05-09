use async_trait::async_trait;
use starknet::core::types::requests::{CallRequest, GetStorageAtRequest};
use starknet::core::types::{
    AddressFilter, BlockId, BlockTag, BlockWithTxs, ContractClass, DeclareTransaction,
    DeployAccountTransaction, EventFilter, ExecutionResult, Felt, FunctionCall, InvokeTransaction,
    MaybePreConfirmedBlockWithTxs, StorageKey, Transaction, TransactionReceipt, TransactionTrace,
};
use starknet::core::utils::get_contract_address;
use starknet::providers::{
    JsonRpcClient, Provider, ProviderRequestData, ProviderResponseData, jsonrpc::HttpTransport,
};
use tracing::{debug, warn};
use url::Url;

use super::DataSource;
use super::types::*;
use crate::error::{Result, SnbeatError};
use crate::utils::felt_to_u128;

/// RPC-based data source using starknet-rust Provider.
pub struct RpcDataSource {
    provider: JsonRpcClient<HttpTransport>,
    rpc_url: String,
}

impl RpcDataSource {
    pub fn new(rpc_url: &str) -> Self {
        let provider = JsonRpcClient::new(HttpTransport::new(
            Url::parse(rpc_url).expect("invalid RPC URL"),
        ));
        Self {
            provider,
            rpc_url: rpc_url.to_string(),
        }
    }

    /// Shared helper: find the starting block with an expanding-window search, then
    /// paginate through matching events newest-first.
    ///
    /// Unlike `starknet_getEvents` (which returns oldest-first and has no
    /// reverse flag), this walks the range in fixed windows from `latest`
    /// downward. Each window is fully paginated oldest-first, then reversed
    /// locally, so the caller sees the *globally newest* events first and we
    /// stop as soon as `limit` is satisfied.
    async fn fetch_events_paginated(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
        keys: Option<Vec<Vec<Felt>>>,
        debug_label: &str,
    ) -> Result<Vec<SnEvent>> {
        let latest = self
            .provider
            .block_number()
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;
        // Upper bound for the reverse walk. Caller-specified `to_block` is
        // clamped to `latest` so requests for future blocks don't error out.
        let upper = to_block.map(|b| b.min(latest)).unwrap_or(latest);

        // Resolve the lower bound of the search range.
        // - Caller-provided: use as-is (cache-driven incremental).
        // - Otherwise: expanding-window probe from the tip to find the oldest
        //   window that still has events, so we don't walk the whole chain.
        let lower = if let Some(fb) = from_block {
            fb
        } else {
            let windows = [50_000u64, 200_000, 1_000_000, latest];
            let mut found_from = 0;
            for window in windows {
                let test_from = upper.saturating_sub(window);
                let test_filter = EventFilter {
                    from_block: Some(BlockId::Number(test_from)),
                    to_block: Some(BlockId::Number(upper)),
                    address: Some(AddressFilter::Single(address)),
                    keys: keys.clone(),
                };
                match self.provider.get_events(test_filter, None, 1).await {
                    Ok(page) if !page.events.is_empty() => {
                        found_from = test_from;
                        debug!(window, found_from, "Found events in block window");
                        break;
                    }
                    _ => continue,
                }
            }
            found_from
        };

        // Walk backwards in fixed windows, newest-first.
        // Window size is a trade-off: larger = fewer RPC calls on dense
        // addresses; smaller = less overfetch on sparse ones. 10k matches the
        // app's cold-start window for contracts in network/address.rs.
        const WINDOW: u64 = 10_000;
        const CHUNK_SIZE: u64 = 1000;

        let mut collected: Vec<SnEvent> = Vec::new();
        let mut to = upper;

        'outer: while to >= lower && collected.len() < limit {
            let win_from = lower.max(to.saturating_sub(WINDOW));

            // Fully paginate this window (oldest-first per RPC contract).
            let mut window_events: Vec<SnEvent> = Vec::new();
            let mut continuation_token: Option<String> = None;
            let filter = EventFilter {
                from_block: Some(BlockId::Number(win_from)),
                to_block: Some(BlockId::Number(to)),
                address: Some(AddressFilter::Single(address)),
                keys: keys.clone(),
            };

            // Per-block event index — reset every time the block changes so
            // (tx_hash, block_number, event_index) is a stable dedup key
            // matching what pathfinder emits.
            let mut prev_block: u64 = u64::MAX;
            let mut idx_in_block: u64 = 0;

            loop {
                let page = self
                    .provider
                    .get_events(filter.clone(), continuation_token.clone(), CHUNK_SIZE)
                    .await
                    .map_err(|e| SnbeatError::Provider(e.to_string()))?;

                for e in &page.events {
                    let bn = e.block_number.unwrap_or(0);
                    if bn != prev_block {
                        idx_in_block = 0;
                        prev_block = bn;
                    }
                    window_events.push(SnEvent {
                        from_address: e.from_address,
                        keys: e.keys.clone(),
                        data: e.data.clone(),
                        transaction_hash: e.transaction_hash,
                        block_number: bn,
                        event_index: idx_in_block,
                    });
                    idx_in_block += 1;
                }

                match page.continuation_token {
                    Some(token) => continuation_token = Some(token),
                    None => break,
                }
            }

            // Reverse within-window events so this window's newest come first,
            // then append to the (globally newest-first) accumulator.
            window_events.reverse();
            collected.extend(window_events);

            if collected.len() >= limit {
                break 'outer;
            }
            if win_from == lower {
                break;
            }
            to = win_from.saturating_sub(1);
        }

        debug!(total = collected.len(), lower, "{debug_label}");
        // Trim to limit (windows may overshoot if the final window is dense).
        collected.truncate(limit);
        Ok(collected)
    }
}

#[async_trait]
impl DataSource for RpcDataSource {
    async fn get_latest_block_number(&self) -> Result<u64> {
        self.provider
            .block_number()
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn get_block(&self, number: u64) -> Result<SnBlock> {
        let block = self
            .provider
            .get_block_with_txs(BlockId::Number(number), None)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;

        match block {
            MaybePreConfirmedBlockWithTxs::Block(b) => Ok(convert_block(&b)),
            MaybePreConfirmedBlockWithTxs::PreConfirmedBlock(b) => {
                Ok(convert_block_from_pre_confirmed(&b))
            }
        }
    }

    async fn get_block_by_hash(&self, hash: Felt) -> Result<u64> {
        let block = self
            .provider
            .get_block_with_txs(BlockId::Hash(hash), None)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;

        match block {
            MaybePreConfirmedBlockWithTxs::Block(b) => Ok(b.block_number),
            MaybePreConfirmedBlockWithTxs::PreConfirmedBlock(_) => {
                Err(SnbeatError::Provider("Block is pre-confirmed".to_string()))
            }
        }
    }

    async fn get_block_with_txs(&self, number: u64) -> Result<(SnBlock, Vec<SnTransaction>)> {
        let block = self
            .provider
            .get_block_with_txs(BlockId::Number(number), None)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;

        match block {
            MaybePreConfirmedBlockWithTxs::Block(b) => {
                let sn_block = convert_block(&b);
                let txs = b
                    .transactions
                    .iter()
                    .enumerate()
                    .map(|(i, tx)| convert_transaction(tx, b.block_number, i as u64))
                    .collect();
                Ok((sn_block, txs))
            }
            MaybePreConfirmedBlockWithTxs::PreConfirmedBlock(b) => {
                let sn_block = convert_block_from_pre_confirmed(&b);
                let txs = b
                    .transactions
                    .iter()
                    .enumerate()
                    .map(|(i, tx)| convert_transaction(tx, 0, i as u64))
                    .collect();
                Ok((sn_block, txs))
            }
        }
    }

    async fn get_transaction(&self, hash: Felt) -> Result<SnTransaction> {
        let tx = self
            .provider
            .get_transaction_by_hash(hash, None)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;

        Ok(convert_transaction(&tx, 0, 0))
    }

    async fn get_receipt(&self, hash: Felt) -> Result<SnReceipt> {
        let receipt_with_block = self
            .provider
            .get_transaction_receipt(hash)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;

        Ok(convert_receipt(
            &receipt_with_block.receipt,
            &receipt_with_block.block,
        ))
    }

    async fn get_nonce(&self, address: Felt) -> Result<Felt> {
        self.provider
            .get_nonce(BlockId::Tag(BlockTag::Latest), address)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn get_class_hash(&self, address: Felt) -> Result<Felt> {
        self.provider
            .get_class_hash_at(BlockId::Tag(BlockTag::Latest), address)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn get_class_hash_at(&self, address: Felt, block: u64) -> Result<Felt> {
        self.provider
            .get_class_hash_at(BlockId::Number(block), address)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn get_class(&self, class_hash: Felt) -> Result<ContractClass> {
        self.provider
            .get_class(BlockId::Tag(BlockTag::Latest), class_hash)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn get_storage_at(&self, contract: Felt, key: Felt, block: Option<u64>) -> Result<Felt> {
        let block_id = match block {
            Some(n) => BlockId::Number(n),
            None => BlockId::Tag(BlockTag::Latest),
        };
        // starknet-rust 0.19 added a 4th param for response flags (RPC v0.10
        // optional `INCLUDE_LAST_UPDATE_BLOCK` metadata). We don't need
        // metadata for the privacy-pool sync, so pass `None` and unwrap the
        // bare value via `GetStorageAtResult::value()`.
        let result = self
            .provider
            .get_storage_at(contract, key, block_id, None)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))?;
        Ok(result.value())
    }

    async fn get_trace(&self, hash: Felt) -> Result<TransactionTrace> {
        self.provider
            .trace_transaction(hash)
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn get_recent_blocks(&self, count: usize) -> Result<Vec<SnBlock>> {
        let latest = self.get_latest_block_number().await?;
        debug!(latest, count, "RPC: fetching recent blocks");
        let start = latest.saturating_sub(count as u64 - 1);
        let mut blocks = Vec::with_capacity(count);

        // Fetch blocks in parallel batches
        let mut handles = Vec::new();
        for num in start..=latest {
            let provider = JsonRpcClient::new(HttpTransport::new(
                Url::parse(&self.rpc_url).expect("invalid RPC URL"),
            ));
            handles.push(tokio::spawn(async move {
                let result = provider
                    .get_block_with_txs(BlockId::Number(num), None)
                    .await;
                (num, result)
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            results.push(
                handle
                    .await
                    .map_err(|e| SnbeatError::Other(e.to_string()))?,
            );
        }

        // Sort by block number descending (newest first)
        results.sort_by(|a, b| b.0.cmp(&a.0));

        for (_num, result) in results {
            match result {
                Ok(block) => {
                    let sn_block = match block {
                        MaybePreConfirmedBlockWithTxs::Block(b) => convert_block(&b),
                        MaybePreConfirmedBlockWithTxs::PreConfirmedBlock(b) => {
                            convert_block_from_pre_confirmed(&b)
                        }
                    };
                    blocks.push(sn_block);
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch block: {e}");
                }
            }
        }

        Ok(blocks)
    }

    async fn get_events_for_address(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SnEvent>> {
        // Use transaction_executed selector to get exactly 1 event per account tx.
        // This is emitted by every account contract on every invoke.
        let tx_executed_selector =
            Felt::from_hex(crate::data::pathfinder::TRANSACTION_EXECUTED_SELECTOR).unwrap();
        let keys = Some(vec![vec![tx_executed_selector]]);
        self.fetch_events_paginated(
            address,
            from_block,
            to_block,
            limit,
            keys,
            "Fetched events for address",
        )
        .await
    }

    async fn get_contract_events(
        &self,
        address: Felt,
        from_block: Option<u64>,
        to_block: Option<u64>,
        limit: usize,
    ) -> Result<Vec<SnEvent>> {
        // No key filter — get ALL events from this contract
        self.fetch_events_paginated(
            address,
            from_block,
            to_block,
            limit,
            None,
            "Fetched contract events",
        )
        .await
    }

    async fn call_contract(
        &self,
        contract_address: Felt,
        selector: Felt,
        calldata: Vec<Felt>,
    ) -> Result<Vec<Felt>> {
        let call = FunctionCall {
            contract_address,
            entry_point_selector: selector,
            calldata,
        };
        self.provider
            .call(call, BlockId::Tag(BlockTag::Latest))
            .await
            .map_err(|e| SnbeatError::Provider(e.to_string()))
    }

    async fn batch_call_contracts(
        &self,
        calls: Vec<(Felt, Felt, Vec<Felt>)>,
    ) -> Vec<Result<Vec<Felt>>> {
        if calls.is_empty() {
            return Vec::new();
        }
        let requests: Vec<ProviderRequestData> = calls
            .iter()
            .map(|(contract_address, selector, calldata)| {
                ProviderRequestData::Call(CallRequest {
                    request: FunctionCall {
                        contract_address: *contract_address,
                        entry_point_selector: *selector,
                        calldata: calldata.clone(),
                    },
                    block_id: BlockId::Tag(BlockTag::Latest),
                })
            })
            .collect();

        match self.provider.batch_requests(&requests).await {
            Ok(responses) => responses
                .into_iter()
                .map(|resp| match resp {
                    ProviderResponseData::Call(v) => Ok(v),
                    _ => Err(SnbeatError::Provider(
                        "unexpected response type in batch".into(),
                    )),
                })
                .collect(),
            Err(e) => {
                // `batch_requests` is all-or-nothing — if any single call in the
                // batch fails, the whole thing errors. Fall back to per-call
                // requests so one bad call (e.g., a contract without the
                // expected selector) doesn't sink the rest.
                warn!(error = %e, "batch_requests failed, falling back to sequential");
                let mut out = Vec::with_capacity(calls.len());
                for (contract, selector, calldata) in calls {
                    out.push(self.call_contract(contract, selector, calldata).await);
                }
                out
            }
        }
    }

    async fn batch_get_storage_at(
        &self,
        contract: Felt,
        keys: &[Felt],
        block: Option<u64>,
    ) -> Vec<Result<Felt>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let block_id = match block {
            Some(n) => BlockId::Number(n),
            None => BlockId::Tag(BlockTag::Latest),
        };
        let requests: Vec<ProviderRequestData> = keys
            .iter()
            .map(|k| {
                ProviderRequestData::GetStorageAt(GetStorageAtRequest {
                    contract_address: contract,
                    key: StorageKey(format!("{:#x}", k)),
                    block_id,
                    response_flags: None,
                })
            })
            .collect();
        match self.provider.batch_requests(&requests).await {
            Ok(responses) => responses
                .into_iter()
                .map(|resp| match resp {
                    ProviderResponseData::GetStorageAt(v) => Ok(v.value()),
                    _ => Err(SnbeatError::Provider(
                        "unexpected response type in storage batch".into(),
                    )),
                })
                .collect(),
            Err(e) => {
                // Batch is all-or-nothing — fall back to sequential per-key
                // reads so one bad slot (e.g., contract not deployed yet at
                // `block`) doesn't sink the whole batch.
                warn!(error = %e, "batch_requests (storage) failed, falling back to sequential");
                let mut out = Vec::with_capacity(keys.len());
                for k in keys {
                    out.push(self.get_storage_at(contract, *k, block).await);
                }
                out
            }
        }
    }
}

// --- Type conversion helpers ---

fn convert_block(b: &BlockWithTxs) -> SnBlock {
    SnBlock {
        number: b.block_number,
        hash: b.block_hash,
        parent_hash: b.parent_hash,
        timestamp: b.timestamp,
        sequencer_address: b.sequencer_address,
        transaction_count: b.transactions.len(),
        l1_gas_price_fri: felt_to_u128(&b.l1_gas_price.price_in_fri),
        l1_gas_price_wei: felt_to_u128(&b.l1_gas_price.price_in_wei),
        l2_gas_price_fri: felt_to_u128(&b.l2_gas_price.price_in_fri),
        l1_data_gas_price_fri: felt_to_u128(&b.l1_data_gas_price.price_in_fri),
        starknet_version: b.starknet_version.clone(),
    }
}

fn convert_block_from_pre_confirmed(
    b: &starknet::core::types::PreConfirmedBlockWithTxs,
) -> SnBlock {
    SnBlock {
        number: b.block_number,
        hash: Felt::ZERO,
        parent_hash: Felt::ZERO,
        timestamp: b.timestamp,
        sequencer_address: b.sequencer_address,
        transaction_count: b.transactions.len(),
        l1_gas_price_fri: felt_to_u128(&b.l1_gas_price.price_in_fri),
        l1_gas_price_wei: felt_to_u128(&b.l1_gas_price.price_in_wei),
        l2_gas_price_fri: felt_to_u128(&b.l2_gas_price.price_in_fri),
        l1_data_gas_price_fri: felt_to_u128(&b.l1_data_gas_price.price_in_fri),
        starknet_version: b.starknet_version.clone(),
    }
}

fn convert_resource_bounds(rb: &starknet::core::types::ResourceBoundsMapping) -> SnResourceBounds {
    SnResourceBounds {
        l1_gas_max_amount: rb.l1_gas.max_amount,
        l1_gas_max_price: rb.l1_gas.max_price_per_unit,
        l2_gas_max_amount: rb.l2_gas.max_amount,
        l2_gas_max_price: rb.l2_gas.max_price_per_unit,
        l1_data_gas_max_amount: rb.l1_data_gas.max_amount,
        l1_data_gas_max_price: rb.l1_data_gas.max_price_per_unit,
    }
}

fn convert_transaction(tx: &Transaction, block_number: u64, index: u64) -> SnTransaction {
    match tx {
        Transaction::Invoke(invoke) => {
            let (hash, sender, calldata, nonce, version, tip, rb) = match invoke {
                InvokeTransaction::V0(v) => (
                    v.transaction_hash,
                    v.contract_address,
                    v.calldata.clone(),
                    None,
                    Felt::ZERO,
                    0u64,
                    None,
                ),
                InvokeTransaction::V1(v) => (
                    v.transaction_hash,
                    v.sender_address,
                    v.calldata.clone(),
                    Some(v.nonce),
                    Felt::ONE,
                    0u64,
                    None,
                ),
                InvokeTransaction::V3(v) => (
                    v.transaction_hash,
                    v.sender_address,
                    v.calldata.clone(),
                    Some(v.nonce),
                    Felt::THREE,
                    v.tip,
                    Some(convert_resource_bounds(&v.resource_bounds)),
                ),
            };
            SnTransaction::Invoke(InvokeTx {
                hash,
                sender_address: sender,
                calldata,
                nonce,
                version,
                actual_fee: None,
                execution_status: ExecutionStatus::Unknown,
                block_number,
                index,
                tip,
                resource_bounds: rb,
            })
        }
        Transaction::Declare(declare) => {
            let (hash, sender, class_hash, version, tip, rb) = match declare {
                DeclareTransaction::V0(v) => (
                    v.transaction_hash,
                    v.sender_address,
                    v.class_hash,
                    Felt::ZERO,
                    0u64,
                    None,
                ),
                DeclareTransaction::V1(v) => (
                    v.transaction_hash,
                    v.sender_address,
                    v.class_hash,
                    Felt::ONE,
                    0u64,
                    None,
                ),
                DeclareTransaction::V2(v) => (
                    v.transaction_hash,
                    v.sender_address,
                    v.class_hash,
                    Felt::TWO,
                    0u64,
                    None,
                ),
                DeclareTransaction::V3(v) => (
                    v.transaction_hash,
                    v.sender_address,
                    v.class_hash,
                    Felt::THREE,
                    v.tip,
                    Some(convert_resource_bounds(&v.resource_bounds)),
                ),
            };
            SnTransaction::Declare(DeclareTx {
                hash,
                sender_address: sender,
                class_hash,
                version,
                actual_fee: None,
                execution_status: ExecutionStatus::Unknown,
                block_number,
                index,
                tip,
                resource_bounds: rb,
            })
        }
        Transaction::DeployAccount(deploy_account) => {
            let (hash, class_hash, calldata, salt, version, nonce, tip, rb) = match deploy_account {
                DeployAccountTransaction::V1(v) => (
                    v.transaction_hash,
                    v.class_hash,
                    v.constructor_calldata.clone(),
                    v.contract_address_salt,
                    Felt::ONE,
                    v.nonce,
                    0u64,
                    None,
                ),
                DeployAccountTransaction::V3(v) => (
                    v.transaction_hash,
                    v.class_hash,
                    v.constructor_calldata.clone(),
                    v.contract_address_salt,
                    Felt::THREE,
                    v.nonce,
                    v.tip,
                    Some(convert_resource_bounds(&v.resource_bounds)),
                ),
            };
            let contract_address = get_contract_address(salt, class_hash, &calldata, Felt::ZERO);
            SnTransaction::DeployAccount(DeployAccountTx {
                hash,
                contract_address,
                class_hash,
                constructor_calldata: calldata,
                contract_address_salt: salt,
                nonce: Some(nonce),
                version,
                actual_fee: None,
                execution_status: ExecutionStatus::Unknown,
                block_number,
                index,
                tip,
                resource_bounds: rb,
            })
        }
        Transaction::L1Handler(l1) => SnTransaction::L1Handler(L1HandlerTx {
            hash: l1.transaction_hash,
            contract_address: l1.contract_address,
            entry_point_selector: l1.entry_point_selector,
            calldata: l1.calldata.clone(),
            nonce: Some(Felt::from(l1.nonce)),
            actual_fee: None,
            execution_status: ExecutionStatus::Unknown,
            block_number,
            index,
        }),
        Transaction::Deploy(deploy) => SnTransaction::Deploy(DeployTx {
            hash: deploy.transaction_hash,
            contract_address: Felt::ZERO,
            class_hash: deploy.class_hash,
            constructor_calldata: deploy.constructor_calldata.clone(),
            version: deploy.version,
            actual_fee: None,
            execution_status: ExecutionStatus::Unknown,
            block_number,
            index,
        }),
    }
}

fn convert_receipt(
    receipt: &TransactionReceipt,
    block: &starknet::core::types::ReceiptBlock,
) -> SnReceipt {
    use starknet::core::types::PriceUnit;

    // Helper macro to extract common fields from receipt variants
    macro_rules! extract_receipt {
        ($r:expr) => {
            (
                $r.transaction_hash,
                $r.actual_fee.amount,
                match $r.actual_fee.unit {
                    PriceUnit::Fri => "STRK",
                    PriceUnit::Wei => "ETH",
                },
                &$r.events,
                &$r.execution_result,
                $r.execution_resources.clone(),
                match &$r.finality_status {
                    starknet::core::types::TransactionFinalityStatus::AcceptedOnL2 => {
                        "ACCEPTED_ON_L2"
                    }
                    starknet::core::types::TransactionFinalityStatus::AcceptedOnL1 => {
                        "ACCEPTED_ON_L1"
                    }
                    starknet::core::types::TransactionFinalityStatus::PreConfirmed => {
                        "PRE_CONFIRMED"
                    }
                },
            )
        };
    }

    let (tx_hash, actual_fee, fee_unit, events, exec_result, exec_resources, finality) =
        match receipt {
            TransactionReceipt::Invoke(r) => extract_receipt!(r),
            TransactionReceipt::Declare(r) => extract_receipt!(r),
            TransactionReceipt::DeployAccount(r) => extract_receipt!(r),
            TransactionReceipt::L1Handler(r) => extract_receipt!(r),
            TransactionReceipt::Deploy(r) => extract_receipt!(r),
        };

    let block_number = block.block_number();
    let block_hash = block.block_hash();

    let (status, revert_reason) = match exec_result {
        ExecutionResult::Succeeded => (ExecutionStatus::Succeeded, None),
        ExecutionResult::Reverted { reason } => (
            ExecutionStatus::Reverted(reason.clone()),
            Some(reason.clone()),
        ),
    };

    let sn_events = events
        .iter()
        .enumerate()
        .map(|(i, e)| SnEvent {
            from_address: e.from_address,
            keys: e.keys.clone(),
            data: e.data.clone(),
            transaction_hash: tx_hash,
            block_number: 0,
            event_index: i as u64,
        })
        .collect();

    SnReceipt {
        transaction_hash: tx_hash,
        block_number,
        block_hash,
        actual_fee,
        fee_unit: fee_unit.to_string(),
        execution_status: status,
        execution_resources: SnExecutionResources {
            l1_gas: exec_resources.l1_gas,
            l2_gas: exec_resources.l2_gas,
            l1_data_gas: exec_resources.l1_data_gas,
        },
        events: sn_events,
        revert_reason,
        finality: finality.to_string(),
    }
}
