//! WebSocket subscription manager for real-time Starknet data.
//!
//! Maintains a single WebSocket connection that can hold multiple concurrent
//! subscriptions:
//!   - `starknet_subscribeNewHeads` — always active, replaces polling
//!   - `starknet_subscribeEvents` — per address, added when viewing an address
//!   - `starknet_subscribeNewTransactions` — per account sender address
//!
//! Uses starknet-rust types for serialization of subscribe requests and
//! deserialization of notification payloads. Raw connection management uses
//! tokio-tungstenite (starknet-rust has no WebSocket transport).
//!
//! The public entry point is [`spawn_ws_subscriber`], which returns a
//! [`WsSubscriptionManager`] for dynamically adding/removing subscriptions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use starknet::core::types::{
    AddressFilter, EmittedEventWithFinality, Felt, L2TransactionFinalityStatus,
    L2TransactionStatus, SubscriptionId, Transaction,
    requests::{
        SubscribeEventsRequest, SubscribeNewHeadsRequest, SubscribeNewTransactionsRequest,
        UnsubscribeRequest,
    },
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::app::actions::{Action, Source};
use crate::app::state::SourceStatus;
use crate::data::DataSource;
use crate::data::types::{AddressTxSummary, ContractCallSummary};
use crate::utils::felt_to_u64;

/// Maximum reconnect delay (capped exponential backoff).
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// Initial reconnect delay.
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// Give up after this many consecutive connection failures.
const MAX_CONSECUTIVE_FAILURES: u32 = 5;
/// Timeout waiting for subscription confirmation.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Identifies what an active subscription is tracking.
#[derive(Debug, Clone)]
enum SubscriptionKind {
    NewHeads,
    Events { address: Felt },
    Transactions { address: Felt },
}

/// Command sent to the WS task to manage subscriptions.
#[derive(Debug)]
pub enum WsCommand {
    /// Subscribe to events emitted by `address` (contract view).
    SubscribeEvents { address: Felt },
    /// Subscribe to new transactions sent by `address` (account view).
    SubscribeTransactions { address: Felt },
    /// Remove all subscriptions for `address`.
    UnsubscribeAddress { address: Felt },
}

/// Handle for controlling the WS subscription manager from the UI thread.
///
/// `Send + Sync` because the inner `mpsc::UnboundedSender` is.
pub struct WsSubscriptionManager {
    cmd_tx: mpsc::UnboundedSender<WsCommand>,
}

impl WsSubscriptionManager {
    /// Subscribe to events emitted by `address` (use for contracts).
    pub fn subscribe_events(&self, address: Felt) {
        let _ = self.cmd_tx.send(WsCommand::SubscribeEvents { address });
    }

    /// Subscribe to new transactions sent from `address` (use for accounts).
    pub fn subscribe_transactions(&self, address: Felt) {
        let _ = self
            .cmd_tx
            .send(WsCommand::SubscribeTransactions { address });
    }

    /// Subscribe to both events and transactions for `address`.
    /// Safe to call regardless of address type — the server will send
    /// whichever notifications match.
    pub fn subscribe_address(&self, address: Felt) {
        let _ = self.cmd_tx.send(WsCommand::SubscribeEvents { address });
        let _ = self
            .cmd_tx
            .send(WsCommand::SubscribeTransactions { address });
    }

    /// Stop all subscriptions for `address`.
    pub fn unsubscribe_address(&self, address: Felt) {
        let _ = self.cmd_tx.send(WsCommand::UnsubscribeAddress { address });
    }
}

/// Spawns the WebSocket subscription task.
///
/// Returns a join handle (runs indefinitely with auto-reconnect) and a
/// [`WsSubscriptionManager`] for dynamically adding/removing subscriptions.
pub fn spawn_ws_subscriber(
    ws_url: String,
    data_source: Arc<dyn DataSource>,
    response_tx: mpsc::UnboundedSender<Action>,
) -> (tokio::task::JoinHandle<()>, WsSubscriptionManager) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<WsCommand>();
    let manager = WsSubscriptionManager { cmd_tx };

    let handle = tokio::spawn(async move {
        let mut delay = INITIAL_RECONNECT_DELAY;
        let mut consecutive_failures: u32 = 0;
        let mut cmd_rx = cmd_rx;
        // Active address subscriptions to re-register on reconnect
        let mut active_address_subs: Vec<WsCommand> = Vec::new();

        loop {
            info!(url = %ws_url, "Connecting to WebSocket");
            let _ = response_tx.send(ws_source_update(SourceStatus::Configured));

            match connect_and_run(
                &ws_url,
                &data_source,
                &response_tx,
                &mut cmd_rx,
                &active_address_subs,
            )
            .await
            {
                Ok(new_subs) => {
                    // Clean exit — update our known active subs from what was active
                    active_address_subs = new_subs;
                    info!("WebSocket stream ended cleanly");
                    delay = INITIAL_RECONNECT_DELAY;
                    consecutive_failures = 0;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    error!(error = %e, attempt = consecutive_failures, "WebSocket connection failed");
                    let _ = response_tx
                        .send(ws_source_update(SourceStatus::ConnectError(e.to_string())));

                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        error!(
                            "WebSocket gave up after {consecutive_failures} consecutive failures"
                        );
                        return;
                    }
                }
            }

            warn!(
                delay_secs = delay.as_secs(),
                "Reconnecting WebSocket after delay"
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_RECONNECT_DELAY);
        }
    });

    (handle, manager)
}

fn ws_source_update(status: SourceStatus) -> Action {
    Action::SourceUpdate {
        source: Source::Ws,
        status,
    }
}

// ---------------------------------------------------------------------------
// Internal: connection state
// ---------------------------------------------------------------------------

/// State tracked for the lifetime of a single WS connection.
struct ConnectionState {
    /// Maps subscription ID string → what it tracks.
    subscriptions: HashMap<String, SubscriptionKind>,
    /// Maps address → list of subscription ID strings (for cleanup).
    address_subs: HashMap<Felt, Vec<String>>,
    /// Pending subscribe requests: JSON-RPC request id → kind being subscribed to.
    pending: HashMap<u64, SubscriptionKind>,
    /// Next JSON-RPC request ID to use.
    next_id: u64,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            address_subs: HashMap::new(),
            pending: HashMap::new(),
            next_id: 2, // 1 is used for the initial NewHeads subscription
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn confirm_subscribe(&mut self, request_id: u64, sub_id: String) {
        if let Some(kind) = self.pending.remove(&request_id) {
            // Track address → sub_id mapping for cleanup
            if let Some(addr) = kind_address(&kind) {
                self.address_subs
                    .entry(addr)
                    .or_default()
                    .push(sub_id.clone());
            }
            self.subscriptions.insert(sub_id, kind);
        }
    }

    fn remove_address(&mut self, address: Felt) -> Vec<String> {
        self.address_subs
            .remove(&address)
            .unwrap_or_default()
            .into_iter()
            .inspect(|id| {
                self.subscriptions.remove(id);
            })
            .collect()
    }

    fn subscription_kind(&self, sub_id: &str) -> Option<&SubscriptionKind> {
        self.subscriptions.get(sub_id)
    }

    /// Collect all active address subscriptions (for reconnect re-subscribe).
    fn active_address_commands(&self) -> Vec<WsCommand> {
        let mut seen = std::collections::HashSet::new();
        let mut cmds = Vec::new();
        for (_, kind) in &self.subscriptions {
            if let Some(addr) = kind_address(kind) {
                if seen.insert(addr) {
                    cmds.push(WsCommand::SubscribeEvents { address: addr });
                    cmds.push(WsCommand::SubscribeTransactions { address: addr });
                }
            }
        }
        cmds
    }
}

fn kind_address(kind: &SubscriptionKind) -> Option<Felt> {
    match kind {
        SubscriptionKind::Events { address } | SubscriptionKind::Transactions { address } => {
            Some(*address)
        }
        SubscriptionKind::NewHeads => None,
    }
}

// ---------------------------------------------------------------------------
// Raw JSON-RPC envelope types (minimal — avoids strict starknet-rust wrappers)
// ---------------------------------------------------------------------------

/// Generic JSON-RPC message: covers both responses (has `id`) and notifications (has `method`).
#[derive(Debug, Deserialize)]
struct RawMessage {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: Option<String>,
    params: Option<RawParams>,
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

/// Notification params: subscription ID + pushed result.
#[derive(Debug, Deserialize)]
struct RawParams {
    /// Spec uses `subscription_id`; Pathfinder uses `subscription`.
    #[serde(alias = "subscription", alias = "subscription_id")]
    subscription_id: Value,
    result: Value,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    #[allow(dead_code)]
    code: i64,
    message: String,
}

// ---------------------------------------------------------------------------
// Internal: connect, subscribe, and run the select! loop
// ---------------------------------------------------------------------------

async fn connect_and_run(
    ws_url: &str,
    data_source: &Arc<dyn DataSource>,
    response_tx: &mpsc::UnboundedSender<Action>,
    cmd_rx: &mut mpsc::UnboundedReceiver<WsCommand>,
    initial_subs: &[WsCommand],
) -> Result<Vec<WsCommand>, Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    let mut state = ConnectionState::new();

    // Always subscribe to new block headers first (id = 1)
    let heads_msg = build_subscribe_msg(
        1,
        "starknet_subscribeNewHeads",
        serde_json::to_value(SubscribeNewHeadsRequest { block_id: None })?,
    );
    write.send(Message::Text(heads_msg.into())).await?;
    state.pending.insert(1, SubscriptionKind::NewHeads);
    debug!("Sent starknet_subscribeNewHeads");

    // Re-subscribe to any address subscriptions from the previous connection
    for cmd in initial_subs {
        send_subscribe_cmd(&mut write, &mut state, cmd).await?;
    }

    let _ = response_tx.send(ws_source_update(SourceStatus::Live));

    // Main event loop: multiplex WS messages and manager commands
    loop {
        tokio::select! {
            // Incoming WS message
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_message(&text, &mut write, &mut state, data_source, response_tx).await;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WebSocket server sent close frame");
                        break;
                    }
                    Some(Ok(_)) => {} // Binary, Pong, etc.
                    Some(Err(e)) => {
                        return Err(format!("WS read error: {e}").into());
                    }
                    None => {
                        info!("WebSocket stream closed");
                        break;
                    }
                }
            }

            // Command from WsSubscriptionManager
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(WsCommand::UnsubscribeAddress { address }) => {
                        let ids = state.remove_address(address);
                        for sub_id in ids {
                            let req_id = state.next_id();
                            let msg = build_subscribe_msg(
                                req_id,
                                "starknet_unsubscribe",
                                serde_json::to_value(UnsubscribeRequest {
                                    subscription_id: SubscriptionId(sub_id.clone()),
                                })?,
                            );
                            if write.send(Message::Text(msg.into())).await.is_err() {
                                break;
                            }
                            debug!(sub_id, "Sent unsubscribe");
                        }
                    }
                    Some(cmd) => {
                        if let Err(e) = send_subscribe_cmd(&mut write, &mut state, &cmd).await {
                            warn!(error = %e, "Failed to send subscribe command");
                        }
                    }
                    None => {
                        // Manager dropped — clean shutdown
                        info!("WsSubscriptionManager dropped, stopping WS task");
                        return Ok(Vec::new());
                    }
                }
            }
        }
    }

    Ok(state.active_address_commands())
}

/// Build a JSON-RPC request string.
fn build_subscribe_msg(id: u64, method: &str, params: Value) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
    .to_string()
}

/// Send a subscribe command over the WS write half and record it in state.
async fn send_subscribe_cmd(
    write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    state: &mut ConnectionState,
    cmd: &WsCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        WsCommand::SubscribeEvents { address } => {
            let id = state.next_id();
            let req = SubscribeEventsRequest {
                from_address: Some(AddressFilter::Single(*address)),
                keys: None,
                block_id: None,
                finality_status: Some(L2TransactionFinalityStatus::AcceptedOnL2),
            };
            let msg =
                build_subscribe_msg(id, "starknet_subscribeEvents", serde_json::to_value(req)?);
            write.send(Message::Text(msg.into())).await?;
            state
                .pending
                .insert(id, SubscriptionKind::Events { address: *address });
            debug!(address = %format!("{:#x}", address), "Sent starknet_subscribeEvents");
        }
        WsCommand::SubscribeTransactions { address } => {
            let id = state.next_id();
            let req = SubscribeNewTransactionsRequest {
                sender_address: Some(vec![*address]),
                finality_status: Some(vec![L2TransactionStatus::AcceptedOnL2]),
                tags: None,
            };
            let msg = build_subscribe_msg(
                id,
                "starknet_subscribeNewTransactions",
                serde_json::to_value(req)?,
            );
            write.send(Message::Text(msg.into())).await?;
            state
                .pending
                .insert(id, SubscriptionKind::Transactions { address: *address });
            debug!(address = %format!("{:#x}", address), "Sent starknet_subscribeNewTransactions");
        }
        WsCommand::UnsubscribeAddress { .. } => {
            // Handled at the call site with access to state.remove_address()
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Message handling: route by subscription ID
// ---------------------------------------------------------------------------

async fn handle_message(
    text: &str,
    write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    state: &mut ConnectionState,
    data_source: &Arc<dyn DataSource>,
    response_tx: &mpsc::UnboundedSender<Action>,
) {
    let raw: RawMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "Failed to parse WS message");
            return;
        }
    };

    // Handle responses to our subscribe/unsubscribe requests
    if let Some(id_val) = &raw.id {
        let req_id = id_val.as_u64().unwrap_or(0);
        if let Some(err) = &raw.error {
            warn!(req_id, error = %err.message, "Subscribe/unsubscribe failed");
            state.pending.remove(&req_id);
            return;
        }
        if let Some(result) = &raw.result {
            // Subscription confirmation: result is the subscription ID
            if let Ok(sub_id) = serde_json::from_value::<SubscriptionId>(result.clone()) {
                debug!(req_id, sub_id = %sub_id.0, "Subscription confirmed");
                state.confirm_subscribe(req_id, sub_id.0);
            }
            return;
        }
    }

    // Handle push notifications
    if let Some(method) = &raw.method {
        if !method.starts_with("starknet_subscription") {
            debug!(method, "Ignoring non-subscription notification");
            return;
        }
    }

    let params = match raw.params {
        Some(p) => p,
        None => return,
    };

    // Extract subscription ID as a canonical string (handles string or integer)
    let sub_id_str = match &params.subscription_id {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => {
            warn!(value = ?other, "Unexpected subscription_id type");
            return;
        }
    };

    let kind = match state.subscription_kind(&sub_id_str) {
        Some(k) => k.clone(),
        None => {
            // Unknown subscription ID — may be a race with confirmation
            debug!(
                sub_id = sub_id_str,
                "Notification for unknown subscription, ignoring"
            );
            return;
        }
    };

    match kind {
        SubscriptionKind::NewHeads => {
            handle_new_heads(&params.result, data_source, response_tx).await;
        }
        SubscriptionKind::Events { address } => {
            handle_event(&params.result, address, response_tx);
        }
        SubscriptionKind::Transactions { address } => {
            handle_new_transaction(&params.result, address, response_tx);
        }
    }

    let _ = write; // suppress unused warning
}

// ---------------------------------------------------------------------------
// Per-notification-type handlers
// ---------------------------------------------------------------------------

async fn handle_new_heads(
    result: &Value,
    data_source: &Arc<dyn DataSource>,
    response_tx: &mpsc::UnboundedSender<Action>,
) {
    let block_number = match result["block_number"].as_u64() {
        Some(n) => n,
        None => {
            warn!("NewHeads notification missing block_number: {result}");
            return;
        }
    };

    info!(block_number, "New block header via WebSocket");

    // Fetch full block via RPC (WS header doesn't include tx data)
    match data_source.get_block(block_number).await {
        Ok(block) => {
            let _ = response_tx.send(Action::NewBlock(block));
        }
        Err(e) => {
            // Fallback: build a minimal block from the notification fields
            warn!(block_number, error = %e, "Failed to fetch full block via RPC, using WS header");
            if let Some(block) = block_from_raw(result) {
                let _ = response_tx.send(Action::NewBlock(block));
            }
        }
    }
}

fn handle_event(result: &Value, address: Felt, response_tx: &mpsc::UnboundedSender<Action>) {
    let event: EmittedEventWithFinality = match serde_json::from_value(result.clone()) {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "Failed to deserialize event notification");
            return;
        }
    };

    let tx_hash = event.emitted_event.transaction_hash;
    let block_number = event.emitted_event.block_number.unwrap_or(0);
    debug!(
        address = %format!("{:#x}", address),
        tx = %format!("{:#x}", tx_hash),
        block = block_number,
        "Received event via WS → Calls tab"
    );

    // Events tell us the contract was invoked (called) — route to Calls tab.
    // The tx sender and function name are not available from the event payload;
    // they will be enriched later via the normal enrichment pipeline.
    let call = ContractCallSummary {
        tx_hash,
        sender: Felt::ZERO, // Unknown from event alone; will be enriched
        function_name: String::new(),
        block_number,
        timestamp: 0,
        total_fee_fri: 0,
        status: "OK".to_string(), // Events only fire for successful txs
    };

    let _ = response_tx.send(Action::AddressCallsStreamed {
        address,
        calls: vec![call],
    });

    // If the viewed address is an Argent/Braavos account, every invoke —
    // including execute_from_outside* — emits TRANSACTION_EXECUTED. Classify
    // the tx to see whether it's a meta-tx with this address as the intender,
    // and if so stream it into the MetaTxs tab. Cheap check up front: only
    // dispatch when the first key matches the selector.
    if event
        .emitted_event
        .keys
        .first()
        .map(|k| *k == tx_executed_selector())
        .unwrap_or(false)
    {
        let _ = response_tx.send(Action::ClassifyPotentialMetaTx { address, tx_hash });
    }
}

/// Cached `TRANSACTION_EXECUTED` selector as `Felt` (parsed once per process).
fn tx_executed_selector() -> Felt {
    use std::sync::OnceLock;
    static SELECTOR: OnceLock<Felt> = OnceLock::new();
    *SELECTOR.get_or_init(|| {
        Felt::from_hex(crate::data::pathfinder::TRANSACTION_EXECUTED_SELECTOR)
            .expect("TRANSACTION_EXECUTED_SELECTOR is a valid felt")
    })
}

fn handle_new_transaction(
    result: &Value,
    address: Felt,
    response_tx: &mpsc::UnboundedSender<Action>,
) {
    // Try to deserialize as a starknet-rust Transaction
    let summary = if let Ok(tx) = serde_json::from_value::<Transaction>(result.clone()) {
        tx_to_summary(&tx)
    } else {
        // Fall back to extracting from JSON directly
        tx_from_raw(result)
    };

    let summary = match summary {
        Some(s) => s,
        None => {
            debug!("Could not build summary from transaction notification");
            return;
        }
    };

    debug!(
        address = %format!("{:#x}", address),
        tx = %format!("{:#x}", summary.hash),
        "Received transaction via WS"
    );

    let _ = response_tx.send(Action::AddressTxsStreamed {
        address,
        source: Source::Ws,
        tx_summaries: vec![summary],
        complete: false,
    });
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

/// Build an `AddressTxSummary` from a starknet-rust `Transaction`.
fn tx_to_summary(tx: &Transaction) -> Option<AddressTxSummary> {
    let (hash, nonce, tx_type, sender) = match tx {
        Transaction::Invoke(invoke) => match invoke {
            starknet::core::types::InvokeTransaction::V0(t) => {
                (t.transaction_hash, 0u64, "INVOKE", None::<Felt>)
            }
            starknet::core::types::InvokeTransaction::V1(t) => (
                t.transaction_hash,
                felt_to_u64(&t.nonce),
                "INVOKE",
                Some(t.sender_address),
            ),
            starknet::core::types::InvokeTransaction::V3(t) => (
                t.transaction_hash,
                felt_to_u64(&t.nonce),
                "INVOKE",
                Some(t.sender_address),
            ),
        },
        Transaction::Declare(declare) => match declare {
            starknet::core::types::DeclareTransaction::V0(t) => {
                (t.transaction_hash, 0u64, "DECLARE", Some(t.sender_address))
            }
            starknet::core::types::DeclareTransaction::V1(t) => (
                t.transaction_hash,
                felt_to_u64(&t.nonce),
                "DECLARE",
                Some(t.sender_address),
            ),
            starknet::core::types::DeclareTransaction::V2(t) => (
                t.transaction_hash,
                felt_to_u64(&t.nonce),
                "DECLARE",
                Some(t.sender_address),
            ),
            starknet::core::types::DeclareTransaction::V3(t) => (
                t.transaction_hash,
                felt_to_u64(&t.nonce),
                "DECLARE",
                Some(t.sender_address),
            ),
        },
        Transaction::DeployAccount(deploy_acc) => match deploy_acc {
            starknet::core::types::DeployAccountTransaction::V1(t) => {
                (t.transaction_hash, 0u64, "DEPLOY_ACCOUNT", None)
            }
            starknet::core::types::DeployAccountTransaction::V3(t) => {
                (t.transaction_hash, 0u64, "DEPLOY_ACCOUNT", None)
            }
        },
        Transaction::Deploy(t) => (t.transaction_hash, 0u64, "DEPLOY", None),
        Transaction::L1Handler(t) => (t.transaction_hash, 0u64, "L1_HANDLER", None),
    };

    Some(AddressTxSummary {
        hash,
        nonce,
        block_number: 0, // Not in pending tx notification
        timestamp: 0,
        endpoint_names: String::new(),
        total_fee_fri: 0,
        tip: 0,
        tx_type: tx_type.to_string(),
        status: "?".to_string(), // Pending — enrichment will update
        sender,
    })
}

/// Extract minimal AddressTxSummary from raw JSON value (fallback).
fn tx_from_raw(v: &Value) -> Option<AddressTxSummary> {
    let hash_str = v["transaction_hash"].as_str()?;
    let hash = Felt::from_hex(hash_str).ok()?;
    let nonce = v["nonce"]
        .as_str()
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .unwrap_or(0);
    let tx_type = v["type"].as_str().unwrap_or("INVOKE").to_string();
    let sender = v["sender_address"]
        .as_str()
        .and_then(|s| Felt::from_hex(s).ok());

    Some(AddressTxSummary {
        hash,
        nonce,
        block_number: 0,
        timestamp: 0,
        endpoint_names: String::new(),
        total_fee_fri: 0,
        tip: 0,
        tx_type,
        status: "?".to_string(),
        sender,
    })
}

/// Build a minimal `SnBlock` from a raw WS notification result value.
fn block_from_raw(v: &Value) -> Option<crate::data::types::SnBlock> {
    let block_number = v["block_number"].as_u64()?;
    let block_hash = Felt::from_hex(v["block_hash"].as_str()?).ok()?;
    let parent_hash = Felt::from_hex(v["parent_hash"].as_str().unwrap_or("0x0")).ok()?;
    let timestamp = v["timestamp"].as_u64().unwrap_or(0);
    let sequencer_address =
        Felt::from_hex(v["sequencer_address"].as_str().unwrap_or("0x0")).ok()?;
    let starknet_version = v["starknet_version"].as_str().unwrap_or("").to_string();
    let transaction_count = v["transaction_count"].as_u64().unwrap_or(0) as usize;

    let gas_fri = |field: &str| -> u128 {
        v[field]["price_in_fri"]
            .as_str()
            .and_then(|s| u128::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .unwrap_or(0)
    };
    let gas_wei = |field: &str| -> u128 {
        v[field]["price_in_wei"]
            .as_str()
            .and_then(|s| u128::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            .unwrap_or(0)
    };

    Some(crate::data::types::SnBlock {
        number: block_number,
        hash: block_hash,
        parent_hash,
        timestamp,
        sequencer_address,
        transaction_count,
        l1_gas_price_fri: gas_fri("l1_gas_price"),
        l1_gas_price_wei: gas_wei("l1_gas_price"),
        l2_gas_price_fri: gas_fri("l2_gas_price"),
        l1_data_gas_price_fri: gas_fri("l1_data_gas_price"),
        starknet_version,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use starknet::core::types::AddressFilter;

    #[test]
    fn build_subscribe_new_heads_msg() {
        let params = serde_json::to_value(SubscribeNewHeadsRequest { block_id: None }).unwrap();
        let msg = build_subscribe_msg(1, "starknet_subscribeNewHeads", params);
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 1);
        assert_eq!(v["method"], "starknet_subscribeNewHeads");
    }

    #[test]
    fn build_subscribe_events_msg_serializes_address() {
        let address = Felt::from_hex_unchecked(
            "0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f",
        );
        let req = SubscribeEventsRequest {
            from_address: Some(AddressFilter::Single(address)),
            keys: None,
            block_id: None,
            finality_status: Some(L2TransactionFinalityStatus::AcceptedOnL2),
        };
        let params = serde_json::to_value(req).unwrap();
        let msg = build_subscribe_msg(2, "starknet_subscribeEvents", params);
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["method"], "starknet_subscribeEvents");
        // from_address should be serialized as a hex string (AddressFilter::Single)
        assert!(v["params"]["from_address"].is_string());
    }

    #[test]
    fn build_subscribe_transactions_msg_serializes_sender() {
        let address = Felt::from_hex_unchecked(
            "0x01176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8",
        );
        let req = SubscribeNewTransactionsRequest {
            sender_address: Some(vec![address]),
            finality_status: Some(vec![L2TransactionStatus::AcceptedOnL2]),
            tags: None,
        };
        let params = serde_json::to_value(req).unwrap();
        let msg = build_subscribe_msg(3, "starknet_subscribeNewTransactions", params);
        let v: Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["method"], "starknet_subscribeNewTransactions");
        assert!(v["params"]["sender_address"].is_array());
    }

    #[test]
    fn parse_subscription_confirmation() {
        let json = r#"{"jsonrpc":"2.0","result":"0xdeadbeef","id":1}"#;
        let raw: RawMessage = serde_json::from_str(json).unwrap();
        assert!(raw.id.is_some());
        assert!(raw.error.is_none());
        let sub_id: SubscriptionId = serde_json::from_value(raw.result.unwrap()).unwrap();
        assert_eq!(sub_id.0, "0xdeadbeef");
    }

    #[test]
    fn parse_subscription_error() {
        let json =
            r#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}"#;
        let raw: RawMessage = serde_json::from_str(json).unwrap();
        assert!(raw.result.is_none());
        assert_eq!(raw.error.unwrap().message, "Method not found");
    }

    #[test]
    fn parse_event_notification_pathfinder_style() {
        // Pathfinder sends "subscription" instead of "subscription_id"
        let json = r#"{
            "jsonrpc": "2.0",
            "method": "starknet_subscriptionEvents",
            "params": {
                "subscription": "0x1",
                "result": {
                    "from_address": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
                    "keys": ["0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9"],
                    "data": ["0x0", "0x1"],
                    "block_hash": "0xabc",
                    "block_number": 100,
                    "transaction_hash": "0xdef",
                    "transaction_index": 0,
                    "event_index": 0,
                    "finality_status": "ACCEPTED_ON_L2"
                }
            }
        }"#;
        let raw: RawMessage = serde_json::from_str(json).unwrap();
        assert_eq!(raw.method.as_deref(), Some("starknet_subscriptionEvents"));
        let params = raw.params.unwrap();
        assert_eq!(params.subscription_id, serde_json::json!("0x1"));

        let event: EmittedEventWithFinality = serde_json::from_value(params.result).unwrap();
        assert_eq!(
            event.emitted_event.from_address,
            Felt::from_hex_unchecked(
                "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7"
            )
        );
        assert_eq!(event.emitted_event.block_number, Some(100));
    }

    #[test]
    fn parse_event_notification_spec_style() {
        // Spec uses "subscription_id"
        let json = r#"{
            "jsonrpc": "2.0",
            "method": "starknet_subscription",
            "params": {
                "subscription_id": "0x42",
                "result": {
                    "from_address": "0x1",
                    "keys": ["0x2"],
                    "data": [],
                    "block_hash": "0x3",
                    "block_number": 200,
                    "transaction_hash": "0x4",
                    "transaction_index": 0,
                    "event_index": 1,
                    "finality_status": "ACCEPTED_ON_L2"
                }
            }
        }"#;
        let raw: RawMessage = serde_json::from_str(json).unwrap();
        let params = raw.params.unwrap();
        assert_eq!(params.subscription_id, serde_json::json!("0x42"));
        let event: EmittedEventWithFinality = serde_json::from_value(params.result).unwrap();
        assert_eq!(event.emitted_event.block_number, Some(200));
    }

    #[test]
    fn block_from_raw_extracts_fields() {
        let json = serde_json::json!({
            "block_number": 12345,
            "block_hash": "0xabc",
            "parent_hash": "0xdef",
            "timestamp": 1700000000u64,
            "sequencer_address": "0x1",
            "starknet_version": "0.13.4",
            "transaction_count": 42,
            "l1_gas_price": { "price_in_fri": "0x100", "price_in_wei": "0x200" },
            "l2_gas_price": { "price_in_fri": "0x50" },
            "l1_data_gas_price": { "price_in_fri": "0x10" }
        });
        let block = block_from_raw(&json).expect("block");
        assert_eq!(block.number, 12345);
        assert_eq!(block.transaction_count, 42);
        assert_eq!(block.l1_gas_price_fri, 0x100);
        assert_eq!(block.l1_gas_price_wei, 0x200);
        assert_eq!(block.starknet_version, "0.13.4");
    }

    #[test]
    fn connection_state_tracks_subscriptions() {
        let mut state = ConnectionState::new();
        let addr = Felt::from_hex_unchecked("0x123");
        state
            .pending
            .insert(1, SubscriptionKind::Events { address: addr });
        state
            .pending
            .insert(2, SubscriptionKind::Transactions { address: addr });

        state.confirm_subscribe(1, "sub_events".to_string());
        state.confirm_subscribe(2, "sub_txs".to_string());

        assert!(state.subscriptions.contains_key("sub_events"));
        assert!(state.subscriptions.contains_key("sub_txs"));
        assert_eq!(state.address_subs[&addr].len(), 2);

        let removed = state.remove_address(addr);
        assert_eq!(removed.len(), 2);
        assert!(state.subscriptions.is_empty());
    }
}
