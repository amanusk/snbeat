//! Integration tests for Starknet WebSocket subscriptions.
//!
//! These tests verify that our WS subscription code works against a real node.
//! They use starknet-rust types for serialization/deserialization.
//!
//! Run with:
//!   APP_WS_URL=ws://localhost:9545/ws cargo test ws_subscription -- --ignored --nocapture

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use starknet::core::types::{
    AddressFilter, EmittedEventWithFinality, Felt, SubscriptionId,
    requests::{
        SubscribeEventsRequest, SubscribeNewHeadsRequest, SubscribeNewTransactionsRequest,
        UnsubscribeRequest,
    },
};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

// High-volume contracts for testing — events arrive constantly on mainnet
const AVNU_EXCHANGE: &str = "0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f";
const ETH_TOKEN: &str = "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";
const STRK_TOKEN: &str = "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d";

fn ws_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("APP_WS_URL").expect("APP_WS_URL must be set for WS integration tests")
}

fn felt(hex: &str) -> Felt {
    Felt::from_hex(hex).expect("valid hex felt")
}

/// A generic JSON-RPC notification or response envelope.
#[derive(Debug, Deserialize)]
struct RawMessage {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    /// Present in responses to our subscribe/unsubscribe requests.
    id: Option<Value>,
    /// Present in notifications pushed by the server.
    method: Option<String>,
    /// Present in subscription notifications.
    params: Option<RawNotificationParams>,
    /// Present in subscription confirmation responses.
    result: Option<Value>,
    error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct RawNotificationParams {
    #[serde(alias = "subscription", alias = "subscription_id")]
    subscription_id: Value,
    result: Value,
}

/// Send a JSON-RPC subscribe request and return the confirmed SubscriptionId.
async fn send_subscribe(
    write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    read: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    id: u64,
    method: &str,
    params: Value,
) -> SubscriptionId {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    write
        .send(Message::Text(request.to_string().into()))
        .await
        .expect("send subscribe request");

    // Wait for confirmation response
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), read.next())
            .await
            .expect("timeout waiting for confirmation")
            .expect("stream ended")
            .expect("ws error");

        if let Message::Text(text) = msg {
            let raw: RawMessage = serde_json::from_str(&text).expect("parse response");
            if raw.id.as_ref().and_then(|v| v.as_u64()) == Some(id) {
                if let Some(err) = raw.error {
                    panic!("subscribe rejected: {err}");
                }
                let sub_id_val = raw.result.expect("result field in confirmation");
                let sub_id: SubscriptionId =
                    serde_json::from_value(sub_id_val).expect("parse SubscriptionId");
                return sub_id;
            }
        }
    }
}

/// Send an unsubscribe request.
async fn send_unsubscribe(
    write: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    id: u64,
    subscription_id: &SubscriptionId,
) {
    let req = UnsubscribeRequest {
        subscription_id: subscription_id.clone(),
    };
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "starknet_unsubscribe",
        "params": serde_json::to_value(req).unwrap(),
    });
    write
        .send(Message::Text(request.to_string().into()))
        .await
        .expect("send unsubscribe");
}

/// Wait for a notification with the given subscription_id, with timeout.
async fn wait_for_notification(
    read: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    subscription_id: &SubscriptionId,
    timeout_secs: u64,
) -> RawNotificationParams {
    let deadline = Duration::from_secs(timeout_secs);
    loop {
        let msg = tokio::time::timeout(deadline, read.next())
            .await
            .expect("timeout waiting for notification — is APP_WS_URL a high-volume node?")
            .expect("stream ended")
            .expect("ws error");

        if let Message::Text(text) = msg {
            let raw: RawMessage = serde_json::from_str(&text)
                .unwrap_or_else(|e| panic!("failed to parse ws message: {e}\nraw: {text}"));
            if let Some(method) = &raw.method
                && method.starts_with("starknet_subscription")
                && let Some(params) = raw.params
            {
                // Match by subscription_id (handles both string and int forms)
                let our_id = serde_json::to_value(&subscription_id.0).unwrap();
                if params.subscription_id == our_id
                    || params.subscription_id.as_str() == Some(&subscription_id.0)
                {
                    return params;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test 1: starknet_subscribeNewHeads — verify starknet-rust types work
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_subscribe_new_heads() {
    let url = ws_url();
    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    let params = serde_json::to_value(SubscribeNewHeadsRequest { block_id: None }).unwrap();
    let sub_id = send_subscribe(
        &mut write,
        &mut read,
        1,
        "starknet_subscribeNewHeads",
        params,
    )
    .await;
    println!("subscribeNewHeads confirmed, sub_id={}", sub_id.0);

    // Wait for one block notification (may take up to ~30s on mainnet)
    let notif = wait_for_notification(&mut read, &sub_id, 90).await;
    println!("block notification result: {}", notif.result);

    let block_number = notif.result["block_number"]
        .as_u64()
        .expect("block_number in result");
    let block_hash = notif.result["block_hash"].as_str().expect("block_hash");
    assert!(block_number > 0, "block_number should be > 0");
    assert!(block_hash.starts_with("0x"), "block_hash should be hex");
    println!("Got block #{block_number}, hash={block_hash}");

    send_unsubscribe(&mut write, 2, &sub_id).await;
    println!("Unsubscribed successfully");
}

// ---------------------------------------------------------------------------
// Test 2: starknet_subscribeEvents — AVNU Exchange (high volume contract)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_subscribe_events_avnu() {
    let url = ws_url();
    let avnu = felt(AVNU_EXCHANGE);

    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    let req = SubscribeEventsRequest {
        from_address: Some(AddressFilter::Single(avnu)),
        keys: None,
        block_id: None,
        finality_status: None,
    };
    let params = serde_json::to_value(req).unwrap();
    println!("subscribeEvents params: {params}");

    let sub_id = send_subscribe(&mut write, &mut read, 1, "starknet_subscribeEvents", params).await;
    println!("subscribeEvents AVNU confirmed, sub_id={}", sub_id.0);

    // AVNU may not have swaps every minute — allow up to 5 min
    let notif = wait_for_notification(&mut read, &sub_id, 300).await;
    println!("event notification result: {}", notif.result);

    let event: EmittedEventWithFinality = serde_json::from_value(notif.result.clone())
        .unwrap_or_else(|e| {
            panic!(
                "failed to deserialize EmittedEventWithFinality: {e}\nraw: {}",
                notif.result
            )
        });

    assert_eq!(
        event.emitted_event.from_address, avnu,
        "event from_address should match AVNU"
    );
    assert_ne!(
        event.emitted_event.transaction_hash,
        Felt::ZERO,
        "tx hash should not be zero"
    );
    println!(
        "Got event from AVNU: tx={:#x}, block={:?}",
        event.emitted_event.transaction_hash, event.emitted_event.block_number
    );

    send_unsubscribe(&mut write, 2, &sub_id).await;
}

// ---------------------------------------------------------------------------
// Test 3: starknet_subscribeEvents — ETH token (very high volume)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_subscribe_events_eth_token() {
    let url = ws_url();
    let eth = felt(ETH_TOKEN);

    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    let req = SubscribeEventsRequest {
        from_address: Some(AddressFilter::Single(eth)),
        keys: None,
        block_id: None,
        finality_status: None,
    };
    let sub_id = send_subscribe(
        &mut write,
        &mut read,
        1,
        "starknet_subscribeEvents",
        serde_json::to_value(req).unwrap(),
    )
    .await;
    println!("subscribeEvents ETH confirmed, sub_id={}", sub_id.0);

    let notif = wait_for_notification(&mut read, &sub_id, 60).await;
    let event: EmittedEventWithFinality = serde_json::from_value(notif.result.clone())
        .unwrap_or_else(|e| panic!("failed to deserialize event: {e}\nraw: {}", notif.result));

    assert_eq!(event.emitted_event.from_address, eth);
    println!(
        "ETH event: tx={:#x}, finality={:?}",
        event.emitted_event.transaction_hash, event.finality_status
    );

    send_unsubscribe(&mut write, 2, &sub_id).await;
}

// ---------------------------------------------------------------------------
// Test 4: starknet_subscribeNewTransactions — all senders (unfiltered)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_subscribe_new_transactions_unfiltered() {
    let url = ws_url();
    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    // Subscribe with no sender filter — get all new transactions
    let req = SubscribeNewTransactionsRequest {
        sender_address: None,
        finality_status: None,
        tags: None,
    };
    let sub_id = send_subscribe(
        &mut write,
        &mut read,
        1,
        "starknet_subscribeNewTransactions",
        serde_json::to_value(req).unwrap(),
    )
    .await;
    println!("subscribeNewTransactions confirmed, sub_id={}", sub_id.0);

    let notif = wait_for_notification(&mut read, &sub_id, 30).await;
    println!("tx notification: {}", notif.result);

    // Verify it has a transaction hash
    let tx_hash = notif.result["transaction_hash"]
        .as_str()
        .or_else(|| notif.result["hash"].as_str());
    assert!(
        tx_hash.is_some(),
        "expected transaction_hash in result: {}",
        notif.result
    );
    println!("New tx: {}", tx_hash.unwrap());

    send_unsubscribe(&mut write, 2, &sub_id).await;
}

// ---------------------------------------------------------------------------
// Test 5: Multiple subscriptions on one connection
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_multiple_subscriptions_one_connection() {
    let url = ws_url();
    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    // Subscribe to NewHeads
    let heads_params = serde_json::to_value(SubscribeNewHeadsRequest { block_id: None }).unwrap();
    let heads_sub = send_subscribe(
        &mut write,
        &mut read,
        1,
        "starknet_subscribeNewHeads",
        heads_params,
    )
    .await;
    println!("NewHeads sub_id={}", heads_sub.0);

    // Subscribe to events for ETH token
    let eth = felt(ETH_TOKEN);
    let events_req = SubscribeEventsRequest {
        from_address: Some(AddressFilter::Single(eth)),
        keys: None,
        block_id: None,
        finality_status: None,
    };
    let events_sub = send_subscribe(
        &mut write,
        &mut read,
        2,
        "starknet_subscribeEvents",
        serde_json::to_value(events_req).unwrap(),
    )
    .await;
    println!("Events sub_id={}", events_sub.0);

    assert_ne!(heads_sub.0, events_sub.0, "subscription IDs must be unique");

    // Wait for an event notification from the events subscription
    let notif = wait_for_notification(&mut read, &events_sub, 60).await;
    let event: EmittedEventWithFinality =
        serde_json::from_value(notif.result).expect("parse event");
    assert_eq!(event.emitted_event.from_address, eth);
    println!(
        "Got ETH event on multi-sub connection: tx={:#x}",
        event.emitted_event.transaction_hash
    );

    // Unsubscribe both
    send_unsubscribe(&mut write, 3, &heads_sub).await;
    send_unsubscribe(&mut write, 4, &events_sub).await;
    println!("Both unsubscribed successfully");
}

// ---------------------------------------------------------------------------
// Test 6: Unsubscribe stops notifications (uses ETH token — high frequency)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_unsubscribe_stops_notifications() {
    let url = ws_url();
    let avnu = felt(ETH_TOKEN); // ETH token has high frequency events

    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    let req = SubscribeEventsRequest {
        from_address: Some(AddressFilter::Single(avnu)),
        keys: None,
        block_id: None,
        finality_status: None,
    };
    let sub_id = send_subscribe(
        &mut write,
        &mut read,
        1,
        "starknet_subscribeEvents",
        serde_json::to_value(req).unwrap(),
    )
    .await;

    // Get one notification to confirm subscription works
    let notif = wait_for_notification(&mut read, &sub_id, 60).await;
    let event: EmittedEventWithFinality =
        serde_json::from_value(notif.result).expect("parse event");
    println!(
        "Got event before unsub: tx={:#x}",
        event.emitted_event.transaction_hash
    );

    // Unsubscribe
    send_unsubscribe(&mut write, 2, &sub_id).await;

    // Wait for the actual unsubscribe confirmation (id=2), skipping any buffered notifications
    let confirmed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let msg = read.next().await?.ok()?;
            if let Message::Text(ref text) = msg {
                println!("Unsubscribe wait, got: {text}");
                if let Ok(raw) = serde_json::from_str::<RawMessage>(text) {
                    // Confirmation is a response with id=2 (our unsubscribe request id)
                    if raw.id == Some(serde_json::Value::Number(2.into())) {
                        return Some(());
                    }
                    // Otherwise it's a buffered notification — skip it
                }
            }
        }
    })
    .await;
    assert!(
        confirmed.is_ok(),
        "did not receive unsubscribe confirmation within 10s"
    );

    // Now no more notifications should arrive for this sub_id (within 5s)
    let extra = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let msg = read.next().await?.ok()?;
            if let Message::Text(text) = msg {
                let raw: RawMessage = serde_json::from_str(&text).ok()?;
                if let Some(params) = raw.params
                    && params.subscription_id.as_str() == Some(&sub_id.0)
                {
                    return Some(params.result);
                }
            }
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "should not receive more notifications after unsubscribe"
    );
    println!("Confirmed: no notifications after unsubscribe");
}

// ---------------------------------------------------------------------------
// Test 7: STRK token events — verify event structure
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires APP_WS_URL"]
async fn test_subscribe_events_strk_token() {
    let url = ws_url();
    let strk = felt(STRK_TOKEN);

    let (ws, _) = connect_async(&url).await.expect("connect");
    let (mut write, mut read) = ws.split();

    let req = SubscribeEventsRequest {
        from_address: Some(AddressFilter::Single(strk)),
        keys: None,
        block_id: None,
        finality_status: None,
    };
    let sub_id = send_subscribe(
        &mut write,
        &mut read,
        1,
        "starknet_subscribeEvents",
        serde_json::to_value(req).unwrap(),
    )
    .await;
    println!("STRK events sub_id={}", sub_id.0);

    let notif = wait_for_notification(&mut read, &sub_id, 60).await;
    let event: EmittedEventWithFinality = serde_json::from_value(notif.result.clone())
        .unwrap_or_else(|e| panic!("deserialize event: {e}\nraw: {}", notif.result));

    assert_eq!(
        event.emitted_event.from_address, strk,
        "from_address should be STRK"
    );
    assert!(
        !event.emitted_event.keys.is_empty(),
        "event should have keys (event selector)"
    );
    assert_ne!(event.emitted_event.transaction_hash, Felt::ZERO);

    let selector = event.emitted_event.keys[0];
    println!(
        "STRK event: selector={:#x}, tx={:#x}, finality={:?}",
        selector, event.emitted_event.transaction_hash, event.finality_status
    );

    send_unsubscribe(&mut write, 2, &sub_id).await;
}
