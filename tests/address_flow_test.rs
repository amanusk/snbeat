//! Integration tests for the address info fetch flow.
//! Tests: event-based fetch, nonce gap detection, Dune client.
//!
//! Run with: cargo test --test address_flow_test -- --ignored --nocapture

use starknet::core::types::{BlockId, BlockTag, Felt};
use starknet::providers::{JsonRpcClient, Provider, jsonrpc::HttpTransport};
use std::sync::Arc;
use url::Url;

fn provider() -> Arc<JsonRpcClient<HttpTransport>> {
    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("APP_RPC_URL").expect("APP_RPC_URL required");
    Arc::new(JsonRpcClient::new(HttpTransport::new(
        Url::parse(&rpc_url).unwrap(),
    )))
}

// Address known to have reverted txs
const REVERTED_ADDR: &str = "0x0164dab8b7762d5af0834cb29fc11069b567a59a5228a9885976b3ec81d5885e";

#[tokio::test]
#[ignore]
async fn test_nonce_gap_detection() {
    let p = provider();
    let addr = Felt::from_hex(REVERTED_ADDR).unwrap();

    let nonce = p
        .get_nonce(BlockId::Tag(BlockTag::Latest), addr)
        .await
        .expect("get_nonce failed");
    let nonce_val = felt_to_u64(&nonce);
    println!("Address nonce: {}", nonce_val);

    // Fetch some events
    let tx_executed =
        Felt::from_hex("0x01dcde06aabdbca2f80aa51392b345d7549d7757aa855f7e37f5d335ac8243b1")
            .unwrap();

    use starknet::core::types::{AddressFilter, EventFilter};
    let latest = p.block_number().await.unwrap();

    let filter = EventFilter {
        from_block: Some(BlockId::Number(latest.saturating_sub(50_000))),
        to_block: Some(BlockId::Tag(BlockTag::Latest)),
        address: Some(AddressFilter::Single(addr)),
        keys: Some(vec![vec![tx_executed]]),
    };

    let page = p
        .get_events(filter, None, 50)
        .await
        .expect("get_events failed");
    println!("Events: {}", page.events.len());

    // Fetch txs to get nonces
    let mut nonces = Vec::new();
    for e in page.events.iter().take(20) {
        if let Ok(tx) = p.get_transaction_by_hash(e.transaction_hash, None).await {
            let n = extract_nonce(&tx);
            nonces.push((n, e.block_number.unwrap_or(0)));
        }
    }
    nonces.sort_by(|a, b| b.0.cmp(&a.0));

    println!("Nonces found (newest first):");
    for (n, b) in &nonces {
        println!("  nonce {} in block {}", n, b);
    }

    // Check for gaps
    if nonces.len() > 1 {
        let mut gaps = 0;
        for i in 0..nonces.len() - 1 {
            let diff = nonces[i].0 - nonces[i + 1].0;
            if diff > 1 {
                println!(
                    "  GAP: {} missing nonces between {} and {}",
                    diff - 1,
                    nonces[i + 1].0,
                    nonces[i].0
                );
                gaps += diff - 1;
            }
        }
        println!("Total gaps: {} missing nonces", gaps);
        // For the reverted address, we expect gaps
        if gaps > 0 {
            println!("CONFIRMED: gaps exist — these are reverted txs");
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_block_scan_finds_reverted_tx() {
    let p = provider();
    let addr = Felt::from_hex(REVERTED_ADDR).unwrap();

    // Get events to find a nonce gap
    let tx_executed =
        Felt::from_hex("0x01dcde06aabdbca2f80aa51392b345d7549d7757aa855f7e37f5d335ac8243b1")
            .unwrap();

    use starknet::core::types::{
        AddressFilter, EventFilter, InvokeTransaction, MaybePreConfirmedBlockWithTxs, Transaction,
    };
    let latest = p.block_number().await.unwrap();

    let filter = EventFilter {
        from_block: Some(BlockId::Number(latest.saturating_sub(50_000))),
        to_block: Some(BlockId::Tag(BlockTag::Latest)),
        address: Some(AddressFilter::Single(addr)),
        keys: Some(vec![vec![tx_executed]]),
    };

    let page = p
        .get_events(filter, None, 20)
        .await
        .expect("get_events failed");
    if page.events.len() < 2 {
        println!("Not enough events to test gap detection, skipping");
        return;
    }

    // Get nonces for first two events
    let tx1 = p
        .get_transaction_by_hash(page.events[0].transaction_hash, None)
        .await
        .unwrap();
    let tx2 = p
        .get_transaction_by_hash(page.events[1].transaction_hash, None)
        .await
        .unwrap();
    let n1 = extract_nonce(&tx1);
    let n2 = extract_nonce(&tx2);
    let b1 = page.events[0].block_number.unwrap_or(0);
    let b2 = page.events[1].block_number.unwrap_or(0);

    println!("Event 0: nonce {} block {}", n1, b1);
    println!("Event 1: nonce {} block {}", n2, b2);

    let (high_n, high_b, low_n, low_b) = if n1 > n2 {
        (n1, b1, n2, b2)
    } else {
        (n2, b2, n1, b1)
    };
    let gap = high_n - low_n;

    if gap <= 1 {
        println!("No gap between these two events, skipping block scan test");
        return;
    }

    println!(
        "Gap: {} missing nonces between nonce {} (block {}) and nonce {} (block {})",
        gap - 1,
        low_n,
        low_b,
        high_n,
        high_b
    );

    // Scan blocks in the gap range
    let scan_from = low_b.min(high_b);
    let scan_to = low_b.max(high_b);
    println!(
        "Scanning blocks {} to {} for sender txs...",
        scan_from, scan_to
    );

    let mut found_txs = Vec::new();
    for block_num in scan_from..=scan_to {
        let block = p.get_block_with_txs(BlockId::Number(block_num), None).await;
        if let Ok(MaybePreConfirmedBlockWithTxs::Block(b)) = block {
            for tx in &b.transactions {
                let sender = match tx {
                    Transaction::Invoke(InvokeTransaction::V3(v)) => Some(v.sender_address),
                    Transaction::Invoke(InvokeTransaction::V1(v)) => Some(v.sender_address),
                    _ => None,
                };
                if sender == Some(addr) {
                    let nonce = extract_nonce_raw(tx);
                    println!(
                        "  Found tx in block {}: nonce {} hash {:#x}",
                        block_num,
                        nonce,
                        tx_hash(tx)
                    );
                    found_txs.push((nonce, block_num));
                }
            }
        }
    }

    println!(
        "Found {} txs by scanning {} blocks",
        found_txs.len(),
        scan_to - scan_from + 1
    );
    if found_txs.len() > 2 {
        println!("SUCCESS: Block scanning found more txs than events (likely reverted txs)");
    }
}

fn felt_to_u64(felt: &Felt) -> u64 {
    let bytes = felt.to_bytes_be();
    u64::from_be_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]))
}

fn extract_nonce(tx: &starknet::core::types::Transaction) -> u64 {
    extract_nonce_raw(tx)
}

fn extract_nonce_raw(tx: &starknet::core::types::Transaction) -> u64 {
    use starknet::core::types::{InvokeTransaction, Transaction};
    match tx {
        Transaction::Invoke(InvokeTransaction::V3(v)) => felt_to_u64(&v.nonce),
        Transaction::Invoke(InvokeTransaction::V1(v)) => felt_to_u64(&v.nonce),
        Transaction::Invoke(InvokeTransaction::V0(_)) => 0,
        _ => 0,
    }
}

fn tx_hash(tx: &starknet::core::types::Transaction) -> Felt {
    use starknet::core::types::{
        DeclareTransaction, DeployAccountTransaction, InvokeTransaction, Transaction,
    };
    match tx {
        Transaction::Invoke(InvokeTransaction::V3(v)) => v.transaction_hash,
        Transaction::Invoke(InvokeTransaction::V1(v)) => v.transaction_hash,
        Transaction::Invoke(InvokeTransaction::V0(v)) => v.transaction_hash,
        Transaction::Declare(DeclareTransaction::V3(v)) => v.transaction_hash,
        Transaction::Declare(DeclareTransaction::V2(v)) => v.transaction_hash,
        Transaction::Declare(DeclareTransaction::V1(v)) => v.transaction_hash,
        Transaction::Declare(DeclareTransaction::V0(v)) => v.transaction_hash,
        Transaction::DeployAccount(DeployAccountTransaction::V3(v)) => v.transaction_hash,
        Transaction::DeployAccount(DeployAccountTransaction::V1(v)) => v.transaction_hash,
        Transaction::L1Handler(v) => v.transaction_hash,
        Transaction::Deploy(v) => v.transaction_hash,
    }
}
