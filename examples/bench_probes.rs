//! Benchmark different probe approaches for discovering address activity ranges.
//!
//! Usage:
//!   cargo run --example bench_probes -- <address_hex>
//!
//! Requires env vars: DUNE_API_KEY, APP_RPC_URL, APP_PATHFINDER_SERVICE_URL (optional),
//! VOYAGER_API_KEY (optional).

use std::time::Instant;

use starknet::core::types::{AddressFilter, BlockId, BlockTag, EventFilter, Felt};
use starknet::providers::{JsonRpcClient, Provider, jsonrpc::HttpTransport};
use url::Url;

const DUNE_API_BASE: &str = "https://api.dune.com/api/v1";

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: bench_probes <address_hex>");
        std::process::exit(1);
    }
    let address = Felt::from_hex(&args[1]).expect("invalid address hex");
    let addr_hex = format!("{:#066x}", address);

    println!("=== Probe Benchmarks for {} ===\n", &addr_hex[..12]);

    // --- RPC benchmarks ---
    if let Ok(rpc_url) = std::env::var("APP_RPC_URL") {
        let provider = JsonRpcClient::new(HttpTransport::new(
            Url::parse(&rpc_url).expect("bad RPC URL"),
        ));

        // R0: get_nonce
        let t = Instant::now();
        let nonce = provider
            .get_nonce(BlockId::Tag(BlockTag::Latest), address)
            .await;
        let nonce_ms = t.elapsed().as_millis();
        let nonce_val = nonce
            .as_ref()
            .map(|n| format!("{}", n))
            .unwrap_or("err".into());
        println!(
            "R0  get_nonce              {:>6}ms  nonce={}",
            nonce_ms, nonce_val
        );

        let is_account = nonce.as_ref().map(|n| *n != Felt::ZERO).unwrap_or(false);

        // R1: get_events 5k window
        let latest = provider.block_number().await.unwrap_or(0);
        for (label, window) in [("5k", 5_000u64), ("10k", 10_000), ("50k", 50_000)] {
            let from = latest.saturating_sub(window);
            let filter = EventFilter {
                from_block: Some(BlockId::Number(from)),
                to_block: Some(BlockId::Tag(BlockTag::Latest)),
                address: Some(AddressFilter::Single(address)),
                keys: None,
            };
            let t = Instant::now();
            let result = provider.get_events(filter, None, 10).await;
            let ms = t.elapsed().as_millis();
            let count = result.as_ref().map(|p| p.events.len()).unwrap_or(0);
            println!(
                "R{:<3}events {}w          {:>6}ms  found={}",
                label, label, ms, count
            );
        }

        // R: expanding window
        let t = Instant::now();
        let mut found_in = "none";
        for (label, window) in [
            ("50k", 50_000u64),
            ("200k", 200_000),
            ("1M", 1_000_000),
            ("all", latest),
        ] {
            let from = latest.saturating_sub(window);
            let filter = EventFilter {
                from_block: Some(BlockId::Number(from)),
                to_block: Some(BlockId::Tag(BlockTag::Latest)),
                address: Some(AddressFilter::Single(address)),
                keys: None,
            };
            if let Ok(page) = provider.get_events(filter, None, 1).await {
                if !page.events.is_empty() {
                    found_in = label;
                    break;
                }
            }
        }
        let ms = t.elapsed().as_millis();
        println!(
            "R   expanding window      {:>6}ms  found_in={}",
            ms, found_in
        );

        println!("    is_account={}", is_account);
    } else {
        println!("(RPC skipped — APP_RPC_URL not set)");
    }

    println!();

    // --- Pathfinder benchmarks ---
    if let Ok(pf_url) = std::env::var("APP_PATHFINDER_SERVICE_URL") {
        let client = reqwest::Client::new();

        // P1: nonce-history limit=1
        let t = Instant::now();
        let resp = client
            .get(format!("{}/nonce-history/{}?limit=1", pf_url, addr_hex))
            .send()
            .await;
        let ms = t.elapsed().as_millis();
        let body = resp.map(|r| r.text()).ok();
        let body_str = if let Some(f) = body {
            f.await.unwrap_or_default()
        } else {
            String::new()
        };
        let count = body_str.matches("block_number").count();
        println!("P1  nonce-history?limit=1  {:>6}ms  entries={}", ms, count);

        // P2: sender-txs limit=10
        let t = Instant::now();
        let resp = client
            .get(format!("{}/sender-txs/{}?limit=10", pf_url, addr_hex))
            .send()
            .await;
        let ms = t.elapsed().as_millis();
        let body = resp.map(|r| r.text()).ok();
        let body_str = if let Some(f) = body {
            f.await.unwrap_or_default()
        } else {
            String::new()
        };
        let count = body_str.matches("\"hash\"").count();
        println!("P2  sender-txs?limit=10   {:>6}ms  txs={}", ms, count);
    } else {
        println!("(PF skipped — APP_PATHFINDER_SERVICE_URL not set)");
    }

    println!();

    // --- Voyager benchmarks ---
    if let Ok(api_key) = std::env::var("VOYAGER_API_KEY") {
        let client = reqwest::Client::new();
        let t = Instant::now();
        let resp = client
            .get(format!(
                "https://api.voyager.online/beta/contracts/{}",
                addr_hex
            ))
            .header("X-API-Key", &api_key)
            .send()
            .await;
        let ms = t.elapsed().as_millis();
        if let Ok(r) = resp {
            let body: serde_json::Value = r.json().await.unwrap_or_default();
            let block = body.get("blockNumber").and_then(|v| v.as_u64());
            let alias = body
                .get("contractAlias")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            let is_account = body
                .get("isAccount")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            println!(
                "V1  /contracts             {:>6}ms  deploy_block={:?} alias={} is_account={}",
                ms, block, alias, is_account
            );
        } else {
            println!("V1  /contracts             {:>6}ms  error", ms);
        }
    } else {
        println!("(Voyager skipped — VOYAGER_API_KEY not set)");
    }

    println!();

    // --- Dune benchmarks ---
    if let Ok(api_key) = std::env::var("DUNE_API_KEY") {
        let client = reqwest::Client::new();

        let probes = vec![
            (
                "D1 UNION ALL (current)",
                format!(
                    "SELECT 'sender' AS role, COUNT(*) AS cnt, MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                     FROM starknet.transactions WHERE sender_address = {addr} AND block_date >= date '2024-01-01' \
                     UNION ALL \
                     SELECT 'callee' AS role, COUNT(*) AS cnt, MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                     FROM starknet.calls WHERE contract_address = {addr} AND block_date >= date '2024-01-01' AND call_type = 'CALL'",
                    addr = addr_hex
                ),
            ),
            (
                "D2 events since 2024",
                format!(
                    "SELECT COUNT(*) AS cnt, MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                     FROM starknet.events WHERE from_address = {addr} AND block_date >= date '2024-01-01'",
                    addr = addr_hex
                ),
            ),
            (
                "D3 events 30d",
                format!(
                    "SELECT COUNT(*) AS cnt, MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                     FROM starknet.events WHERE from_address = {addr} AND block_date >= CURRENT_DATE - INTERVAL '30' DAY",
                    addr = addr_hex
                ),
            ),
            (
                "D4 events 7d",
                format!(
                    "SELECT COUNT(*) AS cnt, MIN(block_number) AS min_block, MAX(block_number) AS max_block \
                     FROM starknet.events WHERE from_address = {addr} AND block_date >= CURRENT_DATE - INTERVAL '7' DAY",
                    addr = addr_hex
                ),
            ),
            (
                "D5 existence 7d",
                format!(
                    "SELECT 1 AS found FROM starknet.events WHERE from_address = {addr} AND block_date >= CURRENT_DATE - INTERVAL '7' DAY LIMIT 1",
                    addr = addr_hex
                ),
            ),
        ];

        for (label, sql) in &probes {
            let t = Instant::now();
            match dune_execute(&client, &api_key, sql).await {
                Ok((rows, credits)) => {
                    let ms = t.elapsed().as_millis();
                    let summary = if rows.is_empty() {
                        "empty".to_string()
                    } else {
                        let r = &rows[0];
                        let cnt = r.get("cnt").and_then(|v| v.as_u64());
                        let max = r.get("max_block").and_then(|v| v.as_u64());
                        let min = r.get("min_block").and_then(|v| v.as_u64());
                        let found = r.get("found").and_then(|v| v.as_u64());
                        if let Some(f) = found {
                            format!("found={}", f)
                        } else if let Some(c) = cnt {
                            format!("cnt={} blocks={:?}..{:?}", c, min, max)
                        } else {
                            format!("{:?}", rows[0])
                        }
                    };
                    println!(
                        "{:<25} {:>6}ms  {:.3} credits  {}",
                        label, ms, credits, summary
                    );
                }
                Err(e) => {
                    let ms = t.elapsed().as_millis();
                    println!("{:<25} {:>6}ms  ERROR: {}", label, ms, e);
                }
            }
        }
    } else {
        println!("(Dune skipped — DUNE_API_KEY not set)");
    }

    println!("\nDone.");
}

/// Execute a SQL query on Dune and return (rows, credits_cost).
async fn dune_execute(
    client: &reqwest::Client,
    api_key: &str,
    sql: &str,
) -> Result<(Vec<serde_json::Value>, f64), String> {
    #[derive(serde::Deserialize)]
    struct CreateResp {
        query_id: u64,
    }
    #[derive(serde::Deserialize)]
    struct ExecResp {
        execution_id: String,
    }
    #[derive(serde::Deserialize)]
    struct StatusResp {
        state: String,
        result: Option<ResultData>,
    }
    #[derive(serde::Deserialize)]
    struct ResultData {
        rows: Vec<serde_json::Value>,
    }

    let create: CreateResp = client
        .post(format!("{}/query", DUNE_API_BASE))
        .header("X-Dune-API-Key", api_key)
        .json(&serde_json::json!({
            "name": format!("bench_{}", chrono::Utc::now().timestamp()),
            "query_sql": sql,
            "is_private": true
        }))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;

    let exec: ExecResp = client
        .post(format!(
            "{}/query/{}/execute",
            DUNE_API_BASE, create.query_id
        ))
        .header("X-Dune-API-Key", api_key)
        .json(&serde_json::json!({}))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;

    for _ in 0..60 {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let resp_text = client
            .get(format!(
                "{}/execution/{}/results",
                DUNE_API_BASE, exec.execution_id
            ))
            .header("X-Dune-API-Key", api_key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .text()
            .await
            .map_err(|e| e.to_string())?;

        let status: StatusResp =
            serde_json::from_str(&resp_text).map_err(|e| format!("parse: {e}"))?;

        match status.state.as_str() {
            "QUERY_STATE_COMPLETED" => {
                let rows = status.result.map(|r| r.rows).unwrap_or_default();
                // Extract credits from raw JSON
                let raw: serde_json::Value = serde_json::from_str(&resp_text).unwrap_or_default();
                let credits = raw
                    .get("execution_cost_credits")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                return Ok((rows, credits));
            }
            "QUERY_STATE_FAILED" | "QUERY_STATE_CANCELLED" | "QUERY_STATE_EXPIRED" => {
                return Err(format!("query {}: {}", create.query_id, status.state));
            }
            _ => continue,
        }
    }
    Err("timeout".into())
}
