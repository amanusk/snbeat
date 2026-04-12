//! Populate the selector_names DB with ABIs from all known contracts.
//!
//! Run with: cargo test --test populate_selectors -- --ignored --nocapture
//!
//! This fetches class ABIs for every address in labels.toml + bundled known addresses,
//! parses them, and writes all function/event selectors to ~/.config/snbeat/cache.db.
//! After running, the explorer will show decoded names without needing RPC lookups.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rusqlite::{Connection, params};
use starknet::core::types::{BlockId, BlockTag, Felt};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::{JsonRpcClient, Provider, jsonrpc::HttpTransport};
use url::Url;

fn provider() -> Arc<JsonRpcClient<HttpTransport>> {
    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("APP_RPC_URL").expect("APP_RPC_URL required");
    Arc::new(JsonRpcClient::new(HttpTransport::new(
        Url::parse(&rpc_url).unwrap(),
    )))
}

fn cache_db() -> Connection {
    let path = dirs::config_dir()
        .map(|d| d.join("snbeat").join("cache.db"))
        .unwrap_or_else(|| std::path::PathBuf::from(".snbeat/cache.db"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = Connection::open(&path).expect("Failed to open cache.db");
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS selector_names (
            selector TEXT PRIMARY KEY,
            name TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS parsed_abis (
            class_hash TEXT PRIMARY KEY,
            data TEXT NOT NULL
        );",
    )
    .unwrap();
    println!("Using cache DB: {}", path.display());
    db
}

fn insert_selector(db: &Connection, selector: Felt, name: &str) -> bool {
    let hex = format!("{:#x}", selector);
    db.execute(
        "INSERT OR IGNORE INTO selector_names (selector, name) VALUES (?1, ?2)",
        params![hex, name],
    )
    .map(|changed| changed > 0)
    .unwrap_or(false)
}

fn insert_abi(db: &Connection, class_hash: Felt, abi: &snbeat::decode::abi::ParsedAbi) {
    if let Ok(json) = serde_json::to_string(abi) {
        let hex = format!("{:#x}", class_hash);
        let _ = db.execute(
            "INSERT OR REPLACE INTO parsed_abis (class_hash, data) VALUES (?1, ?2)",
            params![hex, json],
        );
    }
}

/// Collect all unique addresses from labels.toml + bundled known addresses.
fn collect_all_addresses() -> Vec<(Felt, String)> {
    let mut addresses = Vec::new();
    let mut seen = HashSet::new();

    // Load from labels.toml
    let labels_path = Path::new("labels.toml");
    if labels_path.exists() {
        if let Ok((labels, _)) = snbeat::registry::user_labels::load_user_labels(labels_path) {
            for label in labels {
                if seen.insert(label.address) {
                    addresses.push((label.address, label.name));
                }
            }
        }
    }

    // Load bundled known addresses
    if let Ok(known) =
        snbeat::registry::known_addresses::load_known_addresses(Path::new("/nonexistent"))
    {
        for addr in known {
            if seen.insert(addr.address) {
                addresses.push((addr.address, addr.name));
            }
        }
    }

    addresses
}

/// Well-known selectors that we always want indexed, even if we can't fetch the class.
fn hardcoded_selectors() -> Vec<(Felt, &'static str)> {
    let mut selectors = Vec::new();

    // Common ERC20 functions
    let functions = [
        "transfer",
        "transferFrom",
        "approve",
        "balance_of",
        "balanceOf",
        "total_supply",
        "totalSupply",
        "allowance",
        "name",
        "symbol",
        "decimals",
        "mint",
        "burn",
        "increase_allowance",
        "decrease_allowance",
    ];
    for name in functions {
        if let Ok(sel) = get_selector_from_name(name) {
            selectors.push((sel, name));
        }
    }

    // Common ERC20 events
    let events = ["Transfer", "Approval"];
    for name in events {
        if let Ok(sel) = get_selector_from_name(name) {
            selectors.push((sel, name));
        }
    }

    // Account events
    let account_events = ["transaction_executed", "TransactionExecuted"];
    for name in account_events {
        if let Ok(sel) = get_selector_from_name(name) {
            selectors.push((sel, name));
        }
    }

    // Starknet system
    let system = [
        "__execute__",
        "__validate__",
        "__validate_declare__",
        "__validate_deploy__",
        "constructor",
        "upgrade",
    ];
    for name in system {
        if let Ok(sel) = get_selector_from_name(name) {
            selectors.push((sel, name));
        }
    }

    // DEX common
    let dex = [
        "swap",
        "multi_route_swap",
        "swap_exact_tokens_for_tokens",
        "add_liquidity",
        "remove_liquidity",
        "lock",
        "maybe_lock_and_then",
        "maybe_lock_and_then_with_native",
        "withdraw",
        "deposit",
    ];
    for name in dex {
        if let Ok(sel) = get_selector_from_name(name) {
            selectors.push((sel, name));
        }
    }

    // Lending common
    let lending = ["modify_position", "flash_loan", "supply", "borrow", "repay"];
    for name in lending {
        if let Ok(sel) = get_selector_from_name(name) {
            selectors.push((sel, name));
        }
    }

    // Known event selectors from memory (hardcoded hex)
    let known_hex = [
        (
            "0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
            "Transfer",
        ),
        (
            "0x0134692b230b9e1ffa39098904722134159652b09c5bc41d88d6698779d228ff",
            "Approval",
        ),
        (
            "0x01dcde06aabdbca2f80aa51392b345d7549d7757aa855f7e37f5d335ac8243b1",
            "TransactionExecuted",
        ),
        (
            "0x026b160f10156dea0639bec90696772c640b9706a47f5b8c52ea1abe5858b34d",
            "ContractDeployed",
        ),
        (
            "0x0157717768aca88da4ac4279765f09f4d0151823d573537fbbeb950cdbd9a870",
            "Swapped",
        ),
        (
            "0x03a7adca3546c213ce791fabf3b04090c163e419c808c9830fb343a4a395946e",
            "PositionUpdated",
        ),
        (
            "0x00e316f0d9d2a3affa97de1d99bb2aac0538e2666d0d8545545ead241ef0ccab",
            "Swap",
        ),
    ];
    for (hex, name) in known_hex {
        if let Ok(felt) = Felt::from_hex(hex) {
            selectors.push((felt, name));
        }
    }

    selectors
}

#[tokio::test]
#[ignore]
async fn populate_selectors() {
    let db = cache_db();
    let p = provider();

    // Step 1: Insert hardcoded selectors
    println!("\n=== Inserting hardcoded selectors ===");
    let mut inserted = 0usize;
    for (sel, name) in hardcoded_selectors() {
        if insert_selector(&db, sel, name) {
            inserted += 1;
        }
    }
    println!("Inserted {} hardcoded selectors", inserted);

    // Step 2: Collect all known addresses
    let addresses = collect_all_addresses();
    println!("\n=== Processing {} known addresses ===", addresses.len());

    // Step 3: Deduplicate by class_hash (many addresses share the same class)
    let mut class_hashes: Vec<(Felt, String)> = Vec::new();
    let mut seen_classes = HashSet::new();
    let mut failed = 0usize;

    // Batch fetch class_hashes (chunks of 20)
    for chunk in addresses.chunks(20) {
        let futs: Vec<_> = chunk
            .iter()
            .map(|(addr, name)| {
                let p = Arc::clone(&p);
                let addr = *addr;
                let name = name.clone();
                async move {
                    let result = p
                        .get_class_hash_at(BlockId::Tag(BlockTag::Latest), addr)
                        .await;
                    (addr, name, result)
                }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        for (addr, name, result) in results {
            match result {
                Ok(class_hash) => {
                    if seen_classes.insert(class_hash) {
                        class_hashes.push((class_hash, name));
                    }
                }
                Err(e) => {
                    println!("  SKIP {}: {} ({})", name, format!("{:#x}", addr), e);
                    failed += 1;
                }
            }
        }
    }
    println!(
        "Found {} unique class hashes ({} addresses failed)",
        class_hashes.len(),
        failed
    );

    // Step 4: Fetch and parse ABIs for each unique class
    println!("\n=== Fetching ABIs for {} classes ===", class_hashes.len());
    let total_new_selectors = AtomicUsize::new(0);

    for chunk in class_hashes.chunks(10) {
        let futs: Vec<_> = chunk
            .iter()
            .map(|(class_hash, name)| {
                let p = Arc::clone(&p);
                let ch = *class_hash;
                let name = name.clone();
                async move {
                    let result = p.get_class(BlockId::Tag(BlockTag::Latest), ch).await;
                    (ch, name, result)
                }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        for (class_hash, name, result) in results {
            match result {
                Ok(class) => {
                    let abi = snbeat::decode::abi::parse_contract_class(&class);
                    let mut count = 0;
                    for (key, func) in &abi.functions {
                        if insert_selector(&db, key.0, &func.name) {
                            count += 1;
                        }
                    }
                    for (key, event) in &abi.events {
                        if insert_selector(&db, key.0, &event.name) {
                            count += 1;
                        }
                    }
                    // Also cache the parsed ABI
                    insert_abi(&db, class_hash, &abi);

                    total_new_selectors.fetch_add(count, Ordering::Relaxed);
                    println!(
                        "  {} ({:#x}): {} fns, {} events, {} new selectors",
                        name,
                        class_hash,
                        abi.functions.len(),
                        abi.events.len(),
                        count,
                    );
                }
                Err(e) => {
                    println!("  FAIL {} ({:#x}): {}", name, class_hash, e);
                }
            }
        }
    }

    // Summary
    let total_selectors: i64 = db
        .query_row("SELECT COUNT(*) FROM selector_names", [], |row| row.get(0))
        .unwrap_or(0);
    let total_abis: i64 = db
        .query_row("SELECT COUNT(*) FROM parsed_abis", [], |row| row.get(0))
        .unwrap_or(0);

    println!("\n=== Done ===");
    println!(
        "New selectors added: {}",
        total_new_selectors.load(Ordering::Relaxed)
    );
    println!("Total selectors in DB: {}", total_selectors);
    println!("Total cached ABIs: {}", total_abis);
}
