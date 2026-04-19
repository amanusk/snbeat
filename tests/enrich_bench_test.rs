//! Benchmark: compare old RPC-only enrichment path vs. new pf-query-primary
//! path. Requires both `APP_RPC_URL` and `APP_PATHFINDER_SERVICE_URL`. Ignored
//! by default.
//!
//! Run with:
//!   APP_RPC_URL=... APP_PATHFINDER_SERVICE_URL=... cargo test --test
//!     enrich_bench_test -- --ignored --nocapture

use std::sync::Arc;
use std::time::Instant;

use starknet::core::types::Felt;

use snbeat::data::DataSource;
use snbeat::data::pathfinder::PathfinderClient;
use snbeat::data::rpc::RpcDataSource;
use snbeat::data::types::{AddressTxSummary, SnTransaction};
use snbeat::decode::AbiRegistry;
use snbeat::decode::class_cache::ClassCache;
use snbeat::decode::functions::parse_multicall;
use snbeat::network::helpers;

fn rpc_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("APP_RPC_URL").expect("APP_RPC_URL must be set")
}

fn pf_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("APP_PATHFINDER_SERVICE_URL")
        .expect("APP_PATHFINDER_SERVICE_URL must be set for benchmarks")
}

fn new_abi_registry(ds: Arc<dyn DataSource>) -> Arc<AbiRegistry> {
    // Fresh in-memory SQLite — no persistence across runs so both paths start
    // equally cold on class ABIs.
    let db = rusqlite::Connection::open_in_memory().expect("in-memory sqlite");
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS parsed_abis (
            class_hash TEXT PRIMARY KEY,
            data TEXT NOT NULL
        );",
    )
    .expect("init parsed_abis table");
    let class_cache = ClassCache::new(db, 500);
    Arc::new(AbiRegistry::new(ds, class_cache))
}

/// Pull ~N invoke tx hashes from a recent block (skipping a few blocks to
/// avoid the pre-confirmed / latest boundary).
async fn sample_invoke_hashes(ds: &Arc<dyn DataSource>, n: usize) -> Vec<Felt> {
    let latest = ds.get_latest_block_number().await.expect("latest block");
    let mut hashes: Vec<Felt> = Vec::new();
    let mut blk = latest.saturating_sub(5);
    while hashes.len() < n && blk > 0 {
        if let Ok((_, txs)) = ds.get_block_with_txs(blk).await {
            for t in &txs {
                if matches!(t, SnTransaction::Invoke(_)) {
                    hashes.push(t.hash());
                    if hashes.len() >= n {
                        break;
                    }
                }
            }
        }
        blk = blk.saturating_sub(1);
    }
    hashes
}

/// Simulate the old enrichment path: per-hash parallel get_transaction +
/// get_receipt via RPC, then build_tx_summary. Pre-warms ABIs for target
/// addresses exactly the way `enrich_address_txs` did before this change.
async fn enrich_rpc_path(
    hashes: &[Felt],
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
) -> Vec<AddressTxSummary> {
    let futs: Vec<_> = hashes
        .iter()
        .map(|h| {
            let ds_t = Arc::clone(ds);
            let ds_r = Arc::clone(ds);
            let h = *h;
            async move { (h, ds_t.get_transaction(h).await, ds_r.get_receipt(h).await) }
        })
        .collect();
    let results = futures::future::join_all(futs).await;

    let mut target_addresses = std::collections::HashSet::new();
    for (_h, tx_r, _rx_r) in &results {
        if let Ok(SnTransaction::Invoke(invoke)) = tx_r {
            for call in parse_multicall(&invoke.calldata) {
                target_addresses.insert(call.contract_address);
            }
        }
    }
    for addr in target_addresses {
        let _ = abi_reg.get_abi_for_address(&addr).await;
    }

    let mut out = Vec::new();
    for (hash, tx_r, rx_r) in results {
        if let Ok(tx) = tx_r {
            let receipt = rx_r.ok();
            let block_num = receipt.as_ref().map(|r| r.block_number).unwrap_or(0);
            out.push(helpers::build_tx_summary(
                hash,
                &tx,
                receipt.as_ref(),
                block_num,
                0,
                abi_reg,
            ));
        }
    }
    helpers::backfill_timestamps(&mut out, ds, None).await;
    out
}

/// Simulate the new enrichment path: one `/txs-by-hash` round trip, pre-warm
/// ABIs, then `build_tx_summary_from_pf_data`.
async fn enrich_pf_path(
    hashes: &[Felt],
    ds: &Arc<dyn DataSource>,
    pf: &Arc<PathfinderClient>,
    abi_reg: &Arc<AbiRegistry>,
) -> Vec<AddressTxSummary> {
    let pf_rows = pf
        .get_txs_by_hash(hashes)
        .await
        .expect("pf get_txs_by_hash");

    let mut target_addresses = std::collections::HashSet::new();
    for row in &pf_rows {
        for addr in helpers::pf_tx_target_addresses(row) {
            target_addresses.insert(addr);
        }
    }
    for addr in target_addresses {
        let _ = abi_reg.get_abi_for_address(&addr).await;
    }

    let mut out = Vec::new();
    for row in &pf_rows {
        if let Some(s) = helpers::build_tx_summary_from_pf_data(row, abi_reg) {
            out.push(s);
        }
    }
    helpers::backfill_timestamps(&mut out, ds, Some(pf)).await;
    out
}

fn compare_summaries(
    rpc_summaries: &[AddressTxSummary],
    pf_summaries: &[AddressTxSummary],
) -> Vec<String> {
    let by_hash_pf: std::collections::HashMap<Felt, &AddressTxSummary> =
        pf_summaries.iter().map(|s| (s.hash, s)).collect();

    let mut mismatches = Vec::new();
    for rpc_s in rpc_summaries {
        let Some(pf_s) = by_hash_pf.get(&rpc_s.hash) else {
            continue;
        };
        let h = rpc_s.hash;
        if rpc_s.tx_type != pf_s.tx_type {
            mismatches.push(format!(
                "{h:#x}: tx_type rpc={} pf={}",
                rpc_s.tx_type, pf_s.tx_type
            ));
        }
        if rpc_s.nonce != pf_s.nonce {
            mismatches.push(format!(
                "{h:#x}: nonce rpc={} pf={}",
                rpc_s.nonce, pf_s.nonce
            ));
        }
        if rpc_s.total_fee_fri != pf_s.total_fee_fri {
            mismatches.push(format!(
                "{h:#x}: fee rpc={} pf={}",
                rpc_s.total_fee_fri, pf_s.total_fee_fri
            ));
        }
        if rpc_s.status != pf_s.status {
            mismatches.push(format!(
                "{h:#x}: status rpc={} pf={}",
                rpc_s.status, pf_s.status
            ));
        }
        if rpc_s.sender != pf_s.sender {
            mismatches.push(format!(
                "{h:#x}: sender rpc={:?} pf={:?}",
                rpc_s.sender, pf_s.sender
            ));
        }
        if rpc_s.block_number != pf_s.block_number {
            mismatches.push(format!(
                "{h:#x}: block_number rpc={} pf={}",
                rpc_s.block_number, pf_s.block_number
            ));
        }
        if rpc_s.endpoint_names != pf_s.endpoint_names {
            mismatches.push(format!(
                "{h:#x}: endpoint_names rpc={:?} pf={:?}",
                rpc_s.endpoint_names, pf_s.endpoint_names
            ));
        }
        if rpc_s.timestamp != pf_s.timestamp && pf_s.timestamp != 0 && rpc_s.timestamp != 0 {
            mismatches.push(format!(
                "{h:#x}: timestamp rpc={} pf={}",
                rpc_s.timestamp, pf_s.timestamp
            ));
        }
    }
    mismatches
}

/// Benchmark + correctness check. Runs enrichment at a few different N and
/// compares RPC-only vs. pf-primary paths side by side.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires APP_RPC_URL and APP_PATHFINDER_SERVICE_URL"]
async fn bench_rpc_vs_pf_enrichment() {
    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc_url()));
    let pf = Arc::new(PathfinderClient::new(pf_url()));

    for &n in &[50usize, 200, 500] {
        let hashes = sample_invoke_hashes(&ds, n).await;
        if hashes.len() < n / 2 {
            println!("skipping N={n}: only sampled {}", hashes.len());
            continue;
        }
        println!("\n----- Benchmark N={} -----", hashes.len());

        // Warm ABIs once via RPC path so both runs start with warm class
        // cache — isolates the tx+receipt cost from ABI fetching.
        let warm_abi = new_abi_registry(Arc::clone(&ds));
        let _ = enrich_rpc_path(&hashes, &ds, &warm_abi).await;

        let t0 = Instant::now();
        let rpc_summaries = enrich_rpc_path(&hashes, &ds, &warm_abi).await;
        let rpc_ms = t0.elapsed().as_millis();

        let t0 = Instant::now();
        let pf_summaries = enrich_pf_path(&hashes, &ds, &pf, &warm_abi).await;
        let pf_ms = t0.elapsed().as_millis();

        println!(
            "  warm-ABI:  RPC {rpc_ms:>5} ms ({} txs)  |  PF {pf_ms:>5} ms ({} txs)  |  speedup {:.2}x",
            rpc_summaries.len(),
            pf_summaries.len(),
            rpc_ms as f64 / pf_ms.max(1) as f64
        );

        let cold_rpc_abi = new_abi_registry(Arc::clone(&ds));
        let t0 = Instant::now();
        let _ = enrich_rpc_path(&hashes, &ds, &cold_rpc_abi).await;
        let cold_rpc_ms = t0.elapsed().as_millis();

        let cold_pf_abi = new_abi_registry(Arc::clone(&ds));
        let t0 = Instant::now();
        let _ = enrich_pf_path(&hashes, &ds, &pf, &cold_pf_abi).await;
        let cold_pf_ms = t0.elapsed().as_millis();

        println!(
            "  cold-ABI:  RPC {cold_rpc_ms:>5} ms               |  PF {cold_pf_ms:>5} ms                |  speedup {:.2}x",
            cold_rpc_ms as f64 / cold_pf_ms.max(1) as f64
        );

        let mismatches = compare_summaries(&rpc_summaries, &pf_summaries);
        println!(
            "  correctness: rpc={} pf={} mismatches={}",
            rpc_summaries.len(),
            pf_summaries.len(),
            mismatches.len()
        );
        for m in mismatches.iter().take(10) {
            println!("    MISMATCH {m}");
        }
        assert!(
            mismatches.is_empty(),
            "field mismatches between RPC and PF paths at N={}",
            hashes.len()
        );
    }
}

/// Measure user-click latency while background enrichment is in-flight.
///
/// This models the bug the pf-query migration targets: a user click on a tx
/// was blocked behind the enrichment's RPC storm. With the pf path, the
/// enrichment hits Pathfinder instead of RPC, so user clicks against RPC
/// should stay snappy.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires APP_RPC_URL and APP_PATHFINDER_SERVICE_URL"]
async fn bench_click_latency_under_enrichment_load() {
    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc_url()));
    let pf = Arc::new(PathfinderClient::new(pf_url()));

    // Large enrichment batch — the pathological case.
    let enrich_hashes = sample_invoke_hashes(&ds, 400).await;
    // Smaller set of hashes to simulate user clicks; drawn from the same
    // pool so the server is asked for real data.
    let click_hashes: Vec<Felt> = enrich_hashes
        .iter()
        .step_by(enrich_hashes.len() / 10)
        .take(10)
        .copied()
        .collect();

    println!(
        "Enrichment batch: {} hashes, click samples: {}",
        enrich_hashes.len(),
        click_hashes.len()
    );

    for label in &["rpc-path", "pf-path"] {
        let abi = new_abi_registry(Arc::clone(&ds));
        // Warm ABIs so class fetches don't dominate.
        let _ = enrich_rpc_path(&enrich_hashes, &ds, &abi).await;

        // Spawn enrichment concurrently with click probes.
        let ds_bg = Arc::clone(&ds);
        let pf_bg = Arc::clone(&pf);
        let abi_bg = Arc::clone(&abi);
        let hashes_bg = enrich_hashes.clone();
        let path = *label;
        let enrich_start = Instant::now();
        let enrich_task = tokio::spawn(async move {
            match path {
                "rpc-path" => {
                    // Mimic the old `enrich_all_empty_endpoints` pattern:
                    // batches of 20 running sequentially, each batch 20-way
                    // parallel. That's what saturated the pool pre-fix.
                    for chunk in hashes_bg.chunks(20) {
                        let _ = enrich_rpc_path(chunk, &ds_bg, &abi_bg).await;
                    }
                }
                "pf-path" => {
                    let _ = enrich_pf_path(&hashes_bg, &ds_bg, &pf_bg, &abi_bg).await;
                }
                _ => unreachable!(),
            }
        });

        // Fire click probes at 50ms intervals while enrichment runs.
        let mut click_latencies: Vec<u128> = Vec::new();
        for h in &click_hashes {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let ds_c = Arc::clone(&ds);
            let h = *h;
            let t0 = Instant::now();
            let (tx_r, rx_r) = tokio::join!(ds_c.get_transaction(h), ds_c.get_receipt(h));
            let ms = t0.elapsed().as_millis();
            let _ = tx_r;
            let _ = rx_r;
            click_latencies.push(ms);
        }

        let _ = enrich_task.await;
        let enrich_ms = enrich_start.elapsed().as_millis();

        click_latencies.sort();
        let min = *click_latencies.first().unwrap();
        let p50 = click_latencies[click_latencies.len() / 2];
        let p90 = click_latencies[click_latencies.len() * 9 / 10];
        let max = *click_latencies.last().unwrap();
        let sum: u128 = click_latencies.iter().sum();
        let mean = sum / click_latencies.len() as u128;

        println!(
            "\n[{label}] enrichment={enrich_ms} ms  click latency (ms) min={min} p50={p50} p90={p90} max={max} mean={mean}"
        );
    }
}

// ---------------------------------------------------------------------------
// P0 baselines for pf-query parallel-decode work (issue #16).
// These benchmarks time the server endpoints end-to-end so we can record
// before/after numbers when the server-side spawn_blocking + rayon change
// lands. Run against the live pf-query host:
//
//   APP_RPC_URL=... APP_PATHFINDER_SERVICE_URL=... \
//     cargo test --release --test enrich_bench_test -- --ignored --nocapture \
//     bench_pf_sender_txs_heavy bench_pf_txs_by_hash_bulk
// ---------------------------------------------------------------------------

/// Proxy-metric: last 8 bytes of the Felt interpreted as u64. Nonces always
/// fit in u64 for real accounts, so this ranks them correctly.
fn felt_low_u64(f: Felt) -> u64 {
    let bytes = f.to_bytes_be();
    let mut low = [0u8; 8];
    low.copy_from_slice(&bytes[24..32]);
    u64::from_be_bytes(low)
}

/// Find an active invoke sender in recent blocks. "Active" = highest nonce
/// observed across N scanned blocks, which correlates with the size of the
/// account's nonce-update history and therefore with the server-side decode
/// workload that this benchmark exercises.
async fn sample_heavy_sender(ds: &Arc<dyn DataSource>, scan_blocks: u64) -> Option<Felt> {
    let latest = ds.get_latest_block_number().await.ok()?;
    let mut best: Option<(u64, Felt)> = None;
    for off in 5..(5 + scan_blocks) {
        let blk = latest.checked_sub(off)?;
        let Ok((_, txs)) = ds.get_block_with_txs(blk).await else {
            continue;
        };
        for t in &txs {
            if !matches!(t, SnTransaction::Invoke(_)) {
                continue;
            }
            let Some(nonce) = t.nonce() else { continue };
            let n = felt_low_u64(nonce);
            if best.map(|(b, _)| n > b).unwrap_or(true) {
                best = Some((n, t.sender()));
            }
        }
    }
    best.map(|(_, addr)| addr)
}

fn summarize(label: &str, mut samples: Vec<u128>, extra: &str) {
    samples.sort();
    let min = *samples.first().unwrap_or(&0);
    let p50 = samples[samples.len() / 2];
    let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
    let max = *samples.last().unwrap_or(&0);
    let mean: u128 = samples.iter().sum::<u128>() / samples.len() as u128;
    println!(
        "  {label:<28} runs={:>2} min={min:>5} p50={p50:>5} p95={p95:>5} max={max:>5} mean={mean:>5} ms  {extra}",
        samples.len()
    );
}

/// Time GET /sender-txs?limit=2000 on an active account. On the serial-decode
/// server, wall time scales with the number of distinct blocks in the
/// account's nonce history; after rayon+spawn_blocking, expect ~Ncores
/// speedup on the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires APP_RPC_URL and APP_PATHFINDER_SERVICE_URL"]
async fn bench_pf_sender_txs_heavy() {
    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc_url()));
    let pf = Arc::new(PathfinderClient::new(pf_url()));

    let addr = sample_heavy_sender(&ds, 30)
        .await
        .expect("no invoke sender found in recent blocks");
    println!("\n[bench_pf_sender_txs_heavy] sampled sender {:#x}", addr);

    for &limit in &[500u32, 2000] {
        // Warm-up run (discarded).
        let _ = pf.get_sender_txs(addr, limit).await;

        let mut samples = Vec::with_capacity(5);
        let mut rows_last = 0usize;
        for _ in 0..5 {
            let t0 = Instant::now();
            let rows = pf
                .get_sender_txs(addr, limit)
                .await
                .expect("pf get_sender_txs");
            samples.push(t0.elapsed().as_millis());
            rows_last = rows.len();
        }
        summarize(
            &format!("sender-txs limit={limit}"),
            samples,
            &format!("rows={rows_last}"),
        );
    }
}

/// Time cold-cache ABI resolution for a tx's event sources, comparing:
///   - OLD: serial `for event in events { get_abi_for_address(e.from_address).await }`
///   - NEW: `prewarm_abis(unique_sources)` (join_all in parallel), then serial
///     cache-hit awaits in the decode loop.
/// Picks a tx with many distinct event sources from recent blocks — the
/// worst-case for the serial pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires APP_RPC_URL"]
async fn bench_event_abi_prewarm() {
    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc_url()));

    // Scan recent blocks for invoke txs with many unique event sources.
    let latest = ds.get_latest_block_number().await.expect("latest block");
    let mut candidates: Vec<(Felt, Vec<Felt>)> = Vec::new(); // (tx_hash, unique_event_sources)
    'outer: for off in 5..60 {
        let blk = match latest.checked_sub(off) {
            Some(b) => b,
            None => break,
        };
        let Ok((_, txs)) = ds.get_block_with_txs(blk).await else {
            continue;
        };
        for t in txs.iter().filter(|t| matches!(t, SnTransaction::Invoke(_))) {
            let h = t.hash();
            let Ok(receipt) = ds.get_receipt(h).await else {
                continue;
            };
            let unique: std::collections::HashSet<Felt> =
                receipt.events.iter().map(|e| e.from_address).collect();
            if unique.len() >= 5 {
                candidates.push((h, unique.into_iter().collect()));
                if candidates.len() >= 8 {
                    break 'outer;
                }
            }
        }
    }
    if candidates.is_empty() {
        println!("[bench_event_abi_prewarm] no suitable txs found");
        return;
    }
    println!(
        "\n[bench_event_abi_prewarm] found {} txs, unique-sources per tx: {:?}",
        candidates.len(),
        candidates.iter().map(|(_, s)| s.len()).collect::<Vec<_>>()
    );

    // OLD path: serial get_abi_for_address per event source, cold cache.
    let mut serial_samples = Vec::with_capacity(candidates.len());
    for (_, sources) in &candidates {
        let abi = new_abi_registry(Arc::clone(&ds));
        let t0 = Instant::now();
        for addr in sources {
            let _ = abi.get_abi_for_address(addr).await;
        }
        serial_samples.push(t0.elapsed().as_millis());
    }
    summarize(
        "serial per-event (cold)",
        serial_samples,
        &format!("txs={}", candidates.len()),
    );

    // NEW path: prewarm_abis in parallel, then serial cache-hit awaits.
    let mut prewarm_samples = Vec::with_capacity(candidates.len());
    for (_, sources) in &candidates {
        let abi = new_abi_registry(Arc::clone(&ds));
        let t0 = Instant::now();
        snbeat::network::helpers::prewarm_abis(sources.iter().copied(), &abi).await;
        // simulate the post-prewarm serial decode loop (cache hits).
        for addr in sources {
            let _ = abi.get_abi_for_address(addr).await;
        }
        prewarm_samples.push(t0.elapsed().as_millis());
    }
    summarize(
        "prewarm + serial (cold)",
        prewarm_samples,
        &format!("txs={}", candidates.len()),
    );
}

/// Time POST /txs-by-hash with N hashes sampled from recent blocks. Decode
/// cost scales with the number of distinct blocks in the request (the server
/// decodes each block blob once). After the parallel-decode change, expect
/// linear speedup in the number of distinct blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires APP_RPC_URL and APP_PATHFINDER_SERVICE_URL"]
async fn bench_pf_txs_by_hash_bulk() {
    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc_url()));
    let pf = Arc::new(PathfinderClient::new(pf_url()));

    for &n in &[100usize, 500, 2000] {
        let hashes = sample_invoke_hashes(&ds, n).await;
        if hashes.len() < n / 2 {
            println!(
                "[bench_pf_txs_by_hash_bulk] skipping N={n} (only sampled {})",
                hashes.len()
            );
            continue;
        }

        // Warm-up run (discarded).
        let _ = pf.get_txs_by_hash(&hashes).await;

        let mut samples = Vec::with_capacity(5);
        let mut rows_last = 0usize;
        for _ in 0..5 {
            let t0 = Instant::now();
            let rows = pf.get_txs_by_hash(&hashes).await.expect("pf txs-by-hash");
            samples.push(t0.elapsed().as_millis());
            rows_last = rows.len();
        }
        summarize(
            &format!("txs-by-hash N={n}"),
            samples,
            &format!("rows={rows_last}"),
        );
    }
}

/// Compare `provider.batch_requests([tx, receipt, ...])` in a single HTTP
/// round-trip vs the current `join_all` of individual per-hash calls.
/// Accept criterion (per plan P4): ≥ 1.3× faster at N=20, no p95 regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires APP_RPC_URL"]
async fn bench_rpc_batch_vs_join_all() {
    use starknet::core::types::requests::{
        GetTransactionByHashRequest, GetTransactionReceiptRequest,
    };
    use starknet::core::types::Felt;
    use starknet::providers::{
        JsonRpcClient, Provider, ProviderRequestData, jsonrpc::HttpTransport,
    };
    use url::Url;

    let ds: Arc<dyn DataSource> = Arc::new(RpcDataSource::new(&rpc_url()));
    let provider =
        Arc::new(JsonRpcClient::new(HttpTransport::new(Url::parse(&rpc_url()).unwrap())));

    for &n in &[20usize, 50, 100] {
        let hashes = sample_invoke_hashes(&ds, n).await;
        if hashes.len() < n {
            println!(
                "[bench_rpc_batch_vs_join_all] skipping N={n} (only sampled {})",
                hashes.len()
            );
            continue;
        }
        let hashes: Vec<Felt> = hashes.into_iter().take(n).collect();

        // --- Warm-up (discarded) ---
        let _ = {
            let futs: Vec<_> = hashes
                .iter()
                .map(|h| {
                    let p = Arc::clone(&provider);
                    let h = *h;
                    async move {
                        let tx = p.get_transaction_by_hash(h, None).await;
                        let rx = p.get_transaction_receipt(h).await;
                        (tx, rx)
                    }
                })
                .collect();
            futures::future::join_all(futs).await
        };
        let _ = {
            let mut reqs: Vec<ProviderRequestData> = Vec::with_capacity(n * 2);
            for h in &hashes {
                reqs.push(ProviderRequestData::GetTransactionByHash(
                    GetTransactionByHashRequest {
                        transaction_hash: *h,
                        response_flags: None,
                    },
                ));
                reqs.push(ProviderRequestData::GetTransactionReceipt(
                    GetTransactionReceiptRequest {
                        transaction_hash: *h,
                    },
                ));
            }
            provider.batch_requests(&reqs).await
        };

        // --- join_all path ---
        let mut join_samples = Vec::with_capacity(5);
        for _ in 0..5 {
            let t0 = Instant::now();
            let futs: Vec<_> = hashes
                .iter()
                .map(|h| {
                    let p = Arc::clone(&provider);
                    let h = *h;
                    async move {
                        let tx = p.get_transaction_by_hash(h, None).await;
                        let rx = p.get_transaction_receipt(h).await;
                        (tx, rx)
                    }
                })
                .collect();
            let results = futures::future::join_all(futs).await;
            let ok = results.iter().filter(|(t, r)| t.is_ok() && r.is_ok()).count();
            join_samples.push(t0.elapsed().as_millis());
            assert_eq!(ok, n, "join_all: some requests failed");
        }
        summarize(
            &format!("join_all N={n}"),
            join_samples.clone(),
            &format!("req_count={}", n * 2),
        );

        // --- batch_requests path ---
        let mut batch_samples = Vec::with_capacity(5);
        for _ in 0..5 {
            let mut reqs: Vec<ProviderRequestData> = Vec::with_capacity(n * 2);
            for h in &hashes {
                reqs.push(ProviderRequestData::GetTransactionByHash(
                    GetTransactionByHashRequest {
                        transaction_hash: *h,
                        response_flags: None,
                    },
                ));
                reqs.push(ProviderRequestData::GetTransactionReceipt(
                    GetTransactionReceiptRequest {
                        transaction_hash: *h,
                    },
                ));
            }
            let t0 = Instant::now();
            let resp = provider.batch_requests(&reqs).await;
            batch_samples.push(t0.elapsed().as_millis());
            let responses = resp.expect("batch_requests should succeed");
            assert_eq!(responses.len(), n * 2, "batch returned wrong count");
        }
        summarize(
            &format!("batch    N={n}"),
            batch_samples,
            &format!("req_count={}", n * 2),
        );
    }
}
