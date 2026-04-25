//! Benchmark: SQLite cache contention under concurrent reader + writer load.
//!
//! Purpose — capture baseline p50/p95/p99 latency for the read methods hit on
//! an address-view navigation while a firehose of WS-like writes streams into
//! the same `cache.db`. Runs against the current `Mutex<Connection>`
//! implementation so we have numbers to compare against a future
//! `r2d2::Pool<SqliteConnectionManager>` migration.
//!
//! Ignored by default; takes ~30s. Run with:
//!
//!   cargo test --release --test cache_pool_bench_test -- --ignored --nocapture
//!
//! The benchmark is self-contained (tempdir + synthetic data + NullUpstream)
//! so it needs no network or credentials.
//!
//! What it measures, per read method, under two scenarios:
//!   1. READ-ONLY: no writer running (pure contention-free baseline)
//!   2. UNDER-LOAD: writer task runs every 500ms and performs the same
//!      small-table writes that `writer_tick` issues in this benchmark:
//!      save_activity_range_with_count, save_cached_nonce, and
//!      save_search_progress — plus a concurrent ClassCache.put_selector_name
//!      burst every 1s simulating an ABI-decode flurry.
//!
//! Reader methods measured (= what `fetch_and_send_address_info` hits):
//!   - load_cached_nonce
//!   - load_cached_deploy_info
//!   - load_cached_activity_range_with_count
//!   - load_cached_activity_range_any_age
//!   - load_activity_total
//!   - load_cached_address_txs
//!   - load_cached_address_calls
//!   - load_cached_meta_txs
//!   - load_address_events
//!   - load_search_progress

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use starknet::core::types::{ContractClass, Felt};
use tokio::sync::Notify;

use snbeat::data::cache::CachingDataSource;
use snbeat::data::types::{
    AddressTxSummary, ContractCallSummary, MetaTxIntenderSummary, SnBlock, SnEvent, SnReceipt,
    SnTransaction,
};
// SnBlock only used by NullUpstream signature; keep the import.
use snbeat::data::{DataSource, FilterKind};
use snbeat::decode::class_cache::ClassCache;
use snbeat::error::Result;

// ---------------------------------------------------------------------------
// NullUpstream — the cache never falls through because the benchmark only
// exercises the cache-local sync methods. Mirrors the stub in cache.rs tests.
// ---------------------------------------------------------------------------

struct NullUpstream;

#[async_trait]
impl DataSource for NullUpstream {
    async fn get_latest_block_number(&self) -> Result<u64> {
        unimplemented!()
    }
    async fn get_block(&self, _number: u64) -> Result<SnBlock> {
        unimplemented!()
    }
    async fn get_block_by_hash(&self, _hash: Felt) -> Result<u64> {
        unimplemented!()
    }
    async fn get_block_with_txs(&self, _number: u64) -> Result<(SnBlock, Vec<SnTransaction>)> {
        unimplemented!()
    }
    async fn get_transaction(&self, _hash: Felt) -> Result<SnTransaction> {
        unimplemented!()
    }
    async fn get_receipt(&self, _hash: Felt) -> Result<SnReceipt> {
        unimplemented!()
    }
    async fn get_nonce(&self, _address: Felt) -> Result<Felt> {
        unimplemented!()
    }
    async fn get_class_hash(&self, _address: Felt) -> Result<Felt> {
        unimplemented!()
    }
    async fn get_class(&self, _class_hash: Felt) -> Result<ContractClass> {
        unimplemented!()
    }
    async fn get_recent_blocks(&self, _count: usize) -> Result<Vec<SnBlock>> {
        unimplemented!()
    }
    async fn get_events_for_address(
        &self,
        _address: Felt,
        _from_block: Option<u64>,
        _to_block: Option<u64>,
        _limit: usize,
    ) -> Result<Vec<SnEvent>> {
        unimplemented!()
    }
    async fn call_contract(
        &self,
        _contract_address: Felt,
        _selector: Felt,
        _calldata: Vec<Felt>,
    ) -> Result<Vec<Felt>> {
        unimplemented!()
    }
}

// ---------------------------------------------------------------------------
// Synthetic data builders. Kept small & cheap — the SQL shape is what matters.
// ---------------------------------------------------------------------------

fn felt(n: u64) -> Felt {
    Felt::from(n)
}

fn mk_event(addr: Felt, block: u64, idx: u64) -> SnEvent {
    SnEvent {
        from_address: addr,
        keys: vec![felt(0x1001), felt(idx)],
        data: vec![felt(1), felt(2), felt(3)],
        transaction_hash: felt(0x7000_0000 + idx),
        block_number: block,
        event_index: idx,
    }
}

fn mk_tx_summary(addr: Felt, idx: u64) -> AddressTxSummary {
    AddressTxSummary {
        hash: felt(0x8000_0000 + idx),
        nonce: idx,
        block_number: 1_000_000 + idx,
        timestamp: 1_700_000_000 + idx,
        endpoint_names: "transfer,approve".to_string(),
        total_fee_fri: 123_456_789,
        tip: 1000,
        tx_type: "INVOKE".to_string(),
        status: "OK".to_string(),
        sender: Some(addr),
        called_contracts: Vec::new(),
    }
}

fn mk_call_summary(idx: u64) -> ContractCallSummary {
    ContractCallSummary {
        tx_hash: felt(0x9000_0000 + idx),
        sender: felt(0xBEEF_0000 + (idx % 256)),
        function_name: "swap_exact_in".to_string(),
        block_number: 1_000_000 + idx,
        timestamp: 1_700_000_000 + idx,
        total_fee_fri: 234_567,
        status: "OK".to_string(),
        nonce: Some(idx),
        tip: 0,
    }
}

fn mk_meta_tx(idx: u64) -> MetaTxIntenderSummary {
    MetaTxIntenderSummary {
        hash: felt(0xA000_0000 + idx),
        block_number: 1_000_000 + idx,
        tx_index: idx % 100,
        timestamp: 1_700_000_000 + idx,
        paymaster: felt(0xFACE),
        version: "v2".to_string(),
        oe_nonce: felt(idx),
        total_fee_fri: 345_678,
        status: "OK".to_string(),
        inner_targets: vec![felt(0xAAA1), felt(0xAAA2)],
        inner_endpoints: "approve, swap_exact_in".to_string(),
        caller: felt(0),
    }
}

// ---------------------------------------------------------------------------
// Seed the cache with "already large" state — what an address view would
// open with after a user has been watching the address for a while.
// ---------------------------------------------------------------------------

fn seed_cache(ds: &CachingDataSource, addr: Felt, events: usize, txs: usize, calls: usize) {
    let evts: Vec<SnEvent> = (0..events as u64)
        .map(|i| mk_event(addr, 1_000_000 + i, i))
        .collect();
    // merge_address_events is the public trait method that writes through.
    let _ = ds.merge_address_events(&addr, &evts);

    let tx_sums: Vec<AddressTxSummary> = (0..txs as u64).map(|i| mk_tx_summary(addr, i)).collect();
    ds.save_address_txs(&addr, &tx_sums);

    let call_sums: Vec<ContractCallSummary> = (0..calls as u64).map(mk_call_summary).collect();
    ds.save_address_calls(&addr, &call_sums);

    let meta_sums: Vec<MetaTxIntenderSummary> = (0..(txs / 2) as u64).map(mk_meta_tx).collect();
    ds.save_meta_txs(&addr, &meta_sums);

    ds.save_activity_range_with_count(&addr, 1_000_000, 1_000_000 + events as u64, events as u64);
    ds.save_activity_total(&addr, events as u64);
    ds.save_cached_nonce(&addr, &felt(txs as u64), 1_000_000 + events as u64);
    ds.save_deploy_info(
        &addr,
        &felt(0xDEAD_BEEF),
        1_000_000,
        Some(&felt(0xDEADDEAD)),
    );
    ds.save_search_progress(
        &addr,
        FilterKind::Unkeyed,
        1_000_000,
        1_000_000 + events as u64,
    );
    ds.save_search_progress(
        &addr,
        FilterKind::Keyed,
        1_000_000,
        1_000_000 + events as u64,
    );
}

// ---------------------------------------------------------------------------
// Runners
// ---------------------------------------------------------------------------

/// Single reader "burst" = one navigation's worth of sync cache reads.
/// Returns Vec<(name, micros)> for each method.
fn reader_burst(ds: &CachingDataSource, addr: &Felt) -> Vec<(&'static str, u128)> {
    let mut out = Vec::with_capacity(10);

    let t = Instant::now();
    let _ = ds.load_cached_nonce(addr);
    out.push(("load_cached_nonce", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_cached_deploy_info(addr);
    out.push(("load_cached_deploy_info", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_cached_activity_range_with_count(addr);
    out.push(("load_activity_range_w_count", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_cached_activity_range_any_age(addr);
    out.push(("load_activity_range_any_age", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_activity_total(addr);
    out.push(("load_activity_total", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_cached_address_txs(addr);
    out.push(("load_cached_address_txs", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_cached_address_calls(addr);
    out.push(("load_cached_address_calls", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_cached_meta_txs(addr);
    out.push(("load_cached_meta_txs", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_address_events(addr);
    out.push(("load_address_events", t.elapsed().as_micros()));

    let t = Instant::now();
    let _ = ds.load_search_progress(addr, FilterKind::Unkeyed);
    out.push(("load_search_progress", t.elapsed().as_micros()));

    out
}

/// Single writer "tick" = the small-table writes the WS / stream path
/// performs per block tip. Intentionally omits merge_address_events because
/// that path rewrites the entire events table (DELETE + N INSERTs, one
/// fsync per row) — calling it at any steady cadence saturates the single
/// mutex completely and makes the READER latency impossible to measure.
/// The contention from merges is ITSELF a key finding, but we measure it
/// separately (bench_merge_address_events_cost) rather than mixing it in.
fn writer_tick(ds: &CachingDataSource, addr: &Felt, tick: u64) {
    let block_num = 2_000_000 + tick;
    ds.save_activity_range_with_count(addr, 1_000_000, block_num, block_num - 1_000_000 + 1);
    ds.save_cached_nonce(addr, &felt(block_num), block_num);
    ds.save_search_progress(addr, FilterKind::Unkeyed, 1_000_000, block_num);
}

/// Selector-name write burst on a shared ClassCache. Simulates the
/// `index_abi_selectors` storm that happens when the decode loop resolves a
/// previously-unseen class ABI.
fn class_cache_burst(class_cache: &ClassCache, base: u64) {
    for i in 0..50u64 {
        class_cache.put_selector_name(felt(0xCAFE_0000 + base * 100 + i), format!("fn_{i}"));
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Stats {
    samples: Vec<u128>,
}

impl Stats {
    fn push(&mut self, v: u128) {
        self.samples.push(v);
    }

    fn summarize(&self, label: &str) {
        if self.samples.is_empty() {
            println!("  {label:<32} NO SAMPLES");
            return;
        }
        let mut s = self.samples.clone();
        s.sort_unstable();
        let n = s.len();
        let p50 = s[n / 2];
        let p95 = s[(n * 95 / 100).min(n - 1)];
        let p99 = s[(n * 99 / 100).min(n - 1)];
        let max = s[n - 1];
        let mean: u128 = s.iter().sum::<u128>() / n as u128;
        println!(
            "  {label:<32} n={n:>5} p50={p50:>6}us p95={p95:>7}us p99={p99:>7}us max={max:>7}us mean={mean:>6}us",
        );
    }
}

// ---------------------------------------------------------------------------
// The benchmark itself. Runs two phases, prints per-method stats for each.
// ---------------------------------------------------------------------------

async fn run_bench(label: &str, cache_path: &Path) {
    println!("\n===== {label}  ({}) =====", cache_path.display());

    let upstream: Arc<dyn DataSource> = Arc::new(NullUpstream);
    let ds = Arc::new(CachingDataSource::new(upstream, cache_path).expect("open cache"));

    // Shared ClassCache on the SAME file — models the real deployment. Apply
    // the same PRAGMAs the main cache pool uses so this connection doesn't
    // trip SQLITE_BUSY on every concurrent write and silently drop selector
    // rows (which would under-count contention on `put_selector_name`).
    let class_db = rusqlite::Connection::open(cache_path).expect("class cache open");
    class_db
        .execute_batch(
            "PRAGMA journal_mode = WAL; \
             PRAGMA synchronous = NORMAL; \
             PRAGMA busy_timeout = 5000;",
        )
        .expect("configure class cache connection");
    let class_cache = Arc::new(ClassCache::new(class_db, 500));

    let watched = felt(0x0_59AA_7EAF);
    // Volumes: scaled down from production (a heavy address can have 10k+
    // events) because the current non-transactional write path fsyncs per
    // row — at 10k events and one `save_address_events` call per block-tick,
    // each tick rewrites the whole table → quadratic fsync storm. Seeding
    // 10k alone takes minutes. That slowness IS the finding, but we only
    // need enough rows to make per-read cost measurable.
    let (n_events, n_txs, n_calls) = (500usize, 300usize, 300usize);
    println!("Seeding cache ({n_events} events, {n_txs} txs, {n_calls} calls)...");
    let t0 = Instant::now();
    // Seeding hammers SQLite synchronously; wrap in spawn_blocking so it
    // doesn't park a Tokio worker (and starve other runtime tasks) while
    // the bench is still setting up.
    {
        let ds_for_seed = Arc::clone(&ds);
        tokio::task::spawn_blocking(move || {
            seed_cache(&ds_for_seed, watched, n_events, n_txs, n_calls);
        })
        .await
        .expect("seed_cache task panicked");
    }
    println!("  seed took {} ms", t0.elapsed().as_millis());

    // ----------------- PHASE 1: pure-reader baseline ---------------------
    println!("\n[phase 1] pure reader (no writer)");
    let mut per_method: std::collections::BTreeMap<&'static str, Stats> = Default::default();
    let phase_dur = Duration::from_secs(10);
    let phase_start = Instant::now();
    while phase_start.elapsed() < phase_dur {
        let ds_c = Arc::clone(&ds);
        let addr = watched;
        let samples = tokio::task::spawn_blocking(move || reader_burst(&ds_c, &addr))
            .await
            .unwrap();
        for (name, us) in samples {
            per_method.entry(name).or_default().push(us);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    for (name, stats) in &per_method {
        stats.summarize(name);
    }

    // ----------------- PHASE 2: reader under writer load -----------------
    println!("\n[phase 2] reader under write load (500ms writer bursts + ABI index storm)");
    let stop = Arc::new(Notify::new());

    // Writer task — every 500ms, small-table writes. These are the cheap
    // single-row writes the WS path does every block; they still take the
    // mutex + fsync, so they expose baseline reader-vs-writer contention.
    let writer_ds = Arc::clone(&ds);
    let writer_stop = Arc::clone(&stop);
    let writer_addr = watched;
    let writer = tokio::spawn(async move {
        let mut tick = 0u64;
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = writer_stop.notified() => break,
                _ = interval.tick() => {
                    let ds_c = Arc::clone(&writer_ds);
                    let addr = writer_addr;
                    let t = tick;
                    // Surface JoinError (panic/cancel) so the bench fails
                    // loudly instead of silently dropping write load.
                    tokio::task::spawn_blocking(move || {
                        writer_tick(&ds_c, &addr, t);
                    }).await.expect("writer_tick task panicked");
                    tick += 1;
                }
            }
        }
        tick
    });

    // ABI indexer task — every 1s burst 50 selector_name writes.
    let cc = Arc::clone(&class_cache);
    let cc_stop = Arc::clone(&stop);
    let indexer = tokio::spawn(async move {
        let mut base = 0u64;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = cc_stop.notified() => break,
                _ = interval.tick() => {
                    let cc_c = Arc::clone(&cc);
                    let b = base;
                    // Surface JoinError so a panic in the indexer can't
                    // silently reduce the contention the bench is measuring.
                    tokio::task::spawn_blocking(move || {
                        class_cache_burst(&cc_c, b);
                    }).await.expect("class_cache_burst task panicked");
                    base += 1;
                }
            }
        }
    });

    let mut per_method_load: std::collections::BTreeMap<&'static str, Stats> = Default::default();
    let phase_dur = Duration::from_secs(15);
    let phase_start = Instant::now();
    while phase_start.elapsed() < phase_dur {
        let ds_c = Arc::clone(&ds);
        let addr = watched;
        let samples = tokio::task::spawn_blocking(move || reader_burst(&ds_c, &addr))
            .await
            .unwrap();
        for (name, us) in samples {
            per_method_load.entry(name).or_default().push(us);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    stop.notify_waiters();
    let writer_ticks = writer.await.unwrap_or(0);
    let _ = indexer.await;

    println!(
        "  writer completed {writer_ticks} ticks over {} s",
        phase_dur.as_secs()
    );
    for (name, stats) in &per_method_load {
        stats.summarize(name);
    }

    // Side-by-side delta for quick diffing.
    println!("\n[delta] under-load vs pure-reader (p95)");
    for (name, under) in &per_method_load {
        if let Some(pure) = per_method.get(name) {
            let pure_p95 = percentile(&pure.samples, 95);
            let under_p95 = percentile(&under.samples, 95);
            let ratio = if pure_p95 == 0 {
                0.0
            } else {
                under_p95 as f64 / pure_p95 as f64
            };
            println!(
                "  {name:<32} pure={pure_p95:>6}us  load={under_p95:>7}us  x{:.2}",
                ratio
            );
        }
    }
}

fn percentile(samples: &[u128], p: usize) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    let mut s = samples.to_vec();
    s.sort_unstable();
    s[(s.len() * p / 100).min(s.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "long-running benchmark; run with --release --ignored --nocapture"]
async fn bench_cache_contention_baseline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache_path = dir.path().join("cache.db");
    run_bench("Mutex<Connection> baseline", &cache_path).await;
    // Keep tempdir alive to the end.
    drop(dir);
}

/// Standalone write-path bench: how long does a single call take to the
/// heavy list-rewrite methods? These matter because they're what freezes
/// the UI during WS ticks on a busy address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-running benchmark; run with --release --ignored --nocapture"]
async fn bench_write_path_costs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache_path = dir.path().join("cache.db");

    let upstream: Arc<dyn DataSource> = Arc::new(NullUpstream);
    let ds = Arc::new(CachingDataSource::new(upstream, &cache_path).expect("open cache"));
    let addr = felt(0x0_59AA_7EAF);

    println!("\n===== Write-path costs (cold db) =====");
    for &n in &[100usize, 500, 1000, 2000] {
        let events: Vec<SnEvent> = (0..n as u64)
            .map(|i| mk_event(addr, 1_000_000 + i, i))
            .collect();
        let ds_c = Arc::clone(&ds);
        let evts = events.clone();
        let a = addr;
        let t0 = Instant::now();
        tokio::task::spawn_blocking(move || ds_c.save_address_txs(&a, &[]))
            .await
            .unwrap();
        let _ = t0; // warm
        let ds_c = Arc::clone(&ds);
        let a = addr;
        let t0 = Instant::now();
        tokio::task::spawn_blocking(move || {
            // Path of interest: full merge + rewrite.
            let _ = ds_c.merge_address_events(&a, &evts);
        })
        .await
        .unwrap();
        let ms = t0.elapsed().as_millis();
        println!("  merge_address_events n={n:>5}  {ms:>6} ms");
    }

    drop(dir);
}
