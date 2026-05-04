#![allow(dead_code)]

mod app;
mod config;
mod data;
mod decode;
mod error;
mod network;
mod registry;
mod search;
mod ui;
mod utils;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use app::App;
use app::actions::Action;
use config::AppConfig;
use data::cache::CachingDataSource;
use data::rpc::RpcDataSource;
use decode::AbiRegistry;
use decode::class_cache::ClassCache;
use registry::AddressRegistry;
use search::SearchEngine;

/// Initialize file-based logging. Returns a guard that must be held alive
/// for the duration of the program (dropping it flushes pending writes).
fn init_logging(config: &AppConfig) -> tracing_appender::non_blocking::WorkerGuard {
    let log_dir = config
        .log_dir
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| snbeat_config_dir().join("logs"));

    std::fs::create_dir_all(&log_dir).expect("Failed to create log directory");

    let file_appender = tracing_appender::rolling::daily(&log_dir, "snbeat.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_ansi(false)
                .with_target(true)
                .with_thread_ids(false)
                .with_writer(non_blocking),
        )
        .init();

    guard
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env: try CWD first, then ~/.config/snbeat/.env
    if dotenvy::dotenv().is_err() {
        let global_env = snbeat_config_dir().join(".env");
        if global_env.exists() {
            dotenvy::from_path(&global_env).ok();
        }
    }

    let config = AppConfig::parse();
    let _log_guard = init_logging(&config);

    info!(rpc_url = %config.rpc_url, ws_url = ?config.ws_url, "snbeat starting");

    // Startup checks
    startup_checks(&config)?;

    // Setup cache directory
    let cache_dir = snbeat_config_dir();
    std::fs::create_dir_all(&cache_dir)?;
    let cache_db = cache_dir.join("cache.db");
    info!(cache_db = %cache_db.display(), "Using local cache");

    // Create data source with persistent cache
    let rpc = RpcDataSource::new(&config.rpc_url);
    let cached = CachingDataSource::new(Arc::new(rpc) as Arc<dyn data::DataSource>, &cache_db)?;
    let data_source: Arc<dyn data::DataSource> = Arc::new(cached);

    // ABI registry with persistent class cache
    let class_cache_db = rusqlite::Connection::open(cache_dir.join("cache.db"))
        .map_err(|e| anyhow::anyhow!("Failed to open class cache db: {e}"))?;
    // Ensure the parsed_abis table exists (same DB as data cache)
    class_cache_db
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS parsed_abis (
                class_hash TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );",
        )
        .map_err(|e| anyhow::anyhow!("Failed to init class cache: {e}"))?;
    let class_cache = ClassCache::new(class_cache_db, 500);
    let abi_registry = Arc::new(AbiRegistry::new(Arc::clone(&data_source), class_cache));

    // Address registry — resolve labels path with XDG fallback
    let user_labels_path = resolve_config_file(&config.user_labels, "labels.toml");
    let (registry_inner, labels_warning) =
        AddressRegistry::load(&user_labels_path).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to load address registry, using empty");
            AddressRegistry::load(std::path::Path::new("/dev/null")).unwrap()
        });
    let registry = Arc::new(registry_inner);
    let search_engine = Arc::new(SearchEngine::new(Arc::clone(&registry)));

    // Channels
    let (action_tx, action_rx) = mpsc::unbounded_channel::<Action>();
    let (response_tx, mut response_rx) = mpsc::unbounded_channel::<Action>();

    // Create app
    let mut app = App::new(action_tx.clone());
    app.search_engine = Some(Arc::clone(&search_engine));
    if let Some(w) = labels_warning {
        app.error_message = Some(w);
    }
    app.connection_status = app::state::ConnectionStatus::Connected {
        network: "mainnet".to_string(),
    };
    app.is_loading = true;

    // Create Dune client (optional — only if API key is set)
    let dune_client = config
        .dune_api_key
        .as_ref()
        .filter(|k| !k.is_empty())
        .map(|key| {
            info!(is_private = config.dune_private_queries, "Dune API enabled");
            Arc::new(network::dune::DuneClient::new(
                key.clone(),
                config.dune_private_queries,
            ))
        });

    // DefiLlama needs no API key, so the client is always created when the cache opens.
    let price_client: Option<Arc<network::prices::PriceClient>> =
        match network::prices::PriceClient::new(&cache_db) {
            Ok(client) => {
                info!("DefiLlama price client enabled");
                Some(Arc::new(client))
            }
            Err(e) => {
                warn!(error = %e, "Failed to initialize price client (USD prices disabled)");
                None
            }
        };
    app.price_client = price_client.clone();

    // Create Voyager client (optional — only if API key is set)
    let voyager_client: Option<Arc<network::voyager::VoyagerClient>> = config
        .voyager_api_key
        .as_ref()
        .filter(|k| !k.is_empty())
        .and_then(
            |key| match network::voyager::VoyagerClient::new(key.clone(), &cache_db) {
                Ok(client) => {
                    info!("Voyager API enabled");
                    Some(Arc::new(client))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to initialize Voyager client");
                    None
                }
            },
        );

    // Seed the search index with cached Voyager labels so they're searchable immediately
    if let Some(vc) = &voyager_client {
        let cached = vc.load_all_cached_labels();
        if !cached.is_empty() {
            info!(
                count = cached.len(),
                "Seeding search index with cached Voyager labels"
            );
            for (address, name) in cached {
                registry.add_voyager_label(address, &name);
            }
        }
    }

    // Create Pathfinder query service client (optional — only if URL is set)
    let pf_client = config
        .pathfinder_service_url
        .as_ref()
        .filter(|u| !u.is_empty())
        .map(|url| {
            info!(url = %url, "Pathfinder query service enabled");
            Arc::new(data::pathfinder::PathfinderClient::new(url.clone()))
        });

    // Record which data sources are configured
    app.data_sources = app::state::DataSources {
        rpc: app::state::SourceStatus::Configured,
        dune: if dune_client.is_some() {
            app::state::SourceStatus::Configured
        } else {
            app::state::SourceStatus::Off
        },
        pathfinder: if pf_client.is_some() {
            app::state::SourceStatus::Configured
        } else {
            app::state::SourceStatus::Off
        },
        voyager: if voyager_client.is_some() {
            app::state::SourceStatus::Configured
        } else {
            app::state::SourceStatus::Off
        },
        ws: app::state::SourceStatus::Off,
    };

    // Probe Voyager health in background, upgrade to Live if reachable
    if let Some(vc) = &voyager_client {
        let vc_c = Arc::clone(vc);
        let resp_tx_c = response_tx.clone();
        tokio::spawn(async move {
            match vc_c.health_check().await {
                Ok(()) => {
                    info!("Voyager API reachable");
                    let _ = resp_tx_c.send(app::actions::Action::SourceUpdate {
                        source: app::actions::Source::Voyager,
                        status: app::state::SourceStatus::Live,
                    });
                }
                Err(e) => {
                    warn!(error = %e, "Voyager API unreachable");
                    let _ = resp_tx_c.send(app::actions::Action::SourceUpdate {
                        source: app::actions::Source::Voyager,
                        status: app::state::SourceStatus::ConnectError(e),
                    });
                }
            }
        });
    }

    // Probe PF health in background, upgrade to Live if reachable
    if let Some(pf) = &pf_client {
        let pf_c = Arc::clone(pf);
        let resp_tx_c = response_tx.clone();
        tokio::spawn(async move {
            match pf_c.health().await {
                Ok(_) => {
                    let _ = resp_tx_c.send(app::actions::Action::SourceUpdate {
                        source: app::actions::Source::Pathfinder,
                        status: app::state::SourceStatus::Live,
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Pathfinder service unreachable");
                    let _ = resp_tx_c.send(app::actions::Action::SourceUpdate {
                        source: app::actions::Source::Pathfinder,
                        status: app::state::SourceStatus::ConnectError(e.to_string()),
                    });
                }
            }
        });
    }

    // Probe Dune health in background, upgrade to Live if reachable
    if let Some(dune) = &dune_client {
        let dune_c = Arc::clone(dune);
        let resp_tx_c = response_tx.clone();
        tokio::spawn(async move {
            match dune_c.health().await {
                Ok(()) => {
                    let _ = resp_tx_c.send(app::actions::Action::SourceUpdate {
                        source: app::actions::Source::Dune,
                        status: app::state::SourceStatus::Live,
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Dune API unreachable");
                    let _ = resp_tx_c.send(app::actions::Action::SourceUpdate {
                        source: app::actions::Source::Dune,
                        status: app::state::SourceStatus::ConnectError(e.to_string()),
                    });
                }
            }
        });
    }

    // Spawn network task
    let ds_clone = Arc::clone(&data_source);
    let abi_clone = Arc::clone(&abi_registry);
    let resp_tx_clone = response_tx.clone();
    tokio::spawn(async move {
        network::run_network_task(
            ds_clone,
            abi_clone,
            dune_client,
            pf_client,
            voyager_client,
            price_client,
            action_rx,
            resp_tx_clone,
        )
        .await;
    });

    // Spawn block update mechanism: prefer WebSocket, fall back to polling
    let _block_updater: tokio::task::JoinHandle<()> = if let Some(ws_url) = &config.ws_url {
        info!(ws_url = %ws_url, "Using WebSocket for new block headers and address streaming");
        app.data_sources.ws = app::state::SourceStatus::Configured;
        let (handle, ws_manager) = network::ws::spawn_ws_subscriber(
            ws_url.clone(),
            Arc::clone(&data_source),
            response_tx.clone(),
        );
        app.ws_manager = Some(ws_manager);
        handle
    } else {
        info!("No WS URL configured, using HTTP polling (3s interval)");
        network::spawn_block_poller(
            Arc::clone(&data_source),
            response_tx.clone(),
            Duration::from_secs(3),
        )
    };

    // Periodic address-view refresh ticker: every 60s, nudge the reducer to
    // re-fetch the currently viewed address from RPC if WS isn't `Live`.
    // The reducer checks view/source/context guards before dispatching work,
    // so this timer is a cheap unconditional heartbeat.
    let resp_tx_poll = response_tx.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        // Burn the immediate first tick so we don't fire at t=0 before any
        // address view exists.
        tick.tick().await;
        loop {
            tick.tick().await;
            if resp_tx_poll.send(Action::PeriodicAddressPollTick).is_err() {
                break;
            }
        }
    });

    // Request initial data
    info!("Requesting initial block fetch");
    let _ = action_tx.send(Action::FetchRecentBlocks { count: 30 });

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    info!("TUI initialized, entering event loop");

    // TUI event loop
    let result = run_loop(&mut terminal, &mut app, &mut response_rx).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    info!("snbeat exiting");
    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    response_rx: &mut mpsc::UnboundedReceiver<Action>,
) -> anyhow::Result<()> {
    // EventStream is cancellation-safe: dropping its `.next()` future does NOT
    // consume buffered events. The previous `spawn_blocking(event::read)` form
    // was racy — when `tokio::select!` picked the network branch, the blocking
    // task could finish reading a key but its result would be dropped, silently
    // losing characters mid-paste under heavy background traffic.
    use futures::StreamExt;
    let mut event_stream = EventStream::new();

    loop {
        // Draw
        terminal.draw(|f| ui::draw(f, app))?;

        // Check for quit
        if app.should_quit {
            return Ok(());
        }

        tokio::select! {
            maybe_event = event_stream.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) => {
                        // Clear error on any keypress
                        app.error_message = None;
                        if let Some(action) = app::input::handle_key(app, key) {
                            debug!(?action, "Dispatching action from input");
                            app.is_loading = true;
                            let _ = app.action_tx.send(action);
                        }
                    }
                    Some(Ok(Event::Mouse(mouse))) => match mouse.kind {
                        MouseEventKind::ScrollUp => app.select_previous(),
                        MouseEventKind::ScrollDown => app.select_next(),
                        _ => {} // ignore clicks/motion — text selection works via Shift+drag
                    },
                    Some(Ok(_)) => {} // resize, focus, paste — ignored
                    Some(Err(e)) => {
                        // Terminal input errors are usually fatal (closed tty,
                        // broken pipe). Log and quit cleanly so the select! arm
                        // doesn't spin on persistent errors.
                        warn!(error = %e, "Terminal event stream error, quitting");
                        return Ok(());
                    }
                    None => {
                        // Stream end (terminal closed). Quit.
                        info!("Terminal event stream ended");
                        return Ok(());
                    }
                }
            }
            // Handle responses from network task
            Some(action) = response_rx.recv() => {
                match &action {
                    Action::BlocksLoaded(blocks) => {
                        info!(count = blocks.len(), "Blocks loaded");
                    }
                    Action::NewBlock(block) => {
                        debug!(number = block.number, "New block received");
                    }
                    Action::BlockDetailLoaded { block, transactions, endpoint_names, .. } => {
                        let resolved = endpoint_names.iter().filter(|n| n.is_some()).count();
                        info!(block = block.number, tx_count = transactions.len(), resolved_endpoints = resolved, "Block detail loaded");
                    }
                    Action::TransactionLoaded { transaction, receipt, decoded_events, .. } => {
                        let decoded_count = decoded_events.iter().filter(|e| e.event_name.is_some()).count();
                        info!(tx = %format!("{:#x}", transaction.hash()), events = receipt.events.len(), decoded = decoded_count, "Transaction loaded");
                    }
                    Action::AddressInfoLoaded { info, decoded_events, tx_summaries, .. } => {
                        info!(address = %format!("{:#x}", info.address), events = decoded_events.len(), txs = tx_summaries.len(), balances = info.token_balances.len(), "Address info loaded");
                    }
                    Action::Error(msg) => {
                        warn!(error = %msg, "Network error");
                    }
                    _ => {}
                }
                app.handle_action(action);
            }
        }
    }
}

fn snbeat_config_dir() -> std::path::PathBuf {
    if let Some(config_dir) = dirs::config_dir() {
        config_dir.join("snbeat")
    } else {
        std::path::PathBuf::from(".snbeat")
    }
}

/// Resolve a config file path: if the user set a custom value, use it as-is.
/// Otherwise (default), try CWD first, then fall back to ~/.config/snbeat/.
fn resolve_config_file(configured: &str, default_name: &str) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(configured);
    // User explicitly set a path (not the default) — use as-is
    if configured != default_name {
        return path;
    }
    // Default name: prefer CWD, fall back to config dir
    if path.exists() {
        path
    } else {
        let global = snbeat_config_dir().join(default_name);
        if global.exists() { global } else { path }
    }
}

/// Validate prerequisites before starting the app.
fn startup_checks(config: &AppConfig) -> anyhow::Result<()> {
    let mut warnings = Vec::new();

    // Check RPC URL is set and looks valid
    if config.rpc_url.is_empty() {
        anyhow::bail!(
            "APP_RPC_URL is required. Set it in .env, ~/.config/snbeat/.env, or pass --rpc-url"
        );
    }
    if !config.rpc_url.starts_with("http://") && !config.rpc_url.starts_with("https://") {
        anyhow::bail!(
            "APP_RPC_URL must start with http:// or https://. Got: {}",
            config.rpc_url
        );
    }

    // Validate WS URL if provided
    if let Some(ws_url) = &config.ws_url
        && !ws_url.starts_with("ws://")
        && !ws_url.starts_with("wss://")
    {
        anyhow::bail!(
            "APP_WS_URL must start with ws:// or wss://. Got: {}",
            ws_url
        );
    }

    // Check config directory is writable
    let config_dir = snbeat_config_dir();
    if let Err(e) = std::fs::create_dir_all(&config_dir) {
        anyhow::bail!(
            "Cannot create config directory {}: {}",
            config_dir.display(),
            e
        );
    }

    // Check SQLite works (bundled, should always work)
    let test_db_path = config_dir.join(".startup_check.db");
    match rusqlite::Connection::open(&test_db_path) {
        Ok(db) => {
            let _ = db.execute_batch("CREATE TABLE IF NOT EXISTS _test (id INTEGER)");
            drop(db);
            let _ = std::fs::remove_file(&test_db_path);
        }
        Err(e) => {
            anyhow::bail!(
                "SQLite check failed: {}. Cache directory: {}",
                e,
                config_dir.display()
            );
        }
    }

    // Check optional API keys
    if config.dune_api_key.as_ref().is_none_or(|k| k.is_empty()) {
        warnings.push(
            "DUNE_API_KEY not set — account history and contract call discovery will be limited",
        );
    }
    if config.voyager_api_key.as_ref().is_none_or(|k| k.is_empty()) {
        warnings.push("VOYAGER_API_KEY not set — address metadata enrichment unavailable");
    }

    // Check labels file (with XDG fallback)
    let labels_path = resolve_config_file(&config.user_labels, "labels.toml");
    if !labels_path.exists() {
        warnings
            .push("User labels file not found — create labels.toml in CWD or ~/.config/snbeat/");
    }

    // Log warnings
    for w in &warnings {
        info!("Startup: {}", w);
    }

    info!(
        rpc = %config.rpc_url,
        cache_dir = %config_dir.display(),
        dune = config.dune_api_key.as_ref().is_some_and(|k| !k.is_empty()),
        voyager = config.voyager_api_key.as_ref().is_some_and(|k| !k.is_empty()),
        labels = labels_path.exists(),
        "Startup checks passed"
    );

    Ok(())
}
