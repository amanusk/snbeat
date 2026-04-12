use clap::Parser;

#[derive(Parser, Clone)]
#[command(name = "snbeat")]
#[command(about = "TUI Starknet block explorer")]
pub struct AppConfig {
    /// Starknet RPC endpoint
    #[arg(long, env = "APP_RPC_URL")]
    pub rpc_url: String,

    /// WebSocket endpoint for new block subscriptions
    #[arg(long, env = "APP_WS_URL")]
    pub ws_url: Option<String>,

    /// Path to pathfinder SQLite database (alternative to RPC)
    #[arg(long, env = "APP_PATHFINDER_DB")]
    pub pathfinder_db: Option<String>,

    /// Path to user labels TOML file
    #[arg(long, env = "APP_USER_LABELS", default_value = "labels.toml")]
    pub user_labels: String,

    /// Path to known addresses TOML file
    #[arg(long, env = "APP_KNOWN_ADDRESSES", default_value = "known.toml")]
    pub known_addresses: String,

    /// Voyager API key
    #[arg(long, env = "VOYAGER_API_KEY")]
    pub voyager_api_key: Option<String>,

    /// Dune API key
    #[arg(long, env = "DUNE_API_KEY")]
    pub dune_api_key: Option<String>,

    /// Pathfinder query service URL (e.g. http://steak:8234)
    #[arg(long, env = "APP_PATHFINDER_SERVICE_URL")]
    pub pathfinder_service_url: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, env = "APP_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Directory for log files (default: ~/.config/snbeat/logs)
    #[arg(long, env = "APP_LOG_DIR")]
    pub log_dir: Option<String>,
}
