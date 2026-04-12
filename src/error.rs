use thiserror::Error;

#[derive(Error, Debug)]
pub enum SnbeatError {
    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Provider error: {0}")]
    Provider(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("ABI decode error: {0}")]
    Decode(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("{0}")]
    Other(String),
}

impl From<serde_json::Error> for SnbeatError {
    fn from(e: serde_json::Error) -> Self {
        SnbeatError::Serialization(e.to_string())
    }
}

impl From<reqwest::Error> for SnbeatError {
    fn from(e: reqwest::Error) -> Self {
        SnbeatError::Rpc(e.to_string())
    }
}

impl From<toml::de::Error> for SnbeatError {
    fn from(e: toml::de::Error) -> Self {
        SnbeatError::Config(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SnbeatError>;
