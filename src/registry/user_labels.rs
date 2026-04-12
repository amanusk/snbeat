use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::error::{Result, SnbeatError};

/// User-editable labels file. Simple name mappings for addresses the user cares about.
#[derive(Debug, Default, Deserialize)]
pub struct UserLabelsFile {
    #[serde(default)]
    pub addresses: HashMap<String, UserAddressEntry>,
    #[serde(default)]
    pub transactions: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum UserAddressEntry {
    /// Simple: just a name string
    Simple(String),
    /// Rich: name + optional tags
    Rich {
        name: String,
        #[serde(default)]
        tags: Vec<String>,
    },
}

impl UserAddressEntry {
    pub fn name(&self) -> &str {
        match self {
            UserAddressEntry::Simple(s) => s,
            UserAddressEntry::Rich { name, .. } => name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserLabel {
    pub address: Felt,
    pub name: String,
    pub tags: Vec<String>,
}

/// Load user labels from a TOML file. Returns (labels, optional_warning).
/// On missing file: empty labels, no warning.
/// On corrupt/malformed file: empty labels + warning string.
pub fn load_user_labels(path: &Path) -> Result<(Vec<UserLabel>, Option<String>)> {
    if !path.exists() {
        debug!(path = %path.display(), "User labels file not found, using empty");
        return Ok((Vec::new(), None));
    }

    let content = std::fs::read_to_string(path)
        .map_err(|e| SnbeatError::Config(format!("Failed to read labels file: {e}")))?;

    let file: UserLabelsFile = match toml::from_str(&content) {
        Ok(f) => f,
        Err(e) => {
            let msg =
                format!("labels.toml is corrupted (TOML parse error: {e}) — no user labels loaded");
            warn!("{}", msg);
            return Ok((Vec::new(), Some(msg)));
        }
    };

    let mut labels = Vec::new();
    for (hex, entry) in &file.addresses {
        let felt = match Felt::from_hex(hex) {
            Ok(f) => f,
            Err(e) => {
                warn!(address = hex, error = %e, "Invalid address in labels file, skipping");
                continue;
            }
        };
        let (name, tags) = match entry {
            UserAddressEntry::Simple(s) => (s.clone(), Vec::new()),
            UserAddressEntry::Rich { name, tags } => (name.clone(), tags.clone()),
        };
        labels.push(UserLabel {
            address: felt,
            name,
            tags,
        });
    }

    debug!(count = labels.len(), "Loaded user labels");
    Ok((labels, None))
}
