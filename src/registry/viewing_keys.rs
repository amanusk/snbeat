//! User-supplied Privacy Pool viewing keys.
//!
//! Loaded from a separate TOML file (default `viewing_keys.toml`) — kept
//! out of `labels.toml` because viewing keys are SECRETS: anyone holding
//! one can deanonymize all that user's privacy-pool activity. The labels
//! file is meant to be shareable; this one must not be.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::decode::privacy_crypto::types::SecretFelt;
use crate::error::{Result, SnbeatError};

#[derive(Debug, Default, Deserialize)]
pub struct ViewingKeysFile {
    /// `user_address (hex felt)` → `private viewing key (hex felt)`.
    #[serde(default)]
    pub keys: HashMap<String, String>,
}

/// A loaded viewing-key entry: the user address and their private viewing
/// key. The private key is stored as a `SecretFelt` so it zeroes on drop
/// and prints `[REDACTED]` in any accidental log.
#[derive(Debug, Clone)]
pub struct ViewingKey {
    pub user: Felt,
    pub private_key: SecretFelt,
}

/// Load viewing keys from a TOML file. Returns `(keys, optional_warning)`.
/// Missing file → empty list, no warning. Corrupt file → empty list +
/// warning surfaced to the user.
pub fn load_viewing_keys(path: &Path) -> Result<(Vec<ViewingKey>, Option<String>)> {
    if !path.exists() {
        debug!(path = %path.display(), "Viewing keys file not found, using empty");
        return Ok((Vec::new(), None));
    }

    if let Err(msg) = check_permissions(path) {
        warn!("{}", msg);
    }

    let content = std::fs::read_to_string(path)
        .map_err(|e| SnbeatError::Config(format!("Failed to read viewing keys file: {e}")))?;

    let file: ViewingKeysFile = match toml::from_str(&content) {
        Ok(f) => f,
        Err(e) => {
            let msg = format!(
                "viewing_keys.toml is corrupted (TOML parse error: {e}) — no viewing keys loaded"
            );
            warn!("{}", msg);
            return Ok((Vec::new(), Some(msg)));
        }
    };

    let mut keys = Vec::new();
    for (user_hex, key_hex) in &file.keys {
        let user = match Felt::from_hex(user_hex) {
            Ok(f) => f,
            Err(e) => {
                warn!(address = user_hex, error = %e, "Invalid user address in viewing keys, skipping");
                continue;
            }
        };
        let private_felt = match Felt::from_hex(key_hex) {
            Ok(f) => f,
            Err(e) => {
                // Don't echo `key_hex` — even an "invalid" key may be
                // close to a valid one and a typo we don't want in logs.
                warn!(address = user_hex, error = %e, "Invalid private viewing key, skipping");
                continue;
            }
        };
        keys.push(ViewingKey {
            user,
            private_key: SecretFelt::new(private_felt),
        });
    }

    debug!(count = keys.len(), "Loaded viewing keys");
    Ok((keys, None))
}

/// Warn if the file is world-readable. On non-unix this is a no-op.
#[cfg(unix)]
fn check_permissions(path: &Path) -> std::result::Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let meta =
        std::fs::metadata(path).map_err(|e| format!("Cannot stat viewing keys file: {e}"))?;
    let mode = meta.permissions().mode();
    // Bits 0o077: any group/other read/write/execute.
    if mode & 0o077 != 0 {
        return Err(format!(
            "viewing_keys.toml at {} has loose permissions ({:o}) — \
             chmod 600 to restrict to owner only",
            path.display(),
            mode & 0o777
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_permissions(_path: &Path) -> std::result::Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_is_ok() {
        let path = Path::new("/tmp/snbeat-nonexistent-viewing-keys.toml");
        let (keys, warn) = load_viewing_keys(path).unwrap();
        assert!(keys.is_empty());
        assert!(warn.is_none());
    }

    #[test]
    fn parses_valid_file() {
        // Synthetic test fixtures only — never put a real address +
        // viewing key pair in committed test data.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vk.toml");
        std::fs::write(
            &path,
            r#"
[keys]
"0xabcd" = "0xc0ffee"
"#,
        )
        .unwrap();
        // Set 0600 so the perm check doesn't warn during test.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut p = std::fs::metadata(&path).unwrap().permissions();
            p.set_mode(0o600);
            std::fs::set_permissions(&path, p).unwrap();
        }
        let (keys, warn) = load_viewing_keys(&path).unwrap();
        assert!(warn.is_none());
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].user, Felt::from_hex("0xabcd").unwrap());
    }

    #[test]
    fn corrupt_toml_yields_warning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vk.toml");
        std::fs::write(&path, "this is { not valid").unwrap();
        let (keys, warn) = load_viewing_keys(&path).unwrap();
        assert!(keys.is_empty());
        assert!(warn.is_some());
    }
}
