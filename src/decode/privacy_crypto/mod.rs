//! Vendored decryption primitives for the Starknet Privacy Pool.
//!
//! Source: `crates/discovery-core/src/privacy_pool/` from
//! `github.com/starkware-libs/starknet-privacy` at commit
//! `009a94cea8ca7744a7598e6c40d132d6c0b989bc` (tag `PRIVACY-0.14.2-RC.5`).
//!
//! The four files here are pure functions — storage-slot derivation,
//! ECDH-based decryption, Poseidon hash masks, and the `SecretFelt`
//! zeroize-on-drop wrapper. No async, no I/O. Vendoring (rather than a
//! git dep on `discovery-core`) avoids pulling in the upstream's pinned
//! `starknet-rust` git source, which conflicts with our crates-io
//! `starknet-rust = 0.19`.
//!
//! When upstream changes, regenerate from the same commit and re-run
//! `cargo test -p snbeat decode::privacy_crypto`.

pub mod decryption;
pub mod hashes;
pub mod keys;
pub mod storage_slots;
pub mod types;
