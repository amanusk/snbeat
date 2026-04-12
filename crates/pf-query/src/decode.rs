//! Decompression and deserialization of Pathfinder transaction/event blobs.

use std::sync::LazyLock;

use crate::dto::{EventsForBlock, TransactionWithReceiptV4, TransactionsWithReceiptsForBlock};

const MAX_UNCOMPRESSED_SIZE: usize = 128 * 1024 * 1024;

static TXS_DICT: LazyLock<zstd::dict::DecoderDictionary<'static>> =
    LazyLock::new(|| zstd::dict::DecoderDictionary::copy(include_bytes!("assets/txs.zdict")));

static EVENTS_DICT: LazyLock<zstd::dict::DecoderDictionary<'static>> =
    LazyLock::new(|| zstd::dict::DecoderDictionary::copy(include_bytes!("assets/events.zdict")));

/// Decode a transactions blob into individual transaction+receipt pairs.
pub fn decode_transactions(blob: &[u8]) -> anyhow::Result<Vec<TransactionWithReceiptV4>> {
    let decompressed = {
        let mut dec = zstd::bulk::Decompressor::with_prepared_dictionary(&TXS_DICT)?;
        dec.decompress(blob, MAX_UNCOMPRESSED_SIZE)?
    };
    let (block, _): (TransactionsWithReceiptsForBlock, _) =
        bincode::serde::decode_from_slice(&decompressed, bincode::config::standard())?;
    Ok(block.into_latest())
}

/// Decode an events blob into per-transaction event lists.
pub fn decode_events(blob: &[u8]) -> anyhow::Result<Vec<Vec<crate::dto::Event>>> {
    let decompressed = {
        let mut dec = zstd::bulk::Decompressor::with_prepared_dictionary(&EVENTS_DICT)?;
        dec.decompress(blob, MAX_UNCOMPRESSED_SIZE)?
    };
    let (block, _): (EventsForBlock, _) =
        bincode::serde::decode_from_slice(&decompressed, bincode::config::standard())?;
    Ok(block.events())
}
