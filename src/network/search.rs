//! Search resolution: dispatches a user query to the appropriate handler
//! (block number, address, tx hash, block hash, or class hash).
#![allow(clippy::too_many_arguments)]

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::decode::AbiRegistry;

use super::address;
use super::block;
use super::class;
use super::dune;
use super::transaction;
use super::voyager;

/// Resolve a search query: try as block number, then address, then tx hash,
/// then block hash, then class hash.
pub(super) async fn resolve_search(
    query: String,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    dune: &Option<Arc<dune::DuneClient>>,
    pf: &Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    voyager: &Option<Arc<voyager::VoyagerClient>>,
    head_block: &Arc<super::HeadTracker>,
    tx: &mpsc::UnboundedSender<Action>,
    cancel: &CancellationToken,
) {
    // Try as block number first
    if let Ok(num) = query.parse::<u64>() {
        block::fetch_and_send_block_detail(num, ds, abi_reg, voyager, tx).await;
        return;
    }

    // Try as hex — could be address, tx hash, block hash, or class hash.
    // Three-stage probe ordered by query frequency:
    //   1. class_hash (most common: address searches) — single RPC, usually
    //      cache-served.
    //   2. tx + receipt joined (tx-hash searches) — both are needed to
    //      dispatch a tx detail, so race them together.
    //   3. block_hash + class joined (rare) — only probed if 1 and 2 miss.
    // Staging avoids waiting for a slow class-fetch on every tx-hash search,
    // which `tokio::join!` of all four would otherwise force.
    let hex = query.strip_prefix("0x").unwrap_or(&query);
    if let Ok(felt) = starknet::core::types::Felt::from_hex(&format!("0x{hex}")) {
        // Cache-first probe: if we've ever resolved this hex as an address
        // before, we have a cached class_hash, nonce, or class_history row.
        // Short-circuit straight to the address pipeline so the cache-first
        // emit can paint without first round-tripping `get_class_hash`.
        let cached_address = ds.load_cached_class_hash(&felt).is_some()
            || ds.load_cached_nonce(&felt).is_some()
            || !ds.load_cached_class_history(&felt).is_empty();
        if cached_address {
            let _ = tx.send(Action::NavigateToAddress { address: felt });
            address::fetch_and_send_address_info(
                felt, ds, abi_reg, dune, pf, voyager, head_block, tx, cancel,
            )
            .await;
            return;
        }
        if ds.get_class_hash(felt).await.is_ok() {
            let _ = tx.send(Action::NavigateToAddress { address: felt });
            address::fetch_and_send_address_info(
                felt, ds, abi_reg, dune, pf, voyager, head_block, tx, cancel,
            )
            .await;
            return;
        }

        let (tx_res, receipt_res) = tokio::join!(ds.get_transaction(felt), ds.get_receipt(felt));

        if let Ok(transaction) = tx_res {
            match receipt_res {
                Ok(receipt) => {
                    transaction::decode_and_send_transaction(
                        transaction,
                        receipt,
                        ds,
                        pf.as_ref(),
                        abi_reg,
                        tx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = tx.send(Action::Error(format!(
                        "Found tx {felt:#x} but failed to fetch receipt: {err}"
                    )));
                }
            }
            return;
        }

        let (block_hash_res, class_res) =
            tokio::join!(ds.get_block_by_hash(felt), ds.get_class(felt));

        if let Ok(number) = block_hash_res {
            block::fetch_and_send_block_detail(number, ds, abi_reg, voyager, tx).await;
            return;
        }

        if class_res.is_ok() {
            class::fetch_class_info(felt, ds, abi_reg, dune, pf, tx).await;
            return;
        }

        let _ = tx.send(Action::Error(
            "Not found as address, transaction, block hash, or class hash".to_string(),
        ));
    }
}
