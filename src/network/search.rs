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
    tx: &mpsc::UnboundedSender<Action>,
    cancel: &CancellationToken,
) {
    // Try as block number first
    if let Ok(num) = query.parse::<u64>() {
        block::fetch_and_send_block_detail(num, ds, abi_reg, voyager, tx).await;
        return;
    }

    // Try as hex — could be address, tx hash, block hash, or class hash.
    // Starknet uses the same Felt encoding for all four, so we race the four
    // backend probes in parallel and dispatch on results in priority order:
    //   1. class_hash present → address (deployed contract / account)
    //   2. transaction        → tx detail
    //   3. block_by_hash      → block detail
    //   4. class              → class info
    // Cache writes are idempotent so the speculative parallel fetches are
    // harmless on miss, and on hit they collapse what used to be up to four
    // sequential RPCs into a single round-trip. The receipt is fetched in
    // the same batch so a tx-hash search resolves in one RTT instead of two.
    let hex = query.strip_prefix("0x").unwrap_or(&query);
    if let Ok(felt) = starknet::core::types::Felt::from_hex(&format!("0x{hex}")) {
        let (class_hash_res, tx_res, receipt_res, block_hash_res, class_res) = tokio::join!(
            ds.get_class_hash(felt),
            ds.get_transaction(felt),
            ds.get_receipt(felt),
            ds.get_block_by_hash(felt),
            ds.get_class(felt),
        );

        if class_hash_res.is_ok() {
            let _ = tx.send(Action::NavigateToAddress { address: felt });
            address::fetch_and_send_address_info(felt, ds, abi_reg, dune, pf, voyager, tx, cancel)
                .await;
            return;
        }

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
