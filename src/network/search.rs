//! Search resolution: dispatches a user query to the appropriate handler
//! (block number, address, tx hash, block hash, or class hash).

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

    // Try as hex — could be address or tx hash.
    // Starknet addresses and tx hashes have the same format.
    // Strategy: check class_hash first (fast) — if it exists, it's a contract/account.
    // Otherwise try as tx hash.
    let hex = query.strip_prefix("0x").unwrap_or(&query);
    if let Ok(felt) = starknet::core::types::Felt::from_hex(&format!("0x{hex}")) {
        // Step 1: Check if it's a deployed contract/account (has class_hash)
        let is_contract = ds.get_class_hash(felt).await.is_ok();

        if is_contract {
            // It's an address — go to address view
            let _ = tx.send(Action::NavigateToAddress { address: felt });
            address::fetch_and_send_address_info(felt, ds, abi_reg, dune, pf, voyager, tx, cancel)
                .await;
            return;
        }

        // Step 2: Not a contract — try as tx hash
        match ds.get_transaction(felt).await {
            Ok(transaction) => {
                // Reuse the fetched transaction, only fetch receipt
                match ds.get_receipt(felt).await {
                    Ok(receipt) => {
                        transaction::decode_and_send_transaction(
                            transaction,
                            receipt,
                            ds,
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
            Err(_) => {
                // Step 3: Not a tx — try as block hash
                match ds.get_block_by_hash(felt).await {
                    Ok(number) => {
                        block::fetch_and_send_block_detail(number, ds, abi_reg, voyager, tx).await;
                    }
                    Err(_) => {
                        // Step 4: Try as class hash
                        if ds.get_class(felt).await.is_ok() {
                            class::fetch_class_info(felt, ds, abi_reg, dune, pf, tx).await;
                        } else {
                            let _ = tx.send(Action::Error(
                                "Not found as address, transaction, block hash, or class hash"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }
    }
}
