//! Class-related network functions: fetching class info, ABI, declaration, and contracts.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::app::actions::Action;
use crate::data::DataSource;
use crate::data::types::SnTransaction;
use crate::decode::AbiRegistry;

use super::dune;

/// Fetch class info from all available sources in parallel.
pub(super) async fn fetch_class_info(
    class_hash: starknet::core::types::Felt,
    ds: &Arc<dyn DataSource>,
    abi_reg: &Arc<AbiRegistry>,
    dune: &Option<Arc<dune::DuneClient>>,
    pf: &Option<Arc<crate::data::pathfinder::PathfinderClient>>,
    tx: &mpsc::UnboundedSender<Action>,
) {
    // Navigate immediately
    let _ = tx.send(Action::NavigateToClassInfo { class_hash });

    // 1. ABI from registry (may be cached)
    {
        let abi_reg = Arc::clone(abi_reg);
        let tx = tx.clone();
        tokio::spawn(async move {
            let abi = abi_reg.get_abi_for_class(&class_hash).await;
            debug!(%class_hash, has_abi = abi.is_some(), "ABI fetch complete");
            let _ = tx.send(Action::ClassAbiLoaded { class_hash, abi });
        });
    }

    // 2. Declaration info + contracts list
    // Primary: PF for declaration block → RPC for block txs → find declare tx
    // Fallback: Dune for declare tx
    //
    // Cache-first: declarations are immutable and contracts-by-class is
    // TTL'd — checking the local store here turns a repeat visit into zero
    // upstream round-trips. Each spawned task still emits its tab's action
    // even on cache hit so the UI never hangs waiting for a "loaded" signal.
    let cached_declare = ds.load_cached_class_declaration(&class_hash);
    let cached_contracts = ds.load_cached_class_contracts(&class_hash);

    if let Some(pf_client) = pf {
        // Shared declaration block lookup — fetched once, used by both tasks.
        // Seed from cached declare info so we skip the pf round trip too.
        let decl_block_cell: Arc<tokio::sync::OnceCell<Option<u64>>> =
            Arc::new(tokio::sync::OnceCell::new());
        if let Some(info) = &cached_declare {
            let _ = decl_block_cell.set(Some(info.block_number));
        }

        // Spawn contracts-by-class fetch
        let pf_c = Arc::clone(pf_client);
        let ds_c = Arc::clone(ds);
        let tx_c = tx.clone();
        let decl_cell_c = Arc::clone(&decl_block_cell);
        let cached_contracts_c = cached_contracts.clone();
        let cached_decl_block = cached_declare.as_ref().map(|d| d.block_number);
        tokio::spawn(async move {
            // Cache hit → emit immediately, no upstream calls.
            if let Some(entries) = cached_contracts_c {
                debug!(%class_hash, n = entries.len(), "class contracts: cache hit");
                let _ = tx_c.send(Action::ClassContractsLoaded {
                    class_hash,
                    contracts: entries,
                    declaration_block: cached_decl_block,
                });
                return;
            }
            let decl_block = *decl_cell_c
                .get_or_init(|| async { pf_c.get_class_declaration(class_hash).await.ok() })
                .await;
            let contracts = pf_c
                .get_contracts_by_class(class_hash)
                .await
                .unwrap_or_default();
            let contract_entries: Vec<crate::data::types::ClassContractEntry> = contracts
                .into_iter()
                .filter_map(
                    |c| match starknet::core::types::Felt::from_hex(&c.contract_address) {
                        Ok(address) => Some(crate::data::types::ClassContractEntry {
                            address,
                            block_number: c.block_number,
                        }),
                        Err(e) => {
                            warn!(
                                %class_hash,
                                contract_address = %c.contract_address,
                                error = %e,
                                "Skipping invalid PF contract address"
                            );
                            None
                        }
                    },
                )
                .collect();
            ds_c.save_class_contracts(&class_hash, &contract_entries);
            let _ = tx_c.send(Action::ClassContractsLoaded {
                class_hash,
                contracts: contract_entries,
                declaration_block: decl_block,
            });
        });

        // Spawn declare tx fetch: PF declaration block → RPC block txs → find Declare tx
        let pf_c = Arc::clone(pf_client);
        let ds_c = Arc::clone(ds);
        let tx_c = tx.clone();
        let decl_cell_c = Arc::clone(&decl_block_cell);
        let cached_declare_c = cached_declare.clone();
        tokio::spawn(async move {
            // Cache hit → emit immediately, no pf + no get_block_with_txs.
            if let Some(info) = cached_declare_c {
                debug!(%class_hash, block_number = info.block_number, "class declare: cache hit");
                let _ = tx_c.send(Action::ClassDeclareLoaded {
                    class_hash,
                    declare_info: Some(info),
                });
                return;
            }
            let declare_info = match *decl_cell_c
                .get_or_init(|| async { pf_c.get_class_declaration(class_hash).await.ok() })
                .await
            {
                Some(block_number) => {
                    info!(block_number, %class_hash, "PF: class declared at block");
                    match ds_c.get_block_with_txs(block_number).await {
                        Ok((_block, txs)) => {
                            let declare_count = txs
                                .iter()
                                .filter(|t| matches!(t, SnTransaction::Declare(_)))
                                .count();
                            debug!(
                                tx_count = txs.len(),
                                declare_count, "Block txs fetched for declare lookup"
                            );
                            let result = txs.into_iter().find_map(|t| {
                                if let SnTransaction::Declare(decl) = t
                                    && decl.class_hash == class_hash
                                {
                                    return Some(crate::data::types::ClassDeclareInfo {
                                        tx_hash: decl.hash,
                                        sender: decl.sender_address,
                                        block_number: decl.block_number,
                                        timestamp: 0,
                                    });
                                }
                                None
                            });
                            if result.is_none() {
                                warn!(%class_hash, block_number, "Declare tx not found in block");
                            }
                            result
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to fetch block txs for declare lookup");
                            None
                        }
                    }
                }
                None => {
                    warn!(%class_hash, "Class declaration lookup failed in PF");
                    None
                }
            };
            if let Some(info) = &declare_info {
                ds_c.save_class_declaration(&class_hash, info);
            }
            let _ = tx_c.send(Action::ClassDeclareLoaded {
                class_hash,
                declare_info,
            });
        });
    } else if let Some(dune_client) = dune {
        // Fallback: Dune for declare tx. Cache hit short-circuits upstream.
        let dune_c = Arc::clone(dune_client);
        let ds_c = Arc::clone(ds);
        let tx_c = tx.clone();
        tokio::spawn(async move {
            let declare_info = if let Some(info) = cached_declare.clone() {
                debug!(%class_hash, block_number = info.block_number, "class declare: cache hit (dune path)");
                Some(info)
            } else {
                let fetched = match dune_c.query_declare_tx(class_hash).await {
                    Ok(info) => info,
                    Err(e) => {
                        warn!(error = %e, "Dune declare tx query failed");
                        None
                    }
                };
                if let Some(info) = &fetched {
                    ds_c.save_class_declaration(&class_hash, info);
                }
                fetched
            };
            let decl_block = declare_info.as_ref().map(|d| d.block_number);
            let _ = tx_c.send(Action::ClassDeclareLoaded {
                class_hash,
                declare_info,
            });
            // No contracts list without PF
            let _ = tx_c.send(Action::ClassContractsLoaded {
                class_hash,
                contracts: Vec::new(),
                declaration_block: decl_block,
            });
        });
    } else {
        // No PF or Dune — send empty results
        let _ = tx.send(Action::ClassDeclareLoaded {
            class_hash,
            declare_info: None,
        });
        let _ = tx.send(Action::ClassContractsLoaded {
            class_hash,
            contracts: Vec::new(),
            declaration_block: None,
        });
    }
}
