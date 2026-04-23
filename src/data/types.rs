use starknet::core::types::Felt;

/// Block summary for the block list.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnBlock {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
    pub sequencer_address: Felt,
    pub transaction_count: usize,
    // Gas prices in fri (10^-18 STRK)
    pub l1_gas_price_fri: u128,
    pub l1_gas_price_wei: u128,
    pub l2_gas_price_fri: u128,
    pub l1_data_gas_price_fri: u128,
    pub starknet_version: String,
}

/// Transaction types on Starknet.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SnTransaction {
    Invoke(InvokeTx),
    Declare(DeclareTx),
    DeployAccount(DeployAccountTx),
    L1Handler(L1HandlerTx),
    Deploy(DeployTx),
}

impl SnTransaction {
    pub fn hash(&self) -> Felt {
        match self {
            SnTransaction::Invoke(tx) => tx.hash,
            SnTransaction::Declare(tx) => tx.hash,
            SnTransaction::DeployAccount(tx) => tx.hash,
            SnTransaction::L1Handler(tx) => tx.hash,
            SnTransaction::Deploy(tx) => tx.hash,
        }
    }

    pub fn sender(&self) -> Felt {
        match self {
            SnTransaction::Invoke(tx) => tx.sender_address,
            SnTransaction::Declare(tx) => tx.sender_address,
            SnTransaction::DeployAccount(tx) => tx.contract_address,
            SnTransaction::L1Handler(tx) => tx.contract_address,
            SnTransaction::Deploy(tx) => tx.contract_address,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            SnTransaction::Invoke(_) => "INVOKE",
            SnTransaction::Declare(_) => "DECLARE",
            SnTransaction::DeployAccount(_) => "DEPLOY_ACCOUNT",
            SnTransaction::L1Handler(_) => "L1_HANDLER",
            SnTransaction::Deploy(_) => "DEPLOY",
        }
    }

    pub fn actual_fee(&self) -> Option<Felt> {
        match self {
            SnTransaction::Invoke(tx) => tx.actual_fee,
            SnTransaction::Declare(tx) => tx.actual_fee,
            SnTransaction::DeployAccount(tx) => tx.actual_fee,
            SnTransaction::L1Handler(tx) => tx.actual_fee,
            SnTransaction::Deploy(tx) => tx.actual_fee,
        }
    }

    pub fn set_actual_fee(&mut self, fee: Felt) {
        match self {
            SnTransaction::Invoke(tx) => tx.actual_fee = Some(fee),
            SnTransaction::Declare(tx) => tx.actual_fee = Some(fee),
            SnTransaction::DeployAccount(tx) => tx.actual_fee = Some(fee),
            SnTransaction::L1Handler(tx) => tx.actual_fee = Some(fee),
            SnTransaction::Deploy(tx) => tx.actual_fee = Some(fee),
        }
    }

    pub fn tip(&self) -> u64 {
        match self {
            SnTransaction::Invoke(tx) => tx.tip,
            SnTransaction::Declare(tx) => tx.tip,
            SnTransaction::DeployAccount(tx) => tx.tip,
            SnTransaction::L1Handler(_) | SnTransaction::Deploy(_) => 0,
        }
    }

    pub fn block_number(&self) -> u64 {
        match self {
            SnTransaction::Invoke(tx) => tx.block_number,
            SnTransaction::Declare(tx) => tx.block_number,
            SnTransaction::DeployAccount(tx) => tx.block_number,
            SnTransaction::L1Handler(tx) => tx.block_number,
            SnTransaction::Deploy(tx) => tx.block_number,
        }
    }

    pub fn index(&self) -> u64 {
        match self {
            SnTransaction::Invoke(tx) => tx.index,
            SnTransaction::Declare(tx) => tx.index,
            SnTransaction::DeployAccount(tx) => tx.index,
            SnTransaction::L1Handler(tx) => tx.index,
            SnTransaction::Deploy(tx) => tx.index,
        }
    }

    pub fn nonce(&self) -> Option<Felt> {
        match self {
            SnTransaction::Invoke(tx) => tx.nonce,
            SnTransaction::Declare(_) => None,
            SnTransaction::DeployAccount(tx) => tx.nonce,
            SnTransaction::L1Handler(tx) => tx.nonce,
            SnTransaction::Deploy(_) => None,
        }
    }

    /// The first selector called (for Invoke txs, from multicall calldata).
    /// Used for the decoded endpoint column in block detail.
    pub fn first_selector(&self) -> Option<Felt> {
        match self {
            SnTransaction::Invoke(tx) => {
                // Multicall: calldata[0]=num_calls, [1]=addr, [2]=selector
                if tx.calldata.len() >= 3 {
                    Some(tx.calldata[2])
                } else {
                    None
                }
            }
            SnTransaction::L1Handler(tx) => Some(tx.entry_point_selector),
            _ => None,
        }
    }
}

/// Resource bounds for a V3 transaction.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SnResourceBounds {
    pub l1_gas_max_amount: u64,
    pub l1_gas_max_price: u128,
    pub l2_gas_max_amount: u64,
    pub l2_gas_max_price: u128,
    pub l1_data_gas_max_amount: u64,
    pub l1_data_gas_max_price: u128,
}

/// Actual resources consumed (from receipt).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SnExecutionResources {
    pub l1_gas: u64,
    pub l2_gas: u64,
    pub l1_data_gas: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InvokeTx {
    pub hash: Felt,
    pub sender_address: Felt,
    pub calldata: Vec<Felt>,
    pub nonce: Option<Felt>,
    pub version: Felt,
    pub actual_fee: Option<Felt>,
    pub execution_status: ExecutionStatus,
    pub block_number: u64,
    pub index: u64,
    pub tip: u64,
    pub resource_bounds: Option<SnResourceBounds>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeclareTx {
    pub hash: Felt,
    pub sender_address: Felt,
    pub class_hash: Felt,
    pub version: Felt,
    pub actual_fee: Option<Felt>,
    pub execution_status: ExecutionStatus,
    pub block_number: u64,
    pub index: u64,
    pub tip: u64,
    pub resource_bounds: Option<SnResourceBounds>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeployAccountTx {
    pub hash: Felt,
    pub contract_address: Felt,
    pub class_hash: Felt,
    pub constructor_calldata: Vec<Felt>,
    #[serde(default)]
    pub contract_address_salt: Felt,
    pub nonce: Option<Felt>,
    pub version: Felt,
    pub actual_fee: Option<Felt>,
    pub execution_status: ExecutionStatus,
    pub block_number: u64,
    pub index: u64,
    pub tip: u64,
    pub resource_bounds: Option<SnResourceBounds>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct L1HandlerTx {
    pub hash: Felt,
    pub contract_address: Felt,
    pub entry_point_selector: Felt,
    pub calldata: Vec<Felt>,
    pub nonce: Option<Felt>,
    pub actual_fee: Option<Felt>,
    pub execution_status: ExecutionStatus,
    pub block_number: u64,
    pub index: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeployTx {
    pub hash: Felt,
    pub contract_address: Felt,
    pub class_hash: Felt,
    pub constructor_calldata: Vec<Felt>,
    pub version: Felt,
    pub actual_fee: Option<Felt>,
    pub execution_status: ExecutionStatus,
    pub block_number: u64,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecutionStatus {
    Succeeded,
    Reverted(String),
    Unknown,
}

/// Transaction receipt.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnReceipt {
    pub transaction_hash: Felt,
    pub block_number: u64,
    pub block_hash: Option<Felt>,
    pub actual_fee: Felt,
    pub fee_unit: String,
    pub execution_status: ExecutionStatus,
    pub execution_resources: SnExecutionResources,
    pub events: Vec<SnEvent>,
    pub revert_reason: Option<String>,
    pub finality: String, // "ACCEPTED_ON_L2", "ACCEPTED_ON_L1", "PRE_CONFIRMED"
}

/// A single event emitted during transaction execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnEvent {
    pub from_address: Felt,
    pub keys: Vec<Felt>,
    pub data: Vec<Felt>,
    pub transaction_hash: Felt,
    pub block_number: u64,
    pub event_index: u64,
}

/// A transaction summary for the address tx list.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AddressTxSummary {
    pub hash: Felt,
    pub nonce: u64,
    pub block_number: u64,
    pub timestamp: u64,
    pub endpoint_names: String, // comma-separated decoded selectors
    pub total_fee_fri: u128,
    pub tip: u64,
    pub tx_type: String,
    pub status: String, // "OK", "REVERTED", or "?"
    /// The actual sender of this transaction (may differ from the viewed address for deployment txs).
    #[serde(default)]
    pub sender: Option<Felt>,
}

/// A meta-transaction (SNIP-9 outside execution) summary for the MetaTxs tab on
/// an address view. Represents a tx where the viewed address is the intender
/// (original signer), not the on-chain sender (paymaster/relayer).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetaTxIntenderSummary {
    /// On-chain tx hash.
    pub hash: Felt,
    /// Block the tx landed in.
    pub block_number: u64,
    /// Index within the block (tie-break for recency ordering).
    pub tx_index: u64,
    /// Block timestamp (unix seconds).
    pub timestamp: u64,
    /// The address that submitted the on-chain tx (relayer / AVNU forwarder / paymaster).
    pub paymaster: Felt,
    /// Outside-execution version: "v1" | "v2" | "v3" | "avnu".
    pub version: String,
    /// Outside-execution nonce (signed by the intender, not the sender's tx nonce).
    pub oe_nonce: Felt,
    /// Total fee paid by the paymaster (FRI).
    pub total_fee_fri: u128,
    /// Execution status ("OK" / "REV" / "?").
    pub status: String,
    /// Target addresses of every inner call (first rendered in the row, rest shown as "+N").
    pub inner_targets: Vec<Felt>,
    /// Comma-joined decoded entrypoint names of the inner calls (e.g. "approve, swap_exact_in").
    pub inner_endpoints: String,
    /// The `caller` field from the OE struct (ANY_CALLER sentinel or a specific forwarder).
    pub caller: Felt,
}

/// A call to a contract (for the Calls tab on contract addresses).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ContractCallSummary {
    pub tx_hash: Felt,
    pub sender: Felt,
    pub function_name: String,
    pub block_number: u64,
    pub timestamp: u64,
    pub total_fee_fri: u128,
    pub status: String,
    /// Sender tx nonce. `None` from Dune-sourced rows (not in `starknet.calls`);
    /// populated from RPC/pf-query rows and merged in by `deduplicate_contract_calls`.
    #[serde(default)]
    pub nonce: Option<u64>,
    /// Sender tip (FRI). `0` from Dune-sourced rows and stubs; filled by RPC/pf path.
    #[serde(default)]
    pub tip: u64,
}

/// Label information fetched from Voyager for a contract/account address.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VoyagerLabelInfo {
    /// Display name shown on Voyager (e.g. "Binance: Hot Wallet", "Ether").
    pub name: Option<String>,
    /// Human-readable class alias (e.g. "OpenZeppelin Account", "ERC20").
    pub class_alias: Option<String>,
    /// Block number where the contract was deployed (from Voyager `blockNumber` field).
    /// Provides a known lower bound for the activity range.
    #[serde(default)]
    pub deploy_block: Option<u64>,
}

/// Deduplicate contract calls by transaction hash.
///
/// When multiple calls share the same `tx_hash` (e.g. a single transaction
/// calls the target contract several times from different endpoints), merge
/// them into one entry:
/// - Function names are joined with ", " (duplicates removed).
/// - Fee, status, block, and timestamp are taken from whichever entry has data.
/// - The sender from the first occurrence is kept; callers can overwrite it
///   with the real tx sender later.
pub fn deduplicate_contract_calls(calls: Vec<ContractCallSummary>) -> Vec<ContractCallSummary> {
    use std::collections::HashMap;

    let mut seen: HashMap<Felt, usize> = HashMap::new();
    let mut result: Vec<ContractCallSummary> = Vec::new();

    for call in calls {
        if let Some(&idx) = seen.get(&call.tx_hash) {
            let existing = &mut result[idx];
            // Merge function names, skipping duplicates
            if !call.function_name.is_empty() {
                let existing_names: std::collections::HashSet<String> = existing
                    .function_name
                    .split(", ")
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                for name in call.function_name.split(", ") {
                    if !name.is_empty() && !existing_names.contains(name) {
                        if existing.function_name.is_empty() {
                            existing.function_name = name.to_string();
                        } else {
                            existing.function_name =
                                format!("{}, {}", existing.function_name, name);
                        }
                    }
                }
            }
            // Fill in missing data from later entries
            if existing.total_fee_fri == 0 && call.total_fee_fri > 0 {
                existing.total_fee_fri = call.total_fee_fri;
            }
            if existing.timestamp == 0 && call.timestamp > 0 {
                existing.timestamp = call.timestamp;
            }
            if existing.nonce.is_none() && call.nonce.is_some() {
                existing.nonce = call.nonce;
            }
            if existing.tip == 0 && call.tip > 0 {
                existing.tip = call.tip;
            }
        } else {
            seen.insert(call.tx_hash, result.len());
            result.push(call);
        }
    }

    result
}

/// Address information for the address view.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnAddressInfo {
    pub address: Felt,
    pub nonce: Felt,
    pub class_hash: Option<Felt>,
    pub recent_events: Vec<SnEvent>,
    pub token_balances: Vec<TokenBalance>,
}

/// A token balance for an address.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenBalance {
    pub token_address: Felt,
    pub token_name: String,
    pub balance_raw: Felt,
    pub decimals: u8,
}

/// A contract deployed with a given class hash (for the ClassInfo view).
#[derive(Debug, Clone)]
pub struct ClassContractEntry {
    pub address: Felt,
    pub block_number: u64,
}

/// Declaration info for a class hash (from PF+RPC or Dune fallback).
#[derive(Debug, Clone)]
pub struct ClassDeclareInfo {
    pub tx_hash: Felt,
    pub sender: Felt,
    pub block_number: u64,
    pub timestamp: u64,
}
