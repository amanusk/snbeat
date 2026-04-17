//! Pathfinder storage DTO types for decoding transaction and event blobs.
//!
//! These types replicate the exact serde layout of pathfinder's internal DTOs
//! so we can deserialize the zstd+bincode blobs from the `transactions` table
//! without depending on the full pathfinder crate tree.
//!
//! Reference: pathfinder/crates/storage/src/connection/transaction.rs (dto module)

use std::fmt;

use primitive_types::H160;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// MinimalFelt — variable-length Felt encoding matching pathfinder's format
// ---------------------------------------------------------------------------

/// A 32-byte big-endian field element that strips leading zeros during
/// serialization (matching pathfinder's `MinimalFelt`).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct MinimalFelt(pub [u8; 32]);

impl MinimalFelt {
    pub fn to_hex(&self) -> String {
        let zeros = self.0.iter().take_while(|&&x| x == 0).count();
        if zeros == 32 {
            "0x0".to_string()
        } else {
            format!("0x{}", hex::encode(&self.0[zeros..]))
        }
    }

    /// Interpret as u64 (last 8 bytes, big-endian).
    pub fn as_u64(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.0[24..32]);
        u64::from_be_bytes(buf)
    }

    /// Interpret as u128 (last 16 bytes, big-endian).
    pub fn as_u128(&self) -> u128 {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&self.0[16..32]);
        u128::from_be_bytes(buf)
    }
}

impl serde::Serialize for MinimalFelt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let zeros = self.0.iter().take_while(|&&x| x == 0).count();
        self.0[zeros..].serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for MinimalFelt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = MinimalFelt;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a sequence of bytes")
            }

            fn visit_seq<B>(self, mut seq: B) -> Result<Self::Value, B::Error>
            where
                B: serde::de::SeqAccess<'de>,
            {
                let len = seq.size_hint().unwrap();
                let mut bytes = [0u8; 32];
                let start = 32 - len;
                let mut i = start;
                while let Some(value) = seq.next_element()? {
                    bytes[i] = value;
                    i += 1;
                }
                Ok(MinimalFelt(bytes))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}

// ---------------------------------------------------------------------------
// Wrapper types matching pathfinder_common's serde layout
// ---------------------------------------------------------------------------

/// Transaction index within a block.
/// pathfinder uses custom serde that serializes as plain u64.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct TransactionIndex(pub u64);

impl serde::Serialize for TransactionIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> serde::Deserialize<'de> for TransactionIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self(u64::deserialize(deserializer)?))
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Tip(pub u64);

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceAmount(pub u64);

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourcePricePerUnit(pub u128);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct EthereumAddress(pub H160);

// ---------------------------------------------------------------------------
// Entry point type
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum EntryPointType {
    External,
    L1Handler,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum EventsForBlock {
    V0 { events: Vec<Vec<Event>> },
}

impl EventsForBlock {
    pub fn events(self) -> Vec<Vec<Event>> {
        match self {
            EventsForBlock::V0 { events } => events,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub data: Vec<MinimalFelt>,
    pub from_address: MinimalFelt,
    pub keys: Vec<MinimalFelt>,
}

// ---------------------------------------------------------------------------
// Execution resources
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionResourcesV0 {
    pub builtins: BuiltinCountersV0,
    pub n_steps: u64,
    pub n_memory_holes: u64,
    pub data_availability: L1Gas,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionResourcesV1 {
    pub builtins: BuiltinCountersV1,
    pub n_steps: u64,
    pub n_memory_holes: u64,
    pub data_availability: L1Gas,
    pub total_gas_consumed: L1Gas,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionResourcesV2 {
    pub builtins: BuiltinCountersV1,
    pub n_steps: u64,
    pub n_memory_holes: u64,
    pub data_availability: L1Gas,
    pub total_gas_consumed: L1Gas,
    pub l2_gas_consumed: L2Gas,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct L1Gas {
    pub l1_gas: Option<u128>,
    pub l1_data_gas: Option<u128>,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct L2Gas {
    pub l2_gas: u128,
}

impl From<ExecutionResourcesV0> for ExecutionResourcesV2 {
    fn from(v: ExecutionResourcesV0) -> Self {
        Self {
            builtins: v.builtins.into(),
            n_steps: v.n_steps,
            n_memory_holes: v.n_memory_holes,
            data_availability: v.data_availability,
            total_gas_consumed: Default::default(),
            l2_gas_consumed: Default::default(),
        }
    }
}

impl From<ExecutionResourcesV1> for ExecutionResourcesV2 {
    fn from(v: ExecutionResourcesV1) -> Self {
        Self {
            builtins: v.builtins,
            n_steps: v.n_steps,
            n_memory_holes: v.n_memory_holes,
            data_availability: v.data_availability,
            total_gas_consumed: Default::default(),
            l2_gas_consumed: Default::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Builtin counters
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BuiltinCountersV0 {
    pub output: u64,
    pub pedersen: u64,
    pub range_check: u64,
    pub ecdsa: u64,
    pub bitwise: u64,
    pub ec_op: u64,
    pub keccak: u64,
    pub poseidon: u64,
    pub segment_arena: u64,
}

#[derive(Copy, Clone, Default, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BuiltinCountersV1 {
    pub output: u64,
    pub pedersen: u64,
    pub range_check: u64,
    pub ecdsa: u64,
    pub bitwise: u64,
    pub ec_op: u64,
    pub keccak: u64,
    pub poseidon: u64,
    pub segment_arena: u64,
    pub add_mod: u64,
    pub mul_mod: u64,
    pub range_check96: u64,
}

impl From<BuiltinCountersV0> for BuiltinCountersV1 {
    fn from(v: BuiltinCountersV0) -> Self {
        Self {
            output: v.output,
            pedersen: v.pedersen,
            range_check: v.range_check,
            ecdsa: v.ecdsa,
            bitwise: v.bitwise,
            ec_op: v.ec_op,
            keccak: v.keccak,
            poseidon: v.poseidon,
            segment_arena: v.segment_arena,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// L2 to L1 messages
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct L2ToL1MessageV0 {
    pub from_address: MinimalFelt,
    pub payload: Vec<MinimalFelt>,
    pub to_address: EthereumAddress,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct L2ToL1MessageV1 {
    pub from_address: MinimalFelt,
    pub payload: Vec<MinimalFelt>,
    pub to_address: MinimalFelt,
}

impl From<L2ToL1MessageV0> for L2ToL1MessageV1 {
    fn from(v: L2ToL1MessageV0) -> Self {
        // Convert H160 (20 bytes) to a 32-byte Felt (right-aligned)
        let mut bytes = [0u8; 32];
        bytes[12..32].copy_from_slice(v.to_address.0.as_bytes());
        Self {
            from_address: v.from_address,
            payload: v.payload,
            to_address: MinimalFelt(bytes),
        }
    }
}

// ---------------------------------------------------------------------------
// Execution status
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ExecutionStatus {
    Succeeded,
    Reverted { reason: String },
}

// ---------------------------------------------------------------------------
// Receipts
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptV0 {
    pub actual_fee: MinimalFelt,
    pub execution_resources: Option<ExecutionResourcesV0>,
    pub l2_to_l1_messages: Vec<L2ToL1MessageV0>,
    pub transaction_hash: MinimalFelt,
    pub transaction_index: TransactionIndex,
    pub execution_status: ExecutionStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptV1 {
    pub actual_fee: MinimalFelt,
    pub execution_resources: Option<ExecutionResourcesV0>,
    pub l2_to_l1_messages: Vec<L2ToL1MessageV1>,
    pub transaction_hash: MinimalFelt,
    pub transaction_index: TransactionIndex,
    pub execution_status: ExecutionStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptV2 {
    pub actual_fee: MinimalFelt,
    pub execution_resources: Option<ExecutionResourcesV1>,
    pub l2_to_l1_messages: Vec<L2ToL1MessageV1>,
    pub transaction_hash: MinimalFelt,
    pub transaction_index: TransactionIndex,
    pub execution_status: ExecutionStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptV3 {
    pub actual_fee: MinimalFelt,
    pub execution_resources: Option<ExecutionResourcesV2>,
    pub l2_to_l1_messages: Vec<L2ToL1MessageV1>,
    pub transaction_hash: MinimalFelt,
    pub transaction_index: TransactionIndex,
    pub execution_status: ExecutionStatus,
}

impl From<ReceiptV0> for ReceiptV3 {
    fn from(v: ReceiptV0) -> Self {
        Self {
            actual_fee: v.actual_fee,
            execution_resources: v.execution_resources.map(Into::into),
            l2_to_l1_messages: v.l2_to_l1_messages.into_iter().map(Into::into).collect(),
            transaction_hash: v.transaction_hash,
            transaction_index: v.transaction_index,
            execution_status: v.execution_status,
        }
    }
}

impl From<ReceiptV1> for ReceiptV3 {
    fn from(v: ReceiptV1) -> Self {
        Self {
            actual_fee: v.actual_fee,
            execution_resources: v.execution_resources.map(Into::into),
            l2_to_l1_messages: v.l2_to_l1_messages,
            transaction_hash: v.transaction_hash,
            transaction_index: v.transaction_index,
            execution_status: v.execution_status,
        }
    }
}

impl From<ReceiptV2> for ReceiptV3 {
    fn from(v: ReceiptV2) -> Self {
        Self {
            actual_fee: v.actual_fee,
            execution_resources: v.execution_resources.map(Into::into),
            l2_to_l1_messages: v.l2_to_l1_messages,
            transaction_hash: v.transaction_hash,
            transaction_index: v.transaction_index,
            execution_status: v.execution_status,
        }
    }
}

// ---------------------------------------------------------------------------
// Data availability & resource bounds
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataAvailabilityMode {
    L1,
    L2,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceBoundsV0 {
    pub l1_gas: ResourceBound,
    pub l2_gas: ResourceBound,
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceBoundsV1 {
    pub l1_gas: ResourceBound,
    pub l2_gas: ResourceBound,
    pub l1_data_gas: Option<ResourceBound>,
}

impl From<ResourceBoundsV0> for ResourceBoundsV1 {
    fn from(v: ResourceBoundsV0) -> Self {
        Self {
            l1_gas: v.l1_gas,
            l2_gas: v.l2_gas,
            l1_data_gas: None,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResourceBound {
    pub max_amount: ResourceAmount,
    pub max_price_per_unit: ResourcePricePerUnit,
}

// ---------------------------------------------------------------------------
// Transaction + Receipt blocks (versioned top-level)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum TransactionsWithReceiptsForBlock {
    V0 {
        transactions_with_receipts: Vec<TransactionWithReceiptV0>,
    },
    V1 {
        transactions_with_receipts: Vec<TransactionWithReceiptV1>,
    },
    V2 {
        transactions_with_receipts: Vec<TransactionWithReceiptV2>,
    },
    V3 {
        transactions_with_receipts: Vec<TransactionWithReceiptV3>,
    },
    V4 {
        transactions_with_receipts: Vec<TransactionWithReceiptV4>,
    },
}

impl TransactionsWithReceiptsForBlock {
    pub fn into_latest(self) -> Vec<TransactionWithReceiptV4> {
        match self {
            Self::V0 {
                transactions_with_receipts: v,
            } => v.into_iter().map(Into::into).collect(),
            Self::V1 {
                transactions_with_receipts: v,
            } => v.into_iter().map(Into::into).collect(),
            Self::V2 {
                transactions_with_receipts: v,
            } => v.into_iter().map(Into::into).collect(),
            Self::V3 {
                transactions_with_receipts: v,
            } => v.into_iter().map(Into::into).collect(),
            Self::V4 {
                transactions_with_receipts: v,
            } => v,
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction + Receipt pairs per version
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionWithReceiptV0 {
    pub transaction: TransactionV0,
    pub receipt: ReceiptV0,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionWithReceiptV1 {
    pub transaction: TransactionV1,
    pub receipt: ReceiptV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionWithReceiptV2 {
    pub transaction: TransactionV1,
    pub receipt: ReceiptV2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionWithReceiptV3 {
    pub transaction: TransactionV2,
    pub receipt: ReceiptV3,
}

/// V4 block format (pathfinder >= 0.22): uses TransactionV3 with InvokeV5 support.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionWithReceiptV4 {
    pub transaction: TransactionV3,
    pub receipt: ReceiptV3,
}

impl From<TransactionWithReceiptV0> for TransactionWithReceiptV4 {
    fn from(v: TransactionWithReceiptV0) -> Self {
        let v3: TransactionWithReceiptV3 = v.into();
        v3.into()
    }
}

impl From<TransactionWithReceiptV1> for TransactionWithReceiptV4 {
    fn from(v: TransactionWithReceiptV1) -> Self {
        let v3: TransactionWithReceiptV3 = v.into();
        v3.into()
    }
}

impl From<TransactionWithReceiptV2> for TransactionWithReceiptV4 {
    fn from(v: TransactionWithReceiptV2) -> Self {
        let v3: TransactionWithReceiptV3 = v.into();
        v3.into()
    }
}

impl From<TransactionWithReceiptV3> for TransactionWithReceiptV4 {
    fn from(v: TransactionWithReceiptV3) -> Self {
        Self {
            transaction: v.transaction.into(),
            receipt: v.receipt,
        }
    }
}

// Keep the V0→V3 conversions for the intermediate chain
impl From<TransactionWithReceiptV0> for TransactionWithReceiptV3 {
    fn from(v: TransactionWithReceiptV0) -> Self {
        Self {
            transaction: v.transaction.into(),
            receipt: v.receipt.into(),
        }
    }
}

impl From<TransactionWithReceiptV1> for TransactionWithReceiptV3 {
    fn from(v: TransactionWithReceiptV1) -> Self {
        Self {
            transaction: v.transaction.into(),
            receipt: v.receipt.into(),
        }
    }
}

impl From<TransactionWithReceiptV2> for TransactionWithReceiptV3 {
    fn from(v: TransactionWithReceiptV2) -> Self {
        Self {
            transaction: v.transaction.into(),
            receipt: v.receipt.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction versioned wrappers
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransactionV0 {
    pub hash: MinimalFelt,
    pub variant: TransactionVariantV0,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum TransactionVariantV0 {
    DeclareV0(DeclareTransactionV0V1),
    DeclareV1(DeclareTransactionV0V1),
    DeclareV2(DeclareTransactionV2),
    DeclareV3(DeclareTransactionV3),
    Deploy(DeployTransaction),
    DeployAccountV1(DeployAccountTransactionV1),
    DeployAccountV3(DeployAccountTransactionV3),
    InvokeV0(InvokeTransactionV0),
    InvokeV1(InvokeTransactionV1),
    InvokeV3(InvokeTransactionV3),
    L1HandlerV0(L1HandlerTransactionV0),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransactionV1 {
    pub hash: MinimalFelt,
    pub variant: TransactionVariantV1,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum TransactionVariantV1 {
    DeclareV0(DeclareTransactionV0V1),
    DeclareV1(DeclareTransactionV0V1),
    DeclareV2(DeclareTransactionV2),
    DeclareV3(DeclareTransactionV3),
    DeployV0(DeployTransactionV0),
    DeployV1(DeployTransactionV1),
    DeployAccountV1(DeployAccountTransactionV1),
    DeployAccountV3(DeployAccountTransactionV3),
    InvokeV0(InvokeTransactionV0),
    InvokeV1(InvokeTransactionV1),
    InvokeV3(InvokeTransactionV3),
    L1HandlerV0(L1HandlerTransactionV0),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransactionV2 {
    pub hash: MinimalFelt,
    pub variant: TransactionVariantV2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum TransactionVariantV2 {
    DeclareV0(DeclareTransactionV0V1),
    DeclareV1(DeclareTransactionV0V1),
    DeclareV2(DeclareTransactionV2),
    DeclareV3(DeclareTransactionV3),
    DeclareV4(DeclareTransactionV4),
    DeployV0(DeployTransactionV0),
    DeployV1(DeployTransactionV1),
    DeployAccountV1(DeployAccountTransactionV1),
    DeployAccountV3(DeployAccountTransactionV3),
    DeployAccountV4(DeployAccountTransactionV4),
    InvokeV0(InvokeTransactionV0),
    InvokeV1(InvokeTransactionV1),
    InvokeV3(InvokeTransactionV3),
    InvokeV4(InvokeTransactionV4),
    L1HandlerV0(L1HandlerTransactionV0),
}

// V0 -> V2 conversion
impl From<TransactionVariantV0> for TransactionVariantV2 {
    fn from(v: TransactionVariantV0) -> Self {
        match v {
            TransactionVariantV0::DeclareV0(tx) => Self::DeclareV0(tx),
            TransactionVariantV0::DeclareV1(tx) => Self::DeclareV1(tx),
            TransactionVariantV0::DeclareV2(tx) => Self::DeclareV2(tx),
            TransactionVariantV0::DeclareV3(tx) => Self::DeclareV3(tx),
            TransactionVariantV0::Deploy(tx) => {
                // Check version field to distinguish V0 vs V1 deploys
                let is_zero = tx.version.0.iter().all(|&b| b == 0);
                if is_zero {
                    Self::DeployV0(DeployTransactionV0 {
                        contract_address: tx.contract_address,
                        contract_address_salt: tx.contract_address_salt,
                        class_hash: tx.class_hash,
                        constructor_calldata: tx.constructor_calldata,
                    })
                } else {
                    Self::DeployV1(DeployTransactionV1 {
                        contract_address: tx.contract_address,
                        contract_address_salt: tx.contract_address_salt,
                        class_hash: tx.class_hash,
                        constructor_calldata: tx.constructor_calldata,
                    })
                }
            }
            TransactionVariantV0::DeployAccountV1(tx) => Self::DeployAccountV1(tx),
            TransactionVariantV0::DeployAccountV3(tx) => Self::DeployAccountV3(tx),
            TransactionVariantV0::InvokeV0(tx) => Self::InvokeV0(tx),
            TransactionVariantV0::InvokeV1(tx) => Self::InvokeV1(tx),
            TransactionVariantV0::InvokeV3(tx) => Self::InvokeV3(tx),
            TransactionVariantV0::L1HandlerV0(tx) => Self::L1HandlerV0(tx),
        }
    }
}

// V1 -> V2 conversion
impl From<TransactionVariantV1> for TransactionVariantV2 {
    fn from(v: TransactionVariantV1) -> Self {
        match v {
            TransactionVariantV1::DeclareV0(tx) => Self::DeclareV0(tx),
            TransactionVariantV1::DeclareV1(tx) => Self::DeclareV1(tx),
            TransactionVariantV1::DeclareV2(tx) => Self::DeclareV2(tx),
            TransactionVariantV1::DeclareV3(tx) => Self::DeclareV3(tx),
            TransactionVariantV1::DeployV0(tx) => Self::DeployV0(tx),
            TransactionVariantV1::DeployV1(tx) => Self::DeployV1(tx),
            TransactionVariantV1::DeployAccountV1(tx) => Self::DeployAccountV1(tx),
            TransactionVariantV1::DeployAccountV3(tx) => Self::DeployAccountV3(tx),
            TransactionVariantV1::InvokeV0(tx) => Self::InvokeV0(tx),
            TransactionVariantV1::InvokeV1(tx) => Self::InvokeV1(tx),
            TransactionVariantV1::InvokeV3(tx) => Self::InvokeV3(tx),
            TransactionVariantV1::L1HandlerV0(tx) => Self::L1HandlerV0(tx),
        }
    }
}

impl From<TransactionV0> for TransactionV2 {
    fn from(v: TransactionV0) -> Self {
        Self {
            hash: v.hash,
            variant: v.variant.into(),
        }
    }
}

impl From<TransactionV1> for TransactionV2 {
    fn from(v: TransactionV1) -> Self {
        Self {
            hash: v.hash,
            variant: v.variant.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionV3 — adds InvokeV5 (pathfinder >= 0.22)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransactionV3 {
    pub hash: MinimalFelt,
    pub variant: TransactionVariantV3,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub enum TransactionVariantV3 {
    DeclareV0(DeclareTransactionV0V1),
    DeclareV1(DeclareTransactionV0V1),
    DeclareV2(DeclareTransactionV2),
    DeclareV3(DeclareTransactionV3),
    DeclareV4(DeclareTransactionV4),
    DeployV0(DeployTransactionV0),
    DeployV1(DeployTransactionV1),
    DeployAccountV1(DeployAccountTransactionV1),
    DeployAccountV3(DeployAccountTransactionV3),
    DeployAccountV4(DeployAccountTransactionV4),
    InvokeV0(InvokeTransactionV0),
    InvokeV1(InvokeTransactionV1),
    InvokeV3(InvokeTransactionV3),
    InvokeV4(InvokeTransactionV4),
    InvokeV5(InvokeTransactionV5),
    L1HandlerV0(L1HandlerTransactionV0),
}

impl TransactionV3 {
    pub fn sender_address(&self) -> Option<&MinimalFelt> {
        self.variant.sender_address()
    }
    pub fn nonce(&self) -> Option<u64> {
        self.variant.nonce()
    }
    pub fn tip(&self) -> u64 {
        self.variant.tip()
    }
    pub fn tx_type(&self) -> &'static str {
        self.variant.tx_type()
    }
    pub fn calldata(&self) -> &[MinimalFelt] {
        self.variant.calldata()
    }
}

impl From<TransactionVariantV2> for TransactionVariantV3 {
    fn from(v: TransactionVariantV2) -> Self {
        match v {
            TransactionVariantV2::DeclareV0(tx) => Self::DeclareV0(tx),
            TransactionVariantV2::DeclareV1(tx) => Self::DeclareV1(tx),
            TransactionVariantV2::DeclareV2(tx) => Self::DeclareV2(tx),
            TransactionVariantV2::DeclareV3(tx) => Self::DeclareV3(tx),
            TransactionVariantV2::DeclareV4(tx) => Self::DeclareV4(tx),
            TransactionVariantV2::DeployV0(tx) => Self::DeployV0(tx),
            TransactionVariantV2::DeployV1(tx) => Self::DeployV1(tx),
            TransactionVariantV2::DeployAccountV1(tx) => Self::DeployAccountV1(tx),
            TransactionVariantV2::DeployAccountV3(tx) => Self::DeployAccountV3(tx),
            TransactionVariantV2::DeployAccountV4(tx) => Self::DeployAccountV4(tx),
            TransactionVariantV2::InvokeV0(tx) => Self::InvokeV0(tx),
            TransactionVariantV2::InvokeV1(tx) => Self::InvokeV1(tx),
            TransactionVariantV2::InvokeV3(tx) => Self::InvokeV3(tx),
            TransactionVariantV2::InvokeV4(tx) => Self::InvokeV4(tx),
            TransactionVariantV2::L1HandlerV0(tx) => Self::L1HandlerV0(tx),
        }
    }
}

impl From<TransactionV2> for TransactionV3 {
    fn from(v: TransactionV2) -> Self {
        Self {
            hash: v.hash,
            variant: v.variant.into(),
        }
    }
}

impl TransactionVariantV3 {
    pub fn sender_address(&self) -> Option<&MinimalFelt> {
        match self {
            Self::DeclareV0(tx) | Self::DeclareV1(tx) => Some(&tx.sender_address),
            Self::DeclareV2(tx) => Some(&tx.sender_address),
            Self::DeclareV3(tx) => Some(&tx.sender_address),
            Self::DeclareV4(tx) => Some(&tx.sender_address),
            Self::DeployV0(tx) => Some(&tx.contract_address),
            Self::DeployV1(tx) => Some(&tx.contract_address),
            Self::DeployAccountV1(tx) => Some(&tx.contract_address),
            Self::DeployAccountV3(tx) => Some(&tx.sender_address),
            Self::DeployAccountV4(tx) => Some(&tx.sender_address),
            Self::InvokeV0(tx) => Some(&tx.sender_address),
            Self::InvokeV1(tx) => Some(&tx.sender_address),
            Self::InvokeV3(tx) => Some(&tx.sender_address),
            Self::InvokeV4(tx) => Some(&tx.sender_address),
            Self::InvokeV5(tx) => Some(&tx.sender_address),
            Self::L1HandlerV0(_) => None,
        }
    }

    pub fn nonce(&self) -> Option<u64> {
        match self {
            Self::DeclareV0(tx) | Self::DeclareV1(tx) => Some(tx.nonce.as_u64()),
            Self::DeclareV2(tx) => Some(tx.nonce.as_u64()),
            Self::DeclareV3(tx) => Some(tx.nonce.as_u64()),
            Self::DeclareV4(tx) => Some(tx.nonce.as_u64()),
            Self::DeployV0(_) | Self::DeployV1(_) => None,
            Self::DeployAccountV1(tx) => Some(tx.nonce.as_u64()),
            Self::DeployAccountV3(tx) => Some(tx.nonce.as_u64()),
            Self::DeployAccountV4(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV0(_) => None,
            Self::InvokeV1(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV3(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV4(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV5(tx) => Some(tx.nonce.as_u64()),
            Self::L1HandlerV0(tx) => Some(tx.nonce.as_u64()),
        }
    }

    pub fn tip(&self) -> u64 {
        match self {
            Self::DeclareV3(tx) => tx.tip.0,
            Self::DeclareV4(tx) => tx.tip.0,
            Self::DeployAccountV3(tx) => tx.tip.0,
            Self::DeployAccountV4(tx) => tx.tip.0,
            Self::InvokeV3(tx) => tx.tip.0,
            Self::InvokeV4(tx) => tx.tip.0,
            Self::InvokeV5(tx) => tx.tip.0,
            _ => 0,
        }
    }

    pub fn tx_type(&self) -> &'static str {
        match self {
            Self::DeclareV0(_) => "DECLARE_V0",
            Self::DeclareV1(_) => "DECLARE_V1",
            Self::DeclareV2(_) => "DECLARE_V2",
            Self::DeclareV3(_) => "DECLARE_V3",
            Self::DeclareV4(_) => "DECLARE_V4",
            Self::DeployV0(_) => "DEPLOY_V0",
            Self::DeployV1(_) => "DEPLOY_V1",
            Self::DeployAccountV1(_) => "DEPLOY_ACCOUNT_V1",
            Self::DeployAccountV3(_) => "DEPLOY_ACCOUNT_V3",
            Self::DeployAccountV4(_) => "DEPLOY_ACCOUNT_V4",
            Self::InvokeV0(_) => "INVOKE_V0",
            Self::InvokeV1(_) => "INVOKE_V1",
            Self::InvokeV3(_) => "INVOKE_V3",
            Self::InvokeV4(_) => "INVOKE_V4",
            Self::InvokeV5(_) => "INVOKE_V5",
            Self::L1HandlerV0(_) => "L1_HANDLER",
        }
    }

    /// The calldata payload for this tx. Invoke txs carry the multicall
    /// calldata; L1Handler and DeployAccount carry their constructor/entry
    /// calldata. Declare/Deploy have no calldata payload → empty slice.
    pub fn calldata(&self) -> &[MinimalFelt] {
        match self {
            Self::InvokeV0(tx) => &tx.calldata,
            Self::InvokeV1(tx) => &tx.calldata,
            Self::InvokeV3(tx) => &tx.calldata,
            Self::InvokeV4(tx) => &tx.calldata,
            Self::InvokeV5(tx) => &tx.calldata,
            Self::L1HandlerV0(tx) => &tx.calldata,
            Self::DeployV0(tx) => &tx.constructor_calldata,
            Self::DeployV1(tx) => &tx.constructor_calldata,
            Self::DeployAccountV1(tx) => &tx.constructor_calldata,
            Self::DeployAccountV3(tx) => &tx.constructor_calldata,
            Self::DeployAccountV4(tx) => &tx.constructor_calldata,
            Self::DeclareV0(_)
            | Self::DeclareV1(_)
            | Self::DeclareV2(_)
            | Self::DeclareV3(_)
            | Self::DeclareV4(_) => &[],
        }
    }
}

// ---------------------------------------------------------------------------
// Convenience: extract sender + nonce + type from TransactionVariantV2
// ---------------------------------------------------------------------------

impl TransactionVariantV2 {
    /// Extract the sender address (if applicable to this tx type).
    pub fn sender_address(&self) -> Option<&MinimalFelt> {
        match self {
            Self::DeclareV0(tx) | Self::DeclareV1(tx) => Some(&tx.sender_address),
            Self::DeclareV2(tx) => Some(&tx.sender_address),
            Self::DeclareV3(tx) => Some(&tx.sender_address),
            Self::DeclareV4(tx) => Some(&tx.sender_address),
            Self::DeployV0(tx) => Some(&tx.contract_address),
            Self::DeployV1(tx) => Some(&tx.contract_address),
            Self::DeployAccountV1(tx) => Some(&tx.contract_address),
            Self::DeployAccountV3(tx) => Some(&tx.sender_address),
            Self::DeployAccountV4(tx) => Some(&tx.sender_address),
            Self::InvokeV0(tx) => Some(&tx.sender_address),
            Self::InvokeV1(tx) => Some(&tx.sender_address),
            Self::InvokeV3(tx) => Some(&tx.sender_address),
            Self::InvokeV4(tx) => Some(&tx.sender_address),
            Self::L1HandlerV0(_) => None,
        }
    }

    /// Extract the nonce (if present).
    pub fn nonce(&self) -> Option<u64> {
        match self {
            Self::DeclareV0(tx) | Self::DeclareV1(tx) => Some(tx.nonce.as_u64()),
            Self::DeclareV2(tx) => Some(tx.nonce.as_u64()),
            Self::DeclareV3(tx) => Some(tx.nonce.as_u64()),
            Self::DeclareV4(tx) => Some(tx.nonce.as_u64()),
            Self::DeployV0(_) | Self::DeployV1(_) => None,
            Self::DeployAccountV1(tx) => Some(tx.nonce.as_u64()),
            Self::DeployAccountV3(tx) => Some(tx.nonce.as_u64()),
            Self::DeployAccountV4(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV0(_) => None,
            Self::InvokeV1(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV3(tx) => Some(tx.nonce.as_u64()),
            Self::InvokeV4(tx) => Some(tx.nonce.as_u64()),
            Self::L1HandlerV0(tx) => Some(tx.nonce.as_u64()),
        }
    }

    /// Get the tip value (v3+ transactions only).
    pub fn tip(&self) -> u64 {
        match self {
            Self::DeclareV3(tx) => tx.tip.0,
            Self::DeclareV4(tx) => tx.tip.0,
            Self::DeployAccountV3(tx) => tx.tip.0,
            Self::DeployAccountV4(tx) => tx.tip.0,
            Self::InvokeV3(tx) => tx.tip.0,
            Self::InvokeV4(tx) => tx.tip.0,
            _ => 0,
        }
    }

    /// Human-readable transaction type string.
    pub fn tx_type(&self) -> &'static str {
        match self {
            Self::DeclareV0(_) => "DECLARE_V0",
            Self::DeclareV1(_) => "DECLARE_V1",
            Self::DeclareV2(_) => "DECLARE_V2",
            Self::DeclareV3(_) => "DECLARE_V3",
            Self::DeclareV4(_) => "DECLARE_V4",
            Self::DeployV0(_) => "DEPLOY_V0",
            Self::DeployV1(_) => "DEPLOY_V1",
            Self::DeployAccountV1(_) => "DEPLOY_ACCOUNT_V1",
            Self::DeployAccountV3(_) => "DEPLOY_ACCOUNT_V3",
            Self::DeployAccountV4(_) => "DEPLOY_ACCOUNT_V4",
            Self::InvokeV0(_) => "INVOKE_V0",
            Self::InvokeV1(_) => "INVOKE_V1",
            Self::InvokeV3(_) => "INVOKE_V3",
            Self::InvokeV4(_) => "INVOKE_V4",
            Self::L1HandlerV0(_) => "L1_HANDLER",
        }
    }
}

// ---------------------------------------------------------------------------
// Individual transaction type structs
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeclareTransactionV0V1 {
    pub class_hash: MinimalFelt,
    pub max_fee: MinimalFelt,
    pub nonce: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeclareTransactionV2 {
    pub class_hash: MinimalFelt,
    pub max_fee: MinimalFelt,
    pub nonce: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
    pub compiled_class_hash: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeclareTransactionV3 {
    pub class_hash: MinimalFelt,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV0,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub signature: Vec<MinimalFelt>,
    pub account_deployment_data: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
    pub compiled_class_hash: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeclareTransactionV4 {
    pub class_hash: MinimalFelt,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV1,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub signature: Vec<MinimalFelt>,
    pub account_deployment_data: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
    pub compiled_class_hash: MinimalFelt,
}

/// Old-style Deploy with version field (only in V0 block format).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeployTransaction {
    pub contract_address: MinimalFelt,
    pub version: MinimalFelt,
    pub contract_address_salt: MinimalFelt,
    pub class_hash: MinimalFelt,
    pub constructor_calldata: Vec<MinimalFelt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeployTransactionV0 {
    pub contract_address: MinimalFelt,
    pub contract_address_salt: MinimalFelt,
    pub class_hash: MinimalFelt,
    pub constructor_calldata: Vec<MinimalFelt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeployTransactionV1 {
    pub contract_address: MinimalFelt,
    pub contract_address_salt: MinimalFelt,
    pub class_hash: MinimalFelt,
    pub constructor_calldata: Vec<MinimalFelt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeployAccountTransactionV1 {
    pub contract_address: MinimalFelt,
    pub max_fee: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
    pub contract_address_salt: MinimalFelt,
    pub constructor_calldata: Vec<MinimalFelt>,
    pub class_hash: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeployAccountTransactionV3 {
    pub sender_address: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV0,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub contract_address_salt: MinimalFelt,
    pub constructor_calldata: Vec<MinimalFelt>,
    pub class_hash: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeployAccountTransactionV4 {
    pub sender_address: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV1,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub contract_address_salt: MinimalFelt,
    pub constructor_calldata: Vec<MinimalFelt>,
    pub class_hash: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvokeTransactionV0 {
    pub calldata: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
    pub entry_point_selector: MinimalFelt,
    pub entry_point_type: Option<EntryPointType>,
    pub max_fee: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvokeTransactionV1 {
    pub calldata: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
    pub max_fee: MinimalFelt,
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvokeTransactionV3 {
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV0,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub account_deployment_data: Vec<MinimalFelt>,
    pub calldata: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvokeTransactionV4 {
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV1,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub account_deployment_data: Vec<MinimalFelt>,
    pub calldata: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvokeTransactionV5 {
    pub signature: Vec<MinimalFelt>,
    pub nonce: MinimalFelt,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
    pub resource_bounds: ResourceBoundsV1,
    pub tip: Tip,
    pub paymaster_data: Vec<MinimalFelt>,
    pub account_deployment_data: Vec<MinimalFelt>,
    pub calldata: Vec<MinimalFelt>,
    pub sender_address: MinimalFelt,
    pub proof_facts: Vec<MinimalFelt>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct L1HandlerTransactionV0 {
    pub contract_address: MinimalFelt,
    pub entry_point_selector: MinimalFelt,
    pub nonce: MinimalFelt,
    pub calldata: Vec<MinimalFelt>,
}
