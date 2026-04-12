use std::collections::HashMap;

use serde::Deserialize;
use starknet::core::types::{
    CompressedLegacyContractClass, ContractClass, Felt, FlattenedSierraClass,
    LegacyContractAbiEntry,
};
use starknet::core::utils::get_selector_from_name;
use tracing::{debug, warn};

/// Bumped whenever new fields are added that old cache entries lack.
/// Entries with a lower version are re-fetched and re-parsed.
pub const ABI_SCHEMA_VERSION: u32 = 1;

/// Parsed ABI ready for selector-based lookups.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ParsedAbi {
    /// Schema version — used to invalidate stale cache entries.
    #[serde(default)]
    pub schema_version: u32,
    /// function selector → definition
    pub functions: HashMap<FeltKey, FunctionDef>,
    /// event selector → definition
    pub events: HashMap<FeltKey, EventDef>,
    /// fully-qualified name → struct definition (for calldata decoding)
    #[serde(default)]
    pub structs: HashMap<String, AbiStructDef>,
    /// fully-qualified name → enum definition (for calldata decoding)
    #[serde(default)]
    pub enums: HashMap<String, AbiEnumDef>,
}

/// Wrapper around Felt for use as HashMap key with serde support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeltKey(pub Felt);

impl serde::Serialize for FeltKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{:#x}", self.0))
    }
}

impl<'de> serde::Deserialize<'de> for FeltKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Felt::from_hex(&s)
            .map(FeltKey)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FunctionDef {
    pub name: String,
    pub inputs: Vec<(String, String)>, // (param_name, type_name)
    pub outputs: Vec<String>,
    pub state_mutability: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EventDef {
    pub name: String,
    pub keys: Vec<(String, String)>, // (param_name, type_name)
    pub data: Vec<(String, String)>,
}

/// A struct definition from the ABI (for calldata decoding).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AbiStructDef {
    pub name: String,
    pub members: Vec<(String, String)>, // (member_name, type_name)
}

/// An enum definition from the ABI (for calldata decoding).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AbiEnumDef {
    pub name: String,
    pub variants: Vec<(String, String)>, // (variant_name, type_name)
}

impl ParsedAbi {
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty() && self.events.is_empty()
    }

    /// Look up a function by its selector.
    pub fn get_function(&self, selector: &Felt) -> Option<&FunctionDef> {
        self.functions.get(&FeltKey(*selector))
    }

    /// Look up an event by its selector (first key).
    pub fn get_event(&self, selector: &Felt) -> Option<&EventDef> {
        self.events.get(&FeltKey(*selector))
    }
}

/// Parse a `ContractClass` (from starknet-rust `get_class`) into a `ParsedAbi`.
pub fn parse_contract_class(class: &ContractClass) -> ParsedAbi {
    match class {
        ContractClass::Sierra(sierra) => parse_sierra_abi(sierra),
        ContractClass::Legacy(legacy) => parse_legacy_abi(legacy),
    }
}

/// Parse a Sierra (Cairo 1+) class ABI. The ABI is a JSON string.
fn parse_sierra_abi(class: &FlattenedSierraClass) -> ParsedAbi {
    let mut abi = ParsedAbi::default();

    let items: Vec<SierraAbiItem> = match serde_json::from_str(&class.abi) {
        Ok(items) => items,
        Err(e) => {
            warn!(error = %e, "Failed to parse Sierra ABI JSON");
            return abi;
        }
    };

    for item in &items {
        match item {
            SierraAbiItem::Function(f) | SierraAbiItem::L1Handler(f) => {
                if let Ok(selector) = get_selector_from_name(&f.name) {
                    abi.functions.insert(
                        FeltKey(selector),
                        FunctionDef {
                            name: f.name.clone(),
                            inputs: f
                                .inputs
                                .iter()
                                .map(|p| (p.name.clone(), p.r#type.clone()))
                                .collect(),
                            outputs: f.outputs.iter().map(|p| p.r#type.clone()).collect(),
                            state_mutability: f.state_mutability.clone(),
                        },
                    );
                }
            }
            SierraAbiItem::Event(e) => {
                // For events, the selector is computed from the event name.
                // Sierra events can be nested (enum variants), use the leaf name.
                let event_name = extract_event_name(&e.name);
                if let Ok(selector) = get_selector_from_name(&event_name) {
                    let (keys, data) = e.keys_and_data();
                    abi.events.insert(
                        FeltKey(selector),
                        EventDef {
                            name: event_name,
                            keys,
                            data,
                        },
                    );
                }
            }
            SierraAbiItem::Interface(iface) => {
                // Interfaces contain functions — parse them too.
                for f in &iface.items {
                    if let SierraAbiItem::Function(func) | SierraAbiItem::L1Handler(func) = f {
                        if let Ok(selector) = get_selector_from_name(&func.name) {
                            abi.functions.insert(
                                FeltKey(selector),
                                FunctionDef {
                                    name: func.name.clone(),
                                    inputs: func
                                        .inputs
                                        .iter()
                                        .map(|p| (p.name.clone(), p.r#type.clone()))
                                        .collect(),
                                    outputs: func
                                        .outputs
                                        .iter()
                                        .map(|p| p.r#type.clone())
                                        .collect(),
                                    state_mutability: func.state_mutability.clone(),
                                },
                            );
                        }
                    }
                }
            }
            SierraAbiItem::Struct(s) => {
                abi.structs.insert(
                    s.name.clone(),
                    AbiStructDef {
                        name: s.name.clone(),
                        members: s
                            .members
                            .iter()
                            .map(|m| (m.name.clone(), m.r#type.clone()))
                            .collect(),
                    },
                );
            }
            SierraAbiItem::Enum(e) => {
                abi.enums.insert(
                    e.name.clone(),
                    AbiEnumDef {
                        name: e.name.clone(),
                        variants: e
                            .variants
                            .iter()
                            .map(|v| (v.name.clone(), v.r#type.clone()))
                            .collect(),
                    },
                );
            }
            _ => {} // Constructors, impls — skip
        }
    }

    abi.schema_version = ABI_SCHEMA_VERSION;
    debug!(
        functions = abi.functions.len(),
        events = abi.events.len(),
        structs = abi.structs.len(),
        enums = abi.enums.len(),
        "Parsed Sierra ABI"
    );
    abi
}

/// Parse a legacy (Cairo 0) class ABI.
fn parse_legacy_abi(class: &CompressedLegacyContractClass) -> ParsedAbi {
    let mut abi = ParsedAbi::default();

    let entries = match &class.abi {
        Some(entries) => entries,
        None => return abi,
    };

    for entry in entries {
        match entry {
            LegacyContractAbiEntry::Function(f) => {
                if let Ok(selector) = get_selector_from_name(&f.name) {
                    abi.functions.insert(
                        FeltKey(selector),
                        FunctionDef {
                            name: f.name.clone(),
                            inputs: f
                                .inputs
                                .iter()
                                .map(|p| (p.name.clone(), p.r#type.clone()))
                                .collect(),
                            outputs: f.outputs.iter().map(|p| p.r#type.clone()).collect(),
                            state_mutability: f
                                .state_mutability
                                .as_ref()
                                .map(|s| format!("{:?}", s)),
                        },
                    );
                }
            }
            LegacyContractAbiEntry::Event(e) => {
                if let Ok(selector) = get_selector_from_name(&e.name) {
                    abi.events.insert(
                        FeltKey(selector),
                        EventDef {
                            name: e.name.clone(),
                            keys: e
                                .keys
                                .iter()
                                .map(|p| (p.name.clone(), p.r#type.clone()))
                                .collect(),
                            data: e
                                .data
                                .iter()
                                .map(|p| (p.name.clone(), p.r#type.clone()))
                                .collect(),
                        },
                    );
                }
            }
            LegacyContractAbiEntry::Struct(s) => {
                abi.structs.insert(
                    s.name.clone(),
                    AbiStructDef {
                        name: s.name.clone(),
                        members: s
                            .members
                            .iter()
                            .map(|m| (m.name.clone(), m.r#type.clone()))
                            .collect(),
                    },
                );
            }
        }
    }

    abi.schema_version = ABI_SCHEMA_VERSION;
    debug!(
        functions = abi.functions.len(),
        events = abi.events.len(),
        "Parsed legacy ABI"
    );
    abi
}

// --- Sierra ABI JSON types ---
// The Sierra ABI is a JSON array of these items.

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
enum SierraAbiItem {
    #[serde(rename = "function")]
    Function(SierraFunctionDef),
    #[serde(rename = "l1_handler")]
    L1Handler(SierraFunctionDef),
    #[serde(rename = "constructor")]
    Constructor(SierraFunctionDef),
    #[serde(rename = "event")]
    Event(SierraEventDef),
    #[serde(rename = "struct")]
    Struct(SierraStructDef),
    #[serde(rename = "enum")]
    Enum(SierraEnumDef),
    #[serde(rename = "interface")]
    Interface(SierraInterfaceDef),
    #[serde(rename = "impl")]
    Impl(SierraImplDef),
}

#[derive(Debug, Clone, Deserialize)]
struct SierraFunctionDef {
    name: String,
    #[serde(default)]
    inputs: Vec<SierraParam>,
    #[serde(default)]
    outputs: Vec<SierraOutput>,
    state_mutability: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraParam {
    name: String,
    r#type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraOutput {
    r#type: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraEventDef {
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    members: Vec<SierraEventMember>,
    #[serde(default)]
    variants: Vec<SierraEventVariant>,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraEventMember {
    name: String,
    r#type: String,
    kind: String, // "key" or "data"
}

#[derive(Debug, Clone, Deserialize)]
struct SierraEventVariant {
    name: String,
    r#type: String,
    kind: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraStructDef {
    name: String,
    #[serde(default)]
    members: Vec<SierraParam>,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraEnumDef {
    name: String,
    #[serde(default)]
    variants: Vec<SierraParam>,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraInterfaceDef {
    name: String,
    #[serde(default)]
    items: Vec<SierraAbiItem>,
}

#[derive(Debug, Clone, Deserialize)]
struct SierraImplDef {
    name: String,
    interface_name: String,
}

/// Extract the short event name from a fully-qualified Sierra path.
/// e.g. "openzeppelin::token::erc20::erc20::ERC20Component::Transfer" → "Transfer"
fn extract_event_name(full_name: &str) -> String {
    full_name
        .rsplit("::")
        .next()
        .unwrap_or(full_name)
        .to_string()
}

/// Parse a Sierra ABI's event definition into keys and data vectors,
/// looking at members directly.
impl SierraEventDef {
    fn keys_and_data(&self) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let mut keys = Vec::new();
        let mut data = Vec::new();
        for m in &self.members {
            let pair = (m.name.clone(), m.r#type.clone());
            match m.kind.as_str() {
                "key" => keys.push(pair),
                "data" => data.push(pair),
                _ => data.push(pair),
            }
        }
        (keys, data)
    }
}
