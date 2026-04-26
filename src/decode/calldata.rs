use starknet::core::types::Felt;

use super::abi::{AbiEnumDef, AbiStructDef, ParsedAbi};
use crate::utils::{felt_to_u64, felt_to_u128};

// ---------------------------------------------------------------------------
// Decoded value tree
// ---------------------------------------------------------------------------

/// A decoded calldata value. Mirrors Cairo's type system for display purposes.
#[derive(Debug, Clone)]
pub enum DecodedValue {
    Felt(Felt),
    Bool(bool),
    /// Unsigned integers that fit in a single felt (u8..u128).
    Uint(u128),
    /// Signed integers (i8..i128). Stored as the raw felt for display.
    Int(i128),
    /// u256 = (low: u128, high: u128).
    U256 {
        low: u128,
        high: u128,
    },
    /// ContractAddress / ClassHash / EthAddress / StorageAddress.
    Address(Felt),
    /// ByteArray decoded to a UTF-8 string (best-effort).
    String(String),
    /// Array or Span.
    Array(Vec<DecodedValue>),
    /// Named struct with fields.
    Struct {
        name: String,
        fields: Vec<(String, DecodedValue)>,
    },
    /// Enum variant.
    Enum {
        name: String,
        variant: String,
        value: Option<Box<DecodedValue>>,
    },
    /// Tuple.
    Tuple(Vec<DecodedValue>),
    /// Fallback: raw felt when type is unknown.
    Raw(Felt),
}

impl std::fmt::Display for DecodedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodedValue::Felt(v) => write!(f, "{:#x}", v),
            DecodedValue::Bool(b) => write!(f, "{b}"),
            DecodedValue::Uint(v) => {
                if *v > 1_000_000 {
                    write!(f, "{v} ({:#x})", v)
                } else {
                    write!(f, "{v}")
                }
            }
            DecodedValue::Int(v) => write!(f, "{v}"),
            DecodedValue::U256 { low, high } => {
                if *high == 0 {
                    if *low > 1_000_000 {
                        write!(f, "{low} ({:#x})", low)
                    } else {
                        write!(f, "{low}")
                    }
                } else if *low == u128::MAX && *high == u128::MAX {
                    write!(f, "U256::MAX")
                } else {
                    write!(f, "0x{high:x}{low:032x}")
                }
            }
            DecodedValue::Address(v) => write!(f, "{:#x}", v),
            DecodedValue::String(s) => write!(f, "\"{s}\""),
            DecodedValue::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    // Truncate display for large arrays
                    if i >= 5 && items.len() > 6 {
                        write!(f, "... +{} more", items.len() - i)?;
                        break;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            DecodedValue::Struct { name, fields } => {
                let short = short_name(name);
                write!(f, "{short} {{ ")?;
                for (i, (fname, val)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{fname}: {val}")?;
                }
                write!(f, " }}")
            }
            DecodedValue::Enum {
                name,
                variant,
                value,
            } => {
                let short = short_name(name);
                match value {
                    Some(v) => write!(f, "{short}::{variant}({v})"),
                    None => write!(f, "{short}::{variant}"),
                }
            }
            DecodedValue::Tuple(items) => {
                write!(f, "(")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ")")
            }
            DecodedValue::Raw(v) => write!(f, "{:#x}", v),
        }
    }
}

/// Extract the short name from a fully-qualified path.
fn short_name(full: &str) -> &str {
    full.rsplit("::").next().unwrap_or(full)
}

// ---------------------------------------------------------------------------
// Buffer reader (felt cursor)
// ---------------------------------------------------------------------------

struct BufferReader<'a> {
    buf: &'a [Felt],
    pos: usize,
}

impl<'a> BufferReader<'a> {
    fn new(buf: &'a [Felt]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_felt(&mut self) -> Option<Felt> {
        if self.pos < self.buf.len() {
            let f = self.buf[self.pos];
            self.pos += 1;
            Some(f)
        } else {
            None
        }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// A single decoded calldata parameter with optional name and type info.
#[derive(Debug, Clone)]
pub struct DecodedCallParam {
    pub name: Option<String>,
    pub type_name: Option<String>,
    pub value: DecodedValue,
}

/// Decode calldata felts using a function's ABI input definitions.
/// Returns a list of decoded parameters. Falls back to raw felts on failure.
pub fn decode_calldata(
    data: &[Felt],
    inputs: &[(String, String)], // (param_name, type_name) from FunctionDef
    abi: &ParsedAbi,
) -> Vec<DecodedCallParam> {
    let mut reader = BufferReader::new(data);
    let mut result = Vec::with_capacity(inputs.len());

    for (name, type_name) in inputs {
        if reader.remaining() == 0 {
            break;
        }
        let value = decode_type(type_name, abi, &mut reader);
        result.push(DecodedCallParam {
            name: Some(name.clone()),
            type_name: Some(type_name.clone()),
            value,
        });
    }

    // Any remaining felts that weren't consumed by the ABI definition
    while reader.remaining() > 0 {
        if let Some(f) = reader.read_felt() {
            result.push(DecodedCallParam {
                name: None,
                type_name: None,
                value: DecodedValue::Raw(f),
            });
        }
    }

    result
}

/// Decode return-value felts using a function's ABI output types.
///
/// Outputs are stored as types only (Cairo functions don't name their return
/// values), so each `DecodedCallParam` comes back with `name = None`. Trailing
/// felts that the ABI didn't account for are returned as raw entries, mirroring
/// `decode_calldata` so the UI can still surface them.
pub fn decode_results(data: &[Felt], outputs: &[String], abi: &ParsedAbi) -> Vec<DecodedCallParam> {
    let mut reader = BufferReader::new(data);
    let mut result = Vec::with_capacity(outputs.len());

    for type_name in outputs {
        if reader.remaining() == 0 {
            break;
        }
        let value = decode_type(type_name, abi, &mut reader);
        result.push(DecodedCallParam {
            name: None,
            type_name: Some(type_name.clone()),
            value,
        });
    }

    while reader.remaining() > 0 {
        if let Some(f) = reader.read_felt() {
            result.push(DecodedCallParam {
                name: None,
                type_name: None,
                value: DecodedValue::Raw(f),
            });
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Recursive type decoder
// ---------------------------------------------------------------------------

fn decode_type(type_str: &str, abi: &ParsedAbi, reader: &mut BufferReader) -> DecodedValue {
    let type_str = type_str.trim();

    // Tuple: "(type1, type2, ...)"
    if type_str.starts_with('(') && type_str.ends_with(')') {
        return decode_tuple(&type_str[1..type_str.len() - 1], abi, reader);
    }

    // Check for generic types: Array<T>, Span<T>, Option<T>
    if let Some((base, inner)) = split_generic(type_str) {
        return decode_generic(&base, &inner, abi, reader);
    }

    // Extract the last segment for primitive matching
    let last_segment = type_str.rsplit("::").next().unwrap_or(type_str);

    // Try primitive types first (matched by last path segment)
    if let Some(val) = decode_primitive(last_segment, reader) {
        return val;
    }

    // Look up as struct in ABI
    if let Some(struct_def) = abi.structs.get(type_str) {
        return decode_struct(struct_def, abi, reader);
    }

    // Look up as enum in ABI
    if let Some(enum_def) = abi.enums.get(type_str) {
        return decode_enum(enum_def, abi, reader);
    }

    // Fallback: consume one felt as raw
    reader
        .read_felt()
        .map(DecodedValue::Raw)
        .unwrap_or(DecodedValue::Raw(Felt::ZERO))
}

fn decode_primitive(last_segment: &str, reader: &mut BufferReader) -> Option<DecodedValue> {
    match last_segment {
        "felt" | "felt252" => Some(DecodedValue::Felt(reader.read_felt()?)),
        "bool" => {
            let f = reader.read_felt()?;
            Some(DecodedValue::Bool(f != Felt::ZERO))
        }
        "u8" | "u16" | "u32" | "u64" | "u128" => {
            let f = reader.read_felt()?;
            Some(DecodedValue::Uint(felt_to_u128(&f)))
        }
        "u256" => {
            let low = felt_to_u128(&reader.read_felt()?);
            let high = felt_to_u128(&reader.read_felt()?);
            Some(DecodedValue::U256 { low, high })
        }
        "i8" | "i16" | "i32" | "i64" | "i128" => {
            let f = reader.read_felt()?;
            // Cairo signed integers use two's complement in felt
            Some(DecodedValue::Int(felt_to_i128(&f)))
        }
        "ContractAddress" | "ClassHash" | "StorageAddress" | "EthAddress" => {
            Some(DecodedValue::Address(reader.read_felt()?))
        }
        "ByteArray" => Some(decode_byte_array(reader)),
        "bytes31" => {
            // bytes31 is a single felt
            Some(DecodedValue::Felt(reader.read_felt()?))
        }
        _ => None,
    }
}

fn decode_generic(
    base: &str,
    inner: &str,
    abi: &ParsedAbi,
    reader: &mut BufferReader,
) -> DecodedValue {
    // Normalize: match last segments
    let base_short = base.rsplit("::").next().unwrap_or(base);
    match base_short {
        "Array" | "Span" => {
            let len = reader
                .read_felt()
                .map(|f| felt_to_u64(&f) as usize)
                .unwrap_or(0);
            let mut items = Vec::with_capacity(len.min(1000)); // safety cap
            for _ in 0..len.min(1000) {
                if reader.remaining() == 0 {
                    break;
                }
                items.push(decode_type(inner, abi, reader));
            }
            DecodedValue::Array(items)
        }
        "Option" => {
            // Option is an enum with variants 0=Some(T), 1=None
            let variant_idx = reader.read_felt().map(|f| felt_to_u64(&f)).unwrap_or(0);
            if variant_idx == 0 {
                // Some(T)
                let val = decode_type(inner, abi, reader);
                DecodedValue::Enum {
                    name: "Option".to_string(),
                    variant: "Some".to_string(),
                    value: Some(Box::new(val)),
                }
            } else {
                DecodedValue::Enum {
                    name: "Option".to_string(),
                    variant: "None".to_string(),
                    value: None,
                }
            }
        }
        _ => {
            // Unknown generic — try as struct/enum in ABI with full generic name
            if let Some(struct_def) = abi.structs.get(base) {
                return decode_struct(struct_def, abi, reader);
            }
            if let Some(enum_def) = abi.enums.get(base) {
                return decode_enum(enum_def, abi, reader);
            }
            // Fallback
            reader
                .read_felt()
                .map(DecodedValue::Raw)
                .unwrap_or(DecodedValue::Raw(Felt::ZERO))
        }
    }
}

fn decode_struct(def: &AbiStructDef, abi: &ParsedAbi, reader: &mut BufferReader) -> DecodedValue {
    let mut fields = Vec::with_capacity(def.members.len());
    for (name, type_name) in &def.members {
        if reader.remaining() == 0 {
            break;
        }
        let val = decode_type(type_name, abi, reader);
        fields.push((name.clone(), val));
    }
    DecodedValue::Struct {
        name: def.name.clone(),
        fields,
    }
}

fn decode_enum(def: &AbiEnumDef, abi: &ParsedAbi, reader: &mut BufferReader) -> DecodedValue {
    let variant_idx = reader
        .read_felt()
        .map(|f| felt_to_u64(&f) as usize)
        .unwrap_or(0);

    let (variant_name, variant_type) = if variant_idx < def.variants.len() {
        let (name, ty) = &def.variants[variant_idx];
        (name.clone(), ty.as_str())
    } else {
        return DecodedValue::Raw(Felt::from(variant_idx as u64));
    };

    let value = if variant_type == "()" || variant_type.is_empty() {
        None
    } else {
        Some(Box::new(decode_type(variant_type, abi, reader)))
    };

    DecodedValue::Enum {
        name: def.name.clone(),
        variant: variant_name,
        value,
    }
}

fn decode_tuple(inner: &str, abi: &ParsedAbi, reader: &mut BufferReader) -> DecodedValue {
    let types = split_tuple_types(inner);
    let mut items = Vec::with_capacity(types.len());
    for ty in &types {
        if reader.remaining() == 0 {
            break;
        }
        items.push(decode_type(ty, abi, reader));
    }
    DecodedValue::Tuple(items)
}

fn decode_byte_array(reader: &mut BufferReader) -> DecodedValue {
    // ByteArray = { data: Array<felt252>, pending_word: felt252, pending_word_len: felt252 }
    // data array: length felt + N word felts (each 31 bytes)
    let num_words = reader
        .read_felt()
        .map(|f| felt_to_u64(&f) as usize)
        .unwrap_or(0);
    let mut bytes = Vec::new();

    for _ in 0..num_words.min(1000) {
        if let Some(f) = reader.read_felt() {
            // Each word is 31 bytes (big-endian in the felt)
            let b = f.to_bytes_be();
            bytes.extend_from_slice(&b[1..32]); // skip first byte (felt is 32 bytes, word is 31)
        }
    }

    let pending_word = reader.read_felt().unwrap_or(Felt::ZERO);
    let pending_len = reader
        .read_felt()
        .map(|f| felt_to_u64(&f) as usize)
        .unwrap_or(0);

    if pending_len > 0 && pending_len <= 31 {
        let b = pending_word.to_bytes_be();
        bytes.extend_from_slice(&b[32 - pending_len..32]);
    }

    match String::from_utf8(bytes) {
        Ok(s) => DecodedValue::String(s),
        Err(e) => {
            // Not valid UTF-8, show as hex
            let hex: String = e.into_bytes().iter().map(|b| format!("{b:02x}")).collect();
            DecodedValue::String(format!("0x{hex}"))
        }
    }
}

// ---------------------------------------------------------------------------
// Type string parsing helpers
// ---------------------------------------------------------------------------

/// Split a generic type string like "core::array::Array::<core::felt252>"
/// into (base, inner) = ("core::array::Array", "core::felt252").
/// Returns None if not a generic type.
fn split_generic(type_str: &str) -> Option<(String, String)> {
    // Find the first `<` that's part of the generic args
    // Handle both `Array::<T>` and `Array<T>` syntax
    let lt_pos = type_str.find('<')?;
    if !type_str.ends_with('>') {
        return None;
    }

    let mut base = type_str[..lt_pos].to_string();
    // Remove trailing "::" from "Array::<T>" form
    if base.ends_with("::") {
        base.truncate(base.len() - 2);
    }

    let inner = type_str[lt_pos + 1..type_str.len() - 1].to_string();
    Some((base, inner))
}

/// Split tuple inner types, respecting nested generics.
/// e.g. "core::felt252, core::array::Array::<u32>" → ["core::felt252", "core::array::Array::<u32>"]
fn split_tuple_types(inner: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in inner.char_indices() {
        match ch {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ',' if depth == 0 => {
                let segment = inner[start..i].trim();
                if !segment.is_empty() {
                    result.push(segment.to_string());
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty() {
        result.push(last.to_string());
    }
    result
}

// ---------------------------------------------------------------------------
// Felt conversion helpers
// ---------------------------------------------------------------------------

fn felt_to_i128(felt: &Felt) -> i128 {
    // Cairo signed integers: if the value > type::MAX, it's negative (two's complement)
    // For display purposes, show the raw u128 interpretation
    felt_to_u128(felt) as i128
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::abi::{AbiEnumDef, AbiStructDef, ParsedAbi};

    /// Helper: build a minimal ParsedAbi with optional structs/enums.
    fn empty_abi() -> ParsedAbi {
        ParsedAbi::default()
    }

    /// Helper: decode with given inputs and return the decoded params.
    fn decode(felts: &[Felt], inputs: &[(&str, &str)], abi: &ParsedAbi) -> Vec<DecodedCallParam> {
        let inputs: Vec<(String, String)> = inputs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect();
        decode_calldata(felts, &inputs, abi)
    }

    // ===== PRIMITIVE TYPES =====

    #[test]
    fn test_felt252() {
        let felts = [Felt::from(0x1234u64)];
        let result = decode(&felts, &[("x", "core::felt252")], &empty_abi());
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, DecodedValue::Felt(f) if *f == Felt::from(0x1234u64)));
    }

    #[test]
    fn test_felt_short_name() {
        // "felt252" without path prefix should also work
        let felts = [Felt::from(42u64)];
        let result = decode(&felts, &[("x", "felt252")], &empty_abi());
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, DecodedValue::Felt(_)));
    }

    #[test]
    fn test_bool_true() {
        let felts = [Felt::from(1u64)];
        let result = decode(&felts, &[("flag", "core::bool")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Bool(true)));
    }

    #[test]
    fn test_bool_false() {
        let felts = [Felt::ZERO];
        let result = decode(&felts, &[("flag", "core::bool")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Bool(false)));
    }

    #[test]
    fn test_u8() {
        let felts = [Felt::from(255u64)];
        let result = decode(&felts, &[("x", "core::integer::u8")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Uint(255)));
    }

    #[test]
    fn test_u16() {
        let felts = [Felt::from(65535u64)];
        let result = decode(&felts, &[("x", "core::integer::u16")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Uint(65535)));
    }

    #[test]
    fn test_u32() {
        let felts = [Felt::from(123456u64)];
        let result = decode(&felts, &[("x", "core::integer::u32")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Uint(123456)));
    }

    #[test]
    fn test_u64() {
        let felts = [Felt::from(9999999999u64)];
        let result = decode(&felts, &[("x", "core::integer::u64")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Uint(9999999999)));
    }

    #[test]
    fn test_u128() {
        let felts = [Felt::from(u128::MAX)];
        let result = decode(&felts, &[("x", "core::integer::u128")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Uint(v) if *v == u128::MAX));
    }

    #[test]
    fn test_u256() {
        // u256 = low u128, high u128
        let felts = [Felt::from(1000u64), Felt::from(0u64)];
        let result = decode(&felts, &[("amount", "core::integer::u256")], &empty_abi());
        assert!(matches!(
            &result[0].value,
            DecodedValue::U256 { low: 1000, high: 0 }
        ));
    }

    #[test]
    fn test_u256_large() {
        let felts = [Felt::from(u128::MAX), Felt::from(1u64)];
        let result = decode(&felts, &[("amount", "core::integer::u256")], &empty_abi());
        assert!(
            matches!(&result[0].value, DecodedValue::U256 { low, high } if *low == u128::MAX && *high == 1)
        );
    }

    #[test]
    fn test_u256_max() {
        let felts = [Felt::from(u128::MAX), Felt::from(u128::MAX)];
        let result = decode(&felts, &[("amount", "core::integer::u256")], &empty_abi());
        match &result[0].value {
            DecodedValue::U256 { low, high } => {
                assert_eq!(*low, u128::MAX);
                assert_eq!(*high, u128::MAX);
                assert_eq!(result[0].value.to_string(), "U256::MAX");
            }
            _ => panic!("Expected U256"),
        }
    }

    #[test]
    fn test_i8() {
        let felts = [Felt::from(42u64)];
        let result = decode(&felts, &[("x", "core::integer::i8")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Int(42)));
    }

    #[test]
    fn test_i128() {
        let felts = [Felt::from(100u64)];
        let result = decode(&felts, &[("x", "core::integer::i128")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Int(100)));
    }

    #[test]
    fn test_contract_address() {
        let addr = Felt::from(0xDEADu64);
        let felts = [addr];
        let result = decode(
            &felts,
            &[("to", "core::starknet::ContractAddress")],
            &empty_abi(),
        );
        assert!(matches!(&result[0].value, DecodedValue::Address(a) if *a == addr));
    }

    #[test]
    fn test_class_hash() {
        let hash = Felt::from(0xBEEFu64);
        let felts = [hash];
        let result = decode(
            &felts,
            &[("class", "core::starknet::ClassHash")],
            &empty_abi(),
        );
        assert!(matches!(&result[0].value, DecodedValue::Address(a) if *a == hash));
    }

    #[test]
    fn test_eth_address() {
        let addr = Felt::from(0xCAFEu64);
        let felts = [addr];
        let result = decode(
            &felts,
            &[("eth_addr", "core::starknet::EthAddress")],
            &empty_abi(),
        );
        assert!(matches!(&result[0].value, DecodedValue::Address(a) if *a == addr));
    }

    #[test]
    fn test_bytes31() {
        let val = Felt::from(0xABCDu64);
        let felts = [val];
        let result = decode(&felts, &[("b", "core::bytes_31::bytes31")], &empty_abi());
        assert!(matches!(&result[0].value, DecodedValue::Felt(f) if *f == val));
    }

    // ===== MULTIPLE PRIMITIVES =====

    #[test]
    fn test_multiple_params() {
        let felts = [
            Felt::from(0xAAAu64), // recipient (ContractAddress)
            Felt::from(1000u64),  // amount low (u256)
            Felt::from(0u64),     // amount high (u256)
        ];
        let result = decode(
            &felts,
            &[
                ("recipient", "core::starknet::ContractAddress"),
                ("amount", "core::integer::u256"),
            ],
            &empty_abi(),
        );
        assert_eq!(result.len(), 2);
        assert!(matches!(&result[0].value, DecodedValue::Address(_)));
        assert!(matches!(
            &result[1].value,
            DecodedValue::U256 { low: 1000, high: 0 }
        ));
    }

    // ===== ARRAY / SPAN =====

    #[test]
    fn test_empty_array() {
        let felts = [Felt::from(0u64)]; // length = 0
        let result = decode(
            &felts,
            &[("items", "core::array::Array::<core::felt252>")],
            &empty_abi(),
        );
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0].value, DecodedValue::Array(items) if items.is_empty()));
    }

    #[test]
    fn test_array_of_felts() {
        let felts = [
            Felt::from(3u64),    // length
            Felt::from(0x10u64), // [0]
            Felt::from(0x20u64), // [1]
            Felt::from(0x30u64), // [2]
        ];
        let result = decode(
            &felts,
            &[("data", "core::array::Array::<core::felt252>")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Array(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(&items[0], DecodedValue::Felt(f) if *f == Felt::from(0x10u64)));
                assert!(matches!(&items[1], DecodedValue::Felt(f) if *f == Felt::from(0x20u64)));
                assert!(matches!(&items[2], DecodedValue::Felt(f) if *f == Felt::from(0x30u64)));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_array_of_u256() {
        let felts = [
            Felt::from(2u64),   // length
            Felt::from(100u64), // [0].low
            Felt::from(0u64),   // [0].high
            Felt::from(200u64), // [1].low
            Felt::from(0u64),   // [1].high
        ];
        let result = decode(
            &felts,
            &[("amounts", "core::array::Array::<core::integer::u256>")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(
                    &items[0],
                    DecodedValue::U256 { low: 100, high: 0 }
                ));
                assert!(matches!(
                    &items[1],
                    DecodedValue::U256 { low: 200, high: 0 }
                ));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_span_of_addresses() {
        let addr1 = Felt::from(0xAAu64);
        let addr2 = Felt::from(0xBBu64);
        let felts = [
            Felt::from(2u64), // length
            addr1,
            addr2,
        ];
        let result = decode(
            &felts,
            &[(
                "addrs",
                "core::array::Span::<core::starknet::ContractAddress>",
            )],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], DecodedValue::Address(a) if *a == addr1));
                assert!(matches!(&items[1], DecodedValue::Address(a) if *a == addr2));
            }
            _ => panic!("Expected Array (Span)"),
        }
    }

    #[test]
    fn test_array_without_turbofish() {
        // Test "Array<felt252>" syntax (no :: before <>)
        let felts = [Felt::from(1u64), Felt::from(42u64)];
        let result = decode(
            &felts,
            &[("items", "core::array::Array<core::felt252>")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Array(items) => assert_eq!(items.len(), 1),
            _ => panic!("Expected Array"),
        }
    }

    // ===== STRUCTS =====

    #[test]
    fn test_simple_struct() {
        let mut abi = empty_abi();
        abi.structs.insert(
            "mymod::Point".to_string(),
            AbiStructDef {
                name: "mymod::Point".to_string(),
                members: vec![
                    ("x".to_string(), "core::felt252".to_string()),
                    ("y".to_string(), "core::felt252".to_string()),
                ],
            },
        );

        let felts = [Felt::from(10u64), Felt::from(20u64)];
        let result = decode(&felts, &[("point", "mymod::Point")], &abi);
        match &result[0].value {
            DecodedValue::Struct { name, fields } => {
                assert_eq!(name, "mymod::Point");
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "x");
                assert!(matches!(&fields[0].1, DecodedValue::Felt(f) if *f == Felt::from(10u64)));
                assert_eq!(fields[1].0, "y");
                assert!(matches!(&fields[1].1, DecodedValue::Felt(f) if *f == Felt::from(20u64)));
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_struct_with_u256_member() {
        let mut abi = empty_abi();
        abi.structs.insert(
            "mymod::Transfer".to_string(),
            AbiStructDef {
                name: "mymod::Transfer".to_string(),
                members: vec![
                    (
                        "to".to_string(),
                        "core::starknet::ContractAddress".to_string(),
                    ),
                    ("amount".to_string(), "core::integer::u256".to_string()),
                ],
            },
        );

        let to_addr = Felt::from(0xDEADu64);
        let felts = [to_addr, Felt::from(5000u64), Felt::ZERO];
        let result = decode(&felts, &[("transfer", "mymod::Transfer")], &abi);
        match &result[0].value {
            DecodedValue::Struct { fields, .. } => {
                assert_eq!(fields.len(), 2);
                assert!(matches!(&fields[0].1, DecodedValue::Address(a) if *a == to_addr));
                assert!(matches!(
                    &fields[1].1,
                    DecodedValue::U256 { low: 5000, high: 0 }
                ));
            }
            _ => panic!("Expected Struct"),
        }
    }

    #[test]
    fn test_nested_struct() {
        let mut abi = empty_abi();
        abi.structs.insert(
            "mymod::Inner".to_string(),
            AbiStructDef {
                name: "mymod::Inner".to_string(),
                members: vec![("val".to_string(), "core::felt252".to_string())],
            },
        );
        abi.structs.insert(
            "mymod::Outer".to_string(),
            AbiStructDef {
                name: "mymod::Outer".to_string(),
                members: vec![
                    ("a".to_string(), "core::integer::u32".to_string()),
                    ("inner".to_string(), "mymod::Inner".to_string()),
                    ("b".to_string(), "core::bool".to_string()),
                ],
            },
        );

        let felts = [
            Felt::from(42u64),   // a: u32
            Felt::from(0x99u64), // inner.val: felt252
            Felt::from(1u64),    // b: bool
        ];
        let result = decode(&felts, &[("data", "mymod::Outer")], &abi);
        match &result[0].value {
            DecodedValue::Struct { fields, .. } => {
                assert_eq!(fields.len(), 3);
                assert!(matches!(&fields[0].1, DecodedValue::Uint(42)));
                match &fields[1].1 {
                    DecodedValue::Struct {
                        fields: inner_fields,
                        ..
                    } => {
                        assert_eq!(inner_fields.len(), 1);
                        assert!(
                            matches!(&inner_fields[0].1, DecodedValue::Felt(f) if *f == Felt::from(0x99u64))
                        );
                    }
                    _ => panic!("Expected inner Struct"),
                }
                assert!(matches!(&fields[2].1, DecodedValue::Bool(true)));
            }
            _ => panic!("Expected Struct"),
        }
    }

    // ===== ENUMS =====

    #[test]
    fn test_enum_unit_variant() {
        let mut abi = empty_abi();
        abi.enums.insert(
            "mymod::Color".to_string(),
            AbiEnumDef {
                name: "mymod::Color".to_string(),
                variants: vec![
                    ("Red".to_string(), "()".to_string()),
                    ("Green".to_string(), "()".to_string()),
                    ("Blue".to_string(), "()".to_string()),
                ],
            },
        );

        // Select variant 1 = Green
        let felts = [Felt::from(1u64)];
        let result = decode(&felts, &[("color", "mymod::Color")], &abi);
        match &result[0].value {
            DecodedValue::Enum {
                name,
                variant,
                value,
            } => {
                assert_eq!(name, "mymod::Color");
                assert_eq!(variant, "Green");
                assert!(value.is_none());
            }
            _ => panic!("Expected Enum"),
        }
    }

    #[test]
    fn test_enum_with_payload() {
        let mut abi = empty_abi();
        abi.enums.insert(
            "mymod::Action".to_string(),
            AbiEnumDef {
                name: "mymod::Action".to_string(),
                variants: vec![
                    ("None".to_string(), "()".to_string()),
                    ("Transfer".to_string(), "core::integer::u256".to_string()),
                    ("Call".to_string(), "core::felt252".to_string()),
                ],
            },
        );

        // Select variant 1 = Transfer(u256)
        let felts = [
            Felt::from(1u64),   // variant index
            Felt::from(500u64), // u256 low
            Felt::from(0u64),   // u256 high
        ];
        let result = decode(&felts, &[("action", "mymod::Action")], &abi);
        match &result[0].value {
            DecodedValue::Enum { variant, value, .. } => {
                assert_eq!(variant, "Transfer");
                assert!(value.is_some());
                let inner = value.as_ref().unwrap();
                assert!(matches!(
                    inner.as_ref(),
                    DecodedValue::U256 { low: 500, high: 0 }
                ));
            }
            _ => panic!("Expected Enum"),
        }
    }

    #[test]
    fn test_option_some() {
        let felts = [Felt::from(0u64), Felt::from(42u64)];
        let result = decode(
            &felts,
            &[("maybe", "core::option::Option::<core::felt252>")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Enum {
                name,
                variant,
                value,
            } => {
                assert_eq!(name, "Option");
                assert_eq!(variant, "Some");
                assert!(value.is_some());
                let inner = value.as_ref().unwrap();
                assert!(matches!(inner.as_ref(), DecodedValue::Felt(f) if *f == Felt::from(42u64)));
            }
            _ => panic!("Expected Option::Some"),
        }
    }

    #[test]
    fn test_option_none() {
        let felts = [Felt::from(1u64)];
        let result = decode(
            &felts,
            &[("maybe", "core::option::Option::<core::felt252>")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Enum { variant, value, .. } => {
                assert_eq!(variant, "None");
                assert!(value.is_none());
            }
            _ => panic!("Expected Option::None"),
        }
    }

    // ===== TUPLES =====

    #[test]
    fn test_tuple() {
        let felts = [Felt::from(10u64), Felt::from(20u64)];
        let result = decode(
            &felts,
            &[("pair", "(core::felt252, core::integer::u32)")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Tuple(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], DecodedValue::Felt(f) if *f == Felt::from(10u64)));
                assert!(matches!(&items[1], DecodedValue::Uint(20)));
            }
            _ => panic!("Expected Tuple"),
        }
    }

    #[test]
    fn test_tuple_with_address() {
        let addr = Felt::from(0xCAFEu64);
        let felts = [addr, Felt::from(100u64), Felt::ZERO]; // (addr, u256)
        let result = decode(
            &felts,
            &[(
                "pair",
                "(core::starknet::ContractAddress, core::integer::u256)",
            )],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Tuple(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], DecodedValue::Address(a) if *a == addr));
                assert!(matches!(
                    &items[1],
                    DecodedValue::U256 { low: 100, high: 0 }
                ));
            }
            _ => panic!("Expected Tuple"),
        }
    }

    // ===== BYTEARRAY =====

    #[test]
    fn test_byte_array_short() {
        // "hello" = 5 bytes, fits in pending_word
        // data: length=0, pending_word=0x68656c6c6f, pending_word_len=5
        let felts = [
            Felt::from(0u64),                        // data array length = 0
            Felt::from_hex("0x68656c6c6f").unwrap(), // pending_word = "hello"
            Felt::from(5u64),                        // pending_word_len = 5
        ];
        let result = decode(
            &felts,
            &[("name", "core::byte_array::ByteArray")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::String(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected String, got {:?}", result[0].value),
        }
    }

    #[test]
    fn test_byte_array_empty() {
        let felts = [Felt::ZERO, Felt::ZERO, Felt::ZERO]; // length=0, pending_word=0, pending_len=0
        let result = decode(
            &felts,
            &[("s", "core::byte_array::ByteArray")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::String(s) => assert_eq!(s, ""),
            _ => panic!("Expected empty String"),
        }
    }

    #[test]
    fn test_byte_array_with_full_words() {
        // "ABCDEFGHIJKLMNOPQRSTUVWXYZ01234" = 31 bytes (1 full word) + "56789" = 5 more bytes
        // word = 0x4142434445464748494a4b4c4d4e4f505152535455565758595a3031323334
        // pending = 0x3536373839, pending_len = 5
        let word =
            Felt::from_hex("0x4142434445464748494a4b4c4d4e4f505152535455565758595a3031323334")
                .unwrap();
        let pending = Felt::from_hex("0x3536373839").unwrap();
        let felts = [
            Felt::from(1u64), // 1 full word
            word,
            pending,
            Felt::from(5u64),
        ];
        let result = decode(
            &felts,
            &[("s", "core::byte_array::ByteArray")],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::String(s) => {
                assert_eq!(s, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
            }
            _ => panic!("Expected String"),
        }
    }

    // ===== COMPLEX / MIXED =====

    #[test]
    fn test_array_of_structs() {
        let mut abi = empty_abi();
        abi.structs.insert(
            "mymod::Pair".to_string(),
            AbiStructDef {
                name: "mymod::Pair".to_string(),
                members: vec![
                    ("key".to_string(), "core::felt252".to_string()),
                    ("value".to_string(), "core::integer::u32".to_string()),
                ],
            },
        );

        let felts = [
            Felt::from(2u64),    // array length
            Felt::from(0xAAu64), // [0].key
            Felt::from(100u64),  // [0].value
            Felt::from(0xBBu64), // [1].key
            Felt::from(200u64),  // [1].value
        ];
        let result = decode(
            &felts,
            &[("pairs", "core::array::Array::<mymod::Pair>")],
            &abi,
        );
        match &result[0].value {
            DecodedValue::Array(items) => {
                assert_eq!(items.len(), 2);
                match &items[0] {
                    DecodedValue::Struct { fields, .. } => {
                        assert_eq!(fields[0].0, "key");
                        assert_eq!(fields[1].0, "value");
                        assert!(matches!(&fields[1].1, DecodedValue::Uint(100)));
                    }
                    _ => panic!("Expected Struct in array"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_enum_with_struct_payload() {
        let mut abi = empty_abi();
        abi.structs.insert(
            "mymod::Data".to_string(),
            AbiStructDef {
                name: "mymod::Data".to_string(),
                members: vec![
                    ("x".to_string(), "core::integer::u32".to_string()),
                    ("y".to_string(), "core::integer::u32".to_string()),
                ],
            },
        );
        abi.enums.insert(
            "mymod::Msg".to_string(),
            AbiEnumDef {
                name: "mymod::Msg".to_string(),
                variants: vec![
                    ("Empty".to_string(), "()".to_string()),
                    ("WithData".to_string(), "mymod::Data".to_string()),
                ],
            },
        );

        let felts = [
            Felt::from(1u64),  // variant = WithData
            Felt::from(10u64), // Data.x
            Felt::from(20u64), // Data.y
        ];
        let result = decode(&felts, &[("msg", "mymod::Msg")], &abi);
        match &result[0].value {
            DecodedValue::Enum { variant, value, .. } => {
                assert_eq!(variant, "WithData");
                match value.as_ref().unwrap().as_ref() {
                    DecodedValue::Struct { fields, .. } => {
                        assert_eq!(fields.len(), 2);
                        assert!(matches!(&fields[0].1, DecodedValue::Uint(10)));
                        assert!(matches!(&fields[1].1, DecodedValue::Uint(20)));
                    }
                    _ => panic!("Expected Struct payload"),
                }
            }
            _ => panic!("Expected Enum"),
        }
    }

    // ===== REALISTIC ERC20 CALLDATA =====

    #[test]
    fn test_erc20_transfer_calldata() {
        // transfer(recipient: ContractAddress, amount: u256) -> bool
        let recipient =
            Felt::from_hex("0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
                .unwrap();
        let amount_low = Felt::from(1_000_000_000_000_000_000u128); // 1 token (18 decimals)
        let amount_high = Felt::ZERO;
        let felts = [recipient, amount_low, amount_high];
        let inputs = [
            ("recipient", "core::starknet::ContractAddress"),
            ("amount", "core::integer::u256"),
        ];
        let result = decode(&felts, &inputs, &empty_abi());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name.as_deref(), Some("recipient"));
        assert!(matches!(&result[0].value, DecodedValue::Address(a) if *a == recipient));
        assert_eq!(result[1].name.as_deref(), Some("amount"));
        assert!(
            matches!(&result[1].value, DecodedValue::U256 { low, high } if *low == 1_000_000_000_000_000_000u128 && *high == 0)
        );
    }

    #[test]
    fn test_erc20_approve_calldata() {
        // approve(spender: ContractAddress, amount: u256) -> bool
        let spender = Felt::from(0xABCDu64);
        let felts = [spender, Felt::from(u128::MAX), Felt::from(u128::MAX)]; // unlimited approval
        let inputs = [
            ("spender", "core::starknet::ContractAddress"),
            ("amount", "core::integer::u256"),
        ];
        let result = decode(&felts, &inputs, &empty_abi());
        assert_eq!(result.len(), 2);
        match &result[1].value {
            DecodedValue::U256 { low, high } => {
                assert_eq!(*low, u128::MAX);
                assert_eq!(*high, u128::MAX);
            }
            _ => panic!("Expected U256"),
        }
    }

    // ===== EDGE CASES =====

    #[test]
    fn test_empty_calldata() {
        let result = decode(&[], &[("x", "core::felt252")], &empty_abi());
        // No felts to consume → no params decoded
        assert!(result.is_empty());
    }

    #[test]
    fn test_extra_felts_become_raw() {
        let felts = [Felt::from(1u64), Felt::from(2u64), Felt::from(3u64)];
        let result = decode(&felts, &[("x", "core::felt252")], &empty_abi());
        assert_eq!(result.len(), 3);
        assert!(matches!(&result[0].value, DecodedValue::Felt(_)));
        assert!(matches!(&result[1].value, DecodedValue::Raw(_)));
        assert!(matches!(&result[2].value, DecodedValue::Raw(_)));
    }

    #[test]
    fn test_no_inputs_all_raw() {
        let felts = [Felt::from(1u64), Felt::from(2u64)];
        let result = decode(&felts, &[], &empty_abi());
        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .all(|p| matches!(&p.value, DecodedValue::Raw(_)))
        );
    }

    #[test]
    fn test_unknown_type_fallback() {
        let felts = [Felt::from(99u64)];
        let result = decode(&felts, &[("x", "some::unknown::Type")], &empty_abi());
        assert_eq!(result.len(), 1);
        // Unknown type → raw felt consumed
        assert!(matches!(&result[0].value, DecodedValue::Raw(_)));
    }

    #[test]
    fn test_enum_invalid_variant_index() {
        let mut abi = empty_abi();
        abi.enums.insert(
            "mymod::Small".to_string(),
            AbiEnumDef {
                name: "mymod::Small".to_string(),
                variants: vec![("A".to_string(), "()".to_string())],
            },
        );

        let felts = [Felt::from(99u64)]; // variant 99 doesn't exist
        let result = decode(&felts, &[("x", "mymod::Small")], &abi);
        // Should fall back to Raw
        assert!(matches!(&result[0].value, DecodedValue::Raw(_)));
    }

    // ===== TYPE STRING PARSING =====

    #[test]
    fn test_split_generic_turbofish() {
        let result = split_generic("core::array::Array::<core::felt252>");
        assert_eq!(
            result,
            Some((
                "core::array::Array".to_string(),
                "core::felt252".to_string()
            ))
        );
    }

    #[test]
    fn test_split_generic_no_turbofish() {
        let result = split_generic("core::array::Array<core::felt252>");
        assert_eq!(
            result,
            Some((
                "core::array::Array".to_string(),
                "core::felt252".to_string()
            ))
        );
    }

    #[test]
    fn test_split_generic_nested() {
        let result = split_generic("core::array::Array::<core::array::Array::<core::felt252>>");
        assert!(result.is_some());
        let (base, inner) = result.unwrap();
        assert_eq!(base, "core::array::Array");
        assert_eq!(inner, "core::array::Array::<core::felt252>");
    }

    #[test]
    fn test_split_generic_not_generic() {
        assert!(split_generic("core::felt252").is_none());
        assert!(split_generic("core::bool").is_none());
    }

    #[test]
    fn test_split_tuple_types_simple() {
        let result = split_tuple_types("core::felt252, core::integer::u32");
        assert_eq!(result, vec!["core::felt252", "core::integer::u32"]);
    }

    #[test]
    fn test_split_tuple_types_with_generic() {
        let result = split_tuple_types("core::felt252, core::array::Array::<core::integer::u32>");
        assert_eq!(
            result,
            vec!["core::felt252", "core::array::Array::<core::integer::u32>"]
        );
    }

    #[test]
    fn test_split_tuple_types_nested_tuple() {
        let result = split_tuple_types("core::felt252, (core::integer::u32, core::bool)");
        assert_eq!(
            result,
            vec!["core::felt252", "(core::integer::u32, core::bool)"]
        );
    }

    // ===== NESTED ARRAYS =====

    #[test]
    fn test_nested_array() {
        // Array<Array<u32>>
        let felts = [
            Felt::from(2u64),  // outer length
            Felt::from(2u64),  // inner[0] length
            Felt::from(10u64), // inner[0][0]
            Felt::from(20u64), // inner[0][1]
            Felt::from(1u64),  // inner[1] length
            Felt::from(30u64), // inner[1][0]
        ];
        let result = decode(
            &felts,
            &[(
                "matrix",
                "core::array::Array::<core::array::Array::<core::integer::u32>>",
            )],
            &empty_abi(),
        );
        match &result[0].value {
            DecodedValue::Array(outer) => {
                assert_eq!(outer.len(), 2);
                match &outer[0] {
                    DecodedValue::Array(inner) => {
                        assert_eq!(inner.len(), 2);
                        assert!(matches!(&inner[0], DecodedValue::Uint(10)));
                        assert!(matches!(&inner[1], DecodedValue::Uint(20)));
                    }
                    _ => panic!("Expected inner Array"),
                }
                match &outer[1] {
                    DecodedValue::Array(inner) => {
                        assert_eq!(inner.len(), 1);
                        assert!(matches!(&inner[0], DecodedValue::Uint(30)));
                    }
                    _ => panic!("Expected inner Array"),
                }
            }
            _ => panic!("Expected outer Array"),
        }
    }

    // ===== DISPLAY FORMAT =====

    #[test]
    fn test_display_bool() {
        assert_eq!(DecodedValue::Bool(true).to_string(), "true");
        assert_eq!(DecodedValue::Bool(false).to_string(), "false");
    }

    #[test]
    fn test_display_uint_small() {
        assert_eq!(DecodedValue::Uint(42).to_string(), "42");
    }

    #[test]
    fn test_display_uint_large() {
        let display = DecodedValue::Uint(2_000_000).to_string();
        assert!(display.contains("2000000"));
        assert!(display.contains("0x")); // should show hex too
    }

    #[test]
    fn test_display_u256_max() {
        let val = DecodedValue::U256 {
            low: u128::MAX,
            high: u128::MAX,
        };
        assert_eq!(val.to_string(), "U256::MAX");
    }

    #[test]
    fn test_display_string() {
        assert_eq!(
            DecodedValue::String("hello".to_string()).to_string(),
            "\"hello\""
        );
    }

    #[test]
    fn test_display_array() {
        let val = DecodedValue::Array(vec![DecodedValue::Uint(1), DecodedValue::Uint(2)]);
        assert_eq!(val.to_string(), "[1, 2]");
    }

    #[test]
    fn test_display_enum_unit() {
        let val = DecodedValue::Enum {
            name: "mymod::Color".to_string(),
            variant: "Red".to_string(),
            value: None,
        };
        assert_eq!(val.to_string(), "Color::Red");
    }

    #[test]
    fn test_display_enum_with_value() {
        let val = DecodedValue::Enum {
            name: "Option".to_string(),
            variant: "Some".to_string(),
            value: Some(Box::new(DecodedValue::Uint(42))),
        };
        assert_eq!(val.to_string(), "Option::Some(42)");
    }

    #[test]
    fn test_display_tuple() {
        let val = DecodedValue::Tuple(vec![DecodedValue::Uint(1), DecodedValue::Bool(true)]);
        assert_eq!(val.to_string(), "(1, true)");
    }

    #[test]
    fn test_display_struct() {
        let val = DecodedValue::Struct {
            name: "mymod::Point".to_string(),
            fields: vec![
                ("x".to_string(), DecodedValue::Uint(10)),
                ("y".to_string(), DecodedValue::Uint(20)),
            ],
        };
        assert_eq!(val.to_string(), "Point { x: 10, y: 20 }");
    }

    // ===== FELT CONSUMPTION CORRECTNESS =====

    #[test]
    fn test_felt_consumption_multi_param() {
        // Ensure the buffer advances correctly across multiple parameters
        // transfer(recipient: ContractAddress, amount: u256, salt: felt252)
        let felts = [
            Felt::from(0xAu64),  // recipient
            Felt::from(100u64),  // amount low
            Felt::from(0u64),    // amount high
            Felt::from(0xFFu64), // salt
        ];
        let inputs = [
            ("recipient", "core::starknet::ContractAddress"),
            ("amount", "core::integer::u256"),
            ("salt", "core::felt252"),
        ];
        let result = decode(&felts, &inputs, &empty_abi());
        assert_eq!(result.len(), 3);
        assert!(matches!(&result[0].value, DecodedValue::Address(a) if *a == Felt::from(0xAu64)));
        assert!(matches!(
            &result[1].value,
            DecodedValue::U256 { low: 100, high: 0 }
        ));
        assert!(matches!(&result[2].value, DecodedValue::Felt(f) if *f == Felt::from(0xFFu64)));
    }

    #[test]
    fn test_array_then_scalar() {
        // Ensure array consumes the right number of felts, leaving the rest for the next param
        let felts = [
            Felt::from(2u64),    // array length
            Felt::from(0xAu64),  // array[0]
            Felt::from(0xBu64),  // array[1]
            Felt::from(0xFFu64), // scalar after array
        ];
        let inputs = [
            ("items", "core::array::Array::<core::felt252>"),
            ("extra", "core::felt252"),
        ];
        let result = decode(&felts, &inputs, &empty_abi());
        assert_eq!(result.len(), 2);
        match &result[0].value {
            DecodedValue::Array(items) => assert_eq!(items.len(), 2),
            _ => panic!("Expected Array"),
        }
        assert!(matches!(&result[1].value, DecodedValue::Felt(f) if *f == Felt::from(0xFFu64)));
    }

    // ===== COMPLEX STRUCT (mirrors starknet-foundry's test_complex_struct) =====

    #[test]
    fn test_complex_struct_all_types() {
        // ComplexStruct {
        //   a: NestedStructWithField { a: SimpleStruct { a: felt252 }, b: felt252 },
        //   b: felt252,
        //   c: u8,
        //   d: i32,
        //   e: Enum (variant Two = u128),
        //   f: ByteArray ("seven"),
        //   g: Array<felt252>,
        //   h: u256,
        //   i: (i128, u128)
        // }
        let mut abi = empty_abi();
        abi.structs.insert(
            "pkg::SimpleStruct".to_string(),
            AbiStructDef {
                name: "pkg::SimpleStruct".to_string(),
                members: vec![("a".to_string(), "core::felt252".to_string())],
            },
        );
        abi.structs.insert(
            "pkg::NestedStructWithField".to_string(),
            AbiStructDef {
                name: "pkg::NestedStructWithField".to_string(),
                members: vec![
                    ("a".to_string(), "pkg::SimpleStruct".to_string()),
                    ("b".to_string(), "core::felt252".to_string()),
                ],
            },
        );
        abi.enums.insert(
            "pkg::Enum".to_string(),
            AbiEnumDef {
                name: "pkg::Enum".to_string(),
                variants: vec![
                    ("One".to_string(), "()".to_string()),
                    ("Two".to_string(), "core::integer::u128".to_string()),
                    (
                        "Three".to_string(),
                        "pkg::NestedStructWithField".to_string(),
                    ),
                ],
            },
        );
        abi.structs.insert(
            "pkg::ComplexStruct".to_string(),
            AbiStructDef {
                name: "pkg::ComplexStruct".to_string(),
                members: vec![
                    ("a".to_string(), "pkg::NestedStructWithField".to_string()),
                    ("b".to_string(), "core::felt252".to_string()),
                    ("c".to_string(), "core::integer::u8".to_string()),
                    ("d".to_string(), "core::integer::i32".to_string()),
                    ("e".to_string(), "pkg::Enum".to_string()),
                    ("f".to_string(), "core::byte_array::ByteArray".to_string()),
                    (
                        "g".to_string(),
                        "core::array::Array::<core::felt252>".to_string(),
                    ),
                    ("h".to_string(), "core::integer::u256".to_string()),
                    (
                        "i".to_string(),
                        "(core::integer::i128, core::integer::u128)".to_string(),
                    ),
                ],
            },
        );

        let felts = [
            // a: NestedStructWithField { a: SimpleStruct { a: 0x1 }, b: 0x2 }
            Felt::from(0x1u64),
            Felt::from(0x2u64),
            // b: felt252 = 0x3
            Felt::from(0x3u64),
            // c: u8 = 4
            Felt::from(4u64),
            // d: i32 = 5
            Felt::from(5u64),
            // e: Enum::Two(6_u128) → variant index 1, then 6
            Felt::from(1u64),
            Felt::from(6u64),
            // f: ByteArray "seven" → data_len=0, pending_word=0x736576656e, pending_len=5
            Felt::from(0u64),
            Felt::from_hex("0x736576656e").unwrap(),
            Felt::from(5u64),
            // g: Array<felt252> = [0x8, 0x9] → length=2
            Felt::from(2u64),
            Felt::from(0x8u64),
            Felt::from(0x9u64),
            // h: u256 = 10 → low=10, high=0
            Felt::from(10u64),
            Felt::from(0u64),
            // i: (i128, u128) = (11, 12)
            Felt::from(11u64),
            Felt::from(12u64),
        ];

        let result = decode(&felts, &[("s", "pkg::ComplexStruct")], &abi);
        assert_eq!(
            result.len(),
            1,
            "Should decode as a single struct parameter"
        );

        match &result[0].value {
            DecodedValue::Struct { name, fields } => {
                assert_eq!(name, "pkg::ComplexStruct");
                assert_eq!(fields.len(), 9, "ComplexStruct has 9 fields");

                // a: NestedStructWithField
                assert_eq!(fields[0].0, "a");
                match &fields[0].1 {
                    DecodedValue::Struct { fields: nested, .. } => {
                        assert_eq!(nested.len(), 2);
                        match &nested[0].1 {
                            DecodedValue::Struct { fields: inner, .. } => {
                                assert!(
                                    matches!(&inner[0].1, DecodedValue::Felt(f) if *f == Felt::from(0x1u64))
                                );
                            }
                            _ => panic!("Expected inner SimpleStruct"),
                        }
                        assert!(
                            matches!(&nested[1].1, DecodedValue::Felt(f) if *f == Felt::from(0x2u64))
                        );
                    }
                    _ => panic!("Expected NestedStruct for field a"),
                }

                // b: felt252
                assert_eq!(fields[1].0, "b");
                assert!(matches!(&fields[1].1, DecodedValue::Felt(f) if *f == Felt::from(0x3u64)));

                // c: u8
                assert_eq!(fields[2].0, "c");
                assert!(matches!(&fields[2].1, DecodedValue::Uint(4)));

                // d: i32
                assert_eq!(fields[3].0, "d");
                assert!(matches!(&fields[3].1, DecodedValue::Int(5)));

                // e: Enum::Two(6)
                assert_eq!(fields[4].0, "e");
                match &fields[4].1 {
                    DecodedValue::Enum { variant, value, .. } => {
                        assert_eq!(variant, "Two");
                        assert!(matches!(
                            value.as_ref().unwrap().as_ref(),
                            DecodedValue::Uint(6)
                        ));
                    }
                    _ => panic!("Expected Enum for field e"),
                }

                // f: ByteArray "seven"
                assert_eq!(fields[5].0, "f");
                match &fields[5].1 {
                    DecodedValue::String(s) => assert_eq!(s, "seven"),
                    _ => panic!("Expected String for field f, got {:?}", fields[5].1),
                }

                // g: Array [0x8, 0x9]
                assert_eq!(fields[6].0, "g");
                match &fields[6].1 {
                    DecodedValue::Array(items) => {
                        assert_eq!(items.len(), 2);
                        assert!(
                            matches!(&items[0], DecodedValue::Felt(f) if *f == Felt::from(0x8u64))
                        );
                        assert!(
                            matches!(&items[1], DecodedValue::Felt(f) if *f == Felt::from(0x9u64))
                        );
                    }
                    _ => panic!("Expected Array for field g"),
                }

                // h: u256 = 10
                assert_eq!(fields[7].0, "h");
                assert!(matches!(
                    &fields[7].1,
                    DecodedValue::U256 { low: 10, high: 0 }
                ));

                // i: (i128, u128) = (11, 12)
                assert_eq!(fields[8].0, "i");
                match &fields[8].1 {
                    DecodedValue::Tuple(items) => {
                        assert_eq!(items.len(), 2);
                        assert!(matches!(&items[0], DecodedValue::Int(11)));
                        assert!(matches!(&items[1], DecodedValue::Uint(12)));
                    }
                    _ => panic!("Expected Tuple for field i"),
                }
            }
            _ => panic!("Expected Struct"),
        }
    }

    // ===== ENUM IN TUPLE (mirrors foundry's test_tuple_enum_nested_struct) =====

    #[test]
    fn test_tuple_with_enum_and_nested_struct() {
        let mut abi = empty_abi();
        abi.structs.insert(
            "pkg::SimpleStruct".to_string(),
            AbiStructDef {
                name: "pkg::SimpleStruct".to_string(),
                members: vec![("a".to_string(), "core::felt252".to_string())],
            },
        );
        abi.structs.insert(
            "pkg::NestedStructWithField".to_string(),
            AbiStructDef {
                name: "pkg::NestedStructWithField".to_string(),
                members: vec![
                    ("a".to_string(), "pkg::SimpleStruct".to_string()),
                    ("b".to_string(), "core::felt252".to_string()),
                ],
            },
        );
        abi.enums.insert(
            "pkg::Enum".to_string(),
            AbiEnumDef {
                name: "pkg::Enum".to_string(),
                variants: vec![
                    ("One".to_string(), "()".to_string()),
                    ("Two".to_string(), "core::integer::u128".to_string()),
                    (
                        "Three".to_string(),
                        "pkg::NestedStructWithField".to_string(),
                    ),
                ],
            },
        );

        // (felt252, u8, Enum::Three(NestedStructWithField { a: SimpleStruct { a: 0x159 }, b: 0x1c8 }))
        let felts = [
            Felt::from(123u64), // felt252
            Felt::from(234u64), // u8
            Felt::from(2u64),   // Enum variant index = Three
            Felt::from(345u64), // NestedStruct.a.a
            Felt::from(456u64), // NestedStruct.b
        ];
        let result = decode(
            &felts,
            &[("t", "(core::felt252, core::integer::u8, pkg::Enum)")],
            &abi,
        );
        match &result[0].value {
            DecodedValue::Tuple(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(&items[0], DecodedValue::Felt(f) if *f == Felt::from(123u64)));
                assert!(matches!(&items[1], DecodedValue::Uint(234)));
                match &items[2] {
                    DecodedValue::Enum { variant, value, .. } => {
                        assert_eq!(variant, "Three");
                        match value.as_ref().unwrap().as_ref() {
                            DecodedValue::Struct { fields, .. } => {
                                assert_eq!(fields.len(), 2);
                                // a: SimpleStruct { a: 0x159 }
                                match &fields[0].1 {
                                    DecodedValue::Struct { fields: inner, .. } => {
                                        assert!(
                                            matches!(&inner[0].1, DecodedValue::Felt(f) if *f == Felt::from(345u64))
                                        );
                                    }
                                    _ => panic!("Expected inner struct"),
                                }
                                // b: 0x1c8
                                assert!(
                                    matches!(&fields[1].1, DecodedValue::Felt(f) if *f == Felt::from(456u64))
                                );
                            }
                            _ => panic!("Expected struct in enum"),
                        }
                    }
                    _ => panic!("Expected Enum"),
                }
            }
            _ => panic!("Expected Tuple"),
        }
    }

    // ===== COMPLEX FUNCTION WITH MULTIPLE PARAMS (mirrors foundry's test_happy_case_complex_function) =====

    #[test]
    fn test_complex_function_multi_params() {
        // complex_fn(
        //   a: Array<Array<felt252>>,
        //   b: u8,
        //   c: i16,
        //   d: ByteArray,
        //   e: (felt252, u32),
        //   f: bool,
        //   g: u256,
        // )
        let felts = [
            // a: Array<Array<felt252>> = [[0x2137, 0x420], [0x420, 0x2137]]
            Felt::from(2u64), // outer length
            Felt::from(2u64), // inner[0] length
            Felt::from(0x2137u64),
            Felt::from(0x420u64),
            Felt::from(2u64), // inner[1] length
            Felt::from(0x420u64),
            Felt::from(0x2137u64),
            // b: u8 = 8
            Felt::from(8u64),
            // c: i16 = -270 (as felt: PRIME - 270)
            Felt::from_hex("0x800000000000010fffffffffffffffffffffffffffffffffffffffffffffef3")
                .unwrap(),
            // d: ByteArray "some_string"
            Felt::from(0u64),                                    // data_len = 0
            Felt::from_hex("0x736f6d655f737472696e67").unwrap(), // pending_word
            Felt::from(11u64),                                   // pending_len
            // e: (felt252, u32) = (0x73686f727420737472696e67, 100)
            Felt::from_hex("0x73686f727420737472696e67").unwrap(),
            Felt::from(100u64),
            // f: bool = true
            Felt::from(1u64),
            // g: u256 = MAX
            Felt::from(u128::MAX),
            Felt::from(u128::MAX),
        ];

        let inputs = [
            (
                "a",
                "core::array::Array::<core::array::Array::<core::felt252>>",
            ),
            ("b", "core::integer::u8"),
            ("c", "core::integer::i16"),
            ("d", "core::byte_array::ByteArray"),
            ("e", "(core::felt252, core::integer::u32)"),
            ("f", "core::bool"),
            ("g", "core::integer::u256"),
        ];

        let result = decode(&felts, &inputs, &empty_abi());
        assert_eq!(result.len(), 7, "Should decode all 7 parameters");

        // a: Array<Array<felt252>>
        match &result[0].value {
            DecodedValue::Array(outer) => {
                assert_eq!(outer.len(), 2);
                match &outer[0] {
                    DecodedValue::Array(inner) => {
                        assert_eq!(inner.len(), 2);
                        assert!(
                            matches!(&inner[0], DecodedValue::Felt(f) if *f == Felt::from(0x2137u64))
                        );
                        assert!(
                            matches!(&inner[1], DecodedValue::Felt(f) if *f == Felt::from(0x420u64))
                        );
                    }
                    _ => panic!("Expected inner array"),
                }
            }
            _ => panic!("Expected outer array"),
        }

        // b: u8 = 8
        assert!(matches!(&result[1].value, DecodedValue::Uint(8)));

        // c: i16 — the felt is PRIME-270, which as u128 is huge; our decoder stores as i128 cast
        assert!(matches!(&result[2].value, DecodedValue::Int(_)));

        // d: ByteArray "some_string"
        match &result[3].value {
            DecodedValue::String(s) => assert_eq!(s, "some_string"),
            _ => panic!("Expected String"),
        }

        // e: (felt252, u32)
        match &result[4].value {
            DecodedValue::Tuple(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[1], DecodedValue::Uint(100)));
            }
            _ => panic!("Expected Tuple"),
        }

        // f: bool = true
        assert!(matches!(&result[5].value, DecodedValue::Bool(true)));

        // g: u256 = MAX
        match &result[6].value {
            DecodedValue::U256 { low, high } => {
                assert_eq!(*low, u128::MAX);
                assert_eq!(*high, u128::MAX);
            }
            _ => panic!("Expected U256"),
        }
    }

    // ===== NO ARGUMENTS =====

    #[test]
    fn test_no_arguments() {
        let result = decode(&[], &[], &empty_abi());
        assert!(result.is_empty());
    }

    // ===== EKUBO SWAP (real-world: nested structs spanning multiple felts) =====
    // tx: 0x3c90444de7e99f03986f11481b1e035d375101a857181225ed63e78b9e4146b
    // swap(node: RouteNode, token_amount: TokenAmount) → 11 felts total
    //
    // RouteNode { pool_key: PoolKey { token0, token1, fee, tick_spacing, extension }, sqrt_ratio_limit: u256, skip_ahead: u128 }
    // TokenAmount { token: ContractAddress, amount: i129 { mag: u128, sign: bool } }
    #[test]
    fn test_ekubo_swap_nested_structs() {
        let mut abi = empty_abi();

        // i129: { mag: u128, sign: bool }
        abi.structs.insert(
            "ekubo::types::i129::i129".to_string(),
            AbiStructDef {
                name: "ekubo::types::i129::i129".to_string(),
                members: vec![
                    ("mag".to_string(), "core::integer::u128".to_string()),
                    ("sign".to_string(), "core::bool".to_string()),
                ],
            },
        );

        // PoolKey: { token0, token1, fee, tick_spacing, extension }
        abi.structs.insert(
            "ekubo::types::pool_key::PoolKey".to_string(),
            AbiStructDef {
                name: "ekubo::types::pool_key::PoolKey".to_string(),
                members: vec![
                    (
                        "token0".to_string(),
                        "core::starknet::ContractAddress".to_string(),
                    ),
                    (
                        "token1".to_string(),
                        "core::starknet::ContractAddress".to_string(),
                    ),
                    ("fee".to_string(), "core::integer::u128".to_string()),
                    (
                        "tick_spacing".to_string(),
                        "core::integer::u128".to_string(),
                    ),
                    (
                        "extension".to_string(),
                        "core::starknet::ContractAddress".to_string(),
                    ),
                ],
            },
        );

        // RouteNode: { pool_key: PoolKey, sqrt_ratio_limit: u256, skip_ahead: u128 }
        abi.structs.insert(
            "ekubo::types::pool_key::RouteNode".to_string(),
            AbiStructDef {
                name: "ekubo::types::pool_key::RouteNode".to_string(),
                members: vec![
                    (
                        "pool_key".to_string(),
                        "ekubo::types::pool_key::PoolKey".to_string(),
                    ),
                    (
                        "sqrt_ratio_limit".to_string(),
                        "core::integer::u256".to_string(),
                    ),
                    ("skip_ahead".to_string(), "core::integer::u128".to_string()),
                ],
            },
        );

        // TokenAmount: { token: ContractAddress, amount: i129 }
        abi.structs.insert(
            "ekubo::types::delta::TokenAmount".to_string(),
            AbiStructDef {
                name: "ekubo::types::delta::TokenAmount".to_string(),
                members: vec![
                    (
                        "token".to_string(),
                        "core::starknet::ContractAddress".to_string(),
                    ),
                    ("amount".to_string(), "ekubo::types::i129::i129".to_string()),
                ],
            },
        );

        // Felts from the real tx (11 total):
        // RouteNode (8 felts): pool_key(token0, token1, fee, tick_spacing, extension), sqrt_ratio_limit(low, high), skip_ahead
        let token0 =
            Felt::from_hex("0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
                .unwrap();
        let token1 =
            Felt::from_hex("0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8")
                .unwrap();
        let fee = Felt::from_hex("0x20c49ba5e353f80000000000000000").unwrap();
        let tick_spacing = Felt::from(0x3e8u64);
        let extension = Felt::ZERO;
        let sqrt_ratio_low = Felt::from_hex("0x72f696befb234e407dd5fb056bd").unwrap();
        let sqrt_ratio_high = Felt::ZERO;
        let skip_ahead = Felt::ZERO;
        // TokenAmount (3 felts): token, amount.mag, amount.sign
        let ta_token =
            Felt::from_hex("0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8")
                .unwrap();
        let ta_mag = Felt::from_hex("0xbe92a72").unwrap();
        let ta_sign = Felt::ZERO; // false = positive

        let felts = [
            token0,
            token1,
            fee,
            tick_spacing,
            extension,
            sqrt_ratio_low,
            sqrt_ratio_high,
            skip_ahead,
            ta_token,
            ta_mag,
            ta_sign,
        ];
        let inputs = [
            ("node", "ekubo::types::pool_key::RouteNode"),
            ("token_amount", "ekubo::types::delta::TokenAmount"),
        ];
        let result = decode(&felts, &inputs, &abi);

        // Should decode to exactly 2 named params with no leftover "?" params
        assert_eq!(
            result.len(),
            2,
            "Expected 2 params, got {} (struct decode failed)",
            result.len()
        );
        assert_eq!(result[0].name.as_deref(), Some("node"));
        assert_eq!(result[1].name.as_deref(), Some("token_amount"));

        // node should be a Struct, not a Raw felt
        match &result[0].value {
            DecodedValue::Struct { name, fields } => {
                assert!(name.contains("RouteNode"));
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].0, "pool_key");
                // pool_key itself should be a struct
                match &fields[0].1 {
                    DecodedValue::Struct {
                        name: pk_name,
                        fields: pk_fields,
                    } => {
                        assert!(pk_name.contains("PoolKey"));
                        assert_eq!(pk_fields.len(), 5);
                        assert!(
                            matches!(&pk_fields[0].1, DecodedValue::Address(a) if *a == token0)
                        );
                        assert!(
                            matches!(&pk_fields[1].1, DecodedValue::Address(a) if *a == token1)
                        );
                    }
                    _ => panic!("Expected pool_key to be a Struct"),
                }
                assert_eq!(fields[1].0, "sqrt_ratio_limit");
                assert!(matches!(&fields[1].1, DecodedValue::U256 { .. }));
                assert_eq!(fields[2].0, "skip_ahead");
            }
            _ => panic!("Expected node to be a Struct, got {:?}", result[0].value),
        }

        // token_amount should be a Struct
        match &result[1].value {
            DecodedValue::Struct { name, fields } => {
                assert!(name.contains("TokenAmount"));
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "token");
                assert!(matches!(&fields[0].1, DecodedValue::Address(a) if *a == ta_token));
                assert_eq!(fields[1].0, "amount");
                // i129 nested struct
                match &fields[1].1 {
                    DecodedValue::Struct {
                        name: i129_name,
                        fields: i129_fields,
                    } => {
                        assert!(i129_name.contains("i129"));
                        assert_eq!(i129_fields.len(), 2);
                        assert!(matches!(&i129_fields[0].1, DecodedValue::Uint(_))); // mag
                        assert!(matches!(&i129_fields[1].1, DecodedValue::Bool(false))); // sign
                    }
                    _ => panic!("Expected amount to be i129 Struct"),
                }
            }
            _ => panic!(
                "Expected token_amount to be a Struct, got {:?}",
                result[1].value
            ),
        }
    }

    // ===== RESULT (RETURN-VALUE) DECODING =====

    fn decode_res(felts: &[Felt], outputs: &[&str], abi: &ParsedAbi) -> Vec<DecodedCallParam> {
        let outputs: Vec<String> = outputs.iter().map(|s| s.to_string()).collect();
        decode_results(felts, &outputs, abi)
    }

    #[test]
    fn test_results_single_bool() {
        // ERC20::transfer returns bool
        let felts = [Felt::from(1u64)];
        let result = decode_res(&felts, &["core::bool"], &empty_abi());
        assert_eq!(result.len(), 1);
        assert!(result[0].name.is_none());
        assert_eq!(result[0].type_name.as_deref(), Some("core::bool"));
        assert!(matches!(&result[0].value, DecodedValue::Bool(true)));
    }

    #[test]
    fn test_results_single_u256() {
        // ERC20::balanceOf returns u256
        let felts = [Felt::from(1_000u64), Felt::ZERO];
        let result = decode_res(&felts, &["core::integer::u256"], &empty_abi());
        assert_eq!(result.len(), 1);
        assert!(matches!(
            &result[0].value,
            DecodedValue::U256 { low: 1000, high: 0 }
        ));
    }

    #[test]
    fn test_results_multiple_outputs() {
        // Two scalar outputs
        let felts = [Felt::from(7u64), Felt::from(42u64)];
        let result = decode_res(
            &felts,
            &["core::integer::u64", "core::integer::u64"],
            &empty_abi(),
        );
        assert_eq!(result.len(), 2);
        assert!(matches!(&result[0].value, DecodedValue::Uint(7)));
        assert!(matches!(&result[1].value, DecodedValue::Uint(42)));
    }

    #[test]
    fn test_results_empty_outputs_falls_back_to_raw() {
        // No declared outputs but result felts present — preserve them as raw entries
        let felts = [Felt::from(0xabcu64), Felt::from(0xdefu64)];
        let result = decode_res(&felts, &[], &empty_abi());
        assert_eq!(result.len(), 2);
        assert!(matches!(&result[0].value, DecodedValue::Raw(_)));
        assert!(matches!(&result[1].value, DecodedValue::Raw(_)));
    }

    #[test]
    fn test_results_no_outputs_no_data() {
        let result = decode_res(&[], &[], &empty_abi());
        assert!(result.is_empty());
    }
}
