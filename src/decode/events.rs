use starknet::core::types::Felt;

use super::abi::ParsedAbi;
use crate::data::types::SnEvent;

/// A decoded event with human-readable names where available.
#[derive(Debug, Clone)]
pub struct DecodedEvent {
    pub contract_address: Felt,
    pub event_name: Option<String>,
    pub decoded_keys: Vec<DecodedParam>,
    pub decoded_data: Vec<DecodedParam>,
    pub raw: SnEvent,
}

/// A single decoded parameter.
#[derive(Debug, Clone)]
pub struct DecodedParam {
    pub name: Option<String>,
    pub type_name: Option<String>,
    /// Low 128 bits (or full value for non-u256 types).
    pub value: Felt,
    /// High 128 bits, only set for `u256` parameters (which span 2 felts on-chain).
    pub value_high: Option<Felt>,
}

/// Decode a raw event using a parsed ABI.
/// The first key of a Starknet event is the event selector.
pub fn decode_event(event: &SnEvent, abi: Option<&ParsedAbi>) -> DecodedEvent {
    let selector = event.keys.first();
    let event_def = selector.and_then(|s| abi.and_then(|a| a.get_event(s)));

    let event_name = event_def.map(|d| d.name.clone());

    // Decode keys: skip first key (selector), match remaining against ABI key defs.
    let decoded_keys = decode_params(
        &event.keys[1..], // skip selector
        event_def.map(|d| &d.keys),
    );

    // Decode data: match against ABI data defs.
    let decoded_data = decode_params(&event.data, event_def.map(|d| &d.data));

    DecodedEvent {
        contract_address: event.from_address,
        event_name,
        decoded_keys,
        decoded_data,
        raw: event.clone(),
    }
}

/// Decode a list of felt values against optional ABI parameter definitions.
///
/// u256 parameters serialize as two consecutive felts (low u128, high u128).
/// When the ABI definition says a param is u256, we consume both and store the
/// high word in `DecodedParam::value_high`.
fn decode_params(values: &[Felt], defs: Option<&Vec<(String, String)>>) -> Vec<DecodedParam> {
    let mut result = Vec::new();
    let mut value_idx = 0;

    if let Some(defs) = defs {
        for (name, type_name) in defs {
            if value_idx >= values.len() {
                break;
            }
            let val = values[value_idx];
            value_idx += 1;

            // u256 on Starknet = two felts: [low: u128, high: u128]
            let is_u256 = type_name.contains("u256");
            let value_high = if is_u256 && value_idx < values.len() {
                let high = values[value_idx];
                value_idx += 1;
                Some(high)
            } else {
                None
            };

            result.push(DecodedParam {
                name: Some(name.clone()),
                type_name: Some(type_name.clone()),
                value: val,
                value_high,
            });
        }
        // Remaining values with no matching ABI definition
        while value_idx < values.len() {
            result.push(DecodedParam {
                name: None,
                type_name: None,
                value: values[value_idx],
                value_high: None,
            });
            value_idx += 1;
        }
    } else {
        for val in values {
            result.push(DecodedParam {
                name: None,
                type_name: None,
                value: *val,
                value_high: None,
            });
        }
    }

    result
}

/// Group decoded events by emitting contract address.
pub fn group_events_by_contract(events: &[DecodedEvent]) -> Vec<ContractEvents> {
    let mut map: Vec<(Felt, Vec<&DecodedEvent>)> = Vec::new();

    for event in events {
        if let Some(entry) = map
            .iter_mut()
            .find(|(addr, _)| *addr == event.contract_address)
        {
            entry.1.push(event);
        } else {
            map.push((event.contract_address, vec![event]));
        }
    }

    map.into_iter()
        .map(|(address, events)| ContractEvents {
            contract_address: address,
            events: events.into_iter().cloned().collect(),
        })
        .collect()
}

/// Events grouped under a single contract.
#[derive(Debug, Clone)]
pub struct ContractEvents {
    pub contract_address: Felt,
    pub events: Vec<DecodedEvent>,
}
