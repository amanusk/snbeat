use ratatui::text::Span;
use starknet::core::types::Felt;

use crate::app::state::TxNavItem;
use crate::decode::events::DecodedParam;
use crate::registry::AddressRegistry;
use crate::ui::theme;
use crate::ui::widgets::address_color::AddressColorMap;

/// Format a decoded parameter for display, using the registry for address labels
/// and token decimals where available.
///
/// `contract_address` is the address of the contract that emitted the event —
/// used to look up token decimals for u256 amount fields.
pub fn format_param(
    p: &DecodedParam,
    contract_address: &Felt,
    registry: Option<&AddressRegistry>,
    format_addr: &dyn Fn(&Felt) -> String,
    expand: bool,
) -> String {
    let value_str = format_param_value(p, contract_address, registry, format_addr, expand);
    match &p.name {
        Some(name) => format!("{name}: {value_str}"),
        None => value_str,
    }
}

/// Like `format_param` but returns `Vec<Span<'static>>` so ContractAddress params
/// can be rendered with their assigned palette color from `color_map`.
/// When `selected` is `Some(TxNavItem::Address(a))` and matches the param value,
/// the span is highlighted with the visual-mode selection style.
pub fn format_param_styled(
    p: &DecodedParam,
    contract_address: &Felt,
    registry: Option<&AddressRegistry>,
    color_map: &AddressColorMap,
    selected: Option<&TxNavItem>,
    format_addr: &dyn Fn(&Felt) -> String,
    expand: bool,
) -> Vec<Span<'static>> {
    let type_name = p.type_name.as_deref().unwrap_or("");
    let name_prefix = p
        .name
        .as_ref()
        .map(|n| format!("{n}: "))
        .unwrap_or_default();

    // Render as labeled address if type is ContractAddress, or if the value
    // resolves to a known label (handles untyped params like UDC event data).
    let is_address_type = type_name.contains("ContractAddress");
    let is_known_address = !is_address_type
        && type_name.is_empty()
        && p.value != Felt::ZERO
        && registry.is_some_and(|r| r.resolve(&p.value).is_some());
    if is_address_type || is_known_address {
        let label = format_addr(&p.value);
        let style = if matches!(selected, Some(TxNavItem::Address(a)) if *a == p.value) {
            theme::VISUAL_SELECTED_STYLE
        } else {
            color_map.style_for(&p.value)
        };
        return vec![Span::raw(name_prefix), Span::styled(label, style)];
    }

    // All other types: compute the string value and return unstyled
    let value_str = format_param_value(p, contract_address, registry, format_addr, expand);
    vec![Span::raw(format!("{name_prefix}{value_str}"))]
}

fn format_param_value(
    p: &DecodedParam,
    contract_address: &Felt,
    registry: Option<&AddressRegistry>,
    format_addr: &dyn Fn(&Felt) -> String,
    expand: bool,
) -> String {
    let type_name = p.type_name.as_deref().unwrap_or("");

    // ContractAddress → show label (uses format_addr which includes Voyager fallback)
    // Also match untyped params that resolve to known addresses.
    if type_name.contains("ContractAddress")
        || (type_name.is_empty()
            && p.value != Felt::ZERO
            && registry.is_some_and(|r| r.resolve(&p.value).is_some()))
    {
        return format_addr(&p.value);
    }

    if let Some(reg) = registry {
        // u256 → format as token amount if the emitting contract is a known token
        if type_name.contains("u256") {
            let low = u128_from_felt(&p.value);
            let high = p.value_high.as_ref().map(u128_from_felt).unwrap_or(0);

            if let Some(decimals) = reg.get_decimals(contract_address) {
                return format_token_amount(low, high, decimals);
            }
            // Not a known token — display as a plain number or hex
            if high == 0 {
                return format_u128_display(low);
            }
            return format!("0x{high:x}:{low:032x}");
        }
    }

    if expand {
        format!("{:#x}", p.value)
    } else {
        format_felt_short(&p.value)
    }
}

/// Format a u256 value as a human-readable token amount with the given decimals.
///
/// For example, with decimals=18: 1_500_000_000_000_000_000 → "1.5"
pub fn format_token_amount(low: u128, high: u128, decimals: u8) -> String {
    // U256::MAX = unlimited approval
    if low == u128::MAX && high == u128::MAX {
        return "U256::MAX".to_string();
    }
    if high != 0 {
        // Too large to fit in u128 — show raw hex
        return format!("0x{high:x}{low:032x}");
    }
    if decimals == 0 {
        return low.to_string();
    }
    let divisor = 10u128.pow(decimals as u32);
    let whole = low / divisor;
    let frac = low % divisor;
    if frac == 0 {
        return whole.to_string();
    }
    let frac_str = format!("{:0>width$}", frac, width = decimals as usize);
    let trimmed = frac_str.trim_end_matches('0');
    // Show up to 6 significant decimal places
    let display_len = trimmed.len().min(6);
    format!("{}.{}", whole, &frac_str[..display_len])
}

/// Display a u128 as decimal if small, otherwise hex.
fn format_u128_display(val: u128) -> String {
    if val == 0 {
        return "0".to_string();
    }
    if val < 1_000_000 {
        return val.to_string();
    }
    format!("{:#x}", val)
}

/// Short Felt display: "0x1234..abcd", or plain number if small.
pub fn format_felt_short(felt: &Felt) -> String {
    if *felt == Felt::ZERO {
        return "0".to_string();
    }
    let hex = format!("{:#x}", felt);
    if hex.len() <= 12 {
        hex
    } else {
        format!("{}..{}", &hex[..6], &hex[hex.len() - 4..])
    }
}

fn u128_from_felt(felt: &Felt) -> u128 {
    let bytes = felt.to_bytes_be();
    u128::from_be_bytes(bytes[16..32].try_into().unwrap_or([0u8; 16]))
}
