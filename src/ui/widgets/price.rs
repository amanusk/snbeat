//! Cache-first USD price helpers for the UI. All reads are sync — misses
//! trigger an async fetch via `Action::FetchTokenPrices*` from the views.

use starknet::core::types::Felt;

use crate::app::App;
use crate::decode::events::DecodedParam;
use crate::registry::AddressRegistry;
use crate::utils::felt_to_u128;

/// Format a USD value like `$1,234.56`. Sub-cent values get extra precision
/// so prices like `$0.0021` stay legible.
pub fn format_usd(v: f64) -> String {
    if !v.is_finite() {
        return String::new();
    }
    if v.abs() < 0.01 {
        return format!("${v:.4}");
    }
    let whole = v.trunc() as i64;
    let cents = (v.fract().abs() * 100.0).round() as u64;
    let mut whole_str = whole.abs().to_string();
    let mut grouped = String::new();
    while whole_str.len() > 3 {
        let split = whole_str.len() - 3;
        grouped = format!(",{}{}", &whole_str[split..], grouped);
        whole_str.truncate(split);
    }
    grouped = format!("{}{}", whole_str, grouped);
    let sign = if whole < 0 { "-" } else { "" };
    format!("{sign}${grouped}.{cents:02}")
}

/// `(amount_as_f64, decimals)` for u256 amounts of known tokens, else `None`.
/// Values above u128 are skipped — they're not meaningful as USD here.
pub fn token_amount_from_param(
    p: &DecodedParam,
    contract_address: &Felt,
    registry: Option<&AddressRegistry>,
) -> Option<(f64, u8)> {
    let type_name = p.type_name.as_deref().unwrap_or("");
    if !type_name.contains("u256") {
        return None;
    }
    let high = p.value_high.as_ref().map(felt_to_u128).unwrap_or(0);
    if high != 0 {
        return None;
    }
    let decimals = registry?.get_decimals(contract_address)?;
    let low = felt_to_u128(&p.value);
    let scale = 10f64.powi(decimals as i32);
    Some((low as f64 / scale, decimals))
}

pub fn token_prices(app: &App, token: &Felt, ts: Option<u64>) -> (Option<f64>, Option<f64>) {
    let Some(pc) = app.price_client.as_ref() else {
        return (None, None);
    };
    let today = pc.get_today_price(token);
    let historic = ts.and_then(|t| pc.get_historic_price(token, t));
    (today, historic)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_usd_groups_thousands() {
        assert_eq!(format_usd(1234567.891), "$1,234,567.89");
        assert_eq!(format_usd(1234.5), "$1,234.50");
        assert_eq!(format_usd(0.0021), "$0.0021");
        assert_eq!(format_usd(-12.5), "-$12.50");
    }

    #[test]
    fn format_usd_handles_non_finite() {
        assert_eq!(format_usd(f64::NAN), "");
        assert_eq!(format_usd(f64::INFINITY), "");
    }
}
