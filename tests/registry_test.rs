use std::io::Write;

use starknet::core::types::Felt;

const ETH_TOKEN: &str = "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";
const USDC_TOKEN: &str = "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8";

fn make_user_labels(content: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f
}

#[test]
fn test_load_bundled_known_addresses() {
    let registry = snbeat::registry::AddressRegistry::load(
        std::path::Path::new("/dev/null"), // no user labels
    )
    .unwrap()
    .0;

    // Should have bundled addresses
    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    assert_eq!(registry.resolve(&eth), Some("ETH"));

    let usdc = Felt::from_hex(USDC_TOKEN).unwrap();
    assert_eq!(registry.resolve(&usdc), Some("USDC (bridged)"));
}

#[test]
fn test_user_labels_override_known() {
    let labels = make_user_labels(&format!(
        r#"
[addresses]
"{ETH_TOKEN}" = "My ETH"
"#
    ));

    let registry = snbeat::registry::AddressRegistry::load(labels.path())
        .unwrap()
        .0;

    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    assert_eq!(registry.resolve(&eth), Some("My ETH"));
}

#[test]
fn test_search_prefix_match() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    let results = registry.search("ET", 10);
    assert!(!results.is_empty(), "Should find ETH with prefix 'ET'");
    assert!(
        results[0].display.contains("ETH"),
        "First result should be ETH"
    );
}

#[test]
fn test_search_substring_match() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    let results = registry.search("swap", 10);
    assert!(!results.is_empty(), "Should find swap-related addresses");
    for r in &results {
        assert!(
            r.display.to_lowercase().contains("swap"),
            "Result should contain 'swap': {}",
            r.display
        );
    }
}

#[test]
fn test_search_hex_prefix() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    // Felt strips leading zeros: 0x049d... → 0x49d... in display
    let results = registry.search("0x49d", 10);
    assert!(!results.is_empty(), "Should find ETH by hex prefix");
    assert!(results[0].display.contains("ETH"));
}

#[test]
fn test_search_empty_returns_nothing() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    assert!(registry.search("", 10).is_empty());
}

#[test]
fn test_search_limit() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    // Search for something that matches many entries
    let results = registry.search("0x0", 3);
    assert!(results.len() <= 3);
}

#[test]
fn test_resolve_by_name() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    assert_eq!(registry.resolve_by_name("ETH"), Some(eth));
    assert_eq!(registry.resolve_by_name("eth"), Some(eth)); // case insensitive
    assert_eq!(registry.resolve_by_name("nonexistent"), None);
}

#[test]
fn test_format_address() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    assert_eq!(registry.format_address(&eth), "[ETH]");

    let unknown = Felt::from_hex("0xdeadbeef1234567890").unwrap();
    let formatted = registry.format_address(&unknown);
    assert!(formatted.contains(".."), "Unknown should be truncated hex");
}

#[test]
fn test_resolve_tx_label() {
    let labels = make_user_labels(
        r#"
[transactions]
"0x05c8845db9ac7775e736853b456a8dddaf22367a81d35ae6951825c36e1a27c3" = "60M STRK deposit"
"#,
    );

    let registry = snbeat::registry::AddressRegistry::load(labels.path())
        .unwrap()
        .0;

    let tx_hash =
        Felt::from_hex("0x05c8845db9ac7775e736853b456a8dddaf22367a81d35ae6951825c36e1a27c3")
            .unwrap();
    assert_eq!(registry.resolve_tx(&tx_hash), Some("60M STRK deposit"));

    let other = Felt::from_hex("0xdeadbeef").unwrap();
    assert_eq!(registry.resolve_tx(&other), None);
}

#[test]
fn test_tx_label_appears_in_search() {
    let labels = make_user_labels(
        r#"
[transactions]
"0x05c8845db9ac7775e736853b456a8dddaf22367a81d35ae6951825c36e1a27c3" = "60M STRK deposit"
"#,
    );

    let registry = snbeat::registry::AddressRegistry::load(labels.path())
        .unwrap()
        .0;

    let results = registry.search("60M", 10);
    assert!(!results.is_empty(), "tx label should be searchable by name");
    assert!(results[0].is_user);
    assert!(results[0].display.contains("60M STRK deposit"));
    assert!(
        matches!(results[0].kind, snbeat::registry::EntryKind::Transaction),
        "result kind should be Transaction"
    );

    // resolve_by_name returns the tx hash so the network resolver can navigate
    let felt = registry.resolve_by_name("60M STRK deposit").unwrap();
    assert_eq!(
        felt,
        Felt::from_hex("0x05c8845db9ac7775e736853b456a8dddaf22367a81d35ae6951825c36e1a27c3")
            .unwrap()
    );
}

#[test]
fn test_corrupted_labels_returns_warning() {
    let labels = make_user_labels("[transactions]\nthis is not toml = broken");
    let (_reg, warning) = snbeat::registry::AddressRegistry::load(labels.path()).unwrap();
    assert!(warning.is_some());
}

#[test]
fn test_get_decimals() {
    let registry = snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0;

    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    assert_eq!(registry.get_decimals(&eth), Some(18));

    let usdc = Felt::from_hex(USDC_TOKEN).unwrap();
    assert_eq!(registry.get_decimals(&usdc), Some(6));
}
