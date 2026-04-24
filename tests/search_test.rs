use starknet::core::types::Felt;

const ETH_TOKEN: &str = "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";

fn registry() -> snbeat::registry::AddressRegistry {
    snbeat::registry::AddressRegistry::load(std::path::Path::new("/dev/null"))
        .unwrap()
        .0
}

// --- Parser tests ---

#[test]
fn test_classify_block_number() {
    let reg = registry();
    let result = snbeat::search::parser::classify("123456", &reg).unwrap();
    assert!(matches!(
        result,
        snbeat::search::parser::SearchQuery::BlockNumber(123456)
    ));
}

#[test]
fn test_classify_zero() {
    let reg = registry();
    let result = snbeat::search::parser::classify("0", &reg).unwrap();
    assert!(matches!(
        result,
        snbeat::search::parser::SearchQuery::BlockNumber(0)
    ));
}

#[test]
fn test_classify_known_label() {
    let reg = registry();
    let result = snbeat::search::parser::classify("ETH", &reg).unwrap();
    match result {
        snbeat::search::parser::SearchQuery::Label(_name, addr) => {
            assert_eq!(addr, Felt::from_hex(ETH_TOKEN).unwrap());
        }
        other => panic!("Expected Label, got {:?}", other),
    }
}

#[test]
fn test_classify_known_label_case_insensitive() {
    let reg = registry();
    let result = snbeat::search::parser::classify("eth", &reg).unwrap();
    assert!(matches!(
        result,
        snbeat::search::parser::SearchQuery::Label(..)
    ));
}

#[test]
fn test_classify_known_hex() {
    let reg = registry();
    // ETH token address should resolve as Label since it's known
    let result = snbeat::search::parser::classify(ETH_TOKEN, &reg).unwrap();
    assert!(matches!(
        result,
        snbeat::search::parser::SearchQuery::Label(..)
    ));
}

#[test]
fn test_classify_unknown_hex() {
    let reg = registry();
    let result = snbeat::search::parser::classify("0xdeadbeef12345678", &reg).unwrap();
    assert!(matches!(
        result,
        snbeat::search::parser::SearchQuery::Ambiguous(_)
    ));
}

#[test]
fn test_classify_empty() {
    let reg = registry();
    let result = snbeat::search::parser::classify("", &reg).unwrap();
    assert!(matches!(result, snbeat::search::parser::SearchQuery::Empty));
}

#[test]
fn test_classify_invalid_input() {
    let reg = registry();
    let result = snbeat::search::parser::classify("not_a_thing_xyz", &reg);
    assert!(result.is_err());
}

// --- SearchEngine tests ---

#[test]
fn test_search_engine_suggest() {
    let reg = std::sync::Arc::new(registry());
    let engine = snbeat::search::SearchEngine::new(reg);

    let suggestions = engine.suggest("ET");
    assert!(!suggestions.is_empty());
    assert!(suggestions[0].display.contains("ETH"));
}

#[test]
fn test_search_engine_suggest_empty() {
    let reg = std::sync::Arc::new(registry());
    let engine = snbeat::search::SearchEngine::new(reg);

    let suggestions = engine.suggest("");
    assert!(suggestions.is_empty());
}

#[test]
fn test_user_labels_prioritized() {
    use std::io::Write;

    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(
        r#"
[addresses]
"0x0000000000000000000000000000000000000000000000000000000000001234" = "MyAddr"
"#
        .to_string()
        .as_bytes(),
    )
    .unwrap();

    let reg = snbeat::registry::AddressRegistry::load(f.path()).unwrap().0;

    let results = reg.search("My", 10);
    assert!(!results.is_empty());
    assert!(results[0].is_user, "User labels should appear first");
    assert!(results[0].display.contains("MyAddr"));
}
