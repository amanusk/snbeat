use starknet::core::types::{BlockId, BlockTag, ContractClass, Felt};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::{JsonRpcClient, Provider, jsonrpc::HttpTransport};
use url::Url;

// Well-known mainnet addresses
const ETH_TOKEN: &str = "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";
const USDC_TOKEN: &str = "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8";
const STRK_TOKEN: &str = "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d";

fn provider() -> JsonRpcClient<HttpTransport> {
    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("APP_RPC_URL").expect("APP_RPC_URL required");
    JsonRpcClient::new(HttpTransport::new(Url::parse(&rpc_url).unwrap()))
}

fn parse_abi(class: &ContractClass) -> snbeat::decode::abi::ParsedAbi {
    snbeat::decode::abi::parse_contract_class(class)
}

// ---- Selector computation tests (pure, no RPC) ----

#[test]
fn test_transfer_selector() {
    let selector = get_selector_from_name("Transfer").unwrap();
    // Known Transfer event selector
    let expected = get_selector_from_name("Transfer").unwrap();
    assert_eq!(selector, expected);
    assert_ne!(selector, Felt::ZERO);
}

#[test]
fn test_approval_selector() {
    let selector = get_selector_from_name("Approval").unwrap();
    assert_ne!(selector, Felt::ZERO);
    // Approval and Transfer must be different
    let transfer = get_selector_from_name("Transfer").unwrap();
    assert_ne!(selector, transfer);
}

#[test]
fn test_known_function_selectors() {
    // Common ERC20 functions
    let transfer = get_selector_from_name("transfer").unwrap();
    let approve = get_selector_from_name("approve").unwrap();
    let balance_of = get_selector_from_name("balance_of").unwrap();
    let name = get_selector_from_name("name").unwrap();

    // All must be distinct
    let selectors = [transfer, approve, balance_of, name];
    for (i, a) in selectors.iter().enumerate() {
        for (j, b) in selectors.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "Selectors for different functions must differ");
            }
        }
    }
}

// ---- Sierra ABI parsing tests (require RPC) ----

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_parse_eth_abi() {
    let p = provider();
    let eth = Felt::from_hex(ETH_TOKEN).unwrap();

    // Get class hash for ETH token
    let class_hash = p
        .get_class_hash_at(BlockId::Tag(BlockTag::Latest), eth)
        .await
        .expect("Failed to get ETH class hash");

    // Fetch the class
    let class = p
        .get_class(BlockId::Tag(BlockTag::Latest), class_hash)
        .await
        .expect("Failed to get ETH class");

    let abi = parse_abi(&class);

    // ETH token must have Transfer event
    let transfer_selector = get_selector_from_name("Transfer").unwrap();
    let transfer_event = abi.get_event(&transfer_selector);
    assert!(
        transfer_event.is_some(),
        "ETH ABI must contain Transfer event"
    );
    let transfer = transfer_event.unwrap();
    assert_eq!(transfer.name, "Transfer");
    println!("ETH Transfer event: {:?}", transfer);

    // Must have Approval event
    let approval_selector = get_selector_from_name("Approval").unwrap();
    let approval = abi.get_event(&approval_selector);
    assert!(approval.is_some(), "ETH ABI must contain Approval event");
    println!("ETH Approval event: {:?}", approval.unwrap());

    // Must have transfer function (lowercase)
    let transfer_fn_selector = get_selector_from_name("transfer").unwrap();
    let transfer_fn = abi.get_function(&transfer_fn_selector);
    assert!(
        transfer_fn.is_some(),
        "ETH ABI must contain transfer function"
    );
    let tf = transfer_fn.unwrap();
    assert_eq!(tf.name, "transfer");
    assert!(!tf.inputs.is_empty(), "transfer function must have inputs");
    println!("ETH transfer function: {:?}", tf);

    // Must have balance_of function
    let balance_selector = get_selector_from_name("balance_of").unwrap();
    assert!(
        abi.get_function(&balance_selector).is_some(),
        "ETH ABI must contain balance_of"
    );

    println!(
        "ETH ABI: {} functions, {} events",
        abi.functions.len(),
        abi.events.len()
    );
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_parse_usdc_abi() {
    let p = provider();
    let usdc = Felt::from_hex(USDC_TOKEN).unwrap();

    let class_hash = p
        .get_class_hash_at(BlockId::Tag(BlockTag::Latest), usdc)
        .await
        .expect("Failed to get USDC class hash");

    let class = p
        .get_class(BlockId::Tag(BlockTag::Latest), class_hash)
        .await
        .expect("Failed to get USDC class");

    let abi = parse_abi(&class);

    // USDC must also have Transfer and Approval
    let transfer_selector = get_selector_from_name("Transfer").unwrap();
    assert!(abi.get_event(&transfer_selector).is_some());

    let approval_selector = get_selector_from_name("Approval").unwrap();
    assert!(abi.get_event(&approval_selector).is_some());

    println!(
        "USDC ABI: {} functions, {} events",
        abi.functions.len(),
        abi.events.len()
    );
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_parse_strk_abi() {
    let p = provider();
    let strk = Felt::from_hex(STRK_TOKEN).unwrap();

    let class_hash = p
        .get_class_hash_at(BlockId::Tag(BlockTag::Latest), strk)
        .await
        .expect("Failed to get STRK class hash");

    let class = p
        .get_class(BlockId::Tag(BlockTag::Latest), class_hash)
        .await
        .expect("Failed to get STRK class");

    let abi = parse_abi(&class);

    // STRK must have Transfer event
    let transfer_selector = get_selector_from_name("Transfer").unwrap();
    assert!(
        abi.get_event(&transfer_selector).is_some(),
        "STRK ABI must contain Transfer event"
    );

    println!(
        "STRK ABI: {} functions, {} events",
        abi.functions.len(),
        abi.events.len()
    );
}

// ---- Event decoding tests ----

#[test]
fn test_decode_event_with_abi() {
    use snbeat::data::types::SnEvent;
    use snbeat::decode::abi::{EventDef, FeltKey, ParsedAbi};
    use snbeat::decode::events::decode_event;

    let transfer_selector = get_selector_from_name("Transfer").unwrap();

    // Build a mock ABI with a Transfer event
    let mut abi = ParsedAbi::default();
    abi.events.insert(
        FeltKey(transfer_selector),
        EventDef {
            name: "Transfer".to_string(),
            keys: vec![
                ("from".to_string(), "ContractAddress".to_string()),
                ("to".to_string(), "ContractAddress".to_string()),
            ],
            data: vec![("value".to_string(), "u256".to_string())],
        },
    );

    // Create a mock event
    let event = SnEvent {
        from_address: Felt::from_hex(ETH_TOKEN).unwrap(),
        keys: vec![
            transfer_selector,
            Felt::from_hex("0xaaa").unwrap(), // from
            Felt::from_hex("0xbbb").unwrap(), // to
        ],
        data: vec![Felt::from(1000u64)], // value
        transaction_hash: Felt::ZERO,
        block_number: 0,
        event_index: 0,
    };

    let decoded = decode_event(&event, Some(&abi));
    assert_eq!(decoded.event_name.as_deref(), Some("Transfer"));
    assert_eq!(decoded.decoded_keys.len(), 2);
    assert_eq!(decoded.decoded_keys[0].name.as_deref(), Some("from"));
    assert_eq!(decoded.decoded_keys[1].name.as_deref(), Some("to"));
    assert_eq!(decoded.decoded_data.len(), 1);
    assert_eq!(decoded.decoded_data[0].name.as_deref(), Some("value"));
}

#[test]
fn test_decode_event_without_abi() {
    use snbeat::data::types::SnEvent;
    use snbeat::decode::events::decode_event;

    let event = SnEvent {
        from_address: Felt::from_hex(ETH_TOKEN).unwrap(),
        keys: vec![Felt::from(0x1234u64)],
        data: vec![Felt::from(42u64)],
        transaction_hash: Felt::ZERO,
        block_number: 0,
        event_index: 0,
    };

    // No ABI → falls back gracefully
    let decoded = decode_event(&event, None);
    assert!(decoded.event_name.is_none());
    assert!(decoded.decoded_keys.is_empty()); // key[0] is selector, skipped
    assert_eq!(decoded.decoded_data.len(), 1);
    assert!(decoded.decoded_data[0].name.is_none());
}

// ---- Event grouping tests ----

#[test]
fn test_group_events_by_contract() {
    use snbeat::data::types::SnEvent;
    use snbeat::decode::events::{DecodedEvent, group_events_by_contract};

    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    let usdc = Felt::from_hex(USDC_TOKEN).unwrap();
    let dummy_raw = SnEvent {
        from_address: Felt::ZERO,
        keys: vec![],
        data: vec![],
        transaction_hash: Felt::ZERO,
        block_number: 0,
        event_index: 0,
    };

    let events = vec![
        DecodedEvent {
            contract_address: eth,
            event_name: Some("Transfer".into()),
            decoded_keys: vec![],
            decoded_data: vec![],
            raw: dummy_raw.clone(),
        },
        DecodedEvent {
            contract_address: usdc,
            event_name: Some("Transfer".into()),
            decoded_keys: vec![],
            decoded_data: vec![],
            raw: dummy_raw.clone(),
        },
        DecodedEvent {
            contract_address: eth,
            event_name: Some("Approval".into()),
            decoded_keys: vec![],
            decoded_data: vec![],
            raw: dummy_raw.clone(),
        },
    ];

    let groups = group_events_by_contract(&events);
    assert_eq!(groups.len(), 2, "Should have 2 contract groups");
    assert_eq!(groups[0].contract_address, eth);
    assert_eq!(groups[0].events.len(), 2, "ETH should have 2 events");
    assert_eq!(groups[1].contract_address, usdc);
    assert_eq!(groups[1].events.len(), 1, "USDC should have 1 event");
}

// ---- Multicall parsing tests ----

#[test]
fn test_parse_multicall() {
    use snbeat::decode::functions::parse_multicall;

    let contract1 = Felt::from_hex("0x1111").unwrap();
    let selector1 = get_selector_from_name("transfer").unwrap();
    let contract2 = Felt::from_hex("0x2222").unwrap();
    let selector2 = get_selector_from_name("approve").unwrap();

    // 2 calls: transfer(3 args), approve(2 args)
    let calldata = vec![
        Felt::from(2u64),   // num_calls
        contract1,          // call 0: address
        selector1,          // call 0: selector
        Felt::from(3u64),   // call 0: data_len
        Felt::from(0xau64), // call 0: data[0]
        Felt::from(0xbu64), // call 0: data[1]
        Felt::from(0xcu64), // call 0: data[2]
        contract2,          // call 1: address
        selector2,          // call 1: selector
        Felt::from(2u64),   // call 1: data_len
        Felt::from(0xdu64), // call 1: data[0]
        Felt::from(0xeu64), // call 1: data[1]
    ];

    let calls = parse_multicall(&calldata);
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].contract_address, contract1);
    assert_eq!(calls[0].selector, selector1);
    assert_eq!(calls[0].data.len(), 3);
    assert_eq!(calls[1].contract_address, contract2);
    assert_eq!(calls[1].selector, selector2);
    assert_eq!(calls[1].data.len(), 2);
}

#[test]
fn test_parse_multicall_empty() {
    use snbeat::decode::functions::parse_multicall;
    assert!(parse_multicall(&[]).is_empty());
    assert!(parse_multicall(&[Felt::ZERO]).is_empty()); // 0 calls
}

// ---- ParsedAbi serialization round-trip ----

#[test]
fn test_parsed_abi_serde_roundtrip() {
    use snbeat::decode::abi::{FeltKey, FunctionDef, ParsedAbi};

    let mut abi = ParsedAbi::default();
    let selector = get_selector_from_name("transfer").unwrap();
    abi.functions.insert(
        FeltKey(selector),
        FunctionDef {
            name: "transfer".into(),
            inputs: vec![
                ("recipient".into(), "ContractAddress".into()),
                ("amount".into(), "u256".into()),
            ],
            outputs: vec!["bool".into()],
            state_mutability: None,
        },
    );

    let json = serde_json::to_string(&abi).expect("serialize");
    let roundtripped: ParsedAbi = serde_json::from_str(&json).expect("deserialize");
    assert!(roundtripped.get_function(&selector).is_some());
    assert_eq!(
        roundtripped.get_function(&selector).unwrap().name,
        "transfer"
    );
}

// ---- RawCall function_name resolution ----

#[test]
fn test_raw_call_function_name_field() {
    use snbeat::decode::functions::parse_multicall;

    let contract = Felt::from_hex("0x1111").unwrap();
    let selector = get_selector_from_name("transfer").unwrap();

    let calldata = vec![
        Felt::from(1u64),   // num_calls
        contract,           // call 0: address
        selector,           // call 0: selector
        Felt::from(2u64),   // call 0: data_len
        Felt::from(0xau64), // call 0: data[0]
        Felt::from(0xbu64), // call 0: data[1]
    ];

    let mut calls = parse_multicall(&calldata);
    assert_eq!(calls.len(), 1);
    assert!(calls[0].function_name.is_none(), "Initially no name");

    // Simulate what the network task does: resolve the selector
    calls[0].function_name = Some("transfer".to_string());
    assert_eq!(calls[0].function_name.as_deref(), Some("transfer"));
}
