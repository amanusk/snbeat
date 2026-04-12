use starknet::core::types::{BlockId, BlockTag, Felt, FunctionCall, MaybePreConfirmedBlockWithTxs};
use starknet::core::utils::get_selector_from_name;
use starknet::providers::{JsonRpcClient, Provider, jsonrpc::HttpTransport};
use url::Url;

const ETH_TOKEN: &str = "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7";

fn provider() -> JsonRpcClient<HttpTransport> {
    dotenvy::dotenv().ok();
    let rpc_url =
        std::env::var("APP_RPC_URL").expect("APP_RPC_URL must be set for integration tests");
    JsonRpcClient::new(HttpTransport::new(
        Url::parse(&rpc_url).expect("invalid RPC URL"),
    ))
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_block_number() {
    let p = provider();
    let block_number = p.block_number().await.expect("block_number failed");
    assert!(block_number > 8_000_000, "Expected mainnet block > 8M");
    println!("Latest block: {block_number}");
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_get_block_with_txs() {
    let p = provider();
    let latest = p.block_number().await.unwrap();
    let block = p
        .get_block_with_txs(BlockId::Number(latest), None)
        .await
        .expect("get_block_with_txs failed");

    match block {
        MaybePreConfirmedBlockWithTxs::Block(b) => {
            assert_eq!(b.block_number, latest);
            println!(
                "Block #{}: {} txs, hash={:#x}",
                b.block_number,
                b.transactions.len(),
                b.block_hash
            );
        }
        MaybePreConfirmedBlockWithTxs::PreConfirmedBlock(b) => {
            println!("Pre-confirmed block: {} txs", b.transactions.len());
        }
    }
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_get_block_hash_and_number() {
    let p = provider();
    let result = p
        .block_hash_and_number()
        .await
        .expect("block_hash_and_number failed");
    assert!(result.block_number > 0);
    assert_ne!(result.block_hash, Felt::ZERO);
    println!(
        "Block #{}: hash={:#x}",
        result.block_number, result.block_hash
    );
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_chain_id() {
    let p = provider();
    let chain_id = p.chain_id().await.expect("chain_id failed");
    // SN_MAIN = 0x534e5f4d41494e
    println!("Chain ID: {:#x}", chain_id);
    assert_ne!(chain_id, Felt::ZERO);
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_get_nonce_for_known_contract() {
    let p = provider();
    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    // ETH token is a contract, not an account — nonce should be 0
    let nonce = p
        .get_nonce(BlockId::Tag(BlockTag::Latest), eth)
        .await
        .expect("get_nonce failed");
    println!("ETH token nonce: {:#x}", nonce);
}

#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn test_balance_of_call() {
    let p = provider();
    let eth = Felt::from_hex(ETH_TOKEN).unwrap();
    let balance_of = get_selector_from_name("balance_of").unwrap();
    // Check ETH balance of the Starknet sequencer
    let sequencer =
        Felt::from_hex("0x01176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8")
            .unwrap();

    let result = p
        .call(
            FunctionCall {
                contract_address: eth,
                entry_point_selector: balance_of,
                calldata: vec![sequencer],
            },
            BlockId::Tag(BlockTag::Latest),
        )
        .await
        .expect("call balance_of failed");

    assert!(
        !result.is_empty(),
        "balance_of should return at least 1 felt"
    );
    println!("Sequencer ETH balance (raw): {:#x}", result[0]);
}
