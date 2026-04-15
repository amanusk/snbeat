use starknet::core::types::{DeployAccountTransaction, Felt, Transaction, TransactionReceipt};
use starknet::core::utils::get_contract_address;
use starknet::providers::{JsonRpcClient, Provider, jsonrpc::HttpTransport};
use url::Url;

use snbeat::data::DataSource;
use snbeat::data::rpc::RpcDataSource;
use snbeat::data::types::SnTransaction;

fn provider() -> JsonRpcClient<HttpTransport> {
    dotenvy::dotenv().ok();
    let rpc_url =
        std::env::var("APP_RPC_URL").expect("APP_RPC_URL must be set for integration tests");
    JsonRpcClient::new(HttpTransport::new(
        Url::parse(&rpc_url).expect("invalid RPC URL"),
    ))
}

fn rpc_ds() -> RpcDataSource {
    dotenvy::dotenv().ok();
    let rpc_url =
        std::env::var("APP_RPC_URL").expect("APP_RPC_URL must be set for integration tests");
    RpcDataSource::new(&rpc_url)
}

/// Well-known DEPLOY_ACCOUNT tx on Starknet mainnet (Argent account).
/// The ETH token contract is the deployer-free reference.
const DEPLOY_ACCOUNT_TX: &str = "0x568c996e5828e933d366f8750138e729eeb178c53572f21e9b96aef48eb09ff";
const DEPLOY_BLOCK: u64 = 8745245;

/// Verify that starknet-rs `get_contract_address` computes the correct deployed
/// address for a DEPLOY_ACCOUNT tx by comparing against the receipt's
/// `contract_address`.
#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn deploy_account_address_matches_receipt() {
    let p = provider();
    let tx_hash = Felt::from_hex(DEPLOY_ACCOUNT_TX).unwrap();

    // Fetch the raw transaction
    let raw_tx = p
        .get_transaction_by_hash(tx_hash, None)
        .await
        .expect("get_transaction_by_hash failed");

    let (class_hash, salt, calldata, nonce) = match &raw_tx {
        Transaction::DeployAccount(DeployAccountTransaction::V1(v)) => (
            v.class_hash,
            v.contract_address_salt,
            &v.constructor_calldata,
            v.nonce,
        ),
        Transaction::DeployAccount(DeployAccountTransaction::V3(v)) => (
            v.class_hash,
            v.contract_address_salt,
            &v.constructor_calldata,
            v.nonce,
        ),
        other => panic!("Expected DeployAccount tx, got: {:?}", other),
    };

    println!("class_hash:  {:#x}", class_hash);
    println!("salt:        {:#x}", salt);
    println!("nonce:       {:#x}", nonce);
    println!(
        "calldata:    [{}]",
        calldata
            .iter()
            .map(|f| format!("{:#x}", f))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Compute address the same way our convert_transaction does
    let computed = get_contract_address(salt, class_hash, calldata, Felt::ZERO);
    println!("computed:    {:#x}", computed);

    // Fetch receipt — it has the authoritative contract_address
    let receipt_with_block = p
        .get_transaction_receipt(tx_hash)
        .await
        .expect("get_receipt failed");
    let receipt_addr = match &receipt_with_block.receipt {
        TransactionReceipt::DeployAccount(r) => r.contract_address,
        other => panic!("Expected DeployAccount receipt, got: {:?}", other),
    };
    println!("from receipt: {:#x}", receipt_addr);

    assert_eq!(
        computed, receipt_addr,
        "Computed address should match receipt contract_address"
    );
}

/// Verify that `RpcDataSource::get_transaction` sets the correct sender for
/// DEPLOY_ACCOUNT txs (sender == deployed contract address == fee payer).
#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn deploy_account_sender_via_datasource() {
    let ds = rpc_ds();
    let p = provider();
    let tx_hash = Felt::from_hex(DEPLOY_ACCOUNT_TX).unwrap();

    // Get the expected address from the receipt
    let receipt_with_block = p
        .get_transaction_receipt(tx_hash)
        .await
        .expect("get_receipt failed");
    let expected_addr = match &receipt_with_block.receipt {
        TransactionReceipt::DeployAccount(r) => r.contract_address,
        other => panic!("Expected DeployAccount receipt, got: {:?}", other),
    };

    // Now check our DataSource abstraction
    let sn_tx = ds
        .get_transaction(tx_hash)
        .await
        .expect("get_transaction failed");

    match &sn_tx {
        SnTransaction::DeployAccount(da) => {
            println!("contract_address: {:#x}", da.contract_address);
            println!("expected:         {:#x}", expected_addr);
            println!("nonce:            {:?}", da.nonce);
            assert_eq!(
                da.contract_address, expected_addr,
                "DeployAccountTx.contract_address should be the deployed account"
            );
            assert_eq!(
                da.nonce,
                Some(Felt::ZERO),
                "DEPLOY_ACCOUNT nonce should be 0"
            );
        }
        other => panic!("Expected DeployAccount, got: {}", other.type_name()),
    }

    assert_eq!(
        sn_tx.sender(),
        expected_addr,
        "sender() should return the deployed address"
    );
}

/// Verify that `get_block_with_txs` correctly includes and identifies the
/// DEPLOY_ACCOUNT tx in the block.
#[tokio::test]
#[ignore = "requires APP_RPC_URL"]
async fn deploy_account_found_in_block() {
    let ds = rpc_ds();
    let p = provider();
    let tx_hash = Felt::from_hex(DEPLOY_ACCOUNT_TX).unwrap();

    // Get the expected address from the receipt
    let receipt_with_block = p
        .get_transaction_receipt(tx_hash)
        .await
        .expect("get_receipt failed");
    let expected_addr = match &receipt_with_block.receipt {
        TransactionReceipt::DeployAccount(r) => r.contract_address,
        other => panic!("Expected DeployAccount receipt, got: {:?}", other),
    };

    // Fetch the block and find the DEPLOY_ACCOUNT tx
    let (_block, txs) = ds
        .get_block_with_txs(DEPLOY_BLOCK)
        .await
        .expect("get_block_with_txs failed");

    let deploy_tx = txs
        .iter()
        .find(|t| t.hash() == tx_hash)
        .expect("DEPLOY_ACCOUNT tx not found in block");

    assert_eq!(deploy_tx.type_name(), "DEPLOY_ACCOUNT");
    assert_eq!(
        deploy_tx.sender(),
        expected_addr,
        "sender in block view should be the deployed address"
    );

    // Verify the find_deploy_tx matching logic would work:
    // it checks `da.contract_address == addr`
    match deploy_tx {
        SnTransaction::DeployAccount(da) => {
            assert_eq!(
                da.contract_address, expected_addr,
                "contract_address must match for find_deploy_tx to identify the tx"
            );
        }
        _ => unreachable!(),
    }
}
