use std::collections::HashMap;

use serde::Deserialize;
use starknet::core::types::Felt;
use tracing::{debug, warn};

use crate::error::Result;

#[derive(Debug, Clone, Deserialize)]
pub struct KnownAddressEntry {
    pub name: String,
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub verified: bool,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub decimals: Option<u8>,
}

#[derive(Debug, Default, Deserialize)]
pub struct KnownAddressesFile {
    #[serde(default)]
    pub addresses: HashMap<String, KnownAddressEntry>,
}

#[derive(Debug, Clone)]
pub struct KnownAddress {
    pub address: Felt,
    pub name: String,
    pub addr_type: String,
    pub verified: bool,
    pub source: String,
    pub decimals: Option<u8>,
}

/// Load known addresses from the bundled baseline.
pub fn load_known_addresses() -> Result<Vec<KnownAddress>> {
    let content = BUNDLED_KNOWN_ADDRESSES;

    let file: KnownAddressesFile = toml::from_str(&content)?;
    let mut addresses = Vec::new();

    for (hex, entry) in &file.addresses {
        let felt = match Felt::from_hex(hex) {
            Ok(f) => f,
            Err(e) => {
                warn!(address = hex, error = %e, "Invalid address in known addresses, skipping");
                continue;
            }
        };
        addresses.push(KnownAddress {
            address: felt,
            name: entry.name.clone(),
            addr_type: entry.r#type.clone(),
            verified: entry.verified,
            source: entry.source.clone(),
            decimals: entry.decimals,
        });
    }

    debug!(count = addresses.len(), "Loaded known addresses");
    Ok(addresses)
}

/// Bundled baseline of well-known Starknet mainnet addresses.
const BUNDLED_KNOWN_ADDRESSES: &str = r#"
[addresses]
# Tokens
"0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7" = { name = "ETH", type = "ERC20", verified = true, source = "bundled", decimals = 18 }
"0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8" = { name = "USDC (bridged)", type = "ERC20", verified = true, source = "bundled", decimals = 6 }
"0x033068f6539f8e6e6b131e6b2b814e6c34a5224bc66947c47dab9dfee93b35fb" = { name = "USDC (native)", type = "ERC20", verified = true, source = "bundled", decimals = 6 }
"0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d" = { name = "STRK", type = "ERC20", verified = true, source = "bundled", decimals = 18 }
"0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8" = { name = "USDT", type = "ERC20", verified = true, source = "bundled", decimals = 6 }
"0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3" = { name = "DAI", type = "ERC20", verified = true, source = "bundled", decimals = 18 }
"0x0124aeb495b947201f5fac96fd1138e326ad86195b98df6dec9009158a533b49" = { name = "wBTC", type = "ERC20", verified = true, source = "bundled", decimals = 8 }
"0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05" = { name = "wstETH", type = "ERC20", verified = true, source = "bundled", decimals = 18 }
"0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a" = { name = "xSTRK", type = "ERC20", verified = true, source = "bundled", decimals = 18 }
"0x042b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2" = { name = "nstSTRK", type = "ERC20", verified = true, source = "bundled", decimals = 18 }

# DEXes
"0x00000005dd3d2f4429af886cd1a3b08289dbcea99a294197e9eb43b0e0325b4b" = { name = "Ekubo Core", type = "DEX", verified = true, source = "bundled" }
"0x02e0af29598b407c8716b17f6d2795eca1b471413fa03fb145a5e33722184067" = { name = "Ekubo Positions", type = "DEX", verified = true, source = "bundled" }
"0x0199741822c2dc722f6f605204f35e56dbc23bceed54818168c4c49e4fb8737e" = { name = "Ekubo Router", type = "DEX", verified = true, source = "bundled" }
"0x041fd22b238fa21cfcf5dd45a8548974d8263b3a531a60388411c5e230f97023" = { name = "Jediswap Router", type = "DEX", verified = true, source = "bundled" }
"0x010884171baf1914edc28d7afb619b40a4051cfae78a094a55d230f19e944a28" = { name = "MySwap", type = "DEX", verified = true, source = "bundled" }
"0x01b5bd713e72fdc5d63ffd83762f81297f6175a5e0a4771cdadbc1dd5fe72cb1" = { name = "10KSwap Router", type = "DEX", verified = true, source = "bundled" }

# Aggregators
"0x04270219d365d6b017231b52e92b3fb5d7c8378b05e9abc97724537a80e93b0f" = { name = "AVNU Exchange", type = "DEX Aggregator", verified = true, source = "bundled" }
"0x06a09ccb1caaecf3d9683efe335a667b2169a409d19c589ba1eb771cd210af75" = { name = "Fibrous Finance", type = "DEX Aggregator", verified = true, source = "bundled" }

# AVNU internals
"0x06cad934ec5c48d9cdebe63e3139017fa01af101b4c5eff49f30fd6d57d3eebe" = { name = "AVNU Owner", type = "Account", verified = true, source = "bundled" }
"0x0360fb3a51bd291e5db0892b6249918a5689bc61760adcb350fe39cd725e1d22" = { name = "AVNU Fee Recipient", type = "Account", verified = true, source = "bundled" }

# Lending
"0x02545b2e5d519fc230e9cd781046d3a64e092114f07e44771e0d719d148571e3" = { name = "Vesu Singleton", type = "Lending", verified = true, source = "bundled" }
"0x07e2a13b40fc1119ec55e0bcf9428eedaa581ab3c924561ad4e955f95da63138" = { name = "Zklend Market", type = "Lending", verified = true, source = "bundled" }

# Staking
"0x00ca1702e64c81d9a07b86bd2c540188d92a2c73cf5cc0e508d949015e7e84a7" = { name = "Staking Contract", type = "Staking", verified = true, source = "bundled" }

# Bridges
"0x073314940630fd6dcda0d772d4c972c4e0a9946bef9dabf4ef84eda8ef542b82" = { name = "StarkGate ETH Bridge", type = "Bridge", verified = true, source = "bundled" }
"0x05cd48fccbfd8aa2773fe22c217e808319ffcc1c5a6a463f7d8fa2da48218196" = { name = "StarkGate USDC Bridge", type = "Bridge", verified = true, source = "bundled" }

# Infrastructure
"0x01176a1bd84444c89232ec27754698e5d2e7e1a7f1539f12027f28b23ec9f3d8" = { name = "Starknet Sequencer", type = "Sequencer", verified = true, source = "bundled" }
"0x041a78e741e5af2fec34b695679bc6891742439f7afb8484ecd7766661ad02bf" = { name = "UDC", type = "Infrastructure", verified = true, source = "bundled" }
"0x0243eed6e2b2b02a24e249f2b39ef9e5bc8bc32b252174d1c0961a458640ed8a" = { name = "Pragma Oracle", type = "Oracle", verified = true, source = "bundled" }

# Known exchanges/wallets
"0x0620102ea610be8518125cf2de850d0c4f5d0c5d81f969cff666fb53b05042d2" = { name = "Kraken Hot Wallet", type = "Exchange", verified = true, source = "bundled" }
"#;
