# snbeat

snbeat is a local, terminal based, Block explorer for the Starknet Blockchain. It supports multiple data sources, to give the best experience, while prioritizing privacy, recency and caching.

<table>
  <tr>
    <td><strong>Block Detail</strong><br><img src="screenshots/block_detail.png" alt="Block detail view" width="400" height="300"></td>
    <td><strong>Transaction Detail</strong><br><img src="screenshots/tx_detail.png" alt="Transaction detail view" width="400" height="300"></td>
  </tr>
  <tr>
    <td><strong>Address Info</strong><br><img src="screenshots/address_info.png" alt="Address info view" width="400" height="300"></td>
    <td><strong>Class Info</strong><br><img src="screenshots/class_info.png" alt="Class info view" width="400" height="300"></td>
  </tr>
</table>

- Privacy first: snbeat works best with a local RPC node, and can connect directly to the Pathfinder DB. Fully local. External APIs (Dune/Voyager) can be used but are optional. It is open source. Hack any feature you need.
- Cache first: Every fetched data is cached. Any data visited is cached for subsequent queries. No unnecessary indexing of data that is never used.
- Recency first: See recent txs first, stream incoming txs, wait longer for full data fetch.
- Vim!

## Features

- **Data backends** - RPC, WS, Pathfinder query service, Dune and Voyager can be used simultaneously to fetch data
- **ABI-aware decoding** - decodes calldata, events, and multicalls using on-chain class ABIs
- **Persistent local cache** - SQLite; blocks, transactions, receipts, and ABIs are cached for instant fetch on subsequent visits
- **Live feed** - Stream new blocks, transactions, and events as they happen (requires WebSocket support from the RPC)
- **Custom labels** - tag addresses and transactions with human-readable names and searchable tags
- **Fast search** - prefix/substring search over labelled addresses; navigate by hash or block number
- **Nonce-based and Block based navigation** - jump to the next/previous transaction by account, by index, or jump to the next block

---

## Installation

### Prerequisites

- Rust 1.85+ (edition 2024)
- A Starknet RPC endpoint

### Install from crates.io

```bash
cargo install snbeat
```

### Install from git

```bash
cargo install --git https://github.com/amanusk/snbeat
```

Then place your config in `~/.config/snbeat/` (see [Configuration](#configuration) below).

### Build from source

```bash
git clone https://github.com/amanusk/snbeat
cd snbeat
cargo build --release
```

The binary is at `target/release/snbeat`.

### Run

```bash
# From the repo (uses local .env)
cp .env.example .env
cargo run --release

# Or directly
./target/release/snbeat
```

snbeat reads environment variables directly, so you can also export them in your shell or pass them inline:

```bash
APP_RPC_URL=https://api.zan.top/public/starknet-mainnet/rpc/v0_10 snbeat
```

---

## Configuration

snbeat looks for configuration files in two locations, with local files taking priority:

1. **Current working directory** - for development or per-project setups
2. **`~/.config/snbeat/`** - for system-wide installs (e.g. `cargo install`)

This applies to both `.env` and `labels.toml`. If a file exists in the current directory it is used; otherwise snbeat falls back to `~/.config/snbeat/`.

### Quick setup (cargo install)

```bash
mkdir -p ~/.config/snbeat
cp .env.example ~/.config/snbeat/.env
# Edit ~/.config/snbeat/.env with your settings
```

### Environment variables

All variables are optional except `APP_RPC_URL`.

| Variable                     | Default                 | Description                                    |
| ---------------------------- | ----------------------- | ---------------------------------------------- |
| `APP_RPC_URL`                | _(required)_            | Starknet JSON-RPC endpoint                     |
| `APP_WS_URL`                 | -                       | WebSocket endpoint for new-block subscriptions |
| `APP_PATHFINDER_SERVICE_URL` | -                       | URL of a running `pf-query` instance           |
| `VOYAGER_API_KEY`            | -                       | Voyager API key for address metadata           |
| `DUNE_API_KEY`               | -                       | Dune Analytics API key                         |
| `DUNE_PRIVATE_QUERIES`       | `true`                  | Mark dynamic Dune queries private; set `false` to dodge private-query quota |
| `APP_USER_LABELS`            | `labels.toml`           | Path to your custom labels file                |
| `APP_VIEWING_KEYS`           | `viewing_keys.toml`     | Path to privacy-pool viewing keys (held separately because they are secrets) |
| `APP_LOG_LEVEL`              | `info`                  | `trace` / `debug` / `info` / `warn` / `error`  |
| `APP_LOG_DIR`                | `~/.config/snbeat/logs` | Log file directory                             |

---

## Custom Labels

Create a `labels.toml` file to tag addresses and transactions with names and searchable tags. Place it in the current directory or in `~/.config/snbeat/labels.toml` for a global install.

```toml
[addresses]
# Simple name
"0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7" = "ETH"

# Name + tags (tags are searchable in the / search bar)
"0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7" = { name = "My Wallet", tags = ["defi", "nft"] }

[transactions]
# Optional transaction labels
"0xabc123..." = "Initial deploy"
```

User labels take priority over the built-in known address registry (tokens, DEXes, bridges, etc.). The file is loaded at startup; a malformed file prints a warning but does not prevent startup.

---

## ABI Decoding

snbeat fetches the class ABI for every contract it encounters and decodes:

- **Calldata** - function arguments with named parameters and typed values (u256, structs, enums, arrays, …)
- **Multicalls** - account transactions that bundle multiple inner calls are unpacked individually
- **Events** - keys and data fields are matched against the ABI and rendered with parameter names

Decoded ABIs are stored in the local SQLite cache (`~/.config/snbeat/cache.db`) and are never re-fetched.

Press `d` in a transaction detail view to toggle between raw and decoded calldata.

---

## Local Cache

All fetched data is persisted to `~/.config/snbeat/cache.db` (SQLite). The cache stores:

- Block headers
- Transactions and receipts
- Per-address event and transaction lists
- Parsed ABIs (keyed by class hash)

On subsequent visits to the same block or address, data is served from disk with no network round-trip.

---

## Keybindings

Press `?` at any time to open the in-app help overlay; it lists the same controls as the tables below.

### Navigation (all views)

| Key                 | Action                                                            |
| ------------------- | ----------------------------------------------------------------- |
| `j` / `↓`           | Move down                                                         |
| `k` / `↑`           | Move up                                                           |
| `l` / `→` / `Enter` | Drill in / navigate forward                                       |
| `h` / `←` / `Esc`   | Go back                                                           |
| `Ctrl+O`            | Jump back (vim-style)                                             |
| `]`                 | Jump forward in history                                           |
| `g` / `G`           | Jump to top / bottom                                              |
| `Ctrl+U` / `PgUp`   | Page up (list scroll, or active tab body in TxDetail)             |
| `Ctrl+D` / `PgDn`   | Page down                                                         |
| `Ctrl+P` / `Ctrl+N` | Up / down axis (previous / next block or tx, wraps between blocks)|
| `n` / `N`           | Next / previous tx by same sender (nonce-based)                   |
| `Tab` / `Shift+Tab` | Cycle tabs (TxDetail / AddressInfo)                               |
| `r`                 | Refresh current view (Blocks / Address / Class)                   |
| `q`                 | Jump to home, or quit if already at home                          |
| `Ctrl+C`            | Quit                                                              |
| `?`                 | Toggle help overlay                                               |

### Search

Press `/` to open the search bar. You can search by:

- Address, transaction hash, or class hash (0x…)
- Block number
- Label name or tag

Arrow keys to navigate suggestions; `Enter` confirms; `Tab` fills in the highlighted suggestion; `Esc` closes.

### Transaction Detail

Tabs cycle in this order with `Tab` / `Shift+Tab`: **Events / Calls / Transfers / Trace / Privacy***.

| Key | Action                                                              |
| --- | ------------------------------------------------------------------- |
| `c` | Toggle raw calldata (Calls tab)                                     |
| `d` | Toggle ABI-decoded calldata (Calls tab)                             |
| `o` | Toggle outside-execution intent view (SNIP-9 meta-txs, Calls tab)   |
| `e` | Expand everything — un-truncates hashes, expands structs/arrays, forces decoded calldata and OE intent on |
| `v` | Enter visual mode (highlight addresses / block refs)                |

\* The **Privacy** tab is only shown on transactions that interact with the [Starknet Privacy Pool](https://github.com/starknet-id/privacy-pool).

### Address Info

Tabs cycle in this order with `Tab` / `Shift+Tab`: **Transactions / MetaTxs / Calls / Balances / Events / ClassHistory**.

| Key | Action                                                              |
| --- | ------------------------------------------------------------------- |
| `r` | Refresh the address                                                 |
| `v` | Enter visual mode on the header (class, deployer, deploy tx, block) |

### Class Info

| Key | Action                                                          |
| --- | --------------------------------------------------------------- |
| `a` | Toggle the full ABI pane                                        |
| `v` | Enter visual mode (navigate referenced contracts / classes)     |
| `r` | Refresh the class info                                          |

### Visual Mode

Press `v` from a TxDetail, BlockDetail, AddressInfo, or ClassInfo view to enter visual mode. Use `j`/`k` to step through navigable items (addresses, block refs, hashes); `Enter` drills into the highlighted item; `Esc` exits. Inside TxDetail visual mode, `Tab` / `Shift+Tab` still switch tabs (and the highlight snaps to the first item visible in the new tab), and `c` / `d` / `o` still work as toggles.

---

## Data Backends

snbeat can draw data from multiple sources simultaneously. Configure only the ones you have access to.

### Data source matrix

Each cell shows the fetch priority when multiple sources provide the same data (`1` = primary, `2` = fallback, etc.). A dash means the source cannot provide this data.

| Data point                        | RPC            | WS       | PF-query         | Dune | Voyager |
| --------------------------------- | -------------- | -------- | ---------------- | ---- | ------- |
| **Blocks**                        |                |          |                  |      |         |
| Latest block number               | 1 (poll)       | 1 (push) | -                | -    | -       |
| Block header                      | 1              | -        | -                | -    | -       |
| Block transactions                | 1              | -        | 1                | -    | -       |
| **Transactions**                  |                |          |                  |      |         |
| Transaction by hash               | 1              | -        | -                | -    | -       |
| Transaction receipt (fee, status) | 1              | -        | -                | -    | -       |
| Tx hash → block + index lookup    | -              | -        | 1                | -    | -       |
| Calldata                          | 1              | -        | -                | -    | -       |
| Multicall decoding                | 1              | -        | -                | -    | -       |
| **Address - Account history**     |                |          |                  |      |         |
| Account tx history                | 3 (events)     | -        | 1                | 2    | -       |
| Nonce history (timeline)          | -              | -        | 1                | -    | -       |
| Current nonce                     | 1              | -        | -                | -    | -       |
| **Address - Contract history**    |                |          |                  |      |         |
| Contract call history             | 2 (events)     | -        | -                | 1    | -       |
| Contract events                   | 1              | -        | 1                | -    | -       |
| Activity range probe              | -              | -        | -                | 1    | -       |
| **Address - General**             |                |          |                  |      |         |
| Class hash at address             | 1              | -        | -                | -    | -       |
| Token balances (ETH, STRK, …)     | 1 (call)       | -        | -                | -    | -       |
| Address label / metadata          | -              | -        | -                | -    | 1       |
| Deploy tx detection               | 1              | -        | -                | -    | -       |
| **Classes & ABIs**                |                |          |                  |      |         |
| Class definition (Sierra ABI)     | 1              | -        | -                | -    | -       |
| Selector → function/event name    | 1 (via ABI)    | -        | -                | -    | -       |
| Class declaration block           | -              | -        | 1                | 2    | -       |
| Declare tx info                   | 1 (block scan) | -        | 1 (block lookup) | 2    | -       |
| Contracts deployed with class     | -              | -        | 1                | -    | -       |
| Class upgrade history             | -              | -        | 1                | -    | -       |
| **WebSocket subscriptions**       |                |          |                  |      |         |
| New block headers (live)          | -              | 1        | -                | -    | -       |
| Live events for address           | -              | 1        | -                | -    | -       |
| Live transactions from address    | -              | 1        | -                | -    | -       |
| **Search**                        |                |          |                  |      |         |
| Block number lookup               | 1              | -        | -                | -    | -       |
| Transaction hash search           | 1              | -        | -                | -    | -       |
| Address resolution                | 1              | -        | -                | -    | -       |
| Class hash search                 | 1              | -        | -                | -    | -       |
| Block hash search                 | 1              | -        | -                | -    | -       |
| Label / tag search                | -              | -        | -                | -    | -       |

> **Note:** All fetched data is cached locally in SQLite. Subsequent visits serve from cache with no network round-trip. Label/tag search is powered by the local registry (`labels.toml` + built-in known addresses) with no external data source.

### RPC (required)

All data fetching starts here. Set `APP_RPC_URL` to any Starknet JSON-RPC v0.7+ endpoint (local node or hosted).

```
APP_RPC_URL=http://localhost:9545/rpc/v0_10
```

An optional WebSocket URL enables push-based block, txs and events

```
APP_WS_URL=ws://localhost:9545/ws
```

### Pathfinder Query Service (optional, recommended for local nodes)

If you run a [Pathfinder](https://github.com/eqlabs/pathfinder) node, the bundled `pf-query` service exposes its SQLite database over HTTP. This unlocks fast data lookups, which power the address timeline.

See [Setting up pf-query](#setting-up-pf-query) below.

```
APP_PATHFINDER_SERVICE_URL=http://localhost:8234
```

### Dune Analytics (optional)

A Dune API key enables additional address history features:

- **Account transaction history** - full transaction list for an account, used as a fallback when Pathfinder is unavailable
- **Contract call history** - the _Calls_ tab on an address shows all transactions that called that contract
- **Activity range probe** - detects the active block range for an address

```
DUNE_API_KEY=your_key_here
```

### Voyager API (optional)

Connect to Voyager API to optionally get Voyager tags for addresses, classes etc

```
VOYAGER_API_KEY=your_key_here
```

---

## Setting up pf-query

`pf-query` is a lightweight HTTP service that exposes the Pathfinder SQLite database for fast nonce-history queries. It lives in `crates/pf-query/`.

### Build and run locally

```bash
# Build
cargo build --release --manifest-path crates/pf-query/Cargo.toml

# Run (point it at your Pathfinder database)
PF_DB_PATH=/var/lib/pathfinder/pathfinder.db ./target/release/pf-query
# Listening on 0.0.0.0:8234 by default
```

| Variable     | Default      | Description                            |
| ------------ | ------------ | -------------------------------------- |
| `PF_DB_PATH` | _(required)_ | Path to the Pathfinder SQLite database |
| `PF_PORT`    | `8234`       | Port to listen on                      |
| `PF_HOST`    | `127.0.0.1`  | Host address to bind to                |

### Run on a remote server

If your Pathfinder node is on a different machine, run `pf-query` on that machine and set `APP_PATHFINDER_SERVICE_URL` in your snbeat `.env` to point at it:

```
APP_PATHFINDER_SERVICE_URL=http://192.168.1.10:8234
```

### API

| Endpoint                                   | Description                                                                                              |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `GET /health`                              | Returns `{ "latest_block": N }`                                                                          |
| `GET /nonce-history/{address}?limit=N`     | Ordered nonce update history (max 2000 entries)                                                          |
| `GET /class-history/{address}`             | Class hash history for a contract                                                                        |
| `GET /contracts-by-class/{class_hash}`     | Contracts deployed with a given class hash                                                               |
| `GET /class-declaration/{class_hash}`      | Declaration info for a class hash                                                                        |
| `GET /tx-by-hash/{hash}`                   | Looks up a transaction by hash → `(block_number, tx_index)`                                              |
| `POST /txs-by-hash`                        | Bulk lookup: body `{ hashes: [...] }`, returns sender / nonce / calldata / fee / status per tx           |
| `POST /storage-batch`                      | Read many storage slots from one contract at a block in a single SQLite transaction                      |
| `GET /block-txs/{block_number}`            | Decoded transactions in a block                                                                          |
| `GET /block-timestamps?from=N&to=M`        | Bulk block timestamps (single indexed range scan; max span 50 000)                                       |
| `GET /sender-txs/{address}`                | Transactions sent by an address. Supports `?limit=N&before_block=B&from_block=F` for pagination.         |
| `GET /contract-events/{address}`           | Events emitted by a contract, accelerated by bloom filters. Supports `?from_block`, `?to_block`, `?keys` (positional filter), `?limit`, `?continuation_token` for newest-first pagination |
| `GET /contract-event-count/{address}`      | Total event count over a range. Cheaper than `/contract-events` (skips tx-hash + timestamp decoding)     |

See [`crates/pf-query/README.md`](crates/pf-query/README.md) for full request / response shapes and query-parameter syntax.

---

## Testing

```bash
# Unit and integration tests (no network required)
cargo test

# RPC-dependent integration tests (requires APP_RPC_URL in .env)
cargo test -- --ignored
```

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contributing

1. Fork the repository and create a feature branch.
2. Run `cargo fmt` and `cargo clippy` before opening a PR.
3. Add tests for new behaviour where practical.
4. Keep PRs focused; one feature or fix per PR.

Please open an issue first for significant changes to discuss the approach.
