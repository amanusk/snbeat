# snbeat

A terminal-based Starknet block explorer with ABI-aware decoding, multi-source data backends, and a persistent local cache.

```
┌──────────────────────────────────────────────────────────────┐
│  Block List  │  Transaction Detail  │  Address Info           │
│──────────────┼──────────────────────┼─────────────────────────│
│ #1234567     │ Hash: 0xabc…         │ 0xdef…  [My Wallet]│
│ #1234566     │ From: 0xdef…         │ Events  Calls  Txs       │
│ #1234565     │ transfer(…)          │  ├─ Transfer(…)          │
│  …           │   to:  0x049d… ETH  │  ├─ Approval(…)         │
└──────────────┴──────────────────────┴─────────────────────────┘
```

---

## Features

- **Live block feed** — polls the chain every 3 s (or subscribes via WebSocket)
- **ABI-aware decoding** — decodes calldata, events, and multicalls using on-chain class ABIs
- **Three data backends** — RPC, Pathfinder query service, and Dune Analytics (mix and match)
- **Persistent local cache** — SQLite; blocks, transactions, receipts, and ABIs are never re-fetched
- **Custom labels** — tag addresses and transactions with human-readable names and searchable tags
- **Fast search** — prefix/substring search over labelled addresses; navigate by hash or block number
- **Nonce-based navigation** — jump to the next/previous transaction by the same sender (`n`/`N`)

---

## Data Backends

snbeat can draw data from three sources simultaneously. Configure only the ones you have access to.

### RPC (required)

All data fetching starts here. Set `APP_RPC_URL` to any Starknet JSON-RPC v0.7+ endpoint (local node or hosted).

```
APP_RPC_URL=http://localhost:9545/rpc/v0_10
```

An optional WebSocket URL enables push-based block subscriptions instead of polling:

```
APP_WS_URL=ws://localhost:9545/ws
```

### Pathfinder Query Service (optional, recommended for local nodes)

If you run a [Pathfinder](https://github.com/eqlabs/pathfinder) node, the bundled `pf-query` service exposes its SQLite database over HTTP. This unlocks fast nonce-history lookups, which power the `n`/`N` navigation keys and the address timeline.

See [Setting up pf-query](#setting-up-pf-query) below.

```
APP_PATHFINDER_SERVICE_URL=http://localhost:8234
```

### Dune Analytics (optional)

A Dune API key enables two additional features:

- **Reverted transaction detection** — reverted txs appear differently from successful ones
- **Contract call history** — the *Calls* tab on an address shows all transactions that called that contract

```
DUNE_API_KEY=your_key_here
```

---

## Installation

### Prerequisites

- Rust 1.85+ (edition 2024)
- A Starknet RPC endpoint

### Build

```bash
git clone https://github.com/yourorg/snbeat
cd snbeat
cargo build --release
```

The binary is at `target/release/snbeat`.

### Run

```bash
cp .env.example .env
# Edit .env with your settings
cargo run --release
# or
./target/release/snbeat
```

snbeat reads environment variables directly, so you can also export them in your shell or pass them inline:

```bash
APP_RPC_URL=http://localhost:9545/rpc/v0_10 ./target/release/snbeat
```

---

## Configuration

Copy `.env.example` to `.env` and fill in the variables you need. All variables are optional except `APP_RPC_URL`.

| Variable | Default | Description |
|---|---|---|
| `APP_RPC_URL` | *(required)* | Starknet JSON-RPC endpoint |
| `APP_WS_URL` | — | WebSocket endpoint for new-block subscriptions |
| `APP_PATHFINDER_SERVICE_URL` | — | URL of a running `pf-query` instance |
| `VOYAGER_API_KEY` | — | Voyager API key for address metadata |
| `DUNE_API_KEY` | — | Dune Analytics API key |
| `APP_USER_LABELS` | `labels.toml` | Path to your custom labels file |
| `APP_KNOWN_ADDRESSES` | `known.toml` | Path to the curated address registry |
| `APP_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |
| `APP_LOG_DIR` | `~/.config/snbeat/logs` | Log file directory |

---

## Custom Labels

Create a `labels.toml` file (or set `APP_USER_LABELS`) to tag addresses and transactions with names and searchable tags.

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

User labels take priority over the built-in `known.toml` registry. The file is loaded at startup; a malformed file prints a warning but does not prevent startup.

---

## ABI Decoding

snbeat fetches the class ABI for every contract it encounters and decodes:

- **Calldata** — function arguments with named parameters and typed values (u256, structs, enums, arrays, …)
- **Multicalls** — account transactions that bundle multiple inner calls are unpacked individually
- **Events** — keys and data fields are matched against the ABI and rendered with parameter names

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

### Navigation

| Key | Action |
|---|---|
| `j` / `↓` | Move down |
| `k` / `↑` | Move up |
| `l` / `→` / `Enter` | Drill in / navigate forward |
| `h` / `←` / `Esc` | Go back |
| `Ctrl+O` | Jump back (vim-style) |
| `]` | Jump forward |
| `g` | Jump to top |
| `G` | Jump to bottom |
| `Ctrl+U` / `PgUp` | Previous block or transaction |
| `Ctrl+D` / `PgDn` | Next block or transaction |
| `n` | Next transaction by same sender |
| `N` | Previous transaction by same sender |
| `Tab` | Cycle tabs (Address Info view) |
| `q` | Jump to home / quit |
| `Ctrl+C` | Quit |

### Search

Press `/` to open the search bar. You can search by:

- Address or transaction hash (0x…)
- Block number
- Label name or tag

`j`/`k` navigate suggestions; `Enter` confirms; `Tab` fills in the highlighted suggestion; `Esc` closes.

### Transaction Detail

| Key | Action |
|---|---|
| `c` | Toggle raw calldata |
| `d` | Toggle ABI-decoded calldata |
| `v` | Enter visual mode (highlight addresses / block refs) |
| `r` | Refresh |
| `?` | Toggle help overlay |

### Visual Mode

Press `v` in a Transaction or Block detail view to enter visual mode. Use `j`/`k` to cycle through addresses and block references, then `Enter` to navigate to the highlighted item. `Esc` exits.

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

| Variable | Default | Description |
|---|---|---|
| `PF_DB_PATH` | *(required)* | Path to the Pathfinder SQLite database |
| `PF_PORT` | `8234` | Port to listen on |

### Run on a remote server

If your Pathfinder node is on a different machine, run `pf-query` on that machine and set `APP_PATHFINDER_SERVICE_URL` in your snbeat `.env` to point at it:

```
APP_PATHFINDER_SERVICE_URL=http://192.168.1.10:8234
```

### API

| Endpoint | Description |
|---|---|
| `GET /health` | Returns `{ "latest_block": N }` |
| `GET /nonce-history/{address}?limit=N` | Returns ordered nonce update history (max 2000 entries) |

---

## Populating the Selector Registry

The `populate_selectors` script fetches all declared class hashes from Dune and pre-populates the local ABI cache, so function and event names resolve immediately without waiting for individual contract lookups.

Requires both `APP_RPC_URL` and `DUNE_API_KEY`.

```bash
cargo test populate_selectors
```

---

## Testing

```bash
# Unit and integration tests (no network required)
cargo test

# RPC-dependent integration tests (requires APP_RPC_URL in .env)
cargo test -- --ignored
```

---

## Contributing

1. Fork the repository and create a feature branch.
2. Run `cargo fmt` and `cargo clippy` before opening a PR.
3. Add tests for new behaviour where practical.
4. Keep PRs focused; one feature or fix per PR.

Please open an issue first for significant changes to discuss the approach.
