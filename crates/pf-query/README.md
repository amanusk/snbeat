# pf-query

A small HTTP service that exposes a [Pathfinder](https://github.com/eqlabs/pathfinder) node's
SQLite database as a JSON API. It powers the fast lookups used by
[snbeat](https://github.com/amanusk/snbeat) (nonce history, contract events,
account transaction history, storage batch reads, etc.) but works as a
standalone service for any client that wants cheap, indexed access to a
Pathfinder DB without going through the node's RPC.

The service is **read-only**: it opens the Pathfinder SQLite database with
`SQLITE_OPEN_READ_ONLY` and never writes. You can safely run it side-by-side
with a live Pathfinder node.

## Why this exists

Pathfinder already stores a fully-indexed copy of Starknet state on disk. Going
back through the Starknet JSON-RPC interface to answer questions like _"give me
this account's last 500 nonce updates"_ or _"find every event this contract
emitted in the last 100k blocks"_ is wasteful — pf-query reads the data
directly out of SQLite (and decompresses the relevant zstd+bincode blobs) in a
single round trip.

Hot paths (block-blob decode, bloom-filter walks) run on a blocking thread
pool with `rayon` parallelism so the tokio worker is never blocked and the
server's cores are fully used.

## Install

From crates.io:

```bash
cargo install pf-query
```

From source:

```bash
git clone https://github.com/amanusk/snbeat
cd snbeat
cargo build --release --manifest-path crates/pf-query/Cargo.toml
```

The binary is `pf-query`. It needs **read access** to a Pathfinder SQLite
database (`pathfinder.db`). For best performance, run it on the same machine as
your Pathfinder node.

## Run

```bash
PF_DB_PATH=/var/lib/pathfinder/pathfinder.db pf-query
# Listening on 127.0.0.1:8234
```

Flags (all also available as environment variables):

| Flag         | Env var         | Default     | Description                            |
| ------------ | --------------- | ----------- | -------------------------------------- |
| `--db-path`  | `PF_DB_PATH`    | _required_  | Path to the Pathfinder SQLite database |
| `--port`     | `PF_PORT`       | `8234`      | Port to listen on                      |
| `--host`     | `PF_HOST`       | `127.0.0.1` | Host address to bind to                |

On startup, pf-query opens the DB, reads the latest block, and runs a decode
smoke test on the latest block's transaction blob. If any of these fail the
process exits — this catches stale paths and schema mismatches up front.

Logging uses `tracing-subscriber` with `RUST_LOG`-style filtering. The default
is `pf_query=info,tower_http=debug`.

## API

All endpoints return JSON. Errors come back with an appropriate HTTP status
code and a plain-text body.

### `GET /health`

Returns the latest block number known to the database. Useful as a liveness
check.

```json
{ "latest_block": 1234567 }
```

### `GET /nonce-history/{address}?limit=N`

Nonce updates for an account, newest-first. `limit` defaults to 500, clamped
to 2000.

```json
[
  { "block_number": 1234567, "nonce": 42, "timestamp": 1715000000 },
  { "block_number": 1234560, "nonce": 41, "timestamp": 1714999000 }
]
```

### `GET /class-history/{address}`

Every class-hash change (deploys + upgrades) for a contract, newest-first.

```json
[
  { "block_number": 1234567, "class_hash": "0xabc..." }
]
```

### `GET /contracts-by-class/{class_hash}`

Up to 500 contracts deployed with the given class hash, newest-first.

```json
[
  { "contract_address": "0x...", "block_number": 1234567 }
]
```

### `GET /class-declaration/{class_hash}`

Block where a class was first declared. Returns `404` if the class is not
known to the DB.

```json
{ "block_number": 1234000 }
```

### `GET /tx-by-hash/{hash}`

Locate a transaction by hash. Returns `404` if unknown.

```json
{ "block_number": 1234567, "tx_index": 3 }
```

### `POST /txs-by-hash`

Bulk lookup of full tx + receipt data by hash. Groups requested hashes by
block, decodes each block's transaction blob at most once, and returns
sender, nonce, calldata, fee, status, and block timestamp for every found
hash. Unknown hashes are silently omitted.

Body:

```json
{ "hashes": ["0x...", "0x..."] }
```

Response: array of

```json
{
  "hash": "0x...",
  "block_number": 1234567,
  "block_timestamp": 1715000000,
  "tx_index": 3,
  "sender": "0x...",
  "nonce": 42,
  "tx_type": "INVOKE_V3",
  "calldata": ["0x...", "0x..."],
  "actual_fee": "0x...",
  "tip": 0,
  "status": "OK",
  "revert_reason": null
}
```

Server-side cap: 10 000 hashes per request.

### `POST /storage-batch`

Read many storage slots from a single contract at a given block, in one
SQLite transaction. Mirrors Pathfinder's own `storage_value(block, contract,
key)` query. Missing slots surface as `"0x0"`.

Body:

```json
{
  "contract": "0x...",
  "keys": ["0x1", "0x2", "0x3"],
  "block": "latest"
}
```

`block` is either the literal `"latest"` or a decimal block number. Defaults
to `"latest"`. Hashes are not accepted.

Response:

```json
{
  "values": ["0x...", "0x0", "0x..."],
  "block_number": 1234567
}
```

`block_number` is the concrete block the read was pinned to (after `latest`
resolution), so the caller can use the same snapshot for subsequent decoding.

Server-side cap: 5000 keys per request.

### `GET /block-txs/{block_number}`

Decoded transactions for a single block. Returns `404` if the block is not
indexed.

```json
[
  {
    "hash": "0x...",
    "sender": "0x...",
    "nonce": 42,
    "tx_type": "INVOKE_V3",
    "actual_fee": "0x...",
    "tip": 0,
    "status": "OK",
    "revert_reason": null
  }
]
```

### `GET /block-timestamps?from=N&to=M`

Bulk block-timestamp lookup over an inclusive `[from, to]` range. Single
indexed range scan on `block_headers`. Server-side cap: 50 000 blocks per
request.

```json
[
  { "block_number": 1234560, "timestamp": 1714999000 },
  { "block_number": 1234561, "timestamp": 1714999005 }
]
```

### `GET /sender-txs/{address}`

Full transaction history for an account, combining `nonce_updates` (to find
the relevant blocks) with transaction-blob decoding (to surface hash, fee,
status, type). Newest-first.

Query parameters:

| Param          | Description                                              |
| -------------- | -------------------------------------------------------- |
| `limit`        | Max entries (default 500, clamped to 2000).              |
| `before_block` | Exclusive upper bound on `block_number` — paginate down. |
| `from_block`   | Inclusive lower bound on `block_number`.                 |

Response: array of

```json
{
  "hash": "0x...",
  "sender_address": "0x...",
  "nonce": 42,
  "block_number": 1234567,
  "timestamp": 1715000000,
  "tx_type": "INVOKE_V3",
  "actual_fee": "0x...",
  "tip": 0,
  "status": "OK",
  "revert_reason": null
}
```

### `GET /contract-events/{address}`

Events emitted by a contract, accelerated by Pathfinder's persisted bloom
filters (`event_filters` table). Walks the newest blocks first and paginates
via a continuation token.

Query parameters:

| Param                | Description                                                                 |
| -------------------- | --------------------------------------------------------------------------- |
| `from_block`         | Inclusive lower bound. Defaults to `0`.                                     |
| `to_block`           | Inclusive upper bound. Defaults to latest block.                            |
| `limit`              | Max events per page (default 500, clamped to 5000).                         |
| `continuation_token` | Pagination cursor returned by a previous call. Takes precedence over `to_block`. |
| `keys`               | Positional key filter (see below).                                          |

**Key filter syntax.** Groups are separated by `;`, OR-keys within a group by
`,`. An empty group is a wildcard for that position. Example:

```
keys=0x3db,0x0af;;0xc2f
```

means `(key[0] IN {0x3db, 0x0af}) AND (key[2] == 0xc2f)`.

Response:

```json
{
  "events": [
    {
      "tx_index": 0,
      "event_index": 1,
      "tx_hash": "0x...",
      "from_address": "0x...",
      "keys": ["0x..."],
      "data": ["0x..."],
      "block_number": 1234567,
      "timestamp": 1715000000
    }
  ],
  "continuation_token": 1234560
}
```

When `continuation_token` is `null`, the scan reached `from_block`. When it
is set, pass it back as `continuation_token` (or `to_block`) on the next
request to continue newest-first.

A per-request candidate-block cap (10 000) bounds latency on very dense
contracts. If the cap is hit, you'll get a continuation token even when the
response has fewer than `limit` events.

### `GET /contract-event-count/{address}`

Total count of events emitted by a contract over a block range, with the
same key-filter syntax as `/contract-events`. Skips per-block tx-hash and
timestamp decoding, so it can afford to scan much more of the chain in a
single request (cap: 200 000 candidate blocks).

Query parameters: `from_block`, `to_block`, `keys` (same semantics as above).

Response:

```json
{
  "count": 12345,
  "min_block": 100000,
  "max_block": 1234567,
  "complete": true,
  "scanned_from": 0,
  "scanned_to": 1234567
}
```

When `complete` is `false`, the candidate cap was reached and `count` is a
lower bound — the caller should narrow the range or accept the floor.

## Database compatibility

pf-query targets the Pathfinder DB schema as of late 2024 / 2025 mainline
releases. It reads from:

- `block_headers`
- `transaction_hashes`
- `transactions` (zstd+bincode-compressed `transactions` and `events` blobs)
- `nonce_updates`, `contract_addresses`
- `contract_updates`
- `class_definitions`
- `storage_updates`, `storage_addresses`
- `event_filters` (aggregate bloom filters)

If Pathfinder changes blob encoding or schema in a future release, expect to
need a matching pf-query update.

## License

MIT. See `LICENSE` in the repository root.
