# grin-lws: Grin light-wallet-server

A light-wallet-server for Grin, mirroring [monero-lws]: register a view
credential, run a background scanner over the Grin chain, store recognized
outputs (with recovered derivation paths) in a database, and serve
balance, unspent-outputs, broadcast, and height fast from that store.

It is the Grin analogue of monero-lws. A wallet backend proxies to it the same
way it proxies to a Monero LWS: forward a view credential, serve the stored
results back to the wallet.

Status: functional and running in production. The view-only rangeproof rewind,
the PMMR-index node reads, and the background scanner (discover, spend-reconcile,
reorg, backfill) are implemented and tested. The rewind is cross-validated
against grin's own reference `ProofBuilder` and passed an adversarial money-safety
review. An end-to-end test drives the real scanner against a mock grin node
(modeled on Foreign API v2) over both SQLite and Postgres, and it has been
validated against a live grin node with a funded wallet.

[monero-lws]: https://github.com/vtnerd/monero-lws

---

## Why this exists (and why it needs a DB)

A common wallet-backend approach is an on-demand, stateless grin scan: each
request forwards a `rewind_hash` to grin-wallet's `scan_rewind_hash`, which
rescans the chain and returns matches, storing nothing. That works for Grin's
small UTXO set, but it is not a light-wallet-server: every read re-pays the scan
cost, and it has no output store to recover derivation paths from.

A real LWS, like monero-lws, keeps a per-account output store maintained by a
background scanner, so client reads are O(rows) not O(chain), and it can return
each output's derivation path. That store is a database, hence this service and
the three tables in `migrations/`.

The payoff for the wallet: `get_unspent_outs` returns outputs with `key_id` and
`n_child`, so the client spends directly, with no client-side identify search. It
closes the path-identification gap that an on-demand scan leaves open.

Non-custodial, always. An account is a `rewind_hash`, a view credential derived
from the wallet's public root key (Grin has had view/spend separation since
2021). It can recognize outputs and read amounts, but it cannot spend. This
service never stores a private or spend key.

---

## Architecture

Three parts, matching monero-lws's user-api, admin-api, and scanner:

```
                          ┌──────────────────────────────────────────┐
 wallet ──JWT──▶ backend ─┤  grin-lws                                 │
   /wallet/grin/*   proxy │                                           │
                          │  1. REGISTRATION   POST /register         │
                          │       rewind_hash -> accounts row         │
                          │                                           │
                          │  2. SCANNER (background task)             │
                          │       walk the output PMMR by index,      │
                          │       rewind each output vs each account, │
                          │       store matches, spend-reconcile,     │
                          │       reorg-safe chain_cursor             │
                          │                     │                     │
                          │                     ▼                     │
                          │  3. USER API      ┌─────────┐             │
                          │     read from ───▶│   DB    │◀── writes   │
                          │     the store     └─────────┘   (scanner) │
                          └───────────────────────┬──────────────────┘
                                                   │
                                          grin node (Foreign API v2)
```

1. Registration: `POST /register {rewind_hash, start_height?}` inserts an
   `accounts` row that the scanner will follow. It is idempotent (the monero-lws
   `add_account` analog, but self-service and view-only, so no spend risk).
2. Background scanner: Grin's chain state is the set of unspent outputs in a
   Merkle Mountain Range, so the scanner walks that set in PMMR insertion-index
   order (not block by block; `get_block` does not even return rangeproofs). Each
   tick pages new outputs from the cursor via `get_unspent_outputs` (with
   proofs), rewinds each against every registered `rewind_hash`, and on a match
   stores the output together with the derivation path recovered from the proof
   message. Spends are detected by absence: a stored commit the node no longer
   lists has been spent. A reorg-safe `chain_cursor` rolls back rows above the
   fork.
3. User API: reads served from the DB store (fast), plus broadcast and tip.

---

## HTTP surface

The user API is unauthenticated at this layer: grin-lws binds to loopback or a
private network and trusts the proxying backend (which does JWT auth) as its sole
caller. The admin API is gated by a shared bearer (`GRINLWS_ADMIN_KEY`) and
disabled when that is unset.

### User API (mirrors monero-lws user endpoints)

| Route | Body | Returns |
|-------|------|---------|
| `POST /register` | `{rewind_hash, start_height?}` | `{registered, new_account, scan_height, start_height}` |
| `POST /get_balance` | `{rewind_hash}` | `{total, unlocked, count, scanned_height, blockchain_height}` |
| `POST /get_unspent_outs` | `{rewind_hash}` | `{outputs[], blockchain_height}` |
| `POST /submit_raw_tx` | `{tx}` | `{ok}` |
| `GET /height` | | `{height}` |
| `GET /health` | | liveness (always 200 if up) |
| `GET /ready` | | readiness (502 if the node is unreachable) |

`get_unspent_outs` outputs carry the recovered path:
```jsonc
{ "commit": "09…", "value": 1000000000, "height": 1851234,
  "mmr_index": 987654, "is_coinbase": false, "lock_height": 0,
  "spendable": true, "key_id": "0300000000…", "n_child": 7 }
```
`key_id` is the authoritative derivation identifier, so the client spends without
a local identify search. `spendable` is `lock_height <= blockchain_height`, so an
immature coinbase is never presented as spendable.

### Admin API (bearer-gated)

| Route | Body | Purpose |
|-------|------|---------|
| `POST /list_accounts` | | enumerate registered `rewind_hash`es |
| `POST /rescan` | `{rewind_hash, height}` | backwards-only re-derive from `height` |

`/rescan` is backwards-only (it mirrors the monero-lws rescan invariant): it
clears stored outputs at or above `height` and lowers `scan_height`, so the
scanner re-derives them. A rescan to a height at or above the current scan height
is rejected as a no-op.

---

## Database schema

See `migrations/0001_init.sql`. It runs unchanged on both PostgreSQL and SQLite
(verified in a test). It never stores a private or spend key.

```
accounts(rewind_hash PK, start_height, scan_height, created_at)
outputs("commit" PK, rewind_hash → accounts, value, height, mmr_index,
        is_coinbase, lock_height, key_id, n_child, spent, spent_height)
        INDEX(rewind_hash, spent), INDEX("commit")
chain_cursor(id=1 PK, output_mmr_index, height, block_hash)
```

Notes:
- `outputs.key_id` and `n_child` are recovered during rewind; they are the
  direct-spend enabler.
- `outputs.value` is stored as a decimal string, because sqlx's `Any` driver
  truncates i64 to i32 for SQLite. Amounts are summed in Rust.
- `outputs.spent` and `spent_height` are set when a stored commit leaves the
  node's unspent set.
- `chain_cursor` holds the scanner's resume point: `output_mmr_index` is how far
  it has walked the output PMMR; `height` and `block_hash` are the chain tip it
  last saw, the reorg checkpoint.

---

## How a wallet backend proxies to it

Wiring identical to a Monero LWS client: forward the view credential, serve the
stored results.

```
wallet ──JWT──▶ wallet backend ──private──▶ grin-lws ──▶ grin node
  /wallet/grin/scan             /get_unspent_outs         (Foreign API v2)
```

1. Add a grin-lws client config (`url`, optional `admin_url` and `admin_key`)
   alongside your existing Monero-LWS config.
2. Add an HTTP client mirroring your Monero-LWS client: a shared `reqwest::Client`,
   timeouts, size-capped hostile-response reads, and a redacted admin key.
3. Behind a feature flag, have your grin scan endpoint forward the `rewind_hash`
   to grin-lws `/get_unspent_outs` (which returns outputs with paths) instead of
   driving grin-wallet `scan_rewind_hash` on-demand.
4. Register at balance-fetch time by forwarding `rewind_hash` to grin-lws
   `/register` (idempotent), exactly as you await LWS registration for Monero.
5. Trust grin-lws only once it is synced (`scanned_height` near
   `blockchain_height`); while an account is still backfilling, fall back to the
   authoritative grin-wallet scan so a not-yet-scanned account never reports a
   false zero.

The non-custodial guarantee is preserved end to end: the backend forwards only
the `rewind_hash` and stores nothing; grin-lws holds the `rewind_hash` for
scanning, just as monero-lws holds the view key. No spend key ever leaves the
wallet.

Once grin-lws is wired in, the wallet client can drop any client-side
output-identify helper: `/get_unspent_outs` returns paths directly.

---

## Running

```sh
# 1. Provision a DB and run the migration.
#    Postgres:  createdb grinlws && psql grinlws < migrations/0001_init.sql
#    SQLite:    sqlite3 grin-lws.db < migrations/0001_init.sql
# 2. Configure.
cp .env.example .env    # edit DATABASE_URL and GRIN_NODE_URL
# 3. Run.
cargo run               # binds GRINLWS_BIND (default 127.0.0.1:3480)
curl localhost:3480/health
```

`/height`, `/submit_raw_tx`, and `/ready` require a reachable grin node (they
return `502` otherwise). `/register`, `/get_balance`, and `/get_unspent_outs`
read from the DB store, which the background scanner fills once it has a grin node
to page the output set from. `/health` always works.

On a fresh database the scanner first walks the whole unspent-output set (a large,
one-time download), then keeps up incrementally. Register accounts with their
wallet birthday (`start_height`), not from genesis, so each account's backfill
stays small.

## Deployment

Run as a private service alongside a grin node and its database. Bind to loopback
or a private network, and expose it only to the proxying backend. Add a readiness
probe on `/ready`. Everything is env-configured, so no code changes to deploy.

---

## License and provenance

MIT; see [`LICENSE`](LICENSE).

grin-lws was extracted from the [Smirk](https://smirk.cash) wallet's backend and
generalized into a standalone service that any wallet backend can run.
