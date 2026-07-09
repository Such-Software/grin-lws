# grin-lws — Grin light-wallet-server

A **real** light-wallet-server for Grin, mirroring [monero-lws]: register a
view credential, run a **background scanner** over the Grin chain, **store**
recognized outputs (with recovered derivation paths) in a **database**, and
serve balance / unspent-outputs / broadcast / height fast from that store.

It is the Grin analogue of monero-lws: a wallet backend proxies to it exactly the
way a backend proxies to the Monero LWS — forward a view credential, serve the
stored results back to the wallet.

**Status: FUNCTIONAL.** The view-only rangeproof rewind, the PMMR-index node
reads, and the background scanner (discover / spend-reconcile / reorg / backfill)
are all implemented and tested. The rewind is cross-validated against grin's own
reference `ProofBuilder` and passed an adversarial money-safety review; an
end-to-end test drives the real scanner against a mock grin node (modeled on the
Foreign API v2) over both SQLite and Postgres. Remaining before production: a
smoke test against a live grin node with a funded wallet. See
[Build order](#build-order).

[monero-lws]: https://github.com/vtnerd/monero-lws

---

## Why this exists (and why it needs a DB)

A common wallet-backend approach is an **on-demand, stateless** grin scan: each
request forwards a `rewind_hash` to grin-wallet's `scan_rewind_hash`, which
rescans the chain and returns matches, **storing nothing**. That works for
Grin's small UTXO set, but it is *not* a light-wallet-server — every read
re-pays the scan cost, and it has no output store to recover derivation paths
from.

A real LWS — like monero-lws — keeps a **per-account output store** maintained by
a **background scanner**, so client reads are O(rows) not O(chain), and it can
return each output's **derivation path**. That store is a database. Hence this
service, and hence the three tables in `migrations/`.

The payoff for the wallet: `get_unspent_outs` returns outputs *with* `key_id` /
`n_child`, so the client spends directly with **no client-side identify search**
(it closes the path-identification gap that on-demand scan leaves open).

**Non-custodial, always.** An account is a `rewind_hash` — a *view credential*
derived from the wallet's public root key (Grin has had view/spend separation
since 2021). It can recognize outputs and read amounts but **cannot spend**. This
service never stores a private or spend key.

---

## Architecture

Three parts, matching monero-lws's user-api + admin-api + scanner:

```
                          ┌──────────────────────────────────────────┐
 wallet ──JWT──▶ backend ─┤  grin-lws                                 │
   /wallet/grin/*   proxy │                                           │
                          │  1. REGISTRATION   POST /register         │
                          │       rewind_hash -> accounts row         │
                          │                                           │
                          │  2. SCANNER (background task)             │
                          │       follow tip -> per block:            │
                          │         rewind outputs vs each account,   │
                          │         store matches + spend detection,  │
                          │         reorg-safe chain_cursor           │
                          │                     │                     │
                          │                     ▼                     │
                          │  3. USER API      ┌─────────┐             │
                          │     read from ───▶│   DB    │◀── writes   │
                          │     the store     └─────────┘   (scanner) │
                          └───────────────────────┬──────────────────┘
                                                   │
                                          grin node (Foreign API v2)
                                        get_tip / get_block / push_transaction
```

1. **Registration** — `POST /register {rewind_hash, start_height?}` inserts an
   `accounts` row that the scanner will follow. Idempotent (the monero-lws
   `add_account` analog, but self-service and view-only, so no spend risk).
2. **Background scanner** (the hard part; its own long-lived task) — follows the
   chain tip. For each new block: rewind each output's rangeproof against every
   registered `rewind_hash`; on a match, store the output **and** the derivation
   path recovered from the proof message; detect spends by matching input
   commitments against stored outputs; maintain a reorg-safe `chain_cursor`
   (roll back rows above the fork on a reorg).
3. **User API** — reads served from the DB store (fast), plus broadcast + tip.

---

## HTTP surface

The **user API** is unauthenticated at this layer — grin-lws binds to loopback /
a private network and trusts the proxying backend (which does JWT auth) as its
sole caller. The **admin API** is gated by a shared bearer (`GRINLWS_ADMIN_KEY`)
and disabled when that is unset.

### User API (mirrors monero-lws user endpoints)

| Route | Body | Returns |
|-------|------|---------|
| `POST /register` | `{rewind_hash, start_height?}` | `{registered, new_account, scan_height, start_height}` |
| `POST /get_balance` | `{rewind_hash}` | `{total, count, scanned_height, blockchain_height}` |
| `POST /get_unspent_outs` | `{rewind_hash}` | `{outputs[], blockchain_height}` |
| `POST /submit_raw_tx` | `{tx}` | `{ok}` |
| `GET /height` | — | `{height}` |
| `GET /health` | — | liveness (always 200 if up) |
| `GET /ready` | — | readiness (503 if the node is unreachable) |

`get_unspent_outs` outputs — **crucially with the recovered path**:
```jsonc
{ "commit": "09…", "value": 1000000000, "height": 1851234,
  "mmr_index": 987654, "is_coinbase": false, "lock_height": 0,
  "key_id": "0300000000…", "n_child": 7 }   // ← direct-spend, no client identify
```

### Admin API (bearer-gated)

| Route | Body | Purpose |
|-------|------|---------|
| `POST /list_accounts` | — | enumerate registered `rewind_hash`es |
| `POST /rescan` | `{rewind_hash, height}` | **backwards-only** re-derive from `height` |

`/rescan` is backwards-only (mirrors the monero-lws rescan invariant): it clears
stored outputs at/above `height` and lowers `scan_height`, so the scanner
re-derives them. A rescan to a height `>=` the current scan height is rejected as
a no-op.

---

## Database schema

See `migrations/0001_init.sql`. It runs **unchanged on both PostgreSQL and
SQLite** (verified in a test). **Never** stores a private/spend key.

```
accounts(rewind_hash PK, start_height, scan_height, created_at)
outputs("commit" PK, rewind_hash → accounts, value, height, mmr_index,
        is_coinbase, lock_height, key_id, n_child, spent, spent_height)
        INDEX(rewind_hash, spent), INDEX("commit")
chain_cursor(id=1 PK, height, block_hash)   -- reorg-safe resume
```

* `outputs.key_id` / `n_child` are recovered during rewind — the direct-spend
  enabler.
* `outputs.spent` / `spent_height` are set when a stored output's commitment
  appears as a block input.
* `chain_cursor` is the single-row global resume point; on a reorg the scanner
  rolls back rows above the fork and reseeks from it.

---

## How a wallet backend proxies to it

Identical wiring to a Monero LWS client — forward the view credential, serve the
stored results:

```
wallet ──JWT──▶ wallet backend ──private──▶ grin-lws ──▶ grin node
  /wallet/grin/scan             /get_unspent_outs         (Foreign API v2)
```

1. Add a grin-lws client config (`lws_url`, optional `admin_url` + `admin_key`)
   alongside your existing Monero-LWS config.
2. Add an HTTP client mirroring your Monero-LWS client (shared `reqwest::Client`,
   timeouts, size-capped hostile-response reads, a `Secret`-wrapped admin key).
3. Behind a feature flag, have your grin scan endpoint forward the `rewind_hash`
   to grin-lws `/get_unspent_outs` (returning outputs **with paths**) instead of
   driving grin-wallet `scan_rewind_hash` on-demand.
4. Register at balance-fetch time by forwarding `rewind_hash` to grin-lws
   `/register` (idempotent), exactly as you await LWS registration for Monero.

**Non-custodial guarantee preserved end to end:** the backend forwards only the
`rewind_hash` and stores nothing; grin-lws holds the `rewind_hash` for scanning
just as monero-lws holds the view key. No spend key ever leaves the wallet.

Once grin-lws is wired in, the wallet client can **drop** any client-side
output-identify helper — `/get_unspent_outs` returns paths directly.

---

## Build order

Ship in stages; the deployed on-demand `/wallet/grin/scan` keeps serving balance
throughout, so nothing here blocks the client refactor.

1. **Scaffold.** ✅ Routes, DTOs, config, DB layer, migrations, health/readiness.
2. **Rangeproof rewind (the core milestone).** ✅ `grin::rewind_output`: compute
   the rewind nonce from `rewind_hash` + commitment, rewind the Bulletproof, and
   parse the proof message to recover `(value, key_id, n_child)` — VIEW-ONLY, the
   server-side analog of `grin_recover_output`. Cross-validated against grin's
   reference `ProofBuilder`; adversarial money-safety review passed. (v3 outputs
   only — legacy needs the master secret, same as grin-wallet's view-only scan.)
3. **Grin node reads.** ✅ `get_unspent_outputs` (PMMR-index, with proofs),
   `get_outputs` (spend-by-absence), `get_pmmr_indices`, `get_header`, `get_tip`,
   `push_transaction` — all against the Foreign API v2, defensively parsed.
4. **Scanner.** ✅ The `tick` loop: page the UTXO set from the cursor, rewind per
   caught-up account, `insert_output` on matches, spend-reconcile by absence,
   advance the cursor + account scan heights, coinbase-maturity/lock read path.
5. **Reorg + backfill.** ✅ Detect via header-hash mismatch at the checkpoint,
   bounded `rollback_to`, reseek; backfill a new account from its birthday index.
6. **Live-node smoke test.** ⏳ Point at a real grin node with a funded wallet and
   confirm balance/outputs match grin-wallet's `scan_rewind_hash`.
7. **Backend proxy.** ⏳ Land a `GrinLwsClient` in a wallet backend behind a
   feature flag; parity-check against the on-demand path; retire the embedded
   grin-wallet coupling.

---

## Running

```sh
# 1. Provision a DB and run the migration.
#    Postgres:  createdb grinlws && psql grinlws < migrations/0001_init.sql
#    SQLite:    sqlite3 grin-lws.db < migrations/0001_init.sql
# 2. Configure.
cp .env.example .env    # edit DATABASE_URL + GRIN_NODE_URL
# 3. Run.
cargo run               # binds GRINLWS_BIND (default 127.0.0.1:3480)
curl localhost:3480/health
```

`/height`, `/submit_raw_tx`, and `/ready` require a reachable grin node (they
return `502` otherwise). `/register`, `/get_balance`, and `/get_unspent_outs`
read from the DB store, which the background scanner fills once it has a grin
node to page the output set from. `/health` always works.

## Deployment

Run as a private service alongside a grin node and its database. Bind to
loopback / a private network; expose it only to the proxying backend. Add a
readiness probe on `/ready`. Everything is env-configured — no code changes to
deploy.

---

## License & provenance

MIT — see [`LICENSE`](LICENSE).

grin-lws was extracted from the [Smirk](https://smirk.cash) wallet's backend and
generalized into a standalone service that any wallet backend can run.
