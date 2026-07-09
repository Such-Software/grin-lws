-- grin-lws initial schema.
--
-- This service is a REAL light-wallet-server: it holds a per-account output
-- store keyed by each account's `rewind_hash` (a view credential) and a
-- reorg-safe chain cursor. It NEVER stores a private/spend key — `rewind_hash`
-- is view-only (it can recognize a wallet's outputs and read amounts, but
-- cannot spend).
--
-- Dialect note: written for PostgreSQL. For a single-operator SQLite build,
-- replace BIGINT -> INTEGER, BOOLEAN -> INTEGER (0/1), TIMESTAMPTZ ->
-- TEXT/INTEGER, and drop `DEFAULT now()`. `commit` is a reserved word in SQL,
-- hence it is always quoted as "commit".

-- An account = one registered view credential.
CREATE TABLE IF NOT EXISTS accounts (
    -- The wallet's rewind_hash (64 hex). View-only; safe to persist.
    rewind_hash  TEXT        PRIMARY KEY,
    -- The height the scanner should (re)start from when this account was added
    -- (a wallet birthday). Never advances backwards on its own.
    start_height BIGINT      NOT NULL,
    -- How far the scanner has processed this account. Advances toward the tip;
    -- lowered only by an explicit backwards rescan.
    scan_height  BIGINT      NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outputs the scanner recognized for a registered account (rewound rangeproof
-- matched). Crucially stores the recovered derivation path (`key_id` /
-- `n_child`) so a client can spend WITHOUT a client-side identify search.
CREATE TABLE IF NOT EXISTS outputs (
    "commit"     TEXT        PRIMARY KEY,
    rewind_hash  TEXT        NOT NULL REFERENCES accounts(rewind_hash) ON DELETE CASCADE,
    value        BIGINT      NOT NULL,
    height       BIGINT      NOT NULL,
    mmr_index    BIGINT      NOT NULL,
    is_coinbase  BOOLEAN     NOT NULL DEFAULT false,
    lock_height  BIGINT      NOT NULL DEFAULT 0,
    -- Recovered from the proof message during rewind (grin_recover_output
    -- equivalent, server-side). Enables direct spend by the client.
    key_id       TEXT,
    n_child      INTEGER,
    -- Spend tracking: set when this output's commitment appears as a tx input.
    spent        BOOLEAN     NOT NULL DEFAULT false,
    spent_height BIGINT
);

-- Balance / unspent-set queries filter by (account, spent).
CREATE INDEX IF NOT EXISTS idx_outputs_account_spent ON outputs (rewind_hash, spent);
-- Spend detection matches an on-chain input commitment against stored outputs.
CREATE INDEX IF NOT EXISTS idx_outputs_commit ON outputs ("commit");

-- Single-row scanner cursor. Grin is scanned by walking the output PMMR in
-- insertion-index order, so the resume point is an output index, not a block
-- position. `height`/`block_hash` hold the chain tip last seen (the reorg-
-- detection checkpoint). On a reorg the scanner rolls back rows above the fork
-- and reseeks from there.
CREATE TABLE IF NOT EXISTS chain_cursor (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    -- Highest output PMMR index already scanned (forward-scan resume point).
    output_mmr_index BIGINT  NOT NULL DEFAULT 0,
    -- Chain tip height + block hash at the last tick (reorg checkpoint).
    height           BIGINT  NOT NULL,
    block_hash       TEXT    NOT NULL
);
