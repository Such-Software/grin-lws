//! Persistence layer: the per-account output store, accounts table, and the
//! reorg-safe chain cursor.
//!
//! Uses `sqlx::Any` so a single build serves both Postgres (scale) and SQLite
//! (single operator) — the driver is chosen at runtime from `DATABASE_URL`.
//!
//! NOTE (dialect): the SQL below uses Postgres `$N` placeholders. sqlx's `Any`
//! backend forwards placeholders verbatim, so a SQLite deployment should run the
//! SQLite-flavored migration (see `migrations/`) and, if it hits placeholder
//! incompatibility, swap `$N` for `?`. This scaffold keeps one dialect for
//! readability; pick your target DB before landing the scanner.
//!
//! SECURITY: only view-only data is ever stored. No spend key, no private key.

// Several write helpers (insert_output, mark_spent, set_cursor, rollback_to) are
// exercised only by the scanner, which is a deferred milestone in this scaffold.
// They are real and tested-shaped; allow them to be unused until the scanner
// loop is wired up.
#![allow(dead_code)]

use sqlx::any::AnyPoolOptions;
use sqlx::{AnyPool, Row};

use crate::error::{Error, Result};

/// A stored output as served to the client. Includes the recovered derivation
/// path (`key_id` / `n_child`) so the client can spend directly — the whole
/// point of a real LWS over an on-demand scan.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StoredOutput {
    pub commit: String,
    pub value: u64,
    pub height: u64,
    pub mmr_index: u64,
    pub is_coinbase: bool,
    pub lock_height: u64,
    pub key_id: Option<String>,
    pub n_child: Option<u32>,
}

/// The scanner's resume point.
#[derive(Debug, Clone)]
pub struct ChainCursor {
    pub height: u64,
    pub block_hash: String,
}

/// Connect (lazily) to the database. Does not require a live DB at construction;
/// the first query establishes the connection. Run `migrations/` before serving.
pub async fn connect(database_url: &str) -> Result<AnyPool> {
    // Registers the compiled-in drivers (postgres / sqlite) for the `Any` pool.
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(8)
        .connect_lazy(database_url)
        .map_err(Error::Db)?;
    Ok(pool)
}

// ── accounts ──────────────────────────────────────────────────────────────────

/// Register an account (idempotent). A new account starts scanning at
/// `start_height`; an existing one is left untouched (its scan progress and view
/// key are preserved). Returns `true` if a new row was inserted.
pub async fn register_account(pool: &AnyPool, rewind_hash: &str, start_height: i64) -> Result<bool> {
    let res = sqlx::query(
        "INSERT INTO accounts (rewind_hash, start_height, scan_height) \
         VALUES ($1, $2, $2) ON CONFLICT (rewind_hash) DO NOTHING",
    )
    .bind(rewind_hash)
    .bind(start_height)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// All registered accounts' rewind_hashes (the scanner iterates these per block).
pub async fn list_account_hashes(pool: &AnyPool) -> Result<Vec<String>> {
    let rows = sqlx::query("SELECT rewind_hash FROM accounts")
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(|r| r.get::<String, _>("rewind_hash")).collect())
}

/// Lower an account's scan progress to `height` for a backwards rescan (also
/// clears its stored outputs at/above `height` so they are re-derived). SAFETY:
/// backwards-only, mirroring the monero-lws rescan invariant.
pub async fn rescan_account(pool: &AnyPool, rewind_hash: &str, height: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM outputs WHERE rewind_hash = $1 AND height >= $2")
        .bind(rewind_hash)
        .bind(height)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE accounts SET scan_height = $2 \
         WHERE rewind_hash = $1 AND scan_height > $2",
    )
    .bind(rewind_hash)
    .bind(height)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

// ── outputs ────────────────────────────────────────────────────────────────────

/// Persist a recognized output (scanner match). Idempotent on the commitment.
#[allow(clippy::too_many_arguments)]
pub async fn insert_output(pool: &AnyPool, out: &StoredOutput, rewind_hash: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO outputs \
         (\"commit\", rewind_hash, value, height, mmr_index, is_coinbase, lock_height, key_id, n_child) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (\"commit\") DO NOTHING",
    )
    .bind(&out.commit)
    .bind(rewind_hash)
    .bind(out.value as i64)
    .bind(out.height as i64)
    .bind(out.mmr_index as i64)
    .bind(out.is_coinbase)
    .bind(out.lock_height as i64)
    .bind(&out.key_id)
    .bind(out.n_child.map(|n| n as i32))
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark an output spent when its commitment appears as a tx input on-chain.
pub async fn mark_spent(pool: &AnyPool, commit: &str, spent_height: i64) -> Result<()> {
    sqlx::query("UPDATE outputs SET spent = true, spent_height = $2 WHERE \"commit\" = $1")
        .bind(commit)
        .bind(spent_height)
        .execute(pool)
        .await?;
    Ok(())
}

/// Unspent outputs for an account — the spendable set, WITH derivation paths.
pub async fn unspent_outputs(pool: &AnyPool, rewind_hash: &str) -> Result<Vec<StoredOutput>> {
    let rows = sqlx::query(
        "SELECT \"commit\", value, height, mmr_index, is_coinbase, lock_height, key_id, n_child \
         FROM outputs WHERE rewind_hash = $1 AND spent = false ORDER BY height ASC",
    )
    .bind(rewind_hash)
    .fetch_all(pool)
    .await?;

    Ok(rows.iter().map(row_to_output).collect())
}

/// (total unspent, count) for an account — the balance read.
pub async fn account_totals(pool: &AnyPool, rewind_hash: &str) -> Result<(u64, u64)> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(value),0) AS total, COUNT(*) AS n \
         FROM outputs WHERE rewind_hash = $1 AND spent = false",
    )
    .bind(rewind_hash)
    .fetch_one(pool)
    .await?;
    let total: i64 = row.try_get("total").unwrap_or(0);
    let n: i64 = row.try_get("n").unwrap_or(0);
    Ok((total.max(0) as u64, n.max(0) as u64))
}

/// The height at which the scanner last processed this account.
pub async fn account_scan_height(pool: &AnyPool, rewind_hash: &str) -> Result<Option<u64>> {
    let row = sqlx::query("SELECT scan_height FROM accounts WHERE rewind_hash = $1")
        .bind(rewind_hash)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get::<i64, _>("scan_height").max(0) as u64))
}

fn row_to_output(r: &sqlx::any::AnyRow) -> StoredOutput {
    StoredOutput {
        commit: r.get::<String, _>("commit"),
        value: r.get::<i64, _>("value").max(0) as u64,
        height: r.get::<i64, _>("height").max(0) as u64,
        mmr_index: r.get::<i64, _>("mmr_index").max(0) as u64,
        is_coinbase: r.get::<bool, _>("is_coinbase"),
        lock_height: r.get::<i64, _>("lock_height").max(0) as u64,
        key_id: r.try_get::<Option<String>, _>("key_id").ok().flatten(),
        n_child: r
            .try_get::<Option<i32>, _>("n_child")
            .ok()
            .flatten()
            .map(|n| n.max(0) as u32),
    }
}

// ── chain cursor (reorg-safe resume) ───────────────────────────────────────────

/// Read the scanner's global cursor (last block processed across all accounts).
pub async fn get_cursor(pool: &AnyPool) -> Result<Option<ChainCursor>> {
    let row = sqlx::query("SELECT height, block_hash FROM chain_cursor WHERE id = 1")
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| ChainCursor {
        height: r.get::<i64, _>("height").max(0) as u64,
        block_hash: r.get::<String, _>("block_hash"),
    }))
}

/// Advance (or initialize) the scanner cursor.
pub async fn set_cursor(pool: &AnyPool, height: i64, block_hash: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_cursor (id, height, block_hash) VALUES (1, $1, $2) \
         ON CONFLICT (id) DO UPDATE SET height = $1, block_hash = $2",
    )
    .bind(height)
    .bind(block_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Roll back all stored state above `fork_height` after a detected reorg.
pub async fn rollback_to(pool: &AnyPool, fork_height: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    // Outputs created above the fork never happened on the new chain.
    sqlx::query("DELETE FROM outputs WHERE height > $1")
        .bind(fork_height)
        .execute(&mut *tx)
        .await?;
    // Spends recorded above the fork are undone (the input may be unspent again).
    sqlx::query("UPDATE outputs SET spent = false, spent_height = NULL WHERE spent_height > $1")
        .bind(fork_height)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
