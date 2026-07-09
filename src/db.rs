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

/// The scanner's resume point: how far through the output PMMR it has scanned,
/// plus the chain tip it last saw (for reorg detection).
#[derive(Debug, Clone)]
pub struct ChainCursor {
    /// Highest output PMMR index already processed (the forward-scan resume point).
    pub output_mmr_index: u64,
    /// Chain tip height at the last tick (reorg checkpoint).
    pub tip_height: u64,
    /// Chain tip block hash at the last tick (reorg detection probe).
    pub tip_hash: String,
}

/// A registered account, for the scanner's per-account loop.
#[derive(Debug, Clone)]
pub struct AccountRow {
    pub rewind_hash: String,
    pub start_height: u64,
    pub scan_height: u64,
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

/// All registered accounts' rewind_hashes (the admin `/list_accounts` view).
pub async fn list_account_hashes(pool: &AnyPool) -> Result<Vec<String>> {
    let rows = sqlx::query("SELECT rewind_hash FROM accounts")
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(|r| r.get::<String, _>("rewind_hash")).collect())
}

/// All registered accounts with their scan progress (the scanner's per-account
/// loop iterates these).
pub async fn list_accounts(pool: &AnyPool) -> Result<Vec<AccountRow>> {
    let rows = sqlx::query("SELECT rewind_hash, start_height, scan_height FROM accounts")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|r| AccountRow {
            rewind_hash: r.get::<String, _>("rewind_hash"),
            start_height: r.get::<i64, _>("start_height").max(0) as u64,
            scan_height: r.get::<i64, _>("scan_height").max(0) as u64,
        })
        .collect())
}

/// Advance the scan height of already-caught-up accounts (scan_height at/above
/// the previous tip) to the new tip after a forward scan pass.
pub async fn advance_caught_up_scan_heights(
    pool: &AnyPool,
    to_height: i64,
    from_height: i64,
) -> Result<()> {
    sqlx::query("UPDATE accounts SET scan_height = $1 WHERE scan_height >= $2")
        .bind(to_height)
        .bind(from_height)
        .execute(pool)
        .await?;
    Ok(())
}

/// Set one account's scan height (used after its backfill completes).
pub async fn set_account_scan_height(pool: &AnyPool, rewind_hash: &str, height: i64) -> Result<()> {
    sqlx::query("UPDATE accounts SET scan_height = $1 WHERE rewind_hash = $2")
        .bind(height)
        .bind(rewind_hash)
        .execute(pool)
        .await?;
    Ok(())
}

/// Clamp any account whose scan height sits above `fork_height` back down to it
/// (a reorg rolled the chain back below their recorded progress).
pub async fn clamp_account_scan_heights(pool: &AnyPool, fork_height: i64) -> Result<()> {
    sqlx::query("UPDATE accounts SET scan_height = $1 WHERE scan_height > $1")
        .bind(fork_height)
        .execute(pool)
        .await?;
    Ok(())
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

/// Mark an output spent (its commitment left the node's unspent set).
pub async fn mark_spent(pool: &AnyPool, commit: &str, spent_height: i64) -> Result<()> {
    sqlx::query("UPDATE outputs SET spent = true, spent_height = $2 WHERE \"commit\" = $1")
        .bind(commit)
        .bind(spent_height)
        .execute(pool)
        .await?;
    Ok(())
}

/// Un-mark a spend (a reorg brought a previously-spent output back into the
/// unspent set). Self-heals an over-eager spend reconcile.
pub async fn mark_unspent(pool: &AnyPool, commit: &str) -> Result<()> {
    sqlx::query("UPDATE outputs SET spent = false, spent_height = NULL WHERE \"commit\" = $1")
        .bind(commit)
        .execute(pool)
        .await?;
    Ok(())
}

/// Every currently-unspent commitment across all accounts — the set the scanner
/// re-checks against the node to detect spends by absence.
pub async fn all_unspent_commits(pool: &AnyPool) -> Result<Vec<String>> {
    let rows = sqlx::query("SELECT \"commit\" FROM outputs WHERE spent = false")
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(|r| r.get::<String, _>("commit")).collect())
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

/// (unlocked total, count) for an account: unspent AND spendable at `tip_height`
/// (`lock_height <= tip`). Immature coinbase / time-locked outputs are excluded,
/// so this never reports coins a spend would reject. Mirrors monero-lws's
/// unlocked-vs-total split.
pub async fn account_unlocked_totals(
    pool: &AnyPool,
    rewind_hash: &str,
    tip_height: i64,
) -> Result<(u64, u64)> {
    let row = sqlx::query(
        "SELECT COALESCE(SUM(value),0) AS total, COUNT(*) AS n \
         FROM outputs WHERE rewind_hash = $1 AND spent = false AND lock_height <= $2",
    )
    .bind(rewind_hash)
    .bind(tip_height)
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

/// Read the scanner's global cursor (output-PMMR position + last-seen tip).
pub async fn get_cursor(pool: &AnyPool) -> Result<Option<ChainCursor>> {
    let row = sqlx::query("SELECT output_mmr_index, height, block_hash FROM chain_cursor WHERE id = 1")
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| ChainCursor {
        output_mmr_index: r.get::<i64, _>("output_mmr_index").max(0) as u64,
        tip_height: r.get::<i64, _>("height").max(0) as u64,
        tip_hash: r.get::<String, _>("block_hash"),
    }))
}

/// Advance (or initialize) the scanner cursor: the highest output PMMR index
/// processed, plus the tip height + hash at that point (`height` / `block_hash`
/// columns now hold the tip checkpoint, not a per-block position).
pub async fn set_cursor(
    pool: &AnyPool,
    output_mmr_index: i64,
    tip_height: i64,
    tip_hash: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_cursor (id, output_mmr_index, height, block_hash) VALUES (1, $1, $2, $3) \
         ON CONFLICT (id) DO UPDATE SET output_mmr_index = $1, height = $2, block_hash = $3",
    )
    .bind(output_mmr_index)
    .bind(tip_height)
    .bind(tip_hash)
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
