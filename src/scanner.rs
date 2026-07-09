//! Background chain scanner — the heart of a real light-wallet-server.
//!
//! Grin's chain state is the set of unspent outputs in a Merkle Mountain Range.
//! Scanning = walking that output set in PMMR insertion-index order, rewinding
//! each output against every registered `rewind_hash` (view credential) and
//! storing the matches WITH their recovered derivation path. It is incremental:
//! a global cursor records the highest output index already processed, so each
//! tick only rewinds outputs added since last time.
//!
//! Each `tick()` runs, against the shared cursor:
//!   0. REORG check — if the chain's block at our checkpoint height no longer
//!      matches the hash we recorded, roll back and reseek.
//!   1. DISCOVER — page new outputs (index > cursor) via `get_unspent_outputs`
//!      (with proofs), rewind against caught-up accounts, insert matches.
//!   2. SPEND reconcile — a stored output whose commit is no longer in the
//!      node's unspent set has been spent; mark it. (Grin has no per-block input
//!      list to scan; spends are detected by absence.)
//!   3. ADVANCE the cursor + caught-up accounts.
//!   4. BACKFILL one pending new account (birthday → cursor) against its hash.
//!
//! Idempotent throughout (`insert_output`/`mark_spent` use ON CONFLICT), so a
//! failed tick simply retries: the cursor is only advanced after discovery AND
//! spend-reconcile both succeed, so balances never advance past unreconciled
//! state.

use std::time::Duration;

use sqlx::AnyPool;

use crate::config::Config;
use crate::db::{self, StoredOutput};
use crate::grin::GrinNode;

/// Grin coinbase maturity: a coinbase output is unspendable until this many
/// blocks after its creation height (consensus `DAY_HEIGHT` = 24×60).
const COINBASE_MATURITY: u64 = 1440;
/// Max outputs per `get_unspent_outputs` page (node hard-caps at 10_000).
const PAGE_MAX: u64 = 1000;
/// How far back a reorg is auto-repaired. Grin's practical reorgs are a few
/// blocks; deeper than this is not auto-healed (documented limitation).
const REORG_DEPTH: u64 = 100;

/// Spawn the background scanner. Returns immediately; the task runs until the
/// process exits.
pub fn spawn(pool: AnyPool, node: GrinNode, cfg: Config) {
    tokio::spawn(async move {
        let poll = Duration::from_secs(cfg.scan_poll_secs.max(1));
        tracing::info!(poll_secs = cfg.scan_poll_secs, "grin-lws scanner started");
        loop {
            if let Err(e) = tick(&pool, &node).await {
                tracing::warn!(error = %e, "scanner tick failed; will retry");
            }
            tokio::time::sleep(poll).await;
        }
    });
}

/// One scan pass. See the module docs for the phase breakdown.
async fn tick(pool: &AnyPool, node: &GrinNode) -> crate::error::Result<()> {
    let (tip_height, tip_hash) = node.get_tip().await?;
    let cursor = db::get_cursor(pool).await?;

    // ── Phase 0: reorg check ────────────────────────────────────────────────
    if let Some(c) = &cursor {
        if c.tip_height > 0 {
            // Chain shorter than our checkpoint ⇒ reorg (header probe would just
            // 404). Otherwise, a changed block hash at the checkpoint height ⇒
            // reorg. A transient node error propagates and aborts the tick.
            if tip_height < c.tip_height {
                return handle_reorg(pool, node, tip_height).await;
            }
            let canon = node.header_hash_at(c.tip_height).await?;
            if canon != c.tip_hash {
                return handle_reorg(pool, node, tip_height).await;
            }
        }
    }

    let prev_tip = cursor.as_ref().map(|c| c.tip_height).unwrap_or(0);
    let from_index = cursor.as_ref().map(|c| c.output_mmr_index + 1).unwrap_or(1);

    // ── Phase 1: discover new outputs for caught-up accounts ────────────────
    let accounts = db::list_accounts(pool).await?;
    let caught_up: Vec<String> = accounts
        .iter()
        .filter(|a| a.scan_height >= prev_tip)
        .map(|a| a.rewind_hash.clone())
        .collect();

    let highest = forward_scan(pool, node, from_index, &caught_up).await?;
    let new_mmr_index = highest.max(cursor.as_ref().map(|c| c.output_mmr_index).unwrap_or(0));

    // ── Phase 2: spend reconcile (by absence from the unspent set) ──────────
    reconcile_spends(pool, node, tip_height).await?;

    // ── Phase 3: advance the cursor + caught-up accounts ────────────────────
    // (Only after discovery AND reconcile succeeded — Risk R1.)
    db::set_cursor(pool, new_mmr_index as i64, tip_height as i64, &tip_hash).await?;
    db::advance_caught_up_scan_heights(pool, tip_height as i64, prev_tip as i64).await?;

    // ── Phase 4: backfill one pending new account (birthday → cursor) ───────
    if let Some(acct) = accounts.iter().find(|a| a.scan_height < prev_tip) {
        backfill_account(pool, node, acct, new_mmr_index, tip_height).await?;
    }

    Ok(())
}

/// Page the output set from `from_index` to the current tip, rewinding each
/// output against every `rewind_hash` in `accounts` and inserting matches.
/// Returns the highest PMMR index reached. Rewinds nothing if `accounts` empty
/// but still walks to learn `highest_index` so the cursor can advance.
async fn forward_scan(
    pool: &AnyPool,
    node: &GrinNode,
    from_index: u64,
    accounts: &[String],
) -> crate::error::Result<u64> {
    let mut start = from_index;
    let mut highest = from_index.saturating_sub(1);
    loop {
        let page = node.get_unspent_outputs(start, None, PAGE_MAX).await?;
        highest = highest.max(page.highest_index);
        for out in &page.outputs {
            store_first_match(pool, out, accounts).await?;
        }
        // Terminate when the page reached the tip, or made no forward progress
        // (defensive against a node that doesn't advance `last_retrieved_index`).
        if page.last_retrieved_index >= page.highest_index || page.last_retrieved_index < start {
            break;
        }
        start = page.last_retrieved_index + 1;
    }
    Ok(highest)
}

/// Rewind `out` against each account; on the first match, store it (an output
/// belongs to at most one wallet) and stop.
async fn store_first_match(
    pool: &AnyPool,
    out: &crate::grin::ChainOutput,
    accounts: &[String],
) -> crate::error::Result<()> {
    for rh in accounts {
        if let Some(r) = crate::grin::rewind_output(rh, out) {
            let lock_height = if out.is_coinbase {
                out.height.saturating_add(COINBASE_MATURITY)
            } else {
                out.height
            };
            db::insert_output(
                pool,
                &StoredOutput {
                    commit: out.commit.clone(),
                    value: r.value,
                    height: out.height,
                    mmr_index: out.mmr_index,
                    is_coinbase: out.is_coinbase,
                    lock_height,
                    key_id: Some(r.key_id),
                    n_child: Some(r.n_child),
                },
                rh,
            )
            .await?;
            break;
        }
    }
    Ok(())
}

/// Mark any stored-unspent output that the node no longer lists as spent.
async fn reconcile_spends(
    pool: &AnyPool,
    node: &GrinNode,
    tip_height: u64,
) -> crate::error::Result<()> {
    let stored = db::all_unspent_commits(pool).await?;
    if stored.is_empty() {
        return Ok(());
    }
    let present = node.outputs_present(&stored).await?;
    for commit in &stored {
        if !present.contains(commit) {
            db::mark_spent(pool, commit, tip_height as i64).await?;
        }
    }
    Ok(())
}

/// Bring a newly-registered account up to date: scan the live output set from
/// its birthday index up to the current cursor against ONLY its hash (its
/// pre-existing outputs sit below the shared cursor), then mark it caught up so
/// it joins the forward scan next tick.
async fn backfill_account(
    pool: &AnyPool,
    node: &GrinNode,
    acct: &db::AccountRow,
    up_to_index: u64,
    tip_height: u64,
) -> crate::error::Result<()> {
    let start = node.start_index_for_height(acct.start_height).await?;
    let only = [acct.rewind_hash.clone()];
    let mut idx = start;
    loop {
        let page = node.get_unspent_outputs(idx, Some(up_to_index), PAGE_MAX).await?;
        for out in &page.outputs {
            store_first_match(pool, out, &only).await?;
        }
        let reached = page.last_retrieved_index.min(page.highest_index);
        if reached >= up_to_index || page.last_retrieved_index < idx {
            break;
        }
        idx = page.last_retrieved_index + 1;
    }
    db::set_account_scan_height(pool, &acct.rewind_hash, tip_height as i64).await?;
    tracing::info!(height = tip_height, "grin-lws backfilled a new account");
    Ok(())
}

/// A reorg was detected: roll back everything above a bounded fork point and
/// reseek from there. Idempotent re-discovery + spend-reconcile on subsequent
/// ticks re-derive the canonical state. Reorgs deeper than `REORG_DEPTH` are not
/// auto-repaired (the read-path maturity gate still protects spendability).
async fn handle_reorg(
    pool: &AnyPool,
    node: &GrinNode,
    tip_height: u64,
) -> crate::error::Result<()> {
    let fork = tip_height.saturating_sub(REORG_DEPTH);
    tracing::warn!(fork, tip = tip_height, "grin-lws reorg detected; rolling back");
    db::rollback_to(pool, fork as i64).await?;
    db::clamp_account_scan_heights(pool, fork as i64).await?;
    let fork_index = node.start_index_for_height(fork).await?;
    let fork_hash = node.header_hash_at(fork).await?;
    // Cursor sits just below the fork's first index so Phase 1 re-pages from it.
    db::set_cursor(pool, fork_index.saturating_sub(1) as i64, fork as i64, &fork_hash).await?;
    Ok(())
}
