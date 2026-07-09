//! Background chain scanner — the heart of a real light-wallet-server, and the
//! bulk of the remaining work.
//!
//! It is a long-lived task that follows the grin chain tip and, for every new
//! block, does two things per registered account:
//!   1. CREATE: rewind each block output's rangeproof against the account's
//!      `rewind_hash`. On a match, persist the output WITH its recovered
//!      derivation path (so the client can spend directly — no client-side
//!      identify search).
//!   2. SPEND: for each input commitment in the block, if it matches a stored
//!      output, mark that output spent at this height.
//! It maintains a reorg-safe [`chain_cursor`](crate::db): on a reorg (a block
//! whose `prev_hash` does not match our cursor), roll back rows above the fork
//! and resume from the fork point.
//!
//! STATUS: SCAFFOLD. The loop structure, cursor handling, and DB seams are real;
//! the per-output rewind ([`crate::grin::rewind_output`]) and block parsing
//! ([`crate::grin::GrinNode::get_block`]) are stubbed. Until this lands, the
//! deployed on-demand `/wallet/grin/scan` proxy serves balance for grin's small
//! UTXO set and the client uses a client-side identify helper for spend paths.

use std::time::Duration;

use sqlx::AnyPool;

use crate::config::Config;
use crate::grin::GrinNode;

/// Spawn the background scanner. Returns immediately; the task runs until the
/// process exits.
pub fn spawn(pool: AnyPool, node: GrinNode, cfg: Config) {
    tokio::spawn(async move {
        let poll = Duration::from_secs(cfg.scan_poll_secs.max(1));
        tracing::info!(
            poll_secs = cfg.scan_poll_secs,
            batch = cfg.scan_batch_blocks,
            "grin-lws scanner started (SCAFFOLD — block parse + rewind stubbed)"
        );
        loop {
            if let Err(e) = tick(&pool, &node, &cfg).await {
                tracing::warn!(error = %e, "scanner tick failed; will retry");
            }
            tokio::time::sleep(poll).await;
        }
    });
}

/// One scan pass: catch up from the cursor toward the tip, a batch at a time.
async fn tick(pool: &AnyPool, node: &GrinNode, cfg: &Config) -> crate::error::Result<()> {
    let tip = node.get_tip_height().await?;
    let cursor = crate::db::get_cursor(pool).await?;
    let from = cursor.as_ref().map(|c| c.height + 1).unwrap_or(0);
    if from > tip {
        return Ok(()); // caught up
    }
    let to = (from + cfg.scan_batch_blocks).min(tip);
    tracing::debug!(from, to, tip, "scanner batch");

    for height in from..=to {
        // REAL IMPL (deferred):
        //   let block = node.get_block(height).await?;
        //   reorg check: if block.prev_hash != cursor.block_hash { rollback + reseek }
        //   let accounts = db::list_account_hashes(pool).await?;
        //   for out in &block.outputs {
        //       for rh in &accounts {
        //           if let Some(r) = grin::rewind_output(rh, out) {
        //               db::insert_output(pool, &to_stored(out, &r), rh).await?;
        //           }
        //       }
        //   }
        //   for commit in &block.input_commits { db::mark_spent(pool, commit, height).await?; }
        //   db::set_cursor(pool, height, &block.hash).await?;
        let _ = height;
        return Ok(()); // scaffold: stop after entering the loop; no chain reads yet.
    }
    Ok(())
}
