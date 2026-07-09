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

    tracing::debug!(
        accounts = accounts.len(),
        caught_up = caught_up.len(),
        from_index,
        prev_tip,
        "tick: scanning"
    );
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
    let (mut scanned, mut matched) = (0u64, 0u64);
    loop {
        let page = node.get_unspent_outputs(start, None, PAGE_MAX).await?;
        highest = highest.max(page.highest_index);
        for out in &page.outputs {
            scanned += 1;
            if store_first_match(pool, out, accounts).await? {
                matched += 1;
            }
        }
        // Terminate when the page reached the tip, or made no forward progress
        // (defensive against a node that doesn't advance `last_retrieved_index`).
        if page.last_retrieved_index >= page.highest_index || page.last_retrieved_index < start {
            break;
        }
        start = page.last_retrieved_index + 1;
    }
    tracing::debug!(from_index, highest, scanned, matched, "forward_scan done");
    Ok(highest)
}

/// Rewind `out` against each account; on the first match, store it (an output
/// belongs to at most one wallet) and stop.
async fn store_first_match(
    pool: &AnyPool,
    out: &crate::grin::ChainOutput,
    accounts: &[String],
) -> crate::error::Result<bool> {
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
            return Ok(true);
        }
    }
    Ok(false)
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
    tracing::info!(birthday = acct.start_height, start, up_to_index, "backfill start");
    let only = [acct.rewind_hash.clone()];
    let mut idx = start;
    let mut scanned = 0u64;
    loop {
        let page = node.get_unspent_outputs(idx, Some(up_to_index), PAGE_MAX).await?;
        for out in &page.outputs {
            scanned += 1;
            store_first_match(pool, out, &only).await?;
        }
        let reached = page.last_retrieved_index.min(page.highest_index);
        if reached >= up_to_index || page.last_retrieved_index < idx {
            break;
        }
        idx = page.last_retrieved_index + 1;
    }
    db::set_account_scan_height(pool, &acct.rewind_hash, tip_height as i64).await?;
    tracing::info!(height = tip_height, scanned, "grin-lws backfilled a new account");
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

#[cfg(test)]
mod integration {
    //! End-to-end: drive the real `tick()` against a MOCK grin node serving the
    //! golden-vector output, over a real SQLite store. Proves the whole pipeline
    //! (node listing → view-only rewind → store → balance → spend reconcile).
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use tokio::net::TcpListener;

    use super::*;
    use crate::config::Config;
    use crate::secret::Secret;

    // Golden depth-3 (Grim) output — its rewind_hash/commit/proof are the ones
    // grin's own ProofBuilder produced (see src/rewind.rs golden vectors).
    const RH: &str = "c2f675f2b05b5bed7d7f01480c6f2aaf368fe4da5e169c3b6d18bb354ba784a2";
    const COMMIT: &str = "099c6701a2c0cf2c02ada5a051a87e554524bcfa7d77339caf35b607fc1a1b2226";
    const PROOF: &str = "69d7d34b41a9cff366101cd1473fd28dde406de95fa29d403a30684f93a6c857409eb08742b6fd8704c73ea65e982f3f332dbb6f2c61458b6acb901b8d98be9b01cf44cbcf9a8e087638ceabd7666c2bc9728d46ce21a5560da9710e69d4ae6099ad49666851f4181b2fea9f10ff400a5fc789ef132b96668bedcc8697349798747fc68dcb32c081f39810d7d05846f955cf398abf358ec4c013c6cf35e7abb4552a1bf8a2eaff0a2c4bd5a3f232fd323f1cd59588623dfdbbd3276e1e4dbc5c00038cb18ba9ee83d70a803bcbb5f32972153454a093bbee48b14c2653ed34c5bc94b0220d10f3b97f4914d62c0288e9146ad34527634736ff28db7dc6171cee1bed2bbe3c85788417778556ceb259fe8fd7571ff884b6dacc375d56d7a528cc037993c0402c6f7b2ca13940501e798b6476ae24c4b65cca0805e98de96819b246997de4f3680bf81b3c7dbf4c456c29677e3a3be281993e06b8c8aae14559672f5103b80f5aaba187ea0c74903af9896433aec723063b695efd2254b831143c7335058496aa4a8d8a90c0bfb4260da8790f32073f4f809a59f93ef33c0379d20a48212c1ac36181eeef59ababfb1b6f9a0ce4c9baaa83299c21ccad09a0493ad79f35083b00a149d72745feb8e6776fb16f06128a37f1ffd406476deeb5e7f35208b3cc7544197be7def4509fdcbd92d8bd72e3fd0437b3f249c247981ba70bc2ebf985c3efa546ceaf953e7061b33b4f1a5da78a32406b6258483efb1c64e49b77e3ca960e1f9f1be3779d657fd879ad8d282a3bf0a19d2c6f3a0b161e4fb0e2c2dff2efbb7fbf8be5b355b22f0fe2a2166ebe0627b9006cb740a190a50dcde3c068734f9554efaa9a196c7a985239ddce0b246c34c8627297fa3b3050cac82d7af9ab7063e8873f000f533f9a250cdc777c4980cf569f6c4011fcb3dfb5b1eb995c";
    const VALUE: u64 = 12_345_678_900;
    const MMR: u64 = 3;
    const HEIGHT: u64 = 100;

    struct MockNode {
        highest_index: u64,
        tip_height: u64,
        tip_hash: String,
        /// Currently-unspent outputs (mmr_index, commit, proof).
        utxos: Vec<(u64, String, String)>,
        /// Commits the node still lists as unspent (for get_outputs).
        present: HashSet<String>,
    }

    async fn rpc(State(m): State<Arc<Mutex<MockNode>>>, Json(req): Json<Value>) -> Json<Value> {
        let m = m.lock().unwrap();
        let method = req["method"].as_str().unwrap_or("");
        let params = &req["params"];
        let ok = |v: Value| Json(json!({ "result": { "Ok": v } }));
        match method {
            "get_tip" => ok(json!({
                "height": m.tip_height, "last_block_pushed": m.tip_hash,
                "prev_block_to_last": "", "total_difficulty": 0
            })),
            "get_header" => ok(json!({ "hash": m.tip_hash, "height": m.tip_height })),
            "get_unspent_outputs" => {
                let start = params[0].as_u64().unwrap_or(0);
                let end = params[1].as_u64().unwrap_or(u64::MAX);
                let outs: Vec<Value> = m
                    .utxos
                    .iter()
                    .filter(|(idx, _, _)| *idx >= start && *idx <= end)
                    .map(|(idx, commit, proof)| {
                        json!({
                            "output_type": "Transaction", "commit": commit, "spent": false,
                            "proof": proof, "block_height": HEIGHT, "mmr_index": idx
                        })
                    })
                    .collect();
                let last = outs
                    .iter()
                    .filter_map(|o| o["mmr_index"].as_u64())
                    .max()
                    .unwrap_or(m.highest_index);
                ok(json!({
                    "highest_index": m.highest_index,
                    "last_retrieved_index": last,
                    "outputs": outs
                }))
            }
            "get_outputs" => {
                let commits = params[0].as_array().cloned().unwrap_or_default();
                let present: Vec<Value> = commits
                    .iter()
                    .filter_map(|c| c.as_str())
                    .filter(|c| m.present.contains(*c))
                    .map(|c| json!({ "commit": c }))
                    .collect();
                ok(Value::Array(present))
            }
            "get_pmmr_indices" => {
                ok(json!({ "highest_index": m.highest_index, "last_retrieved_index": 1, "outputs": [] }))
            }
            _ => Json(json!({ "result": { "Err": "unknown method" } })),
        }
    }

    fn cfg_for(url: String) -> Config {
        Config {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            database_url: Secret::new(String::new()),
            node_foreign_api_url: url,
            node_foreign_api_secret: Secret::new(String::new()),
            scan_poll_secs: 1,
            scan_batch_blocks: 100,
            restore_max_depth_days: 0,
            admin_key: Secret::new(String::new()),
        }
    }

    #[tokio::test]
    async fn scan_discovers_then_spends_the_golden_output() {
        // ── mock node serving the golden output as an unspent UTXO ──
        let state = Arc::new(Mutex::new(MockNode {
            highest_index: MMR,
            tip_height: 500,
            tip_hash: "tiphash".into(),
            utxos: vec![(MMR, COMMIT.into(), PROOF.into())],
            present: HashSet::from([COMMIT.to_string()]),
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = Router::new().route("/v2/foreign", post(rpc)).with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let node = GrinNode::new(&cfg_for(format!("http://{addr}/v2/foreign"))).unwrap();

        // ── real SQLite store + migration ──
        sqlx::any::install_default_drivers();
        let path = std::env::temp_dir().join("grinlws_scan_it.db");
        let _ = std::fs::remove_file(&path);
        let pool = sqlx::any::AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        let schema = include_str!("../migrations/0001_init.sql");
        let no_comments: String = schema
            .lines()
            .map(|l| l.find("--").map(|i| &l[..i]).unwrap_or(l))
            .collect::<Vec<_>>()
            .join("\n");
        for s in no_comments.split(';') {
            let s = s.trim();
            if !s.is_empty() {
                sqlx::query(s).execute(&pool).await.unwrap();
            }
        }

        db::register_account(&pool, RH, HEIGHT as i64).await.unwrap();

        // ── tick 1: discover ──
        tick(&pool, &node).await.expect("tick 1");
        let (total, count) = db::account_totals(&pool, RH).await.unwrap();
        assert_eq!((total, count), (VALUE, 1), "golden output discovered + stored");
        let (unlocked, _) = db::account_unlocked_totals(&pool, RH, 500).await.unwrap();
        assert_eq!(unlocked, VALUE, "regular output is spendable now");
        let outs = db::unspent_outputs(&pool, RH).await.unwrap();
        assert_eq!(outs[0].key_id.as_deref(), Some("0300000000000000000000000700000000"));

        // ── spend it: remove from the node's unspent set ──
        {
            let mut m = state.lock().unwrap();
            m.utxos.clear();
            m.present.clear();
        }
        // ── tick 2: reconcile by absence ──
        tick(&pool, &node).await.expect("tick 2");
        assert_eq!(db::account_totals(&pool, RH).await.unwrap().0, 0, "spent → zero balance");

        let _ = std::fs::remove_file(&path);
    }
}
