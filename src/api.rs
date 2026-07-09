//! HTTP surface — mirrors the monero-lws user API so a wallet backend can proxy
//! to it exactly as it proxies a Monero LWS.
//!
//! AUTH: the user API is unauthenticated at THIS layer — grin-lws binds to
//! loopback / a private network and trusts the proxying wallet backend (which
//! does the JWT auth) as its only caller. The admin API (`/list_accounts`,
//! `/rescan`) is gated by a shared bearer (`GRINLWS_ADMIN_KEY`) and disabled
//! when unset.
//!
//! The `rewind_hash` in every user request is a VIEW credential (public-key
//! derived — it cannot spend). Its request DTOs omit `Debug` so it is never
//! logged.

use std::sync::Arc;

use axum::{
    extract::State,
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::db;
use crate::error::{validate_hex64, Error, Result};
use crate::AppState;

// ── user DTOs ──────────────────────────────────────────────────────────────────

/// Register an account (a view credential) for background scanning. Idempotent.
#[derive(Deserialize)]
pub struct RegisterRequest {
    pub rewind_hash: String,
    /// Wallet birthday to scan from. Omit to start at the current tip.
    #[serde(default)]
    pub start_height: Option<u64>,
}

#[derive(Serialize)]
pub struct RegisterResponse {
    pub registered: bool,
    /// True if this call created a new account (vs a no-op on an existing one).
    pub new_account: bool,
    pub scan_height: u64,
    pub start_height: u64,
}

/// Carries a view credential — omit `Debug`, never log.
#[derive(Deserialize)]
pub struct RewindHashRequest {
    pub rewind_hash: String,
}

#[derive(Serialize)]
pub struct BalanceResponse {
    /// Total unspent (nanogrin).
    pub total: u64,
    /// Number of unspent outputs.
    pub count: u64,
    /// How far the scanner has processed this account.
    pub scanned_height: u64,
    /// Current chain tip.
    pub blockchain_height: u64,
}

/// An unspent output served to the client — WITH the recovered derivation path,
/// so the client spends directly (no client-side identify search).
#[derive(Serialize)]
pub struct UnspentOut {
    pub commit: String,
    pub value: u64,
    pub height: u64,
    pub mmr_index: u64,
    pub is_coinbase: bool,
    pub lock_height: u64,
    pub key_id: Option<String>,
    pub n_child: Option<u32>,
}

#[derive(Serialize)]
pub struct UnspentOutsResponse {
    pub outputs: Vec<UnspentOut>,
    pub blockchain_height: u64,
}

#[derive(Deserialize)]
pub struct SubmitRawTxRequest {
    /// The finalized transaction object (grin node `push_transaction` input).
    pub tx: serde_json::Value,
}

#[derive(Serialize)]
pub struct SubmitRawTxResponse {
    pub ok: bool,
}

#[derive(Serialize)]
pub struct HeightResponse {
    pub height: u64,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}

// ── admin DTOs ─────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ListAccountsResponse {
    pub accounts: Vec<String>,
}

#[derive(Deserialize)]
pub struct RescanRequest {
    pub rewind_hash: String,
    /// Backwards-only target height (must be below the account's current
    /// scan_height); the scanner re-derives outputs from here.
    pub height: u64,
}

// ── user handlers ──────────────────────────────────────────────────────────────

/// `POST /register` — add a view credential to the scan set (idempotent). Analog
/// of monero-lws admin `add_account`, but self-service (view-only, no spend risk).
pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>> {
    validate_hex64(&req.rewind_hash, "rewind_hash")?;

    // Default new accounts to the current tip; a restore supplies an earlier
    // birthday, gated by the restore-depth policy.
    let tip = state.node.get_tip_height().await?;
    let start = match req.start_height {
        Some(h) => {
            enforce_restore_depth(&state, h, tip)?;
            h
        }
        None => tip,
    };

    let new_account = db::register_account(&state.pool, &req.rewind_hash, start as i64).await?;
    let scan_height = db::account_scan_height(&state.pool, &req.rewind_hash)
        .await?
        .unwrap_or(start);

    Ok(Json(RegisterResponse {
        registered: true,
        new_account,
        scan_height,
        start_height: start,
    }))
}

/// `POST /get_balance` — the account's unspent total + scan progress.
pub async fn get_balance(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RewindHashRequest>,
) -> Result<Json<BalanceResponse>> {
    validate_hex64(&req.rewind_hash, "rewind_hash")?;
    let (total, count) = db::account_totals(&state.pool, &req.rewind_hash).await?;
    let scanned_height = db::account_scan_height(&state.pool, &req.rewind_hash)
        .await?
        .unwrap_or(0);
    let blockchain_height = state.node.get_tip_height().await?;
    Ok(Json(BalanceResponse {
        total,
        count,
        scanned_height,
        blockchain_height,
    }))
}

/// `POST /get_unspent_outs` — the spendable set WITH derivation paths.
pub async fn get_unspent_outs(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RewindHashRequest>,
) -> Result<Json<UnspentOutsResponse>> {
    validate_hex64(&req.rewind_hash, "rewind_hash")?;
    let stored = db::unspent_outputs(&state.pool, &req.rewind_hash).await?;
    let blockchain_height = state.node.get_tip_height().await?;
    let outputs = stored
        .into_iter()
        .map(|o| UnspentOut {
            commit: o.commit,
            value: o.value,
            height: o.height,
            mmr_index: o.mmr_index,
            is_coinbase: o.is_coinbase,
            lock_height: o.lock_height,
            key_id: o.key_id,
            n_child: o.n_child,
        })
        .collect();
    Ok(Json(UnspentOutsResponse {
        outputs,
        blockchain_height,
    }))
}

/// `POST /submit_raw_tx` — relay a finalized tx to the node.
pub async fn submit_raw_tx(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SubmitRawTxRequest>,
) -> Result<Json<SubmitRawTxResponse>> {
    state.node.push_transaction(&req.tx).await?;
    Ok(Json(SubmitRawTxResponse { ok: true }))
}

/// `GET /height` — current chain tip.
pub async fn height(State(state): State<Arc<AppState>>) -> Result<Json<HeightResponse>> {
    let height = state.node.get_tip_height().await?;
    Ok(Json(HeightResponse { height }))
}

// ── health ─────────────────────────────────────────────────────────────────────

/// Liveness — 200 if the process is up. No upstream I/O.
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "grin-lws",
    })
}

/// Readiness — reaches the grin node (503 when the node is down).
pub async fn ready(State(state): State<Arc<AppState>>) -> Result<Json<HealthResponse>> {
    state.node.health_check().await?;
    Ok(Json(HealthResponse {
        status: "ready",
        service: "grin-lws",
    }))
}

// ── admin handlers (bearer-gated) ──────────────────────────────────────────────

pub async fn list_accounts(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ListAccountsResponse>> {
    require_admin(&state, &headers)?;
    let accounts = db::list_account_hashes(&state.pool).await?;
    Ok(Json(ListAccountsResponse { accounts }))
}

pub async fn rescan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RescanRequest>,
) -> Result<Json<HealthResponse>> {
    require_admin(&state, &headers)?;
    validate_hex64(&req.rewind_hash, "rewind_hash")?;
    db::rescan_account(&state.pool, &req.rewind_hash, req.height as i64).await?;
    Ok(Json(HealthResponse {
        status: "ok",
        service: "grin-lws",
    }))
}

// ── helpers ────────────────────────────────────────────────────────────────────

/// Enforce the restore-depth bound: reject a `start_height` more than
/// `restore_max_depth_days` worth of blocks behind the tip (0 = unbounded).
/// Grin targets ~60s blocks ⇒ 1440 blocks/day.
fn enforce_restore_depth(state: &AppState, start_height: u64, tip: u64) -> Result<()> {
    let max_days = state.config.restore_max_depth_days;
    if max_days == 0 {
        return Ok(());
    }
    let max_depth = (max_days as u64).saturating_mul(1440);
    if tip.saturating_sub(start_height) > max_depth {
        return Err(Error::Validation(format!(
            "start_height too far behind tip (max {max_days} days)"
        )));
    }
    Ok(())
}

/// Gate the admin API on the shared bearer. Disabled (401) when no admin key is
/// configured, so admin routes are never open by default.
fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<()> {
    if !state.config.admin_enabled() {
        return Err(Error::Unauthorized);
    }
    let expected = format!("Bearer {}", state.config.admin_key.expose());
    let ok = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == expected)
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(Error::Unauthorized)
    }
}

// ── router ─────────────────────────────────────────────────────────────────────

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // health
        .route("/health", get(health))
        .route("/ready", get(ready))
        // user API (mirrors monero-lws user endpoints)
        .route("/register", post(register))
        .route("/get_balance", post(get_balance))
        .route("/get_unspent_outs", post(get_unspent_outs))
        .route("/submit_raw_tx", post(submit_raw_tx))
        .route("/height", get(height))
        // admin API (bearer-gated)
        .route("/list_accounts", post(list_accounts))
        .route("/rescan", post(rescan))
        .with_state(state)
}
