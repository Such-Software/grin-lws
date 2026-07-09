//! grin-lws — a Grin light-wallet-server.
//!
//! A REAL light-wallet-server mirroring [monero-lws]: register a view credential
//! (`rewind_hash`), run a BACKGROUND scanner over the grin chain, rewind block
//! outputs against registered credentials, STORE the matches (with recovered
//! derivation paths) in a database, and serve balance / unspent-outputs /
//! broadcast / height fast from that store.
//!
//! [monero-lws]: https://github.com/vtnerd/monero-lws
//!
//! WHY A DB
//! --------
//! A typical wallet backend exposes an ON-DEMAND, STATELESS grin scan: each
//! request forwards a `rewind_hash` to grin-wallet's `scan_rewind_hash`, which
//! rescans and returns matches, storing NOTHING. That is fine for grin's small
//! UTXO set but it is NOT a light-wallet-server. A real LWS — like monero-lws —
//! keeps a per-account output store maintained by a background scanner, so reads
//! are O(rows) not O(chain). Hence this service needs a database (the 3 tables in
//! `migrations/`).
//!
//! HOW A WALLET BACKEND PROXIES TO IT
//! ----------------------------------
//! Identical wiring to a Monero LWS client: the backend adds a grin-lws client
//! (`lws_url`, optional `admin_url` + `admin_key`); behind a feature flag, its
//! grin scan endpoint forwards the `rewind_hash` to this service's
//! `/get_unspent_outs` (and registers via `/register` at balance-fetch time,
//! idempotently, exactly as it awaits LWS registration for Monero). Non-custodial
//! guarantee preserved: the backend forwards only the `rewind_hash` and stores
//! nothing; grin-lws holds the `rewind_hash` for scanning just as monero-lws
//! holds the view key. NEVER a spend key.
//!
//! STATUS: SCAFFOLD. Routes, DTOs, config, DB layer, and the scanner loop
//! structure are real; the two chain-crypto seams — block parsing
//! (`grin::GrinNode::get_block`) and rangeproof rewind (`grin::rewind_output`) —
//! are stubbed. See README.md for the build-order plan.
//!
//! PUBLIC-CLEAN: all config is env-based with generic loopback defaults. No
//! hostnames, operator IPs, secrets, or deploy specifics anywhere in this repo.

mod api;
mod config;
mod db;
mod error;
mod grin;
mod scanner;
mod secret;

use std::sync::Arc;

use crate::config::Config;
use crate::grin::GrinNode;

/// Process-wide shared state.
pub struct AppState {
    pub config: Config,
    pub pool: sqlx::AnyPool,
    pub node: GrinNode,
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "grin_lws=info,tower_http=info".into()),
        )
        .init();

    let config = Config::from_env();
    let bind_addr = config.bind_addr;

    let pool = db::connect(config.database_url.expose())
        .await
        .expect("failed to build database pool");
    // In production, run `sqlx migrate run` (or embed `sqlx::migrate!()`) against
    // DATABASE_URL before serving. Left explicit here so the scaffold starts
    // without a live DB.

    let node = GrinNode::new(&config).expect("failed to build grin node client");

    // The background scanner is what makes this a real LWS.
    scanner::spawn(pool.clone(), node.clone(), config.clone());

    let state = Arc::new(AppState {
        config,
        pool,
        node,
    });

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .expect("failed to bind grin-lws listener");
    tracing::info!(
        %bind_addr,
        "grin-lws listening (SCAFFOLD — scanner block-parse + rewind stubbed)"
    );
    axum::serve(listener, api::router(state))
        .await
        .expect("grin-lws server error");
}
