//! Grin node access — the chain data the scanner and broadcast path need.
//!
//! Talks to a grin node's Foreign API v2 (JSON-RPC): read the tip, page through
//! block outputs (commit + rangeproof), and relay a finalized transaction. This
//! is the ONLY component that reaches the chain; everything else reads the DB.
//!
//! Hostile-upstream hygiene (carried from smirk-backend-core's node client):
//! per-request + connect timeouts, a streaming body-size cap, and errors that
//! never interpolate an untrusted node body.
//!
//! REWIND (the hard, deferred part): recognizing which of a block's outputs
//! belong to a registered account means rewinding each output's Bulletproof
//! rangeproof against that account's `rewind_hash`-derived nonce, then parsing
//! the proof message to recover the derivation path — exactly what
//! `grin_recover_output` does client-side, done here server-side. That requires
//! the grin crypto stack (secp256k1 / bulletproofs) and is stubbed below.

use std::time::Duration;

use serde::Deserialize;

use crate::config::Config;
use crate::error::{Error, Result};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard cap on any node response body (a full block page is the largest).
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// A raw output as read from a block: the commitment and its rangeproof. The
/// rangeproof is what the scanner rewinds to test account ownership.
#[derive(Debug, Clone)]
pub struct ChainOutput {
    pub commit: String,
    pub proof: String,
    pub is_coinbase: bool,
    pub height: u64,
    pub mmr_index: u64,
    pub lock_height: u64,
}

/// A block's spend/create surface: inputs consume outputs (by commitment),
/// outputs create them.
#[derive(Debug, Clone)]
pub struct BlockView {
    pub height: u64,
    pub hash: String,
    pub prev_hash: String,
    /// Input commitments spent in this block (for spend detection).
    pub input_commits: Vec<String>,
    /// Outputs created in this block (candidates for rewind).
    pub outputs: Vec<ChainOutput>,
}

#[derive(Clone)]
pub struct GrinNode {
    foreign_api_url: String,
    api_secret: String,
    http: reqwest::Client,
}

impl GrinNode {
    pub fn new(cfg: &Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|_| Error::Node("failed to build HTTP client"))?;
        Ok(Self {
            foreign_api_url: cfg.node_foreign_api_url.clone(),
            api_secret: cfg.node_foreign_api_secret.expose().to_string(),
            http,
        })
    }

    /// Current chain tip height (Foreign API `get_tip`).
    pub async fn get_tip_height(&self) -> Result<u64> {
        #[derive(Deserialize)]
        struct Tip {
            height: u64,
        }
        let tip: Tip = self.rpc("get_tip", serde_json::json!([])).await?;
        Ok(tip.height)
    }

    /// Read a block at `height` as a [`BlockView`] (Foreign API `get_block`).
    ///
    /// STUB: real impl parses the node's block JSON into inputs + outputs
    /// (commit + rangeproof + features). Left unimplemented pending the scanner.
    pub async fn get_block(&self, _height: u64) -> Result<BlockView> {
        Err(Error::NotImplemented("grin get_block"))
    }

    /// Relay a finalized transaction to the node (Foreign API `push_transaction`).
    ///
    /// STUB: real impl posts `push_transaction(tx, fluff=false)` and treats any
    /// JSON-RPC error as [`Error::Node`].
    pub async fn push_transaction(&self, _tx: &serde_json::Value) -> Result<()> {
        Err(Error::NotImplemented("grin push_transaction"))
    }

    /// Liveness probe for `/ready` — a cheap tip read.
    pub async fn health_check(&self) -> Result<()> {
        self.get_tip_height().await.map(|_| ())
    }

    /// Minimal JSON-RPC helper with a size-capped response read. Non-2xx and
    /// JSON-RPC `error` payloads map to a generic [`Error::Node`] (no body
    /// interpolation).
    async fn rpc<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        params: serde_json::Value,
    ) -> Result<T> {
        #[derive(Deserialize)]
        struct RpcResp<T> {
            result: Option<RpcOk<T>>,
        }
        #[derive(Deserialize)]
        struct RpcOk<T> {
            #[serde(rename = "Ok")]
            ok: Option<T>,
        }

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let mut req = self.http.post(&self.foreign_api_url).json(&body);
        if !self.api_secret.is_empty() {
            req = req.basic_auth("grin", Some(&self.api_secret));
        }
        let resp = req.send().await.map_err(|_| Error::Node(method_label(method)))?;
        if !resp.status().is_success() {
            return Err(Error::Node(method_label(method)));
        }
        let bytes = read_capped(resp, MAX_BODY_BYTES).await?;
        let parsed: RpcResp<T> = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Node(method_label(method)))?;
        parsed
            .result
            .and_then(|r| r.ok)
            .ok_or(Error::Node(method_label(method)))
    }
}

/// Map a method to a static label so the untrusted node body is never surfaced.
fn method_label(method: &str) -> &'static str {
    match method {
        "get_tip" => "grin get_tip",
        "get_block" => "grin get_block",
        "push_transaction" => "grin push_transaction",
        _ => "grin rpc",
    }
}

/// Read a response body enforcing `cap` as bytes arrive (content-length is
/// attacker-asserted, so untrusted).
async fn read_capped(resp: reqwest::Response, cap: usize) -> Result<Vec<u8>> {
    use futures::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| Error::Node("grin read failed"))?;
        if buf.len().saturating_add(chunk.len()) > cap {
            return Err(Error::Node("grin response exceeded size limit"));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// Rewind a single output's rangeproof against an account's `rewind_hash`. On a
/// match, returns the recovered value + derivation path; `None` if the output
/// does not belong to the account.
///
/// STUB (the core deferred milestone): this is the server-side analog of
/// `grin_recover_output`. It needs the grin crypto stack (secp256k1 +
/// bulletproofs) to compute the rewind nonce from `rewind_hash` + commitment and
/// rewind the proof. Uncomment the `secp256k1` / `aes-gcm`-adjacent crypto deps
/// in Cargo.toml and port the rewind + proof-message parse when the scanner
/// lands.
#[allow(dead_code)]
pub struct RewoundOutput {
    pub value: u64,
    pub key_id: String,
    pub n_child: u32,
}

#[allow(dead_code)]
pub fn rewind_output(_rewind_hash: &str, _out: &ChainOutput) -> Option<RewoundOutput> {
    // TODO: compute nonce = f(rewind_hash, commit); rewind bulletproof; parse
    // proof message -> (value, key_id, n_child). Returns None on non-match.
    None
}
