//! Grin node access — the chain data the scanner and broadcast path need.
//!
//! Talks to a grin node's Foreign API v2 (JSON-RPC): read the tip, page through
//! block outputs (commit + rangeproof), and relay a finalized transaction. This
//! is the ONLY component that reaches the chain; everything else reads the DB.
//!
//! Hostile-upstream hygiene: per-request + connect timeouts, a streaming
//! body-size cap, and errors that never interpolate an untrusted node body.
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

/// The recovered value + advisory derivation identifier for a matched output.
/// `key_id` is the authoritative 17-byte Grin Identifier (hex); `n_child` is
/// grin-canonical (see [`crate::rewind`]).
#[allow(dead_code)]
pub struct RewoundOutput {
    pub value: u64,
    pub key_id: String,
    pub n_child: u32,
}

/// Rewind a single block output against an account's `rewind_hash` (VIEW-ONLY).
/// Returns the recovered value + advisory derivation path on a match, or `None`
/// if the output does not belong to the account (or the node data is malformed).
///
/// `rewind_hash` is 64 hex chars (a 32-byte view credential); `out.commit` is a
/// 33-byte Pedersen commitment and `out.proof` the Bulletproof, both hex. Every
/// decode is fallible-to-`None`, so hostile/garbled node data can never panic the
/// scanner. The crypto (and its v3-only limitation) lives in [`crate::rewind`].
#[allow(dead_code)]
pub fn rewind_output(rewind_hash: &str, out: &ChainOutput) -> Option<RewoundOutput> {
    let rh: [u8; 32] = decode_hex_array(rewind_hash)?;
    let commit: [u8; crate::rewind::COMMITMENT_LEN] = decode_hex_array(&out.commit)?;
    let proof = hex::decode(&out.proof).ok()?;
    match crate::rewind::rewind_output_view_only(&rh, &commit, &proof) {
        Ok(Some(r)) => Some(RewoundOutput {
            value: r.value,
            key_id: r.key_id,
            n_child: r.n_child,
        }),
        Ok(None) => None,
        Err(e) => {
            // Invalid nonce / oversized proof from a hostile or lagging node —
            // skip this output for this account rather than failing the batch.
            tracing::trace!(error = %e, "grin rewind skipped an output");
            None
        }
    }
}

/// Decode a fixed-length hex string into `[u8; N]`; `None` on bad hex or a
/// length mismatch (never panics on hostile node data).
#[allow(dead_code)]
fn decode_hex_array<const N: usize>(s: &str) -> Option<[u8; N]> {
    hex::decode(s).ok()?.try_into().ok()
}
