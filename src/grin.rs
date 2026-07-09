//! Grin node access — the chain data the scanner and broadcast path need.
//!
//! Talks to a grin node's Foreign API v2 (JSON-RPC). Grin's chain state is a
//! set of unspent outputs in a Merkle Mountain Range; a light wallet scans by
//! walking that output set in **PMMR insertion-index order** (NOT block by
//! block — `get_block` doesn't even return rangeproofs). So the surface here is:
//!   - `get_unspent_outputs(start, end, max)` — page the UTXO set WITH proofs
//!     (the discovery call the scanner rewinds); returns outputs in ascending
//!     `mmr_index`, only currently-unspent ones.
//!   - `get_outputs(commits)` — which of our tracked commits are still unspent
//!     (spend detection is by ABSENCE from the set).
//!   - `get_pmmr_indices(height)` — map a wallet birthday height → a start index.
//!   - `get_header(height)` / `get_tip` — reorg detection + chain tip.
//!   - `push_transaction(tx)` — relay a finalized tx.
//!
//! This is the ONLY component that reaches the chain; everything else reads the
//! DB. Hostile-upstream hygiene: per-request + connect timeouts, a streaming
//! body-size cap, generic errors that never interpolate an untrusted node body,
//! and permissive (never `deny_unknown_fields`) deserialization so a node
//! version bump can't break parsing. Per-output decode failures skip that output
//! rather than aborting a page.

use std::collections::HashSet;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::Config;
use crate::error::{Error, Result};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard cap on any node response body (a full output page is the largest).
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Chunk size when re-checking tracked commits (grin-wallet's default batch).
const OUTPUTS_CHUNK: usize = 500;

/// A single unspent output from the UTXO listing: the commitment + its
/// rangeproof (both hex), plus the classification the scanner needs. The
/// rangeproof is what [`rewind_output`] rewinds to test account ownership.
/// (`lock_height` is NOT carried — the node does not send it; the scanner
/// derives it from `is_coinbase` + `height`.)
#[derive(Debug, Clone)]
pub struct ChainOutput {
    pub commit: String,
    pub proof: String,
    pub is_coinbase: bool,
    pub height: u64,
    pub mmr_index: u64,
}

/// One page of the UTXO traversal (`get_unspent_outputs`).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OutputPage {
    /// Highest insertion index available in the output PMMR (the scan target).
    pub highest_index: u64,
    /// Index of the last output on THIS page (advance the cursor to here).
    pub last_retrieved_index: u64,
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
        let tip: Tip = self.rpc("get_tip", json!([])).await?;
        Ok(tip.height)
    }

    /// Chain tip height + its block hash (`last_block_pushed`) — the hash is the
    /// reorg-detection checkpoint the scanner stores in its cursor.
    #[allow(dead_code)]
    pub async fn get_tip(&self) -> Result<(u64, String)> {
        #[derive(Deserialize)]
        struct Tip {
            height: u64,
            last_block_pushed: String,
        }
        let tip: Tip = self.rpc("get_tip", json!([])).await?;
        Ok((tip.height, tip.last_block_pushed))
    }

    /// Page the unspent-output set by PMMR index, WITH rangeproofs. The trailing
    /// `include_proof = true` is mandatory — the proof is absent otherwise, which
    /// would silently yield zero rewind matches. Outputs come back only if
    /// currently unspent, in ascending `mmr_index`; ones missing a proof or
    /// creation height are skipped (never usable for a rewind).
    #[allow(dead_code)]
    pub async fn get_unspent_outputs(
        &self,
        start_index: u64,
        end_index: Option<u64>,
        max: u64,
    ) -> Result<OutputPage> {
        let params = json!([start_index, end_index, max, true]);
        let listing: OutputListingResp = self.rpc("get_unspent_outputs", params).await?;
        let outputs = listing
            .outputs
            .into_iter()
            .filter_map(ChainOutput::from_printable)
            .collect();
        Ok(OutputPage {
            highest_index: listing.highest_index,
            last_retrieved_index: listing.last_retrieved_index,
            outputs,
        })
    }

    /// Of `commits`, return the set the node still lists as UNSPENT. Any tracked
    /// commit NOT in the returned set has left the UTXO set = spent/pruned. The
    /// node includes no proofs here (`include_proof = false`). Chunked to stay
    /// under request limits.
    #[allow(dead_code)]
    pub async fn outputs_present(&self, commits: &[String]) -> Result<HashSet<String>> {
        #[derive(Deserialize)]
        struct OutResp {
            commit: String,
        }
        let mut present = HashSet::with_capacity(commits.len());
        for chunk in commits.chunks(OUTPUTS_CHUNK) {
            // [commits, start_height=null, end_height=null, include_proof=false,
            //  include_merkle_proof=false]
            let params = json!([chunk, Value::Null, Value::Null, false, false]);
            let outs: Vec<OutResp> = self.rpc("get_outputs", params).await?;
            for o in outs {
                present.insert(o.commit);
            }
        }
        Ok(present)
    }

    /// The first PMMR output index at or after block `height` — used to start a
    /// new account's backfill from its birthday instead of index 0.
    #[allow(dead_code)]
    pub async fn start_index_for_height(&self, height: u64) -> Result<u64> {
        let params = json!([height, Value::Null]);
        let listing: OutputListingResp = self.rpc("get_pmmr_indices", params).await?;
        Ok(listing.last_retrieved_index)
    }

    /// The canonical main-chain block hash at `height` — the reorg probe: if it
    /// differs from the hash the cursor recorded for that height, the chain
    /// reorganized. Propagates an error (rather than a sentinel) on node failure
    /// so a transient hiccup aborts the tick instead of faking a reorg.
    #[allow(dead_code)]
    pub async fn header_hash_at(&self, height: u64) -> Result<String> {
        let params = json!([height, Value::Null, Value::Null]);
        #[derive(Deserialize)]
        struct Header {
            hash: String,
        }
        let h: Header = self.rpc("get_header", params).await?;
        Ok(h.hash)
    }

    /// Relay a finalized transaction to the node (Foreign API `push_transaction`,
    /// `fluff = false`). Success is `{"result":{"Ok":null}}`; the null payload is
    /// why this can't go through the value-returning [`Self::rpc`] helper.
    pub async fn push_transaction(&self, tx: &Value) -> Result<()> {
        self.rpc_ok_unit("push_transaction", json!([tx, false])).await
    }

    /// Liveness probe for `/ready` — a cheap tip read.
    pub async fn health_check(&self) -> Result<()> {
        self.get_tip_height().await.map(|_| ())
    }

    // ── JSON-RPC plumbing ───────────────────────────────────────────────────

    /// POST a JSON-RPC call and return the size-capped raw response bytes.
    /// Non-2xx maps to a generic [`Error::Node`] (no body interpolation).
    async fn rpc_raw(&self, method: &'static str, params: Value) -> Result<Vec<u8>> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let mut req = self.http.post(&self.foreign_api_url).json(&body);
        if !self.api_secret.is_empty() {
            req = req.basic_auth("grin", Some(&self.api_secret));
        }
        let resp = req.send().await.map_err(|_| Error::Node(method_label(method)))?;
        if !resp.status().is_success() {
            return Err(Error::Node(method_label(method)));
        }
        read_capped(resp, MAX_BODY_BYTES).await
    }

    /// JSON-RPC call returning a deserialized `result.Ok` payload. A `result.Err`
    /// or a JSON `"Ok": null` both map to [`Error::Node`] (use [`Self::rpc_ok_unit`]
    /// for calls whose success payload is null).
    async fn rpc<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
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
        let bytes = self.rpc_raw(method, params).await?;
        let parsed: RpcResp<T> =
            serde_json::from_slice(&bytes).map_err(|_| Error::Node(method_label(method)))?;
        parsed
            .result
            .and_then(|r| r.ok)
            .ok_or(Error::Node(method_label(method)))
    }

    /// JSON-RPC call whose success payload is `Ok` with any value (including
    /// `null`, as `push_transaction` returns). Success = a `result.Ok` key is
    /// present and there is no `result.Err` / top-level `error`.
    async fn rpc_ok_unit(&self, method: &'static str, params: Value) -> Result<()> {
        #[derive(Deserialize)]
        struct RawResp {
            #[serde(default)]
            result: Option<serde_json::Map<String, Value>>,
            #[serde(default)]
            error: Option<Value>,
        }
        let bytes = self.rpc_raw(method, params).await?;
        let parsed: RawResp =
            serde_json::from_slice(&bytes).map_err(|_| Error::Node(method_label(method)))?;
        if parsed.error.is_some() {
            return Err(Error::Node(method_label(method)));
        }
        match parsed.result {
            // "Ok" present (value may be null) and no "Err" → success.
            Some(map) if map.contains_key("Ok") && !map.contains_key("Err") => Ok(()),
            _ => Err(Error::Node(method_label(method))),
        }
    }
}

impl ChainOutput {
    /// Map a node `OutputPrintable` to a scanner-usable output. Returns `None`
    /// (skip) for a spent output, or one missing the proof / creation height —
    /// neither can be rewound or aged, and neither aborts the page.
    fn from_printable(p: OutputPrintableResp) -> Option<ChainOutput> {
        if p.spent {
            return None;
        }
        Some(ChainOutput {
            commit: p.commit,
            proof: p.proof?,
            is_coinbase: matches!(p.output_type, OutputTypeResp::Coinbase),
            height: p.block_height?,
            mmr_index: p.mmr_index,
        })
    }
}

// ── permissive node deserialization (the "finicky Foreign API") ──────────────
// Our OWN structs, not grin's api::OutputPrintable (whose hand-written Deserialize
// hard-errors on missing fields and pulls in the secp stack). `#[serde(default)]`
// on optional-ish fields; never `deny_unknown_fields`.

#[derive(Deserialize)]
struct OutputListingResp {
    highest_index: u64,
    last_retrieved_index: u64,
    #[serde(default)]
    outputs: Vec<OutputPrintableResp>,
}

#[derive(Deserialize)]
struct OutputPrintableResp {
    output_type: OutputTypeResp,
    commit: String,
    #[serde(default)]
    spent: bool,
    #[serde(default)]
    proof: Option<String>,
    #[serde(default)]
    block_height: Option<u64>,
    mmr_index: u64,
}

#[derive(Deserialize)]
enum OutputTypeResp {
    Coinbase,
    Transaction,
}

/// Map a method to a static label so an untrusted node body is never surfaced.
fn method_label(method: &str) -> &'static str {
    match method {
        "get_tip" => "grin get_tip",
        "get_unspent_outputs" => "grin get_unspent_outputs",
        "get_outputs" => "grin get_outputs",
        "get_pmmr_indices" => "grin get_pmmr_indices",
        "get_header" => "grin get_header",
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

/// Rewind a single UTXO-set output against an account's `rewind_hash`
/// (VIEW-ONLY). Returns the recovered value + advisory derivation path on a
/// match, or `None` if the output does not belong to the account (or the node
/// data is malformed).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_output_listing_and_filters_spent_and_proofless() {
        // A representative `get_unspent_outputs` result.Ok (OutputListing).
        let json = r#"{
            "highest_index": 1000,
            "last_retrieved_index": 6,
            "outputs": [
                {"output_type":"Transaction","commit":"09aa","spent":false,"proof":"dead","proof_hash":"x","block_height":100,"merkle_proof":null,"mmr_index":3},
                {"output_type":"Coinbase","commit":"09bb","spent":false,"proof":"beef","block_height":50,"mmr_index":4},
                {"output_type":"Transaction","commit":"09cc","spent":true,"proof":"9999","block_height":90,"mmr_index":5},
                {"output_type":"Transaction","commit":"09dd","spent":false,"block_height":95,"mmr_index":6}
            ]
        }"#;
        let listing: OutputListingResp = serde_json::from_str(json).expect("listing parses");
        assert_eq!(listing.highest_index, 1000);
        assert_eq!(listing.last_retrieved_index, 6);

        let outs: Vec<ChainOutput> = listing
            .outputs
            .into_iter()
            .filter_map(ChainOutput::from_printable)
            .collect();
        // 09cc is spent, 09dd has no proof → both filtered; 09aa + 09bb remain.
        assert_eq!(outs.len(), 2, "spent + proofless outputs must be dropped");
        assert_eq!(outs[0].commit, "09aa");
        assert!(!outs[0].is_coinbase);
        assert_eq!(outs[0].height, 100);
        assert_eq!(outs[1].commit, "09bb");
        assert!(outs[1].is_coinbase, "Coinbase output_type → is_coinbase");
    }

    #[test]
    fn tolerates_unknown_fields_and_missing_optionals() {
        // A future node version adds a field and omits `spent` — must still parse.
        let j = r#"{"output_type":"Transaction","commit":"09ff","proof":"aa","block_height":1,"mmr_index":9,"future_field":true}"#;
        let p: OutputPrintableResp = serde_json::from_str(j).expect("tolerant parse");
        let out = ChainOutput::from_printable(p).expect("kept");
        assert_eq!(out.commit, "09ff");
        assert!(!out.is_coinbase);
        assert_eq!(out.mmr_index, 9);
    }
}
