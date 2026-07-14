//! zkBoost HTTP API client.
//!
//! Wraps the subset of the zkBoost Proof Node API that `proofessoor` needs.
//! Responses are treated as untrusted: status codes are checked and bodies are
//! decoded into explicit types.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use url::Url;
use zkboost_client::{ChainConfig, Hash256, NewPayloadRequest, ProofType, zkBoostClient};

pub use zkboost_client::ProofEvent;

/// Bound on establishing a TCP connection to zkBoost.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on each socket read. This is the hang detector for both unary calls
/// and the long-lived SSE event stream: zkBoost sends an SSE keep-alive every
/// 15s, so a 60s read gap means the connection is dead, not quiet. A *total*
/// request timeout would instead kill the healthy event stream on schedule
/// (it did, every 30s), forcing constant reconnect/reconcile churn.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Parses a proof type name (e.g. `reth-zisk`) into a zkBoost [`ProofType`].
pub fn parse_proof_type(name: &str) -> Result<ProofType> {
    name.parse()
        .map_err(|e| anyhow!("unknown proof type '{name}': {e}"))
}

/// Default directory for saved proofs when `--out-dir` is not given.
const DEFAULT_OUT_DIR: &str = "./proofs";

/// Optional actions taken for each proof once it completes.
#[derive(Debug, Clone, Default)]
pub struct Artifacts {
    /// Save the completed proof bytes to disk.
    pub download: bool,
    /// Verify the completed proof through zkBoost.
    pub verify: bool,
    /// Directory to save proofs in (implies download). Defaults to `./proofs`.
    pub out_dir: Option<PathBuf>,
}

impl Artifacts {
    /// Whether any action requires fetching the proof bytes.
    pub fn needs_proof_bytes(&self) -> bool {
        self.saves() || self.verify
    }

    /// Whether the proof should be written to disk.
    fn saves(&self) -> bool {
        self.download || self.out_dir.is_some()
    }

    /// The directory to save proofs in.
    fn out_dir(&self) -> &Path {
        self.out_dir
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_OUT_DIR))
    }
}

/// Capabilities of a single proof type advertised by a zkBoost server.
///
/// Only the fields `proof_type` validation needs are decoded; the server may
/// send additional fields (`kind`, `can_verify`), which are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct ProofTypeInfo {
    /// The proof type identifier (e.g. `reth-zisk`). Encoded on the wire as a string.
    pub proof_type: String,
    /// Whether the server can generate this proof.
    pub can_prove: bool,
}

/// Response body for `GET /v1/proof_types`.
#[derive(Debug, Deserialize)]
struct ProofTypesResponse {
    proof_types: Vec<ProofTypeInfo>,
}

/// HTTP client for the zkBoost Proof Node API.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    endpoint: Url,
    inner: zkBoostClient,
}

impl Client {
    /// Creates a client targeting the given zkBoost base URL.
    ///
    /// With the crate's `otel` feature, zkboost-client injects the current
    /// span's W3C trace context into every outbound request. The application
    /// installs the propagator when OTLP export is configured.
    pub fn new(endpoint: Url) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .context("failed to build zkBoost HTTP client")?;
        let inner = zkBoostClient::with_http_client(endpoint.clone(), http.clone());
        Ok(Self {
            http,
            endpoint,
            inner,
        })
    }

    /// Submits a proof request and returns the `new_payload_request_root`
    /// computed by the server (`POST /v1/execution_proof_requests`). The
    /// chain config names the block's active execution fork; zkBoost rejects
    /// requests whose config does not match the payload.
    pub async fn request_proof(
        &self,
        request: &NewPayloadRequest,
        chain_config: &ChainConfig,
        proof_types: &[ProofType],
    ) -> Result<Hash256> {
        let response = self
            .inner
            .request_proof(request, chain_config, proof_types)
            .await
            .context("zkBoost rejected the proof request")?;
        Ok(response.new_payload_request_root)
    }

    /// Subscribes to all zkBoost proof events (no root filter).
    pub fn subscribe_proof_events(&self) -> impl Stream<Item = Result<ProofEvent>> + Send + '_ {
        self.inner
            .subscribe_proof_events(None)
            .map(|event| event.context("zkBoost proof event stream error"))
    }

    /// Subscribes to the proof events of a single request root.
    ///
    /// On connect zkBoost replays the completions it still holds cached for
    /// that root before any live events; failures are never replayed. That
    /// replay is what reconciliation probes for after a stream drop.
    pub fn subscribe_root_events(
        &self,
        root: Hash256,
    ) -> impl Stream<Item = Result<ProofEvent>> + Send + '_ {
        self.inner
            .subscribe_proof_events(Some(root))
            .map(|event| event.context("zkBoost proof event stream error"))
    }

    /// Waits for every requested proof to complete or fail, logging each result.
    ///
    /// Subscribes to the proof event stream filtered to `root`. For completed
    /// proofs it optionally downloads, verifies, and writes the bytes per
    /// `artifacts`; verification requires the block's chain config. Returns an
    /// error if any requested proof fails or the stream ends before all are
    /// resolved.
    pub async fn wait_for_proofs(
        &self,
        root: Hash256,
        proof_types: &[ProofType],
        chain_config: Option<&ChainConfig>,
        artifacts: &Artifacts,
    ) -> Result<()> {
        let mut events = Box::pin(self.inner.subscribe_proof_events(Some(root)));
        let mut remaining: HashSet<ProofType> = proof_types.iter().copied().collect();
        let mut failed: Vec<ProofType> = Vec::new();

        while !remaining.is_empty() {
            let Some(event) = events.next().await else {
                bail!("zkBoost proof event stream ended before all proofs resolved");
            };
            let event = event.context("error reading the zkBoost proof event stream")?;

            // Ignore events for proof types that were not requested.
            if !remaining.remove(&event.proof_type()) {
                continue;
            }

            match event {
                ProofEvent::ProofComplete(complete) => {
                    tracing::info!(%root, proof_type = %complete.proof_type, "proof complete");
                    if artifacts.needs_proof_bytes() {
                        self.collect_artifacts(root, complete.proof_type, chain_config, artifacts)
                            .await?;
                    }
                }
                ProofEvent::ProofFailure(failure) => {
                    tracing::warn!(
                        %root,
                        proof_type = %failure.proof_type,
                        reason = ?failure.reason,
                        error = %failure.error,
                        "proof failed"
                    );
                    failed.push(failure.proof_type);
                }
            }
        }

        if failed.is_empty() {
            Ok(())
        } else {
            let names: Vec<&str> = failed.iter().map(ProofType::as_str).collect();
            bail!("proof generation failed for: {}", names.join(","))
        }
    }

    /// Fetches a completed proof once, then verifies and/or saves it per
    /// `artifacts`. Verification sends the block's chain config alongside the
    /// proof, so `--verify` requires it to have resolved.
    pub async fn collect_artifacts(
        &self,
        root: Hash256,
        proof_type: ProofType,
        chain_config: Option<&ChainConfig>,
        artifacts: &Artifacts,
    ) -> Result<()> {
        let proof = self
            .inner
            .get_proof(root, proof_type)
            .await
            .context("failed to download proof bytes")?;

        if artifacts.verify {
            let chain_config = chain_config
                .context("cannot verify: no chain config resolved for the proof's block")?;
            let response = self
                .inner
                .verify_proof(root, chain_config, proof_type, &proof)
                .await
                .context("failed to verify proof")?;
            if !response.status.is_valid() {
                bail!("proof for {proof_type} failed verification");
            }
            tracing::info!(%root, %proof_type, "proof verified");
        }

        if artifacts.saves() {
            let dir = artifacts.out_dir();
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("failed to create out-dir {}", dir.display()))?;
            let path = dir.join(format!("{root}_{proof_type}.proof"));
            tokio::fs::write(&path, &proof)
                .await
                .with_context(|| format!("failed to write proof to {}", path.display()))?;
            tracing::info!(%root, %proof_type, path = %path.display(), bytes = proof.len(), "proof saved");
        }

        Ok(())
    }

    /// Fetches the proof types advertised by the server (`GET /v1/proof_types`).
    pub async fn proof_types(&self) -> Result<Vec<ProofTypeInfo>> {
        let url = self
            .endpoint
            .join("/v1/proof_types")
            .context("failed to construct proof_types URL")?;

        let response = self
            .http
            .get(url)
            .send()
            .await
            .context("failed to reach zkBoost")?
            .error_for_status()
            .context("zkBoost returned an error status for GET /v1/proof_types")?;

        let body: ProofTypesResponse = response
            .json()
            .await
            .context("failed to decode the proof_types response")?;

        Ok(body.proof_types)
    }
}
