//! Request-status model and its persistent store.
//!
//! The [`StatusStore`] trait is a narrow, swappable interface: stream mode
//! records each proof request and its outcome, deduplicates already-requested
//! roots across restarts, and exposes the latest processed slot. The default
//! [`JsonStatusStore`] keeps the state in memory and snapshots it to a JSON
//! file; a different backend (SQLite, etc.) can be dropped in behind the trait.
//!
//! A [`BlockRecord`] holds the block-level facts and one [`ProofRecord`] per
//! requested proof type; the block outcome is derived, worst-of, across its
//! proofs. State files written by earlier releases (v0.2.x and below) used a
//! flat, single-outcome record shape and are not readable — delete the state
//! directory when upgrading across that boundary.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// Outcome of a recorded proof request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Submitted to zkBoost; outcome not yet known.
    Sent,
    /// All requested proofs completed.
    Complete,
    /// At least one requested proof failed.
    Failed,
}

impl Outcome {
    /// The lowercase wire/display name (`sent`, `complete`, `failed`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sent => "sent",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

/// Which side a failure occurred on.
///
/// Submit-stage failures originate on the requestor side and may be retried;
/// proving-stage failures originate at zkBoost, which owns their coordination,
/// so they are not resubmitted here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    /// Failed before or at submission to zkBoost (requestor side).
    Submit,
    /// Failed during proving at zkBoost (prover side).
    Proving,
}

/// Structured detail for a failed proof, recorded for display and debugging.
///
/// `reason` is the low-cardinality category safe to use as a metric label;
/// `error` is free-form text and belongs only on the record, never as a label.
#[derive(Debug, Clone)]
pub struct Failure {
    /// Which side the failure occurred on.
    pub stage: FailureStage,
    /// Failure category (e.g. `WitnessTimeout`).
    pub reason: String,
    /// Human-readable detail about the specific failure.
    pub error: String,
}

/// Status of one requested proof (one proof type) for a block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofRecord {
    /// Proof type requested (e.g. `reth-zisk`).
    pub proof_type: String,
    /// Latest known outcome for this proof.
    pub outcome: Outcome,
    /// Which side the failure occurred on, set when the outcome is `Failed`.
    #[serde(default)]
    pub stage: Option<FailureStage>,
    /// Failure category (e.g. `WitnessTimeout`), set when the outcome is `Failed`.
    #[serde(default)]
    pub reason: Option<String>,
    /// Human-readable failure detail, set when the outcome is `Failed`.
    #[serde(default)]
    pub error: Option<String>,
    /// Unix milliseconds when the request was submitted.
    pub requested_at_ms: u64,
    /// Unix milliseconds when this proof resolved (completed or failed), if it has.
    #[serde(default)]
    pub resolved_at_ms: Option<u64>,
    /// Time this proof spent queued inside zkBoost before proving started.
    /// Always `None` today — zkBoost's proof events do not carry queue timing.
    /// TODO: populate by denormalizing zkBoost's SSE payload once events carry it.
    #[serde(default)]
    pub queue_ms: Option<u64>,
    /// Pure proving time inside zkBoost, excluding queueing.
    /// Always `None` today, for the same reason as `queue_ms`.
    /// TODO: populate by denormalizing zkBoost's SSE payload once events carry it.
    #[serde(default)]
    pub prove_ms: Option<u64>,
    /// 1-based attempt number. Always 1 today: failures are recorded, not
    /// resubmitted (zkBoost owns proof coordination). This field is where
    /// per-attempt bookkeeping lives once retry of transient submit failures
    /// is implemented.
    #[serde(default = "default_attempt")]
    pub attempt: u32,
}

/// Records created before attempt tracking carry an implicit first attempt.
fn default_attempt() -> u32 {
    1
}

impl ProofRecord {
    /// Creates a proof in the [`Outcome::Sent`] state, stamped with the submit time.
    fn sent(proof_type: String, requested_at_ms: u64) -> Self {
        Self {
            proof_type,
            outcome: Outcome::Sent,
            stage: None,
            reason: None,
            error: None,
            requested_at_ms,
            resolved_at_ms: None,
            queue_ms: None,
            prove_ms: None,
            attempt: 1,
        }
    }
}

/// A recorded proof request for one beacon block.
///
/// Holds the block-level facts plus one [`ProofRecord`] per requested proof
/// type. Block-level outcome and timing are derived from the proofs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRecord {
    /// Slot of the beacon block.
    pub slot: u64,
    /// Beacon block root (0x-hex).
    pub beacon_block_root: String,
    /// Execution block number.
    pub execution_block_number: u64,
    /// Execution block hash (0x-hex). The join key for zkBoost's per-block
    /// dashboard data (which is keyed by execution hash, not by the request
    /// root), for zkBoost's structured logs, and for explorer links. Unlike
    /// the block number, it stays unambiguous across reorgs.
    pub execution_block_hash: String,
    /// The `new_payload_request_root` identifying the request (0x-hex).
    pub new_payload_request_root: String,
    /// Unix milliseconds when the block was discovered (processing started).
    #[serde(default)]
    pub observed_at_ms: u64,
    /// OpenTelemetry trace id (hex) of the span covering this block's pipeline,
    /// when tracing is enabled and the trace was sampled.
    #[serde(default)]
    pub trace_id: Option<String>,
    /// Witness-generation time reported by zkBoost. Always `None` today —
    /// zkBoost's proof events do not carry witness timing.
    /// TODO: populate by denormalizing zkBoost's SSE payload once events carry it.
    #[serde(default)]
    pub witness_ms: Option<u64>,
    /// Per-proof-type status, one entry per requested proof type.
    pub proofs: Vec<ProofRecord>,
}

impl BlockRecord {
    /// Creates a record whose proofs are all in the [`Outcome::Sent`] state,
    /// stamped with the submit time.
    pub fn new(
        slot: u64,
        beacon_block_root: String,
        execution_block_number: u64,
        execution_block_hash: String,
        new_payload_request_root: String,
        proof_types: Vec<String>,
        observed_at_ms: u64,
    ) -> Self {
        let requested_at_ms = now_ms();
        Self {
            slot,
            beacon_block_root,
            execution_block_number,
            execution_block_hash,
            new_payload_request_root,
            observed_at_ms,
            trace_id: None,
            witness_ms: None,
            proofs: proof_types
                .into_iter()
                .map(|proof_type| ProofRecord::sent(proof_type, requested_at_ms))
                .collect(),
        }
    }

    /// Derived block outcome, worst-of across proofs: any failed proof makes
    /// the block failed; otherwise any unresolved proof keeps it in flight;
    /// otherwise every proof completed.
    pub fn outcome(&self) -> Outcome {
        let mut outcome = Outcome::Complete;
        for proof in &self.proofs {
            match proof.outcome {
                Outcome::Failed => return Outcome::Failed,
                Outcome::Sent => outcome = Outcome::Sent,
                Outcome::Complete => {}
            }
        }
        outcome
    }

    /// When the request was submitted: the earliest submission across proofs
    /// (all proofs of a block are submitted in one request today).
    pub fn requested_at_ms(&self) -> Option<u64> {
        self.proofs.iter().map(|p| p.requested_at_ms).min()
    }

    /// When the block resolved: the latest proof resolution, present only once
    /// every proof has resolved.
    pub fn resolved_at_ms(&self) -> Option<u64> {
        let mut latest: Option<u64> = None;
        for proof in &self.proofs {
            let resolved = proof.resolved_at_ms?;
            latest = Some(latest.map_or(resolved, |ms| ms.max(resolved)));
        }
        latest
    }

    /// Prep time (discovery to submit) in milliseconds.
    pub fn prep_ms(&self) -> u64 {
        self.requested_at_ms()
            .unwrap_or(self.observed_at_ms)
            .saturating_sub(self.observed_at_ms)
    }

    /// zkBoost turnaround (submit to resolution) in milliseconds, if resolved.
    pub fn completion_ms(&self) -> Option<u64> {
        match (self.resolved_at_ms(), self.requested_at_ms()) {
            (Some(resolved), Some(requested)) => Some(resolved.saturating_sub(requested)),
            _ => None,
        }
    }

    /// End-to-end time (discovery to resolution) in milliseconds, if resolved.
    pub fn end_to_end_ms(&self) -> Option<u64> {
        self.resolved_at_ms()
            .map(|resolved| resolved.saturating_sub(self.observed_at_ms))
    }

    /// The first failed proof's failure category, if any proof failed.
    pub fn failure_reason(&self) -> Option<&str> {
        self.proofs
            .iter()
            .find(|proof| proof.outcome == Outcome::Failed)
            .and_then(|proof| proof.reason.as_deref())
    }
}

/// The result of resolving one proof to a terminal outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProofResolution {
    /// Submit-to-resolution duration for the resolved proof, in milliseconds.
    pub duration_ms: u64,
    /// The derived block outcome after this transition.
    pub block_outcome: Outcome,
    /// Whether every proof on the block has now resolved.
    pub block_resolved: bool,
}

/// A narrow, swappable interface for persisting request status.
#[async_trait]
pub trait StatusStore: Send + Sync {
    /// Whether a request for this `new_payload_request_root` is already recorded.
    async fn seen(&self, root: &str) -> bool;

    /// Records (or replaces) a request record.
    ///
    /// Returns the request roots of any records evicted to honor the history
    /// cap, so the caller can release what it holds per record (open span
    /// handles) and refresh state-derived gauges.
    async fn record(&self, record: BlockRecord) -> Result<Vec<String>>;

    /// Resolves one proof of a recorded request to a terminal outcome.
    ///
    /// Returns `None` when there is nothing to transition: the root is not
    /// recorded, the proof type was not requested, or the proof already
    /// resolved (duplicate events are ignored, keeping this idempotent).
    async fn resolve_proof(
        &self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Result<Option<ProofResolution>>;

    /// The highest slot recorded so far, if any.
    async fn latest_slot(&self) -> Option<u64>;

    /// Number of proofs currently unresolved (submitted, outcome unknown).
    ///
    /// The inflight gauge is set absolutely from this count rather than
    /// maintained as event deltas, so it survives restarts and history
    /// eviction without drifting.
    async fn inflight_proofs(&self) -> usize;

    /// Records with at least one unresolved proof, newest slot first.
    ///
    /// Lives on the trait so the reconciliation sweep never forces a backend
    /// to materialize the whole history just to filter it.
    async fn unresolved_records(&self) -> Vec<BlockRecord>;

    /// All recorded requests, newest slot first.
    async fn records(&self) -> Vec<BlockRecord>;
}

/// In-memory status store that snapshots to a JSON file in a state directory.
#[derive(Debug)]
pub struct JsonStatusStore {
    path: PathBuf,
    max_history: usize,
    state: Mutex<State>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    /// Records keyed by `new_payload_request_root`.
    records: HashMap<String, BlockRecord>,
}

impl State {
    fn seen(&self, root: &str) -> bool {
        self.records.contains_key(root)
    }

    fn insert(&mut self, record: BlockRecord) {
        self.records
            .insert(record.new_payload_request_root.clone(), record);
    }

    fn resolve_proof(
        &mut self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Option<ProofResolution> {
        let record = self.records.get_mut(root)?;
        // Only a still-sent proof can transition; a repeated terminal event
        // (e.g. a reconciliation racing the live stream) is a no-op.
        let proof = record
            .proofs
            .iter_mut()
            .find(|proof| proof.proof_type == proof_type && proof.outcome == Outcome::Sent)?;
        let now = now_ms();
        proof.outcome = outcome;
        if let Some(failure) = failure {
            proof.stage = Some(failure.stage);
            proof.reason = Some(failure.reason);
            proof.error = Some(failure.error);
        }
        proof.resolved_at_ms = Some(now);
        let duration_ms = now.saturating_sub(proof.requested_at_ms);
        Some(ProofResolution {
            duration_ms,
            block_outcome: record.outcome(),
            block_resolved: record
                .proofs
                .iter()
                .all(|proof| proof.outcome != Outcome::Sent),
        })
    }

    fn latest_slot(&self) -> Option<u64> {
        self.records.values().map(|r| r.slot).max()
    }

    /// Number of proofs currently unresolved (submitted, outcome unknown).
    fn inflight_proofs(&self) -> usize {
        self.records
            .values()
            .flat_map(|record| &record.proofs)
            .filter(|proof| proof.outcome == Outcome::Sent)
            .count()
    }

    fn snapshot(&self) -> Vec<BlockRecord> {
        let mut records: Vec<BlockRecord> = self.records.values().cloned().collect();
        records.sort_by_key(|record| std::cmp::Reverse(record.slot));
        records
    }

    /// Records with at least one unresolved proof, newest slot first.
    fn unresolved(&self) -> Vec<BlockRecord> {
        let mut records: Vec<BlockRecord> = self
            .records
            .values()
            .filter(|record| record.proofs.iter().any(|p| p.outcome == Outcome::Sent))
            .cloned()
            .collect();
        records.sort_by_key(|record| std::cmp::Reverse(record.slot));
        records
    }

    /// Evicts the lowest-slot records until at most `max_history` remain
    /// (`max_history` of 0 means unlimited). Returns the request roots of the
    /// evicted records so callers can release per-record resources — an
    /// evicted record may still be unresolved, and its inflight-gauge share
    /// and open span handle must not outlive it.
    fn prune(&mut self, max_history: usize) -> Vec<String> {
        let mut evicted = Vec::new();
        if max_history == 0 {
            return evicted;
        }
        while self.records.len() > max_history {
            let oldest = self
                .records
                .iter()
                .min_by_key(|(_, record)| record.slot)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => {
                    self.records.remove(&key);
                    evicted.push(key);
                }
                None => break,
            }
        }
        evicted
    }
}

impl JsonStatusStore {
    /// Loads (or initializes) the store from `state_dir/status.json`.
    pub async fn load(state_dir: &Path, max_history: usize) -> Result<Self> {
        tokio::fs::create_dir_all(state_dir)
            .await
            .with_context(|| format!("failed to create state dir {}", state_dir.display()))?;
        let path = state_dir.join("status.json");

        let state = match tokio::fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).context("failed to parse status.json")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(error) => {
                return Err(error).context("failed to read status.json");
            }
        };

        Ok(Self {
            path,
            max_history,
            state: Mutex::new(state),
        })
    }

    /// Atomically writes the current state to disk (temp file + rename).
    async fn persist(&self, state: &State) -> Result<()> {
        let json = serde_json::to_vec_pretty(state).context("failed to serialize status")?;
        let tmp = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp, &json)
            .await
            .context("failed to write status snapshot")?;
        tokio::fs::rename(&tmp, &self.path)
            .await
            .context("failed to commit status snapshot")?;
        Ok(())
    }
}

#[async_trait]
impl StatusStore for JsonStatusStore {
    async fn seen(&self, root: &str) -> bool {
        self.state.lock().await.seen(root)
    }

    async fn record(&self, record: BlockRecord) -> Result<Vec<String>> {
        let mut state = self.state.lock().await;
        state.insert(record);
        let evicted = state.prune(self.max_history);
        self.persist(&state).await?;
        Ok(evicted)
    }

    async fn resolve_proof(
        &self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Result<Option<ProofResolution>> {
        let mut state = self.state.lock().await;
        let resolution = state.resolve_proof(root, proof_type, outcome, failure);
        // Persist only when something transitioned; no-op events cost no I/O.
        if resolution.is_some() {
            self.persist(&state).await?;
        }
        Ok(resolution)
    }

    async fn latest_slot(&self) -> Option<u64> {
        self.state.lock().await.latest_slot()
    }

    async fn inflight_proofs(&self) -> usize {
        self.state.lock().await.inflight_proofs()
    }

    async fn unresolved_records(&self) -> Vec<BlockRecord> {
        self.state.lock().await.unresolved()
    }

    async fn records(&self) -> Vec<BlockRecord> {
        self.state.lock().await.snapshot()
    }
}

/// In-memory status store without persistence (used when no state dir is set).
#[derive(Debug, Default)]
pub struct MemoryStatusStore {
    max_history: usize,
    state: Mutex<State>,
}

impl MemoryStatusStore {
    /// Creates a store retaining at most `max_history` records (0 = unlimited).
    pub fn new(max_history: usize) -> Self {
        Self {
            max_history,
            state: Mutex::new(State::default()),
        }
    }
}

#[async_trait]
impl StatusStore for MemoryStatusStore {
    async fn seen(&self, root: &str) -> bool {
        self.state.lock().await.seen(root)
    }

    async fn record(&self, record: BlockRecord) -> Result<Vec<String>> {
        let mut state = self.state.lock().await;
        state.insert(record);
        Ok(state.prune(self.max_history))
    }

    async fn resolve_proof(
        &self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Result<Option<ProofResolution>> {
        Ok(self
            .state
            .lock()
            .await
            .resolve_proof(root, proof_type, outcome, failure))
    }

    async fn latest_slot(&self) -> Option<u64> {
        self.state.lock().await.latest_slot()
    }

    async fn inflight_proofs(&self) -> usize {
        self.state.lock().await.inflight_proofs()
    }

    async fn unresolved_records(&self) -> Vec<BlockRecord> {
        self.state.lock().await.unresolved()
    }

    async fn records(&self) -> Vec<BlockRecord> {
        self.state.lock().await.snapshot()
    }
}

/// Current Unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Reads recorded block status from `state_dir/status.json`, sorted by slot.
pub async fn read_records(state_dir: &Path) -> Result<Vec<BlockRecord>> {
    let path = state_dir.join("status.json");
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    let state: State = serde_json::from_slice(&bytes).context("failed to parse status.json")?;
    let mut records: Vec<BlockRecord> = state.records.into_values().collect();
    records.sort_by_key(|record| record.slot);
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(slot: u64, root: &str) -> BlockRecord {
        BlockRecord::new(
            slot,
            "0xbeacon".to_string(),
            slot - 1,
            "0xexechash".to_string(),
            root.to_string(),
            vec!["reth-zisk".to_string()],
            0,
        )
    }

    fn multi_proof_record(slot: u64, root: &str) -> BlockRecord {
        BlockRecord::new(
            slot,
            "0xbeacon".to_string(),
            slot - 1,
            "0xexechash".to_string(),
            root.to_string(),
            vec!["reth-zisk".to_string(), "ethrex-sp1".to_string()],
            0,
        )
    }

    #[tokio::test]
    async fn records_dedup_and_latest_slot() {
        let dir = std::env::temp_dir().join("proofessoor_status_test_dedup");
        let _ = tokio::fs::remove_dir_all(&dir).await;

        let store = JsonStatusStore::load(&dir, 0).await.expect("load");
        assert!(!store.seen("0xa").await);
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(105, "0xb")).await.expect("record");

        assert!(store.seen("0xa").await);
        assert!(!store.seen("0xc").await);
        assert_eq!(store.latest_slot().await, Some(105));

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn persists_and_reloads_across_restart() {
        let dir = std::env::temp_dir().join("proofessoor_status_test_reload");
        let _ = tokio::fs::remove_dir_all(&dir).await;

        let store = JsonStatusStore::load(&dir, 0).await.expect("load");
        store.record(record(200, "0xroot")).await.expect("record");
        store
            .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("resolve proof");

        // Reload as if after a restart.
        let reloaded = JsonStatusStore::load(&dir, 0).await.expect("reload");
        assert!(reloaded.seen("0xroot").await);
        assert_eq!(reloaded.latest_slot().await, Some(200));
        let records = reloaded.records().await;
        let reloaded_record = records.first().expect("one record");
        assert_eq!(reloaded_record.outcome(), Outcome::Complete);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn records_failure_reason_and_error() {
        let store = MemoryStatusStore::new(0);
        store.record(record(300, "0xroot")).await.expect("record");
        store
            .resolve_proof(
                "0xroot",
                "reth-zisk",
                Outcome::Failed,
                Some(Failure {
                    stage: FailureStage::Proving,
                    reason: "WitnessTimeout".to_string(),
                    error: "witness fetch exceeded 12s".to_string(),
                }),
            )
            .await
            .expect("resolve proof");

        let records = store.records().await;
        let failed = records.first().expect("one record");
        assert_eq!(failed.outcome(), Outcome::Failed);
        assert_eq!(failed.failure_reason(), Some("WitnessTimeout"));
        let proof = failed.proofs.first().expect("one proof");
        assert_eq!(proof.outcome, Outcome::Failed);
        assert_eq!(proof.stage, Some(FailureStage::Proving));
        assert_eq!(proof.reason.as_deref(), Some("WitnessTimeout"));
        assert_eq!(proof.error.as_deref(), Some("witness fetch exceeded 12s"));
    }

    #[tokio::test]
    async fn resolves_proofs_independently_and_derives_block_outcome() {
        let store = MemoryStatusStore::new(0);
        store
            .record(multi_proof_record(400, "0xroot"))
            .await
            .expect("record");

        // First proof completes: the other is still in flight, so the block is too.
        let resolution = store
            .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("resolve proof")
            .expect("transitioned");
        assert_eq!(resolution.block_outcome, Outcome::Sent);
        assert!(!resolution.block_resolved);

        // Second proof fails: worst-of makes the block failed and fully resolved.
        let resolution = store
            .resolve_proof(
                "0xroot",
                "ethrex-sp1",
                Outcome::Failed,
                Some(Failure {
                    stage: FailureStage::Proving,
                    reason: "ProvingError".to_string(),
                    error: "boom".to_string(),
                }),
            )
            .await
            .expect("resolve proof")
            .expect("transitioned");
        assert_eq!(resolution.block_outcome, Outcome::Failed);
        assert!(resolution.block_resolved);

        let records = store.records().await;
        let block = records.first().expect("one record");
        assert_eq!(block.outcome(), Outcome::Failed);
        assert_eq!(block.failure_reason(), Some("ProvingError"));
    }

    #[tokio::test]
    async fn resolve_proof_ignores_duplicates_and_unknown_proofs() {
        let store = MemoryStatusStore::new(0);
        store.record(record(500, "0xroot")).await.expect("record");

        // Unknown root and unrequested proof type transition nothing.
        assert!(
            store
                .resolve_proof("0xother", "reth-zisk", Outcome::Complete, None)
                .await
                .expect("resolve proof")
                .is_none()
        );
        assert!(
            store
                .resolve_proof("0xroot", "ethrex-sp1", Outcome::Complete, None)
                .await
                .expect("resolve proof")
                .is_none()
        );

        // First terminal event transitions; a duplicate is a no-op.
        assert!(
            store
                .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
                .await
                .expect("resolve proof")
                .is_some()
        );
        assert!(
            store
                .resolve_proof("0xroot", "reth-zisk", Outcome::Failed, None)
                .await
                .expect("resolve proof")
                .is_none()
        );

        let records = store.records().await;
        assert_eq!(
            records.first().expect("one record").outcome(),
            Outcome::Complete
        );
    }

    #[tokio::test]
    async fn prunes_oldest_beyond_max_history() {
        let store = MemoryStatusStore::new(2);
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        store.record(record(102, "0xc")).await.expect("record");

        // The oldest (slot 100) is evicted; the two newest remain.
        assert!(!store.seen("0xa").await);
        assert!(store.seen("0xb").await);
        assert!(store.seen("0xc").await);
        assert_eq!(store.latest_slot().await, Some(102));
    }

    #[tokio::test]
    async fn record_returns_the_evicted_roots() {
        let store = MemoryStatusStore::new(1);
        assert!(
            store
                .record(record(100, "0xa"))
                .await
                .expect("record")
                .is_empty()
        );

        // Inserting a newer record over the cap evicts the older one and
        // reports it, so the caller can release its span handle.
        let evicted = store.record(record(101, "0xb")).await.expect("record");
        assert_eq!(evicted, vec!["0xa".to_string()]);
    }

    #[tokio::test]
    async fn inflight_proofs_counts_only_unresolved_proofs() {
        let store = MemoryStatusStore::new(0);
        assert_eq!(store.inflight_proofs().await, 0);

        store
            .record(multi_proof_record(100, "0xa"))
            .await
            .expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        assert_eq!(store.inflight_proofs().await, 3);

        store
            .resolve_proof("0xa", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("resolve proof");
        assert_eq!(store.inflight_proofs().await, 2);

        // Eviction drops the evicted record's unresolved proof from the count.
        let store = MemoryStatusStore::new(1);
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        assert_eq!(store.inflight_proofs().await, 1);
    }

    #[test]
    fn current_shape_round_trips() {
        let mut original = record(600, "0xroot");
        original.trace_id = Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string());
        let json = serde_json::to_string(&original).expect("serializes");
        let parsed: BlockRecord = serde_json::from_str(&json).expect("parses");
        assert_eq!(parsed.slot, 600);
        assert_eq!(parsed.trace_id, original.trace_id);
        assert_eq!(parsed.proofs.len(), 1);
        assert_eq!(parsed.outcome(), Outcome::Sent);
    }
}
