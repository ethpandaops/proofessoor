//! Request-status model and its persistent store.
//!
//! The [`StatusStore`] trait is a narrow, swappable interface: stream mode
//! records each proof request and its outcome, deduplicates already-requested
//! roots across restarts, and exposes the latest processed slot. The default
//! [`SqliteStatusStore`] persists configured state transactionally, while
//! [`MemoryStatusStore`] keeps request-only runs free of persistence.
//!
//! A [`BlockRecord`] holds the block-level facts and one [`ProofRecord`] per
//! requested proof type; the block outcome is derived, worst-of, across its
//! proofs. State files written by earlier releases (v0.2.x and below) used a
//! flat, single-outcome record shape and are not readable — delete the state
//! directory when upgrading across that boundary.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use ::metrics::counter;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::metrics::STORE_EVICTIONS;

mod sqlite;

pub use sqlite::SqliteStatusStore;

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
    /// Current zkBoost proof events do not expose this value. If the event
    /// schema adds it, event recording must populate this field.
    #[serde(default)]
    pub queue_ms: Option<u64>,
    /// Pure proving time inside zkBoost, excluding queueing. Current zkBoost
    /// proof events do not expose this value. If the event schema adds it,
    /// event recording must populate this field.
    #[serde(default)]
    pub prove_ms: Option<u64>,
    /// 1-based attempt number. Stream mode records attempt 1 because it does not
    /// retry submissions. A retry path must increment this value for each
    /// resubmission while preserving the terminal-outcome invariant.
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
    /// Witness-generation time reported by zkBoost. Current proof events do not
    /// expose this value. If the event schema adds it, event recording must
    /// populate this field.
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

    /// Whether at least one requested proof still needs a terminal outcome.
    fn has_outstanding_proofs(&self) -> bool {
        self.proofs
            .iter()
            .any(|proof| proof.outcome == Outcome::Sent)
    }

    /// When the request was submitted: the earliest submission across proofs.
    /// All proof types for a block are submitted in one request.
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
    /// Beacon slot of the resolved proof's block, for resolving the chain
    /// config that post-completion actions (verification) must carry.
    pub slot: u64,
    /// The derived block outcome after this transition.
    pub block_outcome: Outcome,
    /// Whether every proof on the block has now resolved.
    pub block_resolved: bool,
}

/// What [`StatusStore::resolve_proof`] did — or why it did nothing.
///
/// The no-op cases are deliberately distinguished: an event for a root or
/// proof type that was never recorded is routine noise (other requestors
/// share the event stream), while an event for an *already-resolved* proof
/// means an outcome arrived after the single-transition rule locked the
/// record — most importantly the truth arriving after reconciliation wrote a
/// false verdict. Callers must be able to see the difference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// The proof transitioned from `Sent` to the requested terminal outcome.
    Transitioned(ProofResolution),
    /// The proof already carries the given terminal outcome; nothing changed.
    AlreadyResolved(Outcome),
    /// The root is not recorded, or that proof type was not requested.
    Unknown,
}

/// Stable boundary for fetching the next page of request records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordCursor {
    /// Slot of the final record on the previous page.
    pub slot: u64,
    /// Request root disambiguating records that share a slot.
    pub request_root: String,
}

/// Validated block-outcome filter for dashboard pages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordFilter {
    #[default]
    All,
    Sent,
    Failed,
}

impl RecordFilter {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Sent => "sent",
            Self::Failed => "failed",
        }
    }

    fn matches(self, record: &BlockRecord) -> bool {
        match self {
            Self::All => true,
            Self::Sent => record.outcome() == Outcome::Sent,
            Self::Failed => record.outcome() == Outcome::Failed,
        }
    }
}

/// One bounded page of request records.
#[derive(Debug, Clone)]
pub struct RecordPage {
    /// Records in descending `(slot, request_root)` order.
    pub records: Vec<BlockRecord>,
    /// Boundary for the next older page, when more records exist.
    pub next_cursor: Option<RecordCursor>,
}

/// Aggregate request outcomes across all retained records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusSummary {
    pub total: usize,
    pub sent: usize,
    pub complete: usize,
    pub failed: usize,
    pub latest_slot: Option<u64>,
}

/// Observable size of a persistent status backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageStats {
    pub records: usize,
    pub bytes: u64,
}

/// A request removed to enforce the configured retention cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionEviction {
    /// Protocol identity of the removed request.
    pub request_root: String,
    /// Slot used for deterministic oldest-first selection and diagnostics.
    pub slot: u64,
    /// Whether removing the request abandoned outstanding work.
    ///
    /// This means at least one proof remained `sent`. Any durable post-proof
    /// workflow must include its pending actions in this classification so
    /// retention cannot silently abandon them.
    pub outstanding: bool,
}

/// How recording a request changed the durable proof set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// The request root and all of its proof rows were new.
    Inserted,
    /// The request root existed and at least one new proof type was appended.
    Extended,
    /// Every proof type was already recorded; terminal state was preserved.
    Duplicate,
}

/// Result of recording a request and enforcing the history cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordWrite {
    pub outcome: RecordOutcome,
    pub evicted: Vec<RetentionEviction>,
}

impl RetentionEviction {
    fn kind(&self) -> &'static str {
        if self.outstanding {
            "outstanding"
        } else {
            "settled"
        }
    }
}

#[cfg(test)]
impl ResolveOutcome {
    /// The resolution when a transition actually happened, `None` otherwise.
    /// Test-only sugar: production callers match on the variants, because
    /// each no-op case needs its own handling.
    pub fn transitioned(self) -> Option<ProofResolution> {
        match self {
            Self::Transitioned(resolution) => Some(resolution),
            Self::AlreadyResolved(_) | Self::Unknown => None,
        }
    }
}

/// A narrow, swappable interface for persisting request status.
#[async_trait]
pub trait StatusStore: Send + Sync {
    /// Whether a request for this `new_payload_request_root` is already recorded.
    async fn seen(&self, root: &str) -> Result<bool>;

    /// Records a request without replacing existing proof state.
    ///
    /// Existing proof keys are idempotent no-ops; new proof types are appended.
    /// Conflicting execution metadata for the same request root is rejected.
    /// The result also reports records evicted to honor the history cap, so the
    /// caller can release per-request resources and refresh state-derived
    /// gauges.
    async fn record(&self, record: BlockRecord) -> Result<RecordWrite>;

    /// Resolves one proof of a recorded request to a terminal outcome.
    ///
    /// Only a proof still marked sent transitions, keeping this idempotent.
    /// The no-op cases are reported distinctly (see [`ResolveOutcome`]): an
    /// unknown root or proof type is routine, while an already-resolved proof
    /// means a late event was discarded and the caller should surface it.
    async fn resolve_proof(
        &self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Result<ResolveOutcome>;

    /// The highest slot recorded so far, if any.
    async fn latest_slot(&self) -> Result<Option<u64>>;

    /// Number of proofs currently unresolved (submitted, outcome unknown).
    ///
    /// The inflight gauge is set absolutely from this count rather than
    /// maintained as event deltas, so it survives restarts and history
    /// eviction without drifting.
    async fn inflight_proofs(&self) -> Result<usize>;

    /// Records with at least one unresolved proof, newest slot first.
    ///
    /// Lives on the trait so the reconciliation sweep never forces a backend
    /// to materialize the whole history just to filter it.
    async fn unresolved_records(&self) -> Result<Vec<BlockRecord>>;

    /// All recorded requests, newest slot first.
    async fn records(&self) -> Result<Vec<BlockRecord>>;

    /// A bounded page of recorded requests, newest first.
    async fn records_page(
        &self,
        cursor: Option<&RecordCursor>,
        filter: RecordFilter,
        limit: usize,
    ) -> Result<RecordPage>;

    /// Aggregate block outcomes across all retained requests.
    async fn summary(&self) -> Result<StatusSummary>;

    /// Persistent store size, or `None` for the in-memory backend.
    async fn storage_stats(&self) -> Result<Option<StorageStats>>;
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

    fn insert(&mut self, mut record: BlockRecord) -> Result<RecordOutcome> {
        validate_record(&record)?;
        let root = record.new_payload_request_root.clone();
        let Some(existing) = self.records.get_mut(&root) else {
            self.records.insert(root, record);
            return Ok(RecordOutcome::Inserted);
        };
        ensure_compatible_request(existing, &record)?;
        let requested_at_ms = existing
            .requested_at_ms()
            .context("stored request has no proof submission timestamp")?;

        let mut extended = false;
        for mut proof in record.proofs.drain(..) {
            if existing
                .proofs
                .iter()
                .any(|stored| stored.proof_type == proof.proof_type)
            {
                continue;
            }
            proof.requested_at_ms = requested_at_ms;
            existing.proofs.push(proof);
            extended = true;
        }
        Ok(if extended {
            RecordOutcome::Extended
        } else {
            RecordOutcome::Duplicate
        })
    }

    fn resolve_proof(
        &mut self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> ResolveOutcome {
        let Some(record) = self.records.get_mut(root) else {
            return ResolveOutcome::Unknown;
        };
        let Some(proof) = record
            .proofs
            .iter_mut()
            .find(|proof| proof.proof_type == proof_type)
        else {
            return ResolveOutcome::Unknown;
        };
        // Only a still-sent proof can transition; a repeated terminal event
        // (e.g. a reconciliation racing the live stream) is a no-op, but the
        // prior outcome is reported so the caller can tell a benign duplicate
        // from an outcome arriving after the record was already judged.
        if proof.outcome != Outcome::Sent {
            return ResolveOutcome::AlreadyResolved(proof.outcome);
        }
        let now = now_ms();
        proof.outcome = outcome;
        if let Some(failure) = failure {
            proof.stage = Some(failure.stage);
            proof.reason = Some(failure.reason);
            proof.error = Some(failure.error);
        }
        proof.resolved_at_ms = Some(now);
        let duration_ms = now.saturating_sub(proof.requested_at_ms);
        ResolveOutcome::Transitioned(ProofResolution {
            duration_ms,
            slot: record.slot,
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
        sort_records(&mut records);
        records
    }

    /// Records with at least one unresolved proof, newest slot first.
    fn unresolved(&self) -> Vec<BlockRecord> {
        let mut records: Vec<BlockRecord> = self
            .records
            .values()
            .filter(|record| record.has_outstanding_proofs())
            .cloned()
            .collect();
        sort_records(&mut records);
        records
    }

    /// Evicts settled history first, oldest slot/root first within each class,
    /// until at most `max_history` records remain (`0` means unlimited).
    /// Outstanding work is evicted only when it alone exceeds the hard cap.
    fn prune(&mut self, max_history: usize) -> Vec<RetentionEviction> {
        let mut evicted = Vec::new();
        if max_history == 0 {
            return evicted;
        }
        while self.records.len() > max_history {
            let oldest = self
                .records
                .iter()
                .min_by(|(left_root, left), (right_root, right)| {
                    let left_outstanding = left.has_outstanding_proofs();
                    let right_outstanding = right.has_outstanding_proofs();
                    left_outstanding
                        .cmp(&right_outstanding)
                        .then_with(|| left.slot.cmp(&right.slot))
                        .then_with(|| left_root.cmp(right_root))
                })
                .map(|(key, record)| RetentionEviction {
                    request_root: key.clone(),
                    slot: record.slot,
                    outstanding: record.has_outstanding_proofs(),
                });
            match oldest {
                Some(eviction) => {
                    self.records.remove(&eviction.request_root);
                    evicted.push(eviction);
                }
                None => break,
            }
        }
        evicted
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
    async fn seen(&self, root: &str) -> Result<bool> {
        Ok(self.state.lock().await.seen(root))
    }

    async fn record(&self, record: BlockRecord) -> Result<RecordWrite> {
        let (outcome, evicted) = {
            let mut state = self.state.lock().await;
            let outcome = state.insert(record)?;
            (outcome, state.prune(self.max_history))
        };
        observe_retention_evictions(&evicted);
        Ok(RecordWrite { outcome, evicted })
    }

    async fn resolve_proof(
        &self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Result<ResolveOutcome> {
        Ok(self
            .state
            .lock()
            .await
            .resolve_proof(root, proof_type, outcome, failure))
    }

    async fn latest_slot(&self) -> Result<Option<u64>> {
        Ok(self.state.lock().await.latest_slot())
    }

    async fn inflight_proofs(&self) -> Result<usize> {
        Ok(self.state.lock().await.inflight_proofs())
    }

    async fn unresolved_records(&self) -> Result<Vec<BlockRecord>> {
        Ok(self.state.lock().await.unresolved())
    }

    async fn records(&self) -> Result<Vec<BlockRecord>> {
        Ok(self.state.lock().await.snapshot())
    }

    async fn records_page(
        &self,
        cursor: Option<&RecordCursor>,
        filter: RecordFilter,
        limit: usize,
    ) -> Result<RecordPage> {
        let mut records = self.state.lock().await.snapshot();
        records.retain(|record| filter.matches(record));
        if let Some(cursor) = cursor {
            records.retain(|record| record_is_before(record, cursor));
        }
        Ok(finish_page(records, limit))
    }

    async fn summary(&self) -> Result<StatusSummary> {
        let state = self.state.lock().await;
        let mut summary = StatusSummary {
            total: state.records.len(),
            sent: 0,
            complete: 0,
            failed: 0,
            latest_slot: state.latest_slot(),
        };
        for record in state.records.values() {
            match record.outcome() {
                Outcome::Sent => summary.sent += 1,
                Outcome::Complete => summary.complete += 1,
                Outcome::Failed => summary.failed += 1,
            }
        }
        Ok(summary)
    }

    async fn storage_stats(&self) -> Result<Option<StorageStats>> {
        Ok(None)
    }
}

fn validate_record(record: &BlockRecord) -> Result<()> {
    if record.proofs.is_empty() {
        bail!("cannot record a request without any proof types");
    }
    let mut proof_types = HashSet::with_capacity(record.proofs.len());
    for proof in &record.proofs {
        if !proof_types.insert(proof.proof_type.as_str()) {
            bail!(
                "request {} contains duplicate proof type {}",
                record.new_payload_request_root,
                proof.proof_type
            );
        }
    }
    Ok(())
}

fn ensure_compatible_request(existing: &BlockRecord, incoming: &BlockRecord) -> Result<()> {
    if existing.execution_block_number != incoming.execution_block_number
        || existing.execution_block_hash != incoming.execution_block_hash
    {
        bail!(
            "request root {} conflicts with stored execution payload {} ({})",
            incoming.new_payload_request_root,
            existing.execution_block_hash,
            existing.execution_block_number
        );
    }
    Ok(())
}

fn sort_records(records: &mut [BlockRecord]) {
    records.sort_by(|left, right| {
        right.slot.cmp(&left.slot).then_with(|| {
            right
                .new_payload_request_root
                .cmp(&left.new_payload_request_root)
        })
    });
}

fn record_is_before(record: &BlockRecord, cursor: &RecordCursor) -> bool {
    record.slot < cursor.slot
        || (record.slot == cursor.slot
            && record.new_payload_request_root.as_str() < cursor.request_root.as_str())
}

fn finish_page(mut records: Vec<BlockRecord>, limit: usize) -> RecordPage {
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_cursor = if has_more {
        records.last().map(|record| RecordCursor {
            slot: record.slot,
            request_root: record.new_payload_request_root.clone(),
        })
    } else {
        None
    };
    RecordPage {
        next_cursor,
        records,
    }
}

/// Emits retention observability only after the state change has committed.
fn observe_retention_evictions(evictions: &[RetentionEviction]) {
    for eviction in evictions {
        counter!(STORE_EVICTIONS, "kind" => eviction.kind()).increment(1);
        if eviction.outstanding {
            warn!(
                request_root = %eviction.request_root,
                slot = eviction.slot,
                "retention cap evicted outstanding request work"
            );
        } else {
            debug!(
                request_root = %eviction.request_root,
                slot = eviction.slot,
                "retention cap evicted settled request history"
            );
        }
    }
}

/// Current Unix time in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Reads recorded block status without creating, migrating, or importing state.
///
/// An existing SQLite database is opened read-only. If a legacy JSON file is
/// present before the service performs its one-time import, it is decoded
/// directly and left untouched. A missing or mistyped state directory fails
/// loudly instead of being mistaken for empty history.
pub async fn read_records(state_dir: &Path) -> Result<Vec<BlockRecord>> {
    let database_path = state_dir.join(sqlite::DATABASE_FILE);
    match tokio::fs::metadata(&database_path).await {
        Ok(metadata) if metadata.is_file() => {
            let store = SqliteStatusStore::open_read_only(state_dir).await?;
            let mut records = store.records().await?;
            records.reverse();
            Ok(records)
        }
        Ok(_) => bail!(
            "status database path is not a file: {}",
            database_path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            read_legacy_records(state_dir).await
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to inspect {}", database_path.display()))
        }
    }
}

async fn read_legacy_records(state_dir: &Path) -> Result<Vec<BlockRecord>> {
    let legacy_path = state_dir.join(sqlite::LEGACY_JSON_FILE);
    let bytes = match tokio::fs::read(&legacy_path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "no status database or legacy status file found in {}",
                state_dir.display()
            )
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", legacy_path.display()));
        }
    };
    let state: State = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", legacy_path.display()))?;
    let mut records = state.snapshot();
    records.reverse();
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

    fn temp_state_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "proofessoor-{name}-{}-{}",
            std::process::id(),
            NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }

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
        let dir = temp_state_dir("status-dedup");

        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        assert!(!store.seen("0xa").await.expect("check root"));
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(105, "0xb")).await.expect("record");

        assert!(store.seen("0xa").await.expect("check root"));
        assert!(!store.seen("0xc").await.expect("check root"));
        assert_eq!(store.latest_slot().await.expect("latest slot"), Some(105));

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn memory_record_is_idempotent_and_appends_new_proof_types() {
        let store = MemoryStatusStore::new(0);
        let inserted = store
            .record(record(150, "0xroot"))
            .await
            .expect("insert request");
        assert_eq!(inserted.outcome, RecordOutcome::Inserted);
        store
            .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("complete first proof");

        let mut extension = multi_proof_record(150, "0xroot");
        extension
            .proofs
            .iter_mut()
            .find(|proof| proof.proof_type == "ethrex-sp1")
            .expect("new proof type")
            .requested_at_ms = u64::MAX;
        let extended = store.record(extension).await.expect("extend request");
        assert_eq!(extended.outcome, RecordOutcome::Extended);
        let duplicate = store
            .record(multi_proof_record(150, "0xroot"))
            .await
            .expect("repeat request");
        assert_eq!(duplicate.outcome, RecordOutcome::Duplicate);

        let records = store.records().await.expect("read request");
        let stored = records.first().expect("one request");
        let mut proofs = stored.proofs.iter();
        let first = proofs.next().expect("first proof");
        let second = proofs.next().expect("second proof");
        assert!(proofs.next().is_none(), "expected exactly two proofs");
        assert_eq!(first.proof_type, "reth-zisk");
        assert_eq!(first.outcome, Outcome::Complete);
        assert_eq!(second.proof_type, "ethrex-sp1");
        assert_eq!(second.outcome, Outcome::Sent);
        assert_eq!(second.requested_at_ms, first.requested_at_ms);
    }

    #[tokio::test]
    async fn sqlite_record_preserves_terminal_state_and_proof_order() {
        let dir = temp_state_dir("sqlite-idempotent-record");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        let inserted = store
            .record(record(160, "0xroot"))
            .await
            .expect("insert request");
        assert_eq!(inserted.outcome, RecordOutcome::Inserted);
        store
            .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("complete first proof");

        let extended = store
            .record(multi_proof_record(160, "0xroot"))
            .await
            .expect("extend request");
        assert_eq!(extended.outcome, RecordOutcome::Extended);
        let duplicate = store
            .record(multi_proof_record(160, "0xroot"))
            .await
            .expect("repeat request");
        assert_eq!(duplicate.outcome, RecordOutcome::Duplicate);

        let records = store.records().await.expect("read request");
        let stored = records.first().expect("one request");
        let mut proofs = stored.proofs.iter();
        let first = proofs.next().expect("first proof");
        let second = proofs.next().expect("second proof");
        assert!(proofs.next().is_none(), "expected exactly two proofs");
        assert_eq!(first.proof_type, "reth-zisk");
        assert_eq!(first.outcome, Outcome::Complete);
        assert_eq!(second.proof_type, "ethrex-sp1");
        assert_eq!(second.outcome, Outcome::Sent);

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_record_rejects_conflicting_payload_metadata() {
        let dir = temp_state_dir("sqlite-conflicting-record");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        store
            .record(record(170, "0xroot"))
            .await
            .expect("insert request");
        let mut conflicting = record(170, "0xroot");
        conflicting.execution_block_hash = "0xdifferent".to_string();

        let error = store
            .record(conflicting)
            .await
            .expect_err("conflicting payload must fail");
        assert!(
            error
                .to_string()
                .contains("conflicts with stored execution payload")
        );
        let records = store.records().await.expect("read request");
        assert_eq!(records.len(), 1);
        let stored = records.first().expect("one request");
        assert_eq!(stored.execution_block_hash, "0xexechash");
        assert_eq!(stored.proofs.len(), 1);

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_record_rejects_duplicate_proof_types_atomically() {
        let dir = temp_state_dir("sqlite-duplicate-proof-types");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        let duplicate = BlockRecord::new(
            180,
            "0xbeacon".to_string(),
            179,
            "0xexechash".to_string(),
            "0xroot".to_string(),
            vec!["reth-zisk".to_string(), "reth-zisk".to_string()],
            0,
        );

        let error = store
            .record(duplicate)
            .await
            .expect_err("duplicate proof types must fail");
        assert!(error.to_string().contains("duplicate proof type"));
        assert!(!store.seen("0xroot").await.expect("check request"));

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn persists_and_reloads_across_restart() {
        let dir = temp_state_dir("status-reload");

        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        store.record(record(200, "0xroot")).await.expect("record");
        store
            .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("resolve proof");
        drop(store);

        // Reload as if after a restart.
        let reloaded = SqliteStatusStore::open(&dir, 0).await.expect("reload");
        assert!(reloaded.seen("0xroot").await.expect("check root"));
        assert_eq!(
            reloaded.latest_slot().await.expect("latest slot"),
            Some(200)
        );
        let records = reloaded.records().await.expect("read records");
        let reloaded_record = records.first().expect("one record");
        assert_eq!(reloaded_record.outcome(), Outcome::Complete);

        drop(reloaded);
        let inspected = read_records(&dir).await.expect("inspect read-only status");
        assert_eq!(inspected.len(), 1);
        assert_eq!(
            inspected
                .first()
                .expect("one inspected request")
                .new_payload_request_root,
            "0xroot"
        );
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn status_read_missing_state_fails_without_creating_it() {
        let dir = temp_state_dir("missing-read-only-status");

        let error = read_records(&dir)
            .await
            .expect_err("missing status must fail");

        assert!(error.to_string().contains("no status database"));
        assert!(!dir.exists(), "status inspection must not create state");
    }

    #[tokio::test]
    async fn status_read_legacy_json_does_not_consume_the_import() {
        let dir = temp_state_dir("read-only-legacy-status");
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create state dir");
        let mut legacy = State::default();
        legacy
            .insert(record(205, "0xlegacy"))
            .expect("insert legacy record");
        tokio::fs::write(
            dir.join(sqlite::LEGACY_JSON_FILE),
            serde_json::to_vec_pretty(&legacy).expect("serialize legacy state"),
        )
        .await
        .expect("write legacy state");

        let inspected = read_records(&dir).await.expect("inspect legacy status");
        assert_eq!(inspected.len(), 1);
        assert_eq!(
            inspected
                .first()
                .expect("one inspected legacy request")
                .new_payload_request_root,
            "0xlegacy"
        );
        assert!(
            !dir.join(sqlite::DATABASE_FILE).exists(),
            "status inspection must not create the SQLite database"
        );

        let store = SqliteStatusStore::open(&dir, 0)
            .await
            .expect("service imports legacy state later");
        assert!(
            store
                .seen("0xlegacy")
                .await
                .expect("check imported request")
        );

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn status_read_does_not_migrate_an_existing_database() {
        let dir = temp_state_dir("read-only-unmigrated-status");
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create state dir");
        let path = dir.join(sqlite::DATABASE_FILE);
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let creator = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("create unmigrated database");
        creator.close().await;

        read_records(&dir)
            .await
            .expect_err("unmigrated database must fail inspection");

        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .read_only(true);
        let inspector = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("inspect schema");
        let migration_tables: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = '_sqlx_migrations'",
        )
        .fetch_one(&inspector)
        .await
        .expect("inspect migration table");
        assert_eq!(migration_tables, 0, "status inspection must not migrate");
        inspector.close().await;

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn imports_legacy_json_once() {
        let dir = temp_state_dir("legacy-import");
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create state dir");
        let mut legacy = State::default();
        legacy
            .insert(record(210, "0xlegacy"))
            .expect("insert legacy record");
        tokio::fs::write(
            dir.join("status.json"),
            serde_json::to_vec_pretty(&legacy).expect("serialize legacy state"),
        )
        .await
        .expect("write legacy state");

        let store = SqliteStatusStore::open(&dir, 0).await.expect("import");
        assert!(store.seen("0xlegacy").await.expect("check imported root"));
        drop(store);

        // The marker, rather than the presence of request rows, controls the
        // one-time import. Later changes to the retained legacy file are ignored.
        let mut changed_legacy = State::default();
        changed_legacy
            .insert(record(211, "0xlate"))
            .expect("insert changed legacy record");
        tokio::fs::write(
            dir.join("status.json"),
            serde_json::to_vec_pretty(&changed_legacy).expect("serialize changed legacy state"),
        )
        .await
        .expect("rewrite legacy state");
        let reopened = SqliteStatusStore::open(&dir, 0).await.expect("reopen");
        assert!(
            reopened
                .seen("0xlegacy")
                .await
                .expect("check imported root")
        );
        assert!(!reopened.seen("0xlate").await.expect("check late root"));

        drop(reopened);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn empty_database_records_the_legacy_import_marker() {
        let dir = temp_state_dir("empty-import-marker");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        drop(store);

        let mut late_legacy = State::default();
        late_legacy
            .insert(record(220, "0xlate"))
            .expect("insert late legacy record");
        tokio::fs::write(
            dir.join("status.json"),
            serde_json::to_vec_pretty(&late_legacy).expect("serialize late legacy state"),
        )
        .await
        .expect("write late legacy state");
        let reopened = SqliteStatusStore::open(&dir, 0).await.expect("reopen");
        assert!(!reopened.seen("0xlate").await.expect("check late root"));

        drop(reopened);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_retention_hard_cap_evicts_oldest_outstanding_request() {
        let dir = temp_state_dir("sqlite-retention");
        let store = SqliteStatusStore::open(&dir, 2).await.expect("open");
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        let write = store.record(record(102, "0xc")).await.expect("record");

        assert_eq!(
            write.evicted,
            vec![RetentionEviction {
                request_root: "0xa".to_string(),
                slot: 100,
                outstanding: true,
            }]
        );
        assert!(!store.seen("0xa").await.expect("check old root"));
        assert!(store.seen("0xb").await.expect("check retained root"));
        assert!(store.seen("0xc").await.expect("check retained root"));

        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(store.path())
            .read_only(true);
        let reader = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("open read-only inspection connection");
        let orphaned_proofs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM proofs WHERE request_root = '0xa'")
                .fetch_one(&reader)
                .await
                .expect("count evicted request proofs");
        assert_eq!(orphaned_proofs, 0, "request pruning must cascade to proofs");
        reader.close().await;

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_retention_prefers_settled_history_over_outstanding_work() {
        let dir = temp_state_dir("sqlite-settled-first-retention");
        let store = SqliteStatusStore::open(&dir, 2).await.expect("open");
        store
            .record(record(100, "0xold-outstanding"))
            .await
            .expect("record old outstanding request");
        store
            .record(record(101, "0xsettled"))
            .await
            .expect("record settled candidate");
        store
            .resolve_proof("0xsettled", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("settle request");

        let write = store
            .record(record(102, "0xnew-outstanding"))
            .await
            .expect("record new outstanding request");

        assert_eq!(
            write.evicted,
            vec![RetentionEviction {
                request_root: "0xsettled".to_string(),
                slot: 101,
                outstanding: false,
            }]
        );
        assert!(
            store
                .seen("0xold-outstanding")
                .await
                .expect("check old outstanding request")
        );
        assert!(
            !store
                .seen("0xsettled")
                .await
                .expect("check settled request")
        );
        assert!(
            store
                .seen("0xnew-outstanding")
                .await
                .expect("check new outstanding request")
        );

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_cursor_pages_stay_stable_as_new_slots_arrive() {
        let dir = temp_state_dir("sqlite-pagination");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        for slot in 101..=105 {
            store
                .record(record(slot, format!("0x{slot}").as_str()))
                .await
                .expect("record");
        }

        let first = store
            .records_page(None, RecordFilter::All, 2)
            .await
            .expect("first page");
        assert_eq!(
            first
                .records
                .iter()
                .map(|record| record.slot)
                .collect::<Vec<_>>(),
            vec![105, 104]
        );
        let cursor = first.next_cursor.expect("next cursor");

        // A new head does not shift the boundary for the older second page.
        store.record(record(106, "0x106")).await.expect("record");
        let second = store
            .records_page(Some(&cursor), RecordFilter::All, 2)
            .await
            .expect("second page");
        assert_eq!(
            second
                .records
                .iter()
                .map(|record| record.slot)
                .collect::<Vec<_>>(),
            vec![103, 102]
        );

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_cursor_disambiguates_requests_at_the_same_slot() {
        let dir = temp_state_dir("sqlite-same-slot-pagination");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        for root in ["0xa", "0xc", "0xb"] {
            store.record(record(200, root)).await.expect("record");
        }

        let first = store
            .records_page(None, RecordFilter::All, 2)
            .await
            .expect("first page");
        assert_eq!(
            first
                .records
                .iter()
                .map(|record| record.new_payload_request_root.as_str())
                .collect::<Vec<_>>(),
            vec!["0xc", "0xb"]
        );
        let second = store
            .records_page(first.next_cursor.as_ref(), RecordFilter::All, 2)
            .await
            .expect("second page");
        assert_eq!(
            second
                .records
                .iter()
                .map(|record| record.new_payload_request_root.as_str())
                .collect::<Vec<_>>(),
            vec!["0xa"]
        );

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_concurrent_terminal_events_preserve_first_terminal_wins() {
        let dir = temp_state_dir("sqlite-concurrent-resolution");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        store.record(record(250, "0xroot")).await.expect("record");

        let complete = store.resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None);
        let failed = store.resolve_proof(
            "0xroot",
            "reth-zisk",
            Outcome::Failed,
            Some(Failure {
                stage: FailureStage::Proving,
                reason: "ProvingError".to_string(),
                error: "proof failed".to_string(),
            }),
        );
        let (complete, failed) = tokio::join!(complete, failed);
        let outcomes = (
            complete.expect("complete event"),
            failed.expect("failure event"),
        );
        assert!(
            matches!(
                &outcomes,
                (
                    ResolveOutcome::Transitioned(_),
                    ResolveOutcome::AlreadyResolved(Outcome::Complete),
                ) | (
                    ResolveOutcome::AlreadyResolved(Outcome::Failed),
                    ResolveOutcome::Transitioned(_),
                )
            ),
            "unexpected concurrent resolution outcomes: {outcomes:?}"
        );

        drop(store);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn sqlite_page_limits_requests_not_joined_proof_rows() {
        let dir = temp_state_dir("sqlite-multi-proof-page");
        let store = SqliteStatusStore::open(&dir, 0).await.expect("open");
        store
            .record(multi_proof_record(301, "0xmulti"))
            .await
            .expect("record multi-proof request");
        store
            .record(record(300, "0xsingle"))
            .await
            .expect("record single-proof request");

        let page = store
            .records_page(None, RecordFilter::All, 1)
            .await
            .expect("page");
        assert_eq!(page.records.len(), 1);
        let first = page.records.first().expect("one request");
        assert_eq!(first.new_payload_request_root, "0xmulti");
        assert_eq!(first.proofs.len(), 2);
        assert!(page.next_cursor.is_some());

        let summary = store.summary().await.expect("summary");
        assert_eq!(summary.total, 2);
        assert_eq!(summary.sent, 2);
        assert_eq!(summary.complete, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.latest_slot, Some(301));

        store
            .resolve_proof(
                "0xsingle",
                "reth-zisk",
                Outcome::Failed,
                Some(Failure {
                    stage: FailureStage::Proving,
                    reason: "ProvingError".to_string(),
                    error: "proof failed".to_string(),
                }),
            )
            .await
            .expect("resolve failed proof");
        let failed = store
            .records_page(None, RecordFilter::Failed, 100)
            .await
            .expect("failed page");
        assert_eq!(failed.records.len(), 1);
        assert_eq!(
            failed
                .records
                .first()
                .expect("one failed request")
                .new_payload_request_root,
            "0xsingle"
        );
        let sent = store
            .records_page(None, RecordFilter::Sent, 100)
            .await
            .expect("sent page");
        assert_eq!(sent.records.len(), 1);
        assert_eq!(
            sent.records
                .first()
                .expect("one sent request")
                .new_payload_request_root,
            "0xmulti"
        );
        let summary = store.summary().await.expect("updated summary");
        assert_eq!(summary.sent, 1);
        assert_eq!(summary.failed, 1);
        let stats = store
            .storage_stats()
            .await
            .expect("storage stats")
            .expect("persistent store stats");
        assert_eq!(stats.records, 2);
        assert!(stats.bytes > 0);

        drop(store);
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

        let records = store.records().await.expect("read records");
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
            .transitioned()
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
            .transitioned()
            .expect("transitioned");
        assert_eq!(resolution.block_outcome, Outcome::Failed);
        assert!(resolution.block_resolved);

        let records = store.records().await.expect("read records");
        let block = records.first().expect("one record");
        assert_eq!(block.outcome(), Outcome::Failed);
        assert_eq!(block.failure_reason(), Some("ProvingError"));
    }

    #[tokio::test]
    async fn resolve_proof_ignores_duplicates_and_unknown_proofs() {
        let store = MemoryStatusStore::new(0);
        store.record(record(500, "0xroot")).await.expect("record");

        // Unknown root and unrequested proof type transition nothing, and are
        // reported as unknown (routine noise, not a discarded late event).
        assert_eq!(
            store
                .resolve_proof("0xother", "reth-zisk", Outcome::Complete, None)
                .await
                .expect("resolve proof"),
            ResolveOutcome::Unknown
        );
        assert_eq!(
            store
                .resolve_proof("0xroot", "ethrex-sp1", Outcome::Complete, None)
                .await
                .expect("resolve proof"),
            ResolveOutcome::Unknown
        );

        // First terminal event transitions; a duplicate is a no-op that
        // reports the outcome already recorded.
        assert!(
            store
                .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
                .await
                .expect("resolve proof")
                .transitioned()
                .is_some()
        );
        assert_eq!(
            store
                .resolve_proof("0xroot", "reth-zisk", Outcome::Failed, None)
                .await
                .expect("resolve proof"),
            ResolveOutcome::AlreadyResolved(Outcome::Complete)
        );

        let records = store.records().await.expect("read records");
        assert_eq!(
            records.first().expect("one record").outcome(),
            Outcome::Complete
        );
    }

    #[tokio::test]
    async fn late_completion_reports_the_prior_failed_outcome() {
        let store = MemoryStatusStore::new(0);
        store.record(record(550, "0xroot")).await.expect("record");

        // Reconciliation (or a real failure event) resolves the proof first.
        store
            .resolve_proof(
                "0xroot",
                "reth-zisk",
                Outcome::Failed,
                Some(Failure {
                    stage: FailureStage::Proving,
                    reason: "Unresolved".to_string(),
                    error: "silent past the cutoff".to_string(),
                }),
            )
            .await
            .expect("resolve proof");

        // The truth arriving afterwards is discarded, but the caller learns
        // exactly which verdict it contradicts.
        assert_eq!(
            store
                .resolve_proof("0xroot", "reth-zisk", Outcome::Complete, None)
                .await
                .expect("resolve proof"),
            ResolveOutcome::AlreadyResolved(Outcome::Failed)
        );
        assert_eq!(
            store
                .records()
                .await
                .expect("read records")
                .first()
                .expect("one record")
                .outcome(),
            Outcome::Failed
        );
    }

    #[tokio::test]
    async fn prunes_oldest_beyond_max_history() {
        let store = MemoryStatusStore::new(2);
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        store.record(record(102, "0xc")).await.expect("record");

        // The oldest (slot 100) is evicted; the two newest remain.
        assert!(!store.seen("0xa").await.expect("check root"));
        assert!(store.seen("0xb").await.expect("check root"));
        assert!(store.seen("0xc").await.expect("check root"));
        assert_eq!(store.latest_slot().await.expect("latest slot"), Some(102));
    }

    #[tokio::test]
    async fn record_returns_structured_retention_evictions() {
        let store = MemoryStatusStore::new(1);
        assert!(
            store
                .record(record(100, "0xa"))
                .await
                .expect("record")
                .evicted
                .is_empty()
        );

        // Inserting a newer record over the cap evicts the older one and
        // reports it, so the caller can release its span handle.
        let write = store.record(record(101, "0xb")).await.expect("record");
        assert_eq!(
            write.evicted,
            vec![RetentionEviction {
                request_root: "0xa".to_string(),
                slot: 100,
                outstanding: true,
            }]
        );
    }

    #[tokio::test]
    async fn memory_retention_prefers_settled_history_over_outstanding_work() {
        let store = MemoryStatusStore::new(2);
        store
            .record(multi_proof_record(100, "0xold-outstanding"))
            .await
            .expect("record old outstanding request");
        store
            .resolve_proof(
                "0xold-outstanding",
                "reth-zisk",
                Outcome::Failed,
                Some(Failure {
                    stage: FailureStage::Proving,
                    reason: "ProvingError".to_string(),
                    error: "one proof failed while another remains sent".to_string(),
                }),
            )
            .await
            .expect("partially resolve old outstanding request");
        assert_eq!(
            store
                .records()
                .await
                .expect("read partially resolved request")
                .first()
                .expect("one request")
                .outcome(),
            Outcome::Failed,
            "worst-of block outcome must not hide its remaining sent proof"
        );
        store
            .record(record(101, "0xsettled"))
            .await
            .expect("record settled candidate");
        store
            .resolve_proof("0xsettled", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("settle request");

        let write = store
            .record(record(102, "0xnew-outstanding"))
            .await
            .expect("record new outstanding request");
        assert_eq!(
            write.evicted,
            vec![RetentionEviction {
                request_root: "0xsettled".to_string(),
                slot: 101,
                outstanding: false,
            }]
        );
        assert!(
            store
                .seen("0xold-outstanding")
                .await
                .expect("check old outstanding request")
        );
    }

    #[tokio::test]
    async fn inflight_proofs_counts_only_unresolved_proofs() {
        let store = MemoryStatusStore::new(0);
        assert_eq!(store.inflight_proofs().await.expect("count inflight"), 0);

        store
            .record(multi_proof_record(100, "0xa"))
            .await
            .expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        assert_eq!(store.inflight_proofs().await.expect("count inflight"), 3);

        store
            .resolve_proof("0xa", "reth-zisk", Outcome::Complete, None)
            .await
            .expect("resolve proof");
        assert_eq!(store.inflight_proofs().await.expect("count inflight"), 2);

        // Eviction drops the evicted record's unresolved proof from the count.
        let store = MemoryStatusStore::new(1);
        store.record(record(100, "0xa")).await.expect("record");
        store.record(record(101, "0xb")).await.expect("record");
        assert_eq!(store.inflight_proofs().await.expect("count inflight"), 1);
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
