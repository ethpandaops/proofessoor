//! Prometheus metric names and recorder installation.
//!
//! Metric names live here as constants; the runner and watcher emit them via the
//! `metrics` facade. When no HTTP address is configured the recorder is not
//! installed and the emit calls are cheap no-ops. The HTTP surface that renders
//! these lives in [`crate::web`].

use ::metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge};
use anyhow::{Context, Result};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Total non-optimistic blocks observed.
pub const BLOCKS_OBSERVED: &str = "proofessoor_blocks_observed_total";
/// Total blocks skipped (optimistic or already requested).
pub const BLOCKS_SKIPPED: &str = "proofessoor_blocks_skipped_total";
/// Total proof requests submitted to zkBoost.
pub const PROOF_REQUESTS: &str = "proofessoor_proof_requests_total";
/// Total proof requests that failed to submit.
pub const PROOF_REQUEST_FAILURES: &str = "proofessoor_proof_request_failures_total";
/// Total proofs that completed.
pub const PROOF_COMPLETIONS: &str = "proofessoor_proof_completions_total";
/// Total proofs that failed.
pub const PROOF_FAILURES: &str = "proofessoor_proof_failures_total";
/// Total proofs resolved by reconciliation after a watcher reconnect
/// (labeled by verdict: complete, unresolved).
pub const RECONCILE_ACTIONS: &str = "proofessoor_reconcile_actions_total";
/// Total proof events discarded because the proof had already resolved
/// (labeled by kind: completion, failure). A nonzero rate here means outcomes
/// are arriving after the record was judged — most importantly the truth
/// arriving after reconciliation wrote a false `Unresolved` verdict.
pub const LATE_EVENTS_DISCARDED: &str = "proofessoor_late_events_discarded_total";
/// Currently outstanding proof jobs (submitted, unresolved; one per proof type).
pub const INFLIGHT_REQUESTS: &str = "proofessoor_inflight_requests";
/// Highest slot for which a proof was requested.
pub const LATEST_REQUESTED_SLOT: &str = "proofessoor_latest_requested_slot";
/// Highest slot observed from the beacon event stream.
pub const LATEST_SEEN_SLOT: &str = "proofessoor_latest_seen_slot";
/// Slots between the latest seen block and the latest requested one.
pub const HEAD_LAG: &str = "proofessoor_head_lag_slots";
/// Time spent fetching, building, and submitting a request.
pub const REQUEST_DURATION: &str = "proofessoor_proof_request_duration_seconds";
/// Per-stage time within the request path (labeled by stage: fetch, ssz_decode, build, submit).
pub const REQUEST_STAGE_DURATION: &str = "proofessoor_request_stage_duration_seconds";
/// Time from request submission to proof completion (labeled by proof_type).
pub const COMPLETION_DURATION: &str = "proofessoor_proof_completion_duration_seconds";

/// Histogram buckets in seconds, spanning sub-millisecond CPU work to multi-minute proving.
const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
    120.0, 300.0,
];

/// Installs the Prometheus recorder, returning a handle for rendering `/metrics`.
///
/// Histograms use explicit buckets so they render as Prometheus histograms (aggregatable
/// across instances) rather than client-side summaries.
pub fn install() -> Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)
        .context("failed to configure metric buckets")?
        .install_recorder()
        .context("failed to install the Prometheus recorder")?;
    register();
    Ok(handle)
}

/// Describes every metric and initializes the unlabeled series to zero.
///
/// Describing attaches HELP/TYPE metadata; touching each label-free series makes
/// `/metrics` render meaningful output before the first block is processed,
/// rather than an empty page. Labeled series (by proof_type, reason, stage)
/// appear once emitted, to avoid inventing label values up front.
fn register() {
    describe_counter!(BLOCKS_OBSERVED, "Total non-optimistic blocks observed");
    describe_counter!(
        BLOCKS_SKIPPED,
        "Total blocks skipped (optimistic or already requested)"
    );
    describe_counter!(PROOF_REQUESTS, "Total proof requests submitted to zkBoost");
    describe_counter!(
        PROOF_REQUEST_FAILURES,
        "Total proof requests that failed to submit"
    );
    describe_counter!(PROOF_COMPLETIONS, "Total proofs that completed");
    describe_counter!(PROOF_FAILURES, "Total proofs that failed");
    describe_counter!(
        RECONCILE_ACTIONS,
        "Total proofs resolved by reconciliation after a watcher reconnect"
    );
    describe_counter!(
        LATE_EVENTS_DISCARDED,
        "Total proof events discarded because the proof had already resolved"
    );
    describe_gauge!(INFLIGHT_REQUESTS, "Currently outstanding proof jobs");
    describe_gauge!(LATEST_REQUESTED_SLOT, "Highest slot requested");
    describe_gauge!(LATEST_SEEN_SLOT, "Highest slot seen from the event stream");
    describe_gauge!(
        HEAD_LAG,
        "Slots between the latest seen and requested block"
    );
    describe_histogram!(
        REQUEST_DURATION,
        Unit::Seconds,
        "Time to fetch, build, and submit a request"
    );
    describe_histogram!(
        REQUEST_STAGE_DURATION,
        Unit::Seconds,
        "Per-stage time within the request path"
    );
    describe_histogram!(
        COMPLETION_DURATION,
        Unit::Seconds,
        "Time from submission to proof completion"
    );

    counter!(BLOCKS_OBSERVED).increment(0);
    counter!(BLOCKS_SKIPPED).increment(0);
    counter!(PROOF_REQUESTS).increment(0);
    counter!(PROOF_REQUEST_FAILURES).increment(0);
    gauge!(INFLIGHT_REQUESTS).set(0.0);
    gauge!(LATEST_REQUESTED_SLOT).set(0.0);
    gauge!(LATEST_SEEN_SLOT).set(0.0);
    gauge!(HEAD_LAG).set(0.0);
}
