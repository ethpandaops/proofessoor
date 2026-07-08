//! Stream-mode orchestration.
//!
//! Consumes the Beacon API block event stream and, for each new non-optimistic
//! block, builds and submits a proof request under bounded concurrency, so
//! submission keeps pace with block arrival. A separate watcher task observes
//! zkBoost's proof events, records each proof's outcome in the status registry,
//! and optionally downloads/verifies completed proofs. Each time the watcher
//! (re)establishes its subscription it reconciles proofs still marked sent —
//! and keeps re-sweeping them periodically while connected — since events
//! that fired while disconnected are not redelivered; silence verdicts are
//! gated on observed zkBoost liveness. The daemon stops on SIGINT/SIGTERM.

use std::collections::{HashMap, HashSet};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ::metrics::{counter, gauge, histogram};
use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{Instrument, Span, field, info, info_span, warn};
use zkboost_client::{Hash256, ProofType};

use crate::beacon::{self, BlockEvent};
use crate::config::{BlockId, StreamArgs};
use crate::metrics::{
    BLOCKS_OBSERVED, BLOCKS_SKIPPED, COMPLETION_DURATION, HEAD_LAG, INFLIGHT_REQUESTS,
    LATEST_REQUESTED_SLOT, LATEST_SEEN_SLOT, PROOF_COMPLETIONS, PROOF_FAILURES,
    PROOF_REQUEST_FAILURES, PROOF_REQUESTS, RECONCILE_ACTIONS, REQUEST_DURATION,
    REQUEST_STAGE_DURATION,
};
use crate::request;
use crate::status::{
    self, BlockRecord, FailureStage, JsonStatusStore, MemoryStatusStore, Outcome, ProofResolution,
    StatusStore,
};
use crate::zkboost::{self, ProofEvent};

/// Delay before reconnecting after an event stream drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How many stuck records reconciliation probes concurrently.
const RECONCILE_CONCURRENCY: usize = 4;

/// How long a reconciliation probe waits for zkBoost to replay a completion.
/// Replays arrive immediately on connect, so a short window suffices.
const RECONCILE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound on the cheap liveness request that gates silence verdicts.
const LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How often reconciliation re-sweeps unresolved proofs while the event
/// stream stays connected. A healthy stream can live for hours, so a proof
/// whose verdict was deferred on reconnect (too young, zkBoost unreachable)
/// would otherwise never be revisited.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(60);

/// Failure category for proofs written off by reconciliation. Deliberately one
/// low-cardinality bucket: a missed failure event and a zkBoost restart that
/// orphaned the job are indistinguishable from here.
const UNRESOLVED_REASON: &str = "Unresolved";

/// Open `prove_block` root-span handles, keyed by request root.
///
/// A held handle keeps the span open past submission, so it spans the whole
/// pipeline; the watcher or reconciliation records the block's outcome and
/// drops the handle on the terminal transition, which closes the span for
/// export. Remaining handles drop with the runner on shutdown.
type SpanRegistry = Arc<Mutex<HashMap<String, Span>>>;

/// Start of the current observed-liveness streak, as unix milliseconds shared
/// between the watcher and reconciliation. Zero means zkBoost is not
/// currently known reachable. Silence verdicts age proofs against this
/// anchor, so time spent unreachable never counts as silence.
type LivenessAnchor = Arc<AtomicU64>;

/// Records positive evidence that zkBoost is reachable right now. The anchor
/// keeps the streak's start, so repeated evidence does not move it.
fn mark_alive(liveness: &AtomicU64) {
    if liveness.load(Ordering::Acquire) == 0 {
        // A benign race between two observers stores near-identical values.
        liveness.store(status::now_ms().max(1), Ordering::Release);
    }
}

/// Resets the liveness streak: reachability must be re-proven before any
/// further silence verdict, and the streak restarts from that proof.
fn mark_unreachable(liveness: &AtomicU64) {
    liveness.store(0, Ordering::Release);
}

/// The status model tracks proofs per type, but stream mode is restricted to a
/// single proof type: multi-proof streaming has not been exercised end to end
/// (dashboard aggregation, per-type failure display), and proving several
/// types per block multiplies prover cost. The one-shot `request` command
/// accepts multiple types.
fn ensure_single_proof_type(count: usize) -> Result<()> {
    anyhow::ensure!(
        count == 1,
        "stream mode supports exactly one proof type (got {count}); \
         use `request` for multiple"
    );
    Ok(())
}

/// Runs stream mode: request proofs for new non-optimistic beacon blocks.
pub async fn run(args: StreamArgs) -> Result<()> {
    ensure_single_proof_type(args.proof_types.len())?;

    let beacon = Arc::new(beacon::Client::new(
        args.endpoints.beacon_url.clone(),
        &args.endpoints.beacon_header,
    )?);
    let zkboost = Arc::new(zkboost::Client::new(args.endpoints.zkboost_url.clone())?);
    let proof_types: Arc<Vec<ProofType>> = Arc::new(
        args.proof_types
            .iter()
            .map(|name| zkboost::parse_proof_type(name.as_str()))
            .collect::<Result<Vec<_>>>()?,
    );
    let semaphore = Arc::new(Semaphore::new(args.max_inflight.get()));
    let latest_requested = Arc::new(AtomicU64::new(0));
    let artifacts = Arc::new(zkboost::Artifacts {
        download: args.download,
        verify: args.verify,
        out_dir: args.out_dir.clone(),
    });

    let store: Arc<dyn StatusStore> = match &args.state_dir {
        Some(dir) => {
            let store = JsonStatusStore::load(dir, args.max_history).await?;
            let latest_slot = store.latest_slot().await;
            info!(
                state_dir = %dir.display(),
                ?latest_slot,
                "loaded request status from state directory"
            );
            Arc::new(store)
        }
        None => Arc::new(MemoryStatusStore::new(args.max_history)),
    };

    let spans: SpanRegistry = Arc::new(Mutex::new(HashMap::new()));

    // Observe proof outcomes (and run artifact actions) independently of submission.
    let watcher = tokio::spawn(watch(
        zkboost.clone(),
        store.clone(),
        artifacts.clone(),
        spans.clone(),
        args.reconcile_after,
    ));

    let http_server = match args.http_addr {
        Some(addr) => {
            let handle = crate::metrics::install()?;
            info!(%addr, "serving health, metrics, and the dashboard API");
            Some(tokio::spawn(crate::web::serve(
                addr,
                handle,
                store.clone(),
                args.ui_dir.clone(),
            )))
        }
        None => None,
    };
    // Seed the inflight gauge from loaded state now that the recorder exists;
    // starting from the recorder's implicit zero would send the gauge negative
    // as soon as reconciliation resolves proofs recorded by a previous run.
    sync_inflight_gauge(&store).await;

    let mut sigterm =
        signal(SignalKind::terminate()).context("failed to install SIGTERM handler")?;
    let mut tasks = JoinSet::new();

    info!(
        max_inflight = args.max_inflight.get(),
        "streaming beacon block events"
    );

    'outer: loop {
        let mut events = Box::pin(beacon.subscribe_block_events());

        loop {
            let event = tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("received SIGINT, shutting down");
                    break 'outer;
                }
                _ = sigterm.recv() => {
                    info!("received SIGTERM, shutting down");
                    break 'outer;
                }
                event = events.next() => match event {
                    Some(Ok(event)) => event,
                    Some(Err(error)) => {
                        warn!(%error, "beacon event stream error; reconnecting");
                        break;
                    }
                    None => {
                        warn!("beacon event stream ended; reconnecting");
                        break;
                    }
                },
            };

            // Reap finished tasks so the join set does not grow unbounded.
            while tasks.try_join_next().is_some() {}

            gauge!(LATEST_SEEN_SLOT).set(event.slot as f64);
            let lag = event
                .slot
                .saturating_sub(latest_requested.load(Ordering::Relaxed));
            gauge!(HEAD_LAG).set(lag as f64);

            if event.execution_optimistic {
                counter!(BLOCKS_SKIPPED).increment(1);
                info!(slot = event.slot, "skipping optimistic block");
                continue;
            }
            counter!(BLOCKS_OBSERVED).increment(1);

            // Bounded submission concurrency: wait for a free slot, but stay interruptible.
            let permit = tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("received SIGINT, shutting down");
                    break 'outer;
                }
                _ = sigterm.recv() => {
                    info!("received SIGTERM, shutting down");
                    break 'outer;
                }
                permit = semaphore.clone().acquire_owned() => {
                    permit.context("proof submission semaphore closed")?
                }
            };

            let beacon = beacon.clone();
            let zkboost = zkboost.clone();
            let proof_types = proof_types.clone();
            let store = store.clone();
            let latest_requested = latest_requested.clone();
            let spans = spans.clone();
            tasks.spawn(async move {
                let _permit = permit;
                if let Err(error) = process_block(
                    &beacon,
                    &zkboost,
                    &proof_types,
                    &store,
                    &latest_requested,
                    &spans,
                    &event,
                )
                .await
                {
                    warn!(slot = event.slot, block = %event.block, %error, "block processing failed");
                }
            });
        }

        // Back off before reconnecting, but stay interruptible.
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("received SIGINT, shutting down");
                break 'outer;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break 'outer;
            }
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }

    info!(
        in_flight = tasks.len(),
        "stopping; draining in-flight submissions"
    );
    tasks.shutdown().await;
    watcher.abort();
    if let Some(server) = http_server {
        server.abort();
    }
    Ok(())
}

/// Runs one block's pipeline under its `prove_block` root span.
///
/// The span stays open past submission via [`SpanRegistry`], so the proving
/// wait is on the trace too; the watcher or reconciliation records its
/// `outcome` field on the block's terminal transition. Errors that bubble out
/// here (fetch/decode failures) never reach the store, so the outcome is
/// recorded before the span drops.
async fn process_block(
    beacon: &beacon::Client,
    zkboost: &zkboost::Client,
    proof_types: &[ProofType],
    store: &Arc<dyn StatusStore>,
    latest_requested: &AtomicU64,
    spans: &SpanRegistry,
    event: &BlockEvent,
) -> Result<()> {
    let span = prove_block_span(event.slot, &event.block.to_string());
    let result = submit_block(
        beacon,
        zkboost,
        proof_types,
        store,
        latest_requested,
        spans,
        event,
        &span,
    )
    .instrument(span.clone())
    .await;
    if result.is_err() {
        span.record("outcome", "failed");
    }
    result
}

/// Fetches, builds, and submits the proof request for a single block event.
async fn submit_block(
    beacon: &beacon::Client,
    zkboost: &zkboost::Client,
    proof_types: &[ProofType],
    store: &Arc<dyn StatusStore>,
    latest_requested: &AtomicU64,
    spans: &SpanRegistry,
    event: &BlockEvent,
    span: &Span,
) -> Result<()> {
    let observed_at_ms = status::now_ms();
    let start = Instant::now();
    let trace_id = current_trace_id(span);
    let block_id = BlockId::Root(event.block.to_string());
    let fetched = beacon
        .get_block(&block_id)
        .instrument(info_span!("fetch_block"))
        .await?;

    let build_start = Instant::now();
    let (payload_request, local_root) = info_span!("build_request").in_scope(|| {
        let payload_request = request::build(fetched.block())?;
        let local_root = request::root(&payload_request);
        anyhow::Ok((payload_request, local_root))
    })?;
    let root_hex = local_root.to_string();
    histogram!(REQUEST_STAGE_DURATION, "stage" => "build")
        .record(build_start.elapsed().as_secs_f64());

    // Skip blocks already requested (in this run or a previous one).
    if store.seen(&root_hex).await {
        counter!(BLOCKS_SKIPPED).increment(1);
        span.record("outcome", "skipped");
        info!(slot = fetched.slot(), root = %local_root, "request already recorded; skipping");
        return Ok(());
    }

    let submit_start = Instant::now();
    let server_root = match zkboost
        .request_proof(&payload_request, proof_types)
        .instrument(info_span!("submit_request"))
        .await
    {
        Ok(root) => root,
        Err(error) => {
            // Record the submit failure (often transient) rather than dropping it,
            // so the attempt shows as a failure instead of an absent slot. zkBoost
            // owns retry coordination, so the request is not auto-resubmitted here.
            counter!(PROOF_REQUEST_FAILURES).increment(1);
            let mut record = failed_record(
                &fetched,
                payload_request.block_number(),
                payload_request.block_hash().to_string(),
                root_hex.clone(),
                proof_types,
                observed_at_ms,
                "SubmitError",
                format!("{error:#}"),
            );
            record.trace_id = trace_id;
            let evicted = store.record(record).await?;
            close_evicted_spans(spans, &evicted);
            span.record("outcome", "failed");
            warn!(slot = fetched.slot(), root = %local_root, %error, "proof submission failed");
            return Ok(());
        }
    };
    histogram!(REQUEST_STAGE_DURATION, "stage" => "submit")
        .record(submit_start.elapsed().as_secs_f64());
    if server_root != local_root {
        // The server recomputed a different root, so this request lives under a
        // root with no incoming proof events; record it instead of leaving it to
        // linger unresolved.
        counter!(PROOF_REQUEST_FAILURES).increment(1);
        let mut record = failed_record(
            &fetched,
            payload_request.block_number(),
            payload_request.block_hash().to_string(),
            root_hex.clone(),
            proof_types,
            observed_at_ms,
            "RootMismatch",
            format!("local {local_root} != server {server_root}"),
        );
        record.trace_id = trace_id;
        let evicted = store.record(record).await?;
        close_evicted_spans(spans, &evicted);
        span.record("outcome", "failed");
        warn!(
            slot = fetched.slot(),
            local_root = %local_root,
            server_root = %server_root,
            "new_payload_request_root mismatch"
        );
        return Ok(());
    }

    latest_requested.fetch_max(fetched.slot(), Ordering::Relaxed);
    counter!(PROOF_REQUESTS).increment(1);
    gauge!(LATEST_REQUESTED_SLOT).set(fetched.slot() as f64);
    histogram!(REQUEST_DURATION).record(start.elapsed().as_secs_f64());

    let mut record = BlockRecord::new(
        fetched.slot(),
        fetched.root().to_string(),
        payload_request.block_number(),
        payload_request.block_hash().to_string(),
        root_hex.clone(),
        proof_types.iter().map(|p| p.as_str().to_string()).collect(),
        observed_at_ms,
    );
    record.trace_id = trace_id;

    // Hold the span open before the record lands, so the watcher can never
    // resolve a record whose span handle is not registered yet.
    register_span(spans, root_hex.clone(), span.clone());
    match store.record(record).await {
        Ok(evicted) => close_evicted_spans(spans, &evicted),
        Err(error) => {
            drop_span(spans, &root_hex);
            return Err(error);
        }
    }
    sync_inflight_gauge(store).await;

    info!(
        slot = fetched.slot(),
        beacon_block_root = %fetched.root(),
        fork = %fetched.fork(),
        execution_block_number = payload_request.block_number(),
        new_payload_request_root = %server_root,
        "proof requested"
    );
    Ok(())
}

/// Root span covering one block's pipeline, from discovery through proving.
/// `outcome` starts empty and is recorded on the terminal transition.
fn prove_block_span(slot: u64, block_root: &str) -> Span {
    info_span!("prove_block", slot, block_root, outcome = field::Empty)
}

/// The trace id backing a block's root span: `None` unless the crate is built
/// with the `otel` feature, an exporter is configured, and the span was
/// sampled.
fn current_trace_id(span: &Span) -> Option<String> {
    #[cfg(feature = "otel")]
    {
        crate::otel::trace_id(span)
    }
    #[cfg(not(feature = "otel"))]
    {
        let _ = span;
        None
    }
}

/// Holds a block's root span open until its terminal transition.
fn register_span(spans: &SpanRegistry, root_hex: String, span: Span) {
    if let Ok(mut map) = spans.lock() {
        map.insert(root_hex, span);
    }
}

/// Drops a registered span handle without recording an outcome (used when the
/// record it belongs to failed to persist).
fn drop_span(spans: &SpanRegistry, root_hex: &str) {
    if let Ok(mut map) = spans.lock() {
        map.remove(root_hex);
    }
}

/// Closes the span handles of records evicted from the store's history cap.
///
/// An evicted record may still be unresolved; without this its root span
/// would stay open (and leak) until shutdown. Resolved records were already
/// removed from the registry on their terminal transition, so most evictions
/// are no-ops here.
fn close_evicted_spans(spans: &SpanRegistry, evicted: &[String]) {
    if evicted.is_empty() {
        return;
    }
    if let Ok(mut map) = spans.lock() {
        for root in evicted {
            if let Some(span) = map.remove(root) {
                span.record("outcome", "evicted");
            }
        }
    }
}

/// Sets the inflight gauge absolutely from store state.
///
/// A state-derived gauge cannot drift: restarts (which reset the recorder to
/// zero) and history eviction (which can drop still-unresolved proofs) are
/// both reflected on the next set, where event deltas would leave the gauge
/// negative or permanently inflated.
async fn sync_inflight_gauge(store: &Arc<dyn StatusStore>) {
    gauge!(INFLIGHT_REQUESTS).set(store.inflight_proofs().await as f64);
}

/// On a block's terminal transition, records the derived outcome on its root
/// span and drops the handle, closing the span for export. A no-op while
/// proofs are still unresolved or when no handle is held (pre-submit
/// failures resolve before registration; restarts lose handles by design).
fn finish_block_span(spans: &SpanRegistry, root_hex: &str, resolution: &ProofResolution) {
    if !resolution.block_resolved {
        return;
    }
    let removed = spans.lock().ok().and_then(|mut map| map.remove(root_hex));
    if let Some(span) = removed {
        span.record("outcome", resolution.block_outcome.as_str());
    }
}

/// Builds a `Failed` record for a request that never reached the proving stage,
/// so the attempt is visible as a failure rather than lost as an absent slot.
fn failed_record(
    fetched: &beacon::FetchedBlock,
    block_number: u64,
    block_hash: String,
    root_hex: String,
    proof_types: &[ProofType],
    observed_at_ms: u64,
    reason: &str,
    error: String,
) -> BlockRecord {
    let record = BlockRecord::new(
        fetched.slot(),
        fetched.root().to_string(),
        block_number,
        block_hash,
        root_hex,
        proof_types.iter().map(|p| p.as_str().to_string()).collect(),
        observed_at_ms,
    );
    // Pre-submit failures all originate on the requestor side, so they are
    // tagged as the submit stage.
    mark_failed(record, FailureStage::Submit, reason, error)
}

/// Marks every proof of a record failed with the given stage, reason, and
/// detail, stamping resolution. A failure before or at submission affects all
/// requested proof types alike, since none of them reached the prover.
fn mark_failed(
    mut record: BlockRecord,
    stage: FailureStage,
    reason: &str,
    error: String,
) -> BlockRecord {
    let resolved_at_ms = status::now_ms();
    for proof in &mut record.proofs {
        proof.outcome = Outcome::Failed;
        proof.stage = Some(stage);
        proof.reason = Some(reason.to_string());
        proof.error = Some(error.clone());
        proof.resolved_at_ms = Some(resolved_at_ms);
    }
    record
}

/// Observes proof events, recording outcomes and running artifact actions.
///
/// Reconnects after a transient stream drop; runs until aborted on shutdown.
/// Every (re)connect — including the first after a restart — starts a
/// reconciliation sweep over proofs still marked sent, since outcomes emitted
/// while disconnected are not redelivered on the live stream; the sweep then
/// repeats periodically while the subscription lives, so verdicts deferred as
/// too young (or while zkBoost was unreachable) are eventually revisited.
async fn watch(
    zkboost: Arc<zkboost::Client>,
    store: Arc<dyn StatusStore>,
    artifacts: Arc<zkboost::Artifacts>,
    spans: SpanRegistry,
    reconcile_after: Duration,
) {
    let liveness: LivenessAnchor = Arc::new(AtomicU64::new(0));
    loop {
        let mut events = Box::pin(zkboost.subscribe_proof_events());
        let reconciler = tokio::spawn(reconcile_periodically(
            zkboost.clone(),
            store.clone(),
            spans.clone(),
            reconcile_after,
            liveness.clone(),
        ));
        while let Some(event) = events.next().await {
            let event = match event {
                Ok(event) => {
                    // A delivered event is positive liveness evidence.
                    mark_alive(&liveness);
                    event
                }
                Err(error) => {
                    warn!(%error, "proof event stream error; reconnecting");
                    break;
                }
            };
            if let Err(error) =
                handle_proof_event(&zkboost, &store, &artifacts, &spans, event).await
            {
                warn!(%error, "failed to handle proof event");
            }
        }
        // The connection is gone; the liveness streak ends with it.
        mark_unreachable(&liveness);
        // Stop the periodic sweep and wait it out, so a stale pass never
        // overlaps the one the reconnect starts.
        reconciler.abort();
        if let Err(error) = reconciler.await
            && !error.is_cancelled()
        {
            warn!(%error, "reconciliation task failed");
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Runs a reconciliation pass immediately, then repeats every
/// [`RECONCILE_INTERVAL`] until aborted (when the event stream drops). The
/// immediate pass picks up outcomes missed while disconnected; the periodic
/// re-sweep revisits deferred verdicts, which a healthy long-lived stream
/// would otherwise never trigger again.
async fn reconcile_periodically(
    zkboost: Arc<zkboost::Client>,
    store: Arc<dyn StatusStore>,
    spans: SpanRegistry,
    unresolved_after: Duration,
    liveness: LivenessAnchor,
) {
    let mut ticks = tokio::time::interval(RECONCILE_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await; // The first tick fires immediately.
        reconcile(&zkboost, &store, &spans, unresolved_after, &liveness).await;
    }
}

/// Confirms zkBoost is alive right now with a cheap `GET /v1/proof_types`,
/// updating the liveness anchor either way.
///
/// This is the positive evidence silence verdicts require. A per-root probe
/// cannot supply it: the client consumes the SSE open event internally, so a
/// hanging endpoint (LB drain, partition without a RST) times out exactly
/// like a healthy-but-quiet subscription.
async fn check_liveness(zkboost: &zkboost::Client, liveness: &AtomicU64) -> bool {
    match tokio::time::timeout(LIVENESS_PROBE_TIMEOUT, zkboost.proof_types()).await {
        Ok(Ok(_)) => {
            mark_alive(liveness);
            true
        }
        Ok(Err(error)) => {
            warn!(%error, "zkBoost liveness check failed");
            mark_unreachable(liveness);
            false
        }
        Err(_) => {
            warn!(
                timeout_s = LIVENESS_PROBE_TIMEOUT.as_secs(),
                "zkBoost liveness check timed out"
            );
            mark_unreachable(liveness);
            false
        }
    }
}

/// Records a single proof event's outcome and runs artifact actions on completion.
///
/// Events are routed to the matching [`crate::status::ProofRecord`] by proof
/// type; unknown
/// roots, unrequested proof types, and duplicate terminal events resolve
/// nothing and are ignored.
async fn handle_proof_event(
    zkboost: &zkboost::Client,
    store: &Arc<dyn StatusStore>,
    artifacts: &zkboost::Artifacts,
    spans: &SpanRegistry,
    event: ProofEvent,
) -> Result<()> {
    match event {
        ProofEvent::ProofComplete(complete) => {
            let root_hex = complete.new_payload_request_root.to_string();
            let Some(_resolution) =
                record_completion(store, spans, &root_hex, complete.proof_type.as_str()).await?
            else {
                return Ok(());
            };
            info!(root = %root_hex, proof_type = %complete.proof_type, "proof complete");
            if artifacts.needs_proof_bytes() {
                zkboost
                    .collect_artifacts(
                        complete.new_payload_request_root,
                        complete.proof_type,
                        artifacts,
                    )
                    .await?;
            }
        }
        ProofEvent::ProofFailure(failure) => {
            let root_hex = failure.new_payload_request_root.to_string();
            let reason = format!("{:?}", failure.reason);
            let detail = status::Failure {
                stage: FailureStage::Proving,
                reason,
                error: failure.error.clone(),
            };
            let Some(_resolution) =
                record_failure(store, spans, &root_hex, failure.proof_type.as_str(), detail)
                    .await?
            else {
                return Ok(());
            };
            warn!(
                root = %root_hex,
                proof_type = %failure.proof_type,
                reason = ?failure.reason,
                error = %failure.error,
                "proof failed"
            );
        }
    }
    Ok(())
}

/// Resolves one proof to `Complete` in the store, emitting the completion
/// metrics when something actually transitioned. Shared by the live watcher
/// and reconciliation, so a race between them counts exactly once.
async fn record_completion(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    root_hex: &str,
    proof_type: &str,
) -> Result<Option<ProofResolution>> {
    let Some(resolution) = store
        .resolve_proof(root_hex, proof_type, Outcome::Complete, None)
        .await?
    else {
        return Ok(None);
    };
    finish_block_span(spans, root_hex, &resolution);
    counter!(PROOF_COMPLETIONS, "proof_type" => proof_type.to_owned()).increment(1);
    sync_inflight_gauge(store).await;
    histogram!(COMPLETION_DURATION, "proof_type" => proof_type.to_owned())
        .record(resolution.duration_ms as f64 / 1000.0);
    Ok(Some(resolution))
}

/// Resolves one proof to `Failed` in the store, emitting the failure metrics
/// when something actually transitioned. Metrics stay labeled by the
/// low-cardinality reason only; the free-form error text is kept on the
/// record, never as a label.
async fn record_failure(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    root_hex: &str,
    proof_type: &str,
    failure: status::Failure,
) -> Result<Option<ProofResolution>> {
    let reason = failure.reason.clone();
    let Some(resolution) = store
        .resolve_proof(root_hex, proof_type, Outcome::Failed, Some(failure))
        .await?
    else {
        return Ok(None);
    };
    finish_block_span(spans, root_hex, &resolution);
    counter!(PROOF_FAILURES, "proof_type" => proof_type.to_owned(), "reason" => reason)
        .increment(1);
    sync_inflight_gauge(store).await;
    Ok(Some(resolution))
}

/// One reconciliation sweep over proofs still marked sent.
///
/// Outcomes arrive only on zkBoost's live event stream — events that fired
/// while disconnected are gone. zkBoost does, however, replay its cached
/// completions when a subscription is opened for a specific root (an LRU of
/// the most recent completions; failures are never replayed), so each stuck
/// record is probed with a short-lived per-root subscription, and what stays
/// silent is judged by age. Every silence verdict is gated on positive
/// liveness evidence bracketing the probe window: an unreachable or hanging
/// zkBoost defers all verdicts to a later sweep, because an outage must never
/// write off a proof whose completion sits unreachable in that cache.
/// Nothing is ever resubmitted.
async fn reconcile(
    zkboost: &zkboost::Client,
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    unresolved_after: Duration,
    liveness: &LivenessAnchor,
) {
    let stuck = store.unresolved_records().await;
    if stuck.is_empty() {
        return;
    }
    // Silence is only evidence while zkBoost is demonstrably reachable.
    if !check_liveness(zkboost, liveness).await {
        info!(
            records = stuck.len(),
            "zkBoost not reachable; deferring all reconciliation verdicts"
        );
        return;
    }
    info!(records = stuck.len(), "reconciling in-flight proofs");
    let probed: Vec<(BlockRecord, bool)> = futures::stream::iter(stuck)
        .map(|record| async move {
            let judgeable = match probe_record(zkboost, store, spans, &record).await {
                Ok(judgeable) => judgeable,
                Err(error) => {
                    warn!(
                        root = %record.new_payload_request_root,
                        %error,
                        "failed to probe record; deferring its verdict"
                    );
                    false
                }
            };
            (record, judgeable)
        })
        .buffer_unordered(RECONCILE_CONCURRENCY)
        .collect()
        .await;
    // Re-confirm liveness after the probes: a probe window that elapsed in
    // silence means silence only if zkBoost stayed reachable through it.
    if !check_liveness(zkboost, liveness).await {
        info!("zkBoost became unreachable during the probe window; deferring silence verdicts");
        return;
    }
    let alive_since_ms = liveness.load(Ordering::Acquire);
    let now_ms = status::now_ms();
    for (record, judgeable) in probed {
        if !judgeable {
            continue;
        }
        if let Err(error) = resolve_silent_proofs(
            store,
            spans,
            &record,
            unresolved_after,
            now_ms,
            alive_since_ms,
        )
        .await
        {
            warn!(
                root = %record.new_payload_request_root,
                %error,
                "failed to apply the silence policy"
            );
        }
    }
}

/// Probes one record's per-root subscription for replayed completions.
///
/// Returns whether the record may face the silence policy: `true` when the
/// probe stayed healthy — everything zkBoost had cached for this root was
/// seen and recorded — and `false` when the stream erred or a store write
/// failed, in which case silence proves nothing and judgment is deferred.
async fn probe_record(
    zkboost: &zkboost::Client,
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    record: &BlockRecord,
) -> Result<bool> {
    let root_hex = record.new_payload_request_root.as_str();
    let root: Hash256 = root_hex
        .parse()
        .with_context(|| format!("recorded request root '{root_hex}' is not a valid hash"))?;
    let pending: HashSet<String> = record
        .proofs
        .iter()
        .filter(|proof| proof.outcome == Outcome::Sent)
        .map(|proof| proof.proof_type.clone())
        .collect();

    // The subscription replays cached completions immediately on connect; if
    // nothing arrives within the window, nothing is cached for this root.
    let probe = apply_probe_events(
        store,
        spans,
        root_hex,
        pending,
        zkboost.subscribe_root_events(root),
    );
    match tokio::time::timeout(RECONCILE_PROBE_TIMEOUT, probe).await {
        Ok(judgeable) => Ok(judgeable),
        // An elapsed window alone cannot distinguish a quiet healthy
        // subscription from one that never connected (the client consumes
        // the SSE open event internally); it counts as observed silence only
        // because the caller brackets the window with liveness checks.
        Err(_elapsed) => Ok(true),
    }
}

/// Applies completions from a per-root probe stream until every pending proof
/// type resolves or the stream ends (the caller bounds it with a timeout).
///
/// Returns whether the record may face the silence policy afterward: `false`
/// means the stream erred (zkBoost was not observed) or a store write failed
/// (the completion exists but was not applied — judging that proof silent
/// would write off work known to have finished).
///
/// Only completions are handled: zkBoost never replays failures, and any live
/// failure racing in here also reaches the main watcher stream, whose
/// resolution path is idempotent with this one.
async fn apply_probe_events(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    root_hex: &str,
    mut pending: HashSet<String>,
    events: impl Stream<Item = Result<ProofEvent>> + Send,
) -> bool {
    let mut events = pin!(events);
    while !pending.is_empty() {
        let event = match events.next().await {
            Some(Ok(event)) => event,
            Some(Err(error)) => {
                warn!(root = %root_hex, %error, "reconciliation probe stream error");
                return false;
            }
            // Exhaustion without an error: the live subscription surfaces a
            // dropped connection as an `Err` item first, so getting here
            // means everything sent (any replays included) was seen.
            None => return true,
        };
        let ProofEvent::ProofComplete(complete) = event else {
            continue;
        };
        if complete.new_payload_request_root.to_string() != root_hex {
            continue;
        }
        let proof_type = complete.proof_type.as_str();
        if !pending.contains(proof_type) {
            continue;
        }
        // A replayed completion counts as handled — and leaves `pending` —
        // only once its store write is confirmed (or the live stream
        // demonstrably beat this one to it).
        match record_completion(store, spans, root_hex, proof_type).await {
            Ok(Some(_)) => {
                pending.remove(proof_type);
                counter!(RECONCILE_ACTIONS, "verdict" => "complete").increment(1);
                info!(
                    root = %root_hex,
                    proof_type,
                    verdict = "complete",
                    "reconciled proof from replayed completion"
                );
            }
            // The live stream already resolved it — the race is a no-op here.
            Ok(None) => {
                pending.remove(proof_type);
            }
            Err(error) => {
                warn!(root = %root_hex, proof_type, %error, "failed to record a replayed completion");
                return false;
            }
        }
    }
    // Every pending proof resolved from the replay.
    true
}

/// Applies the silence policy to a record's still-sent proofs.
///
/// Silence only counts while zkBoost is observably alive: each proof ages
/// from the later of its submission and `alive_since_ms` — the start of the
/// current observed-liveness streak (`0` means alive since before any
/// submission). Time zkBoost spent unreachable proves nothing about a proof;
/// completions from such a window are recovered by the probe replay instead.
/// Proofs younger than the cutoff are left alone (zkBoost may still be
/// queueing or proving them), while older ones resolve `Failed`/`Unresolved`
/// — their outcome event is gone (missed while disconnected, or orphaned by
/// a zkBoost restart), no completion is cached, and no event will ever
/// arrive.
async fn resolve_silent_proofs(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    record: &BlockRecord,
    cutoff: Duration,
    now_ms: u64,
    alive_since_ms: u64,
) -> Result<()> {
    let cutoff_ms = u64::try_from(cutoff.as_millis()).unwrap_or(u64::MAX);
    for proof in &record.proofs {
        if proof.outcome != Outcome::Sent {
            continue;
        }
        let silent_since_ms = proof.requested_at_ms.max(alive_since_ms);
        let age_ms = now_ms.saturating_sub(silent_since_ms);
        if age_ms < cutoff_ms {
            continue;
        }
        let failure = status::Failure {
            stage: FailureStage::Proving,
            reason: UNRESOLVED_REASON.to_owned(),
            error: format!(
                "silent for {}s with zkBoost reachable, and no completion \
                 cached there; the outcome event was missed or the job was lost",
                cutoff.as_secs()
            ),
        };
        // The record snapshot may be stale; resolve_proof only transitions a
        // proof still marked sent, so a probe or live event that beat this
        // write makes it a no-op.
        if record_failure(
            store,
            spans,
            &record.new_payload_request_root,
            &proof.proof_type,
            failure,
        )
        .await?
        .is_some()
        {
            counter!(RECONCILE_ACTIONS, "verdict" => "unresolved").increment(1);
            warn!(
                root = %record.new_payload_request_root,
                proof_type = %proof.proof_type,
                verdict = "unresolved",
                age_ms,
                "reconciled proof as unresolved"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use zkboost_client::ProofComplete;

    use super::*;

    /// A single-proof record whose root round-trips through [`Hash256`], with
    /// its submission time pinned to `requested_at_ms` for age-based tests.
    fn sent_record(requested_at_ms: u64) -> BlockRecord {
        let root = Hash256::repeat_byte(0xab);
        let mut record = BlockRecord::new(
            100,
            "0xbeacon".to_string(),
            99,
            "0xexechash".to_string(),
            root.to_string(),
            vec!["reth-zisk".to_string()],
            requested_at_ms,
        );
        for proof in &mut record.proofs {
            proof.requested_at_ms = requested_at_ms;
        }
        record
    }

    fn memory_store() -> Arc<dyn StatusStore> {
        Arc::new(MemoryStatusStore::new(0))
    }

    fn span_registry() -> SpanRegistry {
        Arc::new(Mutex::new(HashMap::new()))
    }

    async fn stored_record(store: &Arc<dyn StatusStore>) -> BlockRecord {
        store
            .records()
            .await
            .into_iter()
            .next()
            .expect("one record")
    }

    #[tokio::test]
    async fn reconciliation_marks_stale_sent_proofs_unresolved() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // 180s cutoff, and the proof has been silent for exactly that long
        // with zkBoost observed alive throughout.
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(180),
            181_000,
            0,
        )
        .await
        .expect("silence policy");

        let stored = stored_record(&store).await;
        assert_eq!(stored.outcome(), Outcome::Failed);
        let proof = stored.proofs.first().expect("one proof");
        assert_eq!(proof.stage, Some(FailureStage::Proving));
        assert_eq!(proof.reason.as_deref(), Some(UNRESOLVED_REASON));
    }

    #[tokio::test]
    async fn reconciliation_leaves_young_sent_proofs_alone() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // Only 10s of silence against a 180s cutoff: still proving.
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(180),
            11_000,
            0,
        )
        .await
        .expect("silence policy");

        assert_eq!(stored_record(&store).await.outcome(), Outcome::Sent);
    }

    #[tokio::test]
    async fn silence_ages_from_the_liveness_anchor_not_submission() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // The proof is 499s past submission (cutoff 180s), but zkBoost has
        // only been observed alive for the last 100s — the earlier silence
        // proves nothing, so the verdict is deferred.
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(180),
            500_000,
            400_000,
        )
        .await
        .expect("silence policy");
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Sent);

        // With the liveness streak covering a full cutoff of silence, the
        // proof is judged.
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(180),
            500_000,
            200_000,
        )
        .await
        .expect("silence policy");
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Failed);
    }

    /// A store whose writes fail, for exercising deferred-judgment paths.
    struct FailingStore;

    #[async_trait::async_trait]
    impl StatusStore for FailingStore {
        async fn seen(&self, _root: &str) -> bool {
            false
        }

        async fn record(&self, _record: BlockRecord) -> Result<Vec<String>> {
            anyhow::bail!("store write failed")
        }

        async fn resolve_proof(
            &self,
            _root: &str,
            _proof_type: &str,
            _outcome: Outcome,
            _failure: Option<status::Failure>,
        ) -> Result<Option<ProofResolution>> {
            anyhow::bail!("store write failed")
        }

        async fn latest_slot(&self) -> Option<u64> {
            None
        }

        async fn inflight_proofs(&self) -> usize {
            0
        }

        async fn unresolved_records(&self) -> Vec<BlockRecord> {
            Vec::new()
        }

        async fn records(&self) -> Vec<BlockRecord> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn failed_store_write_for_a_replayed_completion_defers_judgment() {
        let store: Arc<dyn StatusStore> = Arc::new(FailingStore);
        let record = sent_record(1_000);

        // The replayed completion proves the proof finished; failing to
        // record it must not leave the proof exposed to the silence policy.
        let root: Hash256 = record.new_payload_request_root.parse().expect("valid root");
        let replay = futures::stream::iter(vec![Ok(ProofEvent::ProofComplete(ProofComplete {
            new_payload_request_root: root,
            proof_type: zkboost::parse_proof_type("reth-zisk").expect("valid proof type"),
        }))]);
        let pending: HashSet<String> = ["reth-zisk".to_string()].into();
        let judgeable = apply_probe_events(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            pending,
            replay,
        )
        .await;

        assert!(!judgeable);
    }

    #[tokio::test]
    async fn replayed_completion_resolves_complete() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        let root: Hash256 = record.new_payload_request_root.parse().expect("valid root");
        let replay = futures::stream::iter(vec![Ok(ProofEvent::ProofComplete(ProofComplete {
            new_payload_request_root: root,
            proof_type: zkboost::parse_proof_type("reth-zisk").expect("valid proof type"),
        }))]);
        let pending: HashSet<String> = ["reth-zisk".to_string()].into();
        let observed = apply_probe_events(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            pending,
            replay,
        )
        .await;

        assert!(observed);
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Complete);
    }

    #[tokio::test]
    async fn probe_stream_error_is_not_treated_as_silence() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // The probe never reached zkBoost, so its silence proves nothing and
        // the caller must not apply the silence policy.
        let replay = futures::stream::iter(vec![Err(anyhow::anyhow!("connection refused"))]);
        let pending: HashSet<String> = ["reth-zisk".to_string()].into();
        let observed = apply_probe_events(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            pending,
            replay,
        )
        .await;

        assert!(!observed);
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Sent);
    }

    #[tokio::test]
    async fn reconciliation_racing_a_live_resolution_is_a_noop() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // The live stream wins the race and resolves the proof first.
        store
            .resolve_proof(
                &record.new_payload_request_root,
                "reth-zisk",
                Outcome::Complete,
                None,
            )
            .await
            .expect("resolve proof")
            .expect("transitioned");
        let resolved_at = stored_record(&store)
            .await
            .proofs
            .first()
            .expect("one proof")
            .resolved_at_ms;

        // A replayed completion for the already-resolved proof changes nothing.
        let root: Hash256 = record.new_payload_request_root.parse().expect("valid root");
        let replay = futures::stream::iter(vec![Ok(ProofEvent::ProofComplete(ProofComplete {
            new_payload_request_root: root,
            proof_type: zkboost::parse_proof_type("reth-zisk").expect("valid proof type"),
        }))]);
        let pending: HashSet<String> = ["reth-zisk".to_string()].into();
        let observed = apply_probe_events(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            pending,
            replay,
        )
        .await;
        assert!(observed);

        // The silence policy over the stale (still-sent) snapshot is a no-op too.
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(0),
            999_000,
            0,
        )
        .await
        .expect("silence policy");

        let stored = stored_record(&store).await;
        assert_eq!(stored.outcome(), Outcome::Complete);
        let proof = stored.proofs.first().expect("one proof");
        assert_eq!(proof.resolved_at_ms, resolved_at);
        assert_eq!(proof.reason, None);
    }

    #[test]
    fn stream_accepts_a_single_proof_type() {
        assert!(ensure_single_proof_type(1).is_ok());
    }

    #[test]
    fn stream_rejects_multiple_proof_types() {
        assert!(ensure_single_proof_type(2).is_err());
    }

    #[test]
    fn mark_failed_sets_outcome_and_detail_on_every_proof() {
        let base = BlockRecord::new(
            100,
            "0xbeacon".to_string(),
            99,
            "0xexechash".to_string(),
            "0xroot".to_string(),
            vec!["reth-zisk".to_string(), "ethrex-sp1".to_string()],
            1_000,
        );
        let failed = mark_failed(
            base,
            FailureStage::Submit,
            "SubmitError",
            "connection refused".to_string(),
        );

        assert_eq!(failed.outcome(), Outcome::Failed);
        assert_eq!(failed.proofs.len(), 2);
        for proof in &failed.proofs {
            assert_eq!(proof.outcome, Outcome::Failed);
            assert_eq!(proof.stage, Some(FailureStage::Submit));
            assert_eq!(proof.reason.as_deref(), Some("SubmitError"));
            assert_eq!(proof.error.as_deref(), Some("connection refused"));
            assert!(proof.resolved_at_ms.is_some());
        }
        // Identity fields from the base record are preserved.
        assert_eq!(failed.slot, 100);
        assert_eq!(failed.new_payload_request_root, "0xroot");
    }

    #[tokio::test]
    async fn eviction_closes_the_evicted_records_span_handle() {
        let store: Arc<dyn StatusStore> = Arc::new(MemoryStatusStore::new(1));
        let spans = span_registry();

        let old = BlockRecord::new(
            100,
            "0xbeacon".to_string(),
            99,
            "0xexechash".to_string(),
            "0xold".to_string(),
            vec!["reth-zisk".to_string()],
            1_000,
        );
        register_span(
            &spans,
            "0xold".to_string(),
            prove_block_span(100, "0xbeacon"),
        );
        let evicted = store.record(old).await.expect("record");
        assert!(evicted.is_empty());

        // A newer record over the history cap evicts the old unresolved one;
        // its span handle must not outlive the record.
        let newer = BlockRecord::new(
            101,
            "0xbeacon".to_string(),
            100,
            "0xexechash".to_string(),
            "0xnew".to_string(),
            vec!["reth-zisk".to_string()],
            2_000,
        );
        let evicted = store.record(newer).await.expect("record");
        close_evicted_spans(&spans, &evicted);
        assert!(spans.lock().expect("registry lock").is_empty());
    }

    #[test]
    fn finish_block_span_drops_the_handle_only_on_terminal_transitions() {
        let spans = span_registry();
        register_span(
            &spans,
            "0xroot".to_string(),
            prove_block_span(1, "0xbeacon"),
        );

        // A partial resolution keeps the span open.
        finish_block_span(
            &spans,
            "0xroot",
            &ProofResolution {
                duration_ms: 10,
                block_outcome: Outcome::Sent,
                block_resolved: false,
            },
        );
        assert!(spans.lock().expect("registry lock").contains_key("0xroot"));

        // The terminal transition records the outcome and releases the handle.
        finish_block_span(
            &spans,
            "0xroot",
            &ProofResolution {
                duration_ms: 10,
                block_outcome: Outcome::Complete,
                block_resolved: true,
            },
        );
        assert!(spans.lock().expect("registry lock").is_empty());
    }

    /// Without the `otel` feature there is no exporter to sample the span, so
    /// records must carry no trace id.
    #[cfg(not(feature = "otel"))]
    #[test]
    fn trace_id_is_none_without_the_otel_feature() {
        assert_eq!(current_trace_id(&prove_block_span(123, "0xbeacon")), None);
    }

    /// With the feature compiled in but no otel layer installed (no OTLP
    /// endpoint configured), spans carry no valid trace context.
    #[cfg(feature = "otel")]
    #[test]
    fn trace_id_is_none_without_an_exporter() {
        assert_eq!(current_trace_id(&prove_block_span(123, "0xbeacon")), None);
    }

    /// With an exporter installed, the `prove_block` span exports carrying the
    /// slot, block root, and recorded outcome, and its trace id is captured.
    #[cfg(feature = "otel")]
    #[tokio::test]
    async fn prove_block_span_exports_with_slot_and_block_root() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing_subscriber::layer::SubscriberExt;

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        // Warm the span callsite before installing the subscriber, so a
        // parallel test hitting it subscriber-less cannot leave a stale
        // `never` interest cached (mirrors zkBoost's otel span test).
        let _ = prove_block_span(0, "0xwarmup");
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::OpenTelemetryLayer::new(provider.tracer("test")),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        let mut exported = None;
        for _ in 0..5 {
            tracing::callsite::rebuild_interest_cache();
            let span = prove_block_span(123, "0xbeacon");
            let trace_id = current_trace_id(&span);
            span.record("outcome", "complete");
            drop(span);

            provider.force_flush().expect("flush spans");
            let spans = exporter.get_finished_spans().expect("finished spans");
            if let Some(span_data) = spans.iter().find(|s| s.name == "prove_block") {
                exported = Some((span_data.clone(), trace_id));
                break;
            }
        }

        let (span_data, trace_id) = exported.expect("prove_block span should export");
        let attr = |key: &str| {
            span_data
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.to_string())
        };
        assert_eq!(attr("slot").as_deref(), Some("123"));
        assert_eq!(attr("block_root").as_deref(), Some("0xbeacon"));
        assert_eq!(attr("outcome").as_deref(), Some("complete"));
        // The captured trace id is the exported span's, so the record links
        // to exactly this trace.
        assert_eq!(
            trace_id,
            Some(span_data.span_context.trace_id().to_string())
        );
    }
}
