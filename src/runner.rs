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
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use ::metrics::{counter, gauge, histogram};
use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{Instrument, Span, debug, field, info, info_span, warn};
use zkboost_client::{Hash256, ProofType};

use crate::beacon::{self, BlockEvent};
use crate::chain_config::ChainConfigSchedule;
use crate::config::{BlockId, StreamArgs};
use crate::metrics::{
    BLOCKS_OBSERVED, BLOCKS_SKIPPED, COMPLETION_DURATION, HEAD_LAG, INFLIGHT_REQUESTS,
    LATE_EVENTS_DISCARDED, LATEST_REQUESTED_SLOT, LATEST_SEEN_SLOT, PROOF_COMPLETIONS,
    PROOF_FAILURES, PROOF_REQUEST_FAILURES, PROOF_REQUESTS, RECONCILE_ACTIONS, REQUEST_DURATION,
    REQUEST_STAGE_DURATION, STORE_BYTES, STORE_RECORDS,
};
use crate::request;
use crate::status::{
    self, BlockRecord, FailureStage, MemoryStatusStore, Outcome, ProofResolution, ResolveOutcome,
    RetentionEviction, SqliteStatusStore, StatusStore,
};
use crate::zkboost::{self, ProofEvent};

/// Delay before reconnecting after an event stream drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How many stuck records reconciliation probes concurrently.
const RECONCILE_CONCURRENCY: usize = 4;

/// How long a reconciliation probe waits for zkBoost to replay a terminal
/// outcome. A failure stays buffered for this window so a concurrent retry's
/// completion can supersede a stale replay before either reaches the store.
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

/// Start of the current observed-liveness streak, as a [`monotonic_ms`]
/// reading shared between the watcher and reconciliation. Zero means zkBoost
/// is not currently known reachable. Silence verdicts age proofs against
/// this anchor, so time spent unreachable never counts as silence.
///
/// Monotonic on purpose: an NTP step or a VM pause landing mid-streak must
/// not stretch or shrink the observed silence. The residual wall-clock
/// exposure lives in the proofs' `requested_at_ms`, which stays `SystemTime`
/// because it is persisted and must remain meaningful across restarts — so
/// the submission-age half of the silence window can still be skewed by a
/// clock step across a restart; the streak half, which is what gates
/// verdicts after an outage, cannot.
type LivenessAnchor = Arc<AtomicU64>;

/// Milliseconds elapsed on a process-local monotonic clock.
///
/// The zero point is the first call, which never matters: only differences
/// between readings are used. [`Instant`] itself cannot be stored in an
/// atomic, so streak state shared through [`LivenessAnchor`] uses this
/// millisecond reading instead.
fn monotonic_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = *START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Records positive evidence that zkBoost is reachable right now. The anchor
/// keeps the streak's start, so repeated evidence does not move it.
fn mark_alive(liveness: &AtomicU64) {
    if liveness.load(Ordering::Acquire) == 0 {
        // A benign race between two observers stores near-identical values.
        liveness.store(monotonic_ms().max(1), Ordering::Release);
    }
}

/// Resets the liveness streak: reachability must be re-proven before any
/// further silence verdict, and the streak restarts from that proof.
fn mark_unreachable(liveness: &AtomicU64) {
    liveness.store(0, Ordering::Release);
}

/// Shared handle to the currently running periodic reconciler.
///
/// The reconciler is spawned inside `watch()`, but `watch()` itself is
/// stopped by abort on shutdown — an abort drops the watch future without
/// running any of its code, so a watch-local `JoinHandle` would simply be
/// dropped and the sweep would keep running detached, resolving proofs after
/// the otel provider has flushed. Holding the handle in a slot shared with
/// [`run`] lets every cancellation path abort *and await* the sweep.
type ReconcilerSlot = Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>;

/// Aborts and awaits the reconciler currently in the slot, if any.
///
/// Both `watch()` (on every stream drop) and `run()` (on shutdown, after the
/// watcher is awaited) call this; `Option::take` under the lock guarantees
/// exactly one caller awaits a given sweep. Resolutions already handed to the
/// detached recording tasks still land — that shield is deliberate and
/// bounded — but no *new* sweep work starts after this returns.
async fn stop_reconciler(slot: &ReconcilerSlot) {
    let handle = slot.lock().ok().and_then(|mut slot| slot.take());
    let Some(handle) = handle else {
        return;
    };
    handle.abort();
    if let Err(error) = handle.await
        && !error.is_cancelled()
    {
        warn!(%error, "reconciliation task failed");
    }
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
    // Install the recorder before opening persistent state so startup
    // retention (including one-time legacy import pruning) is counted too.
    let metrics_handle = if args.http_addr.is_some() {
        Some(crate::metrics::install()?)
    } else {
        None
    };

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

    // zkBoost requires each block's active execution fork alongside the
    // payload. The schedule is fixed by the consensus spec and genesis time,
    // so it is built once at startup; a spec change (a newly scheduled fork
    // on a devnet) needs a restart to be picked up.
    let config = beacon
        .get_config()
        .await
        .context("failed to fetch the beacon spec config")?;
    let genesis_time = beacon
        .get_genesis_time()
        .await
        .context("failed to fetch the beacon genesis time")?;
    let schedule = Arc::new(ChainConfigSchedule::new(&config, genesis_time)?);
    info!(
        chain_id = schedule.chain_id(),
        "built the chain-config fork schedule"
    );
    let artifacts = Arc::new(zkboost::Artifacts {
        download: args.download,
        verify: args.verify,
        out_dir: args.out_dir.clone(),
    });

    let store: Arc<dyn StatusStore> = match &args.state_dir {
        Some(dir) => {
            let store = SqliteStatusStore::open(dir, args.max_history).await?;
            let latest_slot = store.latest_slot().await?;
            let storage = store.storage_stats().await?;
            info!(
                state_dir = %dir.display(),
                database = %store.path().display(),
                ?latest_slot,
                records = storage.map_or(0, |stats| stats.records),
                bytes = storage.map_or(0, |stats| stats.bytes),
                "loaded request status from state directory"
            );
            Arc::new(store)
        }
        None => Arc::new(MemoryStatusStore::new(args.max_history)),
    };

    let spans: SpanRegistry = Arc::new(Mutex::new(HashMap::new()));

    // Observe proof outcomes (and run artifact actions) independently of submission.
    let reconciler: ReconcilerSlot = Arc::new(Mutex::new(None));
    let watcher = tokio::spawn(watch(
        zkboost.clone(),
        store.clone(),
        artifacts.clone(),
        schedule.clone(),
        spans.clone(),
        args.reconcile_after,
        reconciler.clone(),
    ));

    let http_server = match args.http_addr {
        Some(addr) => {
            let handle = metrics_handle.context("metrics recorder was not initialized")?;
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
    sync_status_gauges(&store).await;

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
            let schedule = schedule.clone();
            tasks.spawn(async move {
                let _permit = permit;
                if let Err(error) = process_block(
                    &beacon,
                    &zkboost,
                    &schedule,
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
    // Abort *and await* the background tasks: awaiting lets any in-flight
    // resolution land (store write, metrics, span close) before main flushes
    // the otel provider, where an un-awaited abort would race that flush and
    // drop the tail of the run's spans.
    watcher.abort();
    if let Err(error) = watcher.await
        && !error.is_cancelled()
    {
        warn!(%error, "watcher task failed during shutdown");
    }
    // Aborting the watcher drops its future without running its cleanup, so
    // the reconciler it spawned must be stopped here too — otherwise the
    // sweep would outlive the runner and keep resolving proofs after the
    // otel flush in main.
    stop_reconciler(&reconciler).await;
    if let Some(server) = http_server {
        server.abort();
        if let Err(error) = server.await
            && !error.is_cancelled()
        {
            warn!(%error, "http server task failed during shutdown");
        }
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
    schedule: &ChainConfigSchedule,
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
        schedule,
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
    schedule: &ChainConfigSchedule,
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

    // Skip blocks already requested (in this run or a previous one). The
    // check-then-record pair is not atomic: above the default --max-inflight
    // of 1, two concurrent submissions of one root could both pass it and
    // submit twice. The store keys records by root — the second record
    // replaces the first and events still resolve once — so the cost is a
    // redundant submission to zkBoost, not a corrupt record.
    if store.seen(&root_hex).await? {
        counter!(BLOCKS_SKIPPED).increment(1);
        span.record("outcome", "skipped");
        info!(slot = fetched.slot(), root = %local_root, "request already recorded; skipping");
        return Ok(());
    }

    // Errors bubble like fetch/build failures: a block whose timestamp
    // precedes every scheduled fork has no payload to prove anyway.
    let chain_config = schedule
        .resolve(payload_request.timestamp())
        .context("no execution fork is active at the block's timestamp")?;

    let submit_start = Instant::now();
    let server_root = match zkboost
        .request_proof(&payload_request, &chain_config, proof_types)
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
                request::block_hash(&payload_request).to_string(),
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
            request::block_hash(&payload_request).to_string(),
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
        request::block_hash(&payload_request).to_string(),
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
    sync_status_gauges(store).await;

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
///
/// Above `--max-inflight 1`, a duplicate submission of the same root (see the
/// non-atomic seen-check note in [`submit_block`]) overwrites the previous
/// handle here: the replaced span closes without an outcome recorded.
/// Accepted — the store record is replaced the same way, so the surviving
/// span and record stay consistent with each other.
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
fn close_evicted_spans(spans: &SpanRegistry, evicted: &[RetentionEviction]) {
    if evicted.is_empty() {
        return;
    }
    if let Ok(mut map) = spans.lock() {
        for eviction in evicted {
            if let Some(span) = map.remove(&eviction.request_root) {
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
///
/// The count-then-set pair is not atomic: two concurrent syncs can interleave
/// and briefly publish the staler count. Accepted — the very next sync
/// self-corrects, since every value is recomputed from store state rather
/// than accumulated.
async fn sync_status_gauges(store: &Arc<dyn StatusStore>) {
    match store.inflight_proofs().await {
        Ok(count) => gauge!(INFLIGHT_REQUESTS).set(count as f64),
        Err(error) => warn!(%error, "failed to refresh inflight gauge from status store"),
    }
    match store.storage_stats().await {
        Ok(Some(stats)) => {
            gauge!(STORE_RECORDS).set(stats.records as f64);
            gauge!(STORE_BYTES).set(stats.bytes as f64);
        }
        Ok(None) => {}
        Err(error) => warn!(%error, "failed to refresh status-store size gauges"),
    }
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
    schedule: Arc<ChainConfigSchedule>,
    spans: SpanRegistry,
    reconcile_after: Duration,
    reconciler: ReconcilerSlot,
) {
    let liveness: LivenessAnchor = Arc::new(AtomicU64::new(0));
    loop {
        let mut events = Box::pin(zkboost.subscribe_proof_events());
        let sweep = tokio::spawn(reconcile_periodically(
            zkboost.clone(),
            store.clone(),
            spans.clone(),
            reconcile_after,
            liveness.clone(),
        ));
        match reconciler.lock() {
            Ok(mut slot) => *slot = Some(sweep),
            // A poisoned slot cannot track the sweep; never let it run detached.
            Err(_) => sweep.abort(),
        }
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
                handle_proof_event(&zkboost, &store, &artifacts, &schedule, &spans, event).await
            {
                warn!(%error, "failed to handle proof event");
            }
        }
        // Stop the periodic sweep and wait it out BEFORE ending the liveness
        // streak. The reverse order had a race: a sweep that had already
        // passed its liveness re-check could load the just-zeroed anchor,
        // read it as "alive since forever", and hand every stuck proof a
        // maximum-age silence verdict at the exact moment the stream died.
        // Awaiting first also guarantees a stale pass never overlaps the one
        // the reconnect starts.
        stop_reconciler(&reconciler).await;
        // The connection is gone; the liveness streak ends with it.
        mark_unreachable(&liveness);
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
    schedule: &ChainConfigSchedule,
    spans: &SpanRegistry,
    event: ProofEvent,
) -> Result<()> {
    match event {
        ProofEvent::ProofComplete(complete) => {
            let root_hex = complete.new_payload_request_root.to_string();
            let Some(resolution) =
                record_completion(store, spans, &root_hex, complete.proof_type.as_str()).await?
            else {
                return Ok(());
            };
            info!(root = %root_hex, proof_type = %complete.proof_type, "proof complete");
            if artifacts.needs_proof_bytes() {
                // Verification must carry the chain config of the proof's own
                // block, resolved from its recorded slot.
                let chain_config = schedule.resolve_slot(resolution.slot);
                zkboost
                    .collect_artifacts(
                        complete.new_payload_request_root,
                        complete.proof_type,
                        chain_config.as_ref(),
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
///
/// The persist+metrics pair runs on a detached task, shielding it from
/// cancellation: callers live inside abortable tasks (the watcher, the
/// reconciliation sweep — the latter aborted on every stream drop), and an
/// abort landing between the store write and the metric emission would
/// persist a resolution whose metrics never fire. Detached, the pair runs to
/// completion even if the caller is dropped mid-await.
async fn record_completion(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    root_hex: &str,
    proof_type: &str,
) -> Result<Option<ProofResolution>> {
    let store = store.clone();
    let spans = spans.clone();
    let root_hex = root_hex.to_owned();
    let proof_type = proof_type.to_owned();
    tokio::spawn(async move {
        let resolution = match store
            .resolve_proof(&root_hex, &proof_type, Outcome::Complete, None)
            .await?
        {
            ResolveOutcome::Transitioned(resolution) => resolution,
            ResolveOutcome::AlreadyResolved(prior) => {
                note_late_event(
                    &root_hex,
                    &proof_type,
                    "completion",
                    Outcome::Complete,
                    prior,
                );
                return anyhow::Ok(None);
            }
            ResolveOutcome::Unknown => return anyhow::Ok(None),
        };
        finish_block_span(&spans, &root_hex, &resolution);
        counter!(PROOF_COMPLETIONS, "proof_type" => proof_type.clone()).increment(1);
        sync_status_gauges(&store).await;
        histogram!(COMPLETION_DURATION, "proof_type" => proof_type)
            .record(resolution.duration_ms as f64 / 1000.0);
        Ok(Some(resolution))
    })
    .await
    .context("proof-completion recording task failed")?
}

/// Resolves one proof to `Failed` in the store, emitting the failure metrics
/// when something actually transitioned. Metrics stay labeled by the
/// low-cardinality reason only; the free-form error text is kept on the
/// record, never as a label.
///
/// Shielded from cancellation the same way as [`record_completion`], and for
/// the same reason.
async fn record_failure(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    root_hex: &str,
    proof_type: &str,
    failure: status::Failure,
) -> Result<Option<ProofResolution>> {
    let store = store.clone();
    let spans = spans.clone();
    let root_hex = root_hex.to_owned();
    let proof_type = proof_type.to_owned();
    tokio::spawn(async move {
        let reason = failure.reason.clone();
        let resolution = match store
            .resolve_proof(&root_hex, &proof_type, Outcome::Failed, Some(failure))
            .await?
        {
            ResolveOutcome::Transitioned(resolution) => resolution,
            ResolveOutcome::AlreadyResolved(prior) => {
                note_late_event(&root_hex, &proof_type, "failure", Outcome::Failed, prior);
                return anyhow::Ok(None);
            }
            ResolveOutcome::Unknown => return anyhow::Ok(None),
        };
        finish_block_span(&spans, &root_hex, &resolution);
        counter!(PROOF_FAILURES, "proof_type" => proof_type, "reason" => reason).increment(1);
        sync_status_gauges(&store).await;
        Ok(Some(resolution))
    })
    .await
    .context("proof-failure recording task failed")?
}

/// Makes a discarded late event observable instead of silently dropping it.
///
/// The single-transition rule means a proof event landing on an already
/// resolved proof changes nothing — correct for duplicate deliveries, but it
/// would also silently eat the one case that matters: a real outcome arriving
/// *after* reconciliation already wrote the proof off (a false `Unresolved`
/// or a missed failure). Every discard counts toward the metric; a discard
/// that contradicts the recorded outcome — the stored verdict is now known
/// wrong — logs at warn, while a same-outcome duplicate (e.g. a probe replay
/// racing the live stream) stays at debug.
fn note_late_event(
    root_hex: &str,
    proof_type: &str,
    kind: &'static str,
    arrived: Outcome,
    prior: Outcome,
) {
    counter!(LATE_EVENTS_DISCARDED, "kind" => kind).increment(1);
    if arrived == prior {
        debug!(
            root = %root_hex,
            proof_type,
            kind,
            prior_outcome = prior.as_str(),
            "discarded a duplicate proof event for an already-resolved proof"
        );
    } else {
        warn!(
            root = %root_hex,
            proof_type,
            kind,
            prior_outcome = prior.as_str(),
            "discarded a late proof event that contradicts the recorded outcome; \
             the stored verdict for this proof is wrong"
        );
    }
}

/// One reconciliation sweep over proofs still marked sent.
///
/// Outcomes arrive on zkBoost's live event stream. A filtered subscription
/// also replays cached terminal outcomes, so each stuck record is probed with
/// a short-lived per-root subscription and what stays silent is judged by age.
/// Every silence verdict is gated on positive
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
    let stuck = match store.unresolved_records().await {
        Ok(records) => records,
        Err(error) => {
            warn!(%error, "failed to load unresolved records for reconciliation");
            return;
        }
    };
    // Age floor: only probe records that have waited at least a quarter of
    // the verdict cutoff. A seconds-old record cannot face the silence policy
    // for a long time, so probing it every sweep would only churn one
    // short-lived SSE subscription per record per RECONCILE_INTERVAL (and,
    // with enough stuck records, keep sweeps running back to back). A quarter
    // still leaves several probe opportunities before any record can reach
    // the cutoff; the cost is that a completion missed during a brief
    // disconnect is picked up from the replay cache up to cutoff/4 later
    // instead of on the next sweep.
    let probe_floor_ms = u64::try_from((unresolved_after / 4).as_millis()).unwrap_or(u64::MAX);
    let sweep_start_ms = status::now_ms();
    let stuck: Vec<BlockRecord> = stuck
        .into_iter()
        .filter(|record| old_enough_to_probe(record, sweep_start_ms, probe_floor_ms))
        .collect();
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
    // A stored root that does not parse can never be probed or matched by a
    // proof event; deferring it would warn-and-retry on every sweep forever.
    // Such a record is resolved terminally right here instead.
    let mut probeable: Vec<(BlockRecord, Hash256)> = Vec::with_capacity(stuck.len());
    for record in stuck {
        match record.new_payload_request_root.parse::<Hash256>() {
            Ok(root) => probeable.push((record, root)),
            Err(error) => {
                resolve_unprobeable_record(store, spans, &record, &error.to_string()).await;
            }
        }
    }
    if probeable.is_empty() {
        return;
    }
    let probed: Vec<(BlockRecord, bool)> = futures::stream::iter(probeable)
        .map(|(record, root)| async move {
            let judgeable = probe_record(zkboost, store, spans, &record, root).await;
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
    // A zero anchor here means the streak ended between the re-check above
    // and this load; liveness is not established, so defer everything. The
    // sweep is stopped and awaited before the watcher zeroes the anchor, so
    // this should be unreachable — it is kept as defense in depth.
    let anchor_ms = liveness.load(Ordering::Acquire);
    if anchor_ms == 0 {
        info!("liveness anchor cleared during the sweep; deferring silence verdicts");
        return;
    }
    let alive_streak_ms = monotonic_ms().saturating_sub(anchor_ms);
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
            alive_streak_ms,
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

/// Whether a record has waited past the sweep's probe age floor (see the
/// comment in [`reconcile`]). Ages from the record's earliest submission; a
/// record with no proofs (which the unresolved filter never yields) counts
/// as old enough rather than silently unprobeable.
fn old_enough_to_probe(record: &BlockRecord, now_ms: u64, floor_ms: u64) -> bool {
    record
        .requested_at_ms()
        .is_none_or(|requested| now_ms.saturating_sub(requested) >= floor_ms)
}

/// Resolves every still-sent proof of a record whose stored request root
/// does not parse — such a record can never be probed and no proof event can
/// ever match it, so deferring it (the previous behavior) meant warn-spam on
/// every sweep forever. It gets one terminal `Failed`/`Unresolved` verdict
/// carrying the parse error instead. Recorded roots are rendered from parsed
/// hashes, so this only fires on a corrupted or hand-edited state file.
async fn resolve_unprobeable_record(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    record: &BlockRecord,
    detail: &str,
) {
    for proof in &record.proofs {
        if proof.outcome != Outcome::Sent {
            continue;
        }
        let failure = status::Failure {
            // The defect is in this side's stored record, not evidence about
            // the prover, so it files as a submit-stage failure.
            stage: FailureStage::Submit,
            reason: UNRESOLVED_REASON.to_owned(),
            error: format!(
                "stored request root '{}' is not a valid hash ({detail}); \
                 no proof event can ever resolve it",
                record.new_payload_request_root
            ),
        };
        match record_failure(
            store,
            spans,
            &record.new_payload_request_root,
            &proof.proof_type,
            failure,
        )
        .await
        {
            Ok(Some(_)) => {
                counter!(RECONCILE_ACTIONS, "verdict" => "unresolved").increment(1);
                warn!(
                    root = %record.new_payload_request_root,
                    proof_type = %proof.proof_type,
                    verdict = "unresolved",
                    "resolved a proof whose stored request root is unparseable"
                );
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    root = %record.new_payload_request_root,
                    proof_type = %proof.proof_type,
                    %error,
                    "failed to resolve an unparseable-root record; it will be retried next sweep"
                );
            }
        }
    }
}

/// Probes one record's per-root subscription for replayed terminal outcomes.
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
    root: Hash256,
) -> bool {
    let root_hex = record.new_payload_request_root.as_str();
    let pending: HashSet<String> = record
        .proofs
        .iter()
        .filter(|proof| proof.outcome == Outcome::Sent)
        .map(|proof| proof.proof_type.clone())
        .collect();

    // Replays arrive immediately on connect. The processor owns the deadline
    // so an elapsed window can finalize a buffered failure instead of
    // cancellation dropping it.
    apply_probe_events(
        store,
        spans,
        root_hex,
        pending,
        RECONCILE_PROBE_TIMEOUT,
        zkboost.subscribe_root_events(root),
    )
    .await
}

/// Applies terminal outcomes from a per-root probe stream until every pending
/// proof type resolves or the probe deadline elapses.
///
/// Returns whether the record may face the silence policy afterward: `false`
/// means the stream erred (zkBoost was not observed) or a store write failed
/// (an outcome exists but was not applied — judging that proof silent
/// would write off work known to have finished).
///
/// Replayed failures are buffered rather than written immediately. zkBoost's
/// replay is latest-wins but can race a retry that has already completed: a
/// stale failure may be emitted just before the new completion. Holding the
/// failure for the bounded probe window lets that completion win without
/// weakening the store's global first-terminal-wins invariant.
async fn apply_probe_events(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    root_hex: &str,
    mut pending: HashSet<String>,
    probe_timeout: Duration,
    events: impl Stream<Item = Result<ProofEvent>> + Send,
) -> bool {
    let mut events = pin!(events);
    let deadline = tokio::time::Instant::now() + probe_timeout;
    let mut buffered_failures = HashMap::new();
    while !pending.is_empty() {
        let event = match tokio::time::timeout_at(deadline, events.next()).await {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(error))) => {
                warn!(root = %root_hex, %error, "reconciliation probe stream error");
                return false;
            }
            // Exhaustion without an error and an elapsed healthy window both
            // finalize any replayed failures collected so far. Liveness
            // checks around the whole probe distinguish healthy silence from
            // a connection that never became usable.
            Ok(None) | Err(_) => break,
        };
        match event {
            ProofEvent::ProofComplete(complete) => {
                if complete.new_payload_request_root.to_string() != root_hex {
                    continue;
                }
                let proof_type = complete.proof_type.as_str();
                if !pending.contains(proof_type) {
                    continue;
                }
                buffered_failures.remove(proof_type);
                // A replayed completion counts as handled — and leaves
                // `pending` — only once its store write is confirmed (or the
                // live stream demonstrably beat this one to it).
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
            ProofEvent::ProofFailure(failure) => {
                if failure.new_payload_request_root.to_string() != root_hex {
                    continue;
                }
                let proof_type = failure.proof_type.as_str();
                if pending.contains(proof_type) {
                    buffered_failures.insert(proof_type.to_owned(), failure);
                }
            }
        }
    }

    for (proof_type, failure) in buffered_failures {
        if !pending.contains(&proof_type) {
            continue;
        }
        let detail = status::Failure {
            stage: FailureStage::Proving,
            reason: format!("{:?}", failure.reason),
            error: failure.error,
        };
        match record_failure(store, spans, root_hex, &proof_type, detail).await {
            Ok(Some(_)) => {
                pending.remove(&proof_type);
                counter!(RECONCILE_ACTIONS, "verdict" => "failed").increment(1);
                warn!(
                    root = %root_hex,
                    proof_type,
                    verdict = "failed",
                    "reconciled proof from replayed failure"
                );
            }
            // The live stream resolved it while this probe was open.
            Ok(None) => {
                pending.remove(&proof_type);
            }
            Err(error) => {
                warn!(root = %root_hex, proof_type, %error, "failed to record a replayed failure");
                return false;
            }
        }
    }

    true
}

/// Applies the silence policy to a record's still-sent proofs.
///
/// A proof's observed silence is the overlap of two windows: how long it has
/// been waiting since submission (wall clock — `requested_at_ms` is persisted
/// with the record, so it spans restarts) and how long zkBoost has been
/// continuously observed alive (`alive_streak_ms`, monotonic — an NTP step or
/// VM pause cannot stretch it). Time zkBoost spent unreachable proves nothing
/// about a proof; completions from such a window are recovered by the probe
/// replay instead, and a zero streak defers every verdict by construction.
/// Proofs whose silence is shorter than the cutoff are left alone (zkBoost
/// may still be queueing or proving them), while the rest resolve
/// `Failed`/`Unresolved` — their outcome event is gone (missed while
/// disconnected, or orphaned by a zkBoost restart), no completion is cached,
/// and no event will ever arrive.
async fn resolve_silent_proofs(
    store: &Arc<dyn StatusStore>,
    spans: &SpanRegistry,
    record: &BlockRecord,
    cutoff: Duration,
    now_ms: u64,
    alive_streak_ms: u64,
) -> Result<()> {
    let cutoff_ms = u64::try_from(cutoff.as_millis()).unwrap_or(u64::MAX);
    for proof in &record.proofs {
        if proof.outcome != Outcome::Sent {
            continue;
        }
        let waited_ms = now_ms.saturating_sub(proof.requested_at_ms);
        let silent_ms = waited_ms.min(alive_streak_ms);
        if silent_ms < cutoff_ms {
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
                silent_ms,
                "reconciled proof as unresolved"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use zkboost_client::{FailureReason, ProofComplete, ProofFailure};

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
            .expect("read records")
            .into_iter()
            .next()
            .expect("one record")
    }

    fn proof_failure(root: Hash256, error: &str) -> ProofEvent {
        ProofEvent::ProofFailure(ProofFailure {
            new_payload_request_root: root,
            proof_type: zkboost::parse_proof_type("reth-zisk").expect("valid proof type"),
            reason: FailureReason::ProvingError,
            error: error.to_owned(),
        })
    }

    #[tokio::test]
    async fn reconciliation_marks_stale_sent_proofs_unresolved() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // 180s cutoff, and the proof has been silent for exactly that long
        // with zkBoost observed alive throughout the wait.
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(180),
            181_000,
            180_000,
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
            1_000_000,
        )
        .await
        .expect("silence policy");

        assert_eq!(stored_record(&store).await.outcome(), Outcome::Sent);
    }

    #[tokio::test]
    async fn zero_liveness_streak_defers_every_verdict() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // Ancient silence, but zkBoost has no observed-liveness streak at
        // all: nothing may be judged — a missing streak must never read as
        // "alive since forever".
        resolve_silent_proofs(
            &store,
            &span_registry(),
            &record,
            Duration::from_secs(180),
            999_000_000,
            0,
        )
        .await
        .expect("silence policy");

        assert_eq!(stored_record(&store).await.outcome(), Outcome::Sent);
    }

    #[tokio::test]
    async fn silence_is_bounded_by_the_liveness_streak_not_submission_age() {
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
            100_000,
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
            300_000,
        )
        .await
        .expect("silence policy");
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Failed);
    }

    /// A store whose writes fail, for exercising deferred-judgment paths.
    struct FailingStore;

    #[async_trait::async_trait]
    impl StatusStore for FailingStore {
        async fn seen(&self, _root: &str) -> Result<bool> {
            Ok(false)
        }

        async fn record(&self, _record: BlockRecord) -> Result<Vec<RetentionEviction>> {
            anyhow::bail!("store write failed")
        }

        async fn resolve_proof(
            &self,
            _root: &str,
            _proof_type: &str,
            _outcome: Outcome,
            _failure: Option<status::Failure>,
        ) -> Result<ResolveOutcome> {
            anyhow::bail!("store write failed")
        }

        async fn latest_slot(&self) -> Result<Option<u64>> {
            Ok(None)
        }

        async fn inflight_proofs(&self) -> Result<usize> {
            Ok(0)
        }

        async fn unresolved_records(&self) -> Result<Vec<BlockRecord>> {
            Ok(Vec::new())
        }

        async fn records(&self) -> Result<Vec<BlockRecord>> {
            Ok(Vec::new())
        }

        async fn records_page(
            &self,
            _cursor: Option<&status::RecordCursor>,
            _filter: status::RecordFilter,
            _limit: usize,
        ) -> Result<status::RecordPage> {
            anyhow::bail!("store read failed")
        }

        async fn summary(&self) -> Result<status::StatusSummary> {
            anyhow::bail!("store read failed")
        }

        async fn storage_stats(&self) -> Result<Option<status::StorageStats>> {
            anyhow::bail!("store read failed")
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
            Duration::from_secs(1),
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
            Duration::from_secs(1),
            replay,
        )
        .await;

        assert!(observed);
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Complete);
    }

    #[tokio::test]
    async fn replayed_failure_resolves_failed_when_no_completion_follows() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        let root: Hash256 = record.new_payload_request_root.parse().expect("valid root");
        // A real SSE subscription stays open after replaying the cached
        // failure. Keep this stream pending so the probe deadline, rather
        // than stream exhaustion, is what finalizes the buffered failure.
        let replay = futures::stream::iter(vec![Ok(proof_failure(root, "proving exploded"))])
            .chain(futures::stream::pending::<Result<ProofEvent>>());
        let pending: HashSet<String> = ["reth-zisk".to_string()].into();
        let observed = apply_probe_events(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            pending,
            Duration::from_millis(10),
            replay,
        )
        .await;

        assert!(observed);
        let stored = stored_record(&store).await;
        assert_eq!(stored.outcome(), Outcome::Failed);
        let proof = stored.proofs.first().expect("one proof");
        assert_eq!(proof.stage, Some(FailureStage::Proving));
        assert_eq!(proof.reason.as_deref(), Some("ProvingError"));
        assert_eq!(proof.error.as_deref(), Some("proving exploded"));
    }

    #[tokio::test]
    async fn completion_supersedes_a_stale_replayed_failure() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        let root: Hash256 = record.new_payload_request_root.parse().expect("valid root");
        let proof_type = zkboost::parse_proof_type("reth-zisk").expect("valid proof type");
        let replay = futures::stream::iter(vec![
            Ok(proof_failure(root, "stale retry failure")),
            Ok(ProofEvent::ProofComplete(ProofComplete {
                new_payload_request_root: root,
                proof_type,
            })),
        ]);
        let pending: HashSet<String> = ["reth-zisk".to_string()].into();
        let observed = apply_probe_events(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            pending,
            Duration::from_secs(1),
            replay,
        )
        .await;

        assert!(observed);
        let stored = stored_record(&store).await;
        assert_eq!(stored.outcome(), Outcome::Complete);
        let proof = stored.proofs.first().expect("one proof");
        assert_eq!(proof.reason, None);
        assert_eq!(proof.error, None);
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
            Duration::from_secs(1),
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
            .transitioned()
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
            Duration::from_secs(1),
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
            1,
        )
        .await
        .expect("silence policy");

        let stored = stored_record(&store).await;
        assert_eq!(stored.outcome(), Outcome::Complete);
        let proof = stored.proofs.first().expect("one proof");
        assert_eq!(proof.resolved_at_ms, resolved_at);
        assert_eq!(proof.reason, None);
    }

    #[tokio::test]
    async fn late_completion_after_a_verdict_is_discarded_not_applied() {
        let store = memory_store();
        let record = sent_record(1_000);
        store.record(record.clone()).await.expect("record");

        // Reconciliation writes the proof off first (a false verdict).
        store
            .resolve_proof(
                &record.new_payload_request_root,
                "reth-zisk",
                Outcome::Failed,
                Some(status::Failure {
                    stage: FailureStage::Proving,
                    reason: UNRESOLVED_REASON.to_owned(),
                    error: "silent past the cutoff".to_owned(),
                }),
            )
            .await
            .expect("resolve proof")
            .transitioned()
            .expect("transitioned");

        // The real completion arriving afterwards must not flip the record
        // (single-transition rule), and must report that nothing transitioned
        // — note_late_event makes the discard observable on this path.
        let result = record_completion(
            &store,
            &span_registry(),
            &record.new_payload_request_root,
            "reth-zisk",
        )
        .await
        .expect("record completion");
        assert!(result.is_none());
        assert_eq!(stored_record(&store).await.outcome(), Outcome::Failed);
    }

    #[tokio::test]
    async fn unparseable_stored_root_is_resolved_once_not_deferred() {
        let store = memory_store();
        let mut record = sent_record(1_000);
        record.new_payload_request_root = "0xnot-a-root".to_string();
        store.record(record.clone()).await.expect("record");

        resolve_unprobeable_record(&store, &span_registry(), &record, "invalid hex").await;

        let stored = stored_record(&store).await;
        assert_eq!(stored.outcome(), Outcome::Failed);
        let proof = stored.proofs.first().expect("one proof");
        assert_eq!(proof.stage, Some(FailureStage::Submit));
        assert_eq!(proof.reason.as_deref(), Some(UNRESOLVED_REASON));
        assert!(
            proof
                .error
                .as_deref()
                .expect("error detail")
                .contains("not a valid hash")
        );

        // Terminal: later sweeps find nothing unresolved, so the record is
        // never revisited (no warn-spam, no re-judging).
        assert!(
            store
                .unresolved_records()
                .await
                .expect("read unresolved records")
                .is_empty()
        );
    }

    #[test]
    fn probe_age_floor_skips_young_records() {
        let record = sent_record(100_000);

        // Younger than the floor: skipped this sweep, probed on a later one.
        assert!(!old_enough_to_probe(&record, 130_000, 45_000));
        // At or past the floor: probed.
        assert!(old_enough_to_probe(&record, 145_000, 45_000));
        assert!(old_enough_to_probe(&record, 200_000, 45_000));
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
                slot: 1,
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
                slot: 1,
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
