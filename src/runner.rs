//! Stream-mode orchestration.
//!
//! Consumes the Beacon API block event stream and, for each new non-optimistic
//! block, builds and submits a proof request under bounded concurrency, so
//! submission keeps pace with block arrival. A separate watcher task observes
//! zkBoost's proof events, records each proof's outcome in the status registry,
//! and optionally downloads/verifies completed proofs. The daemon stops on
//! SIGINT/SIGTERM.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ::metrics::{counter, gauge, histogram};
use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};
use zkboost_client::ProofType;

use crate::beacon::{self, BlockEvent};
use crate::config::{BlockId, StreamArgs};
use crate::metrics::{
    BLOCKS_OBSERVED, BLOCKS_SKIPPED, COMPLETION_DURATION, HEAD_LAG, INFLIGHT_REQUESTS,
    LATEST_REQUESTED_SLOT, LATEST_SEEN_SLOT, PROOF_COMPLETIONS, PROOF_FAILURES,
    PROOF_REQUEST_FAILURES, PROOF_REQUESTS, REQUEST_DURATION, REQUEST_STAGE_DURATION,
};
use crate::request;
use crate::status::{
    self, BlockRecord, FailureStage, JsonStatusStore, MemoryStatusStore, Outcome, StatusStore,
};
use crate::zkboost::{self, ProofEvent};

/// Delay before reconnecting after an event stream drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

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

    // Observe proof outcomes (and run artifact actions) independently of submission.
    let watcher = tokio::spawn(watch(zkboost.clone(), store.clone(), artifacts.clone()));

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
            tasks.spawn(async move {
                let _permit = permit;
                if let Err(error) =
                    process_block(&beacon, &zkboost, &proof_types, &store, &latest_requested, &event)
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

/// Fetches, builds, and submits the proof request for a single block event.
async fn process_block(
    beacon: &beacon::Client,
    zkboost: &zkboost::Client,
    proof_types: &[ProofType],
    store: &Arc<dyn StatusStore>,
    latest_requested: &AtomicU64,
    event: &BlockEvent,
) -> Result<()> {
    let observed_at_ms = status::now_ms();
    let start = Instant::now();
    let block_id = BlockId::Root(event.block.to_string());
    let fetched = beacon.get_block(&block_id).await?;

    let build_start = Instant::now();
    let payload_request = request::build(fetched.block())?;
    let local_root = request::root(&payload_request);
    let root_hex = local_root.to_string();
    histogram!(REQUEST_STAGE_DURATION, "stage" => "build")
        .record(build_start.elapsed().as_secs_f64());

    // Skip blocks already requested (in this run or a previous one).
    if store.seen(&root_hex).await {
        counter!(BLOCKS_SKIPPED).increment(1);
        info!(slot = fetched.slot(), root = %local_root, "request already recorded; skipping");
        return Ok(());
    }

    let submit_start = Instant::now();
    let server_root = match zkboost.request_proof(&payload_request, proof_types).await {
        Ok(root) => root,
        Err(error) => {
            // Record the submit failure (often transient) rather than dropping it,
            // so the attempt shows as a failure instead of an absent slot. zkBoost
            // owns retry coordination, so the request is not auto-resubmitted here.
            counter!(PROOF_REQUEST_FAILURES).increment(1);
            store
                .record(failed_record(
                    &fetched,
                    payload_request.block_number(),
                    payload_request.block_hash().to_string(),
                    root_hex.clone(),
                    proof_types,
                    observed_at_ms,
                    "SubmitError",
                    format!("{error:#}"),
                ))
                .await?;
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
        store
            .record(failed_record(
                &fetched,
                payload_request.block_number(),
                payload_request.block_hash().to_string(),
                root_hex.clone(),
                proof_types,
                observed_at_ms,
                "RootMismatch",
                format!("local {local_root} != server {server_root}"),
            ))
            .await?;
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
    gauge!(INFLIGHT_REQUESTS).increment(proof_types.len() as f64);
    gauge!(LATEST_REQUESTED_SLOT).set(fetched.slot() as f64);
    histogram!(REQUEST_DURATION).record(start.elapsed().as_secs_f64());

    store
        .record(BlockRecord::new(
            fetched.slot(),
            fetched.root().to_string(),
            payload_request.block_number(),
            payload_request.block_hash().to_string(),
            root_hex,
            proof_types.iter().map(|p| p.as_str().to_string()).collect(),
            observed_at_ms,
        ))
        .await?;

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
async fn watch(
    zkboost: Arc<zkboost::Client>,
    store: Arc<dyn StatusStore>,
    artifacts: Arc<zkboost::Artifacts>,
) {
    loop {
        let mut events = Box::pin(zkboost.subscribe_proof_events());
        while let Some(event) = events.next().await {
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    warn!(%error, "proof event stream error; reconnecting");
                    break;
                }
            };
            if let Err(error) = handle_proof_event(&zkboost, &store, &artifacts, event).await {
                warn!(%error, "failed to handle proof event");
            }
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
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
    event: ProofEvent,
) -> Result<()> {
    match event {
        ProofEvent::ProofComplete(complete) => {
            let root_hex = complete.new_payload_request_root.to_string();
            let Some(resolution) = store
                .resolve_proof(
                    &root_hex,
                    complete.proof_type.as_str(),
                    Outcome::Complete,
                    None,
                )
                .await?
            else {
                return Ok(());
            };
            let proof_type = complete.proof_type.to_string();
            counter!(PROOF_COMPLETIONS, "proof_type" => proof_type.clone()).increment(1);
            gauge!(INFLIGHT_REQUESTS).decrement(1.0);
            histogram!(COMPLETION_DURATION, "proof_type" => proof_type)
                .record(resolution.duration_ms as f64 / 1000.0);
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
            let Some(_resolution) = store
                .resolve_proof(
                    &root_hex,
                    failure.proof_type.as_str(),
                    Outcome::Failed,
                    Some(status::Failure {
                        stage: FailureStage::Proving,
                        reason: reason.clone(),
                        error: failure.error.clone(),
                    }),
                )
                .await?
            else {
                return Ok(());
            };
            // Metrics stay labeled by the low-cardinality reason only; the
            // free-form error text is kept on the record, never as a label.
            let proof_type = failure.proof_type.to_string();
            counter!(PROOF_FAILURES, "proof_type" => proof_type, "reason" => reason).increment(1);
            gauge!(INFLIGHT_REQUESTS).decrement(1.0);
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
