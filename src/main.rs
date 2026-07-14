//! `proofessoor` — a minimal, clientless execution-proof requestor for zkBoost.
//!
//! The binary parses and validates the CLI, initializes logging, and dispatches
//! to the requested subcommand.

mod beacon;
mod config;
mod metrics;
#[cfg(feature = "otel")]
mod otel;
mod request;
mod runner;
mod status;
mod web;
mod zkboost;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

use crate::config::{CheckArgs, Cli, Command, RequestArgs, StatusArgs};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    #[cfg(feature = "otel")]
    let (otel_provider, otel_layer) = otel::init()?;
    #[cfg(not(feature = "otel"))]
    let otel_layer: Option<tracing_subscriber::layer::Identity> = None;

    init_tracing(&cli.log_level, otel_layer)?;

    let result = match cli.command {
        Command::Request(args) => run_request(args).await,
        Command::Stream(args) => runner::run(args).await,
        Command::Check(args) => run_check(args).await,
        Command::Status(args) => run_status(args).await,
    };

    // Flush batched spans before exit; without this the tail of a run is lost.
    #[cfg(feature = "otel")]
    if let Some(provider) = otel_provider
        && let Err(error) = provider.shutdown()
    {
        tracing::warn!(%error, "otel provider shutdown failed");
    }

    result
}

/// Initializes the global tracing subscriber: the optional OpenTelemetry
/// layer (compiled in by the `otel` feature and active only when an OTLP
/// endpoint is configured) composed with the usual fmt output.
///
/// `RUST_LOG` takes precedence; otherwise the `--log-level` value is used.
fn init_tracing(
    log_level: &str,
    otel_layer: Option<impl Layer<Registry> + Send + Sync>,
) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .with_context(|| format!("invalid log level '{log_level}'"))?;

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(filter)
        .init();

    Ok(())
}

/// Handles the `request` subcommand.
///
/// Fetches the requested beacon block, builds the zkBoost payload request,
/// submits it for proving, and reports the resulting `new_payload_request_root`.
async fn run_request(args: RequestArgs) -> Result<()> {
    let artifacts = zkboost::Artifacts {
        download: args.download,
        verify: args.verify,
        out_dir: args.out_dir.clone(),
    };
    if artifacts.needs_proof_bytes() && !args.wait {
        anyhow::bail!("--download, --verify, and --out-dir require --wait");
    }

    let beacon = beacon::Client::new(
        args.endpoints.beacon_url.clone(),
        &args.endpoints.beacon_header,
    )?;
    let zkboost = zkboost::Client::new(args.endpoints.zkboost_url.clone())?;
    let proof_types = args
        .proof_types
        .iter()
        .map(|name| zkboost::parse_proof_type(name.as_str()))
        .collect::<Result<Vec<_>>>()?;

    let block = beacon.get_block(&args.block_id).await?;
    let payload_request = request::build(block.block())?;
    let local_root = request::root(&payload_request);

    let server_root = zkboost
        .request_proof(&payload_request, &proof_types)
        .await?;

    // The server recomputes the root from the submitted SSZ body; a mismatch means
    // the local encoding disagrees with zkBoost's and the request is not the one built.
    if server_root != local_root {
        anyhow::bail!(
            "new_payload_request_root mismatch: local {local_root} != server {server_root}"
        );
    }

    tracing::info!(
        slot = block.slot(),
        beacon_block_root = %block.root(),
        fork = %block.fork(),
        execution_block_hash = %payload_request.block_hash(),
        execution_block_number = payload_request.block_number(),
        new_payload_request_root = %server_root,
        request_bytes = request::ssz_len(&payload_request),
        proof_types = %render_proof_types(&args.proof_types),
        "proof requested"
    );

    if args.wait {
        zkboost
            .wait_for_proofs(server_root, &proof_types, &artifacts)
            .await?;
    }
    Ok(())
}

/// Handles the `check` subcommand.
///
/// Confirms zkBoost is reachable, reports the provable proof types, and fails
/// if any requested proof type is not available.
async fn run_check(args: CheckArgs) -> Result<()> {
    let client = zkboost::Client::new(args.zkboost_url.clone())?;
    let available = client.proof_types().await?;

    let provable: Vec<&str> = available
        .iter()
        .filter(|info| info.can_prove)
        .map(|info| info.proof_type.as_str())
        .collect();

    tracing::info!(
        zkboost_url = %args.zkboost_url,
        provable = %provable.join(","),
        "zkBoost reachable"
    );

    if args.proof_types.is_empty() {
        return Ok(());
    }

    let missing: Vec<&str> = args
        .proof_types
        .iter()
        .map(config::ProofTypeName::as_str)
        .filter(|name| !provable.contains(name))
        .collect();

    if missing.is_empty() {
        tracing::info!(
            requested = %render_proof_types(&args.proof_types),
            "all requested proof types are available"
        );
        Ok(())
    } else {
        anyhow::bail!(
            "requested proof types not available on zkBoost: {}",
            missing.join(",")
        )
    }
}

/// Handles the `status` subcommand: print recorded requests and per-block timing.
async fn run_status(args: StatusArgs) -> Result<()> {
    let records = status::read_records(&args.state_dir).await?;
    if records.is_empty() {
        println!("no recorded requests in {}", args.state_dir.display());
        return Ok(());
    }

    println!(
        "{:<10} {:<10} {:<9} {:>8} {:>8} {:>8}  root",
        "slot", "exec#", "outcome", "prep", "zkboost", "e2e"
    );
    for record in &records {
        let fmt_ms =
            |value: Option<u64>| value.map_or_else(|| "-".to_string(), |ms| format!("{ms}ms"));
        // Only the short reason category goes inline; the free-form error text
        // would blow out the column layout and stays in the API and status.json.
        // The line shows the block's derived (worst-of) outcome and the first
        // failed proof's reason; per-proof detail lives in the API.
        let failure = record
            .failure_reason()
            .map(|reason| format!("  {reason}"))
            .unwrap_or_default();
        println!(
            "{:<10} {:<10} {:<9} {:>8} {:>8} {:>8}  {}{}",
            record.slot,
            record.execution_block_number,
            record.outcome().as_str(),
            format!("{}ms", record.prep_ms()),
            fmt_ms(record.completion_ms()),
            fmt_ms(record.end_to_end_ms()),
            record.new_payload_request_root,
            failure,
        );
    }
    Ok(())
}

/// Renders proof types as a comma-separated string for logging.
fn render_proof_types(proof_types: &[config::ProofTypeName]) -> String {
    proof_types
        .iter()
        .map(config::ProofTypeName::as_str)
        .collect::<Vec<_>>()
        .join(",")
}
