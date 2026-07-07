//! CLI argument and configuration types for `proofessoor`.
//!
//! This module owns command-line parsing and the validation of protocol
//! concepts (block identifiers, proof types). It performs no network I/O.

use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use url::Url;

/// Top-level CLI for the clientless zkBoost proof requestor.
#[derive(Debug, Parser)]
#[command(
    name = "proofessoor",
    version,
    about = "Clientless execution-proof requestor for zkBoost"
)]
pub struct Cli {
    /// Log level when `RUST_LOG` is unset (error, warn, info, debug, trace).
    #[arg(long, global = true, default_value = "info")]
    pub log_level: String,

    /// Subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The supported subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Request a proof for a single beacon block, then exit.
    Request(RequestArgs),

    /// Continuously request proofs for new beacon blocks.
    Stream(StreamArgs),

    /// Check connectivity to zkBoost and validate requested proof types.
    Check(CheckArgs),

    /// Show recorded request status and per-block timing from a state directory.
    Status(StatusArgs),
}

/// Arguments for `proofessoor status`.
#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Directory containing the persisted status (status.json).
    #[arg(long)]
    pub state_dir: PathBuf,
}

/// Beacon API and zkBoost endpoints shared by `request` and `stream`.
#[derive(Debug, Args)]
pub struct EndpointArgs {
    /// Beacon API base URL (e.g. http://127.0.0.1:5052).
    #[arg(long, env = "PROOFESSOOR_BEACON_URL")]
    pub beacon_url: Url,

    /// zkBoost API base URL (e.g. http://127.0.0.1:3000).
    #[arg(long, env = "PROOFESSOOR_ZKBOOST_URL")]
    pub zkboost_url: Url,

    /// Extra HTTP header for the Beacon API in "Name: Value" form (e.g. an API
    /// key). Repeatable on the CLI; a single header via PROOFESSOOR_BEACON_HEADER.
    /// Blank entries are ignored, so an unset PROOFESSOOR_BEACON_HEADER is fine.
    #[arg(long, env = "PROOFESSOOR_BEACON_HEADER")]
    pub beacon_header: Vec<String>,
}

/// Arguments for `proofessoor request`.
#[derive(Debug, Args)]
pub struct RequestArgs {
    #[command(flatten)]
    pub endpoints: EndpointArgs,

    /// Comma-separated proof backends to request (e.g. reth-zisk,ethrex-sp1).
    #[arg(
        long,
        required = true,
        value_delimiter = ',',
        value_parser = parse_proof_type
    )]
    pub proof_types: Vec<ProofTypeName>,

    /// Beacon block to prove: head, genesis, finalized, justified, a slot, or a 0x root.
    #[arg(long, default_value = "head", value_parser = parse_block_id)]
    pub block_id: BlockId,

    /// Wait for each requested proof to complete or fail before exiting.
    #[arg(long)]
    pub wait: bool,

    /// Save completed proofs to --out-dir (requires --wait).
    #[arg(long)]
    pub download: bool,

    /// Verify completed proofs through zkBoost (requires --wait).
    #[arg(long)]
    pub verify: bool,

    /// Directory to save proofs in (default ./proofs; implies --download; requires --wait).
    #[arg(long)]
    pub out_dir: Option<PathBuf>,
}

/// Arguments for `proofessoor stream`.
#[derive(Debug, Args)]
pub struct StreamArgs {
    #[command(flatten)]
    pub endpoints: EndpointArgs,

    /// Comma-separated proof backends to request (e.g. reth-zisk,ethrex-sp1).
    #[arg(
        long,
        required = true,
        value_delimiter = ',',
        value_parser = parse_proof_type
    )]
    pub proof_types: Vec<ProofTypeName>,

    /// Maximum number of concurrent proof submissions. Conservative default of 1.
    #[arg(long, default_value_t = NonZeroUsize::MIN)]
    pub max_inflight: NonZeroUsize,

    /// Save completed proofs to --out-dir.
    #[arg(long)]
    pub download: bool,

    /// Verify completed proofs through zkBoost.
    #[arg(long)]
    pub verify: bool,

    /// Directory to save proofs in (default ./proofs; implies --download).
    #[arg(long)]
    pub out_dir: Option<PathBuf>,

    /// Directory for persistent request status (enables restart de-duplication).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Maximum proof requests to retain in the status registry (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub max_history: usize,

    /// Address to serve /health, Prometheus /metrics, and the dashboard API on
    /// (e.g. 127.0.0.1:9090).
    #[arg(long)]
    pub http_addr: Option<SocketAddr>,

    /// Directory of built dashboard assets to serve at / (e.g. frontend/dist).
    #[arg(long)]
    pub ui_dir: Option<PathBuf>,

    /// How long a submitted proof may stay unresolved before reconciliation
    /// (run when the proof-event stream reconnects) marks it failed with
    /// reason "Unresolved". Set it above zkBoost's witness_timeout +
    /// proof_timeout so a proof still being proven is never written off.
    /// Accepts seconds, or a value suffixed with s/m/h (e.g. 180, 300s, 5m).
    #[arg(long, default_value = "180s", value_parser = parse_duration)]
    pub reconcile_after: Duration,
}

/// Arguments for `proofessoor check`.
#[derive(Debug, Args)]
pub struct CheckArgs {
    /// zkBoost API base URL (e.g. http://127.0.0.1:3000).
    #[arg(long, env = "PROOFESSOOR_ZKBOOST_URL")]
    pub zkboost_url: Url,

    /// Optional proof backends to validate against the zkBoost configuration.
    #[arg(long, value_delimiter = ',', value_parser = parse_proof_type)]
    pub proof_types: Vec<ProofTypeName>,
}

/// A Beacon API block identifier.
///
/// Mirrors the `{block_id}` values accepted by `GET /eth/v2/beacon/blocks/{block_id}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockId {
    /// The current head block.
    Head,
    /// The genesis block.
    Genesis,
    /// The most recent finalized block.
    Finalized,
    /// The most recent justified block.
    Justified,
    /// A specific slot number.
    Slot(u64),
    /// A 0x-prefixed, 32-byte block root (normalized to lowercase).
    Root(String),
}

impl BlockId {
    /// Parses a Beacon API block identifier, validating its shape.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "head" => Ok(Self::Head),
            "genesis" => Ok(Self::Genesis),
            "finalized" => Ok(Self::Finalized),
            "justified" => Ok(Self::Justified),
            other => {
                if let Some(hex) = other.strip_prefix("0x") {
                    if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                        Ok(Self::Root(format!("0x{}", hex.to_ascii_lowercase())))
                    } else {
                        Err(format!(
                            "invalid block root '{other}': expected 0x followed by 64 hex characters"
                        ))
                    }
                } else if let Ok(slot) = other.parse::<u64>() {
                    Ok(Self::Slot(slot))
                } else {
                    Err(format!(
                        "invalid block id '{other}': expected head, genesis, finalized, justified, \
                         a slot number, or a 0x-prefixed 32-byte root"
                    ))
                }
            }
        }
    }

    /// Renders the identifier as the Beacon API `{block_id}` URL path segment.
    pub fn to_path_segment(&self) -> String {
        match self {
            Self::Head => "head".to_string(),
            Self::Genesis => "genesis".to_string(),
            Self::Finalized => "finalized".to_string(),
            Self::Justified => "justified".to_string(),
            Self::Slot(slot) => slot.to_string(),
            Self::Root(root) => root.clone(),
        }
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_path_segment())
    }
}

/// A validated zkBoost proof type identifier (e.g. `reth-zisk`).
///
/// Validation covers the textual shape only; checking a name against the
/// server's configured backends happens where the zkBoost API is reachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofTypeName(String);

impl ProofTypeName {
    /// Parses and validates a proof type name.
    pub fn parse(value: &str) -> Result<Self, String> {
        let name = value.trim();
        if name.is_empty() {
            return Err("proof type cannot be empty".to_string());
        }
        if !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(format!(
                "invalid proof type '{name}': expected lowercase letters, digits, and hyphens \
                 (e.g. reth-zisk)"
            ));
        }
        Ok(Self(name.to_string()))
    }

    /// Returns the proof type as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProofTypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// clap value parser for [`BlockId`].
fn parse_block_id(value: &str) -> Result<BlockId, String> {
    BlockId::parse(value)
}

/// clap value parser for [`ProofTypeName`].
fn parse_proof_type(value: &str) -> Result<ProofTypeName, String> {
    ProofTypeName::parse(value)
}

/// clap value parser for durations: plain seconds, or an integer suffixed
/// with `s` (seconds), `m` (minutes), or `h` (hours).
fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let (digits, unit_secs) = if let Some(digits) = value.strip_suffix('s') {
        (digits, 1)
    } else if let Some(digits) = value.strip_suffix('m') {
        (digits, 60)
    } else if let Some(digits) = value.strip_suffix('h') {
        (digits, 3600)
    } else {
        (value, 1)
    };
    let amount: u64 = digits.trim().parse().map_err(|_| {
        format!("invalid duration '{value}': expected seconds or an s/m/h-suffixed integer")
    })?;
    amount
        .checked_mul(unit_secs)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("duration '{value}' overflows"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_id_parses_named_identifiers() {
        assert_eq!(BlockId::parse("head"), Ok(BlockId::Head));
        assert_eq!(BlockId::parse("genesis"), Ok(BlockId::Genesis));
        assert_eq!(BlockId::parse("finalized"), Ok(BlockId::Finalized));
        assert_eq!(BlockId::parse("justified"), Ok(BlockId::Justified));
    }

    #[test]
    fn block_id_parses_slot_number() {
        assert_eq!(BlockId::parse("12345"), Ok(BlockId::Slot(12345)));
    }

    #[test]
    fn block_id_normalizes_root_to_lowercase() {
        let root = format!("0x{}", "AB".repeat(32));
        let expected = format!("0x{}", "ab".repeat(32));
        assert_eq!(BlockId::parse(&root), Ok(BlockId::Root(expected)));
    }

    #[test]
    fn block_id_rejects_malformed_root() {
        // Too short.
        assert!(BlockId::parse("0xdeadbeef").is_err());
        // Non-hex character.
        let bad = format!("0x{}", "zz".repeat(32));
        assert!(BlockId::parse(&bad).is_err());
    }

    #[test]
    fn block_id_rejects_unknown_identifier() {
        assert!(BlockId::parse("latest").is_err());
    }

    #[test]
    fn block_id_round_trips_through_path_segment() {
        for input in ["head", "genesis", "finalized", "justified", "999"] {
            let id = BlockId::parse(input).expect("valid block id");
            assert_eq!(id.to_path_segment(), input);
        }
    }

    #[test]
    fn proof_type_accepts_valid_names() {
        assert_eq!(
            ProofTypeName::parse("reth-zisk").map(|p| p.as_str().to_string()),
            Ok("reth-zisk".to_string())
        );
        assert_eq!(
            ProofTypeName::parse("  ethrex-sp1  ").map(|p| p.as_str().to_string()),
            Ok("ethrex-sp1".to_string())
        );
    }

    #[test]
    fn duration_parses_seconds_and_suffixes() {
        assert_eq!(parse_duration("180"), Ok(Duration::from_secs(180)));
        assert_eq!(parse_duration("300s"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(300)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_duration(" 90s "), Ok(Duration::from_secs(90)));
    }

    #[test]
    fn duration_rejects_malformed_values() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("1.5s").is_err());
        assert!(parse_duration("-5s").is_err());
        assert!(parse_duration("5d").is_err());
    }

    #[test]
    fn proof_type_rejects_invalid_names() {
        assert!(ProofTypeName::parse("").is_err());
        assert!(ProofTypeName::parse("   ").is_err());
        assert!(ProofTypeName::parse("Reth_Zisk").is_err());
        assert!(ProofTypeName::parse("reth zisk").is_err());
    }
}
