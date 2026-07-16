//! Chain-config resolution for zkBoost proof requests and verifications.
//!
//! Since v0.9.0, zkBoost requires each proof request and verification to carry
//! a [`ChainConfig`] naming the block's active *execution* fork. The consensus
//! spec plus the genesis time fix an activation timestamp for every scheduled
//! fork (including the BPO blob-schedule forks, which change blob parameters
//! without a consensus fork name); a payload's active fork is the latest one
//! activated at or before its timestamp. This mirrors the reference
//! implementation in eth-act/lighthouse#40, consuming lighthouse's parsed
//! spec directly so config parsing and blob-schedule semantics stay
//! maintained upstream — only the mapping onto zkBoost's wire types lives
//! here.
//!
//! Deliberately one self-contained module: the upstream schema
//! (execution-specs `tests-zkevm`) has announced this shape will change again
//! in its next release.

use std::cmp::Reverse;

use anyhow::{Context, Result};
use lighthouse_types::{ChainSpec, Config, Epoch, EthSpec, MainnetEthSpec};
use zkboost_types::{BlobSchedule, ChainConfig, ForkActivation, ForkConfig, ProtocolFork};

/// The chain configs of every execution fork the consensus spec schedules,
/// ready for latest-active resolution by payload timestamp or beacon slot.
#[derive(Debug, Clone)]
pub struct ChainConfigSchedule {
    /// Sorted descending by (activation timestamp, fork discriminant), so
    /// resolution picks the newest fork among any activated at the same
    /// timestamp (a BPO entry landing exactly on a consensus fork's epoch).
    chain_configs: Vec<ChainConfig>,
    /// The execution chain id every config carries.
    chain_id: u64,
    /// Unix timestamp of the chain's genesis, for slot resolution.
    genesis_time: u64,
    /// Seconds per slot, for slot resolution.
    seconds_per_slot: u64,
}

impl ChainConfigSchedule {
    /// Builds the schedule from the node's spec config (`/eth/v1/config/spec`
    /// decoded as lighthouse's standard [`Config`]) and the genesis time.
    ///
    /// The [`ChainSpec`] derived from the config supplies fork epochs and
    /// per-epoch blob maxima; the raw config supplies the BPO schedule
    /// entries, which the spec type does not expose.
    pub fn new(config: &Config, genesis_time: u64) -> Result<Self> {
        let spec = ChainSpec::from_config::<MainnetEthSpec>(config)
            .context("beacon spec config is incompatible with the mainnet preset")?;
        let chain_id = spec.deposit_chain_id;
        let seconds_per_slot = spec.get_slot_duration().as_secs();
        let epoch_seconds = seconds_per_slot.saturating_mul(MainnetEthSpec::slots_per_epoch());
        let activation = |epoch: Epoch| {
            genesis_time.saturating_add(epoch.as_u64().saturating_mul(epoch_seconds))
        };

        let mut chain_configs = Vec::with_capacity(8);
        for (epoch, fork) in [
            (spec.bellatrix_fork_epoch, ProtocolFork::Paris),
            (spec.capella_fork_epoch, ProtocolFork::Shanghai),
            (spec.deneb_fork_epoch, ProtocolFork::Cancun),
            (spec.electra_fork_epoch, ProtocolFork::Prague),
            (spec.fulu_fork_epoch, ProtocolFork::Osaka),
            (spec.gloas_fork_epoch, ProtocolFork::Amsterdam),
        ] {
            // A fork left at the far-future sentinel is not scheduled.
            let Some(epoch) = epoch.filter(|&epoch| epoch != spec.far_future_epoch) else {
                continue;
            };
            let blob_max = (fork >= ProtocolFork::Cancun).then(|| spec.max_blobs_per_block(epoch));
            chain_configs.push(chain_config(chain_id, fork, activation(epoch), blob_max));
        }

        // Each BPO blob-schedule entry is its own execution fork. The
        // schedule is reverse-sorted by epoch, so walking it backwards pairs
        // the earliest change with BPO1. Only BPO1 and BPO2 exist on the wire
        // today (the spec is removing BPO3-5), so later entries are skipped,
        // like the reference implementation does.
        let bpo_entries = config.blob_schedule.as_vec();
        if bpo_entries.len() > 2 {
            tracing::warn!(
                entries = bpo_entries.len(),
                "blob schedule has more entries than wire BPO forks; ignoring the extras"
            );
        }
        for (entry, fork) in bpo_entries
            .iter()
            .rev()
            .zip([ProtocolFork::BPO1, ProtocolFork::BPO2])
        {
            chain_configs.push(chain_config(
                chain_id,
                fork,
                activation(entry.epoch),
                Some(entry.max_blobs_per_block),
            ));
        }

        chain_configs.sort_by_key(|config| {
            Reverse((
                activation_timestamp(config),
                config.active_fork.fork.as_u64(),
            ))
        });
        Ok(Self {
            chain_configs,
            chain_id,
            genesis_time,
            seconds_per_slot,
        })
    }

    /// The execution chain id every scheduled config carries.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Resolves the chain config active at an execution payload timestamp,
    /// or `None` before the first scheduled fork.
    pub fn resolve(&self, timestamp: u64) -> Option<ChainConfig> {
        self.chain_configs
            .iter()
            .find(|config| activation_timestamp(config) <= timestamp)
            .cloned()
    }

    /// Resolves the chain config active for a beacon slot. Post-merge, a
    /// block's execution payload timestamp equals its slot's timestamp, so
    /// this serves paths that hold a recorded slot instead of the payload.
    pub fn resolve_slot(&self, slot: u64) -> Option<ChainConfig> {
        let timestamp = self
            .genesis_time
            .saturating_add(slot.saturating_mul(self.seconds_per_slot));
        self.resolve(timestamp)
    }
}

/// Builds a [`ChainConfig`] for a fork activated at `timestamp`.
fn chain_config(
    chain_id: u64,
    fork: ProtocolFork,
    timestamp: u64,
    blob_max: Option<u64>,
) -> ChainConfig {
    ChainConfig {
        chain_id,
        active_fork: ForkConfig::new(
            fork,
            ForkActivation::new(None, Some(timestamp)),
            // `target` and `base_fee_update_fraction` stay 0 for the proof
            // node to fill in, matching the reference consensus-client
            // behavior; the spec plans to drop `BlobSchedule` entirely.
            blob_max.map(|max| BlobSchedule {
                target: 0,
                max,
                base_fee_update_fraction: 0,
            }),
        ),
    }
}

/// The activation timestamp a chain config was built for. Every config here
/// is constructed with a timestamp activation, so the fallback never fires.
fn activation_timestamp(config: &ChainConfig) -> u64 {
    config.active_fork.activation.timestamp().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use lighthouse_types::BlobParameters;

    use super::*;

    /// Mainnet's spec with Gloas scheduled one epoch after the last BPO, like
    /// the reference implementation's test, so every wire fork is exercised.
    fn scheduled_spec() -> ChainSpec {
        let mut spec = ChainSpec::mainnet();
        let latest_bpo = latest_bpo_epoch(&spec);
        spec.gloas_fork_epoch = Some(Epoch::new(latest_bpo.as_u64() + 1));
        spec
    }

    fn config_of(spec: &ChainSpec) -> Config {
        Config::from_chain_spec::<MainnetEthSpec>(spec)
    }

    fn schedule_of(spec: &ChainSpec) -> ChainConfigSchedule {
        ChainConfigSchedule::new(&config_of(spec), GENESIS).expect("mainnet config builds")
    }

    /// The BPO entries of a spec's blob schedule, earliest first.
    fn bpo_entries(spec: &ChainSpec) -> Vec<BlobParameters> {
        let config = config_of(spec);
        let mut entries = config.blob_schedule.as_vec().clone();
        entries.reverse();
        assert_eq!(
            entries.len(),
            2,
            "these tests assume mainnet schedules exactly BPO1 and BPO2"
        );
        entries
    }

    fn latest_bpo_epoch(spec: &ChainSpec) -> Epoch {
        bpo_entries(spec).last().expect("bpo entries").epoch
    }

    const GENESIS: u64 = 1_000_000;

    fn at_epoch(spec: &ChainSpec, epoch: Epoch) -> u64 {
        GENESIS
            + epoch.as_u64()
                * spec.get_slot_duration().as_secs()
                * MainnetEthSpec::slots_per_epoch()
    }

    /// The wire forks in activation order, paired with their epochs.
    fn wire_forks(spec: &ChainSpec) -> Vec<(ProtocolFork, Epoch)> {
        let bpos = bpo_entries(spec);
        let bpo_epoch = |index: usize| bpos.get(index).expect("two bpo entries").epoch;
        vec![
            (
                ProtocolFork::Paris,
                spec.bellatrix_fork_epoch.expect("scheduled"),
            ),
            (
                ProtocolFork::Shanghai,
                spec.capella_fork_epoch.expect("scheduled"),
            ),
            (
                ProtocolFork::Cancun,
                spec.deneb_fork_epoch.expect("scheduled"),
            ),
            (
                ProtocolFork::Prague,
                spec.electra_fork_epoch.expect("scheduled"),
            ),
            (
                ProtocolFork::Osaka,
                spec.fulu_fork_epoch.expect("scheduled"),
            ),
            (ProtocolFork::BPO1, bpo_epoch(0)),
            (ProtocolFork::BPO2, bpo_epoch(1)),
            (
                ProtocolFork::Amsterdam,
                spec.gloas_fork_epoch.expect("scheduled"),
            ),
        ]
    }

    #[test]
    fn resolves_each_fork_at_and_after_its_activation() {
        let spec = scheduled_spec();
        let schedule = schedule_of(&spec);

        let mut previous_fork: Option<ProtocolFork> = None;
        for (fork, epoch) in wire_forks(&spec) {
            let timestamp = at_epoch(&spec, epoch);
            let config = schedule.resolve(timestamp).expect("fork resolves");
            assert_eq!(config.active_fork.fork, fork, "at activation of {fork:?}");
            assert_eq!(config.chain_id, spec.deposit_chain_id);
            assert_eq!(config.active_fork.activation.timestamp(), Some(timestamp));
            // Just before this activation, the previous fork is active.
            if let Some(previous_fork) = previous_fork {
                let previous = schedule
                    .resolve(timestamp - 1)
                    .expect("previous fork resolves");
                assert_eq!(previous.active_fork.fork, previous_fork);
            }
            previous_fork = Some(fork);
        }
    }

    #[test]
    fn nothing_resolves_before_the_first_scheduled_fork() {
        let spec = scheduled_spec();
        let schedule = schedule_of(&spec);
        let bellatrix = at_epoch(&spec, spec.bellatrix_fork_epoch.expect("scheduled"));
        assert!(schedule.resolve(bellatrix - 1).is_none());
    }

    #[test]
    fn unscheduled_forks_are_skipped() {
        // Plain mainnet: Gloas is unscheduled, so the far future resolves to
        // the last BPO, not Amsterdam. The far-future sentinel behaves the
        // same as an absent epoch.
        let mut spec = ChainSpec::mainnet();
        assert_eq!(spec.gloas_fork_epoch, None);
        let config = schedule_of(&spec).resolve(u64::MAX).expect("fork resolves");
        assert_eq!(config.active_fork.fork, ProtocolFork::BPO2);

        spec.gloas_fork_epoch = Some(spec.far_future_epoch);
        let config = schedule_of(&spec).resolve(u64::MAX).expect("fork resolves");
        assert_eq!(config.active_fork.fork, ProtocolFork::BPO2);
    }

    #[test]
    fn blob_schedule_follows_the_active_fork() {
        let spec = scheduled_spec();
        let schedule = schedule_of(&spec);
        let max_at = |epoch: Epoch| {
            schedule
                .resolve(at_epoch(&spec, epoch))
                .expect("fork resolves")
                .active_fork
                .blob_schedule()
                .map(|blob| blob.max)
        };

        // Pre-Cancun forks carry no blob schedule; from Cancun on, each CL
        // fork carries the spec's per-epoch maximum (Fulu has no schedule
        // entry at its own epoch on mainnet, so Electra's carries over).
        assert_eq!(max_at(spec.capella_fork_epoch.expect("scheduled")), None);
        for epoch in [
            spec.deneb_fork_epoch.expect("scheduled"),
            spec.electra_fork_epoch.expect("scheduled"),
            spec.fulu_fork_epoch.expect("scheduled"),
        ] {
            assert_eq!(max_at(epoch), Some(spec.max_blobs_per_block(epoch)));
        }
        assert_eq!(
            max_at(spec.fulu_fork_epoch.expect("scheduled")),
            max_at(spec.electra_fork_epoch.expect("scheduled")),
        );
        // The BPO entries and Amsterdam take the scheduled maxima.
        let bpos = bpo_entries(&spec);
        for bpo in &bpos {
            assert_eq!(max_at(bpo.epoch), Some(bpo.max_blobs_per_block));
        }
        assert_eq!(
            max_at(spec.gloas_fork_epoch.expect("scheduled")),
            bpos.last().map(|bpo| bpo.max_blobs_per_block)
        );
    }

    #[test]
    fn resolve_slot_maps_slots_onto_the_timeline() {
        let spec = scheduled_spec();
        let schedule = schedule_of(&spec);

        // The first slot of Capella's activation epoch resolves to Shanghai;
        // one slot earlier is still Paris.
        let capella_slot = spec.capella_fork_epoch.expect("scheduled").as_u64()
            * MainnetEthSpec::slots_per_epoch();
        let config = schedule.resolve_slot(capella_slot).expect("fork resolves");
        assert_eq!(config.active_fork.fork, ProtocolFork::Shanghai);
        let config = schedule
            .resolve_slot(capella_slot - 1)
            .expect("fork resolves");
        assert_eq!(config.active_fork.fork, ProtocolFork::Paris);
    }

    #[test]
    fn a_bpo_entry_on_a_consensus_fork_epoch_wins_resolution() {
        let spec = scheduled_spec();
        // A blob-schedule entry exactly at Fulu's activation epoch: both
        // forks activate at the same timestamp, and the newer (BPO1) must win.
        let fulu = spec.fulu_fork_epoch.expect("scheduled");
        let mut config = config_of(&spec);
        config.blob_schedule = lighthouse_types::BlobSchedule::new(vec![BlobParameters {
            epoch: fulu,
            max_blobs_per_block: 12,
        }]);
        let schedule = ChainConfigSchedule::new(&config, GENESIS).expect("mainnet config builds");

        let resolved = schedule
            .resolve(at_epoch(&spec, fulu))
            .expect("fork resolves");
        assert_eq!(resolved.active_fork.fork, ProtocolFork::BPO1);
        assert_eq!(
            resolved.active_fork.blob_schedule().map(|blob| blob.max),
            Some(12)
        );
    }
}
