//! Conversion from a beacon block to a zkBoost `NewPayloadRequest`.
//!
//! This is the protocol-critical step. It extracts the execution payload and,
//! for post-Deneb forks, the blob versioned hashes, parent beacon block root,
//! and execution requests, producing the spec-mirrored SSZ type zkBoost
//! accepts. Since zkBoost v0.9.0 the wire types mirror the execution-specs
//! stateless schema (plain byte arrays and bounded lists, no consensus-client
//! types), so lighthouse's decoded block is converted field by field here; the
//! golden test locks the resulting encoding against drift. The
//! `hash_tree_root` of the built value is the `new_payload_request_root`
//! zkBoost uses to identify the request.

use anyhow::{Result, anyhow, bail};
use lighthouse_types as lh;
use sha2::{Digest, Sha256};
use zkboost_types::{
    Bloom, ConsolidationRequest, DepositRequest, ExecutionPayloadV1, ExecutionPayloadV2,
    ExecutionPayloadV3, ExecutionRequests, Hash256, HashTreeRoot, NewPayloadRequest,
    NewPayloadRequestBellatrix, NewPayloadRequestCapella, NewPayloadRequestDeneb,
    NewPayloadRequestElectraFulu, Sha2Hasher, SszEncode, SszList, Transaction, Transactions,
    VersionedHash, VersionedHashes, Withdrawal, Withdrawals,
};

/// EIP-4844 versioned hash version byte for KZG commitments.
const VERSIONED_HASH_VERSION_KZG: u8 = 0x01;

/// Expands to a spec execution-payload struct literal, converting the common
/// (V1) field set from a lighthouse payload binding, plus the per-version
/// extra fields given by the caller. A macro rather than a function because
/// the lighthouse payload types share their field set without a common trait.
macro_rules! convert_payload {
    ($p:ident => $out:ident { $($field:ident: $value:expr),* $(,)? }) => {
        $out {
            parent_hash: $p.parent_hash.0.0,
            fee_recipient: $p.fee_recipient.0.0,
            state_root: $p.state_root.0,
            receipts_root: $p.receipts_root.0,
            logs_bloom: bloom(&$p.logs_bloom)?,
            prev_randao: $p.prev_randao.0,
            block_number: $p.block_number,
            gas_limit: $p.gas_limit,
            gas_used: $p.gas_used,
            timestamp: $p.timestamp,
            extra_data: byte_list(&$p.extra_data, "extra_data")?,
            base_fee_per_gas: $p.base_fee_per_gas.to_le_bytes(),
            block_hash: $p.block_hash.0.0,
            transactions: transactions(&$p.transactions)?,
            $($field: $value,)*
        }
    };
}

/// Builds the zkBoost `NewPayloadRequest` for a decoded beacon block.
///
/// Returns an error for forks without an execution payload (pre-Bellatrix) and
/// for forks not yet supported by zkBoost (Gloas).
pub fn build(block: &lh::SignedBeaconBlock<lh::MainnetEthSpec>) -> Result<NewPayloadRequest> {
    Ok(match block.message() {
        lh::BeaconBlockRef::Base(_) | lh::BeaconBlockRef::Altair(_) => {
            bail!("pre-Bellatrix blocks have no execution payload to prove")
        }
        lh::BeaconBlockRef::Bellatrix(b) => {
            let payload = &b.body.execution_payload.execution_payload;
            NewPayloadRequest::Bellatrix(NewPayloadRequestBellatrix {
                execution_payload: convert_payload!(payload => ExecutionPayloadV1 {}),
            })
        }
        lh::BeaconBlockRef::Capella(b) => {
            let payload = &b.body.execution_payload.execution_payload;
            NewPayloadRequest::Capella(NewPayloadRequestCapella {
                execution_payload: convert_payload!(payload => ExecutionPayloadV2 {
                    withdrawals: withdrawals(&payload.withdrawals)?,
                }),
            })
        }
        lh::BeaconBlockRef::Deneb(b) => {
            let payload = &b.body.execution_payload.execution_payload;
            NewPayloadRequest::Deneb(NewPayloadRequestDeneb {
                execution_payload: convert_payload!(payload => ExecutionPayloadV3 {
                    withdrawals: withdrawals(&payload.withdrawals)?,
                    blob_gas_used: payload.blob_gas_used,
                    excess_blob_gas: payload.excess_blob_gas,
                }),
                versioned_hashes: versioned_hashes(&b.body.blob_kzg_commitments)?,
                parent_beacon_block_root: b.parent_root.0,
            })
        }
        lh::BeaconBlockRef::Electra(b) => {
            let payload = &b.body.execution_payload.execution_payload;
            NewPayloadRequest::ElectraFulu(NewPayloadRequestElectraFulu {
                execution_payload: convert_payload!(payload => ExecutionPayloadV3 {
                    withdrawals: withdrawals(&payload.withdrawals)?,
                    blob_gas_used: payload.blob_gas_used,
                    excess_blob_gas: payload.excess_blob_gas,
                }),
                versioned_hashes: versioned_hashes(&b.body.blob_kzg_commitments)?,
                parent_beacon_block_root: b.parent_root.0,
                execution_requests: execution_requests(&b.body.execution_requests)?,
            })
        }
        lh::BeaconBlockRef::Fulu(b) => {
            let payload = &b.body.execution_payload.execution_payload;
            NewPayloadRequest::ElectraFulu(NewPayloadRequestElectraFulu {
                execution_payload: convert_payload!(payload => ExecutionPayloadV3 {
                    withdrawals: withdrawals(&payload.withdrawals)?,
                    blob_gas_used: payload.blob_gas_used,
                    excess_blob_gas: payload.excess_blob_gas,
                }),
                versioned_hashes: versioned_hashes(&b.body.blob_kzg_commitments)?,
                parent_beacon_block_root: b.parent_root.0,
                execution_requests: execution_requests(&b.body.execution_requests)?,
            })
        }
        lh::BeaconBlockRef::Gloas(_) => bail!("Gloas execution proofs are not yet supported"),
    })
}

/// The `new_payload_request_root` identifying the request.
pub fn root(request: &NewPayloadRequest) -> Hash256 {
    Hash256::from(request.hash_tree_root(&Sha2Hasher))
}

/// The execution block hash of the request's payload, as a displayable hash.
pub fn block_hash(request: &NewPayloadRequest) -> Hash256 {
    Hash256::from(request.block_hash())
}

/// The length in bytes of the SSZ-encoded payload request inside the body
/// zkBoost receives.
pub fn ssz_len(request: &NewPayloadRequest) -> usize {
    request.encoded_len()
}

/// Converts a byte slice into the spec's bounded SSZ byte list.
fn byte_list<const N: usize>(bytes: &[u8], what: &str) -> Result<SszList<u8, N>> {
    SszList::try_from(bytes.to_vec()).map_err(|e| anyhow!("{what} exceeds the spec bound: {e:?}"))
}

/// Converts the payload's 256-byte logs bloom.
fn bloom(bytes: &[u8]) -> Result<Bloom> {
    Bloom::try_from(bytes).map_err(|_| anyhow!("logs_bloom is not 256 bytes"))
}

/// Converts the payload's transactions into the spec's nested SSZ lists.
fn transactions(txs: &lh::Transactions<lh::MainnetEthSpec>) -> Result<Transactions> {
    let txs: Vec<Transaction> = txs
        .iter()
        .map(|tx| byte_list(tx, "transaction"))
        .collect::<Result<_>>()?;
    SszList::try_from(txs).map_err(|e| anyhow!("too many transactions: {e:?}"))
}

/// Converts the payload's withdrawals into the spec's SSZ list.
fn withdrawals(withdrawals: &[lh::Withdrawal]) -> Result<Withdrawals> {
    let withdrawals: Vec<Withdrawal> = withdrawals
        .iter()
        .map(|w| Withdrawal {
            index: w.index,
            validator_index: w.validator_index,
            address: w.address.0.0,
            amount: w.amount,
        })
        .collect();
    SszList::try_from(withdrawals).map_err(|e| anyhow!("too many withdrawals: {e:?}"))
}

/// Converts the block's EIP-7685 execution requests into the spec's container.
fn execution_requests(
    requests: &lh::ExecutionRequestsElectra<lh::MainnetEthSpec>,
) -> Result<ExecutionRequests> {
    let deposits: Vec<DepositRequest> = requests
        .deposits
        .iter()
        .map(|d| DepositRequest {
            pubkey: d.pubkey.serialize(),
            withdrawal_credentials: d.withdrawal_credentials.0,
            amount: d.amount,
            signature: d.signature.serialize(),
            index: d.index,
        })
        .collect();
    let withdrawals: Vec<zkboost_types::WithdrawalRequest> = requests
        .withdrawals
        .iter()
        .map(|w| zkboost_types::WithdrawalRequest {
            source_address: w.source_address.0.0,
            validator_pubkey: w.validator_pubkey.serialize(),
            amount: w.amount,
        })
        .collect();
    let consolidations: Vec<ConsolidationRequest> = requests
        .consolidations
        .iter()
        .map(|c| ConsolidationRequest {
            source_address: c.source_address.0.0,
            source_pubkey: c.source_pubkey.serialize(),
            target_pubkey: c.target_pubkey.serialize(),
        })
        .collect();
    Ok(ExecutionRequests {
        deposits: SszList::try_from(deposits)
            .map_err(|e| anyhow!("too many deposit requests: {e:?}"))?,
        withdrawals: SszList::try_from(withdrawals)
            .map_err(|e| anyhow!("too many withdrawal requests: {e:?}"))?,
        consolidations: SszList::try_from(consolidations)
            .map_err(|e| anyhow!("too many consolidation requests: {e:?}"))?,
    })
}

/// Derives the blob versioned hashes from a block's KZG commitments.
fn versioned_hashes(commitments: &[lh::KzgCommitment]) -> Result<VersionedHashes> {
    let hashes: Vec<VersionedHash> = commitments
        .iter()
        .map(kzg_commitment_to_versioned_hash)
        .collect();
    SszList::try_from(hashes).map_err(|e| anyhow!("too many blob commitments: {e:?}"))
}

/// Computes the EIP-4844 versioned hash for a single KZG commitment.
fn kzg_commitment_to_versioned_hash(commitment: &lh::KzgCommitment) -> VersionedHash {
    let mut hash: [u8; 32] = Sha256::digest(commitment.0).into();
    hash[0] = VERSIONED_HASH_VERSION_KZG;
    hash
}

#[cfg(test)]
mod tests {
    use lh::{ForkName, ForkVersionDecode, SignedBeaconBlock};

    use super::*;

    /// A real Fulu beacon block from the hoodi testnet (slot 3326688).
    const FULU_BLOCK: &[u8] = include_bytes!("../tests/fixtures/hoodi_block_3326688_fulu.ssz");

    /// Golden test: the recorded block must produce a stable request root and
    /// execution metadata, locking the lighthouse-to-spec conversion against
    /// drift. The root is the one the pre-v0.9.0 lighthouse-typed encoding
    /// produced, so it also proves the wire encoding survived the type swap.
    #[test]
    fn builds_fulu_request_with_expected_root() {
        let block = SignedBeaconBlock::<lh::MainnetEthSpec>::from_ssz_bytes_by_fork(
            FULU_BLOCK,
            ForkName::Fulu,
        )
        .expect("fixture decodes as a fulu block");

        let request = build(&block).expect("builds a new payload request");

        assert_eq!(request.block_number(), 3067357);
        assert_eq!(request.gas_used(), 1981488);
        assert_eq!(
            root(&request).to_string(),
            "0xf1aaa504269559061901140a556e3dd10acafa0951e4b1b657fdf8cf2ed4fe27"
        );
    }
}
