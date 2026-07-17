// Shapes returned by the requestor's HTTP API. Pure data, no runtime — shared
// by the data layer (api.ts) and the pure helpers (format.ts).

export type Outcome = 'sent' | 'complete' | 'failed'

/** Which side a failure occurred on. */
export type FailureStage = 'submit' | 'proving'

/** Status of one requested proof (one proof type) for a block. */
export interface ProofRecord {
  /** Proof type requested (e.g. reth-zisk). */
  proof_type: string
  outcome: Outcome
  /** Which side the failure occurred on; present when failed. */
  stage: FailureStage | null
  /** Short failure category (e.g. WitnessTimeout); present when failed. */
  reason: string | null
  /** Free-form failure detail; present when failed. */
  error: string | null
  requested_at_ms: number
  resolved_at_ms: number | null
  /** Queue time inside zkBoost; not exposed by current proof events. */
  queue_ms: number | null
  /** Pure proving time inside zkBoost; not exposed by current proof events. */
  prove_ms: number | null
  /** 1-based attempt number; always 1 until submit retries exist. */
  attempt: number
}

/**
 * Block-level facts plus one ProofRecord per requested proof type. Block
 * outcome and timing are derived, worst-of, across the proofs — format.ts
 * mirrors the backend derivation.
 */
export interface BlockRecord {
  slot: number
  beacon_block_root: string
  execution_block_number: number
  /** Execution block hash — the key zkBoost uses for its per-block data. */
  execution_block_hash: string
  new_payload_request_root: string
  observed_at_ms: number
  /** OpenTelemetry trace id, when tracing was enabled and sampled. */
  trace_id: string | null
  /** Witness-generation time; not exposed by current proof events. */
  witness_ms: number | null
  proofs: ProofRecord[]
}

export interface StatusSummary {
  total: number
  sent: number
  complete: number
  failed: number
  latest_slot: number | null
}
