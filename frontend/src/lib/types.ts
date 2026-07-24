// Shapes returned by the requestor's HTTP API. Pure data, no runtime — shared
// by the data layer (api.ts) and the pure helpers (format.ts).

export type Outcome = 'sent' | 'complete' | 'failed'

/** Which side a failure occurred on. */
export type FailureStage = 'submit' | 'proving'

/** How proofessoor learned the terminal outcome. */
export type ResolutionSource = 'live' | 'reconciled'

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
  /** Queue time inside zkBoost. */
  queue_ms: number | null
  /** Proof-generation time inside zkBoost, after leaving its queue. */
  prove_ms: number | null
  /** Whether the terminal outcome arrived live or through reconciliation. */
  resolution_source: ResolutionSource | null
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
  /** Witness-generation time reported by zkBoost. */
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

export interface DashboardConfig {
  grafana_url: string | null
}
