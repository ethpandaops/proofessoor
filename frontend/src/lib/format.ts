// Pure derivations over BlockRecords — no DOM, no fetch, no reactivity — so the
// timing math and the cadence/scaling logic can be unit-tested in isolation.

import type { BlockRecord, Outcome, ProofRecord } from './types'

/**
 * Derived block outcome, worst-of across proofs — mirrors the backend: any
 * failed proof fails the block; otherwise any unresolved proof keeps it in
 * flight; otherwise every proof completed.
 */
export const outcome = (r: BlockRecord): Outcome => {
  let derived: Outcome = 'complete'
  for (const p of r.proofs) {
    if (p.outcome === 'failed') return 'failed'
    if (p.outcome === 'sent') derived = 'sent'
  }
  return derived
}

/** The first failed proof's failure category, if any proof failed. */
export const failureReason = (r: BlockRecord): string | null =>
  r.proofs.find((p) => p.outcome === 'failed')?.reason ?? null

/** The requested proof types, in request order. */
export const proofTypes = (r: BlockRecord): string[] => r.proofs.map((p) => p.proof_type)

/** When the request was submitted: the earliest submission across proofs. */
export const requestedAtMs = (r: BlockRecord): number =>
  r.proofs.length ? Math.min(...r.proofs.map((p) => p.requested_at_ms)) : r.observed_at_ms

/**
 * When the block resolved: the latest proof resolution, present only once
 * every proof has resolved — mirrors the backend derivation.
 */
export const resolvedAtMs = (r: BlockRecord): number | null => {
  let latest: number | null = null
  for (const p of r.proofs) {
    if (p.resolved_at_ms === null) return null
    latest = latest === null ? p.resolved_at_ms : Math.max(latest, p.resolved_at_ms)
  }
  return latest
}

export const prepMs = (r: BlockRecord) => requestedAtMs(r) - r.observed_at_ms

export const provingMs = (r: BlockRecord): number | null => {
  const resolved = resolvedAtMs(r)
  return resolved === null ? null : resolved - requestedAtMs(r)
}

export const e2eMs = (r: BlockRecord): number | null => {
  const resolved = resolvedAtMs(r)
  return resolved === null ? null : resolved - r.observed_at_ms
}

/** One proof's submit-to-resolution duration, if it has resolved. */
export const proofDurationMs = (p: ProofRecord): number | null =>
  p.resolved_at_ms === null ? null : p.resolved_at_ms - p.requested_at_ms

export const fmt = (ms: number | null) => (ms === null ? '—' : `${ms} ms`)

export const shortRoot = (h: string) => `${h.slice(0, 8)}…${h.slice(-6)}`

/**
 * Split a resolved block into prep% + proving%, summing to 100% of its own
 * end-to-end. Bars built from this fill their track exactly, so the inline bar
 * reads as a prep:proving ratio while magnitude lives in the numeric columns.
 */
export const splitPct = (r: BlockRecord) => {
  const e = Math.max(1, e2eMs(r) ?? 1)
  return { prep: (prepMs(r) / e) * 100, proving: ((provingMs(r) ?? 0) / e) * 100 }
}

export interface Domain {
  min: number
  max: number
}

/** Fastest/slowest end-to-end among completed blocks — the log-scaling domain. */
export const e2eDomain = (blocks: BlockRecord[]): Domain => {
  const xs = blocks.filter((b) => outcome(b) === 'complete').map((b) => e2eMs(b)!)
  if (!xs.length) return { min: 1000, max: 3000 }
  return { min: Math.min(...xs), max: Math.max(...xs) }
}

const MIN_BAR = 6
const MAX_BAR = 56

/**
 * Bar height for the cadence strip on a log curve between the domain's min and
 * max. Log spreads the clustered normal blocks apart while letting outliers —
 * and failures, which exceed max and clamp — tower above them.
 */
export const barHeight = (
  r: BlockRecord,
  domain: Domain,
  minBar = MIN_BAR,
  maxBar = MAX_BAR,
): number => {
  const e = e2eMs(r)
  if (e === null) return 18
  const { min, max } = domain
  if (max <= min) return maxBar
  const t = Math.log(e / min) / Math.log(max / min)
  return Math.round(minBar + (maxBar - minBar) * Math.min(1, Math.max(0, t)))
}

export type Cell = { kind: 'block'; r: BlockRecord } | { kind: 'gap' }

/**
 * Oldest→newest cells for the most recent `limit` blocks, inserting up to four
 * gap stubs per run of skipped slot numbers so the strip shows rhythm, not just
 * bars. A gap only means "no record for that slot" — the cause is unknown.
 */
export const buildCadence = (blocks: BlockRecord[], limit: number): Cell[] => {
  const recent = blocks.slice(0, limit).reverse()
  const cells: Cell[] = []
  for (let i = 0; i < recent.length; i++) {
    if (i > 0) {
      const missed = recent[i].slot - recent[i - 1].slot - 1
      for (let g = 0; g < Math.min(missed, 4); g++) cells.push({ kind: 'gap' })
    }
    cells.push({ kind: 'block', r: recent[i] })
  }
  return cells
}

export interface ProvingStats {
  fastest: BlockRecord
  slowest: BlockRecord
  median: number
}

/** Fastest, median, and slowest proving times across all completed blocks. */
export const provingStats = (blocks: BlockRecord[]): ProvingStats | null => {
  const done = blocks.filter((b) => outcome(b) === 'complete')
  if (!done.length) return null
  const sorted = [...done].sort((a, b) => provingMs(a)! - provingMs(b)!)
  return {
    fastest: sorted[0],
    slowest: sorted[sorted.length - 1],
    median: provingMs(sorted[Math.floor(sorted.length / 2)])!,
  }
}
