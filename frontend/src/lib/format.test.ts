import { describe, expect, it } from 'vitest'
import type { BlockRecord, Outcome, ProofRecord } from './types'
import {
  barHeight,
  buildCadence,
  e2eDomain,
  e2eMs,
  failureReason,
  outcome,
  prepMs,
  proofDurationMs,
  provingMs,
  provingStats,
  shortRoot,
  splitPct,
} from './format'

// One proof in the given state, submitted at `requested`, resolved +proving.
function proof(
  proofType: string,
  requested: number,
  proving: number,
  state: Outcome,
  reason: string | null = null,
): ProofRecord {
  return {
    proof_type: proofType,
    outcome: state,
    stage: state === 'failed' ? 'proving' : null,
    reason,
    error: null,
    requested_at_ms: requested,
    resolved_at_ms: state === 'sent' ? null : requested + proving,
    queue_ms: null,
    prove_ms: null,
    attempt: 1,
  }
}

// A record observed at `observed` whose proofs were submitted +prep and each
// resolved +proving (block outcome derives worst-of from the proof states).
function block(
  slot: number,
  opts: { observed?: number; prep?: number; proving?: number; outcome?: Outcome } = {},
): BlockRecord {
  const { observed = 1000, prep = 500, proving = 1500, outcome = 'complete' } = opts
  return {
    slot,
    beacon_block_root: '0xbeacon',
    execution_block_number: slot - 1,
    execution_block_hash: '0xexechash',
    new_payload_request_root: `0x${slot.toString(16).padStart(64, '0')}`,
    observed_at_ms: observed,
    trace_id: null,
    witness_ms: null,
    proofs: [proof('reth-zisk', observed + prep, proving, outcome)],
  }
}

describe('outcome', () => {
  // Multi-proof block with explicit per-proof states.
  const multi = (states: [Outcome, Outcome]): BlockRecord => ({
    ...block(1),
    proofs: [proof('reth-zisk', 1500, 1500, states[0]), proof('ethrex-sp1', 1500, 2500, states[1])],
  })

  it('derives worst-of across proofs, mirroring the backend', () => {
    expect(outcome(multi(['complete', 'complete']))).toBe('complete')
    expect(outcome(multi(['complete', 'sent']))).toBe('sent')
    expect(outcome(multi(['sent', 'failed']))).toBe('failed')
    expect(outcome(multi(['complete', 'failed']))).toBe('failed')
  })

  it('reports the first failed proof reason', () => {
    const failing = {
      ...block(1),
      proofs: [
        proof('reth-zisk', 1500, 1500, 'complete'),
        proof('ethrex-sp1', 1500, 2500, 'failed', 'WitnessTimeout'),
      ],
    }
    expect(failureReason(failing)).toBe('WitnessTimeout')
    expect(failureReason(block(1))).toBeNull()
  })

  it('resolves block timing only when every proof resolved', () => {
    const half = multi(['complete', 'sent'])
    expect(e2eMs(half)).toBeNull()
    expect(provingMs(half)).toBeNull()

    // Fully resolved: the block resolves at the slowest proof (+2500).
    const done = multi(['complete', 'complete'])
    expect(provingMs(done)).toBe(2500)
    expect(proofDurationMs(done.proofs[0])).toBe(1500)
    expect(proofDurationMs(done.proofs[1])).toBe(2500)
  })
})

describe('timing', () => {
  it('derives prep, proving, and end-to-end', () => {
    const b = block(1, { observed: 1000, prep: 500, proving: 1500 })
    expect(prepMs(b)).toBe(500)
    expect(provingMs(b)).toBe(1500)
    expect(e2eMs(b)).toBe(2000)
  })

  it('returns null for unresolved blocks', () => {
    const b = block(1, { outcome: 'sent' })
    expect(provingMs(b)).toBeNull()
    expect(e2eMs(b)).toBeNull()
  })
})

describe('splitPct', () => {
  it('prep and proving sum to 100% of end-to-end', () => {
    const { prep, proving } = splitPct(block(1, { prep: 500, proving: 1500 }))
    expect(prep + proving).toBeCloseTo(100)
    expect(prep).toBeCloseTo(25)
    expect(proving).toBeCloseTo(75)
  })
})

describe('e2eDomain', () => {
  it('spans fastest to slowest completed end-to-end', () => {
    const blocks = [
      block(3, { proving: 500 }), // e2e 1000
      block(2, { proving: 4500 }), // e2e 5000
      block(1, { outcome: 'sent' }), // ignored
    ]
    expect(e2eDomain(blocks)).toEqual({ min: 1000, max: 5000 })
  })

  it('falls back when nothing is completed', () => {
    expect(e2eDomain([block(1, { outcome: 'sent' })])).toEqual({ min: 1000, max: 3000 })
  })
})

describe('barHeight', () => {
  const domain = { min: 1000, max: 5000 }

  it('maps domain ends to bar ends and stays monotonic', () => {
    const lo = barHeight(block(1, { observed: 0, prep: 0, proving: 1000 }), domain)
    const mid = barHeight(block(2, { observed: 0, prep: 0, proving: 2500 }), domain)
    const hi = barHeight(block(3, { observed: 0, prep: 0, proving: 5000 }), domain)
    expect(lo).toBe(6)
    expect(hi).toBe(56)
    expect(mid).toBeGreaterThan(lo)
    expect(mid).toBeLessThan(hi)
  })

  it('clamps blocks above the domain max (e.g. failures)', () => {
    expect(barHeight(block(1, { observed: 0, prep: 0, proving: 12000 }), domain)).toBe(56)
  })
})

describe('buildCadence', () => {
  it('orders oldest→newest and inserts gap stubs for skipped slots', () => {
    // API order is newest-first; slots 10 and 13 leave two skipped in between.
    const cells = buildCadence([block(13), block(10)], 80)
    expect(cells.map((c) => (c.kind === 'block' ? c.r.slot : 'gap'))).toEqual([10, 'gap', 'gap', 13])
  })

  it('caps gap stubs at four per run', () => {
    const cells = buildCadence([block(100), block(1)], 80)
    expect(cells.filter((c) => c.kind === 'gap')).toHaveLength(4)
  })
})

describe('provingStats', () => {
  it('reports fastest, slowest, and median proving', () => {
    const stats = provingStats([
      block(1, { proving: 3000 }),
      block(2, { proving: 1000 }),
      block(3, { proving: 2000 }),
    ])!
    expect(stats.fastest.slot).toBe(2)
    expect(stats.slowest.slot).toBe(1)
    expect(stats.median).toBe(2000)
  })

  it('is null without completed blocks', () => {
    expect(provingStats([block(1, { outcome: 'sent' })])).toBeNull()
  })
})

describe('shortRoot', () => {
  it('keeps the 8-char prefix and 6-char suffix', () => {
    expect(shortRoot('0x1234567890abcdef1234')).toBe('0x123456…ef1234')
  })
})
