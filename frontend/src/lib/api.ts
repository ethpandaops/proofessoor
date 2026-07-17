// Data layer: fetch the requestor's dashboard state and the pure liveness rule.
// Reactivity lives in App.svelte; this stays free of runes so it's testable.

import type { BlockRecord, StatusSummary } from './types'

export type RequestFilter = 'all' | 'sent' | 'failed'

export interface Dashboard {
  blocks: BlockRecord[]
  summary: StatusSummary
  nextCursor: string | null
}

export interface BlocksPage {
  blocks: BlockRecord[]
  next_cursor: string | null
}

async function json<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(`request failed with HTTP ${response.status}`)
  return response.json() as Promise<T>
}

export async function fetchBlocks(
  cursor: string | null = null,
  status: RequestFilter = 'all',
): Promise<BlocksPage> {
  const query = new URLSearchParams({ limit: '100', status })
  if (cursor) query.set('cursor', cursor)
  return fetch(`/api/blocks?${query}`).then(json<BlocksPage>)
}

export async function fetchDashboard(): Promise<Dashboard> {
  const [blocks, summary] = await Promise.all([
    fetchBlocks(),
    fetch('/api/status').then(json<StatusSummary>),
  ])
  return { blocks: blocks.blocks, summary, nextCursor: blocks.next_cursor }
}

/** A requestor that stops seeing new slots has stalled, not merely answered. */
const STALE_AFTER_MS = 30_000

/** Live = a new head was observed within the stale window. */
export const isLive = (lastAdvanceMs: number, nowMs: number, staleAfterMs = STALE_AFTER_MS) =>
  lastAdvanceMs > 0 && nowMs - lastAdvanceMs < staleAfterMs
