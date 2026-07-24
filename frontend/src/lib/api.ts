// Data layer: fetch the requestor's dashboard state and the pure liveness rule.
// Reactivity lives in App.svelte; this stays free of runes so it's testable.

import type { BlockRecord, DashboardConfig, StatusSummary } from './types'

export type RequestFilter = 'all' | 'sent' | 'complete' | 'failed'
export type SortOrder = 'asc' | 'desc'

export const durationFields = [
  { label: 'prep', sortLabel: 'prep time', sort: 'prep_ms', minKey: 'min_prep_ms', maxKey: 'max_prep_ms' },
  {
    label: 'turnaround',
    sortLabel: 'zkBoost turnaround',
    sort: 'proving_ms',
    minKey: 'min_proving_ms',
    maxKey: 'max_proving_ms',
  },
  { label: 'total', sortLabel: 'end-to-end', sort: 'total_ms', minKey: 'min_total_ms', maxKey: 'max_total_ms' },
  {
    label: 'witness',
    sortLabel: 'witness time',
    sort: 'witness_ms',
    minKey: 'min_witness_ms',
    maxKey: 'max_witness_ms',
  },
  { label: 'queue', sortLabel: 'queue wait', sort: 'queue_ms', minKey: 'min_queue_ms', maxKey: 'max_queue_ms' },
  {
    label: 'generation',
    sortLabel: 'proof generation',
    sort: 'prove_ms',
    minKey: 'min_prove_ms',
    maxKey: 'max_prove_ms',
  },
] as const

type DurationField = (typeof durationFields)[number]
export type DurationKey = DurationField['minKey'] | DurationField['maxKey']
export type RequestSort = 'slot' | DurationField['sort']

export const durationInputs: { key: DurationKey; label: string }[] = durationFields.flatMap(
  (field) => [
    { key: field.minKey, label: `${field.label} min` },
    { key: field.maxKey, label: `${field.label} max` },
  ],
)

export interface RequestQuery {
  status: RequestFilter
  search: string
  min_prep_ms: number | null
  max_prep_ms: number | null
  min_proving_ms: number | null
  max_proving_ms: number | null
  min_total_ms: number | null
  max_total_ms: number | null
  min_witness_ms: number | null
  max_witness_ms: number | null
  min_queue_ms: number | null
  max_queue_ms: number | null
  min_prove_ms: number | null
  max_prove_ms: number | null
  sort: RequestSort
  order: SortOrder
}

export const defaultRequestQuery = (): RequestQuery => ({
  status: 'all',
  search: '',
  min_prep_ms: null,
  max_prep_ms: null,
  min_proving_ms: null,
  max_proving_ms: null,
  min_total_ms: null,
  max_total_ms: null,
  min_witness_ms: null,
  max_witness_ms: null,
  min_queue_ms: null,
  max_queue_ms: null,
  min_prove_ms: null,
  max_prove_ms: null,
  sort: 'slot',
  order: 'desc',
})

export const isDefaultRequestQuery = (query: RequestQuery): boolean =>
  JSON.stringify(query) === JSON.stringify(defaultRequestQuery())

export interface BlocksPage {
  blocks: BlockRecord[]
  next_cursor: string | null
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

async function json<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const detail = (await response.text()).trim()
    throw new ApiError(response.status, detail || `request failed with HTTP ${response.status}`)
  }
  return response.json() as Promise<T>
}

export async function fetchBlocks(
  cursor: string | null = null,
  request: RequestQuery = defaultRequestQuery(),
): Promise<BlocksPage> {
  const query = new URLSearchParams({
    limit: '100',
    status: request.status,
    sort: request.sort,
    order: request.order,
  })
  if (request.search.trim()) query.set('search', request.search.trim())
  for (const field of durationFields) {
    for (const key of [field.minKey, field.maxKey]) {
      const value = request[key]
      if (value !== null) query.set(key, value.toString())
    }
  }
  if (cursor) query.set('cursor', cursor)
  return fetch(`/api/blocks?${query}`).then(json<BlocksPage>)
}

export const fetchStatus = (): Promise<StatusSummary> => fetch('/api/status').then(json<StatusSummary>)

export const fetchDashboardConfig = (): Promise<DashboardConfig> =>
  fetch('/api/config').then(json<DashboardConfig>)

export function tempoTraceUrl(grafanaUrl: string, traceId: string): string {
  const base = grafanaUrl.endsWith('/') ? grafanaUrl : `${grafanaUrl}/`
  const url = new URL('explore', base)
  url.searchParams.set(
    'left',
    JSON.stringify({
      datasource: 'tempo',
      queries: [{ refId: 'A', query: traceId, queryType: 'traceql' }],
    }),
  )
  return url.toString()
}

/** A requestor that stops seeing new slots has stalled, not merely answered. */
const STALE_AFTER_MS = 30_000

/** Live = a new head was observed within the stale window. */
export const isLive = (lastAdvanceMs: number, nowMs: number, staleAfterMs = STALE_AFTER_MS) =>
  lastAdvanceMs > 0 && nowMs - lastAdvanceMs < staleAfterMs
