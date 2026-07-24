import { afterEach, expect, test } from 'vitest'
import { ApiError, defaultRequestQuery, fetchBlocks, tempoTraceUrl } from './api'

const originalFetch = globalThis.fetch

afterEach(() => {
  globalThis.fetch = originalFetch
})

test('fetchBlocks preserves HTTP query errors', async () => {
  globalThis.fetch = async () => new Response('minimum total duration exceeds maximum', { status: 400 })

  await expect(fetchBlocks()).rejects.toEqual(
    new ApiError(400, 'minimum total duration exceeds maximum'),
  )
})

test('fetchBlocks serializes every duration bound including zero', async () => {
  let requested = ''
  globalThis.fetch = async (input) => {
    requested = input.toString()
    return Response.json({ blocks: [], next_cursor: null })
  }
  await fetchBlocks('v2:slot:desc:1:1:0xroot', {
    ...defaultRequestQuery(),
    min_prep_ms: 0,
    max_prep_ms: 1,
    min_proving_ms: 2,
    max_proving_ms: 3,
    min_total_ms: 4,
    max_total_ms: 5,
    min_witness_ms: 6,
    max_witness_ms: 7,
    min_queue_ms: 8,
    max_queue_ms: 9,
    min_prove_ms: 10,
    max_prove_ms: 11,
  })

  const query = new URL(requested, 'http://localhost').searchParams
  expect(Object.fromEntries(query)).toEqual({
    limit: '100',
    status: 'all',
    sort: 'slot',
    order: 'desc',
    min_prep_ms: '0',
    max_prep_ms: '1',
    min_proving_ms: '2',
    max_proving_ms: '3',
    min_total_ms: '4',
    max_total_ms: '5',
    min_witness_ms: '6',
    max_witness_ms: '7',
    min_queue_ms: '8',
    max_queue_ms: '9',
    min_prove_ms: '10',
    max_prove_ms: '11',
    cursor: 'v2:slot:desc:1:1:0xroot',
  })
})

test('tempoTraceUrl preserves the configured Grafana base path', () => {
  const url = new URL(tempoTraceUrl('https://grafana.example/ops', 'abc123'))

  expect(url.origin).toBe('https://grafana.example')
  expect(url.pathname).toBe('/ops/explore')
  expect(JSON.parse(url.searchParams.get('left') ?? '{}')).toEqual({
    datasource: 'tempo',
    queries: [{ refId: 'A', query: 'abc123', queryType: 'traceql' }],
  })
})
