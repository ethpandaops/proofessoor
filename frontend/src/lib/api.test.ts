import { afterEach, expect, test } from 'vitest'
import { ApiError, defaultRequestQuery, fetchBlocks } from './api'

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
    cursor: 'v2:slot:desc:1:1:0xroot',
  })
})
