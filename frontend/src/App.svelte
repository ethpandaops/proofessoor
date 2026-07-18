<script lang="ts">
  import Header from './components/Header.svelte'
  import CadenceStrip from './components/CadenceStrip.svelte'
  import RequestsTable from './components/RequestsTable.svelte'
  import BlockModal from './components/BlockModal.svelte'
  import {
    defaultRequestQuery,
    fetchBlocks,
    fetchStatus,
    isDefaultRequestQuery,
    isLive,
    ApiError,
    type RequestQuery,
  } from './lib/api'
  import type { BlockRecord, StatusSummary } from './lib/types'

  let blocks = $state<BlockRecord[]>([])
  let tableBlocks = $state<BlockRecord[]>([])
  let tableNextCursor = $state<string | null>(null)
  let tableError = $state<string | null>(null)
  let tableCursor = $state<string | null>(null)
  let pageCursors = $state<(string | null)[]>([null])
  let page = $state(0)
  let query = $state<RequestQuery>(defaultRequestQuery())
  let summary = $state<StatusSummary | null>(null)
  let connected = $state(false)
  let live = $state(false)
  let paused = $state(false)
  let selected = $state<BlockRecord | null>(null)

  let lastSlot = -1
  let lastAdvanceMs = 0
  let requestGeneration = 0

  const requestError = (error: unknown) =>
    error instanceof ApiError ? error.message : 'Unable to load request history.'

  async function refresh(force = false, nextQuery = query) {
    if (paused && !force) return
    const generation = ++requestGeneration
    const needsSeparateTable = page !== 0 || !isDefaultRequestQuery(nextQuery)
    const recentPromise = fetchBlocks()
    const tablePromise = needsSeparateTable ? fetchBlocks(tableCursor, nextQuery) : recentPromise
    const [recent, status, table] = await Promise.allSettled([
      recentPromise,
      fetchStatus(),
      tablePromise,
    ])
    if (generation !== requestGeneration) return

    connected = recent.status === 'fulfilled' || status.status === 'fulfilled'
    if (!connected) live = false
    if (recent.status === 'fulfilled') blocks = recent.value.blocks
    if (status.status === 'fulfilled') {
      summary = status.value
      const slot = status.value.latest_slot ?? -1
      if (slot > lastSlot) {
        lastSlot = slot
        lastAdvanceMs = Date.now()
      }
      live = isLive(lastAdvanceMs, Date.now())
    }
    if (table.status === 'fulfilled') {
      tableBlocks = table.value.blocks
      tableNextCursor = table.value.next_cursor
      tableError = null
    } else {
      tableError = requestError(table.reason)
    }
  }

  async function loadTable(cursor: string | null, nextQuery = query) {
    const generation = ++requestGeneration
    try {
      const table = await fetchBlocks(cursor, nextQuery)
      if (generation !== requestGeneration) return
      tableBlocks = table.blocks
      tableNextCursor = table.next_cursor
      tableError = null
      connected = true
    } catch (error) {
      if (generation !== requestGeneration) return
      tableError = requestError(error)
      if (!(error instanceof ApiError)) {
        connected = false
        live = false
      }
    }
  }

  async function updateQuery(nextQuery: RequestQuery) {
    query = nextQuery
    page = 0
    tableCursor = null
    tableNextCursor = null
    pageCursors = [null]
    await refresh(true, nextQuery)
  }

  async function nextPage() {
    if (!tableNextCursor) return
    page += 1
    tableCursor = tableNextCursor
    pageCursors = [...pageCursors.slice(0, page), tableCursor]
    await loadTable(tableCursor)
  }

  async function previousPage() {
    if (page === 0) return
    page -= 1
    tableCursor = pageCursors[page] ?? null
    await loadTable(tableCursor)
  }

  async function togglePause() {
    paused = !paused
    if (!paused) await refresh(true)
  }

  $effect(() => {
    refresh()
    const id = setInterval(refresh, 3000)
    return () => clearInterval(id)
  })
</script>

<svelte:window onkeydown={(e) => e.key === 'Escape' && (selected = null)} />

<div class="min-h-dvh">
  <Header {summary} {connected} {live} />

  <main class="mx-auto flex max-w-7xl flex-col gap-6 px-8 py-7">
    <CadenceStrip {blocks} {summary} onSelect={(r) => (selected = r)} />
    <RequestsTable
      blocks={tableBlocks}
      error={tableError}
      {paused}
      {query}
      {page}
      hasNext={tableNextCursor !== null}
      onQuery={updateQuery}
      onNext={nextPage}
      onPrevious={previousPage}
      onTogglePause={togglePause}
      onSelect={(r) => (selected = r)}
    />
  </main>

  {#if selected}
    <BlockModal record={selected} onClose={() => (selected = null)} />
  {/if}
</div>
