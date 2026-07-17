<script lang="ts">
  import Header from './components/Header.svelte'
  import CadenceStrip from './components/CadenceStrip.svelte'
  import RequestsTable from './components/RequestsTable.svelte'
  import BlockModal from './components/BlockModal.svelte'
  import { fetchBlocks, fetchDashboard, isLive, type RequestFilter } from './lib/api'
  import type { BlockRecord, StatusSummary } from './lib/types'

  let blocks = $state<BlockRecord[]>([])
  let tableBlocks = $state<BlockRecord[]>([])
  let tableNextCursor = $state<string | null>(null)
  let tableCursor = $state<string | null>(null)
  let pageCursors = $state<(string | null)[]>([null])
  let page = $state(0)
  let filter = $state<RequestFilter>('all')
  let summary = $state<StatusSummary | null>(null)
  let connected = $state(false)
  let live = $state(false)
  let paused = $state(false)
  let selected = $state<BlockRecord | null>(null)

  let lastSlot = -1
  let lastAdvanceMs = 0

  async function refresh() {
    if (paused) return
    try {
      const data = await fetchDashboard()
      blocks = data.blocks
      summary = data.summary
      if (page === 0 && filter === 'all') {
        tableBlocks = data.blocks
        tableNextCursor = data.nextCursor
      } else {
        const table = await fetchBlocks(tableCursor, filter)
        tableBlocks = table.blocks
        tableNextCursor = table.next_cursor
      }
      connected = true
      const slot = data.summary.latest_slot ?? -1
      if (slot > lastSlot) {
        lastSlot = slot
        lastAdvanceMs = Date.now()
      }
      live = isLive(lastAdvanceMs, Date.now())
    } catch {
      connected = false
      live = false
    }
  }

  async function loadTable(cursor: string | null, nextFilter = filter) {
    try {
      const table = await fetchBlocks(cursor, nextFilter)
      tableBlocks = table.blocks
      tableNextCursor = table.next_cursor
      connected = true
    } catch {
      connected = false
    }
  }

  async function selectFilter(nextFilter: RequestFilter) {
    filter = nextFilter
    page = 0
    tableCursor = null
    pageCursors = [null]
    await loadTable(null, nextFilter)
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
      {paused}
      {filter}
      {page}
      hasNext={tableNextCursor !== null}
      onFilter={selectFilter}
      onNext={nextPage}
      onPrevious={previousPage}
      onTogglePause={() => (paused = !paused)}
      onSelect={(r) => (selected = r)}
    />
  </main>

  {#if selected}
    <BlockModal record={selected} onClose={() => (selected = null)} />
  {/if}
</div>
