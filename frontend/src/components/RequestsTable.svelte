<script lang="ts">
  import type { BlockRecord } from '../lib/types'
  import {
    defaultRequestQuery,
    durationFields,
    durationInputs,
    isDefaultRequestQuery,
    type DurationKey,
    type RequestFilter,
    type RequestQuery,
    type RequestSort,
  } from '../lib/api'
  import { e2eMs, failureReason, fmt, outcome, proofTypes, provingMs, shortRoot, splitPct } from '../lib/format'

  let {
    blocks,
    error,
    paused,
    query,
    page,
    hasNext,
    onQuery,
    onNext,
    onPrevious,
    onTogglePause,
    onSelect,
  }: {
    blocks: BlockRecord[]
    error: string | null
    paused: boolean
    query: RequestQuery
    page: number
    hasNext: boolean
    onQuery: (query: RequestQuery) => void
    onNext: () => void
    onPrevious: () => void
    onTogglePause: () => void
    onSelect: (r: BlockRecord) => void
  } = $props()

  const chips: [RequestFilter, string][] = [
    ['all', 'all'],
    ['sent', 'in-flight'],
    ['complete', 'complete'],
    ['failed', 'failed'],
  ]

  const sorts: [RequestSort, string][] = [
    ['slot', 'slot'],
    ['prep_ms', 'prep time'],
    ['proving_ms', 'proving time'],
    ['total_ms', 'end-to-end'],
  ]

  let durationDraft = $state<Partial<Record<DurationKey, string>>>({})
  let durationError = $state<string | null>(null)

  $effect(() => {
    for (const input of durationInputs) {
      durationDraft[input.key] = query[input.key]?.toString() ?? ''
    }
    durationError = null
  })

  function update(patch: Partial<RequestQuery>) {
    onQuery({ ...query, ...patch })
  }

  function updateSearch(event: Event) {
    const input = event.currentTarget as HTMLInputElement
    if (input.validity.valid) update({ search: input.value })
  }

  function editDuration(key: DurationKey, event: Event) {
    durationDraft[key] = (event.currentTarget as HTMLInputElement).value
  }

  function applyDurations() {
    const patch: Partial<Record<DurationKey, number | null>> = {}
    for (const input of durationInputs) {
      const raw = durationDraft[input.key] ?? ''
      const value = raw === '' ? null : Number(raw)
      if (value !== null && (!Number.isSafeInteger(value) || value < 0)) {
        durationError = `${input.label} must be a non-negative whole number`
        return
      }
      patch[input.key] = value
    }
    for (const field of durationFields) {
      const min = patch[field.minKey]
      const max = patch[field.maxKey]
      if (min !== null && min !== undefined && max !== null && max !== undefined && min > max) {
        durationError = `${field.label} minimum cannot exceed its maximum`
        return
      }
    }
    durationError = null
    update(patch)
  }

  const orderLabel = () => {
    if (query.sort === 'slot') return query.order === 'desc' ? 'newest first' : 'oldest first'
    return query.order === 'desc' ? 'high to low' : 'low to high'
  }
</script>

<section class="overflow-hidden rounded-xl border border-line bg-slate/60">
  <header class="flex flex-wrap items-center gap-3 border-b border-line px-5 py-3.5">
    <div class="flex w-full items-center justify-between gap-3">
      <h2 class="text-base/6 font-semibold">Proof requests</h2>
      <button
        type="button"
        onclick={onTogglePause}
        title={paused
          ? 'Automatic updates are paused; searches and filters refresh once'
          : 'Pause automatic dashboard updates'}
        class="rounded-md border border-line px-2.5 py-1 text-xs/5 transition-colors hover:border-violet {paused
          ? 'text-gold'
          : 'text-chalk/55'}">{paused ? '▶ resume live' : '⏸ pause live'}</button
      >
    </div>

    <div class="flex flex-wrap items-center gap-2">
      <div class="flex items-center gap-1 rounded-lg border border-line p-0.5 text-xs/5">
        {#each chips as [key, label] (key)}
          <button
            type="button"
            onclick={() => update({ status: key })}
            class="rounded-md px-2.5 py-1 transition-colors {query.status === key
              ? 'bg-violet/20 text-chalk'
              : 'text-chalk/55 hover:text-chalk'}">{label}</button
          >
        {/each}
      </div>

      <input
        type="search"
        value={query.search}
        pattern="[0-9]+|0[xX][0-9a-fA-F]{64}"
        onchange={updateSearch}
        aria-label="Search by slot or request root"
        placeholder="slot or request root"
        title="Enter an exact beacon slot or 0x-prefixed request root"
        class="w-44 rounded-md border border-line bg-ink/50 px-2.5 py-1 text-xs/5 text-chalk outline-hidden placeholder:text-chalk/30 focus:border-violet invalid:border-ember"
      />

      <select
        value={query.sort}
        onchange={(event) => update({ sort: event.currentTarget.value as RequestSort })}
        aria-label="Sort field"
        class="rounded-md border border-line bg-ink/50 px-2.5 py-1 text-xs/5 text-chalk/70 outline-hidden focus:border-violet"
      >
        {#each sorts as [key, label] (key)}
          <option value={key}>sort by {label}</option>
        {/each}
      </select>

      <button
        type="button"
        onclick={() => update({ order: query.order === 'desc' ? 'asc' : 'desc' })}
        class="rounded-md border border-line px-2.5 py-1 text-xs/5 text-chalk/55 transition-colors hover:border-violet hover:text-chalk"
        >{orderLabel()}</button
      >

      {#if !isDefaultRequestQuery(query)}
        <button
          type="button"
          onclick={() => onQuery(defaultRequestQuery())}
          class="px-1.5 py-1 text-xs/5 text-chalk/40 transition-colors hover:text-chalk">reset</button
        >
      {/if}
    </div>

    <details class="w-full text-xs/5 text-chalk/55">
      <summary class="w-fit cursor-pointer select-none transition-colors hover:text-chalk"
        >duration filters</summary
      >
      <div class="mt-2 grid max-w-3xl grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
        {#each durationInputs as { key, label } (key)}
          <label class="flex flex-col gap-1">
            <span class="text-[10px]/4 tracking-wide text-chalk/35 uppercase">{label} ms</span>
            <input
              type="number"
              min="0"
              step="1"
              value={durationDraft[key] ?? ''}
              oninput={(event) => editDuration(key, event)}
              onchange={applyDurations}
              class="min-w-0 rounded-md border border-line bg-ink/50 px-2 py-1 text-chalk outline-hidden focus:border-violet invalid:border-ember"
            />
          </label>
        {/each}
      </div>
      {#if durationError}
        <p class="mt-2 text-ember">{durationError}. The previous filters are still applied.</p>
      {/if}
    </details>
  </header>

  {#if error}
    <p class="border-b border-ember/25 bg-ember/8 px-5 py-2 text-xs/5 text-ember">{error}</p>
  {/if}

  {#if blocks.length === 0}
    <p class="px-5 py-14 text-center text-sm/6 text-chalk/40">
      {isDefaultRequestQuery(query) ? 'Waiting for beacon blocks…' : 'No requests match these filters.'}
    </p>
  {:else}
    <div class="overflow-x-auto">
      <table class="mono w-full text-sm/6">
        <thead class="text-left text-[11px]/4 tracking-wide text-chalk/40 uppercase">
          <tr class="border-b border-line">
            <th class="py-2.5 pr-4 pl-5 font-medium">Slot</th>
            <th class="px-4 py-2.5 font-medium">Exec #</th>
            <th class="px-4 py-2.5 font-medium">Types</th>
            <th class="px-4 py-2.5 font-medium">Prep ▏ Proving</th>
            <th class="px-4 py-2.5 text-right font-medium">Proving</th>
            <th class="px-4 py-2.5 text-right font-medium">End-to-end</th>
            <th class="px-4 py-2.5 font-medium">Status</th>
            <th class="py-2.5 pr-5 pl-4 font-medium">Root</th>
          </tr>
        </thead>
        <tbody>
          {#each blocks as r (r.new_payload_request_root)}
            {@const s = splitPct(r)}
            {@const o = outcome(r)}
            <tr
              onclick={() => onSelect(r)}
              class="cursor-pointer border-b border-line/60 transition-colors last:border-0 hover:bg-violet/6"
            >
              <td class="py-2.5 pr-4 pl-5">{r.slot}</td>
              <td class="px-4 py-2.5 text-chalk/55">{r.execution_block_number}</td>
              <td class="px-4 py-2.5 text-chalk/70">{proofTypes(r).join(', ')}</td>
              <td class="px-4 py-2.5">
                {#if o === 'sent'}
                  <span class="text-xs/5 text-gold/80">…</span>
                {:else}
                  <div class="flex h-1.5 w-32 overflow-hidden rounded-full bg-ink">
                    <div class="h-full bg-violet" style="width: {s.prep}%"></div>
                    <div
                      class="h-full {o === 'failed' ? 'bg-ember/70' : 'bg-spark'}"
                      style="width: {s.proving}%"
                    ></div>
                  </div>
                {/if}
              </td>
              <td class="px-4 py-2.5 text-right font-medium text-spark/90">{fmt(provingMs(r))}</td>
              <td class="px-4 py-2.5 text-right text-chalk/80">{fmt(e2eMs(r))}</td>
              <td class="px-4 py-2.5">
                {#if o === 'complete'}
                  <span class="rounded-md bg-spark/12 px-2 py-0.5 text-xs/5 text-spark">complete</span>
                {:else if o === 'failed'}
                  <span class="rounded-md bg-ember/12 px-2 py-0.5 text-xs/5 text-ember"
                    >failed · {failureReason(r) ?? 'unknown'}</span
                  >
                {:else}
                  <span class="rounded-md bg-gold/12 px-2 py-0.5 text-xs/5 text-gold">in-flight</span>
                {/if}
              </td>
              <td class="py-2.5 pr-5 pl-4 text-chalk/45">{shortRoot(r.new_payload_request_root)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
  <footer class="flex items-center justify-end gap-2 border-t border-line px-5 py-3 text-xs/5">
    <button
      type="button"
      disabled={page === 0}
      onclick={onPrevious}
      class="rounded-md border border-line px-2.5 py-1 text-chalk/55 transition-colors hover:border-violet disabled:cursor-not-allowed disabled:opacity-30"
      >Previous</button
    >
    <span class="min-w-16 text-center text-chalk/40">Page {page + 1}</span>
    <button
      type="button"
      disabled={!hasNext}
      onclick={onNext}
      class="rounded-md border border-line px-2.5 py-1 text-chalk/55 transition-colors hover:border-violet disabled:cursor-not-allowed disabled:opacity-30"
      >Next</button
    >
  </footer>
</section>
