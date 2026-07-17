<script lang="ts">
  import type { BlockRecord } from '../lib/types'
  import type { RequestFilter } from '../lib/api'
  import { e2eMs, failureReason, fmt, outcome, proofTypes, provingMs, shortRoot, splitPct } from '../lib/format'

  let {
    blocks,
    paused,
    filter,
    page,
    hasNext,
    onFilter,
    onNext,
    onPrevious,
    onTogglePause,
    onSelect,
  }: {
    blocks: BlockRecord[]
    paused: boolean
    filter: RequestFilter
    page: number
    hasNext: boolean
    onFilter: (filter: RequestFilter) => void
    onNext: () => void
    onPrevious: () => void
    onTogglePause: () => void
    onSelect: (r: BlockRecord) => void
  } = $props()

  const chips: [RequestFilter, string][] = [
    ['all', 'all'],
    ['sent', 'in-flight'],
    ['failed', 'failed'],
  ]
</script>

<section class="overflow-hidden rounded-xl border border-line bg-slate/60">
  <header class="flex flex-wrap items-center justify-between gap-3 border-b border-line px-5 py-3.5">
    <h2 class="text-base/6 font-semibold">Proof requests</h2>
    <div class="flex items-center gap-3">
      <div class="flex items-center gap-1 rounded-lg border border-line p-0.5 text-xs/5">
        {#each chips as [key, label] (key)}
          <button
            type="button"
            onclick={() => onFilter(key)}
            class="rounded-md px-2.5 py-1 transition-colors {filter === key
              ? 'bg-violet/20 text-chalk'
              : 'text-chalk/55 hover:text-chalk'}">{label}</button
          >
        {/each}
      </div>
      <button
        type="button"
        onclick={onTogglePause}
        class="rounded-md border border-line px-2.5 py-1 text-xs/5 transition-colors hover:border-violet {paused
          ? 'text-gold'
          : 'text-chalk/55'}">{paused ? '▶ paused' : '⏸ live'}</button
      >
    </div>
  </header>

  {#if blocks.length === 0}
    <p class="px-5 py-14 text-center text-sm/6 text-chalk/40">
      {filter === 'all' ? 'Waiting for beacon blocks…' : 'No requests match this filter.'}
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
