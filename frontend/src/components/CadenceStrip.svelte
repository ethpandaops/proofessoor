<script lang="ts">
  import type { BlockRecord, StatusSummary } from '../lib/types'
  import { barHeight, buildCadence, e2eDomain, fmt, outcome, splitPct, turnaroundMs, turnaroundStats } from '../lib/format'
  import Tooltip from './Tooltip.svelte'

  let {
    blocks,
    summary,
    onSelect,
  }: {
    blocks: BlockRecord[]
    summary: StatusSummary | null
    onSelect: (r: BlockRecord) => void
  } = $props()

  // Each cell is a 7px bar + 3px gap. Clamp the rendered count to what the
  // measured strip can hold so bars never overflow the card on small screens.
  const PER_CELL = 10
  const MAX_BARS = 80
  let stripWidth = $state(0)

  const cells = $derived(buildCadence(blocks, MAX_BARS))
  const capacity = $derived(stripWidth > 0 ? Math.max(8, Math.floor(stripWidth / PER_CELL)) : MAX_BARS)
  const visible = $derived(cells.slice(-capacity))
  const visibleBlocks = $derived(visible.flatMap((c) => (c.kind === 'block' ? [c.r] : [])))
  const domain = $derived(e2eDomain(visibleBlocks))
  const stats = $derived(turnaroundStats(blocks))
</script>

<section class="rounded-xl border border-line bg-slate/60 p-5">
  <div class="flex items-baseline justify-between gap-4">
    <h2 class="flex items-baseline gap-2 text-[11px]/4 font-semibold tracking-widest text-chalk/50 uppercase">
      Request cadence
      <span class="text-[10px]/4 tracking-normal text-chalk/30 normal-case">recent {capacity}</span>
    </h2>
    <p class="mono flex flex-wrap items-baseline gap-x-2 text-xs/4 text-chalk/55">
      {#if stats}
        {@const fastest = stats.fastest}
        {@const slowest = stats.slowest}
        <span class="text-chalk/35">all {summary?.total ?? 0} · turnaround</span>
        <Tooltip text="slot {fastest.slot} — fastest">
          <button
            type="button"
            onclick={() => onSelect(fastest)}
            class="underline decoration-dotted underline-offset-2 transition-colors hover:text-chalk"
            >min {fmt(turnaroundMs(fastest))}</button
          >
        </Tooltip>
        <span class="text-chalk/30">·</span>
        <span>median <span class="font-semibold text-spark">{fmt(stats.median)}</span></span>
        <span class="text-chalk/30">·</span>
        <Tooltip text="slot {slowest.slot} — slowest">
          <button
            type="button"
            onclick={() => onSelect(slowest)}
            class="underline decoration-dotted underline-offset-2 transition-colors hover:text-chalk"
            >max {fmt(turnaroundMs(slowest))}</button
          >
        </Tooltip>
        <span class="text-chalk/30">·</span>
      {/if}
      <span class="text-ember">{summary?.failed ?? 0} failed</span>
      <span class="text-chalk/30">·</span>
      <span class="text-gold">{summary?.sent ?? 0} in-flight</span>
    </p>
  </div>

  <div class="mt-4 flex h-14 items-end gap-0.75 overflow-hidden" bind:clientWidth={stripWidth}>
    {#each visible as cell, i (i)}
      {#if cell.kind === 'gap'}
        <div class="h-1 w-1.25 shrink-0 self-end rounded-full bg-line" title="no record for this slot"></div>
      {:else}
        {@const r = cell.r}
        {@const s = splitPct(r)}
        {@const o = outcome(r)}
        <button
          type="button"
          onclick={() => onSelect(r)}
          title="slot {r.slot} · {o}"
          class="group flex w-1.75 shrink-0 flex-col justify-end overflow-hidden rounded-xs transition-transform hover:-translate-y-0.5"
          style="height: {barHeight(r, domain)}px"
        >
          {#if o === 'failed'}
            <div class="size-full bg-ember/85 group-hover:bg-ember"></div>
          {:else if o === 'sent'}
            <div class="size-full animate-pulse bg-gold/70"></div>
          {:else}
            <div class="w-full bg-spark" style="height: {s.turnaround}%"></div>
            <div class="w-full bg-violet" style="height: {s.prep}%"></div>
          {/if}
        </button>
      {/if}
    {/each}
    {#if visible.length === 0}
      <p class="self-center text-sm/5 text-chalk/40">waiting for beacon blocks…</p>
    {/if}
  </div>

  <div class="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px]/4 text-chalk/45">
    <span class="flex items-center gap-1.5"><span class="size-2 rounded-xs bg-violet"></span> prep</span>
    <span class="flex items-center gap-1.5"><span class="size-2 rounded-xs bg-spark"></span> turnaround</span>
    <span class="flex items-center gap-1.5"><span class="size-2 rounded-xs bg-ember"></span> failed</span>
    <span class="flex items-center gap-1.5"><span class="size-2 rounded-xs bg-gold"></span> in-flight</span>
    <Tooltip
      wide
      text="A slot with no record. Could be a genuinely missed proposal, an optimistic block skipped by proofessoor, or a request that never recorded — the cause isn't known from here."
    >
      <span class="flex cursor-help items-center gap-1.5">
        <span class="size-1 rounded-full bg-line"></span> missed slot
      </span>
    </Tooltip>
    <span class="ml-auto">height = end-to-end (log) · click for detail</span>
  </div>
</section>
