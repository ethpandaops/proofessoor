<script lang="ts">
  import type { BlockRecord } from '../lib/types'
  import { e2eMs, failureReason, fmt, outcome, prepMs, proofDurationMs, proofTypes, provingMs } from '../lib/format'

  let { record, onClose }: { record: BlockRecord; onClose: () => void } = $props()

  async function copy(text: string) {
    try {
      await navigator.clipboard.writeText(text)
    } catch {
      /* clipboard unavailable */
    }
  }

  const blockOutcome = $derived(outcome(record))
  // The first failed proof carries the failure detail shown for the block.
  const failedProof = $derived(record.proofs.find((p) => p.outcome === 'failed') ?? null)

  const roots = $derived<[string, string][]>([
    ['request root', record.new_payload_request_root],
    ['beacon block root', record.beacon_block_root],
    ['execution block hash', record.execution_block_hash],
  ])
</script>

<div
  class="fixed inset-0 z-20 flex items-center justify-center bg-ink/70 p-6 backdrop-blur-sm"
  onclick={onClose}
  role="presentation"
>
  <div
    class="w-full max-w-2xl rounded-xl border border-line bg-slate p-6 shadow-2xl"
    onclick={(e) => e.stopPropagation()}
    role="presentation"
  >
    <div class="flex items-start justify-between gap-4">
      <div>
        <p class="text-[11px]/4 tracking-widest text-chalk/40 uppercase">slot</p>
        <p class="mono text-2xl/8 font-semibold">{record.slot}</p>
      </div>
      <button
        type="button"
        onclick={onClose}
        class="rounded-md border border-line px-2 py-1 text-sm/5 text-chalk/55 hover:text-chalk">✕</button
      >
    </div>

    <div class="mt-5 grid grid-cols-2 gap-4 text-sm/6">
      <div>
        <p class="text-[11px]/4 text-chalk/40 uppercase">execution block</p>
        <p class="mono">{record.execution_block_number}</p>
      </div>
      <div>
        <p class="text-[11px]/4 text-chalk/40 uppercase">proof types</p>
        <p class="mono">{proofTypes(record).join(', ')}</p>
      </div>
      <div>
        <p class="text-[11px]/4 text-chalk/40 uppercase">status</p>
        {#if blockOutcome === 'failed'}
          <p class="text-ember">failed · {failureReason(record) ?? 'unknown'}</p>
        {:else if blockOutcome === 'complete'}
          <p class="text-spark">complete</p>
        {:else}
          <p class="text-gold">in-flight</p>
        {/if}
      </div>
      <div>
        <p class="text-[11px]/4 text-chalk/40 uppercase">end-to-end</p>
        <p class="mono">{fmt(e2eMs(record))}</p>
      </div>
    </div>

    <!-- Per-proof rows: each requested proof type with its own outcome and timing. -->
    <div class="mt-5">
      <p class="text-[11px]/4 text-chalk/40 uppercase">proofs</p>
      <div class="mono mt-2 flex flex-col gap-1.5 text-xs/5">
        {#each record.proofs as proof (proof.proof_type)}
          <div class="flex flex-wrap items-center gap-2 rounded-md border border-line bg-ink px-3 py-2">
            <span class="text-chalk/80">{proof.proof_type}</span>
            {#if proof.outcome === 'failed'}
              <span class="rounded-md bg-ember/12 px-2 py-0.5 text-ember">failed · {proof.reason ?? 'unknown'}</span>
            {:else if proof.outcome === 'complete'}
              <span class="rounded-md bg-spark/12 px-2 py-0.5 text-spark">complete</span>
            {:else}
              <span class="rounded-md bg-gold/12 px-2 py-0.5 text-gold">in-flight</span>
            {/if}
            <span class="ml-auto text-chalk/55">{fmt(proofDurationMs(proof))}</span>
          </div>
        {/each}
      </div>
    </div>

    <!-- Failure detail: stage, reason, and the free-form error text (which fits here). -->
    {#if failedProof}
      <div class="mt-5">
        <p class="text-[11px]/4 text-chalk/40 uppercase">failure</p>
        <div class="mono mt-2 flex flex-wrap items-center gap-2 text-xs/5">
          <span class="rounded-md bg-ember/12 px-2 py-1 text-ember">{failedProof.reason ?? 'unknown'}</span>
          {#if failedProof.stage === 'submit'}
            <span class="rounded-md bg-chalk/10 px-2 py-1 text-chalk/60">requestor side</span>
          {:else if failedProof.stage === 'proving'}
            <span class="rounded-md bg-chalk/10 px-2 py-1 text-chalk/60">zkBoost · prover side</span>
          {/if}
        </div>
        {#if failedProof.error}
          <p class="mono mt-2 rounded-md border border-ember/30 bg-ember/8 px-3 py-2 text-xs/5 text-ember/90">
            {failedProof.error}
          </p>
        {/if}
      </div>
    {/if}

    <!-- Timeline -->
    <div class="mt-5">
      <p class="text-[11px]/4 text-chalk/40 uppercase">timeline</p>
      <div class="mono mt-2 flex items-center gap-2 text-xs/5">
        <span class="rounded-md bg-violet/15 px-2 py-1 text-violet">prep {fmt(prepMs(record))}</span>
        <span class="text-chalk/30">→</span>
        <span class="rounded-md bg-spark/15 px-2 py-1 text-spark">proving {fmt(provingMs(record))}</span>
        <span class="text-chalk/30">=</span>
        <span class="rounded-md bg-chalk/10 px-2 py-1">end-to-end {fmt(e2eMs(record))}</span>
      </div>
    </div>

    <!-- Roots -->
    <div class="mt-5 flex flex-col gap-3">
      {#each roots as [label, value] (label)}
        <div>
          <p class="text-[11px]/4 text-chalk/40 uppercase">{label}</p>
          <button
            type="button"
            onclick={() => copy(value)}
            title="copy"
            class="mono mt-1 w-full truncate rounded-md border border-line bg-ink px-3 py-2 text-left text-xs/5 text-chalk/70 hover:text-chalk"
            >{value}</button
          >
        </div>
      {/each}
    </div>
  </div>
</div>
