# proofessoor dashboard

The requestor-view dashboard for proofessoor: outcome tiles and a live
proof-request table with per-block prep, end-to-end, witness, queue, and proof
generation timings. The three zkBoost stage columns progressively hide on narrower
viewports but remain sortable and filterable. Request history can be searched
by exact slot or request root, filtered by outcome and duration, then sorted by
slot or timing without loading the full history into the browser.

Block detail shows the complete stage breakdown, each proof's live or
reconciled resolution source, and its trace ID. Pass `--grafana-url` or
`PROOFESSOOR_GRAFANA_URL` to turn trace IDs into Tempo Explore links; otherwise
they remain copyable plain text.

Vite + Svelte 5 + Tailwind v4. Built with [bun](https://bun.sh/); dependency
versions are pinned exact via `bun.lock`.

## Develop

```bash
bun install
bun run dev          # dev server; run a `proofessoor stream --http-addr ...` for the /api data
```

## Build

```bash
bun run build        # outputs static assets to dist/
```

Serve the built `dist/` from the proofessoor binary:

```bash
proofessoor stream ... --http-addr 127.0.0.1:9090 --ui-dir frontend/dist
# dashboard at http://127.0.0.1:9090/
```
