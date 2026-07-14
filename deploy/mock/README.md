# proofessoor + zkboost (mock proving)

A no-GPU stack: **proofessoor** (requestor + dashboard) → **zkboost** with its
**mock** backend. No `ere-server`, no prover, no GPU.

## What "mock" means here

zkboost's mock backend still does almost everything the real one does: it fetches
the execution witness from the EL and executes the block statelessly. It only
replaces the expensive ZK proving step with a **random 2-10 second delay** and
returns a dummy proof. That makes this stack cheap to run anywhere while still
exercising the full proofessoor → zkboost path — witness fetch, block execution,
request lifecycle, the dashboards, and the timing data — exactly as production
would, minus the prover.

Because the witness fetch and execution are real, **you still need a working EL
and Beacon API**. The only thing removed is the prover.

## Prerequisites

- An **EL RPC** that serves `debug_executionWitnessByBlockHash` (e.g. a hoodi
  reth supernode) — zkBoost fetches the witness here; a rate-limited public RPC
  causes `WitnessTimeout`.
- A **Beacon API** that serves blocks, `/eth/v1/config/spec`, and
  `/eth/v1/beacon/genesis` — proofessoor reads the fork schedule once at startup.

## Quick start

```bash
# 1. set your beacon API (and optionally PROOFESSOOR_PORT)
cp .env.example .env
#    then edit .env -> BEACON_URL

# 2. create the ignored operator config and set its EL endpoint
cp zkboost.toml zkboost.local.toml
#    then edit zkboost.local.toml -> el_endpoint

# 3. build proofessoor from source and start the stack
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d
```

Once the image is published, the base file alone (`docker compose up -d`) pulls
it instead of building (set `PROOFESSOOR_IMAGE` to pin a version).

For an authenticated EL, uncomment `[el_headers]` in `zkboost.local.toml` and
set only the header your provider requires. Keep that ignored file private.

### Run with local observability

The observability overlay exports proofessoor and zkBoost traces over OTLP to
Tempo, scrapes both services with Prometheus, and provisions both data sources
in Grafana:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  -f ../docker-compose.observability.yml \
  up --build -d
```

The default local endpoints are:

| What | URL |
| --- | --- |
| Grafana | http://localhost:13002 |
| Prometheus | http://localhost:19090 |
| Tempo API | http://localhost:13200 |
| proofessoor dashboard | http://localhost:19100 |
| zkboost dashboard | http://localhost:3000/dashboard |

## Tuning the mock

The proving delay lives in your `zkboost.local.toml` under `[[zkvm]]`:

```toml
mock_proving_time = { kind = "random", min_ms = 2000, max_ms = 10000 }
```

Other modes the mock backend accepts:

- `{ kind = "constant", ms = 6000 }` — a fixed delay.
- `{ kind = "linear", ms_per_mgas = 500 }` — delay proportional to gas used, so
  bigger blocks "prove" slower (closer to real-world behaviour).

If you change `max_ms` above the `proof_timeout_secs` in the same block, proofs
will time out — keep the timeout comfortably larger.

## Chain config

Each proof request carries the active fork derived by proofessoor from the
Beacon API. zkBoost separately reads the execution-layer genesis config via
`debug_chainConfig` to complete and validate the execution-only blob parameters.
With a proper EL there is nothing to set. Some public RPCs do not serve that
method; if yours does not, generate the EL genesis config for your network and
point zkBoost at it (hoodi shown — swap `hoodi` for your network):

```bash
curl -s https://raw.githubusercontent.com/eth-clients/hoodi/main/metadata/genesis.json \
  | jq '.config' > hoodi_chain_config.json
```

Set `chain_config_path = "/config/chain_config.json"` in your ignored
`zkboost.local.toml`, then add the shared overlay when starting the stack:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.local.yml \
  -f ../docker-compose.chain-config.yml \
  -f ../docker-compose.observability.yml \
  up --build -d
```

Set `ZKBOOST_CHAIN_CONFIG` in `.env` when the file is not
`./hoodi_chain_config.json`. Regenerate it if the network schedules a new fork.

## Notes

- For real proving on a GPU box, see [`../gpu/`](../gpu/) — the same stack with an
  `ere-server` (ZisK) backend instead of the mock.
- `proofessoor` builds from source via `docker-compose.local.yml`; drop that `-f`
  to run the published image instead.
