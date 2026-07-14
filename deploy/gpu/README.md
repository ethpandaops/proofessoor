# proofessoor + zkboost + ere (GPU)

A self-contained Docker Compose stack: **proofessoor** (requestor + dashboard) →
**zkboost** (proof coordinator + dashboard) → **ere-server-zisk** (real ZisK
prover on the GPUs).

## Layout

One proof type (`reth-zisk`) proven across **4 GPUs by a single ere-server**
(devices `0-3`). To use more proof types, add another `ere-server-*` service and
a second `[[zkvm]]` block pointing at it (give it different `device_ids`).

## Prerequisites

- NVIDIA drivers + **NVIDIA Container Toolkit** (so Docker can reserve GPUs).
- Two endpoints on the network you want to prove (e.g. hoodi):
  - an **EL RPC** that serves `debug_executionWitnessByBlockHash` (e.g. a hoodi
    reth supernode) — zkBoost fetches the witness here; a rate-limited public RPC
    causes `WitnessTimeout`.
  - a **Beacon API** that serves blocks, `/eth/v1/config/spec`, and
    `/eth/v1/beacon/genesis` — proofessoor reads the fork schedule once at
    startup.

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

The shared observability overlay exports proofessoor and zkBoost traces over
OTLP to Tempo, scrapes both services with Prometheus, and provisions both data
sources in Grafana:

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

- **First boot is slow.** `ERE_ZISK_SETUP_ON_INIT=1` makes the prover precompute
  its setup before serving proofs — several minutes. The `service_healthy` gate
  holds zkBoost/proofessoor back until it's listening, and the `ere-zisk-setup`
  volume persists the result so later restarts skip it.
- Image versions track zkboost `v0.9.0` (ere/ere-guests `v0.13.0`). If you bump
  zkboost, re-check the pinned ere version in zkboost's `Cargo.toml`.
- `proofessoor` builds from source via `docker-compose.local.yml`; drop that `-f`
  to run the published image instead.
