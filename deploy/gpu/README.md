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
  - a **Beacon API** — proofessoor reads blocks here.

## Quick start

```bash
# 1. set your beacon API (and optionally PROOFESSOOR_PORT)
cp .env.example .env
#    then edit .env -> BEACON_URL

# 2. set your EL endpoint in zkboost.toml -> el_endpoint

# 3. build proofessoor from source and start the stack
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d
```

Once the image is published, the base file alone (`docker compose up -d`) pulls
it instead of building (set `PROOFESSOOR_IMAGE` to pin a version).

| What | URL |
| --- | --- |
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

then uncomment `chain_config_path` in `zkboost.toml` and its mount in
`docker-compose.yml`. Regenerate it if the network schedules a new fork.

## Notes

- **First boot is slow.** `ERE_ZISK_SETUP_ON_INIT=1` makes the prover precompute
  its setup before serving proofs — several minutes. The `service_healthy` gate
  holds zkBoost/proofessoor back until it's listening, and the `ere-zisk-setup`
  volume persists the result so later restarts skip it.
- Image versions track zkboost `v0.9.0` (ere/ere-guests `v0.13.0`). If you bump
  zkboost, re-check the pinned ere version in zkboost's `Cargo.toml`.
- `proofessoor` builds from source via `docker-compose.local.yml`; drop that `-f`
  to run the published image instead.
