# syntax=docker/dockerfile:1
#
# Self-contained single-arch build (the host's) for local use and the compose
# `build:` path: bun builds the dashboard, cargo builds the binary, distroless
# runs it. Multi-arch release images are built in CI from Dockerfile.release.
#
#   docker build -t proofessoor .

# --- Stage 1: dashboard assets ---
FROM oven/bun:1.3.10-alpine AS frontend
WORKDIR /app
# Install against the committed lockfile first so this layer caches on deps alone.
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend/ ./
RUN bun run build

# --- Stage 2: binary ---
# The full rust image (buildpack-deps based) already ships pkg-config, libssl-dev,
# git, and CA certs, so no apt step is needed. The pinned toolchain
# (rust-toolchain.toml, 1.93.1) is installed by rustup on first cargo invocation.
FROM rust:1.93-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --features otel \
    && cp target/release/proofessoor /usr/local/bin/proofessoor

# Created here so the distroless runtime (no shell) can COPY it in owned by the
# nonroot uid; a mounted state volume then inherits writable ownership.
RUN mkdir -p /state

# --- Stage 3: runtime ---
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=build /usr/local/bin/proofessoor /usr/local/bin/proofessoor
COPY --from=frontend /app/dist /srv/ui
# Writable state dir for --state-dir; owned by the distroless nonroot uid (65532).
COPY --from=build --chown=65532:65532 /state /state
# The HTTP server (metrics + dashboard) is opt-in via --http-addr; publish
# whatever port you bind, e.g. -p 9090:9090 with --http-addr 0.0.0.0:9090.
ENTRYPOINT ["/usr/local/bin/proofessoor"]
