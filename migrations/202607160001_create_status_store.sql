CREATE TABLE app_metadata (
    key   TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE requests (
    request_root           TEXT PRIMARY KEY NOT NULL,
    slot                   INTEGER NOT NULL CHECK (slot >= 0),
    beacon_block_root      TEXT NOT NULL,
    execution_block_number INTEGER NOT NULL CHECK (execution_block_number >= 0),
    execution_block_hash   TEXT NOT NULL,
    observed_at_ms         INTEGER NOT NULL CHECK (observed_at_ms >= 0),
    requested_at_ms        INTEGER NOT NULL CHECK (requested_at_ms >= 0),
    trace_id               TEXT,
    witness_ms             INTEGER CHECK (witness_ms IS NULL OR witness_ms >= 0)
) STRICT;

CREATE INDEX requests_by_slot
    ON requests (slot DESC, request_root DESC);

CREATE TABLE proofs (
    request_root    TEXT NOT NULL REFERENCES requests(request_root) ON DELETE CASCADE,
    proof_type      TEXT NOT NULL,
    proof_index     INTEGER NOT NULL CHECK (proof_index >= 0),
    outcome         TEXT NOT NULL CHECK (outcome IN ('sent', 'complete', 'failed')),
    failure_stage   TEXT CHECK (failure_stage IS NULL OR failure_stage IN ('submit', 'proving')),
    failure_reason  TEXT,
    failure_error   TEXT,
    resolved_at_ms  INTEGER CHECK (resolved_at_ms IS NULL OR resolved_at_ms >= 0),
    queue_ms        INTEGER CHECK (queue_ms IS NULL OR queue_ms >= 0),
    prove_ms        INTEGER CHECK (prove_ms IS NULL OR prove_ms >= 0),
    attempt         INTEGER NOT NULL CHECK (attempt >= 1),
    PRIMARY KEY (request_root, proof_type),
    UNIQUE (request_root, proof_index)
) STRICT;

CREATE INDEX proofs_by_outcome
    ON proofs (outcome, request_root);
