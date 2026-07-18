use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use tracing::info;

use super::{
    BlockRecord, Failure, FailureStage, Outcome, ProofRecord, ProofResolution, RecordCursor,
    RecordFilter, RecordOutcome, RecordPage, RecordQuery, RecordSearch, RecordWrite,
    ResolveOutcome, RetentionEviction, SortOrder, State, StatusStore, StatusSummary, StorageStats,
    finish_page, now_ms, observe_retention_evictions, validate_record,
};

pub(super) const DATABASE_FILE: &str = "proofessoor.sqlite";
pub(super) const LEGACY_JSON_FILE: &str = "status.json";
const LEGACY_IMPORT_KEY: &str = "legacy_status_json_v0_3_imported";
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// SQLite-backed request status for a configured state directory.
#[derive(Debug)]
pub struct SqliteStatusStore {
    pool: SqlitePool,
    path: PathBuf,
    max_history: usize,
}

impl SqliteStatusStore {
    /// Opens the state database, creating it and applying embedded migrations
    /// when necessary.
    pub async fn open(state_dir: &Path, max_history: usize) -> Result<Self> {
        tokio::fs::create_dir_all(state_dir)
            .await
            .with_context(|| format!("failed to create state dir {}", state_dir.display()))?;

        let path = state_dir.join(DATABASE_FILE);
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open SQLite database {}", path.display()))?;

        sqlx::migrate!()
            .run(&pool)
            .await
            .with_context(|| format!("failed to migrate SQLite database {}", path.display()))?;

        let store = Self {
            pool,
            path,
            max_history,
        };
        let imported = store.import_legacy_json(state_dir).await?;
        if imported > 0 {
            info!(records = imported, "imported legacy JSON request status");
        }
        Ok(store)
    }

    /// Opens an existing database for inspection without creating state,
    /// applying migrations, or importing legacy JSON.
    pub async fn open_read_only(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join(DATABASE_FILE);
        let metadata = tokio::fs::metadata(&path)
            .await
            .with_context(|| format!("status database not found at {}", path.display()))?;
        if !metadata.is_file() {
            bail!("status database path is not a file: {}", path.display());
        }

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .read_only(true)
            .foreign_keys(true)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| {
                format!(
                    "failed to open SQLite database read-only at {}",
                    path.display()
                )
            })?;

        Ok(Self {
            pool,
            path,
            max_history: 0,
        })
    }

    /// Path of the primary SQLite database file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn import_legacy_json(&self, state_dir: &Path) -> Result<usize> {
        let imported: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM app_metadata WHERE key = ?)")
                .bind(LEGACY_IMPORT_KEY)
                .fetch_one(&self.pool)
                .await
                .context("failed to inspect legacy-import metadata")?;
        if imported != 0 {
            return Ok(0);
        }

        let legacy_path = state_dir.join(LEGACY_JSON_FILE);
        let state = match tokio::fs::read(&legacy_path).await {
            Ok(bytes) => serde_json::from_slice::<State>(&bytes)
                .with_context(|| format!("failed to parse {}", legacy_path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read {}", legacy_path.display()));
            }
        };
        let record_count = state.records.len();

        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin legacy status import")?;
        let already_imported: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM app_metadata WHERE key = ?)")
                .bind(LEGACY_IMPORT_KEY)
                .fetch_one(&mut *transaction)
                .await
                .context("failed to recheck legacy-import metadata")?;
        if already_imported != 0 {
            transaction
                .commit()
                .await
                .context("failed to finish legacy-import check")?;
            return Ok(0);
        }

        let existing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM requests")
            .fetch_one(&mut *transaction)
            .await
            .context("failed to inspect database before legacy import")?;
        if existing != 0 {
            bail!(
                "SQLite status contains {existing} requests but has no legacy-import marker; refusing to merge ambiguous state"
            );
        }

        for record in state.records.values() {
            let _ = insert_record(&mut transaction, record).await?;
        }
        let evicted = prune_requests(&mut transaction, self.max_history).await?;
        sqlx::query("INSERT INTO app_metadata (key, value) VALUES (?, ?)")
            .bind(LEGACY_IMPORT_KEY)
            .bind("v0.3")
            .execute(&mut *transaction)
            .await
            .context("failed to record legacy-import completion")?;
        transaction
            .commit()
            .await
            .context("failed to commit legacy status import")?;
        observe_retention_evictions(&evicted);
        Ok(record_count)
    }

    async fn load_records(&self, unresolved_only: bool) -> Result<Vec<BlockRecord>> {
        const ALL: &str = r#"
            SELECT
                r.request_root,
                r.slot,
                r.beacon_block_root,
                r.execution_block_number,
                r.execution_block_hash,
                r.observed_at_ms,
                r.requested_at_ms,
                r.trace_id,
                r.witness_ms,
                p.proof_type,
                p.outcome,
                p.failure_stage,
                p.failure_reason,
                p.failure_error,
                p.resolved_at_ms,
                p.queue_ms,
                p.prove_ms,
                p.attempt
            FROM requests AS r
            JOIN proofs AS p ON p.request_root = r.request_root
            ORDER BY r.slot DESC, r.request_root DESC, p.proof_index ASC
        "#;
        const UNRESOLVED: &str = r#"
            SELECT
                r.request_root,
                r.slot,
                r.beacon_block_root,
                r.execution_block_number,
                r.execution_block_hash,
                r.observed_at_ms,
                r.requested_at_ms,
                r.trace_id,
                r.witness_ms,
                p.proof_type,
                p.outcome,
                p.failure_stage,
                p.failure_reason,
                p.failure_error,
                p.resolved_at_ms,
                p.queue_ms,
                p.prove_ms,
                p.attempt
            FROM requests AS r
            JOIN proofs AS p ON p.request_root = r.request_root
            WHERE EXISTS (
                SELECT 1
                FROM proofs AS unresolved
                WHERE unresolved.request_root = r.request_root
                  AND unresolved.outcome = 'sent'
            )
            ORDER BY r.slot DESC, r.request_root DESC, p.proof_index ASC
        "#;

        let rows = sqlx::query(if unresolved_only { UNRESOLVED } else { ALL })
            .fetch_all(&self.pool)
            .await
            .context("failed to query request status")?;
        rows_to_records(rows)
    }

    async fn load_page(
        &self,
        cursor: Option<&RecordCursor>,
        query: &RecordQuery,
        limit: usize,
    ) -> Result<RecordPage> {
        if limit == 0 {
            return Ok(finish_page(Vec::new(), 0, query));
        }
        if query == &RecordQuery::default() {
            return self.load_default_page(cursor, query, limit).await;
        }
        if cursor.is_some_and(|cursor| !cursor.is_valid_for(query)) {
            bail!("dashboard cursor does not match requested ordering");
        }
        if query.requires_proof_aggregation() {
            self.load_aggregate_page(cursor, query, limit).await
        } else {
            self.load_request_page(cursor, query, limit).await
        }
    }

    /// Serves request identity, outcome, slot, and prep queries without
    /// grouping the complete proofs table. Root and slot searches stay on the
    /// request primary key / slot index; outcome checks use proofs_by_outcome.
    async fn load_request_page(
        &self,
        cursor: Option<&RecordCursor>,
        query: &RecordQuery,
        limit: usize,
    ) -> Result<RecordPage> {
        let sort_column = match query.sort {
            super::RecordSort::Slot => "slot",
            super::RecordSort::PrepMs => "prep_ms",
            super::RecordSort::ProvingMs | super::RecordSort::TotalMs => {
                bail!("proof timing sort reached request-level dashboard query")
            }
        };
        // Prep mirrors BlockRecord's saturating discovery-to-request timing.
        let mut sql = QueryBuilder::<Sqlite>::new(
            r#"
            WITH candidates AS (
                SELECT
                    r.request_root,
                    r.slot,
                    MAX(r.requested_at_ms - r.observed_at_ms, 0) AS prep_ms
                FROM requests AS r
                WHERE 1 = 1
            "#,
        );
        push_request_search(&mut sql, query.search.as_ref())?;
        push_outcome_filter(&mut sql, query.outcome);
        push_duration_bounds(
            &mut sql,
            "MAX(r.requested_at_ms - r.observed_at_ms, 0)",
            query.min_prep_ms,
            query.max_prep_ms,
        )?;
        push_page_window(&mut sql, sort_column, query.order, cursor, limit)?;

        let rows = sql
            .build()
            .fetch_all(&self.pool)
            .await
            .context("failed to query indexed dashboard request page")?;
        Ok(finish_page(rows_to_records(rows)?, limit, query))
    }

    /// Derives proof-wide completion timestamps only for queries that filter
    /// or sort by proving/end-to-end duration. Request-level predicates are
    /// pushed before GROUP BY so exact searches and outcome filters still
    /// reduce the aggregation input.
    async fn load_aggregate_page(
        &self,
        cursor: Option<&RecordCursor>,
        query: &RecordQuery,
        limit: usize,
    ) -> Result<RecordPage> {
        // Only these allowlisted column names and directions are interpolated;
        // every value supplied by the caller remains a bound parameter.
        let sort_column = query.sort.as_str();
        // These formulas mirror BlockRecord's saturating proof-wide timing.
        // The shared memory/SQLite dashboard tests lock multi-proof latest-
        // resolution and unresolved-value parity.
        let mut sql = QueryBuilder::<Sqlite>::new(
            r#"
            WITH request_facts AS (
                SELECT
                    r.request_root,
                    r.slot,
                    MAX(r.requested_at_ms - r.observed_at_ms, 0) AS prep_ms,
                    CASE
                        WHEN COUNT(p.resolved_at_ms) = COUNT(*)
                            THEN MAX(MAX(p.resolved_at_ms) - r.requested_at_ms, 0)
                        ELSE NULL
                    END AS proving_ms,
                    CASE
                        WHEN COUNT(p.resolved_at_ms) = COUNT(*)
                            THEN MAX(MAX(p.resolved_at_ms) - r.observed_at_ms, 0)
                        ELSE NULL
                    END AS total_ms
                FROM requests AS r
                JOIN proofs AS p ON p.request_root = r.request_root
                WHERE 1 = 1
            "#,
        );
        push_request_search(&mut sql, query.search.as_ref())?;
        push_outcome_filter(&mut sql, query.outcome);
        push_duration_bounds(
            &mut sql,
            "MAX(r.requested_at_ms - r.observed_at_ms, 0)",
            query.min_prep_ms,
            query.max_prep_ms,
        )?;
        sql.push(
            r#"
                GROUP BY r.request_root, r.slot
            ),
            candidates AS (
                SELECT * FROM request_facts AS candidate
                WHERE 1 = 1
            "#,
        );
        push_duration_bounds(
            &mut sql,
            "candidate.proving_ms",
            query.min_proving_ms,
            query.max_proving_ms,
        )?;
        push_duration_bounds(
            &mut sql,
            "candidate.total_ms",
            query.min_total_ms,
            query.max_total_ms,
        )?;
        push_page_window(&mut sql, sort_column, query.order, cursor, limit)?;

        let rows = sql
            .build()
            .fetch_all(&self.pool)
            .await
            .context("failed to query aggregate dashboard request page")?;
        Ok(finish_page(rows_to_records(rows)?, limit, query))
    }

    /// Keeps the polling hot path on the `(slot, request_root)` index. Rich
    /// filters aggregate proof rows, but the default dashboard must not scan
    /// the full retained history every three seconds.
    async fn load_default_page(
        &self,
        cursor: Option<&RecordCursor>,
        query: &RecordQuery,
        limit: usize,
    ) -> Result<RecordPage> {
        if cursor.is_some_and(|cursor| !cursor.is_valid_for(query)) {
            bail!("dashboard cursor does not match requested ordering");
        }
        const PAGE: &str = r#"
            WITH page AS (
                SELECT request_root, slot
                FROM requests
                WHERE (
                    ? IS NULL
                    OR slot < ?
                    OR (slot = ? AND request_root < ?)
                )
                ORDER BY slot DESC, request_root DESC
                LIMIT ?
            )
            SELECT
                r.request_root,
                r.slot,
                r.beacon_block_root,
                r.execution_block_number,
                r.execution_block_hash,
                r.observed_at_ms,
                r.requested_at_ms,
                r.trace_id,
                r.witness_ms,
                p.proof_type,
                p.outcome,
                p.failure_stage,
                p.failure_reason,
                p.failure_error,
                p.resolved_at_ms,
                p.queue_ms,
                p.prove_ms,
                p.attempt
            FROM page
            JOIN requests AS r ON r.request_root = page.request_root
            JOIN proofs AS p ON p.request_root = r.request_root
            ORDER BY r.slot DESC, r.request_root DESC, p.proof_index ASC
        "#;

        let fetch_limit = limit
            .checked_add(1)
            .context("dashboard page limit overflow")?;
        let cursor_slot = cursor
            .map(|cursor| to_i64(cursor.slot, "dashboard cursor slot"))
            .transpose()?;
        let rows = sqlx::query(PAGE)
            .bind(cursor_slot)
            .bind(cursor_slot)
            .bind(cursor_slot)
            .bind(cursor.map(|cursor| cursor.request_root.as_str()))
            .bind(usize_to_i64(fetch_limit, "dashboard page limit")?)
            .fetch_all(&self.pool)
            .await
            .context("failed to query default dashboard request page")?;
        Ok(finish_page(rows_to_records(rows)?, limit, query))
    }
}

fn push_request_search(
    sql: &mut QueryBuilder<'_, Sqlite>,
    search: Option<&RecordSearch>,
) -> Result<()> {
    match search {
        Some(RecordSearch::Slot(slot)) => {
            sql.push(" AND r.slot = ")
                .push_bind(to_i64(*slot, "dashboard search slot")?);
        }
        Some(RecordSearch::RequestRoot(root)) => {
            sql.push(" AND r.request_root = ").push_bind(root.clone());
        }
        None => {}
    }
    Ok(())
}

/// Mirrors BlockRecord::outcome without grouping proof rows: failure wins,
/// then any unresolved proof keeps the request sent, otherwise it is complete.
fn push_outcome_filter(sql: &mut QueryBuilder<'_, Sqlite>, outcome: RecordFilter) {
    const FAILED: &str = r#"
        EXISTS (
            SELECT 1 FROM proofs AS outcome_proof
            WHERE outcome_proof.outcome = 'failed'
              AND outcome_proof.request_root = r.request_root
        )
    "#;
    const SENT: &str = r#"
        EXISTS (
            SELECT 1 FROM proofs AS outcome_proof
            WHERE outcome_proof.outcome = 'sent'
              AND outcome_proof.request_root = r.request_root
        )
    "#;
    match outcome {
        RecordFilter::All => {}
        RecordFilter::Failed => {
            sql.push(" AND ").push(FAILED);
        }
        RecordFilter::Sent => {
            sql.push(" AND NOT ").push(FAILED).push(" AND ").push(SENT);
        }
        RecordFilter::Complete => {
            sql.push(" AND NOT ")
                .push(FAILED)
                .push(" AND NOT ")
                .push(SENT);
        }
    }
}

fn push_page_window(
    sql: &mut QueryBuilder<'_, Sqlite>,
    sort_column: &str,
    order: SortOrder,
    cursor: Option<&RecordCursor>,
    limit: usize,
) -> Result<()> {
    let fetch_limit = limit
        .checked_add(1)
        .context("dashboard page limit overflow")?;
    let sort_direction = match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    };
    let cursor_operator = match order {
        SortOrder::Asc => ">",
        SortOrder::Desc => "<",
    };

    sql.push("), page AS (SELECT request_root, slot, ")
        .push(sort_column)
        .push(" AS sort_value FROM candidates WHERE 1 = 1");
    if let Some(cursor) = cursor {
        let cursor_value = cursor
            .sort_value
            .map(|value| to_i64(value, "dashboard cursor sort value"))
            .transpose()?
            .unwrap_or_default();
        let cursor_missing = i64::from(cursor.sort_value.is_none());
        let cursor_slot = to_i64(cursor.slot, "dashboard cursor slot")?;
        sql.push(" AND ((")
            .push(sort_column)
            .push(" IS NULL) > ")
            .push_bind(cursor_missing)
            .push(" OR ((")
            .push(sort_column)
            .push(" IS NULL) = ")
            .push_bind(cursor_missing)
            .push(" AND (COALESCE(")
            .push(sort_column)
            .push(", 0) ")
            .push(cursor_operator)
            .push(" ")
            .push_bind(cursor_value)
            .push(" OR (COALESCE(")
            .push(sort_column)
            .push(", 0) = ")
            .push_bind(cursor_value)
            .push(" AND (slot < ")
            .push_bind(cursor_slot)
            .push(" OR (slot = ")
            .push_bind(cursor_slot)
            .push(" AND request_root < ")
            .push_bind(cursor.request_root.clone())
            .push("))))))");
    }
    sql.push(" ORDER BY (")
        .push(sort_column)
        .push(" IS NULL) ASC, ")
        .push(sort_column)
        .push(" ")
        .push(sort_direction)
        .push(", slot DESC, request_root DESC LIMIT ")
        .push_bind(usize_to_i64(fetch_limit, "dashboard page limit")?)
        .push(
            r#"
        )
        SELECT
            r.request_root,
            r.slot,
            r.beacon_block_root,
            r.execution_block_number,
            r.execution_block_hash,
            r.observed_at_ms,
            r.requested_at_ms,
            r.trace_id,
            r.witness_ms,
            p.proof_type,
            p.outcome,
            p.failure_stage,
            p.failure_reason,
            p.failure_error,
            p.resolved_at_ms,
            p.queue_ms,
            p.prove_ms,
            p.attempt
        FROM page
        JOIN requests AS r ON r.request_root = page.request_root
        JOIN proofs AS p ON p.request_root = r.request_root
        ORDER BY (page.sort_value IS NULL) ASC, page.sort_value
        "#,
        )
        .push(sort_direction)
        .push(", r.slot DESC, r.request_root DESC, p.proof_index ASC");
    Ok(())
}

fn push_duration_bounds(
    sql: &mut QueryBuilder<'_, Sqlite>,
    column: &str,
    min: Option<u64>,
    max: Option<u64>,
) -> Result<()> {
    if let Some(min) = min {
        sql.push(" AND ")
            .push(column)
            .push(" >= ")
            .push_bind(to_i64(min, "dashboard duration bound")?);
    }
    if let Some(max) = max {
        sql.push(" AND ")
            .push(column)
            .push(" <= ")
            .push_bind(to_i64(max, "dashboard duration bound")?);
    }
    Ok(())
}

async fn insert_record(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &BlockRecord,
) -> Result<RecordOutcome> {
    validate_record(record)?;
    let requested_at_ms = record
        .requested_at_ms()
        .context("cannot record a request without any proof types")?;
    let execution_block_number = to_i64(record.execution_block_number, "execution block number")?;
    let inserted_request = sqlx::query(
        r#"
        INSERT INTO requests (
            request_root,
            slot,
            beacon_block_root,
            execution_block_number,
            execution_block_hash,
            observed_at_ms,
            requested_at_ms,
            trace_id,
            witness_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(request_root) DO NOTHING
        "#,
    )
    .bind(&record.new_payload_request_root)
    .bind(to_i64(record.slot, "slot")?)
    .bind(&record.beacon_block_root)
    .bind(execution_block_number)
    .bind(&record.execution_block_hash)
    .bind(to_i64(record.observed_at_ms, "observation timestamp")?)
    .bind(to_i64(requested_at_ms, "request timestamp")?)
    .bind(&record.trace_id)
    .bind(
        record
            .witness_ms
            .map(|value| to_i64(value, "witness duration"))
            .transpose()?,
    )
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to store request {}",
            record.new_payload_request_root
        )
    })?
    .rows_affected()
        == 1;

    if !inserted_request {
        let existing: (i64, String) = sqlx::query_as(
            "SELECT execution_block_number, execution_block_hash FROM requests WHERE request_root = ?",
        )
        .bind(&record.new_payload_request_root)
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to validate existing request {}",
                record.new_payload_request_root
            )
        })?;
        if existing.0 != execution_block_number || existing.1 != record.execution_block_hash {
            bail!(
                "request root {} conflicts with stored execution payload {} ({})",
                record.new_payload_request_root,
                existing.1,
                existing.0
            );
        }
    }

    let mut next_proof_index: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(proof_index) + 1, 0) FROM proofs WHERE request_root = ?",
    )
    .bind(&record.new_payload_request_root)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to allocate proof index for request {}",
            record.new_payload_request_root
        )
    })?;
    let mut inserted_proofs = 0_u64;

    for proof in &record.proofs {
        let result = sqlx::query(
            r#"
            INSERT INTO proofs (
                request_root,
                proof_type,
                proof_index,
                outcome,
                failure_stage,
                failure_reason,
                failure_error,
                resolved_at_ms,
                queue_ms,
                prove_ms,
                attempt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_root, proof_type) DO NOTHING
            "#,
        )
        .bind(&record.new_payload_request_root)
        .bind(&proof.proof_type)
        .bind(next_proof_index)
        .bind(proof.outcome.as_str())
        .bind(proof.stage.map(failure_stage_name))
        .bind(&proof.reason)
        .bind(&proof.error)
        .bind(
            proof
                .resolved_at_ms
                .map(|value| to_i64(value, "resolution timestamp"))
                .transpose()?,
        )
        .bind(
            proof
                .queue_ms
                .map(|value| to_i64(value, "queue duration"))
                .transpose()?,
        )
        .bind(
            proof
                .prove_ms
                .map(|value| to_i64(value, "proving duration"))
                .transpose()?,
        )
        .bind(i64::from(proof.attempt))
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to store proof {} for request {}",
                proof.proof_type, record.new_payload_request_root
            )
        })?;
        if result.rows_affected() == 1 {
            inserted_proofs += 1;
            next_proof_index = next_proof_index
                .checked_add(1)
                .context("proof index exceeds SQLite INTEGER range")?;
        }
    }
    Ok(if inserted_request {
        RecordOutcome::Inserted
    } else if inserted_proofs > 0 {
        RecordOutcome::Extended
    } else {
        RecordOutcome::Duplicate
    })
}

async fn prune_requests(
    transaction: &mut Transaction<'_, Sqlite>,
    max_history: usize,
) -> Result<Vec<RetentionEviction>> {
    if max_history == 0 {
        return Ok(Vec::new());
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM requests")
        .fetch_one(&mut **transaction)
        .await
        .context("failed to count retained requests")?;
    let limit = usize_to_i64(max_history, "history limit")?;
    let excess = count.saturating_sub(limit).max(0);
    if excess == 0 {
        return Ok(Vec::new());
    }

    let mut evicted = select_retention_candidates(transaction, false, excess).await?;
    let outstanding_limit = excess.saturating_sub(
        i64::try_from(evicted.len()).context("settled eviction count does not fit i64")?,
    );
    if outstanding_limit > 0 {
        evicted.extend(select_retention_candidates(transaction, true, outstanding_limit).await?);
    }

    for eviction in &evicted {
        sqlx::query("DELETE FROM requests WHERE request_root = ?")
            .bind(&eviction.request_root)
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("failed to prune request {}", eviction.request_root))?;
    }
    Ok(evicted)
}

async fn select_retention_candidates(
    transaction: &mut Transaction<'_, Sqlite>,
    outstanding: bool,
    limit: i64,
) -> Result<Vec<RetentionEviction>> {
    const SETTLED: &str = r#"
        SELECT request_root, slot
        FROM requests AS candidate
        WHERE NOT EXISTS (
            SELECT 1
            FROM proofs
            WHERE proofs.request_root = candidate.request_root
              AND proofs.outcome = 'sent'
        )
        ORDER BY slot ASC, request_root ASC
        LIMIT ?
    "#;
    const OUTSTANDING: &str = r#"
        SELECT request_root, slot
        FROM requests AS candidate
        WHERE EXISTS (
            SELECT 1
            FROM proofs
            WHERE proofs.request_root = candidate.request_root
              AND proofs.outcome = 'sent'
        )
        ORDER BY slot ASC, request_root ASC
        LIMIT ?
    "#;

    let rows = sqlx::query(if outstanding { OUTSTANDING } else { SETTLED })
        .bind(limit)
        .fetch_all(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to select {} requests for retention pruning",
                if outstanding {
                    "outstanding"
                } else {
                    "settled"
                }
            )
        })?;
    rows.into_iter()
        .map(|row| {
            Ok(RetentionEviction {
                request_root: row.try_get("request_root")?,
                slot: nonnegative_u64(row.try_get("slot")?, "slot")?,
                outstanding,
            })
        })
        .collect()
}

#[async_trait]
impl StatusStore for SqliteStatusStore {
    async fn seen(&self, root: &str) -> Result<bool> {
        let seen: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM requests WHERE request_root = ?)")
                .bind(root)
                .fetch_one(&self.pool)
                .await
                .with_context(|| format!("failed to check recorded request {root}"))?;
        Ok(seen != 0)
    }

    async fn record(&self, record: BlockRecord) -> Result<RecordWrite> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin request-status transaction")?;
        let outcome = insert_record(&mut transaction, &record).await?;

        let evicted = prune_requests(&mut transaction, self.max_history).await?;

        transaction
            .commit()
            .await
            .context("failed to commit request status")?;
        observe_retention_evictions(&evicted);
        Ok(RecordWrite { outcome, evicted })
    }

    async fn resolve_proof(
        &self,
        root: &str,
        proof_type: &str,
        outcome: Outcome,
        failure: Option<Failure>,
    ) -> Result<ResolveOutcome> {
        match (outcome, failure.as_ref()) {
            (Outcome::Complete, None) | (Outcome::Failed, Some(_)) => {}
            (Outcome::Sent, _) => bail!("cannot resolve a proof to sent"),
            (Outcome::Complete, Some(_)) => bail!("a completed proof cannot carry a failure"),
            (Outcome::Failed, None) => bail!("a failed proof must carry failure detail"),
        }

        let now = now_ms();
        let now_i64 = to_i64(now, "resolution timestamp")?;
        let (stage, reason, error) = match failure.as_ref() {
            Some(failure) => (
                Some(failure_stage_name(failure.stage)),
                Some(failure.reason.as_str()),
                Some(failure.error.as_str()),
            ),
            None => (None, None, None),
        };
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin proof-resolution transaction")?;
        // First-terminal-wins is deliberate: live and replayed events may
        // transition only a still-sent proof. Supporting authoritative
        // correction of a terminal verdict requires a separate conditional
        // transition and corresponding second-transition handling in metrics
        // and the UI.
        let result = sqlx::query(
            r#"
            UPDATE proofs
            SET outcome = ?,
                failure_stage = ?,
                failure_reason = ?,
                failure_error = ?,
                resolved_at_ms = ?
            WHERE request_root = ?
              AND proof_type = ?
              AND outcome = 'sent'
            "#,
        )
        .bind(outcome.as_str())
        .bind(stage)
        .bind(reason)
        .bind(error)
        .bind(now_i64)
        .bind(root)
        .bind(proof_type)
        .execute(&mut *transaction)
        .await
        .with_context(|| format!("failed to resolve proof {proof_type} for request {root}"))?;

        if result.rows_affected() == 0 {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT outcome FROM proofs WHERE request_root = ? AND proof_type = ?",
            )
            .bind(root)
            .bind(proof_type)
            .fetch_optional(&mut *transaction)
            .await
            .with_context(|| format!("failed to inspect proof {proof_type} for request {root}"))?;
            transaction
                .commit()
                .await
                .context("failed to finish proof-resolution lookup")?;
            return existing
                .map(|value| parse_outcome(&value).map(ResolveOutcome::AlreadyResolved))
                .transpose()
                .map(|value| value.unwrap_or(ResolveOutcome::Unknown));
        }

        let row = sqlx::query(
            r#"
            SELECT
                r.slot,
                r.requested_at_ms,
                SUM(CASE WHEN p.outcome = 'sent' THEN 1 ELSE 0 END) AS sent_count,
                SUM(CASE WHEN p.outcome = 'failed' THEN 1 ELSE 0 END) AS failed_count
            FROM requests AS r
            JOIN proofs AS p ON p.request_root = r.request_root
            WHERE r.request_root = ?
            GROUP BY r.request_root, r.slot, r.requested_at_ms
            "#,
        )
        .bind(root)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to derive block outcome for request {root}"))?;
        let slot = nonnegative_u64(row.try_get("slot")?, "slot")?;
        let requested_at_ms =
            nonnegative_u64(row.try_get("requested_at_ms")?, "request timestamp")?;
        let sent_count: i64 = row.try_get("sent_count")?;
        let failed_count: i64 = row.try_get("failed_count")?;
        let block_outcome = if failed_count > 0 {
            Outcome::Failed
        } else if sent_count > 0 {
            Outcome::Sent
        } else {
            Outcome::Complete
        };
        transaction
            .commit()
            .await
            .context("failed to commit proof resolution")?;

        Ok(ResolveOutcome::Transitioned(ProofResolution {
            duration_ms: now.saturating_sub(requested_at_ms),
            slot,
            block_outcome,
            block_resolved: sent_count == 0,
        }))
    }

    async fn latest_slot(&self) -> Result<Option<u64>> {
        let slot: Option<i64> = sqlx::query_scalar("SELECT MAX(slot) FROM requests")
            .fetch_one(&self.pool)
            .await
            .context("failed to query latest recorded slot")?;
        slot.map(|value| nonnegative_u64(value, "slot")).transpose()
    }

    async fn inflight_proofs(&self) -> Result<usize> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proofs WHERE outcome = 'sent'")
            .fetch_one(&self.pool)
            .await
            .context("failed to count unresolved proofs")?;
        usize::try_from(count).context("unresolved proof count does not fit usize")
    }

    async fn unresolved_records(&self) -> Result<Vec<BlockRecord>> {
        self.load_records(true).await
    }

    async fn records(&self) -> Result<Vec<BlockRecord>> {
        self.load_records(false).await
    }

    async fn records_page(
        &self,
        cursor: Option<&RecordCursor>,
        query: &RecordQuery,
        limit: usize,
    ) -> Result<RecordPage> {
        self.load_page(cursor, query, limit).await
    }

    async fn summary(&self) -> Result<StatusSummary> {
        // Keep failure > sent > complete aligned with BlockRecord::outcome.
        // Dashboard parity tests exercise this rule against the memory store.
        let row = sqlx::query(
            r#"
            WITH block_outcomes AS (
                SELECT
                    r.request_root,
                    r.slot,
                    CASE
                        WHEN SUM(CASE WHEN p.outcome = 'failed' THEN 1 ELSE 0 END) > 0
                            THEN 'failed'
                        WHEN SUM(CASE WHEN p.outcome = 'sent' THEN 1 ELSE 0 END) > 0
                            THEN 'sent'
                        ELSE 'complete'
                    END AS outcome
                FROM requests AS r
                JOIN proofs AS p ON p.request_root = r.request_root
                GROUP BY r.request_root, r.slot
            )
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN outcome = 'sent' THEN 1 ELSE 0 END), 0) AS sent,
                COALESCE(SUM(CASE WHEN outcome = 'complete' THEN 1 ELSE 0 END), 0) AS complete,
                COALESCE(SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                MAX(slot) AS latest_slot
            FROM block_outcomes
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to query dashboard status summary")?;
        Ok(StatusSummary {
            total: nonnegative_usize(row.try_get("total")?, "request count")?,
            sent: nonnegative_usize(row.try_get("sent")?, "sent request count")?,
            complete: nonnegative_usize(row.try_get("complete")?, "complete request count")?,
            failed: nonnegative_usize(row.try_get("failed")?, "failed request count")?,
            latest_slot: optional_u64(row.try_get("latest_slot")?, "latest slot")?,
        })
    }

    async fn storage_stats(&self) -> Result<Option<StorageStats>> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM requests")
            .fetch_one(&self.pool)
            .await
            .context("failed to count stored requests")?;
        let bytes = file_size(&self.path)
            .await?
            .saturating_add(file_size(&wal_path(&self.path)).await?);
        Ok(Some(StorageStats {
            records: nonnegative_usize(count, "request count")?,
            bytes,
        }))
    }
}

fn rows_to_records(rows: Vec<SqliteRow>) -> Result<Vec<BlockRecord>> {
    let mut records: Vec<BlockRecord> = Vec::new();
    for row in rows {
        let request_root: String = row.try_get("request_root")?;
        let requested_at_ms =
            nonnegative_u64(row.try_get("requested_at_ms")?, "request timestamp")?;
        let proof = ProofRecord {
            proof_type: row.try_get("proof_type")?,
            outcome: parse_outcome(row.try_get::<String, _>("outcome")?.as_str())?,
            stage: row
                .try_get::<Option<String>, _>("failure_stage")?
                .map(|value| parse_failure_stage(&value))
                .transpose()?,
            reason: row.try_get("failure_reason")?,
            error: row.try_get("failure_error")?,
            requested_at_ms,
            resolved_at_ms: optional_u64(row.try_get("resolved_at_ms")?, "resolution timestamp")?,
            queue_ms: optional_u64(row.try_get("queue_ms")?, "queue duration")?,
            prove_ms: optional_u64(row.try_get("prove_ms")?, "proving duration")?,
            attempt: u32::try_from(row.try_get::<i64, _>("attempt")?)
                .context("proof attempt does not fit u32")?,
        };

        if let Some(record) = records
            .last_mut()
            .filter(|record| record.new_payload_request_root == request_root)
        {
            record.proofs.push(proof);
            continue;
        }

        records.push(BlockRecord {
            slot: nonnegative_u64(row.try_get("slot")?, "slot")?,
            beacon_block_root: row.try_get("beacon_block_root")?,
            execution_block_number: nonnegative_u64(
                row.try_get("execution_block_number")?,
                "execution block number",
            )?,
            execution_block_hash: row.try_get("execution_block_hash")?,
            new_payload_request_root: request_root,
            observed_at_ms: nonnegative_u64(
                row.try_get("observed_at_ms")?,
                "observation timestamp",
            )?,
            trace_id: row.try_get("trace_id")?,
            witness_ms: optional_u64(row.try_get("witness_ms")?, "witness duration")?,
            proofs: vec![proof],
        });
    }
    Ok(records)
}

fn failure_stage_name(stage: FailureStage) -> &'static str {
    match stage {
        FailureStage::Submit => "submit",
        FailureStage::Proving => "proving",
    }
}

fn parse_failure_stage(value: &str) -> Result<FailureStage> {
    match value {
        "submit" => Ok(FailureStage::Submit),
        "proving" => Ok(FailureStage::Proving),
        _ => bail!("database contains unknown failure stage {value:?}"),
    }
}

fn parse_outcome(value: &str) -> Result<Outcome> {
    match value {
        "sent" => Ok(Outcome::Sent),
        "complete" => Ok(Outcome::Complete),
        "failed" => Ok(Outcome::Failed),
        _ => bail!("database contains unknown proof outcome {value:?}"),
    }
}

fn to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite INTEGER range"))
}

fn usize_to_i64(value: usize, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite INTEGER range"))
}

fn nonnegative_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("database {field} is negative"))
}

fn nonnegative_usize(value: i64, field: &str) -> Result<usize> {
    usize::try_from(value).with_context(|| format!("database {field} is negative or too large"))
}

fn optional_u64(value: Option<i64>, field: &str) -> Result<Option<u64>> {
    value.map(|value| nonnegative_u64(value, field)).transpose()
}

fn wal_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push("-wal");
    PathBuf::from(path)
}

async fn file_size(path: &Path) -> Result<u64> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}
