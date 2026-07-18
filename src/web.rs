//! HTTP server: health, Prometheus metrics, and the dashboard API.
//!
//! Serves the requestor's view of the status registry for the dashboard, kept
//! distinct from zkBoost's own proving dashboard.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use tower_http::services::{ServeDir, ServeFile};
use tracing::error;

use crate::status::{
    BlockRecord, RecordCursor, RecordFilter, RecordQuery, RecordSearch, RecordSort, SortOrder,
    StatusStore, StatusSummary,
};

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 250;

/// Shared state for the HTTP handlers.
#[derive(Clone)]
struct AppState {
    metrics: PrometheusHandle,
    store: Arc<dyn StatusStore>,
}

/// Serves health, metrics, the dashboard API, and (optionally) the dashboard
/// assets on `addr` until the task is cancelled.
pub async fn serve(
    addr: SocketAddr,
    metrics: PrometheusHandle,
    store: Arc<dyn StatusStore>,
    ui_dir: Option<PathBuf>,
) -> Result<()> {
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(render_metrics))
        .route("/api/blocks", get(blocks))
        .route("/api/status", get(status))
        .with_state(AppState { metrics, store });

    // Serve the built dashboard at `/`, falling back to index.html for SPA routes.
    if let Some(dir) = ui_dir {
        let index = dir.join("index.html");
        app = app.fallback_service(ServeDir::new(dir).fallback(ServeFile::new(index)));
    }

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind HTTP listener on {addr}"))?;
    axum::serve(listener, app)
        .await
        .context("HTTP server error")
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn render_metrics(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

#[derive(Debug, Deserialize)]
struct BlocksQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    #[serde(default)]
    status: RecordFilter,
    search: Option<String>,
    min_prep_ms: Option<u64>,
    max_prep_ms: Option<u64>,
    min_proving_ms: Option<u64>,
    max_proving_ms: Option<u64>,
    min_total_ms: Option<u64>,
    max_total_ms: Option<u64>,
    #[serde(default)]
    sort: RecordSort,
    #[serde(default)]
    order: SortOrder,
}

#[derive(Serialize)]
struct BlocksResponse {
    blocks: Vec<BlockRecord>,
    next_cursor: Option<String>,
}

/// Returns one stable page of recorded block requests, newest slot first.
async fn blocks(
    State(state): State<AppState>,
    Query(query): Query<BlocksQuery>,
) -> Result<Json<BlocksResponse>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if limit == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "limit must be greater than zero".to_string(),
        ));
    }
    validate_bounds("prep", query.min_prep_ms, query.max_prep_ms)?;
    validate_bounds("proving", query.min_proving_ms, query.max_proving_ms)?;
    validate_bounds("total", query.min_total_ms, query.max_total_ms)?;
    let record_query = RecordQuery {
        outcome: query.status,
        search: parse_search(query.search.as_deref())?,
        min_prep_ms: query.min_prep_ms,
        max_prep_ms: query.max_prep_ms,
        min_proving_ms: query.min_proving_ms,
        max_proving_ms: query.max_proving_ms,
        min_total_ms: query.min_total_ms,
        max_total_ms: query.max_total_ms,
        sort: query.sort,
        order: query.order,
    };
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(|message| (StatusCode::BAD_REQUEST, message.to_string()))?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| !cursor.is_valid_for(&record_query))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "cursor does not match requested ordering".to_string(),
        ));
    }
    let page = state
        .store
        .records_page(cursor.as_ref(), &record_query, limit.min(MAX_PAGE_SIZE))
        .await
        .map_err(|error| {
            error!(%error, "failed to query dashboard block records");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to query request status".to_string(),
            )
        })?;
    Ok(Json(BlocksResponse {
        blocks: page.records,
        next_cursor: page.next_cursor.as_ref().map(encode_cursor),
    }))
}

async fn status(State(state): State<AppState>) -> Result<Json<StatusSummary>, StatusCode> {
    state.store.summary().await.map(Json).map_err(|error| {
        error!(%error, "failed to query dashboard status summary");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

fn encode_cursor(cursor: &RecordCursor) -> String {
    let value = cursor
        .sort_value
        .map_or_else(|| "n".to_string(), |value| value.to_string());
    format!(
        "v2:{}:{}:{value}:{}:{}",
        cursor.sort.as_str(),
        cursor.order.as_str(),
        cursor.slot,
        cursor.request_root
    )
}

fn decode_cursor(value: &str) -> std::result::Result<RecordCursor, &'static str> {
    let mut fields = value.split(':');
    match fields.next() {
        Some("v1") => {
            let slot = parse_cursor_integer(fields.next())?;
            let request_root = parse_cursor_root(fields.next(), fields.next())?;
            Ok(RecordCursor {
                sort: RecordSort::Slot,
                order: SortOrder::Desc,
                sort_value: Some(slot),
                slot,
                request_root: request_root.to_string(),
            })
        }
        Some("v2") => {
            let sort = fields
                .next()
                .and_then(RecordSort::from_wire)
                .ok_or("invalid cursor")?;
            let order = fields
                .next()
                .and_then(SortOrder::from_wire)
                .ok_or("invalid cursor")?;
            let sort_value = match fields.next() {
                Some("n") => None,
                value => Some(parse_cursor_integer(value)?),
            };
            let slot = parse_cursor_integer(fields.next())?;
            let request_root = parse_cursor_root(fields.next(), fields.next())?;
            let cursor = RecordCursor {
                sort,
                order,
                sort_value,
                slot,
                request_root: request_root.to_string(),
            };
            if !cursor.is_valid_for(&RecordQuery {
                sort,
                order,
                ..RecordQuery::default()
            }) {
                return Err("invalid cursor");
            }
            Ok(cursor)
        }
        _ => Err("invalid cursor"),
    }
}

fn parse_cursor_integer(value: Option<&str>) -> std::result::Result<u64, &'static str> {
    let value = value.ok_or("invalid cursor")?;
    let value = value.parse::<u64>().map_err(|_| "invalid cursor")?;
    if value > i64::MAX as u64 {
        return Err("invalid cursor");
    }
    Ok(value)
}

fn parse_cursor_root<'a>(
    value: Option<&'a str>,
    trailing: Option<&str>,
) -> std::result::Result<&'a str, &'static str> {
    match (value, trailing) {
        (Some(value), None) if !value.is_empty() => Ok(value),
        _ => Err("invalid cursor"),
    }
}

fn validate_bounds(
    name: &str,
    min: Option<u64>,
    max: Option<u64>,
) -> Result<(), (StatusCode, String)> {
    if min.is_some_and(|value| value > i64::MAX as u64)
        || max.is_some_and(|value| value > i64::MAX as u64)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("{name} duration is too large"),
        ));
    }
    if min.zip(max).is_some_and(|(min, max)| min > max) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("minimum {name} duration exceeds maximum"),
        ));
    }
    Ok(())
}

fn parse_search(value: Option<&str>) -> Result<Option<RecordSearch>, (StatusCode, String)> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if let Ok(slot) = value.parse::<u64>() {
        if slot > i64::MAX as u64 {
            return Err((
                StatusCode::BAD_REQUEST,
                "search slot is too large".to_string(),
            ));
        }
        return Ok(Some(RecordSearch::Slot(slot)));
    }
    let hex = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"));
    if hex.is_some_and(|hex| hex.len() == 64 && hex.chars().all(|byte| byte.is_ascii_hexdigit())) {
        return Ok(Some(RecordSearch::RequestRoot(value.to_ascii_lowercase())));
    }
    Err((
        StatusCode::BAD_REQUEST,
        "search must be an exact slot or 0x-prefixed request root".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_cursor_round_trips() {
        let cursor = RecordCursor {
            sort: RecordSort::TotalMs,
            order: SortOrder::Asc,
            sort_value: Some(456),
            slot: 123,
            request_root: "0xabc".to_string(),
        };
        assert_eq!(encode_cursor(&cursor), "v2:total_ms:asc:456:123:0xabc");
        assert_eq!(decode_cursor(&encode_cursor(&cursor)), Ok(cursor));
    }

    #[test]
    fn dashboard_cursor_accepts_v1_as_default_slot_ordering() {
        assert_eq!(
            decode_cursor("v1:123:0xabc"),
            Ok(RecordCursor {
                sort: RecordSort::Slot,
                order: SortOrder::Desc,
                sort_value: Some(123),
                slot: 123,
                request_root: "0xabc".to_string(),
            })
        );
    }

    #[test]
    fn dashboard_cursor_rejects_malformed_values() {
        assert!(decode_cursor("not-a-cursor").is_err());
        assert!(decode_cursor("abc:0xroot").is_err());
        assert!(decode_cursor("v1:slot:desc:123:123:0xroot").is_err());
        assert!(decode_cursor("v2:unknown:desc:123:123:0xroot").is_err());
        assert!(decode_cursor("v2:slot:sideways:123:123:0xroot").is_err());
        assert!(decode_cursor("v2:slot:desc:123:123:").is_err());
        assert!(decode_cursor("v2:slot:desc:123:123:0xroot:extra").is_err());
        assert!(decode_cursor("v2:slot:desc:122:123:0xroot").is_err());
        assert!(decode_cursor("v2:prep_ms:asc:n:123:0xroot").is_err());
    }

    #[test]
    fn dashboard_cursor_rejects_slots_outside_the_storage_range() {
        assert!(decode_cursor(&format!("v2:slot:desc:{}:{}:0xroot", i64::MAX, i64::MAX)).is_ok());
        assert!(
            decode_cursor(&format!(
                "v2:slot:desc:{}:{}:0xroot",
                i64::MAX as u64 + 1,
                i64::MAX as u64 + 1
            ))
            .is_err()
        );
    }

    #[test]
    fn dashboard_duration_bounds_are_ordered_and_fit_sqlite() {
        assert!(validate_bounds("total", Some(100), Some(200)).is_ok());
        assert!(validate_bounds("total", Some(200), Some(100)).is_err());
        assert!(validate_bounds("total", Some(i64::MAX as u64 + 1), None).is_err());
    }

    #[test]
    fn dashboard_search_accepts_slots_and_exact_request_roots() {
        assert_eq!(
            parse_search(Some(" 123 ")),
            Ok(Some(RecordSearch::Slot(123)))
        );
        let root = format!("0x{}", "aB".repeat(32));
        assert_eq!(
            parse_search(Some(&root)),
            Ok(Some(RecordSearch::RequestRoot(root.to_ascii_lowercase())))
        );
        assert_eq!(parse_search(Some("   ")), Ok(None));
        assert!(parse_search(Some("0xabc")).is_err());
        assert!(parse_search(Some("latest")).is_err());
    }

    #[test]
    fn dashboard_cursor_enum_wire_names_round_trip() {
        for sort in [
            RecordSort::Slot,
            RecordSort::PrepMs,
            RecordSort::ProvingMs,
            RecordSort::TotalMs,
        ] {
            assert_eq!(RecordSort::from_wire(sort.as_str()), Some(sort));
        }
        for order in [SortOrder::Asc, SortOrder::Desc] {
            assert_eq!(SortOrder::from_wire(order.as_str()), Some(order));
        }
    }
}
