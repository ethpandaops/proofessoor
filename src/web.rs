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

use crate::status::{BlockRecord, RecordCursor, RecordFilter, StatusStore, StatusSummary};

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
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()
        .map_err(|message| (StatusCode::BAD_REQUEST, message.to_string()))?;
    let page = state
        .store
        .records_page(cursor.as_ref(), query.status, limit.min(MAX_PAGE_SIZE))
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
    format!("v1:{}:{}", cursor.slot, cursor.request_root)
}

fn decode_cursor(value: &str) -> std::result::Result<RecordCursor, &'static str> {
    let mut fields = value.splitn(3, ':');
    let version = fields.next();
    let slot = fields.next();
    let request_root = fields.next();
    let (Some("v1"), Some(slot), Some(request_root)) = (version, slot, request_root) else {
        return Err("invalid cursor");
    };
    if request_root.is_empty() || request_root.contains(':') {
        return Err("invalid cursor");
    }
    let slot: u64 = slot.parse().map_err(|_| "invalid cursor")?;
    if slot > i64::MAX as u64 {
        return Err("invalid cursor");
    }
    Ok(RecordCursor {
        slot,
        request_root: request_root.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_cursor_round_trips() {
        let cursor = RecordCursor {
            slot: 123,
            request_root: "0xabc".to_string(),
        };
        assert_eq!(encode_cursor(&cursor), "v1:123:0xabc");
        assert_eq!(decode_cursor(&encode_cursor(&cursor)), Ok(cursor));
    }

    #[test]
    fn dashboard_cursor_rejects_malformed_values() {
        assert!(decode_cursor("not-a-cursor").is_err());
        assert!(decode_cursor("abc:0xroot").is_err());
        assert!(decode_cursor("123:0xroot").is_err());
        assert!(decode_cursor("v2:123:0xroot").is_err());
        assert!(decode_cursor("v1:123:").is_err());
        assert!(decode_cursor("v1:123:0xroot:extra").is_err());
    }

    #[test]
    fn dashboard_cursor_rejects_slots_outside_the_storage_range() {
        assert!(decode_cursor(&format!("v1:{}:0xroot", i64::MAX as u64)).is_ok());
        assert!(decode_cursor(&format!("v1:{}:0xroot", i64::MAX as u64 + 1)).is_err());
    }
}
