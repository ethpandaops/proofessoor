//! OpenTelemetry tracing initialization (OTLP/gRPC) and trace-id capture.
//!
//! Compiled only with the `otel` cargo feature. Mirrors zkBoost's telemetry
//! setup so both services export to the same collector: the exporter reads
//! `OTEL_EXPORTER_OTLP_ENDPOINT`, and when that is unset no layer is
//! installed, leaving behavior identical to a build without the feature.

use std::env;

use anyhow::{Context, Result};
use opentelemetry::trace::{TraceContextExt, TracerProvider};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{SdkTracer, SdkTracerProvider},
};
use tracing_opentelemetry::{OpenTelemetryLayer, OpenTelemetrySpanExt};
use tracing_subscriber::Registry;

/// The OpenTelemetry tracing layer composed into the subscriber.
pub type OtelLayer = OpenTelemetryLayer<Registry, SdkTracer>;

/// Initializes OpenTelemetry tracing if `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
///
/// Returns a provider handle for explicit shutdown (flushing batched spans)
/// and a layer to compose into the tracing subscriber; both are `None` when
/// no endpoint is configured. The service name defaults to `proofessoor` and
/// can be overridden via `OTEL_SERVICE_NAME`. The global propagator is set to
/// W3C trace context, so outbound calls can carry `traceparent` once a
/// per-request injection point exists.
pub fn init() -> Result<(Option<SdkTracerProvider>, Option<OtelLayer>)> {
    let Ok(endpoint) = env::var("OTEL_EXPORTER_OTLP_ENDPOINT") else {
        return Ok((None, None));
    };
    let service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "proofessoor".to_owned());

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .context("failed to create the OTLP span exporter")?;
    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .build();
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();
    let layer = OpenTelemetryLayer::new(provider.tracer(service_name));

    Ok((Some(provider), Some(layer)))
}

/// The hex trace id of a span, when the otel layer is installed and the span
/// was sampled; `None` otherwise (no layer, disabled span, or dropped by the
/// sampler).
pub fn trace_id(span: &tracing::Span) -> Option<String> {
    let context = span.context();
    let span_ref = context.span();
    let span_context = span_ref.span_context();
    (span_context.is_valid() && span_context.is_sampled())
        .then(|| span_context.trace_id().to_string())
}
