//! OpenTelemetry telemetry initialization.
//!
//! Provides configuration and lifecycle management for OTel tracing and metrics
//! with OTLP exporters. The `[telemetry]` section in a pipeline TOML config
//! controls whether telemetry is enabled and where spans/metrics are exported.

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics as sdkmetrics;
use opentelemetry_sdk::trace as sdktrace;
use opentelemetry_sdk::Resource;

/// Telemetry configuration parsed from the pipeline TOML `[telemetry]` section.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct TelemetryConfig {
    /// Whether to enable OTel tracing. Default: false.
    #[serde(default)]
    pub enabled: bool,
    /// OTLP endpoint (e.g., `"http://localhost:4317"`).
    /// Falls back to the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Service name reported to OTel. Default: `"pipeliner"`.
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

fn default_service_name() -> String {
    "pipeliner".to_string()
}

/// Initialize OpenTelemetry tracing and metrics with OTLP exporters.
///
/// Returns a guard that shuts down the tracer and meter providers on drop.
/// Returns `None` if telemetry is disabled or initialization fails.
pub fn init_telemetry(config: &TelemetryConfig) -> Option<TelemetryGuard> {
    if !config.enabled {
        return None;
    }

    let endpoint = config
        .otlp_endpoint
        .clone()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .unwrap_or_else(|| "http://localhost:4317".to_string());

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", config.service_name.clone()))
        .build();

    // --- Tracing ---
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.clone())
        .build()
        .ok()?;

    let tracer_provider = sdktrace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // --- Metrics ---
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .ok()?;

    let metric_reader = sdkmetrics::PeriodicReader::builder(metric_exporter).build();

    let meter_provider = sdkmetrics::SdkMeterProvider::builder()
        .with_reader(metric_reader)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Some(TelemetryGuard {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
    })
}

/// Guard that shuts down the OTel tracer and meter providers on drop.
pub struct TelemetryGuard {
    tracer_provider: Option<sdktrace::SdkTracerProvider>,
    meter_provider: Option<sdkmetrics::SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}
