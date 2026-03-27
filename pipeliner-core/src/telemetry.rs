//! OpenTelemetry telemetry initialization.
//!
//! Provides configuration and lifecycle management for OTel tracing with an
//! OTLP exporter. The `[telemetry]` section in a pipeline TOML config controls
//! whether tracing is enabled and where spans are exported.

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
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

/// Initialize OpenTelemetry tracing with an OTLP exporter.
///
/// Returns a guard that shuts down the tracer provider on drop.
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

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .ok()?;

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", config.service_name.clone()))
        .build();

    let provider = sdktrace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_tracer_provider(provider.clone());

    Some(TelemetryGuard {
        provider: Some(provider),
    })
}

/// Guard that shuts down the OTel tracer provider on drop.
pub struct TelemetryGuard {
    provider: Option<sdktrace::SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            let _ = provider.shutdown();
        }
    }
}
