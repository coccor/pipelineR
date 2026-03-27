//! Pipeline configuration loader.
//!
//! Parses TOML pipeline files into typed config structs, performs environment
//! variable substitution (`${VAR_NAME}`), and resolves connector names to binary paths
//! via a connector registry (`connectors.toml`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::dsl::parser::parse_step;
use crate::error::PipelineError;
use crate::telemetry::TelemetryConfig;
use crate::connector::{
    ConnectorProcess, SinkConnectorClientWrapper, SourceConnectorClientWrapper,
};
use crate::runtime::PipelineDefinition;
use pipeliner_proto::{RuntimeParams, SinkConfig, SourceConfig};

// ---------------------------------------------------------------------------
// Pipeline config structs (deserialized from TOML)
// ---------------------------------------------------------------------------

/// Top-level pipeline configuration.
#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    /// Pipeline metadata.
    pub pipeline: PipelineMeta,
    /// Source connector specification.
    pub source: SourceSpec,
    /// Transform step groups (each `[[transforms]]` entry).
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,
    /// One or more sink connector specifications.
    #[serde(default)]
    pub sinks: Vec<SinkSpec>,
    /// Optional dead-letter sink.
    pub dead_letter: Option<SinkSpec>,
    /// Optional telemetry configuration for OpenTelemetry tracing.
    #[serde(default)]
    pub telemetry: Option<TelemetryConfig>,
}

/// Pipeline metadata (name, description).
#[derive(Debug, Deserialize)]
pub struct PipelineMeta {
    /// Pipeline name.
    pub name: String,
    /// Optional description.
    #[serde(default)]
    pub description: String,
}

/// Source connector specification.
#[derive(Debug, Deserialize)]
pub struct SourceSpec {
    /// Connector name (must match an entry in the connector registry).
    pub connector: String,
    /// Arbitrary connector-specific configuration (serialized to JSON for the connector).
    #[serde(default = "default_toml_table")]
    pub config: toml::Value,
}

fn default_toml_table() -> toml::Value {
    toml::Value::Table(toml::map::Map::new())
}

/// A named group of transform steps.
#[derive(Debug, Deserialize)]
pub struct TransformSpec {
    /// Human-readable name for this transform group.
    pub name: String,
    /// DSL step expressions (e.g., `'set(.amount, to_float(.amount))'`).
    #[serde(default)]
    pub steps: Vec<String>,
}

/// Sink connector specification.
#[derive(Debug, Deserialize)]
pub struct SinkSpec {
    /// Connector name (must match an entry in the connector registry).
    pub connector: String,
    /// Arbitrary connector-specific configuration (serialized to JSON for the connector).
    #[serde(default = "default_toml_table")]
    pub config: toml::Value,
}

// ---------------------------------------------------------------------------
// Connector registry (connectors.toml)
// ---------------------------------------------------------------------------

/// Connector registry loaded from `connectors.toml`.
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ConnectorRegistry {
    /// Map of connector name → connector entry.
    #[serde(default)]
    pub connectors: HashMap<String, ConnectorEntry>,
}

/// A single connector entry in the registry.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConnectorEntry {
    /// Path to the connector binary.
    pub path: String,
    /// Optional description.
    #[serde(default)]
    pub description: String,
}

// ---------------------------------------------------------------------------
// Environment variable substitution
// ---------------------------------------------------------------------------

/// Substitute `${VAR_NAME}` patterns in a string with environment variable values.
///
/// Returns an error if a referenced variable is not set.
pub fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("valid regex");
    let mut result = input.to_string();
    for cap in re.captures_iter(input) {
        let var_name = &cap[1];
        let var_value = std::env::var(var_name).map_err(|_| ConfigError::MissingEnvVar {
            var: var_name.to_string(),
        })?;
        result = result.replace(&cap[0], &var_value);
    }
    Ok(result)
}

/// Recursively substitute env vars in all string values within a TOML value.
fn substitute_toml_value(value: &mut toml::Value) -> Result<(), ConfigError> {
    match value {
        toml::Value::String(s) => {
            *s = substitute_env_vars(s)?;
        }
        toml::Value::Array(arr) => {
            for v in arr {
                substitute_toml_value(v)?;
            }
        }
        toml::Value::Table(table) => {
            let keys: Vec<String> = table.keys().cloned().collect();
            for key in keys {
                if let Some(v) = table.get_mut(&key) {
                    substitute_toml_value(v)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

/// Load and parse a pipeline configuration from a TOML file.
///
/// Performs environment variable substitution on all string values.
pub fn load_pipeline_config(path: &Path) -> Result<PipelineConfig, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::IoError {
        path: path.to_path_buf(),
        message: e.to_string(),
    })?;
    parse_pipeline_config(&content)
}

/// Parse a pipeline configuration from a TOML string.
///
/// Performs environment variable substitution on all string values.
pub fn parse_pipeline_config(toml_str: &str) -> Result<PipelineConfig, ConfigError> {
    // First parse as a generic TOML value to do env var substitution.
    let mut raw: toml::Value =
        toml::from_str(toml_str).map_err(|e| ConfigError::ParseError(e.to_string()))?;
    substitute_toml_value(&mut raw)?;

    // Then deserialize into the typed config.
    let config: PipelineConfig =
        raw.try_into().map_err(|e: toml::de::Error| ConfigError::ParseError(e.to_string()))?;

    // Validate basic structure.
    if config.sinks.is_empty() {
        return Err(ConfigError::Validation("at least one [[sinks]] entry is required".to_string()));
    }

    Ok(config)
}

/// Load the connector registry from a `connectors.toml` file.
///
/// If the file does not exist, returns an empty registry.
pub fn load_connector_registry(path: &Path) -> Result<ConnectorRegistry, ConfigError> {
    if !path.exists() {
        return Ok(ConnectorRegistry::default());
    }
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::IoError {
        path: path.to_path_buf(),
        message: e.to_string(),
    })?;
    let registry: ConnectorRegistry =
        toml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
    Ok(registry)
}

/// Resolve a connector name to a binary path using the registry.
///
/// Falls back to searching for `pipeliner-plugin-<name>` in the same directory
/// as the current executable if the registry doesn't have an entry.
pub fn resolve_connector_binary(
    name: &str,
    registry: &ConnectorRegistry,
) -> Result<PathBuf, ConfigError> {
    // Check registry first.
    if let Some(entry) = registry.connectors.get(name) {
        let path = PathBuf::from(&entry.path);
        if path.exists() {
            return Ok(path);
        }
        return Err(ConfigError::ConnectorNotFound {
            name: name.to_string(),
            message: format!("registry path does not exist: {}", entry.path),
        });
    }

    // Fallback: look next to the current executable.
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let binary_name = format!("pipeliner-connector-{name}");
    let candidate = exe_dir.join(&binary_name);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(ConfigError::ConnectorNotFound {
        name: name.to_string(),
        message: format!(
            "not in registry and '{}' not found next to executable",
            binary_name
        ),
    })
}

// ---------------------------------------------------------------------------
// Config → PipelineDefinition builder
// ---------------------------------------------------------------------------

/// Merge CLI `--param key=value` overrides into a `RuntimeParams`.
pub fn build_runtime_params(params: &[(String, String)]) -> RuntimeParams {
    let mut map = HashMap::new();
    for (k, v) in params {
        map.insert(k.clone(), v.clone());
    }
    RuntimeParams { params: map }
}

/// Convert a TOML value to a JSON string for passing to connectors.
pub fn toml_value_to_json(value: &toml::Value) -> String {
    // Convert TOML value → serde_json::Value → string.
    let json = toml_to_json_value(value);
    json.to_string()
}

/// Convert a `toml::Value` to a `serde_json::Value`.
fn toml_to_json_value(value: &toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(s) => serde_json::Value::String(s.clone()),
        toml::Value::Integer(i) => serde_json::json!(*i),
        toml::Value::Float(f) => serde_json::json!(*f),
        toml::Value::Boolean(b) => serde_json::Value::Bool(*b),
        toml::Value::Datetime(dt) => serde_json::Value::String(dt.to_string()),
        toml::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(toml_to_json_value).collect())
        }
        toml::Value::Table(table) => {
            let map: serde_json::Map<String, serde_json::Value> = table
                .iter()
                .map(|(k, v)| (k.clone(), toml_to_json_value(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}

/// Spawned connectors that must be kept alive for the duration of a pipeline run.
///
/// Dropping this struct kills all connector processes.
pub struct SpawnedConnectors {
    /// Source connector process.
    pub source: ConnectorProcess,
    /// Sink connector processes (one per configured sink).
    pub sinks: Vec<ConnectorProcess>,
    /// Optional dead-letter sink connector process.
    pub dead_letter: Option<ConnectorProcess>,
}

impl SpawnedConnectors {
    /// Kill all connector processes.
    pub async fn kill_all(&mut self) {
        self.source.kill().await.ok();
        for sink in &mut self.sinks {
            sink.kill().await.ok();
        }
        if let Some(ref mut dl) = self.dead_letter {
            dl.kill().await.ok();
        }
    }
}

/// Build a `PipelineDefinition` from a parsed config: spawn connectors, connect gRPC clients,
/// parse transform steps.
///
/// Returns the definition and the spawned connector handles (caller must keep them alive).
pub async fn build_pipeline(
    config: &PipelineConfig,
    registry: &ConnectorRegistry,
    runtime_params: RuntimeParams,
) -> Result<(PipelineDefinition, SpawnedConnectors), PipelineError> {
    // Resolve and spawn source connector.
    let source_binary = resolve_connector_binary(&config.source.connector, registry)
        .map_err(PipelineError::Config)?;
    let source_binary_path = source_binary.to_str().unwrap_or_default().to_string();
    let source_process =
        ConnectorProcess::spawn(&config.source.connector, &source_binary_path)
            .await
            .map_err(|e| {
                PipelineError::Source(format!(
                    "failed to spawn source connector '{}' (binary: '{}'): {}",
                    config.source.connector, source_binary_path, e
                ))
            })?;

    // Small delay for the gRPC server to be ready.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let source_addr = source_process.address().to_string();
    let source_client = SourceConnectorClientWrapper::connect(source_process.address())
        .await
        .map_err(|e| {
            PipelineError::Source(format!(
                "failed to connect to source connector '{}' at {}: {}",
                config.source.connector, source_addr, e
            ))
        })?;

    // Build source config JSON.
    let source_config_json = toml_value_to_json(&config.source.config);
    let source_config = SourceConfig {
        config_json: source_config_json,
    };

    // Parse all transform steps.
    let mut transforms = Vec::new();
    for group in &config.transforms {
        for step_str in &group.steps {
            let step = parse_step(step_str).map_err(|e| {
                PipelineError::Config(ConfigError::Validation(format!(
                    "transform '{}': invalid step '{}': {}",
                    group.name, step_str, e
                )))
            })?;
            transforms.push(step);
        }
    }

    // Resolve and spawn sink connectors.
    let mut sink_processes = Vec::new();
    let mut sink_clients = Vec::new();
    let mut sink_configs = Vec::new();

    for (i, sink_spec) in config.sinks.iter().enumerate() {
        let sink_binary = resolve_connector_binary(&sink_spec.connector, registry)
            .map_err(PipelineError::Config)?;
        let sink_binary_path = sink_binary.to_str().unwrap_or_default().to_string();
        let label = format!("{}-sink-{}", sink_spec.connector, i);
        let process = ConnectorProcess::spawn(&label, &sink_binary_path)
            .await
            .map_err(|e| {
                PipelineError::Sink(format!(
                    "failed to spawn sink[{}] connector '{}' (binary: '{}'): {}",
                    i, sink_spec.connector, sink_binary_path, e
                ))
            })?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let sink_addr = process.address().to_string();
        let client = SinkConnectorClientWrapper::connect(process.address())
            .await
            .map_err(|e| {
                PipelineError::Sink(format!(
                    "failed to connect to sink[{}] connector '{}' at {}: {}",
                    i, sink_spec.connector, sink_addr, e
                ))
            })?;
        let config_json = toml_value_to_json(&sink_spec.config);

        sink_processes.push(process);
        sink_clients.push(client);
        sink_configs.push(SinkConfig {
            config_json,
        });
    }

    // Optionally resolve and spawn dead-letter sink connector.
    let mut dl_process = None;
    let mut dl_client = None;
    let mut dl_config = None;

    if let Some(dl_spec) = &config.dead_letter {
        let dl_binary = resolve_connector_binary(&dl_spec.connector, registry)
            .map_err(PipelineError::Config)?;
        let dl_binary_path = dl_binary.to_str().unwrap_or_default().to_string();
        let label = format!("{}-dead-letter", dl_spec.connector);
        let process =
            ConnectorProcess::spawn(&label, &dl_binary_path)
                .await
                .map_err(|e| {
                    PipelineError::Sink(format!(
                        "failed to spawn dead-letter connector '{}' (binary: '{}'): {}",
                        dl_spec.connector, dl_binary_path, e
                    ))
                })?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let dl_addr = process.address().to_string();
        let client = SinkConnectorClientWrapper::connect(process.address())
            .await
            .map_err(|e| {
                PipelineError::Sink(format!(
                    "failed to connect to dead-letter connector '{}' at {}: {}",
                    dl_spec.connector, dl_addr, e
                ))
            })?;
        let config_json = toml_value_to_json(&dl_spec.config);

        dl_process = Some(process);
        dl_client = Some(client);
        dl_config = Some(SinkConfig { config_json });
    }

    let definition = PipelineDefinition {
        source: source_client,
        source_config,
        source_params: runtime_params,
        transforms,
        sinks: sink_clients,
        sink_configs,
        dead_letter_sink: dl_client,
        dead_letter_config: dl_config,
        pipeline_name: Some(config.pipeline.name.clone()),
        run_id: None,
        partition_key: None,
    };

    let spawned = SpawnedConnectors {
        source: source_process,
        sinks: sink_processes,
        dead_letter: dl_process,
    };

    Ok((definition, spawned))
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Validate a pipeline config by parsing transforms and checking connector availability.
///
/// Does not spawn connectors — only checks that the config is structurally valid
/// and connectors can be resolved. Returns a list of error/warning strings (empty
/// means the config is valid).
pub fn validate_config(
    config: &PipelineConfig,
    registry: &ConnectorRegistry,
) -> Vec<String> {
    let mut errors = Vec::new();

    // Check pipeline name is not empty.
    if config.pipeline.name.trim().is_empty() {
        errors.push("pipeline.name: must not be empty".to_string());
    }

    // Check source connector exists.
    if let Err(e) = resolve_connector_binary(&config.source.connector, registry) {
        errors.push(format!("source connector '{}': {e}", config.source.connector));
    }

    // Check transform steps parse.
    for group in &config.transforms {
        if group.steps.is_empty() {
            // Empty steps list is valid but worth noting — no-op transform group.
            continue;
        }
        for (j, step_str) in group.steps.iter().enumerate() {
            if let Err(e) = parse_step(step_str) {
                errors.push(format!(
                    "transforms['{}']: invalid step[{}] '{}': {}",
                    group.name, j, step_str, e
                ));
            }
        }
    }

    // Check sink connectors exist and detect duplicate connector names.
    let mut seen_sink_connectors = std::collections::HashSet::new();
    for (i, sink_spec) in config.sinks.iter().enumerate() {
        if let Err(e) = resolve_connector_binary(&sink_spec.connector, registry) {
            errors.push(format!("sinks[{}] connector '{}': {e}", i, sink_spec.connector));
        }
        if !seen_sink_connectors.insert(&sink_spec.connector) {
            errors.push(format!(
                "sinks[{}]: duplicate connector name '{}' (consider using distinct config to differentiate)",
                i, sink_spec.connector
            ));
        }
    }

    // Check dead-letter connector exists.
    if let Some(dl) = &config.dead_letter {
        if let Err(e) = resolve_connector_binary(&dl.connector, registry) {
            errors.push(format!("dead_letter connector '{}': {e}", dl.connector));
        }
        // Warn if dead-letter connector is the same as the source connector.
        if dl.connector == config.source.connector {
            errors.push(format!(
                "dead_letter: connector '{}' is the same as source connector (may cause feedback loop)",
                dl.connector
            ));
        }
    }

    errors
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors from configuration loading and parsing.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to read a config file.
    #[error("failed to read {path}: {message}")]
    IoError {
        /// File path.
        path: PathBuf,
        /// Error details.
        message: String,
    },

    /// TOML parsing error.
    #[error("config parse error: {0}")]
    ParseError(String),

    /// Config validation error.
    #[error("config validation error: {0}")]
    Validation(String),

    /// A referenced environment variable is not set.
    #[error("environment variable '${var}' is not set")]
    MissingEnvVar {
        /// Variable name.
        var: String,
    },

    /// A connector could not be found.
    #[error("connector '{name}' not found: {message}")]
    ConnectorNotFound {
        /// Connector name.
        name: String,
        /// Details.
        message: String,
    },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_pipeline_config() {
        let toml = r#"
[pipeline]
name = "test"

[source]
connector = "file"
config.path = "/tmp/input.csv"
config.format = "csv"

[[transforms]]
name = "clean"
steps = [
    'set(.amount, to_float(.amount))',
    'where(.amount > 0.0)',
]

[[sinks]]
connector = "file"
config.path = "/tmp/output.json"
config.format = "json"
"#;
        let config = parse_pipeline_config(toml).unwrap();
        assert_eq!(config.pipeline.name, "test");
        assert_eq!(config.source.connector, "file");
        assert_eq!(config.transforms.len(), 1);
        assert_eq!(config.transforms[0].steps.len(), 2);
        assert_eq!(config.sinks.len(), 1);
        assert_eq!(config.sinks[0].connector, "file");
        assert!(config.dead_letter.is_none());
    }

    #[test]
    fn parse_multiple_sinks() {
        let toml = r#"
[pipeline]
name = "multi-sink"

[source]
connector = "file"

[[transforms]]
name = "noop"
steps = []

[[sinks]]
connector = "file"
config.path = "/tmp/a.json"
config.format = "json"

[[sinks]]
connector = "file"
config.path = "/tmp/b.csv"
config.format = "csv"
"#;
        let config = parse_pipeline_config(toml).unwrap();
        assert_eq!(config.sinks.len(), 2);
    }

    #[test]
    fn reject_no_sinks() {
        let toml = r#"
[pipeline]
name = "bad"

[source]
connector = "file"
"#;
        let err = parse_pipeline_config(toml).unwrap_err();
        assert!(err.to_string().contains("at least one [[sinks]]"));
    }

    #[test]
    fn env_var_substitution() {
        std::env::set_var("PIPELINER_TEST_VAR", "hello_world");
        let result = substitute_env_vars("prefix_${PIPELINER_TEST_VAR}_suffix").unwrap();
        assert_eq!(result, "prefix_hello_world_suffix");
        std::env::remove_var("PIPELINER_TEST_VAR");
    }

    #[test]
    fn env_var_substitution_missing_var() {
        let result = substitute_env_vars("${PIPELINER_NONEXISTENT_VAR_12345}");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("PIPELINER_NONEXISTENT_VAR_12345"));
    }

    #[test]
    fn toml_to_json_conversion() {
        let toml_val: toml::Value = toml::from_str(
            r#"
path = "/tmp/test.csv"
format = "csv"
options.delimiter = ","
options.has_header = true
"#,
        )
        .unwrap();
        let json_str = toml_value_to_json(&toml_val);
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(json["path"], "/tmp/test.csv");
        assert_eq!(json["format"], "csv");
        assert_eq!(json["options"]["delimiter"], ",");
        assert_eq!(json["options"]["has_header"], true);
    }

    #[test]
    fn connector_registry_parsing() {
        let toml = r#"
[connectors.file]
path = "/usr/local/bin/pipeliner-connector-file"
description = "File source and sink"

[connectors.sql]
path = "/usr/local/bin/pipeliner-connector-sql"
"#;
        let registry: ConnectorRegistry = toml::from_str(toml).unwrap();
        assert_eq!(registry.connectors.len(), 2);
        assert!(registry.connectors.contains_key("file"));
        assert!(registry.connectors.contains_key("sql"));
    }

    #[test]
    fn build_runtime_params_from_pairs() {
        let params = build_runtime_params(&[
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]);
        assert_eq!(params.params.get("key1").unwrap(), "val1");
        assert_eq!(params.params.get("key2").unwrap(), "val2");
    }
}
