//! Pipeline configuration loader.
//!
//! Parses TOML pipeline files into typed config structs, performs environment
//! variable substitution (`${VAR_NAME}`), and resolves plugin names to binary paths
//! via a plugin registry (`plugins.toml`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::Deserialize;

use crate::dsl::parser::parse_step;
use crate::error::PipelineError;
use crate::plugin::{
    PluginProcess, SinkPluginClientWrapper, SourcePluginClientWrapper,
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
    /// Source plugin specification.
    pub source: SourceSpec,
    /// Transform step groups (each `[[transforms]]` entry).
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,
    /// One or more sink plugin specifications.
    #[serde(default)]
    pub sinks: Vec<SinkSpec>,
    /// Optional dead-letter sink.
    pub dead_letter: Option<SinkSpec>,
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

/// Source plugin specification.
#[derive(Debug, Deserialize)]
pub struct SourceSpec {
    /// Plugin name (must match an entry in the plugin registry).
    pub plugin: String,
    /// Arbitrary plugin-specific configuration (serialized to JSON for the plugin).
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

/// Sink plugin specification.
#[derive(Debug, Deserialize)]
pub struct SinkSpec {
    /// Plugin name (must match an entry in the plugin registry).
    pub plugin: String,
    /// Arbitrary plugin-specific configuration (serialized to JSON for the plugin).
    #[serde(default = "default_toml_table")]
    pub config: toml::Value,
}

// ---------------------------------------------------------------------------
// Plugin registry (plugins.toml)
// ---------------------------------------------------------------------------

/// Plugin registry loaded from `plugins.toml`.
#[derive(Debug, Deserialize, Default)]
pub struct PluginRegistry {
    /// Map of plugin name → plugin entry.
    #[serde(default)]
    pub plugins: HashMap<String, PluginEntry>,
}

/// A single plugin entry in the registry.
#[derive(Debug, Deserialize, Clone)]
pub struct PluginEntry {
    /// Path to the plugin binary.
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

/// Load the plugin registry from a `plugins.toml` file.
///
/// If the file does not exist, returns an empty registry.
pub fn load_plugin_registry(path: &Path) -> Result<PluginRegistry, ConfigError> {
    if !path.exists() {
        return Ok(PluginRegistry::default());
    }
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::IoError {
        path: path.to_path_buf(),
        message: e.to_string(),
    })?;
    let registry: PluginRegistry =
        toml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
    Ok(registry)
}

/// Resolve a plugin name to a binary path using the registry.
///
/// Falls back to searching for `pipeliner-plugin-<name>` in the same directory
/// as the current executable if the registry doesn't have an entry.
pub fn resolve_plugin_binary(
    name: &str,
    registry: &PluginRegistry,
) -> Result<PathBuf, ConfigError> {
    // Check registry first.
    if let Some(entry) = registry.plugins.get(name) {
        let path = PathBuf::from(&entry.path);
        if path.exists() {
            return Ok(path);
        }
        return Err(ConfigError::PluginNotFound {
            name: name.to_string(),
            message: format!("registry path does not exist: {}", entry.path),
        });
    }

    // Fallback: look next to the current executable.
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."));

    let binary_name = format!("pipeliner-plugin-{name}");
    let candidate = exe_dir.join(&binary_name);
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(ConfigError::PluginNotFound {
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

/// Convert a TOML value to a JSON string for passing to plugins.
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

/// Spawned plugins that must be kept alive for the duration of a pipeline run.
///
/// Dropping this struct kills all plugin processes.
pub struct SpawnedPlugins {
    /// Source plugin process.
    pub source: PluginProcess,
    /// Sink plugin processes (one per configured sink).
    pub sinks: Vec<PluginProcess>,
}

impl SpawnedPlugins {
    /// Kill all plugin processes.
    pub async fn kill_all(&mut self) {
        self.source.kill().await.ok();
        for sink in &mut self.sinks {
            sink.kill().await.ok();
        }
    }
}

/// Build a `PipelineDefinition` from a parsed config: spawn plugins, connect gRPC clients,
/// parse transform steps.
///
/// Returns the definition and the spawned plugin handles (caller must keep them alive).
pub async fn build_pipeline(
    config: &PipelineConfig,
    registry: &PluginRegistry,
    runtime_params: RuntimeParams,
) -> Result<(PipelineDefinition, SpawnedPlugins), PipelineError> {
    // Resolve and spawn source plugin.
    let source_binary = resolve_plugin_binary(&config.source.plugin, registry)
        .map_err(PipelineError::Config)?;
    let source_process =
        PluginProcess::spawn(&config.source.plugin, source_binary.to_str().unwrap_or_default())
            .await?;

    // Small delay for the gRPC server to be ready.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let source_client = SourcePluginClientWrapper::connect(source_process.address()).await?;

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

    // Resolve and spawn sink plugins.
    let mut sink_processes = Vec::new();
    let mut sink_clients = Vec::new();
    let mut sink_configs = Vec::new();

    for (i, sink_spec) in config.sinks.iter().enumerate() {
        let sink_binary = resolve_plugin_binary(&sink_spec.plugin, registry)
            .map_err(PipelineError::Config)?;
        let label = format!("{}-sink-{}", sink_spec.plugin, i);
        let process = PluginProcess::spawn(&label, sink_binary.to_str().unwrap_or_default()).await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let client = SinkPluginClientWrapper::connect(process.address()).await?;
        let config_json = toml_value_to_json(&sink_spec.config);

        sink_processes.push(process);
        sink_clients.push(client);
        sink_configs.push(SinkConfig {
            config_json,
        });
    }

    let definition = PipelineDefinition {
        source: source_client,
        source_config,
        source_params: runtime_params,
        transforms,
        sinks: sink_clients,
        sink_configs,
    };

    let spawned = SpawnedPlugins {
        source: source_process,
        sinks: sink_processes,
    };

    Ok((definition, spawned))
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Validate a pipeline config by parsing transforms and checking plugin availability.
///
/// Does not spawn plugins — only checks that the config is structurally valid
/// and plugins can be resolved.
pub fn validate_config(
    config: &PipelineConfig,
    registry: &PluginRegistry,
) -> Vec<String> {
    let mut errors = Vec::new();

    // Check source plugin exists.
    if let Err(e) = resolve_plugin_binary(&config.source.plugin, registry) {
        errors.push(format!("source: {e}"));
    }

    // Check transform steps parse.
    for group in &config.transforms {
        for step_str in &group.steps {
            if let Err(e) = parse_step(step_str) {
                errors.push(format!(
                    "transform '{}': invalid step '{}': {}",
                    group.name, step_str, e
                ));
            }
        }
    }

    // Check sink plugins exist.
    for (i, sink_spec) in config.sinks.iter().enumerate() {
        if let Err(e) = resolve_plugin_binary(&sink_spec.plugin, registry) {
            errors.push(format!("sink[{}]: {e}", i));
        }
    }

    // Check dead-letter plugin exists.
    if let Some(dl) = &config.dead_letter {
        if let Err(e) = resolve_plugin_binary(&dl.plugin, registry) {
            errors.push(format!("dead_letter: {e}"));
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

    /// A plugin could not be found.
    #[error("plugin '{name}' not found: {message}")]
    PluginNotFound {
        /// Plugin name.
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
plugin = "file"
config.path = "/tmp/input.csv"
config.format = "csv"

[[transforms]]
name = "clean"
steps = [
    'set(.amount, to_float(.amount))',
    'where(.amount > 0.0)',
]

[[sinks]]
plugin = "file"
config.path = "/tmp/output.json"
config.format = "json"
"#;
        let config = parse_pipeline_config(toml).unwrap();
        assert_eq!(config.pipeline.name, "test");
        assert_eq!(config.source.plugin, "file");
        assert_eq!(config.transforms.len(), 1);
        assert_eq!(config.transforms[0].steps.len(), 2);
        assert_eq!(config.sinks.len(), 1);
        assert_eq!(config.sinks[0].plugin, "file");
        assert!(config.dead_letter.is_none());
    }

    #[test]
    fn parse_multiple_sinks() {
        let toml = r#"
[pipeline]
name = "multi-sink"

[source]
plugin = "file"

[[transforms]]
name = "noop"
steps = []

[[sinks]]
plugin = "file"
config.path = "/tmp/a.json"
config.format = "json"

[[sinks]]
plugin = "file"
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
plugin = "file"
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
    fn plugin_registry_parsing() {
        let toml = r#"
[plugins.file]
path = "/usr/local/bin/pipeliner-plugin-file"
description = "File source and sink"

[plugins.sql]
path = "/usr/local/bin/pipeliner-plugin-sql"
"#;
        let registry: PluginRegistry = toml::from_str(toml).unwrap();
        assert_eq!(registry.plugins.len(), 2);
        assert!(registry.plugins.contains_key("file"));
        assert!(registry.plugins.contains_key("sql"));
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
