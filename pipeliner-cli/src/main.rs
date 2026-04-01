//! pipeliner CLI — execute, validate, and inspect pipelines from the command line.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing::{error, info};

use pipeliner_core::config::{
    build_pipeline, build_runtime_params, load_connector_registry, load_pipeline_config,
    resolve_connector_binary, validate_config, ConnectorEntry, ConnectorRegistry,
};
use pipeliner_core::connector::{ConnectorProcess, SourceConnectorClientWrapper};
use pipeliner_core::runtime::execute_pipeline;
use pipeliner_proto::SourceConfig;

/// pipeliner — a config-driven batch ETL engine.
#[derive(Parser)]
#[command(name = "pipeliner", version, about)]
struct Cli {
    /// Path to connectors.toml registry file.
    #[arg(long, default_value = "connectors.toml")]
    connectors_file: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Output logs as JSON.
    #[arg(long)]
    json_log: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a pipeline end-to-end.
    Run {
        /// Path to the pipeline TOML config file.
        pipeline: PathBuf,
        /// Runtime parameter overrides (key=value).
        #[arg(long = "param", value_parser = parse_param)]
        params: Vec<(String, String)>,
    },
    /// Validate a pipeline config (parse + check plugins).
    Validate {
        /// Path to the pipeline TOML config file.
        pipeline: PathBuf,
    },
    /// Discover and print the source schema.
    Schema {
        /// Path to the pipeline TOML config file.
        pipeline: PathBuf,
        /// Runtime parameter overrides (key=value).
        #[arg(long = "param", value_parser = parse_param)]
        params: Vec<(String, String)>,
    },
    /// Discover and print source partitions.
    Partitions {
        /// Path to the pipeline TOML config file.
        pipeline: PathBuf,
        /// Runtime parameter overrides (key=value).
        #[arg(long = "param", value_parser = parse_param)]
        params: Vec<(String, String)>,
    },
    /// Start the gRPC server (sidecar mode).
    Serve {
        /// Port to listen on.
        #[arg(long, default_value = "50051")]
        port: u16,
    },
    /// Connector management commands.
    Connectors {
        #[command(subcommand)]
        command: ConnectorsCommands,
    },
    /// Print a machine-readable JSON specification for AI-assisted pipeline creation.
    AiSpec,
}

#[derive(Subcommand)]
enum ConnectorsCommands {
    /// List registered connectors.
    List,
    /// Install a connector from a crate name.
    Install {
        /// Crate name (e.g., "pipeliner-connector-sql") or short name (e.g., "sql").
        name: String,
        /// Optional version constraint.
        #[arg(long)]
        version: Option<String>,
    },
    /// Remove a connector.
    Remove {
        /// Connector short name (e.g., "sql").
        name: String,
    },
}

/// Parse a `key=value` parameter string.
fn parse_param(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid param '{s}': expected format key=value"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

fn init_logging(level: &str, json: bool) {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));

    if json {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(env_filter).init();
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(&cli.log_level, cli.json_log);

    match cli.command {
        Commands::Run { pipeline, params } => {
            cmd_run(&pipeline, &params, &cli.connectors_file).await
        }
        Commands::Validate { pipeline } => cmd_validate(&pipeline, &cli.connectors_file),
        Commands::Schema { pipeline, params } => {
            cmd_schema(&pipeline, &params, &cli.connectors_file).await
        }
        Commands::Partitions { pipeline, params } => {
            cmd_partitions(&pipeline, &params, &cli.connectors_file).await
        }
        Commands::Serve { port } => cmd_serve(port, &cli.connectors_file).await,
        Commands::AiSpec => cmd_ai_spec(&cli.connectors_file),
        Commands::Connectors { command } => match command {
            ConnectorsCommands::List => cmd_connectors_list(&cli.connectors_file),
            ConnectorsCommands::Install { name, version } => {
                cmd_connectors_install(&name, version.as_deref(), &cli.connectors_file).await
            }
            ConnectorsCommands::Remove { name } => {
                cmd_connectors_remove(&name, &cli.connectors_file)
            }
        },
    }
}

/// Execute a pipeline end-to-end.
async fn cmd_run(
    pipeline_path: &Path,
    params: &[(String, String)],
    connectors_file: &Path,
) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_connector_registry(connectors_file) {
        Ok(r) => r,
        Err(e) => {
            error!("connector registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let runtime_params = build_runtime_params(params);

    // Initialize OpenTelemetry tracing if configured.
    let _telemetry_guard = config
        .telemetry
        .as_ref()
        .and_then(pipeliner_core::telemetry::init_telemetry);

    info!(pipeline = %config.pipeline.name, "starting pipeline");

    let (definition, mut spawned) = match build_pipeline(&config, &registry, runtime_params).await {
        Ok(r) => r,
        Err(e) => {
            error!("failed to build pipeline: {e}");
            return ExitCode::from(1);
        }
    };

    let result = execute_pipeline(definition).await;
    spawned.kill_all().await;

    match result {
        Ok(result) => {
            info!(
                watermark = %result.watermark,
                records_read = result.records_read,
                "pipeline completed"
            );
            for sr in &result.sink_results {
                info!(
                    sink = sr.index,
                    rows_written = sr.rows_written,
                    rows_errored = sr.rows_errored,
                    "sink result"
                );
                if !sr.error_message.is_empty() {
                    error!(sink = sr.index, error = %sr.error_message, "sink error");
                }
            }

            // Exit with 1 if any sink had errors.
            if result.sink_results.iter().any(|s| s.rows_errored > 0) {
                return ExitCode::from(1);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!("pipeline failed: {e}");
            ExitCode::from(1)
        }
    }
}

/// Validate a pipeline config without executing it.
fn cmd_validate(pipeline_path: &Path, connectors_file: &Path) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_connector_registry(connectors_file) {
        Ok(r) => r,
        Err(e) => {
            error!("connector registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let errors = validate_config(&config, &registry);
    if errors.is_empty() {
        info!(pipeline = %config.pipeline.name, "configuration is valid");
        ExitCode::SUCCESS
    } else {
        for err in &errors {
            error!("{err}");
        }
        ExitCode::from(2)
    }
}

/// Discover and print the source schema.
async fn cmd_schema(
    pipeline_path: &Path,
    params: &[(String, String)],
    connectors_file: &Path,
) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_connector_registry(connectors_file) {
        Ok(r) => r,
        Err(e) => {
            error!("connector registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let source_binary = match resolve_connector_binary(&config.source.connector, &registry) {
        Ok(p) => p,
        Err(e) => {
            error!("source connector: {e}");
            return ExitCode::from(2);
        }
    };

    let mut process = match ConnectorProcess::spawn(
        &config.source.connector,
        source_binary.to_str().unwrap_or_default(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            error!("failed to spawn source connector: {e}");
            return ExitCode::from(1);
        }
    };

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = match SourceConnectorClientWrapper::connect(process.address()).await {
        Ok(c) => c,
        Err(e) => {
            error!("failed to connect to source connector: {e}");
            process.kill().await.ok();
            return ExitCode::from(1);
        }
    };

    let source_config = SourceConfig {
        config_json: pipeliner_core::config::toml_value_to_json(&config.source.config),
    };
    let runtime_params = build_runtime_params(params);

    match client.discover_schema(source_config, runtime_params).await {
        Ok(schema_resp) => {
            let json = serde_json::json!({
                "columns": schema_resp.columns.iter().map(|c| {
                    serde_json::json!({
                        "name": c.name,
                        "data_type": c.data_type,
                        "nullable": c.nullable,
                        "description": c.description,
                    })
                }).collect::<Vec<_>>(),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&json).expect("json serialization")
            );
        }
        Err(e) => {
            error!("schema discovery failed: {e}");
            process.kill().await.ok();
            return ExitCode::from(1);
        }
    }

    process.kill().await.ok();
    ExitCode::SUCCESS
}

/// Discover and print source partitions.
async fn cmd_partitions(
    pipeline_path: &Path,
    params: &[(String, String)],
    connectors_file: &Path,
) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_connector_registry(connectors_file) {
        Ok(r) => r,
        Err(e) => {
            error!("connector registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let source_binary = match resolve_connector_binary(&config.source.connector, &registry) {
        Ok(p) => p,
        Err(e) => {
            error!("source connector: {e}");
            return ExitCode::from(2);
        }
    };

    let mut process = match ConnectorProcess::spawn(
        &config.source.connector,
        source_binary.to_str().unwrap_or_default(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            error!("failed to spawn source connector: {e}");
            return ExitCode::from(1);
        }
    };

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = match SourceConnectorClientWrapper::connect(process.address()).await {
        Ok(c) => c,
        Err(e) => {
            error!("failed to connect to source connector: {e}");
            process.kill().await.ok();
            return ExitCode::from(1);
        }
    };

    let source_config = SourceConfig {
        config_json: pipeliner_core::config::toml_value_to_json(&config.source.config),
    };
    let runtime_params = build_runtime_params(params);

    match client
        .discover_partitions(source_config, runtime_params)
        .await
    {
        Ok(partitions_resp) => {
            let json = serde_json::json!({
                "partitions": partitions_resp.partitions.iter().map(|p| {
                    serde_json::json!({
                        "key": p.key,
                        "params": p.params,
                    })
                }).collect::<Vec<_>>(),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&json).expect("json serialization")
            );
        }
        Err(e) => {
            error!("partition discovery failed: {e}");
            process.kill().await.ok();
            return ExitCode::from(1);
        }
    }

    process.kill().await.ok();
    ExitCode::SUCCESS
}

/// List registered connectors from the connector registry.
fn cmd_connectors_list(connectors_file: &Path) -> ExitCode {
    let registry = match load_connector_registry(connectors_file) {
        Ok(r) => r,
        Err(e) => {
            error!("connector registry error: {e}");
            return ExitCode::from(2);
        }
    };

    if registry.connectors.is_empty() {
        println!("No connectors registered.");
        println!("Create a connectors.toml file or use --plugins-file to specify one.");
    } else {
        println!(
            "{name:<20} {path:<40} DESCRIPTION",
            name = "NAME",
            path = "PATH"
        );
        println!("{}", "-".repeat(80));
        for (name, entry) in &registry.connectors {
            println!(
                "{name:<20} {path:<40} {desc}",
                path = entry.path,
                desc = entry.description
            );
        }
    }

    ExitCode::SUCCESS
}

/// Install a connector via `cargo install`, then register it in connectors.toml.
async fn cmd_connectors_install(
    name: &str,
    version: Option<&str>,
    connectors_file: &Path,
) -> ExitCode {
    let short_name = name.strip_prefix("pipeliner-connector-").unwrap_or(name);
    let crate_name = if name.starts_with("pipeliner-connector-") {
        name.to_string()
    } else {
        format!("pipeliner-connector-{name}")
    };

    info!(connector = %short_name, crate_name = %crate_name, "installing connector");

    let mut cmd = tokio::process::Command::new("cargo");
    cmd.arg("install").arg(&crate_name);
    if let Some(ver) = version {
        cmd.arg("--version").arg(ver);
    }

    let status = cmd.status().await;
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => {
            error!(
                "cargo install failed with exit code: {}",
                s.code().unwrap_or(-1)
            );
            return ExitCode::from(1);
        }
        Err(e) => {
            error!("failed to run cargo install: {e}");
            return ExitCode::from(1);
        }
    }

    // Locate the installed binary in ~/.cargo/bin/.
    let binary_path = home_cargo_bin(&crate_name).unwrap_or_else(|| PathBuf::from(&crate_name));

    // Update connectors.toml.
    if let Err(e) = update_registry(connectors_file, short_name, &binary_path) {
        error!("failed to update connector registry: {e}");
        return ExitCode::from(1);
    }

    info!(
        connector = %short_name,
        path = %binary_path.display(),
        "connector installed successfully"
    );
    ExitCode::SUCCESS
}

/// Remove a connector from the registry and optionally delete its binary.
fn cmd_connectors_remove(name: &str, connectors_file: &Path) -> ExitCode {
    let mut registry = match load_connector_registry(connectors_file) {
        Ok(r) => r,
        Err(e) => {
            error!("connector registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let entry = registry.connectors.remove(name);
    if entry.is_none() {
        error!("connector '{name}' is not registered");
        return ExitCode::from(1);
    }

    // Try to delete the binary.
    if let Some(ref e) = entry {
        let path = Path::new(&e.path);
        if path.exists() {
            if let Err(err) = std::fs::remove_file(path) {
                error!("warning: could not delete binary {}: {err}", path.display());
            }
        }
    }

    if let Err(e) = write_registry(connectors_file, &registry) {
        error!("failed to write connector registry: {e}");
        return ExitCode::from(1);
    }

    info!(connector = %name, "connector removed");
    ExitCode::SUCCESS
}

/// Resolve `~/.cargo/bin/<name>` if it exists.
fn home_cargo_bin(crate_name: &str) -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    let p = PathBuf::from(home).join(".cargo/bin").join(crate_name);
    if p.exists() {
        Some(p)
    } else {
        None
    }
}

/// Add or update a connector entry in the registry file.
fn update_registry(path: &Path, name: &str, binary: &Path) -> Result<(), String> {
    let mut registry = load_connector_registry(path).unwrap_or_default();
    registry.connectors.insert(
        name.to_string(),
        ConnectorEntry {
            path: binary.display().to_string(),
            description: format!("Installed connector: pipeliner-connector-{name}"),
        },
    );
    write_registry(path, &registry)
}

/// Serialize and write the registry to disk.
fn write_registry(path: &Path, registry: &ConnectorRegistry) -> Result<(), String> {
    let content =
        toml::to_string_pretty(registry).map_err(|e| format!("TOML serialize error: {e}"))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("cannot create directory {}: {e}", parent.display()))?;
    }
    std::fs::write(path, content).map_err(|e| format!("cannot write {}: {e}", path.display()))
}

/// Print a machine-readable JSON specification for AI-assisted pipeline creation.
///
/// This gives AI tools everything they need to generate valid pipeline TOML configs:
/// connectors, DSL syntax, config schemas, and examples.
fn cmd_ai_spec(connectors_file: &Path) -> ExitCode {
    let registry = load_connector_registry(connectors_file).unwrap_or_default();

    // Build installed connectors list from registry.
    let installed: serde_json::Value = registry
        .connectors
        .iter()
        .map(|(name, entry)| {
            serde_json::json!({
                "name": name,
                "description": entry.description,
            })
        })
        .collect();

    let spec = serde_json::json!({
        "version": "1",
        "description": "pipeliner pipeline configuration specification for AI-assisted pipeline creation",
        "config_format": "toml",
        "installed_connectors": installed,
        "source_connectors": {
            "file": {
                "description": "Read CSV, JSON, or Parquet files from local filesystem or cloud storage (S3, Azure Blob, GCS)",
                "config": {
                    "path": { "type": "string", "required": true, "description": "File path or glob pattern" },
                    "format": { "type": "string", "required": true, "enum": ["csv", "json", "parquet"], "description": "File format" },
                    "csv": {
                        "type": "object", "required": false, "description": "CSV-specific options",
                        "fields": {
                            "delimiter": { "type": "char", "default": ",", "description": "Field delimiter" },
                            "quote": { "type": "char", "default": "\"", "description": "Quote character" },
                            "has_header": { "type": "bool", "default": true, "description": "First row is header" }
                        }
                    },
                    "storage": {
                        "type": "object", "required": false, "description": "Cloud storage backend",
                        "variants": {
                            "local": { "description": "Local filesystem (default)" },
                            "s3": { "fields": { "region": "string (optional)", "endpoint": "string (optional, for S3-compatible)" } },
                            "azure": { "fields": { "connection_string": "string (optional)" } },
                            "gcs": { "fields": { "project_id": "string (optional)" } }
                        }
                    }
                }
            },
            "sql": {
                "description": "Query PostgreSQL, MySQL, or SQL Server databases",
                "config": {
                    "driver": { "type": "string", "required": true, "enum": ["postgres", "mysql", "sqlserver"] },
                    "connection_string": { "type": "string", "required": true, "description": "Database connection string (use ${ENV_VAR} for secrets)" },
                    "query": { "type": "string", "required": true, "description": "SQL SELECT query" },
                    "watermark_column": { "type": "string", "required": false, "description": "Column for incremental load watermark" },
                    "partition": {
                        "type": "object", "required": false,
                        "strategies": {
                            "single": "No partitioning (default)",
                            "date_range": { "fields": { "column": "string", "start": "ISO 8601 date", "end": "ISO 8601 date", "interval": "e.g. 1d, 7d, 1m" } },
                            "key_range": { "fields": { "column": "string", "values": "array of strings" } }
                        }
                    }
                }
            },
            "rest": {
                "description": "Fetch data from REST APIs with pagination, auth, and rate limiting",
                "config": {
                    "base_url": { "type": "string", "required": true },
                    "endpoint": { "type": "string", "required": true },
                    "method": { "type": "string", "default": "GET", "enum": ["GET", "POST"] },
                    "headers": { "type": "map<string,string>", "required": false },
                    "query_params": { "type": "map<string,string>", "required": false },
                    "body_template": { "type": "string", "required": false, "description": "Request body for POST" },
                    "auth": {
                        "type": "object", "required": false,
                        "variants": {
                            "bearer": { "fields": { "token": "string" } },
                            "api_key": { "fields": { "header": "string (optional)", "query_param": "string (optional)", "value": "string" } },
                            "oauth2": { "fields": { "token_url": "string", "client_id": "string", "client_secret": "string", "scope": "string (optional)" } }
                        }
                    },
                    "pagination": {
                        "type": "object", "required": false,
                        "variants": {
                            "cursor": { "fields": { "cursor_field": "string", "cursor_param": "string" } },
                            "offset": { "fields": { "offset_param": "string", "limit_param": "string", "limit": "integer" } },
                            "page_number": { "fields": { "page_param": "string", "page_size_param": "string (optional)", "page_size": "integer (optional)" } },
                            "link_header": "RFC 8288 Link header pagination"
                        }
                    },
                    "response_mapping": {
                        "type": "object", "required": true,
                        "fields": {
                            "records_path": { "type": "string", "description": "Dot-path to records array in response" },
                            "total_path": { "type": "string", "required": false },
                            "cursor_path": { "type": "string", "required": false }
                        }
                    },
                    "rate_limit": { "type": "object", "required": false, "fields": { "requests_per_second": "float" } },
                    "retry": { "type": "object", "required": false, "fields": { "max_retries": "int (default 3)", "initial_backoff_ms": "int (default 1000)", "max_backoff_ms": "int (default 30000)" } },
                    "watermark_field": { "type": "string", "required": false }
                }
            }
        },
        "sink_connectors": {
            "file": {
                "description": "Write CSV or JSON files",
                "config": {
                    "path": { "type": "string", "required": true },
                    "format": { "type": "string", "required": true, "enum": ["csv", "json"] }
                }
            },
            "sql": {
                "description": "Write to PostgreSQL, MySQL, or SQL Server",
                "config": {
                    "driver": { "type": "string", "required": true, "enum": ["postgres", "mysql", "sqlserver"] },
                    "connection_string": { "type": "string", "required": true },
                    "table": { "type": "string", "required": true },
                    "write_mode": { "type": "string", "default": "insert", "enum": ["insert", "upsert", "truncate_and_load"] },
                    "merge_keys": { "type": "array<string>", "required": false, "description": "Required for upsert mode" },
                    "batch_size": { "type": "integer", "default": 1000 }
                }
            },
            "delta": {
                "description": "Write to Delta Lake tables (S3, Azure, GCS, or local)",
                "config": {
                    "table_uri": { "type": "string", "required": true, "description": "s3://, az://, gs://, or local path" },
                    "write_mode": { "type": "string", "default": "append", "enum": ["append", "overwrite", "merge"] },
                    "partition_columns": { "type": "array<string>", "required": false },
                    "merge_keys": { "type": "array<string>", "required": false, "description": "Required for merge mode" },
                    "merge_predicate": { "type": "string", "required": false }
                }
            },
            "parquet": {
                "description": "Write Parquet files with optional compression and partitioning",
                "config": {
                    "path": { "type": "string", "required": true },
                    "compression": { "type": "string", "default": "snappy", "enum": ["snappy", "zstd", "gzip", "none"] },
                    "partition_columns": { "type": "array<string>", "required": false },
                    "nested_handling": { "type": "string", "default": "json_string", "enum": ["flatten", "json_string"] }
                }
            }
        },
        "dsl": {
            "steps": {
                "set": { "syntax": "set(.field_path, expression)", "description": "Evaluate expression and assign result to field" },
                "rename": { "syntax": "rename(.old_name, .new_name)", "description": "Rename a top-level field" },
                "remove": { "syntax": "remove(.field_path)", "description": "Delete a top-level field" },
                "where": { "syntax": "where(predicate)", "description": "Keep only records where predicate is true" }
            },
            "functions": {
                "to_string": { "signature": "to_string(value) -> String", "description": "Convert any value to string" },
                "to_int": { "signature": "to_int(value) -> Int", "description": "Convert string/float/bool to integer" },
                "to_float": { "signature": "to_float(value) -> Float", "description": "Convert string/int to float" },
                "to_bool": { "signature": "to_bool(value) -> Bool", "description": "Convert string/int to boolean (recognizes true/false/yes/no/1/0)" },
                "to_timestamp": { "signature": "to_timestamp(string, format) -> Timestamp", "description": "Parse string to UTC timestamp (chrono format)" },
                "parse_timestamp": { "signature": "parse_timestamp(string, format) -> Timestamp", "description": "Alias for to_timestamp" },
                "is_null": { "signature": "is_null(value) -> Bool", "description": "Check if value is null" },
                "concat": { "signature": "concat(str1, str2, ...) -> String", "description": "Concatenate strings (variadic)" },
                "coalesce": { "signature": "coalesce(val1, val2, ...) -> Value", "description": "Return first non-null argument (variadic)" }
            },
            "expressions": {
                "field_access": ".field, .nested.field, .array[0], .[\"quoted key\"]",
                "literals": "42, 3.14, \"string\", true, false, null",
                "arithmetic": "+, -, *, /",
                "comparison": "==, !=, >, >=, <, <=",
                "logical": "&&, ||",
                "null_coalesce": ".field ?? \"default\"",
                "conditional": "if expr { then_expr } else { else_expr }"
            }
        },
        "optional_sections": {
            "dead_letter": {
                "description": "Route failed records to a separate sink instead of dropping them",
                "config": "Same as any sink connector config. Failed records get __error_step, __error_message, __error_timestamp fields."
            },
            "telemetry": {
                "description": "OpenTelemetry tracing and metrics",
                "config": {
                    "enabled": { "type": "bool", "default": false },
                    "otlp_endpoint": { "type": "string", "default": "http://localhost:4317" },
                    "service_name": { "type": "string", "default": "pipeliner" }
                }
            }
        },
        "env_var_substitution": "Use ${VAR_NAME} in any string value. The variable must be set at runtime.",
        "validation_command": "pipeliner validate <pipeline.toml>",
        "examples": [
            {
                "name": "CSV to JSON with transforms",
                "toml": "[pipeline]\nname = \"sales_etl\"\n\n[source]\nconnector = \"file\"\n[source.config]\npath = \"data/sales.csv\"\nformat = \"csv\"\n\n[[transforms]]\nname = \"clean\"\nsteps = [\n    'set(.amount, to_float(.amount))',\n    'set(.quantity, to_int(.quantity))',\n    'set(.total, .amount * to_float(.quantity))',\n    'where(.total > 0.0)',\n]\n\n[[sinks]]\nconnector = \"file\"\n[sinks.config]\npath = \"output/sales.json\"\nformat = \"json\"\n"
            },
            {
                "name": "PostgreSQL to Delta Lake",
                "toml": "[pipeline]\nname = \"orders_to_lake\"\n\n[source]\nconnector = \"sql\"\n[source.config]\ndriver = \"postgres\"\nconnection_string = \"${DATABASE_URL}\"\nquery = \"SELECT * FROM orders WHERE updated_at > '${WATERMARK}'\"\nwatermark_column = \"updated_at\"\n\n[[transforms]]\nname = \"enrich\"\nsteps = [\n    'set(.region, coalesce(.region, \"unknown\"))',\n    'set(.order_date, to_timestamp(.created_at, \"%Y-%m-%d %H:%M:%S\"))',\n    'remove(.internal_notes)',\n]\n\n[[sinks]]\nconnector = \"delta\"\n[sinks.config]\ntable_uri = \"s3://data-lake/orders\"\nwrite_mode = \"merge\"\nmerge_keys = [\"order_id\"]\n\n[dead_letter]\nconnector = \"file\"\n[dead_letter.config]\npath = \"errors/orders_dead_letters.json\"\nformat = \"json\"\n"
            },
            {
                "name": "REST API to Parquet",
                "toml": "[pipeline]\nname = \"api_ingest\"\n\n[source]\nconnector = \"rest\"\n[source.config]\nbase_url = \"https://api.example.com\"\nendpoint = \"/v1/events\"\n[source.config.auth]\ntype = \"bearer\"\ntoken = \"${API_TOKEN}\"\n[source.config.pagination]\ntype = \"cursor\"\ncursor_field = \"next_cursor\"\ncursor_param = \"cursor\"\n[source.config.response_mapping]\nrecords_path = \"data\"\n[source.config.rate_limit]\nrequests_per_second = 5.0\n\n[[transforms]]\nname = \"flatten\"\nsteps = [\n    'set(.event_type, .metadata.type)',\n    'set(.timestamp, to_timestamp(.created_at, \"%+\"))',\n    'remove(.metadata)',\n]\n\n[[sinks]]\nconnector = \"parquet\"\n[sinks.config]\npath = \"output/events.parquet\"\ncompression = \"zstd\"\n"
            }
        ],
        "ai_instructions": {
            "workflow": [
                "1. Ask the user: What is your data source? (file, database, API)",
                "2. Ask: What is the source location/connection details?",
                "3. Ask: What transformations do you need? (type conversions, filtering, computed fields, renaming)",
                "4. Ask: Where should the output go? (file, database, data lake)",
                "5. Ask: Do you need error handling (dead-letter sink)?",
                "6. Generate the TOML config",
                "7. Suggest running `pipeliner validate <file>` to verify"
            ],
            "rules": [
                "Always use ${ENV_VAR} for secrets — never hardcode credentials",
                "At least one [[sinks]] entry is required",
                "Transforms are optional — omit if no data transformation needed",
                "Use the correct connector config fields from this spec",
                "Validate generated configs with `pipeliner validate`"
            ]
        }
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&spec).expect("json serialization")
    );
    ExitCode::SUCCESS
}

/// Start the gRPC server in sidecar mode.
async fn cmd_serve(port: u16, connectors_file: &Path) -> ExitCode {
    let server = match pipeliner_core::server::PipelineRServer::from_registry_path(connectors_file)
    {
        Ok(s) => s,
        Err(e) => {
            error!("failed to load connector registry: {e}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = pipeliner_core::server::start_server(server, port).await {
        error!("server error: {e}");
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}
