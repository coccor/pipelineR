//! pipeliner CLI — execute, validate, and inspect pipelines from the command line.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing::{error, info};

use pipeliner_core::config::{
    build_pipeline, build_runtime_params, load_pipeline_config, load_plugin_registry,
    resolve_plugin_binary, validate_config,
};
use pipeliner_core::plugin::{PluginProcess, SourcePluginClientWrapper};
use pipeliner_core::runtime::execute_pipeline;
use pipeliner_proto::SourceConfig;

/// pipeliner — a config-driven batch ETL engine.
#[derive(Parser)]
#[command(name = "pipeliner", version, about)]
struct Cli {
    /// Path to plugins.toml registry file.
    #[arg(long, default_value = "plugins.toml")]
    plugins_file: PathBuf,

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
    /// Plugin management commands.
    Plugins {
        #[command(subcommand)]
        command: PluginsCommands,
    },
}

#[derive(Subcommand)]
enum PluginsCommands {
    /// List registered plugins.
    List,
}

/// Parse a `key=value` parameter string.
fn parse_param(s: &str) -> Result<(String, String), String> {
    let pos = s.find('=').ok_or_else(|| {
        format!("invalid param '{s}': expected format key=value")
    })?;
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
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .init();
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logging(&cli.log_level, cli.json_log);

    match cli.command {
        Commands::Run { pipeline, params } => {
            cmd_run(&pipeline, &params, &cli.plugins_file).await
        }
        Commands::Validate { pipeline } => {
            cmd_validate(&pipeline, &cli.plugins_file)
        }
        Commands::Schema { pipeline, params } => {
            cmd_schema(&pipeline, &params, &cli.plugins_file).await
        }
        Commands::Partitions { pipeline, params } => {
            cmd_partitions(&pipeline, &params, &cli.plugins_file).await
        }
        Commands::Serve { port } => cmd_serve(port, &cli.plugins_file).await,
        Commands::Plugins { command } => match command {
            PluginsCommands::List => cmd_plugins_list(&cli.plugins_file),
        },
    }
}

/// Execute a pipeline end-to-end.
async fn cmd_run(
    pipeline_path: &Path,
    params: &[(String, String)],
    plugins_file: &Path,
) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_plugin_registry(plugins_file) {
        Ok(r) => r,
        Err(e) => {
            error!("plugin registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let runtime_params = build_runtime_params(params);

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
fn cmd_validate(pipeline_path: &Path, plugins_file: &Path) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_plugin_registry(plugins_file) {
        Ok(r) => r,
        Err(e) => {
            error!("plugin registry error: {e}");
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
    plugins_file: &Path,
) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_plugin_registry(plugins_file) {
        Ok(r) => r,
        Err(e) => {
            error!("plugin registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let source_binary = match resolve_plugin_binary(&config.source.plugin, &registry) {
        Ok(p) => p,
        Err(e) => {
            error!("source plugin: {e}");
            return ExitCode::from(2);
        }
    };

    let mut process = match PluginProcess::spawn(
        &config.source.plugin,
        source_binary.to_str().unwrap_or_default(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            error!("failed to spawn source plugin: {e}");
            return ExitCode::from(1);
        }
    };

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = match SourcePluginClientWrapper::connect(process.address()).await {
        Ok(c) => c,
        Err(e) => {
            error!("failed to connect to source plugin: {e}");
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
            println!("{}", serde_json::to_string_pretty(&json).expect("json serialization"));
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
    plugins_file: &Path,
) -> ExitCode {
    let config = match load_pipeline_config(pipeline_path) {
        Ok(c) => c,
        Err(e) => {
            error!("config error: {e}");
            return ExitCode::from(2);
        }
    };

    let registry = match load_plugin_registry(plugins_file) {
        Ok(r) => r,
        Err(e) => {
            error!("plugin registry error: {e}");
            return ExitCode::from(2);
        }
    };

    let source_binary = match resolve_plugin_binary(&config.source.plugin, &registry) {
        Ok(p) => p,
        Err(e) => {
            error!("source plugin: {e}");
            return ExitCode::from(2);
        }
    };

    let mut process = match PluginProcess::spawn(
        &config.source.plugin,
        source_binary.to_str().unwrap_or_default(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            error!("failed to spawn source plugin: {e}");
            return ExitCode::from(1);
        }
    };

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = match SourcePluginClientWrapper::connect(process.address()).await {
        Ok(c) => c,
        Err(e) => {
            error!("failed to connect to source plugin: {e}");
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
            println!("{}", serde_json::to_string_pretty(&json).expect("json serialization"));
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

/// List registered plugins from the plugin registry.
fn cmd_plugins_list(plugins_file: &Path) -> ExitCode {
    let registry = match load_plugin_registry(plugins_file) {
        Ok(r) => r,
        Err(e) => {
            error!("plugin registry error: {e}");
            return ExitCode::from(2);
        }
    };

    if registry.plugins.is_empty() {
        println!("No plugins registered.");
        println!("Create a plugins.toml file or use --plugins-file to specify one.");
    } else {
        println!("{name:<20} {path:<40} DESCRIPTION", name = "NAME", path = "PATH");
        println!("{}", "-".repeat(80));
        for (name, entry) in &registry.plugins {
            println!("{name:<20} {path:<40} {desc}", path = entry.path, desc = entry.description);
        }
    }

    ExitCode::SUCCESS
}

/// Start the gRPC server in sidecar mode.
async fn cmd_serve(port: u16, plugins_file: &Path) -> ExitCode {
    let server = match pipeliner_core::server::PipelineRServer::from_registry_path(plugins_file) {
        Ok(s) => s,
        Err(e) => {
            error!("failed to load plugin registry: {e}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = pipeliner_core::server::start_server(server, port).await {
        error!("server error: {e}");
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}
