//! Orchestrator-facing gRPC server implementation (`PipelineR` service).
//!
//! Manages concurrent pipeline runs with cancellation, event streaming,
//! and status polling. Each `RunPipeline` call spawns an isolated Tokio task.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use pipeliner_proto::pipeliner::v1::pipeline_r_server::PipelineR;
use pipeliner_proto::pipeliner::v1::{
    self as pb, CancelRunRequest, CancelRunResponse, GetRunStatusRequest, HealthRequest,
    HealthResponse, PipelineDiscoverPartitionsRequest, PipelineDiscoverSchemaRequest,
    RunPipelineRequest, RunPipelineResponse, ValidatePipelineRequest, WatchRunRequest,
};
use pipeliner_proto::{RuntimeParams, SchemaResponse, SourceConfig, ValidationResult};

use crate::config::{
    build_pipeline, build_runtime_params, load_plugin_registry, parse_pipeline_config,
    validate_config, PluginRegistry,
};
use crate::runtime::{execute_pipeline_with, RunEvent, RunEventSender};

/// In-memory state for a single pipeline run.
struct RunState {
    /// Current run status.
    status: pb::RunStatus,
    /// Cancellation token to stop the pipeline.
    cancel: CancellationToken,
    /// Event sender for WatchRun streaming. Set to `None` after the run completes
    /// to close the broadcast channel and end all WatchRun streams.
    events: Option<RunEventSender>,
    /// When the run was started.
    started_at: Instant,
}

/// Shared server state holding all active and completed runs.
pub struct PipelineRServer {
    /// All runs indexed by run_id.
    runs: RwLock<HashMap<String, Arc<Mutex<RunState>>>>,
    /// Plugin registry for resolving plugin binaries.
    registry: PluginRegistry,
}

impl PipelineRServer {
    /// Create a new server instance.
    pub fn new(registry: PluginRegistry) -> Self {
        Self {
            runs: RwLock::new(HashMap::new()),
            registry,
        }
    }

    /// Create a new server instance with default (empty) registry.
    pub fn with_default_registry() -> Self {
        Self::new(PluginRegistry::default())
    }

    /// Create a new server instance loading registry from a file path.
    ///
    /// If the file doesn't exist, uses an empty registry.
    pub fn from_registry_path(path: &Path) -> Result<Self, crate::config::ConfigError> {
        let registry = load_plugin_registry(path)?;
        Ok(Self::new(registry))
    }
}

/// Parse a pipeline config from a request's config oneof.
#[allow(clippy::result_large_err)]
fn parse_config_input(
    config_toml: Option<String>,
    config_path: Option<String>,
) -> Result<crate::config::PipelineConfig, Status> {
    // Determine which config variant was provided.
    let toml_str = match (config_toml, config_path) {
        (Some(toml), _) => toml,
        (None, Some(path)) => std::fs::read_to_string(&path).map_err(|e| {
            Status::invalid_argument(format!("failed to read config file '{path}': {e}"))
        })?,
        (None, None) => {
            return Err(Status::invalid_argument(
                "either config_toml or config_path must be set",
            ));
        }
    };

    parse_pipeline_config(&toml_str)
        .map_err(|e| Status::invalid_argument(format!("config parse error: {e}")))
}

/// Extract the config oneof from a RunPipelineRequest.
fn extract_run_config(
    req: &RunPipelineRequest,
) -> (Option<String>, Option<String>) {
    use pipeliner_proto::pipeliner::v1::run_pipeline_request::Config;
    match &req.config {
        Some(Config::ConfigToml(s)) => (Some(s.clone()), None),
        Some(Config::ConfigPath(s)) => (None, Some(s.clone())),
        None => (None, None),
    }
}

/// Extract the config oneof from a discover schema request.
fn extract_discover_schema_config(
    req: &PipelineDiscoverSchemaRequest,
) -> (Option<String>, Option<String>) {
    use pipeliner_proto::pipeliner::v1::pipeline_discover_schema_request::Config;
    match &req.config {
        Some(Config::ConfigToml(s)) => (Some(s.clone()), None),
        Some(Config::ConfigPath(s)) => (None, Some(s.clone())),
        None => (None, None),
    }
}

/// Extract the config oneof from a discover partitions request.
fn extract_discover_partitions_config(
    req: &PipelineDiscoverPartitionsRequest,
) -> (Option<String>, Option<String>) {
    use pipeliner_proto::pipeliner::v1::pipeline_discover_partitions_request::Config;
    match &req.config {
        Some(Config::ConfigToml(s)) => (Some(s.clone()), None),
        Some(Config::ConfigPath(s)) => (None, Some(s.clone())),
        None => (None, None),
    }
}

/// Extract the config oneof from a validate pipeline request.
fn extract_validate_config(
    req: &ValidatePipelineRequest,
) -> (Option<String>, Option<String>) {
    use pipeliner_proto::pipeliner::v1::validate_pipeline_request::Config;
    match &req.config {
        Some(Config::ConfigToml(s)) => (Some(s.clone()), None),
        Some(Config::ConfigPath(s)) => (None, Some(s.clone())),
        None => (None, None),
    }
}

/// Convert internal `RunEvent` to proto `RunEvent`.
fn to_proto_event(run_id: &str, evt: &RunEvent) -> pb::RunEvent {
    let event = match evt {
        RunEvent::StageTransition { stage, status } => {
            pb::run_event::Event::StageTransition(pb::StageTransition {
                stage: stage.clone(),
                status: status.clone(),
            })
        }
        RunEvent::BatchProgress {
            stage,
            batch_number,
            records_in_batch,
        } => pb::run_event::Event::BatchProgress(pb::BatchProgress {
            stage: stage.clone(),
            batch_number: *batch_number,
            records_in_batch: *records_in_batch,
        }),
        RunEvent::Error { stage, message } => {
            pb::run_event::Event::Error(pb::ErrorEvent {
                stage: stage.clone(),
                message: message.clone(),
            })
        }
    };
    pb::RunEvent {
        run_id: run_id.to_string(),
        event: Some(event),
    }
}

#[tonic::async_trait]
impl PipelineR for PipelineRServer {
    async fn discover_schema(
        &self,
        request: Request<PipelineDiscoverSchemaRequest>,
    ) -> Result<Response<SchemaResponse>, Status> {
        let req = request.into_inner();
        let (toml, path) = extract_discover_schema_config(&req);
        let config = parse_config_input(toml, path)?;
        let params: Vec<(String, String)> = req.params.into_iter().collect();
        let runtime_params = build_runtime_params(&params);

        // Resolve, spawn, connect to source plugin, call discover_schema.
        let source_binary =
            crate::config::resolve_plugin_binary(&config.source.plugin, &self.registry)
                .map_err(|e| Status::not_found(format!("source plugin: {e}")))?;

        let mut process = crate::plugin::PluginProcess::spawn(
            &config.source.plugin,
            source_binary.to_str().unwrap_or_default(),
        )
        .await
        .map_err(|e| Status::internal(format!("failed to spawn source plugin: {e}")))?;

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut client =
            crate::plugin::SourcePluginClientWrapper::connect(process.address())
                .await
                .map_err(|e| Status::internal(format!("failed to connect to source: {e}")))?;

        let source_config = SourceConfig {
            config_json: crate::config::toml_value_to_json(&config.source.config),
        };

        let result = client
            .discover_schema(source_config, runtime_params)
            .await
            .map_err(|e| Status::internal(format!("schema discovery failed: {e}")))?;

        process.kill().await.ok();
        Ok(Response::new(result))
    }

    async fn discover_partitions(
        &self,
        request: Request<PipelineDiscoverPartitionsRequest>,
    ) -> Result<Response<pipeliner_proto::PartitionsResponse>, Status> {
        let req = request.into_inner();
        let (toml, path) = extract_discover_partitions_config(&req);
        let config = parse_config_input(toml, path)?;
        let params: Vec<(String, String)> = req.params.into_iter().collect();
        let runtime_params = build_runtime_params(&params);

        let source_binary =
            crate::config::resolve_plugin_binary(&config.source.plugin, &self.registry)
                .map_err(|e| Status::not_found(format!("source plugin: {e}")))?;

        let mut process = crate::plugin::PluginProcess::spawn(
            &config.source.plugin,
            source_binary.to_str().unwrap_or_default(),
        )
        .await
        .map_err(|e| Status::internal(format!("failed to spawn source plugin: {e}")))?;

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut client =
            crate::plugin::SourcePluginClientWrapper::connect(process.address())
                .await
                .map_err(|e| Status::internal(format!("failed to connect to source: {e}")))?;

        let source_config = SourceConfig {
            config_json: crate::config::toml_value_to_json(&config.source.config),
        };

        let result = client
            .discover_partitions(source_config, runtime_params)
            .await
            .map_err(|e| Status::internal(format!("partition discovery failed: {e}")))?;

        process.kill().await.ok();
        Ok(Response::new(result))
    }

    async fn run_pipeline(
        &self,
        request: Request<RunPipelineRequest>,
    ) -> Result<Response<RunPipelineResponse>, Status> {
        let req = request.into_inner();
        let (toml, path) = extract_run_config(&req);
        let config = parse_config_input(toml, path)?;
        let params: Vec<(String, String)> = req.params.into_iter().collect();
        let runtime_params = build_runtime_params(&params);

        let run_id = Uuid::new_v4().to_string();
        let cancel = CancellationToken::new();
        let (event_sender, _) = RunEventSender::new(256);

        let initial_status = pb::RunStatus {
            run_id: run_id.clone(),
            state: pb::RunState::Running as i32,
            error_message: String::new(),
            metrics: Some(pb::RunMetrics::default()),
            watermark: String::new(),
        };

        let run_state = Arc::new(Mutex::new(RunState {
            status: initial_status,
            cancel: cancel.clone(),
            events: Some(event_sender.clone()),
            started_at: Instant::now(),
        }));

        // Store run state.
        {
            let mut runs = self.runs.write().await;
            runs.insert(run_id.clone(), run_state.clone());
        }

        // Spawn the pipeline execution in a background task.
        let registry = self.registry.clone();
        let run_id_clone = run_id.clone();
        tokio::spawn(async move {
            let result = async {
                let proto_params = RuntimeParams {
                    params: runtime_params.params,
                };
                let (definition, spawned) =
                    build_pipeline(&config, &registry, proto_params).await?;
                let result =
                    execute_pipeline_with(definition, cancel.clone(), Some(event_sender.clone()))
                        .await;
                Ok::<_, crate::error::PipelineError>((result, spawned))
            }
            .await;

            // Drop the local event_sender clone so the broadcast channel can close
            // after we drop state.events below.
            drop(event_sender);

            let mut state = run_state.lock().await;
            let duration_ms = state.started_at.elapsed().as_millis() as i64;

            match result {
                Ok((Ok(pipeline_result), mut spawned)) => {
                    spawned.kill_all().await;

                    let total_written: i64 =
                        pipeline_result.sink_results.iter().map(|s| s.rows_written).sum();
                    let total_errored: i64 =
                        pipeline_result.sink_results.iter().map(|s| s.rows_errored).sum();

                    state.status.state = pb::RunState::Completed as i32;
                    state.status.watermark = pipeline_result.watermark;
                    state.status.metrics = Some(pb::RunMetrics {
                        records_written: total_written,
                        records_dropped: total_errored,
                        duration_ms,
                        ..Default::default()
                    });

                    // Emit completion event then drop sender to close WatchRun streams.
                    if let Some(ref e) = state.events {
                        e.emit(RunEvent::StageTransition {
                            stage: "pipeline".to_string(),
                            status: "completed".to_string(),
                        });
                    }
                    state.events = None;
                }
                Ok((Err(e), mut spawned)) => {
                    spawned.kill_all().await;

                    let is_cancelled = cancel.is_cancelled();
                    state.status.state = if is_cancelled {
                        pb::RunState::Cancelled as i32
                    } else {
                        pb::RunState::Failed as i32
                    };
                    state.status.error_message = e.to_string();
                    state.status.metrics = Some(pb::RunMetrics {
                        duration_ms,
                        ..Default::default()
                    });

                    if let Some(ref ev) = state.events {
                        ev.emit(RunEvent::Error {
                            stage: "pipeline".to_string(),
                            message: e.to_string(),
                        });
                    }
                    state.events = None;
                }
                Err(e) => {
                    state.status.state = pb::RunState::Failed as i32;
                    state.status.error_message = e.to_string();
                    state.status.metrics = Some(pb::RunMetrics {
                        duration_ms,
                        ..Default::default()
                    });

                    if let Some(ref ev) = state.events {
                        ev.emit(RunEvent::Error {
                            stage: "pipeline".to_string(),
                            message: e.to_string(),
                        });
                    }
                    state.events = None;
                }
            }
        });

        Ok(Response::new(RunPipelineResponse {
            run_id: run_id_clone,
        }))
    }

    type WatchRunStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::RunEvent, Status>> + Send>>;

    async fn watch_run(
        &self,
        request: Request<WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        let run_id = request.into_inner().run_id;

        let runs = self.runs.read().await;
        let run_state = runs
            .get(&run_id)
            .ok_or_else(|| Status::not_found(format!("run '{run_id}' not found")))?
            .clone();

        let state = run_state.lock().await;
        let current_state = state.status.state;
        let rx = state.events.as_ref().map(|e| e.subscribe());
        drop(state);

        // If the run is already finished or events are gone, return final status immediately.
        if current_state != pb::RunState::Running as i32 || rx.is_none() {
            let final_state = run_state.lock().await;
            let completed_event = pb::RunEvent {
                run_id: run_id.clone(),
                event: Some(pb::run_event::Event::Completed(final_state.status.clone())),
            };
            let stream = tokio_stream::once(Ok(completed_event));
            return Ok(Response::new(Box::pin(stream)));
        }

        let rx = rx.expect("checked above");
        let run_id_for_stream = run_id.clone();
        let run_state_for_final = run_state.clone();

        let event_stream = BroadcastStream::new(rx).filter_map(move |result| {
            let run_id = run_id_for_stream.clone();
            match result {
                Ok(evt) => Some(Ok(to_proto_event(&run_id, &evt))),
                Err(_) => None,
            }
        });

        // When the event stream ends (sender dropped), emit a final completed event.
        let run_id_final = run_id.clone();
        let final_event_stream = async_stream::stream! {
            tokio::pin!(event_stream);
            while let Some(item) = event_stream.next().await {
                yield item;
            }
            // Stream ended — emit final status.
            let state = run_state_for_final.lock().await;
            yield Ok(pb::RunEvent {
                run_id: run_id_final.clone(),
                event: Some(pb::run_event::Event::Completed(state.status.clone())),
            });
        };

        Ok(Response::new(Box::pin(final_event_stream)))
    }

    async fn get_run_status(
        &self,
        request: Request<GetRunStatusRequest>,
    ) -> Result<Response<pb::RunStatus>, Status> {
        let run_id = request.into_inner().run_id;

        let runs = self.runs.read().await;
        let run_state = runs
            .get(&run_id)
            .ok_or_else(|| Status::not_found(format!("run '{run_id}' not found")))?
            .clone();

        let state = run_state.lock().await;
        Ok(Response::new(state.status.clone()))
    }

    async fn cancel_run(
        &self,
        request: Request<CancelRunRequest>,
    ) -> Result<Response<CancelRunResponse>, Status> {
        let run_id = request.into_inner().run_id;

        let runs = self.runs.read().await;
        let run_state = runs
            .get(&run_id)
            .ok_or_else(|| Status::not_found(format!("run '{run_id}' not found")))?
            .clone();

        let state = run_state.lock().await;
        state.cancel.cancel();

        Ok(Response::new(CancelRunResponse {
            acknowledged: true,
        }))
    }

    async fn validate_pipeline(
        &self,
        request: Request<ValidatePipelineRequest>,
    ) -> Result<Response<ValidationResult>, Status> {
        let req = request.into_inner();
        let (toml, path) = extract_validate_config(&req);
        let config = parse_config_input(toml, path)?;

        let errors = validate_config(&config, &self.registry);
        Ok(Response::new(ValidationResult {
            valid: errors.is_empty(),
            errors,
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: pb::health_response::ServingStatus::Serving as i32,
        }))
    }
}

/// Start the PipelineR gRPC server on the given port.
///
/// Binds to `127.0.0.1:<port>` and serves until the process is terminated.
pub async fn start_server(
    server: PipelineRServer,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("127.0.0.1:{port}").parse()?;

    tracing::info!(%addr, "starting PipelineR gRPC server");

    tonic::transport::Server::builder()
        .add_service(
            pipeliner_proto::pipeliner::v1::pipeline_r_server::PipelineRServer::new(server)
                .max_decoding_message_size(64 * 1024 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .serve(addr)
        .await?;

    Ok(())
}
