//! Pipeline runtime — orchestrates source → transform chain → sink fan-out.
//!
//! Uses bounded async channels between stages for backpressure.
//! Supports optional cancellation (via `CancellationToken`) and event emission
//! (via `RunEventSender`) for the gRPC server's `WatchRun` streaming RPC.

use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use pipeliner_proto::pipeliner::v1::load_request::Payload;
use pipeliner_proto::pipeliner::v1::{LoadMetadata, LoadRequest};
use pipeliner_proto::{RecordBatch as ProtoRecordBatch, RuntimeParams, SinkConfig, SourceConfig};

use crate::convert::{core_batch_to_proto, proto_batch_to_core};
use crate::dsl::ast::TransformStep;
use crate::dsl::step::execute_step;
use crate::error::PipelineError;
use crate::connector::{SinkConnectorClientWrapper, SourceConnectorClientWrapper};

/// Bounded channel capacity between pipeline stages.
const STAGE_CHANNEL_CAPACITY: usize = 32;

/// Events emitted during pipeline execution for `WatchRun`.
#[derive(Debug, Clone)]
pub enum RunEvent {
    /// A pipeline stage started or finished.
    StageTransition {
        /// Stage name (e.g., "source", "transform", "sink-0").
        stage: String,
        /// Status (e.g., "started", "completed", "failed").
        status: String,
    },
    /// A batch was processed by a stage.
    BatchProgress {
        /// Stage name.
        stage: String,
        /// Batch sequence number (1-based).
        batch_number: i64,
        /// Number of records in the batch.
        records_in_batch: i64,
    },
    /// An error occurred in a stage.
    Error {
        /// Stage name.
        stage: String,
        /// Error message.
        message: String,
    },
}

/// Sender for pipeline run events. Wraps a broadcast channel.
///
/// Events are best-effort: if no receiver is listening, events are silently dropped.
#[derive(Clone)]
pub struct RunEventSender {
    tx: broadcast::Sender<RunEvent>,
}

impl RunEventSender {
    /// Create a new event sender/receiver pair.
    pub fn new(capacity: usize) -> (Self, broadcast::Receiver<RunEvent>) {
        let (tx, rx) = broadcast::channel(capacity);
        (Self { tx }, rx)
    }

    /// Emit an event (best-effort; does not fail if no receivers).
    pub fn emit(&self, event: RunEvent) {
        // Ignore send error — no active receivers is OK.
        let _ = self.tx.send(event);
    }

    /// Subscribe to events. Returns a new receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.tx.subscribe()
    }
}

/// Result of a single sink's load operation.
#[derive(Debug, Clone)]
pub struct SinkResult {
    /// Sink index (0-based).
    pub index: usize,
    /// Number of rows successfully written.
    pub rows_written: i64,
    /// Number of rows that failed to write.
    pub rows_errored: i64,
    /// Error message, if any.
    pub error_message: String,
}

/// Result of a pipeline execution.
#[derive(Debug)]
pub struct PipelineResult {
    /// Watermark from the source (e.g., max last-modified timestamp).
    pub watermark: String,
    /// Results from each sink, in the order they were configured.
    pub sink_results: Vec<SinkResult>,
}

/// A pipeline definition: source, transforms, and sinks connected over gRPC.
pub struct PipelineDefinition {
    /// The source connector gRPC client.
    pub source: SourceConnectorClientWrapper,
    /// Source configuration (JSON-encoded).
    pub source_config: SourceConfig,
    /// Runtime parameters for the source (e.g., partition params).
    pub source_params: RuntimeParams,
    /// Ordered list of DSL transform steps to apply.
    pub transforms: Vec<TransformStep>,
    /// Sink plugin gRPC clients, one per configured sink.
    pub sinks: Vec<SinkConnectorClientWrapper>,
    /// Sink configurations, one per sink (parallel to `sinks`).
    pub sink_configs: Vec<SinkConfig>,
}

/// Execute a pipeline without cancellation or event emission.
///
/// This is the simple API for CLI usage and tests.
///
/// # Errors
///
/// Returns `PipelineError` if any stage fails.
pub async fn execute_pipeline(
    pipeline: PipelineDefinition,
) -> Result<PipelineResult, PipelineError> {
    execute_pipeline_with(pipeline, CancellationToken::new(), None).await
}

/// Execute a pipeline with cancellation support and optional event emission.
///
/// - `cancel`: a `CancellationToken` that, when triggered, stops the pipeline between batches.
/// - `events`: optional event sender for streaming progress to watchers.
///
/// # Errors
///
/// Returns `PipelineError` if any stage fails or the run is cancelled.
pub async fn execute_pipeline_with(
    mut pipeline: PipelineDefinition,
    cancel: CancellationToken,
    events: Option<RunEventSender>,
) -> Result<PipelineResult, PipelineError> {
    let num_sinks = pipeline.sinks.len();

    // Helper to emit events.
    let emit = |evt: RunEvent| {
        if let Some(ref e) = events {
            e.emit(evt);
        }
    };

    // --- Stage 1: Source extraction → transform input channel ---
    let (source_tx, source_rx) = mpsc::channel::<ProtoRecordBatch>(STAGE_CHANNEL_CAPACITY);

    let source_config = pipeline.source_config.clone();
    let source_params = pipeline.source_params.clone();
    let source_cancel = cancel.clone();
    let source_events = events.clone();
    let source_handle = tokio::spawn(async move {
        run_source(
            &mut pipeline.source,
            source_config,
            source_params,
            source_tx,
            source_cancel,
            source_events,
        )
        .await
    });

    emit(RunEvent::StageTransition {
        stage: "source".to_string(),
        status: "started".to_string(),
    });

    // --- Stage 2: Transform → sink fan-out channels ---
    let mut sink_txs: Vec<mpsc::Sender<ProtoRecordBatch>> = Vec::with_capacity(num_sinks);
    let mut sink_rxs: Vec<mpsc::Receiver<ProtoRecordBatch>> = Vec::with_capacity(num_sinks);
    for _ in 0..num_sinks {
        let (tx, rx) = mpsc::channel::<ProtoRecordBatch>(STAGE_CHANNEL_CAPACITY);
        sink_txs.push(tx);
        sink_rxs.push(rx);
    }

    let transforms = pipeline.transforms;
    let transform_cancel = cancel.clone();
    let transform_events = events.clone();
    let transform_handle = tokio::spawn(async move {
        run_transforms(source_rx, &transforms, sink_txs, transform_cancel, transform_events).await
    });

    // --- Stage 3: Sink load tasks ---
    let mut sink_handles = Vec::with_capacity(num_sinks);
    for (i, (mut client, (config, rx))) in pipeline
        .sinks
        .into_iter()
        .zip(pipeline.sink_configs.into_iter().zip(sink_rxs))
        .enumerate()
    {
        let sink_events = events.clone();
        sink_handles.push(tokio::spawn(async move {
            run_sink(i, &mut client, config, rx, sink_events).await
        }));
    }

    // --- Collect results ---
    let watermark = source_handle
        .await
        .map_err(|e| PipelineError::Source(format!("source task panicked: {e}")))?
        .map_err(|e| PipelineError::Source(e.to_string()))?;

    emit(RunEvent::StageTransition {
        stage: "source".to_string(),
        status: "completed".to_string(),
    });

    transform_handle
        .await
        .map_err(|e| {
            PipelineError::Transform(crate::dsl::error::TransformError::StepFailed {
                step: "transform_task".to_string(),
                message: format!("transform task panicked: {e}"),
            })
        })?
        .map_err(PipelineError::Transform)?;

    emit(RunEvent::StageTransition {
        stage: "transform".to_string(),
        status: "completed".to_string(),
    });

    let mut sink_results = Vec::with_capacity(sink_handles.len());
    for handle in sink_handles {
        let result = handle
            .await
            .map_err(|e| PipelineError::Sink(format!("sink task panicked: {e}")))?
            .map_err(|e| PipelineError::Sink(e.to_string()))?;
        sink_results.push(result);
    }

    Ok(PipelineResult {
        watermark,
        sink_results,
    })
}

/// Extract from source and send batches to the transform stage.
async fn run_source(
    client: &mut SourceConnectorClientWrapper,
    config: SourceConfig,
    params: RuntimeParams,
    tx: mpsc::Sender<ProtoRecordBatch>,
    cancel: CancellationToken,
    events: Option<RunEventSender>,
) -> Result<String, PipelineError> {
    let mut stream = client.extract(config, params).await?;

    let mut watermark = String::new();
    let mut batch_num: i64 = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(PipelineError::Source("cancelled".to_string()));
        }

        match stream.next().await {
            Some(Ok(resp)) => {
                if let Some(batch) = resp.batch {
                    batch_num += 1;
                    let record_count = batch.records.len() as i64;
                    tx.send(batch).await.map_err(|_| {
                        PipelineError::ChannelClosed("source → transform".to_string())
                    })?;
                    if let Some(ref e) = events {
                        e.emit(RunEvent::BatchProgress {
                            stage: "source".to_string(),
                            batch_number: batch_num,
                            records_in_batch: record_count,
                        });
                    }
                }
                if !resp.watermark.is_empty() {
                    watermark = resp.watermark;
                }
            }
            Some(Err(e)) => return Err(PipelineError::Grpc(e)),
            None => break,
        }
    }

    Ok(watermark)
}

/// Apply transform steps to each batch, then fan out to all sinks.
async fn run_transforms(
    mut rx: mpsc::Receiver<ProtoRecordBatch>,
    transforms: &[TransformStep],
    sink_txs: Vec<mpsc::Sender<ProtoRecordBatch>>,
    cancel: CancellationToken,
    events: Option<RunEventSender>,
) -> Result<(), crate::dsl::error::TransformError> {
    if let Some(ref e) = events {
        e.emit(RunEvent::StageTransition {
            stage: "transform".to_string(),
            status: "started".to_string(),
        });
    }

    let mut batch_num: i64 = 0;
    while let Some(proto_batch) = rx.recv().await {
        if cancel.is_cancelled() {
            break;
        }

        let mut core_batch = proto_batch_to_core(&proto_batch);

        // Apply each transform step in order.
        for step in transforms {
            execute_step(step, &mut core_batch.records)?;
        }

        batch_num += 1;

        // Skip empty batches (e.g., all records filtered by `where`).
        if core_batch.records.is_empty() {
            continue;
        }

        let record_count = core_batch.records.len() as i64;
        let transformed = core_batch_to_proto(&core_batch);

        if let Some(ref e) = events {
            e.emit(RunEvent::BatchProgress {
                stage: "transform".to_string(),
                batch_number: batch_num,
                records_in_batch: record_count,
            });
        }

        // Fan out: clone and send to each sink.
        for tx in &sink_txs {
            // Ignore send errors — sink may have already failed and dropped its receiver.
            let _ = tx.send(transformed.clone()).await;
        }
    }

    // Drop senders to signal end-of-stream to sinks.
    drop(sink_txs);
    Ok(())
}

/// Send data to a single sink via gRPC client-streaming.
async fn run_sink(
    index: usize,
    client: &mut SinkConnectorClientWrapper,
    config: SinkConfig,
    mut rx: mpsc::Receiver<ProtoRecordBatch>,
    events: Option<RunEventSender>,
) -> Result<SinkResult, PipelineError> {
    let stage_name = format!("sink-{index}");
    if let Some(ref e) = events {
        e.emit(RunEvent::StageTransition {
            stage: stage_name.clone(),
            status: "started".to_string(),
        });
    }

    // Build a stream: metadata first, then batches.
    let (stream_tx, stream_rx) = mpsc::channel::<LoadRequest>(STAGE_CHANNEL_CAPACITY);

    // Send metadata as the first message.
    let metadata_msg = LoadRequest {
        payload: Some(Payload::Metadata(LoadMetadata {
            config: Some(config),
            schema: None,
        })),
    };
    stream_tx
        .send(metadata_msg)
        .await
        .map_err(|_| PipelineError::ChannelClosed("sink stream metadata".to_string()))?;

    // Spawn a task to forward batches from the transform channel to the gRPC stream.
    let forward_handle = tokio::spawn(async move {
        while let Some(batch) = rx.recv().await {
            let msg = LoadRequest {
                payload: Some(Payload::Batch(batch)),
            };
            if stream_tx.send(msg).await.is_err() {
                break;
            }
        }
        // stream_tx is dropped here, closing the stream.
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(stream_rx);
    let load_result = client.load(stream).await?;

    forward_handle.await.ok();

    if let Some(ref e) = events {
        e.emit(RunEvent::StageTransition {
            stage: stage_name,
            status: "completed".to_string(),
        });
    }

    Ok(SinkResult {
        index,
        rows_written: load_result.rows_written,
        rows_errored: load_result.rows_errored,
        error_message: load_result.error_message,
    })
}
