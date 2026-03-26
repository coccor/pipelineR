//! Pipeline runtime — orchestrates source → transform chain → sink fan-out.
//!
//! Uses bounded async channels between stages for backpressure.

use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use pipeliner_proto::pipeliner::v1::load_request::Payload;
use pipeliner_proto::pipeliner::v1::{LoadMetadata, LoadRequest};
use pipeliner_proto::{RecordBatch as ProtoRecordBatch, RuntimeParams, SinkConfig, SourceConfig};

use crate::convert::{core_batch_to_proto, proto_batch_to_core};
use crate::dsl::ast::TransformStep;
use crate::dsl::step::execute_step;
use crate::error::PipelineError;
use crate::plugin::{SinkPluginClientWrapper, SourcePluginClientWrapper};

/// Bounded channel capacity between pipeline stages.
const STAGE_CHANNEL_CAPACITY: usize = 32;

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
    /// The source plugin gRPC client.
    pub source: SourcePluginClientWrapper,
    /// Source configuration (JSON-encoded).
    pub source_config: SourceConfig,
    /// Runtime parameters for the source (e.g., partition params).
    pub source_params: RuntimeParams,
    /// Ordered list of DSL transform steps to apply.
    pub transforms: Vec<TransformStep>,
    /// Sink plugin gRPC clients, one per configured sink.
    pub sinks: Vec<SinkPluginClientWrapper>,
    /// Sink configurations, one per sink (parallel to `sinks`).
    pub sink_configs: Vec<SinkConfig>,
}

/// Execute a pipeline: extract from source, apply transforms, fan out to sinks.
///
/// Returns the watermark and per-sink load results.
///
/// # Errors
///
/// Returns `PipelineError` if any stage fails.
pub async fn execute_pipeline(
    mut pipeline: PipelineDefinition,
) -> Result<PipelineResult, PipelineError> {
    let num_sinks = pipeline.sinks.len();

    // --- Stage 1: Source extraction → transform input channel ---
    let (source_tx, source_rx) = mpsc::channel::<ProtoRecordBatch>(STAGE_CHANNEL_CAPACITY);

    let source_config = pipeline.source_config.clone();
    let source_params = pipeline.source_params.clone();
    let source_handle = tokio::spawn(async move {
        run_source(&mut pipeline.source, source_config, source_params, source_tx).await
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
    let transform_handle = tokio::spawn(async move {
        run_transforms(source_rx, &transforms, sink_txs).await
    });

    // --- Stage 3: Sink load tasks ---
    let mut sink_handles = Vec::with_capacity(num_sinks);
    for (i, (mut client, (config, rx))) in pipeline
        .sinks
        .into_iter()
        .zip(pipeline.sink_configs.into_iter().zip(sink_rxs))
        .enumerate()
    {
        sink_handles.push(tokio::spawn(async move {
            run_sink(i, &mut client, config, rx).await
        }));
    }

    // --- Collect results ---
    let watermark = source_handle
        .await
        .map_err(|e| PipelineError::Source(format!("source task panicked: {e}")))?
        .map_err(|e| PipelineError::Source(e.to_string()))?;

    transform_handle
        .await
        .map_err(|e| PipelineError::Transform(crate::dsl::error::TransformError::StepFailed {
            step: "transform_task".to_string(),
            message: format!("transform task panicked: {e}"),
        }))?
        .map_err(PipelineError::Transform)?;

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
    client: &mut SourcePluginClientWrapper,
    config: SourceConfig,
    params: RuntimeParams,
    tx: mpsc::Sender<ProtoRecordBatch>,
) -> Result<String, PipelineError> {
    let mut stream = client.extract(config, params).await?;

    let mut watermark = String::new();
    while let Some(resp) = stream.next().await {
        let resp = resp?;
        if let Some(batch) = resp.batch {
            tx.send(batch)
                .await
                .map_err(|_| PipelineError::ChannelClosed("source → transform".to_string()))?;
        }
        if !resp.watermark.is_empty() {
            watermark = resp.watermark;
        }
    }

    Ok(watermark)
}

/// Apply transform steps to each batch, then fan out to all sinks.
async fn run_transforms(
    mut rx: mpsc::Receiver<ProtoRecordBatch>,
    transforms: &[TransformStep],
    sink_txs: Vec<mpsc::Sender<ProtoRecordBatch>>,
) -> Result<(), crate::dsl::error::TransformError> {
    while let Some(proto_batch) = rx.recv().await {
        let mut core_batch = proto_batch_to_core(&proto_batch);

        // Apply each transform step in order.
        for step in transforms {
            execute_step(step, &mut core_batch.records)?;
        }

        // Skip empty batches (e.g., all records filtered by `where`).
        if core_batch.records.is_empty() {
            continue;
        }

        let transformed = core_batch_to_proto(&core_batch);

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
    client: &mut SinkPluginClientWrapper,
    config: SinkConfig,
    mut rx: mpsc::Receiver<ProtoRecordBatch>,
) -> Result<SinkResult, PipelineError> {
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

    Ok(SinkResult {
        index,
        rows_written: load_result.rows_written,
        rows_errored: load_result.rows_errored,
        error_message: load_result.error_message,
    })
}
