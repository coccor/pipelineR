//! gRPC service bridge for the [`Sink`] trait.
//!
//! Wraps any `Sink` implementation so it can be served as a tonic `SinkPlugin` gRPC service.

use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

use pipeliner_proto::pipeliner::v1::sink_plugin_server::SinkPlugin;
use pipeliner_proto::{
    load_request, Empty, LoadRequest, SchemaRequirementResponse, SinkConfig, SinkDescriptor,
    ValidationResult,
};

use crate::sink::Sink;

/// A tonic gRPC service that delegates to a [`Sink`] trait implementation.
pub struct GrpcSinkService<K: Sink> {
    sink: Arc<K>,
}

impl<K: Sink> GrpcSinkService<K> {
    /// Create a new gRPC sink service wrapping the given sink.
    pub fn new(sink: K) -> Self {
        Self {
            sink: Arc::new(sink),
        }
    }
}

#[tonic::async_trait]
impl<K: Sink> SinkPlugin for GrpcSinkService<K> {
    /// Return metadata about this sink plugin.
    async fn describe(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SinkDescriptor>, Status> {
        Ok(Response::new(self.sink.describe()))
    }

    /// Validate the provided sink configuration.
    async fn validate(
        &self,
        request: Request<SinkConfig>,
    ) -> Result<Response<ValidationResult>, Status> {
        let config = request.into_inner();
        match self.sink.validate(&config).await {
            Ok(()) => Ok(Response::new(ValidationResult {
                valid: true,
                errors: vec![],
            })),
            Err(e) => Ok(Response::new(ValidationResult {
                valid: false,
                errors: vec![e.to_string()],
            })),
        }
    }

    /// Declare schema requirements for this sink.
    async fn schema_requirement(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SchemaRequirementResponse>, Status> {
        Ok(Response::new(self.sink.schema_requirement()))
    }

    /// Load data into the sink via a client-streaming request.
    ///
    /// The first message must contain `LoadMetadata` (config + schema).
    /// Subsequent messages carry `RecordBatch` payloads.
    async fn load(
        &self,
        request: Request<Streaming<LoadRequest>>,
    ) -> Result<Response<pipeliner_proto::LoadResult>, Status> {
        let mut stream = request.into_inner();

        // First message must be metadata.
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty load stream"))?;

        let metadata = match first.payload {
            Some(load_request::Payload::Metadata(m)) => m,
            _ => {
                return Err(Status::invalid_argument(
                    "first load message must be LoadMetadata",
                ));
            }
        };

        let config = metadata.config.unwrap_or_default();
        let schema = metadata.schema;

        let (batch_tx, batch_rx) = mpsc::channel::<pipeliner_proto::RecordBatch>(32);

        let sink = Arc::clone(&self.sink);
        let load_handle = tokio::spawn(async move {
            sink.load(&config, schema, batch_rx).await
        });

        // Forward remaining stream messages to the channel.
        while let Some(msg) = stream.message().await? {
            if let Some(load_request::Payload::Batch(batch)) = msg.payload {
                if batch_tx.send(batch).await.is_err() {
                    // Sink task dropped its receiver — stop forwarding.
                    break;
                }
            }
            // Ignore unexpected metadata messages after the first.
        }

        // Drop sender to signal end of stream to the sink.
        drop(batch_tx);

        let result = load_handle
            .await
            .map_err(|e| Status::internal(format!("load task panicked: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(pipeliner_proto::LoadResult {
            rows_written: result.rows_written,
            rows_errored: result.rows_errored,
            error_message: result.error_message,
        }))
    }
}
