//! gRPC service bridge for the [`Source`] trait.
//!
//! Wraps any `Source` implementation so it can be served as a tonic `SourceConnector` gRPC service.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use pipeliner_proto::pipeliner::v1::source_connector_server::SourceConnector;
use pipeliner_proto::{
    DiscoverPartitionsRequest, DiscoverSchemaRequest, Empty, ExtractRequest, ExtractResponse,
    PartitionsResponse, SchemaResponse, SourceConfig, SourceDescriptor, ValidationResult,
};

use crate::source::Source;

/// A tonic gRPC service that delegates to a [`Source`] trait implementation.
pub struct GrpcSourceService<S: Source> {
    source: Arc<S>,
}

impl<S: Source> GrpcSourceService<S> {
    /// Create a new gRPC source service wrapping the given source.
    pub fn new(source: S) -> Self {
        Self {
            source: Arc::new(source),
        }
    }
}

#[tonic::async_trait]
impl<S: Source> SourceConnector for GrpcSourceService<S> {
    type ExtractStream = ReceiverStream<Result<ExtractResponse, Status>>;

    /// Return metadata about this source connector.
    async fn describe(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SourceDescriptor>, Status> {
        Ok(Response::new(self.source.describe()))
    }

    /// Validate the provided source configuration.
    async fn validate(
        &self,
        request: Request<SourceConfig>,
    ) -> Result<Response<ValidationResult>, Status> {
        let config = request.into_inner();
        match self.source.validate(&config).await {
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

    /// Discover the schema of the data source.
    async fn discover_schema(
        &self,
        request: Request<DiscoverSchemaRequest>,
    ) -> Result<Response<SchemaResponse>, Status> {
        let inner = request.into_inner();
        let config = inner.config.unwrap_or_default();
        let params = inner.params.unwrap_or_default();
        self.source
            .discover_schema(&config, &params)
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }

    /// Discover partitions for parallel extraction.
    async fn discover_partitions(
        &self,
        request: Request<DiscoverPartitionsRequest>,
    ) -> Result<Response<PartitionsResponse>, Status> {
        let inner = request.into_inner();
        let config = inner.config.unwrap_or_default();
        let params = inner.params.unwrap_or_default();
        self.source
            .discover_partitions(&config, &params)
            .await
            .map(|partitions| Response::new(PartitionsResponse { partitions }))
            .map_err(|e| Status::internal(e.to_string()))
    }

    /// Extract data from the source, streaming `ExtractResponse` messages.
    async fn extract(
        &self,
        request: Request<ExtractRequest>,
    ) -> Result<Response<Self::ExtractStream>, Status> {
        let inner = request.into_inner();
        let config = inner.config.unwrap_or_default();
        let params = inner.params.unwrap_or_default();

        let (batch_tx, mut batch_rx) = mpsc::channel::<pipeliner_proto::RecordBatch>(32);
        let (stream_tx, stream_rx) =
            mpsc::channel::<Result<ExtractResponse, Status>>(32);

        let source = Arc::clone(&self.source);

        // Spawn extraction — sends batches to batch_tx.
        let extract_handle = tokio::spawn(async move {
            source.extract(&config, &params, batch_tx).await
        });

        // Spawn forwarding loop: batch_rx → stream_tx, then send watermark.
        tokio::spawn(async move {
            while let Some(batch) = batch_rx.recv().await {
                let resp = ExtractResponse {
                    batch: Some(batch),
                    watermark: String::new(),
                };
                if stream_tx.send(Ok(resp)).await.is_err() {
                    return;
                }
            }

            // All batches forwarded; await the extraction result for watermark.
            match extract_handle.await {
                Ok(Ok(watermark)) => {
                    let _ = stream_tx
                        .send(Ok(ExtractResponse {
                            batch: None,
                            watermark,
                        }))
                        .await;
                }
                Ok(Err(e)) => {
                    let _ = stream_tx
                        .send(Err(Status::internal(e.to_string())))
                        .await;
                }
                Err(join_err) => {
                    let _ = stream_tx
                        .send(Err(Status::internal(format!(
                            "extract task panicked: {join_err}"
                        ))))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(stream_rx)))
    }
}
