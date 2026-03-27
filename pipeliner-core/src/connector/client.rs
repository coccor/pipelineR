use pipeliner_proto::pipeliner::v1::source_connector_client::SourceConnectorClient;
use pipeliner_proto::pipeliner::v1::sink_connector_client::SinkConnectorClient;
use pipeliner_proto::pipeliner::v1::{
    DiscoverPartitionsRequest, DiscoverSchemaRequest, Empty, ExtractRequest, ExtractResponse,
    LoadRequest, LoadResult, PartitionsResponse, SchemaRequirementResponse, SchemaResponse,
    SinkConfig, SinkDescriptor, SourceConfig, SourceDescriptor, ValidationResult,
};
use pipeliner_proto::RuntimeParams;
use tonic::transport::Channel;
use tonic::Streaming;

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Client wrapper for communicating with a source connector over gRPC.
///
/// Named `SourceConnectorClientWrapper` to avoid collision with the tonic-generated
/// `SourceConnectorClient<Channel>`.
pub struct SourceConnectorClientWrapper {
    client: SourceConnectorClient<Channel>,
}

impl SourceConnectorClientWrapper {
    /// Connect to a source connector at the given address (e.g. `http://127.0.0.1:50051`).
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = SourceConnectorClient::connect(addr).await.map(|c| {
            c.max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE)
        })?;
        Ok(Self { client })
    }

    /// Retrieve the plugin descriptor (name, version, description).
    pub async fn describe(&mut self) -> Result<SourceDescriptor, tonic::Status> {
        self.client
            .describe(Empty {})
            .await
            .map(|r| r.into_inner())
    }

    /// Validate a source configuration.
    pub async fn validate(&mut self, config: SourceConfig) -> Result<ValidationResult, tonic::Status> {
        self.client.validate(config).await.map(|r| r.into_inner())
    }

    /// Discover the schema for the given source configuration and runtime parameters.
    pub async fn discover_schema(
        &mut self,
        config: SourceConfig,
        params: RuntimeParams,
    ) -> Result<SchemaResponse, tonic::Status> {
        self.client
            .discover_schema(DiscoverSchemaRequest {
                config: Some(config),
                params: Some(params),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// Discover available partitions for the given source configuration and runtime parameters.
    pub async fn discover_partitions(
        &mut self,
        config: SourceConfig,
        params: RuntimeParams,
    ) -> Result<PartitionsResponse, tonic::Status> {
        self.client
            .discover_partitions(DiscoverPartitionsRequest {
                config: Some(config),
                params: Some(params),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// Start extraction, returning a stream of record batches.
    pub async fn extract(
        &mut self,
        config: SourceConfig,
        params: RuntimeParams,
    ) -> Result<Streaming<ExtractResponse>, tonic::Status> {
        self.client
            .extract(ExtractRequest {
                config: Some(config),
                params: Some(params),
            })
            .await
            .map(|r| r.into_inner())
    }
}

/// Client wrapper for communicating with a sink connector over gRPC.
///
/// Named `SinkConnectorClientWrapper` to avoid collision with the tonic-generated
/// `SinkConnectorClient<Channel>`.
pub struct SinkConnectorClientWrapper {
    client: SinkConnectorClient<Channel>,
}

impl SinkConnectorClientWrapper {
    /// Connect to a sink connector at the given address (e.g. `http://127.0.0.1:50052`).
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = SinkConnectorClient::connect(addr).await.map(|c| {
            c.max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE)
        })?;
        Ok(Self { client })
    }

    /// Retrieve the sink connector descriptor (name, version, description).
    pub async fn describe(&mut self) -> Result<SinkDescriptor, tonic::Status> {
        self.client
            .describe(Empty {})
            .await
            .map(|r| r.into_inner())
    }

    /// Validate a sink configuration.
    pub async fn validate(&mut self, config: SinkConfig) -> Result<ValidationResult, tonic::Status> {
        self.client.validate(config).await.map(|r| r.into_inner())
    }

    /// Query the sink's schema requirement (flexible, match source, or fixed).
    pub async fn schema_requirement(&mut self) -> Result<SchemaRequirementResponse, tonic::Status> {
        self.client
            .schema_requirement(Empty {})
            .await
            .map(|r| r.into_inner())
    }

    /// Send a stream of load requests (metadata + record batches) and receive the load result.
    pub async fn load(
        &mut self,
        requests: impl tokio_stream::Stream<Item = LoadRequest> + Send + 'static,
    ) -> Result<LoadResult, tonic::Status> {
        self.client.load(requests).await.map(|r| r.into_inner())
    }
}
