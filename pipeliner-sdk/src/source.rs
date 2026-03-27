//! Source plugin trait definition.

use async_trait::async_trait;
use pipeliner_proto::{Partition, RuntimeParams, SchemaResponse, SourceConfig, SourceDescriptor};
use tokio::sync::mpsc;

use crate::error::{DiscoveryError, ExtractionError, ValidationError};

/// Trait that source connectors implement.
///
/// A source is responsible for connecting to an external data system,
/// discovering its schema and partitions, and extracting data as
/// protobuf `RecordBatch` messages.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Return metadata about this source connector.
    fn describe(&self) -> SourceDescriptor;

    /// Validate the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if the config is invalid or missing required fields.
    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError>;

    /// Discover the schema of the data source.
    ///
    /// An empty `columns` list indicates a schema-less source.
    ///
    /// # Errors
    ///
    /// Returns `DiscoveryError` if schema discovery fails.
    async fn discover_schema(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError>;

    /// Discover partitions for parallel extraction.
    ///
    /// # Errors
    ///
    /// Returns `DiscoveryError` if partition discovery fails.
    async fn discover_partitions(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError>;

    /// Extract data, sending proto `RecordBatch` messages through the channel.
    ///
    /// Returns a watermark string indicating the extraction progress.
    ///
    /// # Errors
    ///
    /// Returns `ExtractionError` if extraction fails or the channel is closed.
    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<pipeliner_proto::RecordBatch>,
    ) -> Result<String, ExtractionError>;
}
