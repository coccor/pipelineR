//! Sink plugin trait definition.

use async_trait::async_trait;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use tokio::sync::mpsc;

use crate::error::{LoadError, ValidationError};

/// Result of a sink load operation.
#[derive(Debug, Clone)]
pub struct LoadResult {
    /// Number of rows successfully written.
    pub rows_written: i64,
    /// Number of rows that failed to write.
    pub rows_errored: i64,
    /// Error message, empty if no errors occurred.
    pub error_message: String,
}

/// Trait that sink plugins implement.
///
/// A sink is responsible for receiving `RecordBatch` messages and writing
/// them to an external data system.
#[async_trait]
pub trait Sink: Send + Sync + 'static {
    /// Return metadata about this sink plugin.
    fn describe(&self) -> SinkDescriptor;

    /// Validate the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if the config is invalid or missing required fields.
    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError>;

    /// Declare schema requirements for this sink.
    ///
    /// Returns whether the sink is flexible, must match the source schema,
    /// or has a fixed schema.
    fn schema_requirement(&self) -> SchemaRequirementResponse;

    /// Load data from the receiver channel into the sink.
    ///
    /// # Errors
    ///
    /// Returns `LoadError` if loading fails.
    async fn load(
        &self,
        config: &SinkConfig,
        schema: Option<SchemaResponse>,
        rx: mpsc::Receiver<pipeliner_proto::RecordBatch>,
    ) -> Result<LoadResult, LoadError>;
}
