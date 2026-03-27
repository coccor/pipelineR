//! Parquet sink connector — writes records as Parquet files.

use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

use crate::config::{Compression, ParquetSinkConfig};

/// The Parquet sink connector.
pub struct ParquetSink;

/// Parse and validate the sink configuration from a `SinkConfig`.
fn parse_config(config: &SinkConfig) -> Result<ParquetSinkConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

#[async_trait]
impl Sink for ParquetSink {
    /// Return metadata about this sink connector.
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "parquet".to_string(),
            version: "0.1.0".to_string(),
            description: "Parquet sink connector — writes Arrow-columnar Parquet files".to_string(),
        }
    }

    /// Validate the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if `path` is missing or the parent directory does not exist.
    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg = parse_config(config)?;

        if cfg.path.is_empty() {
            return Err(ValidationError::MissingField("path".to_string()));
        }

        let path = std::path::Path::new(&cfg.path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(ValidationError::InvalidConfig(format!(
                    "parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        Ok(())
    }

    /// Declare schema requirements — the Parquet sink is flexible.
    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // FLEXIBLE
            fixed_schema: vec![],
        }
    }

    /// Load data from the receiver channel and write it as a Parquet file.
    ///
    /// # Errors
    ///
    /// Returns `LoadError` if record conversion, file creation, or writing fails.
    async fn load(
        &self,
        config: &SinkConfig,
        _schema: Option<SchemaResponse>,
        mut rx: mpsc::Receiver<pipeliner_proto::RecordBatch>,
    ) -> Result<LoadResult, LoadError> {
        let cfg = parse_config(config).map_err(|e| LoadError::Failed(e.to_string()))?;

        // Collect all records from the channel.
        let mut all_records = Vec::new();
        while let Some(batch) = rx.recv().await {
            let core_batch = proto_batch_to_core(&batch);
            all_records.extend(core_batch.records);
        }

        let total_rows = all_records.len() as i64;

        if all_records.is_empty() {
            return Ok(LoadResult {
                rows_written: 0,
                rows_errored: 0,
                error_message: String::new(),
            });
        }

        // Convert to Arrow RecordBatch.
        let arrow_batch = pipeliner_arrow_convert::records_to_arrow_batch(&all_records, None)
            .map_err(|e| LoadError::Failed(e.to_string()))?;

        // Build writer properties with configured compression.
        let compression = match cfg.compression {
            Some(Compression::None) => ParquetCompression::UNCOMPRESSED,
            Some(Compression::Snappy) | None => ParquetCompression::SNAPPY,
            Some(Compression::Zstd) => ParquetCompression::ZSTD(Default::default()),
            Some(Compression::Gzip) => ParquetCompression::GZIP(Default::default()),
        };

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        // Write the Parquet file (blocking I/O wrapped in spawn_blocking).
        let schema = Arc::new(ArrowSchema::clone(arrow_batch.schema().as_ref()));
        let path = cfg.path.clone();

        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let file = File::create(&path).map_err(|e| format!("failed to create file: {e}"))?;
            let mut writer = ArrowWriter::try_new(file, schema, Some(props))
                .map_err(|e| format!("failed to create parquet writer: {e}"))?;
            writer
                .write(&arrow_batch)
                .map_err(|e| format!("failed to write batch: {e}"))?;
            writer
                .close()
                .map_err(|e| format!("failed to close writer: {e}"))?;
            Ok(())
        })
        .await
        .map_err(|e| LoadError::Failed(format!("task join error: {e}")))?
        .map_err(LoadError::Failed)?;

        Ok(LoadResult {
            rows_written: total_rows,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}
