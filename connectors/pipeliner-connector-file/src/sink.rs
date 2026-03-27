//! File sink connector — implements the `Sink` trait for CSV and JSON output.
//!
//! Supports local filesystem and cloud storage backends (S3, Azure Blob, GCS).

use async_trait::async_trait;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

use crate::config::{FileSinkConfig, SinkFileFormat, StorageBackendConfig};
use crate::csv_writer::CsvFileWriter;
use crate::json_writer::JsonFileWriter;
use crate::storage;

/// The file sink connector.
pub struct FileSink;

fn parse_sink_config(config: &SinkConfig) -> Result<FileSinkConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

/// Returns `true` if the config specifies a cloud storage backend.
fn is_cloud(config: &FileSinkConfig) -> bool {
    matches!(
        config.storage.as_ref(),
        Some(
            StorageBackendConfig::S3 { .. }
                | StorageBackendConfig::Azure { .. }
                | StorageBackendConfig::Gcs { .. }
        )
    )
}

#[async_trait]
impl Sink for FileSink {
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "file".to_string(),
            version: "0.1.0".to_string(),
            description: "File sink connector — writes CSV and JSON files to local filesystem or cloud storage"
                .to_string(),
        }
    }

    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg = parse_sink_config(config)?;

        if cfg.path.is_empty() {
            return Err(ValidationError::MissingField("path".to_string()));
        }

        if is_cloud(&cfg) {
            // For cloud backends, validate the path can be parsed.
            storage::parse_cloud_path(&cfg.path)
                .map_err(|e| ValidationError::InvalidConfig(e.to_string()))?;
        } else {
            // Check that the parent directory exists.
            let path = std::path::Path::new(&cfg.path);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    return Err(ValidationError::InvalidConfig(format!(
                        "parent directory does not exist: {}",
                        parent.display()
                    )));
                }
            }
        }

        Ok(())
    }

    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // FLEXIBLE — accepts any schema.
            fixed_schema: vec![],
        }
    }

    async fn load(
        &self,
        config: &SinkConfig,
        _schema: Option<SchemaResponse>,
        mut rx: mpsc::Receiver<pipeliner_proto::RecordBatch>,
    ) -> Result<LoadResult, LoadError> {
        let cfg = parse_sink_config(config).map_err(|e| LoadError::Failed(e.to_string()))?;

        if is_cloud(&cfg) {
            // Cloud path: write to a temp file first, then upload.
            let tmp = tempfile::Builder::new()
                .suffix(format_suffix(&cfg.format))
                .tempfile()
                .map_err(|e| LoadError::Failed(format!("create temp file: {e}")))?;
            let tmp_path = tmp.path().to_path_buf();

            let mut rows_written: i64 = 0;

            match cfg.format {
                SinkFileFormat::Csv => {
                    let opts = cfg.csv.clone().unwrap_or_default();
                    let mut writer = CsvFileWriter::new(&tmp_path, &opts)
                        .map_err(|e| LoadError::Failed(e.to_string()))?;

                    while let Some(batch) = rx.recv().await {
                        let core_batch = proto_batch_to_core(&batch);
                        let count = core_batch.records.len() as i64;
                        writer
                            .write_records(&core_batch.records)
                            .map_err(|e| LoadError::Failed(e.to_string()))?;
                        rows_written += count;
                    }

                    writer
                        .finish()
                        .map_err(|e| LoadError::Failed(e.to_string()))?;
                }
                SinkFileFormat::Json => {
                    let mut writer = JsonFileWriter::new(&tmp_path)
                        .map_err(|e| LoadError::Failed(e.to_string()))?;

                    while let Some(batch) = rx.recv().await {
                        let core_batch = proto_batch_to_core(&batch);
                        let count = core_batch.records.len() as i64;
                        writer
                            .write_records(&core_batch.records)
                            .map_err(|e| LoadError::Failed(e.to_string()))?;
                        rows_written += count;
                    }

                    writer
                        .finish()
                        .map_err(|e| LoadError::Failed(e.to_string()))?;
                }
            }

            // Read the temp file and upload to cloud storage.
            let data = tokio::fs::read(&tmp_path)
                .await
                .map_err(|e| LoadError::Failed(format!("read temp file: {e}")))?;

            let store = storage::create_storage(cfg.storage.as_ref())
                .await
                .map_err(|e| LoadError::Failed(e.to_string()))?;

            store
                .write_object(&cfg.path, &data)
                .await
                .map_err(|e| LoadError::Failed(e.to_string()))?;

            Ok(LoadResult {
                rows_written,
                rows_errored: 0,
                error_message: String::new(),
            })
        } else {
            // Local backend — original logic.
            let path = std::path::Path::new(&cfg.path);
            let mut rows_written: i64 = 0;

            match cfg.format {
                SinkFileFormat::Csv => {
                    let opts = cfg.csv.unwrap_or_default();
                    let mut writer = CsvFileWriter::new(path, &opts)
                        .map_err(|e| LoadError::Failed(e.to_string()))?;

                    while let Some(batch) = rx.recv().await {
                        let core_batch = proto_batch_to_core(&batch);
                        let count = core_batch.records.len() as i64;
                        writer
                            .write_records(&core_batch.records)
                            .map_err(|e| LoadError::Failed(e.to_string()))?;
                        rows_written += count;
                    }

                    writer
                        .finish()
                        .map_err(|e| LoadError::Failed(e.to_string()))?;
                }
                SinkFileFormat::Json => {
                    let mut writer = JsonFileWriter::new(path)
                        .map_err(|e| LoadError::Failed(e.to_string()))?;

                    while let Some(batch) = rx.recv().await {
                        let core_batch = proto_batch_to_core(&batch);
                        let count = core_batch.records.len() as i64;
                        writer
                            .write_records(&core_batch.records)
                            .map_err(|e| LoadError::Failed(e.to_string()))?;
                        rows_written += count;
                    }

                    writer
                        .finish()
                        .map_err(|e| LoadError::Failed(e.to_string()))?;
                }
            }

            Ok(LoadResult {
                rows_written,
                rows_errored: 0,
                error_message: String::new(),
            })
        }
    }
}

/// File extension suffix for the configured sink format.
fn format_suffix(format: &SinkFileFormat) -> &'static str {
    match format {
        SinkFileFormat::Csv => ".csv",
        SinkFileFormat::Json => ".jsonl",
    }
}
