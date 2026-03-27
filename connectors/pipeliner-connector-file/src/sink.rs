//! File sink connector — implements the `Sink` trait for CSV and JSON output.

use async_trait::async_trait;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

use crate::config::{FileSinkConfig, SinkFileFormat};
use crate::csv_writer::CsvFileWriter;
use crate::json_writer::JsonFileWriter;

/// The file sink connector.
pub struct FileSink;

fn parse_sink_config(config: &SinkConfig) -> Result<FileSinkConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

#[async_trait]
impl Sink for FileSink {
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "file".to_string(),
            version: "0.1.0".to_string(),
            description: "File sink connector — writes CSV and JSON files to local filesystem"
                .to_string(),
        }
    }

    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg = parse_sink_config(config)?;

        if cfg.path.is_empty() {
            return Err(ValidationError::MissingField("path".to_string()));
        }

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
        let path = std::path::Path::new(&cfg.path);

        let mut rows_written: i64 = 0;

        match cfg.format {
            SinkFileFormat::Csv => {
                let opts = cfg.csv.unwrap_or_default();
                let mut writer =
                    CsvFileWriter::new(path, &opts).map_err(|e| LoadError::Failed(e.to_string()))?;

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
                let mut writer =
                    JsonFileWriter::new(path).map_err(|e| LoadError::Failed(e.to_string()))?;

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
