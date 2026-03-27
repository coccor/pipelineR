//! File source connector — implements the `Source` trait for CSV, JSON, and Parquet files.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pipeliner_proto::{
    ColumnSchema, Partition, RuntimeParams, SchemaResponse, SourceConfig, SourceDescriptor,
};
use pipeliner_sdk::error::{DiscoveryError, ExtractionError, ValidationError};
use pipeliner_sdk::Source;
use tokio::sync::mpsc;

use crate::config::{FileFormat, FileSourceConfig};
use crate::csv_reader;
use crate::error::FileSourceError;
use crate::json_reader;
use crate::parquet_reader;

/// The file source connector.
pub struct FileSource;

impl FileSource {
    /// Resolve the config path (or glob) to a list of matching file paths.
    fn resolve_files(pattern: &str) -> Result<Vec<PathBuf>, FileSourceError> {
        // Try as glob first.
        let paths: Vec<PathBuf> = glob::glob(pattern)
            .map_err(|e| FileSourceError::Io(format!("invalid glob pattern '{pattern}': {e}")))?
            .filter_map(|entry| entry.ok())
            .filter(|p| p.is_file())
            .collect();

        if paths.is_empty() {
            // Maybe it's a literal path.
            let p = PathBuf::from(pattern);
            if p.is_file() {
                return Ok(vec![p]);
            }
            return Err(FileSourceError::NoFilesMatched(pattern.to_string()));
        }

        Ok(paths)
    }

    /// Get the last-modified timestamp of a file.
    fn file_modified(path: &Path) -> Option<DateTime<Utc>> {
        std::fs::metadata(path)
            .ok()
            .and_then(|m| m.modified().ok())
            .map(DateTime::<Utc>::from)
    }

    /// Compute the watermark: the maximum last-modified time across all files.
    fn compute_watermark(paths: &[PathBuf]) -> String {
        paths
            .iter()
            .filter_map(|p| Self::file_modified(p))
            .max()
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default()
    }
}

fn parse_file_config(config: &SourceConfig) -> Result<FileSourceConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

#[async_trait]
impl Source for FileSource {
    fn describe(&self) -> SourceDescriptor {
        SourceDescriptor {
            name: "file".to_string(),
            version: "0.1.0".to_string(),
            description: "File source connector — reads CSV, JSON, and Parquet files from local filesystem".to_string(),
        }
    }

    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError> {
        let cfg = parse_file_config(config)?;

        if cfg.path.is_empty() {
            return Err(ValidationError::MissingField("path".to_string()));
        }

        // Verify at least one file matches.
        Self::resolve_files(&cfg.path)
            .map_err(|e| ValidationError::InvalidConfig(e.to_string()))?;

        Ok(())
    }

    async fn discover_schema(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError> {
        let cfg =
            parse_file_config(config).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let files =
            Self::resolve_files(&cfg.path).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let first = files
            .first()
            .ok_or_else(|| DiscoveryError::Failed("no files found".to_string()))?;

        let schema_pairs = match cfg.format {
            FileFormat::Csv => {
                let opts = cfg.csv.unwrap_or_default();
                csv_reader::infer_csv_schema(first, &opts)
                    .map_err(|e| DiscoveryError::Failed(e.to_string()))?
            }
            FileFormat::Json => json_reader::infer_json_schema(first, 100)
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?,
            FileFormat::Parquet => parquet_reader::infer_parquet_schema(first)
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?,
        };

        let columns = schema_pairs
            .into_iter()
            .map(|(name, data_type)| ColumnSchema {
                name,
                data_type,
                nullable: true,
                description: String::new(),
            })
            .collect();

        Ok(SchemaResponse { columns })
    }

    async fn discover_partitions(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError> {
        let cfg =
            parse_file_config(config).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let files =
            Self::resolve_files(&cfg.path).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let partitions = files
            .into_iter()
            .map(|p| {
                let key = p.display().to_string();
                let mut params = std::collections::HashMap::new();
                params.insert("file".to_string(), key.clone());
                Partition { key, params }
            })
            .collect();

        Ok(partitions)
    }

    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<pipeliner_proto::RecordBatch>,
    ) -> Result<String, ExtractionError> {
        let cfg =
            parse_file_config(config).map_err(|e| ExtractionError::Failed(e.to_string()))?;

        // If a specific file partition param is set, use that; otherwise resolve from config path.
        let files = if let Some(file_path) = params.params.get("file") {
            vec![PathBuf::from(file_path)]
        } else {
            Self::resolve_files(&cfg.path)
                .map_err(|e| ExtractionError::Failed(e.to_string()))?
        };

        let watermark = Self::compute_watermark(&files);

        for file_path in &files {
            let batches = match cfg.format {
                FileFormat::Csv => {
                    let opts = cfg.csv.clone().unwrap_or_default();
                    csv_reader::read_csv(file_path, &opts, pipeliner_core::record::DEFAULT_BATCH_SIZE)
                        .map_err(|e| ExtractionError::Failed(e.to_string()))?
                }
                FileFormat::Json => {
                    json_reader::read_ndjson(file_path, pipeliner_core::record::DEFAULT_BATCH_SIZE)
                        .map_err(|e| ExtractionError::Failed(e.to_string()))?
                }
                FileFormat::Parquet => {
                    parquet_reader::read_parquet(
                        file_path,
                        pipeliner_core::record::DEFAULT_BATCH_SIZE,
                    )
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?
                }
            };

            let convert = pipeliner_sdk::convert::core_batch_to_proto;
            for batch in &batches {
                tx.send(convert(batch))
                    .await
                    .map_err(|_| ExtractionError::ChannelClosed)?;
            }
        }

        Ok(watermark)
    }
}
