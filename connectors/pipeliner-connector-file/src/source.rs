//! File source connector — implements the `Source` trait for CSV, JSON, and Parquet files.
//!
//! Supports local filesystem and cloud storage backends (S3, Azure Blob, GCS).

use std::io::Write;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pipeliner_proto::{
    ColumnSchema, Partition, RuntimeParams, SchemaResponse, SourceConfig, SourceDescriptor,
};
use pipeliner_sdk::error::{DiscoveryError, ExtractionError, ValidationError};
use pipeliner_sdk::Source;
use tokio::sync::mpsc;

use crate::config::{FileFormat, FileSourceConfig, StorageBackendConfig};
use crate::csv_reader;
use crate::error::FileSourceError;
use crate::json_reader;
use crate::parquet_reader;
use crate::storage::{self, ObjectInfo};

/// The file source connector.
pub struct FileSource;

impl FileSource {
    /// Resolve the config path (or glob) to a list of matching file paths.
    ///
    /// Used only for the local backend for backward compatibility.
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

    /// Compute the watermark from cloud object info.
    fn compute_cloud_watermark(objects: &[ObjectInfo]) -> String {
        objects
            .iter()
            .filter_map(|o| o.last_modified)
            .max()
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default()
    }

    /// Returns `true` if the config specifies a cloud storage backend.
    fn is_cloud(config: &FileSourceConfig) -> bool {
        matches!(
            config.storage.as_ref(),
            Some(
                StorageBackendConfig::S3 { .. }
                    | StorageBackendConfig::Azure { .. }
                    | StorageBackendConfig::Gcs { .. }
            )
        )
    }

    /// Download cloud object bytes to a temporary file and return the temp file handle + path.
    ///
    /// The caller must keep the returned `NamedTempFile` alive for the duration
    /// the path is used, because dropping it deletes the file.
    fn write_bytes_to_temp(
        data: &[u8],
        suffix: &str,
    ) -> Result<tempfile::NamedTempFile, FileSourceError> {
        let mut tmp = tempfile::Builder::new()
            .suffix(suffix)
            .tempfile()
            .map_err(|e| FileSourceError::Io(format!("create temp file: {e}")))?;
        tmp.write_all(data)
            .map_err(|e| FileSourceError::Io(format!("write temp file: {e}")))?;
        tmp.flush()
            .map_err(|e| FileSourceError::Io(format!("flush temp file: {e}")))?;
        Ok(tmp)
    }

    /// File extension suffix for the configured format (used for temp files).
    fn format_suffix(config: &FileSourceConfig) -> &'static str {
        match config.format {
            FileFormat::Csv => ".csv",
            FileFormat::Json => ".jsonl",
            FileFormat::Parquet => ".parquet",
        }
    }

    /// Read records from bytes by writing to a temp file and using existing readers.
    fn read_bytes_as_batches(
        data: &[u8],
        config: &FileSourceConfig,
    ) -> Result<Vec<pipeliner_core::record::RecordBatch>, FileSourceError> {
        let tmp = Self::write_bytes_to_temp(data, Self::format_suffix(config))?;
        let path = tmp.path();
        match config.format {
            FileFormat::Csv => {
                let opts = config.csv.clone().unwrap_or_default();
                csv_reader::read_csv(path, &opts, pipeliner_core::record::DEFAULT_BATCH_SIZE)
            }
            FileFormat::Json => {
                json_reader::read_ndjson(path, pipeliner_core::record::DEFAULT_BATCH_SIZE)
            }
            FileFormat::Parquet => {
                parquet_reader::read_parquet(path, pipeliner_core::record::DEFAULT_BATCH_SIZE)
            }
        }
    }

    /// Infer schema from bytes by writing to a temp file and using existing readers.
    fn infer_schema_from_bytes(
        data: &[u8],
        config: &FileSourceConfig,
    ) -> Result<Vec<(String, String)>, FileSourceError> {
        let tmp = Self::write_bytes_to_temp(data, Self::format_suffix(config))?;
        let path = tmp.path();
        match config.format {
            FileFormat::Csv => {
                let opts = config.csv.clone().unwrap_or_default();
                csv_reader::infer_csv_schema(path, &opts)
            }
            FileFormat::Json => json_reader::infer_json_schema(path, 100),
            FileFormat::Parquet => parquet_reader::infer_parquet_schema(path),
        }
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
            description: "File source connector — reads CSV, JSON, and Parquet files from local filesystem or cloud storage".to_string(),
        }
    }

    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError> {
        let cfg = parse_file_config(config)?;

        if cfg.path.is_empty() {
            return Err(ValidationError::MissingField("path".to_string()));
        }

        if Self::is_cloud(&cfg) {
            // For cloud backends, validate the path can be parsed.
            storage::parse_cloud_path(&cfg.path)
                .map_err(|e| ValidationError::InvalidConfig(e.to_string()))?;
        } else {
            // Verify at least one file matches.
            Self::resolve_files(&cfg.path)
                .map_err(|e| ValidationError::InvalidConfig(e.to_string()))?;
        }

        Ok(())
    }

    async fn discover_schema(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError> {
        let cfg =
            parse_file_config(config).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let schema_pairs = if Self::is_cloud(&cfg) {
            // Cloud path: download first object and infer schema.
            let store = storage::create_storage(cfg.storage.as_ref())
                .await
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

            let objects = store
                .list_objects(&cfg.path)
                .await
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

            let first_key = objects
                .first()
                .ok_or_else(|| DiscoveryError::Failed("no objects found".to_string()))?
                .key
                .clone();

            let data = store
                .read_object(&first_key)
                .await
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

            Self::infer_schema_from_bytes(&data, &cfg)
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?
        } else {
            let files = Self::resolve_files(&cfg.path)
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

            let first = files
                .first()
                .ok_or_else(|| DiscoveryError::Failed("no files found".to_string()))?;

            match cfg.format {
                FileFormat::Csv => {
                    let opts = cfg.csv.unwrap_or_default();
                    csv_reader::infer_csv_schema(first, &opts)
                        .map_err(|e| DiscoveryError::Failed(e.to_string()))?
                }
                FileFormat::Json => json_reader::infer_json_schema(first, 100)
                    .map_err(|e| DiscoveryError::Failed(e.to_string()))?,
                FileFormat::Parquet => parquet_reader::infer_parquet_schema(first)
                    .map_err(|e| DiscoveryError::Failed(e.to_string()))?,
            }
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

        if Self::is_cloud(&cfg) {
            let store = storage::create_storage(cfg.storage.as_ref())
                .await
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

            let objects = store
                .list_objects(&cfg.path)
                .await
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

            let partitions = objects
                .into_iter()
                .map(|obj| {
                    let mut params = std::collections::HashMap::new();
                    params.insert("file".to_string(), obj.key.clone());
                    Partition {
                        key: obj.key,
                        params,
                    }
                })
                .collect();

            Ok(partitions)
        } else {
            let files = Self::resolve_files(&cfg.path)
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

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
    }

    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<pipeliner_proto::RecordBatch>,
    ) -> Result<String, ExtractionError> {
        let cfg =
            parse_file_config(config).map_err(|e| ExtractionError::Failed(e.to_string()))?;

        if Self::is_cloud(&cfg) {
            let store = storage::create_storage(cfg.storage.as_ref())
                .await
                .map_err(|e| ExtractionError::Failed(e.to_string()))?;

            // Determine which objects to process.
            let objects = if let Some(file_path) = params.params.get("file") {
                vec![ObjectInfo {
                    key: file_path.clone(),
                    last_modified: None,
                    size: 0,
                }]
            } else {
                store
                    .list_objects(&cfg.path)
                    .await
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?
            };

            let watermark = Self::compute_cloud_watermark(&objects);

            for obj in &objects {
                let data = store
                    .read_object(&obj.key)
                    .await
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?;

                let batches = Self::read_bytes_as_batches(&data, &cfg)
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?;

                let convert = pipeliner_sdk::convert::core_batch_to_proto;
                for batch in &batches {
                    tx.send(convert(batch))
                        .await
                        .map_err(|_| ExtractionError::ChannelClosed)?;
                }
            }

            Ok(watermark)
        } else {
            // Local backend — original logic.
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
                        csv_reader::read_csv(
                            file_path,
                            &opts,
                            pipeliner_core::record::DEFAULT_BATCH_SIZE,
                        )
                        .map_err(|e| ExtractionError::Failed(e.to_string()))?
                    }
                    FileFormat::Json => json_reader::read_ndjson(
                        file_path,
                        pipeliner_core::record::DEFAULT_BATCH_SIZE,
                    )
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?,
                    FileFormat::Parquet => parquet_reader::read_parquet(
                        file_path,
                        pipeliner_core::record::DEFAULT_BATCH_SIZE,
                    )
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?,
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
}
