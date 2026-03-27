//! Configuration types for the file connector (source and sink).

use serde::Deserialize;

/// Storage backend selector.
///
/// Determines whether files are read from / written to the local filesystem or a
/// cloud object store. When omitted from TOML config, defaults to `Local`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageBackendConfig {
    /// Local filesystem (default).
    #[default]
    Local,
    /// Amazon S3 (or S3-compatible stores such as MinIO).
    S3 {
        /// AWS region (e.g. `us-east-1`). Falls back to SDK default chain when `None`.
        region: Option<String>,
        /// Custom endpoint URL for S3-compatible stores (e.g. MinIO).
        endpoint: Option<String>,
    },
    /// Azure Blob Storage.
    Azure {
        /// Storage account connection string. Falls back to `AZURE_STORAGE_CONNECTION_STRING`.
        connection_string: Option<String>,
    },
    /// Google Cloud Storage.
    Gcs {
        /// GCP project ID. Falls back to `GOOGLE_CLOUD_PROJECT` env var.
        project_id: Option<String>,
    },
}


/// File format.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}

/// Sink-supported file formats (CSV and JSON only — no Parquet sink in file connector).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SinkFileFormat {
    Csv,
    Json,
}

/// CSV-specific options.
#[derive(Debug, Clone, Deserialize)]
pub struct CsvOptions {
    /// Field delimiter character. Defaults to `,`.
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Quote character. Defaults to `"`.
    #[serde(default = "default_quote")]
    pub quote: char,
    /// Whether the file has a header row. Defaults to `true`.
    #[serde(default = "default_true")]
    pub has_header: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            quote: '"',
            has_header: true,
        }
    }
}

fn default_delimiter() -> char {
    ','
}

fn default_quote() -> char {
    '"'
}

fn default_true() -> bool {
    true
}

/// Top-level configuration for the file source connector.
#[derive(Debug, Clone, Deserialize)]
pub struct FileSourceConfig {
    /// Path to a file or a glob pattern (e.g., `data/*.csv`).
    ///
    /// Interpretation depends on the storage backend:
    /// - `Local`: filesystem path or glob
    /// - `S3`: `s3://bucket/prefix/key.csv`
    /// - `Azure`: `azure://container/prefix/key.csv`
    /// - `GCS`: `gs://bucket/prefix/key.csv`
    pub path: String,
    /// File format.
    pub format: FileFormat,
    /// CSV-specific options (only used when `format` is `csv`).
    #[serde(default)]
    pub csv: Option<CsvOptions>,
    /// Storage backend. Defaults to `Local` when omitted.
    #[serde(default)]
    pub storage: Option<StorageBackendConfig>,
}

/// Top-level configuration for the file sink connector.
#[derive(Debug, Clone, Deserialize)]
pub struct FileSinkConfig {
    /// Output file path.
    ///
    /// Interpretation depends on the storage backend:
    /// - `Local`: filesystem path
    /// - `S3`: `s3://bucket/prefix/key.csv`
    /// - `Azure`: `azure://container/prefix/key.csv`
    /// - `GCS`: `gs://bucket/prefix/key.csv`
    pub path: String,
    /// Output file format.
    pub format: SinkFileFormat,
    /// CSV-specific options (only used when `format` is `csv`).
    #[serde(default)]
    pub csv: Option<CsvOptions>,
    /// Storage backend. Defaults to `Local` when omitted.
    #[serde(default)]
    pub storage: Option<StorageBackendConfig>,
}
