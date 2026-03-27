//! Configuration types for the Parquet sink connector.

use serde::Deserialize;

/// Configuration for the Parquet sink connector.
#[derive(Debug, Clone, Deserialize)]
pub struct ParquetSinkConfig {
    /// Output file path (local filesystem).
    pub path: String,
    /// Compression codec to use. Defaults to Snappy.
    pub compression: Option<Compression>,
    /// How to handle nested types (Array, Map). Defaults to `JsonString`.
    pub nested_handling: Option<NestedHandling>,
    /// Columns to partition by (Hive-style: `column=value/` directory structure).
    /// If empty, all data is written to a single file.
    #[serde(default)]
    pub partition_columns: Vec<String>,
}

/// Compression codec for Parquet output.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// No compression.
    None,
    /// Snappy compression (default).
    Snappy,
    /// Zstd compression.
    Zstd,
    /// Gzip compression.
    Gzip,
}

/// Strategy for handling nested Value types (Array, Map) in columnar output.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NestedHandling {
    /// Flatten nested structures into top-level columns.
    Flatten,
    /// Serialise nested values as JSON strings (default).
    JsonString,
}
