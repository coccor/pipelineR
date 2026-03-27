//! Configuration types for the Delta Lake sink connector.

use serde::Deserialize;

/// Configuration for the Delta Lake sink connector.
#[derive(Debug, Clone, Deserialize)]
pub struct DeltaSinkConfig {
    /// Table URI -- local path or cloud storage URI (s3://, az://, gs://).
    pub table_uri: String,
    /// Write mode: append, overwrite, or merge.
    #[serde(default = "default_write_mode")]
    pub write_mode: WriteMode,
    /// Columns to partition by (Hive-style partitioning).
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Merge keys for merge/upsert mode.
    #[serde(default)]
    pub merge_keys: Vec<String>,
    /// Optional predicate for merge (e.g., "target.date = source.date").
    #[serde(default)]
    pub merge_predicate: Option<String>,
}

/// Write mode for the Delta Lake sink.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Append new rows to the table.
    Append,
    /// Overwrite the entire table.
    Overwrite,
    /// Merge (upsert) rows based on merge keys.
    Merge,
}

/// Return the default write mode (Append).
fn default_write_mode() -> WriteMode {
    WriteMode::Append
}
