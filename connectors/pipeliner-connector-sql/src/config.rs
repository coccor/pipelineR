//! Configuration types for the SQL connector (source and sink).

use serde::Deserialize;

/// Supported SQL database drivers.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SqlDriver {
    /// Microsoft SQL Server (via tiberius).
    Sqlserver,
    /// PostgreSQL (via tokio-postgres).
    Postgres,
    /// MySQL (via mysql_async).
    Mysql,
}

/// Write mode for the SQL sink.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Plain INSERT statements.
    Insert,
    /// Upsert (MERGE / ON CONFLICT / ON DUPLICATE KEY).
    Upsert,
    /// TRUNCATE the table, then batch INSERT.
    TruncateAndLoad,
}

/// Partition strategy for the SQL source.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// Single partition — no splitting.
    Single,
    /// Date-range partitioning.
    DateRange,
    /// Key-range partitioning based on a list of explicit values.
    KeyRange,
}

/// Date-range partition configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DateRangeConfig {
    /// Column name containing the date to partition on.
    pub column: String,
    /// Start date (inclusive), ISO 8601 format.
    pub start: String,
    /// End date (exclusive), ISO 8601 format.
    pub end: String,
    /// Interval string, e.g. `"1d"`, `"7d"`, `"1m"`.
    pub interval: String,
}

/// Key-range partition configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct KeyRangeConfig {
    /// Column name to partition on.
    pub column: String,
    /// Explicit list of key values; one partition per value.
    pub values: Vec<String>,
}

/// Partition configuration for the SQL source.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionConfig {
    /// The partitioning strategy.
    pub strategy: PartitionStrategy,
    /// Date-range partition config (required when strategy is `date_range`).
    #[serde(default)]
    pub date_range: Option<DateRangeConfig>,
    /// Key-range partition config (required when strategy is `key_range`).
    #[serde(default)]
    pub key_range: Option<KeyRangeConfig>,
}

/// Top-level configuration for the SQL source connector.
#[derive(Debug, Clone, Deserialize)]
pub struct SqlSourceConfig {
    /// Database driver to use.
    pub driver: SqlDriver,
    /// Connection string for the database.
    pub connection_string: String,
    /// SQL query to execute.
    pub query: String,
    /// Optional partition configuration.
    #[serde(default)]
    pub partition: Option<PartitionConfig>,
    /// Optional watermark column name (for computing max value).
    #[serde(default)]
    pub watermark_column: Option<String>,
}

fn default_batch_size() -> usize {
    1000
}

/// Top-level configuration for the SQL sink connector.
#[derive(Debug, Clone, Deserialize)]
pub struct SqlSinkConfig {
    /// Database driver to use.
    pub driver: SqlDriver,
    /// Connection string for the database.
    pub connection_string: String,
    /// Target table name.
    pub table: String,
    /// Write mode.
    #[serde(default = "default_write_mode")]
    pub write_mode: WriteMode,
    /// Merge keys for upsert mode.
    #[serde(default)]
    pub merge_keys: Vec<String>,
    /// Number of rows per INSERT batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_write_mode() -> WriteMode {
    WriteMode::Insert
}
