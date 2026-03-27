//! Error types for the file connector (source and sink).

use thiserror::Error;

/// Errors produced by the file source connector.
#[derive(Debug, Error)]
pub enum FileSourceError {
    /// An I/O error occurred (file not found, permission denied, etc.).
    #[error("io error: {0}")]
    Io(String),
    /// A parsing error occurred (malformed CSV, invalid JSON, corrupt Parquet).
    #[error("parse error: {0}")]
    Parse(String),
    /// No files matched the provided path or glob pattern.
    #[error("no files matched pattern: {0}")]
    NoFilesMatched(String),
}

/// Errors produced by the file sink connector.
#[derive(Debug, Error)]
pub enum FileSinkError {
    /// An I/O error occurred writing the output file.
    #[error("io error: {0}")]
    Io(String),
    /// A serialization error occurred.
    #[error("serialization error: {0}")]
    Serialization(String),
}
