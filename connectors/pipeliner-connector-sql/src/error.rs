//! Error types for the SQL connector (source and sink).

use thiserror::Error;

/// Errors produced by the SQL connector.
#[derive(Debug, Error)]
pub enum SqlError {
    /// A connection error occurred.
    #[error("connection error: {0}")]
    Connection(String),
    /// A query execution error occurred.
    #[error("query error: {0}")]
    Query(String),
    /// A configuration error occurred.
    #[error("config error: {0}")]
    Config(String),
    /// A schema discovery error occurred.
    #[error("schema error: {0}")]
    Schema(String),
    /// A type conversion error occurred.
    #[error("type conversion error: {0}")]
    TypeConversion(String),
}
