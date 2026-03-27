//! Error types for the Parquet sink connector.

use thiserror::Error;

/// Errors that can occur during Parquet sink operations.
#[derive(Debug, Error)]
pub enum ParquetSinkError {
    /// An error from the Arrow conversion layer.
    #[error("arrow conversion error: {0}")]
    ArrowConvert(#[from] pipeliner_arrow_convert::ArrowConvertError),
    /// An error from the Parquet writer.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// An error from the Arrow library.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// An I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A general error with a descriptive message.
    #[error("{0}")]
    General(String),
}
