//! Error types for the Delta Lake sink connector.

use thiserror::Error;

/// Errors that can occur during Delta Lake sink operations.
#[derive(Debug, Error)]
pub enum DeltaSinkError {
    /// An error from the Arrow conversion layer.
    #[error("arrow conversion error: {0}")]
    ArrowConvert(#[from] pipeliner_arrow_convert::ArrowConvertError),
    /// An error from the Delta Lake library.
    #[error("delta error: {0}")]
    Delta(#[from] deltalake::DeltaTableError),
    /// An error from the Arrow library.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A general error with a descriptive message.
    #[error("{0}")]
    General(String),
}
