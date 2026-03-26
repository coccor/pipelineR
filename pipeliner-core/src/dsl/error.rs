use thiserror::Error;

/// Error type for DSL transform operations.
#[derive(Debug, Error)]
pub enum TransformError {
    #[error("parse error: {0}")]
    Parse(String),
}
