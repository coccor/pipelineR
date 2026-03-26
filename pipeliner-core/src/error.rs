use thiserror::Error;

/// Top-level error type for the pipeliner-core crate.
#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("transform error: {0}")]
    Transform(#[from] crate::dsl::error::TransformError),
}
