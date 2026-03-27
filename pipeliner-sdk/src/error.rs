//! Error types for connector SDK operations.

use thiserror::Error;

/// Errors that can occur during plugin configuration validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// The configuration is invalid.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// A required field is missing from the configuration.
    #[error("missing required field: {0}")]
    MissingField(String),
}

/// Errors that can occur during schema or partition discovery.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// Discovery failed for an unspecified reason.
    #[error("discovery failed: {0}")]
    Failed(String),
    /// A connection error occurred during discovery.
    #[error("connection error: {0}")]
    Connection(String),
}

/// Errors that can occur during data extraction.
#[derive(Debug, Error)]
pub enum ExtractionError {
    /// Extraction failed for an unspecified reason.
    #[error("extraction failed: {0}")]
    Failed(String),
    /// The output channel was closed before extraction completed.
    #[error("channel closed")]
    ChannelClosed,
    /// A connection error occurred during extraction.
    #[error("connection error: {0}")]
    Connection(String),
}

/// Errors that can occur during data loading into a sink.
#[derive(Debug, Error)]
pub enum LoadError {
    /// Load failed for an unspecified reason.
    #[error("load failed: {0}")]
    Failed(String),
    /// A connection error occurred during loading.
    #[error("connection error: {0}")]
    Connection(String),
}
