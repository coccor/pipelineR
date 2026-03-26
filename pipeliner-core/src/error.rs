use thiserror::Error;

/// Top-level error type for the pipeliner-core crate.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// A DSL transform step failed.
    #[error("transform error: {0}")]
    Transform(#[from] crate::dsl::error::TransformError),

    /// A plugin process failed to start.
    #[error("plugin spawn error: {0}")]
    PluginSpawn(#[from] crate::plugin::PluginSpawnError),

    /// A gRPC communication error with a plugin.
    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::Status),

    /// A gRPC transport error (connection failure).
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// The source plugin returned no data or an invalid stream.
    #[error("source error: {0}")]
    Source(String),

    /// A sink plugin failed during load.
    #[error("sink error: {0}")]
    Sink(String),

    /// An internal channel was closed unexpectedly.
    #[error("channel closed: {0}")]
    ChannelClosed(String),

    /// A configuration error.
    #[error("config error: {0}")]
    Config(#[from] crate::config::ConfigError),
}
