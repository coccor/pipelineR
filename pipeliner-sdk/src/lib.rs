//! Plugin SDK for pipelineR — provides traits, error types, and helpers for source/sink plugins.

pub mod config;
pub mod error;
pub mod grpc_sink;
pub mod grpc_source;
pub mod sink;
pub mod source;

pub use config::parse_config;
pub use error::*;
pub use grpc_sink::GrpcSinkService;
pub use grpc_source::GrpcSourceService;
pub use sink::{LoadResult, Sink};
pub use source::Source;
