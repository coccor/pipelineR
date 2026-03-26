//! Plugin SDK for pipelineR — provides traits, error types, and helpers for source/sink plugins.

pub mod config;
pub mod error;
pub mod grpc_sink;
pub mod grpc_source;
pub mod server;
pub mod sink;
pub mod source;

pub use config::parse_config;
pub use error::*;
pub use grpc_sink::GrpcSinkService;
pub use grpc_source::GrpcSourceService;
pub use server::{run_plugin, run_sink_plugin, run_source_plugin};
pub use sink::{LoadResult, Sink};
pub use source::Source;
