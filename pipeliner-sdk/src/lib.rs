//! Connector SDK for pipelineR — provides traits, error types, and helpers for source/sink connectors.

pub mod config;
pub mod convert;
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
pub use server::{run_connector, run_sink_connector, run_source_connector};
pub use sink::{LoadResult, Sink};
pub use source::Source;
