/// gRPC client wrappers for communicating with connector processes.
pub mod client;
/// Connector process lifecycle management (spawn, connect, kill).
pub mod manager;

pub use client::{SinkConnectorClientWrapper, SourceConnectorClientWrapper};
pub use manager::{ConnectorProcess, ConnectorSpawnError};
