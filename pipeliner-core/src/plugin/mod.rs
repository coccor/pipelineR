/// gRPC client wrappers for communicating with plugin processes.
pub mod client;
/// Plugin process lifecycle management (spawn, connect, kill).
pub mod manager;

pub use client::{SinkPluginClientWrapper, SourcePluginClientWrapper};
pub use manager::{PluginProcess, PluginSpawnError};
