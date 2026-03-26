//! Plugin SDK for pipelineR — provides traits, error types, and helpers for source/sink plugins.

pub mod config;
pub mod error;

pub use config::parse_config;
pub use error::*;
