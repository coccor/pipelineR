//! REST API source connector for pipelineR.
//!
//! Fetches JSON data from HTTP endpoints with support for authentication,
//! pagination, rate limiting, and configurable response mapping.

pub mod auth;
pub mod config;
pub mod error;
pub mod pagination;
pub mod response;
pub mod source;

pub use source::RestSource;
