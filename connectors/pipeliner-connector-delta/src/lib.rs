//! Delta Lake sink connector for pipelineR.
//!
//! Writes row-based `Record` data to Delta Lake tables using Apache Arrow
//! as the intermediate representation. Supports append, overwrite, and
//! merge (upsert) write modes with optional Hive-style partitioning.

pub mod config;
pub mod error;
pub mod sink;

pub use sink::DeltaSink;
