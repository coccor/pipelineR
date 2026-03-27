//! Parquet sink connector for pipelineR.
//!
//! Writes row-based `Record` data as columnar Parquet files using Apache Arrow
//! as the intermediate representation.

pub mod config;
pub mod error;
pub mod sink;

pub use sink::ParquetSink;
