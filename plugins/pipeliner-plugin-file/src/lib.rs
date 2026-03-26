//! File source plugin for pipelineR.
//!
//! Reads CSV, newline-delimited JSON, and Parquet files from the local filesystem.

pub mod config;
pub mod csv_reader;
pub mod error;
pub mod json_reader;
pub mod parquet_reader;
pub mod source;

pub use source::FileSource;
