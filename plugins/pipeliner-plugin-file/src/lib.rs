//! File plugin for pipelineR.
//!
//! Reads CSV, newline-delimited JSON, and Parquet files from the local filesystem (source).
//! Writes CSV and newline-delimited JSON files to the local filesystem (sink).

pub mod config;
pub mod csv_reader;
pub mod csv_writer;
pub mod error;
pub mod json_reader;
pub mod json_writer;
pub mod parquet_reader;
pub mod sink;
pub mod source;

pub use sink::FileSink;
pub use source::FileSource;
