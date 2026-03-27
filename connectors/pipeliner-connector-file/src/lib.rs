//! File connector for pipelineR.
//!
//! Reads CSV, newline-delimited JSON, and Parquet files from local filesystem or
//! cloud storage backends (S3, Azure Blob, GCS). Writes CSV and newline-delimited
//! JSON files to local filesystem or cloud storage.

pub mod config;
pub mod csv_reader;
pub mod csv_writer;
pub mod error;
pub mod json_reader;
pub mod json_writer;
pub mod parquet_reader;
pub mod sink;
pub mod source;
pub mod storage;
pub mod storage_azure;
pub mod storage_gcs;
pub mod storage_local;
pub mod storage_s3;

pub use sink::FileSink;
pub use source::FileSource;
