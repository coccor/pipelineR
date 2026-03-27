//! SQL connector for pipelineR.
//!
//! Reads from and writes to SQL Server, PostgreSQL, and MySQL databases.

pub mod config;
pub mod driver;
pub mod driver_mysql;
pub mod driver_postgres;
pub mod driver_sqlserver;
pub mod error;
pub mod sink;
pub mod source;

pub use sink::SqlSink;
pub use source::SqlSource;
