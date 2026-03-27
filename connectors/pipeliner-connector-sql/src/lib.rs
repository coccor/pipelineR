//! SQL connector for pipelineR.
//!
//! Reads from and writes to SQL Server, PostgreSQL, and MySQL databases.

use std::sync::LazyLock;

pub mod config;
pub mod driver;
pub mod driver_mysql;
pub mod driver_postgres;
pub mod driver_sqlserver;
pub mod error;
pub mod sink;
pub mod source;

pub use driver::DriverPool;
pub use sink::SqlSink;
pub use source::SqlSource;

/// Process-wide connection pool shared between source and sink operations.
///
/// Connections are cached by (driver type, connection string) so that
/// multiple operations against the same database reuse the same TCP
/// connection instead of opening a new one each time.
pub static DRIVER_POOL: LazyLock<DriverPool> = LazyLock::new(DriverPool::new);
