//! SQL driver abstraction trait and factory function.

use async_trait::async_trait;
use pipeliner_core::record::Value;

use crate::config::SqlDriver;
use crate::driver_mysql::MysqlDriver;
use crate::driver_postgres::PostgresDriver;
use crate::driver_sqlserver::SqlServerDriver;
use crate::error::SqlError;

/// A row returned from a query: ordered list of (column_name, value) pairs.
pub type SqlRow = Vec<(String, Value)>;

/// Abstraction over different SQL database drivers.
///
/// Each driver implementation connects to a specific database engine and
/// provides a uniform interface for querying and executing statements.
#[async_trait]
pub trait SqlDriverTrait: Send + Sync {
    /// Execute a SELECT query and return all rows.
    ///
    /// # Errors
    ///
    /// Returns `SqlError` if the query fails or the connection is lost.
    async fn execute_query(&self, query: &str) -> Result<Vec<SqlRow>, SqlError>;

    /// Execute a non-SELECT statement (INSERT, UPDATE, DELETE, TRUNCATE, etc.)
    /// and return the number of affected rows.
    ///
    /// # Errors
    ///
    /// Returns `SqlError` if the statement fails.
    async fn execute_statement(&self, stmt: &str) -> Result<u64, SqlError>;

    /// Query INFORMATION_SCHEMA.COLUMNS for schema discovery.
    /// Returns a list of (column_name, data_type_string) pairs.
    ///
    /// # Errors
    ///
    /// Returns `SqlError` if the schema query fails.
    async fn query_schema(&self, table_or_query: &str) -> Result<Vec<(String, String)>, SqlError>;
}

/// Create a connected SQL driver for the given database type.
///
/// # Errors
///
/// Returns `SqlError::Connection` if the connection cannot be established.
pub async fn create_driver(
    driver: &SqlDriver,
    connection_string: &str,
) -> Result<Box<dyn SqlDriverTrait>, SqlError> {
    match driver {
        SqlDriver::Postgres => {
            let d = PostgresDriver::connect(connection_string).await?;
            Ok(Box::new(d))
        }
        SqlDriver::Sqlserver => {
            let d = SqlServerDriver::connect(connection_string).await?;
            Ok(Box::new(d))
        }
        SqlDriver::Mysql => {
            let d = MysqlDriver::connect(connection_string).await?;
            Ok(Box::new(d))
        }
    }
}

/// Map a SQL type string (from INFORMATION_SCHEMA) to a pipelineR data type string.
///
/// Returns one of: `"string"`, `"int"`, `"float"`, `"bool"`, `"timestamp"`.
pub fn map_sql_type_to_pipeliner(sql_type: &str) -> String {
    let lower = sql_type.to_lowercase();
    if lower.contains("int") || lower.contains("serial") {
        "int".to_string()
    } else if lower.contains("float")
        || lower.contains("double")
        || lower.contains("real")
        || lower.contains("numeric")
        || lower.contains("decimal")
        || lower.contains("money")
    {
        "float".to_string()
    } else if lower.contains("bool") || lower == "bit" || lower == "tinyint(1)" {
        "bool".to_string()
    } else if lower.contains("timestamp")
        || lower.contains("datetime")
        || lower.contains("date")
        || lower.contains("time")
    {
        "timestamp".to_string()
    } else {
        "string".to_string()
    }
}
