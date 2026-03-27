//! SQL driver abstraction trait, factory function, and connection pool.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use pipeliner_core::record::Value;
use tokio::sync::Mutex;

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

    /// Check whether the connection is still alive by running a lightweight query.
    ///
    /// # Errors
    ///
    /// Returns `SqlError` if the connection is dead.
    async fn ping(&self) -> Result<(), SqlError> {
        self.execute_query("SELECT 1").await.map(|_| ())
    }
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

/// Connection pool that caches SQL drivers by (driver type, connection string).
///
/// When a driver is requested for a connection that already exists in the pool,
/// the existing driver is returned (after a health check). This avoids creating
/// redundant TCP connections when the same database is used as both source and
/// sink, or when `discover_schema` and `extract` are called separately.
///
/// Each pooled driver is wrapped in `Arc` so it can be shared across callers.
pub struct DriverPool {
    drivers: Mutex<HashMap<(SqlDriver, String), Arc<dyn SqlDriverTrait>>>,
}

impl DriverPool {
    /// Create a new, empty connection pool.
    pub fn new() -> Self {
        Self {
            drivers: Mutex::new(HashMap::new()),
        }
    }

    /// Get a cached driver or create a new one for the given connection.
    ///
    /// If a driver already exists for this (driver, connection_string) pair,
    /// a health check (`ping`) is performed. If the ping fails, the stale
    /// connection is discarded and a fresh one is created.
    ///
    /// # Errors
    ///
    /// Returns `SqlError::Connection` if a new connection cannot be established.
    pub async fn get_or_create(
        &self,
        driver: &SqlDriver,
        connection_string: &str,
    ) -> Result<Arc<dyn SqlDriverTrait>, SqlError> {
        let key = (driver.clone(), connection_string.to_string());

        let mut map = self.drivers.lock().await;

        // Check if we already have a driver for this connection.
        if let Some(existing) = map.get(&key) {
            // Verify the connection is still alive.
            if existing.ping().await.is_ok() {
                return Ok(Arc::clone(existing));
            }
            // Connection is dead — remove it and fall through to create a new one.
            map.remove(&key);
        }

        // Create a fresh driver.
        let new_driver: Arc<dyn SqlDriverTrait> = match driver {
            SqlDriver::Postgres => {
                Arc::new(PostgresDriver::connect(connection_string).await?)
            }
            SqlDriver::Sqlserver => {
                Arc::new(SqlServerDriver::connect(connection_string).await?)
            }
            SqlDriver::Mysql => {
                Arc::new(MysqlDriver::connect(connection_string).await?)
            }
        };

        map.insert(key, Arc::clone(&new_driver));
        Ok(new_driver)
    }

    /// Remove all cached connections from the pool.
    pub async fn clear(&self) {
        self.drivers.lock().await.clear();
    }

    /// Return the number of cached connections.
    pub async fn len(&self) -> usize {
        self.drivers.lock().await.len()
    }

    /// Return true if the pool has no cached connections.
    pub async fn is_empty(&self) -> bool {
        self.drivers.lock().await.is_empty()
    }
}

impl Default for DriverPool {
    fn default() -> Self {
        Self::new()
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
