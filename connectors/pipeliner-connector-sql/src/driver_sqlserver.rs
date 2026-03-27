//! SQL Server driver implementation using `tiberius`.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pipeliner_core::record::Value;
use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::driver::{SqlDriverTrait, SqlRow};
use crate::error::SqlError;

/// SQL Server driver backed by `tiberius`.
pub struct SqlServerDriver {
    client: Mutex<Client<tokio_util::compat::Compat<TcpStream>>>,
}

impl SqlServerDriver {
    /// Connect to a SQL Server database.
    ///
    /// # Errors
    ///
    /// Returns `SqlError::Connection` if the connection fails.
    pub async fn connect(connection_string: &str) -> Result<Self, SqlError> {
        let config = Config::from_ado_string(connection_string).map_err(|e| {
            SqlError::Connection(format!("invalid SQL Server connection string: {e}"))
        })?;

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| SqlError::Connection(format!("SQL Server TCP connect failed: {e}")))?;

        tcp.set_nodelay(true)
            .map_err(|e| SqlError::Connection(format!("failed to set TCP_NODELAY: {e}")))?;

        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| SqlError::Connection(format!("SQL Server login failed: {e}")))?;

        Ok(Self {
            client: Mutex::new(client),
        })
    }

    /// Extract a value from a tiberius Row at the given column index.
    ///
    /// Tries multiple type conversions in order, falling back to string.
    fn extract_value(row: &Row, idx: usize) -> Value {
        // Try bool first (BIT columns).
        if let Ok(Some(v)) = row.try_get::<bool, _>(idx) {
            return Value::Bool(v);
        }
        // Try integer types.
        if let Ok(Some(v)) = row.try_get::<i64, _>(idx) {
            return Value::Int(v);
        }
        if let Ok(Some(v)) = row.try_get::<i32, _>(idx) {
            return Value::Int(i64::from(v));
        }
        if let Ok(Some(v)) = row.try_get::<i16, _>(idx) {
            return Value::Int(i64::from(v));
        }
        if let Ok(Some(v)) = row.try_get::<u8, _>(idx) {
            return Value::Int(i64::from(v));
        }
        // Try float types.
        if let Ok(Some(v)) = row.try_get::<f64, _>(idx) {
            return Value::Float(v);
        }
        if let Ok(Some(v)) = row.try_get::<f32, _>(idx) {
            return Value::Float(f64::from(v));
        }
        // Try datetime types (via chrono feature).
        if let Ok(Some(v)) = row.try_get::<DateTime<Utc>, _>(idx) {
            return Value::Timestamp(v);
        }
        if let Ok(Some(v)) = row.try_get::<NaiveDateTime, _>(idx) {
            return Value::Timestamp(DateTime::<Utc>::from_naive_utc_and_offset(v, Utc));
        }
        if let Ok(Some(v)) = row.try_get::<NaiveDate, _>(idx) {
            return v.and_hms_opt(0, 0, 0).map_or(Value::Null, |ndt| {
                Value::Timestamp(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
            });
        }
        if let Ok(Some(v)) = row.try_get::<NaiveTime, _>(idx) {
            return Value::String(v.to_string());
        }
        // Try binary.
        if let Ok(Some(v)) = row.try_get::<&[u8], _>(idx) {
            return Value::Bytes(v.to_vec());
        }
        // Try string (catch-all for NVARCHAR, VARCHAR, XML, etc.).
        if let Ok(Some(v)) = row.try_get::<&str, _>(idx) {
            return Value::String(v.to_string());
        }
        // NULL or unrecognised type.
        Value::Null
    }
}

#[async_trait]
impl SqlDriverTrait for SqlServerDriver {
    async fn execute_query(&self, query: &str) -> Result<Vec<SqlRow>, SqlError> {
        let mut client = self.client.lock().await;

        let stream = client
            .query(query, &[])
            .await
            .map_err(|e| SqlError::Query(format!("SQL Server query failed: {e}")))?;

        let rows = stream
            .into_results()
            .await
            .map_err(|e| SqlError::Query(format!("SQL Server result streaming failed: {e}")))?;

        let mut result = Vec::new();
        for result_set in &rows {
            for row in result_set {
                let columns = row.columns();
                let mut sql_row = Vec::with_capacity(columns.len());
                for (idx, col) in columns.iter().enumerate() {
                    let value = Self::extract_value(row, idx);
                    sql_row.push((col.name().to_string(), value));
                }
                result.push(sql_row);
            }
        }

        Ok(result)
    }

    async fn execute_statement(&self, stmt: &str) -> Result<u64, SqlError> {
        let mut client = self.client.lock().await;

        let result = client
            .execute(stmt, &[])
            .await
            .map_err(|e| SqlError::Query(format!("SQL Server statement failed: {e}")))?;

        Ok(result.rows_affected().iter().sum::<u64>())
    }

    async fn query_schema(&self, table_or_query: &str) -> Result<Vec<(String, String)>, SqlError> {
        let parts: Vec<&str> = table_or_query.split('.').collect();
        let (schema, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("dbo", parts[0])
        };

        let query = format!(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' \
             ORDER BY ORDINAL_POSITION"
        );

        let rows = self
            .execute_query(&query)
            .await
            .map_err(|e| SqlError::Schema(format!("SQL Server schema query failed: {e}")))?;

        let mut columns = Vec::with_capacity(rows.len());
        for row in &rows {
            let name = row
                .first()
                .map(|(_, v)| match v {
                    Value::String(s) => s.clone(),
                    _ => String::new(),
                })
                .unwrap_or_default();
            let data_type = row
                .get(1)
                .map(|(_, v)| match v {
                    Value::String(s) => s.clone(),
                    _ => String::new(),
                })
                .unwrap_or_default();
            columns.push((name, data_type));
        }

        Ok(columns)
    }
}
