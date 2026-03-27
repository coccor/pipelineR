//! PostgreSQL driver implementation using `tokio-postgres`.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use pipeliner_core::record::Value;
use tokio_postgres::{types::Type, Client, NoTls, Row};

use crate::driver::{SqlDriverTrait, SqlRow};
use crate::error::SqlError;

/// PostgreSQL driver backed by `tokio-postgres`.
pub struct PostgresDriver {
    client: Client,
    // The connection task handle — kept alive so the connection stays open.
    _handle: tokio::task::JoinHandle<()>,
}

impl PostgresDriver {
    /// Connect to a PostgreSQL database.
    ///
    /// # Errors
    ///
    /// Returns `SqlError::Connection` if the connection fails.
    pub async fn connect(connection_string: &str) -> Result<Self, SqlError> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .map_err(|e| SqlError::Connection(format!("postgres connection failed: {e}")))?;

        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {e}");
            }
        });

        Ok(Self {
            client,
            _handle: handle,
        })
    }

    /// Convert a single cell from a tokio-postgres Row into a pipelineR Value.
    fn row_value(row: &Row, idx: usize, col_type: &Type) -> Value {
        // Check for NULL first via try-get with Option.
        match *col_type {
            Type::BOOL => match row.try_get::<_, Option<bool>>(idx) {
                Ok(Some(v)) => Value::Bool(v),
                _ => Value::Null,
            },
            Type::INT2 => match row.try_get::<_, Option<i16>>(idx) {
                Ok(Some(v)) => Value::Int(i64::from(v)),
                _ => Value::Null,
            },
            Type::INT4 => match row.try_get::<_, Option<i32>>(idx) {
                Ok(Some(v)) => Value::Int(i64::from(v)),
                _ => Value::Null,
            },
            Type::INT8 | Type::OID => match row.try_get::<_, Option<i64>>(idx) {
                Ok(Some(v)) => Value::Int(v),
                _ => Value::Null,
            },
            Type::FLOAT4 => match row.try_get::<_, Option<f32>>(idx) {
                Ok(Some(v)) => Value::Float(f64::from(v)),
                _ => Value::Null,
            },
            Type::FLOAT8 => match row.try_get::<_, Option<f64>>(idx) {
                Ok(Some(v)) => Value::Float(v),
                _ => Value::Null,
            },
            Type::TIMESTAMP => match row.try_get::<_, Option<NaiveDateTime>>(idx) {
                Ok(Some(v)) => Value::Timestamp(DateTime::<Utc>::from_naive_utc_and_offset(v, Utc)),
                _ => Value::Null,
            },
            Type::TIMESTAMPTZ => match row.try_get::<_, Option<DateTime<Utc>>>(idx) {
                Ok(Some(v)) => Value::Timestamp(v),
                _ => Value::Null,
            },
            Type::DATE => match row.try_get::<_, Option<NaiveDate>>(idx) {
                Ok(Some(v)) => {
                    let dt = v
                        .and_hms_opt(0, 0, 0)
                        .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
                    dt.map_or(Value::Null, Value::Timestamp)
                }
                _ => Value::Null,
            },
            Type::TIME => match row.try_get::<_, Option<NaiveTime>>(idx) {
                Ok(Some(v)) => Value::String(v.to_string()),
                _ => Value::Null,
            },
            Type::BYTEA => match row.try_get::<_, Option<Vec<u8>>>(idx) {
                Ok(Some(v)) => Value::Bytes(v),
                _ => Value::Null,
            },
            _ => {
                // Fallback: try as string.
                match row.try_get::<_, Option<String>>(idx) {
                    Ok(Some(v)) => Value::String(v),
                    _ => Value::Null,
                }
            }
        }
    }
}

#[async_trait]
impl SqlDriverTrait for PostgresDriver {
    async fn execute_query(&self, query: &str) -> Result<Vec<SqlRow>, SqlError> {
        let rows = self
            .client
            .query(query, &[])
            .await
            .map_err(|e| SqlError::Query(format!("postgres query failed: {e}")))?;

        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let columns = row.columns();
            let mut sql_row = Vec::with_capacity(columns.len());
            for (idx, col) in columns.iter().enumerate() {
                let value = Self::row_value(row, idx, col.type_());
                sql_row.push((col.name().to_string(), value));
            }
            result.push(sql_row);
        }

        Ok(result)
    }

    async fn execute_statement(&self, stmt: &str) -> Result<u64, SqlError> {
        self.client
            .execute(stmt, &[])
            .await
            .map_err(|e| SqlError::Query(format!("postgres statement failed: {e}")))
    }

    async fn query_schema(&self, table_or_query: &str) -> Result<Vec<(String, String)>, SqlError> {
        // Extract table name — handle schema-qualified names.
        let parts: Vec<&str> = table_or_query.split('.').collect();
        let (schema, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", parts[0])
        };

        let query = format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = '{schema}' AND table_name = '{table}' \
             ORDER BY ordinal_position"
        );

        let rows = self
            .client
            .query(&query, &[])
            .await
            .map_err(|e| SqlError::Schema(format!("postgres schema query failed: {e}")))?;

        let mut columns = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String = row
                .try_get(0)
                .map_err(|e| SqlError::Schema(format!("failed to read column_name: {e}")))?;
            let data_type: String = row
                .try_get(1)
                .map_err(|e| SqlError::Schema(format!("failed to read data_type: {e}")))?;
            columns.push((name, data_type));
        }

        Ok(columns)
    }
}
