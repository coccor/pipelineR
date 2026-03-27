//! MySQL driver implementation using `mysql_async`.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Row, Value as MysqlValue};
use pipeliner_core::record::Value;
use tokio::sync::Mutex;

use crate::driver::{SqlDriverTrait, SqlRow};
use crate::error::SqlError;

/// MySQL driver backed by `mysql_async`.
pub struct MysqlDriver {
    conn: Mutex<Conn>,
}

impl MysqlDriver {
    /// Connect to a MySQL database.
    ///
    /// # Errors
    ///
    /// Returns `SqlError::Connection` if the connection fails.
    pub async fn connect(connection_string: &str) -> Result<Self, SqlError> {
        let opts = Opts::from_url(connection_string)
            .map_err(|e| SqlError::Connection(format!("invalid MySQL connection string: {e}")))?;

        let conn = Conn::new(opts)
            .await
            .map_err(|e| SqlError::Connection(format!("MySQL connection failed: {e}")))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Convert a `mysql_async::Value` to a pipelineR `Value`.
    fn mysql_value_to_value(val: &MysqlValue) -> Value {
        match val {
            MysqlValue::NULL => Value::Null,
            MysqlValue::Bytes(b) => {
                // Try to interpret as UTF-8 string first.
                match String::from_utf8(b.clone()) {
                    Ok(s) => Value::String(s),
                    Err(_) => Value::Bytes(b.clone()),
                }
            }
            MysqlValue::Int(i) => Value::Int(*i),
            MysqlValue::UInt(u) => Value::Int(*u as i64),
            MysqlValue::Float(f) => Value::Float(*f as f64),
            MysqlValue::Double(f) => Value::Float(*f),
            MysqlValue::Date(year, month, day, hour, min, sec, _micro) => {
                let nd = NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day));
                let ndt = nd.and_then(|d| d.and_hms_opt(u32::from(*hour), u32::from(*min), u32::from(*sec)));
                ndt.map_or(Value::Null, |dt| {
                    Value::Timestamp(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
                })
            }
            MysqlValue::Time(_neg, _days, hours, mins, secs, _micro) => {
                Value::String(format!("{hours:02}:{mins:02}:{secs:02}"))
            }
        }
    }
}

#[async_trait]
impl SqlDriverTrait for MysqlDriver {
    async fn execute_query(&self, query: &str) -> Result<Vec<SqlRow>, SqlError> {
        let mut conn = self.conn.lock().await;

        let rows: Vec<Row> = conn
            .query(query)
            .await
            .map_err(|e| SqlError::Query(format!("MySQL query failed: {e}")))?;

        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            let columns = row.columns_ref();
            let mut sql_row = Vec::with_capacity(columns.len());
            for (idx, col) in columns.iter().enumerate() {
                let name = col.name_str().to_string();
                let raw_val: MysqlValue = row
                    .as_ref(idx)
                    .cloned()
                    .unwrap_or(MysqlValue::NULL);
                let value = Self::mysql_value_to_value(&raw_val);
                sql_row.push((name, value));
            }
            result.push(sql_row);
        }

        Ok(result)
    }

    async fn execute_statement(&self, stmt: &str) -> Result<u64, SqlError> {
        let mut conn = self.conn.lock().await;

        conn.query_drop(stmt)
            .await
            .map_err(|e| SqlError::Query(format!("MySQL statement failed: {e}")))?;

        Ok(conn.affected_rows())
    }

    async fn query_schema(&self, table_or_query: &str) -> Result<Vec<(String, String)>, SqlError> {
        let parts: Vec<&str> = table_or_query.split('.').collect();
        let table = if parts.len() == 2 { parts[1] } else { parts[0] };

        let query = format!(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = '{table}' \
             ORDER BY ORDINAL_POSITION"
        );

        let rows = self
            .execute_query(&query)
            .await
            .map_err(|e| SqlError::Schema(format!("MySQL schema query failed: {e}")))?;

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
