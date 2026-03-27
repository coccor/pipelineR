//! SQL sink connector — implements the `Sink` trait for SQL databases.

use async_trait::async_trait;
use pipeliner_core::record::Value;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

use crate::config::{SqlDriver, SqlSinkConfig, WriteMode};
use crate::DRIVER_POOL;

/// The SQL sink connector.
pub struct SqlSink;

fn parse_sink_config(config: &SinkConfig) -> Result<SqlSinkConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

/// Escape a SQL string value by doubling single quotes.
fn escape_sql_value(s: &str) -> String {
    s.replace('\'', "''")
}

/// Convert a pipelineR `Value` to a SQL literal string.
fn value_to_sql_literal(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => format!("'{}'", escape_sql_value(s)),
        Value::Bytes(b) => format!("0x{}", hex_encode(b)),
        Value::Timestamp(ts) => format!("'{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f")),
        Value::Array(_) | Value::Map(_) => {
            // Serialize complex types as JSON strings.
            format!("'{}'", escape_sql_value(&val.to_string()))
        }
    }
}

/// Simple hex encoding for byte arrays.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Build a multi-row INSERT statement.
fn build_insert_sql(
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
) -> String {
    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let value_rows: Vec<String> = rows
        .iter()
        .map(|row| format!("({})", row.join(", ")))
        .collect();

    format!(
        "INSERT INTO {table} ({col_list}) VALUES {}",
        value_rows.join(", ")
    )
}

/// Build an upsert statement for PostgreSQL (INSERT ... ON CONFLICT).
fn build_postgres_upsert(
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
    merge_keys: &[String],
) -> String {
    let insert = build_insert_sql(table, columns, rows);
    let conflict_cols = merge_keys
        .iter()
        .map(|k| format!("\"{}\"", k))
        .collect::<Vec<_>>()
        .join(", ");

    let update_cols: Vec<String> = columns
        .iter()
        .filter(|c| !merge_keys.contains(c))
        .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c, c))
        .collect();

    if update_cols.is_empty() {
        format!("{insert} ON CONFLICT ({conflict_cols}) DO NOTHING")
    } else {
        format!(
            "{insert} ON CONFLICT ({conflict_cols}) DO UPDATE SET {}",
            update_cols.join(", ")
        )
    }
}

/// Build an upsert statement for MySQL (INSERT ... ON DUPLICATE KEY UPDATE).
fn build_mysql_upsert(
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
    merge_keys: &[String],
) -> String {
    let insert = build_insert_sql(table, columns, rows);

    let update_cols: Vec<String> = columns
        .iter()
        .filter(|c| !merge_keys.contains(c))
        .map(|c| format!("`{}` = VALUES(`{}`)", c, c))
        .collect();

    if update_cols.is_empty() {
        insert
    } else {
        format!(
            "{insert} ON DUPLICATE KEY UPDATE {}",
            update_cols.join(", ")
        )
    }
}

/// Build an upsert statement for SQL Server (MERGE).
fn build_sqlserver_upsert(
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
    merge_keys: &[String],
) -> String {
    // Build a VALUES table expression for the source.
    let value_rows: Vec<String> = rows
        .iter()
        .map(|row| format!("({})", row.join(", ")))
        .collect();

    let col_list = columns
        .iter()
        .map(|c| format!("[{}]", c))
        .collect::<Vec<_>>()
        .join(", ");

    let source_cols = columns
        .iter()
        .map(|c| format!("src.[{}]", c))
        .collect::<Vec<_>>()
        .join(", ");

    let on_clause = merge_keys
        .iter()
        .map(|k| format!("tgt.[{}] = src.[{}]", k, k))
        .collect::<Vec<_>>()
        .join(" AND ");

    let update_set: Vec<String> = columns
        .iter()
        .filter(|c| !merge_keys.contains(c))
        .map(|c| format!("tgt.[{}] = src.[{}]", c, c))
        .collect();

    let update_clause = if update_set.is_empty() {
        String::new()
    } else {
        format!(
            "WHEN MATCHED THEN UPDATE SET {}",
            update_set.join(", ")
        )
    };

    format!(
        "MERGE INTO {table} AS tgt \
         USING (VALUES {}) AS src ({col_list}) \
         ON {on_clause} \
         {update_clause} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({source_cols});",
        value_rows.join(", ")
    )
}

#[async_trait]
impl Sink for SqlSink {
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "sql".to_string(),
            version: "0.1.0".to_string(),
            description: "SQL sink connector — writes to SQL Server, PostgreSQL, and MySQL"
                .to_string(),
        }
    }

    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg = parse_sink_config(config)?;

        if cfg.connection_string.is_empty() {
            return Err(ValidationError::MissingField(
                "connection_string".to_string(),
            ));
        }
        if cfg.table.is_empty() {
            return Err(ValidationError::MissingField("table".to_string()));
        }
        if cfg.write_mode == WriteMode::Upsert && cfg.merge_keys.is_empty() {
            return Err(ValidationError::MissingField(
                "merge_keys (required for upsert mode)".to_string(),
            ));
        }

        Ok(())
    }

    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // FLEXIBLE
            fixed_schema: vec![],
        }
    }

    async fn load(
        &self,
        config: &SinkConfig,
        _schema: Option<SchemaResponse>,
        mut rx: mpsc::Receiver<pipeliner_proto::RecordBatch>,
    ) -> Result<LoadResult, LoadError> {
        let cfg = parse_sink_config(config).map_err(|e| LoadError::Failed(e.to_string()))?;

        let driver = DRIVER_POOL
            .get_or_create(&cfg.driver, &cfg.connection_string)
            .await
            .map_err(|e| LoadError::Connection(e.to_string()))?;

        // Truncate if truncate_and_load mode.
        if cfg.write_mode == WriteMode::TruncateAndLoad {
            driver
                .execute_statement(&format!("TRUNCATE TABLE {}", cfg.table))
                .await
                .map_err(|e| LoadError::Failed(format!("truncate failed: {e}")))?;
        }

        let mut rows_written: i64 = 0;

        while let Some(proto_batch) = rx.recv().await {
            let core_batch = proto_batch_to_core(&proto_batch);

            if core_batch.records.is_empty() {
                continue;
            }

            // Collect column names from the first record.
            let columns: Vec<String> = core_batch.records[0].keys().cloned().collect();

            if columns.is_empty() {
                continue;
            }

            // Convert records to SQL literal rows and batch them.
            let mut literal_rows: Vec<Vec<String>> = Vec::new();

            for record in &core_batch.records {
                let row: Vec<String> = columns
                    .iter()
                    .map(|col| {
                        record
                            .get(col)
                            .map_or_else(|| "NULL".to_string(), value_to_sql_literal)
                    })
                    .collect();
                literal_rows.push(row);

                if literal_rows.len() >= cfg.batch_size {
                    let sql = build_write_sql(
                        &cfg.driver,
                        &cfg.write_mode,
                        &cfg.table,
                        &columns,
                        &literal_rows,
                        &cfg.merge_keys,
                    );
                    driver
                        .execute_statement(&sql)
                        .await
                        .map_err(|e| LoadError::Failed(format!("insert failed: {e}")))?;
                    rows_written += literal_rows.len() as i64;
                    literal_rows.clear();
                }
            }

            // Flush remaining rows.
            if !literal_rows.is_empty() {
                let sql = build_write_sql(
                    &cfg.driver,
                    &cfg.write_mode,
                    &cfg.table,
                    &columns,
                    &literal_rows,
                    &cfg.merge_keys,
                );
                driver
                    .execute_statement(&sql)
                    .await
                    .map_err(|e| LoadError::Failed(format!("insert failed: {e}")))?;
                rows_written += literal_rows.len() as i64;
            }
        }

        Ok(LoadResult {
            rows_written,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}

/// Build the appropriate SQL write statement based on driver and write mode.
fn build_write_sql(
    driver: &SqlDriver,
    write_mode: &WriteMode,
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
    merge_keys: &[String],
) -> String {
    match write_mode {
        WriteMode::Insert | WriteMode::TruncateAndLoad => {
            build_insert_sql(table, columns, rows)
        }
        WriteMode::Upsert => match driver {
            SqlDriver::Postgres => build_postgres_upsert(table, columns, rows, merge_keys),
            SqlDriver::Mysql => build_mysql_upsert(table, columns, rows, merge_keys),
            SqlDriver::Sqlserver => build_sqlserver_upsert(table, columns, rows, merge_keys),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_sql_value() {
        assert_eq!(escape_sql_value("it's"), "it''s");
        assert_eq!(escape_sql_value("normal"), "normal");
    }

    #[test]
    fn test_value_to_sql_literal() {
        assert_eq!(value_to_sql_literal(&Value::Null), "NULL");
        assert_eq!(value_to_sql_literal(&Value::Int(42)), "42");
        assert_eq!(value_to_sql_literal(&Value::Float(3.14)), "3.14");
        assert_eq!(
            value_to_sql_literal(&Value::String("hello".to_string())),
            "'hello'"
        );
        assert_eq!(value_to_sql_literal(&Value::Bool(true)), "1");
        assert_eq!(value_to_sql_literal(&Value::Bool(false)), "0");
    }

    #[test]
    fn test_build_insert_sql() {
        let sql = build_insert_sql(
            "users",
            &["id".to_string(), "name".to_string()],
            &[
                vec!["1".to_string(), "'Alice'".to_string()],
                vec!["2".to_string(), "'Bob'".to_string()],
            ],
        );
        assert!(sql.contains("INSERT INTO users"));
        assert!(sql.contains("(1, 'Alice')"));
        assert!(sql.contains("(2, 'Bob')"));
    }

    #[test]
    fn test_build_postgres_upsert() {
        let sql = build_postgres_upsert(
            "users",
            &["id".to_string(), "name".to_string()],
            &[vec!["1".to_string(), "'Alice'".to_string()]],
            &["id".to_string()],
        );
        assert!(sql.contains("ON CONFLICT"));
        assert!(sql.contains("DO UPDATE SET"));
    }

    #[test]
    fn test_build_mysql_upsert() {
        let sql = build_mysql_upsert(
            "users",
            &["id".to_string(), "name".to_string()],
            &[vec!["1".to_string(), "'Alice'".to_string()]],
            &["id".to_string()],
        );
        assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    #[test]
    fn test_build_sqlserver_upsert() {
        let sql = build_sqlserver_upsert(
            "users",
            &["id".to_string(), "name".to_string()],
            &[vec!["1".to_string(), "'Alice'".to_string()]],
            &["id".to_string()],
        );
        assert!(sql.contains("MERGE INTO"));
        assert!(sql.contains("WHEN NOT MATCHED"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }
}
