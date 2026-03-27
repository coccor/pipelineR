//! SQL source connector — implements the `Source` trait for SQL databases.

use async_trait::async_trait;
use chrono::{Duration, NaiveDate};
use pipeliner_core::record::{Record, Value, DEFAULT_BATCH_SIZE};
use pipeliner_proto::{
    ColumnSchema, Partition, RuntimeParams, SchemaResponse, SourceConfig, SourceDescriptor,
};
use pipeliner_sdk::error::{DiscoveryError, ExtractionError, ValidationError};
use pipeliner_sdk::Source;
use tokio::sync::mpsc;

use crate::config::{PartitionStrategy, SqlSourceConfig};
use crate::driver::map_sql_type_to_pipeliner;
use crate::DRIVER_POOL;

/// The SQL source connector.
pub struct SqlSource;

fn parse_source_config(config: &SourceConfig) -> Result<SqlSourceConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

/// Convert a list of (column_name, value) pairs into a core `Record`.
fn sql_row_to_record(row: Vec<(String, Value)>) -> Record {
    row.into_iter().collect()
}

/// Parse an interval string like `"1d"`, `"7d"`, `"30d"` into a `chrono::Duration`.
fn parse_interval(interval: &str) -> Result<Duration, String> {
    let s = interval.trim();
    if s.is_empty() {
        return Err("empty interval".to_string());
    }
    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: i64 = num_str
        .parse()
        .map_err(|e| format!("invalid interval number '{num_str}': {e}"))?;

    match unit {
        "d" => Ok(Duration::days(num)),
        "h" => Ok(Duration::hours(num)),
        "m" => Ok(Duration::days(num * 30)), // approximate months
        "w" => Ok(Duration::weeks(num)),
        _ => Err(format!("unknown interval unit '{unit}', expected d/h/w/m")),
    }
}

#[async_trait]
impl Source for SqlSource {
    fn describe(&self) -> SourceDescriptor {
        SourceDescriptor {
            name: "sql".to_string(),
            version: "0.1.0".to_string(),
            description: "SQL source connector — reads from SQL Server, PostgreSQL, and MySQL"
                .to_string(),
        }
    }

    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError> {
        let cfg = parse_source_config(config)?;

        if cfg.connection_string.is_empty() {
            return Err(ValidationError::MissingField(
                "connection_string".to_string(),
            ));
        }
        if cfg.query.is_empty() {
            return Err(ValidationError::MissingField("query".to_string()));
        }

        // Validate partition config if present.
        if let Some(ref partition) = cfg.partition {
            match partition.strategy {
                PartitionStrategy::DateRange => {
                    if partition.date_range.is_none() {
                        return Err(ValidationError::MissingField(
                            "partition.date_range".to_string(),
                        ));
                    }
                }
                PartitionStrategy::KeyRange => {
                    if partition.key_range.is_none() {
                        return Err(ValidationError::MissingField(
                            "partition.key_range".to_string(),
                        ));
                    }
                }
                PartitionStrategy::Single => {}
            }
        }

        Ok(())
    }

    async fn discover_schema(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError> {
        let cfg =
            parse_source_config(config).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let driver = DRIVER_POOL
            .get_or_create(&cfg.driver, &cfg.connection_string)
            .await
            .map_err(|e| DiscoveryError::Connection(e.to_string()))?;

        // Try to extract the table name from the query for INFORMATION_SCHEMA lookup.
        // If that fails, fall back to executing the query with LIMIT 0.
        let table_name = extract_table_name(&cfg.query);

        let schema_pairs = if let Some(table) = table_name {
            driver
                .query_schema(&table)
                .await
                .map_err(|e| DiscoveryError::Failed(e.to_string()))?
        } else {
            // Execute query with limit 0 to discover column names.
            let limited_query = format!("SELECT * FROM ({}) AS _q LIMIT 0", cfg.query);
            let rows = driver
                .execute_query(&limited_query)
                .await
                .unwrap_or_default();
            // If we got rows, use first row's column names; otherwise return empty.
            if let Some(first) = rows.first() {
                first
                    .iter()
                    .map(|(name, _)| (name.clone(), "string".to_string()))
                    .collect()
            } else {
                Vec::new()
            }
        };

        let columns = schema_pairs
            .into_iter()
            .map(|(name, data_type)| ColumnSchema {
                name,
                data_type: map_sql_type_to_pipeliner(&data_type),
                nullable: true,
                description: String::new(),
            })
            .collect();

        Ok(SchemaResponse { columns })
    }

    async fn discover_partitions(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError> {
        let cfg =
            parse_source_config(config).map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let partition_cfg = match cfg.partition {
            Some(ref p) => p,
            None => {
                // Default: single partition.
                return Ok(vec![Partition {
                    key: "single".to_string(),
                    params: std::collections::HashMap::new(),
                }]);
            }
        };

        match partition_cfg.strategy {
            PartitionStrategy::Single => Ok(vec![Partition {
                key: "single".to_string(),
                params: std::collections::HashMap::new(),
            }]),
            PartitionStrategy::DateRange => {
                let dr = partition_cfg.date_range.as_ref().ok_or_else(|| {
                    DiscoveryError::Failed("date_range config is required".to_string())
                })?;

                let start = NaiveDate::parse_from_str(&dr.start, "%Y-%m-%d")
                    .map_err(|e| DiscoveryError::Failed(format!("invalid start date: {e}")))?;
                let end = NaiveDate::parse_from_str(&dr.end, "%Y-%m-%d")
                    .map_err(|e| DiscoveryError::Failed(format!("invalid end date: {e}")))?;
                let interval = parse_interval(&dr.interval)
                    .map_err(|e| DiscoveryError::Failed(format!("invalid interval: {e}")))?;

                let mut partitions = Vec::new();
                let mut current = start;
                while current < end {
                    let next = current + interval;
                    let next = if next > end { end } else { next };

                    let mut params = std::collections::HashMap::new();
                    params.insert("start".to_string(), current.format("%Y-%m-%d").to_string());
                    params.insert("end".to_string(), next.format("%Y-%m-%d").to_string());
                    params.insert("column".to_string(), dr.column.clone());

                    let key = format!("{}..{}", current.format("%Y-%m-%d"), next.format("%Y-%m-%d"));
                    partitions.push(Partition { key, params });

                    current = next;
                }

                Ok(partitions)
            }
            PartitionStrategy::KeyRange => {
                let kr = partition_cfg.key_range.as_ref().ok_or_else(|| {
                    DiscoveryError::Failed("key_range config is required".to_string())
                })?;

                let partitions = kr
                    .values
                    .iter()
                    .map(|v| {
                        let mut params = std::collections::HashMap::new();
                        params.insert("key_value".to_string(), v.clone());
                        params.insert("column".to_string(), kr.column.clone());
                        Partition {
                            key: v.clone(),
                            params,
                        }
                    })
                    .collect();

                Ok(partitions)
            }
        }
    }

    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<pipeliner_proto::RecordBatch>,
    ) -> Result<String, ExtractionError> {
        let cfg =
            parse_source_config(config).map_err(|e| ExtractionError::Failed(e.to_string()))?;

        let driver = DRIVER_POOL
            .get_or_create(&cfg.driver, &cfg.connection_string)
            .await
            .map_err(|e| ExtractionError::Connection(e.to_string()))?;

        // Substitute partition placeholders in the query.
        let query = substitute_query(&cfg.query, &params.params);

        let rows = driver
            .execute_query(&query)
            .await
            .map_err(|e| ExtractionError::Failed(e.to_string()))?;

        // Compute watermark if configured.
        let mut watermark = String::new();
        if let Some(ref wm_col) = cfg.watermark_column {
            for row in &rows {
                for (col_name, val) in row {
                    if col_name == wm_col {
                        let val_str = val.to_string();
                        if val_str > watermark {
                            watermark = val_str;
                        }
                    }
                }
            }
        }

        // Batch rows and send as proto RecordBatch.
        let convert = pipeliner_sdk::convert::core_batch_to_proto;
        let mut batch_records: Vec<Record> = Vec::with_capacity(DEFAULT_BATCH_SIZE);

        for row in rows {
            batch_records.push(sql_row_to_record(row));

            if batch_records.len() >= DEFAULT_BATCH_SIZE {
                let core_batch =
                    pipeliner_core::record::RecordBatch::new(std::mem::take(&mut batch_records));
                tx.send(convert(&core_batch))
                    .await
                    .map_err(|_| ExtractionError::ChannelClosed)?;
            }
        }

        // Flush remaining rows.
        if !batch_records.is_empty() {
            let core_batch = pipeliner_core::record::RecordBatch::new(batch_records);
            tx.send(convert(&core_batch))
                .await
                .map_err(|_| ExtractionError::ChannelClosed)?;
        }

        Ok(watermark)
    }
}

/// Try to extract a simple table name from a query like `SELECT ... FROM table_name`.
fn extract_table_name(query: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let from_idx = upper.find("FROM")?;
    let after_from = &query[from_idx + 4..].trim_start();
    let table = after_from
        .split_whitespace()
        .next()?
        .trim_end_matches(';')
        .to_string();

    // Skip subqueries.
    if table.starts_with('(') {
        return None;
    }

    Some(table)
}

/// Substitute `$start`, `$end`, `$key_value`, `$column` placeholders in a query
/// using partition params. Values are SQL-escaped to prevent injection.
fn substitute_query(query: &str, params: &std::collections::HashMap<String, String>) -> String {
    let mut result = query.to_string();
    for (key, value) in params {
        let placeholder = format!("${key}");
        let escaped = escape_sql_param(value);
        result = result.replace(&placeholder, &escaped);
    }
    result
}

/// Escape a SQL parameter value by doubling single quotes.
/// This escapes the value content but does NOT add surrounding quotes —
/// the query template is expected to include quotes where needed
/// (e.g., `WHERE date >= '$start'`).
fn escape_sql_param(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_table_name_simple() {
        assert_eq!(
            extract_table_name("SELECT * FROM users WHERE id > 1"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_extract_table_name_schema_qualified() {
        assert_eq!(
            extract_table_name("SELECT id, name FROM public.users"),
            Some("public.users".to_string())
        );
    }

    #[test]
    fn test_extract_table_name_subquery() {
        assert_eq!(
            extract_table_name("SELECT * FROM (SELECT 1) AS t"),
            None
        );
    }

    #[test]
    fn test_substitute_query() {
        let mut params = std::collections::HashMap::new();
        params.insert("start".to_string(), "2024-01-01".to_string());
        params.insert("end".to_string(), "2024-02-01".to_string());

        let result = substitute_query(
            "SELECT * FROM orders WHERE created_at >= '$start' AND created_at < '$end'",
            &params,
        );
        assert_eq!(
            result,
            "SELECT * FROM orders WHERE created_at >= '2024-01-01' AND created_at < '2024-02-01'"
        );
    }

    #[test]
    fn test_substitute_query_escapes_values() {
        let mut params = std::collections::HashMap::new();
        params.insert("key_value".to_string(), "O'Brien".to_string());

        let result = substitute_query(
            "SELECT * FROM users WHERE name = '$key_value'",
            &params,
        );
        assert_eq!(
            result,
            "SELECT * FROM users WHERE name = 'O''Brien'"
        );
    }

    #[test]
    fn test_parse_interval_days() {
        let d = parse_interval("7d").unwrap();
        assert_eq!(d, Duration::days(7));
    }

    #[test]
    fn test_parse_interval_hours() {
        let d = parse_interval("24h").unwrap();
        assert_eq!(d, Duration::hours(24));
    }

    #[test]
    fn test_parse_interval_invalid() {
        assert!(parse_interval("").is_err());
        assert!(parse_interval("abc").is_err());
    }
}
