//! Parquet sink connector — writes records as Parquet files.

use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use indexmap::IndexMap;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use pipeliner_core::record::{Record, Value};
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

use crate::config::{Compression, ParquetSinkConfig};

/// The Parquet sink connector.
pub struct ParquetSink;

/// Parse and validate the sink configuration from a `SinkConfig`.
fn parse_config(config: &SinkConfig) -> Result<ParquetSinkConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

#[async_trait]
impl Sink for ParquetSink {
    /// Return metadata about this sink connector.
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "parquet".to_string(),
            version: "0.1.0".to_string(),
            description: "Parquet sink connector — writes Arrow-columnar Parquet files".to_string(),
        }
    }

    /// Validate the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if `path` is missing or the parent directory does not exist.
    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg = parse_config(config)?;

        if cfg.path.is_empty() {
            return Err(ValidationError::MissingField("path".to_string()));
        }

        let path = std::path::Path::new(&cfg.path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(ValidationError::InvalidConfig(format!(
                    "parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        Ok(())
    }

    /// Declare schema requirements — the Parquet sink is flexible.
    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // FLEXIBLE
            fixed_schema: vec![],
        }
    }

    /// Load data from the receiver channel and write it as a Parquet file.
    ///
    /// # Errors
    ///
    /// Returns `LoadError` if record conversion, file creation, or writing fails.
    async fn load(
        &self,
        config: &SinkConfig,
        _schema: Option<SchemaResponse>,
        mut rx: mpsc::Receiver<pipeliner_proto::RecordBatch>,
    ) -> Result<LoadResult, LoadError> {
        let cfg = parse_config(config).map_err(|e| LoadError::Failed(e.to_string()))?;

        // Collect all records from the channel.
        let mut all_records = Vec::new();
        while let Some(batch) = rx.recv().await {
            let core_batch = proto_batch_to_core(&batch);
            all_records.extend(core_batch.records);
        }

        let total_rows = all_records.len() as i64;

        if all_records.is_empty() {
            return Ok(LoadResult {
                rows_written: 0,
                rows_errored: 0,
                error_message: String::new(),
            });
        }

        if cfg.partition_columns.is_empty() {
            // Single-file write (existing behaviour).
            write_parquet_file(&cfg.path, &all_records, &cfg).await?;
        } else {
            // Hive-style partitioned write.
            let groups = group_by_partitions(&all_records, &cfg.partition_columns);
            for (partition_path, records) in &groups {
                let dir = format!("{}/{}", cfg.path, partition_path);
                std::fs::create_dir_all(&dir)
                    .map_err(|e| LoadError::Failed(format!("mkdir failed: {e}")))?;
                let file_path = format!("{dir}/part-0.parquet");

                // Strip partition columns from the written data.
                let stripped: Vec<Record> = records
                    .iter()
                    .map(|r| {
                        r.iter()
                            .filter(|(k, _)| !cfg.partition_columns.contains(k))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    })
                    .collect();

                write_parquet_file(&file_path, &stripped, &cfg).await?;
            }
        }

        Ok(LoadResult {
            rows_written: total_rows,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}

/// Write a set of records to a single Parquet file at `path`.
async fn write_parquet_file(
    path: &str,
    records: &[Record],
    cfg: &ParquetSinkConfig,
) -> Result<(), LoadError> {
    let arrow_batch = pipeliner_arrow_convert::records_to_arrow_batch(records, None)
        .map_err(|e| LoadError::Failed(e.to_string()))?;

    let compression = match cfg.compression {
        Some(Compression::None) => ParquetCompression::UNCOMPRESSED,
        Some(Compression::Snappy) | None => ParquetCompression::SNAPPY,
        Some(Compression::Zstd) => ParquetCompression::ZSTD(Default::default()),
        Some(Compression::Gzip) => ParquetCompression::GZIP(Default::default()),
    };

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let schema = Arc::new(ArrowSchema::clone(arrow_batch.schema().as_ref()));
    let path = path.to_string();

    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let file = File::create(&path).map_err(|e| format!("failed to create file: {e}"))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| format!("failed to create parquet writer: {e}"))?;
        writer
            .write(&arrow_batch)
            .map_err(|e| format!("failed to write batch: {e}"))?;
        writer
            .close()
            .map_err(|e| format!("failed to close writer: {e}"))?;
        Ok(())
    })
    .await
    .map_err(|e| LoadError::Failed(format!("task join error: {e}")))?
    .map_err(LoadError::Failed)?;

    Ok(())
}

/// Group records by partition column values, returning an ordered map from
/// Hive-style partition path (e.g. `region=us/year=2024`) to the records
/// belonging to that partition.
pub fn group_by_partitions(
    records: &[Record],
    partition_columns: &[String],
) -> IndexMap<String, Vec<Record>> {
    let mut groups: IndexMap<String, Vec<Record>> = IndexMap::new();
    for record in records {
        let partition_path = partition_columns
            .iter()
            .map(|col| {
                let val = match record.get(col) {
                    Some(Value::String(s)) => s.clone(),
                    Some(v) => v.to_string(),
                    None => "__null__".to_string(),
                };
                format!("{col}={val}")
            })
            .collect::<Vec<_>>()
            .join("/");
        groups.entry(partition_path).or_default().push(record.clone());
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipeliner_core::record::Value;

    fn make_record(pairs: Vec<(&str, Value)>) -> Record {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    #[test]
    fn group_by_single_column() {
        let records = vec![
            make_record(vec![
                ("region", Value::String("us".into())),
                ("id", Value::Int(1)),
            ]),
            make_record(vec![
                ("region", Value::String("eu".into())),
                ("id", Value::Int(2)),
            ]),
            make_record(vec![
                ("region", Value::String("us".into())),
                ("id", Value::Int(3)),
            ]),
        ];

        let groups = group_by_partitions(&records, &["region".to_string()]);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups["region=us"].len(), 2);
        assert_eq!(groups["region=eu"].len(), 1);
    }

    #[test]
    fn group_by_multiple_columns() {
        let records = vec![
            make_record(vec![
                ("region", Value::String("us".into())),
                ("year", Value::Int(2024)),
                ("id", Value::Int(1)),
            ]),
            make_record(vec![
                ("region", Value::String("us".into())),
                ("year", Value::Int(2025)),
                ("id", Value::Int(2)),
            ]),
            make_record(vec![
                ("region", Value::String("us".into())),
                ("year", Value::Int(2024)),
                ("id", Value::Int(3)),
            ]),
        ];

        let cols = vec!["region".to_string(), "year".to_string()];
        let groups = group_by_partitions(&records, &cols);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups["region=us/year=2024"].len(), 2);
        assert_eq!(groups["region=us/year=2025"].len(), 1);
    }

    #[test]
    fn group_by_missing_column_uses_null_placeholder() {
        let records = vec![
            make_record(vec![("id", Value::Int(1))]),
            make_record(vec![
                ("region", Value::String("eu".into())),
                ("id", Value::Int(2)),
            ]),
        ];

        let groups = group_by_partitions(&records, &["region".to_string()]);

        assert_eq!(groups.len(), 2);
        assert!(groups.contains_key("region=__null__"));
        assert!(groups.contains_key("region=eu"));
    }

    #[test]
    fn group_by_empty_columns_returns_single_group() {
        let records = vec![
            make_record(vec![("id", Value::Int(1))]),
            make_record(vec![("id", Value::Int(2))]),
        ];

        let groups = group_by_partitions(&records, &[]);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[""].len(), 2);
    }
}
