//! Delta Lake sink connector -- writes records to Delta Lake tables.

use async_trait::async_trait;
use datafusion::prelude::{col, SessionContext};
use deltalake::operations::DeltaOps;
use deltalake::protocol::SaveMode;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

use crate::config::{DeltaSinkConfig, WriteMode};

/// The Delta Lake sink connector.
pub struct DeltaSink;

/// Parse and validate the sink configuration from a `SinkConfig`.
fn parse_config(config: &SinkConfig) -> Result<DeltaSinkConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

/// Build a merge predicate from merge keys.
///
/// Joins each key as `"target.{key} = source.{key}"` with `" AND "`.
fn build_merge_predicate(merge_keys: &[String]) -> String {
    merge_keys
        .iter()
        .map(|k| format!("target.{k} = source.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[async_trait]
impl Sink for DeltaSink {
    /// Return metadata about this sink connector.
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "delta".to_string(),
            version: "0.1.0".to_string(),
            description: "Delta Lake sink connector — writes to Delta tables with append, overwrite, or merge modes".to_string(),
        }
    }

    /// Validate the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns `ValidationError` if `table_uri` is empty or merge mode is
    /// selected without merge keys.
    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg = parse_config(config)?;

        if cfg.table_uri.is_empty() {
            return Err(ValidationError::MissingField("table_uri".to_string()));
        }

        if cfg.write_mode == WriteMode::Merge && cfg.merge_keys.is_empty() {
            return Err(ValidationError::InvalidConfig(
                "merge write_mode requires at least one merge_key".to_string(),
            ));
        }

        Ok(())
    }

    /// Declare schema requirements -- the Delta sink is flexible.
    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // FLEXIBLE
            fixed_schema: vec![],
        }
    }

    /// Load data from the receiver channel and write it to a Delta Lake table.
    ///
    /// # Errors
    ///
    /// Returns `LoadError` if record conversion, table creation, or writing fails.
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

        // Convert to Arrow RecordBatch.
        let arrow_batch = pipeliner_arrow_convert::records_to_arrow_batch(&all_records, None)
            .map_err(|e| LoadError::Failed(e.to_string()))?;

        match cfg.write_mode {
            WriteMode::Append => {
                write_with_save_mode(&cfg, arrow_batch, SaveMode::Append).await?;
            }
            WriteMode::Overwrite => {
                write_with_save_mode(&cfg, arrow_batch, SaveMode::Overwrite).await?;
            }
            WriteMode::Merge => {
                merge_into_table(&cfg, arrow_batch).await?;
            }
        }

        Ok(LoadResult {
            rows_written: total_rows,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}

/// Write a batch to a Delta table using append or overwrite mode.
///
/// Opens or creates the table at `table_uri`, then writes the batch with the
/// specified save mode and optional partition columns.
async fn write_with_save_mode(
    cfg: &DeltaSinkConfig,
    batch: arrow::record_batch::RecordBatch,
    save_mode: SaveMode,
) -> Result<(), LoadError> {
    let ops = DeltaOps::try_from_uri(&cfg.table_uri)
        .await
        .map_err(|e| LoadError::Failed(format!("failed to open delta table: {e}")))?;

    let mut builder = ops.write(vec![batch]).with_save_mode(save_mode);

    if !cfg.partition_columns.is_empty() {
        builder = builder.with_partition_columns(cfg.partition_columns.clone());
    }

    builder
        .await
        .map_err(|e| LoadError::Failed(format!("delta write failed: {e}")))?;

    Ok(())
}

/// Merge (upsert) a batch into an existing Delta table.
///
/// Uses the configured merge keys to build a predicate and performs
/// WHEN MATCHED THEN UPDATE ALL / WHEN NOT MATCHED THEN INSERT ALL.
/// Each non-key column is explicitly mapped as `source.{col}`.
async fn merge_into_table(
    cfg: &DeltaSinkConfig,
    batch: arrow::record_batch::RecordBatch,
) -> Result<(), LoadError> {
    let table = deltalake::open_table(&cfg.table_uri)
        .await
        .map_err(|e| LoadError::Failed(format!("failed to open delta table for merge: {e}")))?;

    // Build a DataFrame from the Arrow batch so deltalake can use it as the merge source.
    let schema = batch.schema();
    let column_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let ctx = SessionContext::new();
    let source_df = ctx
        .read_batch(batch)
        .map_err(|e| LoadError::Failed(format!("failed to create dataframe from batch: {e}")))?;

    let predicate = cfg
        .merge_predicate
        .clone()
        .unwrap_or_else(|| build_merge_predicate(&cfg.merge_keys));

    let merge_builder = DeltaOps(table)
        .merge(source_df, predicate)
        .with_source_alias("source")
        .with_target_alias("target");

    // Build update clause: set every column to source.{col}.
    let merge_builder = merge_builder
        .when_matched_update(|update| {
            let mut u = update;
            for name in &column_names {
                u = u.update(name.as_str(), col(format!("source.{name}")));
            }
            u
        })
        .map_err(|e| LoadError::Failed(format!("delta merge update clause failed: {e}")))?;

    // Build insert clause: set every column to source.{col}.
    let merge_builder = merge_builder
        .when_not_matched_insert(|insert| {
            let mut i = insert;
            for name in &column_names {
                i = i.set(name.as_str(), col(format!("source.{name}")));
            }
            i
        })
        .map_err(|e| LoadError::Failed(format!("delta merge insert clause failed: {e}")))?;

    let (_table, _metrics) = merge_builder
        .await
        .map_err(|e| LoadError::Failed(format!("delta merge failed: {e}")))?;

    Ok(())
}
