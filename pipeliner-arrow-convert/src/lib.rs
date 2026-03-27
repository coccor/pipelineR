//! Row-based Record to Arrow RecordBatch conversion utility.
//!
//! Used by columnar sink connectors (Delta Lake, Parquet) to convert
//! pipelineR's internal `Record` type to Apache Arrow `RecordBatch`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use indexmap::IndexMap;
use pipeliner_core::record::{Record, Value};

/// Errors that can occur during Record-to-Arrow conversion.
#[derive(Debug, thiserror::Error)]
pub enum ArrowConvertError {
    /// An underlying Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A conversion error with a descriptive message.
    #[error("conversion error: {0}")]
    Conversion(String),
}

/// Infer an Arrow `Schema` from a slice of `Record`s.
///
/// Scans all records to build the most complete schema. For each field, the
/// first non-null value determines the Arrow data type. Fields that are only
/// ever null default to `Utf8`.
pub fn infer_schema(records: &[Record]) -> ArrowSchema {
    // Collect field names in insertion order, and determine types.
    let mut fields: IndexMap<String, DataType> = IndexMap::new();

    for record in records {
        for (key, value) in record {
            let entry = fields.entry(key.clone()).or_insert(DataType::Utf8);
            // Only update if the current type is still the default and this value is non-null.
            if *entry == DataType::Utf8 || matches!(value, Value::Null) {
                let inferred = value_to_arrow_type(value);
                if !matches!(value, Value::Null) {
                    *entry = inferred;
                }
            }
        }
    }

    let arrow_fields: Vec<Field> = fields
        .into_iter()
        .map(|(name, dt)| Field::new(name, dt, true))
        .collect();

    ArrowSchema::new(arrow_fields)
}

/// Convert a `Vec<Record>` to an Arrow `RecordBatch` using the given schema.
///
/// If `schema` is `None`, the schema is inferred from the records by scanning
/// all values. Records missing a field get a null entry; type mismatches also
/// produce nulls.
///
/// # Errors
///
/// Returns `ArrowConvertError` if Arrow array construction or RecordBatch
/// creation fails.
pub fn records_to_arrow_batch(
    records: &[Record],
    schema: Option<&ArrowSchema>,
) -> Result<ArrowRecordBatch, ArrowConvertError> {
    if records.is_empty() {
        let s = schema.cloned().unwrap_or_else(|| ArrowSchema::new(Vec::<Field>::new()));
        return Ok(ArrowRecordBatch::new_empty(Arc::new(s)));
    }

    let inferred;
    let schema = match schema {
        Some(s) => s,
        None => {
            inferred = infer_schema(records);
            &inferred
        }
    };

    let num_rows = records.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col = build_array(field.name(), field.data_type(), records, num_rows)?;
        columns.push(col);
    }

    let batch = ArrowRecordBatch::try_new(Arc::new(schema.clone()), columns)?;
    Ok(batch)
}

/// Map a `Value` variant to the corresponding Arrow `DataType`.
fn value_to_arrow_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Utf8,
        Value::Bool(_) => DataType::Boolean,
        Value::Int(_) => DataType::Int64,
        Value::Float(_) => DataType::Float64,
        Value::String(_) => DataType::Utf8,
        Value::Bytes(_) => DataType::Binary,
        Value::Timestamp(_) => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        Value::Array(_) | Value::Map(_) => DataType::Utf8, // serialised as JSON string
    }
}

/// Build an Arrow array for a single column from all records.
fn build_array(
    field_name: &str,
    data_type: &DataType,
    records: &[Record],
    num_rows: usize,
) -> Result<ArrayRef, ArrowConvertError> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for record in records {
                match record.get(field_name) {
                    Some(Value::Bool(b)) => builder.append_value(*b),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(), // type mismatch
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for record in records {
                match record.get(field_name) {
                    Some(Value::Int(i)) => builder.append_value(*i),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for record in records {
                match record.get(field_name) {
                    Some(Value::Float(f)) => builder.append_value(*f),
                    Some(Value::Int(i)) => builder.append_value(*i as f64), // promote int to float
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for record in records {
                match record.get(field_name) {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(Value::Array(arr)) => {
                        let json = serialize_value_to_json(&Value::Array(arr.clone()));
                        builder.append_value(json);
                    }
                    Some(Value::Map(m)) => {
                        let json = serialize_value_to_json(&Value::Map(m.clone()));
                        builder.append_value(json);
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 64);
            for record in records {
                match record.get(field_name) {
                    Some(Value::Bytes(b)) => builder.append_value(b),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(num_rows);
            for record in records {
                match record.get(field_name) {
                    Some(Value::Timestamp(ts)) => {
                        builder.append_value(ts.timestamp_nanos_opt().unwrap_or(0));
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            let array = builder
                .finish()
                .with_timezone("UTC");
            Ok(Arc::new(array))
        }
        other => Err(ArrowConvertError::Conversion(format!(
            "unsupported Arrow data type for field '{field_name}': {other}"
        ))),
    }
}

/// Serialise a `Value` to a JSON string for nested types (Array, Map).
fn serialize_value_to_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_finite() {
                serde_json::to_string(f).unwrap_or_else(|_| f.to_string())
            } else {
                "null".to_string()
            }
        }
        Value::String(s) => serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\"")),
        Value::Bytes(b) => {
            use serde_json::Value as JV;
            let arr: Vec<JV> = b.iter().map(|byte| JV::from(*byte)).collect();
            serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())
        }
        Value::Timestamp(t) => format!("\"{t}\""),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(serialize_value_to_json).collect();
            format!("[{}]", items.join(","))
        }
        Value::Map(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    let key_json = serde_json::to_string(k).unwrap_or_else(|_| format!("\"{k}\""));
                    let val_json = serialize_value_to_json(v);
                    format!("{key_json}:{val_json}")
                })
                .collect();
            format!("{{{}}}", entries.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_infer_schema_basic() {
        let records: Vec<Record> = vec![
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
        ];
        let schema = infer_schema(&records);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(*schema.field(1).data_type(), DataType::Int64);
    }

    #[test]
    fn test_infer_schema_null_defaults_to_utf8() {
        let records: Vec<Record> = vec![
            [("x".to_string(), Value::Null)].into_iter().collect(),
        ];
        let schema = infer_schema(&records);
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
    }

    #[test]
    fn test_records_to_arrow_batch_empty() {
        let records: Vec<Record> = vec![];
        let result = records_to_arrow_batch(&records, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().num_rows(), 0);
    }

    #[test]
    fn test_records_to_arrow_batch_basic() {
        let records: Vec<Record> = vec![
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
                ("active".to_string(), Value::Bool(true)),
            ]
            .into_iter()
            .collect(),
            [
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::Int(25)),
                ("active".to_string(), Value::Bool(false)),
            ]
            .into_iter()
            .collect(),
        ];

        let batch = records_to_arrow_batch(&records, None).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_records_to_arrow_batch_with_timestamp() {
        let now = Utc::now();
        let records: Vec<Record> = vec![
            [("ts".to_string(), Value::Timestamp(now))].into_iter().collect(),
        ];
        let batch = records_to_arrow_batch(&records, None).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_records_to_arrow_batch_with_nulls() {
        let records: Vec<Record> = vec![
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
            [
                ("name".to_string(), Value::Null),
                ("age".to_string(), Value::Null),
            ]
            .into_iter()
            .collect(),
        ];

        let batch = records_to_arrow_batch(&records, None).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
