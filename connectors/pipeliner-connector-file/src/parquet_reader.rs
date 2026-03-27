//! Parquet file reader — maps Parquet types to `Value` variants.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, RecordBatch as ArrowBatch, StringArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use indexmap::IndexMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pipeliner_core::record::{Record, RecordBatch, Value};

use crate::error::FileSourceError;

/// Read a Parquet file and return all records in batches.
pub fn read_parquet(path: &Path, batch_size: usize) -> Result<Vec<RecordBatch>, FileSourceError> {
    let file = std::fs::File::open(path)
        .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| FileSourceError::Parse(format!("{}: {e}", path.display())))?
        .with_batch_size(batch_size);

    let reader = builder
        .build()
        .map_err(|e| FileSourceError::Parse(format!("{}: {e}", path.display())))?;

    let mut batches = Vec::new();

    for arrow_batch in reader {
        let arrow_batch =
            arrow_batch.map_err(|e| FileSourceError::Parse(format!("{}: {e}", path.display())))?;
        let records = arrow_batch_to_records(&arrow_batch)?;
        if !records.is_empty() {
            batches.push(RecordBatch::new(records));
        }
    }

    Ok(batches)
}

/// Infer schema from Parquet file metadata.
pub fn infer_parquet_schema(path: &Path) -> Result<Vec<(String, String)>, FileSourceError> {
    let file = std::fs::File::open(path)
        .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| FileSourceError::Parse(format!("{}: {e}", path.display())))?;

    let schema = builder.schema();
    let columns: Vec<(String, String)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), arrow_type_to_string(f.data_type())))
        .collect();

    Ok(columns)
}

/// Convert an Arrow `RecordBatch` into a `Vec<Record>`.
fn arrow_batch_to_records(batch: &ArrowBatch) -> Result<Vec<Record>, FileSourceError> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut records = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut record: Record = IndexMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let value = arrow_col_to_value(col, row_idx, field.data_type())?;
            record.insert(field.name().clone(), value);
        }
        records.push(record);
    }

    Ok(records)
}

/// Extract a single value from an Arrow column at a given row index.
fn arrow_col_to_value(
    col: &Arc<dyn Array>,
    row: usize,
    dt: &DataType,
) -> Result<Value, FileSourceError> {
    if col.is_null(row) {
        return Ok(Value::Null);
    }

    match dt {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast BooleanArray".into())
            })?;
            Ok(Value::Bool(arr.value(row)))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast Int8Array".into())
            })?;
            Ok(Value::Int(i64::from(arr.value(row))))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast Int16Array".into())
            })?;
            Ok(Value::Int(i64::from(arr.value(row))))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast Int32Array".into())
            })?;
            Ok(Value::Int(i64::from(arr.value(row))))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast Int64Array".into())
            })?;
            Ok(Value::Int(arr.value(row)))
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast UInt8Array".into())
            })?;
            Ok(Value::Int(i64::from(arr.value(row))))
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast UInt16Array".into())
            })?;
            Ok(Value::Int(i64::from(arr.value(row))))
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast UInt32Array".into())
            })?;
            Ok(Value::Int(i64::from(arr.value(row))))
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast UInt64Array".into())
            })?;
            #[allow(clippy::cast_possible_wrap)]
            let val = arr.value(row) as i64;
            Ok(Value::Int(val))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast Float32Array".into())
            })?;
            Ok(Value::Float(f64::from(arr.value(row))))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast Float64Array".into())
            })?;
            Ok(Value::Float(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                FileSourceError::Parse("failed to downcast StringArray".into())
            })?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    FileSourceError::Parse("failed to downcast LargeStringArray".into())
                })?;
            Ok(Value::String(arr.value(row).to_string()))
        }
        DataType::Binary | DataType::LargeBinary => {
            // Binary data: represent as bytes or fallback to string.
            Ok(Value::String(format!("<binary at row {row}>")))
        }
        DataType::Timestamp(unit, _tz) => {
            let micros = match unit {
                TimeUnit::Second => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampSecondArray>()
                        .ok_or_else(|| {
                            FileSourceError::Parse("failed to downcast TimestampSecondArray".into())
                        })?;
                    arr.value(row) * 1_000_000
                }
                TimeUnit::Millisecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            FileSourceError::Parse(
                                "failed to downcast TimestampMillisecondArray".into(),
                            )
                        })?;
                    arr.value(row) * 1_000
                }
                TimeUnit::Microsecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            FileSourceError::Parse(
                                "failed to downcast TimestampMicrosecondArray".into(),
                            )
                        })?;
                    arr.value(row)
                }
                TimeUnit::Nanosecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            FileSourceError::Parse(
                                "failed to downcast TimestampNanosecondArray".into(),
                            )
                        })?;
                    arr.value(row) / 1_000
                }
            };
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nanos).ok_or_else(|| {
                FileSourceError::Parse(format!("invalid timestamp micros: {micros}"))
            })?;
            Ok(Value::Timestamp(dt))
        }
        _ => {
            // Unsupported types: render as string.
            Ok(Value::String(format!(
                "<unsupported type {dt:?} at row {row}>"
            )))
        }
    }
}

/// Map an Arrow `DataType` to a human-readable type name string for schema discovery.
fn arrow_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "int".to_string(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            "int".to_string()
        }
        DataType::Float32 | DataType::Float64 => "float".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Binary | DataType::LargeBinary => "bytes".to_string(),
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        other => format!("{other:?}"),
    }
}
