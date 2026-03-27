//! Newline-delimited JSON reader — preserves nested structure.

use std::io::{BufRead, BufReader};
use std::path::Path;

use indexmap::IndexMap;
use pipeliner_core::record::{Record, RecordBatch, Value};

use crate::error::FileSourceError;

/// Read a newline-delimited JSON file and return records in batches.
pub fn read_ndjson(path: &Path, batch_size: usize) -> Result<Vec<RecordBatch>, FileSourceError> {
    let file = std::fs::File::open(path)
        .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;
    let reader = BufReader::new(file);

    let mut batches = Vec::new();
    let mut current_batch: Vec<Record> = Vec::with_capacity(batch_size);

    for (line_num, line) in reader.lines().enumerate() {
        let line =
            line.map_err(|e| FileSourceError::Io(format!("{}:{}: {e}", path.display(), line_num)))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let json_val: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            FileSourceError::Parse(format!("{}:{}: {e}", path.display(), line_num + 1))
        })?;

        let record = json_value_to_record(&json_val)?;
        current_batch.push(record);

        if current_batch.len() >= batch_size {
            batches.push(RecordBatch::new(std::mem::take(&mut current_batch)));
            current_batch = Vec::with_capacity(batch_size);
        }
    }

    if !current_batch.is_empty() {
        batches.push(RecordBatch::new(current_batch));
    }

    Ok(batches)
}

/// Convert a top-level JSON object to a `Record`.
fn json_value_to_record(val: &serde_json::Value) -> Result<Record, FileSourceError> {
    match val {
        serde_json::Value::Object(map) => {
            let mut record: Record = IndexMap::new();
            for (k, v) in map {
                record.insert(k.clone(), json_to_value(v));
            }
            Ok(record)
        }
        other => Err(FileSourceError::Parse(format!(
            "expected JSON object, got {}",
            json_type_name(other)
        ))),
    }
}

/// Recursively convert a JSON value to a core `Value`.
fn json_to_value(val: &serde_json::Value) -> Value {
    match val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(map) => {
            let mut record: Record = IndexMap::new();
            for (k, v) in map {
                record.insert(k.clone(), json_to_value(v));
            }
            Value::Map(record)
        }
    }
}

fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Infer schema from the first N lines of a NDJSON file by sampling.
pub fn infer_json_schema(
    path: &Path,
    sample_lines: usize,
) -> Result<Vec<(String, String)>, FileSourceError> {
    let file = std::fs::File::open(path)
        .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;
    let reader = BufReader::new(file);

    let mut columns: IndexMap<String, String> = IndexMap::new();

    for (i, line) in reader.lines().enumerate() {
        if i >= sample_lines {
            break;
        }
        let line = line.map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let json_val: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|e| FileSourceError::Parse(format!("{}:{}: {e}", path.display(), i + 1)))?;

        if let serde_json::Value::Object(map) = &json_val {
            for (k, v) in map {
                columns.entry(k.clone()).or_insert_with(|| infer_type(v));
            }
        }
    }

    Ok(columns.into_iter().collect())
}

/// Infer a type name string from a JSON value.
fn infer_type(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "string".to_string(),
        serde_json::Value::Bool(_) => "bool".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                "int".to_string()
            } else {
                "float".to_string()
            }
        }
        serde_json::Value::String(_) => "string".to_string(),
        serde_json::Value::Array(_) => "array".to_string(),
        serde_json::Value::Object(_) => "map".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_ndjson(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn read_ndjson_basic() {
        let f = write_ndjson(
            r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}
"#,
        );
        let batches = read_ndjson(f.path(), 1000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].records.len(), 2);
        assert_eq!(
            batches[0].records[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(batches[0].records[0].get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn read_ndjson_nested() {
        let f = write_ndjson(r#"{"user":{"name":"Alice"},"tags":["a","b"]}"#);
        let batches = read_ndjson(f.path(), 1000).unwrap();
        let rec = &batches[0].records[0];
        match rec.get("user") {
            Some(Value::Map(m)) => {
                assert_eq!(m.get("name"), Some(&Value::String("Alice".into())));
            }
            other => panic!("expected Map, got {other:?}"),
        }
        match rec.get("tags") {
            Some(Value::Array(a)) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0], Value::String("a".into()));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn infer_json_schema_basic() {
        let f = write_ndjson(
            r#"{"name":"Alice","age":30,"active":true}
{"name":"Bob","age":25,"active":false}
"#,
        );
        let schema = infer_json_schema(f.path(), 10).unwrap();
        assert_eq!(schema.len(), 3);
        assert!(schema.contains(&("name".into(), "string".into())));
        assert!(schema.contains(&("age".into(), "int".into())));
        assert!(schema.contains(&("active".into(), "bool".into())));
    }

    #[test]
    fn read_ndjson_skips_blank_lines() {
        let f = write_ndjson(
            r#"{"a":1}

{"b":2}
"#,
        );
        let batches = read_ndjson(f.path(), 1000).unwrap();
        assert_eq!(batches[0].records.len(), 2);
    }
}
