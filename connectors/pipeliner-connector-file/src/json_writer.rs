//! Newline-delimited JSON writer — preserves nested structure.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use pipeliner_core::record::{Record, Value};

use crate::error::FileSinkError;

/// A streaming NDJSON writer that appends records to a file.
pub struct JsonFileWriter {
    writer: BufWriter<File>,
}

impl JsonFileWriter {
    /// Create a new NDJSON writer for the given output path.
    pub fn new(path: &Path) -> Result<Self, FileSinkError> {
        let file = File::create(path)
            .map_err(|e| FileSinkError::Io(format!("{}: {e}", path.display())))?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Write a batch of records as newline-delimited JSON.
    pub fn write_records(&mut self, records: &[Record]) -> Result<(), FileSinkError> {
        for record in records {
            let json_val = record_to_json(record);
            serde_json::to_writer(&mut self.writer, &json_val)
                .map_err(|e| FileSinkError::Serialization(e.to_string()))?;
            self.writer
                .write_all(b"\n")
                .map_err(|e| FileSinkError::Io(e.to_string()))?;
        }
        Ok(())
    }

    /// Flush the writer and finalize the file.
    pub fn finish(mut self) -> Result<(), FileSinkError> {
        self.writer
            .flush()
            .map_err(|e| FileSinkError::Io(e.to_string()))
    }
}

/// Convert a `Record` to a `serde_json::Value` for serialization.
fn record_to_json(record: &Record) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = record
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    serde_json::Value::Object(map)
}

/// Convert a core `Value` to `serde_json::Value`.
fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Float(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(format!("<{} bytes>", b.len())),
        Value::Timestamp(t) => serde_json::Value::String(t.to_rfc3339()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Map(m) => record_to_json(m),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use tempfile::NamedTempFile;

    fn make_record(pairs: Vec<(&str, Value)>) -> Record {
        let mut rec = IndexMap::new();
        for (k, v) in pairs {
            rec.insert(k.to_string(), v);
        }
        rec
    }

    #[test]
    fn write_ndjson_basic() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();

        let records = vec![
            make_record(vec![
                ("name", Value::String("Alice".into())),
                ("age", Value::Int(30)),
            ]),
            make_record(vec![
                ("name", Value::String("Bob".into())),
                ("age", Value::Int(25)),
            ]),
        ];

        let mut writer = JsonFileWriter::new(&path).unwrap();
        writer.write_records(&records).unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 2);

        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["name"], "Alice");
        assert_eq!(parsed["age"], 30);
    }

    #[test]
    fn write_ndjson_nested() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();

        let mut inner = IndexMap::new();
        inner.insert("city".to_string(), Value::String("Portland".into()));
        let records = vec![make_record(vec![
            ("name", Value::String("Alice".into())),
            ("address", Value::Map(inner)),
            ("tags", Value::Array(vec![Value::String("a".into()), Value::String("b".into())])),
        ])];

        let mut writer = JsonFileWriter::new(&path).unwrap();
        writer.write_records(&records).unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(parsed["address"]["city"], "Portland");
        assert_eq!(parsed["tags"][0], "a");
        assert_eq!(parsed["tags"][1], "b");
    }

    #[test]
    fn write_ndjson_nulls_and_bools() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();

        let records = vec![make_record(vec![
            ("active", Value::Bool(true)),
            ("deleted", Value::Null),
            ("score", Value::Float(3.14)),
        ])];

        let mut writer = JsonFileWriter::new(&path).unwrap();
        writer.write_records(&records).unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(parsed["active"], true);
        assert!(parsed["deleted"].is_null());
        assert_eq!(parsed["score"], 3.14);
    }
}
