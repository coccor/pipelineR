//! CSV file writer — writes records to a CSV file.

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use pipeliner_core::record::{Record, Value};

use crate::config::CsvOptions;
use crate::error::FileSinkError;

/// A streaming CSV writer that appends records to a file.
pub struct CsvFileWriter {
    writer: csv::Writer<BufWriter<File>>,
    headers_written: bool,
}

impl CsvFileWriter {
    /// Create a new CSV writer for the given output path.
    pub fn new(path: &Path, options: &CsvOptions) -> Result<Self, FileSinkError> {
        let file = File::create(path)
            .map_err(|e| FileSinkError::Io(format!("{}: {e}", path.display())))?;
        let buf = BufWriter::new(file);

        let writer = csv::WriterBuilder::new()
            .delimiter(options.delimiter as u8)
            .quote(options.quote as u8)
            .has_headers(false) // We write headers manually on first batch.
            .from_writer(buf);

        Ok(Self {
            writer,
            headers_written: false,
        })
    }

    /// Write a batch of records to CSV.
    ///
    /// On the first call, writes a header row derived from the first record's keys.
    pub fn write_records(&mut self, records: &[Record]) -> Result<(), FileSinkError> {
        if records.is_empty() {
            return Ok(());
        }

        // Write header row from the first record's keys.
        if !self.headers_written {
            let headers: Vec<&str> = records[0].keys().map(|k| k.as_str()).collect();
            self.writer
                .write_record(&headers)
                .map_err(|e| FileSinkError::Serialization(e.to_string()))?;
            self.headers_written = true;
        }

        let columns: Vec<String> = records[0].keys().cloned().collect();

        for record in records {
            let row: Vec<String> = columns
                .iter()
                .map(|col| value_to_csv_string(record.get(col).unwrap_or(&Value::Null)))
                .collect();
            self.writer
                .write_record(&row)
                .map_err(|e| FileSinkError::Serialization(e.to_string()))?;
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

/// Convert a `Value` to its CSV string representation.
fn value_to_csv_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Bytes(b) => base64_encode(b),
        Value::Timestamp(t) => t.to_rfc3339(),
        Value::Array(a) => format!("[{} items]", a.len()),
        Value::Map(m) => format!("{{{} fields}}", m.len()),
    }
}

/// Simple base64 encoding for bytes values in CSV output.
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
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
    fn write_csv_basic() {
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

        let mut writer = CsvFileWriter::new(&path, &CsvOptions::default()).unwrap();
        writer.write_records(&records).unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "name,age");
        assert_eq!(lines[1], "Alice,30");
        assert_eq!(lines[2], "Bob,25");
    }

    #[test]
    fn write_csv_multiple_batches() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();

        let batch1 = vec![make_record(vec![("id", Value::Int(1))])];
        let batch2 = vec![make_record(vec![("id", Value::Int(2))])];

        let mut writer = CsvFileWriter::new(&path, &CsvOptions::default()).unwrap();
        writer.write_records(&batch1).unwrap();
        writer.write_records(&batch2).unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 rows
        assert_eq!(lines[0], "id");
        assert_eq!(lines[1], "1");
        assert_eq!(lines[2], "2");
    }

    #[test]
    fn write_csv_null_and_float() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();

        let records = vec![make_record(vec![
            ("val", Value::Float(3.14)),
            ("empty", Value::Null),
        ])];

        let mut writer = CsvFileWriter::new(&path, &CsvOptions::default()).unwrap();
        writer.write_records(&records).unwrap();
        writer.finish().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines[1], "3.14,");
    }
}
