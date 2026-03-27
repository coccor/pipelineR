//! CSV file reader — emits records with all values as strings.

use std::path::Path;

use indexmap::IndexMap;
use pipeliner_core::record::{Record, RecordBatch, Value};

use crate::config::CsvOptions;
use crate::error::FileSourceError;

/// Read a CSV file and return all records in batches.
///
/// All values are emitted as `Value::String`; type coercion is handled by
/// DSL transform steps downstream.
pub fn read_csv(
    path: &Path,
    options: &CsvOptions,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, FileSourceError> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(options.delimiter as u8)
        .quote(options.quote as u8)
        .has_headers(options.has_header)
        .from_path(path)
        .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;

    let headers: Vec<String> = if options.has_header {
        reader
            .headers()
            .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        // Generate column_0, column_1, ... for headerless files.
        let first = reader.records().next();
        match first {
            Some(Ok(ref row)) => (0..row.len()).map(|i| format!("column_{i}")).collect(),
            Some(Err(e)) => return Err(FileSourceError::Io(format!("{}: {e}", path.display()))),
            None => return Ok(vec![]),
        }
    };

    let mut batches = Vec::new();
    let mut current_batch: Vec<Record> = Vec::with_capacity(batch_size);

    for result in reader.records() {
        let row = result.map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;
        let mut record: Record = IndexMap::new();
        for (i, field) in row.iter().enumerate() {
            let col_name = headers
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("column_{i}"));
            record.insert(col_name, Value::String(field.to_string()));
        }
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

/// Infer schema from CSV headers — all columns are typed as `string`.
pub fn infer_csv_schema(
    path: &Path,
    options: &CsvOptions,
) -> Result<Vec<(String, String)>, FileSourceError> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(options.delimiter as u8)
        .quote(options.quote as u8)
        .has_headers(options.has_header)
        .from_path(path)
        .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?;

    let headers: Vec<String> = if options.has_header {
        reader
            .headers()
            .map_err(|e| FileSourceError::Io(format!("{}: {e}", path.display())))?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        // Peek at first row to count columns.
        let first = reader.records().next();
        match first {
            Some(Ok(ref row)) => (0..row.len()).map(|i| format!("column_{i}")).collect(),
            Some(Err(e)) => return Err(FileSourceError::Io(format!("{}: {e}", path.display()))),
            None => return Ok(vec![]),
        }
    };

    Ok(headers
        .into_iter()
        .map(|h| (h, "string".to_string()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_csv(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn read_csv_basic() {
        let f = write_csv("name,age\nAlice,30\nBob,25\n");
        let batches = read_csv(f.path(), &CsvOptions::default(), 1000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].records.len(), 2);
        assert_eq!(
            batches[0].records[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            batches[0].records[0].get("age"),
            Some(&Value::String("30".into()))
        );
    }

    #[test]
    fn read_csv_batching() {
        let f = write_csv("id\n1\n2\n3\n4\n5\n");
        let batches = read_csv(f.path(), &CsvOptions::default(), 2).unwrap();
        assert_eq!(batches.len(), 3); // 2 + 2 + 1
        assert_eq!(batches[0].records.len(), 2);
        assert_eq!(batches[2].records.len(), 1);
    }

    #[test]
    fn infer_csv_schema_basic() {
        let f = write_csv("name,age,active\nAlice,30,true\n");
        let schema = infer_csv_schema(f.path(), &CsvOptions::default()).unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0], ("name".into(), "string".into()));
        assert_eq!(schema[1], ("age".into(), "string".into()));
        assert_eq!(schema[2], ("active".into(), "string".into()));
    }

    #[test]
    fn read_csv_custom_delimiter() {
        let f = write_csv("name\tage\nAlice\t30\n");
        let opts = CsvOptions {
            delimiter: '\t',
            ..CsvOptions::default()
        };
        let batches = read_csv(f.path(), &opts, 1000).unwrap();
        assert_eq!(batches[0].records.len(), 1);
        assert_eq!(
            batches[0].records[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
    }
}
