use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::fmt;

/// Default maximum number of records in a batch.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// A flexible value type for representing data flowing through pipelines.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null / missing value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// UTC timestamp.
    Timestamp(DateTime<Utc>),
    /// Array of values.
    Array(Vec<Value>),
    /// Nested record (ordered map).
    Map(Record),
}

/// A single record: an ordered map of field names to values.
pub type Record = IndexMap<String, Value>;

/// A batch of records with metadata.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// The records in this batch.
    pub records: Vec<Record>,
    /// Maximum number of records allowed in this batch.
    pub max_size: usize,
}

impl RecordBatch {
    /// Create a new batch with default max size.
    pub fn new(records: Vec<Record>) -> Self {
        Self {
            records,
            max_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Create a new batch with a custom max size.
    pub fn with_max_size(records: Vec<Record>, max_size: usize) -> Self {
        Self { records, max_size }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::String(s) => write!(f, "\"{s}\""),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::Timestamp(t) => write!(f, "{t}"),
            Value::Array(a) => write!(f, "[{} items]", a.len()),
            Value::Map(m) => write!(f, "{{{} fields}}", m.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_null() {
        assert_eq!(Value::Null, Value::Null);
    }

    #[test]
    fn value_nested_map() {
        let inner: Record = [("city".to_string(), Value::String("Portland".to_string()))]
            .into_iter()
            .collect();
        let val = Value::Map(inner);
        match &val {
            Value::Map(m) => assert_eq!(
                m.get("city"),
                Some(&Value::String("Portland".to_string()))
            ),
            _ => panic!("expected Map"),
        }
    }

    #[test]
    fn value_array() {
        let arr = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        match &arr {
            Value::Array(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected Array"),
        }
    }

    #[test]
    fn record_batch_default_max_size() {
        let batch = RecordBatch::new(vec![]);
        assert_eq!(batch.max_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn record_from_pairs() {
        let rec: Record = [
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]
        .into_iter()
        .collect();
        assert_eq!(rec.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(rec.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn value_display() {
        assert_eq!(format!("{}", Value::Null), "null");
        assert_eq!(format!("{}", Value::Bool(true)), "true");
        assert_eq!(format!("{}", Value::Int(42)), "42");
        assert_eq!(format!("{}", Value::Float(3.14)), "3.14");
        assert_eq!(
            format!("{}", Value::String("hello".to_string())),
            "\"hello\""
        );
    }
}
