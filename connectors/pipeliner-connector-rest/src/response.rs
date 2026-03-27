//! Response parsing utilities — JSON path extraction and record conversion.

use pipeliner_core::record::{Record, Value};
use serde_json::Value as JsonValue;

/// Traverse a JSON value using dot-notation path (e.g., `data.items`).
///
/// Returns `None` if any segment of the path does not exist.
pub fn extract_path<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Convert a JSON value to a pipelineR `Record`.
///
/// Only JSON objects are converted to records. Non-object values produce an empty record.
pub fn json_to_record(value: &JsonValue) -> Record {
    let mut record = Record::new();
    if let JsonValue::Object(map) = value {
        for (key, val) in map {
            record.insert(key.clone(), json_to_value(val));
        }
    }
    record
}

/// Convert a JSON value to a pipelineR `Value`.
pub fn json_to_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::String(n.to_string())
            }
        }
        JsonValue::String(s) => Value::String(s.clone()),
        JsonValue::Array(arr) => {
            let values: Vec<Value> = arr.iter().map(json_to_value).collect();
            Value::Array(values)
        }
        JsonValue::Object(_) => Value::Map(json_to_record(value)),
    }
}

/// Infer a pipelineR data type string from a JSON value.
///
/// Returns a type name compatible with `ColumnSchema.data_type`.
pub fn infer_type(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "string",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(n) => {
            if n.is_i64() {
                "int"
            } else {
                "float"
            }
        }
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "map",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_path_simple() {
        let json: JsonValue = serde_json::json!({"data": {"items": [1, 2, 3]}});
        let result = extract_path(&json, "data.items");
        assert_eq!(result, Some(&serde_json::json!([1, 2, 3])));
    }

    #[test]
    fn extract_path_empty() {
        let json: JsonValue = serde_json::json!({"a": 1});
        let result = extract_path(&json, "");
        assert_eq!(result, Some(&json));
    }

    #[test]
    fn extract_path_missing() {
        let json: JsonValue = serde_json::json!({"a": 1});
        let result = extract_path(&json, "b.c");
        assert_eq!(result, None);
    }

    #[test]
    fn json_to_record_object() {
        let json: JsonValue = serde_json::json!({"name": "Alice", "age": 30, "active": true});
        let record = json_to_record(&json);
        assert_eq!(
            record.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(record.get("age"), Some(&Value::Int(30)));
        assert_eq!(record.get("active"), Some(&Value::Bool(true)));
    }

    #[test]
    fn json_to_record_nested() {
        let json: JsonValue = serde_json::json!({"user": {"id": 1}});
        let record = json_to_record(&json);
        match record.get("user") {
            Some(Value::Map(inner)) => {
                assert_eq!(inner.get("id"), Some(&Value::Int(1)));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn json_to_value_null() {
        assert_eq!(json_to_value(&JsonValue::Null), Value::Null);
    }

    #[test]
    fn json_to_value_array() {
        let json: JsonValue = serde_json::json!([1, "two", null]);
        match json_to_value(&json) {
            Value::Array(arr) => assert_eq!(arr.len(), 3),
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
