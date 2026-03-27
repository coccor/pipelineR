use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};

use crate::dsl::error::EvalError;
use crate::record::Value;

/// Check that a function received the expected number of arguments.
fn check_arity(name: &str, args: &[Value], expected: usize) -> Result<(), EvalError> {
    if args.len() != expected {
        return Err(EvalError::ArityMismatch {
            function: name.to_string(),
            expected,
            got: args.len(),
        });
    }
    Ok(())
}

/// Dispatch a built-in function call by name.
///
/// Returns [`EvalError::UnknownFunction`] if the function name is not recognised.
pub fn call_function(name: &str, args: &[Value]) -> Result<Value, EvalError> {
    match name {
        "to_string" => fn_to_string(args),
        "to_int" => fn_to_int(args),
        "to_float" => fn_to_float(args),
        "to_bool" => fn_to_bool(args),
        "to_timestamp" | "parse_timestamp" => fn_parse_timestamp(args, name),
        "is_null" => fn_is_null(args),
        "concat" => fn_concat(args),
        "coalesce" => fn_coalesce(args),
        _ => Err(EvalError::UnknownFunction(name.to_string())),
    }
}

/// Convert any value to its string representation.
fn fn_to_string(args: &[Value]) -> Result<Value, EvalError> {
    check_arity("to_string", args, 1)?;
    let s = match &args[0] {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Bytes(b) => format!("<{} bytes>", b.len()),
        Value::Timestamp(t) => t.to_string(),
        Value::Array(a) => format!("[{} items]", a.len()),
        Value::Map(m) => format!("{{{} fields}}", m.len()),
    };
    Ok(Value::String(s))
}

/// Convert a value to an integer.
fn fn_to_int(args: &[Value]) -> Result<Value, EvalError> {
    check_arity("to_int", args, 1)?;
    match &args[0] {
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Int(*f as i64)),
        Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
        Value::String(s) => {
            let i: i64 = s
                .parse()
                .map_err(|_| EvalError::ParseError(format!("cannot parse '{}' as integer", s)))?;
            Ok(Value::Int(i))
        }
        other => Err(EvalError::TypeMismatch {
            expected: "String, Float, or Bool".to_string(),
            got: type_name(other),
        }),
    }
}

/// Convert a value to a float.
fn fn_to_float(args: &[Value]) -> Result<Value, EvalError> {
    check_arity("to_float", args, 1)?;
    match &args[0] {
        Value::Float(f) => Ok(Value::Float(*f)),
        Value::Int(i) => Ok(Value::Float(*i as f64)),
        Value::String(s) => {
            let f: f64 = s
                .parse()
                .map_err(|_| EvalError::ParseError(format!("cannot parse '{}' as float", s)))?;
            Ok(Value::Float(f))
        }
        other => Err(EvalError::TypeMismatch {
            expected: "String or Int".to_string(),
            got: type_name(other),
        }),
    }
}

/// Convert a value to a boolean.
fn fn_to_bool(args: &[Value]) -> Result<Value, EvalError> {
    check_arity("to_bool", args, 1)?;
    match &args[0] {
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::Int(i) => Ok(Value::Bool(*i != 0)),
        Value::String(s) => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(Value::Bool(true)),
            "false" | "0" | "no" => Ok(Value::Bool(false)),
            _ => Err(EvalError::ParseError(format!(
                "cannot parse '{}' as bool",
                s
            ))),
        },
        other => Err(EvalError::TypeMismatch {
            expected: "String or Int".to_string(),
            got: type_name(other),
        }),
    }
}

/// Parse a string into a UTC timestamp using a chrono format string.
fn fn_parse_timestamp(args: &[Value], name: &str) -> Result<Value, EvalError> {
    check_arity(name, args, 2)?;
    let s = match &args[0] {
        Value::String(s) => s,
        other => {
            return Err(EvalError::TypeMismatch {
                expected: "String".to_string(),
                got: type_name(other),
            })
        }
    };
    let fmt = match &args[1] {
        Value::String(f) => f,
        other => {
            return Err(EvalError::TypeMismatch {
                expected: "String".to_string(),
                got: type_name(other),
            })
        }
    };

    // Try NaiveDateTime first, then NaiveDate (midnight UTC).
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
        let dt = Utc.from_utc_datetime(&ndt);
        return Ok(Value::Timestamp(dt));
    }
    if let Ok(nd) = NaiveDate::parse_from_str(s, fmt) {
        if let Some(ndt) = nd.and_hms_opt(0, 0, 0) {
            let dt = Utc.from_utc_datetime(&ndt);
            return Ok(Value::Timestamp(dt));
        }
    }
    Err(EvalError::ParseError(format!(
        "cannot parse '{}' with format '{}'",
        s, fmt
    )))
}

/// Check whether a value is null.
fn fn_is_null(args: &[Value]) -> Result<Value, EvalError> {
    check_arity("is_null", args, 1)?;
    Ok(Value::Bool(args[0] == Value::Null))
}

/// Concatenate string values.
fn fn_concat(args: &[Value]) -> Result<Value, EvalError> {
    let mut result = String::new();
    for (i, arg) in args.iter().enumerate() {
        match arg {
            Value::String(s) => result.push_str(s),
            other => {
                return Err(EvalError::TypeMismatch {
                    expected: format!("String (argument {})", i),
                    got: type_name(other),
                })
            }
        }
    }
    Ok(Value::String(result))
}

/// Return the first non-null argument.
fn fn_coalesce(args: &[Value]) -> Result<Value, EvalError> {
    for arg in args {
        if *arg != Value::Null {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

/// Return a human-readable type name for a [`Value`].
pub fn type_name(val: &Value) -> String {
    match val {
        Value::Null => "Null".to_string(),
        Value::Bool(_) => "Bool".to_string(),
        Value::Int(_) => "Int".to_string(),
        Value::Float(_) => "Float".to_string(),
        Value::String(_) => "String".to_string(),
        Value::Bytes(_) => "Bytes".to_string(),
        Value::Timestamp(_) => "Timestamp".to_string(),
        Value::Array(_) => "Array".to_string(),
        Value::Map(_) => "Map".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_from_int() {
        let result = call_function("to_string", &[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::String("42".to_string()));
    }

    #[test]
    fn to_string_from_float() {
        let result = call_function("to_string", &[Value::Float(3.14)]).unwrap();
        assert_eq!(result, Value::String("3.14".to_string()));
    }

    #[test]
    fn to_string_from_bool() {
        let result = call_function("to_string", &[Value::Bool(true)]).unwrap();
        assert_eq!(result, Value::String("true".to_string()));
    }

    #[test]
    fn to_string_from_null() {
        let result = call_function("to_string", &[Value::Null]).unwrap();
        assert_eq!(result, Value::String("null".to_string()));
    }

    #[test]
    fn to_int_from_string() {
        let result = call_function("to_int", &[Value::String("123".to_string())]).unwrap();
        assert_eq!(result, Value::Int(123));
    }

    #[test]
    fn to_int_from_float() {
        let result = call_function("to_int", &[Value::Float(3.9)]).unwrap();
        assert_eq!(result, Value::Int(3));
    }

    #[test]
    fn to_int_bad_string() {
        let result = call_function("to_int", &[Value::String("abc".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn to_float_from_string() {
        let result = call_function("to_float", &[Value::String("3.14".to_string())]).unwrap();
        assert_eq!(result, Value::Float(3.14));
    }

    #[test]
    fn to_float_from_int() {
        let result = call_function("to_float", &[Value::Int(42)]).unwrap();
        assert_eq!(result, Value::Float(42.0));
    }

    #[test]
    fn to_bool_from_string() {
        assert_eq!(
            call_function("to_bool", &[Value::String("true".to_string())]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            call_function("to_bool", &[Value::String("false".to_string())]).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            call_function("to_bool", &[Value::String("yes".to_string())]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            call_function("to_bool", &[Value::String("no".to_string())]).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn to_bool_from_int() {
        assert_eq!(
            call_function("to_bool", &[Value::Int(0)]).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            call_function("to_bool", &[Value::Int(1)]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            call_function("to_bool", &[Value::Int(-5)]).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn is_null_true() {
        assert_eq!(
            call_function("is_null", &[Value::Null]).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn is_null_false() {
        assert_eq!(
            call_function("is_null", &[Value::Int(1)]).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn concat_strings() {
        let result = call_function(
            "concat",
            &[
                Value::String("hello".to_string()),
                Value::String(" ".to_string()),
                Value::String("world".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn coalesce_returns_first_non_null() {
        let result = call_function(
            "coalesce",
            &[
                Value::Null,
                Value::String("found".to_string()),
                Value::Int(1),
            ],
        )
        .unwrap();
        assert_eq!(result, Value::String("found".to_string()));
    }

    #[test]
    fn coalesce_all_null() {
        let result = call_function("coalesce", &[Value::Null, Value::Null]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn parse_timestamp_valid() {
        let result = call_function(
            "parse_timestamp",
            &[
                Value::String("2026-03-25".to_string()),
                Value::String("%Y-%m-%d".to_string()),
            ],
        )
        .unwrap();
        match result {
            Value::Timestamp(dt) => {
                assert_eq!(dt.format("%Y-%m-%d").to_string(), "2026-03-25");
            }
            other => panic!("expected Timestamp, got {:?}", other),
        }
    }

    #[test]
    fn parse_timestamp_invalid() {
        let result = call_function(
            "parse_timestamp",
            &[
                Value::String("not-a-date".to_string()),
                Value::String("%Y-%m-%d".to_string()),
            ],
        );
        assert!(result.is_err());
    }

    #[test]
    fn to_timestamp_alias() {
        let result = call_function(
            "to_timestamp",
            &[
                Value::String("2026-03-25".to_string()),
                Value::String("%Y-%m-%d".to_string()),
            ],
        )
        .unwrap();
        assert!(matches!(result, Value::Timestamp(_)));
    }

    #[test]
    fn unknown_function() {
        let result = call_function("does_not_exist", &[]);
        assert!(result.is_err());
        match result.unwrap_err() {
            EvalError::UnknownFunction(name) => assert_eq!(name, "does_not_exist"),
            other => panic!("expected UnknownFunction, got {:?}", other),
        }
    }
}
