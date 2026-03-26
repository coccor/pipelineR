use crate::dsl::ast::{Expr, PathSegment, TransformStep};
use crate::dsl::error::{EvalError, TransformError};
use crate::dsl::eval::eval_expr;
use crate::record::{Record, Value};

/// Extract the top-level field name from a [`Expr::FieldPath`].
///
/// Returns an error if the expression is not a field path or has no segments.
fn extract_top_level_field_name(expr: &Expr) -> Result<String, EvalError> {
    match expr {
        Expr::FieldPath(segments) => match segments.first() {
            Some(PathSegment::Field(name) | PathSegment::QuotedField(name)) => {
                Ok(name.clone())
            }
            Some(PathSegment::Index(_)) => Err(EvalError::Custom(
                "expected field name, got array index".to_string(),
            )),
            None => Err(EvalError::Custom("empty field path".to_string())),
        },
        _ => Err(EvalError::TypeMismatch {
            expected: "FieldPath".to_string(),
            got: "expression".to_string(),
        }),
    }
}

/// Execute a single transform step on a batch of records.
///
/// Mutates the record vector in place. Returns a [`TransformError`] if a step
/// encounters an unrecoverable error.
pub fn execute_step(
    step: &TransformStep,
    records: &mut Vec<Record>,
) -> Result<(), TransformError> {
    match step.function.as_str() {
        "set" => step_set(&step.args, records),
        "rename" => step_rename(&step.args, records),
        "remove" => step_remove(&step.args, records),
        "where" => step_where(&step.args, records),
        other => Err(TransformError::StepFailed {
            step: other.to_string(),
            message: format!("unknown step function '{}'", other),
        }),
    }
}

/// `set(field_path, expr)` — evaluate expr for each record, write to top-level field.
fn step_set(args: &[Expr], records: &mut [Record]) -> Result<(), TransformError> {
    if args.len() != 2 {
        return Err(TransformError::StepFailed {
            step: "set".to_string(),
            message: format!("expected 2 arguments, got {}", args.len()),
        });
    }
    let field_name =
        extract_top_level_field_name(&args[0]).map_err(|source| TransformError::EvalFailed {
            index: 0,
            source,
        })?;
    for (i, record) in records.iter_mut().enumerate() {
        let value =
            eval_expr(&args[1], record).map_err(|source| TransformError::EvalFailed {
                index: i,
                source,
            })?;
        record.insert(field_name.clone(), value);
    }
    Ok(())
}

/// `rename(old_path, new_path)` — move value from old to new field name (top-level).
fn step_rename(args: &[Expr], records: &mut [Record]) -> Result<(), TransformError> {
    if args.len() != 2 {
        return Err(TransformError::StepFailed {
            step: "rename".to_string(),
            message: format!("expected 2 arguments, got {}", args.len()),
        });
    }
    let old_name =
        extract_top_level_field_name(&args[0]).map_err(|source| TransformError::EvalFailed {
            index: 0,
            source,
        })?;
    let new_name =
        extract_top_level_field_name(&args[1]).map_err(|source| TransformError::EvalFailed {
            index: 0,
            source,
        })?;
    for record in records.iter_mut() {
        if let Some(value) = record.swap_remove(&old_name) {
            record.insert(new_name.clone(), value);
        }
    }
    Ok(())
}

/// `remove(field_path)` — delete top-level field.
fn step_remove(args: &[Expr], records: &mut [Record]) -> Result<(), TransformError> {
    if args.len() != 1 {
        return Err(TransformError::StepFailed {
            step: "remove".to_string(),
            message: format!("expected 1 argument, got {}", args.len()),
        });
    }
    let field_name =
        extract_top_level_field_name(&args[0]).map_err(|source| TransformError::EvalFailed {
            index: 0,
            source,
        })?;
    for record in records.iter_mut() {
        record.swap_remove(&field_name);
    }
    Ok(())
}

/// `where(predicate)` — keep only records where predicate evaluates to `Bool(true)`.
fn step_where(args: &[Expr], records: &mut Vec<Record>) -> Result<(), TransformError> {
    if args.len() != 1 {
        return Err(TransformError::StepFailed {
            step: "where".to_string(),
            message: format!("expected 1 argument, got {}", args.len()),
        });
    }
    let predicate = &args[0];
    let mut kept = Vec::new();
    for (i, record) in records.iter().enumerate() {
        let val =
            eval_expr(predicate, record).map_err(|source| TransformError::EvalFailed {
                index: i,
                source,
            })?;
        if val == Value::Bool(true) {
            kept.push(record.clone());
        }
    }
    *records = kept;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::parser::parse_step;
    use indexmap::IndexMap;

    fn make_record(pairs: Vec<(&str, Value)>) -> Record {
        let mut rec = IndexMap::new();
        for (k, v) in pairs {
            rec.insert(k.to_string(), v);
        }
        rec
    }

    #[test]
    fn step_set_simple() {
        let step = parse_step("set(.amount, to_float(.amount))").unwrap();
        let mut records = vec![make_record(vec![(
            "amount",
            Value::String("42.5".to_string()),
        )])];
        execute_step(&step, &mut records).unwrap();
        assert_eq!(records[0].get("amount"), Some(&Value::Float(42.5)));
    }

    #[test]
    fn step_set_nested_path() {
        let step =
            parse_step("set(.merchant_name, .metadata.merchant.name ?? \"Unknown\")").unwrap();
        let mut merchant = IndexMap::new();
        merchant.insert("name".to_string(), Value::String("Acme".to_string()));
        let mut metadata = IndexMap::new();
        metadata.insert("merchant".to_string(), Value::Map(merchant));
        let mut records = vec![make_record(vec![("metadata", Value::Map(metadata))])];
        execute_step(&step, &mut records).unwrap();
        assert_eq!(
            records[0].get("merchant_name"),
            Some(&Value::String("Acme".to_string()))
        );
    }

    #[test]
    fn step_rename() {
        let step = parse_step("rename(.old_name, .new_name)").unwrap();
        let mut records = vec![make_record(vec![(
            "old_name",
            Value::String("hello".to_string()),
        )])];
        execute_step(&step, &mut records).unwrap();
        assert_eq!(records[0].get("old_name"), None);
        assert_eq!(
            records[0].get("new_name"),
            Some(&Value::String("hello".to_string()))
        );
    }

    #[test]
    fn step_remove() {
        let step = parse_step("remove(.secret)").unwrap();
        let mut records = vec![make_record(vec![
            ("name", Value::String("Alice".to_string())),
            ("secret", Value::String("password".to_string())),
        ])];
        execute_step(&step, &mut records).unwrap();
        assert_eq!(records[0].get("secret"), None);
        assert_eq!(
            records[0].get("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn step_where_filters() {
        let step = parse_step("where(.amount > 0.0)").unwrap();
        let mut records = vec![
            make_record(vec![("amount", Value::Float(10.0))]),
            make_record(vec![("amount", Value::Float(-5.0))]),
            make_record(vec![("amount", Value::Float(3.0))]),
        ];
        execute_step(&step, &mut records).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get("amount"), Some(&Value::Float(10.0)));
        assert_eq!(records[1].get("amount"), Some(&Value::Float(3.0)));
    }

    #[test]
    fn step_where_all_filtered() {
        let step = parse_step("where(.amount > 100.0)").unwrap();
        let mut records = vec![
            make_record(vec![("amount", Value::Float(10.0))]),
            make_record(vec![("amount", Value::Float(5.0))]),
        ];
        execute_step(&step, &mut records).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn multiple_steps_pipeline() {
        // rename .old_amount -> .amount, set .amount_f = to_float(.amount),
        // where(.amount_f > 0.0), remove(.amount)
        let steps = vec![
            parse_step("rename(.old_amount, .amount)").unwrap(),
            parse_step("set(.amount_f, to_float(.amount))").unwrap(),
            parse_step("where(.amount_f > 0.0)").unwrap(),
            parse_step("remove(.amount)").unwrap(),
        ];

        let mut records = vec![
            make_record(vec![("old_amount", Value::String("10.5".to_string()))]),
            make_record(vec![("old_amount", Value::String("-3.0".to_string()))]),
            make_record(vec![("old_amount", Value::String("7.0".to_string()))]),
        ];

        for step in &steps {
            execute_step(step, &mut records).unwrap();
        }

        assert_eq!(records.len(), 2);
        // old_amount and amount should be gone
        assert_eq!(records[0].get("old_amount"), None);
        assert_eq!(records[0].get("amount"), None);
        // amount_f should be present
        assert_eq!(records[0].get("amount_f"), Some(&Value::Float(10.5)));
        assert_eq!(records[1].get("amount_f"), Some(&Value::Float(7.0)));
    }
}
