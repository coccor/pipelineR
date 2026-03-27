use chrono::Utc;

use crate::dsl::ast::{Expr, PathSegment, TransformStep};
use crate::dsl::error::{EvalError, TransformError};
use crate::dsl::eval::eval_expr;
use crate::record::{Record, Value};

/// Result of executing a step with dead-letter support.
#[derive(Debug)]
pub struct StepExecutionResult {
    /// Records that errored and should be sent to the dead-letter sink.
    pub dead_letters: Vec<DeadLetterRecord>,
}

/// A record that failed a transform step, with error context.
#[derive(Debug, Clone)]
pub struct DeadLetterRecord {
    /// The original record that failed.
    pub original: Record,
    /// Name of the transform step that failed.
    pub step_name: String,
    /// Error message.
    pub error_message: String,
    /// Timestamp when the error occurred (ISO 8601 UTC).
    pub error_timestamp: String,
}

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

// ---------------------------------------------------------------------------
// Dead-letter-aware step execution
// ---------------------------------------------------------------------------

/// Execute a single transform step with dead-letter support.
///
/// Per-record errors in `set` and `where` steps are caught: the failed record
/// is removed from the batch and returned as a [`DeadLetterRecord`] instead of
/// aborting the entire batch. Steps that cannot fail per-record (`rename`,
/// `remove`) delegate to their normal implementations.
pub fn execute_step_with_dead_letter(
    step: &TransformStep,
    records: &mut Vec<Record>,
) -> Result<StepExecutionResult, TransformError> {
    match step.function.as_str() {
        "set" => step_set_with_dl(&step.args, records, &step.function),
        "where" => step_where_with_dl(&step.args, records, &step.function),
        "rename" => {
            step_rename(&step.args, records)?;
            Ok(StepExecutionResult {
                dead_letters: vec![],
            })
        }
        "remove" => {
            step_remove(&step.args, records)?;
            Ok(StepExecutionResult {
                dead_letters: vec![],
            })
        }
        other => Err(TransformError::StepFailed {
            step: other.to_string(),
            message: format!("unknown step function '{other}'"),
        }),
    }
}

/// Dead-letter-aware version of `set(field, expr)`.
///
/// Extracts the target field name (batch-level — still errors normally on bad
/// arguments). For each record, tries `eval_expr`; on error the record is moved
/// to the dead-letter list instead of aborting.
fn step_set_with_dl(
    args: &[Expr],
    records: &mut Vec<Record>,
    step_name: &str,
) -> Result<StepExecutionResult, TransformError> {
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

    let mut dead_letters = Vec::new();
    let mut kept = Vec::new();
    let timestamp = Utc::now().to_rfc3339();

    for record in records.drain(..) {
        match eval_expr(&args[1], &record) {
            Ok(value) => {
                let mut rec = record;
                rec.insert(field_name.clone(), value);
                kept.push(rec);
            }
            Err(e) => {
                dead_letters.push(DeadLetterRecord {
                    original: record,
                    step_name: step_name.to_string(),
                    error_message: e.to_string(),
                    error_timestamp: timestamp.clone(),
                });
            }
        }
    }

    *records = kept;
    Ok(StepExecutionResult { dead_letters })
}

/// Dead-letter-aware version of `where(predicate)`.
///
/// For each record, tries `eval_expr`; on error the record is moved to the
/// dead-letter list. Records where the predicate evaluates to `true` are kept.
fn step_where_with_dl(
    args: &[Expr],
    records: &mut Vec<Record>,
    step_name: &str,
) -> Result<StepExecutionResult, TransformError> {
    if args.len() != 1 {
        return Err(TransformError::StepFailed {
            step: "where".to_string(),
            message: format!("expected 1 argument, got {}", args.len()),
        });
    }
    let predicate = &args[0];
    let mut dead_letters = Vec::new();
    let mut kept = Vec::new();
    let timestamp = Utc::now().to_rfc3339();

    for record in records.drain(..) {
        match eval_expr(predicate, &record) {
            Ok(val) => {
                if val == Value::Bool(true) {
                    kept.push(record);
                }
                // Records where predicate is false are simply dropped (not dead-lettered).
            }
            Err(e) => {
                dead_letters.push(DeadLetterRecord {
                    original: record,
                    step_name: step_name.to_string(),
                    error_message: e.to_string(),
                    error_timestamp: timestamp.clone(),
                });
            }
        }
    }

    *records = kept;
    Ok(StepExecutionResult { dead_letters })
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

    // --- Dead-letter-aware tests ---

    #[test]
    fn dl_set_routes_bad_records() {
        // to_float on a non-numeric string should dead-letter that record.
        let step = parse_step("set(.amount, to_float(.amount))").unwrap();
        let mut records = vec![
            make_record(vec![("amount", Value::String("42.5".to_string()))]),
            make_record(vec![("amount", Value::String("not_a_number".to_string()))]),
            make_record(vec![("amount", Value::String("7.0".to_string()))]),
        ];
        let result = execute_step_with_dead_letter(&step, &mut records).unwrap();
        // Good records stay
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get("amount"), Some(&Value::Float(42.5)));
        assert_eq!(records[1].get("amount"), Some(&Value::Float(7.0)));
        // Bad record is dead-lettered
        assert_eq!(result.dead_letters.len(), 1);
        assert_eq!(result.dead_letters[0].step_name, "set");
        assert_eq!(
            result.dead_letters[0].original.get("amount"),
            Some(&Value::String("not_a_number".to_string()))
        );
    }

    #[test]
    fn dl_where_routes_bad_records() {
        // Comparing a string to a float should error.
        let step = parse_step("where(.amount > 0.0)").unwrap();
        let mut records = vec![
            make_record(vec![("amount", Value::Float(10.0))]),
            make_record(vec![("amount", Value::String("bad".to_string()))]),
            make_record(vec![("amount", Value::Float(3.0))]),
        ];
        let result = execute_step_with_dead_letter(&step, &mut records).unwrap();
        // Good records where predicate is true stay
        assert_eq!(records.len(), 2);
        // Bad record is dead-lettered
        assert_eq!(result.dead_letters.len(), 1);
        assert_eq!(result.dead_letters[0].step_name, "where");
    }

    #[test]
    fn dl_rename_no_dead_letters() {
        let step = parse_step("rename(.old, .new)").unwrap();
        let mut records = vec![make_record(vec![(
            "old",
            Value::String("hello".to_string()),
        )])];
        let result = execute_step_with_dead_letter(&step, &mut records).unwrap();
        assert!(result.dead_letters.is_empty());
        assert_eq!(
            records[0].get("new"),
            Some(&Value::String("hello".to_string()))
        );
    }

    #[test]
    fn dl_set_all_good() {
        let step = parse_step("set(.amount, to_float(.amount))").unwrap();
        let mut records = vec![
            make_record(vec![("amount", Value::String("1.0".to_string()))]),
            make_record(vec![("amount", Value::String("2.0".to_string()))]),
        ];
        let result = execute_step_with_dead_letter(&step, &mut records).unwrap();
        assert_eq!(records.len(), 2);
        assert!(result.dead_letters.is_empty());
    }
}
