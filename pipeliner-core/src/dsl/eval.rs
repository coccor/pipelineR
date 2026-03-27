use crate::dsl::ast::{Expr, Op, PathSegment};
use crate::dsl::error::EvalError;
use crate::dsl::functions;
use crate::record::{Record, Value};

/// Return a human-readable type name for a [`Value`].
pub fn type_name(val: &Value) -> String {
    functions::type_name(val)
}

/// Evaluate an expression against a record, producing a [`Value`].
///
/// Field paths that reference missing fields return [`Value::Null`] (safe navigation).
pub fn eval_expr(expr: &Expr, record: &Record) -> Result<Value, EvalError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::FieldPath(segments) => Ok(eval_field_path(segments, record)),

        Expr::NullCoalesce(left, right) => {
            let lval = eval_expr(left, record)?;
            if lval == Value::Null {
                eval_expr(right, record)
            } else {
                Ok(lval)
            }
        }

        Expr::IfElse(cond, then_expr, else_expr) => {
            let cval = eval_expr(cond, record)?;
            match cval {
                Value::Bool(true) => eval_expr(then_expr, record),
                Value::Bool(false) => eval_expr(else_expr, record),
                other => Err(EvalError::TypeMismatch {
                    expected: "Bool".to_string(),
                    got: type_name(&other),
                }),
            }
        }

        Expr::BinaryOp(left, op, right) => {
            let lval = eval_expr(left, record)?;
            let rval = eval_expr(right, record)?;
            eval_binary_op(&lval, *op, &rval)
        }

        Expr::FunctionCall(name, args) => {
            let mut evaluated = Vec::with_capacity(args.len());
            for arg in args {
                evaluated.push(eval_expr(arg, record)?);
            }
            functions::call_function(name, &evaluated)
        }
    }
}

/// Walk a field path through a record, returning Null for missing/invalid paths.
fn eval_field_path(segments: &[PathSegment], record: &Record) -> Value {
    if segments.is_empty() {
        return Value::Null;
    }

    // Get the first value from the record.
    let first = match &segments[0] {
        PathSegment::Field(name) | PathSegment::QuotedField(name) => match record.get(name) {
            Some(v) => v.clone(),
            None => return Value::Null,
        },
        PathSegment::Index(_) => return Value::Null,
    };

    // Walk remaining segments.
    let mut current = first;
    for seg in &segments[1..] {
        current = match seg {
            PathSegment::Field(name) | PathSegment::QuotedField(name) => match current {
                Value::Map(ref m) => match m.get(name) {
                    Some(v) => v.clone(),
                    None => return Value::Null,
                },
                _ => return Value::Null,
            },
            PathSegment::Index(idx) => match current {
                Value::Array(ref arr) => match arr.get(*idx) {
                    Some(v) => v.clone(),
                    None => return Value::Null,
                },
                _ => return Value::Null,
            },
        };
    }

    current
}

/// Evaluate a binary operation on two values.
fn eval_binary_op(left: &Value, op: Op, right: &Value) -> Result<Value, EvalError> {
    match op {
        Op::Eq => Ok(Value::Bool(values_equal(left, right))),
        Op::Neq => Ok(Value::Bool(!values_equal(left, right))),

        Op::And => {
            let l = as_bool(left, "left operand of &&")?;
            let r = as_bool(right, "right operand of &&")?;
            Ok(Value::Bool(l && r))
        }
        Op::Or => {
            let l = as_bool(left, "left operand of ||")?;
            let r = as_bool(right, "right operand of ||")?;
            Ok(Value::Bool(l || r))
        }

        Op::Gt | Op::Gte | Op::Lt | Op::Lte | Op::Add | Op::Sub | Op::Mul | Op::Div => {
            eval_numeric_op(left, op, right)
        }
    }
}

/// Compare two values for equality. Different types are never equal.
fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        _ => false,
    }
}

/// Extract a bool from a Value or return a type mismatch error.
fn as_bool(val: &Value, context: &str) -> Result<bool, EvalError> {
    match val {
        Value::Bool(b) => Ok(*b),
        other => Err(EvalError::TypeMismatch {
            expected: format!("Bool ({context})"),
            got: type_name(other),
        }),
    }
}

/// Evaluate numeric binary operations with Int/Float promotion.
fn eval_numeric_op(left: &Value, op: Op, right: &Value) -> Result<Value, EvalError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => eval_int_op(*a, op, *b),
        (Value::Float(a), Value::Float(b)) => eval_float_op(*a, op, *b),
        (Value::Int(a), Value::Float(b)) => eval_float_op(*a as f64, op, *b),
        (Value::Float(a), Value::Int(b)) => eval_float_op(*a, op, *b as f64),
        _ => Err(EvalError::TypeMismatch {
            expected: "numeric (Int or Float)".to_string(),
            got: format!("{} and {}", type_name(left), type_name(right)),
        }),
    }
}

/// Integer arithmetic / comparison.
fn eval_int_op(a: i64, op: Op, b: i64) -> Result<Value, EvalError> {
    match op {
        Op::Add => Ok(Value::Int(a + b)),
        Op::Sub => Ok(Value::Int(a - b)),
        Op::Mul => Ok(Value::Int(a * b)),
        Op::Div => {
            if b == 0 {
                return Err(EvalError::Custom("division by zero".to_string()));
            }
            Ok(Value::Int(a / b))
        }
        Op::Gt => Ok(Value::Bool(a > b)),
        Op::Gte => Ok(Value::Bool(a >= b)),
        Op::Lt => Ok(Value::Bool(a < b)),
        Op::Lte => Ok(Value::Bool(a <= b)),
        _ => Err(EvalError::Custom(format!(
            "unexpected op {:?} for ints",
            op
        ))),
    }
}

/// Float arithmetic / comparison.
fn eval_float_op(a: f64, op: Op, b: f64) -> Result<Value, EvalError> {
    match op {
        Op::Add => Ok(Value::Float(a + b)),
        Op::Sub => Ok(Value::Float(a - b)),
        Op::Mul => Ok(Value::Float(a * b)),
        Op::Div => {
            if b == 0.0 {
                return Err(EvalError::Custom("division by zero".to_string()));
            }
            Ok(Value::Float(a / b))
        }
        Op::Gt => Ok(Value::Bool(a > b)),
        Op::Gte => Ok(Value::Bool(a >= b)),
        Op::Lt => Ok(Value::Bool(a < b)),
        Op::Lte => Ok(Value::Bool(a <= b)),
        _ => Err(EvalError::Custom(format!(
            "unexpected op {:?} for floats",
            op
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    fn sample_record() -> Record {
        let mut merchant = IndexMap::new();
        merchant.insert("name".to_string(), Value::String("Acme".to_string()));
        let mut metadata = IndexMap::new();
        metadata.insert("merchant".to_string(), Value::Map(merchant));
        let mut rec = IndexMap::new();
        rec.insert("name".to_string(), Value::String("Alice".to_string()));
        rec.insert("amount".to_string(), Value::Float(42.5));
        rec.insert("count".to_string(), Value::Int(10));
        rec.insert("active".to_string(), Value::Bool(true));
        rec.insert("metadata".to_string(), Value::Map(metadata));
        rec.insert(
            "tags".to_string(),
            Value::Array(vec![
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ]),
        );
        rec
    }

    #[test]
    fn eval_literal() {
        let rec = sample_record();
        let expr = Expr::Literal(Value::Int(42));
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Int(42));
    }

    #[test]
    fn eval_field_path_simple() {
        let rec = sample_record();
        let expr = Expr::FieldPath(vec![PathSegment::Field("name".to_string())]);
        assert_eq!(
            eval_expr(&expr, &rec).unwrap(),
            Value::String("Alice".to_string())
        );
    }

    #[test]
    fn eval_field_path_nested() {
        let rec = sample_record();
        let expr = Expr::FieldPath(vec![
            PathSegment::Field("metadata".to_string()),
            PathSegment::Field("merchant".to_string()),
            PathSegment::Field("name".to_string()),
        ]);
        assert_eq!(
            eval_expr(&expr, &rec).unwrap(),
            Value::String("Acme".to_string())
        );
    }

    #[test]
    fn eval_field_path_missing_returns_null() {
        let rec = sample_record();
        let expr = Expr::FieldPath(vec![PathSegment::Field("nonexistent".to_string())]);
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Null);
    }

    #[test]
    fn eval_field_path_traverse_non_map_returns_null() {
        let rec = sample_record();
        let expr = Expr::FieldPath(vec![
            PathSegment::Field("name".to_string()),
            PathSegment::Field("foo".to_string()),
        ]);
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Null);
    }

    #[test]
    fn eval_array_index() {
        let rec = sample_record();
        let expr = Expr::FieldPath(vec![
            PathSegment::Field("tags".to_string()),
            PathSegment::Index(1),
        ]);
        assert_eq!(
            eval_expr(&expr, &rec).unwrap(),
            Value::String("b".to_string())
        );
    }

    #[test]
    fn eval_null_coalesce_non_null() {
        let rec = sample_record();
        let expr = Expr::NullCoalesce(
            Box::new(Expr::FieldPath(vec![PathSegment::Field(
                "name".to_string(),
            )])),
            Box::new(Expr::Literal(Value::String("default".to_string()))),
        );
        assert_eq!(
            eval_expr(&expr, &rec).unwrap(),
            Value::String("Alice".to_string())
        );
    }

    #[test]
    fn eval_null_coalesce_null() {
        let rec = sample_record();
        let expr = Expr::NullCoalesce(
            Box::new(Expr::FieldPath(vec![PathSegment::Field(
                "missing".to_string(),
            )])),
            Box::new(Expr::Literal(Value::String("default".to_string()))),
        );
        assert_eq!(
            eval_expr(&expr, &rec).unwrap(),
            Value::String("default".to_string())
        );
    }

    #[test]
    fn eval_if_else_true() {
        let rec = sample_record();
        let expr = Expr::IfElse(
            Box::new(Expr::Literal(Value::Bool(true))),
            Box::new(Expr::Literal(Value::Int(1))),
            Box::new(Expr::Literal(Value::Int(0))),
        );
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Int(1));
    }

    #[test]
    fn eval_if_else_false() {
        let rec = sample_record();
        let expr = Expr::IfElse(
            Box::new(Expr::Literal(Value::Bool(false))),
            Box::new(Expr::Literal(Value::Int(1))),
            Box::new(Expr::Literal(Value::Int(0))),
        );
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Int(0));
    }

    #[test]
    fn eval_binary_eq() {
        let rec = sample_record();
        // Int == Int
        let expr = Expr::BinaryOp(
            Box::new(Expr::Literal(Value::Int(1))),
            Op::Eq,
            Box::new(Expr::Literal(Value::Int(1))),
        );
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Bool(true));

        // Int != Int
        let expr = Expr::BinaryOp(
            Box::new(Expr::Literal(Value::Int(1))),
            Op::Neq,
            Box::new(Expr::Literal(Value::Int(2))),
        );
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Bool(true));

        // Different types are never equal
        let expr = Expr::BinaryOp(
            Box::new(Expr::Literal(Value::Int(1))),
            Op::Eq,
            Box::new(Expr::Literal(Value::String("1".to_string()))),
        );
        assert_eq!(eval_expr(&expr, &rec).unwrap(), Value::Bool(false));
    }
}
