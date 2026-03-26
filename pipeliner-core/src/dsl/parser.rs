use winnow::combinator::{alt, opt, preceded, separated};
use winnow::error::ContextError;
use winnow::prelude::*;
use winnow::token::{any, take_while};
use winnow::Result as WResult;

use crate::dsl::ast::{Expr, Op, PathSegment};
use crate::dsl::error::EvalError;
use crate::record::Value;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse an expression string into an [`Expr`] AST node.
///
/// Returns an [`EvalError::ParseError`] if the input is not a valid expression.
pub fn parse_expr(input: &str) -> Result<Expr, EvalError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(EvalError::ParseError("empty input".to_string()));
    }
    let mut s = input;
    let expr = expr_top
        .parse_next(&mut s)
        .map_err(|e| EvalError::ParseError(format!("{e}")))?;
    let remaining = s.trim();
    if !remaining.is_empty() {
        return Err(EvalError::ParseError(format!(
            "unexpected trailing input: {remaining}"
        )));
    }
    Ok(expr)
}

// ---------------------------------------------------------------------------
// Whitespace
// ---------------------------------------------------------------------------

fn ws(input: &mut &str) -> WResult<()> {
    take_while(0.., |c: char| c.is_ascii_whitespace())
        .void()
        .parse_next(input)
}

// ---------------------------------------------------------------------------
// Literals
// ---------------------------------------------------------------------------

fn null_literal(input: &mut &str) -> WResult<Expr> {
    "null".parse_next(input)?;
    // Reject if it's a prefix of an identifier
    if input
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(ContextError::new());
    }
    Ok(Expr::Literal(Value::Null))
}

fn bool_literal(input: &mut &str) -> WResult<Expr> {
    let val = alt(("true", "false")).parse_next(input)?;
    // Reject if it's a prefix of an identifier
    if input
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(ContextError::new());
    }
    Ok(Expr::Literal(Value::Bool(val == "true")))
}

fn digit_sequence<'a>(input: &mut &'a str) -> WResult<&'a str> {
    take_while(1.., |c: char| c.is_ascii_digit()).parse_next(input)
}

fn number_literal(input: &mut &str) -> WResult<Expr> {
    let neg = opt("-").parse_next(input)?;
    let int_part: &str = digit_sequence.parse_next(input)?;

    // Check for decimal point followed by digits
    let frac: Option<&str> = opt(preceded(".", digit_sequence)).parse_next(input)?;

    let sign = if neg.is_some() { "-" } else { "" };

    if let Some(frac_digits) = frac {
        let s = format!("{sign}{int_part}.{frac_digits}");
        let f: f64 = s.parse().map_err(|_| ContextError::new())?;
        Ok(Expr::Literal(Value::Float(f)))
    } else {
        let s = format!("{sign}{int_part}");
        let i: i64 = s.parse().map_err(|_| ContextError::new())?;
        Ok(Expr::Literal(Value::Int(i)))
    }
}

fn string_literal(input: &mut &str) -> WResult<Expr> {
    "\"".parse_next(input)?;
    let mut result = String::new();
    loop {
        let c = any.parse_next(input)?;
        match c {
            '"' => break,
            '\\' => {
                let escaped = any.parse_next(input)?;
                match escaped {
                    'n' => result.push('\n'),
                    't' => result.push('\t'),
                    '\\' => result.push('\\'),
                    '"' => result.push('"'),
                    other => {
                        result.push('\\');
                        result.push(other);
                    }
                }
            }
            other => result.push(other),
        }
    }
    Ok(Expr::Literal(Value::String(result)))
}

// ---------------------------------------------------------------------------
// Field paths
// ---------------------------------------------------------------------------

fn ident(input: &mut &str) -> WResult<String> {
    let first: &str =
        take_while(1, |c: char| c.is_ascii_alphabetic() || c == '_').parse_next(input)?;
    let rest: &str =
        take_while(0.., |c: char| c.is_ascii_alphanumeric() || c == '_').parse_next(input)?;
    Ok(format!("{first}{rest}"))
}

fn path_segment(input: &mut &str) -> WResult<PathSegment> {
    alt((
        // Indexed: [0]
        delimited_index,
        // Quoted field: .["key"]
        preceded(".", quoted_field_segment),
        // Named field: .foo
        preceded(".", named_field_segment),
    ))
    .parse_next(input)
}

fn delimited_index(input: &mut &str) -> WResult<PathSegment> {
    "[".parse_next(input)?;
    let seg = index_segment.parse_next(input)?;
    "]".parse_next(input)?;
    Ok(seg)
}

fn index_segment(input: &mut &str) -> WResult<PathSegment> {
    let digits: &str = digit_sequence.parse_next(input)?;
    let idx: usize = digits.parse().map_err(|_| ContextError::new())?;
    Ok(PathSegment::Index(idx))
}

fn quoted_field_segment(input: &mut &str) -> WResult<PathSegment> {
    "[\"".parse_next(input)?;
    let mut result = String::new();
    loop {
        let c = any.parse_next(input)?;
        match c {
            '"' => break,
            '\\' => {
                let escaped = any.parse_next(input)?;
                result.push(escaped);
            }
            other => result.push(other),
        }
    }
    "]".parse_next(input)?;
    Ok(PathSegment::QuotedField(result))
}

fn named_field_segment(input: &mut &str) -> WResult<PathSegment> {
    let name = ident.parse_next(input)?;
    Ok(PathSegment::Field(name))
}

fn field_path(input: &mut &str) -> WResult<Expr> {
    // First segment must start with `.`
    let first = alt((
        preceded(".", quoted_field_segment),
        preceded(".", named_field_segment),
    ))
    .parse_next(input)?;

    let mut segments = vec![first];

    // Additional segments
    loop {
        let checkpoint = *input;
        match path_segment.parse_next(input) {
            Ok(seg) => segments.push(seg),
            Err(_) => {
                *input = checkpoint;
                break;
            }
        }
    }

    Ok(Expr::FieldPath(segments))
}

// ---------------------------------------------------------------------------
// Function calls
// ---------------------------------------------------------------------------

fn function_call(input: &mut &str) -> WResult<Expr> {
    let name = ident.parse_next(input)?;
    // Reject keywords as function names
    if matches!(name.as_str(), "if" | "else" | "true" | "false" | "null") {
        return Err(ContextError::new());
    }
    ws.parse_next(input)?;
    "(".parse_next(input)?;
    ws.parse_next(input)?;

    let args: Vec<Expr> = opt(separated(
        1..,
        |i: &mut &str| {
            ws.parse_next(i)?;
            let e = expr_top.parse_next(i)?;
            ws.parse_next(i)?;
            Ok(e)
        },
        ",",
    ))
    .parse_next(input)?
    .unwrap_or_default();

    ws.parse_next(input)?;
    ")".parse_next(input)?;
    Ok(Expr::FunctionCall(name, args))
}

// ---------------------------------------------------------------------------
// If/else
// ---------------------------------------------------------------------------

fn if_else_expr(input: &mut &str) -> WResult<Expr> {
    "if".parse_next(input)?;
    // Must be followed by whitespace
    if input
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(ContextError::new());
    }
    ws.parse_next(input)?;
    let cond = expr_top.parse_next(input)?;
    ws.parse_next(input)?;
    "{".parse_next(input)?;
    ws.parse_next(input)?;
    let then_expr = expr_top.parse_next(input)?;
    ws.parse_next(input)?;
    "}".parse_next(input)?;
    ws.parse_next(input)?;
    "else".parse_next(input)?;
    ws.parse_next(input)?;
    "{".parse_next(input)?;
    ws.parse_next(input)?;
    let else_expr = expr_top.parse_next(input)?;
    ws.parse_next(input)?;
    "}".parse_next(input)?;
    Ok(Expr::IfElse(
        Box::new(cond),
        Box::new(then_expr),
        Box::new(else_expr),
    ))
}

// ---------------------------------------------------------------------------
// Parenthesized expressions
// ---------------------------------------------------------------------------

fn paren_expr(input: &mut &str) -> WResult<Expr> {
    "(".parse_next(input)?;
    ws.parse_next(input)?;
    let e = expr_top.parse_next(input)?;
    ws.parse_next(input)?;
    ")".parse_next(input)?;
    Ok(e)
}

// ---------------------------------------------------------------------------
// Atom (highest-precedence expression)
// ---------------------------------------------------------------------------

fn atom(input: &mut &str) -> WResult<Expr> {
    ws.parse_next(input)?;
    let result = alt((
        if_else_expr,
        paren_expr,
        null_literal,
        bool_literal,
        function_call,
        string_literal,
        field_path,
        number_literal,
    ))
    .parse_next(input)?;
    ws.parse_next(input)?;
    Ok(result)
}

/// Atom that also handles unary minus for negative number literals
/// when it appears at the start of an atom position.
fn atom_with_neg(input: &mut &str) -> WResult<Expr> {
    ws.parse_next(input)?;
    // Try negative number literal first (only in atom position)
    let checkpoint = *input;
    if input.starts_with('-') {
        if let Ok(e) = number_literal.parse_next(input) {
            ws.parse_next(input)?;
            return Ok(e);
        }
        *input = checkpoint;
    }
    atom.parse_next(input)
}

// ---------------------------------------------------------------------------
// Precedence climbing (binary operators)
// Precedence (low to high): ??, ||, &&, == != > >= < <=, + -, * /
// ---------------------------------------------------------------------------

/// Try to parse a literal string token, returning `None` on failure without consuming input.
fn try_token<'a>(input: &mut &'a str, token: &str) -> Option<&'a str> {
    let checkpoint = *input;
    let result: core::result::Result<&str, ContextError> =
        winnow::token::literal(token).parse_next(input);
    match result {
        Ok(matched) => Some(matched),
        Err(_) => {
            *input = checkpoint;
            None
        }
    }
}

fn mul_div(input: &mut &str) -> WResult<Expr> {
    let mut left = atom_with_neg.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let op = if try_token(input, "*").is_some() {
            Op::Mul
        } else if try_token(input, "/").is_some() {
            Op::Div
        } else {
            break;
        };
        let right = atom_with_neg.parse_next(input)?;
        left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
    }
    Ok(left)
}

fn add_sub(input: &mut &str) -> WResult<Expr> {
    let mut left = mul_div.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let op = if try_token(input, "+").is_some() {
            Op::Add
        } else if try_token(input, "-").is_some() {
            Op::Sub
        } else {
            break;
        };
        let right = mul_div.parse_next(input)?;
        left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
    }
    Ok(left)
}

fn comparison(input: &mut &str) -> WResult<Expr> {
    let mut left = add_sub.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let op = if try_token(input, "==").is_some() {
            Op::Eq
        } else if try_token(input, "!=").is_some() {
            Op::Neq
        } else if try_token(input, ">=").is_some() {
            Op::Gte
        } else if try_token(input, "<=").is_some() {
            Op::Lte
        } else if try_token(input, ">").is_some() {
            Op::Gt
        } else if try_token(input, "<").is_some() {
            Op::Lt
        } else {
            break;
        };
        let right = add_sub.parse_next(input)?;
        left = Expr::BinaryOp(Box::new(left), op, Box::new(right));
    }
    Ok(left)
}

fn and_expr(input: &mut &str) -> WResult<Expr> {
    let mut left = comparison.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        if try_token(input, "&&").is_none() {
            break;
        }
        let right = comparison.parse_next(input)?;
        left = Expr::BinaryOp(Box::new(left), Op::And, Box::new(right));
    }
    Ok(left)
}

fn or_expr(input: &mut &str) -> WResult<Expr> {
    let mut left = and_expr.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        if try_token(input, "||").is_none() {
            break;
        }
        let right = and_expr.parse_next(input)?;
        left = Expr::BinaryOp(Box::new(left), Op::Or, Box::new(right));
    }
    Ok(left)
}

/// Null coalescing `??` — right-associative.
fn null_coalesce(input: &mut &str) -> WResult<Expr> {
    let left = or_expr.parse_next(input)?;
    ws.parse_next(input)?;
    if try_token(input, "??").is_some() {
        // Right-recursive for right-associativity
        let right = null_coalesce.parse_next(input)?;
        Ok(Expr::NullCoalesce(Box::new(left), Box::new(right)))
    } else {
        Ok(left)
    }
}

fn expr_top(input: &mut &str) -> WResult<Expr> {
    null_coalesce.parse_next(input)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::ast::{Expr, Op, PathSegment};
    use crate::record::Value;

    // === Task 4: Literals and field paths ===

    #[test]
    fn parse_null_literal() {
        assert_eq!(parse_expr("null").unwrap(), Expr::Literal(Value::Null));
    }

    #[test]
    fn parse_bool_literal() {
        assert_eq!(
            parse_expr("true").unwrap(),
            Expr::Literal(Value::Bool(true))
        );
        assert_eq!(
            parse_expr("false").unwrap(),
            Expr::Literal(Value::Bool(false))
        );
    }

    #[test]
    fn parse_int_literal() {
        assert_eq!(parse_expr("42").unwrap(), Expr::Literal(Value::Int(42)));
        assert_eq!(parse_expr("-7").unwrap(), Expr::Literal(Value::Int(-7)));
    }

    #[test]
    fn parse_float_literal() {
        assert_eq!(
            parse_expr("3.14").unwrap(),
            Expr::Literal(Value::Float(3.14))
        );
        assert_eq!(
            parse_expr("-0.5").unwrap(),
            Expr::Literal(Value::Float(-0.5))
        );
    }

    #[test]
    fn parse_string_literal() {
        assert_eq!(
            parse_expr("\"hello\"").unwrap(),
            Expr::Literal(Value::String("hello".to_string()))
        );
    }

    #[test]
    fn parse_simple_field_path() {
        assert_eq!(
            parse_expr(".name").unwrap(),
            Expr::FieldPath(vec![PathSegment::Field("name".to_string())])
        );
    }

    #[test]
    fn parse_nested_field_path() {
        let expr = parse_expr(".metadata.merchant.name").unwrap();
        assert_eq!(
            expr,
            Expr::FieldPath(vec![
                PathSegment::Field("metadata".to_string()),
                PathSegment::Field("merchant".to_string()),
                PathSegment::Field("name".to_string()),
            ])
        );
    }

    #[test]
    fn parse_indexed_field_path() {
        let expr = parse_expr(".items[0].name").unwrap();
        assert_eq!(
            expr,
            Expr::FieldPath(vec![
                PathSegment::Field("items".to_string()),
                PathSegment::Index(0),
                PathSegment::Field("name".to_string()),
            ])
        );
    }

    #[test]
    fn parse_quoted_field_path() {
        let expr = parse_expr(".[\"key with spaces\"]").unwrap();
        assert_eq!(
            expr,
            Expr::FieldPath(vec![PathSegment::QuotedField(
                "key with spaces".to_string()
            )])
        );
    }

    // === Task 5: Function calls, binary ops, if/else, null coalesce ===

    #[test]
    fn parse_function_call_no_args() {
        assert_eq!(
            parse_expr("now()").unwrap(),
            Expr::FunctionCall("now".to_string(), vec![])
        );
    }

    #[test]
    fn parse_function_call_one_arg() {
        assert_eq!(
            parse_expr("to_float(.amount)").unwrap(),
            Expr::FunctionCall(
                "to_float".to_string(),
                vec![Expr::FieldPath(vec![PathSegment::Field(
                    "amount".to_string()
                )])]
            )
        );
    }

    #[test]
    fn parse_nested_function_call() {
        let expr = parse_expr("to_float(to_string(.x))").unwrap();
        assert_eq!(
            expr,
            Expr::FunctionCall(
                "to_float".to_string(),
                vec![Expr::FunctionCall(
                    "to_string".to_string(),
                    vec![Expr::FieldPath(vec![PathSegment::Field("x".to_string())])]
                )]
            )
        );
    }

    #[test]
    fn parse_function_call_multiple_args() {
        let expr = parse_expr("concat(.first, \" \", .last)").unwrap();
        match expr {
            Expr::FunctionCall(name, args) => {
                assert_eq!(name, "concat");
                assert_eq!(args.len(), 3);
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn parse_comparison() {
        let expr = parse_expr(".amount > 0.0").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp(
                Box::new(Expr::FieldPath(vec![PathSegment::Field(
                    "amount".to_string()
                )])),
                Op::Gt,
                Box::new(Expr::Literal(Value::Float(0.0)))
            )
        );
    }

    #[test]
    fn parse_equality() {
        let expr = parse_expr(".status == \"active\"").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp(
                Box::new(Expr::FieldPath(vec![PathSegment::Field(
                    "status".to_string()
                )])),
                Op::Eq,
                Box::new(Expr::Literal(Value::String("active".to_string())))
            )
        );
    }

    #[test]
    fn parse_not_equal() {
        let expr = parse_expr(".status != \"voided\"").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp(
                Box::new(Expr::FieldPath(vec![PathSegment::Field(
                    "status".to_string()
                )])),
                Op::Neq,
                Box::new(Expr::Literal(Value::String("voided".to_string())))
            )
        );
    }

    #[test]
    fn parse_arithmetic() {
        let expr = parse_expr(".amount * .rate").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp(
                Box::new(Expr::FieldPath(vec![PathSegment::Field(
                    "amount".to_string()
                )])),
                Op::Mul,
                Box::new(Expr::FieldPath(vec![PathSegment::Field(
                    "rate".to_string()
                )]))
            )
        );
    }

    #[test]
    fn parse_and_or() {
        let expr = parse_expr(".a > 0 && .b > 0").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp(
                Box::new(Expr::BinaryOp(
                    Box::new(Expr::FieldPath(vec![PathSegment::Field("a".to_string())])),
                    Op::Gt,
                    Box::new(Expr::Literal(Value::Int(0)))
                )),
                Op::And,
                Box::new(Expr::BinaryOp(
                    Box::new(Expr::FieldPath(vec![PathSegment::Field("b".to_string())])),
                    Op::Gt,
                    Box::new(Expr::Literal(Value::Int(0)))
                ))
            )
        );
    }

    #[test]
    fn parse_null_coalesce() {
        let expr = parse_expr(".name ?? \"Unknown\"").unwrap();
        assert_eq!(
            expr,
            Expr::NullCoalesce(
                Box::new(Expr::FieldPath(vec![PathSegment::Field(
                    "name".to_string()
                )])),
                Box::new(Expr::Literal(Value::String("Unknown".to_string())))
            )
        );
    }

    #[test]
    fn parse_if_else() {
        let expr =
            parse_expr("if .currency == \"USD\" { .amount } else { .amount * .rate }").unwrap();
        match expr {
            Expr::IfElse(cond, then_br, else_br) => {
                assert_eq!(
                    *cond,
                    Expr::BinaryOp(
                        Box::new(Expr::FieldPath(vec![PathSegment::Field(
                            "currency".to_string()
                        )])),
                        Op::Eq,
                        Box::new(Expr::Literal(Value::String("USD".to_string())))
                    )
                );
                assert_eq!(
                    *then_br,
                    Expr::FieldPath(vec![PathSegment::Field("amount".to_string())])
                );
                assert_eq!(
                    *else_br,
                    Expr::BinaryOp(
                        Box::new(Expr::FieldPath(vec![PathSegment::Field(
                            "amount".to_string()
                        )])),
                        Op::Mul,
                        Box::new(Expr::FieldPath(vec![PathSegment::Field(
                            "rate".to_string()
                        )]))
                    )
                );
            }
            other => panic!("expected IfElse, got {other:?}"),
        }
    }

    #[test]
    fn parse_parenthesized_expr() {
        let expr = parse_expr("(.a + .b) * .c").unwrap();
        assert_eq!(
            expr,
            Expr::BinaryOp(
                Box::new(Expr::BinaryOp(
                    Box::new(Expr::FieldPath(vec![PathSegment::Field("a".to_string())])),
                    Op::Add,
                    Box::new(Expr::FieldPath(vec![PathSegment::Field("b".to_string())]))
                )),
                Op::Mul,
                Box::new(Expr::FieldPath(vec![PathSegment::Field("c".to_string())]))
            )
        );
    }
}
