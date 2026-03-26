use winnow::combinator::{alt, opt, preceded};
use winnow::error::ContextError;
use winnow::prelude::*;
use winnow::token::{any, take_while};
use winnow::Result as WResult;

use crate::dsl::ast::{Expr, PathSegment};
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
    let expr = atom
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
// Atom (highest-precedence expression)
// ---------------------------------------------------------------------------

fn atom(input: &mut &str) -> WResult<Expr> {
    ws.parse_next(input)?;
    let result = alt((
        null_literal,
        bool_literal,
        string_literal,
        field_path,
        number_literal,
    ))
    .parse_next(input)?;
    ws.parse_next(input)?;
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::ast::{Expr, PathSegment};
    use crate::record::Value;

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
}
