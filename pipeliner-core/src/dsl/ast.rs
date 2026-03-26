use crate::record::Value;

/// A single parsed transform step: `function_name(arg1, arg2, ...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformStep {
    /// The name of the transform function (e.g. `set`, `rename`, `remove`).
    pub function: String,
    /// The arguments passed to the function.
    pub args: Vec<Expr>,
}

/// An expression in the DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Field access path: `.foo.bar[0].baz`
    FieldPath(Vec<PathSegment>),
    /// Literal value: `42`, `"hello"`, `true`, `null`
    Literal(Value),
    /// Function call: `to_float(.amount)`
    FunctionCall(String, Vec<Expr>),
    /// Binary operation: `.x > 0.0`
    BinaryOp(Box<Expr>, Op, Box<Expr>),
    /// Conditional: `if .x > 0 { .x } else { 0 }`
    IfElse(Box<Expr>, Box<Expr>, Box<Expr>),
    /// Null coalescing: `.x ?? "default"`
    NullCoalesce(Box<Expr>, Box<Expr>),
}

/// A segment in a field access path.
#[derive(Debug, Clone, PartialEq)]
pub enum PathSegment {
    /// Named field: `.foo`
    Field(String),
    /// Array index: `[0]`
    Index(usize),
    /// Quoted field: `.["key with spaces"]`
    QuotedField(String),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Equality: `==`
    Eq,
    /// Inequality: `!=`
    Neq,
    /// Greater than: `>`
    Gt,
    /// Greater than or equal: `>=`
    Gte,
    /// Less than: `<`
    Lt,
    /// Less than or equal: `<=`
    Lte,
    /// Logical and: `&&`
    And,
    /// Logical or: `||`
    Or,
    /// Addition: `+`
    Add,
    /// Subtraction: `-`
    Sub,
    /// Multiplication: `*`
    Mul,
    /// Division: `/`
    Div,
}
