---
name: dsl-parser
description: "Transform DSL implementation for pipelineR. Use this skill when working on the DSL parser (parsing transform step strings into AST), the DSL interpreter (executing AST against Records), adding new DSL operations, or debugging transform behavior. Trigger when files in pipeliner-core/src/dsl/ or pipeliner-core/src/transform/ are involved, or when the user mentions transform steps, DSL functions, field access paths, or the expression language."
---

# pipelineR transform DSL

## Design constraints

- NOT Turing-complete. No loops, no recursion, no user-defined functions (v1).
- Every operation is fallible. Errors produce `TransformError`, never panic.
- Operates on `Record` (HashMap<String, Value>). The DSL mutates records in place or filters them.

## Grammar overview

A transform step is a single function call string:

```
step = function_name '(' arguments ')'
arguments = expression (',' expression)*
expression = field_path | literal | function_call | binary_expr | if_expr | null_coalesce
field_path = '.' identifier ('.' identifier | '[' index ']')*
literal = string | number | bool | 'null'
binary_expr = expression operator expression
if_expr = 'if' expression '{' expression '}' 'else' '{' expression '}'
null_coalesce = expression '??' expression
```

Examples:
```
set(.amount, to_float(.amount))
rename(.old_name, .new_name)
where(.amount > 0.0)
set(.full_name, concat(.first_name, " ", .last_name))
set(.x, if .currency == "USD" { .amount } else { .amount * .rate })
set(.name, .metadata.merchant.name ?? "Unknown")
remove(.internal_field)
```

## Parser implementation

Use `winnow` (preferred over `nom` for better error messages). The parser produces an `Expr` AST:

```rust
enum Expr {
    FieldPath(Vec<PathSegment>),    // .foo.bar[0].baz
    Literal(Value),                  // "hello", 42, 3.14, true, null
    FunctionCall(String, Vec<Expr>), // to_float(.amount)
    BinaryOp(Box<Expr>, Op, Box<Expr>), // .x > 0.0
    IfElse(Box<Expr>, Box<Expr>, Box<Expr>), // if cond { a } else { b }
    NullCoalesce(Box<Expr>, Box<Expr>), // .x ?? "default"
}

enum PathSegment {
    Field(String),    // .foo
    Index(usize),     // [0]
    QuotedField(String), // .["key with spaces"]
}

enum Op { Eq, Neq, Gt, Gte, Lt, Lte, And, Or, Add, Sub, Mul, Div }
```

A transform step is parsed as:
```rust
struct TransformStep {
    function: String,      // "set", "rename", "where", "remove", etc.
    args: Vec<Expr>,
}
```

## Interpreter implementation

The interpreter has two layers:

### 1. Step executor

Takes a `TransformStep` and a `&mut RecordBatch`, returns `Result<(), TransformError>`.

Top-level functions:
- `set(field_path, expr)` — evaluate expr, write result to field_path
- `rename(old_path, new_path)` — move value from old to new
- `remove(field_path)` — delete field
- `where(predicate)` — filter: keep rows where predicate is truthy. Applied batch-wide: iterate records, retain matching.
- `select(field1, field2, ...)` — keep only listed fields
- `reject(field1, field2, ...)` — drop listed fields
- `route(condition, output_name)` — split rows (future: fan-out)

`set`, `rename`, `remove` are row-level: iterate each record in the batch.
`where` is batch-level: produces a new (possibly smaller) batch.

### 2. Expression evaluator

Takes an `Expr` and a `&Record`, returns `Result<Value, EvalError>`.

- `FieldPath` → walk the record's nested maps/arrays following each segment
- `Literal` → return the literal value
- `FunctionCall` → evaluate args, dispatch to built-in function
- `BinaryOp` → evaluate both sides, apply operator with type coercion rules
- `IfElse` → evaluate condition, return the matching branch
- `NullCoalesce` → evaluate left; if Null, evaluate right

### Type coercion in binary ops

- Numeric ops (`+`, `-`, `*`, `/`, `>`, `<`, `>=`, `<=`): both sides must be numeric (Int or Float). If mixed, promote Int to Float.
- Equality (`==`, `!=`): compare same types. Different types are never equal (no implicit coercion).
- Boolean ops (`&&`, `||`): both sides must be Bool.
- String concatenation: use `concat()` function, not `+`.

## Built-in functions by category

Implement in phases. M1 needs the core set marked with *.

| Function | Args | Returns | M1? |
|---|---|---|---|
| `to_string(v)` | any | String | * |
| `to_int(v)` | String/Float | Int | * |
| `to_float(v)` | String/Int | Float | * |
| `to_bool(v)` | String/Int | Bool | * |
| `to_timestamp(s, fmt)` | String, String | Timestamp | * |
| `is_null(v)` | any | Bool | * |
| `is_string(v)` | any | Bool | |
| `is_int(v)` | any | Bool | |
| `upcase(s)` | String | String | |
| `downcase(s)` | String | String | |
| `trim(s)` | String | String | |
| `replace(s, from, to)` | String×3 | String | |
| `concat(a, b, ...)` | String... | String | * |
| `contains(s, sub)` | String×2 | Bool | |
| `starts_with(s, pre)` | String×2 | Bool | |
| `parse_timestamp(s, fmt)` | String, String | Timestamp | * |
| `format_timestamp(t, fmt)` | Timestamp, String | String | |
| `now()` | none | Timestamp | |
| `round(n, places)` | Float, Int | Float | |
| `length(v)` | String/Array | Int | |
| `first(arr)` | Array | Value | |
| `last(arr)` | Array | Value | |
| `coalesce(a, b, ...)` | any... | first non-null | * |
| `flatten(map)` | Map | Map (1 level) | |

## Error handling

Each record that fails a transform step is handled based on pipeline config:

1. If `[dead_letter]` is configured → route the original record + error context to the dead-letter sink
2. Else → log a warning with the step name, record index, and error message. Drop the record.

The batch continues processing remaining records. A transform error never fails the pipeline.

## Gotchas

- Field paths that traverse into a non-Map value (e.g., `.foo.bar` where `.foo` is a String) should return `Null`, not error. This enables safe navigation patterns: `.metadata.merchant.name ?? "Unknown"`.
- `where` must not mutate records — it only filters. If someone writes `where(set(...))`, that's a parse error.
- String escaping in TOML: transform steps are TOML strings. Single-quoted strings (`'...'`) don't need escape sequences. Double-quoted strings need `\"`. Recommend single quotes in docs and examples.
- `parse_timestamp` format strings use `chrono::format::strftime` syntax. Document the common patterns: `%Y-%m-%d`, `%Y-%m-%dT%H:%M:%S`, `%s` (unix epoch).
