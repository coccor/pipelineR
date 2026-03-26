Add a new DSL function: `$ARGUMENTS`

Steps:
1. Read `.claude/skills/dsl-parser/SKILL.md` for the grammar and interpreter patterns
2. Add the function signature to the built-in functions table in `pipeliner-core/src/dsl/functions.rs`
3. Implement the function body — it takes `&[Value]` args and returns `Result<Value, EvalError>`
4. Add parser test: verify the step string parses to the correct AST
5. Add interpreter test: verify the function produces correct output for normal inputs
6. Add error test: verify the function returns `EvalError` for invalid inputs (wrong types, null, etc.)
7. Update the DSL operations table in `docs/REQUIREMENTS.md` if the function is new (not already listed)
