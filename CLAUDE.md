# pipelineR

A lightweight, config-driven batch ETL engine in Rust. Runs as a K8s sidecar, exposes a gRPC API for orchestrators.

## Project structure

```
pipeliner-core/       # Engine library (config, runtime, DSL interpreter, plugin manager)
pipeliner-cli/        # CLI binary wrapping pipeliner-core
pipeliner-sdk/        # Plugin SDK (Source/Sink traits, gRPC scaffolding)
pipeliner-proto/      # Protobuf definitions (shared between engine and plugins)
pipeliner-arrow-convert/  # Row-based Record → Arrow RecordBatch utility (used by columnar sinks)
plugins/
  pipeliner-plugin-file/   # File source + sink (CSV, JSON, Parquet, cloud storage)
  pipeliner-plugin-sql/    # SQL source + sink (SQL Server, PostgreSQL, MySQL)
  pipeliner-plugin-rest/   # REST API source
  pipeliner-plugin-delta/  # Delta Lake sink
  pipeliner-plugin-parquet/ # Parquet sink
```

## Build and test

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
```

Run a single plugin integration test (requires Docker for DB tests):
```bash
cargo test -p pipeliner-plugin-sql --test integration -- --ignored
```

## Key conventions

- Every public function needs a doc comment.
- Error types use `thiserror`. Never `unwrap()` or `expect()` in library code. CLI may `expect()` only in `main()`.
- Async runtime is `tokio` (multi-thread). All I/O is async.
- gRPC uses `tonic` + `prost`. Proto files live in `pipeliner-proto/proto/`.
- Plugin binaries communicate with the engine over gRPC on a random port assigned at spawn time.
- The internal data model is row-based (`Record = Map<String, Value>`), NOT Apache Arrow. Arrow is only used at the sink boundary by columnar sink plugins.
- DSL parsing uses `nom` or `winnow`. The DSL is not Turing-complete — no loops, no recursion.
- Config files are TOML, parsed with the `toml` crate.
- All telemetry uses `opentelemetry` + `opentelemetry-otlp` crates.

## Architecture references

Read these before making structural changes:
- `docs/REQUIREMENTS.md` — full PRD with data model, plugin traits, gRPC API, DSL spec
- `docs/IMPLEMENTATION-PLAN.md` — phased milestones with dependencies
- `.claude/skills/` — domain-specific guidance for DSL parser, gRPC proto, plugin SDK, engine architecture

## When compacting

Always preserve: the current milestone being worked on, the list of files modified in this session, any failing test names, and the gRPC proto service definition.
