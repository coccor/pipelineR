---
name: rust-etl-engine
description: "Architecture and patterns for pipelineR's core engine. Use this skill when working on pipeliner-core (the pipeline runtime, config loader, plugin manager, record batching, backpressure, or the orchestration loop that wires source → transform → sink). Also use when adding new pipeline features, changing the Record/Value data model, or debugging pipeline execution issues. Trigger on any work in pipeliner-core/ or when the task involves how data flows through the system."
---

# pipelineR core engine

## Data model

The internal representation is row-based, NOT columnar.

```rust
pub type Record = HashMap<String, Value>;

pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(DateTime<Utc>),
    Array(Vec<Value>),
    Map(HashMap<String, Value>),
}
```

Records are grouped into `RecordBatch` (`Vec<Record>`, max 1000 records default). Batches flow through bounded async channels between stages.

## Pipeline runtime

The execution model is a three-stage async pipeline:

```
Source plugin (gRPC stream) → Transform chain (DSL interpreter) → Sink plugin(s) (gRPC stream)
```

Each stage runs in its own Tokio task. Stages are connected by `tokio::sync::mpsc` bounded channels. Backpressure propagates naturally — if the sink is slow, the channel fills, the transform blocks, the source blocks.

### Key patterns

- **Fan-out to multiple sinks:** Clone each `RecordBatch` and send to each sink's channel independently. Sinks run in parallel tasks.
- **Cancellation:** Use `tokio_util::sync::CancellationToken`. Check between batches in each stage. Propagate to plugin gRPC calls via tonic's cancellation.
- **Error handling:** Source/sink errors fail the pipeline (after retry). Transform errors route to dead-letter or drop the record. Never panic.
- **Watermark:** Collected from the source plugin after extraction completes. Stored in `RunStatus` and returned to the orchestrator.
- **Metrics:** Atomic counters incremented by each stage. Snapshot on demand for `WatchRun` events and `GetRunStatus`.

## Config loading

Pipeline configs are TOML. The loader:

1. Reads the TOML file (or inline string from gRPC request)
2. Resolves `${VAR}` placeholders from environment + runtime params (params take precedence)
3. Validates the structure (source, transforms, sinks, optional dead_letter)
4. Resolves plugin names to binary paths via `plugins.toml` registry
5. Returns a `PipelineConfig` struct ready for execution

## Plugin process management

Plugins are child processes. The engine:

1. Spawns the plugin binary with a `--port 0` flag (plugin picks a random port)
2. Plugin writes its actual port to stdout (first line)
3. Engine reads the port, connects via gRPC
4. On pipeline completion or engine shutdown, sends SIGTERM, waits, then SIGKILL

Keep it simple in v1: no hot-reload, no restart-on-crash, no connection pooling across runs.

## Gotchas

- `HashMap` iteration order is nondeterministic. If field ordering matters for a sink (e.g., CSV columns), the schema declaration or a `select` transform step must define the order.
- `Value::Timestamp` uses `chrono::DateTime<Utc>`. The DSL's `parse_timestamp` must always produce UTC. Timezone-aware parsing is a future concern.
- `RecordBatch` cloning for fan-out is a deep clone of all records. For large batches with big `Bytes` fields, this is expensive. Consider `Arc<RecordBatch>` if profiling shows this matters.
- Plugin gRPC streams can stall. Always set a deadline/timeout on the tonic client calls.
