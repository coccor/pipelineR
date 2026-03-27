# pipelineR Architecture

This document describes the internal architecture of pipelineR: how data flows through the system, how plugins communicate with the engine, and why key design decisions were made.

## High-level design

pipelineR is a **row-based, streaming batch ETL engine** with a **plugin-per-process** architecture. Pipelines are defined in TOML config files and executed by the engine, which orchestrates three stages:

```
Source Connector  -->  Transform (DSL)  -->  Sink Connector(s)
     (gRPC)              (in-engine)            (gRPC)
```

Each connector runs as a **separate OS process** that communicates with the engine over **localhost gRPC**. The engine manages connector lifecycles: spawning, port discovery, health checking, and teardown.

## Data model

The internal data model is **row-based**, not columnar:

```
Value = Null | Bool | Int(i64) | Float(f64) | String | Bytes
      | Timestamp(DateTime<Utc>) | Array(Vec<Value>) | Map(Record)

Record = IndexMap<String, Value>     # ordered key-value pairs
RecordBatch = Vec<Record> + metadata # batch of records for streaming
```

**Why row-based?** Transforms operate on individual records (field access, conditional logic, type coercion). A columnar representation would require constant row-to-column-to-row conversion. Arrow is only used at the **sink boundary** by columnar sinks (Parquet, Delta Lake) via the `pipeliner-arrow-convert` crate.

## Pipeline execution

### Stages and channels

The runtime (`pipeliner-core/src/runtime.rs`) wires stages together with **bounded async channels** (`tokio::mpsc`, capacity 32):

```
                    ┌─────────────┐
                    │   Source     │ (gRPC streaming)
                    │  Extraction  │
                    └──────┬──────┘
                           │ mpsc(32)
                    ┌──────▼──────┐
                    │  Transform  │ (DSL steps, in-process)
                    │   Chain     │
                    └──────┬──────┘
                      ┌────┼────┐
                 mpsc │    │    │ mpsc (clone per sink)
                 (32) │    │    │ (32)
               ┌──────▼┐ ┌▼────▼──┐ ┌──────────┐
               │ Sink 0 │ │ Sink 1 │ │ Dead-Ltr │
               │ (gRPC) │ │ (gRPC) │ │  (gRPC)  │
               └────────┘ └────────┘ └──────────┘
```

**Backpressure:** When a channel is full, the sending stage blocks. If a sink is slow, the transform stage (and transitively the source) slows down automatically. No unbounded memory growth.

**Fan-out:** After transform, each batch is cloned and sent to every configured sink. Sinks process independently -- one slow sink does not block others.

### Stage details

**Source extraction** (`run_source`):
- Calls the source connector's `Extract` RPC (server-streaming)
- Receives `RecordBatch` messages until the stream ends
- Extracts the watermark from the final response
- Sends each batch to the transform stage channel

**Transform** (`run_transforms`):
- Receives batches from the source channel
- Applies DSL transform steps in order using dead-letter-aware execution
- Records that fail a step are enriched with error context and sent to the dead-letter channel
- Records that pass `where` filters continue; others are silently dropped
- Clones the transformed batch and fans out to each sink channel

**Sink load** (`run_sink`):
- Sends a `LoadMetadata` message first (config + schema)
- Streams `RecordBatch` messages via client-streaming `Load` RPC
- Collects the `LoadResult` (rows_written, rows_errored)

### Cancellation

Each pipeline run has a `CancellationToken` (from `tokio-util`). When `CancelRun` is called:

1. The token is cancelled
2. Source checks `cancel.is_cancelled()` between batches and exits
3. Transform checks the token and breaks its loop
4. Channels close naturally, causing sinks to finish

Cancellation is **between-batch** -- a batch in progress will complete before the stage exits. This prevents partial writes.

### Event streaming

The `RunEventSender` wraps a `broadcast::channel` for real-time progress updates:

- `StageTransition` -- stage started/completed/failed
- `BatchProgress` -- batch number and record count per stage
- `Error` -- error messages per stage

Events are best-effort: if no `WatchRun` listener is active, events are silently dropped.

## Connector architecture

### Process model

Each connector is a **standalone binary** that implements a gRPC server:

```
Engine                          Connector Binary
  │                                  │
  │── spawn(binary, --port 0) ──────>│
  │                                  │── bind to random port
  │<── stdout: "PORT=12345" ─────────│
  │                                  │── start gRPC server
  │── gRPC: Describe() ────────────>│
  │<── SourceDescriptor ────────────│
  │── gRPC: Extract(config) ───────>│
  │<── stream RecordBatch ──────────│
  │── kill ─────────────────────────>│
```

**Why separate processes?**
- **Isolation:** A connector crash does not take down the engine
- **Language independence:** Connectors can be written in any language (though the SDK is Rust)
- **Resource control:** Each connector can have its own memory/CPU limits

### SDK scaffold

The `pipeliner-sdk` crate provides helpers so connector authors implement a trait, not gRPC boilerplate:

```rust
// Source connector author writes:
struct MySource;

#[async_trait]
impl Source for MySource {
    fn describe(&self) -> SourceDescriptor { ... }
    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError> { ... }
    async fn discover_schema(...) -> Result<SchemaResponse, DiscoveryError> { ... }
    async fn discover_partitions(...) -> Result<Vec<Partition>, DiscoveryError> { ... }
    async fn extract(..., tx: mpsc::Sender<RecordBatch>) -> Result<String, ExtractionError> { ... }
}

// main.rs:
pipeliner_sdk::run_source_connector(MySource).await?;
```

The SDK wraps the trait impl in a gRPC service (`GrpcSourceService`), starts a tonic server, prints `PORT=<n>` to stdout, and handles graceful shutdown.

### Connection pooling (SQL)

The SQL connector maintains a process-wide `DriverPool` that caches connections by `(driver, connection_string)`. Before returning a cached connection, `ping()` verifies it's alive. Dead connections are automatically replaced.

### Retry logic (REST)

The REST connector retries on transient HTTP errors (429, 500, 502, 503, 504) with exponential backoff. For 429 responses, the `Retry-After` header value is respected.

## gRPC protocol

Two sets of gRPC services:

### Connector-facing (plugin protocol)

Defined in `connector.proto`:

- `SourceConnector` service: `Describe`, `Validate`, `DiscoverSchema`, `DiscoverPartitions`, `Extract` (server-streaming)
- `SinkConnector` service: `Describe`, `Validate`, `SchemaRequirement`, `Load` (client-streaming)

### Orchestrator-facing (engine API)

Defined in `service.proto`:

- `PipelineR` service: `RunPipeline`, `WatchRun` (server-streaming), `GetRunStatus`, `CancelRun`, `DiscoverSchema`, `DiscoverPartitions`, `ValidatePipeline`, `Health`

Both use Protocol Buffers v3 with `tonic` as the Rust gRPC framework. Max message size: 64 MB.

## DSL interpreter

The transform DSL is parsed and executed in four layers:

```
"set(.amount, to_float(.amount))"
         │
    ┌────▼────┐
    │ Parser  │  (winnow)  -->  TransformStep { function, args }
    └────┬────┘
    ┌────▼────┐
    │   AST   │  Expr enum: FieldPath, Literal, FunctionCall, BinaryOp, IfElse, NullCoalesce
    └────┬────┘
    ┌────▼────┐
    │  Eval   │  eval_expr(expr, record) --> Value
    └────┬────┘
    ┌────▼────┐
    │  Step   │  execute_step(step, records) -- mutates Vec<Record> in place
    └─────────┘
```

**Parser:** Uses `winnow` (a `nom` successor) for zero-copy parsing with proper operator precedence.

**Evaluator:** Recursive expression evaluation. Field paths support nested access (`.meta.name`), array indexing (`.items[0]`), and quoted fields (`["key with spaces"]`). Missing fields return `Null` (safe navigation).

**Step executor:** Dead-letter-aware execution. Each step returns successfully-processed records and a list of `DeadLetterRecord` entries for records that failed.

## Observability

### Tracing

OpenTelemetry spans are created at each pipeline stage:

```
pipeline_run (root)
  ├── source_extract
  ├── transform
  ├── sink_load [sink_index=0]
  ├── sink_load [sink_index=1]
  └── ...
```

Attributes: `pipeline_name`, `run_id`, `partition_key`, `records_read`, `sink_index`.

### Metrics

Five OTLP metrics are recorded per pipeline run:

- `pipeline.records_read` (counter)
- `pipeline.records_written` (counter)
- `pipeline.records_dropped` (counter)
- `pipeline.transform_errors` (counter)
- `pipeline.duration_seconds` (histogram)

All metrics include a `pipeline_name` attribute.

### Configuration

Telemetry is initialized by the CLI or gRPC server from the `[telemetry]` section in the pipeline TOML. Falls back to the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable. Disabled by default.

## Arrow conversion

The `pipeliner-arrow-convert` crate converts `Vec<Record>` to Arrow `RecordBatch` for columnar sinks:

1. **Schema inference:** Scan all records to determine field names and types. First non-null value per field determines the Arrow type. Null-only fields default to `Utf8`.
2. **Type mapping:** `Bool` -> `Boolean`, `Int` -> `Int64`, `Float` -> `Float64`, `String` -> `Utf8`, `Bytes` -> `Binary`, `Timestamp` -> `Timestamp(Nanosecond, UTC)`, `Array`/`Map` -> `Utf8` (JSON-serialized).
3. **Array building:** One Arrow array per column, populated row-by-row with null for missing/mismatched values.

## Configuration loading

Pipeline TOML files go through several processing steps:

1. **Read** the raw TOML file
2. **Substitute** `${VAR_NAME}` references with environment variable values
3. **Deserialize** into typed `PipelineConfig` struct
4. **Validate** transform step syntax (parse each DSL string)
5. **Resolve** connector names to binary paths via the connector registry
6. **Build** the `PipelineDefinition` by spawning connector processes and connecting gRPC clients

## Error handling

- **Library code:** Uses `thiserror` for typed errors. No `unwrap()` or `expect()`.
- **CLI:** May `expect()` only in `main()`.
- **Per-record errors:** Caught by the dead-letter-aware step executor. Failed records are enriched with `__error_step`, `__error_message`, `__error_timestamp` and routed to the dead-letter sink.
- **gRPC errors:** Mapped to appropriate `tonic::Status` codes (`NOT_FOUND`, `INVALID_ARGUMENT`, `INTERNAL`).
- **Exit codes:** `0` = success, `1` = pipeline failure, `2` = config error.

## Limitations (v1)

- **Batch only.** No streaming or CDC.
- **Single node.** No distributed execution; parallelism via orchestrator fan-out.
- **No state.** Watermarks are reported per run; the orchestrator persists them.
- **Fixed DSL.** No custom functions registered by plugins.
- **In-memory run state.** Run tracking is lost on restart (acceptable for sidecar model).
- **No hot-reload.** Connector processes are spawned on first use and killed on shutdown.
