# pipelineR — A Lightweight, Config-Driven ETL Engine in Rust

## Product Requirements Document — v1

---

## 1. Vision

A single-binary, config-driven ETL engine written in Rust that enables external systems to orchestrate batch data pipelines without owning the extraction, transformation, or loading logic. The engine ships with a plugin SDK for community-built connectors, a purpose-built transform DSL, and first-class support for modern data lake formats.

The project exists to fill a gap: lightweight tools like Vector.dev solve observability routing but not batch ETL; platforms like Airbyte solve connectors but are too heavy to embed. pipelineR targets the middle ground — a fast, embeddable, extensible engine that an orchestrator can invoke via gRPC and trust to handle the rest.

---

## 2. Goals

- **Zero application-side ETL code.** The calling application is the orchestrator. It defines *what* to run (which pipeline, which parameters). pipelineR handles *how* (connect, extract, transform, load).
- **Single binary deployment.** No JVM, no Python runtime, no container orchestrator required. One binary, one config file, one gRPC call.
- **Sidecar-native.** Designed to run as a Kubernetes sidecar alongside an orchestrator application. Communicates over localhost gRPC with minimal latency and zero network infrastructure.
- **Source-aware partitioning.** The engine (via source plugins) can discover how a dataset should be partitioned, enabling the orchestrator to fan out parallel runs without knowing source-specific details.
- **Plugin-first architecture.** Every source and sink is a plugin. Built-in connectors ship as default plugins; community connectors use the same SDK and protocol.
- **Predictable resource usage.** Bounded memory, no GC pauses, streaming-style processing even in batch mode (row batches flow through the pipeline, not "load everything into memory then transform").
- **Config-driven pipelines.** Common ETL jobs are defined entirely in TOML — no Rust code required. Custom connectors require code; using them does not.

---

## 3. Non-Goals (v1)

- **Streaming / CDC.** v1 is batch-only. Pipelines have a defined start and end. Streaming and change-data-capture are future scope.
- **Built-in scheduling.** pipelineR does not include a cron-like scheduler. External systems trigger pipeline runs.
- **Distributed execution.** v1 runs on a single node. Parallelism is achieved by the orchestrator fanning out partition runs to multiple worker pods, each with its own pipelineR sidecar.
- **GUI / web dashboard.** No visual pipeline builder. Pipelines are config files managed by the user.
- **Data quality / validation framework.** pipelineR transforms data; it does not enforce schema contracts or run data quality assertions.
- **Standalone deployment.** v1 does not target a standalone Deployment/Service topology. The sidecar model is the primary deployment target.
- **HTTP API.** Out of scope. gRPC is the only remote interface.
- **State management.** pipelineR does not track extraction cursors or watermarks between runs. It reports watermarks per run; the orchestrator persists them.
- **Schema evolution handling.** When a source's schema changes between runs, the orchestrator is responsible for detecting and handling the change. pipelineR reports schema mismatches as errors.
- **DSL extensibility.** v1 does not support custom DSL functions registered by plugins. The built-in function set is fixed.

---

## 4. User Personas

### 4.1 Pipeline Operator (config-only user)

A DevOps engineer, data engineer, or backend developer who writes TOML pipeline definitions and triggers runs via CLI or the orchestrator application. They do not write Rust. They choose from available source/sink plugins, configure connection strings and credentials, define transforms using the built-in DSL, and monitor pipeline execution via logs and OpenTelemetry.

### 4.2 Connector Author (SDK user)

A developer (Rust) who builds a new source or sink plugin using the pipelineR SDK. They implement a trait (including schema discovery and partition discovery for sources), compile a plugin binary, and publish it as a crate. Their plugin communicates with the engine over gRPC.

### 4.3 Orchestrator Application (machine caller)

An external application (C#, Python, Go, etc.) running in the same Kubernetes pod as pipelineR. It claims jobs from a work queue, calls `DiscoverSchema` and `DiscoverPartitions` to understand the data, fans out parallel `RunPipeline` calls across workers, collects watermarks from each partition's result, computes the final watermark, and persists it. It owns scheduling, retry policy, state tracking, schema evolution, and business logic.

---

## 5. Architecture Overview

### 5.1 Deployment Model — Sidecar

pipelineR runs as a **sidecar container** alongside the orchestrator application in the same Kubernetes pod. The orchestrator communicates with pipelineR over **localhost gRPC**.

```
┌─ Kubernetes Pod ──────────────────────────────────────────┐
│                                                            │
│  ┌─────────────────────┐    gRPC     ┌──────────────────┐ │
│  │  Orchestrator        │◀──────────▶│  pipelineR        │ │
│  │  (C#, Python, etc.)  │ localhost  │  Sidecar          │ │
│  │                      │  :50051    │  (gRPC server)    │ │
│  │  - Claims jobs       │            │                    │ │
│  │  - Discovers schema  │            │  - Runs pipelines  │ │
│  │  - Discovers parts.  │            │  - Manages plugins │ │
│  │  - Fans out to       │            │  - Discovers schema│ │
│  │    workers           │            │  - Discovers parts.│ │
│  │  - Collects          │            │  - Returns water-  │ │
│  │    watermarks        │            │    marks per run   │ │
│  └─────────────────────┘            └──────────────────┘ │
│                                            │               │
│                                      ┌─────┴─────┐        │
│                                      │  Plugins   │        │
│                                      │ (bundled   │        │
│                                      │  or local) │        │
│                                      └───────────┘        │
└────────────────────────────────────────────────────────────┘
```

Why sidecar:

- **Shared lifecycle.** The pipelineR process starts and stops with the pod.
- **Localhost gRPC.** No network hop, no service discovery, no load balancer.
- **Shared filesystem.** Pipeline configs mounted as ConfigMaps, plugins in shared volumes.
- **No server infrastructure.** No separate Deployment, Service, or Ingress.

Tradeoffs:

- **No independent scaling.** pipelineR scales 1:1 with worker pods. More throughput = more workers.
- **Shared resource contention.** Mitigated by per-container resource requests/limits in the pod spec.
- **Sidecar restart impacts.** In-flight pipeline runs are lost. The orchestrator retries.

### 5.2 Orchestration Flow

The typical lifecycle of a pipeline execution:

```
Orchestrator                           pipelineR (any sidecar)
    │                                        │
    ├── DiscoverSchema(pipeline.toml) ──────▶│
    │◀── SchemaResponse(columns, types) ─────┤
    │                                        │
    ├── DiscoverPartitions(pipeline.toml) ──▶│
    │◀── PartitionsResponse([p1, p2, p3]) ───┤
    │                                        │
    │   ┌─ For each partition, fan out to available workers ─┐
    │   │                                                     │
    ├───┼── RunPipeline(pipeline.toml, partition=p1) ────▶ Worker A sidecar
    ├───┼── RunPipeline(pipeline.toml, partition=p2) ────▶ Worker B sidecar
    ├───┼── RunPipeline(pipeline.toml, partition=p3) ────▶ Worker C sidecar
    │   │                                                     │
    │   │   (each worker streams events via WatchRun)         │
    │   │                                                     │
    │◀──┼── RunStatus(ok, watermark="2026-03-25T23:59:59") ──┤
    │◀──┼── RunStatus(ok, watermark="2026-03-24T23:59:59") ──┤
    │◀──┼── RunStatus(ok, watermark="2026-03-23T23:59:59") ──┤
    │   └─────────────────────────────────────────────────────┘
    │
    ├── Compute final watermark = min(all partition watermarks)
    ├── Persist watermark for next run
    │
```

Key points:

- **Schema and partition discovery happen on any sidecar.** The orchestrator can call any available pipelineR instance — these are read-only operations that don't move data.
- **Partition runs are distributed across workers.** The orchestrator sends each partition to a different worker pod's sidecar. This is how parallelism is achieved without distributed execution inside pipelineR.
- **Watermarks are per-partition.** Each `RunStatus` includes the watermark that the source plugin computed for the data it actually processed. The orchestrator computes the final watermark (typically the minimum across all partitions).

### 5.3 Core Crate (`pipeliner-core`)

The engine library. Embeddable in any Rust application. Responsible for:

- Loading and validating pipeline configuration
- Discovering and connecting to source/sink plugins via gRPC
- Delegating schema discovery and partition discovery to source plugins
- Executing the transform pipeline (DSL interpreter)
- Managing record flow (streaming rows between source, transform, and sink)
- Internal backpressure between pipeline stages (bounded channels)
- Collecting execution metrics and watermarks
- Emitting telemetry via OpenTelemetry

### 5.4 CLI (`pipeliner`)

A thin wrapper around `pipeliner-core` that provides:

- `pipeliner run <pipeline.toml>` — execute a single pipeline (development and testing)
- `pipeliner validate <pipeline.toml>` — validate config without executing
- `pipeliner schema <pipeline.toml>` — discover and print source schema
- `pipeliner partitions <pipeline.toml>` — discover and print partitions
- `pipeliner plugins list` — list available/installed plugins
- `pipeliner plugins install <n>` — download and register a plugin from crate registry
- `pipeliner serve` — start the gRPC server (sidecar mode)

### 5.5 gRPC Orchestration API

The primary interface for orchestrator applications. Exposed on a configurable port (default: `50051`) when running in server mode (`pipeliner serve`).

```protobuf
service PipelineR {
  // Discover the schema of the source defined in the pipeline config.
  // Returns column names, types, and nullability.
  rpc DiscoverSchema(DiscoverSchemaRequest) returns (SchemaResponse);

  // Discover partitions for the source defined in the pipeline config.
  // Returns a list of partitions, each with a key and a params map.
  rpc DiscoverPartitions(DiscoverPartitionsRequest) returns (PartitionsResponse);

  // Submit a pipeline run. Returns immediately with a run ID.
  // Accepts optional partition params to scope the extraction.
  rpc RunPipeline(RunPipelineRequest) returns (RunPipelineResponse);

  // Stream pipeline status and metrics in real-time.
  rpc WatchRun(WatchRunRequest) returns (stream RunEvent);

  // Poll for current status of a pipeline run.
  // Includes watermark when the run completes successfully.
  rpc GetRunStatus(GetRunStatusRequest) returns (RunStatus);

  // Cancel a running pipeline.
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);

  // Validate a pipeline config without executing.
  rpc ValidatePipeline(ValidatePipelineRequest) returns (ValidationResult);

  // Health check (K8s liveness/readiness probes).
  rpc Health(HealthRequest) returns (HealthResponse);
}

message DiscoverSchemaRequest {
  // Pipeline config (inline TOML or file path).
  oneof config {
    string config_toml = 1;
    string config_path = 2;
  }
  // Optional runtime params (e.g., credentials).
  map<string, string> params = 3;
}

message SchemaResponse {
  repeated ColumnSchema columns = 1;
}

message ColumnSchema {
  string name = 1;
  string type = 2;       // "string", "int", "float", "bool", "timestamp", "bytes", "array", "map"
  bool nullable = 3;
  string description = 4; // optional, plugin-provided
}

message DiscoverPartitionsRequest {
  oneof config {
    string config_toml = 1;
    string config_path = 2;
  }
  map<string, string> params = 3;
}

message PartitionsResponse {
  repeated Partition partitions = 1;
}

message Partition {
  string key = 1;                    // unique identifier (e.g., "2026-03-25", "account-123")
  map<string, string> params = 2;    // params to pass to RunPipeline for this partition
}

message RunPipelineRequest {
  oneof config {
    string config_toml = 1;
    string config_path = 2;
  }
  map<string, string> params = 3;    // runtime params (merged with partition params)
}

message RunPipelineResponse {
  string run_id = 1;
}

message RunStatus {
  string run_id = 1;
  enum State {
    RUNNING = 0;
    COMPLETED = 1;
    FAILED = 2;
    CANCELLED = 3;
  }
  State state = 2;
  string error_message = 3;          // populated on FAILED
  RunMetrics metrics = 4;
  string watermark = 5;              // set by source plugin on COMPLETED
}

message RunMetrics {
  int64 records_read = 1;
  int64 records_written = 2;
  int64 records_dropped = 3;
  int64 transform_errors = 4;
  int64 duration_ms = 5;
}

message RunEvent {
  string run_id = 1;
  oneof event {
    StageTransition stage_transition = 2;
    BatchProgress batch_progress = 3;
    ErrorEvent error = 4;
    RunStatus completed = 5;
  }
}
```

Key design decisions:

- **`DiscoverSchema` delegates to the source plugin.** The orchestrator gets column names and types without extracting data. Useful for pre-creating sink tables, validation, or display.
- **`DiscoverPartitions` delegates to the source plugin.** The source knows its data layout — date ranges in SQL, object prefixes in S3, pagination in REST APIs. The orchestrator receives opaque `Partition` objects and distributes them without understanding the partitioning strategy.
- **Partitions are just params.** A `Partition` is a key (for tracking) and a `params` map. The orchestrator passes these params into `RunPipeline`. The source plugin interprets them to scope the extraction. This keeps the contract generic.
- **Watermarks are returned per run.** `RunStatus.watermark` is a string set by the source plugin representing the "high water mark" of the data it processed (e.g., a timestamp, an offset, a cursor). The orchestrator collects watermarks from all partition runs and computes the final watermark for persistence.
- **`WatchRun` is a server-streaming RPC.** Real-time events, acts as a heartbeat — if pipelineR crashes, the stream breaks immediately.
- **Concurrency.** The server accepts multiple concurrent `RunPipeline` calls. Each runs in its own Tokio task with isolated state.

### 5.6 Kubernetes Integration

```yaml
spec:
  containers:
    - name: worker
      image: myregistry/my-worker:latest
    - name: pipeliner
      image: myregistry/pipeliner:latest
      args: ["serve", "--port", "50051"]
      ports:
        - containerPort: 50051
          name: grpc
      resources:
        requests:
          memory: "128Mi"
          cpu: "100m"
        limits:
          memory: "1Gi"
          cpu: "2"
      volumeMounts:
        - name: pipeline-configs
          mountPath: /etc/pipeliner/pipelines
          readOnly: true
        - name: plugins
          mountPath: /etc/pipeliner/plugins
      env:
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: "http://otel-collector.monitoring:4317"
      livenessProbe:
        grpc:
          port: 50051
        initialDelaySeconds: 5
        periodSeconds: 10
      readinessProbe:
        grpc:
          port: 50051
        initialDelaySeconds: 3
        periodSeconds: 5
  initContainers:
    - name: install-plugins
      image: myregistry/pipeliner:latest
      command: ["pipeliner", "plugins", "install", "pipeliner-plugin-custom-api"]
      volumeMounts:
        - name: plugins
          mountPath: /etc/pipeliner/plugins
  volumes:
    - name: pipeline-configs
      configMap:
        name: pipeliner-pipelines
    - name: plugins
      emptyDir: {}
```

Key points:

- **Pipeline configs as ConfigMaps.** Pipeline TOML files are mounted read-only.
- **Secrets via environment variables.** Injected via K8s Secrets, resolved at runtime via `${VAR}` substitution.
- **Plugin installation via init container.** Custom plugins are installed at pod startup by running `pipeliner plugins install` in an init container. The plugin binaries are written to a shared `emptyDir` volume. Built-in plugins are baked into the pipelineR image.
- **Additional plugins can also be baked into a custom image.** For fully deterministic deployments, build a custom pipelineR image with `pipeliner plugins install` in the Dockerfile.
- **gRPC health probes.** Native K8s gRPC probes for liveness and readiness.
- **Graceful shutdown.** On `SIGTERM`, pipelineR stops accepting new runs, waits for in-flight runs to complete (configurable timeout), then exits.

---

## 6. Data Model

### 6.1 Internal Record Format

All data flowing through the pipeline is represented as a **stream of Records**, where each Record is a flexible key-value map with JSON-like semantics.

```
Record = Map<String, Value>

Value =
  | Null
  | Bool(bool)
  | Int(i64)
  | Float(f64)
  | String(String)
  | Bytes(Vec<u8>)
  | Timestamp(DateTime<Utc>)
  | Array(Vec<Value>)
  | Map(Map<String, Value>)
```

This model is chosen for maximum flexibility across diverse data sources:

- **Nested objects.** REST API responses with deeply nested JSON are represented naturally without flattening.
- **Mixed-type arrays.** Arrays where elements have different types (common in loosely-typed APIs) are supported.
- **Schema-optional.** Sources can emit records without declaring a schema upfront.
- **Binary data.** The `Bytes` variant supports binary payloads alongside structured fields.

### 6.2 Batching

Records are grouped into **RecordBatches** for efficient transport and processing:

- A batch is a `Vec<Record>` with a configurable maximum size (default: 1000 records).
- Sources emit batches. Transforms process one batch at a time. Sinks receive batches.
- Batching amortizes gRPC overhead and allows sinks to perform bulk operations.

### 6.3 Schema Handling

- **Schema is optional.** Sources *may* declare a schema (via `DiscoverSchema`) before emitting records. This enables early validation and sink optimization.
- **Schema-less is the default.** If a source does not declare a schema, the engine infers types from the first batch and treats subsequent batches permissively.
- **Type coercion is explicit.** The DSL provides `to_string`, `to_int`, `to_float`, `to_timestamp` etc. The engine does not silently coerce types between stages.

### 6.4 Serialization

Records are serialized over gRPC using Protocol Buffers:

```protobuf
message Record {
  map<string, Value> fields = 1;
}

message Value {
  oneof kind {
    google.protobuf.NullValue null_value = 1;
    bool bool_value = 2;
    int64 int_value = 3;
    double float_value = 4;
    string string_value = 5;
    bytes bytes_value = 6;
    google.protobuf.Timestamp timestamp_value = 7;
    ArrayValue array_value = 8;
    MapValue map_value = 9;
  }
}

message ArrayValue {
  repeated Value values = 1;
}

message MapValue {
  map<string, Value> fields = 1;
}

message RecordBatch {
  repeated Record records = 1;
}
```

---

## 7. Plugin SDK

### 7.1 Plugin Model

Plugins are **standalone binaries** that communicate with the engine over **gRPC**. This provides:

- **Language flexibility (future).** v1 SDK is Rust-only, but the gRPC contract allows future SDKs in other languages.
- **Process isolation.** A misbehaving plugin cannot crash the engine.
- **Independent versioning.** Plugins are versioned and released independently of the engine.
- **Distribution via crates.** Plugins are published as Rust crates and installed via `pipeliner plugins install <crate-name>`. Precompiled binaries are downloaded from crate release artifacts when available; compilation from source is the fallback.

### 7.2 Source Plugin Trait

A source plugin must implement:

```
trait Source {
    /// Return metadata: name, version, description, config schema.
    fn describe() -> SourceDescriptor;

    /// Validate the provided configuration. Called before execution.
    fn validate(config: SourceConfig) -> Result<(), ValidationError>;

    /// Discover the schema of the data this source produces.
    /// Returns None if the source is schema-less (e.g., untyped JSON).
    fn discover_schema(config: SourceConfig, params: RuntimeParams)
        -> Result<Option<Vec<ColumnSchema>>, DiscoveryError>;

    /// Discover how the dataset can be partitioned for parallel extraction.
    /// Returns a list of partitions, each with a key and params.
    /// Returns a single partition if the source does not support partitioning.
    fn discover_partitions(config: SourceConfig, params: RuntimeParams)
        -> Result<Vec<Partition>, DiscoveryError>;

    /// Open a connection and begin extracting data for the given params.
    /// Yields RecordBatches via a streaming gRPC response.
    /// Returns a watermark string representing the high-water mark of
    /// the data that was actually extracted.
    fn extract(config: SourceConfig, params: RuntimeParams)
        -> (Stream<Result<RecordBatch, ExtractionError>>, Watermark);
}
```

Key design decisions:

- **Schema discovery.** The orchestrator calls `DiscoverSchema` before extraction to learn column names and types. This is delegated to the source plugin, which connects to the data system and inspects metadata (e.g., `INFORMATION_SCHEMA` for SQL, response sampling for REST APIs).
- **Partition discovery.** The source plugin knows its data layout. A SQL plugin might partition by date range or key hash. A file plugin might partition by object prefix. A REST API plugin might partition by account or page range. The orchestrator receives opaque partitions and distributes them.
- **Single-partition fallback.** Sources that don't support partitioning return a single partition containing all params. The orchestrator treats this as "one run, no parallelism."
- **Watermark on extraction.** After streaming all records, the source plugin reports the watermark — the latest timestamp, offset, or cursor it processed. This allows the orchestrator to track progress without understanding the source's internal state model.
- **Streaming extraction.** Sources yield batches incrementally, keeping memory bounded.
- **Runtime parameters.** The orchestrator passes parameters per invocation (date ranges, cursors, partition params). The source plugin interprets them.

### 7.3 Sink Plugin Trait

```
trait Sink {
    /// Return metadata: name, version, description, config schema.
    fn describe() -> SinkDescriptor;

    /// Validate the provided configuration.
    fn validate(config: SinkConfig) -> Result<(), ValidationError>;

    /// Declare schema requirements (fixed schema, flexible, or "match source").
    fn schema_requirement() -> SchemaRequirement;

    /// Open a connection and begin loading data.
    /// Receives RecordBatches via a streaming gRPC request.
    /// Returns a LoadResult with rows written, errors, etc.
    fn load(config: SinkConfig, schema: Option<Vec<ColumnSchema>>, batches: Stream<RecordBatch>)
        -> Result<LoadResult, LoadError>;
}
```

Key design decisions:

- **Streaming load.** Sinks receive batches incrementally. A sink that supports bulk operations can buffer internally to its optimal batch size.
- **Row-to-columnar conversion.** Columnar sinks (Delta Lake, Parquet) are responsible for converting `RecordBatch` (row-based) to their internal columnar format. This conversion lives in each sink plugin, not in the engine.
- **Transactional semantics are sink-specific.** The engine does not impose a transaction model. Each sink documents its delivery guarantees.

### 7.4 Plugin Discovery & Registration

- Plugins are standalone binaries, stored in a configurable plugin directory (default: `/etc/pipeliner/plugins/`).
- A plugin registry file (`plugins.toml`) maps plugin names to binary paths and versions.
- `pipeliner plugins install <crate-name>` downloads precompiled binaries from release artifacts when available, falls back to compiling from source.
- Plugins can also be registered locally by path, for development and custom connectors.
- In K8s, plugins are installed via an init container or baked into a custom pipelineR Docker image.

---

## 8. Transform DSL

### 8.1 Design Principles

- **Not Turing-complete.** The DSL is an expression language for row-level and batch-level data transformations. No loops, recursion, or arbitrary control flow.
- **Safe by default.** All operations are fallible. Errors route the row to the dead-letter sink or drop it with a warning. Never panic.
- **Composable.** Transforms are a sequence of named steps. Each step receives the output of the previous step.
- **Nested-data aware.** The DSL natively supports accessing and manipulating nested objects and arrays.

### 8.2 Core Operations

| Category | Operations |
|---|---|
| **Field access** | `.field_name`, `.nested.path`, `.array[0]`, `.["key with spaces"]` |
| **Type coercion** | `to_string`, `to_int`, `to_float`, `to_bool`, `to_timestamp`, `to_bytes` |
| **Type checking** | `is_null`, `is_string`, `is_int`, `is_float`, `is_array`, `is_map` |
| **String** | `upcase`, `downcase`, `trim`, `replace`, `split`, `concat`, `starts_with`, `ends_with`, `contains`, `regex_match`, `regex_capture` |
| **Numeric** | `round`, `ceil`, `floor`, `abs`, `min`, `max`, `clamp` |
| **Temporal** | `parse_timestamp`, `format_timestamp`, `now`, `duration`, `add_duration`, `truncate_timestamp` |
| **Structural** | `rename`, `remove`, `set`, `coalesce`, `merge`, `flatten`, `select`, `reject`, `unnest` |
| **Array** | `push`, `length`, `map_values`, `filter_values`, `first`, `last` |
| **Conditional** | `if / else`, `match` (pattern matching on values), `null` coalescing (`??`) |
| **Aggregation** | `count`, `sum`, `avg`, `min`, `max`, `distinct` (batch-level) |
| **Filtering** | `where` (predicate to include/exclude rows) |
| **Routing** | `route` (split rows into named outputs based on conditions) |
| **Error handling** | `try / catch`, dead-letter routing for rows that fail transforms |

### 8.3 Example

```toml
[[transforms]]
name = "clean_transactions"
steps = [
  'rename(.transaction_date, .txn_date)',
  'rename(.transaction_amount, .amount)',
  'set(.txn_date, parse_timestamp(.txn_date, "%Y-%m-%d"))',
  'set(.amount, to_float(.amount))',
  'set(.merchant_name, .metadata.merchant.name ?? "Unknown")',
  'set(.tags, .metadata.tags)',
  'set(.currency, upcase(.currency ?? "USD"))',
  'set(.amount_usd, if .currency == "USD" { .amount } else { .amount * .exchange_rate })',
  'where(.amount_usd > 0.0)',
  'remove(.raw_payload)',
  'remove(.exchange_rate)',
  'remove(.metadata)',
]
```

---

## 9. Pipeline Configuration

A pipeline is a TOML file that declares a source, zero or more transforms, and one or more sinks.

### 9.1 Example Pipeline

```toml
[pipeline]
name = "daily_transactions"
description = "Extract transactions from payment API, transform, load to Delta Lake"

[source]
plugin = "rest-api"
config.base_url = "https://api.payments.example.com/v1"
config.auth.type = "oauth2_client_credentials"
config.auth.token_url = "https://api.payments.example.com/oauth/token"
config.auth.client_id = "${PAYMENTS_CLIENT_ID}"
config.auth.client_secret = "${PAYMENTS_CLIENT_SECRET}"
config.endpoint = "/transactions"
config.pagination.type = "cursor"
config.pagination.cursor_field = "next_cursor"
config.params.from_date = "${FROM_DATE}"
config.params.to_date = "${TO_DATE}"

[[transforms]]
name = "clean_transactions"
steps = [
  'rename(.transaction_date, .txn_date)',
  'set(.txn_date, parse_timestamp(.txn_date, "%Y-%m-%d"))',
  'set(.amount, to_float(.amount))',
  'where(.status != "voided")',
  'remove(.internal_metadata)',
]

[[sinks]]
plugin = "delta-lake"
config.uri = "abfss://data@storageaccount.dfs.core.windows.net/bronze/transactions"
config.mode = "append"
config.partition_by = ["txn_date"]

[[sinks]]
plugin = "sql"
config.driver = "sqlserver"
config.connection_string = "${SQL_CONNECTION_STRING}"
config.table = "staging.Transactions"
config.write_mode = "upsert"
config.merge_keys = ["transaction_id"]

[dead_letter]
plugin = "kafka"
config.brokers = "${KAFKA_BROKERS}"
config.topic = "etl.dead_letter.daily_transactions"
```

### 9.2 Configuration Features

- **Environment variable substitution.** `${VAR_NAME}` is resolved at runtime.
- **Runtime parameter overrides.** Params passed in `RunPipelineRequest.params` take precedence over environment variables. Partition params are merged automatically.
- **Multiple sinks.** Fan out to multiple sinks from a single source.
- **Dead-letter sink.** Configurable per pipeline. Rows that fail transforms are routed to the dead-letter sink with error context. If not configured, transform errors are logged and the row is dropped.
- **Secrets.** Via environment variables injected by K8s Secrets. No secrets in config files.

---

## 10. Built-in Plugins (v1)

These plugins ship with the binary as default plugins. They use the same SDK and gRPC protocol as community plugins.

### 10.1 Sources

| Plugin | Description |
|---|---|
| `rest-api` | Generic HTTP/REST source with configurable auth (API key, OAuth2 client credentials, Bearer token), pagination (cursor, offset, page-number, link-header), and response mapping. Partitions by configurable strategy (e.g., date range params, account list). |
| `sql` | Generic SQL source supporting parameterized queries. Drivers for Microsoft SQL Server (TDS), PostgreSQL, and MySQL. Partitions by date range, key range, or table partitions. Schema discovered from `INFORMATION_SCHEMA`. |
| `file` | Read CSV, JSON, or Parquet files from local filesystem, Azure Blob Storage / ADLS Gen2, AWS S3, or GCP GCS. Partitions by file/object (one partition per file or per prefix group). Schema inferred from first file or Parquet metadata. |

### 10.2 Sinks

| Plugin | Description |
|---|---|
| `delta-lake` | Write to Delta Lake tables on local filesystem or cloud object storage. Supports append, overwrite, and merge (upsert) modes. Partitioning support. Handles row-to-columnar conversion internally. Built on delta-rs. |
| `parquet` | Write Parquet files to local filesystem or cloud object storage. Handles row-to-columnar conversion internally. |
| `sql` | Generic SQL sink supporting bulk insert, upsert (merge), and truncate-and-load. Drivers for Microsoft SQL Server, PostgreSQL, and MySQL. |
| `file` | Write CSV or JSON to local filesystem or cloud object storage (Azure Blob / ADLS Gen2, AWS S3, GCP GCS). |

---

## 11. Observability

### 11.1 Logging

- Structured logging (JSON) to stdout/stderr, collected by the K8s log driver.
- Log levels: `error`, `warn`, `info`, `debug`, `trace`.
- Per-pipeline and per-stage context (pipeline name, run ID, source/sink plugin name, batch number, partition key).

### 11.2 OpenTelemetry

pipelineR exports telemetry via OpenTelemetry, configurable in the engine's global config file (`pipeliner.toml`):

```toml
[telemetry]
enabled = true
otlp_endpoint = "${OTEL_EXPORTER_OTLP_ENDPOINT}"
otlp_protocol = "grpc"
service_name = "pipeliner"
resource_attributes = { "deployment.environment" = "production", "k8s.pod.name" = "${POD_NAME}" }
export_traces = true
export_metrics = true
```

Alternatively, pipelineR respects standard OpenTelemetry environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, etc.).

#### Traces

Each pipeline run produces a trace with spans for:

- `pipeline.run` — root span (includes partition key as attribute)
- `source.extract` — extraction phase
- `transform.{step_name}` — one span per transform step
- `sink.load.{sink_name}` — loading phase per sink

Spans include error events with full context when a stage fails.

#### Metrics

| Metric | Type | Description |
|---|---|---|
| `pipeliner.pipeline.duration` | Histogram | Pipeline execution time |
| `pipeliner.source.records` | Counter | Total rows extracted |
| `pipeliner.source.bytes` | Counter | Total bytes extracted |
| `pipeliner.transform.records_in` | Counter | Rows entering transforms |
| `pipeliner.transform.records_out` | Counter | Rows exiting transforms |
| `pipeliner.transform.records_dropped` | Counter | Rows filtered or dead-lettered |
| `pipeliner.transform.errors` | Counter | Transform errors (step name label) |
| `pipeliner.sink.records` | Counter | Rows loaded (per sink label) |
| `pipeliner.sink.bytes` | Counter | Bytes loaded (per sink label) |
| `pipeliner.sink.duration` | Histogram | Time in sink (per sink label) |

### 11.3 Error Handling

- **Source errors** are retried with configurable backoff, then fail the pipeline. Spans annotated with error status.
- **Transform errors** route the row to the dead-letter sink (if configured) or drop with a warning log. The pipeline continues.
- **Sink errors** are retried with configurable backoff, then fail the pipeline. Partial writes documented in `RunStatus`.
- **Sidecar crash.** The orchestrator detects gRPC stream termination and retries the partition. Pipelines should be idempotent.

---

## 12. Security

- **No secrets in config.** Credentials via environment variables (K8s Secrets).
- **Plugin isolation.** Plugins run as separate processes.
- **Localhost-only by default.** gRPC binds to `127.0.0.1:50051`. Binding to `0.0.0.0` requires explicit opt-in.
- **gRPC metadata authentication.** Optional API key via metadata headers. Recommended in multi-tenant environments.
- **TLS for plugins.** gRPC between engine and out-of-process plugins supports mTLS.
- **Audit logging.** Every pipeline run logged with: run ID, pipeline name, partition key, parameters, start/end time, result, row counts, watermark. Also emitted as OTel trace events.

---

## 13. User Stories

### Pipeline Operator

1. **As an operator**, I can write a TOML file that defines a source, transforms, and a sink, and test it locally with `pipeliner run`.
2. **As an operator**, I can validate my pipeline config without executing it.
3. **As an operator**, I can inspect the source schema with `pipeliner schema` and the available partitions with `pipeliner partitions`.
4. **As an operator**, I can deploy pipeline configs as K8s ConfigMaps.
5. **As an operator**, I can fan out to multiple sinks from a single source without writing code.
6. **As an operator**, I can configure retry and dead-letter behavior per pipeline.
7. **As an operator**, I can configure OpenTelemetry export to send traces and metrics to my existing collector.
8. **As an operator**, I can install additional plugins via an init container or bake them into a custom Docker image.

### Connector Author

9. **As a connector author**, I can scaffold a new source plugin with `pipeliner sdk init --source` and implement one trait.
10. **As a connector author**, I can implement `discover_schema` to let orchestrators inspect my source's columns and types.
11. **As a connector author**, I can implement `discover_partitions` to let orchestrators parallelize extraction.
12. **As a connector author**, I can return a watermark from `extract` so the orchestrator can track progress.
13. **As a connector author**, I can emit Records with nested objects and arrays without flattening.
14. **As a connector author**, I can publish my plugin as a Rust crate.
15. **As a connector author**, I can test my plugin locally by registering it by path.

### Orchestrator Application

16. **As an orchestrator**, I can call `DiscoverSchema` to learn the source's columns and types before extraction.
17. **As an orchestrator**, I can call `DiscoverPartitions` to get a list of partitions without understanding the source's internal data layout.
18. **As an orchestrator**, I can fan out partitions to multiple worker pods' sidecars in parallel.
19. **As an orchestrator**, I can open a `WatchRun` stream per partition for real-time progress.
20. **As an orchestrator**, I can collect watermarks from all partition runs and compute the final watermark.
21. **As an orchestrator**, I can persist the final watermark and pass it as a parameter on the next run for incremental extraction.
22. **As an orchestrator**, I can cancel a running pipeline via `CancelRun`.
23. **As an orchestrator**, I can detect a sidecar crash via gRPC stream termination and retry the partition.
24. **As an orchestrator**, I can verify sidecar readiness via the `Health` RPC or K8s readiness probe.

---

## 14. Constraints & Technical Decisions

| Decision | Rationale |
|---|---|
| **Rust** | Predictable performance, no GC, single binary, native delta-rs interop. |
| **Row-based data model** | Maximum flexibility for diverse sources. Nested objects, mixed-type arrays, and schema-optional sources are first-class. |
| **gRPC only (no HTTP)** | Strongly typed contract, server-streaming for real-time events, efficient binary serialization, native K8s health probes. HTTP adds scope without value for a sidecar. |
| **Sidecar deployment** | Shared lifecycle, localhost communication, shared filesystem, no additional K8s infrastructure. |
| **Source-owned partition discovery** | The source plugin already connects to the data system and understands its data layout. The orchestrator stays source-agnostic. |
| **Source-reported watermarks** | The source plugin knows what data it actually extracted. The orchestrator computes the final watermark and persists it. |
| **Orchestrator-owned state** | pipelineR is stateless between runs. The orchestrator tracks cursors, watermarks, and incremental extraction state. |
| **TOML for config** | Readable, well-specified, good Rust ecosystem support. Less ambiguous than YAML. |
| **Custom DSL over SQL** | Focused on row-level transforms. Simple, safe, fast. SQL may be added later for batch-level aggregation. |
| **Process-isolated plugins** | Safety and fault isolation outweigh IPC overhead for batch workloads. |
| **Dead-letter per pipeline** | Each pipeline configures its own dead-letter sink. Flexible without baking a strategy into the engine. |
| **Plugin distribution via crates** | Leverages Rust's package ecosystem. Precompiled binaries where available, compile from source as fallback. |
| **Row-to-columnar at the sink** | Columnar sinks (Delta Lake, Parquet) handle conversion internally. Keeps the engine data-model-agnostic. |
| **OpenTelemetry for observability** | Industry standard. Integrates with existing collector infrastructure. Per-pipeline per-partition traces. |
