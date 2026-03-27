# pipelineR — Implementation Plan

## Overview

This plan breaks pipelineR into 8 milestones, ordered by dependency. Each milestone produces a working, testable artifact. The principle is: get a vertical slice running end-to-end as early as possible, then widen the surface area.

```
M1  Core data model + DSL interpreter
 │
M2  Plugin SDK + gRPC protocol
 │
M3  First source plugin (file/CSV)
 │
M4  First sink plugin (file/CSV) ← first E-T-L pipeline works here
 │
M5  CLI + pipeline config loader
 │
M6  gRPC server (sidecar mode) + schema/partition discovery
 │
M7  Production plugins (SQL, REST API, Delta Lake, Parquet)
 │
M8  Observability (OpenTelemetry) + dead-letter + polish
```

---

## Milestone 1 — Core data model and DSL interpreter

**Goal:** The `Record` type exists, and you can parse and execute transform steps against a `Vec<Record>` in a unit test.

**Deliverables:**

- `pipeliner-core` crate scaffold (workspace with `pipeliner-core`, `pipeliner-cli`, `pipeliner-sdk`)
- `Record` and `Value` types as defined in the PRD (Map, nested values, all variants)
- `RecordBatch` type (`Vec<Record>` + batch metadata)
- DSL parser — parse a single transform step string (e.g., `'set(.amount, to_float(.amount))'`) into an AST
- DSL interpreter — execute a parsed step against a `Record`, returning a modified `Record` or an error
- Core DSL operations implemented: field access (including nested paths, array indexing), `set`, `remove`, `rename`, `to_string`, `to_int`, `to_float`, `to_bool`, `to_timestamp`, `where`, `if/else`, `??` (null coalescing)
- Unit tests for each operation, including error cases (null access, type mismatch)

**Not yet:** No gRPC, no plugins, no CLI, no config files. Everything is tested via Rust unit tests.

**Exit criteria:** `cargo test` passes. You can construct a `RecordBatch`, apply a sequence of transform steps, and assert on the output records.

---

## Milestone 2 — Plugin SDK and gRPC protocol

**Goal:** Define the plugin contract. A source or sink plugin can be a separate binary that talks to the engine over gRPC.

**Deliverables:**

- `pipeliner-sdk` crate with the `Source` and `Sink` traits as defined in the PRD
- Protobuf definitions for all plugin-facing messages: `Record`, `Value`, `RecordBatch`, `SourceConfig`, `SinkConfig`, `RuntimeParams`, `ColumnSchema`, `Partition`, `Watermark`, `LoadResult`, `ExtractionError`, `LoadError`
- Protobuf service definitions for source plugins (`SourcePlugin` service) and sink plugins (`SinkPlugin` service)
- gRPC server scaffold in the SDK — a helper that takes a `Source` or `Sink` impl and runs it as a gRPC server (so plugin authors write a trait impl, not gRPC boilerplate)
- gRPC client in `pipeliner-core` — the engine side that connects to a plugin binary, calls `describe`, `validate`, `discover_schema`, `discover_partitions`, `extract`, `load`
- Plugin process management — engine spawns a plugin binary as a child process, connects to its gRPC port, and manages its lifecycle
- A dummy/mock source plugin (emits hardcoded records) and a dummy sink plugin (receives and discards records) for integration testing

**Not yet:** No real connectors. No orchestrator-facing gRPC API. No CLI.

**Exit criteria:** An integration test spawns a mock source plugin process, the engine connects to it via gRPC, calls `extract`, receives a stream of `RecordBatch`es, runs DSL transforms from M1, and sends the results to a mock sink plugin.

---

## Milestone 3 — File source plugin

**Goal:** The first real source plugin that reads actual data from disk.

**Deliverables:**

- `pipeliner-connector-file` crate implementing the `Source` trait
- CSV reader — reads a CSV file, emits `RecordBatch`es with string values (type coercion handled by DSL transforms)
- JSON reader — reads newline-delimited JSON, emits records preserving nested structure
- Parquet reader — reads Parquet files, maps Parquet types to `Value` variants
- `discover_schema` implementation: infer from CSV headers, JSON sampling, or Parquet metadata
- `discover_partitions` implementation: one partition per file (or per glob match)
- Watermark: returns the last-modified timestamp of the file(s) read
- Local filesystem support only (cloud storage comes in M7)
- Config schema: `path` (file or glob), `format` (csv/json/parquet), CSV-specific options (delimiter, quote, header)

**Exit criteria:** Integration test reads a CSV file through the file source plugin → applies DSL transforms → sends to mock sink. Schema discovery returns correct column names.

---

## Milestone 4 — File sink plugin

**Goal:** First complete E-T-L pipeline. Data goes from a file, through transforms, and out to a file.

**Deliverables:**

- `pipeliner-connector-file` extended with the `Sink` trait (same crate, both source and sink)
- CSV writer — receives `RecordBatch`es, writes CSV
- JSON writer — writes newline-delimited JSON preserving nested structure
- Local filesystem only
- Config schema: `path`, `format` (csv/json), CSV-specific options
- Pipeline runtime in `pipeliner-core`: the orchestration loop that wires source → transform chain → sink(s)
- Multiple sink fan-out: engine clones each batch and delivers to each configured sink
- Backpressure: bounded async channel between source, transform, and sink stages

**Exit criteria:** A Rust integration test defines a pipeline programmatically (source=CSV file, transforms=rename+filter+coerce, sinks=JSON file + CSV file), executes it via `pipeliner-core` API, and asserts on the output files. This is the first time the full E-T-L loop runs.

---

## Milestone 5 — CLI and pipeline config loader

**Goal:** Pipelines are defined in TOML files and executed from the command line.

**Deliverables:**

- TOML config parser — parse `pipeline.toml` into the internal pipeline definition (source config, transform steps, sink configs, dead-letter config)
- Environment variable substitution (`${VAR_NAME}`) in config values
- Runtime parameter overrides via CLI flags (`--param key=value`)
- Plugin discovery — read `connectors.toml` registry, resolve connector names to binary paths
- CLI commands:
  - `pipeliner run <pipeline.toml>` — execute a pipeline end-to-end
  - `pipeliner validate <pipeline.toml>` — parse config + call plugin `validate` methods
  - `pipeliner schema <pipeline.toml>` — call source plugin's `discover_schema`, print results
  - `pipeliner partitions <pipeline.toml>` — call source plugin's `discover_partitions`, print results
  - `pipeliner connectors list` — list registered plugins
- Structured JSON logging to stdout/stderr with log level filtering
- Exit code conventions: 0 = success, 1 = pipeline failure, 2 = config error

**Exit criteria:** From a terminal: `pipeliner run pipeline.toml` reads a CSV, transforms it, writes JSON. `pipeliner schema pipeline.toml` prints the source columns. `pipeliner validate pipeline.toml` catches a bad config and exits with code 2.

---

## Milestone 6 — gRPC server (sidecar mode) and orchestration API

**Goal:** pipelineR runs as a long-lived gRPC server that an external application can call to execute pipelines.

**Deliverables:**

- `pipeliner serve --port 50051` starts the gRPC server
- Orchestrator-facing protobuf service (`PipelineR` service) as defined in the PRD:
  - `DiscoverSchema` — delegates to source plugin, returns `SchemaResponse`
  - `DiscoverPartitions` — delegates to source plugin, returns `PartitionsResponse`
  - `RunPipeline` — accepts inline TOML or file path + params, returns `RunPipelineResponse` with run ID
  - `WatchRun` — server-streaming RPC, emits `RunEvent`s (stage transitions, batch progress, errors, completion with watermark)
  - `GetRunStatus` — poll-based status check, returns `RunStatus` with watermark
  - `CancelRun` — sets a cancellation token, pipeline stages check it between batches
  - `ValidatePipeline` — delegates to config parser + plugin validate methods
  - `Health` — returns serving status (for K8s probes)
- Concurrent pipeline execution — each `RunPipeline` call spawns a Tokio task with isolated state
- Cancellation propagation — `CancelRun` triggers a `CancellationToken` that the pipeline runtime checks between stages and between batches
- Run ID generation and run state tracking (in-memory, lost on restart — this is fine for a sidecar)
- Watermark propagation — source plugin's watermark is included in `RunStatus` on completion

**Exit criteria:** A test gRPC client (written in Rust, simulating the C# orchestrator) calls `DiscoverPartitions`, then `RunPipeline` for each partition, opens `WatchRun` streams, receives completion events with watermarks. A separate test verifies `CancelRun` stops a slow pipeline.

---

## Milestone 7 — Production plugins

**Goal:** The built-in connectors from the PRD are implemented. pipelineR can talk to real data systems.

This milestone has four parallel tracks. Each can be developed and tested independently.

### M7a — SQL source and sink

- `pipeliner-connector-sql` crate
- Driver abstraction over SQL Server (TDS via `tiberius`), PostgreSQL (`tokio-postgres`), MySQL (`mysql_async`)
- Source: parameterized `SELECT` queries, streaming result sets as `RecordBatch`es
- Source `discover_schema`: queries `INFORMATION_SCHEMA.COLUMNS`
- Source `discover_partitions`: configurable strategies — date range chunking (given a date column + range, emit N partitions), key range, or single partition
- Source watermark: max value of a configured watermark column from the extracted data
- Sink: bulk insert, upsert (merge), truncate-and-load modes
- Sink handles row-to-SQL type mapping from `Value` variants
- Connection pooling within a single pipeline run
- Config schema: `driver`, `connection_string`, `query` (source) or `table` + `write_mode` + `merge_keys` (sink)

### M7b — REST API source

- `pipeliner-connector-rest` crate
- Configurable authentication: API key (header or query param), OAuth2 client credentials (machine-to-machine token fetch), Bearer token
- Configurable pagination: cursor-based, offset-based, page-number, link-header (RFC 8288)
- Response mapping: JSONPath-like config to extract the record array and pagination fields from the response body
- `discover_schema`: optional — samples the first page of results and infers schema. Returns `None` if disabled.
- `discover_partitions`: configurable — can partition by a list of param values (e.g., account IDs), date range chunks, or single partition
- Watermark: configurable — max value of a specified field, or last cursor value
- Rate limiting: configurable requests-per-second with backoff on 429 responses
- Config schema: `base_url`, `endpoint`, `auth`, `pagination`, `response_mapping`, `rate_limit`

### M7c — Cloud storage for file plugin

- Extend `pipeliner-connector-file` with cloud storage backends
- Azure Blob Storage / ADLS Gen2 (via `azure_storage_blobs` crate)
- AWS S3 (via `aws-sdk-s3` crate)
- GCP GCS (via `cloud-storage` crate)
- `discover_partitions`: list objects by prefix, one partition per object or per prefix group
- Authentication: environment variables (connection string for Azure, AWS credentials chain for S3, service account for GCS)
- Source and sink both get cloud support

### M7d — Delta Lake and Parquet sinks

- `pipeliner-connector-delta` crate — sink only
- Built on `deltalake` (delta-rs) crate
- Append, overwrite, and merge (upsert) write modes
- Partition-by columns support
- Row-to-Arrow conversion: convert `Vec<Record>` to Arrow `RecordBatch` using inferred or declared schema. This is the critical piece — map each `Value` variant to the appropriate Arrow type, handle nested structures by flattening or storing as JSON strings (configurable)
- Cloud storage support (same backends as file plugin)
- `pipeliner-connector-parquet` crate — sink only
- Same row-to-Arrow conversion as Delta Lake plugin (shared utility crate: `pipeliner-arrow-convert`)
- Writes Parquet files with configurable compression (snappy, zstd, gzip)
- Cloud storage support

**Exit criteria per track:**

- M7a: Integration test connects to a real SQL Server (Docker), reads from a table with parameterized date range, writes to another table via upsert. Partitioned run works.
- M7b: Integration test hits a mock HTTP server (wiremock-rs), paginates through multiple pages, handles OAuth token refresh.
- M7c: Integration test reads from / writes to MinIO (S3-compatible, in Docker). Azure and GCS tested against emulators or with `#[ignore]` tests for CI.
- M7d: Integration test writes a Delta Lake table to local filesystem, reads it back with `deltalake` crate to verify. Merge mode tested with overlapping data.

---

## Milestone 8 — Observability, dead-letter, and polish

**Goal:** Production readiness. Proper telemetry, error routing, plugin installation, and hardening.

**Deliverables:**

### Observability

- OpenTelemetry integration in `pipeliner-core`
- Tracing: root span per pipeline run, child spans per stage (source, each transform step, each sink), error events on spans
- Metrics: all counters and histograms from the PRD (records, bytes, duration, errors) via OTLP export
- Configuration via `pipeliner.toml` `[telemetry]` section and standard OTel environment variables
- Trace context propagation: run ID and partition key as span attributes for correlation

### Dead-letter routing

- Dead-letter sink configuration in pipeline TOML (`[dead_letter]` section)
- When a transform step fails on a record, the original record + error context (step name, error message, timestamp) are routed to the dead-letter sink
- Dead-letter sink uses the same `Sink` plugin trait — any sink plugin can be a dead-letter destination (Kafka, file, SQL, etc.)
- If no dead-letter sink is configured, transform errors are logged and the record is dropped

### Connector installation

- `pipeliner connectors install <crate-name>` command
- Download precompiled binary from crate release artifacts (GitHub releases convention: `pipeliner-connector-<name>-<target>-<version>.tar.gz`)
- Fallback: `cargo install` from source if no precompiled binary is found
- Binary placed in connector directory, `connectors.toml` updated
- `pipeliner connectors remove <name>` — unregister and delete binary

### Hardening

- Graceful shutdown on `SIGTERM` — stop accepting new runs, drain in-flight runs with configurable timeout
- gRPC server binds to `127.0.0.1` by default (localhost-only)
- Input validation on all gRPC request fields
- Config validation: catch common mistakes early (missing required fields, invalid connector names, unreachable endpoints)
- Error messages: every error surfaced to the orchestrator includes enough context to diagnose without reading pipelineR logs (plugin name, config field, underlying error)
- Integration test suite that runs a full pipeline through the gRPC API (simulating the orchestrator flow from the PRD sequence diagram)
- README, architecture doc, plugin authoring guide

**Exit criteria:** A full end-to-end test: gRPC client calls `DiscoverSchema` → `DiscoverPartitions` → fans out `RunPipeline` for 3 partitions → `WatchRun` streams events including a transform error routed to a dead-letter file → all runs complete with watermarks → traces visible in a local Jaeger instance → metrics visible in a local Prometheus.

---

## Dependency Graph

```
M1 ─────────▶ M2 ─────────▶ M3 ─────────▶ M4 ─────────▶ M5 ─────────▶ M6
              (SDK)         (file src)     (file sink)   (CLI)         (gRPC server)
                                                                         │
                                                                         ▼
                                                               M7a  M7b  M7c  M7d
                                                               (SQL)(REST)(Cloud)(Delta)
                                                                ──── parallel ────
                                                                         │
                                                                         ▼
                                                                        M8
                                                                   (OTel, DLQ,
                                                                    polish)
```

M1 through M6 are strictly sequential — each depends on the previous. M7's tracks are parallel and can be developed in any order. M8 depends on M6 (gRPC server) and at least one M7 track (to have a real plugin to instrument).

---

## Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| **SQL Server TDS client (`tiberius`) maturity** | M7a could stall on driver bugs or missing features (e.g., bulk copy, specific auth modes) | Evaluate `tiberius` early (spike in M2 timeframe). Fallback: ODBC via `odbc-api` crate. |
| **delta-rs row-to-Arrow conversion complexity** | M7d's `Record` → Arrow `RecordBatch` conversion for nested/mixed-type data could be surprisingly hard | Build `pipeliner-arrow-convert` as a shared utility. Start with flat schemas, add nested support iteratively. Accept "flatten or store as JSON string" as the v1 escape hatch. |
| **DSL scope creep** | Users will want more operations than what M1 ships. Easy to spend unbounded time on the DSL. | Ship M1 with the minimal set from the PRD. Add operations only when a real plugin or pipeline needs them. |
| **gRPC streaming complexity** | `WatchRun` server-streaming + concurrent runs + cancellation is the hardest systems code in the project | Prototype the streaming + cancellation pattern in M6 with mock pipelines before wiring real plugins. Use `tokio-util`'s `CancellationToken`. |
| **Plugin process management** | Spawning, health-checking, and restarting plugin child processes reliably across platforms | Keep it simple in v1: spawn on first use, kill on engine shutdown. No hot-reload, no restart-on-crash. |
| **Solo developer bandwidth** | M7 has four parallel tracks but one developer | Prioritize M7a (SQL) first — it's the highest-value connector and validates the hardest plugin patterns. Then M7c (cloud storage), M7d (Delta/Parquet), M7b (REST API) last (REST API source is the most complex but least risky). |
