# pipelineR

A lightweight, config-driven batch ETL engine in Rust. Runs as a Kubernetes sidecar, exposes a gRPC API for orchestrators, and ships with built-in connectors for SQL databases, REST APIs, files, Delta Lake, and Parquet.

## Features

- **Config-driven pipelines** -- define ETL jobs entirely in TOML; no Rust code required
- **Plugin architecture** -- every source and sink is a connector binary that communicates over gRPC
- **Built-in transform DSL** -- rename, filter, coerce, and compute fields without writing code
- **Sidecar-native** -- runs alongside your orchestrator on `localhost:50051`, zero network infra
- **Source-aware partitioning** -- connectors discover how data should be split; orchestrators fan out runs
- **Bounded memory** -- streaming row batches with backpressure, not "load everything then transform"
- **OpenTelemetry** -- distributed traces and metrics exported via OTLP
- **Dead-letter routing** -- failed records go to a configurable sink instead of being silently dropped

## Quick start

### Build

```bash
cargo build --workspace
```

### Run a pipeline

Create a `pipeline.toml`:

```toml
[pipeline]
name = "csv-to-json"

[source]
connector = "file"
[source.config]
path = "input.csv"
format = "csv"

[[transforms]]
name = "clean"
steps = [
  'set(.amount, to_float(.amount))',
  'rename(.transaction_date, .date)',
  'where(.amount > 0.0)',
  'remove(.raw_payload)',
]

[[sinks]]
connector = "file"
[sinks.config]
path = "output.json"
format = "json"
```

Register connectors and run:

```bash
# Register the file connector
echo '[file]' > connectors.toml
echo 'path = "target/debug/pipeliner-connector-file"' >> connectors.toml

# Execute
pipeliner run pipeline.toml
```

### Start the gRPC server (sidecar mode)

```bash
pipeliner serve --port 50051
```

## CLI reference

```
pipeliner <COMMAND> [OPTIONS]

Commands:
  run          Execute a pipeline end-to-end
  validate     Validate a pipeline config (parse + check connectors)
  schema       Discover and print the source schema
  partitions   Discover and print source partitions
  serve        Start the gRPC server (sidecar mode)
  connectors   Connector management (list, install, remove)

Global options:
  --connectors-file <PATH>  Path to connectors.toml [default: connectors.toml]
  --log-level <LEVEL>       Log level: trace, debug, info, warn, error [default: info]
  --json-log                Output logs as structured JSON
```

### `pipeliner run`

```bash
pipeliner run pipeline.toml [--param key=value ...]
```

Execute a pipeline. Spawns source and sink connector processes, runs transforms, and reports results. Exit codes: `0` = success, `1` = pipeline failure, `2` = config error.

### `pipeliner validate`

```bash
pipeliner validate pipeline.toml
```

Parse the config, resolve connector binaries, and validate transform steps without executing.

### `pipeliner schema`

```bash
pipeliner schema pipeline.toml [--param key=value ...]
```

Connect to the source and print discovered column schemas as JSON.

### `pipeliner partitions`

```bash
pipeliner partitions pipeline.toml [--param key=value ...]
```

Connect to the source and print discovered partitions as JSON.

### `pipeliner serve`

```bash
pipeliner serve --port 50051
```

Start the gRPC server. Binds to `127.0.0.1` (localhost only). Supports concurrent pipeline runs, cancellation, and real-time event streaming. Graceful shutdown on SIGTERM with a 30-second drain timeout.

### `pipeliner connectors`

```bash
pipeliner connectors list                     # List registered connectors
pipeliner connectors install sql [--version]  # Install from crates.io
pipeliner connectors remove sql               # Unregister and delete
```

## Pipeline configuration

Pipeline files are TOML. Environment variables can be referenced as `${VAR_NAME}`.

```toml
[pipeline]
name = "my-pipeline"
description = "Optional description"

[source]
connector = "sql"       # Connector name from connectors.toml
[source.config]          # Connector-specific config (passed as JSON)
driver = "postgres"
connection_string = "${PG_CONN_STRING}"
query = "SELECT * FROM orders WHERE date >= '$start' AND date < '$end'"

[[transforms]]
name = "clean-and-coerce"
steps = [
  'rename(.order_date, .date)',
  'set(.amount, to_float(.amount))',
  'set(.currency, .currency ?? "USD")',
  'where(.amount > 0.0)',
  'remove(.internal_id)',
]

[[sinks]]
connector = "file"
[sinks.config]
path = "output.json"
format = "json"

[[sinks]]
connector = "parquet"
[sinks.config]
path = "/data/output.parquet"
compression = "snappy"

[dead_letter]
connector = "file"
[dead_letter.config]
path = "dead_letters.json"
format = "json"

[telemetry]
enabled = true
otlp_endpoint = "http://localhost:4317"
service_name = "pipeliner"
```

### Connector registry (`connectors.toml`)

Maps connector names to binary paths:

```toml
[file]
path = "/usr/local/bin/pipeliner-connector-file"
description = "File source/sink (CSV, JSON, Parquet)"

[sql]
path = "/usr/local/bin/pipeliner-connector-sql"
description = "SQL source/sink (PostgreSQL, MySQL, SQL Server)"

[rest]
path = "/usr/local/bin/pipeliner-connector-rest"
description = "REST API source"

[delta]
path = "/usr/local/bin/pipeliner-connector-delta"
description = "Delta Lake sink"

[parquet]
path = "/usr/local/bin/pipeliner-connector-parquet"
description = "Parquet sink"
```

If a connector is not in the registry, pipelineR falls back to searching for `pipeliner-connector-{name}` in the executable directory.

## Transform DSL

The DSL is a simple expression language for record-level transformations. It is not Turing-complete -- no loops, no recursion.

### Steps

| Step | Syntax | Description |
|------|--------|-------------|
| `set` | `set(.field, expr)` | Set a field to the result of an expression |
| `rename` | `rename(.old, .new)` | Rename a field |
| `remove` | `remove(.field)` | Remove a field |
| `where` | `where(predicate)` | Filter: keep records where predicate is true |

### Expressions

| Expression | Example | Description |
|------------|---------|-------------|
| Field access | `.name`, `.meta.key`, `.items[0]` | Read a field (nested paths, array indexing) |
| Literals | `42`, `3.14`, `"hello"`, `true`, `null` | Constant values |
| Arithmetic | `.a + .b`, `.x * 1.1` | `+`, `-`, `*`, `/` |
| Comparison | `.age > 18`, `.status == "active"` | `==`, `!=`, `>`, `>=`, `<`, `<=` |
| Logical | `.a && .b`, `.x \|\| .y` | AND, OR |
| Null coalesce | `.currency ?? "USD"` | Return left if non-null, else right |
| Conditional | `if .x > 0 { .x } else { 0 }` | If/else |

### Built-in functions

| Function | Example | Description |
|----------|---------|-------------|
| `to_string(val)` | `to_string(.age)` | Convert to string |
| `to_int(val)` | `to_int(.count)` | Convert to integer |
| `to_float(val)` | `to_float(.amount)` | Convert to float |
| `to_bool(val)` | `to_bool(.flag)` | Convert to boolean |
| `to_timestamp(str, fmt)` | `to_timestamp(.date, "%Y-%m-%d")` | Parse timestamp |
| `parse_timestamp(str, fmt)` | `parse_timestamp(.ts, "%+")` | Alias for `to_timestamp` |
| `is_null(val)` | `is_null(.field)` | Returns true if null |
| `concat(a, b, ...)` | `concat(.first, " ", .last)` | Concatenate strings |
| `coalesce(a, b, ...)` | `coalesce(.primary, .fallback)` | First non-null value |

## Built-in connectors

### File (`pipeliner-connector-file`)

**Source and sink.** Reads/writes CSV, JSON (newline-delimited), and Parquet.

Source config:
```toml
[source.config]
path = "data/*.csv"        # File path or glob pattern
format = "csv"             # csv, json, parquet
[source.config.csv]        # Optional CSV options
delimiter = ","
quote = '"'
has_header = true
```

Sink config:
```toml
[sinks.config]
path = "output.json"
format = "json"            # csv or json
```

Supports cloud storage: `s3://bucket/key`, `azure://container/blob`, `gs://bucket/object`. Configure backends via `[source.config.storage]`.

### SQL (`pipeliner-connector-sql`)

**Source and sink.** PostgreSQL, MySQL, SQL Server.

Source config:
```toml
[source.config]
driver = "postgres"                      # postgres, mysql, sqlserver
connection_string = "${DATABASE_URL}"
query = "SELECT * FROM orders WHERE date >= '$start' AND date < '$end'"
watermark_column = "updated_at"
[source.config.partition]
strategy = "date_range"                  # single, date_range, key_range
[source.config.partition.date_range]
column = "date"
start = "2024-01-01"
end = "2024-12-31"
interval = "7d"                          # d=days, h=hours, w=weeks, m=months
```

Sink config:
```toml
[sinks.config]
driver = "postgres"
connection_string = "${DATABASE_URL}"
table = "target_table"
write_mode = "upsert"                   # insert, upsert, truncate_and_load
merge_keys = ["id"]
batch_size = 1000
```

### REST API (`pipeliner-connector-rest`)

**Source only.** Fetches JSON data from HTTP APIs with pagination, authentication, and rate limiting.

```toml
[source.config]
base_url = "https://api.example.com"
endpoint = "/api/v1/users"
[source.config.auth]
type = "oauth2"
token_url = "https://auth.example.com/token"
client_id = "${CLIENT_ID}"
client_secret = "${CLIENT_SECRET}"
[source.config.pagination]
type = "cursor"
cursor_field = "next_cursor"
cursor_param = "cursor"
[source.config.response_mapping]
records_path = "data.items"
[source.config.rate_limit]
requests_per_second = 10.0
[source.config.retry]
max_retries = 3
initial_backoff_ms = 1000
max_backoff_ms = 30000
```

Auth types: `api_key`, `bearer`, `oauth2`. Pagination types: `cursor`, `offset`, `page_number`, `link_header`. Retries on HTTP 429, 500, 502, 503, 504 with exponential backoff.

### Delta Lake (`pipeliner-connector-delta`)

**Sink only.** Writes to Delta Lake tables.

```toml
[sinks.config]
table_uri = "/data/delta/my_table"       # Local or cloud URI (s3://, az://, gs://)
write_mode = "append"                    # append, overwrite, merge
partition_columns = ["date", "region"]   # Hive-style partitioning
# For merge mode:
# merge_keys = ["id"]
# merge_predicate = "target.id = source.id"
```

### Parquet (`pipeliner-connector-parquet`)

**Sink only.** Writes Parquet files with configurable compression.

```toml
[sinks.config]
path = "/data/output.parquet"
compression = "snappy"                   # none, snappy, zstd, gzip
partition_columns = ["region"]           # Hive-style col=val/ directories
```

## gRPC API

When running in server mode (`pipeliner serve`), pipelineR exposes the `PipelineR` gRPC service:

| RPC | Type | Description |
|-----|------|-------------|
| `DiscoverSchema` | Unary | Returns source column schemas |
| `DiscoverPartitions` | Unary | Returns partition keys from the source |
| `RunPipeline` | Unary | Submit a pipeline run; returns a run ID immediately |
| `WatchRun` | Server-streaming | Stream real-time events (stage transitions, batch progress, errors, completion) |
| `GetRunStatus` | Unary | Poll current run status with metrics and watermark |
| `CancelRun` | Unary | Cancel a running pipeline |
| `ValidatePipeline` | Unary | Validate config without executing |
| `Health` | Unary | K8s liveness/readiness probe |

Proto definitions are in `pipeliner-proto/proto/pipeliner/v1/service.proto`.

### Orchestrator flow

```
1. DiscoverSchema(pipeline.toml)        -> column names and types
2. DiscoverPartitions(pipeline.toml)    -> [{key: "2024-01..02", params: {start, end}}, ...]
3. For each partition:
     RunPipeline(pipeline.toml, params) -> run_id
     WatchRun(run_id)                   -> stream of events until completion
     GetRunStatus(run_id)               -> final watermark
4. Persist watermarks for next incremental run
```

## Observability

### Tracing

When `[telemetry]` is enabled, pipelineR exports OpenTelemetry traces via OTLP:

- **Root span:** `pipeline_run` with attributes: `pipeline_name`, `run_id`, `partition_key`, `records_read`
- **Child spans:** `source_extract`, `transform`, `sink_load` (per sink)
- **Error events:** attached to spans on failure

### Metrics

Counters and histograms exported via OTLP:

| Metric | Type | Description |
|--------|------|-------------|
| `pipeline.records_read` | Counter | Records extracted from source |
| `pipeline.records_written` | Counter | Records written to sinks |
| `pipeline.records_dropped` | Counter | Records dropped due to errors |
| `pipeline.transform_errors` | Counter | Records that failed transform steps |
| `pipeline.duration_seconds` | Histogram | Pipeline execution duration |

### Configuration

```toml
[telemetry]
enabled = true
otlp_endpoint = "http://localhost:4317"  # Or set OTEL_EXPORTER_OTLP_ENDPOINT
service_name = "pipeliner"
```

## Dead-letter routing

Records that fail during transform steps are routed to a configurable dead-letter sink. Failed records are enriched with `__error_step`, `__error_message`, and `__error_timestamp` fields.

```toml
[dead_letter]
connector = "file"
[dead_letter.config]
path = "dead_letters.json"
format = "json"
```

Any connector can serve as a dead-letter destination. If no dead-letter sink is configured, failed records are logged and dropped.

## Project structure

```
pipeliner-core/                  # Engine library (config, runtime, DSL, plugin manager)
pipeliner-cli/                   # CLI binary
pipeliner-sdk/                   # Plugin SDK (Source/Sink traits, gRPC scaffolding)
pipeliner-proto/                 # Protobuf definitions
pipeliner-arrow-convert/         # Record -> Arrow RecordBatch conversion
connectors/
  pipeliner-connector-file/      # File source/sink (CSV, JSON, Parquet, cloud storage)
  pipeliner-connector-sql/       # SQL source/sink (PostgreSQL, MySQL, SQL Server)
  pipeliner-connector-rest/      # REST API source
  pipeliner-connector-delta/     # Delta Lake sink
  pipeliner-connector-parquet/   # Parquet sink
```

## Development

```bash
cargo build --workspace          # Build everything
cargo test --workspace           # Run all tests
cargo clippy --workspace -- -D warnings  # Lint
```

Run a single connector's tests:

```bash
cargo test -p pipeliner-connector-sql
```

## License

See [LICENSE](LICENSE) for details.
