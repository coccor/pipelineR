# pipeliner — AI Pipeline Creator Guide

This file teaches AI assistants how to create pipeliner pipeline configurations.

## What is pipeliner?

A config-driven batch ETL engine. Pipelines are defined entirely in TOML — no code required. Each pipeline reads from a **source**, applies **transforms** using a built-in DSL, and writes to one or more **sinks**.

## Quick Start: Creating a Pipeline

To create a pipeline, generate a `.toml` file with this structure:

```toml
[pipeline]
name = "my_pipeline"
description = "What this pipeline does"

[source]
connector = "file"        # or "sql", "rest"
[source.config]
# connector-specific config here

[[transforms]]
name = "transform_group_name"
steps = [
    'set(.field, expression)',
    'where(.field > 0)',
]

[[sinks]]
connector = "file"        # or "sql", "delta", "parquet"
[sinks.config]
# connector-specific config here
```

## Discovery: Use `pipeliner ai-spec`

Run `pipeliner ai-spec` to get a machine-readable JSON specification of all available connectors, their config options, DSL functions, and examples. This is the best way to discover what's available at runtime.

Run `pipeliner connectors list` to see which connectors are installed.

## Available Connectors

### Source Connectors

#### `file` — Read CSV, JSON, or Parquet files
```toml
[source]
connector = "file"
[source.config]
path = "data/input.csv"          # file path or glob pattern
format = "csv"                    # "csv", "json", or "parquet"

# Optional CSV options:
[source.config.csv]
delimiter = ","
quote = '"'
has_header = true

# Optional cloud storage:
[source.config.storage]
type = "s3"                       # "local", "s3", "azure", "gcs"
region = "us-east-1"              # S3 only
# endpoint = "http://localhost:9000"  # S3-compatible endpoints
```

#### `sql` — Query PostgreSQL, MySQL, or SQL Server
```toml
[source]
connector = "sql"
[source.config]
driver = "postgres"               # "postgres", "mysql", "sqlserver"
connection_string = "${DATABASE_URL}"  # use env vars for secrets
query = "SELECT * FROM orders WHERE created_at > '2024-01-01'"
watermark_column = "created_at"   # optional, for incremental loads

# Optional partitioning:
[source.config.partition]
strategy = "date_range"           # "single", "date_range", "key_range"
[source.config.partition.date_range]
column = "created_at"
start = "2024-01-01"
end = "2024-12-31"
interval = "1m"                   # "1d", "7d", "1m"
```

#### `rest` — Fetch from REST APIs
```toml
[source]
connector = "rest"
[source.config]
base_url = "https://api.example.com"
endpoint = "/v1/records"
method = "GET"                    # "GET" or "POST"

[source.config.headers]
Accept = "application/json"

[source.config.query_params]
limit = "100"

# Authentication (pick one):
[source.config.auth]
type = "bearer"
token = "${API_TOKEN}"
# OR:
# type = "api_key"
# header = "X-API-Key"
# value = "${API_KEY}"
# OR:
# type = "oauth2"
# token_url = "https://auth.example.com/token"
# client_id = "${CLIENT_ID}"
# client_secret = "${CLIENT_SECRET}"

# Pagination (pick one):
[source.config.pagination]
type = "cursor"
cursor_field = "next_cursor"
cursor_param = "cursor"
# OR:
# type = "offset"
# offset_param = "offset"
# limit_param = "limit"
# limit = 100

[source.config.response_mapping]
records_path = "data.items"       # dot-path to the records array
total_path = "meta.total"         # optional

[source.config.rate_limit]
requests_per_second = 10.0

[source.config.retry]
max_retries = 3
initial_backoff_ms = 1000
max_backoff_ms = 30000
```

### Sink Connectors

#### `file` — Write CSV or JSON files
```toml
[[sinks]]
connector = "file"
[sinks.config]
path = "output/results.json"
format = "json"                   # "csv" or "json"
```

#### `sql` — Write to PostgreSQL, MySQL, or SQL Server
```toml
[[sinks]]
connector = "sql"
[sinks.config]
driver = "postgres"
connection_string = "${DATABASE_URL}"
table = "target_table"
write_mode = "upsert"            # "insert", "upsert", "truncate_and_load"
merge_keys = ["id"]              # required for upsert
batch_size = 1000
```

#### `delta` — Write to Delta Lake tables
```toml
[[sinks]]
connector = "delta"
[sinks.config]
table_uri = "s3://bucket/warehouse/table"  # or local path
write_mode = "append"            # "append", "overwrite", "merge"
partition_columns = ["year", "month"]
merge_keys = ["id"]              # required for merge mode
```

#### `parquet` — Write Parquet files
```toml
[[sinks]]
connector = "parquet"
[sinks.config]
path = "output/data.parquet"
compression = "snappy"           # "snappy", "zstd", "gzip", "none"
partition_columns = ["region"]
```

## Transform DSL

### Step Types

| Step | Syntax | Description |
|------|--------|-------------|
| `set` | `set(.field, expr)` | Compute expression, assign to field |
| `rename` | `rename(.old_name, .new_name)` | Rename a field |
| `remove` | `remove(.field)` | Delete a field |
| `where` | `where(predicate)` | Keep only records matching predicate |

### Built-in Functions

| Function | Example | Description |
|----------|---------|-------------|
| `to_string(val)` | `to_string(.id)` | Convert to string |
| `to_int(val)` | `to_int(.quantity)` | Convert to integer |
| `to_float(val)` | `to_float(.price)` | Convert to float |
| `to_bool(val)` | `to_bool(.active)` | Convert to boolean |
| `to_timestamp(str, fmt)` | `to_timestamp(.date, "%Y-%m-%d")` | Parse timestamp |
| `parse_timestamp(str, fmt)` | `parse_timestamp(.ts, "%+")` | Alias for to_timestamp |
| `is_null(val)` | `is_null(.email)` | Check if null |
| `concat(a, b, ...)` | `concat(.first, " ", .last)` | Concatenate strings |
| `coalesce(a, b, ...)` | `coalesce(.nickname, .name)` | First non-null value |

### Expressions

```
# Field access (nested, with array indexing)
.amount
.metadata.merchant.name
.items[0].price

# Literals
42, 3.14, "hello", true, false, null

# Arithmetic
.price * .quantity
.total - .discount

# Comparison
.amount > 0.0
.status == "active"

# Logical
.amount > 0.0 && .status == "active"

# Null coalescing
.currency ?? "USD"

# Conditional
if .amount > 100.0 { "high" } else { "low" }
```

### Transform Examples

```toml
[[transforms]]
name = "clean_and_enrich"
steps = [
    # Type coercion
    'set(.amount, to_float(.amount))',
    'set(.quantity, to_int(.quantity))',

    # Computed fields
    'set(.total, .amount * to_float(.quantity))',
    'set(.full_name, concat(.first_name, " ", .last_name))',

    # Defaults
    'set(.currency, .currency ?? "USD")',
    'set(.tier, if .total > 1000.0 { "premium" } else { "standard" })',

    # Clean up
    'rename(.date, .order_date)',
    'remove(.raw_payload)',

    # Filter
    'where(.total > 0.0)',
    'where(.status != "cancelled")',
]
```

## Optional Sections

### Dead-Letter Sink (capture failed records)
```toml
[dead_letter]
connector = "file"
[dead_letter.config]
path = "errors/dead_letters.json"
format = "json"
```

Failed records are enriched with `__error_step`, `__error_message`, and `__error_timestamp`.

### Telemetry (OpenTelemetry)
```toml
[telemetry]
enabled = true
otlp_endpoint = "http://localhost:4317"
service_name = "pipeliner"
```

## Environment Variables

Use `${VAR_NAME}` in any string value for env var substitution:
```toml
connection_string = "${DATABASE_URL}"
```

## Validation

After generating a pipeline config, validate it:
```bash
pipeliner validate pipeline.toml
```

## Key Rules for AI Assistants

1. **Always ask** what the source data is and where the output should go
2. **Ask about transforms** — what fields need cleaning, filtering, or enrichment
3. **Use env vars** for secrets (connection strings, API tokens) — never hardcode
4. **At least one sink** is required
5. **Transforms are optional** — skip if no data transformation is needed
6. **Multiple sinks** are supported for fan-out patterns
7. **Validate** the generated config with `pipeliner validate`
8. **Run `pipeliner ai-spec`** to discover installed connectors at runtime
