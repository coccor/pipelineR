---
name: plugin-sdk
description: "Plugin SDK patterns for pipelineR. Use this skill when working on pipeliner-sdk (the crate plugin authors depend on), implementing a built-in plugin (file, sql, rest-api, delta, parquet), or debugging plugin process lifecycle, gRPC communication between engine and plugins, or plugin registration. Trigger when files in pipeliner-sdk/ or plugins/ are involved."
---

# pipelineR plugin SDK

## Plugin binary contract

A plugin is a standalone Rust binary that:

1. Accepts a `--port <n>` CLI arg (0 = pick random port)
2. Starts a tonic gRPC server implementing `SourcePlugin` and/or `SinkPlugin`
3. Prints the actual bound port to stdout as the first line (e.g., `PORT=54321`)
4. Serves requests until SIGTERM, then shuts down gracefully

The SDK provides a `run_plugin` helper that handles all of this:

```rust
// In the plugin's main.rs
use pipeliner_sdk::run_plugin;

#[tokio::main]
async fn main() {
    run_plugin(MySource, MySink).await;
    // or run_source_plugin(MySource) / run_sink_plugin(MySink)
}
```

## Source trait

```rust
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Metadata about this source.
    fn describe(&self) -> SourceDescriptor;

    /// Validate config. Return Err with a human-readable message on failure.
    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError>;

    /// Discover schema. Return None if schema-less.
    async fn discover_schema(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
    ) -> Result<Option<Vec<ColumnSchema>>, DiscoveryError>;

    /// Discover partitions. Return a single partition if not partitionable.
    async fn discover_partitions(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError>;

    /// Extract data. Yield batches via the sender. Return the watermark on completion.
    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<RecordBatch>,
    ) -> Result<String, ExtractionError>;  // returns watermark
}
```

### Implementing extract

The `tx` sender is a bounded channel. The plugin sends `RecordBatch`es through it. The engine reads from the other end. When extraction is complete, the plugin drops `tx` (or it goes out of scope) and returns the watermark string.

```rust
async fn extract(&self, config: &SourceConfig, params: &RuntimeParams, tx: mpsc::Sender<RecordBatch>) -> Result<String, ExtractionError> {
    let mut batch = Vec::with_capacity(1000);
    let mut watermark = String::new();

    for row in self.fetch_rows(config, params).await? {
        watermark = row.get("updated_at").to_string();
        batch.push(row);
        if batch.len() >= 1000 {
            tx.send(RecordBatch { records: std::mem::take(&mut batch) }).await
                .map_err(|_| ExtractionError::ChannelClosed)?;
        }
    }
    if !batch.is_empty() {
        tx.send(RecordBatch { records: batch }).await
            .map_err(|_| ExtractionError::ChannelClosed)?;
    }

    Ok(watermark)
}
```

### Implementing discover_partitions

Return a `Vec<Partition>` where each partition has a `key` (for tracking/logging) and a `params` map that will be merged into the `RuntimeParams` when `extract` is called for that partition.

```rust
// SQL plugin: partition by date range
async fn discover_partitions(&self, config: &SourceConfig, params: &RuntimeParams) -> Result<Vec<Partition>, DiscoveryError> {
    let from = params.get("from_date")?;
    let to = params.get("to_date")?;
    let days = date_range(from, to);

    Ok(days.iter().map(|day| Partition {
        key: day.to_string(),
        params: hashmap! {
            "partition_from" => day.to_string(),
            "partition_to" => day.succ().to_string(),
        },
    }).collect())
}
```

## Sink trait

```rust
#[async_trait]
pub trait Sink: Send + Sync + 'static {
    fn describe(&self) -> SinkDescriptor;

    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError>;

    fn schema_requirement(&self) -> SchemaRequirement;

    /// Load data. Read batches from rx until the channel closes.
    /// Return the total rows written and any errors.
    async fn load(
        &self,
        config: &SinkConfig,
        schema: Option<Vec<ColumnSchema>>,
        rx: mpsc::Receiver<RecordBatch>,
    ) -> Result<LoadResult, LoadError>;
}
```

### SchemaRequirement

```rust
pub enum SchemaRequirement {
    /// Sink requires a specific schema (e.g., fixed SQL table).
    Fixed(Vec<ColumnSchema>),
    /// Sink accepts any schema (e.g., JSON file, Parquet).
    Flexible,
    /// Sink will use whatever schema the source provides.
    MatchSource,
}
```

## Plugin config parsing

Plugin configs are passed as `serde_json::Value` (deserialized from the TOML `config.*` section). Each plugin deserializes into its own config struct:

```rust
#[derive(Deserialize)]
struct SqlSourceConfig {
    driver: String,             // "sqlserver", "postgres", "mysql"
    connection_string: String,
    query: String,
    watermark_column: Option<String>,
    partition_column: Option<String>,
    partition_strategy: Option<String>, // "date_range", "key_range"
}
```

The SDK provides `parse_config::<T>(config: &SourceConfig) -> Result<T, ValidationError>` for ergonomic deserialization with helpful error messages on missing/wrong fields.

## Built-in plugin specifics

### File plugin

- Source: glob patterns for file discovery. One partition per file.
- Sink: writes to a single file path. Cloud storage via `object_store` crate.
- Parquet reading uses `parquet` crate. Maps Parquet logical types to `Value` variants.
- CSV uses `csv` crate with configurable delimiter, quote char, headers.

### SQL plugin

- Uses `tiberius` for SQL Server, `tokio-postgres` for PostgreSQL, `mysql_async` for MySQL.
- Source streams rows using database cursors / async result sets.
- Sink batches records and uses driver-specific bulk insert methods.
- `tiberius` limitation: no native bulk copy in the Rust client. Use parameterized INSERT batches (500 rows per batch) as workaround. Monitor `tiberius` for bulk copy support.

### REST API plugin

- Auth: `reqwest` client with configurable auth middleware.
- Pagination: generic loop — fetch page, extract records from response body via JSONPath, check for next page indicator (cursor field, link header, has_more flag).
- Rate limiting: `governor` crate for token bucket rate limiter.

### Delta Lake sink

- Uses `deltalake` crate (delta-rs).
- Requires row-to-Arrow conversion. Use `pipeliner-arrow-convert` crate.
- Merge (upsert) mode uses `DeltaOps::merge()`.
- Partition columns are extracted from records and used as Hive-style partitioning.

### Parquet sink

- Uses `parquet` + `arrow` crates.
- Same `pipeliner-arrow-convert` for row-to-Arrow.
- Compression configurable: snappy (default), zstd, gzip, none.

## Testing plugins

Integration tests for plugins that need external services:

- SQL Server: `testcontainers` crate with `mcr.microsoft.com/mssql/server:2022-latest`
- PostgreSQL: `testcontainers` with `postgres:16`
- MySQL: `testcontainers` with `mysql:8`
- S3: `testcontainers` with MinIO
- REST API: `wiremock` crate (in-process mock HTTP server)

Mark integration tests with `#[ignore]` so `cargo test` runs fast. Run with `cargo test -- --ignored` in CI.

## Gotchas

- `tiberius` requires TLS by default. For Docker SQL Server tests, set `TrustServerCertificate=true` in the connection string.
- `tokio-postgres` and `tiberius` have different error type hierarchies. The SDK should provide a `DriverError` enum that normalizes them.
- Plugin processes that fail to start (bad binary path, port conflict) must be caught within a timeout (5 seconds) and reported as a clear error, not as a hanging pipeline.
- The `object_store` crate unifies S3/Azure/GCS access. Use it as the storage backend for file and Delta Lake plugins rather than separate SDK crates per cloud.
