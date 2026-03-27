# Plugin authoring guide

This guide walks you through building a custom source or sink connector for pipelineR using the Rust SDK.

## Overview

A pipelineR connector is a **standalone Rust binary** that implements the `Source` trait, the `Sink` trait, or both. The SDK handles all gRPC boilerplate -- you implement trait methods, and the SDK runs them as a gRPC server.

At runtime, the pipelineR engine:
1. Spawns your binary as a child process with `--port 0`
2. Reads the assigned port from your binary's stdout (`PORT=<n>`)
3. Connects to your gRPC server and calls trait methods via RPC
4. Kills the process when the pipeline completes

## Project setup

### Create the crate

```bash
cargo new --bin pipeliner-connector-mydata
cd pipeliner-connector-mydata
```

### Add dependencies

```toml
# Cargo.toml
[package]
name = "pipeliner-connector-mydata"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "pipeliner-connector-mydata"
path = "src/main.rs"

[dependencies]
pipeliner-core = { path = "../pipeliner-core" }      # Record/Value types
pipeliner-proto = { path = "../pipeliner-proto" }     # Proto message types
pipeliner-sdk = { path = "../pipeliner-sdk" }         # Source/Sink traits + server
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

If publishing to crates.io, use version dependencies instead of path dependencies.

## Implementing a source

### Define your config

Create a config struct that deserializes from JSON. The engine passes connector config as a JSON string.

```rust
// src/config.rs
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MySourceConfig {
    pub endpoint: String,
    pub api_key: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize { 1000 }
```

### Implement the Source trait

```rust
// src/source.rs
use async_trait::async_trait;
use pipeliner_core::record::{Record, RecordBatch, Value};
use pipeliner_proto::{
    ColumnSchema, Partition, RuntimeParams, SchemaResponse, SourceConfig, SourceDescriptor,
};
use pipeliner_sdk::error::{DiscoveryError, ExtractionError, ValidationError};
use pipeliner_sdk::Source;
use tokio::sync::mpsc;

use crate::config::MySourceConfig;

pub struct MySource;

fn parse_config(config: &SourceConfig) -> Result<MySourceConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

#[async_trait]
impl Source for MySource {
    fn describe(&self) -> SourceDescriptor {
        SourceDescriptor {
            name: "mydata".to_string(),
            version: "0.1.0".to_string(),
            description: "My custom data source".to_string(),
        }
    }

    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError> {
        let cfg = parse_config(config)?;
        if cfg.endpoint.is_empty() {
            return Err(ValidationError::MissingField("endpoint".to_string()));
        }
        Ok(())
    }

    async fn discover_schema(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError> {
        // Connect to your data source and return column metadata.
        Ok(SchemaResponse {
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    description: "Primary key".to_string(),
                },
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: "string".to_string(),
                    nullable: true,
                    description: String::new(),
                },
            ],
        })
    }

    async fn discover_partitions(
        &self,
        _config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError> {
        // Return a single partition or split by date range, key, etc.
        Ok(vec![Partition {
            key: "default".to_string(),
            params: std::collections::HashMap::new(),
        }])
    }

    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<pipeliner_proto::RecordBatch>,
    ) -> Result<String, ExtractionError> {
        let cfg = parse_config(config)
            .map_err(|e| ExtractionError::Failed(e.to_string()))?;

        // Fetch data from your source system.
        // Build Record objects and send them in batches.
        let mut batch: Vec<Record> = Vec::new();
        let mut watermark = String::new();

        // Example: iterate over your data
        for item in fetch_items(&cfg).await? {
            let mut record = Record::new();
            record.insert("id".to_string(), Value::Int(item.id));
            record.insert("name".to_string(), Value::String(item.name));
            batch.push(record);

            // Update watermark (e.g., max timestamp)
            if item.updated_at > watermark {
                watermark = item.updated_at.clone();
            }

            // Send when batch is full
            if batch.len() >= cfg.batch_size {
                let rb = RecordBatch::new(std::mem::take(&mut batch));
                let proto = pipeliner_sdk::convert::core_batch_to_proto(&rb);
                tx.send(proto).await
                    .map_err(|_| ExtractionError::ChannelClosed)?;
            }
        }

        // Flush remaining records
        if !batch.is_empty() {
            let rb = RecordBatch::new(batch);
            let proto = pipeliner_sdk::convert::core_batch_to_proto(&rb);
            tx.send(proto).await
                .map_err(|_| ExtractionError::ChannelClosed)?;
        }

        // Return the watermark for the orchestrator
        Ok(watermark)
    }
}
```

### Key points for sources

- **Batching:** Don't send one record at a time. Batch into groups of 100-1000 records for efficiency.
- **Watermarks:** Return the "high water mark" of extracted data (e.g., max `updated_at` timestamp). The orchestrator uses this to know where to resume next time.
- **Partitions:** If your data can be split for parallel extraction (by date range, key, region, etc.), return multiple `Partition` objects from `discover_partitions`. Each partition's `params` map is passed back to `extract` as `RuntimeParams`.
- **Channel:** Send data through `tx`. If the channel is closed (engine cancelled the run), return `ExtractionError::ChannelClosed`.

## Implementing a sink

```rust
// src/sink.rs
use async_trait::async_trait;
use pipeliner_proto::{SchemaRequirementResponse, SchemaResponse, SinkConfig, SinkDescriptor};
use pipeliner_sdk::convert::proto_batch_to_core;
use pipeliner_sdk::error::{LoadError, ValidationError};
use pipeliner_sdk::sink::LoadResult;
use pipeliner_sdk::Sink;
use tokio::sync::mpsc;

pub struct MySink;

#[async_trait]
impl Sink for MySink {
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "mydata".to_string(),
            version: "0.1.0".to_string(),
            description: "My custom data sink".to_string(),
        }
    }

    async fn validate(&self, config: &SinkConfig) -> Result<(), ValidationError> {
        let cfg: MyConfig = pipeliner_sdk::parse_config(&config.config_json)?;
        // Validate config fields...
        Ok(())
    }

    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // 0 = FLEXIBLE, 1 = MATCH_SOURCE, 2 = FIXED
            fixed_schema: vec![],
        }
    }

    async fn load(
        &self,
        config: &SinkConfig,
        _schema: Option<SchemaResponse>,
        mut rx: mpsc::Receiver<pipeliner_proto::RecordBatch>,
    ) -> Result<LoadResult, LoadError> {
        let cfg: MyConfig = pipeliner_sdk::parse_config(&config.config_json)
            .map_err(|e| LoadError::Failed(e.to_string()))?;

        let mut rows_written: i64 = 0;

        // Receive batches until the channel closes (source + transform done)
        while let Some(proto_batch) = rx.recv().await {
            let core_batch = proto_batch_to_core(&proto_batch);

            for record in &core_batch.records {
                // Write each record to your destination
                write_record(&cfg, record).await
                    .map_err(|e| LoadError::Failed(e.to_string()))?;
                rows_written += 1;
            }
        }

        Ok(LoadResult {
            rows_written,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}
```

### Key points for sinks

- **Batching writes:** Don't write one row at a time to your destination. Collect rows and use bulk operations (e.g., multi-row INSERT, batch API calls).
- **Schema requirement:** Return `FLEXIBLE` (0) if you accept any schema. Return `MATCH_SOURCE` (1) if you need the source schema. Return `FIXED` (2) with `fixed_schema` if your destination has a fixed schema.
- **Error reporting:** Return `rows_errored` for partial failures. If the entire load fails, return `LoadError`.

## Writing the entry point

```rust
// src/main.rs
use pipeliner_connector_mydata::{MySource, MySink};

#[tokio::main]
async fn main() {
    // Run both source and sink in the same binary:
    pipeliner_sdk::run_connector(MySource, MySink)
        .await
        .expect("connector failed");

    // Or source-only:
    // pipeliner_sdk::run_source_connector(MySource).await.expect("failed");

    // Or sink-only:
    // pipeliner_sdk::run_sink_connector(MySink).await.expect("failed");
}
```

The SDK handles:
- Parsing the `--port` CLI argument
- Binding to `127.0.0.1:<port>` (port 0 = OS-assigned)
- Printing `PORT=<actual_port>` to stdout (the engine reads this)
- Running the gRPC server with 64 MB max message size
- Graceful shutdown on SIGTERM and Ctrl-C

## Data types

The `Value` enum represents all data types flowing through pipelineR:

| Variant | Rust type | Usage |
|---------|-----------|-------|
| `Null` | -- | Missing or unknown value |
| `Bool(bool)` | `bool` | Boolean |
| `Int(i64)` | `i64` | Integer |
| `Float(f64)` | `f64` | Floating point |
| `String(String)` | `String` | Text |
| `Bytes(Vec<u8>)` | `Vec<u8>` | Binary data |
| `Timestamp(DateTime<Utc>)` | `chrono::DateTime<Utc>` | UTC timestamp |
| `Array(Vec<Value>)` | `Vec<Value>` | Ordered list |
| `Map(Record)` | `IndexMap<String, Value>` | Nested record |

A `Record` is an `IndexMap<String, Value>` -- an ordered map preserving insertion order.

## Proto conversion helpers

The SDK provides conversion functions between core types and protobuf messages:

```rust
use pipeliner_sdk::convert::{
    core_batch_to_proto,   // RecordBatch -> proto RecordBatch
    proto_batch_to_core,   // proto RecordBatch -> RecordBatch
    core_record_to_proto,  // Record -> proto Record
    proto_record_to_core,  // proto Record -> Record
};
```

Sources should use `core_batch_to_proto` to send data. Sinks should use `proto_batch_to_core` to receive data.

## Error types

The SDK defines four error enums in `pipeliner_sdk::error`:

| Error | Used by | Variants |
|-------|---------|----------|
| `ValidationError` | `validate()` | `InvalidConfig(String)`, `MissingField(String)` |
| `DiscoveryError` | `discover_schema()`, `discover_partitions()` | `Failed(String)`, `Connection(String)` |
| `ExtractionError` | `extract()` | `Failed(String)`, `Connection(String)`, `ChannelClosed` |
| `LoadError` | `load()` | `Failed(String)`, `Connection(String)` |

## Config parsing

Use `pipeliner_sdk::parse_config` to deserialize the JSON config string into your typed struct:

```rust
let cfg: MyConfig = pipeliner_sdk::parse_config(&config.config_json)?;
```

This uses `serde_json` under the hood. Invalid JSON or missing fields return `ValidationError::InvalidConfig`.

The engine serializes the TOML `[source.config]` or `[sinks.config]` table to JSON before passing it to your connector.

## Testing

### Unit tests

Test your business logic directly:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_rejects_empty_endpoint() {
        let source = MySource;
        let config = SourceConfig {
            config_json: r#"{"endpoint": "", "api_key": "test"}"#.to_string(),
        };
        assert!(source.validate(&config).await.is_err());
    }
}
```

### Integration tests

Test the full gRPC flow using the SDK's gRPC bridges:

```rust
use pipeliner_sdk::grpc_source::GrpcSourceService;
use pipeliner_proto::pipeliner::v1::source_connector_server::SourceConnectorServer;

#[tokio::test]
async fn test_extract_over_grpc() {
    let source = MySource;
    let service = SourceConnectorServer::new(GrpcSourceService::new(source));

    // Start server on random port...
    // Connect client...
    // Call Extract and verify records...
}
```

See `pipeliner-core/tests/m2_integration.rs` for a complete example.

## Registering your connector

Add an entry to `connectors.toml`:

```toml
[mydata]
path = "/path/to/pipeliner-connector-mydata"
description = "My custom data connector"
```

Or install via the CLI:

```bash
pipeliner connectors install mydata
```

## Publishing

1. Publish your crate to crates.io
2. Users install with `pipeliner connectors install pipeliner-connector-mydata`
3. The CLI runs `cargo install` and registers the binary

## Checklist

Before publishing a connector:

- [ ] `describe()` returns a unique name and version
- [ ] `validate()` checks all required config fields
- [ ] `discover_schema()` returns accurate column metadata (or empty for schema-less sources)
- [ ] `discover_partitions()` returns at least one partition
- [ ] `extract()` sends data in reasonably-sized batches (100-1000 records)
- [ ] `extract()` returns a meaningful watermark
- [ ] Error messages include enough context to diagnose without reading connector logs
- [ ] No `unwrap()` or `expect()` in library code -- use `?` and typed errors
- [ ] Config struct has sensible defaults via `#[serde(default)]`
- [ ] Unit tests cover validation, happy path, and error cases
