---
name: grpc-proto
description: "Protobuf definitions and gRPC patterns for pipelineR. Use this skill when editing .proto files in pipeliner-proto/, implementing gRPC services or clients in tonic, working on the orchestrator-facing PipelineR service, or working on the plugin-facing SourcePlugin/SinkPlugin services. Also trigger when debugging gRPC streaming, serialization, or connection issues."
---

# pipelineR gRPC protocol

## Proto file organization

All .proto files live in `pipeliner-proto/proto/`:

```
pipeliner-proto/proto/
├── pipeliner/v1/
│   ├── service.proto      # Orchestrator-facing PipelineR service
│   ├── plugin.proto        # Plugin-facing SourcePlugin + SinkPlugin services
│   ├── records.proto       # Record, Value, RecordBatch messages
│   ├── schema.proto        # ColumnSchema, SchemaResponse
│   ├── pipeline.proto      # RunStatus, RunEvent, RunMetrics, Partition, Watermark
│   └── common.proto        # Shared types (RuntimeParams, config oneof)
```

Use `prost-build` in `pipeliner-proto/build.rs` to generate Rust code. The generated code is NOT checked in — it's in the `target/` directory.

## Two separate gRPC contracts

### 1. Orchestrator → Engine (`service.proto`)

The `PipelineR` service runs inside the sidecar. The orchestrator calls it on `localhost:50051`.

```protobuf
service PipelineR {
  rpc DiscoverSchema(DiscoverSchemaRequest) returns (SchemaResponse);
  rpc DiscoverPartitions(DiscoverPartitionsRequest) returns (PartitionsResponse);
  rpc RunPipeline(RunPipelineRequest) returns (RunPipelineResponse);
  rpc WatchRun(WatchRunRequest) returns (stream RunEvent);
  rpc GetRunStatus(GetRunStatusRequest) returns (RunStatus);
  rpc CancelRun(CancelRunRequest) returns (CancelRunResponse);
  rpc ValidatePipeline(ValidatePipelineRequest) returns (ValidationResult);
  rpc Health(HealthRequest) returns (HealthResponse);
}
```

`WatchRun` is server-streaming. The server sends `RunEvent` messages until the pipeline completes or fails. The last event is always a `RunEvent` with the `completed` field set to a `RunStatus`.

### 2. Engine → Plugin (`plugin.proto`)

The engine connects to plugin binaries as a gRPC *client*. Each plugin runs its own gRPC *server*.

```protobuf
service SourcePlugin {
  rpc Describe(Empty) returns (SourceDescriptor);
  rpc Validate(SourceConfig) returns (ValidationResult);
  rpc DiscoverSchema(DiscoverSchemaRequest) returns (SchemaResponse);
  rpc DiscoverPartitions(DiscoverPartitionsRequest) returns (PartitionsResponse);
  rpc Extract(ExtractRequest) returns (stream ExtractResponse);
}

service SinkPlugin {
  rpc Describe(Empty) returns (SinkDescriptor);
  rpc Validate(SinkConfig) returns (ValidationResult);
  rpc SchemaRequirement(Empty) returns (SchemaRequirementResponse);
  rpc Load(stream LoadRequest) returns (LoadResult);
}
```

`Extract` is server-streaming (plugin → engine). `Load` is client-streaming (engine → plugin).

The `ExtractResponse` contains a `RecordBatch` and is also the final message that carries the `watermark` string.

## Tonic patterns

- Use `tonic-build` in build.rs, not manual codegen.
- Server: `#[tonic::async_trait]` on the service impl.
- Streaming: return `ReceiverStream<Result<T, Status>>` from server-streaming RPCs.
- Client-streaming (Load): use `tonic::Streaming<LoadRequest>` as the input parameter.
- Deadlines: always set `.timeout(Duration::from_secs(300))` on client calls. Source extraction can be long — use a generous timeout or no timeout with cancellation.
- Health check: implement `grpc.health.v1.Health` for native K8s gRPC probes.

## Serialization of Value

The `Value` type maps to a protobuf `oneof`. See `records.proto` for the full definition. Key rules:

- `Timestamp` uses `google.protobuf.Timestamp` (seconds + nanos, always UTC).
- `Map` uses `map<string, Value>` — this is recursive via `MapValue`.
- `Array` uses `repeated Value` inside an `ArrayValue` wrapper.
- `Bytes` maps directly to protobuf `bytes`.

## Gotchas

- Protobuf `map<string, Value>` does NOT preserve insertion order. If field ordering matters, you need a `repeated` of key-value pairs instead. For v1, accept unordered.
- gRPC message size default limit is 4MB. Large `RecordBatch`es (many records, large binary fields) can exceed this. Set `max_decoding_message_size` and `max_encoding_message_size` on both client and server to a higher value (e.g., 64MB).
- `tonic::Status` codes: use `INVALID_ARGUMENT` for config validation errors, `INTERNAL` for plugin crashes, `CANCELLED` for cancelled runs, `NOT_FOUND` for unknown run IDs.
- When a plugin process dies mid-stream, the tonic client gets a transport error. Map this to a meaningful `RunEvent::error` with the plugin name and exit code.
