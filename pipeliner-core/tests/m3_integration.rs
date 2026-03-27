//! M3 integration test: file source connector (CSV) → DSL transforms → mock sink.
//!
//! This test spawns the actual `pipeliner-connector-file` binary, reads a CSV file
//! through the gRPC protocol, applies transforms, and sends to a mock sink.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use pipeliner_core::connector::{ConnectorProcess, SourceConnectorClientWrapper};
use pipeliner_core::dsl::parser::parse_step;
use pipeliner_core::dsl::step::execute_step;
use pipeliner_core::record::{Record, Value};
use pipeliner_proto::pipeliner::v1::sink_connector_server::SinkConnectorServer;
use pipeliner_proto::{
    RecordBatch as ProtoRecordBatch, RuntimeParams, SchemaRequirementResponse, SchemaResponse,
    SinkConfig, SinkDescriptor, SourceConfig,
};
use pipeliner_sdk::convert::{core_batch_to_proto, proto_batch_to_core};
use pipeliner_sdk::{GrpcSinkService, LoadError, LoadResult, Sink, ValidationError};

// ---------------------------------------------------------------------------
// MockSink — same as M2 test
// ---------------------------------------------------------------------------

struct MockSink {
    received: Arc<Mutex<Vec<Record>>>,
}

#[async_trait]
impl Sink for MockSink {
    fn describe(&self) -> SinkDescriptor {
        SinkDescriptor {
            name: "mock-sink".to_string(),
            version: "0.1.0".to_string(),
            description: "A mock sink for integration testing".to_string(),
        }
    }

    async fn validate(&self, _config: &SinkConfig) -> Result<(), ValidationError> {
        Ok(())
    }

    fn schema_requirement(&self) -> SchemaRequirementResponse {
        SchemaRequirementResponse {
            requirement: 0, // FLEXIBLE
            fixed_schema: vec![],
        }
    }

    async fn load(
        &self,
        _config: &SinkConfig,
        _schema: Option<SchemaResponse>,
        mut rx: mpsc::Receiver<ProtoRecordBatch>,
    ) -> Result<LoadResult, LoadError> {
        let mut total = 0i64;
        while let Some(batch) = rx.recv().await {
            let core_batch = proto_batch_to_core(&batch);
            total += core_batch.records.len() as i64;
            self.received.lock().await.extend(core_batch.records);
        }
        Ok(LoadResult {
            rows_written: total,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: find the compiled connector binary
// ---------------------------------------------------------------------------

fn connector_binary_path() -> String {
    // The binary is in target/debug/ when running tests.
    let mut path = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("parent")
        .parent()
        .expect("parent")
        .to_path_buf();
    path.push("pipeliner-connector-file");
    path.to_str().expect("path to string").to_string()
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn csv_source_extract_transform_to_mock_sink() {
    // --- Create a temp CSV file ---
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("test_data.csv");
    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "name,amount,active").unwrap();
        writeln!(f, "Alice,42.50,true").unwrap();
        writeln!(f, "Bob,-10.00,false").unwrap();
        writeln!(f, "Carol,100.00,true").unwrap();
    }

    // --- Spawn the file source connector binary ---
    let mut plugin = ConnectorProcess::spawn("file", &connector_binary_path())
        .await
        .expect("failed to spawn file connector");

    // Small delay for the server to be ready.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Connect gRPC client ---
    let mut source_client = SourceConnectorClientWrapper::connect(plugin.address())
        .await
        .expect("failed to connect to file connector");

    // --- Describe ---
    let desc = source_client.describe().await.unwrap();
    assert_eq!(desc.name, "file");
    assert_eq!(desc.version, "0.1.0");

    // --- Build config ---
    let config_json = serde_json::json!({
        "path": csv_path.to_str().unwrap(),
        "format": "csv"
    });
    let source_config = SourceConfig {
        config_json: config_json.to_string(),
    };

    // --- Validate ---
    let validation = source_client.validate(source_config.clone()).await.unwrap();
    assert!(
        validation.valid,
        "validation errors: {:?}",
        validation.errors
    );

    // --- Discover Schema ---
    let schema = source_client
        .discover_schema(source_config.clone(), RuntimeParams::default())
        .await
        .unwrap();
    assert_eq!(schema.columns.len(), 3);
    let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(col_names, vec!["name", "amount", "active"]);
    // CSV schema infers all as string.
    for col in &schema.columns {
        assert_eq!(col.data_type, "string");
    }

    // --- Discover Partitions ---
    let partitions_resp = source_client
        .discover_partitions(source_config.clone(), RuntimeParams::default())
        .await
        .unwrap();
    assert_eq!(partitions_resp.partitions.len(), 1); // One file = one partition.
    assert!(partitions_resp.partitions[0].params.contains_key("file"));

    // --- Extract ---
    let mut stream = source_client
        .extract(source_config.clone(), RuntimeParams::default())
        .await
        .unwrap();

    let mut proto_batches: Vec<ProtoRecordBatch> = Vec::new();
    let mut watermark = String::new();

    while let Some(resp) = stream.message().await.unwrap() {
        if let Some(batch) = resp.batch {
            proto_batches.push(batch);
        }
        if !resp.watermark.is_empty() {
            watermark = resp.watermark;
        }
    }

    assert!(!watermark.is_empty(), "watermark should be set");

    // Convert to core records.
    let mut records: Vec<Record> = proto_batches
        .iter()
        .flat_map(|b| proto_batch_to_core(b).records)
        .collect();
    assert_eq!(records.len(), 3);

    // --- Apply DSL transforms ---
    // Convert amount to float.
    let step1 = parse_step("set(.amount, to_float(.amount))").unwrap();
    execute_step(&step1, &mut records).unwrap();

    // Filter: keep only positive amounts.
    let step2 = parse_step("where(.amount > 0.0)").unwrap();
    execute_step(&step2, &mut records).unwrap();

    // After filter: Alice (42.5) and Carol (100.0) remain.
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].get("name"), Some(&Value::String("Alice".into())));
    assert_eq!(records[0].get("amount"), Some(&Value::Float(42.5)));
    assert_eq!(records[1].get("name"), Some(&Value::String("Carol".into())));
    assert_eq!(records[1].get("amount"), Some(&Value::Float(100.0)));

    // --- Start mock sink and send results ---
    let sink_store: Arc<Mutex<Vec<Record>>> = Arc::new(Mutex::new(Vec::new()));
    let mock_sink = MockSink {
        received: Arc::clone(&sink_store),
    };

    let sink_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_addr = sink_listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(SinkConnectorServer::new(GrpcSinkService::new(mock_sink)))
            .serve_with_incoming(TcpListenerStream::new(sink_listener))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sink_client = pipeliner_core::connector::SinkConnectorClientWrapper::connect(format!(
        "http://{sink_addr}"
    ))
    .await
    .unwrap();

    use pipeliner_proto::pipeliner::v1::load_request::Payload;
    use pipeliner_proto::pipeliner::v1::{LoadMetadata, LoadRequest};

    let transformed_batch = core_batch_to_proto(&pipeliner_core::record::RecordBatch::new(records));

    let metadata_msg = LoadRequest {
        payload: Some(Payload::Metadata(LoadMetadata {
            config: Some(SinkConfig::default()),
            schema: None,
        })),
    };
    let batch_msg = LoadRequest {
        payload: Some(Payload::Batch(transformed_batch)),
    };

    let load_stream = tokio_stream::iter(vec![metadata_msg, batch_msg]);
    let load_result = sink_client.load(load_stream).await.unwrap();
    assert_eq!(load_result.rows_written, 2);
    assert_eq!(load_result.rows_errored, 0);

    // Verify sink received correct data.
    let stored = sink_store.lock().await;
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].get("name"), Some(&Value::String("Alice".into())));
    assert_eq!(stored[1].get("name"), Some(&Value::String("Carol".into())));

    // Cleanup.
    plugin.kill().await.ok();
}

#[tokio::test]
async fn json_source_schema_discovery_and_extract() {
    // --- Create a temp NDJSON file ---
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("test_data.jsonl");
    {
        let mut f = std::fs::File::create(&json_path).unwrap();
        writeln!(f, r#"{{"id":1,"name":"Alice","score":95.5}}"#).unwrap();
        writeln!(f, r#"{{"id":2,"name":"Bob","score":87.0}}"#).unwrap();
    }

    // --- Spawn plugin ---
    let mut plugin = ConnectorProcess::spawn("file", &connector_binary_path())
        .await
        .expect("failed to spawn file connector");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = SourceConnectorClientWrapper::connect(plugin.address())
        .await
        .unwrap();

    let config_json = serde_json::json!({
        "path": json_path.to_str().unwrap(),
        "format": "json"
    });
    let source_config = SourceConfig {
        config_json: config_json.to_string(),
    };

    // --- Schema discovery ---
    let schema = client
        .discover_schema(source_config.clone(), RuntimeParams::default())
        .await
        .unwrap();
    let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"score"));

    // --- Extract ---
    let mut stream = client
        .extract(source_config, RuntimeParams::default())
        .await
        .unwrap();

    let mut records: Vec<Record> = Vec::new();
    while let Some(resp) = stream.message().await.unwrap() {
        if let Some(batch) = resp.batch {
            records.extend(proto_batch_to_core(&batch).records);
        }
    }

    assert_eq!(records.len(), 2);
    // JSON reader preserves types.
    assert_eq!(records[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(records[0].get("name"), Some(&Value::String("Alice".into())));
    assert_eq!(records[0].get("score"), Some(&Value::Float(95.5)));

    plugin.kill().await.ok();
}
