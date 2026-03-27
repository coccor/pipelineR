use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use pipeliner_core::connector::{SinkConnectorClientWrapper, SourceConnectorClientWrapper};
use pipeliner_core::dsl::parser::parse_step;
use pipeliner_core::dsl::step::execute_step;
use pipeliner_core::record::{Record, Value};
use pipeliner_proto::pipeliner::v1::load_request::Payload;
use pipeliner_proto::pipeliner::v1::sink_connector_server::SinkConnectorServer;
use pipeliner_proto::pipeliner::v1::source_connector_server::SourceConnectorServer;
use pipeliner_proto::pipeliner::v1::{LoadMetadata, LoadRequest};
use pipeliner_proto::{
    Partition, RecordBatch as ProtoRecordBatch, RuntimeParams, SchemaRequirementResponse,
    SchemaResponse, SinkConfig, SinkDescriptor, SourceConfig, SourceDescriptor,
};
use pipeliner_sdk::convert::{core_batch_to_proto, proto_batch_to_core};
use pipeliner_sdk::{
    DiscoveryError, ExtractionError, GrpcSinkService, GrpcSourceService, LoadError, LoadResult,
    Sink, Source, ValidationError,
};

// ---------------------------------------------------------------------------
// MockSource
// ---------------------------------------------------------------------------

struct MockSource;

#[async_trait]
impl Source for MockSource {
    fn describe(&self) -> SourceDescriptor {
        SourceDescriptor {
            name: "mock-source".to_string(),
            version: "0.1.0".to_string(),
            description: "A mock source for integration testing".to_string(),
        }
    }

    async fn validate(&self, _config: &SourceConfig) -> Result<(), ValidationError> {
        Ok(())
    }

    async fn discover_schema(
        &self,
        _config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError> {
        Ok(SchemaResponse { columns: vec![] })
    }

    async fn discover_partitions(
        &self,
        _config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError> {
        Ok(vec![])
    }

    async fn extract(
        &self,
        _config: &SourceConfig,
        _params: &RuntimeParams,
        tx: mpsc::Sender<ProtoRecordBatch>,
    ) -> Result<String, ExtractionError> {
        // Build two core records and convert to proto batches.
        let mut alice: Record = IndexMap::new();
        alice.insert("name".to_string(), Value::String("Alice".to_string()));
        alice.insert("amount".to_string(), Value::String("42.50".to_string()));

        let mut bob: Record = IndexMap::new();
        bob.insert("name".to_string(), Value::String("Bob".to_string()));
        bob.insert("amount".to_string(), Value::String("-10.00".to_string()));

        let batch1 = pipeliner_core::record::RecordBatch::new(vec![alice]);
        let batch2 = pipeliner_core::record::RecordBatch::new(vec![bob]);

        tx.send(core_batch_to_proto(&batch1))
            .await
            .map_err(|_| ExtractionError::ChannelClosed)?;
        tx.send(core_batch_to_proto(&batch2))
            .await
            .map_err(|_| ExtractionError::ChannelClosed)?;

        Ok("2026-03-25T23:59:59Z".to_string())
    }
}

// ---------------------------------------------------------------------------
// MockSink
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
            let count = core_batch.records.len() as i64;
            total += count;
            let mut store = self.received.lock().await;
            store.extend(core_batch.records);
        }
        Ok(LoadResult {
            rows_written: total,
            rows_errored: 0,
            error_message: String::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Integration test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn source_extract_transform_sink_load() {
    // --- Start source gRPC server ---
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_addr = source_listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(SourceConnectorServer::new(GrpcSourceService::new(
                MockSource,
            )))
            .serve_with_incoming(TcpListenerStream::new(source_listener))
            .await
            .unwrap();
    });

    // --- Start sink gRPC server ---
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

    // Let servers start.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- Connect to source and extract ---
    let mut source_client = SourceConnectorClientWrapper::connect(format!("http://{source_addr}"))
        .await
        .unwrap();

    let descriptor = source_client.describe().await.unwrap();
    assert_eq!(descriptor.name, "mock-source");
    assert_eq!(descriptor.version, "0.1.0");

    let mut stream = source_client
        .extract(SourceConfig::default(), RuntimeParams::default())
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

    assert_eq!(watermark, "2026-03-25T23:59:59Z");

    // Convert proto batches to core records.
    let mut records: Vec<Record> = proto_batches
        .iter()
        .flat_map(|b| proto_batch_to_core(b).records)
        .collect();

    assert_eq!(records.len(), 2);

    // --- Apply DSL transforms ---
    let set_step = parse_step("set(.amount, to_float(.amount))").unwrap();
    execute_step(&set_step, &mut records).unwrap();

    let where_step = parse_step("where(.amount > 0.0)").unwrap();
    execute_step(&where_step, &mut records).unwrap();

    // After filtering: only Alice (42.5) remains; Bob (-10.0) is filtered out.
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(records[0].get("amount"), Some(&Value::Float(42.5)));

    // --- Send to sink ---
    let mut sink_client = SinkConnectorClientWrapper::connect(format!("http://{sink_addr}"))
        .await
        .unwrap();

    // Build the load request stream: metadata first, then the transformed batch.
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
    assert_eq!(load_result.rows_written, 1);
    assert_eq!(load_result.rows_errored, 0);
    assert!(load_result.error_message.is_empty());

    // Verify the sink received the correct records.
    let stored = sink_store.lock().await;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        stored[0].get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(stored[0].get("amount"), Some(&Value::Float(42.5)));
}
