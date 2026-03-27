//! M6 integration test: gRPC server (sidecar mode) and orchestration API.
//!
//! Simulates a C# orchestrator calling the PipelineR service:
//! - DiscoverSchema
//! - DiscoverPartitions
//! - RunPipeline → WatchRun (stream events until completion)
//! - GetRunStatus (poll-based)
//! - CancelRun (stops a running pipeline)
//! - ValidatePipeline
//! - Health

use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use pipeliner_core::config::{ConnectorEntry, ConnectorRegistry};
use pipeliner_core::server::PipelineRServer;
use pipeliner_proto::pipeliner::v1::pipeline_r_client::PipelineRClient;
use pipeliner_proto::pipeliner::v1::{
    CancelRunRequest, GetRunStatusRequest, HealthRequest, PipelineDiscoverPartitionsRequest,
    PipelineDiscoverSchemaRequest, RunPipelineRequest, RunState, ValidatePipelineRequest,
    WatchRunRequest,
};
use tokio_stream::StreamExt;

/// Find the compiled file connector binary in the target directory.
fn connector_binary_path() -> String {
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

/// Build a connector registry pointing to our compiled file connector.
fn test_registry() -> ConnectorRegistry {
    let mut connectors = HashMap::new();
    connectors.insert(
        "file".to_string(),
        ConnectorEntry {
            path: connector_binary_path(),
            description: "File source and sink".to_string(),
        },
    );
    ConnectorRegistry { connectors }
}

/// Start the PipelineR gRPC server on a random port and return the client + port.
async fn start_test_server() -> (PipelineRClient<tonic::transport::Channel>, u16) {
    let server = PipelineRServer::new(test_registry());

    // Bind to port 0 to get an OS-assigned port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // Start the server using the listener directly.
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(
                pipeliner_proto::pipeliner::v1::pipeline_r_server::PipelineRServer::new(server)
                    .max_decoding_message_size(64 * 1024 * 1024)
                    .max_encoding_message_size(64 * 1024 * 1024),
            )
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Wait for the server to be ready.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = PipelineRClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("connect to server");

    (client, port)
}

/// Create a test TOML config for a CSV → JSON pipeline.
fn test_pipeline_toml(csv_input: &str, json_output: &str) -> String {
    format!(
        r#"
[pipeline]
name = "integration_test"

[source]
connector = "file"
config.path = "{input}"
config.format = "csv"

[[transforms]]
name = "clean"
steps = [
    'set(.amount, to_float(.amount))',
    'where(.amount > 0.0)',
]

[[sinks]]
connector = "file"
config.path = "{output}"
config.format = "json"
"#,
        input = csv_input.replace('\\', "\\\\"),
        output = json_output.replace('\\', "\\\\"),
    )
}

#[tokio::test]
async fn health_check() {
    let (mut client, _port) = start_test_server().await;

    let resp = client.health(HealthRequest {}).await.unwrap().into_inner();
    assert_eq!(resp.status, 0); // SERVING = 0
}

#[tokio::test]
async fn validate_pipeline_valid_config() {
    let (mut client, _port) = start_test_server().await;

    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount").unwrap();
        writeln!(f, "Alice,42").unwrap();
    }
    let json_output = dir.path().join("output.jsonl");

    let toml = test_pipeline_toml(csv_input.to_str().unwrap(), json_output.to_str().unwrap());

    let resp = client
        .validate_pipeline(ValidatePipelineRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::validate_pipeline_request::Config::ConfigToml(toml),
            ),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(
        resp.valid,
        "expected valid config, got errors: {:?}",
        resp.errors
    );
}

#[tokio::test]
async fn validate_pipeline_bad_config() {
    let (mut client, _port) = start_test_server().await;

    let result = client
        .validate_pipeline(ValidatePipelineRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::validate_pipeline_request::Config::ConfigToml(
                    "invalid toml [[[".to_string(),
                ),
            ),
        })
        .await;

    // Should return INVALID_ARGUMENT status.
    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn discover_schema() {
    let (mut client, _port) = start_test_server().await;

    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("schema.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "col_a,col_b,col_c").unwrap();
        writeln!(f, "1,hello,true").unwrap();
    }

    let toml = format!(
        r#"
[pipeline]
name = "schema_test"

[source]
connector = "file"
config.path = "{}"
config.format = "csv"

[[transforms]]
name = "noop"
steps = []

[[sinks]]
connector = "file"
config.path = "/dev/null"
config.format = "json"
"#,
        csv_input.to_str().unwrap().replace('\\', "\\\\")
    );

    let resp = client
        .discover_schema(PipelineDiscoverSchemaRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::pipeline_discover_schema_request::Config::ConfigToml(
                    toml,
                ),
            ),
            params: HashMap::new(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.columns.is_empty());
    let names: Vec<&str> = resp.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"col_a"));
    assert!(names.contains(&"col_b"));
    assert!(names.contains(&"col_c"));
}

#[tokio::test]
async fn discover_partitions() {
    let (mut client, _port) = start_test_server().await;

    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("parts.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "id,val").unwrap();
        writeln!(f, "1,a").unwrap();
    }

    let toml = format!(
        r#"
[pipeline]
name = "partitions_test"

[source]
connector = "file"
config.path = "{}"
config.format = "csv"

[[transforms]]
name = "noop"
steps = []

[[sinks]]
connector = "file"
config.path = "/dev/null"
config.format = "json"
"#,
        csv_input.to_str().unwrap().replace('\\', "\\\\")
    );

    let resp = client
        .discover_partitions(PipelineDiscoverPartitionsRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::pipeline_discover_partitions_request::Config::ConfigToml(
                    toml,
                ),
            ),
            params: HashMap::new(),
        })
        .await
        .unwrap()
        .into_inner();

    // File connector returns one partition per file.
    assert!(!resp.partitions.is_empty());
}

#[tokio::test]
async fn run_pipeline_and_get_status() {
    let (mut client, _port) = start_test_server().await;

    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount").unwrap();
        writeln!(f, "Alice,42.50").unwrap();
        writeln!(f, "Bob,-10.00").unwrap();
        writeln!(f, "Carol,100.00").unwrap();
    }
    let json_output = dir.path().join("output.jsonl");

    let toml = test_pipeline_toml(csv_input.to_str().unwrap(), json_output.to_str().unwrap());

    // Submit the run.
    let run_resp = client
        .run_pipeline(RunPipelineRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::run_pipeline_request::Config::ConfigToml(toml),
            ),
            params: HashMap::new(),
        })
        .await
        .unwrap()
        .into_inner();

    let run_id = run_resp.run_id;
    assert!(!run_id.is_empty());

    // Poll until complete (with timeout).
    let mut final_status = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let status = client
            .get_run_status(GetRunStatusRequest {
                run_id: run_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        if status.state != RunState::Running as i32 {
            final_status = Some(status);
            break;
        }
    }

    let status = final_status.expect("pipeline did not complete within timeout");
    assert_eq!(
        status.state,
        RunState::Completed as i32,
        "expected COMPLETED, got state={}, error={}",
        status.state,
        status.error_message
    );
    assert!(!status.watermark.is_empty(), "watermark should be set");

    // Verify output file.
    let json_content = std::fs::read_to_string(&json_output).unwrap();
    let lines: Vec<&str> = json_content.trim().lines().collect();
    // Alice (42.5) and Carol (100.0) pass the filter.
    assert_eq!(lines.len(), 2, "expected 2 JSON lines, got {}", lines.len());
}

#[tokio::test]
async fn run_pipeline_and_watch_run() {
    let (mut client, _port) = start_test_server().await;

    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount").unwrap();
        writeln!(f, "Alice,42.50").unwrap();
        writeln!(f, "Carol,100.00").unwrap();
    }
    let json_output = dir.path().join("output.jsonl");

    let toml = test_pipeline_toml(csv_input.to_str().unwrap(), json_output.to_str().unwrap());

    // Submit the run.
    let run_resp = client
        .run_pipeline(RunPipelineRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::run_pipeline_request::Config::ConfigToml(toml),
            ),
            params: HashMap::new(),
        })
        .await
        .unwrap()
        .into_inner();

    let run_id = run_resp.run_id;

    // Small delay to let the pipeline make progress.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Open WatchRun stream.
    let mut stream = client
        .watch_run(WatchRunRequest {
            run_id: run_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    // Collect events until we see a completion event.
    let mut events = Vec::new();
    let timeout = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            maybe_event = stream.next() => {
                match maybe_event {
                    Some(Ok(event)) => {
                        events.push(event.clone());
                        // Check if this is a completion event.
                        if let Some(pipeliner_proto::pipeliner::v1::run_event::Event::Completed(status)) = &event.event {
                            assert_eq!(status.state, RunState::Completed as i32);
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        panic!("WatchRun stream error: {e}");
                    }
                    None => {
                        // Stream ended without completion event — check last event.
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                panic!("WatchRun timed out after 15s. Events so far: {events:?}");
            }
        }
    }

    // We should have received at least one event (the completion).
    assert!(!events.is_empty(), "expected at least one event");

    // The last event should be a completion with COMPLETED state.
    let last = events.last().unwrap();
    if let Some(pipeliner_proto::pipeliner::v1::run_event::Event::Completed(status)) = &last.event {
        assert_eq!(status.state, RunState::Completed as i32);
    } else {
        panic!("last event was not a completion: {last:?}");
    }
}

#[tokio::test]
async fn cancel_run() {
    let (mut client, _port) = start_test_server().await;

    // Create a large CSV to give us time to cancel.
    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("large.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount").unwrap();
        for i in 0..10000 {
            writeln!(f, "Person{i},{i}.0").unwrap();
        }
    }
    let json_output = dir.path().join("output.jsonl");

    let toml = test_pipeline_toml(csv_input.to_str().unwrap(), json_output.to_str().unwrap());

    // Submit the run.
    let run_resp = client
        .run_pipeline(RunPipelineRequest {
            config: Some(
                pipeliner_proto::pipeliner::v1::run_pipeline_request::Config::ConfigToml(toml),
            ),
            params: HashMap::new(),
        })
        .await
        .unwrap()
        .into_inner();

    let run_id = run_resp.run_id;

    // Give pipeline a moment to start.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cancel the run.
    let cancel_resp = client
        .cancel_run(CancelRunRequest {
            run_id: run_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();

    assert!(cancel_resp.acknowledged);

    // Poll until the run finishes.
    let mut final_status = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let status = client
            .get_run_status(GetRunStatusRequest {
                run_id: run_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();

        if status.state != RunState::Running as i32 {
            final_status = Some(status);
            break;
        }
    }

    let status = final_status.expect("run did not finish after cancel");
    // Should be either CANCELLED or FAILED (both are acceptable when cancelled).
    assert!(
        status.state == RunState::Cancelled as i32 || status.state == RunState::Failed as i32,
        "expected CANCELLED or FAILED, got state={}",
        status.state
    );
}

#[tokio::test]
async fn get_run_status_unknown_run() {
    let (mut client, _port) = start_test_server().await;

    let result = client
        .get_run_status(GetRunStatusRequest {
            run_id: "nonexistent-run-id".to_string(),
        })
        .await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}
