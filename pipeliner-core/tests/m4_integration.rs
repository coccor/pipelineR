//! M4 integration test: first complete E-T-L pipeline.
//!
//! CSV file source → rename + filter + coerce transforms → JSON file sink + CSV file sink.
//! Validates the pipeline runtime orchestration loop with multiple sink fan-out.

use std::io::Write;
use std::time::Duration;

use pipeliner_core::dsl::parser::parse_step;
use pipeliner_core::plugin::{PluginProcess, SinkPluginClientWrapper, SourcePluginClientWrapper};
use pipeliner_core::runtime::{execute_pipeline, PipelineDefinition};
use pipeliner_proto::{RuntimeParams, SinkConfig, SourceConfig};

/// Find the compiled file plugin binary in the target directory.
fn plugin_binary_path() -> String {
    let mut path = std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("parent")
        .parent()
        .expect("parent")
        .to_path_buf();
    path.push("pipeliner-plugin-file");
    path.to_str().expect("path to string").to_string()
}

#[tokio::test]
async fn full_etl_pipeline_csv_to_json_and_csv() {
    // --- Create input CSV file ---
    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount,active").unwrap();
        writeln!(f, "Alice,42.50,true").unwrap();
        writeln!(f, "Bob,-10.00,false").unwrap();
        writeln!(f, "Carol,100.00,true").unwrap();
        writeln!(f, "Dave,0.00,true").unwrap();
    }

    let json_output = dir.path().join("output.jsonl");
    let csv_output = dir.path().join("output.csv");

    // --- Spawn the file plugin (serves both source and sink) ---
    // We need separate plugin instances for source and each sink (each is a separate gRPC server).
    let mut source_plugin = PluginProcess::spawn("file-source", &plugin_binary_path())
        .await
        .expect("failed to spawn source plugin");
    let mut sink_json_plugin = PluginProcess::spawn("file-sink-json", &plugin_binary_path())
        .await
        .expect("failed to spawn json sink plugin");
    let mut sink_csv_plugin = PluginProcess::spawn("file-sink-csv", &plugin_binary_path())
        .await
        .expect("failed to spawn csv sink plugin");

    // Small delay for servers to be ready.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // --- Connect gRPC clients ---
    let source_client = SourcePluginClientWrapper::connect(source_plugin.address())
        .await
        .expect("failed to connect to source plugin");

    let sink_json_client = SinkPluginClientWrapper::connect(sink_json_plugin.address())
        .await
        .expect("failed to connect to json sink plugin");

    let sink_csv_client = SinkPluginClientWrapper::connect(sink_csv_plugin.address())
        .await
        .expect("failed to connect to csv sink plugin");

    // --- Define transform steps ---
    // 1. Coerce amount to float
    // 2. Rename "active" to "is_active"
    // 3. Filter: keep only positive amounts
    let transforms = vec![
        parse_step("set(.amount, to_float(.amount))").unwrap(),
        parse_step("rename(.active, .is_active)").unwrap(),
        parse_step("where(.amount > 0.0)").unwrap(),
    ];

    // --- Build pipeline definition ---
    let source_config_json = serde_json::json!({
        "path": csv_input.to_str().unwrap(),
        "format": "csv"
    });

    let json_sink_config_json = serde_json::json!({
        "path": json_output.to_str().unwrap(),
        "format": "json"
    });

    let csv_sink_config_json = serde_json::json!({
        "path": csv_output.to_str().unwrap(),
        "format": "csv"
    });

    let pipeline = PipelineDefinition {
        source: source_client,
        source_config: SourceConfig {
            config_json: source_config_json.to_string(),
        },
        source_params: RuntimeParams::default(),
        transforms,
        sinks: vec![sink_json_client, sink_csv_client],
        sink_configs: vec![
            SinkConfig {
                config_json: json_sink_config_json.to_string(),
            },
            SinkConfig {
                config_json: csv_sink_config_json.to_string(),
            },
        ],
    };

    // --- Execute the pipeline ---
    let result = execute_pipeline(pipeline).await.expect("pipeline failed");

    // --- Verify watermark ---
    assert!(!result.watermark.is_empty(), "watermark should be set");

    // --- Verify sink results ---
    assert_eq!(result.sink_results.len(), 2);
    // Alice (42.5) and Carol (100.0) pass the filter — Bob (-10) and Dave (0) do not.
    for sink_result in &result.sink_results {
        assert_eq!(
            sink_result.rows_written, 2,
            "sink {} should have written 2 rows, got {}",
            sink_result.index, sink_result.rows_written
        );
        assert_eq!(sink_result.rows_errored, 0);
        assert!(sink_result.error_message.is_empty());
    }

    // --- Verify JSON output file ---
    let json_content = std::fs::read_to_string(&json_output).unwrap();
    let json_lines: Vec<&str> = json_content.trim().lines().collect();
    assert_eq!(json_lines.len(), 2, "expected 2 JSON lines");

    let line1: serde_json::Value = serde_json::from_str(json_lines[0]).unwrap();
    let line2: serde_json::Value = serde_json::from_str(json_lines[1]).unwrap();

    // Check that records have the right fields after transforms.
    assert_eq!(line1["name"], "Alice");
    assert_eq!(line1["amount"], 42.5);
    // "active" should be renamed to "is_active".
    assert!(line1.get("active").is_none(), "active should be renamed");
    assert_eq!(line1["is_active"], "true"); // CSV reader emits strings, we only coerced amount.

    assert_eq!(line2["name"], "Carol");
    assert_eq!(line2["amount"], 100.0);

    // --- Verify CSV output file ---
    let csv_content = std::fs::read_to_string(&csv_output).unwrap();
    let csv_lines: Vec<&str> = csv_content.trim().lines().collect();
    assert_eq!(csv_lines.len(), 3, "expected header + 2 data rows");
    // Header should contain the renamed field.
    assert!(csv_lines[0].contains("is_active"), "header should have is_active");
    assert!(!csv_lines[0].contains(",active,"), "header should not have 'active'");

    // --- Cleanup ---
    source_plugin.kill().await.ok();
    sink_json_plugin.kill().await.ok();
    sink_csv_plugin.kill().await.ok();
}

#[tokio::test]
async fn pipeline_with_all_records_filtered() {
    // Test edge case: all records are filtered out by transforms.
    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount").unwrap();
        writeln!(f, "Alice,-5.0").unwrap();
        writeln!(f, "Bob,-10.0").unwrap();
    }

    let json_output = dir.path().join("output.jsonl");

    let mut source_plugin = PluginProcess::spawn("file-source", &plugin_binary_path())
        .await
        .expect("spawn");
    let mut sink_plugin = PluginProcess::spawn("file-sink", &plugin_binary_path())
        .await
        .expect("spawn");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let source_client = SourcePluginClientWrapper::connect(source_plugin.address())
        .await
        .unwrap();
    let sink_client = SinkPluginClientWrapper::connect(sink_plugin.address())
        .await
        .unwrap();

    let transforms = vec![
        parse_step("set(.amount, to_float(.amount))").unwrap(),
        parse_step("where(.amount > 0.0)").unwrap(),
    ];

    let pipeline = PipelineDefinition {
        source: source_client,
        source_config: SourceConfig {
            config_json: serde_json::json!({
                "path": csv_input.to_str().unwrap(),
                "format": "csv"
            })
            .to_string(),
        },
        source_params: RuntimeParams::default(),
        transforms,
        sinks: vec![sink_client],
        sink_configs: vec![SinkConfig {
            config_json: serde_json::json!({
                "path": json_output.to_str().unwrap(),
                "format": "json"
            })
            .to_string(),
        }],
    };

    let result = execute_pipeline(pipeline).await.expect("pipeline failed");

    // All records filtered — sink writes 0 rows.
    assert_eq!(result.sink_results.len(), 1);
    assert_eq!(result.sink_results[0].rows_written, 0);

    // Output file should be empty (or contain nothing).
    let content = std::fs::read_to_string(&json_output).unwrap();
    assert!(content.trim().is_empty(), "output should be empty");

    source_plugin.kill().await.ok();
    sink_plugin.kill().await.ok();
}
