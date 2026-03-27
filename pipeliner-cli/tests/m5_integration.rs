//! M5 integration test: CLI + pipeline config loader.
//!
//! Tests the full flow: TOML config → spawn plugins → execute pipeline → verify output.
//! Also tests the `validate` and `schema` commands via the library API.

use std::io::Write;
use std::time::Duration;

use pipeliner_core::config::{
    build_pipeline, build_runtime_params, parse_pipeline_config, toml_value_to_json,
    validate_config, ConnectorEntry, ConnectorRegistry,
};
use pipeliner_core::connector::{ConnectorProcess, SourceConnectorClientWrapper};
use pipeliner_core::runtime::execute_pipeline;
use pipeliner_proto::{RuntimeParams, SourceConfig};

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
    let mut connectors = std::collections::HashMap::new();
    connectors.insert(
        "file".to_string(),
        ConnectorEntry {
            path: connector_binary_path(),
            description: "File source and sink".to_string(),
        },
    );
    ConnectorRegistry { connectors }
}

#[tokio::test]
async fn run_pipeline_from_toml_config() {
    // --- Create input CSV and output paths ---
    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "name,amount,active").unwrap();
        writeln!(f, "Alice,42.50,true").unwrap();
        writeln!(f, "Bob,-10.00,false").unwrap();
        writeln!(f, "Carol,100.00,true").unwrap();
    }

    let json_output = dir.path().join("output.jsonl");

    // --- Write pipeline TOML config ---
    let toml_content = format!(
        r#"
[pipeline]
name = "test_csv_to_json"
description = "Integration test pipeline"

[source]
connector = "file"
config.path = "{input}"
config.format = "csv"

[[transforms]]
name = "clean"
steps = [
    'set(.amount, to_float(.amount))',
    'rename(.active, .is_active)',
    'where(.amount > 0.0)',
]

[[sinks]]
connector = "file"
config.path = "{output}"
config.format = "json"
"#,
        input = csv_input.to_str().unwrap().replace('\\', "\\\\"),
        output = json_output.to_str().unwrap().replace('\\', "\\\\"),
    );

    let config = parse_pipeline_config(&toml_content).unwrap();
    let registry = test_registry();
    let runtime_params = build_runtime_params(&[]);

    // --- Execute pipeline ---
    let (definition, mut spawned) = build_pipeline(&config, &registry, runtime_params)
        .await
        .expect("build_pipeline failed");

    let result = execute_pipeline(definition).await.expect("pipeline failed");
    spawned.kill_all().await;

    // --- Verify results ---
    assert!(!result.watermark.is_empty(), "watermark should be set");
    assert_eq!(result.sink_results.len(), 1);
    assert_eq!(result.sink_results[0].rows_written, 2); // Alice + Carol
    assert_eq!(result.sink_results[0].rows_errored, 0);

    // Verify JSON output.
    let json_content = std::fs::read_to_string(&json_output).unwrap();
    let lines: Vec<&str> = json_content.trim().lines().collect();
    assert_eq!(lines.len(), 2);

    let line1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(line1["name"], "Alice");
    assert_eq!(line1["amount"], 42.5);
    assert!(line1.get("active").is_none(), "active should be renamed");
    assert_eq!(line1["is_active"], "true");

    let line2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(line2["name"], "Carol");
    assert_eq!(line2["amount"], 100.0);
}

#[tokio::test]
async fn run_pipeline_with_multiple_sinks_from_toml() {
    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("input.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "id,value").unwrap();
        writeln!(f, "1,hello").unwrap();
        writeln!(f, "2,world").unwrap();
    }

    let json_output = dir.path().join("out.jsonl");
    let csv_output = dir.path().join("out.csv");

    let toml_content = format!(
        r#"
[pipeline]
name = "multi_sink"

[source]
connector = "file"
config.path = "{input}"
config.format = "csv"

[[transforms]]
name = "passthrough"
steps = []

[[sinks]]
connector = "file"
config.path = "{json_out}"
config.format = "json"

[[sinks]]
connector = "file"
config.path = "{csv_out}"
config.format = "csv"
"#,
        input = csv_input.to_str().unwrap().replace('\\', "\\\\"),
        json_out = json_output.to_str().unwrap().replace('\\', "\\\\"),
        csv_out = csv_output.to_str().unwrap().replace('\\', "\\\\"),
    );

    let config = parse_pipeline_config(&toml_content).unwrap();
    let registry = test_registry();
    let runtime_params = build_runtime_params(&[]);

    let (definition, mut spawned) = build_pipeline(&config, &registry, runtime_params)
        .await
        .expect("build_pipeline failed");

    let result = execute_pipeline(definition).await.expect("pipeline failed");
    spawned.kill_all().await;

    assert_eq!(result.sink_results.len(), 2);
    for sr in &result.sink_results {
        assert_eq!(sr.rows_written, 2);
    }

    // Verify both output files exist with content.
    assert!(json_output.exists());
    assert!(csv_output.exists());
    assert_eq!(
        std::fs::read_to_string(&json_output)
            .unwrap()
            .trim()
            .lines()
            .count(),
        2
    );
    assert_eq!(
        std::fs::read_to_string(&csv_output)
            .unwrap()
            .trim()
            .lines()
            .count(),
        3
    ); // header + 2 rows
}

#[test]
fn validate_config_catches_bad_transform() {
    let toml_content = r#"
[pipeline]
name = "bad_transforms"

[source]
connector = "file"

[[transforms]]
name = "broken"
steps = [
    'this is not a valid step!!!',
]

[[sinks]]
connector = "file"
"#;
    let config = parse_pipeline_config(toml_content).unwrap();
    // Use an empty registry — plugin resolution will fail but that's separate.
    let registry = ConnectorRegistry::default();
    let errors = validate_config(&config, &registry);

    // Should have errors for: source connector not found, sink connector not found, bad transform.
    let has_transform_error = errors.iter().any(|e| e.contains("invalid step"));
    assert!(
        has_transform_error,
        "expected transform parse error, got: {errors:?}"
    );
}

#[test]
fn validate_config_catches_missing_plugin() {
    let toml_content = r#"
[pipeline]
name = "missing_plugin"

[source]
connector = "nonexistent"

[[transforms]]
name = "noop"
steps = []

[[sinks]]
connector = "also-nonexistent"
"#;
    let config = parse_pipeline_config(toml_content).unwrap();
    let registry = ConnectorRegistry::default();
    let errors = validate_config(&config, &registry);

    assert!(errors.iter().any(|e| e.contains("nonexistent")));
    assert!(errors.iter().any(|e| e.contains("also-nonexistent")));
}

#[test]
fn parse_config_with_env_var_substitution() {
    std::env::set_var("PIPELINER_TEST_INPUT_PATH", "/tmp/test_input.csv");

    let toml_content = r#"
[pipeline]
name = "env_test"

[source]
connector = "file"
config.path = "${PIPELINER_TEST_INPUT_PATH}"
config.format = "csv"

[[transforms]]
name = "noop"
steps = []

[[sinks]]
connector = "file"
config.path = "/tmp/output.json"
config.format = "json"
"#;
    let config = parse_pipeline_config(toml_content).unwrap();
    let source_json = toml_value_to_json(&config.source.config);
    let parsed: serde_json::Value = serde_json::from_str(&source_json).unwrap();
    assert_eq!(parsed["path"], "/tmp/test_input.csv");

    std::env::remove_var("PIPELINER_TEST_INPUT_PATH");
}

#[tokio::test]
async fn schema_discovery_from_config() {
    let dir = tempfile::tempdir().unwrap();
    let csv_input = dir.path().join("schema_test.csv");
    {
        let mut f = std::fs::File::create(&csv_input).unwrap();
        writeln!(f, "col_a,col_b,col_c").unwrap();
        writeln!(f, "1,hello,true").unwrap();
    }

    let registry = test_registry();
    let source_binary =
        pipeliner_core::config::resolve_connector_binary("file", &registry).unwrap();

    let mut process = ConnectorProcess::spawn("file", source_binary.to_str().unwrap())
        .await
        .expect("spawn");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut client = SourceConnectorClientWrapper::connect(process.address())
        .await
        .expect("connect");

    let source_config = SourceConfig {
        config_json: serde_json::json!({
            "path": csv_input.to_str().unwrap(),
            "format": "csv"
        })
        .to_string(),
    };

    let schema = client
        .discover_schema(source_config, RuntimeParams::default())
        .await
        .expect("discover_schema");

    assert!(!schema.columns.is_empty());
    let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(col_names.contains(&"col_a"));
    assert!(col_names.contains(&"col_b"));
    assert!(col_names.contains(&"col_c"));

    process.kill().await.ok();
}
