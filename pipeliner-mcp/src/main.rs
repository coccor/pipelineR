//! pipeliner MCP server — exposes pipeline creation tools to AI assistants
//! via the Model Context Protocol (JSON-RPC 2.0 over stdio).

use std::io::{self, BufRead, Read as _, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use pipeliner_core::config::{
    load_connector_registry, parse_pipeline_config, validate_config, ConnectorRegistry,
};

// ---------------------------------------------------------------------------
// JSON-RPC types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError { code, message }),
        }
    }
}

// ---------------------------------------------------------------------------
// MCP protocol types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct McpTool {
    name: &'static str,
    description: &'static str,
    #[serde(rename = "inputSchema")]
    input_schema: Value,
}

#[derive(Debug, Serialize)]
struct McpToolResult {
    content: Vec<McpContent>,
    #[serde(rename = "isError", skip_serializing_if = "std::ops::Not::not")]
    is_error: bool,
}

#[derive(Debug, Serialize)]
struct McpContent {
    #[serde(rename = "type")]
    content_type: &'static str,
    text: String,
}

impl McpToolResult {
    fn text(s: String) -> Self {
        Self {
            content: vec![McpContent {
                content_type: "text",
                text: s,
            }],
            is_error: false,
        }
    }

    fn error(s: String) -> Self {
        Self {
            content: vec![McpContent {
                content_type: "text",
                text: s,
            }],
            is_error: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Tool definitions
// ---------------------------------------------------------------------------

fn tool_definitions() -> Vec<McpTool> {
    vec![
        McpTool {
            name: "list_connectors",
            description: "List installed pipeliner connectors and their descriptions. Call this first to discover what source and sink connectors are available.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        McpTool {
            name: "get_pipeline_spec",
            description: "Get the full pipeliner specification: all connector config schemas, DSL transform syntax, built-in functions, expression grammar, and complete examples. Use this to understand how to build a valid pipeline TOML config.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {},
                "required": []
            }),
        },
        McpTool {
            name: "validate_pipeline",
            description: "Validate a pipeline TOML config string. Returns validation errors or confirms the config is valid. Use this after generating a pipeline to check correctness before writing to disk.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "toml_config": {
                        "type": "string",
                        "description": "The full pipeline TOML configuration to validate"
                    }
                },
                "required": ["toml_config"]
            }),
        },
        McpTool {
            name: "create_pipeline",
            description: "Create a pipeline TOML config file on disk. Validates the config first, then writes it to the specified path. Returns the file path on success or validation errors on failure.",
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "toml_config": {
                        "type": "string",
                        "description": "The full pipeline TOML configuration"
                    },
                    "output_path": {
                        "type": "string",
                        "description": "File path where the pipeline TOML should be written (e.g. 'pipelines/my_pipeline.toml')"
                    }
                },
                "required": ["toml_config", "output_path"]
            }),
        },
    ]
}

// ---------------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------------

fn handle_list_connectors(registry: &ConnectorRegistry) -> McpToolResult {
    let connectors: Vec<Value> = registry
        .connectors
        .iter()
        .map(|(name, entry)| {
            serde_json::json!({
                "name": name,
                "description": entry.description,
            })
        })
        .collect();

    let result = serde_json::json!({
        "installed_connectors": connectors,
        "known_source_connectors": ["file", "sql", "rest"],
        "known_sink_connectors": ["file", "sql", "delta", "parquet"],
        "hint": "Use get_pipeline_spec to see full config schemas for each connector."
    });

    McpToolResult::text(serde_json::to_string_pretty(&result).unwrap())
}

fn handle_get_pipeline_spec(registry: &ConnectorRegistry) -> McpToolResult {
    let installed: Value = registry
        .connectors
        .iter()
        .map(|(name, entry)| {
            serde_json::json!({ "name": name, "description": entry.description })
        })
        .collect();

    let spec = build_full_spec(installed);
    McpToolResult::text(serde_json::to_string_pretty(&spec).unwrap())
}

fn handle_validate_pipeline(
    params: &Value,
    registry: &ConnectorRegistry,
) -> McpToolResult {
    let toml_str = match params.get("toml_config").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return McpToolResult::error("Missing required parameter: toml_config".into()),
    };

    // Parse the TOML config.
    let config = match parse_pipeline_config(toml_str) {
        Ok(c) => c,
        Err(e) => {
            return McpToolResult::error(format!("Parse error: {e}"));
        }
    };

    // Validate connector references and transform steps.
    let errors = validate_config(&config, registry);
    if errors.is_empty() {
        McpToolResult::text(serde_json::json!({
            "valid": true,
            "pipeline_name": config.pipeline.name,
            "source": config.source.connector,
            "transforms": config.transforms.len(),
            "sinks": config.sinks.len(),
            "message": "Pipeline configuration is valid."
        }).to_string())
    } else {
        McpToolResult::error(serde_json::json!({
            "valid": false,
            "errors": errors,
            "hint": "Fix the errors above and try again."
        }).to_string())
    }
}

fn handle_create_pipeline(
    params: &Value,
    registry: &ConnectorRegistry,
) -> McpToolResult {
    let toml_str = match params.get("toml_config").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return McpToolResult::error("Missing required parameter: toml_config".into()),
    };
    let output_path = match params.get("output_path").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return McpToolResult::error("Missing required parameter: output_path".into()),
    };

    // Validate first.
    let config = match parse_pipeline_config(toml_str) {
        Ok(c) => c,
        Err(e) => {
            return McpToolResult::error(format!("Parse error: {e}"));
        }
    };

    let errors = validate_config(&config, registry);
    if !errors.is_empty() {
        return McpToolResult::error(serde_json::json!({
            "valid": false,
            "errors": errors,
            "hint": "Fix the errors above before creating the pipeline file."
        }).to_string());
    }

    // Write to disk.
    let path = Path::new(output_path);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return McpToolResult::error(format!(
                    "Failed to create directory {}: {e}",
                    parent.display()
                ));
            }
        }
    }

    if let Err(e) = std::fs::write(path, toml_str) {
        return McpToolResult::error(format!("Failed to write file: {e}"));
    }

    McpToolResult::text(serde_json::json!({
        "created": true,
        "path": output_path,
        "pipeline_name": config.pipeline.name,
        "message": format!("Pipeline '{}' written to {}", config.pipeline.name, output_path),
        "next_steps": [
            format!("Validate: pipeliner validate {output_path}"),
            format!("Run: pipeliner run {output_path}"),
        ]
    }).to_string())
}

// ---------------------------------------------------------------------------
// Full specification builder (same as CLI ai-spec)
// ---------------------------------------------------------------------------

fn build_full_spec(installed_connectors: Value) -> Value {
    serde_json::json!({
        "version": "1",
        "description": "pipeliner pipeline configuration specification for AI-assisted pipeline creation",
        "config_format": "toml",
        "installed_connectors": installed_connectors,
        "source_connectors": {
            "file": {
                "description": "Read CSV, JSON, or Parquet files from local filesystem or cloud storage",
                "config": {
                    "path": { "type": "string", "required": true, "description": "File path or glob pattern" },
                    "format": { "type": "string", "required": true, "enum": ["csv", "json", "parquet"] },
                    "csv": {
                        "type": "object", "required": false,
                        "fields": {
                            "delimiter": { "type": "char", "default": "," },
                            "quote": { "type": "char", "default": "\"" },
                            "has_header": { "type": "bool", "default": true }
                        }
                    },
                    "storage": {
                        "type": "object", "required": false,
                        "variants": {
                            "local": "Local filesystem (default)",
                            "s3": { "fields": { "region": "string (optional)", "endpoint": "string (optional)" } },
                            "azure": { "fields": { "connection_string": "string (optional)" } },
                            "gcs": { "fields": { "project_id": "string (optional)" } }
                        }
                    }
                }
            },
            "sql": {
                "description": "Query PostgreSQL, MySQL, or SQL Server databases",
                "config": {
                    "driver": { "type": "string", "required": true, "enum": ["postgres", "mysql", "sqlserver"] },
                    "connection_string": { "type": "string", "required": true, "description": "Use ${ENV_VAR} for secrets" },
                    "query": { "type": "string", "required": true },
                    "watermark_column": { "type": "string", "required": false },
                    "partition": {
                        "type": "object", "required": false,
                        "strategies": {
                            "single": "No partitioning",
                            "date_range": { "fields": { "column": "string", "start": "ISO 8601", "end": "ISO 8601", "interval": "e.g. 1d, 7d, 1m" } },
                            "key_range": { "fields": { "column": "string", "values": "array of strings" } }
                        }
                    }
                }
            },
            "rest": {
                "description": "Fetch from REST APIs with pagination, auth, and rate limiting",
                "config": {
                    "base_url": { "type": "string", "required": true },
                    "endpoint": { "type": "string", "required": true },
                    "method": { "type": "string", "default": "GET", "enum": ["GET", "POST"] },
                    "headers": { "type": "map<string,string>", "required": false },
                    "query_params": { "type": "map<string,string>", "required": false },
                    "auth": {
                        "type": "object", "required": false,
                        "variants": {
                            "bearer": { "fields": { "token": "string" } },
                            "api_key": { "fields": { "header": "string (optional)", "query_param": "string (optional)", "value": "string" } },
                            "oauth2": { "fields": { "token_url": "string", "client_id": "string", "client_secret": "string", "scope": "string (optional)" } }
                        }
                    },
                    "pagination": {
                        "type": "object", "required": false,
                        "variants": {
                            "cursor": { "fields": { "cursor_field": "string", "cursor_param": "string" } },
                            "offset": { "fields": { "offset_param": "string", "limit_param": "string", "limit": "integer" } },
                            "page_number": { "fields": { "page_param": "string", "page_size_param": "string (optional)", "page_size": "integer (optional)" } },
                            "link_header": "RFC 8288 Link header"
                        }
                    },
                    "response_mapping": {
                        "type": "object", "required": true,
                        "fields": {
                            "records_path": "string (dot-path to records array)",
                            "total_path": "string (optional)",
                            "cursor_path": "string (optional)"
                        }
                    },
                    "rate_limit": { "fields": { "requests_per_second": "float" } },
                    "retry": { "fields": { "max_retries": "int (default 3)", "initial_backoff_ms": "int (default 1000)", "max_backoff_ms": "int (default 30000)" } }
                }
            }
        },
        "sink_connectors": {
            "file": {
                "description": "Write CSV or JSON files",
                "config": {
                    "path": { "type": "string", "required": true },
                    "format": { "type": "string", "required": true, "enum": ["csv", "json"] }
                }
            },
            "sql": {
                "description": "Write to PostgreSQL, MySQL, or SQL Server",
                "config": {
                    "driver": { "type": "string", "required": true, "enum": ["postgres", "mysql", "sqlserver"] },
                    "connection_string": { "type": "string", "required": true },
                    "table": { "type": "string", "required": true },
                    "write_mode": { "type": "string", "default": "insert", "enum": ["insert", "upsert", "truncate_and_load"] },
                    "merge_keys": { "type": "array<string>", "required": false, "description": "Required for upsert" },
                    "batch_size": { "type": "integer", "default": 1000 }
                }
            },
            "delta": {
                "description": "Write to Delta Lake tables",
                "config": {
                    "table_uri": { "type": "string", "required": true },
                    "write_mode": { "type": "string", "default": "append", "enum": ["append", "overwrite", "merge"] },
                    "partition_columns": { "type": "array<string>", "required": false },
                    "merge_keys": { "type": "array<string>", "required": false, "description": "Required for merge" }
                }
            },
            "parquet": {
                "description": "Write Parquet files",
                "config": {
                    "path": { "type": "string", "required": true },
                    "compression": { "type": "string", "default": "snappy", "enum": ["snappy", "zstd", "gzip", "none"] },
                    "partition_columns": { "type": "array<string>", "required": false }
                }
            }
        },
        "dsl": {
            "steps": {
                "set": { "syntax": "set(.field, expression)", "description": "Evaluate expression, assign to field" },
                "rename": { "syntax": "rename(.old, .new)", "description": "Rename a field" },
                "remove": { "syntax": "remove(.field)", "description": "Delete a field" },
                "where": { "syntax": "where(predicate)", "description": "Keep records where predicate is true" }
            },
            "functions": {
                "to_string": "to_string(value) -> String",
                "to_int": "to_int(value) -> Int",
                "to_float": "to_float(value) -> Float",
                "to_bool": "to_bool(value) -> Bool",
                "to_timestamp": "to_timestamp(string, format) -> Timestamp",
                "parse_timestamp": "parse_timestamp(string, format) -> Timestamp (alias)",
                "is_null": "is_null(value) -> Bool",
                "concat": "concat(str1, str2, ...) -> String",
                "coalesce": "coalesce(val1, val2, ...) -> Value (first non-null)"
            },
            "expressions": {
                "field_access": ".field, .nested.field, .array[0]",
                "literals": "42, 3.14, \"string\", true, false, null",
                "arithmetic": "+, -, *, /",
                "comparison": "==, !=, >, >=, <, <=",
                "logical": "&&, ||",
                "null_coalesce": ".field ?? \"default\"",
                "conditional": "if expr { then } else { else }"
            }
        },
        "examples": [
            {
                "name": "CSV to JSON with transforms",
                "toml": "[pipeline]\nname = \"sales_etl\"\n\n[source]\nconnector = \"file\"\n[source.config]\npath = \"data/sales.csv\"\nformat = \"csv\"\n\n[[transforms]]\nname = \"clean\"\nsteps = [\n    'set(.amount, to_float(.amount))',\n    'set(.quantity, to_int(.quantity))',\n    'set(.total, .amount * to_float(.quantity))',\n    'where(.total > 0.0)',\n]\n\n[[sinks]]\nconnector = \"file\"\n[sinks.config]\npath = \"output/sales.json\"\nformat = \"json\"\n"
            },
            {
                "name": "PostgreSQL to Delta Lake",
                "toml": "[pipeline]\nname = \"orders_to_lake\"\n\n[source]\nconnector = \"sql\"\n[source.config]\ndriver = \"postgres\"\nconnection_string = \"${DATABASE_URL}\"\nquery = \"SELECT * FROM orders\"\nwatermark_column = \"updated_at\"\n\n[[transforms]]\nname = \"enrich\"\nsteps = [\n    'set(.region, coalesce(.region, \"unknown\"))',\n    'remove(.internal_notes)',\n]\n\n[[sinks]]\nconnector = \"delta\"\n[sinks.config]\ntable_uri = \"s3://data-lake/orders\"\nwrite_mode = \"merge\"\nmerge_keys = [\"order_id\"]\n"
            },
            {
                "name": "REST API to Parquet",
                "toml": "[pipeline]\nname = \"api_ingest\"\n\n[source]\nconnector = \"rest\"\n[source.config]\nbase_url = \"https://api.example.com\"\nendpoint = \"/v1/events\"\n[source.config.auth]\ntype = \"bearer\"\ntoken = \"${API_TOKEN}\"\n[source.config.pagination]\ntype = \"cursor\"\ncursor_field = \"next_cursor\"\ncursor_param = \"cursor\"\n[source.config.response_mapping]\nrecords_path = \"data\"\n\n[[transforms]]\nname = \"flatten\"\nsteps = [\n    'set(.event_type, .metadata.type)',\n    'set(.timestamp, to_timestamp(.created_at, \"%+\"))',\n    'remove(.metadata)',\n]\n\n[[sinks]]\nconnector = \"parquet\"\n[sinks.config]\npath = \"output/events.parquet\"\ncompression = \"zstd\"\n"
            }
        ],
        "workflow": [
            "1. Call list_connectors to see what's installed",
            "2. Ask the user: What is your data source? (file, database, API)",
            "3. Ask: What is the source location/connection details?",
            "4. Ask: What transformations do you need?",
            "5. Ask: Where should the output go?",
            "6. Call validate_pipeline to check the generated config",
            "7. Call create_pipeline to write it to disk"
        ]
    })
}

// ---------------------------------------------------------------------------
// MCP message I/O (Content-Length framed, like LSP)
// ---------------------------------------------------------------------------

fn read_message(reader: &mut impl BufRead) -> io::Result<Option<String>> {
    // Read headers until empty line.
    let mut content_length: Option<usize> = None;

    loop {
        let mut header = String::new();
        let bytes_read = reader.read_line(&mut header)?;
        if bytes_read == 0 {
            return Ok(None); // EOF
        }

        let header = header.trim();
        if header.is_empty() {
            break; // End of headers
        }

        if let Some(len_str) = header.strip_prefix("Content-Length:") {
            content_length = len_str.trim().parse().ok();
        }
    }

    let length = match content_length {
        Some(l) => l,
        None => return Err(io::Error::new(io::ErrorKind::InvalidData, "missing Content-Length")),
    };

    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    Ok(Some(String::from_utf8_lossy(&body).into_owned()))
}

fn write_message(writer: &mut impl Write, body: &str) -> io::Result<()> {
    write!(writer, "Content-Length: {}\r\n\r\n{}", body.len(), body)?;
    writer.flush()
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

fn main() {
    // Parse optional --connectors-file arg.
    let connectors_file = std::env::args()
        .skip(1)
        .find(|a| a.starts_with("--connectors-file="))
        .map(|a| PathBuf::from(a.strip_prefix("--connectors-file=").unwrap()))
        .unwrap_or_else(|| PathBuf::from("connectors.toml"));

    let registry = load_connector_registry(&connectors_file).unwrap_or_default();

    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    loop {
        let body = match read_message(&mut reader) {
            Ok(Some(b)) => b,
            Ok(None) => break, // EOF
            Err(_) => break,
        };

        let request: JsonRpcRequest = match serde_json::from_str(&body) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse::error(
                    Value::Null,
                    -32700,
                    format!("Parse error: {e}"),
                );
                let out = serde_json::to_string(&resp).unwrap();
                write_message(&mut writer, &out).ok();
                continue;
            }
        };

        let id = request.id.clone().unwrap_or(Value::Null);
        let response = handle_request(&request, &registry);

        // Notifications (no id) don't get a response.
        if request.id.is_some() {
            let resp = match response {
                Ok(result) => JsonRpcResponse::success(id, result),
                Err((code, msg)) => JsonRpcResponse::error(id, code, msg),
            };
            let out = serde_json::to_string(&resp).unwrap();
            write_message(&mut writer, &out).ok();
        }
    }
}

fn handle_request(
    req: &JsonRpcRequest,
    registry: &ConnectorRegistry,
) -> Result<Value, (i64, String)> {
    match req.method.as_str() {
        "initialize" => Ok(serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": "pipeliner-mcp",
                "version": env!("CARGO_PKG_VERSION")
            }
        })),

        "notifications/initialized" => Ok(Value::Null),

        "tools/list" => {
            let tools = tool_definitions();
            Ok(serde_json::json!({ "tools": tools }))
        }

        "tools/call" => {
            let tool_name = req
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or((-32602, "Missing tool name".to_string()))?;

            let arguments = req
                .params
                .get("arguments")
                .cloned()
                .unwrap_or(serde_json::json!({}));

            let result = match tool_name {
                "list_connectors" => handle_list_connectors(registry),
                "get_pipeline_spec" => handle_get_pipeline_spec(registry),
                "validate_pipeline" => handle_validate_pipeline(&arguments, registry),
                "create_pipeline" => handle_create_pipeline(&arguments, registry),
                _ => McpToolResult::error(format!("Unknown tool: {tool_name}")),
            };

            Ok(serde_json::to_value(&result).unwrap())
        }

        "ping" => Ok(serde_json::json!({})),

        _ => Err((-32601, format!("Method not found: {}", req.method))),
    }
}
