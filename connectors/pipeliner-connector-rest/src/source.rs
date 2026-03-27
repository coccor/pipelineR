//! REST source connector — implements the `Source` trait for HTTP/JSON APIs.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pipeliner_core::record::{RecordBatch, DEFAULT_BATCH_SIZE};
use pipeliner_proto::{
    ColumnSchema, Partition, RuntimeParams, SchemaResponse, SourceConfig, SourceDescriptor,
};
use pipeliner_sdk::error::{DiscoveryError, ExtractionError, ValidationError};
use pipeliner_sdk::Source;
use reqwest::Client;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

use crate::auth::{apply_auth_to_request, fetch_oauth2_token};
use crate::config::{AuthConfig, HttpMethod, PaginationConfig, RestSourceConfig};
use crate::error::RestError;
use crate::pagination::{apply_page_state, next_page, NextPage, PageContext, PageState};
use crate::response::{extract_path, infer_type, json_to_record};

/// The REST API source connector.
pub struct RestSource;

/// Parse config JSON into `RestSourceConfig`.
fn parse_rest_config(config: &SourceConfig) -> Result<RestSourceConfig, ValidationError> {
    pipeliner_sdk::parse_config(&config.config_json)
}

/// Build the base URL from config fields.
fn build_url(cfg: &RestSourceConfig) -> Result<url::Url, RestError> {
    let full = format!(
        "{}{}",
        cfg.base_url.trim_end_matches('/'),
        cfg.endpoint
    );
    url::Url::parse(&full).map_err(|e| RestError::InvalidUrl(e.to_string()))
}

/// Apply static query params from config to a URL.
fn apply_query_params(url: &mut url::Url, params: &Option<HashMap<String, String>>) {
    if let Some(qp) = params {
        let mut pairs = url.query_pairs_mut();
        for (k, v) in qp {
            pairs.append_pair(k, v);
        }
    }
}

/// Apply initial pagination params (for the first request).
fn apply_initial_pagination(url: &mut url::Url, pagination: &Option<PaginationConfig>) {
    if let Some(ref pg) = pagination {
        match pg {
            PaginationConfig::Offset {
                offset_param,
                limit_param,
                limit,
            } => {
                url.query_pairs_mut()
                    .append_pair(offset_param, "0")
                    .append_pair(limit_param, &limit.to_string());
            }
            PaginationConfig::PageNumber {
                page_param,
                page_size_param,
                page_size,
            } => {
                url.query_pairs_mut()
                    .append_pair(page_param, "1");
                if let (Some(param), Some(size)) = (page_size_param, page_size) {
                    url.query_pairs_mut()
                        .append_pair(param, &size.to_string());
                }
            }
            _ => {}
        }
    }
}

/// Fetch one page of data from the API.
async fn fetch_page(
    client: &Client,
    url: &url::Url,
    cfg: &RestSourceConfig,
    oauth_token: Option<&str>,
) -> Result<(JsonValue, reqwest::header::HeaderMap), RestError> {
    let method = cfg.effective_method();

    let mut builder = match method {
        HttpMethod::Get => client.get(url.as_str()),
        HttpMethod::Post => {
            let mut b = client.post(url.as_str());
            if let Some(ref tmpl) = cfg.body_template {
                b = b
                    .header("Content-Type", "application/json")
                    .body(tmpl.clone());
            }
            b
        }
    };

    // Apply custom headers.
    if let Some(ref hdrs) = cfg.headers {
        for (k, v) in hdrs {
            builder = builder.header(k.as_str(), v.as_str());
        }
    }

    // Apply auth.
    if let Some(ref auth) = cfg.auth {
        builder = apply_auth_to_request(builder, auth, oauth_token);
    }

    let resp = builder.send().await.map_err(RestError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(RestError::HttpStatus {
            status: status.as_u16(),
            body,
        });
    }
    let headers = resp.headers().clone();
    let body: JsonValue = resp
        .json()
        .await
        .map_err(|e| RestError::JsonParse(e.to_string()))?;

    Ok((body, headers))
}

/// Wait for rate limiting if configured.
async fn rate_limit_wait(
    cfg: &RestSourceConfig,
    last_request: &mut Option<Instant>,
) {
    if let Some(ref rl) = cfg.rate_limit {
        if rl.requests_per_second > 0.0 {
            let interval = Duration::from_secs_f64(1.0 / rl.requests_per_second);
            if let Some(last) = last_request {
                let elapsed = last.elapsed();
                if elapsed < interval {
                    tokio::time::sleep(interval - elapsed).await;
                }
            }
        }
    }
    *last_request = Some(Instant::now());
}

/// Extract records from a JSON response body using the configured records path.
fn extract_records(
    body: &JsonValue,
    records_path: &str,
) -> Result<Vec<serde_json::Value>, RestError> {
    let arr_val = extract_path(body, records_path)
        .ok_or_else(|| RestError::PathNotFound(records_path.to_string()))?;

    match arr_val {
        JsonValue::Array(arr) => Ok(arr.clone()),
        _ => Err(RestError::NotAnArray(records_path.to_string())),
    }
}

/// Compute the maximum watermark value from records.
fn compute_watermark(
    records: &[serde_json::Value],
    watermark_field: &str,
    current_max: &str,
) -> String {
    let mut max = current_max.to_string();
    for rec in records {
        if let Some(val) = rec.get(watermark_field) {
            let s = match val {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                _ => continue,
            };
            if s > max {
                max = s;
            }
        }
    }
    max
}

#[async_trait]
impl Source for RestSource {
    /// Return metadata about the REST source connector.
    fn describe(&self) -> SourceDescriptor {
        SourceDescriptor {
            name: "rest".to_string(),
            version: "0.1.0".to_string(),
            description: "REST API source connector — fetches JSON data from HTTP endpoints"
                .to_string(),
        }
    }

    /// Validate the REST source configuration.
    async fn validate(&self, config: &SourceConfig) -> Result<(), ValidationError> {
        let cfg = parse_rest_config(config)?;

        if cfg.base_url.is_empty() {
            return Err(ValidationError::MissingField("base_url".to_string()));
        }
        if cfg.endpoint.is_empty() {
            return Err(ValidationError::MissingField("endpoint".to_string()));
        }
        if cfg.response_mapping.records_path.is_empty() {
            return Err(ValidationError::MissingField(
                "response_mapping.records_path".to_string(),
            ));
        }

        // Validate URL can be constructed.
        build_url(&cfg).map_err(|e| ValidationError::InvalidConfig(e.to_string()))?;

        Ok(())
    }

    /// Discover the schema by fetching the first page and inferring types from the first record.
    async fn discover_schema(
        &self,
        config: &SourceConfig,
        _params: &RuntimeParams,
    ) -> Result<SchemaResponse, DiscoveryError> {
        let cfg = parse_rest_config(config).map_err(|e| DiscoveryError::Failed(e.to_string()))?;
        let client = Client::new();

        // Fetch OAuth2 token if needed.
        let oauth_token = resolve_oauth_token(&client, &cfg)
            .await
            .map_err(|e| DiscoveryError::Connection(e.to_string()))?;

        let mut url = build_url(&cfg).map_err(|e| DiscoveryError::Failed(e.to_string()))?;
        apply_query_params(&mut url, &cfg.query_params);
        apply_initial_pagination(&mut url, &cfg.pagination);

        let (body, _headers) = fetch_page(&client, &url, &cfg, oauth_token.as_deref())
            .await
            .map_err(|e| DiscoveryError::Connection(e.to_string()))?;

        let records = extract_records(&body, &cfg.response_mapping.records_path)
            .map_err(|e| DiscoveryError::Failed(e.to_string()))?;

        let first = records
            .first()
            .ok_or_else(|| DiscoveryError::Failed("no records in response".to_string()))?;

        let columns = match first {
            JsonValue::Object(map) => map
                .iter()
                .map(|(name, val)| ColumnSchema {
                    name: name.clone(),
                    data_type: infer_type(val).to_string(),
                    nullable: true,
                    description: String::new(),
                })
                .collect(),
            _ => vec![],
        };

        Ok(SchemaResponse { columns })
    }

    /// Discover partitions for the REST source.
    ///
    /// Returns a single partition by default. If a `partition_param` runtime parameter
    /// is provided (comma-separated values), each value becomes a separate partition.
    async fn discover_partitions(
        &self,
        _config: &SourceConfig,
        params: &RuntimeParams,
    ) -> Result<Vec<Partition>, DiscoveryError> {
        // Check for partition_values runtime param.
        if let Some(values) = params.params.get("partition_values") {
            let param_name = params
                .params
                .get("partition_param")
                .cloned()
                .unwrap_or_else(|| "partition".to_string());

            let partitions = values
                .split(',')
                .map(|v| {
                    let v = v.trim().to_string();
                    let mut p = HashMap::new();
                    p.insert(param_name.clone(), v.clone());
                    Partition {
                        key: v,
                        params: p,
                    }
                })
                .collect();

            return Ok(partitions);
        }

        // Default: single partition.
        Ok(vec![Partition {
            key: "default".to_string(),
            params: HashMap::new(),
        }])
    }

    /// Extract data from the REST API, handling pagination and rate limiting.
    async fn extract(
        &self,
        config: &SourceConfig,
        params: &RuntimeParams,
        tx: mpsc::Sender<pipeliner_proto::RecordBatch>,
    ) -> Result<String, ExtractionError> {
        let cfg =
            parse_rest_config(config).map_err(|e| ExtractionError::Failed(e.to_string()))?;
        let client = Client::new();

        // Fetch OAuth2 token if needed.
        let oauth_token = resolve_oauth_token(&client, &cfg)
            .await
            .map_err(|e| ExtractionError::Connection(e.to_string()))?;

        let base_url = build_url(&cfg).map_err(|e| ExtractionError::Failed(e.to_string()))?;

        let mut watermark = String::new();
        let mut last_request: Option<Instant> = None;
        let mut page_state: Option<PageState> = None;
        let mut current_offset: u64 = 0;
        let mut current_page: u64 = 1;

        let convert = pipeliner_sdk::convert::core_batch_to_proto;

        loop {
            // Rate limit.
            rate_limit_wait(&cfg, &mut last_request).await;

            // Build URL for this page.
            let mut url = base_url.clone();
            apply_query_params(&mut url, &cfg.query_params);

            // Apply partition-specific params.
            {
                let mut pairs = url.query_pairs_mut();
                for (k, v) in &params.params {
                    pairs.append_pair(k, v);
                }
            }

            // Apply pagination.
            if let Some(ref state) = page_state {
                if let PageState::LinkUrl(ref link_url) = state {
                    // For link-header pagination, use the full URL directly.
                    url = url::Url::parse(link_url)
                        .map_err(|e| ExtractionError::Failed(e.to_string()))?;
                } else if let Some(ref pg) = cfg.pagination {
                    apply_page_state(&mut url, pg, state);
                }
            } else {
                // First page — apply initial pagination params.
                apply_initial_pagination(&mut url, &cfg.pagination);
            }

            // Fetch page.
            let (body, headers) =
                fetch_page(&client, &url, &cfg, oauth_token.as_deref())
                    .await
                    .map_err(|e| ExtractionError::Connection(e.to_string()))?;

            // Extract records.
            let json_records =
                extract_records(&body, &cfg.response_mapping.records_path)
                    .map_err(|e| ExtractionError::Failed(e.to_string()))?;

            let records_count = json_records.len() as u64;

            // Update watermark.
            if let Some(ref wf) = cfg.watermark_field {
                watermark = compute_watermark(&json_records, wf, &watermark);
            }

            // Convert to pipelineR records and send in batches.
            let records: Vec<_> = json_records.iter().map(json_to_record).collect();

            for chunk in records.chunks(DEFAULT_BATCH_SIZE) {
                let batch = RecordBatch::new(chunk.to_vec());
                tx.send(convert(&batch))
                    .await
                    .map_err(|_| ExtractionError::ChannelClosed)?;
            }

            // Check pagination.
            if let Some(ref pg) = cfg.pagination {
                let ctx = PageContext {
                    config: pg,
                    body: &body,
                    headers: &headers,
                    cursor_path: cfg.response_mapping.cursor_path.as_deref(),
                    total_path: cfg.response_mapping.total_path.as_deref(),
                    current_offset,
                    records_count,
                    current_page,
                };
                let next = next_page(&ctx);

                match next {
                    NextPage::Continue(state) => {
                        // Update tracking state.
                        match &state {
                            PageState::Offset(o) => current_offset = *o,
                            PageState::PageNumber(p) => current_page = *p,
                            _ => {}
                        }
                        page_state = Some(state);
                    }
                    NextPage::Done => break,
                }
            } else {
                // No pagination configured — single request.
                break;
            }
        }

        Ok(watermark)
    }
}

/// Resolve an OAuth2 token if the config uses OAuth2 auth.
async fn resolve_oauth_token(
    client: &Client,
    cfg: &RestSourceConfig,
) -> Result<Option<String>, RestError> {
    match &cfg.auth {
        Some(AuthConfig::OAuth2 {
            token_url,
            client_id,
            client_secret,
            scope,
        }) => {
            let token =
                fetch_oauth2_token(client, token_url, client_id, client_secret, scope.as_deref())
                    .await?;
            Ok(Some(token))
        }
        _ => Ok(None),
    }
}
