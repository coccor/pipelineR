//! Configuration types for the REST source connector.

use std::collections::HashMap;

use serde::Deserialize;

/// Top-level configuration for the REST source connector.
#[derive(Debug, Clone, Deserialize)]
pub struct RestSourceConfig {
    /// Base URL of the API (e.g., `https://api.example.com`).
    pub base_url: String,
    /// API endpoint path (e.g., `/api/v1/users`).
    pub endpoint: String,
    /// HTTP method. Defaults to GET.
    #[serde(default)]
    pub method: Option<HttpMethod>,
    /// Additional HTTP headers to include in every request.
    #[serde(default)]
    pub headers: Option<HashMap<String, String>>,
    /// Query parameters to include in every request.
    #[serde(default)]
    pub query_params: Option<HashMap<String, String>>,
    /// JSON body template for POST requests.
    #[serde(default)]
    pub body_template: Option<String>,
    /// Authentication configuration.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    /// Pagination configuration.
    #[serde(default)]
    pub pagination: Option<PaginationConfig>,
    /// Response mapping configuration.
    pub response_mapping: ResponseMapping,
    /// Rate limiting configuration.
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    /// Optional field name whose maximum value is returned as the watermark.
    #[serde(default)]
    pub watermark_field: Option<String>,
}

impl RestSourceConfig {
    /// Return the effective HTTP method (defaults to GET).
    pub fn effective_method(&self) -> HttpMethod {
        self.method.clone().unwrap_or(HttpMethod::Get)
    }
}

/// Supported HTTP methods.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
}

/// Authentication configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// API key authentication — sent as a header or query parameter.
    ApiKey {
        /// Header name to send the API key in (e.g., `X-API-Key`).
        #[serde(default)]
        header: Option<String>,
        /// Query parameter name to send the API key in.
        #[serde(default)]
        query_param: Option<String>,
        /// The API key value.
        value: String,
    },
    /// Bearer token authentication.
    Bearer {
        /// The bearer token value.
        token: String,
    },
    /// OAuth2 client credentials grant.
    #[serde(rename = "oauth2")]
    OAuth2 {
        /// Token endpoint URL.
        token_url: String,
        /// Client ID.
        client_id: String,
        /// Client secret.
        client_secret: String,
        /// Optional scope.
        #[serde(default)]
        scope: Option<String>,
    },
}

/// Pagination configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaginationConfig {
    /// Cursor-based pagination.
    Cursor {
        /// JSON path in the response where the next cursor value lives.
        cursor_field: String,
        /// Query parameter name to send the cursor value.
        cursor_param: String,
    },
    /// Offset-based pagination.
    Offset {
        /// Query parameter name for the offset.
        offset_param: String,
        /// Query parameter name for the limit.
        limit_param: String,
        /// Number of records to request per page.
        limit: u64,
    },
    /// Page-number-based pagination.
    PageNumber {
        /// Query parameter name for the page number.
        page_param: String,
        /// Query parameter name for the page size.
        #[serde(default)]
        page_size_param: Option<String>,
        /// Page size to request.
        #[serde(default)]
        page_size: Option<u64>,
    },
    /// RFC 8288 Link header pagination — follows the `next` link.
    LinkHeader,
}

/// Maps the API response structure to pipelineR records.
#[derive(Debug, Clone, Deserialize)]
pub struct ResponseMapping {
    /// Dot-notation path to the array of records in the response (e.g., `data.items`).
    pub records_path: String,
    /// Dot-notation path to the total record count (used by offset pagination).
    #[serde(default)]
    pub total_path: Option<String>,
    /// Dot-notation path to the next cursor value (used by cursor pagination).
    #[serde(default)]
    pub cursor_path: Option<String>,
}

/// Rate limiting configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum number of HTTP requests per second.
    pub requests_per_second: f64,
}
