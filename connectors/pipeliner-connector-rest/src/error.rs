//! Error types for the REST source connector.

use thiserror::Error;

/// Errors specific to the REST source connector.
#[derive(Debug, Error)]
pub enum RestError {
    /// An HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Failed to parse the response body as JSON.
    #[error("JSON parse error: {0}")]
    JsonParse(String),

    /// The response mapping path did not resolve to a value.
    #[error("path '{0}' not found in response")]
    PathNotFound(String),

    /// The records path resolved to a non-array value.
    #[error("path '{0}' resolved to a non-array value")]
    NotAnArray(String),

    /// OAuth2 token exchange failed.
    #[error("OAuth2 token exchange failed: {0}")]
    OAuth2(String),

    /// The server returned a non-success HTTP status.
    #[error("HTTP error {status}: {body}")]
    HttpStatus {
        /// The HTTP status code.
        status: u16,
        /// The response body.
        body: String,
        /// The value of the Retry-After header, if present (in seconds).
        retry_after: Option<u64>,
    },

    /// URL construction failed.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    /// Rate limit exceeded or unexpected rate-limit response.
    #[error("rate limit error: {0}")]
    RateLimit(String),
}
