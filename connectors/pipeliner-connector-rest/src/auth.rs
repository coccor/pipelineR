//! Authentication handling for the REST source connector.

use reqwest::Client;

use crate::config::AuthConfig;
use crate::error::RestError;

/// Fetch an OAuth2 access token using the client credentials grant.
///
/// Sends a POST request to the token URL with `grant_type=client_credentials`,
/// the client ID/secret, and an optional scope.
pub async fn fetch_oauth2_token(
    client: &Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scope: Option<&str>,
) -> Result<String, RestError> {
    let mut params = vec![
        ("grant_type", "client_credentials"),
        ("client_id", client_id),
        ("client_secret", client_secret),
    ];
    // We need to own the scope string for the borrow checker.
    let scope_owned;
    if let Some(s) = scope {
        scope_owned = s.to_string();
        params.push(("scope", &scope_owned));
    }

    let resp = client
        .post(token_url)
        .form(&params)
        .send()
        .await
        .map_err(RestError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(RestError::OAuth2(format!(
            "token endpoint returned {status}: {body}"
        )));
    }

    let json: serde_json::Value = resp.json().await.map_err(RestError::Http)?;
    json.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| RestError::OAuth2("response missing 'access_token' field".to_string()))
}

/// Apply authentication to a request builder.
///
/// Modifies the builder in place by adding the appropriate headers or query parameters.
pub fn apply_auth_to_request(
    mut builder: reqwest::RequestBuilder,
    auth: &AuthConfig,
    oauth_token: Option<&str>,
) -> reqwest::RequestBuilder {
    match auth {
        AuthConfig::ApiKey {
            header,
            query_param,
            value,
        } => {
            if let Some(h) = header {
                builder = builder.header(h.as_str(), value.as_str());
            }
            if let Some(qp) = query_param {
                builder = builder.query(&[(qp.as_str(), value.as_str())]);
            }
            builder
        }
        AuthConfig::Bearer { token } => builder.bearer_auth(token),
        AuthConfig::OAuth2 { .. } => {
            if let Some(token) = oauth_token {
                builder.bearer_auth(token)
            } else {
                builder
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_bearer_auth() {
        let client = Client::new();
        let auth = AuthConfig::Bearer {
            token: "test-token".to_string(),
        };
        // Just verify it doesn't panic — we can't easily inspect the builder.
        let _builder = apply_auth_to_request(client.get("http://localhost"), &auth, None);
    }

    #[test]
    fn apply_api_key_header_auth() {
        let client = Client::new();
        let auth = AuthConfig::ApiKey {
            header: Some("X-API-Key".to_string()),
            query_param: None,
            value: "secret".to_string(),
        };
        let _builder = apply_auth_to_request(client.get("http://localhost"), &auth, None);
    }
}
