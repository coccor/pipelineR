//! Configuration parsing utilities for plugins.

use crate::error::ValidationError;
use serde::de::DeserializeOwned;

/// Parse a plugin config from a JSON string into a typed struct.
///
/// # Errors
///
/// Returns `ValidationError::InvalidConfig` if the JSON cannot be deserialized
/// into the target type.
pub fn parse_config<T: DeserializeOwned>(config_json: &str) -> Result<T, ValidationError> {
    serde_json::from_str(config_json)
        .map_err(|e| ValidationError::InvalidConfig(format!("failed to parse config: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestConfig {
        host: String,
        port: u16,
    }

    #[test]
    fn parse_valid_config() {
        let json = r#"{"host": "localhost", "port": 5432}"#;
        let cfg: TestConfig = parse_config(json).unwrap();
        assert_eq!(
            cfg,
            TestConfig {
                host: "localhost".into(),
                port: 5432,
            }
        );
    }

    #[test]
    fn parse_invalid_config_missing_field() {
        let json = r#"{"host": "localhost"}"#;
        let result: Result<TestConfig, _> = parse_config(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid config"), "got: {err}");
    }

    #[test]
    fn parse_malformed_json() {
        let json = r#"{not valid json"#;
        let result: Result<TestConfig, _> = parse_config(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid config"), "got: {err}");
    }
}
