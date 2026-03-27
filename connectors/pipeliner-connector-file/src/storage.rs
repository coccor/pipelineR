//! Storage abstraction layer for the file connector.
//!
//! Provides a unified [`Storage`] trait that backends implement for local
//! filesystem, AWS S3, Azure Blob Storage, and Google Cloud Storage.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::config::StorageBackendConfig;
use crate::error::FileSourceError;
use crate::storage_azure::AzureStorage;
use crate::storage_gcs::GcsStorage;
use crate::storage_local::LocalStorage;
use crate::storage_s3::S3Storage;

/// Metadata about a single object in a storage backend.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    /// Object key or file path.
    pub key: String,
    /// Last modification timestamp, if available.
    pub last_modified: Option<DateTime<Utc>>,
    /// Object size in bytes.
    pub size: u64,
}

/// Unified storage interface for reading and writing objects.
#[async_trait]
pub trait Storage: Send + Sync {
    /// List objects matching a prefix or glob pattern.
    ///
    /// Returns a vector of [`ObjectInfo`] describing each matched object.
    async fn list_objects(&self, path: &str) -> Result<Vec<ObjectInfo>, FileSourceError>;

    /// Read an entire object into memory as raw bytes.
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, FileSourceError>;

    /// Write raw bytes to the given object path, creating or overwriting.
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<(), FileSourceError>;
}

/// Create a [`Storage`] implementation from the provided backend configuration.
///
/// When `config` is `None`, defaults to [`LocalStorage`].
pub async fn create_storage(
    config: Option<&StorageBackendConfig>,
) -> Result<Box<dyn Storage>, FileSourceError> {
    let backend = config.cloned().unwrap_or_default();
    match backend {
        StorageBackendConfig::Local => Ok(Box::new(LocalStorage)),
        StorageBackendConfig::S3 { region, endpoint } => {
            let s3 = S3Storage::new(region, endpoint).await?;
            Ok(Box::new(s3))
        }
        StorageBackendConfig::Azure { connection_string } => {
            let az = AzureStorage::new(connection_string)?;
            Ok(Box::new(az))
        }
        StorageBackendConfig::Gcs { project_id: _ } => {
            let gcs = GcsStorage::new()?;
            Ok(Box::new(gcs))
        }
    }
}

/// Parse a cloud URI into `(bucket, key)`.
///
/// Accepted schemes: `s3://`, `azure://`, `gs://`.
/// Returns an error if the path cannot be parsed.
pub fn parse_cloud_path(path: &str) -> Result<(String, String), FileSourceError> {
    // Strip scheme prefix.
    let without_scheme = path
        .find("://")
        .map(|idx| &path[idx + 3..])
        .ok_or_else(|| {
            FileSourceError::InvalidCloudPath(format!("missing '://' scheme in path: {path}"))
        })?;

    let slash_idx = without_scheme.find('/');
    match slash_idx {
        Some(idx) => {
            let bucket = &without_scheme[..idx];
            let key = &without_scheme[idx + 1..];
            if bucket.is_empty() {
                return Err(FileSourceError::InvalidCloudPath(format!(
                    "empty bucket name in path: {path}"
                )));
            }
            Ok((bucket.to_string(), key.to_string()))
        }
        None => {
            if without_scheme.is_empty() {
                return Err(FileSourceError::InvalidCloudPath(format!(
                    "empty bucket name in path: {path}"
                )));
            }
            // Bucket only, no key.
            Ok((without_scheme.to_string(), String::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_path() {
        let (bucket, key) = parse_cloud_path("s3://my-bucket/folder/file.csv").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "folder/file.csv");
    }

    #[test]
    fn parse_azure_path() {
        let (container, blob) = parse_cloud_path("azure://mycontainer/dir/data.json").unwrap();
        assert_eq!(container, "mycontainer");
        assert_eq!(blob, "dir/data.json");
    }

    #[test]
    fn parse_gs_path() {
        let (bucket, obj) = parse_cloud_path("gs://bucket/object.parquet").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(obj, "object.parquet");
    }

    #[test]
    fn parse_bucket_only() {
        let (bucket, key) = parse_cloud_path("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "");
    }

    #[test]
    fn parse_empty_bucket_errors() {
        assert!(parse_cloud_path("s3://").is_err());
    }

    #[test]
    fn parse_no_scheme_errors() {
        assert!(parse_cloud_path("just/a/path").is_err());
    }
}
