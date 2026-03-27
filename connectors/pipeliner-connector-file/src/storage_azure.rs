//! Azure Blob Storage backend.
//!
//! Paths are expected in the form `azure://container/prefix/blob.csv`.
//! Authentication uses an access key extracted from a connection string,
//! read from the config field or the `AZURE_STORAGE_CONNECTION_STRING`
//! environment variable.

use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use chrono::{DateTime, Utc};
use futures::StreamExt;

use crate::error::FileSourceError;
use crate::storage::{parse_cloud_path, ObjectInfo, Storage};

/// Azure Blob Storage backend.
pub struct AzureStorage {
    client: BlobServiceClient,
}

impl AzureStorage {
    /// Create a new Azure storage client.
    ///
    /// Uses the provided `connection_string`, or falls back to the
    /// `AZURE_STORAGE_CONNECTION_STRING` environment variable. The connection
    /// string must contain `AccountName` and `AccountKey` fields.
    pub fn new(connection_string: Option<String>) -> Result<Self, FileSourceError> {
        let conn_str = connection_string
            .or_else(|| std::env::var("AZURE_STORAGE_CONNECTION_STRING").ok())
            .ok_or_else(|| {
                FileSourceError::CloudStorage(
                    "Azure connection string not provided and AZURE_STORAGE_CONNECTION_STRING env var not set".to_string(),
                )
            })?;

        let account = extract_field(&conn_str, "AccountName").ok_or_else(|| {
            FileSourceError::CloudStorage(
                "could not extract AccountName from Azure connection string".to_string(),
            )
        })?;

        let key = extract_field(&conn_str, "AccountKey").ok_or_else(|| {
            FileSourceError::CloudStorage(
                "could not extract AccountKey from Azure connection string".to_string(),
            )
        })?;

        let credentials = StorageCredentials::access_key(&account, key);
        let client = BlobServiceClient::new(&account, credentials);

        Ok(Self { client })
    }
}

/// Extract a `Key=Value` field from an Azure Storage connection string.
fn extract_field(conn_str: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    for part in conn_str.split(';') {
        let trimmed = part.trim();
        if let Some(value) = trimmed.strip_prefix(&prefix) {
            return Some(value.to_string());
        }
    }
    None
}

#[async_trait]
impl Storage for AzureStorage {
    /// List blobs in an Azure container matching the given prefix.
    ///
    /// The `path` must be of the form `azure://container/prefix`.
    async fn list_objects(&self, path: &str) -> Result<Vec<ObjectInfo>, FileSourceError> {
        let (container, prefix) = parse_cloud_path(path)?;

        let container_client = self.client.container_client(&container);

        let mut objects = Vec::new();
        let mut stream = container_client
            .list_blobs()
            .prefix(prefix.clone())
            .into_stream();

        while let Some(page) = stream.next().await {
            let page = page.map_err(|e| {
                FileSourceError::CloudStorage(format!("Azure list blobs '{path}': {e}"))
            })?;
            for blob in page.blobs.blobs() {
                // Convert OffsetDateTime to RFC3339 then parse to DateTime<Utc>.
                let last_modified = blob
                    .properties
                    .last_modified
                    .format(&time::format_description::well_known::Rfc3339)
                    .ok()
                    .and_then(|s| s.parse::<DateTime<Utc>>().ok());

                objects.push(ObjectInfo {
                    key: format!("azure://{container}/{}", blob.name),
                    last_modified,
                    size: blob.properties.content_length,
                });
            }
        }

        if objects.is_empty() {
            return Err(FileSourceError::NoFilesMatched(path.to_string()));
        }

        Ok(objects)
    }

    /// Download an Azure blob fully into memory.
    ///
    /// The `path` must be of the form `azure://container/blob`.
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, FileSourceError> {
        let (container, blob_name) = parse_cloud_path(path)?;

        let blob_client = self
            .client
            .container_client(&container)
            .blob_client(&blob_name);

        let data = blob_client
            .get_content()
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("Azure get blob '{path}': {e}")))?;

        Ok(data)
    }

    /// Upload bytes to an Azure blob.
    ///
    /// The `path` must be of the form `azure://container/blob`.
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<(), FileSourceError> {
        let (container, blob_name) = parse_cloud_path(path)?;

        let blob_client = self
            .client
            .container_client(&container)
            .blob_client(&blob_name);

        blob_client
            .put_block_blob(data.to_vec())
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("Azure put blob '{path}': {e}")))?;

        Ok(())
    }
}
