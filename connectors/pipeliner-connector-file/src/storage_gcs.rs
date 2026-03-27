//! Google Cloud Storage backend.
//!
//! Paths are expected in the form `gs://bucket/prefix/object.csv`.
//! Authentication follows the standard GCP Application Default Credentials chain.

use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;

use crate::error::FileSourceError;
use crate::storage::{parse_cloud_path, ObjectInfo, Storage};

/// Google Cloud Storage backend.
pub struct GcsStorage {
    client: cloud_storage::Client,
}

impl GcsStorage {
    /// Create a new GCS storage client.
    ///
    /// Uses Application Default Credentials for authentication.
    pub fn new() -> Result<Self, FileSourceError> {
        let client = cloud_storage::Client::default();
        Ok(Self { client })
    }
}

#[async_trait]
impl Storage for GcsStorage {
    /// List objects in a GCS bucket matching the given prefix.
    ///
    /// The `path` must be of the form `gs://bucket/prefix`.
    async fn list_objects(&self, path: &str) -> Result<Vec<ObjectInfo>, FileSourceError> {
        let (bucket, prefix) = parse_cloud_path(path)?;

        let stream = self
            .client
            .object()
            .list(
                &bucket,
                cloud_storage::ListRequest {
                    prefix: Some(prefix),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                FileSourceError::CloudStorage(format!("GCS list objects '{path}': {e}"))
            })?;

        let mut stream = Pin::from(Box::new(stream));
        let mut objects = Vec::new();
        while let Some(page_result) = stream.next().await {
            let page = page_result.map_err(|e| {
                FileSourceError::CloudStorage(format!("GCS list objects page '{path}': {e}"))
            })?;
            for obj in &page.items {
                let last_modified: Option<DateTime<Utc>> = Some(obj.updated);
                objects.push(ObjectInfo {
                    key: format!("gs://{}/{}", bucket, obj.name),
                    last_modified,
                    size: obj.size,
                });
            }
        }

        if objects.is_empty() {
            return Err(FileSourceError::NoFilesMatched(path.to_string()));
        }

        Ok(objects)
    }

    /// Download a GCS object fully into memory.
    ///
    /// The `path` must be of the form `gs://bucket/object`.
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, FileSourceError> {
        let (bucket, object) = parse_cloud_path(path)?;

        let data = self
            .client
            .object()
            .download(&bucket, &object)
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("GCS download '{path}': {e}")))?;

        Ok(data)
    }

    /// Upload bytes to a GCS object.
    ///
    /// The `path` must be of the form `gs://bucket/object`.
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<(), FileSourceError> {
        let (bucket, object) = parse_cloud_path(path)?;

        // Infer MIME type from extension for a reasonable default.
        let mime = if object.ends_with(".csv") {
            "text/csv"
        } else if object.ends_with(".json") || object.ends_with(".jsonl") {
            "application/json"
        } else {
            "application/octet-stream"
        };

        self.client
            .object()
            .create(&bucket, data.to_vec(), &object, mime)
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("GCS upload '{path}': {e}")))?;

        Ok(())
    }
}
