//! AWS S3 storage backend.
//!
//! Paths are expected in the form `s3://bucket/prefix/key.csv`.
//! Authentication follows the standard AWS SDK credential chain
//! (env vars, shared credentials file, instance profile, etc.).

use async_trait::async_trait;
use aws_sdk_s3::Client;
use chrono::DateTime;

use crate::error::FileSourceError;
use crate::storage::{parse_cloud_path, ObjectInfo, Storage};

/// AWS S3 storage backend.
pub struct S3Storage {
    client: Client,
}

impl S3Storage {
    /// Create a new S3 storage client.
    ///
    /// Uses the standard AWS SDK credential chain. An optional custom `endpoint`
    /// can be provided for S3-compatible stores such as MinIO.
    pub async fn new(
        region: Option<String>,
        endpoint: Option<String>,
    ) -> Result<Self, FileSourceError> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref r) = region {
            config_loader = config_loader.region(aws_config::Region::new(r.clone()));
        }
        if let Some(ref ep) = endpoint {
            config_loader = config_loader.endpoint_url(ep);
        }

        let sdk_config = config_loader.load().await;
        let client = Client::new(&sdk_config);

        Ok(Self { client })
    }
}

#[async_trait]
impl Storage for S3Storage {
    /// List objects in an S3 bucket matching the given prefix.
    ///
    /// The `path` must be of the form `s3://bucket/prefix`.
    async fn list_objects(&self, path: &str) -> Result<Vec<ObjectInfo>, FileSourceError> {
        let (bucket, prefix) = parse_cloud_path(path)?;

        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix);

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| FileSourceError::CloudStorage(format!("S3 ListObjectsV2: {e}")))?;

            for obj in resp.contents() {
                let key = obj.key().unwrap_or_default().to_string();
                let last_modified = obj.last_modified().and_then(|t| {
                    DateTime::from_timestamp(t.secs(), t.subsec_nanos())
                });
                let size = obj.size().unwrap_or(0) as u64;
                objects.push(ObjectInfo {
                    key: format!("s3://{bucket}/{key}"),
                    last_modified,
                    size,
                });
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        if objects.is_empty() {
            return Err(FileSourceError::NoFilesMatched(path.to_string()));
        }

        Ok(objects)
    }

    /// Download an S3 object fully into memory.
    ///
    /// The `path` must be of the form `s3://bucket/key`.
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, FileSourceError> {
        let (bucket, key) = parse_cloud_path(path)?;

        let resp = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("S3 GetObject '{path}': {e}")))?;

        let body = resp
            .body
            .collect()
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("S3 read body '{path}': {e}")))?;

        Ok(body.into_bytes().to_vec())
    }

    /// Upload bytes to an S3 object.
    ///
    /// The `path` must be of the form `s3://bucket/key`.
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<(), FileSourceError> {
        let (bucket, key) = parse_cloud_path(path)?;

        self.client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(data.to_vec().into())
            .send()
            .await
            .map_err(|e| FileSourceError::CloudStorage(format!("S3 PutObject '{path}': {e}")))?;

        Ok(())
    }
}
