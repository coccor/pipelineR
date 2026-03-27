//! Local filesystem storage backend.
//!
//! Wraps the existing file I/O operations behind the [`Storage`] trait so that
//! the source and sink code can treat local and cloud backends uniformly.

use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::FileSourceError;
use crate::storage::{ObjectInfo, Storage};

/// Local filesystem storage backend.
pub struct LocalStorage;

#[async_trait]
impl Storage for LocalStorage {
    /// List files matching a glob pattern.
    ///
    /// Returns one [`ObjectInfo`] per matched file with its last-modified time.
    async fn list_objects(&self, pattern: &str) -> Result<Vec<ObjectInfo>, FileSourceError> {
        let paths: Vec<PathBuf> = glob::glob(pattern)
            .map_err(|e| FileSourceError::Io(format!("invalid glob pattern '{pattern}': {e}")))?
            .filter_map(|entry| entry.ok())
            .filter(|p| p.is_file())
            .collect();

        if paths.is_empty() {
            // Try as a literal path.
            let p = PathBuf::from(pattern);
            if p.is_file() {
                let meta = std::fs::metadata(&p)
                    .map_err(|e| FileSourceError::Io(format!("{}: {e}", p.display())))?;
                let modified = meta.modified().ok().map(DateTime::<Utc>::from);
                return Ok(vec![ObjectInfo {
                    key: p.display().to_string(),
                    last_modified: modified,
                    size: meta.len(),
                }]);
            }
            return Err(FileSourceError::NoFilesMatched(pattern.to_string()));
        }

        let mut objects = Vec::with_capacity(paths.len());
        for p in paths {
            let meta = std::fs::metadata(&p)
                .map_err(|e| FileSourceError::Io(format!("{}: {e}", p.display())))?;
            let modified = meta.modified().ok().map(DateTime::<Utc>::from);
            objects.push(ObjectInfo {
                key: p.display().to_string(),
                last_modified: modified,
                size: meta.len(),
            });
        }

        Ok(objects)
    }

    /// Read a local file fully into memory.
    async fn read_object(&self, path: &str) -> Result<Vec<u8>, FileSourceError> {
        tokio::fs::read(path)
            .await
            .map_err(|e| FileSourceError::Io(format!("{path}: {e}")))
    }

    /// Write bytes to a local file, creating or overwriting.
    async fn write_object(&self, path: &str, data: &[u8]) -> Result<(), FileSourceError> {
        // Ensure parent directory exists.
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| FileSourceError::Io(format!("{}: {e}", parent.display())))?;
            }
        }
        tokio::fs::write(path, data)
            .await
            .map_err(|e| FileSourceError::Io(format!("{path}: {e}")))
    }
}
