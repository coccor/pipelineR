//! Configuration types for the file source plugin.

use serde::Deserialize;

/// File format.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}

/// CSV-specific options.
#[derive(Debug, Clone, Deserialize)]
pub struct CsvOptions {
    /// Field delimiter character. Defaults to `,`.
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Quote character. Defaults to `"`.
    #[serde(default = "default_quote")]
    pub quote: char,
    /// Whether the file has a header row. Defaults to `true`.
    #[serde(default = "default_true")]
    pub has_header: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            quote: '"',
            has_header: true,
        }
    }
}

fn default_delimiter() -> char {
    ','
}

fn default_quote() -> char {
    '"'
}

fn default_true() -> bool {
    true
}

/// Top-level configuration for the file source plugin.
#[derive(Debug, Clone, Deserialize)]
pub struct FileSourceConfig {
    /// Path to a file or a glob pattern (e.g., `data/*.csv`).
    pub path: String,
    /// File format.
    pub format: FileFormat,
    /// CSV-specific options (only used when `format` is `csv`).
    #[serde(default)]
    pub csv: Option<CsvOptions>,
}
