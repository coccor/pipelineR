//! Binary entry point for the file connector (source + sink).

use pipeliner_connector_file::{FileSink, FileSource};

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_connector(FileSource, FileSink)
        .await
        .expect("file connector failed");
}
