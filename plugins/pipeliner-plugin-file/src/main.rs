//! Binary entry point for the file plugin (source + sink).

use pipeliner_plugin_file::{FileSink, FileSource};

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_plugin(FileSource, FileSink)
        .await
        .expect("file plugin failed");
}
