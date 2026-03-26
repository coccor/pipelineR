//! Binary entry point for the file source plugin.

use pipeliner_plugin_file::FileSource;

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_source_plugin(FileSource)
        .await
        .expect("file source plugin failed");
}
