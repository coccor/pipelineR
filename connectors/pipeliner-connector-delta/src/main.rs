//! Binary entry point for the Delta Lake sink connector.

use pipeliner_connector_delta::DeltaSink;

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_sink_connector(DeltaSink)
        .await
        .expect("delta connector failed");
}
