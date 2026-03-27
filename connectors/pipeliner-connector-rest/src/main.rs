//! Binary entry point for the REST API source connector.

use pipeliner_connector_rest::RestSource;

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_source_connector(RestSource)
        .await
        .expect("REST source connector failed");
}
