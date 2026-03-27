//! Binary entry point for the Parquet sink connector.

use pipeliner_connector_parquet::ParquetSink;

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_sink_connector(ParquetSink)
        .await
        .expect("parquet connector failed");
}
