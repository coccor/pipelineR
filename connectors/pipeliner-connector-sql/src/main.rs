//! Binary entry point for the SQL connector (source + sink).

use pipeliner_connector_sql::{SqlSink, SqlSource};

#[tokio::main]
async fn main() {
    pipeliner_sdk::run_connector(SqlSource, SqlSink)
        .await
        .expect("sql connector failed");
}
