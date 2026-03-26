//! Plugin server helpers for running source and sink plugins as gRPC services.
//!
//! These functions handle port binding, signal handling, and tonic server setup
//! so that plugin binaries only need to implement the [`Source`] or [`Sink`] traits.

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use pipeliner_proto::pipeliner::v1::sink_plugin_server::SinkPluginServer;
use pipeliner_proto::pipeliner::v1::source_plugin_server::SourcePluginServer;

use crate::grpc_sink::GrpcSinkService;
use crate::grpc_source::GrpcSourceService;
use crate::sink::Sink;
use crate::source::Source;

/// Maximum gRPC message size (64 MB).
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Run a plugin that implements both [`Source`] and [`Sink`].
///
/// Binds to `127.0.0.1:<port>` (use `--port <n>` CLI arg, default 0 = OS-assigned),
/// prints `PORT=<actual_port>` to stdout, and serves until SIGTERM or Ctrl-C.
///
/// # Errors
///
/// Returns an error if binding or serving fails.
pub async fn run_plugin<S, K>(source: S, sink: K) -> Result<(), Box<dyn std::error::Error>>
where
    S: Source,
    K: Sink,
{
    let port = parse_port_arg();
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let local_addr = listener.local_addr()?;
    println!("PORT={}", local_addr.port());

    let source_svc = SourcePluginServer::new(GrpcSourceService::new(source))
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let sink_svc = SinkPluginServer::new(GrpcSinkService::new(sink))
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    Server::builder()
        .add_service(source_svc)
        .add_service(sink_svc)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal())
        .await?;

    Ok(())
}

/// Run a plugin that implements only [`Source`].
///
/// Binds to `127.0.0.1:<port>` (use `--port <n>` CLI arg, default 0 = OS-assigned),
/// prints `PORT=<actual_port>` to stdout, and serves until SIGTERM or Ctrl-C.
///
/// # Errors
///
/// Returns an error if binding or serving fails.
pub async fn run_source_plugin<S: Source>(source: S) -> Result<(), Box<dyn std::error::Error>> {
    let port = parse_port_arg();
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let local_addr = listener.local_addr()?;
    println!("PORT={}", local_addr.port());

    let svc = SourcePluginServer::new(GrpcSourceService::new(source))
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal())
        .await?;

    Ok(())
}

/// Run a plugin that implements only [`Sink`].
///
/// Binds to `127.0.0.1:<port>` (use `--port <n>` CLI arg, default 0 = OS-assigned),
/// prints `PORT=<actual_port>` to stdout, and serves until SIGTERM or Ctrl-C.
///
/// # Errors
///
/// Returns an error if binding or serving fails.
pub async fn run_sink_plugin<K: Sink>(sink: K) -> Result<(), Box<dyn std::error::Error>> {
    let port = parse_port_arg();
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let local_addr = listener.local_addr()?;
    println!("PORT={}", local_addr.port());

    let svc = SinkPluginServer::new(GrpcSinkService::new(sink))
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown_signal())
        .await?;

    Ok(())
}

/// Parse the `--port <n>` argument from CLI args. Returns 0 (OS-assigned) if not found.
fn parse_port_arg() -> u16 {
    let args: Vec<String> = std::env::args().collect();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--port" {
            if let Some(val) = iter.next() {
                if let Ok(p) = val.parse::<u16>() {
                    return p;
                }
            }
        }
    }
    0
}

/// Wait for a shutdown signal (SIGTERM or Ctrl-C).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap_or_else(|_| panic!("failed to install SIGTERM handler"));
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
