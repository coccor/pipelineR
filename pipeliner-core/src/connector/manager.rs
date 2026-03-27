use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::process::{Child, Command};

/// A spawned plugin child process.
///
/// Manages the lifecycle of a connector binary that communicates over gRPC.
/// The child process is killed when `kill()` is called or when the `ConnectorProcess`
/// is dropped (via `kill_on_drop`).
pub struct ConnectorProcess {
    child: Child,
    port: u16,
    name: String,
}

impl ConnectorProcess {
    /// Spawn a connector binary and wait for it to report its listening port.
    ///
    /// The connector binary is started with `--port 0` (requesting an OS-assigned port)
    /// and must print `PORT=<n>` as its first stdout line within 5 seconds.
    pub async fn spawn(name: &str, binary_path: &str) -> Result<Self, ConnectorSpawnError> {
        let mut child = Command::new(binary_path)
            .arg("--port")
            .arg("0")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ConnectorSpawnError::LaunchFailed {
                name: name.to_string(),
                message: e.to_string(),
            })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ConnectorSpawnError::LaunchFailed {
                name: name.to_string(),
                message: "failed to capture stdout".to_string(),
            })?;

        let mut reader = tokio::io::BufReader::new(stdout).lines();

        let port = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(line) =
                reader
                    .next_line()
                    .await
                    .map_err(|e| ConnectorSpawnError::LaunchFailed {
                        name: name.to_string(),
                        message: format!("failed to read stdout: {e}"),
                    })?
            {
                if let Some(p) = extract_port_from_line(&line) {
                    return Ok(p);
                }
            }
            Err(ConnectorSpawnError::LaunchFailed {
                name: name.to_string(),
                message: "process exited without reporting port".to_string(),
            })
        })
        .await
        .map_err(|_| ConnectorSpawnError::Timeout {
            name: name.to_string(),
        })??;

        Ok(Self {
            child,
            port,
            name: name.to_string(),
        })
    }

    /// The port the plugin is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The gRPC address to connect to this plugin (e.g. `http://127.0.0.1:12345`).
    pub fn address(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// The connector name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Kill the plugin child process.
    pub async fn kill(&mut self) -> Result<(), std::io::Error> {
        self.child.kill().await
    }
}

/// Extract a port number from a line matching the pattern `PORT=<number>`.
fn extract_port_from_line(line: &str) -> Option<u16> {
    line.strip_prefix("PORT=")?.parse().ok()
}

/// Errors that can occur when spawning a connector process.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorSpawnError {
    /// The connector binary failed to start or exited unexpectedly.
    #[error("plugin '{name}' failed to start: {message}")]
    LaunchFailed {
        /// Connector name.
        name: String,
        /// Failure details.
        message: String,
    },
    /// The plugin did not report its port within the timeout period.
    #[error("plugin '{name}' did not report port within timeout")]
    Timeout {
        /// Connector name.
        name: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_port_from_output() {
        assert_eq!(extract_port_from_line("PORT=12345"), Some(12345));
        assert_eq!(extract_port_from_line("PORT=0"), Some(0));
        assert_eq!(extract_port_from_line("something else"), None);
        assert_eq!(extract_port_from_line("PORT=abc"), None);
    }
}
