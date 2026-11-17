//! `marsad-agent bridge`: pump this process's stdio to the local daemon endpoint.

use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::transport::Endpoint;
use crate::wire::{codes, encode, Frame, RpcError, DEFAULT_MAX_FRAME};

#[derive(Debug, Clone)]
pub struct BridgeOptions {
    /// Bound on one endpoint connect.
    pub connect_timeout: Duration,
    /// Bound on the caller's daemon start closure.
    pub start_timeout: Duration,
    /// Connect retries after the daemon was started (backoff schedule).
    pub retries_after_start: u32,
    pub retry_initial: Duration,
    /// After stdin ends, how long the daemon may keep writing before the bridge exits.
    pub drain_timeout: Duration,
}

impl Default for BridgeOptions {
    fn default() -> Self {
        BridgeOptions {
            connect_timeout: Duration::from_secs(5),
            start_timeout: Duration::from_secs(20),
            retries_after_start: 5,
            retry_initial: Duration::from_millis(50),
            drain_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("cannot reach the local daemon: {0}")]
    Unavailable(String),
    #[error("bridge i/o: {0}")]
    Io(#[from] io::Error),
}

/// Connect `stdin`/`stdout` to `endpoint`, byte for byte.
///
/// If nobody serves the endpoint (absent or stale), `start_daemon` is awaited (bounded); it
/// should resolve once the daemon is ready (or fail). If the endpoint still cannot be reached,
/// one `goodbye` frame with code `bridge_unavailable` is written to `stdout` — a single JSON
/// line the client's handshake surfaces as [`crate::ConnectError::Refused`].
///
/// Returns when both directions have finished.
pub async fn bridge<R, W, F, Fut>(
    stdin: R,
    mut stdout: W,
    endpoint: &Endpoint,
    start_daemon: F,
    opts: BridgeOptions,
) -> Result<(), BridgeError>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = io::Result<()>>,
{
    let connected = match connect_once(endpoint, &opts).await {
        Ok(s) => Ok(s),
        Err(e) if Endpoint::is_absent_error(&e) => {
            match tokio::time::timeout(opts.start_timeout, start_daemon()).await {
                Err(_) => Err("starting the daemon timed out".to_string()),
                Ok(Err(e)) => Err(format!("starting the daemon failed: {e}")),
                Ok(Ok(())) => connect_after_start(endpoint, &opts)
                    .await
                    .map_err(|e| format!("daemon started but endpoint unreachable: {e}")),
            }
        }
        Err(e) => Err(e.to_string()),
    };
    let socket = match connected {
        Ok(s) => s,
        Err(message) => {
            let message = format!("bridge cannot reach the daemon at {endpoint:?}: {message}");
            let frame = Frame::Goodbye {
                reason: RpcError::new(codes::BRIDGE_UNAVAILABLE, message.clone()),
            };
            if let Ok(line) = encode(&frame, DEFAULT_MAX_FRAME) {
                // Best effort: if stdout is gone there is nobody to tell.
                let _ = stdout.write_all(&line).await;
                let _ = stdout.flush().await;
            }
            return Err(BridgeError::Unavailable(message));
        }
    };
    pump(stdin, stdout, socket, opts.drain_timeout).await
}

/// Copy both ways. When the daemon side ends, the bridge ends (its caller exits and so closes
/// stdout, which is how the far client learns of it: shutting down a process's stdout handle
/// does not close the descriptor). When stdin ends, the daemon gets EOF and a bounded moment
/// to finish writing.
async fn pump<R, W>(
    mut stdin: R,
    mut stdout: W,
    socket: crate::transport::BoxDuplex,
    drain: Duration,
) -> Result<(), BridgeError>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let (mut sr, mut sw) = tokio::io::split(socket);
    let up = async {
        let r = tokio::io::copy(&mut stdin, &mut sw).await;
        // EOF to the daemon; if the socket is already gone there is nothing to signal.
        let _ = sw.shutdown().await;
        r
    };
    let down = async {
        let r = tokio::io::copy(&mut sr, &mut stdout).await;
        let f = stdout.flush().await;
        r.and(f.map(|_| 0))
    };
    tokio::pin!(up, down);
    tokio::select! {
        r = &mut down => {
            r?;
        }
        r = &mut up => {
            r?;
            if let Ok(r) = tokio::time::timeout(drain, &mut down).await {
                r?;
            }
        }
    }
    Ok(())
}

async fn connect_once(
    endpoint: &Endpoint,
    opts: &BridgeOptions,
) -> io::Result<crate::transport::BoxDuplex> {
    match tokio::time::timeout(opts.connect_timeout, endpoint.connect()).await {
        Ok(r) => r,
        Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "connect timed out")),
    }
}

async fn connect_after_start(
    endpoint: &Endpoint,
    opts: &BridgeOptions,
) -> io::Result<crate::transport::BoxDuplex> {
    let mut delay = opts.retry_initial;
    let mut last = None;
    for attempt in 0..=opts.retries_after_start {
        if attempt > 0 {
            // Backoff schedule (the start closure is expected to signal readiness; this only
            // absorbs a daemon that reports ready a moment before it listens).
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
        match connect_once(endpoint, opts).await {
            Ok(s) => return Ok(s),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("no attempt made")))
}
