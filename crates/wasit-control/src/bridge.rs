//! `marsad-agent bridge`: pump this process's stdio to the local daemon endpoint.

use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::transport::Endpoint;
use crate::wire::{codes, encode, Frame, RpcError, DEFAULT_MAX_FRAME};

#[derive(Debug, Clone)]
pub struct BridgeOptions {
    /// After stdin ends, how long the daemon may keep writing before the bridge exits.
    pub drain_timeout: Duration,
}

impl Default for BridgeOptions {
    fn default() -> Self {
        BridgeOptions {
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
/// If nobody serves the endpoint (absent or stale), `start_daemon` is awaited and not bounded:
/// a daemon may take minutes to become ready (a slow login environment, a long startup), and
/// that is progress, not failure. It must resolve once the daemon is ready to accept
/// connections. The endpoint is then dialled once. Once connected, a `ready` frame is written
/// to `stdout` before anything else: the dialling side starts its hello deadline there. If the
/// endpoint cannot be reached, one `goodbye` frame with code `bridge_unavailable` is written
/// instead — a single JSON line the client's handshake surfaces as
/// [`crate::ConnectError::Refused`].
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
    let connected = match endpoint.connect().await {
        Ok(s) => Ok(s),
        Err(e) if Endpoint::is_absent_error(&e) => match start_daemon().await {
            Err(e) => Err(format!("starting the daemon failed: {e}")),
            Ok(()) => endpoint.connect().await.map_err(|e| {
                format!("the daemon reported ready but its endpoint is unreachable: {e}")
            }),
        },
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
    let ready =
        encode(&Frame::Ready, DEFAULT_MAX_FRAME).map_err(|e| io::Error::other(e.to_string()))?;
    stdout.write_all(&ready).await?;
    stdout.flush().await?;
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
