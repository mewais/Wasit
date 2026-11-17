//! Accepting side: runs the hello exchange on each incoming stream and serves a [`Service`].

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::connection::{ConnConfig, Connection};
use crate::error::{ConnClose, ConnectError};
use crate::service::Service;
use crate::transport::{BoxDuplex, Listener};
use crate::wire::{codes, Hello, RpcError};

struct ServerInner {
    service: Arc<dyn Service>,
    hello: Hello,
    cfg: ConnConfig,
    conns: Mutex<HashMap<u64, Connection>>,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

/// Serves one [`Service`] to any number of connections.
#[derive(Clone)]
pub struct ControlServer {
    inner: Arc<ServerInner>,
}

impl ControlServer {
    pub fn new(service: Arc<dyn Service>, hello: Hello, cfg: ConnConfig) -> Self {
        ControlServer {
            inner: Arc::new(ServerInner {
                service,
                hello,
                cfg,
                conns: Mutex::new(HashMap::new()),
                shutdown: CancellationToken::new(),
                tracker: TaskTracker::new(),
            }),
        }
    }

    pub fn hello(&self) -> &Hello {
        &self.inner.hello
    }

    /// Accept connections until [`ControlServer::shutdown`] is called. If the listener runs
    /// dry (e.g. [`crate::StdioListener`]), waits until all its connections have closed.
    pub async fn serve<L: Listener>(&self, mut listener: L) -> io::Result<()> {
        let mut accept_failures = 0u32;
        loop {
            let accepted = tokio::select! {
                _ = self.inner.shutdown.cancelled() => return Ok(()),
                r = listener.accept() => r,
            };
            match accepted {
                Ok(Some(io)) => {
                    accept_failures = 0;
                    self.serve_connection(io);
                }
                Ok(None) => break,
                Err(e) => {
                    // Transient accept failures (fd exhaustion, aborted handshakes) must not
                    // kill the daemon; back off and retry.
                    accept_failures = accept_failures.saturating_add(1);
                    let delay = Duration::from_millis(10u64 << accept_failures.min(7));
                    tracing::warn!(error = %e, ?delay, "accept failed");
                    tokio::select! {
                        _ = self.inner.shutdown.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
        // Listener exhausted: run until its connections are done or we are shut down.
        tokio::select! {
            _ = self.inner.shutdown.cancelled() => Ok(()),
            _ = wait_tracker_idle(&self.inner.tracker) => Ok(()),
        }
    }

    /// Serve one already-connected stream (handshake + run). The handle resolves when the
    /// connection has closed.
    pub fn serve_connection(&self, io: BoxDuplex) -> JoinHandle<Result<ConnClose, ConnectError>> {
        let inner = self.inner.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.inner.tracker.spawn(async move {
            let res = run_connection(inner, io).await;
            // The caller may not be interested in the outcome.
            let _ = tx.send(res);
        });
        tokio::spawn(async move {
            rx.await
                .unwrap_or_else(|_| Err(ConnectError::Io("server task ended".into())))
        })
    }

    /// Live connections (e.g. to call back into a peer).
    pub fn connections(&self) -> Vec<Connection> {
        self.inner
            .conns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Stop accepting, say goodbye to every connection, and wait (at most `grace`) for the
    /// connection tasks to finish.
    pub async fn shutdown(&self, grace: Duration) {
        self.inner.shutdown.cancel();
        for c in self.connections() {
            c.goodbye(RpcError::new(codes::SHUTTING_DOWN, "server shutting down"));
        }
        self.inner.tracker.close();
        if tokio::time::timeout(grace, self.inner.tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!("server shutdown grace period elapsed with tasks still running");
        }
    }
}

async fn wait_tracker_idle(tracker: &TaskTracker) {
    tracker.close();
    tracker.wait().await;
}

async fn run_connection(inner: Arc<ServerInner>, io: BoxDuplex) -> Result<ConnClose, ConnectError> {
    let conn = tokio::select! {
        _ = inner.shutdown.cancelled() => return Err(ConnectError::Io("server shutting down".into())),
        r = Connection::establish(io, inner.hello.clone(), Some(inner.service.clone()), inner.cfg.clone()) => r,
    };
    let conn = match conn {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(error = %e, "incoming connection failed handshake");
            return Err(e);
        }
    };
    let id = conn.id();
    inner
        .conns
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(id, conn.clone());
    if inner.shutdown.is_cancelled() {
        conn.goodbye(RpcError::new(codes::SHUTTING_DOWN, "server shutting down"));
    }
    let reason = conn.closed().await;
    inner
        .conns
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&id);
    Ok(reason)
}
