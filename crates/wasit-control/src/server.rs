//! Accepting side: runs the hello exchange on each incoming stream and serves a [`Service`].

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::connection::{ConnConfig, Connection};
use crate::diag;
use crate::error::{ConnClose, ConnectError};
use crate::service::Service;
use crate::transport::{BoxDuplex, Listener};
use crate::wire::{codes, Hello, RpcError};

struct ServerInner {
    service: Arc<dyn Service>,
    hello: Hello,
    cfg: ConnConfig,
    /// Serves peers inside this process only: its connections are not recorded as accepted or
    /// closed (see [`ControlServer::in_process`]).
    in_process: bool,
    conns: Mutex<HashMap<u64, Connection>>,
    /// Bumped whenever a connection is added or removed.
    conn_changes: watch::Sender<u64>,
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
        Self::build(service, hello, cfg, false)
    }

    /// A server for peers inside this process (a loopback link to itself): like [`Self::new`],
    /// but connections to it are not recorded in diagnostics as accepted or closed (a failed
    /// handshake still is).
    pub fn in_process(service: Arc<dyn Service>, hello: Hello, cfg: ConnConfig) -> Self {
        Self::build(service, hello, cfg, true)
    }

    fn build(service: Arc<dyn Service>, hello: Hello, cfg: ConnConfig, in_process: bool) -> Self {
        ControlServer {
            inner: Arc::new(ServerInner {
                service,
                hello,
                cfg,
                in_process,
                conns: Mutex::new(HashMap::new()),
                conn_changes: watch::channel(0).0,
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

    /// A counter bumped every time a connection is established or closes: await `changed()`
    /// and re-read [`ControlServer::connections`] (e.g. to notice an uplink coming back).
    pub fn connection_changes(&self) -> watch::Receiver<u64> {
        self.inner.conn_changes.subscribe()
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
            diag::handshake_failed(&e);
            return Err(e);
        }
    };
    let accepted = Instant::now();
    if !inner.in_process {
        diag::conn_accepted(conn.remote_hello());
    }
    let id = conn.id();
    inner
        .conns
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(id, conn.clone());
    inner.conn_changes.send_modify(|n| *n += 1);
    if inner.shutdown.is_cancelled() {
        conn.goodbye(RpcError::new(codes::SHUTTING_DOWN, "server shutting down"));
    }
    let reason = conn.closed().await;
    // Let the writer flush what was queued before the close (bounded by its own timeouts),
    // so a shutting-down process does not cut off the final response or goodbye.
    conn.finished().await;
    inner
        .conns
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&id);
    inner.conn_changes.send_modify(|n| *n += 1);
    // Not when the server itself is shutting down: every connection then closes by our own
    // goodbye, which the process's shutdown record already says (a record per connection at
    // exit would also follow it).
    if !inner.in_process && !inner.shutdown.is_cancelled() {
        diag::conn_closed(conn.remote_hello(), &reason, accepted);
    }
    Ok(reason)
}
