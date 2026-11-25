//! Dialing side.

use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;

use crate::command::ExitInfo;
use crate::connection::{ConnConfig, Connection, HelloTiming, ProgressHook};
use crate::error::ConnectError;
use crate::service::Service;
use crate::transport::{BoxDuplex, Endpoint};
use crate::wire::Hello;

/// Options for dialing.
#[derive(Clone)]
pub struct ClientOptions {
    pub hello: Hello,
    /// Serves requests the peer sends back over this connection (e.g. a remote daemon asking
    /// the hub to route). `None` answers them with `method_not_found`.
    pub service: Option<Arc<dyn Service>>,
    pub config: ConnConfig,
    /// The peer is a bridge that reports `ready` once connected to its daemon: the hello
    /// deadline starts then (see [`HelloTiming::AfterReady`]).
    pub await_ready: bool,
    /// Told what a bridge is doing while it has not reached its daemon yet (its progress
    /// frames). `None` ignores them.
    pub on_progress: Option<ProgressHook>,
}

impl ClientOptions {
    pub fn new(hello: Hello) -> Self {
        ClientOptions {
            hello,
            service: None,
            config: ConnConfig::default(),
            await_ready: false,
            on_progress: None,
        }
    }

    pub fn service(mut self, service: Arc<dyn Service>) -> Self {
        self.service = Some(service);
        self
    }

    pub fn config(mut self, config: ConnConfig) -> Self {
        self.config = config;
        self
    }
}

/// What a transport can say about why it went away: a child process's exit status and the tail
/// of its stderr.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportFacts {
    pub exit: Option<ExitInfo>,
    /// The last bytes of stderr, decoded and trimmed (empty when there were none).
    pub stderr: String,
}

impl TransportFacts {
    /// The one-line explanation shown with a link's last error.
    pub fn describe(&self) -> String {
        match (self.exit, self.stderr.is_empty()) {
            (None, true) => String::new(),
            (None, false) => format!("stderr: {}", self.stderr),
            (Some(e), true) => format!("transport {e}"),
            (Some(e), false) => format!("transport {e}; stderr: {}", self.stderr),
        }
    }
}

/// Explains why a transport went away (e.g. a child process's exit status and stderr).
#[async_trait]
pub trait Diagnostics: Send + Sync + 'static {
    /// The facts, `None` when the transport knows nothing (no exit, no stderr).
    async fn facts(&self) -> Option<TransportFacts>;

    async fn describe(&self) -> Option<String> {
        self.facts().await.map(|f| f.describe())
    }
}

/// A dialed connection. Derefs to [`Connection`] for `request`/`subscribe`/`closed`.
#[derive(Clone)]
pub struct ControlClient {
    conn: Connection,
    _attachment: Option<Arc<dyn Any + Send + Sync>>,
    diagnostics: Option<Arc<dyn Diagnostics>>,
}

impl std::fmt::Debug for ControlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ControlClient").field(&self.conn).finish()
    }
}

impl ControlClient {
    /// Run the hello exchange over `io`.
    pub async fn connect(io: BoxDuplex, opts: ClientOptions) -> Result<Self, ConnectError> {
        let timing = if opts.await_ready {
            HelloTiming::AfterReady
        } else {
            HelloTiming::Immediate
        };
        let conn = Connection::establish_observed(
            io,
            opts.hello,
            opts.service,
            opts.config,
            timing,
            opts.on_progress,
        )
        .await?;
        Ok(ControlClient {
            conn,
            _attachment: None,
            diagnostics: None,
        })
    }

    /// Dial a local endpoint (bounded by the hello timeout for the handshake; the socket
    /// connect itself is local and immediate).
    pub async fn connect_endpoint(
        endpoint: &Endpoint,
        opts: ClientOptions,
    ) -> Result<Self, ConnectError> {
        let io = tokio::time::timeout(opts.config.hello_timeout, endpoint.connect())
            .await
            .map_err(|_| ConnectError::Timeout)?
            .map_err(ConnectError::io)?;
        Self::connect(io, opts).await
    }

    /// Keep `value` alive as long as this client (e.g. a child process guard).
    pub fn with_attachment(mut self, value: Arc<dyn Any + Send + Sync>) -> Self {
        self._attachment = Some(value);
        self
    }

    pub fn with_diagnostics(mut self, d: Arc<dyn Diagnostics>) -> Self {
        self.diagnostics = Some(d);
        self
    }

    /// Transport-level explanation of a failure, if the transport offers one.
    pub async fn diagnostics(&self) -> Option<String> {
        match &self.diagnostics {
            Some(d) => d.describe().await,
            None => None,
        }
    }

    /// The same explanation as structured facts (exit status, stderr tail).
    pub async fn transport_facts(&self) -> Option<TransportFacts> {
        match &self.diagnostics {
            Some(d) => d.facts().await,
            None => None,
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl Deref for ControlClient {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.conn
    }
}
