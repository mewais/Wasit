//! Dialing side.

use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;

use crate::connection::{ConnConfig, Connection};
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
}

impl ClientOptions {
    pub fn new(hello: Hello) -> Self {
        ClientOptions {
            hello,
            service: None,
            config: ConnConfig::default(),
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

/// Explains why a transport went away (e.g. a child process's exit status and stderr).
#[async_trait]
pub trait Diagnostics: Send + Sync + 'static {
    async fn describe(&self) -> Option<String>;
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
        let conn = Connection::establish(io, opts.hello, opts.service, opts.config).await?;
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
