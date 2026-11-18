#![allow(dead_code)]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::Notify;
use wasit_control::demo::DemoService;
use wasit_control::{
    memory_pair, CallContext, ClientOptions, ConnConfig, ControlClient, ControlServer, Cursor,
    EventStream, Hello, RpcError, Service,
};

/// Upper bound for any single awaited condition in tests. Never used as synchronisation:
/// tests await signals and use this only so a bug fails instead of hanging.
pub const BOUND: Duration = Duration::from_secs(20);

pub async fn within<T>(fut: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, fut)
        .await
        .expect("condition not reached within the test bound")
}

pub fn hello(host: &str) -> Hello {
    Hello::new("test-build", host, "test").with_features(["a", "b"])
}

pub fn cfg() -> ConnConfig {
    ConnConfig::default()
}

/// Serve `service` as `host` and connect a client to it over an in-memory duplex.
pub async fn mem_pair(
    service: Arc<dyn Service>,
    host: &str,
    server_cfg: ConnConfig,
    client_cfg: ConnConfig,
) -> (ControlClient, ControlServer) {
    let server = ControlServer::new(service, hello(host), server_cfg);
    let client = mem_connect(
        &server,
        ClientOptions::new(hello("client")).config(client_cfg),
    )
    .await
    .expect("connect");
    (client, server)
}

pub async fn mem_connect(
    server: &ControlServer,
    opts: ClientOptions,
) -> Result<ControlClient, wasit_control::ConnectError> {
    let (a, b) = memory_pair(64 * 1024);
    server.serve_connection(b);
    ControlClient::connect(a, opts).await
}

/// Demo service plus `block` (waits for `gate`) and `panic`.
pub struct TestService {
    pub demo: DemoService,
    pub gate: Arc<Notify>,
    pub entered: Arc<Notify>,
}

impl TestService {
    pub fn new(host: &str) -> Self {
        TestService {
            demo: DemoService::new(host),
            gate: Arc::new(Notify::new()),
            entered: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl Service for TestService {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        match method {
            "block" => {
                self.entered.notify_one();
                self.gate.notified().await;
                Ok(json!("released"))
            }
            "panic" => panic!("test handler panic"),
            _ => self.demo.call(ctx, method, params).await,
        }
    }

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        self.demo.subscribe(ctx, topic, params, from).await
    }
}

pub fn demo_bin() -> &'static str {
    env!("CARGO_BIN_EXE_wasit-control-demo")
}

/// Wait until `rx`'s value satisfies `pred`.
pub async fn wait_watch<T: Clone>(
    rx: &mut tokio::sync::watch::Receiver<T>,
    pred: impl FnMut(&T) -> bool,
) -> T {
    within(rx.wait_for(pred))
        .await
        .expect("watch closed")
        .clone()
}

pub fn scratch_dir(name: &str) -> std::path::PathBuf {
    // Unix socket paths are limited to ~104 bytes (macOS temp dirs alone use ~50), so keep the
    // directory name short and, on Unix, under /tmp.
    #[cfg(unix)]
    let root = std::path::PathBuf::from("/tmp");
    #[cfg(not(unix))]
    let root = std::env::temp_dir();
    let base = root.join(format!(
        "wc-{}-{:x}-{:x}",
        name,
        std::process::id(),
        fastrand::u32(..)
    ));
    std::fs::create_dir_all(&base).unwrap();
    base
}
