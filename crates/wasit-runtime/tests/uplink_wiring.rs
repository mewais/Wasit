//! `RuntimeClient::with_uplink`: a durable subscription to a host that refuses it waits for a
//! route change instead of retrying on the backoff schedule. Without the uplink the same
//! subscription retries on the backoff, which the second test shows so the first one means
//! something.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use serde_json::Value;
use wasit_control::{
    codes, connector, memory_pair, CallContext, ClientOptions, ConnConfig, ControlClient,
    ControlServer, Cursor, EventStream, Hello, RpcError, Service, Supervised, SupervisedOptions,
    Uplink,
};
use wasit_runtime::RuntimeClient;

/// Upper bound for any awaited condition. Never a synchronisation point: a bug fails here
/// instead of hanging.
const BOUND: Duration = Duration::from_secs(20);

/// Refuses every subscription with `host_unreachable` while the host is down, counting the
/// calls. Once up, serves an empty topic (it ends at once).
#[derive(Default)]
struct FarHost {
    calls: AtomicUsize,
    up: AtomicBool,
}

#[async_trait]
impl Service for FarHost {
    async fn call(&self, _ctx: CallContext, method: &str, _p: Value) -> Result<Value, RpcError> {
        Err(RpcError::method_not_found(method))
    }

    async fn subscribe(
        &self,
        _ctx: CallContext,
        _topic: &str,
        _params: Value,
        _from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.up.load(Ordering::SeqCst) {
            Ok(Box::pin(stream::empty()))
        } else {
            Err(RpcError::new(
                codes::HOST_UNREACHABLE,
                "no route to the host",
            ))
        }
    }
}

/// Serves nothing; stands in for the daemon whose uplink is watched.
struct Idle;

#[async_trait]
impl Service for Idle {
    async fn call(&self, _ctx: CallContext, method: &str, _p: Value) -> Result<Value, RpcError> {
        Err(RpcError::method_not_found(method))
    }

    async fn subscribe(
        &self,
        _ctx: CallContext,
        _topic: &str,
        _params: Value,
        _from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        Err(RpcError::method_not_found("subscribe"))
    }
}

fn hello(host: &str, role: &str) -> Hello {
    Hello::new("test-build", host, role)
}

/// A client whose link serves `far`, optionally with `uplink` as its route source.
fn client_to(far: Arc<FarHost>, uplink: Option<Uplink>) -> (RuntimeClient, Supervised) {
    let server = ControlServer::new(far, hello("far", "daemon"), ConnConfig::default());
    let conn = connector(move || {
        let server = server.clone();
        async move {
            let (a, b) = memory_pair(64 * 1024);
            server.serve_connection(b);
            ControlClient::connect(a, ClientOptions::new(hello("gui", "gui"))).await
        }
    });
    let sup = Supervised::spawn(conn, SupervisedOptions::default());
    let mut client = RuntimeClient::new(sup.clone()).with_host("far");
    if let Some(u) = uplink {
        client = client.with_uplink(u);
    }
    (client, sup)
}

async fn wait_for_calls(far: &FarHost, n: usize) {
    tokio::time::timeout(BOUND, async {
        while far.calls.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the subscription was not asked again within the bound");
}

#[tokio::test]
async fn with_uplink_a_refused_subscription_waits_for_a_route_change() {
    let far = Arc::new(FarHost::default());
    let uplink_server = ControlServer::new(
        Arc::new(Idle),
        hello("daemon", "daemon"),
        ConnConfig::default(),
    );
    let (client, sup) = client_to(
        far.clone(),
        Some(Uplink::via_server(uplink_server.clone(), "hub")),
    );
    tokio::time::timeout(BOUND, sup.wait_connected(BOUND))
        .await
        .expect("bound")
        .expect("connected");

    let mut sub = client.subscribe_host(None);
    let first = tokio::spawn(async move { sub.next().await.is_none() });
    wait_for_calls(&far, 1).await;

    // Past the backoff schedule's first retries (250 ms, 500 ms, 1 s, ...): with no route change
    // nothing asks again. The window is a negative check, not a synchronisation point.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        far.calls.load(Ordering::SeqCst),
        1,
        "asked again while no route changed"
    );

    // A hub connects to the uplink: the route changed, so the subscription is asked once more.
    far.up.store(true, Ordering::SeqCst);
    let (a, b) = memory_pair(64 * 1024);
    uplink_server.serve_connection(b);
    let _hub = ControlClient::connect(a, ClientOptions::new(hello("hub", "hub")))
        .await
        .expect("hub connects");
    wait_for_calls(&far, 2).await;
    let ended = tokio::time::timeout(BOUND, first)
        .await
        .expect("bound")
        .expect("task");
    assert!(ended, "the served topic should have ended");
    sup.stop().await;
}

#[tokio::test]
async fn without_uplink_a_refused_subscription_retries_on_the_backoff() {
    let far = Arc::new(FarHost::default());
    let (client, sup) = client_to(far.clone(), None);
    tokio::time::timeout(BOUND, sup.wait_connected(BOUND))
        .await
        .expect("bound")
        .expect("connected");

    let mut sub = client.subscribe_host(None);
    let first = tokio::spawn(async move { sub.next().await });
    // No route to watch: the backoff asks again on its own, with no route change at all.
    wait_for_calls(&far, 2).await;
    far.up.store(true, Ordering::SeqCst);
    sup.stop().await;
    first.abort();
}
