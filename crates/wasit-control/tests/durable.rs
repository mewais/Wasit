//! A durable subscription to a host that is down waits for a link or route change; it does not
//! ask again on a timer.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use futures::stream;
use serde_json::{json, Value};
use tokio::sync::watch;
use wasit_control::{
    codes, connector, ClientOptions, ControlServer, Cursor, DurableOpts, EventStream, HostId,
    LinkState, RpcError, Service, Supervised, SupervisedOptions, UplinkChanges,
};

/// Refuses every subscription with `host_unreachable` while the host is down, counting the
/// calls. Once up, serves one event and ends the topic.
#[derive(Default)]
struct FarHost {
    calls: AtomicUsize,
    up: AtomicBool,
}

#[async_trait]
impl Service for FarHost {
    async fn call(
        &self,
        _ctx: wasit_control::CallContext,
        method: &str,
        _params: Value,
    ) -> Result<Value, RpcError> {
        Err(RpcError::method_not_found(method))
    }

    async fn subscribe(
        &self,
        _ctx: wasit_control::CallContext,
        _topic: &str,
        _params: Value,
        _from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.up.load(Ordering::SeqCst) {
            return Err(RpcError::new(
                codes::HOST_UNREACHABLE,
                "no route to the host",
            ));
        }
        Ok(Box::pin(stream::iter(vec![Ok((
            Cursor::from_u64(1),
            json!("hello"),
        ))])))
    }
}

#[tokio::test]
async fn a_refused_resubscribe_waits_for_the_route_and_makes_no_calls_meanwhile() {
    let far = Arc::new(FarHost::default());
    let server = ControlServer::new(far.clone(), hello("far"), cfg());
    let c = connector(move || {
        let server = server.clone();
        async move { mem_connect(&server, ClientOptions::new(hello("gui"))).await }
    });
    let sup = Supervised::spawn(c, SupervisedOptions::default());
    within(sup.wait_connected(BOUND)).await.expect("connected");

    let (route_tx, route_rx) = watch::channel(BTreeMap::<HostId, LinkState>::new());
    let mut durable = sup
        .subscribe_durable("news", DurableOpts::default())
        .route_changes(UplinkChanges::Hub(route_rx));
    let first = tokio::spawn(async move { durable.next().await });

    // The host refuses the first subscription.
    wait_for_calls(&far, 1).await;
    // Well past the backoff the old retry schedule used: nothing is asked again while it is down.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        far.calls.load(Ordering::SeqCst),
        1,
        "asked again while down"
    );

    // The route comes back: the subscription is asked once more, and it is served.
    far.up.store(true, Ordering::SeqCst);
    route_tx.send_modify(|_| {});
    let item = within(first).await.unwrap().expect("an event");
    let (cursor, value) = item.expect("no error");
    assert_eq!(cursor.as_u64(), Some(1));
    assert_eq!(value, json!("hello"));
    assert_eq!(far.calls.load(Ordering::SeqCst), 2);
    sup.stop().await;
}

async fn wait_for_calls(far: &FarHost, n: usize) {
    within(async {
        while far.calls.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}
