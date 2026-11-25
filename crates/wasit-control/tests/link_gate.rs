//! The hub's inbound link gate: what a remote daemon may ask the hub over its link.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::*;
use serde_json::{json, Value};
use wasit_control::{
    codes, CallContext, CallError, HostId, LinkGate, LinkPolicy, RequestOpts, RpcError, Service,
};

/// Records every call that reaches it and answers `"served"`.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<String>>,
}

#[async_trait]
impl Service for Recorder {
    async fn call(&self, _ctx: CallContext, method: &str, _p: Value) -> Result<Value, RpcError> {
        self.seen.lock().unwrap().push(method.to_string());
        Ok(json!("served"))
    }
}

/// The policy shape Marsad installs: a fixed table, forwarded calls treated more narrowly.
fn policy() -> LinkPolicy {
    Arc::new(|method: &str, forwarded: bool| {
        matches!(
            (method, forwarded),
            ("wasl.supervisor.spawn_plan", false) | ("wasl.node.deliver", _)
        )
    })
}

async fn gated() -> (wasit_control::ControlClient, Arc<Recorder>) {
    let rec = Arc::new(Recorder::default());
    let gate = LinkGate::new(HostId::new("hub"), rec.clone(), policy());
    let (client, _server) = mem_pair(Arc::new(gate), "hub", cfg(), cfg()).await;
    (client, rec)
}

fn opts(host: Option<&str>) -> RequestOpts {
    let mut o = RequestOpts::new(D);
    o.host = host.map(HostId::new);
    o
}

const D: std::time::Duration = std::time::Duration::from_secs(10);

fn refused(e: CallError) -> RpcError {
    match e {
        CallError::Rpc(r) => r,
        other => panic!("expected a typed refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn remote_link_is_refused_host_exec_and_never_reaches_the_service() {
    let (client, rec) = gated().await;
    let e = refused(
        client
            .call("host.exec.start", Value::Null, opts(None))
            .await
            .unwrap_err(),
    );
    assert_eq!(e.code, codes::LINK_REFUSED);
    assert!(e.message.contains("host.exec.start"), "{}", e.message);
    assert!(rec.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn remote_link_forward_to_another_host_is_refused_for_host_calls() {
    let (client, rec) = gated().await;
    let e = refused(
        client
            .call("host.exec.start", Value::Null, opts(Some("other")))
            .await
            .unwrap_err(),
    );
    assert_eq!(e.code, codes::LINK_REFUSED);
    assert!(e.message.contains("another host"), "{}", e.message);
    assert!(rec.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn supervisor_spawn_is_allowed_to_the_hub_but_not_forwarded() {
    let (client, rec) = gated().await;
    let ok: String = client
        .request_with("wasl.supervisor.spawn_plan", Value::Null, opts(None))
        .await
        .unwrap();
    assert_eq!(ok, "served");
    // The same method addressed to another host is not a Wasl route a remote node needs.
    let e = refused(
        client
            .call(
                "wasl.supervisor.spawn_plan",
                Value::Null,
                opts(Some("other")),
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(e.code, codes::LINK_REFUSED);
    assert_eq!(
        *rec.seen.lock().unwrap(),
        vec!["wasl.supervisor.spawn_plan"]
    );
}

#[tokio::test]
async fn node_delivery_is_allowed_locally_and_forwarded() {
    let (client, rec) = gated().await;
    client
        .call("wasl.node.deliver", Value::Null, opts(None))
        .await
        .unwrap();
    client
        .call("wasl.node.deliver", Value::Null, opts(Some("other")))
        .await
        .unwrap();
    assert_eq!(rec.seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn remote_link_subscriptions_are_refused() {
    let (client, _rec) = gated().await;
    let e = client.subscribe("fleet", None).await.unwrap_err();
    let CallError::Rpc(r) = e else {
        panic!("expected a typed refusal, got {e:?}");
    };
    assert_eq!(r.code, codes::LINK_REFUSED);
}
