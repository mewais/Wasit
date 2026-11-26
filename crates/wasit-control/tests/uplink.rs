//! `Uplink`: reaching other hosts from a remote daemon (over the hub's link into it) and from
//! the hub's own daemon (over its links), with change signals for store-and-forward users.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use serde_json::Value;
use wasit_control::demo::DemoService;
use wasit_control::{
    Attempt, CallError, ClientOptions, ConnectError, ControlClient, ControlServer, Hello, HostId,
    HostSpec, Hub, HubConfig, LinkConnector, LinkState, RoutingService, Service, Uplink,
};

const D: Duration = Duration::from_secs(10);

#[derive(Default)]
struct MemConnector {
    targets: Mutex<HashMap<HostId, ControlServer>>,
}

#[async_trait]
impl LinkConnector for MemConnector {
    async fn connect(
        &self,
        spec: &HostSpec,
        opts: ClientOptions,
        _attempt: Attempt,
    ) -> Result<ControlClient, ConnectError> {
        let server = self.targets.lock().unwrap().get(&spec.id).cloned();
        match server {
            Some(s) => mem_connect(&s, opts).await,
            None => Err(ConnectError::Io(format!("no route to {}", spec.id))),
        }
    }
}

fn daemon(host: &str) -> ControlServer {
    ControlServer::new(
        Arc::new(DemoService::new(host)),
        Hello::new("v1", host, "daemon"),
        cfg(),
    )
}

fn hub_cfg() -> HubConfig {
    let mut c = HubConfig::new(Hello::new("v1", "hub", "hub"));
    c.supervise.backoff.initial = Duration::from_millis(5);
    c.supervise.backoff.max = Duration::from_millis(40);
    c
}

#[tokio::test]
async fn uplink_via_server_and_via_hub_reach_other_hosts_and_signal_changes() {
    let mc = Arc::new(MemConnector::default());
    let a = daemon("a");
    let b = daemon("b");
    mc.targets.lock().unwrap().insert("a".into(), a.clone());
    mc.targets.lock().unwrap().insert("b".into(), b.clone());
    let hub = Hub::new(hub_cfg(), mc.clone(), None);
    let routing: Arc<dyn Service> = Arc::new(RoutingService::new(
        hub.clone(),
        Arc::new(DemoService::new("hub")),
    ));
    hub.set_inbound_service(routing);

    let up_a = Uplink::via_server(a.clone(), "hub");
    let host_b = HostId::from("b");
    assert!(!up_a.is_up(&host_b));
    assert!(matches!(
        up_a.call(&host_b, "whoami", Value::Null, D).await,
        Err(CallError::NotConnected)
    ));
    // A connection with another role is not an uplink.
    let _gui = mem_connect(&a, ClientOptions::new(hello("gui")))
        .await
        .unwrap();
    assert!(!up_a.is_up(&host_b));

    let mut changes = up_a.changes();
    let mut st = hub.states();
    hub.add_link(HostSpec::new("a")).unwrap();
    hub.add_link(HostSpec::new("b")).unwrap();
    wait_watch(&mut st, |m| {
        m.len() == 2 && m.values().all(LinkState::is_connected)
    })
    .await;
    within(async {
        while !up_a.is_up(&host_b) {
            assert!(changes.changed().await);
        }
    })
    .await;
    let who: String =
        serde_json::from_value(up_a.call(&host_b, "whoami", Value::Null, D).await.unwrap())
            .unwrap();
    assert_eq!(who, "b");

    // The hub's own daemon reaches `a` over its link.
    let up_hub = Uplink::via_hub(hub.clone());
    let who: String = serde_json::from_value(
        up_hub
            .call(&HostId::from("a"), "whoami", Value::Null, D)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(who, "a");
    assert!(!up_hub.is_up(&HostId::from("nowhere")));

    // The link to `a` goes away: the uplink on `a` is gone and the change is signalled.
    let mut changes = up_a.changes();
    let mut hub_changes = up_hub.changes();
    assert!(hub.remove_link(&HostId::from("a")).await);
    within(async {
        while up_a.is_up(&host_b) {
            assert!(changes.changed().await);
        }
    })
    .await;
    within(hub_changes.changed()).await;
    assert!(!up_hub.is_up(&HostId::from("a")));
    hub.shutdown().await;
}
