//! Link supervisor, provisioning state machine and routing.

mod common;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use serde_json::{json, Value};
use tokio::sync::{broadcast, watch};
use wasit_control::demo::DemoService;
use wasit_control::{
    codes, connector, Attempt, CallError, ClientOptions, CommandFailureKind, ConnectError,
    ControlClient, ControlServer, DurableOpts, Hello, HostId, HostSpec, Hub, HubConfig, HubError,
    LinkConnector, LinkEvent, LinkState, ProvisionError, ProvisionReason, Provisioner, RequestOpts,
    RoutingService, Service, SubscribeOpts, Supervised, SupervisedConfig, SupervisedOptions,
    TraceId,
};

const D: Duration = Duration::from_secs(10);

/// In-process "remote hosts" with scripted connect failures.
#[derive(Default)]
struct MemConnector {
    targets: Mutex<HashMap<HostId, ControlServer>>,
    scripted: Mutex<HashMap<HostId, VecDeque<ConnectError>>>,
    attempts: watch::Sender<u32>,
    /// Hosts whose links must wait for a person ([`LinkConnector::hold`]).
    held: Mutex<HashSet<HostId>>,
}

impl MemConnector {
    fn set_target(&self, host: &str, server: ControlServer) {
        self.targets.lock().unwrap().insert(host.into(), server);
    }
    fn hold_on(&self, host: &str, on: bool) {
        let mut held = self.held.lock().unwrap();
        if on {
            held.insert(host.into());
        } else {
            held.remove(&HostId::from(host));
        }
    }
    fn script(&self, host: &str, errs: Vec<ConnectError>) {
        self.scripted
            .lock()
            .unwrap()
            .insert(host.into(), errs.into());
    }
}

#[async_trait]
impl LinkConnector for MemConnector {
    async fn connect(
        &self,
        spec: &HostSpec,
        opts: ClientOptions,
        _attempt: Attempt,
    ) -> Result<ControlClient, ConnectError> {
        self.attempts.send_modify(|n| *n += 1);
        let scripted = self
            .scripted
            .lock()
            .unwrap()
            .get_mut(&spec.id)
            .and_then(VecDeque::pop_front);
        if let Some(e) = scripted {
            return Err(e);
        }
        let server = self.targets.lock().unwrap().get(&spec.id).cloned();
        match server {
            Some(s) => mem_connect(&s, opts).await,
            None => Err(ConnectError::Io(format!("no route to {}", spec.id))),
        }
    }

    async fn hold(&self, spec: &HostSpec) -> Option<String> {
        self.held
            .lock()
            .unwrap()
            .contains(&spec.id)
            .then(|| "a person must start this one".to_owned())
    }
}

fn hub_cfg(host: &str, build: &str) -> HubConfig {
    let mut c = HubConfig::new(Hello::new(build, host, "hub"));
    c.supervise = SupervisedConfig::default();
    c.supervise.backoff.initial = Duration::from_millis(5);
    c.supervise.backoff.max = Duration::from_millis(40);
    c
}

fn daemon(host: &str, build: &str) -> ControlServer {
    ControlServer::new(
        Arc::new(DemoService::new(host)),
        Hello::new(build, host, "daemon"),
        cfg(),
    )
}

fn spec(host: &str) -> HostSpec {
    HostSpec::new(host, ["unused"])
}

/// Collect state transitions of `host` until `stop` matches (inclusive).
async fn collect_until(
    rx: &mut broadcast::Receiver<LinkEvent>,
    host: &str,
    mut stop: impl FnMut(&LinkState) -> bool,
) -> Vec<LinkState> {
    let mut out = Vec::new();
    within(async {
        loop {
            let ev = rx.recv().await.expect("event stream");
            if ev.host.as_str() != host {
                continue;
            }
            let Some(s) = ev.state else { continue };
            let done = stop(&s);
            out.push(s);
            if done {
                break;
            }
        }
    })
    .await;
    out
}

fn names(states: &[LinkState]) -> Vec<&'static str> {
    states
        .iter()
        .map(|s| match s {
            LinkState::Connecting => "connecting",
            LinkState::Provisioning { .. } => "provisioning",
            LinkState::Connected { .. } => "connected",
            LinkState::Reconnecting { .. } => "reconnecting",
            LinkState::AuthNeeded { .. } => "auth_needed",
            LinkState::Incompatible { .. } => "incompatible",
            LinkState::Failed { .. } => "failed",
            LinkState::Stopped => "stopped",
        })
        .collect()
}

type Fix = Box<dyn Fn(&ProvisionReason) -> Result<(), ProvisionError> + Send + Sync>;

/// Provisioner that records calls and runs a fix-up closure.
struct TestProvisioner {
    calls: Mutex<Vec<(HostId, ProvisionReason)>>,
    fix: Fix,
}

#[async_trait]
impl Provisioner for TestProvisioner {
    async fn ensure(
        &self,
        host: &HostSpec,
        problem: ProvisionReason,
    ) -> Result<(), ProvisionError> {
        self.calls
            .lock()
            .unwrap()
            .push((host.id.clone(), problem.clone()));
        (self.fix)(&problem)
    }
}

fn not_found() -> ConnectError {
    ConnectError::Command {
        kind: CommandFailureKind::CommandNotFound,
        exit_code: Some(127),
        stderr: "marsad-agent: command not found".into(),
    }
}

#[tokio::test]
async fn missing_helper_is_provisioned_then_connected() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    mc.script("r1", vec![not_found()]);
    let prov = Arc::new(TestProvisioner {
        calls: Mutex::new(Vec::new()),
        fix: Box::new(|_| Ok(())),
    });
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), Some(prov.clone()));
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(
        names(&states),
        ["connecting", "provisioning", "connecting", "connected"]
    );
    assert!(matches!(
        &states[1],
        LinkState::Provisioning { reason: ProvisionReason::CommandNotFound { detail } } if detail.contains("not found")
    ));
    assert_eq!(prov.calls.lock().unwrap().len(), 1);
    assert!(hub.states().borrow()[&HostId::from("r1")].is_connected());
    hub.shutdown().await;
}

#[tokio::test]
async fn different_build_is_provisioned_then_connected() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "old"));
    let mc2 = mc.clone();
    let prov = Arc::new(TestProvisioner {
        calls: Mutex::new(Vec::new()),
        fix: Box::new(move |_| {
            // "Upgrade" the remote helper.
            mc2.set_target("r1", daemon("r1", "new"));
            Ok(())
        }),
    });
    let hub = Hub::new(hub_cfg("hub", "new"), mc.clone(), Some(prov.clone()));
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(
        names(&states),
        ["connecting", "provisioning", "connecting", "connected"]
    );
    assert_eq!(
        states[1],
        LinkState::Provisioning {
            reason: ProvisionReason::BuildMismatch {
                expected: "new".into(),
                found: "old".into()
            }
        }
    );
    match &states[3] {
        LinkState::Connected { hello } => assert_eq!(hello.build, "new"),
        _ => unreachable!(),
    }
    hub.shutdown().await;
}

#[tokio::test]
async fn provisioning_that_does_not_help_fails_until_reconnect_now() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    mc.script("r1", vec![not_found(), not_found()]);
    let prov = Arc::new(TestProvisioner {
        calls: Mutex::new(Vec::new()),
        fix: Box::new(|_| Ok(())),
    });
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), Some(prov.clone()));
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::Failed { .. })).await;
    assert_eq!(
        names(&states),
        ["connecting", "provisioning", "connecting", "failed"]
    );
    assert_eq!(
        prov.calls.lock().unwrap().len(),
        1,
        "provisions once per cycle"
    );
    let attempts = *mc.attempts.borrow();
    hub.reconnect_now(&HostId::from("r1"));
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(names(&states), ["connecting", "connected"]);
    assert_eq!(*mc.attempts.borrow(), attempts + 1);
    hub.shutdown().await;
}

#[tokio::test]
async fn non_retryable_provisioning_error_is_failed_and_retryable_backs_off() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    mc.script("r1", vec![not_found(), not_found()]);
    let n = Arc::new(Mutex::new(0));
    let n2 = n.clone();
    let prov = Arc::new(TestProvisioner {
        calls: Mutex::new(Vec::new()),
        fix: Box::new(move |_| {
            let mut n = n2.lock().unwrap();
            *n += 1;
            Err(ProvisionError {
                message: format!("copy failed #{n}"),
                retryable: *n == 1,
            })
        }),
    });
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), Some(prov));
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::Failed { .. })).await;
    assert_eq!(
        names(&states),
        [
            "connecting",
            "provisioning",
            "reconnecting",
            "connecting",
            "provisioning",
            "failed"
        ]
    );
    match states.last().unwrap() {
        LinkState::Failed { reason, .. } => assert!(reason.contains("copy failed #2")),
        _ => unreachable!(),
    }
    hub.shutdown().await;
}

/// Configuration problems (unknown WSL distro, no helper build, foreign bootstrap output,
/// permission denied, a transport program that cannot start) fail the link without retrying;
/// only a manual reconnect tries again. Transport drops back off and retry.
#[tokio::test]
async fn configuration_problems_fail_without_retrying() {
    let cases = vec![
        (
            ConnectError::Command {
                kind: CommandFailureKind::UnknownDistro,
                exit_code: Some(1),
                stderr: "There is no distribution with the supplied name.".into(),
            },
            Some(CommandFailureKind::UnknownDistro),
        ),
        (
            ConnectError::Command {
                kind: CommandFailureKind::UnsupportedPlatform,
                exit_code: None,
                stderr: "no helper for linux/riscv64".into(),
            },
            Some(CommandFailureKind::UnsupportedPlatform),
        ),
        (
            ConnectError::Command {
                kind: CommandFailureKind::UnexpectedOutput,
                exit_code: None,
                stderr: "Welcome to my server".into(),
            },
            Some(CommandFailureKind::UnexpectedOutput),
        ),
        (
            ConnectError::Command {
                kind: CommandFailureKind::PermissionDenied,
                exit_code: Some(126),
                stderr: "Permission denied".into(),
            },
            Some(CommandFailureKind::PermissionDenied),
        ),
        (
            ConnectError::Spawn("wsl.exe: program not found".into()),
            None,
        ),
    ];
    for (err, problem) in cases {
        let mc = Arc::new(MemConnector::default());
        mc.set_target("r1", daemon("r1", "v1"));
        mc.script("r1", vec![err.clone()]);
        let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
        let mut ev = hub.events();
        hub.add_link(spec("r1")).unwrap();
        let states = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::Failed { .. })).await;
        assert_eq!(names(&states), ["connecting", "failed"], "{err:?}");
        match &states[1] {
            LinkState::Failed { problem: p, .. } => assert_eq!(*p, problem, "{err:?}"),
            _ => unreachable!(),
        }
        let mut attempts = mc.attempts.subscribe();
        let retried =
            tokio::time::timeout(Duration::from_millis(300), attempts.wait_for(|n| *n > 1))
                .await
                .is_ok();
        assert!(!retried, "failed link retried on its own: {err:?}");
        hub.reconnect_now(&HostId::from("r1"));
        let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
        assert_eq!(names(&states), ["connecting", "connected"]);
        hub.shutdown().await;
    }
    // A transport drop is retried with backoff.
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    mc.script(
        "r1",
        vec![ConnectError::Command {
            kind: CommandFailureKind::Unreachable,
            exit_code: Some(255),
            stderr: "Connection reset by peer".into(),
        }],
    );
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(
        names(&states),
        ["connecting", "reconnecting", "connecting", "connected"]
    );
    hub.shutdown().await;
}

#[tokio::test]
async fn auth_problems_park_until_reconnect_now() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    mc.script(
        "r1",
        vec![ConnectError::Command {
            kind: CommandFailureKind::HostKeyUnknown,
            exit_code: Some(255),
            stderr: "Host key verification failed.".into(),
        }],
    );
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::AuthNeeded { .. })).await;
    assert_eq!(names(&states), ["connecting", "auth_needed"]);
    assert!(matches!(
        states[1],
        LinkState::AuthNeeded {
            problem: CommandFailureKind::HostKeyUnknown,
            ..
        }
    ));
    // Parked: no retry happens on its own. (Bounded check: a backoff retry would come
    // within 40ms.)
    let mut attempts = mc.attempts.subscribe();
    let retried = tokio::time::timeout(Duration::from_millis(300), attempts.wait_for(|n| *n > 1))
        .await
        .is_ok();
    assert!(!retried, "parked link retried on its own");
    hub.reconnect_now(&HostId::from("r1"));
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(names(&states), ["connecting", "connected"]);
    hub.shutdown().await;
}

#[tokio::test]
async fn transport_failures_back_off_with_increasing_attempts_then_recover() {
    let mc = Arc::new(MemConnector::default());
    mc.script(
        "r1",
        vec![
            ConnectError::Io("e1".into()),
            ConnectError::Io("e2".into()),
            ConnectError::Io("e3".into()),
        ],
    );
    mc.set_target("r1", daemon("r1", "v1"));
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    let attempts: Vec<(u32, String)> = states
        .iter()
        .filter_map(|s| match s {
            LinkState::Reconnecting {
                attempt,
                last_error,
                ..
            } => Some((*attempt, last_error.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(attempts.len(), 3);
    for (i, (a, e)) in attempts.iter().enumerate() {
        assert_eq!(*a, i as u32 + 1);
        assert!(e.contains(&format!("e{}", i + 1)));
    }
    // Link loss after connecting goes back to reconnecting, then connected again.
    let first = hub.link(&HostId::from("r1")).unwrap().current().unwrap();
    first.close();
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(names(&states), ["reconnecting", "connecting", "connected"]);
    hub.shutdown().await;
}

/// A link the connector holds is parked as `AuthNeeded { held }` without any attempt: at its
/// start, and after a drop (where it never announces a reconnect it will not make and keeps the
/// reason the link dropped). `reconnect_now` while the hold stands changes nothing; once the
/// connector lets go, it connects.
#[tokio::test]
async fn a_held_link_parks_without_an_attempt_and_keeps_why_it_dropped() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    mc.hold_on("r1", true);
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
    let mut ev = hub.events();
    let id = HostId::from("r1");
    hub.add_link(spec("r1")).unwrap();

    // At its start: parked, with no attempt and no cause (it had not been up).
    let states = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::AuthNeeded { .. })).await;
    assert_eq!(names(&states), ["auth_needed"]);
    match &states[0] {
        LinkState::AuthNeeded {
            problem,
            detail,
            cause,
            held,
        } => {
            assert_eq!(*problem, CommandFailureKind::AuthNeeded);
            assert_eq!(detail, "a person must start this one");
            assert_eq!((cause, held), (&None, &true));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(*mc.attempts.borrow(), 0);

    // A reconnect request while the hold stands starts nothing: the link says it is still
    // waiting (the event is the proof the supervisor looked).
    hub.reconnect_now(&id);
    let again = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::AuthNeeded { .. })).await;
    assert_eq!(names(&again), ["auth_needed"]);
    assert_eq!(*mc.attempts.borrow(), 0);

    // Let go and ask: it connects.
    mc.hold_on("r1", false);
    hub.reconnect_now(&id);
    let states = collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(names(&states), ["connecting", "connected"]);
    assert_eq!(*mc.attempts.borrow(), 1);

    // The link drops while the hold stands: parked at once, no "reconnecting", no attempt, and
    // the drop's own reason is kept next to the held fact.
    mc.hold_on("r1", true);
    let first = hub.link(&id).unwrap().current().unwrap();
    first.close();
    let states = collect_until(&mut ev, "r1", |s| matches!(s, LinkState::AuthNeeded { .. })).await;
    assert_eq!(names(&states), ["auth_needed"]);
    match &states[0] {
        LinkState::AuthNeeded {
            detail,
            cause,
            held,
            ..
        } => {
            assert_eq!(detail, "a person must start this one");
            assert!(*held);
            let cause = cause.as_deref().expect("the drop's reason");
            assert!(!cause.is_empty() && !cause.contains("person"), "{cause}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(*mc.attempts.borrow(), 1, "no attempt after the drop");

    // Another click, hold released: connects once more; the old reason is not carried over.
    mc.hold_on("r1", false);
    hub.reconnect_now(&id);
    collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(*mc.attempts.borrow(), 2);
    hub.shutdown().await;
}

#[tokio::test]
async fn links_can_be_added_removed_and_listed() {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("r1", daemon("r1", "v1"));
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
    let mut ev = hub.events();
    hub.add_link(spec("r1")).unwrap();
    assert_eq!(
        hub.add_link(spec("r1")),
        Err(HubError::AlreadyExists("r1".into()))
    );
    assert_eq!(
        hub.add_link(spec("hub")),
        Err(HubError::LocalHost("hub".into()))
    );
    collect_until(&mut ev, "r1", LinkState::is_connected).await;
    assert_eq!(hub.link_ids(), vec![HostId::from("r1")]);
    assert!(hub.remove_link(&HostId::from("r1")).await);
    // Stopped, then removal.
    let mut saw_stopped = false;
    within(async {
        loop {
            let e = ev.recv().await.unwrap();
            match e.state {
                Some(LinkState::Stopped) => saw_stopped = true,
                None => break,
                _ => {}
            }
        }
    })
    .await;
    assert!(saw_stopped);
    assert!(hub.states().borrow().is_empty());
    assert!(!hub.remove_link(&HostId::from("r1")).await);
}

/// Hub `hub` with local demo service and links to `a`, `b`; front server = routing service.
async fn routed_setup() -> (Hub, Arc<MemConnector>, ControlServer, ControlClient) {
    let mc = Arc::new(MemConnector::default());
    mc.set_target("a", daemon("a", "v1"));
    mc.set_target("b", daemon("b", "v1"));
    let hub = Hub::new(hub_cfg("hub", "v1"), mc.clone(), None);
    let routing: Arc<dyn Service> = Arc::new(RoutingService::new(
        hub.clone(),
        Arc::new(DemoService::new("hub")),
    ));
    hub.set_inbound_service(routing.clone());
    let front = ControlServer::new(routing, Hello::new("v1", "hub", "hub"), cfg());
    let mut st = hub.states();
    hub.add_link(spec("a")).unwrap();
    hub.add_link(spec("b")).unwrap();
    wait_watch(&mut st, |m| {
        m.len() == 2 && m.values().all(LinkState::is_connected)
    })
    .await;
    let gui = mem_connect(&front, ClientOptions::new(hello("gui")))
        .await
        .unwrap();
    (hub, mc, front, gui)
}

fn to(host: &str) -> RequestOpts {
    RequestOpts::new(D).host(host)
}

#[tokio::test]
async fn requests_are_routed_by_host() {
    let (hub, _mc, _front, gui) = routed_setup().await;
    let who: String = gui.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "hub");
    for h in ["hub", "a", "b"] {
        let who: String = gui
            .request_with("whoami", Value::Null, to(h))
            .await
            .unwrap();
        assert_eq!(who, h);
    }
    let e = gui
        .request_with::<Value>("whoami", Value::Null, to("zz"))
        .await
        .unwrap_err();
    assert!(
        matches!(e, CallError::Rpc(ref r) if r.code == codes::HOST_UNKNOWN),
        "{e:?}"
    );
    // Errors from the remote service pass through unchanged.
    let e = gui
        .request_with::<Value>("fail", Value::Null, to("a"))
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref r) if r.code == "demo_failure"));

    // Subscriptions are forwarded with cursor passthrough.
    let mut opts = SubscribeOpts::new(D)
        .params(json!({"limit": 10}))
        .from(Some(wasit_control::Cursor::from_u64(5)));
    opts.host = Some("a".into());
    let mut sub = gui.subscribe_with("count", opts).await.unwrap();
    let mut got = Vec::new();
    while let Some((c, _)) = within(sub.next()).await {
        got.push(c.as_u64().unwrap());
    }
    assert_eq!(got, vec![6, 7, 8, 9, 10]);
    assert_eq!(sub.close_reason(), Some(&wasit_control::CloseReason::Ended));
    hub.shutdown().await;
}

/// A trace id travels with a request: client → hub → remote service, and on a chain
/// client → hub → host → hub → host; a request without one arrives without one.
#[tokio::test]
async fn a_trace_survives_every_hop_of_a_routed_request() {
    let (hub, _mc, _front, gui) = routed_setup().await;
    let t = TraceId::new("u-0123456789abcdef").unwrap();
    for h in ["hub", "a", "b"] {
        let seen: Option<TraceId> = gui
            .request_with("trace", Value::Null, to(h).trace(Some(t.clone())))
            .await
            .unwrap();
        assert_eq!(seen.as_ref(), Some(&t), "served by {h}");
        let none: Option<TraceId> = gui.request_with("trace", Value::Null, to(h)).await.unwrap();
        assert_eq!(none, None, "an untraced request to {h}");
    }
    let seen: Option<TraceId> = gui
        .request_with(
            "peer_call",
            json!({"host": "b", "method": "trace"}),
            to("a").trace(Some(t.clone())),
        )
        .await
        .unwrap();
    assert_eq!(seen, Some(t), "gui -> hub -> a -> hub -> b");
    hub.shutdown().await;
}

#[tokio::test]
async fn remote_to_hub_to_remote_routing() {
    let (hub, mc, _front, gui) = routed_setup().await;
    // The daemon on `a` asks the hub (over its link) to reach `b`.
    let a_server = mc.targets.lock().unwrap()[&HostId::from("a")].clone();
    let hub_conn = a_server
        .connections()
        .into_iter()
        .find(|c| c.remote_hello().role == "hub")
        .expect("hub link on a");
    let who: String = hub_conn
        .request_with("whoami", Value::Null, to("b"))
        .await
        .unwrap();
    assert_eq!(who, "b");
    // No host on an inbound link request = the hub itself.
    let who: String = hub_conn.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "hub");

    // Full chain: gui -> hub -> a (peer_call) -> hub -> b.
    let who: String = gui
        .request_with(
            "peer_call",
            json!({"host": "b", "method": "whoami"}),
            to("a"),
        )
        .await
        .unwrap();
    assert_eq!(who, "b");
    hub.shutdown().await;
}

#[tokio::test]
async fn routing_loops_are_cut_by_the_hop_limit() {
    // Two hubs that each believe the other leads to `x`.
    let mc1 = Arc::new(MemConnector::default());
    let mc2 = Arc::new(MemConnector::default());
    let hub1 = Hub::new(hub_cfg("h1", "v1"), mc1.clone(), None);
    let hub2 = Hub::new(hub_cfg("h2", "v1"), mc2.clone(), None);
    let front1 = ControlServer::new(
        Arc::new(RoutingService::new(
            hub1.clone(),
            Arc::new(DemoService::new("h1")),
        )),
        Hello::new("v1", "h1", "hub"),
        cfg(),
    );
    let front2 = ControlServer::new(
        Arc::new(RoutingService::new(
            hub2.clone(),
            Arc::new(DemoService::new("h2")),
        )),
        Hello::new("v1", "h2", "hub"),
        cfg(),
    );
    mc1.set_target("x", front2.clone());
    mc2.set_target("x", front1.clone());
    let (mut s1, mut s2) = (hub1.states(), hub2.states());
    hub1.add_link(spec("x")).unwrap();
    hub2.add_link(spec("x")).unwrap();
    wait_watch(&mut s1, |m| {
        m.values().all(LinkState::is_connected) && !m.is_empty()
    })
    .await;
    wait_watch(&mut s2, |m| {
        m.values().all(LinkState::is_connected) && !m.is_empty()
    })
    .await;

    let gui = mem_connect(&front1, ClientOptions::new(hello("gui")))
        .await
        .unwrap();
    let e = gui
        .request_with::<Value>("whoami", Value::Null, to("x"))
        .await
        .unwrap_err();
    match e {
        CallError::Rpc(r) => {
            assert_eq!(r.code, codes::HOP_LIMIT);
            assert!(r.message.contains("4 hops"), "{}", r.message);
        }
        other => panic!("unexpected {other:?}"),
    }
    hub1.shutdown().await;
    hub2.shutdown().await;
}

#[tokio::test]
async fn link_loss_closes_forwarded_subscription_and_durable_client_resumes() {
    let (hub, mc, front, _gui) = routed_setup().await;

    // A supervised GUI client with a durable subscription to `count` on host `a`.
    let f2 = front.clone();
    let conn = connector(move || {
        let f = f2.clone();
        async move { mem_connect(&f, ClientOptions::new(hello("gui"))).await }
    });
    let mut config = SupervisedConfig::default();
    config.backoff.initial = Duration::from_millis(5);
    config.backoff.max = Duration::from_millis(40);
    let sup = Supervised::spawn(
        conn,
        SupervisedOptions {
            config,
            ..Default::default()
        },
    );
    const N: u64 = 2000;
    let mut durable = sup.subscribe_durable(
        "count",
        DurableOpts {
            host: Some("a".into()),
            params: json!({ "limit": N }),
            ..Default::default()
        },
    );
    let mut expected = 1u64;
    while expected <= 50 {
        let (c, _) = within(durable.next()).await.unwrap().unwrap();
        assert_eq!(c.as_u64(), Some(expected));
        expected += 1;
    }

    // Host `a` restarts: the link drops (downstream sees HostLost), then comes back.
    let old = mc.targets.lock().unwrap()[&HostId::from("a")].clone();
    let mut st = hub.states();
    mc.set_target("a", daemon("a", "v1"));
    old.shutdown(D).await;
    wait_watch(&mut st, |m| !m[&HostId::from("a")].is_connected()).await;

    while let Some(item) = within(durable.next()).await {
        let (c, _) = item.unwrap();
        assert_eq!(c.as_u64(), Some(expected), "gap or duplicate");
        expected += 1;
    }
    assert_eq!(expected, N + 1);
    assert!(durable.subscribe_count() >= 2);
    sup.stop().await;
    hub.shutdown().await;
}

/// Whoever is told a link is connected can use it at once: the client is in place before the
/// state is announced. (Marsad applies a host's settings when its link connects; when its
/// request won the race against the client being installed, routing answered "host is not
/// connected" and a restarted daemon never got its settings back.)
#[tokio::test]
async fn a_link_announced_connected_is_usable_at_once() {
    let server = daemon("a", "v1");
    let (gate, gate_rx) = watch::channel(false);
    let conn = connector(move || {
        let s = server.clone();
        let mut gate = gate_rx.clone();
        async move {
            // Connect only once the test holds the supervisor (the callback reads it).
            let _ = gate.wait_for(|open| *open).await;
            mem_connect(&s, ClientOptions::new(hello("gui"))).await
        }
    });
    let slot: Arc<std::sync::OnceLock<Supervised>> = Arc::default();
    let (tx, mut usable) = tokio::sync::mpsc::unbounded_channel();
    let seen = slot.clone();
    let on_state = Arc::new(move |s: &LinkState| {
        if s.is_connected() {
            let _ = tx.send(seen.get().and_then(Supervised::current).is_some());
        }
    });
    let mut config = SupervisedConfig::default();
    config.backoff.initial = Duration::from_millis(5);
    let sup = Supervised::spawn(
        conn,
        SupervisedOptions {
            config,
            on_state: Some(on_state),
            ..Default::default()
        },
    );
    assert!(slot.set(sup.clone()).is_ok());
    gate.send_replace(true);
    assert_eq!(within(usable.recv()).await, Some(true), "first connect");
    sup.reconnect_now();
    assert_eq!(within(usable.recv()).await, Some(true), "reconnect");
    sup.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn hub_links_over_real_child_processes() {
    let mut c = hub_cfg("hub", "hub-build");
    c.require_same_build = true; // no provisioner: mismatch is accepted
    let hub = Hub::new(c, Arc::new(wasit_control::CommandConnector), None);
    let routing: Arc<dyn Service> = Arc::new(RoutingService::new(
        hub.clone(),
        Arc::new(DemoService::new("hub")),
    ));
    hub.set_inbound_service(routing.clone());
    let mut ev = hub.events();
    hub.add_link(HostSpec::new(
        "p1",
        [
            demo_bin(),
            "serve-stdio",
            "--host",
            "p1",
            "--build",
            "other",
        ],
    ))
    .unwrap();
    hub.add_link(HostSpec::new(
        "broken",
        [
            "sh",
            "-c",
            "echo 'Permission denied (publickey).' >&2; exit 255",
        ],
    ))
    .unwrap();
    let p1 = collect_until(&mut ev, "p1", LinkState::is_connected).await;
    assert_eq!(names(&p1), ["connecting", "connected"]);
    let mut st = hub.states();
    wait_watch(&mut st, |m| {
        matches!(
            m.get(&HostId::from("broken")),
            Some(LinkState::AuthNeeded {
                problem: CommandFailureKind::AuthNeeded,
                ..
            })
        )
    })
    .await;

    let front = ControlServer::new(routing, Hello::new("hub-build", "hub", "hub"), cfg());
    let gui = mem_connect(&front, ClientOptions::new(hello("gui")))
        .await
        .unwrap();
    let who: String = gui
        .request_with("whoami", Value::Null, to("p1"))
        .await
        .unwrap();
    assert_eq!(who, "p1");
    let e = gui
        .request_with::<Value>("whoami", Value::Null, to("broken"))
        .await
        .unwrap_err();
    assert!(
        matches!(e, CallError::Rpc(ref r) if r.code == codes::HOST_UNREACHABLE),
        "{e:?}"
    );
    hub.shutdown().await;
}

#[tokio::test]
async fn forwarded_subscription_closes_with_host_lost_when_the_link_drops() {
    let (hub, mc, _front, gui) = routed_setup().await;
    let mut opts = SubscribeOpts::new(D);
    opts.host = Some("a".into());
    let mut sub = gui.subscribe_with("count", opts).await.unwrap();
    assert!(within(sub.next()).await.is_some());
    let a = mc.targets.lock().unwrap()[&HostId::from("a")].clone();
    a.shutdown(D).await;
    while within(sub.next()).await.is_some() {}
    assert_eq!(
        sub.close_reason(),
        Some(&wasit_control::CloseReason::HostLost)
    );
    // The GUI's own connection to the hub is unaffected.
    assert!(!gui.is_closed());
    let who: String = gui.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "hub");
    hub.shutdown().await;
}
