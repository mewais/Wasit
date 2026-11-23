//! The connection trail (design S13): real links, a real sink, real child processes. One global
//! sink per process: every test uses its own link label (or host ids) and reads back only the
//! records that name it. This binary holds no other test that loops on reconnects, so the
//! per-kind budgets are never what a test sees.
//!
//! Nothing here waits for time to pass: tests await the state a link publishes (a record is
//! emitted before the state it describes is published) and the sink's own flush.

mod common;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use wasit_control::demo::DemoService;
use wasit_control::{
    codes, connector_with_attempt, Backoff, CallError, ClientOptions, CloseReason,
    CommandFailureKind, ConnConfig, ConnectError, ControlClient, ControlServer, Cursor,
    EventStream, Hello, HostId, HostSpec, Hub, HubConfig, LinkConnector, LinkState,
    ProtocolVersion, ProvisionError, ProvisionReason, RequestOpts, RoutingService, RpcError,
    Service, SubscribeOpts, Supervised, SupervisedConfig, SupervisedOptions, SupervisorHooks,
    TraceId,
};
use wasit_diag::{Code, Ident, SinkConfig};

const FILE: &str = "control.ndjson";
/// A secret registered with the scrubber; none of the tests' records may contain it.
const SECRET: &str = "hunter2-hunter2";

// ── the sink ────────────────────────────────────────────────────────────────

fn dir() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        // Kept for the life of the process: the sink writes here until the process ends.
        let dir = tempfile::tempdir().unwrap().keep();
        let cfg = SinkConfig::new(Code::st("hub"), Ident::st("test"), &dir, FILE);
        wasit_diag::install(cfg).unwrap();
        assert!(wasit_diag::scrub::register_secret(SECRET));
        dir
    })
}

/// Every record written so far (what was emitted before this call is in it).
fn all() -> Vec<Value> {
    let dir = dir();
    assert!(wasit_diag::flush(BOUND), "the sink did not flush");
    // The writer creates the file with the first record.
    std::fs::read_to_string(dir.join(FILE))
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .filter(|v| v.get("diag_file").is_none())
        .collect()
}

fn of_link(link: &str) -> Vec<Value> {
    all().into_iter().filter(|v| v["link"] == link).collect()
}

fn evs(records: &[Value]) -> Vec<&str> {
    records.iter().map(|v| v["ev"].as_str().unwrap()).collect()
}

fn find<'a>(records: &'a [Value], ev: &str) -> Vec<&'a Value> {
    records.iter().filter(|v| v["ev"] == ev).collect()
}

fn one<'a>(records: &'a [Value], ev: &str) -> &'a Value {
    let v = find(records, ev);
    assert_eq!(v.len(), 1, "{ev} in {records:#?}");
    v[0]
}

fn no_secret(records: &[Value]) {
    let text = serde_json::to_string(records).unwrap();
    assert!(!text.contains(SECRET), "a secret reached a record: {text}");
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn daemon(host: &str) -> ControlServer {
    ControlServer::new(
        Arc::new(DemoService::new(host)),
        Hello::new("b1", host, "daemon"),
        cfg(),
    )
}

/// A config whose backoff never fires on its own: a test moves a link on with
/// `reconnect_now` or stops it.
fn slow() -> SupervisedConfig {
    SupervisedConfig {
        backoff: Backoff {
            initial: Duration::from_secs(3600),
            max: Duration::from_secs(3600),
            multiplier: 2.0,
            jitter: 0.0,
        },
        ..SupervisedConfig::default()
    }
}

/// The codes of the states a link publishes, in order.
struct States {
    rx: mpsc::UnboundedReceiver<LinkState>,
}

impl States {
    /// The options that report every state change here.
    fn options(
        label: &str,
        config: SupervisedConfig,
        hooks: Option<Arc<dyn SupervisorHooks>>,
    ) -> (SupervisedOptions, States) {
        let (tx, rx) = mpsc::unbounded_channel();
        let opts = SupervisedOptions {
            config,
            hooks,
            on_state: Some(Arc::new(move |s: &LinkState| {
                // The test may have stopped listening.
                let _ = tx.send(s.clone());
            })),
            diag: Some(Ident::new(label)),
        };
        (opts, States { rx })
    }

    /// Waits for the next state `pred` accepts.
    async fn until(&mut self, mut pred: impl FnMut(&LinkState) -> bool) -> LinkState {
        within(async {
            loop {
                let s = self.rx.recv().await.expect("link ended");
                if pred(&s) {
                    return s;
                }
            }
        })
        .await
    }

    async fn settled(&mut self) -> LinkState {
        self.until(|s| !matches!(s, LinkState::Connecting)).await
    }
}

type Script = Arc<Mutex<VecDeque<Result<(), ConnectError>>>>;

/// A connector that fails as scripted and then connects to `server`; it remembers the attempt
/// numbers it was given.
fn scripted(
    server: ControlServer,
    script: Script,
    seen: Arc<Mutex<Vec<u64>>>,
) -> wasit_control::Connector {
    connector_with_attempt(move |attempt| {
        let server = server.clone();
        let script = script.clone();
        let seen = seen.clone();
        async move {
            seen.lock().unwrap().push(attempt);
            let next = script.lock().unwrap().pop_front();
            match next {
                Some(Err(e)) => Err(e),
                _ => mem_connect(&server, ClientOptions::new(hello("diag"))).await,
            }
        }
    })
}

fn script(items: Vec<Result<(), ConnectError>>) -> Script {
    Arc::new(Mutex::new(items.into()))
}

fn command(kind: CommandFailureKind, stderr: &str) -> ConnectError {
    ConnectError::Command {
        kind,
        exit_code: Some(255),
        stderr: stderr.into(),
    }
}

// ── an attempt, its outcome and the states ──────────────────────────────────

#[tokio::test]
async fn a_link_that_connects_records_its_attempt_and_every_transition_in_order() {
    dir();
    let seen = Arc::default();
    let (opts, mut states) = States::options("t:connects", slow(), None);
    let sup = Supervised::spawn(
        scripted(daemon("peer"), script(vec![]), Arc::clone(&seen)),
        opts,
    );
    states.until(|s| s.is_connected()).await;
    sup.stop().await;
    states.until(|s| matches!(s, LinkState::Stopped)).await;

    assert_eq!(*seen.lock().unwrap(), [1]);
    let r = of_link("t:connects");
    assert_eq!(
        evs(&r),
        [
            "link.attempt_begin",
            "link.attempt_end",
            "link.state",
            "link.closed",
            "link.state"
        ],
        "{r:#?}"
    );
    assert_eq!(r[0]["n"], 1);
    assert_eq!(r[1]["n"], 1);
    assert_eq!(r[1]["outcome"], "connected");
    assert!(r[1].get("error").is_none());
    assert!(r[1]["ms"].is_u64());
    assert_eq!(
        (r[2]["from"].as_str(), r[2]["to"].as_str()),
        (Some("connecting"), Some("connected"))
    );
    assert!(r[2].get("error").is_none());
    // Stopped by this side: no transport to ask, so only the reason and how long it was up.
    assert_eq!(r[3]["reason"], "local");
    assert!(r[3]["connected_ms"].is_u64());
    assert!(r[3].get("transport_exit").is_none() && r[3].get("stderr").is_none());
    assert_eq!(
        (r[4]["from"].as_str(), r[4]["to"].as_str()),
        (Some("connected"), Some("stopped"))
    );
}

#[tokio::test]
async fn attempt_numbers_count_every_attempt_and_are_never_reset() {
    dir();
    let seen = Arc::default();
    let sc = script(vec![
        Err(ConnectError::Io("refused".into())),
        Err(ConnectError::Io("refused".into())),
        Ok(()),
    ]);
    let (opts, mut states) = States::options("t:numbers", slow(), None);
    let sup = Supervised::spawn(scripted(daemon("peer"), sc, Arc::clone(&seen)), opts);
    states
        .until(|s| matches!(s, LinkState::Reconnecting { attempt: 1, .. }))
        .await;
    sup.reconnect_now();
    states
        .until(|s| matches!(s, LinkState::Reconnecting { attempt: 2, .. }))
        .await;
    sup.reconnect_now();
    states.until(|s| s.is_connected()).await;
    // A drop and a reconnect after a success: the backoff counter starts again, the attempt
    // number does not.
    sup.reconnect_now();
    states.until(|s| matches!(s, LinkState::Connecting)).await;
    states.until(|s| s.is_connected()).await;
    sup.stop().await;

    assert_eq!(*seen.lock().unwrap(), [1, 2, 3, 4]);
    let r = of_link("t:numbers");
    let ends: Vec<(u64, &str)> = find(&r, "link.attempt_end")
        .iter()
        .map(|v| (v["n"].as_u64().unwrap(), v["outcome"].as_str().unwrap()))
        .collect();
    assert_eq!(
        ends,
        [
            (1, "backoff"),
            (2, "backoff"),
            (3, "connected"),
            (4, "connected")
        ]
    );
    let begins: Vec<u64> = find(&r, "link.attempt_begin")
        .iter()
        .map(|v| v["n"].as_u64().unwrap())
        .collect();
    assert_eq!(begins, [1, 2, 3, 4]);
    // The backoff counter on the states is the supervisor's own and does restart.
    let attempts: Vec<u64> = find(&r, "link.state")
        .iter()
        .filter(|v| v["to"] == "reconnecting")
        .map(|v| v["attempt"].as_u64().unwrap())
        .collect();
    assert_eq!(attempts, [1, 2]);
    // A closed link is recorded as closed, however it ended.
    assert_eq!(find(&r, "link.closed").len(), 2);
}

/// What each classified failure turns the link into, and what is recorded of it.
#[tokio::test]
async fn every_classified_failure_is_recorded_with_its_outcome_state_and_code() {
    dir();
    struct Case {
        name: &'static str,
        err: ConnectError,
        outcome: &'static str,
        to: &'static str,
        code: &'static str,
    }
    let case = |name, err, outcome, to, code| Case {
        name,
        err,
        outcome,
        to,
        code,
    };
    let cases = vec![
        case(
            "auth",
            command(
                CommandFailureKind::AuthNeeded,
                "Permission denied (publickey).",
            ),
            "park",
            "auth_needed",
            "auth_needed",
        ),
        case(
            "hostkey_new",
            command(
                CommandFailureKind::HostKeyUnknown,
                "Host key verification failed.",
            ),
            "park",
            "auth_needed",
            "host_key_unknown",
        ),
        case(
            "hostkey_changed",
            command(
                CommandFailureKind::HostKeyChanged,
                "REMOTE HOST IDENTIFICATION HAS CHANGED",
            ),
            "park",
            "auth_needed",
            "host_key_changed",
        ),
        case(
            "distro",
            command(
                CommandFailureKind::UnknownDistro,
                "There is no distribution with the supplied name.",
            ),
            "park",
            "failed",
            "unknown_distro",
        ),
        case(
            "denied",
            command(
                CommandFailureKind::PermissionDenied,
                "sh: Permission denied",
            ),
            "park",
            "failed",
            "permission_denied",
        ),
        case(
            "unreachable",
            command(
                CommandFailureKind::Unreachable,
                "ssh: connect to host h port 22: Connection refused",
            ),
            "backoff",
            "reconnecting",
            "unreachable",
        ),
        case(
            "other",
            command(CommandFailureKind::Other, "something broke"),
            "backoff",
            "reconnecting",
            "other",
        ),
        case(
            "not_found",
            command(
                CommandFailureKind::CommandNotFound,
                "marsad-agent: command not found",
            ),
            "provision",
            "failed",
            "command_not_found",
        ),
        case(
            "incompatible",
            ConnectError::Incompatible {
                ours: ProtocolVersion { major: 1, minor: 0 },
                theirs: ProtocolVersion { major: 9, minor: 0 },
            },
            "provision",
            "incompatible",
            "incompatible",
        ),
        case(
            "spawn",
            ConnectError::Spawn("ssh: No such file or directory".into()),
            "park",
            "failed",
            "spawn",
        ),
        case(
            "io",
            ConnectError::Io("Broken pipe (os error 32)".into()),
            "backoff",
            "reconnecting",
            "io",
        ),
        case(
            "protocol",
            ConnectError::Protocol("expected value at line 1 column 1".into()),
            "backoff",
            "reconnecting",
            "protocol",
        ),
        case(
            "hello_timeout",
            ConnectError::HandshakeTimeout,
            "backoff",
            "reconnecting",
            "handshake_timeout",
        ),
        case(
            "closed",
            ConnectError::ClosedDuringHandshake,
            "backoff",
            "reconnecting",
            "closed_during_handshake",
        ),
        case(
            "refused",
            ConnectError::Refused(RpcError::new(codes::SHUTTING_DOWN, "server shutting down")),
            "backoff",
            "reconnecting",
            "refused:shutting_down",
        ),
    ];
    for c in cases {
        let label = format!("t:fail:{}", c.name);
        let err = c.err.clone();
        let (opts, mut states) = States::options(&label, slow(), None);
        let sup = Supervised::spawn(
            connector_with_attempt(move |_| {
                let err = err.clone();
                async move { Err::<ControlClient, _>(err) }
            }),
            opts,
        );
        let state = states.settled().await;
        sup.stop().await;
        assert_eq!(state.code(), c.to, "{}", c.name);

        let r = of_link(&label);
        let end = one(&r, "link.attempt_end");
        assert_eq!(end["outcome"], c.outcome, "{}", c.name);
        assert_eq!(end["error"]["code"], c.code, "{}", c.name);
        let st = find(&r, "link.state");
        assert_eq!(st[0]["from"], "connecting", "{}", c.name);
        assert_eq!(st[0]["to"], c.to, "{}", c.name);
        // The text that goes with the state is there for the failures that have one.
        assert!(st[0]["error"].is_string(), "{}: {st:#?}", c.name);
    }
}

#[tokio::test]
async fn a_connect_that_does_not_finish_is_recorded_as_a_timeout() {
    dir();
    let config = SupervisedConfig {
        connect_timeout: Duration::from_millis(30),
        ..slow()
    };
    let (opts, mut states) = States::options("t:timeout", config, None);
    let sup = Supervised::spawn(
        connector_with_attempt(|_| std::future::pending::<Result<ControlClient, ConnectError>>()),
        opts,
    );
    let s = states.settled().await;
    sup.stop().await;
    assert!(matches!(s, LinkState::Reconnecting { .. }), "{s:?}");
    let r = of_link("t:timeout");
    let end = one(&r, "link.attempt_end");
    assert_eq!(end["outcome"], "backoff");
    assert_eq!(end["error"]["code"], "timeout");
    assert_eq!(end["error"]["text"], "connect timed out");
    assert!(end["ms"].as_u64().unwrap() >= 30);
}

#[tokio::test]
async fn a_parked_bridge_unavailable_names_the_attempts_but_not_the_peers_words() {
    dir();
    let config = SupervisedConfig {
        bridge_unavailable_attempts: 2,
        ..slow()
    };
    let (opts, mut states) = States::options("t:bridge", config, None);
    let sup = Supervised::spawn(
        connector_with_attempt(|_| async {
            Err::<ControlClient, _>(ConnectError::Refused(RpcError::new(
                codes::BRIDGE_UNAVAILABLE,
                "bridge cannot reach the daemon",
            )))
        }),
        opts,
    );
    states
        .until(|s| matches!(s, LinkState::Reconnecting { .. }))
        .await;
    sup.reconnect_now();
    let parked = states
        .until(|s| matches!(s, LinkState::Failed { .. }))
        .await;
    sup.stop().await;
    assert_eq!(parked.code(), "failed");
    let r = of_link("t:bridge");
    let outcomes: Vec<&str> = find(&r, "link.attempt_end")
        .iter()
        .map(|v| v["outcome"].as_str().unwrap())
        .collect();
    assert_eq!(outcomes, ["backoff", "park"]);
}

// ── provisioning ────────────────────────────────────────────────────────────

type Provision = Box<dyn Fn() -> Option<Result<(), ProvisionError>> + Send + Sync>;

struct Hooks {
    reject_builds: bool,
    provision: Provision,
    /// Never answers when set (to meet the provisioning deadline).
    hang: bool,
}

#[async_trait]
impl SupervisorHooks for Hooks {
    fn check_hello(&self, hello: &Hello) -> Option<ProvisionReason> {
        self.reject_builds.then(|| ProvisionReason::BuildMismatch {
            expected: "wanted".into(),
            found: hello.build.clone(),
        })
    }

    async fn provision(&self, _reason: &ProvisionReason) -> Option<Result<(), ProvisionError>> {
        if self.hang {
            std::future::pending::<()>().await;
        }
        (self.provision)()
    }
}

fn hooks(provision: Provision) -> Arc<dyn SupervisorHooks> {
    Arc::new(Hooks {
        reject_builds: false,
        provision,
        hang: false,
    })
}

fn not_found() -> ConnectError {
    command(
        CommandFailureKind::CommandNotFound,
        "marsad-agent: command not found",
    )
}

#[tokio::test]
async fn a_provisioning_run_that_fixes_the_host_is_recorded_and_the_attempt_follows() {
    dir();
    let sc = script(vec![Err(not_found()), Ok(())]);
    let (opts, mut states) = States::options(
        "t:prov:done",
        slow(),
        Some(hooks(Box::new(|| Some(Ok(()))))),
    );
    let sup = Supervised::spawn(scripted(daemon("peer"), sc, Arc::default()), opts);
    states.until(|s| s.is_connected()).await;
    sup.stop().await;

    let r = of_link("t:prov:done");
    let p = one(&r, "link.provision");
    assert_eq!(p["reason"], "command_not_found");
    assert_eq!(p["outcome"], "done");
    assert!(p["ms"].is_u64() && p.get("error").is_none());
    let ends: Vec<(&str, u64)> = find(&r, "link.attempt_end")
        .iter()
        .map(|v| (v["outcome"].as_str().unwrap(), v["n"].as_u64().unwrap()))
        .collect();
    assert_eq!(ends, [("provision", 1), ("connected", 2)]);
    let path: Vec<(&str, &str)> = find(&r, "link.state")
        .iter()
        .map(|v| (v["from"].as_str().unwrap(), v["to"].as_str().unwrap()))
        .collect();
    assert_eq!(
        &path[..3],
        [
            ("connecting", "provisioning"),
            ("provisioning", "connecting"),
            ("connecting", "connected")
        ]
    );
    // The first state names why provisioning was needed.
    assert!(find(&r, "link.state")[0]["error"]
        .as_str()
        .unwrap()
        .contains("command not found"));
}

#[tokio::test]
async fn every_way_provisioning_can_end_is_recorded() {
    dir();
    struct Case {
        name: &'static str,
        provision: Provision,
        hang: bool,
        outcome: &'static str,
        error: bool,
        to: &'static str,
    }
    let cases = [
        Case {
            name: "retryable",
            provision: Box::new(|| {
                Some(Err(ProvisionError {
                    message: "upload failed".into(),
                    retryable: true,
                }))
            }),
            hang: false,
            outcome: "failed_retryable",
            error: true,
            to: "reconnecting",
        },
        Case {
            name: "failed",
            provision: Box::new(|| {
                Some(Err(ProvisionError {
                    message: "no space left".into(),
                    retryable: false,
                }))
            }),
            hang: false,
            outcome: "failed",
            error: true,
            to: "failed",
        },
        Case {
            name: "unavailable",
            provision: Box::new(|| None),
            hang: false,
            outcome: "unavailable",
            error: false,
            to: "failed",
        },
        Case {
            name: "timeout",
            provision: Box::new(|| None),
            hang: true,
            outcome: "timed_out",
            error: false,
            to: "reconnecting",
        },
    ];
    for c in cases {
        let label = format!("t:prov:{}", c.name);
        let config = SupervisedConfig {
            provision_timeout: Duration::from_millis(30),
            ..slow()
        };
        let hooks: Arc<dyn SupervisorHooks> = Arc::new(Hooks {
            reject_builds: false,
            provision: c.provision,
            hang: c.hang,
        });
        let (opts, mut states) = States::options(&label, config, Some(hooks));
        let sup = Supervised::spawn(
            connector_with_attempt(|_| async { Err::<ControlClient, _>(not_found()) }),
            opts,
        );
        let end = states
            .until(|s| !matches!(s, LinkState::Connecting | LinkState::Provisioning { .. }))
            .await;
        sup.stop().await;
        assert_eq!(end.code(), c.to, "{}", c.name);
        let r = of_link(&label);
        let p = one(&r, "link.provision");
        assert_eq!(p["outcome"], c.outcome, "{}", c.name);
        assert_eq!(
            p["error"]["code"] == "provision_failed",
            c.error,
            "{}",
            c.name
        );
    }
}

#[tokio::test]
async fn a_hello_that_needs_provisioning_and_one_that_still_fails_after_it_are_recorded() {
    dir();
    // The peer answers but runs another build; the provisioner "fixes" it, the peer still
    // answers with the same build: the link parks instead of looping.
    let hooks: Arc<dyn SupervisorHooks> = Arc::new(Hooks {
        reject_builds: true,
        provision: Box::new(|| Some(Ok(()))),
        hang: false,
    });
    let (opts, mut states) = States::options("t:prov:still", slow(), Some(hooks));
    let sup = Supervised::spawn(
        scripted(daemon("peer"), script(vec![]), Arc::default()),
        opts,
    );
    let parked = states
        .until(|s| matches!(s, LinkState::Failed { .. }))
        .await;
    sup.stop().await;
    assert!(
        matches!(&parked, LinkState::Failed { reason, .. } if reason.contains("still failing"))
    );
    let r = of_link("t:prov:still");
    // One provisioning run only; the second rejection parks without running another.
    assert_eq!(one(&r, "link.provision")["reason"], "build_mismatch");
    let ends: Vec<&str> = find(&r, "link.attempt_end")
        .iter()
        .map(|v| v["outcome"].as_str().unwrap())
        .collect();
    assert_eq!(ends, ["provision", "provision"]);
    let parked = find(&r, "link.state")
        .into_iter()
        .find(|v| v["to"] == "failed")
        .expect("the link parks");
    assert!(parked["error"]
        .as_str()
        .unwrap()
        .contains("still failing after provisioning"));
}

// ── a link that ends ────────────────────────────────────────────────────────

#[tokio::test]
async fn a_link_closed_by_the_peer_records_why_and_for_how_long_without_the_peers_words() {
    dir();
    let server = daemon("peer");
    let (opts, mut states) = States::options("t:goodbye", slow(), None);
    let sup = Supervised::spawn(
        scripted(server.clone(), script(vec![]), Arc::default()),
        opts,
    );
    states.until(|s| s.is_connected()).await;
    // The peer says goodbye with a message of its own.
    let conn = server.connections().pop().expect("one connection");
    conn.goodbye(RpcError::new(codes::SHUTTING_DOWN, "SECRET-PEER-WORDS"));
    let s = states
        .until(|s| matches!(s, LinkState::Reconnecting { .. }))
        .await;
    // The state the UI shows is unchanged by diagnostics: it still carries the message.
    assert!(
        matches!(&s, LinkState::Reconnecting { last_error, .. } if last_error.contains("SECRET-PEER-WORDS"))
    );
    sup.stop().await;

    let r = of_link("t:goodbye");
    let closed = one(&r, "link.closed");
    assert_eq!(closed["reason"], "goodbye");
    assert!(closed["connected_ms"].is_u64());
    assert_eq!(closed["lvl"], "warn");
    let st = find(&r, "link.state");
    let re = st.iter().find(|v| v["to"] == "reconnecting").unwrap();
    assert_eq!(re["error"], "peer said goodbye: shutting_down");
    assert!(!serde_json::to_string(&r)
        .unwrap()
        .contains("SECRET-PEER-WORDS"));

    // And a peer that just closes: EOF.
    let server = daemon("peer2");
    let (opts, mut states) = States::options("t:eof", slow(), None);
    let sup = Supervised::spawn(
        scripted(server.clone(), script(vec![]), Arc::default()),
        opts,
    );
    states.until(|s| s.is_connected()).await;
    server.connections().pop().unwrap().close();
    states
        .until(|s| matches!(s, LinkState::Reconnecting { .. }))
        .await;
    sup.stop().await;
    let r = of_link("t:eof");
    assert_eq!(one(&r, "link.closed")["reason"], "eof");
}

// ── real processes ──────────────────────────────────────────────────────────

fn hub(host: &str) -> Hub {
    let mut c = HubConfig::new(Hello::new("b1", host, "hub"));
    c.supervise = slow();
    Hub::new(c, Arc::new(wasit_control::CommandConnector), None)
}

/// Waits until `host`'s link is in a state `pred` accepts (a state stays published until the
/// link leaves it, so there is no race with a transition that already happened).
async fn link_state(hub: &Hub, host: &str, pred: impl Fn(&LinkState) -> bool) -> LinkState {
    let mut rx = hub.states();
    let id = HostId::new(host);
    wait_watch(&mut rx, |m| m.get(&id).is_some_and(&pred)).await[&id].clone()
}

#[tokio::test]
async fn a_transport_that_exits_with_stderr_is_recorded_with_its_exit_and_scrubbed_text() {
    dir();
    let host = "diag-proc-fail";
    let h = hub("diag-hub-1");
    h.add_link(HostSpec::new(
        host,
        [
            demo_bin(),
            "fail",
            &format!("kex: {SECRET} refused by remote-secret-argument"),
            "--code",
            "3",
        ],
    ))
    .unwrap();
    let state = link_state(&h, host, |s| matches!(s, LinkState::Reconnecting { .. })).await;
    h.shutdown().await;
    assert!(
        matches!(&state, LinkState::Reconnecting { last_error, .. } if last_error.contains(SECRET))
    );

    let r = of_link(&format!("hosts:{host}"));
    assert_eq!(
        evs(&r)[..5],
        [
            "link.attempt_begin",
            "transport.spawned",
            "transport.exited",
            "link.attempt_end",
            "link.state"
        ],
        "{r:#?}"
    );
    let spawned = one(&r, "transport.spawned");
    // The program's file name and the argument count; no argument.
    assert_eq!(spawned["program"], "wasit-control-demo");
    assert_eq!(spawned["argc"], 5);
    assert_eq!(spawned["n"], 1);
    assert!(spawned["pid"].as_u64().unwrap() > 0);
    let text = serde_json::to_string(&r).unwrap();
    for arg in ["kex", "remote-secret-argument", "--code"] {
        assert!(
            !serde_json::to_string(spawned).unwrap().contains(arg),
            "an argument reached transport.spawned: {spawned}"
        );
    }
    let exited = one(&r, "transport.exited");
    assert_eq!(exited["code"], 3);
    assert_eq!(exited["n"], 1);
    assert!(exited["stderr_bytes"].as_u64().unwrap() > 0);
    assert!(exited.get("signal").is_none());
    let end = one(&r, "link.attempt_end");
    assert_eq!(
        (end["outcome"].as_str(), end["error"]["code"].as_str()),
        (Some("backoff"), Some("other"))
    );
    // The state's text carries the exit and the stderr, with the registered secret masked.
    let st = find(&r, "link.state")[0];
    let t = st["error"].as_str().unwrap();
    assert!(
        t.contains("exit Some(3)") && t.contains("refused by remote-secret-argument"),
        "{t}"
    );
    assert!(t.contains("••••"), "{t}");
    assert!(
        !text.contains(SECRET),
        "the secret reached a record: {text}"
    );
}

#[tokio::test]
async fn an_established_transport_that_dies_records_its_exit_and_scrubbed_stderr_tail() {
    dir();
    let host = "diag-proc-crash";
    let h = hub("diag-hub-2");
    h.add_link(HostSpec::new(
        host,
        [demo_bin(), "serve-stdio", "--host", host],
    ))
    .unwrap();
    link_state(&h, host, LinkState::is_connected).await;
    let client = h.link(&HostId::new(host)).unwrap().current().unwrap();
    // The child says something with the secret in it and dies with code 3.
    let e = client
        .request::<Value>(
            "crash",
            json!({"stderr": format!("fatal: {SECRET} is not allowed")}),
            D,
        )
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::ConnectionLost(_)), "{e:?}");
    link_state(&h, host, |s| matches!(s, LinkState::Reconnecting { .. })).await;
    h.shutdown().await;

    let r = of_link(&format!("hosts:{host}"));
    let closed = one(&r, "link.closed");
    assert_eq!(closed["reason"], "eof");
    assert_eq!(closed["transport_exit"], 3);
    assert!(closed["connected_ms"].is_u64());
    let stderr = closed["stderr"].as_str().unwrap();
    assert!(
        stderr.contains("is not allowed") && stderr.contains("••••"),
        "{stderr}"
    );
    let exited = one(&r, "transport.exited");
    assert_eq!(exited["code"], 3);
    assert!(exited["stderr_bytes"].as_u64().unwrap() > 0);
    no_secret(&r);
    // The close and the exit of the same attempt carry the same link and number.
    assert_eq!(one(&r, "transport.spawned")["n"], exited["n"]);
}

const D: Duration = Duration::from_secs(10);

// ── a connector that is given the attempt number ────────────────────────────

struct Numbered(Arc<Mutex<Vec<(String, u64)>>>);

#[async_trait]
impl LinkConnector for Numbered {
    async fn connect(
        &self,
        spec: &HostSpec,
        _opts: ClientOptions,
        attempt: u64,
    ) -> Result<ControlClient, ConnectError> {
        self.0.lock().unwrap().push((spec.id.to_string(), attempt));
        Err(ConnectError::Io("refused".into()))
    }
}

#[tokio::test]
async fn the_hub_hands_each_connector_the_links_attempt_number_and_labels_its_links() {
    dir();
    let seen = Arc::default();
    let mut c = HubConfig::new(Hello::new("b1", "diag-hub-3", "hub"));
    c.supervise = slow();
    let h = Hub::new(c, Arc::new(Numbered(Arc::clone(&seen))), None);
    let host = "diag-numbered";
    h.add_link(HostSpec::new(host, ["unused"])).unwrap();
    link_state(&h, host, |s| {
        matches!(s, LinkState::Reconnecting { attempt: 1, .. })
    })
    .await;
    h.reconnect_now(&HostId::new(host));
    link_state(&h, host, |s| {
        matches!(s, LinkState::Reconnecting { attempt: 2, .. })
    })
    .await;
    h.shutdown().await;
    assert_eq!(
        *seen.lock().unwrap(),
        [(host.to_string(), 1), (host.to_string(), 2)]
    );
    let r = of_link(&format!("hosts:{host}"));
    assert_eq!(find(&r, "link.attempt_begin").len(), 2);
    assert_eq!(h.spec(&HostId::new(host)), None);
}

// ── the accepting side ──────────────────────────────────────────────────────

/// Connects `opts` to `server` over memory; returns the client and the handle that resolves
/// when the server has finished with the connection (its records are written by then).
async fn connect_joined(
    server: &ControlServer,
    opts: ClientOptions,
) -> (
    ControlClient,
    tokio::task::JoinHandle<Result<wasit_control::ConnClose, ConnectError>>,
) {
    let (a, b) = wasit_control::memory_pair(64 * 1024);
    let join = server.serve_connection(b);
    (ControlClient::connect(a, opts).await.unwrap(), join)
}

#[tokio::test]
async fn an_accepted_connection_is_recorded_when_it_starts_and_when_it_ends() {
    dir();
    let server = daemon("srv-accept");
    let (client, join) = connect_joined(
        &server,
        ClientOptions::new(
            Hello::new("0.4.2+abc", "peer-accept", "role-accept").with_machine("m1"),
        ),
    )
    .await;
    client.close();
    // The server's side ends when the client's does.
    assert_eq!(
        within(join).await.unwrap().unwrap(),
        wasit_control::ConnClose::Eof
    );

    let all = all();
    let acc: Vec<&Value> = all
        .iter()
        .filter(|v| v["peer_host"] == "peer-accept")
        .collect();
    assert_eq!(acc.len(), 1);
    assert_eq!(acc[0]["ev"], "conn.accepted");
    assert_eq!(acc[0]["role"], "role-accept");
    assert_eq!(acc[0]["peer_build"], "0.4.2+abc");
    assert_eq!(acc[0]["machine"], "m1");
    let closed: Vec<&Value> = all
        .iter()
        .filter(|v| v["ev"] == "conn.closed" && v["role"] == "role-accept")
        .collect();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0]["reason"], "eof");
    assert!(closed[0]["ms"].is_u64());
}

#[tokio::test]
async fn connections_closed_by_a_server_shutdown_are_not_recorded_one_by_one() {
    dir();
    let server = daemon("srv-shut");
    let (client, join) = connect_joined(
        &server,
        ClientOptions::new(Hello::new("b1", "peer-shut", "role-shut")),
    )
    .await;
    server.shutdown(D).await;
    within(join).await.unwrap().unwrap();
    within(client.closed()).await;
    let all = all();
    assert_eq!(
        all.iter().filter(|v| v["peer_host"] == "peer-shut").count(),
        1,
        "accepted is recorded"
    );
    assert!(all
        .iter()
        .all(|v| !(v["ev"] == "conn.closed" && v["role"] == "role-shut")));
}

#[tokio::test]
async fn a_failed_handshake_is_recorded_with_its_code_and_never_the_peers_words() {
    dir();
    let before = find_handshakes();
    // 1. The peer sends something that is not a frame.
    let server = daemon("srv-hs");
    let (a, b) = wasit_control::memory_pair(64 * 1024);
    let join = server.serve_connection(b);
    {
        use tokio::io::AsyncWriteExt;
        let mut a = a;
        a.write_all(format!("{SECRET} is not json\n").as_bytes())
            .await
            .unwrap();
        a.flush().await.unwrap();
        let res = within(join).await.unwrap();
        assert!(matches!(res, Err(ConnectError::Protocol(_))), "{res:?}");
    }
    // 2. The peer closes before it says hello.
    let (mut a, b) = wasit_control::memory_pair(64 * 1024);
    let join = server.serve_connection(b);
    {
        // Let the server's hello arrive first: it has written it, so what it then sees is a
        // clean end of the stream.
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1];
        within(a.read_exact(&mut buf)).await.unwrap();
    }
    drop(a);
    let res = within(join).await.unwrap();
    assert_eq!(res.unwrap_err(), ConnectError::ClosedDuringHandshake);
    // 3. The peer stays silent past the hello timeout.
    let quick = ControlServer::new(
        Arc::new(DemoService::new("srv-hs2")),
        Hello::new("b1", "srv-hs2", "daemon"),
        ConnConfig {
            hello_timeout: Duration::from_millis(50),
            ..cfg()
        },
    );
    let (a, b) = wasit_control::memory_pair(64 * 1024);
    let join = quick.serve_connection(b);
    let res = within(join).await.unwrap();
    assert_eq!(res.unwrap_err(), ConnectError::HandshakeTimeout);
    drop(a);

    let now = find_handshakes();
    let new: Vec<&Value> = now.iter().skip(before.len()).collect();
    let codes: Vec<&str> = new
        .iter()
        .map(|v| v["error"]["code"].as_str().unwrap())
        .collect();
    assert_eq!(
        codes,
        ["protocol", "closed_during_handshake", "handshake_timeout"],
        "{new:#?}"
    );
    assert!(new.iter().all(|v| v["lvl"] == "warn"));
    // The malformed line's own text is not kept, only its size and shape.
    assert!(new[0]["error"]["len"].is_u64() && new[0]["error"]["fp"].is_string());
    assert!(new[0]["error"].get("text").is_none());
    assert_eq!(
        new[1]["error"]["text"],
        "connection closed during handshake"
    );
    no_secret(&all());
}

fn find_handshakes() -> Vec<Value> {
    all()
        .into_iter()
        .filter(|v| v["ev"] == "conn.handshake_failed")
        .collect()
}

#[tokio::test]
async fn an_in_process_server_records_no_accept_or_close() {
    dir();
    let server = ControlServer::in_process(
        Arc::new(DemoService::new("srv-loop")),
        Hello::new("b1", "srv-loop", "hub"),
        cfg(),
    );
    let (client, join) = connect_joined(
        &server,
        ClientOptions::new(Hello::new("b1", "peer-loop", "inproc-role")),
    )
    .await;
    client.close();
    assert_eq!(
        within(join).await.unwrap().unwrap(),
        wasit_control::ConnClose::Eof
    );
    assert!(all()
        .iter()
        .all(|v| v["role"] != "inproc-role" && v["peer_host"] != "peer-loop"));
}

// ── routing and subscriptions ───────────────────────────────────────────────

/// A hub with a local demo service and a link to each host in `hosts` (connected over memory).
struct Routed {
    hub: Hub,
    gui: ControlClient,
    servers: Vec<ControlServer>,
}

struct Mem(Mutex<Vec<(HostId, ControlServer)>>);

#[async_trait]
impl LinkConnector for Mem {
    async fn connect(
        &self,
        spec: &HostSpec,
        opts: ClientOptions,
        _attempt: u64,
    ) -> Result<ControlClient, ConnectError> {
        let server = self
            .0
            .lock()
            .unwrap()
            .iter()
            .find(|(h, _)| *h == spec.id)
            .map(|(_, s)| s.clone());
        match server {
            Some(s) => mem_connect(&s, opts).await,
            None => Err(ConnectError::Io("no route".into())),
        }
    }
}

async fn routed(hub_id: &str, hosts: &[(&str, Arc<dyn Service>)]) -> Routed {
    let servers: Vec<(HostId, ControlServer)> = hosts
        .iter()
        .map(|(h, svc)| {
            (
                HostId::new(*h),
                ControlServer::new(svc.clone(), Hello::new("b1", *h, "daemon"), cfg()),
            )
        })
        .collect();
    let mut c = HubConfig::new(Hello::new("b1", hub_id, "hub"));
    c.supervise = slow();
    let hub = Hub::new(c, Arc::new(Mem(Mutex::new(servers.clone()))), None);
    let routing: Arc<dyn Service> = Arc::new(RoutingService::new(
        hub.clone(),
        Arc::new(DemoService::new(hub_id)),
    ));
    hub.set_inbound_service(routing.clone());
    let front = ControlServer::new(routing, Hello::new("b1", hub_id, "hub"), cfg());
    let mut st = hub.states();
    for (h, _) in &servers {
        hub.add_link(HostSpec::new(h.clone(), ["unused"])).unwrap();
    }
    wait_watch(&mut st, |m| {
        m.len() == servers.len() && m.values().all(LinkState::is_connected)
    })
    .await;
    let gui = mem_connect(&front, ClientOptions::new(hello("gui")))
        .await
        .unwrap();
    Routed {
        hub,
        gui,
        servers: servers.into_iter().map(|(_, s)| s).collect(),
    }
}

fn to(host: &str) -> RequestOpts {
    RequestOpts::new(D).host(host)
}

fn forward_failures(host: &str) -> Vec<Value> {
    all()
        .into_iter()
        .filter(|v| v["ev"] == "rpc.forward_failed" && v["host"] == host)
        .collect()
}

#[tokio::test]
async fn a_request_that_cannot_be_forwarded_is_recorded_and_a_remote_error_is_not() {
    dir();
    let r = routed("hub-fwd", &[("fwd-a", Arc::new(DemoService::new("fwd-a")))]).await;

    // An unknown host and a missing link are routing failures.
    let e = r
        .gui
        .request_with::<Value>("whoami", Value::Null, to("fwd-ghost"))
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref x) if x.code == codes::HOST_UNKNOWN));
    let rec = forward_failures("fwd-ghost");
    assert_eq!(rec.len(), 1);
    assert_eq!(
        (rec[0]["method"].as_str(), rec[0]["code"].as_str()),
        (Some("whoami"), Some("host_unknown"))
    );
    assert!(rec[0]["ms"].is_u64());

    // The remote service answers with its own error: the forwarding worked.
    let e = r
        .gui
        .request_with::<Value>("fail", Value::Null, to("fwd-a"))
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref x) if x.code == "demo_failure"));
    assert!(forward_failures("fwd-a").is_empty());

    // A subscription for a host with no link is recorded under its topic.
    let mut opts = SubscribeOpts::new(D);
    opts.host = Some("fwd-ghost".into());
    r.gui.subscribe_with("count", opts).await.unwrap_err();
    let rec = forward_failures("fwd-ghost");
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1]["method"], "subscribe:count");

    // The link to a host drops: its requests are now `host_unreachable`.
    let mut st = r.hub.states();
    r.servers[0].connections().pop().unwrap().close();
    wait_watch(&mut st, |m| !m.values().all(LinkState::is_connected)).await;
    let e = r
        .gui
        .request_with::<Value>("whoami", Value::Null, to("fwd-a"))
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref x) if x.code == codes::HOST_UNREACHABLE));
    let rec = forward_failures("fwd-a");
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0]["code"], "host_unreachable");
    r.hub.shutdown().await;
}

/// A service whose `ends` topic yields one event and then fails with `error`.
struct Failing {
    error: RpcError,
}

#[async_trait]
impl Service for Failing {
    async fn call(
        &self,
        _ctx: wasit_control::CallContext,
        m: &str,
        _p: Value,
    ) -> Result<Value, RpcError> {
        Err(RpcError::method_not_found(m))
    }

    async fn subscribe(
        &self,
        _ctx: wasit_control::CallContext,
        _topic: &str,
        _params: Value,
        _from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        use futures::StreamExt;
        let error = self.error.clone();
        Ok(futures::stream::iter(vec![
            Ok((Cursor::from_u64(1), json!(1))),
            Err(CloseReason::Error { error }),
        ])
        .boxed())
    }
}

fn sub_closed(topic: &str) -> Vec<Value> {
    all()
        .into_iter()
        .filter(|v| v["ev"] == "sub.closed" && v["topic"] == topic)
        .collect()
}

#[tokio::test]
async fn a_subscription_that_ends_abnormally_is_recorded_with_its_reason() {
    dir();
    // The service fails the subscription with an error code and a message of its own.
    let svc = Arc::new(Failing {
        error: RpcError::new("boom_code", "SECRET-SERVICE-WORDS"),
    });
    let (client, _server) = mem_pair(svc, "sub-err", cfg(), cfg()).await;
    let mut sub = client.subscribe("errors.topic:1", None).await.unwrap();
    assert!(within(sub.next()).await.is_some());
    assert!(within(sub.next()).await.is_none());
    let rec = sub_closed("errors.topic");
    assert_eq!(rec.len(), 1);
    assert_eq!(
        (rec[0]["reason"].as_str(), rec[0]["code"].as_str()),
        (Some("error"), Some("boom_code"))
    );
    assert!(rec[0].get("host").is_none());
    assert!(!serde_json::to_string(&rec).unwrap().contains("SECRET"));

    // A subscriber that stops reading falls behind: the server closes it as lagged.
    let svc = Arc::new(DemoService::new("sub-lag"));
    let stats = svc.stats.clone();
    let server_cfg = ConnConfig {
        lag_timeout: Duration::from_millis(50),
        ..cfg()
    };
    // The window a subscriber announces is its own queue size: four events, then no credit.
    let client_cfg = ConnConfig {
        subscription_queue: 4,
        ..cfg()
    };
    let (client, _server) = mem_pair(svc, "sub-lag", server_cfg, client_cfg).await;
    let mut dropped = stats.streams_dropped.subscribe();
    let sub = client.subscribe("count", None).await.unwrap();
    // The subscription is never read: the window fills and stays full.
    wait_watch(&mut dropped, |n| *n >= 1).await;
    drop(sub);
    let rec = sub_closed("count");
    assert!(rec.iter().any(|v| v["reason"] == "lagged"), "{rec:#?}");
}

#[tokio::test]
async fn a_forwarded_subscription_whose_host_is_lost_is_recorded_with_the_host() {
    dir();
    let r = routed(
        "hub-sub",
        &[("sub-host", Arc::new(DemoService::new("sub-host")))],
    )
    .await;
    let mut opts = SubscribeOpts::new(D);
    opts.host = Some("sub-host".into());
    let mut sub = r.gui.subscribe_with("count", opts).await.unwrap();
    assert!(within(sub.next()).await.is_some());
    // The host goes away under the subscription.
    r.servers[0].connections().pop().unwrap().close();
    while within(sub.next()).await.is_some() {}
    assert_eq!(sub.close_reason(), Some(&CloseReason::HostLost));
    let rec: Vec<Value> = sub_closed("count")
        .into_iter()
        .filter(|v| v["host"] == "sub-host")
        .collect();
    assert_eq!(rec.len(), 1, "{rec:#?}");
    assert_eq!(rec[0]["reason"], "host_lost");
    r.hub.shutdown().await;
}

// ── a link without a label records nothing ──────────────────────────────────

#[tokio::test]
async fn a_link_without_a_label_records_nothing() {
    dir();
    let sup = Supervised::spawn(
        connector_with_attempt(|_| async {
            Err::<ControlClient, _>(ConnectError::Io("UNLABELLED-MARKER".into()))
        }),
        SupervisedOptions {
            config: slow(),
            ..SupervisedOptions::default()
        },
    );
    let mut st = sup.state();
    wait_watch(&mut st, |s| matches!(s, LinkState::Reconnecting { .. })).await;
    sup.stop().await;
    // Every record this link would have made (attempt, error text, state text) would carry
    // the marker.
    let text = serde_json::to_string(&all()).unwrap();
    assert!(!text.contains("UNLABELLED-MARKER"));
}

// ── traced requests ─────────────────────────────────────────────────────────

fn traced_for(host: &str) -> Vec<Value> {
    all()
        .into_iter()
        .filter(|v| v["ev"] == "rpc.traced" && v["host"] == host)
        .collect()
}

#[tokio::test]
async fn a_traced_request_is_recorded_by_every_hop_that_serves_it_and_an_untraced_one_is_not() {
    dir();
    let r = routed("hub-trc", &[("trc-a", Arc::new(DemoService::new("trc-a")))]).await;

    // Untraced requests, answered or failed, leave no record at all.
    r.gui
        .request_with::<Value>("whoami", Value::Null, to("trc-a"))
        .await
        .unwrap();
    r.gui
        .request_with::<Value>("fail", Value::Null, to("trc-a"))
        .await
        .unwrap_err();
    assert!(traced_for("trc-a").is_empty(), "{:#?}", traced_for("trc-a"));

    // A traced one is recorded where the hub served it (it forwarded it) and where the host did.
    let t = TraceId::new("u-trc00000000000a1").unwrap();
    let opts = to("trc-a").trace(Some(t.clone()));
    r.gui
        .request_with::<Value>("whoami", Value::Null, opts.clone())
        .await
        .unwrap();
    let rec = traced_for("trc-a");
    assert_eq!(rec.len(), 2, "{rec:#?}");
    let (hub_side, host_side) = (
        rec.iter()
            .find(|v| v["hops"] == 0)
            .expect("the hub's record"),
        rec.iter()
            .find(|v| v["hops"] == 1)
            .expect("the host's record"),
    );
    for v in [hub_side, host_side] {
        assert_eq!(v["trace"], "u-trc00000000000a1");
        assert_eq!(v["method"], "whoami");
        assert_eq!(v["outcome"], "ok");
        assert!(v["ms"].is_u64());
        assert!(v.get("code").is_none());
    }

    // An error is recorded with its code and never its message.
    let before = traced_for("trc-a").len();
    r.gui
        .request_with::<Value>("fail", Value::Null, opts)
        .await
        .unwrap_err();
    let rec = traced_for("trc-a");
    assert_eq!(rec.len(), before + 2, "{rec:#?}");
    let failed: Vec<&Value> = rec.iter().filter(|v| v["method"] == "fail").collect();
    assert_eq!(failed.len(), 2);
    for v in &failed {
        assert_eq!(
            (v["outcome"].as_str(), v["code"].as_str()),
            (Some("error"), Some("demo_failure"))
        );
    }
    assert!(!serde_json::to_string(&rec)
        .unwrap()
        .contains("asked to fail"));

    // A request the hub itself refuses is recorded too: the hub is the serving side.
    let t2 = TraceId::new("u-trc00000000000b2").unwrap();
    r.gui
        .request_with::<Value>("whoami", Value::Null, to("trc-ghost").trace(Some(t2)))
        .await
        .unwrap_err();
    let rec = traced_for("trc-ghost");
    assert_eq!(rec.len(), 1, "{rec:#?}");
    assert_eq!(
        (
            rec[0]["outcome"].as_str(),
            rec[0]["code"].as_str(),
            rec[0]["hops"].as_u64()
        ),
        (Some("error"), Some("host_unknown"), Some(0))
    );
    r.hub.shutdown().await;
}

/// A service with `trace` (answers with the trace id it got) and `hold` (never answers; says
/// when it started and when its request was dropped).
struct Hold {
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
}

struct OnDrop(Arc<tokio::sync::Notify>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait]
impl Service for Hold {
    async fn call(
        &self,
        ctx: wasit_control::CallContext,
        m: &str,
        _p: Value,
    ) -> Result<Value, RpcError> {
        match m {
            "trace" => Ok(json!(ctx.trace)),
            "hold" => {
                let _dropped = OnDrop(self.dropped.clone());
                self.started.notify_one();
                std::future::pending::<()>().await;
                Ok(Value::Null)
            }
            other => Err(RpcError::method_not_found(other)),
        }
    }
}

fn of_trace(trace: &str) -> Vec<Value> {
    all()
        .into_iter()
        .filter(|v| v["ev"] == "rpc.traced" && v["trace"] == trace)
        .collect()
}

/// Runs on the default current-thread runtime: the serving task drops the request and records it
/// in one go, with no await between, so when the test is woken by the drop the record exists.
#[tokio::test]
async fn a_traced_request_without_a_host_has_none_and_a_dropped_connection_is_recorded() {
    dir();
    let (started, dropped) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let svc = Arc::new(Hold {
        started: started.clone(),
        dropped: dropped.clone(),
    });
    let (client, server) = mem_pair(svc, "trc-local", cfg(), cfg()).await;
    let t = TraceId::new("u-trc00000000000c3").unwrap();
    let v: Option<TraceId> = client
        .request_with(
            "trace",
            Value::Null,
            RequestOpts::new(D).trace(Some(t.clone())),
        )
        .await
        .unwrap();
    assert_eq!(v, Some(t));
    let rec = of_trace("u-trc00000000000c3");
    assert_eq!(rec.len(), 1, "{rec:#?}");
    assert!(rec[0].get("host").is_none());
    assert_eq!(rec[0]["method"], "trace");

    // The connection goes away while the service works: the record says it never answered.
    let t2 = TraceId::new("u-trc00000000000d4").unwrap();
    let c2 = client.clone();
    let pending = tokio::spawn(async move {
        c2.request_with::<Value>("hold", Value::Null, RequestOpts::new(D).trace(Some(t2)))
            .await
    });
    within(started.notified()).await;
    server.connections().pop().unwrap().close();
    within(dropped.notified()).await;
    assert!(pending.await.unwrap().is_err());
    let rec = of_trace("u-trc00000000000d4");
    assert_eq!(rec.len(), 1, "{rec:#?}");
    assert_eq!(rec[0]["outcome"], "cancelled");
    assert_eq!(rec[0]["method"], "hold");
}
