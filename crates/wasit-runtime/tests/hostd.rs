//! End to end through the `wasit-hostd` binary and the typed `RuntimeClient` over a Unix
//! socket, including `kill -9` of the daemon and a restart with the client's supervised link
//! resuming its durable subscriptions.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::process::Stdio;

use common::*;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use wasit_control::{ClientOptions, ControlClient, Endpoint, Hello};
use wasit_core::{AgentEvent, Command, GateDecision};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::{HostEvent, SessionEvent, SessionEventKind};
use wasit_runtime::{ClientError, RuntimeClient};

fn daemon_args(data: &Path) -> Vec<String> {
    let mut v = vec![
        "--data".to_string(),
        data.to_string_lossy().into_owned(),
        "--stop-grace-ms".into(),
        "500".into(),
    ];
    for d in DRIVERS {
        v.push("--driver-bin".into());
        v.push(format!("{d}={}", fake().display()));
        v.push("--driver-env".into());
        v.push(format!("{d}=WASIT_FAKE_PERSONA={d}"));
    }
    v
}

async fn start_daemon(data: &Path) -> tokio::process::Child {
    let mut child = tokio::process::Command::new(hostd())
        .arg("serve")
        .args(daemon_args(data))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut line = String::new();
    let out = child.stdout.take().unwrap();
    tokio::time::timeout(T, BufReader::new(out).read_line(&mut line))
        .await
        .expect("daemon readiness")
        .unwrap();
    assert!(line.contains("\"ready\":true"), "{line:?}");
    child
}

fn client(data: &Path) -> RuntimeClient {
    RuntimeClient::connect_endpoint(
        Endpoint::Unix(data.join("hostd.sock")),
        Hello::new("test", "gui-test", "gui"),
    )
}

async fn next_event(sub: &mut wasit_runtime::TypedSubscription<SessionEvent>) -> SessionEvent {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok((c, e)))) => {
            assert_eq!(c.as_u64(), Some(e.cursor));
            e
        }
        other => panic!(
            "no event: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostd_end_to_end_with_daemon_kill_and_restart() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;

    // Single instance: a second daemon on the same data dir refuses to start.
    let second = tokio::process::Command::new(hostd())
        .arg("serve")
        .args(daemon_args(&data))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .unwrap();
    assert_eq!(second.code(), Some(3));

    let c = client(&data);
    let info = c.host_info().await.unwrap();
    assert!(!info.host_id.is_empty());
    assert_eq!(info.drivers.len(), 4);
    let mut host = c.subscribe_host(None);
    match tokio::time::timeout(T, host.next()).await.unwrap() {
        Some(Ok((_, HostEvent::Reset { sessions }))) => assert!(sessions.is_empty()),
        other => panic!("expected Reset, got {other:?}"),
    }

    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .tool("Bash", json!({"command": "ls"}))
                .needs_permission()
                .result_if_allowed("listed")
                .done()
                .say("finished listing"),
        )
        .build();
    let id = c.create(create(d.path(), "claude", &sc)).await.unwrap().id;
    // Typed errors survive the wire.
    let err = c
        .create(wasit_runtime::wire::CreateSession::new("nope"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Runtime(wasit_runtime::wire::RuntimeError::UnknownDriver(_))
        ),
        "{err:?}"
    );
    let err = c
        .command(
            &id,
            Command::AnswerGate {
                request_id: "nope".into(),
                decision: GateDecision::approve(),
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Command(wasit_runtime::wire::CommandError::Rejected { .. })
        ),
        "{err:?}"
    );

    let mut sub = c.subscribe_session(&id, None);
    let mut seen: Vec<SessionEvent> = Vec::new();
    loop {
        let e = next_event(&mut sub).await;
        let idle = is_phase(&e, "idle");
        seen.push(e);
        if idle {
            break;
        }
    }
    c.command(
        &id,
        Command::Prompt {
            text: "list".into(),
        },
    )
    .await
    .unwrap();
    let request_id = loop {
        let e = next_event(&mut sub).await;
        let rid = match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::GateRequested { gate },
            } => Some(gate.request_id().to_string()),
            _ => None,
        };
        seen.push(e);
        if let Some(r) = rid {
            break r;
        }
    };
    assert_eq!(sub.subscribe_count(), 1);

    // kill -9 the daemon; the session (held by its shim) keeps waiting on the gate.
    daemon.start_kill().unwrap();
    daemon.wait().await.unwrap();
    let mut daemon = start_daemon(&data).await;

    // The supervised link reconnects; the gate is answerable through the new daemon.
    c.command(
        &id,
        Command::AnswerGate {
            request_id,
            decision: GateDecision::approve(),
        },
    )
    .await
    .unwrap();
    loop {
        let e = next_event(&mut sub).await;
        let done = is_turn_completed(&e);
        seen.push(e);
        if done {
            break;
        }
    }
    assert!(
        sub.subscribe_count() >= 2,
        "durable subscription resubscribed"
    );
    assert!(assistant_text(&seen).contains("finished listing"));
    // Exactly the session's history: no gap, no duplicate across the restart.
    let page = c.history(&id, None, 2000).await.unwrap();
    assert_eq!(&page.events[..seen.len()], &seen[..]);
    let snap = c.snapshot(&id).await.unwrap();
    assert_eq!(snap.turns_completed, 1);

    // The host feed restarted with the daemon: a Reset carrying the adopted session.
    let mut saw_session = false;
    while !saw_session {
        match tokio::time::timeout(T, host.next()).await.unwrap() {
            Some(Ok((_, HostEvent::Reset { sessions }))) => {
                saw_session = sessions.iter().any(|s| s.id == id)
            }
            Some(Ok((_, HostEvent::Upsert { session }))) => saw_session = session.id == id,
            Some(Ok(_)) => {}
            other => panic!("host feed ended: {other:?}"),
        }
    }

    c.delete(&id).await.unwrap();
    assert!(c.list().await.unwrap().is_empty());
    let err = c.snapshot(&id).await.unwrap_err();
    assert!(matches!(
        err,
        ClientError::Runtime(wasit_runtime::wire::RuntimeError::SessionNotFound(_))
    ));
    daemon.start_kill().unwrap();
    daemon.wait().await.unwrap();
}

#[tokio::test]
async fn bridge_starts_the_daemon_and_pipes_the_protocol() {
    let d = TempDir::new();
    let data = d.path().join("b");
    let mut bridge = tokio::process::Command::new(hostd())
        .arg("bridge")
        .args(daemon_args(&data))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let io =
        wasit_control::join_duplex(bridge.stdout.take().unwrap(), bridge.stdin.take().unwrap());
    let c = ControlClient::connect(io, ClientOptions::new(Hello::new("t", "hub", "hub")))
        .await
        .unwrap();
    let info: wasit_runtime::wire::HostInfo = c
        .request("host.info", (), std::time::Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(info.drivers.len(), 4);
    drop(c);
    // The daemon the bridge started is detached; stop it.
    let _ = std::process::Command::new("kill")
        .arg(info.pid.to_string())
        .status();
    let _ = bridge.wait().await;
}
