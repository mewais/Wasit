//! Real child processes: the demo binary as a stdio server, failing commands, the bridge.

mod common;

use std::time::Duration;

use common::*;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use wasit_control::{
    codes, ClientOptions, CloseReason, CommandFailureKind, CommandSpec, CommandTransport,
    ConnClose, ConnectError, ProtocolVersion, SubscribeOpts,
};

const D: Duration = Duration::from_secs(10);

fn demo(args: &[&str]) -> CommandSpec {
    let mut argv = vec![demo_bin().to_string()];
    argv.extend(args.iter().map(|s| s.to_string()));
    CommandSpec::new(argv)
}

/// A command that prints `stderr` and exits with `code`, like a failing `ssh`.
fn failing(stderr: &str, code: u8) -> CommandSpec {
    demo(&["fail", stderr, "--code", &code.to_string()])
}

#[tokio::test]
async fn command_transport_speaks_to_a_stdio_server() {
    let client = CommandTransport::connect(
        &demo(&["serve-stdio", "--host", "p1", "--build", "b9"]),
        ClientOptions::new(hello("hub")),
    )
    .await
    .unwrap();
    assert_eq!(client.remote_hello().build, "b9");
    let who: String = client.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "p1");
    let mut sub = client
        .subscribe_with("count", SubscribeOpts::new(D).params(json!({"limit": 500})))
        .await
        .unwrap();
    let mut n = 0;
    while let Some((c, _)) = within(sub.next()).await {
        n += 1;
        assert_eq!(c.as_u64(), Some(n));
    }
    assert_eq!(n, 500);
    assert_eq!(sub.close_reason(), Some(&CloseReason::Ended));
}

#[tokio::test]
async fn version_mismatch_with_a_child_is_incompatible() {
    let err = CommandTransport::connect(
        &demo(&["serve-stdio", "--major", "9"]),
        ClientOptions::new(hello("hub")),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err,
        ConnectError::Incompatible {
            ours: ProtocolVersion { major: 1, minor: 0 },
            theirs: ProtocolVersion { major: 9, minor: 0 },
        }
    );
}

#[tokio::test]
async fn child_dying_mid_stream_closes_subscription_with_host_lost() {
    let client = CommandTransport::connect(
        &demo(&["serve-stdio", "--pause-after-events", "50"]),
        ClientOptions::new(hello("hub")),
    )
    .await
    .unwrap();
    let mut sub = client.subscribe("count", None).await.unwrap();
    for n in 1..=50u64 {
        let (c, _) = within(sub.next()).await.unwrap();
        assert_eq!(c.as_u64(), Some(n));
    }
    // The child dies while the subscription is open.
    let e = client
        .request::<Value>("crash", Value::Null, D)
        .await
        .unwrap_err();
    assert!(
        matches!(e, wasit_control::CallError::ConnectionLost(_)),
        "{e:?}"
    );
    assert!(within(sub.next()).await.is_none());
    assert_eq!(sub.close_reason(), Some(&CloseReason::HostLost));
    assert_eq!(within(client.closed()).await, ConnClose::Eof);
    let diag = within(client.diagnostics()).await.unwrap();
    assert!(diag.contains("exit code 3"), "{diag}");
    let e = client
        .request::<Value>("echo", json!(1), D)
        .await
        .unwrap_err();
    assert!(matches!(e, wasit_control::CallError::ConnectionLost(_)));
}

#[tokio::test]
async fn failing_transport_commands_are_classified() {
    let cases = [
        (
            "user@h: Permission denied (publickey).",
            255,
            CommandFailureKind::AuthNeeded,
        ),
        (
            "Host key verification failed.",
            255,
            CommandFailureKind::HostKeyUnknown,
        ),
        (
            "ssh: Could not resolve hostname nope: Name or service not known",
            255,
            CommandFailureKind::Unreachable,
        ),
        (
            "bash: marsad-agent: command not found",
            127,
            CommandFailureKind::CommandNotFound,
        ),
        ("", 1, CommandFailureKind::Other),
    ];
    for (stderr, code, want) in cases {
        let err =
            CommandTransport::connect(&failing(stderr, code), ClientOptions::new(hello("hub")))
                .await
                .unwrap_err();
        match err {
            ConnectError::Command { kind, .. } => assert_eq!(kind, want, "{stderr}"),
            other => panic!("{stderr}: unexpected {other:?}"),
        }
    }
    let err = CommandTransport::connect(
        &CommandSpec::new(["/nonexistent/wasit-ssh"]),
        ClientOptions::new(hello("hub")),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ConnectError::Spawn(_)), "{err:?}");
}

#[tokio::test]
async fn child_is_killed_when_its_handle_is_dropped() {
    let (mut io, child) = CommandTransport::spawn(&demo(&["serve-stdio"])).unwrap();
    // The server writes its hello first.
    let mut buf = [0u8; 1];
    within(io.read_exact(&mut buf)).await.unwrap();
    drop(child);
    // Its stdin is still open (we hold `io`), so EOF on stdout means it was killed.
    let mut rest = Vec::new();
    within(io.read_to_end(&mut rest)).await.unwrap();
}

#[tokio::test]
async fn bridge_starts_the_daemon_and_pumps_bytes() {
    let dir = scratch_dir("bridge");
    let path = dir.join("d.sock");
    let p = path.to_str().unwrap();
    let spec = demo(&["bridge", p, "--host", "bridged"]);

    // First use: no daemon yet; the bridge starts it.
    let c1 = CommandTransport::connect(&spec, ClientOptions::new(hello("hub")))
        .await
        .unwrap();
    assert_eq!(c1.remote_hello().host_id.as_str(), "bridged");
    let who: String = c1.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "bridged");
    #[cfg(unix)]
    assert!(path.exists());

    // Second bridge reuses the running daemon (it would fail to start a second one).
    let c2 = CommandTransport::connect(&spec, ClientOptions::new(hello("hub")))
        .await
        .unwrap();
    let mut sub = c2
        .subscribe_with("count", SubscribeOpts::new(D).params(json!({"limit": 20})))
        .await
        .unwrap();
    let mut n = 0;
    while within(sub.next()).await.is_some() {
        n += 1;
    }
    assert_eq!(n, 20);

    // Stop the detached daemon.
    // The response races with the shutdown goodbye; either proves the daemon is stopping.
    match c1.request::<String>("shutdown", Value::Null, D).await {
        Ok(bye) => assert_eq!(bye, "bye"),
        Err(wasit_control::CallError::ConnectionLost(ConnClose::Goodbye { reason })) => {
            assert_eq!(reason.code, codes::SHUTTING_DOWN)
        }
        Err(e) => panic!("unexpected {e:?}"),
    }
    within(c2.closed()).await;
    within(c1.closed()).await;

    // A daemon that cannot start: the bridge answers with one goodbye line.
    let bad = dir.join("missing-dir").join("d.sock");
    let err = CommandTransport::connect(
        &demo(&["bridge", bad.to_str().unwrap()]),
        ClientOptions::new(hello("hub")),
    )
    .await
    .unwrap_err();
    match err {
        ConnectError::Refused(r) => assert_eq!(r.code, codes::BRIDGE_UNAVAILABLE),
        other => panic!("unexpected {other:?}"),
    }
}
