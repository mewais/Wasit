//! Real child processes: the demo binary as a stdio server, failing commands, the bridge.

mod common;

use std::time::Duration;

use common::*;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
#[cfg(unix)]
use wasit_control::ControlClient;
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

// ── the end of a helper: what the transport's status means ──────────────────

/// The demo binary as `program`, failing like a transport whose remote program ended: stderr
/// `words`, then exit status `code`, with nothing on stdout (the handshake never starts).
#[cfg(unix)]
fn ends(program: &str, words: &str, code: u8) -> CommandSpec {
    CommandSpec::new([
        demo_named(program),
        "fail".into(),
        words.into(),
        "--code".into(),
        code.to_string(),
    ])
}

#[cfg(unix)]
fn command_failure(err: ConnectError) -> (CommandFailureKind, Option<i32>, String) {
    match err {
        ConnectError::Command {
            kind,
            exit_code,
            stderr,
        } => (kind, exit_code, stderr),
        other => panic!("not a command failure: {other:?}"),
    }
}

/// The remote program ended by itself (a status of 1 to 254 of an ssh command): that is not
/// the network and not the sign-in, and running the command again cannot change it.
#[cfg(unix)]
#[tokio::test]
async fn a_remote_program_that_ends_by_itself_before_the_hello_is_helper_failed_over_ssh() {
    let err = CommandTransport::connect(
        &ends("ssh", "refusing the runtime directory", 2),
        ClientOptions::new(hello("hub")),
    )
    .await
    .unwrap_err();
    let (kind, exit, text) = command_failure(err);
    assert_eq!(kind, CommandFailureKind::HelperFailed);
    assert_eq!(exit, Some(2));
    // The reason is the program's words and its status; the handshake's own error says only
    // that the stream closed.
    assert!(text.contains("refusing the runtime directory"), "{text}");
    assert!(text.contains("exit code 2"), "{text}");
    assert!(
        text.contains("connection closed during handshake"),
        "{text}"
    );

    // Without a word of stderr the status and the handshake's error are all there is.
    let err = CommandTransport::connect(&ends("ssh", "", 3), ClientOptions::new(hello("hub")))
        .await
        .unwrap_err();
    let (kind, exit, text) = command_failure(err);
    assert_eq!((kind, exit), (CommandFailureKind::HelperFailed, Some(3)));
    assert!(text.contains("exit code 3"), "{text}");
}

/// Everything else keeps the decision it had: ssh's own failure and a clean exit are retried;
/// what the words recognise keeps its kind; and a transport whose status contract is not known
/// (`wsl.exe`, any other program) never claims the status is the remote program's.
#[cfg(unix)]
#[tokio::test]
async fn only_a_status_that_is_the_remote_programs_own_is_helper_failed() {
    let cases = [
        (
            "ssh",
            "client_loop: send disconnect: Broken pipe",
            255,
            CommandFailureKind::Other,
        ),
        (
            "ssh",
            "Connection reset by peer",
            255,
            CommandFailureKind::Unreachable,
        ),
        (
            "ssh",
            "user@h: Permission denied (publickey).",
            255,
            CommandFailureKind::AuthNeeded,
        ),
        (
            "ssh",
            "sh: 1: /x/marsad-agent: Permission denied",
            126,
            CommandFailureKind::PermissionDenied,
        ),
        ("ssh", "", 127, CommandFailureKind::CommandNotFound),
        ("ssh", "ended", 0, CommandFailureKind::Other),
        (
            "ssh.exe",
            "bad argument",
            2,
            CommandFailureKind::HelperFailed,
        ),
        ("wsl.exe", "bad argument", 2, CommandFailureKind::Other),
        ("wsl", "bad argument", 1, CommandFailureKind::Other),
        (
            "some-other-transport",
            "bad argument",
            2,
            CommandFailureKind::Other,
        ),
    ];
    for (program, words, code, want) in cases {
        let err = CommandTransport::connect(
            &ends(program, words, code),
            ClientOptions::new(hello("hub")),
        )
        .await
        .unwrap_err();
        let (kind, exit, _) = command_failure(err);
        assert_eq!(kind, want, "{program} {code} {words:?}");
        assert_eq!(exit, Some(i32::from(code)), "{program}");
    }
}

/// A connector that talks to the child before the hello (the product's bootstrap) and then runs
/// the hello itself gets, from `ChildHandle::explain_handshake_failure`, exactly what
/// `CommandTransport::connect` gives.
#[cfg(unix)]
#[tokio::test]
async fn a_custom_connector_explains_a_failed_hello_exactly_as_connect_does() {
    let specs = [
        ends("ssh", "refusing the runtime directory", 2),
        ends("ssh", "", 4),
        ends("ssh", "client_loop: send disconnect: Broken pipe", 255),
        ends("wsl.exe", "bad argument", 2),
        ends("ssh", "Connection reset by peer", 255),
    ];
    for spec in specs {
        let direct = CommandTransport::connect(&spec, ClientOptions::new(hello("hub")))
            .await
            .unwrap_err();
        let (io, child) = CommandTransport::spawn(&spec).unwrap();
        // (The connector's own conversation with the child would be here.)
        let failed = ControlClient::connect(io, ClientOptions::new(hello("hub")))
            .await
            .unwrap_err();
        let explained = child.explain_handshake_failure(failed).await;
        assert_eq!(explained, direct, "{spec:?}");
    }

    // A peer that answered is passed through as it is, with no look at the child.
    let (io, child) = CommandTransport::spawn(&demo(&["serve-stdio", "--major", "9"])).unwrap();
    let failed = ControlClient::connect(io, ClientOptions::new(hello("hub")))
        .await
        .unwrap_err();
    assert!(matches!(failed, ConnectError::Incompatible { .. }));
    assert_eq!(
        child.explain_handshake_failure(failed.clone()).await,
        failed
    );
}

// ── the transport's end and our hello meet in either order ──────────────────

/// Which side of the handshake sees the child go away first. The two are a race between two
/// processes that the product does not control, so each is forced here by what the test awaits:
/// `Write`: the child has already exited when our hello is written, so the write fails (a broken
/// pipe) before the read sees the end of the stream; `Read`: the child takes our hello and only
/// then exits, so the write succeeds and the read sees the end of the stream.
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum First {
    Write,
    Read,
}

/// The demo binary as `program` ending the way `ends` does, with `--read-hello` for the order
/// where our hello is taken first.
#[cfg(unix)]
fn ends_in_order(program: &str, words: &str, code: u8, first: First) -> CommandSpec {
    let mut spec = ends(program, words, code);
    if first == First::Read {
        spec.argv.push("--read-hello".into());
    }
    spec
}

/// Runs the handshake over a fresh child in the given order; returns the handshake's own error
/// and what `explain_handshake_failure` makes of it.
#[cfg(unix)]
async fn handshake_in_order(spec: &CommandSpec, first: First) -> (ConnectError, ConnectError) {
    let (io, child) = CommandTransport::spawn(spec).unwrap();
    if first == First::Write {
        // The child is gone before we write: awaiting its exit is the signal, not a delay.
        assert!(within(child.wait_exit(BOUND)).await.is_some());
    }
    let raw = ControlClient::connect(io, ClientOptions::new(hello("hub")))
        .await
        .unwrap_err();
    let explained = child.explain_handshake_failure(raw.clone()).await;
    (raw, explained)
}

/// The precondition of the pair below: with the child gone, writing to it fails with an error
/// that means "the peer is gone" while reading from it is the end of the stream. Both are one
/// event, and `wasit_control::peer_gone` is what says so.
#[cfg(unix)]
#[tokio::test]
async fn a_child_that_is_gone_breaks_our_write_and_ends_our_read() {
    use tokio::io::AsyncWriteExt;
    let (mut io, child) = CommandTransport::spawn(&ends("ssh", "gone", 2)).unwrap();
    assert!(within(child.wait_exit(BOUND)).await.is_some());
    let e = io.write_all(b"{}\n").await.unwrap_err();
    assert!(wasit_control::peer_gone(&e), "{e:?}");
    let mut rest = Vec::new();
    within(io.read_to_end(&mut rest)).await.unwrap();
    assert!(rest.is_empty());
}

/// Whether the write or the read noticed first, the handshake ends the same way, and so does
/// the explanation: the same error, kind, exit code and words.
#[cfg(unix)]
#[tokio::test]
async fn a_child_that_goes_away_gives_one_handshake_failure_in_either_order() {
    for (program, words, code, kind) in [
        (
            "ssh",
            "refusing the runtime directory",
            2,
            CommandFailureKind::HelperFailed,
        ),
        ("ssh", "", 4, CommandFailureKind::HelperFailed),
        (
            "ssh",
            "bash: marsad-agent: command not found",
            127,
            CommandFailureKind::CommandNotFound,
        ),
        (
            "ssh",
            "client_loop: send disconnect: Broken pipe",
            255,
            CommandFailureKind::Other,
        ),
        (
            "ssh",
            "Connection reset by peer",
            255,
            CommandFailureKind::Unreachable,
        ),
        ("wsl.exe", "bad argument", 2, CommandFailureKind::Other),
        ("some-other-transport", "", 1, CommandFailureKind::Other),
    ] {
        let mut seen = Vec::new();
        for first in [First::Write, First::Read] {
            let spec = ends_in_order(program, words, code, first);
            let (raw, explained) = handshake_in_order(&spec, first).await;
            assert_eq!(
                raw,
                ConnectError::ClosedDuringHandshake,
                "{program} {first:?}"
            );
            let (k, exit, text) = command_failure(explained.clone());
            assert_eq!(
                (k, exit),
                (kind, Some(i32::from(code))),
                "{program} {code} {words:?} {first:?}"
            );
            // Never the raw error of the write.
            assert!(
                !text.contains("os error") && !text.contains("i/o error"),
                "{text}"
            );
            if kind == CommandFailureKind::HelperFailed {
                assert!(
                    text.contains("connection closed during handshake"),
                    "{text}"
                );
            }
            seen.push((raw, explained));
        }
        assert_eq!(seen[0], seen[1], "{program} {code} {words:?}");
    }
}

/// `wait_exit_unbounded` returns with the child's status whenever the child ends, however long
/// that takes (here: when its input is closed): it is pending while the child runs, and has no
/// bound of its own to give up at.
#[cfg(unix)]
#[tokio::test]
async fn waiting_for_a_childs_exit_has_no_bound_of_its_own() {
    use futures::FutureExt as _;
    let spec = CommandSpec::new(["sh", "-c", "read x; exit 3"]);
    let (io, child) = CommandTransport::spawn(&spec).unwrap();
    assert!(child.exit().is_none());
    let mut waiting = Box::pin(child.wait_exit_unbounded());
    // Polled once while the child runs: pending (nothing waited for).
    assert!(waiting.as_mut().now_or_never().is_none());
    // Its input closes: it reads EOF and exits with its own status.
    drop(io);
    let exit = within(waiting).await.expect("it exited");
    assert_eq!(exit.code, Some(3));
}
