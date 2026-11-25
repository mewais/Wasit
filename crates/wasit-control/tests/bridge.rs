//! The bridge in front of a daemon that starts slowly: the dialing side waits for its `ready`
//! frame instead of failing the hello, and the bridge dials once after the daemon is ready.
#![cfg(unix)]

mod common;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use wasit_control::{
    bridge, codes, join_duplex, local_endpoint, BridgeOptions, ClientOptions, ConnConfig,
    ConnectError, ControlClient, ControlServer, UnixSocketListener, STEP_STARTING_DAEMON,
};

/// How long the fake daemon takes to become ready: far longer than the hello deadline below.
const SLOW_START: Duration = Duration::from_secs(3);

/// Dialing side with a 1 s hello deadline: it could only pass if the deadline starts at ready.
fn dialer() -> ClientOptions {
    let hello_timeout = Duration::from_secs(1);
    let config = ConnConfig {
        hello_timeout,
        ..cfg()
    };
    ClientOptions {
        await_ready: true,
        ..ClientOptions::new(hello("hub")).config(config)
    }
}

#[tokio::test]
async fn the_hello_waits_for_a_slow_daemon_to_be_ready() {
    let dir = scratch_dir("slowbridge");
    let path = dir.join("d.sock");
    let ep = local_endpoint(&path);
    let (hub_out, bridge_in) = tokio::io::duplex(64 * 1024);
    let (bridge_out, hub_in) = tokio::io::duplex(64 * 1024);

    // The daemon: nothing listens until the start closure has waited for its readiness.
    let socket = path.clone();
    let start = move || async move {
        tokio::time::sleep(SLOW_START).await;
        let listener = UnixSocketListener::bind_exclusive(socket)
            .await
            .map_err(io::Error::other)?;
        let server = ControlServer::new(Arc::new(TestService::new("slow")), hello("slow"), cfg());
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        Ok::<(), io::Error>(())
    };
    let bridged = tokio::spawn(async move {
        bridge(bridge_in, bridge_out, &ep, start, BridgeOptions::default()).await
    });

    let client = within(ControlClient::connect(
        join_duplex(hub_in, hub_out),
        dialer(),
    ))
    .await
    .expect("the hello deadline must not run while the daemon starts");
    assert_eq!(client.remote_hello().host_id.as_str(), "slow");
    bridged.abort();
}

#[tokio::test]
async fn an_unreachable_daemon_is_reported_after_one_dial_with_no_retries() {
    let dir = scratch_dir("nodaemon");
    let path = dir.join("d.sock");
    let ep = local_endpoint(&path);
    let (hub_out, bridge_in) = tokio::io::duplex(64 * 1024);
    let (bridge_out, hub_in) = tokio::io::duplex(64 * 1024);
    // The start "succeeds" but nothing ever listens: the bridge must answer that at once.
    let start = || async { Ok::<(), io::Error>(()) };
    let bridged = tokio::spawn(async move {
        bridge(bridge_in, bridge_out, &ep, start, BridgeOptions::default()).await
    });

    let err = within(ControlClient::connect(
        join_duplex(hub_in, hub_out),
        dialer(),
    ))
    .await
    .expect_err("nothing listens");
    match err {
        ConnectError::Refused(reason) => assert_eq!(reason.code, codes::BRIDGE_UNAVAILABLE),
        other => panic!("expected the bridge's refusal, got {other:?}"),
    }
    assert!(within(bridged).await.unwrap().is_err());
}

/// While the bridge starts a daemon nobody serves, the dialing side is told what it is doing
/// before the daemon is ready, and the hello still completes once it is. The daemon starts only
/// when the test opens its gate, so the report is seen while the start is still pending.
#[tokio::test]
async fn the_dialer_is_told_the_daemon_is_starting_before_it_is_ready() {
    use tokio::sync::{mpsc, Notify};
    let dir = scratch_dir("progress");
    let path = dir.join("d.sock");
    let ep = local_endpoint(&path);
    let (hub_out, bridge_in) = tokio::io::duplex(64 * 1024);
    let (bridge_out, hub_in) = tokio::io::duplex(64 * 1024);

    let gate = Arc::new(Notify::new());
    let open = gate.clone();
    let socket = path.clone();
    let start = move || async move {
        open.notified().await;
        let listener = UnixSocketListener::bind_exclusive(socket)
            .await
            .map_err(io::Error::other)?;
        let server = ControlServer::new(Arc::new(TestService::new("gated")), hello("gated"), cfg());
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        Ok::<(), io::Error>(())
    };
    let bridged = tokio::spawn(async move {
        bridge(bridge_in, bridge_out, &ep, start, BridgeOptions::default()).await
    });

    let (steps_tx, mut steps) = mpsc::unbounded_channel::<String>();
    let opts = ClientOptions {
        on_progress: Some(Arc::new(move |step: &str| {
            let _ = steps_tx.send(step.to_owned());
        })),
        ..dialer()
    };
    let dialing = tokio::spawn(ControlClient::connect(join_duplex(hub_in, hub_out), opts));

    let step = within(steps.recv()).await.expect("a progress report");
    assert_eq!(step, STEP_STARTING_DAEMON);
    assert!(
        !dialing.is_finished(),
        "the hello cannot finish before the daemon is ready"
    );
    gate.notify_one();

    let client = within(dialing)
        .await
        .unwrap()
        .expect("the hello completes once the daemon is ready");
    assert_eq!(client.remote_hello().host_id.as_str(), "gated");
    bridged.abort();
}
