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
    ConnectError, ControlClient, ControlServer, UnixSocketListener,
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
