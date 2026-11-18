//! Real Unix sockets: single-instance lock, stale socket cleanup, server restart.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use wasit_control::demo::DemoService;
use wasit_control::{
    connector, BindError, ClientOptions, ControlClient, ControlServer, DurableOpts, Endpoint,
    InstanceLock, LinkState, LockError, RequestOpts, Supervised, SupervisedConfig,
    SupervisedOptions, UnixSocketListener,
};

const D: Duration = Duration::from_secs(10);

async fn start(path: &Path, host: &str) -> (ControlServer, JoinHandle<std::io::Result<()>>) {
    let listener = UnixSocketListener::bind_exclusive(path)
        .await
        .expect("bind");
    let server = ControlServer::new(Arc::new(DemoService::new(host)), hello(host), cfg());
    let s2 = server.clone();
    let task = tokio::spawn(async move { s2.serve(listener).await });
    (server, task)
}

#[tokio::test]
async fn single_instance_lock_and_requests_over_unix_socket() {
    let dir = scratch_dir("lock");
    let path = dir.join("d.sock");
    let (server, task) = start(&path, "u1").await;

    // A second instance cannot bind and must not remove the live socket.
    match UnixSocketListener::bind_exclusive(&path).await {
        Err(BindError::Lock(LockError::Held(_))) => {}
        Err(e) => panic!("unexpected error {e}"),
        Ok(_) => panic!("second instance bound"),
    }
    assert!(path.exists());

    let client = ControlClient::connect_endpoint(
        &Endpoint::Unix(path.clone()),
        ClientOptions::new(hello("c")),
    )
    .await
    .unwrap();
    let who: String = client.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "u1");

    server.shutdown(D).await;
    within(task).await.unwrap().unwrap();
    // Listener dropped: socket removed and lock released.
    assert!(!path.exists());
    let _again = InstanceLock::try_acquire(wasit_control::lock_path_for(&path))
        .await
        .unwrap();
}

#[tokio::test]
async fn stale_socket_is_removed_only_by_the_lock_holder() {
    let dir = scratch_dir("stale");
    let path = dir.join("d.sock");
    // A dead instance left its socket file behind.
    drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
    assert!(path.exists());
    let e = ControlClient::connect_endpoint(
        &Endpoint::Unix(path.clone()),
        ClientOptions::new(hello("c")),
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("refused"), "{e}");

    // Someone else holds the lock: we must not touch the socket.
    let lock = InstanceLock::try_acquire(wasit_control::lock_path_for(&path))
        .await
        .unwrap();
    assert!(matches!(
        UnixSocketListener::bind_exclusive(&path).await,
        Err(BindError::Lock(LockError::Held(_)))
    ));
    assert!(path.exists());
    drop(lock);

    // With the lock acquired the stale file is replaced.
    let (server, task) = start(&path, "u2").await;
    let client = ControlClient::connect_endpoint(
        &Endpoint::Unix(path.clone()),
        ClientOptions::new(hello("c")),
    )
    .await
    .unwrap();
    let who: String = client.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "u2");
    server.shutdown(D).await;
    within(task).await.unwrap().unwrap();
}

#[tokio::test]
async fn server_restart_supervised_client_reconnects_and_durable_resumes() {
    let dir = scratch_dir("restart");
    let path = dir.join("d.sock");
    let (server1, task1) = start(&path, "gen1").await;

    let ep = Endpoint::Unix(path.clone());
    let conn = connector(move || {
        let ep = ep.clone();
        async move { ControlClient::connect_endpoint(&ep, ClientOptions::new(hello("gui"))).await }
    });
    let mut config = SupervisedConfig::default();
    config.backoff.initial = Duration::from_millis(5);
    config.backoff.max = Duration::from_millis(50);
    let sup = Supervised::spawn(
        conn,
        SupervisedOptions {
            config,
            ..Default::default()
        },
    );
    let mut state = sup.state();
    let s = wait_watch(&mut state, LinkState::is_connected).await;
    match s {
        LinkState::Connected { hello } => assert_eq!(hello.host_id.as_str(), "gen1"),
        _ => unreachable!(),
    }

    const N: u64 = 3000;
    let mut durable = sup.subscribe_durable(
        "count",
        DurableOpts {
            params: json!({ "limit": N }),
            ..Default::default()
        },
    );
    let mut expected = 1u64;
    while expected <= 100 {
        let (c, _) = within(durable.next()).await.unwrap().unwrap();
        assert_eq!(c.as_u64(), Some(expected));
        expected += 1;
    }

    server1.shutdown(D).await;
    within(task1).await.unwrap().unwrap();
    wait_watch(&mut state, |s| matches!(s, LinkState::Reconnecting { .. })).await;
    let e = sup
        .request::<Value>("whoami", Value::Null, RequestOpts::new(D), Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(e, wasit_control::CallError::NotConnected);

    let (server2, task2) = start(&path, "gen2").await;
    while let Some(item) = within(durable.next()).await {
        let (c, _) = item.unwrap();
        assert_eq!(
            c.as_u64(),
            Some(expected),
            "gap or duplicate across restart"
        );
        expected += 1;
    }
    assert_eq!(expected, N + 1);
    assert!(durable.subscribe_count() >= 2);
    let who: String = sup
        .request("whoami", Value::Null, RequestOpts::new(D), BOUND)
        .await
        .unwrap();
    assert_eq!(who, "gen2");

    sup.stop().await;
    server2.shutdown(D).await;
    within(task2).await.unwrap().unwrap();
}
