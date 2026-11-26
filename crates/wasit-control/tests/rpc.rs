//! Connection-level behaviour over in-memory duplex streams.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use wasit_control::demo::DemoService;
use wasit_control::{
    codes, connector, memory_pair, CallError, ClientOptions, CloseReason, ConnClose, ConnConfig,
    ConnectError, ControlClient, ControlServer, Cursor, DurableEnd, DurableOpts, Frame, LinkState,
    ProtocolVersion, RequestOpts, SubscribeOpts, Supervised, SupervisedConfig, SupervisedOptions,
};

const D: Duration = Duration::from_secs(10);

#[tokio::test]
async fn hello_exchange_negotiates_minor_and_features() {
    let (client, server) = mem_pair(Arc::new(DemoService::new("srv")), "srv", cfg(), cfg()).await;
    assert_eq!(client.remote_hello().host_id.as_str(), "srv");
    assert_eq!(client.negotiated().features, vec!["a", "b"]);
    let who: String = client.request("whoami", Value::Null, D).await.unwrap();
    assert_eq!(who, "srv");
    let conns = within(async {
        loop {
            let c = server.connections();
            if !c.is_empty() {
                break c;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(conns[0].remote_hello().host_id.as_str(), "client");
}

#[tokio::test]
async fn different_major_is_a_typed_incompatible_error_on_both_sides() {
    let mut theirs = hello("srv");
    theirs.protocol = ProtocolVersion { major: 2, minor: 7 };
    let server = ControlServer::new(Arc::new(DemoService::new("srv")), theirs, cfg());
    let (a, b) = memory_pair(4096);
    let server_side = server.serve_connection(b);
    let err = ControlClient::connect(a, ClientOptions::new(hello("c")))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        ConnectError::Incompatible {
            ours: ProtocolVersion { major: 1, minor: 0 },
            theirs: ProtocolVersion { major: 2, minor: 7 },
        }
    );
    let server_err = within(server_side).await.unwrap().unwrap_err();
    assert!(matches!(server_err, ConnectError::Incompatible { .. }));
}

#[tokio::test]
async fn different_minor_and_unknown_features_are_negotiated() {
    let mut theirs = hello("srv").with_features(["b", "z"]);
    theirs.protocol.minor = 5;
    let server = ControlServer::new(Arc::new(DemoService::new("srv")), theirs, cfg());
    let client = mem_connect(&server, ClientOptions::new(hello("c")))
        .await
        .unwrap();
    assert_eq!(client.negotiated().minor, 0);
    assert_eq!(client.negotiated().features, vec!["b"]);
}

#[tokio::test]
async fn slow_handler_does_not_block_other_requests() {
    let svc = Arc::new(TestService::new("srv"));
    let gate = svc.gate.clone();
    let entered = svc.entered.clone();
    let (client, _server) = mem_pair(svc, "srv", cfg(), cfg()).await;

    let c2 = client.clone();
    let slow = tokio::spawn(async move { c2.request::<String>("block", Value::Null, D).await });
    within(entered.notified()).await;

    let mut fast = Vec::new();
    for i in 0..50 {
        let c = client.clone();
        fast.push(tokio::spawn(async move {
            c.request::<Value>("echo", json!({ "i": i }), D).await
        }));
    }
    for (i, f) in fast.into_iter().enumerate() {
        assert_eq!(within(f).await.unwrap().unwrap(), json!({ "i": i }));
    }
    assert!(!slow.is_finished());
    gate.notify_one();
    assert_eq!(within(slow).await.unwrap().unwrap(), "released");
}

#[tokio::test]
async fn request_deadline_is_enforced_and_late_response_ignored() {
    let svc = Arc::new(TestService::new("srv"));
    let gate = svc.gate.clone();
    let (client, _server) = mem_pair(svc, "srv", cfg(), cfg()).await;
    let err = client
        .request::<String>("block", Value::Null, Duration::from_millis(50))
        .await
        .unwrap_err();
    assert_eq!(err, CallError::Timeout);
    gate.notify_one(); // the late response arrives and is dropped
    let v: Value = client.request("echo", json!(1), D).await.unwrap();
    assert_eq!(v, json!(1));
}

#[tokio::test]
async fn errors_are_typed() {
    let svc = Arc::new(TestService::new("srv"));
    let (client, _server) = mem_pair(svc, "srv", cfg(), cfg()).await;
    let e = client
        .request::<Value>("nope", Value::Null, D)
        .await
        .unwrap_err();
    assert!(
        matches!(e, CallError::Rpc(ref r) if r.code == codes::METHOD_NOT_FOUND),
        "{e:?}"
    );
    let e = client
        .request::<Value>("fail", Value::Null, D)
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref r) if r.code == "demo_failure"));
    let e = client
        .request::<Value>("panic", Value::Null, D)
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref r) if r.code == codes::INTERNAL));
    let e = client
        .request::<u64>("echo", json!("str"), D)
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Decode(_)));
    let e = client.subscribe("nope", None).await.unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref r) if r.code == codes::TOPIC_NOT_FOUND));
    // Still healthy.
    let v: Value = client.request("echo", json!(2), D).await.unwrap();
    assert_eq!(v, json!(2));
}

#[tokio::test]
async fn subscription_replays_from_cursor_then_ends() {
    let (client, _server) = mem_pair(Arc::new(DemoService::new("srv")), "srv", cfg(), cfg()).await;
    let mut sub = client
        .subscribe_with(
            "count",
            SubscribeOpts::new(D)
                .params(json!({"limit": 10}))
                .from(Some(Cursor::from_u64(3))),
        )
        .await
        .unwrap();
    let mut got = Vec::new();
    while let Some((c, v)) = within(sub.next()).await {
        assert_eq!(c.as_u64(), v.as_u64());
        got.push(v.as_u64().unwrap());
    }
    assert_eq!(got, (4..=10).collect::<Vec<_>>());
    assert_eq!(sub.close_reason(), Some(&CloseReason::Ended));
    assert_eq!(sub.last_cursor(), Some(&Cursor::from_u64(10)));
}

#[tokio::test]
async fn unsubscribe_on_drop_stops_the_server_stream() {
    let svc = Arc::new(DemoService::new("srv"));
    let stats = svc.stats.clone();
    let (client, _server) = mem_pair(svc, "srv", cfg(), cfg()).await;
    let mut dropped = stats.streams_dropped.subscribe();
    let mut sub = client.subscribe("count", None).await.unwrap();
    assert!(within(sub.next()).await.is_some());
    drop(sub);
    wait_watch(&mut dropped, |n| *n >= 1).await;
}

#[tokio::test]
async fn lagging_consumer_gets_lagged_after_buffered_events() {
    let svc = Arc::new(DemoService::new("srv"));
    let stats = svc.stats.clone();
    let client_cfg = ConnConfig {
        subscription_queue: 4,
        ..cfg()
    };
    // The server honours the 4-event window and declares lag after 50ms without credit.
    let server_cfg = ConnConfig {
        lag_timeout: Duration::from_millis(50),
        ..cfg()
    };
    let (client, _server) = mem_pair(svc, "srv", server_cfg, client_cfg).await;
    let mut dropped = stats.streams_dropped.subscribe();
    let mut sub = client
        .subscribe_with(
            "count",
            SubscribeOpts::new(D).params(json!({"limit": 100000})),
        )
        .await
        .unwrap();
    // Do not read: the queue overflows, the client unsubscribes, the server drops the stream.
    wait_watch(&mut dropped, |n| *n >= 1).await;
    let mut got = Vec::new();
    while let Some((_, v)) = within(sub.next()).await {
        got.push(v.as_u64().unwrap());
    }
    assert_eq!(sub.close_reason(), Some(&CloseReason::Lagged));
    assert_eq!(got, (1..=got.len() as u64).collect::<Vec<_>>());
    assert!(got.len() <= 4);
    // Resubscribe from the last cursor continues without gap.
    let mut sub2 = client
        .subscribe_with(
            "count",
            SubscribeOpts::new(D)
                .params(json!({"limit": 100000}))
                .from(sub.last_cursor().cloned()),
        )
        .await
        .unwrap();
    let (_, v) = within(sub2.next()).await.unwrap();
    assert_eq!(v.as_u64().unwrap(), got.len() as u64 + 1);
}

#[tokio::test]
async fn server_side_lag_closes_only_that_subscription() {
    // The client never reads the transport: the server's outbound queue fills and the
    // subscription is closed as Lagged by the server, while the connection stays up.
    let svc = Arc::new(DemoService::new("srv"));
    let stats = svc.stats.clone();
    let server_cfg = ConnConfig {
        outbound_queue: 8,
        lag_timeout: Duration::from_millis(50),
        ..cfg()
    };
    let server = ControlServer::new(svc, hello("srv"), server_cfg);
    let (raw, b) = memory_pair(1024);
    server.serve_connection(b);
    let (r, mut w) = tokio::io::split(raw);
    let mut lines = BufReader::new(r).lines();
    let hello_line = wasit_control::wire::encode(&Frame::Hello(hello("raw")), 1 << 20).unwrap();
    w.write_all(&hello_line).await.unwrap();
    let first = within(lines.next_line()).await.unwrap().unwrap();
    assert!(first.contains("\"hello\""));
    let sub = wasit_control::wire::encode(
        &Frame::Subscribe {
            id: 1,
            host: None,
            topic: "count".into(),
            params: json!({"limit": 1000000}),
            from: None,
            hops: 0,
            window: 0,
        },
        1 << 20,
    )
    .unwrap();
    w.write_all(&sub).await.unwrap();
    let mut dropped = stats.streams_dropped.subscribe();
    wait_watch(&mut dropped, |n| *n >= 1).await;
    // Now drain: events..., then subscription_closed{lagged}; the connection still answers.
    let mut saw_lagged = false;
    let mut last = 0u64;
    while let Some(line) = within(lines.next_line()).await.unwrap() {
        let f = wasit_control::wire::decode(line.as_bytes())
            .unwrap()
            .unwrap();
        match f {
            Frame::Event { cursor, .. } => {
                let n = cursor.as_u64().unwrap();
                assert_eq!(n, last + 1);
                last = n;
            }
            Frame::SubscriptionClosed { reason, .. } => {
                assert_eq!(reason, CloseReason::Lagged);
                saw_lagged = true;
                let req = wasit_control::wire::encode(
                    &Frame::Request {
                        id: 2,
                        host: None,
                        method: "echo".into(),
                        params: json!("alive"),
                        hops: 0,
                        trace: None,
                    },
                    1 << 20,
                )
                .unwrap();
                w.write_all(&req).await.unwrap();
            }
            Frame::Response { id: 2, result, .. } => {
                assert_eq!(result, json!("alive"));
                break;
            }
            _ => {}
        }
    }
    assert!(saw_lagged);
}

#[tokio::test]
async fn oversized_frames_are_rejected() {
    let server_cfg = ConnConfig {
        max_frame: 1024,
        ..cfg()
    };
    let (client, _server) =
        mem_pair(Arc::new(DemoService::new("srv")), "srv", server_cfg, cfg()).await;
    // Response too large for the server's limit: error response, connection survives.
    let e = client
        .request::<Value>("big", json!({"bytes": 4096}), D)
        .await
        .unwrap_err();
    assert!(
        matches!(e, CallError::Rpc(ref r) if r.code == codes::FRAME_TOO_LARGE),
        "{e:?}"
    );
    let v: Value = client.request("echo", json!(1), D).await.unwrap();
    assert_eq!(v, json!(1));
    // Request too large for the server: it says goodbye and closes.
    let e = client
        .request::<Value>("echo", json!("y".repeat(4096)), D)
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::ConnectionLost(_)), "{e:?}");
    match within(client.closed()).await {
        ConnClose::Goodbye { reason } => assert_eq!(reason.code, codes::FRAME_TOO_LARGE),
        other => panic!("unexpected close {other:?}"),
    }
    // Locally oversized requests fail before sending.
    let small = ConnConfig {
        max_frame: 300,
        ..cfg()
    };
    let (client, _server) = mem_pair(Arc::new(DemoService::new("srv")), "srv", cfg(), small).await;
    let e = client
        .request::<Value>("echo", json!("z".repeat(400)), D)
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::Rpc(ref r) if r.code == codes::FRAME_TOO_LARGE));
    assert!(!client.is_closed());
}

#[tokio::test]
async fn garbage_input_closes_that_connection_only() {
    let server = ControlServer::new(Arc::new(DemoService::new("srv")), hello("srv"), cfg());

    // Garbage instead of hello: handshake fails.
    let (raw, b) = memory_pair(4096);
    let h = server.serve_connection(b);
    let (r, mut w) = tokio::io::split(raw);
    w.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    let err = within(h).await.unwrap().unwrap_err();
    assert!(matches!(err, ConnectError::Protocol(_)), "{err:?}");
    drop(r);

    // Garbage after hello: goodbye{protocol_error}, then EOF.
    let (raw, b) = memory_pair(4096);
    let h = server.serve_connection(b);
    let (r, mut w) = tokio::io::split(raw);
    let mut lines = BufReader::new(r).lines();
    w.write_all(&wasit_control::wire::encode(&Frame::Hello(hello("raw")), 1 << 20).unwrap())
        .await
        .unwrap();
    assert!(within(lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .contains("hello"));
    w.write_all(b"{not json\n").await.unwrap();
    let bye = within(lines.next_line()).await.unwrap().unwrap();
    assert!(
        bye.contains("goodbye") && bye.contains(codes::PROTOCOL),
        "{bye}"
    );
    assert!(within(lines.next_line()).await.unwrap().is_none());
    assert!(matches!(
        within(h).await.unwrap().unwrap(),
        ConnClose::Protocol { .. }
    ));

    // Unknown frame types are ignored; the server still works.
    let client = mem_connect(&server, ClientOptions::new(hello("c")))
        .await
        .unwrap();
    let v: Value = client.request("echo", json!("ok"), D).await.unwrap();
    assert_eq!(v, json!("ok"));
}

#[tokio::test]
async fn silent_peer_hits_idle_timeout() {
    let client_cfg = ConnConfig {
        idle_timeout: Duration::from_millis(200),
        ping_interval: Duration::from_millis(50),
        ..cfg()
    };
    let (a, raw) = memory_pair(64 * 1024);
    let (r, mut w) = tokio::io::split(raw);
    w.write_all(&wasit_control::wire::encode(&Frame::Hello(hello("silent")), 1 << 20).unwrap())
        .await
        .unwrap();
    let client = ControlClient::connect(a, ClientOptions::new(hello("c")).config(client_cfg))
        .await
        .unwrap();
    assert_eq!(within(client.closed()).await, ConnClose::IdleTimeout);
    drop(r);
}

#[tokio::test]
async fn pings_keep_an_idle_connection_alive() {
    let fast = ConnConfig {
        idle_timeout: Duration::from_millis(300),
        ping_interval: Duration::from_millis(30),
        ..cfg()
    };
    let (client, _server) =
        mem_pair(Arc::new(DemoService::new("srv")), "srv", fast.clone(), fast).await;
    // Wait (bounded) on something that never happens: the connection must not close.
    let r = tokio::time::timeout(Duration::from_millis(900), client.closed()).await;
    assert!(r.is_err(), "connection closed: {r:?}");
}

#[tokio::test]
async fn graceful_shutdown_says_goodbye() {
    let (client, server) = mem_pair(Arc::new(DemoService::new("srv")), "srv", cfg(), cfg()).await;
    let mut sub = client.subscribe("count", None).await.unwrap();
    assert!(within(sub.next()).await.is_some());
    server.shutdown(D).await;
    match within(client.closed()).await {
        ConnClose::Goodbye { reason } => assert_eq!(reason.code, codes::SHUTTING_DOWN),
        other => panic!("unexpected close {other:?}"),
    }
    while within(sub.next()).await.is_some() {}
    assert_eq!(sub.close_reason(), Some(&CloseReason::HostLost));
    let e = client
        .request::<Value>("echo", json!(1), D)
        .await
        .unwrap_err();
    assert!(matches!(e, CallError::ConnectionLost(_)));
}

#[tokio::test]
async fn either_side_can_call_the_other() {
    // The client serves a service too; the server-side service calls back over ctx.peer.
    let server = ControlServer::new(Arc::new(DemoService::new("srv")), hello("srv"), cfg());
    let client = mem_connect(
        &server,
        ClientOptions::new(hello("cli")).service(Arc::new(DemoService::new("cli"))),
    )
    .await
    .unwrap();
    let v: String = client
        .request("peer_call", json!({"method": "whoami"}), D)
        .await
        .unwrap();
    assert_eq!(v, "cli");
}

fn supervised_mem(server: ControlServer, sub_queue: usize) -> Supervised {
    let conn = connector(move || {
        let server = server.clone();
        async move {
            let cfg = ConnConfig {
                subscription_queue: sub_queue,
                ..cfg()
            };
            mem_connect(&server, ClientOptions::new(hello("sup")).config(cfg)).await
        }
    });
    let mut config = SupervisedConfig::default();
    config.backoff.initial = Duration::from_millis(5);
    config.backoff.max = Duration::from_millis(50);
    Supervised::spawn(
        conn,
        SupervisedOptions {
            config,
            ..Default::default()
        },
    )
}

/// Property-style: random pause points force `Lagged` closes; the durable subscription still
/// delivers every event exactly once, in order.
#[tokio::test]
async fn durable_subscription_survives_lag_without_loss_or_duplication() {
    for seed in [1u64, 7, 42] {
        let svc = Arc::new(DemoService::new("srv"));
        let stats = svc.stats.clone();
        let server_cfg = ConnConfig {
            lag_timeout: Duration::from_millis(20),
            ..cfg()
        };
        let server = ControlServer::new(svc, hello("srv"), server_cfg);
        let sup = supervised_mem(server, 8);
        const N: u64 = 20_000;
        let mut durable = sup.subscribe_durable(
            "count",
            DurableOpts {
                params: json!({ "limit": N }),
                ..Default::default()
            },
        );
        let mut rng = fastrand::Rng::with_seed(seed);
        let mut pauses: Vec<u64> = (0..5).map(|_| rng.u64(1..N - 5_000)).collect();
        pauses.sort_unstable();
        let mut dropped = stats.streams_dropped.subscribe();
        let mut expected = 1u64;
        while let Some(item) = within(durable.next()).await {
            let (c, v) = item.expect("no terminal error");
            assert_eq!(c.as_u64(), Some(expected), "seed {seed}");
            assert_eq!(v.as_u64(), Some(expected));
            if pauses.first() == Some(&expected) {
                pauses.remove(0);
                // Stop consuming until every server-side stream created so far (including the
                // current one) has been torn down, i.e. the current subscription lagged.
                let created = stats.subscribes.load(Ordering::SeqCst);
                wait_watch(&mut dropped, |n| *n >= created).await;
            }
            expected += 1;
        }
        assert_eq!(expected, N + 1, "seed {seed}");
        assert_eq!(durable.end_reason(), Some(&DurableEnd::Ended));
        assert!(
            durable.subscribe_count() >= 6,
            "each of the 5 pauses must have forced a resubscribe"
        );
        assert_eq!(
            stats.subscribes.load(Ordering::Relaxed),
            durable.subscribe_count()
        );
        sup.stop().await;
    }
}

#[tokio::test]
async fn supervised_requests_fail_fast_or_wait_when_disconnected() {
    let conn = connector(|| async { Err(ConnectError::Io("refused".into())) });
    let mut config = SupervisedConfig::default();
    config.backoff.initial = Duration::from_millis(5);
    config.backoff.max = Duration::from_millis(20);
    let sup = Supervised::spawn(
        conn,
        SupervisedOptions {
            config,
            ..Default::default()
        },
    );
    let mut st = sup.state();
    let s = wait_watch(
        &mut st,
        |s| matches!(s, LinkState::Reconnecting { attempt, .. } if *attempt >= 2),
    )
    .await;
    match s {
        LinkState::Reconnecting { last_error, .. } => assert!(last_error.contains("refused")),
        _ => unreachable!(),
    }
    let e = sup
        .request::<Value>("echo", json!(1), RequestOpts::new(D), Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(e, CallError::NotConnected);
    let e = sup
        .request::<Value>(
            "echo",
            json!(1),
            RequestOpts::new(D),
            Duration::from_millis(30),
        )
        .await
        .unwrap_err();
    assert_eq!(e, CallError::NotConnected);
    sup.stop().await;
    assert_eq!(sup.current_state(), LinkState::Stopped);
}

#[tokio::test]
async fn supervised_request_waits_for_link_within_caller_deadline() {
    let svc = Arc::new(DemoService::new("srv"));
    let server = ControlServer::new(svc, hello("srv"), cfg());
    let sup = supervised_mem(server, 64);
    let v: Value = sup
        .request("echo", json!("hi"), RequestOpts::new(D), BOUND)
        .await
        .unwrap();
    assert_eq!(v, json!("hi"));
    // reconnect_now drops the link and reconnects.
    let first = sup.current().unwrap().id();
    sup.reconnect_now();
    let mut st = sup.state();
    within(async {
        loop {
            if let Some(c) = sup.current() {
                if c.id() != first {
                    break;
                }
            }
            st.changed().await.unwrap();
        }
    })
    .await;
    sup.stop().await;
}

/// A reconnect requested while an attempt is in flight is satisfied by the link that attempt
/// makes: the new link is kept, not dropped and dialled again for that same request.
#[tokio::test]
async fn reconnect_requested_during_an_attempt_keeps_the_link_it_makes() {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use tokio::sync::Notify;

    let server = ControlServer::new(Arc::new(DemoService::new("srv")), hello("srv"), cfg());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let conn = {
        let (server, entered, release, calls) = (
            server.clone(),
            entered.clone(),
            release.clone(),
            calls.clone(),
        );
        connector(move || {
            let (server, entered, release, calls) = (
                server.clone(),
                entered.clone(),
                release.clone(),
                calls.clone(),
            );
            async move {
                // Only the first attempt is held open: it is the one a reconnect lands in.
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                mem_connect(&server, ClientOptions::new(hello("sup")).config(cfg())).await
            }
        })
    };
    let states = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let log = states.clone();
    let sup = Supervised::spawn(
        conn,
        SupervisedOptions {
            on_state: Some(Arc::new(move |s: &LinkState| {
                log.lock().unwrap().push(s.code())
            })),
            ..Default::default()
        },
    );
    within(entered.notified()).await;
    sup.reconnect_now();
    release.notify_one();
    let mut st = sup.state();
    within(st.wait_for(|s| s.is_connected())).await.unwrap();
    sup.stop().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the reconnect made during the attempt must not dial a second time"
    );
    assert_eq!(
        *states.lock().unwrap(),
        ["connecting", "connected", "stopped"],
        "the link the attempt made must not be dropped"
    );
}

#[tokio::test]
async fn flow_control_lets_a_large_replay_through_a_small_window_without_lag() {
    let small = ConnConfig {
        subscription_queue: 16,
        ..cfg()
    };
    let (client, _server) = mem_pair(Arc::new(DemoService::new("srv")), "srv", cfg(), small).await;
    let mut sub = client
        .subscribe_with(
            "count",
            SubscribeOpts::new(D).params(json!({"limit": 20000})),
        )
        .await
        .unwrap();
    let mut n = 0u64;
    while let Some((c, _)) = within(sub.next()).await {
        n += 1;
        assert_eq!(c.as_u64(), Some(n));
        if n.is_multiple_of(1000) {
            // A consumer that is briefly busy is not lagging.
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(n, 20000);
    assert_eq!(sub.close_reason(), Some(&CloseReason::Ended));
}

#[tokio::test]
async fn supervised_parks_failed_after_repeated_bridge_unavailable() {
    let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let c = calls.clone();
    let conn = connector(move || {
        c.fetch_add(1, Ordering::SeqCst);
        async {
            Err(ConnectError::Refused(wasit_control::RpcError::new(
                codes::BRIDGE_UNAVAILABLE,
                "bridge cannot reach the daemon",
            )))
        }
    });
    let mut config = SupervisedConfig::default();
    config.backoff.initial = Duration::from_millis(5);
    config.backoff.max = Duration::from_millis(20);
    let sup = Supervised::spawn(
        conn,
        SupervisedOptions {
            config,
            ..Default::default()
        },
    );
    let mut st = sup.state();
    let s = wait_watch(&mut st, |s| matches!(s, LinkState::Failed { .. })).await;
    match s {
        LinkState::Failed { reason, .. } => {
            assert!(reason.contains("could not start or reach"), "{reason}");
            assert!(reason.contains("3 attempts"), "{reason}");
        }
        _ => unreachable!(),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    sup.stop().await;
}

/// The frames of a peer that predates traces (no `trace` key) are served, and a frame that has
/// one reaches the service with it: the field is additive.
#[tokio::test]
async fn requests_with_and_without_a_trace_key_are_both_served() {
    let server = ControlServer::new(Arc::new(DemoService::new("srv")), hello("srv"), cfg());
    let (raw, b) = memory_pair(64 * 1024);
    server.serve_connection(b);
    let (r, mut w) = tokio::io::split(raw);
    let mut lines = BufReader::new(r).lines();
    let hello_line = wasit_control::wire::encode(&Frame::Hello(hello("raw")), 1 << 20).unwrap();
    w.write_all(&hello_line).await.unwrap();
    assert!(within(lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .contains("\"hello\""));

    // Exactly what a build without traces writes.
    w.write_all(b"{\"type\":\"request\",\"id\":1,\"method\":\"trace\",\"params\":null}\n")
        .await
        .unwrap();
    w.write_all(
        b"{\"type\":\"request\",\"id\":2,\"method\":\"trace\",\"params\":null,\"trace\":\"w-task-00000001\"}\n",
    )
    .await
    .unwrap();
    let mut got = std::collections::BTreeMap::new();
    while got.len() < 2 {
        let line = within(lines.next_line()).await.unwrap().unwrap();
        if let Some(Frame::Response { id, result, error }) =
            wasit_control::wire::decode(line.as_bytes()).unwrap()
        {
            assert!(error.is_none(), "{error:?}");
            got.insert(id, result);
        }
    }
    assert_eq!(got[&1], Value::Null);
    assert_eq!(got[&2], json!("w-task-00000001"));
}

// ── a peer that is gone is one event, whichever of the write and the read finds out first ──

mod peer_gone {
    use std::collections::VecDeque;
    use std::io::{Error, ErrorKind};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use wasit_control::wire::encode;
    use wasit_control::{join_duplex, peer_gone, BoxDuplex, Hello};

    use super::*;

    /// What a stream's read side does once its bytes are given out.
    #[derive(Clone, Copy)]
    enum End {
        Eof,
        Fail(ErrorKind),
        /// Nothing more, ever (the peer is alive but silent).
        Silent,
    }

    struct Reads {
        bytes: VecDeque<u8>,
        end: End,
    }

    impl AsyncRead for Reads {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if !self.bytes.is_empty() {
                while buf.remaining() > 0 {
                    match self.bytes.pop_front() {
                        Some(b) => buf.put_slice(&[b]),
                        None => break,
                    }
                }
                return Poll::Ready(Ok(()));
            }
            match self.end {
                End::Eof => Poll::Ready(Ok(())),
                End::Fail(k) => Poll::Ready(Err(Error::from(k))),
                End::Silent => Poll::Pending,
            }
        }
    }

    /// Takes `ok` writes, then fails every one with `kind` (`None`: never fails).
    struct Writes {
        ok: usize,
        kind: Option<ErrorKind>,
    }

    impl AsyncWrite for Writes {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            match self.kind {
                Some(k) if self.ok == 0 => Poll::Ready(Err(Error::from(k))),
                _ => {
                    self.ok = self.ok.saturating_sub(1);
                    Poll::Ready(Ok(buf.len()))
                }
            }
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn stream(
        peer_hello: bool,
        end: End,
        ok_writes: usize,
        write_fails: Option<ErrorKind>,
    ) -> BoxDuplex {
        let bytes = if peer_hello {
            encode(&Frame::Hello(Hello::new("b", "srv", "daemon")), 1 << 20).unwrap()
        } else {
            Vec::new()
        };
        join_duplex(
            Reads {
                bytes: bytes.into(),
                end,
            },
            Writes {
                ok: ok_writes,
                kind: write_fails,
            },
        )
    }

    /// The kinds that mean the peer is gone, and some that do not.
    const GONE: [ErrorKind; 5] = [
        ErrorKind::BrokenPipe,
        ErrorKind::ConnectionReset,
        ErrorKind::ConnectionAborted,
        ErrorKind::NotConnected,
        ErrorKind::UnexpectedEof,
    ];
    const ABOUT_US: [ErrorKind; 4] = [
        ErrorKind::PermissionDenied,
        ErrorKind::WriteZero,
        ErrorKind::TimedOut,
        ErrorKind::InvalidInput,
    ];

    #[test]
    fn the_set_of_kinds_that_mean_the_peer_is_gone_is_the_decided_one() {
        for k in GONE {
            assert!(peer_gone(&Error::from(k)), "{k:?}");
        }
        for k in ABOUT_US {
            assert!(!peer_gone(&Error::from(k)), "{k:?}");
        }
        for k in [
            ErrorKind::Interrupted,
            ErrorKind::WouldBlock,
            ErrorKind::Other,
        ] {
            assert!(!peer_gone(&Error::from(k)), "{k:?}");
        }
    }

    async fn handshake(io: BoxDuplex) -> Result<ControlClient, ConnectError> {
        ControlClient::connect(io, ClientOptions::new(hello("hub"))).await
    }

    /// Our hello cannot be written (the peer is gone), or can and the read then fails, or can
    /// and the stream ends: for a peer that is gone the handshake says one thing.
    #[tokio::test]
    async fn a_handshake_with_a_peer_that_is_gone_is_closed_during_handshake_in_every_order() {
        for k in GONE {
            // Write first: the hello does not go out; the read then finds the end.
            let e = handshake(stream(false, End::Eof, 0, Some(k)))
                .await
                .unwrap_err();
            assert_eq!(e, ConnectError::ClosedDuringHandshake, "write {k:?}");
            // The read first: the hello went out; the read says the peer is gone, or the end.
            let e = handshake(stream(false, End::Fail(k), usize::MAX, None))
                .await
                .unwrap_err();
            assert_eq!(e, ConnectError::ClosedDuringHandshake, "read {k:?}");
            // Both say it.
            let e = handshake(stream(false, End::Fail(k), 0, Some(k)))
                .await
                .unwrap_err();
            assert_eq!(e, ConnectError::ClosedDuringHandshake, "both {k:?}");
        }
        let e = handshake(stream(false, End::Eof, usize::MAX, None))
            .await
            .unwrap_err();
        assert_eq!(e, ConnectError::ClosedDuringHandshake, "end");
        // In memory, with a real peer: it is gone before our write, or after it took our hello.
        let (a, b) = memory_pair(64 * 1024);
        drop(b);
        let e = handshake(a).await.unwrap_err();
        assert_eq!(e, ConnectError::ClosedDuringHandshake, "dropped first");
        let (a, mut b) = memory_pair(64 * 1024);
        let taken = tokio::spawn(async move {
            let mut line = String::new();
            BufReader::new(&mut b).read_line(&mut line).await.unwrap();
            // `b` is dropped with the line taken.
        });
        let e = handshake(a).await.unwrap_err();
        taken.await.unwrap();
        assert_eq!(
            e,
            ConnectError::ClosedDuringHandshake,
            "dropped after its read"
        );
    }

    /// An error that is about us stays an I/O error, from the write or from the read.
    #[tokio::test]
    async fn a_handshake_failing_for_a_reason_that_is_not_the_peer_stays_an_io_error() {
        for k in ABOUT_US {
            let e = handshake(stream(false, End::Eof, 0, Some(k)))
                .await
                .unwrap_err();
            assert!(matches!(e, ConnectError::Io(_)), "write {k:?}: {e:?}");
            let e = handshake(stream(false, End::Fail(k), usize::MAX, None))
                .await
                .unwrap_err();
            assert!(matches!(e, ConnectError::Io(_)), "read {k:?}: {e:?}");
        }
    }

    /// The same after the handshake: an established connection whose peer is gone closes as
    /// `Eof` whether its writer (a frame that cannot be written) or its reader (the end, or a
    /// reset) finds out first; a failure that is about us is `Io`.
    #[tokio::test]
    async fn an_established_connection_whose_peer_is_gone_closes_the_same_in_either_order() {
        for k in GONE {
            // The writer first: the peer is silent, our request cannot be written.
            let c = handshake(stream(true, End::Silent, 1, Some(k)))
                .await
                .unwrap();
            let e = c.request::<Value>("echo", json!(1), D).await.unwrap_err();
            assert_eq!(e, CallError::ConnectionLost(ConnClose::Eof), "write {k:?}");
            assert_eq!(within(c.closed()).await, ConnClose::Eof, "write {k:?}");
            // The reader first: the writes would work.
            for end in [End::Eof, End::Fail(k)] {
                let c = handshake(stream(true, end, usize::MAX, None))
                    .await
                    .unwrap();
                assert_eq!(within(c.closed()).await, ConnClose::Eof, "read {k:?}");
            }
        }
        for k in ABOUT_US {
            let c = handshake(stream(true, End::Silent, 1, Some(k)))
                .await
                .unwrap();
            c.request::<Value>("echo", json!(1), D).await.unwrap_err();
            let ConnClose::Io { message } = within(c.closed()).await else {
                panic!("write {k:?}");
            };
            assert!(!message.is_empty());
            let c = handshake(stream(true, End::Fail(k), usize::MAX, None))
                .await
                .unwrap();
            assert!(
                matches!(within(c.closed()).await, ConnClose::Io { .. }),
                "read {k:?}"
            );
        }
    }
}

/// A request without a deadline waits for an answer longer than any bounded one would; the
/// bounded request of the same call times out first. A slow host is not failed by a deadline the
/// request does not need.
#[tokio::test]
async fn unbounded_request_outlasts_a_bounded_one() {
    let (client, _server) = mem_pair(Arc::new(DemoService::new("srv")), "srv", cfg(), cfg()).await;
    let bounded = client
        .request_with::<Value>(
            "sleep",
            json!({ "ms": 300 }),
            RequestOpts::new(Duration::from_millis(50)),
        )
        .await;
    assert_eq!(bounded.unwrap_err(), CallError::Timeout);
    let v = client
        .request_with::<Value>("sleep", json!({ "ms": 300 }), RequestOpts::unbounded())
        .await
        .unwrap();
    assert_eq!(v, json!("slept"));
}
