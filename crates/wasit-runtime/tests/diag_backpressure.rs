//! Diagnostics never slow a session (design §10, "never blocks, real process"): a daemon whose
//! diagnostics file cannot be opened — it is a FIFO nobody reads, so the writer thread blocks in
//! `open` — and whose record queue holds 16 records still runs a session from create to a
//! completed turn. Only then is the FIFO read: it yields the queued records and a
//! `diag.dropped` that counts the rest, and the daemon goes on recording.
#![cfg(unix)]

mod common;

use common::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use wasit_core::Command;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::SessionEvent;

const QUEUE: u64 = 16;

async fn next_event(sub: &mut wasit_runtime::TypedSubscription<SessionEvent>) -> SessionEvent {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok((_, e)))) => e,
        other => panic!(
            "no event: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

async fn next_line(lines: &mut tokio::io::Lines<BufReader<tokio::fs::File>>) -> serde_json::Value {
    let line = tokio::time::timeout(T, lines.next_line())
        .await
        .expect("a line from the diagnostics file")
        .unwrap()
        .expect("the file ended");
    serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON ({e}): {line:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocked_diagnostics_file_does_not_slow_a_session() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let fifo = data.join("diag").join("daemon.ndjson");
    std::fs::create_dir_all(fifo.parent().unwrap()).unwrap();
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(made.success());

    let mut daemon = daemon_command(&data)
        .env("WASIT_DIAG_QUEUE", QUEUE.to_string())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    await_ready(&mut daemon).await;

    // A session: create, prompt, turn completed. The daemon answers every request while its
    // writer thread sits in `open`.
    let c = client(&data);
    let sc = Scenario::builder()
        .turn(Turn::builder().say("finished"))
        .build();
    let id = c.create(create(d.path(), "claude", &sc)).await.unwrap().id;
    let mut sub = c.subscribe_session(&id, None);
    loop {
        if is_phase(&next_event(&mut sub).await, "idle") {
            break;
        }
    }
    c.command(&id, Command::prompt("go")).await.unwrap();
    loop {
        if is_turn_completed(&next_event(&mut sub).await) {
            break;
        }
    }
    // More requests than the queue holds, so records are certainly dropped.
    for _ in 0..(2 * QUEUE) {
        c.host_info().await.unwrap();
    }

    // Only now is the file read. Opening it for reading releases the writer's `open`.
    let file = tokio::time::timeout(T, tokio::fs::File::open(&fifo))
        .await
        .expect("the writer opens the FIFO")
        .unwrap();
    let mut lines = BufReader::new(file).lines();
    let header = next_line(&mut lines).await;
    assert_eq!(header["diag_file"], 1);
    assert_eq!(header["src"], "daemon");

    let mut written = Vec::new();
    let dropped = loop {
        let r = next_line(&mut lines).await;
        if r["ev"] == "diag.dropped" {
            break r;
        }
        written.push(r);
    };
    // The queue's records, in order from the first: no more than it holds.
    assert!(
        !written.is_empty() && written.len() as u64 <= QUEUE,
        "{} records",
        written.len()
    );
    assert_eq!(written[0]["ev"], "process.start");
    assert!(written.iter().any(|r| r["ev"] == "hostd.start"));
    for (i, r) in written.iter().enumerate() {
        assert_eq!(r["seq"], i as u64 + 1);
    }
    // The drop count names exactly what is missing.
    let first = dropped["first_seq"].as_u64().unwrap();
    let last = dropped["last_seq"].as_u64().unwrap();
    assert_eq!(first, written.len() as u64 + 1);
    assert_eq!(dropped["count"].as_u64().unwrap(), last - first + 1);
    assert_eq!(dropped["lvl"], "warn");
    assert!(
        dropped["count"].as_u64().unwrap() >= QUEUE,
        "the requests alone exceed the queue: {dropped}"
    );

    // And the daemon records again once the file takes writes.
    c.host_info().await.unwrap();
    loop {
        let r = next_line(&mut lines).await;
        if r["ev"] == "hostd.request" && r["seq"].as_u64().unwrap() > last {
            assert_eq!(r["method"], "host.info");
            break;
        }
    }
}
