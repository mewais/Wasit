#![cfg(unix)]

mod common;

use std::time::Duration;

use common::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use wasit_shim::{probe, read_journal, Error, ExitInfo, Kind, ShimClient, ShimState, Sig};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_roundtrip_and_clean_exit() {
    let d = TempDir::new();
    let h = start(d.path(), "cat", &[]).await;
    assert_eq!(h.spawned_seq, 1);
    assert_eq!(probe(d.path()).await.unwrap(), ShimState::Running);

    let c = ShimClient::connect(d.path()).await.unwrap();
    assert_eq!(c.child_pid(), h.child_pid);
    let mut s = c.attach(1).await.unwrap();
    let spawned = next(&mut s).await;
    assert_eq!(spawned.kind, Kind::Spawned);
    assert_eq!(spawned.data["pid"], h.child_pid);
    assert_eq!(spawned.data["argv"][0], "cat");

    let seq = c.write("hello").await.unwrap();
    let r = next(&mut s).await;
    assert_eq!((r.seq, r.kind, r.text()), (seq, Kind::In, Some("hello")));
    let r = next(&mut s).await;
    assert_eq!((r.kind, r.text()), (Kind::Out, Some("hello")));
    assert!(r.seq > seq);

    c.close_stdin().await.unwrap();
    let tail = until_exited(&mut s).await;
    let exited = tail.last().unwrap();
    assert_eq!(
        exited.exit_info(),
        Some(ExitInfo {
            code: Some(0),
            signal: None
        })
    );
    until_end(&mut s).await;
    // The lock is released before the endpoint closes.
    assert_eq!(
        probe(d.path()).await.unwrap(),
        ShimState::Exited(ExitInfo {
            code: Some(0),
            signal: None
        })
    );
    assert!(!wasit_shim::endpoint_name(d.path()).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_and_out_are_ordered() {
    let d = TempDir::new();
    start(d.path(), "cat", &[]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(2).await.unwrap();
    let mut acks = Vec::new();
    for i in 0..200 {
        acks.push(c.write(&format!("line {i}")).await.unwrap());
    }
    assert!(acks.windows(2).all(|w| w[0] < w[1]));
    let mut recs = until(&mut s, |r| {
        r.kind == Kind::Out && r.text() == Some("line 199")
    })
    .await;
    assert_contiguous(&recs, 2);
    let ins = texts(&recs, Kind::In);
    let outs = texts(&recs, Kind::Out);
    let want: Vec<String> = (0..200).map(|i| format!("line {i}")).collect();
    assert_eq!(ins, want);
    assert_eq!(outs, want);
    for (i, ack) in acks.iter().enumerate() {
        let text = format!("line {i}");
        let pos_in = recs
            .iter()
            .position(|r| r.kind == Kind::In && r.text() == Some(&text));
        let pos_out = recs
            .iter()
            .position(|r| r.kind == Kind::Out && r.text() == Some(&text));
        assert!(pos_in < pos_out, "in must precede out for {text}");
        assert_eq!(recs[pos_in.unwrap()].seq, *ack);
    }
    c.signal(Sig::Kill).await.unwrap();
    recs = until_exited(&mut s).await;
    assert_eq!(recs.last().unwrap().exit_info().unwrap().signal, Some(9));
}

/// Attach from seq N while the child is writing a fast burst: exactly N.. with no gap and no
/// duplicate, across replacing controllers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attach_seam_has_no_gap_or_duplicate() {
    const B1: u64 = 20_000;
    const B2: u64 = 60_000;
    // Full expected journal (kind, text) after both bursts and close_stdin.
    let mut expected: Vec<(Kind, Option<String>)> = vec![(Kind::Spawned, None)];
    expected.push((Kind::In, Some(B1.to_string())));
    expected.extend((1..=B1).map(|i| (Kind::Out, Some(i.to_string()))));
    expected.push((Kind::In, Some(B2.to_string())));
    expected.extend((1..=B2).map(|i| (Kind::Out, Some(i.to_string()))));
    expected.push((Kind::Note, None));
    expected.push((Kind::Exited, None));
    let total = expected.len() as u64;

    let mut concurrent = 0;
    for iter in 0..10u64 {
        let d = TempDir::new();
        start(d.path(), "sh", &["-c", "while read n; do seq 1 $n; done"]).await;
        let a = ShimClient::connect(d.path()).await.unwrap();
        let mut sa = a.attach(1).await.unwrap();
        a.write(&B1.to_string()).await.unwrap();
        let first = until(&mut sa, |r| r.text() == Some("10000")).await;
        assert_contiguous(&first, 1);
        // Alternate between resuming right after the last record and re-reading history.
        let from = if iter % 2 == 0 {
            first.last().unwrap().seq + 1
        } else {
            1000 + iter * 731
        };
        drop(sa);
        drop(a);
        let b = ShimClient::connect(d.path()).await.unwrap();
        // Start the second burst, then attach while it is being written.
        b.write(&B2.to_string()).await.unwrap();
        let mut sb = b.attach(from).await.unwrap();
        let on_disk = std::fs::read(d.path().join("journal.ndjson"))
            .unwrap()
            .iter()
            .filter(|&&c| c == b'\n')
            .count() as u64;
        if on_disk < total - 2 {
            concurrent += 1;
        }
        let mut got = until(&mut sb, |r| r.seq == total - 2).await;
        b.close_stdin().await.unwrap();
        got.extend(until_exited(&mut sb).await);
        assert_contiguous(&got, from);
        assert_eq!(got.len() as u64, total - from + 1);
        for (r, (kind, text)) in got.iter().zip(&expected[from as usize - 1..]) {
            assert_eq!(r.kind, *kind, "{r:?}");
            if let Some(t) = text {
                assert_eq!(r.text(), Some(t.as_str()), "{r:?}");
            }
        }
        until_end(&mut sb).await;
    }
    eprintln!("attaches concurrent with the writer: {concurrent}/10");
    assert!(concurrent > 0, "no attach raced with the writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_resumes_exactly() {
    let d = TempDir::new();
    start(d.path(), "cat", &[]).await;
    let a = ShimClient::connect(d.path()).await.unwrap();
    let mut sa = a.attach(1).await.unwrap();
    for i in 1..=5 {
        a.write(&format!("l{i}")).await.unwrap();
    }
    let seen = until(&mut sa, |r| r.text() == Some("l5") && r.kind == Kind::Out).await;
    let k = seen.last().unwrap().seq;
    drop(sa);
    drop(a);

    let b = ShimClient::connect(d.path()).await.unwrap();
    let mut sb = b.attach(k + 1).await.unwrap();
    let seq = b.write("l6").await.unwrap();
    assert_eq!(seq, k + 1);
    let r = next(&mut sb).await;
    assert_eq!((r.seq, r.kind, r.text()), (k + 1, Kind::In, Some("l6")));
    let r = next(&mut sb).await;
    assert_eq!((r.seq, r.text()), (k + 2, Some("l6")));

    // Re-attaching on the same connection from history replays then continues live.
    let mut sb2 = b.attach(k - 3).await.unwrap();
    let replay = until(&mut sb2, |r| r.seq == k + 2).await;
    assert_contiguous(&replay, k - 3);
    b.write("l7").await.unwrap();
    let r = next(&mut sb2).await;
    assert_eq!((r.seq, r.text()), (k + 3, Some("l7")));
    b.close_stdin().await.unwrap();
    until_exited(&mut sb2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_controller_replaces_old() {
    let d = TempDir::new();
    start(d.path(), "cat", &[]).await;
    let a = ShimClient::connect(d.path()).await.unwrap();
    let mut sa = a.attach(1).await.unwrap();
    next(&mut sa).await; // spawned
    let b = ShimClient::connect(d.path()).await.unwrap();
    let r = tokio::time::timeout(T, sa.next()).await.unwrap();
    assert!(matches!(r, Some(Err(Error::Replaced))), "got {r:?}");
    assert!(a.write("nope").await.is_err());
    let mut sb = b.attach(1).await.unwrap();
    b.write("yes").await.unwrap();
    let recs = until(&mut sb, |r| r.kind == Kind::Out).await;
    assert_eq!(recs.last().unwrap().text(), Some("yes"));
    b.close_stdin().await.unwrap();
    until_exited(&mut sb).await;
}

/// A controller that stops reading is disconnected; the child is never blocked; nothing is
/// lost (the rest is in the journal).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_controller_is_dropped_and_child_not_blocked() {
    const LINES: usize = 400_000;
    let d = TempDir::new();
    let mut sp = spec(
        d.path(),
        "sh",
        &[
            "-c",
            "yes 0123456789012345678901234567890123456789 | head -n 400000",
        ],
    );
    sp.options.controller_write_timeout = Duration::from_millis(300);
    sp.options.linger = Duration::from_secs(1);
    let h = start_spec(sp).await;

    let sock = tokio::net::UnixStream::connect(wasit_shim::endpoint_name(d.path()))
        .await
        .unwrap();
    let (rd, mut wr) = sock.into_split();
    wr.write_all(b"{\"type\":\"attach\",\"from_seq\":1}\n")
        .await
        .unwrap();
    // Never read while the child runs and the shim finishes.
    assert!(
        await_pid_exit(h.child_pid, T).await,
        "child blocked by a stalled controller"
    );
    assert!(
        await_pid_exit(h.shim_pid, T).await,
        "shim waited for a stalled controller"
    );
    let log = std::fs::read_to_string(d.path().join(wasit_shim::LOG_FILE)).unwrap();
    assert!(log.contains("did not keep up"), "log: {log}");

    // The stalled connection received only a prefix, gap-free.
    let mut rd = BufReader::new(rd);
    let mut last = 0u64;
    let mut count = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        let n = tokio::time::timeout(T, rd.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        if n == 0 {
            break;
        }
        let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
        if v["type"] == "record" {
            let seq = v["record"]["seq"].as_u64().unwrap();
            assert_eq!(seq, last + 1);
            last = seq;
            count += 1;
        }
    }
    assert!(
        count > 0 && count < LINES,
        "expected a partial delivery, got {count}"
    );

    // Nothing lost: the journal holds the rest.
    let mut j = read_journal(d.path(), last + 1).await.unwrap();
    let mut rest = Vec::new();
    while let Some(r) = j.next().await {
        rest.push(r.unwrap());
    }
    assert_contiguous(&rest, last + 1);
    let outs_before = last as usize - 1; // seq 1 is `spawned`
    assert_eq!(outs_before + texts(&rest, Kind::Out).len(), LINES);
    assert_eq!(rest.last().unwrap().exit_info().unwrap().code, Some(0));
}

/// A client whose consumer stops polling never blocks its own requests: the ack queued behind
/// unconsumed records still arrives, and the stream ends with `Lagged` after the records that
/// fit. The peer is a scripted endpoint on a real socket so the interleaving is exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_consumer_gets_lagged_and_requests_still_complete() {
    let d = TempDir::new();
    let listener = tokio::net::UnixListener::bind(wasit_shim::endpoint_name(d.path())).unwrap();
    let peer = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (rd, mut wr) = sock.into_split();
        let mut rd = BufReader::new(rd);
        wr.write_all(b"{\"type\":\"hello\",\"proto\":1,\"shim_pid\":1,\"child_pid\":2}\n")
            .await
            .unwrap();
        let mut line = String::new();
        for _ in 0..2 {
            line.clear();
            rd.read_line(&mut line).await.unwrap();
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            let id = v["id"].as_u64().unwrap();
            if v["type"] == "write" {
                for seq in 1..=100 {
                    let f = format!(
                        "{{\"type\":\"record\",\"record\":{{\"seq\":{seq},\"t\":0,\"kind\":\"out\",\"data\":\"x\"}}}}\n"
                    );
                    wr.write_all(f.as_bytes()).await.unwrap();
                }
            }
            let ack = format!("{{\"type\":\"ack\",\"id\":{id},\"ok\":true,\"seq\":101}}\n");
            wr.write_all(ack.as_bytes()).await.unwrap();
        }
        // Keep the connection open until the client is done.
        line.clear();
        let _ = rd.read_line(&mut line).await;
    });
    let opts = wasit_shim::ClientOptions {
        record_queue: 16,
        lag_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let c = ShimClient::connect_with(d.path(), opts).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    // Not consuming: the ack behind 100 records must still arrive.
    assert_eq!(c.write("go").await.unwrap(), 101);
    let mut got = Vec::new();
    let end = loop {
        match next_item(&mut s).await {
            Some(Ok(r)) => got.push(r),
            other => break other,
        }
    };
    assert!(matches!(end, Some(Err(Error::Lagged))), "got {end:?}");
    assert_eq!(got.len(), 16);
    assert_contiguous(&got, 1);
    drop(c);
    peer.await.unwrap();
}

async fn next_item(s: &mut wasit_shim::RecordStream) -> Option<Result<wasit_shim::Record, Error>> {
    tokio::time::timeout(T, s.next()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shim_survives_death_of_its_spawner() {
    let d = TempDir::new();
    let out = tokio::time::timeout(
        T,
        tokio::process::Command::new(shim_bin())
            .arg("launch")
            .arg("--dir")
            .arg(d.path())
            .args(["--", "cat"])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let ready: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(ready["ready"], true);
    // The launcher process is gone; the shim is not.
    assert_eq!(probe(d.path()).await.unwrap(), ShimState::Running);
    let c = ShimClient::connect(d.path()).await.unwrap();
    assert_eq!(c.shim_pid() as u64, ready["pid"].as_u64().unwrap());
    let mut s = c.attach(1).await.unwrap();
    c.write("still here").await.unwrap();
    let recs = until(&mut s, |r| r.kind == Kind::Out).await;
    assert_eq!(recs.last().unwrap().text(), Some("still here"));
    c.close_stdin().await.unwrap();
    until_exited(&mut s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_code_and_signals_are_recorded() {
    async fn run(script: &str, sig: Option<Sig>) -> ExitInfo {
        let d = TempDir::new();
        start(d.path(), "sh", &["-c", script]).await;
        let c = ShimClient::connect(d.path()).await.unwrap();
        let mut s = c.attach(1).await.unwrap();
        if let Some(sig) = sig {
            next(&mut s).await; // spawned: child is running
            c.signal(sig).await.unwrap();
        }
        let recs = until_exited(&mut s).await;
        if let Some(sig) = sig {
            assert!(recs
                .iter()
                .any(|r| r.kind == Kind::Note && r.data["signal"] == sig.as_str()));
        }
        let info = recs.last().unwrap().exit_info().unwrap();
        until_end(&mut s).await;
        assert_eq!(probe(d.path()).await.unwrap(), ShimState::Exited(info));
        info
    }
    assert_eq!(
        run("exit 7", None).await,
        ExitInfo {
            code: Some(7),
            signal: None
        }
    );
    assert_eq!(run("kill -TERM $$", None).await.signal, Some(libc::SIGTERM));
    assert_eq!(
        run("exec sleep 1000", Some(Sig::Kill)).await.signal,
        Some(libc::SIGKILL)
    );
    assert_eq!(
        run("exec sleep 1000", Some(Sig::Term)).await.signal,
        Some(libc::SIGTERM)
    );
    assert_eq!(
        run("exec sleep 1000", Some(Sig::Int)).await.signal,
        Some(libc::SIGINT)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_to_shim_terminates_child_group() {
    let d = TempDir::new();
    let h = start(d.path(), "sh", &["-c", "sleep 1000 & echo $!; wait"]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let bg: u32 = until(&mut s, |r| r.kind == Kind::Out)
        .await
        .last()
        .unwrap()
        .text()
        .unwrap()
        .parse()
        .unwrap();
    // SAFETY: plain syscall.
    assert_eq!(unsafe { libc::kill(h.shim_pid as i32, libc::SIGTERM) }, 0);
    let recs = until_exited(&mut s).await;
    assert!(
        recs.iter()
            .any(|r| r.kind == Kind::Note
                && r.data["shim"].as_str().unwrap_or("").contains("SIGTERM"))
    );
    assert_eq!(
        recs.last().unwrap().exit_info().unwrap().signal,
        Some(libc::SIGTERM)
    );
    assert!(
        await_pid_exit(bg, T).await,
        "background grandchild survived"
    );
    assert!(await_pid_exit(h.shim_pid, T).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_9_of_shim_kills_child_group_and_probe_says_dead() {
    let d = TempDir::new();
    let h = start(d.path(), "sh", &["-c", "sleep 1000 & echo $!; wait"]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let bg: u32 = until(&mut s, |r| r.kind == Kind::Out)
        .await
        .last()
        .unwrap()
        .text()
        .unwrap()
        .parse()
        .unwrap();
    // SAFETY: plain syscall.
    assert_eq!(unsafe { libc::kill(h.shim_pid as i32, libc::SIGKILL) }, 0);
    assert!(await_pid_exit(h.shim_pid, T).await);
    assert_eq!(probe(d.path()).await.unwrap(), ShimState::Dead);
    assert!(
        await_pid_exit(h.child_pid, T).await,
        "child survived the shim"
    );
    assert!(await_pid_exit(bg, T).await, "grandchild survived the shim");
    // The stream ends: the connection died with the shim.
    let end = tokio::time::timeout(T, async {
        loop {
            match s.next().await {
                Some(Ok(_)) => continue,
                other => return other.is_none() || matches!(other, Some(Err(_))),
            }
        }
    })
    .await
    .unwrap();
    assert!(end);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torn_final_line_is_truncated_and_seq_continues() {
    let d = TempDir::new();
    std::fs::write(
        d.path().join("journal.ndjson"),
        "{\"seq\":1,\"t\":1,\"kind\":\"note\",\"data\":\"a\"}\n{\"seq\":2,\"t\":1,\"kind\":\"note\",\"data\":\"b\"}\n{\"seq\":3,\"t\":",
    )
    .unwrap();
    let h = start(d.path(), "sh", &["-c", "echo x"]).await;
    assert_eq!(h.spawned_seq, 3);
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let recs = until_exited(&mut s).await;
    assert_contiguous(&recs, 1);
    assert_eq!(recs[2].kind, Kind::Spawned);
    assert_eq!(texts(&recs, Kind::Out), vec!["x"]);
    until_end(&mut s).await;

    let text = std::fs::read_to_string(d.path().join("journal.ndjson")).unwrap();
    assert!(text.ends_with('\n'));
    for l in text.lines() {
        serde_json::from_str::<wasit_shim::Record>(l).unwrap();
    }
    let mut j = read_journal(d.path(), 2).await.unwrap();
    let mut from_file = Vec::new();
    while let Some(r) = j.next().await {
        from_file.push(r.unwrap());
    }
    assert_eq!(from_file, recs[1..].to_vec());

    // Reusing the directory continues the sequence.
    let last = recs.last().unwrap().seq;
    let h2 = start(d.path(), "sh", &["-c", "echo y"]).await;
    assert_eq!(h2.spawned_seq, last + 1);
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(last + 1).await.unwrap();
    let recs = until_exited(&mut s).await;
    assert_contiguous(&recs, last + 1);
    assert_eq!(texts(&recs, Kind::Out), vec!["y"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn huge_line_is_split_into_capped_records() {
    let d = TempDir::new();
    start(
        d.path(),
        "sh",
        &[
            "-c",
            "head -c 20000000 /dev/zero | tr '\\0' x; echo; echo after",
        ],
    )
    .await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(2).await.unwrap();
    let recs = until_exited(&mut s).await;
    let outs: Vec<_> = recs.iter().filter(|r| r.kind == Kind::Out).collect();
    let cap = 8 * 1024 * 1024;
    assert_eq!(outs.len(), 4);
    assert_eq!(outs[0].text().unwrap().len(), cap);
    assert!(outs[0].partial && outs[1].partial && !outs[2].partial && !outs[3].partial);
    assert_eq!(outs[1].text().unwrap().len(), cap);
    assert_eq!(outs[2].text().unwrap().len(), 20_000_000 - 2 * cap);
    assert!(outs[..3]
        .iter()
        .all(|r| r.text().unwrap().bytes().all(|b| b == b'x')));
    assert_eq!(outs[3].text(), Some("after"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_respects_utf8_boundaries() {
    let d = TempDir::new();
    let line = "é".repeat(19); // 38 bytes
    let mut sp = spec(d.path(), "sh", &["-c", &format!("printf '{line}\\n'")]);
    sp.options.max_record_bytes = 17;
    start_spec(sp).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let recs = until_exited(&mut s).await;
    let outs = texts(&recs, Kind::Out);
    assert_eq!(outs, vec!["é".repeat(8), "é".repeat(8), "é".repeat(3)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_utf8_and_stderr_are_journaled() {
    let d = TempDir::new();
    start(
        d.path(),
        "sh",
        &[
            "-c",
            "printf 'a\\377b\\r\\n'; printf 'e1\\n' >&2; printf 'no-newline'",
        ],
    )
    .await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let recs = until_exited(&mut s).await;
    assert_eq!(texts(&recs, Kind::Out), vec!["a\u{FFFD}b", "no-newline"]);
    assert_eq!(texts(&recs, Kind::Err), vec!["e1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_failure_is_reported_and_probe_handles_missing_dirs() {
    let d = TempDir::new();
    let r = wasit_shim::spawn_detached(&spec(d.path(), "/nonexistent/prog", &[])).await;
    match r {
        Err(Error::Spawn(m)) => assert!(m.contains("nonexistent"), "{m}"),
        other => panic!("expected spawn error, got {other:?}"),
    }
    assert_eq!(probe(d.path()).await.unwrap(), ShimState::Dead);
    assert_eq!(
        probe(&d.path().join("missing")).await.unwrap(),
        ShimState::Dead
    );
}

/// Controller notes are journaled in order with writes, and refused after `exited`.
#[tokio::test]
async fn controller_notes_are_ordered_and_refused_after_exit() {
    let d = TempDir::new();
    start(d.path(), "cat", &[]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let n1 = c.note(serde_json::json!({"cmd": 1})).await.unwrap();
    let w = c.write("hello").await.unwrap();
    let n2 = c.note(serde_json::json!({"cmd": 2})).await.unwrap();
    assert!(n1 < w && w < n2);
    let recs = until(&mut s, |r| r.kind == Kind::Out).await;
    let note = recs.iter().find(|r| r.seq == n1).unwrap();
    assert_eq!(note.kind, Kind::Note);
    assert_eq!(note.data, serde_json::json!({"cmd": 1}));
    c.close_stdin().await.unwrap();
    until_exited(&mut s).await;
    // The shim lingers until we have seen `exited`; a note now must be refused.
    match c.note(serde_json::json!({"late": true})).await {
        Err(Error::Remote(m)) => assert!(m.contains("exited"), "{m}"),
        Err(Error::Disconnected) => {} // the shim already went away
        other => panic!("late note accepted: {other:?}"),
    }
    assert!(matches!(
        wasit_shim::probe(d.path()).await.unwrap(),
        wasit_shim::ShimState::Exited(_) | wasit_shim::ShimState::Running
    ));
}
