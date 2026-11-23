mod common;

use std::time::Duration;

use common::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use wasit_shim::{probe, read_journal, Error, ExitInfo, Kind, ShimClient, ShimState, Sig};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_roundtrip_and_clean_exit() {
    let d = TempDir::new();
    let h = start_child(d.path(), "cat", &[]).await;
    assert_eq!(h.spawned_seq, 1);
    assert_eq!(probe(d.path()).await.unwrap(), ShimState::Running);

    let c = ShimClient::connect(d.path()).await.unwrap();
    assert_eq!(c.child_pid(), h.child_pid);
    let mut s = c.attach(1).await.unwrap();
    let spawned = next(&mut s).await;
    assert_eq!(spawned.kind, Kind::Spawned);
    assert_eq!(spawned.data["pid"], h.child_pid);
    assert_eq!(spawned.data["argv"][0], child_bin());

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
    start_child(d.path(), "cat", &[]).await;
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
    let info = recs.last().unwrap().exit_info().unwrap();
    #[cfg(unix)]
    assert_eq!(info.signal, Some(9));
    #[cfg(windows)]
    assert_eq!(
        info,
        ExitInfo {
            code: Some(1),
            signal: None
        }
    );
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
        start_child(d.path(), "seqloop", &[]).await;
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
    start_child(d.path(), "cat", &[]).await;
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
    start_child(d.path(), "cat", &[]).await;
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
    let mut sp = child_spec(d.path(), "yes", &["400000"]);
    sp.options.controller_write_timeout = Duration::from_millis(300);
    sp.options.linger = Duration::from_secs(1);
    let h = start_spec(sp).await;

    let (rd, mut wr) = raw_connect(d.path()).await;
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
    // The shim has exited, so its log is complete: the controller was dropped for not keeping up.
    let log = shim_log(d.path());
    let dropped: Vec<_> = events(&log, "shim.controller")
        .into_iter()
        .filter(|r| r["event"] == "slow_dropped")
        .collect();
    assert_eq!(dropped.len(), 1, "log: {log:?}");
    assert_eq!(dropped[0]["write_timeout_ms"], 300);

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
        // The shim drops a stalled controller mid-write, so the last line may be torn.
        if n == 0 || !line.ends_with('\n') {
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
    let listener = RawListener::bind(d.path());
    let peer = tokio::spawn(async move {
        let (rd, mut wr) = listener.accept().await;
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
            .args(["--", &child_bin(), "cat"])
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

/// Start the child, optionally deliver `sig` once it runs, return how it ended.
async fn run_to_exit(mode: &str, args: &[&str], sig: Option<Sig>) -> ExitInfo {
    let d = TempDir::new();
    start_child(d.path(), mode, args).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_code_is_recorded() {
    assert_eq!(
        run_to_exit("exit", &["7"], None).await,
        ExitInfo {
            code: Some(7),
            signal: None
        }
    );
}

/// POSIX signals: the child dies from the very signal that was sent.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn posix_signals_are_recorded() {
    async fn sh(script: &str, sig: Option<Sig>) -> ExitInfo {
        let d = TempDir::new();
        start(d.path(), "sh", &["-c", script]).await;
        let c = ShimClient::connect(d.path()).await.unwrap();
        let mut s = c.attach(1).await.unwrap();
        if let Some(sig) = sig {
            next(&mut s).await;
            c.signal(sig).await.unwrap();
        }
        let recs = until_exited(&mut s).await;
        recs.last().unwrap().exit_info().unwrap()
    }
    assert_eq!(sh("kill -TERM $$", None).await.signal, Some(libc::SIGTERM));
    assert_eq!(
        sh("exec sleep 1000", Some(Sig::Kill)).await.signal,
        Some(libc::SIGKILL)
    );
    assert_eq!(
        sh("exec sleep 1000", Some(Sig::Term)).await.signal,
        Some(libc::SIGTERM)
    );
    assert_eq!(
        sh("exec sleep 1000", Some(Sig::Int)).await.signal,
        Some(libc::SIGINT)
    );
    let info = run_to_exit("sleep", &[], Some(Sig::Kill)).await;
    assert_eq!(info.signal, Some(libc::SIGKILL));
}

/// Windows has no POSIX signals: kill and term terminate the job (exit code 1).
#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn job_termination_is_recorded() {
    for sig in [Sig::Kill, Sig::Term] {
        let info = run_to_exit("sleep", &[], Some(sig)).await;
        assert_eq!(
            info,
            ExitInfo {
                code: Some(1),
                signal: None
            }
        );
    }
}

/// Read the pid the `spawn-sleep` child prints for its grandchild.
async fn grandchild_pid(s: &mut wasit_shim::RecordStream) -> u32 {
    until(s, |r| r.kind == Kind::Out)
        .await
        .last()
        .unwrap()
        .text()
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_to_shim_terminates_child_group() {
    let d = TempDir::new();
    let h = start_child(d.path(), "spawn-sleep", &[]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let bg = grandchild_pid(&mut s).await;
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

/// Killing the shim outright takes the whole tree with it (process group on Unix, job object
/// on Windows) and the session probes as dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killing_the_shim_kills_child_tree_and_probe_says_dead() {
    let d = TempDir::new();
    let h = start_child(d.path(), "spawn-sleep", &[]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    let bg = grandchild_pid(&mut s).await;
    kill_hard(h.shim_pid);
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
    let h = start_child(d.path(), "print", &["x"]).await;
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
    let h2 = start_child(d.path(), "print", &["y"]).await;
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
    start_child(d.path(), "huge", &[]).await;
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
    // 19 x "é" = 38 bytes
    let mut sp = child_spec(d.path(), "utf8", &[]);
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
    start_child(d.path(), "bytes", &[]).await;
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
    start_child(d.path(), "cat", &[]).await;
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

/// Lock and endpoint in a separate runtime directory (a shared home keeps only the journal).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_dir_holds_lock_and_endpoint() {
    let d = TempDir::new();
    let r = TempDir::new();
    let data = d.path().join("s1");
    let run = r.path().join("s1");
    let mut sp = child_spec(&data, "cat", &[]);
    sp.run_dir = Some(run.clone());
    start_spec(sp).await;
    assert!(data.join(wasit_shim::JOURNAL_FILE).is_file());
    assert!(!data.join(wasit_shim::LOCK_FILE).exists());
    assert!(run.join(wasit_shim::LOCK_FILE).is_file());
    // Seen from the data directory alone (another machine's view), nobody holds the lock.
    assert_eq!(
        wasit_shim::probe_at(&data, &run).await.unwrap(),
        ShimState::Running
    );
    assert_eq!(probe(&data).await.unwrap(), ShimState::Dead);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert!(wasit_shim::endpoint_name(&run).exists());
        let mode = std::fs::metadata(&run).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }
    let c = ShimClient::connect(&run).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    c.close_stdin().await.unwrap();
    until_exited(&mut s).await;
    until_end(&mut s).await;
    assert!(matches!(
        wasit_shim::probe_at(&data, &run).await.unwrap(),
        ShimState::Exited(_)
    ));
}

/// `shim.log` is NDJSON diagnostic records: a header, then events for start, attach and exit,
/// every line JSON, and nothing a child or a controller said (nor an argument, nor an
/// environment value).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shim_log_holds_diagnostic_records_and_no_content() {
    const MARK: &str = "LOG-CONTENT-MARK-3c9e";
    let d = TempDir::new();
    let mut sp = child_spec(d.path(), "cat", &[MARK]);
    sp.env
        .push(("SECRET_VAR".into(), format!("secret-{MARK}").into()));
    sp.cwd = Some(d.path().join(format!("project-{MARK}")));
    std::fs::create_dir_all(sp.cwd.as_ref().unwrap()).unwrap();
    let h = start_spec(sp).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    c.write(&format!("prompt {MARK}")).await.unwrap();
    until(&mut s, |r| r.kind == Kind::Out).await;
    c.note(serde_json::json!({"note": MARK})).await.unwrap();
    c.close_stdin().await.unwrap();
    until_exited(&mut s).await;
    until_end(&mut s).await;
    drop(c);
    assert!(await_pid_exit(h.shim_pid, T).await);

    let text = std::fs::read_to_string(d.path().join(wasit_shim::LOG_FILE)).unwrap();
    assert!(!text.contains(MARK), "{text}");
    assert!(!shim_stderr(d.path()).contains(MARK));
    let log = shim_log(d.path());
    let header = &log[0];
    assert_eq!(header["diag_file"], 1);
    assert_eq!(header["src"], "shim");
    assert_eq!(header["pid"], h.shim_pid);
    let records = &log[1..];
    assert!(records
        .iter()
        .all(|r| r["src"] == "shim" && r["b"] == header["b"]));
    // Sequence numbers are the sink's: strictly increasing from 1.
    let seqs: Vec<u64> = records.iter().map(|r| r["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());

    assert_eq!(events(&log, "process.start").len(), 1);
    let start = events(&log, "shim.start");
    assert_eq!(start.len(), 1);
    assert_eq!(start[0]["pid"], h.shim_pid);
    assert_eq!(start[0]["child_pid"], h.child_pid);
    assert_eq!(start[0]["spawned_seq"], h.spawned_seq);
    assert_eq!(start[0]["separate_run_dir"], false);
    assert_eq!(
        start[0]["session"],
        d.path().file_name().unwrap().to_str().unwrap()
    );
    // The connection's own end (`disconnected`) may be cut off by the process exit that follows
    // the final record, so only the attach is certain.
    let controller = events(&log, "shim.controller");
    assert_eq!(controller[0]["event"], "attached");
    assert_eq!(controller[0]["from_seq"], 1);
    let exit = events(&log, "shim.exit");
    assert_eq!(exit.len(), 1);
    assert_eq!(exit[0]["code"], 0);
    assert_eq!(exit[0]["synced"], true);
    // start comes first, exit last.
    assert_eq!(records.last().unwrap()["ev"], "shim.exit");
    assert!(events(&log, "diag.dropped").is_empty());
}

/// A signal sent by a controller is a record naming the signal and who sent it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signals_are_recorded_with_who_sent_them() {
    let d = TempDir::new();
    let h = start_child(d.path(), "sleep", &[]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    next(&mut s).await; // spawned
    c.signal(Sig::Kill).await.unwrap();
    until_exited(&mut s).await;
    until_end(&mut s).await;
    drop(c);
    assert!(await_pid_exit(h.shim_pid, T).await);
    let log = shim_log(d.path());
    let sig = events(&log, "shim.signal");
    assert_eq!(sig.len(), 1, "{log:?}");
    assert_eq!(
        (sig[0]["sig"].as_str(), sig[0]["by"].as_str()),
        (Some("kill"), Some("controller"))
    );
    let exit = events(&log, "shim.exit");
    assert_eq!(
        (exit[0]["signal"].as_i64(), exit[0].get("code")),
        (Some(9), None)
    );
}

/// Leak L2: a malformed `--env` never has its value in the readiness line, in `shim.log`, in
/// the shim's stderr, or in the error `spawn_detached` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_env_is_refused_without_echoing_the_value() {
    const MARK: &str = "ENV-VALUE-MARK-77aa";
    // The shim binary itself: its readiness line, its log, its stderr.
    let d = TempDir::new();
    for bad in [MARK.to_string(), format!("={MARK}")] {
        let out = tokio::time::timeout(
            T,
            tokio::process::Command::new(shim_bin())
                .arg("--dir")
                .arg(d.path())
                .args(["--env", &bad, "--", &child_bin(), "cat"])
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(out.status.code(), Some(2));
        let stdout = String::from_utf8_lossy(&out.stdout);
        let ready: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
        assert_eq!(ready["ready"], false);
        let message = ready["error"].as_str().unwrap();
        assert!(message.contains("--env #1"), "{message}");
        assert!(!stdout.contains(MARK) && !String::from_utf8_lossy(&out.stderr).contains(MARK));
        let text = std::fs::read_to_string(d.path().join(wasit_shim::LOG_FILE)).unwrap();
        assert!(!text.contains(MARK), "{text}");
        assert!(!shim_stderr(d.path()).contains(MARK));
        let log = shim_log(d.path());
        let not_ready = events(&log, "shim.not_ready");
        assert!(!not_ready.is_empty(), "{log:?}");
        assert_eq!(not_ready.last().unwrap()["lvl"], "error");
        assert_eq!(not_ready.last().unwrap()["error"]["code"], "not_ready");
        // The child was never started.
        assert!(events(&log, "shim.start").is_empty());
    }

    // Through `spawn_detached` (what the runtime does): the error returned to the caller. A
    // value that is not UTF-8 cannot be passed on the command line as text.
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let d = TempDir::new();
        let mut value = format!("K={MARK}").into_bytes();
        value.push(0xff);
        let mut sp = child_spec(d.path(), "cat", &[]);
        sp.env
            .push(("BAD".into(), std::ffi::OsString::from_vec(value)));
        let err = wasit_shim::spawn_detached(&sp).await.unwrap_err();
        assert!(matches!(err, Error::Spawn(_)), "{err:?}");
        let shown = format!("{err} {err:?}");
        assert!(!shown.contains(MARK), "{shown}");
        let text = std::fs::read_to_string(d.path().join(wasit_shim::LOG_FILE)).unwrap();
        assert!(!text.contains(MARK), "{text}");
        assert!(!shim_stderr(d.path()).contains(MARK));
        assert!(!events(&shim_log(d.path()), "shim.not_ready").is_empty());
    }
}

/// The shim's stderr is a file of its own, cut to nothing when it is over its bound, and the
/// diagnostics log is still a separate file with a single writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stderr_is_a_separate_bounded_file() {
    let d = TempDir::new();
    std::fs::write(
        d.path().join(wasit_shim::STDERR_FILE),
        vec![b'x'; 70 * 1024],
    )
    .unwrap();
    start_child(d.path(), "print", &["x"]).await;
    let c = ShimClient::connect(d.path()).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    until_exited(&mut s).await;
    assert_eq!(shim_stderr(d.path()), "");
    // Small leftovers stay.
    std::fs::write(d.path().join(wasit_shim::STDERR_FILE), "earlier crash\n").unwrap();
    let d2 = d.path().to_path_buf();
    let h = start_child(&d2, "print", &["y"]).await;
    assert!(await_pid_exit(h.shim_pid, T).await);
    assert_eq!(shim_stderr(&d2), "earlier crash\n");
}

/// Design S23 (leak L4): session directory `0700`, journal, log and stderr files `0600`, also
/// when an older build left them wider.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_files_are_private_to_the_owner() {
    use std::os::unix::fs::PermissionsExt;
    fn mode(p: &std::path::Path) -> u32 {
        std::fs::metadata(p).unwrap().permissions().mode() & 0o777
    }
    let d = TempDir::new();
    let dir = d.path().join("s1");
    let h = start_child(&dir, "cat", &[]).await;
    let c = ShimClient::connect(&dir).await.unwrap();
    let mut s = c.attach(1).await.unwrap();
    c.close_stdin().await.unwrap();
    until_exited(&mut s).await;
    until_end(&mut s).await;
    drop(c);
    assert!(await_pid_exit(h.shim_pid, T).await);
    assert_eq!(mode(&dir), 0o700);
    for f in [
        wasit_shim::JOURNAL_FILE,
        wasit_shim::LOG_FILE,
        wasit_shim::STDERR_FILE,
        wasit_shim::LOCK_FILE,
    ] {
        assert_eq!(mode(&dir.join(f)), 0o600, "{f}");
    }

    // A directory and files of an older build, wider than that: the next shim tightens them.
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    for f in [
        wasit_shim::JOURNAL_FILE,
        wasit_shim::LOG_FILE,
        wasit_shim::STDERR_FILE,
        wasit_shim::LOCK_FILE,
    ] {
        std::fs::set_permissions(dir.join(f), std::fs::Permissions::from_mode(0o644)).unwrap();
    }
    let h = start_child(&dir, "print", &["again"]).await;
    assert!(await_pid_exit(h.shim_pid, T).await);
    assert_eq!(mode(&dir), 0o700);
    for f in [
        wasit_shim::JOURNAL_FILE,
        wasit_shim::LOG_FILE,
        wasit_shim::STDERR_FILE,
        wasit_shim::LOCK_FILE,
    ] {
        assert_eq!(mode(&dir.join(f)), 0o600, "{f}");
    }
}

/// A newer controller displacing the attached one is a record, in order with the attaches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_replacement_is_recorded() {
    let d = TempDir::new();
    let h = start_child(d.path(), "cat", &[]).await;
    let a = ShimClient::connect(d.path()).await.unwrap();
    let mut sa = a.attach(3).await.unwrap();
    let b = ShimClient::connect(d.path()).await.unwrap();
    let r = tokio::time::timeout(T, sa.next()).await.unwrap();
    assert!(matches!(r, Some(Err(Error::Replaced))), "got {r:?}");
    let mut sb = b.attach(1).await.unwrap();
    b.close_stdin().await.unwrap();
    until_exited(&mut sb).await;
    until_end(&mut sb).await;
    assert!(await_pid_exit(h.shim_pid, T).await);
    let log = shim_log(d.path());
    let seen: Vec<(&str, Option<u64>)> = events(&log, "shim.controller")
        .iter()
        .map(|r| (r["event"].as_str().unwrap(), r["from_seq"].as_u64()))
        .filter(|(e, _)| *e != "disconnected")
        .collect();
    assert_eq!(
        seen,
        [
            ("attached", Some(3)),
            ("replaced", None),
            ("attached", Some(1))
        ]
    );
}
