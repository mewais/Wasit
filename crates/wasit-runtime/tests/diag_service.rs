//! `DiagService` against a real `wasit-hostd` (real shims, the fake CLI): `diag.sources`,
//! `diag.read` (whole lines, paging, what it refuses) and the `diag.tail` topic (a reset, then
//! live records without gap or duplicate, resuming by cursor, filters by session and trace),
//! and the content test: markers in a session's environment, MCP server and directory are in no
//! file the service can read.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use common::*;
use serde_json::Value;
use wasit_control::{Cursor, TraceId};
use wasit_core::{Command, McpServerConfig, McpTransport};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::{
    DiagTailEvent, DiagTailParams, RuntimeError, SessionEvent, SessionId, DIAG_READ_MAX,
};
use wasit_runtime::{ClientError, RuntimeClient, TypedSubscription};

fn trace(s: &str) -> TraceId {
    TraceId::new(s).unwrap()
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn next_session_event(sub: &mut TypedSubscription<SessionEvent>) -> SessionEvent {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok((_, e)))) => e,
        other => panic!(
            "no event: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

/// A session of the fake CLI that has run one turn (so its shim has written its log).
async fn session_with_a_turn(
    c: &RuntimeClient,
    d: &Path,
    req: Option<wasit_runtime::wire::CreateSession>,
) -> SessionId {
    let sc = Scenario::builder()
        .turn(Turn::builder().say("done"))
        .build();
    let mut req = req.unwrap_or_else(|| create(d, "claude", &sc));
    req.first_prompt = Some("go".into());
    let id = c.create(req).await.unwrap().id;
    let mut sub = c.subscribe_session(&id, None);
    loop {
        if is_turn_completed(&next_session_event(&mut sub).await) {
            return id;
        }
    }
}

/// Every line of `source` generation `gen` read in chunks of `max` bytes; the number of chunks.
async fn read_all(c: &RuntimeClient, source: &str, gen: u64, max: u32) -> (String, usize) {
    let (mut text, mut offset, mut pages) = (String::new(), 0u64, 0usize);
    loop {
        let chunk = c.diag_read(source, gen, offset, max).await.unwrap();
        assert!(!chunk.gone, "{source} gen {gen} is gone");
        assert_eq!(chunk.next, offset + chunk.data.len() as u64);
        assert!(
            chunk.data.is_empty() || chunk.data.ends_with('\n'),
            "a chunk holds whole lines: {:?}",
            chunk.data
        );
        pages += 1;
        text.push_str(&chunk.data);
        offset = chunk.next;
        if chunk.eof {
            return (text, pages);
        }
        assert!(!chunk.data.is_empty(), "no progress before the end");
    }
}

fn lines(text: &str) -> Vec<Value> {
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sources_list_the_files_and_reads_return_whole_lines() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let _daemon = start_daemon(&data).await;
    let c = client(&data);
    let id = session_with_a_turn(&c, d.path(), None).await;

    let sent = wall_ms();
    let s = c.diag_sources().await.unwrap();
    let got = wall_ms();
    // The host's clock, taken while the request was in flight.
    assert!(
        (sent..=got).contains(&s.now_ms),
        "{} not in {sent}..={got}",
        s.now_ms
    );
    let file = data.join("diag/daemon.ndjson");
    let on_disk = std::fs::read_to_string(&file).unwrap();
    let header: Value = serde_json::from_str(on_disk.lines().next().unwrap()).unwrap();
    assert_eq!(s.boot, header["b"].as_str().unwrap());

    let names: Vec<&str> = s.sources.iter().map(|x| x.name.as_str()).collect();
    assert_eq!(names, ["daemon", format!("shim:{id}").as_str()]);
    let daemon = &s.sources[0].gens;
    assert_eq!(daemon.len(), 1, "{daemon:?}");
    let g = &daemon[0];
    assert_eq!(Some(g.gen), header["gen"].as_u64());
    assert!(
        g.bytes > 0 && g.bytes <= std::fs::metadata(&file).unwrap().len(),
        "{g:?}"
    );
    let (first, last) = (g.first_t.unwrap(), g.last_t.unwrap());
    assert!(
        first <= last && last <= s.now_ms && s.now_ms - first < 5 * 60_000,
        "{g:?} at {}",
        s.now_ms
    );
    let shim = &s.sources[1].gens;
    assert_eq!(shim.len(), 1, "{shim:?}");
    assert!(
        shim[0].bytes > 0 && shim[0].last_t.unwrap() <= s.now_ms,
        "{shim:?}"
    );

    // Paged through in small chunks: whole lines, each page continuing the last, the end
    // reported, and what the file holds (the daemon keeps writing, so a prefix of it).
    let (text, pages) = read_all(&c, "daemon", g.gen, 1024).await;
    assert!(pages >= 2, "only {pages} page(s) of {} bytes", text.len());
    assert!(
        std::fs::read_to_string(&file).unwrap().starts_with(&text),
        "the pages are not the file"
    );
    let records = lines(&text);
    assert_eq!(records[0]["diag_file"], 1);
    assert!(records.iter().any(|r| r["ev"] == "hostd.start"));
    assert!(records
        .iter()
        .any(|r| r["ev"] == "hostd.request" && r["method"] == "sessions.create"));
    // One chunk of everything gives the same bytes; reading from the middle of the file starts
    // at a line.
    let all = c.diag_read("daemon", g.gen, 0, 1 << 20).await.unwrap();
    assert!(all.eof && all.data.starts_with(&text));
    let second = text.find('\n').unwrap() as u64 + 1;
    let tail = c.diag_read("daemon", g.gen, second, 1 << 20).await.unwrap();
    assert!(tail.data.starts_with("{\"b\""), "{:?}", tail.data);
    // Past the end: nothing, at the end.
    let beyond = c
        .diag_read("daemon", g.gen, all.next + 10_000_000, 1024)
        .await
        .unwrap();
    assert!(
        (beyond.data.as_str(), beyond.eof, beyond.gone) == ("", true, false),
        "{beyond:?}"
    );
    // A generation that does not exist (what a rotated-away one looks like: see
    // `diag_service_rotation.rs` for the real rotation).
    let gone = c.diag_read("daemon", g.gen + 100, 0, 1024).await.unwrap();
    assert!((gone.data.as_str(), gone.gone) == ("", true), "{gone:?}");

    // The session's shim log: its own diagnostics records.
    let src = format!("shim:{id}");
    let (text, _) = read_all(&c, &src, shim[0].gen, 1 << 20).await;
    let records = lines(&text);
    assert_eq!(
        (records[0]["diag_file"].as_u64(), records[0]["src"].as_str()),
        (Some(1), Some("shim"))
    );
    assert!(records.iter().any(|r| r["ev"] == "shim.start"), "{text}");
}

/// Names the service must refuse: not listed by `diag.sources`, path-like, the session journal,
/// the shim's raw stderr file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_refuse_everything_but_the_listed_sources() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let _daemon = start_daemon(&data).await;
    let c = client(&data);
    let id = session_with_a_turn(&c, d.path(), None).await;
    let dir = data.join("sessions").join(id.as_str());
    // The files exist, so refusing them is not an accident of their absence.
    for f in ["journal.ndjson", "session.json", "shim.stderr", "shim.log"] {
        assert!(dir.join(f).exists(), "{f}");
    }
    let gen = c.diag_sources().await.unwrap().sources[0].gens[0].gen;

    let other = SessionId::random();
    let bad = [
        "nope".to_string(),
        String::new(),
        "daemon.ndjson".into(),
        "daemon.1".into(),
        "daemon/../daemon".into(),
        "../diag/daemon".into(),
        "..\\diag\\daemon".into(),
        "/etc/passwd".into(),
        ".daemon".into(),
        "aux".into(),
        "journal".into(),
        "journal.ndjson".into(),
        "session.json".into(),
        "shim.log".into(),
        "shim.stderr".into(),
        "shim:".into(),
        "shim:..".into(),
        "shim:../x".into(),
        "shim:AAAAAAAAAAAA".into(),
        format!("shim:{other}"),
        format!("shim:{id}/../journal.ndjson"),
        format!("shim:{id}/journal.ndjson"),
        format!("shim:{id}.stderr"),
        format!("shim:{id}:journal"),
        format!("shim:{id} "),
        format!("shim:{id}\0"),
        format!("SHIM:{id}"),
        dir.join("journal.ndjson").display().to_string(),
        dir.join("shim.stderr").display().to_string(),
        data.join("diag/daemon.ndjson").display().to_string(),
    ];
    for source in bad {
        for g in [0, gen] {
            let err = c.diag_read(&source, g, 0, 4096).await.unwrap_err();
            assert!(
                matches!(&err, ClientError::Runtime(RuntimeError::InvalidRequest(_))),
                "{source:?} gen {g}: {err:?}"
            );
        }
    }
    // The request's own limits.
    for max in [0, DIAG_READ_MAX + 1, u32::MAX] {
        let err = c.diag_read("daemon", gen, 0, max).await.unwrap_err();
        assert!(
            matches!(&err, ClientError::Runtime(RuntimeError::InvalidRequest(_))),
            "{max}: {err:?}"
        );
    }
    // The largest chunk is allowed.
    let big = c.diag_read("daemon", gen, 0, DIAG_READ_MAX).await.unwrap();
    assert!(big.eof && !big.data.is_empty());
}

async fn next_tail(sub: &mut TypedSubscription<DiagTailEvent>) -> (Cursor, DiagTailEvent) {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok(x))) => x,
        other => panic!(
            "no tail event: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

fn reset_records(ev: DiagTailEvent) -> Vec<Value> {
    match ev {
        DiagTailEvent::Reset { records } => records
            .iter()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect(),
        other => panic!("not a reset: {other:?}"),
    }
}

fn record_line(ev: DiagTailEvent) -> Value {
    match ev {
        DiagTailEvent::Record { line } => serde_json::from_str(&line).unwrap(),
        other => panic!("not a record: {other:?}"),
    }
}

fn is_host_info(r: &Value) -> bool {
    r["ev"] == "hostd.request" && r["method"] == "host.info"
}

/// A reset, then every record produced while the subscriber reads — each once, in order, with
/// no gap — then a resume from a cursor, from the head and from cursors the ring cannot honour.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tail_resets_then_follows_without_gap_and_resumes_by_cursor() {
    const N: usize = 300;
    let d = TempDir::new();
    let data = d.path().join("d");
    let _daemon = start_daemon(&data).await;
    let c = client(&data);
    c.host_info().await.unwrap();
    // Records are being produced while the subscription is made and read.
    let producer = {
        let c = c.clone();
        tokio::spawn(async move {
            for _ in 0..N {
                c.host_info().await.unwrap();
            }
        })
    };
    let mut sub = c.subscribe_diag_tail(&DiagTailParams::default(), None);
    let (cursor, ev) = next_tail(&mut sub).await;
    let reset = reset_records(ev);
    let boot = reset[0]["b"].as_str().unwrap().to_owned();
    let last_seq = reset.last().unwrap()["seq"].as_u64().unwrap();
    assert_eq!(cursor.as_str(), format!("{boot}.{last_seq}"));
    // The ring from the first record of the process, consecutive.
    assert_eq!(reset[0]["ev"], "process.start");
    let mut seqs: Vec<u64> = reset.iter().map(|r| r["seq"].as_u64().unwrap()).collect();
    let mut host_infos = reset.iter().filter(|r| is_host_info(r)).count();
    let mut after_reset: Vec<(Cursor, Value)> = Vec::new();
    while host_infos < N + 1 {
        let (cursor, ev) = next_tail(&mut sub).await;
        let r = record_line(ev);
        assert_eq!(cursor.as_str(), format!("{boot}.{}", r["seq"]));
        host_infos += usize::from(is_host_info(&r));
        seqs.push(r["seq"].as_u64().unwrap());
        after_reset.push((cursor, r));
    }
    producer.await.unwrap();
    assert_eq!(host_infos, N + 1, "a record was delivered twice");
    assert_eq!(
        seqs,
        (seqs[0]..seqs[0] + seqs.len() as u64).collect::<Vec<_>>(),
        "a gap or a duplicate between the reset and the live records"
    );
    assert_eq!(seqs[0], 1);
    assert!(!after_reset.is_empty(), "the test never left the reset");
    drop(sub);

    // Resume from the middle of what was delivered: exactly the rest, no reset.
    let mid = after_reset.len() / 2;
    let mut sub =
        c.subscribe_diag_tail(&DiagTailParams::default(), Some(after_reset[mid].0.clone()));
    for (cursor, want) in &after_reset[mid + 1..] {
        let (got_cursor, ev) = next_tail(&mut sub).await;
        assert_eq!((&got_cursor, record_line(ev)), (cursor, want.clone()));
    }
    drop(sub);

    // From the head: nothing now (no reset), the next record when there is one.
    let head = after_reset.last().unwrap().0.clone();
    let mut sub = c.subscribe_diag_tail(&DiagTailParams::default(), Some(head.clone()));
    c.host_info().await.unwrap();
    let (cursor, ev) = next_tail(&mut sub).await;
    let r = record_line(ev);
    assert!(is_host_info(&r), "{r}");
    assert_eq!(r["seq"].as_u64().unwrap(), seqs.last().unwrap() + 1);
    assert_eq!(cursor.as_str(), format!("{boot}.{}", r["seq"]));
    drop(sub);

    // Cursors the ring cannot honour start with a reset of what it holds: another boot, one
    // from the future, one that is no cursor.
    let future = format!("{boot}.{}", seqs.last().unwrap() + 100_000);
    for bad in [
        "ffffffff.5".to_string(),
        future,
        "garbage".into(),
        String::new(),
        format!("{boot}.x"),
    ] {
        let mut sub =
            c.subscribe_diag_tail(&DiagTailParams::default(), Some(Cursor::new(bad.clone())));
        let (_, ev) = next_tail(&mut sub).await;
        let records = reset_records(ev);
        assert_eq!(records[0]["ev"], "process.start", "{bad:?}");
        assert!(records.len() > N, "{bad:?}: {}", records.len());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tail_filters_by_event_prefix_session_and_trace() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let _daemon = start_daemon(&data).await;
    let c = client(&data);
    let id = session_with_a_turn(&c, d.path(), None).await;
    let t1 = "u-filter-0000001";
    let t2 = "u-filter-0000002";
    // A traced prompt, a traced request about nothing, a command the session refuses before
    // journaling (`session.command_refused`, with the session), and untraced noise.
    let ct = c.clone().with_trace(trace(t1));
    ct.command(&id, Command::prompt("traced")).await.unwrap();
    c.clone().with_trace(trace(t2)).host_info().await.unwrap();
    let err = c.command(&id, Command::Shutdown).await.unwrap_err();
    assert!(matches!(err, ClientError::Command(_)), "{err:?}");
    c.host_info().await.unwrap();

    // By trace: the `rpc.traced` record of that trace and of no other.
    let params = DiagTailParams {
        traces: vec![trace(t1)],
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    let records = reset_records(next_tail(&mut sub).await.1);
    assert!(!records.is_empty());
    assert!(records.iter().all(|r| r["trace"] == t1), "{records:?}");
    let traced: Vec<&Value> = records.iter().filter(|r| r["ev"] == "rpc.traced").collect();
    assert_eq!(traced.len(), 1, "{records:?}");
    assert_eq!(
        (traced[0]["method"].as_str(), traced[0]["outcome"].as_str()),
        (Some("session.command"), Some("ok"))
    );
    // ... and live: a record of another trace and untraced ones are not delivered, this one is.
    let t3 = "u-filter-0000003";
    c.clone().with_trace(trace(t2)).host_info().await.unwrap();
    c.host_info().await.unwrap();
    c.clone().with_trace(trace(t3)).host_info().await.unwrap();
    ct.host_info().await.unwrap();
    let r = record_line(next_tail(&mut sub).await.1);
    assert_eq!(
        (r["ev"].as_str(), r["trace"].as_str(), r["method"].as_str()),
        (Some("rpc.traced"), Some(t1), Some("host.info"))
    );
    drop(sub);
    // Several traces at once.
    let params = DiagTailParams {
        traces: vec![trace(t2), trace(t3)],
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    let records = reset_records(next_tail(&mut sub).await.1);
    let seen: Vec<&str> = records
        .iter()
        .map(|r| r["trace"].as_str().unwrap())
        .collect();
    assert_eq!(seen, [t2, t2, t3], "{records:?}");
    drop(sub);

    // By session: what the daemon recorded about this session, nothing about others.
    let params = DiagTailParams {
        session: Some(id.clone()),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    let records = reset_records(next_tail(&mut sub).await.1);
    assert!(
        records.iter().all(|r| r["session"] == id.as_str()),
        "{records:?}"
    );
    assert!(
        records.iter().any(|r| r["ev"] == "session.command_refused"),
        "{records:?}"
    );
    let other = DiagTailParams {
        session: Some(SessionId::random()),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&other, None);
    assert!(reset_records(next_tail(&mut sub).await.1).is_empty());
    drop(sub);

    // By event prefix, and combined with a session.
    let params = DiagTailParams {
        ev_prefix: Some("hostd.req".into()),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    let records = reset_records(next_tail(&mut sub).await.1);
    assert!(records.len() > 4 && records.iter().all(|r| r["ev"] == "hostd.request"));
    let params = DiagTailParams {
        ev_prefix: Some("rpc.".into()),
        session: Some(id.clone()),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    assert!(reset_records(next_tail(&mut sub).await.1).is_empty());
    // By host: a record names a host only when it is about one; this daemon's own start does.
    let mut all = c.subscribe_diag_tail(&DiagTailParams::default(), None);
    let all = reset_records(next_tail(&mut all).await.1);
    let me = all.iter().find(|r| r["ev"] == "hostd.start").unwrap()["host"]
        .as_str()
        .unwrap()
        .to_owned();
    let params = DiagTailParams {
        host: Some(me.clone()),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    let records = reset_records(next_tail(&mut sub).await.1);
    assert!(
        !records.is_empty() && records.iter().all(|r| r["host"] == me.as_str()),
        "{records:?}"
    );
    drop(sub);

    // More than 64 traces, and a filter that is not one: refused, not truncated.
    let params = DiagTailParams {
        traces: (0..65).map(|i| trace(&format!("u-many-{i:08}"))).collect(),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Err(ClientError::Runtime(RuntimeError::InvalidRequest(_))))) => {}
        other => panic!(
            "not refused: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c))),
        ),
    }
    let params = DiagTailParams {
        ev_prefix: Some("x".repeat(129)),
        ..DiagTailParams::default()
    };
    let mut sub = c.subscribe_diag_tail(&params, None);
    assert!(matches!(
        tokio::time::timeout(T, sub.next()).await,
        Ok(Some(Err(ClientError::Runtime(
            RuntimeError::InvalidRequest(_)
        ))))
    ));
}

/// Markers in a session's environment, MCP server (env and headers) and directory name are in no
/// file under `diag/`, in none of the shim's, and in nothing `diag.read` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_secrets_and_directories_are_in_no_log() {
    const ENV: &str = "ENV-SECRET-8c1f2a";
    const MCP_ENV: &str = "MCP-ENV-SECRET-4d9e7b";
    const MCP_HEADER: &str = "MCP-HEADER-SECRET-b30a55";
    const DIR: &str = "PROJECT-DIR-MARKER-67aa01";
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let c = client(&data);
    let sc = Scenario::builder()
        .turn(Turn::builder().say("done"))
        .build();
    let mut req = create(d.path(), "claude", &sc);
    let cwd = d.path().join(DIR);
    std::fs::create_dir_all(&cwd).unwrap();
    req.cwd = Some(cwd.to_string_lossy().into_owned());
    req.config.env.push(("MY_TOKEN".into(), ENV.into()));
    req.config.mcp_servers = vec![
        McpServerConfig {
            name: "local".into(),
            transport: McpTransport::Stdio {
                command: "true".into(),
                args: vec![],
                env: [("API_TOKEN".to_string(), MCP_ENV.to_string())].into(),
            },
            trusted: false,
        },
        McpServerConfig {
            name: "remote".into(),
            transport: McpTransport::Http {
                url: "http://127.0.0.1:9/mcp".into(),
                headers: [("Authorization".to_string(), MCP_HEADER.to_string())].into(),
            },
            trusted: false,
        },
    ];
    let id = session_with_a_turn(&c, d.path(), Some(req)).await;
    // A refused request about the session and a traced one, for records that name them.
    let _ = c.command(&id, Command::Shutdown).await;
    c.clone()
        .with_trace(trace("u-secret-000001"))
        .host_info()
        .await
        .unwrap();

    // What the service reads, every source, every generation.
    let s = c.diag_sources().await.unwrap();
    let mut read_text = String::new();
    for src in &s.sources {
        for g in &src.gens {
            read_text.push_str(&read_all(&c, &src.name, g.gen, 1 << 20).await.0);
        }
    }
    assert!(read_text.contains("hostd.start") && read_text.contains("shim.start"));
    // And the files themselves, after the daemon wrote what it had.
    kill_with("TERM", daemon.id().unwrap());
    tokio::time::timeout(T, daemon.wait())
        .await
        .unwrap()
        .unwrap();
    let mut files = String::new();
    for dir in [data.join("diag"), data.join("sessions").join(id.as_str())] {
        for ent in std::fs::read_dir(dir).unwrap() {
            let p = ent.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            // The journal and the session file hold the content of the session by design; they
            // are never served.
            if p.is_file()
                && (name.starts_with("daemon")
                    || name.starts_with("shim.log")
                    || name == "aux.ndjson")
            {
                files.push_str(&std::fs::read_to_string(&p).unwrap());
            }
        }
    }
    assert!(files.contains("\"ev\":\"hostd.start\""));
    // The markers did reach the host and its session store, so their absence above is not an
    // accident of the test.
    let store =
        std::fs::read_to_string(data.join("sessions").join(id.as_str()).join("session.json"))
            .unwrap();
    for marker in [ENV, MCP_ENV, MCP_HEADER, DIR] {
        assert!(
            store.contains(marker),
            "{marker} never reached session.json"
        );
    }
    for marker in [ENV, MCP_ENV, MCP_HEADER, DIR] {
        assert!(!read_text.contains(marker), "{marker} was served");
        assert!(!files.contains(marker), "{marker} is in a log file");
    }
}
