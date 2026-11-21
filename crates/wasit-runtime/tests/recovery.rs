//! Recovery, continuation, history and fan-out behaviour of the runtime (real shims, fake CLI).

mod common;

use std::collections::HashSet;
use std::time::Duration;

use common::*;
use serde_json::json;
use wasit_core::{AgentEvent, Command, GateChoiceKind, GateDecision};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::{Phase, SessionEvent, SessionEventKind, SessionSnapshot};
use wasit_runtime::{SessionManager, StreamEnd};

fn gate_scenario() -> Scenario {
    Scenario::builder()
        .turn(
            Turn::builder()
                .say("let me run something")
                .tool("Bash", json!({"command": "make"}))
                .needs_permission()
                .result_if_allowed("built")
                .done()
                .say("all done"),
        )
        .build()
}

fn comparable(s: &SessionSnapshot) -> SessionSnapshot {
    let mut s = s.clone();
    s.loaded = true;
    s
}

/// Drop the whole manager while a turn waits on a gate; a new manager over the same data dir
/// rebuilds the identical state by replay, the gate is answerable, the turn completes, and a
/// subscriber resuming from its last cursor sees no gap and no duplicate. Nothing is written to
/// the CLI twice.
async fn restart_mid_turn(driver: &str) {
    let d = TempDir::new();
    let transcript = d.path().join("fake-transcript.ndjson");
    let mut req = create(d.path(), driver, &gate_scenario());
    req.config.env.push((
        "WASIT_FAKE_TRANSCRIPT".into(),
        transcript.to_string_lossy().into_owned(),
    ));
    let (id, before, seen) = {
        let mgr = manager(d.path()).await;
        let id = mgr.create(req).await.unwrap().id;
        wait_phase(&mgr, &id, "idle").await;
        let mut s = mgr.subscribe(&id, None).await.unwrap();
        mgr.command(&id, Command::prompt("build it")).await.unwrap();
        let snap = wait_phase(&mgr, &id, "needs_you").await;
        let seen = until(&mut s, |e| e.cursor >= snap.cursor).await;
        (id, snap, seen)
        // `mgr` dropped here: actors stop, shims keep running.
    };
    let last = seen.last().unwrap().cursor;
    let mgr = manager(d.path()).await;
    let report = mgr.adopt_all().await.unwrap();
    assert_eq!(report.running, vec![id.clone()]);
    let after = wait_snapshot(&mgr, &id, "replayed snapshot", |s| {
        s.loaded && s.cursor == before.cursor
    })
    .await;
    assert_eq!(comparable(&after), comparable(&before));
    assert_eq!(after.phase, Phase::NeedsYou);

    let mut s = mgr.subscribe(&id, Some(last)).await.unwrap();
    let gate = after.pending_gates[0].gate.clone();
    let decision =
        GateDecision::by_kind(&gate, &[GateChoiceKind::AllowOnce]).expect("an allow choice");
    mgr.command(
        &id,
        Command::AnswerGate {
            request_id: gate.request_id().into(),
            decision,
        },
    )
    .await
    .unwrap();
    let resumed = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&resumed).contains("all done"));
    wait_phase(&mgr, &id, "idle").await;

    // No gap, no duplicate: old prefix + resumed suffix == the full history.
    let full = mgr.history(&id, None, 2000).await.unwrap().events;
    let mut joined: Vec<SessionEvent> = seen.clone();
    joined.extend(resumed.iter().cloned());
    assert_eq!(&full[..joined.len()], &joined[..]);
    assert_monotonic(&full);

    // The fake received every request exactly once (replay re-wrote nothing).
    let text = std::fs::read_to_string(&transcript).unwrap();
    let ins: Vec<String> = text
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v["dir"] == "in")
        .map(|v| v["msg"].to_string())
        .collect();
    let unique: HashSet<&String> = ins.iter().collect();
    assert_eq!(
        unique.len(),
        ins.len(),
        "duplicate writes to the CLI: {ins:#?}"
    );
}

#[tokio::test]
async fn claude_restart_mid_turn() {
    restart_mid_turn("claude").await
}
#[tokio::test]
async fn codex_restart_mid_turn() {
    restart_mid_turn("codex").await
}
#[tokio::test]
async fn opencode_restart_mid_turn() {
    restart_mid_turn("opencode").await
}
#[tokio::test]
async fn kilo_restart_mid_turn() {
    restart_mid_turn("kilo").await
}

/// A command whose effects the previous daemon never wrote is written after adoption: the
/// first prompt is queued until the handshake, and the daemon is dropped before the CLI answers
/// the handshake... approximated by dropping right after create.
#[tokio::test]
async fn restart_right_after_create_still_handshakes_and_runs_first_prompt() {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("first reply"))
        .build();
    let mut req = create(d.path(), "claude", &sc);
    req.first_prompt = Some("hello".into());
    let id = {
        let mgr = manager(d.path()).await;
        mgr.create(req).await.unwrap().id
    };
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&events).contains("first reply"));
    assert_eq!(
        events
            .iter()
            .filter(|e| is_agent(e, |a| matches!(a, AgentEvent::TurnStarted { .. })))
            .count(),
        1
    );
}

/// Pids of the processes whose command line mentions `dir` (the session's shim).
fn shim_pids(dir: &std::path::Path) -> Vec<u32> {
    #[cfg(unix)]
    let out = std::process::Command::new("pgrep")
        .args(["-f", &dir.display().to_string()])
        .output()
        .unwrap();
    #[cfg(windows)]
    let out = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            &format!(
                "Get-CimInstance Win32_Process | Where-Object {{ $_.CommandLine -like '*{}*' -and $_.Name -like 'wasit-hostd*' }} | ForEach-Object {{ $_.ProcessId }}",
                dir.display()
            ),
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

#[tokio::test]
async fn killing_the_shim_makes_the_session_lost() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = mgr
        .create(create(d.path(), "codex", &Scenario::default()))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    let sdir = d.path().join("sessions").join(id.as_str());
    let pids = shim_pids(&sdir);
    assert!(!pids.is_empty(), "no shim process found");

    for p in pids {
        kill_pid(p.into());
    }
    let snap = wait_phase(&mgr, &id, "lost").await;
    assert!(snap.pending_gates.is_empty() && snap.turn.is_none());
    let r = mgr.command(&id, Command::prompt("x")).await;
    assert!(r.is_err());
    // A new manager agrees (from the journal alone).
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "lost after adopt", |s| {
        s.loaded && matches!(s.phase, Phase::Lost { .. })
    })
    .await;
}

#[tokio::test]
async fn continue_starts_a_second_generation_with_monotonic_history() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("one"))
        .turn(Turn::builder().say("two"))
        .build();
    let id = mgr
        .create(create(d.path(), "claude", &sc))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("a")).await.unwrap();
    until(&mut s, is_turn_completed).await;
    let native = mgr.snapshot(&id).unwrap().native_session_id.unwrap();
    // Continue is refused while running.
    assert!(mgr.continue_session(&id).await.is_err());
    mgr.stop(&id).await.unwrap();
    wait_phase(&mgr, &id, "exited").await;
    mgr.continue_session(&id).await.unwrap();
    let snap = wait_snapshot(&mgr, &id, "generation 2 idle", |s| {
        s.generation == 2 && s.phase == Phase::Idle
    })
    .await;
    assert_eq!(snap.native_session_id.as_deref(), Some(native.as_str()));
    mgr.command(&id, Command::prompt("b")).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&events).contains("one"));
    let full = mgr.history(&id, None, 2000).await.unwrap();
    assert!(!full.has_more);
    assert_monotonic(&full.events);
    let spawned = full
        .events
        .iter()
        .filter(|e| matches!(e.kind, SessionEventKind::Spawned { .. }))
        .count();
    assert_eq!(spawned, 2);
    // The second generation resumed the CLI's conversation.
    let argv = full
        .events
        .iter()
        .rev()
        .find_map(|e| match &e.kind {
            SessionEventKind::Spawned { argv, .. } => Some(argv.clone()),
            _ => None,
        })
        .unwrap();
    assert!(argv
        .windows(2)
        .any(|w| w[0] == "--resume" && w[1] == native));
    // Re-adoption replays both generations to the same snapshot.
    let before = mgr.snapshot(&id).unwrap();
    drop(s);
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let after = wait_snapshot(&mgr, &id, "replayed", |s| s.cursor == before.cursor).await;
    assert_eq!(comparable(&after), comparable(&before));
}

#[tokio::test]
async fn history_pages_match_the_full_stream_with_a_tiny_tail() {
    let d = TempDir::new();
    let mut cfg = config(d.path(), timeouts());
    cfg.event_tail = 8;
    let mgr = SessionManager::new(cfg).await.unwrap();
    let mut b = Scenario::builder();
    for i in 0..5 {
        b = b.turn(Turn::builder().say_chunked(format!("reply number {i} with words"), 4));
    }
    let id = mgr
        .create(create(d.path(), "opencode", &b.build()))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    for i in 0..5 {
        mgr.command(&id, Command::prompt(format!("p{i}")))
            .await
            .unwrap();
        until(&mut s, is_turn_completed).await;
    }
    wait_phase(&mgr, &id, "idle").await;
    // The reference: a fresh subscriber from the beginning (journal replay for the evicted
    // prefix, then the in-memory tail, then live).
    let mut all = mgr.subscribe(&id, None).await.unwrap();
    let cursor = mgr.snapshot(&id).unwrap().cursor;
    let reference = until(&mut all, |e| e.cursor >= cursor).await;
    assert_monotonic(&reference);
    assert!(reference.len() > 40, "{}", reference.len());
    // Page backwards with an odd page size.
    let mut pages: Vec<SessionEvent> = Vec::new();
    let mut before = None;
    loop {
        let p = mgr.history(&id, before, 7).await.unwrap();
        assert!(p.events.len() <= 7);
        let mut evs = p.events;
        before = evs.first().map(|e| e.cursor);
        evs.extend(pages);
        pages = evs;
        if !p.has_more {
            break;
        }
    }
    assert_eq!(pages, reference);
    // A page fully inside the tail and one straddling it agree with the reference too.
    let mid = reference[reference.len() - 3].cursor;
    let p = mgr.history(&id, Some(mid), 5).await.unwrap();
    let i = reference.iter().position(|e| e.cursor == mid).unwrap();
    assert_eq!(p.events, reference[i - 5..i]);
}

#[tokio::test]
async fn slow_subscriber_is_dropped_not_buffered() {
    let d = TempDir::new();
    let mut cfg = config(d.path(), timeouts());
    cfg.subscriber_queue = 4;
    let mgr = SessionManager::new(cfg).await.unwrap();
    let sc = Scenario::builder()
        .turn(Turn::builder().say_chunked("a b c d e f g h i j k l m n o p q r s t", 20))
        .build();
    let id = mgr.create(create(d.path(), "codex", &sc)).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    let cursor = mgr.snapshot(&id).unwrap().cursor;
    let mut slow = mgr.subscribe(&id, Some(cursor)).await.unwrap();
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    // The session is not slowed down by a subscriber that does not read.
    let done = wait_snapshot(&mgr, &id, "turn done", |s| s.turns_completed == 1).await;
    let got: Vec<SessionEvent> = mgr
        .history(&id, None, 2000)
        .await
        .unwrap()
        .events
        .into_iter()
        .filter(|e| e.cursor > cursor && e.cursor <= done.cursor)
        .collect();
    assert!(got.len() > 8, "{}", got.len());
    // The slow reader gets at most its queue, then `Lagged`.
    let mut n: usize = 0;
    loop {
        match tokio::time::timeout(T, slow.next()).await.unwrap() {
            Some(Ok(e)) => {
                assert_eq!(e, got[n]);
                n += 1
            }
            Some(Err(StreamEnd::Lagged)) => break,
            other => panic!("expected Lagged, got {other:?}"),
        }
    }
    // (Its backlog at subscribe time plus a queue of 4: far less than the turn.)
    assert!(n < got.len(), "{n} of {} events buffered", got.len());
    // Resubscribing from the last cursor it saw continues seamlessly.
    let from = if n == 0 { cursor } else { got[n - 1].cursor };
    let mut again = mgr.subscribe(&id, Some(from)).await.unwrap();
    let rest = until(&mut again, |e| e.cursor >= done.cursor).await;
    assert_eq!(&got[n..], &rest[..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_sessions_stay_responsive_with_one_hung() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let hung_sc = Scenario::builder().turn(Turn::builder().hang()).build();
    let ok_sc = Scenario::builder()
        .turn(Turn::builder().say("pong"))
        .build();
    let hung = mgr
        .create(create(d.path(), "claude", &hung_sc))
        .await
        .unwrap()
        .id;
    let mut ids = Vec::new();
    for i in 0..39 {
        let driver = DRIVERS[i % 4];
        ids.push(
            mgr.create(create(d.path(), driver, &ok_sc))
                .await
                .unwrap()
                .id,
        );
    }
    wait_phase(&mgr, &hung, "idle").await;
    mgr.command(&hung, Command::prompt("hang")).await.unwrap();
    wait_phase(&mgr, &hung, "working").await;
    for id in &ids {
        wait_phase(&mgr, id, "idle").await;
    }
    let started = std::time::Instant::now();
    let mut tasks = Vec::new();
    for id in ids.clone() {
        let mgr = mgr.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = mgr
                .subscribe(&id, Some(mgr.snapshot(&id).unwrap().cursor))
                .await
                .unwrap();
            mgr.command(&id, Command::prompt("ping")).await.unwrap();
            let ev = until(&mut s, is_turn_completed).await;
            assert!(assistant_text(&ev).contains("pong"));
        }));
    }
    // The hung session accepts (and journals) commands without holding anyone up.
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        mgr.command(&hung, Command::prompt("more")),
    )
    .await
    .expect("hung session's command was answered");
    assert!(r.is_ok(), "{r:?}");
    for t in tasks {
        tokio::time::timeout(T, t).await.unwrap().unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "fan-out took {:?}",
        started.elapsed()
    );
    assert_eq!(mgr.snapshot(&hung).unwrap().phase, Phase::Working);
    assert_eq!(mgr.list().len(), 40);
}

#[tokio::test]
async fn delete_stops_and_removes_and_host_feed_reports_it() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let mut host = mgr.subscribe_host(None);
    let first = tokio::time::timeout(T, host.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(first.1, wasit_runtime::wire::HostEvent::Reset { ref sessions } if sessions.is_empty())
    );
    let id = mgr
        .create(create(d.path(), "kilo", &Scenario::default()))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    let dir = d.path().join("sessions").join(id.as_str());
    assert!(dir.exists());
    mgr.delete(&id).await.unwrap();
    assert!(!dir.exists());
    assert!(mgr.snapshot(&id).is_err());
    let mut saw_upsert = false;
    let mut last_cursor;
    loop {
        let (c, ev) = tokio::time::timeout(T, host.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        last_cursor = c;
        match ev {
            wasit_runtime::wire::HostEvent::Upsert { session } if session.id == id => {
                saw_upsert = true
            }
            wasit_runtime::wire::HostEvent::Removed { id: gone } if gone == id => break,
            _ => {}
        }
    }
    assert!(saw_upsert);
    // Resuming from the last cursor yields nothing old; a bogus cursor yields a Reset.
    let mut again = mgr.subscribe_host(Some(&last_cursor));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), again.next())
            .await
            .is_err()
    );
    let mut fresh = mgr.subscribe_host(Some("bogus.1"));
    let (_, ev) = tokio::time::timeout(T, fresh.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(ev, wasit_runtime::wire::HostEvent::Reset { ref sessions } if sessions.is_empty())
    );
}
