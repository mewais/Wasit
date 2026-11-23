//! The session trail through the real stack (`SessionManager` → actor → real shim → fake CLI):
//! the trace id of a request on the `input` event and in the trail, the trail of a prompt in
//! every driver, a queued prompt dropped by an interrupt, the trail after a daemon restart, and
//! paging from memory and from a replay.
//!
//! Nothing here waits for time: tests await the events and snapshots the runtime publishes.

mod common;

use common::*;
use serde_json::{json, Value};
use wasit_control::TraceId;
use wasit_core::{AgentEvent, Command, GateChoiceKind, GateDecision, GateRequest};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::{CommandOutcome, SessionEvent, SessionEventKind, SessionId, TrailItem};
use wasit_runtime::SessionManager;

fn trace(s: &str) -> TraceId {
    TraceId::new(s).unwrap()
}

/// The entries of a session's trail, as the JSON a client reads.
fn js(items: &[TrailItem]) -> Vec<Value> {
    items
        .iter()
        .map(|i| serde_json::to_value(i).unwrap())
        .collect()
}

async fn trail(mgr: &SessionManager, id: &SessionId) -> Vec<Value> {
    let page = mgr.trail_page(id, None, 2000).await.unwrap();
    assert!(!page.has_more);
    js(&page.entries)
}

/// The index of the first entry `f` accepts.
fn at(entries: &[Value], what: &str, f: impl Fn(&Value) -> bool) -> usize {
    entries
        .iter()
        .position(f)
        .unwrap_or_else(|| panic!("no {what} in {entries:#?}"))
}

fn is(ev: &'static str) -> impl Fn(&Value) -> bool {
    move |v| v["ev"] == ev
}

fn is_for(ev: &'static str, input: u64) -> impl Fn(&Value) -> bool {
    move |v| v["ev"] == ev && v["input"] == input
}

async fn create_idle(
    mgr: &SessionManager,
    dir: &std::path::Path,
    driver: &str,
    sc: &Scenario,
) -> SessionId {
    let id = mgr.create(create(dir, driver, sc)).await.unwrap().id;
    wait_phase(mgr, &id, "idle").await;
    id
}

fn prompt_input(events: &[SessionEvent], text: &str) -> (u64, Option<String>) {
    events
        .iter()
        .find_map(|e| match &e.kind {
            SessionEventKind::Input {
                input,
                command: Command::Prompt { text: t, .. },
                trace,
                ..
            } if t == text => Some((input.0, trace.as_ref().map(|t| t.as_str().to_owned()))),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no input for {text:?} in {events:#?}"))
}

// ── a prompt ────────────────────────────────────────────────────────────────

/// A prompt travels `input.applied` → written → `turn.started` for its input → `turn.completed` →
/// `phase{idle, turn_completed}`, carrying the request's trace; the same trace is on the
/// `input` event. (Where a driver acknowledges inputs, the acknowledgement follows the write.)
async fn trail_of_a_prompt(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("hello from the fake").usage(10, 5))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    let out = mgr
        .command_traced(&id, Command::prompt("hi"), Some(trace("u-prompt-0000001")))
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let events = until(&mut s, is_turn_completed).await;
    wait_phase(&mgr, &id, "idle").await;
    let (input, event_trace) = prompt_input(&events, "hi");
    assert_eq!(event_trace.as_deref(), Some("u-prompt-0000001"));

    let t = trail(&mgr, &id).await;
    let applied = at(&t, "input.applied", is_for("input.applied", input));
    assert_eq!(t[applied]["kind"], "prompt");
    assert_eq!(t[applied]["status"], "sent");
    assert_eq!(t[applied]["chars"], 2);
    assert_eq!(t[applied]["trace"], "u-prompt-0000001");
    let written = at(&t, "input.written", is_for("input.written", input));
    let started = at(
        &t,
        "turn.started for the input",
        is_for("turn.started", input),
    );
    let completed = at(&t, "turn.completed", is("turn.completed"));
    let idle = at(&t, "phase idle after the turn", |v| {
        v["ev"] == "phase" && v["to"] == "idle" && v["cause"]["kind"] == "turn_completed"
    });
    assert!(applied < written, "{t:#?}");
    assert!(applied < started && started < completed, "{t:#?}");
    assert_eq!(
        idle,
        completed + 1,
        "the phase change follows its cause: {t:#?}"
    );
    assert_eq!(t[idle]["cause"]["interrupted"], false);
    assert_eq!(t[completed]["output_tokens"], 5);
    assert_eq!(t[completed]["interrupted"], false);
    assert!(
        t.iter().filter(|v| v["ev"] == "input.applied").count() == 1,
        "{t:#?}"
    );
    if driver == "claude" {
        // claude's own report that it took the input in.
        let acked = at(
            &t,
            "input.acknowledged",
            is_for("input.acknowledged", input),
        );
        assert!(written < acked, "{t:#?}");
        assert_eq!(t[acked]["steered"], false);
    }
    // The cause of working is the turn, of the first idle the handshake.
    assert!(t.iter().any(|v| v["ev"] == "phase"
        && v["to"] == "working"
        && v["cause"]["kind"] == "turn_started"
        && v["cause"]["input"] == input));
    assert!(t.iter().any(|v| v["ev"] == "phase"
        && v["to"] == "idle"
        && v["cause"]["kind"] == "handshake_done"));
    // Cursors are the events' cursors: strictly increasing, and each entry sits where its event
    // does in the history.
    let cursors: Vec<u64> = t.iter().map(|v| v["cursor"].as_u64().unwrap()).collect();
    assert!(cursors.windows(2).all(|w| w[0] < w[1]), "{cursors:?}");
    let history = mgr.history(&id, None, 2000).await.unwrap().events;
    for c in &cursors {
        assert!(history.iter().any(|e| e.cursor == *c), "{c}");
    }
    mgr.stop(&id).await.unwrap();
}

macro_rules! per_driver {
    ($f:ident => $($name:ident: $driver:literal),+ $(,)?) => {
        $( #[tokio::test] async fn $name() { $f($driver).await } )+
    };
}

per_driver!(trail_of_a_prompt =>
    claude_trail_of_a_prompt: "claude",
    codex_trail_of_a_prompt: "codex",
    opencode_trail_of_a_prompt: "opencode",
    kilo_trail_of_a_prompt: "kilo",
);

// ── the trace id ────────────────────────────────────────────────────────────

/// The first prompt of a session takes the trace of the request that created it; a later
/// command its own; a command without one has none. The same shows in the trail, and after a
/// restart (the journal holds it).
async fn traces_of_requests(driver: &str) {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("one"))
        .turn(Turn::builder().say("two"))
        .turn(Turn::builder().say("three"))
        .build();
    let mut req = create(d.path(), driver, &sc);
    req.first_prompt = Some("first".into());
    let (id, seen) = {
        let mgr = manager(d.path()).await;
        let id = mgr
            .create_traced(req, Some(trace("u-create-0000001")))
            .await
            .unwrap()
            .id;
        let mut s = mgr.subscribe(&id, None).await.unwrap();
        let mut seen = until(&mut s, is_turn_completed).await;
        wait_phase(&mgr, &id, "idle").await;
        mgr.command_traced(
            &id,
            Command::prompt("second"),
            Some(trace("u-second-000001")),
        )
        .await
        .unwrap();
        seen.extend(until(&mut s, is_turn_completed).await);
        mgr.command(&id, Command::prompt("third")).await.unwrap();
        seen.extend(until(&mut s, is_turn_completed).await);
        wait_phase(&mgr, &id, "idle").await;
        (id, seen)
    };
    let want = |text: &str, t: Option<&str>| {
        assert_eq!(prompt_input(&seen, text).1.as_deref(), t, "{text}");
    };
    want("first", Some("u-create-0000001"));
    want("second", Some("u-second-000001"));
    want("third", None);

    // The trail names them, and `process.started` carries the one of the request that started
    // the session.
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "replayed", |s| {
        s.loaded && s.turns_completed == 3
    })
    .await;
    let t = trail(&mgr, &id).await;
    let started = &t[at(&t, "process.started", is("process.started"))];
    assert_eq!(started["trace"], "u-create-0000001");
    let applied: Vec<(Value, Value)> = t
        .iter()
        .filter(|v| v["ev"] == "input.applied")
        .map(|v| {
            (
                v["chars"].clone(),
                v.get("trace").cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    assert_eq!(
        applied,
        [
            (json!(5), json!("u-create-0000001")),
            (json!(6), json!("u-second-000001")),
            (json!(5), Value::Null),
        ]
    );
    assert_eq!(
        started["first_input"],
        t[at(&t, "first input", is("input.applied"))]["input"]
    );
    // And the replayed events carry them too.
    let history = mgr.history(&id, None, 2000).await.unwrap().events;
    assert_eq!(
        prompt_input(&history, "first").1.as_deref(),
        Some("u-create-0000001")
    );
    assert_eq!(
        prompt_input(&history, "second").1.as_deref(),
        Some("u-second-000001")
    );
    assert_eq!(prompt_input(&history, "third").1, None);
}

per_driver!(traces_of_requests =>
    claude_traces_of_requests: "claude",
    codex_traces_of_requests: "codex",
    opencode_traces_of_requests: "opencode",
    kilo_traces_of_requests: "kilo",
);

/// A session created with a trace but without a first prompt still names it (`process.started`).
#[tokio::test]
async fn a_session_created_with_a_trace_and_no_prompt_names_it_on_start() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let req = create(d.path(), "claude", &Scenario::default());
    let id = mgr
        .create_traced(req, Some(trace("h-launch-0000001")))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    let t = trail(&mgr, &id).await;
    let started = &t[at(&t, "process.started", is("process.started"))];
    assert_eq!(started["trace"], "h-launch-0000001");
    assert!(started.get("first_input").is_none());
    assert!(!t.iter().any(is("input.applied")));
}

/// The trace of the request that created a session is that of its first generation: a continued
/// generation starts without one.
#[tokio::test]
async fn a_continued_generation_does_not_inherit_the_trace_of_the_creating_request() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let mut req = create(d.path(), "claude", &Scenario::default());
    // (claude names its conversation with the first turn; a generation continues that one.)
    req.first_prompt = Some("hello".into());
    let id = mgr
        .create_traced(req, Some(trace("h-launch-0000001")))
        .await
        .unwrap()
        .id;
    wait_snapshot(&mgr, &id, "the first turn", |s| {
        s.turns_completed == 1 && s.native_session_id.is_some()
    })
    .await;
    mgr.stop(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "exit", |s| !s.phase.is_live()).await;
    mgr.continue_session(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "second generation", |s| {
        s.generation == 2 && s.phase == wasit_runtime::wire::Phase::Idle
    })
    .await;
    let t = trail(&mgr, &id).await;
    let started: Vec<Option<&str>> = t
        .iter()
        .filter(|v| v["ev"] == "process.started")
        .map(|v| v["trace"].as_str())
        .collect();
    assert_eq!(started, [Some("h-launch-0000001"), None], "{t:#?}");
    mgr.stop(&id).await.unwrap();
}

// ── a queued prompt, an interrupt ───────────────────────────────────────────

/// A prompt the driver holds behind the running turn shows as `input.applied{queued}`; the
/// interrupt that clears it shows as `input.dropped{interrupted}` for the same input, and no turn
/// ever starts for it.
async fn queued_prompt_then_interrupt(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working").wait_for_interrupt())
        .turn(Turn::builder().say("next"))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("long")).await.unwrap();
    until(&mut s, |e| {
        is_agent(e, |a| matches!(a, AgentEvent::AssistantDelta { .. }))
    })
    .await;
    let out = mgr
        .command_traced(
            &id,
            Command::prompt("later"),
            Some(trace("u-later-0000001")),
        )
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Queued);
    mgr.command_traced(&id, Command::Interrupt, Some(trace("u-stop-00000001")))
        .await
        .unwrap();
    let events = until(&mut s, is_turn_completed).await;
    wait_phase(&mgr, &id, "idle").await;
    let (later, _) = prompt_input(&events, "later");

    let t = trail(&mgr, &id).await;
    let queued = at(&t, "the queued prompt", |v| {
        v["ev"] == "input.applied" && v["input"] == later
    });
    assert_eq!(t[queued]["status"], "queued");
    assert_eq!(t[queued]["trace"], "u-later-0000001");
    let dropped = at(&t, "input.dropped", is_for("input.dropped", later));
    assert_eq!(t[dropped]["reason"], "interrupted");
    assert!(queued < dropped, "{t:#?}");
    assert!(
        !t.iter()
            .any(|v| (v["ev"] == "input.sent" || v["ev"] == "turn.started") && v["input"] == later),
        "{t:#?}"
    );
    // The interrupt is an input of its own, with its own trace, and the turn it ended says so.
    let interrupt = at(&t, "the interrupt", |v| {
        v["ev"] == "input.applied" && v["kind"] == "interrupt"
    });
    assert_eq!(t[interrupt]["trace"], "u-stop-00000001");
    assert!(t
        .iter()
        .any(|v| v["ev"] == "turn.completed" && v["interrupted"] == true));
    mgr.stop(&id).await.unwrap();
}

per_driver!(queued_prompt_then_interrupt =>
    claude_queued_prompt_then_interrupt: "claude",
    opencode_queued_prompt_then_interrupt: "opencode",
    kilo_queued_prompt_then_interrupt: "kilo",
);

// ── a daemon restart ────────────────────────────────────────────────────────

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

/// The manager is dropped while a turn waits on a gate; a new one over the same data directory
/// shows the same trail (same entries, same cursors), a subscriber resuming from its last
/// cursor gets neither a gap nor a duplicate, and the trail is the same served from a replay
/// of the journal alone (a one-event memory forces it).
async fn trail_after_a_restart(driver: &str) {
    let d = TempDir::new();
    let (id, before, last) = {
        let mgr = manager(d.path()).await;
        let id = create_idle(&mgr, d.path(), driver, &gate_scenario()).await;
        mgr.command_traced(
            &id,
            Command::prompt("build it"),
            Some(trace("u-build-0000001")),
        )
        .await
        .unwrap();
        let snap = wait_phase(&mgr, &id, "needs_you").await;
        let mut s = mgr.subscribe(&id, None).await.unwrap();
        let seen = until(&mut s, |e| e.cursor >= snap.cursor).await;
        let before = trail(&mgr, &id).await;
        assert!(before.iter().any(is("gate.raised")), "{before:#?}");
        (id, before, seen.last().unwrap().cursor)
        // The manager is dropped: actors stop, the shim keeps the session.
    };

    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "replayed snapshot", |s| {
        s.loaded && s.cursor == last
    })
    .await;
    assert_eq!(
        trail(&mgr, &id).await,
        before,
        "adoption rebuilds the same trail"
    );

    // Resume from the last cursor, finish the turn.
    let mut s = mgr.subscribe(&id, Some(last)).await.unwrap();
    let snap = mgr.snapshot(&id).unwrap();
    let gate: GateRequest = snap.pending_gates[0].gate.clone();
    mgr.command(
        &id,
        Command::AnswerGate {
            request_id: gate.request_id().into(),
            decision: GateDecision::by_kind(&gate, &[GateChoiceKind::AllowOnce]).unwrap(),
        },
    )
    .await
    .unwrap();
    until(&mut s, is_turn_completed).await;
    wait_phase(&mgr, &id, "idle").await;
    let full = trail(&mgr, &id).await;
    // No gap, no duplicate: what was seen is a prefix of the whole.
    assert_eq!(&full[..before.len()], &before[..]);
    let cursors: Vec<u64> = full.iter().map(|v| v["cursor"].as_u64().unwrap()).collect();
    assert!(cursors.windows(2).all(|w| w[0] < w[1]));
    assert!(full.iter().any(|v| v["ev"] == "phase"
        && v["to"] == "idle"
        && v["cause"]["kind"] == "turn_completed"));
    // The answer is an input of its own, with the choice and the gate it names.
    let answer = &full[at(&full, "the answer", |v| {
        v["ev"] == "input.applied" && v["kind"] == "answer_gate"
    })];
    assert_eq!(answer["target"], gate.request_id());
    assert_eq!(answer["status"], "sent");
    mgr.stop(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "exit", |s| !s.phase.is_live()).await;
    let finished = trail(&mgr, &id).await;
    drop(mgr);

    // A manager whose memory holds three events answers from the journal alone.
    let mut cfg = config(d.path(), timeouts());
    cfg.event_tail = 3;
    let small = SessionManager::new(cfg).await.unwrap();
    small.adopt_all().await.unwrap();
    wait_snapshot(&small, &id, "replayed", |s| s.loaded).await;
    assert_eq!(trail(&small, &id).await, finished);
}

per_driver!(trail_after_a_restart =>
    claude_trail_after_a_restart: "claude",
    codex_trail_after_a_restart: "codex",
    opencode_trail_after_a_restart: "opencode",
    kilo_trail_after_a_restart: "kilo",
);

// ── paging ──────────────────────────────────────────────────────────────────

/// Walks a trail backwards, `limit` at a time, with `before` = the oldest cursor so far.
async fn walk(mgr: &SessionManager, id: &SessionId, limit: usize) -> (Vec<Value>, Vec<bool>) {
    let mut all: Vec<Value> = Vec::new();
    let mut more = Vec::new();
    let mut before = None;
    loop {
        let page = mgr.trail_page(id, before, limit).await.unwrap();
        assert!(page.entries.len() <= limit);
        let mut older = js(&page.entries);
        if let Some(first) = page.entries.first() {
            before = Some(first.cursor);
        }
        older.extend(all);
        all = older;
        more.push(page.has_more);
        if !page.has_more {
            return (all, more);
        }
        assert_eq!(page.entries.len(), limit, "a page that has more is full");
    }
}

/// `session.trail.page` semantics: the last `limit` entries before a cursor, oldest first,
/// `has_more` while older ones exist; and the pages are the same from a memory that holds three
/// events (the journal is replayed) as from one that holds everything.
#[tokio::test]
async fn trail_pages_walk_back_to_the_start_from_memory_and_from_a_replay() {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("one").usage(1, 1))
        .turn(Turn::builder().say("two").usage(1, 1))
        .turn(Turn::builder().say("three").usage(1, 1))
        .build();
    let mut cfg = config(d.path(), timeouts());
    cfg.event_tail = 3;
    let (id, small_full) = {
        let small = SessionManager::new(cfg).await.unwrap();
        let id = create_idle(&small, d.path(), "claude", &sc).await;
        let mut s = small.subscribe(&id, None).await.unwrap();
        for p in ["a", "bb", "ccc"] {
            small.command(&id, Command::prompt(p)).await.unwrap();
            until(&mut s, is_turn_completed).await;
            wait_phase(&small, &id, "idle").await;
        }
        let full = trail(&small, &id).await;
        assert!(full.len() > 12, "{full:#?}");
        for limit in [1, 4, 7, 2000] {
            // Served from a replay (three events of memory), walked back to the start.
            let (walked, more) = walk(&small, &id, limit).await;
            assert_eq!(walked, full, "limit {limit}");
            assert_eq!(more.last(), Some(&false));
            assert!(more[..more.len() - 1].iter().all(|m| *m));
        }
        // `before` is exclusive: the entry at the cursor is not in the page before it.
        let cut = full[5]["cursor"].as_u64().unwrap();
        let page = small.trail_page(&id, Some(cut), 3).await.unwrap();
        assert_eq!(js(&page.entries), full[2..5].to_vec());
        assert!(page.has_more);
        let page = small.trail_page(&id, Some(cut), 5).await.unwrap();
        assert_eq!(js(&page.entries), full[..5].to_vec());
        assert!(!page.has_more, "exactly the entries before the cursor");
        let page = small
            .trail_page(&id, Some(full[0]["cursor"].as_u64().unwrap()), 5)
            .await
            .unwrap();
        assert!(page.entries.is_empty() && !page.has_more);
        // A limit of zero or beyond the cap is clamped, not refused.
        assert_eq!(
            small.trail_page(&id, None, 0).await.unwrap().entries.len(),
            1
        );
        assert_eq!(
            small
                .trail_page(&id, None, 100_000)
                .await
                .unwrap()
                .entries
                .len(),
            full.len()
        );
        (id, full)
    };
    // The same session, adopted by a manager whose memory holds the whole journal.
    let big = manager(d.path()).await;
    big.adopt_all().await.unwrap();
    wait_snapshot(&big, &id, "replayed", |s| {
        s.loaded && s.turns_completed == 3
    })
    .await;
    assert_eq!(trail(&big, &id).await, small_full);
    for limit in [1, 5, 9] {
        assert_eq!(walk(&big, &id, limit).await.0, small_full, "limit {limit}");
    }
    big.stop(&id).await.unwrap();
}
