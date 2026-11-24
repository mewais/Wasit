//! The core's handling of a conversation's earlier history: what makes it history, what it never
//! touches, when it is accepted and when it is the same history again, and that a replay of the
//! journal reproduces it exactly.

use std::sync::Arc;

use serde_json::json;
use wasit_core::transcript::{self, Record as T};
use wasit_core::{AgentEvent, DriverDescriptor, HistoryReport, ResumeMode, SpawnSpec};
use wasit_shim::{Kind, Record};

use crate::core::{Conversation, Core, Output, SessionMeta};
use crate::note::{EarlierPlan, RuntimeNote};
use crate::wire::{
    EarlierSource, EarlierState, EarlierUnavailable, SessionEvent, SessionEventKind,
    SessionSnapshot,
};

const CODEX_RESUME: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../wasit-codex/tests/fixtures/resume.ndjson"
));
const ACP_RESUME: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../wasit-opencode/tests/fixtures/resume.ndjson"
));

fn meta(driver: &str, spec: &SpawnSpec) -> SessionMeta {
    SessionMeta {
        id: crate::wire::SessionId("aaaaaaaaaaaa".into()),
        driver: driver.into(),
        cwd: None,
        labels: Default::default(),
        created_at_ms: 0,
        display_name: None,
        command: None,
        conversation: Conversation::of_spec(spec),
        forked_from: None,
    }
}

/// A core fed a journal the way a live actor feeds it, remembering everything it said.
struct Sim {
    core: Core,
    seq: u64,
    journal: Vec<Record>,
    events: Vec<SessionEvent>,
    earlier: Vec<SessionEvent>,
}

impl Sim {
    fn new(desc: Arc<dyn DriverDescriptor>, meta: &SessionMeta) -> Sim {
        Sim {
            core: Core::new(desc, meta),
            seq: 0,
            journal: vec![],
            events: vec![],
            earlier: vec![],
        }
    }

    fn rec(&mut self, kind: Kind, data: serde_json::Value) -> Output {
        self.seq += 1;
        let r = Record {
            seq: self.seq,
            t: 1_000 + self.seq,
            kind,
            data,
            partial: false,
        };
        self.journal.push(r.clone());
        let out = self.core.apply(&r);
        self.events.extend(out.events.clone());
        self.earlier.extend(out.earlier.clone());
        out
    }

    /// The shim journals every line the core asked it to write.
    fn feed(&mut self, kind: Kind, data: serde_json::Value) -> Output {
        let out = self.rec(kind, data);
        for w in out.writes.clone() {
            self.rec(Kind::In, json!(w));
        }
        out
    }

    fn note(&mut self, n: RuntimeNote) -> Output {
        self.feed(Kind::Note, n.to_value())
    }

    fn spawn(&mut self) {
        self.feed(Kind::Spawned, json!({"pid": 7, "argv": ["agent"]}));
    }

    fn start(&mut self, spec: &SpawnSpec, plan: Option<EarlierPlan>) -> Output {
        self.spawn();
        self.note(RuntimeNote::Start {
            spec: Box::new(spec.clone()),
            trace: None,
            earlier: plan,
        })
    }

    fn exit(&mut self) {
        self.feed(Kind::Exited, json!({"code": 0}));
    }

    fn snap(&self) -> SessionSnapshot {
        self.core.snapshot().clone()
    }

    fn earlier_states(&self) -> Vec<EarlierState> {
        self.events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Earlier { earlier } => Some(earlier.state.clone()),
                _ => None,
            })
            .collect()
    }

    /// A fresh core over this journal: the same events, items and snapshot.
    fn assert_replays_exactly(&self, desc: Arc<dyn DriverDescriptor>, meta: &SessionMeta) {
        let mut again = Sim::new(desc, meta);
        for r in &self.journal {
            let out = again.core.apply(r);
            again.events.extend(out.events);
            again.earlier.extend(out.earlier);
        }
        assert_eq!(again.events, self.events, "events");
        assert_eq!(again.earlier, self.earlier, "earlier items");
        assert_eq!(again.snap(), self.snap(), "snapshot");
    }
}

fn history_of(e: &SessionEvent) -> (Option<u64>, &AgentEvent) {
    match &e.kind {
        SessionEventKind::Agent { event } => event.as_history().expect("a history item"),
        other => panic!("not an agent event: {other:?}"),
    }
}

/// The records of a recorded exchange: its start spec and the lines the CLI printed.
fn recording(text: &str) -> (SpawnSpec, Vec<String>) {
    let records = transcript::parse(text).unwrap();
    let spec = records
        .iter()
        .find_map(|r| match r {
            T::Start(s) => Some((**s).clone()),
            _ => None,
        })
        .unwrap();
    let lines = records
        .into_iter()
        .filter_map(|r| match r {
            T::Out(l) => Some(l),
            _ => None,
        })
        .collect();
    (spec, lines)
}

/// The recorded codex `thread/resume` exchange, optionally with the turns taken out of the
/// result (what an agent that sent no history would have printed).
fn codex_run(with_turns: bool) -> (Sim, SessionMeta, SpawnSpec) {
    let (spec, lines) = recording(CODEX_RESUME);
    let m = meta("codex", &spec);
    let mut sim = Sim::new(wasit_codex::descriptor(), &m);
    sim.start(&spec, None);
    for line in lines {
        let line = if with_turns { line } else { strip_turns(&line) };
        sim.feed(Kind::Out, json!(line));
    }
    (sim, m, spec)
}

fn strip_turns(line: &str) -> String {
    let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
    if let Some(turns) = v.pointer_mut("/result/thread/turns") {
        *turns = json!([]);
        // A thread with a first message and no turns would be said to have lost them.
        if let Some(preview) = v.pointer_mut("/result/thread/preview") {
            *preview = json!("");
        }
    }
    v.to_string()
}

/// What the agent replays over its protocol is history: it is kept apart from the live events,
/// and nothing live changes because of it. Compared with the same exchange without the
/// replayed turns, the snapshot differs in `earlier` only: not in usage or cost (the agent
/// reports its totals itself), turns, phase, tasks, gates.
#[test]
fn a_replayed_conversation_is_history_and_changes_no_live_state() {
    let (with, m, _) = codex_run(true);
    let (without, _, _) = codex_run(false);

    // Two items, in order, as history with the agent's own times; none of them is a live event.
    assert_eq!(with.earlier.len(), 2, "{:#?}", with.earlier);
    let (at0, first) = history_of(&with.earlier[0]);
    let (at1, second) = history_of(&with.earlier[1]);
    assert_eq!(
        first,
        &AgentEvent::UserMessage {
            text: "hello".into()
        }
    );
    assert!(
        matches!(second, AgentEvent::AssistantDelta { text, .. } if text == "Hello from fake: hello")
    );
    assert!(at0.is_some() && at1.is_some() && at0 <= at1);
    assert!(with.earlier[0].cursor < with.earlier[1].cursor);
    assert!(
        with.events
            .iter()
            .all(|e| !matches!(&e.kind, SessionEventKind::Agent { event } if event.as_history().is_some())),
        "history in the live stream"
    );
    assert!(without.earlier.is_empty());

    // The snapshot says where the history stands and nothing else of it.
    let a = with.snap();
    let mut b = without.snap();
    assert_eq!(a.earlier.state, EarlierState::Loaded { complete: true });
    assert_eq!(a.earlier.source, Some(EarlierSource::AgentReplay));
    assert_eq!(a.earlier.items, 2);
    assert_eq!(b.earlier.items, 0);
    b.earlier = a.earlier.clone();
    // Live events of the two runs differ in cursors only after the replay point.
    assert_eq!(a.turns_completed, 1);
    assert_eq!(a.usage, b.usage, "history counted into the usage");
    b.cursor = a.cursor;
    assert_eq!(a, b);
    // The stream tells the loading moved, twice, and the second time it is complete.
    assert_eq!(
        with.earlier_states(),
        [
            EarlierState::Loading,
            EarlierState::Loaded { complete: true }
        ]
    );

    with.assert_replays_exactly(wasit_codex::descriptor(), &m);
}

/// The agent replays the whole conversation at every start of the same session (each continue
/// reloads it): what the journal holds is not added again.
#[test]
fn a_reloaded_conversation_is_not_imported_twice() {
    let (mut sim, m, spec) = codex_run(true);
    assert_eq!(sim.snap().earlier.items, 2);
    sim.exit();
    // The next process generation resumes the same conversation (a continue) and the agent
    // replays it all again.
    let (_, lines) = recording(CODEX_RESUME);
    let mut next = spec.clone();
    next.first_prompt = None;
    sim.start(&next, None);
    let before = sim.earlier.len();
    for line in lines.iter().take(12) {
        sim.feed(Kind::Out, json!(line));
    }
    assert_eq!(
        sim.earlier.len(),
        before,
        "the same history was journaled twice"
    );
    let snap = sim.snap();
    assert_eq!(snap.earlier.items, 2);
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: true });
    assert_eq!(snap.earlier.gaps.replays_ignored, 1);
    sim.assert_replays_exactly(wasit_codex::descriptor(), &m);
}

/// A conversation this session started has nothing earlier: when a later generation resumes it
/// (a continue) and the agent replays it all, that is the session's own events, already in the
/// journal, and is not added again.
#[test]
fn a_session_that_started_its_conversation_takes_no_replay() {
    let (spec, lines) = recording(ACP_RESUME);
    let fresh = SpawnSpec {
        resume: ResumeMode::Fresh,
        first_prompt: None,
        ..spec.clone()
    };
    let m = meta("opencode", &fresh);
    let mut sim = Sim::new(wasit_opencode::opencode(), &m);
    sim.start(&fresh, None);
    sim.exit();
    let resumed = SpawnSpec {
        first_prompt: None,
        ..spec
    };
    sim.start(&resumed, None);
    for line in lines.iter().take(4) {
        sim.feed(Kind::Out, json!(line));
    }
    assert!(sim.earlier.is_empty());
    let snap = sim.snap();
    assert_eq!(snap.earlier.state, EarlierState::None);
    assert_eq!(snap.earlier.items, 0);
    assert_eq!(snap.earlier.gaps.replays_ignored, 1);
    assert!(sim.earlier_states().is_empty());
    sim.assert_replays_exactly(wasit_opencode::opencode(), &m);
}

/// An agent that replays while the session loads (ACP) and a process that ends in the middle
/// of it: the state says so, and the next process generation's replay adds only what the
/// journal lacks.
#[test]
fn an_interrupted_replay_goes_on_without_repeating_itself() {
    let (spec, lines) = recording(ACP_RESUME);
    let m = meta("opencode", &spec);
    let mut sim = Sim::new(wasit_opencode::opencode(), &m);
    sim.start(&spec, None);
    // initialize reply, the replayed user message, the replayed answer; then the process dies
    // before `session/load` answers.
    for line in lines.iter().take(3) {
        sim.feed(Kind::Out, json!(line));
    }
    assert_eq!(sim.snap().earlier.state, EarlierState::Loading);
    sim.exit();
    let snap = sim.snap();
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: false });
    assert!(snap.earlier.gaps.interrupted);
    let held = sim.earlier.len();
    assert!(held >= 1, "{:?}", sim.earlier);
    // The next generation: the whole replay again, then the load result.
    let mut next = spec.clone();
    next.first_prompt = None;
    sim.start(&next, None);
    assert_eq!(sim.snap().earlier.state, EarlierState::Loading);
    for line in lines.iter().take(4) {
        sim.feed(Kind::Out, json!(line));
    }
    let snap = sim.snap();
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: true });
    assert!(!snap.earlier.gaps.interrupted);
    assert_eq!(sim.earlier.len(), 2, "{:#?}", sim.earlier);
    assert_eq!(snap.earlier.items, 2);
    sim.assert_replays_exactly(wasit_opencode::opencode(), &m);
}

// ── an agent that keeps its history in its own storage ───────────────────────────────────────

const U: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

fn claude_spec(resume: ResumeMode) -> SpawnSpec {
    SpawnSpec {
        resume,
        session_id: Some("aaaaaaaaaaaa".into()),
        ..SpawnSpec::default()
    }
}

fn item(text: &str, at: u64) -> AgentEvent {
    AgentEvent::UserMessage { text: text.into() }.earlier(Some(at))
}

fn plan() -> EarlierPlan {
    EarlierPlan::Storage {
        path: "/agent/projects/x/id.jsonl".into(),
        len: 1234,
    }
}

fn storage_sim() -> (Sim, SessionMeta, SpawnSpec) {
    let spec = claude_spec(ResumeMode::Resume(U.into()));
    let m = meta("claude", &spec);
    let mut sim = Sim::new(wasit_claude::descriptor(), &m);
    sim.start(&spec, Some(plan()));
    (sim, m, spec)
}

fn chunk(sim: &mut Sim, first: u64, texts: &[&str]) -> Output {
    sim.note(RuntimeNote::EarlierChunk {
        first,
        events: texts
            .iter()
            .enumerate()
            .map(|(i, t)| item(t, 10 + first + i as u64))
            .collect(),
    })
}

#[test]
fn a_planned_import_loads_in_chunks_and_ends_complete() {
    let (mut sim, m, _) = storage_sim();
    let snap = sim.snap();
    assert_eq!(snap.earlier.state, EarlierState::Loading);
    assert_eq!(snap.earlier.source, Some(EarlierSource::AgentStorage));
    // The actor is told what to import, from where, and with what already there.
    let work = sim.core.earlier_work().expect("an import to run");
    assert_eq!(
        (work.path.as_str(), work.len, work.skip),
        ("/agent/projects/x/id.jsonl", 1234, 0)
    );

    let out = chunk(&mut sim, 0, &["one", "two"]);
    // The items are not live events: only the progress is.
    assert_eq!(out.earlier.len(), 2);
    assert!(out
        .events
        .iter()
        .all(|e| matches!(e.kind, SessionEventKind::Earlier { .. })));
    // A prompt delivered while it loads is an ordinary input: the import never holds it back.
    let c = sim.note(RuntimeNote::Command {
        token: "t".into(),
        command: wasit_core::Command::prompt("hello"),
        trace: None,
    });
    assert!(c
        .events
        .iter()
        .any(|e| matches!(&e.kind, SessionEventKind::Input { .. })));
    chunk(&mut sim, 2, &["three"]);
    assert_eq!(sim.snap().earlier.items, 3);
    assert_eq!(sim.core.earlier_work().unwrap().skip, 3);
    sim.note(RuntimeNote::EarlierEnd {
        report: HistoryReport {
            messages: 3,
            events: 3,
            ..HistoryReport::default()
        },
    });
    let snap = sim.snap();
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: true });
    assert_eq!(snap.earlier.items, 3);
    assert!(sim.core.earlier_work().is_none());
    // Cursors of the items: unique and in the order the history has.
    let cursors: Vec<u64> = sim.earlier.iter().map(|e| e.cursor).collect();
    assert!(cursors.windows(2).all(|w| w[0] < w[1]), "{cursors:?}");
    sim.assert_replays_exactly(wasit_claude::descriptor(), &m);
}

#[test]
fn history_never_changes_live_state_even_when_it_holds_a_turn_end() {
    let (mut sim, _, _) = storage_sim();
    let before = sim.snap();
    sim.note(RuntimeNote::EarlierChunk {
        first: 0,
        events: vec![
            AgentEvent::TurnCompleted {
                end_reason: None,
                was_interrupted: false,
                num_turns: 3,
                usage: wasit_core::Usage {
                    input_tokens: 999,
                    output_tokens: 999,
                    cost_usd: 9.5,
                    ..Default::default()
                },
                turn_usage: None,
                per_model: Default::default(),
                reply: Some("x".into()),
                origin: None,
            }
            .earlier(None),
            AgentEvent::UsageUpdate {
                usage: wasit_core::Usage {
                    input_tokens: 5,
                    ..Default::default()
                },
                context_pct: Some(50.0),
            }
            .earlier(None),
            AgentEvent::GateResolved {
                request_id: "g".into(),
                reason: None,
            }
            .earlier(None),
        ],
    });
    let after = sim.snap();
    assert_eq!(after.usage, before.usage);
    assert_eq!(after.live_usage, before.live_usage);
    assert_eq!(after.turns_completed, before.turns_completed);
    assert_eq!(after.turn, before.turn);
    assert_eq!(after.last_turn, before.last_turn);
    assert_eq!(after.phase, before.phase);
    assert_eq!(after.context_pct, before.context_pct);
    assert!(after.pending_gates.is_empty());
    assert_eq!(after.earlier.items, 3);
    // ...no anomaly (history is not agent activity), and nothing of it reached the stream.
    assert!(!sim
        .events
        .iter()
        .any(|e| matches!(&e.kind, SessionEventKind::Anomaly { .. })));
    assert!(!sim.events.iter().any(|e| matches!(
        &e.kind,
        SessionEventKind::Agent { event } if event.as_history().is_some()
    )));
}

#[test]
fn a_chunk_journaled_twice_or_overlapping_is_never_added_twice() {
    let (mut sim, m, _) = storage_sim();
    chunk(&mut sim, 0, &["a", "b", "c"]);
    // The same chunk again, one that overlaps the end, one that starts inside what is held.
    chunk(&mut sim, 0, &["a", "b", "c"]);
    chunk(&mut sim, 2, &["c", "d"]);
    assert_eq!(sim.snap().earlier.items, 4);
    let texts: Vec<String> = sim
        .earlier
        .iter()
        .map(|e| match history_of(e).1 {
            AgentEvent::UserMessage { text } => text.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(texts, ["a", "b", "c", "d"]);
    // A chunk that leaves a hole is refused, and said.
    let out = chunk(&mut sim, 9, &["z"]);
    assert!(out.earlier.is_empty());
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e.kind, SessionEventKind::Notice { .. })));
    sim.assert_replays_exactly(wasit_claude::descriptor(), &m);
}

#[test]
fn an_import_that_ends_with_gaps_says_what_is_left_out() {
    let (mut sim, _, _) = storage_sim();
    chunk(&mut sim, 0, &["a"]);
    sim.note(RuntimeNote::EarlierEnd {
        report: HistoryReport {
            older_omitted: 120,
            shortened: 2,
            unreadable: 1,
            unrecognised: 3,
            unrecognised_kinds: vec!["weird".into()],
            missing_files: 1,
            truncated_tail: true,
            ..HistoryReport::default()
        },
    });
    let e = sim.snap().earlier;
    assert_eq!(e.state, EarlierState::Loaded { complete: false });
    assert_eq!(e.gaps.omitted, 120);
    assert_eq!(e.gaps.shortened, 2);
    assert_eq!(e.gaps.unreadable, 1);
    assert_eq!(e.gaps.unrecognised, 3);
    assert_eq!(e.gaps.unrecognised_kinds, ["weird"]);
    assert_eq!(e.gaps.missing_files, 1);
    assert!(e.gaps.truncated_tail);
}

#[test]
fn an_import_that_cannot_start_is_unavailable_with_its_reason_never_empty() {
    let spec = claude_spec(ResumeMode::Resume(U.into()));
    let m = meta("claude", &spec);
    // Planned as unavailable: the reason is in the first start note.
    let mut sim = Sim::new(wasit_claude::descriptor(), &m);
    sim.start(
        &spec,
        Some(EarlierPlan::Unavailable {
            why: EarlierUnavailable::SourceMissing,
        }),
    );
    let e = sim.snap().earlier;
    assert_eq!(
        e.state,
        EarlierState::Unavailable {
            reason: EarlierUnavailable::SourceMissing
        }
    );
    assert!(sim.core.earlier_work().is_none());
    // Failing while running, before anything arrived: unavailable.
    let (mut sim, _, _) = storage_sim();
    sim.note(RuntimeNote::EarlierFailed {
        reason: EarlierUnavailable::FormatNotUnderstood { record: 4 },
    });
    assert_eq!(
        sim.snap().earlier.state,
        EarlierState::Unavailable {
            reason: EarlierUnavailable::FormatNotUnderstood { record: 4 }
        }
    );
    // Failing after some of it arrived: what arrived stays, and the gap is said.
    let (mut sim, _, _) = storage_sim();
    chunk(&mut sim, 0, &["a"]);
    sim.note(RuntimeNote::EarlierFailed {
        reason: EarlierUnavailable::Failed { detail: "x".into() },
    });
    let e = sim.snap().earlier;
    assert_eq!(e.state, EarlierState::Loaded { complete: false });
    assert!(e.gaps.interrupted);
    assert_eq!(e.items, 1);
}

/// The process dies in the middle of the import: nobody can journal the rest, so the state says
/// partial; the next generation continues from the item the journal holds, and a restarted
/// daemon (a replay) reaches the same conclusion.
#[test]
fn an_import_interrupted_by_the_process_ending_continues_in_the_next_generation() {
    let (mut sim, m, spec) = storage_sim();
    chunk(&mut sim, 0, &["a", "b"]);
    sim.exit();
    let e = sim.snap().earlier;
    assert_eq!(e.state, EarlierState::Loaded { complete: false });
    assert!(e.gaps.interrupted);
    assert!(
        sim.core.earlier_work().is_none(),
        "no process to journal into"
    );
    // The next generation (a continue: a resume of the same conversation, no plan in its start).
    let mut next = spec.clone();
    next.first_prompt = None;
    sim.start(&next, None);
    assert_eq!(sim.snap().earlier.state, EarlierState::Loading);
    let work = sim.core.earlier_work().expect("the import goes on");
    assert_eq!(work.skip, 2);
    chunk(&mut sim, 2, &["c"]);
    sim.note(RuntimeNote::EarlierEnd {
        report: HistoryReport::default(),
    });
    let e = sim.snap().earlier;
    assert_eq!(e.state, EarlierState::Loaded { complete: true });
    assert!(!e.gaps.interrupted);
    assert_eq!(e.items, 3);
    // Once complete, a later generation does nothing.
    sim.exit();
    sim.start(&next, None);
    assert!(sim.core.earlier_work().is_none());
    assert_eq!(
        sim.snap().earlier.state,
        EarlierState::Loaded { complete: true }
    );
    sim.assert_replays_exactly(wasit_claude::descriptor(), &m);
}

/// Journals of builds before earlier history: a session that resumed a conversation shows no
/// history and no error, and a later continue does not start importing into a journal that has
/// its own events of the conversation.
#[test]
fn a_journal_without_a_plan_replays_as_before() {
    let spec = claude_spec(ResumeMode::Resume(U.into()));
    let m = meta("claude", &spec);
    let mut sim = Sim::new(wasit_claude::descriptor(), &m);
    sim.start(&spec, None);
    assert_eq!(sim.snap().earlier, Default::default());
    assert!(sim.core.earlier_work().is_none());
    sim.exit();
    sim.start(&spec, None);
    assert_eq!(sim.snap().earlier, Default::default());
    assert!(sim.core.earlier_work().is_none());
    assert!(sim.earlier_states().is_empty());
}

/// A fresh conversation has nothing earlier, and a later generation that resumes it (a
/// continue) has nothing to import either: the journal has the conversation's own events.
#[test]
fn a_conversation_started_here_never_imports() {
    let spec = claude_spec(ResumeMode::Fresh);
    let m = meta("claude", &spec);
    let mut sim = Sim::new(wasit_claude::descriptor(), &m);
    sim.start(&spec, None);
    sim.exit();
    let next = claude_spec(ResumeMode::Resume(U.into()));
    sim.start(&next, Some(plan()));
    assert!(sim.core.earlier_work().is_none());
    assert_eq!(sim.snap().earlier.state, EarlierState::None);
}

#[test]
fn inline_media_in_history_is_moved_to_blobs_like_any_other() {
    let (mut sim, _, _) = storage_sim();
    let data = wasit_core::content::b64_encode(&[7u8; 4096]);
    let out = sim.note(RuntimeNote::EarlierChunk {
        first: 0,
        events: vec![AgentEvent::UserContent {
            parts: vec![wasit_core::ContentPart::inline_image("image/png", data)],
        }
        .earlier(None)],
    });
    assert_eq!(out.blobs.len(), 1, "the image bytes go to the blob store");
    let (_, inner) = history_of(&out.earlier[0]);
    match inner {
        AgentEvent::UserContent { parts } => assert!(matches!(
            &parts[0],
            wasit_core::ContentPart::Image {
                source: wasit_core::MediaSource::Blob { .. },
                ..
            }
        )),
        other => panic!("{other:?}"),
    }
}
