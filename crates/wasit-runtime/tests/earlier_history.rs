//! A conversation a session resumes shows its earlier messages before its live events (real
//! shims, the fake CLI over real storage files in the real formats).
//!
//! The agent's storage is the test's: claude's transcripts, codex's rollouts and threads and the
//! ACP agents' sessions live under `<data>/agent-state`.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use serde_json::json;
use wasit_core::{AgentEvent, DriverId};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_fake_cli::transcript::{Content, SubagentSpec, Transcript};
use wasit_runtime::wire::{
    CreateSession, EarlierSource, EarlierState, EarlierUnavailable, HistoryScope, ResumeRequest,
    RuntimeError, SessionEvent, SessionEventKind, SessionId, SessionSnapshot,
};
use wasit_runtime::{DriverOverride, RuntimeConfig, SessionManager};

const ID: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

/// Where the fake agents keep their storage in this test.
fn state(d: &Path) -> PathBuf {
    d.join("agent-state")
}

fn claude_config(d: &Path) -> PathBuf {
    state(d).join("claude-config")
}

/// The project folder sessions run in: the canonical path of the data directory, which is what
/// a session's working directory is normalised to (and what the agent files its storage under).
fn project(d: &Path) -> String {
    std::fs::canonicalize(d)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

/// A runtime configuration whose discovery and history see the agents' storage where the fakes
/// keep it.
fn cfg(d: &Path) -> RuntimeConfig {
    let mut cfg = config(d, timeouts());
    let set = |cfg: &mut RuntimeConfig, driver: &str, var: &str, value: PathBuf| {
        let o: &mut DriverOverride = cfg.overrides.get_mut(&DriverId::new(driver)).unwrap();
        o.env
            .push((var.into(), value.to_string_lossy().into_owned()));
    };
    set(&mut cfg, "claude", "CLAUDE_CONFIG_DIR", claude_config(d));
    set(&mut cfg, "codex", "CODEX_HOME", state(d).join("codex-home"));
    cfg
}

async fn mgr(d: &Path) -> SessionManager {
    SessionManager::new(cfg(d)).await.unwrap()
}

fn resume(d: &Path, driver: &str, native: &str) -> CreateSession {
    let mut req = create(d, driver, &Scenario::builder().build());
    req.cwd = Some(project(d));
    req.resume = ResumeRequest::Resume {
        native_session_id: native.into(),
    };
    req
}

/// What an item of earlier history says, in a word and what matters.
fn describe(e: &SessionEvent) -> String {
    fn of(a: &AgentEvent) -> String {
        match a {
            AgentEvent::UserMessage { text } => format!("user:{text}"),
            AgentEvent::AssistantDelta { text, .. } => format!("assistant:{text}"),
            AgentEvent::ThinkingDelta { .. } => "thinking".into(),
            AgentEvent::UserContent { parts } => format!("user-content:{}", parts.len()),
            AgentEvent::ToolCallStarted { name, .. } => format!("tool:{name}"),
            AgentEvent::ToolCallFinished { name, ok, .. } => format!("result:{name}:{ok}"),
            AgentEvent::Compaction { .. } => "compaction".into(),
            AgentEvent::Scoped { scope, event } => {
                format!(
                    "scoped[{}]:{}",
                    scope.agent_id.as_deref().unwrap_or("?"),
                    of(event)
                )
            }
            AgentEvent::Notice { message } => format!("notice:{message}"),
            other => format!("{other:?}"),
        }
    }
    match &e.kind {
        SessionEventKind::Agent { event } => match event.as_history() {
            Some((_, inner)) => of(inner),
            None => panic!("a live event among the earlier items: {event:?}"),
        },
        other => panic!("not an agent event: {other:?}"),
    }
}

/// Every earlier item of a session, oldest first, paged the way a client pages.
async fn earlier(
    mgr: &SessionManager,
    id: &SessionId,
    scope: HistoryScope,
    page: usize,
) -> Vec<SessionEvent> {
    let mut all: Vec<SessionEvent> = Vec::new();
    let mut before = None;
    loop {
        let p = mgr
            .earlier_scoped(id, before, page, scope.clone())
            .await
            .unwrap();
        let first = p.events.first().map(|e| e.cursor);
        let mut chunk = p.events;
        chunk.extend(std::mem::take(&mut all));
        all = chunk;
        match (p.has_more, first) {
            (true, Some(c)) => before = Some(c),
            _ => break,
        }
    }
    all
}

async fn loaded(mgr: &SessionManager, id: &SessionId) -> SessionSnapshot {
    wait_snapshot(mgr, id, "the earlier history loaded", |s| {
        s.loaded && !matches!(s.earlier.state, EarlierState::Loading | EarlierState::None)
    })
    .await
}

async fn turn_done(mgr: &SessionManager, id: &SessionId, n: u64) -> SessionSnapshot {
    wait_snapshot(mgr, id, "the turn completed", |s| s.turns_completed >= n).await
}

fn b64(bytes: &[u8]) -> String {
    wasit_core::content::b64_encode(bytes)
}

/// A conversation only the agent's storage has.
fn conversation() -> Transcript {
    Transcript::deterministic(5)
        .user("what is in the folder")
        .thinking()
        .assistant("let me look")
        .tool("Bash", json!({"command": "ls"}), "alpha beta")
        .assistant("two files")
        .user_with_image("and this picture", "image/png", &b64(&png()))
        .assistant("a picture of something")
        .subagent(
            SubagentSpec::new("dig deeper", "look at alpha")
                .content(Content::text("reading alpha"))
                .content(Content::tool("Read", json!({"file": "alpha"}), "contents"))
                .answer("alpha is fine"),
        )
        .assistant("all checked")
        .unknown_record("future-kind")
}

#[tokio::test]
async fn a_conversation_only_the_agent_has_shows_before_the_live_events() {
    let d = TempDir::new();
    let path = conversation().write(&claude_config(d.path()), &project(d.path()), ID);
    assert!(path.is_file());
    let mgr = mgr(d.path()).await;
    let mut req = resume(d.path(), "claude", ID);
    req.first_prompt = Some("and now".into());
    let id = mgr.create(req).await.unwrap().id;
    turn_done(&mgr, &id, 1).await;
    let snap = loaded(&mgr, &id).await;

    // The state says where the history came from and that it is whole except for the record
    // kind the driver does not know.
    assert_eq!(snap.earlier.source, Some(EarlierSource::AgentStorage));
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: false });
    assert_eq!(snap.earlier.gaps.unrecognised, 1);
    assert_eq!(snap.earlier.gaps.unrecognised_kinds, ["future-kind"]);

    let items = earlier(&mgr, &id, HistoryScope::All, 7).await;
    assert_eq!(items.len() as u64, snap.earlier.items);
    let said: Vec<String> = items.iter().map(describe).collect();
    // In the order of the conversation: the subagent's own messages follow the call that
    // spawned it, attributed to it.
    let sub: Vec<&str> = said
        .iter()
        .filter(|s| s.starts_with("scoped["))
        .map(String::as_str)
        .collect();
    assert_eq!(sub.len(), 4, "{said:#?}");
    let main: Vec<&str> = said
        .iter()
        .filter(|s| !s.starts_with("scoped["))
        .map(String::as_str)
        .collect();
    assert_eq!(
        main,
        [
            "user:what is in the folder",
            "assistant:let me look",
            "tool:Bash",
            "result:Bash:true",
            "assistant:two files",
            "user:and this picture",
            "user-content:1",
            "assistant:a picture of something",
            "tool:Agent",
            "result:Agent:true",
            "assistant:all checked",
        ]
    );
    // Every item has the time the agent recorded for it, in order.
    let times: Vec<u64> = items
        .iter()
        .map(|e| match &e.kind {
            SessionEventKind::Agent { event } => event.as_history().unwrap().0.expect("a time"),
            _ => unreachable!(),
        })
        .collect();
    assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");
    // Their cursors are journal cursors: unique and increasing.
    assert!(items.windows(2).all(|w| w[0].cursor < w[1].cursor));

    // The live transcript has the new turn only: no history in it.
    let live = mgr.history(&id, None, 2000).await.unwrap().events;
    assert!(live.iter().all(|e| !matches!(
        &e.kind,
        SessionEventKind::Agent { event } if event.as_history().is_some()
    )));
    assert!(assistant_text(&live).contains("and now"));
    // The live turn is the only one: history counted nothing.
    assert_eq!(snap.turns_completed, 1);
    assert_eq!(snap.title.as_deref(), Some("and now"));
    // Scopes: the subagent's own conversation, and the session's own without it.
    let call = items
        .iter()
        .find_map(|e| match &e.kind {
            SessionEventKind::Agent { event } => match event.as_history().unwrap().1 {
                AgentEvent::Scoped { scope, .. } => Some(scope.call_id.clone()),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    let only = earlier(&mgr, &id, HistoryScope::Subagent { call_id: call }, 3).await;
    assert_eq!(only.len(), 4);
    assert!(only.iter().all(|e| describe(e).starts_with("scoped[")));
    let main_only = earlier(&mgr, &id, HistoryScope::Main, 100).await;
    assert_eq!(main_only.len(), main.len());
    // The image of an earlier message is a blob like any other.
    let image = items
        .iter()
        .find_map(|e| match &e.kind {
            SessionEventKind::Agent { event } => match event.as_history().unwrap().1 {
                AgentEvent::UserContent { parts } => Some(parts.clone()),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        &image[0],
        wasit_core::ContentPart::Image {
            source: wasit_core::MediaSource::Blob { .. },
            ..
        }
    ));
}

fn journal_notes(d: &Path, id: &SessionId, kind: &str) -> Vec<u64> {
    let text = std::fs::read_to_string(d.join("sessions").join(id.as_str()).join("journal.ndjson"))
        .unwrap();
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v["kind"] == "note" && v["data"]["wasit"] == kind)
        .map(|v| v["seq"].as_u64().unwrap())
        .collect()
}

async fn stopped(mgr: &SessionManager, id: &SessionId) {
    mgr.stop(id).await.unwrap();
    wait_snapshot(mgr, id, "the session ended", |s| !s.phase.is_live()).await;
}

/// A history too big to be read before the session starts does not hold the session back: its
/// first prompt is delivered, answered and its turn completes while the history is still being
/// read from the agent's file and journaled.
#[tokio::test]
async fn a_session_is_usable_while_a_large_history_is_imported() {
    let d = TempDir::new();
    // About 24 MB of conversation in the agent's own file.
    let path = Transcript::deterministic(9)
        .user("the beginning")
        .assistant("hello")
        .filler(3000, 8000)
        .write(&claude_config(d.path()), &project(d.path()), ID);
    assert!(std::fs::metadata(&path).unwrap().len() > 20 * 1024 * 1024);
    let mgr = mgr(d.path()).await;
    let mut req = resume(d.path(), "claude", ID);
    req.first_prompt = Some("go on".into());
    let id = mgr.create(req).await.unwrap().id;
    let mut events = mgr.subscribe(&id, None).await.unwrap();
    let seen = until(&mut events, is_turn_completed).await;
    // The prompt was delivered and its turn completed...
    assert!(assistant_text(&seen).contains("go on"));
    // ...while the history was still arriving: it says so, truthfully.
    let during = mgr.snapshot(&id).unwrap();
    assert_eq!(
        during.earlier.state,
        EarlierState::Loading,
        "{:?}",
        during.earlier
    );
    assert!(during.phase.name() == "idle" && during.turns_completed == 1);
    // And the journal agrees on the order: the start (with the first prompt) came before any
    // piece of the history.
    let done = loaded(&mgr, &id).await;
    assert_eq!(done.earlier.state, EarlierState::Loaded { complete: true });
    let start = journal_notes(d.path(), &id, "start")[0];
    let chunks = journal_notes(d.path(), &id, "earlier_chunk");
    assert!(chunks.len() > 5, "{} chunks", chunks.len());
    assert!(start < chunks[0]);
    // Every record is far below the cap of a journal record.
    let journal = std::fs::read_to_string(
        d.path()
            .join("sessions")
            .join(id.as_str())
            .join("journal.ndjson"),
    )
    .unwrap();
    assert!(journal.lines().all(|l| l.len() < 4 * 1024 * 1024));
    // The items are all there, in order, once.
    let items = earlier(&mgr, &id, HistoryScope::Main, 2000).await;
    assert_eq!(items.len() as u64, done.earlier.items);
    assert_eq!(items.len(), 2 + 3000 * 4);
    assert_eq!(describe(&items[0]), "user:the beginning");
    assert_eq!(describe(&items[2]), "user:question 0");
    assert_eq!(
        describe(items.last().unwrap()).get(..13),
        Some("assistant:ans")
    );
    // Replay: a new manager over the same data reads the same.
    stopped(&mgr, &id).await;
    drop(events);
    drop(mgr);
    let again = self::mgr(d.path()).await;
    again.adopt_all().await.unwrap();
    let snap = wait_snapshot(&again, &id, "replayed", |s| s.loaded).await;
    assert_eq!(snap.earlier, done.earlier);
    let items2 = earlier(&again, &id, HistoryScope::Main, 2000).await;
    assert_eq!(items2, items);
}

/// The daemon restarts in the middle of the import (a live shim, an actor that is gone): the new
/// actor reads the journal, sees how much of the history it holds and carries on from there.
/// Nothing is duplicated and nothing is missing.
#[tokio::test]
async fn a_restart_in_the_middle_of_the_import_carries_on_without_duplicates() {
    let d = TempDir::new();
    Transcript::deterministic(3)
        .user("the beginning")
        .filler(250, 20_000)
        .write(&claude_config(d.path()), &project(d.path()), ID);
    let mut c = cfg(d.path());
    // Small chunks: many notes, so there is a middle to restart in.
    c.history.chunk_bytes = 4 * 1024;
    let first = SessionManager::new(c.clone()).await.unwrap();
    let id = first
        .create(resume(d.path(), "claude", ID))
        .await
        .unwrap()
        .id;
    let mut events = first.subscribe(&id, None).await.unwrap();
    let seen = until(&mut events, |e| {
        matches!(&e.kind, SessionEventKind::Earlier { earlier, .. }
            if earlier.state == EarlierState::Loading && earlier.items > 0)
    })
    .await;
    assert!(!seen.is_empty());
    drop(events);
    drop(first);
    // The first actor is gone; the journal holds some of the history, not all.
    assert!(
        journal_notes(d.path(), &id, "earlier_end").is_empty(),
        "the import had finished"
    );
    let held = journal_notes(d.path(), &id, "earlier_chunk").len();
    assert!(held > 0);

    let second = SessionManager::new(c).await.unwrap();
    second.adopt_all().await.unwrap();
    let done = loaded(&second, &id).await;
    assert_eq!(done.earlier.state, EarlierState::Loaded { complete: true });
    let items = earlier(&second, &id, HistoryScope::All, 500).await;
    assert_eq!(items.len(), 1 + 250 * 3 + 250);
    // Each question once, in order.
    let questions: Vec<String> = items
        .iter()
        .map(describe)
        .filter(|s| s.starts_with("user:question"))
        .collect();
    let want: Vec<String> = (0..250).map(|i| format!("user:question {i}")).collect();
    assert_eq!(questions, want);
    assert_eq!(journal_notes(d.path(), &id, "earlier_end").len(), 1);
    // The count the snapshot gives is what the journal holds.
    assert_eq!(done.earlier.items as usize, items.len());
}

#[tokio::test]
async fn a_history_that_cannot_be_read_says_why_never_an_empty_transcript() {
    // The conversation is stored under another folder's project: the agent finds it from
    // anywhere, but the file is not where this session's folder puts it.
    let d = TempDir::new();
    Transcript::deterministic(1)
        .user("elsewhere")
        .assistant("answer")
        .write(&claude_config(d.path()), "/some/other/folder", ID);
    let mgr1 = mgr(d.path()).await;
    let id = mgr1
        .create(resume(d.path(), "claude", ID))
        .await
        .unwrap()
        .id;
    let snap = loaded(&mgr1, &id).await;
    assert_eq!(
        snap.earlier.state,
        EarlierState::Unavailable {
            reason: EarlierUnavailable::SourceMissing
        }
    );
    assert_eq!(snap.earlier.source, Some(EarlierSource::AgentStorage));
    assert_eq!(snap.earlier.items, 0);
    assert!(earlier(&mgr1, &id, HistoryScope::All, 10).await.is_empty());
    // The session itself works.
    wait_phase(&mgr1, &id, "idle").await;

    // A conversation the agent does not have at all: the agent refuses to start, which is what
    // the session shows (the history is not the question then).
    let d = TempDir::new();
    let mgr0 = mgr(d.path()).await;
    let id = mgr0
        .create(resume(d.path(), "claude", ID))
        .await
        .unwrap()
        .id;
    let snap = wait_snapshot(&mgr0, &id, "failed", |s| {
        matches!(s.phase, wasit_runtime::wire::Phase::Failed { .. })
    })
    .await;
    let (reason, tail) = failed_reason(&snap.phase).unwrap();
    assert!(reason.contains("exited") && tail.iter().any(|l| l.contains("No conversation found")));

    // A file nothing in which is a record of the format.
    let d = TempDir::new();
    let path = wasit_fake_cli::transcript::transcript_path(
        &claude_config(d.path()),
        &project(d.path()),
        ID,
    );
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "not a record\nneither is this\n").unwrap();
    let mgr2 = mgr(d.path()).await;
    let id = mgr2
        .create(resume(d.path(), "claude", ID))
        .await
        .unwrap()
        .id;
    let snap = loaded(&mgr2, &id).await;
    assert_eq!(
        snap.earlier.state,
        EarlierState::Unavailable {
            reason: EarlierUnavailable::FormatNotUnderstood { record: 1 }
        }
    );
    // The state reached the stream and the trail, as a fact with no content in it.
    let trail = mgr2.trail_page(&id, None, 2000).await.unwrap();
    let said = serde_json::to_string(&trail.entries).unwrap();
    assert!(said.contains("earlier.history") && said.contains("format_not_understood"));
    assert!(!said.contains("not a record"));
}

#[tokio::test]
async fn a_cut_last_line_and_a_bounded_import_are_shown_as_what_they_are() {
    // The agent was writing when the file was read: the last line is cut.
    let d = TempDir::new();
    Transcript::deterministic(2)
        .user("one")
        .assistant("two")
        .truncated_tail()
        .write(&claude_config(d.path()), &project(d.path()), ID);
    let m = mgr(d.path()).await;
    let id = m.create(resume(d.path(), "claude", ID)).await.unwrap().id;
    let snap = loaded(&m, &id).await;
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: false });
    assert!(snap.earlier.gaps.truncated_tail, "{:?}", snap.earlier);
    assert_eq!(snap.earlier.items, 2);

    // A window that holds only the end of a long conversation: the transcript starts at a
    // prompt and says how many messages are older and not shown.
    let d = TempDir::new();
    Transcript::deterministic(4)
        .user("the very first prompt")
        .filler(100, 10_000)
        .write(&claude_config(d.path()), &project(d.path()), ID);
    let mut c = cfg(d.path());
    c.history.limits.window_bytes = 200_000;
    let m = SessionManager::new(c).await.unwrap();
    let id = m.create(resume(d.path(), "claude", ID)).await.unwrap().id;
    let snap = loaded(&m, &id).await;
    assert_eq!(snap.earlier.state, EarlierState::Loaded { complete: false });
    assert!(snap.earlier.gaps.omitted > 100, "{:?}", snap.earlier.gaps);
    let items = earlier(&m, &id, HistoryScope::All, 1000).await;
    // The transcript says it: the first thing in it is that older messages exist and are not
    // shown, then the part that is.
    let first = describe(&items[0]);
    assert!(
        first.starts_with("notice:")
            && first.contains("not shown here")
            && first.contains(&snap.earlier.gaps.omitted.to_string()),
        "{first}"
    );
    assert!(
        describe(&items[1]).starts_with("user:question"),
        "{}",
        describe(&items[1])
    );
    assert!(describe(items.last().unwrap()).starts_with("assistant:answer 99"));
    assert!(items.len() < 100 * 4);
}

/// A later start of the same session does not import its conversation again.
#[tokio::test]
async fn a_continued_session_does_not_import_twice() {
    let d = TempDir::new();
    conversation().write(&claude_config(d.path()), &project(d.path()), ID);
    let m = mgr(d.path()).await;
    let id = m.create(resume(d.path(), "claude", ID)).await.unwrap().id;
    let first = loaded(&m, &id).await;
    wait_phase(&m, &id, "idle").await;
    stopped(&m, &id).await;
    m.continue_session(&id).await.unwrap();
    wait_phase(&m, &id, "idle").await;
    m.command(&id, wasit_core::Command::prompt("again"))
        .await
        .unwrap();
    turn_done(&m, &id, 1).await;
    stopped(&m, &id).await;
    let second = m.snapshot(&id).unwrap();
    assert_eq!(second.earlier, first.earlier);
    assert_eq!(journal_notes(d.path(), &id, "earlier_end").len(), 1);
    // The conversation grew by the first process's turns (a live transcript now), and the
    // earlier part is the same items.
    let items = earlier(&m, &id, HistoryScope::All, 1000).await;
    assert_eq!(items.len() as u64, first.earlier.items);
}

fn tool_scenario() -> Scenario {
    Scenario::builder()
        .turn(
            Turn::builder()
                .say("looking around")
                .tool("Bash", json!({"command": "ls"}))
                .result_if_allowed("alpha beta")
                .done()
                .say("two files"),
        )
        .build()
}

/// Every driver: a conversation one session had is resumed by another, which shows it.
async fn another_session_resumes_the_conversation(driver: &str) {
    let d = TempDir::new();
    let m = mgr(d.path()).await;
    let mut req = create(d.path(), driver, &tool_scenario());
    req.cwd = Some(project(d.path()));
    req.first_prompt = Some("list the files".into());
    let one = m.create(req).await.unwrap().id;
    turn_done(&m, &one, 1).await;
    let native = m.snapshot(&one).unwrap().native_session_id.unwrap();
    stopped(&m, &one).await;

    let two = m
        .create(resume(d.path(), driver, &native))
        .await
        .unwrap()
        .id;
    let snap = loaded(&m, &two).await;
    let source = if driver == "claude" {
        EarlierSource::AgentStorage
    } else {
        EarlierSource::AgentReplay
    };
    assert_eq!(snap.earlier.source, Some(source), "{driver}");
    let items = earlier(&m, &two, HistoryScope::All, 100).await;
    let said: Vec<String> = items.iter().map(describe).collect();
    assert_eq!(
        said.first().map(String::as_str),
        Some("user:list the files"),
        "{driver}: {said:?}"
    );
    assert!(
        said.iter().any(|s| s == "assistant:two files")
            || said.iter().any(|s| s.contains("two files")),
        "{driver}: {said:?}"
    );
    assert!(
        said.iter().any(|s| s.starts_with("tool:")),
        "{driver}: {said:?}"
    );
    assert_eq!(snap.earlier.items as usize, items.len());
    // Nothing of it is live: no turn, no usage, nothing running.
    assert_eq!(snap.turns_completed, 0, "{driver}");
    assert_eq!(snap.usage, Default::default(), "{driver}");
    assert!(snap.pending_gates.is_empty() && snap.tasks.is_empty());
    assert!(snap.phase.is_live());

    // A prompt of this session answers on top of it.
    m.command(&two, wasit_core::Command::prompt("and again"))
        .await
        .unwrap();
    turn_done(&m, &two, 1).await;
    // A continue of this session reloads the conversation in the agent; the journal already has
    // it, so it is not added again.
    let before = m.snapshot(&two).unwrap().earlier;
    stopped(&m, &two).await;
    m.continue_session(&two).await.unwrap();
    wait_phase(&m, &two, "idle").await;
    let after = m.snapshot(&two).unwrap().earlier;
    assert_eq!(after.items, before.items, "{driver}");
    assert_eq!(after.state, before.state, "{driver}");
    if driver != "claude" {
        assert_eq!(after.gaps.replays_ignored, 1, "{driver}");
    }
    assert_eq!(
        earlier(&m, &two, HistoryScope::All, 100).await.len(),
        items.len(),
        "{driver}"
    );
}

#[tokio::test]
async fn claude_conversation_resumed_by_another_session() {
    another_session_resumes_the_conversation("claude").await
}
#[tokio::test]
async fn codex_conversation_resumed_by_another_session() {
    another_session_resumes_the_conversation("codex").await
}
#[tokio::test]
async fn opencode_conversation_resumed_by_another_session() {
    another_session_resumes_the_conversation("opencode").await
}
#[tokio::test]
async fn kilo_conversation_resumed_by_another_session() {
    another_session_resumes_the_conversation("kilo").await
}

// ── "continue the most recent conversation" ─────────────────────────────────────────────────

async fn first_conversation(m: &SessionManager, d: &Path, driver: &str, prompt: &str) -> String {
    let mut req = create(d, driver, &Scenario::builder().build());
    req.cwd = Some(project(d));
    req.first_prompt = Some(prompt.into());
    let id = m.create(req).await.unwrap().id;
    turn_done(m, &id, 1).await;
    let native = m.snapshot(&id).unwrap().native_session_id.unwrap();
    stopped(m, &id).await;
    native
}

fn latest(d: &Path, driver: &str) -> CreateSession {
    let mut req = create(d, driver, &Scenario::builder().build());
    req.cwd = Some(project(d));
    req.resume = ResumeRequest::Latest;
    req
}

/// The runtime resolves the newest conversation of the folder itself and resumes it by its id;
/// a folder with none refuses the create instead of starting a new conversation.
async fn continue_most_recent(driver: &str) {
    let d = TempDir::new();
    let m = mgr(d.path()).await;
    // Nothing to continue: a typed error, nothing created.
    let err = m.create(latest(d.path(), driver)).await.unwrap_err();
    assert_eq!(
        err,
        RuntimeError::NothingToContinue(project(d.path())),
        "{driver}"
    );
    assert!(m.list().is_empty(), "{driver}: a session was created");

    let older = first_conversation(&m, d.path(), driver, "the older conversation").await;
    let newer = first_conversation(&m, d.path(), driver, "the newer conversation").await;
    assert_ne!(older, newer);
    let id = m.create(latest(d.path(), driver)).await.unwrap().id;
    let snap = loaded(&m, &id).await;
    // It resumed the newer one, by its id.
    assert_eq!(
        snap.native_session_id.as_deref(),
        Some(newer.as_str()),
        "{driver}"
    );
    let started = m
        .history(&id, None, 100)
        .await
        .unwrap()
        .events
        .into_iter()
        .find_map(|e| match e.kind {
            SessionEventKind::Started {
                resume, resume_id, ..
            } => Some((resume, resume_id)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        started,
        (
            wasit_runtime::wire::StartResume::Resume,
            Some(newer.clone())
        ),
        "{driver}"
    );
    // And shows it.
    let items = earlier(&m, &id, HistoryScope::All, 50).await;
    assert_eq!(
        describe(&items[0]),
        "user:the newer conversation",
        "{driver}"
    );
}

#[tokio::test]
async fn claude_continue_most_recent() {
    continue_most_recent("claude").await
}
#[tokio::test]
async fn codex_continue_most_recent() {
    continue_most_recent("codex").await
}
#[tokio::test]
async fn opencode_continue_most_recent() {
    continue_most_recent("opencode").await
}
#[tokio::test]
async fn kilo_continue_most_recent() {
    continue_most_recent("kilo").await
}

/// Events of a remote subscription until (and including) the first that `pred` accepts.
async fn remote_until(
    sub: &mut wasit_runtime::TypedSubscription<SessionEvent>,
    pred: impl Fn(&SessionEvent) -> bool,
) -> Vec<SessionEvent> {
    let mut v = Vec::new();
    loop {
        let next = tokio::time::timeout(T, sub.next())
            .await
            .expect("an event in time");
        let (_, e) = next.expect("the topic is open").expect("a decodable event");
        let hit = pred(&e);
        v.push(e);
        if hit {
            return v;
        }
    }
}

/// Over the control protocol: the snapshot carries the state, `session.history` pages the
/// earlier items with `earlier: true` and the live ones without, and the live stream and the
/// trail carry the progress but never an item.
#[tokio::test]
async fn the_control_protocol_serves_the_earlier_history_apart_from_the_live_events() {
    let d = TempDir::new();
    conversation().write(&claude_config(d.path()), &project(d.path()), ID);
    let c = cfg(d.path());
    let m = SessionManager::new(c.clone()).await.unwrap();
    let discovery = wasit_runtime::Discovery::new(wasit_runtime::DiscoveryConfig::from_runtime(&c));
    let service = std::sync::Arc::new(wasit_runtime::RuntimeService::new(
        m.clone(),
        discovery,
        "h",
        "test",
    ));
    let client = loopback(service);
    let id = m.create(resume(d.path(), "claude", ID)).await.unwrap().id;
    let mut events = client.subscribe_session(&id, None);
    let snap = loaded(&m, &id).await;
    let remote = client.snapshot(&id).await.unwrap();
    assert_eq!(remote.earlier, snap.earlier);
    // The stream: progress, never an item.
    let seen = remote_until(&mut events, |e| {
        matches!(&e.kind, SessionEventKind::Earlier { earlier, .. }
            if matches!(earlier.state, EarlierState::Loaded { .. }))
    })
    .await;
    assert!(seen.iter().any(|e| matches!(&e.kind, SessionEventKind::Earlier { earlier, .. } if earlier.state == EarlierState::Loading)));
    assert!(seen.iter().all(|e| !matches!(
        &e.kind,
        SessionEventKind::Agent { event } if event.as_history().is_some()
    )));
    // Paging, remote: the same items as locally.
    let items = client
        .earlier_scoped(&id, None, 1000, HistoryScope::All)
        .await
        .unwrap();
    assert!(!items.has_more);
    assert_eq!(
        items.events,
        earlier(&m, &id, HistoryScope::All, 1000).await
    );
    let live = client.history(&id, None, 1000).await.unwrap();
    assert!(live.events.iter().all(|e| !matches!(
        &e.kind,
        SessionEventKind::Agent { event } if event.as_history().is_some()
    )));
}

/// A prompt sent while the history is being journaled does not wait for it: its note goes into the
/// journal between the pieces of the history, and it is delivered and answered.
#[tokio::test]
async fn a_prompt_sent_during_the_import_is_journaled_between_its_chunks() {
    let d = TempDir::new();
    Transcript::deterministic(8)
        .user("the beginning")
        .filler(250, 20_000)
        .write(&claude_config(d.path()), &project(d.path()), ID);
    let mut c = cfg(d.path());
    c.history.chunk_bytes = 4 * 1024;
    let m = SessionManager::new(c).await.unwrap();
    let id = m.create(resume(d.path(), "claude", ID)).await.unwrap().id;
    let mut events = m.subscribe(&id, None).await.unwrap();
    until(&mut events, |e| {
        matches!(&e.kind, SessionEventKind::Earlier { earlier, .. }
            if earlier.state == EarlierState::Loading && earlier.items > 0)
    })
    .await;
    // The import is under way (it has journaled something, not everything): send a prompt.
    m.command(&id, wasit_core::Command::prompt("now"))
        .await
        .unwrap();
    turn_done(&m, &id, 1).await;
    let done = loaded(&m, &id).await;
    assert_eq!(done.earlier.state, EarlierState::Loaded { complete: true });
    let commands = journal_notes(d.path(), &id, "command");
    let chunks = journal_notes(d.path(), &id, "earlier_chunk");
    assert_eq!(commands.len(), 1);
    assert!(
        chunks.first().unwrap() < &commands[0] && &commands[0] < chunks.last().unwrap(),
        "the prompt (note {}) waited for the history (notes {}..{})",
        commands[0],
        chunks.first().unwrap(),
        chunks.last().unwrap()
    );
}

/// Every driver: a conversation that exists only in the agent's own storage (the scenario's
/// pre-existing session: nobody ran it through a Marsad session) is resumed by id and shows.
async fn an_agent_only_conversation_shows(driver: &str) {
    use wasit_fake_cli::scenario::{HistoryMessage, SessionInfo};
    let d = TempDir::new();
    let native = if driver == "claude" {
        ID.to_string()
    } else if driver == "codex" {
        "01a1ebde-42e6-7691-9a24-6133bdd96a56".to_string()
    } else {
        "ses_agentonly".to_string()
    };
    let scenario = Scenario::builder()
        .session(SessionInfo {
            id: native.clone(),
            title: Some("agent only".into()),
            cwd: Some(project(d.path())),
            history: vec![
                HistoryMessage {
                    role: "user".into(),
                    text: "an earlier question".into(),
                },
                HistoryMessage {
                    role: "assistant".into(),
                    text: "an earlier answer".into(),
                },
            ],
        })
        .build();
    if driver == "claude" {
        wasit_fake_cli::transcript::materialise_sessions(
            &claude_config(d.path()),
            &project(d.path()),
            &scenario.sessions,
        );
    }
    let m = mgr(d.path()).await;
    let mut req = create(d.path(), driver, &scenario);
    req.cwd = Some(project(d.path()));
    req.resume = ResumeRequest::Resume {
        native_session_id: native,
    };
    req.first_prompt = Some("and a new question".into());
    let id = m.create(req).await.unwrap().id;
    turn_done(&m, &id, 1).await;
    let snap = loaded(&m, &id).await;
    assert_eq!(
        snap.earlier.state,
        EarlierState::Loaded { complete: true },
        "{driver}"
    );
    let said: Vec<String> = earlier(&m, &id, HistoryScope::All, 50)
        .await
        .iter()
        .map(describe)
        .collect();
    assert_eq!(
        said,
        ["user:an earlier question", "assistant:an earlier answer"],
        "{driver}"
    );
    // The live transcript is the new turn alone.
    let live = m.history(&id, None, 500).await.unwrap().events;
    assert!(
        assistant_text(&live).contains("and a new question"),
        "{driver}"
    );
    assert!(
        !assistant_text(&live).contains("earlier answer"),
        "{driver}"
    );
}

#[tokio::test]
async fn claude_agent_only_conversation() {
    an_agent_only_conversation_shows("claude").await
}
#[tokio::test]
async fn codex_agent_only_conversation() {
    an_agent_only_conversation_shows("codex").await
}
#[tokio::test]
async fn opencode_agent_only_conversation() {
    an_agent_only_conversation_shows("opencode").await
}
#[tokio::test]
async fn kilo_agent_only_conversation() {
    an_agent_only_conversation_shows("kilo").await
}

/// A session ended in the middle of the import (nobody can journal the rest of it) shows the
/// history as partly loaded and interrupted; continuing the session carries the import on from
/// the item the journal holds, without repeating any.
#[tokio::test]
async fn an_import_interrupted_by_ending_the_session_continues_with_the_next_process() {
    let d = TempDir::new();
    Transcript::deterministic(6)
        .user("the beginning")
        .filler(250, 20_000)
        .write(&claude_config(d.path()), &project(d.path()), ID);
    let mut c = cfg(d.path());
    c.history.chunk_bytes = 4 * 1024;
    let m = SessionManager::new(c).await.unwrap();
    let id = m.create(resume(d.path(), "claude", ID)).await.unwrap().id;
    let mut events = m.subscribe(&id, None).await.unwrap();
    until(&mut events, |e| {
        matches!(&e.kind, SessionEventKind::Earlier { earlier, .. }
            if earlier.state == EarlierState::Loading && earlier.items > 0)
    })
    .await;
    stopped(&m, &id).await;
    // The process ended with most of the history still to come: partly loaded, and it says so.
    let partial = m.snapshot(&id).unwrap().earlier;
    assert_eq!(partial.state, EarlierState::Loaded { complete: false });
    assert!(partial.gaps.interrupted);
    assert!(partial.items > 0);
    // Continuing the session carries the import on.
    m.continue_session(&id).await.unwrap();
    let done = wait_snapshot(&m, &id, "the history complete", |s| {
        s.earlier.state == EarlierState::Loaded { complete: true }
    })
    .await
    .earlier;
    assert!(!done.gaps.interrupted);
    assert!(done.items > partial.items);
    let items = earlier(&m, &id, HistoryScope::All, 500).await;
    assert_eq!(items.len(), 1 + 250 * 3 + 250);
    assert_eq!(items.len() as u64, done.items);
    // Each question once, in order.
    let questions: Vec<String> = items
        .iter()
        .map(describe)
        .filter(|s| s.starts_with("user:question"))
        .collect();
    let want: Vec<String> = (0..250).map(|i| format!("user:question {i}")).collect();
    assert_eq!(questions, want);
    assert_eq!(journal_notes(d.path(), &id, "earlier_end").len(), 1);
}
