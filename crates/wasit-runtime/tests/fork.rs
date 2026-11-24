//! `sessions.fork` and the capability guards of `Command::Compact` / `Command::RunShell`, through
//! the manager and a real shim running the fake CLI.
//!
//! The fake claude persona forks as claude does: `--resume <id> --fork-session` starts a new
//! conversation under a new id, copying the resumed one, which is left unchanged. The opencode
//! persona has no fork, no compaction and no shell command, so it is the refusal case.

mod common;

use std::path::Path;

use common::*;
use wasit_core::Command;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::{
    CommandError, CreateSessionReply, ForkedFrom, Phase, RuntimeError, SessionEvent,
    SessionEventKind, SessionId, SessionSnapshot,
};
use wasit_runtime::SessionManager;

// ── helpers ─────────────────────────────────────────────────────────────────

/// A scenario whose prompt `slow` runs until it is interrupted; every other prompt is echoed.
fn slow_scenario() -> Scenario {
    Scenario::builder()
        .turn(Turn::when("slow").wait_for_interrupt())
        .build()
}

async fn idle(mgr: &SessionManager, d: &Path, driver: &str, scenario: &Scenario) -> SessionId {
    let id = mgr.create(create(d, driver, scenario)).await.unwrap().id;
    wait_phase(mgr, &id, "idle").await;
    id
}

/// One completed turn of `text` (the echo reply).
async fn turn(mgr: &SessionManager, id: &SessionId, text: &str) {
    let snap = mgr.snapshot(id).unwrap();
    let mut s = mgr.subscribe(id, Some(snap.cursor)).await.unwrap();
    mgr.command(id, Command::prompt(text)).await.unwrap();
    until(&mut s, is_turn_completed).await;
}

async fn events(mgr: &SessionManager, id: &SessionId) -> Vec<SessionEvent> {
    mgr.history(id, None, 2000).await.unwrap().events
}

/// The `session.json` the runtime keeps for a session.
fn session_json(d: &Path, id: &SessionId) -> serde_json::Value {
    let text = std::fs::read_to_string(d.join("sessions").join(id.as_str()).join("session.json"))
        .expect("session.json");
    serde_json::from_str(&text).unwrap()
}

/// The files a fork must never change in its parent: the journal and the derived event log.
/// Missing files read as empty (a log that is not built yet has no bytes).
fn parent_bytes(d: &Path, id: &SessionId) -> Vec<(String, Vec<u8>)> {
    let dir = d.join("sessions").join(id.as_str());
    [
        "journal.ndjson",
        "events.ndjson",
        "earlier.ndjson",
        "meta.json",
    ]
    .iter()
    .map(|name| {
        let bytes = std::fs::read(dir.join(name)).unwrap_or_default();
        (name.to_string(), bytes)
    })
    .collect()
}

/// The argv of process generation `generation` (1-based).
fn argv_of(events: &[SessionEvent], generation: usize) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Spawned { argv, .. } => Some(argv.clone()),
            _ => None,
        })
        .nth(generation - 1)
        .unwrap_or_else(|| panic!("no generation {generation}"))
}

fn has_flag(argv: &[String], flag: &str, value: &str) -> bool {
    argv.windows(2).any(|w| w[0] == flag && w[1] == value)
}

fn fork_err(r: Result<CreateSessionReply, RuntimeError>) -> RuntimeError {
    match r {
        Ok(reply) => panic!("fork was accepted: {reply:?}"),
        Err(e) => e,
    }
}

/// Interrupt the running turn and wait for the session to be idle again.
async fn interrupt_to_idle(mgr: &SessionManager, id: &SessionId) {
    mgr.command(id, Command::Interrupt).await.unwrap();
    wait_phase(mgr, id, "idle").await;
}

// ── fork refusals ───────────────────────────────────────────────────────────

#[tokio::test]
async fn fork_is_refused_while_the_parent_is_working() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let parent = idle(&mgr, d.path(), "claude", &slow_scenario()).await;
    turn(&mgr, &parent, "hello").await;
    mgr.command(&parent, Command::prompt("slow")).await.unwrap();
    wait_phase(&mgr, &parent, "working").await;

    let err = fork_err(mgr.fork(&parent, None, None).await);
    assert!(matches!(err, RuntimeError::Busy(_)), "{err:?}");
    assert_eq!(mgr.list().len(), 1, "a refused fork creates no session");

    interrupt_to_idle(&mgr, &parent).await;
}

#[tokio::test]
async fn fork_is_refused_before_the_parent_completed_a_turn() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let parent = idle(&mgr, d.path(), "claude", &Scenario::default()).await;
    // The conversation id is known from creation, but there is nothing to copy yet.
    assert!(mgr.snapshot(&parent).unwrap().native_session_id.is_some());

    let err = fork_err(mgr.fork(&parent, None, None).await);
    assert!(matches!(err, RuntimeError::InvalidState(_)), "{err:?}");
    assert_eq!(mgr.list().len(), 1);
}

#[tokio::test]
async fn fork_is_refused_when_the_driver_cannot_fork() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    // opencode has no fork (its capability is false), whatever the session's state.
    let parent = idle(&mgr, d.path(), "opencode", &Scenario::default()).await;
    turn(&mgr, &parent, "hello").await;

    let err = fork_err(mgr.fork(&parent, None, None).await);
    assert!(matches!(err, RuntimeError::Unsupported(_)), "{err:?}");
    assert_eq!(mgr.list().len(), 1);
}

#[tokio::test]
async fn fork_is_refused_for_an_unknown_parent() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let ghost = SessionId::parse("aaaaaaaaaaaa").unwrap();
    let err = fork_err(mgr.fork(&ghost, None, None).await);
    assert!(matches!(err, RuntimeError::SessionNotFound(_)), "{err:?}");
}

// ── fork creation ───────────────────────────────────────────────────────────

#[tokio::test]
async fn fork_starts_a_session_resuming_the_parent_conversation_with_its_lineage() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let parent = idle(&mgr, d.path(), "claude", &Scenario::default()).await;
    turn(&mgr, &parent, "hello").await;
    let parent_snap = mgr.snapshot(&parent).unwrap();
    let parent_native = parent_snap
        .native_session_id
        .clone()
        .expect("parent native id");
    let parent_cwd = parent_snap.cwd.clone();

    let reply = mgr
        .fork(&parent, Some("  my fork  ".into()), None)
        .await
        .unwrap();
    let child = reply.id.clone();
    assert_ne!(child, parent);
    assert_eq!(reply.cwd, parent_cwd, "the fork runs where the parent runs");

    // The fork's spec resumes a fork of the parent's conversation.
    let file = session_json(d.path(), &child);
    assert_eq!(
        file["spec"]["resume"],
        serde_json::json!({"fork": parent_native}),
        "{file}"
    );
    assert_eq!(
        file["forked_from"]["session"],
        serde_json::json!(parent.as_str())
    );
    assert_eq!(
        file["forked_from"]["native_session_id"],
        serde_json::json!(parent_native)
    );
    assert_eq!(file["display_name"], serde_json::json!("my fork"));

    // The process is started with claude's fork flags, over the parent's conversation.
    wait_phase(&mgr, &child, "idle").await;
    let argv = argv_of(&events(&mgr, &child).await, 1);
    assert!(argv.iter().any(|a| a == "--fork-session"), "{argv:?}");
    assert!(has_flag(&argv, "--resume", &parent_native), "{argv:?}");

    // The fork is a conversation of its own: claude reports its new id with the fork's first
    // turn (as it does for any conversation it starts).
    turn(&mgr, &child, "in the fork").await;
    let child_done = mgr.snapshot(&child).unwrap();
    assert_eq!(child_done.display_name.as_deref(), Some("my fork"));
    let child_native = child_done
        .native_session_id
        .clone()
        .expect("child native id");
    assert_ne!(child_native, parent_native);

    // The lineage is on the snapshot.
    assert_eq!(
        child_done.forked_from,
        Some(ForkedFrom {
            session: parent.clone(),
            native_session_id: parent_native.clone(),
        })
    );
    assert_eq!(mgr.snapshot(&parent).unwrap().forked_from, None);
    assert_eq!(
        mgr.snapshot(&child).unwrap().forked_from,
        child_done.forked_from
    );

    // The lineage survives a restart of the manager: it is kept in session.json.
    let lineage = child_done.forked_from.clone();
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let reloaded: SessionSnapshot = wait_snapshot(&mgr, &child, "reloaded", |s| s.loaded).await;
    assert_eq!(reloaded.forked_from, lineage);
    assert_eq!(
        session_json(d.path(), &child)["spec"]["resume"],
        serde_json::json!({"fork": parent_native})
    );
}

/// The seq of the last journal record in `bytes` (0 when there is none).
fn last_seq(bytes: &[u8]) -> u64 {
    let text = std::str::from_utf8(bytes).expect("journal is UTF-8");
    text.lines()
        .last()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("record")["seq"].as_u64())
        .map(|s| s.expect("seq"))
        .unwrap_or(0)
}

/// Asserts that `after` is `before` plus whole records the parent itself appended: `before` is
/// an exact prefix of `after`, the appended bytes are complete lines, and none of them mentions
/// a string of `child` (the child's ids, the fork's prompt).
fn assert_only_parent_appends(name: &str, before: &[u8], after: &[u8], child: &[&str]) {
    assert!(
        after.starts_with(before),
        "{name}: the bytes from before the fork are not a prefix of the bytes after it"
    );
    let appended = &after[before.len()..];
    assert!(
        appended.is_empty() || appended.ends_with(b"\n"),
        "{name}: the appended bytes end in a partial record"
    );
    let text = String::from_utf8_lossy(appended);
    for needle in child {
        assert!(
            !text.contains(needle),
            "{name}: an appended record mentions the child ({needle}): {text}"
        );
    }
}

/// Like [`assert_only_parent_appends`] for the journal, where every appended record also carries
/// the next seq: the parent's shim numbers its own records without a gap, so an appended record
/// it did not write shows as a seq that does not follow.
fn assert_journal_appends_are_sequential(before: &[u8], after: &[u8]) {
    let appended = &after[before.len()..];
    let mut seq = last_seq(before);
    for line in String::from_utf8_lossy(appended).lines() {
        let rec: serde_json::Value = serde_json::from_str(line).expect("appended record");
        seq += 1;
        assert_eq!(
            rec["seq"].as_u64(),
            Some(seq),
            "journal: an appended record does not continue the parent's seq: {line}"
        );
    }
}

#[tokio::test]
async fn fork_leaves_the_parents_journal_and_log_byte_identical() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let parent = idle(&mgr, d.path(), "claude", &Scenario::default()).await;
    turn(&mgr, &parent, "hello").await;
    // A live parent's shim keeps journaling after the turn is reported done (its command
    // lifecycle records follow the turn), so comparing bytes against a live parent races with
    // those writes. Stop the parent and let its log writer finish: the journal and log are then
    // final, and any change the fork made would show. (The live parent is covered by
    // `fork_of_a_live_parent_only_appends_the_parents_own_records`.)
    mgr.stop(&parent).await.unwrap();
    mgr.close_log(&parent).await.unwrap();
    // The parent's log is what a reader sees: build it before the fork, then compare.
    let before_events = events(&mgr, &parent).await;
    let before = parent_bytes(d.path(), &parent);
    let before_snap = mgr.snapshot(&parent).unwrap();
    assert!(
        before.iter().any(|(_, b)| !b.is_empty()),
        "the parent has bytes"
    );

    let child = mgr.fork(&parent, None, None).await.unwrap().id;
    wait_phase(&mgr, &child, "idle").await;
    turn(&mgr, &child, "in the fork").await;

    assert_eq!(
        parent_bytes(d.path(), &parent),
        before,
        "parent files changed"
    );
    assert_eq!(events(&mgr, &parent).await, before_events);
    let after_snap = mgr.snapshot(&parent).unwrap();
    assert_eq!(after_snap.phase, before_snap.phase);
    assert_eq!(after_snap.turns_completed, before_snap.turns_completed);
    assert_eq!(after_snap.native_session_id, before_snap.native_session_id);
}

#[tokio::test]
async fn fork_of_a_live_parent_only_appends_the_parents_own_records() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let parent = idle(&mgr, d.path(), "claude", &Scenario::default()).await;
    turn(&mgr, &parent, "hello").await;
    // The parent stays live and idle: its shim may still append its own records after the turn.
    // Whatever it appends, the bytes read before the fork must remain an exact prefix.
    let before_events = events(&mgr, &parent).await;
    let before = parent_bytes(d.path(), &parent);
    let before_snap = mgr.snapshot(&parent).unwrap();
    assert!(
        before.iter().any(|(_, b)| !b.is_empty()),
        "the parent has bytes"
    );

    let child = mgr.fork(&parent, None, None).await.unwrap().id;
    wait_phase(&mgr, &child, "idle").await;
    turn(&mgr, &child, "in the fork").await;
    let child_native = mgr
        .snapshot(&child)
        .unwrap()
        .native_session_id
        .expect("child native id");

    let after = parent_bytes(d.path(), &parent);
    let child_names = [child.as_str(), child_native.as_str(), "in the fork", "fork"];
    for ((name, b), (name_after, a)) in before.iter().zip(after.iter()) {
        assert_eq!(name, name_after);
        assert_only_parent_appends(name, b, a, &child_names);
    }
    let journal = |files: &[(String, Vec<u8>)]| {
        files
            .iter()
            .find(|(n, _)| n == "journal.ndjson")
            .unwrap()
            .1
            .clone()
    };
    assert_journal_appends_are_sequential(&journal(&before), &journal(&after));

    // The derived log is a prefix too: the events read before are still the first ones.
    let after_events = events(&mgr, &parent).await;
    assert!(
        after_events.len() >= before_events.len()
            && after_events[..before_events.len()] == before_events[..],
        "the parent's events read before the fork are not a prefix of those read after it"
    );
    let after_snap = mgr.snapshot(&parent).unwrap();
    assert_eq!(after_snap.phase, before_snap.phase);
    assert_eq!(after_snap.turns_completed, before_snap.turns_completed);
    assert_eq!(after_snap.native_session_id, before_snap.native_session_id);
}

// ── guards ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn compact_is_refused_when_the_driver_cannot_compact() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), "opencode", &Scenario::default()).await;
    let err = mgr.command(&id, Command::Compact).await.unwrap_err();
    assert_eq!(
        err,
        CommandError::Unsupported {
            operation: "compact".into()
        }
    );
}

#[tokio::test]
async fn run_shell_is_refused_when_the_driver_has_no_shell_command() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    // claude's run_shell capability is false.
    let id = idle(&mgr, d.path(), "claude", &Scenario::default()).await;
    let err = mgr
        .command(
            &id,
            Command::RunShell {
                line: "ls".into(),
                timeout_ms: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        CommandError::Unsupported {
            operation: "run_shell".into()
        }
    );
    // Refused before anything was journaled for it.
    assert!(!events(&mgr, &id).await.iter().any(|e| matches!(
        &e.kind,
        SessionEventKind::Input {
            command: Command::RunShell { .. },
            ..
        }
    )));
}

#[tokio::test]
async fn compact_is_refused_while_the_session_is_working() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), "claude", &slow_scenario()).await;
    mgr.command(&id, Command::prompt("slow")).await.unwrap();
    wait_phase(&mgr, &id, "working").await;

    let err = mgr.command(&id, Command::Compact).await.unwrap_err();
    assert_eq!(err, CommandError::Busy);

    interrupt_to_idle(&mgr, &id).await;
}

#[test]
fn phase_busy_is_the_starting_working_and_needs_you_phases() {
    assert!(Phase::Starting.is_busy());
    assert!(Phase::Working.is_busy());
    assert!(Phase::NeedsYou.is_busy());
    assert!(!Phase::Idle.is_busy());
    assert!(!Phase::Exited { code: Some(0) }.is_busy());
    assert!(!Phase::Lost {
        reason: String::new()
    }
    .is_busy());
}
