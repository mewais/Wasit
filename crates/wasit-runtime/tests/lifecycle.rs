//! The session lifecycle through the whole runtime (manager → actor → real shim → the fake CLI
//! over each driver's real wire protocol): the working directory, a conversation id known from
//! creation, `continuable`, continuing an ended session (explicitly, and by sending it a prompt),
//! a name at create, and the command that opens the conversation in the user's own CLI.
//!
//! What each CLI does is what the real one does (see the fake's README): claude stores a
//! conversation with its first prompt and accepts a chosen id; codex reports a thread id at once
//! but stores it with its first turn; ACP agents (opencode, kilo) store a session when it is
//! created.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use wasit_core::{AgentEvent, Command, DriverId};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{
    CommandError, CommandOutcome, CreateSession, NotContinuable, Phase, RuntimeError, SessionEvent,
    SessionEventKind, SessionId, SessionSnapshot,
};
use wasit_runtime::{default_registry, SessionManager};

// ── helpers ─────────────────────────────────────────────────────────────────

async fn idle(mgr: &SessionManager, d: &Path, driver: &str) -> SessionId {
    let id = mgr
        .create(create(d, driver, &Scenario::default()))
        .await
        .unwrap()
        .id;
    wait_phase(mgr, &id, "idle").await;
    id
}

/// One turn: the prompt, then until it completed. Returns the events of the turn.
async fn turn(mgr: &SessionManager, id: &SessionId, text: &str) -> Vec<SessionEvent> {
    let snap = mgr.snapshot(id).unwrap();
    let mut s = mgr.subscribe(id, Some(snap.cursor)).await.unwrap();
    mgr.command(id, Command::prompt(text)).await.unwrap();
    until(&mut s, is_turn_completed).await
}

/// Stop and wait for the exit.
async fn end(mgr: &SessionManager, id: &SessionId) -> SessionSnapshot {
    mgr.stop(id).await.unwrap();
    wait_snapshot(mgr, id, "ended", |s| !s.phase.is_live()).await
}

async fn events(mgr: &SessionManager, id: &SessionId) -> Vec<SessionEvent> {
    mgr.history(id, None, 2000).await.unwrap().events
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

fn prompts(events: &[SessionEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Input {
                command: Command::Prompt { text, .. },
                ..
            } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// The journal of a session, as written by its shim.
fn journal(d: &Path, id: &SessionId) -> Vec<serde_json::Value> {
    std::fs::read_to_string(d.join("sessions").join(id.as_str()).join("journal.ndjson"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn session_json(d: &Path, id: &SessionId) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(d.join("sessions").join(id.as_str()).join("session.json"))
            .unwrap(),
    )
    .unwrap()
}

/// Returns once the session's shim is gone (it holds `shim.lock` for its lifetime; a blocking
/// lock is granted when it ends), so nothing writes into the session directory any more.
async fn shim_gone(d: &Path, id: &SessionId) {
    let lock = d.join("sessions").join(id.as_str()).join("shim.lock");
    tokio::task::spawn_blocking(move || {
        let f = std::fs::File::options()
            .read(true)
            .write(true)
            .open(lock)
            .unwrap();
        f.lock().unwrap();
    })
    .await
    .unwrap();
}

fn command_notes(d: &Path, id: &SessionId) -> usize {
    journal(d, id)
        .iter()
        .filter(|r| r["kind"] == "note" && r["data"]["wasit"] == "command")
        .count()
}

/// The transcripts claude's own storage (the fake's) holds for conversation `native`, over all
/// projects.
fn transcripts(d: &Path, native: &str) -> Vec<PathBuf> {
    let projects = d.join("agent-state/claude-config/projects");
    let Ok(rd) = std::fs::read_dir(projects) else {
        return Vec::new();
    };
    rd.flatten()
        .map(|p| p.path().join(format!("{native}.jsonl")))
        .filter(|p| p.is_file())
        .collect()
}

/// The driver knows a command with which the user opens a conversation in their own CLI.
fn has_user_command(driver: &str) -> bool {
    default_registry()
        .get(&DriverId::new(driver))
        .unwrap()
        .user_resume_argv("x")
        .is_some()
}

/// What each driver's CLI does about a conversation that never had a prompt (the facts of
/// `wasit_core::DriverDescriptor::saves_conversation` / `chosen_session_id`, which this checks).
#[derive(Clone, Copy)]
struct Facts {
    /// The runtime chooses the conversation's id at create.
    chosen: bool,
    /// The agent stores the conversation before its first turn.
    stored_before_prompt: bool,
}

fn facts(driver: &str) -> Facts {
    let desc = default_registry().get(&DriverId::new(driver)).unwrap();
    let f = match driver {
        "claude" => Facts {
            chosen: true,
            stored_before_prompt: false,
        },
        "codex" => Facts {
            chosen: false,
            stored_before_prompt: false,
        },
        _ => Facts {
            chosen: false,
            stored_before_prompt: true,
        },
    };
    assert_eq!(desc.chosen_session_id([7; 16]).is_some(), f.chosen);
    assert_eq!(
        desc.saves_conversation() == wasit_core::SavesConversation::WhenReported,
        f.chosen || f.stored_before_prompt
    );
    f
}

// ── a conversation id from creation; ended before any prompt ─────────────────

async fn never_prompted(driver: &str) {
    let f = facts(driver);
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), driver).await;
    let snap = mgr.snapshot(&id).unwrap();
    let native = snap.native_session_id.clone().expect("a native id");
    assert!(!snap.continuable);
    assert_eq!(snap.not_continuable, Some(NotContinuable::NotEnded));
    // The user's own command is offered only once the agent's storage holds the conversation.
    assert_eq!(
        snap.resume_command.is_some(),
        f.stored_before_prompt && has_user_command(driver),
        "{:?}",
        snap.resume_command
    );
    if f.chosen {
        // Known from creation: the first snapshot of the session had it, and so does
        // `session.json` before the agent says anything.
        assert_eq!(session_json(d.path(), &id)["native_session_id"], native);
    }
    let ended = end(&mgr, &id).await;
    assert_eq!(ended.native_session_id.as_deref(), Some(native.as_str()));
    let events_before = events(&mgr, &id).await;
    if f.chosen || f.stored_before_prompt {
        assert!(ended.continuable, "{:?}", ended.not_continuable);
        assert_eq!(ended.not_continuable, None);
        mgr.continue_session(&id).await.unwrap();
        let again = wait_snapshot(&mgr, &id, "generation 2 idle", |s| {
            s.generation == 2 && s.phase == Phase::Idle
        })
        .await;
        assert_eq!(again.native_session_id.as_deref(), Some(native.as_str()));
        let evs = events(&mgr, &id).await;
        let argv = argv_of(&evs, 2);
        if f.chosen {
            // Never stored by the agent: started again under the same id, not resumed.
            assert!(has_flag(&argv, "--session-id", &native), "{argv:?}");
            assert!(!argv.contains(&"--resume".to_string()), "{argv:?}");
        }
        // A turn, an end, a second continue: now the agent holds the conversation.
        turn(&mgr, &id, "hello").await;
        let ended = end(&mgr, &id).await;
        assert_eq!(ended.native_session_id.as_deref(), Some(native.as_str()));
        mgr.continue_session(&id).await.unwrap();
        let third = wait_snapshot(&mgr, &id, "generation 3 idle", |s| {
            s.generation == 3 && s.phase == Phase::Idle
        })
        .await;
        assert_eq!(third.native_session_id.as_deref(), Some(native.as_str()));
        if f.chosen {
            let argv = argv_of(&events(&mgr, &id).await, 3);
            assert!(has_flag(&argv, "--resume", &native), "{argv:?}");
            assert!(!argv.contains(&"--session-id".to_string()), "{argv:?}");
            // The agent's own storage holds ONE transcript for the conversation.
            assert_eq!(transcripts(d.path(), &native).len(), 1);
        }
        mgr.stop(&id).await.unwrap();
    } else {
        // The agent reported an id but stored nothing, and cannot start again under it: the
        // snapshot says so, and neither continue nor a prompt pretends otherwise.
        assert!(!ended.continuable);
        assert_eq!(ended.not_continuable, Some(NotContinuable::NothingSaved));
        let e = mgr.continue_session(&id).await.unwrap_err();
        assert!(matches!(e, RuntimeError::InvalidState(_)), "{e:?}");
        let e = mgr
            .command(&id, Command::prompt("hello"))
            .await
            .unwrap_err();
        assert!(
            matches!(
                &e,
                CommandError::ContinueFailed {
                    blocked: Some(NotContinuable::NothingSaved),
                    ..
                }
            ),
            "{e:?}"
        );
        // Nothing was journaled for it and no process was started.
        let after = events(&mgr, &id).await;
        assert_eq!(after.len(), events_before.len());
        assert_eq!(command_notes(d.path(), &id), 0);
        assert_eq!(mgr.snapshot(&id).unwrap().generation, 1);
        assert!(matches!(
            mgr.snapshot(&id).unwrap().phase,
            Phase::Exited { .. }
        ));
    }
}

#[tokio::test]
async fn claude_never_prompted_starts_fresh_under_the_same_id_then_resumes() {
    never_prompted("claude").await
}
#[tokio::test]
async fn codex_never_prompted_cannot_be_continued_and_says_so() {
    never_prompted("codex").await
}
#[tokio::test]
async fn opencode_never_prompted_resumes() {
    never_prompted("opencode").await
}
#[tokio::test]
async fn kilo_never_prompted_resumes() {
    never_prompted("kilo").await
}

// ── after a turn ────────────────────────────────────────────────────────────

async fn after_a_turn(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), driver).await;
    turn(&mgr, &id, "one").await;
    let ended = end(&mgr, &id).await;
    let native = ended.native_session_id.clone().unwrap();
    assert!(ended.continuable, "{:?}", ended.not_continuable);
    mgr.continue_session(&id).await.unwrap();
    // What a continue answers `Ok` to is a snapshot that shows the new process.
    let at_once = mgr.snapshot(&id).unwrap();
    assert_eq!(at_once.generation, 2);
    assert!(at_once.phase.is_live(), "{:?}", at_once.phase);
    let again = wait_snapshot(&mgr, &id, "generation 2 idle", |s| {
        s.generation == 2 && s.phase == Phase::Idle
    })
    .await;
    assert_eq!(again.native_session_id.as_deref(), Some(native.as_str()));
    if driver == "claude" {
        let argv = argv_of(&events(&mgr, &id).await, 2);
        assert!(has_flag(&argv, "--resume", &native), "{argv:?}");
    }
    // The conversation goes on in the same session: the earlier turn is still in its history.
    let evs = turn(&mgr, &id, "two").await;
    assert!(assistant_text(&evs).contains("two"));
    let all = events(&mgr, &id).await;
    assert_eq!(prompts(&all), ["one", "two"]);
    assert_monotonic(&all);
    mgr.stop(&id).await.unwrap();
}

#[tokio::test]
async fn claude_after_a_turn_resumes() {
    after_a_turn("claude").await
}
#[tokio::test]
async fn codex_after_a_turn_resumes() {
    after_a_turn("codex").await
}
#[tokio::test]
async fn opencode_after_a_turn_resumes() {
    after_a_turn("opencode").await
}
#[tokio::test]
async fn kilo_after_a_turn_resumes() {
    after_a_turn("kilo").await
}

// ── a prompt to an ended session ────────────────────────────────────────────

/// The prompt continues the session and is delivered through the new generation's ordinary
/// input path; living through it and replaying it give the same events.
async fn prompt_to_an_ended_session(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), driver).await;
    turn(&mgr, &id, "one").await;
    let ended = end(&mgr, &id).await;
    let native = ended.native_session_id.clone().unwrap();
    let snap = mgr.snapshot(&id).unwrap();
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    let r = mgr.command(&id, Command::prompt("two")).await.unwrap();
    assert!(matches!(r, CommandOutcome::Queued | CommandOutcome::Sent));
    let evs = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&evs).contains("two"), "{evs:#?}");
    // Order: the new process, its start, then the prompt as an input of that generation.
    let at = |f: &dyn Fn(&SessionEventKind) -> bool| evs.iter().position(|e| f(&e.kind));
    let spawned = at(&|k| matches!(k, SessionEventKind::Spawned { generation: 2, .. })).unwrap();
    let started = at(&|k| matches!(k, SessionEventKind::Started { generation: 2, .. })).unwrap();
    let input = at(&|k| {
        matches!(
            k,
            SessionEventKind::Input {
                command: Command::Prompt { .. },
                ..
            }
        )
    })
    .unwrap();
    assert!(spawned < started && started < input, "{evs:#?}");
    let snap = mgr.snapshot(&id).unwrap();
    assert_eq!(snap.generation, 2);
    assert_eq!(snap.native_session_id.as_deref(), Some(native.as_str()));
    // Replay: a new manager over the same data reads exactly the same history.
    let live = events(&mgr, &id).await;
    assert_eq!(prompts(&live), ["one", "two"]);
    mgr.stop(&id).await.unwrap();
    let before = wait_snapshot(&mgr, &id, "ended again", |s| !s.phase.is_live()).await;
    let live = events(&mgr, &id).await;
    drop(s);
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let after = wait_snapshot(&mgr, &id, "replayed", |s| {
        s.loaded && s.cursor == before.cursor
    })
    .await;
    assert_eq!(events(&mgr, &id).await, live);
    assert_eq!(after.continuable, before.continuable);
    assert_eq!(after.not_continuable, before.not_continuable);
    assert_eq!(after.resume_command, before.resume_command);
    assert_eq!(after.native_session_id, before.native_session_id);
}

#[tokio::test]
async fn claude_prompt_to_an_ended_session_continues_it() {
    prompt_to_an_ended_session("claude").await
}
#[tokio::test]
async fn codex_prompt_to_an_ended_session_continues_it() {
    prompt_to_an_ended_session("codex").await
}
#[tokio::test]
async fn opencode_prompt_to_an_ended_session_continues_it() {
    prompt_to_an_ended_session("opencode").await
}
#[tokio::test]
async fn kilo_prompt_to_an_ended_session_continues_it() {
    prompt_to_an_ended_session("kilo").await
}

/// Two prompts that arrive while the continue is in progress are both delivered, in the order
/// they arrived, exactly once, to one new process.
async fn two_prompts_during_a_continue(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), driver).await;
    turn(&mgr, &id, "zero").await;
    let ended = end(&mgr, &id).await;
    let mut s = mgr.subscribe(&id, Some(ended.cursor)).await.unwrap();
    // `join!` polls in order, and a command reaches the actor's mailbox in its first poll.
    let (a, b) = tokio::join!(
        mgr.command(&id, Command::prompt("first")),
        mgr.command(&id, Command::prompt("second"))
    );
    a.unwrap();
    b.unwrap();
    // Until the agent took both (each is acknowledged, started as a turn, or dropped).
    let mut inputs: Vec<wasit_core::InputId> = Vec::new();
    let mut resolved: Vec<wasit_core::InputId> = Vec::new();
    loop {
        let e = next(&mut s).await;
        match &e.kind {
            SessionEventKind::Input {
                input,
                command: Command::Prompt { .. },
                ..
            } => inputs.push(*input),
            SessionEventKind::Agent {
                event:
                    AgentEvent::InputAcknowledged { input, .. }
                    | AgentEvent::InputDropped { input, .. }
                    | AgentEvent::TurnStarted {
                        input: Some(input), ..
                    },
            } => resolved.push(*input),
            _ => {}
        }
        if inputs.len() == 2 && inputs.iter().all(|i| resolved.contains(i)) {
            break;
        }
    }
    wait_snapshot(&mgr, &id, "settled", |s| {
        s.generation == 2 && matches!(s.phase, Phase::Idle | Phase::Working)
    })
    .await;
    let evs = events(&mgr, &id).await;
    assert_eq!(prompts(&evs), ["zero", "first", "second"]);
    let spawned = evs
        .iter()
        .filter(|e| matches!(e.kind, SessionEventKind::Spawned { .. }))
        .count();
    assert_eq!(spawned, 2, "one continue for both prompts");
    // Applied in the order they arrived, each exactly once.
    let order: Vec<wasit_core::InputId> = evs
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Input {
                input,
                command: Command::Prompt { text, .. },
                ..
            } if text != "zero" => Some(*input),
            _ => None,
        })
        .collect();
    assert_eq!(order, inputs);
    assert!(inputs[0] < inputs[1]);
    assert_eq!(command_notes(d.path(), &id), 3);
    mgr.stop(&id).await.unwrap();
}

#[tokio::test]
async fn claude_two_prompts_during_a_continue() {
    two_prompts_during_a_continue("claude").await
}
#[tokio::test]
async fn codex_two_prompts_during_a_continue() {
    two_prompts_during_a_continue("codex").await
}
#[tokio::test]
async fn opencode_two_prompts_during_a_continue() {
    two_prompts_during_a_continue("opencode").await
}
#[tokio::test]
async fn kilo_two_prompts_during_a_continue() {
    two_prompts_during_a_continue("kilo").await
}

/// A slash command continues an ended session like a prompt does (for a driver that has them).
#[tokio::test]
async fn claude_slash_command_to_an_ended_session_continues_it() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder().command("review", "Review").build();
    let id = mgr
        .create(create(d.path(), "claude", &sc))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    turn(&mgr, &id, "one").await;
    end(&mgr, &id).await;
    let r = mgr
        .command(
            &id,
            Command::RunSlash {
                name: "review".into(),
                args: String::new(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(r, CommandOutcome::Queued | CommandOutcome::Sent));
    wait_snapshot(&mgr, &id, "continued", |s| s.generation == 2).await;
    mgr.stop(&id).await.unwrap();
}

/// A driver with no slash commands refuses one as it always did, ended session or not.
#[tokio::test]
async fn a_command_the_driver_does_not_have_is_unsupported_not_a_continue() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), "codex").await;
    turn(&mgr, &id, "one").await;
    end(&mgr, &id).await;
    let e = mgr
        .command(
            &id,
            Command::RunSlash {
                name: "x".into(),
                args: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, CommandError::Unsupported { .. }), "{e:?}");
    assert_eq!(mgr.snapshot(&id).unwrap().generation, 1);
}

// ── a continue that fails ───────────────────────────────────────────────────

/// A continue that cannot start answers with a typed error that says so, journals nothing for the
/// prompt, and leaves the session as it was: still ended, still continuable.
#[tokio::test]
async fn a_continue_that_cannot_spawn_fails_typed_and_leaves_the_session_continuable() {
    let d = TempDir::new();
    let id = {
        let mgr = manager(d.path()).await;
        let id = idle(&mgr, d.path(), "claude").await;
        turn(&mgr, &id, "one").await;
        end(&mgr, &id).await;
        id
    };
    // The same data, a host whose shim launcher is gone.
    let mut cfg = config(d.path(), timeouts());
    cfg.launcher = wasit_runtime::ShimLauncher::binary("/nonexistent/wasit-shim");
    let mgr = SessionManager::new(cfg).await.unwrap();
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "loaded", |s| s.loaded).await;
    let notes = journal(d.path(), &id).len();
    let e = mgr.command(&id, Command::prompt("two")).await.unwrap_err();
    match &e {
        CommandError::ContinueFailed { reason, blocked } => {
            assert!(reason.contains("could not start"), "{reason}");
            assert_eq!(*blocked, None);
        }
        other => panic!("{other:?}"),
    }
    let e = mgr.continue_session(&id).await.unwrap_err();
    assert!(matches!(e, RuntimeError::Spawn(_)), "{e:?}");
    // Nothing was journaled for the prompt; the session is as it was.
    assert_eq!(journal(d.path(), &id).len(), notes);
    assert_eq!(command_notes(d.path(), &id), 1);
    let snap = mgr.snapshot(&id).unwrap();
    assert!(matches!(snap.phase, Phase::Exited { .. }));
    assert!(snap.continuable);
    assert_eq!(snap.generation, 1);
    // Nothing stays pending: the next attempt (on a host that can spawn) is not refused as "being
    // continued".
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "loaded", |s| s.loaded).await;
    mgr.command(&id, Command::prompt("three")).await.unwrap();
    wait_snapshot(&mgr, &id, "continued", |s| s.generation == 2).await;
    mgr.stop(&id).await.unwrap();
}

/// A process that dies as it starts answers the prompt held behind its continue with the
/// continue's failure and how the process ended, and nothing stays pending: the continue after
/// that (the CLI behaving again) works.
async fn a_new_process_that_dies_at_once(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc_path = d.path().join("scenario-dies.json");
    Scenario::default().write_to(&sc_path).unwrap();
    let mut req = create(d.path(), driver, &Scenario::default());
    req.config.env = vec![(
        "WASIT_FAKE_SCENARIO".into(),
        sc_path.to_string_lossy().into_owned(),
    )];
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    turn(&mgr, &id, "one").await;
    end(&mgr, &id).await;
    // From now on the CLI dies as it starts (the fake reads its scenario at each start).
    std::fs::write(
        &sc_path,
        r#"{"startup":[{"type":"stderr","text":"boom"},{"type":"exit","code":2}]}"#,
    )
    .unwrap();
    // The prompt is answered, one of two ways that the journal's order decides (the CLI's death
    // and the runtime's attach are two processes racing; neither order is the product's to
    // pick): the new process was already gone when the prompt reached its shim, so the continue
    // failed and says how it ended; or the shim took the prompt first, so it was journaled
    // into that process's window and is `Queued`, and the process died after. The actor's unit
    // tests pin each order. Either way the failure shows in the session and nothing stays pending.
    match mgr.command(&id, Command::prompt("two")).await {
        Err(CommandError::ContinueFailed { reason, .. }) => assert!(!reason.is_empty()),
        Ok(CommandOutcome::Queued) => {}
        other => panic!("{other:?}"),
    }
    let failed = wait_snapshot(&mgr, &id, "failed", |s| {
        s.generation == 2 && matches!(s.phase, Phase::Failed { .. })
    })
    .await;
    assert_eq!(failed.exit.as_ref().unwrap().code, Some(2));
    assert!(failed.continuable);
    // The CLI works again: nothing is stuck pending, the session continues.
    Scenario::default().write_to(&sc_path).unwrap();
    mgr.command(&id, Command::prompt("three")).await.unwrap();
    wait_snapshot(&mgr, &id, "third generation", |s| {
        s.generation == 3 && s.phase != Phase::Starting
    })
    .await;
    mgr.stop(&id).await.unwrap();
}

#[tokio::test]
async fn claude_a_new_process_that_dies_at_once() {
    a_new_process_that_dies_at_once("claude").await
}
#[tokio::test]
async fn kilo_a_new_process_that_dies_at_once() {
    a_new_process_that_dies_at_once("kilo").await
}

/// Claude refusing the resume (its storage lost the conversation) shows as the continue's
/// failure with the agent's own words, not as "not running".
#[tokio::test]
async fn a_resume_the_agent_refuses_fails_the_continue_with_its_reason() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = idle(&mgr, d.path(), "claude").await;
    turn(&mgr, &id, "one").await;
    let ended = end(&mgr, &id).await;
    let native = ended.native_session_id.unwrap();
    for t in transcripts(d.path(), &native) {
        std::fs::remove_file(t).unwrap();
    }
    // As in `a_new_process_that_dies_at_once`: the agent's refusal and the runtime's attach race,
    // and the journal's order decides whether the prompt reached the shim before the process
    // ended (`Queued`; the refusal shows in the session) or not (the continue's failure).
    match mgr.command(&id, Command::prompt("two")).await {
        Err(CommandError::ContinueFailed { reason, .. }) => {
            assert!(reason.contains("No conversation found"), "{reason}")
        }
        Ok(CommandOutcome::Queued) => {}
        other => panic!("{other:?}"),
    }
    let snap = wait_snapshot(&mgr, &id, "failed", |s| {
        s.generation == 2 && matches!(s.phase, Phase::Failed { .. })
    })
    .await;
    assert!(snap
        .stderr_tail
        .iter()
        .any(|l| l.contains("No conversation found")));
}

// ── the working directory ───────────────────────────────────────────────────

/// A temp "home" with `proj/sub`, canonical.
fn home() -> (TempDir, PathBuf) {
    let d = TempDir::new();
    // (Canonical, as the host resolves it: on Windows without the `\\?\` prefix.)
    let home = wasit_process::plain_path(&std::fs::canonicalize(d.path()).unwrap()).join("home");
    std::fs::create_dir_all(home.join("proj/sub")).unwrap();
    (d, home)
}

async fn manager_with_home(dir: &Path, home: &Path) -> SessionManager {
    let mut cfg = config(dir, timeouts());
    cfg.home = Some(home.to_path_buf());
    SessionManager::new(cfg).await.unwrap()
}

fn create_in(d: &Path, cwd: &str) -> CreateSession {
    let mut req = create(d, "claude", &Scenario::default());
    req.cwd = Some(cwd.to_string());
    req
}

/// `~` and `~/sub` are the real directories, in the spawn, in `session.json`, in the snapshot,
/// and in the process's own view of its working directory.
#[tokio::test]
async fn a_tilde_working_directory_is_the_home_and_is_used_everywhere() {
    let (d, home) = home();
    let mgr = manager_with_home(d.path(), &home).await;
    for (given, real) in [
        ("~", home.clone()),
        ("~/", home.clone()),
        ("~/proj/sub", home.join("proj/sub")),
        // `..` and a symlink: the canonical path is what the agent sees.
        ("~/proj/sub/..", home.join("proj")),
    ] {
        let real = real.to_str().unwrap().to_string();
        let reply = mgr.create(create_in(d.path(), given)).await.unwrap();
        let id = reply.id;
        // The reply echoes the normalised folder, for clients that remember project folders.
        assert_eq!(reply.cwd.as_deref(), Some(real.as_str()), "{given}");
        let snap = wait_phase(&mgr, &id, "idle").await;
        assert_eq!(snap.cwd.as_deref(), Some(real.as_str()), "{given}");
        assert_eq!(session_json(d.path(), &id)["cwd"], real, "{given}");
        assert_eq!(session_json(d.path(), &id)["spec"]["cwd"], real, "{given}");
        let j = journal(d.path(), &id);
        let spawned = j.iter().find(|r| r["kind"] == "spawned").unwrap();
        assert_eq!(spawned["data"]["cwd"], real, "{given}");
        // The CLI's own view of where it runs (claude reports it with its first turn).
        turn(&mgr, &id, "where").await;
        let j = journal(d.path(), &id);
        let init = j
            .iter()
            .filter(|r| r["kind"] == "out")
            .filter_map(|r| serde_json::from_str::<serde_json::Value>(r["data"].as_str()?).ok())
            .find(|m| m["type"] == "system" && m["subtype"] == "init")
            .unwrap();
        assert_eq!(init["cwd"], real, "{given}");
        mgr.stop(&id).await.unwrap();
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(home.join("proj"), home.join("link")).unwrap();
        let id = mgr
            .create(create_in(d.path(), "~/link/sub"))
            .await
            .unwrap()
            .id;
        let snap = wait_phase(&mgr, &id, "idle").await;
        assert_eq!(
            snap.cwd.as_deref(),
            Some(home.join("proj/sub").to_str().unwrap())
        );
        mgr.stop(&id).await.unwrap();
    }
}

#[tokio::test]
async fn a_relative_a_missing_or_a_file_working_directory_is_refused_naming_it_as_given() {
    let (d, home) = home();
    std::fs::write(home.join("file"), b"x").unwrap();
    let mgr = manager_with_home(d.path(), &home).await;
    for given in ["proj", ".", "../x", "~/missing", "~/file", "~someone/x"] {
        let e = mgr.create(create_in(d.path(), given)).await.unwrap_err();
        match e {
            RuntimeError::WorkingDirectory(m) => {
                assert!(m.contains(&format!("{given:?}")), "{given}: {m}")
            }
            other => panic!("{given}: {other:?}"),
        }
        assert_eq!(e_code(given), "bad_working_directory");
    }
    // Nothing was created for any of them.
    assert!(mgr.list().is_empty());
}

fn e_code(_given: &str) -> &'static str {
    RuntimeError::WorkingDirectory(String::new()).code()
}

/// What discovery lists for `~/proj/sub` and what a session launched there runs in agree: the
/// inventory is of the resolved folder, and the session the fake claude stored there is listed
/// with the id the session reports.
#[tokio::test]
async fn listing_resumable_sessions_and_launching_agree_on_the_folder() {
    let (d, home) = home();
    let mut cfg = config(d.path(), timeouts());
    cfg.home = Some(home.clone());
    // Where claude's own storage is (the fake keeps its transcripts there), as discovery reads it.
    let config_dir = d.path().join("agent-state/claude-config");
    cfg.overrides
        .get_mut(&DriverId::new("claude"))
        .unwrap()
        .env
        .push((
            "CLAUDE_CONFIG_DIR".into(),
            config_dir.to_string_lossy().into_owned(),
        ));
    let mgr = SessionManager::new(cfg.clone()).await.unwrap();
    let discovery =
        wasit_runtime::Discovery::new(wasit_runtime::DiscoveryConfig::from_runtime(&cfg));
    let service = std::sync::Arc::new(wasit_runtime::RuntimeService::new(
        mgr.clone(),
        discovery,
        "h",
        "test",
    ));
    let client = loopback(service);
    let id = mgr
        .create(create_in(d.path(), "~/proj/sub"))
        .await
        .unwrap()
        .id;
    wait_phase(&mgr, &id, "idle").await;
    turn(&mgr, &id, "hello").await;
    let native = mgr.snapshot(&id).unwrap().native_session_id.unwrap();
    let real = home.join("proj/sub").to_str().unwrap().to_string();
    // Asked for with the path as typed, the inventory is the resolved folder's, settled.
    let inv = wait_inventory(&client, "~/proj/sub", &native).await;
    let claude = inv
        .drivers
        .iter()
        .find(|x| x.driver.as_str() == "claude")
        .unwrap();
    assert_eq!(claude.project.as_deref(), Some(real.as_str()));
    assert!(claude.error.is_none(), "{:?}", claude.error);
    let listed = claude.sessions.iter().find(|s| s.id == native).unwrap();
    assert_eq!(listed.project.as_deref(), Some(real.as_str()));
    // A folder that is not there is reported in the inventory (not refused): it cannot be listed.
    let missing = wait_settled(&client, "~/missing").await;
    assert!(missing.drivers.iter().filter(|d| d.installed).all(|d| d
        .error
        .as_deref()
        .is_some_and(|e| e.contains("\"~/missing\""))));
    mgr.stop(&id).await.unwrap();
}

/// The inventory of `project` once no driver is refreshing it.
async fn wait_settled(
    client: &wasit_runtime::RuntimeClient,
    project: &str,
) -> wasit_runtime::wire::InventoryReply {
    let mut sub = client.subscribe_inventory(Some(project.to_string()));
    tokio::time::timeout(T, async {
        loop {
            let (_, inv) = sub.next().await.expect("open").expect("event");
            if inv.drivers.iter().all(|d| !d.refreshing) {
                return inv;
            }
        }
    })
    .await
    .expect("the inventory settles")
}

/// The settled inventory of `project` listing session `native` (a listing that predates the
/// transcript is refreshed once).
async fn wait_inventory(
    client: &wasit_runtime::RuntimeClient,
    project: &str,
    native: &str,
) -> wasit_runtime::wire::InventoryReply {
    let listed = |i: &wasit_runtime::wire::InventoryReply| {
        i.drivers
            .iter()
            .any(|d| d.sessions.iter().any(|s| s.id == native))
    };
    let inv = wait_settled(client, project).await;
    if listed(&inv) {
        return inv;
    }
    client
        .refresh(Some(DriverId::new("claude")), Some(project.to_string()))
        .await
        .unwrap();
    wait_settled(client, project).await
}

// ── a name at create ────────────────────────────────────────────────────────

#[tokio::test]
async fn a_name_at_create_is_the_display_name_and_reaches_the_agent_that_can_take_it() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    for (driver, takes_it) in [("claude", true), ("codex", false), ("kilo", false)] {
        let mut req = create(d.path(), driver, &Scenario::default());
        req.name = Some("  Fix the build  ".into());
        let id = mgr.create(req).await.unwrap().id;
        // At once, exactly as a rename would show it: trimmed.
        assert_eq!(
            mgr.snapshot(&id).unwrap().display_name.as_deref(),
            Some("Fix the build")
        );
        wait_phase(&mgr, &id, "idle").await;
        let argv = argv_of(&events(&mgr, &id).await, 1);
        assert_eq!(
            has_flag(&argv, "--name", "Fix the build"),
            takes_it,
            "{driver}: {argv:?}"
        );
        assert_eq!(session_json(d.path(), &id)["display_name"], "Fix the build");
        if driver == "claude" {
            // The agent's own storage holds the title, written with the first prompt.
            turn(&mgr, &id, "go").await;
            let native = mgr.snapshot(&id).unwrap().native_session_id.unwrap();
            let t = transcripts(d.path(), &native);
            let text = std::fs::read_to_string(&t[0]).unwrap();
            assert!(text.contains("\"customTitle\":\"Fix the build\""));
        }
        mgr.stop(&id).await.unwrap();
    }
    // A session that resumes an existing conversation is named in Marsad only: the conversation's
    // title is the user's.
    let first = idle(&mgr, d.path(), "claude").await;
    turn(&mgr, &first, "hello").await;
    let native = end(&mgr, &first).await.native_session_id.unwrap();
    let mut req = create(d.path(), "claude", &Scenario::default());
    req.resume = wasit_runtime::wire::ResumeRequest::Resume {
        native_session_id: native.clone(),
    };
    req.name = Some("Second look".into());
    let id = mgr.create(req).await.unwrap().id;
    assert_eq!(
        mgr.snapshot(&id).unwrap().display_name.as_deref(),
        Some("Second look")
    );
    // The id it resumes is its id from the start.
    assert_eq!(
        mgr.snapshot(&id).unwrap().native_session_id.as_deref(),
        Some(native.as_str())
    );
    wait_phase(&mgr, &id, "idle").await;
    let argv = argv_of(&events(&mgr, &id).await, 1);
    assert!(has_flag(&argv, "--resume", &native), "{argv:?}");
    assert!(!argv.contains(&"--name".to_string()), "{argv:?}");
    // A blank name is no name.
    let mut req = create(d.path(), "claude", &Scenario::default());
    req.name = Some("   ".into());
    let id = mgr.create(req).await.unwrap().id;
    assert_eq!(mgr.snapshot(&id).unwrap().display_name, None);
    let argv = argv_of(
        &{
            wait_phase(&mgr, &id, "idle").await;
            events(&mgr, &id).await
        },
        1,
    );
    assert!(!argv.contains(&"--name".to_string()));
}

/// The name goes to the agent when a generation starts the conversation (also again, when a
/// continue before the first prompt starts it fresh: the session's name as it is by then), never
/// when one resumes it. Renaming later changes Marsad's name only.
#[tokio::test]
async fn the_name_follows_the_conversation_that_is_started_not_the_one_resumed() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let mut req = create(d.path(), "claude", &Scenario::default());
    req.name = Some("First name".into());
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.rename(&id, Some("Second name".into())).await.unwrap();
    end(&mgr, &id).await;
    // Never prompted: the conversation starts now, under the current name.
    mgr.continue_session(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "generation 2", |s| {
        s.generation == 2 && s.phase == Phase::Idle
    })
    .await;
    let argv = argv_of(&events(&mgr, &id).await, 2);
    assert!(has_flag(&argv, "--name", "Second name"), "{argv:?}");
    turn(&mgr, &id, "go").await;
    mgr.rename(&id, Some("Third name".into())).await.unwrap();
    end(&mgr, &id).await;
    // The agent holds the conversation (with its title): resumed without a name.
    mgr.continue_session(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "generation 3", |s| {
        s.generation == 3 && s.phase == Phase::Idle
    })
    .await;
    let argv = argv_of(&events(&mgr, &id).await, 3);
    assert!(!argv.contains(&"--name".to_string()), "{argv:?}");
    assert_eq!(
        mgr.snapshot(&id).unwrap().display_name.as_deref(),
        Some("Third name")
    );
    mgr.stop(&id).await.unwrap();
}

// ── the user's own command ──────────────────────────────────────────────────

#[tokio::test]
async fn the_snapshot_carries_the_command_that_opens_the_conversation_in_the_users_cli() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let cwd = d.path().to_string_lossy().into_owned();
    for (driver, expect) in [
        ("claude", Some(vec!["claude", "--resume"])),
        ("codex", Some(vec!["codex", "resume"])),
        ("kilo", Some(vec!["kilo", "--session"])),
        ("opencode", None),
    ] {
        let id = idle(&mgr, d.path(), driver).await;
        let before = mgr.snapshot(&id).unwrap();
        // Offered once the agent's storage holds the conversation: ACP agents store a session
        // when they create it, claude and codex with the first turn.
        assert_eq!(
            before.resume_command.is_some(),
            facts(driver).stored_before_prompt && expect.is_some(),
            "{driver}"
        );
        turn(&mgr, &id, "hello").await;
        let snap = mgr.snapshot(&id).unwrap();
        let native = snap.native_session_id.clone().unwrap();
        match expect {
            Some(prefix) => {
                let c = snap
                    .resume_command
                    .clone()
                    .unwrap_or_else(|| panic!("{driver}"));
                let mut want: Vec<String> = prefix.iter().map(|s| s.to_string()).collect();
                want.push(native);
                assert_eq!(c.argv, want, "{driver}");
                assert_eq!(c.cwd, cwd, "{driver}");
            }
            None => assert_eq!(snap.resume_command, None, "{driver}"),
        }
        mgr.stop(&id).await.unwrap();
    }
}

// ── sessions of earlier builds ──────────────────────────────────────────────

/// What an older build wrote has no chosen id, no name and no `--session-id`: it replays, reports
/// `continuable` from what its journal says, and continues by the old rules.
#[tokio::test]
async fn a_session_of_the_previous_build_replays_and_follows_the_old_rules() {
    let d = TempDir::new();
    let (reported, never) = {
        let mgr = manager(d.path()).await;
        let a = idle(&mgr, d.path(), "claude").await;
        turn(&mgr, &a, "hello").await;
        end(&mgr, &a).await;
        let b = idle(&mgr, d.path(), "claude").await;
        end(&mgr, &b).await;
        (a, b)
    };
    // Rewritten below: nothing may still be appending to them.
    shim_gone(d.path(), &reported).await;
    shim_gone(d.path(), &never).await;
    // Rewrite both as the previous build wrote them: spec without the new fields, `start`
    // notes without them, argv without `--session-id`; the session that got no prompt never
    // learned an id.
    for id in [&reported, &never] {
        let dir = d.path().join("sessions").join(id.as_str());
        let mut file = session_json(d.path(), id);
        file["spec"]
            .as_object_mut()
            .unwrap()
            .remove("native_session_id");
        file["spec"].as_object_mut().unwrap().remove("name");
        if id == &never {
            file["native_session_id"] = serde_json::Value::Null;
        }
        std::fs::write(
            dir.join("session.json"),
            serde_json::to_vec_pretty(&file).unwrap(),
        )
        .unwrap();
        let text = std::fs::read_to_string(dir.join("journal.ndjson")).unwrap();
        let mut out = String::new();
        for line in text.lines() {
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            if v["kind"] == "note" && v["data"]["wasit"] == "start" {
                v["data"]["spec"]
                    .as_object_mut()
                    .unwrap()
                    .remove("native_session_id");
                v["data"]["spec"].as_object_mut().unwrap().remove("name");
            }
            if v["kind"] == "spawned" {
                let argv = v["data"]["argv"].as_array_mut().unwrap();
                if let Some(i) = argv.iter().position(|a| a == "--session-id") {
                    argv.drain(i..i + 2);
                }
            }
            out.push_str(&format!("{v}\n"));
        }
        std::fs::write(dir.join("journal.ndjson"), out).unwrap();
    }
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    // Reported an id: continuable, by resuming.
    let snap = wait_snapshot(&mgr, &reported, "loaded", |s| s.loaded).await;
    assert!(snap.continuable, "{:?}", snap.not_continuable);
    assert!(snap.native_session_id.is_some());
    assert!(snap.resume_command.is_some());
    // Never got an id: the snapshot says so truthfully, and the continue refuses as before.
    let snap = wait_snapshot(&mgr, &never, "loaded", |s| s.loaded).await;
    assert!(!snap.continuable);
    assert_eq!(snap.not_continuable, Some(NotContinuable::NoConversationId));
    assert_eq!(snap.native_session_id, None);
    let e = mgr.continue_session(&never).await.unwrap_err();
    assert!(matches!(e, RuntimeError::InvalidState(_)), "{e:?}");
    let e = mgr.command(&never, Command::prompt("x")).await.unwrap_err();
    assert!(
        matches!(
            &e,
            CommandError::ContinueFailed {
                blocked: Some(NotContinuable::NoConversationId),
                ..
            }
        ),
        "{e:?}"
    );
    // And the one that has an id continues: its first generation had no `--session-id`, and
    // resumes.
    mgr.continue_session(&reported).await.unwrap();
    wait_snapshot(&mgr, &reported, "continued", |s| {
        s.generation == 2 && s.phase == Phase::Idle
    })
    .await;
    let argv = argv_of(&events(&mgr, &reported).await, 2);
    assert!(argv.contains(&"--resume".to_string()), "{argv:?}");
    mgr.stop(&reported).await.unwrap();
}
