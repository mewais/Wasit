//! `session.local_shell`: a shell command the host ran for the user is journaled as a
//! `local_shell` event of origin `host` in the session's own journal: it reaches the live
//! subscription and the history, replays after a daemon restart, and an ended session refuses
//! it (nothing can journal it without a live shim). Real shims, fake CLI.

mod common;

use common::*;
use wasit_core::{AgentEvent, LocalShellOrigin};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{CommandError, LocalShellRecord, SessionEvent, SessionId};
use wasit_runtime::{SessionManager, MAX_HISTORY_PAGE};

fn shell(command: &str, output: &str) -> LocalShellRecord {
    LocalShellRecord {
        command: command.into(),
        cwd: Some("/work/project".into()),
        exit_code: Some(0),
        output: output.into(),
        output_truncated: false,
    }
}

fn is_local_shell(e: &SessionEvent, command: &str) -> bool {
    is_agent(
        e,
        |a| matches!(a, AgentEvent::LocalShell { command: c, .. } if c == command),
    )
}

/// Every event of the session, oldest first.
async fn all_events(mgr: &SessionManager, id: &SessionId) -> Vec<SessionEvent> {
    let mut pages = Vec::new();
    let mut before = None;
    loop {
        let page = mgr.history(id, before, MAX_HISTORY_PAGE).await.unwrap();
        if page.events.is_empty() {
            break;
        }
        before = page.events.first().map(|e| e.cursor);
        let more = page.has_more;
        pages.push(page.events);
        if !more {
            break;
        }
    }
    pages.into_iter().rev().flatten().collect()
}

fn local_shells(events: &[SessionEvent]) -> Vec<&SessionEvent> {
    events
        .iter()
        .filter(|e| is_agent(e, |a| matches!(a, AgentEvent::LocalShell { .. })))
        .collect()
}

async fn live_session_journals_and_replays(driver: &str) {
    let d = TempDir::new();
    let req = create(d.path(), driver, &Scenario::builder().build());
    let (id, live_event) = {
        let mgr = manager(d.path()).await;
        let id = mgr.create(req).await.unwrap().id;
        let snap = wait_phase(&mgr, &id, "idle").await;
        let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
        mgr.local_shell(&id, shell("ls", "a\nb\n")).await.unwrap();
        let seen = until(&mut s, |e| is_local_shell(e, "ls")).await;
        let event = seen.last().unwrap().clone();
        assert!(
            event.cursor > snap.cursor,
            "{driver}: event is after the snapshot"
        );
        assert!(matches!(
            &event.kind,
            wasit_runtime::wire::SessionEventKind::Agent {
                event: AgentEvent::LocalShell {
                    command,
                    cwd,
                    exit_code: Some(0),
                    output,
                    output_truncated: false,
                    origin: LocalShellOrigin::Host,
                },
            } if command == "ls" && cwd.as_deref() == Some("/work/project") && output == "a\nb\n"
        ));
        // The history has it too, once, in the same place.
        let history = all_events(&mgr, &id).await;
        assert_eq!(local_shells(&history).len(), 1, "{driver}: one record");
        assert_eq!(local_shells(&history)[0], &event);
        mgr.stop(&id).await.unwrap();
        wait_snapshot(&mgr, &id, "stopped", |s| !s.phase.is_live()).await;
        (id, event)
        // Manager dropped: the daemon restarts below.
    };
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "replayed", |s| s.loaded).await;
    let replayed = all_events(&mgr, &id).await;
    let found: Vec<_> = local_shells(&replayed).into_iter().cloned().collect();
    assert_eq!(
        found,
        vec![live_event],
        "{driver}: replay shows the same record"
    );
}

#[tokio::test]
async fn local_shell_is_journaled_streamed_and_replayed() {
    for d in DRIVERS {
        live_session_journals_and_replays(d).await;
    }
}

#[tokio::test]
async fn local_shell_keeps_its_place_among_agent_events() {
    let d = TempDir::new();
    let req = create(d.path(), "codex", &Scenario::builder().build());
    let mgr = manager(d.path()).await;
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.local_shell(&id, shell("first", "1")).await.unwrap();
    mgr.command(&id, wasit_core::Command::prompt("hi"))
        .await
        .unwrap();
    wait_turns_completed(&mgr, &id, 1).await;
    mgr.local_shell(&id, shell("second", "2")).await.unwrap();
    let events = all_events(&mgr, &id).await;
    let order: Vec<&str> = events
        .iter()
        .filter_map(|e| match &e.kind {
            wasit_runtime::wire::SessionEventKind::Agent {
                event: AgentEvent::LocalShell { command, .. },
            } => Some(command.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(order, ["first", "second"]);
    let turn = events
        .iter()
        .position(|e| is_agent(e, |a| matches!(a, AgentEvent::TurnCompleted { .. })))
        .expect("a turn completed");
    let first = events
        .iter()
        .position(|e| is_local_shell(e, "first"))
        .unwrap();
    let second = events
        .iter()
        .position(|e| is_local_shell(e, "second"))
        .unwrap();
    assert!(first < turn && turn < second, "journal order is kept");
}

async fn wait_turns_completed(mgr: &SessionManager, id: &SessionId, n: u64) {
    wait_snapshot(mgr, id, "turn completed", |s| s.turns_completed >= n).await;
}

#[tokio::test]
async fn ended_session_refuses_local_shell_and_journals_nothing() {
    let d = TempDir::new();
    let req = create(d.path(), "codex", &Scenario::builder().build());
    let mgr = manager(d.path()).await;
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.stop(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "stopped", |s| !s.phase.is_live()).await;
    let before = all_events(&mgr, &id).await.len();

    let refused = mgr.local_shell(&id, shell("ls", "x")).await;
    assert!(
        matches!(refused, Err(CommandError::NotRunning { .. })),
        "{refused:?}"
    );
    assert_eq!(all_events(&mgr, &id).await.len(), before);
    assert!(local_shells(&all_events(&mgr, &id).await).is_empty());
}

#[tokio::test]
async fn local_shell_output_is_capped_on_a_character_boundary() {
    let d = TempDir::new();
    let req = create(d.path(), "codex", &Scenario::builder().build());
    let mgr = manager(d.path()).await;
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;

    // Exactly the cap: kept whole.
    let exact = "x".repeat(LocalShellRecord::OUTPUT_MAX);
    mgr.local_shell(&id, shell("exact", &exact)).await.unwrap();
    // Over the cap with 3-byte characters: cut before the first character that does not fit.
    let wide = "€".repeat(LocalShellRecord::OUTPUT_MAX / 3 + 10);
    mgr.local_shell(&id, shell("wide", &wide)).await.unwrap();

    let events = all_events(&mgr, &id).await;
    let mut seen = std::collections::HashMap::new();
    for e in local_shells(&events) {
        if let wasit_runtime::wire::SessionEventKind::Agent {
            event:
                AgentEvent::LocalShell {
                    command,
                    output,
                    output_truncated,
                    ..
                },
        } = &e.kind
        {
            seen.insert(command.clone(), (output.len(), *output_truncated));
        }
    }
    assert_eq!(seen["exact"], (LocalShellRecord::OUTPUT_MAX, false));
    let kept = LocalShellRecord::OUTPUT_MAX / 3 * 3;
    assert_eq!(seen["wide"], (kept, true));

    // Output the host already cut stays marked, and an empty command is refused.
    let mut cut = shell("cut", "partial");
    cut.output_truncated = true;
    mgr.local_shell(&id, cut).await.unwrap();
    let events = all_events(&mgr, &id).await;
    assert!(events.iter().any(|e| is_agent(e, |a| matches!(
        a,
        AgentEvent::LocalShell { command, output_truncated: true, .. } if command == "cut"
    ))));
    assert!(matches!(
        mgr.local_shell(&id, shell("   ", "x")).await,
        Err(CommandError::Rejected { .. })
    ));
}
