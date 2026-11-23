//! Background work through the real stack (SessionManager → actor → real shim → the fake claude
//! CLI, which models what claude 2.1.293 does: a subagent or shell command runs on after the turn
//! that started it ended, its lines stream while the session is idle, and when it ends the agent
//! starts a turn by itself). The session must say what is true: working while the task runs,
//! with or without a turn; whose turn is whose; what each subagent said; and it can be
//! interrupted and its tasks stopped.

mod common;

use common::*;
use serde_json::json;
use wasit_core::{AgentEvent, Command, TurnTrigger};
use wasit_fake_cli::scenario::{BackgroundStep, Scenario, Step, ToolStep, Turn};
use wasit_runtime::wire::{
    CommandOutcome, HistoryScope, Phase, PhaseCause, SessionEvent, SessionEventKind, SessionId,
};
use wasit_runtime::SessionManager;

fn say(text: &str) -> Step {
    Step::Say {
        text: text.into(),
        chunks: None,
    }
}

fn bash(after_ms: u64, then: Vec<Step>) -> BackgroundStep {
    BackgroundStep {
        kind: "local_bash".into(),
        description: "Run delayed echo".into(),
        command: Some("sleep 1; echo bgdone".into()),
        after_ms,
        then,
        ..BackgroundStep::default()
    }
}

fn agent(after_ms: u64, then: Vec<Step>, on_stop: Vec<Step>) -> BackgroundStep {
    BackgroundStep {
        kind: "local_agent".into(),
        description: "probe".into(),
        subagent_type: Some("general-purpose".into()),
        after_ms,
        steps: vec![
            Step::Tool(ToolStep {
                name: "Bash".into(),
                input: json!({"command": "echo hi", "description": "Say hi"}),
                result_if_allowed: Some("hi".into()),
                ..Default::default()
            }),
            say("hi from the subagent"),
        ],
        then,
        on_stop,
        ..BackgroundStep::default()
    }
}

fn scenario(task: BackgroundStep) -> Scenario {
    Scenario::builder()
        .turn(Turn::builder().background(task).say("STARTED"))
        .build()
}

async fn start(mgr: &SessionManager, dir: &std::path::Path, sc: &Scenario) -> SessionId {
    let r = mgr.create(create(dir, "claude", sc)).await.expect("create");
    wait_phase(mgr, &r.id, "idle").await;
    r.id
}

fn phases(events: &[SessionEvent]) -> Vec<(String, PhaseCause)> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Phase { phase, cause, .. } => {
                Some((phase.name().to_string(), cause.clone()))
            }
            _ => None,
        })
        .collect()
}

fn agent_events(events: &[SessionEvent]) -> Vec<&AgentEvent> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent { event } => Some(event),
            _ => None,
        })
        .collect()
}

/// The owner's report, end to end: the session stays `working` from the task's start to its end —
/// first as the turn that started it, then with no turn at all — and the turn the agent starts by
/// itself when the task ends is working again, attributed to nobody and said so.
#[tokio::test]
async fn a_session_stays_working_while_a_background_task_runs_and_says_whose_turn_is_whose() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = scenario(bash(400, vec![say("FINISHED")]));
    let id = start(&mgr, d.path(), &sc).await;
    assert_eq!(
        mgr.command(&id, Command::prompt("go")).await.unwrap(),
        CommandOutcome::Sent
    );
    // The first turn ends while the task runs: the snapshot says working with no turn.
    let mid = wait_snapshot(&mgr, &id, "background work with no turn", |s| {
        s.turns_completed == 1 && s.turn.is_none() && s.background_running == 1
    })
    .await;
    assert_eq!(mid.phase, Phase::Working, "{mid:#?}");
    assert_eq!(mid.tasks.len(), 1);
    assert!(mid.tasks[0].running && mid.tasks[0].background);
    assert_eq!(mid.tasks[0].kind, "local_bash");
    // The task ends, the agent answers it with a turn of its own.
    let end = wait_snapshot(&mgr, &id, "the agent's own turn done", |s| {
        s.turns_completed == 2 && s.phase == Phase::Idle
    })
    .await;
    assert_eq!(end.background_running, 0);
    assert!(!end.tasks[0].running);
    assert_eq!(end.tasks[0].status.as_deref(), Some("completed"));
    assert_eq!(end.tasks[0].ok, Some(true));
    assert_eq!(
        end.last_turn
            .as_ref()
            .unwrap()
            .origin
            .as_ref()
            .map(|o| o.kind.as_str()),
        Some("task-notification")
    );
    let all = mgr.history(&id, None, 2000).await.unwrap().events;
    let ph = phases(&all);
    let names: Vec<&str> = ph.iter().map(|(p, _)| p.as_str()).collect();
    // idle (handshake), working (our turn: stays working through the task), idle (the task
    // ended, nothing running for an instant: the CLI prints the agent's turn a moment later),
    // working (the agent's turn), idle.
    assert_eq!(
        names,
        ["idle", "working", "idle", "working", "idle"],
        "{ph:#?}"
    );
    assert!(matches!(
        &ph[1].1,
        PhaseCause::TurnStarted {
            input: Some(_),
            trigger: TurnTrigger::Input
        }
    ));
    assert!(
        matches!(&ph[2].1, PhaseCause::TaskEnded { .. }),
        "{:?}",
        ph[2]
    );
    assert!(matches!(
        &ph[3].1,
        PhaseCause::TurnStarted {
            input: None,
            trigger: TurnTrigger::Agent
        }
    ));
    // Between the start of our turn and the end of the task there is no `idle`, although the
    // turn ended in between (the first `turn_completed` precedes the `idle`).
    let first_end = all.iter().position(is_turn_completed).unwrap();
    let idle_at = all
        .iter()
        .position(|e| is_phase(e, "idle") && e.cursor > all[first_end].cursor)
        .unwrap();
    let task_end = all
        .iter()
        .position(|e| is_agent(e, |a| matches!(a, AgentEvent::TaskEnded { .. })))
        .unwrap();
    assert!(first_end < task_end && task_end < idle_at);
    assert_monotonic(&all);
    // The trail (the content-free projection diagnostics read) tells the same story in words.
    let trail = mgr.trail_page(&id, None, 2000).await.unwrap();
    let json = serde_json::to_string(&trail.entries).unwrap();
    for needle in [
        r#""ev":"turn.started","trigger":"input""#,
        r#""ev":"turn.started","trigger":"agent""#,
        r#""ev":"task.started""#,
        r#""ev":"task.ended""#,
        // (The phase was `working` already when the task started: no phase change, no cause.)
        r#""kind":"task_ended""#,
        r#""origin":"task-notification""#,
    ] {
        assert!(json.contains(needle), "{needle} not in {json}");
    }
    assert!(
        !json.contains("Run delayed echo") && !json.contains("sleep 1"),
        "{json}"
    );
    mgr.stop(&id).await.unwrap();
}

/// A background subagent through the stack: attributed lines, running until its task ends, a
/// transcript of its own, and nothing of it in the session's.
#[tokio::test]
async fn a_background_subagent_is_attributed_and_has_a_transcript_of_its_own() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = scenario(agent(300, vec![say("OK")], vec![]));
    let id = start(&mgr, d.path(), &sc).await;
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    // After the parent's turn: the subagent still runs and nobody is in a turn.
    let mid = wait_snapshot(&mgr, &id, "the subagent running with no turn", |s| {
        s.turns_completed == 1 && s.turn.is_none() && s.subagents.first().is_some_and(|a| a.running)
    })
    .await;
    assert_eq!(mid.phase, Phase::Working);
    assert_eq!(mid.subagents.len(), 1);
    // Done: it ended with its task, with what it did.
    let end = wait_snapshot(&mgr, &id, "the subagent ended", |s| {
        s.turns_completed >= 2 && s.phase == Phase::Idle
    })
    .await;
    let sub = &end.subagents[0];
    assert!(!sub.running);
    assert_eq!(sub.ok, Some(true));
    assert_eq!(sub.status.as_deref(), Some("completed"));
    assert!(sub.task_id.is_some() && sub.agent_id.is_some());
    assert_eq!(sub.tool_calls, 1);
    assert_eq!(sub.last_tool.as_deref(), Some("Bash"));
    assert_eq!(end.tasks.len(), 1);
    // No anomaly: everything the subagent did is explained.
    let all = mgr.history(&id, None, 2000).await.unwrap().events;
    assert!(
        !all.iter()
            .any(|e| matches!(&e.kind, SessionEventKind::Anomaly { .. })),
        "{all:#?}"
    );
    // The subagent's transcript: its own lines, none of the session's.
    let call = sub.call_id.clone();
    let sub_scope = HistoryScope::Subagent {
        call_id: call.clone(),
    };
    let sub_page = mgr
        .history_scoped(&id, None, 2000, sub_scope.clone())
        .await
        .unwrap();
    assert!(!sub_page.has_more);
    let text: String = agent_events(&sub_page.events)
        .iter()
        .filter_map(|e| match e {
            AgentEvent::Scoped { event, .. } => match &**event {
                AgentEvent::AssistantDelta { text, .. } => Some(text.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(text, "hi from the subagent");
    let kinds: Vec<&str> = agent_events(&sub_page.events)
        .iter()
        .map(|e| match e {
            AgentEvent::Scoped { event, .. } => match &**event {
                AgentEvent::ToolCallStarted { .. } => "tool_started",
                AgentEvent::ToolCallFinished { .. } => "tool_finished",
                AgentEvent::AssistantDelta { .. } => "text",
                AgentEvent::UsageUpdate { .. } => "usage",
                _ => "other",
            },
            AgentEvent::TaskStarted { .. } => "task_started",
            AgentEvent::TaskEnded { .. } => "task_ended",
            _ => "unexpected",
        })
        .collect();
    assert_eq!(kinds[0], "task_started");
    assert_eq!(*kinds.last().unwrap(), "task_ended");
    assert!(
        kinds.contains(&"tool_started")
            && kinds.contains(&"tool_finished")
            && kinds.contains(&"text")
    );
    assert!(
        !kinds.contains(&"unexpected") && !kinds.contains(&"other"),
        "{kinds:?}"
    );
    // (`usage` is the subagent's own usage report, kept apart from the session's.)
    // The session's own transcript has none of it, and everything else.
    let main = mgr
        .history_scoped(&id, None, 2000, HistoryScope::Main)
        .await
        .unwrap();
    assert!(!agent_events(&main.events)
        .iter()
        .any(|e| matches!(e, AgentEvent::Scoped { .. })));
    let main_text = assistant_text(&main.events);
    assert!(
        main_text.contains("STARTED") && main_text.contains("OK"),
        "{main_text}"
    );
    assert!(!main_text.contains("hi from the subagent"));
    // The two scopes and the rest are the whole stream, and `Main` ∪ scoped == All.
    let scoped_count = all
        .iter()
        .filter(|e| is_agent(e, |a| matches!(a, AgentEvent::Scoped { .. })))
        .count();
    assert_eq!(main.events.len() + scoped_count, all.len());
    // Paging a subagent's transcript is paging the filtered stream: two events at a time.
    let mut paged: Vec<SessionEvent> = vec![];
    let mut before = None;
    loop {
        let p = mgr
            .history_scoped(&id, before, 2, sub_scope.clone())
            .await
            .unwrap();
        assert!(p.events.len() <= 2);
        before = p.events.first().map(|e| e.cursor);
        let mut page = p.events.clone();
        page.extend(paged);
        paged = page;
        if !p.has_more {
            break;
        }
    }
    assert_eq!(paged, sub_page.events);
    // Another call's transcript is empty.
    let none = mgr
        .history_scoped(
            &id,
            None,
            100,
            HistoryScope::Subagent {
                call_id: "toolu_other".into(),
            },
        )
        .await
        .unwrap();
    assert!(none.events.is_empty() && !none.has_more);
    // The trail shows the tasks without their content.
    let trail = mgr.trail_page(&id, None, 2000).await.unwrap();
    let json = serde_json::to_string(&trail.entries).unwrap();
    assert!(
        json.contains("task.started") && json.contains("task.ended") && json.contains("\"scope\"")
    );
    assert!(
        !json.contains("hi from the subagent") && !json.contains("probe"),
        "{json}"
    );
    mgr.stop(&id).await.unwrap();
}

/// An interrupt stops a turn the agent started by itself (the `interrupt` of a driver that only
/// knew its own writes could not), and the session is idle afterwards.
#[tokio::test]
async fn an_interrupt_stops_a_turn_the_agent_started_by_itself() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = scenario(bash(
        200,
        vec![say("working on it"), Step::WaitForInterrupt],
    ));
    let id = start(&mgr, d.path(), &sc).await;
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let running = wait_snapshot(&mgr, &id, "the agent's own turn running", |s| {
        s.turns_completed == 1
            && s.turn
                .as_ref()
                .is_some_and(|t| t.trigger == TurnTrigger::Agent)
    })
    .await;
    assert_eq!(running.phase, Phase::Working);
    assert_eq!(running.turn.as_ref().unwrap().input, None);
    assert_eq!(
        mgr.command(&id, Command::Interrupt).await.unwrap(),
        CommandOutcome::Sent
    );
    let end = wait_snapshot(&mgr, &id, "the agent's turn cancelled", |s| {
        s.turns_completed == 2 && s.phase == Phase::Idle
    })
    .await;
    let last = end.last_turn.unwrap();
    assert!(last.was_interrupted);
    assert_eq!(last.end_reason, Some(wasit_core::EndReason::Cancelled));
    assert!(last.origin.is_some());
    mgr.stop(&id).await.unwrap();
}

/// When no turn runs but a background task does, `Interrupt` stops the task (`interrupt` itself
/// does not); `StopTask` stops one task. A stopped command starts no turn; a stopped subagent
/// makes the agent report it.
#[tokio::test]
async fn interrupt_and_stop_task_stop_background_work() {
    // A command, stopped by Interrupt.
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = scenario(bash(120_000, vec![say("never")]));
    let id = start(&mgr, d.path(), &sc).await;
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let mid = wait_snapshot(&mgr, &id, "background work with no turn", |s| {
        s.turns_completed == 1 && s.turn.is_none() && s.background_running == 1
    })
    .await;
    assert_eq!(mid.phase, Phase::Working);
    mgr.command(&id, Command::Interrupt).await.unwrap();
    let end = wait_snapshot(&mgr, &id, "the task stopped", |s| s.background_running == 0).await;
    assert_eq!(
        end.phase,
        Phase::Idle,
        "stopping a command starts no turn: {end:#?}"
    );
    assert_eq!(end.tasks[0].status.as_deref(), Some("stopped"));
    assert_eq!(end.tasks[0].ok, Some(false));
    assert_eq!(end.turns_completed, 1);
    // Nothing is running any more: another interrupt is a no-op.
    mgr.command(&id, Command::Interrupt).await.unwrap();
    mgr.stop(&id).await.unwrap();
    drop(mgr);

    // A subagent, stopped by its task id: the agent answers with a turn of its own.
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = scenario(agent(
        120_000,
        vec![say("never")],
        vec![say("the agent was stopped")],
    ));
    let id = start(&mgr, d.path(), &sc).await;
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let mid = wait_snapshot(&mgr, &id, "the subagent running", |s| {
        s.turns_completed == 1
            && s.subagents
                .first()
                .is_some_and(|a| a.running && a.task_id.is_some())
    })
    .await;
    let task_id = mid.subagents[0].task_id.clone().unwrap();
    // A task that does not exist is refused here, not silently accepted.
    let err = mgr
        .command(
            &id,
            Command::StopTask {
                task_id: "nosuch".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no running task"), "{err}");
    mgr.command(&id, Command::StopTask { task_id })
        .await
        .unwrap();
    let end = wait_snapshot(&mgr, &id, "the agent reported the stop", |s| {
        s.turns_completed == 2 && s.phase == Phase::Idle
    })
    .await;
    assert_eq!(end.subagents[0].status.as_deref(), Some("stopped"));
    assert!(!end.subagents[0].running);
    assert_eq!(end.subagents[0].ok, Some(false));
    assert_eq!(
        end.last_turn.unwrap().reply.as_deref(),
        Some("the agent was stopped")
    );
    mgr.stop(&id).await.unwrap();
}

/// A prompt sent while the agent runs a turn of its own waits for it, then runs as its own turn,
/// attributed to its input; the agent's turn is nobody's.
#[tokio::test]
async fn a_prompt_sent_during_the_agents_own_turn_waits_and_gets_its_own_turn() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(bash(
                    150,
                    vec![say("working"), Step::SleepMs { ms: 600 }, say("FINISHED")],
                ))
                .say("STARTED"),
        )
        .turn(Turn::builder().say("PONG"))
        .build();
    let id = start(&mgr, d.path(), &sc).await;
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    wait_snapshot(&mgr, &id, "the agent's own turn running", |s| {
        s.turn
            .as_ref()
            .is_some_and(|t| t.trigger == TurnTrigger::Agent)
    })
    .await;
    let out = mgr.command(&id, Command::prompt("ping")).await.unwrap();
    assert_eq!(out, CommandOutcome::Queued, "held behind the agent's turn");
    let end = wait_snapshot(&mgr, &id, "the prompt's own turn done", |s| {
        s.turns_completed == 3 && s.phase == Phase::Idle
    })
    .await;
    assert_eq!(end.last_turn.unwrap().reply.as_deref(), Some("PONG"));
    let all = mgr.history(&id, None, 2000).await.unwrap().events;
    let starts: Vec<(Option<u64>, TurnTrigger)> = agent_events(&all)
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnStarted { input, trigger, .. } => Some((input.map(|i| i.0), *trigger)),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 3, "{starts:?}");
    assert_eq!(starts[1], (None, TurnTrigger::Agent));
    assert_eq!(starts[2].1, TurnTrigger::Input);
    // The prompt was sent after the agent's turn ended, and acknowledged by the CLI.
    let agent_end = all
        .iter()
        .enumerate()
        .filter(|(_, e)| is_turn_completed(e))
        .map(|(i, _)| i)
        .nth(1)
        .unwrap();
    let sent = all
        .iter()
        .position(|e| is_agent(e, |a| matches!(a, AgentEvent::InputSent { .. })))
        .unwrap();
    assert!(agent_end < sent);
    // Replay: a fresh runtime over the same journal tells the same story.
    mgr.stop(&id).await.unwrap();
    wait_snapshot(&mgr, &id, "exit", |s| !s.phase.is_live()).await;
    drop(mgr);
    let again = manager(d.path()).await;
    again.adopt_all().await.unwrap();
    let replayed = again.history(&id, None, 2000).await.unwrap().events;
    // (The live events end where the stop began; the replay adds its end.)
    assert_eq!(
        &replayed[..all.len()],
        &all[..],
        "a replay is the live stream"
    );
}
