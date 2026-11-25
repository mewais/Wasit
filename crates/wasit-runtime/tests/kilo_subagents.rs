//! Kilo's background subagents through the real stack (SessionManager → actor → shim → the
//! kilo-bridge → the fake `kilo acp`, which serves Kilo's HTTP and SSE routes too). A `task` the
//! parent starts in the background is a child session: the session keeps working while it runs,
//! steering reaches it, its permission requests are gates the user answers, and an interrupt of
//! the parent's turn leaves it running.

mod common;

use common::*;
use serde_json::json;
use wasit_core::{AgentEvent, Command, GateChoiceKind, GateDecision, GateRequest, TurnTrigger};
use wasit_fake_cli::scenario::{BackgroundStep, Scenario, Step, ToolStep, Turn};
use wasit_runtime::wire::{CommandOutcome, Phase, SessionEvent, SessionEventKind, SessionId};
use wasit_runtime::SessionManager;

fn say(text: &str) -> Step {
    Step::Say {
        text: text.into(),
        chunks: None,
    }
}

/// A background `task` that runs for `after_ms` and then does `steps`.
fn task(after_ms: u64, steps: Vec<Step>) -> BackgroundStep {
    BackgroundStep {
        description: "probe subagent".into(),
        subagent_type: Some("general".into()),
        after_ms,
        steps,
        then: vec![say("PARENT_SAW_CHILD")],
        ..BackgroundStep::default()
    }
}

/// The child id a `TaskStarted` event names.
fn child_of(events: &[SessionEvent]) -> Option<String> {
    events.iter().find_map(|e| match &e.kind {
        SessionEventKind::Agent {
            event: AgentEvent::TaskStarted { task_id, .. },
        } => Some(task_id.clone()),
        _ => None,
    })
}

/// The id of the child the turn started: the subagent is announced over the server's events,
/// which may arrive after the turn's own. Waits for it on the stream when `seen` lacks it.
async fn child_started(s: &mut wasit_runtime::SessionEventStream, seen: &[SessionEvent]) -> String {
    if let Some(child) = child_of(seen) {
        return child;
    }
    let events = until(s, |e| child_of(std::slice::from_ref(e)).is_some()).await;
    child_of(&events).expect("the task starts")
}

/// An event that belongs to the subagent `child` (its scope names it).
fn from_child(e: &SessionEvent, child: &str) -> bool {
    matches!(&e.kind, SessionEventKind::Agent {
        event: AgentEvent::Scoped { scope, .. },
    } if scope.agent_id.as_deref() == Some(child))
}

async fn start(mgr: &SessionManager, dir: &std::path::Path, sc: &Scenario) -> SessionId {
    let r = mgr.create(create(dir, "kilo", sc)).await.expect("create");
    assert!(r.unsupported.is_empty());
    wait_phase(mgr, &r.id, "idle").await;
    r.id
}

/// The owner's turn that started the task ends; the task keeps the session working. When the task
/// completes, the owner's own turn answers it, and the session goes idle.
#[tokio::test]
async fn a_background_child_keeps_the_session_working_until_it_completes() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(task(1500, vec![say("CHILD_DONE")]))
                .say("STARTED"),
        )
        .build();
    let id = start(&mgr, d.path(), &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let first = until(&mut s, is_turn_completed).await;
    let child = child_started(&mut s, &first).await;
    // The turn is over and the task still runs: the session says working, with no turn.
    let mid = mgr.snapshot(&id).unwrap();
    assert_eq!(mid.phase, Phase::Working, "{mid:#?}");
    assert!(
        mid.turn.is_none() && mid.background_running == 1,
        "{mid:#?}"
    );
    // The child completes: its own status (idle) and the owner's answer, a turn the agent starts.
    let rest = until(&mut s, |e| is_phase(e, "idle")).await;
    let completed = rest.iter().chain(first.iter()).find_map(|e| match &e.kind {
        SessionEventKind::Agent {
            event:
                AgentEvent::TaskEnded {
                    task_id,
                    status,
                    ok,
                    ..
                },
        } if *task_id == child => Some((status.clone(), *ok)),
        _ => None,
    });
    assert_eq!(completed, Some(("completed".into(), true)), "{rest:#?}");
    assert!(
        rest.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent {
                event: AgentEvent::TurnStarted {
                    trigger: TurnTrigger::Agent,
                    ..
                },
            }
        )),
        "the owner answers the completed task: {rest:#?}"
    );
    let end = wait_phase(&mgr, &id, "idle").await;
    assert_eq!(end.background_running, 0, "{end:#?}");
}

/// Steering a running subagent sends the message to the child: it reads it as its own user
/// message, while the owner's turn is over.
#[tokio::test]
async fn steering_a_running_subagent_reaches_the_child() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(task(60_000, vec![say("CHILD_WORKING")]))
                .say("STARTED"),
        )
        .build();
    let id = start(&mgr, d.path(), &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let first = until(&mut s, is_turn_completed).await;
    let child = child_started(&mut s, &first).await;
    let out = mgr
        .command(
            &id,
            Command::SteerSubagent {
                task_id: child.clone(),
                text: "STEER_MARK use the other file".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let steered = until(&mut s, |e| {
        from_child(e, &child) && serde_json::to_string(e).is_ok_and(|j| j.contains("STEER_MARK"))
    })
    .await;
    assert!(
        steered.last().is_some_and(|e| from_child(e, &child)),
        "{steered:#?}"
    );
    mgr.stop(&id).await.unwrap();
}

/// The subagent's permission request is a gate of the session: the user answers it, and the
/// subagent goes on with the answer.
#[tokio::test]
async fn a_subagent_permission_request_is_a_gate_the_user_answers() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let child_tool = Step::Tool(ToolStep {
        name: "Bash".into(),
        input: json!({"command": "make", "description": "Build"}),
        needs_permission: true,
        result_if_allowed: Some("built".into()),
        ..Default::default()
    });
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(task(200, vec![child_tool, say("CHILD_BUILT")]))
                .say("STARTED"),
        )
        .build();
    let id = start(&mgr, d.path(), &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let first = until(&mut s, is_turn_completed).await;
    let child = child_started(&mut s, &first).await;
    let gated = until(&mut s, |e| {
        matches!(
            &e.kind,
            SessionEventKind::Agent {
                event: AgentEvent::GateRequested {
                    gate: GateRequest::Permission { .. }
                },
            }
        )
    })
    .await;
    let gate = match &gated.last().expect("the gate event").kind {
        SessionEventKind::Agent {
            event: AgentEvent::GateRequested { gate },
        } => gate.clone(),
        other => panic!("{other:?}"),
    };
    let decision = GateDecision::by_kind(&gate, &[GateChoiceKind::AllowOnce]).expect("allow");
    let out = mgr
        .command(
            &id,
            Command::AnswerGate {
                request_id: gate.request_id().to_string(),
                decision,
            },
        )
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let done = until(&mut s, |e| {
        matches!(&e.kind, SessionEventKind::Agent {
            event: AgentEvent::TaskEnded { task_id, ok: true, .. },
        } if *task_id == child)
    })
    .await;
    let ran = done.iter().any(|e| match &e.kind {
        SessionEventKind::Agent {
            event: AgentEvent::Scoped { scope, event },
        } => {
            scope.agent_id.as_deref() == Some(child.as_str())
                && matches!(**event, AgentEvent::ToolCallFinished { ok: true, .. })
        }
        _ => false,
    });
    assert!(ran, "the subagent's command ran with the answer: {done:#?}");
    mgr.stop(&id).await.unwrap();
}

/// An interrupt ends the owner's running turn; the background subagent it started keeps running,
/// so the session stays working and no task ends.
#[tokio::test]
async fn an_interrupt_leaves_the_background_subagent_running() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(task(60_000, vec![say("CHILD_WORKING")]))
                .wait_for_interrupt(),
        )
        .build();
    let id = start(&mgr, d.path(), &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let started = until(&mut s, |e| {
        matches!(
            &e.kind,
            SessionEventKind::Agent {
                event: AgentEvent::TaskStarted { .. },
            }
        )
    })
    .await;
    let child = child_started(&mut s, &started).await;
    mgr.command(&id, Command::Interrupt).await.unwrap();
    let ended = until(&mut s, is_turn_completed).await;
    assert!(
        ended.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent {
                event: AgentEvent::TurnCompleted {
                    was_interrupted: true,
                    ..
                },
            }
        )),
        "{ended:#?}"
    );
    assert!(
        !ended.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent {
                event: AgentEvent::TaskEnded { .. },
            }
        )),
        "the subagent is not ended by the interrupt: {ended:#?}"
    );
    let snap = mgr.snapshot(&id).unwrap();
    assert_eq!(snap.phase, Phase::Working, "{snap:#?}");
    assert_eq!(snap.background_running, 1, "{snap:#?}");
    assert!(
        snap.tasks.iter().any(|t| t.task_id == child && t.running),
        "{snap:#?}"
    );
    mgr.stop(&id).await.unwrap();
}

/// Stopping a running subagent by its task id aborts its tree: the child ends stopped, and the
/// owner's turn, already over, is not touched. The session then goes idle with nothing running.
#[tokio::test]
async fn stopping_a_running_child_ends_it_stopped() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(task(60_000, vec![say("CHILD_WORKING")]))
                .say("STARTED"),
        )
        .build();
    let id = start(&mgr, d.path(), &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let first = until(&mut s, is_turn_completed).await;
    let child = child_started(&mut s, &first).await;
    let out = mgr
        .command(
            &id,
            Command::StopTask {
                task_id: child.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let ended = until(&mut s, |e| {
        matches!(&e.kind, SessionEventKind::Agent {
            event: AgentEvent::TaskEnded { task_id, .. },
        } if *task_id == child)
    })
    .await;
    let stopped = ended.iter().find_map(|e| match &e.kind {
        SessionEventKind::Agent {
            event:
                AgentEvent::TaskEnded {
                    task_id,
                    status,
                    ok,
                    ..
                },
        } if *task_id == child => Some((status.clone(), *ok)),
        _ => None,
    });
    assert_eq!(stopped, Some(("stopped".into(), false)), "{ended:#?}");
    assert!(
        !ended.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent {
                event: AgentEvent::TurnCompleted {
                    was_interrupted: true,
                    ..
                },
            }
        )),
        "the owner's turn was over and is not interrupted: {ended:#?}"
    );
    let end = wait_phase(&mgr, &id, "idle").await;
    assert_eq!(end.background_running, 0, "{end:#?}");
    assert!(
        end.tasks.iter().any(|t| t.task_id == child && !t.running),
        "{end:#?}"
    );
    mgr.stop(&id).await.unwrap();
}
