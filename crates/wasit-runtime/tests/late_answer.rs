//! A late answer: an answer to an external gate that was withdrawn is journaled, refused with a
//! typed error that says why, and the session tells the user it arrived too late (a notice right
//! behind the answer's own input record). Real shim, fake CLI.

mod common;

use common::*;
use wasit_core::{
    AgentEvent, Command, GateChoice, GateChoiceKind, GateDecision, GateReason, GateRequest,
};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{CommandError, CommandStatus, NoticeCode, SessionEventKind};

fn handoff(rid: &str) -> GateRequest {
    GateRequest::Handoff {
        request_id: rid.into(),
        task_id: "task-1".into(),
        from: "h1/orchestrator".into(),
        to: "h2/worker".into(),
        mode: "consult".into(),
        instruction: "what is 2+2?".into(),
        details: serde_json::Value::Null,
        choices: vec![GateChoice::agent(
            "allow_once",
            "Allow once",
            GateChoiceKind::AllowOnce,
        )],
    }
}

fn answer(rid: &str) -> Command {
    Command::AnswerGate {
        request_id: rid.into(),
        decision: GateDecision::choose("allow_once"),
    }
}

#[tokio::test]
async fn a_late_answer_is_refused_with_its_reason_and_the_session_says_it_was_not_delivered() {
    let d = TempDir::new();
    let req = create(d.path(), "claude", &Scenario::builder().build());
    let mgr = manager(d.path()).await;
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.raise_gate(&id, handoff("late-1")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    let reason = GateReason::new("turn_ended").with_detail("the asking turn ended");
    mgr.resolve_gate(&id, "late-1", reason.clone())
        .await
        .unwrap();
    wait_phase(&mgr, &id, "idle").await;

    // The process is running, so the answer is journaled; the core refuses it as withdrawn.
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    assert_eq!(
        mgr.command(&id, answer("late-1")).await,
        Err(CommandError::GateWithdrawn {
            request_id: "late-1".into(),
            reason: Some(reason),
        })
    );
    let seen = until(&mut s, |e| {
        matches!(
            &e.kind,
            SessionEventKind::Notice {
                code: NoticeCode::AnswerNotDelivered,
                ..
            }
        )
    })
    .await;
    let n = seen.len();
    assert!(
        n >= 2,
        "{:?}",
        seen.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );
    // The answer's input record (journaled, status withdrawn) comes right before the notice.
    assert!(
        matches!(
            &seen[n - 2].kind,
            SessionEventKind::Input {
                command: Command::AnswerGate { request_id, .. },
                status: CommandStatus::Withdrawn { request_id: rid, .. },
                ..
            } if request_id == "late-1" && rid == "late-1"
        ),
        "{:?}",
        seen[n - 2].kind
    );
    match &seen[n - 1].kind {
        SessionEventKind::Notice { message, .. } => assert_eq!(
            message,
            "your answer arrived after the request was withdrawn (the asking turn ended) and was not delivered"
        ),
        other => panic!("{other:?}"),
    }
    // Nothing was written to the CLI for it, and the gate stays gone.
    assert!(mgr.snapshot(&id).unwrap().pending_gates.is_empty());
    assert!(!seen
        .iter()
        .any(|e| is_agent(e, |a| matches!(a, AgentEvent::GateRequested { .. }))));
}

#[tokio::test]
async fn a_late_answer_to_a_gate_of_a_stopped_session_is_refused_without_a_notice() {
    // Nothing can be journaled for a session whose process is gone: the answer is refused with
    // the same typed error, and nothing is journaled, so no notice follows.
    let d = TempDir::new();
    let req = create(d.path(), "claude", &Scenario::builder().build());
    let mgr = manager(d.path()).await;
    let id = mgr.create(req).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.raise_gate(&id, handoff("late-2")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.stop(&id).await.unwrap();
    let seen = until(&mut s, |e| {
        matches!(e.kind, SessionEventKind::Exited { .. })
    })
    .await;
    assert!(
        !seen.iter().any(|e| matches!(
            e.kind,
            SessionEventKind::Notice {
                code: NoticeCode::AnswerNotDelivered,
                ..
            }
        )),
        "{:?}",
        seen.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );
    assert_eq!(
        mgr.command(&id, answer("late-2")).await,
        Err(CommandError::GateWithdrawn {
            request_id: "late-2".into(),
            reason: Some(GateReason::new(GateReason::SESSION_STOPPED)),
        })
    );
}
