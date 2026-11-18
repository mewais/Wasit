//! External gates (raised on a session from outside the CLI protocol): visible like CLI gates,
//! answered through the normal command path, withdrawable, and restored by replay after the
//! daemon restarts (real shims, fake CLI).

mod common;

use common::*;
use wasit_core::{Command, GateDecision, GateRequest};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{CommandError, SessionEventKind};

fn handoff(rid: &str) -> GateRequest {
    GateRequest::Handoff {
        request_id: rid.into(),
        task_id: "task-1".into(),
        from: "h1/orchestrator".into(),
        to: "h2/worker".into(),
        mode: "consult".into(),
        instruction: "what is 2+2?".into(),
        details: serde_json::Value::Null,
        choices: vec![wasit_core::GateChoice::agent(
            "allow_once",
            "Allow once",
            wasit_core::GateChoiceKind::AllowOnce,
        )],
    }
}

async fn external_gate_survives_restart(driver: &str) {
    let d = TempDir::new();
    let req = create(d.path(), driver, &Scenario::builder().build());
    let id = {
        let mgr = manager(d.path()).await;
        let id = mgr.create(req).await.unwrap().id;
        wait_phase(&mgr, &id, "idle").await;
        mgr.raise_gate(&id, handoff("h-1")).await.unwrap();
        let snap = wait_phase(&mgr, &id, "needs_you").await;
        assert_eq!(snap.pending_gates.len(), 1);
        assert_eq!(snap.pending_gates[0].gate, handoff("h-1"));
        // Duplicate id: rejected.
        assert!(matches!(
            mgr.raise_gate(&id, handoff("h-1")).await,
            Err(CommandError::Rejected { .. })
        ));
        // A prompt still runs while the external gate is pending, and the gate outlives it.
        let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
        mgr.command(&id, Command::Prompt { text: "hi".into() })
            .await
            .unwrap();
        until(&mut s, is_turn_completed).await;
        let snap = wait_phase(&mgr, &id, "needs_you").await;
        assert_eq!(snap.pending_gates.len(), 1);
        id
        // Manager dropped: the shim keeps running.
    };
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let snap = wait_snapshot(&mgr, &id, "replayed gate", |s| {
        s.loaded && s.pending_gates.len() == 1
    })
    .await;
    assert_eq!(snap.phase.name(), "needs_you");
    let raised_at = snap.pending_gates[0].cursor;

    // A choice the gate does not offer is refused, typed, and the gate stays pending.
    let bad = mgr
        .command(
            &id,
            Command::AnswerGate {
                request_id: "h-1".into(),
                decision: GateDecision::choose("always_allow"),
            },
        )
        .await;
    assert!(matches!(bad, Err(CommandError::Rejected { .. })), "{bad:?}");
    assert_eq!(mgr.snapshot(&id).unwrap().pending_gates.len(), 1);
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.command(
        &id,
        Command::AnswerGate {
            request_id: "h-1".into(),
            decision: GateDecision::choose("allow_once"),
        },
    )
    .await
    .unwrap();
    let seen = until(&mut s, |e| {
        matches!(&e.kind, SessionEventKind::Input { command: Command::AnswerGate { request_id, .. }, .. } if request_id == "h-1")
    })
    .await;
    assert!(seen.last().unwrap().cursor > raised_at);
    wait_phase(&mgr, &id, "idle").await;
    // Answering again: no longer pending, so the protocol rejects it.
    assert!(mgr
        .command(
            &id,
            Command::AnswerGate {
                request_id: "h-1".into(),
                decision: GateDecision::choose("allow_once"),
            },
        )
        .await
        .is_err());

    // Raise + withdraw.
    mgr.raise_gate(&id, handoff("h-2")).await.unwrap();
    wait_phase(&mgr, &id, "needs_you").await;
    mgr.resolve_gate(&id, "h-2").await.unwrap();
    wait_phase(&mgr, &id, "idle").await;
    assert!(matches!(
        mgr.resolve_gate(&id, "h-2").await,
        Err(CommandError::Rejected { .. })
    ));

    // The gate ends with the process; raising on a stopped session is refused.
    mgr.raise_gate(&id, handoff("h-3")).await.unwrap();
    mgr.stop(&id).await.unwrap();
    let snap = wait_snapshot(&mgr, &id, "exit", |s| !s.phase.is_live()).await;
    assert!(snap.pending_gates.is_empty());
    assert!(matches!(
        mgr.raise_gate(&id, handoff("h-4")).await,
        Err(CommandError::NotRunning { .. })
    ));
}

#[tokio::test]
async fn claude_external_gate_survives_restart() {
    external_gate_survives_restart("claude").await
}

#[tokio::test]
async fn opencode_external_gate_survives_restart() {
    external_gate_survives_restart("opencode").await
}

#[tokio::test]
async fn codex_external_gate_survives_restart() {
    external_gate_survives_restart("codex").await
}
