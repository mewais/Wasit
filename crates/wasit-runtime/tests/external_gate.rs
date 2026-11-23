//! External gates (raised on a session from outside the CLI protocol): visible like CLI gates,
//! answered through the normal command path, withdrawable, and restored by replay after the
//! daemon restarts (real shims, fake CLI).

mod common;

use common::*;
use wasit_core::{
    AgentEvent, Command, GateChoiceKind, GateDecision, GateReason, GateRequest, TurnBinding,
};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::{CommandError, SessionEvent, SessionEventKind};

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
        mgr.command(&id, Command::prompt("hi")).await.unwrap();
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

    // Raise + take down: `gate_resolved` carries the resolver's reason code and detail, and a
    // late answer is refused with a typed error.
    mgr.raise_gate(&id, handoff("h-2")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    // A reason that is not a machine word is refused, and nothing is resolved.
    for bad in [
        GateReason::new(""),
        GateReason::new("has space"),
        GateReason::new("x".repeat(GateReason::CODE_MAX + 1)),
        GateReason::new("ok").with_detail(""),
        GateReason::new("ok").with_detail("x".repeat(GateReason::DETAIL_MAX + 1)),
    ] {
        assert!(
            matches!(
                mgr.resolve_gate(&id, "h-2", bad).await,
                Err(CommandError::Rejected { .. })
            ),
            "a malformed reason must be refused"
        );
    }
    assert_eq!(mgr.snapshot(&id).unwrap().pending_gates.len(), 1);
    mgr.resolve_gate(
        &id,
        "h-2",
        GateReason::new("turn_ended").with_detail("the asking turn ended"),
    )
    .await
    .unwrap();
    until(&mut s, |e| {
        is_resolved(
            e,
            "h-2",
            &GateReason::new("turn_ended").with_detail("the asking turn ended"),
        )
    })
    .await;
    wait_phase(&mgr, &id, "idle").await;
    assert!(matches!(
        mgr.resolve_gate(&id, "h-2", GateReason::new("withdrawn"))
            .await,
        Err(CommandError::Rejected { .. })
    ));
    assert_eq!(
        late_answer(&mgr, &id, "h-2").await,
        Err(CommandError::GateWithdrawn {
            request_id: "h-2".into()
        })
    );
    // A reason without a detail is carried as it is.
    mgr.raise_gate(&id, handoff("h-2b")).await.unwrap();
    wait_phase(&mgr, &id, "needs_you").await;
    mgr.resolve_gate(&id, "h-2b", GateReason::new("denied"))
        .await
        .unwrap();
    wait_phase(&mgr, &id, "idle").await;

    // The gate ends with the process, and the runtime says so with its own code; raising on a
    // stopped session is refused.
    mgr.raise_gate(&id, handoff("h-3")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.stop(&id).await.unwrap();
    until(&mut s, |e| {
        is_resolved(e, "h-3", &GateReason::new(GateReason::SESSION_STOPPED))
    })
    .await;
    let snap = wait_snapshot(&mgr, &id, "exit", |s| !s.phase.is_live()).await;
    assert!(snap.pending_gates.is_empty());
    assert!(matches!(
        late_answer(&mgr, &id, "h-3").await,
        Err(CommandError::GateWithdrawn { .. })
    ));
    assert!(matches!(
        mgr.raise_gate(&id, handoff("h-4")).await,
        Err(CommandError::NotRunning { .. })
    ));

    // Replay after a daemon restart gives exactly what the live stream said.
    let live = resolutions(&mgr, &id).await;
    assert_eq!(
        live,
        vec![
            (
                "h-2".to_string(),
                Some(GateReason::new("turn_ended").with_detail("the asking turn ended"))
            ),
            ("h-2b".to_string(), Some(GateReason::new("denied"))),
            (
                "h-3".to_string(),
                Some(GateReason::new(GateReason::SESSION_STOPPED))
            ),
        ]
    );
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "replayed", |s| s.loaded).await;
    assert_eq!(resolutions(&mgr, &id).await, live);
    drop(mgr);

    // A journal written before a resolution could say why (the `resolve_gate` notes have no
    // reason) replays as a plain withdrawal; everything else is unchanged.
    let path = d
        .path()
        .join("sessions")
        .join(id.as_str())
        .join("journal.ndjson");
    let mut stripped = 0;
    let mut out = String::new();
    for line in std::fs::read_to_string(&path).unwrap().lines() {
        let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
        if v["kind"] == "note" && v["data"]["wasit"] == "resolve_gate" {
            v["data"].as_object_mut().unwrap().remove("reason").unwrap();
            stripped += 1;
        }
        out.push_str(&format!("{v}\n"));
    }
    // h-2, the refused second resolution of it, and h-2b.
    assert_eq!(stripped, 3, "the journal holds all the resolve_gate notes");
    std::fs::write(&path, out).unwrap();
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "replayed old journal", |s| s.loaded).await;
    assert_eq!(
        resolutions(&mgr, &id).await,
        vec![
            (
                "h-2".to_string(),
                Some(GateReason::new(GateReason::WITHDRAWN))
            ),
            (
                "h-2b".to_string(),
                Some(GateReason::new(GateReason::WITHDRAWN))
            ),
            (
                "h-3".to_string(),
                Some(GateReason::new(GateReason::SESSION_STOPPED))
            ),
        ]
    );
}

/// Every external gate resolution in the session's history, oldest first (`h-1` was answered,
/// so it is not one): request id and reason.
async fn resolutions(
    mgr: &wasit_runtime::SessionManager,
    id: &wasit_runtime::wire::SessionId,
) -> Vec<(String, Option<GateReason>)> {
    let mut s = mgr.subscribe(id, None).await.unwrap();
    let seen = until(&mut s, |e| is_resolved_any(e, "h-3")).await;
    seen.iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::GateResolved { request_id, reason },
            } => Some((request_id.clone(), reason.clone())),
            _ => None,
        })
        .collect()
}

fn is_resolved_any(e: &wasit_runtime::wire::SessionEvent, rid: &str) -> bool {
    is_agent(
        e,
        |a| matches!(a, AgentEvent::GateResolved { request_id, .. } if request_id == rid),
    )
}

fn is_resolved(e: &wasit_runtime::wire::SessionEvent, rid: &str, why: &GateReason) -> bool {
    is_agent(e, |a| {
        matches!(a, AgentEvent::GateResolved { request_id, reason }
            if request_id == rid && reason.as_ref() == Some(why))
    })
}

async fn late_answer(
    mgr: &wasit_runtime::SessionManager,
    id: &wasit_runtime::wire::SessionId,
    rid: &str,
) -> Result<wasit_runtime::wire::CommandOutcome, CommandError> {
    mgr.command(
        id,
        Command::AnswerGate {
            request_id: rid.into(),
            decision: GateDecision::choose("allow_once"),
        },
    )
    .await
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

// ── gates bound to the turn that asked ─────────────────────────────────────

fn whole_turn() -> TurnBinding {
    TurnBinding::Turn {
        interrupted: Some("the user interrupted the turn".into()),
        ended: Some("the turn ended before an answer".into()),
    }
}

/// What matters about gates and the turn in `events`, in order: the interrupt, gate raises
/// and resolutions (request id, reason code, detail) and turn completions.
fn gate_trace(events: &[SessionEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Input {
                command: Command::Interrupt,
                ..
            } => Some("interrupt".to_string()),
            SessionEventKind::Agent {
                event: AgentEvent::GateRequested { gate },
            } if gate.request_id().starts_with("t-") || gate.request_id().starts_with("s-") => {
                Some(format!("requested:{}", gate.request_id()))
            }
            SessionEventKind::Agent {
                event: AgentEvent::GateResolved { request_id, reason },
            } if request_id.starts_with("t-") || request_id.starts_with("s-") => {
                let r = reason.as_ref().expect("an external gate says why");
                Some(format!(
                    "resolved:{request_id}:{}:{}",
                    r.code,
                    r.detail.as_deref().unwrap_or("-")
                ))
            }
            SessionEventKind::Agent {
                event:
                    AgentEvent::TurnCompleted {
                        was_interrupted, ..
                    },
            } => Some(format!("turn_completed:{was_interrupted}")),
            _ => None,
        })
        .collect()
}

/// A gate raised for the open turn is resolved by the session itself when the turn is
/// interrupted: right behind the interrupt's `input` event (adjacent, one step), before the
/// CLI's own events about it, before the turn completes — also when the daemon restarted
/// between the raise and the interrupt, and the history replays to the same order.
async fn bound_gate_is_resolved_by_the_interrupt(driver: &str) {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working on it").wait_for_interrupt())
        .build();
    let req = create(d.path(), driver, &sc);
    let id = {
        let mgr = manager(d.path()).await;
        let id = mgr.create(req).await.unwrap().id;
        wait_phase(&mgr, &id, "idle").await;
        mgr.command(&id, Command::prompt("go")).await.unwrap();
        wait_snapshot(&mgr, &id, "an open turn", |s| s.turn.is_some()).await;
        mgr.raise_gate_for_turn(&id, handoff("t-1"), whole_turn())
            .await
            .unwrap();
        // An unbound one beside it.
        mgr.raise_gate(&id, handoff("s-1")).await.unwrap();
        wait_snapshot(&mgr, &id, "both gates", |s| s.pending_gates.len() == 2).await;
        id
        // The manager is dropped: the shim keeps running.
    };
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let snap = wait_snapshot(&mgr, &id, "replayed gates", |s| {
        s.loaded && s.pending_gates.len() == 2 && s.turn.is_some()
    })
    .await;
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.command(&id, Command::Interrupt).await.unwrap();
    let seen = until(&mut s, is_turn_completed).await;
    let at = |f: &dyn Fn(&SessionEventKind) -> bool| seen.iter().position(|e| f(&e.kind));
    let interrupt = at(&|k| {
        matches!(
            k,
            SessionEventKind::Input {
                command: Command::Interrupt,
                ..
            }
        )
    })
    .expect("the interrupt is an event");
    assert!(
        matches!(
            &seen[interrupt + 1].kind,
            SessionEventKind::Agent {
                event: AgentEvent::GateResolved { request_id, reason: Some(r) }
            } if request_id == "t-1"
                && r.code == GateReason::TURN_INTERRUPTED
                && r.detail.as_deref() == Some("the user interrupted the turn")
        ),
        "the gate is resolved right behind the interrupt: {:#?}",
        seen.iter().map(|e| &e.kind).collect::<Vec<_>>()
    );
    assert_eq!(
        gate_trace(&seen),
        [
            "interrupt".to_string(),
            "resolved:t-1:turn_interrupted:the user interrupted the turn".into(),
            "turn_completed:true".into(),
        ],
        "the session's gate is not touched"
    );
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    assert_eq!(snap.pending_gates.len(), 1);
    assert_eq!(snap.pending_gates[0].gate.request_id(), "s-1");
    // The resolved one is withdrawn for good.
    assert_eq!(
        late_answer(&mgr, &id, "t-1").await,
        Err(CommandError::GateWithdrawn {
            request_id: "t-1".into()
        })
    );

    // Replay after another restart tells the same story in the same order.
    let live = gate_trace(&mgr.history(&id, None, 2000).await.unwrap().events);
    drop(mgr);
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    wait_snapshot(&mgr, &id, "replayed", |s| s.loaded).await;
    assert_eq!(
        gate_trace(&mgr.history(&id, None, 2000).await.unwrap().events),
        live
    );
    assert_eq!(
        live,
        [
            "requested:t-1",
            "requested:s-1",
            "interrupt",
            "resolved:t-1:turn_interrupted:the user interrupted the turn",
            "turn_completed:true"
        ]
    );
}

/// A turn that ends normally with the gate unanswered: the session resolves it with
/// `turn_ended` before the turn's completion event; a session's gate stays.
async fn bound_gate_is_resolved_when_the_turn_ends(driver: &str) {
    let d = TempDir::new();
    // The turn waits at a permission gate of the CLI, so it ends exactly when it is answered.
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .say("let me run something")
                .tool("Bash", serde_json::json!({"command": "make"}))
                .needs_permission()
                .result_if_allowed("built")
                .done()
                .say("all done"),
        )
        .build();
    let mgr = manager(d.path()).await;
    let id = mgr.create(create(d.path(), driver, &sc)).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.command(&id, Command::prompt("build it")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    let cli_gate = snap.pending_gates[0].gate.clone();
    mgr.raise_gate_for_turn(&id, handoff("t-1"), whole_turn())
        .await
        .unwrap();
    mgr.raise_gate(&id, handoff("s-1")).await.unwrap();
    let snap = wait_snapshot(&mgr, &id, "three gates", |s| s.pending_gates.len() == 3).await;
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    let decision =
        GateDecision::by_kind(&cli_gate, &[GateChoiceKind::AllowOnce]).expect("an allow choice");
    mgr.command(
        &id,
        Command::AnswerGate {
            request_id: cli_gate.request_id().into(),
            decision,
        },
    )
    .await
    .unwrap();
    let seen = until(&mut s, is_turn_completed).await;
    assert_eq!(
        gate_trace(&seen),
        [
            "resolved:t-1:turn_ended:the turn ended before an answer".to_string(),
            "turn_completed:false".into(),
        ]
    );
    let snap = wait_snapshot(&mgr, &id, "the session's gate only", |s| {
        s.turn.is_none() && s.pending_gates.len() == 1
    })
    .await;
    assert_eq!(snap.pending_gates[0].gate.request_id(), "s-1");
    assert_eq!(snap.phase.name(), "needs_you");
    // A gate raised for a turn when none is open belongs to the session: nothing to follow.
    mgr.raise_gate_for_turn(&id, handoff("t-2"), whole_turn())
        .await
        .unwrap();
    let snap = wait_snapshot(&mgr, &id, "both session gates", |s| {
        s.pending_gates.len() == 2
    })
    .await;
    assert!(snap
        .pending_gates
        .iter()
        .any(|g| g.gate.request_id() == "t-2"));
    // A malformed detail is refused before anything is journaled.
    let bad = TurnBinding::Turn {
        interrupted: Some(String::new()),
        ended: None,
    };
    assert!(matches!(
        mgr.raise_gate_for_turn(&id, handoff("t-3"), bad).await,
        Err(CommandError::Rejected { .. })
    ));
}

#[tokio::test]
async fn claude_bound_gate_is_resolved_by_the_interrupt() {
    bound_gate_is_resolved_by_the_interrupt("claude").await
}

#[tokio::test]
async fn opencode_bound_gate_is_resolved_by_the_interrupt() {
    bound_gate_is_resolved_by_the_interrupt("opencode").await
}

#[tokio::test]
async fn codex_bound_gate_is_resolved_by_the_interrupt() {
    bound_gate_is_resolved_by_the_interrupt("codex").await
}

#[tokio::test]
async fn kilo_bound_gate_is_resolved_by_the_interrupt() {
    bound_gate_is_resolved_by_the_interrupt("kilo").await
}

#[tokio::test]
async fn claude_bound_gate_is_resolved_when_the_turn_ends() {
    bound_gate_is_resolved_when_the_turn_ends("claude").await
}

#[tokio::test]
async fn opencode_bound_gate_is_resolved_when_the_turn_ends() {
    bound_gate_is_resolved_when_the_turn_ends("opencode").await
}

#[tokio::test]
async fn codex_bound_gate_is_resolved_when_the_turn_ends() {
    bound_gate_is_resolved_when_the_turn_ends("codex").await
}

#[tokio::test]
async fn kilo_bound_gate_is_resolved_when_the_turn_ends() {
    bound_gate_is_resolved_when_the_turn_ends("kilo").await
}
