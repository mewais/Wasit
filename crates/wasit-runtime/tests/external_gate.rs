//! External gates (raised on a session from outside the CLI protocol): visible like CLI gates,
//! answered through the normal command path, withdrawable, and restored by replay after the
//! daemon restarts (real shims, fake CLI).

mod common;

use common::*;
use wasit_core::{AgentEvent, Command, GateDecision, GateReason, GateRequest};
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
