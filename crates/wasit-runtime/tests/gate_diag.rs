//! The gate diagnostics (`gate.raised`, `gate.answered`, `gate.withdrawn`, `gate.answer_rejected`)
//! written by a real session (real shim, fake CLI) to a diag sink of this process: each record
//! has the fields its event declares, the facts are the ones the gates went through, and no
//! record holds a gate's text or a withdrawal's detail. One test function: the diag sink is
//! process-wide.

mod common;

use std::time::{Duration, Instant};

use common::*;
use serde_json::Value;
use wasit_core::{
    Command, GateChoice, GateChoiceKind, GateDecision, GateReason, GateRequest, TurnBinding,
};
use wasit_diag::{Code, Ident, SinkConfig};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::CommandError;

const INSTRUCTION: &str = "what is 2+2?";
const OWNER_DETAIL: &str = "SECRET-OWNER-DETAIL-TEXT";

fn handoff(rid: &str) -> GateRequest {
    GateRequest::Handoff {
        request_id: rid.into(),
        task_id: "task-1".into(),
        from: "h1/orchestrator".into(),
        to: "h2/worker".into(),
        mode: "consult".into(),
        instruction: INSTRUCTION.into(),
        details: serde_json::Value::Null,
        choices: vec![GateChoice::agent(
            "allow_once",
            "Allow once",
            GateChoiceKind::AllowOnce,
        )],
    }
}

fn answer(rid: &str, choice: &str) -> Command {
    Command::AnswerGate {
        request_id: rid.into(),
        decision: GateDecision::choose(choice),
    }
}

/// Waits until every record emitted so far is written (or dropped) by the sink.
async fn written() {
    let until = Instant::now() + T;
    loop {
        let s = wasit_diag::stats().expect("the sink is installed");
        if s.written + s.dropped >= s.emitted {
            return;
        }
        assert!(Instant::now() < until, "the sink did not write its records");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The non-header fields of a record, by name.
fn fields(v: &Value) -> Vec<String> {
    let mut keys: Vec<String> = v
        .as_object()
        .unwrap()
        .keys()
        .filter(|k| !matches!(k.as_str(), "b" | "seq" | "t" | "m" | "src" | "lvl" | "ev"))
        .cloned()
        .collect();
    keys.sort();
    keys
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_changes_are_written_as_content_free_records() {
    let d = TempDir::new();
    let diag = d.path().join("diag");
    wasit_diag::install(SinkConfig::new(
        Code::st("daemon"),
        Ident::st("test"),
        &diag,
        "gates.ndjson",
    ))
    .unwrap();
    let req = create(d.path(), "claude", &Scenario::builder().build());
    let mgr = manager(d.path()).await;
    let id = mgr.create(req).await.unwrap().id;
    let sid = id.as_str().to_string();
    wait_phase(&mgr, &id, "idle").await;

    // Raised, then answered.
    mgr.raise_gate(&id, handoff("d-1")).await.unwrap();
    wait_phase(&mgr, &id, "needs_you").await;
    mgr.command(&id, answer("d-1", "allow_once")).await.unwrap();
    wait_phase(&mgr, &id, "idle").await;

    // Raised bound to a turn, then taken down by its owner with a detail.
    mgr.raise_gate_for_turn(&id, handoff("d-2"), TurnBinding::Interrupt { detail: None })
        .await
        .unwrap();
    wait_phase(&mgr, &id, "needs_you").await;
    mgr.resolve_gate(
        &id,
        "d-2",
        GateReason::new("owner_closed").with_detail(OWNER_DETAIL),
    )
    .await
    .unwrap();
    wait_phase(&mgr, &id, "idle").await;
    // A late answer to it: refused, as withdrawn.
    assert!(matches!(
        mgr.command(&id, answer("d-2", "allow_once")).await,
        Err(CommandError::GateWithdrawn { .. })
    ));

    // Raised, then answered with a choice the gate does not offer.
    mgr.raise_gate(&id, handoff("d-3")).await.unwrap();
    wait_phase(&mgr, &id, "needs_you").await;
    assert!(mgr.command(&id, answer("d-3", "nope")).await.is_err());
    written().await;

    let text = std::fs::read_to_string(diag.join("gates.ndjson")).unwrap();
    for secret in [INSTRUCTION, OWNER_DETAIL] {
        assert!(
            !text.contains(secret),
            "{secret:?} leaked into the diag records"
        );
    }
    let records: Vec<Value> = text
        .lines()
        .skip(1)
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .filter(|v| v["ev"].as_str().is_some_and(|e| e.starts_with("gate.")))
        .collect();
    let got: Vec<(&str, &str)> = records
        .iter()
        .map(|v| (v["ev"].as_str().unwrap(), v["request_id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        got,
        [
            ("gate.raised", "d-1"),
            ("gate.answered", "d-1"),
            ("gate.raised", "d-2"),
            ("gate.withdrawn", "d-2"),
            ("gate.answer_rejected", "d-2"),
            ("gate.raised", "d-3"),
            ("gate.answer_rejected", "d-3"),
        ],
        "{records:#?}"
    );
    for v in &records {
        assert_eq!(v["session"], sid.as_str(), "{v}");
    }
    let by_ev = |i: usize| (records[i]["ev"].as_str().unwrap(), fields(&records[i]));
    let f = |names: &[&str]| names.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    assert_eq!(
        by_ev(0),
        (
            "gate.raised",
            f(&["bound_to", "kind", "request_id", "session"])
        )
    );
    assert_eq!(records[0]["kind"], "handoff");
    assert_eq!(records[0]["bound_to"], "none");
    assert_eq!(
        by_ev(1),
        ("gate.answered", f(&["ms", "request_id", "session"]))
    );
    assert!(records[1]["ms"].as_u64().is_some(), "{}", records[1]);
    assert_eq!(records[2]["bound_to"], "interrupt");
    assert_eq!(
        by_ev(3),
        ("gate.withdrawn", f(&["reason", "request_id", "session"]))
    );
    assert_eq!(records[3]["reason"], "owner_closed");
    assert_eq!(
        by_ev(4),
        (
            "gate.answer_rejected",
            f(&["reason", "request_id", "session"])
        )
    );
    assert_eq!(records[4]["reason"], "withdrawn");
    assert_eq!(records[5]["bound_to"], "none");
    assert_eq!(records[6]["reason"], "invalid_choice");
}
