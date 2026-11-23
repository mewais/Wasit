//! Replays real `codex app-server` 0.169.1 transcripts (recorded with
//! `wasit-smoke --record` against a scripted Responses-API fake) through
//! `CodexProtocol` and checks the events and the exact lines written back.

use serde_json::Value;
use wasit_codex::CodexProtocol;
use wasit_core::transcript::{parse, replay, Record, Replay};
use wasit_core::{AgentEvent, EndReason, ResumeMode};

fn load(name: &str) -> Vec<Record> {
    let path = format!(
        "{}/tests/fixtures/{name}.ndjson",
        env!("CARGO_MANIFEST_DIR")
    );
    parse(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}")))
        .unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn run(name: &str) -> Replay {
    let records = load(name);
    let r = replay(&mut CodexProtocol::new(), &records);
    assert!(r.encode_errors.is_empty(), "{name}: {:?}", r.encode_errors);
    assert_eq!(
        r.written, r.expected_written,
        "{name}: written lines differ"
    );
    let again = replay(&mut CodexProtocol::new(), &records);
    assert_eq!(
        format!("{r:?}"),
        format!("{again:?}"),
        "{name}: not deterministic"
    );
    r
}

fn json(line: &str) -> Value {
    serde_json::from_str(line).unwrap()
}

fn methods(r: &Replay) -> Vec<String> {
    r.written
        .iter()
        .map(|l| {
            json(l)["method"]
                .as_str()
                .unwrap_or("<response>")
                .to_string()
        })
        .collect()
}

fn turns(r: &Replay) -> Vec<(Option<EndReason>, bool, u64)> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                usage,
                ..
            } => Some((*end_reason, *was_interrupted, usage.input_tokens)),
            _ => None,
        })
        .collect()
}

#[test]
fn handshake_and_turn() {
    let r = run("basic");
    assert_eq!(
        methods(&r)[..6],
        [
            "initialize",
            "initialized",
            "model/list",
            "thread/start",
            "collaborationMode/list",
            "turn/start"
        ]
    );
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::ModelsUpdated { models, .. } if !models.is_empty())));
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::SessionInit { model: Some(m), .. } if m == "fake")));
    assert!(r.events.iter().any(|e| matches!(e, AgentEvent::ModesUpdated { available_modes, .. } if available_modes.len() == 2)));
    assert!(r.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnStarted {
            native_turn_id: Some(_),
            ..
        }
    )));
    assert!(r.events.iter().any(
        |e| matches!(e, AgentEvent::AssistantDelta { text, .. } if text == "Hello from fake: hello")
    ));
    // Usage from thread/tokenUsage/updated lands in TurnCompleted.
    let t = turns(&r);
    assert_eq!(t.len(), 1);
    assert!(t[0].0.is_none() && t[0].2 > 0);
}

#[test]
fn gate_approve_and_deny() {
    let r = run("gates");
    let answers: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v.get("result").is_some())
        .collect();
    assert_eq!(answers.len(), 2);
    assert_eq!(
        answers[0]["result"],
        serde_json::json!({"decision": "accept"})
    );
    // codex 0.169.1 offers `availableDecisions` accept / acceptWithExecpolicyAmendment /
    // cancel for these commands, so the refusal is `cancel` (the recording answered
    // `decline`, which codex also accepts; only that answer line was adjusted).
    assert_eq!(
        answers[1]["result"],
        serde_json::json!({"decision": "cancel"})
    );
    let finished: Vec<bool> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::ToolCallFinished { ok, .. } => Some(*ok),
            _ => None,
        })
        .collect();
    assert_eq!(finished, vec![true, false]);
    assert_eq!(turns(&r).len(), 2);
}

#[test]
fn interrupt_uses_turn_id_and_declines_gate() {
    let r = run("interrupt");
    let started: Vec<String> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnStarted { native_turn_id, .. } => native_turn_id.clone(),
            _ => None,
        })
        .collect();
    let interrupts: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v["method"] == "turn/interrupt")
        .collect();
    assert_eq!(interrupts.len(), 2);
    for (i, turn) in interrupts.iter().zip(&started) {
        assert_eq!(&i["params"]["turnId"], turn.as_str());
    }
    let t = turns(&r);
    assert!(t
        .iter()
        .all(|(reason, interrupted, _)| *reason == Some(EndReason::Cancelled) && *interrupted));
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::GateResolved { .. })));
    let decline = r
        .written
        .iter()
        .position(|l| json(l)["result"]["decision"] == "decline")
        .unwrap();
    let last_int = r
        .written
        .iter()
        .rposition(|l| json(l)["method"] == "turn/interrupt")
        .unwrap();
    assert!(decline < last_int);
}

#[test]
fn mode_and_model_changes() {
    let r = run("mode");
    let updates: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v["method"] == "thread/settings/update")
        .collect();
    assert_eq!(updates.len(), 3);
    assert_eq!(updates[0]["params"]["collaborationMode"]["mode"], "plan");
    assert_eq!(updates[1]["params"]["model"], "fake-2");
    // The mode switch after the model switch carries the new model.
    assert_eq!(
        updates[2]["params"]["collaborationMode"]["settings"]["model"],
        "fake-2"
    );
    assert!(r.events.contains(&AgentEvent::ModesUpdated {
        available_modes: vec![],
        current_mode: Some("plan".into())
    }));
    assert!(r.events.contains(&AgentEvent::ModelChanged {
        model: "fake-2".into()
    }));
    assert_eq!(turns(&r).len(), 1);
}

#[test]
fn resume_and_continue() {
    let records = load("resume");
    let Record::Start(spec) = &records[0] else {
        panic!()
    };
    let ResumeMode::Resume(id) = &spec.resume else {
        panic!()
    };
    let r = run("resume");
    let open = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["method"] == "thread/resume")
        .expect("thread/resume");
    assert_eq!(&open["params"]["threadId"], id.as_str());
    assert!(r.events.iter().any(
        |e| matches!(e, AgentEvent::SessionInit { native_session_id: Some(s), .. } if s == id)
    ));
    assert_eq!(turns(&r).len(), 1);

    let r = run("continue");
    let m = methods(&r);
    let list = m
        .iter()
        .position(|x| x == "thread/list")
        .expect("thread/list");
    assert_eq!(m[list + 1], "thread/resume");
    assert_eq!(turns(&r).len(), 1);
}
