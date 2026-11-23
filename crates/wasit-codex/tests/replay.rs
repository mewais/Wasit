//! Replays real `codex app-server` 0.169.1 transcripts (recorded with
//! `wasit-smoke --record` against a scripted Responses-API fake) through
//! `CodexProtocol` and checks the events and the exact lines written back.

use serde_json::Value;
use wasit_codex::CodexProtocol;
use wasit_core::transcript::{input_id, parse, replay, Record, Replay};
use wasit_core::{AgentEvent, Command, EndReason, InputId, ResumeMode};

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

/// The inputs a transcript sends the CLI: the start record's first prompt, then each prompt command.
fn prompt_inputs(records: &[Record]) -> Vec<InputId> {
    records
        .iter()
        .enumerate()
        .filter(|(_, r)| match r {
            Record::Start(spec) => spec.first_prompt.is_some(),
            Record::Cmd(Command::Prompt { .. }) => true,
            _ => false,
        })
        .map(|(i, _)| input_id(i))
        .collect()
}

fn acks(r: &Replay) -> Vec<(InputId, bool)> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputAcknowledged { input, steered } => Some((*input, *steered)),
            _ => None,
        })
        .collect()
}

/// New with the acknowledgements: in every recording (codex-cli 0.169.1) each prompt written as
/// a `turn/start` is acknowledged exactly once, on its result (which precedes `turn/started`
/// in all of them), as a turn of its own, and no prompt is dropped.
#[test]
fn every_prompt_is_acknowledged_once() {
    for name in ["basic", "gates", "interrupt", "mode", "resume", "continue"] {
        let r = run(name);
        let expected: Vec<(InputId, bool)> = prompt_inputs(&load(name))
            .into_iter()
            .map(|i| (i, false))
            .collect();
        assert!(!expected.is_empty(), "{name}");
        assert_eq!(acks(&r), expected, "{name}");
        assert!(
            !r.events
                .iter()
                .any(|e| matches!(e, AgentEvent::InputDropped { .. })),
            "{name}"
        );
        // The acknowledgement names the input the turn it opens names.
        let started: Vec<InputId> = r
            .events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::TurnStarted { input, .. } => *input,
                _ => None,
            })
            .collect();
        assert_eq!(
            started,
            expected.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            "{name}"
        );
        // Result first, then the notification, so the acknowledgement comes before `TurnStarted`.
        for (input, _) in &expected {
            let ack = r
                .events
                .iter()
                .position(
                    |e| matches!(e, AgentEvent::InputAcknowledged { input: i, .. } if i == input),
                )
                .unwrap();
            let turn = r
                .events
                .iter()
                .position(
                    |e| matches!(e, AgentEvent::TurnStarted { input: Some(i), .. } if i == input),
                )
                .unwrap();
            assert!(ack < turn, "{name}");
        }
    }
}

/// Recorded with codex-cli 0.161.0 against the scripted fake Responses server
/// (`SLOW please`, then a prompt while the turn streams): `turn/steer` is answered
/// `{"turnId": <the running turn>}`, codex folds the text into the same turn (a `userMessage`
/// item, a second model call) and the one turn ends. Only the host name and installation id in
/// the `remoteControl/status/changed` line were replaced.
#[test]
fn steered_prompt_is_acknowledged_as_steered() {
    let records = load("steer");
    let first = input_id(0);
    let steered = records
        .iter()
        .position(|r| matches!(r, Record::Cmd(Command::Prompt { .. })))
        .map(input_id)
        .unwrap();
    let r = run("steer");
    // The first prompt opened the turn; the second joined it.
    assert_eq!(acks(&r), vec![(first, false), (steered, true)]);
    let started: Vec<_> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnStarted {
                native_turn_id,
                input,
            } => Some((native_turn_id.clone().unwrap(), *input)),
            _ => None,
        })
        .collect();
    assert_eq!(
        started.len(),
        1,
        "a steered prompt starts no turn: {started:?}"
    );
    assert_eq!(started[0].1, Some(first));
    let steer = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["method"] == "turn/steer")
        .expect("turn/steer written");
    assert_eq!(steer["params"]["expectedTurnId"], started[0].0.as_str());
    assert_eq!(steer["params"]["input"][0]["text"], "steer me please");
    assert!(!r.events.iter().any(|e| matches!(
        e,
        AgentEvent::InputDropped { .. } | AgentEvent::CommandRejected { .. }
    )));
    // One turn ends, with the reply to the steered text.
    assert_eq!(turns(&r).len(), 1);
    assert!(r.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted { reply: Some(t), .. } if t == "Hello from fake: steer me please"
    )));
}
