//! Replays real `opencode acp` 1.19.6 and `kilo acp` 7.8.27 transcripts
//! (recorded with `wasit-smoke --record`, free models) through
//! `AcpProtocol` and checks the events and the exact lines written back.

use serde_json::Value;
use wasit_core::transcript::{parse, replay, Record, Replay};
use wasit_core::{AgentEvent, EndReason, ResumeMode};
use wasit_opencode::{AcpProtocol, Flavor};

fn load(name: &str) -> Vec<Record> {
    let path = format!(
        "{}/tests/fixtures/{name}.ndjson",
        env!("CARGO_MANIFEST_DIR")
    );
    parse(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}")))
        .unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn run(name: &str, flavor: Flavor) -> Replay {
    let records = load(name);
    let r = replay(&mut AcpProtocol::new(flavor.client_name()), &records);
    assert!(r.encode_errors.is_empty(), "{name}: {:?}", r.encode_errors);
    assert_eq!(
        r.written, r.expected_written,
        "{name}: written lines differ"
    );
    let again = replay(&mut AcpProtocol::new(flavor.client_name()), &records);
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

fn turns(r: &Replay) -> Vec<(Option<EndReason>, bool)> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                ..
            } => Some((*end_reason, *was_interrupted)),
            _ => None,
        })
        .collect()
}

fn check_basic(name: &str, flavor: Flavor, model: &str) {
    let r = run(name, flavor);
    let m = methods(&r);
    assert_eq!(
        m[..4],
        [
            "initialize",
            "session/new",
            "session/set_config_option",
            "session/prompt"
        ],
        "{name}"
    );
    assert_eq!(
        json(&r.written[0])["params"]["clientInfo"]["name"],
        flavor.client_name()
    );
    assert_eq!(json(&r.written[2])["params"]["value"], model);
    assert!(r.events.iter().any(|e| matches!(e, AgentEvent::SessionInit { available_modes, .. } if !available_modes.is_empty())));
    assert!(r.events.contains(&AgentEvent::ModelChanged {
        model: model.into()
    }));
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::AssistantDelta { .. })));
    assert_eq!(turns(&r), vec![(None, false)]);
}

#[test]
fn opencode_basic() {
    check_basic(
        "basic",
        Flavor::OpenCode,
        "opencode/deepseek-v4.1-flash-free",
    );
}

#[test]
fn kilo_basic() {
    check_basic("kilo-basic", Flavor::Kilo, "kilo/kilo-auto/free");
}

fn check_gates(name: &str, flavor: Flavor) {
    let r = run(name, flavor);
    let answers: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v.get("result").is_some())
        .collect();
    assert_eq!(answers.len(), 2, "{name}");
    assert_eq!(
        answers[0]["result"]["outcome"],
        serde_json::json!({"outcome": "selected", "optionId": "once"})
    );
    assert_eq!(
        answers[1]["result"]["outcome"],
        serde_json::json!({"outcome": "selected", "optionId": "reject"})
    );
    let gates = r
        .events
        .iter()
        .filter(|e| matches!(e, AgentEvent::GateRequested { .. }))
        .count();
    assert_eq!(gates, 2);
    assert_eq!(turns(&r).len(), 2);
}

#[test]
fn opencode_gates() {
    check_gates("gates", Flavor::OpenCode);
}

#[test]
fn kilo_gates() {
    check_gates("kilo-gates", Flavor::Kilo);
}

#[test]
fn opencode_interrupt() {
    let r = run("interrupt", Flavor::OpenCode);
    let cancels = r
        .written
        .iter()
        .filter(|l| json(l)["method"] == "session/cancel")
        .count();
    assert_eq!(cancels, 2);
    assert!(turns(&r)
        .iter()
        .all(|t| *t == (Some(EndReason::Cancelled), true)));
    assert_eq!(turns(&r).len(), 2);
    // The pending permission was answered `cancelled` before session/cancel.
    let cancelled = r
        .written
        .iter()
        .position(|l| json(l)["result"]["outcome"]["outcome"] == "cancelled")
        .unwrap();
    let last_cancel = r
        .written
        .iter()
        .rposition(|l| json(l)["method"] == "session/cancel")
        .unwrap();
    assert!(cancelled < last_cancel);
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::GateResolved { .. })));
}

#[test]
fn opencode_mode_and_slash() {
    let r = run("mode", Flavor::OpenCode);
    assert!(r.events.contains(&AgentEvent::ModesUpdated {
        available_modes: vec![],
        current_mode: Some("plan".into())
    }));
    assert!(r.events.contains(&AgentEvent::ModesUpdated {
        available_modes: vec![],
        current_mode: Some("build".into())
    }));
    assert!(r.events.iter().any(
        |e| matches!(e, AgentEvent::CommandRejected { command, .. } if command == "set_mode")
    ));
    assert!(r
        .written
        .iter()
        .any(|l| json(l)["params"]["prompt"][0]["text"] == "/wasit-echo pong"));
    assert!(r.events.iter().any(|e| matches!(e, AgentEvent::AssistantDelta { text, .. } if text.to_lowercase().contains("pong"))));
    assert_eq!(turns(&r).len(), 1);
}

#[test]
fn kilo_mode() {
    let r = run("kilo-mode", Flavor::Kilo);
    assert!(r.events.contains(&AgentEvent::ModesUpdated {
        available_modes: vec![],
        current_mode: Some("ask".into())
    }));
}

#[test]
fn opencode_resume_and_continue() {
    let records = load("resume");
    let Record::Start(spec) = &records[0] else {
        panic!()
    };
    let ResumeMode::Resume(id) = &spec.resume else {
        panic!()
    };
    let r = run("resume", Flavor::OpenCode);
    let load_req = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["method"] == "session/load")
        .expect("session/load");
    assert_eq!(&load_req["params"]["sessionId"], id.as_str());
    assert!(r.events.iter().any(
        |e| matches!(e, AgentEvent::SessionInit { native_session_id: Some(s), .. } if s == id)
    ));
    assert_eq!(turns(&r), vec![(None, false)]);

    let r = run("continue", Flavor::OpenCode);
    let m = methods(&r);
    let list = m
        .iter()
        .position(|x| x == "session/list")
        .expect("session/list");
    assert_eq!(m[list + 1], "session/load");
    assert_eq!(turns(&r).len(), 1);
}
