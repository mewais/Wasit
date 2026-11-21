//! Replays real claude 2.1.326 transcripts (recorded with `wasit-smoke
//! --record`) through `ClaudeProtocol` and checks the events and the exact
//! lines written back.

use serde_json::Value;
use wasit_claude::{descriptor, ClaudeProtocol};
use wasit_core::transcript::{parse, replay, Record, Replay};
use wasit_core::{AgentEvent, EndReason, GateRequest, ResumeMode};

fn load(name: &str) -> Vec<Record> {
    let path = format!(
        "{}/tests/fixtures/{name}.ndjson",
        env!("CARGO_MANIFEST_DIR")
    );
    parse(&std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}")))
        .unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// Replay, check writes are byte-identical to the recording, and that a
/// second replay is identical (determinism).
fn run(name: &str) -> Replay {
    let records = load(name);
    let r = replay(&mut ClaudeProtocol::new(), &records);
    assert!(r.encode_errors.is_empty(), "{name}: {:?}", r.encode_errors);
    assert_eq!(
        r.written, r.expected_written,
        "{name}: written lines differ"
    );
    let again = replay(&mut ClaudeProtocol::new(), &records);
    assert_eq!(
        format!("{:?}", r),
        format!("{:?}", again),
        "{name}: not deterministic"
    );
    r
}

fn json(line: &str) -> Value {
    serde_json::from_str(line).unwrap()
}

fn count(r: &Replay, f: impl Fn(&AgentEvent) -> bool) -> usize {
    r.events.iter().filter(|e| f(e)).count()
}

fn turns(r: &Replay) -> Vec<(Option<EndReason>, bool, Option<String>)> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                reply,
                ..
            } => Some((*end_reason, *was_interrupted, reply.clone())),
            _ => None,
        })
        .collect()
}

fn start_spec(name: &str) -> wasit_core::SpawnSpec {
    match &load(name)[0] {
        Record::Start(spec) => (**spec).clone(),
        other => panic!("first record is {other:?}"),
    }
}

#[test]
fn basic_turn() {
    let r = run("basic");
    assert_eq!(json(&r.written[0])["request"]["subtype"], "initialize");
    assert_eq!(
        json(&r.written[1])["message"]["content"][0]["text"],
        "Reply with just the word hi"
    );
    assert_eq!(
        count(&r, |e| matches!(e, AgentEvent::SessionInit { .. })),
        1
    );
    assert_eq!(
        count(
            &r,
            |e| matches!(e, AgentEvent::ModelsUpdated { models, .. } if !models.is_empty())
        ),
        1
    );
    assert_eq!(
        count(&r, |e| matches!(e, AgentEvent::SlashCommandsUpdated { .. })),
        1
    );
    // TurnStarted is emitted when the prompt is written, before any output.
    let first_turn = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::TurnStarted { .. }));
    let first_delta = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::AssistantDelta { .. }));
    assert!(first_turn < first_delta);
    assert_eq!(turns(&r), vec![(None, false, Some("hi".into()))]);
    // `active_goal` / `autocompact_state` precede everything and are tolerated.
    assert!(matches!(&r.events[0], AgentEvent::Unknown { raw } if raw["type"] == "active_goal"));
    assert!(r.closed_stdin);
}

#[test]
fn gate_approve_and_deny() {
    let r = run("gates");
    let gates: Vec<&GateRequest> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::GateRequested { gate } => Some(gate),
            _ => None,
        })
        .collect();
    assert_eq!(gates.len(), 2);
    let GateRequest::Permission {
        request_id,
        tool_name,
        command,
        raw_input,
        ..
    } = gates[0]
    else {
        panic!("{:?}", gates[0]);
    };
    assert_eq!(tool_name, "Bash");
    assert_eq!(command, "touch wasit_gate_a.txt");
    let approve = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["response"]["request_id"] == request_id.as_str())
        .expect("approval written");
    assert_eq!(approve["response"]["response"]["behavior"], "allow");
    assert_eq!(&approve["response"]["response"]["updatedInput"], raw_input);
    let deny = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["response"]["request_id"] == gates[1].request_id())
        .expect("denial written");
    assert_eq!(deny["response"]["response"]["behavior"], "deny");
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
    assert!(turns(&r).iter().all(|t| t.0.is_none()));
}

#[test]
fn interrupt_mid_turn_and_with_pending_gate() {
    let r = run("interrupt");
    let interrupts: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v["request"]["subtype"] == "interrupt")
        .collect();
    assert_eq!(interrupts.len(), 2);
    let t = turns(&r);
    assert_eq!(t.len(), 2);
    assert!(t
        .iter()
        .all(|(reason, interrupted, _)| *reason == Some(EndReason::Cancelled) && *interrupted));
    // The pending gate was declined before the interrupt and reported resolved.
    let gate_id = r
        .events
        .iter()
        .find_map(|e| match e {
            AgentEvent::GateRequested { gate } => Some(gate.request_id().to_string()),
            _ => None,
        })
        .expect("a gate");
    assert!(r.events.contains(&AgentEvent::GateResolved {
        request_id: gate_id.clone(),
        reason: None,
    }));
    let deny_pos = r
        .written
        .iter()
        .position(|l| json(l)["response"]["request_id"] == gate_id.as_str())
        .unwrap();
    let int_pos = r
        .written
        .iter()
        .rposition(|l| json(l)["request"]["subtype"] == "interrupt")
        .unwrap();
    assert!(deny_pos < int_pos);
}

#[test]
fn mode_model_and_slash_command() {
    let r = run("mode");
    assert!(r.events.contains(&AgentEvent::ModesUpdated {
        available_modes: vec![],
        current_mode: Some("plan".into())
    }));
    assert_eq!(
        count(
            &r,
            |e| matches!(e, AgentEvent::CommandRejected { command, .. } if command == "set_mode")
        ),
        1
    );
    assert_eq!(
        count(&r, |e| matches!(e, AgentEvent::ModelChanged { .. })),
        1
    );
    assert!(r
        .written
        .iter()
        .any(|l| json(l)["message"]["content"][0]["text"] == "/context"));
    assert_eq!(turns(&r).len(), 1);
    assert!(matches!(&turns(&r)[0], (None, false, Some(reply)) if reply.contains("Context Usage")));
}

#[test]
fn resume_and_continue() {
    let spec = start_spec("resume");
    let ResumeMode::Resume(id) = &spec.resume else {
        panic!("{:?}", spec.resume)
    };
    let argv = descriptor().argv(&spec);
    assert!(argv.windows(2).any(|w| w[0] == "--resume" && &w[1] == id));
    let r = run("resume");
    assert!(r.events.iter().any(
        |e| matches!(e, AgentEvent::SessionInit { native_session_id: Some(s), .. } if s == id)
    ));
    assert_eq!(turns(&r), vec![(None, false, Some("hi".into()))]);

    let spec = start_spec("continue");
    assert_eq!(spec.resume, ResumeMode::Continue);
    assert!(descriptor().argv(&spec).contains(&"--continue".to_string()));
    let r = run("continue");
    assert!(r.events.iter().any(
        |e| matches!(e, AgentEvent::SessionInit { native_session_id: Some(s), .. } if s == id)
    ));
    assert_eq!(turns(&r).len(), 1);
}

// ── media (claude 2.1.327) ──────────────────────────────────────────────────

const RED_PNG_B64: &str = "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAAF0lEQVR4nGP4z8BAEiJN9aiGUQ1DSgMAkPn/Afnh+ngAAAAASUVORK5CYII=";

#[test]
fn image_attached_to_a_prompt_is_an_image_content_block() {
    let r = run("claude-image-prompt");
    let user = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["type"] == "user")
        .expect("user message written");
    let content = &user["message"]["content"];
    assert_eq!(content[0]["type"], "text");
    assert_eq!(content[1]["type"], "image");
    assert_eq!(content[1]["source"]["type"], "base64");
    assert_eq!(content[1]["source"]["media_type"], "image/png");
    assert_eq!(content[1]["source"]["data"], RED_PNG_B64);
    // The model saw it.
    let reply: String = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::AssistantDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert!(reply.to_lowercase().contains("red"), "{reply}");
}

#[test]
fn read_of_a_png_reports_the_image_as_a_tool_result_part() {
    let r = run("claude-read-image");
    let parts = r
        .events
        .iter()
        .find_map(|e| match e {
            AgentEvent::ToolCallFinished { name, parts, .. } if name == "Read" => {
                Some(parts.clone())
            }
            _ => None,
        })
        .expect("Read finished");
    assert_eq!(
        parts,
        vec![wasit_core::ContentPart::inline_image(
            "image/png",
            RED_PNG_B64
        )]
    );
}
