//! Replays real `opencode acp` 1.19.6 and `kilo acp` 7.8.27 transcripts
//! (recorded with `wasit-smoke --record`, free models) through
//! `AcpProtocol` and checks the events and the exact lines written back.

use serde_json::Value;
use wasit_core::transcript::{input_id, parse, replay, Record, Replay};
use wasit_core::{AgentEvent, Command, EndReason, InputDropReason, InputId, ResumeMode};
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

// ── media (opencode 1.19.6, kilo 7.8.27) ───────────────────────────────────

const RED_PNG_B64: &str = "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAAF0lEQVR4nGP4z8BAEiJN9aiGUQ1DSgMAkPn/Afnh+ngAAAAASUVORK5CYII=";

fn reply(r: &Replay) -> String {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::AssistantDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn check_image_prompt(name: &str, flavor: Flavor) {
    let r = run(name, flavor);
    let prompt = r
        .written
        .iter()
        .map(|l| json(l))
        .find(|v| v["method"] == "session/prompt")
        .expect("prompt written");
    let blocks = &prompt["params"]["prompt"];
    assert_eq!(blocks[0]["type"], "text");
    assert_eq!(blocks[1]["type"], "image");
    assert_eq!(blocks[1]["mimeType"], "image/png");
    assert_eq!(blocks[1]["data"], RED_PNG_B64);
    assert!(
        reply(&r).to_lowercase().contains("red"),
        "{name}: {}",
        reply(&r)
    );
}

#[test]
fn opencode_image_prompt_is_an_image_block_and_a_vision_model_sees_it() {
    // opencode/deepseek-v4.1-flash-free
    check_image_prompt("opencode-image-prompt", Flavor::OpenCode);
}

#[test]
fn kilo_image_prompt_is_an_image_block_and_a_vision_model_sees_it() {
    // kilo/nvidia/nemotron-3.5-super-vl:free (kilo-auto/free answers that it cannot see images)
    check_image_prompt("kilo-image-prompt", Flavor::Kilo);
}

#[test]
fn opencode_read_of_a_png_reports_the_image_as_a_tool_part() {
    let r = run("opencode-read-image", Flavor::OpenCode);
    let parts = r
        .events
        .iter()
        .find_map(|e| match e {
            AgentEvent::ToolCallFinished { parts, .. } if !parts.is_empty() => Some(parts.clone()),
            _ => None,
        })
        .expect("a tool result with an image");
    assert_eq!(
        parts,
        vec![wasit_core::ContentPart::inline_image(
            "image/png",
            RED_PNG_B64
        )]
    );
}

// ── acknowledgement and refusal of prompts ─────────────────────────────────

/// The inputs a transcript sends the CLI: the start record's first prompt, then each prompt command.
fn prompt_inputs(records: &[Record]) -> Vec<InputId> {
    records
        .iter()
        .enumerate()
        .filter(|(_, r)| match r {
            Record::Start(spec) => spec.first_prompt.is_some(),
            Record::Cmd(Command::Prompt { .. } | Command::RunSlash { .. }) => true,
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

/// New with the acknowledgements: in every recording (opencode 1.19.6, kilo 7.8.27) each prompt
/// the CLI took is acknowledged exactly once, no prompt is dropped, and what acknowledges it is
/// the agent working (or the prompt's result), never `session/load` history, `session/new`
/// follow-ups (`available_commands_update`, `config_option_update`), or a usage update.
#[test]
fn every_prompt_is_acknowledged_once_by_the_agent_working() {
    let all = [
        ("basic", Flavor::OpenCode),
        ("kilo-basic", Flavor::Kilo),
        ("gates", Flavor::OpenCode),
        ("kilo-gates", Flavor::Kilo),
        ("interrupt", Flavor::OpenCode),
        ("mode", Flavor::OpenCode),
        ("kilo-mode", Flavor::Kilo),
        ("resume", Flavor::OpenCode),
        ("continue", Flavor::OpenCode),
        ("opencode-image-prompt", Flavor::OpenCode),
        ("kilo-image-prompt", Flavor::Kilo),
        ("opencode-read-image", Flavor::OpenCode),
    ];
    for (name, flavor) in all {
        let r = run(name, flavor);
        let expected: Vec<(InputId, bool)> = prompt_inputs(&load(name))
            .into_iter()
            .map(|i| (i, false))
            .collect();
        assert_eq!(acks(&r), expected, "{name}");
        assert!(
            !r.events
                .iter()
                .any(|e| matches!(e, AgentEvent::InputDropped { .. })),
            "{name}"
        );
        // Whatever acknowledges is followed by the agent's own output or the end of the turn:
        // never by the bookkeeping updates that merely arrive after the prompt was written.
        for (i, e) in r.events.iter().enumerate() {
            if matches!(e, AgentEvent::InputAcknowledged { .. }) {
                assert!(
                    !matches!(
                        r.events.get(i + 1),
                        Some(
                            AgentEvent::SlashCommandsUpdated { .. }
                                | AgentEvent::ModelChanged { .. }
                                | AgentEvent::ModesUpdated { .. }
                                | AgentEvent::UsageUpdate { .. }
                        )
                    ),
                    "{name}: acknowledged on a bookkeeping update: {:?}",
                    &r.events[i..i + 2]
                );
            }
        }
    }
}

/// In `basic` (opencode) `available_commands_update` and `config_option_update` arrive between
/// `session/prompt` and the first text chunk: the acknowledgement waits for the chunk.
#[test]
fn bookkeeping_updates_after_the_prompt_do_not_acknowledge() {
    let r = run("basic", Flavor::OpenCode);
    let pos = |f: &dyn Fn(&AgentEvent) -> bool| r.events.iter().position(f).unwrap();
    let commands = pos(&|e| matches!(e, AgentEvent::SlashCommandsUpdated { .. }));
    let ack = pos(&|e| matches!(e, AgentEvent::InputAcknowledged { .. }));
    let text = pos(&|e| matches!(e, AgentEvent::AssistantDelta { .. }));
    assert!(commands < ack && ack + 1 == text, "{:?}", r.events);
    // kilo: the first activity is a thought chunk.
    let r = run("kilo-basic", Flavor::Kilo);
    let ack = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::InputAcknowledged { .. }))
        .unwrap();
    assert!(matches!(
        r.events[ack + 1],
        AgentEvent::ThinkingDelta { .. }
    ));
}

/// History replayed by `session/load` precedes the load result in `resume` / `continue`; the
/// prompt goes out after it, and only the agent's answer to it acknowledges.
#[test]
fn session_load_history_does_not_acknowledge() {
    for name in ["resume", "continue"] {
        let r = run(name, Flavor::OpenCode);
        let ack = r
            .events
            .iter()
            .position(|e| matches!(e, AgentEvent::InputAcknowledged { .. }))
            .unwrap();
        let init = r
            .events
            .iter()
            .position(|e| matches!(e, AgentEvent::SessionInit { .. }))
            .unwrap();
        assert!(init < ack, "{name}");
        assert_eq!(acks(&r).len(), 1, "{name}");
    }
}

/// Recorded with kilo 7.8.8 (no credentials, scratch HOME: the CLI answers without calling a
/// model, so it costs nothing): `available_commands_update` and `usage_update` follow the prompt,
/// then the prompt is answered with an error. Not acknowledged by either update, so it is
/// reported refused, and the turn ends as an execution error.
#[test]
fn kilo_refused_prompt_is_reported_dropped() {
    let r = run("kilo-refused", Flavor::Kilo);
    let input = input_id(0);
    assert!(acks(&r).is_empty(), "{:?}", r.events);
    let dropped: Vec<_> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputDropped {
                input,
                reason,
                error,
            } => Some((*input, *reason, error.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(dropped.len(), 1, "{:?}", r.events);
    assert_eq!(dropped[0].0, input);
    assert_eq!(dropped[0].1, InputDropReason::Refused);
    assert!(
        dropped[0].2.as_deref().unwrap_or("").contains("sign in"),
        "{dropped:?}"
    );
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::UsageUpdate { .. })));
    assert_eq!(turns(&r), vec![(Some(EndReason::ExecutionError), false)]);
    // Order: the usage update, the refusal, the end of the turn.
    let at = |f: &dyn Fn(&AgentEvent) -> bool| r.events.iter().position(f).unwrap();
    let usage = at(&|e| matches!(e, AgentEvent::UsageUpdate { .. }));
    let drop = at(&|e| matches!(e, AgentEvent::InputDropped { .. }));
    let end = at(&|e| matches!(e, AgentEvent::TurnCompleted { .. }));
    assert!(usage < drop && drop < end);
}
