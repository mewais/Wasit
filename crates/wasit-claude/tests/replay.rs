//! Replays real claude transcripts (recorded with `wasit-smoke --record`)
//! through `ClaudeProtocol` and checks the events and the exact lines written
//! back. CLI versions: 2.1.326 (basic, gates, interrupt, mode, resume,
//! continue), 2.1.327 (the image fixtures), 2.1.293 (lifecycle, background-bash,
//! background-agent, stop-task, agent-turn-interrupt, agent-turn-prompt: user messages carry a
//! uuid; names and the home path in the CLI's own output sanitized, long lists trimmed; the
//! background ones recorded with `wasit-smoke` against the real CLI, scripts in
//! `wasit-fake-cli/reference/record/background`).

use serde_json::Value;
use wasit_claude::{descriptor, ClaudeProtocol};
use wasit_core::tasks::{self, TaskInfo};
use wasit_core::transcript::{parse, replay, Record, Replay};
use wasit_core::{
    AgentEvent, EndReason, GateRequest, InputDropReason, InputId, ResumeMode, TurnTrigger,
};

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
    assert_turns_alternate(name, &r);
    r
}

/// The turn edges are the CLI's: every turn that ends started, and none starts twice.
fn assert_turns_alternate(name: &str, r: &Replay) {
    let mut open = false;
    for e in &r.events {
        match e {
            AgentEvent::TurnStarted { .. } => {
                assert!(!open, "{name}: a turn started inside a turn");
                open = true;
            }
            AgentEvent::TurnCompleted { .. } => {
                assert!(open, "{name}: a turn ended without a start");
                open = false;
            }
            _ => {}
        }
    }
    assert!(!open, "{name}: a turn never ended");
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
    // The turn starts at the CLI's `system/init`, before any output.
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

// ── message lifecycle (claude 2.1.293) ──────────────────────────────────────

/// The first prompt (input 1, journaled with the start) runs a long tool; two prompts sent
/// meanwhile wait in the protocol (inputs 9, 10); an interrupt drops both and ends the turn;
/// the next prompt (input 17) gets its own turn. Each prompt is acknowledged on the CLI's
/// `command_lifecycle started` for its uuid — nothing else acknowledges.
#[test]
fn lifecycle_acknowledges_and_an_interrupt_drops_held_prompts() {
    let r = run("lifecycle");
    let users: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v["type"] == "user")
        .collect();
    assert_eq!(users.len(), 2, "the dropped prompts were never written");
    assert!(users.iter().all(|u| u["uuid"].is_string()));
    assert_ne!(users[0]["uuid"], users[1]["uuid"]);
    let ids = |f: &dyn Fn(&AgentEvent) -> Option<InputId>| -> Vec<InputId> {
        r.events.iter().filter_map(f).collect()
    };
    assert_eq!(
        ids(&|e| match e {
            AgentEvent::InputAcknowledged { input, .. } => Some(*input),
            _ => None,
        }),
        [InputId(1), InputId(17)]
    );
    assert_eq!(
        ids(&|e| match e {
            AgentEvent::TurnStarted { input, .. } => *input,
            _ => None,
        }),
        [InputId(1), InputId(17)]
    );
    let drops: Vec<(InputId, InputDropReason)> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputDropped { input, reason, .. } => Some((*input, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        drops,
        [
            (InputId(9), InputDropReason::Interrupted),
            (InputId(10), InputDropReason::Interrupted)
        ]
    );
    let t = turns(&r);
    assert_eq!(t.len(), 2);
    assert_eq!((t[0].0, t[0].1), (Some(EndReason::Cancelled), true));
    assert_eq!(t[1], (None, false, Some("after".into())));
    // The running message had started: the interrupt asked to cancel nothing.
    assert!(!r
        .written
        .iter()
        .any(|l| json(l)["request"]["subtype"] == "cancel_async_message"));
}

// ── background work and turns the CLI starts itself (claude 2.1.293, real recordings) ──────

/// What each turn of a replay was started by, in order, and how it ended.
fn turn_starts(r: &Replay) -> Vec<(Option<InputId>, TurnTrigger)> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnStarted { input, trigger, .. } => Some((*input, *trigger)),
            _ => None,
        })
        .collect()
}

/// The tasks after each event, folded the way the runtime folds them.
fn task_states(r: &Replay) -> Vec<(usize, Vec<TaskInfo>)> {
    let mut tasks = Vec::new();
    let mut out = Vec::new();
    for (i, e) in r.events.iter().enumerate() {
        tasks::apply(&mut tasks, e, i as u64);
        if matches!(e, AgentEvent::TurnCompleted { .. }) {
            tasks::end_foreground(&mut tasks, i as u64);
        }
        out.push((i, tasks.clone()));
    }
    out
}

fn origin_kinds(r: &Replay) -> Vec<Option<String>> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnCompleted { origin, .. } => {
                Some(origin.as_ref().map(|o| o.kind.clone()))
            }
            _ => None,
        })
        .collect()
}

/// A background command: the turn that started it ends at once; the command runs on; when it ends
/// the CLI starts a turn by itself (no input of ours, an `origin` at its `result`). The
/// background task is running at the first turn's end, so a session that is `working` while any
/// background task runs never reads `idle` in between.
#[test]
fn a_background_command_outlives_its_turn_and_the_cli_starts_the_next_turn_itself() {
    let r = run("background-bash");
    assert_eq!(
        turn_starts(&r),
        [
            (Some(InputId(1)), TurnTrigger::Input),
            (None, TurnTrigger::Agent)
        ]
    );
    assert_eq!(
        origin_kinds(&r),
        [None, Some("task-notification".to_string())]
    );
    let states = task_states(&r);
    let first_end = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::TurnCompleted { .. }))
        .unwrap();
    let tasks = &states[first_end].1;
    assert_eq!(tasks::background_running(tasks), 1, "{tasks:?}");
    assert_eq!(tasks[0].kind, "local_bash");
    assert!(tasks[0].background && tasks[0].call_id.is_some());
    // It ends with the agent's report, and then the agent's turn starts.
    let ended = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::TaskEnded { .. }))
        .unwrap();
    assert!(matches!(
        &r.events[ended],
        AgentEvent::TaskEnded { status, ok: true, .. } if status == "completed"
    ));
    assert_eq!(tasks::background_running(&states[ended].1), 0);
    let second_start = r
        .events
        .iter()
        .rposition(|e| matches!(e, AgentEvent::TurnStarted { .. }))
        .unwrap();
    assert!(first_end < ended && ended < second_start);
    // The only input is acknowledged once; the agent's turn is nobody's.
    assert_eq!(
        count(&r, |e| matches!(e, AgentEvent::InputAcknowledged { .. })),
        1
    );
}

fn scoped(r: &Replay) -> Vec<(&wasit_core::Scope, &AgentEvent)> {
    r.events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::Scoped { scope, event } => Some((scope, &**event)),
            _ => None,
        })
        .collect()
}

/// A backgrounded subagent: its tool calls stream after the parent's turn ended, tagged with the
/// call that spawned it and its own id; none of them is the session's own. The subagent runs
/// until the agent says its task ended, not until the `Agent` tool result came back.
#[test]
fn a_background_subagent_is_attributed_and_runs_until_its_task_ends() {
    let r = run("background-agent");
    let spawn = r
        .events
        .iter()
        .find_map(|e| match e {
            AgentEvent::ToolCallStarted {
                name,
                call_id,
                is_subagent: true,
                ..
            } if name == "Agent" => Some(call_id.clone()),
            _ => None,
        })
        .expect("the Agent call");
    let scoped = scoped(&r);
    assert!(scoped.len() >= 4, "{} scoped events", scoped.len());
    assert!(scoped.iter().all(|(s, _)| s.call_id == spawn));
    assert!(scoped.iter().all(|(s, _)| s.agent_id.is_some()));
    let tools: Vec<&str> = scoped
        .iter()
        .filter_map(|(_, e)| match e {
            AgentEvent::ToolCallStarted { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert!(tools.contains(&"Bash"), "{tools:?}");
    // The parent's own tool calls are not scoped: the Agent call and (none else) are top level.
    let own_tools = count(&r, |e| matches!(e, AgentEvent::ToolCallStarted { .. }));
    assert_eq!(own_tools, 1);
    // The subagent's lines come after the parent's first turn ended.
    let first_end = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::TurnCompleted { .. }))
        .unwrap();
    let first_scoped = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::Scoped { .. }))
        .unwrap();
    assert!(first_end < first_scoped);
    // The Agent tool call returned long before the task ended; the task is what is running.
    let returned = r
        .events
        .iter()
        .position(
            |e| matches!(e, AgentEvent::ToolCallFinished { call_id, .. } if *call_id == spawn),
        )
        .unwrap();
    let ended = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::TaskEnded { .. }))
        .unwrap();
    let states = task_states(&r);
    assert!(returned < first_end && first_end < first_scoped && first_scoped < ended);
    assert_eq!(tasks::background_running(&states[returned].1), 1);
    assert_eq!(tasks::background_running(&states[first_end].1), 1);
    assert_eq!(tasks::background_running(&states[ended].1), 0);
    let task = &states[ended].1[0];
    assert_eq!(task.kind, "local_agent");
    assert_eq!(task.call_id.as_deref(), Some(spawn.as_str()));
    assert_eq!(task.subagent_type.as_deref(), Some("general-purpose"));
    assert!(task.usage.is_some_and(|u| u.tool_uses >= 1));
    // The CLI then started turns of its own (the recording has two: the answer, and a second
    // one that did nothing but report).
    let starts = turn_starts(&r);
    assert_eq!(starts[0], (Some(InputId(1)), TurnTrigger::Input));
    assert!(starts.len() >= 2 && starts[1..].iter().all(|s| *s == (None, TurnTrigger::Agent)));
}

/// `stop_task` stops a background task (the agent's `task_notification` says `stopped`); stopping
/// a background command starts no turn, stopping a subagent makes the agent report it with a turn
/// of its own. Both recorded: the driver wrote `stop_task` for the id the agent had reported.
#[test]
fn stop_task_ends_a_background_task_and_the_agent_may_answer_with_a_turn() {
    let r = run("stop-task");
    let stops: Vec<Value> = r
        .written
        .iter()
        .map(|l| json(l))
        .filter(|v| v["request"]["subtype"] == "stop_task")
        .collect();
    assert_eq!(stops.len(), 2, "{:?}", r.written);
    let started: Vec<String> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TaskStarted { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(started.len(), 2);
    for (stop, id) in stops.iter().zip(&started) {
        assert_eq!(stop["request"]["task_id"], id.as_str());
    }
    let ends: Vec<(String, String, bool)> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TaskEnded {
                task_id,
                status,
                ok,
                ..
            } => Some((task_id.clone(), status.clone(), *ok)),
            _ => None,
        })
        .collect();
    assert_eq!(
        ends,
        [
            (started[0].clone(), "stopped".to_string(), false),
            (started[1].clone(), "stopped".to_string(), false)
        ]
    );
    let states = task_states(&r);
    let last = states.last().unwrap();
    assert_eq!(tasks::background_running(&last.1), 0);
    // Turns: our first prompt, then (bash stopped: nothing) our second prompt, then the agent's
    // own turn that reported the stopped subagent.
    assert_eq!(
        turn_starts(&r),
        [
            (Some(InputId(1)), TurnTrigger::Input),
            (Some(InputId(20)), TurnTrigger::Input),
            (None, TurnTrigger::Agent)
        ]
    );
    // Stopping the command ended it, and no turn followed before the next prompt's.
    let bash_end = r
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::TaskEnded { task_id, .. } if *task_id == started[0]))
        .unwrap();
    let second_turn = r
        .events
        .iter()
        .position(|e| {
            matches!(
                e,
                AgentEvent::TurnStarted {
                    input: Some(InputId(20)),
                    ..
                }
            )
        })
        .unwrap();
    assert!(!r.events[bash_end..second_turn]
        .iter()
        .any(|e| matches!(e, AgentEvent::TurnStarted { .. })));
    // Stopping a task is not an interrupt.
    assert!(r.events.iter().all(|e| !matches!(
        e,
        AgentEvent::TurnCompleted {
            was_interrupted: true,
            ..
        }
    )));
}

/// An interrupt stops a turn the CLI started by itself: `result` says it was cancelled (with the
/// agent's own origin), and the next prompt gets a turn of its own.
#[test]
fn an_interrupt_stops_a_turn_the_cli_started_by_itself() {
    let r = run("agent-turn-interrupt");
    assert_eq!(
        turn_starts(&r),
        [
            (Some(InputId(1)), TurnTrigger::Input),
            (None, TurnTrigger::Agent),
            (Some(InputId(26)), TurnTrigger::Input)
        ]
    );
    let ends: Vec<(Option<EndReason>, bool)> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                ..
            } => Some((*end_reason, *was_interrupted)),
            _ => None,
        })
        .collect();
    assert_eq!(
        ends,
        [
            (None, false),
            (Some(EndReason::Cancelled), true),
            (None, false)
        ]
    );
    assert_eq!(
        origin_kinds(&r),
        [None, Some("task-notification".to_string()), None]
    );
    let interrupts = r
        .written
        .iter()
        .filter(|l| json(l)["request"]["subtype"] == "interrupt")
        .count();
    assert_eq!(interrupts, 1);
}

/// A prompt sent while the CLI runs a turn of its own waits for that turn to end (one prompt, one
/// turn) and then runs as its own turn, attributed to it.
#[test]
fn a_prompt_sent_during_a_turn_the_cli_started_waits_for_it() {
    let r = run("agent-turn-prompt");
    assert_eq!(
        turn_starts(&r),
        [
            (Some(InputId(1)), TurnTrigger::Input),
            (None, TurnTrigger::Agent),
            (Some(InputId(20)), TurnTrigger::Input)
        ]
    );
    let pos = |f: &dyn Fn(&AgentEvent) -> bool| r.events.iter().position(f).unwrap();
    let agent_end = r
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, AgentEvent::TurnCompleted { .. }))
        .map(|(i, _)| i)
        .nth(1)
        .unwrap();
    let sent = pos(&|e| matches!(e, AgentEvent::InputSent { input: InputId(20) }));
    assert!(
        agent_end < sent,
        "the prompt was held until the CLI's turn ended"
    );
    assert!(
        sent < pos(&|e| matches!(
            e,
            AgentEvent::InputAcknowledged {
                input: InputId(20),
                steered: false
            }
        ))
    );
    assert_eq!(
        users_written(&r),
        2,
        "the prompt was written once, after the turn"
    );
}

fn users_written(r: &Replay) -> usize {
    r.written
        .iter()
        .filter(|l| json(l)["type"] == "user")
        .count()
}
