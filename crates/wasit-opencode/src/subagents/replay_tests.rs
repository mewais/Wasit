//! The recorded Kilo subagent scenarios (`wasit-fake-cli/reference/kilo/subagent_*`) replayed
//! through the Kilo protocol the way the bridge delivers them: the CLI's ACP lines, and the
//! bridge's notes. The notes are the recorded SSE payloads run through the same [`Tracker`] the
//! bridge runs, and the recorded background-job answers. Everything is merged in the order the
//! recording observed it.
//!
//! The protocol's own requests are answered from the recording by method (the n-th request of a
//! method gets the n-th recorded answer), so no id is assumed. Commands are injected at the time
//! the recording made them, and a permission asked of the owner is answered as the test says.

use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};
use wasit_core::wire::rpc_notification;
use wasit_core::{
    AgentEvent, Command, Encoded, GateChoiceKind, GateDecision, GateRequest, InputId, Protocol,
    SpawnSpec, Step, TurnTrigger,
};

use super::{jobs_note, Tracker, METHOD};
use crate::AcpProtocol;

/// The model the recordings ran with: keeps the protocol's own `set_config_option` request, which
/// the recording answers too.
const MODEL: &str = "fake/fake-model";

fn fixture(name: &str) -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../wasit-fake-cli/reference/kilo/"
    );
    std::fs::read_to_string(format!("{path}{name}"))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

fn lines(scenario: &str, ext: &str) -> Vec<Value> {
    fixture(&format!("subagent_{scenario}.{ext}"))
        .lines()
        .map(|l| serde_json::from_str(l).expect("fixture line is JSON"))
        .collect()
}

fn http(scenario: &str) -> Value {
    serde_json::from_str(&fixture(&format!("subagent_{scenario}.http.json")))
        .expect("http fixture is JSON")
}

/// The child session ids of a recording, in the order they were created.
pub(super) fn child_ids(scenario: &str) -> Vec<String> {
    http(scenario)["child_ids"]
        .as_array()
        .expect("child_ids")
        .iter()
        .map(|v| v.as_str().expect("id").to_string())
        .collect()
}

/// One thing the recording delivers, at its time.
enum Item {
    /// A bridge note (the params of a `_wasit/kilo` notification).
    Note(Value),
    /// A line the CLI wrote to stdout.
    Acp(Value),
    /// A command the test makes at this point.
    Command(Command),
}

/// How a replay differs from the recording.
#[derive(Default)]
pub(super) struct Opts {
    /// Commands made at a time of the recording (in time order); the first is the prompt.
    pub commands: Vec<(u64, Command)>,
    /// Answer every gate with its first allow-once choice.
    pub answer_gates: bool,
    /// Refuse every `session/load` the recording does not answer with an error (a child that
    /// cannot be loaded) instead of accepting it.
    pub load_error: bool,
}

/// What a replay produced.
pub(super) struct Run {
    pub events: Vec<AgentEvent>,
    /// Every line the protocol wrote to the CLI, parsed.
    pub writes: Vec<Value>,
    /// The params of every control line the protocol wrote (the bridge's commands).
    pub controls: Vec<Value>,
}

/// The first process of a recording: its label, and when the next one starts (if it does).
fn first_process(records: &[Value]) -> (Option<String>, u64) {
    let first = records
        .iter()
        .find_map(|r| r.get("proc").and_then(Value::as_str))
        .map(str::to_string);
    let cut = records
        .iter()
        .filter(|r| {
            r.get("proc")
                .and_then(Value::as_str)
                .is_some_and(|p| Some(p) != first.as_deref())
        })
        .filter_map(|r| r.get("t").and_then(Value::as_u64))
        .min()
        .unwrap_or(u64::MAX);
    (first, cut)
}

/// The records of a recording's first CLI process (a recording may hold a restart after it).
struct Recording {
    acp: Vec<Value>,
    sse: Vec<Value>,
    cut: u64,
}

impl Recording {
    fn of(scenario: &str) -> Self {
        let acp = lines(scenario, "acp.jsonl");
        let sse = lines(scenario, "sse.jsonl");
        let (proc_label, cut) = first_process(&[acp.as_slice(), sse.as_slice()].concat());
        let inside = |r: &Value| {
            r.get("t").and_then(Value::as_u64).unwrap_or(0) < cut
                && (r.get("proc").and_then(Value::as_str).is_none()
                    || r.get("proc").and_then(Value::as_str) == proc_label.as_deref())
        };
        Recording {
            acp: acp.iter().filter(|r| inside(r)).cloned().collect(),
            sse: sse.iter().filter(|r| inside(r)).cloned().collect(),
            cut,
        }
    }
}

fn timeline(scenario: &str, commands: &[(u64, Command)]) -> Vec<(u64, Item)> {
    let mut items: Vec<(u64, Item)> = Vec::new();
    let rec = Recording::of(scenario);
    let (acp, sse, cut) = (&rec.acp, &rec.sse, rec.cut);

    // The bridge's own view: the event stream through the tracker, and the job answers.
    let mut tracker = Tracker::default();
    for r in sse {
        let t = r["t"].as_u64().unwrap_or(0);
        match r["dir"].as_str() {
            Some("sse") => {
                let mapped = tracker.on_event(&r["msg"]["payload"]);
                items.extend(mapped.notes.into_iter().map(|n| (t, Item::Note(n))));
            }
            Some("sse_error") => items.push((
                t,
                Item::Note(json!({"kind": "state_lost", "reason": "stream_error"})),
            )),
            _ => {}
        }
    }
    let h = http(scenario);
    for call in h["http"].as_array().into_iter().flatten() {
        let t = call["t"].as_u64().unwrap_or(0);
        let Some(path) = call["path"].as_str() else {
            continue;
        };
        if call["method"] != "GET" || t >= cut {
            continue;
        }
        if let Some(parent) = path
            .strip_prefix("/kilocode/background-jobs?sessionID=")
            .filter(|p| !p.is_empty())
        {
            if let Some(note) = jobs_note(parent, &call["body"]) {
                items.push((t, Item::Note(note)));
            }
        }
    }

    // The CLI's stdout (the first process only).
    for r in acp {
        if r["dir"] == "out" {
            items.push((r["t"].as_u64().unwrap_or(0), Item::Acp(r["msg"].clone())));
        }
    }
    for (t, cmd) in commands {
        items.push((*t, Item::Command(cmd.clone())));
    }
    // Stable: at one time, the order above (notes, then the CLI's lines, then commands).
    items.sort_by_key(|(t, _)| *t);
    items
}

/// The method each request the recording's client sent had, by its id.
fn client_methods(rec: &Recording) -> HashMap<u64, String> {
    rec.acp
        .iter()
        .filter(|r| r["dir"] == "in")
        .filter_map(|r| {
            let m = &r["msg"];
            Some((m["id"].as_u64()?, m["method"].as_str()?.to_string()))
        })
        .collect()
}

struct Feeder {
    p: AcpProtocol,
    opts: Opts,
    /// Requests the protocol wrote and that the recording has not answered yet: (our id, method).
    pending: VecDeque<(u64, String)>,
    /// The recording's answers to `session/load`, if it has any.
    recorded_loads: bool,
    client: HashMap<u64, String>,
    run: Run,
    input: u64,
}

impl Feeder {
    fn apply(&mut self, step: Step) {
        let mut answers = Vec::new();
        for event in &step.events {
            if let AgentEvent::GateRequested { gate } = event {
                if self.opts.answer_gates {
                    if let Some(choice) = allow_once(gate) {
                        answers.push((gate.request_id().to_string(), choice));
                    }
                }
            }
        }
        self.run.events.extend(step.events);
        for line in step.write {
            self.wrote(&line.0);
        }
        for (request_id, choice) in answers {
            let decision = GateDecision::choose(choice);
            let step = self
                .p
                .encode(
                    InputId(0),
                    Command::AnswerGate {
                        request_id,
                        decision,
                    },
                )
                .expect("the gate is pending");
            if let Encoded::Sent(step) = step {
                self.apply(step);
            }
        }
    }

    /// One line the protocol wrote. A `session/load` the recording never answers is answered
    /// here: with an error when the test says the child cannot be loaded, else with success.
    fn wrote(&mut self, line: &str) {
        let v: Value = serde_json::from_str(line).expect("protocol writes JSON");
        self.run.writes.push(v.clone());
        let method = v.get("method").and_then(Value::as_str).map(str::to_string);
        match method.as_deref() {
            Some(m) if m == METHOD => self.run.controls.push(v["params"].clone()),
            Some("session/load") if !self.recorded_loads => {
                let id = v["id"].clone();
                let reply = if self.opts.load_error {
                    json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": "not loadable"}})
                } else {
                    json!({"jsonrpc": "2.0", "id": id, "result": {}})
                };
                let step = self.p.on_line(&reply.to_string());
                self.apply(step);
            }
            Some(m) => {
                if let Some(id) = v.get("id").and_then(Value::as_u64) {
                    self.pending.push_back((id, m.to_string()));
                }
            }
            None => {}
        }
    }

    fn item(&mut self, item: Item) {
        match item {
            Item::Note(params) => {
                let line = rpc_notification(METHOD, Some(params)).0;
                let step = self.p.on_line(&line);
                self.apply(step);
            }
            Item::Acp(msg) => {
                let is_response = msg.get("method").is_none() && msg.get("id").is_some();
                if !is_response {
                    let step = self.p.on_line(&msg.to_string());
                    return self.apply(step);
                }
                // A recorded answer to one of our requests: it takes our id, by method.
                let Some(method) = msg["id"]
                    .as_u64()
                    .and_then(|id| self.client.get(&id))
                    .cloned()
                else {
                    return;
                };
                let Some(pos) = self.pending.iter().position(|(_, m)| *m == method) else {
                    return;
                };
                let (ours, _) = self.pending.remove(pos).expect("position exists");
                let mut msg = msg;
                msg["id"] = json!(ours);
                let step = self.p.on_line(&msg.to_string());
                self.apply(step);
            }
            Item::Command(cmd) => {
                self.input += 1;
                match self.p.encode(InputId(self.input), cmd) {
                    Ok(Encoded::Sent(step)) => self.apply(step),
                    Ok(Encoded::Queued) => {}
                    Err(e) => panic!("command refused: {e:?}"),
                }
            }
        }
    }
}

/// The first allow-once choice of a permission request.
fn allow_once(gate: &GateRequest) -> Option<String> {
    match gate {
        GateRequest::Permission { choices, .. } => choices
            .iter()
            .find(|c| c.kind == GateChoiceKind::AllowOnce)
            .map(|c| c.id.clone()),
        _ => None,
    }
}

/// Replay a recorded scenario through the Kilo protocol.
pub(super) fn run(scenario: &str, opts: Opts) -> Run {
    let rec = Recording::of(scenario);
    let client = client_methods(&rec);
    let recorded_loads = rec.acp.iter().any(|r| {
        r["dir"] == "out"
            && r["msg"]["id"]
                .as_u64()
                .and_then(|id| client.get(&id))
                .is_some_and(|m| m == "session/load")
    });
    let mut feeder = Feeder {
        p: AcpProtocol::with_bridge("wasit-kilo"),
        recorded_loads,
        client,
        pending: VecDeque::new(),
        run: Run {
            events: Vec::new(),
            writes: Vec::new(),
            controls: Vec::new(),
        },
        input: 0,
        opts,
    };
    let spec = SpawnSpec {
        model: Some(MODEL.into()),
        ..SpawnSpec::default()
    };
    let start = feeder.p.start(&spec);
    feeder.apply(start);
    let commands = std::mem::take(&mut feeder.opts.commands);
    for (_, item) in timeline(scenario, &commands) {
        feeder.item(item);
    }
    feeder.run
}

// ── checks ────────────────────────────────────────────────────────────────

/// One line per event that matters to the subagents: a short name, and its ids.
pub(super) fn story(events: &[AgentEvent]) -> Vec<String> {
    events.iter().filter_map(story_line).collect()
}

fn story_line(e: &AgentEvent) -> Option<String> {
    Some(match e {
        AgentEvent::TurnStarted { trigger, .. } => format!("turn_started {trigger:?}"),
        AgentEvent::TurnCompleted {
            was_interrupted, ..
        } => format!("turn_completed interrupted={was_interrupted}"),
        AgentEvent::TaskStarted {
            task_id,
            call_id,
            background,
            ..
        } => format!(
            "task_started {task_id} call={} bg={background}",
            call_id.as_deref().unwrap_or("-")
        ),
        AgentEvent::TaskProgress { task_id, .. } => format!("task_progress {task_id}"),
        AgentEvent::TaskEnded {
            task_id, status, ..
        } => {
            format!("task_ended {task_id} {status}")
        }
        AgentEvent::ToolCallStarted {
            is_subagent: true,
            call_id,
            ..
        } => format!("subagent_call {call_id}"),
        AgentEvent::GateRequested { gate } => format!("gate {}", gate.request_id()),
        AgentEvent::GateResolved { request_id, .. } => format!("gate_resolved {request_id}"),
        AgentEvent::CommandRejected { command, .. } => format!("rejected {command}"),
        AgentEvent::Scoped { scope, event } => format!(
            "scoped {} {}",
            scope.agent_id.as_deref().unwrap_or("-"),
            variant(event)
        ),
        _ => return None,
    })
}

/// The name of an event's variant (its Debug form up to the first field).
fn variant(e: &AgentEvent) -> String {
    let debug = format!("{e:?}");
    debug
        .split([' ', '{', '('])
        .next()
        .unwrap_or_default()
        .to_string()
}

/// The `TaskEnded` events, as (task, status, ok).
pub(super) fn task_ends(events: &[AgentEvent]) -> Vec<(String, String, bool)> {
    events
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
        .collect()
}

/// The `session/load` requests written, as the session each names.
pub(super) fn loads_written(run: &Run) -> Vec<String> {
    run.writes
        .iter()
        .filter(|w| w["method"] == "session/load")
        .filter_map(|w| w["params"]["sessionId"].as_str().map(str::to_string))
        .collect()
}

/// The `TurnStarted` events' triggers, in order.
pub(super) fn turn_triggers(events: &[AgentEvent]) -> Vec<TurnTrigger> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnStarted { trigger, .. } => Some(*trigger),
            _ => None,
        })
        .collect()
}

/// The prompt the tests send at the start of a recording.
pub(super) fn first_prompt() -> (u64, Command) {
    (0, Command::prompt("probe"))
}

// ── the recordings, as tests ──────────────────────────────────────────────

fn prompt_only() -> Opts {
    Opts {
        commands: vec![first_prompt()],
        ..Opts::default()
    }
}

/// The story without the progress lines (they come at each of the child's model responses).
fn story_without_progress(events: &[AgentEvent]) -> Vec<String> {
    story(events)
        .into_iter()
        .filter(|l| !l.starts_with("task_progress"))
        .collect()
}

/// The tokens a child's assistant messages report at the end of the recording: each message
/// counted once, at its last report (the sum the owner's usage should show).
fn recorded_spend(scenario: &str, child: &str) -> (u64, u64) {
    let mut last: HashMap<String, (u64, u64)> = HashMap::new();
    for r in lines(scenario, "sse.jsonl") {
        let p = &r["msg"]["payload"];
        if p["type"] != "message.updated" || p["properties"]["info"]["sessionID"] != child {
            continue;
        }
        let info = &p["properties"]["info"];
        if info["role"] != "assistant" {
            continue;
        }
        let id = info["id"].as_str().unwrap_or_default().to_string();
        let t = &info["tokens"];
        last.insert(
            id,
            (
                t["input"].as_u64().unwrap_or(0),
                t["output"].as_u64().unwrap_or(0),
            ),
        );
    }
    last.values().fold((0, 0), |(i, o), (a, b)| (i + a, o + b))
}

#[test]
fn a_foreground_task_call_is_a_task_until_its_result() {
    let child = child_ids("fg")[0].clone();
    let r = run("fg", prompt_only());
    assert_eq!(
        loads_written(&r),
        std::slice::from_ref(&child),
        "the child is loaded on ACP"
    );
    assert_eq!(
        story_without_progress(&r.events),
        [
            "turn_started Input".to_string(),
            // The call is announced as a subagent before the task that it is.
            "subagent_call call_1263e3b5aea6442d".to_string(),
            format!("task_started {child} call=call_1263e3b5aea6442d bg=false"),
            // Ends at the call's own result, not at the child's idle (which came first).
            format!("task_ended {child} completed"),
            "turn_completed interrupted=false".to_string(),
        ]
    );
    let (input, output) = recorded_spend("fg", &child);
    let progress = r.events.iter().rev().find_map(|e| match e {
        AgentEvent::TaskProgress { usage: Some(u), .. } => Some(*u),
        _ => None,
    });
    let end = r.events.iter().find_map(|e| match e {
        AgentEvent::TaskEnded { usage: Some(u), .. } => Some(*u),
        _ => None,
    });
    assert_eq!(end.and_then(|u| u.input_tokens), Some(input));
    assert_eq!(end.and_then(|u| u.output_tokens), Some(output));
    assert_eq!(progress.and_then(|u| u.input_tokens), Some(input));
    // Three model responses (the glob, the read, the final text), 100 input tokens each.
    assert_eq!(input, 300);
}

#[test]
fn a_backgrounded_task_call_starts_a_background_task() {
    let child = child_ids("bg")[0].clone();
    let r = run("bg", prompt_only());
    let started = r.events.iter().find_map(|e| match e {
        AgentEvent::TaskStarted {
            task_id,
            call_id,
            background,
            run_id,
            ..
        } => Some((
            task_id.clone(),
            call_id.clone(),
            *background,
            run_id.clone(),
        )),
        _ => None,
    });
    assert_eq!(
        started,
        Some((
            child.clone(),
            Some("call_2e157d60205a4cf6".into()),
            true,
            Some(child.clone())
        )),
        "a background task's run is its child's job"
    );
    // The child's own tool calls and text are the child's: scoped to its call.
    let scoped: Vec<&AgentEvent> = r
        .events
        .iter()
        .filter(|e| matches!(e, AgentEvent::Scoped { scope, .. } if scope.agent_id.as_deref() == Some(child.as_str())))
        .collect();
    assert!(
        scoped.len() >= 10,
        "the child's conversation: {}",
        scoped.len()
    );
    assert!(r.events.iter().all(|e| match e {
        AgentEvent::Scoped { scope, .. } => scope.call_id == "call_2e157d60205a4cf6",
        _ => true,
    }));
    // The owner's own turn ends before the job does, and Kilo's delivery turn starts after the
    // child went busy: the owner is working while the delivery runs.
    assert_eq!(
        story(&r.events)
            .iter()
            .filter(|l| l.starts_with("turn_started")
                || l.starts_with("turn_completed")
                || l.starts_with("task_ended"))
            .cloned()
            .collect::<Vec<_>>(),
        [
            "turn_started Input",
            "turn_completed interrupted=false",
            "turn_started Agent",
            format!("task_ended {child} completed").as_str(),
            "turn_completed interrupted=false",
        ]
        .map(String::from)
    );
    assert_eq!(
        turn_triggers(&r.events),
        [TurnTrigger::Input, TurnTrigger::Agent],
        "the owner's prompt, then the turn Kilo runs to deliver the background result"
    );
    assert_eq!(task_ends(&r.events), [(child, "completed".into(), true)]);
}

#[test]
fn background_children_are_paired_with_the_calls_that_completed_them() {
    let r = run("bg_multi", prompt_only());
    let h = http("bg_multi");
    // The completed task calls report the child they ran: the pairing must agree with them.
    let mut reported: HashMap<String, String> = HashMap::new();
    for line in lines("bg_multi", "acp.jsonl") {
        let u = &line["msg"]["params"]["update"];
        if u["sessionUpdate"] == "tool_call_update" && u["status"] == "completed" {
            if let (Some(call), Some(sid)) = (
                u["toolCallId"].as_str(),
                u["rawOutput"]["metadata"]["sessionId"].as_str(),
            ) {
                reported.insert(call.to_string(), sid.to_string());
            }
        }
    }
    assert_eq!(reported.len(), 3, "three completed task calls");
    let starts: Vec<(String, String)> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TaskStarted {
                task_id, call_id, ..
            } => Some((call_id.clone()?, task_id.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 3);
    for (call, task) in &starts {
        assert_eq!(reported.get(call), Some(task), "call {call} ran {task}");
    }
    assert_eq!(h["child_ids"].as_array().map(Vec::len), Some(3));
    assert_eq!(task_ends(&r.events).len(), 3);
    assert!(task_ends(&r.events)
        .iter()
        .all(|(_, s, ok)| s == "completed" && *ok));
}

#[test]
fn steering_a_running_subagent_sends_one_control_line_for_it() {
    let child = child_ids("steer")[0].clone();
    let mut opts = prompt_only();
    opts.commands.push((
        4700,
        Command::SteerSubagent {
            task_id: child.clone(),
            text: "use the other file".into(),
        },
    ));
    let r = run("steer", opts);
    assert_eq!(
        r.controls,
        [json!({"command": "steer", "session": child, "text": "use the other file"})]
    );
    assert!(
        !r.events
            .iter()
            .any(|e| matches!(e, AgentEvent::CommandRejected { .. })),
        "the steer is accepted"
    );
    // The child keeps running: its events still arrive after the steer, on its own scope.
    assert!(r
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::Scoped { .. })));
    assert_eq!(task_ends(&r.events), [(child, "completed".into(), true)]);
}

#[test]
fn a_loaded_subagents_permission_is_raised_once_from_acp() {
    let mut opts = prompt_only();
    opts.answer_gates = true;
    let r = run("permission_loaded", opts);
    let gates: Vec<&GateRequest> = r
        .events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::GateRequested { gate } => Some(gate),
            _ => None,
        })
        .collect();
    assert_eq!(
        gates.len(),
        1,
        "the bridge's copy of the request is not raised again"
    );
    assert_eq!(
        gates[0].request_id(),
        "call_b515d9e094c74f03",
        "the ACP request"
    );
    assert!(
        matches!(gates[0], GateRequest::Permission { explanation, .. } if explanation.starts_with("subagent \"probe subagent\": "))
    );
    // Answered on ACP, with the option the user chose.
    assert!(r.writes.iter().any(|w| w["id"] == 0
        && w["result"]["outcome"] == json!({"outcome": "selected", "optionId": "once"})));
    assert!(r.controls.is_empty(), "nothing goes over the control line");
}

#[test]
fn an_unloaded_subagents_permission_is_raised_from_the_bridge_and_answered_on_the_control_line() {
    let mut opts = prompt_only();
    opts.answer_gates = true;
    opts.load_error = true;
    let r = run("permission_unloaded", opts);
    let child = child_ids("permission_unloaded")[0].clone();
    assert!(
        r.events.iter().any(|e| matches!(e, AgentEvent::CommandRejected { command, .. } if command == "session/load")),
        "the child could not be loaded, and the owner is told"
    );
    assert_eq!(
        r.controls,
        [
            json!({"command": "reply_permission", "id": "per_211105187001vqfRUkWefI6Gtl", "reply": "once"})
        ]
    );
    assert_eq!(
        story_without_progress(&r.events)
            .iter()
            .filter(|l| l.starts_with("gate"))
            .count(),
        1
    );
    assert_eq!(task_ends(&r.events), [(child, "completed".into(), true)]);
}

#[test]
fn interrupt_aborts_the_owner_turn_for_its_session_and_keeps_the_background_subagent() {
    let child = child_ids("interrupt_abort")[0].clone();
    let mut opts = prompt_only();
    // The recording's second prompt, then its interrupt (`abort?scope=session`).
    opts.commands.push((4460, Command::prompt("hold B")));
    opts.commands.push((6501, Command::Interrupt));
    let r = run("interrupt_abort", opts);
    assert_eq!(
        r.controls,
        [
            json!({"command": "abort", "session": child_owner("interrupt_abort"), "scope": "session"})
        ]
    );
    assert!(
        !r.writes.iter().any(|w| w["method"] == "session/cancel"),
        "ACP cancel would abort the subagents too"
    );
    assert!(
        r.events.iter().any(|e| matches!(
            e,
            AgentEvent::TurnCompleted {
                was_interrupted: true,
                ..
            }
        )),
        "the owner's turn was interrupted"
    );
    assert_eq!(
        task_ends(&r.events),
        [(child, "completed".into(), true)],
        "the subagent ran on"
    );
}

/// The owner session of a recording (its parent of the first child).
fn child_owner(scenario: &str) -> String {
    lines(scenario, "sse.jsonl")
        .iter()
        .find_map(|r| {
            let info = &r["msg"]["payload"]["properties"]["info"];
            (r["msg"]["payload"]["type"] == "session.created" && info.get("parentID").is_some())
                .then(|| info["parentID"].as_str().unwrap_or_default().to_string())
        })
        .expect("a child in the recording")
}

#[test]
fn a_lost_stream_ends_the_running_subagent_as_unknown() {
    let child = child_ids("owner_exit")[0].clone();
    let r = run("owner_exit", prompt_only());
    assert_eq!(task_ends(&r.events), [(child, "unknown".into(), false)]);
}

#[test]
fn a_background_subagent_the_cli_cancels_ends_stopped() {
    let child = child_ids("interrupt")[0].clone();
    let mut opts = prompt_only();
    opts.commands.push((6539, Command::Interrupt));
    let r = run("interrupt", opts);
    assert_eq!(
        r.controls,
        [json!({"command": "abort", "session": child_owner("interrupt"), "scope": "session"})]
    );
    assert_eq!(task_ends(&r.events), [(child, "stopped".into(), false)]);
}

#[test]
fn an_interrupt_with_no_turn_sends_nothing_and_the_background_child_runs_on() {
    // In the recording the owner's turn is over and its background child is running. With no
    // turn there is nothing to interrupt: no abort goes out (neither the parent's nor the tree's),
    // and the child is not stopped; it completes on its own, as it does without the interrupt.
    let child = child_ids("bg")[0].clone();
    let mut opts = prompt_only();
    opts.commands.push((5500, Command::Interrupt));
    let r = run("bg", opts);
    assert!(
        r.controls.is_empty(),
        "no turn, no control line: {:?}",
        r.controls
    );
    assert!(
        !r.writes.iter().any(|w| w["method"] == "session/cancel"),
        "no ACP cancel either"
    );
    assert!(
        !r.events.iter().any(|e| matches!(
            e,
            AgentEvent::TurnCompleted {
                was_interrupted: true,
                ..
            }
        )),
        "no turn was interrupted"
    );
    assert_eq!(
        task_ends(&r.events),
        [(child, "completed".into(), true)],
        "the child runs on to its own completion"
    );
}

#[test]
fn without_the_bridge_steer_and_stop_are_unsupported_and_interrupt_is_session_cancel() {
    use crate::AcpProtocol as Acp;
    let mut p = Acp::new("wasit-opencode");
    p.start(&SpawnSpec::default());
    p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}"#);
    p.on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_1"}}"#);
    let steer = Command::SteerSubagent {
        task_id: "ses_c".into(),
        text: "x".into(),
    };
    assert!(matches!(
        p.encode(InputId(1), steer),
        Err(wasit_core::DriverError::Unsupported { .. })
    ));
    assert!(matches!(
        p.encode(
            InputId(2),
            Command::StopTask {
                task_id: "ses_c".into()
            }
        ),
        Err(wasit_core::DriverError::Unsupported { .. })
    ));
}

#[test]
fn the_kilo_driver_claims_subagents_and_opencode_claims_none() {
    let kilo = crate::kilo().capabilities();
    assert!(kilo.stop_task);
    assert!(kilo.subagents.transcript && kilo.subagents.live_state && kilo.subagents.usage);
    assert!(kilo.subagents.steer && kilo.subagents.background);
    let opencode = crate::opencode().capabilities();
    assert!(!opencode.stop_task);
    assert_eq!(opencode.subagents, wasit_core::SubagentSupport::default());
}

#[test]
fn stopping_a_background_subagent_sends_its_tree_abort_on_the_control_line() {
    // The recording's job is still running when the test stops it: the stop is the abort of the
    // child's tree, and the job's own cancellation is what ends the task.
    let child = child_ids("bg")[0].clone();
    let mut opts = prompt_only();
    opts.commands.push((
        5500,
        Command::StopTask {
            task_id: child.clone(),
        },
    ));
    let r = run("bg", opts);
    assert_eq!(
        r.controls,
        [json!({"command": "abort", "session": child, "scope": "tree"})]
    );
}
