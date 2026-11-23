//! The claude persona's background tasks, grounded in the real recordings of claude 2.1.293
//! (`reference/claude/background_bash`, `background_agent`, `stop_task`, `agent_turn_interrupt`,
//! `agent_turn_prompt`): the tool call that starts a task returns at once, the turn ends, the task
//! runs on and its lines stream while the session is idle (a subagent's tagged with the spawning
//! call), and when it ends the agent starts a turn by itself — `system/init`, no lifecycle frame,
//! an `origin` on the `result`. `stop_task` stops a task; stopping a subagent (not a command)
//! makes the agent report it with a turn. A prompt read during the agent's own turn is taken into
//! it. Every message the fake prints has the shape of one the real CLI printed.

mod common;

use common::{shape_diff, Fake, CLAUDE_ARGV};
use serde_json::{json, Value};
use std::time::Duration;
use wasit_fake_cli::reference::{lines, Dir};
use wasit_fake_cli::scenario::{BackgroundStep, Scenario, Step, Turn};

const UUID_A: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const UUID_B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

fn start(sc: &Scenario) -> Fake {
    let mut fake = Fake::spawn(&CLAUDE_ARGV, Some("claude"), sc);
    fake.send(&json!({"type":"control_request","request_id":"init","request":{"subtype":"initialize","hooks":null}}));
    fake.recv_until(|m| m["type"] == "control_response");
    fake
}

fn user(fake: &mut Fake, uuid: &str, text: &str) {
    fake.send(&json!({"type":"user","uuid":uuid,"message":{"role":"user","content":[{"type":"text","text":text}]}}));
}

fn sys(m: &Value, sub: &str) -> bool {
    m["type"] == "system" && m["subtype"] == sub
}

fn is_result(m: &Value) -> bool {
    m["type"] == "result"
}

fn bash(after_ms: u64) -> BackgroundStep {
    BackgroundStep {
        kind: "local_bash".into(),
        description: "Run delayed echo".into(),
        command: Some("sleep 1; echo bgdone".into()),
        after_ms,
        then: vec![Step::Say {
            text: "FINISHED".into(),
            chunks: None,
        }],
        ..BackgroundStep::default()
    }
}

fn agent(after_ms: u64) -> BackgroundStep {
    BackgroundStep {
        kind: "local_agent".into(),
        description: "probe".into(),
        subagent_type: Some("general-purpose".into()),
        after_ms,
        steps: vec![
            Step::Tool(wasit_fake_cli::scenario::ToolStep {
                name: "Bash".into(),
                input: json!({"command": "echo hi", "description": "Say hi"}),
                result_if_allowed: Some("hi".into()),
                ..Default::default()
            }),
            Step::Say {
                text: "hi".into(),
                chunks: None,
            },
        ],
        then: vec![Step::Say {
            text: "OK".into(),
            chunks: None,
        }],
        on_stop: vec![Step::Say {
            text: "the agent was stopped".into(),
            chunks: None,
        }],
        ..BackgroundStep::default()
    }
}

fn says(task: BackgroundStep) -> Scenario {
    Scenario::builder()
        .turn(Turn::builder().background(task).say("STARTED"))
        .build()
}

fn discs(ms: &[Value]) -> Vec<String> {
    ms.iter().map(disc).collect()
}

/// The task and control frames among `ms`, in order (the noise around them left out).
fn task_frames(ms: &[Value]) -> Vec<String> {
    discs(ms)
        .into_iter()
        .filter(|d| {
            matches!(
                d.as_str(),
                "system:task_started"
                    | "system:task_progress"
                    | "system:task_updated"
                    | "system:task_notification"
                    | "system:background_tasks_changed"
                    | "system:init"
                    | "control_response"
            )
        })
        .collect()
}

fn disc(m: &Value) -> String {
    match m["type"].as_str().unwrap_or("?") {
        "system" => format!("system:{}", m["subtype"].as_str().unwrap_or("")),
        "result" => format!("result:{}", m["subtype"].as_str().unwrap_or("")),
        "command_lifecycle" => format!("command_lifecycle:{}", m["state"].as_str().unwrap_or("")),
        t @ ("assistant" | "user") => format!(
            "{t}:{}",
            m["message"]["content"][0]["type"].as_str().unwrap_or("")
        ),
        other => other.to_string(),
    }
}

/// A background command: the turn ends at once, the command ends later, and then the agent
/// starts a turn of its own.
#[test]
fn a_background_command_outlives_its_turn_and_the_agent_starts_a_turn_by_itself() {
    let mut fake = start(&says(bash(300)));
    user(&mut fake, UUID_A, "go");
    let first = fake.recv_until(is_result);
    // The call, the list, the start and the call's result come before the turn ends.
    let d = discs(&first);
    let at = |name: &str| {
        d.iter()
            .position(|x| x == name)
            .unwrap_or_else(|| panic!("{name} in {d:?}"))
    };
    assert!(at("command_lifecycle:queued") < at("command_lifecycle:started"));
    assert!(at("command_lifecycle:started") < at("system:init"));
    assert!(at("system:init") < at("assistant:tool_use"));
    assert!(at("system:background_tasks_changed") < at("system:task_started"));
    assert!(at("system:task_started") < at("user:tool_result"));
    assert!(at("user:tool_result") < at("result:success"));
    let started = first.iter().find(|m| sys(m, "task_started")).unwrap();
    assert_eq!(started["is_backgrounded"], true);
    assert_eq!(started["task_type"], "local_bash");
    let spawn = first
        .iter()
        .find(|m| m["message"]["content"][0]["type"] == "tool_use")
        .unwrap();
    assert_eq!(
        started["tool_use_id"], spawn["message"]["content"][0]["id"],
        "the task names the call that spawned it"
    );
    assert_eq!(first.last().unwrap()["user_message_uuids"], json!([UUID_A]));
    // Later: the end frames, then the agent's own turn.
    let second = fake.recv_until(is_result);
    let d = discs(&second);
    let at = |name: &str| {
        d.iter()
            .position(|x| x == name)
            .unwrap_or_else(|| panic!("{name} in {d:?}"))
    };
    assert!(at("system:task_updated") < at("system:task_notification"));
    assert!(at("system:task_notification") < at("system:background_tasks_changed"));
    assert!(at("system:background_tasks_changed") < at("system:init"));
    // (The first turn's `completed` arrives right after its `result`; nothing else is reported
    // about a message: no message of ours started this turn.)
    assert!(
        !d.iter()
            .any(|x| x == "command_lifecycle:started" || x == "command_lifecycle:queued"),
        "no message of ours: {d:?}"
    );
    let note = second.iter().find(|m| sys(m, "task_notification")).unwrap();
    assert_eq!(note["status"], "completed");
    assert_eq!(note["task_id"], started["task_id"]);
    let list = second
        .iter()
        .find(|m| sys(m, "background_tasks_changed"))
        .unwrap();
    assert_eq!(list["tasks"], json!([]));
    let result = second.last().unwrap();
    assert_eq!(
        result["origin"],
        json!({"kind":"task-notification","producer":"session-task","runId":started["run_id"]})
    );
    assert!(result.get("user_message_uuids").is_none());
    assert_eq!(result["result"], "FINISHED");
    // `system/init` starts every turn (the session's identification and then one per turn).
    let inits = fake.seen.iter().filter(|m| sys(m, "init")).count();
    assert_eq!(inits, 2);
}

/// A background subagent: while the session is idle its lines stream, tagged with the spawning
/// call and its own id, with progress frames, before the end frames.
#[test]
fn a_background_subagent_streams_tagged_lines_between_turns() {
    let mut fake = start(&says(agent(200)));
    user(&mut fake, UUID_A, "go");
    let first = fake.recv_until(is_result);
    let started = first
        .iter()
        .find(|m| sys(m, "task_started"))
        .unwrap()
        .clone();
    assert_eq!(started["task_type"], "local_agent");
    assert_eq!(started["subagent_type"], "general-purpose");
    let spawn = first
        .iter()
        .find(|m| m["message"]["content"][0]["name"] == "Agent")
        .expect("the Agent call");
    let call = spawn["message"]["content"][0]["id"].clone();
    // Nothing of the subagent's came before the turn ended.
    assert!(first.iter().all(|m| m["parent_tool_use_id"].is_null()));
    let rest = fake.recv_until(is_result);
    let tagged: Vec<&Value> = rest
        .iter()
        .filter(|m| m["parent_tool_use_id"].is_string())
        .collect();
    assert!(tagged.len() >= 3, "{}", discs(&rest).join(", "));
    for m in &tagged {
        assert_eq!(m["parent_tool_use_id"], call);
        assert_eq!(m["agent_id"], started["task_id"]);
        assert_eq!(m["subagent_type"], "general-purpose");
        assert_eq!(m["task_description"], "probe");
    }
    let d = discs(&rest);
    let at = |name: &str| {
        d.iter()
            .position(|x| x == name)
            .unwrap_or_else(|| panic!("{name} in {d:?}"))
    };
    assert!(at("assistant:tool_use") < at("system:task_progress"));
    assert!(at("system:task_progress") < at("system:task_notification"));
    // The session's own lines carry no tag.
    let own = rest.last().unwrap();
    assert!(own["parent_tool_use_id"].is_null());
    assert_eq!(own["result"], "OK");
}

/// `stop_task` on a background command: the end frames and the answer, and no turn follows;
/// on a subagent the agent starts a turn that reports it; an unknown id is answered with success
/// and does nothing; a missing id is refused.
#[test]
fn stop_task_stops_a_task_and_a_stopped_subagent_wakes_the_agent() {
    // The command.
    let mut fake = start(&says(bash(60_000)));
    user(&mut fake, UUID_A, "go");
    let first = fake.recv_until(is_result);
    let id = first.iter().find(|m| sys(m, "task_started")).unwrap()["task_id"].clone();
    fake.send(&json!({"type":"control_request","request_id":"s1","request":{"subtype":"stop_task","task_id":id}}));
    let got =
        fake.recv_until(|m| m["type"] == "control_response" && m["response"]["request_id"] == "s1");
    assert_eq!(
        task_frames(&got),
        [
            "system:task_updated",
            "system:task_notification",
            "system:background_tasks_changed",
            "control_response"
        ]
    );
    let frame = |sub: &str| got.iter().find(|m| sys(m, sub)).unwrap().clone();
    assert_eq!(frame("task_updated")["patch"]["status"], "killed");
    assert_eq!(frame("task_notification")["status"], "stopped");
    assert_eq!(frame("background_tasks_changed")["tasks"], json!([]));
    assert_eq!(got.last().unwrap()["response"]["subtype"], "success");
    assert!(
        fake.drain(Duration::from_millis(500))
            .iter()
            .all(|m| !sys(m, "init")),
        "stopping a background command starts no turn"
    );
    // An unknown id: success and nothing else (as the real CLI answers). A missing one: refused.
    fake.send(&json!({"type":"control_request","request_id":"s2","request":{"subtype":"stop_task","task_id":"nosuch"}}));
    let got =
        fake.recv_until(|m| m["type"] == "control_response" && m["response"]["request_id"] == "s2");
    assert_eq!(
        got.last().unwrap()["response"],
        json!({"subtype":"success","request_id":"s2","response":{}})
    );
    fake.send(
        &json!({"type":"control_request","request_id":"s3","request":{"subtype":"stop_task"}}),
    );
    let got =
        fake.recv_until(|m| m["type"] == "control_response" && m["response"]["request_id"] == "s3");
    assert_eq!(got.last().unwrap()["response"]["subtype"], "error");
    drop(fake);

    // The subagent.
    let mut fake = start(&says(agent(60_000)));
    user(&mut fake, UUID_A, "go");
    let first = fake.recv_until(is_result);
    let id = first.iter().find(|m| sys(m, "task_started")).unwrap()["task_id"].clone();
    fake.send(&json!({"type":"control_request","request_id":"s1","request":{"subtype":"stop_task","task_id":id}}));
    let got = fake.recv_until(is_result);
    let d = discs(&got);
    let at = |name: &str| {
        d.iter()
            .position(|x| x == name)
            .unwrap_or_else(|| panic!("{name} in {d:?}"))
    };
    assert!(at("system:task_notification") < at("control_response"));
    assert!(
        at("control_response") < at("system:init"),
        "the agent's turn follows the answer"
    );
    assert_eq!(got.last().unwrap()["result"], "the agent was stopped");
    assert!(got.last().unwrap()["origin"].is_object());
}

/// `interrupt` while only background work runs: answered, and nothing is stopped.
#[test]
fn interrupt_does_not_stop_background_tasks() {
    let mut fake = start(&says(bash(500)));
    user(&mut fake, UUID_A, "go");
    fake.recv_until(is_result);
    fake.send(
        &json!({"type":"control_request","request_id":"i1","request":{"subtype":"interrupt"}}),
    );
    let got =
        fake.recv_until(|m| m["type"] == "control_response" && m["response"]["request_id"] == "i1");
    assert_eq!(got.last().unwrap()["response"]["subtype"], "success");
    // The task still ends on its own and the agent still answers it.
    let rest = fake.recv_until(is_result);
    assert_eq!(
        rest.iter().find(|m| sys(m, "task_notification")).unwrap()["status"],
        "completed"
    );
    assert_eq!(rest.last().unwrap()["result"], "FINISHED");
}

/// An interrupt stops a turn the agent started by itself (recorded: `result` cancelled, with the
/// agent's own origin).
#[test]
fn an_interrupt_stops_the_agents_own_turn() {
    let mut task = bash(100);
    task.then = vec![
        Step::Say {
            text: "starting".into(),
            chunks: None,
        },
        Step::WaitForInterrupt,
    ];
    let mut fake = start(&says(task));
    user(&mut fake, UUID_A, "go");
    fake.recv_until(is_result);
    fake.recv_until(|m| m["type"] == "assistant");
    fake.send(
        &json!({"type":"control_request","request_id":"i1","request":{"subtype":"interrupt"}}),
    );
    let got = fake.recv_until(is_result);
    let result = got.last().unwrap();
    assert_eq!(result["subtype"], "error_during_execution");
    assert!(result["origin"].is_object());
}

/// A prompt read during the agent's own turn: `queued` at once, taken into the turn at the next
/// step boundary (`started` while the turn runs), `completed` just before the `result`, which
/// lists it (recorded in `agent_turn_prompt`).
#[test]
fn a_prompt_read_during_the_agents_own_turn_joins_it() {
    let mut task = bash(100);
    task.then = vec![
        Step::Say {
            text: "working".into(),
            chunks: None,
        },
        Step::SleepMs { ms: 700 },
        Step::Say {
            text: "FINISHED".into(),
            chunks: None,
        },
    ];
    let mut fake = start(&says(task));
    user(&mut fake, UUID_A, "go");
    fake.recv_until(is_result);
    fake.recv_until(|m| m["type"] == "assistant");
    user(&mut fake, UUID_B, "PONG please");
    let got = fake.recv_until(is_result);
    let d = discs(&got);
    let at = |name: &str| {
        d.iter()
            .position(|x| x == name)
            .unwrap_or_else(|| panic!("{name} in {d:?}"))
    };
    assert!(at("command_lifecycle:queued") < at("command_lifecycle:started"));
    assert!(at("command_lifecycle:started") < at("command_lifecycle:completed"));
    assert!(at("command_lifecycle:completed") < at("result:success"));
    assert_eq!(
        d.iter().filter(|x| *x == "system:init").count(),
        0,
        "it started no turn of its own"
    );
    let result = got.last().unwrap();
    assert_eq!(result["user_message_uuids"], json!([UUID_B]));
    assert!(result["origin"].is_object());
    // Nothing is left over: no turn follows.
    assert!(fake
        .drain(Duration::from_millis(400))
        .iter()
        .all(|m| !sys(m, "init")));
}

// ── shapes: every message of the fake is one the real CLI printed ───────────────────────────

fn recorded(names: &[&str]) -> Vec<Value> {
    names
        .iter()
        .flat_map(|n| lines("claude", n))
        .filter(|l| l.dir == Dir::Out)
        .map(|l| l.msg)
        .collect()
}

/// The messages the background features add, compared with the recordings of the real CLI. The
/// task frames and lifecycle frames have the keys and JSON kinds of the recorded ones, and the
/// fake prints every kind of frame the recordings show; what the features add to other messages
/// (the tags of a subagent's lines, the `origin` of a result, `user_message_uuids`) has the
/// recorded kinds. (The rest of an assistant or result message is cloned from the 2.1.326
/// recordings, which the other exchanges compare.)
#[test]
fn the_fakes_background_messages_have_the_shape_of_the_real_ones() {
    let refs = recorded(&[
        "background_bash",
        "background_agent",
        "stop_task",
        "agent_turn_interrupt",
        "agent_turn_prompt",
    ]);
    let mut seen: Vec<Value> = vec![];
    // A command, run to the agent's turn and a prompt joining it; a subagent, run to its end;
    // one stopped; an interrupted agent turn.
    {
        let mut task = bash(100);
        task.then = vec![
            Step::Say {
                text: "working".into(),
                chunks: None,
            },
            Step::SleepMs { ms: 500 },
            Step::Say {
                text: "FINISHED".into(),
                chunks: None,
            },
        ];
        let mut fake = start(&says(task));
        user(&mut fake, UUID_A, "go");
        fake.recv_until(is_result);
        fake.recv_until(|m| m["type"] == "assistant");
        user(&mut fake, UUID_B, "PONG");
        fake.recv_until(is_result);
        seen.extend(fake.seen.clone());
    }
    {
        let mut fake = start(&says(agent(100)));
        user(&mut fake, UUID_A, "go");
        fake.recv_until(is_result);
        fake.recv_until(is_result);
        seen.extend(fake.seen.clone());
    }
    {
        let mut fake = start(&says(agent(60_000)));
        user(&mut fake, UUID_A, "go");
        let first = fake.recv_until(is_result);
        let id = first.iter().find(|m| sys(m, "task_started")).unwrap()["task_id"].clone();
        fake.send(&json!({"type":"control_request","request_id":"s1","request":{"subtype":"stop_task","task_id":id}}));
        fake.recv_until(is_result);
        seen.extend(fake.seen.clone());
    }
    {
        let mut task = bash(100);
        task.then = vec![
            Step::Say {
                text: "x".into(),
                chunks: None,
            },
            Step::WaitForInterrupt,
        ];
        let mut fake = start(&says(task));
        user(&mut fake, UUID_A, "go");
        fake.recv_until(is_result);
        fake.recv_until(|m| m["type"] == "assistant");
        fake.send(
            &json!({"type":"control_request","request_id":"i1","request":{"subtype":"interrupt"}}),
        );
        fake.recv_until(is_result);
        seen.extend(fake.seen.clone());
    }
    let frame = |d: &str| {
        matches!(
            d,
            "system:task_started"
                | "system:task_progress"
                | "system:task_updated"
                | "system:task_notification"
                | "system:background_tasks_changed"
                | "command_lifecycle:queued"
                | "command_lifecycle:started"
                | "command_lifecycle:completed"
        )
    };
    let mut problems = vec![];
    for m in seen.iter().filter(|m| frame(&disc(m))) {
        let d = disc(m);
        let best = refs
            .iter()
            .filter(|r| disc(r) == d)
            .map(|r| shape_diff(m, r))
            .min_by_key(Vec::len);
        match best {
            None => problems.push(format!("[{d}] the recordings have no such message: {m}")),
            Some(diff) if !diff.is_empty() => problems.push(format!("[{d}] {}", diff.join("; "))),
            _ => {}
        }
    }
    for r in refs.iter().filter(|r| frame(&disc(r))) {
        let d = disc(r);
        if !seen.iter().any(|m| disc(m) == d) {
            problems.push(format!("[{d}] the real CLI printed it, the fake did not"));
        }
    }
    // What the features add to other messages.
    let kinds = |ms: &[&Value], key: &str| -> Vec<&'static str> {
        let mut k: Vec<&'static str> = ms
            .iter()
            .map(|m| match &m[key] {
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
                _ => "null",
            })
            .collect();
        k.sort();
        k.dedup();
        k
    };
    let tagged = |ms: &[Value]| -> Vec<Value> {
        ms.iter()
            .filter(|m| m["parent_tool_use_id"].is_string())
            .cloned()
            .collect()
    };
    let (real, fake) = (tagged(&refs), tagged(&seen));
    assert!(!real.is_empty() && !fake.is_empty());
    let (real, fake): (Vec<&Value>, Vec<&Value>) = (real.iter().collect(), fake.iter().collect());
    for key in [
        "parent_tool_use_id",
        "agent_id",
        "subagent_type",
        "task_description",
    ] {
        if kinds(&real, key) != kinds(&fake, key) {
            problems.push(format!(
                "tag {key}: {:?} != {:?}",
                kinds(&fake, key),
                kinds(&real, key)
            ));
        }
    }
    let results =
        |ms: &[Value]| -> Vec<Value> { ms.iter().filter(|m| is_result(m)).cloned().collect() };
    let origin_shape = |ms: &[Value]| -> Vec<String> {
        let mut k: Vec<String> = results(ms)
            .iter()
            .filter(|r| r["origin"].is_object())
            .map(|r| {
                let mut keys: Vec<&String> = r["origin"].as_object().unwrap().keys().collect();
                keys.sort();
                format!("{keys:?}")
            })
            .collect();
        k.sort();
        k.dedup();
        k
    };
    // (The recordings also hold a `peer` origin — a subagent's hand-back, with the report as its
    // `body` — which the fake does not model.)
    let recorded_origins = origin_shape(&refs);
    for shape in origin_shape(&seen) {
        if !recorded_origins.contains(&shape) {
            problems.push(format!(
                "result.origin keys {shape} are not in the recordings"
            ));
        }
    }
    assert!(!origin_shape(&seen).is_empty());
    let listed = |ms: &[Value]| {
        results(ms).iter().any(|r| {
            r["user_message_uuids"]
                .as_array()
                .is_some_and(|a| a.iter().all(Value::is_string))
        })
    };
    assert!(listed(&refs) && listed(&seen));
    problems.sort();
    problems.dedup();
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
