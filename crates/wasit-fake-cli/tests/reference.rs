//! Plays the client side of every recorded real exchange against the fake and checks that the
//! fake's output has the same structure (keys, JSON kinds) as what the real CLI printed.
//!
//! Matching is by message discriminator (claude `type`/`subtype`, JSON-RPC method or the
//! method a response answers), not by position: the real CLIs interleave notifications in
//! timing-dependent order.

mod common;

use common::{shape_diff, Fake, CLAUDE_ARGV};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use wasit_fake_cli::reference::{lines, Dir};
use wasit_fake_cli::scenario::{
    ModelInfo, Scenario, ScenarioBuilder, SessionInfo, Turn, TurnBuilder,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Wire {
    Claude,
    Codex,
    Acp,
}

fn wire_of(persona: &str) -> Wire {
    match persona {
        "claude" => Wire::Claude,
        "codex" => Wire::Codex,
        _ => Wire::Acp,
    }
}

/// Request id → method/subtype, from the recorded client lines (shared by both sides since the
/// replay re-sends the recorded ids).
type Methods = HashMap<String, String>;

fn id_key(v: &Value) -> String {
    v.to_string()
}

fn method_of_in(m: &Value, wire: Wire) -> Option<String> {
    match wire {
        Wire::Claude => (m["type"] == "control_request")
            .then(|| m["request"]["subtype"].as_str().unwrap_or("").to_string()),
        _ => m["method"].as_str().map(str::to_string),
    }
}

fn disc(m: &Value, wire: Wire, methods: &Methods) -> String {
    match wire {
        Wire::Claude => {
            let t = m["type"].as_str().unwrap_or("?");
            match t {
                "system" => format!("system:{}", m["subtype"].as_str().unwrap_or("")),
                "control_request" => {
                    format!(
                        "control_request:{}",
                        m["request"]["subtype"].as_str().unwrap_or("")
                    )
                }
                "control_response" => format!(
                    "control_response:{}:{}",
                    m["response"]["subtype"].as_str().unwrap_or(""),
                    methods
                        .get(&id_key(&m["response"]["request_id"]))
                        .map(String::as_str)
                        .unwrap_or("")
                ),
                "stream_event" => {
                    format!("stream_event:{}", m["event"]["type"].as_str().unwrap_or(""))
                }
                "assistant" | "user" => format!(
                    "{t}:{}",
                    m["message"]["content"][0]["type"].as_str().unwrap_or("")
                ),
                "result" => format!("result:{}", m["subtype"].as_str().unwrap_or("")),
                other => other.to_string(),
            }
        }
        Wire::Codex | Wire::Acp => {
            if let Some(method) = m["method"].as_str() {
                if m["id"].is_null() {
                    if wire == Wire::Acp && method == "session/update" {
                        return format!(
                            "update:{}",
                            m["params"]["update"]["sessionUpdate"]
                                .as_str()
                                .unwrap_or("")
                        );
                    }
                    if wire == Wire::Codex && method.starts_with("item/") && method.ends_with("ed")
                    {
                        return format!(
                            "n:{method}:{}",
                            m["params"]["item"]["type"].as_str().unwrap_or("")
                        );
                    }
                    return format!("n:{method}");
                }
                return format!("req:{method}");
            }
            let q = methods
                .get(&id_key(&m["id"]))
                .map(String::as_str)
                .unwrap_or("?");
            if m["error"].is_object() {
                format!("err:{q}")
            } else {
                format!("resp:{q}")
            }
        }
    }
}

/// Discriminators the real CLI emits that are environment noise a fake need not reproduce.
fn optional_in_reference(d: &str) -> bool {
    matches!(
        d,
        "rate_limit_event"
            | "n:warning"
            | "n:account/rateLimits/updated"
            | "n:thread/readState/changed"
            | "n:deprecationNotice"
            | "n:thread/goal/cleared"
    )
}

/// Is this recorded client line a response to a request the CLI made?
fn is_response_in(m: &Value, wire: Wire) -> bool {
    match wire {
        Wire::Claude => m["type"] == "control_response",
        _ => m["method"].is_null() && !m["id"].is_null(),
    }
}

fn is_interrupt_in(m: &Value, wire: Wire) -> bool {
    match wire {
        Wire::Claude => m["request"]["subtype"] == "interrupt",
        Wire::Codex => m["method"] == "turn/interrupt",
        Wire::Acp => m["method"] == "session/cancel",
    }
}

/// Server-initiated request (needs a client answer).
fn is_server_request(m: &Value, wire: Wire) -> bool {
    match wire {
        Wire::Claude => m["type"] == "control_request",
        _ => m["method"].is_string() && !m["id"].is_null(),
    }
}

fn strings_of(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::String(s) => {
            out.insert(s.clone());
        }
        Value::Array(a) => a.iter().for_each(|x| strings_of(x, out)),
        Value::Object(o) => o.values().for_each(|x| strings_of(x, out)),
        _ => {}
    }
}

/// Zip two messages; wherever the leaf strings differ and the reference's one is something the
/// client later sends back (an id), remember `reference → fake`.
fn learn_ids(
    reference: &Value,
    fake: &Value,
    wanted: &BTreeSet<String>,
    map: &mut HashMap<String, String>,
) {
    match (reference, fake) {
        (Value::String(r), Value::String(f)) if r != f && wanted.contains(r) => {
            map.insert(r.clone(), f.clone());
        }
        (Value::Array(r), Value::Array(f)) => {
            for (a, b) in r.iter().zip(f) {
                learn_ids(a, b, wanted, map);
            }
        }
        (Value::Object(r), Value::Object(f)) => {
            for (k, a) in r {
                if let Some(b) = f.get(k) {
                    learn_ids(a, b, wanted, map);
                }
            }
        }
        _ => {}
    }
}

fn translate(v: &mut Value, map: &HashMap<String, String>) {
    match v {
        Value::String(s) => {
            if let Some(f) = map.get(s.as_str()) {
                *s = f.clone();
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|x| translate(x, map)),
        Value::Object(o) => o.values_mut().for_each(|x| translate(x, map)),
        _ => {}
    }
}

struct Setup {
    argv: Vec<String>,
    persona: Option<&'static str>,
    scenario: Scenario,
}

fn first_in(ins: &[Value], f: impl Fn(&Value) -> bool) -> Option<&Value> {
    ins.iter().find(|m| f(m))
}

fn setup(persona: &'static str, name: &str, ins: &[Value], ref_discs: &BTreeSet<String>) -> Setup {
    let mut sc: ScenarioBuilder = Scenario::builder();
    let has = |d: &str| ref_discs.contains(d);
    let mut turn: TurnBuilder = Turn::builder();
    let tool_input =
        json!({"command": "touch /tmp/x/marker.txt", "description": "Create marker file"});
    let mut argv: Vec<String> = match persona {
        "claude" => CLAUDE_ARGV.iter().map(|s| s.to_string()).collect(),
        "codex" => vec!["app-server".into(), "--stdio".into()],
        _ => vec!["acp".into()],
    };
    let is_acp = !matches!(persona, "claude" | "codex");
    match name {
        "tool_allow" | "tool_deny" => {
            if has("update:agent_thought_chunk") || has("assistant:thinking") {
                turn = turn.think("Let me run it.");
            }
            turn = turn
                .tool("Bash", tool_input)
                .needs_permission()
                .result_if_allowed("(no output)")
                .result_if_denied("The user rejected permission to use this specific tool call.")
                .done();
            // After a rejected tool call the ACP agents stop without another message.
            if !(is_acp && name == "tool_deny") {
                turn = turn.say("Done.");
            }
        }
        "ask_question" | "question" => {
            turn = turn
                .ask_question(
                    "Do you prefer red or blue?",
                    &[("Red", "Choose red"), ("Blue", "Choose blue")],
                )
                .say("done");
        }
        "interrupt" => {
            if is_acp && has("update:agent_thought_chunk") {
                turn = turn.think("thinking");
            }
            turn = turn.say("partial").wait_for_interrupt();
        }
        "turn_error" | "error"
            if persona == "claude" || persona == "codex" || persona == "kilo" =>
        {
            turn = turn.fail("There's an issue with the selected model (no-such-model-xyz).");
        }
        _ => {
            if is_acp && has("update:agent_thought_chunk") {
                turn = turn.think("thinking");
            }
            turn = turn.say("pong");
        }
    }
    sc = sc.turn(turn);
    // Persisted sessions the recording resumes / loads.
    match (persona, name) {
        ("claude", "resume") => {
            let sid = lines("claude", "simple")
                .iter()
                .find_map(|l| l.msg["session_id"].as_str().map(str::to_string))
                .expect("session id");
            argv.push("--resume".into());
            argv.push(sid);
        }
        ("claude", "turn_error") => {
            argv = argv
                .iter()
                .map(|a| {
                    if a == "haiku" {
                        "no-such-model-xyz".to_string()
                    } else {
                        a.clone()
                    }
                })
                .collect();
        }
        ("claude", "partial_messages") => argv.push("--include-partial-messages".into()),
        ("codex", "resume") => {
            let tid = first_in(ins, |m| m["method"] == "thread/resume")
                .map(|m| m["params"]["threadId"].as_str().unwrap().to_string());
            sc = sc.session(SessionInfo {
                id: tid.unwrap(),
                ..SessionInfo::default()
            });
        }
        ("codex", "lifecycle") => {}
        (_, "load") => {
            let sid = first_in(ins, |m| m["method"] == "session/load")
                .map(|m| m["params"]["sessionId"].as_str().unwrap().to_string());
            sc = sc.session(SessionInfo {
                id: sid.unwrap(),
                title: Some("Request for exact pong reply".into()),
                history: vec![
                    wasit_fake_cli::scenario::HistoryMessage {
                        role: "user".into(),
                        text: "Reply with exactly the word: pong".into(),
                    },
                    wasit_fake_cli::scenario::HistoryMessage {
                        role: "assistant".into(),
                        text: "pong".into(),
                    },
                ],
                ..SessionInfo::default()
            });
        }
        _ => {}
    }
    if is_acp {
        sc = sc.model("opencode/deepseek-v4.1-flash-free", "x");
    }
    let mut scenario = sc.build();
    if is_acp {
        // Keep the default model list (big-pickle first) plus the free model the recordings select.
        scenario.models = if persona == "opencode" {
            vec![
                ModelInfo {
                    id: "opencode/big-pickle".into(),
                    name: Some("OpenCode Zen/Big Pickle".into()),
                    description: None,
                },
                ModelInfo {
                    id: "opencode/deepseek-v4.1-flash-free".into(),
                    name: Some("OpenCode Zen/DeepSeek V4.1 Flash Free".into()),
                    description: None,
                },
            ]
        } else {
            vec![ModelInfo {
                id: "kilo/kilo-auto/free".into(),
                name: Some("Kilo Gateway/Kilo Auto Free".into()),
                description: None,
            }]
        };
    }
    Setup {
        argv,
        persona: Some(persona),
        scenario,
    }
}

/// What the fake must have produced before the replay may send the next client line.
fn wait_for(
    fake: &mut Fake,
    wire: Wire,
    cur: &Value,
    next: Option<&Value>,
    since: usize,
    methods: &Methods,
    lifecycle: bool,
) {
    let found = |fake: &Fake, p: &dyn Fn(&Value) -> bool| fake.seen[since..].iter().any(p);
    let drain_until = |fake: &mut Fake, p: &dyn Fn(&Value) -> bool| {
        while !found(fake, p) {
            if fake.recv_timeout(common::WAIT).is_none() {
                let summary: Vec<String> = fake.seen[since..]
                    .iter()
                    .map(|m| disc(m, wire, methods))
                    .collect();
                panic!(
                    "timed out waiting after {cur}; got {summary:?}\nfake stderr: {}",
                    fake.stderr()
                );
            }
        }
    };
    let completes = |m: &Value| -> bool {
        match wire {
            Wire::Claude => {
                if cur["type"] == "user" {
                    m["type"] == "result"
                } else {
                    m["type"] == "control_response"
                        && m["response"]["request_id"] == cur["request_id"]
                }
            }
            Wire::Codex => {
                if cur["method"] == "turn/start" && !is_error_response(&m_for_id(m)) {
                    // A started turn is over at turn/completed (a rejected start only errors).
                    m["method"] == "turn/completed"
                        || (m["id"] == cur["id"] && m["error"].is_object())
                } else {
                    m["method"].is_null() && m["id"] == cur["id"]
                }
            }
            Wire::Acp => m["method"].is_null() && m["id"] == cur["id"],
        }
    };
    let progress = |m: &Value| -> bool {
        match wire {
            Wire::Claude => m["type"] == "assistant",
            Wire::Codex => m["method"] == "item/agentMessage/delta",
            Wire::Acp => disc(m, wire, methods) == "update:agent_message_chunk",
        }
    };
    if cur["method"] == "initialized"
        || (cur["method"].is_string() && cur["id"].is_null() && wire != Wire::Acp)
    {
        return;
    }
    match next {
        Some(n) if is_response_in(n, wire) => drain_until(fake, &|m| is_server_request(m, wire)),
        Some(n) if !lifecycle && is_interrupt_in(n, wire) => drain_until(fake, &progress),
        _ => {
            if !lifecycle && is_interrupt_in(cur, wire) {
                match wire {
                    Wire::Claude => drain_until(fake, &|m| m["type"] == "result"),
                    Wire::Codex => drain_until(fake, &|m| m["method"] == "turn/completed"),
                    Wire::Acp => {
                        drain_until(fake, &|m| m["method"].is_null() && !m["id"].is_null())
                    }
                }
            } else if is_response_in(cur, wire) {
                // The turn continues after an answer; the next wait (or the final drain) covers it.
            } else {
                drain_until(fake, &completes);
            }
        }
    }
}

fn m_for_id(m: &Value) -> Value {
    m.clone()
}

fn is_error_response(m: &Value) -> bool {
    m["error"].is_object()
}

fn replay(persona: &'static str, name: &str) {
    let wire = wire_of(persona);
    let rec = lines(persona, name);
    assert!(!rec.is_empty(), "no recording {persona}/{name}");
    let ins: Vec<Value> = rec
        .iter()
        .filter(|l| l.dir == Dir::In)
        .map(|l| l.msg.clone())
        .collect();
    let refs: Vec<Value> = rec
        .iter()
        .filter(|l| l.dir == Dir::Out)
        .map(|l| l.msg.clone())
        .collect();
    let mut methods: Methods = HashMap::new();
    for m in &ins {
        if let Some(method) = method_of_in(m, wire) {
            let id = if wire == Wire::Claude {
                &m["request_id"]
            } else {
                &m["id"]
            };
            if !id.is_null() {
                methods.insert(id_key(id), method);
            }
        }
    }
    let ref_discs: BTreeSet<String> = refs.iter().map(|m| disc(m, wire, &methods)).collect();
    let Setup {
        argv,
        persona: p,
        scenario,
    } = setup(persona, name, &ins, &ref_discs);
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let mut fake = Fake::spawn(&argv_refs, p, &scenario);

    let mut wanted = BTreeSet::new();
    ins.iter().for_each(|m| strings_of(m, &mut wanted));
    let mut idmap: HashMap<String, String> = HashMap::new();
    let mut learned_upto = 0usize;
    let mut last_server_request: Option<Value> = None;

    for (i, line) in ins.iter().enumerate() {
        let mut msg = line.clone();
        translate(&mut msg, &idmap);
        // Answer the CLI's most recent request: ids of responses are the fake's, not the recording's.
        if is_response_in(&msg, wire) {
            let req = last_server_request.clone().expect("a request to answer");
            match wire {
                Wire::Claude => msg["response"]["request_id"] = req["request_id"].clone(),
                _ => msg["id"] = req["id"].clone(),
            }
        }
        let since = fake.seen.len();
        fake.send(&msg);
        wait_for(
            &mut fake,
            wire,
            &msg,
            ins.get(i + 1),
            since,
            &methods,
            name == "lifecycle",
        );
        // Learn id substitutions from the fake output so far, pairing k-th messages per discriminator.
        let mut fake_by: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for m in &fake.seen {
            fake_by.entry(disc(m, wire, &methods)).or_default().push(m);
        }
        let mut ref_by: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for m in &refs {
            ref_by.entry(disc(m, wire, &methods)).or_default().push(m);
        }
        for (d, fs) in &fake_by {
            if let Some(rs) = ref_by.get(d) {
                for (f, r) in fs.iter().zip(rs) {
                    learn_ids(r, f, &wanted, &mut idmap);
                }
            }
        }
        learned_upto = learned_upto.max(fake.seen.len());
        last_server_request = fake
            .seen
            .iter()
            .rev()
            .find(|m| is_server_request(m, wire))
            .cloned();
    }
    let _ = learned_upto;
    fake.drain(std::time::Duration::from_millis(400));

    // Compare structure per discriminator.
    let mut ref_by: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for m in &refs {
        ref_by.entry(disc(m, wire, &methods)).or_default().push(m);
    }
    let mut fake_by: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for m in &fake.seen {
        fake_by.entry(disc(m, wire, &methods)).or_default().push(m);
    }
    let mut problems = vec![];
    // A recorded interrupt happened before any output; the fake streams a first chunk so the
    // client has something to interrupt. Those chunk shapes are verified by the other recordings.
    let extra_ok = |d: &str| {
        name == "interrupt"
            && (d.starts_with("assistant:")
                || d.starts_with("update:agent_message")
                || d.starts_with("n:item/")
                || d.starts_with("n:thread/tokenUsage"))
    };
    for d in ref_by.keys() {
        if !fake_by.contains_key(d) && !optional_in_reference(d) {
            problems.push(format!("[{d}] real CLI emitted it, the fake did not"));
        }
    }
    for (d, fs) in &fake_by {
        let Some(rs) = ref_by.get(d) else {
            if extra_ok(d) {
                continue;
            }
            problems.push(format!(
                "[{d}] the fake emitted a message the real CLI never did in this exchange: {}",
                fs[0]
            ));
            continue;
        };
        for f in fs {
            // Must have the shape of at least one recorded message with the same discriminator.
            let best = rs
                .iter()
                .map(|r| shape_diff(f, r))
                .min_by_key(Vec::len)
                .unwrap_or_default();
            if !best.is_empty() {
                problems.push(format!("[{d}] {}", best.join("; ")));
            }
        }
    }
    assert!(
        problems.is_empty(),
        "{persona}/{name}: shape mismatches:\n{}",
        problems.join("\n")
    );
}

macro_rules! replays {
    ($($fn_name:ident => ($persona:literal, $rec:literal)),* $(,)?) => {
        $( #[test] fn $fn_name() { replay($persona, $rec); } )*
    };
}

replays! {
    claude_simple => ("claude", "simple"),
    claude_tool_allow => ("claude", "tool_allow"),
    claude_tool_deny => ("claude", "tool_deny"),
    claude_ask_question => ("claude", "ask_question"),
    claude_interrupt => ("claude", "interrupt"),
    claude_resume => ("claude", "resume"),
    claude_set_mode => ("claude", "set_mode"),
    claude_turn_error => ("claude", "turn_error"),
    claude_partial_messages => ("claude", "partial_messages"),
    codex_handshake => ("codex", "handshake"),
    codex_simple => ("codex", "simple"),
    codex_tool_allow => ("codex", "tool_allow"),
    codex_tool_deny => ("codex", "tool_deny"),
    codex_interrupt => ("codex", "interrupt"),
    codex_resume => ("codex", "resume"),
    codex_mode => ("codex", "mode"),
    codex_error => ("codex", "error"),
    codex_question => ("codex", "question"),
    codex_lifecycle => ("codex", "lifecycle"),
    opencode_handshake => ("opencode", "handshake"),
    opencode_simple => ("opencode", "simple"),
    opencode_tool_allow => ("opencode", "tool_allow"),
    opencode_tool_deny => ("opencode", "tool_deny"),
    opencode_interrupt => ("opencode", "interrupt"),
    opencode_load => ("opencode", "load"),
    opencode_mode => ("opencode", "mode"),
    opencode_error => ("opencode", "error"),
    opencode_lifecycle => ("opencode", "lifecycle"),
    kilo_handshake => ("kilo", "handshake"),
    kilo_simple => ("kilo", "simple"),
    kilo_tool_allow => ("kilo", "tool_allow"),
    kilo_tool_deny => ("kilo", "tool_deny"),
    kilo_interrupt => ("kilo", "interrupt"),
    kilo_load => ("kilo", "load"),
    kilo_mode => ("kilo", "mode"),
    kilo_error => ("kilo", "error"),
    kilo_lifecycle => ("kilo", "lifecycle"),
}
