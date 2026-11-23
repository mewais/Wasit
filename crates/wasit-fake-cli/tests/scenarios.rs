//! Scenario-step behaviour, driven through each persona's real wire protocol:
//! permission allow/deny, questions, interrupt, process death mid-turn, hang, garbage lines,
//! failures, usage, turn selection, resume/load, discovery commands.

mod common;

use common::{Fake, CLAUDE_ARGV};
use serde_json::{json, Value};
use std::process::Command;
use std::time::Duration;
use wasit_fake_cli::scenario::{HistoryMessage, Scenario, SessionInfo, Turn};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Wire {
    Claude,
    Codex,
    Acp,
}

#[derive(Debug, PartialEq, Eq)]
enum End {
    Completed,
    Interrupted,
    Failed,
}

/// A client speaking one persona's protocol.
struct Client {
    fake: Fake,
    wire: Wire,
    next_id: u64,
    session: String,
    prompt_id: Value,
}

impl Client {
    fn start(persona: &'static str, scenario: &Scenario) -> Client {
        let (wire, argv): (Wire, Vec<&str>) = match persona {
            "claude" => (Wire::Claude, CLAUDE_ARGV.to_vec()),
            "codex" => (Wire::Codex, vec!["app-server", "--stdio"]),
            _ => (Wire::Acp, vec!["acp"]),
        };
        let fake = Fake::spawn(&argv, Some(persona), scenario);
        let mut c = Client {
            fake,
            wire,
            next_id: 1,
            session: String::new(),
            prompt_id: Value::Null,
        };
        c.handshake();
        c
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.fake
            .send(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        let id = json!(id);
        self.fake
            .recv_until(|m| m["id"] == id && m["method"].is_null())
            .pop()
            .expect("response")
    }

    fn handshake(&mut self) {
        match self.wire {
            Wire::Claude => {
                self.fake.send(&json!({"type":"control_request","request_id":"init","request":{"subtype":"initialize","hooks":null}}));
                self.fake.recv_until(|m| m["type"] == "control_response");
            }
            Wire::Codex => {
                self.fake.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"t","version":"0"},"capabilities":{"experimentalApi":true}}}));
                self.fake.recv_until(|m| m["id"] == 1);
                self.fake
                    .send(&json!({"jsonrpc":"2.0","method":"initialized"}));
                let r = self.request(
                    "thread/start",
                    json!({"cwd":"/tmp","approvalPolicy":"untrusted"}),
                );
                self.session = r["result"]["thread"]["id"].as_str().unwrap().to_string();
            }
            Wire::Acp => {
                self.fake.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{}}}));
                self.fake.recv_until(|m| m["id"] == 1);
                let r = self.request("session/new", json!({"cwd":"/tmp","mcpServers":[]}));
                self.session = r["result"]["sessionId"].as_str().unwrap().to_string();
            }
        }
    }

    fn prompt(&mut self, text: &str) {
        match self.wire {
            Wire::Claude => self.fake.send(&json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":text}]}})),
            Wire::Codex => {
                self.next_id += 1;
                self.prompt_id = json!(self.next_id);
                self.fake.send(&json!({"jsonrpc":"2.0","id":self.next_id,"method":"turn/start","params":{"threadId":self.session,"input":[{"type":"text","text":text}]}}));
            }
            Wire::Acp => {
                self.next_id += 1;
                self.prompt_id = json!(self.next_id);
                self.fake.send(&json!({"jsonrpc":"2.0","id":self.next_id,"method":"session/prompt","params":{"sessionId":self.session,"prompt":[{"type":"text","text":text}]}}));
            }
        }
    }

    fn is_end(&self, m: &Value) -> bool {
        match self.wire {
            Wire::Claude => m["type"] == "result",
            Wire::Codex => m["method"] == "turn/completed",
            Wire::Acp => m["id"] == self.prompt_id && m["method"].is_null(),
        }
    }

    fn is_gate(&self, m: &Value) -> bool {
        match self.wire {
            Wire::Claude => m["type"] == "control_request",
            Wire::Codex => m["method"].is_string() && !m["id"].is_null(),
            Wire::Acp => m["method"] == "session/request_permission",
        }
    }

    /// First streamed agent text (the point where an interrupt is meaningful).
    fn progress(wire: Wire, m: &Value) -> bool {
        match wire {
            Wire::Claude => m["type"] == "assistant",
            Wire::Codex => m["method"] == "item/agentMessage/delta",
            Wire::Acp => m["params"]["update"]["sessionUpdate"] == "agent_message_chunk",
        }
    }

    fn answer(&mut self, gate: &Value, allow: bool) {
        let msg = match self.wire {
            Wire::Claude => {
                let inner = if allow {
                    json!({"behavior":"allow","updatedInput": gate["request"]["input"]})
                } else {
                    json!({"behavior":"deny","message":"not today"})
                };
                json!({"type":"control_response","response":{"subtype":"success","request_id":gate["request_id"],"response":inner}})
            }
            Wire::Codex => {
                json!({"jsonrpc":"2.0","id":gate["id"],"result":{"decision": if allow {"accept"} else {"decline"}}})
            }
            Wire::Acp => {
                json!({"jsonrpc":"2.0","id":gate["id"],"result":{"outcome":{"outcome":"selected","optionId": if allow {"once"} else {"reject"}}}})
            }
        };
        self.fake.send(&msg);
    }

    fn interrupt(&mut self) {
        match self.wire {
            Wire::Claude => self.fake.send(&json!({"type":"control_request","request_id":"int","request":{"subtype":"interrupt"}})),
            Wire::Codex => {
                let turn = self
                    .fake
                    .seen
                    .iter()
                    .rev()
                    .find(|m| m["method"] == "turn/started")
                    .map(|m| m["params"]["turn"]["id"].clone())
                    .expect("turn id");
                self.next_id += 1;
                self.fake.send(&json!({"jsonrpc":"2.0","id":self.next_id,"method":"turn/interrupt","params":{"threadId":self.session,"turnId":turn}}));
            }
            Wire::Acp => self.fake.send(&json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":self.session}})),
        }
    }

    fn end_kind(&self, end: &Value) -> End {
        match self.wire {
            Wire::Claude => match (end["subtype"].as_str(), end["is_error"].as_bool()) {
                (Some("error_during_execution"), _) => End::Interrupted,
                (_, Some(true)) => End::Failed,
                _ => End::Completed,
            },
            Wire::Codex => match end["params"]["turn"]["status"].as_str() {
                Some("interrupted") => End::Interrupted,
                Some("failed") => End::Failed,
                _ => End::Completed,
            },
            Wire::Acp => {
                if end["error"].is_object() {
                    End::Failed
                } else if end["result"]["stopReason"] == "cancelled" {
                    End::Interrupted
                } else {
                    End::Completed
                }
            }
        }
    }

    /// Run a turn to its end, returning every message of the turn.
    fn run_to_end(&mut self) -> Vec<Value> {
        let wire = self.wire;
        let pid = self.prompt_id.clone();
        self.fake.recv_until(move |m| match wire {
            Wire::Claude => m["type"] == "result",
            Wire::Codex => m["method"] == "turn/completed",
            Wire::Acp => m["id"] == pid && m["method"].is_null(),
        })
    }

    fn final_text(&self, msgs: &[Value]) -> String {
        match self.wire {
            Wire::Claude => msgs
                .last()
                .map(|m| m["result"].as_str().unwrap_or("").to_string())
                .unwrap_or_default(),
            Wire::Codex => msgs
                .iter()
                .filter(|m| {
                    m["method"] == "item/completed" && m["params"]["item"]["type"] == "agentMessage"
                })
                .map(|m| {
                    m["params"]["item"]["text"]
                        .as_str()
                        .unwrap_or("")
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(""),
            Wire::Acp => msgs
                .iter()
                .filter(|m| m["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
                .map(|m| {
                    m["params"]["update"]["content"]["text"]
                        .as_str()
                        .unwrap_or("")
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }

    /// `(allowed, tool output text)` of the tool call in `msgs`.
    fn tool_result(&self, msgs: &[Value]) -> (bool, String) {
        match self.wire {
            Wire::Claude => {
                let m = msgs
                    .iter()
                    .find(|m| {
                        m["type"] == "user" && m["message"]["content"][0]["type"] == "tool_result"
                    })
                    .expect("tool_result message");
                let c = &m["message"]["content"][0];
                (
                    c["is_error"] == false,
                    c["content"].as_str().unwrap_or("").to_string(),
                )
            }
            Wire::Codex => {
                let m = msgs
                    .iter()
                    .find(|m| {
                        m["method"] == "item/completed"
                            && m["params"]["item"]["type"] == "commandExecution"
                    })
                    .expect("commandExecution completed");
                let it = &m["params"]["item"];
                (
                    it["status"] == "completed",
                    it["aggregatedOutput"].as_str().unwrap_or("").to_string(),
                )
            }
            Wire::Acp => {
                let m = msgs
                    .iter()
                    .find(|m| {
                        let u = &m["params"]["update"];
                        u["sessionUpdate"] == "tool_call_update"
                            && (u["status"] == "completed" || u["status"] == "failed")
                    })
                    .expect("terminal tool_call_update");
                let u = &m["params"]["update"];
                (
                    u["status"] == "completed",
                    u["content"][0]["content"]["text"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                )
            }
        }
    }
}

fn tool_scenario() -> Scenario {
    Scenario::builder()
        .turn(
            Turn::builder()
                .say("running it")
                .tool(
                    "Bash",
                    json!({"command": "touch marker", "description": "make marker"}),
                )
                .needs_permission()
                .result_if_allowed("TOOL-OUTPUT")
                .result_if_denied("TOOL-DENIED")
                .done()
                .say("all done"),
        )
        .build()
}

macro_rules! per_persona {
    ($($name:ident),* $(,)?) => {
        mod claude_wire { $( per_persona!(@one $name, "claude"); )* }
        mod codex_wire { $( per_persona!(@one $name, "codex"); )* }
        mod opencode_wire { $( per_persona!(@one $name, "opencode"); )* }
        mod kilo_wire { $( per_persona!(@one $name, "kilo"); )* }
    };
    (@one $name:ident, $persona:literal) => {
        #[test]
        fn $name() { super::scenario_impls::$name($persona); }
    };
}

mod scenario_impls {
    use super::*;

    pub fn echo_by_default(persona: &'static str) {
        let mut c = Client::start(persona, &Scenario::default());
        c.prompt("hello there");
        let msgs = c.run_to_end();
        assert_eq!(c.final_text(&msgs), "echo: hello there");
        assert_eq!(c.end_kind(msgs.last().unwrap()), End::Completed);
        // A second turn works on the same process.
        c.prompt("again");
        let msgs = c.run_to_end();
        assert_eq!(c.final_text(&msgs), "echo: again");
    }

    pub fn turns_in_order_and_by_regex(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("first"))
            .turn(Turn::when("^deploy").say("deploying"))
            .turn(Turn::builder().say("second"))
            .build();
        let mut c = Client::start(persona, &sc);
        for (prompt, want) in [
            ("a", "first"),
            ("deploy now", "deploying"),
            ("b", "second"),
            ("deploy again", "deploying"),
            ("c", "echo: c"),
        ] {
            c.prompt(prompt);
            let msgs = c.run_to_end();
            assert_eq!(c.final_text(&msgs), want, "prompt {prompt:?}");
        }
    }

    pub fn permission_allow(persona: &'static str) {
        let mut c = Client::start(persona, &tool_scenario());
        c.prompt("go");
        let gate = c
            .fake
            .recv_until(|m| {
                m["method"].is_string() && !m["id"].is_null() || m["type"] == "control_request"
            })
            .pop()
            .unwrap();
        assert!(c.is_gate(&gate));
        c.answer(&gate, true);
        let msgs = c.run_to_end();
        let (allowed, text) = c.tool_result(&msgs);
        assert!(allowed);
        assert_eq!(text, "TOOL-OUTPUT");
        assert!(
            c.final_text(&msgs).contains("all done"),
            "turn continues after the tool"
        );
    }

    pub fn permission_deny(persona: &'static str) {
        let mut c = Client::start(persona, &tool_scenario());
        c.prompt("go");
        let gate = c
            .fake
            .recv_until(|m| {
                m["method"].is_string() && !m["id"].is_null() || m["type"] == "control_request"
            })
            .pop()
            .unwrap();
        c.answer(&gate, false);
        let msgs = c.run_to_end();
        let (allowed, text) = c.tool_result(&msgs);
        assert!(!allowed);
        if persona == "claude" {
            assert_eq!(
                text, "not today",
                "claude relays the client's own deny message"
            );
            let result = msgs.last().unwrap();
            assert_eq!(result["permission_denials"].as_array().unwrap().len(), 1);
        }
    }

    pub fn interrupt_ends_turn_as_interrupted(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("working").sleep_ms(60_000).say("never"))
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("long task");
        let w = c.wire;
        c.fake.recv_until(move |m| Client::progress(w, m));
        c.interrupt();
        let msgs = c.run_to_end();
        assert_eq!(c.end_kind(msgs.last().unwrap()), End::Interrupted);
        assert!(!c.final_text(&msgs).contains("never"));
        // The session is still usable.
        c.prompt("next");
        let msgs = c.run_to_end();
        assert_eq!(c.end_kind(msgs.last().unwrap()), End::Completed);
    }

    pub fn wait_for_interrupt_blocks_until_interrupted(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("waiting").wait_for_interrupt())
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let w = c.wire;
        c.fake.recv_until(move |m| Client::progress(w, m));
        // Nothing ends the turn on its own.
        let quiet = c.fake.drain(Duration::from_millis(400));
        assert!(
            !quiet.iter().any(|m| c.is_end(m)),
            "turn ended without an interrupt: {quiet:?}"
        );
        c.interrupt();
        let msgs = c.run_to_end();
        assert_eq!(c.end_kind(msgs.last().unwrap()), End::Interrupted);
    }

    pub fn exit_mid_turn(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(
                Turn::builder()
                    .say("about to die")
                    .exit(7)
                    .say("unreachable"),
            )
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let status = c.fake.wait_exit().expect("process should exit");
        assert_eq!(status.code(), Some(7));
        let rest = c.fake.drain(Duration::from_millis(200));
        assert!(
            !rest.iter().any(|m| c.is_end(m)),
            "no turn end after the process died: {rest:?}"
        );
    }

    pub fn hang_stops_responding(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("then silence").hang())
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let w = c.wire;
        c.fake.recv_until(move |m| Client::progress(w, m));
        c.interrupt();
        let quiet = c.fake.drain(Duration::from_millis(500));
        assert!(
            quiet.is_empty() || !quiet.iter().any(|m| c.is_end(m)),
            "hung fake answered: {quiet:?}"
        );
        assert!(c.fake.is_running(), "a hung fake stays alive");
        // Closing stdin releases it.
        c.fake.close_stdin();
        assert!(c.fake.wait_exit().is_some(), "hung fake exits on EOF");
    }

    pub fn garbage_line_then_continues(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(
                Turn::builder()
                    .say("before")
                    .garbage("}{ definitely not json")
                    .say("after"),
            )
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let msgs = c.run_to_end();
        assert!(
            c.fake.raw.iter().any(|l| l == "}{ definitely not json"),
            "raw lines: {:?}",
            c.fake.raw
        );
        assert_eq!(c.end_kind(msgs.last().unwrap()), End::Completed);
        assert!(c.final_text(&msgs).contains("after"));
    }

    pub fn stderr_step(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().stderr("boom on stderr").say("ok"))
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        c.run_to_end();
        assert!(c.fake.stderr_to_end().contains("boom on stderr"));
    }

    pub fn fail_step(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("start").fail("upstream exploded"))
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let msgs = c.run_to_end();
        let end = msgs.last().unwrap();
        assert_eq!(c.end_kind(end), End::Failed);
        assert!(end.to_string().contains("upstream exploded"), "{end}");
    }

    pub fn question_round_trip(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(
                Turn::builder()
                    .ask_question("Red or blue?", &[("Red", "r"), ("Blue", "b")])
                    .say("thanks"),
            )
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let gate = c
            .fake
            .recv_until(|m| {
                m["method"].is_string() && !m["id"].is_null() || m["type"] == "control_request"
            })
            .pop()
            .unwrap();
        // The question reaches the client with its options.
        let text = gate.to_string();
        assert!(
            text.contains("Red or blue?") && text.contains("Blue"),
            "{text}"
        );
        match c.wire {
            Wire::Claude => {
                assert_eq!(gate["request"]["tool_name"], "AskUserQuestion");
                let q = gate["request"]["input"]["questions"].clone();
                c.fake.send(&json!({"type":"control_response","response":{"subtype":"success","request_id":gate["request_id"],
                    "response":{"behavior":"allow","updatedInput":{"questions":q,"answers":{"Red or blue?":"Blue"}}}}}));
            }
            Wire::Codex => {
                assert_eq!(gate["method"], "item/tool/requestUserInput");
                let qid = gate["params"]["questions"][0]["id"].clone();
                c.fake.send(&json!({"jsonrpc":"2.0","id":gate["id"],"result":{"answers":{qid.as_str().unwrap():{"answers":["Blue"]}}}}));
            }
            Wire::Acp => {
                assert_eq!(gate["method"], "session/request_permission");
                c.fake.send(&json!({"jsonrpc":"2.0","id":gate["id"],"result":{"outcome":{"outcome":"selected","optionId":"Blue"}}}));
            }
        }
        let msgs = c.run_to_end();
        assert_eq!(c.end_kind(msgs.last().unwrap()), End::Completed);
        assert!(c.final_text(&msgs).contains("thanks"));
        if c.wire == Wire::Claude {
            assert!(
                msgs.iter()
                    .any(|m| m.to_string().contains("\\\"Red or blue?\\\"=\\\"Blue\\\"")),
                "answer is relayed to the model"
            );
        }
    }

    pub fn usage_is_reported(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("hi").usage(123, 45))
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let msgs = c.run_to_end();
        let all = Value::Array(msgs).to_string();
        assert!(
            all.contains("123") && all.contains("45"),
            "usage numbers appear on the wire: {all}"
        );
    }

    pub fn chunked_say(persona: &'static str) {
        let sc = Scenario::builder()
            .turn(Turn::builder().say_chunked("abcdefghij", 5))
            .build();
        let mut c = Client::start(persona, &sc);
        c.prompt("x");
        let msgs = c.run_to_end();
        assert_eq!(c.final_text(&msgs), "abcdefghij");
        let deltas = msgs
            .iter()
            .filter(|m| {
                m["method"] == "item/agentMessage/delta"
                    || m["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
            })
            .count();
        if c.wire != Wire::Claude {
            assert_eq!(deltas, 5, "five streamed chunks");
        }
    }

    pub fn session_id_from_scenario(persona: &'static str) {
        let sc = Scenario::builder()
            .session_id("11111111-2222-4333-8444-555555555555")
            .build();
        let c = Client::start(persona, &sc);
        match c.wire {
            Wire::Claude => {
                let mut c = c;
                c.prompt("x");
                let msgs = c.run_to_end();
                assert!(msgs
                    .iter()
                    .any(|m| m["session_id"] == "11111111-2222-4333-8444-555555555555"));
            }
            _ => assert_eq!(c.session, "11111111-2222-4333-8444-555555555555"),
        }
    }
}

per_persona! {
    echo_by_default,
    turns_in_order_and_by_regex,
    permission_allow,
    permission_deny,
    interrupt_ends_turn_as_interrupted,
    wait_for_interrupt_blocks_until_interrupted,
    exit_mid_turn,
    hang_stops_responding,
    garbage_line_then_continues,
    stderr_step,
    fail_step,
    question_round_trip,
    usage_is_reported,
    chunked_say,
    session_id_from_scenario,
}

// ---------------------------------------------------------------------------------------------
// Persona-specific behaviour
// ---------------------------------------------------------------------------------------------

#[test]
fn claude_control_requests() {
    let sc = Scenario::builder()
        .model("sonnet-fake", "Sonnet Fake")
        .command("review", "Review the diff")
        .build();
    let mut c = Client::start("claude", &sc);
    c.fake.send(&json!({"type":"control_request","request_id":"m1","request":{"subtype":"set_permission_mode","mode":"plan"}}));
    let r = c
        .fake
        .recv_until(|m| m["type"] == "control_response")
        .pop()
        .unwrap();
    assert_eq!(r["response"]["response"]["mode"], "plan");
    c.fake.send(&json!({"type":"control_request","request_id":"m2","request":{"subtype":"set_permission_mode","mode":"nope"}}));
    let r = c
        .fake
        .recv_until(|m| m["type"] == "control_response" && m["response"]["request_id"] == "m2")
        .pop()
        .unwrap();
    assert_eq!(r["response"]["subtype"], "error");
    assert_eq!(r["response"]["error_code"], "invalid_mode");
    // Models and commands in the initialize reply come from the scenario.
    c.fake.send(&json!({"type":"control_request","request_id":"i2","request":{"subtype":"initialize","hooks":null}}));
    let init = c
        .fake
        .recv_until(|m| m["type"] == "control_response" && m["response"]["request_id"] == "i2")
        .pop()
        .unwrap();
    assert_eq!(
        init["response"]["response"]["models"][0]["value"],
        "sonnet-fake"
    );
    assert_eq!(
        init["response"]["response"]["commands"][0]["name"],
        "review"
    );
}

#[test]
fn claude_resume_argument_sets_session_id() {
    let mut fake = Fake::spawn(
        &[CLAUDE_ARGV.as_slice(), &["--resume", "abc-123"]].concat(),
        Some("claude"),
        &Scenario::default(),
    );
    fake.send(
        &json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}),
    );
    let init = fake
        .recv_until(|m| m["type"] == "system" && m["subtype"] == "init")
        .pop()
        .unwrap();
    assert_eq!(init["session_id"], "abc-123");
    assert_eq!(init["model"], "claude-haiku-5-5");
}

#[test]
fn claude_exits_on_stdin_eof() {
    let mut c = Client::start("claude", &Scenario::default());
    c.fake.close_stdin();
    assert_eq!(c.fake.wait_exit().and_then(|s| s.code()), Some(0));
}

fn acp_models_and_modes(persona: &'static str) {
    let sc = Scenario::builder()
        .model("p/one", "One")
        .model("p/two", "Two")
        .mode("alpha", "Alpha")
        .mode("beta", "Beta")
        .build();
    let mut c = Client::start(persona, &sc);
    let s = c.session.clone();
    let r = c.request(
        "session/set_config_option",
        json!({"sessionId": s, "configId":"mode","value":"beta"}),
    );
    let opts = r["result"]["configOptions"].as_array().unwrap();
    let mode = opts.iter().find(|o| o["id"] == "mode").unwrap();
    assert_eq!(mode["currentValue"], "beta");
    assert_eq!(mode["options"].as_array().unwrap().len(), 2);
    let model = opts.iter().find(|o| o["id"] == "model").unwrap();
    assert_eq!(model["currentValue"], "p/one");
    let r = c.request(
        "session/set_config_option",
        json!({"sessionId": s, "configId":"mode","value":"gamma"}),
    );
    assert_eq!(r["error"]["code"], -32602);
    let r = c.request(
        "session/set_mode",
        json!({"sessionId": s, "modeId":"alpha"}),
    );
    assert_eq!(r["result"], json!({}));
}

#[test]
fn opencode_models_and_modes() {
    acp_models_and_modes("opencode");
}

#[test]
fn kilo_models_and_modes() {
    acp_models_and_modes("kilo");
}

fn acp_load_replays_history(persona: &'static str) {
    let sc = Scenario::builder()
        .session(SessionInfo {
            id: "ses_old".into(),
            title: Some("Old chat".into()),
            cwd: Some("/work".into()),
            history: vec![
                HistoryMessage {
                    role: "user".into(),
                    text: "question".into(),
                },
                HistoryMessage {
                    role: "assistant".into(),
                    text: "answer".into(),
                },
            ],
        })
        .build();
    let mut fake = Fake::spawn(&["acp"], Some(persona), &sc);
    fake.send(
        &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}),
    );
    fake.recv_until(|m| m["id"] == 1);
    fake.send(&json!({"jsonrpc":"2.0","id":2,"method":"session/list","params":{}}));
    let list = fake.recv_until(|m| m["id"] == 2).pop().unwrap();
    assert_eq!(list["result"]["sessions"][0]["sessionId"], "ses_old");
    assert_eq!(list["result"]["sessions"][0]["title"], "Old chat");
    fake.send(&json!({"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"ses_old","cwd":"/work","mcpServers":[]}}));
    let msgs = fake.recv_until(|m| m["id"] == 3);
    let kinds: Vec<&str> = msgs
        .iter()
        .filter_map(|m| m["params"]["update"]["sessionUpdate"].as_str())
        .collect();
    assert_eq!(kinds, ["user_message_chunk", "agent_message_chunk"]);
    assert_eq!(msgs[1]["params"]["update"]["content"]["text"], "answer");
    // An unknown id is the real CLIs' generic service failure.
    fake.send(&json!({"jsonrpc":"2.0","id":4,"method":"session/load","params":{"sessionId":"ses_missing","cwd":"/","mcpServers":[]}}));
    let err = fake.recv_until(|m| m["id"] == 4).pop().unwrap();
    assert_eq!(err["error"]["code"], -32603);
}

#[test]
fn opencode_load_replays_history() {
    acp_load_replays_history("opencode");
}

#[test]
fn kilo_load_replays_history() {
    acp_load_replays_history("kilo");
}

#[test]
fn codex_resume_known_and_unknown_thread() {
    let sc = Scenario::builder()
        .session(SessionInfo {
            id: "01a1ebc4-6d1c-7502-97a3-4dd474957024".into(),
            ..SessionInfo::default()
        })
        .build();
    let mut c = Client::start("codex", &sc);
    let r = c.request(
        "thread/resume",
        json!({"threadId":"01a1ebc4-6d1c-7502-97a3-4dd474957024","cwd":"/tmp"}),
    );
    assert_eq!(
        r["result"]["thread"]["id"],
        "01a1ebc4-6d1c-7502-97a3-4dd474957024"
    );
    let r = c.request(
        "thread/resume",
        json!({"threadId":"01a1ebc4-0000-7000-8000-000000000000","cwd":"/tmp"}),
    );
    assert_eq!(r["error"]["code"], -32600);
    let r = c.request("model/list", json!({}));
    assert!(r["result"]["data"].is_array());
}

#[test]
fn codex_collaboration_modes_from_scenario() {
    let sc = Scenario::builder()
        .mode("plan", "Plan")
        .mode("default", "Default")
        .mode("review", "Review")
        .build();
    let mut c = Client::start("codex", &sc);
    let s = c.session.clone();
    let r = c.request("collaborationMode/list", json!({"threadId": s}));
    let modes: Vec<&str> = r["result"]["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["mode"].as_str())
        .collect();
    assert_eq!(modes, ["plan", "default", "review"]);
}

// ---------------------------------------------------------------------------------------------
// Discovery commands, --version, persona selection
// ---------------------------------------------------------------------------------------------

fn run_cli(args: &[&str], persona: Option<&str>, scenario: Option<&Scenario>) -> (i32, String) {
    let dir = std::env::temp_dir().join(format!(
        "wasit-fake-disc-{}-{}",
        std::process::id(),
        wasit_fake_cli::util::hex(8)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let mut cmd = Command::new(common::BIN);
    cmd.args(args)
        .current_dir(&dir)
        .env_remove("WASIT_FAKE_PERSONA")
        .env_remove("WASIT_FAKE_SCENARIO");
    if let Some(p) = persona {
        cmd.env("WASIT_FAKE_PERSONA", p);
    }
    if let Some(s) = scenario {
        let p = dir.join("s.json");
        s.write_to(&p).unwrap();
        cmd.env("WASIT_FAKE_SCENARIO", p);
    }
    let out = cmd.output().expect("run");
    // Best-effort cleanup of the scratch dir.
    let _ = std::fs::remove_dir_all(&dir);
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

#[test]
fn version_flags() {
    let (code, out) = run_cli(&["--version"], Some("claude"), None);
    assert_eq!(code, 0);
    assert!(
        out.trim().ends_with("(Claude Code)") && out.starts_with("2."),
        "{out}"
    );
    let (_, out) = run_cli(&["--version"], Some("codex"), None);
    assert!(out.starts_with("codex-cli "), "{out}");
    let (_, out) = run_cli(&["--version"], Some("opencode"), None);
    assert_eq!(out.trim(), "1.19.6");
    let (_, out) = run_cli(&["--version"], Some("kilo"), None);
    assert_eq!(out.trim(), "7.8.27");
    let sc = Scenario::builder().version("9.9.9").build();
    let (_, out) = run_cli(&["--version"], Some("opencode"), Some(&sc));
    assert_eq!(out.trim(), "9.9.9");
}

#[test]
fn model_discovery_from_scenario() {
    let sc = Scenario::builder()
        .model("acme/fast", "Fast")
        .model("acme/slow", "Slow")
        .model("other/x", "X")
        .build();
    let (code, out) = run_cli(&["models"], Some("opencode"), Some(&sc));
    assert_eq!(code, 0);
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        ["acme/fast", "acme/slow", "other/x"]
    );
    let (_, out) = run_cli(&["models", "acme"], Some("kilo"), Some(&sc));
    assert_eq!(out.lines().collect::<Vec<_>>(), ["acme/fast", "acme/slow"]);
    let (_, out) = run_cli(&["models"], Some("opencode"), None);
    assert!(out.contains("opencode/big-pickle"));
}

#[test]
fn session_list_from_scenario() {
    let sc = Scenario::builder()
        .session(SessionInfo {
            id: "ses_a".into(),
            title: Some("First".into()),
            ..SessionInfo::default()
        })
        .build();
    let (_, out) = run_cli(&["session", "list"], Some("opencode"), Some(&sc));
    assert!(
        out.contains("Session ID") && out.contains("ses_a") && out.contains("First"),
        "{out}"
    );
    let (_, out) = run_cli(
        &["session", "list", "--format", "json"],
        Some("kilo"),
        Some(&sc),
    );
    let v: Value = serde_json::from_str(&out).expect("json");
    assert_eq!(v[0]["id"], "ses_a");
    assert_eq!(v[0]["title"], "First");
}

#[test]
fn unknown_persona_is_an_error() {
    let (code, _) = run_cli(&["--print"], None, None);
    assert_eq!(code, 2);
}

#[test]
fn persona_detected_from_argv_shape() {
    // No WASIT_FAKE_PERSONA: argv alone decides.
    for (argv, first_reply_key) in [
        (vec!["app-server", "--stdio"], "userAgent"),
        (vec!["acp"], "agentCapabilities"),
    ] {
        let dir = std::env::temp_dir().join(format!(
            "wasit-fake-shape-{}-{}",
            std::process::id(),
            wasit_fake_cli::util::hex(8)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut child = Command::new(common::BIN)
            .args(&argv)
            .env_remove("WASIT_FAKE_PERSONA")
            .env_remove("WASIT_FAKE_SCENARIO")
            .current_dir(&dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::{BufRead, Write};
        let init = if argv[0] == "acp" {
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#
        } else {
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"t","version":"0"}}}"#
        };
        let mut stdin = child.stdin.take().unwrap();
        writeln!(stdin, "{init}").unwrap();
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        assert!(
            v["result"][first_reply_key].is_object() || v["result"][first_reply_key].is_string(),
            "{line}"
        );
        drop(stdin);
        // Best-effort teardown of the child and scratch directory.
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn fake_cli_path_finds_the_built_binary() {
    // The test executable lives in target/<profile>/deps; the binary is built next to it.
    let p = wasit_fake_cli::fake_cli_path().expect("binary built next to the test executable");
    assert!(p.is_file());
    assert_eq!(
        p.file_stem().and_then(|s| s.to_str()),
        Some("wasit-fake-cli")
    );
}

#[test]
fn transcript_records_both_directions() {
    let dir = std::env::temp_dir().join(format!(
        "wasit-fake-tr-{}-{}",
        std::process::id(),
        wasit_fake_cli::util::hex(8)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let transcript = dir.join("t.jsonl");
    let mut cmd = Command::new(common::BIN);
    cmd.arg("acp")
        .env("WASIT_FAKE_PERSONA", "opencode")
        .env("WASIT_FAKE_TRANSCRIPT", &transcript)
        .env_remove("WASIT_FAKE_SCENARIO")
        .current_dir(&dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    use std::io::{BufRead, Write};
    let mut stdin = child.stdin.take().unwrap();
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":1}}}}"#
    )
    .unwrap();
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    drop(stdin);
    let _ = child.wait();
    let text = std::fs::read_to_string(&transcript).unwrap();
    let dirs: Vec<String> = text
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["dir"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(dirs, ["in", "out"]);
    // Best-effort cleanup of the scratch dir.
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn startup_steps_run_before_any_input_and_hang_ignores_the_handshake() {
    use wasit_fake_cli::scenario::Step;
    let sc = Scenario::builder()
        .startup(Step::Stderr {
            text: "booting".into(),
        })
        .startup(Step::Garbage {
            line: Some("not json at all".into()),
        })
        .startup(Step::Hang)
        .build();
    let mut fake = Fake::spawn(&CLAUDE_ARGV, Some("claude"), &sc);
    assert_eq!(
        fake.recv_raw(Duration::from_secs(10)).as_deref(),
        Some("not json at all")
    );
    fake.send(&json!({"type":"control_request","request_id":"init","request":{"subtype":"initialize","hooks":null}}));
    assert!(fake.recv_timeout(Duration::from_millis(300)).is_none());
    assert!(fake.stderr_to_end().contains("booting"));
    assert_eq!(fake.wait_exit().and_then(|s| s.code()), Some(0));
}

#[test]
fn codex_thread_settings_update_confirms_model_and_mode() {
    let mut c = Client::start("codex", &Scenario::default());
    let sid = c.session.clone();
    let r = c.request(
        "thread/settings/update",
        json!({"threadId": sid, "model": "m2"}),
    );
    assert!(r["error"].is_null(), "{r}");
    let n = c
        .fake
        .recv_until(|m| m["method"] == "thread/settings/updated")
        .pop()
        .unwrap();
    assert_eq!(n["params"]["threadSettings"]["model"], "m2");
    assert!(n["params"]["threadSettings"]["collaborationMode"].is_null());
    let r = c.request(
        "thread/settings/update",
        json!({"threadId": sid, "collaborationMode": {"mode": "plan", "settings": {"model": "m2"}}}),
    );
    assert!(r["error"].is_null(), "{r}");
    let n = c
        .fake
        .recv_until(|m| m["method"] == "thread/settings/updated")
        .pop()
        .unwrap();
    assert_eq!(
        n["params"]["threadSettings"]["collaborationMode"]["mode"],
        "plan"
    );
    let r = c.request(
        "thread/settings/update",
        json!({"threadId": "nope", "model": "x"}),
    );
    assert!(!r["error"].is_null());
}

// ---------------------------------------------------------------------------------------------
// codex `turn/steer` and refused prompts (shapes: reference/codex/steer.jsonl, codex-cli 0.161.0)
// ---------------------------------------------------------------------------------------------

fn steer(c: &mut Client, turn: &Value, text: &str) -> Value {
    c.request(
        "turn/steer",
        json!({"threadId": c.session.clone(), "expectedTurnId": turn,
               "input": [{"type":"text","text":text}]}),
    )
}

#[test]
fn codex_steered_prompt_joins_the_running_turn_and_is_answered_in_it() {
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working").sleep_ms(1000))
        .build();
    let mut c = Client::start("codex", &sc);
    c.prompt("go");
    let started = c
        .fake
        .recv_until(|m| m["method"] == "item/agentMessage/delta")
        .into_iter()
        .find(|m| m["method"] == "turn/started")
        .expect("turn/started");
    let turn = started["params"]["turn"]["id"].clone();
    // Wrong turn id: refused, as the real CLI words it.
    let r = steer(&mut c, &json!("01a20687-0000-7000-8000-000000000000"), "no");
    assert_eq!(r["error"]["code"], -32600);
    assert!(
        r["error"]["message"].as_str().unwrap().starts_with(
            "expected active turn id `01a20687-0000-7000-8000-000000000000` but found "
        ),
        "{r}"
    );
    let r = steer(&mut c, &turn, "steer me");
    assert_eq!(r["result"], json!({"turnId": turn}), "{r}");
    // The one turn ends with a reply to the steered text.
    let rest = c.fake.recv_until(|m| m["method"] == "turn/completed");
    let user_texts: Vec<&str> = rest
        .iter()
        .filter(|m| m["method"] == "item/started" && m["params"]["item"]["type"] == "userMessage")
        .filter_map(|m| m["params"]["item"]["content"][0]["text"].as_str())
        .collect();
    assert_eq!(user_texts, ["steer me"]);
    let done = rest.last().unwrap();
    assert_eq!(done["params"]["turn"]["status"], "completed");
    let items = done["params"]["turn"]["items"].as_array().unwrap();
    assert_eq!(items.last().unwrap()["text"], "echo: steer me");
    // The turn is over: a late steer is refused.
    let r = steer(&mut c, &turn, "too late");
    assert_eq!(r["error"]["message"], "no active turn to steer");
}

#[test]
fn codex_interrupted_turn_drops_steered_prompts() {
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working").wait_for_interrupt())
        .build();
    let mut c = Client::start("codex", &sc);
    c.prompt("go");
    let started = c
        .fake
        .recv_until(|m| m["method"] == "item/agentMessage/delta")
        .into_iter()
        .find(|m| m["method"] == "turn/started")
        .unwrap();
    let turn = started["params"]["turn"]["id"].clone();
    assert!(steer(&mut c, &turn, "steer me")["result"].is_object());
    c.request(
        "turn/interrupt",
        json!({"threadId": c.session.clone(), "turnId": turn}),
    );
    let rest = c.fake.recv_until(|m| m["method"] == "turn/completed");
    assert_eq!(
        rest.last().unwrap()["params"]["turn"]["status"],
        "interrupted"
    );
    assert!(!rest
        .iter()
        .any(|m| m["params"]["item"]["type"] == "userMessage"));
}

#[test]
fn codex_refuses_the_prompts_the_scenario_says_it_refuses() {
    let sc = Scenario::builder()
        .refuse("^refuse me", "no thread for you")
        .turn(Turn::builder().say("working").sleep_ms(500))
        .build();
    let mut c = Client::start("codex", &sc);
    // A refused `turn/start` is an error reply and starts nothing.
    c.prompt("refuse me now");
    let r = c
        .fake
        .recv_until(|m| m["id"] == c.prompt_id && m["method"].is_null())
        .pop()
        .unwrap();
    assert_eq!(r["error"]["message"], "no thread for you");
    assert!(c.fake.drain(Duration::from_millis(200)).is_empty());
    // A prompt that is not refused runs, and a refused steer leaves the turn alone.
    c.prompt("go");
    let started = c
        .fake
        .recv_until(|m| m["method"] == "item/agentMessage/delta")
        .into_iter()
        .find(|m| m["method"] == "turn/started")
        .unwrap();
    let turn = started["params"]["turn"]["id"].clone();
    let r = steer(&mut c, &turn, "refuse me too");
    assert_eq!(r["error"]["message"], "no thread for you");
    let rest = c.fake.recv_until(|m| m["method"] == "turn/completed");
    assert_eq!(
        rest.last().unwrap()["params"]["turn"]["status"],
        "completed"
    );
    assert!(!rest
        .iter()
        .any(|m| m["params"]["item"]["type"] == "userMessage"));
}
