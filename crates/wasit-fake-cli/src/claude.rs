//! The `claude` persona: `--print --input-format stream-json --output-format stream-json`
//! with `--permission-prompt-tool stdio` (control requests on the same pipe).

use crate::engine::{Incoming, Outcome, Persona, TurnStats};
use crate::io::Io;
use crate::reference::find_out;
use crate::scenario::{AskStep, PlanEntry, Scenario, ToolStep};
use crate::util::{alnum, chunk_text, hex, iso_now, set, unset, uuid};
use serde_json::{json, Value};

const PERMISSION_MODES: [&str; 6] = [
    "acceptEdits",
    "auto",
    "bypassPermissions",
    "default",
    "dontAsk",
    "plan",
];

/// Options parsed from claude's argv.
#[derive(Debug, Default, Clone)]
pub struct ClaudeArgs {
    pub model: Option<String>,
    pub permission_mode: Option<String>,
    pub resume: Option<String>,
    pub session_id: Option<String>,
    pub partial_messages: bool,
    pub mcp_config: Option<String>,
}

impl ClaudeArgs {
    pub fn parse(args: &[String]) -> ClaudeArgs {
        let mut a = ClaudeArgs::default();
        let mut i = 0;
        while i < args.len() {
            let next = args.get(i + 1).cloned();
            match args[i].as_str() {
                "--model" => a.model = next,
                "--permission-mode" => a.permission_mode = next,
                "--resume" | "-r" => a.resume = next,
                "--session-id" => a.session_id = next,
                "--mcp-config" => a.mcp_config = next,
                "--include-partial-messages" => a.partial_messages = true,
                _ => {
                    i += 1;
                    continue;
                }
            }
            i += 2;
        }
        a
    }
}

pub struct Claude {
    session_id: String,
    model: String,
    mode: String,
    cwd: String,
    version: String,
    partial: bool,
    init_sent: bool,
    scenario: Scenario,
    /// Pending gated tool call: (tool_use_id, request_id).
    tool_use_id: String,
    msg_id: String,
    mcp: std::collections::BTreeMap<String, crate::mcp::McpLaunch>,
}

fn resolve_model(m: &str) -> String {
    match m {
        "haiku" => "claude-haiku-5-5".into(),
        "sonnet" => "claude-sonnet-5-5".into(),
        "opus" => "claude-opus-5-5".into(),
        "default" => "claude-fable-5-2".into(),
        other => other.to_string(),
    }
}

fn first_where(name: &str, pred: impl Fn(&Value) -> bool) -> Value {
    find_out("claude", name, pred)
}

fn ty(m: &Value, t: &str) -> bool {
    m["type"] == t
}

impl Claude {
    pub fn new(args: &ClaudeArgs, scenario: &Scenario) -> Claude {
        let session_id = args
            .resume
            .clone()
            .or_else(|| args.session_id.clone())
            .or_else(|| scenario.session_id.clone())
            .unwrap_or_else(uuid);
        let model = args
            .model
            .clone()
            .or_else(|| scenario.current_model.clone())
            .or_else(|| scenario.models.first().map(|m| m.id.clone()))
            .map(|m| {
                if scenario.models.iter().any(|x| x.id == m) {
                    m
                } else {
                    resolve_model(&m)
                }
            })
            .unwrap_or_else(|| "claude-fake-1".into());
        Claude {
            session_id,
            model,
            mode: args
                .permission_mode
                .clone()
                .or_else(|| scenario.current_mode.clone())
                .unwrap_or_else(|| "default".into()),
            cwd: std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "/".into()),
            version: scenario
                .version
                .clone()
                .unwrap_or_else(|| "2.1.326".to_string()),
            partial: args.partial_messages,
            init_sent: false,
            scenario: scenario.clone(),
            tool_use_id: String::new(),
            msg_id: String::new(),
            mcp: args
                .mcp_config
                .as_deref()
                .map(crate::mcp::servers_from_config)
                .unwrap_or_default(),
        }
    }

    fn envelope(&self, mut m: Value) -> Value {
        set(&mut m, "session_id", json!(self.session_id));
        set(&mut m, "uuid", json!(uuid()));
        m
    }

    fn control_ok(&self, io: &mut Io, request_id: &Value, response: Value) {
        io.send(&json!({"type":"control_response","response":{
            "subtype":"success","request_id":request_id,"response":response}}));
    }

    fn control_err(&self, io: &mut Io, request_id: &Value, error: &str, code: Option<&str>) {
        let mut r = json!({"subtype":"error","request_id":request_id,"error":error});
        if let Some(c) = code {
            r["error_code"] = json!(c);
        }
        io.send(&json!({"type":"control_response","response":r}));
    }

    fn initialize_response(&self, request_id: &Value) -> Value {
        let mut full = first_where("simple", |m| {
            ty(m, "control_response") && m["response"]["response"]["commands"].is_array()
        });
        full["response"]["request_id"] = request_id.clone();
        let mut resp = full["response"]["response"].clone();
        if !self.scenario.commands.is_empty() {
            resp["commands"] = Value::Array(
                self.scenario
                    .commands
                    .iter()
                    .map(|c| {
                        json!({"name": c.name, "description": c.description,
                               "argumentHint": c.input_hint.clone().unwrap_or_default()})
                    })
                    .collect(),
            );
        }
        if !self.scenario.models.is_empty() {
            let proto = resp["models"][0].clone();
            resp["models"] = Value::Array(
                self.scenario
                    .models
                    .iter()
                    .map(|m| {
                        let mut e = proto.clone();
                        e["value"] = json!(m.id);
                        e["resolvedModel"] = json!(m.id);
                        e["displayName"] = json!(m.name.clone().unwrap_or_else(|| m.id.clone()));
                        e["description"] = json!(m.description.clone().unwrap_or_default());
                        e
                    })
                    .collect(),
            );
        }
        full["response"]["response"] = resp;
        full
    }

    fn send_system_init(&mut self, io: &mut Io) {
        if self.init_sent {
            return;
        }
        self.init_sent = true;
        let mut m = first_where("tool_allow", |m| ty(m, "system") && m["subtype"] == "init");
        set(&mut m, "cwd", json!(self.cwd));
        set(&mut m, "original_cwd", json!(self.cwd));
        set(&mut m, "model", json!(self.model));
        set(&mut m, "permissionMode", json!(self.mode));
        set(&mut m, "claude_code_version", json!(self.version));
        set(
            &mut m,
            "scratchpad_path",
            json!(format!("/tmp/claude-fake/{}/scratchpad", self.session_id)),
        );
        if !self.scenario.commands.is_empty() {
            let names: Vec<Value> = self
                .scenario
                .commands
                .iter()
                .map(|c| json!(c.name))
                .collect();
            m["slash_commands"] = Value::Array(names.clone());
            m["skills"] = Value::Array(names);
        }
        let m = self.envelope(m);
        io.send(&m);
    }

    fn assistant(&mut self, content: Value, stats: &TurnStats) -> Value {
        let mut m = first_where("tool_allow", |m| {
            ty(m, "assistant") && m["message"]["content"][0]["type"] == "text"
        });
        set(&mut m, "message.id", json!(self.msg_id));
        set(&mut m, "message.model", json!(self.model));
        set(&mut m, "message.content", content);
        set(
            &mut m,
            "message.usage.input_tokens",
            json!(stats.usage.input_tokens),
        );
        set(
            &mut m,
            "message.usage.output_tokens",
            json!(stats.usage.output_tokens),
        );
        set(&mut m, "timestamp", json!(iso_now()));
        set(&mut m, "request_id", json!(format!("req_{}", alnum(24))));
        self.envelope(m)
    }

    fn stream_event(&self, event: Value) -> Value {
        let mut m = first_where("partial_messages", |m| {
            ty(m, "stream_event") && m["event"]["type"] == "content_block_start"
        });
        m["event"] = event;
        set(&mut m, "api_message_id", json!(self.msg_id));
        self.envelope(m)
    }

    /// Emit one assistant message holding a single text-like block, with partial stream events
    /// when `--include-partial-messages` was passed.
    fn block(&mut self, io: &mut Io, kind: &str, text: &str, chunks: usize, stats: &TurnStats) {
        self.msg_id = format!("msg_{}", alnum(24));
        let (start_block, delta_type, key, content) = match kind {
            "thinking" => (
                json!({"type":"thinking","thinking":"","signature":""}),
                "thinking_delta",
                "thinking",
                json!([{"type":"thinking","thinking":text,"signature":format!("sig-{}", hex(16))}]),
            ),
            _ => (
                json!({"type":"text","text":""}),
                "text_delta",
                "text",
                json!([{"type":"text","text":text}]),
            ),
        };
        if self.partial {
            let mut start = first_where("partial_messages", |m| {
                ty(m, "stream_event") && m["event"]["type"] == "message_start"
            });
            set(&mut start, "event.message.id", json!(self.msg_id));
            set(&mut start, "event.message.model", json!(self.model));
            set(&mut start, "api_message_id", json!(self.msg_id));
            let start = self.envelope(start);
            io.send(&start);
            let e = self.stream_event(
                json!({"type":"content_block_start","index":0,"content_block":start_block}),
            );
            io.send(&e);
            for c in chunk_text(text, chunks) {
                let e = self.stream_event(
                    json!({"type":"content_block_delta","index":0,"delta":{"type":delta_type, key: c}}),
                );
                io.send(&e);
            }
        }
        let mut a = self.assistant(content, stats);
        if kind == "thinking" {
            set(
                &mut a,
                "thinking_duration_ms",
                json!(stats.usage.output_tokens * 10),
            );
            set(&mut a, "thinking_display", json!("updates"));
        }
        io.send(&a);
        if self.partial {
            let e = self.stream_event(json!({"type":"content_block_stop","index":0}));
            io.send(&e);
            let mut d = first_where("partial_messages", |m| {
                ty(m, "stream_event") && m["event"]["type"] == "message_delta"
            });
            set(&mut d, "api_message_id", json!(self.msg_id));
            set(
                &mut d,
                "event.usage.output_tokens",
                json!(stats.usage.output_tokens),
            );
            let d = self.envelope(d);
            io.send(&d);
            let e = self.stream_event(json!({"type":"message_stop"}));
            io.send(&e);
        }
    }

    fn tool_use_message(&mut self, io: &mut Io, name: &str, input: &Value, stats: &TurnStats) {
        self.tool_use_id = format!("toolu_{}", alnum(24));
        self.msg_id = format!("msg_{}", alnum(24));
        let mut m = first_where("tool_allow", |m| {
            ty(m, "assistant") && m["message"]["content"][0]["type"] == "tool_use"
        });
        set(&mut m, "message.id", json!(self.msg_id));
        set(&mut m, "message.model", json!(self.model));
        set(
            &mut m,
            "message.content",
            json!([{"type":"tool_use","id":self.tool_use_id,"name":name,"input":input,
                    "caller":{"type":"direct"}}]),
        );
        set(
            &mut m,
            "message.usage.output_tokens",
            json!(stats.usage.output_tokens),
        );
        set(
            &mut m,
            "wire_tool_inputs",
            json!({ self.tool_use_id.clone(): input }),
        );
        set(&mut m, "timestamp", json!(iso_now()));
        set(&mut m, "request_id", json!(format!("req_{}", alnum(24))));
        let m = self.envelope(m);
        io.send(&m);
    }

    fn tool_result_message(
        &self,
        template: &str,
        content: Value,
        extra: Vec<(&str, Value)>,
    ) -> Value {
        let mut m = first_where(template, |m| {
            ty(m, "user") && m["message"]["content"][0]["type"] == "tool_result"
        });
        set(&mut m, "message.content", content);
        set(&mut m, "timestamp", json!(iso_now()));
        for (k, v) in extra {
            set(&mut m, k, v);
        }
        self.envelope(m)
    }

    fn task_summary(&self, io: &mut Io, detail: &str) {
        let mut t = first_where("tool_allow", |m| {
            ty(m, "system") && m["subtype"] == "task_summary" && m["detail"].is_string()
        });
        t["detail"] = json!(detail);
        let t = self.envelope(t);
        io.send(&t);
    }

    fn describe(tool: &ToolStep) -> String {
        tool.title.clone().unwrap_or_else(|| {
            tool.input["description"]
                .as_str()
                .or_else(|| tool.input["command"].as_str())
                .unwrap_or(&tool.name)
                .to_string()
        })
    }

    fn result_message(&self, outcome: &Outcome, stats: &TurnStats) -> Value {
        let elapsed = stats
            .started
            .map(|s| s.elapsed().as_millis() as u64)
            .unwrap_or(0);
        let template = match outcome {
            Outcome::Completed => "tool_allow",
            Outcome::Interrupted => "interrupt",
            Outcome::Failed(_) => "turn_error",
        };
        let mut m = first_where(template, |m| ty(m, "result"));
        let model_usage_proto = m["modelUsage"]
            .as_object()
            .and_then(|o| o.values().next().cloned());
        match outcome {
            Outcome::Completed => {
                set(&mut m, "result", json!(stats.final_text));
                set(&mut m, "num_turns", json!(1 + stats.tool_calls));
                set(
                    &mut m,
                    "usage.input_tokens",
                    json!(stats.usage.input_tokens),
                );
                set(
                    &mut m,
                    "usage.output_tokens",
                    json!(stats.usage.output_tokens),
                );
                set(
                    &mut m,
                    "usage.cache_read_input_tokens",
                    json!(stats.usage.cached_tokens),
                );
                set(&mut m, "total_cost_usd", json!(stats.usage.cost_usd));
                set(
                    &mut m,
                    "permission_denials",
                    Value::Array(
                        stats
                            .denied
                            .iter()
                            .map(|(n, id, input)| {
                                json!({"tool_name":n,"tool_use_id":id,"tool_input":input})
                            })
                            .collect(),
                    ),
                );
            }
            Outcome::Interrupted => {}
            Outcome::Failed(msg) => {
                set(&mut m, "result", json!(msg));
            }
        }
        if let Some(mut proto) = model_usage_proto {
            if matches!(outcome, Outcome::Completed) {
                proto["inputTokens"] = json!(stats.usage.input_tokens);
                proto["outputTokens"] = json!(stats.usage.output_tokens);
                proto["costUSD"] = json!(stats.usage.cost_usd);
                m["modelUsage"] = json!({ self.model.clone(): proto });
            }
        }
        set(&mut m, "duration_ms", json!(elapsed));
        set(&mut m, "duration_api_ms", json!(elapsed));
        self.envelope(m)
    }
}

impl Persona for Claude {
    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return Incoming::Handled;
        };
        match v["type"].as_str() {
            Some("control_request") => {
                let id = v["request_id"].clone();
                let req = &v["request"];
                match req["subtype"].as_str().unwrap_or("") {
                    "initialize" => {
                        for t in ["active_goal", "autocompact_state"] {
                            let m = first_where("simple", |m| ty(m, t));
                            let m = self.envelope(m);
                            io.send(&m);
                        }
                        let r = self.initialize_response(&id);
                        io.send(&r);
                        Incoming::Handled
                    }
                    "interrupt" => {
                        self.control_ok(io, &id, json!({"still_queued": []}));
                        if in_turn {
                            Incoming::Interrupt
                        } else {
                            Incoming::Handled
                        }
                    }
                    "set_permission_mode" => {
                        let mode = req["mode"].as_str().unwrap_or("");
                        if PERMISSION_MODES.contains(&mode) {
                            self.mode = mode.to_string();
                            self.control_ok(io, &id, json!({"mode": mode}));
                            let mut s = first_where("set_mode", |m| {
                                ty(m, "system") && m["subtype"] == "status"
                            });
                            set(&mut s, "permissionMode", json!(mode));
                            let s = self.envelope(s);
                            io.send(&s);
                        } else {
                            self.control_err(
                                io,
                                &id,
                                "Cannot set permission mode: must be one of acceptEdits, auto, bypassPermissions, default, dontAsk, plan",
                                Some("invalid_mode"),
                            );
                        }
                        Incoming::Handled
                    }
                    "set_model" => {
                        let m = req["model"].as_str().unwrap_or("default");
                        self.model = if self.scenario.models.iter().any(|x| x.id == m) {
                            m.to_string()
                        } else {
                            resolve_model(m)
                        };
                        let model = self.model.clone();
                        self.control_ok(io, &id, json!({"model": model}));
                        Incoming::Handled
                    }
                    "" => {
                        self.control_err(io, &id, "Missing request subtype", None);
                        Incoming::Handled
                    }
                    _ => {
                        self.control_ok(io, &id, json!({}));
                        Incoming::Handled
                    }
                }
            }
            Some("control_response") => {
                let r = &v["response"];
                let body = if r["subtype"] == "error" {
                    json!({"behavior":"deny","message": r["error"]})
                } else {
                    r["response"].clone()
                };
                Incoming::Response {
                    id: r["request_id"].clone(),
                    body,
                }
            }
            Some("user") => {
                let c = &v["message"]["content"];
                let text = match c {
                    Value::String(s) => s.clone(),
                    Value::Array(blocks) => blocks
                        .iter()
                        .filter(|b| b["type"] == "text")
                        .filter_map(|b| b["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    _ => String::new(),
                };
                Incoming::Prompt(text)
            }
            _ => Incoming::Handled,
        }
    }

    fn turn_begin(&mut self, io: &mut Io, _prompt: &str, _stats: &mut TurnStats) {
        self.send_system_init(io);
        if self.partial {
            let mut s = first_where("partial_messages", |m| {
                ty(m, "system") && m["subtype"] == "status"
            });
            set(&mut s, "status", json!("requesting"));
            let s = self.envelope(s);
            io.send(&s);
        }
    }

    fn say(&mut self, io: &mut Io, text: &str, chunks: usize, stats: &mut TurnStats) {
        self.block(io, "text", text, chunks, stats);
    }

    fn think(&mut self, io: &mut Io, text: &str, stats: &mut TurnStats) {
        let tokens = text.split_whitespace().count().max(1) as u64;
        let mut t = first_where("tool_deny", |m| {
            ty(m, "system") && m["subtype"] == "thinking_tokens"
        });
        set(&mut t, "estimated_tokens", json!(tokens));
        set(&mut t, "estimated_tokens_delta", json!(tokens));
        let t = self.envelope(t);
        io.send(&t);
        self.block(io, "thinking", text, 1, stats);
    }

    fn tool_call(&mut self, io: &mut Io, tool: &ToolStep, stats: &mut TurnStats) -> Option<Value> {
        self.tool_use_message(io, &tool.name, &tool.input, stats);
        if tool.needs_permission {
            let id = json!(uuid());
            let desc = Self::describe(tool);
            let mut req = first_where("tool_allow", |m| ty(m, "control_request"));
            set(&mut req, "request_id", id.clone());
            set(&mut req, "request.tool_name", json!(tool.name));
            set(&mut req, "request.display_name", json!(tool.name));
            set(&mut req, "request.input", tool.input.clone());
            set(&mut req, "request.description", json!(desc));
            set(&mut req, "request.tool_use_id", json!(self.tool_use_id));
            unset(&mut req, "request.blocked_path");
            io.send(&req);
            return Some(id);
        }
        self.task_summary(io, &Self::describe(tool));
        let content = tool
            .result_if_allowed
            .clone()
            .unwrap_or_else(|| "ok".to_string());
        let m = self.tool_result_message(
            "tool_allow",
            json!([{"tool_use_id":self.tool_use_id,"type":"tool_result","content":content,"is_error":false}]),
            vec![
                (
                    "tool_use_result",
                    json!({"stdout":content,"stderr":"","interrupted":false,"isImage":false,"noOutputExpected":false}),
                ),
                ("tool_result_meta", json!([])),
            ],
        );
        io.send(&m);
        None
    }

    fn tool_answered(&mut self, io: &mut Io, tool: &ToolStep, body: &Value, stats: &mut TurnStats) {
        let tuid = self.tool_use_id.clone();
        self.task_summary(io, &Self::describe(tool));
        if body["behavior"] == "allow" {
            let content = tool
                .result_if_allowed
                .clone()
                .unwrap_or_else(|| "ok".to_string());
            let m = self.tool_result_message(
                "tool_allow",
                json!([{"tool_use_id":tuid,"type":"tool_result","content":content,"is_error":false}]),
                vec![
                    (
                        "tool_use_result",
                        json!({"stdout":content,"stderr":"","interrupted":false,"isImage":false,"noOutputExpected":false}),
                    ),
                    (
                        "tool_result_meta",
                        json!([{"id":tuid,"permission_decision":{"decision":"accept","source":"user_temporary"}}]),
                    ),
                ],
            );
            io.send(&m);
        } else {
            let msg = body["message"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or_else(|| tool.result_if_denied.clone())
                .unwrap_or_else(|| "Permission denied".into());
            let m = self.tool_result_message(
                "tool_deny",
                json!([{"type":"tool_result","content":msg,"is_error":true,"tool_use_id":tuid}]),
                vec![
                    ("tool_use_result", json!(format!("Error: {msg}"))),
                    (
                        "tool_result_meta",
                        json!([{"id":tuid,"non_execution_kind":"permission-rule",
                                "permission_decision":{"decision":"reject","source":"user_reject"}}]),
                    ),
                ],
            );
            io.send(&m);
            stats
                .denied
                .push((tool.name.clone(), tuid, tool.input.clone()));
        }
    }

    fn question(&mut self, io: &mut Io, ask: &AskStep, stats: &mut TurnStats) -> Value {
        let questions = json!([{
            "question": ask.question,
            "header": ask.header.clone().unwrap_or_default(),
            "options": ask.options.iter().map(|o| json!({"label":o.label,"description":o.description})).collect::<Vec<_>>(),
            "multiSelect": ask.multi_select,
        }]);
        self.tool_use_message(
            io,
            "AskUserQuestion",
            &json!({"questions": questions}),
            stats,
        );
        let id = json!(uuid());
        let mut req = first_where("ask_question", |m| ty(m, "control_request"));
        set(&mut req, "request_id", id.clone());
        set(&mut req, "request.input", json!({"questions": questions}));
        set(&mut req, "request.tool_use_id", json!(self.tool_use_id));
        io.send(&req);
        id
    }

    fn question_answered(
        &mut self,
        io: &mut Io,
        ask: &AskStep,
        body: &Value,
        _stats: &mut TurnStats,
    ) {
        self.task_summary(io, &ask.question);
        let answers = body["updatedInput"]["answers"].clone();
        let summary = answers
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(q, a)| format!("\"{q}\"=\"{}\"", a.as_str().unwrap_or("")))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let tuid = self.tool_use_id.clone();
        let m = self.tool_result_message(
            "ask_question",
            json!([{"type":"tool_result","content":format!("Your questions have been answered: {summary}. You can now continue with these answers in mind."),"tool_use_id":tuid}]),
            vec![(
                "tool_use_result",
                json!({"questions": body["updatedInput"]["questions"], "answers": answers}),
            )],
        );
        io.send(&m);
    }

    fn plan(
        &mut self,
        io: &mut Io,
        entries: &[PlanEntry],
        needs_approval: bool,
        stats: &mut TurnStats,
    ) -> Option<Value> {
        if needs_approval {
            let plan = entries
                .iter()
                .map(|e| format!("- {}", e.content))
                .collect::<Vec<_>>()
                .join("\n");
            self.tool_use_message(io, "ExitPlanMode", &json!({"plan": plan}), stats);
            let id = json!(uuid());
            let mut req = first_where("ask_question", |m| ty(m, "control_request"));
            set(&mut req, "request_id", id.clone());
            set(&mut req, "request.tool_name", json!("ExitPlanMode"));
            set(&mut req, "request.display_name", json!("ExitPlanMode"));
            set(&mut req, "request.input", json!({"plan": plan}));
            set(&mut req, "request.tool_use_id", json!(self.tool_use_id));
            io.send(&req);
            return Some(id);
        }
        let todos: Vec<Value> = entries
            .iter()
            .map(|e| json!({"content":e.content,"status":e.status,"activeForm":e.content}))
            .collect();
        self.tool_use_message(io, "TodoWrite", &json!({"todos": todos}), stats);
        let tuid = self.tool_use_id.clone();
        let m = self.tool_result_message(
            "tool_allow",
            json!([{"tool_use_id":tuid,"type":"tool_result","content":"Todos have been modified successfully.","is_error":false}]),
            vec![("tool_use_result", json!({"newTodos": todos})), ("tool_result_meta", json!([]))],
        );
        io.send(&m);
        None
    }

    fn plan_answered(&mut self, io: &mut Io, body: &Value, _stats: &mut TurnStats) {
        let tuid = self.tool_use_id.clone();
        let approved = body["behavior"] == "allow";
        let m = self.tool_result_message(
            "tool_allow",
            json!([{"tool_use_id":tuid,"type":"tool_result",
                    "content": if approved {"User has approved your plan."} else {"User rejected the plan."},
                    "is_error": !approved}]),
            vec![("tool_result_meta", json!([]))],
        );
        io.send(&m);
    }

    fn mcp_server(&self, name: &str) -> Option<crate::mcp::McpLaunch> {
        self.mcp.get(name).cloned()
    }

    fn turn_end(&mut self, io: &mut Io, outcome: &Outcome, stats: &mut TurnStats) {
        match outcome {
            Outcome::Completed => {
                let mut s = first_where("tool_allow", |m| {
                    ty(m, "system") && m["subtype"] == "post_turn_summary"
                });
                set(&mut s, "summarizes_uuid", json!(uuid()));
                set(&mut s, "status_detail", json!("completed"));
                let s = self.envelope(s);
                io.send(&s);
            }
            Outcome::Interrupted => {
                let mut m = first_where("interrupt", |m| ty(m, "user"));
                set(
                    &mut m,
                    "message.content",
                    json!([{"type":"text","text":"[Request interrupted by user]"}]),
                );
                set(&mut m, "timestamp", json!(iso_now()));
                let m = self.envelope(m);
                io.send(&m);
            }
            Outcome::Failed(msg) => {
                let mut a = first_where("turn_error", |m| ty(m, "assistant"));
                set(
                    &mut a,
                    "message.content",
                    json!([{"type":"text","text":msg}]),
                );
                set(&mut a, "message.id", json!(uuid()));
                let a = self.envelope(a);
                io.send(&a);
            }
        }
        let r = self.result_message(outcome, stats);
        io.send(&r);
        if stats.tool_calls > 0 {
            let mut t = first_where("tool_allow", |m| {
                ty(m, "system") && m["subtype"] == "task_summary" && m["detail"].is_null()
            });
            t["detail"] = Value::Null;
            let t = self.envelope(t);
            io.send(&t);
        }
    }
}
