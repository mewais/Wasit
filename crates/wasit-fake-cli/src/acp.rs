//! The `opencode acp` / `kilo acp` personas: Agent Client Protocol over newline-delimited
//! JSON-RPC 2.0. Both CLIs share the implementation; they differ in identity, default model and
//! the modes they advertise.

use crate::engine::{Incoming, Outcome, Persona, TurnStats};
use crate::io::Io;
use crate::reference::find_out;
use crate::scenario::{AskStep, HistoryMessage, PlanEntry, Scenario, ToolStep};
use crate::util::{alnum, hex, iso_now, set};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Opencode,
    Kilo,
}

impl Flavor {
    fn persona(self) -> &'static str {
        match self {
            Flavor::Opencode => "opencode",
            Flavor::Kilo => "kilo",
        }
    }
}

struct Sess {
    id: String,
    cwd: String,
    title: String,
    model: String,
    mode: String,
    history: Vec<HistoryMessage>,
}

pub struct Acp {
    flavor: Flavor,
    scenario: Scenario,
    sessions: Vec<Sess>,
    first_id: Option<String>,
    next_request_id: u64,
    // Current turn.
    session: String,
    prompt_id: Value,
    call_id: String,
    call_title: String,
    call_kind: &'static str,
    call_input: Value,
    msg_id: String,
}

fn rec(flavor: Flavor, name: &str, pred: impl Fn(&Value) -> bool) -> Value {
    find_out(flavor.persona(), name, pred)
}

fn kind_of(tool: &str) -> &'static str {
    match tool.to_ascii_lowercase().as_str() {
        "bash" | "shell" | "exec_command" => "execute",
        "read" => "read",
        "edit" | "write" | "patch" | "apply_patch" | "multiedit" => "edit",
        "grep" | "glob" | "list" | "search" => "search",
        "webfetch" | "fetch" => "fetch",
        "think" => "think",
        _ => "other",
    }
}

impl Acp {
    pub fn new(flavor: Flavor, scenario: &Scenario) -> Acp {
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "/".into());
        let mut a = Acp {
            flavor,
            scenario: scenario.clone(),
            sessions: vec![],
            first_id: scenario.session_id.clone(),
            next_request_id: 0,
            session: String::new(),
            prompt_id: Value::Null,
            call_id: String::new(),
            call_title: String::new(),
            call_kind: "other",
            call_input: Value::Null,
            msg_id: String::new(),
        };
        let (model, mode) = (a.default_model(), a.default_mode());
        a.sessions = scenario
            .sessions
            .iter()
            .map(|s| Sess {
                id: s.id.clone(),
                cwd: s.cwd.clone().unwrap_or_else(|| cwd.clone()),
                title: s.title.clone().unwrap_or_else(|| "New session".into()),
                model: model.clone(),
                mode: mode.clone(),
                history: s.history.clone(),
            })
            .collect();
        a
    }

    fn models(&self) -> Vec<(String, String)> {
        if !self.scenario.models.is_empty() {
            return self
                .scenario
                .models
                .iter()
                .map(|m| (m.id.clone(), m.name.clone().unwrap_or_else(|| m.id.clone())))
                .collect();
        }
        match self.flavor {
            Flavor::Opencode => vec![
                (
                    "opencode/big-pickle".into(),
                    "OpenCode Zen/Big Pickle".into(),
                ),
                (
                    "opencode/deepseek-v4.1-flash-free".into(),
                    "OpenCode Zen/DeepSeek V4.1 Flash Free".into(),
                ),
            ],
            Flavor::Kilo => vec![(
                "kilo/kilo-auto/free".into(),
                "Kilo Gateway/Kilo Auto Free".into(),
            )],
        }
    }

    fn modes(&self) -> Vec<(String, String, String)> {
        if !self.scenario.modes.is_empty() {
            return self
                .scenario
                .modes
                .iter()
                .map(|m| {
                    (
                        m.id.clone(),
                        m.name.clone().unwrap_or_else(|| m.id.clone()),
                        m.description.clone().unwrap_or_default(),
                    )
                })
                .collect();
        }
        let d = |id: &str, desc: &str| (id.to_string(), id.to_string(), desc.to_string());
        match self.flavor {
            Flavor::Opencode => vec![
                d("build", "The default agent. Executes tools based on configured permissions."),
                d("plan", "Plan mode. Disallows all edit tools."),
            ],
            Flavor::Kilo => vec![
                d("code", "The default agent. Executes tools based on configured permissions."),
                d("ask", "Get answers and explanations without making changes to the codebase."),
                d("debug", "Diagnose and fix software issues with systematic debugging methodology."),
                d("orchestrator", "Coordinate complex tasks by delegating to specialized agents in parallel."),
                d("plan", "Plan mode. Can only edit plan files; all other filesystem mutations are denied."),
            ],
        }
    }

    fn default_model(&self) -> String {
        self.scenario
            .current_model
            .clone()
            .unwrap_or_else(|| self.models()[0].0.clone())
    }

    fn default_mode(&self) -> String {
        self.scenario
            .current_mode
            .clone()
            .unwrap_or_else(|| self.modes()[0].0.clone())
    }

    fn config_options(&self, s: &Sess) -> Value {
        let model = json!({"id":"model","name":"Model","category":"model","type":"select",
            "currentValue": s.model,
            "options": self.models().iter().map(|(v,n)| json!({"value":v,"name":n})).collect::<Vec<_>>()});
        let mode = json!({"id":"mode","name":"Session Mode","category":"mode","type":"select",
            "currentValue": s.mode,
            "options": self.modes().iter().map(|(v,n,d)| json!({"value":v,"name":n,"description":d})).collect::<Vec<_>>()});
        json!([model, mode])
    }

    fn rpc_result(&self, io: &mut Io, id: &Value, result: Value) {
        io.send(&json!({"jsonrpc":"2.0","id":id,"result":result}));
    }

    fn rpc_error(&self, io: &mut Io, id: &Value, code: i64, message: &str, data: Value) {
        io.send(
            &json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message,"data":data}}),
        );
    }

    fn update(&self, io: &mut Io, sid: &str, update: Value) {
        io.send(&json!({"jsonrpc":"2.0","method":"session/update",
                        "params":{"sessionId":sid,"update":update}}));
    }

    fn commands_update(&self, io: &mut Io, sid: &str) {
        let cmds: Vec<Value> = self
            .scenario
            .commands
            .iter()
            .map(|c| json!({"name":c.name,"description":c.description}))
            .collect();
        self.update(
            io,
            sid,
            json!({"sessionUpdate":"available_commands_update","availableCommands":cmds}),
        );
    }

    fn session_not_found(&self, io: &mut Io, id: &Value, sid: &str) {
        self.rpc_error(
            io,
            id,
            -32602,
            &format!("Invalid params: session not found: {sid}"),
            json!({"sessionId": sid}),
        );
    }

    fn new_session(&mut self, cwd: &str) -> usize {
        let id = self
            .first_id
            .take()
            .unwrap_or_else(|| format!("ses_{}", alnum(26)));
        self.sessions.push(Sess {
            id,
            cwd: cwd.to_string(),
            title: format!("New session - {}", iso_now()),
            model: self.default_model(),
            mode: self.default_mode(),
            history: vec![],
        });
        self.sessions.len() - 1
    }

    fn idx(&self, sid: &str) -> Option<usize> {
        self.sessions.iter().position(|s| s.id == sid)
    }

    fn service_failure(&self, io: &mut Io, id: &Value) {
        self.rpc_error(
            io,
            id,
            -32603,
            "Internal error: OpenCode service failure",
            json!({"service":"session"}),
        );
    }

    fn handle_request(&mut self, io: &mut Io, v: &Value) -> Incoming {
        let id = v["id"].clone();
        let p = &v["params"];
        let method = v["method"].as_str().unwrap_or("");
        let sid = p["sessionId"].as_str().unwrap_or("").to_string();
        match method {
            "initialize" => {
                let mut r = rec(self.flavor, "handshake", |m| {
                    m["id"] == 1 && m["result"].is_object()
                })["result"]
                    .clone();
                if let Some(ver) = &self.scenario.version {
                    set(&mut r, "agentInfo.version", json!(ver));
                }
                self.rpc_result(io, &id, r);
            }
            "authenticate" => {
                let ok = rec(self.flavor, "handshake", |m| {
                    m["id"] == 1 && m["result"].is_object()
                })["result"]["authMethods"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|m| m["id"] == p["methodId"]));
                if ok {
                    self.rpc_result(io, &id, json!({}));
                } else {
                    self.rpc_error(
                        io,
                        &id,
                        -32602,
                        &format!(
                            "Invalid params: unknown auth method: {}",
                            p["methodId"].as_str().unwrap_or("")
                        ),
                        json!({"methodId": p["methodId"]}),
                    );
                }
            }
            "session/new" => {
                let i = self.new_session(p["cwd"].as_str().unwrap_or("/"));
                let sid = self.sessions[i].id.clone();
                let r = json!({"sessionId": sid, "configOptions": self.config_options(&self.sessions[i])});
                self.rpc_result(io, &id, r);
                self.commands_update(io, &sid);
            }
            "session/list" => {
                let list: Vec<Value> = self
                    .sessions
                    .iter()
                    .map(|s| json!({"sessionId":s.id,"cwd":s.cwd,"title":s.title,"updatedAt":iso_now()}))
                    .collect();
                self.rpc_result(io, &id, json!({"sessions": list}));
            }
            "session/load" | "session/resume" => match self.idx(&sid) {
                None => self.service_failure(io, &id),
                Some(i) => {
                    if let Some(c) = p["cwd"].as_str() {
                        self.sessions[i].cwd = c.to_string();
                    }
                    if method == "session/load" {
                        for m in self.sessions[i].history.clone() {
                            let (kind, mid) = if m.role == "user" {
                                ("user_message_chunk", format!("msg_{}", hex(26)))
                            } else {
                                ("agent_message_chunk", format!("msg_{}", hex(26)))
                            };
                            self.update(
                                io,
                                &sid,
                                json!({"sessionUpdate":kind,"messageId":mid,
                                "content":{"type":"text","text":m.text}}),
                            );
                        }
                    }
                    let r = json!({"configOptions": self.config_options(&self.sessions[i])});
                    self.rpc_result(io, &id, r);
                    self.commands_update(io, &sid);
                }
            },
            "session/fork" => match self.idx(&sid) {
                None => self.service_failure(io, &id),
                Some(i) => {
                    let n = self.new_session(p["cwd"].as_str().unwrap_or("/"));
                    let (model, mode, history) = {
                        let src = &self.sessions[i];
                        (src.model.clone(), src.mode.clone(), src.history.clone())
                    };
                    self.sessions[n].model = model;
                    self.sessions[n].mode = mode;
                    self.sessions[n].history = history;
                    let nid = self.sessions[n].id.clone();
                    let r = json!({"sessionId": nid, "configOptions": self.config_options(&self.sessions[n])});
                    self.rpc_result(io, &id, r);
                    self.commands_update(io, &nid);
                }
            },
            "session/close" => match self.idx(&sid) {
                None => self.session_not_found(io, &id, &sid),
                Some(i) => {
                    self.sessions.remove(i);
                    self.rpc_result(io, &id, json!({}));
                }
            },
            "session/set_config_option" => match self.idx(&sid) {
                None => self.session_not_found(io, &id, &sid),
                Some(i) => {
                    let value = p["value"].as_str().unwrap_or("").to_string();
                    match p["configId"].as_str().unwrap_or("") {
                        "model" => {
                            if self.models().iter().any(|(m, _)| *m == value) {
                                self.sessions[i].model = value;
                                let opts = self.config_options(&self.sessions[i]);
                                if self.flavor == Flavor::Opencode {
                                    self.update(io, &sid, json!({"sessionUpdate":"config_option_update","configOptions":opts.clone()}));
                                }
                                self.rpc_result(io, &id, json!({"configOptions": opts}));
                            } else {
                                let provider = value.split('/').next().unwrap_or("").to_string();
                                self.rpc_error(
                                    io,
                                    &id,
                                    -32602,
                                    &format!("Invalid params: model not found: {value}"),
                                    json!({"providerId":provider,"modelId":value}),
                                );
                            }
                        }
                        "mode" => {
                            if self.modes().iter().any(|(m, _, _)| *m == value) {
                                self.sessions[i].mode = value;
                                let opts = self.config_options(&self.sessions[i]);
                                self.rpc_result(io, &id, json!({"configOptions": opts}));
                            } else {
                                self.rpc_error(
                                    io,
                                    &id,
                                    -32602,
                                    &format!("Invalid params: mode not found: {value}"),
                                    json!({"mode": value}),
                                );
                            }
                        }
                        other => self.rpc_error(
                            io,
                            &id,
                            -32602,
                            &format!("Invalid params: unknown config option: {other}"),
                            json!({"configId": other}),
                        ),
                    }
                }
            },
            "session/set_mode" => match self.idx(&sid) {
                None => self.session_not_found(io, &id, &sid),
                Some(i) => {
                    let mode = p["modeId"].as_str().unwrap_or("").to_string();
                    if self.modes().iter().any(|(m, _, _)| *m == mode) {
                        self.sessions[i].mode = mode;
                        self.rpc_result(io, &id, json!({}));
                    } else {
                        self.rpc_error(
                            io,
                            &id,
                            -32602,
                            &format!("Invalid params: mode not found: {mode}"),
                            json!({"mode": mode}),
                        );
                    }
                }
            },
            "session/set_model" => match self.idx(&sid) {
                None => self.session_not_found(io, &id, &sid),
                Some(i) => {
                    let model = p["modelId"].as_str().unwrap_or("").to_string();
                    if self.models().iter().any(|(m, _)| *m == model) {
                        self.sessions[i].model = model;
                        self.rpc_result(io, &id, json!({}));
                    } else {
                        let provider = model.split('/').next().unwrap_or("").to_string();
                        self.rpc_error(
                            io,
                            &id,
                            -32602,
                            &format!("Invalid params: model not found: {model}"),
                            json!({"providerId":provider,"modelId":model}),
                        );
                    }
                }
            },
            "session/prompt" => match self.idx(&sid) {
                None => self.session_not_found(io, &id, &sid),
                Some(i) => {
                    let text = p["prompt"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter(|b| b["type"] == "text")
                                .filter_map(|b| b["text"].as_str())
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .unwrap_or_default();
                    self.session = sid;
                    self.prompt_id = id;
                    self.sessions[i].history.push(HistoryMessage {
                        role: "user".into(),
                        text: text.clone(),
                    });
                    return Incoming::Prompt(text);
                }
            },
            other => self.rpc_error(
                io,
                &id,
                -32601,
                &format!("\"Method not found\": {other}"),
                json!({"method": other}),
            ),
        }
        Incoming::Handled
    }

    fn permission_request(&mut self, io: &mut Io, tool_call: Value, options: Value) -> Value {
        let id = json!(self.next_request_id);
        self.next_request_id += 1;
        io.send(
            &json!({"jsonrpc":"2.0","id":id,"method":"session/request_permission",
            "params":{"sessionId":self.session,"toolCall":tool_call,"options":options}}),
        );
        id
    }

    fn cwd(&self) -> String {
        self.idx(&self.session)
            .map(|i| self.sessions[i].cwd.clone())
            .unwrap_or_default()
    }

    fn in_progress(&self, io: &mut Io) {
        let cwd = self.cwd();
        let mut raw = self.call_input.clone();
        if let Some(o) = raw.as_object_mut() {
            o.insert("cwd".into(), json!(cwd));
        }
        self.update(
            io,
            &self.session.clone(),
            json!({"sessionUpdate":"tool_call_update","toolCallId":self.call_id,"status":"in_progress",
                   "kind":self.call_kind,"title":self.call_title,"locations":[{"path":cwd}],"rawInput":raw}),
        );
    }

    fn finish_call(&self, io: &mut Io, ok: bool, text: &str) {
        self.finish_call_with(io, ok, text, None)
    }

    fn finish_call_with(&self, io: &mut Io, ok: bool, text: &str, image: Option<(String, String)>) {
        let sid = self.session.clone();
        if ok {
            let mut content = vec![json!({"type":"content","content":{"type":"text","text":text}})];
            if let Some((mime, data)) = image {
                content.push(json!({"type":"content","content":{"type":"image","mimeType":mime,"data":data}}));
            }
            self.update(
                io,
                &sid,
                json!({"sessionUpdate":"tool_call_update","toolCallId":self.call_id,
                "status":"completed","title":self.call_title,
                "content":content,
                "rawOutput":{"output":text,"metadata":{"output":text,"exit":0,"truncated":false}}}),
            );
        } else {
            let cwd = self.cwd();
            self.update(
                io,
                &sid,
                json!({"sessionUpdate":"tool_call_update","toolCallId":self.call_id,
                "status":"failed","kind":self.call_kind,"title":self.call_title,
                "locations":[{"path":cwd}],"rawInput":self.call_input,
                "content":[{"type":"content","content":{"type":"text","text":text}}],
                "rawOutput":{"error":text}}),
            );
        }
    }

    fn start_call(&mut self, io: &mut Io, name: &str, title: String, input: &Value) {
        self.call_id = format!("call_{}", hex(24));
        self.call_kind = kind_of(name);
        self.call_title = title;
        self.call_input = input.clone();
        let cwd = self.cwd();
        let sid = self.session.clone();
        self.update(
            io,
            &sid,
            json!({"sessionUpdate":"tool_call","toolCallId":self.call_id,
            "title":name,"kind":self.call_kind,"status":"pending",
            "locations":[{"path":cwd}],"rawInput":{"cwd":cwd}}),
        );
        self.in_progress(io);
    }

    fn usage_update(&self, io: &mut Io, used: u64, size: u64, cost: f64) {
        self.update(
            io,
            &self.session.clone(),
            json!({"sessionUpdate":"usage_update","used":used,"size":size,"cost":{"amount":cost,"currency":"USD"}}),
        );
    }
}

const REJECT_TEXT: &str = "The user rejected permission to use this specific tool call.";

fn selected_allows(body: &Value) -> bool {
    body["outcome"]["outcome"] == "selected"
        && matches!(body["outcome"]["optionId"].as_str(), Some(o) if o != "reject")
}

impl Persona for Acp {
    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return Incoming::Handled;
        };
        let has_method = v["method"].is_string();
        let has_id = !v["id"].is_null();
        match (has_method, has_id) {
            (true, true) => self.handle_request(io, &v),
            (true, false) => {
                if v["method"] == "session/cancel" && in_turn {
                    Incoming::Interrupt
                } else {
                    Incoming::Handled
                }
            }
            (false, true) => {
                let body = if v["error"].is_object() {
                    json!({"outcome":{"outcome":"cancelled"}})
                } else {
                    v["result"].clone()
                };
                Incoming::Response {
                    id: v["id"].clone(),
                    body,
                }
            }
            _ => Incoming::Handled,
        }
    }

    fn turn_begin(&mut self, _io: &mut Io, _prompt: &str, _stats: &mut TurnStats) {}

    fn say(&mut self, io: &mut Io, text: &str, chunks: usize, _stats: &mut TurnStats) {
        self.msg_id = format!("msg_{}", hex(26));
        let sid = self.session.clone();
        for c in crate::util::chunk_text(text, chunks) {
            self.update(
                io,
                &sid,
                json!({"sessionUpdate":"agent_message_chunk","messageId":self.msg_id,
                "content":{"type":"text","text":c}}),
            );
        }
    }

    fn think(&mut self, io: &mut Io, text: &str, _stats: &mut TurnStats) {
        let sid = self.session.clone();
        self.update(
            io,
            &sid,
            json!({"sessionUpdate":"agent_thought_chunk",
            "messageId":format!("prt_{}", hex(26)),"content":{"type":"text","text":text}}),
        );
    }

    fn image(&mut self, io: &mut Io, mime: &str, data: &str, _stats: &mut TurnStats) {
        let sid = self.session.clone();
        self.update(
            io,
            &sid,
            json!({"sessionUpdate":"agent_message_chunk","messageId":format!("msg_{}", hex(26)),
            "content":{"type":"image","mimeType":mime,"data":data}}),
        );
    }

    fn tool_call(&mut self, io: &mut Io, tool: &ToolStep, _stats: &mut TurnStats) -> Option<Value> {
        let title = tool.title.clone().unwrap_or_else(|| {
            tool.input["command"]
                .as_str()
                .unwrap_or(&tool.name)
                .to_string()
        });
        self.start_call(io, &tool.name, title.clone(), &tool.input);
        if tool.needs_permission {
            let cwd = self.cwd();
            let mut raw = tool.input.clone();
            if let Some(o) = raw.as_object_mut() {
                o.insert("directories".into(), json!([cwd]));
                o.insert("patterns".into(), json!([format!("{cwd}/*")]));
            }
            let call = json!({"toolCallId":self.call_id,"title":title,"kind":"other","status":"pending",
                "locations":[{"path":cwd}],"rawInput":raw});
            let options = json!([
                {"optionId":"once","kind":"allow_once","name":"Allow once"},
                {"optionId":"always","kind":"allow_always","name":"Always allow"},
                {"optionId":"reject","kind":"reject_once","name":"Reject"}]);
            return Some(self.permission_request(io, call, options));
        }
        let text = tool
            .result_if_allowed
            .clone()
            .unwrap_or_else(|| "ok".into());
        self.finish_call_with(io, true, &text, tool.run());
        None
    }

    fn tool_answered(
        &mut self,
        io: &mut Io,
        tool: &ToolStep,
        body: &Value,
        _stats: &mut TurnStats,
    ) {
        if selected_allows(body) {
            self.in_progress(io);
            let text = tool
                .result_if_allowed
                .clone()
                .unwrap_or_else(|| "ok".into());
            self.finish_call_with(io, true, &text, tool.run());
        } else {
            let text = tool
                .result_if_denied
                .clone()
                .unwrap_or_else(|| REJECT_TEXT.into());
            self.finish_call(io, false, &text);
        }
    }

    fn question(&mut self, io: &mut Io, ask: &AskStep, _stats: &mut TurnStats) -> Value {
        let input = json!({"questions":[{"question":ask.question,"header":ask.header.clone().unwrap_or_default(),
            "options":ask.options.iter().map(|o| json!({"label":o.label,"description":o.description})).collect::<Vec<_>>(),
            "multiple":ask.multi_select}]});
        self.start_call(io, "question", ask.question.clone(), &input);
        let cwd = self.cwd();
        let call = json!({"toolCallId":self.call_id,"title":ask.question,"kind":"other","status":"pending",
            "locations":[{"path":cwd}],"rawInput":input});
        let options: Vec<Value> = ask
            .options
            .iter()
            .map(|o| json!({"optionId":o.label,"kind":"allow_once","name":o.label}))
            .collect();
        self.permission_request(io, call, Value::Array(options))
    }

    fn question_answered(
        &mut self,
        io: &mut Io,
        _ask: &AskStep,
        body: &Value,
        _stats: &mut TurnStats,
    ) {
        if body["outcome"]["outcome"] == "selected" {
            let label = body["outcome"]["optionId"]
                .as_str()
                .unwrap_or("")
                .to_string();
            self.finish_call(io, true, &label);
        } else {
            self.finish_call(io, false, "The user dismissed this question.");
        }
    }

    fn plan(
        &mut self,
        io: &mut Io,
        entries: &[PlanEntry],
        _needs_approval: bool,
        _stats: &mut TurnStats,
    ) -> Option<Value> {
        let sid = self.session.clone();
        self.update(io, &sid, json!({"sessionUpdate":"plan","entries":entries.iter()
            .map(|e| json!({"content":e.content,"priority":e.priority,"status":e.status})).collect::<Vec<_>>()}));
        None
    }

    fn turn_end(&mut self, io: &mut Io, outcome: &Outcome, stats: &mut TurnStats) {
        let u = stats.usage.clone();
        let id = self.prompt_id.clone();
        match outcome {
            Outcome::Completed => {
                self.usage_update(
                    io,
                    u.input_tokens + u.output_tokens,
                    u.context_window,
                    u.cost_usd,
                );
                let mut usage = json!({"inputTokens":u.input_tokens,"outputTokens":u.output_tokens,
                    "totalTokens":u.input_tokens+u.output_tokens});
                if u.thought_tokens > 0 {
                    usage["thoughtTokens"] = json!(u.thought_tokens);
                }
                if u.cached_tokens > 0 {
                    usage["cachedReadTokens"] = json!(u.cached_tokens);
                }
                self.rpc_result(
                    io,
                    &id,
                    json!({"stopReason":"end_turn","usage":usage,"_meta":{}}),
                );
                if let Some(i) = self.idx(&self.session) {
                    self.sessions[i].history.push(HistoryMessage {
                        role: "assistant".into(),
                        text: stats.final_text.clone(),
                    });
                }
            }
            Outcome::Interrupted => {
                self.usage_update(io, 0, u.context_window, 0.0);
                self.rpc_result(
                    io,
                    &id,
                    json!({"stopReason":"cancelled",
                    "usage":{"inputTokens":0,"outputTokens":0,"totalTokens":0},"_meta":{}}),
                );
            }
            Outcome::Failed(msg) => {
                self.usage_update(io, 0, u.context_window, 0.0);
                self.rpc_error(
                    io,
                    &id,
                    -32603,
                    &format!("Internal error: {msg}"),
                    json!({"service":"session","errorName":"APIError"}),
                );
            }
        }
    }
}
