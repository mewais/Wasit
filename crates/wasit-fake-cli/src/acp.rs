//! The `opencode acp` / `kilo acp` personas: Agent Client Protocol over newline-delimited
//! JSON-RPC 2.0. Both CLIs share the implementation; they differ in identity, default model and
//! the modes they advertise.

use crate::engine::{Incoming, Outcome, Persona, TaskEnd, TaskIds, TurnStats};
use crate::io::Io;
use crate::kilo_http::{Answer, Hub, REQUEST_METHOD};
use crate::scenario::BackgroundStep;
use std::collections::BTreeSet;
use std::sync::Arc;

/// A background `task` of a Kilo session: a child session of its parent (see
/// [`Persona::task_started`]). Its status is what the job list reports.
#[derive(Debug, Clone)]
struct Child {
    id: String,
    parent: String,
    title: String,
    /// Kilo's job status word: `running`, `completed`, `error` or `cancelled`.
    status: &'static str,
    started_ms: u64,
    completed_ms: Option<u64>,
}
use crate::reference::find_out;
use crate::scenario::{AskStep, HistoryMessage, PlanEntry, Scenario, ToolStep};
use crate::util::{alnum, hex, iso_from_ms, iso_now, now_ms, set};
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

    /// The variable holding the inline JSON config the real CLI merges over its own (where
    /// Wasit puts MCP servers).
    fn config_env(self) -> &'static str {
        match self {
            Flavor::Opencode => "OPENCODE_CONFIG_CONTENT",
            Flavor::Kilo => "KILO_CONFIG_CONTENT",
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Sess {
    id: String,
    cwd: String,
    title: String,
    model: String,
    mode: String,
    history: Vec<HistoryMessage>,
    /// The `session/update` payloads `session/load` replays, as the live turns sent them (text
    /// messages whole, thoughts, tool calls with their outcome, the usage after each turn).
    /// A session known only by its `history` (a scenario's) replays that instead.
    #[serde(default)]
    replay: Vec<Value>,
    /// Creation and last change, milliseconds (`session list` reports them).
    #[serde(default)]
    created_ms: u64,
    #[serde(default)]
    updated_ms: u64,
}

/// One session as `session list` and `session/list` show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub id: String,
    pub cwd: String,
    pub title: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// The sessions a flavor lists: the scenario's and every one an earlier process stored, newest
/// activity first, those of `cwd` only when it is given. A scenario session counts as created
/// when this process started.
pub fn listed_sessions(flavor: Flavor, scenario: &Scenario, cwd: Option<&str>) -> Vec<Listed> {
    let now = now_ms();
    let here = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "/".into());
    let mut all: Vec<Listed> = scenario
        .sessions
        .iter()
        .map(|s| Listed {
            id: s.id.clone(),
            cwd: s.cwd.clone().unwrap_or_else(|| here.clone()),
            title: s.title.clone().unwrap_or_else(|| "New session".into()),
            created_ms: now,
            updated_ms: now,
        })
        .collect();
    for s in crate::store::list::<Sess>(&format!("acp-{}", flavor.persona())) {
        if !all.iter().any(|x| x.id == s.id) {
            all.push(Listed {
                id: s.id,
                cwd: s.cwd,
                title: s.title,
                created_ms: s.created_ms,
                updated_ms: s.updated_ms.max(s.created_ms),
            });
        }
    }
    all.retain(|l| cwd.is_none_or(|c| c == l.cwd));
    all.sort_by_key(|l| std::cmp::Reverse(l.updated_ms));
    all
}

pub struct Acp {
    flavor: Flavor,
    scenario: Scenario,
    sessions: Vec<Sess>,
    first_id: Option<String>,
    next_request_id: u64,
    /// The HTTP+SSE server of `kilo acp --port` (Kilo only; see [`crate::kilo_http`]).
    hub: Option<Arc<Hub>>,
    /// Sessions with a turn running, as the server reports them (`GET /session/status`).
    busy: BTreeSet<String>,
    /// The background subagents this process started.
    children: Vec<Child>,
    /// The parent session while a subagent's own conversation runs ([`Persona::set_scope`]).
    scope_parent: Option<String>,
    /// A turn the agent started by itself (Kilo delivering a background result) is running: it
    /// has no prompt, so it gets no `session/prompt` result.
    driver_turn: bool,
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
    /// Where this flavor's sessions are kept (see [`crate::store`]).
    fn store_kind(&self) -> String {
        format!("acp-{}", self.flavor.persona())
    }

    /// Write the session down. The real agents (kilo 7.8.8, verified; opencode keeps every
    /// session it opens too) store a session when it is created, before any prompt, so
    /// `session/load` of it works in the next process.
    fn store_session(&mut self, i: usize) {
        self.sessions[i].updated_ms = now_ms();
        crate::store::put(&self.store_kind(), &self.sessions[i].id, &self.sessions[i]);
    }

    /// Remember an update of the running turn for `session/load` to replay.
    fn record(&mut self, update: Value) {
        if let Some(i) = self.idx(&self.session.clone()) {
            self.sessions[i].replay.push(update);
        }
    }

    /// Serve Kilo's HTTP and SSE routes too (`hub` from [`crate::kilo_http::start_from_argv`]).
    pub fn attach_http(&mut self, hub: Option<Arc<Hub>>) {
        self.hub = hub;
    }

    /// Publish a server event, when there is a server.
    fn emit(&self, payload_type: &str, properties: Value) {
        if let Some(hub) = &self.hub {
            hub.publish(payload_type, properties);
        }
    }

    /// A session's turn starts or ends: the status (`GET /session/status`) changes, and with
    /// `announce` the status event and `session.idle` go out too, once per change.
    ///
    /// The owner's own prompt turns are not announced: the prompt's result is their end, and a
    /// status event that reaches the bridge after that result would read as a turn of Kilo's own.
    /// Kilo's own turns (delivering a background result) and the subagents are announced.
    fn set_busy(&mut self, sid: &str, busy: bool, announce: bool) {
        let changed = if busy {
            self.busy.insert(sid.to_string())
        } else {
            self.busy.remove(sid)
        };
        if !changed || !announce {
            return;
        }
        if busy {
            self.emit(
                "session.status",
                json!({"sessionID": sid, "status": {"type": "busy"}}),
            );
        } else {
            self.emit(
                "session.status",
                json!({"sessionID": sid, "status": {"type": "idle"}}),
            );
            self.emit("session.idle", json!({"sessionID": sid}));
        }
    }

    /// Answer one HTTP request the server handed over (`params`: `{id, method, path, body}`).
    /// Only the routes the kilo-bridge uses exist. An abort of the running turn's session ends
    /// that turn and nothing else: its background children keep running (Kilo's `scope=session`).
    /// An abort of a running child with `scope=tree` stops it: the engine ends it, and
    /// [`Persona::control_done`] answers.
    fn http(&mut self, io: &mut Io, params: &Value, in_turn: bool) -> Incoming {
        let Some(hub) = self.hub.clone() else {
            return Incoming::Handled;
        };
        let id = params["id"].as_u64().unwrap_or(0);
        let target = params["path"].as_str().unwrap_or("");
        let (route, query) = target.split_once('?').unwrap_or((target, ""));
        let segs: Vec<&str> = route.split('/').filter(|s| !s.is_empty()).collect();
        let method = params["method"].as_str().unwrap_or("");
        let mut interrupt = false;
        let answer = match (method, segs.as_slice()) {
            ("GET", ["session", "status"]) => {
                let map: serde_json::Map<String, Value> = self
                    .busy
                    .iter()
                    .map(|s| (s.clone(), json!({"type": "busy"})))
                    .collect();
                Answer::json(200, &Value::Object(map))
            }
            ("GET", ["session", parent, "children"]) => {
                let list: Vec<Value> = self
                    .children
                    .iter()
                    .filter(|c| c.parent == *parent)
                    .map(|c| {
                        json!({"id": c.id, "parentID": c.parent, "projectID": "global",
                               "title": c.title, "time": {"created": c.started_ms, "updated": c.started_ms}})
                    })
                    .collect();
                Answer::json(200, &Value::Array(list))
            }
            ("GET", ["kilocode", "background-jobs"]) => {
                let parent = query_value(query, "sessionID").unwrap_or("");
                let jobs: Vec<Value> = self
                    .children
                    .iter()
                    .filter(|c| c.parent == parent)
                    .map(|c| {
                        json!({"id": c.id, "type": "task", "title": c.title, "status": c.status,
                               "started_at": c.started_ms, "completed_at": c.completed_ms,
                               "error": null,
                               "metadata": {"parentSessionId": c.parent, "sessionId": c.id}})
                    })
                    .collect();
                Answer::json(200, &Value::Array(jobs))
            }
            ("GET", ["permission"]) => Answer::json(200, &json!([])),
            ("POST", ["permission", _, "reply"]) => Answer::json(200, &json!(true)),
            ("POST", ["session", sid, "abort"]) => {
                if query_value(query, "scope") == Some("tree") && self.running_child(sid) {
                    // Answered by `control_done`, once the engine has ended the task.
                    return Incoming::StopTask {
                        request: json!(id),
                        task_id: sid.to_string(),
                    };
                }
                if in_turn && *sid == self.session {
                    interrupt = true;
                }
                Answer::json(200, &json!(true))
            }
            ("POST", ["session", sid, "prompt_async"]) => {
                // A message to a running subagent: it reads it as its own user message. The
                // subagent is between its steps while it runs in the background, so nothing
                // waits for a step boundary.
                let text = steer_text(params["body"].as_str().unwrap_or(""));
                if let (true, Some(text)) = (self.running_child(sid), text) {
                    self.child_user_message(io, sid, &text);
                }
                Answer::empty(204)
            }
            _ => Answer::empty(404),
        };
        hub.answer(id, answer);
        if interrupt {
            Incoming::Interrupt
        } else {
            Incoming::Handled
        }
    }

    fn running_child(&self, sid: &str) -> bool {
        self.children
            .iter()
            .any(|c| c.id == sid && c.status == "running")
    }

    /// A user message in a subagent's conversation: sent on its wire and kept in its session.
    fn child_user_message(&mut self, io: &mut Io, sid: &str, text: &str) {
        let update = json!({"sessionUpdate": "user_message_chunk",
            "messageId": format!("msg_{}", hex(26)), "content": {"type": "text", "text": text}});
        self.update(io, sid, update.clone());
        if let Some(i) = self.idx(sid) {
            self.sessions[i].replay.push(update);
            self.sessions[i].history.push(HistoryMessage {
                role: "user".into(),
                text: text.to_string(),
            });
            self.store_session(i);
        }
    }

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
            hub: None,
            busy: BTreeSet::new(),
            children: vec![],
            scope_parent: None,
            driver_turn: false,
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
                replay: vec![],
                created_ms: now_ms(),
                updated_ms: now_ms(),
            })
            .collect();
        // Sessions earlier processes stored.
        for s in crate::store::list::<Sess>(&a.store_kind()) {
            if !a.sessions.iter().any(|x| x.id == s.id) {
                a.sessions.push(s);
            }
        }
        a
    }

    /// What `session/load` replays for session `i`: the updates its turns sent, or, for a
    /// session known only by its messages, one chunk per message.
    fn replay_of(&self, i: usize) -> Vec<Value> {
        let s = &self.sessions[i];
        if !s.replay.is_empty() {
            return s.replay.clone();
        }
        s.history
            .iter()
            .map(|m| {
                let kind = if m.role == "user" {
                    "user_message_chunk"
                } else {
                    "agent_message_chunk"
                };
                json!({"sessionUpdate":kind,"messageId":format!("msg_{}", hex(26)),
                       "content":{"type":"text","text":m.text}})
            })
            .collect()
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
            replay: vec![],
            created_ms: now_ms(),
            updated_ms: now_ms(),
        });
        let i = self.sessions.len() - 1;
        self.store_session(i);
        let (sid, title, directory) = (
            self.sessions[i].id.clone(),
            self.sessions[i].title.clone(),
            cwd.to_string(),
        );
        self.emit(
            "session.created",
            json!({"sessionID": sid, "info": {"id": sid, "directory": directory, "title": title,
                   "version": "wasit-fake"}}),
        );
        i
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
                let list: Vec<Value> =
                    listed_sessions(self.flavor, &self.scenario, p["cwd"].as_str())
                        .into_iter()
                        .map(|s| {
                            json!({"sessionId":s.id,"cwd":s.cwd,"title":s.title,
                        "updatedAt":iso_from_ms(s.updated_ms)})
                        })
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
                        for u in self.replay_of(i) {
                            self.update(io, &sid, u);
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
                    let (model, mode, history, replay) = {
                        let src = &self.sessions[i];
                        (
                            src.model.clone(),
                            src.mode.clone(),
                            src.history.clone(),
                            src.replay.clone(),
                        )
                    };
                    self.sessions[n].model = model;
                    self.sessions[n].mode = mode;
                    self.sessions[n].history = history;
                    self.sessions[n].replay = replay;
                    self.store_session(n);
                    let nid = self.sessions[n].id.clone();
                    let r = json!({"sessionId": nid, "configOptions": self.config_options(&self.sessions[n])});
                    self.rpc_result(io, &id, r);
                    self.commands_update(io, &nid);
                }
            },
            "session/close" => match self.idx(&sid) {
                None => self.session_not_found(io, &id, &sid),
                Some(i) => {
                    let gone = self.sessions.remove(i);
                    crate::store::remove(&self.store_kind(), &gone.id);
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
                    self.sessions[i]
                        .replay
                        .push(json!({"sessionUpdate":"user_message_chunk",
                        "messageId":format!("msg_{}", hex(26)),
                        "content":{"type":"text","text":text}}));
                    self.store_session(i);
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

    fn finish_call(&mut self, io: &mut Io, ok: bool, text: &str) {
        self.finish_call_with(io, ok, text, None)
    }

    fn finish_call_with(
        &mut self,
        io: &mut Io,
        ok: bool,
        text: &str,
        image: Option<(String, String)>,
    ) {
        let sid = self.session.clone();
        let update = if ok {
            let mut content = vec![json!({"type":"content","content":{"type":"text","text":text}})];
            if let Some((mime, data)) = image {
                content.push(json!({"type":"content","content":{"type":"image","mimeType":mime,"data":data}}));
            }
            json!({"sessionUpdate":"tool_call_update","toolCallId":self.call_id,
                "status":"completed","title":self.call_title,
                "content":content,
                "rawOutput":{"output":text,"metadata":{"output":text,"exit":0,"truncated":false}}})
        } else {
            let cwd = self.cwd();
            json!({"sessionUpdate":"tool_call_update","toolCallId":self.call_id,
                "status":"failed","kind":self.call_kind,"title":self.call_title,
                "locations":[{"path":cwd}],"rawInput":self.call_input,
                "content":[{"type":"content","content":{"type":"text","text":text}}],
                "rawOutput":{"error":text}})
        };
        self.update(io, &sid, update.clone());
        self.record(update);
    }

    fn start_call(&mut self, io: &mut Io, name: &str, title: String, input: &Value) {
        self.call_id = format!("call_{}", hex(24));
        self.call_kind = kind_of(name);
        self.call_title = title;
        self.call_input = input.clone();
        let cwd = self.cwd();
        let sid = self.session.clone();
        let call = json!({"sessionUpdate":"tool_call","toolCallId":self.call_id,
            "title":name,"kind":self.call_kind,"status":"pending",
            "locations":[{"path":cwd}],"rawInput":{"cwd":cwd}});
        self.update(io, &sid, call.clone());
        // What a load replays of the call: its start with the arguments it was given.
        let mut replayed = call;
        replayed["rawInput"] = {
            let mut raw = input.clone();
            if let Some(o) = raw.as_object_mut() {
                o.insert("cwd".into(), json!(cwd));
            }
            raw
        };
        self.record(replayed);
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

/// The value of `key` in a query string (`a=1&b=2`).
fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
}

/// The text of a `prompt_async` body (`{"parts": [{"type": "text", "text": …}]}`).
fn steer_text(body: &str) -> Option<String> {
    let v: Value = serde_json::from_str(body).ok()?;
    let parts = v["parts"].as_array()?;
    let text: Vec<&str> = parts
        .iter()
        .filter(|p| p["type"] == "text")
        .filter_map(|p| p["text"].as_str())
        .collect();
    (!text.is_empty()).then(|| text.join("\n"))
}

fn selected_allows(body: &Value) -> bool {
    body["outcome"]["outcome"] == "selected"
        && matches!(body["outcome"]["optionId"].as_str(), Some(o) if o != "reject")
}

impl Persona for Acp {
    fn mcp_server(&self, name: &str) -> Option<crate::mcp::McpLaunch> {
        let cfg = std::env::var(self.flavor.config_env()).ok()?;
        crate::mcp::servers_from_opencode_config(&cfg).remove(name)
    }

    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return Incoming::Handled;
        };
        let has_method = v["method"].is_string();
        let has_id = !v["id"].is_null();
        match (has_method, has_id) {
            (true, true) => self.handle_request(io, &v),
            (true, false) => {
                if v["method"] == REQUEST_METHOD {
                    return self.http(io, &v["params"], in_turn);
                }
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

    fn turn_begin(
        &mut self,
        _io: &mut Io,
        _prompt: &str,
        _id: Option<&str>,
        _stats: &mut TurnStats,
    ) {
        let sid = self.session.clone();
        self.set_busy(&sid, true, false);
    }

    /// Background tasks are Kilo's subagents, and need the server (their status and steering
    /// are over HTTP).
    fn background(&self) -> bool {
        self.flavor == Flavor::Kilo && self.hub.is_some()
    }

    /// Kilo runs a `task` as a child session. The parent's `task` call shows the child's id at
    /// once (`rawOutput.metadata.sessionId`), the child is announced to the server with its
    /// parent, and it is busy until it ends.
    fn task_started(
        &mut self,
        io: &mut Io,
        task: &BackgroundStep,
        _stats: &mut TurnStats,
    ) -> TaskIds {
        let parent = self.session.clone();
        let child = format!("ses_{}", alnum(26));
        let call_id = format!("call_{}", hex(24));
        let title = task.description.clone();
        let cwd = self.cwd();
        let text = format!(
            "<task id=\"{child}\" state=\"running\">\n<summary>Background task started</summary>\n\
             <task_result>\nThe task is working in the background. You will be notified automatically \
             when it finishes.\n</task_result>\n</task>"
        );
        let updates = [
            json!({"sessionUpdate": "tool_call", "toolCallId": call_id, "title": "task",
                   "kind": "think", "status": "pending", "locations": [], "rawInput": {}}),
            json!({"sessionUpdate": "tool_call_update", "toolCallId": call_id, "status": "in_progress",
                   "kind": "think", "title": title, "locations": [],
                   "rawInput": {"description": title, "prompt": "",
                                "subagent_type": task.subagent_type.clone().unwrap_or_else(|| "general".into()),
                                "background": true}}),
            json!({"sessionUpdate": "tool_call_update", "toolCallId": call_id, "status": "completed",
                   "title": title, "content": [{"type": "content", "content": {"type": "text", "text": text}}],
                   "rawOutput": {"output": text, "metadata": {"sessionId": child, "background": true}}}),
        ];
        for u in updates {
            self.update(io, &parent, u.clone());
            self.record(u);
        }
        let now = now_ms();
        self.sessions.push(Sess {
            id: child.clone(),
            cwd: cwd.clone(),
            title: title.clone(),
            model: self.default_model(),
            mode: self.default_mode(),
            history: vec![],
            replay: vec![],
            created_ms: now,
            updated_ms: now,
        });
        let i = self.sessions.len() - 1;
        self.store_session(i);
        self.emit(
            "session.created",
            json!({"sessionID": child, "info": {"id": child, "parentID": parent,
                   "directory": cwd, "title": title, "version": "wasit-fake"}}),
        );
        self.children.push(Child {
            id: child.clone(),
            parent,
            title,
            status: "running",
            started_ms: now,
            completed_ms: None,
        });
        self.set_busy(&child, true, true);
        TaskIds {
            task_id: child.clone(),
            run_id: String::new(),
            call_id,
            agent_id: child,
        }
    }

    /// The subagent's own conversation runs with its session as the current one.
    fn set_scope(&mut self, scope: Option<(&BackgroundStep, &TaskIds)>) {
        match scope {
            Some((_, ids)) => {
                self.scope_parent = Some(self.session.clone());
                self.session = ids.agent_id.clone();
            }
            None => {
                if let Some(parent) = self.scope_parent.take() {
                    self.session = parent;
                }
            }
        }
    }

    /// The child ends: its job reaches its final status before the server reports it idle (the
    /// protocol reads the job list when it hears of the idle).
    fn task_ended(&mut self, _io: &mut Io, _task: &BackgroundStep, ids: &TaskIds, how: TaskEnd) {
        let status = match how {
            TaskEnd::Completed => "completed",
            TaskEnd::Failed => "error",
            TaskEnd::Stopped => "cancelled",
        };
        if let Some(c) = self.children.iter_mut().find(|c| c.id == ids.task_id) {
            c.status = status;
            c.completed_ms = Some(now_ms());
        }
        self.set_busy(&ids.task_id, false, true);
    }

    /// Kilo starts a turn of its own with the child's result: the owner is busy until it ends.
    fn agent_turn_begin(&mut self, _io: &mut Io, _ids: &TaskIds, _stats: &mut TurnStats) {
        self.driver_turn = true;
        let sid = self.session.clone();
        self.set_busy(&sid, true, true);
    }

    /// The answer to a stop (see [`Incoming::StopTask`]): the server answers the abort request.
    fn control_done(&mut self, _io: &mut Io, request: &Value, _found: bool) {
        if let (Some(hub), Some(id)) = (&self.hub, request.as_u64()) {
            hub.answer(id, Answer::json(200, &json!(true)));
        }
    }

    fn say(&mut self, io: &mut Io, text: &str, chunks: usize, _stats: &mut TurnStats) {
        self.msg_id = format!("msg_{}", hex(26));
        let sid = self.session.clone();
        self.record(
            json!({"sessionUpdate":"agent_message_chunk","messageId":self.msg_id,
            "content":{"type":"text","text":text}}),
        );
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
        let thought = json!({"sessionUpdate":"agent_thought_chunk",
            "messageId":format!("prt_{}", hex(26)),"content":{"type":"text","text":text}});
        self.update(io, &sid, thought.clone());
        self.record(thought);
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
        // The session is idle before its result goes out, as Kilo reports it.
        let sid = self.session.clone();
        let driver = self.driver_turn;
        self.set_busy(&sid, false, driver);
        if std::mem::take(&mut self.driver_turn) {
            // The turn Kilo started by itself has no prompt to answer.
            self.usage_update(
                io,
                u.input_tokens + u.output_tokens,
                u.context_window,
                u.cost_usd,
            );
            return;
        }
        match outcome {
            Outcome::Completed => {
                self.usage_update(
                    io,
                    u.input_tokens + u.output_tokens,
                    u.context_window,
                    u.cost_usd,
                );
                self.record(json!({"sessionUpdate":"usage_update",
                    "used":u.input_tokens + u.output_tokens,"size":u.context_window,
                    "cost":{"amount":u.cost_usd,"currency":"USD"}}));
                // Stored before the result: a client that loads the session right after it
                // finds the whole turn.
                if let Some(i) = self.idx(&self.session) {
                    self.sessions[i].history.push(HistoryMessage {
                        role: "assistant".into(),
                        text: stats.final_text.clone(),
                    });
                    self.store_session(i);
                }
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
            }
            Outcome::Interrupted => {
                self.usage_update(io, 0, u.context_window, 0.0);
                self.rpc_result(
                    io,
                    &id,
                    json!({"stopReason":"cancelled",
                    "usage":{"inputTokens":0,"outputTokens":0,"totalTokens":0},"_meta":{}}),
                );
                if let Some(i) = self.idx(&self.session) {
                    self.store_session(i);
                }
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
                if let Some(i) = self.idx(&self.session) {
                    self.store_session(i);
                }
            }
        }
    }
}
