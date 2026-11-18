//! ACP (Agent Client Protocol, JSON-RPC 2.0 over NDJSON) as a pure
//! [`Protocol`], shared by the OpenCode and Kilo Code flavors.
//!
//! Wire facts (verified against `opencode acp` 1.19.6 and `kilo acp` 7.8.27):
//! - Handshake: `initialize {protocolVersion: 1}` → `session/new {cwd,
//!   mcpServers}` | `session/load {sessionId, cwd, mcpServers}`. The result
//!   carries `configOptions` (`model`, `mode`, kilo also `effort`).
//!   `ResumeMode::Continue` uses `session/list {cwd}` and loads the most
//!   recently updated session.
//! - Model / mode: `session/set_config_option {sessionId, configId, value}`;
//!   the agent also pushes `config_option_update` / `current_mode_update`.
//!   The launch model / mode are applied this way right after the session
//!   opens (`opencode acp` has no `--model`).
//! - Turns: `session/prompt` (one at a time) → `session/update`
//!   notifications → the prompt result `{stopReason, usage}`. Prompts sent
//!   during a turn are held by this protocol. Text starting with `/name`
//!   runs that command / skill.
//! - Interrupt: `session/cancel` (a notification); pending permission
//!   requests are answered `{"outcome":{"outcome":"cancelled"}}` first.
//! - Gates: `session/request_permission {toolCall, options}` answered with
//!   `{"outcome":{"outcome":"selected","optionId":…}}`.
//!   Each option becomes a [`GateChoice`] with the option's id, its `name` as the label
//!   (the agent's own wording) and its `kind` mapped one to one; `AnswerGate` selects the
//!   chosen option by id. ACP has no question requests, so `AnswerQuestion` is refused.

use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::{json, Value};
use wasit_core::wire::{
    id_text, opt_s, rpc_error, rpc_notification, rpc_request, rpc_result, s, u, RpcIn,
};
use wasit_core::{
    AgentEvent, Command, CompactionInfo, DriverError, Encoded, EndReason, GateChoice,
    GateChoiceKind, GateDecision, GateRequest, ModelInfo, OutLine, PlanEntry, Protocol, ResumeMode,
    SessionMode, SlashCommandInfo, SpawnSpec, Step, Usage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    New,
    Handshaking,
    Ready,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Req {
    Initialize,
    List,
    Open,
    SetModel,
    SetMode,
    Prompt,
}

impl Req {
    fn command(self) -> &'static str {
        match self {
            Req::Initialize => "initialize",
            Req::List => "continue",
            Req::Open => "session/open",
            Req::SetModel => "set_model",
            Req::SetMode => "set_mode",
            Req::Prompt => "prompt",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PendingGate {
    routing_id: String,
    raw_id: Value,
    choices: Vec<GateChoice>,
}

/// The ACP protocol state machine.
#[derive(Debug)]
pub struct AcpProtocol {
    client_name: &'static str,
    phase: Phase,
    next_id: u64,
    outstanding: HashMap<u64, Req>,
    queue: VecDeque<Command>,
    spec: SpawnSpec,
    session_id: Option<String>,
    can_list: bool,
    /// `session/load` in flight: its history replay is not live activity.
    loading: bool,
    prompt_active: bool,
    cancel_sent: bool,
    model: Option<String>,
    mode: Option<String>,
    gates: Vec<PendingGate>,
    tools: HashMap<String, String>,
    subagents: HashSet<String>,
    subagents_started: HashSet<String>,
}

impl AcpProtocol {
    /// `client_name` is announced in `initialize.clientInfo.name`.
    pub fn new(client_name: &'static str) -> Self {
        AcpProtocol {
            client_name,
            phase: Phase::New,
            next_id: 1,
            outstanding: HashMap::new(),
            queue: VecDeque::new(),
            spec: SpawnSpec::default(),
            session_id: None,
            can_list: false,
            loading: false,
            prompt_active: false,
            cancel_sent: false,
            model: None,
            mode: None,
            gates: Vec::new(),
            tools: HashMap::new(),
            subagents: HashSet::new(),
            subagents_started: HashSet::new(),
        }
    }

    fn request(&mut self, req: Req, method: &str, params: Value) -> OutLine {
        let id = self.next_id;
        self.next_id += 1;
        self.outstanding.insert(id, req);
        rpc_request(id, method, params)
    }

    fn sid(&self) -> String {
        self.session_id.clone().unwrap_or_default()
    }

    // ── handshake ───────────────────────────────────────────────────────────

    fn on_initialized(&mut self, result: &Value) -> Step {
        let caps = result
            .get("agentCapabilities")
            .cloned()
            .unwrap_or(Value::Null);
        self.can_list = caps
            .get("sessionCapabilities")
            .and_then(|c| c.get("list"))
            .is_some();
        let line = match self.spec.resume.clone() {
            ResumeMode::Resume(id) => self.session_load(&id),
            ResumeMode::Continue if self.can_list => self.request(
                Req::List,
                "session/list",
                json!({"cwd": self.spec.cwd_or_dot()}),
            ),
            ResumeMode::Continue => {
                let line = self.session_new();
                return Step {
                    events: vec![AgentEvent::CommandRejected {
                        command: "continue".into(),
                        message: "the agent cannot list sessions; started a new one".into(),
                    }],
                    write: vec![line],
                    close_stdin: false,
                };
            }
            ResumeMode::Fresh => self.session_new(),
        };
        Step {
            write: vec![line],
            ..Step::default()
        }
    }

    fn session_new(&mut self) -> OutLine {
        let params = json!({"cwd": self.spec.cwd_or_dot(), "mcpServers": []});
        self.request(Req::Open, "session/new", params)
    }

    fn session_load(&mut self, id: &str) -> OutLine {
        self.session_id = Some(id.to_string());
        self.loading = true;
        let params = json!({"sessionId": id, "cwd": self.spec.cwd_or_dot(), "mcpServers": []});
        self.request(Req::Open, "session/load", params)
    }

    fn on_opened(&mut self, result: &Value) -> Step {
        self.loading = false;
        if let Some(id) = opt_s(result, "sessionId") {
            self.session_id = Some(id);
        }
        let (modes, mode) = config_modes(result);
        let (models, model) = config_models(result);
        self.model = model.clone();
        self.mode = mode.clone();
        let mut step = events(vec![AgentEvent::SessionInit {
            native_session_id: self.session_id.clone(),
            model,
            cwd: self.spec.cwd.clone(),
            tools: vec![],
            mcp_servers: vec![],
            slash_commands: vec![],
            available_modes: modes,
            current_mode: mode.unwrap_or_default(),
            cli_version: String::new(),
        }]);
        if !models.is_empty() {
            step.events.push(AgentEvent::ModelsUpdated {
                models,
                current: self.model.clone(),
            });
        }
        if let Some(m) = self.spec.model.clone().filter(|m| !m.is_empty()) {
            if self.model.as_deref() != Some(m.as_str()) {
                let line = self.set_option(Req::SetModel, "model", &m);
                step.write.push(line);
            }
        }
        if !self.spec.mode.is_empty() && self.mode.as_deref() != Some(self.spec.mode.as_str()) {
            let mode = self.spec.mode.clone();
            let line = self.set_option(Req::SetMode, "mode", &mode);
            step.write.push(line);
        }
        self.phase = Phase::Ready;
        step.extend(self.drain());
        step
    }

    fn set_option(&mut self, req: Req, id: &str, value: &str) -> OutLine {
        let params = json!({"sessionId": self.sid(), "configId": id, "value": value});
        self.request(req, "session/set_config_option", params)
    }

    // ── commands ────────────────────────────────────────────────────────────

    fn must_queue(&self, cmd: &Command) -> bool {
        let prompt = matches!(cmd, Command::Prompt { .. } | Command::RunSlash { .. });
        match self.phase {
            Phase::New | Phase::Handshaking => {
                prompt || matches!(cmd, Command::SetModel { .. } | Command::SetMode { .. })
            }
            Phase::Ready => prompt && (self.prompt_active || !self.queue.is_empty()),
            Phase::Closed => false,
        }
    }

    fn encode_now(&mut self, cmd: Command) -> Result<Step, DriverError> {
        let mut step = Step::default();
        match cmd {
            Command::Prompt { text } => step = self.prompt(&text),
            Command::RunSlash { name, args } => {
                let name = name.trim_start_matches('/');
                let text = if args.trim().is_empty() {
                    format!("/{name}")
                } else {
                    format!("/{name} {}", args.trim())
                };
                step = self.prompt(&text);
            }
            Command::AnswerGate {
                request_id,
                decision,
            } => {
                let gate = self.take_gate(&request_id)?;
                if let Err(detail) = check_choice(&gate.choices, &request_id, &decision) {
                    self.gates.push(gate);
                    return Err(DriverError::Invalid { detail });
                }
                step.write.push(selected(&gate.raw_id, &decision.choice));
            }
            Command::AnswerQuestion { request_id, .. } => {
                return Err(DriverError::Invalid {
                    detail: format!(
                        "gate {request_id} is not a question (ACP has no question requests)"
                    ),
                });
            }
            Command::Interrupt => step = self.interrupt(),
            Command::SetModel { model } => {
                let line = self.set_option(Req::SetModel, "model", &model);
                step.write.push(line);
            }
            Command::SetMode { mode } => {
                let line = self.set_option(Req::SetMode, "mode", &mode);
                step.write.push(line);
            }
            Command::Shutdown => {
                self.phase = Phase::Closed;
                self.queue.clear();
                step.close_stdin = true;
            }
        }
        Ok(step)
    }

    fn prompt(&mut self, text: &str) -> Step {
        self.prompt_active = true;
        self.cancel_sent = false;
        let line = self.request(
            Req::Prompt,
            "session/prompt",
            json!({"sessionId": self.sid(), "prompt": [{"type": "text", "text": text}]}),
        );
        Step {
            events: vec![AgentEvent::TurnStarted {
                native_turn_id: None,
            }],
            write: vec![line],
            close_stdin: false,
        }
    }

    fn take_gate(&mut self, routing_id: &str) -> Result<PendingGate, DriverError> {
        let pos = self
            .gates
            .iter()
            .position(|g| g.routing_id == routing_id)
            .ok_or_else(|| DriverError::NotFound {
                detail: format!("no pending gate {routing_id}"),
            })?;
        Ok(self.gates.remove(pos))
    }

    fn interrupt(&mut self) -> Step {
        let mut step = Step::default();
        self.queue
            .retain(|c| !matches!(c, Command::Prompt { .. } | Command::RunSlash { .. }));
        for g in std::mem::take(&mut self.gates) {
            step.write.push(cancelled(&g.raw_id));
            step.events.push(AgentEvent::GateResolved {
                request_id: g.routing_id,
                reason: None,
            });
        }
        if self.prompt_active && !self.cancel_sent {
            self.cancel_sent = true;
            step.write.push(rpc_notification(
                "session/cancel",
                Some(json!({"sessionId": self.sid()})),
            ));
        }
        step
    }

    fn drain(&mut self) -> Step {
        let mut step = Step::default();
        while let Some(cmd) = self.queue.front() {
            let prompt = matches!(cmd, Command::Prompt { .. } | Command::RunSlash { .. });
            if self.phase != Phase::Ready || (prompt && self.prompt_active) {
                break;
            }
            let cmd = self.queue.pop_front().expect("front exists");
            let name = cmd.name();
            match self.encode_now(cmd) {
                Ok(s) => step.extend(s),
                Err(e) => step.events.push(AgentEvent::CommandRejected {
                    command: name.into(),
                    message: e.to_string(),
                }),
            }
        }
        step
    }

    // ── incoming ────────────────────────────────────────────────────────────

    fn on_response(&mut self, id: &Value, result: Value) -> Step {
        let Some(req) = id.as_u64().and_then(|n| self.outstanding.remove(&n)) else {
            return events(vec![AgentEvent::Unknown {
                raw: json!({"id": id, "result": result}),
            }]);
        };
        match req {
            Req::Initialize => self.on_initialized(&result),
            Req::List => {
                let latest = result
                    .get("sessions")
                    .and_then(Value::as_array)
                    .and_then(|a| {
                        a.iter()
                            .filter(|x| opt_s(x, "sessionId").is_some())
                            .max_by(|a, b| s(a, "updatedAt").cmp(s(b, "updatedAt")))
                    })
                    .and_then(|x| opt_s(x, "sessionId"));
                let line = match latest {
                    Some(id) => self.session_load(&id),
                    None => self.session_new(),
                };
                Step {
                    write: vec![line],
                    ..Step::default()
                }
            }
            Req::Open => self.on_opened(&result),
            Req::SetModel | Req::SetMode => events(self.on_config(&result)),
            Req::Prompt => self.on_prompt_result(&result),
        }
    }

    fn on_error(&mut self, id: &Value, message: String) -> Step {
        let Some(req) = id.as_u64().and_then(|n| self.outstanding.remove(&n)) else {
            return events(vec![AgentEvent::Unknown {
                raw: json!({"id": id, "error": {"message": message}}),
            }]);
        };
        match req {
            Req::Prompt => {
                self.prompt_active = false;
                self.cancel_sent = false;
                let mut step = events(vec![AgentEvent::TurnCompleted {
                    end_reason: Some(EndReason::ExecutionError),
                    was_interrupted: false,
                    num_turns: 0,
                    usage: Usage::default(),
                    per_model: HashMap::new(),
                    reply: Some(message),
                }]);
                step.extend(self.drain());
                step
            }
            Req::List => {
                let line = self.session_new();
                Step {
                    events: vec![AgentEvent::CommandRejected {
                        command: req.command().into(),
                        message,
                    }],
                    write: vec![line],
                    close_stdin: false,
                }
            }
            other => events(vec![AgentEvent::CommandRejected {
                command: other.command().into(),
                message,
            }]),
        }
    }

    fn on_prompt_result(&mut self, result: &Value) -> Step {
        let stop = s(result, "stopReason");
        let cancelled = stop == "cancelled";
        let end_reason = match stop {
            "cancelled" => Some(EndReason::Cancelled),
            "max_turn_requests" => Some(EndReason::MaxTurns),
            "refusal" => Some(EndReason::ExecutionError),
            _ => None,
        };
        let usage = result.get("usage").cloned().unwrap_or(Value::Null);
        self.prompt_active = false;
        self.cancel_sent = false;
        let mut step = Step::default();
        for g in std::mem::take(&mut self.gates) {
            step.events.push(AgentEvent::GateResolved {
                request_id: g.routing_id,
                reason: None,
            });
        }
        step.events.push(AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted: cancelled,
            num_turns: 0,
            usage: Usage {
                input_tokens: u(&usage, "inputTokens"),
                output_tokens: u(&usage, "outputTokens"),
                cache_read_tokens: u(&usage, "cachedReadTokens"),
                cache_creation_tokens: u(&usage, "cachedWriteTokens"),
                cost_usd: 0.0,
            },
            per_model: HashMap::new(),
            reply: None,
        });
        step.extend(self.drain());
        step
    }

    /// Events for a changed `configOptions` set (only what changed).
    fn on_config(&mut self, container: &Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();
        let (_, model) = config_models(container);
        if let Some(m) = model {
            if self.model.as_deref() != Some(m.as_str()) {
                self.model = Some(m.clone());
                out.push(AgentEvent::ModelChanged { model: m });
            }
        }
        let (_, mode) = config_modes(container);
        if let Some(m) = mode {
            if self.mode.as_deref() != Some(m.as_str()) {
                self.mode = Some(m.clone());
                out.push(AgentEvent::ModesUpdated {
                    available_modes: vec![],
                    current_mode: Some(m),
                });
            }
        }
        out
    }

    fn on_request(&mut self, id: Value, method: String, params: Value) -> Step {
        if method != "session/request_permission" {
            // We declare no client capabilities (fs, terminal), so these are
            // unexpected; answer with an error so the agent never waits.
            let raw = json!({"id": id, "method": method, "params": params});
            return Step {
                events: vec![AgentEvent::Unknown { raw }],
                write: vec![rpc_error(
                    &id,
                    -32601,
                    &format!("method not supported: {method}"),
                )],
                close_stdin: false,
            };
        }
        let tool = params.get("toolCall").cloned().unwrap_or(Value::Null);
        let routing_id =
            opt_s(&tool, "toolCallId").unwrap_or_else(|| format!("acp:{}", id_text(&id)));
        let choices: Vec<GateChoice> = params
            .get("options")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(acp_choice).collect())
            .unwrap_or_default();
        let kind = s(&tool, "kind");
        let title = s(&tool, "title");
        let gate = GateRequest::Permission {
            request_id: routing_id.clone(),
            tool_name: self
                .tools
                .get(&routing_id)
                .cloned()
                .unwrap_or_else(|| if kind.is_empty() { "tool" } else { kind }.to_string()),
            command: title.to_string(),
            cwd: tool
                .get("rawInput")
                .map(|r| s(r, "cwd").to_string())
                .unwrap_or_default(),
            explanation: if kind.is_empty() {
                title.to_string()
            } else {
                format!("{kind}: {title}")
            },
            choices: choices.clone(),
            raw_input: tool.get("rawInput").cloned().unwrap_or(Value::Null),
        };
        self.gates.push(PendingGate {
            routing_id,
            raw_id: id,
            choices,
        });
        events(vec![AgentEvent::GateRequested { gate }])
    }

    fn on_update(&mut self, params: &Value) -> Vec<AgentEvent> {
        let Some(update) = params.get("update") else {
            return vec![];
        };
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.loading
            && matches!(
                kind,
                "user_message_chunk"
                    | "agent_message_chunk"
                    | "agent_thought_chunk"
                    | "tool_call"
                    | "tool_call_update"
                    | "plan"
                    | "usage_update"
            )
        {
            // History replayed by `session/load`, not live activity.
            return vec![];
        }
        let message_id = opt_s(update, "messageId");
        match kind {
            "agent_message_chunk" => text_of(update)
                .map(|text| AgentEvent::AssistantDelta { text, message_id })
                .into_iter()
                .collect(),
            "agent_thought_chunk" => text_of(update)
                .map(|text| AgentEvent::ThinkingDelta { text, message_id })
                .into_iter()
                .collect(),
            "user_message_chunk" => vec![],
            "tool_call" => self.on_tool_call(update),
            "tool_call_update" => self.on_tool_update(update),
            "usage_update" => {
                let cost = update.get("cost").cloned().unwrap_or(Value::Null);
                let cost_usd = if s(&cost, "currency") == "USD" {
                    cost.get("amount").and_then(Value::as_f64).unwrap_or(0.0)
                } else {
                    0.0
                };
                let size = u(update, "size");
                vec![AgentEvent::UsageUpdate {
                    usage: Usage {
                        cost_usd,
                        ..Usage::default()
                    },
                    context_pct: (size > 0)
                        .then(|| (u(update, "used") as f64 * 100.0 / size as f64) as f32),
                }]
            }
            "plan" => vec![AgentEvent::PlanUpdated {
                entries: update
                    .get("entries")
                    .or_else(|| update.get("plan").and_then(|p| p.get("entries")))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .map(|e| PlanEntry {
                                content: s(e, "content").to_string(),
                                priority: s(e, "priority").to_string(),
                                status: s(e, "status").to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }],
            "available_commands_update" => vec![AgentEvent::SlashCommandsUpdated {
                commands: update
                    .get("availableCommands")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|c| {
                                Some(SlashCommandInfo {
                                    name: opt_s(c, "name")?,
                                    description: s(c, "description").to_string(),
                                    argument_hint: c
                                        .get("input")
                                        .map(|i| s(i, "hint").to_string())
                                        .unwrap_or_default(),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }],
            "current_mode_update" => match opt_s(update, "currentModeId") {
                Some(m) if self.mode.as_deref() != Some(m.as_str()) => {
                    self.mode = Some(m.clone());
                    vec![AgentEvent::ModesUpdated {
                        available_modes: vec![],
                        current_mode: Some(m),
                    }]
                }
                _ => vec![],
            },
            "config_option_update" => self.on_config(update),
            "context_compaction" => vec![AgentEvent::Compaction {
                info: CompactionInfo::default(),
            }],
            _ => vec![AgentEvent::Unknown {
                raw: update.clone(),
            }],
        }
    }

    fn on_tool_call(&mut self, update: &Value) -> Vec<AgentEvent> {
        let id = s(update, "toolCallId").to_string();
        let title = s(update, "title");
        let name = if title.is_empty() {
            s(update, "kind").to_string()
        } else {
            title.to_string()
        };
        if name == "task" {
            // Subagents are announced once their description is known.
            self.subagents.insert(id);
            return vec![];
        }
        self.tools.insert(id.clone(), name.clone());
        vec![AgentEvent::ToolCallStarted {
            name,
            call_id: id,
            input: update.get("rawInput").cloned().unwrap_or(Value::Null),
            is_subagent: false,
            subagent_model: None,
        }]
    }

    fn on_tool_update(&mut self, update: &Value) -> Vec<AgentEvent> {
        let id = s(update, "toolCallId").to_string();
        let status = s(update, "status");
        let content: Vec<Value> = update
            .get("content")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let terminal = matches!(status, "completed" | "failed");
        if self.subagents.contains(&id) {
            return self.on_subagent_update(&id, status, update, content);
        }
        let name = self.tools.get(&id).cloned().unwrap_or_else(|| {
            let t = s(update, "title");
            if t.is_empty() {
                s(update, "kind").to_string()
            } else {
                t.to_string()
            }
        });
        if terminal {
            self.tools.remove(&id);
            return vec![AgentEvent::ToolCallFinished {
                call_id: id,
                name,
                ok: status == "completed",
                result: (!content.is_empty()).then_some(Value::Array(content)),
            }];
        }
        // The arguments come in an update (the start carries only `cwd`, say).
        let input = update
            .get("rawInput")
            .filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
            .cloned();
        vec![AgentEvent::ToolCallUpdated {
            call_id: id,
            name,
            content,
            status: (!status.is_empty()).then(|| status.to_string()),
            input,
        }]
    }

    fn on_subagent_update(
        &mut self,
        id: &str,
        status: &str,
        update: &Value,
        content: Vec<Value>,
    ) -> Vec<AgentEvent> {
        let raw_input = update.get("rawInput").cloned().unwrap_or(Value::Null);
        let name = opt_s(&raw_input, "description")
            .or_else(|| opt_s(update, "title").filter(|t| t != "task"));
        let mut out = Vec::new();
        let terminal = matches!(status, "completed" | "failed");
        if (name.is_some() || terminal) && self.subagents_started.insert(id.to_string()) {
            out.push(AgentEvent::ToolCallStarted {
                name: name.clone().unwrap_or_else(|| "subagent".into()),
                call_id: id.to_string(),
                input: raw_input,
                is_subagent: true,
                subagent_model: None,
            });
        }
        if terminal {
            self.subagents.remove(id);
            self.subagents_started.remove(id);
            out.push(AgentEvent::ToolCallFinished {
                call_id: id.to_string(),
                name: name.unwrap_or_else(|| "subagent".into()),
                ok: status == "completed",
                result: (!content.is_empty()).then_some(Value::Array(content)),
            });
        }
        out
    }
}

impl Protocol for AcpProtocol {
    fn start(&mut self, spec: &SpawnSpec) -> Step {
        self.spec = spec.clone();
        self.phase = Phase::Handshaking;
        if let Some(p) = &spec.first_prompt {
            self.queue.push_back(Command::Prompt { text: p.clone() });
        }
        let line = self.request(
            Req::Initialize,
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {"name": self.client_name, "version": env!("CARGO_PKG_VERSION")},
            }),
        );
        Step {
            write: vec![line],
            ..Step::default()
        }
    }

    fn on_line(&mut self, line: &str) -> Step {
        let line = line.trim();
        if line.is_empty() {
            return Step::default();
        }
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return events(vec![AgentEvent::Unknown {
                raw: Value::String(line.to_string()),
            }]);
        };
        match RpcIn::classify(&v) {
            Some(RpcIn::Response { id, result }) => self.on_response(&id, result),
            Some(RpcIn::Error { id, message, .. }) => self.on_error(&id, message),
            Some(RpcIn::Request { id, method, params }) => self.on_request(id, method, params),
            Some(RpcIn::Notification { method, params }) if method == "session/update" => {
                events(self.on_update(&params))
            }
            _ => events(vec![AgentEvent::Unknown { raw: v }]),
        }
    }

    fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    fn encode(&mut self, cmd: Command) -> Result<Encoded, DriverError> {
        if self.phase == Phase::Closed {
            return Err(DriverError::Shutdown);
        }
        if matches!(cmd, Command::Interrupt) && self.phase != Phase::Ready {
            self.queue
                .retain(|c| !matches!(c, Command::Prompt { .. } | Command::RunSlash { .. }));
            return Ok(Encoded::Sent(Step::default()));
        }
        if self.must_queue(&cmd) {
            self.queue.push_back(cmd);
            return Ok(Encoded::Queued);
        }
        self.encode_now(cmd).map(Encoded::Sent)
    }
}

fn events(events: Vec<AgentEvent>) -> Step {
    Step {
        events,
        ..Step::default()
    }
}

fn text_of(update: &Value) -> Option<String> {
    let c = update.get("content")?;
    if s(c, "type") != "text" {
        return None;
    }
    opt_s(c, "text")
}

fn config_group<'a>(container: &'a Value, id: &str) -> Option<&'a Value> {
    container
        .get("configOptions")
        .and_then(Value::as_array)?
        .iter()
        .find(|o| s(o, "id") == id)
}

/// `(available modes, current mode)` from a `configOptions` container.
pub fn config_modes(container: &Value) -> (Vec<SessionMode>, Option<String>) {
    let Some(g) = config_group(container, "mode") else {
        return (vec![], None);
    };
    let modes = g
        .get("options")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|o| {
                    let id = opt_s(o, "value")?;
                    Some(SessionMode {
                        label: opt_s(o, "name").unwrap_or_else(|| id.clone()),
                        description: s(o, "description").to_string(),
                        id,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    (modes, opt_s(g, "currentValue"))
}

/// `(available models, current model)` from a `configOptions` container.
pub fn config_models(container: &Value) -> (Vec<ModelInfo>, Option<String>) {
    let Some(g) = config_group(container, "model") else {
        return (vec![], None);
    };
    let models = g
        .get("options")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|o| {
                    let id = opt_s(o, "value")?;
                    Some(ModelInfo {
                        display_name: opt_s(o, "name").unwrap_or_else(|| id.clone()),
                        description: s(o, "description").to_string(),
                        is_default: false,
                        id,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    (models, opt_s(g, "currentValue"))
}

/// One ACP `PermissionOption` (`{optionId, name, kind}`) as a choice: the agent's own id,
/// wording and kind.
fn acp_choice(o: &Value) -> Option<GateChoice> {
    let id = opt_s(o, "optionId")?;
    let kind = match s(o, "kind") {
        "allow_always" => GateChoiceKind::AllowAlways,
        "reject_once" => GateChoiceKind::RejectOnce,
        "reject_always" => GateChoiceKind::RejectAlways,
        // `allow_once`; an unknown kind is treated as the narrowest allow.
        _ => GateChoiceKind::AllowOnce,
    };
    Some(match opt_s(o, "name") {
        Some(name) => GateChoice::agent(id, name, kind),
        None => GateChoice::neutral(id.clone(), id, kind),
    })
}

/// `Ok` when `decision` names one of `choices`.
fn check_choice(
    choices: &[GateChoice],
    request_id: &str,
    decision: &GateDecision,
) -> Result<(), String> {
    if choices.iter().any(|c| c.id == decision.choice) {
        Ok(())
    } else {
        Err(format!(
            "gate {request_id} offers no option {:?} (offers: {})",
            decision.choice,
            choices
                .iter()
                .map(|c| c.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

fn selected(id: &Value, option: &str) -> OutLine {
    rpc_result(
        id,
        json!({"outcome": {"outcome": "selected", "optionId": option}}),
    )
}

fn cancelled(id: &Value) -> OutLine {
    rpc_result(id, json!({"outcome": {"outcome": "cancelled"}}))
}
