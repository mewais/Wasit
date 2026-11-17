//! The claude stream-json control protocol as a pure [`Protocol`].
//!
//! Wire facts (verified against claude 2.1.326):
//! - We open with a `control_request {subtype:"initialize"}`. Claude may
//!   print unrelated lines first (`active_goal`, `autocompact_state`); they
//!   are reported as [`AgentEvent::Unknown`]. The `initialize` response
//!   carries the slash commands, the model catalog and the current
//!   permission mode.
//! - `system/init` is printed at the start of EVERY turn; only the first one
//!   becomes [`AgentEvent::SessionInit`], later ones only report a model
//!   change.
//! - A user message is `{"type":"user","message":{"role":"user","content":[…]}}`.
//!   A message whose text starts with `/name` runs that slash command / skill.
//!   Claude merges user messages written while a turn is starting into one
//!   turn, so this protocol holds prompts while a turn runs and sends the next
//!   one after the `result` line (one prompt = one turn).
//! - Tool permission gates arrive as `control_request {subtype:"can_use_tool"}`
//!   (also `AskUserQuestion` / `ExitPlanMode` by tool name) or
//!   `{subtype:"elicitation"}`; they are answered with `control_response`.
//! - `control_request {subtype:"interrupt"}` ends the running turn with a
//!   `result {subtype:"error_during_execution"}`; the result carries no
//!   interrupt flag, so the protocol remembers that it asked.
//! - `set_model` / `set_permission_mode` control requests switch model / mode;
//!   errors come back as `control_response {subtype:"error"}`.

use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};
use wasit_core::wire::{opt_s, s, u};
use wasit_core::{
    AgentEvent, Command, CompactionInfo, DriverError, Encoded, EndReason, GateDecision,
    GateRequest, McpServerInfo, ModelInfo, ModelUsage, OutLine, Protocol, QuestionAnswer,
    QuestionOption, QuestionPrompt, RateLimitInfo, SlashCommandInfo, SpawnSpec, Step, Usage,
};

/// What a pending gate needs to encode its answer.
#[derive(Debug, Clone, PartialEq)]
enum GateKind {
    /// `can_use_tool` (incl. `ExitPlanMode`); holds the original input.
    Tool { input: Value },
    /// `AskUserQuestion`; holds the original input (its `questions`).
    Question { input: Value },
    /// MCP elicitation.
    Elicitation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// `start` not called yet.
    New,
    /// `initialize` sent, response not seen.
    Initializing,
    Ready,
    /// `Shutdown` encoded.
    Closed,
}

/// The claude protocol state machine. Create with [`ClaudeProtocol::new`].
#[derive(Debug)]
pub struct ClaudeProtocol {
    phase: Phase,
    next_request: u64,
    init_request_id: String,
    /// Our outstanding control requests: request id → command name.
    outstanding: HashMap<String, &'static str>,
    /// Commands waiting for the handshake or for the running turn to end.
    queue: VecDeque<Command>,
    /// A turn is running (prompt written, `result` not seen).
    busy: bool,
    /// We sent `interrupt` during the running turn.
    interrupt_sent: bool,
    session_init_sent: bool,
    model: Option<String>,
    /// Pending gates in arrival order.
    gates: Vec<(String, GateKind)>,
    tool_names: HashMap<String, String>,
    last_usage_message: Option<String>,
    context_window: Option<u64>,
}

impl Default for ClaudeProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeProtocol {
    pub fn new() -> Self {
        ClaudeProtocol {
            phase: Phase::New,
            next_request: 1,
            init_request_id: String::new(),
            outstanding: HashMap::new(),
            queue: VecDeque::new(),
            busy: false,
            interrupt_sent: false,
            session_init_sent: false,
            model: None,
            gates: Vec::new(),
            tool_names: HashMap::new(),
            last_usage_message: None,
            context_window: None,
        }
    }

    fn request_id(&mut self) -> String {
        let id = format!("req_{}", self.next_request);
        self.next_request += 1;
        id
    }

    fn control_request(&mut self, command: &'static str, request: Value) -> OutLine {
        let id = self.request_id();
        self.outstanding.insert(id.clone(), command);
        OutLine::json(&json!({"type": "control_request", "request_id": id, "request": request}))
    }

    // ── command encoding ────────────────────────────────────────────────────

    /// Whether `cmd` must wait in the queue right now.
    fn must_queue(&self, cmd: &Command) -> bool {
        match self.phase {
            Phase::New | Phase::Initializing => matches!(
                cmd,
                Command::Prompt { .. }
                    | Command::RunSlash { .. }
                    | Command::SetModel { .. }
                    | Command::SetMode { .. }
            ),
            Phase::Ready => {
                matches!(cmd, Command::Prompt { .. } | Command::RunSlash { .. })
                    && (self.busy || !self.queue.is_empty())
            }
            Phase::Closed => false,
        }
    }

    fn encode_now(&mut self, cmd: Command) -> Result<Step, DriverError> {
        match cmd {
            Command::Prompt { text } => Ok(self.write_prompt(&text)),
            Command::RunSlash { name, args } => {
                let name = name.trim_start_matches('/');
                let text = if args.trim().is_empty() {
                    format!("/{name}")
                } else {
                    format!("/{name} {}", args.trim())
                };
                Ok(self.write_prompt(&text))
            }
            Command::AnswerGate {
                request_id,
                decision,
            } => {
                let kind = self.take_gate(&request_id)?;
                let line = match (&kind, decision) {
                    (GateKind::Question { .. }, _) => {
                        self.gates.push((request_id.clone(), kind));
                        return Err(DriverError::Invalid {
                            detail: format!(
                                "gate {request_id} is a question; answer it with answer_question"
                            ),
                        });
                    }
                    (
                        GateKind::Tool { input },
                        GateDecision::Approve {
                            updated_input,
                            permission_updates,
                            ..
                        },
                    ) => encode_allow(
                        &request_id,
                        updated_input.unwrap_or_else(|| input.clone()),
                        permission_updates,
                    ),
                    (
                        GateKind::Elicitation,
                        GateDecision::Approve {
                            elicitation_content,
                            ..
                        },
                    ) => encode_elicitation_accept(&request_id, elicitation_content),
                    (GateKind::Tool { .. }, GateDecision::Deny { reason }) => {
                        encode_deny(&request_id, reason.as_deref().unwrap_or("denied by user"))
                    }
                    (GateKind::Elicitation, GateDecision::Deny { .. }) => {
                        encode_elicitation_decline(&request_id)
                    }
                };
                Ok(Step {
                    write: vec![line],
                    ..Step::default()
                })
            }
            Command::AnswerQuestion { request_id, answer } => {
                let kind = self.take_gate(&request_id)?;
                match kind {
                    GateKind::Question { input } => Ok(Step {
                        write: vec![encode_question_answer(&request_id, &input, &answer)],
                        ..Step::default()
                    }),
                    other => {
                        self.gates.push((request_id.clone(), other));
                        Err(DriverError::Invalid {
                            detail: format!(
                                "gate {request_id} is not a question; answer it with answer_gate"
                            ),
                        })
                    }
                }
            }
            Command::Interrupt => Ok(self.interrupt()),
            Command::SetModel { model } => {
                let line = self
                    .control_request("set_model", json!({"subtype": "set_model", "model": model}));
                Ok(Step {
                    write: vec![line],
                    ..Step::default()
                })
            }
            Command::SetMode { mode } => {
                let line = self.control_request(
                    "set_mode",
                    json!({"subtype": "set_permission_mode", "mode": mode}),
                );
                Ok(Step {
                    write: vec![line],
                    ..Step::default()
                })
            }
            Command::Shutdown => {
                self.phase = Phase::Closed;
                self.queue.clear();
                Ok(Step {
                    close_stdin: true,
                    ..Step::default()
                })
            }
        }
    }

    fn write_prompt(&mut self, text: &str) -> Step {
        self.busy = true;
        self.interrupt_sent = false;
        Step {
            events: vec![AgentEvent::TurnStarted {
                native_turn_id: None,
            }],
            write: vec![encode_user_message(text)],
            close_stdin: false,
        }
    }

    fn take_gate(&mut self, request_id: &str) -> Result<GateKind, DriverError> {
        let pos = self
            .gates
            .iter()
            .position(|(id, _)| id == request_id)
            .ok_or_else(|| DriverError::NotFound {
                detail: format!("no pending gate {request_id}"),
            })?;
        Ok(self.gates.remove(pos).1)
    }

    /// Interrupt: drop prompts that never started, decline pending gates,
    /// then ask claude to stop the turn.
    fn interrupt(&mut self) -> Step {
        let mut step = Step::default();
        self.queue
            .retain(|c| !matches!(c, Command::Prompt { .. } | Command::RunSlash { .. }));
        for (id, kind) in std::mem::take(&mut self.gates) {
            let line = match kind {
                GateKind::Elicitation => encode_elicitation_decline(&id),
                GateKind::Tool { .. } | GateKind::Question { .. } => {
                    encode_deny(&id, "interrupted by user")
                }
            };
            step.write.push(line);
            step.events
                .push(AgentEvent::GateResolved { request_id: id });
        }
        if self.busy && !self.interrupt_sent {
            self.interrupt_sent = true;
            let line = self.control_request("interrupt", json!({"subtype": "interrupt"}));
            step.write.push(line);
        }
        step
    }

    /// Send queued commands that may go now, in order.
    fn drain(&mut self) -> Step {
        let mut step = Step::default();
        while let Some(cmd) = self.queue.front() {
            let is_prompt = matches!(cmd, Command::Prompt { .. } | Command::RunSlash { .. });
            if self.phase != Phase::Ready || (is_prompt && self.busy) {
                break;
            }
            let cmd = self.queue.pop_front().expect("front exists");
            match self.encode_now(cmd) {
                Ok(s) => step.extend(s),
                Err(e) => step.events.push(AgentEvent::CommandRejected {
                    command: "queued".into(),
                    message: e.to_string(),
                }),
            }
        }
        step
    }

    // ── line parsing ────────────────────────────────────────────────────────

    fn on_value(&mut self, v: Value) -> Step {
        let ty = s(&v, "type").to_string();
        match ty.as_str() {
            "control_response" => self.on_control_response(&v),
            "control_request" => self.on_control_request(v),
            "control_cancel_request" => {
                let id = s(&v, "request_id").to_string();
                match self.take_gate(&id) {
                    Ok(_) => Step {
                        events: vec![AgentEvent::GateResolved { request_id: id }],
                        ..Step::default()
                    },
                    Err(_) => events(vec![AgentEvent::Unknown { raw: v }]),
                }
            }
            "system" => events(self.on_system(v)),
            "assistant" => events(self.on_assistant(&v)),
            "user" => events(self.on_user(&v)),
            "result" => self.on_result(&v),
            "rate_limit_event" => events(vec![rate_limit(&v)]),
            _ => events(vec![AgentEvent::Unknown { raw: v }]),
        }
    }

    fn on_control_response(&mut self, v: &Value) -> Step {
        let resp = v.get("response").cloned().unwrap_or(Value::Null);
        let id = s(&resp, "request_id").to_string();
        let ok = s(&resp, "subtype") == "success";
        let payload = resp.get("response").cloned().unwrap_or(Value::Null);
        if id == self.init_request_id && self.phase == Phase::Initializing {
            self.phase = Phase::Ready;
            let mut step = Step::default();
            if ok {
                step.events.extend(init_response_events(&payload));
            } else {
                step.events.push(AgentEvent::CommandRejected {
                    command: "initialize".into(),
                    message: error_text(&resp),
                });
            }
            step.extend(self.drain());
            return step;
        }
        let Some(command) = self.outstanding.remove(&id) else {
            return events(vec![AgentEvent::Unknown { raw: v.clone() }]);
        };
        if !ok {
            return events(vec![AgentEvent::CommandRejected {
                command: command.into(),
                message: error_text(&resp),
            }]);
        }
        match command {
            "set_model" => match opt_s(&payload, "model") {
                Some(m) => {
                    self.model = Some(m.clone());
                    events(vec![AgentEvent::ModelChanged { model: m }])
                }
                None => Step::default(),
            },
            // `set_permission_mode` is confirmed by a `system/status` line;
            // `interrupt` by the `result` line.
            _ => Step::default(),
        }
    }

    fn on_control_request(&mut self, v: Value) -> Step {
        let id = s(&v, "request_id").to_string();
        let req = v.get("request").cloned().unwrap_or(Value::Null);
        match s(&req, "subtype") {
            "can_use_tool" => {
                let (gate, kind) = tool_gate(&id, &req);
                self.gates.push((id, kind));
                events(vec![AgentEvent::GateRequested { gate }])
            }
            "elicitation" => {
                let gate = elicitation_gate(&id, &req);
                self.gates.push((id, GateKind::Elicitation));
                events(vec![AgentEvent::GateRequested { gate }])
            }
            other => {
                // Hook callbacks / SDK MCP messages are features this host
                // does not register; answer with an error so claude is never
                // left waiting on us.
                let line = OutLine::json(&json!({
                    "type": "control_response",
                    "response": {
                        "subtype": "error",
                        "request_id": id,
                        "error": format!("unsupported control request: {other}"),
                    }
                }));
                Step {
                    events: vec![AgentEvent::Unknown { raw: v }],
                    write: vec![line],
                    close_stdin: false,
                }
            }
        }
    }

    fn on_system(&mut self, v: Value) -> Vec<AgentEvent> {
        match s(&v, "subtype") {
            "init" => {
                let model = opt_s(&v, "model");
                if self.session_init_sent {
                    if model.is_some() && model != self.model {
                        self.model = model.clone();
                        return vec![AgentEvent::ModelChanged {
                            model: model.unwrap_or_default(),
                        }];
                    }
                    return vec![];
                }
                self.session_init_sent = true;
                self.model = model.clone();
                vec![session_init(&v)]
            }
            "status" => match opt_s(&v, "permissionMode") {
                Some(mode) => vec![AgentEvent::ModesUpdated {
                    available_modes: vec![],
                    current_mode: Some(mode),
                }],
                None => vec![AgentEvent::Unknown { raw: v }],
            },
            "compact_boundary" => {
                let m = v.get("compact_metadata").cloned().unwrap_or(Value::Null);
                vec![AgentEvent::Compaction {
                    info: CompactionInfo {
                        trigger: s(&m, "trigger").to_string(),
                        pre_tokens: u(&m, "pre_tokens"),
                        post_tokens: u(&m, "post_tokens"),
                        cumulative_dropped_tokens: u(&m, "cumulative_dropped_tokens"),
                        duration_ms: u(&m, "duration_ms"),
                    },
                }]
            }
            _ => vec![AgentEvent::Unknown { raw: v }],
        }
    }

    fn on_assistant(&mut self, v: &Value) -> Vec<AgentEvent> {
        let Some(msg) = v.get("message") else {
            return vec![AgentEvent::Unknown { raw: v.clone() }];
        };
        let message_id = opt_s(msg, "id");
        let mut out = Vec::new();
        for block in msg
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            match s(block, "type") {
                "text" => {
                    let text = s(block, "text");
                    if !text.is_empty() {
                        out.push(AgentEvent::AssistantDelta {
                            text: text.to_string(),
                            message_id: message_id.clone(),
                        });
                    }
                }
                "thinking" => {
                    let text = s(block, "thinking");
                    if !text.is_empty() {
                        out.push(AgentEvent::ThinkingDelta {
                            text: text.to_string(),
                            message_id: message_id.clone(),
                        });
                    }
                }
                "tool_use" => {
                    let call_id = s(block, "id").to_string();
                    let name = s(block, "name").to_string();
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    self.tool_names.insert(call_id.clone(), name.clone());
                    let is_subagent = name == "Task" || name == "Agent";
                    let subagent_model = if is_subagent {
                        opt_s(&input, "model")
                    } else {
                        None
                    };
                    out.push(AgentEvent::ToolCallStarted {
                        name,
                        call_id,
                        input,
                        is_subagent,
                        subagent_model,
                    });
                }
                _ => {}
            }
        }
        // One API message is printed as several `assistant` lines (one per
        // content block) that repeat the same usage: report it once.
        if let Some(usage) = msg.get("usage") {
            if message_id.is_none() || message_id != self.last_usage_message {
                self.last_usage_message = message_id;
                let usage = parse_usage(usage);
                let context_pct = self.context_window.filter(|w| *w > 0).map(|w| {
                    let used =
                        usage.input_tokens + usage.cache_read_tokens + usage.cache_creation_tokens;
                    (used as f64 * 100.0 / w as f64) as f32
                });
                out.push(AgentEvent::UsageUpdate { usage, context_pct });
            }
        }
        out
    }

    fn on_user(&mut self, v: &Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();
        let content = v
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array);
        for block in content.into_iter().flatten() {
            if s(block, "type") == "tool_result" {
                let call_id = s(block, "tool_use_id").to_string();
                let name = self.tool_names.remove(&call_id).unwrap_or_default();
                out.push(AgentEvent::ToolCallFinished {
                    call_id,
                    name,
                    ok: !block
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    result: block.get("content").cloned(),
                });
            }
        }
        out
    }

    fn on_result(&mut self, v: &Value) -> Step {
        let is_error = v.get("is_error").and_then(Value::as_bool).unwrap_or(false);
        let interrupted = self.interrupt_sent && (is_error || s(v, "subtype") != "success");
        let end_reason = if interrupted {
            Some(EndReason::Cancelled)
        } else if s(v, "subtype") == "error_max_turns" {
            Some(EndReason::MaxTurns)
        } else if is_error {
            Some(EndReason::ExecutionError)
        } else {
            None
        };
        let mut per_model = HashMap::new();
        let mut usage = Usage::default();
        if let Some(map) = v.get("modelUsage").and_then(Value::as_object) {
            for (model, row) in map {
                let mu = ModelUsage {
                    input_tokens: u(row, "inputTokens"),
                    output_tokens: u(row, "outputTokens"),
                    cost_usd: row.get("costUSD").and_then(Value::as_f64).unwrap_or(0.0),
                    estimated: false,
                };
                usage.input_tokens += mu.input_tokens;
                usage.output_tokens += mu.output_tokens;
                usage.cache_read_tokens += u(row, "cacheReadInputTokens");
                usage.cache_creation_tokens += u(row, "cacheCreationInputTokens");
                if Some(model) == self.model.as_ref() {
                    let w = u(row, "contextWindow");
                    if w > 0 {
                        self.context_window = Some(w);
                    }
                }
                per_model.insert(model.clone(), mu);
            }
        }
        usage.cost_usd = v
            .get("total_cost_usd")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let reply = opt_s(v, "result").or_else(|| {
            v.get("errors")
                .and_then(Value::as_array)
                .map(|e| {
                    e.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .filter(|s| !s.is_empty() && is_error && !interrupted)
        });
        self.busy = false;
        self.interrupt_sent = false;
        let mut step = events(vec![AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted: interrupted,
            num_turns: u(v, "num_turns"),
            usage,
            per_model,
            reply,
        }]);
        step.extend(self.drain());
        step
    }
}

impl Protocol for ClaudeProtocol {
    fn start(&mut self, spec: &SpawnSpec) -> Step {
        self.phase = Phase::Initializing;
        self.model = spec.model.clone();
        self.init_request_id = self.request_id();
        let init = OutLine::json(&json!({
            "type": "control_request",
            "request_id": self.init_request_id,
            "request": {"subtype": "initialize", "hooks": null},
        }));
        if let Some(p) = &spec.first_prompt {
            self.queue.push_back(Command::Prompt { text: p.clone() });
        }
        Step {
            write: vec![init],
            ..Step::default()
        }
    }

    fn on_line(&mut self, line: &str) -> Step {
        let line = line.trim();
        if line.is_empty() {
            return Step::default();
        }
        match serde_json::from_str::<Value>(line) {
            Ok(v) if v.is_object() => self.on_value(v),
            _ => events(vec![AgentEvent::Unknown {
                raw: Value::String(line.to_string()),
            }]),
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
            // Nothing is running yet: drop queued prompts.
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

fn error_text(resp: &Value) -> String {
    match resp.get("error") {
        Some(Value::String(e)) => e.clone(),
        Some(other) => other.to_string(),
        None => "control request failed".to_string(),
    }
}

/// Events derived from the `initialize` response: slash commands, model
/// catalog, current permission mode.
fn init_response_events(p: &Value) -> Vec<AgentEvent> {
    let mut out = Vec::new();
    if let Some(cmds) = p.get("commands").and_then(Value::as_array) {
        out.push(AgentEvent::SlashCommandsUpdated {
            commands: cmds.iter().filter_map(slash_command).collect(),
        });
    }
    if let Some(models) = p.get("models").and_then(Value::as_array) {
        out.push(AgentEvent::ModelsUpdated {
            models: models
                .iter()
                .filter_map(|m| {
                    let id = opt_s(m, "value")?;
                    Some(ModelInfo {
                        display_name: opt_s(m, "displayName").unwrap_or_else(|| id.clone()),
                        description: s(m, "description").to_string(),
                        is_default: id == "default",
                        id,
                    })
                })
                .collect(),
            current: None,
        });
    }
    if let Some(mode) = opt_s(p, "current_permission_mode") {
        out.push(AgentEvent::ModesUpdated {
            available_modes: vec![],
            current_mode: Some(mode),
        });
    }
    out
}

fn slash_command(v: &Value) -> Option<SlashCommandInfo> {
    if let Some(name) = v.as_str() {
        return Some(SlashCommandInfo {
            name: name.trim_start_matches('/').to_string(),
            ..SlashCommandInfo::default()
        });
    }
    Some(SlashCommandInfo {
        name: opt_s(v, "name")?.trim_start_matches('/').to_string(),
        description: s(v, "description").to_string(),
        argument_hint: s(v, "argumentHint").to_string(),
    })
}

fn session_init(v: &Value) -> AgentEvent {
    let strings = |key: &str| -> Vec<String> {
        v.get(key)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(str::to_string).or_else(|| opt_s(t, "name")))
                    .collect()
            })
            .unwrap_or_default()
    };
    AgentEvent::SessionInit {
        native_session_id: opt_s(v, "session_id"),
        model: opt_s(v, "model"),
        cwd: opt_s(v, "cwd"),
        tools: strings("tools"),
        mcp_servers: v
            .get("mcp_servers")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|m| {
                        Some(McpServerInfo {
                            name: opt_s(m, "name")?,
                            status: s(m, "status").to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        slash_commands: v
            .get("slash_commands")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(slash_command).collect())
            .unwrap_or_default(),
        available_modes: vec![],
        current_mode: s(v, "permissionMode").to_string(),
        cli_version: s(v, "claude_code_version").to_string(),
    }
}

fn tool_gate(id: &str, req: &Value) -> (GateRequest, GateKind) {
    let tool_name = s(req, "tool_name").to_string();
    let input = req.get("input").cloned().unwrap_or(Value::Null);
    match tool_name.as_str() {
        "ExitPlanMode" => (
            GateRequest::Plan {
                request_id: id.to_string(),
                summary: opt_s(&input, "plan")
                    .or_else(|| opt_s(&input, "content"))
                    .unwrap_or_default(),
            },
            GateKind::Tool { input },
        ),
        "AskUserQuestion" => {
            let questions = input
                .get("questions")
                .and_then(Value::as_array)
                .map(|a| a.iter().map(question_prompt).collect())
                .unwrap_or_default();
            (
                GateRequest::Question {
                    request_id: id.to_string(),
                    questions,
                },
                GateKind::Question { input },
            )
        }
        _ => {
            let command = opt_s(&input, "command")
                .or_else(|| opt_s(&input, "file_path"))
                .or_else(|| opt_s(&input, "description"))
                .or_else(|| opt_s(req, "description"))
                .unwrap_or_default();
            let gate = GateRequest::Permission {
                request_id: id.to_string(),
                tool_name,
                command,
                cwd: s(&input, "cwd").to_string(),
                explanation: opt_s(&input, "description")
                    .or_else(|| opt_s(req, "description"))
                    .unwrap_or_default(),
                is_elicitation: false,
                elicitation_choices: vec![],
                permission_suggestions: req
                    .get("permission_suggestions")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                raw_input: input.clone(),
            };
            (gate, GateKind::Tool { input })
        }
    }
}

fn question_prompt(q: &Value) -> QuestionPrompt {
    QuestionPrompt {
        question: s(q, "question").to_string(),
        header: s(q, "header").to_string(),
        multi_select: q
            .get("multiSelect")
            .or_else(|| q.get("multi_select"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        options: q
            .get("options")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .map(|o| QuestionOption {
                        label: s(o, "label").to_string(),
                        description: s(o, "description").to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn elicitation_gate(id: &str, req: &Value) -> GateRequest {
    let message = s(req, "message").to_string();
    GateRequest::Permission {
        request_id: id.to_string(),
        tool_name: opt_s(req, "mcp_server_name")
            .or_else(|| opt_s(req, "server_name"))
            .unwrap_or_default(),
        command: message.clone(),
        cwd: String::new(),
        explanation: message,
        is_elicitation: true,
        elicitation_choices: wasit_core::wire::elicitation_choices(req.get("requested_schema")),
        permission_suggestions: vec![],
        raw_input: Value::Null,
    }
}

fn rate_limit(v: &Value) -> AgentEvent {
    let i = v.get("rate_limit_info").cloned().unwrap_or(Value::Null);
    AgentEvent::RateLimitNotice {
        info: RateLimitInfo {
            status: s(&i, "status").to_string(),
            resets_at: u(&i, "resetsAt"),
            rate_limit_type: s(&i, "rateLimitType").to_string(),
            overage_status: s(&i, "overageStatus").to_string(),
            is_using_overage: i
                .get("isUsingOverage")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        },
    }
}

fn parse_usage(v: &Value) -> Usage {
    Usage {
        input_tokens: u(v, "input_tokens"),
        output_tokens: u(v, "output_tokens"),
        cache_read_tokens: u(v, "cache_read_input_tokens"),
        cache_creation_tokens: u(v, "cache_creation_input_tokens"),
        cost_usd: 0.0,
    }
}

// ── wire encoders ─────────────────────────────────────────────────────────────

fn control_success(request_id: &str, response: Value) -> OutLine {
    OutLine::json(&json!({
        "type": "control_response",
        "response": {"subtype": "success", "request_id": request_id, "response": response},
    }))
}

/// `can_use_tool` allow.
pub fn encode_allow(request_id: &str, input: Value, permission_updates: Option<Value>) -> OutLine {
    let mut r = json!({"behavior": "allow", "updatedInput": input});
    if let Some(p) = permission_updates {
        r["updatedPermissions"] = p;
    }
    control_success(request_id, r)
}

/// `can_use_tool` deny.
pub fn encode_deny(request_id: &str, message: &str) -> OutLine {
    control_success(request_id, json!({"behavior": "deny", "message": message}))
}

/// Elicitation accept.
pub fn encode_elicitation_accept(request_id: &str, content: Option<Value>) -> OutLine {
    control_success(
        request_id,
        json!({"action": "accept", "content": content.unwrap_or_else(|| json!({}))}),
    )
}

/// Elicitation decline.
pub fn encode_elicitation_decline(request_id: &str) -> OutLine {
    control_success(request_id, json!({"action": "decline"}))
}

/// `AskUserQuestion` answer: allow with the original questions plus answers.
pub fn encode_question_answer(request_id: &str, input: &Value, answer: &QuestionAnswer) -> OutLine {
    let answers: serde_json::Map<String, Value> = answer
        .answers
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let questions = input.get("questions").cloned().unwrap_or_else(|| json!([]));
    encode_allow(
        request_id,
        json!({"questions": questions, "answers": answers}),
        None,
    )
}

/// A user message.
pub fn encode_user_message(text: &str) -> OutLine {
    OutLine::json(&json!({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }))
}
