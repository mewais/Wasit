//! The `codex app-server` JSON-RPC protocol as a pure [`Protocol`].
//!
//! Wire facts (verified against codex-cli 0.169.1 and its generated JSON
//! schema, `codex app-server generate-json-schema --experimental`):
//! - Codex omits `"jsonrpc"` on its own lines; we include it on ours.
//! - Handshake: `initialize` (with `capabilities.experimentalApi`, required for
//!   `collaborationMode/list`) → wait for the response → `initialized`
//!   notification → `model/list` → `thread/start` | `thread/resume`
//!   (for `ResumeMode::Continue`: `thread/list {cwd, limit:1}` first). Codex
//!   drops a `thread/start` sent before the `initialize` response.
//! - The `thread/start|resume` result carries `thread.id`, `model`, `cwd` →
//!   [`AgentEvent::SessionInit`]; then `collaborationMode/list` → modes.
//! - Turns: `turn/start` → `turn/started` … `turn/completed`. Usage arrives as
//!   `thread/tokenUsage/updated` (`total` is cumulative for the thread).
//! - A prompt during a running turn is sent as `turn/steer` with
//!   `expectedTurnId` (injected into the running turn).
//! - Interrupt: `turn/interrupt {threadId, turnId}` with the tracked turn id.
//! - Model / collaboration mode: `thread/settings/update`, confirmed by
//!   `thread/settings/updated`.
//! - Gates are server→client requests (`item/commandExecution/requestApproval`,
//!   `item/fileChange/requestApproval`, `item/permissions/requestApproval`,
//!   legacy `execCommandApproval` / `applyPatchApproval`,
//!   `item/tool/requestUserInput`, `mcpServer/elicitation/request`); a gate
//!   resolved by codex itself is announced with `serverRequest/resolved`.

use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};
use wasit_core::wire::{
    id_text, opt_s, rpc_error, rpc_notification, rpc_request, rpc_result, s, u, RpcIn,
};
use wasit_core::{
    AgentEvent, Command, CompactionInfo, DriverError, Encoded, EndReason, GateDecision,
    GateRequest, ModelInfo, OutLine, PlanEntry, Protocol, QuestionAnswer, QuestionOption,
    QuestionPrompt, ResumeMode, SessionMode, SpawnSpec, Step, Usage,
};

/// `protocol_options` key holding skill roots (a JSON array of absolute
/// paths) that the protocol registers with `skills/extraRoots/set`.
pub const SKILL_ROOTS_OPTION: &str = "codex.skill_roots";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    New,
    Handshaking,
    Ready,
    Closed,
}

/// What one of our outstanding requests was.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Req {
    Initialize,
    ModelList,
    SkillRoots,
    ThreadList,
    ThreadOpen,
    CollabList,
    ModeAtStart,
    TurnStart,
    Steer,
    Interrupt,
    SetModel,
    SetMode,
}

impl Req {
    fn command(&self) -> &'static str {
        match self {
            Req::Initialize => "initialize",
            Req::ModelList => "model/list",
            Req::SkillRoots => "skills/extraRoots/set",
            Req::ThreadList => "thread/list",
            Req::ThreadOpen => "thread/start",
            Req::CollabList => "collaborationMode/list",
            Req::ModeAtStart | Req::SetMode => "set_mode",
            Req::TurnStart => "prompt",
            Req::Steer => "prompt",
            Req::Interrupt => "interrupt",
            Req::SetModel => "set_model",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct PendingGate {
    routing_id: String,
    raw_id: Value,
    method: String,
    params: Value,
}

/// The codex protocol state machine. Create with [`CodexProtocol::new`].
#[derive(Debug)]
pub struct CodexProtocol {
    phase: Phase,
    next_id: u64,
    outstanding: HashMap<u64, Req>,
    queue: VecDeque<Command>,
    spec: SpawnSpec,
    thread_id: Option<String>,
    model: Option<String>,
    mode: Option<String>,
    /// The running turn's id, once known.
    active_turn: Option<String>,
    /// `turn/start` sent, turn id not known yet.
    turn_pending: bool,
    /// Interrupt asked for while the turn id was not known yet.
    interrupt_wanted: bool,
    gates: Vec<PendingGate>,
    tools: HashMap<String, String>,
    total_usage: Usage,
}

impl Default for CodexProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexProtocol {
    pub fn new() -> Self {
        CodexProtocol {
            phase: Phase::New,
            next_id: 1,
            outstanding: HashMap::new(),
            queue: VecDeque::new(),
            spec: SpawnSpec::default(),
            thread_id: None,
            model: None,
            mode: None,
            active_turn: None,
            turn_pending: false,
            interrupt_wanted: false,
            gates: Vec::new(),
            tools: HashMap::new(),
            total_usage: Usage::default(),
        }
    }

    fn request(&mut self, req: Req, method: &str, params: Value) -> OutLine {
        let id = self.next_id;
        self.next_id += 1;
        self.outstanding.insert(id, req);
        rpc_request(id, method, params)
    }

    fn thread(&self) -> String {
        self.thread_id.clone().unwrap_or_default()
    }

    // ── handshake ───────────────────────────────────────────────────────────

    fn on_initialized(&mut self) -> Step {
        let mut write = vec![rpc_notification("initialized", None)];
        write.push(self.request(Req::ModelList, "model/list", json!({})));
        if let Some(roots) = self.spec.protocol_options.get(SKILL_ROOTS_OPTION).cloned() {
            write.push(self.request(
                Req::SkillRoots,
                "skills/extraRoots/set",
                json!({"extraRoots": roots}),
            ));
        }
        let cwd = self.spec.cwd_or_dot().to_string();
        let open = match self.spec.resume.clone() {
            ResumeMode::Resume(id) => self.thread_resume(&id),
            ResumeMode::Continue => self.request(
                Req::ThreadList,
                "thread/list",
                json!({"cwd": cwd, "limit": 1}),
            ),
            ResumeMode::Fresh => self.thread_start(),
        };
        write.push(open);
        Step {
            write,
            ..Step::default()
        }
    }

    fn thread_start(&mut self) -> OutLine {
        let mut params = json!({
            "cwd": self.spec.cwd_or_dot(),
            // Surface approvals to the human; never let codex decide silently.
            "approvalPolicy": "on-request",
            "sandbox": "workspace-write",
        });
        if let Some(m) = self.spec.model.as_deref().filter(|m| !m.is_empty()) {
            params["model"] = json!(m);
        }
        self.request(Req::ThreadOpen, "thread/start", params)
    }

    fn thread_resume(&mut self, id: &str) -> OutLine {
        let mut params = json!({"threadId": id, "cwd": self.spec.cwd_or_dot()});
        if let Some(m) = self.spec.model.as_deref().filter(|m| !m.is_empty()) {
            params["model"] = json!(m);
        }
        self.request(Req::ThreadOpen, "thread/resume", params)
    }

    fn on_thread_open(&mut self, result: &Value) -> Step {
        let thread = result.get("thread").cloned().unwrap_or(Value::Null);
        self.thread_id = opt_s(&thread, "id");
        self.model = opt_s(result, "model").or_else(|| opt_s(&thread, "model"));
        let mut step = events(vec![AgentEvent::SessionInit {
            native_session_id: self.thread_id.clone(),
            model: self.model.clone(),
            cwd: opt_s(result, "cwd").or_else(|| opt_s(&thread, "cwd")),
            tools: vec![],
            mcp_servers: vec![],
            slash_commands: vec![],
            available_modes: vec![],
            current_mode: String::new(),
            cli_version: String::new(),
        }]);
        step.write
            .push(self.request(Req::CollabList, "collaborationMode/list", json!({})));
        if !self.spec.mode.is_empty() {
            let mode = self.spec.mode.clone();
            let line = self.mode_update(Req::ModeAtStart, &mode);
            step.write.push(line);
        }
        self.phase = Phase::Ready;
        step.extend(self.drain());
        step
    }

    fn mode_update(&mut self, req: Req, mode: &str) -> OutLine {
        let params = json!({
            "threadId": self.thread(),
            "collaborationMode": {
                "mode": mode,
                "settings": {"model": self.model.clone().unwrap_or_default(), "developer_instructions": null},
            },
        });
        self.request(req, "thread/settings/update", params)
    }

    // ── commands ────────────────────────────────────────────────────────────

    fn must_queue(&self, cmd: &Command) -> bool {
        match self.phase {
            Phase::New | Phase::Handshaking => matches!(
                cmd,
                Command::Prompt { .. } | Command::SetModel { .. } | Command::SetMode { .. }
            ),
            // A prompt can be steered only once the running turn's id is known.
            Phase::Ready => {
                matches!(cmd, Command::Prompt { .. })
                    && ((self.turn_pending && self.active_turn.is_none()) || !self.queue.is_empty())
            }
            Phase::Closed => false,
        }
    }

    fn encode_now(&mut self, cmd: Command) -> Result<Step, DriverError> {
        let mut step = Step::default();
        match cmd {
            Command::Prompt { text } => {
                let input = json!([{"type": "text", "text": text}]);
                let line = match self.active_turn.clone() {
                    Some(turn) => self.request(
                        Req::Steer,
                        "turn/steer",
                        json!({"threadId": self.thread(), "expectedTurnId": turn, "input": input}),
                    ),
                    None => {
                        self.turn_pending = true;
                        self.request(
                            Req::TurnStart,
                            "turn/start",
                            json!({"threadId": self.thread(), "input": input}),
                        )
                    }
                };
                step.write.push(line);
            }
            Command::RunSlash { .. } => {
                return Err(DriverError::Unsupported {
                    operation: "run_slash (codex app-server has no slash commands)".into(),
                })
            }
            Command::AnswerGate {
                request_id,
                decision,
            } => {
                let gate = self.take_gate(&request_id)?;
                if gate.method == "item/tool/requestUserInput" {
                    self.gates.push(gate);
                    return Err(DriverError::Invalid {
                        detail: format!("gate {request_id} is a question; use answer_question"),
                    });
                }
                step.write.push(encode_gate_answer(&gate, &decision));
            }
            Command::AnswerQuestion { request_id, answer } => {
                let gate = self.take_gate(&request_id)?;
                if gate.method != "item/tool/requestUserInput" {
                    self.gates.push(gate);
                    return Err(DriverError::Invalid {
                        detail: format!("gate {request_id} is not a question; use answer_gate"),
                    });
                }
                step.write.push(encode_user_input_answer(&gate, &answer));
            }
            Command::Interrupt => return Ok(self.interrupt()),
            Command::SetModel { model } => {
                let line = self.request(
                    Req::SetModel,
                    "thread/settings/update",
                    json!({"threadId": self.thread(), "model": model}),
                );
                step.write.push(line);
            }
            Command::SetMode { mode } => {
                let line = self.mode_update(Req::SetMode, &mode);
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
        self.queue.retain(|c| !matches!(c, Command::Prompt { .. }));
        for g in std::mem::take(&mut self.gates) {
            step.write
                .push(encode_gate_answer(&g, &GateDecision::deny(None)));
            step.events.push(AgentEvent::GateResolved {
                request_id: g.routing_id,
            });
        }
        match self.active_turn.clone() {
            Some(turn) if !self.interrupt_wanted => {
                self.interrupt_wanted = true;
                let line = self.request(
                    Req::Interrupt,
                    "turn/interrupt",
                    json!({"threadId": self.thread(), "turnId": turn}),
                );
                step.write.push(line);
            }
            None if self.turn_pending => self.interrupt_wanted = true,
            _ => {}
        }
        step
    }

    /// Called when the running turn's id becomes known.
    fn on_turn_id(&mut self, turn: String) -> Step {
        let first = self.active_turn.is_none();
        self.active_turn = Some(turn.clone());
        self.turn_pending = false;
        let mut step = Step::default();
        if first && self.interrupt_wanted {
            let line = self.request(
                Req::Interrupt,
                "turn/interrupt",
                json!({"threadId": self.thread(), "turnId": turn}),
            );
            step.write.push(line);
        }
        step.extend(self.drain());
        step
    }

    fn drain(&mut self) -> Step {
        let mut step = Step::default();
        while let Some(cmd) = self.queue.front() {
            if self.phase != Phase::Ready {
                break;
            }
            if matches!(cmd, Command::Prompt { .. })
                && self.turn_pending
                && self.active_turn.is_none()
            {
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
            Req::Initialize => self.on_initialized(),
            Req::ModelList => events(vec![AgentEvent::ModelsUpdated {
                models: result
                    .get("data")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter(|m| !m.get("hidden").and_then(Value::as_bool).unwrap_or(false))
                            .filter_map(|m| {
                                let id = opt_s(m, "id").or_else(|| opt_s(m, "model"))?;
                                Some(ModelInfo {
                                    display_name: opt_s(m, "displayName")
                                        .unwrap_or_else(|| id.clone()),
                                    description: s(m, "description").to_string(),
                                    is_default: m
                                        .get("isDefault")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false),
                                    id,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                current: None,
            }]),
            Req::ThreadList => {
                let latest = result
                    .get("data")
                    .and_then(Value::as_array)
                    .and_then(|a| a.first())
                    .and_then(|t| opt_s(t, "id"));
                let line = match latest {
                    Some(id) => self.thread_resume(&id),
                    None => self.thread_start(),
                };
                Step {
                    write: vec![line],
                    ..Step::default()
                }
            }
            Req::ThreadOpen => self.on_thread_open(&result),
            Req::CollabList => {
                let modes: Vec<SessionMode> = result
                    .get("data")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|m| {
                                let id = opt_s(m, "mode")?;
                                Some(SessionMode {
                                    label: opt_s(m, "name").unwrap_or_else(|| id.clone()),
                                    id,
                                    description: String::new(),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                events(vec![AgentEvent::ModesUpdated {
                    available_modes: modes,
                    current_mode: self.mode.clone(),
                }])
            }
            Req::TurnStart => match result.get("turn").and_then(|t| opt_s(t, "id")) {
                Some(turn) if self.turn_pending && self.active_turn.is_none() => {
                    self.on_turn_id(turn)
                }
                _ => Step::default(),
            },
            // Confirmed by notifications (`thread/settings/updated`,
            // `turn/completed`) or nothing to report.
            Req::SkillRoots
            | Req::ModeAtStart
            | Req::Steer
            | Req::Interrupt
            | Req::SetModel
            | Req::SetMode => Step::default(),
        }
    }

    fn on_error(&mut self, id: &Value, message: String) -> Step {
        let Some(req) = id.as_u64().and_then(|n| self.outstanding.remove(&n)) else {
            return events(vec![AgentEvent::Unknown {
                raw: json!({"id": id, "error": {"message": message}}),
            }]);
        };
        match req {
            Req::TurnStart => {
                self.turn_pending = false;
                self.interrupt_wanted = false;
                let mut step = events(vec![AgentEvent::TurnCompleted {
                    end_reason: Some(EndReason::ExecutionError),
                    was_interrupted: false,
                    num_turns: 0,
                    usage: self.total_usage.clone(),
                    per_model: HashMap::new(),
                    reply: Some(message),
                }]);
                step.extend(self.drain());
                step
            }
            Req::ThreadList => {
                // Cannot find the latest thread: start a fresh one, and say so.
                let line = self.thread_start();
                Step {
                    events: vec![AgentEvent::CommandRejected {
                        command: "continue".into(),
                        message,
                    }],
                    write: vec![line],
                    close_stdin: false,
                }
            }
            Req::Initialize => {
                let mut step = events(vec![AgentEvent::CommandRejected {
                    command: req.command().into(),
                    message,
                }]);
                step.extend(self.on_initialized());
                step
            }
            other => events(vec![AgentEvent::CommandRejected {
                command: other.command().into(),
                message,
            }]),
        }
    }

    fn on_request(&mut self, id: Value, method: String, params: Value) -> Step {
        match method.as_str() {
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
            | "item/tool/requestUserInput"
            | "mcpServer/elicitation/request" => {
                let routing_id = opt_s(&params, "approvalId")
                    .or_else(|| opt_s(&params, "itemId"))
                    .unwrap_or_else(|| format!("codex:{}", id_text(&id)));
                let gate = build_gate(&routing_id, &method, &params);
                self.gates.push(PendingGate {
                    routing_id,
                    raw_id: id,
                    method,
                    params,
                });
                events(vec![AgentEvent::GateRequested { gate }])
            }
            _ => {
                // Dynamic tools, auth refresh, attestation, clock reads: this
                // host registers none of them. Answer with an error so codex
                // never waits on us.
                let raw = json!({"id": id, "method": method, "params": params});
                Step {
                    events: vec![AgentEvent::Unknown { raw }],
                    write: vec![rpc_error(
                        &id,
                        -32601,
                        &format!("method not supported by this client: {method}"),
                    )],
                    close_stdin: false,
                }
            }
        }
    }

    fn on_notification(&mut self, method: &str, params: Value) -> Step {
        match method {
            "turn/started" => {
                let turn = params.get("turn").and_then(|t| opt_s(t, "id"));
                let mut step = events(vec![AgentEvent::TurnStarted {
                    native_turn_id: turn.clone(),
                }]);
                if let Some(t) = turn {
                    if self.active_turn.as_deref() != Some(t.as_str()) {
                        step.extend(self.on_turn_id(t));
                    }
                }
                step
            }
            "turn/completed" => self.on_turn_completed(&params),
            "item/agentMessage/delta" => {
                let delta = s(&params, "delta");
                if delta.is_empty() {
                    return Step::default();
                }
                events(vec![AgentEvent::AssistantDelta {
                    text: delta.to_string(),
                    message_id: opt_s(&params, "itemId"),
                }])
            }
            "item/reasoning/textDelta" | "item/reasoning/summaryTextDelta" => {
                let delta = s(&params, "delta");
                if delta.is_empty() {
                    return Step::default();
                }
                events(vec![AgentEvent::ThinkingDelta {
                    text: delta.to_string(),
                    message_id: opt_s(&params, "itemId"),
                }])
            }
            "item/started" => events(self.on_item_started(&params)),
            "item/completed" => events(self.on_item_completed(&params)),
            "thread/tokenUsage/updated" => {
                let tu = params.get("tokenUsage").cloned().unwrap_or(Value::Null);
                let total = tu.get("total").cloned().unwrap_or(Value::Null);
                self.total_usage = token_usage(&total);
                let window = u(&tu, "modelContextWindow");
                let context_pct = (window > 0).then(|| {
                    let last = tu.get("last").cloned().unwrap_or(Value::Null);
                    (u(&last, "totalTokens") as f64 * 100.0 / window as f64) as f32
                });
                events(vec![AgentEvent::UsageUpdate {
                    usage: self.total_usage.clone(),
                    context_pct,
                }])
            }
            "turn/plan/updated" => events(vec![AgentEvent::PlanUpdated {
                entries: params
                    .get("plan")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .map(|p| PlanEntry {
                                content: s(p, "step").to_string(),
                                priority: String::new(),
                                status: s(p, "status").to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }]),
            "thread/settings/updated" => {
                let ts = params.get("threadSettings").cloned().unwrap_or(Value::Null);
                let mut out = Vec::new();
                if let Some(m) = opt_s(&ts, "model") {
                    if self.model.as_deref() != Some(m.as_str()) {
                        self.model = Some(m.clone());
                        out.push(AgentEvent::ModelChanged { model: m });
                    }
                }
                if let Some(mode) = ts.get("collaborationMode").and_then(|c| opt_s(c, "mode")) {
                    if self.mode.as_deref() != Some(mode.as_str()) {
                        self.mode = Some(mode.clone());
                        out.push(AgentEvent::ModesUpdated {
                            available_modes: vec![],
                            current_mode: Some(mode),
                        });
                    }
                }
                events(out)
            }
            "serverRequest/resolved" => {
                let rid = params.get("requestId").cloned().unwrap_or(Value::Null);
                match self.gates.iter().position(|g| g.raw_id == rid) {
                    Some(pos) => {
                        let g = self.gates.remove(pos);
                        events(vec![AgentEvent::GateResolved {
                            request_id: g.routing_id,
                        }])
                    }
                    // Our own answer being acknowledged.
                    None => Step::default(),
                }
            }
            "model/rerouted" => match opt_s(&params, "toModel") {
                Some(m) => {
                    self.model = Some(m.clone());
                    events(vec![AgentEvent::ModelChanged { model: m }])
                }
                None => Step::default(),
            },
            "error" => {
                let msg = params
                    .get("error")
                    .and_then(|e| opt_s(e, "message"))
                    .unwrap_or_else(|| "codex error".into());
                let retry = params
                    .get("willRetry")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                events(vec![AgentEvent::Notice {
                    message: if retry {
                        format!("{msg} (retrying)")
                    } else {
                        msg
                    },
                }])
            }
            "warning" | "configWarning" | "deprecationNotice" | "guardianWarning" => {
                events(vec![AgentEvent::Notice {
                    message: opt_s(&params, "message")
                        .or_else(|| opt_s(&params, "summary"))
                        .unwrap_or_else(|| params.to_string()),
                }])
            }
            // Bookkeeping notifications with no user-visible meaning.
            "thread/started"
            | "thread/status/changed"
            | "thread/readState/changed"
            | "account/rateLimits/updated"
            | "remoteControl/status/changed"
            | "item/commandExecution/outputDelta"
            | "item/fileChange/outputDelta"
            | "turn/diff/updated"
            | "mcpServer/startupStatus/updated"
            | "skills/changed" => Step::default(),
            _ => events(vec![AgentEvent::Unknown {
                raw: json!({"method": method, "params": params}),
            }]),
        }
    }

    fn on_turn_completed(&mut self, params: &Value) -> Step {
        let turn = params.get("turn").cloned().unwrap_or(Value::Null);
        let status = s(&turn, "status");
        let interrupted = status == "interrupted";
        let end_reason = match status {
            "completed" => None,
            "interrupted" => Some(EndReason::Cancelled),
            _ => Some(EndReason::ExecutionError),
        };
        let reply = if end_reason == Some(EndReason::ExecutionError) {
            turn.get("error").and_then(|e| opt_s(e, "message"))
        } else {
            turn.get("items")
                .and_then(Value::as_array)
                .and_then(|items| {
                    items
                        .iter()
                        .rev()
                        .find(|i| s(i, "type") == "agentMessage")
                        .and_then(|i| opt_s(i, "text"))
                })
        };
        self.active_turn = None;
        self.turn_pending = false;
        self.interrupt_wanted = false;
        self.tools.clear();
        let mut step = Step::default();
        for g in std::mem::take(&mut self.gates) {
            step.events.push(AgentEvent::GateResolved {
                request_id: g.routing_id,
            });
        }
        step.events.push(AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted: interrupted,
            num_turns: 0,
            usage: self.total_usage.clone(),
            per_model: HashMap::new(),
            reply,
        });
        step.extend(self.drain());
        step
    }

    fn on_item_started(&mut self, params: &Value) -> Vec<AgentEvent> {
        let item = params.get("item").cloned().unwrap_or(Value::Null);
        let ty = s(&item, "type");
        let Some(id) = opt_s(&item, "id") else {
            return vec![];
        };
        let (name, input) = match ty {
            "userMessage" | "agentMessage" | "reasoning" | "plan" | "contextCompaction" => {
                return vec![]
            }
            "commandExecution" => (s(&item, "command").to_string(), Value::Null),
            "fileChange" => (
                "apply_patch".to_string(),
                item.get("changes").cloned().unwrap_or(Value::Null),
            ),
            "mcpToolCall" => (
                format!("{}.{}", s(&item, "server"), s(&item, "tool")),
                item.get("arguments").cloned().unwrap_or(Value::Null),
            ),
            "dynamicToolCall" => (
                s(&item, "tool").to_string(),
                item.get("arguments").cloned().unwrap_or(Value::Null),
            ),
            other => (other.to_string(), Value::Null),
        };
        self.tools.insert(id.clone(), name.clone());
        let is_subagent = ty == "collabAgentToolCall";
        vec![AgentEvent::ToolCallStarted {
            name,
            call_id: id,
            input,
            is_subagent,
            subagent_model: None,
        }]
    }

    fn on_item_completed(&mut self, params: &Value) -> Vec<AgentEvent> {
        let item = params.get("item").cloned().unwrap_or(Value::Null);
        if s(&item, "type") == "contextCompaction" {
            return vec![AgentEvent::Compaction {
                info: CompactionInfo::default(),
            }];
        }
        let Some(id) = opt_s(&item, "id") else {
            return vec![];
        };
        let Some(name) = self.tools.remove(&id) else {
            return vec![];
        };
        let status = s(&item, "status");
        let exit_ok = item
            .get("exitCode")
            .and_then(Value::as_i64)
            .map(|c| c == 0)
            .unwrap_or(true);
        let result = item
            .get("aggregatedOutput")
            .filter(|v| !v.is_null())
            .or_else(|| item.get("result").filter(|v| !v.is_null()))
            .cloned();
        vec![AgentEvent::ToolCallFinished {
            call_id: id,
            name,
            ok: status == "completed" && exit_ok,
            result,
        }]
    }
}

impl Protocol for CodexProtocol {
    fn start(&mut self, spec: &SpawnSpec) -> Step {
        self.spec = spec.clone();
        self.phase = Phase::Handshaking;
        self.mode = None;
        if let Some(p) = &spec.first_prompt {
            self.queue.push_back(Command::Prompt { text: p.clone() });
        }
        let line = self.request(
            Req::Initialize,
            "initialize",
            json!({
                "clientInfo": {"name": "wasit", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"experimentalApi": true},
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
            Some(RpcIn::Notification { method, params }) => self.on_notification(&method, params),
            None => events(vec![AgentEvent::Unknown { raw: v }]),
        }
    }

    fn is_ready(&self) -> bool {
        self.phase == Phase::Ready
    }

    fn encode(&mut self, cmd: Command) -> Result<Encoded, DriverError> {
        if self.phase == Phase::Closed {
            return Err(DriverError::Shutdown);
        }
        if matches!(cmd, Command::RunSlash { .. }) {
            return self.encode_now(cmd).map(Encoded::Sent);
        }
        if matches!(cmd, Command::Interrupt) && self.phase != Phase::Ready {
            self.queue.retain(|c| !matches!(c, Command::Prompt { .. }));
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

fn token_usage(v: &Value) -> Usage {
    Usage {
        input_tokens: u(v, "inputTokens"),
        output_tokens: u(v, "outputTokens"),
        cache_read_tokens: u(v, "cachedInputTokens"),
        cache_creation_tokens: u(v, "cacheWriteInputTokens"),
        cost_usd: 0.0,
    }
}

fn build_gate(routing_id: &str, method: &str, params: &Value) -> GateRequest {
    match method {
        "item/tool/requestUserInput" => GateRequest::Question {
            request_id: routing_id.to_string(),
            questions: params
                .get("questions")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|q| QuestionPrompt {
                            question: s(q, "question").to_string(),
                            header: s(q, "header").to_string(),
                            multi_select: false,
                            options: q
                                .get("options")
                                .and_then(Value::as_array)
                                .map(|o| {
                                    o.iter()
                                        .map(|o| QuestionOption {
                                            label: s(o, "label").to_string(),
                                            description: s(o, "description").to_string(),
                                        })
                                        .collect()
                                })
                                .unwrap_or_default(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
        "mcpServer/elicitation/request" => {
            let message = s(params, "message").to_string();
            GateRequest::Permission {
                request_id: routing_id.to_string(),
                tool_name: opt_s(params, "serverName")
                    .or_else(|| opt_s(params, "server"))
                    .unwrap_or_else(|| "mcp".into()),
                command: message.clone(),
                cwd: String::new(),
                explanation: message,
                is_elicitation: true,
                elicitation_choices: wasit_core::wire::elicitation_choices(
                    params.get("requestedSchema"),
                ),
                permission_suggestions: vec![],
                raw_input: params.clone(),
            }
        }
        _ => {
            let (tool, command) = match method {
                "item/fileChange/requestApproval" | "applyPatchApproval" => (
                    "apply_patch",
                    params
                        .get("changes")
                        .or_else(|| params.get("fileChanges"))
                        .map(Value::to_string)
                        .unwrap_or_default(),
                ),
                "item/permissions/requestApproval" => (
                    "permissions",
                    params
                        .get("permissions")
                        .map(Value::to_string)
                        .unwrap_or_default(),
                ),
                _ => (
                    "shell",
                    match params.get("command") {
                        Some(Value::Array(parts)) => parts
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(" "),
                        Some(Value::String(c)) => c.clone(),
                        _ => String::new(),
                    },
                ),
            };
            GateRequest::Permission {
                request_id: routing_id.to_string(),
                tool_name: tool.to_string(),
                command,
                cwd: s(params, "cwd").to_string(),
                explanation: s(params, "reason").to_string(),
                is_elicitation: false,
                elicitation_choices: vec![],
                permission_suggestions: vec![],
                raw_input: params.clone(),
            }
        }
    }
}

/// The JSON-RPC result answering a gate.
fn encode_gate_answer(gate: &PendingGate, decision: &GateDecision) -> OutLine {
    let approve = matches!(decision, GateDecision::Approve { .. });
    let for_session = matches!(
        decision,
        GateDecision::Approve {
            permission_updates: Some(_),
            ..
        }
    );
    let result = match gate.method.as_str() {
        "mcpServer/elicitation/request" => match decision {
            GateDecision::Approve {
                elicitation_content,
                ..
            } => {
                json!({"action": "accept", "content": elicitation_content.clone().unwrap_or_else(|| json!({}))})
            }
            GateDecision::Deny { .. } => json!({"action": "decline"}),
        },
        "execCommandApproval" | "applyPatchApproval" => {
            json!({"decision": match (approve, for_session) {
                (true, true) => "approved_for_session",
                (true, false) => "approved",
                (false, _) => "denied",
            }})
        }
        "item/permissions/requestApproval" => {
            if approve {
                json!({
                    "permissions": gate.params.get("permissions").cloned().unwrap_or_else(|| json!({})),
                    "scope": if for_session { "session" } else { "turn" },
                })
            } else {
                json!({"permissions": {}})
            }
        }
        // item/commandExecution + item/fileChange.
        _ => json!({"decision": match (approve, for_session) {
            (true, true) => "acceptForSession",
            (true, false) => "accept",
            (false, _) => "decline",
        }}),
    };
    rpc_result(&gate.raw_id, result)
}

/// `item/tool/requestUserInput` answer: question text → id mapping.
fn encode_user_input_answer(gate: &PendingGate, answer: &QuestionAnswer) -> OutLine {
    let mut answers = serde_json::Map::new();
    for q in gate
        .params
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let qid = s(q, "id").to_string();
        let text = s(q, "question");
        if let Some(a) = answer
            .answers
            .get(text)
            .or_else(|| answer.answers.get(&qid))
        {
            let parts: Vec<Value> = a
                .split(',')
                .map(|p| Value::String(p.trim().to_string()))
                .filter(|p| p.as_str() != Some(""))
                .collect();
            answers.insert(qid, json!({"answers": parts}));
        }
    }
    rpc_result(&gate.raw_id, json!({"answers": answers}))
}
