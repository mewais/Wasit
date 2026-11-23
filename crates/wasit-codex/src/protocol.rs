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
//!   [`AgentEvent::SessionInit`]; then `collaborationMode/list` → modes. A `thread/resume`
//!   result also carries the thread's earlier turns (`thread.turns[].items[]`, full-history
//!   hydration, which codex 0.169.1 announces as deprecated in favour of `thread/turns/list`
//!   and still sends); each item becomes an [`AgentEvent::History`] event ahead of
//!   `SessionInit`, mapped with the same functions as the live items, and a turn whose items
//!   codex did not send (`itemsView` not `full`) says so in a history notice. History
//!   feeds no state of this protocol (usage, turn, tools, gates).
//! - Turns: `turn/start` → `turn/started` … `turn/completed`. Usage arrives as
//!   `thread/tokenUsage/updated` (`total` is cumulative for the thread).
//! - A prompt during a running turn is sent as `turn/steer` with
//!   `expectedTurnId` (injected into the running turn). Its result `{turnId}` is the
//!   acknowledgement ([`AgentEvent::InputAcknowledged`] `steered`); an error (`no active turn to
//!   steer`, `expected active turn id … but found …`; codex-cli 0.161.0) means the prompt did not
//!   join the turn and is reported [`AgentEvent::InputDropped`] `Refused`. It is never re-sent as
//!   a new turn on its own: that is the user's decision.
//! - A `turn/start` is acknowledged by its result `{turn}` or by the `turn/started`
//!   notification, whichever arrives first (0.169.1 and 0.161.0 send the result first); a
//!   `turn/start` error before either is `Refused` (and ends the turn as `ExecutionError`).
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
use wasit_core::content::{base_name, path_reference_text, sniff_mime, COMMON_IMAGE_MIMES};
use wasit_core::wire::{
    id_text, opt_s, rpc_error, rpc_notification, rpc_request, rpc_result, s, u, RpcIn,
};
use wasit_core::{
    AgentEvent, Attachment, Command, CompactionInfo, ContentPart, DriverError, Encoded, EndReason,
    GateChoice, GateChoiceKind, GateRequest, InputDropReason, InputId, MediaSource, ModelInfo,
    OutLine, PlanEntry, Protocol, QuestionAnswer, QuestionOption, QuestionPrompt, ResumeMode,
    SessionMode, SpawnSpec, Step, Usage,
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
    /// `turn/start` for this input.
    TurnStart(InputId),
    /// `turn/steer` for this input.
    Steer(InputId),
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
            Req::TurnStart(_) | Req::Steer(_) => "prompt",
            Req::Interrupt => "interrupt",
            Req::SetModel => "set_model",
        }
    }
}

/// A `turn/start` we wrote, until the CLI announces its turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Starting {
    input: InputId,
    /// `InputAcknowledged` was reported (the `turn/start` result came first).
    acknowledged: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingGate {
    routing_id: String,
    raw_id: Value,
    method: String,
    params: Value,
    /// Per choice id, the JSON-RPC result that answers the request with it.
    answers: Vec<(String, Value)>,
}

/// The codex protocol state machine. Create with [`CodexProtocol::new`].
#[derive(Debug)]
pub struct CodexProtocol {
    phase: Phase,
    next_id: u64,
    outstanding: HashMap<u64, Req>,
    queue: VecDeque<(InputId, Command)>,
    /// The `turn/start` whose turn the CLI has not announced (`turn/started`) yet.
    starting: Option<Starting>,
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
    /// The open request in flight is a `thread/resume`: its result carries the thread's earlier
    /// turns.
    resuming: bool,
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
            starting: None,
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
            resuming: false,
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
        self.resuming = false;
        self.request(Req::ThreadOpen, "thread/start", params)
    }

    fn thread_resume(&mut self, id: &str) -> OutLine {
        let mut params = json!({"threadId": id, "cwd": self.spec.cwd_or_dot()});
        if let Some(m) = self.spec.model.as_deref().filter(|m| !m.is_empty()) {
            params["model"] = json!(m);
        }
        self.resuming = true;
        self.request(Req::ThreadOpen, "thread/resume", params)
    }

    fn on_thread_open(&mut self, result: &Value) -> Step {
        let thread = result.get("thread").cloned().unwrap_or(Value::Null);
        self.thread_id = opt_s(&thread, "id");
        self.model = opt_s(result, "model").or_else(|| opt_s(&thread, "model"));
        // What the agent says happened before, ahead of the event that opens the session: it is
        // history, never live activity, and touches no state of this protocol.
        let mut opened = if std::mem::take(&mut self.resuming) {
            thread_history(&thread)
        } else {
            Vec::new()
        };
        opened.push(AgentEvent::SessionInit {
            native_session_id: self.thread_id.clone(),
            model: self.model.clone(),
            cwd: opt_s(result, "cwd").or_else(|| opt_s(&thread, "cwd")),
            tools: vec![],
            mcp_servers: vec![],
            slash_commands: vec![],
            available_modes: vec![],
            current_mode: String::new(),
            cli_version: String::new(),
        });
        let mut step = events(opened);
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

    fn encode_now(&mut self, input: InputId, cmd: Command) -> Result<Step, DriverError> {
        let mut step = Step::default();
        match cmd {
            Command::Prompt { text, attachments } => {
                let items = Value::Array(user_input(&text, &attachments));
                let line = match self.active_turn.clone() {
                    Some(turn) => self.request(
                        Req::Steer(input),
                        "turn/steer",
                        json!({"threadId": self.thread(), "expectedTurnId": turn, "input": items}),
                    ),
                    None => {
                        self.turn_pending = true;
                        self.starting = Some(Starting {
                            input,
                            acknowledged: false,
                        });
                        self.request(
                            Req::TurnStart(input),
                            "turn/start",
                            json!({"threadId": self.thread(), "input": items}),
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
                match gate.answers.iter().find(|(id, _)| *id == decision.choice) {
                    Some((_, result)) => step.write.push(rpc_result(&gate.raw_id, result.clone())),
                    None => {
                        let detail =
                            format!("gate {request_id} offers no choice {:?}", decision.choice);
                        self.gates.push(gate);
                        return Err(DriverError::Invalid { detail });
                    }
                }
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
            Command::StopTask { .. } => {
                return Err(DriverError::Unsupported {
                    operation: "stop_task".into(),
                })
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
                step.events = std::mem::take(&mut self.queue)
                    .into_iter()
                    .map(|(input, _)| dropped(input, InputDropReason::Shutdown))
                    .collect();
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

    /// Take the queued prompts out of the queue: one `InputDropped` each, in queue order.
    fn drop_queued_prompts(&mut self, reason: InputDropReason) -> Vec<AgentEvent> {
        let (prompts, rest): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.queue)
            .into_iter()
            .partition(|(_, c)| matches!(c, Command::Prompt { .. }));
        self.queue = rest;
        prompts
            .into_iter()
            .map(|(input, _)| dropped(input, reason))
            .collect()
    }

    fn interrupt(&mut self) -> Step {
        let mut step = events(self.drop_queued_prompts(InputDropReason::Interrupted));
        for g in std::mem::take(&mut self.gates) {
            step.write.push(rpc_result(&g.raw_id, interrupt_answer(&g)));
            step.events.push(AgentEvent::GateResolved {
                request_id: g.routing_id,
                reason: None,
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

    /// The CLI took the `turn/start` for `input` (its result): report it once, whichever of the
    /// result and the `turn/started` notification comes first.
    fn acknowledge_start(&mut self, input: InputId) -> Vec<AgentEvent> {
        match self.starting.as_mut() {
            Some(s) if s.input == input && !s.acknowledged => {
                s.acknowledged = true;
                vec![AgentEvent::InputAcknowledged {
                    input,
                    steered: false,
                }]
            }
            _ => vec![],
        }
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
        while let Some((_, cmd)) = self.queue.front() {
            if self.phase != Phase::Ready {
                break;
            }
            if matches!(cmd, Command::Prompt { .. })
                && self.turn_pending
                && self.active_turn.is_none()
            {
                break;
            }
            let (input, cmd) = self.queue.pop_front().expect("front exists");
            match self.encode_now(input, cmd) {
                Ok(s) => {
                    step.events.push(AgentEvent::InputSent { input });
                    step.extend(s);
                }
                Err(e) => step.events.push(AgentEvent::InputDropped {
                    input,
                    reason: InputDropReason::Unencodable,
                    error: Some(e.to_string()),
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
            // The CLI took the prompt as a turn (its `turn/started` may come before or after).
            Req::TurnStart(input) => {
                let mut step = events(self.acknowledge_start(input));
                if let Some(turn) = result.get("turn").and_then(|t| opt_s(t, "id")) {
                    if self.turn_pending && self.active_turn.is_none() {
                        step.extend(self.on_turn_id(turn));
                    }
                }
                step
            }
            // The CLI folded the prompt into the running turn.
            Req::Steer(input) => events(vec![AgentEvent::InputAcknowledged {
                input,
                steered: true,
            }]),
            // Confirmed by notifications (`thread/settings/updated`,
            // `turn/completed`) or nothing to report.
            Req::SkillRoots | Req::ModeAtStart | Req::Interrupt | Req::SetModel | Req::SetMode => {
                Step::default()
            }
        }
    }

    fn on_error(&mut self, id: &Value, message: String) -> Step {
        let Some(req) = id.as_u64().and_then(|n| self.outstanding.remove(&n)) else {
            return events(vec![AgentEvent::Unknown {
                raw: json!({"id": id, "error": {"message": message}}),
            }]);
        };
        match req {
            Req::TurnStart(input) => {
                self.turn_pending = false;
                self.interrupt_wanted = false;
                // Refused: the CLI never took the prompt (unless it had already announced the
                // turn, in which case this error is about something else).
                let unacknowledged = self
                    .starting
                    .take()
                    .is_some_and(|s| s.input == input && !s.acknowledged);
                let mut step = Step::default();
                if unacknowledged {
                    step.events.push(refused(input, &message));
                }
                step.events.push(AgentEvent::TurnCompleted {
                    end_reason: Some(EndReason::ExecutionError),
                    was_interrupted: false,
                    num_turns: 0,
                    usage: self.total_usage.clone(),
                    per_model: HashMap::new(),
                    reply: Some(message),
                    origin: None,
                });
                step.extend(self.drain());
                step
            }
            // The prompt did not join the turn (it ended meanwhile, or the CLI said otherwise):
            // reported as refused, never sent again on its own.
            Req::Steer(input) => events(vec![refused(input, &message)]),
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
                let (gate, answers) = build_gate(&routing_id, &method, &params);
                self.gates.push(PendingGate {
                    routing_id,
                    raw_id: id,
                    method,
                    params,
                    answers,
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
                let starting = self.starting.take();
                let mut step = events(vec![AgentEvent::turn_started(
                    turn.clone(),
                    starting.map(|s| s.input),
                )]);
                // The notification came before the `turn/start` result: it says the same.
                if let Some(Starting {
                    input,
                    acknowledged: false,
                }) = starting
                {
                    step.events.push(AgentEvent::InputAcknowledged {
                        input,
                        steered: false,
                    });
                }
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
                            reason: None,
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
                reason: None,
            });
        }
        step.events.push(AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted: interrupted,
            num_turns: 0,
            usage: self.total_usage.clone(),
            per_model: HashMap::new(),
            reply,
            origin: None,
        });
        step.extend(self.drain());
        step
    }

    fn on_item_started(&mut self, params: &Value) -> Vec<AgentEvent> {
        let item = params.get("item").cloned().unwrap_or(Value::Null);
        let Some(id) = opt_s(&item, "id") else {
            return vec![];
        };
        let Some(start) = tool_started(&item, &id) else {
            return vec![];
        };
        if let AgentEvent::ToolCallStarted { name, .. } = &start {
            self.tools.insert(id, name.clone());
        }
        vec![start]
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
        vec![tool_finished(&item, id, name)]
    }
}

/// The tool call an item starts (`None` for the item types that are not tool calls: messages,
/// reasoning, plans, compactions). Pure: the live protocol and the replay of a resumed thread's
/// history map items with this one function.
fn tool_started(item: &Value, id: &str) -> Option<AgentEvent> {
    let ty = s(item, "type");
    let (name, input) = match ty {
        "userMessage" | "agentMessage" | "reasoning" | "plan" | "contextCompaction" => return None,
        "commandExecution" => (s(item, "command").to_string(), Value::Null),
        "fileChange" => (
            "apply_patch".to_string(),
            item.get("changes").cloned().unwrap_or(Value::Null),
        ),
        "mcpToolCall" => (
            format!("{}.{}", s(item, "server"), s(item, "tool")),
            item.get("arguments").cloned().unwrap_or(Value::Null),
        ),
        "dynamicToolCall" => (
            s(item, "tool").to_string(),
            item.get("arguments").cloned().unwrap_or(Value::Null),
        ),
        other => (other.to_string(), Value::Null),
    };
    Some(AgentEvent::ToolCallStarted {
        name,
        call_id: id.to_string(),
        input,
        is_subagent: ty == "collabAgentToolCall",
        subagent_model: None,
    })
}

/// The end of the tool call `item` (named `name` when it started). Pure, see [`tool_started`].
fn tool_finished(item: &Value, id: String, name: String) -> AgentEvent {
    let status = s(item, "status");
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
    AgentEvent::ToolCallFinished {
        call_id: id,
        name,
        ok: status == "completed" && exit_ok,
        result,
        parts: item_parts(item),
    }
}

// ── history of a resumed thread ─────────────────────────────────────────────

/// Item types that are tool calls in the history of a resumed thread. (Live, any other type is
/// shown as a tool call named after it; for history only the types the driver knows are shown,
/// the rest is counted and said.)
const HISTORY_TOOL_ITEMS: [&str; 7] = [
    "commandExecution",
    "fileChange",
    "mcpToolCall",
    "dynamicToolCall",
    "collabAgentToolCall",
    "imageView",
    "imageGeneration",
];

/// The earlier history a `thread/resume` result carries (`thread.turns[].items[]`) as
/// [`AgentEvent::History`] events, oldest first. Pure: it reads the result and nothing else, so
/// the protocol's own state is exactly what it is without history.
fn thread_history(thread: &Value) -> Vec<AgentEvent> {
    let mut out = Vec::new();
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if turns.is_empty()
        && thread
            .get("preview")
            .and_then(Value::as_str)
            .is_some_and(|p| !p.is_empty())
    {
        // A thread that has a first message and no turns in the result: codex sent no history
        // (a version that only pages it), which must not read as an empty conversation.
        out.push(
            AgentEvent::Notice {
                message: "codex did not send the earlier turns of this thread".into(),
            }
            .earlier(None),
        );
    }
    for turn in turns {
        let turn_ms = turn
            .get("startedAt")
            .and_then(Value::as_u64)
            .map(|secs| secs.saturating_mul(1000));
        let items = turn.get("items").and_then(Value::as_array);
        let full = match turn.get("itemsView").and_then(Value::as_str) {
            Some(view) => view == "full",
            None => items.is_some(),
        };
        if !full {
            out.push(
                AgentEvent::Notice {
                    message: "codex did not send the items of an earlier turn".into(),
                }
                .earlier(turn_ms),
            );
        }
        let mut skipped = 0usize;
        for item in items.map(Vec::as_slice).unwrap_or(&[]) {
            skipped += history_item(item, turn_ms, &mut out);
        }
        if skipped > 0 {
            out.push(
                AgentEvent::Notice {
                    message: format!(
                        "{skipped} items of an earlier turn are of kinds the driver does not show"
                    ),
                }
                .earlier(turn_ms),
            );
        }
    }
    out
}

/// The events of one item of an earlier turn, pushed to `out`; the number of items skipped (0
/// or 1).
fn history_item(item: &Value, turn_ms: Option<u64>, out: &mut Vec<AgentEvent>) -> usize {
    let started = item.get("startedAtMs").and_then(Value::as_u64).or(turn_ms);
    let completed = item
        .get("completedAtMs")
        .and_then(Value::as_u64)
        .or(started);
    let id = opt_s(item, "id").unwrap_or_default();
    match s(item, "type") {
        "userMessage" => {
            let mut text: Vec<&str> = Vec::new();
            let mut parts = Vec::new();
            for block in item
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                match s(block, "type") {
                    "text" => text.push(s(block, "text")),
                    "image" => {
                        if let Some(p) = history_image(block) {
                            parts.push(p);
                        }
                    }
                    "localImage" => {
                        if let Some(path) = opt_s(block, "path") {
                            parts.push(path_image(path));
                        }
                    }
                    _ => {}
                }
            }
            let text = text.join("\n");
            if !text.is_empty() {
                out.push(AgentEvent::UserMessage { text }.earlier(started));
            }
            if !parts.is_empty() {
                out.push(AgentEvent::UserContent { parts }.earlier(started));
            }
        }
        "agentMessage" => {
            let text = s(item, "text");
            if !text.is_empty() {
                out.push(
                    AgentEvent::AssistantDelta {
                        text: text.to_string(),
                        message_id: Some(id).filter(|i| !i.is_empty()),
                    }
                    .earlier(started),
                );
            }
        }
        "reasoning" => {
            for key in ["summary", "content"] {
                for text in item
                    .get(key)
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|t| !t.is_empty())
                {
                    out.push(
                        AgentEvent::ThinkingDelta {
                            text: text.to_string(),
                            message_id: Some(id.clone()).filter(|i| !i.is_empty()),
                        }
                        .earlier(started),
                    );
                }
            }
        }
        "contextCompaction" => out.push(
            AgentEvent::Compaction {
                info: CompactionInfo::default(),
            }
            .earlier(started),
        ),
        ty if HISTORY_TOOL_ITEMS.contains(&ty) && !id.is_empty() => {
            if let Some(start) = tool_started(item, &id) {
                let name = match &start {
                    AgentEvent::ToolCallStarted { name, .. } => name.clone(),
                    _ => String::new(),
                };
                out.push(start.earlier(started));
                // A call that never completed (the turn was cut) has no end to report.
                if s(item, "status") != "inProgress" {
                    out.push(tool_finished(item, id, name).earlier(completed));
                }
            }
        }
        _ => return 1,
    }
    0
}

/// An `image` input block: a `data:` URL carries the bytes, any other URL is a link.
fn history_image(block: &Value) -> Option<ContentPart> {
    let url = opt_s(block, "url").or_else(|| opt_s(block, "imageUrl"))?;
    Some(match data_url(&url) {
        Some((mime, data)) => ContentPart::inline_image(mime, data),
        None => ContentPart::Image {
            mime: sniff_mime(&[], &url),
            source: MediaSource::Url { url },
            width: None,
            height: None,
            alt: None,
        },
    })
}

fn path_image(path: String) -> ContentPart {
    ContentPart::Image {
        mime: sniff_mime(&[], &path),
        alt: Some(base_name(&path)),
        source: MediaSource::Path { path },
        width: None,
        height: None,
    }
}

/// `turn/start` input: the text (with non-image files named by path), then each image as a
/// `localImage` (the uploaded copy on this host).
fn user_input(text: &str, attachments: &[Attachment]) -> Vec<Value> {
    let (images, files): (Vec<&Attachment>, Vec<&Attachment>) = attachments
        .iter()
        .partition(|a| COMMON_IMAGE_MIMES.contains(&a.mime.as_str()));
    let mut input =
        vec![json!({"type": "text", "text": format!("{text}{}", path_reference_text(&files))})];
    for a in images {
        input.push(json!({"type": "localImage", "path": a.path}));
    }
    input
}

/// Images an item shows: `imageView.path`, `imageGeneration.savedPath`, MCP result image
/// blocks, dynamic tool `inputImage` items.
fn item_parts(item: &Value) -> Vec<ContentPart> {
    let image_at = |path: String| ContentPart::Image {
        mime: sniff_mime(&[], &path),
        alt: Some(base_name(&path)),
        source: MediaSource::Path { path },
        width: None,
        height: None,
    };
    match s(item, "type") {
        "imageView" => opt_s(item, "path").map(image_at).into_iter().collect(),
        "imageGeneration" => opt_s(item, "savedPath").map(image_at).into_iter().collect(),
        _ => {
            let mut out = Vec::new();
            let blocks = item
                .get("result")
                .and_then(|r| r.get("content"))
                .or_else(|| item.get("contentItems"))
                .and_then(Value::as_array);
            for b in blocks.into_iter().flatten() {
                match s(b, "type") {
                    "image" => {
                        if let Some(data) = opt_s(b, "data") {
                            out.push(ContentPart::inline_image(
                                opt_s(b, "mimeType").unwrap_or_else(|| "image/png".into()),
                                data,
                            ));
                        }
                    }
                    "inputImage" => {
                        if let Some((mime, data)) = data_url(s(b, "imageUrl")) {
                            out.push(ContentPart::inline_image(mime, data));
                        }
                    }
                    _ => {}
                }
            }
            out
        }
    }
}

/// `(mime, base64)` of a `data:<mime>;base64,<data>` URL.
fn data_url(u: &str) -> Option<(String, String)> {
    let rest = u.strip_prefix("data:")?;
    let (head, data) = rest.split_once(',')?;
    let mime = head.strip_suffix(";base64")?;
    Some((mime.to_string(), data.to_string()))
}

impl Protocol for CodexProtocol {
    fn start(&mut self, spec: &SpawnSpec) -> Step {
        self.spec = spec.clone();
        self.phase = Phase::Handshaking;
        self.mode = None;
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

    fn encode(&mut self, input: InputId, cmd: Command) -> Result<Encoded, DriverError> {
        if self.phase == Phase::Closed {
            return Err(DriverError::Shutdown);
        }
        if matches!(cmd, Command::RunSlash { .. }) {
            return self.encode_now(input, cmd).map(Encoded::Sent);
        }
        if matches!(cmd, Command::Interrupt) && self.phase != Phase::Ready {
            // Nothing is running yet: what the interrupt clears is the queued prompts.
            let dropped = self.drop_queued_prompts(InputDropReason::Interrupted);
            return Ok(Encoded::Sent(events(dropped)));
        }
        if self.must_queue(&cmd) {
            self.queue.push_back((input, cmd));
            return Ok(Encoded::Queued);
        }
        self.encode_now(input, cmd).map(Encoded::Sent)
    }
}

fn events(events: Vec<AgentEvent>) -> Step {
    Step {
        events,
        ..Step::default()
    }
}

fn dropped(input: InputId, reason: InputDropReason) -> AgentEvent {
    AgentEvent::InputDropped {
        input,
        reason,
        error: None,
    }
}

/// The CLI answered an input's request with an error: it will not run it.
fn refused(input: InputId, message: &str) -> AgentEvent {
    AgentEvent::InputDropped {
        input,
        reason: InputDropReason::Refused,
        error: Some(message.to_string()),
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

/// The gate a server request raises, with the JSON-RPC result each choice answers it with.
fn build_gate(
    routing_id: &str,
    method: &str,
    params: &Value,
) -> (GateRequest, Vec<(String, Value)>) {
    match method {
        "item/tool/requestUserInput" => (
            GateRequest::Question {
                request_id: routing_id.to_string(),
                questions: params
                    .get("questions")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().map(user_input_question).collect())
                    .unwrap_or_default(),
            },
            vec![],
        ),
        "mcpServer/elicitation/request" => {
            let message = s(params, "message").to_string();
            let pairs = wasit_core::wire::elicitation_choices(params.get("requestedSchema"));
            let answers = pairs
                .iter()
                .map(|(c, content)| {
                    let r = match content {
                        Some(c) => json!({"action": "accept", "content": c}),
                        None => json!({"action": c.id}),
                    };
                    (c.id.clone(), r)
                })
                .collect();
            (
                GateRequest::Permission {
                    request_id: routing_id.to_string(),
                    tool_name: opt_s(params, "serverName")
                        .or_else(|| opt_s(params, "server"))
                        .unwrap_or_else(|| "mcp".into()),
                    command: message.clone(),
                    cwd: String::new(),
                    explanation: message,
                    choices: pairs.into_iter().map(|(c, _)| c).collect(),
                    raw_input: params.clone(),
                },
                answers,
            )
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
            let pairs = approval_choices(method, params);
            (
                GateRequest::Permission {
                    request_id: routing_id.to_string(),
                    tool_name: tool.to_string(),
                    command,
                    cwd: s(params, "cwd").to_string(),
                    explanation: s(params, "reason").to_string(),
                    choices: pairs.iter().map(|(c, _)| c.clone()).collect(),
                    raw_input: params.clone(),
                },
                pairs.into_iter().map(|(c, r)| (c.id, r)).collect(),
            )
        }
    }
}

fn user_input_question(q: &Value) -> QuestionPrompt {
    let options: Vec<QuestionOption> = q
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
        .unwrap_or_default();
    let flag = |k: &str| q.get(k).and_then(Value::as_bool).unwrap_or(false);
    QuestionPrompt {
        id: s(q, "id").to_string(),
        question: s(q, "question").to_string(),
        header: s(q, "header").to_string(),
        multi_select: false,
        allow_other: flag("isOther") || options.is_empty(),
        secret: flag("isSecret"),
        options,
    }
}

/// The decisions an approval request accepts (codex-cli 0.169.1 app-server schema), each as a
/// choice with the result it sends. Codex carries no wording, so labels are neutral. A
/// `commandExecution` request lists its decisions in `availableDecisions`; otherwise the
/// method's full decision set applies, with the policy amendments codex proposed in the request.
fn approval_choices(method: &str, params: &Value) -> Vec<(GateChoice, Value)> {
    use GateChoiceKind::*;
    let decision = |id: &str, label: &str, kind, d: Value| {
        (GateChoice::neutral(id, label, kind), json!({"decision": d}))
    };
    let amendment = params
        .get("proposedExecpolicyAmendment")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty())
        .cloned();
    let prefix = |a: &[Value]| {
        a.iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" ")
    };
    match method {
        "item/permissions/requestApproval" => {
            let perms = params
                .get("permissions")
                .cloned()
                .unwrap_or_else(|| json!({}));
            vec![
                (
                    GateChoice::neutral("accept", "Allow", AllowOnce),
                    json!({"permissions": perms, "scope": "turn"}),
                ),
                (
                    GateChoice::neutral("acceptForSession", "Allow for this session", AllowAlways),
                    json!({"permissions": perms, "scope": "session"}),
                ),
                (
                    GateChoice::neutral("decline", "Deny", RejectOnce),
                    json!({"permissions": {}}),
                ),
            ]
        }
        "execCommandApproval" | "applyPatchApproval" => {
            let mut v = vec![
                decision("approved", "Allow", AllowOnce, json!("approved")),
                decision(
                    "approved_for_session",
                    "Allow for this session",
                    AllowAlways,
                    json!("approved_for_session"),
                ),
            ];
            if let Some(a) = &amendment {
                let (c, r) = decision(
                    "approved_execpolicy_amendment",
                    "Allow and remember",
                    AllowAlways,
                    json!({"approved_execpolicy_amendment": {"proposed_execpolicy_amendment": a}}),
                );
                v.push((
                    c.with_description(format!("commands starting with `{}`", prefix(a))),
                    r,
                ));
            }
            v.push(decision("denied", "Deny", RejectOnce, json!("denied")));
            v.push(decision(
                "abort",
                "Deny and stop the turn",
                Cancel,
                json!("abort"),
            ));
            v
        }
        _ => {
            let listed: Vec<Value> = params
                .get("availableDecisions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_else(|| {
                    let mut d = vec![json!("accept"), json!("acceptForSession")];
                    if method == "item/commandExecution/requestApproval" {
                        if let Some(a) = &amendment {
                            d.push(json!({"acceptWithExecpolicyAmendment": {"execpolicy_amendment": a}}));
                        }
                        for n in params
                            .get("proposedNetworkPolicyAmendments")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                        {
                            d.push(json!({"applyNetworkPolicyAmendment": {"network_policy_amendment": n}}));
                        }
                    }
                    d.push(json!("decline"));
                    d.push(json!("cancel"));
                    d
                });
            listed
                .into_iter()
                .enumerate()
                .filter_map(|(i, d)| v2_decision(i, d))
                .collect()
        }
    }
}

/// One v2 `CommandExecutionApprovalDecision` / `FileChangeApprovalDecision` as a choice.
fn v2_decision(i: usize, d: Value) -> Option<(GateChoice, Value)> {
    use GateChoiceKind::*;
    let choice = match &d {
        Value::String(k) => match k.as_str() {
            "accept" => GateChoice::neutral("accept", "Allow", AllowOnce),
            "acceptForSession" => {
                GateChoice::neutral("acceptForSession", "Allow for this session", AllowAlways)
            }
            "decline" => GateChoice::neutral("decline", "Deny", RejectOnce),
            "cancel" => GateChoice::neutral("cancel", "Deny and stop the turn", Cancel),
            _ => return None,
        },
        Value::Object(o) => {
            if let Some(a) = o.get("acceptWithExecpolicyAmendment") {
                let prefix = a
                    .get("execpolicy_amendment")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .unwrap_or_default();
                GateChoice::neutral(
                    "acceptWithExecpolicyAmendment",
                    "Allow and remember",
                    AllowAlways,
                )
                .with_description(format!("commands starting with `{prefix}`"))
            } else {
                let n = o
                    .get("applyNetworkPolicyAmendment")?
                    .get("network_policy_amendment")
                    .cloned()
                    .unwrap_or_default();
                let allow = s(&n, "action") != "deny";
                GateChoice::neutral(
                    format!("network:{i}"),
                    if allow {
                        "Allow and remember"
                    } else {
                        "Deny and remember"
                    },
                    if allow { AllowAlways } else { RejectAlways },
                )
                .with_description(format!("network access to {}", s(&n, "host")))
            }
        }
        _ => return None,
    };
    Some((choice, json!({"decision": d})))
}

/// The result that declines a gate when the turn is interrupted (the `turn/interrupt` that
/// follows stops the turn, so this is the plain refusal of each method).
fn interrupt_answer(g: &PendingGate) -> Value {
    match g.method.as_str() {
        "item/tool/requestUserInput" => json!({"answers": {}}),
        "mcpServer/elicitation/request" => json!({"action": "decline"}),
        "item/permissions/requestApproval" => json!({"permissions": {}}),
        "execCommandApproval" | "applyPatchApproval" => json!({"decision": "denied"}),
        _ => json!({"decision": "decline"}),
    }
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
            .get(&qid)
            .or_else(|| answer.answers.get(text))
        {
            let parts: Vec<Value> = a
                .iter()
                .map(|p| p.trim())
                .filter(|p| !p.is_empty())
                .map(|p| Value::String(p.to_string()))
                .collect();
            answers.insert(qid, json!({"answers": parts}));
        }
    }
    rpc_result(&gate.raw_id, json!({"answers": answers}))
}
