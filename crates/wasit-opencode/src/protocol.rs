//! ACP (Agent Client Protocol, JSON-RPC 2.0 over NDJSON) as a pure
//! [`Protocol`], shared by the OpenCode and Kilo Code flavors.
//!
//! Wire facts (verified against `opencode acp` 1.19.6 and `kilo acp` 7.8.27):
//! - Handshake: `initialize {protocolVersion: 1}` → `session/new {cwd,
//!   mcpServers}` | `session/load {sessionId, cwd, mcpServers}`. The result
//!   carries `configOptions` (`model`, `mode`, kilo also `effort`).
//!   `ResumeMode::Continue` uses `session/list {cwd}` and loads the most
//!   recently updated session. `session/load` replays the conversation as `session/update`
//!   notifications before it answers: they become [`AgentEvent::History`] events (a user
//!   message's chunks are one `UserMessage`; no replay carries a time); the replayed usage and
//!   plan are the past's and are dropped, and nothing replayed acknowledges a prompt or leaves
//!   state behind.
//! - Model / mode: `session/set_config_option {sessionId, configId, value}`;
//!   the agent also pushes `config_option_update` / `current_mode_update`.
//!   The launch model / mode are applied this way right after the session
//!   opens (`opencode acp` has no `--model`).
//! - Turns: `session/prompt` (one at a time) → `session/update`
//!   notifications → the prompt result `{stopReason, usage}`. Prompts sent
//!   during a turn are held by this protocol. Text starting with `/name`
//!   runs that command / skill. A prompt is acknowledged by the first `session/update` that is
//!   the agent working on it (message / thought chunk, tool call, plan; for this session) or
//!   by the prompt's result, whichever comes first; an error answering a prompt nothing had
//!   acknowledged is [`AgentEvent::InputDropped`] `Refused`.
//! - Interrupt: `session/cancel` (a notification); pending permission
//!   requests are answered `{"outcome":{"outcome":"cancelled"}}` first.
//! - Gates: `session/request_permission {toolCall, options}` answered with
//!   `{"outcome":{"outcome":"selected","optionId":…}}`.
//!   Each option becomes a [`GateChoice`] with the option's id, its `name` as the label
//!   (the agent's own wording) and its `kind` mapped one to one; `AnswerGate` selects the
//!   chosen option by id. ACP has no question requests, so `AnswerQuestion` is refused.

use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::{json, Value};
use wasit_core::content::{
    b64_decode, b64_encode, base_name, file_uri_path, is_text_mime, path_file_uri,
};
use wasit_core::wire::{
    id_text, opt_s, rpc_error, rpc_notification, rpc_request, rpc_result, s, u, RpcIn,
};
use wasit_core::{
    AgentEvent, Attachment, Command, CompactionInfo, ContentPart, DriverError, Encoded, EndReason,
    GateChoice, GateChoiceKind, GateDecision, GateRequest, InputDropReason, InputId, MediaSource,
    ModelInfo, OutLine, PlanEntry, Protocol, ResumeMode, SessionMode, SlashCommandInfo, SpawnSpec,
    Step, Usage,
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
    /// `session/prompt` for this input.
    Prompt(InputId),
}

impl Req {
    fn command(self) -> &'static str {
        match self {
            Req::Initialize => "initialize",
            Req::List => "continue",
            Req::Open => "session/open",
            Req::SetModel => "set_model",
            Req::SetMode => "set_mode",
            Req::Prompt(_) => "prompt",
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
    queue: VecDeque<(InputId, Command)>,
    spec: SpawnSpec,
    session_id: Option<String>,
    can_list: bool,
    /// `session/load` in flight: its history replay is not live activity.
    loading: bool,
    /// The user message being replayed (its `messageId` and the text of its chunks so far),
    /// emitted as one event when the replay moves on.
    replay_user: Option<(Option<String>, String)>,
    prompt_active: bool,
    /// The written `session/prompt` the CLI has not shown any sign of taking yet.
    awaiting_ack: Option<InputId>,
    cancel_sent: bool,
    model: Option<String>,
    mode: Option<String>,
    gates: Vec<PendingGate>,
    tools: HashMap<String, String>,
    subagents: HashSet<String>,
    subagents_started: HashSet<String>,
    /// `promptCapabilities` from `initialize`.
    prompt_image: bool,
    prompt_embedded: bool,
    /// The session's cost in USD as the CLI last reported it. ACP `usage_update.cost` is
    /// cumulative for the whole session (kilo sums every assistant message's cost), and the
    /// prompt result carries no cost, so every `TurnCompleted` reports this value.
    session_cost: Option<f64>,
    /// The tokens of this process's prompts, summed (a prompt's `usage` is that prompt's own).
    session_tokens: Usage,
    /// The session cost as of the last turn's end: the baseline the next turn's cost is counted
    /// from. A new session starts at zero; a loaded one is unknown until a cost is reported.
    cost_base: Option<f64>,
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
            replay_user: None,
            prompt_active: false,
            awaiting_ack: None,
            cancel_sent: false,
            model: None,
            mode: None,
            gates: Vec::new(),
            tools: HashMap::new(),
            subagents: HashSet::new(),
            prompt_image: false,
            prompt_embedded: false,
            subagents_started: HashSet::new(),
            session_cost: None,
            session_tokens: Usage::default(),
            cost_base: None,
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
        let pc = caps.get("promptCapabilities");
        let flag = |k: &str| pc.and_then(|p| p.get(k)).and_then(Value::as_bool) == Some(true);
        self.prompt_image = flag("image");
        self.prompt_embedded = flag("embeddedContext");
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
            // Nothing asks for a fork before the fork capability is implemented: start a new
            // session and say so, rather than loading (and so writing to) the source.
            ResumeMode::Fork(_) => {
                let line = self.session_new();
                return Step {
                    events: vec![AgentEvent::CommandRejected {
                        command: "fork".into(),
                        message: "forking is not supported; started a new session".into(),
                    }],
                    write: vec![line],
                    close_stdin: false,
                };
            }
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
        // The replay is over: the last replayed message is complete, and the tool calls it
        // showed are not running (nothing replayed may leave state behind).
        let replayed = self.flush_replayed_user();
        if self.loading {
            self.tools.clear();
            self.subagents.clear();
            self.subagents_started.clear();
        } else {
            // A new session has spent nothing yet; a loaded one's cost is not known from here.
            self.cost_base = Some(0.0);
        }
        self.loading = false;
        if let Some(id) = opt_s(result, "sessionId") {
            self.session_id = Some(id);
        }
        let (modes, mode) = config_modes(result);
        let (models, model) = config_models(result);
        self.model = model.clone();
        self.mode = mode.clone();
        let mut opened = replayed;
        opened.push(AgentEvent::SessionInit {
            native_session_id: self.session_id.clone(),
            model,
            cwd: self.spec.cwd.clone(),
            tools: vec![],
            mcp_servers: vec![],
            slash_commands: vec![],
            available_modes: modes,
            current_mode: mode.unwrap_or_default(),
            cli_version: String::new(),
        });
        let mut step = events(opened);
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

    fn encode_now(&mut self, input: InputId, cmd: Command) -> Result<Step, DriverError> {
        let mut step = Step::default();
        match cmd {
            Command::Prompt { text, attachments } => step = self.prompt(input, &text, &attachments),
            Command::RunSlash { name, args } => {
                let name = name.trim_start_matches('/');
                let text = if args.trim().is_empty() {
                    format!("/{name}")
                } else {
                    format!("/{name} {}", args.trim())
                };
                step = self.prompt(input, &text, &[]);
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
            Command::StopTask { .. } => {
                return Err(DriverError::Unsupported {
                    operation: "stop_task".into(),
                })
            }
            Command::SteerSubagent { .. } => {
                return Err(DriverError::Unsupported {
                    operation: "steer_subagent".into(),
                })
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
            Command::Compact => {
                return Err(DriverError::Unsupported {
                    operation: "compact".into(),
                })
            }
            Command::RunShell { .. } => {
                return Err(DriverError::Unsupported {
                    operation: "run_shell".into(),
                })
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

    fn prompt(&mut self, input: InputId, text: &str, attachments: &[Attachment]) -> Step {
        self.prompt_active = true;
        self.awaiting_ack = Some(input);
        self.cancel_sent = false;
        let blocks = prompt_blocks(text, attachments, self.prompt_image, self.prompt_embedded);
        let line = self.request(
            Req::Prompt(input),
            "session/prompt",
            json!({"sessionId": self.sid(), "prompt": blocks}),
        );
        Step {
            events: vec![AgentEvent::turn_started(None, Some(input))],
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

    /// Take the queued prompts out of the queue: one `InputDropped` each, in queue order.
    fn drop_queued_prompts(&mut self, reason: InputDropReason) -> Vec<AgentEvent> {
        let (prompts, rest): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.queue)
            .into_iter()
            .partition(|(_, c)| matches!(c, Command::Prompt { .. } | Command::RunSlash { .. }));
        self.queue = rest;
        prompts
            .into_iter()
            .map(|(input, _)| dropped(input, reason))
            .collect()
    }

    fn interrupt(&mut self) -> Step {
        let mut step = events(self.drop_queued_prompts(InputDropReason::Interrupted));
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
        while let Some((_, cmd)) = self.queue.front() {
            let prompt = matches!(cmd, Command::Prompt { .. } | Command::RunSlash { .. });
            if self.phase != Phase::Ready || (prompt && self.prompt_active) {
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
            Req::Prompt(input) => self.on_prompt_result(input, &result),
        }
    }

    fn on_error(&mut self, id: &Value, message: String) -> Step {
        let Some(req) = id.as_u64().and_then(|n| self.outstanding.remove(&n)) else {
            return events(vec![AgentEvent::Unknown {
                raw: json!({"id": id, "error": {"message": message}}),
            }]);
        };
        match req {
            Req::Prompt(input) => {
                self.prompt_active = false;
                self.cancel_sent = false;
                let mut step = Step::default();
                // Refused: the CLI never showed a sign of taking the prompt (an error after it
                // streamed something is the turn failing, not the prompt being refused).
                if self.awaiting_ack == Some(input) {
                    self.awaiting_ack = None;
                    step.events.push(AgentEvent::InputDropped {
                        input,
                        reason: InputDropReason::Refused,
                        error: Some(message.clone()),
                    });
                }
                let (usage, turn_usage) = self.close_turn(&Value::Null);
                step.events.push(AgentEvent::TurnCompleted {
                    end_reason: Some(EndReason::ExecutionError),
                    was_interrupted: false,
                    num_turns: 0,
                    usage,
                    turn_usage,
                    per_model: HashMap::new(),
                    reply: Some(message),
                    origin: None,
                });
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

    fn on_prompt_result(&mut self, input: InputId, result: &Value) -> Step {
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
        // A result is the CLI's own answer to the prompt, even for a turn that produced nothing.
        step.events.extend(self.acknowledge(input));
        for g in std::mem::take(&mut self.gates) {
            step.events.push(AgentEvent::GateResolved {
                request_id: g.routing_id,
                reason: None,
            });
        }
        let (usage, turn_usage) = self.close_turn(&usage);
        step.events.push(AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted: cancelled,
            num_turns: 0,
            usage,
            turn_usage,
            per_model: HashMap::new(),
            reply: None,
            origin: None,
        });
        step.extend(self.drain());
        step
    }

    /// Closes a turn's usage: `tokens` is the prompt result's `usage` (`Null` when the prompt
    /// produced none). Returns the cumulative usage for the process and this turn's own usage.
    ///
    /// The turn's tokens are the prompt's own; the turn's cost is the session cost's growth since
    /// the previous turn ended. Where that baseline is unknown (a loaded session whose cost was
    /// never reported before this turn) the turn has no usage it can state, so it is `None`.
    fn close_turn(&mut self, tokens: &Value) -> (Usage, Option<Usage>) {
        let prompt = tokens.is_object().then(|| Usage {
            input_tokens: u(tokens, "inputTokens"),
            output_tokens: u(tokens, "outputTokens"),
            cache_read_tokens: u(tokens, "cachedReadTokens"),
            cache_creation_tokens: u(tokens, "cachedWriteTokens"),
            cost_usd: 0.0,
        });
        // Cost not reported at all means no cost was spent: 0.0, as `Usage` documents.
        let turn_cost = match (self.cost_base, self.session_cost) {
            (Some(base), Some(now)) => (now >= base).then_some(now - base),
            (Some(_), None) | (None, None) => Some(0.0),
            (None, Some(_)) => None,
        };
        if let Some(p) = &prompt {
            self.session_tokens.input_tokens += p.input_tokens;
            self.session_tokens.output_tokens += p.output_tokens;
            self.session_tokens.cache_read_tokens += p.cache_read_tokens;
            self.session_tokens.cache_creation_tokens += p.cache_creation_tokens;
        }
        let turn = match (prompt, turn_cost) {
            (Some(p), Some(cost)) => Some(Usage {
                cost_usd: cost,
                ..p
            }),
            _ => None,
        };
        if self.session_cost.is_some() {
            self.cost_base = self.session_cost;
        }
        let cumulative = Usage {
            cost_usd: self.session_cost.unwrap_or(0.0),
            ..self.session_tokens.clone()
        };
        (cumulative, turn)
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

    /// The CLI took the prompt written as `input` (the first sign of it, or its result): report
    /// it once.
    fn acknowledge(&mut self, input: InputId) -> Vec<AgentEvent> {
        if self.awaiting_ack != Some(input) {
            return vec![];
        }
        self.awaiting_ack = None;
        vec![AgentEvent::InputAcknowledged {
            input,
            steered: false,
        }]
    }

    /// `session/update`: the events it carries, after the acknowledgement it may be the first
    /// sign of.
    ///
    /// Only updates that are the agent working on the prompt acknowledge it, and only for this
    /// session: `available_commands_update` and `config_option_update` follow `session/new` and
    /// `set_config_option` and arrive after the prompt was written whether or not it is taken
    /// (all recordings), and `usage_update` precedes the error of a prompt kilo refuses
    /// (`reference/kilo/error.jsonl`, `kilo-refused.ndjson`: "You need to sign in to use this
    /// model."). History replayed by `session/load` is never activity. An update from the
    /// previous prompt cannot arrive after this one was written: ACP sends all of a turn's
    /// updates before its result (every recording), and one prompt is in flight at a time here.
    fn on_update(&mut self, params: &Value) -> Vec<AgentEvent> {
        let kind = params
            .get("update")
            .and_then(|u| u.get("sessionUpdate"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let ours = params.get("sessionId").and_then(Value::as_str) == self.session_id.as_deref();
        let mut out = match self.awaiting_ack {
            Some(input)
                if !self.loading
                    && ours
                    && matches!(
                        kind,
                        "agent_message_chunk"
                            | "agent_thought_chunk"
                            | "tool_call"
                            | "tool_call_update"
                            | "plan"
                    ) =>
            {
                self.acknowledge(input)
            }
            _ => vec![],
        };
        out.extend(self.update_events(params));
        out
    }

    fn update_events(&mut self, params: &Value) -> Vec<AgentEvent> {
        let Some(update) = params.get("update") else {
            return vec![];
        };
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.loading {
            if let Some(history) = self.replayed(kind, update) {
                return history;
            }
        }
        match kind {
            "agent_message_chunk" | "agent_thought_chunk" => chunk_events(kind, update),
            "user_message_chunk" => vec![],
            "tool_call" => self.on_tool_call(update),
            "tool_call_update" => self.on_tool_update(update),
            "usage_update" => {
                let reported = reported_cost(update);
                if let Some(cost) = reported {
                    self.session_cost = Some(cost);
                }
                let size = u(update, "size");
                vec![AgentEvent::UsageUpdate {
                    usage: Usage {
                        cost_usd: reported.unwrap_or(0.0),
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

    /// What `session/load` replays of the conversation, as history; `None` for an update that is
    /// not part of the replay (it is handled like any other). Replayed usage and plans are the
    /// past's: they change nothing and are not shown. Chunks of one user message are one event.
    fn replayed(&mut self, kind: &str, update: &Value) -> Option<Vec<AgentEvent>> {
        let message_id = opt_s(update, "messageId");
        let mut out = Vec::new();
        if kind == "user_message_chunk" {
            let content = update.get("content");
            let text = content.and_then(|_| text_of(update));
            let part = content.and_then(acp_part);
            if self
                .replay_user
                .as_ref()
                .is_some_and(|(id, _)| *id != message_id)
            {
                out.extend(self.flush_replayed_user());
            }
            if let Some(t) = text {
                match &mut self.replay_user {
                    Some((_, buf)) => buf.push_str(&t),
                    None => self.replay_user = Some((message_id, t)),
                }
            }
            if let Some(part) = part {
                out.extend(self.flush_replayed_user());
                out.push(AgentEvent::UserContent { parts: vec![part] }.earlier(None));
            }
            return Some(out);
        }
        let replayed = matches!(
            kind,
            "agent_message_chunk"
                | "agent_thought_chunk"
                | "tool_call"
                | "tool_call_update"
                | "plan"
                | "usage_update"
        );
        if !replayed {
            return None;
        }
        out.extend(self.flush_replayed_user());
        let events = match kind {
            "agent_message_chunk" | "agent_thought_chunk" => chunk_events(kind, update),
            "tool_call" => self.on_tool_call(update),
            "tool_call_update" => self.on_tool_update(update),
            // The past's plan and usage are not the session's.
            _ => Vec::new(),
        };
        out.extend(events.into_iter().map(|e| e.earlier(None)));
        Some(out)
    }

    /// The replayed user message, once complete.
    fn flush_replayed_user(&mut self) -> Vec<AgentEvent> {
        match self.replay_user.take() {
            Some((_, text)) if !text.is_empty() => {
                vec![AgentEvent::UserMessage { text }.earlier(None)]
            }
            _ => Vec::new(),
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
        let parts = tool_parts(&content);
        if terminal {
            self.tools.remove(&id);
            let ok = status == "completed";
            return vec![AgentEvent::ToolCallFinished {
                call_id: id,
                name,
                ok,
                result: (!content.is_empty()).then_some(Value::Array(content)),
                parts: without_diffs_unless(parts, ok),
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
            parts,
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
                parts: without_diffs_unless(tool_parts(&content), status == "completed"),
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

    fn encode(&mut self, input: InputId, cmd: Command) -> Result<Encoded, DriverError> {
        if self.phase == Phase::Closed {
            return Err(DriverError::Shutdown);
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

/// The events of an agent message / thought chunk (the same live and in a replayed history).
/// The session cost a `usage_update` reports, in USD, when it reports a positive one. Zero,
/// absent and non-USD costs are not reports: the cost is cumulative, so a zero never supersedes
/// a cost already known.
fn reported_cost(update: &Value) -> Option<f64> {
    let cost = update.get("cost")?;
    if s(cost, "currency") != "USD" {
        return None;
    }
    cost.get("amount")
        .and_then(Value::as_f64)
        .filter(|amount| *amount > 0.0)
}

fn chunk_events(kind: &str, update: &Value) -> Vec<AgentEvent> {
    let message_id = opt_s(update, "messageId");
    if kind == "agent_thought_chunk" {
        return text_of(update)
            .map(|text| AgentEvent::ThinkingDelta { text, message_id })
            .into_iter()
            .collect();
    }
    match update.get("content").and_then(acp_part) {
        Some(part) => vec![AgentEvent::AssistantContent {
            parts: vec![part],
            message_id,
        }],
        None => text_of(update)
            .map(|text| AgentEvent::AssistantDelta { text, message_id })
            .into_iter()
            .collect(),
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

/// The non-text part an ACP content block carries (`image`, `audio`, `resource`,
/// `resource_link`); `None` for text and unknown blocks.
pub(crate) fn acp_part(block: &Value) -> Option<ContentPart> {
    let mime = opt_s(block, "mimeType");
    match s(block, "type") {
        "image" => {
            let source = match (opt_s(block, "data"), opt_s(block, "uri")) {
                (Some(data), _) => MediaSource::Inline { data },
                (None, Some(uri)) => uri_source(&uri),
                (None, None) => return None,
            };
            Some(ContentPart::Image {
                mime: mime.unwrap_or_else(|| "image/png".into()),
                source,
                width: None,
                height: None,
                alt: None,
            })
        }
        "audio" => Some(ContentPart::File {
            source: MediaSource::Inline {
                data: opt_s(block, "data")?,
            },
            name: None,
            mime,
            uri: None,
        }),
        "resource" => {
            let r = block.get("resource")?;
            let uri = opt_s(r, "uri");
            let data = match (opt_s(r, "blob"), r.get("text").and_then(Value::as_str)) {
                (Some(b), _) => b,
                (None, Some(t)) => b64_encode(t.as_bytes()),
                (None, None) => return None,
            };
            Some(ContentPart::File {
                source: MediaSource::Inline { data },
                name: uri.as_deref().map(base_name),
                mime: opt_s(r, "mimeType"),
                uri,
            })
        }
        "resource_link" => {
            let uri = opt_s(block, "uri")?;
            Some(ContentPart::File {
                source: uri_source(&uri),
                name: opt_s(block, "name").or_else(|| Some(base_name(&uri))),
                mime,
                uri: Some(uri),
            })
        }
        _ => None,
    }
}

fn uri_source(uri: &str) -> MediaSource {
    match file_uri_path(uri) {
        Some(path) => MediaSource::Path { path },
        None => MediaSource::Url {
            url: uri.to_string(),
        },
    }
}

/// The non-text parts of ACP tool call content (`{"type":"content","content":{block}}`).
fn tool_parts(content: &[Value]) -> Vec<ContentPart> {
    content
        .iter()
        .filter_map(|c| match s(c, "type") {
            "content" => c.get("content").and_then(acp_part),
            "diff" => acp_diff(c),
            _ => None,
        })
        .collect()
}

/// An ACP `diff` block (`{"type":"diff","path","oldText","newText"}`) as a diff part; `oldText`
/// is absent or null for a file the call created.
fn acp_diff(block: &Value) -> Option<ContentPart> {
    Some(ContentPart::Diff {
        path: opt_s(block, "path")?,
        old_text: block
            .get("oldText")
            .and_then(Value::as_str)
            .map(str::to_string),
        new_text: block.get("newText").and_then(Value::as_str)?.to_string(),
        hunk: None,
    })
}

/// Drops the diff parts of a call that did not complete: a failed edit changed nothing.
fn without_diffs_unless(mut parts: Vec<ContentPart>, completed: bool) -> Vec<ContentPart> {
    if !completed {
        parts.retain(|p| !matches!(p, ContentPart::Diff { .. }));
    }
    parts
}

/// The `session/prompt` blocks for a prompt: the text, then each attachment as an `image`
/// block (when the agent takes images), an embedded `resource` (text files, when the agent
/// takes embedded context) or a `resource_link` to the uploaded copy (every ACP agent takes
/// links).
pub(crate) fn prompt_blocks(
    text: &str,
    attachments: &[Attachment],
    image: bool,
    embedded: bool,
) -> Vec<Value> {
    let mut blocks = vec![json!({"type": "text", "text": text})];
    for a in attachments {
        let uri = path_file_uri(&a.path);
        let block = match a.data.as_deref() {
            Some(data) if image && a.mime.starts_with("image/") => {
                json!({"type": "image", "mimeType": a.mime, "data": data, "uri": uri})
            }
            Some(data) if embedded && is_text_mime(&a.mime) => {
                match b64_decode(data).and_then(|b| String::from_utf8(b).ok()) {
                    Some(t) => json!({"type": "resource", "resource": {
                        "uri": uri, "mimeType": a.mime, "text": t}}),
                    None => resource_link(a, &uri),
                }
            }
            _ => resource_link(a, &uri),
        };
        blocks.push(block);
    }
    blocks
}

fn resource_link(a: &Attachment, uri: &str) -> Value {
    json!({"type": "resource_link", "uri": uri, "name": a.name, "mimeType": a.mime, "size": a.size})
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
