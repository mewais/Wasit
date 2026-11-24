//! The claude stream-json control protocol as a pure [`Protocol`].
//!
//! Wire facts (verified against claude 2.1.326):
//! - We open with a `control_request {subtype:"initialize"}`. Claude may
//!   print unrelated lines first (`active_goal`, `autocompact_state`); they
//!   are reported as [`AgentEvent::Unknown`]. The `initialize` response
//!   carries the slash commands, the model catalog and the current
//!   permission mode.
//! - `system/init` is printed at the start of EVERY turn, whoever started it; the first one
//!   becomes [`AgentEvent::SessionInit`], later ones only report a model change. Each is
//!   [`AgentEvent::TurnStarted`], and the turn ends at its `result`: the turn edges are the CLI's
//!   own, never inferred from what this protocol writes. The CLI starts turns by itself (verified
//!   on 2.1.293): when a background task ends it prints `system/init` … `result` for the
//!   notification, with an `origin` on the `result`. Which input a turn runs for is known only
//!   from the lifecycle `started` frame (below) that precedes its `system/init`; a turn with no
//!   such frame is the agent's own, and a CLI that reports no lifecycle leaves the input unknown
//!   (`TurnTrigger::Unreported`): a turn is never attributed to a prompt on a guess.
//! - A user message is `{"type":"user","message":{"role":"user","content":[…]}}`.
//!   A message whose text starts with `/name` runs that slash command / skill.
//!   Claude merges user messages written while a turn is starting into one
//!   turn, so this protocol holds prompts while a turn runs (the CLI's turn, whoever
//!   started it) or while the previous prompt has not started, and sends the next
//!   one after the `result` line (one prompt = one turn).
//! - Tool permission gates arrive as `control_request {subtype:"can_use_tool"}`
//!   (also `AskUserQuestion` / `ExitPlanMode` by tool name) or
//!   `{subtype:"elicitation"}`; they are answered with `control_response`.
//! - `control_request {subtype:"interrupt"}` ends the running turn with a
//!   `result {subtype:"error_during_execution"}`; the result carries no
//!   interrupt flag, so the protocol remembers that it asked.
//! - A user message may carry a `uuid`. Claude (verified on 2.1.293) then
//!   reports what happens to it with `command_lifecycle {command_uuid, state}`
//!   frames: `queued` (read from stdin), `started` (taken into a turn — the
//!   acknowledgement this protocol reports), `completed`, `cancelled`; a
//!   `result` lists the uuids it ran in `user_message_uuids`. A message the CLI
//!   holds but has not started survives an `interrupt` and runs right after it;
//!   `control_request {subtype:"cancel_async_message", message_uuid}` removes
//!   it (`{cancelled:true}` plus a `cancelled` frame; `{cancelled:false}` when
//!   it already started). Nothing here assumes a CLI version: the protocol
//!   sends uuids only when the runtime gave it a session id to derive them from,
//!   acknowledges only on those frames, and asks for cancellations only after
//!   the CLI has shown it reports lifecycles. A CLI that reports nothing leaves
//!   its prompts "written, not acknowledged". A `started` that arrives while a turn is already
//!   open (or another input is about to start it) joined that turn: the acknowledgement says
//!   `steered`.
//! - Subagents (`Agent` / `Task` tool) and shell commands run as *tasks*. The CLI backgrounds
//!   them even when told to wait: the tool result returns at once, the parent's turn ends, and
//!   the work continues, reported by `system` frames `task_started`, `task_progress`,
//!   `task_updated`, `task_notification` (the end) and `background_tasks_changed` (the
//!   complete list of background tasks). Every line of a subagent's own conversation carries
//!   `parent_tool_use_id` (the spawning call), `agent_id`, `subagent_type`: those events are
//!   [`AgentEvent::Scoped`]. `interrupt` does not stop background tasks (verified); the control
//!   request `stop_task {task_id}` does (an unknown id answers success and does nothing).
//! - `set_model` / `set_permission_mode` control requests switch model / mode;
//!   errors come back as `control_response {subtype:"error"}`.

use std::collections::{HashMap, VecDeque};

use crate::edit::edit_diffs;
use serde_json::{json, Value};
use wasit_core::content::{path_reference_text, COMMON_IMAGE_MIMES};
use wasit_core::wire::{opt_s, s, u};
use wasit_core::{
    AgentEvent, Attachment, BackgroundTask, ChoiceText, Command, CompactionInfo, ContentPart,
    DriverError, Encoded, EndReason, GateChoice, GateChoiceKind, GateRequest, InputDropReason,
    InputId, McpServerInfo, MediaSource, ModelInfo, ModelUsage, OutLine, Protocol, QuestionAnswer,
    QuestionOption, QuestionPrompt, RateLimitInfo, Scope, SlashCommandInfo, SpawnSpec, Step,
    TaskInfo, TaskUsage, TurnOrigin, TurnTrigger, Usage,
};

/// What a pending gate needs to encode its answer.
#[derive(Debug, Clone, PartialEq)]
enum GateKind {
    /// `can_use_tool` (incl. `ExitPlanMode`): the original input and, per choice id, the
    /// native answer.
    Tool {
        input: Value,
        answers: Vec<(String, ToolAnswer)>,
    },
    /// `AskUserQuestion`; holds the original input (its `questions`).
    Question { input: Value },
    /// MCP elicitation: per choice id, the content to accept with (`None` = the choice is
    /// `decline` / `cancel`, sent as that action).
    Elicitation {
        answers: Vec<(String, Option<Value>)>,
    },
}

/// The `can_use_tool` response a choice stands for.
#[derive(Debug, Clone, PartialEq)]
enum ToolAnswer {
    /// `{behavior:"allow"}`, with `updatedPermissions` when the choice remembers something.
    Allow { updated_permissions: Option<Value> },
    /// `{behavior:"deny", message, interrupt}`.
    Deny { interrupt: bool },
}

/// What the CLI reported about a user message we wrote (by its uuid).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliState {
    /// Nothing yet.
    Unknown,
    /// `command_lifecycle queued`: read, waiting in the CLI's own queue.
    Queued,
    /// `command_lifecycle started`: taken into a turn.
    Started,
}

/// A user message we wrote with a uuid, until the CLI reports it completed or cancelled.
#[derive(Debug, Clone)]
struct Written {
    input: InputId,
    uuid: String,
    cli: CliState,
    /// `InputAcknowledged` was reported.
    acknowledged: bool,
    /// We asked the CLI to drop it (`cancel_async_message`).
    cancel_requested: bool,
}

/// What the CLI lists in `system/init` `capabilities` when it reports `command_lifecycle`
/// frames for the user messages that carry a `uuid`.
const LIFECYCLE_CAPABILITY: &str = "msg_lifecycle_v1";

/// How many written messages are remembered for the CLI's lifecycle reports.
const MAX_WRITTEN: usize = 256;

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
    /// Our outstanding control requests: request id → command name, and the input a
    /// `cancel_async_message` is about.
    outstanding: HashMap<String, (&'static str, Option<InputId>)>,
    /// Commands waiting for the handshake or for the running turn to end.
    queue: VecDeque<(InputId, Command)>,
    /// A turn is running: `system/init` seen, `result` not (whoever started the turn).
    turn_open: bool,
    /// The input whose lifecycle `started` frame came and whose turn's `system/init` has not:
    /// the turn that starts next runs for it.
    started: Option<InputId>,
    /// A prompt was written and the CLI has said nothing about it yet (no lifecycle frame, no
    /// `system/init`, no `result` since): a turn may be starting for it. Cleared by whatever the
    /// CLI reports next.
    unobserved_write: bool,
    /// The tasks the CLI runs (the one reduction of the task events, [`wasit_core::tasks`]):
    /// which are running decides what `Interrupt` and `StopTask` do.
    tasks: Vec<TaskInfo>,
    /// Seed of the message uuids (from `SpawnSpec::session_id`); `None`: messages carry no
    /// uuid (a spec from before session ids: the lines stay what they always were).
    uuid_seed: Option<u64>,
    /// Messages written with a uuid whose lifecycle has not ended, oldest first (bounded).
    written: VecDeque<Written>,
    /// The CLI has reported a `command_lifecycle` in this process.
    lifecycle_seen: bool,
    /// The CLI said at `system/init` that it reports message lifecycles
    /// (`capabilities` holds [`LIFECYCLE_CAPABILITY`]): a turn that no `started` frame preceded
    /// is its own, even before the first frame was seen.
    lifecycle_capable: bool,
    /// We sent `interrupt` during the running turn.
    interrupt_sent: bool,
    session_init_sent: bool,
    model: Option<String>,
    /// Pending gates in arrival order.
    gates: Vec<(String, GateKind)>,
    tool_names: HashMap<String, String>,
    /// The file edits of each tool call not yet finished, attached to its result.
    tool_diffs: HashMap<String, Vec<ContentPart>>,
    last_usage_message: Option<String>,
    /// The process's cumulative usage as the last `result` reported it (the baseline of the next
    /// turn's usage). A new process starts again from zero.
    counted_usage: Usage,
    context_window: Option<u64>,
    /// The session's reported slash commands include `compact` (from the `initialize` response or
    /// `system/init`): `Command::Compact` runs `/compact` only then.
    compact_listed: bool,
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
            turn_open: false,
            started: None,
            unobserved_write: false,
            tasks: Vec::new(),
            uuid_seed: None,
            written: VecDeque::new(),
            lifecycle_seen: false,
            lifecycle_capable: false,
            interrupt_sent: false,
            session_init_sent: false,
            model: None,
            gates: Vec::new(),
            tool_names: HashMap::new(),
            tool_diffs: HashMap::new(),
            last_usage_message: None,
            counted_usage: Usage::default(),
            context_window: None,
            compact_listed: false,
        }
    }

    fn request_id(&mut self) -> String {
        let id = format!("req_{}", self.next_request);
        self.next_request += 1;
        id
    }

    fn control_request(&mut self, command: &'static str, request: Value) -> OutLine {
        self.control_request_about(command, None, request)
    }

    fn control_request_about(
        &mut self,
        command: &'static str,
        input: Option<InputId>,
        request: Value,
    ) -> OutLine {
        let id = self.request_id();
        self.outstanding.insert(id.clone(), (command, input));
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
                    | Command::Compact
                    | Command::SetModel { .. }
                    | Command::SetMode { .. }
            ),
            Phase::Ready => {
                is_turn_command(cmd) && (self.prompt_blocked() || !self.queue.is_empty())
            }
            Phase::Closed => false,
        }
    }

    /// A prompt written now would start a second turn, or merge into one that is running or
    /// starting: wait. The CLI's turn counts whoever started it; so does a prompt of ours the
    /// CLI has queued, or that it has not yet said anything about.
    fn prompt_blocked(&self) -> bool {
        self.turn_open
            || self.started.is_some()
            || self.unobserved_write
            || self.written.iter().any(|w| w.cli == CliState::Queued)
    }

    /// A turn is running or about to run: what an `interrupt` has to stop.
    fn turn_running(&self) -> bool {
        self.turn_open || self.started.is_some() || (!self.lifecycle_seen && self.unobserved_write)
    }

    fn encode_now(&mut self, input: InputId, cmd: Command) -> Result<Step, DriverError> {
        match cmd {
            Command::Prompt { text, attachments } => {
                Ok(self.write_prompt(input, &text, &attachments))
            }
            Command::RunSlash { name, args } => {
                let name = name.trim_start_matches('/');
                let text = if args.trim().is_empty() {
                    format!("/{name}")
                } else {
                    format!("/{name} {}", args.trim())
                };
                Ok(self.write_prompt(input, &text, &[]))
            }
            Command::AnswerGate {
                request_id,
                decision,
            } => {
                let kind = self.take_gate(&request_id)?;
                let line = match &kind {
                    GateKind::Question { .. } => None,
                    GateKind::Tool { input, answers } => answers
                        .iter()
                        .find(|(id, _)| *id == decision.choice)
                        .map(|(_, a)| match a {
                            ToolAnswer::Allow {
                                updated_permissions,
                            } => encode_allow(
                                &request_id,
                                input.clone(),
                                updated_permissions.clone(),
                            ),
                            ToolAnswer::Deny { interrupt } => encode_deny(
                                &request_id,
                                decision.text.as_deref().unwrap_or("denied by user"),
                                *interrupt,
                            ),
                        }),
                    GateKind::Elicitation { answers } => answers
                        .iter()
                        .find(|(id, _)| *id == decision.choice)
                        .map(|(id, content)| match content {
                            Some(c) => encode_elicitation_accept(&request_id, Some(c.clone())),
                            None if id == "cancel" => encode_elicitation_cancel(&request_id),
                            None => encode_elicitation_decline(&request_id),
                        }),
                };
                let Some(line) = line else {
                    let detail = match &kind {
                        GateKind::Question { .. } => format!(
                            "gate {request_id} is a question; answer it with answer_question"
                        ),
                        _ => format!("gate {request_id} offers no choice {:?}", decision.choice),
                    };
                    self.gates.push((request_id.clone(), kind));
                    return Err(DriverError::Invalid { detail });
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
            Command::StopTask { task_id } => {
                if !self.tasks.iter().any(|t| t.running && t.task_id == task_id) {
                    // The CLI answers success to any id and does nothing: say so here.
                    return Err(DriverError::NotFound {
                        detail: format!("no running task {task_id}"),
                    });
                }
                let line = self.stop_task_line(&task_id);
                Ok(Step {
                    write: vec![line],
                    ..Step::default()
                })
            }
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
            // Claude has no compact command of its own: `/compact` is a slash command the session
            // lists, or nothing.
            Command::Compact => {
                if !self.compact_listed {
                    return Err(DriverError::Unsupported {
                        operation: "compact".into(),
                    });
                }
                self.encode_now(
                    input,
                    Command::RunSlash {
                        name: "compact".into(),
                        args: String::new(),
                    },
                )
            }
            Command::RunShell { .. } => Err(DriverError::Unsupported {
                operation: "run_shell".into(),
            }),
            Command::Shutdown => {
                self.phase = Phase::Closed;
                let events = std::mem::take(&mut self.queue)
                    .into_iter()
                    .map(|(input, _)| dropped(input, InputDropReason::Shutdown))
                    .collect();
                Ok(Step {
                    events,
                    close_stdin: true,
                    ..Step::default()
                })
            }
        }
    }

    fn write_prompt(&mut self, input: InputId, text: &str, attachments: &[Attachment]) -> Step {
        // The turn starts when the CLI says so (`system/init`), not when this line is written.
        self.unobserved_write = true;
        let uuid = self.uuid_seed.map(|seed| message_uuid(seed, input));
        let line = user_message(text, attachments, uuid.as_deref());
        if let Some(uuid) = uuid {
            self.written.push_back(Written {
                input,
                uuid,
                cli: CliState::Unknown,
                acknowledged: false,
                cancel_requested: false,
            });
            if self.written.len() > MAX_WRITTEN {
                self.written.pop_front();
            }
        }
        Step {
            write: vec![line],
            ..Step::default()
        }
    }

    /// Take the queued prompts out of the queue: one `InputDropped` each, in queue order.
    fn drop_queued_prompts(&mut self, reason: InputDropReason) -> Vec<AgentEvent> {
        let (prompts, rest): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.queue)
            .into_iter()
            .partition(|(_, c)| is_turn_command(c));
        self.queue = rest;
        prompts
            .into_iter()
            .map(|(input, _)| dropped(input, reason))
            .collect()
    }

    /// The CLI reported that it took the message at `pos` in; `steered`: into a turn that was
    /// already running (or about to run for another input) instead of a turn of its own.
    fn acknowledge(&mut self, pos: usize, steered: bool) -> Vec<AgentEvent> {
        let w = &mut self.written[pos];
        if w.acknowledged {
            return vec![];
        }
        w.acknowledged = true;
        vec![AgentEvent::InputAcknowledged {
            input: w.input,
            steered,
        }]
    }

    /// The CLI dropped the message at `pos` (lifecycle `cancelled`, or `cancelled:true` in
    /// the answer to our `cancel_async_message`).
    fn cancelled(&mut self, pos: usize) -> Step {
        let Some(w) = self.written.remove(pos) else {
            return Step::default();
        };
        if self.started == Some(w.input) {
            // Its turn will not start for it.
            self.started = None;
        }
        if w.cli == CliState::Started || w.acknowledged {
            // It ran and was interrupted; the turn's `result` says so.
            return Step::default();
        }
        let reason = if w.cancel_requested {
            InputDropReason::Interrupted
        } else {
            InputDropReason::Refused
        };
        let mut step = events(vec![dropped(w.input, reason)]);
        // It was holding the later prompts back.
        step.extend(self.drain());
        step
    }

    fn on_lifecycle(&mut self, v: Value) -> Step {
        let uuid = s(&v, "command_uuid");
        let Some(pos) = self.written.iter().position(|w| w.uuid == uuid) else {
            return events(vec![AgentEvent::Unknown { raw: v }]);
        };
        self.lifecycle_seen = true;
        // The CLI is reporting on its own now.
        self.unobserved_write = false;
        match s(&v, "state") {
            "queued" => {
                let w = &mut self.written[pos];
                if w.cli == CliState::Unknown {
                    w.cli = CliState::Queued;
                }
                Step::default()
            }
            "started" => {
                let input = self.written[pos].input;
                self.written[pos].cli = CliState::Started;
                // A turn is open, or another input is about to start the next one: this one
                // joins it. Otherwise its turn is the next `system/init`.
                let steered = self.turn_open || self.started.is_some();
                if !steered {
                    self.started = Some(input);
                }
                events(self.acknowledge(pos, steered))
            }
            "completed" => {
                let step = events(self.acknowledge(pos, false));
                if let Some(w) = self.written.remove(pos) {
                    if self.started == Some(w.input) {
                        self.started = None;
                    }
                }
                step
            }
            "cancelled" => self.cancelled(pos),
            _ => events(vec![AgentEvent::Unknown { raw: v }]),
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

    /// The `stop_task` request for `task_id`.
    fn stop_task_line(&mut self, task_id: &str) -> OutLine {
        self.control_request(
            "stop_task",
            json!({"subtype": "stop_task", "task_id": task_id}),
        )
    }

    /// Interrupt: drop prompts that never started, decline pending gates, ask claude to drop
    /// the messages it holds but has not started, then stop what runs: the turn when one is
    /// running (a turn the CLI started by itself too; background tasks are left alone, as the
    /// CLI's own interrupt does), else the background tasks (`interrupt` does not stop them;
    /// `stop_task` does).
    fn interrupt(&mut self) -> Step {
        let mut step = events(self.drop_queued_prompts(InputDropReason::Interrupted));
        for (id, kind) in std::mem::take(&mut self.gates) {
            let line = match kind {
                GateKind::Elicitation { .. } => encode_elicitation_cancel(&id),
                GateKind::Tool { .. } | GateKind::Question { .. } => {
                    encode_deny(&id, "interrupted by user", false)
                }
            };
            step.write.push(line);
            step.events.push(AgentEvent::GateResolved {
                request_id: id,
                reason: None,
            });
        }
        // A message the CLI has read but not started would survive the interrupt and run right
        // after it. Ask only a CLI that reports lifecycles (it knows the uuids); ask before the
        // interrupt so the message cannot start in between.
        if self.lifecycle_seen {
            let unstarted: Vec<(InputId, String)> = self
                .written
                .iter_mut()
                .filter(|w| w.cli != CliState::Started && !w.acknowledged && !w.cancel_requested)
                .map(|w| {
                    w.cancel_requested = true;
                    (w.input, w.uuid.clone())
                })
                .collect();
            for (input, uuid) in unstarted {
                let line = self.control_request_about(
                    "interrupt",
                    Some(input),
                    json!({"subtype": "cancel_async_message", "message_uuid": uuid}),
                );
                step.write.push(line);
            }
        }
        if self.turn_running() {
            if !self.interrupt_sent {
                self.interrupt_sent = true;
                let line = self.control_request("interrupt", json!({"subtype": "interrupt"}));
                step.write.push(line);
            }
        } else {
            let running: Vec<String> = self
                .tasks
                .iter()
                .filter(|t| t.running && t.background)
                .map(|t| t.task_id.clone())
                .collect();
            for id in running {
                let line = self.stop_task_line(&id);
                step.write.push(line);
            }
        }
        step
    }

    /// Send queued commands that may go now, in order.
    /// Records whether the slash commands the CLI reported now include `compact`.
    fn note_listing(&mut self, events: &[AgentEvent]) {
        for e in events {
            let names: Option<Vec<&str>> = match e {
                AgentEvent::SlashCommandsUpdated { commands } => {
                    Some(commands.iter().map(|c| c.name.as_str()).collect())
                }
                AgentEvent::SessionInit { slash_commands, .. } => {
                    Some(slash_commands.iter().map(|c| c.name.as_str()).collect())
                }
                _ => None,
            };
            if let Some(names) = names {
                self.compact_listed = names.contains(&"compact");
            }
        }
    }

    fn drain(&mut self) -> Step {
        let mut step = Step::default();
        while let Some((_, cmd)) = self.queue.front() {
            let is_turn = is_turn_command(cmd);
            if self.phase != Phase::Ready || (is_turn && self.prompt_blocked()) {
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
                        events: vec![AgentEvent::GateResolved {
                            request_id: id,
                            reason: None,
                        }],
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
            "command_lifecycle" => self.on_lifecycle(v),
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
                self.note_listing(&step.events);
            } else {
                step.events.push(AgentEvent::CommandRejected {
                    command: "initialize".into(),
                    message: error_text(&resp),
                });
            }
            step.extend(self.drain());
            return step;
        }
        let Some((command, about)) = self.outstanding.remove(&id) else {
            return events(vec![AgentEvent::Unknown { raw: v.clone() }]);
        };
        if !ok {
            return events(vec![AgentEvent::CommandRejected {
                command: command.into(),
                message: error_text(&resp),
            }]);
        }
        if let Some(input) = about {
            // The answer to `cancel_async_message`: `cancelled:false` means it already started
            // (its lifecycle says the rest); a `cancelled` frame usually came first.
            let pos = self.written.iter().position(|w| w.input == input);
            return match pos {
                Some(pos) if payload.get("cancelled").and_then(Value::as_bool) == Some(true) => {
                    self.cancelled(pos)
                }
                // Too late to cancel: its turn is the one the interrupt was meant to stop.
                _ if payload.get("cancelled").and_then(Value::as_bool) == Some(false)
                    && self.turn_running()
                    && !self.interrupt_sent =>
                {
                    self.interrupt_sent = true;
                    Step {
                        write: vec![
                            self.control_request("interrupt", json!({"subtype": "interrupt"}))
                        ],
                        ..Step::default()
                    }
                }
                _ => Step::default(),
            };
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
                let (gate, kind) = elicitation_gate(&id, &req);
                self.gates.push((id, kind));
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
            "init" => self.on_init(&v),
            "task_started"
            | "task_progress"
            | "task_updated"
            | "task_notification"
            | "background_tasks_changed" => match task_event(&v) {
                Some(event) => {
                    wasit_core::tasks::apply(&mut self.tasks, &event, 0);
                    vec![event]
                }
                None => vec![AgentEvent::Unknown { raw: v }],
            },
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
                        trigger: Some(s(&m, "trigger").to_string()),
                        pre_tokens: Some(u(&m, "pre_tokens")),
                        post_tokens: Some(u(&m, "post_tokens")),
                        cumulative_dropped_tokens: Some(u(&m, "cumulative_dropped_tokens")),
                        duration_ms: Some(u(&m, "duration_ms")),
                    },
                }]
            }
            _ => vec![AgentEvent::Unknown { raw: v }],
        }
    }

    /// `system/init`: a turn starts. The first one also identifies the session; later ones
    /// report a model change.
    fn on_init(&mut self, v: &Value) -> Vec<AgentEvent> {
        let model = opt_s(v, "model");
        let mut out = Vec::new();
        if self.session_init_sent {
            if model.is_some() && model != self.model {
                self.model = model.clone();
                out.push(AgentEvent::ModelChanged {
                    model: model.unwrap_or_default(),
                });
            }
        } else {
            self.session_init_sent = true;
            self.model = model.clone();
            out.push(session_init(v));
        }
        if v.get("capabilities")
            .and_then(Value::as_array)
            .is_some_and(|c| c.iter().any(|c| c.as_str() == Some(LIFECYCLE_CAPABILITY)))
        {
            self.lifecycle_capable = true;
        }
        self.note_listing(&out);
        // Whatever the CLI had not said about our write, it has begun working since.
        self.unobserved_write = false;
        if !self.turn_open {
            self.turn_open = true;
            // The input whose `started` frame came just before: the turn is for it. When the
            // CLI reports lifecycles for our messages (it said so, or already did) and no such
            // frame came, the turn is the agent's own; otherwise the driver cannot tell, and
            // says so rather than guess.
            let reports =
                (self.lifecycle_seen || self.lifecycle_capable) && self.uuid_seed.is_some();
            let (input, trigger) = match self.started.take() {
                Some(input) => (Some(input), TurnTrigger::Input),
                None if reports => (None, TurnTrigger::Agent),
                None => (None, TurnTrigger::Unreported),
            };
            out.push(AgentEvent::TurnStarted {
                native_turn_id: None,
                input,
                trigger,
            });
        }
        out
    }

    fn on_assistant(&mut self, v: &Value) -> Vec<AgentEvent> {
        let Some(msg) = v.get("message") else {
            return vec![AgentEvent::Unknown { raw: v.clone() }];
        };
        let message_id = opt_s(msg, "id");
        let scope = scope_of(v);
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
                        out.push(
                            AgentEvent::AssistantDelta {
                                text: text.to_string(),
                                message_id: message_id.clone(),
                            }
                            .scoped(scope.clone()),
                        );
                    }
                }
                "thinking" => {
                    let text = s(block, "thinking");
                    if !text.is_empty() {
                        out.push(
                            AgentEvent::ThinkingDelta {
                                text: text.to_string(),
                                message_id: message_id.clone(),
                            }
                            .scoped(scope.clone()),
                        );
                    }
                }
                "tool_use" => {
                    let call_id = s(block, "id").to_string();
                    let name = s(block, "name").to_string();
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    let diffs = edit_diffs(&name, &input);
                    if !diffs.is_empty() {
                        self.tool_diffs.insert(call_id.clone(), diffs);
                    }
                    self.tool_names.insert(call_id.clone(), name.clone());
                    let is_subagent = name == "Task" || name == "Agent";
                    let subagent_model = if is_subagent {
                        opt_s(&input, "model")
                    } else {
                        None
                    };
                    out.push(
                        AgentEvent::ToolCallStarted {
                            name,
                            call_id,
                            input,
                            is_subagent,
                            subagent_model,
                        }
                        .scoped(scope.clone()),
                    );
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
                // The context window fill is the session's own conversation's; a subagent's
                // messages have a context of their own.
                let context_pct = self
                    .context_window
                    .filter(|w| *w > 0 && scope.is_none())
                    .map(|w| {
                        let used = usage.input_tokens
                            + usage.cache_read_tokens
                            + usage.cache_creation_tokens;
                        (used as f64 * 100.0 / w as f64) as f32
                    });
                out.push(AgentEvent::UsageUpdate { usage, context_pct }.scoped(scope.clone()));
            }
        }
        out
    }

    fn on_user(&mut self, v: &Value) -> Vec<AgentEvent> {
        let mut out = Vec::new();
        let scope = scope_of(v);
        let content = v
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array);
        let mut user_parts = Vec::new();
        for block in content.into_iter().flatten() {
            if s(block, "type") == "tool_result" {
                let call_id = s(block, "tool_use_id").to_string();
                let name = self.tool_names.remove(&call_id).unwrap_or_default();
                let mut parts: Vec<ContentPart> = block
                    .get("content")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(claude_part).collect())
                    .unwrap_or_default();
                let ok = !block
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                // A failed edit changed nothing: its diff is not shown.
                let diffs = self.tool_diffs.remove(&call_id).unwrap_or_default();
                if ok {
                    parts.extend(diffs);
                }
                out.push(
                    AgentEvent::ToolCallFinished {
                        call_id,
                        name,
                        ok,
                        result: block.get("content").cloned(),
                        parts,
                    }
                    .scoped(scope.clone()),
                );
            } else if let Some(p) = claude_part(block) {
                user_parts.push(p);
            }
        }
        if !user_parts.is_empty() {
            out.push(AgentEvent::UserContent { parts: user_parts }.scoped(scope));
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
        // `modelUsage` and `total_cost_usd` are the process's running totals: this turn is what
        // they grew by since the previous result. A result that carries neither has no usage to
        // report, and does not move the baseline.
        let has_usage =
            v.get("modelUsage").is_some_and(Value::is_object) || v.get("total_cost_usd").is_some();
        let turn_usage = if has_usage {
            let turn = usage.since(&self.counted_usage);
            self.counted_usage = usage.clone();
            turn
        } else {
            // The session's usage stays what the process last reported.
            usage = self.counted_usage.clone();
            None
        };
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
        // The messages this turn ran, as the CLI lists them: proof they were taken in, also
        // when it reported no lifecycle frame for them.
        let mut acks = Vec::new();
        for uuid in v
            .get("user_message_uuids")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            if let Some(pos) = self.written.iter().position(|w| w.uuid == uuid) {
                acks.extend(self.acknowledge(pos, false));
            }
        }
        let mut step = events(acks);
        if !self.turn_open {
            // The CLI ended a turn whose start it did not report: still one turn, never an end
            // without a start.
            step.events.push(AgentEvent::TurnStarted {
                native_turn_id: None,
                input: None,
                trigger: TurnTrigger::Unreported,
            });
        }
        self.turn_open = false;
        self.started = None;
        self.unobserved_write = false;
        self.interrupt_sent = false;
        // What does not run in the background cannot outlive its turn.
        wasit_core::tasks::end_foreground(&mut self.tasks, 0);
        step.events.push(AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted: interrupted,
            num_turns: u(v, "num_turns"),
            usage,
            turn_usage,
            per_model,
            reply,
            origin: turn_origin(v),
        });
        step.extend(self.drain());
        step
    }
}

impl Protocol for ClaudeProtocol {
    fn start(&mut self, spec: &SpawnSpec) -> Step {
        self.phase = Phase::Initializing;
        self.model = spec.model.clone();
        self.uuid_seed = spec.session_id.as_deref().map(fnv1a);
        self.init_request_id = self.request_id();
        let init = OutLine::json(&json!({
            "type": "control_request",
            "request_id": self.init_request_id,
            "request": {"subtype": "initialize", "hooks": null},
        }));
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

/// Commands that start a turn of their own, so they wait for the running turn (and the handshake).
fn is_turn_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Prompt { .. } | Command::RunSlash { .. } | Command::Compact
    )
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

/// 64-bit FNV-1a: a stable hash (the same in every build), so replays derive the same uuids.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The `uuid` of the user message carrying `input`: 60 bits of the session's seed and 62 bits
/// of the input id in the shape of an RFC 9562 version-4 uuid. Unique per session and input,
/// deterministic (replay writes the same line), never reused by a later process generation
/// (input ids are journal seqs).
fn message_uuid(seed: u64, input: InputId) -> String {
    let lo = input.0;
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        seed >> 32,
        (seed >> 16) & 0xffff,
        (seed >> 4) & 0xfff,
        0x8000 | ((lo >> 48) & 0x3fff),
        lo & 0xffff_ffff_ffff
    )
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
    let suggestions: Vec<Value> = req
        .get("permission_suggestions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    match tool_name.as_str() {
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
        "ExitPlanMode" => {
            let (choices, answers) = tool_choices(&suggestions, true);
            (
                GateRequest::Plan {
                    request_id: id.to_string(),
                    summary: opt_s(&input, "plan")
                        .or_else(|| opt_s(&input, "content"))
                        .unwrap_or_default(),
                    choices,
                },
                GateKind::Tool { input, answers },
            )
        }
        _ => {
            let command = opt_s(&input, "command")
                .or_else(|| opt_s(&input, "file_path"))
                .or_else(|| opt_s(&input, "description"))
                .or_else(|| opt_s(req, "description"))
                .unwrap_or_default();
            let (choices, answers) = tool_choices(&suggestions, false);
            let gate = GateRequest::Permission {
                request_id: id.to_string(),
                tool_name,
                command,
                cwd: s(&input, "cwd").to_string(),
                explanation: opt_s(&input, "description")
                    .or_else(|| opt_s(req, "description"))
                    .unwrap_or_default(),
                choices,
                raw_input: input.clone(),
            };
            (gate, GateKind::Tool { input, answers })
        }
    }
}

/// The answers a `can_use_tool` request implies. claude's control protocol carries no
/// wording, so the labels are neutral: allow once; allow and remember (only when claude sent
/// `permission_suggestions`: they are echoed back as `updatedPermissions`, all of them, the
/// way the CLI's own "don't ask again" applies them; the description lists them); deny with an
/// optional message to claude (the agent continues); deny with an optional message and stop
/// the turn (`interrupt: true`, what the CLI's own "No" does).
fn tool_choices(suggestions: &[Value], plan: bool) -> (Vec<GateChoice>, Vec<(String, ToolAnswer)>) {
    let mut choices = vec![GateChoice::neutral(
        "allow",
        if plan { "Approve plan" } else { "Allow" },
        GateChoiceKind::AllowOnce,
    )];
    let mut answers = vec![(
        "allow".to_string(),
        ToolAnswer::Allow {
            updated_permissions: None,
        },
    )];
    if !suggestions.is_empty() {
        let effects: Vec<String> = suggestions.iter().map(describe_suggestion).collect();
        choices.push(
            GateChoice::neutral(
                "allow_always",
                if plan {
                    "Approve and remember"
                } else {
                    "Allow and remember"
                },
                GateChoiceKind::AllowAlways,
            )
            .with_description(effects.join("; "))
            .with_effects(effects),
        );
        answers.push((
            "allow_always".to_string(),
            ToolAnswer::Allow {
                updated_permissions: Some(Value::Array(suggestions.to_vec())),
            },
        ));
    }
    choices.push(
        GateChoice::neutral(
            "deny",
            if plan { "Keep planning" } else { "Deny" },
            GateChoiceKind::RejectOnce,
        )
        .with_text(ChoiceText::Optional),
    );
    answers.push(("deny".to_string(), ToolAnswer::Deny { interrupt: false }));
    choices.push(
        GateChoice::neutral(
            "deny_stop",
            "Deny and stop the turn",
            GateChoiceKind::Cancel,
        )
        .with_text(ChoiceText::Optional),
    );
    answers.push((
        "deny_stop".to_string(),
        ToolAnswer::Deny { interrupt: true },
    ));
    (choices, answers)
}

/// One claude `PermissionUpdate` in words, using only what it carries (`addRules`,
/// `replaceRules`, `removeRules`, `setMode`, `addDirectories`, `removeDirectories`).
fn describe_suggestion(v: &Value) -> String {
    let dest = match s(v, "destination") {
        "" => String::new(),
        d => format!(" ({d})"),
    };
    let rules = || {
        v.get("rules")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .map(|r| match opt_s(r, "ruleContent") {
                        Some(c) => format!("{}({c})", s(r, "toolName")),
                        None => s(r, "toolName").to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    };
    let dirs = || {
        v.get("directories")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default()
    };
    match s(v, "type") {
        "addRules" | "replaceRules" => format!("{} rule {}{dest}", s(v, "behavior"), rules()),
        "removeRules" => format!("remove rule {}{dest}", rules()),
        "setMode" => format!("switch to mode {}{dest}", s(v, "mode")),
        "addDirectories" => format!("add directory {}{dest}", dirs()),
        "removeDirectories" => format!("remove directory {}{dest}", dirs()),
        other => format!("{other}{dest}"),
    }
}

fn question_prompt(q: &Value) -> QuestionPrompt {
    QuestionPrompt {
        // claude keys answers by the question text.
        id: s(q, "question").to_string(),
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
        // AskUserQuestion always accepts a free-text answer (the CLI's "Other").
        allow_other: true,
        secret: false,
    }
}

fn elicitation_gate(id: &str, req: &Value) -> (GateRequest, GateKind) {
    let message = s(req, "message").to_string();
    let pairs = wasit_core::wire::elicitation_choices(req.get("requested_schema"));
    let gate = GateRequest::Permission {
        request_id: id.to_string(),
        tool_name: opt_s(req, "mcp_server_name")
            .or_else(|| opt_s(req, "server_name"))
            .unwrap_or_default(),
        command: message.clone(),
        cwd: String::new(),
        explanation: message,
        choices: pairs.iter().map(|(c, _)| c.clone()).collect(),
        raw_input: Value::Null,
    };
    let answers = pairs.into_iter().map(|(c, v)| (c.id, v)).collect();
    (gate, GateKind::Elicitation { answers })
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

/// The subagent a line belongs to: lines of a subagent's own conversation carry the spawning
/// call's id as `parent_tool_use_id` (and the subagent's own as `agent_id`).
fn scope_of(v: &Value) -> Option<Scope> {
    Some(Scope {
        call_id: opt_s(v, "parent_tool_use_id").filter(|c| !c.is_empty())?,
        agent_id: opt_s(v, "agent_id").filter(|c| !c.is_empty()),
    })
}

/// `result.origin`: why the CLI ran a turn it started by itself, as it says.
fn turn_origin(v: &Value) -> Option<TurnOrigin> {
    let o = v.get("origin").filter(|o| o.is_object())?;
    Some(TurnOrigin {
        kind: opt_s(o, "kind")?,
        producer: opt_s(o, "producer"),
        run_id: opt_s(o, "runId"),
    })
}

fn task_usage(v: &Value) -> Option<TaskUsage> {
    let u = v.get("usage").filter(|u| u.is_object())?;
    Some(TaskUsage {
        total_tokens: wasit_core::wire::u(u, "total_tokens"),
        tool_uses: wasit_core::wire::u(u, "tool_uses"),
        duration_ms: wasit_core::wire::u(u, "duration_ms"),
    })
}

/// The event of a `system` task frame; `None` when it names no task.
fn task_event(v: &Value) -> Option<AgentEvent> {
    let subtype = s(v, "subtype");
    if subtype == "background_tasks_changed" {
        let tasks = v
            .get("tasks")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|t| {
                        Some(BackgroundTask {
                            task_id: opt_s(t, "task_id")?,
                            kind: s(t, "task_type").to_string(),
                            description: s(t, "description").to_string(),
                            subagent_type: opt_s(t, "subagent_type"),
                            run_id: opt_s(t, "run_id"),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        return Some(AgentEvent::BackgroundTasksChanged { tasks });
    }
    let task_id = opt_s(v, "task_id")?;
    let call_id = opt_s(v, "tool_use_id").filter(|c| !c.is_empty());
    Some(match subtype {
        "task_started" => AgentEvent::TaskStarted {
            task_id,
            kind: s(v, "task_type").to_string(),
            call_id,
            description: s(v, "description").to_string(),
            subagent_type: opt_s(v, "subagent_type"),
            background: v
                .get("is_backgrounded")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            run_id: opt_s(v, "run_id"),
        },
        "task_progress" => AgentEvent::TaskProgress {
            task_id,
            call_id,
            description: s(v, "description").to_string(),
            last_tool: opt_s(v, "last_tool_name"),
            usage: task_usage(v),
        },
        "task_updated" => AgentEvent::TaskUpdated {
            task_id,
            status: v.get("patch").and_then(|p| opt_s(p, "status")),
        },
        "task_notification" => {
            let status = s(v, "status").to_string();
            AgentEvent::TaskEnded {
                task_id,
                call_id,
                ok: status == "completed",
                status,
                summary: opt_s(v, "summary"),
                usage: task_usage(v),
            }
        }
        _ => return None,
    })
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

/// `can_use_tool` deny; `interrupt` also stops the turn.
pub fn encode_deny(request_id: &str, message: &str, interrupt: bool) -> OutLine {
    let mut r = json!({"behavior": "deny", "message": message});
    if interrupt {
        r["interrupt"] = Value::Bool(true);
    }
    control_success(request_id, r)
}

/// Elicitation accept.
pub fn encode_elicitation_accept(request_id: &str, content: Option<Value>) -> OutLine {
    control_success(
        request_id,
        json!({"action": "accept", "content": content.unwrap_or_else(|| json!({}))}),
    )
}

/// Elicitation cancel.
pub fn encode_elicitation_cancel(request_id: &str) -> OutLine {
    control_success(request_id, json!({"action": "cancel"}))
}

/// Elicitation decline.
pub fn encode_elicitation_decline(request_id: &str) -> OutLine {
    control_success(request_id, json!({"action": "decline"}))
}

/// `AskUserQuestion` answer: allow with the original questions plus answers.
pub fn encode_question_answer(request_id: &str, input: &Value, answer: &QuestionAnswer) -> OutLine {
    // claude takes one string per question; several picks are comma-joined, as its own UI does.
    let answers: serde_json::Map<String, Value> = answer
        .answers
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| (k.clone(), Value::String(v.join(", "))))
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
    encode_user_message_with(text, &[])
}

/// A user message with attachments: images as `image` content blocks (base64 source); every
/// other file is named by path in the text (claude reads it with its own tools).
pub fn encode_user_message_with(text: &str, attachments: &[Attachment]) -> OutLine {
    user_message(text, attachments, None)
}

/// A user message, with the `uuid` claude reports its lifecycle by when there is one.
fn user_message(text: &str, attachments: &[Attachment], uuid: Option<&str>) -> OutLine {
    let (images, files): (Vec<&Attachment>, Vec<&Attachment>) = attachments
        .iter()
        .partition(|a| a.data.is_some() && COMMON_IMAGE_MIMES.contains(&a.mime.as_str()));
    let text = format!("{text}{}", path_reference_text(&files));
    let mut content = vec![json!({"type": "text", "text": text})];
    for a in images {
        content.push(json!({"type": "image", "source": {
            "type": "base64", "media_type": a.mime, "data": a.data}}));
    }
    let mut msg = json!({
        "type": "user",
        "message": {"role": "user", "content": content},
    });
    if let Some(uuid) = uuid {
        msg["uuid"] = json!(uuid);
    }
    OutLine::json(&msg)
}

/// The image / document a claude content block carries (`{"type":"image","source":{…}}`).
pub(crate) fn claude_part(block: &Value) -> Option<ContentPart> {
    let kind = s(block, "type");
    if kind != "image" && kind != "document" {
        return None;
    }
    let src = block.get("source")?;
    let mime = opt_s(src, "media_type");
    let source = match s(src, "type") {
        "base64" => MediaSource::Inline {
            data: opt_s(src, "data")?,
        },
        "url" => MediaSource::Url {
            url: opt_s(src, "url")?,
        },
        "text" => MediaSource::Inline {
            data: wasit_core::content::b64_encode(s(src, "data").as_bytes()),
        },
        _ => return None,
    };
    Some(if kind == "image" {
        ContentPart::Image {
            mime: mime.unwrap_or_else(|| "image/png".into()),
            source,
            width: None,
            height: None,
            alt: None,
        }
    } else {
        ContentPart::File {
            source,
            name: opt_s(block, "title"),
            mime,
            uri: None,
        }
    })
}

#[cfg(test)]
mod drain_tests {
    use super::*;

    /// A queued command that cannot be encoded when its turn comes is dropped with the reason.
    /// (No command claude queues fails today; this pins the mapping.)
    #[test]
    fn drain_failure_drops_the_input() {
        let mut p = ClaudeProtocol::new();
        p.start(&SpawnSpec::default());
        p.queue.push_back((
            InputId(4),
            Command::AnswerQuestion {
                request_id: "gone".into(),
                answer: QuestionAnswer::default(),
            },
        ));
        p.queue.push_back((InputId(5), Command::prompt("next")));
        let step = p.on_line(
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#,
        );
        assert!(matches!(
            &step.events[0],
            AgentEvent::InputDropped {
                input: InputId(4),
                reason: InputDropReason::Unencodable,
                error: Some(e),
            } if e.contains("gone")
        ));
        assert_eq!(step.events[1], AgentEvent::InputSent { input: InputId(5) });
    }
}
