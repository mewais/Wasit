//! The [`Protocol`] trait: a pure, transport-free, deterministic driver.
//!
//! A protocol never touches a process, a socket or a clock. The runtime feeds
//! it the lines the CLI wrote (`on_line`) and the commands the user issued
//! (`encode`); the protocol answers with [`AgentEvent`]s and the lines to write
//! to the CLI's stdin. Its state is a function of exactly
//! `(start spec, lines read, commands encoded)` in order, so replaying the
//! same inputs reproduces the same outputs byte for byte. Request ids are
//! counters inside the protocol.

use serde::{Deserialize, Serialize};

use crate::content::Attachment;
use crate::event::{AgentEvent, GateDecision, QuestionAnswer};
use crate::spec::SpawnSpec;

/// One line to write to the CLI's stdin, without the trailing newline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct OutLine(pub String);

impl OutLine {
    /// Serialize a JSON value as one line.
    pub fn json(value: &serde_json::Value) -> Self {
        // `Value`'s Display is infallible and compact (no newlines).
        OutLine(value.to_string())
    }

    /// The line text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for OutLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The result of feeding the protocol one input.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Step {
    /// Events to publish, in order.
    pub events: Vec<AgentEvent>,
    /// Lines to write to the CLI's stdin, in order, after publishing `events`.
    pub write: Vec<OutLine>,
    /// After writing `write`, close the CLI's stdin (graceful shutdown).
    pub close_stdin: bool,
}

impl Step {
    /// A step with nothing to do.
    pub fn empty() -> Self {
        Step::default()
    }

    /// Append another step's outputs.
    pub fn extend(&mut self, other: Step) {
        self.events.extend(other.events);
        self.write.extend(other.write);
        self.close_stdin |= other.close_stdin;
    }

    /// `true` when there is nothing to publish, write or close.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.write.is_empty() && !self.close_stdin
    }
}

/// What `encode` did with a command.
#[derive(Debug, Clone, PartialEq)]
pub enum Encoded {
    /// The command was encoded now; perform the step.
    Sent(Step),
    /// The protocol is still handshaking (or waiting on a value it needs,
    /// e.g. the active turn id, or for the running turn to end) and buffered
    /// the command. Its lines will be part of the [`Step`] returned by a later
    /// `on_line`, in the order the commands were encoded; that step carries
    /// [`AgentEvent::InputSent`] for it. A buffered command the protocol
    /// gives up on is reported with [`AgentEvent::InputDropped`].
    Queued,
}

/// The identity of one input (a command the runtime journaled): the journal seq of the note
/// that carried it (the `start` note for a session's first prompt). Assigned by the journal,
/// so a replay reproduces it; drivers echo it back in [`AgentEvent::InputSent`],
/// [`AgentEvent::InputAcknowledged`], [`AgentEvent::InputDropped`] and
/// [`AgentEvent::TurnStarted`]. `0` means "unknown" (an event from a peer that predates ids).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct InputId(pub u64);

impl std::fmt::Display for InputId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A user/runtime command for the CLI.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    /// Send a user prompt (starts a turn; see
    /// [`DriverCapabilities::prompt_while_busy`](crate::DriverCapabilities::prompt_while_busy)
    /// for what happens during a turn).
    Prompt {
        text: String,
        /// Files sent with the prompt (uploaded first); see
        /// [`DriverDescriptor::prompt_input`](crate::DriverDescriptor::prompt_input).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<Attachment>,
    },
    /// Answer a permission or plan gate.
    AnswerGate {
        request_id: String,
        decision: GateDecision,
    },
    /// Answer a question gate.
    AnswerQuestion {
        request_id: String,
        answer: QuestionAnswer,
    },
    /// Stop what the agent is doing: the running turn (a turn it started by itself too), after
    /// declining pending gates and dropping prompts that never started; when no turn is running
    /// but the agent works in the background, stop that background work. A no-op when nothing
    /// runs. With a turn running, background tasks are left alone (stop them one by one with
    /// [`Command::StopTask`]).
    Interrupt,
    /// Switch model for subsequent turns.
    SetModel { model: String },
    /// Switch mode.
    SetMode { mode: String },
    /// Run a slash command. `name` has no leading `/`.
    RunSlash { name: String, args: String },
    /// Stop one task the agent runs (a background subagent or shell command): `task_id` is a
    /// [`TaskInfo::task_id`](crate::TaskInfo::task_id) of the session. Needs
    /// [`DriverCapabilities::stop_task`](crate::DriverCapabilities::stop_task). The agent reports
    /// the end of the task as [`AgentEvent::TaskEnded`](crate::AgentEvent::TaskEnded).
    StopTask { task_id: String },
    /// Send a message to a subagent that is running now, in place: the subagent (a task the
    /// session owns, [`TaskInfo::task_id`](crate::TaskInfo::task_id), the subagent's own
    /// session) takes it as its next input and the session keeps owning it. Needs
    /// [`DriverCapabilities::subagents`](crate::DriverCapabilities::subagents)`.steer`. Stopping
    /// it is [`Command::StopTask`]. The text is the user's message to it, as the user wrote it.
    SteerSubagent { task_id: String, text: String },
    /// Compact the conversation's context. Needs
    /// [`DriverCapabilities::compact`](crate::DriverCapabilities::compact).
    Compact,
    /// Run a shell command in the session's working directory, as the user's own `!` command.
    /// Needs [`DriverCapabilities::run_shell`](crate::DriverCapabilities::run_shell). The field
    /// is `line` because `command` is the serde tag. `timeout_ms` `None` means the CLI's default.
    /// The result is reported as [`AgentEvent::LocalShell`](crate::AgentEvent::LocalShell).
    RunShell {
        line: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    /// End the session gracefully (close stdin).
    Shutdown,
}

impl Command {
    /// A text-only prompt.
    pub fn prompt(text: impl Into<String>) -> Self {
        Command::Prompt {
            text: text.into(),
            attachments: Vec::new(),
        }
    }

    /// Stable snake_case name, used in errors and `CommandRejected`.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Prompt { .. } => "prompt",
            Command::AnswerGate { .. } => "answer_gate",
            Command::AnswerQuestion { .. } => "answer_question",
            Command::Interrupt => "interrupt",
            Command::SetModel { .. } => "set_model",
            Command::SetMode { .. } => "set_mode",
            Command::RunSlash { .. } => "run_slash",
            Command::StopTask { .. } => "stop_task",
            Command::SteerSubagent { .. } => "steer_subagent",
            Command::Compact => "compact",
            Command::RunShell { .. } => "run_shell",
            Command::Shutdown => "shutdown",
        }
    }
}

/// Errors a protocol returns from `encode`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DriverError {
    /// The driver cannot do this (its capabilities say so).
    #[error("operation not supported by this driver: {operation}")]
    Unsupported { operation: String },
    /// The referenced gate / request is not pending.
    #[error("not found: {detail}")]
    NotFound { detail: String },
    /// The command is malformed for this driver (e.g. wrong gate kind).
    #[error("invalid command: {detail}")]
    Invalid { detail: String },
    /// The session was shut down; no further commands are accepted.
    #[error("session has shut down")]
    Shutdown,
}

/// A pure, deterministic driver state machine. See the module docs.
pub trait Protocol: Send {
    /// Lines to write right after the process starts (the handshake opener).
    /// Called exactly once, before any `on_line` or `encode`. A session's first
    /// prompt is not part of the spec the protocol sees: it arrives as the first
    /// `encode` (see [`SpawnSpec::split_first_prompt`]).
    fn start(&mut self, spec: &SpawnSpec) -> Step;

    /// Feed one line the CLI wrote to stdout (without the newline).
    fn on_line(&mut self, line: &str) -> Step;

    /// Encode a command. `input` is its identity; every event the protocol reports about this
    /// command later carries it. `Err(Unsupported)` only when the descriptor's capabilities say
    /// the operation is unsupported.
    fn encode(&mut self, input: InputId, cmd: Command) -> Result<Encoded, DriverError>;

    /// `true` once the handshake has completed and the CLI accepts commands
    /// (commands are no longer queued *for the handshake*; they may still be
    /// queued behind a running turn). A pure query of the protocol state; the
    /// runtime derives its `Starting → Idle` transition from it.
    fn is_ready(&self) -> bool;
}

#[cfg(test)]
mod shape_tests {
    use super::*;

    #[test]
    fn compact_and_run_shell_have_their_wire_shapes() {
        assert_eq!(
            serde_json::to_value(Command::Compact).unwrap(),
            serde_json::json!({"command": "compact"})
        );
        let run = Command::RunShell {
            line: "ls -la".into(),
            timeout_ms: None,
        };
        assert_eq!(
            serde_json::to_value(&run).unwrap(),
            serde_json::json!({"command": "run_shell", "line": "ls -la"})
        );
        let timed = Command::RunShell {
            line: "make".into(),
            timeout_ms: Some(5000),
        };
        assert_eq!(
            serde_json::to_value(&timed).unwrap(),
            serde_json::json!({"command": "run_shell", "line": "make", "timeout_ms": 5000})
        );
        for cmd in [Command::Compact, run, timed] {
            let back: Command =
                serde_json::from_value(serde_json::to_value(&cmd).unwrap()).unwrap();
            assert_eq!(back, cmd);
        }
        assert_eq!(Command::Compact.name(), "compact");
    }

    /// Steering is a command to a running subagent: its wire shape is pinned, and it is the
    /// only subagent command of its own (stopping one is `StopTask`).
    #[test]
    fn steer_subagent_has_its_wire_shape_and_stop_is_stop_task() {
        let steer = Command::SteerSubagent {
            task_id: "ses_child".into(),
            text: "use the other file".into(),
        };
        let json = serde_json::to_value(&steer).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"command": "steer_subagent", "task_id": "ses_child",
                               "text": "use the other file"})
        );
        assert_eq!(serde_json::from_value::<Command>(json).unwrap(), steer);
        assert_eq!(steer.name(), "steer_subagent");
        assert_eq!(
            serde_json::to_value(Command::StopTask {
                task_id: "ses_child".into()
            })
            .unwrap(),
            serde_json::json!({"command": "stop_task", "task_id": "ses_child"})
        );
        assert_eq!(
            Command::RunShell {
                line: String::new(),
                timeout_ms: None
            }
            .name(),
            "run_shell"
        );
    }
}
