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
    /// e.g. the active turn id) and buffered the command. Its lines will be
    /// part of the [`Step`] returned by a later `on_line`, in the order the
    /// commands were encoded.
    Queued,
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
    /// Stop the running turn. Pending gates are declined first. A no-op when
    /// no turn is running.
    Interrupt,
    /// Switch model for subsequent turns.
    SetModel { model: String },
    /// Switch mode.
    SetMode { mode: String },
    /// Run a slash command. `name` has no leading `/`.
    RunSlash { name: String, args: String },
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
    /// Called exactly once, before any `on_line` or `encode`.
    fn start(&mut self, spec: &SpawnSpec) -> Step;

    /// Feed one line the CLI wrote to stdout (without the newline).
    fn on_line(&mut self, line: &str) -> Step;

    /// Encode a command. `Err(Unsupported)` only when the descriptor's
    /// capabilities say the operation is unsupported.
    fn encode(&mut self, cmd: Command) -> Result<Encoded, DriverError>;

    /// `true` once the handshake has completed and the CLI accepts commands
    /// (commands are no longer queued *for the handshake*; they may still be
    /// queued behind a running turn). A pure query of the protocol state; the
    /// runtime derives its `Starting → Idle` transition from it.
    fn is_ready(&self) -> bool;
}
