//! Every type that crosses the runtime's control surface: method parameters and results,
//! subscription payloads, the session snapshot and session events.
//!
//! These are the *only* definitions of these shapes: the daemon side ([`crate::RuntimeService`])
//! and every client ([`crate::RuntimeClient`]) use them, so nothing else hand-writes this JSON.
//!
//! # Methods (`Request.method` → params → result)
//!
//! | method | params | result |
//! |---|---|---|
//! | [`methods::HOST_INFO`] | — | [`HostInfo`] |
//! | [`methods::DRIVERS_INVENTORY`] | [`InventoryRequest`] | [`InventoryReply`] |
//! | [`methods::DRIVERS_REFRESH`] | [`RefreshRequest`] | [`Ack`] |
//! | [`methods::HOST_ENVIRONMENT`] | — | `Option<`[`HostEnvironment`]`>` |
//! | [`methods::HOST_ENVIRONMENT_REFRESH`] | — | [`HostEnvironment`] (re-captured; agents re-detected) |
//! | [`methods::SESSIONS_LIST`] | — | [`SessionList`] |
//! | [`methods::SESSIONS_CREATE`] | [`CreateSession`] | [`CreateSessionReply`] |
//! | [`methods::SESSIONS_CONTINUE`] | [`SessionRef`] | [`Ack`] |
//! | [`methods::SESSIONS_STOP`] | [`SessionRef`] | [`Ack`] |
//! | [`methods::SESSIONS_DELETE`] | [`SessionRef`] | [`Ack`] |
//! | [`methods::SESSIONS_RENAME`] | [`RenameSession`] | [`Ack`] |
//! | [`methods::SESSION_SNAPSHOT`] | [`SessionRef`] | [`SessionSnapshot`] |
//! | [`methods::SESSION_HISTORY`] | [`HistoryRequest`] | [`HistoryPage`] |
//! | [`methods::SESSION_COMMAND`] | [`CommandRequest`] | [`CommandReply`] |
//! | [`methods::SESSION_ATTACHMENT_PUT`] | [`crate::media::AttachmentPut`] | [`crate::media::AttachmentPutReply`] |
//! | [`methods::SESSION_FILE_READ`] | [`crate::media::FileReadRequest`] | [`crate::media::FileChunk`] |
//! | [`methods::SESSION_FILES_STAT`] | [`crate::media::FilesStatRequest`] | [`crate::media::FilesStatReply`] |
//!
//! Errors are `RpcError`s whose `code` is one of [`codes`]; command failures carry a
//! [`CommandError`] and other failures a [`RuntimeError`] as `data`.
//!
//! # Topics (`Subscribe.topic` → event payload, cursor)
//!
//! | topic | payload | cursor |
//! |---|---|---|
//! | [`topics::HOST_SESSIONS`] | [`HostEvent`] | `"<epoch>.<n>"`; an unknown or expired cursor restarts with [`HostEvent::Reset`] |
//! | `session.events:<id>` ([`topics::session_events`]) | [`SessionEvent`] | [`SessionEvent::cursor`] as a decimal `u64`; stable across daemon restarts |
//! | [`topics::DRIVERS_INVENTORY`] | [`InventoryReply`] | `"<epoch>.<n>"`; every event is a full inventory |

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use wasit_core::{
    AgentConfig, AgentEvent, Command, ConfigItem, DriverCapabilities, DriverId, EndReason,
    GateRequest, ModelInfo, ModelUsage, PlanEntry, PromptInput, ResumableSession, SessionMode,
    SlashCommandInfo, Unsupported, Usage,
};

/// Method names.
pub mod methods {
    pub const HOST_INFO: &str = "host.info";
    pub const HOST_ENVIRONMENT: &str = "host.environment";
    pub const HOST_ENVIRONMENT_REFRESH: &str = "host.environment.refresh";
    pub const DRIVERS_INVENTORY: &str = "drivers.inventory";
    pub const DRIVERS_REFRESH: &str = "drivers.refresh";
    pub const SESSIONS_LIST: &str = "sessions.list";
    pub const SESSIONS_CREATE: &str = "sessions.create";
    pub const SESSIONS_CONTINUE: &str = "sessions.continue";
    pub const SESSIONS_STOP: &str = "sessions.stop";
    pub const SESSIONS_DELETE: &str = "sessions.delete";
    pub const SESSIONS_RENAME: &str = "sessions.rename";
    pub const SESSION_SNAPSHOT: &str = "session.snapshot";
    pub const SESSION_HISTORY: &str = "session.history";
    pub const SESSION_COMMAND: &str = "session.command";
    pub const SESSION_ATTACHMENT_PUT: &str = "session.attachment.put";
    pub const SESSION_FILE_READ: &str = "session.file.read";
    pub const SESSION_FILES_STAT: &str = "session.files.stat";
}

/// Topic names.
pub mod topics {
    pub const HOST_SESSIONS: &str = "host.sessions";
    pub const DRIVERS_INVENTORY: &str = "drivers.inventory";
    /// Prefix of the per-session event topic.
    pub const SESSION_EVENTS_PREFIX: &str = "session.events:";

    /// The event topic of one session.
    pub fn session_events(id: &str) -> String {
        format!("{SESSION_EVENTS_PREFIX}{id}")
    }
}

/// The routes [`crate::RuntimeService`] serves, for registering it in a
/// [`wasit_control::ServiceMux`] next to other services.
pub const ROUTES: &[&str] = &[
    methods::HOST_INFO,
    methods::HOST_ENVIRONMENT,
    methods::HOST_ENVIRONMENT_REFRESH,
    topics::HOST_SESSIONS,
    "drivers.",
    "sessions.",
    "session.",
];

/// Error codes (in addition to `wasit_control::codes`).
pub mod codes {
    pub const SESSION_NOT_FOUND: &str = "session_not_found";
    pub const UNKNOWN_DRIVER: &str = "unknown_driver";
    pub const INVALID_STATE: &str = "invalid_state";
    pub const UNSUPPORTED: &str = "unsupported";
    pub const NOT_RUNNING: &str = "not_running";
    pub const REJECTED: &str = "rejected";
    pub const SPAWN_FAILED: &str = "spawn_failed";
    pub const IO: &str = "io";
    pub const BUSY: &str = "busy";
    pub const GATE_WITHDRAWN: &str = "gate_withdrawn";
}

/// A session id: 12 characters of lowercase RFC 4648 base32 (`a-z2-7`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct SessionId(pub String);

impl SessionId {
    pub const LEN: usize = 12;
    const ALPHABET: &'static [u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

    /// A fresh random id.
    pub fn random() -> Self {
        let s = (0..Self::LEN)
            .map(|_| Self::ALPHABET[fastrand::usize(..32)] as char)
            .collect();
        SessionId(s)
    }

    /// Parse and validate (ids become directory names, so nothing else is accepted).
    pub fn parse(s: &str) -> Option<Self> {
        (s.len() == Self::LEN && s.bytes().all(|b| Self::ALPHABET.contains(&b)))
            .then(|| SessionId(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── Session state ───────────────────────────────────────────────────────────

/// What a session is doing, derived only from facts (journal records and the protocol state).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Phase {
    /// The process is spawned but its handshake has not completed.
    Starting,
    /// Ready, no turn running, nothing pending.
    Idle,
    /// A turn is running.
    Working,
    /// At least one gate waits for a human.
    NeedsYou,
    /// The process exited cleanly: code 0, or any exit after a requested stop.
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
    },
    /// The process exited with an error, was killed unexpectedly, or missed its handshake
    /// deadline.
    Failed {
        reason: String,
        /// The last stderr lines before the failure.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        stderr_tail: Vec<String>,
    },
    /// The shim is gone and the journal has no exit record.
    Lost { reason: String },
}

impl Phase {
    /// The process may still be running (not exited, failed or lost).
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            Phase::Starting | Phase::Idle | Phase::Working | Phase::NeedsYou
        )
    }

    /// Short name (`"starting"`, `"idle"`, …).
    pub fn name(&self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Idle => "idle",
            Phase::Working => "working",
            Phase::NeedsYou => "needs_you",
            Phase::Exited { .. } => "exited",
            Phase::Failed { .. } => "failed",
            Phase::Lost { .. } => "lost",
        }
    }
}

/// A gate waiting for an answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct PendingGate {
    pub gate: GateRequest,
    /// Journal time the gate was raised (ms since the Unix epoch).
    pub raised_at_ms: u64,
    /// Cursor of the `gate_requested` event.
    pub cursor: u64,
}

/// The running turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct TurnInfo {
    pub started_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_turn_id: Option<String>,
    /// The prompt that started it, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Tool calls started in this turn (the turn's steps as far as the runtime can see).
    pub tool_calls: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tool: Option<String>,
    /// The tool running right now (started, not finished), when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_tool: Option<String>,
    /// Call id of `current_tool`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_call: Option<String>,
}

/// How the last turn ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct TurnOutcome {
    pub ended_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_reason: Option<EndReason>,
    pub was_interrupted: bool,
    pub num_turns: u64,
    /// Final reply / error text, truncated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply: Option<String>,
}

/// A subagent started by a tool call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SubagentInfo {
    pub call_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The task description, when the tool input has one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub started_at_ms: u64,
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
}

/// How the process of the latest generation ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ExitSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub at_ms: u64,
    /// A stop was requested before the exit.
    pub requested: bool,
}

/// The continuously reduced state of one session. Rebuilt exactly by replaying its journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SessionSnapshot {
    pub id: SessionId,
    pub driver: DriverId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    pub created_at_ms: u64,
    pub capabilities: DriverCapabilities,
    /// What a prompt may carry besides text.
    #[serde(default)]
    pub prompt_input: PromptInput,
    pub phase: Phase,
    /// Process generations so far (1 after create; +1 per continue).
    pub generation: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    /// The first prompt, truncated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The name the user gave the session (`sessions.rename`), stored with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default)]
    pub available_models: Vec<ModelInfo>,
    #[serde(default)]
    pub available_modes: Vec<SessionMode>,
    #[serde(default)]
    pub slash_commands: Vec<SlashCommandInfo>,
    #[serde(default)]
    pub pending_gates: Vec<PendingGate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<TurnInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_turn: Option<TurnOutcome>,
    pub turns_completed: u64,
    /// Cumulative usage as of the last completed turn.
    pub usage: Usage,
    /// Per-model breakdown of `usage`, when the CLI reports one (claude `modelUsage`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub usage_by_model: BTreeMap<String, ModelUsage>,
    /// The latest live usage report (meaning per driver: see `AgentEvent::UsageUpdate`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_pct: Option<f32>,
    #[serde(default)]
    pub subagents: Vec<SubagentInfo>,
    #[serde(default)]
    pub plan: Vec<PlanEntry>,
    /// Journal time of the last CLI output or user input (ms since the Unix epoch).
    pub last_activity_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit: Option<ExitSummary>,
    /// The last stderr lines (bounded), for diagnostics.
    #[serde(default)]
    pub stderr_tail: Vec<String>,
    /// Cursor of the last event folded into this snapshot.
    pub cursor: u64,
    /// `false` while the snapshot of an exited session is still being rebuilt from its
    /// journal (only `id`, `driver`, labels and a probe-derived phase are meaningful then).
    pub loaded: bool,
}

// ── Session events ──────────────────────────────────────────────────────────

/// What a command did when the session applied it (journal order).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommandStatus {
    /// Encoded and written to the CLI.
    Sent,
    /// Buffered by the protocol (handshake not done, or behind the running turn); its lines go
    /// out later.
    Queued,
    Unsupported {
        operation: String,
    },
    /// The protocol refused it (e.g. the gate is not pending).
    Rejected {
        reason: String,
    },
    NotRunning {
        reason: String,
    },
    /// An answer to an external gate that was withdrawn (its request is gone).
    Withdrawn {
        request_id: String,
    },
}

impl CommandStatus {
    /// As the result delivered to the requester.
    pub fn into_result(self) -> Result<CommandOutcome, CommandError> {
        match self {
            CommandStatus::Sent => Ok(CommandOutcome::Sent),
            CommandStatus::Queued => Ok(CommandOutcome::Queued),
            CommandStatus::Unsupported { operation } => {
                Err(CommandError::Unsupported { operation })
            }
            CommandStatus::Rejected { reason } => Err(CommandError::Rejected { reason }),
            CommandStatus::NotRunning { reason } => Err(CommandError::NotRunning { reason }),
            CommandStatus::Withdrawn { request_id } => {
                Err(CommandError::GateWithdrawn { request_id })
            }
        }
    }
}

/// One session event. `cursor = journal_seq << 16 | index` (index within the events derived
/// from that record; `0xFFFF` for runtime-synthesised events such as [`SessionEventKind::Lost`]),
/// so cursors are monotonic and identical whether an event is produced live or by a replay
/// after a daemon restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SessionEvent {
    pub cursor: u64,
    /// Journal time of the record (ms since the Unix epoch).
    pub t: u64,
    #[serde(flatten)]
    pub kind: SessionEventKind,
}

/// The payload of a [`SessionEvent`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionEventKind {
    /// A process generation started.
    Spawned {
        generation: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pid: Option<u32>,
        #[serde(default)]
        argv: Vec<String>,
    },
    /// Something the driver reported.
    Agent { event: AgentEvent },
    /// A user command, as applied in journal order.
    Input {
        command: Command,
        status: CommandStatus,
    },
    /// A line the CLI wrote to stderr.
    Stderr { text: String },
    /// The phase changed.
    Phase { phase: Phase },
    /// The process exited.
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal: Option<i32>,
    },
    /// The shim vanished without an exit record.
    Lost { reason: String },
    /// A runtime / shim annotation (stop requested, signal sent, handshake deadline, …).
    Notice { message: String },
}

/// Cursor of the `index`-th event derived from journal record `seq`.
pub fn event_cursor(seq: u64, index: usize) -> u64 {
    (seq << 16) | (index.min(0xFFFE) as u64)
}

/// Cursor of a runtime-synthesised event after journal record `seq`.
pub fn synthetic_cursor(seq: u64) -> u64 {
    (seq << 16) | 0xFFFF
}

/// The journal seq a cursor belongs to.
pub fn cursor_seq(cursor: u64) -> u64 {
    cursor >> 16
}

// ── Requests and replies ────────────────────────────────────────────────────

/// Empty success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct Ack {}

/// How a new session starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResumeRequest {
    /// A brand-new conversation.
    #[default]
    New,
    /// Continue the CLI's most recent conversation in `cwd`, the way the CLI's own "continue"
    /// does (claude `--continue`, codex latest thread, ACP latest session). Needs
    /// [`DriverCapabilities::continue_latest`].
    Latest,
    /// Resume one conversation the CLI already has, by its native id (from
    /// `drivers.inventory` sessions). Needs [`DriverCapabilities::resume`].
    Resume { native_session_id: String },
}

/// `sessions.create`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct CreateSession {
    pub driver: DriverId,
    /// Use this id instead of a fresh random one (e.g. so the caller can put the id into the
    /// session's own configuration, such as an MCP server that needs to know which session it
    /// serves). Refused when taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_prompt: Option<String>,
    #[serde(default)]
    pub resume: ResumeRequest,
    #[serde(default)]
    pub config: AgentConfig,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl CreateSession {
    pub fn new(driver: impl Into<DriverId>) -> Self {
        CreateSession {
            driver: driver.into(),
            id: None,
            cwd: None,
            model: None,
            mode: None,
            first_prompt: None,
            resume: ResumeRequest::New,
            config: AgentConfig::default(),
            labels: BTreeMap::new(),
        }
    }
}

/// Result of `sessions.create`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct CreateSessionReply {
    pub id: SessionId,
    /// Config items the driver could not honour (the launch did not fail).
    #[serde(default)]
    pub unsupported: Vec<Unsupported>,
}

/// Names one session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SessionRef {
    pub id: SessionId,
}

/// `sessions.rename`: set (or with `None` / blank, clear) the user-given display name, stored
/// in the session's `session.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct RenameSession {
    pub id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

/// Result of `sessions.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SessionList {
    pub sessions: Vec<SessionSnapshot>,
}

/// `session.history`: the events with `cursor < before` (all when `None`), the last `limit` of
/// them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HistoryRequest {
    pub id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<u64>,
    pub limit: u32,
}

/// Result of `session.history`, oldest first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HistoryPage {
    pub events: Vec<SessionEvent>,
    /// Older events exist (page again with `before = events[0].cursor`).
    pub has_more: bool,
}

/// `session.command`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct CommandRequest {
    pub id: SessionId,
    pub command: Command,
}

/// A command the session accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum CommandOutcome {
    /// Written to the CLI.
    Sent,
    /// Buffered by the protocol; it goes out when the handshake / running turn allows.
    Queued,
}

/// Result of `session.command`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct CommandReply {
    pub outcome: CommandOutcome,
}

/// Why a command was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum CommandError {
    #[error("not supported by this driver: {operation}")]
    Unsupported { operation: String },
    #[error("the session is not running: {reason}")]
    NotRunning { reason: String },
    #[error("rejected: {reason}")]
    Rejected { reason: String },
    #[error("the command was not applied in time")]
    Timeout,
    #[error("the session is busy; try again")]
    Busy,
    #[error("no such session: {id}")]
    SessionNotFound { id: String },
    /// The gate was withdrawn (the request that raised it is gone); the answer did nothing.
    #[error("the gate {request_id} is no longer needed (withdrawn)")]
    GateWithdrawn { request_id: String },
}

impl CommandError {
    pub fn code(&self) -> &'static str {
        match self {
            CommandError::Unsupported { .. } => codes::UNSUPPORTED,
            CommandError::NotRunning { .. } => codes::NOT_RUNNING,
            CommandError::Rejected { .. } => codes::REJECTED,
            CommandError::Timeout => wasit_control::codes::TIMEOUT,
            CommandError::Busy => codes::BUSY,
            CommandError::SessionNotFound { .. } => codes::SESSION_NOT_FOUND,
            CommandError::GateWithdrawn { .. } => codes::GATE_WITHDRAWN,
        }
    }
}

/// A failed runtime operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "error", content = "message", rename_all = "snake_case")]
pub enum RuntimeError {
    #[error("no such session: {0}")]
    SessionNotFound(String),
    #[error("unknown driver: {0}")]
    UnknownDriver(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("not possible in the session's current state: {0}")]
    InvalidState(String),
    #[error("not supported: {0}")]
    Unsupported(String),
    #[error("could not start the session: {0}")]
    Spawn(String),
    #[error("i/o error: {0}")]
    Io(String),
    #[error("busy: {0}")]
    Busy(String),
    #[error("timed out: {0}")]
    Timeout(String),
}

impl RuntimeError {
    pub fn code(&self) -> &'static str {
        match self {
            RuntimeError::SessionNotFound(_) => codes::SESSION_NOT_FOUND,
            RuntimeError::UnknownDriver(_) => codes::UNKNOWN_DRIVER,
            RuntimeError::InvalidRequest(_) => wasit_control::codes::INVALID_PARAMS,
            RuntimeError::InvalidState(_) => codes::INVALID_STATE,
            RuntimeError::Unsupported(_) => codes::UNSUPPORTED,
            RuntimeError::Spawn(_) => codes::SPAWN_FAILED,
            RuntimeError::Io(_) => codes::IO,
            RuntimeError::Busy(_) => codes::BUSY,
            RuntimeError::Timeout(_) => wasit_control::codes::TIMEOUT,
        }
    }
}

// ── Host ────────────────────────────────────────────────────────────────────

/// One driver this host knows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DriverSummary {
    pub id: DriverId,
    pub display_name: String,
    pub capabilities: DriverCapabilities,
    #[serde(default)]
    pub prompt_input: PromptInput,
}

/// Result of `host.info`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HostInfo {
    pub host_id: String,
    pub build: String,
    pub pid: u32,
    pub started_at_ms: u64,
    pub data_dir: String,
    pub os: String,
    pub arch: String,
    pub drivers: Vec<DriverSummary>,
}

/// Where a host's base environment came from (`crate::environment`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum EnvSource {
    /// The login shell in login + interactive mode (rc files included).
    LoginInteractiveShell,
    /// The login shell in login mode only (the interactive attempt failed).
    LoginShell,
    /// Windows: the user's and machine's variables from the registry.
    Registry,
    /// The daemon's own environment (every capture failed; see the notes).
    Process,
}

/// What the host's environment stage found (no values beyond `PATH`: other variables may be
/// secrets).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HostEnvironment {
    pub source: EnvSource,
    /// The shell that was asked (Unix).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub captured_at_ms: u64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub duration_ms: u64,
    /// `PATH` as processes started on this host get it, one entry per element.
    pub path: Vec<String>,
    /// Why a capture attempt failed (deadline, no output, stderr tail).
    #[serde(default)]
    pub notes: Vec<String>,
}

/// Payload of the `host.sessions` topic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostEvent {
    /// The full session list (first event of a fresh subscription, or after an expired cursor).
    Reset { sessions: Vec<SessionSnapshot> },
    /// A session was added or its snapshot changed (coalesced: intermediate snapshots may be
    /// skipped, the latest is always delivered).
    Upsert { session: Box<SessionSnapshot> },
    /// A session was deleted.
    Removed { id: SessionId },
}

// ── Discovery ───────────────────────────────────────────────────────────────

/// `drivers.inventory`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct InventoryRequest {
    /// Project directory for session discovery (`None` = all projects).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// `drivers.refresh`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct RefreshRequest {
    /// Only this driver (`None` = all).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<DriverId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Outcome of one discovery probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ProbeReport {
    pub kind: wasit_core::DiscoveryKind,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// What discovery knows about one driver on this host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DriverInventory {
    pub driver: DriverId,
    pub display_name: String,
    pub capabilities: DriverCapabilities,
    /// The binary was found (`PATH` lookup or configured path).
    pub installed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_path: Option<String>,
    /// Why the driver is unusable here (e.g. the binary was not found).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default)]
    pub models: Vec<ModelInfo>,
    #[serde(default)]
    pub modes: Vec<SessionMode>,
    #[serde(default)]
    pub sessions: Vec<ResumableSession>,
    /// What the CLI itself has configured on this host for `project` (MCP servers, skills,
    /// plugins, commands, agents), each with its source.
    #[serde(default)]
    pub config: Vec<ConfigItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Probes still running.
    pub refreshing: bool,
    #[serde(default)]
    pub probes: Vec<ProbeReport>,
    /// When the last completed refresh finished (0 = never).
    pub refreshed_at_ms: u64,
}

/// Result of `drivers.inventory`; also the payload of the `drivers.inventory` topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct InventoryReply {
    pub drivers: Vec<DriverInventory>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_validated() {
        let id = SessionId::random();
        assert_eq!(SessionId::parse(id.as_str()), Some(id));
        assert!(SessionId::parse("../../etc/pa").is_none());
        assert!(SessionId::parse("ABCDEFGHIJKL").is_none());
        assert!(SessionId::parse("abc").is_none());
    }

    #[test]
    fn cursors_order_by_record_then_index() {
        assert!(event_cursor(5, 3) < event_cursor(5, 4));
        assert!(event_cursor(5, 70000) < synthetic_cursor(5));
        assert!(synthetic_cursor(5) < event_cursor(6, 0));
        assert_eq!(cursor_seq(event_cursor(77, 2)), 77);
    }

    #[test]
    fn event_shape_is_flat() {
        let e = SessionEvent {
            cursor: 1,
            t: 2,
            kind: SessionEventKind::Stderr { text: "x".into() },
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"cursor":1,"t":2,"kind":"stderr","text":"x"})
        );
        assert_eq!(serde_json::from_value::<SessionEvent>(v).unwrap(), e);
    }
}
