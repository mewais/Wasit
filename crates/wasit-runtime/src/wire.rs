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
//! | [`methods::HOST_ENVIRONMENT_CONFIGURE`] | [`ConfigureEnvironment`] | [`HostEnvironment`] (setup, variables, agent commands applied; agents re-detected when it changed) |
//! | [`methods::HOST_ENVIRONMENT_TEST`] | [`ConfigureEnvironment`] | [`EnvironmentTest`] (applied again, setup re-run, agents re-detected) |
//! | [`methods::SESSIONS_LIST`] | — | [`SessionList`] |
//! | [`methods::SESSIONS_CREATE`] | [`CreateSession`] | [`CreateSessionReply`] |
//! | [`methods::SESSIONS_FORK`] | [`ForkSession`] | [`CreateSessionReply`] (the new session; its snapshot names the parent in `forked_from`) |
//! | [`methods::SESSIONS_CONTINUE`] | [`SessionRef`] | [`Ack`] |
//! | [`methods::SESSIONS_STOP`] | [`SessionRef`] | [`Ack`] |
//! | [`methods::SESSIONS_DELETE`] | [`SessionRef`] | [`Ack`] |
//! | [`methods::SESSIONS_RENAME`] | [`RenameSession`] | [`Ack`] |
//! | [`methods::SESSION_SNAPSHOT`] | [`SessionRef`] | [`SessionSnapshot`] |
//! | [`methods::SESSION_HISTORY`] | [`HistoryRequest`] (`earlier: true` pages the conversation's earlier history instead of the session's own events, see [`EarlierHistory`]) | [`HistoryPage`] |
//! | [`methods::SESSION_TRAIL_PAGE`] | [`TrailRequest`] | [`TrailPage`] |
//! | [`methods::SESSION_COMMAND`] | [`CommandRequest`] | [`CommandReply`] |
//! | [`methods::SESSION_ATTACHMENT_PUT`] | [`crate::media::AttachmentPut`] | [`crate::media::AttachmentPutReply`] |
//! | [`methods::SESSION_FILE_READ`] | [`crate::media::FileReadRequest`] | [`crate::media::FileChunk`] |
//! | [`methods::SESSION_FILES_STAT`] | [`crate::media::FilesStatRequest`] | [`crate::media::FilesStatReply`] |
//! | [`methods::SESSION_LOCAL_SHELL`] | [`LocalShellRequest`] | [`Ack`] |
//! | [`methods::DIAG_SOURCES`] | — | [`DiagSources`] (served by [`crate::DiagService`], routes [`DIAG_ROUTES`]) |
//! | [`methods::DIAG_READ`] | [`DiagReadRequest`] | [`DiagChunk`] (served by [`crate::DiagService`]) |
//!
//! Errors are `RpcError`s whose `code` is one of [`codes`]; command failures carry a
//! [`CommandError`] and other failures a [`RuntimeError`] as `data`.
//!
//! # Topics (`Subscribe.topic` → event payload, cursor)
//!
//! | topic | payload | cursor |
//! |---|---|---|
//! | [`topics::HOST_SESSIONS`] | [`HostEvent`] | `"<epoch>.<n>"`; an unknown or expired cursor restarts with [`HostEvent::Reset`] |
//! | `session.events:<id>` ([`topics::session_events`]) | [`SessionEvent`] (the loading of earlier history as [`SessionEventKind::Earlier`], never its items) | [`SessionEvent::cursor`] as a decimal `u64`; stable across daemon restarts |
//! | `session.trail:<id>` ([`topics::session_trail`]) | [`TrailItem`] | the cursor of the [`SessionEvent`] it was projected from, as a decimal `u64`: the same cursors as `session.events:<id>`, stable across daemon restarts |
//! | [`topics::DRIVERS_INVENTORY`] | [`InventoryReply`] | `"<epoch>.<n>"`; every event is a full inventory |
//! | [`topics::DIAG_TAIL`] (params [`DiagTailParams`]; served by [`crate::DiagService`]) | [`DiagTailEvent`] | `"<boot>.<seq>"` of the process's diagnostics sink; an unknown or expired cursor restarts with [`DiagTailEvent::Reset`] |

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use wasit_control::TraceId;
use wasit_core::{
    AgentConfig, AgentEvent, Command, ConfigItem, DriverCapabilities, DriverId, EndReason,
    GateRequest, HistoryReport, InputId, ModelInfo, ModelUsage, PlanEntry, PromptInput,
    ResumableSession, SessionMode, SlashCommandInfo, TaskInfo, TaskUsage, TurnOrigin, TurnTrigger,
    Unsupported, Usage,
};

/// Method names.
pub mod methods {
    pub const HOST_INFO: &str = "host.info";
    pub const HOST_ENVIRONMENT: &str = "host.environment";
    pub const HOST_ENVIRONMENT_REFRESH: &str = "host.environment.refresh";
    pub const HOST_ENVIRONMENT_CONFIGURE: &str = "host.environment.configure";
    pub const HOST_ENVIRONMENT_TEST: &str = "host.environment.test";
    pub const DRIVERS_INVENTORY: &str = "drivers.inventory";
    pub const DRIVERS_REFRESH: &str = "drivers.refresh";
    pub const SESSIONS_LIST: &str = "sessions.list";
    pub const SESSIONS_CREATE: &str = "sessions.create";
    pub const SESSIONS_FORK: &str = "sessions.fork";
    pub const SESSIONS_CONTINUE: &str = "sessions.continue";
    pub const SESSIONS_STOP: &str = "sessions.stop";
    pub const SESSIONS_DELETE: &str = "sessions.delete";
    pub const SESSIONS_RENAME: &str = "sessions.rename";
    pub const SESSION_SNAPSHOT: &str = "session.snapshot";
    pub const SESSION_HISTORY: &str = "session.history";
    pub const SESSION_SEARCH: &str = "session.search";
    pub const SESSION_TRAIL_PAGE: &str = "session.trail.page";
    pub const SESSION_COMMAND: &str = "session.command";
    pub const SESSION_ATTACHMENT_PUT: &str = "session.attachment.put";
    pub const SESSION_FILE_READ: &str = "session.file.read";
    pub const SESSION_FILES_STAT: &str = "session.files.stat";
    pub const SESSION_LOCAL_SHELL: &str = "session.local_shell";
    pub const DIAG_SOURCES: &str = "diag.sources";
    pub const DIAG_READ: &str = "diag.read";
}

/// Topic names.
pub mod topics {
    pub const HOST_SESSIONS: &str = "host.sessions";
    pub const DRIVERS_INVENTORY: &str = "drivers.inventory";
    pub const DIAG_TAIL: &str = "diag.tail";
    /// Prefix of the per-session event topic.
    pub const SESSION_EVENTS_PREFIX: &str = "session.events:";
    /// Prefix of the per-session trail topic (the content-free projection of the events).
    pub const SESSION_TRAIL_PREFIX: &str = "session.trail:";

    /// The event topic of one session.
    pub fn session_events(id: &str) -> String {
        format!("{SESSION_EVENTS_PREFIX}{id}")
    }

    /// The trail topic of one session.
    pub fn session_trail(id: &str) -> String {
        format!("{SESSION_TRAIL_PREFIX}{id}")
    }
}

/// The routes [`crate::RuntimeService`] serves, for registering it in a
/// [`wasit_control::ServiceMux`] next to other services.
pub const ROUTES: &[&str] = &[
    methods::HOST_INFO,
    methods::HOST_ENVIRONMENT,
    methods::HOST_ENVIRONMENT_REFRESH,
    methods::HOST_ENVIRONMENT_CONFIGURE,
    methods::HOST_ENVIRONMENT_TEST,
    topics::HOST_SESSIONS,
    "drivers.",
    "sessions.",
    "session.",
];

/// The routes [`crate::DiagService`] serves (its own constant: a daemon mounts that service
/// next to [`crate::RuntimeService`], and a hub routes these by `host` like [`ROUTES`]).
pub const DIAG_ROUTES: &[&str] = &["diag."];

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
    pub const CONTINUE_FAILED: &str = "continue_failed";
    pub const BAD_WORKING_DIRECTORY: &str = "bad_working_directory";
    pub const NOTHING_TO_CONTINUE: &str = "nothing_to_continue";
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
    /// Ready: no turn running, no background task running, nothing pending.
    Idle,
    /// The agent is working: a turn is running (`snapshot.turn`), or no turn is open but a task
    /// runs in the background (`snapshot.background_running > 0`, `snapshot.turn` is `None`).
    /// It is never `Idle` while either is true; a client that treats `Working` as "busy" is
    /// right in both cases.
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

    /// A process is starting or a turn, a background task or a gate holds it: what a fork or a
    /// compaction must not interrupt. Decided from the phase alone.
    pub fn is_busy(&self) -> bool {
        matches!(self, Phase::Starting | Phase::Working | Phase::NeedsYou)
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
    /// The input the turn runs for, when the agent said so (see `trigger`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<InputId>,
    /// What started the turn: one of our inputs, the agent itself, or the driver cannot tell.
    #[serde(default)]
    pub trigger: TurnTrigger,
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
    /// Why the agent ran the turn when it started it by itself and said why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<TurnOrigin>,
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
    /// From its start until the agent says its task ended (a backgrounded subagent keeps
    /// running after the tool call that started it returned and after the turn ended); for an
    /// agent that reports no tasks, until the tool call returned or the turn ended.
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    /// The agent's task for it ([`TaskInfo::task_id`]; what `Command::StopTask` takes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// The subagent's own id, from the lines it produced (`Scope::agent_id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    /// The agent's status word for how its task ended (`completed`, `failed`, `stopped`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<u64>,
    /// Tool calls the subagent made (events scoped to it).
    #[serde(default)]
    pub tool_calls: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tool: Option<String>,
    /// What the agent says the task used (not part of the session's own `usage`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TaskUsage>,
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

/// Why a session cannot be continued ([`SessionSnapshot::not_continuable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum NotContinuable {
    /// The process is running (or starting): there is nothing to continue.
    NotEnded,
    /// The snapshot is still being rebuilt from the journal (`loaded` is `false`).
    Loading,
    /// The agent cannot resume a conversation.
    AgentCannotResume,
    /// The agent has not reported a conversation id yet and none was chosen for it.
    NoConversationId,
    /// The agent reported a conversation id but has not saved the conversation (it saves it with
    /// its first turn), so there is nothing to resume and the id cannot be used to start again.
    NothingSaved,
}

/// A command the user runs in their own terminal ([`SessionSnapshot::resume_command`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ResumeCommand {
    /// The program and its arguments; never a shell string.
    pub argv: Vec<String>,
    /// The directory to run it in: the session's working directory.
    pub cwd: String,
}

/// Where a conversation's earlier history came from ([`EarlierHistory::source`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum EarlierSource {
    /// The agent sent it over its protocol while the session opened the conversation (codex,
    /// OpenCode, Kilo).
    AgentReplay,
    /// The runtime read it from the agent's own storage (claude's transcript file).
    AgentStorage,
}

/// Why a resumed conversation's earlier history is not available ([`EarlierState::Unavailable`]).
/// Never a silent empty transcript: this says what is wrong.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EarlierUnavailable {
    /// The agent gives its history out neither over its protocol nor in storage the runtime can
    /// read.
    NotOffered,
    /// The agent's file for the conversation does not exist.
    SourceMissing,
    /// The agent's file exists and could not be read (`detail` is the host's own wording).
    SourceUnreadable { detail: String },
    /// The agent's file is larger than the runtime reads.
    TooLarge {
        #[cfg_attr(feature = "ts", ts(type = "number"))]
        bytes: u64,
        #[cfg_attr(feature = "ts", ts(type = "number"))]
        limit: u64,
    },
    /// Nothing in the file is a record of the format the driver knows (`record`: the 1-based
    /// number of the first line it did not understand).
    FormatNotUnderstood {
        #[cfg_attr(feature = "ts", ts(type = "number"))]
        record: u64,
    },
    /// The import failed for a reason of the runtime's own (`detail`).
    Failed { detail: String },
}

/// Where the loading of a resumed conversation's earlier history stands.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EarlierState {
    /// The conversation began in this session (or the session predates earlier history):
    /// nothing earlier exists, or none is expected.
    #[default]
    None,
    /// The history is arriving; what is there already can be shown. The session is usable
    /// meanwhile.
    Loading,
    /// The history is in the journal. `complete: false`: something of the agent's history is not
    /// shown (see [`EarlierHistory::gaps`]).
    Loaded { complete: bool },
    /// It could not be loaded, and why.
    Unavailable { reason: EarlierUnavailable },
}

/// What the history that was loaded leaves out. Every count is of the agent's own records or
/// messages, never a guess.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct EarlierGaps {
    /// Messages older than the import window (or items beyond what one record can carry): they
    /// exist in the agent's own history and are not shown.
    #[serde(default)]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub omitted: u64,
    /// Items cut to the per-item bound, or whose media was left out.
    #[serde(default)]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub shortened: u64,
    /// Lines of the agent's file that were not records.
    #[serde(default)]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub unreadable: u64,
    /// Records of kinds the driver does not know (`unrecognised_kinds`: the first few names).
    #[serde(default)]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub unrecognised: u64,
    #[serde(default)]
    pub unrecognised_kinds: Vec<String>,
    /// Files of the conversation (a subagent's transcript) that could not be read.
    #[serde(default)]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub missing_files: u64,
    /// The last line of the file had no end: the agent may have been writing it.
    #[serde(default)]
    pub truncated_tail: bool,
    /// The process ended before all of the history arrived.
    #[serde(default)]
    pub interrupted: bool,
    /// How many times the agent sent the history again (a later start of the same session
    /// reloads the conversation) and the runtime, which already had it, ignored it.
    #[serde(default)]
    pub replays_ignored: u32,
}

impl EarlierGaps {
    /// Nothing is left out.
    pub fn is_empty(&self) -> bool {
        self.omitted == 0
            && self.shortened == 0
            && self.unreadable == 0
            && self.unrecognised == 0
            && self.missing_files == 0
            && !self.truncated_tail
            && !self.interrupted
    }

    /// What `report` says is left out.
    pub fn of_report(r: &HistoryReport) -> EarlierGaps {
        EarlierGaps {
            omitted: r.older_omitted,
            shortened: r.shortened,
            unreadable: r.unreadable,
            unrecognised: r.unrecognised,
            unrecognised_kinds: r.unrecognised_kinds.clone(),
            missing_files: r.missing_files,
            truncated_tail: r.truncated_tail,
            interrupted: false,
            replays_ignored: 0,
        }
    }
}

/// What a session knows of the conversation before it: the earlier messages of a conversation
/// it resumed (see `ARCHITECTURE.md` §3.8 for the contract clients render by).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct EarlierHistory {
    pub state: EarlierState,
    /// Where the history comes from, once it is known to be coming.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<EarlierSource>,
    /// History items in the journal so far (the transcript's earlier part has this many events:
    /// page them with `session.history {earlier: true}`).
    #[serde(default)]
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub items: u64,
    #[serde(default)]
    pub gaps: EarlierGaps,
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
    /// The command this session's CLI is started as instead of the driver's default binary
    /// (the host's agent command, or a configured binary), stored with the session: a
    /// continued session keeps it. `None`: the default binary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    pub capabilities: DriverCapabilities,
    /// What a prompt may carry besides text.
    #[serde(default)]
    pub prompt_input: PromptInput,
    pub phase: Phase,
    /// Process generations so far (1 after create; +1 per continue).
    pub generation: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// The agent's own id of the conversation, known from the session's creation when the agent
    /// accepts an id chosen by the runtime (or the session resumes one), else once the agent
    /// reports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,
    /// A new process generation can be started now (`sessions.continue`, or a prompt sent to the
    /// session, which continues it first). Decided by the runtime alone, from the driver's
    /// capabilities and the session's state; clients show what it says.
    #[serde(default)]
    pub continuable: bool,
    /// Why not, when `continuable` is `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_continuable: Option<NotContinuable>,
    /// How the user opens this conversation in their own CLI, when the agent's storage holds it
    /// and the driver knows a command; clients only show or copy it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_command: Option<ResumeCommand>,
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
    /// The tasks the agent runs or ran (subagents, shell commands), newest last, bounded.
    #[serde(default)]
    pub tasks: Vec<TaskInfo>,
    /// How many tasks run in the background right now. While it is above zero the phase is
    /// `working`, with or without a `turn`.
    #[serde(default)]
    pub background_running: u32,
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
    /// What the session knows of the conversation before it (see [`EarlierHistory`]).
    #[serde(default)]
    pub earlier: EarlierHistory,
    /// The session this one forked from ([`ForkedFrom`]); `None` for a session that started its
    /// own conversation or resumed one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<ForkedFrom>,
}

/// The lineage of a forked session: the session it was forked from and the conversation it
/// copied. Set once, when the fork is created; kept in the session's `session.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ForkedFrom {
    /// The parent session.
    pub session: SessionId,
    /// The parent's native conversation id (the one the fork copied).
    pub native_session_id: String,
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

/// What made the phase change (the last fact of the journal record that decided the new phase).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PhaseCause {
    /// A process generation was spawned.
    Spawned { generation: u32 },
    /// The CLI completed its handshake.
    HandshakeDone,
    /// A turn started (for this input, when it is one of ours; `trigger` says what started it).
    TurnStarted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<InputId>,
        #[serde(default)]
        trigger: TurnTrigger,
    },
    /// A background task started running (the agent works in the background).
    TaskStarted { task_id: String },
    /// A background task stopped running.
    TaskEnded { task_id: String },
    /// A turn ended.
    TurnCompleted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_reason: Option<EndReason>,
        interrupted: bool,
    },
    /// A gate was raised (by the CLI, or from outside the CLI protocol: `external`).
    GateRaised { request_id: String, external: bool },
    /// A gate stopped being pending.
    GateCleared {
        request_id: String,
        how: GateClearing,
    },
    /// The process exited.
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal: Option<i32>,
        /// A stop was requested before the exit.
        requested: bool,
        /// The handshake deadline had passed.
        handshake_timeout: bool,
    },
    /// The shim vanished without an exit record.
    Lost,
    /// Nothing more specific is known (also: a peer that predates causes).
    #[default]
    Other,
}

/// How a gate stopped being pending ([`PhaseCause::GateCleared`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum GateClearing {
    /// Answered by the human (or a policy they set).
    Answered,
    /// Resolved by the CLI or the protocol (e.g. declined on interrupt).
    Resolved,
    /// An external gate taken down by its owner, or ended with its process.
    Withdrawn,
}

/// What a [`SessionEventKind::Notice`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum NoticeCode {
    /// A graceful stop was requested.
    StopRequested,
    /// The handshake deadline passed; the process is being stopped.
    HandshakeDeadline,
    /// A signal was sent to the process.
    SignalSent,
    /// A new process generation started although the journal never recorded the previous
    /// one's exit.
    PreviousGenerationUnclosed,
    /// A `start` note arrived outside a fresh process generation and was ignored.
    StartIgnored,
    /// A note of the shim about itself.
    Shim,
    /// A runtime note this build does not understand.
    UnrecognisedNote,
    /// Anything else.
    #[default]
    Other,
}

/// How a process generation was started ([`SessionEventKind::Started`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum StartResume {
    Fresh,
    /// The CLI's most recent conversation in the working directory.
    Continue,
    /// A conversation by its native id.
    Resume,
    /// A new conversation that copies the one the `resume_id` of the start names; that one is
    /// left unchanged ([`wasit_core::ResumeMode::Fork`]).
    Fork,
}

/// A fact the runtime noticed that does not fit the session's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    /// The agent produced output (text, thinking, tool calls) while no turn of the session was
    /// running (`idle`), e.g. background work or a turn the CLI started by itself.
    ActivityWhileIdle,
}

/// The payload of a [`SessionEvent`].
///
/// Variants marked `#[non_exhaustive]` gain fields over time: match them with `..`.
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
    /// The generation's `start` note was applied: how the CLI was asked to start.
    #[non_exhaustive]
    Started {
        generation: u32,
        resume: StartResume,
        /// The native id resumed ([`StartResume::Resume`]).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resume_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
        /// The first prompt journaled with the start (its `input` event follows).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        first_input: Option<InputId>,
        /// The trace id of the request that started the session, when it had one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<TraceId>,
    },
    /// Something the driver reported.
    Agent { event: AgentEvent },
    /// A user command, as applied in journal order. The first prompt of a session is one too
    /// (journaled with the `start` note).
    #[non_exhaustive]
    Input {
        /// Its identity: the journal seq of the note that carried it.
        #[serde(default)]
        input: InputId,
        command: Command,
        status: CommandStatus,
        /// The trace id of the request that carried the command, when it had one (a first
        /// prompt takes the one of the request that started the session).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<TraceId>,
    },
    /// The lines an input asked to write all reached the shim's journal of stdin writes (the
    /// shim records a line, then writes it; a failure follows as [`Self::WriteFailed`]).
    #[non_exhaustive]
    Written { input: InputId },
    /// Writing to the CLI's stdin failed (for the input whose line it was, when known).
    #[non_exhaustive]
    WriteFailed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<InputId>,
        error: String,
    },
    /// A line the CLI wrote to stderr.
    Stderr { text: String },
    /// The phase changed, and why.
    #[non_exhaustive]
    Phase {
        phase: Phase,
        #[serde(default)]
        cause: PhaseCause,
    },
    /// Something that does not fit the session's state (once per occurrence period: an
    /// `activity_while_idle` is reported once until the phase changes). `first` is the type of
    /// the first event that showed it (e.g. `tool_call_started`).
    #[non_exhaustive]
    Anomaly { anomaly: AnomalyKind, first: String },
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
    #[non_exhaustive]
    Notice {
        message: String,
        #[serde(default)]
        code: NoticeCode,
    },
    /// The loading of the resumed conversation's earlier history moved (it began, made
    /// progress, ended complete or partial, or turned out unavailable): the new state, which is
    /// also the snapshot's `earlier`. The history items themselves are never in the live event
    /// stream: they are paged with `session.history {earlier: true}`.
    #[non_exhaustive]
    Earlier { earlier: EarlierHistory },
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
    /// The session's name: its display name exactly as `sessions.rename` would set it (trimmed,
    /// bounded; blank is none), and, where the agent's CLI can name its own conversation when it
    /// starts one, the conversation's title in the agent's own storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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
            name: None,
        }
    }
}

/// `sessions.fork`: a new session that copies the conversation of `parent`, with the parent's
/// driver, working directory and agent configuration. The parent is never changed. Refused with
/// [`RuntimeError::Unsupported`] when its driver cannot fork, [`RuntimeError::Busy`] while it is
/// starting or working, and [`RuntimeError::InvalidState`] when it has no completed turn or no
/// conversation id yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ForkSession {
    pub parent: SessionId,
    /// The new session's name (as `sessions.rename` would set it); blank is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Result of `sessions.create` (and of `sessions.fork`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct CreateSessionReply {
    pub id: SessionId,
    /// The working directory as the host normalised it (`~` expanded, canonical): what the
    /// session runs in and its snapshot reports; clients that remember project folders store
    /// this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
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
    /// Which events: everything (default), the session's own transcript, or one subagent's.
    #[serde(default, skip_serializing_if = "HistoryScope::is_all")]
    pub scope: HistoryScope,
    /// Page the conversation's *earlier* history instead of the session's own events: the items
    /// ([`wasit_core::AgentEvent::History`]) the journal holds of the conversation before this
    /// session, oldest first, with the same `before` / `limit` / `has_more` and the same
    /// `scope` filters (applied to the events inside the history items). Their cursors are
    /// journal cursors like any other (greater than the live events shown before the history
    /// arrived): render them above the live transcript, in cursor order among themselves.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub earlier: bool,
    /// Byte budget of the page, see [`page_budget`]. A page stops once it has read this many
    /// bytes of the log, after at least one item, so it may exceed the budget by one event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u32>,
}

/// Byte budget of a history or trail page when the request names none.
pub const DEFAULT_PAGE_BYTES: u32 = 1 << 20;
/// Largest byte budget a page may ask for; larger requests are clamped to it.
pub const MAX_PAGE_BYTES: u32 = 4 << 20;

/// The byte budget of a page: the request's (clamped to `1..=MAX_PAGE_BYTES`), or
/// [`DEFAULT_PAGE_BYTES`] when it names none.
pub fn page_budget(requested: Option<u32>) -> u64 {
    requested
        .unwrap_or(DEFAULT_PAGE_BYTES)
        .clamp(1, MAX_PAGE_BYTES) as u64
}

/// Which events a history page holds. The cursors are the same whatever the scope (a scoped page
/// is the filter of the full stream), so `before` pages a subagent's transcript exactly like the
/// session's, and `has_more` says whether older events *of that scope* exist.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HistoryScope {
    /// Every event (what the history API always returned).
    #[default]
    All,
    /// The session's own transcript: every event except the `scoped` ones of subagents.
    Main,
    /// One subagent's transcript: the `scoped` events of the tool call that spawned it
    /// ([`SubagentInfo::call_id`]) and the start and end of its task.
    Subagent { call_id: String },
}

impl HistoryScope {
    pub fn is_all(&self) -> bool {
        matches!(self, HistoryScope::All)
    }

    /// Whether `event` belongs to this scope.
    pub fn admits(&self, event: &SessionEvent) -> bool {
        let SessionEventKind::Agent { event: agent } = &event.kind else {
            return !matches!(self, HistoryScope::Subagent { .. });
        };
        // An earlier-history item is filtered by what it holds.
        let agent = match agent {
            AgentEvent::History { event, .. } => event.as_ref(),
            other => other,
        };
        match self {
            HistoryScope::All => true,
            HistoryScope::Main => !matches!(agent, AgentEvent::Scoped { .. }),
            HistoryScope::Subagent { call_id } => match agent {
                AgentEvent::Scoped { scope, .. } => scope.call_id == *call_id,
                AgentEvent::TaskStarted { call_id: c, .. }
                | AgentEvent::TaskEnded { call_id: c, .. } => c.as_deref() == Some(call_id),
                _ => false,
            },
        }
    }
}

/// Result of `session.history`, oldest first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HistoryPage {
    pub events: Vec<SessionEvent>,
    /// Older events exist (page again with `before = events[0].cursor`).
    pub has_more: bool,
}

/// Which event log a search reads: the session's own events, or the earlier history of the
/// conversation (see [`HistoryRequest::earlier`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum SearchPart {
    #[default]
    Live,
    Earlier,
}

/// The order a search walks the log in: `backward` from the anchor towards the start (newest
/// first), or `forward` towards the end (oldest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum SearchDirection {
    #[default]
    Backward,
    Forward,
}

/// `session.search`: the hits of `query` in the session's transcript, from the anchor in the
/// direction, at most `limit` (clamped to `1..=50`) per page.
///
/// Reads the derived event log only, never starts an actor. A page examines at most 8 MiB of
/// records; when it stops short of the end of the log, `next` continues it. Pass the `next` of
/// the previous page as `resume` to continue (with the same query, scope, kinds, direction
/// and part), or give an `anchor` to start: `anchor` is an exclusive cursor, the next event
/// before it when backward and after it when forward. Give one of the two, not both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SearchRequest {
    pub session: SessionId,
    pub query: String,
    /// Which events: everything (default), the session's own transcript, or one subagent's.
    #[serde(default, skip_serializing_if = "HistoryScope::is_all")]
    pub scope: HistoryScope,
    /// The fields to match, by name (`user_text`, `assistant_text`, `thinking`, `tool_input`,
    /// `tool_result`, `notice`); every field when none is named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<String>>,
    #[serde(default)]
    pub direction: SearchDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<u64>,
    /// The `next` of the previous page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<String>,
    #[serde(default)]
    pub part: SearchPart,
    pub limit: u32,
}

/// One match: the cursor and journal time of its event, the field it is in, and a snippet of
/// the field around it. `start` and `len` are character offsets of the match within `snippet`
/// (so the app highlights it without a second search).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SearchHit {
    pub cursor: u64,
    pub part: SearchPart,
    pub field: String,
    pub snippet: String,
    pub start: u32,
    pub len: u32,
    pub at: u64,
}

/// Result of `session.search`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    /// Resume token: send it as `resume` to continue the search. Absent when the search reached
    /// the end of the log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Bytes of event-log records this page examined.
    pub scanned_bytes: u64,
}

/// `session.trail.page`: the trail entries with `cursor < before` (all when `None`), the last
/// `limit` of them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct TrailRequest {
    pub id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<u64>,
    pub limit: u32,
    /// Byte budget of the page, see [`page_budget`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u32>,
}

/// One entry of a session's trail with the cursor and journal time of the event it was
/// projected from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct TrailItem {
    pub cursor: u64,
    /// Journal time of the record (ms since the Unix epoch).
    pub t: u64,
    #[serde(flatten)]
    pub entry: crate::trail::TrailEntry,
}

/// Result of `session.trail.page`, oldest first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct TrailPage {
    pub entries: Vec<TrailItem>,
    /// Older entries exist (page again with `before = entries[0].cursor`).
    pub has_more: bool,
}

/// `session.command`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct CommandRequest {
    pub id: SessionId,
    pub command: Command,
}

/// What a shell command the host ran for the user printed and exited with (`session.local_shell`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct LocalShellRecord {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// `None` when the command did not exit normally (killed, or no code reported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub output: String,
    /// The output was cut before it reached the runtime.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub output_truncated: bool,
}

impl LocalShellRecord {
    /// The most output the journal keeps for one command. The runtime cuts longer output here,
    /// so one record stays well inside the shim's record bound.
    pub const OUTPUT_MAX: usize = 1024 * 1024;

    /// The record with its output cut to [`Self::OUTPUT_MAX`] bytes on a character boundary;
    /// cut output is marked `output_truncated`.
    pub fn capped(mut self) -> Self {
        if self.output.len() > Self::OUTPUT_MAX {
            let cut = (0..=Self::OUTPUT_MAX)
                .rev()
                .find(|&i| self.output.is_char_boundary(i))
                .unwrap_or(0);
            self.output.truncate(cut);
            self.output_truncated = true;
        }
        self
    }
}

/// `session.local_shell`: journal a shell command the host ran for the user (`!`) after it
/// finished, as a `local_shell` event of the session (origin `host`). A session whose process
/// is not running refuses it with [`CommandError::NotRunning`]: nothing can journal it then.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct LocalShellRequest {
    pub session: SessionId,
    #[serde(flatten)]
    pub shell: LocalShellRecord,
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
    /// The session had ended and sending the command continues it first; the continue failed,
    /// so nothing was journaled for the command and it was not sent.
    #[error("the session had ended and could not be continued, so nothing was sent: {reason}")]
    ContinueFailed {
        reason: String,
        /// The rule that forbids continuing, when that is why (see
        /// [`SessionSnapshot::not_continuable`]).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blocked: Option<NotContinuable>,
    },
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
            CommandError::ContinueFailed { .. } => codes::CONTINUE_FAILED,
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
    /// The working directory was refused (it names the path as given).
    #[error("{0}")]
    WorkingDirectory(String),
    /// "Continue the most recent conversation" found none in the folder (the message names it):
    /// nothing was started, and nothing is started in its place.
    #[error("there is no earlier conversation to continue in {0}")]
    NothingToContinue(String),
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
            RuntimeError::WorkingDirectory(_) => codes::BAD_WORKING_DIRECTORY,
            RuntimeError::NothingToContinue(_) => codes::NOTHING_TO_CONTINUE,
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
/// secrets; texts are scrubbed of the configured secret values).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HostEnvironment {
    /// Where the login environment came from (the first stage).
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
    /// The configuration in effect ([`ConfigureEnvironment::revision`]; `None`: never
    /// configured since this daemon started).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// How the setup commands went (`None`: there are none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<SetupReport>,
    /// Names of the variables the configuration sets (values are never reported).
    #[serde(default)]
    pub variables: Vec<String>,
    /// The agent command of each driver that has one.
    #[serde(default)]
    pub commands: BTreeMap<DriverId, String>,
}

/// One run of a host's setup commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SetupReport {
    /// The commands ran to the end with exit status 0 and their environment was taken.
    pub ok: bool,
    /// What ran them (`/bin/bash -l -i`, `powershell -NoProfile`).
    pub shell: String,
    /// Exit status of the last command (or of the shell, when the setup ended it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Why it failed, in plain words (deadline, ended the shell, syntax error…).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The last lines the commands printed (stdout and stderr together).
    #[serde(default)]
    pub output: String,
    /// The environment after the commands is the one processes get (`false`: the login
    /// environment without the setup's changes, because the setup did not finish).
    pub applied: bool,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub duration_ms: u64,
}

/// A host's environment configuration, applied after the login capture, in this order:
/// setup commands → variables → agent commands (ARCHITECTURE.md §3.5).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HostEnvConfig {
    /// Shell lines run in the host's login shell (PowerShell on Windows) before agents,
    /// sessions, terminals and git start; the environment they leave is what those get.
    #[serde(default)]
    pub setup: String,
    #[serde(default)]
    pub vars: Vec<EnvVar>,
    /// Per driver: a command (name on `PATH`, or a path) that stands in for the CLI.
    #[serde(default)]
    pub commands: BTreeMap<DriverId, String>,
}

/// One variable. A secret's value is never reported, logged or put in an error.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct EnvVar {
    pub name: String,
    pub value: String,
    #[serde(default)]
    pub secret: bool,
}

impl std::fmt::Debug for EnvVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value: &dyn std::fmt::Debug = if self.secret {
            &"<secret>"
        } else {
            &self.value
        };
        f.debug_struct("EnvVar")
            .field("name", &self.name)
            .field("value", value)
            .field("secret", &self.secret)
            .finish()
    }
}

impl HostEnvConfig {
    /// Names are `[A-Za-z_][A-Za-z0-9_]*` and unique (case-insensitively: Windows); agent
    /// commands are not blank. Messages name variables, never values.
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        for v in &self.vars {
            let ok = v
                .name
                .starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                && v.name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !ok {
                return Err(format!(
                    "{:?} is not a variable name (letters, digits and '_', not starting with a digit)",
                    v.name
                ));
            }
            if !seen.insert(v.name.to_ascii_uppercase()) {
                return Err(format!("variable {} is set twice", v.name));
            }
            if v.value.contains('\0') {
                return Err(format!("the value of {} contains a NUL character", v.name));
            }
        }
        for (d, c) in &self.commands {
            if c.trim().is_empty() || c.trim() != c {
                return Err(format!("the {d} command is blank or has spaces around it"));
            }
        }
        Ok(())
    }

    /// The values of the secret variables (to scrub from texts).
    pub fn secrets(&self) -> Vec<&str> {
        self.vars
            .iter()
            .filter(|v| v.secret && !v.value.is_empty())
            .map(|v| v.value.as_str())
            .collect()
    }
}

/// `host.environment.configure` / `.test`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ConfigureEnvironment {
    pub config: HostEnvConfig,
    /// Chosen by whoever owns the configuration; reported back in
    /// [`HostEnvironment::revision`] so it can tell whether a host has it.
    pub revision: String,
}

/// Result of `host.environment.test`: the environment after the setup ran again, and every
/// driver as discovery then found it (command, resolved path, version or error, warning).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct EnvironmentTest {
    pub environment: HostEnvironment,
    pub agents: Vec<DriverInventory>,
    /// Every driver finished probing within the test's deadline (else some are still
    /// `refreshing` and their results arrive with the inventory topic).
    pub complete: bool,
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

// ── Diagnostics ─────────────────────────────────────────────────────────────

/// Result of `diag.sources`: the diagnostics files this process wrote (named as the sink names
/// them: `daemon`, `aux`, …) and, for each session it knows with a shim log, `shim:<session id>`.
/// Generations are oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DiagSources {
    /// This host's wall clock (ms since the Unix epoch) when it answered: the reader measures the
    /// round trip and so the offset between the clocks; no time in a file is ever rewritten.
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub now_ms: u64,
    /// The process's boot id (`b` of its records).
    pub boot: String,
    pub sources: Vec<DiagSource>,
}

/// One source of [`DiagSources`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DiagSource {
    pub name: String,
    pub gens: Vec<DiagGen>,
}

/// One file of a source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DiagGen {
    /// The generation number in the file's header; it stays with the file when it is rotated.
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub gen: u64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub bytes: u64,
    /// The `t` of the first and last record of the file, when it has records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    pub first_t: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(type = "number | null"))]
    pub last_t: Option<u64>,
}

/// Largest `max_bytes` of `diag.read` (2 MiB of NDJSON, escaped once more as a JSON string, is
/// far below the control protocol's frame limit).
pub const DIAG_READ_MAX: u32 = 2 << 20;

/// `diag.read`: at most `max_bytes` (1 ..= [`DIAG_READ_MAX`]) of whole lines of generation
/// `gen` of `source`, from `offset`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DiagReadRequest {
    /// A name from `diag.sources`; nothing else is accepted.
    pub source: String,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub gen: u64,
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub offset: u64,
    pub max_bytes: u32,
}

/// Result of `diag.read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DiagChunk {
    /// Complete lines, each ending in `\n`; a trailing partial line is left for the next read.
    pub data: String,
    /// The offset to read from next.
    #[cfg_attr(feature = "ts", ts(type = "number"))]
    pub next: u64,
    /// `next` was the end of the file when it was read.
    pub eof: bool,
    /// The generation no longer exists (rotated away): nothing was read.
    pub gone: bool,
}

/// Params of the `diag.tail` topic; a record must match every filter given. Most records carry
/// none of these keys: a filter on one of them excludes them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DiagTailParams {
    /// Records whose `host` field is this host (a daemon's own records name a host only when
    /// they are about one: a hub's link records, a host's own start record). Which host a
    /// request is served by is the request's `host`, not this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Records about this session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionId>,
    /// Records of any of these traces (at most [`DIAG_TAIL_TRACES`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub traces: Vec<TraceId>,
    /// Records whose event name starts with this (`rpc.`, `session.`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ev_prefix: Option<String>,
}

/// Most trace ids one `diag.tail` subscription filters on.
pub const DIAG_TAIL_TRACES: usize = 64;

/// Payload of the `diag.tail` topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiagTailEvent {
    /// What the process's ring holds that matches (oldest first; the newest that fit a bounded
    /// size): the first event of a fresh subscription, or after an unknown or expired cursor.
    Reset { records: Vec<String> },
    /// One record, as the NDJSON line the file gets (without its newline).
    Record { line: String },
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
    /// The host's agent command for this driver, when it has one (what was looked up).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// The agent command was found but does not answer like this CLI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
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
    /// How many items the lists above leave out: more than their maximum
    /// ([`wasit_core::limits`]), or than fit in an inventory's size budget
    /// ([`crate::discovery::ENTRY_BYTES`]). `None` = every list is whole. Text that was cut
    /// ends with [`wasit_core::bounds::CUT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_listed: Option<wasit_core::NotListed>,
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

    /// The state of the earlier history is wire-stable, and a snapshot of a build without it
    /// reads as "nothing earlier".
    #[test]
    fn the_earlier_history_has_its_wire_shape() {
        let e = EarlierHistory {
            state: EarlierState::Unavailable {
                reason: EarlierUnavailable::FormatNotUnderstood { record: 4 },
            },
            source: Some(EarlierSource::AgentStorage),
            items: 0,
            gaps: EarlierGaps::default(),
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(
            v["state"],
            serde_json::json!({"kind": "unavailable",
                               "reason": {"kind": "format_not_understood", "record": 4}})
        );
        assert_eq!(v["source"], "agent_storage");
        assert_eq!(serde_json::from_value::<EarlierHistory>(v).unwrap(), e);
        let loaded = serde_json::to_value(EarlierState::Loaded { complete: false }).unwrap();
        assert_eq!(
            loaded,
            serde_json::json!({"kind": "loaded", "complete": false})
        );
        assert_eq!(
            serde_json::from_value::<EarlierHistory>(
                serde_json::json!({"state": {"kind": "none"}})
            )
            .unwrap(),
            EarlierHistory::default()
        );
        // A client that does not know `earlier` asks for the live events; one that does says so.
        let live: HistoryRequest =
            serde_json::from_value(serde_json::json!({"id": "aaaaaaaaaaaa", "limit": 5})).unwrap();
        assert!(!live.earlier);
        assert!(serde_json::to_value(&live)
            .unwrap()
            .get("earlier")
            .is_none());
        let asked: HistoryRequest = serde_json::from_value(
            serde_json::json!({"id": "aaaaaaaaaaaa", "limit": 5, "earlier": true}),
        )
        .unwrap();
        assert!(asked.earlier);
    }

    /// A scope looks inside an earlier-history item for what it holds.
    #[test]
    fn scopes_filter_earlier_items_by_what_they_hold() {
        let scope = wasit_core::Scope {
            call_id: "call-1".into(),
            agent_id: None,
        };
        let sub = SessionEvent {
            cursor: 1,
            t: 0,
            kind: SessionEventKind::Agent {
                event: AgentEvent::AssistantDelta {
                    text: "x".into(),
                    message_id: None,
                }
                .scoped(Some(scope))
                .earlier(None),
            },
        };
        let main = SessionEvent {
            cursor: 2,
            t: 0,
            kind: SessionEventKind::Agent {
                event: AgentEvent::UserMessage { text: "y".into() }.earlier(None),
            },
        };
        let of = |c: &str| HistoryScope::Subagent { call_id: c.into() };
        assert!(HistoryScope::All.admits(&sub) && HistoryScope::All.admits(&main));
        assert!(!HistoryScope::Main.admits(&sub) && HistoryScope::Main.admits(&main));
        assert!(of("call-1").admits(&sub) && !of("call-2").admits(&sub));
        assert!(!of("call-1").admits(&main));
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
