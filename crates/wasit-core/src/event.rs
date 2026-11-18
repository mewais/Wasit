//! The common event model: [`AgentEvent`] plus its value types.
//!
//! Every driver translates its CLI's native wire format into these types. The
//! serde shapes are pinned (`snake_case`, internally tagged) because they are
//! persisted and sent over the control protocol.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

// ── Usage ───────────────────────────────────────────────────────────────────

/// Token and cost breakdown.
///
/// In [`AgentEvent::TurnCompleted`] the counters are the CLI's cumulative
/// totals when it reports them; in [`AgentEvent::UsageUpdate`] see that
/// variant's docs for the per-driver meaning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct Usage {
    /// Prompt tokens sent to the model.
    #[serde(default)]
    pub input_tokens: u64,
    /// Tokens in the model's response.
    #[serde(default)]
    pub output_tokens: u64,
    /// Prompt tokens served from the provider cache.
    #[serde(default)]
    pub cache_read_tokens: u64,
    /// Prompt tokens written into the provider cache.
    #[serde(default)]
    pub cache_creation_tokens: u64,
    /// Cost in USD. `0.0` when the CLI does not report cost.
    #[serde(default)]
    pub cost_usd: f64,
}

/// Per-model usage row (claude `result.modelUsage`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cost_usd: f64,
    /// `true` when the cost was estimated rather than reported by the CLI.
    #[serde(default)]
    pub estimated: bool,
}

// ── Modes / models / catalog entries ──────────────────────────────────────────

/// One selectable agent mode, exactly as the CLI reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct SessionMode {
    /// Wire id used to select the mode (e.g. `"plan"`, `"acceptEdits"`).
    pub id: String,
    /// Human label (may equal `id`).
    pub label: String,
    /// One-line description (may be empty).
    #[serde(default)]
    pub description: String,
}

/// One selectable model, exactly as the CLI reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct ModelInfo {
    /// The id to pass back (`SpawnSpec::model`, `Command::SetModel`).
    pub id: String,
    /// Display name (may equal `id`).
    #[serde(default)]
    pub display_name: String,
    /// Description (may be empty).
    #[serde(default)]
    pub description: String,
    /// The CLI marks this model as its default.
    #[serde(default)]
    pub is_default: bool,
}

/// One MCP server announced by the CLI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct McpServerInfo {
    pub name: String,
    #[serde(default)]
    pub status: String,
}

/// One slash command announced by the CLI. `name` carries no leading `/`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct SlashCommandInfo {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub argument_hint: String,
}

/// One entry of an agent plan / todo list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct PlanEntry {
    pub content: String,
    #[serde(default)]
    pub priority: String,
    /// `pending` / `in_progress` / `completed` when the CLI reports it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
}

/// Context-compaction metadata (claude `system/compact_boundary`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct CompactionInfo {
    #[serde(default)]
    pub trigger: String,
    #[serde(default)]
    pub pre_tokens: u64,
    #[serde(default)]
    pub post_tokens: u64,
    #[serde(default)]
    pub cumulative_dropped_tokens: u64,
    #[serde(default)]
    pub duration_ms: u64,
}

/// Rate-limit status (claude `rate_limit_event`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct RateLimitInfo {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub resets_at: u64,
    #[serde(default)]
    pub rate_limit_type: String,
    #[serde(default)]
    pub overage_status: String,
    #[serde(default)]
    pub is_using_overage: bool,
}

// ── Gates ─────────────────────────────────────────────────────────────────────

/// One allowed value of an elicitation / permission-option choice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct ElicitationChoice {
    /// The property key this choice belongs under (`"optionId"` for ACP).
    pub key: String,
    /// The value sent back on accept.
    pub value: String,
    /// Human label.
    pub label: String,
}

/// A request for a human decision. The agent is blocked until it is answered
/// with [`Command::AnswerGate`](crate::Command::AnswerGate) or
/// [`Command::AnswerQuestion`](crate::Command::AnswerQuestion).
///
/// `request_id` is the protocol's routing id: pass it back verbatim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "gate_type", rename_all = "snake_case")]
pub enum GateRequest {
    /// The agent wants to run a tool (or an MCP server asks for input).
    Permission {
        request_id: String,
        /// Tool name (claude `Bash`, codex `shell`/`apply_patch`, ACP kind).
        tool_name: String,
        /// One-line summary of the action.
        command: String,
        /// Working directory of the action, when known.
        cwd: String,
        /// Longer explanation, when known.
        #[serde(default)]
        explanation: String,
        /// `true` for MCP elicitations (answered accept/decline) and ACP
        /// permission requests that carry options.
        #[serde(default)]
        is_elicitation: bool,
        /// Choices the human can pick from (elicitation enum / ACP options).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        elicitation_choices: Vec<ElicitationChoice>,
        /// Claude "always allow" suggestions / raw ACP options, verbatim.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        permission_suggestions: Vec<serde_json::Value>,
        /// Raw tool input as the CLI reported it.
        #[serde(default)]
        raw_input: serde_json::Value,
    },
    /// The agent proposes a plan and waits for approval (claude `ExitPlanMode`).
    Plan { request_id: String, summary: String },
    /// The agent asks the human questions (claude `AskUserQuestion`, codex
    /// `item/tool/requestUserInput`, ACP permission requests with options).
    Question {
        request_id: String,
        questions: Vec<QuestionPrompt>,
    },
    /// One agent wants to hand work to another (agent-to-agent orchestration). Raised on the
    /// *sending* session by the orchestration layer as an external gate (not by a CLI);
    /// answered with [`Command::AnswerGate`](crate::Command::AnswerGate) like any permission.
    /// By convention a non-`None` `permission_updates` in the approval means "always allow on
    /// this edge".
    Handoff {
        request_id: String,
        /// Orchestration task id.
        task_id: String,
        /// Sending agent (display form, e.g. `host/session`).
        from: String,
        /// Receiving agent (display form).
        to: String,
        /// Delegation mode (e.g. `consult`, `command`, `message`).
        mode: String,
        /// What the receiver is asked to do.
        instruction: String,
        /// Extra structured context (peer names, hop count, …).
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        details: serde_json::Value,
    },
}

impl GateRequest {
    /// The routing id of this gate.
    pub fn request_id(&self) -> &str {
        match self {
            GateRequest::Permission { request_id, .. }
            | GateRequest::Plan { request_id, .. }
            | GateRequest::Question { request_id, .. }
            | GateRequest::Handoff { request_id, .. } => request_id,
        }
    }
}

/// One question inside a [`GateRequest::Question`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct QuestionPrompt {
    pub question: String,
    #[serde(default)]
    pub header: String,
    #[serde(default)]
    pub multi_select: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<QuestionOption>,
}

/// One selectable option of a [`QuestionPrompt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct QuestionOption {
    pub label: String,
    #[serde(default)]
    pub description: String,
}

/// The human's (or an explicit policy's) answer to a permission/plan gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum GateDecision {
    /// Approve the action.
    Approve {
        /// Edited tool input (claude `updatedInput`). `None` echoes the
        /// original input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        updated_input: Option<serde_json::Value>,
        /// "Always allow" choice: claude suggestion objects echoed as
        /// `updatedPermissions`; for codex any value selects the
        /// session-scoped approval (`acceptForSession`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        permission_updates: Option<serde_json::Value>,
        /// Elicitation content (e.g. `{"grant":"session"}`); for ACP
        /// `{"optionId": "<id>"}` picks a specific permission option.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elicitation_content: Option<serde_json::Value>,
    },
    /// Deny the action.
    Deny {
        /// Reason surfaced to the agent, when the CLI supports one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

impl GateDecision {
    /// A plain one-time approval.
    pub fn approve() -> Self {
        GateDecision::Approve {
            updated_input: None,
            permission_updates: None,
            elicitation_content: None,
        }
    }

    /// A denial with an optional reason.
    pub fn deny(reason: Option<&str>) -> Self {
        GateDecision::Deny {
            reason: reason.map(str::to_string),
        }
    }
}

/// The human's answer to a [`GateRequest::Question`]: question text →
/// selected label(s) (comma-joined for multi-select). Empty = skip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct QuestionAnswer {
    pub answers: BTreeMap<String, String>,
}

// ── Turn end reasons ──────────────────────────────────────────────────────────

/// Why a turn ended, when notable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// The user interrupted the turn (not a failure).
    Cancelled,
    /// The agent hit its max-turns limit.
    MaxTurns,
    /// The agent reported an execution error.
    ExecutionError,
    /// The turn stopped because a compaction is needed.
    CompactionNeeded,
}

// ── AgentEvent ────────────────────────────────────────────────────────────────

/// Everything a driver can report. Internally tagged on `"type"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    /// The CLI has identified the session. Emitted once per handshake
    /// (claude `system/init`, codex `thread/start|resume` result, ACP
    /// `session/new|load` result).
    SessionInit {
        /// Native session id (claude UUID, codex thread id, ACP session id).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_session_id: Option<String>,
        /// Model in use, verbatim.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        /// Tools advertised (claude only).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tools: Vec<String>,
        /// MCP servers and their status (claude only).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        mcp_servers: Vec<McpServerInfo>,
        /// Slash commands (claude; ACP sends them via `SlashCommandsUpdated`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        slash_commands: Vec<SlashCommandInfo>,
        /// Modes the CLI reported at init (ACP `mode` options; claude: none,
        /// see `ModesUpdated`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        available_modes: Vec<SessionMode>,
        /// Current mode id, when the CLI reports it.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        current_mode: String,
        /// CLI version string, when the CLI reports it.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        cli_version: String,
    },

    /// A turn started. Emitted when the protocol writes the prompt (claude,
    /// ACP) or when the CLI announces it (codex `turn/started`).
    TurnStarted {
        /// Native turn id (codex only).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_turn_id: Option<String>,
    },

    /// A turn ended.
    TurnCompleted {
        /// `None` = normal success.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_reason: Option<EndReason>,
        /// The turn ended because of a user interrupt.
        #[serde(default)]
        was_interrupted: bool,
        /// Agent loop iterations (claude `num_turns`; `0` when unknown).
        #[serde(default)]
        num_turns: u64,
        /// Cumulative session usage up to this turn, when the CLI reports it
        /// (claude `result`, codex last `thread/tokenUsage/updated` total,
        /// ACP prompt-result usage).
        usage: Usage,
        /// Per-model breakdown (claude only).
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        per_model: HashMap<String, ModelUsage>,
        /// Final reply text or error message, when the CLI surfaces one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply: Option<String>,
    },

    /// Assistant prose (claude: whole text blocks; codex/ACP: deltas).
    AssistantDelta {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },

    /// Reasoning / thinking text.
    ThinkingDelta {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },

    /// A tool call started.
    ToolCallStarted {
        name: String,
        call_id: String,
        #[serde(default)]
        input: serde_json::Value,
        /// The call starts a subagent (claude `Task`/`Agent`, ACP `task`).
        #[serde(default)]
        is_subagent: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_model: Option<String>,
    },

    /// Intermediate progress of a tool call.
    ToolCallUpdated {
        call_id: String,
        name: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        content: Vec<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
    },

    /// A tool call finished.
    ToolCallFinished {
        call_id: String,
        name: String,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
    },

    /// The agent is blocked on a human decision.
    GateRequested { gate: GateRequest },

    /// A pending gate stopped being pending without the human answering it
    /// (declined by the protocol on interrupt, or resolved by the CLI).
    GateResolved { request_id: String },

    /// Usage update. claude: per assistant message; codex: cumulative thread
    /// total; ACP: cost + context window fill.
    UsageUpdate {
        usage: Usage,
        /// Context window fill in percent, when derivable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_pct: Option<f32>,
    },

    /// Plan / todo list snapshot (replace, do not merge).
    PlanUpdated { entries: Vec<PlanEntry> },

    /// Available modes and/or current mode changed.
    ModesUpdated {
        /// Empty = list unchanged.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        available_modes: Vec<SessionMode>,
        /// `None` = current mode unchanged.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_mode: Option<String>,
    },

    /// Available models reported by the CLI (claude `initialize`, codex
    /// `model/list`, ACP `model` config option).
    ModelsUpdated {
        models: Vec<ModelInfo>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current: Option<String>,
    },

    /// The active model changed.
    ModelChanged { model: String },

    /// The slash command catalog changed (full replacement).
    SlashCommandsUpdated { commands: Vec<SlashCommandInfo> },

    /// Context compaction.
    Compaction { info: CompactionInfo },

    /// Provider rate-limit status.
    RateLimitNotice { info: RateLimitInfo },

    /// The CLI rejected a command we sent (error reply to a request that is
    /// not a turn), e.g. an unknown model for `SetModel`.
    CommandRejected {
        /// Which command (`"set_model"`, `"set_mode"`, `"interrupt"`, …).
        command: String,
        message: String,
    },

    /// A non-fatal notice from the CLI (warnings, retries, stderr-free
    /// diagnostics carried on the protocol).
    Notice { message: String },

    /// The process exited. Emitted by the runtime, never by a protocol.
    ProcessExited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
    },

    /// A line the protocol did not recognise, kept for observability.
    Unknown { raw: serde_json::Value },
}
