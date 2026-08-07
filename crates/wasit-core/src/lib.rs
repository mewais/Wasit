//! wasit-core — the frozen, single-source-of-truth contract for CLI agent drivers.
//!
//! Wasit is the common driver library for claude / codex / opencode. It owns:
//!   - The rich `AgentEvent` model: every event any of the three CLIs can emit,
//!     serialized with snake_case + internally-tagged serde so the wire shape is
//!     pinned and forward-compatible.
//!   - Supporting types: `SpawnSpec`, `GateRequest`, `GateDecision`, `Usage`,
//!     `PlanEntry`, `SessionMode`, `SessionInfo`, etc.
//!   - The `AgentDriver` trait and `Session` trait: the object-safe seam all
//!     three concrete drivers (`wasit-claude`, `wasit-codex`, `wasit-opencode`)
//!     implement.
//!
//! This crate is DRIVERS-ONLY: no session lifecycle, no A2A routing, no UI.
//! Bodies are `todo!()`. This file is the FROZEN CONTRACT; all bodies are stubs.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// 1. SpawnSpec — the union of Marsad LaunchSpec + Wasl SpawnContext
// ============================================================================

/// How to resume or start a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResumeMode {
    /// Brand-new conversation (the default).
    #[default]
    Fresh,
    /// Resume the most-recent conversation in `cwd` (`--continue` in claude).
    Continue,
    /// Resume a specific prior conversation by its native id (`--resume <id>`).
    Resume(String),
}

/// The complete description of "how to spawn one agent session".
///
/// This is the union of Marsad's `LaunchSpec` and Wasl's `SpawnContext`.
/// Fields that are optional for some drivers default to `None` / empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct SpawnSpec {
    // ── Location ──────────────────────────────────────────────────────────
    /// Working directory for the spawned process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,

    // ── Identity ──────────────────────────────────────────────────────────
    /// The model name to use (e.g. `"claude-sonnet-4.6"`, `"o4-mini"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// The backend-native mode id (e.g. claude's `"default"` / `"bypassPermissions"`,
    /// opencode's `"plan"` / `"build"`). Empty string = use the driver default.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,

    // ── Process ───────────────────────────────────────────────────────────
    /// Override the agent binary path. `None` = use the driver's compiled-in default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bin: Option<String>,

    /// Extra environment variables merged into the spawned process environment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<(String, String)>,

    /// A shell command run BEFORE the agent process is started (e.g. `module load cuda`).
    /// `None` = no setup step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<String>,

    // ── Restrictions ──────────────────────────────────────────────────────
    /// Tool names the agent is not permitted to call. Passed as
    /// `--disallowedTools` to claude; codex/opencode: driver-specific equivalent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disallowed_tools: Vec<String>,

    /// MCP server configuration, passed verbatim via `--mcp-config` (a JSON blob
    /// or a file path). `None` = no extra MCP servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<String>,

    // ── Conversation resumption ────────────────────────────────────────────
    /// Fresh / continue / resume-by-native-id.
    #[serde(default)]
    pub resume: ResumeMode,

    // ── First prompt ──────────────────────────────────────────────────────
    /// Optional first prompt injected right after spawn. The driver sends it
    /// immediately without waiting for another `send_prompt` call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_prompt: Option<String>,
}

// ============================================================================
// 2. Supporting value types
// ============================================================================

// ── 2.1 Usage ─────────────────────────────────────────────────────────────

/// Token and cost breakdown for one assistant message or a whole turn.
///
/// All counters are CUMULATIVE when produced from a `result` event (the
/// authoritative totals) and PER-MESSAGE when produced from an `assistant` event.
/// Per-model breakdowns are in `AgentEvent::UsageUpdate::per_model`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct Usage {
    /// Tokens in the prompt sent to the model.
    #[serde(default)]
    pub input_tokens: u64,
    /// Tokens in the model's response.
    #[serde(default)]
    pub output_tokens: u64,
    /// Prompt tokens served from the provider's cache (cache read hit).
    #[serde(default)]
    pub cache_read_tokens: u64,
    /// Prompt tokens written into the provider's cache (cache fill).
    #[serde(default)]
    pub cache_creation_tokens: u64,
    /// Running cost in USD. `0.0` when the driver does not report it (codex,
    /// opencode usage_update excepted).
    #[serde(default)]
    pub cost_usd: f64,
}

/// Per-model usage row from claude's `result.modelUsage` (absent for other CLIs).
/// Each row contains the same fields as `Usage` but for one specific model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cost_usd: f64,
    /// `true` when this row's cost was estimated from a local pricing table
    /// rather than reported directly by the CLI (mirrors Marsad's ModelUsage).
    #[serde(default)]
    pub estimated: bool,
}

// ── 2.2 SessionMode ───────────────────────────────────────────────────────

/// One selectable agent mode, exactly as the agent reports it. NEVER a Wasit
/// fabrication — the agent is always the source of truth.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SessionMode {
    /// The wire id used to select the mode (e.g. `"plan"`, `"acceptEdits"`).
    pub id: String,
    /// Human label for display (e.g. `"Plan mode"`). May equal `id`.
    pub label: String,
    /// One-line description of what the mode does. May be empty.
    #[serde(default)]
    pub description: String,
}

// ── 2.3 McpServerInfo / SlashCommandInfo / ToolInfo ───────────────────────

/// One MCP server announced in `SessionInit`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct McpServerInfo {
    pub name: String,
    #[serde(default)]
    pub status: String,
}

/// One slash command announced in `SessionInit` or `SlashCommandsUpdated`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct SlashCommandInfo {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub argument_hint: String,
}

// ── 2.4 PlanEntry ─────────────────────────────────────────────────────────

/// One entry in an opencode plan (plan-mode todo list item).
/// Not produced by claude or codex.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct PlanEntry {
    pub content: String,
    #[serde(default)]
    pub priority: String,
}

// ── 2.5 CompactionInfo ────────────────────────────────────────────────────

/// Metadata from a context-compaction event (claude `system/compact_boundary`).
/// Not produced by codex or opencode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct CompactionInfo {
    /// What triggered the compaction (e.g. `"manual"` for an explicit `/compact`).
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

// ── 2.6 RateLimitInfo ─────────────────────────────────────────────────────

/// Rate-limit status (claude `rate_limit_event`).
/// Not produced by codex or opencode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct RateLimitInfo {
    /// Verbatim rate-limit status string (e.g. `"allowed"`).
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

// ── 2.7 Subagent ──────────────────────────────────────────────────────────

/// Status of a nested subagent task (claude `Task`/`Agent` tool_use, or opencode
/// `task` tool_call).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubagentStatus {
    Running,
    Done,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SubagentInfo {
    /// Subagent display name (from `input.description` or `input.subagent_type`).
    pub name: String,
    /// The driver-native tool_use id (claude) or toolCallId (opencode) that
    /// uniquely identifies this subagent invocation.
    pub call_id: String,
    /// Model override, if the subagent carries one. `None` = inherits session model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub status: SubagentStatus,
}

// ============================================================================
// 3. Gate types — GateRequest / GateDecision
// ============================================================================

// ── 3.1 GateRequest ───────────────────────────────────────────────────────

/// The scope-of-allowance choice from an MCP elicitation's `requested_schema`
/// (e.g. hares' `grant`: `once` / `session` / `deny`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ElicitationChoice {
    /// The JSON schema property key this choice belongs under.
    pub key: String,
    /// The enum value sent back on accept.
    pub value: String,
    /// Human-facing label (from `enumNames`, else equals `value`).
    pub label: String,
}

/// A permission gate or elicitation request: the agent is asking a human to
/// approve or deny an action before proceeding.
///
/// Internally tagged so consumers can switch on `gate_type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "gate_type", rename_all = "snake_case")]
pub enum GateRequest {
    /// A tool-permission gate (`can_use_tool` in claude's control protocol).
    /// The agent wants to execute a tool and is asking for approval.
    Permission {
        /// Unique id for this gate instance (the `request_id` from the
        /// control protocol). Used to route the response back.
        request_id: String,
        /// The tool name (e.g. `"Bash"`, `"Read"`, `"edit"` for opencode).
        tool_name: String,
        /// Human-readable one-line summary of what the tool will do
        /// (e.g. the shell command, or `"Read /path/to/file"`).
        command: String,
        /// Working directory from which the tool would run.
        cwd: String,
        /// Longer explanation of the action (from claude's `input.description`,
        /// or the MCP elicitation body).
        #[serde(default)]
        explanation: String,
        /// `true` when this came from an MCP `elicitation` (answered with
        /// `{action: accept|decline}`) rather than a claude `can_use_tool`
        /// (answered with `{behavior, updatedInput}`). Drives the response
        /// encoding path in the driver.
        #[serde(default)]
        is_elicitation: bool,
        /// Scope-of-allowance choices from an elicitation's `requested_schema`
        /// enum property. Empty for `can_use_tool` gates and schemaless elicitations.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        elicitation_choices: Vec<ElicitationChoice>,
        /// Claude's own "always allow …" suggestions for a `can_use_tool` gate,
        /// carried VERBATIM (each is an `addRules` object). Empty for elicitations
        /// and when claude offers none.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        permission_suggestions: Vec<serde_json::Value>,
        /// Raw tool input JSON (the `input` from the `can_use_tool` request).
        /// Echoed back unchanged in the response's `updatedInput`.
        #[serde(default)]
        raw_input: serde_json::Value,
    },

    /// An `ExitPlanMode` gate: claude is proposing a plan and waiting for
    /// approval before acting. Only claude emits this.
    Plan {
        request_id: String,
        /// The plan markdown text.
        summary: String,
    },

    /// An `AskUserQuestion` gate: the agent has questions for the user
    /// (multiple-choice or free-form). Only claude emits this.
    Question {
        request_id: String,
        questions: Vec<QuestionPrompt>,
    },
}

/// One question inside an `AskUserQuestion` gate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// One selectable option in a `QuestionPrompt`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QuestionOption {
    pub label: String,
    #[serde(default)]
    pub description: String,
}

// ── 3.2 GateDecision ──────────────────────────────────────────────────────

/// The human's (or policy's) answer to a `GateRequest`. Handed back to the
/// session via `Session::answer_gate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum GateDecision {
    /// Approve the action.
    Approve {
        /// For a `can_use_tool` gate: the (possibly edited) tool input echoed
        /// back as `updatedInput`. If `None`, the original `raw_input` is echoed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        updated_input: Option<serde_json::Value>,
        /// For a `can_use_tool` gate where the user chose an "always allow …"
        /// suggestion: the chosen suggestion object(s), echoed back VERBATIM as
        /// `updatedPermissions`. `None` for a plain one-time approval.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        permission_updates: Option<serde_json::Value>,
        /// For an MCP elicitation with a schema-defined choice: the `content`
        /// object to send back (e.g. `{"grant":"session"}`). `None` for a
        /// schemaless elicitation accept.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elicitation_content: Option<serde_json::Value>,
    },
    /// Deny the action.
    Deny {
        /// Optional reason surfaced to the agent as the denial message.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

/// The human's answer to an `AskUserQuestion` gate.
///
/// `answers` is a map from question text → selected label(s). An empty map
/// signals "skip" (the tool returns "did not answer").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QuestionAnswer {
    /// Maps question text → selected label string(s) (joined by `,` for multi-select).
    pub answers: HashMap<String, String>,
}

// ============================================================================
// 4. Turn end reasons
// ============================================================================

/// Why a turn ended, when notable. Distinguishes a clean user cancel from a
/// genuine execution failure or a max-turns stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    /// The user interrupted the turn. Not a failure — maps to a calm Done.
    Cancelled,
    /// The agent hit its max-turns limit (claude `error_max_turns`).
    MaxTurns,
    /// The agent reported an execution error.
    ExecutionError,
    /// The agent was interrupted because a context compaction is needed. Only
    /// opencode (or future CLIs) emit this.
    CompactionNeeded,
}

// ============================================================================
// 5. AgentEvent — the rich common event model
// ============================================================================

/// The complete, per-CLI-lossless event vocabulary that all Wasit drivers emit.
///
/// Serialization: `#[serde(tag = "type", rename_all = "snake_case")]` so every
/// event is a JSON object with a `"type"` discriminant.
///
/// Consumers that only need a subset (Wasl's lean `ProtocolEvent`) down-map by
/// matching the variants they care about and ignoring the rest; Marsad's reducer
/// maps the full set to `SessionState`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    // ── Session lifecycle ────────────────────────────────────────────────

    /// The session process is up and the agent has identified itself. This is the
    /// first event a consumer should wait for before sending prompts.
    ///
    /// **Produced by:** claude (from `system/init`), codex (from
    /// `thread/started`), opencode (synthesized from `initialize` +
    /// `session/new`/`session/load` results).
    SessionInit {
        /// The driver-native session id (claude UUID, codex thread-id,
        /// opencode sessionId). `None` when the CLI does not report one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_session_id: Option<String>,
        /// The model in use, as reported by the CLI (never normalized). `None`
        /// when the CLI does not report it in the init event (opencode: model
        /// comes from `configOptions`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        /// The working directory of the agent process.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        /// Tools advertised in the init (claude `system/init.tools`).
        /// Empty for codex and opencode (they do not announce tool lists).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tools: Vec<String>,
        /// MCP servers connected (claude `system/init.mcp_servers`).
        /// Empty for codex and opencode.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        mcp_servers: Vec<McpServerInfo>,
        /// Slash commands available (claude `system/init.slash_commands`).
        /// Empty for codex; opencode emits them via `SlashCommandsUpdated`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        slash_commands: Vec<SlashCommandInfo>,
        /// The current permission mode id (claude: `"default"` | `"acceptEdits"`
        /// | `"bypassPermissions"` | `"plan"`). Empty for codex and opencode
        /// (their mode is a separate concept, see `ModesUpdated`).
        #[serde(default, skip_serializing_if = "String::is_empty")]
        permission_mode: String,
        /// The claude CLI version string (from `claude_code_version`). Empty for
        /// codex and opencode. Used to derive the available mode list since claude
        /// does not advertise it — the driver populates `available_modes` from
        /// this version; consumers should use `available_modes` directly.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        claude_code_version: String,
        /// JUDGMENT CALL: the available modes for this session, populated by the
        /// driver from the init event. For claude this is version-derived; for
        /// opencode it comes from `configOptions.mode.options`; for codex it is
        /// empty (codex does not report modes at init — a separate `ModesUpdated`
        /// may arrive later if at all).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        available_modes: Vec<SessionMode>,
        /// The current mode id at init (from claude's `permissionMode`, or opencode's
        /// `configOptions.mode.currentValue`). Empty for codex.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        current_mode: String,
    },

    // ── Turn lifecycle ───────────────────────────────────────────────────

    /// The agent accepted the prompt and started working on a new turn.
    ///
    /// **Produced by:** claude (implicit: first `assistant` after a user prompt);
    /// codex (from `turn/started` notification); opencode (implicit: first
    /// `session/update` after a `session/prompt` call).
    ///
    /// JUDGMENT CALL: claude does not emit an explicit "turn started" signal.
    /// The driver synthesizes this from the first `assistant` message after a
    /// user input. This means `TurnStarted` arrives AFTER the first
    /// `AssistantDelta` for claude, unlike for codex/opencode where it arrives
    /// before. Consumers that need strict ordering should wait for the first
    /// `AssistantDelta` anyway.
    TurnStarted {
        /// The driver-native turn id, if any.
        /// claude: none (no per-turn id in the wire protocol).
        /// codex: the `turn.id` from `turn/started`.
        /// opencode: none (turns are identified by the JSON-RPC request id).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_turn_id: Option<String>,
    },

    /// The turn ended — the assistant replied, an error occurred, or the user
    /// cancelled.
    ///
    /// **Produced by:** all three CLIs.
    TurnCompleted {
        /// `None` = a normal successful turn.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_reason: Option<EndReason>,
        /// Whether this result reflects a user-initiated interrupt. Consumed by
        /// the state machine to distinguish a calm Cancel from an error.
        #[serde(default)]
        was_interrupted: bool,
        /// Number of turns consumed (from claude's `result.num_turns`). `0` for
        /// codex/opencode which do not report this.
        #[serde(default)]
        num_turns: u64,
        /// Authoritative cumulative usage for the whole session up to this turn.
        /// claude reports this in `result`; opencode from `session/prompt` usage;
        /// codex does NOT report cumulative usage (all fields 0).
        usage: Usage,
        /// Per-model cumulative breakdown (from claude `result.modelUsage`).
        /// Empty for codex and opencode.
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        per_model: HashMap<String, ModelUsage>,
        /// The final reply text, if the CLI surfaces one in the result event.
        /// claude: `result.result` (present on success, absent on error).
        /// codex: the last `agentMessage.text` from the completed turn's `items`.
        /// opencode: absent (the text arrived incrementally as `AssistantDelta`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reply: Option<String>,
    },

    // ── Content stream ───────────────────────────────────────────────────

    /// An incremental chunk of the assistant's prose output.
    ///
    /// **Produced by:** all three CLIs.
    /// - claude: from each `text` content block in an `assistant` message.
    /// - codex: from `item/agentMessage/delta` notifications.
    /// - opencode: from `agent_message_chunk` updates (type=text).
    AssistantDelta {
        text: String,
        /// The driver-native message id, if available (claude: `message.id`;
        /// codex: none; opencode: `messageId`). Useful for dedup across reconnects.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },

    /// A reasoning / thinking delta (extended-thinking mode).
    ///
    /// **Produced by:** claude (from `content.type == "thinking"` blocks in
    /// `assistant` messages when extended thinking is enabled). Codex and
    /// opencode do not emit thinking blocks (opencode has `agent_thought_chunk`
    /// which is surfaced here for symmetry).
    ThinkingDelta {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },

    // ── Tool calls ───────────────────────────────────────────────────────

    /// A tool call started (the assistant invoked a tool).
    ///
    /// **Produced by:** all three CLIs.
    /// - claude: from `tool_use` content blocks in `assistant` messages.
    /// - codex: from `item/started` with `type == "commandExecution"`.
    /// - opencode: from `tool_call` updates.
    ToolCallStarted {
        /// The tool name.
        /// claude: the tool's `name` field (e.g. `"Bash"`, `"Read"`).
        /// codex: the shell command string from `item.command`.
        /// opencode: the `title` field (e.g. the file path or command), or `kind`.
        name: String,
        /// Driver-native tool_use / call id. Required for correlating `ToolCallUpdated`
        /// and `ToolCallFinished` back to this invocation.
        /// claude: `content[].id` (e.g. `"toolu_01…"`).
        /// codex: `item.id`.
        /// opencode: `toolCallId`.
        call_id: String,
        /// JSON-encoded tool input as the driver reported it. `null` when absent.
        /// claude: the `input` object from the `tool_use` block.
        /// codex: not available (the call_id is the item id, input is implicit).
        /// opencode: the `rawInput` object.
        #[serde(default)]
        input: serde_json::Value,
        /// Whether this tool_use initiates a subagent (claude `Task`/`Agent` tool,
        /// or opencode `task` tool). Consumers that track subagents check this flag.
        #[serde(default)]
        is_subagent: bool,
        /// The subagent model override, when `is_subagent` is true and the tool
        /// input carries a `model` key. `None` otherwise.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_model: Option<String>,
    },

    /// An update to an in-progress tool call (opencode `tool_call_update` with
    /// intermediate result content). Only opencode emits this; claude and codex
    /// go directly from `ToolCallStarted` to `ToolCallFinished`.
    ToolCallUpdated {
        call_id: String,
        name: String,
        /// Intermediate output content items (opencode's `content` array).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        content: Vec<serde_json::Value>,
        /// New status if the CLI reports one (e.g. `"in_progress"`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
    },

    /// A tool call finished (the tool's result was received).
    ///
    /// **Produced by:** all three CLIs.
    /// - claude: from `tool_result` content blocks in `user` messages (stateful
    ///   tracker correlates `tool_use_id` → name).
    /// - codex: from `item/completed` with `type == "commandExecution"` (stateful
    ///   tracker correlates `item.id` → command name).
    /// - opencode: from `tool_call_update` with terminal status (completed/failed).
    ToolCallFinished {
        call_id: String,
        name: String,
        /// `true` if the tool succeeded (exit code 0 or `is_error: false`).
        ok: bool,
        /// Tool result content, if the driver surfaces it.
        /// claude: from the `tool_result` block's `content`.
        /// codex: not available (exit code only).
        /// opencode: from `tool_call_update.content` items.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
    },

    // ── Gates ────────────────────────────────────────────────────────────

    /// The agent is blocked on a permission gate or question and needs a human
    /// answer before it can proceed.
    ///
    /// **Produced by:** claude (from `control_request` can_use_tool /
    /// elicitation / AskUserQuestion / ExitPlanMode); opencode (from
    /// `session/request_permission`).
    ///
    /// Codex does NOT emit gates in this model: under its default config it
    /// approves tool use silently. The codex app-server DOES define approval-
    /// request methods (`execCommandApproval`, `item/*/requestApproval`,
    /// `mcpServer/elicitation/request`) but no Wasit driver can answer them yet
    /// (the known gap documented in `wasit-codex`). Receiving such a request
    /// causes the driver to emit `TurnCompleted { end_reason: ExecutionError }`
    /// describing the unanswerable request.
    GateRequested {
        gate: GateRequest,
    },

    // ── Usage / cost ─────────────────────────────────────────────────────

    /// An incremental or periodic usage/cost update. Supplements `TurnCompleted`'s
    /// authoritative cumulative totals with intermediate signals.
    ///
    /// **Produced by:**
    /// - claude: from per-assistant-message `usage` (incremental, per-message).
    /// - opencode: from `usage_update` ACP updates (cumulative, replaces previous).
    /// - codex: NOT produced (codex does not report token usage).
    UsageUpdate {
        /// The current snapshot (may be per-message incremental or cumulative
        /// total depending on the CLI — see above).
        usage: Usage,
        /// Running context window fill, as a percentage (0–100). Only opencode
        /// reports this directly; claude consumers should derive it from
        /// `usage.input_tokens + cache_read_tokens` vs the model's window.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_pct: Option<f32>,
    },

    // ── Plan ─────────────────────────────────────────────────────────────

    /// The opencode plan-mode todo list was updated. Each event is a FULL
    /// SNAPSHOT (not a patch) — consumers should replace, not merge.
    ///
    /// **Produced by:** opencode only (from `plan` updates in `session/update`).
    /// Claude and codex do NOT emit plan updates in this model.
    PlanUpdated {
        entries: Vec<PlanEntry>,
    },

    // ── Mode / model changes ─────────────────────────────────────────────

    /// The agent's available mode list or current mode changed.
    ///
    /// **Produced by:**
    /// - claude: a `system/status` event carrying `permissionMode`; also
    ///   synthesized at init from the version-derived table.
    /// - opencode: from `current_mode_update` ACP updates and from
    ///   `session/set_config_option` results.
    /// - codex: NOT produced (codex does not report modes in the protocol).
    ModesUpdated {
        /// New available mode list. Empty = "leave the list unchanged, only
        /// `current_mode` changed" (mirrors Marsad's `ModesUpdate.available_modes`).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        available_modes: Vec<SessionMode>,
        /// The current mode id after the change. `None` = unchanged (only the
        /// list changed — uncommon but structurally allowed).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_mode: Option<String>,
    },

    /// The session's active model changed mid-session (via `set_model` or an
    /// agent-internal switch).
    ///
    /// **Produced by:** opencode (from `session/set_config_option` result for
    /// `model`). Claude reports model only at init and does not signal
    /// mid-session changes this way. Codex: not produced.
    ModelChanged {
        model: String,
    },

    /// The available slash commands changed (opencode mid-session catalog
    /// replacement, or claude's init list).
    ///
    /// **Produced by:** opencode (from `available_commands_update`); also emitted
    /// at init for claude's slash_commands list.
    SlashCommandsUpdated {
        commands: Vec<SlashCommandInfo>,
    },

    // ── Context compaction ────────────────────────────────────────────────

    /// A context compaction event (the context was trimmed to make room).
    ///
    /// **Produced by:** claude only (from `system/compact_boundary`). Opencode's
    /// `context_compaction` update type is mapped here too.
    /// Codex does NOT emit compaction events.
    Compaction {
        info: CompactionInfo,
    },

    // ── Rate limiting ─────────────────────────────────────────────────────

    /// A rate-limit status notice from the provider.
    ///
    /// **Produced by:** claude only (from the top-level `rate_limit_event` line).
    /// Codex and opencode do NOT emit rate-limit events in this model.
    RateLimitNotice {
        info: RateLimitInfo,
    },

    // ── Process / transport ───────────────────────────────────────────────

    /// The underlying process exited. This is a terminal event for the session.
    ///
    /// **Produced by:** all three CLIs (when the child process terminates or the
    /// transport detects EOF / a kill signal).
    ProcessExited {
        /// The process exit code, if known. `None` when the process was killed or
        /// the transport could not determine the code.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
    },

    /// An unrecognized or forward-compat event line from the CLI. Preserved so
    /// consumers can log it for observability without crashing. The driver does
    /// NOT emit this for silently-skipped events (those produce nothing); only
    /// for lines that parsed as a recognized outer type but carried an unknown
    /// subtype.
    ///
    /// JUDGMENT CALL: including this variant means consumers get full forward
    /// compatibility for free (new CLI subtypes don't crash anything). The
    /// alternative — silently dropping — loses observability. The chosen
    /// approach matches Marsad's `StreamMsg::Unknown`.
    Unknown {
        raw: serde_json::Value,
    },
}

// ============================================================================
// 6. DriverCapabilities
// ============================================================================

/// What a concrete driver can honestly do. All fields are independent booleans —
/// a `false` is honest, a rounded-up `true` is a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DriverCapabilities {
    /// Supports mid-turn interruption (`Session::interrupt`).
    pub interruptible: bool,
    /// Supports live model change without restart (`Session::set_model`).
    pub model_switchable: bool,
    /// Supports live mode change without restart (`Session::set_mode`).
    pub mode_switchable: bool,
    /// Context persists across turns (the CLI maintains its own history, no
    /// need for the consumer to re-send prior messages).
    pub persistent_conversation: bool,
    /// The session can be resumed by native id after a process restart.
    pub resumable: bool,
    /// Assistant output is streamed as incremental deltas (not delivered as
    /// a single final message). When `false`, a single `AssistantDelta` event
    /// carrying the full reply arrives at turn end.
    pub streaming_output: bool,
    /// The CLI emits per-turn token/cost usage data.
    pub reports_usage: bool,
    /// The CLI emits per-turn or per-message gate requests (permission /
    /// elicitation / question) that can be answered via `Session::answer_gate`.
    pub supports_gates: bool,
}

// ============================================================================
// 7. AgentDriver and Session traits
// ============================================================================

/// Error type for driver operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DriverError {
    #[error("spawn failed: {detail}")]
    SpawnFailed { detail: String },

    #[error("transport error: {detail}")]
    Transport { detail: String },

    #[error("transport error (outcome unknown — partial write may have occurred): {detail}")]
    TransportOutcomeUnknown { detail: String },

    #[error("operation not supported by this driver: {operation}")]
    Unsupported { operation: String },

    #[error("session not found: {detail}")]
    NotFound { detail: String },

    #[error("driver has shut down")]
    Shutdown,
}

/// A factory that can spawn or resume sessions for one CLI backend.
///
/// `async_trait`-flavored — consume it as `Arc<dyn AgentDriver>` or via a concrete
/// struct. Implemented for real by the per-CLI driver crates (wasit-claude,
/// wasit-codex, wasit-opencode).
#[async_trait::async_trait]
pub trait AgentDriver: Send + Sync {
    /// The fixed capability report for this driver. Constant for the driver's lifetime.
    fn capabilities(&self) -> DriverCapabilities;

    /// Spawn a new session (or resume an existing one, per `spec.resume`).
    ///
    /// Returns a `Box<dyn Session>` that is the SOLE owner of the live transport.
    /// The caller must poll `next_event()` promptly; back-pressure is bounded by
    /// the driver's internal event channel.
    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError>;
}

/// A live connection to ONE running agent session. The caller is the SOLE holder —
/// nothing else may concurrently call `send_prompt`, `answer_gate`, or
/// `interrupt` (single-writer invariant). Object-safe.
///
/// All methods are `async`; implemented for real by the per-CLI session types.
#[async_trait::async_trait]
pub trait Session: Send {
    // ── Event stream ──────────────────────────────────────────────────────

    /// Pull the next event from the session. Returns `None` when the session has
    /// ended (the process exited; the last event will have been `ProcessExited`).
    ///
    /// Cancellation-safety: this future MAY be dropped and re-polled. Implementors
    /// MUST guarantee that no event is lost on cancellation.
    async fn next_event(&mut self) -> Option<AgentEvent>;

    // ── Input ─────────────────────────────────────────────────────────────

    /// Send a user prompt to the agent. Returns once the CLI has accepted the
    /// prompt (not once it has replied — the reply arrives via `next_event()`).
    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError>;

    // ── Gate responses ────────────────────────────────────────────────────

    /// Answer a pending `GateRequest::Permission` or `GateRequest::Plan` gate.
    ///
    /// `request_id` must match the `request_id` from the `GateRequest` variant.
    /// The decision is encoded into the CLI-native wire format and written to
    /// the session's stdin pipe.
    ///
    /// Returns `Err(Unsupported)` when `capabilities().supports_gates` is `false`.
    async fn answer_gate(
        &mut self,
        request_id: &str,
        decision: GateDecision,
    ) -> Result<(), DriverError>;

    /// Answer a pending `GateRequest::Question` gate (`AskUserQuestion` in claude).
    ///
    /// `request_id` must match the `request_id` from the `GateRequest::Question`.
    ///
    /// Returns `Err(Unsupported)` when the CLI does not support question gates
    /// (codex, opencode).
    async fn answer_question(
        &mut self,
        request_id: &str,
        answer: QuestionAnswer,
    ) -> Result<(), DriverError>;

    // ── Control ───────────────────────────────────────────────────────────

    /// Send an interrupt to cancel the active turn. Best-effort; only meaningful
    /// when `capabilities().interruptible` is `true`.
    ///
    /// Returns `Err(Unsupported)` when the driver does not support interruption.
    async fn interrupt(&mut self) -> Result<(), DriverError>;

    /// Change the active model mid-session (without restarting). Only supported
    /// when `capabilities().model_switchable` is `true`.
    ///
    /// JUDGMENT CALL: this is a separate method rather than a generic
    /// "set_config_option" because model is a first-class concept all three
    /// CLIs expose (even if only opencode supports live switching). Codex and
    /// claude implementors should return `Err(Unsupported)`.
    async fn set_model(&mut self, model: &str) -> Result<(), DriverError>;

    /// Change the active mode mid-session (without restarting). Only supported
    /// when `capabilities().mode_switchable` is `true`.
    ///
    /// claude: sends `set_permission_mode` on the control protocol.
    /// opencode: sends `session/set_config_option` with `id = "mode"`.
    /// codex: returns `Err(Unsupported)`.
    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError>;

    /// Shut down the session cleanly. The driver should send any necessary
    /// teardown signals and then drop the child process.
    ///
    /// After `shutdown()` returns, `next_event()` should drain any remaining
    /// buffered events (typically just a `ProcessExited`) and then return `None`.
    async fn shutdown(&mut self) -> Result<(), DriverError>;
}
