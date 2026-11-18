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

/// What picking a [`GateChoice`] means, independent of the CLI. Policies ("always allow on this
/// edge"), summaries and interrupts go by kind; the driver encodes the native answer by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum GateChoiceKind {
    /// Allow this one action (also: approve a plan, accept an elicitation value).
    AllowOnce,
    /// Allow and keep allowing: a rule, a session-scoped approval, a mode switch, an edge
    /// policy — whatever the CLI attaches to the choice (see its `description`).
    AllowAlways,
    /// Refuse this action; the agent continues its turn.
    RejectOnce,
    /// Refuse and keep refusing (ACP `reject_always`).
    RejectAlways,
    /// Refuse and stop the turn (codex `cancel` / `abort`, MCP elicitation `cancel`).
    Cancel,
}

impl GateChoiceKind {
    /// `true` for the allow kinds.
    pub fn allows(self) -> bool {
        matches!(
            self,
            GateChoiceKind::AllowOnce | GateChoiceKind::AllowAlways
        )
    }
}

/// Whether a [`GateChoice`] takes free text that is sent to the agent with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum ChoiceText {
    #[default]
    None,
    /// Optional text (e.g. claude's message to the agent with a denial).
    Optional,
    /// The choice cannot be sent without text.
    Required,
}

impl ChoiceText {
    fn is_none(&self) -> bool {
        matches!(self, ChoiceText::None)
    }
}

/// One answer a gate offers, exactly as the agent offers it.
///
/// `label` is the agent's own wording when its protocol carries wording (ACP option `name`);
/// otherwise the driver uses one of the documented neutral labels and sets `neutral_label`
/// (claude `can_use_tool`, codex approvals: "Allow", "Allow for this session",
/// "Allow and remember", "Deny", "Deny and stop the turn"; plans: "Approve plan",
/// "Keep planning"). Answer with [`GateDecision`] carrying `id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct GateChoice {
    /// Pass back verbatim in [`GateDecision::choice`].
    pub id: String,
    pub label: String,
    pub kind: GateChoiceKind,
    /// `label` is a neutral label chosen by the driver because the protocol has no wording.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub neutral_label: bool,
    /// What the choice does beyond its label, in the agent's terms when it says (e.g. the
    /// rules claude would remember: `Bash(npm test:*)`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Free text accepted with this choice.
    #[serde(default, skip_serializing_if = "ChoiceText::is_none")]
    pub text: ChoiceText,
}

impl GateChoice {
    /// A choice whose label is the agent's own wording.
    pub fn agent(id: impl Into<String>, label: impl Into<String>, kind: GateChoiceKind) -> Self {
        GateChoice {
            id: id.into(),
            label: label.into(),
            kind,
            neutral_label: false,
            description: String::new(),
            text: ChoiceText::None,
        }
    }

    /// A choice with a neutral label (the protocol carries no wording).
    pub fn neutral(id: impl Into<String>, label: impl Into<String>, kind: GateChoiceKind) -> Self {
        GateChoice {
            neutral_label: true,
            ..GateChoice::agent(id, label, kind)
        }
    }

    pub fn with_description(mut self, d: impl Into<String>) -> Self {
        self.description = d.into();
        self
    }

    pub fn with_text(mut self, t: ChoiceText) -> Self {
        self.text = t;
        self
    }
}

/// A request for a human decision. The agent is blocked until it is answered
/// with [`Command::AnswerGate`](crate::Command::AnswerGate) (permission, plan, handoff: pick
/// one of `choices`) or [`Command::AnswerQuestion`](crate::Command::AnswerQuestion).
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
        /// The answers the agent offers, in its order.
        choices: Vec<GateChoice>,
        /// Raw tool input as the CLI reported it.
        #[serde(default)]
        raw_input: serde_json::Value,
    },
    /// The agent proposes a plan and waits for approval (claude `ExitPlanMode`).
    Plan {
        request_id: String,
        summary: String,
        choices: Vec<GateChoice>,
    },
    /// The agent asks the human questions (claude `AskUserQuestion`, codex
    /// `item/tool/requestUserInput`).
    Question {
        request_id: String,
        questions: Vec<QuestionPrompt>,
    },
    /// One agent wants to hand work to another (agent-to-agent orchestration). Raised on the
    /// *sending* session by the orchestration layer as an external gate (not by a CLI);
    /// answered with [`Command::AnswerGate`](crate::Command::AnswerGate) like any permission,
    /// with one of the `choices` the orchestration layer offers.
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
        choices: Vec<GateChoice>,
    },
    /// An agent wants to start another agent (a worker) on demand. Raised on the *calling*
    /// session by the orchestration layer as an external gate; answered with
    /// [`Command::AnswerGate`](crate::Command::AnswerGate) with one of `choices`. Every field
    /// is in display form (names, not ids); ids are in `details`.
    Spawn {
        request_id: String,
        /// What would be started (boxed: the variant stays small).
        #[serde(flatten)]
        spawn: Box<SpawnGate>,
        choices: Vec<GateChoice>,
    },
}

/// The request of a [`GateRequest::Spawn`]. Every field is in display form (names, not ids);
/// ids are in `details`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SpawnGate {
    /// The agent asking (display name).
    pub from: String,
    /// Name of the new agent.
    pub name: String,
    /// What the new agent is for, in the asking agent's words.
    pub role: String,
    /// Where it would run (host display name).
    pub host: String,
    /// Which agent CLI (display name).
    pub driver: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Working directory on that host.
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The first task it would be given, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction: Option<String>,
    /// The workstream it joins (display name).
    pub workstream: String,
    /// The asking session is in no workstream: approving creates `workstream`.
    #[serde(default)]
    pub new_workstream: bool,
    /// Extra structured context (ids, limits, …).
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub details: serde_json::Value,
}

impl GateRequest {
    /// The routing id of this gate.
    pub fn request_id(&self) -> &str {
        match self {
            GateRequest::Permission { request_id, .. }
            | GateRequest::Plan { request_id, .. }
            | GateRequest::Question { request_id, .. }
            | GateRequest::Handoff { request_id, .. }
            | GateRequest::Spawn { request_id, .. } => request_id,
        }
    }

    /// The choices of a permission / plan / handoff / spawn gate (empty for questions).
    pub fn choices(&self) -> &[GateChoice] {
        match self {
            GateRequest::Permission { choices, .. }
            | GateRequest::Plan { choices, .. }
            | GateRequest::Handoff { choices, .. }
            | GateRequest::Spawn { choices, .. } => choices,
            GateRequest::Question { .. } => &[],
        }
    }

    /// The choice with this id.
    pub fn choice(&self, id: &str) -> Option<&GateChoice> {
        self.choices().iter().find(|c| c.id == id)
    }

    /// The first choice of one of `kinds` (tried in order).
    pub fn choice_of_kind(&self, kinds: &[GateChoiceKind]) -> Option<&GateChoice> {
        kinds
            .iter()
            .find_map(|k| self.choices().iter().find(|c| c.kind == *k))
    }

    /// Check that `decision` names one of this gate's choices and carries text when the choice
    /// requires it.
    pub fn check(&self, decision: &GateDecision) -> Result<&GateChoice, String> {
        let c = self.choice(&decision.choice).ok_or_else(|| {
            format!(
                "gate {} offers no choice {:?} (offers: {})",
                self.request_id(),
                decision.choice,
                self.choices()
                    .iter()
                    .map(|c| c.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        let empty = decision.text.as_deref().is_none_or(|t| t.trim().is_empty());
        match c.text {
            ChoiceText::Required if empty => Err(format!("choice {:?} needs text", c.id)),
            _ => Ok(c),
        }
    }
}

/// One question inside a [`GateRequest::Question`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct QuestionPrompt {
    /// Key of this question in [`QuestionAnswer::answers`] (codex question id; claude: the
    /// question text, which is how claude keys its answers).
    pub id: String,
    pub question: String,
    #[serde(default)]
    pub header: String,
    #[serde(default)]
    pub multi_select: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<QuestionOption>,
    /// The agent accepts a free-text answer besides the options (claude always; codex
    /// `isOther`; a question without options is free text only).
    #[serde(default)]
    pub allow_other: bool,
    /// The answer is a secret (codex `isSecret`): mask it while typing, never echo it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub secret: bool,
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

/// The human's (or an explicit policy's) answer to a permission / plan / handoff gate: one of
/// the gate's [`GateChoice`]s by id, plus text when the choice takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct GateDecision {
    pub choice: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

impl GateDecision {
    /// Pick the choice `id`.
    pub fn choose(id: impl Into<String>) -> Self {
        GateDecision {
            choice: id.into(),
            text: None,
        }
    }

    /// Pick the choice `id` with text.
    pub fn choose_with(id: impl Into<String>, text: Option<&str>) -> Self {
        GateDecision {
            choice: id.into(),
            text: text.map(str::to_string).filter(|t| !t.trim().is_empty()),
        }
    }

    /// Pick the first choice of `gate` of one of `kinds`, if it offers one.
    pub fn by_kind(gate: &GateRequest, kinds: &[GateChoiceKind]) -> Option<Self> {
        gate.choice_of_kind(kinds)
            .map(|c| GateDecision::choose(&c.id))
    }
}

/// The human's answer to a [`GateRequest::Question`]: [`QuestionPrompt::id`] → the selected
/// option labels and/or free text. A missing or empty entry skips that question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct QuestionAnswer {
    pub answers: BTreeMap<String, Vec<String>>,
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
        /// The call's complete input when it arrives after the start (ACP agents announce a
        /// call first and send its arguments in an update); replaces the started input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<serde_json::Value>,
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
    /// (declined by the protocol on interrupt, or resolved by the CLI), or an external gate
    /// was taken down by its owner or ended with the process (`reason: "withdrawn"`; a late
    /// answer to it is refused).
    GateResolved {
        request_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_gate_round_trips_and_exposes_its_choices() {
        let g = GateRequest::Spawn {
            request_id: "wasl-spawn-r1".into(),
            spawn: Box::new(SpawnGate {
                from: "orchestrator".into(),
                name: "csv-worker".into(),
                role: "exports CSV files".into(),
                host: "build-box".into(),
                driver: "OpenCode".into(),
                model: Some("deepseek".into()),
                cwd: "/w".into(),
                mode: None,
                instruction: None,
                workstream: "Invoice export".into(),
                new_workstream: false,
                details: serde_json::Value::Null,
            }),
            choices: vec![GateChoice::agent(
                "allow_once",
                "Allow once",
                GateChoiceKind::AllowOnce,
            )],
        };
        let v = serde_json::to_value(&g).unwrap();
        assert_eq!(v["gate_type"], "spawn");
        assert!(v.get("mode").is_none());
        assert_eq!(v["name"], "csv-worker");
        let back: GateRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, g);
        assert_eq!(back.request_id(), "wasl-spawn-r1");
        assert!(back.choice("allow_once").is_some());
    }
}
