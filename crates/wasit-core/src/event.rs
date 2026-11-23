//! The common event model: [`AgentEvent`] plus its value types.
//!
//! Every driver translates its CLI's native wire format into these types. The
//! serde shapes are pinned (`snake_case`, internally tagged) because they are
//! persisted and sent over the control protocol.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::content::ContentPart;
use crate::protocol::InputId;

// ── Gate resolution reason ───────────────────────────────────────────────────

/// Why an external gate was resolved without an answer: a machine word chosen by whoever
/// resolved it plus an optional short human-facing detail. The detail is the resolver's text
/// about its own request; it is not diagnostics material (a trail records only the code).
///
/// Serialized as `{"code": ..., "detail": ...}`; a bare string is read as a code with no
/// detail (journals written before a gate resolution could say why carry `"withdrawn"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct GateReason {
    /// A machine word, `[A-Za-z0-9_.:-]{1,64}` (see [`GateReason::check`]).
    pub code: String,
    /// A short sentence for people (see [`GateReason::DETAIL_MAX`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl GateReason {
    /// The owner took the gate down and did not say why (also what a journal written before a
    /// resolution could carry a reason holds).
    pub const WITHDRAWN: &'static str = "withdrawn";
    /// The runtime resolved the gate because the process generation it was raised on ended.
    pub const PROCESS_ENDED: &'static str = "process_ended";
    /// The runtime resolved the gate because the session was asked to stop.
    pub const SESSION_STOPPED: &'static str = "session_stopped";
    /// The runtime resolved a gate bound to a turn ([`TurnBinding`]) because that turn was
    /// interrupted.
    pub const TURN_INTERRUPTED: &'static str = "turn_interrupted";
    /// The runtime resolved a gate bound to a turn ([`TurnBinding::Turn`]) because that turn
    /// ended without the gate being answered.
    pub const TURN_ENDED: &'static str = "turn_ended";
    /// The longest accepted code, in bytes.
    pub const CODE_MAX: usize = 64;
    /// The longest accepted detail, in characters.
    pub const DETAIL_MAX: usize = 400;

    pub fn new(code: impl Into<String>) -> Self {
        GateReason {
            code: code.into(),
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// `Err` with what is wrong when the code is not a machine word or the detail is empty
    /// or too long. A reason that fails the check is refused, never repaired.
    pub fn check(&self) -> Result<(), String> {
        let word = !self.code.is_empty()
            && self.code.len() <= Self::CODE_MAX
            && self
                .code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'));
        if !word {
            return Err(format!(
                "a gate resolution code is 1-{} characters of [A-Za-z0-9_.:-]",
                Self::CODE_MAX
            ));
        }
        if let Some(d) = &self.detail {
            if d.is_empty() || d.chars().count() > Self::DETAIL_MAX {
                return Err(format!(
                    "a gate resolution detail is 1-{} characters when present",
                    Self::DETAIL_MAX
                ));
            }
        }
        Ok(())
    }
}

/// How long an external gate lives relative to the turn that was open when it was raised (the
/// turn the request belongs to). Without a binding a gate lives until it is answered, resolved
/// by its owner, or the process ends. With one, the session's reducer resolves it itself, in
/// the very step that interrupts or ends the turn, with a runtime-owned reason
/// ([`GateReason::TURN_INTERRUPTED`], [`GateReason::TURN_ENDED`]) and the `detail` the raiser
/// supplied here for it. Details are the raiser's own wording and obey [`GateReason::check`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ends", rename_all = "snake_case")]
pub enum TurnBinding {
    /// The gate ends when the turn is interrupted; a turn that ends normally leaves it pending
    /// (it then lives on as a session's gate).
    Interrupt {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    /// The gate ends when the turn is interrupted (`interrupted` detail) or ends without an
    /// answer (`ended` detail).
    Turn {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interrupted: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ended: Option<String>,
    },
}

impl TurnBinding {
    /// The reason the gate is resolved with when the turn is interrupted.
    pub fn interrupted_reason(&self) -> GateReason {
        let detail = match self {
            TurnBinding::Interrupt { detail } => detail,
            TurnBinding::Turn { interrupted, .. } => interrupted,
        };
        Self::reason(GateReason::TURN_INTERRUPTED, detail)
    }

    /// The reason the gate is resolved with when the turn ends without an interrupt; `None`
    /// for a binding that survives a normal turn end.
    pub fn ended_reason(&self) -> Option<GateReason> {
        match self {
            TurnBinding::Interrupt { .. } => None,
            TurnBinding::Turn { ended, .. } => Some(Self::reason(GateReason::TURN_ENDED, ended)),
        }
    }

    fn reason(code: &str, detail: &Option<String>) -> GateReason {
        let reason = GateReason::new(code);
        match detail {
            Some(d) => reason.with_detail(d.clone()),
            None => reason,
        }
    }

    /// `Err` with what is wrong when a detail is not a well-formed [`GateReason`] detail.
    pub fn check(&self) -> Result<(), String> {
        self.interrupted_reason().check()?;
        match self.ended_reason() {
            Some(r) => r.check(),
            None => Ok(()),
        }
    }
}

impl<'de> Deserialize<'de> for GateReason {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Code(String),
            Full {
                code: String,
                #[serde(default)]
                detail: Option<String>,
            },
        }
        Ok(match Repr::deserialize(d)? {
            Repr::Code(code) => GateReason { code, detail: None },
            Repr::Full { code, detail } => GateReason { code, detail },
        })
    }
}

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

// ── Turn starts ───────────────────────────────────────────────────────────────

/// What started a turn, as far as the agent said ([`AgentEvent::TurnStarted::trigger`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum TurnTrigger {
    /// The driver cannot tell: the agent did not report what started the turn, or its protocol
    /// has no such report. [`AgentEvent::TurnStarted::input`] is `None` unless the driver
    /// knows the input from its own protocol (codex, ACP).
    #[default]
    Unreported,
    /// The turn runs for an input of ours: `input` names it.
    Input,
    /// The agent started the turn by itself, for no input of ours (claude: after a background
    /// task ended). The agent's own account of why, when it gives one, is
    /// [`AgentEvent::TurnCompleted::origin`].
    Agent,
}

/// Why a turn the agent started by itself ran, in the agent's own words (claude `result.origin`:
/// `{kind:"task-notification", producer:"session-task", runId}`). Nothing here is a preset list:
/// the values are what the agent reported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct TurnOrigin {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,
    /// The run (background task) it answers, when the agent names one: the `run_id` of the
    /// [`AgentEvent::TaskStarted`] that began it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

// ── Subagent scope and background tasks ───────────────────────────────────────

/// Which subagent an event belongs to ([`AgentEvent::Scoped`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct Scope {
    /// The tool call that spawned the subagent (claude `parent_tool_use_id`): what
    /// `session.history` filters by and what [`ToolCallStarted::is_subagent`] calls carry as
    /// their `call_id`.
    ///
    /// [`ToolCallStarted::is_subagent`]: AgentEvent::ToolCallStarted
    pub call_id: String,
    /// The subagent's own id, when the agent names it (claude `agent_id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

/// What a background task has used so far (claude `usage` of `task_progress` /
/// `task_notification`). As the agent reports it: cumulative for the task, never added to the
/// session's own usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct TaskUsage {
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub tool_uses: u64,
    #[serde(default)]
    pub duration_ms: u64,
}

/// One task the agent says it runs in the background right now
/// ([`AgentEvent::BackgroundTasksChanged`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub struct BackgroundTask {
    pub task_id: String,
    /// The agent's word for the kind of work (claude `local_bash`, `local_agent`).
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

// ── Inputs ────────────────────────────────────────────────────────────────────

/// Why an input was dropped ([`AgentEvent::InputDropped`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum InputDropReason {
    /// An interrupt cleared it (it was waiting behind the turn, or the CLI cancelled it from
    /// its own queue on our request).
    Interrupted,
    /// The session was stopped while it waited.
    Shutdown,
    /// The protocol could not encode it when its turn came (e.g. the gate it answers is gone).
    Unencodable,
    /// The CLI refused or discarded it without running it.
    Refused,
    /// The process generation ended while it was still waiting to be written. Emitted by the
    /// runtime, never by a protocol.
    ProcessEnded,
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

    /// A turn started, whoever started it. claude: the CLI's `system/init`, printed at the start
    /// of every turn; codex: `turn/started`; ACP: the `session/prompt` the protocol wrote. A
    /// [`TurnCompleted`](AgentEvent::TurnCompleted) is never reported without one.
    TurnStarted {
        /// Native turn id (codex only).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_turn_id: Option<String>,
        /// The input (prompt / slash command) the turn runs for: set when the agent said so (or
        /// the driver wrote the prompt that is the turn itself), `None` for a turn that is not
        /// one of ours or that the driver cannot attribute. Never a guess: a turn is attributed
        /// to an input only on the agent's own report.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<InputId>,
        /// What started it. [`TurnTrigger::Input`] exactly when `input` is set.
        #[serde(default)]
        trigger: TurnTrigger,
    },

    /// A command the protocol had buffered ([`Encoded::Queued`](crate::Encoded::Queued)) was
    /// encoded now; its lines are in the same step.
    InputSent { input: InputId },

    /// The CLI itself reported that it took the input in (claude: the `command_lifecycle`
    /// frame `started` for the message's uuid, or the message listed in a `result`). Never
    /// inferred from unrelated output: an input that was written but is never acknowledged
    /// stays "written".
    InputAcknowledged {
        input: InputId,
        /// The input joined a turn that was already running instead of starting its own.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        steered: bool,
    },

    /// An input will never reach the CLI (or the CLI discarded it before running it).
    InputDropped {
        input: InputId,
        reason: InputDropReason,
        /// Why, in words, when there is more to say than the reason (an encode error, the CLI's
        /// refusal).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
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
        /// Why the agent ran this turn when it started it by itself and says why (claude
        /// `result.origin`); `None` for a turn of ours and for agents that do not say.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin: Option<TurnOrigin>,
    },

    /// Assistant prose (claude: whole text blocks; codex/ACP: deltas).
    AssistantDelta {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },

    /// Non-text assistant content (an image or a file the agent shows: ACP agent message
    /// `image` / `resource` / `resource_link`, codex image views and generated images).
    AssistantContent {
        parts: Vec<ContentPart>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },

    /// Non-text content of a user message the CLI reports (history replayed on load). The
    /// user's own prompts are `input` events.
    UserContent { parts: Vec<ContentPart> },

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
        /// Images / files in the update's content (ACP tool call content blocks).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        parts: Vec<ContentPart>,
    },

    /// A tool call finished.
    ToolCallFinished {
        call_id: String,
        name: String,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
        /// Images / files the result carries (claude `tool_result` image blocks, ACP tool
        /// content), in order. Text stays in `result`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        parts: Vec<ContentPart>,
    },

    /// An event of a subagent's own conversation (claude: a line tagged with
    /// `parent_tool_use_id`). Carries only per-message events (`assistant_delta`,
    /// `assistant_content`, `thinking_delta`, `user_content`, `tool_call_*`, `usage_update`),
    /// never nested. A consumer that does not know scopes sees no subagent text in the session's
    /// own conversation, which is the safe default: the session's transcript is the events that
    /// are not `scoped`, a subagent's is the `scoped` events of its `call_id`.
    Scoped {
        scope: Scope,
        event: Box<AgentEvent>,
    },

    /// A task started (claude `system/task_started`): a subagent, a shell command, whatever the
    /// agent tracks. `background` tasks outlive the turn that started them.
    TaskStarted {
        task_id: String,
        /// The agent's word for the kind of work (claude `local_bash`, `local_agent`).
        kind: String,
        /// The tool call that spawned it, when the agent says.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        description: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subagent_type: Option<String>,
        /// It runs in the background: the turn that started it may end while it runs.
        #[serde(default)]
        background: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
    },

    /// Progress of a task (claude `system/task_progress`).
    TaskProgress {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        description: String,
        /// The tool the task ran last.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_tool: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<TaskUsage>,
    },

    /// A task's state changed (claude `system/task_updated`: its `patch`). Does not end the
    /// task: [`AgentEvent::TaskEnded`] does.
    TaskUpdated {
        task_id: String,
        /// The agent's status word (`completed`, `killed`, `failed`, …).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
    },

    /// A task ended (claude `system/task_notification`). A subagent runs until this event, not
    /// until the tool call that spawned it returned.
    TaskEnded {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        /// The agent's status word (`completed`, `failed`, `stopped`, …).
        status: String,
        /// The task succeeded (the driver's reading of `status`).
        ok: bool,
        /// The agent's one-line account of the outcome.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<TaskUsage>,
    },

    /// The agent's complete list of the tasks it runs in the background now (claude
    /// `system/background_tasks_changed`): the authority for which tasks are background work.
    BackgroundTasksChanged { tasks: Vec<BackgroundTask> },

    /// The agent is blocked on a human decision.
    GateRequested { gate: GateRequest },

    /// A pending gate stopped being pending without the human answering it
    /// (declined by the protocol on interrupt, or resolved by the CLI: no `reason`), or an
    /// external gate was taken down by its owner or ended with the process (`reason` says
    /// why; a late answer to it is refused).
    GateResolved {
        request_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<GateReason>,
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

impl AgentEvent {
    /// `TurnStarted` for a driver whose protocol names the input itself (or none): the trigger
    /// is [`TurnTrigger::Input`] when there is an input, else [`TurnTrigger::Unreported`].
    pub fn turn_started(native_turn_id: Option<String>, input: Option<InputId>) -> AgentEvent {
        AgentEvent::TurnStarted {
            native_turn_id,
            input,
            trigger: if input.is_some() {
                TurnTrigger::Input
            } else {
                TurnTrigger::Unreported
            },
        }
    }

    /// `self` attributed to `scope` (unchanged for `None`).
    pub fn scoped(self, scope: Option<Scope>) -> AgentEvent {
        match scope {
            Some(scope) => AgentEvent::Scoped {
                scope,
                event: Box::new(self),
            },
            None => self,
        }
    }

    /// The scope of the event and the event itself with the wrapper removed.
    pub fn unscoped(&self) -> (Option<&Scope>, &AgentEvent) {
        match self {
            AgentEvent::Scoped { scope, event } => (Some(scope), event),
            other => (None, other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serde shapes of what the agent says about turns, tasks and subagents are persisted
    /// and sent over the control protocol: pinned.
    #[test]
    fn turns_tasks_and_scopes_have_their_wire_shapes() {
        let started = AgentEvent::turn_started(None, Some(InputId(7)));
        assert_eq!(
            serde_json::to_value(&started).unwrap(),
            serde_json::json!({"type": "turn_started", "input": 7, "trigger": "input"})
        );
        // An event from before triggers existed reads as "unreported".
        let old: AgentEvent =
            serde_json::from_value(serde_json::json!({"type": "turn_started", "input": 7}))
                .unwrap();
        assert_eq!(
            old,
            AgentEvent::TurnStarted {
                native_turn_id: None,
                input: Some(InputId(7)),
                trigger: TurnTrigger::Unreported
            }
        );
        let agent = AgentEvent::TurnStarted {
            native_turn_id: None,
            input: None,
            trigger: TurnTrigger::Agent,
        };
        assert_eq!(
            serde_json::to_value(&agent).unwrap(),
            serde_json::json!({"type": "turn_started", "trigger": "agent"})
        );
        let scope = Scope {
            call_id: "toolu_1".into(),
            agent_id: Some("a1".into()),
        };
        let scoped = AgentEvent::AssistantDelta {
            text: "hi".into(),
            message_id: None,
        }
        .scoped(Some(scope.clone()));
        let json = serde_json::to_value(&scoped).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "scoped", "scope": {"call_id": "toolu_1", "agent_id": "a1"},
                               "event": {"type": "assistant_delta", "text": "hi"}})
        );
        assert_eq!(serde_json::from_value::<AgentEvent>(json).unwrap(), scoped);
        assert_eq!(scoped.unscoped().0, Some(&scope));
        assert_eq!(
            AgentEvent::Notice {
                message: "x".into()
            }
            .scoped(None),
            AgentEvent::Notice {
                message: "x".into()
            }
        );
        let ended = AgentEvent::TaskEnded {
            task_id: "t".into(),
            call_id: None,
            status: "completed".into(),
            ok: true,
            summary: None,
            usage: Some(TaskUsage {
                total_tokens: 1,
                tool_uses: 2,
                duration_ms: 3,
            }),
        };
        let json = serde_json::to_value(&ended).unwrap();
        assert_eq!(json["type"], "task_ended");
        assert_eq!(
            json["usage"],
            serde_json::json!({"total_tokens": 1, "tool_uses": 2, "duration_ms": 3})
        );
        assert_eq!(serde_json::from_value::<AgentEvent>(json).unwrap(), ended);
    }

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
