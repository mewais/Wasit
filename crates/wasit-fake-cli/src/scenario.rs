//! Scenario types: the JSON that drives a fake CLI session, plus a small builder so tests in
//! other crates can write scenario files programmatically.
//!
//! ```
//! use wasit_fake_cli::scenario::{Scenario, Turn};
//!
//! let scenario = Scenario::builder()
//!     .session_id("sess-1")
//!     .model("fake/fast", "Fake Fast")
//!     .mode("build", "Build")
//!     .turn(
//!         Turn::when("run it")
//!             .say("on it")
//!             .tool("bash", serde_json::json!({"command": "touch x"}))
//!             .needs_permission()
//!             .result_if_allowed("done")
//!             .result_if_denied("nope")
//!             .done()
//!             .say("finished"),
//!     )
//!     .build();
//! let json = scenario.to_json();
//! assert_eq!(Scenario::from_json(&json).unwrap(), scenario);
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// A model the fake advertises.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ModelInfo {
    /// Wire id, e.g. `opencode/big-pickle` or `haiku`.
    pub id: String,
    /// Display name (defaults to the id).
    pub name: Option<String>,
    pub description: Option<String>,
}

/// A mode (ACP session mode, claude permission mode, codex collaboration mode) the fake advertises.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ModeInfo {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
}

/// A slash command / skill the fake advertises.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CommandInfo {
    pub name: String,
    pub description: String,
    pub input_hint: Option<String>,
}

/// One message of a persisted session's history, replayed on `session/load`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct HistoryMessage {
    /// `user` or `assistant`.
    pub role: String,
    pub text: String,
}

/// A session that "already exists" when the fake starts (resume / load / list).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SessionInfo {
    pub id: String,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub history: Vec<HistoryMessage>,
}

/// One option of a question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

/// One plan / todo entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PlanEntry {
    pub content: String,
    /// `pending`, `in_progress` or `completed`.
    pub status: String,
    /// `high`, `medium` or `low`.
    pub priority: String,
}

impl Default for PlanEntry {
    fn default() -> Self {
        PlanEntry {
            content: String::new(),
            status: "pending".into(),
            priority: "medium".into(),
        }
    }
}

/// A tool call step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ToolStep {
    pub name: String,
    pub input: Value,
    /// Gate the call on a permission request to the client.
    pub needs_permission: bool,
    /// Tool output text when allowed (default `ok`).
    pub result_if_allowed: Option<String>,
    /// Tool output text when denied and the client gave no reason.
    pub result_if_denied: Option<String>,
    /// Human title shown with the permission request (default derived from the input).
    pub title: Option<String>,
}

/// A question step (claude `AskUserQuestion`, ACP permission-with-options, codex `requestUserInput`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AskStep {
    pub question: String,
    pub header: Option<String>,
    pub options: Vec<QuestionOption>,
    pub multi_select: bool,
}

/// A usage report; the last `usage` step of a turn is what the turn-end message carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct UsageStep {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub thought_tokens: u64,
    pub cost_usd: f64,
    /// Context window size reported by ACP `usage_update` / codex token usage.
    pub context_window: u64,
}

impl Default for UsageStep {
    fn default() -> Self {
        UsageStep {
            input_tokens: 10,
            output_tokens: 5,
            cached_tokens: 0,
            thought_tokens: 0,
            cost_usd: 0.0,
            context_window: 200_000,
        }
    }
}

/// One step of a turn script. JSON form is `{"type": "<snake_case name>", ...fields}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Step {
    /// Assistant text, optionally streamed in `chunks` deltas.
    Say {
        text: String,
        #[serde(default)]
        chunks: Option<usize>,
    },
    /// Reasoning text.
    Think { text: String },
    /// A tool call (optionally gated on a permission request).
    Tool(ToolStep),
    /// Ask the user a question and wait for the answer.
    AskQuestion(AskStep),
    /// Plan / todo update. For claude `needs_approval` turns it into an `ExitPlanMode` gate.
    Plan {
        entries: Vec<PlanEntry>,
        #[serde(default)]
        needs_approval: bool,
    },
    /// Set the usage the turn reports at its end.
    Usage(UsageStep),
    /// Model a long-running tool: wait, but wake on interrupt.
    SleepMs { ms: u64 },
    /// Block until the client interrupts / cancels the turn.
    WaitForInterrupt,
    /// End the turn with an error.
    Fail { message: String },
    /// The process dies mid-turn with this exit code.
    Exit {
        #[serde(default)]
        code: i32,
    },
    /// Stop responding to anything (watchdog tests). Stdin is drained until EOF.
    Hang,
    /// Emit a malformed (non-JSON by default) line on stdout.
    Garbage {
        #[serde(default)]
        line: Option<String>,
    },
    /// Write text to stderr.
    Stderr { text: String },
}

/// One turn script.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Turn {
    /// Regex on the prompt text. A turn with `when` is reusable and wins over unconditional turns;
    /// unconditional turns are consumed in order.
    pub when: Option<String>,
    pub steps: Vec<Step>,
}

/// The whole scenario.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Scenario {
    /// Id of the first session the fake creates (and, for claude, the session id it reports).
    pub session_id: Option<String>,
    /// Version string reported by `--version` / `initialize`.
    pub version: Option<String>,
    pub models: Vec<ModelInfo>,
    pub current_model: Option<String>,
    pub modes: Vec<ModeInfo>,
    pub current_mode: Option<String>,
    pub commands: Vec<CommandInfo>,
    /// Sessions that exist before the fake starts (load / resume / list).
    pub sessions: Vec<SessionInfo>,
    pub turns: Vec<Turn>,
    /// Steps run once at process start, before any input is read. Only `stderr`, `garbage`,
    /// `hang` and `exit` are meaningful here (others are ignored); e.g. `hang` models a CLI that
    /// never completes its handshake.
    pub startup: Vec<Step>,
    /// Reply for prompts no turn matches is `<echo_prefix><prompt>` (default `echo: `).
    pub echo_prefix: Option<String>,
}

impl Scenario {
    pub fn builder() -> ScenarioBuilder {
        ScenarioBuilder::default()
    }

    pub fn from_json(json: &str) -> Result<Scenario, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Scenario, String> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read scenario {}: {e}", path.display()))?;
        Scenario::from_json(&text).map_err(|e| format!("parse scenario {}: {e}", path.display()))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("scenario is always serializable")
    }

    /// Write the scenario to `path` (parent directory must exist).
    pub fn write_to(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        std::fs::write(path, self.to_json())
    }
}

/// Builder for [`Scenario`].
#[derive(Debug, Default, Clone)]
pub struct ScenarioBuilder {
    scenario: Scenario,
}

impl ScenarioBuilder {
    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.scenario.session_id = Some(id.into());
        self
    }
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.scenario.version = Some(v.into());
        self
    }
    pub fn model(mut self, id: impl Into<String>, name: impl Into<String>) -> Self {
        self.scenario.models.push(ModelInfo {
            id: id.into(),
            name: Some(name.into()),
            description: None,
        });
        self
    }
    pub fn current_model(mut self, id: impl Into<String>) -> Self {
        self.scenario.current_model = Some(id.into());
        self
    }
    pub fn mode(mut self, id: impl Into<String>, name: impl Into<String>) -> Self {
        self.scenario.modes.push(ModeInfo {
            id: id.into(),
            name: Some(name.into()),
            description: None,
        });
        self
    }
    pub fn current_mode(mut self, id: impl Into<String>) -> Self {
        self.scenario.current_mode = Some(id.into());
        self
    }
    pub fn command(mut self, name: impl Into<String>, description: impl Into<String>) -> Self {
        self.scenario.commands.push(CommandInfo {
            name: name.into(),
            description: description.into(),
            input_hint: None,
        });
        self
    }
    pub fn session(mut self, session: SessionInfo) -> Self {
        self.scenario.sessions.push(session);
        self
    }
    pub fn echo_prefix(mut self, p: impl Into<String>) -> Self {
        self.scenario.echo_prefix = Some(p.into());
        self
    }
    /// Append a turn (anything convertible with `Into<Turn>`: a [`Turn`] or a [`TurnBuilder`]).
    pub fn turn(mut self, turn: impl Into<Turn>) -> Self {
        self.scenario.turns.push(turn.into());
        self
    }
    /// Append a step run at process start, before any input (see [`Scenario::startup`]).
    pub fn startup(mut self, step: Step) -> Self {
        self.scenario.startup.push(step);
        self
    }
    pub fn build(self) -> Scenario {
        self.scenario
    }
}

/// Builder for [`Turn`]; also usable directly wherever a `Turn` is accepted.
#[derive(Debug, Default, Clone)]
pub struct TurnBuilder {
    turn: Turn,
}

impl Turn {
    /// A turn matched in order.
    pub fn builder() -> TurnBuilder {
        TurnBuilder::default()
    }
    /// A turn matched by a regex on the prompt text.
    pub fn when(regex: impl Into<String>) -> TurnBuilder {
        TurnBuilder {
            turn: Turn {
                when: Some(regex.into()),
                steps: vec![],
            },
        }
    }
}

impl From<TurnBuilder> for Turn {
    fn from(b: TurnBuilder) -> Turn {
        b.turn
    }
}

impl TurnBuilder {
    pub fn step(mut self, step: Step) -> Self {
        self.turn.steps.push(step);
        self
    }
    pub fn say(self, text: impl Into<String>) -> Self {
        self.step(Step::Say {
            text: text.into(),
            chunks: None,
        })
    }
    pub fn say_chunked(self, text: impl Into<String>, chunks: usize) -> Self {
        self.step(Step::Say {
            text: text.into(),
            chunks: Some(chunks),
        })
    }
    pub fn think(self, text: impl Into<String>) -> Self {
        self.step(Step::Think { text: text.into() })
    }
    /// Start a tool step; finish it with [`ToolBuilder::done`].
    pub fn tool(self, name: impl Into<String>, input: Value) -> ToolBuilder {
        ToolBuilder {
            parent: self,
            tool: ToolStep {
                name: name.into(),
                input,
                ..ToolStep::default()
            },
        }
    }
    pub fn ask_question(self, question: impl Into<String>, options: &[(&str, &str)]) -> Self {
        self.step(Step::AskQuestion(AskStep {
            question: question.into(),
            header: None,
            options: options
                .iter()
                .map(|(l, d)| QuestionOption {
                    label: (*l).into(),
                    description: (*d).into(),
                })
                .collect(),
            multi_select: false,
        }))
    }
    pub fn plan(self, entries: &[&str]) -> Self {
        self.step(Step::Plan {
            entries: entries
                .iter()
                .map(|c| PlanEntry {
                    content: (*c).into(),
                    ..PlanEntry::default()
                })
                .collect(),
            needs_approval: false,
        })
    }
    pub fn usage(self, input_tokens: u64, output_tokens: u64) -> Self {
        self.step(Step::Usage(UsageStep {
            input_tokens,
            output_tokens,
            ..UsageStep::default()
        }))
    }
    pub fn sleep_ms(self, ms: u64) -> Self {
        self.step(Step::SleepMs { ms })
    }
    pub fn wait_for_interrupt(self) -> Self {
        self.step(Step::WaitForInterrupt)
    }
    pub fn fail(self, message: impl Into<String>) -> Self {
        self.step(Step::Fail {
            message: message.into(),
        })
    }
    pub fn exit(self, code: i32) -> Self {
        self.step(Step::Exit { code })
    }
    pub fn hang(self) -> Self {
        self.step(Step::Hang)
    }
    pub fn garbage(self, line: impl Into<String>) -> Self {
        self.step(Step::Garbage {
            line: Some(line.into()),
        })
    }
    pub fn stderr(self, text: impl Into<String>) -> Self {
        self.step(Step::Stderr { text: text.into() })
    }
}

/// Builder for a [`ToolStep`] inside a turn.
#[derive(Debug, Clone)]
pub struct ToolBuilder {
    parent: TurnBuilder,
    tool: ToolStep,
}

impl ToolBuilder {
    pub fn needs_permission(mut self) -> Self {
        self.tool.needs_permission = true;
        self
    }
    pub fn result_if_allowed(mut self, text: impl Into<String>) -> Self {
        self.tool.result_if_allowed = Some(text.into());
        self
    }
    pub fn result_if_denied(mut self, text: impl Into<String>) -> Self {
        self.tool.result_if_denied = Some(text.into());
        self
    }
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.tool.title = Some(title.into());
        self
    }
    /// Finish the tool step and return to the turn.
    pub fn done(self) -> TurnBuilder {
        self.parent.step(Step::Tool(self.tool))
    }
}

impl From<ToolBuilder> for Turn {
    fn from(b: ToolBuilder) -> Turn {
        b.done().into()
    }
}
