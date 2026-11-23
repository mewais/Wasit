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
    /// The result also carries this image (claude `tool_result` image block, ACP `image`
    /// content), like a real `Read` of an image file.
    pub image: Option<ImageStep>,
    /// When the call runs (allowed), write this file first, like a real `Write`.
    pub write_file: Option<WriteFileStep>,
}

/// An image a step shows: `path` (read when the step runs; relative to the cwd) or base64
/// `data`. `mime` defaults from the bytes (`image/png` otherwise).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ImageStep {
    pub mime: Option<String>,
    pub path: Option<String>,
    pub data: Option<String>,
}

/// A file a tool step writes (relative paths against the cwd).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct WriteFileStep {
    pub path: String,
    pub content: String,
}

impl ImageStep {
    /// `(mime, base64)` of the image; `None` when its file cannot be read.
    pub fn load(&self) -> Option<(String, String)> {
        use base64::Engine;
        let data = match (&self.data, &self.path) {
            (Some(d), _) => d.clone(),
            (None, Some(p)) => {
                base64::engine::general_purpose::STANDARD.encode(std::fs::read(p).ok()?)
            }
            (None, None) => return None,
        };
        let mime = self.mime.clone().unwrap_or_else(|| {
            if data.starts_with("/9j/") {
                "image/jpeg".into()
            } else if data.starts_with("R0lG") {
                "image/gif".into()
            } else {
                "image/png".into()
            }
        });
        Some((mime, data))
    }
}

impl ToolStep {
    /// Run the call's effects (write its file) and return the image its result carries.
    pub fn run(&self) -> Option<(String, String)> {
        if let Some(w) = &self.write_file {
            if let Some(dir) = std::path::Path::new(&w.path).parent() {
                // A missing parent shows up as the write error below.
                let _ = std::fs::create_dir_all(dir);
            }
            if let Err(e) = std::fs::write(&w.path, &w.content) {
                eprintln!("fake-cli: write {}: {e}", w.path);
            }
        }
        self.image.as_ref().and_then(ImageStep::load)
    }
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
    /// The agent shows an image in its reply (ACP `agent_message_chunk` image content; the
    /// claude persona has no such message and ignores it).
    Image(ImageStep),
    /// Call a tool of one of the MCP servers the CLI was configured with (claude persona:
    /// `--mcp-config`) and show it as a tool call. `save_as` keeps the result (parsed as JSON
    /// when it is JSON) for `{{name.path}}` placeholders in later steps' text and arguments.
    McpCall(McpCallStep),
    /// Start a task in the background (claude persona; other personas ignore it): a subagent or a
    /// shell command. The turn goes on and ends; the task runs for `after_ms`, then the agent
    /// starts a turn of its own (`then`). See [`BackgroundStep`].
    Background(BackgroundStep),
}

/// A task the agent runs in the background, as the real claude 2.1.293 does (recorded in
/// `reference/claude/background_*.jsonl`, `stop_task.jsonl`, `agent_turn_*.jsonl`): the tool call
/// that starts it returns at once (`task_started` with `is_backgrounded`), the turn ends, the
/// task's own lines stream while the session is idle (a subagent's are tagged with the spawning
/// call), and when it ends (`task_notification`) the agent starts a turn by itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BackgroundStep {
    /// `local_agent` (default) or `local_bash`.
    pub kind: String,
    pub description: String,
    /// The subagent's type (`general-purpose` by default).
    pub subagent_type: Option<String>,
    /// The command a `local_bash` task runs (shown in the spawning call).
    pub command: Option<String>,
    /// How long the task runs after it started, in milliseconds.
    pub after_ms: u64,
    /// What a subagent does while it runs: its own conversation (`say`, `think`, `tool` steps;
    /// its lines carry `parent_tool_use_id`, `agent_id`, `subagent_type`, `task_description`).
    pub steps: Vec<Step>,
    /// `completed` (default) or `failed`.
    pub status: Option<String>,
    /// The agent's one-line account of the end (default: derived from the description).
    pub summary: Option<String>,
    /// The turn the agent starts by itself when the task ended (none: it does not).
    pub then: Vec<Step>,
    /// The turn the agent starts when the task was stopped with `stop_task`: a stopped subagent
    /// makes the agent report it, a stopped shell command starts no turn (both recorded).
    pub on_stop: Vec<Step>,
}

/// See [`Step::McpCall`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct McpCallStep {
    pub server: String,
    pub tool: String,
    pub arguments: Value,
    pub save_as: Option<String>,
}

/// A prompt the CLI refuses outright instead of running it (see [`Scenario::refuse`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Refusal {
    /// Regex on the prompt text.
    pub when: String,
    /// The error the CLI answers with.
    pub message: String,
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
    /// Prompts the CLI refuses with an error instead of running them: the codex persona answers
    /// the `turn/start` or `turn/steer` that carries a matching text with a JSON-RPC error
    /// (the way it does for a thread it does not know or a turn that is not running) and
    /// runs nothing. Other personas ignore this; use a `fail` step for their turn errors.
    pub refuse: Vec<Refusal>,
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
    /// Refuse prompts whose text matches the regex `when` with `message` (see
    /// [`Scenario::refuse`]).
    pub fn refuse(mut self, when: impl Into<String>, message: impl Into<String>) -> Self {
        self.scenario.refuse.push(Refusal {
            when: when.into(),
            message: message.into(),
        });
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
    /// Call `tool` on the configured MCP server `server`; `save_as` keeps the result for
    /// `{{name.path}}` placeholders in later steps.
    pub fn mcp_call(
        self,
        server: impl Into<String>,
        tool: impl Into<String>,
        arguments: Value,
        save_as: Option<&str>,
    ) -> Self {
        self.step(Step::McpCall(McpCallStep {
            server: server.into(),
            tool: tool.into(),
            arguments,
            save_as: save_as.map(str::to_string),
        }))
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
    /// Start a background task (see [`BackgroundStep`]).
    pub fn background(self, task: BackgroundStep) -> Self {
        self.step(Step::Background(task))
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
