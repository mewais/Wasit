//! The `claude` persona: `--print --input-format stream-json --output-format stream-json`
//! with `--permission-prompt-tool stdio` (control requests on the same pipe).

use crate::engine::{Incoming, Outcome, Persona, TaskEnd, TaskIds, TurnStats};
use crate::io::Io;
use crate::reference::find_out;
use crate::scenario::{AskStep, BackgroundStep, PlanEntry, Scenario, ToolStep};
use crate::transcript::{self, encode_project_dir, Writer};
use crate::util::{alnum, chunk_text, hex, iso_now, now_ms, set, unset, uuid};
use serde_json::{json, Value};

const PERMISSION_MODES: [&str; 6] = [
    "acceptEdits",
    "auto",
    "bypassPermissions",
    "default",
    "dontAsk",
    "plan",
];

/// Options parsed from claude's argv.
#[derive(Debug, Default, Clone)]
pub struct ClaudeArgs {
    pub model: Option<String>,
    pub permission_mode: Option<String>,
    pub resume: Option<String>,
    pub session_id: Option<String>,
    /// `--continue`: the most recent conversation of the working directory.
    pub continue_latest: bool,
    /// `--fork-session` (with `--resume`): the session is a new conversation that copies the
    /// resumed one, under a new id; the resumed one is not written.
    pub fork_session: bool,
    /// `--name`: the conversation's title.
    pub name: Option<String>,
    pub partial_messages: bool,
    pub mcp_config: Option<String>,
}

impl ClaudeArgs {
    pub fn parse(args: &[String]) -> ClaudeArgs {
        let mut a = ClaudeArgs::default();
        let mut i = 0;
        while i < args.len() {
            let next = args.get(i + 1).cloned();
            match args[i].as_str() {
                "--model" => a.model = next,
                "--permission-mode" => a.permission_mode = next,
                "--resume" | "-r" => a.resume = next,
                "--session-id" => a.session_id = next,
                "--name" | "-n" => a.name = next,
                "--continue" | "-c" => {
                    a.continue_latest = true;
                    i += 1;
                    continue;
                }
                "--fork-session" => {
                    a.fork_session = true;
                    i += 1;
                    continue;
                }
                "--mcp-config" => a.mcp_config = next,
                "--include-partial-messages" => a.partial_messages = true,
                _ => {
                    i += 1;
                    continue;
                }
            }
            i += 2;
        }
        a
    }
}

/// Why the CLI refuses to start (what the real one prints, then exits 1).
#[derive(Debug, Clone)]
pub struct Refusal {
    /// The line it writes to stdout first, when it writes one.
    pub stdout: Option<Value>,
    pub stderr: String,
}

pub struct Claude {
    session_id: String,
    /// The conversation's title, announced before the first reply (`session_title_changed`).
    title: Option<String>,
    /// `<config dir>/projects/<project>`: where this working directory's transcripts live.
    project_dir: std::path::PathBuf,
    /// The conversation was started with `--name` and its title is not in a transcript yet.
    name_unsaved: Option<String>,
    /// Where this conversation's transcript is (the stored one when it is resumed, else the
    /// working directory's project), and its writer, opened on first use.
    transcript: Option<std::path::PathBuf>,
    writer: Option<Writer>,
    /// The writers of the subagents' own files, by agent id.
    subs: Vec<(String, Writer)>,
    /// The `assistant` record of the tool call being answered (a result's `sourceToolAssistantUUID`).
    tool_assistant: String,
    model: String,
    mode: String,
    cwd: String,
    version: String,
    partial: bool,
    scenario: Scenario,
    /// Pending gated tool call: (tool_use_id, request_id).
    tool_use_id: String,
    msg_id: String,
    mcp: std::collections::BTreeMap<String, crate::mcp::McpLaunch>,
    /// `uuid`s of user messages read while a turn ran, not started or cancelled yet.
    waiting: Vec<String>,
    /// `uuid` of the message the running turn is for.
    current: Option<String>,
    /// The lines printed now are a subagent's own conversation.
    scope: Option<ScopeTag>,
    /// The tool calls the subagent made so far (its `task_progress` counters).
    scope_tools: u64,
    /// The tasks the agent runs in the background, oldest first.
    running: Vec<RunningTask>,
    /// The run whose end the agent's next turn of its own answers (`origin` of its `result`).
    answers_run: Option<String>,
    /// The turn running was started by the agent itself.
    agent_turn: bool,
}

/// What a subagent's lines are tagged with.
#[derive(Debug, Clone)]
struct ScopeTag {
    call_id: String,
    agent_id: String,
    subagent_type: String,
    description: String,
}

/// A task the agent runs in the background.
#[derive(Debug, Clone)]
struct RunningTask {
    ids: TaskIds,
    kind: String,
    description: String,
    subagent_type: Option<String>,
}

fn resolve_model(m: &str) -> String {
    match m {
        "haiku" => "claude-haiku-5-5".into(),
        "sonnet" => "claude-sonnet-5-5".into(),
        "opus" => "claude-opus-5-5".into(),
        "default" => "claude-fable-5-2".into(),
        other => other.to_string(),
    }
}

fn first_where(name: &str, pred: impl Fn(&Value) -> bool) -> Value {
    find_out("claude", name, pred)
}

fn ty(m: &Value, t: &str) -> bool {
    m["type"] == t
}

/// Where the real CLI keeps its state: `CLAUDE_CONFIG_DIR`, else (the fake never touches the
/// real home) `<state>/claude-config`.
fn config_dir() -> std::path::PathBuf {
    match std::env::var_os("CLAUDE_CONFIG_DIR") {
        Some(d) => d.into(),
        None => crate::store::state_dir().join("claude-config"),
    }
}

fn is_uuid(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == 5
        && parts
            .iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(p, n)| p.len() == n && p.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// The transcript of conversation `id`, in any project (`--resume` finds a conversation from
/// whatever directory it is run in).
fn find_transcript(id: &str) -> Option<std::path::PathBuf> {
    let projects = config_dir().join("projects");
    std::fs::read_dir(projects)
        .ok()?
        .flatten()
        .map(|d| d.path().join(format!("{id}.jsonl")))
        .find(|p| p.is_file())
}

/// The newest conversation of a project directory (`--continue`).
fn latest_in(project_dir: &std::path::Path) -> Option<String> {
    std::fs::read_dir(project_dir)
        .ok()?
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "jsonl"))
        .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
        .max()
        .and_then(|(_, p)| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
}

/// The last `custom-title` of a transcript.
fn title_of(transcript: &std::path::Path) -> Option<String> {
    let text = std::fs::read_to_string(transcript).ok()?;
    text.lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["type"] == "custom-title")
        .filter_map(|v| v["customTitle"].as_str().map(str::to_string))
        .next_back()
}

impl Claude {
    /// Start as the real CLI does (verified with claude 2.1.293, see the README): `--session-id`
    /// starts a conversation under a UUID unless one is stored for it in this directory's
    /// project ("Session ID … is already in use."); `--resume` finds a stored conversation in
    /// any project and fails ("No conversation found with session ID: …") for one that was
    /// never stored; a conversation is stored from its first prompt on, not before; `--name`
    /// is the conversation's title, announced with `session_title_changed`.
    pub fn start(args: &ClaudeArgs, scenario: &Scenario) -> Result<Claude, Refusal> {
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| "/".into());
        let project_dir = config_dir().join("projects").join(encode_project_dir(&cwd));
        let mut title = args.name.clone();
        let mut resumed = args.resume.clone();
        let mut resumed_path = None;
        if resumed.is_none() && args.continue_latest && args.session_id.is_none() {
            // No conversation to continue: a fresh one, silently.
            resumed = latest_in(&project_dir);
        }
        if let Some(id) = &resumed {
            // A conversation the scenario says exists is a real transcript in the agent's
            // storage, as one a real run left.
            if let Some(info) = scenario.sessions.iter().find(|s| &s.id == id) {
                transcript::materialise_session(&config_dir(), &cwd, info);
            }
            let stored = find_transcript(id);
            let known = scenario.sessions.iter().any(|s| &s.id == id);
            if stored.is_none() && !known {
                let msg = format!("No conversation found with session ID: {id}");
                return Err(Refusal {
                    stdout: Some(json!({
                        "type":"result","subtype":"error_during_execution","duration_ms":0,
                        "duration_api_ms":0,"is_error":true,"num_turns":0,"stop_reason":null,
                        "session_id":id,"total_cost_usd":0,
                        "usage":{"input_tokens":0,"output_tokens":0,
                                 "cache_creation_input_tokens":0,"cache_read_input_tokens":0},
                        "modelUsage":{},"permission_denials":[],"uuid":uuid(),
                        "errors":[msg],"result_index":0})),
                    stderr: msg,
                });
            }
            if title.is_none() {
                title = stored.as_deref().and_then(title_of);
            }
            resumed_path = stored;
        }
        if resumed.is_none() {
            if let Some(id) = &args.session_id {
                if !is_uuid(id) {
                    return Err(Refusal {
                        stdout: None,
                        stderr: "Error: Invalid session ID. Must be a valid UUID.".into(),
                    });
                }
                if project_dir.join(format!("{id}.jsonl")).is_file() {
                    return Err(Refusal {
                        stdout: None,
                        stderr: format!("Error: Session ID {id} is already in use."),
                    });
                }
            }
        }
        // A fork is a conversation of its own: its transcript is a new file, the resumed one's
        // is never written, and its title is not stored until its first prompt.
        let fork = args.fork_session && resumed.is_some();
        let name_unsaved = (resumed.is_none() || fork)
            .then(|| args.name.clone())
            .flatten();
        let mut c = Claude::new(
            &ClaudeArgs {
                resume: resumed,
                ..args.clone()
            },
            scenario,
        );
        c.title = title;
        c.transcript = if fork { None } else { resumed_path };
        c.project_dir = project_dir;
        c.name_unsaved = name_unsaved;
        Ok(c)
    }

    fn new(args: &ClaudeArgs, scenario: &Scenario) -> Claude {
        let session_id = if args.fork_session && args.resume.is_some() {
            uuid()
        } else {
            args.resume
                .clone()
                .or_else(|| args.session_id.clone())
                .or_else(|| scenario.session_id.clone())
                .unwrap_or_else(uuid)
        };
        let model = args
            .model
            .clone()
            .or_else(|| scenario.current_model.clone())
            .or_else(|| scenario.models.first().map(|m| m.id.clone()))
            .map(|m| {
                if scenario.models.iter().any(|x| x.id == m) {
                    m
                } else {
                    resolve_model(&m)
                }
            })
            .unwrap_or_else(|| "claude-fake-1".into());
        Claude {
            session_id,
            title: None,
            project_dir: std::path::PathBuf::new(),
            name_unsaved: None,
            transcript: None,
            writer: None,
            subs: Vec::new(),
            tool_assistant: String::new(),
            model,
            mode: args
                .permission_mode
                .clone()
                .or_else(|| scenario.current_mode.clone())
                .unwrap_or_else(|| "default".into()),
            cwd: std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "/".into()),
            version: scenario
                .version
                .clone()
                .unwrap_or_else(|| "2.1.326".to_string()),
            partial: args.partial_messages,
            scenario: scenario.clone(),
            tool_use_id: String::new(),
            msg_id: String::new(),
            mcp: args
                .mcp_config
                .as_deref()
                .map(crate::mcp::servers_from_config)
                .unwrap_or_default(),
            waiting: Vec::new(),
            current: None,
            scope: None,
            scope_tools: 0,
            running: Vec::new(),
            answers_run: None,
            agent_turn: false,
        }
    }

    /// `command_lifecycle` for the user message `uuid` (shape: `reference/claude/lifecycle`,
    /// claude 2.1.293): `queued` when read, `started` when its turn begins (before
    /// `system/init`), `completed` after its turn's `result`, `cancelled` when dropped from the
    /// queue or when its turn was interrupted.
    fn lifecycle(&self, io: &mut Io, uuid: &str, state: &str) {
        let mut m = first_where("lifecycle", |m| ty(m, "command_lifecycle"));
        set(&mut m, "command_uuid", json!(uuid));
        set(&mut m, "state", json!(state));
        let m = self.envelope(m);
        io.send(&m);
    }

    /// The writer of this conversation's transcript (opened on first use: the conversation
    /// exists in storage from its first record on, not before). The conversation's title, when
    /// it was started with `--name`, is its first record.
    fn main_writer(&mut self) -> &mut Writer {
        if self.writer.is_none() {
            let path = self
                .transcript
                .clone()
                .unwrap_or_else(|| self.project_dir.join(format!("{}.jsonl", self.session_id)));
            let mut w = Writer::open(path, &self.session_id, &self.cwd);
            if let Some(name) = self.name_unsaved.take() {
                w.titles(&name);
            }
            self.writer = Some(w);
        }
        self.writer.as_mut().expect("just opened")
    }

    /// The writer of the conversation the lines printed now belong to: the running subagent's own
    /// file, else the session's transcript.
    fn rec(&mut self) -> &mut Writer {
        let agent = self.scope.as_ref().map(|s| s.agent_id.clone());
        if let Some(i) = agent.and_then(|a| self.subs.iter().position(|(id, _)| *id == a)) {
            return &mut self.subs[i].1;
        }
        self.main_writer()
    }

    /// Record a tool result the persona just printed (`m` is the stream line) in the transcript.
    fn record_tool_result(&mut self, m: &Value) {
        let block = &m["message"]["content"][0];
        let call = block["tool_use_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let content = block["content"].clone();
        let is_error = block["is_error"].as_bool().unwrap_or(false);
        let result = match &m["tool_use_result"] {
            Value::Null => content.clone(),
            r => r.clone(),
        };
        let assistant = self.tool_assistant.clone();
        let w = self.rec();
        w.tool_result(&call, content, is_error, result, &assistant);
        w.flush_logged();
    }

    /// `tasks` of a `background_tasks_changed`: the background tasks running now, in the shape of
    /// the recording `rec`.
    fn task_list(&self, rec: &str) -> Value {
        let proto = first_where(rec, |m| {
            ty(m, "system")
                && m["subtype"] == "background_tasks_changed"
                && m["tasks"].as_array().is_some_and(|t| !t.is_empty())
        })["tasks"][0]
            .clone();
        Value::Array(
            self.running
                .iter()
                .map(|t| {
                    let mut e = proto.clone();
                    set(&mut e, "task_id", json!(t.ids.task_id));
                    set(&mut e, "run_id", json!(t.ids.run_id));
                    set(&mut e, "task_type", json!(t.kind));
                    set(&mut e, "description", json!(t.description));
                    match &t.subagent_type {
                        Some(st) => set(&mut e, "subagent_type", json!(st)),
                        None => unset(&mut e, "subagent_type"),
                    }
                    e
                })
                .collect(),
        )
    }

    fn envelope(&self, mut m: Value) -> Value {
        set(&mut m, "session_id", json!(self.session_id));
        set(&mut m, "uuid", json!(uuid()));
        // A subagent's own conversation carries what the real CLI tags it with.
        if let Some(scope) = &self.scope {
            if m["type"] == "assistant" || m["type"] == "user" {
                set(&mut m, "parent_tool_use_id", json!(scope.call_id));
                set(&mut m, "agent_id", json!(scope.agent_id));
                set(&mut m, "subagent_type", json!(scope.subagent_type));
                set(&mut m, "task_description", json!(scope.description));
            }
        }
        m
    }

    fn control_ok(&self, io: &mut Io, request_id: &Value, response: Value) {
        io.send(&json!({"type":"control_response","response":{
            "subtype":"success","request_id":request_id,"response":response}}));
    }

    fn control_err(&self, io: &mut Io, request_id: &Value, error: &str, code: Option<&str>) {
        let mut r = json!({"subtype":"error","request_id":request_id,"error":error});
        if let Some(c) = code {
            r["error_code"] = json!(c);
        }
        io.send(&json!({"type":"control_response","response":r}));
    }

    fn initialize_response(&self, request_id: &Value) -> Value {
        let mut full = first_where("simple", |m| {
            ty(m, "control_response") && m["response"]["response"]["commands"].is_array()
        });
        full["response"]["request_id"] = request_id.clone();
        let mut resp = full["response"]["response"].clone();
        if !self.scenario.commands.is_empty() {
            resp["commands"] = Value::Array(
                self.scenario
                    .commands
                    .iter()
                    .map(|c| {
                        json!({"name": c.name, "description": c.description,
                               "argumentHint": c.input_hint.clone().unwrap_or_default()})
                    })
                    .collect(),
            );
        }
        if !self.scenario.models.is_empty() {
            let proto = resp["models"][0].clone();
            resp["models"] = Value::Array(
                self.scenario
                    .models
                    .iter()
                    .map(|m| {
                        let mut e = proto.clone();
                        e["value"] = json!(m.id);
                        e["resolvedModel"] = json!(m.id);
                        e["displayName"] = json!(m.name.clone().unwrap_or_else(|| m.id.clone()));
                        e["description"] = json!(m.description.clone().unwrap_or_default());
                        e
                    })
                    .collect(),
            );
        }
        full["response"]["response"] = resp;
        full
    }

    /// `system/init`: printed at the start of every turn, whoever started it (claude 2.1.293,
    /// every recording; the first one is the session's identification).
    fn send_system_init(&mut self, io: &mut Io) {
        let mut m = first_where("tool_allow", |m| ty(m, "system") && m["subtype"] == "init");
        set(&mut m, "cwd", json!(self.cwd));
        set(&mut m, "original_cwd", json!(self.cwd));
        set(&mut m, "model", json!(self.model));
        set(&mut m, "permissionMode", json!(self.mode));
        set(&mut m, "claude_code_version", json!(self.version));
        set(
            &mut m,
            "scratchpad_path",
            json!(format!("/tmp/claude-fake/{}/scratchpad", self.session_id)),
        );
        if !self.scenario.commands.is_empty() {
            let names: Vec<Value> = self
                .scenario
                .commands
                .iter()
                .map(|c| json!(c.name))
                .collect();
            m["slash_commands"] = Value::Array(names.clone());
            m["skills"] = Value::Array(names);
        }
        let m = self.envelope(m);
        io.send(&m);
    }

    fn assistant(&mut self, content: Value, stats: &TurnStats) -> Value {
        let mut m = first_where("tool_allow", |m| {
            ty(m, "assistant") && m["message"]["content"][0]["type"] == "text"
        });
        set(&mut m, "message.id", json!(self.msg_id));
        set(&mut m, "message.model", json!(self.model));
        set(&mut m, "message.content", content);
        set(
            &mut m,
            "message.usage.input_tokens",
            json!(stats.usage.input_tokens),
        );
        set(
            &mut m,
            "message.usage.output_tokens",
            json!(stats.usage.output_tokens),
        );
        set(&mut m, "timestamp", json!(iso_now()));
        set(&mut m, "request_id", json!(format!("req_{}", alnum(24))));
        self.envelope(m)
    }

    fn stream_event(&self, event: Value) -> Value {
        let mut m = first_where("partial_messages", |m| {
            ty(m, "stream_event") && m["event"]["type"] == "content_block_start"
        });
        m["event"] = event;
        set(&mut m, "api_message_id", json!(self.msg_id));
        self.envelope(m)
    }

    /// Emit one assistant message holding a single text-like block, with partial stream events
    /// when `--include-partial-messages` was passed.
    fn block(&mut self, io: &mut Io, kind: &str, text: &str, chunks: usize, stats: &TurnStats) {
        self.msg_id = format!("msg_{}", alnum(24));
        let (start_block, delta_type, key, content) = match kind {
            "thinking" => (
                json!({"type":"thinking","thinking":"","signature":""}),
                "thinking_delta",
                "thinking",
                json!([{"type":"thinking","thinking":text,"signature":format!("sig-{}", hex(16))}]),
            ),
            _ => (
                json!({"type":"text","text":""}),
                "text_delta",
                "text",
                json!([{"type":"text","text":text}]),
            ),
        };
        if self.partial {
            let mut start = first_where("partial_messages", |m| {
                ty(m, "stream_event") && m["event"]["type"] == "message_start"
            });
            set(&mut start, "event.message.id", json!(self.msg_id));
            set(&mut start, "event.message.model", json!(self.model));
            set(&mut start, "api_message_id", json!(self.msg_id));
            let start = self.envelope(start);
            io.send(&start);
            let e = self.stream_event(
                json!({"type":"content_block_start","index":0,"content_block":start_block}),
            );
            io.send(&e);
            for c in chunk_text(text, chunks) {
                let e = self.stream_event(
                    json!({"type":"content_block_delta","index":0,"delta":{"type":delta_type, key: c}}),
                );
                io.send(&e);
            }
        }
        let mut a = self.assistant(content, stats);
        if kind == "thinking" {
            set(
                &mut a,
                "thinking_duration_ms",
                json!(stats.usage.output_tokens * 10),
            );
            set(&mut a, "thinking_display", json!("updates"));
        }
        io.send(&a);
        let w = self.rec();
        if kind == "thinking" {
            w.assistant_thinking();
        } else {
            w.assistant_text(text);
        }
        w.flush_logged();
        if self.partial {
            let e = self.stream_event(json!({"type":"content_block_stop","index":0}));
            io.send(&e);
            let mut d = first_where("partial_messages", |m| {
                ty(m, "stream_event") && m["event"]["type"] == "message_delta"
            });
            set(&mut d, "api_message_id", json!(self.msg_id));
            set(
                &mut d,
                "event.usage.output_tokens",
                json!(stats.usage.output_tokens),
            );
            let d = self.envelope(d);
            io.send(&d);
            let e = self.stream_event(json!({"type":"message_stop"}));
            io.send(&e);
        }
    }

    fn tool_use_message(&mut self, io: &mut Io, name: &str, input: &Value, stats: &TurnStats) {
        self.tool_use_id = format!("toolu_{}", alnum(24));
        self.msg_id = format!("msg_{}", alnum(24));
        let mut m = first_where("tool_allow", |m| {
            ty(m, "assistant") && m["message"]["content"][0]["type"] == "tool_use"
        });
        set(&mut m, "message.id", json!(self.msg_id));
        set(&mut m, "message.model", json!(self.model));
        set(
            &mut m,
            "message.content",
            json!([{"type":"tool_use","id":self.tool_use_id,"name":name,"input":input,
                    "caller":{"type":"direct"}}]),
        );
        set(
            &mut m,
            "message.usage.output_tokens",
            json!(stats.usage.output_tokens),
        );
        set(
            &mut m,
            "wire_tool_inputs",
            json!({ self.tool_use_id.clone(): input }),
        );
        set(&mut m, "timestamp", json!(iso_now()));
        set(&mut m, "request_id", json!(format!("req_{}", alnum(24))));
        let m = self.envelope(m);
        io.send(&m);
        let call = self.tool_use_id.clone();
        let w = self.rec();
        let assistant = w.tool_use_with_id(&call, name, input);
        w.flush_logged();
        self.tool_assistant = assistant;
    }

    fn tool_result_message(
        &self,
        template: &str,
        content: Value,
        extra: Vec<(&str, Value)>,
    ) -> Value {
        let mut m = first_where(template, |m| {
            ty(m, "user") && m["message"]["content"][0]["type"] == "tool_result"
        });
        set(&mut m, "message.content", content);
        set(&mut m, "timestamp", json!(iso_now()));
        for (k, v) in extra {
            set(&mut m, k, v);
        }
        if self.scope.is_some() {
            // A subagent's tool results carry no `tool_use_result` (recorded).
            unset(&mut m, "tool_use_result");
        }
        self.envelope(m)
    }

    /// Print a tool result and record it in the transcript.
    fn send_result(&mut self, io: &mut Io, m: Value) {
        io.send(&m);
        self.record_tool_result(&m);
    }

    fn task_summary(&self, io: &mut Io, detail: &str) {
        let mut t = first_where("tool_allow", |m| {
            ty(m, "system") && m["subtype"] == "task_summary" && m["detail"].is_string()
        });
        t["detail"] = json!(detail);
        let t = self.envelope(t);
        io.send(&t);
    }

    fn describe(tool: &ToolStep) -> String {
        tool.title.clone().unwrap_or_else(|| {
            tool.input["description"]
                .as_str()
                .or_else(|| tool.input["command"].as_str())
                .unwrap_or(&tool.name)
                .to_string()
        })
    }

    fn result_message(&self, outcome: &Outcome, stats: &TurnStats) -> Value {
        let elapsed = stats
            .started
            .map(|s| s.elapsed().as_millis() as u64)
            .unwrap_or(0);
        let template = match outcome {
            Outcome::Completed => "tool_allow",
            Outcome::Interrupted => "interrupt",
            Outcome::Failed(_) => "turn_error",
        };
        let mut m = first_where(template, |m| ty(m, "result"));
        let model_usage_proto = m["modelUsage"]
            .as_object()
            .and_then(|o| o.values().next().cloned());
        match outcome {
            Outcome::Completed => {
                set(&mut m, "result", json!(stats.final_text));
                set(&mut m, "num_turns", json!(1 + stats.tool_calls));
                set(
                    &mut m,
                    "usage.input_tokens",
                    json!(stats.usage.input_tokens),
                );
                set(
                    &mut m,
                    "usage.output_tokens",
                    json!(stats.usage.output_tokens),
                );
                set(
                    &mut m,
                    "usage.cache_read_input_tokens",
                    json!(stats.usage.cached_tokens),
                );
                set(&mut m, "total_cost_usd", json!(stats.usage.cost_usd));
                set(
                    &mut m,
                    "permission_denials",
                    Value::Array(
                        stats
                            .denied
                            .iter()
                            .map(|(n, id, input)| {
                                json!({"tool_name":n,"tool_use_id":id,"tool_input":input})
                            })
                            .collect(),
                    ),
                );
            }
            Outcome::Interrupted => {}
            Outcome::Failed(msg) => {
                set(&mut m, "result", json!(msg));
            }
        }
        if let Some(mut proto) = model_usage_proto {
            if matches!(outcome, Outcome::Completed) {
                proto["inputTokens"] = json!(stats.usage.input_tokens);
                proto["outputTokens"] = json!(stats.usage.output_tokens);
                proto["costUSD"] = json!(stats.usage.cost_usd);
                m["modelUsage"] = json!({ self.model.clone(): proto });
            }
        }
        set(&mut m, "duration_ms", json!(elapsed));
        set(&mut m, "duration_api_ms", json!(elapsed));
        self.envelope(m)
    }
}

impl Persona for Claude {
    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            return Incoming::Handled;
        };
        match v["type"].as_str() {
            Some("control_request") => {
                let id = v["request_id"].clone();
                let req = &v["request"];
                match req["subtype"].as_str().unwrap_or("") {
                    "initialize" => {
                        if let Some(title) = self.title.take() {
                            // A named or resumed-with-a-title conversation announces it first.
                            let m = json!({"type":"system","subtype":"session_title_changed",
                                           "title":title});
                            let m = self.envelope(m);
                            io.send(&m);
                        }
                        for t in ["active_goal", "autocompact_state"] {
                            let m = first_where("simple", |m| ty(m, t));
                            let m = self.envelope(m);
                            io.send(&m);
                        }
                        let r = self.initialize_response(&id);
                        io.send(&r);
                        Incoming::Handled
                    }
                    "interrupt" => {
                        // Queued messages survive an interrupt (they run after it).
                        let still: Vec<Value> = self.waiting.iter().map(|u| json!(u)).collect();
                        self.control_ok(io, &id, json!({ "still_queued": still }));
                        if in_turn {
                            Incoming::Interrupt
                        } else {
                            Incoming::Handled
                        }
                    }
                    "set_permission_mode" => {
                        let mode = req["mode"].as_str().unwrap_or("");
                        if PERMISSION_MODES.contains(&mode) {
                            self.mode = mode.to_string();
                            self.control_ok(io, &id, json!({"mode": mode}));
                            let mut s = first_where("set_mode", |m| {
                                ty(m, "system") && m["subtype"] == "status"
                            });
                            set(&mut s, "permissionMode", json!(mode));
                            let s = self.envelope(s);
                            io.send(&s);
                        } else {
                            self.control_err(
                                io,
                                &id,
                                "Cannot set permission mode: must be one of acceptEdits, auto, bypassPermissions, default, dontAsk, plan",
                                Some("invalid_mode"),
                            );
                        }
                        Incoming::Handled
                    }
                    "set_model" => {
                        let m = req["model"].as_str().unwrap_or("default");
                        self.model = if self.scenario.models.iter().any(|x| x.id == m) {
                            m.to_string()
                        } else {
                            resolve_model(m)
                        };
                        let model = self.model.clone();
                        self.control_ok(io, &id, json!({"model": model}));
                        Incoming::Handled
                    }
                    "cancel_async_message" => Incoming::Cancel {
                        request: id,
                        id: req["message_uuid"].as_str().unwrap_or("").to_string(),
                    },
                    // Any id is answered with success (recorded); only a missing one is refused.
                    "stop_task" => match req["task_id"].as_str() {
                        Some(task_id) => Incoming::StopTask {
                            request: id,
                            task_id: task_id.to_string(),
                        },
                        None => {
                            self.control_err(io, &id, "stop_task: task_id must be a string", None);
                            Incoming::Handled
                        }
                    },
                    "" => {
                        self.control_err(io, &id, "Missing request subtype", None);
                        Incoming::Handled
                    }
                    _ => {
                        self.control_ok(io, &id, json!({}));
                        Incoming::Handled
                    }
                }
            }
            Some("control_response") => {
                let r = &v["response"];
                let body = if r["subtype"] == "error" {
                    json!({"behavior":"deny","message": r["error"]})
                } else {
                    r["response"].clone()
                };
                Incoming::Response {
                    id: r["request_id"].clone(),
                    body,
                }
            }
            Some("user") => {
                let c = &v["message"]["content"];
                let text = match c {
                    Value::String(s) => s.clone(),
                    Value::Array(blocks) => blocks
                        .iter()
                        .filter(|b| b["type"] == "text")
                        .filter_map(|b| b["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n"),
                    _ => String::new(),
                };
                match v["uuid"].as_str() {
                    Some(uuid) => {
                        self.lifecycle(io, uuid, "queued");
                        if in_turn {
                            self.waiting.push(uuid.to_string());
                        }
                        Incoming::TrackedPrompt {
                            text,
                            id: uuid.to_string(),
                        }
                    }
                    None => Incoming::Prompt(text),
                }
            }
            _ => Incoming::Handled,
        }
    }

    fn turn_begin(&mut self, io: &mut Io, prompt: &str, id: Option<&str>, _stats: &mut TurnStats) {
        let w = self.main_writer();
        w.begin_turn(json!([{"type":"text","text":prompt}]));
        w.flush_logged();
        self.current = id.map(str::to_string);
        if let Some(uuid) = id {
            self.waiting.retain(|w| w != uuid);
            self.lifecycle(io, uuid, "started");
        }
        self.send_system_init(io);
        if self.partial {
            let mut s = first_where("partial_messages", |m| {
                ty(m, "system") && m["subtype"] == "status"
            });
            set(&mut s, "status", json!("requesting"));
            let s = self.envelope(s);
            io.send(&s);
        }
    }

    fn say(&mut self, io: &mut Io, text: &str, chunks: usize, stats: &mut TurnStats) {
        self.block(io, "text", text, chunks, stats);
    }

    fn think(&mut self, io: &mut Io, text: &str, stats: &mut TurnStats) {
        let tokens = text.split_whitespace().count().max(1) as u64;
        let mut t = first_where("tool_deny", |m| {
            ty(m, "system") && m["subtype"] == "thinking_tokens"
        });
        set(&mut t, "estimated_tokens", json!(tokens));
        set(&mut t, "estimated_tokens_delta", json!(tokens));
        let t = self.envelope(t);
        io.send(&t);
        self.block(io, "thinking", text, 1, stats);
    }

    fn tool_call(&mut self, io: &mut Io, tool: &ToolStep, stats: &mut TurnStats) -> Option<Value> {
        self.tool_use_message(io, &tool.name, &tool.input, stats);
        if tool.needs_permission {
            let id = json!(uuid());
            let desc = Self::describe(tool);
            let mut req = first_where("tool_allow", |m| ty(m, "control_request"));
            set(&mut req, "request_id", id.clone());
            set(&mut req, "request.tool_name", json!(tool.name));
            set(&mut req, "request.display_name", json!(tool.name));
            set(&mut req, "request.input", tool.input.clone());
            set(&mut req, "request.description", json!(desc));
            set(&mut req, "request.tool_use_id", json!(self.tool_use_id));
            unset(&mut req, "request.blocked_path");
            io.send(&req);
            return Some(id);
        }
        self.task_summary(io, &Self::describe(tool));
        let content = tool
            .result_if_allowed
            .clone()
            .unwrap_or_else(|| "ok".to_string());
        let body = result_content(&content, tool.run());
        let m = self.tool_result_message(
            "tool_allow",
            json!([{"tool_use_id":self.tool_use_id,"type":"tool_result","content":body,"is_error":false}]),
            vec![
                (
                    "tool_use_result",
                    json!({"stdout":content,"stderr":"","interrupted":false,"isImage":false,"noOutputExpected":false}),
                ),
                ("tool_result_meta", json!([])),
            ],
        );
        self.send_result(io, m);
        None
    }

    fn tool_answered(&mut self, io: &mut Io, tool: &ToolStep, body: &Value, stats: &mut TurnStats) {
        let tuid = self.tool_use_id.clone();
        self.task_summary(io, &Self::describe(tool));
        if body["behavior"] == "allow" {
            let content = tool
                .result_if_allowed
                .clone()
                .unwrap_or_else(|| "ok".to_string());
            let body = result_content(&content, tool.run());
            let m = self.tool_result_message(
                "tool_allow",
                json!([{"tool_use_id":tuid,"type":"tool_result","content":body,"is_error":false}]),
                vec![
                    (
                        "tool_use_result",
                        json!({"stdout":content,"stderr":"","interrupted":false,"isImage":false,"noOutputExpected":false}),
                    ),
                    (
                        "tool_result_meta",
                        json!([{"id":tuid,"permission_decision":{"decision":"accept","source":"user_temporary"}}]),
                    ),
                ],
            );
            self.send_result(io, m);
        } else {
            let msg = body["message"]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .or_else(|| tool.result_if_denied.clone())
                .unwrap_or_else(|| "Permission denied".into());
            let m = self.tool_result_message(
                "tool_deny",
                json!([{"type":"tool_result","content":msg,"is_error":true,"tool_use_id":tuid}]),
                vec![
                    ("tool_use_result", json!(format!("Error: {msg}"))),
                    (
                        "tool_result_meta",
                        json!([{"id":tuid,"non_execution_kind":"permission-rule",
                                "permission_decision":{"decision":"reject","source":"user_reject"}}]),
                    ),
                ],
            );
            self.send_result(io, m);
            stats
                .denied
                .push((tool.name.clone(), tuid, tool.input.clone()));
        }
    }

    fn question(&mut self, io: &mut Io, ask: &AskStep, stats: &mut TurnStats) -> Value {
        let questions = json!([{
            "question": ask.question,
            "header": ask.header.clone().unwrap_or_default(),
            "options": ask.options.iter().map(|o| json!({"label":o.label,"description":o.description})).collect::<Vec<_>>(),
            "multiSelect": ask.multi_select,
        }]);
        self.tool_use_message(
            io,
            "AskUserQuestion",
            &json!({"questions": questions}),
            stats,
        );
        let id = json!(uuid());
        let mut req = first_where("ask_question", |m| ty(m, "control_request"));
        set(&mut req, "request_id", id.clone());
        set(&mut req, "request.input", json!({"questions": questions}));
        set(&mut req, "request.tool_use_id", json!(self.tool_use_id));
        io.send(&req);
        id
    }

    fn question_answered(
        &mut self,
        io: &mut Io,
        ask: &AskStep,
        body: &Value,
        _stats: &mut TurnStats,
    ) {
        self.task_summary(io, &ask.question);
        let answers = body["updatedInput"]["answers"].clone();
        let summary = answers
            .as_object()
            .map(|o| {
                o.iter()
                    .map(|(q, a)| format!("\"{q}\"=\"{}\"", a.as_str().unwrap_or("")))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let tuid = self.tool_use_id.clone();
        let m = self.tool_result_message(
            "ask_question",
            json!([{"type":"tool_result","content":format!("Your questions have been answered: {summary}. You can now continue with these answers in mind."),"tool_use_id":tuid}]),
            vec![(
                "tool_use_result",
                json!({"questions": body["updatedInput"]["questions"], "answers": answers}),
            )],
        );
        self.send_result(io, m);
    }

    fn plan(
        &mut self,
        io: &mut Io,
        entries: &[PlanEntry],
        needs_approval: bool,
        stats: &mut TurnStats,
    ) -> Option<Value> {
        if needs_approval {
            let plan = entries
                .iter()
                .map(|e| format!("- {}", e.content))
                .collect::<Vec<_>>()
                .join("\n");
            self.tool_use_message(io, "ExitPlanMode", &json!({"plan": plan}), stats);
            let id = json!(uuid());
            let mut req = first_where("ask_question", |m| ty(m, "control_request"));
            set(&mut req, "request_id", id.clone());
            set(&mut req, "request.tool_name", json!("ExitPlanMode"));
            set(&mut req, "request.display_name", json!("ExitPlanMode"));
            set(&mut req, "request.input", json!({"plan": plan}));
            set(&mut req, "request.tool_use_id", json!(self.tool_use_id));
            io.send(&req);
            return Some(id);
        }
        let todos: Vec<Value> = entries
            .iter()
            .map(|e| json!({"content":e.content,"status":e.status,"activeForm":e.content}))
            .collect();
        self.tool_use_message(io, "TodoWrite", &json!({"todos": todos}), stats);
        let tuid = self.tool_use_id.clone();
        let m = self.tool_result_message(
            "tool_allow",
            json!([{"tool_use_id":tuid,"type":"tool_result","content":"Todos have been modified successfully.","is_error":false}]),
            vec![("tool_use_result", json!({"newTodos": todos})), ("tool_result_meta", json!([]))],
        );
        self.send_result(io, m);
        None
    }

    fn plan_answered(&mut self, io: &mut Io, body: &Value, _stats: &mut TurnStats) {
        let tuid = self.tool_use_id.clone();
        let approved = body["behavior"] == "allow";
        let m = self.tool_result_message(
            "tool_allow",
            json!([{"tool_use_id":tuid,"type":"tool_result",
                    "content": if approved {"User has approved your plan."} else {"User rejected the plan."},
                    "is_error": !approved}]),
            vec![("tool_result_meta", json!([]))],
        );
        self.send_result(io, m);
    }

    fn mcp_server(&self, name: &str) -> Option<crate::mcp::McpLaunch> {
        self.mcp.get(name).cloned()
    }

    fn background(&self) -> bool {
        true
    }

    fn task_started(
        &mut self,
        io: &mut Io,
        task: &BackgroundStep,
        stats: &mut TurnStats,
    ) -> TaskIds {
        let agent = task.kind != "local_bash";
        let kind = if agent { "local_agent" } else { "local_bash" };
        let rec = if agent {
            "background_agent"
        } else {
            "background_bash"
        };
        let ids_task = format!(
            "{}{}",
            if agent { "a" } else { "b" },
            hex(if agent { 16 } else { 8 })
        );
        let subagent_type = agent.then(|| {
            task.subagent_type
                .clone()
                .unwrap_or_else(|| "general-purpose".into())
        });
        // The spawning call, as the model makes it.
        let input = if agent {
            json!({"description": task.description, "subagent_type": subagent_type,
                   "prompt": task.description, "run_in_background": true})
        } else {
            json!({"command": task.command.clone().unwrap_or_else(|| "sleep 1".into()),
                   "description": task.description, "run_in_background": true})
        };
        self.tool_use_message(io, if agent { "Agent" } else { "Bash" }, &input, stats);
        // The subagent's own id is its task's (recorded).
        let ids = TaskIds {
            agent_id: ids_task.clone(),
            task_id: ids_task,
            run_id: format!("{}-{}", alnum(9).to_lowercase(), hex(8)),
            call_id: self.tool_use_id.clone(),
        };
        self.running.push(RunningTask {
            ids: ids.clone(),
            kind: kind.into(),
            description: task.description.clone(),
            subagent_type: subagent_type.clone(),
        });
        // The list of background tasks, then the start, then the result of the spawning call
        // (the real order, both kinds).
        let mut list = first_where(rec, |m| {
            ty(m, "system")
                && m["subtype"] == "background_tasks_changed"
                && m["tasks"].as_array().is_some_and(|t| !t.is_empty())
        });
        list["tasks"] = self.task_list(rec);
        let list = self.envelope(list);
        io.send(&list);
        let mut started = first_where(rec, |m| ty(m, "system") && m["subtype"] == "task_started");
        set(&mut started, "task_id", json!(ids.task_id));
        set(&mut started, "run_id", json!(ids.run_id));
        set(&mut started, "tool_use_id", json!(ids.call_id));
        set(&mut started, "description", json!(task.description));
        if let Some(t) = &subagent_type {
            set(&mut started, "subagent_type", json!(t));
            set(&mut started, "prompt", json!(task.description));
        }
        let started = self.envelope(started);
        io.send(&started);
        let content = if agent {
            "Async agent launched successfully. (This tool result is internal metadata — it is not for the user.)".to_string()
        } else {
            format!(
                "Command running in background with ID: {}. Output is being written to: /tmp/claude-fake/{}/tasks/{}.output",
                ids.task_id, self.session_id, ids.task_id
            )
        };
        let result = self.tool_result_message(
            rec,
            json!([{"tool_use_id": ids.call_id, "type": "tool_result", "content": content}]),
            vec![],
        );
        io.send(&result);
        if agent {
            // The subagent's conversation is its own file, which the spawning call's result
            // links to by `agentId` (as the real CLI writes it).
            let sub = self.main_writer().begin_subagent(
                &ids.agent_id,
                &ids.call_id,
                &task.description,
                subagent_type.as_deref().unwrap_or("general-purpose"),
                &task.description,
            );
            self.subs.push((ids.agent_id.clone(), sub));
            let assistant = self.tool_assistant.clone();
            let output_file = format!(
                "/tmp/claude-fake/{}/tasks/{}.output",
                self.session_id, ids.agent_id
            );
            let model = self.model.clone();
            let w = self.main_writer();
            w.tool_result(
                &ids.call_id,
                json!([{"type":"text","text":content}]),
                false,
                json!({"status":"async_launched","agentId":ids.agent_id,
                       "description":task.description,"prompt":task.description,
                       "outputFile":output_file,"canReadOutputFile":true,"isAsync":true,
                       "resolvedModel":model}),
                &assistant,
            );
            w.flush_logged();
        } else {
            self.record_tool_result(&result);
        }
        ids
    }

    fn set_scope(&mut self, scope: Option<(&BackgroundStep, &TaskIds)>) {
        self.scope = scope.map(|(task, ids)| ScopeTag {
            call_id: ids.call_id.clone(),
            agent_id: ids.agent_id.clone(),
            subagent_type: task
                .subagent_type
                .clone()
                .unwrap_or_else(|| "general-purpose".into()),
            description: task.description.clone(),
        });
        self.scope_tools = 0;
    }

    fn task_progress(&mut self, io: &mut Io, tool: &str) {
        let Some(scope) = self.scope.clone() else {
            return;
        };
        self.scope_tools += 1;
        let mut m = first_where("background_agent", |m| {
            ty(m, "system") && m["subtype"] == "task_progress"
        });
        set(&mut m, "task_id", json!(scope.agent_id));
        set(&mut m, "tool_use_id", json!(scope.call_id));
        set(&mut m, "description", json!(format!("Running {tool}")));
        set(&mut m, "last_tool_name", json!(tool));
        set(&mut m, "usage.tool_uses", json!(self.scope_tools));
        set(
            &mut m,
            "usage.total_tokens",
            json!(59_000 + 900 * self.scope_tools),
        );
        let m = self.envelope(m);
        io.send(&m);
    }

    fn task_ended(&mut self, io: &mut Io, task: &BackgroundStep, ids: &TaskIds, how: TaskEnd) {
        let agent = task.kind != "local_bash";
        let rec = if agent {
            "background_agent"
        } else {
            "background_bash"
        };
        self.running.retain(|t| t.ids.task_id != ids.task_id);
        let (patch, status) = match how {
            TaskEnd::Completed => ("completed", "completed"),
            TaskEnd::Failed => ("failed", "failed"),
            TaskEnd::Stopped => ("killed", "stopped"),
        };
        let mut u = first_where(rec, |m| ty(m, "system") && m["subtype"] == "task_updated");
        set(&mut u, "task_id", json!(ids.task_id));
        set(&mut u, "run_id", json!(ids.run_id));
        set(&mut u, "patch.status", json!(patch));
        set(&mut u, "patch.end_time", json!(now_ms()));
        let u = self.envelope(u);
        io.send(&u);
        let summary = task.summary.clone().unwrap_or_else(|| {
            if agent {
                task.description.clone()
            } else {
                match how {
                    TaskEnd::Completed => format!(
                        "Background command \"{}\" completed (exit code 0)",
                        task.description
                    ),
                    TaskEnd::Failed => format!(
                        "Background command \"{}\" failed with exit code 1",
                        task.description
                    ),
                    TaskEnd::Stopped => task.description.clone(),
                }
            }
        });
        let mut n = first_where(rec, |m| {
            ty(m, "system") && m["subtype"] == "task_notification"
        });
        set(&mut n, "task_id", json!(ids.task_id));
        set(&mut n, "run_id", json!(ids.run_id));
        set(&mut n, "tool_use_id", json!(ids.call_id));
        set(&mut n, "status", json!(status));
        set(
            &mut n,
            "output_file",
            json!(format!(
                "/tmp/claude-fake/{}/tasks/{}.output",
                self.session_id, ids.task_id
            )),
        );
        set(&mut n, "summary", json!(summary));
        let n = self.envelope(n);
        io.send(&n);
        let mut list = first_where(rec, |m| {
            ty(m, "system")
                && m["subtype"] == "background_tasks_changed"
                && m["tasks"].as_array().is_some_and(|t| t.is_empty())
        });
        list["tasks"] = self.task_list(rec);
        let list = self.envelope(list);
        io.send(&list);
        // The agent's next turn of its own answers this end.
        self.answers_run = Some(ids.run_id.clone());
    }

    fn stop_wakes(&self, task: &BackgroundStep) -> bool {
        // Recorded: stopping a subagent makes the agent report it with a turn of its own;
        // stopping a background command starts none.
        task.kind != "local_bash"
    }

    fn agent_turn_begin(&mut self, io: &mut Io, _ids: &TaskIds, _stats: &mut TurnStats) {
        self.current = None;
        self.agent_turn = true;
        // `system/init`, with no lifecycle frame before it: no message of the client's started it.
        self.send_system_init(io);
    }

    fn prompt_joined(&mut self, io: &mut Io, id: &str) {
        self.waiting.retain(|w| w != id);
        self.lifecycle(io, id, "started");
    }

    fn control_done(&mut self, io: &mut Io, request: &Value, _found: bool) {
        // Answered with success whatever the id (recorded).
        self.control_ok(io, request, json!({}));
    }

    fn cancel_answered(&mut self, io: &mut Io, request: &Value, id: &str, cancelled: bool) {
        if cancelled {
            self.waiting.retain(|w| w != id);
            self.lifecycle(io, id, "cancelled");
        }
        self.control_ok(io, request, json!({ "cancelled": cancelled }));
    }

    fn turn_end(&mut self, io: &mut Io, outcome: &Outcome, stats: &mut TurnStats) {
        match outcome {
            Outcome::Completed => {
                let mut s = first_where("tool_allow", |m| {
                    ty(m, "system") && m["subtype"] == "post_turn_summary"
                });
                set(&mut s, "summarizes_uuid", json!(uuid()));
                set(&mut s, "status_detail", json!("completed"));
                let s = self.envelope(s);
                io.send(&s);
            }
            Outcome::Interrupted => {
                let mut m = first_where("interrupt", |m| ty(m, "user"));
                set(
                    &mut m,
                    "message.content",
                    json!([{"type":"text","text":"[Request interrupted by user]"}]),
                );
                set(&mut m, "timestamp", json!(iso_now()));
                let m = self.envelope(m);
                io.send(&m);
            }
            Outcome::Failed(msg) => {
                let mut a = first_where("turn_error", |m| ty(m, "assistant"));
                set(
                    &mut a,
                    "message.content",
                    json!([{"type":"text","text":msg}]),
                );
                set(&mut a, "message.id", json!(uuid()));
                let a = self.envelope(a);
                io.send(&a);
            }
        }
        // What the CLI leaves in the transcript when a turn ends: how it ended, then the
        // bookkeeping after the last message.
        {
            let w = self.main_writer();
            match outcome {
                Outcome::Completed => {}
                Outcome::Interrupted => {
                    w.interrupted();
                }
                Outcome::Failed(msg) => {
                    w.assistant_error(msg);
                }
            }
            w.end_turn();
            w.flush_logged();
        }
        let mut r = self.result_message(outcome, stats);
        let listed: Vec<&String> = self.current.iter().chain(stats.joined.iter()).collect();
        if !listed.is_empty() {
            set(&mut r, "user_message_uuids", json!(listed));
        }
        // Messages the agent took into its turn are done just before the turn's `result`
        // (recorded; a message that started the turn is done after it).
        for id in stats.joined.clone() {
            self.lifecycle(io, &id, "completed");
        }
        if self.agent_turn {
            if let Some(run) = self.answers_run.take() {
                set(
                    &mut r,
                    "origin",
                    json!({"kind":"task-notification","producer":"session-task","runId":run}),
                );
            }
        }
        self.agent_turn = false;
        io.send(&r);
        if let Some(uuid) = self.current.take() {
            // Not recorded for a failed turn; `completed` is assumed (it ran).
            let state = match outcome {
                Outcome::Interrupted => "cancelled",
                Outcome::Completed | Outcome::Failed(_) => "completed",
            };
            self.lifecycle(io, &uuid, state);
        }
        if stats.tool_calls > 0 {
            let mut t = first_where("tool_allow", |m| {
                ty(m, "system") && m["subtype"] == "task_summary" && m["detail"].is_null()
            });
            t["detail"] = Value::Null;
            let t = self.envelope(t);
            io.send(&t);
        }
    }
}

/// A `tool_result` content: the text, or (with an image) the image block alone, as claude
/// sends a `Read` of an image file.
fn result_content(text: &str, image: Option<(String, String)>) -> Value {
    match image {
        Some((mime, data)) => {
            json!([{"type":"image","source":{"type":"base64","data":data,"media_type":mime}}])
        }
        None => json!(text),
    }
}
