//! Persona-agnostic turn engine: runs a scenario turn's steps against a [`Persona`] that knows
//! how to put each step on its own wire, and handles the control-flow steps itself.

use crate::io::{Io, Recv};
use crate::scenario::{
    AskStep, BackgroundStep, PlanEntry, Scenario, Step, ToolStep, Turn, UsageStep,
};
use regex::Regex;
use serde_json::Value;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// What a received line means to the engine.
#[derive(Debug)]
pub enum Incoming {
    /// A user prompt (starts a turn, or is queued while one runs).
    Prompt(String),
    /// A user prompt with the client's id for it (claude: the user message's `uuid`).
    TrackedPrompt { text: String, id: String },
    /// The client asks to drop the queued prompt `id` (claude `cancel_async_message`);
    /// `request` is the id of that request.
    Cancel { request: Value, id: String },
    /// The client asks to stop the background task `task_id` (claude `stop_task`); `request` is
    /// the id of that request. Answered by [`Persona::task_ended`] + [`Persona::control_done`].
    StopTask { request: Value, task_id: String },
    /// Interrupt / cancel the running turn (the persona already acked it on the wire).
    Interrupt,
    /// The client's answer to a request the fake sent; `id` is the request id.
    Response { id: Value, body: Value },
    /// Fully handled by the persona (or ignorable).
    Handled,
}

/// How a turn ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Completed,
    Interrupted,
    Failed(String),
}

/// Running facts about the current turn, handed to `turn_end`.
#[derive(Debug, Clone, Default)]
pub struct TurnStats {
    pub usage: UsageStep,
    /// Text of the last `say` (the claude `result` text, codex final agent message).
    pub final_text: String,
    /// Every `say` text of the turn, in order (codex puts these into `turn/completed`).
    pub said: Vec<String>,
    /// `(tool name, tool call id, input)` of denied tool calls (claude `permission_denials`).
    pub denied: Vec<(String, String, Value)>,
    pub tool_calls: u32,
    pub started: Option<Instant>,
    /// Ids of prompts the agent took into this turn while it ran (claude: the lifecycle
    /// `started` of a message read during a turn the CLI started by itself).
    pub joined: Vec<String>,
}

/// The ids a background task is known by.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskIds {
    pub task_id: String,
    pub run_id: String,
    /// The tool call that spawned it.
    pub call_id: String,
    /// The subagent's own id (claude: the same as the task's).
    pub agent_id: String,
}

/// How a background task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskEnd {
    Completed,
    Failed,
    /// Stopped by the client (`stop_task`).
    Stopped,
}

/// Wire-specific emission for each scenario step.
pub trait Persona {
    /// Interpret one incoming line. `in_turn` says whether a turn is running. Protocol-level
    /// requests (initialize, set mode, list, …) are answered here, whatever `in_turn` is.
    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming;
    /// A turn starts for `prompt` (`id`: the client's id for it, when it gave one).
    fn turn_begin(&mut self, io: &mut Io, prompt: &str, id: Option<&str>, stats: &mut TurnStats);
    fn say(&mut self, io: &mut Io, text: &str, chunks: usize, stats: &mut TurnStats);
    fn think(&mut self, io: &mut Io, text: &str, stats: &mut TurnStats);
    /// Emit the tool call. With `needs_permission`, also emit the permission request and return
    /// its id; otherwise run the call to completion and return `None`.
    fn tool_call(&mut self, io: &mut Io, tool: &ToolStep, stats: &mut TurnStats) -> Option<Value>;
    /// Finish a gated tool call with the client's answer.
    fn tool_answered(&mut self, io: &mut Io, tool: &ToolStep, body: &Value, stats: &mut TurnStats);
    /// Emit a question; returns the request id to wait on.
    fn question(&mut self, io: &mut Io, ask: &AskStep, stats: &mut TurnStats) -> Value;
    fn question_answered(
        &mut self,
        io: &mut Io,
        ask: &AskStep,
        body: &Value,
        stats: &mut TurnStats,
    );
    /// Plan update. Returns the request id when the plan needs approval.
    fn plan(
        &mut self,
        io: &mut Io,
        entries: &[PlanEntry],
        needs_approval: bool,
        stats: &mut TurnStats,
    ) -> Option<Value>;
    fn plan_answered(&mut self, _io: &mut Io, _body: &Value, _stats: &mut TurnStats) {}
    /// Show an image in the reply (`Step::Image`); personas without such a message ignore it.
    fn image(&mut self, _io: &mut Io, _mime: &str, _data: &str, _stats: &mut TurnStats) {}
    fn turn_end(&mut self, io: &mut Io, outcome: &Outcome, stats: &mut TurnStats);
    /// The MCP server `name` this CLI was configured with (for `mcp_call` steps).
    fn mcp_server(&self, _name: &str) -> Option<crate::mcp::McpLaunch> {
        None
    }
    /// Answer an [`Incoming::Cancel`]: `cancelled` says whether the prompt was still queued
    /// (and is now removed).
    fn cancel_answered(&mut self, _io: &mut Io, _request: &Value, _id: &str, _cancelled: bool) {}

    /// This persona models background tasks ([`Step::Background`]).
    fn background(&self) -> bool {
        false
    }
    /// The agent starts `task` in the background: print what the CLI prints at that moment (the
    /// spawning call, the task frames, the call's result) and return the ids it chose.
    fn task_started(
        &mut self,
        _io: &mut Io,
        _task: &BackgroundStep,
        _stats: &mut TurnStats,
    ) -> TaskIds {
        TaskIds::default()
    }
    /// The lines printed next belong to the subagent `ids` (`None`: back to the session's own).
    fn set_scope(&mut self, _scope: Option<(&BackgroundStep, &TaskIds)>) {}
    /// The subagent just made a tool call: report its progress.
    fn task_progress(&mut self, _io: &mut Io, _tool: &str) {}
    /// The task ended `how`: print the end frames (`task_updated`, `task_notification`, the new
    /// list of background tasks) and remember that the next turn the agent starts is the answer
    /// to it.
    fn task_ended(&mut self, _io: &mut Io, _task: &BackgroundStep, _ids: &TaskIds, _how: TaskEnd) {}
    /// Whether the agent starts a turn when `task` was stopped.
    fn stop_wakes(&self, _task: &BackgroundStep) -> bool {
        false
    }
    /// A turn the agent starts by itself begins (`ids`: the task whose end it answers).
    fn agent_turn_begin(&mut self, _io: &mut Io, _ids: &TaskIds, _stats: &mut TurnStats) {}
    /// The agent took the prompt `id` (read during its turn) into the running turn.
    fn prompt_joined(&mut self, _io: &mut Io, _id: &str) {}
    /// Answer an [`Incoming::StopTask`]: `found` says whether the task was running.
    fn control_done(&mut self, _io: &mut Io, _request: &Value, _found: bool) {}
}

/// A background task that has not ended yet.
struct Pending {
    task: BackgroundStep,
    ids: TaskIds,
    due: Instant,
}

/// The background tasks of the session.
#[derive(Default)]
pub struct Tasks {
    pending: Vec<Pending>,
}

impl Tasks {
    fn next_due(&self) -> Option<Instant> {
        self.pending.iter().map(|p| p.due).min()
    }

    fn take_due(&mut self, now: Instant) -> Option<Pending> {
        let i = self
            .pending
            .iter()
            .enumerate()
            .filter(|(_, p)| p.due <= now)
            .min_by_key(|(_, p)| p.due)
            .map(|(i, _)| i)?;
        Some(self.pending.remove(i))
    }

    fn take(&mut self, task_id: &str) -> Option<Pending> {
        let i = self.pending.iter().position(|p| p.ids.task_id == task_id)?;
        Some(self.pending.remove(i))
    }
}

/// A prompt waiting for its turn.
struct Queued {
    text: String,
    id: Option<String>,
}

/// Remove the queued prompt `id`; `true` when it was there.
fn cancel_queued(queue: &mut VecDeque<Queued>, id: &str) -> bool {
    match queue.iter().position(|q| q.id.as_deref() == Some(id)) {
        Some(i) => queue.remove(i).is_some(),
        None => false,
    }
}

/// Per-process scenario cursor.
pub struct Cursor {
    scenario: Scenario,
    next: usize,
}

impl Cursor {
    pub fn new(scenario: Scenario) -> Cursor {
        Cursor { scenario, next: 0 }
    }

    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    /// Pick the turn for `prompt`: first matching `when` turn, else the next unconsumed
    /// unconditional turn, else the echo reply.
    pub fn select(&mut self, prompt: &str) -> Turn {
        for t in &self.scenario.turns {
            if let Some(re) = &t.when {
                if Regex::new(re).map(|r| r.is_match(prompt)).unwrap_or(false) {
                    return t.clone();
                }
            }
        }
        while self.next < self.scenario.turns.len() {
            let t = &self.scenario.turns[self.next];
            self.next += 1;
            if t.when.is_none() {
                return t.clone();
            }
        }
        let prefix = self.scenario.echo_prefix.as_deref().unwrap_or("echo: ");
        Turn {
            when: None,
            steps: vec![Step::Say {
                text: format!("{prefix}{prompt}"),
                chunks: None,
            }],
        }
    }
}

/// Serve until stdin closes: take prompts, run their turns, and end background tasks when they
/// are due (the agent then starts turns by itself).
pub fn serve<P: Persona>(p: &mut P, io: &mut Io, cursor: &mut Cursor) {
    for step in cursor.scenario.startup.clone() {
        match step {
            Step::Stderr { text } => io.stderr(&text),
            Step::Garbage { line } => io.send_raw(line.as_deref().unwrap_or("{this is not json")),
            Step::Exit { code } => std::process::exit(code),
            Step::Hang => {
                // Never answer; drain stdin so writers never block; exit when it closes.
                while io.recv().is_some() {}
                return;
            }
            _ => {}
        }
    }
    let mut queue: VecDeque<Queued> = VecDeque::new();
    let mut tasks = Tasks::default();
    loop {
        let prompt = match queue.pop_front() {
            Some(q) => q,
            None => {
                // Idle: wait for a line, or for the moment the next background task ends.
                let recv = match tasks.next_due() {
                    Some(due) => match due.checked_duration_since(Instant::now()) {
                        Some(left) if !left.is_zero() => io.recv_timeout(left),
                        _ => Recv::Timeout,
                    },
                    None => match io.recv() {
                        Some(l) => Recv::Line(l),
                        None => Recv::Eof,
                    },
                };
                match recv {
                    Recv::Eof => return,
                    Recv::Timeout => {
                        if let Some(due) = tasks.take_due(Instant::now()) {
                            let wake = end_task(p, io, due, TaskEnd::Completed);
                            if let Some((steps, ids)) = wake {
                                if run_agent_turn(p, io, &steps, &ids, &mut queue, &mut tasks)
                                    == Flow::Eof
                                {
                                    return;
                                }
                            }
                        }
                        continue;
                    }
                    Recv::Line(line) => match p.handle_line(io, &line, false) {
                        Incoming::Prompt(text) => Queued { text, id: None },
                        Incoming::TrackedPrompt { text, id } => Queued { text, id: Some(id) },
                        Incoming::Cancel { request, id } => {
                            p.cancel_answered(io, &request, &id, false);
                            continue;
                        }
                        Incoming::StopTask { request, task_id } => {
                            // The real order (recorded): the end frames, then the answer, then
                            // the turn the agent starts for it, if it does.
                            let found = tasks.take(&task_id);
                            let was_running = found.is_some();
                            let wake = found
                                .and_then(|pending| end_task(p, io, pending, TaskEnd::Stopped));
                            p.control_done(io, &request, was_running);
                            if let Some((steps, ids)) = wake {
                                if run_agent_turn(p, io, &steps, &ids, &mut queue, &mut tasks)
                                    == Flow::Eof
                                {
                                    return;
                                }
                            }
                            continue;
                        }
                        _ => continue,
                    },
                }
            }
        };
        let turn = cursor.select(&prompt.text);
        if run_turn(p, io, &turn, &prompt, &mut queue, &mut tasks) == Flow::Eof {
            return;
        }
    }
}

#[derive(PartialEq, Eq)]
enum Flow {
    Continue,
    Eof,
}

/// What interrupted a wait.
enum Wake {
    Done,
    Interrupted,
    Eof,
}

/// Who started the turn a step list runs in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TurnKind {
    /// A prompt of the client's: a prompt read meanwhile waits for the next turn.
    Prompt,
    /// The agent itself: a prompt read meanwhile is taken into the running turn at the next
    /// step boundary (recorded: `reference/claude/agent_turn_prompt.jsonl`).
    Agent,
    /// A subagent's own conversation, between turns.
    Subagent,
}

/// Print the end of `due` and say which turn the agent starts for it, if any.
fn end_task<P: Persona>(
    p: &mut P,
    io: &mut Io,
    due: Pending,
    how: TaskEnd,
) -> Option<(Vec<Step>, TaskIds)> {
    let Pending { task, ids, .. } = due;
    if how != TaskEnd::Stopped {
        // The subagent's own conversation runs its course first.
        p.set_scope(Some((&task, &ids)));
        let mut stats = TurnStats::default();
        let mut queue = VecDeque::new();
        let mut none = Tasks::default();
        // A subagent has no one to interrupt it between turns; a closed stdin ends the serve loop
        // at its next read.
        let _ = run_steps(
            p,
            io,
            &task.steps,
            &mut stats,
            &mut queue,
            &mut none,
            TurnKind::Subagent,
        );
        p.set_scope(None);
    }
    let how_end = if how == TaskEnd::Completed && task.status.as_deref() == Some("failed") {
        TaskEnd::Failed
    } else {
        how
    };
    p.task_ended(io, &task, &ids, how_end);
    let wakes = match how_end {
        TaskEnd::Completed | TaskEnd::Failed => true,
        TaskEnd::Stopped => p.stop_wakes(&task),
    };
    let steps = if how_end == TaskEnd::Stopped {
        task.on_stop.clone()
    } else {
        task.then.clone()
    };
    (wakes && !steps.is_empty()).then_some((steps, ids))
}

/// A turn the agent starts by itself (background work ended).
fn run_agent_turn<P: Persona>(
    p: &mut P,
    io: &mut Io,
    steps: &[Step],
    ids: &TaskIds,
    queue: &mut VecDeque<Queued>,
    tasks: &mut Tasks,
) -> Flow {
    let mut stats = TurnStats {
        started: Some(Instant::now()),
        ..TurnStats::default()
    };
    p.agent_turn_begin(io, ids, &mut stats);
    let outcome = match run_steps(p, io, steps, &mut stats, queue, tasks, TurnKind::Agent) {
        Ok(o) => o,
        Err(flow) => return flow,
    };
    p.turn_end(io, &outcome, &mut stats);
    Flow::Continue
}

fn run_turn<P: Persona>(
    p: &mut P,
    io: &mut Io,
    turn: &Turn,
    prompt: &Queued,
    queue: &mut VecDeque<Queued>,
    tasks: &mut Tasks,
) -> Flow {
    let mut stats = TurnStats {
        started: Some(Instant::now()),
        ..TurnStats::default()
    };
    p.turn_begin(io, &prompt.text, prompt.id.as_deref(), &mut stats);
    let outcome = match run_steps(
        p,
        io,
        &turn.steps,
        &mut stats,
        queue,
        tasks,
        TurnKind::Prompt,
    ) {
        Ok(o) => o,
        Err(flow) => return flow,
    };
    p.turn_end(io, &outcome, &mut stats);
    Flow::Continue
}

/// Run `steps` in a turn of `kind`; `Err` when the process must stop serving (stdin closed, a
/// step that never returns).
fn run_steps<P: Persona>(
    p: &mut P,
    io: &mut Io,
    steps: &[Step],
    stats: &mut TurnStats,
    queue: &mut VecDeque<Queued>,
    tasks: &mut Tasks,
    kind: TurnKind,
) -> Result<Outcome, Flow> {
    let mut outcome = Outcome::Completed;
    // Results saved by `mcp_call` steps, for `{{name.path}}` placeholders.
    let mut vars: std::collections::BTreeMap<String, Value> = Default::default();
    // Prompts read during an agent's own turn, taken in at the next step boundary.
    let mut joining: VecDeque<Queued> = VecDeque::new();
    for step in steps {
        let wake = match step {
            Step::Say { text, chunks } => {
                let text = &crate::mcp::substitute(text, &vars);
                p.say(io, text, chunks.unwrap_or(1), stats);
                stats.final_text = text.clone();
                stats.said.push(text.clone());
                Wake::Done
            }
            Step::McpCall(c) => {
                stats.tool_calls += 1;
                let args = crate::mcp::substitute_json(&c.arguments, &vars);
                let r = match p.mcp_server(&c.server) {
                    Some(server) => crate::mcp::call(&server, &c.tool, &args),
                    None => Err(format!("no MCP server named {:?} is configured", c.server)),
                };
                let text = match &r {
                    Ok(t) => t.clone(),
                    Err(e) => format!("error: {e}"),
                };
                if let Some(name) = &c.save_as {
                    let v = match &r {
                        Ok(t) => serde_json::from_str(t).unwrap_or(Value::String(t.clone())),
                        Err(e) => serde_json::json!({ "error": e }),
                    };
                    vars.insert(name.clone(), v);
                }
                let tool = crate::scenario::ToolStep {
                    name: format!("mcp__{}__{}", c.server, c.tool),
                    input: args,
                    needs_permission: false,
                    result_if_allowed: Some(text),
                    result_if_denied: None,
                    title: None,
                    image: None,
                    write_file: None,
                };
                p.tool_call(io, &tool, stats);
                Wake::Done
            }
            Step::Think { text } => {
                p.think(io, text, stats);
                Wake::Done
            }
            Step::Image(img) => {
                if let Some((mime, data)) = img.load() {
                    p.image(io, &mime, &data, stats);
                }
                Wake::Done
            }
            Step::Usage(u) => {
                stats.usage = u.clone();
                Wake::Done
            }
            Step::Stderr { text } => {
                io.stderr(text);
                Wake::Done
            }
            Step::Garbage { line } => {
                io.send_raw(line.as_deref().unwrap_or("{this is not json"));
                Wake::Done
            }
            Step::Exit { code } => {
                std::process::exit(*code);
            }
            Step::Hang => {
                // Stop answering anything; drain stdin so the driver's writes never block, and
                // exit when it closes the pipe.
                while io.recv().is_some() {}
                return Err(Flow::Eof);
            }
            Step::Fail { message } => {
                outcome = Outcome::Failed(message.clone());
                break;
            }
            Step::Background(task) => {
                if p.background() {
                    stats.tool_calls += 1;
                    let ids = p.task_started(io, task, stats);
                    tasks.pending.push(Pending {
                        task: task.clone(),
                        ids,
                        due: Instant::now() + Duration::from_millis(task.after_ms),
                    });
                }
                Wake::Done
            }
            Step::SleepMs { ms } => wait(
                p,
                io,
                queue,
                tasks,
                &mut joining,
                kind,
                Some(Duration::from_millis(*ms)),
                |_| false,
            ),
            Step::WaitForInterrupt => {
                wait(p, io, queue, tasks, &mut joining, kind, None, |_| false)
            }
            Step::Tool(tool) => {
                stats.tool_calls += 1;
                match p.tool_call(io, tool, stats) {
                    None => {
                        if kind == TurnKind::Subagent {
                            p.task_progress(io, &tool.name);
                        }
                        Wake::Done
                    }
                    Some(id) => {
                        let mut answer = None;
                        let w = wait(
                            p,
                            io,
                            queue,
                            tasks,
                            &mut joining,
                            kind,
                            None,
                            |inc| match inc {
                                Incoming::Response { id: rid, body } if *rid == id => {
                                    answer = Some(body.clone());
                                    true
                                }
                                _ => false,
                            },
                        );
                        if let (Wake::Done, Some(body)) = (&w, answer) {
                            p.tool_answered(io, tool, &body, stats);
                        }
                        w
                    }
                }
            }
            Step::AskQuestion(ask) => {
                stats.tool_calls += 1;
                let id = p.question(io, ask, stats);
                let mut answer = None;
                let w = wait(
                    p,
                    io,
                    queue,
                    tasks,
                    &mut joining,
                    kind,
                    None,
                    |inc| match inc {
                        Incoming::Response { id: rid, body } if *rid == id => {
                            answer = Some(body.clone());
                            true
                        }
                        _ => false,
                    },
                );
                if let (Wake::Done, Some(body)) = (&w, answer) {
                    p.question_answered(io, ask, &body, stats);
                }
                w
            }
            Step::Plan {
                entries,
                needs_approval,
            } => match p.plan(io, entries, *needs_approval, stats) {
                None => Wake::Done,
                Some(id) => {
                    let mut answer = None;
                    let w = wait(
                        p,
                        io,
                        queue,
                        tasks,
                        &mut joining,
                        kind,
                        None,
                        |inc| match inc {
                            Incoming::Response { id: rid, body } if *rid == id => {
                                answer = Some(body.clone());
                                true
                            }
                            _ => false,
                        },
                    );
                    if let (Wake::Done, Some(body)) = (&w, answer) {
                        p.plan_answered(io, &body, stats);
                    }
                    w
                }
            },
        };
        match wake {
            Wake::Done => {}
            Wake::Interrupted => {
                outcome = Outcome::Interrupted;
                break;
            }
            Wake::Eof => return Err(Flow::Eof),
        }
        // A step boundary: the agent takes in what was read during the step.
        while let Some(q) = joining.pop_front() {
            if let Some(id) = q.id {
                p.prompt_joined(io, &id);
                stats.joined.push(id);
            }
        }
    }
    // A prompt read after the last step boundary, or during an interrupted or failed turn,
    // runs as a turn of its own (a message the CLI holds survives an interrupt: recorded).
    queue.extend(joining.drain(..));
    Ok(outcome)
}

/// Wait while serving protocol traffic. Returns `Done` when `deadline` passes (for a timed wait)
/// or `matches` returns true, `Interrupted` on an interrupt, `Eof` when stdin closes. Prompts that
/// arrive meanwhile are queued for after the turn, or (in a turn the agent started by itself)
/// taken into it at the next step boundary.
#[allow(clippy::too_many_arguments)]
fn wait<P: Persona>(
    p: &mut P,
    io: &mut Io,
    queue: &mut VecDeque<Queued>,
    tasks: &mut Tasks,
    joining: &mut VecDeque<Queued>,
    kind: TurnKind,
    timeout: Option<Duration>,
    mut matches: impl FnMut(&Incoming) -> bool,
) -> Wake {
    let deadline = timeout.map(|t| Instant::now() + t);
    loop {
        let recv = match deadline {
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return Wake::Done;
                }
                io.recv_timeout(d - now)
            }
            None => match io.recv() {
                Some(l) => Recv::Line(l),
                None => Recv::Eof,
            },
        };
        match recv {
            Recv::Timeout => return Wake::Done,
            Recv::Eof => return Wake::Eof,
            Recv::Line(line) => {
                let inc = p.handle_line(io, &line, true);
                if matches(&inc) {
                    return Wake::Done;
                }
                match inc {
                    Incoming::Interrupt => return Wake::Interrupted,
                    Incoming::Prompt(text) => queue.push_back(Queued { text, id: None }),
                    Incoming::TrackedPrompt { text, id } => {
                        let q = Queued { text, id: Some(id) };
                        if kind == TurnKind::Agent {
                            joining.push_back(q)
                        } else {
                            queue.push_back(q)
                        }
                    }
                    Incoming::Cancel { request, id } => {
                        // A prompt waiting to join the running turn can still be cancelled.
                        let joined = joining
                            .iter()
                            .position(|q| q.id.as_deref() == Some(id.as_str()))
                            .and_then(|i| joining.remove(i))
                            .is_some();
                        let cancelled = joined || cancel_queued(queue, &id);
                        p.cancel_answered(io, &request, &id, cancelled);
                    }
                    Incoming::StopTask { request, task_id } => {
                        // Mid-turn the end of the task is absorbed by the turn: no turn of its
                        // own follows.
                        let found = tasks.take(&task_id);
                        let was_running = found.is_some();
                        if let Some(pending) = found {
                            let _ = end_task(p, io, pending, TaskEnd::Stopped);
                        }
                        p.control_done(io, &request, was_running);
                    }
                    Incoming::Response { .. } | Incoming::Handled => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::Turn;

    #[test]
    fn select_orders_and_matches() {
        let sc = Scenario::builder()
            .turn(Turn::builder().say("first"))
            .turn(Turn::when("^special").say("special"))
            .turn(Turn::builder().say("second"))
            .build();
        let mut c = Cursor::new(sc);
        let say = |t: &Turn| match &t.steps[0] {
            Step::Say { text, .. } => text.clone(),
            _ => panic!(),
        };
        assert_eq!(say(&c.select("hello")), "first");
        assert_eq!(say(&c.select("special please")), "special");
        assert_eq!(say(&c.select("hello again")), "second");
        assert_eq!(say(&c.select("special again")), "special");
        assert_eq!(say(&c.select("zzz")), "echo: zzz");
    }
}
