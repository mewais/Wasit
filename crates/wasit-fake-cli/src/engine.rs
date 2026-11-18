//! Persona-agnostic turn engine: runs a scenario turn's steps against a [`Persona`] that knows
//! how to put each step on its own wire, and handles the control-flow steps itself.

use crate::io::{Io, Recv};
use crate::scenario::{AskStep, PlanEntry, Scenario, Step, ToolStep, Turn, UsageStep};
use regex::Regex;
use serde_json::Value;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// What a received line means to the engine.
#[derive(Debug)]
pub enum Incoming {
    /// A user prompt (starts a turn, or is queued while one runs).
    Prompt(String),
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
}

/// Wire-specific emission for each scenario step.
pub trait Persona {
    /// Interpret one incoming line. `in_turn` says whether a turn is running. Protocol-level
    /// requests (initialize, set mode, list, …) are answered here, whatever `in_turn` is.
    fn handle_line(&mut self, io: &mut Io, line: &str, in_turn: bool) -> Incoming;
    fn turn_begin(&mut self, io: &mut Io, prompt: &str, stats: &mut TurnStats);
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
    fn turn_end(&mut self, io: &mut Io, outcome: &Outcome, stats: &mut TurnStats);
    /// The MCP server `name` this CLI was configured with (for `mcp_call` steps).
    fn mcp_server(&self, _name: &str) -> Option<crate::mcp::McpLaunch> {
        None
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

/// Serve until stdin closes: take prompts, run their turns.
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
    let mut queue: VecDeque<String> = VecDeque::new();
    loop {
        let prompt = match queue.pop_front() {
            Some(q) => q,
            None => {
                let Some(line) = io.recv() else { return };
                match p.handle_line(io, &line, false) {
                    Incoming::Prompt(t) => t,
                    _ => continue,
                }
            }
        };
        let turn = cursor.select(&prompt);
        if run_turn(p, io, &turn, &prompt, &mut queue) == Flow::Eof {
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

fn run_turn<P: Persona>(
    p: &mut P,
    io: &mut Io,
    turn: &Turn,
    prompt: &str,
    queue: &mut VecDeque<String>,
) -> Flow {
    let mut stats = TurnStats {
        started: Some(Instant::now()),
        ..TurnStats::default()
    };
    p.turn_begin(io, prompt, &mut stats);
    let mut outcome = Outcome::Completed;
    // Results saved by `mcp_call` steps, for `{{name.path}}` placeholders.
    let mut vars: std::collections::BTreeMap<String, Value> = Default::default();
    for step in &turn.steps {
        let wake = match step {
            Step::Say { text, chunks } => {
                let text = &crate::mcp::substitute(text, &vars);
                p.say(io, text, chunks.unwrap_or(1), &mut stats);
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
                };
                p.tool_call(io, &tool, &mut stats);
                Wake::Done
            }
            Step::Think { text } => {
                p.think(io, text, &mut stats);
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
                return Flow::Eof;
            }
            Step::Fail { message } => {
                outcome = Outcome::Failed(message.clone());
                break;
            }
            Step::SleepMs { ms } => wait(p, io, queue, Some(Duration::from_millis(*ms)), |_| false),
            Step::WaitForInterrupt => wait(p, io, queue, None, |_| false),
            Step::Tool(tool) => {
                stats.tool_calls += 1;
                match p.tool_call(io, tool, &mut stats) {
                    None => Wake::Done,
                    Some(id) => {
                        let mut answer = None;
                        let w = wait(p, io, queue, None, |inc| match inc {
                            Incoming::Response { id: rid, body } if *rid == id => {
                                answer = Some(body.clone());
                                true
                            }
                            _ => false,
                        });
                        if let (Wake::Done, Some(body)) = (&w, answer) {
                            p.tool_answered(io, tool, &body, &mut stats);
                        }
                        w
                    }
                }
            }
            Step::AskQuestion(ask) => {
                stats.tool_calls += 1;
                let id = p.question(io, ask, &mut stats);
                let mut answer = None;
                let w = wait(p, io, queue, None, |inc| match inc {
                    Incoming::Response { id: rid, body } if *rid == id => {
                        answer = Some(body.clone());
                        true
                    }
                    _ => false,
                });
                if let (Wake::Done, Some(body)) = (&w, answer) {
                    p.question_answered(io, ask, &body, &mut stats);
                }
                w
            }
            Step::Plan {
                entries,
                needs_approval,
            } => match p.plan(io, entries, *needs_approval, &mut stats) {
                None => Wake::Done,
                Some(id) => {
                    let mut answer = None;
                    let w = wait(p, io, queue, None, |inc| match inc {
                        Incoming::Response { id: rid, body } if *rid == id => {
                            answer = Some(body.clone());
                            true
                        }
                        _ => false,
                    });
                    if let (Wake::Done, Some(body)) = (&w, answer) {
                        p.plan_answered(io, &body, &mut stats);
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
            Wake::Eof => return Flow::Eof,
        }
    }
    p.turn_end(io, &outcome, &mut stats);
    Flow::Continue
}

/// Wait while serving protocol traffic. Returns `Done` when `deadline` passes (for a timed wait)
/// or `matches` returns true, `Interrupted` on an interrupt, `Eof` when stdin closes. Prompts that
/// arrive meanwhile are queued for after the turn.
fn wait<P: Persona>(
    p: &mut P,
    io: &mut Io,
    queue: &mut VecDeque<String>,
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
                    Incoming::Prompt(t) => queue.push_back(t),
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
