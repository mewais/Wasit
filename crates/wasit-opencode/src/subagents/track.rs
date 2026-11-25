//! The subagents a Kilo session owns, as the protocol learns them from the bridge and from ACP.
//!
//! A subagent is a child session of the owner: the bridge reports its `session.created` with the
//! owner as `parent`. It is learned at that moment and loaded on the owner's ACP connection at
//! once, so its own tool calls, text and permission requests reach the protocol with the child's
//! session id. It is paired with the `task` tool call that spawned it. The pairing is first in,
//! first out: the calls of a turn are announced in the order their children are created (the
//! `bg_multi` recording), and the completed call's `metadata.sessionId` confirms it there.
//!
//! Ends. A background child ends when its background job does (`completed` → completed, `error` →
//! failed, `cancelled` → stopped). A foreground child ends when its `task` call does. A lost event
//! stream ends every running child as `unknown`: it may still be running, and that is never
//! reported as success.
//!
//! Permissions. A loaded child's requests arrive on ACP with the child's session id, so the
//! bridge's copy of the same request is ignored. A child that could not be loaded is never asked
//! over ACP, so its requests are raised from the bridge, and the answer goes back on the control
//! line.
//!
//! Nothing here does I/O: the protocol writes the load requests and control lines this module
//! asks for.

use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;
use wasit_core::{
    AgentEvent, DriverError, GateChoice, GateChoiceKind, GateDecision, GateReason, GateRequest,
    Scope as EventScope, TaskOutcome, TaskUsage,
};

use super::{Command, Job, Note, Reply, Scope};

/// A `task` tool call whose description is known.
#[derive(Debug, Clone, PartialEq)]
struct Call {
    id: String,
    description: String,
    subagent_type: Option<String>,
    background: bool,
}

/// One assistant message's tokens and cost, as the bridge last reported them.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Spend {
    input: u64,
    output: u64,
    reasoning: u64,
    cache_read: u64,
    cache_write: u64,
    cost: f64,
}

/// Whether the child's session was loaded on the owner's connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Load {
    Requested,
    Loaded,
    Failed,
}

#[derive(Debug)]
struct Child {
    id: String,
    load: Load,
    /// The task call this child was paired with.
    call: Option<Call>,
    /// `TaskStarted` was emitted: the runtime knows the task.
    started: bool,
    ended: bool,
    stop_requested: bool,
    /// The child's events before it was paired: emitted with their scope once it is.
    buffered: Vec<AgentEvent>,
    started_calls: HashSet<String>,
    finished_calls: HashSet<String>,
    tool_uses: u64,
    /// Per assistant message, so a message reported again replaces its own figures.
    spend: HashMap<String, Spend>,
}

impl Child {
    fn new(id: String) -> Self {
        Child {
            id,
            load: Load::Requested,
            call: None,
            started: false,
            ended: false,
            stop_requested: false,
            buffered: Vec::new(),
            started_calls: HashSet::new(),
            finished_calls: HashSet::new(),
            tool_uses: 0,
            spend: HashMap::new(),
        }
    }

    fn running(&self) -> bool {
        self.started && !self.ended
    }

    fn background(&self) -> bool {
        self.call.as_ref().is_some_and(|c| c.background)
    }
}

/// What a note or a call changed: the events to emit, and the child sessions to load now.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Effects {
    pub events: Vec<AgentEvent>,
    pub load: Vec<String>,
}

impl Effects {
    fn events(events: Vec<AgentEvent>) -> Self {
        Effects {
            events,
            load: Vec::new(),
        }
    }
}

/// The subagents of one owner session.
#[derive(Debug, Default)]
pub(crate) struct Tasks {
    parent: Option<String>,
    children: Vec<Child>,
    /// Children created and not yet paired with a call, in creation order.
    unpaired_children: VecDeque<String>,
    /// Calls whose description is known and whose child has not been seen yet.
    unpaired_calls: VecDeque<Call>,
    /// Every task call whose description was seen, by call id.
    seen_calls: HashSet<String>,
    /// Background job statuses by job id, as last reported.
    job_status: HashMap<String, String>,
    /// Permission requests raised from the bridge and not yet answered: (request id, child id).
    open_gates: Vec<(String, String)>,
}

impl Tasks {
    /// The owner session the children belong to (known once it is open).
    pub fn set_parent(&mut self, session: &str) {
        self.parent = Some(session.to_string());
    }

    pub fn is_child(&self, session: &str) -> bool {
        self.index(session).is_some()
    }

    /// The description of the task a child runs, for display.
    pub fn label(&self, session: &str) -> Option<String> {
        self.index(session)
            .and_then(|i| self.children[i].call.as_ref())
            .map(|c| c.description.clone())
    }

    /// How a child is named in what the user reads: its task's description, when it has one.
    pub fn who(&self, session: &str) -> String {
        match self.label(session) {
            Some(d) if !d.is_empty() => format!("subagent \"{d}\""),
            _ => "subagent".to_string(),
        }
    }

    /// Whether a background subagent is still running (the turn that started it may be over).
    /// Asked by the tests: the interrupt no longer depends on it.
    #[cfg(test)]
    pub fn has_running_background(&self) -> bool {
        self.children.iter().any(|c| c.running() && c.background())
    }

    /// Reduce one bridge note about the owner's children.
    pub fn note(&mut self, note: Note) -> Effects {
        match note {
            Note::SessionCreated { session, parent } => self.created(session, parent),
            Note::Tokens {
                session,
                message,
                input,
                output,
                reasoning,
                cache_read,
                cache_write,
                cost,
            } => self.tokens(
                &session,
                message,
                Spend {
                    input,
                    output,
                    reasoning,
                    cache_read,
                    cache_write,
                    cost,
                },
            ),
            Note::PermissionAsked {
                id,
                session,
                permission,
            } => self.permission_asked(id, &session, &permission),
            Note::BackgroundJobs { session, jobs } => self.background_jobs(&session, &jobs),
            Note::StateLost { .. } => Effects::events(self.lost()),
            Note::SessionStatus { .. }
            | Note::SessionIdle { .. }
            | Note::CommandResult { .. }
            | Note::CommandFailed { .. } => Effects::default(),
        }
    }

    /// A `task` tool call whose description is known (first seen once; later sightings are
    /// ignored). Returns the events of its pairing with a child, if one is waiting.
    pub fn task_call(
        &mut self,
        call_id: &str,
        description: &str,
        background: bool,
        subagent_type: Option<String>,
    ) -> Vec<AgentEvent> {
        if !self.seen_calls.insert(call_id.to_string()) {
            return Vec::new();
        }
        let call = Call {
            id: call_id.to_string(),
            description: description.to_string(),
            subagent_type,
            background,
        };
        match self.unpaired_children.pop_front() {
            Some(child) => self.pair(&child, call),
            None => {
                self.unpaired_calls.push_back(call);
                Vec::new()
            }
        }
    }

    /// The `task` tool call `call_id` finished. A foreground child's end is this; a background
    /// child's end is its job's.
    pub fn task_result(&mut self, call_id: &str, ok: bool) -> Vec<AgentEvent> {
        let Some(i) = self
            .children
            .iter()
            .position(|c| c.call.as_ref().is_some_and(|k| k.id == call_id))
        else {
            return Vec::new();
        };
        let child = &self.children[i];
        if !child.running() || child.background() {
            return Vec::new();
        }
        let outcome = match (child.stop_requested, ok) {
            (true, _) => TaskOutcome::Stopped,
            (false, true) => TaskOutcome::Completed,
            (false, false) => TaskOutcome::Failed,
        };
        self.end(i, outcome)
    }

    /// The events of a child's own conversation. Emitted with the scope of the call that spawned
    /// it, or held until it is paired. A replayed start or finish of a tool call the protocol
    /// already reported is dropped, so a load replays nothing twice.
    pub fn child_events(&mut self, session: &str, events: Vec<AgentEvent>) -> Vec<AgentEvent> {
        let Some(i) = self.index(session) else {
            return Vec::new();
        };
        let child = &mut self.children[i];
        let mut kept = Vec::with_capacity(events.len());
        for event in events {
            let keep = match &event {
                AgentEvent::ToolCallStarted { call_id, .. } => {
                    let first = child.started_calls.insert(call_id.clone());
                    if first {
                        child.tool_uses += 1;
                    }
                    first
                }
                AgentEvent::ToolCallFinished { call_id, .. } => {
                    child.finished_calls.insert(call_id.clone())
                }
                AgentEvent::ToolCallUpdated { call_id, .. } => {
                    !child.finished_calls.contains(call_id)
                }
                _ => true,
            };
            if keep {
                kept.push(event);
            }
        }
        match child.call.clone() {
            Some(call) => {
                let scope = EventScope {
                    call_id: call.id,
                    agent_id: Some(child.id.clone()),
                };
                kept.into_iter()
                    .map(|e| e.scoped(Some(scope.clone())))
                    .collect()
            }
            None => {
                child.buffered.extend(kept);
                Vec::new()
            }
        }
    }

    /// The outcome of the child's session load: a failed load means it is never asked over ACP.
    pub fn load_result(&mut self, session: &str, error: Option<&str>) -> Vec<AgentEvent> {
        let Some(i) = self.index(session) else {
            return Vec::new();
        };
        match error {
            None => {
                self.children[i].load = Load::Loaded;
                Vec::new()
            }
            Some(message) => {
                self.children[i].load = Load::Failed;
                vec![AgentEvent::CommandRejected {
                    command: "session/load".into(),
                    message: format!("subagent {session} could not be loaded: {message}"),
                }]
            }
        }
    }

    /// The control command that stops a running subagent (its tree is aborted).
    pub fn stop_task(&mut self, task_id: &str) -> Result<Command, DriverError> {
        let i = self.running_index(task_id)?;
        self.children[i].stop_requested = true;
        Ok(Command::Abort {
            session: task_id.to_string(),
            scope: Scope::Tree,
        })
    }

    /// The control command that sends `text` to a running subagent.
    pub fn steer(&self, task_id: &str, text: &str) -> Result<Command, DriverError> {
        self.running_index(task_id)?;
        if text.trim().is_empty() {
            return Err(DriverError::Invalid {
                detail: "steer text is empty".into(),
            });
        }
        Ok(Command::Steer {
            session: task_id.to_string(),
            text: text.to_string(),
        })
    }

    /// The answer to a permission request raised from the bridge: `None` when the request is not
    /// one of these (it belongs to ACP). A choice that is not offered is refused, and the request
    /// stays open.
    pub fn answer_gate(
        &mut self,
        request_id: &str,
        decision: &GateDecision,
    ) -> Option<Result<Command, DriverError>> {
        let pos = self.open_gates.iter().position(|(g, _)| g == request_id)?;
        let reply = match decision.choice.as_str() {
            "once" => Reply::Once,
            "always" => Reply::Always,
            "reject" => Reply::Reject,
            _ => {
                return Some(Err(DriverError::Invalid {
                    detail: format!("gate {request_id} has no such choice"),
                }))
            }
        };
        self.open_gates.remove(pos);
        Some(Ok(Command::ReplyPermission {
            id: request_id.to_string(),
            reply,
        }))
    }

    fn index(&self, session: &str) -> Option<usize> {
        self.children.iter().position(|c| c.id == session)
    }

    fn running_index(&self, task_id: &str) -> Result<usize, DriverError> {
        self.index(task_id)
            .filter(|&i| self.children[i].running())
            .ok_or_else(|| DriverError::NotFound {
                detail: format!("no running subagent {task_id}"),
            })
    }

    fn created(&mut self, session: String, parent: Option<String>) -> Effects {
        if parent.is_none() || parent != self.parent || self.index(&session).is_some() {
            return Effects::default();
        }
        self.children.push(Child::new(session.clone()));
        let events = match self.unpaired_calls.pop_front() {
            Some(call) => self.pair(&session, call),
            None => {
                self.unpaired_children.push_back(session.clone());
                Vec::new()
            }
        };
        Effects {
            events,
            load: vec![session],
        }
    }

    /// Pair a child with its call: `TaskStarted`, then what the child did before this, then its
    /// progress so far, then its end if a background job already finished.
    fn pair(&mut self, session: &str, call: Call) -> Vec<AgentEvent> {
        let Some(i) = self.index(session) else {
            return Vec::new();
        };
        let child = &mut self.children[i];
        child.call = Some(call.clone());
        child.started = true;
        let mut out = vec![AgentEvent::TaskStarted {
            task_id: child.id.clone(),
            kind: "subagent".into(),
            call_id: Some(call.id.clone()),
            description: call.description.clone(),
            subagent_type: call.subagent_type.clone(),
            background: call.background,
            run_id: call.background.then(|| child.id.clone()),
        }];
        let scope = EventScope {
            call_id: call.id.clone(),
            agent_id: Some(child.id.clone()),
        };
        out.extend(
            std::mem::take(&mut child.buffered)
                .into_iter()
                .map(|e| e.scoped(Some(scope.clone()))),
        );
        out.extend(self.progress(i));
        if let Some(status) = self.job_status.get(session).cloned() {
            out.extend(self.end_on_job(i, &status));
        }
        out
    }

    /// `TaskProgress` for a running child that has used something.
    fn progress(&self, i: usize) -> Option<AgentEvent> {
        let child = &self.children[i];
        if !child.running() || child.spend.is_empty() {
            return None;
        }
        let call = child.call.as_ref()?;
        Some(AgentEvent::TaskProgress {
            task_id: child.id.clone(),
            call_id: Some(call.id.clone()),
            description: call.description.clone(),
            last_tool: None,
            usage: Some(usage(child)),
        })
    }

    fn tokens(&mut self, session: &str, message: String, spend: Spend) -> Effects {
        let Some(i) = self.index(session) else {
            return Effects::default();
        };
        self.children[i].spend.insert(message, spend);
        Effects::events(self.progress(i).into_iter().collect())
    }

    fn permission_asked(&mut self, id: String, session: &str, permission: &str) -> Effects {
        let Some(i) = self.index(session) else {
            return Effects::default();
        };
        // A loaded child's request is on ACP (it is not repeated here). Only a child that could
        // not be loaded needs the bridge's copy, which is the only way its request is answered.
        if self.children[i].load != Load::Failed || self.open_gates.iter().any(|(g, _)| *g == id) {
            return Effects::default();
        }
        self.open_gates.push((id.clone(), session.to_string()));
        let gate = GateRequest::Permission {
            request_id: id,
            tool_name: permission.to_string(),
            command: String::new(),
            cwd: String::new(),
            explanation: format!("{}: {permission}", self.who(session)),
            choices: unloaded_choices(),
            raw_input: Value::Null,
        };
        Effects::events(vec![AgentEvent::GateRequested { gate }])
    }

    fn background_jobs(&mut self, session: &str, jobs: &[Job]) -> Effects {
        if self.parent.as_deref() != Some(session) {
            return Effects::default();
        }
        for job in jobs {
            self.job_status.insert(job.id.clone(), job.status.clone());
        }
        let mut events = Vec::new();
        for i in 0..self.children.len() {
            let id = self.children[i].id.clone();
            if let Some(status) = self.job_status.get(&id).cloned() {
                events.extend(self.end_on_job(i, &status));
            }
        }
        Effects::events(events)
    }

    fn end_on_job(&mut self, i: usize, status: &str) -> Vec<AgentEvent> {
        if !self.children[i].running() || !self.children[i].background() {
            return Vec::new();
        }
        match status {
            "completed" => self.end(i, TaskOutcome::Completed),
            "error" => self.end(i, TaskOutcome::Failed),
            "cancelled" => self.end(i, TaskOutcome::Stopped),
            // Still running (or a status word this does not know): the job has not ended.
            _ => Vec::new(),
        }
    }

    fn lost(&mut self) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        for i in 0..self.children.len() {
            if self.children[i].running() {
                events.extend(self.end(i, TaskOutcome::Unknown));
            }
        }
        events
    }

    /// End child `i` with `outcome`, and close the permission requests still open for it.
    fn end(&mut self, i: usize, outcome: TaskOutcome) -> Vec<AgentEvent> {
        let child = &mut self.children[i];
        child.ended = true;
        let mut events = vec![AgentEvent::TaskEnded {
            task_id: child.id.clone(),
            call_id: child.call.as_ref().map(|c| c.id.clone()),
            status: outcome.status().to_string(),
            ok: outcome.ok(),
            summary: None,
            usage: Some(usage(child)),
        }];
        let child_id = child.id.clone();
        let closed: Vec<String> = self
            .open_gates
            .iter()
            .filter(|(_, c)| *c == child_id)
            .map(|(g, _)| g.clone())
            .collect();
        self.open_gates.retain(|(_, c)| *c != child_id);
        for request_id in closed {
            events.push(AgentEvent::GateResolved {
                request_id,
                reason: Some(
                    GateReason::new(GateReason::WITHDRAWN)
                        .with_detail("the subagent ended without the request being answered"),
                ),
            });
        }
        events
    }
}

/// A child's usage so far: the sum over its assistant messages.
fn usage(child: &Child) -> TaskUsage {
    let total = child.spend.values().fold(Spend::default(), |a, s| Spend {
        input: a.input + s.input,
        output: a.output + s.output,
        reasoning: a.reasoning + s.reasoning,
        cache_read: a.cache_read + s.cache_read,
        cache_write: a.cache_write + s.cache_write,
        cost: a.cost + s.cost,
    });
    TaskUsage {
        total_tokens: total.input
            + total.output
            + total.reasoning
            + total.cache_read
            + total.cache_write,
        tool_uses: child.tool_uses,
        duration_ms: 0,
        input_tokens: Some(total.input),
        output_tokens: Some(total.output),
        cost_usd: Some(total.cost),
    }
}

/// The choices of a permission the bridge raises: Kilo's answer words, since the request carries
/// no options of its own.
fn unloaded_choices() -> Vec<GateChoice> {
    vec![
        GateChoice::neutral("once", "Allow once", GateChoiceKind::AllowOnce),
        GateChoice::neutral("always", "Allow always", GateChoiceKind::AllowAlways),
        GateChoice::neutral("reject", "Reject", GateChoiceKind::RejectOnce),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn created(session: &str, parent: &str) -> Note {
        Note::SessionCreated {
            session: session.into(),
            parent: Some(parent.into()),
        }
    }

    #[test]
    fn a_child_of_another_session_is_not_ours() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        let fx = t.note(created("ses_child", "ses_other"));
        assert_eq!(fx, Effects::default());
        assert!(!t.is_child("ses_child"));
    }

    #[test]
    fn a_child_is_loaded_at_once_and_paired_when_its_call_is_known() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        let fx = t.note(created("ses_child", "ses_owner"));
        assert_eq!(fx.load, ["ses_child"]);
        assert!(fx.events.is_empty(), "no call yet: nothing to start");
        let events = t.task_call("call_1", "probe", false, Some("general".into()));
        assert!(matches!(
            &events[0],
            AgentEvent::TaskStarted { task_id, call_id: Some(c), background: false, run_id: None, .. }
                if task_id == "ses_child" && c == "call_1"
        ));
    }

    #[test]
    fn a_call_seen_before_its_child_pairs_with_the_child_when_it_appears() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        assert!(t.task_call("call_1", "probe", true, None).is_empty());
        let fx = t.note(created("ses_child", "ses_owner"));
        assert!(matches!(
            &fx.events[0],
            AgentEvent::TaskStarted { task_id, background: true, run_id: Some(r), .. }
                if task_id == "ses_child" && r == "ses_child"
        ));
    }

    #[test]
    fn pairing_is_first_in_first_out_and_a_call_is_seen_once() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_a", "ses_owner"));
        t.note(created("ses_b", "ses_owner"));
        let first = t.task_call("call_1", "same", true, None);
        let second = t.task_call("call_2", "same", true, None);
        assert!(matches!(&first[0], AgentEvent::TaskStarted{ task_id, .. } if task_id == "ses_a"));
        assert!(matches!(&second[0], AgentEvent::TaskStarted{ task_id, .. } if task_id == "ses_b"));
        assert!(t.task_call("call_1", "same", true, None).is_empty());
    }

    #[test]
    fn a_background_child_ends_with_its_job_and_a_foreground_one_with_its_call() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_bg", "ses_owner"));
        t.task_call("call_bg", "bg", true, None);
        t.note(created("ses_fg", "ses_owner"));
        t.task_call("call_fg", "fg", false, None);
        // A foreground call's completion does not end a background child.
        assert!(t.task_result("call_bg", true).is_empty());
        let running = t.note(Note::BackgroundJobs {
            session: "ses_owner".into(),
            jobs: vec![Job {
                id: "ses_bg".into(),
                kind: "task".into(),
                status: "running".into(),
            }],
        });
        assert!(running.events.is_empty());
        let done = t.note(Note::BackgroundJobs {
            session: "ses_owner".into(),
            jobs: vec![Job {
                id: "ses_bg".into(),
                kind: "task".into(),
                status: "completed".into(),
            }],
        });
        assert!(
            matches!(&done.events[0], AgentEvent::TaskEnded { task_id, ok: true, status, .. }
            if task_id == "ses_bg" && status == "completed")
        );
        let fg = t.task_result("call_fg", true);
        assert!(
            matches!(&fg[0], AgentEvent::TaskEnded { task_id, ok: true, .. } if task_id == "ses_fg")
        );
    }

    /// One background job's completion ends its own child only: the others keep running until
    /// their own jobs end (a delivery of one result is not the end of the others).
    #[test]
    fn one_background_job_ending_does_not_end_the_other_background_subagents() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        for (session, call) in [("ses_a", "call_a"), ("ses_b", "call_b")] {
            t.note(created(session, "ses_owner"));
            t.task_call(call, "same", true, None);
        }
        let job = |id: &str, status: &str| Job {
            id: id.into(),
            kind: "task".into(),
            status: status.into(),
        };
        let one = t.note(Note::BackgroundJobs {
            session: "ses_owner".into(),
            jobs: vec![job("ses_a", "completed"), job("ses_b", "running")],
        });
        assert_eq!(one.events.len(), 1);
        assert!(
            matches!(&one.events[0], AgentEvent::TaskEnded { task_id, .. } if task_id == "ses_a")
        );
        assert!(t.has_running_background(), "ses_b still runs");
        let two = t.note(Note::BackgroundJobs {
            session: "ses_owner".into(),
            jobs: vec![job("ses_b", "error")],
        });
        assert!(
            matches!(&two.events[0], AgentEvent::TaskEnded { task_id, status, ok: false, .. }
            if task_id == "ses_b" && status == "failed")
        );
        assert!(!t.has_running_background());
    }

    #[test]
    fn a_stopped_foreground_child_ends_stopped_even_when_its_call_succeeds() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_fg", "ses_owner"));
        t.task_call("call_fg", "fg", false, None);
        let cmd = t.stop_task("ses_fg").unwrap();
        assert_eq!(
            cmd,
            Command::Abort {
                session: "ses_fg".into(),
                scope: Scope::Tree
            }
        );
        let ended = t.task_result("call_fg", true);
        assert!(
            matches!(&ended[0], AgentEvent::TaskEnded { status, ok: false, .. } if status == "stopped")
        );
    }

    #[test]
    fn a_lost_stream_ends_running_children_as_unknown_and_nothing_else() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_a", "ses_owner"));
        t.task_call("call_a", "a", true, None);
        t.note(created("ses_waiting", "ses_owner"));
        let ended = t.note(Note::StateLost {
            reason: "stream_error".into(),
        });
        assert_eq!(ended.events.len(), 1, "the unpaired child is not a task");
        assert!(
            matches!(&ended.events[0], AgentEvent::TaskEnded { status, ok: false, .. } if status == "unknown")
        );
        assert!(t
            .note(Note::StateLost {
                reason: "stream_error".into()
            })
            .events
            .is_empty());
    }

    #[test]
    fn child_events_are_scoped_to_their_call_and_a_replayed_finish_is_dropped() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_c", "ses_owner"));
        let before = t.child_events(
            "ses_c",
            vec![AgentEvent::AssistantDelta {
                text: "early".into(),
                message_id: None,
            }],
        );
        assert!(before.is_empty(), "held until the child has a call");
        let started = t.task_call("call_c", "c", false, None);
        assert!(matches!(&started[0], AgentEvent::TaskStarted { .. }));
        let scoped = |t: &mut Tasks, events| t.child_events("ses_c", events);
        let out = scoped(
            &mut t,
            vec![
                AgentEvent::ToolCallStarted {
                    name: "glob".into(),
                    call_id: "tool_1".into(),
                    input: Value::Null,
                    is_subagent: false,
                    subagent_model: None,
                },
                AgentEvent::ToolCallFinished {
                    call_id: "tool_1".into(),
                    name: "glob".into(),
                    ok: true,
                    result: None,
                    parts: vec![],
                },
            ],
        );
        assert_eq!(out.len(), 2);
        assert!(
            matches!(&out[0], AgentEvent::Scoped{ scope, event } if scope.call_id == "call_c" && scope.agent_id.as_deref() == Some("ses_c") && matches!(**event, AgentEvent::ToolCallStarted{..}))
        );
        // The same start and finish again (a replay): nothing.
        let replay = scoped(
            &mut t,
            vec![
                AgentEvent::ToolCallStarted {
                    name: "glob".into(),
                    call_id: "tool_1".into(),
                    input: Value::Null,
                    is_subagent: false,
                    subagent_model: None,
                },
                AgentEvent::ToolCallFinished {
                    call_id: "tool_1".into(),
                    name: "glob".into(),
                    ok: true,
                    result: None,
                    parts: vec![],
                },
            ],
        );
        assert!(replay.is_empty());
    }

    #[test]
    fn a_message_reported_again_replaces_its_own_tokens() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_c", "ses_owner"));
        t.task_call("call_c", "c", false, None);
        let tok = |msg: &str, input: u64| Note::Tokens {
            session: "ses_c".into(),
            message: msg.into(),
            input,
            output: 20,
            reasoning: 0,
            cache_read: 0,
            cache_write: 0,
            cost: 0.5,
        };
        t.note(tok("m1", 100));
        let again = t.note(tok("m1", 100));
        match &again.events[0] {
            AgentEvent::TaskProgress { usage: Some(u), .. } => {
                assert_eq!(u.input_tokens, Some(100), "one message, counted once");
                assert_eq!(u.total_tokens, 120);
            }
            e => panic!("{e:?}"),
        }
    }

    #[test]
    fn a_failed_load_refuses_the_child_and_lets_its_permission_be_asked_from_the_bridge() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_c", "ses_owner"));
        t.task_call("call_c", "c", false, None);
        let refused = t.load_result("ses_c", Some("not found"));
        assert!(
            matches!(&refused[0], AgentEvent::CommandRejected { command, .. } if command == "session/load")
        );
        let fx = t.note(Note::PermissionAsked {
            id: "per_1".into(),
            session: "ses_c".into(),
            permission: "bash".into(),
        });
        assert!(
            matches!(&fx.events[0], AgentEvent::GateRequested { gate: GateRequest::Permission { request_id, choices, .. } }
            if request_id == "per_1" && choices.len() == 3)
        );
        assert_eq!(
            t.answer_gate("per_1", &GateDecision::choose("always")),
            Some(Ok(Command::ReplyPermission {
                id: "per_1".into(),
                reply: Reply::Always
            }))
        );
        assert!(
            t.answer_gate("per_1", &GateDecision::choose("once"))
                .is_none(),
            "answered once"
        );
    }

    #[test]
    fn a_loaded_child_permission_is_left_to_acp() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_c", "ses_owner"));
        t.load_result("ses_c", None);
        let fx = t.note(Note::PermissionAsked {
            id: "per_1".into(),
            session: "ses_c".into(),
            permission: "bash".into(),
        });
        assert!(fx.events.is_empty());
    }

    #[test]
    fn steer_needs_a_running_subagent_and_text() {
        let mut t = Tasks::default();
        t.set_parent("ses_owner");
        t.note(created("ses_c", "ses_owner"));
        assert!(matches!(
            t.steer("ses_c", "x"),
            Err(DriverError::NotFound { .. })
        ));
        t.task_call("call_c", "c", true, None);
        assert_eq!(
            t.steer("ses_c", "use the other file"),
            Ok(Command::Steer {
                session: "ses_c".into(),
                text: "use the other file".into()
            })
        );
        assert!(matches!(
            t.steer("ses_c", "  "),
            Err(DriverError::Invalid { .. })
        ));
    }
}
