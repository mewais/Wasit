//! The tasks an agent runs, reduced from the task events of the event model.
//!
//! A *task* is a unit of work the CLI tracks by id: a subagent, a shell command. Some run in the
//! *background*: they outlive the turn that started them (claude backgrounds subagents and shell
//! commands even when told to wait; their results come back as turns the CLI starts by itself).
//! The session is working while any background task runs, whether or not a turn is open.
//!
//! The one reduction of these events lives here, so the runtime's snapshot and every consumer
//! that follows the events themselves (Wasl waiting for a receiver to finish) read the same
//! state from the same rules. It is pure and deterministic: no clock (times are passed in).
//!
//! The rules:
//!
//! * [`AgentEvent::TaskStarted`] adds a running task (or completes one created earlier from
//!   [`AgentEvent::BackgroundTasksChanged`], which can come first);
//! * [`AgentEvent::TaskProgress`] / [`AgentEvent::TaskUpdated`] refresh a task's facts and never
//!   end it;
//! * [`AgentEvent::TaskEnded`] ends it, with the agent's status word;
//! * [`AgentEvent::BackgroundTasksChanged`] is the agent's own list of its background tasks: a
//!   listed task is running in the background; a running background task the agent no longer
//!   lists has ended (status [`UNLISTED`]) — the agent is the authority, so a missed end frame
//!   can never leave a session working forever;
//! * a task that does not run in the background cannot outlive its turn: [`end_foreground`]
//!   closes the ones still running when the turn ends;
//! * [`end_all`] closes everything when the process ends.

use serde::{Deserialize, Serialize};

use crate::event::{AgentEvent, TaskUsage};

/// The status of a background task that the agent stopped listing without reporting its end.
pub const UNLISTED: &str = "unlisted";
/// The status of a foreground task closed because its turn ended.
pub const TURN_ENDED: &str = "turn_ended";
/// The status of a task closed because the process ended.
pub const PROCESS_ENDED: &str = "process_ended";

/// How many tasks are remembered; the oldest ended ones go first.
pub const MAX_TASKS: usize = 100;

/// Cap on a task's description.
const DESCRIPTION_CHARS: usize = 120;

/// One task, as the session's snapshot shows it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct TaskInfo {
    pub task_id: String,
    /// The agent's word for the kind of work (claude `local_bash`, `local_agent`).
    pub kind: String,
    /// The tool call that spawned it, when the agent says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subagent_type: Option<String>,
    /// It runs in the background: it can outlive the turn that started it.
    pub background: bool,
    pub started_at_ms: u64,
    /// Not ended yet.
    pub running: bool,
    /// The agent's latest status word for it (`completed`, `killed`, `stopped`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// How it ended, once it has: `Some(true)` only when the agent reported success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_ms: Option<u64>,
    /// The tool it ran last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tool: Option<String>,
    /// What the agent says it used so far.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TaskUsage>,
}

/// What an event did to the set of running background tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundChange {
    /// Nothing changed there.
    None,
    /// A background task started running (the first id of them).
    Started,
    /// A background task stopped running.
    Ended,
}

/// How many background tasks are running.
pub fn background_running(tasks: &[TaskInfo]) -> u32 {
    tasks.iter().filter(|t| t.running && t.background).count() as u32
}

fn truncate(s: &str) -> String {
    match s.char_indices().nth(DESCRIPTION_CHARS) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

fn find<'a>(tasks: &'a mut [TaskInfo], id: &str) -> Option<&'a mut TaskInfo> {
    tasks.iter_mut().find(|t| t.task_id == id)
}

/// The task `id`, added as a running foreground task when it is not known yet.
fn entry<'a>(tasks: &'a mut Vec<TaskInfo>, id: &str, kind: &str, t: u64) -> &'a mut TaskInfo {
    if let Some(i) = tasks.iter().position(|x| x.task_id == id) {
        return &mut tasks[i];
    }
    tasks.push(TaskInfo {
        task_id: id.to_string(),
        kind: kind.to_string(),
        call_id: None,
        run_id: None,
        description: String::new(),
        subagent_type: None,
        background: false,
        started_at_ms: t,
        running: true,
        status: None,
        ok: None,
        ended_at_ms: None,
        last_tool: None,
        usage: None,
    });
    if tasks.len() > MAX_TASKS {
        // The oldest task that is not running; only when all run, the oldest.
        let drop = tasks.iter().position(|x| !x.running).unwrap_or(0);
        tasks.remove(drop);
    }
    let last = tasks.len() - 1;
    &mut tasks[last]
}

/// Fold one event into `tasks` (`t`: the time of the event). Reports what it did to the running
/// background tasks.
pub fn apply(tasks: &mut Vec<TaskInfo>, event: &AgentEvent, t: u64) -> BackgroundChange {
    let before = background_running(tasks);
    match event {
        AgentEvent::TaskStarted {
            task_id,
            kind,
            call_id,
            description,
            subagent_type,
            background,
            run_id,
        } => {
            let task = entry(tasks, task_id, kind, t);
            task.kind.clone_from(kind);
            task.call_id = call_id.clone().or(task.call_id.take());
            task.run_id = run_id.clone().or(task.run_id.take());
            if !description.is_empty() {
                task.description = truncate(description);
            }
            task.subagent_type = subagent_type.clone().or(task.subagent_type.take());
            // A list that named it a background task first stays right.
            task.background |= *background;
        }
        AgentEvent::TaskProgress {
            task_id,
            call_id,
            description,
            last_tool,
            usage,
        } => {
            let task = entry(tasks, task_id, "", t);
            task.call_id = call_id.clone().or(task.call_id.take());
            if !description.is_empty() {
                task.description = truncate(description);
            }
            task.last_tool = last_tool.clone().or(task.last_tool.take());
            task.usage = usage.or(task.usage);
        }
        AgentEvent::TaskUpdated { task_id, status } => {
            if let Some(task) = find(tasks, task_id) {
                if status.is_some() {
                    task.status.clone_from(status);
                }
            }
        }
        AgentEvent::TaskEnded {
            task_id,
            call_id,
            status,
            ok,
            usage,
            ..
        } => {
            let task = entry(tasks, task_id, "", t);
            task.call_id = call_id.clone().or(task.call_id.take());
            task.running = false;
            task.status = Some(status.clone());
            task.ok = Some(*ok);
            task.ended_at_ms = Some(t);
            task.usage = usage.or(task.usage);
        }
        AgentEvent::BackgroundTasksChanged { tasks: listed } => {
            for l in listed {
                let task = entry(tasks, &l.task_id, &l.kind, t);
                if !task.running {
                    // Ended already (its end frame came first): a list does not revive it.
                    continue;
                }
                task.background = true;
                if task.kind.is_empty() {
                    task.kind.clone_from(&l.kind);
                }
                if task.description.is_empty() {
                    task.description = truncate(&l.description);
                }
                task.subagent_type = l.subagent_type.clone().or(task.subagent_type.take());
                task.run_id = l.run_id.clone().or(task.run_id.take());
            }
            for task in tasks.iter_mut() {
                if task.running
                    && task.background
                    && !listed.iter().any(|l| l.task_id == task.task_id)
                {
                    task.running = false;
                    task.status.get_or_insert_with(|| UNLISTED.to_string());
                    task.ended_at_ms = Some(t);
                }
            }
        }
        _ => {}
    }
    let after = background_running(tasks);
    match after.cmp(&before) {
        std::cmp::Ordering::Greater => BackgroundChange::Started,
        std::cmp::Ordering::Less => BackgroundChange::Ended,
        std::cmp::Ordering::Equal => BackgroundChange::None,
    }
}

/// The turn ended: tasks that do not run in the background end with it.
pub fn end_foreground(tasks: &mut [TaskInfo], t: u64) {
    for task in tasks.iter_mut().filter(|x| x.running && !x.background) {
        task.running = false;
        task.status.get_or_insert_with(|| TURN_ENDED.to_string());
        task.ended_at_ms = Some(t);
    }
}

/// The process ended: nothing runs any more.
pub fn end_all(tasks: &mut [TaskInfo], t: u64) {
    for task in tasks.iter_mut().filter(|x| x.running) {
        task.running = false;
        task.status.get_or_insert_with(|| PROCESS_ENDED.to_string());
        task.ended_at_ms = Some(t);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::BackgroundTask;

    fn started(id: &str, background: bool) -> AgentEvent {
        AgentEvent::TaskStarted {
            task_id: id.into(),
            kind: "local_bash".into(),
            call_id: Some(format!("call-{id}")),
            description: "d".into(),
            subagent_type: None,
            background,
            run_id: Some(format!("run-{id}")),
        }
    }

    fn ended(id: &str, status: &str) -> AgentEvent {
        AgentEvent::TaskEnded {
            task_id: id.into(),
            call_id: Some(format!("call-{id}")),
            status: status.into(),
            ok: status == "completed",
            summary: None,
            usage: None,
        }
    }

    fn listed(ids: &[&str]) -> AgentEvent {
        AgentEvent::BackgroundTasksChanged {
            tasks: ids
                .iter()
                .map(|id| BackgroundTask {
                    task_id: (*id).into(),
                    kind: "local_bash".into(),
                    description: "listed".into(),
                    subagent_type: None,
                    run_id: Some(format!("run-{id}")),
                })
                .collect(),
        }
    }

    /// The order the real CLI printed: the list first, then `task_started`, ..., the end frame,
    /// then the empty list.
    #[test]
    fn a_background_task_runs_from_its_start_to_its_end_frame() {
        let mut t = Vec::new();
        assert_eq!(apply(&mut t, &listed(&["a"]), 1), BackgroundChange::Started);
        assert_eq!(
            apply(&mut t, &started("a", true), 2),
            BackgroundChange::None
        );
        assert_eq!(background_running(&t), 1);
        assert_eq!(t[0].call_id.as_deref(), Some("call-a"));
        assert_eq!(t[0].started_at_ms, 1, "created by the list");
        apply(
            &mut t,
            &AgentEvent::TaskUpdated {
                task_id: "a".into(),
                status: Some("completed".into()),
            },
            3,
        );
        assert_eq!(background_running(&t), 1, "an update does not end it");
        assert_eq!(
            apply(&mut t, &ended("a", "completed"), 4),
            BackgroundChange::Ended
        );
        assert_eq!(apply(&mut t, &listed(&[]), 5), BackgroundChange::None);
        assert_eq!(
            (t[0].running, t[0].ok, t[0].ended_at_ms),
            (false, Some(true), Some(4))
        );
        assert_eq!(t[0].status.as_deref(), Some("completed"));
        // A stale list does not revive it.
        apply(&mut t, &listed(&["a"]), 6);
        assert_eq!(background_running(&t), 0);
    }

    #[test]
    fn the_agents_list_is_the_authority_for_background_work() {
        let mut t = Vec::new();
        apply(&mut t, &started("a", true), 1);
        apply(&mut t, &started("b", true), 2);
        assert_eq!(background_running(&t), 2);
        // `a` is no longer listed and no end frame was seen: it ended.
        assert_eq!(apply(&mut t, &listed(&["b"]), 3), BackgroundChange::Ended);
        assert_eq!(background_running(&t), 1);
        assert_eq!(t[0].status.as_deref(), Some(UNLISTED));
        assert_eq!(t[0].ok, None);
    }

    #[test]
    fn foreground_tasks_end_with_their_turn_and_background_ones_do_not() {
        let mut t = Vec::new();
        apply(&mut t, &started("fg", false), 1);
        apply(&mut t, &started("bg", true), 2);
        assert_eq!(
            background_running(&t),
            1,
            "a foreground task is not background work"
        );
        end_foreground(&mut t, 3);
        assert!(!t[0].running && t[1].running);
        assert_eq!(t[0].status.as_deref(), Some(TURN_ENDED));
        end_all(&mut t, 4);
        assert!(t.iter().all(|x| !x.running));
        assert_eq!(t[1].status.as_deref(), Some(PROCESS_ENDED));
    }

    #[test]
    fn a_progress_frame_for_an_unknown_task_is_kept_as_a_running_foreground_task() {
        let mut t = Vec::new();
        apply(
            &mut t,
            &AgentEvent::TaskProgress {
                task_id: "x".into(),
                call_id: None,
                description: "working".into(),
                last_tool: Some("Bash".into()),
                usage: Some(TaskUsage {
                    total_tokens: 5,
                    tool_uses: 1,
                    duration_ms: 9,
                    ..Default::default()
                }),
            },
            7,
        );
        assert_eq!(t.len(), 1);
        assert!(t[0].running && !t[0].background);
        assert_eq!(t[0].last_tool.as_deref(), Some("Bash"));
        assert_eq!(t[0].usage.map(|u| u.total_tokens), Some(5));
    }

    #[test]
    fn the_table_is_bounded_and_forgets_ended_tasks_first() {
        let mut t = Vec::new();
        apply(&mut t, &started("keep", true), 0);
        for i in 0..MAX_TASKS {
            let id = format!("t{i}");
            apply(&mut t, &started(&id, false), i as u64);
            apply(&mut t, &ended(&id, "completed"), i as u64);
        }
        assert_eq!(t.len(), MAX_TASKS);
        assert!(t.iter().any(|x| x.task_id == "keep" && x.running));
    }
}
