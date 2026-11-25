//! The pure, deterministic heart of a session: journal records in, session events, stdin writes
//! and the reduced [`SessionSnapshot`] out. No I/O, no clock (times come from the records).
//!
//! The live [`crate::actor`] and every offline replay (history pages, old-cursor subscriptions,
//! snapshots of exited sessions) run the same [`Core`] over the same records, which is what
//! makes replay exact.
//!
//! # Replay exactness
//!
//! Protocol state is a function of `(start spec, lines read, commands encoded)` in order. The
//! journal holds all three, in one total order:
//!
//! * `out` records are fed to `Protocol::on_line`;
//! * runtime notes (see [`crate::note`]) carry the start spec (`start`), every command
//!   (`command`), stops (`stop`) — each journaled *before* the lines it causes are written;
//!   the core re-encodes them instead of trusting the `in` lines that follow;
//! * `in` records are only used to know which requested writes have reached the CLI: every line
//!   the protocol asks to write is appended to an *owed* queue, and an `in` record with the same
//!   text pops it. Whatever is still owed when a controller reaches the live end of the journal
//!   was never written (the previous daemon died first) and is written then; lines produced while
//!   replaying history that do have an `in` echo are never written twice.
//!
//! # Inputs
//!
//! Every journaled command is an *input* whose id ([`InputId`]) is the seq of the note that
//! carried it; a session's first prompt travels inside the generation's `start` note and is the
//! first input of that generation, applied exactly like a later `command` note (same `input`
//! event, same `encode`, same queueing). The protocol reports what happens to a queued input
//! (`input_sent` / `input_dropped`) and what the CLI says about it (`input_acknowledged`,
//! `turn_started.input`); the core adds what only the journal knows: `written` once every line
//! the input asked for has its `in` echo, `write_failed` from the shim's `stdin_error` note, and
//! `input_dropped{process_ended}` for inputs still queued or unwritten when the process
//! generation ends.
//!
//! Lines the CLI prints after a `spawned` record but before the generation's `start` note, and
//! commands journaled in that window, are buffered and applied right after `start` — in live
//! operation and in replay alike.
//!
//! # Earlier history
//!
//! The earlier messages of a conversation the session resumed ([`wasit_core::AgentEvent::History`]:
//! replayed by the agent over its protocol, or imported from its storage in `earlier_chunk`
//! notes) never reach the reducer or the live event stream: they come out of [`Core::apply`] as
//! [`Output::earlier`], with cursors of the record they were journaled in, and what the session
//! knows of their loading is `snapshot.earlier`. The rule for when history is missing is in
//! [`Core::earlier_on_start`].

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use wasit_control::TraceId;
use wasit_core::tasks::{self, BackgroundChange};
use wasit_core::{
    AgentEvent, Command, DriverCapabilities, DriverDescriptor, DriverError, Encoded, GateReason,
    InputDropReason, InputId, LocalShellOrigin, Protocol, ResumeMode, SavesConversation, Scope,
    SpawnSpec, Step, TurnBinding,
};
use wasit_shim::{Kind, Record};

use crate::note::{classify, EarlierPlan, NoteKind, RuntimeNote};
use crate::wire::{
    event_cursor, synthetic_cursor, AnomalyKind, CommandStatus, EarlierGaps, EarlierHistory,
    EarlierSource, EarlierState, EarlierUnavailable, ExitSummary, GateClearing, NotContinuable,
    NoticeCode, PendingGate, Phase, PhaseCause, ResumeCommand, SessionEvent, SessionEventKind,
    SessionSnapshot, StartResume, SubagentInfo, TurnInfo, TurnOutcome,
};

/// Bounded diagnostics.
const STDERR_TAIL_LINES: usize = 40;
const STDERR_LINE_CHARS: usize = 500;
const FAILED_TAIL_LINES: usize = 10;
const TITLE_CHARS: usize = 120;
const REPLY_CHARS: usize = 2000;
const MAX_SUBAGENTS: usize = 100;
/// Cap on owed (written-but-not-echoed) lines; beyond it the oldest are forgotten (and count as
/// written).
const MAX_OWED: usize = 4096;
/// Cap on inputs remembered as queued in the protocol or waiting for their lines' echoes.
const MAX_OPEN_INPUTS: usize = 4096;
/// Cap on prompt texts kept for the turn that will run them (`TurnInfo::prompt`).
const MAX_PROMPT_TEXTS: usize = 64;

/// What the runtime knows about the agent's own conversation of a session: its id, and whether
/// the agent's storage holds the conversation. Derived only from the session's creation and the
/// journal (the `start` notes and the events), so a replay reaches the same answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Conversation {
    /// No id is known: the agent reports its own, and has not yet.
    Unknown,
    /// The runtime chose this id for a conversation that is not started yet: starting fresh under
    /// it works, resuming it does not.
    Chosen(String),
    /// The agent reported this id but has not saved the conversation yet
    /// ([`SavesConversation::OnFirstTurn`]): it can be neither resumed nor started again.
    Unsaved(String),
    /// The agent's storage holds the conversation: it can be resumed.
    Saved(String),
}

impl Conversation {
    pub(crate) fn id(&self) -> Option<&str> {
        match self {
            Conversation::Unknown => None,
            Conversation::Chosen(id) | Conversation::Unsaved(id) | Conversation::Saved(id) => {
                Some(id)
            }
        }
    }

    /// What a spec says before the agent has said anything: a resumed conversation exists, a
    /// chosen one does not yet.
    pub(crate) fn of_spec(spec: &SpawnSpec) -> Conversation {
        match (&spec.resume, &spec.native_session_id) {
            (ResumeMode::Resume(id), _) => Conversation::Saved(id.clone()),
            (_, Some(id)) => Conversation::Chosen(id.clone()),
            _ => Conversation::Unknown,
        }
    }
}

/// A fact of an event about the agent's storage of the conversation ([`Core::reduce`]).
enum SavedFact {
    /// The agent reported the conversation's id.
    Reported(String),
    /// A turn started.
    TurnStarted,
}
use SavedFact::{Reported, TurnStarted};

/// How to start the next process generation of an ended session ([`Core::continue_plan`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContinuePlan {
    /// The agent holds the conversation: resume it.
    Resume(String),
    /// It does not hold it yet: start it fresh under the id chosen for it.
    StartFresh(String),
}

/// Whether a session can be continued, from the facts alone: the one place the rule lives.
pub(crate) fn continue_verdict(
    caps: &DriverCapabilities,
    phase: &Phase,
    loaded: bool,
    conversation: &Conversation,
) -> Result<(), NotContinuable> {
    if !loaded {
        return Err(NotContinuable::Loading);
    }
    if phase.is_live() {
        return Err(NotContinuable::NotEnded);
    }
    if !caps.resume {
        return Err(NotContinuable::AgentCannotResume);
    }
    match conversation {
        Conversation::Unknown => Err(NotContinuable::NoConversationId),
        Conversation::Unsaved(_) => Err(NotContinuable::NothingSaved),
        Conversation::Chosen(_) | Conversation::Saved(_) => Ok(()),
    }
}

/// Fills the snapshot's conversation facts (id, `continuable`, `resume_command`) from `conversation`, the
/// snapshot's phase and `desc`.
fn apply_conversation(snap: &mut SessionSnapshot, desc: &dyn DriverDescriptor, c: &Conversation) {
    if let Some(id) = c.id() {
        if snap.native_session_id.as_deref() != Some(id) {
            snap.native_session_id = Some(id.to_string());
        }
    }
    let verdict = continue_verdict(&snap.capabilities, &snap.phase, snap.loaded, c);
    snap.continuable = verdict.is_ok();
    snap.not_continuable = verdict.err();
    snap.resume_command = match (c, &snap.cwd) {
        (Conversation::Saved(id), Some(cwd)) => {
            desc.user_resume_argv(id).map(|argv| ResumeCommand {
                argv,
                cwd: cwd.clone(),
            })
        }
        _ => None,
    };
}

/// Static facts about a session.
#[derive(Debug, Clone)]
pub(crate) struct SessionMeta {
    pub id: crate::wire::SessionId,
    pub driver: wasit_core::DriverId,
    pub cwd: Option<String>,
    pub labels: std::collections::BTreeMap<String, String>,
    pub created_at_ms: u64,
    pub display_name: Option<String>,
    /// The command the CLI is started as instead of the driver's default binary.
    pub command: Option<String>,
    /// What the session's spec says about the agent's conversation before the agent says
    /// anything ([`Conversation::of_spec`]).
    pub conversation: Conversation,
    /// The session this one was forked from (see [`crate::wire::ForkedFrom`]).
    pub forked_from: Option<crate::wire::ForkedFrom>,
}

/// An empty snapshot for `meta` in `phase`.
pub(crate) fn blank_snapshot(
    meta: &SessionMeta,
    desc: &dyn DriverDescriptor,
    phase: Phase,
    loaded: bool,
) -> SessionSnapshot {
    let caps = desc.capabilities();
    let mut snap = SessionSnapshot {
        id: meta.id.clone(),
        driver: meta.driver.clone(),
        cwd: meta.cwd.clone(),
        labels: meta.labels.clone(),
        created_at_ms: meta.created_at_ms,
        command: meta.command.clone(),
        capabilities: caps,
        prompt_input: desc.prompt_input(),
        phase,
        generation: 0,
        pid: None,
        native_session_id: None,
        continuable: false,
        not_continuable: None,
        resume_command: None,
        title: None,
        display_name: meta.display_name.clone(),
        forked_from: meta.forked_from.clone(),
        cli_version: None,
        model: None,
        mode: None,
        available_models: Vec::new(),
        available_modes: Vec::new(),
        slash_commands: Vec::new(),
        pending_gates: Vec::new(),
        turn: None,
        last_turn: None,
        turns_completed: 0,
        usage: Default::default(),
        usage_by_model: Default::default(),
        live_usage: None,
        context_pct: None,
        subagents: Vec::new(),
        tasks: Vec::new(),
        background_running: 0,
        plan: Vec::new(),
        last_activity_ms: meta.created_at_ms,
        exit: None,
        stderr_tail: Vec::new(),
        cursor: 0,
        loaded,
        earlier: EarlierHistory::default(),
    };
    apply_conversation(&mut snap, desc, &meta.conversation);
    snap
}

/// Something received before the generation's `start` note.
enum Buffered {
    Line(String),
    Command {
        token: String,
        input: InputId,
        command: Command,
        trace: Option<TraceId>,
    },
    Stop {
        input: InputId,
    },
}

/// A line the protocol asked to write, until its `in` echo.
struct Owed {
    line: String,
    /// The inputs whose step asked for it (an input is written once none of its lines is owed).
    inputs: Vec<InputId>,
}

enum Gen {
    /// No process generation yet.
    None,
    /// `spawned` seen, `start` note not yet.
    Awaiting(Vec<Buffered>),
    Running(Box<dyn Protocol>),
    Exited,
}

/// What applying one record produced.
#[derive(Debug, Default)]
pub(crate) struct Output {
    pub events: Vec<SessionEvent>,
    /// Lines the protocol asked to write (already recorded as owed).
    pub writes: Vec<String>,
    pub close_stdin: bool,
    /// Results of journaled commands / stops, by the requester's token.
    pub resolved: Vec<(String, CommandStatus)>,
    /// An `attach` note with this token.
    pub attach: Option<String>,
    /// A `start` note was applied (the handshake deadline starts).
    pub started: bool,
    /// A `stop` note was applied (graceful-stop escalation starts).
    pub stop_applied: bool,
    pub spawned: bool,
    pub exited: bool,
    /// Inline media moved out of events: `(blob id, bytes)` for the blob store.
    pub blobs: Vec<(String, Vec<u8>)>,
    /// Items of the conversation's earlier history this record brought (each an
    /// `AgentEvent::History`, with its cursor). They are not part of `events`: the live stream,
    /// the in-memory tail and the trail never see them; they are paged from the journal
    /// (`session.history {earlier: true}`).
    pub earlier: Vec<SessionEvent>,
    /// How gates changed with this record (see [`GateFact`]).
    pub gate_facts: Vec<GateFact>,
}

/// A change to a gate that the runtime's diagnostics report (`crate::diag`). Facts are made
/// while a record is applied, so a replay makes the same ones: the actor reports them only for
/// a record it applies live ([`Output::gate_facts`]). They carry ids, words and times, never
/// the gate's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GateFact {
    /// A gate was raised: its kind and the turn binding it asked for (`turn`, `interrupt` or
    /// `none`).
    Raised {
        request_id: String,
        kind: &'static str,
        bound_to: &'static str,
    },
    /// The gate was answered `ms` after it was raised.
    Answered { request_id: String, ms: u64 },
    /// An external gate was taken down with `reason` (its code).
    Withdrawn { request_id: String, reason: String },
    /// An answer to the gate was refused: `reason` is `invalid_choice`, `withdrawn`,
    /// `not_running`, `unsupported` or `rejected` (the protocol refused it).
    AnswerRejected {
        request_id: String,
        reason: &'static str,
    },
}

/// The request id of an answer to a gate, `None` for any other command.
pub(crate) fn answer_request_id(command: &Command) -> Option<&str> {
    match command {
        Command::AnswerGate { request_id, .. } | Command::AnswerQuestion { request_id, .. } => {
            Some(request_id)
        }
        _ => None,
    }
}

fn gate_kind(gate: &wasit_core::GateRequest) -> &'static str {
    use wasit_core::GateRequest;
    match gate {
        GateRequest::Permission { .. } => "permission",
        GateRequest::Plan { .. } => "plan",
        GateRequest::Question { .. } => "question",
        GateRequest::Handoff { .. } => "handoff",
        GateRequest::Spawn { .. } => "spawn",
    }
}

fn binding_word(binding: Option<&TurnBinding>) -> &'static str {
    match binding {
        None => "none",
        Some(TurnBinding::Interrupt { .. }) => "interrupt",
        Some(TurnBinding::Turn { .. }) => "turn",
    }
}

/// What the user is told when an answer arrives after its gate was withdrawn: the reason's
/// detail for people, else its code.
fn not_delivered_message(reason: &GateReason) -> String {
    let why = reason.detail.as_deref().unwrap_or(&reason.code);
    format!("your answer arrived after the request was withdrawn ({why}) and was not delivered")
}

struct Emitter {
    seq: u64,
    t: u64,
    /// Events and earlier-history items of one record share the numbering of their cursors.
    next: usize,
    events: Vec<SessionEvent>,
    earlier: Vec<SessionEvent>,
}

impl Emitter {
    fn new(seq: u64, t: u64) -> Self {
        Emitter {
            seq,
            t,
            next: 0,
            events: Vec::new(),
            earlier: Vec::new(),
        }
    }

    fn next_cursor(&self) -> u64 {
        event_cursor(self.seq, self.next)
    }

    fn push(&mut self, kind: SessionEventKind) {
        let cursor = self.next_cursor();
        self.next += 1;
        self.events.push(SessionEvent {
            cursor,
            t: self.t,
            kind,
        });
    }

    /// An item of the earlier history: only room for as many as a record's cursors can number.
    fn push_earlier(&mut self, event: AgentEvent) -> bool {
        if self.next >= EARLIER_ITEMS_PER_RECORD {
            return false;
        }
        let cursor = self.next_cursor();
        self.next += 1;
        self.earlier.push(SessionEvent {
            cursor,
            t: self.t,
            kind: SessionEventKind::Agent { event },
        });
        true
    }
}

/// Most earlier-history items one journal record can number (a record's events share 16 bits of
/// cursor); what a protocol replays beyond it is counted as omitted.
const EARLIER_ITEMS_PER_RECORD: usize = 0xFF00;

/// The core's part of the loading of the conversation's earlier history: what the snapshot shows
/// and the facts the next record is judged by (all of them derived from the journal).
#[derive(Debug, Clone, Default)]
struct EarlierCore {
    info: EarlierHistory,
    /// The session's first `start` note decided what the conversation needs.
    decided: bool,
    /// Nothing more is expected: the history is complete, was given up on, or none is needed.
    closed: bool,
    /// Items of the agent's replay seen in this process generation (the replay of a reloaded
    /// conversation starts from its beginning each time).
    replay_pos: u64,
    /// Items accepted from replays so far: a later replay's first `accepted` items are the
    /// ones the journal already holds.
    accepted: u64,
    /// The replay of this generation was ignored already (counted once).
    replay_ignored: bool,
    /// The import of the agent's file, when the plan is one: its path and the length it reads.
    import: Option<(String, u64)>,
    /// History items of the import in the journal: the next chunk starts with this number.
    imported: u64,
}

/// What the actor does to bring the earlier history of the agent's file into the journal
/// ([`Core::earlier_work`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ImportWork {
    pub path: String,
    pub len: u64,
    /// The history items before this number are journaled already.
    pub skip: u64,
}

/// How a turn's gates are being taken down ([`Core::end_turn_gates`]).
#[derive(Clone, Copy)]
enum TurnEnd {
    Interrupted,
    Ended,
}

/// See the module docs.
pub(crate) struct Core {
    desc: Arc<dyn DriverDescriptor>,
    snap: SessionSnapshot,
    /// The agent's conversation (see [`Conversation`]); `snap` shows it.
    conversation: Conversation,
    saves: SavesConversation,
    gen: Gen,
    owed: VecDeque<Owed>,
    /// Inputs some of whose lines are owed, oldest first.
    writing: VecDeque<InputId>,
    /// Inputs of the last echoed line (a `stdin_error` note right behind it is theirs).
    last_echoed: Vec<InputId>,
    /// Inputs the protocol holds (`Queued`, not yet sent or dropped), oldest first.
    queued: VecDeque<InputId>,
    close_owed: bool,
    last_seq: u64,
    partial: HashMap<Kind, String>,
    handshake_done: bool,
    stop_requested: bool,
    handshake_timeout: Option<u64>,
    start_t: Option<u64>,
    lost: Option<String>,
    exit_phase: Option<Phase>,
    /// Texts of accepted prompts / slash commands, by input, until a turn starts for them.
    prompt_texts: VecDeque<(InputId, String)>,
    /// Facts of the record being applied that can change the phase (see [`Core::cause_for`]).
    causes: Vec<PhaseCause>,
    /// An `activity_while_idle` anomaly was reported since the last phase change.
    anomaly_reported: bool,
    stderr_tail: VecDeque<String>,
    /// Pending gates raised from outside the CLI protocol (see [`RuntimeNote::RaiseGate`]), by
    /// request id, with the binding to the open turn that still applies to them. They are
    /// answered without the protocol and end with the process generation; a gate with a binding
    /// also ends with its turn ([`Core::end_turn_gates`]), one without lives on across turns.
    /// Invariant: every bound gate belongs to the turn open now.
    external: BTreeMap<String, Option<TurnBinding>>,
    /// An interrupt was applied to the turn open now (the CLI has not ended it yet).
    interrupt_applied: bool,
    /// External gates that were withdrawn or ended with their process, with the reason (newest
    /// last, bounded): a late answer to one is refused as [`CommandStatus::Withdrawn`].
    withdrawn: VecDeque<(String, GateReason)>,
    /// Gate changes of the record being applied, drained into its [`Output`].
    gate_facts: Vec<GateFact>,
    /// The conversation's earlier history (see [`EarlierCore`]).
    earlier: EarlierCore,
}

/// How many withdrawn external gate ids are remembered for refusing late answers.
const WITHDRAWN_MEMORY: usize = 256;

fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

fn push_bounded<T>(q: &mut VecDeque<T>, item: T, max: usize) {
    q.push_back(item);
    while q.len() > max {
        q.pop_front();
    }
}

/// Remove and return the text of `input`.
fn take_text(texts: &mut VecDeque<(InputId, String)>, input: InputId) -> Option<String> {
    let i = texts.iter().position(|(id, _)| *id == input)?;
    texts.remove(i).map(|(_, t)| t)
}

/// The event type of agent activity (output that means the agent is working).
fn activity(e: &AgentEvent) -> Option<&'static str> {
    // A subagent's activity is activity too: it is the phase (`working` while its task runs)
    // that says whether it is explained.
    Some(match e.unscoped().1 {
        AgentEvent::AssistantDelta { .. } => "assistant_delta",
        AgentEvent::AssistantContent { .. } => "assistant_content",
        AgentEvent::ThinkingDelta { .. } => "thinking_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallUpdated { .. } => "tool_call_updated",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
        _ => return None,
    })
}

/// The ids of the tasks running in the background.
fn running_background_ids(tasks: &[wasit_core::TaskInfo]) -> Vec<String> {
    tasks
        .iter()
        .filter(|t| t.running && t.background)
        .map(|t| t.task_id.clone())
        .collect()
}

fn push_subagent(
    subagents: &mut Vec<SubagentInfo>,
    call_id: &str,
    name: &str,
    input: &serde_json::Value,
    model: &Option<String>,
    t: u64,
) {
    subagents.push(SubagentInfo {
        call_id: call_id.to_string(),
        name: name.to_string(),
        model: model.clone(),
        description: input
            .get("description")
            .and_then(|d| d.as_str())
            .map(|d| truncate(d, TITLE_CHARS))
            .unwrap_or_default(),
        started_at_ms: t,
        running: true,
        ok: None,
        task_id: None,
        agent_id: None,
        subagent_type: input
            .get("subagent_type")
            .and_then(|d| d.as_str())
            .map(str::to_string),
        status: None,
        ended_at_ms: None,
        tool_calls: 0,
        last_tool: None,
        usage: None,
    });
    if subagents.len() > MAX_SUBAGENTS {
        if let Some(i) = subagents.iter().position(|a| !a.running) {
            subagents.remove(i);
        } else {
            subagents.remove(0);
        }
    }
}

/// The tool call that started subagent `call_id` returned. For a subagent the agent reports a
/// task for, that is not its end (a backgrounded one returns at once): the task's end is.
fn finish_subagent_call(subagents: &mut [SubagentInfo], call_id: &str, ok: bool, t: u64) {
    if let Some(a) = subagents.iter_mut().find(|a| a.call_id == call_id) {
        if a.task_id.is_none() {
            a.running = false;
            a.ok = Some(ok);
            a.ended_at_ms = Some(t);
        }
    }
}

/// Make every subagent show what the agent's task for it says: the link is the spawning call
/// (`TaskInfo::call_id`), and the task is the authority for whether it runs.
fn sync_subagents(subagents: &mut [SubagentInfo], tasks: &[wasit_core::TaskInfo]) {
    for a in subagents.iter_mut() {
        let task = match &a.task_id {
            Some(id) => tasks.iter().find(|t| &t.task_id == id),
            None => tasks
                .iter()
                .find(|t| t.call_id.as_deref() == Some(a.call_id.as_str())),
        };
        let Some(task) = task else { continue };
        a.task_id = Some(task.task_id.clone());
        a.running = task.running;
        a.status.clone_from(&task.status);
        a.ok = if task.running { None } else { task.ok };
        a.ended_at_ms = task.ended_at_ms;
        if task.subagent_type.is_some() {
            a.subagent_type.clone_from(&task.subagent_type);
        }
        if task.last_tool.is_some() {
            a.last_tool.clone_from(&task.last_tool);
        }
        a.usage = task.usage;
    }
}

impl Core {
    /// Modes to offer until (unless) the CLI reports its own list.
    pub(crate) fn hint_modes(&mut self, modes: Vec<wasit_core::SessionMode>) {
        if self.snap.available_modes.is_empty() {
            self.snap.available_modes = modes;
        }
    }

    pub(crate) fn new(desc: Arc<dyn DriverDescriptor>, meta: &SessionMeta) -> Self {
        let snap = blank_snapshot(meta, desc.as_ref(), Phase::Starting, true);
        Core {
            saves: desc.saves_conversation(),
            conversation: meta.conversation.clone(),
            desc,
            snap,
            gen: Gen::None,
            owed: VecDeque::new(),
            writing: VecDeque::new(),
            last_echoed: Vec::new(),
            queued: VecDeque::new(),
            close_owed: false,
            last_seq: 0,
            partial: HashMap::new(),
            handshake_done: false,
            stop_requested: false,
            handshake_timeout: None,
            start_t: None,
            lost: None,
            exit_phase: None,
            prompt_texts: VecDeque::new(),
            causes: Vec::new(),
            anomaly_reported: false,
            stderr_tail: VecDeque::new(),
            external: BTreeMap::new(),
            interrupt_applied: false,
            withdrawn: VecDeque::new(),
            gate_facts: Vec::new(),
            earlier: EarlierCore::default(),
        }
    }

    /// Shared constructor for actor and replay: builds a fresh core and applies optional mode hints.
    pub(crate) fn for_session(
        desc: Arc<dyn DriverDescriptor>,
        meta: &SessionMeta,
        mode_hints: Option<Vec<wasit_core::SessionMode>>,
    ) -> Self {
        let mut core = Core::new(desc, meta);
        if let Some(m) = mode_hints {
            core.hint_modes(m);
        }
        core
    }

    /// Set the user-given display name (not journaled: it lives in `session.json`).
    pub(crate) fn set_display_name(&mut self, name: Option<String>) {
        self.snap.display_name = name;
    }

    // ── queries ─────────────────────────────────────────────────────────────

    pub(crate) fn snapshot(&self) -> &SessionSnapshot {
        &self.snap
    }

    pub(crate) fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Lines requested but not yet seen as written.
    pub(crate) fn owed(&self) -> impl Iterator<Item = &String> {
        self.owed.iter().map(|o| &o.line)
    }

    pub(crate) fn close_owed(&self) -> bool {
        self.close_owed
    }

    /// The current generation waits for its `start` note.
    pub(crate) fn awaiting_start(&self) -> bool {
        matches!(self.gen, Gen::Awaiting(_)) && self.lost.is_none()
    }

    /// A process generation is running (as far as the journal says).
    pub(crate) fn process_running(&self) -> bool {
        matches!(self.gen, Gen::Awaiting(_) | Gen::Running(_)) && self.lost.is_none()
    }

    /// The process exited (journal says so) or no generation ever started.
    pub(crate) fn process_ended(&self) -> bool {
        matches!(self.gen, Gen::Exited) || self.lost.is_some()
    }

    pub(crate) fn handshake_pending(&self) -> bool {
        matches!(self.gen, Gen::Running(_)) && !self.handshake_done && self.lost.is_none()
    }

    /// Journal time of the current generation's `start` note.
    pub(crate) fn start_t(&self) -> Option<u64> {
        self.start_t
    }

    pub(crate) fn stop_requested(&self) -> bool {
        self.stop_requested
    }

    /// How to start the next generation of an ended session, or why it cannot be continued.
    /// Decided from the same facts as the snapshot's `continuable`.
    pub(crate) fn continue_plan(&self) -> Result<ContinuePlan, NotContinuable> {
        continue_verdict(
            &self.snap.capabilities,
            &self.snap.phase,
            self.snap.loaded,
            &self.conversation,
        )?;
        match &self.conversation {
            Conversation::Saved(id) => Ok(ContinuePlan::Resume(id.clone())),
            Conversation::Chosen(id) => Ok(ContinuePlan::StartFresh(id.clone())),
            // `continue_verdict` refused these.
            Conversation::Unknown | Conversation::Unsaved(_) => {
                Err(NotContinuable::NoConversationId)
            }
        }
    }

    /// The facts the conversation (and so `continuable`) depends on changed.
    fn conversation_is(&mut self, c: Conversation) {
        self.conversation = c;
        self.refresh_conversation();
    }

    fn refresh_conversation(&mut self) {
        apply_conversation(&mut self.snap, self.desc.as_ref(), &self.conversation);
    }

    // ── input ───────────────────────────────────────────────────────────────

    /// Apply one journal record. Records at or below the last applied seq are ignored, so a
    /// re-attach may overlap.
    pub(crate) fn apply(&mut self, rec: &Record) -> Output {
        let mut out = Output::default();
        if rec.seq <= self.last_seq {
            return out;
        }
        self.last_seq = rec.seq;
        let phase_before = self.snap.phase.clone();
        self.causes.clear();
        let mut em = Emitter::new(rec.seq, rec.t);
        let text = match rec.kind {
            Kind::Out | Kind::Err | Kind::In => {
                let piece = rec.text().unwrap_or_default();
                if rec.partial {
                    self.partial.entry(rec.kind).or_default().push_str(piece);
                    return out;
                }
                match self.partial.remove(&rec.kind) {
                    Some(mut head) => {
                        head.push_str(piece);
                        head
                    }
                    None => piece.to_string(),
                }
            }
            _ => String::new(),
        };
        match rec.kind {
            Kind::Spawned => self.on_spawned(rec, &mut em, &mut out),
            Kind::Out => {
                self.snap.last_activity_ms = rec.t;
                match &mut self.gen {
                    Gen::Awaiting(buf) => buf.push(Buffered::Line(text)),
                    Gen::Running(p) => {
                        let step = p.on_line(&text);
                        self.step(step, None, &mut em, &mut out);
                    }
                    // Output outside a generation cannot occur (`exited` is the last record of
                    // a run); nothing to feed it to.
                    Gen::None | Gen::Exited => {}
                }
            }
            Kind::Err => {
                let line = truncate(&text, STDERR_LINE_CHARS);
                self.stderr_tail.push_back(line);
                while self.stderr_tail.len() > STDERR_TAIL_LINES {
                    self.stderr_tail.pop_front();
                }
                self.snap.stderr_tail = self.stderr_tail.iter().cloned().collect();
                em.push(SessionEventKind::Stderr { text });
            }
            Kind::In => {
                self.snap.last_activity_ms = rec.t;
                self.on_echo(&text, &mut em);
            }
            Kind::Exited => self.on_exited(rec, &mut em, &mut out),
            Kind::Note => self.on_note(rec, &mut em, &mut out),
        }
        out.gate_facts = std::mem::take(&mut self.gate_facts);
        self.finish(em, &mut out, &phase_before);
        out
    }

    /// The shim vanished without an exit record. Returns the events to publish (empty when the
    /// journal already ended the generation).
    pub(crate) fn mark_lost(&mut self, reason: &str) -> Vec<SessionEvent> {
        if self.lost.is_some() || matches!(self.gen, Gen::Exited) {
            return Vec::new();
        }
        let mut kinds = Vec::new();
        for event in self.end_inputs() {
            self.reduce(&event, 0, self.snap.last_activity_ms);
            kinds.push(SessionEventKind::Agent { event });
        }
        self.lost = Some(reason.to_string());
        if self.earlier.info.state == EarlierState::Loading {
            self.earlier.info.state = EarlierState::Loaded { complete: false };
            self.earlier.info.gaps.interrupted = true;
            self.snap.earlier = self.earlier.info.clone();
            kinds.push(SessionEventKind::Earlier {
                earlier: self.earlier.info.clone(),
            });
        }
        self.clear_live_state(self.snap.last_activity_ms);
        let slot = synthetic_cursor(self.last_seq);
        let t = self.snap.last_activity_ms;
        let phase = self.compute_phase();
        self.snap.phase = phase.clone();
        self.refresh_conversation();
        self.snap.cursor = slot;
        self.anomaly_reported = false;
        kinds.push(SessionEventKind::Lost {
            reason: reason.to_string(),
        });
        kinds.push(SessionEventKind::Phase {
            phase,
            cause: PhaseCause::Lost,
        });
        let n = kinds.len() as u64;
        kinds
            .into_iter()
            .enumerate()
            .map(|(i, kind)| SessionEvent {
                cursor: slot - (n - 1 - i as u64),
                t,
                kind,
            })
            .collect()
    }

    // ── record handlers ─────────────────────────────────────────────────────

    fn on_spawned(&mut self, rec: &Record, em: &mut Emitter, out: &mut Output) {
        if matches!(self.gen, Gen::Awaiting(_) | Gen::Running(_)) && self.lost.is_none() {
            em.push(SessionEventKind::Notice {
                message: "the previous process ended without an exit record".into(),
                code: NoticeCode::PreviousGenerationUnclosed,
            });
            self.end_generation_inputs(em);
        }
        self.gen = Gen::Awaiting(Vec::new());
        self.owed.clear();
        self.close_owed = false;
        self.handshake_done = false;
        self.stop_requested = false;
        self.handshake_timeout = None;
        self.start_t = None;
        self.lost = None;
        self.exit_phase = None;
        self.prompt_texts.clear();
        self.clear_live_state(rec.t);
        self.snap.generation += 1;
        self.causes.push(PhaseCause::Spawned {
            generation: self.snap.generation,
        });
        self.snap.exit = None;
        let pid = rec
            .data
            .get("pid")
            .and_then(|v| v.as_u64())
            .map(|p| p as u32);
        self.snap.pid = pid;
        let argv = rec
            .data
            .get("argv")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        em.push(SessionEventKind::Spawned {
            generation: self.snap.generation,
            pid,
            argv,
        });
        out.spawned = true;
    }

    fn on_exited(&mut self, rec: &Record, em: &mut Emitter, out: &mut Output) {
        if !matches!(self.gen, Gen::Awaiting(_) | Gen::Running(_)) {
            return;
        }
        let info = rec.exit_info().unwrap_or_default();
        if let Gen::Awaiting(buf) = std::mem::replace(&mut self.gen, Gen::Exited) {
            // Commands journaled before a `start` that never came: the process ended first.
            let how = match (info.code, info.signal) {
                (Some(c), _) => format!("exit code {c}"),
                (None, Some(sig)) => format!("signal {sig}"),
                (None, None) => "unknown status".to_string(),
            };
            let said = self
                .stderr_tail
                .back()
                .map(|l| format!(": {l}"))
                .unwrap_or_default();
            for b in buf {
                if let Buffered::Command {
                    token,
                    input,
                    command,
                    trace,
                } = b
                {
                    let status = CommandStatus::NotRunning {
                        reason: format!("the process ended before it started ({how}{said})"),
                    };
                    em.push(SessionEventKind::Input {
                        input,
                        command: crate::media::without_bytes(command),
                        status: status.clone(),
                        trace,
                    });
                    out.resolved.push((token, status));
                }
            }
        }
        self.end_generation_inputs(em);
        self.earlier_generation_ended(em);
        self.owed.clear();
        self.close_owed = false;
        // External gates end with the process: their requests are gone.
        let ended = if self.stop_requested {
            GateReason::new(GateReason::SESSION_STOPPED)
        } else {
            GateReason::new(GateReason::PROCESS_ENDED)
        };
        let pending: Vec<String> = self.external.keys().cloned().collect();
        for request_id in pending {
            self.withdraw_external(request_id, ended.clone(), rec.t, em);
        }
        self.clear_live_state(rec.t);
        self.snap.exit = Some(ExitSummary {
            code: info.code,
            signal: info.signal,
            at_ms: rec.t,
            requested: self.stop_requested,
        });
        self.causes.push(PhaseCause::Exited {
            code: info.code,
            signal: info.signal,
            requested: self.stop_requested,
            handshake_timeout: self.handshake_timeout.is_some(),
        });
        let tail: Vec<String> = self
            .stderr_tail
            .iter()
            .skip(self.stderr_tail.len().saturating_sub(FAILED_TAIL_LINES))
            .cloned()
            .collect();
        self.exit_phase = Some(if let Some(after) = self.handshake_timeout {
            Phase::Failed {
                reason: format!("the CLI did not complete its handshake within {after} ms"),
                stderr_tail: tail,
            }
        } else if self.stop_requested || info.code == Some(0) {
            Phase::Exited { code: info.code }
        } else if let Some(c) = info.code {
            Phase::Failed {
                reason: format!("the CLI exited with code {c}"),
                stderr_tail: tail,
            }
        } else if let Some(s) = info.signal {
            Phase::Failed {
                reason: format!("the CLI was killed by signal {s}"),
                stderr_tail: tail,
            }
        } else {
            Phase::Failed {
                reason: "the CLI exited with an unknown status".into(),
                stderr_tail: tail,
            }
        });
        em.push(SessionEventKind::Exited {
            code: info.code,
            signal: info.signal,
        });
        out.exited = true;
    }

    fn on_note(&mut self, rec: &Record, em: &mut Emitter, out: &mut Output) {
        match classify(&rec.data) {
            NoteKind::Runtime(RuntimeNote::Attach { token }) => out.attach = Some(token),
            NoteKind::Runtime(RuntimeNote::Start {
                spec,
                trace,
                earlier,
            }) => self.on_start(&spec, trace, earlier, rec, em, out),
            NoteKind::Runtime(RuntimeNote::EarlierChunk { first, events }) => {
                self.on_earlier_chunk(first, events, em, out)
            }
            NoteKind::Runtime(RuntimeNote::EarlierEnd { report }) => {
                self.on_earlier_end(&report, em)
            }
            NoteKind::Runtime(RuntimeNote::EarlierFailed { reason }) => {
                self.on_earlier_failed(reason, em)
            }
            NoteKind::Runtime(RuntimeNote::Command {
                token,
                command,
                trace,
            }) => {
                self.snap.last_activity_ms = rec.t;
                let input = InputId(rec.seq);
                if let Some((request_id, reason)) = self.answers_withdrawn(&command) {
                    let status = CommandStatus::Withdrawn {
                        request_id,
                        reason: Some(reason.clone()),
                    };
                    em.push(SessionEventKind::Input {
                        input,
                        command: command.clone(),
                        status: status.clone(),
                        trace,
                    });
                    // The answer is journaled, so the session says what became of it.
                    em.push(SessionEventKind::Notice {
                        message: not_delivered_message(&reason),
                        code: NoticeCode::AnswerNotDelivered,
                    });
                    self.note_refused_answer(&command, &status);
                    out.resolved.push((token, status));
                    return;
                }
                if self.answers_external(&command) {
                    self.answer_external(token, input, command, trace, em, out);
                    return;
                }
                match &mut self.gen {
                    Gen::Awaiting(buf) => buf.push(Buffered::Command {
                        token,
                        input,
                        command,
                        trace,
                    }),
                    Gen::Running(_) => {
                        self.apply_command(Some(token), input, command, trace, em, out)
                    }
                    Gen::None | Gen::Exited => {
                        let status = CommandStatus::NotRunning {
                            reason: "the process is not running".into(),
                        };
                        em.push(SessionEventKind::Input {
                            input,
                            command: crate::media::without_bytes(command),
                            status: status.clone(),
                            trace,
                        });
                        out.resolved.push((token, status));
                    }
                }
            }
            NoteKind::Runtime(RuntimeNote::Stop { token }) => {
                out.resolved.push((token, CommandStatus::Sent));
                if !self.process_running() {
                    return;
                }
                self.stop_requested = true;
                out.stop_applied = true;
                em.push(SessionEventKind::Notice {
                    message: "stop requested".into(),
                    code: NoticeCode::StopRequested,
                });
                match &mut self.gen {
                    Gen::Awaiting(buf) => buf.push(Buffered::Stop {
                        input: InputId(rec.seq),
                    }),
                    Gen::Running(_) => self.apply_stop(InputId(rec.seq), em, out),
                    Gen::None | Gen::Exited => {}
                }
            }
            NoteKind::Runtime(RuntimeNote::HandshakeTimeout { after_ms }) => {
                if self.process_running() {
                    self.handshake_timeout = Some(after_ms);
                    em.push(SessionEventKind::Notice {
                        message: format!(
                            "handshake deadline of {after_ms} ms passed; stopping the process"
                        ),
                        code: NoticeCode::HandshakeDeadline,
                    });
                }
            }
            NoteKind::Runtime(RuntimeNote::RaiseGate {
                token,
                gate,
                binding,
            }) => self.raise_external(token, *gate, binding, rec.t, em, out),
            NoteKind::Runtime(RuntimeNote::ResolveGate {
                token,
                request_id,
                reason,
            }) => self.resolve_external(token, request_id, reason, em, out),
            NoteKind::Runtime(RuntimeNote::LocalShell {
                token,
                command,
                cwd,
                exit_code,
                output,
                output_truncated,
            }) => {
                // The host ran it outside the agent: the journal record is the only place its
                // output lives, so it is an event here, in journal order, like any other.
                let event = AgentEvent::LocalShell {
                    command,
                    cwd,
                    exit_code,
                    output,
                    output_truncated,
                    origin: LocalShellOrigin::Host,
                };
                let cursor = em.next_cursor();
                self.reduce(&event, cursor, em.t);
                em.push(SessionEventKind::Agent { event });
                out.resolved.push((token, CommandStatus::Sent));
            }
            NoteKind::StdinClosed => self.close_owed = false,
            NoteKind::StdinError(error) => {
                if self.last_echoed.is_empty() {
                    em.push(SessionEventKind::WriteFailed { input: None, error });
                } else {
                    for input in self.last_echoed.clone() {
                        em.push(SessionEventKind::WriteFailed {
                            input: Some(input),
                            error: error.clone(),
                        });
                    }
                }
            }
            NoteKind::Other { code, message } => {
                em.push(SessionEventKind::Notice { message, code })
            }
        }
    }

    fn on_start(
        &mut self,
        spec: &SpawnSpec,
        trace: Option<TraceId>,
        earlier: Option<EarlierPlan>,
        rec: &Record,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        // The first prompt is the generation's first input, journaled in this very note, and it
        // carries the trace of the request that started the session.
        let (spec, first) = spec.split_first_prompt();
        let input = InputId(rec.seq);
        if !matches!(self.gen, Gen::Awaiting(_)) {
            em.push(SessionEventKind::Notice {
                message: "ignored a start note outside a fresh process generation".into(),
                code: NoticeCode::StartIgnored,
            });
            if let Some(command) = first {
                let status = if self.process_running() {
                    CommandStatus::Rejected {
                        reason: "the process generation had already started".into(),
                    }
                } else {
                    CommandStatus::NotRunning {
                        reason: "the process is not running".into(),
                    }
                };
                em.push(SessionEventKind::Input {
                    input,
                    command,
                    status,
                    trace,
                });
            }
            return;
        }
        let Gen::Awaiting(buf) = std::mem::replace(&mut self.gen, Gen::None) else {
            return;
        };
        let (resume, resume_id) = match &spec.resume {
            ResumeMode::Fresh => (StartResume::Fresh, None),
            ResumeMode::Continue => (StartResume::Continue, None),
            ResumeMode::Resume(id) => (StartResume::Resume, Some(id.clone())),
            ResumeMode::Fork(id) => (StartResume::Fork, Some(id.clone())),
        };
        em.push(SessionEventKind::Started {
            generation: self.snap.generation,
            resume,
            resume_id,
            model: spec.model.clone().filter(|m| !m.is_empty()),
            mode: Some(spec.mode.clone()).filter(|m| !m.is_empty()),
            first_input: first.as_ref().map(|_| input),
            trace: trace.clone(),
        });
        // What this generation was started with tells what the conversation is: a resumed one is
        // saved; an id chosen for a fresh one is the session's id until the agent reports its own
        // (it never replaces what is already known).
        let started = Conversation::of_spec(&spec);
        let adopt = match (&self.conversation, &started) {
            (_, Conversation::Unknown) => false,
            (c, Conversation::Saved(_)) => *c != started,
            (Conversation::Unknown, _) => true,
            _ => false,
        };
        if adopt {
            self.conversation_is(started);
        }
        self.earlier_on_start(&spec, earlier, em);
        let mut p = self.desc.new_protocol();
        let step = p.start(&spec);
        self.gen = Gen::Running(p);
        self.start_t = Some(rec.t);
        out.started = true;
        if self.snap.model.is_none() {
            self.snap.model = spec.model.clone().filter(|m| !m.is_empty());
        }
        if self.snap.mode.is_none() && !spec.mode.is_empty() {
            self.snap.mode = Some(spec.mode.clone());
        }
        self.step(step, None, em, out);
        if let Some(command) = first {
            self.apply_command(None, input, command, trace, em, out);
        }
        for b in buf {
            match b {
                Buffered::Line(line) => {
                    if let Gen::Running(p) = &mut self.gen {
                        let step = p.on_line(&line);
                        self.step(step, None, em, out);
                    }
                }
                Buffered::Command {
                    token,
                    input,
                    command,
                    trace,
                } => self.apply_command(Some(token), input, command, trace, em, out),
                Buffered::Stop { input } => self.apply_stop(input, em, out),
            }
        }
    }

    /// Encode one input and report it (`input` event; the requester's result when `token`
    /// names one). The first prompt (no requester) and every `command` note take this path.
    fn apply_command(
        &mut self,
        token: Option<String>,
        input: InputId,
        command: Command,
        trace: Option<TraceId>,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let Gen::Running(p) = &mut self.gen else {
            return;
        };
        let (status, step) = match p.encode(input, command.clone()) {
            Ok(Encoded::Sent(step)) => (CommandStatus::Sent, Some(step)),
            Ok(Encoded::Queued) => (CommandStatus::Queued, None),
            Err(DriverError::Unsupported { operation }) => {
                (CommandStatus::Unsupported { operation }, None)
            }
            Err(DriverError::Shutdown) => (
                CommandStatus::NotRunning {
                    reason: "the session is shutting down".into(),
                },
                None,
            ),
            Err(e) => (
                CommandStatus::Rejected {
                    reason: e.to_string(),
                },
                None,
            ),
        };
        let interrupts = status == CommandStatus::Sent && matches!(command, Command::Interrupt);
        let accepted = matches!(status, CommandStatus::Sent | CommandStatus::Queued);
        if accepted {
            match &command {
                Command::Prompt { text, .. } => self.note_prompt(input, text.clone()),
                Command::RunSlash { name, args } => {
                    let text = if args.is_empty() {
                        format!("/{name}")
                    } else {
                        format!("/{name} {args}")
                    };
                    self.note_prompt(input, text)
                }
                Command::AnswerGate { request_id, .. }
                | Command::AnswerQuestion { request_id, .. }
                    if status == CommandStatus::Sent =>
                {
                    self.note_answered(request_id, em.t);
                    self.snap
                        .pending_gates
                        .retain(|g| g.gate.request_id() != request_id);
                    self.causes.push(PhaseCause::GateCleared {
                        request_id: request_id.clone(),
                        how: GateClearing::Answered,
                    });
                }
                _ => {}
            }
        }
        if status == CommandStatus::Queued {
            push_bounded(&mut self.queued, input, MAX_OPEN_INPUTS);
        }
        self.note_refused_answer(&command, &status);
        em.push(SessionEventKind::Input {
            input,
            command: crate::media::without_bytes(command),
            status: status.clone(),
            trace,
        });
        if let Some(token) = token {
            out.resolved.push((token, status));
        }
        // The gates of the turn being interrupted go right behind the interrupt itself and
        // before anything the driver reports about it (it may confirm only later, or end the
        // turn normally in a race: either way the gates were the interrupted turn's).
        if interrupts && self.snap.turn.is_some() {
            self.interrupt_applied = true;
            self.end_turn_gates(TurnEnd::Interrupted, em.t, em);
        }
        if let Some(step) = step {
            self.step(step, Some(input), em, out);
        }
    }

    // ── external gates ──────────────────────────────────────────────────────

    /// `command` answers an external gate that was withdrawn (and is not pending again), with
    /// the reason it was withdrawn.
    pub(crate) fn answers_withdrawn(&self, command: &Command) -> Option<(String, GateReason)> {
        let request_id = answer_request_id(command)?;
        if self.external.contains_key(request_id) {
            return None;
        }
        self.withdrawn
            .iter()
            .find(|(w, _)| w == request_id)
            .cloned()
    }

    fn remember_withdrawn(&mut self, request_id: String, reason: GateReason) {
        self.withdrawn.retain(|(w, _)| *w != request_id);
        self.withdrawn.push_back((request_id, reason));
        while self.withdrawn.len() > WITHDRAWN_MEMORY {
            self.withdrawn.pop_front();
        }
    }

    /// The gate `request_id` was answered at `t`: its latency, if it is pending.
    fn note_answered(&mut self, request_id: &str, t: u64) {
        let raised = self
            .snap
            .pending_gates
            .iter()
            .find(|g| g.gate.request_id() == request_id)
            .map(|g| g.raised_at_ms);
        if let Some(raised) = raised {
            self.gate_facts.push(GateFact::Answered {
                request_id: request_id.to_string(),
                ms: t.saturating_sub(raised),
            });
        }
    }

    /// `status` refused an answer to a gate: the fact says why, by the status's word. Other
    /// commands and accepted answers make no fact.
    fn note_refused_answer(&mut self, command: &Command, status: &CommandStatus) {
        let Some(request_id) = answer_request_id(command) else {
            return;
        };
        let reason = match status {
            CommandStatus::Rejected { .. } => "rejected",
            CommandStatus::NotRunning { .. } => "not_running",
            CommandStatus::Unsupported { .. } => "unsupported",
            CommandStatus::Withdrawn { .. } => "withdrawn",
            CommandStatus::Sent | CommandStatus::Queued => return,
        };
        self.gate_facts.push(GateFact::AnswerRejected {
            request_id: request_id.to_string(),
            reason,
        });
    }

    /// `command` answers a pending external gate.
    fn answers_external(&self, command: &Command) -> bool {
        match command {
            Command::AnswerGate { request_id, .. } | Command::AnswerQuestion { request_id, .. } => {
                self.external.contains_key(request_id)
            }
            _ => false,
        }
    }

    /// The answer is recorded (as the `input` event its owner watches for) and the gate stops
    /// being pending; nothing is written to the CLI.
    fn answer_external(
        &mut self,
        token: String,
        input: InputId,
        command: Command,
        trace: Option<TraceId>,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        // An answer must pick one of the gate's own choices (the gate's owner reads it back
        // from the journal and relies on that).
        if let Command::AnswerGate {
            request_id,
            decision,
        } = &command
        {
            let check = self
                .snap
                .pending_gates
                .iter()
                .find(|g| g.gate.request_id() == request_id)
                .map(|g| g.gate.check(decision).map(|_| ()));
            if let Some(Err(reason)) = check {
                self.gate_facts.push(GateFact::AnswerRejected {
                    request_id: request_id.clone(),
                    reason: "invalid_choice",
                });
                let status = CommandStatus::Rejected { reason };
                em.push(SessionEventKind::Input {
                    input,
                    command,
                    status: status.clone(),
                    trace,
                });
                out.resolved.push((token, status));
                return;
            }
        }
        if let Command::AnswerGate { request_id, .. } | Command::AnswerQuestion { request_id, .. } =
            &command
        {
            self.note_answered(request_id, em.t);
            self.external.remove(request_id);
            self.snap
                .pending_gates
                .retain(|g| g.gate.request_id() != request_id);
            self.causes.push(PhaseCause::GateCleared {
                request_id: request_id.clone(),
                how: GateClearing::Answered,
            });
        }
        em.push(SessionEventKind::Input {
            input,
            command,
            status: CommandStatus::Sent,
            trace,
        });
        out.resolved.push((token, CommandStatus::Sent));
    }

    fn raise_external(
        &mut self,
        token: String,
        gate: wasit_core::GateRequest,
        binding: Option<TurnBinding>,
        t: u64,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let rid = gate.request_id().to_string();
        let status = if !self.process_running() {
            CommandStatus::NotRunning {
                reason: "the process is not running".into(),
            }
        } else if self.withdrawn.iter().any(|(w, _)| *w == rid) {
            CommandStatus::Rejected {
                reason: format!("the gate {rid:?} was withdrawn; raise it under a new id"),
            }
        } else if rid.is_empty()
            || self
                .snap
                .pending_gates
                .iter()
                .any(|g| g.gate.request_id() == rid)
        {
            CommandStatus::Rejected {
                reason: format!("a gate with request id {rid:?} is already pending"),
            }
        } else {
            self.external.insert(rid.clone(), None);
            self.gate_facts.push(GateFact::Raised {
                request_id: rid.clone(),
                kind: gate_kind(&gate),
                bound_to: binding_word(binding.as_ref()),
            });
            let event = AgentEvent::GateRequested { gate };
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, t);
            em.push(SessionEventKind::Agent { event });
            if let Some(binding) = binding {
                self.bind_to_turn(rid, binding, t, em);
            }
            CommandStatus::Sent
        };
        out.resolved.push((token, status));
    }

    fn resolve_external(
        &mut self,
        token: String,
        request_id: String,
        reason: GateReason,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let status = if self.external.contains_key(&request_id) {
            self.withdraw_external(request_id, reason, em.t, em);
            CommandStatus::Sent
        } else {
            CommandStatus::Rejected {
                reason: format!("no external gate {request_id:?} is pending"),
            }
        };
        out.resolved.push((token, status));
    }

    /// Take the external gate `request_id` down with `reason`: the one place a gate is resolved
    /// without an answer (its owner asked, the process ended, its turn was interrupted or
    /// ended). Remembered, so a late answer is refused.
    fn withdraw_external(
        &mut self,
        request_id: String,
        reason: GateReason,
        t: u64,
        em: &mut Emitter,
    ) {
        self.external.remove(&request_id);
        self.gate_facts.push(GateFact::Withdrawn {
            request_id: request_id.clone(),
            reason: reason.code.clone(),
        });
        self.remember_withdrawn(request_id.clone(), reason.clone());
        let event = AgentEvent::GateResolved {
            request_id,
            reason: Some(reason),
        };
        let cursor = em.next_cursor();
        self.reduce(&event, cursor, t);
        em.push(SessionEventKind::Agent { event });
    }

    /// Tie the gate just raised to the turn open now ([`TurnBinding`]):
    ///
    /// * a turn is open and no interrupt was applied to it: the gate is bound to it;
    /// * a turn is open and was interrupted already (the CLI has not ended it yet): the turn
    ///   is as good as over, so the gate is resolved at once with `turn_interrupted`;
    /// * no turn is open: the gate belongs to the session (a request raised outside any turn:
    ///   between turns, while only background work runs, before the first turn; there is no
    ///   turn to follow).
    ///
    /// The resolution is part of the raising record, right behind `gate_requested`.
    fn bind_to_turn(&mut self, rid: String, binding: TurnBinding, t: u64, em: &mut Emitter) {
        if self.snap.turn.is_some() {
            if self.interrupt_applied {
                self.withdraw_external(rid, binding.interrupted_reason(), t, em);
            } else {
                self.external.insert(rid, Some(binding));
            }
        }
    }

    /// The open turn is over (`Ended`) or being interrupted (`Interrupted`): resolve the gates
    /// bound to it, in the order they were raised, with the runtime's own reasons. A bound
    /// gate that survives this (an interrupt-only binding at a normal end) becomes the
    /// session's. Always runs *before* the event that ends the turn is published, so a gate
    /// is never left pending behind a finished turn.
    fn end_turn_gates(&mut self, how: TurnEnd, t: u64, em: &mut Emitter) {
        let bound: Vec<(String, TurnBinding)> = self
            .snap
            .pending_gates
            .iter()
            .filter_map(|g| {
                let rid = g.gate.request_id();
                let binding = self.external.get(rid)?.clone()?;
                Some((rid.to_string(), binding))
            })
            .collect();
        for (rid, binding) in bound {
            let reason = match how {
                TurnEnd::Interrupted => Some(binding.interrupted_reason()),
                TurnEnd::Ended => binding.ended_reason(),
            };
            match reason {
                Some(reason) => self.withdraw_external(rid, reason, t, em),
                None => {
                    self.external.insert(rid, None);
                }
            }
        }
    }

    fn note_prompt(&mut self, input: InputId, text: String) {
        if self.snap.title.is_none() {
            self.snap.title = Some(truncate(&text, TITLE_CHARS));
        }
        push_bounded(&mut self.prompt_texts, (input, text), MAX_PROMPT_TEXTS);
    }

    /// `input` is the `stop` note's id (a shutdown is not a user input, but it is encoded like
    /// one).
    fn apply_stop(&mut self, input: InputId, em: &mut Emitter, out: &mut Output) {
        let Gen::Running(p) = &mut self.gen else {
            return;
        };
        match p.encode(input, Command::Shutdown) {
            Ok(Encoded::Sent(step)) => self.step(step, None, em, out),
            // Already shut down / nothing to encode: the actor's escalation takes over.
            Ok(Encoded::Queued) | Err(_) => {}
        }
    }

    fn on_echo(&mut self, text: &str, em: &mut Emitter) {
        let pos = if self.owed.front().map(|o| o.line.as_str()) == Some(text) {
            Some(0)
        } else {
            self.owed.iter().position(|o| o.line == text)
        };
        // Otherwise a foreign write (another tool on the shim): nothing was owed.
        if let Some(owed) = pos.and_then(|i| self.owed.remove(i)) {
            self.settle_written(&owed.inputs, em);
            self.last_echoed = owed.inputs;
        }
    }

    /// `written` for each of `inputs` none of whose lines is owed any more.
    fn settle_written(&mut self, inputs: &[InputId], em: &mut Emitter) {
        for input in inputs {
            if self.writing.contains(input) && !self.owed.iter().any(|o| o.inputs.contains(input)) {
                self.writing.retain(|w| w != input);
                em.push(SessionEventKind::Written { input: *input });
            }
        }
    }

    /// The inputs that will never be written now that the generation ended (still queued in the
    /// protocol, or their lines never echoed), oldest first; forgets them.
    fn end_inputs(&mut self) -> Vec<AgentEvent> {
        let mut ids: Vec<InputId> = self.queued.drain(..).collect();
        ids.extend(self.writing.drain(..));
        ids.into_iter()
            .map(|input| AgentEvent::InputDropped {
                input,
                reason: InputDropReason::ProcessEnded,
                error: None,
            })
            .collect()
    }

    fn end_generation_inputs(&mut self, em: &mut Emitter) {
        for event in self.end_inputs() {
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, em.t);
            em.push(SessionEventKind::Agent { event });
        }
    }

    // ── protocol output ─────────────────────────────────────────────────────

    /// Publish a protocol step. `input` is the command whose `encode` produced it (its lines
    /// are its writes); inputs the step reports sent are added.
    fn step(&mut self, step: Step, input: Option<InputId>, em: &mut Emitter, out: &mut Output) {
        let mut inputs: Vec<InputId> = input.into_iter().collect();
        for mut event in step.events {
            if matches!(event, AgentEvent::History { .. }) {
                // The conversation as it was before this session: kept apart from the live
                // events, never reduced.
                self.earlier_replayed(event, em, out);
                continue;
            }
            crate::media::externalize(&mut event, &mut out.blobs);
            match &event {
                AgentEvent::InputSent { input } => {
                    self.queued.retain(|q| q != input);
                    inputs.push(*input);
                }
                AgentEvent::InputDropped { input, .. } => self.queued.retain(|q| q != input),
                AgentEvent::TurnCompleted {
                    was_interrupted, ..
                } => self.end_turn_gates(
                    if *was_interrupted {
                        TurnEnd::Interrupted
                    } else {
                        TurnEnd::Ended
                    },
                    em.t,
                    em,
                ),
                _ => {}
            }
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, em.t);
            let opened = matches!(event, AgentEvent::SessionInit { .. });
            em.push(SessionEventKind::Agent { event });
            if opened {
                // The agent has told everything it replays of a resumed conversation.
                self.earlier_replay_done(em);
            }
        }
        if !step.write.is_empty() {
            for input in &inputs {
                push_bounded(&mut self.writing, *input, MAX_OPEN_INPUTS);
            }
        }
        for line in step.write {
            self.owed.push_back(Owed {
                line: line.0.clone(),
                inputs: inputs.clone(),
            });
            if self.owed.len() > MAX_OWED {
                // Forgotten: counts as written.
                if let Some(old) = self.owed.pop_front() {
                    self.settle_written(&old.inputs, em);
                }
            }
            out.writes.push(line.0);
        }
        if step.close_stdin {
            self.close_owed = true;
            out.close_stdin = true;
        }
    }

    /// What an event of a subagent's own conversation does to the session: it is never part of
    /// the session's turn (no tool counts, no current tool, no usage meters); it counts toward
    /// the subagent it belongs to.
    fn reduce_scoped(&mut self, scope: &Scope, event: &AgentEvent, t: u64) {
        let s = &mut self.snap;
        let known =
            |subs: &mut Vec<SubagentInfo>| subs.iter().position(|a| a.call_id == scope.call_id);
        if let Some(i) = known(&mut s.subagents) {
            if s.subagents[i].agent_id.is_none() {
                s.subagents[i].agent_id.clone_from(&scope.agent_id);
            }
        }
        match event {
            AgentEvent::ToolCallStarted {
                name,
                call_id,
                input,
                is_subagent,
                subagent_model,
            } => {
                if let Some(i) = known(&mut s.subagents) {
                    s.subagents[i].tool_calls += 1;
                    s.subagents[i].last_tool = Some(name.clone());
                }
                if *is_subagent {
                    push_subagent(&mut s.subagents, call_id, name, input, subagent_model, t);
                    sync_subagents(&mut s.subagents, &s.tasks);
                }
            }
            AgentEvent::ToolCallFinished { call_id, ok, .. } => {
                finish_subagent_call(&mut s.subagents, call_id, *ok, t);
            }
            _ => {}
        }
    }

    fn reduce(&mut self, event: &AgentEvent, cursor: u64, t: u64) {
        if let AgentEvent::Scoped { scope, event } = event {
            self.reduce_scoped(scope, event, t);
            return;
        }
        let s = &mut self.snap;
        let mut cause = None;
        let mut saved: Option<SavedFact> = None;
        match event {
            AgentEvent::SessionInit {
                native_session_id,
                model,
                slash_commands,
                available_modes,
                current_mode,
                cli_version,
                ..
            } => {
                if let Some(id) = native_session_id {
                    s.native_session_id = Some(id.clone());
                    saved = Some(Reported(id.clone()));
                }
                if model.is_some() {
                    s.model = model.clone();
                }
                if !slash_commands.is_empty() {
                    s.slash_commands = slash_commands.clone();
                }
                if !available_modes.is_empty() {
                    s.available_modes = available_modes.clone();
                }
                if !current_mode.is_empty() {
                    s.mode = Some(current_mode.clone());
                }
                if !cli_version.is_empty() {
                    s.cli_version = Some(cli_version.clone());
                }
            }
            AgentEvent::TurnStarted {
                native_turn_id,
                input,
                trigger,
            } => {
                saved = Some(TurnStarted);
                self.interrupt_applied = false;
                let prompt = input.and_then(|i| take_text(&mut self.prompt_texts, i));
                s.turn = Some(TurnInfo {
                    started_at_ms: t,
                    native_turn_id: native_turn_id.clone(),
                    input: *input,
                    trigger: *trigger,
                    prompt,
                    tool_calls: 0,
                    last_tool: None,
                    current_tool: None,
                    current_call: None,
                });
                cause = Some(PhaseCause::TurnStarted {
                    input: *input,
                    trigger: *trigger,
                });
            }
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                num_turns,
                usage,
                // Per-turn usage is not folded into the session yet: the session keeps the
                // cumulative `usage` as before.
                turn_usage: _,
                per_model,
                reply,
                origin,
            } => {
                s.turn = None;
                self.interrupt_applied = false;
                // CLI gates end with the turn; external gates are owned by someone else.
                let external = &self.external;
                s.pending_gates
                    .retain(|g| external.contains_key(g.gate.request_id()));
                s.turns_completed += 1;
                if *usage != Default::default() {
                    s.usage = usage.clone();
                }
                if !per_model.is_empty() {
                    s.usage_by_model = per_model
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                }
                // Work that is not in the background cannot outlive its turn; background tasks
                // go on (that is what makes the session `working` without a turn).
                tasks::end_foreground(&mut s.tasks, t);
                for a in s.subagents.iter_mut().filter(|a| a.running) {
                    if a.task_id.is_none() {
                        // An agent that reports no task for it: the turn's end is the only end.
                        a.running = false;
                        a.ended_at_ms = Some(t);
                    }
                }
                sync_subagents(&mut s.subagents, &s.tasks);
                s.background_running = tasks::background_running(&s.tasks);
                s.last_turn = Some(TurnOutcome {
                    ended_at_ms: t,
                    end_reason: *end_reason,
                    was_interrupted: *was_interrupted,
                    num_turns: *num_turns,
                    reply: reply.as_deref().map(|r| truncate(r, REPLY_CHARS)),
                    origin: origin.clone(),
                });
                cause = Some(PhaseCause::TurnCompleted {
                    end_reason: *end_reason,
                    interrupted: *was_interrupted,
                });
            }
            AgentEvent::ToolCallStarted {
                name,
                call_id,
                input,
                is_subagent,
                subagent_model,
            } => {
                if let Some(turn) = s.turn.as_mut() {
                    turn.tool_calls += 1;
                    turn.last_tool = Some(name.clone());
                    turn.current_tool = Some(name.clone());
                    turn.current_call = Some(call_id.clone());
                }
                if *is_subagent {
                    push_subagent(&mut s.subagents, call_id, name, input, subagent_model, t);
                    sync_subagents(&mut s.subagents, &s.tasks);
                }
            }
            AgentEvent::ToolCallFinished { call_id, ok, .. } => {
                if let Some(turn) = s.turn.as_mut() {
                    if turn.current_call.as_deref() == Some(call_id.as_str()) {
                        turn.current_tool = None;
                        turn.current_call = None;
                    }
                }
                finish_subagent_call(&mut s.subagents, call_id, *ok, t);
            }
            AgentEvent::TaskStarted { .. }
            | AgentEvent::TaskProgress { .. }
            | AgentEvent::TaskUpdated { .. }
            | AgentEvent::TaskEnded { .. }
            | AgentEvent::BackgroundTasksChanged { .. } => {
                let before = running_background_ids(&s.tasks);
                let change = tasks::apply(&mut s.tasks, event, t);
                let after = running_background_ids(&s.tasks);
                sync_subagents(&mut s.subagents, &s.tasks);
                s.background_running = after.len() as u32;
                cause =
                    match change {
                        BackgroundChange::Started => after
                            .iter()
                            .find(|id| !before.contains(id))
                            .map(|id| PhaseCause::TaskStarted {
                                task_id: id.clone(),
                            }),
                        BackgroundChange::Ended => before
                            .iter()
                            .find(|id| !after.contains(id))
                            .map(|id| PhaseCause::TaskEnded {
                                task_id: id.clone(),
                            }),
                        BackgroundChange::None => None,
                    };
            }
            // Handled before the match: see `reduce_scoped`.
            AgentEvent::Scoped { .. } => {}
            AgentEvent::GateRequested { gate } => {
                if !self.external.contains_key(gate.request_id()) {
                    self.gate_facts.push(GateFact::Raised {
                        request_id: gate.request_id().to_string(),
                        kind: gate_kind(gate),
                        bound_to: "none",
                    });
                }
                s.pending_gates
                    .retain(|g| g.gate.request_id() != gate.request_id());
                s.pending_gates.push(PendingGate {
                    gate: gate.clone(),
                    raised_at_ms: t,
                    cursor,
                });
                cause = Some(PhaseCause::GateRaised {
                    request_id: gate.request_id().to_string(),
                    external: self.external.contains_key(gate.request_id()),
                });
            }
            AgentEvent::GateResolved { request_id, reason } => {
                s.pending_gates
                    .retain(|g| g.gate.request_id() != request_id);
                cause = Some(PhaseCause::GateCleared {
                    request_id: request_id.clone(),
                    // Only an external gate's resolution carries a reason.
                    how: if reason.is_some() {
                        GateClearing::Withdrawn
                    } else {
                        GateClearing::Resolved
                    },
                });
            }
            AgentEvent::InputDropped { input, .. } => {
                take_text(&mut self.prompt_texts, *input);
            }
            AgentEvent::UsageUpdate { usage, context_pct } => {
                s.live_usage = Some(usage.clone());
                if context_pct.is_some() {
                    s.context_pct = *context_pct;
                }
            }
            AgentEvent::PlanUpdated { entries } => s.plan = entries.clone(),
            AgentEvent::ModesUpdated {
                available_modes,
                current_mode,
            } => {
                if !available_modes.is_empty() {
                    s.available_modes = available_modes.clone();
                }
                if current_mode.is_some() {
                    s.mode = current_mode.clone();
                }
            }
            AgentEvent::ModelsUpdated { models, current } => {
                s.available_models = models.clone();
                if current.is_some() {
                    s.model = current.clone();
                }
            }
            AgentEvent::ModelChanged { model } => s.model = Some(model.clone()),
            AgentEvent::SlashCommandsUpdated { commands } => s.slash_commands = commands.clone(),
            AgentEvent::AssistantDelta { .. }
            | AgentEvent::ThinkingDelta { .. }
            | AgentEvent::ToolCallUpdated { .. }
            | AgentEvent::Compaction { .. }
            | AgentEvent::RateLimitNotice { .. }
            | AgentEvent::CommandRejected { .. }
            | AgentEvent::Notice { .. }
            | AgentEvent::AssistantContent { .. }
            | AgentEvent::UserContent { .. }
            | AgentEvent::UserMessage { .. }
            // Never reaches here: `step` keeps history out of the reducer.
            | AgentEvent::History { .. }
            | AgentEvent::ProcessExited { .. }
            | AgentEvent::LocalShell { .. }
            | AgentEvent::InputSent { .. }
            | AgentEvent::InputAcknowledged { .. }
            | AgentEvent::Unknown { .. } => {}
        }
        self.causes.extend(cause);
        match saved {
            Some(Reported(id)) => {
                let c = match (&self.conversation, self.saves) {
                    (_, SavesConversation::WhenReported) => Conversation::Saved(id),
                    (Conversation::Saved(had), _) if *had == id => Conversation::Saved(id),
                    _ => Conversation::Unsaved(id),
                };
                if c != self.conversation {
                    self.conversation_is(c);
                }
            }
            Some(TurnStarted) => {
                if let Conversation::Unsaved(id) = &self.conversation {
                    let c = Conversation::Saved(id.clone());
                    self.conversation_is(c);
                }
            }
            None => {}
        }
    }

    fn clear_live_state(&mut self, t: u64) {
        self.snap.turn = None;
        self.snap.pending_gates.clear();
        self.interrupt_applied = false;
        for request_id in std::mem::take(&mut self.external).into_keys() {
            let reason = GateReason::new(GateReason::PROCESS_ENDED);
            self.gate_facts.push(GateFact::Withdrawn {
                request_id: request_id.clone(),
                reason: reason.code.clone(),
            });
            self.remember_withdrawn(request_id, reason);
        }
        tasks::end_all(&mut self.snap.tasks, t);
        for a in self.snap.subagents.iter_mut().filter(|a| a.running) {
            a.running = false;
            a.ended_at_ms = Some(t);
        }
        self.snap.background_running = 0;
    }

    fn compute_phase(&self) -> Phase {
        if let Some(reason) = &self.lost {
            return Phase::Lost {
                reason: reason.clone(),
            };
        }
        match self.gen {
            Gen::None => Phase::Starting,
            Gen::Exited => self
                .exit_phase
                .clone()
                .unwrap_or(Phase::Exited { code: None }),
            Gen::Awaiting(_) | Gen::Running(_) => {
                // A pending gate needs the human whatever else is going on: one raised from
                // outside (a handoff) can come before the CLI's handshake.
                if !self.snap.pending_gates.is_empty() {
                    Phase::NeedsYou
                } else if !self.handshake_done {
                    Phase::Starting
                } else if self.snap.turn.is_some() || self.snap.background_running > 0 {
                    Phase::Working
                } else {
                    Phase::Idle
                }
            }
        }
    }

    /// A subagent's line (it names the call that spawned the subagent) is accounted for when the
    /// session knows that subagent, running or just ended: the agent flushes the last lines of a
    /// task right after its end frames. A line of a subagent nobody told us about is not.
    fn explained_by_a_known_subagent(&self, event: &AgentEvent) -> bool {
        match event.unscoped().0 {
            Some(scope) => {
                self.snap
                    .subagents
                    .iter()
                    .any(|a| a.call_id == scope.call_id)
                    || self
                        .snap
                        .tasks
                        .iter()
                        .any(|t| t.call_id.as_deref() == Some(scope.call_id.as_str()))
            }
            None => false,
        }
    }

    /// Why the phase became `phase`: the last fact of this record that leads to it, else the
    /// last fact, else `other`.
    fn cause_for(&self, phase: &Phase) -> PhaseCause {
        let leads_to = |c: &PhaseCause| match phase {
            Phase::Starting => matches!(c, PhaseCause::Spawned { .. }),
            Phase::Idle => matches!(
                c,
                PhaseCause::TurnCompleted { .. }
                    | PhaseCause::TaskEnded { .. }
                    | PhaseCause::GateCleared { .. }
                    | PhaseCause::HandshakeDone
            ),
            Phase::Working => matches!(
                c,
                PhaseCause::TurnStarted { .. }
                    | PhaseCause::TaskStarted { .. }
                    | PhaseCause::GateCleared { .. }
                    | PhaseCause::HandshakeDone
            ),
            Phase::NeedsYou => matches!(c, PhaseCause::GateRaised { .. }),
            Phase::Exited { .. } | Phase::Failed { .. } => matches!(c, PhaseCause::Exited { .. }),
            Phase::Lost { .. } => matches!(c, PhaseCause::Lost),
        };
        self.causes
            .iter()
            .rev()
            .find(|c| leads_to(c))
            .or(self.causes.last())
            .cloned()
            .unwrap_or_default()
    }

    // ── earlier history ─────────────────────────────────────────────────────

    /// What the first generation's start says about the conversation, and what a later one does
    /// to an earlier history that was interrupted. The rule, from journaled facts alone:
    /// the journal covers the conversation's beginning exactly when the session's first process
    /// generation started it (`Fresh`); a session whose first generation resumed (or continued)
    /// a conversation lacks everything before it, and gets it from the agent's replay or the
    /// import the start note planned, until that is complete.
    fn earlier_on_start(&mut self, spec: &SpawnSpec, plan: Option<EarlierPlan>, em: &mut Emitter) {
        self.earlier.replay_pos = 0;
        self.earlier.replay_ignored = false;
        if self.earlier.decided {
            if !self.earlier.closed && self.earlier.info.state != EarlierState::Loading {
                // An interrupted history goes on: the replay skips what the journal holds, the
                // import carries on from its next chunk.
                self.earlier.info.state = EarlierState::Loading;
                self.earlier.info.gaps.interrupted = false;
                self.earlier_changed(em);
            }
            return;
        }
        self.earlier.decided = true;
        if matches!(spec.resume, ResumeMode::Fresh) {
            // The conversation begins here: nothing earlier exists.
            self.earlier.closed = true;
            return;
        }
        let delivery = self.desc.capabilities().earlier_history;
        match (delivery, plan) {
            (wasit_core::HistoryDelivery::Wire, _) => {
                self.earlier.info.source = Some(EarlierSource::AgentReplay);
                self.earlier.info.state = EarlierState::Loading;
            }
            (wasit_core::HistoryDelivery::Storage, Some(EarlierPlan::Storage { path, len })) => {
                self.earlier.info.source = Some(EarlierSource::AgentStorage);
                self.earlier.info.state = EarlierState::Loading;
                self.earlier.import = Some((path, len));
            }
            (wasit_core::HistoryDelivery::Storage, Some(EarlierPlan::Unavailable { why })) => {
                self.earlier.info.source = Some(EarlierSource::AgentStorage);
                self.earlier.info.state = EarlierState::Unavailable { reason: why };
                self.earlier.closed = true;
            }
            // A journal of a build that planned nothing, or an agent with nothing to read.
            (wasit_core::HistoryDelivery::Storage | wasit_core::HistoryDelivery::None, _) => {
                self.earlier.closed = true;
                return;
            }
        }
        self.earlier_changed(em);
    }

    /// The snapshot's `earlier` changed: say so in the live stream.
    fn earlier_changed(&mut self, em: &mut Emitter) {
        em.push(SessionEventKind::Earlier {
            earlier: self.earlier.info.clone(),
        });
    }

    /// An item the agent replayed over its protocol ([`AgentEvent::History`]).
    fn earlier_replayed(&mut self, mut event: AgentEvent, em: &mut Emitter, out: &mut Output) {
        let e = &mut self.earlier;
        if e.closed || e.info.source != Some(EarlierSource::AgentReplay) {
            // The journal has this history already (or none is expected): a reloaded
            // conversation is replayed in full at every start.
            if !e.replay_ignored {
                e.replay_ignored = true;
                e.info.gaps.replays_ignored = e.info.gaps.replays_ignored.saturating_add(1);
            }
            return;
        }
        let pos = e.replay_pos;
        e.replay_pos += 1;
        if pos < e.accepted {
            // An interrupted replay went on: this item is in the journal already.
            return;
        }
        crate::media::externalize(&mut event, &mut out.blobs);
        if em.push_earlier(event) {
            e.accepted += 1;
            e.info.items += 1;
        } else {
            e.info.gaps.omitted += 1;
        }
    }

    /// The agent's handshake reported the conversation: whatever it replays came before.
    fn earlier_replay_done(&mut self, em: &mut Emitter) {
        let e = &mut self.earlier;
        if e.closed || e.info.source != Some(EarlierSource::AgentReplay) {
            return;
        }
        e.closed = true;
        e.info.state = EarlierState::Loaded {
            complete: e.info.gaps.is_empty(),
        };
        self.earlier_changed(em);
    }

    /// The process generation ended: a history still arriving will not, from this process.
    fn earlier_generation_ended(&mut self, em: &mut Emitter) {
        if self.earlier.info.state != EarlierState::Loading {
            return;
        }
        self.earlier.info.state = EarlierState::Loaded { complete: false };
        self.earlier.info.gaps.interrupted = true;
        self.earlier_changed(em);
    }

    /// The chunk of the imported history that starts with item `first`.
    fn on_earlier_chunk(
        &mut self,
        first: u64,
        events: Vec<AgentEvent>,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let e = &mut self.earlier;
        if e.import.is_none() || e.info.state != EarlierState::Loading {
            em.push(SessionEventKind::Notice {
                message: "ignored a chunk of earlier history outside an import".into(),
                code: NoticeCode::Other,
            });
            return;
        }
        if first > e.imported {
            em.push(SessionEventKind::Notice {
                message: format!(
                    "ignored a chunk of earlier history that starts at item {first}: item {} is next",
                    e.imported
                ),
                code: NoticeCode::Other,
            });
            return;
        }
        // What the journal holds already (a chunk journaled twice, an import resumed with another
        // chunking) is skipped by its number.
        let held = (e.imported - first) as usize;
        let mut changed = false;
        for mut event in events.into_iter().skip(held) {
            changed = true;
            e.imported += 1;
            if !matches!(event, AgentEvent::History { .. }) {
                e.info.gaps.unrecognised += 1;
                continue;
            }
            crate::media::externalize(&mut event, &mut out.blobs);
            if em.push_earlier(event) {
                e.info.items += 1;
            } else {
                e.info.gaps.omitted += 1;
            }
        }
        if changed {
            self.earlier_changed(em);
        }
    }

    /// The import ended: all its chunks are journaled.
    fn on_earlier_end(&mut self, report: &wasit_core::HistoryReport, em: &mut Emitter) {
        let e = &mut self.earlier;
        if e.import.is_none() || e.closed {
            return;
        }
        let replays = e.info.gaps.replays_ignored;
        e.info.gaps = EarlierGaps::of_report(report);
        e.info.gaps.replays_ignored = replays;
        e.closed = true;
        e.info.state = EarlierState::Loaded {
            complete: e.info.gaps.is_empty(),
        };
        self.earlier_changed(em);
    }

    /// The import failed: what it had journaled stays, and the rest is not coming.
    fn on_earlier_failed(&mut self, reason: EarlierUnavailable, em: &mut Emitter) {
        let e = &mut self.earlier;
        if e.import.is_none() || e.closed {
            return;
        }
        e.closed = true;
        if e.info.items == 0 {
            e.info.state = EarlierState::Unavailable { reason };
        } else {
            e.info.state = EarlierState::Loaded { complete: false };
            e.info.gaps.interrupted = true;
        }
        self.earlier_changed(em);
    }

    /// The first `start` note of the session was applied: what its conversation needs is known.
    pub(crate) fn earlier_decided(&self) -> bool {
        self.earlier.decided
    }

    /// The import the actor must run now (or carry on), when there is one: the history is being
    /// loaded from the agent's file, the process runs, and not every chunk is journaled.
    pub(crate) fn earlier_work(&self) -> Option<ImportWork> {
        let e = &self.earlier;
        match &e.import {
            Some((path, len))
                if e.info.state == EarlierState::Loading && self.process_running() =>
            {
                Some(ImportWork {
                    path: path.clone(),
                    len: *len,
                    skip: e.imported,
                })
            }
            _ => None,
        }
    }

    fn finish(&mut self, mut em: Emitter, out: &mut Output, phase_before: &Phase) {
        if let Gen::Running(p) = &self.gen {
            if !self.handshake_done && p.is_ready() {
                self.handshake_done = true;
                self.causes.push(PhaseCause::HandshakeDone);
            }
        }
        let phase = self.compute_phase();
        // The agent working while the session reads as not working (background work, a turn
        // the CLI started by itself): once per such period.
        if !self.anomaly_reported
            && phase == *phase_before
            && matches!(
                phase,
                Phase::Idle | Phase::Exited { .. } | Phase::Failed { .. }
            )
        {
            let first = em.events.iter().find_map(|e| match &e.kind {
                SessionEventKind::Agent { event } => {
                    activity(event).filter(|_| !self.explained_by_a_known_subagent(event))
                }
                _ => None,
            });
            if let Some(first) = first {
                self.anomaly_reported = true;
                em.push(SessionEventKind::Anomaly {
                    anomaly: AnomalyKind::ActivityWhileIdle,
                    first: first.to_string(),
                });
            }
        }
        if phase != self.snap.phase {
            self.snap.phase = phase.clone();
            self.anomaly_reported = false;
            let cause = self.cause_for(&phase);
            em.push(SessionEventKind::Phase { phase, cause });
            // `continuable` follows the phase.
            self.refresh_conversation();
        }
        if let Some(last) = em.events.last() {
            self.snap.cursor = last.cursor;
        }
        if self.snap.earlier != self.earlier.info {
            self.snap.earlier = self.earlier.info.clone();
        }
        out.events = em.events;
        out.earlier = em.earlier;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wasit_core::{GateDecision, TurnTrigger};

    fn meta() -> SessionMeta {
        SessionMeta {
            id: crate::wire::SessionId("aaaaaaaaaaaa".into()),
            driver: "claude".into(),
            cwd: None,
            labels: Default::default(),
            created_at_ms: 0,
            display_name: None,
            command: None,
            conversation: Conversation::Unknown,
            forked_from: None,
        }
    }

    struct J {
        seq: u64,
        recs: Vec<Record>,
    }

    impl J {
        fn new() -> Self {
            J {
                seq: 0,
                recs: vec![],
            }
        }
        fn push(&mut self, kind: Kind, data: serde_json::Value) -> Record {
            self.seq += 1;
            let r = Record {
                seq: self.seq,
                t: self.seq * 10,
                kind,
                data,
                partial: false,
            };
            self.recs.push(r.clone());
            r
        }
    }

    const INIT_OK: &str = r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{"commands":[],"models":[{"value":"haiku","displayName":"Haiku","description":"fast"}],"current_permission_mode":"default"}}}"#;

    /// claude reports only its current permission mode; the discovered choices stay offered.
    #[test]
    fn hinted_modes_survive_a_current_mode_only_report() {
        let desc = wasit_claude::descriptor();
        let mut c = Core::new(desc, &meta());
        let mode = |id: &str| wasit_core::SessionMode {
            id: id.into(),
            label: id.into(),
            description: String::new(),
        };
        c.hint_modes(vec![mode("manual"), mode("plan")]);
        let mut j = J::new();
        for (kind, data) in [
            (Kind::Spawned, json!({"pid": 7, "argv": ["claude"]})),
            (
                Kind::Note,
                RuntimeNote::Start {
                    spec: Box::new(SpawnSpec::default()),
                    trace: None,
                    earlier: None,
                }
                .to_value(),
            ),
            (Kind::Out, json!(INIT_OK)),
        ] {
            let rec = j.push(kind, data);
            c.apply(&rec);
        }
        let ids: Vec<_> = c
            .snapshot()
            .available_modes
            .iter()
            .map(|m| m.id.clone())
            .collect();
        assert_eq!(ids, ["manual", "plan"]);
        assert_eq!(c.snapshot().mode.as_deref(), Some("default"));
    }

    /// Drive a claude session through the core like a live actor would, recording the journal;
    /// then replay the journal through a fresh core and compare everything.
    #[test]
    fn replay_reproduces_events_snapshot_and_owed_writes() {
        let desc = wasit_claude::descriptor();
        let mut live = Core::new(desc.clone(), &meta());
        let mut j = J::new();
        let mut events = vec![];
        let mut feed = |core: &mut Core, j: &mut J, kind, data| {
            let rec = j.push(kind, data);
            let out = core.apply(&rec);
            // Echo every write as the shim would.
            let mut echo = vec![];
            for w in &out.writes {
                echo.push(w.clone());
            }
            events.extend(out.events.clone());
            for w in echo {
                let rec = j.push(Kind::In, json!(w));
                events.extend(core.apply(&rec).events);
            }
            out
        };
        feed(
            &mut live,
            &mut j,
            Kind::Spawned,
            json!({"pid": 7, "argv": ["claude"]}),
        );
        assert!(live.awaiting_start());
        let spec = SpawnSpec {
            first_prompt: Some("hello".into()),
            ..SpawnSpec::default()
        };
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Start {
                spec: Box::new(spec),
                trace: None,
                earlier: None,
            }
            .to_value(),
        );
        assert!(out.started);
        assert_eq!(live.snapshot().phase, Phase::Starting);
        feed(&mut live, &mut j, Kind::Out, json!(INIT_OK));
        assert_eq!(
            live.snapshot().phase,
            Phase::Idle,
            "the first prompt went out, but no turn runs until the CLI says so"
        );
        feed(
            &mut live,
            &mut j,
            Kind::Out,
            json!(r#"{"type":"system","subtype":"init","session_id":"s1","model":"haiku"}"#),
        );
        assert_eq!(
            live.snapshot().phase,
            Phase::Working,
            "the CLI started the turn"
        );
        feed(
            &mut live,
            &mut j,
            Kind::Out,
            json!(
                r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}}"#
            ),
        );
        assert_eq!(live.snapshot().phase, Phase::NeedsYou);
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Command {
                token: "tok".into(),
                command: Command::AnswerGate {
                    request_id: "g1".into(),
                    decision: GateDecision::choose("allow"),
                },
                trace: None,
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("tok".into(), CommandStatus::Sent)]);
        assert_eq!(live.snapshot().phase, Phase::Working);
        // A write the live side requested but the daemon died before writing: no echo.
        let rec = j.push(
            Kind::Note,
            RuntimeNote::Command {
                token: "t2".into(),
                command: Command::SetModel {
                    model: "haiku".into(),
                },
                trace: None,
            }
            .to_value(),
        );
        let out = live.apply(&rec);
        events.extend(out.events);
        assert_eq!(out.writes.len(), 1);
        assert_eq!(live.owed().count(), 1);

        let mut replay = Core::new(desc, &meta());
        let mut revents = vec![];
        for r in &j.recs {
            revents.extend(replay.apply(r).events);
        }
        assert_eq!(revents, events);
        assert_eq!(replay.snapshot(), live.snapshot());
        assert_eq!(
            replay.owed().collect::<Vec<_>>(),
            live.owed().collect::<Vec<_>>()
        );
        assert!(events.windows(2).all(|w| w[0].cursor < w[1].cursor));
    }

    /// A gate raised from outside (a handoff) while the CLI is still starting needs the human
    /// at once: the session reads `needs_you`, not `starting`.
    #[test]
    fn a_gate_raised_while_starting_needs_you() {
        let mut live = Core::new(wasit_claude::descriptor(), &meta());
        let mut j = J::new();
        let feed = |core: &mut Core, j: &mut J, kind, data: serde_json::Value| {
            let rec = j.push(kind, data);
            core.apply(&rec)
        };
        feed(&mut live, &mut j, Kind::Spawned, json!({"pid": 7}));
        feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Start {
                spec: Box::new(SpawnSpec::default()),
                trace: None,
                earlier: None,
            }
            .to_value(),
        );
        assert_eq!(live.snapshot().phase, Phase::Starting);
        let gate = wasit_core::GateRequest::Handoff {
            request_id: "x1".into(),
            task_id: "t1".into(),
            from: "h/a".into(),
            to: "h/b".into(),
            mode: "consult".into(),
            instruction: "look".into(),
            details: serde_json::Value::Null,
            choices: vec![wasit_core::GateChoice::agent(
                "allow",
                "Allow",
                wasit_core::GateChoiceKind::AllowOnce,
            )],
        };
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r1".into(),
                gate: Box::new(gate),
                binding: None,
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("r1".into(), CommandStatus::Sent)]);
        assert_eq!(live.snapshot().phase, Phase::NeedsYou);
        feed(&mut live, &mut j, Kind::Out, json!(INIT_OK));
        assert_eq!(live.snapshot().phase, Phase::NeedsYou);
    }

    /// External gates: pending like CLI gates, kept across a turn end, answered without the
    /// protocol (nothing written), withdrawable, cleared by the exit — and replay-exact.
    #[test]
    fn external_gates_are_journaled_answered_and_replayed() {
        let desc = wasit_claude::descriptor();
        let mut live = Core::new(desc.clone(), &meta());
        let mut j = J::new();
        let mut events = vec![];
        let mut feed = |core: &mut Core, j: &mut J, kind, data: serde_json::Value| {
            let rec = j.push(kind, data);
            let out = core.apply(&rec);
            events.extend(out.events.clone());
            out
        };
        let handoff = |rid: &str| wasit_core::GateRequest::Handoff {
            request_id: rid.into(),
            task_id: "t1".into(),
            from: "h/a".into(),
            to: "h/b".into(),
            mode: "consult".into(),
            instruction: "look".into(),
            details: serde_json::Value::Null,
            choices: vec![wasit_core::GateChoice::agent(
                "allow",
                "Allow",
                wasit_core::GateChoiceKind::AllowOnce,
            )],
        };
        // Not running yet: refused.
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r0".into(),
                gate: Box::new(handoff("x0")),
                binding: None,
            }
            .to_value(),
        );
        assert!(matches!(
            out.resolved[0].1,
            CommandStatus::NotRunning { .. }
        ));
        feed(&mut live, &mut j, Kind::Spawned, json!({"pid": 7}));
        feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Start {
                spec: Box::new(SpawnSpec {
                    first_prompt: Some("go".into()),
                    ..SpawnSpec::default()
                }),
                trace: None,
                earlier: None,
            }
            .to_value(),
        );
        feed(&mut live, &mut j, Kind::Out, json!(INIT_OK));
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r1".into(),
                gate: Box::new(handoff("x1")),
                binding: None,
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("r1".into(), CommandStatus::Sent)]);
        assert!(out.writes.is_empty());
        assert_eq!(live.snapshot().phase, Phase::NeedsYou);
        // Same id twice: rejected.
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r2".into(),
                gate: Box::new(handoff("x1")),
                binding: None,
            }
            .to_value(),
        );
        assert!(matches!(out.resolved[0].1, CommandStatus::Rejected { .. }));
        // The turn ends: the external gate stays.
        feed(
            &mut live,
            &mut j,
            Kind::Out,
            json!(
                r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1}"#
            ),
        );
        assert_eq!(live.snapshot().pending_gates.len(), 1);
        assert_eq!(live.snapshot().phase, Phase::NeedsYou);
        // Answered through the normal command path: nothing reaches the CLI.
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Command {
                token: "a1".into(),
                command: Command::AnswerGate {
                    request_id: "x1".into(),
                    decision: GateDecision::choose("allow"),
                },
                trace: None,
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("a1".into(), CommandStatus::Sent)]);
        assert!(out.writes.is_empty());
        assert!(live.snapshot().pending_gates.is_empty());
        assert_eq!(live.snapshot().phase, Phase::Idle);
        assert!(out.events.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Input { command: Command::AnswerGate { request_id, .. }, status: CommandStatus::Sent, .. } if request_id == "x1"
        )));
        // Raise + withdraw.
        feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r3".into(),
                gate: Box::new(handoff("x2")),
                binding: None,
            }
            .to_value(),
        );
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::ResolveGate {
                token: "w1".into(),
                request_id: "x2".into(),
                reason: GateReason::new("withdrawn").with_detail("the asking turn ended"),
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("w1".into(), CommandStatus::Sent)]);
        assert!(live.snapshot().pending_gates.is_empty());
        assert!(out.events.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent { event: AgentEvent::GateResolved { request_id, reason } }
                if request_id == "x2" && reason.as_ref().is_some_and(|r| r.code == "withdrawn")
        )));
        // A late answer is refused (typed), and the id cannot be raised again.
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Command {
                token: "a2".into(),
                command: Command::AnswerGate {
                    request_id: "x2".into(),
                    decision: GateDecision::choose("allow"),
                },
                trace: None,
            }
            .to_value(),
        );
        assert_eq!(
            out.resolved,
            vec![(
                "a2".into(),
                CommandStatus::Withdrawn {
                    request_id: "x2".into(),
                    reason: Some(GateReason::new("withdrawn").with_detail("the asking turn ended")),
                }
            )]
        );
        assert!(out.writes.is_empty());
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r5".into(),
                gate: Box::new(handoff("x2")),
                binding: None,
            }
            .to_value(),
        );
        assert!(matches!(out.resolved[0].1, CommandStatus::Rejected { .. }));
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::ResolveGate {
                token: "w2".into(),
                request_id: "x2".into(),
                reason: GateReason::new("withdrawn"),
            }
            .to_value(),
        );
        assert!(matches!(out.resolved[0].1, CommandStatus::Rejected { .. }));
        // A pending external gate ends with the process.
        feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::RaiseGate {
                token: "r4".into(),
                gate: Box::new(handoff("x3")),
                binding: None,
            }
            .to_value(),
        );
        assert_eq!(live.snapshot().pending_gates.len(), 1);
        let mut before_exit = Core::new(desc.clone(), &meta());
        for r in &j.recs {
            before_exit.apply(r);
        }
        assert_eq!(before_exit.snapshot(), live.snapshot());
        let out = feed(&mut live, &mut j, Kind::Exited, json!({"code": 0}));
        assert!(live.snapshot().pending_gates.is_empty());
        assert!(out.events.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent { event: AgentEvent::GateResolved { request_id, reason } }
                if request_id == "x3" && reason.as_ref().is_some_and(|r| r.code == "process_ended")
        )));
        let out = feed(
            &mut live,
            &mut j,
            Kind::Note,
            RuntimeNote::Command {
                token: "a3".into(),
                command: Command::AnswerGate {
                    request_id: "x3".into(),
                    decision: GateDecision::choose("allow"),
                },
                trace: None,
            }
            .to_value(),
        );
        assert!(matches!(out.resolved[0].1, CommandStatus::Withdrawn { .. }));

        let mut replay = Core::new(desc, &meta());
        let mut revents = vec![];
        for r in &j.recs {
            revents.extend(replay.apply(r).events);
        }
        assert_eq!(revents, events);
        assert_eq!(replay.snapshot(), live.snapshot());
    }

    /// A host-run command is an event of origin `host` at its journal position, answers its
    /// token, and replays to the same events.
    #[test]
    fn local_shell_note_is_a_host_event_in_journal_order() {
        let mut sim = Sim::new();
        let out = sim.note(RuntimeNote::LocalShell {
            token: "sh1".into(),
            command: "ls".into(),
            cwd: Some("/w".into()),
            exit_code: Some(0),
            output: "a\n".into(),
            output_truncated: false,
        });
        assert_eq!(out.resolved, vec![("sh1".to_string(), CommandStatus::Sent)]);
        assert_eq!(sim.events.len(), 1);
        assert!(matches!(
            &sim.events[0].kind,
            SessionEventKind::Agent {
                event: AgentEvent::LocalShell {
                    command,
                    cwd: Some(cwd),
                    exit_code: Some(0),
                    output,
                    output_truncated: false,
                    origin: wasit_core::LocalShellOrigin::Host,
                },
            } if command == "ls" && cwd == "/w" && output == "a\n"
        ));
        let mut replay = Core::new(sim.desc.clone(), &meta());
        let mut revents = vec![];
        for r in &sim.j.recs {
            revents.extend(replay.apply(r).events);
        }
        assert_eq!(revents, sim.events);
    }

    // ── external gates bound to a turn ──────────────────────────────────────

    fn question_gate(rid: &str) -> wasit_core::GateRequest {
        wasit_core::GateRequest::Handoff {
            request_id: rid.into(),
            task_id: "t1".into(),
            from: "h/a".into(),
            to: "h/b".into(),
            mode: "consult".into(),
            instruction: "look".into(),
            details: serde_json::Value::Null,
            choices: vec![wasit_core::GateChoice::agent(
                "allow",
                "Allow",
                wasit_core::GateChoiceKind::AllowOnce,
            )],
        }
    }

    fn raise(sim: &mut Sim, rid: &str, binding: Option<TurnBinding>) -> CommandStatus {
        let out = sim.note(RuntimeNote::RaiseGate {
            token: format!("raise-{rid}"),
            gate: Box::new(question_gate(rid)),
            binding,
        });
        out.resolved[0].1.clone()
    }

    /// Both endings, with the details the raiser words.
    fn whole_turn() -> TurnBinding {
        TurnBinding::Turn {
            interrupted: Some("the user interrupted the turn".into()),
            ended: Some("the turn ended before an answer".into()),
        }
    }

    /// The order of what matters about gates and the turn: interrupts, gate raises and
    /// resolutions (with their reason code and detail) and turn completions.
    fn gate_trace(sim: &Sim) -> Vec<String> {
        sim.events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Input {
                    command: Command::Interrupt,
                    ..
                } => Some("interrupt".to_string()),
                SessionEventKind::Agent {
                    event: AgentEvent::GateRequested { gate },
                } => Some(format!("requested:{}", gate.request_id())),
                SessionEventKind::Agent {
                    event: AgentEvent::GateResolved { request_id, reason },
                } => Some(match reason {
                    Some(r) => format!(
                        "resolved:{request_id}:{}:{}",
                        r.code,
                        r.detail.as_deref().unwrap_or("-")
                    ),
                    None => format!("resolved:{request_id}"),
                }),
                SessionEventKind::Agent {
                    event:
                        AgentEvent::TurnCompleted {
                            was_interrupted, ..
                        },
                } => Some(format!("turn_completed:{was_interrupted}")),
                _ => None,
            })
            .collect()
    }

    /// A session in the middle of a turn that the CLI started for `"go"`.
    fn sim_in_a_turn() -> Sim {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        sim.command(Command::prompt("go"));
        sim.cli_starts_turn_for_the_last_prompt();
        assert!(sim.core.snapshot().turn.is_some());
        sim
    }

    const RESULT_INTERRUPTED: &str =
        r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1}"#;

    /// The interrupt resolves the turn's gate in its own step: right behind the `input` event
    /// of the interrupt, before anything the CLI says about it and before the turn completes.
    #[test]
    fn an_interrupt_resolves_the_gates_of_the_turn_before_it_completes() {
        let mut sim = sim_in_a_turn();
        assert_eq!(
            raise(&mut sim, "g1", Some(whole_turn())),
            CommandStatus::Sent
        );
        assert_eq!(
            raise(&mut sim, "g2", Some(whole_turn())),
            CommandStatus::Sent
        );
        assert_eq!(sim.core.snapshot().pending_gates.len(), 2);
        let (_, out) = sim.command(Command::Interrupt);
        // The same step: the interrupt, then both gates in the order they were raised, then
        // what the driver wrote for the interrupt; the gates are gone from the snapshot while
        // the turn is still open.
        let kinds: Vec<String> = out.events.iter().map(|e| short(&e.kind)).collect();
        assert!(kinds[0].starts_with("input"), "{kinds:?}");
        assert_eq!(kinds[1], "gate_resolved", "{kinds:?}");
        assert_eq!(kinds[2], "gate_resolved", "{kinds:?}");
        assert!(sim.core.snapshot().pending_gates.is_empty());
        assert!(sim.core.snapshot().turn.is_some());
        sim.feed(Kind::Out, json!(RESULT_INTERRUPTED));
        let why = "the user interrupted the turn";
        assert_eq!(
            gate_trace(&sim),
            [
                "requested:g1".to_string(),
                "requested:g2".into(),
                "interrupt".into(),
                format!("resolved:g1:turn_interrupted:{why}"),
                format!("resolved:g2:turn_interrupted:{why}"),
                "turn_completed:true".into(),
            ]
        );
        // The gate is withdrawn: a late answer is refused as such.
        let (_, out) = sim.command(Command::AnswerGate {
            request_id: "g1".into(),
            decision: GateDecision::choose("allow"),
        });
        // Journaled, refused as withdrawn, and the session says the answer was not delivered.
        let kinds: Vec<&SessionEventKind> = out.events.iter().map(|e| &e.kind).collect();
        assert!(
            matches!(
                kinds.as_slice(),
                [
                    ..,
                    SessionEventKind::Input {
                        status: CommandStatus::Withdrawn { .. },
                        ..
                    },
                    SessionEventKind::Notice {
                        code: NoticeCode::AnswerNotDelivered,
                        ..
                    }
                ]
            ),
            "{kinds:?}"
        );
        sim.assert_replay_exact();
    }

    /// A turn that ends normally takes its gate with it, resolved right before the completion.
    #[test]
    fn a_turn_ending_without_an_answer_resolves_its_gates_right_before_it_completes() {
        let mut sim = sim_in_a_turn();
        raise(&mut sim, "g1", Some(whole_turn()));
        let out = sim.feed(Kind::Out, json!(RESULT_OK));
        assert_eq!(
            gate_trace(&sim),
            [
                "requested:g1".to_string(),
                "resolved:g1:turn_ended:the turn ended before an answer".into(),
                "turn_completed:false".into(),
            ]
        );
        // One record, in this order, with nothing pending when the turn is reported over.
        let kinds: Vec<String> = out.events.iter().map(|e| short(&e.kind)).collect();
        let at = |n: &str| kinds.iter().position(|k| k == n).unwrap();
        assert!(at("gate_resolved") < at("turn_completed"), "{kinds:?}");
        assert!(sim.core.snapshot().pending_gates.is_empty());
        assert_eq!(sim.core.snapshot().phase, Phase::Idle);
        sim.assert_replay_exact();
    }

    /// An interrupt that the CLI answers with a normal result (it ended the turn first): the
    /// gate was the interrupted turn's and says so, once.
    #[test]
    fn an_interrupt_that_loses_the_race_to_a_normal_end_still_resolved_the_gate_as_interrupted() {
        let mut sim = sim_in_a_turn();
        raise(&mut sim, "g1", Some(whole_turn()));
        sim.command(Command::Interrupt);
        sim.feed(Kind::Out, json!(RESULT_OK));
        let trace = gate_trace(&sim);
        assert_eq!(
            trace.iter().filter(|t| t.starts_with("resolved")).count(),
            1
        );
        assert!(trace[2].contains("turn_interrupted"), "{trace:?}");
        assert_eq!(trace[3], "turn_completed:false");
        sim.assert_replay_exact();
    }

    /// Without a binding a gate belongs to the session: turns end and are interrupted around it.
    #[test]
    fn a_session_gate_is_untouched_by_turn_ends_and_interrupts() {
        let mut sim = sim_in_a_turn();
        assert_eq!(raise(&mut sim, "s1", None), CommandStatus::Sent);
        sim.command(Command::Interrupt);
        sim.feed(Kind::Out, json!(RESULT_INTERRUPTED));
        sim.command(Command::prompt("again"));
        sim.cli_starts_turn_for_the_last_prompt();
        sim.feed(Kind::Out, json!(RESULT_OK));
        assert_eq!(sim.core.snapshot().pending_gates.len(), 1);
        assert_eq!(sim.core.snapshot().phase, Phase::NeedsYou);
        assert!(gate_trace(&sim).iter().all(|t| !t.starts_with("resolved")));
        sim.assert_replay_exact();
    }

    /// An interrupt-only binding follows an interrupt of its turn but not a normal end, after
    /// which the gate belongs to the session (a later turn's interrupt leaves it).
    #[test]
    fn an_interrupt_binding_survives_a_normal_end_and_then_belongs_to_the_session() {
        let only_interrupt = || TurnBinding::Interrupt {
            detail: Some("interrupted".into()),
        };
        let mut sim = sim_in_a_turn();
        raise(&mut sim, "h1", Some(only_interrupt()));
        sim.feed(Kind::Out, json!(RESULT_OK));
        assert_eq!(sim.core.snapshot().pending_gates.len(), 1);
        sim.command(Command::prompt("next"));
        sim.cli_starts_turn_for_the_last_prompt();
        sim.command(Command::Interrupt);
        sim.feed(Kind::Out, json!(RESULT_INTERRUPTED));
        assert_eq!(
            sim.core.snapshot().pending_gates.len(),
            1,
            "now the session's"
        );
        // Another one, bound to the turn that is interrupted: resolved by it.
        let mut sim = sim_in_a_turn();
        raise(&mut sim, "h2", Some(only_interrupt()));
        sim.command(Command::Interrupt);
        sim.feed(Kind::Out, json!(RESULT_INTERRUPTED));
        assert_eq!(
            gate_trace(&sim),
            [
                "requested:h2".to_string(),
                "interrupt".into(),
                "resolved:h2:turn_interrupted:interrupted".into(),
                "turn_completed:true".into(),
            ]
        );
        sim.assert_replay_exact();
    }

    /// Raised with no turn open, a gate belongs to the session whatever its binding: there is no
    /// turn to follow, and the next turns neither end nor interrupt it.
    #[test]
    fn a_gate_raised_with_no_turn_open_belongs_to_the_session() {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        assert!(sim.core.snapshot().turn.is_none());
        assert_eq!(
            raise(&mut sim, "n1", Some(whole_turn())),
            CommandStatus::Sent
        );
        assert_eq!(
            raise(
                &mut sim,
                "n2",
                Some(TurnBinding::Interrupt { detail: None })
            ),
            CommandStatus::Sent
        );
        assert_eq!(
            gate_trace(&sim),
            ["requested:n1".to_string(), "requested:n2".into()]
        );
        // Turns come and go (one interrupted) around them.
        sim.command(Command::prompt("go"));
        sim.cli_starts_turn_for_the_last_prompt();
        sim.command(Command::Interrupt);
        sim.feed(Kind::Out, json!(RESULT_INTERRUPTED));
        sim.command(Command::prompt("again"));
        sim.cli_starts_turn_for_the_last_prompt();
        sim.feed(Kind::Out, json!(RESULT_OK));
        assert_eq!(sim.core.snapshot().pending_gates.len(), 2);
        assert!(gate_trace(&sim).iter().all(|t| !t.starts_with("resolved")));
        sim.assert_replay_exact();
    }

    /// Raised after the interrupt was applied but before the CLI ended the turn: the turn is
    /// as good as over, so the gate is resolved as interrupted in the raising record.
    #[test]
    fn a_gate_raised_on_a_turn_already_interrupted_is_resolved_as_interrupted() {
        let mut sim = sim_in_a_turn();
        sim.command(Command::Interrupt);
        assert_eq!(
            raise(&mut sim, "g1", Some(whole_turn())),
            CommandStatus::Sent
        );
        assert!(sim.core.snapshot().pending_gates.is_empty());
        sim.feed(Kind::Out, json!(RESULT_INTERRUPTED));
        // A turn after it is a new turn: its gates are its own.
        sim.command(Command::prompt("again"));
        sim.cli_starts_turn_for_the_last_prompt();
        assert_eq!(
            raise(&mut sim, "g2", Some(whole_turn())),
            CommandStatus::Sent
        );
        assert_eq!(sim.core.snapshot().pending_gates.len(), 1);
        assert_eq!(
            gate_trace(&sim),
            [
                "interrupt".to_string(),
                "requested:g1".into(),
                "resolved:g1:turn_interrupted:the user interrupted the turn".into(),
                "turn_completed:true".into(),
                "requested:g2".into(),
            ]
        );
        sim.assert_replay_exact();
    }

    /// An answer wins over the turn's end: the answered gate is not resolved again.
    #[test]
    fn an_answered_gate_is_not_resolved_by_the_end_of_its_turn() {
        let mut sim = sim_in_a_turn();
        raise(&mut sim, "g1", Some(whole_turn()));
        sim.command(Command::AnswerGate {
            request_id: "g1".into(),
            decision: GateDecision::choose("allow"),
        });
        sim.feed(Kind::Out, json!(RESULT_OK));
        assert!(gate_trace(&sim).iter().all(|t| !t.starts_with("resolved")));
        sim.assert_replay_exact();
    }

    /// The process ending still resolves a bound gate with the runtime's process reasons.
    #[test]
    fn the_end_of_the_process_resolves_a_bound_gate_with_its_own_reason() {
        let mut sim = sim_in_a_turn();
        raise(&mut sim, "g1", Some(whole_turn()));
        sim.feed(Kind::Exited, json!({"code": 1}));
        assert_eq!(
            gate_trace(&sim),
            [
                "requested:g1".to_string(),
                "resolved:g1:process_ended:-".into()
            ]
        );
        sim.assert_replay_exact();
    }

    #[test]
    fn exits_are_classified() {
        let desc = wasit_claude::descriptor();
        let run = |code: Option<i32>, stop: bool, timeout: bool| {
            let mut c = Core::new(desc.clone(), &meta());
            let mut j = J::new();
            c.apply(&j.push(Kind::Spawned, json!({"pid": 1})));
            c.apply(
                &j.push(
                    Kind::Note,
                    RuntimeNote::Start {
                        spec: Box::default(),
                        trace: None,
                        earlier: None,
                    }
                    .to_value(),
                ),
            );
            c.apply(&j.push(Kind::Err, json!("boom")));
            if stop {
                c.apply(&j.push(
                    Kind::Note,
                    RuntimeNote::Stop { token: "s".into() }.to_value(),
                ));
            }
            if timeout {
                c.apply(&j.push(
                    Kind::Note,
                    RuntimeNote::HandshakeTimeout { after_ms: 5 }.to_value(),
                ));
            }
            let data = match code {
                Some(c) => json!({"code": c}),
                None => json!({"signal": 9}),
            };
            c.apply(&j.push(Kind::Exited, data));
            c.snapshot().phase.clone()
        };
        assert_eq!(run(Some(0), false, false), Phase::Exited { code: Some(0) });
        assert_eq!(run(None, true, false), Phase::Exited { code: None });
        assert!(
            matches!(run(Some(3), false, false), Phase::Failed { reason, stderr_tail } if reason.contains("code 3") && stderr_tail == vec!["boom".to_string()])
        );
        assert!(
            matches!(run(None, false, false), Phase::Failed { reason, .. } if reason.contains("signal 9"))
        );
        assert!(
            matches!(run(None, false, true), Phase::Failed { reason, .. } if reason.contains("handshake"))
        );
    }

    #[test]
    fn lost_and_partial_lines() {
        let desc = wasit_claude::descriptor();
        let mut c = Core::new(desc, &meta());
        let mut j = J::new();
        c.apply(&j.push(Kind::Spawned, json!({"pid": 1})));
        c.apply(
            &j.push(
                Kind::Note,
                RuntimeNote::Start {
                    spec: Box::default(),
                    trace: None,
                    earlier: None,
                }
                .to_value(),
            ),
        );
        let (a, b) = INIT_OK.split_at(20);
        let mut r = j.push(Kind::Out, json!(a));
        r.partial = true;
        assert!(c.apply(&r).events.is_empty());
        c.apply(&j.push(Kind::Out, json!(b)));
        assert_eq!(c.snapshot().phase, Phase::Idle);
        assert_eq!(c.snapshot().available_models.len(), 1);
        let ev = c.mark_lost("gone");
        assert_eq!(ev.len(), 2);
        assert!(ev[0].cursor < ev[1].cursor);
        assert!(matches!(c.snapshot().phase, Phase::Lost { .. }));
        assert!(c.mark_lost("again").is_empty());
    }

    // ── inputs: first prompt, ids, written, drops, turn association, causes, anomaly ──

    const RESULT_OK: &str =
        r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1}"#;

    /// `system/init`, printed at the start of every turn.
    const TURN_INIT: &str = r#"{"type":"system","subtype":"init","session_id":"s1","model":"haiku","cwd":"/w","tools":[],"mcp_servers":[],"slash_commands":[],"permissionMode":"default","claude_code_version":"2.1.293"}"#;

    fn lifecycle_line(uuid: &str, state: &str) -> String {
        json!({"type":"command_lifecycle","command_uuid":uuid,"state":state,
               "uuid":"e9cc3bf9-5691-43dc-9491-cf9459579ef0","session_id":"s1"})
        .to_string()
    }

    /// Drives a core like the live actor (records in, the shim's `in` echo for writes when
    /// asked), keeping the journal so a replay can be compared.
    struct Sim {
        desc: Arc<dyn DriverDescriptor>,
        meta: SessionMeta,
        core: Core,
        j: J,
        events: Vec<SessionEvent>,
    }

    impl Sim {
        fn new() -> Self {
            Sim::of(wasit_claude::descriptor(), meta())
        }

        fn of(desc: Arc<dyn DriverDescriptor>, meta: SessionMeta) -> Self {
            Sim {
                core: Core::new(desc.clone(), &meta),
                desc,
                meta,
                j: J::new(),
                events: vec![],
            }
        }

        /// Apply one record (no echo).
        fn rec(&mut self, kind: Kind, data: serde_json::Value) -> Output {
            let r = self.j.push(kind, data);
            let out = self.core.apply(&r);
            self.events.extend(out.events.clone());
            out
        }

        /// The shim writes (and journals) every line `out` asked for.
        fn echo(&mut self, out: &Output) {
            for w in &out.writes {
                self.rec(Kind::In, json!(w));
            }
        }

        fn feed(&mut self, kind: Kind, data: serde_json::Value) -> Output {
            let out = self.rec(kind, data);
            self.echo(&out);
            out
        }

        fn note(&mut self, n: RuntimeNote) -> Output {
            self.feed(Kind::Note, n.to_value())
        }

        fn command(&mut self, command: Command) -> (InputId, Output) {
            let out = self.note(RuntimeNote::Command {
                token: format!("t{}", self.j.seq + 1),
                command,
                trace: None,
            });
            (InputId(self.j.seq - out.writes.len() as u64), out)
        }

        fn start(&mut self, first_prompt: Option<&str>) -> Output {
            self.feed(Kind::Spawned, json!({"pid": 7, "argv": ["claude"]}));
            self.note(RuntimeNote::Start {
                spec: Box::new(SpawnSpec {
                    first_prompt: first_prompt.map(str::to_string),
                    session_id: Some("aaaaaaaaaaaa".into()),
                    ..SpawnSpec::default()
                }),
                trace: None,
                earlier: None,
            })
        }

        /// A fresh core over the same journal publishes exactly the same events and state.
        fn assert_replay_exact(&self) {
            let mut replay = Core::new(self.desc.clone(), &self.meta);
            let mut events = vec![];
            for r in &self.j.recs {
                events.extend(replay.apply(r).events);
            }
            assert_eq!(events, self.events);
            assert_eq!(replay.snapshot(), self.core.snapshot());
            assert_eq!(
                replay.owed().collect::<Vec<_>>(),
                self.core.owed().collect::<Vec<_>>()
            );
            assert!(self.events.windows(2).all(|w| w[0].cursor < w[1].cursor));
        }

        fn kinds(&self) -> Vec<String> {
            self.events.iter().map(|e| short(&e.kind)).collect()
        }

        /// The `uuid` of the latest user message the shim wrote.
        fn last_uuid(&self) -> String {
            self.j
                .recs
                .iter()
                .rev()
                .filter(|r| r.kind == Kind::In)
                .filter_map(|r| serde_json::from_str::<serde_json::Value>(r.text()?).ok())
                .find(|v| v["type"] == "user")
                .and_then(|v| v["uuid"].as_str().map(str::to_string))
                .expect("a user message with a uuid was written")
        }

        /// The CLI takes the latest written prompt in and starts its turn: what the real
        /// CLI prints (`queued`, `started`, then `system/init`).
        fn cli_starts_turn_for_the_last_prompt(&mut self) {
            let uuid = self.last_uuid();
            self.feed(Kind::Out, json!(lifecycle_line(&uuid, "queued")));
            self.feed(Kind::Out, json!(lifecycle_line(&uuid, "started")));
            self.feed(Kind::Out, json!(TURN_INIT));
        }
    }

    /// A compact name of an event, with the input it is about.
    fn short(k: &SessionEventKind) -> String {
        let v = serde_json::to_value(k).unwrap();
        let name = match k {
            SessionEventKind::Agent { event } => serde_json::to_value(event).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string(),
            _ => v["kind"].as_str().unwrap().to_string(),
        };
        let input = match k {
            SessionEventKind::Agent {
                event:
                    AgentEvent::InputSent { input }
                    | AgentEvent::InputAcknowledged { input, .. }
                    | AgentEvent::InputDropped { input, .. }
                    | AgentEvent::TurnStarted {
                        input: Some(input), ..
                    },
            }
            | SessionEventKind::Input { input, .. }
            | SessionEventKind::Written { input }
            | SessionEventKind::WriteFailed {
                input: Some(input), ..
            } => Some(*input),
            _ => None,
        };
        match input {
            Some(i) => format!("{name}:{i}"),
            None => name,
        }
    }

    fn phases(sim: &Sim) -> Vec<(String, PhaseCause)> {
        sim.events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Phase { phase, cause } => {
                    Some((phase.name().to_string(), cause.clone()))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn the_first_prompt_is_an_ordinary_input_applied_with_the_start() {
        let mut sim = Sim::new();
        let out = sim.start(Some("hello"));
        // Journaled in the start note (seq 2): its id is that seq.
        let start_events: Vec<String> = out.events.iter().map(|e| short(&e.kind)).collect();
        assert_eq!(start_events, ["started", "input:2"], "{:#?}", out.events);
        assert!(matches!(
            &out.events[0].kind,
            SessionEventKind::Started {
                generation: 1,
                first_input: Some(InputId(2)),
                ..
            }
        ));
        assert!(matches!(
            &out.events[1].kind,
            SessionEventKind::Input {
                command: Command::Prompt { text, .. },
                status: CommandStatus::Queued,
                ..
            } if text == "hello"
        ));
        assert!(out.resolved.is_empty(), "nobody waits for a first prompt");
        sim.feed(Kind::Out, json!(INIT_OK));
        let k = sim.kinds();
        let at = |name: &str| {
            k.iter()
                .position(|x| x == name)
                .unwrap_or_else(|| panic!("{name} in {k:?}"))
        };
        assert!(at("input:2") < at("input_sent:2"));
        assert!(at("input_sent:2") < at("written:2"));
        assert!(!k.iter().any(|x| x.starts_with("turn_started")), "{k:?}");
        assert_eq!(sim.core.snapshot().phase, Phase::Idle);
        // The CLI takes it in and starts the turn for it: acknowledged, then the turn.
        sim.cli_starts_turn_for_the_last_prompt();
        let k = sim.kinds();
        let at = |name: &str| {
            k.iter()
                .position(|x| x == name)
                .unwrap_or_else(|| panic!("{name} in {k:?}"))
        };
        assert!(at("written:2") < at("input_acknowledged:2"));
        assert!(at("input_acknowledged:2") < at("turn_started:2"));
        assert_eq!(k.iter().filter(|x| x.starts_with("input:")).count(), 1);
        let snap = sim.core.snapshot();
        assert_eq!(snap.phase, Phase::Working);
        let turn = snap.turn.as_ref().unwrap();
        assert_eq!(turn.prompt.as_deref(), Some("hello"));
        assert_eq!(turn.input, Some(InputId(2)));
        assert_eq!(turn.trigger, TurnTrigger::Input);
        assert_eq!(snap.title.as_deref(), Some("hello"));
        sim.feed(Kind::Out, json!(RESULT_OK));
        assert_eq!(sim.core.snapshot().phase, Phase::Idle);
        sim.assert_replay_exact();
    }

    fn trace(s: &str) -> Option<TraceId> {
        Some(TraceId::new(s).unwrap())
    }

    /// The trace journaled with a note shows on the `input` event it produces and on `started`:
    /// the first prompt takes the one of the start note, a command its own, and a command
    /// without one has none. A replay reproduces all of it.
    #[test]
    fn the_trace_of_a_request_reaches_its_input_event() {
        let mut sim = Sim::new();
        sim.feed(Kind::Spawned, json!({"pid": 7, "argv": ["claude"]}));
        let start = sim.note(RuntimeNote::Start {
            spec: Box::new(SpawnSpec {
                first_prompt: Some("first".into()),
                session_id: Some("aaaaaaaaaaaa".into()),
                ..SpawnSpec::default()
            }),
            trace: trace("u-create-0000001"),
            earlier: None,
        });
        assert!(matches!(
            &start.events[0].kind,
            SessionEventKind::Started { trace: Some(t), .. } if t.as_str() == "u-create-0000001"
        ));
        sim.feed(Kind::Out, json!(INIT_OK));
        sim.feed(Kind::Out, json!(RESULT_OK));
        for (text, t) in [("second", trace("u-prompt-0000002")), ("third", None)] {
            sim.note(RuntimeNote::Command {
                token: format!("t-{text}"),
                command: Command::prompt(text),
                trace: t,
            });
            sim.feed(Kind::Out, json!(RESULT_OK));
        }
        let traces: Vec<(String, Option<String>)> = sim
            .events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Input {
                    command: Command::Prompt { text, .. },
                    trace,
                    ..
                } => Some((text.clone(), trace.as_ref().map(|t| t.as_str().to_owned()))),
                _ => None,
            })
            .collect();
        assert_eq!(
            traces,
            [
                ("first".to_string(), Some("u-create-0000001".to_string())),
                ("second".to_string(), Some("u-prompt-0000002".to_string())),
                ("third".to_string(), None),
            ]
        );
        sim.assert_replay_exact();
    }

    /// A journal written by the build before input ids (the first prompt inside the journaled
    /// spec, no session id, the lines that build wrote) replays with the `input` event and
    /// without owing anything: nothing would be written twice after an upgrade.
    #[test]
    fn a_journal_of_the_previous_build_replays_with_the_first_prompt_input() {
        let recs = [
            (Kind::Spawned, json!({"pid": 7, "argv": ["claude"]})),
            (
                Kind::Note,
                json!({"wasit":"start","spec":{"cwd":"/w","session_dir":"/d/sessions/aaaaaaaaaaaa","model":"haiku","resume":"fresh","first_prompt":"hello"}}),
            ),
            (
                Kind::In,
                json!(
                    r#"{"request":{"hooks":null,"subtype":"initialize"},"request_id":"req_1","type":"control_request"}"#
                ),
            ),
            (Kind::Out, json!(INIT_OK)),
            (
                Kind::In,
                json!(
                    r#"{"message":{"content":[{"text":"hello","type":"text"}],"role":"user"},"type":"user"}"#
                ),
            ),
            (Kind::Out, json!(TURN_INIT)),
            (Kind::Out, json!(RESULT_OK)),
        ];
        let mut sim = Sim::new();
        for (kind, data) in recs {
            sim.rec(kind, data);
        }
        assert_eq!(sim.core.owed().count(), 0, "every line matched its echo");
        assert_eq!(
            sim.kinds(),
            [
                "spawned",
                "started",
                "input:2",
                "slash_commands_updated",
                "models_updated",
                "modes_updated",
                "input_sent:2",
                "phase",
                "written:2",
                // That build wrote no message ids: the turn is the CLI's `system/init`, and
                // which input it ran for is not reported.
                "session_init",
                "turn_started",
                "phase",
                "turn_completed",
                "phase"
            ]
            .map(|s| s.to_string())
        );
        let turn = sim
            .events
            .iter()
            .find_map(|e| match &e.kind {
                SessionEventKind::Agent {
                    event: AgentEvent::TurnStarted { input, trigger, .. },
                } => Some((*input, *trigger)),
                _ => None,
            })
            .unwrap();
        assert_eq!(turn, (None, TurnTrigger::Unreported));
        sim.assert_replay_exact();
    }

    /// The same journal where the CLI never printed `system/init` (a result with no start): the
    /// turn is still one turn, never an end without a start.
    #[test]
    fn a_result_without_a_reported_start_still_gets_its_start() {
        let mut sim = Sim::new();
        sim.start(Some("hello"));
        sim.feed(Kind::Out, json!(INIT_OK));
        sim.feed(Kind::Out, json!(RESULT_OK));
        let k = sim.kinds();
        let start = k.iter().position(|x| x == "turn_started").unwrap();
        let end = k.iter().position(|x| x == "turn_completed").unwrap();
        assert!(start < end, "{k:?}");
        assert_eq!(sim.core.snapshot().phase, Phase::Idle);
        assert_eq!(sim.core.snapshot().turns_completed, 1);
        sim.assert_replay_exact();
    }

    #[test]
    fn written_waits_for_every_line_of_the_input() {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        let (go, _) = sim.command(Command::prompt("go"));
        sim.feed(
            Kind::Out,
            json!(r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}}"#),
        );
        // The interrupt writes two lines (deny the gate, interrupt): echo one, a foreign line,
        // then the other.
        let out = sim.rec(
            Kind::Note,
            RuntimeNote::Command {
                token: "i".into(),
                command: Command::Interrupt,
                trace: None,
            }
            .to_value(),
        );
        let interrupt = InputId(sim.j.seq);
        assert_eq!(out.writes.len(), 2);
        let written = |sim: &Sim| {
            sim.events
                .iter()
                .filter(
                    |e| matches!(e.kind, SessionEventKind::Written { input } if input == interrupt),
                )
                .count()
        };
        sim.rec(Kind::In, json!(out.writes[0]));
        sim.rec(Kind::In, json!("a line someone else wrote"));
        assert_eq!(written(&sim), 0);
        sim.rec(Kind::In, json!(out.writes[1]));
        assert_eq!(written(&sim), 1);
        assert!(sim.kinds().contains(&format!("written:{go}")));
        sim.assert_replay_exact();
    }

    #[test]
    fn a_failed_stdin_write_is_reported_for_its_input() {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        let (go, _) = sim.command(Command::prompt("go"));
        let out = sim.rec(
            Kind::Note,
            json!({"stdin_error": "write to child stdin: Broken pipe (os error 32)"}),
        );
        assert!(matches!(
            &out.events[..],
            [SessionEvent { kind: SessionEventKind::WriteFailed { input: Some(i), error }, .. }]
                if *i == go && error.contains("Broken pipe")
        ));
        sim.assert_replay_exact();
    }

    /// Inputs still queued in the protocol, or whose lines never reached the shim, are dropped
    /// by the runtime when the generation ends — and only those.
    #[test]
    fn inputs_left_when_the_process_ends_are_dropped() {
        let mut sim = Sim::new();
        sim.start(Some("first"));
        // Never written: the handshake does not complete.
        let (second, _) = sim.command(Command::prompt("second"));
        let out = sim.feed(Kind::Exited, json!({"code": 1}));
        let drops: Vec<(InputId, InputDropReason)> = out
            .events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Agent {
                    event: AgentEvent::InputDropped { input, reason, .. },
                } => Some((*input, *reason)),
                _ => None,
            })
            .collect();
        assert_eq!(
            drops,
            [
                (InputId(2), InputDropReason::ProcessEnded),
                (second, InputDropReason::ProcessEnded)
            ]
        );
        sim.assert_replay_exact();

        // Written but never echoed (the shim died first), then lost.
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        let out = sim.rec(
            Kind::Note,
            RuntimeNote::Command {
                token: "p".into(),
                command: Command::prompt("go"),
                trace: None,
            }
            .to_value(),
        );
        assert_eq!(out.writes.len(), 1);
        let pending = InputId(sim.j.seq);
        // Sent and echoed inputs are not dropped.
        let (echoed, _) = sim.command(Command::SetModel {
            model: "haiku".into(),
        });
        let lost = sim.core.mark_lost("gone");
        let names: Vec<String> = lost.iter().map(|e| short(&e.kind)).collect();
        assert_eq!(
            names,
            [
                format!("input_dropped:{pending}"),
                "lost".into(),
                "phase".into()
            ]
        );
        assert!(lost.windows(2).all(|w| w[0].cursor < w[1].cursor));
        assert!(!names.contains(&format!("input_dropped:{echoed}")));
        assert!(matches!(
            &lost[2].kind,
            SessionEventKind::Phase {
                cause: PhaseCause::Lost,
                ..
            }
        ));
    }

    /// The running turn names the prompt that started it — not a prompt an interrupt dropped
    /// (the FIFO of prompt texts this replaces handed the next turn a dropped prompt's text).
    #[test]
    fn a_turn_after_an_interrupt_names_its_own_prompt() {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        let (a, _) = sim.command(Command::prompt("A"));
        sim.cli_starts_turn_for_the_last_prompt();
        let (b, out) = sim.command(Command::prompt("B"));
        assert!(out.writes.is_empty(), "B waits behind A's turn");
        let (c, _) = sim.command(Command::prompt("C"));
        sim.command(Command::Interrupt);
        sim.feed(
            Kind::Out,
            json!(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1}"#),
        );
        let (d, _) = sim.command(Command::prompt("D"));
        sim.cli_starts_turn_for_the_last_prompt();
        let k = sim.kinds();
        for dropped in [b, c] {
            assert!(k.contains(&format!("input_dropped:{dropped}")), "{k:?}");
            assert!(!k.contains(&format!("turn_started:{dropped}")), "{k:?}");
        }
        assert!(k.contains(&format!("turn_started:{a}")));
        assert!(k.contains(&format!("turn_started:{d}")));
        assert_eq!(
            sim.core.snapshot().turn.as_ref().unwrap().prompt.as_deref(),
            Some("D")
        );
        sim.assert_replay_exact();
    }

    #[test]
    fn every_phase_change_carries_its_cause() {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        let (go, _) = sim.command(Command::prompt("go"));
        sim.cli_starts_turn_for_the_last_prompt();
        sim.feed(
            Kind::Out,
            json!(r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"}}}"#),
        );
        sim.command(Command::AnswerGate {
            request_id: "g1".into(),
            decision: GateDecision::choose("allow"),
        });
        sim.feed(Kind::Out, json!(RESULT_OK));
        sim.note(RuntimeNote::Stop { token: "s".into() });
        sim.feed(Kind::Exited, json!({"code": 0}));
        assert_eq!(
            phases(&sim),
            [
                ("idle".to_string(), PhaseCause::HandshakeDone),
                (
                    "working".into(),
                    PhaseCause::TurnStarted {
                        input: Some(go),
                        trigger: TurnTrigger::Input,
                    }
                ),
                (
                    "needs_you".into(),
                    PhaseCause::GateRaised {
                        request_id: "g1".into(),
                        external: false
                    }
                ),
                (
                    "working".into(),
                    PhaseCause::GateCleared {
                        request_id: "g1".into(),
                        how: GateClearing::Answered
                    }
                ),
                (
                    "idle".into(),
                    PhaseCause::TurnCompleted {
                        end_reason: None,
                        interrupted: false
                    }
                ),
                (
                    "exited".into(),
                    PhaseCause::Exited {
                        code: Some(0),
                        signal: None,
                        requested: true,
                        handshake_timeout: false
                    }
                ),
            ]
        );
        assert!(sim.events.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Notice {
                code: NoticeCode::StopRequested,
                ..
            }
        )));
        sim.assert_replay_exact();
    }

    /// Output while the session reads as idle (a turn the CLI started by itself, background
    /// work) is reported once per idle period.
    #[test]
    fn activity_while_idle_is_an_anomaly_once_per_idle_period() {
        let mut sim = Sim::new();
        sim.start(None);
        sim.feed(Kind::Out, json!(INIT_OK));
        let anomalies = |sim: &Sim| {
            sim.events
                .iter()
                .filter_map(|e| match &e.kind {
                    SessionEventKind::Anomaly { anomaly, first } => Some((*anomaly, first.clone())),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let tool = r#"{"type":"assistant","message":{"id":"m9","content":[{"type":"tool_use","id":"tu1","name":"Bash","input":{"command":"ls"}}]}}"#;
        sim.feed(Kind::Out, json!(tool));
        sim.feed(Kind::Out, json!(tool));
        assert_eq!(
            anomalies(&sim),
            [(
                AnomalyKind::ActivityWhileIdle,
                "tool_call_started".to_string()
            )]
        );
        // A turn of ours, then idle again: a new period.
        sim.command(Command::prompt("go"));
        sim.cli_starts_turn_for_the_last_prompt();
        sim.feed(
            Kind::Out,
            json!(r#"{"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"hi"}]}}"#),
        );
        sim.feed(Kind::Out, json!(RESULT_OK));
        assert_eq!(anomalies(&sim).len(), 1);
        sim.feed(Kind::Out, json!(tool));
        assert_eq!(anomalies(&sim).len(), 2);
        sim.assert_replay_exact();
    }

    /// The prompt of a continued session's first generation is not applied again.
    #[test]
    fn a_new_generation_starts_without_the_first_prompt() {
        let mut sim = Sim::new();
        sim.start(Some("hello"));
        sim.feed(Kind::Out, json!(INIT_OK));
        sim.feed(Kind::Out, json!(RESULT_OK));
        sim.note(RuntimeNote::Stop { token: "s".into() });
        sim.feed(Kind::Exited, json!({"code": 0}));
        // What `continue` journals: the stored spec without the first prompt.
        sim.feed(
            Kind::Spawned,
            json!({"pid": 8, "argv": ["claude", "--resume", "x"]}),
        );
        let out = sim.note(RuntimeNote::Start {
            spec: Box::new(SpawnSpec {
                resume: ResumeMode::Resume("x".into()),
                session_id: Some("aaaaaaaaaaaa".into()),
                ..SpawnSpec::default()
            }),
            trace: None,
            earlier: None,
        });
        assert!(matches!(
            &out.events[0].kind,
            SessionEventKind::Started {
                generation: 2,
                resume: StartResume::Resume,
                resume_id: Some(id),
                first_input: None,
                ..
            } if id == "x"
        ));
        let prompts = sim
            .events
            .iter()
            .filter(|e| {
                matches!(
                    &e.kind,
                    SessionEventKind::Input {
                        command: Command::Prompt { .. },
                        ..
                    }
                )
            })
            .count();
        assert_eq!(prompts, 1);
        sim.assert_replay_exact();
    }

    // ── the agent's conversation: its id from creation, what can be continued ──

    /// What a driver says about how its CLI keeps conversations, over claude's wire.
    struct Variant {
        inner: Arc<dyn DriverDescriptor>,
        resume: bool,
        saves: SavesConversation,
    }

    impl wasit_core::ConfigRenderer for Variant {
        fn render_config(
            &self,
            c: &wasit_core::AgentConfig,
            s: &mut SpawnSpec,
        ) -> Vec<wasit_core::Unsupported> {
            self.inner.render_config(c, s)
        }
    }

    impl DriverDescriptor for Variant {
        fn id(&self) -> wasit_core::DriverId {
            self.inner.id()
        }
        fn display_name(&self) -> &str {
            self.inner.display_name()
        }
        fn default_binary(&self) -> &str {
            self.inner.default_binary()
        }
        fn argv(&self, spec: &SpawnSpec) -> Vec<String> {
            self.inner.argv(spec)
        }
        fn env(&self, spec: &SpawnSpec) -> Vec<(String, String)> {
            self.inner.env(spec)
        }
        fn capabilities(&self) -> DriverCapabilities {
            DriverCapabilities {
                resume: self.resume,
                ..self.inner.capabilities()
            }
        }
        fn saves_conversation(&self) -> SavesConversation {
            self.saves
        }
        fn new_protocol(&self) -> Box<dyn Protocol> {
            self.inner.new_protocol()
        }
        fn discovery(&self, ctx: &wasit_core::DiscoveryContext) -> Vec<wasit_core::DiscoveryProbe> {
            self.inner.discovery(ctx)
        }
    }

    const U: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

    fn init_line(id: &str) -> String {
        json!({"type":"system","subtype":"init","session_id":id,"model":"haiku","cwd":"/w",
               "tools":[],"mcp_servers":[],"slash_commands":[],"permissionMode":"default",
               "claude_code_version":"2.1.293"})
        .to_string()
    }

    fn meta_with(conversation: Conversation) -> SessionMeta {
        SessionMeta {
            conversation,
            cwd: Some("/w".into()),
            ..meta()
        }
    }

    impl Sim {
        /// A generation: `spawned`, a `start` note for `spec`, then (when given) the agent's
        /// init line; then a requested stop and the exit.
        fn generation(&mut self, spec: SpawnSpec, reports: Option<&str>) {
            self.feed(Kind::Spawned, json!({"pid": 7, "argv": ["claude"]}));
            self.note(RuntimeNote::Start {
                spec: Box::new(spec),
                trace: None,
                earlier: None,
            });
            self.feed(Kind::Out, json!(INIT_OK));
            if let Some(id) = reports {
                self.command(Command::prompt("hi"));
                self.feed(Kind::Out, json!(init_line(id)));
            }
            self.note(RuntimeNote::Stop { token: "s".into() });
            self.feed(Kind::Exited, json!({"code": 0}));
        }

        fn snap(&self) -> &SessionSnapshot {
            self.core.snapshot()
        }
    }

    fn chosen_spec() -> SpawnSpec {
        SpawnSpec {
            native_session_id: Some(U.into()),
            ..SpawnSpec::default()
        }
    }

    /// A session created under a chosen id knows its conversation from the start; ended before
    /// the agent stored anything it is continued by starting the conversation again under the
    /// same id, not by resuming it (which the agent would refuse).
    #[test]
    fn a_chosen_id_is_known_from_creation_and_a_continue_before_the_first_prompt_starts_fresh() {
        let mut sim = Sim::of(
            wasit_claude::descriptor(),
            meta_with(Conversation::Chosen(U.into())),
        );
        assert_eq!(sim.snap().native_session_id.as_deref(), Some(U));
        assert!(!sim.snap().continuable);
        assert_eq!(sim.snap().not_continuable, Some(NotContinuable::NotEnded));
        assert_eq!(
            sim.snap().resume_command,
            None,
            "nothing the user could resume yet"
        );
        sim.generation(chosen_spec(), None);
        assert_eq!(sim.snap().phase, Phase::Exited { code: Some(0) });
        assert_eq!(sim.snap().native_session_id.as_deref(), Some(U));
        assert!(sim.snap().continuable, "{:?}", sim.snap().not_continuable);
        assert_eq!(sim.snap().not_continuable, None);
        assert_eq!(sim.snap().resume_command, None);
        assert_eq!(
            sim.core.continue_plan(),
            Ok(ContinuePlan::StartFresh(U.into()))
        );
        sim.assert_replay_exact();
    }

    /// Once the agent reported the conversation, a continue resumes it; the user's own command
    /// for it appears with the session's directory.
    #[test]
    fn after_the_agent_reports_the_conversation_a_continue_resumes_it() {
        let mut sim = Sim::of(
            wasit_claude::descriptor(),
            meta_with(Conversation::Chosen(U.into())),
        );
        sim.generation(chosen_spec(), Some(U));
        assert!(sim.snap().continuable);
        assert_eq!(sim.core.continue_plan(), Ok(ContinuePlan::Resume(U.into())));
        let cmd = sim.snap().resume_command.clone().unwrap();
        assert_eq!(cmd.argv, ["claude", "--resume", U]);
        assert_eq!(cmd.cwd, "/w");
        // The next generation resumes (its start note says so) and ends: still resumable.
        sim.generation(
            SpawnSpec {
                resume: ResumeMode::Resume(U.into()),
                ..SpawnSpec::default()
            },
            None,
        );
        assert_eq!(sim.core.continue_plan(), Ok(ContinuePlan::Resume(U.into())));
        assert_eq!(sim.snap().generation, 2);
        sim.assert_replay_exact();
    }

    /// A session created by resuming X records X at create and can be continued (by resuming X)
    /// even if its first process ended before the agent said anything.
    #[test]
    fn a_session_created_by_resuming_records_the_resumed_id() {
        let mut sim = Sim::of(
            wasit_claude::descriptor(),
            meta_with(Conversation::Saved("X".into())),
        );
        assert_eq!(sim.snap().native_session_id.as_deref(), Some("X"));
        sim.feed(Kind::Spawned, json!({"pid": 7}));
        sim.feed(Kind::Exited, json!({"code": 1}));
        assert!(matches!(sim.snap().phase, Phase::Failed { .. }));
        assert_eq!(
            sim.core.continue_plan(),
            Ok(ContinuePlan::Resume("X".into()))
        );
        sim.assert_replay_exact();
    }

    /// A session of an older build has no chosen id: until the agent reported one there is
    /// nothing to continue; it says so, and after the report it can be.
    #[test]
    fn a_session_without_an_id_cannot_be_continued_until_the_agent_reported_one() {
        let mut sim = Sim::of(wasit_claude::descriptor(), meta_with(Conversation::Unknown));
        assert_eq!(sim.snap().native_session_id, None);
        sim.generation(SpawnSpec::default(), None);
        assert!(!sim.snap().continuable);
        assert_eq!(
            sim.snap().not_continuable,
            Some(NotContinuable::NoConversationId)
        );
        assert_eq!(
            sim.core.continue_plan(),
            Err(NotContinuable::NoConversationId)
        );
        sim.generation(SpawnSpec::default(), Some(U));
        assert!(sim.snap().continuable);
        assert_eq!(sim.snap().native_session_id.as_deref(), Some(U));
        assert_eq!(sim.core.continue_plan(), Ok(ContinuePlan::Resume(U.into())));
        sim.assert_replay_exact();
    }

    /// An agent that opens a thread (and reports its id) but writes nothing until the first turn
    /// cannot resume it before then, and cannot start it again under its id either. (codex: it
    /// reports the thread at `thread/start`, and its turns with `turn/started`.)
    #[test]
    fn a_conversation_the_agent_has_not_saved_cannot_be_continued_until_a_turn_ran() {
        let desc = wasit_codex::descriptor();
        assert_eq!(desc.saves_conversation(), SavesConversation::OnFirstTurn);
        let mut sim = Sim::of(desc, meta_with(Conversation::Unknown));
        let handshake = |sim: &mut Sim| {
            sim.feed(Kind::Out, json!(r#"{"id":1,"result":{}}"#));
            sim.feed(Kind::Out, json!(r#"{"id":2,"result":{"data":[]}}"#));
            sim.feed(
                Kind::Out,
                json!(format!(
                    r#"{{"id":3,"result":{{"thread":{{"id":"{U}"}},"model":"m","cwd":"/w"}}}}"#
                )),
            );
        };
        sim.feed(Kind::Spawned, json!({"pid": 7}));
        sim.note(RuntimeNote::Start {
            spec: Box::new(SpawnSpec::default()),
            trace: None,
            earlier: None,
        });
        handshake(&mut sim);
        // The agent reports the thread's id; nothing is stored yet.
        assert_eq!(sim.snap().native_session_id.as_deref(), Some(U));
        sim.note(RuntimeNote::Stop { token: "s".into() });
        sim.feed(Kind::Exited, json!({"code": 0}));
        assert_eq!(sim.snap().native_session_id.as_deref(), Some(U));
        assert!(!sim.snap().continuable);
        assert_eq!(
            sim.snap().not_continuable,
            Some(NotContinuable::NothingSaved)
        );
        assert_eq!(sim.snap().resume_command, None);
        // A turn starts in the next generation: the conversation is stored from then on.
        sim.feed(Kind::Spawned, json!({"pid": 8}));
        sim.note(RuntimeNote::Start {
            spec: Box::new(SpawnSpec::default()),
            trace: None,
            earlier: None,
        });
        handshake(&mut sim);
        sim.command(Command::prompt("go"));
        sim.feed(
            Kind::Out,
            json!(format!(
                r#"{{"method":"turn/started","params":{{"threadId":"{U}","turn":{{"id":"u1"}}}}}}"#
            )),
        );
        sim.note(RuntimeNote::Stop { token: "s2".into() });
        sim.feed(Kind::Exited, json!({"code": 0}));
        assert!(sim.snap().continuable, "{:?}", sim.snap().not_continuable);
        assert_eq!(sim.core.continue_plan(), Ok(ContinuePlan::Resume(U.into())));
        sim.assert_replay_exact();
    }

    #[test]
    fn an_agent_that_cannot_resume_says_so() {
        let desc: Arc<dyn DriverDescriptor> = Arc::new(Variant {
            inner: wasit_claude::descriptor(),
            resume: false,
            saves: SavesConversation::WhenReported,
        });
        let mut sim = Sim::of(desc, meta_with(Conversation::Chosen(U.into())));
        sim.generation(chosen_spec(), Some(U));
        assert!(!sim.snap().continuable);
        assert_eq!(
            sim.snap().not_continuable,
            Some(NotContinuable::AgentCannotResume)
        );
        assert_eq!(
            sim.core.continue_plan(),
            Err(NotContinuable::AgentCannotResume)
        );
    }

    /// While a finished session's snapshot is rebuilt from its journal nothing is claimed.
    #[test]
    fn a_snapshot_still_loading_is_not_continuable_for_that_reason() {
        let m = meta_with(Conversation::Saved(U.into()));
        let snap = blank_snapshot(
            &m,
            wasit_claude::descriptor().as_ref(),
            Phase::Exited { code: Some(0) },
            false,
        );
        assert_eq!(snap.native_session_id.as_deref(), Some(U));
        assert!(!snap.continuable);
        assert_eq!(snap.not_continuable, Some(NotContinuable::Loading));
        let snap = blank_snapshot(
            &m,
            wasit_claude::descriptor().as_ref(),
            Phase::Exited { code: Some(0) },
            true,
        );
        assert!(snap.continuable);
    }

    /// A lost session (its shim vanished) can be continued like an exited one, and the answer
    /// follows the phase at once.
    #[test]
    fn a_lost_session_is_continuable_and_continuable_follows_the_phase() {
        let mut sim = Sim::of(
            wasit_claude::descriptor(),
            meta_with(Conversation::Chosen(U.into())),
        );
        sim.feed(Kind::Spawned, json!({"pid": 7}));
        sim.note(RuntimeNote::Start {
            spec: Box::new(chosen_spec()),
            trace: None,
            earlier: None,
        });
        assert!(!sim.snap().continuable);
        let lost = sim.core.mark_lost("the shim is gone");
        assert!(!lost.is_empty());
        assert!(matches!(sim.snap().phase, Phase::Lost { .. }));
        assert!(sim.snap().continuable);
        assert_eq!(
            sim.core.continue_plan(),
            Ok(ContinuePlan::StartFresh(U.into()))
        );
    }

    /// What a continue with prompts waiting journals: the new generation's `spawned`, the
    /// prompts as `command` notes (the actor writes them right after attaching), then the `start`
    /// note. They are applied right after the start, in the order journaled, as ordinary inputs
    /// of that generation (queued until the agent is ready), living and replayed alike.
    #[test]
    fn prompts_journaled_before_a_continued_generations_start_are_its_first_inputs_in_order() {
        let mut sim = Sim::of(
            wasit_claude::descriptor(),
            meta_with(Conversation::Chosen(U.into())),
        );
        sim.generation(chosen_spec(), Some(U));
        let before = sim.events.len();
        sim.feed(
            Kind::Spawned,
            json!({"pid": 8, "argv": ["claude", "--resume", U]}),
        );
        let (first, out1) = sim.command(Command::prompt("one"));
        assert!(
            out1.events.is_empty(),
            "held until the start: {:?}",
            out1.events
        );
        let (second, _) = sim.command(Command::prompt("two"));
        assert!(first < second);
        let out = sim.note(RuntimeNote::Start {
            spec: Box::new(SpawnSpec {
                resume: ResumeMode::Resume(U.into()),
                ..SpawnSpec::default()
            }),
            trace: None,
            earlier: None,
        });
        assert!(out.started);
        let inputs: Vec<(InputId, CommandStatus)> = sim.events[before..]
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Input {
                    input,
                    command: Command::Prompt { .. },
                    status,
                    ..
                } => Some((*input, status.clone())),
                _ => None,
            })
            .collect();
        // Both are queued by the driver until its handshake is done, in order, once each.
        assert_eq!(
            inputs,
            [
                (first, CommandStatus::Queued),
                (second, CommandStatus::Queued)
            ]
        );
        // The agent's handshake completes: the prompts go out one after the other.
        sim.feed(Kind::Out, json!(INIT_OK));
        assert_eq!(sim.snap().generation, 2);
        assert!(sim.snap().phase.is_live());
        let kinds = sim.kinds();
        assert!(
            kinds.iter().any(|k| *k == format!("input_sent:{first}")),
            "{kinds:?}"
        );
        sim.assert_replay_exact();
    }

    // ── background work, turns the CLI starts itself, subagents (real recordings) ──────────

    /// Replay a recording of the claude driver (`wasit-claude/tests/fixtures`) through the core
    /// the way the live actor runs it: the shim echoes every line the protocol writes, the
    /// recording's commands are journaled as notes.
    fn sim_of_fixture(name: &str) -> Sim {
        let path = format!(
            "{}/../wasit-claude/tests/fixtures/{name}.ndjson",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        let records = wasit_core::transcript::parse(&text).unwrap();
        let mut sim = Sim::new();
        for r in records {
            match r {
                wasit_core::transcript::Record::Start(spec) => {
                    sim.feed(Kind::Spawned, json!({"pid": 7, "argv": ["claude"]}));
                    sim.note(RuntimeNote::Start {
                        spec,
                        trace: None,
                        earlier: None,
                    });
                }
                wasit_core::transcript::Record::Out(line) => {
                    sim.feed(Kind::Out, json!(line));
                }
                wasit_core::transcript::Record::Cmd(cmd) => {
                    sim.command(cmd);
                }
                // The shim echoes what the protocol writes; the recording's copies are not
                // journaled twice.
                wasit_core::transcript::Record::In(_)
                | wasit_core::transcript::Record::CloseStdin => {}
            }
        }
        sim
    }

    fn anomalies(sim: &Sim) -> Vec<(AnomalyKind, String)> {
        sim.events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Anomaly { anomaly, first } => Some((*anomaly, first.clone())),
                _ => None,
            })
            .collect()
    }

    /// The snapshot after every event of a journal: what the session said it was doing, between
    /// any two lines the CLI printed.
    fn phase_after_each_record(name: &str) -> Vec<(String, Phase, u32, bool)> {
        let path = format!(
            "{}/../wasit-claude/tests/fixtures/{name}.ndjson",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).unwrap();
        let records = wasit_core::transcript::parse(&text).unwrap();
        let mut sim = Sim::new();
        let mut out = Vec::new();
        for r in records {
            let line = match r {
                wasit_core::transcript::Record::Start(spec) => {
                    sim.feed(Kind::Spawned, json!({"pid": 7, "argv": ["claude"]}));
                    sim.note(RuntimeNote::Start {
                        spec,
                        trace: None,
                        earlier: None,
                    });
                    continue;
                }
                wasit_core::transcript::Record::Out(line) => line,
                wasit_core::transcript::Record::Cmd(cmd) => {
                    sim.command(cmd);
                    continue;
                }
                _ => continue,
            };
            sim.feed(Kind::Out, json!(line.clone()));
            let ty = serde_json::from_str::<serde_json::Value>(&line)
                .map(|v| {
                    format!(
                        "{}/{}",
                        v["type"].as_str().unwrap_or(""),
                        v["subtype"].as_str().unwrap_or("")
                    )
                })
                .unwrap_or_default();
            let snap = sim.core.snapshot();
            out.push((
                ty,
                snap.phase.clone(),
                snap.background_running,
                snap.turn.is_some(),
            ));
        }
        out
    }

    /// The owner's report: the session read `idle` while the agent was still working. With
    /// background work in the model the phase is `working` from the first `task_started` to the
    /// end of the task, with or without a turn; between the turn that started it and the turn
    /// the CLI starts when it ends, the session is working with no turn.
    #[test]
    fn a_session_is_never_idle_while_a_background_task_runs() {
        for name in ["background-bash", "background-agent", "stop-task"] {
            let states = phase_after_each_record(name);
            let mut saw_background_without_turn = false;
            for (ty, phase, running, turn) in &states {
                if *running > 0 {
                    assert_eq!(*phase, Phase::Working, "{name}: after {ty}");
                    saw_background_without_turn |= !turn;
                }
            }
            assert!(
                saw_background_without_turn,
                "{name}: the recording has a stretch of background work with no turn"
            );
        }
    }

    /// Activity that a running background task explains is no anomaly; the same activity with
    /// nothing running (a stale subagent line, a top-level call out of nowhere) still is.
    #[test]
    fn no_anomaly_for_attributed_activity_and_one_for_the_rest() {
        for name in [
            "background-bash",
            "background-agent",
            "stop-task",
            "agent-turn-interrupt",
            "agent-turn-prompt",
            "basic",
            "gates",
            "interrupt",
        ] {
            let sim = sim_of_fixture(name);
            assert_eq!(anomalies(&sim), [], "{name}");
        }
        // A line of a subagent the session knows (its task just ended: the CLI flushes its last
        // lines after the end frames) is accounted for; one of a subagent nobody told us about,
        // and a top-level line with nothing running, are not.
        let mut sim = sim_of_fixture("background-agent");
        assert_eq!(sim.core.snapshot().phase, Phase::Idle);
        let scoped_line = |call: &str| {
            json!(format!(
                r#"{{"type":"assistant","parent_tool_use_id":"{call}","agent_id":"x","message":{{"id":"late","content":[{{"type":"text","text":"late"}}]}}}}"#
            ))
        };
        let call = sim.core.snapshot().subagents[0].call_id.clone();
        sim.feed(Kind::Out, scoped_line(&call));
        assert_eq!(anomalies(&sim), []);
        sim.feed(Kind::Out, scoped_line("toolu_unknown"));
        assert_eq!(
            anomalies(&sim),
            [(
                AnomalyKind::ActivityWhileIdle,
                "assistant_delta".to_string()
            )]
        );
    }

    #[test]
    fn the_snapshot_keeps_tasks_and_subagents_until_their_tasks_end() {
        let name = "background-agent";
        let states = phase_after_each_record(name);
        // After the parent's first `result` the subagent still runs.
        let first_result = states
            .iter()
            .position(|(ty, ..)| ty == "result/success")
            .unwrap();
        assert_eq!(states[first_result].1, Phase::Working);
        assert_eq!(states[first_result].2, 1);
        assert!(
            !states[first_result].3,
            "no turn is open: the work is the subagent's"
        );
        let sim = sim_of_fixture(name);
        let snap = sim.core.snapshot();
        assert_eq!(snap.phase, Phase::Idle);
        assert_eq!(snap.background_running, 0);
        assert_eq!(snap.subagents.len(), 1);
        let sub = &snap.subagents[0];
        assert!(!sub.running);
        assert_eq!(sub.ok, Some(true));
        assert_eq!(sub.status.as_deref(), Some("completed"));
        assert_eq!(sub.subagent_type.as_deref(), Some("general-purpose"));
        assert!(sub.task_id.is_some() && sub.agent_id.is_some() && sub.ended_at_ms.is_some());
        assert!(sub.tool_calls >= 1, "its own tool calls count for it");
        assert!(sub.usage.is_some_and(|u| u.tool_uses >= 1));
        assert_eq!(snap.tasks.len(), 1);
        assert_eq!(snap.tasks[0].task_id, sub.task_id.clone().unwrap());
        // The session's own turn counters never took the subagent's tool calls: one `Agent` call.
        assert_eq!(
            snap.turns_completed as usize,
            sim.kinds()
                .iter()
                .filter(|k| *k == "turn_completed")
                .count()
        );
        // The agent's own turns after the task are labelled so.
        let last = snap.last_turn.as_ref().unwrap();
        assert_eq!(
            last.origin.as_ref().map(|o| o.kind.as_str()),
            Some("task-notification")
        );
        sim.assert_replay_exact();
    }

    #[test]
    fn a_turn_the_agent_started_says_so_in_the_snapshot_and_the_phase_cause() {
        let sim = sim_of_fixture("background-bash");
        // (Message ids of a recording are derived from its own input ids, not this journal's:
        // which turns are ours is the driver's replay test; here, that a turn nobody asked for
        // is `working` with no input.)
        let agent_turn = phases(&sim)
            .into_iter()
            .rev()
            .find(|(_, c)| matches!(c, PhaseCause::TurnStarted { input: None, .. }));
        assert_eq!(agent_turn.map(|(p, _)| p).as_deref(), Some("working"));
        // The background task started, then ended: both are phase causes.
        let causes: Vec<PhaseCause> = phases(&sim).into_iter().map(|(_, c)| c).collect();
        assert!(
            causes
                .iter()
                .any(|c| matches!(c, PhaseCause::TaskEnded { .. })),
            "{causes:?}"
        );
        sim.assert_replay_exact();
    }

    /// Stopping a task and interrupting the agent's own turn, end to end through the core.
    #[test]
    fn stopping_tasks_and_interrupting_a_turn_the_agent_started_replay_exactly() {
        for name in [
            "stop-task",
            "agent-turn-interrupt",
            "agent-turn-prompt",
            "background-bash",
        ] {
            let sim = sim_of_fixture(name);
            sim.assert_replay_exact();
        }
        let sim = sim_of_fixture("agent-turn-interrupt");
        let snap = sim.core.snapshot();
        assert_eq!(snap.phase, Phase::Idle);
        assert_eq!(snap.turns_completed, 3);
        // The agent's cancelled turn is in the record of what ended; the last turn was ours.
        let ends: Vec<_> = sim
            .events
            .iter()
            .filter_map(|e| match &e.kind {
                SessionEventKind::Agent {
                    event:
                        AgentEvent::TurnCompleted {
                            end_reason,
                            was_interrupted,
                            origin,
                            ..
                        },
                } => Some((*end_reason, *was_interrupted, origin.is_some())),
                _ => None,
            })
            .collect();
        assert_eq!(
            ends,
            [
                (None, false, false),
                (Some(wasit_core::EndReason::Cancelled), true, true),
                (None, false, false)
            ]
        );
    }

    /// A subagent's transcript and the session's own are filters of one stream; their union is the
    /// stream, and a subagent's lines are not in the session's.
    #[test]
    fn history_scopes_split_one_stream() {
        use crate::wire::HistoryScope;
        let sim = sim_of_fixture("background-agent");
        let call = sim.core.snapshot().subagents[0].call_id.clone();
        let sub = HistoryScope::Subagent {
            call_id: call.clone(),
        };
        let in_main: Vec<_> = sim
            .events
            .iter()
            .filter(|e| HistoryScope::Main.admits(e))
            .collect();
        let in_sub: Vec<_> = sim.events.iter().filter(|e| sub.admits(e)).collect();
        assert!(!in_sub.is_empty());
        for e in &in_sub {
            assert!(
                !HistoryScope::Main.admits(e)
                    || matches!(
                        &e.kind,
                        SessionEventKind::Agent {
                            event: AgentEvent::TaskStarted { .. } | AgentEvent::TaskEnded { .. }
                        }
                    )
            );
        }
        // Scoped lines are in the subagent's transcript and never in the session's.
        for e in &sim.events {
            if let SessionEventKind::Agent {
                event: AgentEvent::Scoped { scope, .. },
            } = &e.kind
            {
                assert!(!HistoryScope::Main.admits(e));
                assert_eq!(sub.admits(e), scope.call_id == call);
                assert!(HistoryScope::All.admits(e));
            }
        }
        // The session's own conversation is intact: its tool call, its text, its turns.
        let main_kinds: Vec<String> = in_main.iter().map(|e| short(&e.kind)).collect();
        assert!(main_kinds.iter().any(|k| k == "tool_call_started"));
        assert!(
            main_kinds
                .iter()
                .filter(|k| *k == "turn_started" || k.starts_with("turn_started"))
                .count()
                >= 2
        );
        // Another call's transcript is empty.
        let other = HistoryScope::Subagent {
            call_id: "toolu_nope".into(),
        };
        assert_eq!(sim.events.iter().filter(|e| other.admits(e)).count(), 0);
        // The wire shape of the filter.
        assert_eq!(
            serde_json::to_value(&sub).unwrap(),
            json!({"kind":"subagent","call_id": call})
        );
        assert_eq!(
            serde_json::from_value::<HistoryScope>(json!({"kind":"main"})).unwrap(),
            HistoryScope::Main
        );
    }

    /// A process that ends takes its running tasks with it: nothing is left `running`.
    #[test]
    fn the_end_of_the_process_ends_every_task() {
        let mut sim = sim_of_fixture("background-agent");
        // A new task that never reports its end, then the process exits.
        sim.feed(Kind::Out, json!(r#"{"type":"system","subtype":"task_started","task_id":"zz","tool_use_id":"toolu_z","description":"d","is_backgrounded":true,"task_type":"local_bash"}"#));
        assert_eq!(sim.core.snapshot().phase, Phase::Working);
        assert_eq!(sim.core.snapshot().background_running, 1);
        sim.feed(Kind::Exited, json!({"code": 0}));
        let snap = sim.core.snapshot();
        assert!(matches!(
            snap.phase,
            Phase::Exited { .. } | Phase::Failed { .. }
        ));
        assert_eq!(snap.background_running, 0);
        assert!(snap.tasks.iter().all(|t| !t.running));
        assert!(snap.subagents.iter().all(|s| !s.running));
        sim.assert_replay_exact();
    }

    /// Journals the previous build wrote (the recordings are such streams: the same lines, the
    /// same writes) replay through the new core without trouble: exactly, with every turn started
    /// before it ends, nothing running at the end and no anomaly. What they cannot say (which
    /// input a turn was for, when the CLI printed no message lifecycle) stays unsaid.
    #[test]
    fn journals_of_the_previous_build_replay_to_a_truthful_state() {
        for name in [
            "basic",
            "gates",
            "interrupt",
            "mode",
            "resume",
            "continue",
            "claude-image-prompt",
            "claude-read-image",
            "lifecycle",
        ] {
            let sim = sim_of_fixture(name);
            sim.assert_replay_exact();
            let mut open = false;
            for e in &sim.events {
                match &e.kind {
                    SessionEventKind::Agent {
                        event: AgentEvent::TurnStarted { .. },
                    } => {
                        assert!(!open, "{name}: a turn started inside a turn");
                        open = true;
                    }
                    SessionEventKind::Agent {
                        event: AgentEvent::TurnCompleted { .. },
                    } => {
                        assert!(open, "{name}: a turn ended without a start");
                        open = false;
                    }
                    _ => {}
                }
            }
            let snap = sim.core.snapshot();
            assert!(!open && snap.turn.is_none(), "{name}");
            assert_eq!(snap.background_running, 0, "{name}");
            assert!(snap.tasks.is_empty(), "{name}");
            assert_eq!(anomalies(&sim), [], "{name}");
            assert!(snap.turns_completed >= 1, "{name}");
        }
    }
}
