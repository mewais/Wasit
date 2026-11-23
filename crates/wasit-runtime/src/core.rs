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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use wasit_control::TraceId;
use wasit_core::{
    AgentEvent, Command, DriverDescriptor, DriverError, Encoded, InputDropReason, InputId,
    Protocol, ResumeMode, SpawnSpec, Step,
};
use wasit_shim::{Kind, Record};

use crate::note::{classify, NoteKind, RuntimeNote};
use crate::wire::{
    event_cursor, synthetic_cursor, AnomalyKind, CommandStatus, ExitSummary, GateClearing,
    NoticeCode, PendingGate, Phase, PhaseCause, SessionEvent, SessionEventKind, SessionSnapshot,
    StartResume, SubagentInfo, TurnInfo, TurnOutcome,
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
}

/// An empty snapshot for `meta` in `phase`.
pub(crate) fn blank_snapshot(
    meta: &SessionMeta,
    desc: &dyn DriverDescriptor,
    phase: Phase,
    loaded: bool,
) -> SessionSnapshot {
    let caps = desc.capabilities();
    SessionSnapshot {
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
        title: None,
        display_name: meta.display_name.clone(),
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
        plan: Vec::new(),
        last_activity_ms: meta.created_at_ms,
        exit: None,
        stderr_tail: Vec::new(),
        cursor: 0,
        loaded,
    }
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
}

struct Emitter {
    seq: u64,
    t: u64,
    events: Vec<SessionEvent>,
}

impl Emitter {
    fn next_cursor(&self) -> u64 {
        event_cursor(self.seq, self.events.len())
    }

    fn push(&mut self, kind: SessionEventKind) {
        let cursor = self.next_cursor();
        self.events.push(SessionEvent {
            cursor,
            t: self.t,
            kind,
        });
    }
}

/// See the module docs.
pub(crate) struct Core {
    desc: Arc<dyn DriverDescriptor>,
    snap: SessionSnapshot,
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
    /// Request ids of pending gates raised from outside the CLI protocol (see
    /// [`RuntimeNote::RaiseGate`]). They survive turn ends, are answered without the protocol,
    /// and end with the process generation.
    external: BTreeSet<String>,
    /// Request ids of external gates that were withdrawn or ended with their process (newest
    /// last, bounded): a late answer to one is refused as [`CommandStatus::Withdrawn`].
    withdrawn: VecDeque<String>,
}

/// How many withdrawn external gate ids are remembered for refusing late answers.
const WITHDRAWN_MEMORY: usize = 256;

/// `GateResolved::reason` of an external gate whose request is gone.
pub(crate) const WITHDRAWN: &str = "withdrawn";

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
    Some(match e {
        AgentEvent::AssistantDelta { .. } => "assistant_delta",
        AgentEvent::AssistantContent { .. } => "assistant_content",
        AgentEvent::ThinkingDelta { .. } => "thinking_delta",
        AgentEvent::ToolCallStarted { .. } => "tool_call_started",
        AgentEvent::ToolCallUpdated { .. } => "tool_call_updated",
        AgentEvent::ToolCallFinished { .. } => "tool_call_finished",
        _ => return None,
    })
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
            external: BTreeSet::new(),
            withdrawn: VecDeque::new(),
        }
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
        let mut em = Emitter {
            seq: rec.seq,
            t: rec.t,
            events: Vec::new(),
        };
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
        self.clear_live_state();
        let slot = synthetic_cursor(self.last_seq);
        let t = self.snap.last_activity_ms;
        let phase = self.compute_phase();
        self.snap.phase = phase.clone();
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
        self.clear_live_state();
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
            for b in buf {
                if let Buffered::Command {
                    token,
                    input,
                    command,
                    trace,
                } = b
                {
                    let status = CommandStatus::NotRunning {
                        reason: "the process ended before it started".into(),
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
        self.owed.clear();
        self.close_owed = false;
        // External gates end with the process: their requests are gone.
        for request_id in std::mem::take(&mut self.external) {
            self.remember_withdrawn(request_id.clone());
            let event = AgentEvent::GateResolved {
                request_id,
                reason: Some(WITHDRAWN.into()),
            };
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, rec.t);
            em.push(SessionEventKind::Agent { event });
        }
        self.clear_live_state();
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
            NoteKind::Runtime(RuntimeNote::Start { spec, trace }) => {
                self.on_start(&spec, trace, rec, em, out)
            }
            NoteKind::Runtime(RuntimeNote::Command {
                token,
                command,
                trace,
            }) => {
                self.snap.last_activity_ms = rec.t;
                let input = InputId(rec.seq);
                if let Some(request_id) = self.answers_withdrawn(&command) {
                    let status = CommandStatus::Withdrawn { request_id };
                    em.push(SessionEventKind::Input {
                        input,
                        command,
                        status: status.clone(),
                        trace,
                    });
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
            NoteKind::Runtime(RuntimeNote::RaiseGate { token, gate }) => {
                self.raise_external(token, *gate, rec.t, em, out)
            }
            NoteKind::Runtime(RuntimeNote::ResolveGate { token, request_id }) => {
                self.resolve_external(token, request_id, em, out)
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
        em.push(SessionEventKind::Input {
            input,
            command: crate::media::without_bytes(command),
            status: status.clone(),
            trace,
        });
        if let Some(token) = token {
            out.resolved.push((token, status));
        }
        if let Some(step) = step {
            self.step(step, Some(input), em, out);
        }
    }

    // ── external gates ──────────────────────────────────────────────────────

    /// `command` answers an external gate that was withdrawn (and is not pending again).
    pub(crate) fn answers_withdrawn(&self, command: &Command) -> Option<String> {
        match command {
            Command::AnswerGate { request_id, .. } | Command::AnswerQuestion { request_id, .. }
                if !self.external.contains(request_id)
                    && self.withdrawn.iter().any(|w| w == request_id) =>
            {
                Some(request_id.clone())
            }
            _ => None,
        }
    }

    fn remember_withdrawn(&mut self, request_id: String) {
        self.withdrawn.retain(|w| *w != request_id);
        self.withdrawn.push_back(request_id);
        while self.withdrawn.len() > WITHDRAWN_MEMORY {
            self.withdrawn.pop_front();
        }
    }

    /// `command` answers a pending external gate.
    fn answers_external(&self, command: &Command) -> bool {
        match command {
            Command::AnswerGate { request_id, .. } | Command::AnswerQuestion { request_id, .. } => {
                self.external.contains(request_id)
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
        t: u64,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let rid = gate.request_id().to_string();
        let status = if !self.process_running() {
            CommandStatus::NotRunning {
                reason: "the process is not running".into(),
            }
        } else if self.withdrawn.iter().any(|w| *w == rid) {
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
            self.external.insert(rid);
            let event = AgentEvent::GateRequested { gate };
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, t);
            em.push(SessionEventKind::Agent { event });
            CommandStatus::Sent
        };
        out.resolved.push((token, status));
    }

    fn resolve_external(
        &mut self,
        token: String,
        request_id: String,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let status = if self.external.remove(&request_id) {
            self.remember_withdrawn(request_id.clone());
            let event = AgentEvent::GateResolved {
                request_id,
                reason: Some(WITHDRAWN.into()),
            };
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, em.t);
            em.push(SessionEventKind::Agent { event });
            CommandStatus::Sent
        } else {
            CommandStatus::Rejected {
                reason: format!("no external gate {request_id:?} is pending"),
            }
        };
        out.resolved.push((token, status));
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
            crate::media::externalize(&mut event, &mut out.blobs);
            match &event {
                AgentEvent::InputSent { input } => {
                    self.queued.retain(|q| q != input);
                    inputs.push(*input);
                }
                AgentEvent::InputDropped { input, .. } => self.queued.retain(|q| q != input),
                _ => {}
            }
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, em.t);
            em.push(SessionEventKind::Agent { event });
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

    fn reduce(&mut self, event: &AgentEvent, cursor: u64, t: u64) {
        let s = &mut self.snap;
        let mut cause = None;
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
                if native_session_id.is_some() {
                    s.native_session_id = native_session_id.clone();
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
            } => {
                let prompt = input.and_then(|i| take_text(&mut self.prompt_texts, i));
                s.turn = Some(TurnInfo {
                    started_at_ms: t,
                    native_turn_id: native_turn_id.clone(),
                    prompt,
                    tool_calls: 0,
                    last_tool: None,
                    current_tool: None,
                    current_call: None,
                });
                cause = Some(PhaseCause::TurnStarted { input: *input });
            }
            AgentEvent::TurnCompleted {
                end_reason,
                was_interrupted,
                num_turns,
                usage,
                per_model,
                reply,
            } => {
                s.turn = None;
                // CLI gates end with the turn; external gates are owned by someone else.
                let external = &self.external;
                s.pending_gates
                    .retain(|g| external.contains(g.gate.request_id()));
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
                for a in s.subagents.iter_mut().filter(|a| a.running) {
                    a.running = false;
                }
                s.last_turn = Some(TurnOutcome {
                    ended_at_ms: t,
                    end_reason: *end_reason,
                    was_interrupted: *was_interrupted,
                    num_turns: *num_turns,
                    reply: reply.as_deref().map(|r| truncate(r, REPLY_CHARS)),
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
                    s.subagents.push(SubagentInfo {
                        call_id: call_id.clone(),
                        name: name.clone(),
                        model: subagent_model.clone(),
                        description: input
                            .get("description")
                            .and_then(|d| d.as_str())
                            .map(|d| truncate(d, TITLE_CHARS))
                            .unwrap_or_default(),
                        started_at_ms: t,
                        running: true,
                        ok: None,
                    });
                    if s.subagents.len() > MAX_SUBAGENTS {
                        if let Some(i) = s.subagents.iter().position(|a| !a.running) {
                            s.subagents.remove(i);
                        } else {
                            s.subagents.remove(0);
                        }
                    }
                }
            }
            AgentEvent::ToolCallFinished { call_id, ok, .. } => {
                if let Some(turn) = s.turn.as_mut() {
                    if turn.current_call.as_deref() == Some(call_id.as_str()) {
                        turn.current_tool = None;
                        turn.current_call = None;
                    }
                }
                if let Some(a) = s.subagents.iter_mut().find(|a| &a.call_id == call_id) {
                    a.running = false;
                    a.ok = Some(*ok);
                }
            }
            AgentEvent::GateRequested { gate } => {
                s.pending_gates
                    .retain(|g| g.gate.request_id() != gate.request_id());
                s.pending_gates.push(PendingGate {
                    gate: gate.clone(),
                    raised_at_ms: t,
                    cursor,
                });
                cause = Some(PhaseCause::GateRaised {
                    request_id: gate.request_id().to_string(),
                    external: self.external.contains(gate.request_id()),
                });
            }
            AgentEvent::GateResolved { request_id, reason } => {
                s.pending_gates
                    .retain(|g| g.gate.request_id() != request_id);
                cause = Some(PhaseCause::GateCleared {
                    request_id: request_id.clone(),
                    how: if reason.as_deref() == Some(WITHDRAWN) {
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
            | AgentEvent::ProcessExited { .. }
            | AgentEvent::InputSent { .. }
            | AgentEvent::InputAcknowledged { .. }
            | AgentEvent::Unknown { .. } => {}
        }
        self.causes.extend(cause);
    }

    fn clear_live_state(&mut self) {
        self.snap.turn = None;
        self.snap.pending_gates.clear();
        for request_id in std::mem::take(&mut self.external) {
            self.remember_withdrawn(request_id);
        }
        for a in self.snap.subagents.iter_mut().filter(|a| a.running) {
            a.running = false;
        }
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
                } else if self.snap.turn.is_some() {
                    Phase::Working
                } else {
                    Phase::Idle
                }
            }
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
                    | PhaseCause::GateCleared { .. }
                    | PhaseCause::HandshakeDone
            ),
            Phase::Working => matches!(
                c,
                PhaseCause::TurnStarted { .. }
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
                SessionEventKind::Agent { event } => activity(event),
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
        }
        if let Some(last) = em.events.last() {
            self.snap.cursor = last.cursor;
        }
        out.events = em.events;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wasit_core::GateDecision;

    fn meta() -> SessionMeta {
        SessionMeta {
            id: crate::wire::SessionId("aaaaaaaaaaaa".into()),
            driver: "claude".into(),
            cwd: None,
            labels: Default::default(),
            created_at_ms: 0,
            display_name: None,
            command: None,
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
            }
            .to_value(),
        );
        assert!(out.started);
        assert_eq!(live.snapshot().phase, Phase::Starting);
        feed(&mut live, &mut j, Kind::Out, json!(INIT_OK));
        assert_eq!(
            live.snapshot().phase,
            Phase::Working,
            "first prompt went out"
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
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("w1".into(), CommandStatus::Sent)]);
        assert!(live.snapshot().pending_gates.is_empty());
        assert!(out.events.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Agent { event: AgentEvent::GateResolved { request_id, reason } }
                if request_id == "x2" && reason.as_deref() == Some("withdrawn")
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
                    request_id: "x2".into()
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
                if request_id == "x3" && reason.as_deref() == Some("withdrawn")
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

    /// Drives a core like the live actor (records in, the shim's `in` echo for writes when
    /// asked), keeping the journal so a replay can be compared.
    struct Sim {
        core: Core,
        j: J,
        events: Vec<SessionEvent>,
    }

    impl Sim {
        fn new() -> Self {
            Sim {
                core: Core::new(wasit_claude::descriptor(), &meta()),
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
            })
        }

        /// A fresh core over the same journal publishes exactly the same events and state.
        fn assert_replay_exact(&self) {
            let mut replay = Core::new(wasit_claude::descriptor(), &meta());
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
        assert!(at("input_sent:2") < at("turn_started:2"));
        assert!(at("turn_started:2") < at("written:2"));
        assert_eq!(k.iter().filter(|x| x.starts_with("input:")).count(), 1);
        let snap = sim.core.snapshot();
        assert_eq!(snap.turn.as_ref().unwrap().prompt.as_deref(), Some("hello"));
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
                "turn_started:2",
                "phase",
                "written:2",
                "turn_completed",
                "phase"
            ]
            .map(|s| s.to_string())
        );
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
        let (b, out) = sim.command(Command::prompt("B"));
        assert!(out.writes.is_empty(), "B waits behind A's turn");
        let (c, _) = sim.command(Command::prompt("C"));
        sim.command(Command::Interrupt);
        sim.feed(
            Kind::Out,
            json!(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1}"#),
        );
        let (d, _) = sim.command(Command::prompt("D"));
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
                    PhaseCause::TurnStarted { input: Some(go) }
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
}
