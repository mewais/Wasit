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
//! Lines the CLI prints after a `spawned` record but before the generation's `start` note, and
//! commands journaled in that window, are buffered and applied right after `start` — in live
//! operation and in replay alike.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use wasit_core::{
    AgentEvent, Command, DriverDescriptor, DriverError, Encoded, Protocol, SpawnSpec, Step,
};
use wasit_shim::{Kind, Record};

use crate::note::{classify, NoteKind, RuntimeNote};
use crate::wire::{
    event_cursor, synthetic_cursor, CommandStatus, ExitSummary, PendingGate, Phase, SessionEvent,
    SessionEventKind, SessionSnapshot, SubagentInfo, TurnInfo, TurnOutcome,
};

/// Bounded diagnostics.
const STDERR_TAIL_LINES: usize = 40;
const STDERR_LINE_CHARS: usize = 500;
const FAILED_TAIL_LINES: usize = 10;
const TITLE_CHARS: usize = 120;
const REPLY_CHARS: usize = 2000;
const MAX_SUBAGENTS: usize = 100;
/// Cap on owed (written-but-not-echoed) lines; beyond it the oldest are forgotten.
const MAX_OWED: usize = 4096;

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
    Command { token: String, command: Command },
    Stop,
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
    owed: VecDeque<String>,
    close_owed: bool,
    last_seq: u64,
    partial: HashMap<Kind, String>,
    handshake_done: bool,
    stop_requested: bool,
    handshake_timeout: Option<u64>,
    start_t: Option<u64>,
    lost: Option<String>,
    exit_phase: Option<Phase>,
    prompts: VecDeque<String>,
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
            close_owed: false,
            last_seq: 0,
            partial: HashMap::new(),
            handshake_done: false,
            stop_requested: false,
            handshake_timeout: None,
            start_t: None,
            lost: None,
            exit_phase: None,
            prompts: VecDeque::new(),
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
        self.owed.iter()
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
                        self.step(step, &mut em, &mut out);
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
                self.on_echo(&text);
            }
            Kind::Exited => self.on_exited(rec, &mut em, &mut out),
            Kind::Note => self.on_note(rec, &mut em, &mut out),
        }
        self.finish(em, &mut out);
        out
    }

    /// The shim vanished without an exit record. Returns the events to publish (empty when the
    /// journal already ended the generation).
    pub(crate) fn mark_lost(&mut self, reason: &str) -> Vec<SessionEvent> {
        if self.lost.is_some() || matches!(self.gen, Gen::Exited) {
            return Vec::new();
        }
        self.lost = Some(reason.to_string());
        self.clear_live_state();
        let slot = synthetic_cursor(self.last_seq);
        let t = self.snap.last_activity_ms;
        let phase = self.compute_phase();
        self.snap.phase = phase.clone();
        self.snap.cursor = slot;
        vec![
            SessionEvent {
                cursor: slot - 1,
                t,
                kind: SessionEventKind::Lost {
                    reason: reason.to_string(),
                },
            },
            SessionEvent {
                cursor: slot,
                t,
                kind: SessionEventKind::Phase { phase },
            },
        ]
    }

    // ── record handlers ─────────────────────────────────────────────────────

    fn on_spawned(&mut self, rec: &Record, em: &mut Emitter, out: &mut Output) {
        if matches!(self.gen, Gen::Awaiting(_) | Gen::Running(_)) && self.lost.is_none() {
            em.push(SessionEventKind::Notice {
                message: "the previous process ended without an exit record".into(),
            });
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
        self.prompts.clear();
        self.clear_live_state();
        self.snap.generation += 1;
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
        self.gen = Gen::Exited;
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
            NoteKind::Runtime(RuntimeNote::Start { spec }) => self.on_start(&spec, rec, em, out),
            NoteKind::Runtime(RuntimeNote::Command { token, command }) => {
                self.snap.last_activity_ms = rec.t;
                if let Some(request_id) = self.answers_withdrawn(&command) {
                    let status = CommandStatus::Withdrawn { request_id };
                    em.push(SessionEventKind::Input {
                        command,
                        status: status.clone(),
                    });
                    out.resolved.push((token, status));
                    return;
                }
                if self.answers_external(&command) {
                    self.answer_external(token, command, em, out);
                    return;
                }
                match &mut self.gen {
                    Gen::Awaiting(buf) => buf.push(Buffered::Command { token, command }),
                    Gen::Running(_) => self.apply_command(token, command, em, out),
                    Gen::None | Gen::Exited => {
                        let status = CommandStatus::NotRunning {
                            reason: "the process is not running".into(),
                        };
                        em.push(SessionEventKind::Input {
                            command: crate::media::without_bytes(command),
                            status: status.clone(),
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
                });
                match &mut self.gen {
                    Gen::Awaiting(buf) => buf.push(Buffered::Stop),
                    Gen::Running(_) => self.apply_stop(em, out),
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
            NoteKind::Other(message) => em.push(SessionEventKind::Notice { message }),
        }
    }

    fn on_start(&mut self, spec: &SpawnSpec, rec: &Record, em: &mut Emitter, out: &mut Output) {
        if !matches!(self.gen, Gen::Awaiting(_)) {
            em.push(SessionEventKind::Notice {
                message: "ignored a start note outside a fresh process generation".into(),
            });
            return;
        }
        let Gen::Awaiting(buf) = std::mem::replace(&mut self.gen, Gen::None) else {
            return;
        };
        let mut p = self.desc.new_protocol();
        let step = p.start(spec);
        self.gen = Gen::Running(p);
        self.start_t = Some(rec.t);
        out.started = true;
        if let Some(first) = &spec.first_prompt {
            self.prompts.push_back(first.clone());
            if self.snap.title.is_none() {
                self.snap.title = Some(truncate(first, TITLE_CHARS));
            }
        }
        if self.snap.model.is_none() {
            self.snap.model = spec.model.clone().filter(|m| !m.is_empty());
        }
        if self.snap.mode.is_none() && !spec.mode.is_empty() {
            self.snap.mode = Some(spec.mode.clone());
        }
        self.step(step, em, out);
        for b in buf {
            match b {
                Buffered::Line(line) => {
                    if let Gen::Running(p) = &mut self.gen {
                        let step = p.on_line(&line);
                        self.step(step, em, out);
                    }
                }
                Buffered::Command { token, command } => self.apply_command(token, command, em, out),
                Buffered::Stop => self.apply_stop(em, out),
            }
        }
    }

    fn apply_command(
        &mut self,
        token: String,
        command: Command,
        em: &mut Emitter,
        out: &mut Output,
    ) {
        let Gen::Running(p) = &mut self.gen else {
            return;
        };
        let (status, step) = match p.encode(command.clone()) {
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
                Command::Prompt { text, .. } => self.note_prompt(text.clone()),
                Command::RunSlash { name, args } => {
                    let text = if args.is_empty() {
                        format!("/{name}")
                    } else {
                        format!("/{name} {args}")
                    };
                    self.note_prompt(text)
                }
                Command::AnswerGate { request_id, .. }
                | Command::AnswerQuestion { request_id, .. }
                    if status == CommandStatus::Sent =>
                {
                    self.snap
                        .pending_gates
                        .retain(|g| g.gate.request_id() != request_id);
                }
                _ => {}
            }
        }
        em.push(SessionEventKind::Input {
            command: crate::media::without_bytes(command),
            status: status.clone(),
        });
        out.resolved.push((token, status));
        if let Some(step) = step {
            self.step(step, em, out);
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
        command: Command,
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
                    command,
                    status: status.clone(),
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
        }
        em.push(SessionEventKind::Input {
            command,
            status: CommandStatus::Sent,
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

    fn note_prompt(&mut self, text: String) {
        if self.snap.title.is_none() {
            self.snap.title = Some(truncate(&text, TITLE_CHARS));
        }
        self.prompts.push_back(text);
        while self.prompts.len() > 64 {
            self.prompts.pop_front();
        }
    }

    fn apply_stop(&mut self, em: &mut Emitter, out: &mut Output) {
        let Gen::Running(p) = &mut self.gen else {
            return;
        };
        match p.encode(Command::Shutdown) {
            Ok(Encoded::Sent(step)) => self.step(step, em, out),
            // Already shut down / nothing to encode: the actor's escalation takes over.
            Ok(Encoded::Queued) | Err(_) => {}
        }
    }

    fn on_echo(&mut self, text: &str) {
        if self.owed.front().map(String::as_str) == Some(text) {
            self.owed.pop_front();
        } else if let Some(i) = self.owed.iter().position(|l| l == text) {
            self.owed.remove(i);
        }
        // Otherwise a foreign write (another tool on the shim): nothing was owed.
    }

    // ── protocol output ─────────────────────────────────────────────────────

    fn step(&mut self, step: Step, em: &mut Emitter, out: &mut Output) {
        for mut event in step.events {
            crate::media::externalize(&mut event, &mut out.blobs);
            let cursor = em.next_cursor();
            self.reduce(&event, cursor, em.t);
            em.push(SessionEventKind::Agent { event });
        }
        for line in step.write {
            self.owed.push_back(line.0.clone());
            if self.owed.len() > MAX_OWED {
                self.owed.pop_front();
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
            AgentEvent::TurnStarted { native_turn_id } => {
                s.turn = Some(TurnInfo {
                    started_at_ms: t,
                    native_turn_id: native_turn_id.clone(),
                    prompt: self.prompts.pop_front(),
                    tool_calls: 0,
                    last_tool: None,
                    current_tool: None,
                    current_call: None,
                });
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
            }
            AgentEvent::GateResolved { request_id, .. } => {
                s.pending_gates
                    .retain(|g| g.gate.request_id() != request_id);
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
            | AgentEvent::Unknown { .. } => {}
        }
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

    fn finish(&mut self, mut em: Emitter, out: &mut Output) {
        if let Gen::Running(p) = &self.gen {
            if !self.handshake_done && p.is_ready() {
                self.handshake_done = true;
            }
        }
        let phase = self.compute_phase();
        if phase != self.snap.phase {
            self.snap.phase = phase.clone();
            em.push(SessionEventKind::Phase { phase });
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
        let mut feed = |core: &mut Core, j: &mut J, kind, data: serde_json::Value| {
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
            }
            .to_value(),
        );
        assert_eq!(out.resolved, vec![("a1".into(), CommandStatus::Sent)]);
        assert!(out.writes.is_empty());
        assert!(live.snapshot().pending_gates.is_empty());
        assert_eq!(live.snapshot().phase, Phase::Idle);
        assert!(out.events.iter().any(|e| matches!(
            &e.kind,
            SessionEventKind::Input { command: Command::AnswerGate { request_id, .. }, status: CommandStatus::Sent } if request_id == "x1"
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
}
