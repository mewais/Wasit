//! The session trail: a content-free projection of a session's events (design §12.5), computed
//! on the session's host, served over the control protocol (`session.trail:<id>`,
//! `session.trail.page`) and never written to a log file.
//!
//! A session's events carry content (prompts, replies, tool input and output, a gate's free text,
//! what the CLI wrote to stderr). The trail is what remains when only facts are kept: which
//! input reached which stage and when, how a turn ended and what it cost, which tool ran, why
//! the phase changed, why the process ended. [`project`] is a pure function of one event, so a
//! trail is the same live, from the actor's ring and from a replay of the journal after a daemon
//! restart, and its cursors are the events' cursors.
//!
//! # Privacy is structural
//!
//! [`TrailEntry`] is declared with `diag_data!`: every field's type must be a `wasit-diag` value
//! type (numbers, [`Ident`], [`Code`], [`Bytes`], [`ErrorInfo`], [`DiagText`], [`TraceId`] …), so
//! a `String`, a `serde_json::Value`, a path or any content type in a field is a compile error at
//! the declaration, exactly as in a log record (`wasit-diag` documents the refusals; its
//! `tests/value_sites.rs` pins every file that invokes the macro, this one included). The same
//! declaration gives the entry its serde form and its TypeScript type, so the shape the UI reads
//! is the shape declared here and nothing else is ever serialized.
//!
//! What each kind of content becomes:
//!
//! * text (prompt, answer, deny reason, stderr line, reply): its length in characters;
//! * attachments: their count and total size, never a name or a path;
//! * a tool: its name as a [`Code`] (a title or a command line has spaces, slashes or quotes and
//!   is refused by the type), never its input or result;
//! * argv: its length and the flag names that look like flags (`--resume`, `-c`), never a value;
//! * what the CLI wrote as an error (a turn's failure, a rejected command, a notice, a refusal):
//!   [`ErrorInfo::foreign`], i.e. a length and a fingerprint;
//! * identifiers the CLI chose (native session, turn, tool call, gate request, model, mode): an
//!   [`Ident`], which drops anything that is not one;
//! * the reason a process failed or was lost: the runtime's own wording, scrubbed
//!   ([`DiagText`], `Origin::Own`).
//!
//! A new [`AgentEvent`] or [`SessionEventKind`] variant is matched exhaustively below, so adding
//! one forces a decision here: project it, or list it as not projected.

use wasit_control::TraceId;
use wasit_core::{AgentEvent, Command, EndReason, GateRequest, InputDropReason, InputId};
use wasit_diag::{Bytes, Code, DiagText, ErrorInfo, Ident, Origin, VEC_MAX};

use crate::wire::{
    AnomalyKind, CommandStatus, GateClearing, NoticeCode, Phase, PhaseCause, SessionEvent,
    SessionEventKind, StartResume, TrailItem,
};

wasit_diag::diag_data! {
    #[diag(tag = "kind")]
    /// Why the phase changed (the last fact of the journal record that decided it).
    #[cfg_attr(feature = "ts", derive(ts_rs::TS))]
    pub enum TrailCause {
        /// A process generation was spawned.
        #[diag(ev = "spawned")]
        Spawned { generation: u32 },
        /// The CLI completed its handshake.
        #[diag(ev = "handshake_done")]
        HandshakeDone {},
        /// A turn started, for this input when it is one of ours.
        #[diag(ev = "turn_started")]
        TurnStarted { ; input: u64 },
        /// A turn ended.
        #[diag(ev = "turn_completed")]
        TurnCompleted { interrupted: bool ; end_reason: Code },
        /// A gate was raised, by the CLI or from outside its protocol (`external`).
        #[diag(ev = "gate_raised")]
        GateRaised { request_id: Ident, external: bool },
        /// A gate stopped being pending: `answered`, `resolved` or `withdrawn`.
        #[diag(ev = "gate_cleared")]
        GateCleared { request_id: Ident, how: Code },
        /// The process exited.
        #[diag(ev = "exited")]
        Exited {
            requested: bool,
            handshake_timeout: bool
            ; code: i32, signal: i32
        },
        /// The shim vanished without an exit record.
        #[diag(ev = "lost")]
        Lost {},
        /// Nothing more specific is known.
        #[diag(ev = "other")]
        Other {},
    }
}

wasit_diag::diag_data! {
    #[diag(tag = "ev")]
    /// One fact about a session, served as `{cursor, t, ev, …fields}` (see [`TrailItem`]).
    #[cfg_attr(feature = "ts", derive(ts_rs::TS))]
    pub enum TrailEntry {
        /// A process generation started: its argv as a length and its flag names.
        #[diag(ev = "process.spawned")]
        ProcessSpawned {
            generation: u32,
            argc: u32,
            /// Flag names only (`--resume`, `-c`), at most 32.
            flags: Vec<Code>
            ; pid: u32
        },
        /// The generation's `start` note was applied: how the CLI was asked to start.
        #[diag(ev = "process.started")]
        ProcessStarted {
            generation: u32,
            /// `fresh`, `continue` or `resume`.
            resume: Code
            ; resume_id: Ident,
            model: Ident,
            mode: Ident,
            /// The first prompt journaled with the start (its `input.applied` follows).
            first_input: u64,
            /// The trace id of the request that started the session.
            trace: TraceId
        },
        /// The CLI identified the session: counts of what it offers, ids it chose.
        #[diag(ev = "agent.init")]
        AgentInit {
            tools: u32,
            mcp_servers: u32,
            slash_commands: u32
            ; native_session_id: Ident,
            model: Ident,
            cli_version: Ident,
            mode: Ident
        },
        /// A command was applied in journal order; `input` is its identity (the journal seq of
        /// its note).
        #[diag(ev = "input.applied")]
        InputApplied {
            input: u64,
            /// The command's name (`prompt`, `answer_gate`, `interrupt`, …).
            kind: Code,
            /// Characters of the text it carries (a prompt, a deny reason, an answer, slash
            /// arguments).
            chars: u32,
            attachments: u32,
            /// Total size of the attachments.
            bytes: Bytes,
            /// `sent`, `queued` (held by the driver until its turn), `unsupported`, `rejected`,
            /// `not_running` or `withdrawn`.
            status: Code
            ; trace: TraceId,
            /// The gate request, model, mode or slash command it names.
            target: Ident,
            /// The choice id of a gate answer.
            choice: Code,
            /// Why it was not accepted: length and fingerprint of the reason.
            reason: ErrorInfo
        },
        /// A queued input left the driver's queue (its lines are written next).
        #[diag(ev = "input.sent")]
        InputSent { input: u64 },
        /// Every line the input asked for reached the CLI's stdin.
        #[diag(ev = "input.written")]
        InputWritten { input: u64 },
        /// Writing to the CLI's stdin failed.
        #[diag(ev = "input.write_failed")]
        InputWriteFailed { error: ErrorInfo ; input: u64 },
        /// The CLI itself reported taking the input in.
        #[diag(ev = "input.acknowledged")]
        InputAcknowledged { input: u64, steered: bool },
        /// The input will never reach the CLI.
        #[diag(ev = "input.dropped")]
        InputDropped {
            input: u64,
            /// `interrupted`, `shutdown`, `unencodable`, `refused` or `process_ended`.
            reason: Code
            ; error: ErrorInfo
        },
        /// A turn started.
        #[diag(ev = "turn.started")]
        TurnStarted { ; input: u64, native_turn_id: Ident },
        /// A turn ended: how, and what it cost.
        #[diag(ev = "turn.completed")]
        TurnCompleted {
            interrupted: bool,
            num_turns: u64,
            input_tokens: u64,
            output_tokens: u64,
            cost_usd_micros: u64,
            reply_chars: u32
            ; end_reason: Code,
            /// Length and fingerprint of the failure the CLI reported (only when the turn
            /// ended in an execution error).
            error: ErrorInfo
        },
        /// A tool call started.
        #[diag(ev = "tool.started")]
        ToolStarted { call_id: Ident, name: Code, subagent: bool },
        /// A tool call finished.
        #[diag(ev = "tool.finished")]
        ToolFinished { call_id: Ident, ok: bool },
        /// The agent is blocked on a human decision.
        #[diag(ev = "gate.raised")]
        GateRaised {
            request_id: Ident,
            /// `permission`, `plan`, `question`, `handoff` or `spawn`.
            gate_type: Code,
            /// Choices on offer (none for a question).
            choices: u32
            ; tool: Code
        },
        /// A gate stopped being pending without the human answering it.
        #[diag(ev = "gate.resolved")]
        GateResolved { request_id: Ident ; reason: Code },
        /// The active model changed.
        #[diag(ev = "agent.model")]
        AgentModel { model: Ident },
        /// The current mode changed.
        #[diag(ev = "agent.mode")]
        AgentMode { mode: Ident },
        /// The CLI rejected a command we sent.
        #[diag(ev = "agent.rejected")]
        AgentRejected { command: Code, error: ErrorInfo },
        /// A notice from the CLI: length and fingerprint.
        #[diag(ev = "agent.notice")]
        AgentNotice { error: ErrorInfo },
        /// Context was compacted.
        #[diag(ev = "agent.compaction")]
        AgentCompaction {
            pre_tokens: u64,
            post_tokens: u64,
            duration_ms: u64
            ; trigger: Code
        },
        /// The provider reported a rate-limit status.
        #[diag(ev = "agent.rate_limited")]
        AgentRateLimited {
            resets_at: u64,
            is_using_overage: bool
            ; status: Code,
            rate_limit_type: Code
        },
        /// A line the protocol did not recognise: only the `type` / `method` / `subtype` word of
        /// it, `line` when it was not a JSON object.
        #[diag(ev = "agent.unknown")]
        AgentUnknown { raw_type: Code },
        /// The CLI wrote a line to stderr: its length.
        #[diag(ev = "process.stderr")]
        ProcessStderr { chars: u32 },
        /// The phase changed, and why.
        #[diag(ev = "phase")]
        Phase {
            /// `starting`, `idle`, `working`, `needs_you`, `exited`, `failed` or `lost`.
            to: Code,
            cause: TrailCause
            ; /// Why a process failed or was lost, in the runtime's own words.
            reason: DiagText
        },
        /// The process exited.
        #[diag(ev = "process.exited")]
        ProcessExited { ; code: i32, signal: i32 },
        /// The shim vanished without an exit record.
        #[diag(ev = "process.lost")]
        ProcessLost { reason: DiagText },
        /// A runtime or shim annotation: its code (`stop_requested`, `signal_sent`, …).
        #[diag(ev = "runtime.notice")]
        RuntimeNotice { code: Code },
        /// Something that does not fit the session's state (`activity_while_idle`); `first` is
        /// the type of the event that showed it.
        #[diag(ev = "anomaly")]
        Anomaly { kind: Code, first: Code },
    }
}

/// The entry `event` projects to, with its cursor and time; `None` for events the trail does not
/// keep (the content itself: deltas, tool updates, usage, plans, catalogs).
pub fn item(event: &SessionEvent) -> Option<TrailItem> {
    project(event).map(|entry| TrailItem {
        cursor: event.cursor,
        t: event.t,
        entry,
    })
}

/// The trail entry of one session event, `None` when the event is not part of the trail.
pub fn project(event: &SessionEvent) -> Option<TrailEntry> {
    Some(match &event.kind {
        SessionEventKind::Spawned {
            generation,
            pid,
            argv,
        } => TrailEntry::ProcessSpawned {
            generation: *generation,
            argc: count(argv.len()),
            flags: flags(argv),
            pid: *pid,
        },
        SessionEventKind::Started {
            generation,
            resume,
            resume_id,
            model,
            mode,
            first_input,
            trace,
            ..
        } => TrailEntry::ProcessStarted {
            generation: *generation,
            resume: Code::st(match resume {
                StartResume::Fresh => "fresh",
                StartResume::Continue => "continue",
                StartResume::Resume => "resume",
            }),
            resume_id: resume_id.as_deref().map(Ident::new),
            model: model.as_deref().map(Ident::new),
            mode: mode.as_deref().map(Ident::new),
            first_input: first_input.map(|i| i.0),
            trace: trace.clone(),
        },
        SessionEventKind::Agent { event: agent } => return project_agent(agent),
        SessionEventKind::Input {
            input,
            command,
            status,
            trace,
            ..
        } => applied(*input, command, status, trace.clone()),
        SessionEventKind::Written { input, .. } => TrailEntry::InputWritten { input: input.0 },
        SessionEventKind::WriteFailed { input, error, .. } => TrailEntry::InputWriteFailed {
            // The shim words this itself, from the failed write alone: `write to child stdin:
            // <the OS's reason>`.
            error: ErrorInfo::ours(Code::st("stdin_write"), error),
            input: input.map(|i| i.0),
        },
        SessionEventKind::Stderr { text } => TrailEntry::ProcessStderr { chars: chars(text) },
        SessionEventKind::Phase { phase, cause, .. } => TrailEntry::Phase {
            to: Code::st(phase.name()),
            cause: cause_of(cause),
            reason: match phase {
                Phase::Failed { reason, .. } | Phase::Lost { reason } => {
                    Some(DiagText::new(Origin::Own, reason))
                }
                Phase::Starting
                | Phase::Idle
                | Phase::Working
                | Phase::NeedsYou
                | Phase::Exited { .. } => None,
            },
        },
        SessionEventKind::Anomaly { anomaly, first, .. } => TrailEntry::Anomaly {
            kind: Code::st(match anomaly {
                AnomalyKind::ActivityWhileIdle => "activity_while_idle",
            }),
            first: Code::new(first),
        },
        SessionEventKind::Exited { code, signal } => TrailEntry::ProcessExited {
            code: *code,
            signal: *signal,
        },
        SessionEventKind::Lost { reason } => TrailEntry::ProcessLost {
            reason: DiagText::new(Origin::Own, reason),
        },
        SessionEventKind::Notice { code, .. } => TrailEntry::RuntimeNotice {
            code: Code::st(match code {
                NoticeCode::StopRequested => "stop_requested",
                NoticeCode::HandshakeDeadline => "handshake_deadline",
                NoticeCode::SignalSent => "signal_sent",
                NoticeCode::PreviousGenerationUnclosed => "previous_generation_unclosed",
                NoticeCode::StartIgnored => "start_ignored",
                NoticeCode::Shim => "shim",
                NoticeCode::UnrecognisedNote => "unrecognised_note",
                NoticeCode::Other => "other",
            }),
        },
    })
}

fn project_agent(event: &AgentEvent) -> Option<TrailEntry> {
    Some(match event {
        AgentEvent::SessionInit {
            native_session_id,
            model,
            tools,
            mcp_servers,
            slash_commands,
            current_mode,
            cli_version,
            ..
        } => TrailEntry::AgentInit {
            tools: count(tools.len()),
            mcp_servers: count(mcp_servers.len()),
            slash_commands: count(slash_commands.len()),
            native_session_id: native_session_id.as_deref().map(Ident::new),
            model: model.as_deref().map(Ident::new),
            cli_version: non_empty(cli_version).map(Ident::new),
            mode: non_empty(current_mode).map(Ident::new),
        },
        AgentEvent::TurnStarted {
            native_turn_id,
            input,
        } => TrailEntry::TurnStarted {
            input: input.map(|i| i.0),
            native_turn_id: native_turn_id.as_deref().map(Ident::new),
        },
        AgentEvent::InputSent { input } => TrailEntry::InputSent { input: input.0 },
        AgentEvent::InputAcknowledged { input, steered } => TrailEntry::InputAcknowledged {
            input: input.0,
            steered: *steered,
        },
        AgentEvent::InputDropped {
            input,
            reason,
            error,
        } => TrailEntry::InputDropped {
            input: input.0,
            reason: Code::st(match reason {
                InputDropReason::Interrupted => "interrupted",
                InputDropReason::Shutdown => "shutdown",
                InputDropReason::Unencodable => "unencodable",
                InputDropReason::Refused => "refused",
                InputDropReason::ProcessEnded => "process_ended",
            }),
            // Words of the driver or of the CLI: never kept, only their length and fingerprint.
            error: error
                .as_deref()
                .map(|e| ErrorInfo::foreign(Code::st("dropped"), e)),
        },
        AgentEvent::TurnCompleted {
            end_reason,
            was_interrupted,
            num_turns,
            usage,
            reply,
            ..
        } => TrailEntry::TurnCompleted {
            interrupted: *was_interrupted,
            num_turns: *num_turns,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cost_usd_micros: micros(usage.cost_usd),
            reply_chars: reply.as_deref().map_or(0, chars),
            end_reason: end_reason.map(end_reason_code),
            // The reply of a failed turn is the CLI's error message.
            error: match (end_reason, reply) {
                (Some(EndReason::ExecutionError), Some(r)) => {
                    Some(ErrorInfo::foreign(Code::st("execution_error"), r))
                }
                _ => None,
            },
        },
        AgentEvent::ToolCallStarted {
            name,
            call_id,
            is_subagent,
            ..
        } => TrailEntry::ToolStarted {
            call_id: Ident::new(call_id),
            name: Code::new(name),
            subagent: *is_subagent,
        },
        AgentEvent::ToolCallFinished { call_id, ok, .. } => TrailEntry::ToolFinished {
            call_id: Ident::new(call_id),
            ok: *ok,
        },
        AgentEvent::GateRequested { gate } => raised(gate),
        AgentEvent::GateResolved { request_id, reason } => TrailEntry::GateResolved {
            request_id: Ident::new(request_id),
            reason: reason.as_ref().map(|r| Code::new(&r.code)),
        },
        AgentEvent::ModelChanged { model } => TrailEntry::AgentModel {
            model: Ident::new(model),
        },
        AgentEvent::ModesUpdated { current_mode, .. } => TrailEntry::AgentMode {
            mode: Ident::new(current_mode.as_deref()?),
        },
        AgentEvent::CommandRejected { command, message } => TrailEntry::AgentRejected {
            command: Code::new(command),
            error: ErrorInfo::foreign(Code::st("rejected"), message),
        },
        AgentEvent::Notice { message } => TrailEntry::AgentNotice {
            error: ErrorInfo::foreign(Code::st("notice"), message),
        },
        AgentEvent::Compaction { info } => TrailEntry::AgentCompaction {
            pre_tokens: info.pre_tokens,
            post_tokens: info.post_tokens,
            duration_ms: info.duration_ms,
            trigger: non_empty(&info.trigger).map(Code::new),
        },
        AgentEvent::RateLimitNotice { info } => TrailEntry::AgentRateLimited {
            resets_at: info.resets_at,
            is_using_overage: info.is_using_overage,
            status: non_empty(&info.status).map(Code::new),
            rate_limit_type: non_empty(&info.rate_limit_type).map(Code::new),
        },
        AgentEvent::Unknown { raw } => TrailEntry::AgentUnknown {
            raw_type: Code::new(
                ["type", "method", "subtype"]
                    .iter()
                    .find_map(|k| raw.get(k).and_then(|v| v.as_str()))
                    .unwrap_or("line"),
            ),
        },
        // Content, or catalogs and counters that carry none of what a trail is for: the text of
        // the assistant and of the user, thinking, what a tool was given and returned, usage
        // meters, plans, the lists of models, modes and slash commands. The runtime's own
        // `Exited` event says the same as `ProcessExited`.
        AgentEvent::AssistantDelta { .. }
        | AgentEvent::AssistantContent { .. }
        | AgentEvent::UserContent { .. }
        | AgentEvent::ThinkingDelta { .. }
        | AgentEvent::ToolCallUpdated { .. }
        | AgentEvent::UsageUpdate { .. }
        | AgentEvent::PlanUpdated { .. }
        | AgentEvent::ModelsUpdated { .. }
        | AgentEvent::SlashCommandsUpdated { .. }
        | AgentEvent::ProcessExited { .. } => return None,
    })
}

/// `input.applied` of a command: sizes, counts and ids of what it carries, never the content.
fn applied(
    input: InputId,
    command: &Command,
    status: &CommandStatus,
    trace: Option<TraceId>,
) -> TrailEntry {
    let (mut n_chars, mut attachments, mut bytes) = (0, 0, 0u64);
    let (mut target, mut choice) = (None, None);
    match command {
        Command::Prompt {
            text,
            attachments: files,
        } => {
            n_chars = chars(text);
            attachments = count(files.len());
            bytes = files.iter().map(|a| a.size).fold(0, u64::saturating_add);
        }
        Command::AnswerGate {
            request_id,
            decision,
        } => {
            n_chars = decision.text.as_deref().map_or(0, chars);
            target = Some(Ident::new(request_id));
            choice = Some(Code::new(&decision.choice));
        }
        Command::AnswerQuestion { request_id, answer } => {
            n_chars = answer
                .answers
                .values()
                .flatten()
                .map(|a| chars(a))
                .fold(0, u32::saturating_add);
            target = Some(Ident::new(request_id));
        }
        Command::SetModel { model } => target = Some(Ident::new(model)),
        Command::SetMode { mode } => target = Some(Ident::new(mode)),
        Command::RunSlash { name, args } => {
            n_chars = chars(args);
            target = Some(Ident::new(name));
        }
        Command::Interrupt | Command::Shutdown => {}
    }
    // Why a command was not accepted is worded by the protocol or the runtime from what the
    // command named; only its length and fingerprint are kept.
    let (status, reason) = match status {
        CommandStatus::Sent => ("sent", None),
        CommandStatus::Queued => ("queued", None),
        CommandStatus::Unsupported { operation } => ("unsupported", Some(operation.as_str())),
        CommandStatus::Rejected { reason } => ("rejected", Some(reason.as_str())),
        CommandStatus::NotRunning { reason } => ("not_running", Some(reason.as_str())),
        CommandStatus::Withdrawn { request_id } => {
            target.get_or_insert_with(|| Ident::new(request_id));
            ("withdrawn", None)
        }
    };
    TrailEntry::InputApplied {
        input: input.0,
        kind: Code::st(command.name()),
        chars: n_chars,
        attachments,
        bytes: Bytes(bytes),
        status: Code::st(status),
        trace,
        target,
        choice,
        reason: reason.map(|r| ErrorInfo::foreign(Code::st(status), r)),
    }
}

/// `gate.raised` of a gate request: its kind, its tool name and how many choices it offers.
fn raised(gate: &GateRequest) -> TrailEntry {
    let (gate_type, tool, choices) = match gate {
        GateRequest::Permission {
            tool_name, choices, ..
        } => ("permission", Some(Code::new(tool_name)), choices.len()),
        GateRequest::Plan { choices, .. } => ("plan", None, choices.len()),
        GateRequest::Question { .. } => ("question", None, 0),
        GateRequest::Handoff { choices, .. } => ("handoff", None, choices.len()),
        GateRequest::Spawn { choices, .. } => ("spawn", None, choices.len()),
    };
    TrailEntry::GateRaised {
        request_id: Ident::new(gate.request_id()),
        gate_type: Code::st(gate_type),
        choices: count(choices),
        tool,
    }
}

fn cause_of(cause: &PhaseCause) -> TrailCause {
    match cause {
        PhaseCause::Spawned { generation } => TrailCause::Spawned {
            generation: *generation,
        },
        PhaseCause::HandshakeDone => TrailCause::HandshakeDone {},
        PhaseCause::TurnStarted { input } => TrailCause::TurnStarted {
            input: input.map(|i| i.0),
        },
        PhaseCause::TurnCompleted {
            end_reason,
            interrupted,
        } => TrailCause::TurnCompleted {
            interrupted: *interrupted,
            end_reason: end_reason.map(end_reason_code),
        },
        PhaseCause::GateRaised {
            request_id,
            external,
        } => TrailCause::GateRaised {
            request_id: Ident::new(request_id),
            external: *external,
        },
        PhaseCause::GateCleared { request_id, how } => TrailCause::GateCleared {
            request_id: Ident::new(request_id),
            how: Code::st(match how {
                GateClearing::Answered => "answered",
                GateClearing::Resolved => "resolved",
                GateClearing::Withdrawn => "withdrawn",
            }),
        },
        PhaseCause::Exited {
            code,
            signal,
            requested,
            handshake_timeout,
        } => TrailCause::Exited {
            requested: *requested,
            handshake_timeout: *handshake_timeout,
            code: *code,
            signal: *signal,
        },
        PhaseCause::Lost => TrailCause::Lost {},
        PhaseCause::Other => TrailCause::Other {},
    }
}

fn end_reason_code(r: EndReason) -> Code {
    Code::st(match r {
        EndReason::Cancelled => "cancelled",
        EndReason::MaxTurns => "max_turns",
        EndReason::ExecutionError => "execution_error",
        EndReason::CompactionNeeded => "compaction_needed",
    })
}

/// The flag names among `argv`: `--name` (lowercase letters, digits and `-`, at most 41
/// characters) or `-x`. Nothing else is kept: not a value, not a subcommand, not an argument
/// with `=`. At most [`VEC_MAX`] names.
fn flags(argv: &[String]) -> Vec<Code> {
    fn is_flag(a: &str) -> bool {
        let b = a.as_bytes();
        match b {
            [b'-', c] => c.is_ascii_alphabetic(),
            [b'-', b'-', rest @ ..] => {
                (1..=41).contains(&rest.len())
                    && rest[0].is_ascii_lowercase()
                    && rest
                        .iter()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
            }
            _ => false,
        }
    }
    argv.iter()
        .filter(|a| is_flag(a))
        .take(VEC_MAX)
        .map(|a| Code::new(a))
        .collect()
}

fn chars(s: &str) -> u32 {
    count(s.chars().count())
}

fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

fn non_empty(s: &str) -> Option<&str> {
    Some(s).filter(|s| !s.is_empty())
}

/// Dollars as whole millionths, saturating; a cost that is not a finite non-negative number is 0.
fn micros(usd: f64) -> u64 {
    if usd.is_finite() && usd > 0.0 {
        // Saturating float-to-int conversion: the intent.
        (usd * 1_000_000.0).round() as u64
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    const TRACE: &str = "u-0123456789abcdef";

    /// One of every variant of `TrailCause`, every optional field present.
    fn causes() -> Vec<TrailCause> {
        vec![
            TrailCause::Spawned { generation: 1 },
            TrailCause::HandshakeDone {},
            TrailCause::TurnStarted { input: Some(5) },
            TrailCause::TurnCompleted {
                interrupted: false,
                end_reason: Some(Code::st("cancelled")),
            },
            TrailCause::GateRaised {
                request_id: Ident::st("g1"),
                external: false,
            },
            TrailCause::GateCleared {
                request_id: Ident::st("g1"),
                how: Code::st("answered"),
            },
            TrailCause::Exited {
                requested: true,
                handshake_timeout: false,
                code: Some(0),
                signal: Some(15),
            },
            TrailCause::Lost {},
            TrailCause::Other {},
        ]
    }

    /// One of every variant of `TrailEntry`, every optional field present.
    fn entries() -> Vec<TrailEntry> {
        let err = || ErrorInfo::foreign(Code::st("x"), "words");
        let id = || Ident::st("id-1");
        let code = || Code::st("word");
        vec![
            TrailEntry::ProcessSpawned {
                generation: 1,
                argc: 3,
                flags: vec![code()],
                pid: Some(9),
            },
            TrailEntry::ProcessStarted {
                generation: 1,
                resume: code(),
                resume_id: Some(id()),
                model: Some(id()),
                mode: Some(id()),
                first_input: Some(4),
                trace: Some(TraceId::new(TRACE).unwrap()),
            },
            TrailEntry::AgentInit {
                tools: 1,
                mcp_servers: 2,
                slash_commands: 3,
                native_session_id: Some(id()),
                model: Some(id()),
                cli_version: Some(id()),
                mode: Some(id()),
            },
            TrailEntry::InputApplied {
                input: 5,
                kind: code(),
                chars: 6,
                attachments: 1,
                bytes: Bytes(7),
                status: code(),
                trace: Some(TraceId::new(TRACE).unwrap()),
                target: Some(id()),
                choice: Some(code()),
                reason: Some(err()),
            },
            TrailEntry::InputSent { input: 5 },
            TrailEntry::InputWritten { input: 5 },
            TrailEntry::InputWriteFailed {
                error: err(),
                input: Some(5),
            },
            TrailEntry::InputAcknowledged {
                input: 5,
                steered: false,
            },
            TrailEntry::InputDropped {
                input: 5,
                reason: code(),
                error: Some(err()),
            },
            TrailEntry::TurnStarted {
                input: Some(5),
                native_turn_id: Some(id()),
            },
            TrailEntry::TurnCompleted {
                interrupted: false,
                num_turns: 1,
                input_tokens: 2,
                output_tokens: 3,
                cost_usd_micros: 4,
                reply_chars: 5,
                end_reason: Some(code()),
                error: Some(err()),
            },
            TrailEntry::ToolStarted {
                call_id: id(),
                name: code(),
                subagent: false,
            },
            TrailEntry::ToolFinished {
                call_id: id(),
                ok: true,
            },
            TrailEntry::GateRaised {
                request_id: id(),
                gate_type: code(),
                choices: 2,
                tool: Some(code()),
            },
            TrailEntry::GateResolved {
                request_id: id(),
                reason: Some(code()),
            },
            TrailEntry::AgentModel { model: id() },
            TrailEntry::AgentMode { mode: id() },
            TrailEntry::AgentRejected {
                command: code(),
                error: err(),
            },
            TrailEntry::AgentNotice { error: err() },
            TrailEntry::AgentCompaction {
                pre_tokens: 1,
                post_tokens: 2,
                duration_ms: 3,
                trigger: Some(code()),
            },
            TrailEntry::AgentRateLimited {
                resets_at: 1,
                is_using_overage: false,
                status: Some(code()),
                rate_limit_type: Some(code()),
            },
            TrailEntry::AgentUnknown { raw_type: code() },
            TrailEntry::ProcessStderr { chars: 3 },
            TrailEntry::Phase {
                to: code(),
                cause: TrailCause::Lost {},
                reason: Some(DiagText::new(Origin::Own, "the shim is gone")),
            },
            TrailEntry::ProcessExited {
                code: Some(1),
                signal: Some(2),
            },
            TrailEntry::ProcessLost {
                reason: DiagText::new(Origin::Own, "gone"),
            },
            TrailEntry::RuntimeNotice { code: code() },
            TrailEntry::Anomaly {
                kind: code(),
                first: code(),
            },
        ]
    }

    /// The keys of `v` except `tag`, as a set.
    fn keys<'a>(v: &'a serde_json::Value, tag: &str) -> BTreeSet<&'a str> {
        v.as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .filter(|k| *k != tag)
            .collect()
    }

    fn declared(
        table: &[(&'static str, &'static [&'static str])],
        ev: &str,
    ) -> BTreeSet<&'static str> {
        table
            .iter()
            .find(|(n, _)| *n == ev)
            .unwrap_or_else(|| panic!("{ev} is not declared"))
            .1
            .iter()
            .copied()
            .collect()
    }

    /// A gate resolution's reason is recorded as its code: the resolver's detail text is never
    /// part of the trail.
    #[test]
    fn a_gate_resolution_keeps_its_code_and_never_its_detail() {
        let event = AgentEvent::GateResolved {
            request_id: "g1".into(),
            reason: Some(wasit_core::GateReason::new("denied").with_detail("SECRET words")),
        };
        let entry = project_agent(&event).unwrap();
        assert_eq!(
            entry,
            TrailEntry::GateResolved {
                request_id: Ident::st("g1"),
                reason: Some(Code::st("denied")),
            }
        );
        assert!(!serde_json::to_string(&entry).unwrap().contains("SECRET"));
    }

    /// Every variant serializes to exactly its declared fields: the declaration, the samples and
    /// the wire form agree, and no variant is left without a sample.
    #[test]
    fn every_entry_serializes_to_exactly_its_declared_fields() {
        let samples = entries();
        let evs: Vec<&str> = samples.iter().map(TrailEntry::ev).collect();
        let want: Vec<&str> = TrailEntry::DECLARED.iter().map(|(n, _)| *n).collect();
        assert_eq!(evs, want, "one sample per variant, in declaration order");
        for e in &samples {
            let v = serde_json::to_value(e).unwrap();
            assert_eq!(v["ev"], e.ev());
            assert_eq!(
                keys(&v, "ev"),
                declared(TrailEntry::DECLARED, e.ev()),
                "{v}"
            );
            // And reads back as what it was.
            assert_eq!(&serde_json::from_value::<TrailEntry>(v).unwrap(), e);
        }
        let samples = causes();
        let evs: Vec<&str> = samples.iter().map(TrailCause::ev).collect();
        let want: Vec<&str> = TrailCause::DECLARED.iter().map(|(n, _)| *n).collect();
        assert_eq!(evs, want);
        for c in &samples {
            let v = serde_json::to_value(c).unwrap();
            assert_eq!(v["kind"], c.ev());
            assert_eq!(
                keys(&v, "kind"),
                declared(TrailCause::DECLARED, c.ev()),
                "{v}"
            );
            assert_eq!(&serde_json::from_value::<TrailCause>(v).unwrap(), c);
        }
        // Optional fields are left out when absent.
        let v = serde_json::to_value(TrailEntry::InputApplied {
            input: 1,
            kind: Code::st("interrupt"),
            chars: 0,
            attachments: 0,
            bytes: Bytes(0),
            status: Code::st("sent"),
            trace: None,
            target: None,
            choice: None,
            reason: None,
        })
        .unwrap();
        assert_eq!(
            keys(&v, "ev"),
            ["input", "kind", "chars", "attachments", "bytes", "status"]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn flags_are_names_never_values() {
        let argv: Vec<String> = [
            "claude",
            "--resume",
            "0123-uuid",
            "--mcp-config",
            "{\"mcpServers\":{}}",
            "--append-system-prompt",
            "-be careful with the keys",
            "--model=secret-model",
            "-c",
            "-x7",
            "--Dangerous",
            "--",
            "--a",
            "--output-format",
        ]
        .map(String::from)
        .to_vec();
        let got: Vec<String> = flags(&argv)
            .iter()
            .map(|c| c.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            got,
            [
                "--resume",
                "--mcp-config",
                "--append-system-prompt",
                "-c",
                "--a",
                "--output-format"
            ]
        );
        let many: Vec<String> = (0..100).map(|i| format!("--f{i}")).collect();
        assert_eq!(flags(&many).len(), VEC_MAX);
    }

    fn ev(kind: SessionEventKind) -> SessionEvent {
        SessionEvent {
            cursor: 65_536,
            t: 1_000,
            kind,
        }
    }

    /// Content in every shape an event can hold it is gone from what is projected, and what
    /// remains is the size and the ids.
    #[test]
    fn projecting_content_keeps_sizes_and_ids_only() {
        const M: &str = "MARKER-PROJ";
        let attachment = wasit_core::Attachment {
            name: format!("{M}-name.txt"),
            mime: "text/plain".into(),
            size: 12,
            path: format!("/tmp/{M}/path"),
            data: Some(format!("{M}-data")),
        };
        let events = vec![
            ev(SessionEventKind::Input {
                input: InputId(7),
                command: Command::Prompt {
                    text: format!("{M} prompt"),
                    attachments: vec![attachment.clone(), attachment],
                },
                status: CommandStatus::Rejected {
                    reason: format!("{M} reason"),
                },
                trace: Some(TraceId::new(TRACE).unwrap()),
            }),
            ev(SessionEventKind::Input {
                input: InputId(8),
                command: Command::AnswerGate {
                    request_id: "g9".into(),
                    decision: wasit_core::GateDecision::choose_with("deny", Some(M)),
                },
                status: CommandStatus::Sent,
                trace: None,
            }),
            ev(SessionEventKind::Stderr {
                text: format!("{M} stderr"),
            }),
            ev(SessionEventKind::WriteFailed {
                input: Some(InputId(7)),
                error: "write to child stdin: Broken pipe (os error 32)".into(),
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::Notice {
                    message: format!("{M} notice"),
                },
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::CommandRejected {
                    command: "set_model".into(),
                    message: format!("{M} rejected"),
                },
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::Unknown {
                    raw: serde_json::json!(format!("{M} garbage line")),
                },
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::ToolCallStarted {
                    name: "Bash".into(),
                    call_id: "call_1".into(),
                    input: serde_json::json!({"command": M}),
                    is_subagent: false,
                    subagent_model: None,
                },
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::ToolCallStarted {
                    name: format!("Run {M} now"),
                    call_id: format!("call {M}"),
                    input: serde_json::Value::Null,
                    is_subagent: false,
                    subagent_model: None,
                },
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::TurnCompleted {
                    end_reason: Some(EndReason::ExecutionError),
                    was_interrupted: false,
                    num_turns: 1,
                    usage: Default::default(),
                    per_model: Default::default(),
                    reply: Some(format!("{M} failure")),
                },
            }),
            ev(SessionEventKind::Agent {
                event: AgentEvent::GateRequested {
                    gate: GateRequest::Permission {
                        request_id: "r1".into(),
                        tool_name: "Bash".into(),
                        command: format!("rm -rf {M}"),
                        cwd: format!("/home/{M}"),
                        explanation: M.into(),
                        choices: vec![],
                        raw_input: serde_json::json!({"x": M}),
                    },
                },
            }),
            ev(SessionEventKind::Phase {
                phase: Phase::Failed {
                    reason: "the CLI exited with code 3".into(),
                    stderr_tail: vec![format!("{M} tail")],
                },
                cause: PhaseCause::Other,
            }),
            ev(SessionEventKind::Notice {
                message: format!("{M} shim note"),
                code: NoticeCode::Shim,
            }),
        ];
        let mut shown = Vec::new();
        for e in &events {
            let entry = project(e).unwrap_or_else(|| panic!("{e:?} projects to nothing"));
            let json = serde_json::to_string(&entry).unwrap();
            assert!(!json.contains(M), "{json}");
            shown.push(serde_json::to_value(&entry).unwrap());
        }
        // The prompt: size, count, bytes, trace and the length and fingerprint of the refusal.
        assert_eq!(shown[0]["ev"], "input.applied");
        assert_eq!(shown[0]["chars"], 18);
        assert_eq!(shown[0]["attachments"], 2);
        assert_eq!(shown[0]["bytes"], 24);
        assert_eq!(shown[0]["trace"], TRACE);
        assert_eq!(shown[0]["status"], "rejected");
        assert_eq!(shown[0]["reason"]["len"], 18);
        // The deny reason is its length; the choice and the request are ids.
        assert_eq!(shown[1]["chars"], M.len());
        assert_eq!(shown[1]["choice"], "deny");
        assert_eq!(shown[1]["target"], "g9");
        assert_eq!(
            shown[2],
            serde_json::json!({"ev": "process.stderr", "chars": 18})
        );
        assert_eq!(
            shown[6],
            serde_json::json!({"ev": "agent.unknown", "raw_type": "line"})
        );
        // A tool's title or command line is not a name.
        assert_eq!(shown[7]["name"], "Bash");
        assert_eq!(shown[8]["name"], "<other:19>");
        assert_eq!(shown[8]["call_id"], "<other:16>");
        assert_eq!(shown[9]["end_reason"], "execution_error");
        assert_eq!(shown[9]["error"]["len"], 19);
        assert_eq!(shown[10]["tool"], "Bash");
        assert_eq!(shown[11]["reason"], "the CLI exited with code 3");
        assert!(events.len() == shown.len());
    }

    #[test]
    fn events_that_are_content_or_catalogs_are_not_projected() {
        for event in [
            AgentEvent::AssistantDelta {
                text: "x".into(),
                message_id: None,
            },
            AgentEvent::ThinkingDelta {
                text: "x".into(),
                message_id: None,
            },
            AgentEvent::ToolCallUpdated {
                call_id: "c".into(),
                name: "n".into(),
                content: vec![],
                status: None,
                input: None,
                parts: vec![],
            },
            AgentEvent::UsageUpdate {
                usage: Default::default(),
                context_pct: None,
            },
            AgentEvent::PlanUpdated { entries: vec![] },
            AgentEvent::ModelsUpdated {
                models: vec![],
                current: None,
            },
            AgentEvent::SlashCommandsUpdated { commands: vec![] },
            AgentEvent::ModesUpdated {
                available_modes: vec![],
                current_mode: None,
            },
        ] {
            assert!(
                project(&ev(SessionEventKind::Agent {
                    event: event.clone()
                }))
                .is_none(),
                "{event:?}"
            );
        }
    }

    #[test]
    fn a_cost_is_whole_millionths_and_never_negative_or_not_a_number() {
        assert_eq!(micros(0.0123456), 12_346);
        assert_eq!(micros(-1.0), 0);
        assert_eq!(micros(f64::NAN), 0);
        assert_eq!(micros(f64::INFINITY), 0);
        assert_eq!(micros(1e30), u64::MAX);
    }

    #[test]
    fn resume_modes_have_a_code() {
        // Pins the mapping the trail shows for how a generation was asked to start.
        for (r, want) in [
            (StartResume::Fresh, "fresh"),
            (StartResume::Continue, "continue"),
            (StartResume::Resume, "resume"),
        ] {
            let e = ev(SessionEventKind::Started {
                generation: 1,
                resume: r,
                resume_id: None,
                model: None,
                mode: None,
                first_input: None,
                trace: None,
            });
            let v = serde_json::to_value(project(&e).unwrap()).unwrap();
            assert_eq!(v["resume"], want);
        }
    }
}

/// The TypeScript the UI reads (`--features ts`).
#[cfg(all(test, feature = "ts"))]
mod ts_tests {
    use ts_rs::TS;

    use super::*;

    #[test]
    fn the_typescript_of_the_trail_is_the_declared_shape() {
        let cfg = ts_rs::Config::default();
        let entry = TrailEntry::decl(&cfg);
        let cause = TrailCause::decl(&cfg);
        let item = crate::wire::TrailItem::decl(&cfg);
        // Tagged unions, optional fields optional, value types as words and numbers.
        assert!(
            entry.contains("{ \"ev\": \"input.applied\", input: bigint,"),
            "{entry}"
        );
        assert!(entry.contains("trace?: TraceId | null"), "{entry}");
        assert!(entry.contains("cause: TrailCause"), "{entry}");
        assert!(entry.contains("flags: Array<string>"), "{entry}");
        assert!(
            entry.contains("error?: { code: string, text?: string, len?: number"),
            "{entry}"
        );
        assert!(
            cause.contains("{ \"kind\": \"turn_started\", input?: bigint | null, }"),
            "{cause}"
        );
        assert!(
            item.contains("type TrailItem = { cursor: bigint,")
                && item.contains("} & ({ \"ev\": \"process.spawned\""),
            "{item}"
        );
    }
}
