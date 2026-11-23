//! The runtime's own journal annotations (`note` records with a `"wasit"` tag).
//!
//! Replay exactness rests on these: every input that changes protocol state and does not come
//! from the CLI is journaled *before* its effects, and replay re-applies the note instead of
//! trusting the `in` lines that followed it.

use serde::{Deserialize, Serialize};
use wasit_control::TraceId;
use wasit_core::{
    AgentEvent, Command, GateReason, GateRequest, HistoryReport, SpawnSpec, TurnBinding,
};

use crate::wire::{EarlierUnavailable, NoticeCode};

/// What a `resolve_gate` note journaled before resolutions carried a reason means.
fn withdrawn() -> GateReason {
    GateReason::new(GateReason::WITHDRAWN)
}

/// How the first `start` note of a session that resumes a conversation says the conversation's
/// earlier history reaches the journal, when the agent keeps it in its own storage. The runtime
/// decides it before the process starts (it is the only one who can look at the agent's file
/// before the agent writes to it again) and journals the decision, so a replay never decides
/// anything and an interrupted import resumes from facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub(crate) enum EarlierPlan {
    /// Read the first `len` bytes of the agent's file `path` (the file as it was when the
    /// session started; the agent appends to it from then on) and journal what it holds in
    /// [`RuntimeNote::EarlierChunk`]s.
    Storage { path: String, len: u64 },
    /// The import cannot be set up: the history is unavailable, and this is why.
    Unavailable { why: EarlierUnavailable },
}

/// A runtime note. Serialized as `{"wasit": "<kind>", …}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "wasit", rename_all = "snake_case")]
pub(crate) enum RuntimeNote {
    /// A controller attached; everything journaled before this note was history to it.
    Attach { token: String },
    /// `Protocol::start(spec)` for the current process generation. A `first_prompt` in the
    /// spec is the session's first input, encoded right after `start` with this note's seq as
    /// its id: journaled in the same record as the start, so neither exists without the other.
    /// `trace` is the trace id of the request that started the session, when it had one; the
    /// first prompt's `input` event carries it. A journal of an older build has none.
    Start {
        spec: Box<SpawnSpec>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<TraceId>,
        /// How the earlier history of a conversation this generation resumes reaches the
        /// journal (only the session's first generation carries one; see [`EarlierPlan`]). A
        /// journal of an older build has none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        earlier: Option<EarlierPlan>,
    },
    /// `Protocol::encode(seq, command)`, where `seq` is this note's journal seq. `trace` is the
    /// trace id of the request that carried the command, when it had one.
    Command {
        token: String,
        command: Command,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trace: Option<TraceId>,
    },
    /// A graceful stop was requested (`Protocol::encode(Shutdown)`).
    Stop { token: String },
    /// The handshake deadline passed; the process is being killed.
    HandshakeTimeout { after_ms: u64 },
    /// An external gate was raised on the session from outside the CLI protocol (e.g. an
    /// orchestration handoff). It is pending like any CLI gate until answered with
    /// `answer_gate` / `answer_question` (journaled as a `command` note, never sent to the CLI)
    /// or withdrawn with `resolve_gate`. A `binding` ties it to the turn open when the note is
    /// applied: the core then resolves it itself when that turn is interrupted or ends (see
    /// [`TurnBinding`]). A note journaled before bindings existed has none: the gate belongs to
    /// the session.
    RaiseGate {
        token: String,
        gate: Box<GateRequest>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<TurnBinding>,
    },
    /// An external gate was taken down by its owner without an answer, for `reason` (a note
    /// journaled before resolutions carried one has none: it reads as `withdrawn`).
    ResolveGate {
        token: String,
        request_id: String,
        #[serde(default = "withdrawn")]
        reason: GateReason,
    },
    /// The history items from number `first` (counting from 0 in the order of the agent's own
    /// history) on, a chunk's worth, as the import of the agent's file produced them: each event
    /// is an [`AgentEvent::History`]. Items the journal already holds are skipped by their
    /// number, so a note journaled twice, or an import that resumed after a restart with a
    /// different chunking, never duplicates anything.
    EarlierChunk { first: u64, events: Vec<AgentEvent> },
    /// The import ended: every chunk is journaled. `report` is what the driver's parser
    /// counted (never content).
    EarlierEnd { report: HistoryReport },
    /// The import failed.
    EarlierFailed { reason: EarlierUnavailable },
}

impl RuntimeNote {
    pub(crate) fn to_value(&self) -> serde_json::Value {
        // Plain enum of serde-derived types: serialization cannot fail.
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

/// How the core reads a `note` record.
// Short-lived value classified per journal record; boxing would only add an allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NoteKind {
    Runtime(RuntimeNote),
    /// Shim: stdin was closed.
    StdinClosed,
    /// Shim: writing the line journaled just before failed.
    StdinError(String),
    /// Any other annotation (signal sent, shim diagnostics, unknown runtime note versions).
    Other {
        code: NoticeCode,
        message: String,
    },
}

pub(crate) fn classify(data: &serde_json::Value) -> NoteKind {
    let other = |code, message| NoteKind::Other { code, message };
    if data.get("wasit").is_some() {
        return match serde_json::from_value::<RuntimeNote>(data.clone()) {
            Ok(n) => NoteKind::Runtime(n),
            Err(e) => other(
                NoticeCode::UnrecognisedNote,
                format!("unrecognised runtime note ({e}): {data}"),
            ),
        };
    }
    if data.get("stdin").and_then(|v| v.as_str()) == Some("closed") {
        return NoteKind::StdinClosed;
    }
    if let Some(msg) = data.get("stdin_error").and_then(|v| v.as_str()) {
        return NoteKind::StdinError(msg.to_string());
    }
    if let Some(sig) = data.get("signal").and_then(|v| v.as_str()) {
        return other(
            NoticeCode::SignalSent,
            format!("signal {sig} sent to the process"),
        );
    }
    if let Some(msg) = data.get("shim").and_then(|v| v.as_str()) {
        return other(NoticeCode::Shim, format!("shim: {msg}"));
    }
    other(NoticeCode::Other, data.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notes_round_trip_and_shim_notes_are_recognised() {
        let n = RuntimeNote::Command {
            token: "t".into(),
            command: Command::Interrupt,
            trace: None,
        };
        let v = n.to_value();
        assert_eq!(v["wasit"], "command");
        assert!(v.get("trace").is_none());
        assert_eq!(classify(&v), NoteKind::Runtime(n));
        // With a trace it is journaled and read back; a note of a build without traces reads
        // as none.
        let traced = RuntimeNote::Command {
            token: "t".into(),
            command: Command::Interrupt,
            trace: Some(TraceId::new("u-0123456789abcdef").unwrap()),
        };
        let v = traced.to_value();
        assert_eq!(v["trace"], "u-0123456789abcdef");
        assert_eq!(classify(&v), NoteKind::Runtime(traced));
        let old =
            serde_json::json!({"wasit":"command","token":"t","command":{"command":"interrupt"}});
        assert!(matches!(
            classify(&old),
            NoteKind::Runtime(RuntimeNote::Command { trace: None, .. })
        ));
        // A gate resolution journaled before it could say why reads as plainly withdrawn; one
        // that says why round-trips with its detail.
        let old_resolve = serde_json::json!({"wasit":"resolve_gate","token":"t","request_id":"r"});
        assert_eq!(
            classify(&old_resolve),
            NoteKind::Runtime(RuntimeNote::ResolveGate {
                token: "t".into(),
                request_id: "r".into(),
                reason: GateReason::new("withdrawn"),
            })
        );
        let resolve = RuntimeNote::ResolveGate {
            token: "t".into(),
            request_id: "r".into(),
            reason: GateReason::new("denied").with_detail("not now"),
        };
        assert_eq!(classify(&resolve.to_value()), NoteKind::Runtime(resolve));
        // A raise journaled before bindings existed has none and belongs to the session; one
        // with a binding round-trips it, and one without writes no key.
        let old_raise = serde_json::json!({"wasit":"raise_gate","token":"t","gate":
            {"gate_type":"question","request_id":"r","questions":[]}});
        assert!(matches!(
            classify(&old_raise),
            NoteKind::Runtime(RuntimeNote::RaiseGate { binding: None, .. })
        ));
        let raise = |binding| RuntimeNote::RaiseGate {
            token: "t".into(),
            gate: Box::new(GateRequest::Question {
                request_id: "r".into(),
                questions: vec![],
            }),
            binding,
        };
        assert!(raise(None).to_value().get("binding").is_none());
        for binding in [
            TurnBinding::Interrupt { detail: None },
            TurnBinding::Turn {
                interrupted: Some("i".into()),
                ended: None,
            },
        ] {
            let n = raise(Some(binding));
            assert_eq!(classify(&n.to_value()), NoteKind::Runtime(n));
        }
        let old_start = serde_json::json!({"wasit":"start","spec":SpawnSpec::default()});
        assert!(matches!(
            classify(&old_start),
            NoteKind::Runtime(RuntimeNote::Start {
                trace: None,
                earlier: None,
                ..
            })
        ));
        // Earlier history: the plan in the first start note, and the notes of an import.
        let planned = RuntimeNote::Start {
            spec: Box::new(SpawnSpec::default()),
            trace: None,
            earlier: Some(EarlierPlan::Storage {
                path: "/a/b.jsonl".into(),
                len: 7,
            }),
        };
        let v = planned.to_value();
        assert_eq!(
            v["earlier"],
            serde_json::json!({"source": "storage", "path": "/a/b.jsonl", "len": 7})
        );
        assert_eq!(classify(&v), NoteKind::Runtime(planned));
        let unavailable = EarlierPlan::Unavailable {
            why: EarlierUnavailable::SourceMissing,
        };
        assert_eq!(
            serde_json::to_value(&unavailable).unwrap(),
            serde_json::json!({"source": "unavailable", "why": {"kind": "source_missing"}})
        );
        for n in [
            RuntimeNote::EarlierChunk {
                first: 40,
                events: vec![AgentEvent::UserMessage { text: "q".into() }.earlier(Some(5))],
            },
            RuntimeNote::EarlierEnd {
                report: HistoryReport {
                    messages: 3,
                    ..HistoryReport::default()
                },
            },
            RuntimeNote::EarlierFailed {
                reason: EarlierUnavailable::FormatNotUnderstood { record: 2 },
            },
        ] {
            assert_eq!(classify(&n.to_value()), NoteKind::Runtime(n));
        }
        assert_eq!(
            classify(&serde_json::json!({"stdin": "closed"})),
            NoteKind::StdinClosed
        );
        assert!(matches!(
            classify(&serde_json::json!({"signal": "term", "by": "controller"})),
            NoteKind::Other { code: NoticeCode::SignalSent, message } if message.contains("term")
        ));
        assert!(matches!(
            classify(&serde_json::json!({"wasit": "from_the_future"})),
            NoteKind::Other {
                code: NoticeCode::UnrecognisedNote,
                ..
            }
        ));
        assert_eq!(
            classify(&serde_json::json!({"stdin_error": "write to child stdin: broken pipe"})),
            NoteKind::StdinError("write to child stdin: broken pipe".into())
        );
    }
}
