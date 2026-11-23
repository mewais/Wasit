//! The runtime's own journal annotations (`note` records with a `"wasit"` tag).
//!
//! Replay exactness rests on these: every input that changes protocol state and does not come
//! from the CLI is journaled *before* its effects, and replay re-applies the note instead of
//! trusting the `in` lines that followed it.

use serde::{Deserialize, Serialize};
use wasit_core::{Command, GateRequest, SpawnSpec};

use crate::wire::NoticeCode;

/// A runtime note. Serialized as `{"wasit": "<kind>", …}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "wasit", rename_all = "snake_case")]
pub(crate) enum RuntimeNote {
    /// A controller attached; everything journaled before this note was history to it.
    Attach { token: String },
    /// `Protocol::start(spec)` for the current process generation. A `first_prompt` in the
    /// spec is the session's first input, encoded right after `start` with this note's seq as
    /// its id: journaled in the same record as the start, so neither exists without the other.
    Start { spec: Box<SpawnSpec> },
    /// `Protocol::encode(seq, command)`, where `seq` is this note's journal seq.
    Command { token: String, command: Command },
    /// A graceful stop was requested (`Protocol::encode(Shutdown)`).
    Stop { token: String },
    /// The handshake deadline passed; the process is being killed.
    HandshakeTimeout { after_ms: u64 },
    /// An external gate was raised on the session from outside the CLI protocol (e.g. an
    /// orchestration handoff). It is pending like any CLI gate until answered with
    /// `answer_gate` / `answer_question` (journaled as a `command` note, never sent to the CLI)
    /// or withdrawn with `resolve_gate`.
    RaiseGate {
        token: String,
        gate: Box<GateRequest>,
    },
    /// An external gate was withdrawn by its owner without an answer.
    ResolveGate { token: String, request_id: String },
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
        };
        let v = n.to_value();
        assert_eq!(v["wasit"], "command");
        assert_eq!(classify(&v), NoteKind::Runtime(n));
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
