//! The runtime's own journal annotations (`note` records with a `"wasit"` tag).
//!
//! Replay exactness rests on these: every input that changes protocol state and does not come
//! from the CLI is journaled *before* its effects, and replay re-applies the note instead of
//! trusting the `in` lines that followed it.

use serde::{Deserialize, Serialize};
use wasit_core::{Command, GateRequest, SpawnSpec};

/// A runtime note. Serialized as `{"wasit": "<kind>", …}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "wasit", rename_all = "snake_case")]
pub(crate) enum RuntimeNote {
    /// A controller attached; everything journaled before this note was history to it.
    Attach { token: String },
    /// `Protocol::start(spec)` for the current process generation.
    Start { spec: Box<SpawnSpec> },
    /// `Protocol::encode(command)`.
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
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NoteKind {
    Runtime(RuntimeNote),
    /// Shim: stdin was closed.
    StdinClosed,
    /// Any other annotation (signal sent, shim diagnostics, unknown runtime note versions).
    Other(String),
}

pub(crate) fn classify(data: &serde_json::Value) -> NoteKind {
    if data.get("wasit").is_some() {
        return match serde_json::from_value::<RuntimeNote>(data.clone()) {
            Ok(n) => NoteKind::Runtime(n),
            Err(e) => NoteKind::Other(format!("unrecognised runtime note ({e}): {data}")),
        };
    }
    if data.get("stdin").and_then(|v| v.as_str()) == Some("closed") {
        return NoteKind::StdinClosed;
    }
    if let Some(sig) = data.get("signal").and_then(|v| v.as_str()) {
        return NoteKind::Other(format!("signal {sig} sent to the process"));
    }
    if let Some(msg) = data.get("shim").and_then(|v| v.as_str()) {
        return NoteKind::Other(format!("shim: {msg}"));
    }
    NoteKind::Other(data.to_string())
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
            NoteKind::Other(m) if m.contains("term")
        ));
        assert!(matches!(
            classify(&serde_json::json!({"wasit": "from_the_future"})),
            NoteKind::Other(_)
        ));
    }
}
