//! The runtime's own diagnostics (design §12.4): live records of lifecycle facts, never a record
//! per event, per line or per streamed delta (a session's history is its journal and its trail,
//! `crate::trail`). Every record is a [`RuntimeEvent`] and holds ids, states and codes only.
//!
//! Today: the two records of a command's path before it is journaled (design §5.1, legs 6 and
//! 14), so "the prompt never arrived" can be told apart from "it arrived and was refused" and
//! from "it was applied after the requester gave up". The rest of the runtime's events join
//! this enum in a later step.

use wasit_control::TraceId;
use wasit_diag::{Code, Ident};

use crate::wire::{CommandError, SessionId};

wasit_diag::diag_events! {
    /// Events of the session runtime.
    pub(crate) enum RuntimeEvent {
        /// A command was refused before it was journaled (the session could not take it); `code`
        /// is the error the requester got. A command the session journaled and then rejected is
        /// on the session's trail instead (`input.applied` with its status).
        #[diag(ev = "session.command_refused", lvl = warn)]
        CommandRefused {
            session: Ident,
            trace: Option<TraceId>,
            kind: Code,
            code: Code,
        },
        /// The requester stopped waiting for a command that had not been journaled yet. It may
        /// still be applied later: the trail shows what became of it.
        #[diag(ev = "session.command_timeout", lvl = warn)]
        CommandTimeout {
            session: Ident,
            trace: Option<TraceId>,
            kind: Code,
        },
    }
}

/// What a command-like request is, for the records about it: its name and the trace of the
/// request that carried it.
#[derive(Debug, Clone)]
pub(crate) struct Intent {
    pub kind: &'static str,
    pub trace: Option<TraceId>,
}

impl Intent {
    pub(crate) fn new(kind: &'static str, trace: Option<TraceId>) -> Self {
        Intent { kind, trace }
    }
}

fn session(id: &SessionId) -> Ident {
    Ident::new(id.as_str())
}

/// `intent` was refused with `err` before it was journaled.
pub(crate) fn command_refused(id: &SessionId, intent: &Intent, err: &CommandError) {
    wasit_diag::emit(&RuntimeEvent::CommandRefused {
        session: session(id),
        trace: intent.trace.clone(),
        kind: Code::new(intent.kind),
        code: Code::new(err.code()),
    });
}

/// The requester of `intent` stopped waiting.
pub(crate) fn command_timeout(id: &SessionId, intent: &Intent) {
    wasit_diag::emit(&RuntimeEvent::CommandTimeout {
        session: session(id),
        trace: intent.trace.clone(),
        kind: Code::new(intent.kind),
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use wasit_diag::{DiagEvent, Sink, SinkConfig};

    use super::*;

    const HEADER: [&str; 7] = ["b", "seq", "t", "m", "src", "lvl", "ev"];

    fn line_of(e: &RuntimeEvent) -> serde_json::Value {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SinkConfig::new(Code::st("daemon"), Ident::st("b1"), dir.path(), "e.ndjson");
        let sink = Sink::start(cfg).unwrap();
        sink.emit(e);
        assert!(sink.shutdown(Duration::from_secs(30)));
        let text = std::fs::read_to_string(dir.path().join("e.ndjson")).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2, "header and the record: {text}");
        lines[1].clone()
    }

    fn check(e: RuntimeEvent, ev: &str, fields: &[&str]) -> serde_json::Value {
        assert_eq!(e.ev(), ev);
        let v = line_of(&e);
        let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        let want: BTreeSet<&str> = HEADER.iter().chain(fields).copied().collect();
        assert_eq!(keys, want, "{v}");
        assert_eq!(
            (v["ev"].as_str(), v["lvl"].as_str()),
            (Some(ev), Some("warn"))
        );
        v
    }

    #[test]
    fn the_records_write_the_header_and_their_declared_fields_only() {
        let id = SessionId::random();
        let v = check(
            RuntimeEvent::CommandRefused {
                session: session(&id),
                trace: Some(TraceId::new("u-0123456789abcdef").unwrap()),
                kind: Code::st("prompt"),
                code: Code::st("busy"),
            },
            "session.command_refused",
            &["session", "trace", "kind", "code"],
        );
        assert_eq!(v["trace"], "u-0123456789abcdef");
        let v = check(
            RuntimeEvent::CommandTimeout {
                session: session(&id),
                trace: None,
                kind: Code::st("interrupt"),
            },
            "session.command_timeout",
            &["session", "kind"],
        );
        assert_eq!(v["kind"], "interrupt");
    }
}
