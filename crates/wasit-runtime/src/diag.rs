//! The runtime's own diagnostics (design §12.4): live records of lifecycle facts, never a record
//! per event, per line or per streamed delta (a session's history is its journal and its trail,
//! `crate::trail`). Every record is a [`RuntimeEvent`] and holds ids, states and codes only.
//!
//! Today: the two records of a command's path before it is journaled (design §5.1, legs 6 and
//! 14), so "the prompt never arrived" can be told apart from "it arrived and was refused" and
//! from "it was applied after the requester gave up"; and the runtime's failures and lifecycle
//! facts that used to be free-form log lines. A failure's text comes from another program, a
//! peer or stored data is kept as [`ErrorInfo::foreign`] (its length and fingerprint), an OS
//! error as [`ErrorInfo::io`] (its kind and number).

use std::fmt::Display;
use std::io;
use std::path::Path;

use wasit_control::TraceId;
use wasit_core::RegistryError;
use wasit_diag::{Code, ErrorInfo, Ident};

use crate::core::GateFact;
use crate::wire::{CommandError, EnvSource, SessionId};

wasit_diag::diag_enum! {
    /// Where the host's environment was taken from (mirrors `wire::EnvSource`).
    pub(crate) enum EnvSourceKind {
        /// The login shell in login + interactive mode.
        LoginInteractiveShell,
        /// The login shell in login mode only.
        LoginShell,
        /// The user's and machine's variables from the registry.
        Registry,
        /// The daemon's own environment.
        Process,
    }
}

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
        /// A gate was raised on the session: its `kind` (`permission`, `plan`, `question`,
        /// `handoff` or `spawn`) and the turn binding it asked for (`turn`, `interrupt`, or
        /// `none` for a gate of the CLI's protocol or one that lives on across turns).
        #[diag(ev = "gate.raised", lvl = info)]
        GateRaised {
            session: Ident,
            request_id: Ident,
            kind: Code,
            bound_to: Code,
        },
        /// The gate was answered `ms` milliseconds after it was raised.
        #[diag(ev = "gate.answered", lvl = info)]
        GateAnswered {
            session: Ident,
            request_id: Ident,
            ms: u64,
        },
        /// An external gate was taken down with `reason` (the reason's code, never its detail).
        #[diag(ev = "gate.withdrawn", lvl = info)]
        GateWithdrawn {
            session: Ident,
            request_id: Ident,
            reason: Code,
        },
        /// An answer to a gate was refused and did nothing: `reason` is `invalid_choice`,
        /// `withdrawn`, `not_running`, `unsupported` or `rejected`.
        #[diag(ev = "gate.answer_rejected", lvl = warn)]
        GateAnswerRejected {
            session: Ident,
            request_id: Ident,
            reason: Code,
        },
        /// The host's environment was captured from `source` in `ms` milliseconds.
        #[diag(ev = "env.capture", lvl = info)]
        EnvCapture { source: EnvSourceKind, ms: u64 },
        /// The host's setup commands ran; `ok` is whether they ran to the end.
        #[diag(ev = "env.setup", lvl = info)]
        EnvSetup { ok: bool, ms: u64 },
        /// A temporary file of the host's setup could not be removed.
        #[diag(ev = "env.scratch_remove_failed", lvl = warn)]
        ScratchRemoveFailed { error: ErrorInfo },
        /// A driver could not be registered in the default registry (a build bug).
        #[diag(ev = "runtime.registry_error", lvl = error)]
        RegistryFailed { driver: Ident },
        /// A failed session's directory could not be removed.
        #[diag(ev = "session.cleanup_failed", lvl = warn)]
        CleanupFailed { session: Ident, error: ErrorInfo },
        /// A session directory could not be made private (the adoption goes on).
        #[diag(ev = "session.tighten_failed", lvl = warn)]
        TightenFailed { session: Ident, error: ErrorInfo },
        /// A session directory found at start-up could not be adopted.
        #[diag(ev = "runtime.adopt_failed", lvl = warn)]
        AdoptFailed { session: Ident, error: ErrorInfo },
        /// The snapshot of a finished session could not be rebuilt from its journal.
        #[diag(ev = "session.snapshot_failed", lvl = warn)]
        SnapshotFailed { session: Ident, error: ErrorInfo },
        /// The session's process is given up: its shim could not be reached.
        #[diag(ev = "session.lost", lvl = warn)]
        Lost { session: Ident, error: ErrorInfo },
        /// The shim could not be reached; the runtime tries again (`attempt` counts from 1).
        #[diag(ev = "session.retry", lvl = debug)]
        Retry {
            session: Ident,
            attempt: u32,
            error: ErrorInfo,
        },
        /// The shim's journal stream failed; the session's stream ends here.
        #[diag(ev = "session.stream_failed", lvl = warn)]
        StreamFailed { session: Ident, error: ErrorInfo },
        /// The shim's request queue was full; the oldest request was dropped.
        #[diag(ev = "session.outbox_overflow", lvl = error)]
        OutboxOverflow { session: Ident },
        /// The shim refused a request that does not change the session (an attach, a note, a
        /// close of stdin); `op` names it.
        #[diag(ev = "session.op_refused", lvl = debug)]
        OpRefused {
            session: Ident,
            op: Code,
            error: ErrorInfo,
        },
        /// The shim failed a request that writes to the CLI (`op` is `write` or `signal`; `sig`
        /// is the signal of a `signal`).
        #[diag(ev = "session.op_failed", lvl = warn)]
        OpFailed {
            session: Ident,
            op: Code,
            sig: Option<Code>,
            error: ErrorInfo,
        },
        /// `session.json` could not be written.
        #[diag(ev = "session.persist_failed", lvl = warn)]
        PersistFailed { session: Ident, error: ErrorInfo },
        /// A blob could not be stored.
        #[diag(ev = "blob.write_failed", lvl = warn)]
        BlobWriteFailed { blob: Ident, error: ErrorInfo },
        /// A blob still being written did not finish in time; the reader stopped waiting.
        #[diag(ev = "blob.wait_timeout", lvl = warn)]
        BlobWaitTimeout { blob: Ident },
        /// One driver's part of the inventory was built (a discovery refresh ended): its size as
        /// sent (`bytes`, JSON), the biggest section (`models`, `modes`, `sessions`, `config`,
        /// `probes` or `other`) and its size, how many items each list holds, and how many it
        /// leaves out (`not_listed`). The inventory is one event or reply and a frame has a size
        /// limit: when a peer reports one that was too large, this says which part of which
        /// driver's was big.
        #[diag(ev = "inventory.sized", lvl = info)]
        InventorySized {
            driver: Ident,
            bytes: u64,
            biggest: Code,
            biggest_bytes: u64,
            models: u64,
            modes: u64,
            sessions: u64,
            config: u64,
            not_listed: u64,
        },
        /// A driver's part of the inventory was over its size budget even with every list within
        /// its maximum: `shed` items were left out (counted in `not_listed`) to bring it from
        /// `bytes` to within `budget`.
        #[diag(ev = "inventory.shed", lvl = warn)]
        InventoryShed {
            driver: Ident,
            bytes: u64,
            budget: u64,
            shed: u64,
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

/// An error whose text came from outside this code (a CLI, a shim, stored data).
fn foreign(code: &'static str, e: &impl Display) -> ErrorInfo {
    ErrorInfo::foreign(Code::st(code), &e.to_string())
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

/// A gate changed state: `fact` is what the session's record made of it. Ids, words and times
/// only: the gate's text never reaches a record.
pub(crate) fn gate_fact(id: &SessionId, fact: &GateFact) {
    let event = match fact {
        GateFact::Raised {
            request_id,
            kind,
            bound_to,
        } => RuntimeEvent::GateRaised {
            session: session(id),
            request_id: Ident::new(request_id),
            kind: Code::st(kind),
            bound_to: Code::st(bound_to),
        },
        GateFact::Answered { request_id, ms } => RuntimeEvent::GateAnswered {
            session: session(id),
            request_id: Ident::new(request_id),
            ms: *ms,
        },
        GateFact::Withdrawn { request_id, reason } => RuntimeEvent::GateWithdrawn {
            session: session(id),
            request_id: Ident::new(request_id),
            reason: Code::new(reason),
        },
        GateFact::AnswerRejected { request_id, reason } => RuntimeEvent::GateAnswerRejected {
            session: session(id),
            request_id: Ident::new(request_id),
            reason: Code::st(reason),
        },
    };
    wasit_diag::emit(&event);
}

/// An answer to `request_id` was refused before it could be journaled, for `reason`.
pub(crate) fn answer_rejected(id: &SessionId, request_id: &str, reason: &'static str) {
    gate_fact(
        id,
        &GateFact::AnswerRejected {
            request_id: request_id.to_string(),
            reason,
        },
    );
}

/// The host's environment was captured from `source`.
pub(crate) fn env_capture(source: &EnvSource, ms: u64) {
    let source = match source {
        EnvSource::LoginInteractiveShell => EnvSourceKind::LoginInteractiveShell,
        EnvSource::LoginShell => EnvSourceKind::LoginShell,
        EnvSource::Registry => EnvSourceKind::Registry,
        EnvSource::Process => EnvSourceKind::Process,
    };
    wasit_diag::emit(&RuntimeEvent::EnvCapture { source, ms });
}

/// The host's setup commands ran.
pub(crate) fn env_setup(ok: bool, ms: u64) {
    wasit_diag::emit(&RuntimeEvent::EnvSetup { ok, ms });
}

/// A setup temporary file could not be removed.
#[cfg(unix)]
pub(crate) fn scratch_remove_failed(e: &io::Error) {
    wasit_diag::emit(&RuntimeEvent::ScratchRemoveFailed {
        error: ErrorInfo::io(e),
    });
}

/// `e` is why a driver could not be registered.
pub(crate) fn registry_failed(e: &RegistryError) {
    let (RegistryError::DuplicateDriver(d) | RegistryError::UnknownDriver(d)) = e;
    wasit_diag::emit(&RuntimeEvent::RegistryFailed {
        driver: Ident::new(&d.0),
    });
}

/// A failed session's directory could not be removed.
pub(crate) fn cleanup_failed(id: &SessionId, e: &io::Error) {
    wasit_diag::emit(&RuntimeEvent::CleanupFailed {
        session: session(id),
        error: ErrorInfo::io(e),
    });
}

/// A session directory could not be made private.
pub(crate) fn tighten_failed(id: &SessionId, e: &io::Error) {
    wasit_diag::emit(&RuntimeEvent::TightenFailed {
        session: session(id),
        error: ErrorInfo::io(e),
    });
}

/// A session directory could not be adopted; `e` is the reason the runtime gave.
pub(crate) fn adopt_failed(id: &SessionId, e: &str) {
    wasit_diag::emit(&RuntimeEvent::AdoptFailed {
        session: session(id),
        error: foreign("adopt", &e),
    });
}

/// The snapshot of a finished session could not be rebuilt; `e` is the reason the runtime gave.
pub(crate) fn snapshot_failed(id: &SessionId, e: &str) {
    wasit_diag::emit(&RuntimeEvent::SnapshotFailed {
        session: session(id),
        error: foreign("replay", &e),
    });
}

/// The session's process is given up; `reason` is what the shim connection reported.
pub(crate) fn lost(id: &SessionId, reason: &str) {
    wasit_diag::emit(&RuntimeEvent::Lost {
        session: session(id),
        error: foreign("shim_unreachable", &reason),
    });
}

/// The shim could not be reached on attempt `attempt`; the runtime tries again.
pub(crate) fn retry(id: &SessionId, attempt: u32, reason: &str) {
    wasit_diag::emit(&RuntimeEvent::Retry {
        session: session(id),
        attempt,
        error: foreign("shim_unreachable", &reason),
    });
}

/// The shim's journal stream failed.
pub(crate) fn stream_failed(id: &SessionId, e: &impl Display) {
    wasit_diag::emit(&RuntimeEvent::StreamFailed {
        session: session(id),
        error: foreign("journal_stream", e),
    });
}

/// The shim's request queue overflowed.
pub(crate) fn outbox_overflow(id: &SessionId) {
    wasit_diag::emit(&RuntimeEvent::OutboxOverflow {
        session: session(id),
    });
}

/// The shim refused the request `op` (`attach`, `note` or `close`).
pub(crate) fn op_refused(id: &SessionId, op: &'static str, e: &impl Display) {
    wasit_diag::emit(&RuntimeEvent::OpRefused {
        session: session(id),
        op: Code::st(op),
        error: foreign("shim_refused", e),
    });
}

/// The shim failed the request `op` (`write`, or `signal` with its `sig`).
pub(crate) fn op_failed(
    id: &SessionId,
    op: &'static str,
    sig: Option<&'static str>,
    e: &impl Display,
) {
    wasit_diag::emit(&RuntimeEvent::OpFailed {
        session: session(id),
        op: Code::st(op),
        sig: sig.map(Code::st),
        error: foreign("shim_failed", e),
    });
}

/// `session.json` could not be written.
pub(crate) fn persist_failed(id: &SessionId, e: &io::Error) {
    wasit_diag::emit(&RuntimeEvent::PersistFailed {
        session: session(id),
        error: ErrorInfo::io(e),
    });
}

/// The blob `blob` could not be stored.
pub(crate) fn blob_write_failed(blob: &str, e: &io::Error) {
    wasit_diag::emit(&RuntimeEvent::BlobWriteFailed {
        blob: Ident::new(blob),
        error: ErrorInfo::io(e),
    });
}

/// A reader stopped waiting for the blob at `path` (its file name is the blob's id).
pub(crate) fn blob_wait_timeout(path: &Path) {
    let blob = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    wasit_diag::emit(&RuntimeEvent::BlobWaitTimeout {
        blob: Ident::new(blob),
    });
}

/// One driver's part of the inventory was built; see [`RuntimeEvent::InventorySized`].
pub(crate) fn inventory_sized(
    driver: &str,
    bytes: usize,
    biggest: (&'static str, usize),
    counts: [usize; 4],
    not_listed: wasit_core::NotListed,
) {
    let [models, modes, sessions, config] = counts.map(|n| n as u64);
    wasit_diag::emit(&RuntimeEvent::InventorySized {
        driver: Ident::new(driver),
        bytes: bytes as u64,
        biggest: Code::st(biggest.0),
        biggest_bytes: biggest.1 as u64,
        models,
        modes,
        sessions,
        config,
        not_listed: not_listed.sessions + not_listed.config + not_listed.models + not_listed.modes,
    });
}

/// A driver's part of the inventory had items left out to fit its budget.
pub(crate) fn inventory_shed(driver: &str, bytes: usize, budget: usize, shed: u64) {
    wasit_diag::emit(&RuntimeEvent::InventoryShed {
        driver: Ident::new(driver),
        bytes: bytes as u64,
        budget: budget as u64,
        shed,
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

    fn check(e: RuntimeEvent, ev: &str, lvl: &str, fields: &[&str]) -> serde_json::Value {
        assert_eq!(e.ev(), ev);
        let v = line_of(&e);
        let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        let want: BTreeSet<&str> = HEADER.iter().chain(fields).copied().collect();
        assert_eq!(keys, want, "{v}");
        assert_eq!((v["ev"].as_str(), v["lvl"].as_str()), (Some(ev), Some(lvl)));
        v
    }

    #[test]
    fn the_records_write_the_header_and_their_declared_fields_only() {
        let id = SessionId::random();
        let trace = || Some(TraceId::new("u-0123456789abcdef").unwrap());
        let err = || ErrorInfo::foreign(Code::st("x"), "SECRET");
        let io = io::Error::from(io::ErrorKind::PermissionDenied);
        let v = check(
            RuntimeEvent::CommandRefused {
                session: session(&id),
                trace: trace(),
                kind: Code::st("prompt"),
                code: Code::st("busy"),
            },
            "session.command_refused",
            "warn",
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
            "warn",
            &["session", "kind"],
        );
        assert_eq!(v["kind"], "interrupt");

        let v = check(
            RuntimeEvent::EnvCapture {
                source: EnvSourceKind::LoginShell,
                ms: 12,
            },
            "env.capture",
            "info",
            &["source", "ms"],
        );
        assert_eq!(v["source"], "login_shell");
        check(
            RuntimeEvent::EnvSetup { ok: true, ms: 3 },
            "env.setup",
            "info",
            &["ok", "ms"],
        );
        check(
            RuntimeEvent::ScratchRemoveFailed {
                error: ErrorInfo::io(&io),
            },
            "env.scratch_remove_failed",
            "warn",
            &["error"],
        );
        check(
            RuntimeEvent::RegistryFailed {
                driver: Ident::st("claude"),
            },
            "runtime.registry_error",
            "error",
            &["driver"],
        );
        check(
            RuntimeEvent::CleanupFailed {
                session: session(&id),
                error: ErrorInfo::io(&io),
            },
            "session.cleanup_failed",
            "warn",
            &["session", "error"],
        );
        check(
            RuntimeEvent::TightenFailed {
                session: session(&id),
                error: ErrorInfo::io(&io),
            },
            "session.tighten_failed",
            "warn",
            &["session", "error"],
        );
        check(
            RuntimeEvent::AdoptFailed {
                session: session(&id),
                error: err(),
            },
            "runtime.adopt_failed",
            "warn",
            &["session", "error"],
        );
        check(
            RuntimeEvent::SnapshotFailed {
                session: session(&id),
                error: err(),
            },
            "session.snapshot_failed",
            "warn",
            &["session", "error"],
        );
        check(
            RuntimeEvent::Lost {
                session: session(&id),
                error: err(),
            },
            "session.lost",
            "warn",
            &["session", "error"],
        );
        let v = check(
            RuntimeEvent::Retry {
                session: session(&id),
                attempt: 2,
                error: err(),
            },
            "session.retry",
            "debug",
            &["session", "attempt", "error"],
        );
        assert_eq!(v["attempt"], 2);
        check(
            RuntimeEvent::StreamFailed {
                session: session(&id),
                error: err(),
            },
            "session.stream_failed",
            "warn",
            &["session", "error"],
        );
        check(
            RuntimeEvent::OutboxOverflow {
                session: session(&id),
            },
            "session.outbox_overflow",
            "error",
            &["session"],
        );
        check(
            RuntimeEvent::OpRefused {
                session: session(&id),
                op: Code::st("attach"),
                error: err(),
            },
            "session.op_refused",
            "debug",
            &["session", "op", "error"],
        );
        let v = check(
            RuntimeEvent::OpFailed {
                session: session(&id),
                op: Code::st("signal"),
                sig: Some(Code::st("term")),
                error: err(),
            },
            "session.op_failed",
            "warn",
            &["session", "op", "sig", "error"],
        );
        assert_eq!(v["sig"], "term");
        check(
            RuntimeEvent::PersistFailed {
                session: session(&id),
                error: ErrorInfo::io(&io),
            },
            "session.persist_failed",
            "warn",
            &["session", "error"],
        );
        check(
            RuntimeEvent::BlobWriteFailed {
                blob: Ident::st("b-1"),
                error: ErrorInfo::io(&io),
            },
            "blob.write_failed",
            "warn",
            &["blob", "error"],
        );
        check(
            RuntimeEvent::BlobWaitTimeout {
                blob: Ident::st("b-1"),
            },
            "blob.wait_timeout",
            "warn",
            &["blob"],
        );
        let v = check(
            RuntimeEvent::InventorySized {
                driver: Ident::st("claude"),
                bytes: 90_000,
                biggest: Code::st("sessions"),
                biggest_bytes: 60_000,
                models: 3,
                modes: 6,
                sessions: 200,
                config: 14,
                not_listed: 0,
            },
            "inventory.sized",
            "info",
            &[
                "driver",
                "bytes",
                "biggest",
                "biggest_bytes",
                "models",
                "modes",
                "sessions",
                "config",
                "not_listed",
            ],
        );
        assert_eq!(
            (v["bytes"].as_u64(), v["biggest"].as_str()),
            (Some(90_000), Some("sessions"))
        );
        check(
            RuntimeEvent::InventoryShed {
                driver: Ident::st("claude"),
                bytes: 500_000,
                budget: 196_608,
                shed: 40,
            },
            "inventory.shed",
            "warn",
            &["driver", "bytes", "budget", "shed"],
        );
    }
}
