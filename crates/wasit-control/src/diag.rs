//! The control layer's diagnostics (design S13, §12.3): every record is a [`ControlEvent`] and
//! holds ids, states, sizes, timings and reasons, never what travelled over a link.
//!
//! * `link.*` and `transport.*` are the connection trail of one supervised link
//!   ([`crate::supervised`], [`crate::command`]); a link has a label (`SupervisedOptions::diag`,
//!   `hosts:<id>` for the hub's links) and `n`, the attempt number the supervisor hands to the
//!   connector, so a connector's own records join the supervisor's on `(link, n)`. A link
//!   without a label (an in-process loopback) records nothing. A link to a host also carries
//!   that host as `host` (`SupervisedOptions::diag_host`), the field a log is filtered by.
//! * `conn.*` are the accepting side's view of a connection ([`crate::server`]).
//! * `rpc.forward_failed` and `sub.closed` are the router's and the subscription pump's
//!   failures; never a record per request, per event or per frame.
//! * `rpc.traced` is the serving side's record of a request that carries a trace id (a user
//!   intent: a prompt, a launch), one per such request on every connection that serves it (the
//!   hub's from the app, the host's from the hub), and none for any other request.
//!
//! **Free text.** Two kinds exist here and both are scrubbed by `wasit-diag`: the text of a link
//! state or of a closed link (a transport's stderr, the reason a parked link shows), as
//! `DiagText(Transport)`; and the `Display` of this crate's own connect errors, as
//! `ErrorInfo::ours`. What a peer or a parser wrote (a goodbye's message, a malformed frame's
//! serde text) is never passed on: a record names its code, and `ErrorInfo::foreign` keeps only
//! its length and fingerprint. The call sites of `DiagText::new` and `ErrorInfo::ours` are the
//! ones in this file and are pinned in `wasit-diag/tests/value_sites.rs`.
//!
//! **Budgets.** A budget belongs to a variant, not to a link: a link in a reconnect loop can use
//! up its variants' minute (`diag.suppressed` then says how many records were left out) and so
//! hide another link's records of the same kind for the rest of that minute.

use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::Value;
use wasit_diag::{Bytes, Code, DiagText, ErrorInfo, Ident, Ms, Origin};

use crate::client::TransportFacts;
use crate::command::{ExitInfo, TransportTrace};
use crate::error::{ConnClose, ConnectError};
use crate::supervised::{LinkState, ProvisionError, ProvisionReason};
use crate::wire::{CloseReason, HostId, RpcError, TraceId};

wasit_diag::diag_enum! {
    /// What the supervisor decided when an attempt ended.
    pub(crate) enum AttemptOutcome {
        /// The peer answered and is accepted; the link is up.
        Connected,
        /// Failed for a reason that may pass; the next attempt follows the backoff schedule.
        Backoff,
        /// Failed for a reason a timer cannot fix; the link waits for a human.
        Park,
        /// The peer needs its helper installed or updated; provisioning follows.
        Provision,
    }
}

wasit_diag::diag_enum! {
    /// How a provisioning run ended.
    pub(crate) enum ProvisionOutcome {
        /// The provisioner fixed it; the next attempt follows at once.
        Done,
        /// No provisioner is installed (or it declined): nothing was run.
        Unavailable,
        /// It failed; retrying later may help.
        FailedRetryable,
        /// It failed for good.
        Failed,
        /// It did not finish within the provisioning timeout.
        TimedOut,
    }
}

wasit_diag::diag_enum! {
    /// Why a served subscription ended abnormally (a normal end is not recorded).
    pub(crate) enum SubCloseReason {
        /// The subscriber could not keep up.
        Lagged,
        /// The service failed it; see `code`.
        Error,
        /// The host serving the topic (or the link to it) was lost.
        HostLost,
    }
}

// A trace id is `[A-Za-z0-9_-]{8,40}`: an `Ident` by every rule of that type, so it may be a
// field of a record and of a trail entry. Pinned in `wasit-diag/tests/value_sites.rs`.
wasit_diag::diag_value!(TraceId => Ident, |t| Ident::new(t.as_str()));

wasit_diag::diag_enum! {
    /// How a traced request was served.
    pub(crate) enum TracedOutcome {
        /// The service answered with a result.
        Ok,
        /// The service (or the router, or the limiter) answered with an error; see `code`.
        Error,
        /// The connection closed before the service answered.
        Cancelled,
    }
}

wasit_diag::diag_events! {
    /// Events of the control layer.
    pub(crate) enum ControlEvent {
        /// A supervised link began connecting; `n` counts attempts of this link, never reset.
        #[diag(ev = "link.attempt_begin", budget = 300)]
        AttemptBegin {
            link: Ident,
            host: Option<Ident>,
            n: u64,
        },
        /// An attempt ended after `ms`. `error` is the connect error (its code; text only for
        /// our own fixed messages).
        #[diag(ev = "link.attempt_end", budget = 300)]
        AttemptEnd {
            link: Ident,
            host: Option<Ident>,
            n: u64,
            outcome: AttemptOutcome,
            error: Option<ErrorInfo>,
            ms: Ms,
        },
        /// The link changed state; `n` is the attempt in progress or just ended (absent before
        /// the first attempt). `error` is the text that goes with the new state (a last error, a
        /// parked reason, why provisioning is needed).
        #[diag(ev = "link.state", budget = 600)]
        State {
            link: Ident,
            host: Option<Ident>,
            n: Option<u64>,
            from: Code,
            to: Code,
            attempt: Option<u32>,
            error: Option<DiagText>,
        },
        /// A provisioning run: why it was needed, how it ended and how long it took.
        #[diag(ev = "link.provision", budget = 120)]
        Provision {
            link: Ident,
            host: Option<Ident>,
            reason: Code,
            outcome: ProvisionOutcome,
            ms: Ms,
            error: Option<ErrorInfo>,
        },
        /// A connected link ended after `connected_ms`; `n` is the attempt that connected it.
        /// `transport_exit` and `stderr` are the transport child's exit code and stderr tail
        /// when the link had one and it had something to say (not known for a close this side
        /// asked for).
        #[diag(ev = "link.closed", lvl = warn, budget = 300)]
        LinkClosed {
            link: Ident,
            host: Option<Ident>,
            n: Option<u64>,
            reason: Code,
            connected_ms: Ms,
            transport_exit: Option<i32>,
            stderr: Option<DiagText>,
        },
        /// A transport command was started: its program's file name and its `argc` (program
        /// included), never an argument.
        #[diag(ev = "transport.spawned", budget = 300)]
        TransportSpawned {
            link: Ident,
            host: Option<Ident>,
            n: u64,
            program: Code,
            argc: u32,
            pid: Option<u32>,
        },
        /// A transport command exited; `stderr_bytes` is all it wrote to stderr.
        #[diag(ev = "transport.exited", budget = 300)]
        TransportExited {
            link: Ident,
            host: Option<Ident>,
            n: u64,
            code: Option<i32>,
            signal: Option<i32>,
            stderr_bytes: Bytes,
        },
        /// A peer completed the handshake. `role` is the peer's.
        #[diag(ev = "conn.accepted", budget = 300)]
        ConnAccepted {
            role: Code,
            peer_build: Ident,
            peer_host: Ident,
            machine: Option<Ident>,
        },
        /// An incoming connection failed its handshake (it never became a connection).
        #[diag(ev = "conn.handshake_failed", lvl = warn, budget = 120)]
        HandshakeFailed { error: ErrorInfo },
        /// A served connection closed after `ms`; `role` is the peer's.
        #[diag(ev = "conn.closed", budget = 300)]
        ConnClosed { role: Code, reason: Code, ms: Ms },
        /// A request or subscription for another host could not be forwarded, or the link
        /// failed under it; the remote service answering with an error is not a failure here.
        /// `method` is a subscription's topic prefix as `subscribe:<prefix>`.
        #[diag(ev = "rpc.forward_failed", lvl = warn, budget = 120)]
        ForwardFailed {
            method: Code,
            host: Ident,
            code: Code,
            ms: Ms,
        },
        /// A request that carries a trace id was served on this connection: `host` is the
        /// frame's target (absent when this side serves it itself), `hops` the router hops it
        /// had taken, `ms` the time from its arrival to the service's answer (a forwarded
        /// request includes the wait for the next hop), `code` the error code of an error.
        #[diag(ev = "rpc.traced", budget = 600)]
        RpcTraced {
            trace: TraceId,
            method: Code,
            host: Option<Ident>,
            hops: u32,
            ms: Ms,
            outcome: TracedOutcome,
            code: Option<Code>,
        },
        /// A served subscription ended abnormally. `topic` is the part before `:`; `code` is
        /// the service's error code with `error`.
        #[diag(ev = "sub.closed", lvl = warn, budget = 120)]
        SubClosed {
            topic: Code,
            host: Option<Ident>,
            reason: SubCloseReason,
            code: Option<Code>,
        },
    }
}

fn emit(event: ControlEvent) {
    wasit_diag::emit(&event);
}

fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// The label of a host's link on the hub (`hosts:<id>`).
pub(crate) fn host_link(host: &HostId) -> Ident {
    Ident::new(&format!("hosts:{host}"))
}

// ── the connection trail ────────────────────────────────────────────────────

/// The attempt number of a record: `None` before the link's first attempt (`n` is 0 then).
fn attempted(n: u64) -> Option<u64> {
    (n > 0).then_some(n)
}

pub(crate) fn attempt_begin(link: &Ident, host: Option<&Ident>, n: u64) {
    emit(ControlEvent::AttemptBegin {
        link: link.clone(),
        host: host.cloned(),
        n,
    });
}

pub(crate) fn attempt_end(
    link: &Ident,
    host: Option<&Ident>,
    n: u64,
    outcome: AttemptOutcome,
    error: Option<ErrorInfo>,
    started: Instant,
) {
    emit(ControlEvent::AttemptEnd {
        link: link.clone(),
        host: host.cloned(),
        n,
        outcome,
        error,
        ms: Ms::of(started.elapsed()),
    });
}

/// A link state change. `n` is the link's attempt number (0 before its first attempt).
pub(crate) fn state(
    link: &Ident,
    host: Option<&Ident>,
    n: u64,
    from: &LinkState,
    to: &LinkState,
    why: Option<&str>,
) {
    let text = why.map(str::to_owned).or_else(|| state_reason(to));
    emit(ControlEvent::State {
        link: link.clone(),
        host: host.cloned(),
        n: attempted(n),
        from: Code::st(from.code()),
        to: Code::st(to.code()),
        attempt: match to {
            LinkState::Reconnecting { attempt, .. } => Some(*attempt),
            _ => None,
        },
        error: text.map(|t| DiagText::new(Origin::Transport, &t)),
    });
}

pub(crate) fn provision(
    link: &Ident,
    host: Option<&Ident>,
    reason: &ProvisionReason,
    outcome: ProvisionOutcome,
    error: Option<&ProvisionError>,
    started: Instant,
) {
    emit(ControlEvent::Provision {
        link: link.clone(),
        host: host.cloned(),
        reason: Code::st(reason.code()),
        outcome,
        ms: Ms::of(started.elapsed()),
        // A provisioner's message is made of what the host answered: only its shape is kept
        // here; the link state that follows carries the (scrubbed) text.
        error: error.map(|e| ErrorInfo::foreign(Code::st("provision_failed"), &e.message)),
    });
}

/// A connected link ended; `n` is the attempt that connected it.
pub(crate) fn link_closed(
    link: &Ident,
    host: Option<&Ident>,
    n: u64,
    close: &ConnClose,
    connected: Duration,
    facts: Option<&TransportFacts>,
) {
    emit(ControlEvent::LinkClosed {
        link: link.clone(),
        host: host.cloned(),
        n: attempted(n),
        reason: Code::st(close.code()),
        connected_ms: Ms::of(connected),
        transport_exit: facts.and_then(|f| f.exit).and_then(|e| e.code),
        stderr: facts
            .filter(|f| !f.stderr.is_empty())
            .map(|f| DiagText::new(Origin::Transport, &f.stderr)),
    });
}

pub(crate) fn transport_spawned(
    trace: &TransportTrace,
    program: &str,
    argc: usize,
    pid: Option<u32>,
) {
    let name = Path::new(program)
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    emit(ControlEvent::TransportSpawned {
        link: trace.link.clone(),
        host: trace.host.clone(),
        n: trace.attempt.n(),
        program: Code::new(&name),
        argc: count(argc),
        pid,
    });
}

pub(crate) fn transport_exited(trace: &TransportTrace, exit: &ExitInfo, stderr_bytes: u64) {
    emit(ControlEvent::TransportExited {
        link: trace.link.clone(),
        host: trace.host.clone(),
        n: trace.attempt.n(),
        code: exit.code,
        signal: exit.signal,
        stderr_bytes: Bytes(stderr_bytes),
    });
}

/// The error of a failed connect attempt as a record field. Our own fixed messages keep their
/// text; what a transport's stderr, a peer or a parser wrote keeps only its length and
/// fingerprint (the scrubbed transport text is on the `link.state` record that follows).
pub(crate) fn connect_error(e: &ConnectError) -> ErrorInfo {
    let ours = |code: &'static str| ErrorInfo::ours(Code::st(code), e);
    match e {
        ConnectError::Incompatible { .. } => ours("incompatible"),
        ConnectError::HandshakeTimeout => ours("handshake_timeout"),
        ConnectError::ClosedDuringHandshake => ours("closed_during_handshake"),
        ConnectError::Timeout => ours("timeout"),
        // The OS's reason for a failed read, write or connect, or the program we tried to
        // start: no peer text, no session content.
        ConnectError::Io(_) => ours("io"),
        ConnectError::Spawn(_) => ours("spawn"),
        ConnectError::Refused(r) => {
            ErrorInfo::foreign(Code::new(&format!("refused:{}", r.code)), &r.message)
        }
        ConnectError::Protocol(m) => ErrorInfo::foreign(Code::st("protocol"), m),
        ConnectError::Command { kind, stderr, .. } => {
            ErrorInfo::foreign(Code::st(kind.code()), stderr)
        }
    }
}

/// A failed connect attempt as the pieces of a record, for a connector that records its own
/// failure (the product's bootstrap): the one mapping from a [`ConnectError`] to its diagnostics
/// representation. A failure of the transport command keeps the classifier's word (`kind`), the
/// child's exit status and the text the user is shown (`stderr`, scrubbed and bounded); any
/// other failure its code, and its text where the text is ours (`error`, see [`connect_error`]).
pub struct ConnectDiag {
    pub kind: Option<Code>,
    pub exit: Option<i32>,
    pub stderr: Option<DiagText>,
    pub error: Option<ErrorInfo>,
}

impl ConnectDiag {
    pub fn of(e: &ConnectError) -> ConnectDiag {
        match e {
            ConnectError::Command {
                kind,
                exit_code,
                stderr,
            } => ConnectDiag {
                kind: Some(Code::st(kind.code())),
                exit: *exit_code,
                stderr: Some(DiagText::new(Origin::Transport, stderr)),
                error: None,
            },
            other => ConnectDiag {
                kind: None,
                exit: None,
                stderr: None,
                error: Some(connect_error(other)),
            },
        }
    }
}

/// `e`'s text for a link state: its `Display`, except that what a peer or a parser wrote (a
/// goodbye's message, a malformed frame's serde text) is replaced by its code.
pub(crate) fn connect_why(e: &ConnectError) -> String {
    match e {
        ConnectError::Refused(r) => format!("connection refused by peer: {}", r.code),
        ConnectError::Protocol(_) => "protocol error during handshake".to_owned(),
        other => other.to_string(),
    }
}

/// `close`'s text with the transport's explanation, for a link state: like the link's
/// `last_error`, but a goodbye's message and a malformed frame's text are replaced by their
/// kind.
pub(crate) fn close_why(close: &ConnClose, facts: Option<&TransportFacts>) -> String {
    let what = match close {
        ConnClose::Goodbye { reason } => format!("peer said goodbye: {}", reason.code),
        ConnClose::Protocol { .. } => "protocol error".to_owned(),
        other => other.to_string(),
    };
    match facts {
        Some(f) => format!("{what} ({})", f.describe()),
        None => what,
    }
}

/// The text that goes with a link state, when it has one.
fn state_reason(s: &LinkState) -> Option<String> {
    let with = |problem: Option<crate::CommandFailureKind>, text: &str| match problem {
        Some(p) => format!("{}: {text}", p.code()),
        None => text.to_owned(),
    };
    match s {
        LinkState::Connecting | LinkState::Connected { .. } | LinkState::Stopped => None,
        LinkState::Provisioning { reason } => Some(match reason {
            ProvisionReason::CommandNotFound { detail } => {
                format!("helper not installed: {detail}")
            }
            ProvisionReason::BuildMismatch { expected, found } => {
                format!("build mismatch: expected {expected}, found {found}")
            }
            ProvisionReason::Incompatible { ours, theirs } => {
                format!("incompatible protocol: ours {ours}, theirs {theirs}")
            }
        }),
        LinkState::Reconnecting { last_error, .. } => Some(last_error.clone()),
        LinkState::AuthNeeded {
            problem, detail, ..
        } => Some(with(Some(*problem), detail)),
        LinkState::Incompatible { ours, theirs } => Some(format!(
            "incompatible protocol: ours {ours}, theirs {theirs}"
        )),
        LinkState::Failed { reason, problem } => Some(with(*problem, reason)),
    }
}

// ── the accepting side ──────────────────────────────────────────────────────

pub(crate) fn conn_accepted(hello: &crate::wire::Hello) {
    emit(ControlEvent::ConnAccepted {
        role: Code::new(&hello.role),
        peer_build: Ident::new(&hello.build),
        peer_host: Ident::new(hello.host_id.as_str()),
        machine: hello.machine.as_deref().map(Ident::new),
    });
}

pub(crate) fn handshake_failed(e: &ConnectError) {
    emit(ControlEvent::HandshakeFailed {
        error: connect_error(e),
    });
}

pub(crate) fn conn_closed(hello: &crate::wire::Hello, close: &ConnClose, started: Instant) {
    emit(ControlEvent::ConnClosed {
        role: Code::new(&hello.role),
        reason: Code::st(close.code()),
        ms: Ms::of(started.elapsed()),
    });
}

/// A request that carries a trace id, from its arrival until it is answered.
pub(crate) struct TracedCall {
    trace: TraceId,
    method: Code,
    host: Option<Ident>,
    hops: u32,
    started: Instant,
}

impl TracedCall {
    pub(crate) fn new(trace: &TraceId, method: &str, host: Option<&HostId>, hops: u32) -> Self {
        TracedCall {
            trace: trace.clone(),
            method: Code::new(method),
            host: host.map(|h| Ident::new(h.as_str())),
            hops,
            started: Instant::now(),
        }
    }

    fn emit(self, outcome: TracedOutcome, code: Option<Code>) {
        emit(ControlEvent::RpcTraced {
            trace: self.trace,
            method: self.method,
            host: self.host,
            hops: self.hops,
            ms: Ms::of(self.started.elapsed()),
            outcome,
            code,
        });
    }

    /// The request was answered with `res`. Only the error's code is recorded, never its
    /// message (it may quote a parameter).
    pub(crate) fn done(self, res: &Result<Value, RpcError>) {
        match res {
            Ok(_) => self.emit(TracedOutcome::Ok, None),
            Err(e) => self.emit(TracedOutcome::Error, Some(Code::new(&e.code))),
        }
    }

    /// The connection closed first.
    pub(crate) fn cancelled(self) {
        self.emit(TracedOutcome::Cancelled, None);
    }
}

// ── routing and subscriptions ───────────────────────────────────────────────

/// The topic's prefix (the part before `:`), as a word.
fn topic_code(topic: &str) -> Code {
    Code::new(topic.split(':').next().unwrap_or(topic))
}

pub(crate) fn forward_failed(method: &str, host: &HostId, code: &str, started: Instant) {
    emit(ControlEvent::ForwardFailed {
        method: Code::new(method),
        host: Ident::new(host.as_str()),
        code: Code::new(code),
        ms: Ms::of(started.elapsed()),
    });
}

pub(crate) fn forward_subscribe_failed(topic: &str, host: &HostId, code: &str, started: Instant) {
    let prefix = topic.split(':').next().unwrap_or(topic);
    forward_failed(&format!("subscribe:{prefix}"), host, code, started);
}

/// A served subscription ended with `reason`; a normal end is not recorded.
pub(crate) fn sub_closed(topic: &str, host: Option<&HostId>, reason: &CloseReason) {
    let (why, code) = match reason {
        CloseReason::Ended => return,
        CloseReason::Lagged => (SubCloseReason::Lagged, None),
        CloseReason::HostLost => (SubCloseReason::HostLost, None),
        CloseReason::Error { error } => (SubCloseReason::Error, Some(Code::new(&error.code))),
    };
    emit(ControlEvent::SubClosed {
        topic: topic_code(topic),
        host: host.map(|h| Ident::new(h.as_str())),
        reason: why,
        code,
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use wasit_diag::{DiagEvent, Sink, SinkConfig};

    use super::*;
    use crate::wire::{ProtocolVersion, RpcError};

    const HEADER: [&str; 7] = ["b", "seq", "t", "m", "src", "lvl", "ev"];

    /// The one record `e` makes in a sink of its own, parsed.
    fn line_of(e: &ControlEvent) -> serde_json::Value {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SinkConfig::new(Code::st("hub"), Ident::st("b1"), dir.path(), "e.ndjson");
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

    /// `e` writes exactly the header keys and `fields`, with the declared name and level.
    fn check(e: ControlEvent, ev: &str, lvl: &str, fields: &[&str]) -> serde_json::Value {
        assert_eq!(e.ev(), ev);
        let v = line_of(&e);
        let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        let want: BTreeSet<&str> = HEADER.iter().chain(fields).copied().collect();
        assert_eq!(keys, want, "{v}");
        assert_eq!(v["ev"], ev);
        assert_eq!(v["lvl"], lvl);
        v
    }

    fn link() -> Ident {
        Ident::st("hosts:gpu")
    }

    fn gpu() -> Option<Ident> {
        Some(Ident::st("gpu"))
    }

    #[test]
    fn every_event_writes_the_header_and_its_declared_fields_only() {
        let err = ErrorInfo::foreign(Code::st("io"), "boom");
        let v = check(
            ControlEvent::AttemptBegin {
                link: link(),
                host: gpu(),
                n: 3,
            },
            "link.attempt_begin",
            "info",
            &["link", "host", "n"],
        );
        assert_eq!(v["host"], "gpu");
        let v = check(
            ControlEvent::AttemptEnd {
                link: link(),
                host: gpu(),
                n: 3,
                outcome: AttemptOutcome::Backoff,
                error: Some(err.clone()),
                ms: Ms(12),
            },
            "link.attempt_end",
            "info",
            &["link", "host", "n", "outcome", "error", "ms"],
        );
        assert_eq!(v["outcome"], "backoff");
        // A link that is not to a host writes no `host`.
        check(
            ControlEvent::AttemptEnd {
                link: link(),
                host: None,
                n: 4,
                outcome: AttemptOutcome::Connected,
                error: None,
                ms: Ms(1),
            },
            "link.attempt_end",
            "info",
            &["link", "n", "outcome", "ms"],
        );
        let v = check(
            ControlEvent::State {
                link: link(),
                host: gpu(),
                n: Some(2),
                from: Code::st("connecting"),
                to: Code::st("reconnecting"),
                attempt: Some(2),
                error: Some(DiagText::new(Origin::Transport, "refused")),
            },
            "link.state",
            "info",
            &["link", "host", "n", "from", "to", "attempt", "error"],
        );
        assert_eq!(
            (v["host"].as_str(), v["n"].as_u64()),
            (Some("gpu"), Some(2))
        );
        // Before the first attempt there is no `n`.
        check(
            ControlEvent::State {
                link: link(),
                host: None,
                n: None,
                from: Code::st("reconnecting"),
                to: Code::st("connecting"),
                attempt: None,
                error: None,
            },
            "link.state",
            "info",
            &["link", "from", "to"],
        );
        let v = check(
            ControlEvent::Provision {
                link: link(),
                host: gpu(),
                reason: Code::st("build_mismatch"),
                outcome: ProvisionOutcome::FailedRetryable,
                ms: Ms(5),
                error: Some(err),
            },
            "link.provision",
            "info",
            &["link", "host", "reason", "outcome", "ms", "error"],
        );
        assert_eq!(v["outcome"], "failed_retryable");
        let v = check(
            ControlEvent::LinkClosed {
                link: link(),
                host: gpu(),
                n: Some(1),
                reason: Code::st("eof"),
                connected_ms: Ms(900),
                transport_exit: Some(3),
                stderr: Some(DiagText::new(Origin::Transport, "bye")),
            },
            "link.closed",
            "warn",
            &[
                "link",
                "host",
                "n",
                "reason",
                "connected_ms",
                "transport_exit",
                "stderr",
            ],
        );
        assert_eq!(v["n"], 1);
        check(
            ControlEvent::LinkClosed {
                link: link(),
                host: None,
                n: None,
                reason: Code::st("local"),
                connected_ms: Ms(1),
                transport_exit: None,
                stderr: None,
            },
            "link.closed",
            "warn",
            &["link", "reason", "connected_ms"],
        );
        let v = check(
            ControlEvent::TransportSpawned {
                link: link(),
                host: gpu(),
                n: 1,
                program: Code::st("ssh"),
                argc: 7,
                pid: Some(42),
            },
            "transport.spawned",
            "info",
            &["link", "host", "n", "program", "argc", "pid"],
        );
        assert_eq!(v["host"], "gpu");
        check(
            ControlEvent::TransportExited {
                link: link(),
                host: gpu(),
                n: 1,
                code: Some(255),
                signal: None,
                stderr_bytes: Bytes(120),
            },
            "transport.exited",
            "info",
            &["link", "host", "n", "code", "stderr_bytes"],
        );
        check(
            ControlEvent::ConnAccepted {
                role: Code::st("hub"),
                peer_build: Ident::st("0.4.2+abc"),
                peer_host: Ident::st("laptop"),
                machine: Some(Ident::st("a42b50b8445607f0")),
            },
            "conn.accepted",
            "info",
            &["role", "peer_build", "peer_host", "machine"],
        );
        check(
            ControlEvent::HandshakeFailed {
                error: ErrorInfo::ours(Code::st("handshake_timeout"), &"handshake timed out"),
            },
            "conn.handshake_failed",
            "warn",
            &["error"],
        );
        check(
            ControlEvent::ConnClosed {
                role: Code::st("hub"),
                reason: Code::st("eof"),
                ms: Ms(77),
            },
            "conn.closed",
            "info",
            &["role", "reason", "ms"],
        );
        check(
            ControlEvent::ForwardFailed {
                method: Code::st("session.command"),
                host: Ident::st("gpu"),
                code: Code::st("host_unreachable"),
                ms: Ms(0),
            },
            "rpc.forward_failed",
            "warn",
            &["method", "host", "code", "ms"],
        );
        let v = check(
            ControlEvent::RpcTraced {
                trace: TraceId::new("u-0123456789abcdef").unwrap(),
                method: Code::st("session.command"),
                host: Some(Ident::st("gpu")),
                hops: 1,
                ms: Ms(8),
                outcome: TracedOutcome::Error,
                code: Some(Code::st("busy")),
            },
            "rpc.traced",
            "info",
            &["trace", "method", "host", "hops", "ms", "outcome", "code"],
        );
        assert_eq!(
            (v["trace"].as_str(), v["outcome"].as_str()),
            (Some("u-0123456789abcdef"), Some("error"))
        );
        check(
            ControlEvent::RpcTraced {
                trace: TraceId::new("u-0123456789abcdef").unwrap(),
                method: Code::st("whoami"),
                host: None,
                hops: 0,
                ms: Ms(1),
                outcome: TracedOutcome::Ok,
                code: None,
            },
            "rpc.traced",
            "info",
            &["trace", "method", "hops", "ms", "outcome"],
        );
        let v = check(
            ControlEvent::SubClosed {
                topic: Code::st("session.events"),
                host: Some(Ident::st("gpu")),
                reason: SubCloseReason::HostLost,
                code: None,
            },
            "sub.closed",
            "warn",
            &["topic", "host", "reason"],
        );
        assert_eq!(v["reason"], "host_lost");
        check(
            ControlEvent::SubClosed {
                topic: Code::st("session.events"),
                host: None,
                reason: SubCloseReason::Error,
                code: Some(Code::st("internal")),
            },
            "sub.closed",
            "warn",
            &["topic", "reason", "code"],
        );
    }

    #[test]
    fn state_and_close_codes_are_the_serialized_words() {
        let states = [
            LinkState::Connecting,
            LinkState::Provisioning {
                reason: ProvisionReason::Incompatible {
                    ours: ProtocolVersion { major: 1, minor: 0 },
                    theirs: ProtocolVersion { major: 2, minor: 0 },
                },
            },
            LinkState::Connected {
                hello: crate::wire::Hello::new("b", "h", "r"),
            },
            LinkState::Reconnecting {
                attempt: 1,
                next_at: std::time::SystemTime::UNIX_EPOCH,
                last_error: String::new(),
            },
            LinkState::AuthNeeded {
                problem: crate::CommandFailureKind::AuthNeeded,
                detail: String::new(),
                cause: None,
                held: false,
            },
            LinkState::Incompatible {
                ours: ProtocolVersion { major: 1, minor: 0 },
                theirs: ProtocolVersion { major: 2, minor: 0 },
            },
            LinkState::Failed {
                reason: String::new(),
                problem: None,
            },
            LinkState::Stopped,
        ];
        for s in &states {
            assert_eq!(serde_json::to_value(s).unwrap()["state"], s.code());
        }
        for r in [
            ProvisionReason::CommandNotFound {
                detail: String::new(),
            },
            ProvisionReason::BuildMismatch {
                expected: String::new(),
                found: String::new(),
            },
            ProvisionReason::Incompatible {
                ours: ProtocolVersion { major: 1, minor: 0 },
                theirs: ProtocolVersion { major: 2, minor: 0 },
            },
        ] {
            assert_eq!(serde_json::to_value(&r).unwrap()["kind"], r.code());
        }
        for c in [
            ConnClose::Eof,
            ConnClose::Goodbye {
                reason: RpcError::new("x", "y"),
            },
            ConnClose::Io {
                message: String::new(),
            },
            ConnClose::Protocol {
                message: String::new(),
            },
            ConnClose::FrameTooLarge,
            ConnClose::IdleTimeout,
            ConnClose::Local,
        ] {
            assert_eq!(serde_json::to_value(&c).unwrap()["kind"], c.code());
        }
        use crate::CommandFailureKind as K;
        for k in [
            K::AuthNeeded,
            K::HostKeyUnknown,
            K::HostKeyChanged,
            K::Unreachable,
            K::CommandNotFound,
            K::UnknownDistro,
            K::WslUnavailable,
            K::PermissionDenied,
            K::UnsupportedPlatform,
            K::HelperMissing,
            K::UnexpectedOutput,
            K::HelperFailed,
            K::Other,
        ] {
            assert_eq!(serde_json::to_value(k).unwrap(), k.code());
        }
    }

    #[test]
    fn what_a_peer_or_parser_wrote_never_reaches_a_state_text() {
        let goodbye = ConnClose::Goodbye {
            reason: RpcError::new("shutting_down", "SECRET-PEER-TEXT"),
        };
        let proto = ConnClose::Protocol {
            message: "invalid type: string \"SECRET-FRAME-TEXT\"".into(),
        };
        assert_eq!(
            close_why(&goodbye, None),
            "peer said goodbye: shutting_down"
        );
        assert_eq!(close_why(&proto, None), "protocol error");
        let refused =
            ConnectError::Refused(RpcError::new("bridge_unavailable", "SECRET-PEER-TEXT"));
        assert_eq!(
            connect_why(&refused),
            "connection refused by peer: bridge_unavailable"
        );
        assert_eq!(
            connect_why(&ConnectError::Protocol("SECRET-FRAME-TEXT".into())),
            "protocol error during handshake"
        );
        for e in [
            refused,
            ConnectError::Protocol("SECRET-FRAME-TEXT".into()),
            ConnectError::Command {
                kind: crate::CommandFailureKind::Other,
                exit_code: Some(1),
                stderr: "SECRET-STDERR".into(),
            },
        ] {
            let v = line_of(&ControlEvent::AttemptEnd {
                link: link(),
                host: None,
                n: 1,
                outcome: AttemptOutcome::Backoff,
                error: Some(connect_error(&e)),
                ms: Ms(1),
            });
            assert!(!v.to_string().contains("SECRET"), "{v}");
        }
    }

    /// The one mapping from a connect error to the pieces of a record: a failure of the
    /// transport command as kind, exit and (scrubbed) text, every other failure as its error.
    #[test]
    fn a_connect_error_maps_to_its_record_pieces_in_one_place() {
        let command = ConnectError::Command {
            kind: crate::CommandFailureKind::HelperFailed,
            exit_code: Some(2),
            stderr: "helper said no".into(),
        };
        let d = ConnectDiag::of(&command);
        assert_eq!(d.kind, Some(Code::st("helper_failed")));
        assert_eq!(d.exit, Some(2));
        assert_eq!(d.stderr.as_ref().unwrap().as_str(), "helper said no");
        assert!(d.error.is_none());
        for e in [
            ConnectError::Incompatible {
                ours: ProtocolVersion { major: 1, minor: 0 },
                theirs: ProtocolVersion { major: 2, minor: 0 },
            },
            ConnectError::Refused(RpcError::new("shutting_down", "SECRET-PEER-TEXT")),
            ConnectError::HandshakeTimeout,
            ConnectError::ClosedDuringHandshake,
            ConnectError::Protocol("SECRET-FRAME-TEXT".into()),
            ConnectError::Io("broken pipe".into()),
            ConnectError::Spawn("ssh: no such file".into()),
            ConnectError::Timeout,
        ] {
            let d = ConnectDiag::of(&e);
            assert!(d.kind.is_none() && d.exit.is_none() && d.stderr.is_none());
            let record = |error: Option<ErrorInfo>| {
                line_of(&ControlEvent::AttemptEnd {
                    link: link(),
                    host: None,
                    n: 1,
                    outcome: AttemptOutcome::Backoff,
                    error,
                    ms: Ms(1),
                })["error"]
                    .clone()
            };
            assert_eq!(record(d.error), record(Some(connect_error(&e))), "{e:?}");
        }
    }
}
