//! The state of a continue: a new process generation of an ended session, from the request to the
//! moment the journal shows the new process. Owned by the session's actor; the actor does the
//! I/O, this holds who is waiting for what and says what every way out of the state resolves.
//!
//! ```text
//! Idle ──begin──▶ Starting ──attached──▶ Attached ──spawned (journal)──▶ Idle
//!                    │ failed                │ connection lost
//!                    ▼                       ▼
//!                  Idle                    Idle
//! ```
//!
//! While `Starting` (the shim is being spawned and attached to) nobody can journal anything for
//! the session, so a prompt that arrives holds its place here: it is neither journaled nor
//! answered. When the shim is attached the held prompts are journaled in the order they arrived,
//! exactly once; if the shim cannot be reached they are all answered with the failure and
//! nothing was journaled for any of them. `Attached` (the new shim is attached, its `spawned`
//! record is not applied yet) is the window in which the journal already takes commands for the
//! new generation although the core still shows the old one; the requests that asked for the
//! continue itself are answered when the core shows the new generation, so what they see is the
//! snapshot their request produced.

use wasit_core::Command;

use crate::actor::{CommandReply, UnitReply};
use crate::diag::Intent;

/// A command held until the continue it started has attached (its `intent` carries the trace of
/// the request).
pub(crate) struct Held {
    pub command: Command,
    pub reply: CommandReply,
    pub intent: Intent,
}

/// Everybody waiting on a continue.
#[derive(Default)]
pub(crate) struct Waiting {
    /// Requests that asked for the continue itself.
    pub continues: Vec<UnitReply>,
    /// Commands that continue the session as their first step, oldest first.
    pub held: Vec<Held>,
    /// Requests to stop the process that is being started.
    pub stops: Vec<UnitReply>,
}

pub(crate) enum Continuation {
    /// No continue under way.
    Idle,
    /// The new shim is being spawned and attached to.
    Starting(Waiting),
    /// The new shim is attached; the journal will show its `spawned` record next. Holds the
    /// requests that asked for the continue, answered when it does.
    Attached(Vec<UnitReply>),
}

impl Continuation {
    pub(crate) fn is_idle(&self) -> bool {
        matches!(self, Continuation::Idle)
    }

    pub(crate) fn is_starting(&self) -> bool {
        matches!(self, Continuation::Starting(_))
    }

    pub(crate) fn is_attached(&self) -> bool {
        matches!(self, Continuation::Attached(_))
    }

    /// A request for a continue that is attached but not yet in the journal joins it.
    pub(crate) fn join_attached(&mut self, reply: UnitReply) -> Result<(), UnitReply> {
        match self {
            Continuation::Attached(w) => {
                w.push(reply);
                Ok(())
            }
            _ => Err(reply),
        }
    }

    /// A continue starts. Only from `Idle`.
    pub(crate) fn begin(&mut self) {
        if self.is_idle() {
            *self = Continuation::Starting(Waiting::default());
        }
    }

    /// The waiters of a continue that is `Starting` (none otherwise).
    pub(crate) fn waiting(&mut self) -> Option<&mut Waiting> {
        match self {
            Continuation::Starting(w) => Some(w),
            _ => None,
        }
    }

    /// Commands held while `Starting`.
    pub(crate) fn held(&self) -> usize {
        match self {
            Continuation::Starting(w) => w.held.len(),
            _ => 0,
        }
    }

    /// The new shim is attached: the commands and stops that waited are now to be journaled
    /// (`continues` stays with the continue until the journal shows its process: `spawned`).
    pub(crate) fn attached(&mut self) -> Waiting {
        match std::mem::replace(self, Continuation::Idle) {
            Continuation::Starting(mut w) => {
                *self = Continuation::Attached(std::mem::take(&mut w.continues));
                w
            }
            other => {
                *self = other;
                Waiting::default()
            }
        }
    }

    /// The continue failed before the shim was attached: everybody who waited is to be answered
    /// with the failure.
    pub(crate) fn failed(&mut self) -> Waiting {
        match std::mem::replace(self, Continuation::Idle) {
            Continuation::Starting(w) => w,
            other => {
                *self = other;
                Waiting::default()
            }
        }
    }

    /// The journal showed the new generation's `spawned` record: the core has caught up. The
    /// requests that asked for the continue are to be answered `Ok`.
    pub(crate) fn spawned(&mut self) -> Vec<UnitReply> {
        match std::mem::replace(self, Continuation::Idle) {
            Continuation::Attached(w) => w,
            other => {
                *self = other;
                Vec::new()
            }
        }
    }

    /// The connection to the new shim ended before its `spawned` record was applied. `Some`
    /// when that was the `Attached` window (what was journaled into it will not be applied),
    /// with the requests that asked for the continue, which are to be answered with the failure.
    pub(crate) fn connection_lost(&mut self) -> Option<Vec<UnitReply>> {
        match std::mem::replace(self, Continuation::Idle) {
            Continuation::Attached(w) => Some(w),
            other => {
                *self = other;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn unit() -> (
        UnitReply,
        oneshot::Receiver<Result<(), crate::wire::RuntimeError>>,
    ) {
        oneshot::channel()
    }

    fn held(
        text: &str,
    ) -> (
        Held,
        oneshot::Receiver<Result<crate::wire::CommandOutcome, crate::wire::CommandError>>,
    ) {
        let (reply, rx) = oneshot::channel();
        (
            Held {
                command: Command::prompt(text),
                reply,
                intent: Intent::new("prompt", None),
            },
            rx,
        )
    }

    #[test]
    fn a_continue_hands_everybody_over_once_in_order_and_ends_when_the_journal_catches_up() {
        let mut c = Continuation::Idle;
        assert!(c.waiting().is_none());
        c.begin();
        assert!(c.is_starting());
        let (a, _ra) = unit();
        let (h1, _r1) = held("one");
        let (h2, _r2) = held("two");
        let w = c.waiting().unwrap();
        w.continues.push(a);
        w.held.push(h1);
        w.held.push(h2);
        assert_eq!(c.held(), 2);
        let w = c.attached();
        assert!(c.is_attached());
        // The explicit continue waits for the journal; the held commands go to the journal now.
        assert!(w.continues.is_empty());
        let texts: Vec<_> = w
            .held
            .iter()
            .map(|h| match &h.command {
                Command::Prompt { text, .. } => text.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(texts, ["one", "two"]);
        // Nothing is handed over twice.
        assert!(c.attached().held.is_empty());
        assert!(c.failed().held.is_empty());
        assert!(c.is_attached());
        assert_eq!(
            c.spawned().len(),
            1,
            "the explicit continue is answered now"
        );
        assert!(c.is_idle());
        assert!(c.spawned().is_empty());
    }

    #[test]
    fn a_failed_continue_hands_everybody_over_for_an_answer_and_becomes_idle() {
        let mut c = Continuation::Idle;
        c.begin();
        let (h, _r) = held("x");
        c.waiting().unwrap().held.push(h);
        let w = c.failed();
        assert_eq!(w.held.len(), 1);
        assert!(c.is_idle());
        // A new continue starts from scratch.
        c.begin();
        assert_eq!(c.held(), 0);
    }

    #[test]
    fn losing_the_connection_before_the_journal_catches_up_ends_the_window() {
        let mut c = Continuation::Idle;
        assert!(c.connection_lost().is_none());
        c.begin();
        assert!(
            c.connection_lost().is_none(),
            "not attached yet: the failure path owns it"
        );
        assert!(c.is_starting());
        let (a, _ra) = unit();
        c.waiting().unwrap().continues.push(a);
        c.attached();
        // The explicit continue is handed back for an answer.
        assert_eq!(c.connection_lost().unwrap().len(), 1);
        assert!(c.is_idle());
    }
}
