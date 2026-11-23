//! One connect attempt of a supervised link, as its connector sees it, and what is known of it
//! if the supervisor ends it before it returns.
//!
//! The supervisor bounds an attempt ([`crate::SupervisedConfig::connect_timeout`]) by dropping
//! the connector's future: the connector never gets to say why it was not done. So the
//! connector reports as it goes, to the [`Attempt`] the supervisor handed it: the step it has
//! reached ([`Attempt::reached`]) and, through [`crate::CommandSpec::traced`], the transport
//! command it started. When the bound runs out the supervisor reads that and the link state
//! says where the attempt was and what the transport had said, instead of only "connect timed
//! out". The supervisor owns the fact (it makes the `Attempt` and reads it); the connector only
//! feeds it; nothing waits for anything.

use std::sync::{Arc, Mutex};

use crate::command::ChildView;

/// What an attempt has reported so far.
#[derive(Default)]
struct Evidence {
    step: Option<&'static str>,
    transport: Option<ChildView>,
}

/// One connect attempt of a link: its number and the place where the connector reports its
/// progress. Cheap to clone; all clones share the report.
#[derive(Clone)]
pub struct Attempt {
    n: u64,
    evidence: Arc<Mutex<Evidence>>,
}

impl std::fmt::Debug for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt").field("n", &self.n).finish()
    }
}

/// Attempts are equal when they have the same number (a trace names the attempt by it).
impl PartialEq for Attempt {
    fn eq(&self, other: &Attempt) -> bool {
        self.n == other.n
    }
}

impl Eq for Attempt {}

impl Attempt {
    /// Attempt `n` (1 for the first of a link). The supervisor makes one per attempt; a
    /// connector called on its own (a test) makes its own, and nothing reads its report.
    pub fn new(n: u64) -> Attempt {
        Attempt {
            n,
            evidence: Arc::default(),
        }
    }

    /// The link's attempt number: 1 for the first, +1 per attempt, never reset (not by a
    /// successful connect, not by `reconnect_now`). What a connector records about the attempt
    /// carries it, so it joins the supervisor's records of the same attempt.
    pub fn n(&self) -> u64 {
        self.n
    }

    /// The connector has reached `step`: a fixed phrase in the connector's own words that
    /// completes "connect timed out while …" ("waiting for the host's report", "sending the
    /// helper"). Replaces the previous step.
    pub fn reached(&self, step: &'static str) {
        self.evidence().step = Some(step);
    }

    /// The attempt's transport command is `view` (the latest one, if the connector starts
    /// several). Kept as a view: it does not keep the child alive, so a cancelled attempt's
    /// child is still killed with its future, and what it had written stays readable.
    pub(crate) fn watch(&self, view: ChildView) {
        self.evidence().transport = Some(view);
    }

    /// "connect timed out", with what is known of the attempt: the step it had reached and what
    /// its transport command had said (stderr tail, and its exit if it had ended).
    pub(crate) fn timed_out(&self) -> String {
        let ev = self.evidence();
        let mut text = String::from("connect timed out");
        if let Some(step) = ev.step {
            text.push_str(&format!(" while {step}"));
        }
        if let Some(t) = &ev.transport {
            let mut said = String::new();
            if let Some(exit) = t.exit() {
                said.push_str(&format!("it ended ({exit})"));
            }
            let stderr = t.stderr_tail();
            if !stderr.is_empty() {
                if !said.is_empty() {
                    said.push_str(": ");
                }
                said.push_str(&stderr);
            }
            if !said.is_empty() {
                text.push_str(&format!("; the transport command said: {said}"));
            }
        }
        text
    }

    fn evidence(&self) -> std::sync::MutexGuard<'_, Evidence> {
        self.evidence.lock().unwrap_or_else(|e| e.into_inner())
    }
}
