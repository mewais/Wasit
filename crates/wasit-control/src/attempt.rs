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
use std::time::Instant;

use wasit_diag::Ident;

use crate::command::ChildView;

/// Receives a new stage of an attempt: the stage's words and when it was entered, in
/// milliseconds since the attempt started. The supervisor's publishes it as the link state.
pub(crate) type StageSink = Arc<dyn Fn(&str, u64) + Send + Sync>;

/// What an attempt has reported so far.
#[derive(Default)]
struct Evidence {
    step: Option<String>,
    transport: Option<ChildView>,
}

/// One connect attempt of a link: its number and the place where the connector reports its
/// progress. Cheap to clone; all clones share the report.
#[derive(Clone)]
pub struct Attempt {
    n: u64,
    host: Option<Ident>,
    /// When the attempt started; stages are timed from it.
    started: Instant,
    /// Where a new stage goes when the supervisor runs the attempt; `None` when nothing reads it.
    sink: Option<StageSink>,
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
            host: None,
            started: Instant::now(),
            sink: None,
            evidence: Arc::default(),
        }
    }

    /// The link's attempt number: 1 for the first, +1 per attempt, never reset (not by a
    /// successful connect, not by `reconnect_now`). What a connector records about the attempt
    /// carries it, so it joins the supervisor's records of the same attempt.
    pub fn n(&self) -> u64 {
        self.n
    }

    /// Gives the attempt the host its link leads to (`SupervisedOptions::diag_host`), so a
    /// transport command started for it records the host (see [`crate::CommandSpec::traced`]).
    pub(crate) fn for_host(mut self, host: Option<Ident>) -> Attempt {
        self.host = host;
        self
    }

    /// Makes the attempt report its stages to `sink`, timed from `started` (the supervisor's
    /// start of the attempt).
    pub(crate) fn reporting(mut self, started: Instant, sink: StageSink) -> Attempt {
        self.started = started;
        self.sink = Some(sink);
        self
    }

    /// The host the attempt's link leads to, if it leads to one.
    pub(crate) fn host(&self) -> Option<&Ident> {
        self.host.as_ref()
    }

    /// The connector has reached `step`: a phrase in the connector's own words that completes
    /// "connect timed out while …" ("waiting for the host's report", "sending the helper").
    /// Replaces the previous step. A bridge's progress report arrives as owned text.
    ///
    /// A step different from the current one is a new stage: the supervisor publishes it as the
    /// link state at once (`LinkState::Connecting` with the stage and when it was entered). The
    /// same step again is not a new stage and changes nothing.
    pub fn reached(&self, step: impl Into<String>) {
        let step = step.into();
        // Held across the publication, so two reports of one attempt are published in the order
        // they were made. The supervisor's publication never calls back into the attempt.
        let mut ev = self.evidence();
        if ev.step.as_deref() == Some(step.as_str()) {
            return;
        }
        let since_ms = Instant::now()
            .saturating_duration_since(self.started)
            .as_millis()
            .min(u64::MAX as u128) as u64;
        if let Some(sink) = &self.sink {
            sink(&step, since_ms);
        }
        ev.step = Some(step);
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
        if let Some(step) = &ev.step {
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
