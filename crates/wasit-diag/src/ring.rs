//! The in-memory ring of recent records and their filter keys (for live tails, design §6.4).

use std::collections::VecDeque;
use std::sync::Arc;

/// Records the ring keeps.
pub const RING: usize = 4096;

/// One serialized record and the keys a tail filters on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The process's boot id (`b`).
    pub boot: Arc<str>,
    pub seq: u64,
    pub ev: &'static str,
    /// The `host` / `session` / `trace` field, when the event has one.
    pub host: Option<Box<str>>,
    pub session: Option<Box<str>>,
    pub trace: Option<Box<str>>,
    /// The whole line, without the trailing newline.
    pub line: Arc<str>,
}

/// What a tail subscriber wants; empty matches everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    pub host: Option<String>,
    pub session: Option<String>,
    /// Any of these trace ids (at most 64 are honoured).
    pub traces: Vec<String>,
    pub ev_prefix: Option<String>,
}

impl Filter {
    pub fn matches(&self, r: &Record) -> bool {
        let eq = |want: &Option<String>, have: &Option<Box<str>>| {
            want.as_deref().is_none_or(|w| have.as_deref() == Some(w))
        };
        eq(&self.host, &r.host)
            && eq(&self.session, &r.session)
            && (self.traces.is_empty()
                || r.trace
                    .as_deref()
                    .is_some_and(|t| self.traces.iter().take(64).any(|w| w == t)))
            && self
                .ev_prefix
                .as_deref()
                .is_none_or(|p| r.ev.starts_with(p))
    }
}

pub(crate) struct Ring {
    records: VecDeque<Arc<Record>>,
}

impl Ring {
    pub(crate) fn new() -> Self {
        Ring {
            records: VecDeque::with_capacity(RING),
        }
    }

    pub(crate) fn push(&mut self, r: Arc<Record>) {
        if self.records.len() == RING {
            self.records.pop_front();
        }
        self.records.push_back(r);
    }

    pub(crate) fn snapshot(&self) -> Vec<Arc<Record>> {
        self.records.iter().cloned().collect()
    }
}
