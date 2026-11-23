//! What [`diag_events!`](crate::diag_events) implements: [`DiagEvent`], the field writer and the
//! per-variant rate budget (design §6.3).

use std::ops::Range;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::json::Json;
use crate::value::DiagValue;

/// A record's level, declared per variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

/// Seal of [`DiagEvent`]: implemented only by [`diag_events!`](crate::diag_events).
pub trait SealedEvent {}

/// An event enum declared with [`diag_events!`](crate::diag_events).
pub trait DiagEvent: SealedEvent {
    /// The dotted event name (`ev`).
    fn ev(&self) -> &'static str;
    fn level(&self) -> Level;
    #[doc(hidden)]
    fn __budget(&self) -> &'static Budget;
    #[doc(hidden)]
    fn __fields(&self, out: &mut Fields);
}

/// Records of one variant admitted per minute unless the declaration says otherwise.
pub const DEFAULT_BUDGET: u32 = 600;

/// The fields of one record, written as `,"key":value…` (the header comes first in the line).
pub struct Fields {
    pub(crate) out: Json,
    pub(crate) host: Option<Range<usize>>,
    pub(crate) session: Option<Range<usize>>,
    pub(crate) trace: Option<Range<usize>>,
}

impl Fields {
    pub(crate) fn new() -> Self {
        Fields {
            out: Json::new(),
            host: None,
            session: None,
            trace: None,
        }
    }

    /// Writes one field; an absent optional value leaves the field out.
    #[doc(hidden)]
    pub fn field<V: DiagValue + ?Sized>(&mut self, key: &'static str, v: &V) {
        if !v.__present() {
            return;
        }
        let key = key.strip_prefix("r#").unwrap_or(key);
        self.out.raw(",\"");
        self.out.raw(key);
        self.out.raw("\":");
        let start = self.out.buf.len();
        v.__write(&mut self.out);
        let end = self.out.buf.len();
        // The ring's filter keys: a string value without its quotes.
        let span =
            (end > start + 1 && self.out.buf.as_bytes()[start] == b'"').then(|| start + 1..end - 1);
        match key {
            "host" => self.host = span,
            "session" => self.session = span,
            "trace" => self.trace = span,
            _ => {}
        }
    }

    pub(crate) fn text(&self) -> &str {
        &self.out.buf
    }

    pub(crate) fn key(&self, r: &Option<Range<usize>>) -> Option<Box<str>> {
        r.as_ref().map(|r| Box::from(&self.out.buf[r.clone()]))
    }
}

/// The per-variant rate budget: one static per variant, made by the macro.
pub struct Budget {
    ev: &'static str,
    limit: u32,
    minute: AtomicU64,
    count: AtomicU32,
    suppressed: AtomicU64,
}

/// What [`Budget::admit`] decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Admit {
    Pass,
    /// Suppressed, and the first suppression since the last summary: the budget must be put on
    /// the sink's list of pending summaries.
    SuppressedFirst,
    Suppressed,
}

impl Budget {
    pub const fn new(ev: &'static str, limit: u32) -> Budget {
        Budget {
            ev,
            limit,
            minute: AtomicU64::new(u64::MAX),
            count: AtomicU32::new(0),
            suppressed: AtomicU64::new(0),
        }
    }

    pub(crate) fn ev(&self) -> &'static str {
        self.ev
    }

    /// Counts one record in `minute`. Concurrent emitters at a minute boundary may let a few
    /// records more through; never fewer.
    pub(crate) fn admit(&self, minute: u64) -> Admit {
        if self.minute.load(Ordering::Relaxed) != minute
            && self.minute.swap(minute, Ordering::AcqRel) != minute
        {
            self.count.store(0, Ordering::Release);
        }
        let n = self.count.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        if n <= self.limit {
            return Admit::Pass;
        }
        if self.suppressed.fetch_add(1, Ordering::AcqRel) == 0 {
            Admit::SuppressedFirst
        } else {
            Admit::Suppressed
        }
    }

    /// Takes the suppressed count for a summary.
    pub(crate) fn take_suppressed(&self) -> u64 {
        self.suppressed.swap(0, Ordering::AcqRel)
    }

    /// Puts back a count whose summary could not be queued; `true` when the budget must be
    /// listed again.
    pub(crate) fn give_back(&self, n: u64) -> bool {
        self.suppressed.fetch_add(n, Ordering::AcqRel) == 0
    }
}

/// `ev` names: `[a-z0-9_.]{1,64}`, not starting or ending with `.`. Checked at compile time by
/// the macro.
pub const fn valid_ev(ev: &str) -> bool {
    let b = ev.as_bytes();
    if b.is_empty() || b.len() > 64 || b[0] == b'.' || b[b.len() - 1] == b'.' {
        return false;
    }
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'.') {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_counts_per_minute_and_reports_first_suppression() {
        let b = Budget::new("x.y", 2);
        assert_eq!(b.admit(7), Admit::Pass);
        assert_eq!(b.admit(7), Admit::Pass);
        assert_eq!(b.admit(7), Admit::SuppressedFirst);
        assert_eq!(b.admit(7), Admit::Suppressed);
        assert_eq!(b.take_suppressed(), 2);
        assert_eq!(b.admit(8), Admit::Pass);
        assert!(b.give_back(5));
        assert_eq!(b.take_suppressed(), 5);
    }

    #[test]
    fn ev_names() {
        assert!(valid_ev("link.attempt_end"));
        assert!(!valid_ev("Link.x"));
        assert!(!valid_ev(".x"));
        assert!(!valid_ev("x y"));
        assert!(!valid_ev(""));
    }
}
