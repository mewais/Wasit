//! A conversation's earlier history, described without doing any I/O.
//!
//! A session that resumes a conversation its own journal has not seen (a conversation the user
//! started in their terminal, or one of a session that was deleted) shows that conversation's
//! earlier messages before its live ones. Where those messages come from is the agent's choice,
//! and there are two shapes ([`HistoryDelivery`]):
//!
//! * **the agent sends them itself** while the session is opened (codex `thread/resume` answers
//!   with the turns, ACP `session/load` replays the conversation as updates): the driver's
//!   [`Protocol`](crate::Protocol) turns that into [`AgentEvent::History`] events, which arrive
//!   as ordinary journaled CLI output;
//! * **the agent keeps them in its own storage** (claude's transcript file): the driver
//!   *describes* what to read and how to understand it — [`DriverDescriptor::history_storage`]
//!   names the file, [`DriverDescriptor::history_parser`] makes a [`HistoryParser`], a pure state
//!   machine over the lines of the files — and the runtime executes it off the async path,
//!   exactly as discovery executes a [`ProbeAction`](crate::ProbeAction).
//!
//! Everything that knows an agent's storage format lives in that driver's crate; this module only
//! has the shapes they share.
//!
//! [`DriverDescriptor::history_storage`]: crate::DriverDescriptor::history_storage
//! [`DriverDescriptor::history_parser`]: crate::DriverDescriptor::history_parser

use serde::{Deserialize, Serialize};

use crate::event::AgentEvent;

/// How a driver's agent gives out the earlier history of a conversation it resumes
/// ([`DriverCapabilities::earlier_history`](crate::DriverCapabilities::earlier_history)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum HistoryDelivery {
    /// The agent gives none out: a resumed conversation shows nothing of what came before.
    #[default]
    None,
    /// The agent sends the history over its protocol while the session is opened.
    Wire,
    /// The agent keeps the history in its own storage; the driver reads it.
    Storage,
}

/// What a history import may do, set by the runtime (a host configures it; tests shrink it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryLimits {
    /// The import takes the most recent part of the source: records within the last
    /// `window_bytes` bytes of the file are read in full, older ones are only counted; when the
    /// conversation spans several files (a subagent's own) the window is one budget for all of
    /// them, the first file taking its share first. What falls outside is reported
    /// (`older_omitted`), never silently dropped.
    pub window_bytes: u64,
    /// One history item is cut to about this many bytes (a tool result's text); its media above
    /// it is left out. Both are counted in the report (`shortened`).
    pub item_bytes: usize,
}

impl HistoryLimits {
    pub const DEFAULT_WINDOW: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_ITEM: usize = 256 * 1024;
}

impl Default for HistoryLimits {
    fn default() -> Self {
        HistoryLimits {
            window_bytes: Self::DEFAULT_WINDOW,
            item_bytes: Self::DEFAULT_ITEM,
        }
    }
}

/// Why a file the history spans could not be read ([`HistoryParser::unavailable`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileProblem {
    Missing,
    Unreadable,
    /// A line longer than the runtime reads in one piece was skipped.
    LineTooLong,
}

/// What the parser made of the files: counts only, so it can be journaled, shown and traced
/// without any of the conversation in it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct HistoryReport {
    /// Lines read, over all files.
    pub records: u64,
    /// Lines that are not records of the format (not JSON, or not an object).
    pub unreadable: u64,
    /// Records of a kind the parser does not know, counted by kind name (bounded: the first
    /// [`HistoryReport::KINDS_MAX`] kinds); the total is `unrecognised`.
    pub unrecognised: u64,
    pub unrecognised_kinds: Vec<String>,
    /// Records of kinds the parser knows to be no part of the conversation (bookkeeping).
    pub ignored: u64,
    /// History events produced.
    pub events: u64,
    /// Conversation messages (the agent's user and assistant records) the history spans.
    pub messages: u64,
    /// Messages older than the import window: they exist in the agent's own history and are not
    /// shown.
    pub older_omitted: u64,
    /// Items cut to the per-item bound, or whose media was left out.
    pub shortened: u64,
    /// Files the conversation spans (a subagent's transcript) that could not be read.
    pub missing_files: u64,
    /// The last line of a file had no end: it may have been cut while the agent wrote it.
    pub truncated_tail: bool,
}

impl HistoryReport {
    /// Most distinct unrecognised kind names kept.
    pub const KINDS_MAX: usize = 8;

    /// Count a record of a kind the parser does not know.
    pub fn unrecognised_kind(&mut self, kind: &str) {
        self.unrecognised += 1;
        let kind: String = kind.chars().take(40).collect();
        if self.unrecognised_kinds.len() < Self::KINDS_MAX
            && !self.unrecognised_kinds.contains(&kind)
        {
            self.unrecognised_kinds.push(kind);
        }
    }

    /// Everything the agent's history holds is in the events: nothing left out, cut or
    /// unreadable.
    pub fn complete(&self) -> bool {
        self.older_omitted == 0
            && self.shortened == 0
            && self.unreadable == 0
            && self.unrecognised == 0
            && self.missing_files == 0
            && !self.truncated_tail
    }
}

/// Why the parser could not make a history of what it was given.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "failure", rename_all = "snake_case")]
pub enum HistoryFailure {
    /// No line is a record of the format the parser knows: `record` is the 1-based number of the
    /// first line, among all files, that was not (or of the first that was not understood).
    FormatNotUnderstood { record: u64 },
}

/// The history, oldest first, and what was left out.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryParse {
    /// Each event is an [`AgentEvent::History`].
    pub events: Vec<AgentEvent>,
    pub report: HistoryReport,
}

/// A pure state machine over the lines of the files a conversation's history is kept in. The
/// runtime reads each file off the async path, line by line, and calls it; it never does I/O
/// itself, so the same bytes always give the same history (which is what makes an interrupted
/// import resumable).
pub trait HistoryParser: Send {
    /// A file is about to be read: exactly `len` bytes of it will be pushed (the runtime never
    /// reads past the length it recorded when the session started, whatever the agent has
    /// written since). Files are read one after the other, the first being the one
    /// [`DriverDescriptor::history_storage`](crate::DriverDescriptor::history_storage) named.
    fn begin(&mut self, file: &str, len: u64);

    /// One line of the file being read, without its end of line. `terminated` is `false` for the
    /// last line of the range when it had no end (it may have been cut).
    fn push(&mut self, line: &str, terminated: bool);

    /// The file being read ended.
    fn end(&mut self);

    /// Files the parser wants read next, now that the files so far were read: full paths, each
    /// asked for once. Asked after every file until it names none.
    fn wants(&mut self) -> Vec<String>;

    /// A file the parser asked for ([`HistoryParser::wants`]) could not be read.
    fn unavailable(&mut self, file: &str, why: FileProblem);

    /// All files are done.
    fn finish(self: Box<Self>) -> Result<HistoryParse, HistoryFailure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_is_complete_only_when_nothing_is_left_out() {
        assert!(HistoryReport::default().complete());
        for broken in [
            HistoryReport {
                older_omitted: 1,
                ..Default::default()
            },
            HistoryReport {
                shortened: 1,
                ..Default::default()
            },
            HistoryReport {
                unreadable: 1,
                ..Default::default()
            },
            HistoryReport {
                unrecognised: 1,
                ..Default::default()
            },
            HistoryReport {
                missing_files: 1,
                ..Default::default()
            },
            HistoryReport {
                truncated_tail: true,
                ..Default::default()
            },
        ] {
            assert!(!broken.complete(), "{broken:?}");
        }
        // Counts that are only bookkeeping do not make it incomplete.
        assert!(HistoryReport {
            records: 100,
            ignored: 40,
            events: 50,
            messages: 60,
            ..Default::default()
        }
        .complete());
    }

    /// Unrecognised kinds are counted all, named a few times at most, each name bounded.
    #[test]
    fn unrecognised_kinds_are_counted_and_their_names_bounded() {
        let mut r = HistoryReport::default();
        for i in 0..20 {
            r.unrecognised_kind(&format!("kind-{}", i % 12));
        }
        r.unrecognised_kind(&"x".repeat(500));
        assert_eq!(r.unrecognised, 21);
        assert_eq!(r.unrecognised_kinds.len(), HistoryReport::KINDS_MAX);
        assert!(r.unrecognised_kinds.iter().all(|k| k.chars().count() <= 40));
        // The same kind twice is named once.
        let mut r = HistoryReport::default();
        r.unrecognised_kind("a");
        r.unrecognised_kind("a");
        assert_eq!((r.unrecognised, r.unrecognised_kinds.len()), (2, 1));
    }
}
