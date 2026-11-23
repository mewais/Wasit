//! The earlier history of a conversation a session resumes, the runtime's side of it (see
//! `ARCHITECTURE.md` §3.7 for the contract and `wasit_core::history` for what a driver says).
//!
//! * [`plan`] runs before the process of a session that resumes a conversation starts: when the
//!   agent keeps the history in its own storage it looks at the file *now*, before the agent
//!   writes to it again, and records how much of it the history is (the journaled
//!   [`EarlierPlan`](crate::note::EarlierPlan)).
//! * [`spawn`] runs the import off the async path: it reads the agent's files line by line on the
//!   blocking pool, feeds them to the driver's pure [`HistoryParser`] and hands the history over
//!   in chunks through a bounded channel. The actor journals each chunk as a note, one at a time,
//!   behind everything else it has to write, so the session is usable while a huge history loads.
//!
//! Nothing here knows an agent's storage format; the driver does.

use std::collections::{HashSet, VecDeque};
use std::io::{BufRead, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use wasit_core::{
    AgentEvent, DriverDescriptor, FileProblem, HistoryDelivery, HistoryFailure, HistoryLimits,
    HistoryParse, HistoryReport, SpawnSpec,
};

use crate::config::RuntimeConfig;
use crate::core::ImportWork;
use crate::note::EarlierPlan;
use crate::wire::EarlierUnavailable;

/// What the runtime does with a history import (shrunk by tests, a host may tune it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryConfig {
    /// What the driver's parser may keep (the import window, the per-item bound).
    pub limits: HistoryLimits,
    /// A chunk of the history is journaled in a note of about this many bytes (events are not
    /// split, so one chunk can be as big as its biggest item above it). Far below the 8 MiB a
    /// record may hold.
    pub chunk_bytes: usize,
    /// Most events in one chunk.
    pub chunk_events: usize,
    /// A history item bigger than this (serialized) is replaced by a notice that it was left
    /// out: a record of the journal stays far below its cap whatever the agent's file holds.
    pub max_event_bytes: usize,
    /// A source file larger than this is not read (the history is `unavailable: too_large`).
    pub max_source_bytes: u64,
    /// A line longer than this is not read; the file is read on.
    pub max_line_bytes: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        HistoryConfig {
            limits: HistoryLimits::default(),
            chunk_bytes: 1024 * 1024,
            chunk_events: 2000,
            max_event_bytes: 4 * 1024 * 1024,
            max_source_bytes: 1024 * 1024 * 1024,
            max_line_bytes: 64 * 1024 * 1024,
        }
    }
}

/// How the import of the agent's file starts, for a session whose first generation resumes the
/// conversation `native_id` in `spec.cwd`: `None` when the agent does not keep its history in
/// storage (nothing to plan: it replays it, or gives none), else the file and the length of it
/// the history is (or why there is no history).
pub(crate) async fn plan(
    cfg: &RuntimeConfig,
    desc: &dyn DriverDescriptor,
    spec: &SpawnSpec,
    native_id: &str,
) -> Option<EarlierPlan> {
    if desc.capabilities().earlier_history != HistoryDelivery::Storage {
        return None;
    }
    // The folder the process runs in: the session's, else the one the daemon's own is.
    let project = spec.cwd.clone().or_else(|| {
        std::env::current_dir()
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
    });
    let ctx = crate::discovery::probe_context(
        &cfg.override_for(&desc.id()).env,
        &spec.env,
        None,
        project,
    );
    let Some(path) = desc.history_storage(&ctx, native_id) else {
        return Some(EarlierPlan::Unavailable {
            why: EarlierUnavailable::NotOffered,
        });
    };
    let limit = cfg.history.max_source_bytes;
    Some(match tokio::fs::metadata(&path).await {
        Ok(m) if m.is_file() && m.len() > limit => EarlierPlan::Unavailable {
            why: EarlierUnavailable::TooLarge {
                bytes: m.len(),
                limit,
            },
        },
        Ok(m) if m.is_file() => EarlierPlan::Storage { path, len: m.len() },
        Ok(_) => EarlierPlan::Unavailable {
            why: EarlierUnavailable::SourceMissing,
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => EarlierPlan::Unavailable {
            why: EarlierUnavailable::SourceMissing,
        },
        Err(e) => EarlierPlan::Unavailable {
            why: EarlierUnavailable::SourceUnreadable {
                detail: e.to_string(),
            },
        },
    })
}

/// What the import hands to the actor.
#[derive(Debug)]
pub(crate) enum ImportMsg {
    /// The history from item `first` on, a chunk's worth.
    Chunk { first: u64, events: Vec<AgentEvent> },
    /// Every chunk was sent.
    End(HistoryReport),
    /// The import cannot go on.
    Failed(EarlierUnavailable),
}

/// An import under way: its messages, and the way to stop it. Dropping it stops the reading at
/// the next line (a session that ended or was deleted does not leave a big file being parsed).
pub(crate) struct ImportRun {
    pub rx: mpsc::Receiver<ImportMsg>,
    cancel: Arc<AtomicBool>,
}

impl Drop for ImportRun {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

/// Start reading the agent's file for `work` on the blocking pool. Messages arrive in order
/// through the bounded channel; a dropped run stops the import. The first `work.skip` history
/// items are not sent again (they are in the journal already).
pub(crate) fn spawn(
    desc: Arc<dyn DriverDescriptor>,
    cfg: HistoryConfig,
    work: ImportWork,
) -> ImportRun {
    let (tx, rx) = mpsc::channel(2);
    let cancel = Arc::new(AtomicBool::new(false));
    let stop = cancel.clone();
    // The blocking pool is the only place a big file is read; the handle is not needed: the
    // task ends when it has sent everything or when the run is dropped.
    drop(tokio::task::spawn_blocking(move || {
        let outcome = run(desc.as_ref(), &cfg, &work, &stop);
        if !stop.load(Ordering::Relaxed) {
            send_outcome(&tx, outcome, &cfg, work.skip);
        }
    }));
    ImportRun { rx, cancel }
}

enum Outcome {
    Parsed(HistoryParse),
    Failed(EarlierUnavailable),
}

fn send_outcome(tx: &mpsc::Sender<ImportMsg>, outcome: Outcome, cfg: &HistoryConfig, skip: u64) {
    let parse = match outcome {
        Outcome::Parsed(p) => p,
        Outcome::Failed(reason) => {
            // The receiver may be gone (the session ended): nothing to tell then.
            let _ = tx.blocking_send(ImportMsg::Failed(reason));
            return;
        }
    };
    let mut events = parse.events;
    if parse.report.older_omitted > 0 {
        // The transcript itself says that there is more of the conversation than it shows: the
        // agent still has it, and this session does not load it.
        events.insert(
            0,
            AgentEvent::Notice {
                message: format!(
                    "{} earlier messages of this conversation are in the agent's own history and are not shown here",
                    parse.report.older_omitted
                ),
            }
            .earlier(None),
        );
    }
    let total = events.len() as u64;
    if skip > total {
        // The journal holds more than the file still yields: it is not the file that was
        // imported from.
        let _ = tx.blocking_send(ImportMsg::Failed(EarlierUnavailable::Failed {
            detail: "the agent's file no longer matches what was imported from it".into(),
        }));
        return;
    }
    let mut report = parse.report;
    let mut chunk: Vec<AgentEvent> = Vec::new();
    let mut bytes = 0usize;
    let mut first = skip;
    for (i, event) in events.into_iter().enumerate() {
        // A size that cannot be taken counts as nothing; the chunk's event limit still bounds it.
        let mut size = serde_json::to_vec(&event).map(|v| v.len()).unwrap_or(0);
        let mut event = event;
        if size > cfg.max_event_bytes {
            // One item is never more than a journal record may hold: it is replaced by a
            // statement that it was left out (counted, like every other cut), the same way on
            // every run, so item numbers stay what they are.
            event = AgentEvent::Notice {
                message: format!(
                    "an item of the earlier history ({size} bytes) is too large to be kept"
                ),
            }
            .earlier(None);
            size = serde_json::to_vec(&event).map(|v| v.len()).unwrap_or(0);
            report.shortened += 1;
        }
        if (i as u64) < skip {
            continue;
        }
        if !chunk.is_empty() && (bytes + size > cfg.chunk_bytes || chunk.len() >= cfg.chunk_events)
        {
            let n = chunk.len() as u64;
            if tx
                .blocking_send(ImportMsg::Chunk {
                    first,
                    events: std::mem::take(&mut chunk),
                })
                .is_err()
            {
                return;
            }
            first += n;
            bytes = 0;
        }
        bytes += size;
        chunk.push(event);
    }
    if !chunk.is_empty()
        && tx
            .blocking_send(ImportMsg::Chunk {
                first,
                events: chunk,
            })
            .is_err()
    {
        return;
    }
    // The receiver may be gone: then nobody waits for the end.
    let _ = tx.blocking_send(ImportMsg::End(report));
}

/// Read the files and parse them. Blocking.
fn run(
    desc: &dyn DriverDescriptor,
    cfg: &HistoryConfig,
    work: &ImportWork,
    stop: &AtomicBool,
) -> Outcome {
    let Some(mut parser) = desc.history_parser(&work.path, &cfg.limits) else {
        return Outcome::Failed(EarlierUnavailable::NotOffered);
    };
    let mut queue: VecDeque<String> = VecDeque::from([work.path.clone()]);
    let mut seen: HashSet<String> = HashSet::from([work.path.clone()]);
    let mut main = true;
    while let Some(file) = queue.pop_front() {
        let range = if main { Some(work.len) } else { None };
        match read_lines(&file, range, cfg.max_line_bytes, stop, &mut *parser) {
            Ok(()) => {}
            // Nobody waits for the outcome any more.
            Err(ReadError::Stopped) => return Outcome::Failed(EarlierUnavailable::NotOffered),
            Err(ReadError::Missing) if main => {
                return Outcome::Failed(EarlierUnavailable::SourceMissing)
            }
            Err(ReadError::Io(detail)) if main => {
                return Outcome::Failed(EarlierUnavailable::SourceUnreadable { detail })
            }
            Err(ReadError::Missing) => parser.unavailable(&file, FileProblem::Missing),
            Err(ReadError::Io(_)) => parser.unavailable(&file, FileProblem::Unreadable),
        }
        main = false;
        for wanted in parser.wants() {
            if seen.insert(wanted.clone()) {
                queue.push_back(wanted);
            }
        }
    }
    match parser.finish() {
        Ok(parse) => Outcome::Parsed(parse),
        Err(HistoryFailure::FormatNotUnderstood { record }) => {
            Outcome::Failed(EarlierUnavailable::FormatNotUnderstood { record })
        }
    }
}

enum ReadError {
    Missing,
    Io(String),
    /// The import was stopped.
    Stopped,
}

/// Push the lines of `file` (its first `range` bytes when given, else as long as it is now) to
/// `parser`. A line over `max_line` is skipped (the parser is told).
fn read_lines(
    file: &str,
    range: Option<u64>,
    max_line: usize,
    stop: &AtomicBool,
    parser: &mut dyn wasit_core::HistoryParser,
) -> Result<(), ReadError> {
    let f = match std::fs::File::open(file) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(ReadError::Missing),
        Err(e) => return Err(ReadError::Io(e.to_string())),
    };
    let len = match range {
        Some(l) => l,
        None => f
            .metadata()
            .map_err(|e| ReadError::Io(e.to_string()))?
            .len(),
    };
    parser.begin(file, len);
    let mut reader = std::io::BufReader::with_capacity(256 * 1024, f.take(len));
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if stop.load(Ordering::Relaxed) {
            return Err(ReadError::Stopped);
        }
        buf.clear();
        let n = reader
            .by_ref()
            .take(max_line as u64 + 1)
            .read_until(b'\n', &mut buf)
            .map_err(|e| ReadError::Io(e.to_string()))?;
        if n == 0 {
            break;
        }
        let terminated = buf.last() == Some(&b'\n');
        if !terminated && buf.len() > max_line {
            // A line longer than the limit: drop the rest of it.
            parser.unavailable(file, FileProblem::LineTooLong);
            skip_line(&mut reader).map_err(|e| ReadError::Io(e.to_string()))?;
            continue;
        }
        if terminated {
            buf.pop();
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        parser.push(&String::from_utf8_lossy(&buf), terminated);
    }
    parser.end();
    Ok(())
}

/// Discard input up to and including the next newline.
fn skip_line(r: &mut impl BufRead) -> std::io::Result<()> {
    loop {
        let (done, used) = {
            let buf = r.fill_buf()?;
            if buf.is_empty() {
                return Ok(());
            }
            match buf.iter().position(|b| *b == b'\n') {
                Some(i) => (true, i + 1),
                None => (false, buf.len()),
            }
        };
        r.consume(used);
        if done {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use wasit_core::HistoryParser;

    /// A parser that records what it is given and makes one history item per line.
    #[derive(Default)]
    struct Stub {
        log: Arc<Mutex<Vec<String>>>,
        events: Vec<AgentEvent>,
        wants: Vec<String>,
    }

    impl HistoryParser for Stub {
        fn begin(&mut self, file: &str, len: u64) {
            self.log
                .lock()
                .unwrap()
                .push(format!("begin {} {len}", file.rsplit('/').next().unwrap()));
        }
        fn push(&mut self, line: &str, terminated: bool) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{line}|{terminated}"));
            self.events
                .push(AgentEvent::UserMessage { text: line.into() }.earlier(None));
        }
        fn end(&mut self) {
            self.log.lock().unwrap().push("end".into());
        }
        fn wants(&mut self) -> Vec<String> {
            std::mem::take(&mut self.wants)
        }
        fn unavailable(&mut self, file: &str, why: FileProblem) {
            self.log.lock().unwrap().push(format!(
                "unavailable {} {why:?}",
                file.rsplit('/').next().unwrap()
            ));
        }
        fn finish(self: Box<Self>) -> Result<HistoryParse, HistoryFailure> {
            Ok(HistoryParse {
                report: HistoryReport {
                    events: self.events.len() as u64,
                    ..HistoryReport::default()
                },
                events: self.events,
            })
        }
    }

    struct Dir(std::path::PathBuf);
    impl Dir {
        fn new() -> Dir {
            static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "whist-{}-{}",
                std::process::id(),
                N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&p).unwrap();
            Dir(p)
        }
        fn file(&self, name: &str, bytes: &[u8]) -> String {
            let p = self.0.join(name);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, bytes).unwrap();
            p.to_string_lossy().into_owned()
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            // Best-effort removal of a scratch directory.
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn lines_of(path: &str, range: Option<u64>, max_line: usize) -> Vec<String> {
        let mut stub = Stub::default();
        let log = stub.log.clone();
        let r = read_lines(path, range, max_line, &AtomicBool::new(false), &mut stub);
        assert!(r.is_ok());
        let v = log.lock().unwrap().clone();
        v
    }

    /// Lines come without their end of line (`\n`, `\r\n`), the last one says whether it had
    /// one, and only the recorded length is read, whatever the file holds now.
    #[test]
    fn lines_are_read_up_to_the_recorded_length() {
        let d = Dir::new();
        let p = d.file("a.jsonl", b"one\r\ntwo\nthree");
        assert_eq!(
            lines_of(&p, None, 1024),
            [
                "begin a.jsonl 14",
                "one|true",
                "two|true",
                "three|false",
                "end"
            ]
        );
        // The agent appended after the session started: not part of the history.
        assert_eq!(
            lines_of(&p, Some(9), 1024),
            ["begin a.jsonl 9", "one|true", "two|true", "end"]
        );
        // A cut in the middle of a line gives its start, unterminated.
        assert_eq!(
            lines_of(&p, Some(7), 1024),
            ["begin a.jsonl 7", "one|true", "tw|false", "end"]
        );
        // Invalid UTF-8 is replaced, never an error.
        let p = d.file("b.jsonl", b"a\xffb\n");
        assert_eq!(
            lines_of(&p, None, 1024),
            ["begin b.jsonl 4", "a\u{fffd}b|true", "end"]
        );
    }

    /// A stopped import reads no more: a session that ended does not leave a big file being
    /// parsed (the run is dropped, the flag is set).
    #[test]
    fn a_stopped_import_stops_at_the_next_line() {
        let d = Dir::new();
        let p = d.file("s.jsonl", b"one\ntwo\nthree\n");
        let mut stub = Stub::default();
        let log = stub.log.clone();
        let stop = AtomicBool::new(true);
        assert!(matches!(
            read_lines(&p, None, 1024, &stop, &mut stub),
            Err(ReadError::Stopped)
        ));
        assert_eq!(log.lock().unwrap().clone(), ["begin s.jsonl 14"]);
        // The run's drop sets the flag.
        let flag = {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let _g = rt.enter();
            let run = spawn(
                wasit_claude::descriptor(),
                HistoryConfig::default(),
                ImportWork {
                    path: p,
                    len: 14,
                    skip: 0,
                },
            );
            let flag = run.cancel.clone();
            drop(run);
            flag
        };
        assert!(flag.load(Ordering::Relaxed));
    }

    #[test]
    fn a_line_over_the_limit_is_skipped_and_the_rest_is_read() {
        let d = Dir::new();
        let mut text = b"ok\n".to_vec();
        text.extend(vec![b'x'; 500]);
        text.extend(b"\nafter\n");
        let p = d.file("c.jsonl", &text);
        assert_eq!(
            lines_of(&p, None, 100),
            [
                &format!("begin c.jsonl {}", text.len()),
                "ok|true",
                "unavailable c.jsonl LineTooLong",
                "after|true",
                "end"
            ]
        );
    }

    fn collect(parse: HistoryParse, skip: u64, cfg: &HistoryConfig) -> Vec<ImportMsg> {
        let (tx, mut rx) = mpsc::channel(10_000);
        send_outcome(&tx, Outcome::Parsed(parse), cfg, skip);
        drop(tx);
        let mut v = Vec::new();
        while let Ok(m) = rx.try_recv() {
            v.push(m);
        }
        v
    }

    /// An item that alone would not fit a record is replaced by a notice, counted, the same way
    /// whatever part of the history is sent.
    #[test]
    fn an_item_too_big_for_a_record_is_replaced_and_counted() {
        let cfg = HistoryConfig {
            max_event_bytes: 300,
            ..HistoryConfig::default()
        };
        let mut parse = parse_of(3);
        parse.events[1] = AgentEvent::UserMessage {
            text: "x".repeat(1000),
        }
        .earlier(None);
        for skip in [0, 2] {
            let msgs = collect(
                HistoryParse {
                    events: parse.events.clone(),
                    report: HistoryReport::default(),
                },
                skip,
                &cfg,
            );
            let sent: Vec<&AgentEvent> = msgs
                .iter()
                .flat_map(|m| match m {
                    ImportMsg::Chunk { events, .. } => events.iter().collect(),
                    _ => vec![],
                })
                .collect();
            if skip == 0 {
                assert!(matches!(
                    sent[1].as_history().unwrap().1,
                    AgentEvent::Notice { message } if message.contains("too large")
                ));
            } else {
                assert_eq!(sent.len(), 1);
            }
            match msgs.last() {
                Some(ImportMsg::End(r)) => assert_eq!(r.shortened, 1),
                other => panic!("{other:?}"),
            }
        }
    }

    fn parse_of(n: usize) -> HistoryParse {
        HistoryParse {
            events: (0..n)
                .map(|i| {
                    AgentEvent::UserMessage {
                        text: format!("m{i}"),
                    }
                    .earlier(None)
                })
                .collect(),
            report: HistoryReport::default(),
        }
    }

    /// The history goes out in chunks bounded by size and count, numbered by the item they start
    /// with; a resumed import skips what the journal holds; the end comes last.
    #[test]
    fn the_history_is_chunked_numbered_and_resumable() {
        let cfg = HistoryConfig {
            chunk_events: 4,
            ..HistoryConfig::default()
        };
        let msgs = collect(parse_of(10), 0, &cfg);
        let shape: Vec<(u64, usize)> = msgs
            .iter()
            .filter_map(|m| match m {
                ImportMsg::Chunk { first, events } => Some((*first, events.len())),
                _ => None,
            })
            .collect();
        assert_eq!(shape, [(0, 4), (4, 4), (8, 2)]);
        assert!(matches!(msgs.last(), Some(ImportMsg::End(_))));
        // Resumed with 6 items in the journal.
        let msgs = collect(parse_of(10), 6, &cfg);
        let shape: Vec<(u64, usize)> = msgs
            .iter()
            .filter_map(|m| match m {
                ImportMsg::Chunk { first, events } => Some((*first, events.len())),
                _ => None,
            })
            .collect();
        assert_eq!(shape, [(6, 4)]);
        // By size: a chunk holds whole items and ends before one that would pass the bound.
        let one = serde_json::to_vec(&parse_of(1).events[0]).unwrap().len();
        let cfg = HistoryConfig {
            chunk_bytes: one * 2 + 1,
            ..HistoryConfig::default()
        };
        let shape: Vec<usize> = collect(parse_of(5), 0, &cfg)
            .iter()
            .filter_map(|m| match m {
                ImportMsg::Chunk { events, .. } => Some(events.len()),
                _ => None,
            })
            .collect();
        assert_eq!(shape, [2, 2, 1]);
        // Nothing to import is only an end.
        let msgs = collect(parse_of(0), 0, &HistoryConfig::default());
        assert!(matches!(msgs.as_slice(), [ImportMsg::End(_)]));
        // The journal holding more than the file yields is a different file: refused.
        let msgs = collect(parse_of(3), 5, &HistoryConfig::default());
        assert!(matches!(
            msgs.as_slice(),
            [ImportMsg::Failed(EarlierUnavailable::Failed { .. })]
        ));
    }

    /// Files the parser asks for are read after the first (each once); one that is missing is
    /// told to it and does not fail the import, the main file missing does.
    #[test]
    fn files_the_parser_wants_are_read_once_and_a_missing_one_is_only_told() {
        struct Desc(Arc<Mutex<Vec<String>>>, Vec<String>);
        let d = Dir::new();
        let main = d.file("m.jsonl", b"x\n");
        let side = d.file("sub/s.jsonl", b"y\n");
        let gone = d.0.join("gone.jsonl").to_string_lossy().into_owned();
        let log = Arc::new(Mutex::new(Vec::new()));
        impl wasit_core::ConfigRenderer for Desc {
            fn render_config(
                &self,
                _: &wasit_core::AgentConfig,
                _: &mut SpawnSpec,
            ) -> Vec<wasit_core::Unsupported> {
                vec![]
            }
        }
        impl DriverDescriptor for Desc {
            fn id(&self) -> wasit_core::DriverId {
                "stub".into()
            }
            fn display_name(&self) -> &str {
                "stub"
            }
            fn default_binary(&self) -> &str {
                "stub"
            }
            fn argv(&self, _: &SpawnSpec) -> Vec<String> {
                vec![]
            }
            fn env(&self, _: &SpawnSpec) -> Vec<(String, String)> {
                vec![]
            }
            fn capabilities(&self) -> wasit_core::DriverCapabilities {
                wasit_claude::descriptor().capabilities()
            }
            fn new_protocol(&self) -> Box<dyn wasit_core::Protocol> {
                wasit_claude::descriptor().new_protocol()
            }
            fn discovery(
                &self,
                _: &wasit_core::DiscoveryContext,
            ) -> Vec<wasit_core::DiscoveryProbe> {
                vec![]
            }
            fn history_parser(&self, _: &str, _: &HistoryLimits) -> Option<Box<dyn HistoryParser>> {
                Some(Box::new(Stub {
                    log: self.0.clone(),
                    wants: self.1.clone(),
                    ..Stub::default()
                }))
            }
        }
        let desc = Desc(
            log.clone(),
            vec![side.clone(), side.clone(), gone.clone(), main.clone()],
        );
        let work = ImportWork {
            path: main.clone(),
            len: 2,
            skip: 0,
        };
        let out = run(
            &desc,
            &HistoryConfig::default(),
            &work,
            &AtomicBool::new(false),
        );
        let Outcome::Parsed(p) = out else {
            panic!("not parsed");
        };
        assert_eq!(p.events.len(), 2);
        assert_eq!(
            log.lock().unwrap().clone(),
            [
                "begin m.jsonl 2",
                "x|true",
                "end",
                "begin s.jsonl 2",
                "y|true",
                "end",
                "unavailable gone.jsonl Missing"
            ]
        );
        // The main file missing is the import failing, with the reason.
        let work = ImportWork {
            path: gone,
            len: 2,
            skip: 0,
        };
        assert!(matches!(
            run(
                &desc,
                &HistoryConfig::default(),
                &work,
                &AtomicBool::new(false)
            ),
            Outcome::Failed(EarlierUnavailable::SourceMissing)
        ));
    }

    fn spec_in(dir: &std::path::Path) -> SpawnSpec {
        SpawnSpec {
            cwd: Some("/work/proj".into()),
            env: vec![(
                "CLAUDE_CONFIG_DIR".into(),
                dir.to_string_lossy().into_owned(),
            )],
            ..SpawnSpec::default()
        }
    }

    /// The plan looks at the file before the agent writes to it again: its length when it is
    /// there, a reason when it is not, nothing for an agent that does not keep it in storage.
    #[tokio::test]
    async fn the_plan_records_the_length_or_the_reason() {
        let d = Dir::new();
        let cfg = RuntimeConfig::new(&d.0, crate::config::ShimLauncher::binary("/nonexistent"));
        let desc = wasit_claude::descriptor();
        let id = "3b241101-e2bb-4255-8caf-4136c566a962";
        let spec = spec_in(&d.0);
        // Missing.
        assert_eq!(
            plan(&cfg, desc.as_ref(), &spec, id).await,
            Some(EarlierPlan::Unavailable {
                why: EarlierUnavailable::SourceMissing
            })
        );
        // There.
        let path = d.file(&format!("projects/-work-proj/{id}.jsonl"), b"0123456789");
        assert_eq!(
            plan(&cfg, desc.as_ref(), &spec, id).await,
            Some(EarlierPlan::Storage { path, len: 10 })
        );
        // Larger than the runtime reads.
        let mut small = cfg.clone();
        small.history.max_source_bytes = 4;
        assert!(matches!(
            plan(&small, desc.as_ref(), &spec, id).await,
            Some(EarlierPlan::Unavailable {
                why: EarlierUnavailable::TooLarge {
                    bytes: 10,
                    limit: 4
                }
            })
        ));
        // An id that is not a conversation id names no file.
        assert!(matches!(
            plan(&cfg, desc.as_ref(), &spec, "../../etc/passwd").await,
            Some(EarlierPlan::Unavailable {
                why: EarlierUnavailable::NotOffered
            })
        ));
        // An agent that replays its history itself plans nothing.
        let codex = wasit_codex::descriptor();
        assert_eq!(plan(&cfg, codex.as_ref(), &spec, id).await, None);
    }

    /// The whole import over a real driver's parser: a transcript is read off the async path,
    /// chunked and handed over; the part the journal holds already is not sent again.
    #[tokio::test]
    async fn an_import_runs_off_the_async_path_and_hands_over_chunks() {
        let d = Dir::new();
        let mut text = String::new();
        let mut parent = String::from("null");
        for i in 0..6 {
            let uuid = format!("00000000-0000-4000-8000-{i:012}");
            let role = if i % 2 == 0 { "user" } else { "assistant" };
            let content = if role == "user" {
                format!("\"q{i}\"")
            } else {
                format!("[{{\"type\":\"text\",\"text\":\"a{i}\"}}]")
            };
            text.push_str(&format!(
                "{{\"type\":\"{role}\",\"uuid\":\"{uuid}\",\"parentUuid\":{parent},\"isSidechain\":false,\"timestamp\":\"2026-11-20T10:00:0{i}.000Z\",\"message\":{{\"role\":\"{role}\",\"content\":{content}}}}}\n"
            ));
            parent = format!("\"{uuid}\"");
        }
        let path = d.file("t.jsonl", text.as_bytes());
        let cfg = HistoryConfig {
            chunk_events: 4,
            ..HistoryConfig::default()
        };
        let mut run = spawn(
            wasit_claude::descriptor(),
            cfg,
            ImportWork {
                path: path.clone(),
                len: text.len() as u64,
                skip: 0,
            },
        );
        let mut items = 0;
        let mut ended = false;
        while let Some(m) = run.rx.recv().await {
            match m {
                ImportMsg::Chunk { first, events } => {
                    assert_eq!(first, items);
                    items += events.len() as u64;
                }
                ImportMsg::End(r) => {
                    ended = true;
                    assert_eq!(r.messages, 6);
                    assert!(r.complete(), "{r:?}");
                }
                ImportMsg::Failed(f) => panic!("{f:?}"),
            }
        }
        assert!(ended);
        assert_eq!(items, 6);
        // Resumed after 4 items: only the last two come.
        let mut run = spawn(
            wasit_claude::descriptor(),
            cfg,
            ImportWork {
                path,
                len: text.len() as u64,
                skip: 4,
            },
        );
        match run.rx.recv().await {
            Some(ImportMsg::Chunk { first: 4, events }) => assert_eq!(events.len(), 2),
            other => panic!("{other:?}"),
        }
    }
}
