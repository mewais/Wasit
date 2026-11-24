use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, watch};

use wasit_shim::journal::sorted_ndjson;

#[cfg(test)]
use crate::core::{blank_snapshot, Conversation, SessionMeta};
#[cfg(test)]
use crate::wire::{event_cursor, NoticeCode, Phase, SessionEventKind, SessionId};
use crate::wire::{SessionEvent, SessionSnapshot};

const FORMAT: u32 = 1;
const META_FILE: &str = "meta.json";
const LOCK_FILE: &str = "writer.lock";
const EVENTS_FILE: &str = "events.ndjson";
const EARLIER_FILE: &str = "earlier.ndjson";
const CURSOR_PREFIX: &[u8] = b"{\"cursor\":";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogState {
    Building,
    Ready,
    Failed(String),
}

pub struct EventLog {
    paths: Paths,
    tx: mpsc::UnboundedSender<LogMsg>,
    state: watch::Receiver<LogState>,
    written: watch::Receiver<u64>,
    submitted: Arc<AtomicU64>,
    writer: Option<tokio::task::JoinHandle<()>>,
    /// The snapshot of the seal the log was opened with, when that seal is valid.
    sealed: Option<Box<SessionSnapshot>>,
}

/// A read-side handle on a log: pages are served from it without owning the writer.
#[derive(Clone)]
pub struct LogView {
    paths: Paths,
    state: watch::Receiver<LogState>,
    written: watch::Receiver<u64>,
    submitted: Arc<AtomicU64>,
}

/// Which of the log's two files a page is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogFile {
    /// The session's own events, in cursor order.
    Events,
    /// The conversation's earlier history items, in cursor order.
    Earlier,
}

/// Bytes a backward scan reads per block (see [`BackwardRecords`]).
const SCAN_BLOCK: usize = 64 * 1024;

/// Where a scan of a log file starts (see [`open_scan`]). A backward scan yields the records
/// with `cursor < before` newest first; a forward one the records with `cursor >= from` oldest
/// first. `None` is the end of the file, or its start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scan {
    Backward { before: Option<u64> },
    Forward { from: Option<u64> },
}

/// The records of a scan, each with the bytes it occupies in the log (its newline included).
pub(crate) type Records = Box<dyn Iterator<Item = io::Result<(SessionEvent, u64)>>>;

fn writer_gone() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "log writer stopped")
}

impl LogView {
    /// Waits until the log is complete (not `Building`) and every record submitted before this
    /// call is on disk, event-driven by the log's watches. Fails when the log failed or its
    /// writer has stopped.
    pub(crate) async fn settled(&self) -> io::Result<()> {
        let target = self.submitted.load(Ordering::Relaxed);
        let mut state = self.state.clone();
        let current = match state.wait_for(|s| !matches!(s, LogState::Building)).await {
            Ok(s) => (*s).clone(),
            Err(_) => return Err(writer_gone()),
        };
        if let LogState::Failed(reason) = current {
            return Err(io::Error::other(reason));
        }
        let mut written = self.written.clone();
        written
            .wait_for(|w| *w >= target)
            .await
            .map_err(|_| writer_gone())?;
        Ok(())
    }

    /// Waits, event-driven, until the writer has put `target` records on disk (a count the
    /// actor sampled from [`EventLog::submitted`]). Unlike [`Self::settled`] it does not wait
    /// for the log to be complete: a log the actor is still catching up holds every record up
    /// to what the actor has published. Fails when the log failed, or its writer stopped short.
    pub(crate) async fn caught_up(&self, target: u64) -> io::Result<()> {
        let mut state = self.state.clone();
        let mut written = self.written.clone();
        loop {
            if let LogState::Failed(reason) = &*state.borrow_and_update() {
                return Err(io::Error::other(reason.clone()));
            }
            if *written.borrow_and_update() >= target {
                return Ok(());
            }
            tokio::select! {
                r = state.changed() => {
                    if r.is_err() && *written.borrow() < target {
                        return Err(writer_gone());
                    }
                }
                r = written.changed() => {
                    if r.is_err() && *written.borrow() < target {
                        return Err(writer_gone());
                    }
                }
            }
        }
    }

    /// The events of the log with `after < cursor` (all when `after` is `None`) and, with
    /// `through`, `cursor <= through`, in cursor order, read by a task of its own into a bounded
    /// channel (it stops when the receiver goes away). Only complete records present when the
    /// read starts are streamed.
    pub(crate) fn stream(
        &self,
        after: Option<u64>,
        through: Option<u64>,
    ) -> mpsc::Receiver<io::Result<SessionEvent>> {
        stream_events(self.paths.events.clone(), after, through)
    }

    /// The last `limit` items `pick` makes of the records of `file` with `cursor < before`
    /// (all when `None`), oldest first, and whether older records exist. Reading stops once
    /// `max_bytes` of the log have been scanned and at least one item is held, so a page is at
    /// most `max_bytes` plus one record. Callers settle the log first.
    pub(crate) async fn page<T: Send + 'static>(
        &self,
        file: LogFile,
        before: Option<u64>,
        limit: usize,
        max_bytes: u64,
        pick: impl FnMut(SessionEvent) -> Option<T> + Send + 'static,
    ) -> io::Result<(Vec<T>, bool)> {
        let path = match file {
            LogFile::Events => self.paths.events.clone(),
            LogFile::Earlier => self.paths.earlier.clone(),
        };
        tokio::task::spawn_blocking(move || {
            page_back_blocking(path, before, limit, max_bytes, pick)
        })
        .await
        .expect("spawn_blocking panicked")
    }

    /// Runs `run` on the complete records of `file` that `scan` selects, on a blocking thread.
    /// The records are read as `run` pulls them, in blocks, so `run` decides how far to go.
    /// Callers settle the log first.
    pub(crate) async fn scan_records<T: Send + 'static>(
        &self,
        file: LogFile,
        scan: Scan,
        run: impl FnOnce(Records) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let path = match file {
            LogFile::Events => self.paths.events.clone(),
            LogFile::Earlier => self.paths.earlier.clone(),
        };
        tokio::task::spawn_blocking(move || run(open_scan(&path, scan)?))
            .await
            .expect("spawn_blocking panicked")
    }
}

#[derive(Debug)]
enum LogMsg {
    Append {
        events: Vec<SessionEvent>,
        earlier: Vec<SessionEvent>,
    },
    Seal {
        snapshot: Box<SessionSnapshot>,
        journal_len: u64,
        last_seq: u64,
    },
    Unseal {
        done: Option<oneshot::Sender<io::Result<()>>>,
    },
    /// The owner has caught up with the journal: an unsealed log is complete from now on.
    Ready,
    Fail(String),
    Reset,
}

#[derive(Clone)]
pub(crate) struct Paths {
    pub(crate) dir: PathBuf,
    pub(crate) events: PathBuf,
    pub(crate) earlier: PathBuf,
    pub(crate) meta: PathBuf,
}

impl Paths {
    fn new(dir: &Path) -> Self {
        let dir = dir.join("derived");
        Paths {
            events: dir.join(EVENTS_FILE),
            earlier: dir.join(EARLIER_FILE),
            meta: dir.join(META_FILE),
            dir,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Meta {
    format: u32,
    build: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal: Option<Seal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Seal {
    journal_len: u64,
    last_seq: u64,
    snapshot: Box<SessionSnapshot>,
}

#[derive(Debug)]
struct RecoverOutcome {
    meta: Meta,
    state: LogState,
    written: u64,
    lock: std::fs::File,
}

impl EventLog {
    pub async fn open(
        session_dir: &Path,
        build: impl Into<String>,
        journal_len: u64,
        last_seq: Option<u64>,
    ) -> io::Result<Self> {
        let build = build.into();
        let paths = Paths::new(session_dir);
        let outcome = tokio::task::spawn_blocking({
            let paths = paths.clone();
            let build = build.clone();
            move || {
                // Before anything is read or truncated: a writer that is still draining (a
                // manager dropped a moment ago) must finish first, or two writers interleave.
                let lock = acquire_writer(&paths)?;
                recover(&paths, &build, journal_len, last_seq, lock)
            }
        })
        .await
        .expect("spawn_blocking panicked")?;

        let sealed = match outcome.state {
            LogState::Ready => outcome.meta.seal.as_ref().map(|s| s.snapshot.clone()),
            _ => None,
        };
        let submitted = Arc::new(AtomicU64::new(outcome.written));
        let (state_tx, state) = watch::channel(outcome.state.clone());
        let (written_tx, written) = watch::channel(outcome.written);
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = LogWriter {
            paths: paths.clone(),
            meta: outcome.meta,
            state: state_tx,
            written: written_tx,
            written_count: outcome.written,
            submitted: submitted.clone(),
            _lock: outcome.lock,
        };
        let join = tokio::spawn(writer.run(rx));

        Ok(EventLog {
            paths,
            tx,
            state,
            written,
            submitted,
            writer: Some(join),
            sealed,
        })
    }

    /// The session snapshot the log was sealed with, if it was opened sealed (complete).
    pub(crate) fn sealed_snapshot(&self) -> Option<&SessionSnapshot> {
        self.sealed.as_deref()
    }

    pub fn state(&self) -> watch::Receiver<LogState> {
        self.state.clone()
    }

    pub fn written(&self) -> watch::Receiver<u64> {
        self.written.clone()
    }

    pub(crate) fn paths(&self) -> Paths {
        self.paths.clone()
    }

    pub fn submitted(&self) -> u64 {
        self.submitted.load(Ordering::Relaxed)
    }

    pub fn view(&self) -> LogView {
        LogView {
            paths: self.paths.clone(),
            state: self.state.clone(),
            written: self.written.clone(),
            submitted: self.submitted.clone(),
        }
    }

    pub(crate) fn take_writer(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.writer.take()
    }

    pub fn append(&self, events: Vec<SessionEvent>, earlier: Vec<SessionEvent>) -> io::Result<()> {
        if events.is_empty() && earlier.is_empty() {
            return Ok(());
        }
        let added = (events.len() + earlier.len()) as u64;
        self.submitted.fetch_add(added, Ordering::Relaxed);
        self.send(LogMsg::Append { events, earlier })
    }

    pub fn seal(
        &self,
        snapshot: SessionSnapshot,
        journal_len: u64,
        last_seq: u64,
    ) -> io::Result<()> {
        self.send(LogMsg::Seal {
            snapshot: Box::new(snapshot),
            journal_len,
            last_seq,
        })
    }

    pub fn unseal(&self) -> io::Result<()> {
        self.send(LogMsg::Unseal { done: None })
    }

    /// Queues an unseal and returns where its outcome arrives, without waiting for it: the
    /// caller (the session actor) must keep processing records meanwhile. Messages are handled
    /// in order, so appends queued after this one land in an unsealed log.
    pub fn unseal_notify(&self) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (tx, rx) = oneshot::channel();
        self.send(LogMsg::Unseal { done: Some(tx) })?;
        Ok(rx)
    }

    pub fn reset(&self) -> io::Result<()> {
        self.submitted.store(0, Ordering::Relaxed);
        self.send(LogMsg::Reset)
    }

    /// Marks an unsealed log complete: its owner has caught up with the journal.
    pub fn mark_ready(&self) -> io::Result<()> {
        self.send(LogMsg::Ready)
    }

    /// Marks the log failed: reads answer with the reason until the next start rebuilds it.
    pub fn fail(&self, reason: String) -> io::Result<()> {
        self.send(LogMsg::Fail(reason))
    }

    #[cfg(test)]
    pub async fn page_back<F>(
        &self,
        before: Option<u64>,
        limit: usize,
        max_bytes: Option<u64>,
        filter: F,
    ) -> io::Result<(Vec<SessionEvent>, bool)>
    where
        F: Fn(&SessionEvent) -> bool + Send + Sync + 'static,
    {
        self.view()
            .page(
                LogFile::Events,
                before,
                limit,
                max_bytes.unwrap_or(u64::MAX),
                move |e| filter(&e).then_some(e),
            )
            .await
    }

    #[cfg(test)]
    pub fn stream_between(
        &self,
        after: Option<u64>,
        below: u64,
    ) -> mpsc::Receiver<io::Result<SessionEvent>> {
        stream_events(self.paths.events.clone(), after, below.checked_sub(1))
    }

    fn send(&self, msg: LogMsg) -> io::Result<()> {
        self.tx
            .send(msg)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "log writer stopped"))
    }
}

/// See [`LogView::stream`].
fn stream_events(
    path: PathBuf,
    after: Option<u64>,
    through: Option<u64>,
) -> mpsc::Receiver<io::Result<SessionEvent>> {
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(async move {
        let start = tokio::task::spawn_blocking({
            let path = path.clone();
            move || stream_start(&path, after)
        })
        .await;
        let (start, good) = match start {
            Ok(Ok(bounds)) => bounds,
            Ok(Err(e)) => {
                let _ = tx.send(Err(e)).await; // the reader may be gone
                return;
            }
            Err(e) => {
                let _ = tx.send(Err(io::Error::other(e.to_string()))).await; // the reader may be gone
                return;
            }
        };
        let mut file = match OpenOptions::new().read(true).open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return,
            Err(e) => {
                let _ = tx.send(Err(e)).await; // the reader may be gone
                return;
            }
        };
        if let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await {
            let _ = tx.send(Err(e)).await; // the reader may be gone
            return;
        }
        let mut reader = BufReader::new(file);
        let mut buf = String::new();
        let mut offset = start;
        loop {
            buf.clear();
            let n = match reader.read_line(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n as u64,
                Err(e) => {
                    let _ = tx.send(Err(e)).await; // the reader may be gone
                    return;
                }
            };
            offset += n;
            if offset > good {
                break;
            }
            crate::stats::log_bytes_read(n);
            let event: SessionEvent =
                match serde_json::from_slice(buf.trim_end_matches('\n').as_bytes()) {
                    Ok(e) => e,
                    Err(e) => {
                        let _ = tx.send(Err(json_err(e))).await; // the reader may be gone
                        return;
                    }
                };
            if through.is_some_and(|t| event.cursor > t) {
                break;
            }
            if after.is_none_or(|a| event.cursor > a) && tx.send(Ok(event)).await.is_err() {
                return;
            }
        }
    });
    rx
}

/// The snapshot a sealed log holds for a finished session, read from `meta.json` alone: `None`
/// unless the log was written under `build` and sealed at `journal_len` bytes of the journal.
/// Takes no lock and changes nothing; a writer that opens the log later recovers it as usual.
pub(crate) async fn sealed_snapshot(
    session_dir: &Path,
    build: &str,
    journal_len: u64,
) -> Option<SessionSnapshot> {
    let path = Paths::new(session_dir).meta;
    let bytes = fs::read(&path).await.ok()?;
    let meta: Meta = serde_json::from_slice(&bytes).ok()?;
    if meta.format != FORMAT || meta.build != build {
        return None;
    }
    let seal = meta.seal?;
    (seal.journal_len == journal_len).then_some(*seal.snapshot)
}

struct LogWriter {
    paths: Paths,
    meta: Meta,
    state: watch::Sender<LogState>,
    written: watch::Sender<u64>,
    written_count: u64,
    submitted: Arc<AtomicU64>,
    /// Exclusive lock on the log directory, held for the writer's life: the next writer of this
    /// log waits for it (see [`acquire_writer`]).
    _lock: std::fs::File,
}

impl LogWriter {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<LogMsg>) {
        while let Some(msg) = rx.recv().await {
            let res = match msg {
                LogMsg::Append { events, earlier } => self.append(events, earlier).await,
                LogMsg::Seal {
                    snapshot,
                    journal_len,
                    last_seq,
                } => self.seal(*snapshot, journal_len, last_seq).await,
                LogMsg::Unseal { done } => {
                    let res = self.unseal().await;
                    if let Some(done) = done {
                        let _ = done.send(
                            res.as_ref()
                                .map(|_| ())
                                .map_err(|e| io::Error::new(e.kind(), e.to_string())),
                        );
                    }
                    res
                }
                LogMsg::Ready => {
                    if matches!(*self.state.borrow(), LogState::Building) {
                        let _ = self.state.send_replace(LogState::Ready);
                    }
                    Ok(())
                }
                LogMsg::Fail(reason) => Err(io::Error::other(reason)),
                LogMsg::Reset => self.reset().await,
            };
            if let Err(e) = res {
                let _ = self.state.send_replace(LogState::Failed(e.to_string()));
                break;
            }
        }
    }

    async fn append(
        &mut self,
        events: Vec<SessionEvent>,
        earlier: Vec<SessionEvent>,
    ) -> io::Result<()> {
        let mut wrote = 0u64;
        if !events.is_empty() {
            append_lines(&self.paths.events, &events).await?;
            wrote += events.len() as u64;
        }
        if !earlier.is_empty() {
            append_lines(&self.paths.earlier, &earlier).await?;
            wrote += earlier.len() as u64;
        }
        if wrote > 0 {
            self.written_count += wrote;
            let _ = self.written.send_replace(self.written_count);
        }
        Ok(())
    }

    async fn seal(
        &mut self,
        snapshot: SessionSnapshot,
        journal_len: u64,
        last_seq: u64,
    ) -> io::Result<()> {
        self.sync_file(&self.paths.events).await?;
        self.sync_file(&self.paths.earlier).await?;
        self.meta.seal = Some(Seal {
            journal_len,
            last_seq,
            snapshot: Box::new(snapshot),
        });
        tokio::task::spawn_blocking({
            let meta_path = self.paths.meta.clone();
            let meta = self.meta.clone();
            move || write_meta(&meta_path, &meta)
        })
        .await
        .expect("spawn_blocking panicked")?;
        let _ = self.state.send_replace(LogState::Ready);
        Ok(())
    }

    async fn unseal(&mut self) -> io::Result<()> {
        if self.meta.seal.is_some() {
            self.meta.seal = None;
            tokio::task::spawn_blocking({
                let meta_path = self.paths.meta.clone();
                let meta = self.meta.clone();
                move || write_meta(&meta_path, &meta)
            })
            .await
            .expect("spawn_blocking panicked")?;
        }
        let _ = self.state.send_replace(LogState::Ready);
        Ok(())
    }

    async fn reset(&mut self) -> io::Result<()> {
        self.meta = Meta {
            format: FORMAT,
            build: self.meta.build.clone(),
            seal: None,
        };
        truncate(&self.paths.events).await?;
        truncate(&self.paths.earlier).await?;
        tokio::task::spawn_blocking({
            let meta_path = self.paths.meta.clone();
            let meta = self.meta.clone();
            move || write_meta(&meta_path, &meta)
        })
        .await
        .expect("spawn_blocking panicked")?;
        self.written_count = 0;
        self.submitted.store(0, Ordering::Relaxed);
        let _ = self.written.send_replace(0);
        let _ = self.state.send_replace(LogState::Building);
        Ok(())
    }

    async fn sync_file(&self, path: &Path) -> io::Result<()> {
        if let Ok(f) = OpenOptions::new().read(true).open(path).await {
            f.sync_all().await
        } else {
            Ok(())
        }
    }
}

/// Takes the log's writer role: blocks until the previous writer (if any) has finished, then
/// holds an exclusive lock on the log until the returned file is dropped.
fn acquire_writer(paths: &Paths) -> io::Result<std::fs::File> {
    std::fs::create_dir_all(&paths.dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(paths.dir.join(LOCK_FILE))?;
    lock.lock()?;
    Ok(lock)
}

fn recover(
    paths: &Paths,
    build: &str,
    journal_len: u64,
    last_seq: Option<u64>,
    lock: std::fs::File,
) -> io::Result<RecoverOutcome> {
    let mut wrote_meta = false;
    let mut meta = match read_meta(&paths.meta)? {
        Some(m) => m,
        None => {
            wrote_meta = true;
            Meta {
                format: FORMAT,
                build: build.to_string(),
                seal: None,
            }
        }
    };
    if meta.format != FORMAT || meta.build != build {
        reset_files(paths)?;
        meta = Meta {
            format: FORMAT,
            build: build.to_string(),
            seal: None,
        };
        wrote_meta = true;
    }
    // Only a seal that matches the journal makes a log complete. Any other log is a partial
    // derivation until its owner (the actor catching up, or an offline build) marks it ready:
    // reads must not see a prefix as the whole.
    let mut state = LogState::Building;
    if let Some(seal) = meta.seal.as_ref() {
        if seal.journal_len == journal_len {
            state = LogState::Ready;
        } else if seal.journal_len > journal_len {
            // The journal is shorter than the one the log was sealed for: it was replaced or
            // rewritten, so the log derives from something that no longer exists, and an
            // offline build (which only appends beyond the log's last cursor) would keep it.
            reset_files(paths)?;
            meta.seal = None;
            wrote_meta = true;
        } else {
            meta.seal = None;
            wrote_meta = true;
        }
    }
    let events_written = recover_file(&paths.events, last_seq)?;
    let earlier_written = recover_file(&paths.earlier, last_seq)?;
    if wrote_meta {
        write_meta(&paths.meta, &meta)?;
    }
    Ok(RecoverOutcome {
        meta,
        state,
        written: events_written + earlier_written,
        lock,
    })
}

fn recover_file(path: &Path, last_seq: Option<u64>) -> io::Result<u64> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let len = f.metadata()?.len();
    let mut good = sorted_ndjson::complete_len(&mut f, len)?;
    if good != len {
        f.set_len(good)?;
        f.sync_all()?;
    }
    if let Some(max_seq) = last_seq {
        if max_seq > 0 && good > 0 {
            let cutoff =
                sorted_ndjson::seek_sorted(path, CURSOR_PREFIX, (max_seq + 1) << 16, good)?;
            if cutoff < good {
                f.set_len(cutoff)?;
                f.sync_all()?;
                good = cutoff;
            }
        }
    }
    drop(f);
    count_lines(path, good)
}

fn reset_files(paths: &Paths) -> io::Result<()> {
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.events)?;
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.earlier)?;
    Ok(())
}

fn read_meta(path: &Path) -> io::Result<Option<Meta>> {
    match std::fs::File::open(path) {
        Ok(f) => Ok(Some(serde_json::from_reader(f).map_err(json_err)?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_meta(path: &Path, meta: &Meta) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        serde_json::to_writer(&mut f, meta).map_err(json_err)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
    }
    Ok(())
}

fn count_lines(path: &Path, limit: u64) -> io::Result<u64> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    if limit == 0 {
        return Ok(0);
    }
    let mut reader = std::io::BufReader::new(&mut f);
    let mut buf = String::new();
    let mut count = 0u64;
    let mut bytes = 0u64;
    while bytes < limit {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        if bytes <= limit {
            count += 1;
        }
    }
    Ok(count)
}

async fn truncate(path: &Path) -> io::Result<()> {
    fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await?
        .sync_all()
        .await
}

async fn append_lines(path: &Path, events: &[SessionEvent]) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    for event in events {
        let line = serde_json::to_vec(event).map_err(json_err)?;
        f.write_all(&line).await?;
        f.write_all(b"\n").await?;
    }
    f.flush().await
}

fn page_back_blocking<T>(
    path: PathBuf,
    before: Option<u64>,
    limit: usize,
    max_bytes: u64,
    mut pick: impl FnMut(SessionEvent) -> Option<T>,
) -> io::Result<(Vec<T>, bool)> {
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), false)),
        Err(e) => return Err(e),
    };
    let len = file.metadata()?.len();
    let good = sorted_ndjson::complete_len(&mut file, len)?;
    let mut end = match before {
        Some(b) => sorted_ndjson::seek_sorted(&path, CURSOR_PREFIX, b, good)?,
        None => good,
    };
    let mut items = Vec::new();
    let mut bytes = 0u64;
    while end > 0 {
        let start = sorted_ndjson::line_start_before(&mut file, end)?;
        let line = sorted_ndjson::read_range(&mut file, start, end - 1)?;
        end = start;
        bytes += (line.len() + 1) as u64;
        crate::stats::log_bytes_read((line.len() + 1) as u64);
        let event: SessionEvent = serde_json::from_slice(&line).map_err(json_err)?;
        if let Some(item) = pick(event) {
            items.push(item);
            if items.len() >= limit || (bytes >= max_bytes && !items.is_empty()) {
                break;
            }
        }
        if bytes >= max_bytes && !items.is_empty() {
            break;
        }
    }
    items.reverse();
    Ok((items, end > 0))
}

/// The records of the file at `path` that `scan` selects, from its complete records only: a torn
/// last line is not one. A missing file has none. The records are read lazily.
pub(crate) fn open_scan(path: &Path, scan: Scan) -> io::Result<Records> {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Box::new(std::iter::empty())),
        Err(e) => return Err(e),
    };
    let len = file.metadata()?.len();
    let good = sorted_ndjson::complete_len(&mut file, len)?;
    match scan {
        Scan::Backward { before } => {
            let end = match before {
                Some(b) => sorted_ndjson::seek_sorted(path, CURSOR_PREFIX, b, good)?,
                None => good,
            };
            Ok(Box::new(BackwardRecords {
                file,
                base: end,
                buf: Vec::new(),
            }))
        }
        Scan::Forward { from } => {
            let start = match from {
                Some(f) => sorted_ndjson::seek_sorted(path, CURSOR_PREFIX, f, good)?,
                None => 0,
            };
            file.seek(SeekFrom::Start(start))?;
            Ok(Box::new(ForwardRecords {
                reader: std::io::BufReader::with_capacity(SCAN_BLOCK, file.take(good - start)),
                line: Vec::new(),
            }))
        }
    }
}

/// The records of a log file in file order. Blank lines are not records.
fn decode(line: &[u8]) -> io::Result<Option<SessionEvent>> {
    let body = line.strip_suffix(b"\n").unwrap_or(line);
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    serde_json::from_slice(body).map(Some).map_err(json_err)
}

struct ForwardRecords {
    reader: std::io::BufReader<std::io::Take<std::fs::File>>,
    line: Vec<u8>,
}

impl Iterator for ForwardRecords {
    type Item = io::Result<(SessionEvent, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line.clear();
            let n = match self.reader.read_until(b'\n', &mut self.line) {
                Ok(0) => return None,
                Ok(n) => n as u64,
                Err(e) => return Some(Err(e)),
            };
            match decode(&self.line) {
                Ok(Some(event)) => {
                    crate::stats::log_bytes_read(n);
                    return Some(Ok((event, n)));
                }
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// The records of a file from `base` backward, read in [`SCAN_BLOCK`] blocks. `buf` holds the
/// bytes of `[base, end)` read and not yet yielded, where `end` is the start of the last record
/// yielded (the scan's end before that). Both ends are line boundaries.
struct BackwardRecords {
    file: std::fs::File,
    base: u64,
    buf: Vec<u8>,
}

impl BackwardRecords {
    /// The last line of the unread bytes, newline included, or `None` at the start of the file.
    fn line(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            let body = self.buf.strip_suffix(b"\n").unwrap_or(&self.buf);
            if let Some(i) = body.iter().rposition(|&b| b == b'\n') {
                return Ok(Some(self.buf.split_off(i + 1)));
            }
            if self.base == 0 {
                return Ok((!self.buf.is_empty()).then(|| std::mem::take(&mut self.buf)));
            }
            self.fill()?;
        }
    }

    /// Prepends the block before `base` to the unread bytes.
    fn fill(&mut self) -> io::Result<()> {
        let n = self.base.min(SCAN_BLOCK as u64);
        let start = self.base - n;
        let mut block = vec![0u8; n as usize];
        self.file.seek(SeekFrom::Start(start))?;
        self.file.read_exact(&mut block)?;
        block.append(&mut self.buf);
        self.buf = block;
        self.base = start;
        Ok(())
    }
}

impl Iterator for BackwardRecords {
    type Item = io::Result<(SessionEvent, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.line() {
                Ok(Some(line)) => line,
                Ok(None) => return None,
                Err(e) => return Some(Err(e)),
            };
            let n = line.len() as u64;
            match decode(&line) {
                Ok(Some(event)) => {
                    crate::stats::log_bytes_read(n);
                    return Some(Ok((event, n)));
                }
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

fn stream_start(path: &Path, after: Option<u64>) -> io::Result<(u64, u64)> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e),
    };
    let len = f.metadata()?.len();
    let good = sorted_ndjson::complete_len(&mut f, len)?;
    let start = match after {
        Some(a) => sorted_ndjson::seek_sorted(path, CURSOR_PREFIX, a + 1, good)?,
        None => 0,
    };
    Ok((start, good))
}

pub(crate) fn json_err(e: serde_json::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("bad record ({:?} at column {})", e.classify(), e.column()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    fn dummy_snapshot() -> SessionSnapshot {
        let desc = wasit_claude::descriptor();
        let meta = SessionMeta {
            id: SessionId("aaaaaaaaaaaa".into()),
            driver: desc.id().clone(),
            cwd: None,
            labels: Default::default(),
            created_at_ms: 0,
            display_name: None,
            command: None,
            conversation: Conversation::Unknown,
            forked_from: None,
        };
        blank_snapshot(&meta, desc.as_ref(), Phase::Starting, true)
    }

    fn ev(cursor: u64, message: &str) -> SessionEvent {
        SessionEvent {
            cursor,
            t: 1,
            kind: SessionEventKind::Notice {
                message: message.to_string(),
                code: NoticeCode::default(),
            },
        }
    }

    async fn wait_written(log: &EventLog, target: u64) {
        let mut w = log.written();
        while *w.borrow() < target {
            w.changed().await.unwrap();
        }
    }

    #[tokio::test]
    async fn torn_tail_truncated_on_recover() {
        let dir = temp();
        let paths = Paths::new(dir.path());
        fs::create_dir_all(&paths.dir).await.unwrap();
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&paths.events)
            .await
            .unwrap();
        let full = [ev(1, "a"), ev(2, "b"), ev(3, "c")];
        for e in full.iter().take(2) {
            let line = serde_json::to_vec(e).unwrap();
            f.write_all(&line).await.unwrap();
            f.write_all(b"\n").await.unwrap();
        }
        let partial = serde_json::to_vec(&full[2]).unwrap();
        f.write_all(&partial[..partial.len() / 2]).await.unwrap();
        drop(f);

        let log = EventLog::open(dir.path(), "build", 0, Some(3))
            .await
            .unwrap();
        let content = std::fs::read_to_string(&paths.events).unwrap();
        assert!(content.contains("\"cursor\":1"));
        assert!(content.contains("\"cursor\":2"));
        assert!(!content.contains("\"cursor\":3"));
        assert_eq!(*log.written.borrow(), 2);
    }

    #[tokio::test]
    async fn truncation_at_every_offset_preserves_prefix() {
        let full_lines: Vec<_> = (1..=4)
            .map(|i| serde_json::to_string(&ev(i, "x")).unwrap() + "\n")
            .collect();
        let full: String = full_lines.clone().into_iter().collect();
        for cut in 0..=full.len() {
            let dir = temp();
            let paths = Paths::new(dir.path());
            fs::create_dir_all(&paths.dir).await.unwrap();
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&paths.events)
                .await
                .unwrap();
            f.write_all(full.as_bytes()).await.unwrap();
            f.set_len(cut as u64).await.unwrap();
            drop(f);
            let _ = EventLog::open(dir.path(), "build", 0, Some(4))
                .await
                .unwrap();
            let recovered = std::fs::read_to_string(&paths.events).unwrap();
            let prefix = &full[..cut.min(full.len())];
            assert!(prefix.starts_with(&recovered));
        }
    }

    #[tokio::test]
    async fn key_mismatch_resets_log() {
        let dir = temp();
        let paths = Paths::new(dir.path());
        fs::create_dir_all(&paths.dir).await.unwrap();
        write_meta(
            &paths.meta,
            &Meta {
                format: FORMAT,
                build: "old".into(),
                seal: None,
            },
        )
        .unwrap();
        std::fs::write(
            &paths.events,
            serde_json::to_string(&ev(1, "a")).unwrap() + "\n",
        )
        .unwrap();
        let log = EventLog::open(dir.path(), "new", 0, Some(1)).await.unwrap();
        assert!(matches!(*log.state.borrow(), LogState::Building));
        assert_eq!(std::fs::read_to_string(&paths.events).unwrap(), "");
    }

    #[tokio::test]
    async fn seal_validity_checks_journal_len() {
        let dir = temp();
        let paths = Paths::new(dir.path());
        fs::create_dir_all(&paths.dir).await.unwrap();
        let snapshot = dummy_snapshot();
        write_meta(
            &paths.meta,
            &Meta {
                format: FORMAT,
                build: "b".into(),
                seal: Some(Seal {
                    journal_len: 10,
                    last_seq: 2,
                    snapshot: Box::new(snapshot),
                }),
            },
        )
        .unwrap();
        let log = EventLog::open(dir.path(), "b", 11, Some(2)).await.unwrap();
        assert!(matches!(*log.state.borrow(), LogState::Building));
        let meta = read_meta(&paths.meta).unwrap().unwrap();
        assert!(meta.seal.is_none());
    }

    #[tokio::test]
    async fn truncate_back_when_journal_shorter() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(2)).await.unwrap();
        log.append(
            vec![ev(event_cursor(1, 0), "a"), ev(event_cursor(3, 0), "b")],
            Vec::new(),
        )
        .unwrap();
        wait_written(&log, 2).await;
        drop(log);
        let reopened = EventLog::open(dir.path(), "b", 0, Some(2)).await.unwrap();
        let content = std::fs::read_to_string(Paths::new(dir.path()).events).unwrap();
        assert!(content.contains(&format!("{}", event_cursor(1, 0))));
        assert!(!content.contains(&format!("{}", event_cursor(3, 0))));
        assert_eq!(*reopened.written.borrow(), 1);
    }

    #[tokio::test]
    async fn page_back_honours_limits_and_bytes() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(10)).await.unwrap();
        let events: Vec<_> = (1..=6)
            .map(|i| ev(event_cursor(i, 0), &format!("e{i}")))
            .collect();
        log.append(events.clone(), Vec::new()).unwrap();
        wait_written(&log, events.len() as u64).await;
        let (page, has_more) = log
            .page_back(None, 3, Some(200), |e| e.cursor >= event_cursor(2, 0))
            .await
            .unwrap();
        assert_eq!(page.len(), 3);
        assert!(has_more);
        let (page_bytes, has_more_bytes) = log
            .page_back(
                None,
                10,
                Some((page[0].cursor.to_string().len() as u64) + 5),
                |_| true,
            )
            .await
            .unwrap();
        assert!(!page_bytes.is_empty());
        assert!(has_more_bytes);
    }

    #[tokio::test]
    async fn stream_between_respects_bounds() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(10)).await.unwrap();
        let events: Vec<_> = (1..=5)
            .map(|i| ev(event_cursor(i, 0), &format!("e{i}")))
            .collect();
        log.append(events.clone(), Vec::new()).unwrap();
        wait_written(&log, events.len() as u64).await;
        let mut rx = log.stream_between(Some(event_cursor(2, 0)), event_cursor(5, 0));
        let mut got = Vec::new();
        while let Some(item) = rx.recv().await {
            got.push(item.unwrap().cursor);
        }
        assert_eq!(got, vec![event_cursor(3, 0), event_cursor(4, 0)]);
    }

    fn records(path: &Path, scan: Scan) -> Vec<u64> {
        open_scan(path, scan)
            .unwrap()
            .map(|r| r.unwrap().0.cursor)
            .collect()
    }

    #[test]
    fn scans_read_every_record_in_both_directions_across_blocks() {
        let dir = temp();
        let path = Paths::new(dir.path()).events;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = String::new();
        for i in 1..=3000u64 {
            body += &(serde_json::to_string(&ev(i, &"x".repeat(100))).unwrap() + "\n");
        }
        // Blank lines are skipped, and the file is several blocks long.
        body = body.replacen("\n", "\n\n", 5);
        std::fs::write(&path, &body).unwrap();
        assert!(body.len() > 4 * SCAN_BLOCK);

        let forward = records(&path, Scan::Forward { from: None });
        assert_eq!(forward, (1..=3000).collect::<Vec<_>>());
        let backward = records(&path, Scan::Backward { before: None });
        assert_eq!(backward, (1..=3000).rev().collect::<Vec<_>>());
        assert_eq!(
            records(&path, Scan::Backward { before: Some(10) }),
            (1..10).rev().collect::<Vec<_>>()
        );
        assert_eq!(
            records(&path, Scan::Forward { from: Some(2990) }),
            (2990..=3000).collect::<Vec<_>>()
        );
        assert!(records(&path, Scan::Forward { from: Some(3001) }).is_empty());
    }

    #[test]
    fn scans_skip_a_torn_last_line() {
        let dir = temp();
        let path = Paths::new(dir.path()).events;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut body = String::new();
        for i in 1..=3u64 {
            body += &(serde_json::to_string(&ev(i, "ok")).unwrap() + "\n");
        }
        let torn = serde_json::to_string(&ev(4, "torn")).unwrap();
        body += &torn[..torn.len() / 2];
        std::fs::write(&path, &body).unwrap();
        assert_eq!(records(&path, Scan::Forward { from: None }), [1, 2, 3]);
        assert_eq!(records(&path, Scan::Backward { before: None }), [3, 2, 1]);
        assert!(records(
            &Paths::new(dir.path()).earlier,
            Scan::Backward { before: None }
        )
        .is_empty());
    }

    #[tokio::test]
    async fn lines_start_with_cursor_prefix() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(1)).await.unwrap();
        log.append(vec![ev(5, "x")], Vec::new()).unwrap();
        wait_written(&log, 1).await;
        let contents = std::fs::read_to_string(Paths::new(dir.path()).events).unwrap();
        assert!(contents.trim_start().starts_with("{\"cursor\":"));
    }
}
