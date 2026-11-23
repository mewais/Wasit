//! The writer thread: the one owner of a sink's file. Batches records, writes when a batch is
//! full or the queue is empty, rotates by size, reports drops and write failures.
//!
//! The file system is reached only through [`Target`]; [`FsTarget`] is the real one. A test
//! gives the same writer another target (for instance one whose writes block) — every decision
//! the writer makes is the same code either way.

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::Duration;

use crate::internal::SinkEvent;
use crate::sink::{self, mono_ms, wall_ms, DropRange, FileMode, Msg, Shared};
use crate::value::ErrorInfo;
use crate::{private, reader};

/// Where a writer's bytes go: a directory of named files.
pub trait Target: Send + 'static {
    type File: Write + Send;

    /// Opens `name` for appending, creating it (and its directory) when missing; returns the
    /// file and its current length.
    fn open(&mut self, name: &str) -> io::Result<(Self::File, u64)>;
    /// The length of `name`, `None` when it does not exist.
    fn len_of(&mut self, name: &str) -> io::Result<Option<u64>>;
    /// The first line of `name` (without the newline), `None` when it does not exist.
    fn first_line(&mut self, name: &str) -> io::Result<Option<Vec<u8>>>;
    /// Renames `from` to `to`, replacing `to`.
    fn rename(&mut self, from: &str, to: &str) -> io::Result<()>;
    fn remove(&mut self, name: &str) -> io::Result<()>;
    /// Makes what was written to `file` durable (rotation and shutdown only, never per record).
    fn sync(&mut self, file: &mut Self::File) -> io::Result<()>;
}

/// Files in a directory. On Unix the directory is `0700` and the files `0600`, and what an older
/// build left with wider permissions is tightened when it is opened (see [`private`]).
pub struct FsTarget {
    dir: PathBuf,
}

impl FsTarget {
    pub fn new(dir: impl Into<PathBuf>) -> FsTarget {
        FsTarget { dir: dir.into() }
    }
}

impl Target for FsTarget {
    type File = std::fs::File;

    fn open(&mut self, name: &str) -> io::Result<(std::fs::File, u64)> {
        private::create_dir_all(&self.dir)?;
        let f = private::open(&self.dir.join(name), |o| {
            o.append(true);
        })?;
        let len = f.metadata()?.len();
        Ok((f, len))
    }

    fn len_of(&mut self, name: &str) -> io::Result<Option<u64>> {
        match std::fs::metadata(self.dir.join(name)) {
            Ok(m) => Ok(Some(m.len())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn first_line(&mut self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match std::fs::File::open(self.dir.join(name)) {
            Ok(mut f) => reader::first_line(&mut f).map(Some),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        std::fs::rename(self.dir.join(from), self.dir.join(to))
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        std::fs::remove_file(self.dir.join(name))
    }

    fn sync(&mut self, file: &mut std::fs::File) -> io::Result<()> {
        file.sync_data()
    }
}

/// Messages taken from the queue before the batch is written.
const BATCH_MSGS: usize = 1024;
/// A batch is written once it holds this many bytes, else when the queue is empty.
const BATCH_BYTES: usize = 256 << 10;
/// Unwritten bytes kept while writes fail; beyond, they are discarded and counted as dropped.
const MAX_PENDING: usize = 1 << 20;

pub(crate) struct Writer<T: Target> {
    shared: Arc<Shared>,
    rx: Receiver<Msg>,
    target: T,
    name: String,
    /// Starts as the configured mode; [`Sink::promote`](crate::Sink::promote) makes it `Owned`.
    mode: FileMode,
    file: Option<T::File>,
    /// Bytes of the current file, including what is batched for it.
    len: u64,
    /// The current file holds a record (a file with only its header is never rotated).
    has_records: bool,
    /// The current file's generation number (its header's `gen`).
    serial: u64,
    buf: Vec<u8>,
    buf_user: u64,
    buf_all: u64,
    buf_first: u64,
    buf_last: u64,
    /// Error kinds reported since the last successful write.
    reported: Vec<io::ErrorKind>,
    stash: Option<Msg>,
    next_tick: Option<u64>,
}

enum Wait {
    Msg(Msg),
    Tick,
    Closed,
}

/// Marks the writer done (also when it panics), so nobody waits for it.
struct DoneGuard(Arc<Shared>);

impl Drop for DoneGuard {
    fn drop(&mut self) {
        sink::lock(&self.0.progress).done = true;
        self.0.progress_cond.notify_all();
    }
}

impl<T: Target> Writer<T> {
    pub(crate) fn new(shared: Arc<Shared>, rx: Receiver<Msg>, target: T) -> Self {
        Writer {
            name: shared.config.file.clone(),
            mode: shared.config.mode,
            shared,
            rx,
            target,
            file: None,
            len: 0,
            has_records: false,
            serial: 0,
            buf: Vec::with_capacity(BATCH_BYTES),
            buf_user: 0,
            buf_all: 0,
            buf_first: 0,
            buf_last: 0,
            reported: Vec::new(),
            stash: None,
            next_tick: None,
        }
    }

    pub(crate) fn run(mut self) {
        let _done = DoneGuard(self.shared.clone());
        self.rotate_watched();
        // The file is opened (and its header written) before the first record is taken, so an
        // open or a write that blocks holds back no more than the queue.
        // (A shared file is opened for each record instead, and its header written by the
        // process that finds it empty.)
        if self.mode == FileMode::Owned {
            if let Err(e) = self.open(None) {
                self.report(&e);
            }
        }
        loop {
            self.tick();
            let first = match self.stash.take() {
                Some(m) => Some(m),
                None => match self.wait() {
                    Wait::Msg(m) => Some(m),
                    Wait::Tick => None,
                    Wait::Closed => break,
                },
            };
            if let Some(m) = first {
                self.handle(m);
                for _ in 0..BATCH_MSGS {
                    match self.rx.try_recv() {
                        Ok(m) => self.handle(m),
                        Err(_) => break,
                    }
                }
            }
            self.tick();
            if self.idle() {
                break;
            }
        }
    }

    fn wait(&mut self) -> Wait {
        match self.next_tick {
            Some(at) => {
                let now = mono_ms();
                if now >= at {
                    return Wait::Tick;
                }
                // The deadline is the next minute boundary, when the per-minute summaries are
                // due: a schedule, not a poll.
                match self.rx.recv_timeout(Duration::from_millis(at - now)) {
                    Ok(m) => Wait::Msg(m),
                    Err(RecvTimeoutError::Timeout) => Wait::Tick,
                    Err(RecvTimeoutError::Disconnected) => Wait::Closed,
                }
            }
            None => match self.rx.recv() {
                Ok(m) => Wait::Msg(m),
                Err(_) => Wait::Closed,
            },
        }
    }

    fn handle(&mut self, m: Msg) {
        if let Msg::Rec {
            seq,
            internal,
            line,
        } = m
        {
            self.add(seq, internal, &line);
        }
    }

    /// Arms the minute tick when something waits for it; queues the summaries when it is due.
    fn tick(&mut self) {
        let now = mono_ms();
        let watching = self.shared.config.rotate_shared.is_some();
        if self.next_tick.is_none() && (watching || self.shared.periodic.load(Ordering::Acquire)) {
            self.next_tick = Some((now / 60_000 + 1) * 60_000);
        }
        if self.next_tick.is_some_and(|at| now >= at) {
            self.next_tick = None;
            self.shared.periodic.store(false, Ordering::Release);
            {
                let mut st = self.shared.lock_state();
                self.shared.drain_periodic(&mut st);
            }
            self.rotate_watched();
        }
    }

    /// Called when a batch is done. With the queue empty (checked under the state lock, so
    /// every seq assigned so far has been taken): queue the drop count or, at shutdown, the
    /// last summaries; else write, flush and publish progress. Returns `true` to end.
    fn idle(&mut self) -> bool {
        let mut st = self.shared.lock_state();
        if let Ok(m) = self.rx.try_recv() {
            self.stash = Some(m);
            return false;
        }
        if self.shared.queue_drops(&mut st) {
            return false;
        }
        if st.shutdown && self.shared.drain_periodic(&mut st) {
            return false;
        }
        let upto = st.next_seq - 1;
        let shutdown = st.shutdown;
        let promote = st.promote.is_some();
        drop(st);
        if promote {
            self.switch();
        }
        let written = self.write_out() && self.flush_file();
        if written {
            sink::lock(&self.shared.progress).flushed_upto = upto;
            self.shared.progress_cond.notify_all();
        }
        if shutdown {
            // What could not be written by now never will be: count it.
            self.shared
                .stats
                .dropped
                .fetch_add(self.buf_user, Ordering::Relaxed);
            if let Some(f) = self.file.as_mut() {
                if let Err(e) = self.target.sync(f) {
                    // Nothing can record it any more: the sink is closed.
                    drop(e);
                }
            }
            return true;
        }
        false
    }

    fn add(&mut self, seq: u64, internal: bool, line: &str) {
        if seq >= self.shared.promote_at.load(Ordering::Acquire) {
            self.switch();
        }
        if let FileMode::SharedAppend { cap } = self.mode {
            self.add_shared(seq, internal, line, cap);
            return;
        }
        if self.file.is_none() {
            if let Err(e) = self.open(None) {
                self.report(&e);
            }
        }
        let n = line.len() as u64 + 1;
        if self.file.is_some() && self.has_records && self.len + n > self.shared.config.max_bytes {
            self.rotate();
        }
        if self.buf_all == 0 {
            self.buf_first = seq;
        }
        self.buf_last = seq;
        self.buf_all += 1;
        if !internal {
            self.buf_user += 1;
        }
        self.buf.extend_from_slice(line.as_bytes());
        self.buf.push(b'\n');
        self.len += n;
        self.has_records = true;
        if self.buf.len() >= BATCH_BYTES {
            self.write_out();
        }
    }

    /// [`FileMode::SharedAppend`]: the record goes to the shared file in one `write`, at once
    /// (never batched, never buffered), or is skipped — and counted as dropped — when the file
    /// already holds `cap` bytes. A record whose write fails is counted the same way and the
    /// failure reported; the file is opened again for the next record.
    fn add_shared(&mut self, seq: u64, internal: bool, line: &str, cap: u64) {
        match self.append_shared(line, cap) {
            Ok(true) => {
                if !internal {
                    self.shared.stats.written.fetch_add(1, Ordering::Relaxed);
                }
                self.reported.clear();
            }
            Ok(false) => self.skip_shared(seq, internal),
            Err(e) => {
                self.report(&e);
                self.skip_shared(seq, internal);
            }
        }
    }

    /// A shared-file record that was not written. A user record is counted and its seq range
    /// reported by a `diag.dropped` record once the file takes records again; a sink-internal
    /// record (a summary or a drop count itself) is not, or a full file would make the writer
    /// queue a drop count for every drop count it cannot write.
    fn skip_shared(&mut self, seq: u64, internal: bool) {
        if internal {
            return;
        }
        self.shared.stats.dropped.fetch_add(1, Ordering::Relaxed);
        let mut st = self.shared.lock_state();
        DropRange::add(&mut st.drops, 1, seq, seq);
    }

    /// Writes `line` to the shared file; `false` when the file is over `cap` (nothing written).
    fn append_shared(&mut self, line: &str, cap: u64) -> io::Result<bool> {
        let (mut file, len) = self.target.open(&self.name)?;
        if len >= cap {
            return Ok(false);
        }
        let mut buf = Vec::with_capacity(line.len() + 1 + if len == 0 { 256 } else { 0 });
        if len == 0 {
            // A file that is empty (new, or rotated away by the daemon) starts with a header.
            let serial = self.next_serial()?;
            buf.extend_from_slice(self.header(serial).as_bytes());
        }
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        file.write_all(&buf)?;
        Ok(true)
    }

    /// The generation of a new file: one after the generation in `<file>.1`.
    fn next_serial(&mut self) -> io::Result<u64> {
        let older = self.target.first_line(&format!("{}.1", self.name))?;
        Ok(older
            .as_deref()
            .and_then(reader::header_gen)
            .map_or(1, |g| g + 1))
    }

    fn header(&self, serial: u64) -> String {
        format!(
            "{{\"diag_file\":1,\"gen\":{serial},\"b\":\"{}\",\"src\":{},\"build\":{},\"pid\":{},\"opened_t\":{}}}\n",
            self.shared.boot,
            crate::json::quoted(self.shared.config.role.as_str().unwrap_or("other")),
            crate::json::quoted(self.shared.config.build.as_str().unwrap_or("other")),
            std::process::id(),
            wall_ms(),
        )
    }

    /// The daemon's duty ([`SinkConfig::rotate_shared`](crate::SinkConfig::rotate_shared)):
    /// rotate the shared file when it is over its limit. Writers that have it open for a record
    /// at that moment finish into the rotated file; the next record opens a new one.
    fn rotate_watched(&mut self) {
        let Some(w) = self.shared.config.rotate_shared.clone() else {
            return;
        };
        match self.target.len_of(&w.file) {
            Ok(Some(len)) if len > w.max_bytes => {
                if let Err(e) = self.shift_named(&w.file, w.generations) {
                    self.report(&e);
                }
            }
            Ok(_) => {}
            Err(e) => self.report(&e),
        }
    }

    /// Opens the current file and writes its header. `serial`: the generation of a new file
    /// after rotation; `None` to continue the existing file (or start one).
    fn open(&mut self, serial: Option<u64>) -> io::Result<()> {
        let (mut file, existing) = self.target.open(&self.name)?;
        let serial = match serial {
            Some(s) => s,
            None if existing > 0 => {
                let first = self.target.first_line(&self.name)?;
                match first.as_deref().and_then(reader::header_gen) {
                    Some(g) => g,
                    None => {
                        // Not one of our files (or damaged): keep it as a rotated generation.
                        drop(file);
                        self.shift()?;
                        return self.open(None);
                    }
                }
            }
            None => self.next_serial()?,
        };
        let header = self.header(serial);
        file.write_all(header.as_bytes())?;
        self.len = existing + header.len() as u64 + self.buf.len() as u64;
        self.has_records = existing > 0;
        self.serial = serial;
        self.file = Some(file);
        Ok(())
    }

    /// `<file>.k` → `<file>.k+1` (the oldest replaced), `<file>` → `<file>.1`.
    fn shift(&mut self) -> io::Result<()> {
        let name = self.name.clone();
        self.shift_named(&name, self.shared.config.generations)
    }

    /// [`Writer::shift`] for any file of the directory.
    fn shift_named(&mut self, name: &str, gens: u32) -> io::Result<()> {
        if gens == 0 {
            return ignore_missing(self.target.remove(name));
        }
        for k in (1..gens).rev() {
            let from = format!("{name}.{k}");
            let to = format!("{name}.{}", k + 1);
            ignore_missing(self.target.rename(&from, &to))?;
        }
        ignore_missing(self.target.rename(name, &format!("{name}.1")))
    }

    fn rotate(&mut self) {
        if !self.write_out() {
            // Keep the batch for the current file; rotation is retried with the next record.
            return;
        }
        if let Some(mut f) = self.file.take() {
            if let Err(e) = self.target.sync(&mut f) {
                self.report(&e);
            }
        }
        let next = self.serial + 1;
        let opened = match self.shift() {
            Ok(()) => self.open(Some(next)),
            Err(e) => {
                // Keep appending to the current file rather than lose records.
                self.report(&e);
                self.open(None)
            }
        };
        if let Err(e) = opened {
            self.report(&e);
        }
    }

    /// Switches to the promoted file.
    fn switch(&mut self) {
        let next = {
            let mut st = self.shared.lock_state();
            self.shared.promote_at.store(u64::MAX, Ordering::Release);
            st.promote.take()
        };
        let Some(name) = next else {
            return;
        };
        self.write_out();
        if let Some(mut f) = self.file.take() {
            if let Err(e) = self.target.sync(&mut f) {
                self.report(&e);
            }
        }
        {
            let mut files = sink::lock(&self.shared.files);
            if !files.contains(&name) {
                files.push(name.clone());
            }
        }
        self.name = name;
        // The promoted file is this process's own.
        self.mode = FileMode::Owned;
        if let Err(e) = self.open(None) {
            self.report(&e);
        }
    }

    /// Writes the batch. `false` when something stays unwritten.
    fn write_out(&mut self) -> bool {
        if self.buf.is_empty() {
            return true;
        }
        if self.file.is_none() {
            if let Err(e) = self.open(None) {
                self.report(&e);
                self.bound();
                return false;
            }
        }
        let Some(f) = self.file.as_mut() else {
            return false;
        };
        match f.write_all(&self.buf) {
            Ok(()) => {
                self.shared
                    .stats
                    .written
                    .fetch_add(self.buf_user, Ordering::Relaxed);
                self.buf.clear();
                self.buf_user = 0;
                self.buf_all = 0;
                self.reported.clear();
                true
            }
            Err(e) => {
                self.report(&e);
                self.bound();
                false
            }
        }
    }

    fn flush_file(&mut self) -> bool {
        match self.file.as_mut().map(|f| f.flush()) {
            Some(Err(e)) => {
                self.report(&e);
                false
            }
            _ => true,
        }
    }

    /// Discards the batch once it exceeds [`MAX_PENDING`], counting it as dropped.
    fn bound(&mut self) {
        if self.buf.len() <= MAX_PENDING {
            return;
        }
        let mut st = self.shared.lock_state();
        DropRange::add(&mut st.drops, self.buf_all, self.buf_first, self.buf_last);
        drop(st);
        self.shared
            .stats
            .dropped
            .fetch_add(self.buf_user, Ordering::Relaxed);
        self.buf.clear();
        self.buf_user = 0;
        self.buf_all = 0;
    }

    /// Queues `diag.write_failed`, once per error kind until a write succeeds again.
    fn report(&mut self, e: &io::Error) {
        if self.reported.contains(&e.kind()) {
            return;
        }
        self.reported.push(e.kind());
        let ev = SinkEvent::WriteFailed {
            error: ErrorInfo::io(e),
        };
        let mut st = self.shared.lock_state();
        if !self.shared.queue(&mut st, &ev, true) {
            // The queue is full: the failure is retried with the next batch and reported then
            // (its kind is forgotten so the next attempt reports it).
            self.reported.pop();
        }
    }
}

fn ignore_missing(r: io::Result<()>) -> io::Result<()> {
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}
