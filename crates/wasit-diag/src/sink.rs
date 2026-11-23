//! The sink (design §6.2, §6.3): `emit` serializes a record, pushes it into the ring and the
//! tail broadcast and `try_send`s it into a bounded queue; one writer thread (`writer.rs`) owns
//! the file. Nothing an emitter does waits on I/O, on a lock held across I/O, or on a full queue.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use crate::event::{Admit, Budget, DiagEvent, Fields, Level};
use crate::internal::SinkEvent;
use crate::json;
use crate::reader::{self, Chunk, Source};
use crate::ring::{Record, Ring};
use crate::value::{Code, Ident};
use crate::writer::{FsTarget, Target, Writer};

/// Environment variable overriding [`SinkConfig::queue`] in [`install`](crate::install), for
/// development and tests (e.g. `WASIT_DIAG_QUEUE=16` to make back-pressure visible quickly).
pub const QUEUE_ENV: &str = "WASIT_DIAG_QUEUE";

/// Name of the writer thread (the panic hook never records or flushes from it).
pub const WRITER_THREAD: &str = "diag-writer";

/// Capacity of the tail broadcast; a subscriber that falls further behind sees `Lagged`.
const TAIL_CAPACITY: usize = 1024;

/// Where and how a process writes its records.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// The record's `src` (`app`, `hub`, `daemon`, `bridge`, `stop`, `askpass`, `shim`, …).
    pub role: Code,
    /// The build written in each file header.
    pub build: Ident,
    /// The directory of the files (created by the writer thread when missing).
    pub dir: PathBuf,
    /// The current file's name, e.g. `daemon.ndjson`; rotated files are `<file>.1` … .
    pub file: String,
    /// A file is rotated before a record would take it past this size.
    pub max_bytes: u64,
    /// Rotated files kept (`<file>.1` ..= `<file>.<generations>`).
    pub generations: u32,
    /// Records the queue between emitters and the writer holds; beyond, records are dropped
    /// and counted.
    pub queue: usize,
}

impl SinkConfig {
    /// Defaults: 8 MiB per file, 3 rotated files, a queue of 8192 records.
    pub fn new(role: Code, build: Ident, dir: impl Into<PathBuf>, file: &str) -> SinkConfig {
        SinkConfig {
            role,
            build,
            dir: dir.into(),
            file: file.to_owned(),
            max_bytes: 8 << 20,
            generations: 3,
            queue: 8192,
        }
    }
}

/// What [`Sink::tail`] returns: the ring's records, and a receiver of every later record.
pub type Tail = (Vec<Arc<Record>>, broadcast::Receiver<Arc<Record>>);

/// Counters of a sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// The queue's capacity.
    pub queue: usize,
    /// Records emitted (past their budget) and given a seq.
    pub emitted: u64,
    /// Of those, records written to a file.
    pub written: u64,
    /// Of those, records not written: the queue was full, or their write kept failing.
    pub dropped: u64,
    /// Records not emitted because their variant was over its budget.
    pub suppressed: u64,
}

pub(crate) enum Msg {
    Rec {
        seq: u64,
        internal: bool,
        line: Arc<str>,
    },
    /// Re-evaluate control requests and timers.
    Wake,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DropRange {
    pub(crate) count: u64,
    pub(crate) first: u64,
    pub(crate) last: u64,
}

impl DropRange {
    pub(crate) fn add(this: &mut Option<DropRange>, count: u64, first: u64, last: u64) {
        *this = Some(match *this {
            None => DropRange { count, first, last },
            Some(d) => DropRange {
                count: d.count + count,
                first: d.first.min(first),
                last: d.last.max(last),
            },
        });
    }
}

pub(crate) struct State {
    /// The next seq to assign (seqs start at 1).
    pub(crate) next_seq: u64,
    closed: bool,
    ring: Ring,
    pub(crate) drops: Option<DropRange>,
    pub(crate) promote: Option<String>,
    pub(crate) shutdown: bool,
}

pub(crate) struct Progress {
    pub(crate) flushed_upto: u64,
    pub(crate) done: bool,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) emitted: AtomicU64,
    pub(crate) written: AtomicU64,
    pub(crate) dropped: AtomicU64,
    pub(crate) suppressed: AtomicU64,
}

pub(crate) struct Shared {
    pub(crate) boot: Arc<str>,
    pub(crate) config: SinkConfig,
    role_json: String,
    state: Mutex<State>,
    tx: SyncSender<Msg>,
    tail: broadcast::Sender<Arc<Record>>,
    pub(crate) progress: Mutex<Progress>,
    pub(crate) progress_cond: Condvar,
    pub(crate) stats: Counters,
    /// Budgets with suppressed records waiting for their summary.
    pending_budgets: Mutex<Vec<&'static Budget>>,
    /// `tracing` events per `(target, level)` since the last summary.
    trace_counts: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
    /// Something waits for the next minute tick (a suppression or `tracing` counts).
    pub(crate) periodic: AtomicBool,
    /// The seq from which records go to the promoted file (`u64::MAX`: no promotion pending).
    pub(crate) promote_at: AtomicU64,
    /// Every file name written during this boot (the sources a reader may ask for).
    pub(crate) files: Mutex<Vec<String>>,
}

/// One sink. A process has exactly one, made by [`install`](crate::install); a `Sink` value of
/// its own is for tests and tools that need a sink with a particular [`Target`].
pub struct Sink {
    shared: Arc<Shared>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

thread_local! {
    /// Set while this thread is inside `emit` (the panic hook does not re-enter).
    pub(crate) static IN_EMIT: Cell<bool> = const { Cell::new(false) };
}

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Milliseconds since the process's first use of diagnostics (the record's `m`).
pub(crate) fn mono_ms() -> u64 {
    u64::try_from(EPOCH.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Milliseconds since the Unix epoch (the record's `t`).
pub(crate) fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The process's random boot id: 8 hex.
pub(crate) static BOOT: LazyLock<Arc<str>> = LazyLock::new(|| {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u32(std::process::id());
    h.write_u64(wall_ms());
    h.write_u128(EPOCH.elapsed().as_nanos());
    Arc::from(format!("{:08x}", h.finish() as u32))
});

/// A sink's files are plain names inside its directory.
fn plain_name(file: &str) -> std::io::Result<()> {
    if file.is_empty() || file.contains(['/', '\\']) || file.starts_with('.') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a diagnostics file is a plain file name",
        ));
    }
    Ok(())
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding one of these locks leaves consistent data (every critical section
    // is a few plain assignments), so the poison flag carries no information.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Sink {
    /// A sink writing files under `config.dir`.
    pub fn start(config: SinkConfig) -> std::io::Result<Sink> {
        let target = FsTarget::new(config.dir.clone());
        Sink::start_with(config, target)
    }

    /// A sink writing through `target` (the same writer, another place for its bytes).
    pub fn start_with<T: Target>(mut config: SinkConfig, target: T) -> std::io::Result<Sink> {
        plain_name(&config.file)?;
        LazyLock::force(&EPOCH);
        config.queue = config.queue.max(1);
        let (tx, rx) = mpsc::sync_channel(config.queue);
        let (tail, _) = broadcast::channel(TAIL_CAPACITY);
        let shared = Arc::new(Shared {
            boot: BOOT.clone(),
            role_json: json::quoted(config.role.as_str().unwrap_or("other")),
            files: Mutex::new(vec![config.file.clone()]),
            config,
            state: Mutex::new(State {
                next_seq: 1,
                closed: false,
                ring: Ring::new(),
                drops: None,
                promote: None,
                shutdown: false,
            }),
            tx,
            tail,
            progress: Mutex::new(Progress {
                flushed_upto: 0,
                done: false,
            }),
            progress_cond: Condvar::new(),
            stats: Counters::default(),
            pending_budgets: Mutex::new(Vec::new()),
            trace_counts: Mutex::new(BTreeMap::new()),
            periodic: AtomicBool::new(false),
            promote_at: AtomicU64::new(u64::MAX),
        });
        let writer = Writer::new(shared.clone(), rx, target);
        let handle = std::thread::Builder::new()
            .name(WRITER_THREAD.to_owned())
            .spawn(move || writer.run())?;
        Ok(Sink {
            shared,
            writer: Mutex::new(Some(handle)),
        })
    }

    /// Records `event`: budget check, serialize, ring + tail, `try_send`. Never waits.
    pub fn emit<E: DiagEvent>(&self, event: &E) {
        let budget = event.__budget();
        match budget.admit(mono_ms() / 60_000) {
            Admit::Pass => {}
            Admit::SuppressedFirst => {
                self.shared.stats.suppressed.fetch_add(1, Ordering::Relaxed);
                lock(&self.shared.pending_budgets).push(budget);
                self.shared.arm_periodic();
                return;
            }
            Admit::Suppressed => {
                self.shared.stats.suppressed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let mut fields = Fields::new();
        event.__fields(&mut fields);
        let was = IN_EMIT.with(|f| f.replace(true));
        let mut st = lock(&self.shared.state);
        self.shared
            .submit_locked(&mut st, event.ev(), event.level(), &fields, false);
        drop(st);
        IN_EMIT.with(|f| f.set(was));
    }

    /// Waits (at most `timeout`) until every record emitted before the call is written or
    /// accounted as dropped. `false` on timeout or when the writer cannot write.
    pub fn flush(&self, timeout: Duration) -> bool {
        let target = lock(&self.shared.state).next_seq - 1;
        self.shared.wake();
        self.wait_progress(timeout, |p| p.flushed_upto >= target)
    }

    /// From the next record on, writes into `file` in the same directory (a daemon switching
    /// from `aux.ndjson` to `daemon.ndjson` once it holds its lock). Returns at once; refuses a
    /// name that is not a plain file name.
    pub fn promote(&self, file: &str) -> std::io::Result<()> {
        plain_name(file)?;
        let mut st = lock(&self.shared.state);
        st.promote = Some(file.to_owned());
        self.shared.promote_at.store(st.next_seq, Ordering::Release);
        drop(st);
        self.shared.wake();
        Ok(())
    }

    /// Stops accepting records, writes what is queued, the pending summaries and the drop
    /// count, syncs the file and ends the writer thread. Waits at most `timeout`; `false` if the
    /// writer did not finish in time (it is then left to finish on its own).
    pub fn shutdown(&self, timeout: Duration) -> bool {
        {
            let mut st = lock(&self.shared.state);
            st.closed = true;
            st.shutdown = true;
        }
        self.shared.wake();
        let done = self.wait_progress(timeout, |p| p.done);
        if done {
            if let Some(h) = lock(&self.writer).take() {
                // The writer has signalled `done`; joining only collects the thread. A panic of
                // the writer was already recorded by the panic hook (or printed).
                let _ = h.join();
            }
        }
        done
    }

    /// The ring's records and a receiver of every later record, without gap or duplicate.
    pub fn tail(&self) -> Tail {
        let st = lock(&self.shared.state);
        (st.ring.snapshot(), self.shared.tail.subscribe())
    }

    pub fn stats(&self) -> Stats {
        let c = &self.shared.stats;
        Stats {
            queue: self.shared.config.queue,
            emitted: c.emitted.load(Ordering::Acquire),
            written: c.written.load(Ordering::Acquire),
            dropped: c.dropped.load(Ordering::Acquire),
            suppressed: c.suppressed.load(Ordering::Acquire),
        }
    }

    /// The sources this sink wrote during this boot (file names without `.ndjson`) with their
    /// generations on disk.
    pub fn sources(&self) -> Vec<Source> {
        let files = lock(&self.shared.files).clone();
        files
            .iter()
            .map(|f| reader::source(&self.shared.config.dir, f))
            .collect()
    }

    /// Reads whole lines of generation `gen` of `source` from `offset` (see [`reader::read`]).
    pub fn read(&self, source: &str, gen: u64, offset: u64, max: usize) -> std::io::Result<Chunk> {
        let file = lock(&self.shared.files)
            .iter()
            .find(|f| reader::source_name(f) == source)
            .cloned()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "unknown diagnostics source")
            })?;
        reader::read(&self.shared.config.dir, &file, gen, offset, max)
    }

    /// Counts one `tracing` event (the bridge layer).
    #[cfg(feature = "tracing")]
    pub(crate) fn count_trace(&self, target: &'static str, level: &'static str) {
        let first = {
            let mut c = lock(&self.shared.trace_counts);
            let first = c.is_empty();
            *c.entry((target, level)).or_insert(0) += 1;
            first
        };
        if first {
            self.shared.arm_periodic();
        }
    }

    fn wait_progress(&self, timeout: Duration, ready: impl Fn(&Progress) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        let mut p = lock(&self.shared.progress);
        loop {
            if ready(&p) {
                return true;
            }
            if p.done {
                return false;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            p = self
                .shared
                .progress_cond
                .wait_timeout(p, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

impl Drop for Sink {
    /// Asks the writer to finish (as [`Sink::shutdown`] does) without waiting for it.
    fn drop(&mut self) {
        {
            let mut st = lock(&self.shared.state);
            st.closed = true;
            st.shutdown = true;
        }
        self.shared.wake();
    }
}

impl Shared {
    /// Wakes the writer to look at control requests and timers. When the queue is full the
    /// writer is busy and looks at them after its current batch anyway.
    pub(crate) fn wake(&self) {
        if let Err(TrySendError::Disconnected(_)) = self.tx.try_send(Msg::Wake) {
            // The writer has ended (shutdown): nothing left to wake.
        }
    }

    fn arm_periodic(&self) {
        if !self.periodic.swap(true, Ordering::AcqRel) {
            self.wake();
        }
    }

    pub(crate) fn lock_state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    fn line(&self, seq: u64, ev: &'static str, lvl: Level, fields: &Fields) -> String {
        let f = fields.text();
        let mut s = String::with_capacity(112 + f.len());
        // Writing into a String cannot fail.
        let _ = write!(
            s,
            "{{\"b\":\"{}\",\"seq\":{seq},\"t\":{},\"m\":{},\"src\":{},\"lvl\":\"{}\",\"ev\":\"{ev}\"{f}}}",
            self.boot,
            wall_ms(),
            mono_ms(),
            self.role_json,
            lvl.as_str(),
        );
        s
    }

    /// Assigns the next seq to a record and queues it; under the state lock, so seq order is
    /// queue order is file order. A pending drop count is queued first. Internal records (the
    /// writer's summaries) are not queued and get no seq when the queue is full: the caller
    /// keeps their data for later. Returns whether the record was queued.
    pub(crate) fn submit_locked(
        &self,
        st: &mut State,
        ev: &'static str,
        lvl: Level,
        fields: &Fields,
        internal: bool,
    ) -> bool {
        if st.closed && !internal {
            return false;
        }
        self.queue_drops(st);
        let seq = st.next_seq;
        let line: Arc<str> = Arc::from(self.line(seq, ev, lvl, fields));
        // While a drop count is pending, later records are dropped too: a `diag.dropped` range
        // then never contains a written record.
        let sent = if st.drops.is_some() {
            Err(())
        } else {
            self.tx
                .try_send(Msg::Rec {
                    seq,
                    internal,
                    line: line.clone(),
                })
                .map_err(|_| ())
        };
        match sent {
            Ok(()) => {
                self.commit(st, seq, ev, fields, line, internal);
                true
            }
            Err(_) if internal => false,
            Err(_) => {
                // Full (or the writer has ended): the record stays visible in the ring and is
                // counted; its seq becomes a gap that a `diag.dropped` record explains.
                self.commit(st, seq, ev, fields, line, false);
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                DropRange::add(&mut st.drops, 1, seq, seq);
                false
            }
        }
    }

    /// Queues the pending `diag.dropped` record, if any; returns whether it was queued.
    pub(crate) fn queue_drops(&self, st: &mut State) -> bool {
        let Some(d) = st.drops.take() else {
            return false;
        };
        let ev = SinkEvent::Dropped {
            count: d.count,
            first_seq: d.first,
            last_seq: d.last,
        };
        if self.queue(st, &ev, true) {
            return true;
        }
        st.drops = Some(d);
        false
    }

    /// Queues one of the sink's own events (internal: no budget, no drop counting).
    pub(crate) fn queue(&self, st: &mut State, ev: &SinkEvent, internal: bool) -> bool {
        let mut fields = Fields::new();
        ev.__fields(&mut fields);
        self.submit_locked(st, ev.ev(), ev.level(), &fields, internal)
    }

    fn commit(
        &self,
        st: &mut State,
        seq: u64,
        ev: &'static str,
        fields: &Fields,
        line: Arc<str>,
        internal: bool,
    ) {
        st.next_seq = seq + 1;
        if !internal {
            self.stats.emitted.fetch_add(1, Ordering::Relaxed);
        }
        let rec = Arc::new(Record {
            boot: self.boot.clone(),
            seq,
            ev,
            host: fields.key(&fields.host),
            session: fields.key(&fields.session),
            trace: fields.key(&fields.trace),
            line,
        });
        st.ring.push(rec.clone());
        // No subscriber is not an error: the record is in the ring for the next one.
        let _ = self.tail.send(rec);
    }

    /// Queues the per-minute summaries (`diag.suppressed`, `trace.counted`). What cannot be
    /// queued stays pending. Returns whether anything was queued.
    pub(crate) fn drain_periodic(&self, st: &mut State) -> bool {
        let mut queued = false;
        let budgets = std::mem::take(&mut *lock(&self.pending_budgets));
        for b in budgets {
            let n = b.take_suppressed();
            if n == 0 {
                continue;
            }
            let ev = SinkEvent::Suppressed {
                event: Code::st(b.ev()),
                count: n,
            };
            if self.queue(st, &ev, true) {
                queued = true;
            } else {
                if b.give_back(n) {
                    lock(&self.pending_budgets).push(b);
                }
                self.periodic.store(true, Ordering::Release);
            }
        }
        let counts = std::mem::take(&mut *lock(&self.trace_counts));
        for ((target, level), n) in counts {
            let ev = SinkEvent::TraceCounted {
                target: Code::st(target),
                level: Code::st(level),
                n,
            };
            if self.queue(st, &ev, true) {
                queued = true;
            } else {
                *lock(&self.trace_counts).entry((target, level)).or_insert(0) += n;
                self.periodic.store(true, Ordering::Release);
            }
        }
        queued
    }
}
