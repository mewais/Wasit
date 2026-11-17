//! The shim process itself.
//!
//! Data flow: stdout/stderr reader tasks and the stdin writer append records to an in-memory
//! batch under one short `std::sync::Mutex` (seq assignment + encoding, no syscalls). A single
//! flusher task writes batches to `journal.ndjson` and publishes the durable end
//! (`Written {seq, offset}`) on a watch channel; it fsyncs on a slower timer and on demand.
//! A controller's `attach` is served by a pump that reads the **file** from the first record
//! with `seq >= from_seq` and then follows the watch channel — the file is the only source, so
//! there is no file→live seam at all: no gap, no duplicate. The controller never slows the
//! child: the pump lags behind the file instead, and a controller whose socket stays unwritable
//! for `write_timeout` is disconnected (it re-attaches from its last seq).

use std::ffi::OsString;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::cli::{self, Config};
use crate::endpoint::{BoxRead, BoxWrite, Listener};
use crate::platform::{self, Signaler};
use crate::record::{now_ms, ExitInfo, Kind};
use crate::wire::{self, ClientFrame, ServerFrame, PROTOCOL};
use crate::{journal, probe, Error, JOURNAL_FILE};

const FSYNC_EVERY: Duration = Duration::from_millis(1000);
const OUT_QUEUE: usize = 1024;
const WRITE_QUEUE: usize = 256;
const MAX_WRITE_BATCH: usize = 256 * 1024;
/// Unflushed journal bytes above which stdout/stderr readers wait for the flusher.
const BACKLOG_MAX: usize = 64 * 1024 * 1024;
const TERM_GRACE: Duration = Duration::from_secs(5);
const SYNC_TIMEOUT: Duration = Duration::from_secs(10);

macro_rules! log {
    ($($a:tt)*) => {{
        // Diagnostics only (stderr is `shim.log` when detached); nothing to do if it fails.
        let _ignored = writeln!(io::stderr(), "[wasit-shim {}] {}", now_ms(), format_args!($($a)*));
    }};
}

/// Process entry point of the shim. `args` are the shim's arguments **without** the program
/// name / host subcommand (see the crate docs for the command line). Returns the process exit
/// code. Must be called before the hosting process starts other threads that matter: on Unix
/// the shim re-executes the current executable with the same argv (`WASIT_SHIM_ROLE=guard`), so
/// the host must route that argv back into `run` without side effects.
pub fn run<I: IntoIterator<Item = OsString>>(args: I) -> i32 {
    let cfg = match cli::parse(args) {
        Ok(c) => c,
        Err(e) => {
            not_ready(&e.to_string());
            return 2;
        }
    };
    if platform::is_guard() {
        return platform::run_guard(&cfg);
    }
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            not_ready(&format!("cannot start runtime: {e}"));
            return 1;
        }
    };
    rt.block_on(serve(cfg))
}

fn ready_line(v: serde_json::Value) {
    let mut out = io::stdout();
    let line = format!("{v}\n");
    if let Err(e) = out.write_all(line.as_bytes()).and_then(|_| out.flush()) {
        log!("cannot write readiness line: {e}");
    }
    if let Err(e) = platform::detach_stdout() {
        log!("cannot detach stdout: {e}");
    }
}

fn not_ready(msg: &str) {
    log!("not ready: {msg}");
    ready_line(json!({ "ready": false, "error": msg }));
}

#[derive(Debug, Clone, Copy, Default)]
struct Written {
    offset: u64,
}

struct JState {
    next_seq: u64,
    pending: Vec<u8>,
    /// The `exited` record has been appended: no controller notes after it.
    ended: bool,
}

struct Shared {
    cfg: Config,
    journal_path: PathBuf,
    journal: Mutex<JState>,
    flush_notify: Notify,
    written: watch::Sender<Written>,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    signaler: Signaler,
    controller: Mutex<Option<(u64, oneshot::Sender<()>)>>,
    next_conn: AtomicU64,
    delivered: watch::Sender<u64>,
    shutdown: watch::Sender<bool>,
    fatal: watch::Sender<Option<String>>,
    shim_pid: u32,
    child_pid: u32,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Shared {
    /// Append one record. `data` is already-encoded JSON. Returns its seq.
    fn append(&self, kind: Kind, data: &str, partial: bool) -> u64 {
        let mut j = lock(&self.journal);
        let seq = j.next_seq;
        j.next_seq += 1;
        let t = now_ms();
        let partial = if partial { r#","partial":true"# } else { "" };
        // Writing into a Vec cannot fail.
        let _infallible = writeln!(
            j.pending,
            r#"{{"seq":{seq},"t":{t},"kind":"{}","data":{data}{partial}}}"#,
            kind.as_str()
        );
        drop(j);
        self.flush_notify.notify_one();
        seq
    }

    fn append_value(&self, kind: Kind, data: &serde_json::Value) -> u64 {
        self.append(kind, &data.to_string(), false)
    }

    fn note(&self, data: serde_json::Value) -> u64 {
        self.append_value(Kind::Note, &data)
    }

    /// Append the final `exited` record and refuse controller notes from now on.
    fn append_exited(&self, data: &serde_json::Value) -> u64 {
        lock(&self.journal).ended = true;
        self.append_value(Kind::Exited, data)
    }

    /// Journal a controller note unless the run has ended.
    fn controller_note(&self, data: &serde_json::Value) -> Result<u64, String> {
        let text = data.to_string();
        if text.len() > self.cfg.max_record {
            return Err(format!("note longer than {} bytes", self.cfg.max_record));
        }
        let mut j = lock(&self.journal);
        if j.ended {
            return Err("the child has exited".into());
        }
        let seq = j.next_seq;
        j.next_seq += 1;
        let t = now_ms();
        // Writing into a Vec cannot fail.
        let _infallible = writeln!(
            j.pending,
            r#"{{"seq":{seq},"t":{t},"kind":"note","data":{text}}}"#
        );
        drop(j);
        self.flush_notify.notify_one();
        Ok(seq)
    }

    fn emit_bytes(&self, kind: Kind, bytes: &[u8], partial: bool) {
        let text = String::from_utf8_lossy(bytes);
        let data = serde_json::Value::String(text.into_owned()).to_string();
        self.append(kind, &data, partial);
    }

    /// Wait while the unflushed batch is above the backlog limit (disk slower than the child).
    async fn wait_backlog(&self) {
        loop {
            let mut rx = self.written.subscribe();
            if lock(&self.journal).pending.len() <= BACKLOG_MAX {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    async fn write_line(&self, line: &str) -> Result<u64, String> {
        if line.contains('\n') {
            return Err("line must not contain a newline".into());
        }
        if line.len() > self.cfg.max_record {
            return Err(format!("line longer than {} bytes", self.cfg.max_record));
        }
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or("stdin is closed")?;
        let mut buf = Vec::with_capacity(line.len() + 1);
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        stdin
            .write_all(&buf)
            .await
            .map_err(|e| format!("write to child stdin: {e}"))?;
        stdin
            .flush()
            .await
            .map_err(|e| format!("flush child stdin: {e}"))?;
        // Journaled while still holding the stdin lock: journal order == write order.
        let data = serde_json::Value::String(line.to_owned()).to_string();
        Ok(self.append(Kind::In, &data, false))
    }

    async fn close_stdin(&self) -> Result<u64, String> {
        let mut guard = self.stdin.lock().await;
        match guard.take() {
            Some(s) => {
                drop(s);
                Ok(self.note(json!({ "stdin": "closed" })))
            }
            None => Err("stdin is already closed".into()),
        }
    }
}

struct Flusher {
    shared: Arc<Shared>,
    file: tokio::fs::File,
    offset: u64,
    spare: Vec<u8>,
    dirty: bool,
}

impl Flusher {
    async fn write_out(&mut self) -> io::Result<()> {
        {
            let mut j = lock(&self.shared.journal);
            if j.pending.is_empty() {
                return Ok(());
            }
            std::mem::swap(&mut j.pending, &mut self.spare);
        }
        self.file.write_all(&self.spare).await?;
        // tokio::fs::File completes writes in the background; flush waits for them.
        self.file.flush().await?;
        self.offset += self.spare.len() as u64;
        self.spare.clear();
        self.dirty = true;
        self.shared.written.send_replace(Written {
            offset: self.offset,
        });
        Ok(())
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.write_out().await?;
        if self.dirty {
            self.file.sync_all().await?;
            self.dirty = false;
        }
        Ok(())
    }

    async fn run(mut self, mut sync_rx: mpsc::Receiver<oneshot::Sender<io::Result<()>>>) {
        let mut tick = tokio::time::interval(FSYNC_EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let shared = self.shared.clone();
            let res = tokio::select! {
                _ = shared.flush_notify.notified() => self.write_out().await,
                _ = tick.tick() => {
                    if self.dirty {
                        self.dirty = false;
                        self.file.sync_data().await
                    } else {
                        Ok(())
                    }
                }
                req = sync_rx.recv() => match req {
                    Some(tx) => {
                        let r = self.sync().await;
                        let failed = r.as_ref().err().map(|e| e.to_string());
                        // The requester may have timed out; the result is also logged below.
                        let _ignored = tx.send(r);
                        match failed {
                            Some(e) => Err(io::Error::other(e)),
                            None => Ok(()),
                        }
                    }
                    None => {
                        if let Err(e) = self.sync().await {
                            log!("final journal sync failed: {e}");
                        }
                        return;
                    }
                },
            };
            if let Err(e) = res {
                log!("journal write failed: {e}");
                self.shared
                    .fatal
                    .send_replace(Some(format!("journal write failed: {e}")));
                return;
            }
        }
    }
}

/// Largest cut <= max that does not split a UTF-8 sequence (when the prefix is valid UTF-8).
fn cut_point(b: &[u8], max: usize) -> usize {
    match std::str::from_utf8(&b[..max]) {
        Ok(_) => max,
        Err(e) if e.error_len().is_none() && e.valid_up_to() > 0 => e.valid_up_to(),
        Err(_) => max,
    }
}

fn emit_line(shared: &Shared, kind: Kind, mut bytes: &[u8]) {
    let max = shared.cfg.max_record;
    while bytes.len() > max {
        let cut = cut_point(bytes, max);
        shared.emit_bytes(kind, &bytes[..cut], true);
        bytes = &bytes[cut..];
    }
    shared.emit_bytes(kind, bytes, false);
}

async fn read_stream<R: AsyncRead + Unpin>(
    shared: Arc<Shared>,
    r: R,
    kind: Kind,
) -> io::Result<()> {
    let max = shared.cfg.max_record;
    let mut reader = BufReader::with_capacity(64 * 1024, r);
    let mut line: Vec<u8> = Vec::new();
    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            if !line.is_empty() {
                emit_line(&shared, kind, &line);
            }
            return Ok(());
        }
        let mut consumed = 0;
        while let Some(i) = buf[consumed..].iter().position(|&b| b == b'\n') {
            let piece = &buf[consumed..consumed + i];
            consumed += i + 1;
            if line.is_empty() {
                let piece = piece.strip_suffix(b"\r").unwrap_or(piece);
                emit_line(&shared, kind, piece);
            } else {
                line.extend_from_slice(piece);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                emit_line(&shared, kind, &line);
                line.clear();
            }
        }
        line.extend_from_slice(&buf[consumed..]);
        let n = buf.len();
        reader.consume(n);
        while line.len() > max {
            let cut = cut_point(&line, max);
            shared.emit_bytes(kind, &line[..cut], true);
            line.drain(..cut);
        }
        shared.wait_backlog().await;
    }
}

struct Out {
    frame: String,
    seq: Option<u64>,
}

impl Out {
    fn frame(f: &ServerFrame) -> Self {
        Out {
            frame: wire::encode(f),
            seq: None,
        }
    }
}

fn ack(id: u64, seq: Option<u64>) -> Out {
    Out::frame(&ServerFrame::Ack { id, ok: true, seq })
}

fn err(id: Option<u64>, message: impl Into<String>) -> Out {
    Out::frame(&ServerFrame::Error {
        id,
        message: message.into(),
    })
}

enum WReq {
    Write { id: u64, line: String },
    Close { id: u64 },
    Note { id: u64, data: serde_json::Value },
}

async fn write_worker(shared: Arc<Shared>, mut rx: mpsc::Receiver<WReq>, out: mpsc::Sender<Out>) {
    while let Some(req) = rx.recv().await {
        let frame = match req {
            WReq::Write { id, line } => match shared.write_line(&line).await {
                Ok(seq) => ack(id, Some(seq)),
                Err(m) => err(Some(id), m),
            },
            WReq::Close { id } => match shared.close_stdin().await {
                Ok(seq) => ack(id, Some(seq)),
                Err(m) => err(Some(id), m),
            },
            WReq::Note { id, data } => match shared.controller_note(&data) {
                Ok(seq) => ack(id, Some(seq)),
                Err(m) => err(Some(id), m),
            },
        };
        // The controller may be gone; the write itself already happened and is journaled.
        let _ignored = out.send(frame).await;
    }
}

/// Stream records with `seq >= from_seq` from the journal file, following its durable end.
async fn pump(shared: Arc<Shared>, from_seq: u64, out: mpsc::Sender<Out>) {
    if let Err(e) = pump_inner(&shared, from_seq, &out).await {
        // Controller may already be gone.
        let _ignored = out.send(err(None, format!("attach failed: {e}"))).await;
    }
}

async fn pump_inner(shared: &Shared, from_seq: u64, out: &mpsc::Sender<Out>) -> io::Result<()> {
    let mut wrx = shared.written.subscribe();
    let w0 = *wrx.borrow_and_update();
    let path = shared.journal_path.clone();
    let start = {
        let path = path.clone();
        tokio::task::spawn_blocking(move || journal::seek_to_seq(&path, from_seq, w0.offset))
            .await
            .map_err(io::Error::other)??
    };
    let mut file = tokio::fs::File::open(&path).await?;
    file.seek(io::SeekFrom::Start(start)).await?;
    let mut rd = BufReader::with_capacity(64 * 1024, file);
    let mut off = start;
    let mut line = Vec::new();
    loop {
        let limit = wrx.borrow_and_update().offset;
        while off < limit {
            line.clear();
            let n = rd.read_until(b'\n', &mut line).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "journal shorter than its durable end",
                ));
            }
            off += n as u64;
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            let seq = journal::parse_seq(&line)?;
            if seq < from_seq {
                continue;
            }
            let mut frame = String::with_capacity(line.len() + 32);
            frame.push_str(r#"{"type":"record","record":"#);
            frame.push_str(&String::from_utf8_lossy(&line));
            frame.push_str("}\n");
            if out
                .send(Out {
                    frame,
                    seq: Some(seq),
                })
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        if wrx.changed().await.is_err() {
            return Ok(());
        }
    }
}

async fn write_loop(
    shared: Arc<Shared>,
    mut wr: BoxWrite,
    mut rx: mpsc::Receiver<Out>,
    mut replaced: oneshot::Receiver<()>,
) {
    let wt = shared.cfg.write_timeout;
    let mut shut = shared.shutdown.subscribe();
    let mut replaced_live = true;
    let mut batch: Vec<u8> = Vec::new();
    loop {
        tokio::select! {
            biased;
            r = &mut replaced, if replaced_live => {
                replaced_live = false;
                if r.is_ok() {
                    let f = wire::encode(&ServerFrame::Replaced);
                    // Best effort: the old controller may be stalled or gone.
                    let _ignored = timeout(wt, wr.write_all(f.as_bytes())).await;
                    break;
                }
            }
            _ = shut.changed() => break,
            o = rx.recv() => {
                let Some(first) = o else { break };
                batch.clear();
                batch.extend_from_slice(first.frame.as_bytes());
                let mut max_seq = first.seq;
                while batch.len() < MAX_WRITE_BATCH {
                    match rx.try_recv() {
                        Ok(o) => {
                            batch.extend_from_slice(o.frame.as_bytes());
                            if o.seq.is_some() {
                                max_seq = o.seq;
                            }
                        }
                        Err(_) => break,
                    }
                }
                match timeout(wt, wr.write_all(&batch)).await {
                    Ok(Ok(())) => {
                        if let Some(s) = max_seq {
                            shared.delivered.send_if_modified(|d| {
                                let newer = s > *d;
                                if newer {
                                    *d = s;
                                }
                                newer
                            });
                        }
                    }
                    Ok(Err(e)) => {
                        log!("controller write failed: {e}");
                        break;
                    }
                    Err(_) => {
                        log!("controller did not keep up for {wt:?}; disconnecting it");
                        break;
                    }
                }
            }
        }
    }
    // Peer may already be gone.
    let _ignored = timeout(wt, wr.shutdown()).await;
}

async fn connection(shared: Arc<Shared>, rd: BoxRead, wr: BoxWrite) {
    let id = shared.next_conn.fetch_add(1, Ordering::Relaxed);
    let (rep_tx, rep_rx) = oneshot::channel();
    let old = lock(&shared.controller).replace((id, rep_tx));
    if let Some((_, old_tx)) = old {
        // The old connection may have ended on its own already.
        let _ignored = old_tx.send(());
    }
    let (out_tx, out_rx) = mpsc::channel::<Out>(OUT_QUEUE);
    let mut writer = tokio::spawn(write_loop(shared.clone(), wr, out_rx, rep_rx));
    let hello = Out::frame(&ServerFrame::Hello {
        proto: PROTOCOL,
        shim_pid: shared.shim_pid,
        child_pid: shared.child_pid,
    });
    if out_tx.send(hello).await.is_err() {
        return;
    }
    let (wq_tx, wq_rx) = mpsc::channel::<WReq>(WRITE_QUEUE);
    tokio::spawn(write_worker(shared.clone(), wq_rx, out_tx.clone()));
    let mut pump_task: Option<JoinHandle<()>> = None;
    let mut rd = BufReader::new(rd);
    let mut buf = Vec::new();
    let mut shut = shared.shutdown.subscribe();
    let max_frame = shared.cfg.max_record * 6 + 4096;
    loop {
        let r = tokio::select! {
            _ = &mut writer => break,
            _ = shut.changed() => break,
            r = wire::read_frame(&mut rd, &mut buf, max_frame) => r,
        };
        match r {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                // Connection is going away anyway.
                let _ignored = out_tx.send(err(None, format!("bad frame: {e}"))).await;
                break;
            }
        }
        let frame = match serde_json::from_slice::<ClientFrame>(&buf) {
            Ok(f) => f,
            Err(e) => {
                if out_tx
                    .send(err(None, format!("bad frame: {e}")))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
        };
        let sent = match frame {
            ClientFrame::Attach { id, from_seq } => {
                if let Some(p) = pump_task.take() {
                    p.abort();
                }
                let ok = match id {
                    Some(id) => out_tx.send(ack(id, None)).await.is_ok(),
                    None => true,
                };
                pump_task = Some(tokio::spawn(pump(
                    shared.clone(),
                    from_seq.max(1),
                    out_tx.clone(),
                )));
                ok
            }
            ClientFrame::Write { id, line } => wq_tx.send(WReq::Write { id, line }).await.is_ok(),
            ClientFrame::CloseStdin { id } => wq_tx.send(WReq::Close { id }).await.is_ok(),
            ClientFrame::Note { id, data } => wq_tx.send(WReq::Note { id, data }).await.is_ok(),
            ClientFrame::Signal { id, sig } => {
                let reply = match shared.signaler.signal(sig) {
                    Ok(()) => {
                        let seq =
                            shared.note(json!({ "signal": sig.as_str(), "by": "controller" }));
                        ack(id, Some(seq))
                    }
                    Err(e) => err(Some(id), format!("signal {}: {e}", sig.as_str())),
                };
                out_tx.send(reply).await.is_ok()
            }
        };
        if !sent {
            break;
        }
    }
    if let Some(p) = pump_task {
        p.abort();
    }
    let mut slot = lock(&shared.controller);
    if matches!(&*slot, Some((cid, _)) if *cid == id) {
        *slot = None;
    }
}

async fn accept_loop(shared: Arc<Shared>, mut listener: Listener) {
    let mut shut = shared.shutdown.subscribe();
    let mut backoff = Duration::from_millis(10);
    loop {
        tokio::select! {
            r = listener.accept() => match r {
                Ok((rd, wr)) => {
                    backoff = Duration::from_millis(10);
                    tokio::spawn(connection(shared.clone(), rd, wr));
                }
                Err(e) => {
                    log!("accept failed: {e}");
                    // Backoff schedule on a failing accept.
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
            },
            _ = shut.changed() => break,
        }
    }
    if let Err(e) = listener.remove() {
        log!("cannot remove control endpoint: {e}");
    }
}

#[cfg(unix)]
async fn termination_requested() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let (mut term, mut int) = match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(t), Ok(i)) => (t, i),
        _ => {
            log!("cannot install signal handlers");
            return std::future::pending().await;
        }
    };
    tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    }
}

#[cfg(windows)]
async fn termination_requested() -> &'static str {
    // A detached process has no console and receives no console control events.
    std::future::pending().await
}

#[cfg(unix)]
fn ignore_hangup() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::hangup()) {
        Ok(mut s) => {
            tokio::spawn(async move { while s.recv().await.is_some() {} });
        }
        Err(e) => log!("cannot install SIGHUP handler: {e}"),
    }
}

#[cfg(windows)]
fn ignore_hangup() {}

struct Running {
    shared: Arc<Shared>,
    lock: std::fs::File,
    exit: JoinHandle<io::Result<ExitInfo>>,
    readers: [JoinHandle<io::Result<()>>; 2],
    sync_tx: mpsc::Sender<oneshot::Sender<io::Result<()>>>,
    accept: JoinHandle<()>,
    spawned_seq: u64,
}

async fn start(cfg: Config) -> Result<Running, Error> {
    tokio::fs::create_dir_all(&cfg.dir).await?;
    let lock_file = probe::acquire(&cfg.dir).await?;
    let journal_path = cfg.dir.join(JOURNAL_FILE);
    let (next_seq, len) = {
        let p = journal_path.clone();
        tokio::task::spawn_blocking(move || journal::recover(&p)).await??
    };
    let file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .await?;
    let listener = Listener::bind(&cfg.dir)?;
    let sp = match platform::spawn(&cfg).await {
        Ok(sp) => sp,
        Err(e) => {
            if let Err(re) = listener.remove() {
                log!("cannot remove control endpoint: {re}");
            }
            return Err(e);
        }
    };
    let cwd = match &cfg.cwd {
        Some(c) => c.clone(),
        None => std::env::current_dir().unwrap_or_default(),
    };
    let argv: Vec<String> = std::iter::once(&cfg.program)
        .chain(cfg.args.iter())
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let shared = Arc::new(Shared {
        journal_path,
        journal: Mutex::new(JState {
            next_seq,
            pending: Vec::new(),
            ended: false,
        }),
        flush_notify: Notify::new(),
        written: watch::Sender::new(Written { offset: len }),
        stdin: tokio::sync::Mutex::new(Some(sp.stdin)),
        signaler: sp.signaler,
        controller: Mutex::new(None),
        next_conn: AtomicU64::new(1),
        delivered: watch::Sender::new(0),
        shutdown: watch::Sender::new(false),
        fatal: watch::Sender::new(None),
        shim_pid: std::process::id(),
        child_pid: sp.pid,
        cfg,
    });
    let spawned_seq = shared.append_value(
        Kind::Spawned,
        &json!({
            "pid": sp.pid,
            "argv": argv,
            "cwd": cwd.to_string_lossy(),
            "started_at": now_ms(),
        }),
    );
    let (sync_tx, sync_rx) = mpsc::channel(4);
    tokio::spawn(
        Flusher {
            shared: shared.clone(),
            file,
            offset: len,
            spare: Vec::new(),
            dirty: false,
        }
        .run(sync_rx),
    );
    let readers = [
        tokio::spawn(read_stream(shared.clone(), sp.stdout, Kind::Out)),
        tokio::spawn(read_stream(shared.clone(), sp.stderr, Kind::Err)),
    ];
    let accept = tokio::spawn(accept_loop(shared.clone(), listener));
    Ok(Running {
        shared,
        lock: lock_file,
        exit: sp.exit,
        readers,
        sync_tx,
        accept,
        spawned_seq,
    })
}

async fn serve(cfg: Config) -> i32 {
    let r = match start(cfg).await {
        Ok(r) => r,
        Err(e) => {
            not_ready(&e.to_string());
            return 1;
        }
    };
    ready_line(json!({
        "ready": true,
        "pid": r.shared.shim_pid,
        "child_pid": r.shared.child_pid,
        "seq": r.spawned_seq,
    }));
    ignore_hangup();
    supervise(r).await
}

fn flatten_exit(r: Result<io::Result<ExitInfo>, tokio::task::JoinError>) -> ExitInfo {
    match r {
        Ok(Ok(e)) => e,
        Ok(Err(e)) => {
            log!("waiting for the child failed: {e}");
            ExitInfo::default()
        }
        Err(e) => {
            log!("child waiter failed: {e}");
            ExitInfo::default()
        }
    }
}

async fn stop_child(shared: &Shared, exit: &mut JoinHandle<io::Result<ExitInfo>>) -> ExitInfo {
    if let Err(e) = shared.signaler.signal(crate::Sig::Term) {
        log!("forwarding SIGTERM failed: {e}");
    }
    if let Ok(r) = timeout(TERM_GRACE, &mut *exit).await {
        return flatten_exit(r);
    }
    shared.note(json!({ "shim": "child ignored SIGTERM; killing" }));
    if let Err(e) = shared.signaler.signal(crate::Sig::Kill) {
        log!("SIGKILL failed: {e}");
    }
    match timeout(TERM_GRACE, &mut *exit).await {
        Ok(r) => flatten_exit(r),
        Err(_) => {
            log!("child did not exit after SIGKILL");
            ExitInfo::default()
        }
    }
}

async fn supervise(mut r: Running) -> i32 {
    let shared = r.shared.clone();
    let mut fatal = shared.fatal.subscribe();
    let mut code = 0;
    let exit = tokio::select! {
        res = &mut r.exit => flatten_exit(res),
        what = termination_requested() => {
            shared.note(json!({ "shim": format!("received {what}; terminating child") }));
            stop_child(&shared, &mut r.exit).await
        }
        _ = fatal.changed() => {
            code = 1;
            stop_child(&shared, &mut r.exit).await
        }
    };

    // Drain stdout/stderr to EOF, bounded: a grandchild may keep the pipes open.
    let [mut out_h, mut err_h] = r.readers;
    let drained = timeout(shared.cfg.drain_timeout, async {
        for (h, name) in [(&mut out_h, "stdout"), (&mut err_h, "stderr")] {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    shared.note(json!({ "shim": format!("reading child {name} failed: {e}") }));
                }
                Err(e) => log!("{name} reader failed: {e}"),
            }
        }
    })
    .await;
    if drained.is_err() {
        out_h.abort();
        err_h.abort();
        shared.note(
            json!({ "shim": "stdio still open after child exit; killing leftover processes" }),
        );
        if let Err(e) = shared.signaler.kill_all() {
            log!("killing leftovers failed: {e}");
        }
    }
    match timeout(Duration::from_secs(1), shared.stdin.lock()).await {
        Ok(mut g) => drop(g.take()),
        Err(_) => log!("stdin writer still busy at exit"),
    }

    let final_seq = shared.append_exited(&json!(exit));
    let (tx, rx) = oneshot::channel();
    let synced: Result<(), String> = match r.sync_tx.send(tx).await {
        Err(_) => Err("journal flusher is gone".into()),
        Ok(()) => match timeout(SYNC_TIMEOUT, rx).await {
            Err(_) => Err("timed out".into()),
            Ok(Err(_)) => Err("flusher dropped the request".into()),
            Ok(Ok(Err(e))) => Err(e.to_string()),
            Ok(Ok(Ok(()))) => Ok(()),
        },
    };
    if let Err(e) = synced {
        log!("final journal sync did not complete: {e}");
        code = 1;
    }

    // Linger so a connected controller receives the tail: until it has received `exited`, or
    // no record has been delivered to any controller for `linger`.
    let mut delivered = shared.delivered.subscribe();
    while *delivered.borrow_and_update() < final_seq {
        match timeout(shared.cfg.linger, delivered.changed()).await {
            Ok(Ok(())) => continue,
            Ok(Err(_)) | Err(_) => break,
        }
    }

    // Release liveness first so that anyone who sees the endpoint close also sees the lock free.
    if let Err(e) = r.lock.unlock() {
        log!("unlock failed: {e}");
    }
    drop(r.lock);
    shared.shutdown.send_replace(true);
    if timeout(Duration::from_secs(1), r.accept).await.is_err() {
        log!("accept loop did not stop in time");
    }
    code
}
