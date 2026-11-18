//! Controller side of the shim endpoint, and a reader for journals whose shim is gone.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::endpoint::{self, BoxRead, BoxWrite};
use crate::record::Record;
use crate::wire::{self, ClientFrame, ServerFrame, Sig};
use crate::{Error, JOURNAL_FILE};

const JOURNAL_QUEUE: usize = 1024;
const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Deadlines for a [`ShimClient`].
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Connecting and receiving `hello`.
    pub connect_timeout: Duration,
    /// Sending a request and receiving its ack.
    pub request_timeout: Duration,
    /// Records buffered for a [`RecordStream`].
    pub record_queue: usize,
    /// A full record queue that stays full this long ends the stream with [`Error::Lagged`].
    pub lag_timeout: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            record_queue: 4096,
            lag_timeout: Duration::from_secs(2),
        }
    }
}

type Pending = HashMap<u64, oneshot::Sender<Result<Option<u64>, Error>>>;
type Terminal = Arc<Mutex<Option<Error>>>;

/// Producer side of a [`RecordStream`].
#[derive(Clone)]
struct RecordTx {
    tx: mpsc::Sender<Result<Record, Error>>,
    terminal: Terminal,
}

impl RecordTx {
    fn new(cap: usize) -> (Self, RecordStream) {
        let (tx, rx) = mpsc::channel(cap.max(1));
        let terminal: Terminal = Arc::new(Mutex::new(None));
        (
            RecordTx {
                tx,
                terminal: terminal.clone(),
            },
            RecordStream {
                rx,
                terminal: Some(terminal),
            },
        )
    }

    /// End the stream: it yields `e` after the records already queued, then `None`.
    fn end(self, e: Error) {
        *lock(&self.terminal) = Some(e);
    }
}

struct Inner {
    writer: tokio::sync::Mutex<BoxWrite>,
    pending: Mutex<Pending>,
    records: Mutex<Option<RecordTx>>,
    /// Stream of an attach whose ack has not arrived yet; installed when the ack arrives, so
    /// frames of a previous attach never leak into it.
    attach_pending: Mutex<Option<(u64, RecordTx)>>,
    next_id: AtomicU64,
    opts: ClientOptions,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A controller connection to one shim. Connecting replaces any previous controller.
///
/// Records are delivered with backpressure. Requests are never blocked for longer than
/// `lag_timeout` by an unconsumed [`RecordStream`]: such a stream ends with [`Error::Lagged`]
/// and the consumer re-attaches from its last seq.
pub struct ShimClient {
    inner: Arc<Inner>,
    reader: JoinHandle<()>,
    shim_pid: u32,
    child_pid: u32,
}

impl Drop for ShimClient {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// A stream of journal records. Ends (`None`) when the connection closes (shim exited, or the
/// shim dropped a controller that could not keep up); its last item is an `Err` for
/// [`Error::Replaced`], [`Error::Lagged`] and transport/protocol errors.
pub struct RecordStream {
    rx: mpsc::Receiver<Result<Record, Error>>,
    terminal: Option<Terminal>,
}

impl RecordStream {
    pub async fn next(&mut self) -> Option<Result<Record, Error>> {
        std::future::poll_fn(|cx| self.poll_item(cx)).await
    }

    fn poll_item(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Record, Error>>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(None) => {
                Poll::Ready(self.terminal.take().and_then(|t| lock(&t).take()).map(Err))
            }
            other => other,
        }
    }
}

impl futures_core::Stream for RecordStream {
    type Item = Result<Record, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_item(cx)
    }
}

impl ShimClient {
    /// Connect with default deadlines.
    pub async fn connect(dir: &Path) -> Result<Self, Error> {
        Self::connect_with(dir, ClientOptions::default()).await
    }

    pub async fn connect_with(dir: &Path, opts: ClientOptions) -> Result<Self, Error> {
        let (rd, wr) = timeout(opts.connect_timeout, endpoint::connect(dir))
            .await
            .map_err(|_| Error::Timeout("connecting to the shim"))??;
        let mut rd = BufReader::new(rd);
        let mut buf = Vec::new();
        let got = timeout(
            opts.connect_timeout,
            wire::read_frame(&mut rd, &mut buf, MAX_FRAME),
        )
        .await
        .map_err(|_| Error::Timeout("waiting for the shim's hello"))??;
        if !got {
            return Err(Error::Disconnected);
        }
        let (shim_pid, child_pid) = match serde_json::from_slice::<ServerFrame>(&buf) {
            Ok(ServerFrame::Hello {
                proto,
                shim_pid,
                child_pid,
            }) => {
                if proto != wire::PROTOCOL {
                    return Err(Error::Protocol(format!(
                        "shim speaks protocol {proto}, client {}",
                        wire::PROTOCOL
                    )));
                }
                (shim_pid, child_pid)
            }
            Ok(other) => return Err(Error::Protocol(format!("expected hello, got {other:?}"))),
            Err(e) => return Err(Error::Protocol(format!("bad hello: {e}"))),
        };
        let inner = Arc::new(Inner {
            writer: tokio::sync::Mutex::new(wr),
            pending: Mutex::new(HashMap::new()),
            records: Mutex::new(None),
            attach_pending: Mutex::new(None),
            next_id: AtomicU64::new(1),
            opts,
        });
        let reader = tokio::spawn(read_loop(inner.clone(), rd));
        Ok(ShimClient {
            inner,
            reader,
            shim_pid,
            child_pid,
        })
    }

    pub fn shim_pid(&self) -> u32 {
        self.shim_pid
    }

    pub fn child_pid(&self) -> u32 {
        self.child_pid
    }

    fn next_id(&self) -> u64 {
        self.inner.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn request(&self, make: impl FnOnce(u64) -> ClientFrame) -> Result<Option<u64>, Error> {
        let id = self.next_id();
        self.request_id(id, make(id)).await
    }

    async fn request_id(&self, id: u64, frame: ClientFrame) -> Result<Option<u64>, Error> {
        let (tx, rx) = oneshot::channel();
        lock(&self.inner.pending).insert(id, tx);
        let frame = wire::encode(&frame);
        let t = self.inner.opts.request_timeout;
        let res = async {
            timeout(t, async {
                let mut w = self.inner.writer.lock().await;
                w.write_all(frame.as_bytes()).await?;
                w.flush().await
            })
            .await
            .map_err(|_| Error::Timeout("sending a request to the shim"))?
            .map_err(|e| {
                use std::io::ErrorKind::*;
                // A peer that went away is a disconnect, whatever the OS calls it.
                match e.kind() {
                    BrokenPipe | ConnectionReset | ConnectionAborted | NotConnected => {
                        Error::Disconnected
                    }
                    _ => Error::Io(e),
                }
            })?;
            match timeout(t, rx).await {
                Err(_) => Err(Error::Timeout("waiting for the shim's ack")),
                Ok(Err(_)) => Err(Error::Disconnected),
                Ok(Ok(r)) => r,
            }
        }
        .await;
        if res.is_err() {
            lock(&self.inner.pending).remove(&id);
        }
        res
    }

    /// Start streaming records with `seq >= from_seq` (then live). A second `attach` ends the
    /// previous stream.
    pub async fn attach(&self, from_seq: u64) -> Result<RecordStream, Error> {
        let (tx, stream) = RecordTx::new(self.inner.opts.record_queue);
        let id = self.next_id();
        *lock(&self.inner.records) = None;
        *lock(&self.inner.attach_pending) = Some((id, tx));
        self.request_id(
            id,
            ClientFrame::Attach {
                id: Some(id),
                from_seq,
            },
        )
        .await?;
        Ok(stream)
    }

    /// Write one line (without `\n`) to the child's stdin. Returns the seq of its `in` record.
    pub async fn write(&self, line: &str) -> Result<u64, Error> {
        if line.contains('\n') {
            return Err(Error::InvalidArgument(
                "line must not contain a newline".into(),
            ));
        }
        let line = line.to_owned();
        self.request(|id| ClientFrame::Write { id, line })
            .await?
            .ok_or_else(|| Error::Protocol("write ack without seq".into()))
    }

    /// Close the child's stdin. Returns the seq of the `note` record.
    pub async fn close_stdin(&self) -> Result<u64, Error> {
        self.request(|id| ClientFrame::CloseStdin { id })
            .await?
            .ok_or_else(|| Error::Protocol("close_stdin ack without seq".into()))
    }

    /// Journal a controller `note` record carrying `data`, ordered after every `write` /
    /// `close_stdin` sent before it on this connection. Fails once the child has exited.
    /// Returns the seq of the note.
    pub async fn note(&self, data: serde_json::Value) -> Result<u64, Error> {
        self.request(|id| ClientFrame::Note { id, data })
            .await?
            .ok_or_else(|| Error::Protocol("note ack without seq".into()))
    }

    /// Signal the child's process group (Unix) / job (Windows). Returns the seq of the `note`.
    pub async fn signal(&self, sig: Sig) -> Result<u64, Error> {
        self.request(|id| ClientFrame::Signal { id, sig })
            .await?
            .ok_or_else(|| Error::Protocol("signal ack without seq".into()))
    }
}

/// Hand a record to the stream. A consumer that is polling gets plain backpressure; one that
/// makes no room for `lag_timeout` (e.g. it is itself waiting for an ack queued behind these
/// records) gets [`Error::Lagged`] and later records are discarded until it re-attaches, so the
/// connection reader — and with it every ack — never stays blocked.
async fn deliver(inner: &Inner, record: Record) {
    let Some(sink) = lock(&inner.records).clone() else {
        return;
    };
    match timeout(inner.opts.lag_timeout, sink.tx.send(Ok(record))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            // Consumer dropped the stream.
            clear_if_current(inner, &sink);
        }
        Err(_) => {
            if clear_if_current(inner, &sink) {
                sink.end(Error::Lagged);
            }
        }
    }
}

fn clear_if_current(inner: &Inner, sink: &RecordTx) -> bool {
    let mut slot = lock(&inner.records);
    let current = slot.as_ref().is_some_and(|s| s.tx.same_channel(&sink.tx));
    if current {
        *slot = None;
    }
    current
}

fn fail_all(inner: &Inner, mk: impl Fn() -> Error) {
    let pending: Vec<_> = lock(&inner.pending).drain().collect();
    for (_, tx) in pending {
        // Requester may have given up already.
        let _ignored = tx.send(Err(mk()));
    }
}

async fn read_loop(inner: Arc<Inner>, mut rd: BufReader<BoxRead>) {
    let mut buf = Vec::new();
    let end: Option<Error> = loop {
        match wire::read_frame(&mut rd, &mut buf, MAX_FRAME).await {
            Ok(true) => {}
            Ok(false) => break None,
            Err(e) => break Some(Error::Io(e)),
        }
        let frame = match serde_json::from_slice::<ServerFrame>(&buf) {
            Ok(f) => f,
            Err(e) => break Some(Error::Protocol(format!("bad frame from shim: {e}"))),
        };
        match frame {
            ServerFrame::Record { record } => {
                deliver(&inner, record).await;
            }
            ServerFrame::Ack { id, seq, .. } => {
                {
                    let mut ap = lock(&inner.attach_pending);
                    if matches!(&*ap, Some((aid, _)) if *aid == id) {
                        *lock(&inner.records) = ap.take().map(|(_, tx)| tx);
                    }
                }
                if let Some(tx) = lock(&inner.pending).remove(&id) {
                    // Requester may have timed out.
                    let _ignored = tx.send(Ok(seq));
                }
            }
            ServerFrame::Error {
                id: Some(id),
                message,
            } => {
                if let Some(tx) = lock(&inner.pending).remove(&id) {
                    // Requester may have timed out.
                    let _ignored = tx.send(Err(Error::Remote(message)));
                }
            }
            ServerFrame::Error { id: None, message } => {
                // The shim's attach pump failed: terminal for this stream.
                end_stream(&inner, Error::Remote(message));
            }
            ServerFrame::Replaced => break Some(Error::Replaced),
            ServerFrame::Hello { .. } => {}
        }
    };
    let replaced = matches!(end, Some(Error::Replaced));
    fail_all(&inner, || {
        if replaced {
            Error::Replaced
        } else {
            Error::Disconnected
        }
    });
    match end {
        Some(e) => end_stream(&inner, e),
        None => drop(lock(&inner.records).take()),
    }
}

/// End the current stream with a terminal error.
fn end_stream(inner: &Inner, e: Error) {
    if let Some(sink) = lock(&inner.records).take() {
        sink.end(e);
    }
}

/// Read records with `seq >= from_seq` from a session's journal as it is on disk now (for
/// sessions whose shim is gone). A torn final line is ignored.
pub async fn read_journal(dir: &Path, from_seq: u64) -> Result<RecordStream, Error> {
    let path = dir.join(JOURNAL_FILE);
    let len = tokio::fs::metadata(&path).await?.len();
    let start = {
        let path = path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
            // Limit the search to complete lines.
            let limit = complete_prefix(&path, len)?;
            crate::journal::seek_to_seq(&path, from_seq, limit)
        })
        .await??
    };
    let mut file = tokio::fs::File::open(&path).await?;
    tokio::io::AsyncSeekExt::seek(&mut file, std::io::SeekFrom::Start(start)).await?;
    let (tx, rx) = mpsc::channel(JOURNAL_QUEUE);
    let stream = RecordStream { rx, terminal: None };
    tokio::spawn(async move {
        let mut rd = BufReader::with_capacity(64 * 1024, file);
        let mut line = Vec::new();
        loop {
            line.clear();
            let item = match rd.read_until(b'\n', &mut line).await {
                Ok(0) => return,
                Ok(_) if line.last() != Some(&b'\n') => return, // torn tail
                Ok(_) => {
                    line.pop();
                    match serde_json::from_slice::<Record>(&line) {
                        Ok(r) if r.seq < from_seq => continue,
                        Ok(r) => Ok(r),
                        Err(e) => Err(Error::Corrupt(e.to_string())),
                    }
                }
                Err(e) => Err(Error::Io(e)),
            };
            let stop = item.is_err();
            if tx.send(item).await.is_err() || stop {
                return;
            }
        }
    });
    Ok(stream)
}

fn complete_prefix(path: &Path, len: u64) -> std::io::Result<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    let mut pos = len;
    let mut buf = vec![0u8; 64 * 1024];
    while pos > 0 {
        let start = pos.saturating_sub(buf.len() as u64);
        let n = (pos - start) as usize;
        f.seek(SeekFrom::Start(start))?;
        f.read_exact(&mut buf[..n])?;
        if let Some(i) = buf[..n].iter().rposition(|&b| b == b'\n') {
            return Ok(start + i as u64 + 1);
        }
        pos = start;
    }
    Ok(0)
}
