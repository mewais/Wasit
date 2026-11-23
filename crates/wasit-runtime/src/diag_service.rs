//! [`DiagService`]: this process's own diagnostics over the control protocol, so a hub can show
//! and export a host's logs (design §6.4, §6.5). A daemon mounts it next to
//! [`RuntimeService`](crate::RuntimeService) on [`DIAG_ROUTES`](crate::wire::DIAG_ROUTES).
//!
//! * `diag.sources` — the files this process's sink wrote during this boot (`daemon`, `aux`) and
//!   `shim:<session id>` for every session it knows that has a shim log, each with its
//!   generations.
//! * `diag.read` — whole lines of one generation of one of those sources.
//! * topic `diag.tail` — the sink's ring, then every later record, filtered.
//!
//! **What can be read.** A source name is checked against what `diag.sources` lists, never
//! turned into a path: a sink file is found by the sink's own list of the files it wrote
//! (`wasit_diag::read`), a shim log by a session id that parses (12 base32 characters) and that
//! the session manager knows, and the path is that session's directory plus the fixed name
//! `shim.log`. Nothing else — the session journal, `session.json`, the shim's raw stderr file
//! — has a source name, and the reader only returns files whose first line is a diagnostics
//! file header.
//!
//! **Nothing here blocks the runtime.** File reads run on the blocking pool; the tail is a
//! stream over the sink's broadcast, the same shape as the other topics. The service does not
//! write records about itself: a log reader that logged each read would feed the file it reads.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use serde_json::Value;
use tokio::sync::broadcast::error::RecvError;
use wasit_control::{CallContext, CloseReason, Cursor, EventStream, RpcError, Service};
use wasit_diag::{reader, Filter, Record};

use crate::manager::SessionManager;
use crate::service::{ok, params, runtime_err};
use crate::store::now_ms;
use crate::wire::{
    methods, topics, DiagChunk, DiagGen, DiagReadRequest, DiagSource, DiagSources, DiagTailEvent,
    DiagTailParams, RuntimeError, SessionId, DIAG_READ_MAX, DIAG_TAIL_TRACES,
};

/// The prefix of a session's shim log in a source name.
const SHIM_PREFIX: &str = "shim:";
/// The file of a session's directory a `shim:<id>` source reads.
const SHIM_LOG: &str = "shim.log";
/// What a `Reset` carries at most, in bytes of records (the newest that fit): escaped as JSON
/// strings it stays far below the frame limit.
const RESET_MAX_BYTES: usize = 2 << 20;
/// Longest `host` / `ev_prefix` filter value.
const FILTER_MAX: usize = 128;

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct DiagService {
    manager: SessionManager,
}

impl DiagService {
    /// `manager` is where the session directories of `shim:<id>` sources are found.
    pub fn new(manager: SessionManager) -> Self {
        DiagService { manager }
    }

    async fn sources(&self) -> Result<Value, RpcError> {
        let boot = wasit_diag::boot().ok_or_else(no_sink)?;
        let shims = self.manager.session_dirs();
        let sources = blocking(move || {
            let mut out: Vec<DiagSource> = wasit_diag::sources().into_iter().map(source).collect();
            // Sessions come in id order. One without a shim log (not started, or a build that
            // wrote none) is not a source.
            for (id, dir) in shims {
                let s = reader::source(&dir, SHIM_LOG);
                if !s.gens.is_empty() {
                    out.push(source(reader::Source {
                        name: format!("{SHIM_PREFIX}{id}"),
                        gens: s.gens,
                    }));
                }
            }
            out
        })
        .await?;
        // Taken last: the reader brackets this call with its own clock to measure the offset.
        ok(DiagSources {
            now_ms: now_ms(),
            boot,
            sources,
        })
    }

    async fn read(&self, r: DiagReadRequest) -> Result<Value, RpcError> {
        if r.max_bytes == 0 || r.max_bytes > DIAG_READ_MAX {
            return Err(runtime_err(RuntimeError::InvalidRequest(format!(
                "max_bytes is 1 to {DIAG_READ_MAX}"
            ))));
        }
        let max = r.max_bytes as usize;
        let chunk = match r.source.strip_prefix(SHIM_PREFIX) {
            Some(id) => {
                let dir = SessionId::parse(id)
                    .and_then(|id| self.manager.known_session_dir(&id))
                    .ok_or_else(unknown_source)?;
                blocking(move || reader::read(&dir, SHIM_LOG, r.gen, r.offset, max)).await?
            }
            None => {
                if wasit_diag::boot().is_none() {
                    return Err(no_sink());
                }
                blocking(move || wasit_diag::read(&r.source, r.gen, r.offset, max)).await?
            }
        }
        .map_err(read_err)?;
        ok(DiagChunk {
            data: chunk.data,
            next: chunk.next,
            eof: chunk.eof,
            gone: chunk.gone,
        })
    }

    fn tail(&self, p: DiagTailParams, from: Option<Cursor>) -> Result<EventStream, RpcError> {
        let filter = filter(p)?;
        // The ring's records and the receiver of every later one, taken together by the sink:
        // nothing between them is lost or repeated.
        let (ring, live) = wasit_diag::tail().ok_or_else(no_sink)?;
        let boot = wasit_diag::boot().ok_or_else(no_sink)?;
        let last = ring.last().map_or(0, |r| r.seq);
        let resume = from
            .as_ref()
            .and_then(|c| parse_cursor(c.as_str()))
            .filter(|(b, n)| *b == boot && in_ring(&ring, *n));
        let head: Vec<Result<(Cursor, Value), CloseReason>> = match resume {
            Some((_, after)) => ring
                .iter()
                .filter(|r| r.seq > after && filter.matches(r))
                .map(|r| item(&boot, r.seq, record_event(r)))
                .collect(),
            None => vec![item(&boot, last, reset_event(&ring, &filter))],
        };
        let tail = stream::unfold(Some((live, filter, boot)), |state| async move {
            let (mut rx, filter, boot) = state?;
            loop {
                match rx.recv().await {
                    Ok(r) if filter.matches(&r) => {
                        let out = item(&boot, r.seq, record_event(&r));
                        return Some((out, Some((rx, filter, boot))));
                    }
                    Ok(_) => {}
                    // Too slow: closed as lagged; the subscriber resumes from its last cursor.
                    Err(RecvError::Lagged(_)) => return Some((Err(CloseReason::Lagged), None)),
                    Err(RecvError::Closed) => return None,
                }
            }
        });
        Ok(stream::iter(head).chain(tail).boxed())
    }
}

#[async_trait]
impl Service for DiagService {
    async fn call(&self, _ctx: CallContext, method: &str, p: Value) -> Result<Value, RpcError> {
        match method {
            methods::DIAG_SOURCES => self.sources().await,
            methods::DIAG_READ => self.read(params(p)?).await,
            other => Err(RpcError::method_not_found(other)),
        }
    }

    async fn subscribe(
        &self,
        _ctx: CallContext,
        topic: &str,
        p: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        if topic != topics::DIAG_TAIL {
            return Err(RpcError::topic_not_found(topic));
        }
        let p: DiagTailParams = if p.is_null() {
            DiagTailParams::default()
        } else {
            params(p)?
        };
        self.tail(p, from)
    }
}

fn no_sink() -> RpcError {
    runtime_err(RuntimeError::Unsupported(
        "this process records no diagnostics".into(),
    ))
}

fn unknown_source() -> RpcError {
    runtime_err(RuntimeError::InvalidRequest(
        "unknown diagnostics source".into(),
    ))
}

/// A reader's failure as the caller's: a name the sink does not list or a line longer than the
/// chunk is the request's fault, anything else the host's disk.
fn read_err(e: std::io::Error) -> RpcError {
    use std::io::ErrorKind::{InvalidInput, NotFound};
    match e.kind() {
        NotFound => unknown_source(),
        InvalidInput => runtime_err(RuntimeError::InvalidRequest(e.to_string())),
        _ => runtime_err(RuntimeError::Io(format!("read diagnostics: {e}"))),
    }
}

/// Runs file I/O on the blocking pool.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, RpcError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| runtime_err(RuntimeError::Io(format!("diagnostics reader failed: {e}"))))
}

fn source(s: reader::Source) -> DiagSource {
    DiagSource {
        name: s.name,
        gens: s
            .gens
            .into_iter()
            .map(|g| DiagGen {
                gen: g.gen,
                bytes: g.bytes,
                first_t: g.first_t,
                last_t: g.last_t,
            })
            .collect(),
    }
}

fn filter(p: DiagTailParams) -> Result<Filter, RpcError> {
    if p.traces.len() > DIAG_TAIL_TRACES {
        return Err(runtime_err(RuntimeError::InvalidRequest(format!(
            "at most {DIAG_TAIL_TRACES} traces"
        ))));
    }
    for v in p.host.iter().chain(p.ev_prefix.iter()) {
        if v.len() > FILTER_MAX {
            return Err(runtime_err(RuntimeError::InvalidRequest(format!(
                "a filter is at most {FILTER_MAX} bytes"
            ))));
        }
    }
    Ok(Filter {
        host: p.host,
        session: p.session.map(|s| s.0),
        traces: p.traces.iter().map(|t| t.as_str().to_owned()).collect(),
        ev_prefix: p.ev_prefix,
    })
}

/// `"<boot>.<seq>"`.
fn parse_cursor(c: &str) -> Option<(&str, u64)> {
    let (boot, seq) = c.split_once('.')?;
    Some((boot, seq.parse().ok()?))
}

/// The ring (seqs are consecutive) still holds every record after `n`, and `n` is not from the
/// future. An empty ring holds only the start (`0`).
fn in_ring(ring: &[Arc<Record>], n: u64) -> bool {
    match (ring.first(), ring.last()) {
        (Some(first), Some(last)) => n + 1 >= first.seq && n <= last.seq,
        _ => n == 0,
    }
}

fn record_event(r: &Record) -> DiagTailEvent {
    DiagTailEvent::Record {
        line: r.line.to_string(),
    }
}

/// The matching records of the ring, oldest first: the newest that fit [`RESET_MAX_BYTES`].
fn reset_event(ring: &[Arc<Record>], filter: &Filter) -> DiagTailEvent {
    let mut bytes = 0usize;
    let mut records: Vec<String> = ring
        .iter()
        .rev()
        .filter(|r| filter.matches(r))
        .take_while(|r| {
            bytes += r.line.len();
            bytes <= RESET_MAX_BYTES
        })
        .map(|r| r.line.to_string())
        .collect();
    records.reverse();
    DiagTailEvent::Reset { records }
}

fn item(boot: &str, seq: u64, ev: DiagTailEvent) -> Result<(Cursor, Value), CloseReason> {
    serde_json::to_value(&ev)
        .map(|v| (Cursor::new(format!("{boot}.{seq}")), v))
        .map_err(|e| CloseReason::Error {
            error: RpcError::internal(e.to_string()),
        })
}
