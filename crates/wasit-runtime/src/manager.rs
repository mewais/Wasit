//! [`SessionManager`]: the per-daemon owner of all sessions under one data directory.
//!
//! It creates sessions (render config → `session.json` → spawn shim → start actor), re-adopts
//! them on start ([`SessionManager::adopt_all`]: live shims get an actor that rebuilds state by
//! replaying the journal; finished sessions get a snapshot from an offline replay and an actor
//! only when someone needs one), and routes every request to the session's actor. No lock is
//! held across an await; every wait is bounded.
//!
//! Dropping the manager stops its actors (not the sessions: shims keep running and a new
//! manager re-adopts them).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use futures::stream::StreamExt;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::JoinHandle;
use wasit_core::{DriverDescriptor, ResumeMode, SpawnSpec};
use wasit_shim::ShimState;

use crate::actor::{Actor, ActorInit, GateOp, HistoryLocal, Msg};
use crate::config::RuntimeConfig;
use crate::core::blank_snapshot;
use crate::host::HostFeed;
use crate::replay;
use crate::store::{self, SessionFile};
use crate::stream::{live, EventStream, StreamEnd};
use crate::wire::{
    CommandError, CommandOutcome, CreateSession, CreateSessionReply, HistoryPage, HostEvent, Phase,
    ResumeRequest, RuntimeError, SessionEvent, SessionId, SessionSnapshot,
};

/// Mailbox capacity of a session actor.
const MAILBOX: usize = 64;
/// Concurrent offline replays (snapshots of finished sessions) during adoption.
const REPLAY_CONCURRENCY: usize = 4;
/// Upper bound of a history page.
pub const MAX_HISTORY_PAGE: usize = 2000;
/// Longest Unix socket path we allow for a session (`sockaddr_un` holds ~104–108 bytes).
#[cfg(unix)]
const MAX_SOCKET_PATH: usize = 100;

/// Stream of a session's events.
pub type SessionEventStream = EventStream<SessionEvent>;
/// Stream of `host.sessions` events with their cursors.
pub type HostEventStream = EventStream<(String, HostEvent)>;

/// What [`SessionManager::adopt_all`] found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AdoptReport {
    /// Sessions with a live shim (an actor is attached).
    pub running: Vec<SessionId>,
    /// Finished sessions (snapshot rebuilt in the background).
    pub finished: Vec<SessionId>,
    /// Directories that could not be adopted, with the reason.
    pub errors: Vec<(PathBuf, String)>,
}

struct ActorHandle {
    tx: mpsc::Sender<Msg>,
    task: JoinHandle<()>,
}

struct Entry {
    dir: PathBuf,
    file: SessionFile,
    desc: Arc<dyn DriverDescriptor>,
    snapshot: watch::Sender<Arc<SessionSnapshot>>,
    actor: Option<ActorHandle>,
    loader: Option<JoinHandle<()>>,
}

struct Inner {
    cfg: Arc<RuntimeConfig>,
    sessions: Mutex<BTreeMap<SessionId, Entry>>,
    feed: Arc<HostFeed>,
    feed_task: JoinHandle<()>,
    replays: Arc<Semaphore>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.feed_task.abort();
        let sessions = self.sessions.get_mut().unwrap_or_else(|e| e.into_inner());
        for e in sessions.values_mut() {
            if let Some(a) = e.actor.take() {
                a.task.abort();
            }
            if let Some(l) = e.loader.take() {
                l.abort();
            }
        }
    }
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<Inner>,
}

fn lock(m: &Mutex<BTreeMap<SessionId, Entry>>) -> MutexGuard<'_, BTreeMap<SessionId, Entry>> {
    // Entries are replaced whole; a poisoned map is still consistent.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn io_err(what: &str, e: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Io(format!("{what}: {e}"))
}

impl SessionManager {
    /// A manager over `cfg.data_dir` (created if missing). Call [`SessionManager::adopt_all`]
    /// next to pick up existing sessions. Must be called inside a tokio runtime.
    pub async fn new(cfg: RuntimeConfig) -> Result<Self, RuntimeError> {
        let sessions_dir = cfg.sessions_dir();
        tokio::fs::create_dir_all(&sessions_dir)
            .await
            .map_err(|e| io_err(&format!("create {}", sessions_dir.display()), e))?;
        #[cfg(unix)]
        {
            let probe = sessions_dir
                .join("x".repeat(SessionId::LEN))
                .join("ctl.sock");
            let len = probe.as_os_str().len();
            if len > MAX_SOCKET_PATH {
                return Err(RuntimeError::InvalidRequest(format!(
                    "data directory {} is too long: session sockets would be {len} bytes (max {MAX_SOCKET_PATH})",
                    cfg.data_dir.display()
                )));
            }
        }
        let feed = HostFeed::new();
        let feed_task = tokio::spawn(feed.clone().run());
        Ok(SessionManager {
            inner: Arc::new(Inner {
                cfg: Arc::new(cfg),
                sessions: Mutex::new(BTreeMap::new()),
                feed,
                feed_task,
                replays: Arc::new(Semaphore::new(REPLAY_CONCURRENCY)),
            }),
        })
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.inner.cfg
    }

    fn session_dir(&self, id: &SessionId) -> PathBuf {
        self.inner.cfg.sessions_dir().join(id.as_str())
    }

    // ── create / adopt ──────────────────────────────────────────────────────

    /// Create and launch a session. Returns once the shim is up (the handshake progresses as
    /// events). Config items the driver cannot honour come back as `unsupported`.
    pub async fn create(&self, req: CreateSession) -> Result<CreateSessionReply, RuntimeError> {
        let cfg = &self.inner.cfg;
        let desc = cfg
            .registry
            .get(&req.driver)
            .map_err(|_| RuntimeError::UnknownDriver(req.driver.to_string()))?;
        let resume = match &req.resume {
            ResumeRequest::New => ResumeMode::Fresh,
            ResumeRequest::Continue { native_session_id } => {
                if !desc.capabilities().resume {
                    return Err(RuntimeError::Unsupported(format!(
                        "driver {} cannot resume sessions",
                        req.driver
                    )));
                }
                ResumeMode::Resume(native_session_id.clone())
            }
        };
        if let Some(cwd) = &req.cwd {
            match tokio::fs::metadata(cwd).await {
                Ok(m) if m.is_dir() => {}
                _ => {
                    return Err(RuntimeError::InvalidRequest(format!(
                        "working directory {cwd:?} does not exist"
                    )))
                }
            }
        }
        let id = loop {
            let id = SessionId::random();
            let taken = lock(&self.inner.sessions).contains_key(&id);
            if !taken
                && !tokio::fs::try_exists(self.session_dir(&id))
                    .await
                    .unwrap_or(true)
            {
                break id;
            }
        };
        let dir = self.session_dir(&id);
        let mut spec = SpawnSpec {
            cwd: req.cwd.clone(),
            session_dir: Some(dir.to_string_lossy().into_owned()),
            model: req.model.clone().filter(|m| !m.is_empty()),
            mode: req.mode.clone().unwrap_or_default(),
            first_prompt: req.first_prompt.clone().filter(|p| !p.is_empty()),
            resume,
            ..SpawnSpec::default()
        };
        store::apply_override(cfg, &req.driver, &mut spec);
        let unsupported = desc.render_config(&req.config, &mut spec);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| io_err(&format!("create {}", dir.display()), e))?;
        let file = SessionFile::new(
            id.clone(),
            req.driver.clone(),
            store::now_ms(),
            req.labels.clone(),
            spec,
            unsupported.clone(),
        );
        let launched = async {
            store::write_generated(&dir, &file.spec).await?;
            store::write(&dir, &file)
                .await
                .map_err(|e| io_err("write session.json", e))?;
            let shim = store::shim_spec(cfg, desc.as_ref(), &dir, &file.spec)?;
            wasit_shim::spawn_detached(&shim)
                .await
                .map_err(|e| RuntimeError::Spawn(e.to_string()))
        }
        .await;
        if let Err(e) = launched {
            if let Err(re) = tokio::fs::remove_dir_all(&dir).await {
                tracing::warn!(dir = %dir.display(), error = %re, "cannot clean up a failed session");
            }
            return Err(e);
        }
        let snap = Arc::new(blank_snapshot(
            &file.meta(),
            desc.capabilities(),
            Phase::Starting,
            true,
        ));
        self.insert(id.clone(), dir, file, desc, snap, true);
        Ok(CreateSessionReply { id, unsupported })
    }

    fn insert(
        &self,
        id: SessionId,
        dir: PathBuf,
        file: SessionFile,
        desc: Arc<dyn DriverDescriptor>,
        snap: Arc<SessionSnapshot>,
        start_actor: bool,
    ) {
        let (snapshot, _) = watch::channel(snap.clone());
        let mut entry = Entry {
            dir,
            file,
            desc,
            snapshot,
            actor: None,
            loader: None,
        };
        if start_actor {
            entry.actor = Some(self.start_actor(&entry));
        }
        lock(&self.inner.sessions).insert(id, entry);
        self.inner.feed.upsert(snap);
    }

    fn start_actor(&self, e: &Entry) -> ActorHandle {
        let feed = self.inner.feed.clone();
        let actor = Actor::new(ActorInit {
            dir: e.dir.clone(),
            desc: e.desc.clone(),
            cfg: self.inner.cfg.clone(),
            file: e.file.clone(),
            snapshot: e.snapshot.clone(),
            notify: Arc::new(move |s| feed.upsert(s.clone())),
        });
        let (tx, rx) = mpsc::channel(MAILBOX);
        let task = tokio::spawn(actor.run(rx));
        ActorHandle { tx, task }
    }

    /// Scan the data directory and adopt every session not yet known: attach actors to live
    /// shims (their full state, pending gates included, is rebuilt by replay) and rebuild the
    /// snapshots of finished sessions in the background.
    pub async fn adopt_all(&self) -> Result<AdoptReport, RuntimeError> {
        let sessions_dir = self.inner.cfg.sessions_dir();
        let mut rd = tokio::fs::read_dir(&sessions_dir)
            .await
            .map_err(|e| io_err(&format!("read {}", sessions_dir.display()), e))?;
        let mut report = AdoptReport::default();
        loop {
            let ent = match rd.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => return Err(io_err("scan sessions", e)),
            };
            let dir = ent.path();
            let name = ent.file_name().to_string_lossy().into_owned();
            let Some(id) = SessionId::parse(&name) else {
                continue;
            };
            if lock(&self.inner.sessions).contains_key(&id) {
                continue;
            }
            match self.adopt_one(&id, &dir).await {
                Ok(true) => report.running.push(id),
                Ok(false) => report.finished.push(id),
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %e, "cannot adopt session");
                    report.errors.push((dir, e));
                }
            }
        }
        Ok(report)
    }

    async fn adopt_one(&self, id: &SessionId, dir: &Path) -> Result<bool, String> {
        let file = store::read(dir).await?;
        if &file.id != id {
            return Err(format!("session.json names {} instead", file.id));
        }
        let desc = self
            .inner
            .cfg
            .registry
            .get(&file.driver)
            .map_err(|e| e.to_string())?;
        let state = wasit_shim::probe(dir).await.map_err(|e| e.to_string())?;
        let meta = file.meta();
        if state == ShimState::Running {
            let snap = Arc::new(blank_snapshot(
                &meta,
                desc.capabilities(),
                Phase::Starting,
                false,
            ));
            self.insert(id.clone(), dir.to_path_buf(), file, desc, snap, true);
            return Ok(true);
        }
        let phase = match state {
            ShimState::Exited(info) if info.code == Some(0) => Phase::Exited { code: info.code },
            ShimState::Exited(info) => Phase::Failed {
                reason: match (info.code, info.signal) {
                    (Some(c), _) => format!("the CLI exited with code {c}"),
                    (None, Some(s)) => format!("the CLI was killed by signal {s}"),
                    _ => "the CLI exited".into(),
                },
                stderr_tail: Vec::new(),
            },
            _ => Phase::Lost {
                reason: "the shim is gone and the journal has no exit record".into(),
            },
        };
        let snap = Arc::new(blank_snapshot(&meta, desc.capabilities(), phase, false));
        let (tx, loader) = {
            let watch_tx = {
                let (snapshot, _) = watch::channel(snap.clone());
                snapshot
            };
            let dir2 = dir.to_path_buf();
            let desc2 = desc.clone();
            let feed = self.inner.feed.clone();
            let permits = self.inner.replays.clone();
            let w = watch_tx.clone();
            let loader = tokio::spawn(async move {
                // The semaphore is never closed.
                let Ok(_permit) = permits.acquire_owned().await else {
                    return;
                };
                match replay::snapshot(&dir2, desc2, &meta).await {
                    Ok(s) => {
                        let s = Arc::new(s);
                        w.send_replace(s.clone());
                        feed.upsert(s);
                    }
                    Err(e) => {
                        tracing::warn!(dir = %dir2.display(), error = %e, "cannot rebuild snapshot")
                    }
                }
            });
            (watch_tx, loader)
        };
        lock(&self.inner.sessions).insert(
            id.clone(),
            Entry {
                dir: dir.to_path_buf(),
                file,
                desc,
                snapshot: tx,
                actor: None,
                loader: Some(loader),
            },
        );
        self.inner.feed.upsert(snap);
        Ok(false)
    }

    // ── queries ─────────────────────────────────────────────────────────────

    /// Every session's current snapshot, oldest first.
    pub fn list(&self) -> Vec<SessionSnapshot> {
        let mut v: Vec<SessionSnapshot> = lock(&self.inner.sessions)
            .values()
            .map(|e| (**e.snapshot.borrow()).clone())
            .collect();
        v.sort_by(|a, b| a.created_at_ms.cmp(&b.created_at_ms).then(a.id.cmp(&b.id)));
        v
    }

    pub fn snapshot(&self, id: &SessionId) -> Result<SessionSnapshot, RuntimeError> {
        lock(&self.inner.sessions)
            .get(id)
            .map(|e| (**e.snapshot.borrow()).clone())
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))
    }

    /// A `watch` of a session's snapshot (e.g. to await a phase).
    pub fn watch(
        &self,
        id: &SessionId,
    ) -> Result<watch::Receiver<Arc<SessionSnapshot>>, RuntimeError> {
        lock(&self.inner.sessions)
            .get(id)
            .map(|e| e.snapshot.subscribe())
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))
    }

    /// Subscribe to the session list (see [`crate::wire::HostEvent`]).
    pub fn subscribe_host(&self, from: Option<&str>) -> HostEventStream {
        self.inner.feed.subscribe(from)
    }

    // ── actor plumbing ──────────────────────────────────────────────────────

    /// The actor's mailbox, starting the actor if the session had none.
    fn mailbox(&self, id: &SessionId) -> Result<mpsc::Sender<Msg>, RuntimeError> {
        let mut map = lock(&self.inner.sessions);
        let e = map
            .get_mut(id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        if let Some(a) = &e.actor {
            if !a.task.is_finished() {
                return Ok(a.tx.clone());
            }
        }
        if let Some(l) = e.loader.take() {
            l.abort();
        }
        let handle = self.start_actor(e);
        let tx = handle.tx.clone();
        e.actor = Some(handle);
        Ok(tx)
    }

    async fn ask<T>(
        &self,
        id: &SessionId,
        make: impl FnOnce(oneshot::Sender<T>) -> Msg,
    ) -> Result<T, RuntimeError> {
        let tx = self.mailbox(id)?;
        let (rtx, rrx) = oneshot::channel();
        let t = &self.inner.cfg.timeouts;
        match tokio::time::timeout(t.mailbox, tx.send(make(rtx))).await {
            Err(_) => {
                return Err(RuntimeError::Busy(format!(
                    "session {id} is not keeping up"
                )))
            }
            Ok(Err(_)) => return Err(RuntimeError::SessionNotFound(id.to_string())),
            Ok(Ok(())) => {}
        }
        // The actor answers every request within its own deadlines; this bound only guards
        // against a crashed actor.
        let bound = t.command + t.mailbox * 2;
        match tokio::time::timeout(bound, rrx).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(_)) => Err(RuntimeError::SessionNotFound(id.to_string())),
            Err(_) => Err(RuntimeError::Timeout(format!(
                "session {id} did not answer"
            ))),
        }
    }

    // ── commands ────────────────────────────────────────────────────────────

    /// Send a command; the result says whether it was written, queued by the protocol, or why
    /// not.
    pub async fn command(
        &self,
        id: &SessionId,
        command: wasit_core::Command,
    ) -> Result<CommandOutcome, CommandError> {
        match self.ask(id, |reply| Msg::Command { command, reply }).await {
            Ok(r) => r,
            Err(RuntimeError::SessionNotFound(id)) => Err(CommandError::SessionNotFound { id }),
            Err(RuntimeError::Busy(_)) => Err(CommandError::Busy),
            Err(RuntimeError::Timeout(_)) => Err(CommandError::Timeout),
            Err(e) => Err(CommandError::Rejected {
                reason: e.to_string(),
            }),
        }
    }

    /// Raise an *external* gate on a running session: a gate that does not come from the CLI
    /// (e.g. an orchestration handoff awaiting approval). It is journaled, so it appears in
    /// the snapshot's `pending_gates` and as a `gate_requested` event exactly like a CLI gate
    /// and survives a daemon restart (replay). It stays pending across turn ends until it is
    /// answered through the normal [`Command::AnswerGate`](wasit_core::Command::AnswerGate) /
    /// `AnswerQuestion` path — which journals the answer as an `input` event (the gate's owner
    /// watches the session's events for it) and writes nothing to the CLI — or withdrawn with
    /// [`SessionManager::resolve_gate`]. It ends with the process generation.
    ///
    /// Rejected when the request id is empty or already pending; `NotRunning` when no process
    /// runs (an external gate cannot be journaled without a live shim).
    pub async fn raise_gate(
        &self,
        id: &SessionId,
        gate: wasit_core::GateRequest,
    ) -> Result<(), CommandError> {
        self.gate_op(id, GateOp::Raise(gate)).await
    }

    /// Withdraw a pending external gate without an answer (emits `gate_resolved`).
    pub async fn resolve_gate(&self, id: &SessionId, request_id: &str) -> Result<(), CommandError> {
        self.gate_op(
            id,
            GateOp::Resolve {
                request_id: request_id.to_string(),
            },
        )
        .await
    }

    async fn gate_op(&self, id: &SessionId, op: GateOp) -> Result<(), CommandError> {
        match self.ask(id, |reply| Msg::Gate { op, reply }).await {
            Ok(r) => r.map(|_| ()),
            Err(RuntimeError::SessionNotFound(id)) => Err(CommandError::SessionNotFound { id }),
            Err(RuntimeError::Busy(_)) => Err(CommandError::Busy),
            Err(RuntimeError::Timeout(_)) => Err(CommandError::Timeout),
            Err(e) => Err(CommandError::Rejected {
                reason: e.to_string(),
            }),
        }
    }

    /// Request a graceful stop (Shutdown → close stdin → TERM → KILL, with the configured
    /// grace between steps). Returns once the request is journaled; the exit arrives as events.
    pub async fn stop(&self, id: &SessionId) -> Result<(), RuntimeError> {
        self.ask(id, |reply| Msg::Stop { reply }).await?
    }

    /// Start a new process generation for a session whose process has ended, resuming the
    /// CLI's conversation by its native session id. History and cursors continue.
    pub async fn continue_session(&self, id: &SessionId) -> Result<(), RuntimeError> {
        self.ask(id, |reply| Msg::Continue { reply }).await?
    }

    /// Stop the session (waiting, bounded, for the process to exit) and remove its directory.
    pub async fn delete(&self, id: &SessionId) -> Result<(), RuntimeError> {
        let mut watch = self.watch(id)?;
        if watch.borrow().phase.is_live() {
            self.stop(id).await?;
            let waited = tokio::time::timeout(
                self.inner.cfg.timeouts.delete,
                watch.wait_for(|s| !s.phase.is_live()),
            )
            .await;
            if waited.is_err() {
                return Err(RuntimeError::Timeout(format!(
                    "session {id} did not exit; not deleted"
                )));
            }
        }
        let entry = lock(&self.inner.sessions).remove(id);
        let Some(mut entry) = entry else {
            return Err(RuntimeError::SessionNotFound(id.to_string()));
        };
        if let Some(a) = entry.actor.take() {
            a.task.abort();
        }
        if let Some(l) = entry.loader.take() {
            l.abort();
        }
        self.inner.feed.remove(id);
        tokio::fs::remove_dir_all(&entry.dir)
            .await
            .map_err(|e| io_err(&format!("remove {}", entry.dir.display()), e))
    }

    // ── events ──────────────────────────────────────────────────────────────

    /// The last `limit` events with `cursor < before` (all when `None`), oldest first.
    pub async fn history(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
    ) -> Result<HistoryPage, RuntimeError> {
        let limit = limit.clamp(1, MAX_HISTORY_PAGE);
        let (has_actor, dir, desc, meta) = {
            let map = lock(&self.inner.sessions);
            let e = map
                .get(id)
                .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
            (
                e.actor.as_ref().is_some_and(|a| !a.task.is_finished()),
                e.dir.clone(),
                e.desc.clone(),
                e.file.meta(),
            )
        };
        if has_actor {
            if let HistoryLocal::Page(p) = self
                .ask(id, |reply| Msg::History {
                    before,
                    limit,
                    reply,
                })
                .await?
            {
                return Ok(p);
            }
        }
        replay::history(&dir, desc, &meta, before, limit)
            .await
            .map_err(RuntimeError::Io)
    }

    /// Subscribe to a session's events strictly after `from` (from the beginning when `None`),
    /// then live. Gap- and duplicate-free; ends with `Err(Lagged)` if the subscriber falls
    /// behind, and when the session is deleted.
    pub async fn subscribe(
        &self,
        id: &SessionId,
        from: Option<u64>,
    ) -> Result<SessionEventStream, RuntimeError> {
        let r = self.ask(id, |reply| Msg::Subscribe { from, reply }).await?;
        let (dir, desc, meta) = {
            let map = lock(&self.inner.sessions);
            let e = map
                .get(id)
                .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
            (e.dir.clone(), e.desc.clone(), e.file.meta())
        };
        let head = match r.replay_below {
            Some(below) => {
                let rx = replay::events_between(dir, desc, meta, from, below);
                futures::stream::unfold(rx, |mut rx| async move {
                    rx.recv()
                        .await
                        .map(|item| (item.map_err(StreamEnd::Error), rx))
                })
                .boxed()
            }
            None => futures::stream::empty().boxed(),
        };
        Ok(EventStream::new(
            head.chain(futures::stream::iter(r.backlog.into_iter().map(Ok)))
                .chain(live(r.live, r.lagged))
                .boxed(),
        ))
    }
}
