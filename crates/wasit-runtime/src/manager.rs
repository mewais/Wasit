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
use wasit_control::TraceId;
use wasit_core::{DriverDescriptor, ResumeMode, SpawnSpec};
use wasit_shim::ShimState;

use crate::actor::{Actor, ActorInit, GateOp, Local, Msg};
use crate::config::RuntimeConfig;
use crate::core::blank_snapshot;
use crate::diag::{self, Intent};
use crate::host::HostFeed;
use crate::replay;
use crate::store::{self, SessionFile};
use crate::stream::{live, EventStream, StreamEnd};
use crate::wire::{
    CommandError, CommandOutcome, CreateSession, CreateSessionReply, HistoryPage, HistoryScope,
    HostEvent, Phase, ResumeRequest, RuntimeError, SessionEvent, SessionId, SessionSnapshot,
    TrailPage,
};

/// Bound on a user-given display name.
const DISPLAY_NAME_CHARS: usize = 200;
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
    /// The actor's `session.json` writer (ends after the actor).
    writer: Option<JoinHandle<()>>,
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

/// A user-given name as stored: trimmed, bounded, `None` when blank. A name at create and a later
/// rename are the same thing.
fn normalise_name(name: Option<String>) -> Option<String> {
    name.map(|n| {
        n.trim()
            .chars()
            .take(DISPLAY_NAME_CHARS)
            .collect::<String>()
    })
    .filter(|n| !n.is_empty())
}

fn io_err(what: &str, e: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Io(format!("{what}: {e}"))
}

impl SessionManager {
    /// A manager over `cfg.data_dir` (created if missing). Call [`SessionManager::adopt_all`]
    /// next to pick up existing sessions. Must be called inside a tokio runtime.
    pub async fn new(cfg: RuntimeConfig) -> Result<Self, RuntimeError> {
        let sessions_dir = cfg.sessions_dir();
        store::private_dir_all(&sessions_dir)
            .await
            .map_err(|e| io_err(&format!("create {}", sessions_dir.display()), e))?;
        #[cfg(unix)]
        {
            let probe = cfg
                .run_sessions_dir()
                .join("x".repeat(SessionId::LEN))
                .join("ctl.sock");
            let len = probe.as_os_str().len();
            if len > MAX_SOCKET_PATH {
                return Err(RuntimeError::InvalidRequest(format!(
                    "runtime directory {} is too long: session sockets would be {len} bytes (max {MAX_SOCKET_PATH})",
                    cfg.run_dir.as_deref().unwrap_or(&cfg.data_dir).display()
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

    /// The directory of a session this manager knows (`None`: no such session, so no path is
    /// ever made from an id a caller merely named).
    pub(crate) fn known_session_dir(&self, id: &SessionId) -> Option<PathBuf> {
        lock(&self.inner.sessions).get(id).map(|e| e.dir.clone())
    }

    /// Every known session and its directory.
    pub(crate) fn session_dirs(&self) -> Vec<(SessionId, PathBuf)> {
        lock(&self.inner.sessions)
            .iter()
            .map(|(id, e)| (id.clone(), e.dir.clone()))
            .collect()
    }

    // ── create / adopt ──────────────────────────────────────────────────────

    /// Create and launch a session. Returns once the shim is up (the handshake progresses as
    /// events). Config items the driver cannot honour come back as `unsupported`.
    pub async fn create(&self, req: CreateSession) -> Result<CreateSessionReply, RuntimeError> {
        self.create_traced(req, None).await
    }

    /// [`SessionManager::create`] for a request that carries a trace id: it is journaled with the
    /// session's `start` note, so the first prompt's `input` event (and `started`) carry it.
    pub async fn create_traced(
        &self,
        req: CreateSession,
        trace: Option<TraceId>,
    ) -> Result<CreateSessionReply, RuntimeError> {
        // First, before anything is awaited, journaled, logged or spawned: from here on no text
        // recorded by this process can hold the session's secrets or its directory.
        crate::scrub::agent_config(&req.config);
        crate::scrub::session_cwd(req.cwd.as_deref());
        let cfg = &self.inner.cfg;
        let name = normalise_name(req.name.clone());
        let desc = cfg
            .registry
            .get(&req.driver)
            .map_err(|_| RuntimeError::UnknownDriver(req.driver.to_string()))?;
        let resume = match &req.resume {
            ResumeRequest::New => ResumeMode::Fresh,
            ResumeRequest::Latest => {
                if !desc.capabilities().continue_latest {
                    return Err(RuntimeError::Unsupported(format!(
                        "driver {} cannot continue its most recent session",
                        req.driver
                    )));
                }
                ResumeMode::Continue
            }
            ResumeRequest::Resume { native_session_id } => {
                if !desc.capabilities().resume {
                    return Err(RuntimeError::Unsupported(format!(
                        "driver {} cannot resume sessions",
                        req.driver
                    )));
                }
                ResumeMode::Resume(native_session_id.clone())
            }
        };
        // The one normalisation of the project folder (see `crate::cwd`): everything below, and
        // everything that later reads this session, uses the result.
        let cwd = match &req.cwd {
            Some(given) => {
                let resolved = crate::cwd::resolve(given, cfg.home.as_deref())
                    .await
                    .map_err(|e| RuntimeError::WorkingDirectory(e.to_string()))?;
                crate::scrub::session_cwd(Some(&resolved));
                Some(resolved)
            }
            None => None,
        };
        let id = match &req.id {
            Some(id) => {
                let taken = lock(&self.inner.sessions).contains_key(id)
                    || tokio::fs::try_exists(self.session_dir(id))
                        .await
                        .unwrap_or(true);
                if taken {
                    return Err(RuntimeError::InvalidRequest(format!(
                        "session id {id} is already taken"
                    )));
                }
                id.clone()
            }
            None => loop {
                let id = SessionId::random();
                let taken = lock(&self.inner.sessions).contains_key(&id);
                if !taken
                    && !tokio::fs::try_exists(self.session_dir(&id))
                        .await
                        .unwrap_or(true)
                {
                    break id;
                }
            },
        };
        let dir = self.session_dir(&id);
        // A new conversation gets the id the runtime chooses when the agent accepts one: the
        // session knows its conversation from its creation, and ending it before the first
        // prompt loses nothing. Chosen here, journaled with the start, never drawn again.
        let native_session_id = match &resume {
            ResumeMode::Fresh => desc.chosen_session_id(fastrand::u128(..).to_le_bytes()),
            ResumeMode::Continue | ResumeMode::Resume(_) => None,
        };
        let mut spec = SpawnSpec {
            cwd,
            // The agent is told the name only when this session starts the conversation: the
            // title of a conversation that already exists (resumed, or the latest one) is the
            // user's, and a session's name is Marsad's.
            name: name.clone().filter(|_| resume == ResumeMode::Fresh),
            native_session_id,
            session_dir: Some(dir.to_string_lossy().into_owned()),
            model: req.model.clone().filter(|m| !m.is_empty()),
            mode: req.mode.clone().unwrap_or_default(),
            first_prompt: req.first_prompt.clone().filter(|p| !p.is_empty()),
            session_id: Some(id.to_string()),
            resume,
            ..SpawnSpec::default()
        };
        store::apply_override(cfg, &req.driver, &mut spec);
        let unsupported = desc.render_config(&req.config, &mut spec);
        store::private_dir_all(&dir)
            .await
            .map_err(|e| io_err(&format!("create {}", dir.display()), e))?;
        let mut file = SessionFile::new(
            id.clone(),
            req.driver.clone(),
            store::now_ms(),
            req.labels.clone(),
            spec,
            unsupported.clone(),
        );
        file.display_name = name;
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
                diag::cleanup_failed(&id, &re);
            }
            return Err(e);
        }
        let snap = Arc::new(blank_snapshot(
            &file.meta(),
            desc.as_ref(),
            Phase::Starting,
            true,
        ));
        let cwd = file.cwd.clone();
        self.insert(id.clone(), dir, file, desc, snap, trace);
        Ok(CreateSessionReply {
            id,
            cwd,
            unsupported,
        })
    }

    /// Registers a session and starts its actor; `start_trace` is journaled with its `start` note.
    fn insert(
        &self,
        id: SessionId,
        dir: PathBuf,
        file: SessionFile,
        desc: Arc<dyn DriverDescriptor>,
        snap: Arc<SessionSnapshot>,
        start_trace: Option<TraceId>,
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
        entry.actor = Some(self.start_actor(&entry, start_trace));
        lock(&self.inner.sessions).insert(id, entry);
        self.inner.feed.upsert(snap);
    }

    fn start_actor(&self, e: &Entry, start_trace: Option<TraceId>) -> ActorHandle {
        let feed = self.inner.feed.clone();
        let mut actor = Actor::new(ActorInit {
            dir: e.dir.clone(),
            desc: e.desc.clone(),
            cfg: self.inner.cfg.clone(),
            file: e.file.clone(),
            snapshot: e.snapshot.clone(),
            notify: Arc::new(move |s| feed.upsert(s.clone())),
            start_trace,
        });
        let writer = actor.take_writer();
        let (tx, rx) = mpsc::channel(MAILBOX);
        let task = tokio::spawn(actor.run(rx));
        ActorHandle { tx, task, writer }
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
                    diag::adopt_failed(&id, &e);
                    report.errors.push((dir, e));
                }
            }
        }
        Ok(report)
    }

    async fn adopt_one(&self, id: &SessionId, dir: &Path) -> Result<bool, String> {
        // A session an older build created may be readable by others: close it now. A failure
        // does not stop the adoption (the user's session still shows), it is reported.
        if let Err(e) = store::tighten_session(dir).await {
            diag::tighten_failed(id, &e);
        }
        let file = store::read(dir).await?;
        if &file.id != id {
            return Err(format!("session.json names {} instead", file.id));
        }
        // Before the session's actor exists: what a restarted daemon records about it must not
        // hold the values it was launched with or its directory.
        crate::scrub::spawn_env(&file.spec);
        crate::scrub::session_cwd(file.cwd.as_deref());
        let desc = self
            .inner
            .cfg
            .registry
            .get(&file.driver)
            .map_err(|e| e.to_string())?;
        let run = self.inner.cfg.session_run_dir(dir);
        let state = wasit_shim::probe_at(dir, &run)
            .await
            .map_err(|e| e.to_string())?;
        let meta = file.meta();
        if state == ShimState::Running {
            let snap = Arc::new(blank_snapshot(&meta, desc.as_ref(), Phase::Starting, false));
            self.insert(id.clone(), dir.to_path_buf(), file, desc, snap, None);
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
        let snap = Arc::new(blank_snapshot(&meta, desc.as_ref(), phase, false));
        let (tx, loader) = {
            let watch_tx = {
                let (snapshot, _) = watch::channel(snap.clone());
                snapshot
            };
            let dir2 = dir.to_path_buf();
            let run2 = run.clone();
            let desc2 = desc.clone();
            let feed = self.inner.feed.clone();
            let permits = self.inner.replays.clone();
            let w = watch_tx.clone();
            let sid = id.clone();
            let loader = tokio::spawn(async move {
                // The semaphore is never closed.
                let Ok(_permit) = permits.acquire_owned().await else {
                    return;
                };
                match replay::snapshot(&dir2, &run2, desc2, &meta).await {
                    Ok(s) => {
                        let s = Arc::new(s);
                        w.send_replace(s.clone());
                        feed.upsert(s);
                    }
                    Err(e) => diag::snapshot_failed(&sid, &e),
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

    /// The read roots of a session: its directory (blobs, attachments) and its cwd.
    fn roots(&self, id: &SessionId) -> Result<crate::media::Roots, RuntimeError> {
        let map = lock(&self.inner.sessions);
        let e = map
            .get(id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        let cwd = e
            .snapshot
            .borrow()
            .cwd
            .clone()
            .or_else(|| e.file.meta().cwd)
            .map(PathBuf::from);
        Ok(crate::media::Roots {
            session_dir: e.dir.clone(),
            cwd,
        })
    }

    /// `session.attachment.put`: one chunk of an upload into the session's attachments.
    pub async fn attachment_put(
        &self,
        r: &crate::media::AttachmentPut,
    ) -> Result<crate::media::AttachmentPutReply, RuntimeError> {
        let dir = self.roots(&r.id)?.session_dir;
        crate::media::put(&dir, r).await
    }

    /// `session.file.read`: a range of a blob or of a file inside the session's roots.
    pub async fn file_read(
        &self,
        r: &crate::media::FileReadRequest,
    ) -> Result<crate::media::FileChunk, RuntimeError> {
        let roots = self.roots(&r.id)?;
        if let Some(b) = &r.blob {
            if crate::media::stored_blob(&roots.session_dir, b)
                .await?
                .is_none()
            {
                // Not stored (yet): the journal has it; replay to find it.
                let (dir, desc, meta) = {
                    let map = lock(&self.inner.sessions);
                    let e = map
                        .get(&r.id)
                        .ok_or_else(|| RuntimeError::SessionNotFound(r.id.to_string()))?;
                    (e.dir.clone(), e.desc.clone(), e.file.meta())
                };
                let _permit = self
                    .inner
                    .replays
                    .acquire()
                    .await
                    .map_err(|_| RuntimeError::Busy("the runtime is shutting down".into()))?;
                match replay::find_blob(&dir, desc, &meta, b).await {
                    Ok(Some(bytes)) => crate::media::restore_blob(&dir, b, &bytes).await?,
                    Ok(None) => {
                        return Err(RuntimeError::InvalidRequest(format!("no such blob: {b}")))
                    }
                    Err(e) => return Err(RuntimeError::Io(e)),
                }
            }
        }
        crate::media::read(&roots, r).await
    }

    /// `session.files.stat`: which paths exist inside the session's roots.
    pub async fn files_stat(
        &self,
        r: &crate::media::FilesStatRequest,
    ) -> Result<crate::media::FilesStatReply, RuntimeError> {
        let roots = self.roots(&r.id)?;
        crate::media::stat(&roots, &r.paths).await
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
        let handle = self.start_actor(e, None);
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
        self.command_traced(id, command, None).await
    }

    /// [`SessionManager::command`] for a request that carries a trace id: it is journaled with
    /// the command and shows on its `input` event. A command refused before it was journaled is
    /// recorded (`session.command_refused`) with the trace.
    pub async fn command_traced(
        &self,
        id: &SessionId,
        mut command: wasit_core::Command,
        trace: Option<TraceId>,
    ) -> Result<CommandOutcome, CommandError> {
        let intent = Intent::new(command.name(), trace.clone());
        let refused = |e: CommandError| {
            diag::command_refused(id, &intent, &e);
            e
        };
        if let wasit_core::Command::Prompt { attachments, .. } = &mut command {
            if !attachments.is_empty() {
                let (dir, input) = {
                    let map = lock(&self.inner.sessions);
                    let e = map.get(id).ok_or_else(|| {
                        refused(CommandError::SessionNotFound { id: id.to_string() })
                    })?;
                    (e.dir.clone(), e.desc.prompt_input())
                };
                crate::media::load_attachments(&dir, &input, attachments)
                    .await
                    .map_err(|e| {
                        refused(match e {
                            RuntimeError::Unsupported(operation) => {
                                CommandError::Unsupported { operation }
                            }
                            e => CommandError::Rejected {
                                reason: e.to_string(),
                            },
                        })
                    })?;
            }
        }
        match self
            .ask(id, |reply| Msg::Command {
                command,
                trace,
                reply,
            })
            .await
        {
            // The actor answered: what it refused before journaling it has recorded itself.
            Ok(r) => r,
            Err(RuntimeError::SessionNotFound(id)) => {
                Err(refused(CommandError::SessionNotFound { id }))
            }
            Err(RuntimeError::Busy(_)) => Err(refused(CommandError::Busy)),
            // Nobody answered within the bound: the requester gives up, the actor may still
            // journal the command.
            Err(RuntimeError::Timeout(_)) => {
                diag::command_timeout(id, &intent);
                Err(CommandError::Timeout)
            }
            Err(e) => Err(refused(CommandError::Rejected {
                reason: e.to_string(),
            })),
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
        self.gate_op(id, GateOp::Raise(gate, None)).await
    }

    /// [`SessionManager::raise_gate`] for a request that belongs to the turn open when the
    /// raise is applied (an agent's tool call waiting for a human): the session itself resolves
    /// the gate, in the step that interrupts or ends that turn, ordered before the turn's own
    /// completion, with [`GateReason::TURN_INTERRUPTED`](wasit_core::GateReason::TURN_INTERRUPTED)
    /// or `TURN_ENDED` and the details of `binding`; the owner only observes `gate_resolved`.
    /// See [`TurnBinding`](wasit_core::TurnBinding) and ARCHITECTURE.md (external gates) for
    /// the exact rules, including a raise with no turn open. A `binding` whose details are not
    /// well formed ([`TurnBinding::check`](wasit_core::TurnBinding::check)) is refused.
    pub async fn raise_gate_for_turn(
        &self,
        id: &SessionId,
        gate: wasit_core::GateRequest,
        binding: wasit_core::TurnBinding,
    ) -> Result<(), CommandError> {
        if let Err(reason) = binding.check() {
            return Err(CommandError::Rejected { reason });
        }
        self.gate_op(id, GateOp::Raise(gate, Some(binding))).await
    }

    /// Take down a pending external gate without an answer (emits `gate_resolved`). `reason`
    /// is why, from the resolver: a machine word and optionally a short detail, journaled with
    /// the resolution and carried on the event. A reason that is not well formed
    /// ([`GateReason::check`]) is refused and nothing is resolved.
    pub async fn resolve_gate(
        &self,
        id: &SessionId,
        request_id: &str,
        reason: wasit_core::GateReason,
    ) -> Result<(), CommandError> {
        if let Err(reason) = reason.check() {
            return Err(CommandError::Rejected { reason });
        }
        self.gate_op(
            id,
            GateOp::Resolve {
                request_id: request_id.to_string(),
                reason,
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

    /// Set (or clear, with `None` or a blank name) the session's user-given display name. Only
    /// Marsad's name changes, never the agent's title of its conversation. Two agents could be
    /// told (claude 2.1.293 takes `/rename <name>` as a slash command over stream-json, which
    /// rewrites its transcript's title but is a turn of its own; codex has `thread/name/set`),
    /// and both need a live process; neither is wired, and nothing here pretends otherwise.
    pub async fn rename(&self, id: &SessionId, name: Option<String>) -> Result<(), RuntimeError> {
        let name = normalise_name(name);
        self.ask(id, |reply| Msg::Rename { name, reply }).await?
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
        // Nothing may write into the directory while it is removed: stop the actor and the
        // loader, then wait for them and for the actor's `session.json` writer to finish (an
        // update the exit caused is written by then; removing during that write fails with
        // "directory not empty").
        if let Some(a) = entry.actor.take() {
            a.task.abort();
            // Cancelled or finished: either way it no longer runs.
            let _ = a.task.await;
            if let Some(w) = a.writer {
                // The writer ends once the actor (its sender) is gone; a panic is logged by tokio.
                let _ = w.await;
            }
        }
        if let Some(l) = entry.loader.take() {
            l.abort();
            // Cancelled or finished: either way it no longer runs.
            let _ = l.await;
        }
        self.inner.feed.remove(id);
        let run = self.inner.cfg.session_run_dir_for_spawn(&entry.dir);
        if run != entry.dir {
            match tokio::fs::remove_dir_all(&run).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err(&format!("remove {}", run.display()), e)),
            }
        }
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
        self.history_scoped(id, before, limit, HistoryScope::All)
            .await
    }

    /// [`Self::history`] restricted to `scope` ([`HistoryScope`]): the session's own transcript
    /// or one subagent's. The cursors are those of the full stream; the limit and `has_more`
    /// count only events of the scope.
    pub async fn history_scoped(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
        scope: HistoryScope,
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
            if let Local::Page(p) = self
                .ask(id, |reply| Msg::History {
                    before,
                    limit,
                    scope: scope.clone(),
                    reply,
                })
                .await?
            {
                return Ok(p);
            }
        }
        replay::history(&dir, desc, &meta, before, limit, scope)
            .await
            .map_err(RuntimeError::Io)
    }

    /// The last `limit` entries of the session's trail with `cursor < before` (all when
    /// `None`), oldest first: the content-free projection of the events `history` returns
    /// ([`crate::trail`]), from the actor's memory when it holds them, else from a replay of the
    /// journal.
    pub async fn trail_page(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
    ) -> Result<TrailPage, RuntimeError> {
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
            if let Local::Page(p) = self
                .ask(id, |reply| Msg::Trail {
                    before,
                    limit,
                    reply,
                })
                .await?
            {
                return Ok(p);
            }
        }
        replay::trail(&dir, desc, &meta, before, limit)
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
