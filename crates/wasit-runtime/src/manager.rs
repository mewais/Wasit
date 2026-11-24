//! [`SessionManager`]: the per-daemon owner of all sessions under one data directory.
//!
//! It creates sessions (render config → `session.json` → spawn shim → start actor), re-adopts
//! them on start ([`SessionManager::adopt_all`]: live shims get an actor that rebuilds state by
//! replaying the journal; finished sessions take their snapshot from their sealed event log, or
//! from an offline build of it, and get an actor only when a command needs one), and routes
//! commands to the session's actor. History and trail pages and subscription backlogs are read
//! from the session's event log, never through the actor's memory and never by replaying the
//! journal, and reads never start an actor. No lock is held across an await.
//!
//! Dropping the manager stops its actors (not the sessions: shims keep running and a new
//! manager re-adopts them).

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use futures::stream::StreamExt;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::JoinHandle;
use wasit_control::TraceId;
use wasit_core::{DriverDescriptor, ResumeMode, SessionMode, SpawnSpec};
use wasit_shim::ShimState;

use crate::actor::{Actor, ActorInit, GateOp, Msg};
use crate::config::RuntimeConfig;
use crate::core::{blank_snapshot, SessionMeta};
use crate::diag::{self, Intent};
use crate::eventlog::{EventLog, LogFile, LogView};
use crate::host::HostFeed;
use crate::replay;
use crate::store::{self, SessionFile};
use crate::stream::EventStream;
use crate::subscribe::Feed;
use crate::wire::{
    page_budget, CommandError, CommandOutcome, CreateSession, CreateSessionReply, HistoryPage,
    HistoryRequest, HistoryScope, HostEvent, Phase, ResumeRequest, RuntimeError, SessionEvent,
    SessionId, SessionSnapshot, TrailPage, TrailRequest,
};

/// Bound on a user-given display name.
const DISPLAY_NAME_CHARS: usize = 200;
/// Mailbox capacity of a session actor.
const MAILBOX: usize = 64;
/// Concurrent offline replays (event log builds, blob lookups).
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
    /// The actor's event log writer (ends after the actor).
    log_writer: Option<JoinHandle<()>>,
}

struct Entry {
    dir: PathBuf,
    file: SessionFile,
    desc: Arc<dyn DriverDescriptor>,
    snapshot: watch::Sender<Arc<SessionSnapshot>>,
    actor: Option<ActorHandle>,
    /// Counts the actors started for the session, bumped under the map lock together with
    /// `actor`: a subscription that found no actor waits for it to change.
    started: watch::Sender<u64>,
    /// Where reads of the session's event log go: the actor's log while it runs, else the log
    /// the manager opened for reads (see [`SessionManager::log_view`]).
    log: Option<LogView>,
    /// The offline build of a log no actor owns; it holds the log's writer until it is done.
    log_task: Option<JoinHandle<()>>,
    /// The writer task of the log opened for reads (ends after `log_task` releases the log).
    log_writer: Option<JoinHandle<()>>,
}

struct Inner {
    cfg: Arc<RuntimeConfig>,
    sessions: Mutex<BTreeMap<SessionId, Entry>>,
    feed: Arc<HostFeed>,
    feed_task: JoinHandle<()>,
    replays: Arc<Semaphore>,
    /// Serializes restarting a session's actor (which awaits its loader and opens its event
    /// log with the entry left in the map) against other restarts and against `delete`: two
    /// writers must never own one log, and a directory is never removed under a restart.
    restarts: tokio::sync::Mutex<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.feed_task.abort();
        let sessions = self.sessions.get_mut().unwrap_or_else(|e| e.into_inner());
        for e in sessions.values_mut() {
            if let Some(a) = e.actor.take() {
                a.task.abort();
            }
            if let Some(b) = e.log_task.take() {
                b.abort();
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

/// Stops offline tasks that own derived files (an offline log build), then waits
/// for the log writers they handed their logs to: once this returns, nothing else writes the
/// session's derived files, and a new log may be opened.
async fn stop_derived(tasks: Vec<JoinHandle<()>>, writers: Vec<JoinHandle<()>>) {
    for t in &tasks {
        t.abort();
    }
    for t in tasks {
        // Cancelled or finished: either way it no longer runs.
        let _ = t.await;
    }
    for w in writers {
        // Ends once its log is dropped by the (now stopped) owner and the queue is drained.
        let _ = w.await;
    }
}

/// The offline owner of a session's log when no actor runs: builds the log from the journal
/// unless it is sealed (one of [`REPLAY_CONCURRENCY`] at a time), publishes the snapshot the
/// build arrives at, then drops the log, which lets its writer finish after the last record.
struct OfflineBuild {
    id: SessionId,
    dir: PathBuf,
    run_dir: PathBuf,
    desc: Arc<dyn DriverDescriptor>,
    meta: SessionMeta,
    hints: Vec<SessionMode>,
    permits: Arc<Semaphore>,
    snapshot: watch::Sender<Arc<SessionSnapshot>>,
    feed: Arc<HostFeed>,
}

impl OfflineBuild {
    async fn run(self, log: EventLog) {
        if let Some(sealed) = log.sealed_snapshot() {
            // Sealed by a writer that finished after the adoption looked (or by a build): the
            // seal's snapshot is the session's.
            let snap = Arc::new(sealed.clone());
            self.snapshot.send_replace(snap.clone());
            self.feed.upsert(snap);
            return;
        }
        // The semaphore is never closed.
        let Ok(_permit) = self.permits.acquire_owned().await else {
            return;
        };
        let built = replay::build(
            &self.dir,
            &self.run_dir,
            self.desc,
            &self.meta,
            &self.hints,
            &log,
        )
        .await;
        match built {
            Ok(snap) => {
                let snap = Arc::new(snap);
                self.snapshot.send_replace(snap.clone());
                self.feed.upsert(snap);
            }
            Err(e) => {
                let err = io::Error::other(e);
                diag::persist_failed(&self.id, &err);
                diag::snapshot_failed(&self.id, &err.to_string());
                // Reads answer with the failure instead of waiting for a build that stopped.
                let _ = log.fail(err.to_string());
            }
        }
    }
}

/// A [`SessionManager`] that does not keep it alive (held by subscriptions while they wait).
#[derive(Clone)]
pub(crate) struct WeakManager(Weak<Inner>);

impl WeakManager {
    pub(crate) fn upgrade(&self) -> Option<SessionManager> {
        self.0.upgrade().map(|inner| SessionManager { inner })
    }
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
                restarts: tokio::sync::Mutex::new(()),
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
            ResumeMode::Continue | ResumeMode::Resume(_) | ResumeMode::Fork(_) => None,
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
        // "Continue the most recent conversation here" is resolved by the runtime, from the
        // agent's own list of the conversations of the folder, and resumes that one by its id:
        // the agent's own "continue" would start a new conversation without a word when the
        // folder has none.
        if matches!(req.resume, ResumeRequest::Latest) {
            let folder = match &spec.cwd {
                Some(c) => c.clone(),
                None => std::env::current_dir()
                    .map(|p| p.to_string_lossy().into_owned())
                    .map_err(|e| io_err("the working directory", e))?,
            };
            match crate::discovery::latest_conversation(cfg, desc.as_ref(), &folder, &spec.env)
                .await
            {
                Ok(Some(found)) => spec.resume = ResumeMode::Resume(found.id),
                Ok(None) => return Err(RuntimeError::NothingToContinue(folder)),
                Err(e) => {
                    return Err(RuntimeError::Io(format!(
                        "cannot look for the most recent conversation in {folder}: {e}"
                    )))
                }
            }
        }
        // A conversation this session did not start has earlier history its journal lacks: the
        // runtime decides, before the agent writes to its storage again, how that reaches the
        // journal.
        let earlier = match &spec.resume {
            ResumeMode::Resume(native) => {
                crate::history::plan(cfg, desc.as_ref(), &spec, native).await
            }
            _ => None,
        };
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
        file.earlier = earlier;
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
        self.insert(id.clone(), dir, file, desc, snap, trace).await;
        Ok(CreateSessionReply {
            id,
            cwd,
            unsupported,
        })
    }

    /// Registers a session and starts its actor; `start_trace` is journaled with its `start` note.
    async fn insert(
        &self,
        id: SessionId,
        dir: PathBuf,
        file: SessionFile,
        desc: Arc<dyn DriverDescriptor>,
        snap: Arc<SessionSnapshot>,
        start_trace: Option<TraceId>,
    ) {
        let (snapshot, _) = watch::channel(snap.clone());
        // Not in the map yet: nothing else can open this log meanwhile.
        let event_log = self.open_log(&dir).await;
        let mut entry = Entry {
            dir,
            file,
            desc,
            snapshot,
            actor: None,
            started: watch::channel(0).0,
            log: event_log.as_ref().map(EventLog::view),
            log_task: None,
            log_writer: None,
        };
        let handle = self.start_actor(&entry, start_trace, event_log);
        entry.attach(handle);
        lock(&self.inner.sessions).insert(id, entry);
        self.inner.feed.upsert(snap);
    }

    /// The session's derived event log; `None` (the actor runs without one) if it cannot be
    /// opened. The caller guarantees no other writer has it open.
    async fn open_log(&self, dir: &Path) -> Option<crate::eventlog::EventLog> {
        let journal_len = tokio::fs::metadata(dir.join("journal.ndjson"))
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        crate::eventlog::EventLog::open(dir, self.inner.cfg.derivation.clone(), journal_len, None)
            .await
            .ok()
    }

    fn start_actor(
        &self,
        e: &Entry,
        start_trace: Option<TraceId>,
        event_log: Option<crate::eventlog::EventLog>,
    ) -> ActorHandle {
        let feed = self.inner.feed.clone();
        let mut actor = Actor::new(ActorInit {
            dir: e.dir.clone(),
            desc: e.desc.clone(),
            cfg: self.inner.cfg.clone(),
            file: e.file.clone(),
            snapshot: e.snapshot.clone(),
            notify: Arc::new(move |s| feed.upsert(s.clone())),
            start_trace,
            event_log,
        });
        let writer = actor.take_writer();
        let log_writer = actor.take_log_writer();
        let (tx, rx) = mpsc::channel(MAILBOX);
        crate::stats::actor_started();
        let task = tokio::spawn(actor.run(rx));
        ActorHandle {
            tx,
            task,
            writer,
            log_writer,
        }
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
        let mut found = Vec::new();
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
            // Newest first: offline builds queue for their permits in adoption order, so the
            // sessions a user most likely opens next are ready first. "Newest" is the last write
            // to the journal (the session directory when there is none).
            let modified = match tokio::fs::metadata(dir.join("journal.ndjson")).await {
                Ok(m) => m.modified().ok(),
                Err(_) => tokio::fs::metadata(&dir)
                    .await
                    .ok()
                    .and_then(|m| m.modified().ok()),
            };
            found.push((modified, id, dir));
        }
        found.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        for (_, id, dir) in found {
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
            self.insert(id.clone(), dir.to_path_buf(), file, desc, snap, None)
                .await;
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
        let journal_len = tokio::fs::metadata(dir.join("journal.ndjson"))
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let derivation = &self.inner.cfg.derivation;
        if let Some(sealed) = crate::eventlog::sealed_snapshot(dir, derivation, journal_len).await {
            // The log was sealed at the journal's present length: its snapshot is the session's,
            // with no replay and no log opened (the first read opens it).
            let snap = Arc::new(sealed);
            let (snapshot, _) = watch::channel(snap.clone());
            lock(&self.inner.sessions).insert(
                id.clone(),
                Entry {
                    dir: dir.to_path_buf(),
                    file,
                    desc,
                    snapshot,
                    actor: None,
                    started: watch::channel(0).0,
                    log: None,
                    log_task: None,
                    log_writer: None,
                },
            );
            self.inner.feed.upsert(snap);
            return Ok(false);
        }
        // Not sealed (or sealed under another build, or for a shorter journal): the snapshot
        // shown meanwhile is a placeholder until the offline build of the log replaces it.
        let snap = Arc::new(blank_snapshot(&meta, desc.as_ref(), phase, false));
        let (snapshot, _) = watch::channel(snap.clone());
        // Not in the map yet: nothing else can open this log meanwhile.
        let (log, log_task, log_writer) = match self.open_log(dir).await {
            Some(mut log) => {
                let view = log.view();
                let writer = log.take_writer();
                let build = OfflineBuild {
                    id: id.clone(),
                    dir: dir.to_path_buf(),
                    run_dir: run,
                    desc: desc.clone(),
                    meta: meta.clone(),
                    hints: self.inner.cfg.mode_hints.get(&meta.driver),
                    permits: self.inner.replays.clone(),
                    snapshot: snapshot.clone(),
                    feed: self.inner.feed.clone(),
                };
                (Some(view), Some(tokio::spawn(build.run(log))), writer)
            }
            None => {
                diag::snapshot_failed(id, "cannot open the event log");
                (None, None, None)
            }
        };
        lock(&self.inner.sessions).insert(
            id.clone(),
            Entry {
                dir: dir.to_path_buf(),
                file,
                desc,
                snapshot,
                actor: None,
                started: watch::channel(0).0,
                log,
                log_task,
                log_writer,
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
    async fn mailbox(&self, id: &SessionId) -> Result<mpsc::Sender<Msg>, RuntimeError> {
        if let Some(tx) = self.running_mailbox(id)? {
            return Ok(tx);
        }
        let _restart = self.inner.restarts.lock().await;
        // Another request may have restarted it while this one waited.
        if let Some(tx) = self.running_mailbox(id)? {
            return Ok(tx);
        }
        // The entry stays in the map (readers, `list`, a concurrent `mailbox` all still see
        // it). The offline owners of its derived files, and the writer of the finished actor's
        // log, must be gone before the log is opened: they may be writing the same files.
        let (dir, tasks, writers) = {
            let mut map = lock(&self.inner.sessions);
            let e = map
                .get_mut(id)
                .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
            let mut writers: Vec<JoinHandle<()>> = e
                .actor
                .take()
                .and_then(|a| a.log_writer)
                .into_iter()
                .collect();
            writers.extend(e.log_writer.take());
            let tasks: Vec<JoinHandle<()>> = e.log_task.take().into_iter().collect();
            e.log = None;
            (e.dir.clone(), tasks, writers)
        };
        stop_derived(tasks, writers).await;
        let event_log = self.open_log(&dir).await;
        let mut map = lock(&self.inner.sessions);
        // `delete` holds `restarts` too, so the entry is still there.
        let e = map
            .get_mut(id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        e.log = event_log.as_ref().map(EventLog::view);
        let handle = self.start_actor(e, None, event_log);
        let tx = handle.tx.clone();
        e.attach(handle);
        Ok(tx)
    }

    /// The mailbox of the session's actor if it runs; `None` if it must be (re)started.
    fn running_mailbox(&self, id: &SessionId) -> Result<Option<mpsc::Sender<Msg>>, RuntimeError> {
        let map = lock(&self.inner.sessions);
        let e = map
            .get(id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        Ok(match &e.actor {
            Some(a) if !a.task.is_finished() => Some(a.tx.clone()),
            _ => None,
        })
    }

    async fn ask<T>(
        &self,
        id: &SessionId,
        make: impl FnOnce(oneshot::Sender<T>) -> Msg,
    ) -> Result<T, RuntimeError> {
        let tx = self.mailbox(id).await?;
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
        // Not while a restart awaits this session's loader or opens its log.
        let _restart = self.inner.restarts.lock().await;
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
            if let Some(w) = a.log_writer {
                let _ = w.await;
            }
        }
        let tasks: Vec<JoinHandle<()>> = entry.log_task.take().into_iter().collect();
        let writers: Vec<JoinHandle<()>> = entry.log_writer.take().into_iter().collect();
        stop_derived(tasks, writers).await;
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

    /// Test support: once the session has exited, stop its actor and wait until its event log
    /// writer has finished, so every record the exit caused (including the seal) is on disk.
    /// The seal is queued before the exit's phase is published, so waiting for a non-live phase
    /// is enough. The actor restarts with the next request.
    #[doc(hidden)]
    pub async fn close_log(&self, id: &SessionId) -> Result<(), RuntimeError> {
        let mut watch = self.watch(id)?;
        let waited = tokio::time::timeout(
            self.inner.cfg.timeouts.delete,
            watch.wait_for(|s| !s.phase.is_live()),
        )
        .await;
        match waited {
            Err(_) => return Err(RuntimeError::Timeout(format!("session {id} did not exit"))),
            // The guard is dropped here, before any await below.
            Ok(r) => r
                .map(|_| ())
                .map_err(|_| RuntimeError::SessionNotFound(id.to_string()))?,
        }
        // Not while a restart opens this session's log.
        let _restart = self.inner.restarts.lock().await;
        let actor = {
            let mut map = lock(&self.inner.sessions);
            map.get_mut(id)
                .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?
                .actor
                .take()
        };
        // Aborting drops the actor, and with it the log's sender: the writer drains its queue
        // (the seal included) and ends.
        let Some(a) = actor else {
            return Ok(());
        };
        a.task.abort();
        // Cancelled or finished: either way it no longer runs.
        let _ = a.task.await;
        if let Some(w) = a.log_writer {
            let _ = w.await;
        }
        Ok(())
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
        self.history_page(id, before, limit, scope, false, None)
            .await
    }

    /// The last `limit` items of the conversation's *earlier* history (the part of the transcript
    /// before the session's own events: `wasit_core::AgentEvent::History` items) with
    /// `cursor < before`, oldest first, restricted to `scope` by what the items hold.
    pub async fn earlier_scoped(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
        scope: HistoryScope,
    ) -> Result<HistoryPage, RuntimeError> {
        self.history_page(id, before, limit, scope, true, None)
            .await
    }

    /// `session.history` as requested on the wire, including its byte budget.
    pub async fn history_request(&self, r: HistoryRequest) -> Result<HistoryPage, RuntimeError> {
        self.history_page(
            &r.id,
            r.before,
            r.limit as usize,
            r.scope,
            r.earlier,
            r.max_bytes,
        )
        .await
    }

    async fn history_page(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
        scope: HistoryScope,
        earlier: bool,
        max_bytes: Option<u32>,
    ) -> Result<HistoryPage, RuntimeError> {
        let file = if earlier {
            LogFile::Earlier
        } else {
            LogFile::Events
        };
        let limit = limit.clamp(1, MAX_HISTORY_PAGE);
        let (events, has_more) = self
            .log_page(id, file, before, limit, max_bytes, move |e| {
                scope.admits(&e).then_some(e)
            })
            .await?;
        Ok(HistoryPage { events, has_more })
    }

    /// The last `limit` entries of the session's trail with `cursor < before` (all when
    /// `None`), oldest first: the content-free projection of the events `history` returns
    /// ([`crate::trail`]), read from the event log.
    pub async fn trail_page(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
    ) -> Result<TrailPage, RuntimeError> {
        self.trail_bytes(id, before, limit, None).await
    }

    /// `session.trail.page` as requested on the wire, including its byte budget.
    pub async fn trail_request(&self, r: TrailRequest) -> Result<TrailPage, RuntimeError> {
        self.trail_bytes(&r.id, r.before, r.limit as usize, r.max_bytes)
            .await
    }

    async fn trail_bytes(
        &self,
        id: &SessionId,
        before: Option<u64>,
        limit: usize,
        max_bytes: Option<u32>,
    ) -> Result<TrailPage, RuntimeError> {
        let limit = limit.clamp(1, MAX_HISTORY_PAGE);
        let (entries, has_more) = self
            .log_page(id, LogFile::Events, before, limit, max_bytes, |e| {
                crate::trail::item(&e)
            })
            .await?;
        Ok(TrailPage { entries, has_more })
    }

    /// One page of a session's event log: the items `pick` makes of the records of `file` with
    /// `cursor < before`, read once the log has settled. Reads never go through the actor's
    /// mailbox and never replay the journal, and they never start an actor.
    async fn log_page<T: Send + 'static>(
        &self,
        id: &SessionId,
        file: LogFile,
        before: Option<u64>,
        limit: usize,
        max_bytes: Option<u32>,
        pick: impl FnMut(SessionEvent) -> Option<T> + Send + 'static,
    ) -> Result<(Vec<T>, bool), RuntimeError> {
        let view = self.log_view(id).await?;
        let budget = page_budget(max_bytes);
        let read = async {
            view.settled().await?;
            view.page(file, before, limit, budget, pick).await
        };
        match tokio::time::timeout(self.inner.cfg.timeouts.command, read).await {
            Err(_) => Err(RuntimeError::Timeout(format!(
                "session {id}'s event log is not ready"
            ))),
            Ok(r) => r.map_err(|e| io_err(&format!("read the event log of session {id}"), e)),
        }
    }

    /// The event log a read is served from. A session with a running actor reads the actor's
    /// log. Otherwise the log the session has (its last actor's, or one opened for reads) is
    /// used; a session with none gets one opened here, with no actor started. An unsealed log
    /// is completed by an offline build that reads wait for.
    pub(crate) async fn log_view(&self, id: &SessionId) -> Result<LogView, RuntimeError> {
        if let Some(v) = self.known_log(id)? {
            return Ok(v);
        }
        // One opener at a time: the restarts lock keeps two writers off one log.
        let _restart = self.inner.restarts.lock().await;
        let (dir, desc, meta, has_actor, snapshot) = {
            let map = lock(&self.inner.sessions);
            let e = map
                .get(id)
                .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
            if let Some(v) = &e.log {
                return Ok(v.clone());
            }
            (
                e.dir.clone(),
                e.desc.clone(),
                e.file.meta(),
                e.actor.as_ref().is_some_and(|a| !a.task.is_finished()),
                e.snapshot.clone(),
            )
        };
        if has_actor {
            return Err(RuntimeError::Io(format!("session {id} has no event log")));
        }
        let mut log = self.open_log(&dir).await.ok_or_else(|| {
            RuntimeError::Io(format!("cannot open the event log of session {id}"))
        })?;
        let view = log.view();
        let writer = log.take_writer();
        let build = OfflineBuild {
            id: id.clone(),
            run_dir: self.inner.cfg.session_run_dir(&dir),
            dir,
            desc,
            hints: self.inner.cfg.mode_hints.get(&meta.driver),
            meta,
            permits: self.inner.replays.clone(),
            snapshot,
            feed: self.inner.feed.clone(),
        };
        let build = tokio::spawn(build.run(log));
        let mut map = lock(&self.inner.sessions);
        match map.get_mut(id) {
            Some(e) => {
                e.log = Some(view.clone());
                e.log_task = Some(build);
                e.log_writer = writer;
                Ok(view)
            }
            None => {
                build.abort();
                Err(RuntimeError::SessionNotFound(id.to_string()))
            }
        }
    }

    /// The session's log view if it has one.
    fn known_log(&self, id: &SessionId) -> Result<Option<LogView>, RuntimeError> {
        let map = lock(&self.inner.sessions);
        let e = map
            .get(id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        Ok(e.log.clone())
    }

    /// Test support: whether the session has a running actor (reads must not start one).
    #[doc(hidden)]
    pub fn actor_running(&self, id: &SessionId) -> bool {
        lock(&self.inner.sessions)
            .get(id)
            .is_some_and(|e| e.actor.as_ref().is_some_and(|a| !a.task.is_finished()))
    }

    /// Subscribe to a session's events strictly after `from` (from the beginning when `None`),
    /// then live, across the session's ends and continues ([`crate::subscribe`]). Gap- and
    /// duplicate-free, in journal order; ends with `Err(Lagged)` if the subscriber falls behind
    /// a running actor, and when the session is deleted or the manager dropped. Never starts an
    /// actor.
    pub async fn subscribe(
        &self,
        id: &SessionId,
        from: Option<u64>,
    ) -> Result<SessionEventStream, RuntimeError> {
        let feed = Feed::start(WeakManager(Arc::downgrade(&self.inner)), id.clone(), from).await?;
        Ok(EventStream::new(
            futures::stream::unfold(feed, |mut feed| async move {
                feed.next().await.map(|item| (item, feed))
            })
            .boxed(),
        ))
    }

    /// Where a subscription attaches now, taken under one lock: the session's actor-start
    /// counter (marked seen) and its running actor's mailbox, if it has one.
    pub(crate) fn attach_point(
        &self,
        id: &SessionId,
    ) -> Result<(watch::Receiver<u64>, Option<mpsc::Sender<Msg>>), RuntimeError> {
        let map = lock(&self.inner.sessions);
        let e = map
            .get(id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        let actor = match &e.actor {
            Some(a) if !a.task.is_finished() => Some(a.tx.clone()),
            _ => None,
        };
        Ok((e.started.subscribe(), actor))
    }
}

impl Entry {
    /// Installs a started actor and counts the start, in one step under the map lock (or before
    /// the entry is in the map): a subscription that saw no actor is woken by the count.
    fn attach(&mut self, handle: ActorHandle) {
        self.actor = Some(handle);
        self.started.send_modify(|n| *n += 1);
    }
}
