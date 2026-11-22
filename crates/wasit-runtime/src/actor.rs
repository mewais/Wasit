//! `SessionActor`: one tokio task per session, the single owner of that session's shim
//! connection and protocol state ([`Core`]).
//!
//! Inputs: journal records from the shim (attached from the last applied seq; seq 1 on start, so
//! state is rebuilt by replay) and messages from a bounded mailbox. Outputs: [`SessionEvent`]s
//! (an in-memory tail ring + per-subscriber bounded queues; a subscriber that falls behind is
//! dropped and told `Lagged`), the reduced snapshot (a `watch`), and replies to requesters.
//!
//! Every shim request (notes, writes, signals) goes through an ordered outbox executed one at a
//! time *inside* the actor's `select!`, so the actor keeps draining journal records while a
//! request is in flight (an ack queued behind unread records can never deadlock it).
//!
//! A command is journaled as a note first and applied when that note comes back through the
//! journal stream, i.e. in journal order — exactly as a replay will apply it. Its result is
//! delivered to the requester then. Right after attaching, the actor journals an `attach` note
//! with a fresh token; when it reads that note back it has reached the live end of the journal
//! (the *boundary*) and writes whatever the replayed protocol still owes the CLI.
//!
//! Timers (handshake deadline, graceful-stop escalation, command deadlines, reconnect backoff)
//! are real timers in the same `select!`. Nothing here kills a session for inactivity.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use wasit_core::{Command, DriverDescriptor, ResumeMode, SpawnSpec};
use wasit_shim::{Error as ShimError, Record, RecordStream, ShimClient, ShimState, Sig};

use crate::config::RuntimeConfig;
use crate::core::Core;
use crate::note::RuntimeNote;
use crate::store::{self, SessionFile};
use crate::stream::Subscriber;
use crate::wire::{
    CommandError, CommandOutcome, HistoryPage, RuntimeError, SessionEvent, SessionId,
    SessionSnapshot,
};

/// Bound on commands awaiting their journal round trip, and on messages deferred while the
/// initial replay runs.
const MAX_PENDING: usize = 256;
/// Bound on queued shim requests.
const MAX_OUTBOX: usize = 8192;
/// Records drained per wake-up before yielding to other branches.
const DRAIN_BATCH: usize = 256;

pub(crate) type CommandReply = oneshot::Sender<Result<CommandOutcome, CommandError>>;
pub(crate) type UnitReply = oneshot::Sender<Result<(), RuntimeError>>;

/// Mailbox messages.
pub(crate) enum Msg {
    Command {
        command: Command,
        reply: CommandReply,
    },
    /// Raise or withdraw an external gate (see [`RuntimeNote::RaiseGate`]).
    Gate {
        op: GateOp,
        reply: CommandReply,
    },
    Stop {
        reply: UnitReply,
    },
    Continue {
        reply: UnitReply,
    },
    /// Set the user-given display name.
    Rename {
        name: Option<String>,
        reply: UnitReply,
    },
    Subscribe {
        from: Option<u64>,
        reply: oneshot::Sender<SubscribeReply>,
    },
    History {
        before: Option<u64>,
        limit: usize,
        reply: oneshot::Sender<HistoryLocal>,
    },
}

/// An external gate operation.
pub(crate) enum GateOp {
    Raise(wasit_core::GateRequest),
    Resolve { request_id: String },
}

/// A live subscription handed out by the actor.
pub(crate) struct SubscribeReply {
    /// Events with cursor in `(from, replay_below)` were evicted from memory and must be
    /// replayed from the journal first (`None` = nothing to replay).
    pub replay_below: Option<u64>,
    pub backlog: Vec<SessionEvent>,
    pub live: mpsc::Receiver<SessionEvent>,
    pub lagged: Arc<AtomicBool>,
}

/// History served from memory, or "replay the journal".
pub(crate) enum HistoryLocal {
    Page(HistoryPage),
    Replay,
}

/// Called after the snapshot changed.
pub(crate) type SnapshotNotify = Arc<dyn Fn(&Arc<SessionSnapshot>) + Send + Sync>;

/// What a session actor starts with.
pub(crate) struct ActorInit {
    pub dir: PathBuf,
    pub desc: Arc<dyn DriverDescriptor>,
    pub cfg: Arc<RuntimeConfig>,
    pub file: SessionFile,
    pub snapshot: watch::Sender<Arc<SessionSnapshot>>,
    pub notify: SnapshotNotify,
}

enum Op {
    /// A note; `token` names the pending requester (command / stop) if any.
    Note {
        data: serde_json::Value,
        token: Option<String>,
        attach: bool,
    },
    Write(String),
    Close,
    Signal(Sig),
}

enum Reply {
    Command(CommandReply),
    Stop(UnitReply),
}

struct Pending {
    reply: Reply,
    deadline: Instant,
}

enum Source {
    Live(Arc<ShimClient>, RecordStream),
    Offline(RecordStream, ShimState),
    /// Could not reach a live shim; retry with backoff.
    Retry(String),
    /// A new process generation could not be spawned.
    SpawnFailed(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StopStage {
    CloseStdin,
    Term,
    Kill,
}

pub(crate) struct Actor {
    id: SessionId,
    dir: PathBuf,
    desc: Arc<dyn DriverDescriptor>,
    cfg: Arc<RuntimeConfig>,
    core: Core,
    file: SessionFile,
    file_tx: mpsc::UnboundedSender<Persist>,
    /// The `session.json` writer; ends once `file_tx` is dropped and its last write is done.
    writer: Option<tokio::task::JoinHandle<()>>,
    snapshot_tx: watch::Sender<Arc<SessionSnapshot>>,
    notify: SnapshotNotify,
    snapshot_dirty: bool,

    ring: VecDeque<SessionEvent>,
    evicted_max: Option<u64>,
    subs: Vec<Subscriber<SessionEvent>>,

    source: Option<BoxFuture<'static, Source>>,
    stream: Option<RecordStream>,
    stream_is_live: bool,
    offline_state: Option<ShimState>,
    client: Option<Arc<ShimClient>>,
    inflight: Option<BoxFuture<'static, (Op, Result<u64, ShimError>)>>,
    outbox: VecDeque<Op>,

    live: bool,
    loading: bool,
    attach_token: String,
    pending: HashMap<String, Pending>,
    deferred: VecDeque<Msg>,
    pending_continue: Option<UnitReply>,
    pending_spec: SpawnSpec,

    handshake_deadline: Option<Instant>,
    stop_stage: Option<(Instant, StopStage)>,
    reconnect_at: Option<Instant>,
    reconnect_attempt: u32,
}

fn token() -> String {
    format!("{:016x}", fastrand::u64(..))
}

async fn opt_fut<T>(f: &mut Option<BoxFuture<'static, T>>) -> T {
    match f {
        Some(f) => f.await,
        None => std::future::pending().await,
    }
}

async fn next_record(s: &mut Option<RecordStream>) -> Option<Result<Record, ShimError>> {
    match s {
        Some(s) => s.next().await,
        None => std::future::pending().await,
    }
}

async fn sleep_until_opt(t: Option<Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// Connect to a live shim and attach from `from`, or read the journal of a finished one.
fn adopt_source(dir: PathBuf, run: PathBuf, from: u64) -> BoxFuture<'static, Source> {
    async move {
        match wasit_shim::probe_at(&dir, &run).await {
            Ok(ShimState::Running) => match ShimClient::connect(&run).await {
                Ok(c) => {
                    let c = Arc::new(c);
                    match c.attach(from).await {
                        Ok(s) => Source::Live(c, s),
                        Err(e) => Source::Retry(format!("attach: {e}")),
                    }
                }
                Err(e) => Source::Retry(format!("connect: {e}")),
            },
            Ok(state) => match wasit_shim::read_journal(&dir, from).await {
                Ok(s) => Source::Offline(s, state),
                Err(e) => Source::Retry(format!("read journal: {e}")),
            },
            Err(e) => Source::Retry(format!("probe: {e}")),
        }
    }
    .boxed()
}

fn reattach_source(client: Arc<ShimClient>, from: u64) -> BoxFuture<'static, Source> {
    async move {
        match client.attach(from).await {
            Ok(s) => Source::Live(client, s),
            Err(e) => Source::Retry(format!("re-attach: {e}")),
        }
    }
    .boxed()
}

fn spawn_source(spec: wasit_shim::ShimSpec, from: u64) -> BoxFuture<'static, Source> {
    async move {
        if let Err(e) = wasit_shim::spawn_detached(&spec).await {
            return Source::SpawnFailed(e.to_string());
        }
        match ShimClient::connect(spec.endpoint_dir()).await {
            Ok(c) => {
                let c = Arc::new(c);
                match c.attach(from).await {
                    Ok(s) => Source::Live(c, s),
                    Err(e) => Source::Retry(format!("attach: {e}")),
                }
            }
            Err(e) => Source::Retry(format!("connect: {e}")),
        }
    }
    .boxed()
}

/// Map a journal time (ms since the epoch) plus a delay to a monotonic deadline.
fn deadline_from_journal(t_ms: u64, after: Duration) -> Instant {
    let due = t_ms.saturating_add(after.as_millis() as u64);
    Instant::now() + Duration::from_millis(due.saturating_sub(store::now_ms()))
}

impl Actor {
    /// The `session.json` writer task, for whoever must wait until nothing writes into the
    /// session directory any more (delete): it ends after the actor is gone.
    pub(crate) fn take_writer(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        self.writer.take()
    }

    pub(crate) fn new(init: ActorInit) -> Self {
        let meta = init.file.meta();
        let mut core = Core::new(init.desc.clone(), &meta);
        core.hint_modes(init.cfg.mode_hints.get(&meta.driver));
        let (file_tx, file_rx) = mpsc::unbounded_channel();
        let writer = tokio::spawn(persist(init.dir.clone(), init.file.clone(), file_rx));
        let source = adopt_source(init.dir.clone(), init.cfg.session_run_dir(&init.dir), 1);
        Actor {
            id: meta.id,
            dir: init.dir,
            desc: init.desc,
            cfg: init.cfg,
            core,
            pending_spec: init.file.spec.clone(),
            file: init.file,
            file_tx,
            writer: Some(writer),
            snapshot_tx: init.snapshot,
            notify: init.notify,
            snapshot_dirty: true,
            ring: VecDeque::new(),
            evicted_max: None,
            subs: Vec::new(),
            source: Some(source),
            stream: None,
            stream_is_live: false,
            offline_state: None,
            client: None,
            inflight: None,
            outbox: VecDeque::new(),
            live: false,
            loading: true,
            attach_token: token(),
            pending: HashMap::new(),
            deferred: VecDeque::new(),
            pending_continue: None,
            handshake_deadline: None,
            stop_stage: None,
            reconnect_at: None,
            reconnect_attempt: 0,
        }
    }

    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<Msg>) {
        loop {
            self.pump_ops();
            self.publish_snapshot();
            let deadline = self.next_deadline();
            tokio::select! {
                r = opt_fut(&mut self.source) => {
                    self.source = None;
                    self.on_source(r);
                }
                item = next_record(&mut self.stream) => {
                    self.on_stream_item(item);
                    self.drain_ready();
                }
                r = opt_fut(&mut self.inflight) => {
                    self.inflight = None;
                    self.on_op_done(r.0, r.1);
                }
                msg = rx.recv() => match msg {
                    Some(m) => self.on_msg(m),
                    None => break,
                },
                _ = sleep_until_opt(deadline) => self.on_timer(),
            }
        }
    }

    // ── sources and records ─────────────────────────────────────────────────

    fn on_source(&mut self, s: Source) {
        match s {
            Source::Live(client, stream) => {
                self.client = Some(client);
                self.stream = Some(stream);
                self.stream_is_live = true;
                self.offline_state = None;
                self.live = false;
                self.attach_token = token();
                // Requests of a previous connection: writes are re-derived from the owed queue
                // at the boundary; only requester notes survive.
                self.outbox
                    .retain(|op| matches!(op, Op::Note { token: Some(_), .. }));
                let data = RuntimeNote::Attach {
                    token: self.attach_token.clone(),
                }
                .to_value();
                self.outbox.push_front(Op::Note {
                    data,
                    token: None,
                    attach: true,
                });
                if let Some(r) = self.pending_continue.take() {
                    // The reply only means the spawn worked.
                    let _ = r.send(Ok(())); // requester may have given up
                }
            }
            Source::Offline(stream, state) => {
                self.stream = Some(stream);
                self.stream_is_live = false;
                self.offline_state = Some(state);
            }
            Source::Retry(reason) => {
                self.reconnect_attempt += 1;
                if self.reconnect_attempt > self.cfg.timeouts.reconnect_attempts {
                    tracing::warn!(session = %self.id, %reason, "giving up on the shim");
                    self.lose(&format!("cannot reach the shim: {reason}"));
                    self.finish_loading();
                } else {
                    tracing::debug!(session = %self.id, %reason, attempt = self.reconnect_attempt, "shim unreachable; retrying");
                    let backoff = Duration::from_millis(50u64 << self.reconnect_attempt.min(6));
                    self.reconnect_at = Some(Instant::now() + backoff);
                }
            }
            Source::SpawnFailed(reason) => {
                if let Some(r) = self.pending_continue.take() {
                    let _ = r.send(Err(RuntimeError::Spawn(reason))); // requester may have given up
                }
            }
        }
    }

    fn drain_ready(&mut self) {
        for _ in 0..DRAIN_BATCH {
            let Some(stream) = self.stream.as_mut() else {
                return;
            };
            match stream.next().now_or_never() {
                Some(item) => self.on_stream_item(item),
                None => return,
            }
        }
    }

    fn on_stream_item(&mut self, item: Option<Result<Record, ShimError>>) {
        match item {
            Some(Ok(rec)) => self.on_record(rec),
            Some(Err(ShimError::Lagged)) if self.stream_is_live => {
                self.stream = None;
                if let Some(c) = self.client.clone() {
                    self.source = Some(reattach_source(c, self.core.last_seq() + 1));
                }
            }
            Some(Err(ShimError::Replaced)) => {
                self.stream = None;
                self.drop_connection();
                self.lose("another controller attached to this session's shim");
                self.finish_loading();
            }
            Some(Err(e)) => {
                tracing::warn!(session = %self.id, error = %e, "journal stream failed");
                self.stream_ended();
            }
            None => self.stream_ended(),
        }
    }

    fn stream_ended(&mut self) {
        self.stream = None;
        if self.stream_is_live {
            self.drop_connection();
            if self.core.process_ended() {
                self.finish_loading();
                return;
            }
            // The shim went away without us seeing `exited`: find out what happened.
            self.source = Some(adopt_source(
                self.dir.clone(),
                self.cfg.session_run_dir(&self.dir),
                self.core.last_seq() + 1,
            ));
        } else {
            let state = self.offline_state.take();
            if !self.core.process_ended() {
                let reason = match state {
                    Some(ShimState::Exited(_)) => {
                        "the journal ends without the exit record the shim reported"
                    }
                    _ => "the shim is gone and the journal has no exit record",
                };
                self.lose(reason);
            }
            self.finish_loading();
        }
    }

    fn drop_connection(&mut self) {
        self.client = None;
        self.inflight = None;
        self.live = false;
        self.handshake_deadline = None;
        self.stop_stage = None;
    }

    fn lose(&mut self, reason: &str) {
        let events = self.core.mark_lost(reason);
        if !events.is_empty() {
            self.snapshot_dirty = true;
            self.publish(events);
        }
        self.fail_pending(reason);
        self.handshake_deadline = None;
        self.stop_stage = None;
    }

    fn fail_pending(&mut self, reason: &str) {
        self.outbox.clear();
        for (_, p) in self.pending.drain() {
            reply_not_running(p.reply, reason);
        }
    }

    fn finish_loading(&mut self) {
        if !self.loading {
            return;
        }
        self.loading = false;
        let deferred: Vec<Msg> = self.deferred.drain(..).collect();
        for m in deferred {
            self.on_msg(m);
        }
    }

    fn on_record(&mut self, rec: Record) {
        let mut out = self.core.apply(&rec);
        self.snapshot_dirty = true;
        for (id, bytes) in std::mem::take(&mut out.blobs) {
            crate::media::write_blob(&self.dir, id, bytes);
        }
        let resolved = out.resolved;
        if !out.events.is_empty() {
            self.publish(out.events);
        }
        // Requesters answered below see the state their request produced.
        self.publish_snapshot();
        for (tok, status) in resolved {
            if let Some(p) = self.pending.remove(&tok) {
                match p.reply {
                    Reply::Command(r) => {
                        let _ = r.send(status.into_result()); // requester may have given up
                    }
                    Reply::Stop(r) => {
                        let _ = r.send(Ok(())); // requester may have given up
                    }
                }
            }
        }
        if self.live {
            for w in out.writes {
                self.push_op(Op::Write(w));
            }
            if out.close_stdin {
                self.push_op(Op::Close);
            }
            if out.started {
                self.arm_handshake();
            }
            if out.stop_applied {
                self.arm_stop();
            }
        }
        if out.attach.as_deref() == Some(self.attach_token.as_str()) {
            self.on_boundary();
        }
        if out.exited {
            self.handshake_deadline = None;
            self.stop_stage = None;
            self.outbox
                .retain(|op| matches!(op, Op::Note { token: Some(_), .. }));
            self.finish_loading();
        }
        if !self.core.handshake_pending() {
            self.handshake_deadline = None;
        }
        let native = self.core.snapshot().native_session_id.clone();
        if native.is_some() && native != self.file.native_session_id {
            self.file.native_session_id = native;
            self.save_file(None);
        }
    }

    /// We read our own `attach` note: everything before it was history.
    fn on_boundary(&mut self) {
        self.live = true;
        self.reconnect_attempt = 0;
        let owed: Vec<String> = self.core.owed().cloned().collect();
        for w in owed {
            self.push_op(Op::Write(w));
        }
        if self.core.close_owed() {
            self.push_op(Op::Close);
        }
        if self.core.awaiting_start() {
            let data = RuntimeNote::Start {
                spec: Box::new(self.pending_spec.clone()),
            }
            .to_value();
            self.push_op(Op::Note {
                data,
                token: None,
                attach: false,
            });
        }
        if self.core.handshake_pending() {
            self.arm_handshake();
        }
        if self.core.stop_requested() && self.core.process_running() {
            self.arm_stop();
        }
        self.finish_loading();
    }

    fn arm_handshake(&mut self) {
        if let Some(t) = self.core.start_t() {
            if self.core.handshake_pending() {
                self.handshake_deadline =
                    Some(deadline_from_journal(t, self.cfg.timeouts.handshake));
            }
        }
    }

    fn arm_stop(&mut self) {
        if self.stop_stage.is_none() {
            self.stop_stage = Some((
                Instant::now() + self.cfg.timeouts.stop_grace,
                StopStage::CloseStdin,
            ));
        }
    }

    fn publish(&mut self, events: Vec<SessionEvent>) {
        // The snapshot first: a subscriber that reads the snapshot on an event must see a
        // state at least as new as that event.
        self.publish_snapshot();
        for e in &events {
            self.subs.retain(|s| s.offer(e.clone()));
        }
        for e in events {
            self.ring.push_back(e);
        }
        while self.ring.len() > self.cfg.event_tail.max(1) {
            if let Some(old) = self.ring.pop_front() {
                self.evicted_max = Some(old.cursor);
            }
        }
    }

    fn publish_snapshot(&mut self) {
        if !self.snapshot_dirty {
            return;
        }
        self.snapshot_dirty = false;
        let snap = Arc::new(self.core.snapshot().clone());
        self.snapshot_tx.send_replace(snap.clone());
        (self.notify)(&snap);
    }

    // ── shim requests ───────────────────────────────────────────────────────

    fn push_op(&mut self, op: Op) {
        if self.outbox.len() >= MAX_OUTBOX {
            tracing::error!(session = %self.id, "shim request queue overflow; dropping the oldest request");
            if let Some(Op::Note { token: Some(t), .. }) = self.outbox.pop_front() {
                if let Some(p) = self.pending.remove(&t) {
                    reply_busy(p.reply);
                }
            }
        }
        self.outbox.push_back(op);
    }

    fn pump_ops(&mut self) {
        if self.inflight.is_some() {
            return;
        }
        let Some(client) = self.client.clone() else {
            return;
        };
        let Some(op) = self.outbox.pop_front() else {
            return;
        };
        self.inflight = Some(
            async move {
                let r = match &op {
                    Op::Note { data, .. } => client.note(data.clone()).await,
                    Op::Write(line) => client.write(line).await,
                    Op::Close => client.close_stdin().await,
                    Op::Signal(sig) => client.signal(*sig).await,
                };
                (op, r)
            }
            .boxed(),
        );
    }

    fn on_op_done(&mut self, op: Op, r: Result<u64, ShimError>) {
        let Err(e) = r else {
            return;
        };
        match op {
            Op::Note { token: Some(t), .. } => {
                if let Some(p) = self.pending.remove(&t) {
                    reply_not_running(p.reply, &e.to_string());
                }
            }
            Op::Note { attach: true, .. } => {
                tracing::debug!(session = %self.id, error = %e, "attach note refused (process ended)");
            }
            Op::Note { .. } => {
                tracing::debug!(session = %self.id, error = %e, "note refused");
            }
            Op::Write(_) => {
                tracing::warn!(session = %self.id, error = %e, "writing to the CLI failed");
            }
            Op::Close => {
                tracing::debug!(session = %self.id, error = %e, "closing stdin failed (already closed?)");
            }
            Op::Signal(sig) => {
                tracing::warn!(session = %self.id, error = %e, sig = sig.as_str(), "signalling the CLI failed");
            }
        }
    }

    // ── timers ──────────────────────────────────────────────────────────────

    fn next_deadline(&self) -> Option<Instant> {
        [
            self.handshake_deadline,
            self.stop_stage.map(|(t, _)| t),
            self.reconnect_at,
            self.pending.values().map(|p| p.deadline).min(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn on_timer(&mut self) {
        let now = Instant::now();
        if self.handshake_deadline.is_some_and(|d| d <= now) {
            self.handshake_deadline = None;
            if self.core.handshake_pending() && self.live {
                let after_ms = self.cfg.timeouts.handshake.as_millis() as u64;
                self.push_op(Op::Note {
                    data: RuntimeNote::HandshakeTimeout { after_ms }.to_value(),
                    token: None,
                    attach: false,
                });
                self.push_op(Op::Signal(Sig::Term));
                self.stop_stage = Some((now + self.cfg.timeouts.stop_grace, StopStage::Kill));
            }
        }
        if let Some((t, stage)) = self.stop_stage {
            if t <= now {
                let grace = self.cfg.timeouts.stop_grace;
                self.stop_stage = None;
                if self.core.process_running() && self.client.is_some() {
                    match stage {
                        StopStage::CloseStdin => {
                            self.push_op(Op::Close);
                            self.stop_stage = Some((now + grace, StopStage::Term));
                        }
                        StopStage::Term => {
                            self.push_op(Op::Signal(Sig::Term));
                            self.stop_stage = Some((now + grace, StopStage::Kill));
                        }
                        StopStage::Kill => self.push_op(Op::Signal(Sig::Kill)),
                    }
                }
            }
        }
        if self.reconnect_at.is_some_and(|t| t <= now) {
            self.reconnect_at = None;
            self.source = Some(adopt_source(
                self.dir.clone(),
                self.cfg.session_run_dir(&self.dir),
                self.core.last_seq() + 1,
            ));
        }
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            if let Some(p) = self.pending.remove(&k) {
                match p.reply {
                    Reply::Command(r) => {
                        let _ = r.send(Err(CommandError::Timeout)); // requester may have given up
                    }
                    Reply::Stop(r) => {
                        let _ = r.send(Err(RuntimeError::Timeout(
                            "the stop request was not journaled in time".into(),
                        ))); // requester may have given up
                    }
                }
            }
        }
    }

    // ── mailbox ─────────────────────────────────────────────────────────────

    fn on_msg(&mut self, msg: Msg) {
        match msg {
            Msg::Subscribe { from, reply } => {
                let r = self.subscribe(from);
                let _ = reply.send(r); // requester may have given up
            }
            Msg::History {
                before,
                limit,
                reply,
            } => {
                let r = if self.loading {
                    HistoryLocal::Replay
                } else {
                    self.history(before, limit)
                };
                let _ = reply.send(r); // requester may have given up
            }
            m if self.loading => {
                if self.deferred.len() >= MAX_PENDING {
                    reply_msg_busy(m);
                } else {
                    self.deferred.push_back(m);
                }
            }
            Msg::Command { command, reply } => self.command(command, reply),
            Msg::Gate { op, reply } => self.gate(op, reply),
            Msg::Stop { reply } => {
                if !self.core.process_running() {
                    let _ = reply.send(Ok(())); // requester may have given up
                    return;
                }
                if self.pending.len() >= MAX_PENDING {
                    let _ = reply.send(Err(RuntimeError::Busy("too many pending requests".into())));
                    return;
                }
                let tok = token();
                self.push_op(Op::Note {
                    data: RuntimeNote::Stop { token: tok.clone() }.to_value(),
                    token: Some(tok.clone()),
                    attach: false,
                });
                self.pending.insert(
                    tok,
                    Pending {
                        reply: Reply::Stop(reply),
                        deadline: Instant::now() + self.cfg.timeouts.command,
                    },
                );
            }
            Msg::Continue { reply } => self.continue_session(reply),
            Msg::Rename { name, reply } => {
                self.file.display_name = name.clone();
                // The reply is sent by the writer once `session.json` holds the new name:
                // a rename that returned survives a restart.
                self.save_file(Some(reply));
                self.core.set_display_name(name);
                self.snapshot_dirty = true;
                self.publish_snapshot();
            }
        }
    }

    /// Queue a write of the actor's copy of `session.json`; `done` is answered once it is on disk.
    fn save_file(&mut self, done: Option<oneshot::Sender<Result<(), RuntimeError>>>) {
        let sent = self.file_tx.send(Persist {
            file: self.file.clone(),
            done,
        });
        if let Err(mpsc::error::SendError(Persist {
            done: Some(done), ..
        })) = sent
        {
            let _ = done.send(Err(RuntimeError::Io("session.json writer stopped".into())));
            // requester may have given up
        }
    }

    fn command(&mut self, command: Command, reply: CommandReply) {
        let caps = self.desc.capabilities();
        let unsupported = match &command {
            Command::Interrupt => !caps.interrupt,
            Command::SetModel { .. } => !caps.set_model,
            Command::SetMode { .. } => !caps.set_mode,
            Command::RunSlash { .. } => !caps.slash_commands,
            _ => false,
        };
        if unsupported {
            let _ = reply.send(Err(CommandError::Unsupported {
                operation: command.name().to_string(),
            })); // requester may have given up
            return;
        }
        if matches!(command, Command::Shutdown) {
            let _ = reply.send(Err(CommandError::Rejected {
                reason: "use sessions.stop to end a session".into(),
            })); // requester may have given up
            return;
        }
        if let Some(request_id) = self.core.answers_withdrawn(&command) {
            // Also after the process ended: say why the answer did nothing.
            let _ = reply.send(Err(CommandError::GateWithdrawn { request_id })); // requester may have given up
            return;
        }
        if !self.core.process_running() {
            let _ = reply.send(Err(CommandError::NotRunning {
                reason: format!("the session is {}", self.core.snapshot().phase.name()),
            })); // requester may have given up
            return;
        }
        if self.pending.len() >= MAX_PENDING {
            let _ = reply.send(Err(CommandError::Busy)); // requester may have given up
            return;
        }
        let tok = token();
        self.push_op(Op::Note {
            data: RuntimeNote::Command {
                token: tok.clone(),
                command,
            }
            .to_value(),
            token: Some(tok.clone()),
            attach: false,
        });
        self.pending.insert(
            tok,
            Pending {
                reply: Reply::Command(reply),
                deadline: Instant::now() + self.cfg.timeouts.command,
            },
        );
    }

    fn gate(&mut self, op: GateOp, reply: CommandReply) {
        if !self.core.process_running() {
            let _ = reply.send(Err(CommandError::NotRunning {
                reason: format!("the session is {}", self.core.snapshot().phase.name()),
            })); // requester may have given up
            return;
        }
        if self.pending.len() >= MAX_PENDING {
            let _ = reply.send(Err(CommandError::Busy)); // requester may have given up
            return;
        }
        let tok = token();
        let data = match op {
            GateOp::Raise(gate) => RuntimeNote::RaiseGate {
                token: tok.clone(),
                gate: Box::new(gate),
            },
            GateOp::Resolve { request_id } => RuntimeNote::ResolveGate {
                token: tok.clone(),
                request_id,
            },
        }
        .to_value();
        self.push_op(Op::Note {
            data,
            token: Some(tok.clone()),
            attach: false,
        });
        self.pending.insert(
            tok,
            Pending {
                reply: Reply::Command(reply),
                deadline: Instant::now() + self.cfg.timeouts.command,
            },
        );
    }

    fn continue_session(&mut self, reply: UnitReply) {
        if self.core.process_running() {
            let _ = reply.send(Err(RuntimeError::InvalidState(
                "the session is still running".into(),
            ))); // requester may have given up
            return;
        }
        if self.stream_is_live && self.stream.is_some() {
            // The previous generation's shim is only lingering to deliver records we already
            // have (its process ended); let it go.
            self.stream = None;
            self.drop_connection();
        }
        if self.pending_continue.is_some() || self.source.is_some() || self.stream.is_some() {
            let _ = reply.send(Err(RuntimeError::InvalidState(
                "the session is already being continued".into(),
            ))); // requester may have given up
            return;
        }
        if !self.desc.capabilities().resume {
            let _ = reply.send(Err(RuntimeError::Unsupported(format!(
                "driver {} cannot resume sessions",
                self.desc.id()
            )))); // requester may have given up
            return;
        }
        let native = self
            .core
            .snapshot()
            .native_session_id
            .clone()
            .or_else(|| self.file.native_session_id.clone());
        let Some(native) = native else {
            let _ = reply.send(Err(RuntimeError::InvalidState(
                "the CLI never reported a native session id; nothing to resume".into(),
            ))); // requester may have given up
            return;
        };
        let mut spec = self.file.spec.clone();
        spec.resume = ResumeMode::Resume(native);
        spec.first_prompt = None;
        let shim_spec = match store::shim_spec(&self.cfg, self.desc.as_ref(), &self.dir, &spec) {
            Ok(s) => s,
            Err(e) => {
                let _ = reply.send(Err(e)); // requester may have given up
                return;
            }
        };
        self.pending_spec = spec.clone();
        self.file.spec = spec;
        self.save_file(None);
        self.reconnect_attempt = 0;
        self.pending_continue = Some(reply);
        self.source = Some(spawn_source(shim_spec, self.core.last_seq() + 1));
    }

    fn subscribe(&mut self, from: Option<u64>) -> SubscribeReply {
        let (sub, rx) = Subscriber::new(self.cfg.subscriber_queue);
        let lagged = sub.lagged.clone();
        let after = from.unwrap_or(0);
        let replay_below = match (self.evicted_max, from) {
            (None, _) => None,
            (Some(ev), Some(c)) if c >= ev => None,
            (Some(_), _) => Some(self.ring.front().map(|e| e.cursor).unwrap_or(u64::MAX)),
        };
        let backlog: Vec<SessionEvent> = self
            .ring
            .iter()
            .filter(|e| from.is_none() || e.cursor > after)
            .cloned()
            .collect();
        self.subs.push(sub);
        SubscribeReply {
            replay_below,
            backlog,
            live: rx,
            lagged,
        }
    }

    fn history(&self, before: Option<u64>, limit: usize) -> HistoryLocal {
        let below: Vec<&SessionEvent> = self
            .ring
            .iter()
            .filter(|e| before.is_none_or(|b| e.cursor < b))
            .collect();
        let complete = self.evicted_max.is_none();
        if below.len() > limit || (below.len() == limit && limit > 0) {
            let start = below.len() - limit;
            let has_more = start > 0 || !complete;
            HistoryLocal::Page(HistoryPage {
                events: below[start..].iter().map(|e| (*e).clone()).collect(),
                has_more,
            })
        } else if complete {
            HistoryLocal::Page(HistoryPage {
                events: below.into_iter().cloned().collect(),
                has_more: false,
            })
        } else {
            HistoryLocal::Replay
        }
    }
}

fn reply_not_running(reply: Reply, reason: &str) {
    match reply {
        Reply::Command(r) => {
            let _ = r.send(Err(CommandError::NotRunning {
                reason: reason.to_string(),
            })); // requester may have given up
        }
        Reply::Stop(r) => {
            // The process is gone, which is what a stop wants.
            let _ = r.send(Ok(())); // requester may have given up
        }
    }
}

fn reply_busy(reply: Reply) {
    match reply {
        Reply::Command(r) => {
            let _ = r.send(Err(CommandError::Busy)); // requester may have given up
        }
        Reply::Stop(r) => {
            let _ = r.send(Err(RuntimeError::Busy("request queue overflow".into())));
            // requester may have given up
        }
    }
}

fn reply_msg_busy(m: Msg) {
    match m {
        Msg::Command { reply, .. } | Msg::Gate { reply, .. } => {
            let _ = reply.send(Err(CommandError::Busy)); // requester may have given up
        }
        Msg::Stop { reply } | Msg::Continue { reply } | Msg::Rename { reply, .. } => {
            let _ = reply.send(Err(RuntimeError::Busy("session is loading".into())));
            // requester may have given up
        }
        Msg::Subscribe { .. } | Msg::History { .. } => {}
    }
}

/// A request to write `session.json`; `done` is answered after the write.
pub(crate) struct Persist {
    file: SessionFile,
    done: Option<oneshot::Sender<Result<(), RuntimeError>>>,
}

/// Write `session.json` whenever the actor's copy changes. Queued requests are coalesced to the
/// newest copy; every waiter is answered only once that copy is on disk (durability first).
async fn persist(dir: PathBuf, initial: SessionFile, mut rx: mpsc::UnboundedReceiver<Persist>) {
    let mut last = initial;
    while let Some(first) = rx.recv().await {
        let mut current = first.file;
        let mut waiters: Vec<_> = first.done.into_iter().collect();
        while let Ok(next) = rx.try_recv() {
            current = next.file;
            waiters.extend(next.done);
        }
        let result = if current != last {
            match store::write(&dir, &current).await {
                Ok(()) => {
                    last = current;
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(dir = %dir.display(), error = %e, "cannot write session.json");
                    Err(RuntimeError::Io(format!("write session.json: {e}")))
                }
            }
        } else {
            Ok(())
        };
        for w in waiters {
            let _ = w.send(result.clone()); // requester may have given up
        }
    }
}
