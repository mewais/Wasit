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
use wasit_control::TraceId;
use wasit_core::{Command, DriverDescriptor, ResumeMode, SpawnSpec};
use wasit_shim::{Error as ShimError, Record, RecordStream, ShimClient, ShimState, Sig};

use crate::config::RuntimeConfig;
use crate::continuation::{Continuation, Held};
use crate::core::{ContinuePlan, Core};
use crate::diag::{self, Intent};
use crate::note::RuntimeNote;
use crate::store::{self, SessionFile};
use crate::stream::Subscriber;
use crate::wire::{
    CommandError, CommandOutcome, HistoryPage, NotContinuable, Phase, RuntimeError, SessionEvent,
    SessionId, SessionSnapshot, TrailPage,
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
        /// The trace id of the request that carried it, journaled with the command.
        trace: Option<TraceId>,
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
        reply: oneshot::Sender<Local<HistoryPage>>,
    },
    /// The same window of the session's trail.
    Trail {
        before: Option<u64>,
        limit: usize,
        reply: oneshot::Sender<Local<TrailPage>>,
    },
}

/// An external gate operation.
pub(crate) enum GateOp {
    Raise(wasit_core::GateRequest),
    Resolve { request_id: String },
}

impl GateOp {
    /// What the operation is called in the records about it.
    fn name(&self) -> &'static str {
        match self {
            GateOp::Raise(_) => "raise_gate",
            GateOp::Resolve { .. } => "resolve_gate",
        }
    }
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

/// A page served from memory, or "replay the journal".
pub(crate) enum Local<P> {
    Page(P),
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
    /// The trace id of the request that creates the session: journaled with its `start` note
    /// (`None` for a session adopted or restarted).
    pub start_trace: Option<TraceId>,
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
    /// The command was held behind a continue: when the process it was to run in is gone, that
    /// is the continue's failure, and the sender is told so ([`CommandError::ContinueFailed`]).
    continued: bool,
    /// The shim refused the note because the process had already exited (the error it said):
    /// the answer is how the process ended, which the journal tells.
    note_failed: Option<String>,
    deadline: Instant,
    /// What the request is, for the records about it (commands and gate operations; a stop has
    /// none).
    intent: Option<Intent>,
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
    /// A continue under way: who waits for it, what is held behind it.
    continuation: Continuation,
    pending_spec: SpawnSpec,
    /// The trace id journaled with the `start` note of the first generation.
    start_trace: Option<TraceId>,

    handshake_deadline: Option<Instant>,
    stop_stage: Option<(Instant, StopStage)>,
    reconnect_at: Option<Instant>,
    reconnect_attempt: u32,
}

/// Why a continue could not start.
struct ContinueRefused {
    error: RuntimeError,
    /// The rule that forbids it, when that is why.
    blocked: Option<NotContinuable>,
}

impl ContinueRefused {
    fn state(blocked: NotContinuable, why: &str) -> Self {
        ContinueRefused {
            error: RuntimeError::InvalidState(why.to_string()),
            blocked: Some(blocked),
        }
    }
}

/// Which commands continue an ended session before they are applied: the product rule is that
/// a prompt (and a slash command) sent to an ended session carries on the conversation; every
/// other command to a session without a process keeps answering "not running".
fn continues_the_session(command: &Command) -> bool {
    matches!(command, Command::Prompt { .. } | Command::RunSlash { .. })
}

/// How an ended phase reads in an error.
fn describe_end(phase: &Phase) -> String {
    match phase {
        Phase::Failed {
            reason,
            stderr_tail,
        } => match stderr_tail.last() {
            Some(line) => format!("{reason}: {line}"),
            None => reason.clone(),
        },
        Phase::Exited { code: Some(c) } => format!("it exited with code {c}"),
        Phase::Exited { code: None } => "it exited".to_string(),
        Phase::Lost { reason } => reason.clone(),
        other => format!("it is {}", other.name()),
    }
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
            continuation: Continuation::Idle,
            start_trace: init.start_trace,
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
                // A continue that was starting has its shim now: everybody who waited for it
                // is told, and the commands held behind it are journaled right after the
                // attach note, in the order they arrived (the generation's `start` note follows
                // at the boundary; the core applies them after it, as a replay does).
                let waiting = self.continuation.attached();
                for h in waiting.held {
                    self.journal_command(h.command, h.reply, h.intent, true);
                }
                for r in waiting.stops {
                    self.request_stop(r);
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
                    diag::lost(&self.id, &reason);
                    // (Also ends a continue that was starting: see `lose`.)
                    self.lose(&format!("cannot reach the shim: {reason}"));
                    self.finish_loading();
                } else {
                    diag::retry(&self.id, self.reconnect_attempt, &reason);
                    let backoff = Duration::from_millis(50u64 << self.reconnect_attempt.min(6));
                    self.reconnect_at = Some(Instant::now() + backoff);
                }
            }
            Source::SpawnFailed(reason) => self.fail_continue(RuntimeError::Spawn(reason)),
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
                diag::stream_failed(&self.id, &e);
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
            if self.continuation.is_starting() {
                // The new shim had already finished when we looked for it: we never attached,
                // so nothing could be journaled for anybody who waited; its journal (read just
                // now) shows how it ended.
                let how = describe_end(&self.core.snapshot().phase);
                self.fail_continue(RuntimeError::Spawn(format!(
                    "the new process had already ended before the runtime could attach to it ({how})"
                )));
            }
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
        if let Some(waiting) = self.continuation.connection_lost() {
            // What was journaled into the new shim's window before the core saw its `spawned`
            // record will not be applied: say so instead of waiting for the deadline.
            let why = "the new process's shim went away before the process started";
            for r in waiting {
                let _ = r.send(Err(RuntimeError::Spawn(why.into()))); // requester may have given up
            }
            self.fail_pending(why);
        }
    }

    fn lose(&mut self, reason: &str) {
        // A continue that cannot reach its shim ends here, with everybody who waited for it.
        self.fail_continue(RuntimeError::Spawn(format!(
            "could not reach the new process: {reason}"
        )));
        let events = self.core.mark_lost(reason);
        if !events.is_empty() {
            self.snapshot_dirty = true;
            self.publish(events);
        }
        self.fail_pending(reason);
        self.handshake_deadline = None;
        self.stop_stage = None;
    }

    /// A continue that was starting ended without a new process the runtime can talk to: the
    /// requests that asked for it get `error`; the commands held behind it were never journaled
    /// and get [`CommandError::ContinueFailed`] saying why.
    fn fail_continue(&mut self, error: RuntimeError) {
        let waiting = self.continuation.failed();
        for r in waiting.continues {
            let _ = r.send(Err(error.clone())); // requester may have given up
        }
        for h in waiting.held {
            self.refuse(
                &h.intent,
                h.reply,
                CommandError::ContinueFailed {
                    reason: error.to_string(),
                    blocked: None,
                },
            );
        }
        for r in waiting.stops {
            // There is no process: what a stop wants.
            let _ = r.send(Ok(())); // requester may have given up
        }
    }

    fn fail_pending(&mut self, reason: &str) {
        self.outbox.clear();
        let pending: Vec<Pending> = self.pending.drain().map(|(_, p)| p).collect();
        for p in pending {
            self.answer_not_running(p, reason);
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
        let continued = if out.spawned {
            // The core shows the new generation now.
            self.continuation.spawned()
        } else {
            Vec::new()
        };
        if !out.events.is_empty() {
            self.publish(out.events);
        }
        // Requesters answered below see the state their request produced.
        self.publish_snapshot();
        for r in continued {
            // The new process is up and attached, and the snapshot shows it.
            let _ = r.send(Ok(())); // requester may have given up
        }
        for (tok, status) in resolved {
            if let Some(p) = self.pending.remove(&tok) {
                match p.reply {
                    Reply::Command(r) => {
                        let result = match status.into_result() {
                            // The process the command waited for ended before it started.
                            Err(CommandError::NotRunning { reason }) if p.continued => {
                                Err(CommandError::ContinueFailed {
                                    reason,
                                    blocked: None,
                                })
                            }
                            other => other,
                        };
                        let _ = r.send(result); // requester may have given up
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
            self.answer_refused_notes();
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
                trace: self.start_trace.clone(),
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
            diag::outbox_overflow(&self.id);
            if let Some(Op::Note { token: Some(t), .. }) = self.outbox.pop_front() {
                if let Some(p) = self.pending.remove(&t) {
                    self.answer_busy(p);
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
                if let Some(mut p) = self.pending.remove(&t) {
                    if p.continued {
                        // Handed to the shim of a continue, which refused: its process is gone.
                        // How it ended is in the journal; the answer waits for that record
                        // (unless the core has it already).
                        if !self.continuation.is_attached() && self.core.process_ended() {
                            self.answer_ended_before_prompt(p);
                        } else {
                            p.note_failed = Some(e.to_string());
                            self.pending.insert(t, p);
                        }
                    } else {
                        self.answer_not_running(p, &e.to_string());
                    }
                }
            }
            Op::Note { attach: true, .. } => {
                diag::op_refused(&self.id, "attach", &e);
            }
            Op::Note { .. } => {
                diag::op_refused(&self.id, "note", &e);
            }
            Op::Write(_) => {
                diag::op_failed(&self.id, "write", None, &e);
            }
            Op::Close => {
                diag::op_refused(&self.id, "close", &e);
            }
            Op::Signal(sig) => {
                diag::op_failed(&self.id, "signal", Some(sig.as_str()), &e);
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
                // The note may still be journaled and applied later: the trail shows it.
                if let Some(i) = &p.intent {
                    diag::command_timeout(&self.id, i);
                }
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
                    Local::Replay
                } else {
                    self.history(before, limit)
                };
                let _ = reply.send(r); // requester may have given up
            }
            Msg::Trail {
                before,
                limit,
                reply,
            } => {
                let r = if self.loading {
                    Local::Replay
                } else {
                    self.trail(before, limit)
                };
                let _ = reply.send(r); // requester may have given up
            }
            m if self.loading => {
                if self.deferred.len() >= MAX_PENDING {
                    self.answer_msg_busy(m);
                } else {
                    self.deferred.push_back(m);
                }
            }
            Msg::Command {
                command,
                trace,
                reply,
            } => self.command(command, trace, reply),
            Msg::Gate { op, reply } => self.gate(op, reply),
            Msg::Stop { reply } => {
                if let Some(w) = self.continuation.waiting() {
                    // The process this stops is not there yet: the stop is journaled as soon as
                    // its shim is attached (or answered, if it never is).
                    w.stops.push(reply);
                } else if !self.takes_commands() {
                    let _ = reply.send(Ok(())); // requester may have given up
                } else {
                    self.request_stop(reply);
                }
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

    /// Journal a stop of the running process; the reply comes when the core applies it.
    fn request_stop(&mut self, reply: UnitReply) {
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
                continued: false,
                note_failed: None,
                deadline: Instant::now() + self.cfg.timeouts.command,
                intent: None,
            },
        );
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

    fn command(&mut self, command: Command, trace: Option<TraceId>, reply: CommandReply) {
        let intent = Intent::new(command.name(), trace);
        let caps = self.desc.capabilities();
        let unsupported = match &command {
            Command::Interrupt => !caps.interrupt,
            Command::SetModel { .. } => !caps.set_model,
            Command::SetMode { .. } => !caps.set_mode,
            Command::RunSlash { .. } => !caps.slash_commands,
            _ => false,
        };
        if unsupported {
            self.refuse(
                &intent,
                reply,
                CommandError::Unsupported {
                    operation: command.name().to_string(),
                },
            );
            return;
        }
        if matches!(command, Command::Shutdown) {
            self.refuse(
                &intent,
                reply,
                CommandError::Rejected {
                    reason: "use sessions.stop to end a session".into(),
                },
            );
            return;
        }
        if let Some(request_id) = self.core.answers_withdrawn(&command) {
            // Also after the process ended: say why the answer did nothing.
            self.refuse(&intent, reply, CommandError::GateWithdrawn { request_id });
            return;
        }
        if !self.takes_commands() {
            if continues_the_session(&command) {
                self.continue_for(command, reply, intent);
            } else {
                let reason = format!("the session is {}", self.core.snapshot().phase.name());
                self.refuse(&intent, reply, CommandError::NotRunning { reason });
            }
            return;
        }
        // A prompt journaled into the window of a continue is part of that continue: if its
        // process is gone, that is the continue's failure.
        let continued = self.continuation.is_attached() && continues_the_session(&command);
        self.journal_command(command, reply, intent, continued);
    }

    /// The journal takes commands for a process: the core shows one running, or the shim of a
    /// continue is attached (its `spawned` record is the next thing the core will apply).
    fn takes_commands(&self) -> bool {
        self.core.process_running() || self.continuation.is_attached()
    }

    /// Journal `command` as a note; its result is delivered when the note comes back through the
    /// journal and the core applies it, exactly as a replay will.
    fn journal_command(
        &mut self,
        command: Command,
        reply: CommandReply,
        intent: Intent,
        continued: bool,
    ) {
        if self.pending.len() >= MAX_PENDING {
            self.refuse(&intent, reply, CommandError::Busy);
            return;
        }
        let tok = token();
        self.push_op(Op::Note {
            data: RuntimeNote::Command {
                token: tok.clone(),
                command,
                trace: intent.trace.clone(),
            }
            .to_value(),
            token: Some(tok.clone()),
            attach: false,
        });
        self.pending.insert(
            tok,
            Pending {
                reply: Reply::Command(reply),
                continued,
                note_failed: None,
                deadline: Instant::now() + self.cfg.timeouts.command,
                intent: Some(intent),
            },
        );
    }

    /// A command that continues an ended session as its first step: starts the continue unless
    /// one is under way, and holds the command until its shim is attached. Nothing is journaled
    /// for the command until then; if the continue cannot start, the sender gets
    /// [`CommandError::ContinueFailed`] and the session is as it was.
    fn continue_for(&mut self, command: Command, reply: CommandReply, intent: Intent) {
        if self.pending.len() + self.continuation.held() >= MAX_PENDING {
            self.refuse(&intent, reply, CommandError::Busy);
            return;
        }
        if self.continuation.is_idle() {
            if let Err(refused) = self.begin_continue() {
                let err = if refused.blocked == Some(NotContinuable::NotEnded) {
                    // No process is shown, but nothing has ended either (the journal has no
                    // record of a process yet): there is nothing to continue.
                    CommandError::NotRunning {
                        reason: format!("the session is {}", self.core.snapshot().phase.name()),
                    }
                } else {
                    CommandError::ContinueFailed {
                        reason: refused.error.to_string(),
                        blocked: refused.blocked,
                    }
                };
                self.refuse(&intent, reply, err);
                return;
            }
        }
        if let Some(w) = self.continuation.waiting() {
            w.held.push(Held {
                command,
                reply,
                intent,
            });
        }
    }

    /// Answers a command-like request that cannot be journaled, and records that it was refused.
    fn refuse(&self, intent: &Intent, reply: CommandReply, err: CommandError) {
        diag::command_refused(&self.id, intent, &err);
        let _ = reply.send(Err(err)); // requester may have given up
    }

    /// A journaled-note request whose note could not be journaled: the session is not running.
    fn answer_not_running(&self, p: Pending, reason: &str) {
        match p.reply {
            Reply::Command(r) => {
                let err = if p.continued {
                    CommandError::ContinueFailed {
                        reason: reason.to_string(),
                        blocked: None,
                    }
                } else {
                    CommandError::NotRunning {
                        reason: reason.to_string(),
                    }
                };
                if let Some(i) = &p.intent {
                    diag::command_refused(&self.id, i, &err);
                }
                let _ = r.send(Err(err)); // requester may have given up
            }
            Reply::Stop(r) => {
                // The process is gone, which is what a stop wants.
                let _ = r.send(Ok(())); // requester may have given up
            }
        }
    }

    /// A command held behind a continue could not be given to the new process: it ended first.
    fn answer_ended_before_prompt(&self, p: Pending) {
        let how = describe_end(&self.core.snapshot().phase);
        self.answer_not_running(
            p,
            &format!("the new process ended before it could be given the command ({how})"),
        );
    }

    /// The core applied the exit of a process some held command could not be given to.
    fn answer_refused_notes(&mut self) {
        let refused: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| p.note_failed.is_some())
            .map(|(t, _)| t.clone())
            .collect();
        for t in refused {
            if let Some(p) = self.pending.remove(&t) {
                self.answer_ended_before_prompt(p);
            }
        }
    }

    fn answer_busy(&self, p: Pending) {
        match p.reply {
            Reply::Command(r) => {
                if let Some(i) = &p.intent {
                    diag::command_refused(&self.id, i, &CommandError::Busy);
                }
                let _ = r.send(Err(CommandError::Busy)); // requester may have given up
            }
            Reply::Stop(r) => {
                let _ = r.send(Err(RuntimeError::Busy("request queue overflow".into())));
                // requester may have given up
            }
        }
    }

    /// A message that arrived while the session loads and could not even be deferred.
    fn answer_msg_busy(&self, m: Msg) {
        match m {
            Msg::Command {
                command,
                trace,
                reply,
            } => self.refuse(
                &Intent::new(command.name(), trace),
                reply,
                CommandError::Busy,
            ),
            Msg::Gate { op, reply } => {
                self.refuse(&Intent::new(op.name(), None), reply, CommandError::Busy)
            }
            Msg::Stop { reply } | Msg::Continue { reply } | Msg::Rename { reply, .. } => {
                let _ = reply.send(Err(RuntimeError::Busy("session is loading".into())));
                // requester may have given up
            }
            Msg::Subscribe { .. } | Msg::History { .. } | Msg::Trail { .. } => {}
        }
    }

    fn gate(&mut self, op: GateOp, reply: CommandReply) {
        let intent = Intent::new(op.name(), None);
        if !self.core.process_running() {
            let reason = format!("the session is {}", self.core.snapshot().phase.name());
            self.refuse(&intent, reply, CommandError::NotRunning { reason });
            return;
        }
        if self.pending.len() >= MAX_PENDING {
            self.refuse(&intent, reply, CommandError::Busy);
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
                continued: false,
                note_failed: None,
                deadline: Instant::now() + self.cfg.timeouts.command,
                intent: Some(intent),
            },
        );
    }

    /// `sessions.continue`: the new process is up and attached when this answers `Ok`. A
    /// continue already under way is joined, not refused.
    fn continue_session(&mut self, reply: UnitReply) {
        match &self.continuation {
            Continuation::Starting(_) => {
                if let Some(w) = self.continuation.waiting() {
                    w.continues.push(reply);
                }
            }
            Continuation::Attached(_) => {
                // The new shim is attached; the journal shows its process next.
                let _ = self.continuation.join_attached(reply);
            }
            Continuation::Idle => match self.begin_continue() {
                Ok(()) => {
                    if let Some(w) = self.continuation.waiting() {
                        w.continues.push(reply);
                    }
                }
                Err(refused) => {
                    let _ = reply.send(Err(refused.error)); // requester may have given up
                }
            },
        }
    }

    /// Start a new process generation of an ended session: the runtime's rule says whether it
    /// can be, and how (resume the agent's conversation if it holds it, else start it fresh under
    /// the id chosen for it), from the session's facts alone. On `Ok` the continuation is
    /// `Starting` and the shim is being spawned; the caller adds who waits for it.
    fn begin_continue(&mut self) -> Result<(), ContinueRefused> {
        let plan = self.core.continue_plan().map_err(|blocked| {
            let error = match blocked {
                NotContinuable::NotEnded => {
                    RuntimeError::InvalidState("the session is still running".into())
                }
                NotContinuable::Loading => {
                    RuntimeError::InvalidState("the session is still loading".into())
                }
                NotContinuable::AgentCannotResume => RuntimeError::Unsupported(format!(
                    "driver {} cannot resume sessions",
                    self.desc.id()
                )),
                NotContinuable::NoConversationId => RuntimeError::InvalidState(
                    "the CLI never reported a native session id; nothing to resume".into(),
                ),
                NotContinuable::NothingSaved => RuntimeError::InvalidState(
                    "the agent has not saved this conversation yet (it saves it with its first \
                     message), so there is nothing to resume"
                        .into(),
                ),
            };
            ContinueRefused {
                error,
                blocked: Some(blocked),
            }
        })?;
        if self.stream.is_some() {
            // The previous generation's journal (a lingering shim, or the file of a finished
            // one) is only still being read for records we already have, the last of which
            // ended its process: let it go.
            self.stream = None;
            self.drop_connection();
        }
        if self.source.is_some() {
            return Err(ContinueRefused::state(
                NotContinuable::Loading,
                "the session is still being read",
            ));
        }
        // One owner of the connection: a reconnect scheduled for the previous process has no
        // process to reach any more.
        self.reconnect_at = None;
        let mut spec = self.file.spec.clone();
        // The first prompt belongs to the first generation (its `start` note); a continued
        // session starts without one.
        spec.first_prompt = None;
        match plan {
            ContinuePlan::Resume(id) => {
                spec.resume = ResumeMode::Resume(id);
                spec.native_session_id = None;
                // The conversation has its title; the user may have renamed the session since.
                spec.name = None;
            }
            ContinuePlan::StartFresh(id) => {
                // The agent never stored the conversation: it starts now, under the id chosen for
                // it, and gets the session's name as it is today.
                spec.resume = ResumeMode::Fresh;
                spec.native_session_id = Some(id);
                spec.name = self.file.display_name.clone();
            }
        }
        // A session created before session ids gets one now: the new generation's protocol
        // starts fresh, so its lines may differ from the old generation's.
        if spec.session_id.is_none() {
            spec.session_id = Some(self.id.to_string());
        }
        let shim_spec =
            store::shim_spec(&self.cfg, self.desc.as_ref(), &self.dir, &spec).map_err(|error| {
                ContinueRefused {
                    error,
                    blocked: None,
                }
            })?;
        self.pending_spec = spec.clone();
        // The trace of the request that created the session belongs to its first generation.
        self.start_trace = None;
        self.file.spec = spec;
        self.save_file(None);
        self.reconnect_attempt = 0;
        self.continuation.begin();
        self.source = Some(spawn_source(shim_spec, self.core.last_seq() + 1));
        Ok(())
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

    fn history(&self, before: Option<u64>, limit: usize) -> Local<HistoryPage> {
        let below: Vec<&SessionEvent> = self
            .ring
            .iter()
            .filter(|e| before.is_none_or(|b| e.cursor < b))
            .collect();
        match last_window(below, limit, self.evicted_max.is_none()) {
            Some((events, has_more)) => Local::Page(HistoryPage {
                events: events.into_iter().cloned().collect(),
                has_more,
            }),
            None => Local::Replay,
        }
    }

    /// The same window of the trail: the projection of the events in memory, the way a replay
    /// projects them ([`crate::trail::item`]).
    fn trail(&self, before: Option<u64>, limit: usize) -> Local<TrailPage> {
        let below: Vec<crate::wire::TrailItem> = self
            .ring
            .iter()
            .filter(|e| before.is_none_or(|b| e.cursor < b))
            .filter_map(crate::trail::item)
            .collect();
        match last_window(below, limit, self.evicted_max.is_none()) {
            Some((entries, has_more)) => Local::Page(TrailPage { entries, has_more }),
            None => Local::Replay,
        }
    }
}

/// The last `limit` of `items` (oldest first) and whether older ones may exist, when memory can
/// answer: `items` fill the window, or nothing was ever evicted from it (`complete`). `None`:
/// the page reaches back past what memory holds; replay the journal.
fn last_window<T>(mut items: Vec<T>, limit: usize, complete: bool) -> Option<(Vec<T>, bool)> {
    if items.len() >= limit {
        let start = items.len() - limit;
        Some((items.split_off(start), start > 0 || !complete))
    } else if complete {
        Some((items, false))
    } else {
        None
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
                    diag::persist_failed(&current.id, &e);
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

#[cfg(test)]
mod tests {
    //! Every way out of a continue that is under way, with the requests waiting for it. The
    //! actor's I/O is not involved: the test hands it the outcome of the spawn the way the run
    //! loop does (`on_source`, `on_stream_item`).

    use super::*;
    use crate::core::{Conversation, SessionMeta};
    use crate::wire::CommandOutcome;
    use wasit_shim::{ExitInfo, Kind};

    const U: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            // Best-effort removal of a scratch directory.
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// An actor of an ended claude session (its first process exited after a stop) with a chosen
    /// conversation id, not loading, with no I/O under way.
    fn ended() -> (Actor, Scratch) {
        let dir =
            std::env::temp_dir().join(format!("wact-{}-{}", std::process::id(), fastrand::u32(..)));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = Arc::new(RuntimeConfig::new(
            &dir,
            crate::config::ShimLauncher::binary("/nonexistent/wasit-shim"),
        ));
        let desc = wasit_claude::descriptor();
        let id = SessionId("aaaaaaaaaaaa".into());
        let spec = SpawnSpec {
            native_session_id: Some(U.into()),
            cwd: Some(dir.to_string_lossy().into_owned()),
            ..SpawnSpec::default()
        };
        let file = SessionFile::new(
            id.clone(),
            desc.id(),
            0,
            Default::default(),
            spec,
            Vec::new(),
        );
        let meta: SessionMeta = file.meta();
        assert_eq!(meta.conversation, Conversation::Chosen(U.into()));
        let (snapshot, _) = watch::channel(Arc::new(crate::core::blank_snapshot(
            &meta,
            desc.as_ref(),
            Phase::Starting,
            true,
        )));
        let mut a = Actor::new(ActorInit {
            dir: dir.clone(),
            desc,
            cfg,
            file,
            snapshot,
            notify: Arc::new(|_| {}),
            start_trace: None,
        });
        // The initial adoption is over: the first generation ended.
        a.source = None;
        a.loading = false;
        for (seq, kind, data) in [
            (
                1,
                Kind::Spawned,
                serde_json::json!({"pid": 7, "argv": ["claude"]}),
            ),
            (2, Kind::Exited, serde_json::json!({"code": 0})),
        ] {
            a.core.apply(&Record {
                seq,
                t: seq,
                kind,
                data,
                partial: false,
            });
        }
        assert!(
            a.core.snapshot().continuable,
            "{:?}",
            a.core.snapshot().not_continuable
        );
        (a, Scratch(dir))
    }

    type Answer = oneshot::Receiver<Result<CommandOutcome, CommandError>>;

    /// A prompt arrives at the actor; its eventual answer.
    fn prompt(a: &mut Actor, text: &str) -> Answer {
        let (reply, rx) = oneshot::channel();
        a.on_msg(Msg::Command {
            command: Command::prompt(text),
            trace: None,
            reply,
        });
        rx
    }

    fn explicit_continue(a: &mut Actor) -> oneshot::Receiver<Result<(), RuntimeError>> {
        let (reply, rx) = oneshot::channel();
        a.on_msg(Msg::Continue { reply });
        rx
    }

    /// Nothing was journaled for anybody, and nobody waits any more.
    fn assert_clean(a: &Actor) {
        assert!(a.continuation.is_idle());
        assert!(a.pending.is_empty());
        assert!(
            !a.outbox
                .iter()
                .any(|op| matches!(op, Op::Note { token: Some(_), .. })),
            "a command note was queued"
        );
    }

    fn continue_failed(
        r: Result<Result<CommandOutcome, CommandError>, oneshot::error::TryRecvError>,
    ) -> String {
        match r {
            Ok(Err(CommandError::ContinueFailed { reason, .. })) => reason,
            other => panic!("expected ContinueFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_prompt_to_an_ended_session_starts_a_continue_and_is_held_not_journaled() {
        let (mut a, _s) = ended();
        let mut one = prompt(&mut a, "one");
        let mut two = prompt(&mut a, "two");
        // One continue, two held prompts, in order; nothing journaled, nobody answered yet.
        assert!(a.continuation.is_starting());
        assert_eq!(a.continuation.held(), 2);
        assert!(a.source.is_some(), "the shim is being spawned");
        assert!(a.pending.is_empty());
        assert!(a.outbox.is_empty());
        assert!(one.try_recv().is_err() && two.try_recv().is_err());
        // The spec of the generation was decided from the facts: the conversation was never
        // stored, so it starts fresh under the chosen id and nothing else.
        assert_eq!(a.pending_spec.resume, ResumeMode::Fresh);
        assert_eq!(a.pending_spec.native_session_id.as_deref(), Some(U));
        assert!(a.pending_spec.first_prompt.is_none());
    }

    #[tokio::test]
    async fn a_spawn_that_fails_answers_everybody_and_journals_nothing() {
        let (mut a, _s) = ended();
        let mut waiter = explicit_continue(&mut a);
        let mut one = prompt(&mut a, "one");
        let mut two = prompt(&mut a, "two");
        a.source = None;
        a.on_source(Source::SpawnFailed("no such file".into()));
        assert!(
            matches!(waiter.try_recv(), Ok(Err(RuntimeError::Spawn(m))) if m == "no such file")
        );
        for rx in [&mut one, &mut two] {
            let reason = continue_failed(rx.try_recv());
            assert!(reason.contains("no such file"), "{reason}");
        }
        assert_clean(&a);
        // The session is as it was and can be continued again.
        assert!(a.core.snapshot().continuable);
        let _again = prompt(&mut a, "three");
        assert!(a.continuation.is_starting());
    }

    #[tokio::test]
    async fn retries_that_run_out_answer_everybody_with_why() {
        let (mut a, _s) = ended();
        let mut waiter = explicit_continue(&mut a);
        let mut one = prompt(&mut a, "one");
        a.source = None;
        let attempts = a.cfg.timeouts.reconnect_attempts;
        for _ in 0..attempts {
            a.on_source(Source::Retry("connect: refused".into()));
            assert!(a.continuation.is_starting(), "still trying");
            a.source = None;
        }
        assert!(waiter.try_recv().is_err() && one.try_recv().is_err());
        a.on_source(Source::Retry("connect: refused".into()));
        assert!(
            matches!(waiter.try_recv(), Ok(Err(RuntimeError::Spawn(m))) if m.contains("connect: refused"))
        );
        let reason = continue_failed(one.try_recv());
        assert!(reason.contains("connect: refused"), "{reason}");
        assert_clean(&a);
        // The session did not become lost: it is the ended session it was.
        assert!(matches!(a.core.snapshot().phase, Phase::Exited { .. }));
        assert!(a.core.snapshot().continuable);
    }

    #[tokio::test]
    async fn a_new_shim_that_had_already_ended_answers_everybody_with_how_it_ended() {
        let (mut a, s) = ended();
        let mut waiter = explicit_continue(&mut a);
        let mut one = prompt(&mut a, "one");
        a.source = None;
        // What the shim journaled before it was reached: a process that died at once.
        let journal: String = [
            r#"{"seq":1,"t":1,"kind":"spawned","data":{"pid":7,"argv":["claude"]}}"#,
            r#"{"seq":2,"t":2,"kind":"exited","data":{"code":0}}"#,
            r#"{"seq":3,"t":3,"kind":"spawned","data":{"pid":8,"argv":["claude"]}}"#,
            r#"{"seq":4,"t":4,"kind":"err","data":"boom"}"#,
            r#"{"seq":5,"t":5,"kind":"exited","data":{"code":2}}"#,
        ]
        .map(|l| format!("{l}\n"))
        .concat();
        std::fs::write(s.0.join("journal.ndjson"), journal).unwrap();
        let stream = wasit_shim::read_journal(&s.0, 3).await.unwrap();
        a.on_source(Source::Offline(
            stream,
            ShimState::Exited(ExitInfo {
                code: Some(2),
                signal: None,
            }),
        ));
        // The records are read, then the stream ends.
        while a.continuation.is_starting() {
            let item = a.stream.as_mut().unwrap().next().await;
            a.on_stream_item(item);
        }
        assert!(
            matches!(waiter.try_recv(), Ok(Err(RuntimeError::Spawn(m))) if m.contains("code 2")),
            "waiter"
        );
        let reason = continue_failed(one.try_recv());
        assert!(reason.contains("code 2"), "{reason}");
        assert_clean(&a);
        // It ran and failed: the session shows that, and can be continued again.
        assert!(matches!(a.core.snapshot().phase, Phase::Failed { .. }));
        assert_eq!(a.core.snapshot().generation, 2);
        assert!(a.core.snapshot().continuable);
    }

    #[tokio::test]
    async fn only_prompts_continue_an_ended_session() {
        let (mut a, _s) = ended();
        let (reply, mut rx) = oneshot::channel();
        a.on_msg(Msg::Command {
            command: Command::Interrupt,
            trace: None,
            reply,
        });
        assert!(matches!(
            rx.try_recv(),
            Ok(Err(CommandError::NotRunning { .. }))
        ));
        assert!(a.continuation.is_idle() && a.source.is_none());
    }

    #[tokio::test]
    async fn a_prompt_to_a_session_that_cannot_be_continued_says_why_and_starts_nothing() {
        let (mut a, _s) = ended();
        // The agent cannot resume what it never stored (a session of an older build, no id).
        a.core = Core::new(
            wasit_claude::descriptor(),
            &SessionMeta {
                conversation: Conversation::Unknown,
                ..a.file.meta()
            },
        );
        a.core.apply(&Record {
            seq: 1,
            t: 1,
            kind: Kind::Spawned,
            data: serde_json::json!({"pid": 7}),
            partial: false,
        });
        a.core.apply(&Record {
            seq: 2,
            t: 2,
            kind: Kind::Exited,
            data: serde_json::json!({"code": 0}),
            partial: false,
        });
        let mut rx = prompt(&mut a, "x");
        match rx.try_recv() {
            Ok(Err(CommandError::ContinueFailed { reason, blocked })) => {
                assert!(reason.contains("never reported"), "{reason}");
                assert_eq!(blocked, Some(crate::wire::NotContinuable::NoConversationId));
            }
            other => panic!("{other:?}"),
        }
        assert_clean(&a);
        assert!(a.source.is_none());
    }

    /// A stop that arrives while the continue is starting is not lost: it waits for the shim and
    /// is answered with the continue's outcome.
    #[tokio::test]
    async fn a_stop_during_a_continue_is_answered_when_the_continue_fails() {
        let (mut a, _s) = ended();
        let _waiter = explicit_continue(&mut a);
        let (reply, mut stop) = oneshot::channel();
        a.on_msg(Msg::Stop { reply });
        assert!(stop.try_recv().is_err(), "held, not answered yet");
        a.source = None;
        a.on_source(Source::SpawnFailed("gone".into()));
        assert!(matches!(stop.try_recv(), Ok(Ok(()))));
        assert_clean(&a);
    }
}
