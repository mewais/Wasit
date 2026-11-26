//! A client that keeps its link up, and durable subscriptions on top of it.

use std::mem::discriminant;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wasit_diag::{ErrorInfo, Ident};

use crate::attempt::{Attempt, StageSink};
use crate::client::ControlClient;
use crate::command::CommandFailureKind;
use crate::connection::{RequestOpts, SubscribeOpts, Subscription};
use crate::diag::{self, AttemptOutcome, ProvisionOutcome};
use crate::error::{CallError, ConnClose, ConnectError};
use crate::uplink::UplinkChanges;
use crate::wire::{CloseReason, Cursor, Hello, HostId, ProtocolVersion, RpcError};

/// Exponential backoff with jitter.
#[derive(Debug, Clone, PartialEq)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
    /// Fraction (0..=1) of the delay that is randomly removed.
    pub jitter: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.3,
        }
    }
}

impl Backoff {
    /// Delay before retry number `attempt` (1-based), given a uniform random `rand01`.
    pub fn delay(&self, attempt: u32, rand01: f64) -> Duration {
        let exp = self
            .multiplier
            .max(1.0)
            .powi(attempt.saturating_sub(1).min(64) as i32);
        let base = (self.initial.as_secs_f64() * exp).min(self.max.as_secs_f64());
        let j = self.jitter.clamp(0.0, 1.0) * rand01.clamp(0.0, 1.0);
        Duration::from_secs_f64((base * (1.0 - j)).max(0.0))
    }

    fn delay_random(&self, attempt: u32) -> Duration {
        self.delay(attempt, fastrand::f64())
    }
}

/// Why a host needs provisioning (installing/updating its helper binary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProvisionReason {
    /// The helper command is not installed on the host.
    CommandNotFound { detail: String },
    /// The helper answered but runs a different build.
    BuildMismatch { expected: String, found: String },
    /// The helper speaks an incompatible protocol major version.
    Incompatible {
        ours: ProtocolVersion,
        theirs: ProtocolVersion,
    },
}

impl ProvisionReason {
    /// The kind as the word it serializes to (`command_not_found`, `build_mismatch`,
    /// `incompatible`).
    pub fn code(&self) -> &'static str {
        match self {
            ProvisionReason::CommandNotFound { .. } => "command_not_found",
            ProvisionReason::BuildMismatch { .. } => "build_mismatch",
            ProvisionReason::Incompatible { .. } => "incompatible",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ProvisionError {
    pub message: String,
    /// Whether retrying later (with backoff) may succeed.
    pub retryable: bool,
}

/// State of a supervised link (§4.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LinkState {
    /// An attempt is running. `stage` is the latest step the connector reported (see
    /// [`Attempt::reached`]), `None` before it reports one. `since_ms` is the wall-clock time at
    /// which that current stage was entered, in milliseconds since the Unix epoch (`0` while no
    /// stage is reported). Within an attempt it never decreases.
    /// A consumer that does not know these fields reads `{"state":"connecting"}` as before.
    Connecting {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "ts", ts(optional))]
        stage: Option<String>,
        #[serde(default)]
        #[cfg_attr(feature = "ts", ts(type = "number"))]
        since_ms: u64,
    },
    Provisioning {
        reason: ProvisionReason,
    },
    Connected {
        hello: Hello,
    },
    Reconnecting {
        attempt: u32,
        #[cfg_attr(
            feature = "ts",
            ts(type = "{ secs_since_epoch: number, nanos_since_epoch: number }")
        )]
        next_at: SystemTime,
        last_error: String,
    },
    /// Needs a human (credentials, host key). Waits for `reconnect_now`.
    AuthNeeded {
        problem: CommandFailureKind,
        detail: String,
        /// Why the link was not connected when it was parked: the reason the connection dropped
        /// (or the last attempt failed), in the supervisor's words. Kept beside `detail`, never
        /// replaced by it. `None` when the link had not been up (or tried) before.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "ts", ts(optional))]
        cause: Option<String>,
        /// The link was parked without an attempt because [`SupervisorHooks::hold`] said a
        /// person must ask for the next one, not because an attempt failed: the next attempt
        /// is the person's click ([`Supervised::reconnect_now`]), and it will ask them
        /// something. `detail` is the hook's words.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        held: bool,
    },
    /// Different protocol major and nothing can fix it automatically. Waits for
    /// `reconnect_now`.
    Incompatible {
        ours: ProtocolVersion,
        theirs: ProtocolVersion,
    },
    /// Gave up until `reconnect_now`: a configuration problem (see
    /// [`CommandFailureKind::is_configuration`]) or provisioning that cannot succeed.
    Failed {
        reason: String,
        /// What kind of problem, when known (lets a UI word it and say what to do).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "ts", ts(optional))]
        problem: Option<CommandFailureKind>,
    },
    Stopped,
}

impl LinkState {
    pub fn is_connected(&self) -> bool {
        matches!(self, LinkState::Connected { .. })
    }

    /// The state as the word its `state` tag serializes to (`connecting`, `reconnecting`, …).
    pub fn code(&self) -> &'static str {
        match self {
            LinkState::Connecting { .. } => "connecting",
            LinkState::Provisioning { .. } => "provisioning",
            LinkState::Connected { .. } => "connected",
            LinkState::Reconnecting { .. } => "reconnecting",
            LinkState::AuthNeeded { .. } => "auth_needed",
            LinkState::Incompatible { .. } => "incompatible",
            LinkState::Failed { .. } => "failed",
            LinkState::Stopped => "stopped",
        }
    }
}

/// Policy hooks (the hub uses them to drive provisioning).
#[async_trait]
pub trait SupervisorHooks: Send + Sync + 'static {
    /// Inspect the peer's hello; `Some` rejects the connection as needing provisioning.
    fn check_hello(&self, _hello: &Hello) -> Option<ProvisionReason> {
        None
    }

    /// Try to fix `reason`. `None` = provisioning is not available.
    async fn provision(&self, _reason: &ProvisionReason) -> Option<Result<(), ProvisionError>> {
        None
    }

    /// Asked before every attempt the supervisor would start by itself (the first one, the one
    /// after a drop, the one after a backoff, the one after provisioning) and before it
    /// announces a backoff: `Some(detail)` parks the link as [`LinkState::AuthNeeded`] with
    /// `held` set, without trying, until [`Supervised::reconnect_now`] and a `None` answer.
    /// For a link whose every connect asks a person for something (a code, a push approval):
    /// retrying it on a timer would spend that on nobody. Never a failure of an attempt, so
    /// nothing is lost: the link keeps the reason it was down.
    ///
    /// Asynchronous because what decides it may need a look outside the process (the product
    /// asks whether a shared connection that would carry the attempt without a sign-in is
    /// still open).
    async fn hold(&self) -> Option<String> {
        None
    }
}

/// Produces a fresh connection on each call. The argument is the [`Attempt`]: its number is 1
/// for the first call of a link, then +1 per call, never reset (not by a successful connect,
/// not by `reconnect_now`), so what a connector records can be joined with what the supervisor
/// records for the same attempt; and it is where the connector reports how far it has got, for
/// the link state if the supervisor ends the attempt at its bound
/// ([`SupervisedConfig::connect_timeout`]).
pub type Connector =
    Arc<dyn Fn(Attempt) -> BoxFuture<'static, Result<ControlClient, ConnectError>> + Send + Sync>;

/// Build a [`Connector`] from an async closure that does not need the attempt number.
pub fn connector<F, Fut>(f: F) -> Connector
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ControlClient, ConnectError>> + Send + 'static,
{
    Arc::new(move |_attempt| Box::pin(f()))
}

/// Build a [`Connector`] from an async closure that is given the [`Attempt`] (its number, and
/// the place to report progress).
pub fn connector_reporting<F, Fut>(f: F) -> Connector
where
    F: Fn(Attempt) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ControlClient, ConnectError>> + Send + 'static,
{
    Arc::new(move |attempt| Box::pin(f(attempt)))
}

/// Build a [`Connector`] from an async closure that is given the attempt number.
pub fn connector_with_attempt<F, Fut>(f: F) -> Connector
where
    F: Fn(u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ControlClient, ConnectError>> + Send + 'static,
{
    Arc::new(move |attempt| Box::pin(f(attempt.n())))
}

pub type StateCallback = Arc<dyn Fn(&LinkState) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct SupervisedConfig {
    pub backoff: Backoff,
    /// Bound on one connect attempt (spawn + handshake). `None`: no deadline; an attempt then
    /// ends only on its own result, or when the link is stopped or dropped. What the attempt
    /// reported ([`Attempt::reached`]) is still shown while it runs.
    pub connect_timeout: Option<Duration>,
    /// Bound on one provisioning run.
    pub provision_timeout: Duration,
    /// Consecutive attempts where the far side ran but its bridge could not reach (or start)
    /// its daemon (`bridge_unavailable`) before the link parks as `Failed`: that is a problem
    /// on the host, not a dropped connection, and retrying forever only hides it.
    pub bridge_unavailable_attempts: u32,
}

impl Default for SupervisedConfig {
    fn default() -> Self {
        SupervisedConfig {
            backoff: Backoff::default(),
            connect_timeout: Some(Duration::from_secs(30)),
            provision_timeout: Duration::from_secs(300),
            bridge_unavailable_attempts: 3,
        }
    }
}

#[derive(Clone, Default)]
pub struct SupervisedOptions {
    pub config: SupervisedConfig,
    pub hooks: Option<Arc<dyn SupervisorHooks>>,
    /// Called synchronously on every state change (no transition is skipped, unlike a
    /// `watch` observer).
    pub on_state: Option<StateCallback>,
    /// The link's label in diagnostics (`hosts:<id>` for a hub's links). With it every attempt,
    /// state change, provisioning run and close of the link is recorded; `None` (an in-process
    /// loopback) records nothing.
    pub diag: Option<Ident>,
    /// The host this link leads to, written as `host` in the link's records so a log can be
    /// filtered by host; `None` for links that are not to a host.
    pub diag_host: Option<Ident>,
}

struct Shared {
    state: watch::Sender<LinkState>,
    client: watch::Sender<Option<ControlClient>>,
    reconnect: watch::Sender<u64>,
    stop: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    backoff: Backoff,
    on_state: Option<StateCallback>,
    diag: Option<Ident>,
    diag_host: Option<Ident>,
    /// Serialises every publication of the link state, so a stage report and a transition
    /// cannot interleave (see [`Shared::stage`]).
    publish: Mutex<()>,
    /// The number of the attempt the supervisor is running now, 0 when none is. A stage report
    /// from an attempt that is no longer running is not published.
    running: AtomicU64,
}

impl Shared {
    /// Publishes `s`. `why` is the text that goes with it in diagnostics when it is not the
    /// one `s` carries itself; `n` is the link's attempt number (0 before the first attempt).
    fn set(&self, s: LinkState, why: Option<&str>, n: u64) {
        let _publishing = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(link) = &self.diag {
            let from = self.state.borrow().clone();
            if from.code() != s.code() {
                diag::state(link, self.diag_host.as_ref(), n, &from, &s, why);
            }
        }
        if let Some(cb) = &self.on_state {
            cb(&s);
        }
        self.state.send_replace(s);
    }

    /// Publishes that attempt `n` has reached `stage`, entered at `since_ms` (epoch ms). Only
    /// while attempt `n` is the one running and the link is still connecting: a report that
    /// arrives after the attempt ended (its connector was dropped, or the link moved on) is
    /// dropped, not published over the state that replaced it.
    fn stage(&self, n: u64, stage: &str, since_ms: u64) {
        let _publishing = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        if self.running.load(Ordering::SeqCst) != n
            || !matches!(*self.state.borrow(), LinkState::Connecting { .. })
        {
            return;
        }
        let s = LinkState::Connecting {
            stage: Some(stage.to_owned()),
            since_ms,
        };
        if let Some(cb) = &self.on_state {
            cb(&s);
        }
        self.state.send_replace(s);
    }

    /// Records the end of an attempt; `error` is only built when the link has a label.
    fn attempt_end(
        &self,
        n: u64,
        started: Instant,
        outcome: AttemptOutcome,
        error: impl FnOnce() -> Option<ErrorInfo>,
    ) {
        if let Some(link) = &self.diag {
            diag::attempt_end(link, self.diag_host.as_ref(), n, outcome, error(), started);
        }
    }
}

struct StopOnDrop(CancellationToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A client that keeps its link up. Cheap to clone; supervision stops when the last clone
/// (including durable subscriptions) is dropped or on [`Supervised::stop`].
#[derive(Clone)]
pub struct Supervised {
    shared: Arc<Shared>,
    _stop: Arc<StopOnDrop>,
}

enum Next {
    Immediate,
    Backoff,
    Park(LinkState),
    Provision {
        reason: ProvisionReason,
        fallback: LinkState,
    },
}

impl Next {
    /// What an attempt that led to this decision is recorded as; `None` for a decision no
    /// failed attempt leads to.
    fn outcome(&self) -> Option<AttemptOutcome> {
        match self {
            Next::Immediate => None,
            Next::Backoff => Some(AttemptOutcome::Backoff),
            Next::Park(_) => Some(AttemptOutcome::Park),
            Next::Provision { .. } => Some(AttemptOutcome::Provision),
        }
    }
}

impl Supervised {
    pub fn spawn(connector: Connector, opts: SupervisedOptions) -> Supervised {
        let stop = CancellationToken::new();
        let shared = Arc::new(Shared {
            state: watch::channel(LinkState::Connecting {
                stage: None,
                since_ms: 0,
            })
            .0,
            client: watch::channel(None).0,
            reconnect: watch::channel(0).0,
            stop: stop.clone(),
            task: Mutex::new(None),
            backoff: opts.config.backoff.clone(),
            on_state: opts.on_state.clone(),
            diag: opts.diag.clone(),
            diag_host: opts.diag_host.clone(),
            publish: Mutex::new(()),
            running: AtomicU64::new(0),
        });
        let task = tokio::spawn(run(shared.clone(), connector, opts.config, opts.hooks));
        *shared.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
        Supervised {
            shared,
            _stop: Arc::new(StopOnDrop(stop)),
        }
    }

    pub fn state(&self) -> watch::Receiver<LinkState> {
        self.shared.state.subscribe()
    }

    pub fn current_state(&self) -> LinkState {
        self.shared.state.borrow().clone()
    }

    /// The live client, if connected.
    pub fn current(&self) -> Option<ControlClient> {
        self.shared
            .client
            .borrow()
            .as_ref()
            .filter(|c| !c.is_closed())
            .cloned()
    }

    /// Wait up to `max` for a live client (`Duration::ZERO` = fail fast).
    pub async fn wait_connected(&self, max: Duration) -> Result<ControlClient, CallError> {
        if let Some(c) = self.current() {
            return Ok(c);
        }
        if max.is_zero() {
            return Err(CallError::NotConnected);
        }
        let mut rx = self.shared.client.subscribe();
        tokio::select! {
            _ = self.shared.stop.cancelled() => Err(CallError::NotConnected),
            r = tokio::time::timeout(max, rx.wait_for(|c| c.as_ref().is_some_and(|c| !c.is_closed()))) => match r {
                Ok(Ok(c)) => c.clone().ok_or(CallError::NotConnected),
                _ => Err(CallError::NotConnected),
            },
        }
    }

    /// Request over the current link. If disconnected, waits up to `wait_connected` for the
    /// link (zero = fail fast with `NotConnected`); then `opts.deadline` bounds the call.
    pub async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: impl Serialize,
        opts: RequestOpts,
        wait_connected: Duration,
    ) -> Result<T, CallError> {
        let client = self.wait_connected(wait_connected).await?;
        client.request_with(method, params, opts).await
    }

    /// A subscription that survives reconnects and lag (resubscribes from its last cursor).
    pub fn subscribe_durable(
        &self,
        topic: impl Into<String>,
        opts: DurableOpts,
    ) -> DurableSubscription {
        DurableSubscription {
            sup: self.clone(),
            topic: topic.into(),
            last: opts.from.clone(),
            opts,
            current: None,
            end: None,
            subscribes: 0,
            route: None,
            retry: 0,
        }
    }

    /// Drop the current connection (if any) and connect again now, skipping any backoff or
    /// parked state (AuthNeeded / Failed / Incompatible).
    pub fn reconnect_now(&self) {
        self.shared.reconnect.send_modify(|n| *n += 1);
    }

    /// Stop supervising and close the link (bounded wait for the task).
    pub async fn stop(&self) {
        self.shared.stop.cancel();
        let task = self
            .shared
            .task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(t) = task {
            if tokio::time::timeout(Duration::from_secs(5), t)
                .await
                .is_err()
            {
                tracing::warn!("supervisor task did not stop within 5s");
            }
        }
    }

    async fn next_client(&self) -> Option<ControlClient> {
        let mut rx = self.shared.client.subscribe();
        tokio::select! {
            _ = self.shared.stop.cancelled() => None,
            r = rx.wait_for(|c| c.as_ref().is_some_and(|c| !c.is_closed())) => r.ok().and_then(|c| c.clone()),
        }
    }
}

async fn run(
    shared: Arc<Shared>,
    connector: Connector,
    cfg: SupervisedConfig,
    hooks: Option<Arc<dyn SupervisorHooks>>,
) {
    let stop = shared.stop.clone();
    let mut reconnect_rx = shared.reconnect.subscribe();
    let mut attempt: u32 = 0;
    // Attempts of this link in all, never reset: what the connector and the diagnostics join on.
    let mut n: u64 = 0;
    let mut provisioned: Option<ProvisionReason> = None;
    let mut last_error = String::new();
    // `last_error` as diagnostics may show it: the same, except that what a peer or a parser
    // wrote is replaced by its code (see `diag`).
    let mut last_why = String::new();
    // Set when a parked state's own text would show `last_error` as it is.
    let mut park_why: Option<String> = None;
    let mut bridge_unavailable: u32 = 0;

    'outer: loop {
        if stop.is_cancelled() {
            break;
        }
        // A link the product says must wait for a person (the first attempt of a link that
        // asks for a sign-in each time, or one that follows a failure) is not tried.
        let held = tokio::select! {
            _ = stop.cancelled() => break,
            h = hold(&hooks) => h,
        };
        if let Some(detail) = held {
            if !wait_for_person(
                &shared,
                &mut reconnect_rx,
                detail,
                &last_error,
                &last_why,
                n,
            )
            .await
            {
                break;
            }
            attempt = 0;
            bridge_unavailable = 0;
            provisioned = None;
            continue 'outer;
        }
        // Requests for a reconnect made before this attempt are satisfied by it.
        reconnect_rx.borrow_and_update();
        n += 1;
        let started = Instant::now();
        // The attempt's stage reports come back here, stamped with their wall-clock entry time.
        let sink: StageSink = {
            let shared = shared.clone();
            Arc::new(move |stage: &str, since_ms: u64| shared.stage(n, stage, since_ms))
        };
        shared.running.store(n, Ordering::SeqCst);
        shared.set(
            LinkState::Connecting {
                stage: None,
                since_ms: 0,
            },
            None,
            n,
        );
        if let Some(link) = &shared.diag {
            diag::attempt_begin(link, shared.diag_host.as_ref(), n);
        }
        let this_attempt = Attempt::new(n)
            .for_host(shared.diag_host.clone())
            .reporting(sink);
        let res = tokio::select! {
            _ = stop.cancelled() => break,
            r = bounded(cfg.connect_timeout, connector(this_attempt.clone())) => r,
        };
        // The attempt has ended (its connector has returned or was dropped): its reports are
        // no longer published.
        shared.running.store(0, Ordering::SeqCst);
        let next = match res {
            None => {
                // The connector's future is gone; what it reported is the evidence.
                last_error = this_attempt.timed_out();
                last_why = last_error.clone();
                shared.attempt_end(n, started, AttemptOutcome::Backoff, || {
                    Some(diag::connect_error(&ConnectError::Timeout))
                });
                Next::Backoff
            }
            Some(Ok(client)) => {
                let rejected = hooks
                    .as_ref()
                    .and_then(|h| h.check_hello(client.remote_hello()));
                if let Some(reason) = rejected {
                    client.close();
                    last_error = format!("peer needs provisioning: {reason:?}");
                    last_why = last_error.clone();
                    shared.attempt_end(n, started, AttemptOutcome::Provision, || None);
                    let fallback = LinkState::Failed {
                        reason: last_error.clone(),
                        problem: None,
                    };
                    Next::Provision { reason, fallback }
                } else {
                    // A reconnect requested while this attempt was in flight asked for a link,
                    // and this attempt is one: it is satisfied here. Only a request made after
                    // the link is up drops it (otherwise the link is torn down at once and
                    // redialled for nothing).
                    reconnect_rx.borrow_and_update();
                    attempt = 0;
                    bridge_unavailable = 0;
                    provisioned = None;
                    // What went wrong before is over: a later park must not quote it.
                    last_error.clear();
                    last_why.clear();
                    shared.attempt_end(n, started, AttemptOutcome::Connected, || None);
                    // The client first: whoever is told the link is connected may use it
                    // at once (a request routed in between would find no client).
                    shared.client.send_replace(Some(client.clone()));
                    let connected_at = Instant::now();
                    shared.set(
                        LinkState::Connected {
                            hello: client.remote_hello().clone(),
                        },
                        None,
                        n,
                    );
                    enum Ev {
                        Stop,
                        Closed(ConnClose),
                        Reconnect,
                    }
                    let ev = tokio::select! {
                        _ = stop.cancelled() => Ev::Stop,
                        r = client.closed() => Ev::Closed(r),
                        _ = reconnect_rx.changed() => Ev::Reconnect,
                    };
                    shared.client.send_replace(None);
                    match ev {
                        Ev::Stop => {
                            client.close();
                            if let Some(link) = &shared.diag {
                                diag::link_closed(
                                    link,
                                    shared.diag_host.as_ref(),
                                    n,
                                    &ConnClose::Local,
                                    connected_at.elapsed(),
                                    None,
                                );
                            }
                            break 'outer;
                        }
                        Ev::Closed(reason) => {
                            let facts = client.transport_facts().await;
                            last_error = match &facts {
                                Some(f) => format!("{reason} ({})", f.describe()),
                                None => reason.to_string(),
                            };
                            last_why = diag::close_why(&reason, facts.as_ref());
                            if let Some(link) = &shared.diag {
                                diag::link_closed(
                                    link,
                                    shared.diag_host.as_ref(),
                                    n,
                                    &reason,
                                    connected_at.elapsed(),
                                    facts.as_ref(),
                                );
                            }
                            Next::Backoff
                        }
                        Ev::Reconnect => {
                            client.close();
                            if let Some(link) = &shared.diag {
                                diag::link_closed(
                                    link,
                                    shared.diag_host.as_ref(),
                                    n,
                                    &ConnClose::Local,
                                    connected_at.elapsed(),
                                    None,
                                );
                            }
                            Next::Immediate
                        }
                    }
                }
            }
            Some(Err(e)) => {
                last_error = e.to_string();
                last_why = diag::connect_why(&e);
                let unavailable = matches!(
                    &e,
                    ConnectError::Refused(r) if r.code == crate::wire::codes::BRIDGE_UNAVAILABLE
                );
                bridge_unavailable = if unavailable {
                    bridge_unavailable + 1
                } else {
                    0
                };
                let error = || Some(diag::connect_error(&e));
                let next = match &e {
                    ConnectError::Refused(r)
                        if unavailable
                            && bridge_unavailable >= cfg.bridge_unavailable_attempts.max(1) =>
                    {
                        Next::Park(LinkState::Failed {
                            reason: format!(
                                "the helper on this host could not start or reach its background service                                  ({bridge_unavailable} attempts): {}",
                                r.message
                            ),
                            problem: None,
                        })
                    }
                    ConnectError::Incompatible { ours, theirs } => Next::Provision {
                        reason: ProvisionReason::Incompatible {
                            ours: *ours,
                            theirs: *theirs,
                        },
                        fallback: LinkState::Incompatible {
                            ours: *ours,
                            theirs: *theirs,
                        },
                    },
                    ConnectError::Command {
                        kind: CommandFailureKind::CommandNotFound,
                        stderr,
                        ..
                    } => Next::Provision {
                        reason: ProvisionReason::CommandNotFound {
                            detail: stderr.clone(),
                        },
                        fallback: LinkState::Failed {
                            reason: format!("helper not installed: {stderr}"),
                            problem: Some(CommandFailureKind::CommandNotFound),
                        },
                    },
                    // Configuration problems: a timer cannot fix them.
                    ConnectError::Command { kind, stderr, .. } if kind.is_configuration() => {
                        Next::Park(LinkState::Failed {
                            reason: stderr.clone(),
                            problem: Some(*kind),
                        })
                    }
                    // The local transport program cannot be started (missing, not
                    // executable): retrying will not help either.
                    ConnectError::Spawn(m) => Next::Park(LinkState::Failed {
                        reason: format!("cannot start the connection command: {m}"),
                        problem: None,
                    }),
                    ConnectError::Command { kind, stderr, .. } if kind.needs_human() => {
                        Next::Park(LinkState::AuthNeeded {
                            problem: *kind,
                            detail: stderr.clone(),
                            cause: None,
                            held: false,
                        })
                    }
                    _ => Next::Backoff,
                };
                if let Some(outcome) = next.outcome() {
                    shared.attempt_end(n, started, outcome, error);
                }
                next
            }
        };

        let next = match next {
            Next::Provision { reason, fallback } => {
                let already = provisioned
                    .as_ref()
                    .is_some_and(|p| discriminant(p) == discriminant(&reason));
                match &hooks {
                    _ if already => {
                        park_why = Some(format!("still failing after provisioning: {last_why}"));
                        Next::Park(LinkState::Failed {
                            reason: format!("still failing after provisioning: {last_error}"),
                            problem: None,
                        })
                    }
                    None => Next::Park(fallback),
                    Some(h) => {
                        shared.set(
                            LinkState::Provisioning {
                                reason: reason.clone(),
                            },
                            Some(&last_why),
                            n,
                        );
                        provisioned = Some(reason.clone());
                        let began = Instant::now();
                        let r = tokio::select! {
                            _ = stop.cancelled() => break,
                            r = tokio::time::timeout(cfg.provision_timeout, h.provision(&reason)) => r,
                        };
                        let record = |outcome: ProvisionOutcome, error: Option<&ProvisionError>| {
                            if let Some(link) = &shared.diag {
                                diag::provision(
                                    link,
                                    shared.diag_host.as_ref(),
                                    &reason,
                                    outcome,
                                    error,
                                    began,
                                );
                            }
                        };
                        match r {
                            Err(_) => {
                                record(ProvisionOutcome::TimedOut, None);
                                last_error = "provisioning timed out".into();
                                last_why = last_error.clone();
                                provisioned = None;
                                Next::Backoff
                            }
                            Ok(None) => {
                                record(ProvisionOutcome::Unavailable, None);
                                Next::Park(fallback)
                            }
                            Ok(Some(Ok(()))) => {
                                record(ProvisionOutcome::Done, None);
                                Next::Immediate
                            }
                            Ok(Some(Err(pe))) if pe.retryable => {
                                record(ProvisionOutcome::FailedRetryable, Some(&pe));
                                last_error = format!("provisioning failed: {}", pe.message);
                                last_why = last_error.clone();
                                provisioned = None;
                                Next::Backoff
                            }
                            Ok(Some(Err(pe))) => {
                                record(ProvisionOutcome::Failed, Some(&pe));
                                Next::Park(LinkState::Failed {
                                    reason: format!("provisioning failed: {}", pe.message),
                                    problem: None,
                                })
                            }
                        }
                    }
                }
            }
            other => other,
        };

        match next {
            Next::Immediate => continue,
            Next::Backoff => {
                // No "reconnecting at …" for a link that will not: it waits for a person, and
                // says why it was down.
                let held = tokio::select! {
                    _ = stop.cancelled() => break,
                    h = hold(&hooks) => h,
                };
                if let Some(detail) = held {
                    if !wait_for_person(
                        &shared,
                        &mut reconnect_rx,
                        detail,
                        &last_error,
                        &last_why,
                        n,
                    )
                    .await
                    {
                        break;
                    }
                    attempt = 0;
                    bridge_unavailable = 0;
                    provisioned = None;
                    continue 'outer;
                }
                attempt = attempt.saturating_add(1);
                let delay = shared.backoff.delay_random(attempt);
                shared.set(
                    LinkState::Reconnecting {
                        attempt,
                        next_at: SystemTime::now() + delay,
                        last_error: last_error.clone(),
                    },
                    Some(&last_why),
                    n,
                );
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                    _ = reconnect_rx.changed() => {}
                }
            }
            Next::Park(state) => {
                shared.set(state, park_why.take().as_deref(), n);
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = reconnect_rx.changed() => {
                        attempt = 0;
                        bridge_unavailable = 0;
                        provisioned = None;
                    }
                }
            }
            Next::Provision { .. } => unreachable!("resolved above"),
        }
    }
    shared.running.store(0, Ordering::SeqCst);
    shared.client.send_replace(None);
    shared.set(LinkState::Stopped, None, n);
}

/// Runs `f` to its result, or gives up on it after `limit` (`None`: no limit). `None` means it
/// timed out, and `f` has been dropped.
async fn bounded<T>(limit: Option<Duration>, f: impl std::future::Future<Output = T>) -> Option<T> {
    match limit {
        Some(d) => tokio::time::timeout(d, f).await.ok(),
        None => Some(f.await),
    }
}

async fn hold(hooks: &Option<Arc<dyn SupervisorHooks>>) -> Option<String> {
    match hooks {
        Some(h) => h.hold().await,
        None => None,
    }
}

/// Parks the link as waiting for a person ([`SupervisorHooks::hold`]) and waits for
/// `reconnect_now`. `cause` and `why` are the reason the link was down (in the supervisor's
/// words, and as diagnostics may show it); empty when it had not been. `false`: the supervisor
/// was stopped meanwhile.
async fn wait_for_person(
    shared: &Shared,
    reconnect_rx: &mut watch::Receiver<u64>,
    detail: String,
    cause: &str,
    why: &str,
    n: u64,
) -> bool {
    shared.set(
        LinkState::AuthNeeded {
            problem: CommandFailureKind::AuthNeeded,
            detail,
            cause: (!cause.is_empty()).then(|| cause.to_owned()),
            held: true,
        },
        (!why.is_empty()).then_some(why),
        n,
    );
    tokio::select! {
        _ = shared.stop.cancelled() => false,
        _ = reconnect_rx.changed() => true,
    }
}

/// Options of a durable subscription.
#[derive(Debug, Clone)]
pub struct DurableOpts {
    pub host: Option<HostId>,
    pub params: Value,
    /// Start after this cursor (`None` = from the beginning as the service defines it).
    pub from: Option<Cursor>,
    pub hops: u32,
    /// Bound on each (re)subscribe round trip.
    pub subscribe_deadline: Duration,
}

impl Default for DurableOpts {
    fn default() -> Self {
        DurableOpts {
            host: None,
            params: Value::Null,
            from: None,
            hops: 0,
            subscribe_deadline: Duration::from_secs(30),
        }
    }
}

/// How a durable subscription ended.
#[derive(Debug, Clone, PartialEq)]
pub enum DurableEnd {
    /// The topic ended.
    Ended,
    /// The service failed the subscription (non-retryable).
    Error(RpcError),
    /// The supervisor was stopped.
    Stopped,
}

/// Delivers each event of a topic exactly once and in order across reconnects and `Lagged`
/// closes, by resubscribing from the last delivered cursor (given a service that replays
/// correctly).
pub struct DurableSubscription {
    sup: Supervised,
    topic: String,
    opts: DurableOpts,
    last: Option<Cursor>,
    current: Option<Subscription>,
    end: Option<DurableEnd>,
    subscribes: u64,
    /// Route changes of the uplink this subscription's host is reached over, when given.
    route: Option<UplinkChanges>,
    /// Refused resubscribes in a row (the backoff schedule, when there is no route to watch).
    retry: u32,
}

/// Waits for the next route change, or never when there is no route source (or it is gone).
async fn route_changed(route: &mut Option<UplinkChanges>) -> bool {
    match route {
        Some(r) => r.changed().await,
        None => std::future::pending().await,
    }
}

impl DurableSubscription {
    /// Resubscribe on route changes (see [`Uplink::changes`]) rather than on a backoff schedule
    /// when the host refuses the subscription. Give it the route of the host this subscription
    /// is for: the link to the supervised peer alone cannot tell that the host is back.
    pub fn route_changes(mut self, changes: UplinkChanges) -> Self {
        self.route = Some(changes);
        self
    }

    /// Next event; `Some(Err)` once for a terminal service error; `None` when ended.
    pub async fn next(&mut self) -> Option<Result<(Cursor, Value), RpcError>> {
        if self.end.is_some() {
            return None;
        }
        loop {
            if self.current.is_none() {
                let Some(client) = self.sup.next_client().await else {
                    self.end = Some(DurableEnd::Stopped);
                    return None;
                };
                let mut opts = SubscribeOpts::new(self.opts.subscribe_deadline)
                    .params(self.opts.params.clone())
                    .from(self.last.clone());
                opts.host = self.opts.host.clone();
                opts.hops = self.opts.hops;
                // Watching from before the call: a change during it is not missed.
                let mut link = self.sup.state();
                match client.subscribe_with(&self.topic, opts).await {
                    Ok(s) => {
                        self.current = Some(s);
                        self.subscribes += 1;
                        self.retry = 0;
                    }
                    Err(e) if !e.is_retryable() => {
                        let e = e.into_rpc();
                        self.end = Some(DurableEnd::Error(e.clone()));
                        return Some(Err(e));
                    }
                    Err(e) => {
                        tracing::debug!(topic = %self.topic, error = %e, "resubscribe refused");
                        if self.route.is_none() {
                            // No route to watch, so nothing signals when the host is back: the
                            // only option is the backoff schedule. Callers that can see the route
                            // (`route_changes`) wait for it instead.
                            self.retry = self.retry.saturating_add(1);
                            let delay = self.sup.shared.backoff.delay_random(self.retry);
                            tokio::select! {
                                _ = self.sup.shared.stop.cancelled() => {}
                                _ = tokio::time::sleep(delay) => {}
                                _ = client.closed() => {}
                            }
                        } else {
                            // The host is unreachable: asking again before the link or the
                            // route changed would only poll it.
                            let route = &mut self.route;
                            let route_gone = tokio::select! {
                                _ = self.sup.shared.stop.cancelled() => false,
                                _ = link.changed() => false,
                                _ = client.closed() => false,
                                changed = route_changed(route) => !changed,
                            };
                            if route_gone {
                                // The route source is gone; nothing more will signal it.
                                self.route = None;
                            }
                        }
                        continue;
                    }
                }
            }
            let Some(sub) = self.current.as_mut() else {
                continue;
            };
            match sub.next().await {
                Some(item) => {
                    self.last = Some(item.0.clone());
                    return Some(Ok(item));
                }
                None => {
                    let reason = sub.close_reason().cloned().unwrap_or(CloseReason::HostLost);
                    self.current = None;
                    match reason {
                        CloseReason::Ended => {
                            self.end = Some(DurableEnd::Ended);
                            return None;
                        }
                        CloseReason::Error { error } => {
                            self.end = Some(DurableEnd::Error(error.clone()));
                            return Some(Err(error));
                        }
                        CloseReason::Lagged | CloseReason::HostLost => continue,
                    }
                }
            }
        }
    }

    pub fn last_cursor(&self) -> Option<&Cursor> {
        self.last.as_ref()
    }

    /// Number of successful (re)subscribes so far.
    pub fn subscribe_count(&self) -> u64 {
        self.subscribes
    }

    pub fn end_reason(&self) -> Option<&DurableEnd> {
        self.end.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_caps_and_jitters() {
        let b = Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: 0.5,
        };
        assert_eq!(b.delay(1, 0.0), Duration::from_millis(100));
        assert_eq!(b.delay(2, 0.0), Duration::from_millis(200));
        assert_eq!(b.delay(4, 0.0), Duration::from_millis(800));
        assert_eq!(b.delay(5, 0.0), Duration::from_secs(1));
        assert_eq!(b.delay(500, 0.0), Duration::from_secs(1));
        assert_eq!(b.delay(2, 1.0), Duration::from_millis(100));
        let d = b.delay(3, 0.5);
        assert!(d > Duration::from_millis(250) && d < Duration::from_millis(400));
    }
}
