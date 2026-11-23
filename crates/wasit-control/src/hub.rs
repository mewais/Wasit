//! Link supervisor + router (§4.1): the local daemon keeps one supervised link per remote host
//! and routes requests/subscriptions by `host`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use wasit_diag::Ident;

use crate::attempt::Attempt;
use crate::client::{ClientOptions, ControlClient};
use crate::command::{CommandSpec, CommandTransport};
use crate::connection::{ConnConfig, RequestOpts, SubscribeOpts};
use crate::diag;
use crate::error::{CallError, ConnectError};
use crate::service::{CallContext, EventStream, Service};
use crate::supervised::{
    connector_reporting, LinkState, ProvisionError, ProvisionReason, Supervised, SupervisedConfig,
    SupervisedOptions, SupervisorHooks,
};
use crate::wire::{codes, Cursor, Hello, HostId, RpcError};

/// How to reach a remote host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSpec {
    pub id: HostId,
    /// e.g. `["ssh", "-o", "BatchMode=yes", "host", "marsad-agent", "bridge"]`.
    pub connect_argv: Vec<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    /// Build the remote helper must run; `None` = the hub's own build (when
    /// [`HubConfig::require_same_build`]).
    #[serde(default)]
    pub expected_build: Option<String>,
}

impl HostSpec {
    pub fn new<I, S>(id: impl Into<HostId>, argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        HostSpec {
            id: id.into(),
            connect_argv: argv.into_iter().map(Into::into).collect(),
            env: Vec::new(),
            expected_build: None,
        }
    }
}

impl HostSpec {
    /// The label of this host's link in diagnostics (`hosts:<id>`); transports started for it
    /// are traced under it ([`CommandSpec::traced`]).
    pub fn diag_link(&self) -> Ident {
        diag::host_link(&self.id)
    }
}

/// Installs/updates the helper on a host. Implemented by the product (copy over ssh etc.).
#[async_trait]
pub trait Provisioner: Send + Sync + 'static {
    async fn ensure(&self, host: &HostSpec, problem: ProvisionReason)
        -> Result<(), ProvisionError>;
}

/// Opens one connection to a host. The default is [`CommandConnector`].
#[async_trait]
pub trait LinkConnector: Send + Sync + 'static {
    /// `attempt` is the link's attempt ([`Attempt::n`]: 1 for the first, +1 per attempt, never
    /// reset): what the connector records about this attempt carries its number, so it joins
    /// the supervisor's records of the same attempt. A connector that starts a transport
    /// command traces it ([`CommandSpec::traced`]) and says which step it has reached
    /// ([`Attempt::reached`]); the supervisor reports both if it has to end the attempt.
    async fn connect(
        &self,
        spec: &HostSpec,
        opts: ClientOptions,
        attempt: Attempt,
    ) -> Result<ControlClient, ConnectError>;

    /// Whether the link to `spec` must wait for a person before its next attempt
    /// ([`SupervisorHooks::hold`]): `Some(detail)` parks it, saying why in `detail`. The
    /// default never holds.
    fn hold(&self, _spec: &HostSpec) -> Option<String> {
        None
    }
}

/// Connects by running `spec.connect_argv` and speaking over its stdio.
#[derive(Debug, Clone, Default)]
pub struct CommandConnector;

#[async_trait]
impl LinkConnector for CommandConnector {
    async fn connect(
        &self,
        spec: &HostSpec,
        opts: ClientOptions,
        attempt: Attempt,
    ) -> Result<ControlClient, ConnectError> {
        let mut cmd =
            CommandSpec::new(spec.connect_argv.clone()).traced(spec.diag_link(), &attempt);
        cmd.env = spec.env.clone();
        CommandTransport::connect(&cmd, opts).await
    }
}

#[derive(Debug, Clone)]
pub struct HubConfig {
    /// Hello the hub sends on its links (its `host_id` is the local host id).
    pub hello: Hello,
    pub conn: ConnConfig,
    pub supervise: SupervisedConfig,
    /// Treat a remote helper with a different build as needing provisioning (only when a
    /// provisioner is installed).
    pub require_same_build: bool,
    /// Requests that have already been forwarded this many times are rejected.
    pub max_hops: u32,
    /// Bound on a forwarded request (and on a forwarded subscribe round trip).
    pub forward_timeout: Duration,
}

impl HubConfig {
    pub fn new(hello: Hello) -> Self {
        HubConfig {
            hello,
            conn: ConnConfig::default(),
            supervise: SupervisedConfig::default(),
            require_same_build: true,
            max_hops: 4,
            forward_timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HubError {
    #[error("link to {0} already exists")]
    AlreadyExists(HostId),
    #[error("{0} is the local host")]
    LocalHost(HostId),
}

/// One link state change.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LinkEvent {
    pub host: HostId,
    /// `None` = link removed.
    pub state: Option<LinkState>,
}

struct LinkEntry {
    spec: HostSpec,
    sup: Supervised,
}

type InboundSlot = Arc<RwLock<Option<Arc<dyn Service>>>>;

struct HubInner {
    cfg: HubConfig,
    connector: Arc<dyn LinkConnector>,
    provisioner: Option<Arc<dyn Provisioner>>,
    links: Mutex<HashMap<HostId, LinkEntry>>,
    states: watch::Sender<BTreeMap<HostId, LinkState>>,
    events: broadcast::Sender<LinkEvent>,
    inbound: InboundSlot,
}

/// Manages named links to remote hosts.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<HubInner>,
}

struct HubHooks {
    spec: HostSpec,
    provisioner: Option<Arc<dyn Provisioner>>,
    connector: Arc<dyn LinkConnector>,
    expected_build: Option<String>,
}

#[async_trait]
impl SupervisorHooks for HubHooks {
    fn check_hello(&self, hello: &Hello) -> Option<ProvisionReason> {
        // Without a provisioner nothing could fix a mismatch, so accept the peer.
        self.provisioner.as_ref()?;
        let expected = self.expected_build.as_ref()?;
        (hello.build != *expected).then(|| ProvisionReason::BuildMismatch {
            expected: expected.clone(),
            found: hello.build.clone(),
        })
    }

    async fn provision(&self, reason: &ProvisionReason) -> Option<Result<(), ProvisionError>> {
        let p = self.provisioner.as_ref()?;
        Some(p.ensure(&self.spec, reason.clone()).await)
    }

    fn hold(&self) -> Option<String> {
        self.connector.hold(&self.spec)
    }
}

/// Forwards inbound link requests to whatever service is installed in the hub.
struct InboundProxy {
    slot: InboundSlot,
}

impl InboundProxy {
    fn get(&self) -> Result<Arc<dyn Service>, RpcError> {
        self.slot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| RpcError::new(codes::SHUTTING_DOWN, "hub serves no inbound requests"))
    }
}

#[async_trait]
impl Service for InboundProxy {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        self.get()?.call(ctx, method, params).await
    }

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        self.get()?.subscribe(ctx, topic, params, from).await
    }
}

impl Hub {
    /// `connector`: how links are opened (normally [`CommandConnector`]).
    pub fn new(
        cfg: HubConfig,
        connector: Arc<dyn LinkConnector>,
        provisioner: Option<Arc<dyn Provisioner>>,
    ) -> Self {
        Hub {
            inner: Arc::new(HubInner {
                cfg,
                connector,
                provisioner,
                links: Mutex::new(HashMap::new()),
                states: watch::channel(BTreeMap::new()).0,
                events: broadcast::channel(256).0,
                inbound: Arc::new(RwLock::new(None)),
            }),
        }
    }

    pub fn local_host(&self) -> &HostId {
        &self.inner.cfg.hello.host_id
    }

    pub fn config(&self) -> &HubConfig {
        &self.inner.cfg
    }

    /// Service that answers requests remote daemons send to the hub over their links
    /// (normally the hub's [`RoutingService`], which enables remote→hub→remote routing).
    /// Cleared by [`Hub::shutdown`].
    pub fn set_inbound_service(&self, service: Arc<dyn Service>) {
        *self
            .inner
            .inbound
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(service);
    }

    /// Start supervising a link.
    pub fn add_link(&self, spec: HostSpec) -> Result<(), HubError> {
        if spec.id == *self.local_host() {
            return Err(HubError::LocalHost(spec.id));
        }
        let mut links = self.inner.links.lock().unwrap_or_else(|e| e.into_inner());
        if links.contains_key(&spec.id) {
            return Err(HubError::AlreadyExists(spec.id));
        }
        let weak = Arc::downgrade(&self.inner);
        let host = spec.id.clone();
        let on_state = Arc::new(move |s: &LinkState| {
            if let Some(inner) = weak.upgrade() {
                inner.states.send_modify(|m| {
                    m.insert(host.clone(), s.clone());
                });
                // No receivers is fine.
                let _ = inner.events.send(LinkEvent {
                    host: host.clone(),
                    state: Some(s.clone()),
                });
            }
        });
        let link_connector = self.inner.connector.clone();
        let opts = ClientOptions {
            hello: self.inner.cfg.hello.clone(),
            service: Some(Arc::new(InboundProxy {
                slot: self.inner.inbound.clone(),
            })),
            config: self.inner.cfg.conn.clone(),
        };
        let spec2 = spec.clone();
        let conn = connector_reporting(move |attempt| {
            let c = link_connector.clone();
            let spec = spec2.clone();
            let opts = opts.clone();
            async move { c.connect(&spec, opts, attempt).await }
        });
        let hooks = HubHooks {
            spec: spec.clone(),
            provisioner: self.inner.provisioner.clone(),
            connector: self.inner.connector.clone(),
            expected_build: spec.expected_build.clone().or_else(|| {
                self.inner
                    .cfg
                    .require_same_build
                    .then(|| self.inner.cfg.hello.build.clone())
            }),
        };
        let sup = Supervised::spawn(
            conn,
            SupervisedOptions {
                config: self.inner.cfg.supervise.clone(),
                hooks: Some(Arc::new(hooks)),
                on_state: Some(on_state),
                diag: Some(spec.diag_link()),
                diag_host: Some(Ident::new(spec.id.as_str())),
            },
        );
        links.insert(spec.id.clone(), LinkEntry { spec, sup });
        Ok(())
    }

    /// Stop and forget a link. Returns whether it existed.
    pub async fn remove_link(&self, id: &HostId) -> bool {
        let entry = self
            .inner
            .links
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        let Some(entry) = entry else {
            return false;
        };
        entry.sup.stop().await;
        self.inner.states.send_modify(|m| {
            m.remove(id);
        });
        // No receivers is fine.
        let _ = self.inner.events.send(LinkEvent {
            host: id.clone(),
            state: None,
        });
        true
    }

    /// Skip backoff / leave a parked state and connect again now.
    pub fn reconnect_now(&self, id: &HostId) -> bool {
        match self.link(id) {
            Some(s) => {
                s.reconnect_now();
                true
            }
            None => false,
        }
    }

    pub fn link(&self, id: &HostId) -> Option<Supervised> {
        self.inner
            .links
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .map(|e| e.sup.clone())
    }

    pub fn spec(&self, id: &HostId) -> Option<HostSpec> {
        self.inner
            .links
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .map(|e| e.spec.clone())
    }

    pub fn link_ids(&self) -> Vec<HostId> {
        let mut v: Vec<HostId> = self
            .inner
            .links
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        v.sort();
        v
    }

    /// Snapshot of all link states (latest value; may skip transitions).
    pub fn states(&self) -> watch::Receiver<BTreeMap<HostId, LinkState>> {
        self.inner.states.subscribe()
    }

    /// Every link state transition. A lagging receiver should re-read [`Hub::states`].
    pub fn events(&self) -> broadcast::Receiver<LinkEvent> {
        self.inner.events.subscribe()
    }

    /// Stop all links and clear the inbound service.
    pub async fn shutdown(&self) {
        *self
            .inner
            .inbound
            .write()
            .unwrap_or_else(|e| e.into_inner()) = None;
        for id in self.link_ids() {
            self.remove_link(&id).await;
        }
    }

    fn route(&self, host: &Option<HostId>, hops: u32) -> Result<Option<ControlClient>, RpcError> {
        let Some(h) = host else {
            return Ok(None);
        };
        if h == self.local_host() {
            return Ok(None);
        }
        if hops >= self.inner.cfg.max_hops {
            return Err(RpcError::new(
                codes::HOP_LIMIT,
                format!("request for {h} exceeded {} hops", self.inner.cfg.max_hops),
            ));
        }
        let Some(sup) = self.link(h) else {
            return Err(RpcError::new(
                codes::HOST_UNKNOWN,
                format!("no link to host {h}"),
            ));
        };
        match sup.current() {
            Some(c) => Ok(Some(c)),
            None => Err(RpcError::new(
                codes::HOST_UNREACHABLE,
                format!("host {h} is not connected"),
            )
            .with_data(serde_json::to_value(sup.current_state()).unwrap_or(Value::Null))),
        }
    }
}

/// `Service` that serves requests for the local host from `local` and forwards everything
/// addressed to another host over the hub's link to it (cursor passthrough for
/// subscriptions; a dropped link closes downstream subscriptions with `HostLost`).
pub struct RoutingService {
    hub: Hub,
    local: Arc<dyn Service>,
}

impl RoutingService {
    pub fn new(hub: Hub, local: Arc<dyn Service>) -> Self {
        RoutingService { hub, local }
    }
}

/// The error a forwarded request returns. A failure of the forwarding itself (the link is gone,
/// the deadline passed) is also handed to `record`; the remote service answering with an error
/// is not a forwarding failure.
fn forwarded_error(e: CallError, record: impl FnOnce(&str)) -> RpcError {
    let forwarding_failed = !matches!(e, CallError::Rpc(_));
    let rpc = e.into_rpc();
    if forwarding_failed {
        record(&rpc.code);
    }
    rpc
}

#[async_trait]
impl Service for RoutingService {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        let started = Instant::now();
        let routed = self.hub.route(&ctx.host, ctx.hops).inspect_err(|e| {
            if let Some(host) = &ctx.host {
                diag::forward_failed(method, host, &e.code, started);
            }
        })?;
        match routed {
            None => self.local.call(ctx, method, params).await,
            Some(link) => {
                let opts = RequestOpts {
                    host: ctx.host.clone(),
                    hops: ctx.hops + 1,
                    deadline: self.hub.inner.cfg.forward_timeout,
                    // The next hop records the same intent.
                    trace: ctx.trace.clone(),
                };
                link.call(method, params, opts).await.map_err(|e| {
                    forwarded_error(e, |code| {
                        if let Some(host) = &ctx.host {
                            diag::forward_failed(method, host, code, started);
                        }
                    })
                })
            }
        }
    }

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        let started = Instant::now();
        let routed = self.hub.route(&ctx.host, ctx.hops).inspect_err(|e| {
            if let Some(host) = &ctx.host {
                diag::forward_subscribe_failed(topic, host, &e.code, started);
            }
        })?;
        match routed {
            None => self.local.subscribe(ctx, topic, params, from).await,
            Some(link) => {
                let mut opts = SubscribeOpts::new(self.hub.inner.cfg.forward_timeout)
                    .params(params)
                    .from(from);
                opts.host = ctx.host.clone();
                opts.hops = ctx.hops + 1;
                let sub = link.subscribe_with(topic, opts).await.map_err(|e| {
                    forwarded_error(e, |code| {
                        if let Some(host) = &ctx.host {
                            diag::forward_subscribe_failed(topic, host, code, started);
                        }
                    })
                })?;
                Ok(sub.into_event_stream())
            }
        }
    }
}
