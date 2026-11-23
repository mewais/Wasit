//! [`RuntimeService`]: the runtime's methods and topics as a [`wasit_control::Service`].
//!
//! See [`crate::wire`] for the method/topic table and payload types. The service only serves
//! the routes in [`crate::wire::ROUTES`], so it composes with other services in a
//! [`wasit_control::ServiceMux`].

use async_trait::async_trait;
use futures::stream::StreamExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use wasit_control::{CallContext, CloseReason, Cursor, EventStream, RpcError, Service};

use crate::trail;

use crate::discovery::Discovery;
use crate::manager::SessionManager;
use crate::store::now_ms;
use crate::stream::StreamEnd;
use crate::wire::{
    methods, topics, Ack, CommandError, CommandReply, CommandRequest, ConfigureEnvironment,
    CreateSession, DriverSummary, EnvironmentTest, HistoryRequest, HostInfo, InventoryRequest,
    RefreshRequest, RenameSession, RuntimeError, SessionId, SessionList, SessionRef, TrailRequest,
};

/// How long `host.environment.test` waits for discovery after the setup ran (a test then
/// answers within the setup deadline plus this, well inside a client's request deadline).
const TEST_DISCOVERY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(50);

/// See the module docs.
#[derive(Clone)]
pub struct RuntimeService {
    manager: SessionManager,
    discovery: Discovery,
    info: HostInfo,
    env: Option<crate::environment::HostEnv>,
}

pub(crate) fn params<T: DeserializeOwned>(v: Value) -> Result<T, RpcError> {
    serde_json::from_value(v).map_err(|e| RpcError::invalid_params(e.to_string()))
}

pub(crate) fn ok<T: Serialize>(v: T) -> Result<Value, RpcError> {
    serde_json::to_value(v).map_err(|e| RpcError::internal(format!("cannot encode result: {e}")))
}

pub(crate) fn runtime_err(e: RuntimeError) -> RpcError {
    let data = serde_json::to_value(&e).ok();
    let mut r = RpcError::new(e.code(), e.to_string());
    r.data = data;
    r
}

fn command_err(e: CommandError) -> RpcError {
    let data = serde_json::to_value(&e).ok();
    let mut r = RpcError::new(e.code(), e.to_string());
    r.data = data;
    r
}

fn close_reason(e: StreamEnd) -> CloseReason {
    match e {
        StreamEnd::Lagged => CloseReason::Lagged,
        StreamEnd::Error(m) => CloseReason::Error {
            error: RpcError::internal(m),
        },
    }
}

/// The session and the cursor a `session.events:<id>` / `session.trail:<id>` subscription names.
fn session_topic(rest: &str, from: Option<&Cursor>) -> Result<(SessionId, Option<u64>), RpcError> {
    let id = SessionId::parse(rest)
        .ok_or_else(|| RpcError::invalid_params(format!("bad session id {rest:?}")))?;
    let from = match from {
        None => None,
        Some(c) => Some(
            c.as_u64()
                .ok_or_else(|| RpcError::invalid_params(format!("bad cursor {c}")))?,
        ),
    };
    Ok((id, from))
}

impl RuntimeService {
    pub fn new(
        manager: SessionManager,
        discovery: Discovery,
        host_id: impl Into<String>,
        build: impl Into<String>,
    ) -> Self {
        let cfg = manager.config();
        let drivers = cfg
            .registry
            .iter()
            .map(|d| DriverSummary {
                id: d.id(),
                display_name: d.display_name().to_string(),
                capabilities: d.capabilities(),
                prompt_input: d.prompt_input(),
            })
            .collect();
        let info = HostInfo {
            host_id: host_id.into(),
            build: build.into(),
            pid: std::process::id(),
            started_at_ms: now_ms(),
            data_dir: cfg.data_dir.to_string_lossy().into_owned(),
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            drivers,
        };
        RuntimeService {
            manager,
            discovery,
            info,
            env: None,
        }
    }

    /// Serve `host.environment[.refresh]` from this environment stage.
    pub fn with_environment(mut self, env: crate::environment::HostEnv) -> Self {
        self.env = Some(env);
        self
    }

    fn env(&self) -> Result<&crate::environment::HostEnv, RpcError> {
        self.env.as_ref().ok_or_else(|| {
            RpcError::new(
                wasit_control::codes::METHOD_NOT_FOUND,
                "this host has no environment stage",
            )
        })
    }

    /// A valid configuration whose agent commands name known drivers. Errors name variables
    /// and drivers, never values.
    fn configuration(&self, p: Value) -> Result<ConfigureEnvironment, RpcError> {
        // Not `params`: a decoding error could quote a value.
        let r: ConfigureEnvironment = serde_json::from_value(p)
            .map_err(|_| RpcError::invalid_params("not an environment configuration"))?;
        r.config.validate().map_err(RpcError::invalid_params)?;
        let reg = &self.manager.config().registry;
        for d in r.config.commands.keys() {
            reg.get(d)
                .map_err(|e| runtime_err(RuntimeError::UnknownDriver(e.to_string())))?;
        }
        Ok(r)
    }

    pub fn manager(&self) -> &SessionManager {
        &self.manager
    }

    /// The project folder a discovery request names, as a session started there would see it
    /// ([`crate::cwd`]): the inventory a client lists resumable sessions from and the session it
    /// launches then agree on the folder. One that cannot be resolved is passed on as given:
    /// discovery reports why in the inventory's `error`.
    async fn project(&self, given: Option<String>) -> Option<String> {
        let home = self.manager.config().home.as_deref();
        match given {
            Some(p) => Some(crate::cwd::resolve(&p, home).await.unwrap_or(p)),
            None => None,
        }
    }

    pub fn discovery(&self) -> &Discovery {
        &self.discovery
    }
}

#[async_trait]
impl Service for RuntimeService {
    async fn call(&self, ctx: CallContext, method: &str, p: Value) -> Result<Value, RpcError> {
        let m = &self.manager;
        match method {
            methods::HOST_INFO => ok(&self.info),
            methods::HOST_ENVIRONMENT => ok(self.env.as_ref().and_then(|e| e.report())),
            methods::HOST_ENVIRONMENT_REFRESH => {
                let report = self.env()?.refresh().await;
                // Agents found (or not) with the old environment are detected again.
                self.discovery.refresh_all();
                ok(report)
            }
            methods::HOST_ENVIRONMENT_CONFIGURE => {
                let r = self.configuration(p)?;
                let (report, changed) = self.env()?.configure(r.config, r.revision, false).await;
                if changed {
                    self.discovery.refresh_all();
                }
                ok(report)
            }
            methods::HOST_ENVIRONMENT_TEST => {
                let r = self.configuration(p)?;
                let (environment, _) = self.env()?.configure(r.config, r.revision, true).await;
                self.discovery.refresh_all();
                let (inv, complete) = self.discovery.settled(None, TEST_DISCOVERY_DEADLINE).await;
                ok(EnvironmentTest {
                    environment,
                    agents: inv.drivers,
                    complete,
                })
            }
            methods::DRIVERS_INVENTORY => {
                let r: InventoryRequest = if p.is_null() {
                    InventoryRequest::default()
                } else {
                    params(p)?
                };
                let project = self.project(r.project).await;
                ok(self.discovery.inventory(project.as_deref()))
            }
            methods::DRIVERS_REFRESH => {
                let r: RefreshRequest = if p.is_null() {
                    RefreshRequest::default()
                } else {
                    params(p)?
                };
                let project = self.project(r.project).await;
                self.discovery
                    .refresh(r.driver.as_ref(), project.as_deref())
                    .map_err(|e| runtime_err(RuntimeError::UnknownDriver(e)))?;
                ok(Ack {})
            }
            methods::SESSIONS_LIST => ok(SessionList { sessions: m.list() }),
            methods::SESSIONS_CREATE => {
                let r: CreateSession = params(p)?;
                if let Some(env) = &self.env {
                    env.register_secrets();
                }
                ok(m.create_traced(r, ctx.trace).await.map_err(runtime_err)?)
            }
            methods::SESSIONS_CONTINUE => {
                let r: SessionRef = params(p)?;
                m.continue_session(&r.id).await.map_err(runtime_err)?;
                ok(Ack {})
            }
            methods::SESSIONS_STOP => {
                let r: SessionRef = params(p)?;
                m.stop(&r.id).await.map_err(runtime_err)?;
                ok(Ack {})
            }
            methods::SESSIONS_DELETE => {
                let r: SessionRef = params(p)?;
                m.delete(&r.id).await.map_err(runtime_err)?;
                ok(Ack {})
            }
            methods::SESSIONS_RENAME => {
                let r: RenameSession = params(p)?;
                m.rename(&r.id, r.display_name).await.map_err(runtime_err)?;
                ok(Ack {})
            }
            methods::SESSION_SNAPSHOT => {
                let r: SessionRef = params(p)?;
                ok(m.snapshot(&r.id).map_err(runtime_err)?)
            }
            methods::SESSION_HISTORY => {
                let r: HistoryRequest = params(p)?;
                ok(m.history(&r.id, r.before, r.limit as usize)
                    .await
                    .map_err(runtime_err)?)
            }
            methods::SESSION_TRAIL_PAGE => {
                let r: TrailRequest = params(p)?;
                ok(m.trail_page(&r.id, r.before, r.limit as usize)
                    .await
                    .map_err(runtime_err)?)
            }
            methods::SESSION_COMMAND => {
                let r: CommandRequest = params(p)?;
                let outcome = m
                    .command_traced(&r.id, r.command, ctx.trace)
                    .await
                    .map_err(command_err)?;
                ok(CommandReply { outcome })
            }
            methods::SESSION_ATTACHMENT_PUT => {
                let r: crate::media::AttachmentPut = params(p)?;
                ok(m.attachment_put(&r).await.map_err(runtime_err)?)
            }
            methods::SESSION_FILE_READ => {
                let r: crate::media::FileReadRequest = params(p)?;
                ok(m.file_read(&r).await.map_err(runtime_err)?)
            }
            methods::SESSION_FILES_STAT => {
                let r: crate::media::FilesStatRequest = params(p)?;
                ok(m.files_stat(&r).await.map_err(runtime_err)?)
            }
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
        if topic == topics::HOST_SESSIONS {
            let s = self
                .manager
                .subscribe_host(from.as_ref().map(Cursor::as_str));
            return Ok(s
                .map(|item| match item {
                    Ok((c, ev)) => serde_json::to_value(ev)
                        .map(|v| (Cursor::new(c), v))
                        .map_err(|e| close_reason(StreamEnd::Error(e.to_string()))),
                    Err(e) => Err(close_reason(e)),
                })
                .boxed());
        }
        if topic == topics::DRIVERS_INVENTORY {
            let r: InventoryRequest = if p.is_null() {
                InventoryRequest::default()
            } else {
                params(p)?
            };
            let project = self.project(r.project).await;
            let s = self
                .discovery
                .subscribe(project, from.as_ref().map(Cursor::as_str));
            return Ok(s
                .map(|item| match item {
                    Ok((c, inv)) => serde_json::to_value(inv)
                        .map(|v| (Cursor::new(c), v))
                        .map_err(|e| close_reason(StreamEnd::Error(e.to_string()))),
                    Err(e) => Err(close_reason(e)),
                })
                .boxed());
        }
        if let Some(rest) = topic.strip_prefix(topics::SESSION_EVENTS_PREFIX) {
            let (id, from) = session_topic(rest, from.as_ref())?;
            let s = self
                .manager
                .subscribe(&id, from)
                .await
                .map_err(runtime_err)?;
            return Ok(s
                .map(|item| match item {
                    Ok(ev) => serde_json::to_value(&ev)
                        .map(|v| (Cursor::from_u64(ev.cursor), v))
                        .map_err(|e| close_reason(StreamEnd::Error(e.to_string()))),
                    Err(e) => Err(close_reason(e)),
                })
                .boxed());
        }
        if let Some(rest) = topic.strip_prefix(topics::SESSION_TRAIL_PREFIX) {
            // The session's events, projected: the same stream (so the same cursors, replay and
            // lag handling as `session.events:`), the events that are not part of the trail
            // left out.
            let (id, from) = session_topic(rest, from.as_ref())?;
            let s = self
                .manager
                .subscribe(&id, from)
                .await
                .map_err(runtime_err)?;
            return Ok(s
                .filter_map(|item| {
                    futures::future::ready(match item {
                        Ok(ev) => trail::item(&ev).map(|it| {
                            serde_json::to_value(&it)
                                .map(|v| (Cursor::from_u64(ev.cursor), v))
                                .map_err(|e| close_reason(StreamEnd::Error(e.to_string())))
                        }),
                        Err(e) => Some(Err(close_reason(e))),
                    })
                })
                .boxed());
        }
        Err(RpcError::topic_not_found(topic))
    }
}
