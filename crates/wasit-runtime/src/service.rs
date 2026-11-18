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

use crate::discovery::Discovery;
use crate::manager::SessionManager;
use crate::store::now_ms;
use crate::stream::StreamEnd;
use crate::wire::{
    methods, topics, Ack, CommandError, CommandReply, CommandRequest, CreateSession, DriverSummary,
    HistoryRequest, HostInfo, InventoryRequest, RefreshRequest, RuntimeError, SessionId,
    SessionList, SessionRef,
};

/// See the module docs.
#[derive(Clone)]
pub struct RuntimeService {
    manager: SessionManager,
    discovery: Discovery,
    info: HostInfo,
}

fn params<T: DeserializeOwned>(v: Value) -> Result<T, RpcError> {
    serde_json::from_value(v).map_err(|e| RpcError::invalid_params(e.to_string()))
}

fn ok<T: Serialize>(v: T) -> Result<Value, RpcError> {
    serde_json::to_value(v).map_err(|e| RpcError::internal(format!("cannot encode result: {e}")))
}

fn runtime_err(e: RuntimeError) -> RpcError {
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
        }
    }

    pub fn manager(&self) -> &SessionManager {
        &self.manager
    }

    pub fn discovery(&self) -> &Discovery {
        &self.discovery
    }
}

#[async_trait]
impl Service for RuntimeService {
    async fn call(&self, _ctx: CallContext, method: &str, p: Value) -> Result<Value, RpcError> {
        let m = &self.manager;
        match method {
            methods::HOST_INFO => ok(&self.info),
            methods::DRIVERS_INVENTORY => {
                let r: InventoryRequest = if p.is_null() {
                    InventoryRequest::default()
                } else {
                    params(p)?
                };
                ok(self.discovery.inventory(r.project.as_deref()))
            }
            methods::DRIVERS_REFRESH => {
                let r: RefreshRequest = if p.is_null() {
                    RefreshRequest::default()
                } else {
                    params(p)?
                };
                self.discovery
                    .refresh(r.driver.as_ref(), r.project.as_deref())
                    .map_err(|e| runtime_err(RuntimeError::UnknownDriver(e)))?;
                ok(Ack {})
            }
            methods::SESSIONS_LIST => ok(SessionList { sessions: m.list() }),
            methods::SESSIONS_CREATE => {
                let r: CreateSession = params(p)?;
                ok(m.create(r).await.map_err(runtime_err)?)
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
            methods::SESSION_COMMAND => {
                let r: CommandRequest = params(p)?;
                let outcome = m.command(&r.id, r.command).await.map_err(command_err)?;
                ok(CommandReply { outcome })
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
            let s = self
                .discovery
                .subscribe(r.project, from.as_ref().map(Cursor::as_str));
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
            let id = SessionId::parse(rest)
                .ok_or_else(|| RpcError::invalid_params(format!("bad session id {rest:?}")))?;
            let from = match &from {
                None => None,
                Some(c) => Some(
                    c.as_u64()
                        .ok_or_else(|| RpcError::invalid_params(format!("bad cursor {c}")))?,
                ),
            };
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
        Err(RpcError::topic_not_found(topic))
    }
}
