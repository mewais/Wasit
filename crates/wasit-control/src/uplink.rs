//! [`Uplink`]: how a daemon reaches *other* hosts — the path to the hub.
//!
//! In the hub topology (§4.1) every remote daemon is the *server* end of the hub's link to it
//! (the hub runs `… bridge` and speaks over its stdio), and the hub's daemon routes requests by
//! `host`. A service on a remote daemon that wants to reach another host therefore sends a
//! request addressed to that host over the connection the hub opened ("the uplink"); the hub's
//! [`crate::RoutingService`] forwards it. On the hub's own daemon, the same request goes out
//! over the hub's link to that host.
//!
//! An `Uplink` hides that difference: [`Uplink::via_server`] picks the newest live connection
//! of a [`ControlServer`] whose peer announced a given role (normally `"hub"`), and
//! [`Uplink::via_hub`] uses a [`Hub`]'s link to the target host. [`Uplink::changes`] signals
//! whenever the route may have changed (a connection came or went, a link changed state), so
//! store-and-forward users can retry at once instead of polling.

use std::time::Duration;

use serde_json::Value;
use tokio::sync::watch;

use crate::connection::{Connection, RequestOpts};
use crate::error::CallError;
use crate::hub::Hub;
use crate::server::ControlServer;
use crate::wire::HostId;

#[derive(Clone)]
enum Kind {
    Server { server: ControlServer, role: String },
    Hub(Hub),
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct Uplink {
    kind: Kind,
}

impl std::fmt::Debug for Uplink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            Kind::Server { role, .. } => write!(f, "Uplink::via_server({role:?})"),
            Kind::Hub(h) => write!(f, "Uplink::via_hub({})", h.local_host()),
        }
    }
}

impl Uplink {
    /// Requests leave on the newest live connection of `server` whose peer's `Hello.role` is
    /// `role` (e.g. the hub's link into this daemon).
    pub fn via_server(server: ControlServer, role: impl Into<String>) -> Self {
        Uplink {
            kind: Kind::Server {
                server,
                role: role.into(),
            },
        }
    }

    /// Requests leave on `hub`'s link to the target host (this daemon *is* the hub).
    pub fn via_hub(hub: Hub) -> Self {
        Uplink {
            kind: Kind::Hub(hub),
        }
    }

    /// The connection a request for `host` would leave on right now.
    pub fn route(&self, host: &HostId) -> Option<Connection> {
        match &self.kind {
            Kind::Server { server, role } => server
                .connections()
                .into_iter()
                .filter(|c| !c.is_closed() && c.remote_hello().role == *role)
                .max_by_key(|c| c.id()),
            Kind::Hub(hub) => hub
                .link(host)
                .and_then(|l| l.current())
                .map(|c| c.connection().clone()),
        }
    }

    /// Whether a route to `host` exists right now (it may still fail downstream, e.g. when the
    /// hub has no live link to that host).
    pub fn is_up(&self, host: &HostId) -> bool {
        self.route(host).is_some()
    }

    /// Send `method` to `host` (bounded by `deadline`). [`CallError::NotConnected`] when there
    /// is no route; errors from the hub (`host_unknown`, `host_unreachable`, …) come back as
    /// [`CallError::Rpc`].
    pub async fn call(
        &self,
        host: &HostId,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<Value, CallError> {
        let conn = self.route(host).ok_or(CallError::NotConnected)?;
        let opts = RequestOpts {
            host: Some(host.clone()),
            hops: 0,
            deadline,
        };
        conn.call(method, params, opts).await
    }

    /// A signal that fires whenever the route to some host may have changed.
    pub fn changes(&self) -> UplinkChanges {
        match &self.kind {
            Kind::Server { server, .. } => UplinkChanges::Server(server.connection_changes()),
            Kind::Hub(hub) => UplinkChanges::Hub(hub.states()),
        }
    }
}

/// See [`Uplink::changes`].
pub enum UplinkChanges {
    Server(watch::Receiver<u64>),
    Hub(watch::Receiver<std::collections::BTreeMap<HostId, crate::supervised::LinkState>>),
}

impl UplinkChanges {
    /// Wait for the next change. `false` once the source is gone (server/hub dropped).
    pub async fn changed(&mut self) -> bool {
        match self {
            UplinkChanges::Server(rx) => rx.changed().await.is_ok(),
            UplinkChanges::Hub(rx) => rx.changed().await.is_ok(),
        }
    }
}
