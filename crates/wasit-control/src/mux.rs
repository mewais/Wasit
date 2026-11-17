//! [`ServiceMux`]: compose several [`Service`]s into one by method / topic name.
//!
//! Each service is registered under one or more *routes*. A route is either an exact name
//! (`"host.info"`) or a prefix ending in `.` or `:` (`"sessions."`, `"session.events:"`). A
//! request or subscription goes to the service with the longest matching route, so a product
//! daemon can serve the runtime's namespaces next to its own (`"git."`, `"terminal."`, …) and
//! even override a single method of a namespace with an exact route.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::service::{CallContext, EventStream, Service};
use crate::wire::{Cursor, RpcError};

/// Error registering a route.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MuxError {
    #[error("route `{0}` is already registered")]
    Duplicate(String),
    #[error("empty route")]
    Empty,
}

/// A prefix/exact-name router over services. See the module docs.
#[derive(Clone, Default)]
pub struct ServiceMux {
    routes: Vec<(String, Arc<dyn Service>)>,
}

impl ServiceMux {
    pub fn new() -> Self {
        ServiceMux::default()
    }

    /// Register `service` for every route in `routes`.
    pub fn register<I, S>(&mut self, routes: I, service: Arc<dyn Service>) -> Result<(), MuxError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for r in routes {
            let r = r.into();
            if r.is_empty() {
                return Err(MuxError::Empty);
            }
            if self.routes.iter().any(|(x, _)| *x == r) {
                return Err(MuxError::Duplicate(r));
            }
            self.routes.push((r, service.clone()));
        }
        Ok(())
    }

    /// Builder form of [`ServiceMux::register`].
    pub fn with<I, S>(mut self, routes: I, service: Arc<dyn Service>) -> Result<Self, MuxError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.register(routes, service)?;
        Ok(self)
    }

    /// The service a name routes to (longest matching route).
    pub fn route(&self, name: &str) -> Option<&Arc<dyn Service>> {
        self.routes
            .iter()
            .filter(|(r, _)| {
                if r.ends_with('.') || r.ends_with(':') {
                    name.starts_with(r.as_str())
                } else {
                    name == r
                }
            })
            .max_by_key(|(r, _)| r.len())
            .map(|(_, s)| s)
    }
}

#[async_trait]
impl Service for ServiceMux {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        match self.route(method) {
            Some(s) => s.call(ctx, method, params).await,
            None => Err(RpcError::method_not_found(method)),
        }
    }

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        match self.route(topic) {
            Some(s) => s.subscribe(ctx, topic, params, from).await,
            None => Err(RpcError::topic_not_found(topic)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::NoService;

    #[test]
    fn longest_route_wins_and_duplicates_are_refused() {
        let a: Arc<dyn Service> = Arc::new(NoService);
        let b: Arc<dyn Service> = Arc::new(NoService);
        let mut m = ServiceMux::new();
        m.register(["sessions.", "host.info"], a.clone()).unwrap();
        m.register(["sessions.special"], b.clone()).unwrap();
        assert!(Arc::ptr_eq(m.route("sessions.list").unwrap(), &a));
        assert!(Arc::ptr_eq(m.route("sessions.special").unwrap(), &b));
        assert!(Arc::ptr_eq(m.route("host.info").unwrap(), &a));
        assert!(m.route("host.infox").is_none());
        assert!(m.route("git.status").is_none());
        assert_eq!(
            m.register(["host.info"], b),
            Err(MuxError::Duplicate("host.info".into()))
        );
    }
}
