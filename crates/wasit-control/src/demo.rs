//! A small demonstration service (used by the `wasit-control-demo` binary and the tests).
//!
//! Methods: `echo` (returns params), `whoami` (local host id), `sleep {ms}` (answers after
//! `ms`), `fail` (always errors), `big {bytes}` (returns a string of that size), `peer_call
//! {host, method, params}` (calls back over the connection the request came on, e.g. to ask a
//! hub to route to another host).
//!
//! Topic `count {limit?}`: cursor `n` carries payload `n`; replay from cursor `c` yields
//! `c+1, c+2, …` up to `limit` (then `Ended`), or forever.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::connection::RequestOpts;
use crate::service::{CallContext, EventStream, Service};
use crate::wire::{Cursor, HostId, RpcError};

/// Counters observable by tests.
#[derive(Debug)]
pub struct DemoStats {
    pub subscribes: AtomicU64,
    pub calls: AtomicU64,
    /// Number of `count` streams dropped (ended, unsubscribed, lagged, connection lost).
    pub streams_dropped: watch::Sender<u64>,
}

pub struct DemoService {
    host: HostId,
    pub stats: Arc<DemoStats>,
}

impl DemoService {
    pub fn new(host: impl Into<HostId>) -> Self {
        DemoService {
            host: host.into(),
            stats: Arc::new(DemoStats {
                subscribes: AtomicU64::new(0),
                calls: AtomicU64::new(0),
                streams_dropped: watch::channel(0).0,
            }),
        }
    }
}

struct DropCounter(Arc<DemoStats>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.streams_dropped.send_modify(|n| *n += 1);
    }
}

/// Stream of `count` events after `from` (exclusive) up to `limit` (inclusive).
pub fn count_stream(from: Option<&Cursor>, limit: Option<u64>) -> Result<EventStream, RpcError> {
    let start = match from {
        None => 0,
        Some(c) => c
            .as_u64()
            .ok_or_else(|| RpcError::invalid_params(format!("bad cursor {c}")))?,
    };
    let s = futures::stream::unfold(start, move |n| async move {
        let next = n + 1;
        if limit.is_some_and(|l| next > l) {
            return None;
        }
        if next % 64 == 0 {
            // Stay fair to other tasks when producing an unbounded stream.
            tokio::task::yield_now().await;
        }
        Some((Ok((Cursor::from_u64(next), json!(next))), next))
    });
    Ok(s.boxed())
}

#[async_trait]
impl Service for DemoService {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        self.stats.calls.fetch_add(1, Ordering::Relaxed);
        match method {
            "echo" => Ok(params),
            "whoami" => Ok(json!(self.host)),
            "sleep" => {
                let ms = params.get("ms").and_then(Value::as_u64).unwrap_or(0);
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(json!("slept"))
            }
            "fail" => Err(RpcError::new("demo_failure", "asked to fail")),
            "big" => {
                let n = params.get("bytes").and_then(Value::as_u64).unwrap_or(0) as usize;
                Ok(json!("x".repeat(n)))
            }
            "peer_call" => {
                let host = params.get("host").and_then(Value::as_str).map(HostId::from);
                let method = params
                    .get("method")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RpcError::invalid_params("missing method"))?;
                let inner = params.get("params").cloned().unwrap_or(Value::Null);
                let opts = RequestOpts {
                    host,
                    hops: ctx.hops,
                    deadline: Duration::from_secs(20),
                };
                ctx.peer
                    .call(method, inner, opts)
                    .await
                    .map_err(|e| e.into_rpc())
            }
            other => Err(RpcError::method_not_found(other)),
        }
    }

    async fn subscribe(
        &self,
        _ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        if topic != "count" {
            return Err(RpcError::topic_not_found(topic));
        }
        self.stats.subscribes.fetch_add(1, Ordering::Relaxed);
        let limit = params.get("limit").and_then(Value::as_u64);
        let inner = count_stream(from.as_ref(), limit)?;
        let guard = DropCounter(self.stats.clone());
        Ok(inner
            .map(move |item| {
                let _keep = &guard;
                item
            })
            .boxed())
    }
}
