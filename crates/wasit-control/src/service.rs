//! The `Service` trait higher layers implement to serve methods and topics.

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::Value;

use crate::connection::Connection;
use crate::wire::{CloseReason, Cursor, Hello, HostId, RpcError, TraceId};

/// Events of one subscription. An `Err` item ends the subscription with that reason
/// (`Err(Ended)` is the same as the stream ending); stream end means `Ended`.
///
/// The service is responsible for replay: given `from = Some(c)` the stream must yield exactly
/// the events after `c`, in order, followed by live events, with no gap and no duplicate.
pub type EventStream = BoxStream<'static, Result<(Cursor, Value), CloseReason>>;

/// Context of one incoming request or subscription.
#[derive(Clone)]
pub struct CallContext {
    /// The connection the request arrived on; can be used to call back into the peer.
    pub peer: Connection,
    /// Target host named in the frame (`None` = whoever serves this connection).
    pub host: Option<HostId>,
    /// Number of router hops this request has already taken.
    pub hops: u32,
    /// The user intent the request serves, when it has one (always `None` for a subscription:
    /// a subscription is not an intent).
    pub trace: Option<TraceId>,
}

impl CallContext {
    pub fn remote_hello(&self) -> &Hello {
        self.peer.remote_hello()
    }
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError>;

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        let _ = (ctx, params, from); // unused by the default implementation
        Err(RpcError::topic_not_found(topic))
    }
}

/// A service that serves nothing (every call answers `method_not_found`).
pub struct NoService;

#[async_trait]
impl Service for NoService {
    async fn call(
        &self,
        _ctx: CallContext,
        method: &str,
        _params: Value,
    ) -> Result<Value, RpcError> {
        Err(RpcError::method_not_found(method))
    }
}
