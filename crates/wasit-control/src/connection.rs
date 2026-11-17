//! A symmetric protocol connection.
//!
//! After the hello exchange both sides are equal: either may send requests and subscriptions,
//! and either may serve them (if it was given a [`Service`]). The "client" and "server" types
//! are thin wrappers that differ only in who dials and who listens.
//!
//! Tasks per connection: one reader (dispatches frames, never blocks on a consumer), one writer
//! (drains the bounded outbound queue), one keepalive ticker, plus one task per in-flight
//! request handler and per served subscription.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::time::{timeout, timeout_at, Instant};
use tokio_util::sync::CancellationToken;

use crate::error::{CallError, ConnClose, ConnectError};
use crate::service::{CallContext, EventStream, Service};
use crate::transport::BoxDuplex;
use crate::wire::{
    codes, decode, encode, CloseReason, Cursor, Frame, FrameError, FrameReader, Hello, HostId,
    RpcError, SubId, DEFAULT_MAX_FRAME,
};

/// Tunables of one connection. Every wait has a bound.
#[derive(Debug, Clone)]
pub struct ConnConfig {
    /// Largest frame accepted or sent.
    pub max_frame: usize,
    /// Capacity (frames) of the outbound queue.
    pub outbound_queue: usize,
    /// Capacity (events) buffered per incoming subscription before it is closed as `Lagged`.
    pub subscription_queue: usize,
    pub hello_timeout: Duration,
    pub ping_interval: Duration,
    /// The connection is declared dead if nothing arrives for this long.
    pub idle_timeout: Duration,
    /// Bound on writing one frame to the transport, and on waiting for outbound queue space
    /// for responses and control frames.
    pub write_timeout: Duration,
    /// A served subscription whose events cannot enter the outbound queue for this long is
    /// closed as `Lagged`.
    pub lag_timeout: Duration,
    /// In-flight request handlers per connection; beyond this requests get `overloaded`.
    pub max_concurrent_requests: usize,
    /// Deadline used by convenience calls that take none.
    pub default_deadline: Duration,
}

impl Default for ConnConfig {
    fn default() -> Self {
        ConnConfig {
            max_frame: DEFAULT_MAX_FRAME,
            outbound_queue: 256,
            subscription_queue: 256,
            hello_timeout: Duration::from_secs(15),
            ping_interval: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(45),
            write_timeout: Duration::from_secs(20),
            lag_timeout: Duration::from_secs(2),
            max_concurrent_requests: 256,
            default_deadline: Duration::from_secs(30),
        }
    }
}

/// Result of version/feature negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiated {
    /// Minor version both sides understand (the lower of the two).
    pub minor: u16,
    /// Features both sides announced.
    pub features: Vec<String>,
}

/// Options of an outgoing request.
#[derive(Debug, Clone)]
pub struct RequestOpts {
    pub host: Option<HostId>,
    pub hops: u32,
    pub deadline: Duration,
}

impl RequestOpts {
    pub fn new(deadline: Duration) -> Self {
        RequestOpts {
            host: None,
            hops: 0,
            deadline,
        }
    }

    pub fn host(mut self, host: impl Into<HostId>) -> Self {
        self.host = Some(host.into());
        self
    }
}

/// Options of an outgoing subscription.
#[derive(Debug, Clone)]
pub struct SubscribeOpts {
    pub host: Option<HostId>,
    pub params: Value,
    pub from: Option<Cursor>,
    pub hops: u32,
    /// Bound on waiting for `Subscribed`.
    pub deadline: Duration,
}

impl SubscribeOpts {
    pub fn new(deadline: Duration) -> Self {
        SubscribeOpts {
            host: None,
            params: Value::Null,
            from: None,
            hops: 0,
            deadline,
        }
    }

    pub fn host(mut self, host: impl Into<HostId>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }

    pub fn from(mut self, from: Option<Cursor>) -> Self {
        self.from = from;
        self
    }
}

/// Flow-control window announced for incoming subscriptions: the local buffer size, so a
/// well-behaved server never overflows it.
fn window(cfg: &ConnConfig) -> u32 {
    u32::try_from(cfg.subscription_queue.max(1)).unwrap_or(u32::MAX)
}

/// A subscription this side serves.
struct Served {
    token: CancellationToken,
    /// Remaining send window (`None` = peer asked for no flow control).
    credit: Option<Arc<Semaphore>>,
    window: u32,
}

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A poisoned map is still structurally valid; the panicking task's entry is just stale.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

struct SubParts {
    sub: SubId,
    items: mpsc::Receiver<(Cursor, Value)>,
    close: oneshot::Receiver<CloseReason>,
}

enum Pending {
    Call(oneshot::Sender<Result<Value, RpcError>>),
    Sub(oneshot::Sender<Result<SubParts, RpcError>>),
}

struct SubSlot {
    items: mpsc::Sender<(Cursor, Value)>,
    close: oneshot::Sender<CloseReason>,
}

pub(crate) struct Inner {
    id: u64,
    local_hello: Hello,
    remote_hello: Hello,
    negotiated: Negotiated,
    cfg: ConnConfig,
    out: mpsc::Sender<Vec<u8>>,
    next_id: AtomicU64,
    next_sub: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    subs: Mutex<HashMap<SubId, SubSlot>>,
    served: Mutex<HashMap<SubId, Served>>,
    cancel: CancellationToken,
    close: watch::Sender<Option<ConnClose>>,
    service: Option<Arc<dyn Service>>,
    limiter: Arc<Semaphore>,
}

/// Closes the connection when the last user-facing handle is dropped.
struct Guard {
    cancel: CancellationToken,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Handle to an established connection. Cheap to clone; the connection closes when the last
/// clone is dropped (or on [`Connection::close`]).
#[derive(Clone)]
pub struct Connection {
    inner: Arc<Inner>,
    _guard: Arc<Guard>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.inner.id)
            .field("remote", &self.inner.remote_hello.host_id)
            .finish()
    }
}

impl Connection {
    /// Perform the hello exchange over `io` and start the connection tasks.
    pub async fn establish(
        io: BoxDuplex,
        hello: Hello,
        service: Option<Arc<dyn Service>>,
        cfg: ConnConfig,
    ) -> Result<Connection, ConnectError> {
        let (r, mut w) = tokio::io::split(io);
        let mut reader = FrameReader::new(r, cfg.max_frame);
        let hello_bytes = encode(&Frame::Hello(hello.clone()), cfg.max_frame)
            .map_err(|e| ConnectError::Protocol(e.to_string()))?;
        let deadline = Instant::now() + cfg.hello_timeout;

        let write_err = match timeout_at(deadline, async {
            w.write_all(&hello_bytes).await?;
            w.flush().await
        })
        .await
        {
            Err(_) => return Err(ConnectError::HandshakeTimeout),
            Ok(Err(e)) => Some(ConnectError::io(e)),
            Ok(Ok(())) => None,
        };

        // Even if our hello could not be written, the peer may have told us why (a bridge's
        // goodbye), so read before reporting the write error.
        let theirs = loop {
            let line = match timeout_at(deadline, reader.next_line()).await {
                Err(_) => return Err(write_err.unwrap_or(ConnectError::HandshakeTimeout)),
                Ok(Err(FrameError::Io(e))) => return Err(write_err.unwrap_or(ConnectError::io(e))),
                Ok(Err(e)) => return Err(ConnectError::Protocol(e.to_string())),
                Ok(Ok(None)) => {
                    return Err(write_err.unwrap_or(ConnectError::ClosedDuringHandshake))
                }
                Ok(Ok(Some(line))) => line,
            };
            match decode(&line) {
                Ok(Some(Frame::Hello(h))) => break h,
                Ok(Some(Frame::Goodbye { reason })) => return Err(ConnectError::Refused(reason)),
                Ok(Some(other)) => {
                    return Err(ConnectError::Protocol(format!(
                        "expected hello, got {}",
                        frame_kind(&other)
                    )))
                }
                Ok(None) => continue,
                Err(e) => return Err(ConnectError::Protocol(e.to_string())),
            }
        };
        if let Some(e) = write_err {
            return Err(e);
        }

        if theirs.protocol.major != hello.protocol.major {
            let reason = RpcError::new(
                codes::INCOMPATIBLE,
                format!(
                    "protocol {} is incompatible with {}",
                    hello.protocol, theirs.protocol
                ),
            );
            if let Ok(b) = encode(&Frame::Goodbye { reason }, cfg.max_frame) {
                // Best effort: the peer detects the mismatch on its own as well.
                let _ = timeout(Duration::from_secs(1), w.write_all(&b)).await;
            }
            return Err(ConnectError::Incompatible {
                ours: hello.protocol,
                theirs: theirs.protocol,
            });
        }

        let negotiated = Negotiated {
            minor: hello.protocol.minor.min(theirs.protocol.minor),
            features: hello
                .features
                .iter()
                .filter(|f| theirs.features.contains(f))
                .cloned()
                .collect(),
        };

        let (tx, rx) = mpsc::channel(cfg.outbound_queue.max(1));
        let (close_tx, _) = watch::channel(None);
        let inner = Arc::new(Inner {
            id: NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed),
            local_hello: hello,
            remote_hello: theirs,
            negotiated,
            out: tx,
            next_id: AtomicU64::new(1),
            next_sub: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            served: Mutex::new(HashMap::new()),
            cancel: CancellationToken::new(),
            close: close_tx,
            service,
            limiter: Arc::new(Semaphore::new(cfg.max_concurrent_requests.max(1))),
            cfg,
        });
        let guard = Arc::new(Guard {
            cancel: inner.cancel.clone(),
        });
        tokio::spawn(writer_task(inner.clone(), w, rx));
        tokio::spawn(reader_task(inner.clone(), Arc::downgrade(&guard), reader));
        tokio::spawn(keepalive_task(inner.clone()));
        Ok(Connection {
            inner,
            _guard: guard,
        })
    }

    /// Process-unique id of this connection.
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn local_hello(&self) -> &Hello {
        &self.inner.local_hello
    }

    pub fn remote_hello(&self) -> &Hello {
        &self.inner.remote_hello
    }

    pub fn negotiated(&self) -> &Negotiated {
        &self.inner.negotiated
    }

    pub fn config(&self) -> &ConnConfig {
        &self.inner.cfg
    }

    pub fn is_closed(&self) -> bool {
        self.inner.cancel.is_cancelled()
    }

    pub fn close_reason(&self) -> Option<ConnClose> {
        self.inner.close.borrow().clone()
    }

    /// Resolves once the connection has closed.
    pub async fn closed(&self) -> ConnClose {
        let mut rx = self.inner.close.subscribe();
        let res = match rx.wait_for(Option::is_some).await {
            Ok(v) => v.clone().unwrap_or(ConnClose::Local),
            // The sender lives in `Inner`, which we hold; unreachable in practice.
            Err(_) => ConnClose::Local,
        };
        res
    }

    /// Close locally without a goodbye.
    pub fn close(&self) {
        self.inner.close(ConnClose::Local);
    }

    /// Send a goodbye carrying `reason`, then close.
    pub fn goodbye(&self, reason: RpcError) {
        if let Ok(b) = encode(&Frame::Goodbye { reason }, self.inner.cfg.max_frame) {
            // If the queue is full the peer will see EOF instead of the goodbye.
            let _ = self.inner.out.try_send(b);
        }
        self.inner.close(ConnClose::Local);
    }

    /// Send a request and wait (bounded by `opts.deadline`) for its response.
    pub async fn call(
        &self,
        method: &str,
        params: Value,
        opts: RequestOpts,
    ) -> Result<Value, CallError> {
        let inner = &self.inner;
        let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let bytes = encode(
            &Frame::Request {
                id,
                host: opts.host,
                method: method.to_owned(),
                params,
                hops: opts.hops,
            },
            inner.cfg.max_frame,
        )
        .map_err(CallError::Rpc)?;
        let (tx, rx) = oneshot::channel();
        lock(&inner.pending).insert(id, Pending::Call(tx));
        if inner.cancel.is_cancelled() {
            lock(&inner.pending).remove(&id);
            return Err(self.lost());
        }
        let res = timeout(opts.deadline, async {
            if inner.out.send(bytes).await.is_err() {
                return Err(self.lost());
            }
            match rx.await {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => Err(CallError::Rpc(e)),
                Err(_) => Err(self.lost()),
            }
        })
        .await;
        match res {
            Ok(r) => r,
            Err(_) => {
                lock(&inner.pending).remove(&id);
                Err(CallError::Timeout)
            }
        }
    }

    /// Typed request with a deadline.
    pub async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: impl Serialize,
        deadline: Duration,
    ) -> Result<T, CallError> {
        self.request_with(method, params, RequestOpts::new(deadline))
            .await
    }

    /// Typed request with full options.
    pub async fn request_with<T: DeserializeOwned>(
        &self,
        method: &str,
        params: impl Serialize,
        opts: RequestOpts,
    ) -> Result<T, CallError> {
        let params = serde_json::to_value(params).map_err(|e| CallError::Encode(e.to_string()))?;
        let v = self.call(method, params, opts).await?;
        serde_json::from_value(v).map_err(|e| CallError::Decode(e.to_string()))
    }

    /// Subscribe to `topic` on the peer, replaying after `from`.
    pub async fn subscribe(
        &self,
        topic: &str,
        from: Option<Cursor>,
    ) -> Result<Subscription, CallError> {
        self.subscribe_with(
            topic,
            SubscribeOpts::new(self.inner.cfg.default_deadline).from(from),
        )
        .await
    }

    pub async fn subscribe_with(
        &self,
        topic: &str,
        opts: SubscribeOpts,
    ) -> Result<Subscription, CallError> {
        let inner = &self.inner;
        let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let bytes = encode(
            &Frame::Subscribe {
                id,
                host: opts.host,
                topic: topic.to_owned(),
                params: opts.params,
                from: opts.from.clone(),
                hops: opts.hops,
                window: window(&inner.cfg),
            },
            inner.cfg.max_frame,
        )
        .map_err(CallError::Rpc)?;
        let (tx, rx) = oneshot::channel();
        lock(&inner.pending).insert(id, Pending::Sub(tx));
        if inner.cancel.is_cancelled() {
            lock(&inner.pending).remove(&id);
            return Err(self.lost());
        }
        let res = timeout(opts.deadline, async {
            if inner.out.send(bytes).await.is_err() {
                return Err(self.lost());
            }
            match rx.await {
                Ok(Ok(parts)) => Ok(parts),
                Ok(Err(e)) => Err(CallError::Rpc(e)),
                Err(_) => Err(self.lost()),
            }
        })
        .await;
        let parts = match res {
            Ok(r) => r?,
            Err(_) => {
                // If `Subscribed` arrives later the reader finds no pending entry and
                // unsubscribes.
                lock(&inner.pending).remove(&id);
                return Err(CallError::Timeout);
            }
        };
        Ok(Subscription {
            sub: parts.sub,
            items: parts.items,
            close: Some(parts.close),
            reason: None,
            last: opts.from,
            conn: Arc::downgrade(&self.inner),
            window: window(&inner.cfg),
            consumed: 0,
        })
    }

    fn lost(&self) -> CallError {
        CallError::ConnectionLost(self.close_reason().unwrap_or(ConnClose::Local))
    }
}

fn frame_kind(f: &Frame) -> &'static str {
    match f {
        Frame::Hello(_) => "hello",
        Frame::Request { .. } => "request",
        Frame::Response { .. } => "response",
        Frame::Subscribe { .. } => "subscribe",
        Frame::Subscribed { .. } => "subscribed",
        Frame::Event { .. } => "event",
        Frame::Unsubscribe { .. } => "unsubscribe",
        Frame::Credit { .. } => "credit",
        Frame::SubscriptionClosed { .. } => "subscription_closed",
        Frame::Ping { .. } => "ping",
        Frame::Pong { .. } => "pong",
        Frame::Goodbye { .. } => "goodbye",
    }
}

impl Inner {
    fn close(&self, reason: ConnClose) {
        self.close.send_if_modified(|c| {
            if c.is_none() {
                *c = Some(reason);
                true
            } else {
                false
            }
        });
        // Cancel first, then drain: inserters re-check `cancel` after inserting.
        self.cancel.cancel();
        let pending = std::mem::take(&mut *lock(&self.pending));
        drop(pending); // waiters observe a dropped sender => ConnectionLost
        let subs = std::mem::take(&mut *lock(&self.subs));
        for (_, slot) in subs {
            // The consumer may already be gone; nothing to tell then.
            let _ = slot.close.send(CloseReason::HostLost);
        }
        let served = std::mem::take(&mut *lock(&self.served));
        for s in served.values() {
            s.token.cancel();
        }
    }

    /// Enqueue a control frame without waiting. If the queue is full, a task waits (bounded by
    /// `write_timeout`) for space.
    fn send_soon(&self, frame: Frame) {
        let Ok(bytes) = encode(&frame, self.cfg.max_frame) else {
            return; // control frames are tiny; cannot exceed the limit
        };
        match self.out.try_send(bytes) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(bytes)) => {
                let out = self.out.clone();
                let wt = self.cfg.write_timeout;
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        // On timeout the connection is stalled; the writer's own timeout
                        // closes it.
                        let _ = timeout(wt, out.send(bytes)).await;
                    });
                }
            }
            // Connection closed: the frame is moot.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Enqueue, waiting at most `write_timeout` for queue space. Closes the connection if the
    /// queue stays full (the peer stopped reading).
    async fn send_bounded(&self, bytes: Vec<u8>) -> bool {
        match timeout(self.cfg.write_timeout, self.out.send(bytes)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                self.close(ConnClose::Io {
                    message: "outbound queue stalled (peer not reading)".into(),
                });
                false
            }
        }
    }

    async fn send_response(&self, id: u64, res: Result<Value, RpcError>) {
        let frame = match res {
            Ok(result) => Frame::Response {
                id,
                result,
                error: None,
            },
            Err(e) => Frame::Response {
                id,
                result: Value::Null,
                error: Some(e),
            },
        };
        let bytes = match encode(&frame, self.cfg.max_frame) {
            Ok(b) => b,
            Err(e) => match encode(
                &Frame::Response {
                    id,
                    result: Value::Null,
                    error: Some(e),
                },
                self.cfg.max_frame,
            ) {
                Ok(b) => b,
                Err(_) => return,
            },
        };
        self.send_bounded(bytes).await;
    }

    fn dispatch(self: &Arc<Self>, frame: Frame, guard: &Weak<Guard>) -> Result<(), ConnClose> {
        match frame {
            Frame::Hello(_) => {
                return Err(ConnClose::Protocol {
                    message: "unexpected second hello".into(),
                })
            }
            Frame::Request {
                id,
                host,
                method,
                params,
                hops,
            } => self.serve_request(guard, id, host, method, params, hops),
            Frame::Response { id, result, error } => {
                let pending = lock(&self.pending).remove(&id);
                let res = match error {
                    Some(e) => Err(e),
                    None => Ok(result),
                };
                match pending {
                    // The caller may have timed out meanwhile.
                    Some(Pending::Call(tx)) => {
                        let _ = tx.send(res);
                    }
                    Some(Pending::Sub(tx)) => {
                        let _ = tx.send(res.and_then(|_| {
                            Err(RpcError::new(
                                codes::PROTOCOL,
                                "subscribe answered with a successful response",
                            ))
                        }));
                    }
                    None => {}
                }
            }
            Frame::Subscribe {
                id,
                host,
                topic,
                params,
                from,
                hops,
                window,
            } => self.serve_subscribe(guard, id, host, topic, params, from, hops, window),
            Frame::Subscribed { id, sub } => {
                let pending = lock(&self.pending).remove(&id);
                match pending {
                    Some(Pending::Sub(tx)) => {
                        let (items_tx, items_rx) =
                            mpsc::channel(self.cfg.subscription_queue.max(1));
                        let (close_tx, close_rx) = oneshot::channel();
                        lock(&self.subs).insert(
                            sub,
                            SubSlot {
                                items: items_tx,
                                close: close_tx,
                            },
                        );
                        if self.cancel.is_cancelled() {
                            if let Some(slot) = lock(&self.subs).remove(&sub) {
                                let _ = slot.close.send(CloseReason::HostLost);
                            }
                        }
                        let parts = SubParts {
                            sub,
                            items: items_rx,
                            close: close_rx,
                        };
                        if tx.send(Ok(parts)).is_err() {
                            lock(&self.subs).remove(&sub);
                            self.send_soon(Frame::Unsubscribe { sub });
                        }
                    }
                    _ => self.send_soon(Frame::Unsubscribe { sub }),
                }
            }
            Frame::Event {
                sub,
                cursor,
                payload,
            } => {
                let mut subs = lock(&self.subs);
                if let Some(slot) = subs.get(&sub) {
                    match slot.items.try_send((cursor, payload)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            if let Some(slot) = subs.remove(&sub) {
                                // Consumer gone is fine.
                                let _ = slot.close.send(CloseReason::Lagged);
                            }
                            drop(subs);
                            self.send_soon(Frame::Unsubscribe { sub });
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            subs.remove(&sub);
                            drop(subs);
                            self.send_soon(Frame::Unsubscribe { sub });
                        }
                    }
                }
                // Events for unknown subs are late arrivals after an unsubscribe.
            }
            Frame::Unsubscribe { sub } => {
                if let Some(s) = lock(&self.served).remove(&sub) {
                    s.token.cancel();
                }
            }
            Frame::Credit { sub, n } => {
                if let Some(s) = lock(&self.served).get(&sub) {
                    if let Some(sem) = &s.credit {
                        // Never let a confused peer grow the window without bound.
                        let cap = (s.window as usize).saturating_mul(2);
                        let add = (n as usize).min(cap.saturating_sub(sem.available_permits()));
                        sem.add_permits(add);
                    }
                }
            }
            Frame::SubscriptionClosed { sub, reason } => {
                if let Some(slot) = lock(&self.subs).remove(&sub) {
                    let _ = slot.close.send(reason); // consumer may be gone
                }
            }
            Frame::Ping { nonce } => self.send_soon(Frame::Pong { nonce }),
            Frame::Pong { .. } => {}
            Frame::Goodbye { reason } => return Err(ConnClose::Goodbye { reason }),
        }
        Ok(())
    }

    fn context(
        self: &Arc<Self>,
        guard: &Weak<Guard>,
        host: Option<HostId>,
        hops: u32,
    ) -> Option<CallContext> {
        let guard = guard.upgrade()?;
        Some(CallContext {
            peer: Connection {
                inner: self.clone(),
                _guard: guard,
            },
            host,
            hops,
        })
    }

    fn serve_request(
        self: &Arc<Self>,
        guard: &Weak<Guard>,
        id: u64,
        host: Option<HostId>,
        method: String,
        params: Value,
        hops: u32,
    ) {
        let Some(service) = self.service.clone() else {
            let inner = self.clone();
            tokio::spawn(async move {
                inner
                    .send_response(id, Err(RpcError::method_not_found(&method)))
                    .await
            });
            return;
        };
        let Some(ctx) = self.context(guard, host, hops) else {
            return; // closing
        };
        let inner = self.clone();
        let Ok(permit) = self.limiter.clone().try_acquire_owned() else {
            tokio::spawn(async move {
                inner
                    .send_response(
                        id,
                        Err(RpcError::new(
                            codes::OVERLOADED,
                            "too many concurrent requests",
                        )),
                    )
                    .await
            });
            return;
        };
        tokio::spawn(async move {
            let fut = AssertUnwindSafe(service.call(ctx, &method, params)).catch_unwind();
            let res = tokio::select! {
                _ = inner.cancel.cancelled() => return,
                r = fut => r.unwrap_or_else(|_| Err(RpcError::internal("request handler panicked"))),
            };
            drop(permit);
            inner.send_response(id, res).await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn serve_subscribe(
        self: &Arc<Self>,
        guard: &Weak<Guard>,
        id: u64,
        host: Option<HostId>,
        topic: String,
        params: Value,
        from: Option<Cursor>,
        hops: u32,
        window: u32,
    ) {
        let Some(service) = self.service.clone() else {
            let inner = self.clone();
            tokio::spawn(async move {
                inner
                    .send_response(id, Err(RpcError::topic_not_found(&topic)))
                    .await
            });
            return;
        };
        let Some(ctx) = self.context(guard, host, hops) else {
            return;
        };
        let inner = self.clone();
        tokio::spawn(async move {
            let fut = AssertUnwindSafe(service.subscribe(ctx, &topic, params, from)).catch_unwind();
            let res = tokio::select! {
                _ = inner.cancel.cancelled() => return,
                r = fut => r.unwrap_or_else(|_| Err(RpcError::internal("subscribe handler panicked"))),
            };
            let stream = match res {
                Ok(s) => s,
                Err(e) => {
                    inner.send_response(id, Err(e)).await;
                    return;
                }
            };
            inner.pump_subscription(id, stream, window).await;
        });
    }

    async fn pump_subscription(self: &Arc<Self>, id: u64, mut stream: EventStream, window: u32) {
        let sub = SubId(self.next_sub.fetch_add(1, Ordering::Relaxed));
        let token = self.cancel.child_token();
        let credit = (window > 0).then(|| Arc::new(Semaphore::new(window as usize)));
        lock(&self.served).insert(
            sub,
            Served {
                token: token.clone(),
                credit: credit.clone(),
                window,
            },
        );
        if self.cancel.is_cancelled() {
            lock(&self.served).remove(&sub);
            return;
        }
        let Ok(b) = encode(&Frame::Subscribed { id, sub }, self.cfg.max_frame) else {
            return;
        };
        if !self.send_bounded(b).await {
            lock(&self.served).remove(&sub);
            return;
        }
        let reason = loop {
            let item = tokio::select! {
                biased;
                _ = token.cancelled() => {
                    // Unsubscribed by the peer, or connection closing: no close frame.
                    lock(&self.served).remove(&sub);
                    return;
                }
                item = stream.next() => item,
            };
            match item {
                None | Some(Err(CloseReason::Ended)) => break CloseReason::Ended,
                Some(Err(reason)) => break reason,
                Some(Ok((cursor, payload))) => {
                    if let Some(sem) = &credit {
                        // Wait for the subscriber to make room; a consumer that stays full
                        // for `lag_timeout` is lagging.
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => {
                                lock(&self.served).remove(&sub);
                                return;
                            }
                            r = timeout(self.cfg.lag_timeout, sem.acquire()) => match r {
                                Ok(Ok(permit)) => permit.forget(),
                                Ok(Err(_)) => return, // semaphore is never closed
                                Err(_) => break CloseReason::Lagged,
                            },
                        }
                    }
                    let bytes = match encode(
                        &Frame::Event {
                            sub,
                            cursor,
                            payload,
                        },
                        self.cfg.max_frame,
                    ) {
                        Ok(b) => b,
                        Err(error) => break CloseReason::Error { error },
                    };
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            lock(&self.served).remove(&sub);
                            return;
                        }
                        r = timeout(self.cfg.lag_timeout, self.out.send(bytes)) => match r {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => return, // connection closed
                            Err(_) => break CloseReason::Lagged,
                        },
                    }
                }
            }
        };
        drop(stream);
        lock(&self.served).remove(&sub);
        if let Ok(b) = encode(
            &Frame::SubscriptionClosed { sub, reason },
            self.cfg.max_frame,
        ) {
            self.send_bounded(b).await;
        }
    }
}

async fn write_bounded(
    w: &mut WriteHalf<BoxDuplex>,
    bytes: &[u8],
    wt: Duration,
) -> Result<(), String> {
    match timeout(wt, w.write_all(bytes)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("write timed out".into()),
    }
}

async fn flush_bounded(w: &mut WriteHalf<BoxDuplex>, wt: Duration) -> Result<(), String> {
    match timeout(wt, w.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("flush timed out".into()),
    }
}

async fn writer_task(
    inner: Arc<Inner>,
    mut w: WriteHalf<BoxDuplex>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
    let wt = inner.cfg.write_timeout;
    let result: Result<(), String> = async {
        loop {
            let bytes = tokio::select! {
                biased;
                _ = inner.cancel.cancelled() => break,
                m = rx.recv() => match m {
                    Some(b) => b,
                    None => break,
                },
            };
            write_bounded(&mut w, &bytes, wt).await?;
            if rx.is_empty() {
                flush_bounded(&mut w, wt).await?;
            }
        }
        // Frames queued before the close (e.g. a goodbye) still go out, bounded.
        while let Ok(b) = rx.try_recv() {
            write_bounded(&mut w, &b, wt).await?;
        }
        flush_bounded(&mut w, wt).await?;
        // Shutting down the write side signals EOF to the peer; failures here are moot.
        let _ = timeout(wt, w.shutdown()).await;
        Ok(())
    }
    .await;
    if let Err(message) = result {
        tracing::debug!(conn = inner.id, %message, "connection writer failed");
        inner.close(ConnClose::Io { message });
    }
}

async fn reader_task(
    inner: Arc<Inner>,
    guard: Weak<Guard>,
    mut reader: FrameReader<tokio::io::ReadHalf<BoxDuplex>>,
) {
    let idle = inner.cfg.idle_timeout;
    let reason = loop {
        let next = tokio::select! {
            biased;
            _ = inner.cancel.cancelled() => break ConnClose::Local,
            r = timeout(idle, reader.next_line()) => r,
        };
        match next {
            Err(_) => break ConnClose::IdleTimeout,
            Ok(Err(FrameError::TooLarge { limit })) => {
                inner.send_soon(Frame::Goodbye {
                    reason: RpcError::new(
                        codes::FRAME_TOO_LARGE,
                        format!("frame exceeds {limit} bytes"),
                    ),
                });
                break ConnClose::FrameTooLarge;
            }
            Ok(Err(FrameError::Io(e))) => {
                break ConnClose::Io {
                    message: e.to_string(),
                }
            }
            Ok(Err(FrameError::Malformed(m))) => {
                inner.send_soon(Frame::Goodbye {
                    reason: RpcError::new(codes::PROTOCOL, m.clone()),
                });
                break ConnClose::Protocol { message: m };
            }
            Ok(Ok(None)) => break ConnClose::Eof,
            Ok(Ok(Some(line))) => match decode(&line) {
                Ok(Some(frame)) => {
                    if let Err(reason) = inner.dispatch(frame, &guard) {
                        if let ConnClose::Protocol { message } = &reason {
                            inner.send_soon(Frame::Goodbye {
                                reason: RpcError::new(codes::PROTOCOL, message.clone()),
                            });
                        }
                        break reason;
                    }
                }
                Ok(None) => {} // unknown frame type: ignored for forward compatibility
                Err(e) => {
                    let m = e.to_string();
                    inner.send_soon(Frame::Goodbye {
                        reason: RpcError::new(codes::PROTOCOL, m.clone()),
                    });
                    break ConnClose::Protocol { message: m };
                }
            },
        }
    };
    tracing::debug!(conn = inner.id, %reason, "connection closed");
    inner.close(reason);
}

async fn keepalive_task(inner: Arc<Inner>) {
    let period = inner.cfg.ping_interval;
    let mut iv = tokio::time::interval_at(Instant::now() + period, period);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut nonce = 0u64;
    loop {
        tokio::select! {
            _ = inner.cancel.cancelled() => break,
            _ = iv.tick() => {
                nonce += 1;
                inner.send_soon(Frame::Ping { nonce });
            }
        }
    }
}

/// An incoming subscription: a stream of `(cursor, payload)` ending with a [`CloseReason`].
pub struct Subscription {
    sub: SubId,
    items: mpsc::Receiver<(Cursor, Value)>,
    close: Option<oneshot::Receiver<CloseReason>>,
    reason: Option<CloseReason>,
    last: Option<Cursor>,
    conn: Weak<Inner>,
    window: u32,
    consumed: u32,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("sub", &self.sub)
            .field("last", &self.last)
            .field("reason", &self.reason)
            .finish()
    }
}

impl Subscription {
    pub fn id(&self) -> SubId {
        self.sub
    }

    /// Next event, or `None` once closed (see [`Subscription::close_reason`]).
    ///
    /// Events buffered before a close are always delivered before `None`.
    pub async fn next(&mut self) -> Option<(Cursor, Value)> {
        if self.reason.is_some() {
            return None;
        }
        match self.items.recv().await {
            Some(item) => {
                self.last = Some(item.0.clone());
                self.consumed += 1;
                if self.consumed >= (self.window / 2).max(1) {
                    if let Some(inner) = self.conn.upgrade() {
                        inner.send_soon(Frame::Credit {
                            sub: self.sub,
                            n: self.consumed,
                        });
                    }
                    self.consumed = 0;
                }
                Some(item)
            }
            None => {
                let reason = match self.close.take() {
                    Some(rx) => rx.await.unwrap_or(CloseReason::HostLost),
                    None => CloseReason::HostLost,
                };
                self.reason = Some(reason);
                None
            }
        }
    }

    pub fn close_reason(&self) -> Option<&CloseReason> {
        self.reason.as_ref()
    }

    /// Cursor of the last event returned by [`Subscription::next`] (or the `from` cursor).
    pub fn last_cursor(&self) -> Option<&Cursor> {
        self.last.as_ref()
    }

    /// Adapt into an [`EventStream`] (e.g. to forward it from a [`Service`]).
    pub fn into_event_stream(self) -> EventStream {
        futures::stream::unfold(Some(self), |state| async move {
            let mut sub = state?;
            match sub.next().await {
                Some(item) => Some((Ok(item), Some(sub))),
                None => match sub.close_reason().cloned() {
                    None | Some(CloseReason::Ended) => None,
                    Some(reason) => Some((Err(reason), None)),
                },
            }
        })
        .boxed()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.reason.is_some() {
            return;
        }
        if let Some(inner) = self.conn.upgrade() {
            if lock(&inner.subs).remove(&self.sub).is_some() {
                inner.send_soon(Frame::Unsubscribe { sub: self.sub });
            }
        }
    }
}
