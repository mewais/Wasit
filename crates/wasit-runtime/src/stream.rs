//! In-process subscription streams.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{BoxStream, StreamExt};
use tokio::sync::mpsc;

/// Why a subscription stream ended before its topic did.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamEnd {
    /// The subscriber fell behind and was dropped; resubscribe from the last cursor.
    #[error("subscriber fell behind; resubscribe from the last cursor")]
    Lagged,
    /// The stream failed (e.g. the journal could not be read for a replay).
    #[error("{0}")]
    Error(String),
}

/// A stream of subscription items. Yields `Err` at most once, as its last item; ends (`None`)
/// when the topic ends (e.g. the session was deleted).
pub struct EventStream<T> {
    inner: BoxStream<'static, Result<T, StreamEnd>>,
}

impl<T: Send + 'static> EventStream<T> {
    /// Wrap `inner`, ending it right after its first `Err`.
    pub(crate) fn new(inner: BoxStream<'static, Result<T, StreamEnd>>) -> Self {
        let inner = inner
            .scan(false, |failed, item| {
                let out = if *failed {
                    None
                } else {
                    *failed = item.is_err();
                    Some(item)
                };
                futures::future::ready(out)
            })
            .boxed();
        EventStream { inner }
    }

    pub async fn next(&mut self) -> Option<Result<T, StreamEnd>> {
        self.inner.next().await
    }

    /// Box the stream (e.g. to adapt it to `wasit_control::EventStream`).
    pub fn into_inner(self) -> BoxStream<'static, Result<T, StreamEnd>> {
        self.inner
    }
}

impl<T> futures::Stream for EventStream<T> {
    type Item = Result<T, StreamEnd>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

/// The live part of a subscription: items from `rx`, then `Err(Lagged)` if the producer dropped
/// us for falling behind.
pub(crate) fn live<T: Send + 'static>(
    rx: mpsc::Receiver<T>,
    lagged: Arc<AtomicBool>,
) -> BoxStream<'static, Result<T, StreamEnd>> {
    futures::stream::unfold(Some((rx, lagged)), |state| async move {
        let (mut rx, lagged) = state?;
        match rx.recv().await {
            Some(v) => Some((Ok(v), Some((rx, lagged)))),
            None if lagged.load(Ordering::Acquire) => Some((Err(StreamEnd::Lagged), None)),
            None => None,
        }
    })
    .boxed()
}

/// A bounded subscriber slot: the producer `try_send`s and drops the subscriber when full.
pub(crate) struct Subscriber<T> {
    pub tx: mpsc::Sender<T>,
    pub lagged: Arc<AtomicBool>,
}

impl<T> Subscriber<T> {
    pub(crate) fn new(cap: usize) -> (Self, mpsc::Receiver<T>) {
        let (tx, rx) = mpsc::channel(cap.max(1));
        (
            Subscriber {
                tx,
                lagged: Arc::new(AtomicBool::new(false)),
            },
            rx,
        )
    }

    /// Offer an item; `false` means the subscriber is gone (closed, or dropped for lagging).
    pub(crate) fn offer(&self, item: T) -> bool {
        match self.tx.try_send(item) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.lagged.store(true, Ordering::Release);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}
