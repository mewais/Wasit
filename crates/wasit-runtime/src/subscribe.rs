//! The stream behind `session.events`: a session's events after a cursor, read from its event
//! log and continued live from its actor, across the session's ends and continues.
//!
//! Each step decides where the next events come from with the session's actor-start counter and
//! its running actor (if any) taken under one lock:
//! - An actor runs: it registers a subscriber and answers with `P`, the last cursor it had
//!   published. Events up to `P` are read from the log once the log has written them; the
//!   subscriber's queue supplies the rest, filtered to cursors beyond what was delivered.
//! - No actor runs: the log is read to its end, then the stream waits for the counter to change
//!   (an actor started by a command or a continue) and decides again.
//!
//! Every event is delivered once, in journal (cursor) order: each source is filtered to cursors
//! beyond the last one delivered, and a source is left only once it holds nothing more. Nothing
//! here starts an actor, polls, or times out. The stream holds no strong reference to the
//! manager while it waits, so dropping the manager ends it.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};

use crate::actor::{Msg, SubscribeReply};
use crate::manager::WeakManager;
use crate::stream::StreamEnd;
use crate::wire::{RuntimeError, SessionEvent, SessionId};

type LogRx = mpsc::Receiver<io::Result<SessionEvent>>;

/// One subscription's position and source.
pub(crate) struct Feed {
    mgr: WeakManager,
    id: SessionId,
    /// The last cursor delivered (`from` until the first event; `None`: from the beginning).
    last: Option<u64>,
    step: Step,
}

enum Step {
    /// Decide where the next events come from.
    Choose,
    /// Events read from the log, then `then`.
    Log {
        rx: LogRx,
        then: Then,
    },
    /// Events from a running actor's subscriber queue.
    Live(Live),
    /// No actor runs and the log is read to its end: wait for an actor to start.
    Wait(watch::Receiver<u64>),
    Done,
}

/// What follows a read of the log.
enum Then {
    Live(Live),
    Wait(watch::Receiver<u64>),
}

struct Live {
    rx: mpsc::Receiver<SessionEvent>,
    lagged: Arc<AtomicBool>,
}

fn log_err(id: &SessionId, e: io::Error) -> RuntimeError {
    RuntimeError::Io(format!("read the event log of session {id}: {e}"))
}

impl Feed {
    /// A subscription after `from`, positioned before it is returned: with an actor running,
    /// its subscriber is registered, so every event published from here on reaches it.
    pub(crate) async fn start(
        mgr: WeakManager,
        id: SessionId,
        from: Option<u64>,
    ) -> Result<Self, RuntimeError> {
        let mut feed = Feed {
            mgr,
            id,
            last: from,
            step: Step::Done,
        };
        feed.step = feed.choose().await?;
        Ok(feed)
    }

    /// The next event; `None` once the subscription ends (the session was deleted, the manager
    /// dropped, or after the one `Err`).
    pub(crate) async fn next(&mut self) -> Option<Result<SessionEvent, StreamEnd>> {
        loop {
            let step = std::mem::replace(&mut self.step, Step::Done);
            self.step = match step {
                Step::Done => return None,
                Step::Choose => match self.choose().await {
                    Ok(s) => s,
                    // Deleted: the topic ended.
                    Err(RuntimeError::SessionNotFound(_)) => return None,
                    Err(e) => return Some(Err(StreamEnd::Error(e.to_string()))),
                },
                Step::Log { mut rx, then } => match rx.recv().await {
                    Some(Ok(e)) => {
                        let fresh = self.last.is_none_or(|l| e.cursor > l);
                        self.step = Step::Log { rx, then };
                        if fresh {
                            self.last = Some(e.cursor);
                            return Some(Ok(e));
                        }
                        continue;
                    }
                    Some(Err(e)) => {
                        return Some(Err(StreamEnd::Error(log_err(&self.id, e).to_string())))
                    }
                    None => match then {
                        Then::Live(l) => Step::Live(l),
                        Then::Wait(w) => Step::Wait(w),
                    },
                },
                Step::Live(mut l) => match l.rx.recv().await {
                    Some(e) => {
                        let fresh = self.last.is_none_or(|last| e.cursor > last);
                        self.step = Step::Live(l);
                        if fresh {
                            self.last = Some(e.cursor);
                            return Some(Ok(e));
                        }
                        // Already delivered from the log.
                        continue;
                    }
                    None if l.lagged.load(Ordering::Acquire) => {
                        return Some(Err(StreamEnd::Lagged));
                    }
                    // The actor is gone: what it published is in its log; decide again.
                    None => Step::Choose,
                },
                Step::Wait(mut started) => match started.changed().await {
                    Ok(()) => Step::Choose,
                    // The session's entry is gone (deleted, or the manager dropped).
                    Err(_) => return None,
                },
            };
        }
    }

    /// Where the events after `last` come from now (see the module docs).
    async fn choose(&mut self) -> Result<Step, RuntimeError> {
        let Some(mgr) = self.mgr.upgrade() else {
            return Ok(Step::Done);
        };
        let (started, actor) = mgr.attach_point(&self.id)?;
        if let Some(tx) = actor {
            drop(mgr);
            let (reply, answer) = oneshot::channel();
            if tx.send(Msg::Subscribe { reply }).await.is_ok() {
                if let Ok(r) = answer.await {
                    return self.via_actor(r).await;
                }
            }
            // The actor ended before it registered the subscriber. Everything it published is
            // in its log, and `started` was taken before: a start after it is not missed.
            let Some(m) = self.mgr.upgrade() else {
                return Ok(Step::Done);
            };
            return self.via_log(m, started).await;
        }
        self.via_log(mgr, started).await
    }

    /// A running actor's subscriber: the log up to the actor's `P`, then its queue.
    async fn via_actor(&self, r: SubscribeReply) -> Result<Step, RuntimeError> {
        let live = Live {
            rx: r.live,
            lagged: r.lagged,
        };
        if self.last.is_some_and(|l| l >= r.through) || r.through == 0 {
            return Ok(Step::Live(live));
        }
        let Some(view) = r.log else {
            return Err(RuntimeError::Io(format!(
                "session {} has no event log to read its events from",
                self.id
            )));
        };
        // The events up to `P` were all handed to the log writer before the actor published
        // them; wait until they are on disk.
        view.caught_up(r.submitted)
            .await
            .map_err(|e| log_err(&self.id, e))?;
        Ok(Step::Log {
            rx: view.stream(self.last, Some(r.through)),
            then: Then::Live(live),
        })
    }

    /// No actor runs: the whole log after `last`, then a wait for the next start.
    async fn via_log(
        &self,
        mgr: crate::manager::SessionManager,
        started: watch::Receiver<u64>,
    ) -> Result<Step, RuntimeError> {
        let view = mgr.log_view(&self.id).await?;
        // Not held while the log settles: dropping the manager must end the stream.
        drop(mgr);
        view.settled().await.map_err(|e| log_err(&self.id, e))?;
        Ok(Step::Log {
            rx: view.stream(self.last, None),
            then: Then::Wait(started),
        })
    }
}
