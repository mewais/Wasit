//! The host-level session feed behind the `host.sessions` topic: session list + snapshot
//! changes, coalesced and bounded.
//!
//! Actors report every snapshot they publish; reports for the same session that arrive before
//! the feed task runs are coalesced (only the latest is kept). The task turns them into
//! [`HostEvent`]s with cursors `"<epoch>.<n>"`, kept in a bounded ring for resubscription and
//! offered to bounded subscriber queues (a subscriber that falls behind is dropped with
//! `Lagged` and resubscribes from its cursor, or gets a fresh [`HostEvent::Reset`] when the
//! cursor has expired or belongs to an earlier daemon run).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use futures::stream::StreamExt;
use tokio::sync::Notify;

use crate::stream::{live, EventStream, Subscriber};
use crate::wire::{HostEvent, SessionId, SessionSnapshot};

const RING: usize = 1024;
const SUB_QUEUE: usize = 1024;

enum Change {
    Upsert(Arc<SessionSnapshot>),
    Removed,
}

struct State {
    pending: BTreeMap<SessionId, Change>,
    current: BTreeMap<SessionId, Arc<SessionSnapshot>>,
    ring: VecDeque<(u64, HostEvent)>,
    /// Cursor number of the last event (0 = none yet).
    last: u64,
    subs: Vec<Subscriber<(String, HostEvent)>>,
}

pub(crate) struct HostFeed {
    epoch: String,
    state: Mutex<State>,
    wake: Notify,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    // Every critical section leaves the state consistent; a poisoned lock is still usable.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl HostFeed {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(HostFeed {
            epoch: format!("{:x}", fastrand::u64(..) >> 16),
            state: Mutex::new(State {
                pending: BTreeMap::new(),
                current: BTreeMap::new(),
                ring: VecDeque::new(),
                last: 0,
                subs: Vec::new(),
            }),
            wake: Notify::new(),
        })
    }

    /// A session's snapshot changed (cheap; called by actors and the manager).
    pub(crate) fn upsert(&self, snap: Arc<SessionSnapshot>) {
        lock(&self.state)
            .pending
            .insert(snap.id.clone(), Change::Upsert(snap));
        self.wake.notify_one();
    }

    pub(crate) fn remove(&self, id: &SessionId) {
        lock(&self.state)
            .pending
            .insert(id.clone(), Change::Removed);
        self.wake.notify_one();
    }

    fn cursor(&self, n: u64) -> String {
        format!("{}.{n}", self.epoch)
    }

    /// The feed task: run until the feed is dropped by everyone else.
    pub(crate) async fn run(self: Arc<Self>) {
        loop {
            self.wake.notified().await;
            let mut st = lock(&self.state);
            let pending = std::mem::take(&mut st.pending);
            for (id, change) in pending {
                let ev = match change {
                    Change::Upsert(snap) => {
                        if st.current.get(&id).is_some_and(|c| **c == *snap) {
                            continue;
                        }
                        st.current.insert(id, snap.clone());
                        HostEvent::Upsert {
                            session: Box::new((*snap).clone()),
                        }
                    }
                    Change::Removed => {
                        if st.current.remove(&id).is_none() {
                            continue;
                        }
                        HostEvent::Removed { id }
                    }
                };
                st.last += 1;
                let n = st.last;
                let cursor = self.cursor(n);
                st.subs.retain(|s| s.offer((cursor.clone(), ev.clone())));
                st.ring.push_back((n, ev));
                while st.ring.len() > RING {
                    st.ring.pop_front();
                }
            }
        }
    }

    /// Subscribe after `from` (a cursor of this feed), or from a fresh `Reset`.
    pub(crate) fn subscribe(&self, from: Option<&str>) -> EventStream<(String, HostEvent)> {
        let mut st = lock(&self.state);
        let after = from.and_then(|c| {
            let (epoch, n) = c.split_once('.')?;
            (epoch == self.epoch).then_some(())?;
            n.parse::<u64>().ok()
        });
        let oldest = st.ring.front().map(|(n, _)| *n).unwrap_or(st.last + 1);
        let backlog: Vec<(String, HostEvent)> = match after {
            // Everything after `n` is still in the ring.
            Some(n) if n <= st.last && n + 1 >= oldest => st
                .ring
                .iter()
                .filter(|(m, _)| *m > n)
                .map(|(m, e)| (self.cursor(*m), e.clone()))
                .collect(),
            _ => vec![(
                self.cursor(st.last),
                HostEvent::Reset {
                    sessions: st.current.values().map(|s| (**s).clone()).collect(),
                },
            )],
        };
        let (sub, rx) = Subscriber::new(SUB_QUEUE);
        let lagged = sub.lagged.clone();
        st.subs.push(sub);
        drop(st);
        EventStream::new(
            futures::stream::iter(backlog.into_iter().map(Ok))
                .chain(live(rx, lagged))
                .boxed(),
        )
    }
}
