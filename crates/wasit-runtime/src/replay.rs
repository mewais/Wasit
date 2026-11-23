//! Offline, deterministic replays of a session journal with a fresh [`Core`]: snapshots of
//! sessions without a live actor, history pages older than an actor's in-memory tail, and the
//! evicted prefix of an old-cursor subscription.
//!
//! Cost: a replay reads the journal from seq 1 (protocol state depends on everything before)
//! and runs the pure core over it — O(journal size) CPU and I/O, O(page size) memory. Live
//! sessions answer most requests from their in-memory tail; replays only serve what fell out
//! of it.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::mpsc;
use wasit_core::DriverDescriptor;
use wasit_shim::ShimState;

use crate::core::{Core, SessionMeta};
use crate::wire::{cursor_seq, HistoryPage, SessionEvent, SessionSnapshot, TrailPage};

/// Feed every journal record with `seq <= until` (all when `None`) through `core`, handing each
/// event to `f`. Stops early when `f` returns `false`.
async fn run(
    dir: &Path,
    core: &mut Core,
    until: Option<u64>,
    mut f: impl FnMut(SessionEvent) -> bool,
) -> Result<(), String> {
    let mut stream = match wasit_shim::read_journal(dir, 1).await {
        Ok(s) => s,
        Err(wasit_shim::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("read journal: {e}")),
    };
    let mut n = 0u32;
    while let Some(item) = stream.next().await {
        let rec = item.map_err(|e| format!("read journal: {e}"))?;
        if until.is_some_and(|u| rec.seq > u) {
            break;
        }
        for e in core.apply(&rec).events {
            if !f(e) {
                return Ok(());
            }
        }
        n = n.wrapping_add(1);
        if n.is_multiple_of(256) {
            // Stay fair to other sessions while replaying a long journal.
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

/// The snapshot of a session that has no live shim, rebuilt from its journal.
pub(crate) async fn snapshot(
    dir: &Path,
    run_dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
) -> Result<SessionSnapshot, String> {
    let mut core = Core::new(desc, meta);
    run(dir, &mut core, None, |_| true).await?;
    if !core.process_ended() {
        match wasit_shim::probe_at(dir, run_dir).await {
            Ok(ShimState::Running) => {}
            Ok(_) => {
                core.mark_lost("the shim is gone and the journal has no exit record");
            }
            Err(e) => {
                core.mark_lost(&format!("cannot probe the shim: {e}"));
            }
        }
    }
    Ok(core.snapshot().clone())
}

/// The last `limit` of what `pick` makes of the events with `cursor < before`, oldest first,
/// and whether older ones exist. One replay serves every kind of page: history picks the events
/// themselves, the trail their projection.
async fn window<T>(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    before: Option<u64>,
    limit: usize,
    mut pick: impl FnMut(SessionEvent) -> Option<T>,
) -> Result<(Vec<T>, bool), String> {
    let mut core = Core::new(desc, meta);
    let mut window: VecDeque<T> = VecDeque::with_capacity(limit.min(4096));
    let mut has_more = false;
    run(dir, &mut core, before.map(cursor_seq), |e| {
        if before.is_none_or(|b| e.cursor < b) {
            if let Some(item) = pick(e) {
                window.push_back(item);
                if window.len() > limit {
                    window.pop_front();
                    has_more = true;
                }
            }
        }
        true
    })
    .await?;
    Ok((window.into(), has_more))
}

/// The last `limit` events with `cursor < before`.
pub(crate) async fn history(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    before: Option<u64>,
    limit: usize,
) -> Result<HistoryPage, String> {
    let (events, has_more) = window(dir, desc, meta, before, limit, Some).await?;
    Ok(HistoryPage { events, has_more })
}

/// The last `limit` trail entries with `cursor < before`: the projection of the same replay.
pub(crate) async fn trail(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    before: Option<u64>,
    limit: usize,
) -> Result<TrailPage, String> {
    let (entries, has_more) =
        window(dir, desc, meta, before, limit, |e| crate::trail::item(&e)).await?;
    Ok(TrailPage { entries, has_more })
}

/// Stream the events with `after < cursor < below` (bounded channel; the replay runs as its own
/// task and stops when the receiver goes away).
pub(crate) fn events_between(
    dir: PathBuf,
    desc: Arc<dyn DriverDescriptor>,
    meta: SessionMeta,
    after: Option<u64>,
    below: u64,
) -> mpsc::Receiver<Result<SessionEvent, String>> {
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(async move {
        let mut core = Core::new(desc, &meta);
        // The replay is synchronous per record; hand events over through a small buffer so
        // backpressure from the consumer applies between records.
        let mut buf: Vec<SessionEvent> = Vec::new();
        let mut stream = match wasit_shim::read_journal(&dir, 1).await {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(format!("read journal: {e}"))).await; // consumer may be gone
                return;
            }
        };
        let until = cursor_seq(below);
        while let Some(item) = stream.next().await {
            let rec = match item {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(format!("read journal: {e}"))).await; // consumer may be gone
                    return;
                }
            };
            if rec.seq > until {
                break;
            }
            buf.extend(
                core.apply(&rec)
                    .events
                    .into_iter()
                    .filter(|e| after.is_none_or(|a| e.cursor > a) && e.cursor < below),
            );
            for e in buf.drain(..) {
                if tx.send(Ok(e)).await.is_err() {
                    return;
                }
            }
        }
    });
    rx
}

/// The bytes of blob `id`, rebuilt from the journal (the blob store is a cache of what the
/// journal holds; a missing blob is found again by replaying it).
pub(crate) async fn find_blob(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    id: &str,
) -> Result<Option<Vec<u8>>, String> {
    let mut core = Core::new(desc, meta);
    let mut stream = match wasit_shim::read_journal(dir, 1).await {
        Ok(s) => s,
        Err(wasit_shim::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None)
        }
        Err(e) => return Err(format!("read journal: {e}")),
    };
    let mut n = 0u32;
    while let Some(item) = stream.next().await {
        let rec = item.map_err(|e| format!("read journal: {e}"))?;
        if let Some((_, bytes)) = core.apply(&rec).blobs.into_iter().find(|(b, _)| b == id) {
            return Ok(Some(bytes));
        }
        n = n.wrapping_add(1);
        if n.is_multiple_of(256) {
            tokio::task::yield_now().await;
        }
    }
    Ok(None)
}
