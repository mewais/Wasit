//! Offline, deterministic replays of a session journal with a fresh [`Core`]: snapshots of
//! sessions without a live actor, history pages older than an actor's in-memory tail, and the
//! evicted prefix of an old-cursor subscription.
//!
//! Cost: a replay reads the journal from seq 1 (protocol state depends on everything before)
//! and runs the pure core over it — O(journal size) CPU and I/O, O(page size) memory. Live
//! sessions answer most requests from their in-memory tail; replays only serve what fell out
//! of it.

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs::{self, OpenOptions};
use tokio::sync::mpsc;
use wasit_core::{DriverDescriptor, SessionMode};
use wasit_shim::{journal::sorted_ndjson, ShimState};

use crate::core::{Core, SessionMeta};
use crate::eventlog::EventLog;
use crate::wire::{
    cursor_seq, HistoryPage, HistoryScope, SessionEvent, SessionSnapshot, TrailPage,
};

/// Feed every journal record with `seq <= until` (all when `None`) through `core`, handing each
/// event to `f` (the items of the conversation's earlier history instead of the events, with
/// `earlier`). Stops early when `f` returns `false`.
async fn run(
    dir: &Path,
    core: &mut Core,
    until: Option<u64>,
    earlier: bool,
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
        let out = core.apply(&rec);
        for e in if earlier { out.earlier } else { out.events } {
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
    mode_hints: &[SessionMode],
) -> Result<SessionSnapshot, String> {
    let mut core = Core::for_session(desc, meta, Some(mode_hints.to_vec()));
    run(dir, &mut core, None, false, |_| true).await?;
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

/// Build an event log from the journal, appending only cursors beyond the current log and sealing
/// it when complete. Used both offline and by the actor.
#[allow(dead_code)] // R2: wiring into actor/manager follows in the next increment
pub(crate) async fn build(
    dir: &Path,
    run_dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    mode_hints: &[SessionMode],
    log: &EventLog,
) -> Result<(), String> {
    let last_cursor = last_cursor(&log.paths()).await.map_err(|e| e.to_string())?;
    let mut core = Core::for_session(desc, meta, Some(mode_hints.to_vec()));
    let mut last_seq = 0u64;
    let mut stream = match wasit_shim::read_journal(dir, 1).await {
        Ok(s) => s,
        Err(wasit_shim::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("read journal: {e}")),
    };

    while let Some(item) = stream.next().await {
        let rec = item.map_err(|e| format!("read journal: {e}"))?;
        last_seq = rec.seq;
        let out = core.apply(&rec);
        let mut events: Vec<_> = out
            .events
            .into_iter()
            .filter(|e| e.cursor > last_cursor)
            .collect();
        let mut earlier: Vec<_> = out
            .earlier
            .into_iter()
            .filter(|e| e.cursor > last_cursor)
            .collect();
        if !events.is_empty() || !earlier.is_empty() {
            log.append(std::mem::take(&mut events), std::mem::take(&mut earlier))
                .map_err(|e| e.to_string())?;
        }
    }

    if !core.process_ended() {
        let lost = match wasit_shim::probe_at(dir, run_dir).await {
            Ok(ShimState::Running) => Vec::new(),
            Ok(_) => core.mark_lost("the shim is gone and the journal has no exit record"),
            Err(e) => core.mark_lost(&format!("cannot probe the shim: {e}")),
        };
        let lost: Vec<_> = lost
            .into_iter()
            .filter(|e| e.cursor > last_cursor)
            .collect();
        if !lost.is_empty() {
            log.append(lost, Vec::new()).map_err(|e| e.to_string())?;
        }
    }

    let journal_len = fs::metadata(dir.join("journal.ndjson"))
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    log.seal(core.snapshot().clone(), journal_len, last_seq)
        .map_err(|e| e.to_string())
}

async fn last_cursor(paths: &crate::eventlog::Paths) -> io::Result<u64> {
    async fn read(path: &Path) -> io::Result<u64> {
        let f = match OpenOptions::new().read(true).open(path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        let len = f.metadata().await?.len();
        if len == 0 {
            return Ok(0);
        }
        let mut stdf = f.into_std().await;
        let good = sorted_ndjson::complete_len(&mut stdf, len)?;
        if good == 0 {
            return Ok(0);
        }
        let start = sorted_ndjson::line_start_before(&mut stdf, good)?;
        let line = sorted_ndjson::read_range(&mut stdf, start, good - 1)?;
        let ev: SessionEvent = serde_json::from_slice(&line).map_err(crate::eventlog::json_err)?;
        Ok(ev.cursor)
    }

    let a = read(&paths.events).await?;
    let b = read(&paths.earlier).await?;
    Ok(a.max(b))
}

/// The last `limit` of what `pick` makes of the events with `cursor < before`, oldest first,
/// and whether older ones exist. One replay serves every kind of page: history picks the events
/// themselves, the trail their projection.
#[allow(clippy::too_many_arguments)]
async fn window<T>(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    before: Option<u64>,
    limit: usize,
    earlier: bool,
    mode_hints: &[SessionMode],
    mut pick: impl FnMut(SessionEvent) -> Option<T>,
) -> Result<(Vec<T>, bool), String> {
    let mut core = Core::for_session(desc, meta, Some(mode_hints.to_vec()));
    let mut window: VecDeque<T> = VecDeque::with_capacity(limit.min(4096));
    let mut has_more = false;
    run(dir, &mut core, before.map(cursor_seq), earlier, |e| {
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

/// The last `limit` events with `cursor < before`; with `earlier`, the last `limit` items of the
/// conversation's earlier history instead.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn history(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    before: Option<u64>,
    limit: usize,
    scope: HistoryScope,
    earlier: bool,
    mode_hints: &[SessionMode],
) -> Result<HistoryPage, String> {
    let (events, has_more) = window(dir, desc, meta, before, limit, earlier, mode_hints, |e| {
        scope.admits(&e).then_some(e)
    })
    .await?;
    Ok(HistoryPage { events, has_more })
}

/// The last `limit` trail entries with `cursor < before`: the projection of the same replay.
pub(crate) async fn trail(
    dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    before: Option<u64>,
    limit: usize,
    mode_hints: &[SessionMode],
) -> Result<TrailPage, String> {
    let (entries, has_more) = window(dir, desc, meta, before, limit, false, mode_hints, |e| {
        crate::trail::item(&e)
    })
    .await?;
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
    mode_hints: Vec<SessionMode>,
) -> mpsc::Receiver<Result<SessionEvent, String>> {
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(async move {
        let mut core = Core::for_session(desc, &meta, Some(mode_hints));
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
    let mut core = Core::for_session(desc, meta, None);
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
