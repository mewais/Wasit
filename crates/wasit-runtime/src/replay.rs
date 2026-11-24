//! Offline, deterministic replays of a session journal with a fresh [`Core`]: the build of a
//! session's event log when no actor owns it (which also yields the session's snapshot), and the
//! lookup of a blob missing from the blob store. Pages and subscriptions are read from the event
//! log (see [`crate::eventlog`]), never from a replay.
//!
//! Cost: a replay reads the journal from seq 1 (protocol state depends on everything before)
//! and runs the pure core over it — O(journal size) CPU and I/O.

use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio::fs::{self, OpenOptions};
use wasit_core::{DriverDescriptor, SessionMode};
use wasit_shim::{journal::sorted_ndjson, ShimState};

use crate::core::{Core, SessionMeta};
use crate::eventlog::EventLog;
use crate::wire::{SessionEvent, SessionSnapshot};

/// Build an event log from the journal, appending only cursors beyond the current log and sealing
/// it when complete; returns the snapshot it sealed. Used by the manager for logs no actor owns.
pub(crate) async fn build(
    dir: &Path,
    run_dir: &Path,
    desc: Arc<dyn DriverDescriptor>,
    meta: &SessionMeta,
    mode_hints: &[SessionMode],
    log: &EventLog,
) -> Result<SessionSnapshot, String> {
    let last_cursor = last_cursor(&log.paths()).await.map_err(|e| e.to_string())?;
    let mut core = Core::for_session(desc, meta, Some(mode_hints.to_vec()));
    let mut last_seq = 0u64;
    let mut stream = match wasit_shim::read_journal(dir, 1).await {
        Ok(s) => s,
        Err(wasit_shim::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(core.snapshot().clone())
        }
        Err(e) => return Err(format!("read journal: {e}")),
    };

    let mut n = 0u32;
    while let Some(item) = stream.next().await {
        let rec = item.map_err(|e| format!("read journal: {e}"))?;
        last_seq = rec.seq;
        crate::stats::journal_record_replayed();
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
        n = n.wrapping_add(1);
        if n.is_multiple_of(256) {
            // Stay fair to other sessions while replaying a long journal.
            tokio::task::yield_now().await;
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
    let snapshot = core.snapshot().clone();
    log.seal(snapshot.clone(), journal_len, last_seq)
        .map_err(|e| e.to_string())?;
    Ok(snapshot)
}

pub(crate) async fn last_cursor(paths: &crate::eventlog::Paths) -> io::Result<u64> {
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
        crate::stats::journal_record_replayed();
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
