//! Process-wide work counters: how many session actors were started, how many journal records
//! an offline replay applied, and how many bytes of event-log records readers consumed. The
//! open-cost tests (`tests/open_cost.rs`) compare readings taken before and after an operation.
//! Not API.
//!
//! The counters are global to the process: a test that reads them must not run beside another
//! test of the same binary that does the same kind of work.

use std::sync::atomic::{AtomicU64, Ordering};

static ACTORS_STARTED: AtomicU64 = AtomicU64::new(0);
static JOURNAL_RECORDS_REPLAYED: AtomicU64 = AtomicU64::new(0);
static LOG_BYTES_READ: AtomicU64 = AtomicU64::new(0);

/// A reading of the counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    /// Session actors started.
    pub actors_started: u64,
    /// Journal records applied by an offline replay (log builds, blob lookups). An actor's own
    /// catch-up is counted by `actors_started`.
    pub journal_records_replayed: u64,
    /// Bytes of event-log records a page or a subscription consumed. The backward page scan
    /// reads in blocks, so the bytes read from disk exceed this by at most one 64 KiB block
    /// (plus the binary search of a `before` cursor).
    pub log_bytes_read: u64,
}

/// The counters now.
pub fn counters() -> Counters {
    Counters {
        actors_started: ACTORS_STARTED.load(Ordering::Relaxed),
        journal_records_replayed: JOURNAL_RECORDS_REPLAYED.load(Ordering::Relaxed),
        log_bytes_read: LOG_BYTES_READ.load(Ordering::Relaxed),
    }
}

pub(crate) fn actor_started() {
    ACTORS_STARTED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn journal_record_replayed() {
    JOURNAL_RECORDS_REPLAYED.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn log_bytes_read(n: u64) {
    LOG_BYTES_READ.fetch_add(n, Ordering::Relaxed);
}
