//! What opening a session costs (R3): one page of its event log, not its journal.
//!
//! * A fresh manager adopts a sealed session of N and one of 10N records; its latest page and a
//!   subscription from that page's end replay no journal record, start no actor, and read at
//!   most the page's byte budget plus 128 KiB of the log, for both lengths.
//! * A subscription to an ended session (no actor) receives the events of a later continue.
//! * A sealed session that is continued and then killed is consistent after a restart.
//!
//! The work counters ([`wasit_runtime::stats`]) are process-wide, so the tests of this file run
//! one at a time.

mod common;

use std::path::Path;
use std::time::Duration;

use common::*;
use wasit_core::Command;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::stats::{counters, Counters};
use wasit_runtime::wire::{HistoryRequest, HistoryScope, SessionId};

/// Records of the short session's turn; the long one has ten times as many.
const N: usize = 1_000;
/// The byte budget the page asks for.
const PAGE_BYTES: u32 = 64 * 1024;
/// Allowed beyond the budget (the last record of a page, the subscription's seek).
const SLACK: u64 = 128 * 1024;
/// Large enough that the byte budget, not the count, ends the page.
const PAGE_LIMIT: u32 = 2_000;

/// Serializes the tests of this file (see the module docs).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// One turn of `chunks` text chunks (one journal record each), then a stop; returns once the
/// session's log is sealed and its manager dropped.
async fn record(d: &Path, chunks: usize) -> SessionId {
    let mgr = manager(d).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say_chunked("x".repeat(chunks), chunks))
        .turn(Turn::builder().say("again"))
        .build();
    let id = mgr.create(create(d, "codex", &sc)).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    wait_snapshot(&mgr, &id, "the turn", |s| s.turns_completed == 1).await;
    mgr.stop(&id).await.unwrap();
    mgr.close_log(&id).await.unwrap();
    id
}

fn since(now: Counters, before: Counters) -> Counters {
    Counters {
        actors_started: now.actors_started - before.actors_started,
        journal_records_replayed: now.journal_records_replayed - before.journal_records_replayed,
        log_bytes_read: now.log_bytes_read - before.log_bytes_read,
    }
}

#[tokio::test]
async fn opening_a_sealed_session_costs_one_page_whatever_its_length() {
    let _serial = SERIAL.lock().await;
    let mut log_sizes = Vec::new();
    for n in [N, 10 * N] {
        let d = TempDir::new();
        let id = record(d.path(), n).await;
        let mgr = manager(d.path()).await;
        let before = counters();
        mgr.adopt_all().await.unwrap();
        let snap = mgr.snapshot(&id).unwrap();
        assert!(snap.loaded, "{n}: a sealed session is loaded at adoption");
        assert_eq!(snap.turns_completed, 1, "{n}: the sealed snapshot");
        let page = mgr
            .history_request(HistoryRequest {
                id: id.clone(),
                before: None,
                limit: PAGE_LIMIT,
                scope: HistoryScope::All,
                earlier: false,
                max_bytes: Some(PAGE_BYTES),
            })
            .await
            .unwrap();
        assert!(page.has_more, "{n}: older events remain");
        let end = page.events.last().unwrap().cursor;
        assert_eq!(end, snap.cursor, "{n}: the page ends at the snapshot");
        let mut s = mgr.subscribe(&id, Some(end)).await.unwrap();
        // Nothing follows the end of an ended session until an actor starts again.
        let idle = tokio::time::timeout(Duration::from_millis(300), s.next()).await;
        assert!(idle.is_err(), "{n}: the subscription waits: {idle:?}");
        let cost = since(counters(), before);
        assert_eq!(cost.journal_records_replayed, 0, "{n}: records replayed");
        assert_eq!(cost.actors_started, 0, "{n}: actors started");
        assert!(cost.log_bytes_read > 0, "{n}: the page read the log");
        assert!(
            cost.log_bytes_read <= PAGE_BYTES as u64 + SLACK,
            "{n}: {} bytes of the log read",
            cost.log_bytes_read
        );
        assert!(!mgr.actor_running(&id));
        let events = d
            .path()
            .join("sessions")
            .join(id.as_str())
            .join("derived/events.ndjson");
        log_sizes.push(std::fs::metadata(events).unwrap().len());
    }
    // The long log is far beyond the budget: the bound is not met by reading it whole.
    assert!(
        log_sizes[1] > 4 * (PAGE_BYTES as u64 + SLACK),
        "log sizes {log_sizes:?}"
    );
}

#[tokio::test]
async fn a_subscription_to_an_ended_session_receives_a_later_continue() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), 20).await;
    let mgr = manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let all = mgr.history(&id, None, 2_000).await.unwrap().events;
    let from = all[all.len() / 2].cursor;

    // From the middle: the log's rest first, then whatever the continue brings, gap-free.
    let mut s = mgr.subscribe(&id, Some(from)).await.unwrap();
    assert!(!mgr.actor_running(&id), "a subscription started an actor");
    let rest: Vec<_> = all.iter().filter(|e| e.cursor > from).cloned().collect();
    let head = until(&mut s, |e| e.cursor == all.last().unwrap().cursor).await;
    assert_eq!(head, rest);
    let follower = tokio::spawn(async move {
        let got = until(&mut s, is_turn_completed).await;
        (got, s)
    });

    mgr.continue_session(&id).await.unwrap();
    wait_phase(&mgr, &id, "idle").await;
    mgr.command(&id, Command::prompt("again")).await.unwrap();
    let (got, _s) = follower.await.unwrap();
    let done = wait_snapshot(&mgr, &id, "two turns", |s| s.turns_completed == 2).await;
    let history: Vec<_> = mgr
        .history(&id, None, 2_000)
        .await
        .unwrap()
        .events
        .into_iter()
        .filter(|e| e.cursor > all.last().unwrap().cursor && e.cursor <= got.last().unwrap().cursor)
        .collect();
    assert_eq!(
        got, history,
        "exactly the events after the subscription's start"
    );
    assert!(got.last().unwrap().cursor <= done.cursor);
    mgr.stop(&id).await.unwrap();
}

#[tokio::test]
async fn a_sealed_session_continued_and_killed_is_consistent_after_a_restart() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), 20).await;

    let before = {
        let mgr = manager(d.path()).await;
        mgr.adopt_all().await.unwrap();
        mgr.continue_session(&id).await.unwrap();
        wait_phase(&mgr, &id, "idle").await;
        mgr.command(&id, Command::prompt("again")).await.unwrap();
        let snap = wait_snapshot(&mgr, &id, "the second turn", |s| {
            s.turns_completed == 2 && s.pid.is_some()
        })
        .await;
        let before = mgr.history(&id, None, 2_000).await.unwrap().events;
        kill_pid(snap.pid.unwrap() as u64);
        wait_snapshot(&mgr, &id, "ended", |s| !s.phase.is_live()).await;
        mgr.close_log(&id).await.unwrap();
        before
    };

    let mgr = manager(d.path()).await;
    let start = counters();
    mgr.adopt_all().await.unwrap();
    let snap = mgr.snapshot(&id).unwrap();
    assert!(snap.loaded && !snap.phase.is_live(), "{snap:?}");
    let after = mgr.history(&id, None, 2_000).await.unwrap().events;
    assert_eq!(
        &after[..before.len()],
        &before[..],
        "what was seen is a prefix"
    );
    assert!(after.len() > before.len(), "the end is recorded after it");
    assert_eq!(after.last().unwrap().cursor, snap.cursor);
    // The pages and the stream of the restarted session agree.
    let mut all = mgr.subscribe(&id, None).await.unwrap();
    let streamed = until(&mut all, |e| e.cursor >= snap.cursor).await;
    assert_eq!(streamed, after);
    let cost = since(counters(), start);
    assert_eq!(cost.journal_records_replayed, 0, "sealed again at the exit");
    assert_eq!(cost.actors_started, 0);
}
