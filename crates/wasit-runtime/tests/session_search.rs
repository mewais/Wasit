//! `session.search` (B6b): hits over a session's derived event log.
//!
//! A sealed session is recorded with the fake CLI; its derived log is then extended with
//! crafted records (live and earlier, main and subagent, long, torn) that the search must see
//! exactly as written. The searches run through a fresh manager, so the log is read as a
//! reopened session's would be. The work counters are process-wide, so the tests of this file
//! run one at a time.

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};

use common::*;
use wasit_core::{AgentEvent, Command, Scope};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::stats::counters;
use wasit_runtime::wire::{
    HistoryScope, SearchDirection, SearchHit, SearchPage, SearchPart, SearchRequest, SessionEvent,
    SessionEventKind, SessionId,
};
use wasit_runtime::SessionManager;

/// Serializes the tests of this file (see the module docs).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The search budget of the runtime (`session.search` examines at most this much per page).
const BUDGET: u64 = 8 << 20;

/// Records one short turn, then closes the session's log; returns the session's id.
async fn record(d: &Path) -> SessionId {
    let mgr = manager(d).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("hello"))
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

/// A fresh manager over the directory, with its sessions adopted (none is started).
async fn reopened(d: &Path) -> SessionManager {
    let mgr = manager(d).await;
    mgr.adopt_all().await.unwrap();
    mgr
}

fn derived(d: &Path, id: &SessionId, file: &str) -> PathBuf {
    d.join("sessions")
        .join(id.as_str())
        .join("derived")
        .join(file)
}

/// The cursor of the last record of `path`.
fn last_cursor(path: &Path) -> u64 {
    let text = std::fs::read_to_string(path).unwrap();
    let line = text.lines().rev().find(|l| !l.trim().is_empty()).unwrap();
    serde_json::from_str::<SessionEvent>(line).unwrap().cursor
}

/// Appends `events` as lines of `path`; cursors are the caller's.
fn append(path: &Path, events: &[SessionEvent]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    for e in events {
        let mut line = serde_json::to_vec(e).unwrap();
        line.push(b'\n');
        f.write_all(&line).unwrap();
    }
}

/// Records of `n` notices after the log's last cursor, numbered from `first`.
fn notices(path: &Path, first: u64, messages: impl Iterator<Item = String>) -> Vec<SessionEvent> {
    let base = last_cursor(path);
    messages
        .enumerate()
        .map(|(i, message)| {
            let cursor = base + first + i as u64;
            notice(cursor, message)
        })
        .collect()
}

/// A notice record, built from its wire form (the kind is not constructible from here).
fn notice(cursor: u64, message: String) -> SessionEvent {
    serde_json::from_value(serde_json::json!({
        "cursor": cursor,
        "t": cursor * 10,
        "kind": "notice",
        "message": message,
    }))
    .unwrap()
}

fn agent(cursor: u64, event: AgentEvent) -> SessionEvent {
    SessionEvent {
        cursor,
        t: cursor * 10,
        kind: SessionEventKind::Agent { event },
    }
}

fn delta(text: &str) -> AgentEvent {
    AgentEvent::AssistantDelta {
        text: text.into(),
        message_id: None,
    }
}

fn request(id: &SessionId, query: &str) -> SearchRequest {
    SearchRequest {
        session: id.clone(),
        query: query.into(),
        scope: HistoryScope::All,
        kinds: None,
        direction: SearchDirection::Backward,
        anchor: None,
        resume: None,
        part: SearchPart::Live,
        limit: 50,
    }
}

async fn search(mgr: &SessionManager, req: SearchRequest) -> SearchPage {
    mgr.search_request(req).await.unwrap()
}

/// Every hit of a search, following `next` page by page with `limit` hits a page; also the
/// number of pages.
async fn walk(mgr: &SessionManager, mut req: SearchRequest, limit: u32) -> (Vec<SearchHit>, u32) {
    req.limit = limit;
    let mut hits = Vec::new();
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 10_000, "the walk does not end");
        let page = search(mgr, req.clone()).await;
        assert!(page.hits.len() <= limit as usize, "a page over its limit");
        hits.extend(page.hits);
        match page.next {
            Some(token) => req.resume = Some(token),
            None => return (hits, pages),
        }
    }
}

fn cursors(hits: &[SearchHit]) -> Vec<u64> {
    hits.iter().map(|h| h.cursor).collect()
}

#[tokio::test]
async fn hits_come_from_the_live_and_the_earlier_logs() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    let earlier = derived(d.path(), &id, "earlier.ndjson");
    let live_events = notices(&live, 1, ["needle live".to_owned()].into_iter());
    append(&live, &live_events);
    let base = last_cursor(&live);
    let history = SessionEvent {
        cursor: base + 1,
        t: 1,
        kind: SessionEventKind::Agent {
            event: AgentEvent::History {
                at_ms: None,
                event: Box::new(AgentEvent::UserMessage {
                    text: "a needle earlier".into(),
                }),
            },
        },
    };
    append(&earlier, std::slice::from_ref(&history));

    let mgr = reopened(d.path()).await;
    let live_page = search(&mgr, request(&id, "NEEDLE")).await;
    assert_eq!(cursors(&live_page.hits), [base], "the live log's hit");
    assert_eq!(live_page.hits[0].part, SearchPart::Live);
    assert_eq!(live_page.hits[0].field, "notice");
    assert!(live_page.next.is_none());

    let mut req = request(&id, "needle");
    req.part = SearchPart::Earlier;
    let earlier_page = search(&mgr, req).await;
    assert_eq!(cursors(&earlier_page.hits), [history.cursor]);
    assert_eq!(earlier_page.hits[0].part, SearchPart::Earlier);
    assert_eq!(earlier_page.hits[0].field, "user_text");
    assert_eq!(earlier_page.hits[0].snippet, "a needle earlier");
    assert_eq!(
        (earlier_page.hits[0].start, earlier_page.hits[0].len),
        (2, 6)
    );
    assert!(!mgr.actor_running(&id));
}

#[tokio::test]
async fn scope_separates_the_main_transcript_from_a_subagent() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    let base = last_cursor(&live);
    let main = agent(base + 1, delta("needle main"));
    let sub = agent(
        base + 2,
        AgentEvent::Scoped {
            scope: Scope {
                call_id: "c9".into(),
                agent_id: None,
            },
            event: Box::new(delta("needle sub")),
        },
    );
    let other = agent(
        base + 3,
        AgentEvent::Scoped {
            scope: Scope {
                call_id: "c8".into(),
                agent_id: None,
            },
            event: Box::new(delta("needle other")),
        },
    );
    append(&live, &[main.clone(), sub.clone(), other.clone()]);

    let mgr = reopened(d.path()).await;
    let with = |scope: HistoryScope| {
        let mut r = request(&id, "needle");
        r.scope = scope;
        r
    };
    let all = search(&mgr, with(HistoryScope::All)).await;
    assert_eq!(cursors(&all.hits), [base + 3, base + 2, base + 1]);

    let main_hits = search(&mgr, with(HistoryScope::Main)).await;
    assert_eq!(cursors(&main_hits.hits), [base + 1], "no subagent row");

    let sub_hits = search(
        &mgr,
        with(HistoryScope::Subagent {
            call_id: "c9".into(),
        }),
    )
    .await;
    assert_eq!(cursors(&sub_hits.hits), [base + 2], "only its own subagent");
    assert_eq!(sub_hits.hits[0].snippet, "needle sub");
    assert!(!mgr.actor_running(&id));
}

#[tokio::test]
async fn resuming_a_search_repeats_no_hit_and_skips_none() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    // Forty notices with two hits each, and one with three: a page can end inside a record.
    let mut events = notices(
        &live,
        1,
        (0..40).map(|i| format!("needle {i} and needle {i}")),
    );
    events.push(notice(
        last_cursor(&live) + 41,
        "needle x needle y needle z".into(),
    ));
    append(&live, &events);

    let mgr = reopened(d.path()).await;
    let (full, _) = walk(&mgr, request(&id, "needle"), 50).await;
    assert_eq!(full.len(), 40 * 2 + 3);
    let key = |h: &SearchHit| (h.cursor, h.snippet.clone(), h.start);
    for direction in [SearchDirection::Backward, SearchDirection::Forward] {
        let mut req = request(&id, "needle");
        req.direction = direction;
        let (whole, _) = walk(&mgr, req.clone(), 50).await;
        let mut got: Vec<_> = whole.iter().map(key).collect();
        let mut want: Vec<_> = full.iter().map(key).collect();
        got.sort();
        want.sort();
        assert_eq!(got, want, "{direction:?}: every hit once");
        for limit in [1, 2, 3, 7, 49] {
            let (paged, pages) = walk(&mgr, req.clone(), limit).await;
            assert!(pages > 1, "limit {limit} gives several pages");
            assert_eq!(
                paged.iter().map(key).collect::<Vec<_>>(),
                whole.iter().map(key).collect::<Vec<_>>(),
                "{direction:?} limit {limit}: no duplicate and no gap"
            );
        }
    }

    // An anchor is exclusive: the hits strictly before it, in the order of the backward walk.
    let anchor = full[10].cursor;
    let mut req = request(&id, "needle");
    req.anchor = Some(anchor);
    let before = search(&mgr, req).await;
    let expect: Vec<u64> = full
        .iter()
        .filter(|h| h.cursor < anchor)
        .map(|h| h.cursor)
        .take(50)
        .collect();
    assert!(!expect.is_empty());
    assert_eq!(cursors(&before.hits), expect);
    assert!(!mgr.actor_running(&id));
}

#[tokio::test]
async fn the_budget_ends_a_page_with_a_token_and_the_token_finds_the_rest() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    let long = "x".repeat(100_000);
    // The only hit is in the oldest of 100 records of 100 KB: a backward search from the end
    // meets the budget (8 MiB) before it.
    let events = notices(
        &live,
        1,
        (0..100).map(|i| {
            if i == 0 {
                format!("needle {long}")
            } else {
                long.clone()
            }
        }),
    );
    append(&live, &events);

    let mgr = reopened(d.path()).await;
    let first = search(&mgr, request(&id, "needle")).await;
    assert!(first.hits.is_empty(), "the budget ends the page first");
    let token = first.next.clone().expect("a token, not the end");
    assert!(first.scanned_bytes >= BUDGET, "{}", first.scanned_bytes);
    assert!(
        first.scanned_bytes < BUDGET + 2 * 100_100,
        "one record over the budget at most: {}",
        first.scanned_bytes
    );

    let mut req = request(&id, "needle");
    req.resume = Some(token);
    let rest = search(&mgr, req).await;
    assert_eq!(cursors(&rest.hits), [events[0].cursor]);
    assert!(rest.next.is_none());
    assert!(!mgr.actor_running(&id));
}

#[tokio::test]
async fn a_torn_last_line_is_not_searched() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    let whole = notices(&live, 1, ["needle whole".to_owned()].into_iter());
    append(&live, &whole);
    let torn =
        serde_json::to_string(&notice(last_cursor(&live) + 1, "needle torn".into())).unwrap();
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&live)
        .unwrap();
    f.write_all(&torn.as_bytes()[..torn.len() - 6]).unwrap();
    drop(f);

    let mgr = reopened(d.path()).await;
    let (hits, _) = walk(&mgr, request(&id, "needle"), 50).await;
    assert_eq!(
        cursors(&hits),
        [whole[0].cursor],
        "the torn record is not a hit"
    );
}

#[tokio::test]
async fn a_search_reads_the_log_and_starts_no_actor() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    append(
        &live,
        &notices(&live, 1, ["needle one".to_owned()].into_iter()),
    );

    let mgr = reopened(d.path()).await;
    let before = counters();
    let page = search(&mgr, request(&id, "needle")).await;
    let now = counters();
    assert_eq!(page.hits.len(), 1);
    assert_eq!(
        now.actors_started, before.actors_started,
        "an actor started"
    );
    assert_eq!(
        now.journal_records_replayed, before.journal_records_replayed,
        "the journal was replayed"
    );
    assert!(
        now.log_bytes_read > before.log_bytes_read,
        "the log was read"
    );
    assert!(!mgr.actor_running(&id));
}

#[tokio::test]
async fn case_folding_keeps_character_offsets_in_multibyte_text() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path()).await;
    let live = derived(d.path(), &id, "events.ndjson");
    append(
        &live,
        &notices(
            &live,
            1,
            ["Ünïcode café CAFÉ ßtraße".to_owned()].into_iter(),
        ),
    );

    let mgr = reopened(d.path()).await;
    let page = search(&mgr, request(&id, "café")).await;
    assert_eq!(page.hits.len(), 2);
    for hit in &page.hits {
        let chars: Vec<char> = hit.snippet.chars().collect();
        let (s, l) = (hit.start as usize, hit.len as usize);
        let matched: String = chars[s..s + l].iter().collect();
        assert_eq!(matched.to_lowercase(), "café", "{hit:?}");
    }
    // Characters, not bytes: `Ü` and `é` take two bytes each, the offsets count one.
    assert_eq!((page.hits[0].start, page.hits[0].len), (8, 4), "café");
    assert_eq!((page.hits[1].start, page.hits[1].len), (13, 4), "CAFÉ");
    let page = search(&mgr, request(&id, "ßtr")).await;
    assert_eq!((page.hits[0].start, page.hits[0].len), (18, 3));
}
