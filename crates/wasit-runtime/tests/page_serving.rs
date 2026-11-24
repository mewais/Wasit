//! `session.history` and `session.trail.page` are served from the session's event log: walking
//! pages back to the start gives the full list for every scope (the session's own events,
//! its transcript, one subagent's, the earlier history and the trail), a page holds to its byte
//! budget, and a read of a session that has ended starts no actor.
//!
//! Real shims and the fake claude CLI over its real storage files; the conversation is resumed
//! from a transcript so the session has earlier history, and a background subagent gives the
//! session's own events a subagent.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use serde_json::json;
use wasit_core::{AgentEvent, Command, DriverId};
use wasit_fake_cli::scenario::{BackgroundStep, Scenario, Step, ToolStep, Turn};
use wasit_fake_cli::transcript::{Content, SubagentSpec, Transcript};
use wasit_runtime::wire::{
    page_budget, HistoryRequest, HistoryScope, Phase, ResumeRequest, SessionEvent,
    SessionEventKind, SessionId, TrailItem, TrailRequest, DEFAULT_PAGE_BYTES, MAX_PAGE_BYTES,
};
use wasit_runtime::{DriverOverride, RuntimeConfig, SessionManager};

const ID: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

fn state(d: &Path) -> PathBuf {
    d.join("agent-state")
}

fn claude_config(d: &Path) -> PathBuf {
    state(d).join("claude-config")
}

fn project(d: &Path) -> String {
    std::fs::canonicalize(d)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn runtime_config(d: &Path) -> RuntimeConfig {
    let mut cfg = config(d, timeouts());
    let o: &mut DriverOverride = cfg.overrides.get_mut(&DriverId::new("claude")).unwrap();
    o.env.push((
        "CLAUDE_CONFIG_DIR".into(),
        claude_config(d).to_string_lossy().into_owned(),
    ));
    cfg
}

async fn runtime(d: &Path) -> SessionManager {
    SessionManager::new(runtime_config(d)).await.unwrap()
}

/// A conversation only the agent's storage has: earlier history with a subagent in it.
fn conversation() -> Transcript {
    Transcript::deterministic(5)
        .user("what is in the folder")
        .assistant("let me look")
        .tool("Bash", json!({"command": "ls"}), "alpha beta")
        .assistant("two files")
        .subagent(
            SubagentSpec::new("dig deeper", "look at alpha")
                .content(Content::text("reading alpha"))
                .answer("alpha is fine"),
        )
        .assistant("all checked")
}

fn say(text: &str) -> Step {
    Step::Say {
        text: text.into(),
        chunks: None,
    }
}

fn agent(after_ms: u64, then: Vec<Step>) -> BackgroundStep {
    BackgroundStep {
        kind: "local_agent".into(),
        description: "probe".into(),
        subagent_type: Some("general-purpose".into()),
        after_ms,
        steps: vec![
            Step::Tool(ToolStep {
                name: "Bash".into(),
                input: json!({"command": "echo hi", "description": "Say hi"}),
                result_if_allowed: Some("hi".into()),
                ..Default::default()
            }),
            say("hi from the subagent"),
        ],
        then,
        ..BackgroundStep::default()
    }
}

/// Two ended claude sessions: one resumed from [`conversation`] (earlier history), one that ran
/// a background subagent of its own. Both are stopped and their logs closed (sealed).
async fn record(d: &Path) -> (SessionId, SessionId) {
    let transcript = conversation().write(&claude_config(d), &project(d), ID);
    assert!(transcript.is_file());
    let mgr = runtime(d).await;

    let mut req = create(d, "claude", &Scenario::builder().build());
    req.cwd = Some(project(d));
    req.resume = ResumeRequest::Resume {
        native_session_id: ID.into(),
    };
    req.first_prompt = Some("and now".into());
    let resumed = mgr.create(req).await.unwrap().id;
    wait_snapshot(&mgr, &resumed, "the resumed turn", |s| {
        s.turns_completed >= 1 && s.phase == Phase::Idle
    })
    .await;

    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .background(agent(300, vec![say("OK")]))
                .say("STARTED"),
        )
        .build();
    let live = mgr.create(create(d, "claude", &sc)).await.unwrap().id;
    wait_phase(&mgr, &live, "idle").await;
    mgr.command(&live, Command::prompt("go")).await.unwrap();
    wait_snapshot(&mgr, &live, "the subagent ended", |s| {
        s.turns_completed >= 2 && s.phase == Phase::Idle && s.subagents.len() == 1
    })
    .await;

    for id in [&resumed, &live] {
        mgr.stop(id).await.unwrap();
        wait_snapshot(&mgr, id, "stopped", |s| !s.phase.is_live()).await;
        mgr.close_log(id).await.unwrap();
    }
    (resumed, live)
}

/// The subagent calls the events belong to (the `scoped` events and their task's start).
fn scoped_calls(events: &[SessionEvent]) -> Vec<String> {
    let mut calls: Vec<String> = Vec::new();
    for e in events {
        let SessionEventKind::Agent { event } = &e.kind else {
            continue;
        };
        let inner = match event {
            AgentEvent::History { event, .. } => event.as_ref(),
            other => other,
        };
        if let AgentEvent::Scoped { scope, .. } = inner {
            if !calls.contains(&scope.call_id) {
                calls.push(scope.call_id.clone());
            }
        }
    }
    calls
}

/// The serialized size of a line of the log, as the page reads it.
fn line_len(e: &SessionEvent) -> usize {
    serde_json::to_vec(e).unwrap().len()
}

async fn history(
    mgr: &SessionManager,
    id: &SessionId,
    before: Option<u64>,
    limit: u32,
    scope: HistoryScope,
    earlier: bool,
    max_bytes: Option<u32>,
) -> wasit_runtime::wire::HistoryPage {
    mgr.history_request(HistoryRequest {
        id: id.clone(),
        before,
        limit,
        scope,
        earlier,
        max_bytes,
    })
    .await
    .unwrap()
}

/// The pages of a walk back from the end, newest first. Each page honours `limit`, and the walk
/// ends exactly where `has_more` says the start is reached.
async fn walk(
    mgr: &SessionManager,
    id: &SessionId,
    scope: HistoryScope,
    earlier: bool,
    limit: u32,
    max_bytes: Option<u32>,
) -> Vec<Vec<SessionEvent>> {
    let mut pages = Vec::new();
    let mut before = None;
    loop {
        let page = history(mgr, id, before, limit, scope.clone(), earlier, max_bytes).await;
        assert!(page.events.len() <= limit as usize, "{page:#?}");
        if page.events.is_empty() {
            assert!(!page.has_more, "an empty page must reach the start");
            break;
        }
        before = page.events.first().map(|e| e.cursor);
        let more = page.has_more;
        pages.push(page.events);
        if !more {
            break;
        }
    }
    pages
}

/// The items of a walk, oldest first.
fn oldest_first(pages: Vec<Vec<SessionEvent>>) -> Vec<SessionEvent> {
    pages.into_iter().rev().flatten().collect()
}

async fn trail_walk(
    mgr: &SessionManager,
    id: &SessionId,
    limit: u32,
    max_bytes: Option<u32>,
) -> (Vec<TrailItem>, usize) {
    let mut items: Vec<Vec<TrailItem>> = Vec::new();
    let mut before = None;
    loop {
        let page = mgr
            .trail_request(TrailRequest {
                id: id.clone(),
                before,
                limit,
                max_bytes,
            })
            .await
            .unwrap();
        assert!(page.entries.len() <= limit as usize);
        if page.entries.is_empty() {
            assert!(!page.has_more);
            break;
        }
        before = page.entries.first().map(|e| e.cursor);
        let more = page.has_more;
        items.push(page.entries);
        if !more {
            break;
        }
    }
    let pages = items.len();
    (items.into_iter().rev().flatten().collect(), pages)
}

/// Every scope a session's history is paged in: its own events, its transcript, one subagent's
/// events, and the earlier history with the same filters.
fn cases(all: &[SessionEvent], earlier: &[SessionEvent]) -> Vec<(HistoryScope, bool)> {
    let mut cases = vec![(HistoryScope::All, false), (HistoryScope::Main, false)];
    if let Some(call) = scoped_calls(all).into_iter().next() {
        cases.push((HistoryScope::Subagent { call_id: call }, false));
    }
    // A session without earlier history has no earlier pages to compare.
    if !earlier.is_empty() {
        cases.push((HistoryScope::All, true));
        cases.push((HistoryScope::Main, true));
        if let Some(call) = scoped_calls(earlier).into_iter().next() {
            cases.push((HistoryScope::Subagent { call_id: call }, true));
        }
    }
    cases
}

async fn full(
    mgr: &SessionManager,
    id: &SessionId,
    scope: HistoryScope,
    earlier: bool,
) -> Vec<SessionEvent> {
    history(mgr, id, None, 2000, scope, earlier, None)
        .await
        .events
}

#[tokio::test]
async fn paging_back_to_the_start_gives_the_full_list_in_every_scope() {
    let d = TempDir::new();
    let (resumed, live) = record(d.path()).await;
    let mgr = runtime(d.path()).await;
    mgr.adopt_all().await.unwrap();
    for id in [&resumed, &live] {
        let all = full(&mgr, id, HistoryScope::All, false).await;
        let earlier_all = full(&mgr, id, HistoryScope::All, true).await;
        assert!(all.len() > 6, "{all:#?}");
        for (scope, earlier) in cases(&all, &earlier_all) {
            let expected = full(&mgr, id, scope.clone(), earlier).await;
            assert!(!expected.is_empty(), "{scope:?} earlier={earlier} is empty");
            let pages = walk(&mgr, id, scope.clone(), earlier, 3, Some(400)).await;
            if expected.len() > 3 {
                assert!(pages.len() > 1, "{scope:?} earlier={earlier}: one page");
            }
            assert_eq!(
                oldest_first(pages),
                expected,
                "{scope:?} earlier={earlier}: paged list differs from the full one"
            );
        }
        let trail_full = mgr
            .trail_request(TrailRequest {
                id: id.clone(),
                before: None,
                limit: 2000,
                max_bytes: None,
            })
            .await
            .unwrap()
            .entries;
        assert!(!trail_full.is_empty());
        let (trail_paged, pages) = trail_walk(&mgr, id, 3, Some(300)).await;
        assert!(pages > 1, "the trail walked in one page");
        assert_eq!(
            trail_paged, trail_full,
            "trail pages differ from the full trail"
        );
    }
}

#[tokio::test]
async fn a_page_holds_to_its_byte_budget() {
    let d = TempDir::new();
    let (resumed, live) = record(d.path()).await;
    let mgr = runtime(d.path()).await;
    mgr.adopt_all().await.unwrap();
    for id in [&resumed, &live] {
        let all = full(&mgr, id, HistoryScope::All, false).await;
        let earlier_all = full(&mgr, id, HistoryScope::All, true).await;
        for (scope, earlier) in cases(&all, &earlier_all) {
            let expected = full(&mgr, id, scope.clone(), earlier).await;
            let Some(largest) = expected.iter().map(line_len).max() else {
                continue;
            };
            for budget in [1u32, 200, 1000, 4000] {
                let pages = walk(&mgr, id, scope.clone(), earlier, 2000, Some(budget)).await;
                for page in &pages {
                    // A page stops once it has read `budget` bytes, after at least one item: it
                    // exceeds the budget by at most the event that crossed it.
                    let bytes: usize = page.iter().map(line_len).sum();
                    assert!(
                        bytes <= budget as usize + largest,
                        "{scope:?} earlier={earlier} budget={budget}: page of {bytes} bytes"
                    );
                    assert!(!page.is_empty());
                }
                if budget == 1 {
                    assert!(
                        pages.iter().all(|p| p.len() == 1),
                        "{scope:?} earlier={earlier}: a 1-byte budget holds one item per page"
                    );
                }
                assert_eq!(oldest_first(pages), expected, "{scope:?} budget={budget}");
            }
        }
    }
}

#[tokio::test]
async fn a_read_of_an_ended_session_starts_no_actor() {
    let d = TempDir::new();
    let (resumed, live) = record(d.path()).await;
    let mgr = runtime(d.path()).await;
    mgr.adopt_all().await.unwrap();
    for id in [&resumed, &live] {
        assert!(!mgr.actor_running(id));
        let page = history(&mgr, id, None, 2000, HistoryScope::All, false, None).await;
        assert!(!page.events.is_empty());
        let earlier = history(&mgr, id, None, 2000, HistoryScope::All, true, Some(64)).await;
        assert!(earlier.events.len() <= 2000);
        let trail = mgr
            .trail_request(TrailRequest {
                id: id.clone(),
                before: None,
                limit: 5,
                max_bytes: Some(128),
            })
            .await
            .unwrap();
        assert!(!trail.entries.is_empty());
        assert!(!mgr.actor_running(id), "a read started an actor for {id}");
    }
}

#[tokio::test]
async fn a_log_missing_on_disk_is_rebuilt_by_a_read_without_an_actor() {
    let d = TempDir::new();
    let (resumed, _live) = record(d.path()).await;
    let expected_all;
    let expected_earlier;
    {
        // The sealed log is what the first daemon left; a read here serves it.
        let mgr = runtime(d.path()).await;
        mgr.adopt_all().await.unwrap();
        expected_all = full(&mgr, &resumed, HistoryScope::All, false).await;
        expected_earlier = full(&mgr, &resumed, HistoryScope::All, true).await;
    }
    assert!(!expected_earlier.is_empty());
    // The derived files are only a cache of the journal: without them the first read builds
    // the log, and still starts no actor.
    std::fs::remove_dir_all(
        d.path()
            .join("sessions")
            .join(resumed.as_str())
            .join("derived"),
    )
    .unwrap();
    let mgr = runtime(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let rebuilt = oldest_first(walk(&mgr, &resumed, HistoryScope::All, false, 7, Some(300)).await);
    assert_eq!(rebuilt, expected_all);
    let rebuilt_earlier =
        oldest_first(walk(&mgr, &resumed, HistoryScope::All, true, 7, Some(300)).await);
    assert_eq!(rebuilt_earlier, expected_earlier);
    assert!(!mgr.actor_running(&resumed));
}

#[test]
fn the_page_budget_defaults_and_clamps() {
    assert_eq!(DEFAULT_PAGE_BYTES, 1 << 20);
    assert_eq!(MAX_PAGE_BYTES, 4 << 20);
    assert_eq!(page_budget(None), 1 << 20);
    assert_eq!(page_budget(Some(0)), 1);
    assert_eq!(page_budget(Some(256 << 10)), 256 << 10);
    assert_eq!(page_budget(Some(u32::MAX)), 4 << 20);
}
