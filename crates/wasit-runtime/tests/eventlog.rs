//! Event log derivations: the actor's derived log (`derived/events.ndjson`,
//! `derived/earlier.ndjson`, `derived/meta.json`) match an offline rebuild from the
//! journal, stay sealed with the correct journal length on exit, unseal while a
//! continued session runs, and reseal identically after it ends again. Scenarios
//! cover every driver and include earlier history, background/subagent traffic,
//! compaction in the earlier transcript and a lost shim.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use serde::Deserialize;
use serde_json::json;
use wasit_core::Command;
use wasit_fake_cli::scenario::{BackgroundStep, Scenario, Step, Turn};
use wasit_fake_cli::transcript::{Content, SubagentSpec, Transcript};
use wasit_runtime::testing::build_event_log;
use wasit_runtime::wire::{HistoryScope, Phase, ResumeRequest, SessionEvent, SessionId};
use wasit_runtime::{SessionManager, MAX_HISTORY_PAGE};

const ID: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

#[derive(Debug, Deserialize)]
struct Meta {
    #[serde(default)]
    seal: Option<Seal>,
}

#[derive(Debug, Deserialize)]
struct Seal {
    journal_len: u64,
    last_seq: u64,
}

struct LogPaths {
    events: PathBuf,
    earlier: PathBuf,
    meta: PathBuf,
    journal: PathBuf,
}

fn log_paths(root: &Path, id: &SessionId) -> LogPaths {
    LogPaths {
        earlier: root
            .join("sessions")
            .join(id.as_str())
            .join("derived")
            .join("earlier.ndjson"),
        events: root
            .join("sessions")
            .join(id.as_str())
            .join("derived")
            .join("events.ndjson"),
        meta: root
            .join("sessions")
            .join(id.as_str())
            .join("derived")
            .join("meta.json"),
        journal: root
            .join("sessions")
            .join(id.as_str())
            .join("journal.ndjson"),
    }
}

fn read_bytes(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap_or_default()
}

fn to_ndjson(events: &[SessionEvent]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in events {
        let line = serde_json::to_vec(e).unwrap();
        out.extend_from_slice(&line);
        out.push(b'\n');
    }
    out
}

fn diff_lines(a: &[u8], b: &[u8]) -> Option<(usize, String, String)> {
    let a_lines: Vec<_> = a.split(|c| *c == b'\n').collect();
    let b_lines: Vec<_> = b.split(|c| *c == b'\n').collect();
    let len = a_lines.len().max(b_lines.len());
    for i in 0..len {
        let aa = a_lines.get(i).copied().unwrap_or_default();
        let bb = b_lines.get(i).copied().unwrap_or_default();
        if aa != bb {
            let truncate = |v: &[u8]| {
                let s = String::from_utf8_lossy(v);
                let mut s = s.to_string();
                if s.len() > 300 {
                    // Cut on a char boundary: `truncate` panics elsewhere.
                    let cut = (0..=300)
                        .rev()
                        .find(|&i| s.is_char_boundary(i))
                        .unwrap_or(0);
                    s.truncate(cut);
                    s.push('…');
                }
                s
            };
            return Some((i + 1, truncate(aa), truncate(bb)));
        }
    }
    None
}

async fn collect_history(mgr: &SessionManager, id: &SessionId, earlier: bool) -> Vec<SessionEvent> {
    let mut pages: Vec<Vec<SessionEvent>> = Vec::new();
    let mut before = None;
    loop {
        let page = if earlier {
            mgr.earlier_scoped(id, before, MAX_HISTORY_PAGE, HistoryScope::All)
                .await
                .unwrap()
        } else {
            mgr.history(id, before, MAX_HISTORY_PAGE).await.unwrap()
        };
        if page.events.is_empty() {
            break;
        }
        before = page.events.first().map(|e| e.cursor);
        let more = page.has_more;
        pages.push(page.events);
        if !more {
            break;
        }
    }
    pages.into_iter().rev().flatten().collect()
}

async fn rebuild_offline(dir: &Path, id: &SessionId) -> (Vec<u8>, Vec<u8>) {
    let mgr = manager(dir).await;
    mgr.adopt_all().await.unwrap();
    let events = collect_history(&mgr, id, false).await;
    let earlier = collect_history(&mgr, id, true).await;
    (to_ndjson(&events), to_ndjson(&earlier))
}

/// The actor's sealed log against [`build_event_log`]: byte-identical files and the same seal.
async fn assert_built_log_matches(label: &str, mgr: &SessionManager, dir: &Path, id: &SessionId) {
    let paths = log_paths(dir, id);
    let live_meta = read_meta(&paths.meta);
    let live_seal = live_meta.seal.as_ref().expect("live log sealed");

    let out = TempDir::new();
    let session_dir = dir.join("sessions").join(id.as_str());
    build_event_log(mgr.config(), &session_dir, out.path())
        .await
        .unwrap_or_else(|e| panic!("{label}: offline build failed: {e}"));
    let out_derived = out.path().join("derived");
    let built_meta = read_meta(&out_derived.join("meta.json"));
    let built_seal = built_meta.seal.as_ref().expect("built log sealed");

    assert_same_bytes(
        &format!("events {label} (built)"),
        &read_bytes(&paths.events),
        &read_bytes(&out_derived.join("events.ndjson")),
    );
    assert_same_bytes(
        &format!("earlier {label} (built)"),
        &read_bytes(&paths.earlier),
        &read_bytes(&out_derived.join("earlier.ndjson")),
    );
    assert_eq!(
        live_seal.journal_len, built_seal.journal_len,
        "{label}: seal journal_len differs from build"
    );
    assert_eq!(
        live_seal.last_seq, built_seal.last_seq,
        "{label}: seal last_seq differs from build"
    );
}

fn assert_same_bytes(what: &str, live: &[u8], built: &[u8]) {
    if live == built {
        return;
    }
    match diff_lines(live, built) {
        Some((line, a, b)) => panic!("{what}: line {line} differs\nlive: {a}\nbuilt: {b}"),
        None => panic!("{what}: logs differ"),
    }
}

async fn assert_logs_match(label: &str, mgr: &SessionManager, dir: &Path, id: &SessionId) {
    assert_built_log_matches(label, mgr, dir, id).await;
    let paths = log_paths(dir, id);
    let live_events = read_bytes(&paths.events);
    let live_earlier = read_bytes(&paths.earlier);
    let (off_events, off_earlier) = rebuild_offline(dir, id).await;

    if live_events != off_events {
        if let Some((line, a, b)) = diff_lines(&live_events, &off_events) {
            panic!(
                "events {}: line {} differs\nlive: {}\noff : {}",
                label, line, a, b
            );
        }
        panic!("events {}: logs differ", label);
    }
    if live_earlier != off_earlier {
        if let Some((line, a, b)) = diff_lines(&live_earlier, &off_earlier) {
            panic!(
                "earlier {}: line {} differs\nlive: {}\noff : {}",
                label, line, a, b
            );
        }
        panic!("earlier {}: logs differ", label);
    }
}

fn project(d: &Path) -> String {
    std::fs::canonicalize(d)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn claude_config(d: &Path) -> PathBuf {
    d.join("agent-state").join("claude-config")
}

fn write_claude_transcript(d: &Path) {
    let transcript = Transcript::deterministic(4)
        .user("what is here")
        .assistant("checking")
        .tool("Bash", json!({"command": "ls"}), "alpha beta")
        .assistant("two files")
        .subagent(
            SubagentSpec::new("probe", "look")
                .content(Content::text("subagent looking"))
                .answer("done"),
        )
        .compaction()
        .assistant("post compaction");
    let path = transcript.write(&claude_config(d), &project(d), ID);
    assert!(path.is_file());
}

fn background_task() -> BackgroundStep {
    BackgroundStep {
        kind: "local_agent".into(),
        description: "bg subagent".into(),
        after_ms: 20,
        steps: vec![Step::Say {
            text: "background running".into(),
            chunks: None,
        }],
        status: Some("completed".into()),
        ..Default::default()
    }
}

fn base_turn(driver: &str) -> Turn {
    let turn = Turn::builder()
        .say("hello")
        .think("thinking")
        .tool("Bash", json!({"command": "echo hi"}))
        .result_if_allowed("hi")
        .done()
        .say("done");
    if driver == "claude" {
        turn.background(background_task()).into()
    } else {
        turn.into()
    }
}

async fn wait_turns(mgr: &SessionManager, id: &SessionId, n: u64) {
    wait_snapshot(mgr, id, "turn complete", |s| s.turns_completed >= n).await;
}

async fn create_first_session(
    mgr: &SessionManager,
    dir: &Path,
    driver: &str,
) -> (SessionId, String) {
    let scenario = Scenario::builder().turn(base_turn(driver)).build();
    let mut req = create(dir, driver, &scenario);
    req.first_prompt = Some("start".into());
    req.cwd = Some(project(dir));
    let id = mgr.create(req).await.unwrap().id;
    wait_turns(mgr, &id, 1).await;
    let native = mgr
        .snapshot(&id)
        .unwrap()
        .native_session_id
        .expect("native id");
    mgr.stop(&id).await.unwrap();
    wait_snapshot(mgr, &id, "stopped", |s| !s.phase.is_live()).await;
    (id, native)
}

async fn create_resumed_session(
    mgr: &SessionManager,
    dir: &Path,
    driver: &str,
    native: &str,
) -> SessionId {
    let scenario = Scenario::builder().turn(base_turn(driver)).build();
    let mut req = create(dir, driver, &scenario);
    req.first_prompt = Some("resume".into());
    req.resume = ResumeRequest::Resume {
        native_session_id: native.into(),
    };
    req.cwd = Some(project(dir));
    let id = mgr.create(req).await.unwrap().id;
    wait_turns(mgr, &id, 1).await;
    wait_snapshot(mgr, &id, "idle", |s| s.phase == Phase::Idle).await;
    mgr.stop(&id).await.unwrap();
    wait_snapshot(mgr, &id, "stopped", |s| !s.phase.is_live()).await;
    id
}

fn read_meta(meta: &Path) -> Meta {
    let bytes = std::fs::read(meta).unwrap_or_else(|e| panic!("read {meta:?}: {e}"));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {meta:?}: {e}"))
}

/// The meta of an exited session, sealed: the actor's log is closed first, so the seal is on
/// disk when this returns.
async fn sealed_meta(mgr: &SessionManager, id: &SessionId, paths: &LogPaths) -> Meta {
    mgr.close_log(id).await.unwrap();
    let meta = read_meta(&paths.meta);
    assert!(meta.seal.is_some(), "meta {:?} is not sealed", paths.meta);
    meta
}

fn assert_journal_len(meta: &Meta, journal: &Path) {
    let seal = meta.seal.as_ref().expect("sealed");
    let journal_len = std::fs::metadata(journal).map(|m| m.len()).unwrap_or(0);
    assert_eq!(seal.journal_len, journal_len);
}

async fn resume_and_verify(driver: &str) {
    let d = TempDir::new();

    // Seed claude with a transcript carrying compaction and a subagent.
    if driver == "claude" {
        write_claude_transcript(d.path());
    }

    // First run to create a conversation to resume (non-claude drivers). For claude the
    // transcript above is the history to resume.
    let (native_id, mgr) = {
        let mgr = manager(d.path()).await;
        if driver == "claude" {
            (ID.to_string(), mgr)
        } else {
            let (_, native) = create_first_session(&mgr, d.path(), driver).await;
            (native, mgr)
        }
    };

    let resumed = create_resumed_session(&mgr, d.path(), driver, &native_id).await;
    let paths = log_paths(d.path(), &resumed);
    let sealed = sealed_meta(&mgr, &resumed, &paths).await;
    assert_journal_len(&sealed, &paths.journal);

    assert_logs_match(&format!("{driver}-sealed"), &mgr, d.path(), &resumed).await;

    // Continue: the meta is unsealed while live and resealed after exit, and the log still
    // matches an offline rebuild. The unseal is written before the new shim can grow the
    // journal, so it is on disk once the session is idle.
    mgr.continue_session(&resumed).await.unwrap();
    wait_phase(&mgr, &resumed, "idle").await;
    let meta_unsealed = read_meta(&paths.meta);
    assert!(meta_unsealed.seal.is_none());

    mgr.command(&resumed, Command::prompt("continued"))
        .await
        .unwrap();
    let baseline = mgr.snapshot(&resumed).unwrap().turns_completed;
    wait_turns(&mgr, &resumed, baseline + 1).await;
    mgr.stop(&resumed).await.unwrap();
    wait_snapshot(&mgr, &resumed, "stopped", |s| !s.phase.is_live()).await;
    let sealed = sealed_meta(&mgr, &resumed, &paths).await;
    assert_journal_len(&sealed, &paths.journal);

    assert_logs_match(&format!("{driver}-continued"), &mgr, d.path(), &resumed).await;
}

#[tokio::test]
async fn event_log_matches_replay_for_all_drivers() {
    for d in DRIVERS {
        resume_and_verify(d).await;
    }
}

#[tokio::test]
async fn event_log_matches_replay_when_shim_is_lost() {
    let d = TempDir::new();
    let driver = "codex";
    let mgr = manager(d.path()).await;
    let scenario = Scenario::builder().turn(base_turn(driver)).build();
    let mut req = create(d.path(), driver, &scenario);
    req.first_prompt = Some("lose".into());
    req.cwd = Some(project(d.path()));
    let id = mgr.create(req).await.unwrap().id;

    let snap = wait_snapshot(&mgr, &id, "pid", |s| s.pid.is_some()).await;
    let pid = snap.pid.unwrap();
    kill_pid(pid as u64);
    wait_snapshot(&mgr, &id, "lost", |s| !s.phase.is_live()).await;
    let paths = log_paths(d.path(), &id);
    let sealed = sealed_meta(&mgr, &id, &paths).await;
    assert_journal_len(&sealed, &paths.journal);

    assert_logs_match("lost-shim", &mgr, d.path(), &id).await;
}
