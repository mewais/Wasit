//! A live test of earlier history against the real claude CLI. Opt-in:
//! `cargo test -p wasit-runtime --test live_earlier -- --ignored --nocapture`. A missing binary
//! fails the test loudly.
//!
//! A conversation is made with the CLI itself in a scratch project folder (two tiny prompts,
//! `claude -p`, the cheapest model), then resumed by its id through the whole runtime (manager →
//! actor → real shim → real claude): the earlier prompts and replies must be there, as history,
//! before the new turn's events. The CLI keeps its transcripts under its own config directory
//! (`~/.claude/projects/-tmp-marsad-b2-…`): that is the CLI's own side effect, nothing here
//! writes there.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::*;
use wasit_core::{AgentEvent, Command};
use wasit_runtime::wire::{
    CreateSession, EarlierSource, EarlierState, HistoryScope, Phase, ResumeRequest,
    SessionEventKind, SessionId,
};
use wasit_runtime::{RuntimeConfig, SessionManager, ShimLauncher};

const LIVE: Duration = Duration::from_secs(240);

fn require(bin: &str) {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let ok = std::env::split_paths(&path).any(|dir| {
        ["", ".exe", ".cmd", ".bat"]
            .iter()
            .any(|ext| dir.join(format!("{bin}{ext}")).is_file())
    });
    assert!(ok, "live test needs `{bin}` on PATH");
}

/// One `claude -p` run in `cwd`; its JSON result.
async fn claude_p(cwd: &std::path::Path, args: &[&str]) -> serde_json::Value {
    let out = tokio::process::Command::new("claude")
        .args(["-p", "--model", "haiku", "--output-format", "json"])
        .args(args)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .expect("run claude");
    assert!(
        out.status.success(),
        "claude failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("claude printed JSON")
}

fn item_text(e: &wasit_runtime::wire::SessionEvent) -> String {
    match &e.kind {
        SessionEventKind::Agent { event } => match event.as_history() {
            Some((_, AgentEvent::UserMessage { text })) => format!("user:{text}"),
            Some((_, AgentEvent::AssistantDelta { text, .. })) => format!("assistant:{text}"),
            Some((_, other)) => format!("other:{other:?}"),
            None => panic!("a live event among the earlier items"),
        },
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
#[ignore = "live: needs an authenticated claude CLI"]
async fn live_claude_resume_shows_the_earlier_conversation() {
    require("claude");
    let base = PathBuf::from(format!("/tmp/marsad-b2/live-{}", std::process::id()));
    let project = base.join("proj");
    std::fs::create_dir_all(&project).unwrap();
    // The project folder as the CLI (and the runtime) see it.
    let project = std::fs::canonicalize(&project).unwrap();

    // 1. A conversation made by the CLI alone.
    let first = claude_p(
        &project,
        &["Remember the word pelican. Reply with just: OK"],
    )
    .await;
    let native = first["session_id"]
        .as_str()
        .expect("a session id")
        .to_string();
    let second = claude_p(
        &project,
        &[
            "--resume",
            &native,
            "Which word did I ask you to remember? Reply with just that word.",
        ],
    )
    .await;
    assert_eq!(second["session_id"].as_str(), Some(native.as_str()));
    eprintln!(
        "[live] the CLI made conversation {native}: replies {:?} and {:?}",
        first["result"], second["result"]
    );

    // 2. Resumed by id through the runtime.
    let d = base.join("data");
    std::fs::create_dir_all(&d).unwrap();
    let mut cfg = RuntimeConfig::new(&d, ShimLauncher::subcommand(hostd(), "shim"));
    cfg.timeouts.handshake = Duration::from_secs(120);
    cfg.timeouts.command = Duration::from_secs(60);
    let mgr = SessionManager::new(cfg).await.unwrap();
    let mut req = CreateSession::new("claude");
    req.cwd = Some(project.to_string_lossy().into_owned());
    req.model = Some("haiku".into());
    req.resume = ResumeRequest::Resume {
        native_session_id: native.clone(),
    };
    let id: SessionId = mgr.create(req).await.unwrap().id;
    let snap = wait_snapshot_within(&mgr, &id, "idle", LIVE, |s| {
        s.phase == Phase::Idle || !s.phase.is_live()
    })
    .await;
    assert_eq!(snap.phase, Phase::Idle, "{snap:#?}");
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.command(&id, Command::prompt("Reply with just the word: done"))
        .await
        .unwrap();
    let live = tokio::time::timeout(LIVE, until(&mut s, is_turn_completed))
        .await
        .expect("the new turn completed");
    eprintln!("[live] new turn's reply: {:?}", assistant_text(&live));

    // 3. The earlier conversation is there, as history, in order, before the live events.
    let snap = wait_snapshot_within(&mgr, &id, "earlier history loaded", LIVE, |s| {
        !matches!(s.earlier.state, EarlierState::Loading | EarlierState::None)
    })
    .await;
    eprintln!("[live] earlier: {:?}", snap.earlier);
    assert_eq!(snap.earlier.source, Some(EarlierSource::AgentStorage));
    assert!(
        matches!(snap.earlier.state, EarlierState::Loaded { .. }),
        "{:?}",
        snap.earlier
    );
    let page = mgr
        .earlier_scoped(&id, None, 100, HistoryScope::All)
        .await
        .unwrap();
    let said: Vec<String> = page.events.iter().map(item_text).collect();
    eprintln!("[live] earlier items: {said:#?}");
    let first_prompt = said
        .iter()
        .position(|s| s.starts_with("user:Remember the word pelican"))
        .expect("the first prompt is there");
    let first_reply = said
        .iter()
        .position(|s| s.starts_with("assistant:") && s.contains("OK"))
        .expect("the first reply is there");
    let second_prompt = said
        .iter()
        .position(|s| s.starts_with("user:Which word did I ask you to remember"))
        .expect("the second prompt is there");
    let second_reply = said
        .iter()
        .position(|s| s.to_lowercase().contains("pelican") && s.starts_with("assistant:"))
        .expect("the second reply is there");
    assert!(
        first_prompt < first_reply && first_reply < second_prompt && second_prompt < second_reply
    );
    // The history carries the times the CLI recorded.
    assert!(page.events.iter().all(|e| match &e.kind {
        SessionEventKind::Agent { event } => event.as_history().unwrap().0.is_some(),
        _ => false,
    }));
    // And they come before the new turn: the live transcript has only the new turn, and the
    // session's own counters know nothing of the earlier ones.
    let live_page = mgr.history(&id, None, 2000).await.unwrap();
    assert!(live_page.events.iter().all(|e| !matches!(
        &e.kind,
        SessionEventKind::Agent { event } if event.as_history().is_some()
    )));
    let live_text = assistant_text(&live_page.events).to_lowercase();
    assert!(live_text.contains("done"), "{live_text:?}");
    assert!(!live_text.contains("pelican"));
    assert_eq!(snap.turns_completed, 1);
    mgr.stop(&id).await.unwrap();
}
