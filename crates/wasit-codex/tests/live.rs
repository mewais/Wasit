//! Live tests against the installed `codex`. They need a model provider:
//! set `WASIT_CODEX_HOME` to a CODEX_HOME whose config.toml points at a
//! reachable Responses API (the fixtures were recorded against a local
//! scripted fake). Run with `cargo test -p wasit-codex -- --ignored`; they
//! fail loudly when the binary or the variable is missing.

use std::time::Duration;

use serde_json::json;
use wasit_core::{AgentEvent, EndReason, SpawnSpec};
use wasit_smoke::{run, RunOptions};

fn opts(prompt: &str, script: serde_json::Value) -> RunOptions {
    let home =
        std::env::var("WASIT_CODEX_HOME").expect("set WASIT_CODEX_HOME for live codex tests");
    let dir = std::env::temp_dir().join("wasit-codex-live");
    std::fs::create_dir_all(&dir).expect("temp dir");
    RunOptions {
        driver: "codex".into(),
        spec: SpawnSpec {
            cwd: Some(dir.to_string_lossy().into_owned()),
            env: vec![("CODEX_HOME".into(), home)],
            first_prompt: Some(prompt.into()),
            ..SpawnSpec::default()
        },
        config: None,
        record: None,
        script: Some(script.as_array().cloned().unwrap()),
        wait: Duration::from_secs(60),
        echo: false,
    }
}

#[tokio::test]
#[ignore = "needs the real codex CLI and WASIT_CODEX_HOME"]
async fn live_prompt_reply_and_modes() {
    let out = run(opts(
        "hello",
        json!([{"wait": "modes_updated"}, {"wait": "turn_completed"}]),
    ))
    .await
    .expect("codex runs");
    assert_eq!(out.script_error, None);
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::SessionInit { .. })));
    assert!(out.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted {
            end_reason: None,
            ..
        }
    )));
}

#[tokio::test]
#[ignore = "needs the real codex CLI and WASIT_CODEX_HOME"]
async fn live_interrupt() {
    let out = run(opts(
        "SLOW please",
        json!([{"wait": "turn_started"}, {"cmd": {"command": "interrupt"}}, {"wait": "turn_completed"}]),
    ))
    .await
    .expect("codex runs");
    assert_eq!(out.script_error, None);
    assert!(out.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            ..
        }
    )));
}
