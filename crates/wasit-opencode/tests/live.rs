//! Live tests against the installed `opencode` and `kilo` (free models).
//! Run with `cargo test -p wasit-opencode -- --ignored`; they fail loudly
//! when a binary is missing.

use std::time::Duration;

use serde_json::json;
use wasit_core::{AgentEvent, EndReason, SpawnSpec};
use wasit_smoke::{run, RunOptions};

fn opts(driver: &str, model: &str, prompt: &str, script: serde_json::Value) -> RunOptions {
    let dir = std::env::temp_dir().join(format!("wasit-{driver}-live"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    RunOptions {
        driver: driver.into(),
        spec: SpawnSpec {
            cwd: Some(dir.to_string_lossy().into_owned()),
            model: Some(model.into()),
            first_prompt: Some(prompt.into()),
            ..SpawnSpec::default()
        },
        config: None,
        record: None,
        script: Some(script.as_array().cloned().unwrap()),
        wait: Duration::from_secs(120),
        echo: false,
    }
}

async fn prompt_reply(driver: &str, model: &str) {
    let out = run(opts(
        driver,
        model,
        "Reply with just the word hi",
        json!([{"wait": "turn_completed"}]),
    ))
    .await
    .unwrap_or_else(|e| panic!("{driver} runs: {e}"));
    assert_eq!(out.script_error, None);
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::SessionInit { .. })));
    assert!(out.events.contains(&AgentEvent::ModelChanged {
        model: model.into()
    }));
    assert!(out.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted {
            end_reason: None,
            ..
        }
    )));
}

#[tokio::test]
#[ignore = "needs the real opencode CLI"]
async fn live_opencode_prompt_reply() {
    prompt_reply("opencode", "opencode/deepseek-v4.1-flash-free").await;
}

#[tokio::test]
#[ignore = "needs the real kilo CLI"]
async fn live_kilo_prompt_reply() {
    prompt_reply("kilo", "kilo/kilo-auto/free").await;
}

#[tokio::test]
#[ignore = "needs the real opencode CLI"]
async fn live_opencode_interrupt() {
    let out = run(opts(
        "opencode",
        "opencode/deepseek-v4.1-flash-free",
        "Write a 60 line poem about the sea.",
        json!([{"wait": "thinking_delta"}, {"cmd": {"command": "interrupt"}}, {"wait": "turn_completed"}]),
    ))
    .await
    .expect("opencode runs");
    assert_eq!(out.script_error, None);
    assert!(out.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            ..
        }
    )));
}
