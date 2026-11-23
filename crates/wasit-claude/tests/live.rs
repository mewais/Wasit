//! Live tests against the installed `claude` (cheapest model, tiny prompts).
//! Run with `cargo test -p wasit-claude -- --ignored`; they fail loudly when
//! the binary is missing or not logged in.

use std::time::Duration;

use serde_json::json;
use wasit_core::{AgentEvent, EndReason, SpawnSpec};
use wasit_smoke::{run, RunOptions};

fn opts(prompt: &str, script: serde_json::Value) -> RunOptions {
    let dir = std::env::temp_dir().join("wasit-claude-live");
    std::fs::create_dir_all(&dir).expect("temp dir");
    RunOptions {
        driver: "claude".into(),
        spec: SpawnSpec {
            cwd: Some(dir.to_string_lossy().into_owned()),
            model: Some("haiku".into()),
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

#[tokio::test]
#[ignore = "needs the real claude CLI (logged in)"]
async fn live_prompt_reply() {
    let out = run(opts(
        "Reply with just the word hi",
        json!([{"wait": "turn_completed"}]),
    ))
    .await
    .expect("claude runs");
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
    // The CLI reported taking the first prompt (input 1: the start record) in.
    assert!(out.events.contains(&AgentEvent::InputAcknowledged {
        input: wasit_core::InputId(1),
        steered: false
    }));
}

#[tokio::test]
#[ignore = "needs the real claude CLI (logged in)"]
async fn live_interrupt() {
    let out = run(opts(
        "Write a 60 line poem about the sea.",
        json!([{"wait": "session_init"}, {"cmd": {"command": "interrupt"}}, {"wait": "turn_completed"}]),
    ))
    .await
    .expect("claude runs");
    assert_eq!(out.script_error, None);
    assert!(out.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            ..
        }
    )));
}
