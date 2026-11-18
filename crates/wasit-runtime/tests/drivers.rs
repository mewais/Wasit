//! Per-driver session behaviour through the real stack: SessionManager → actor → real shim →
//! fake CLI speaking each driver's real wire protocol.
#![cfg(unix)]

mod common;

use common::*;
use serde_json::json;
use wasit_core::{AgentEvent, Command, GateDecision, GateRequest, QuestionAnswer};
use wasit_fake_cli::scenario::{Scenario, Step, Turn};
use wasit_runtime::wire::{CommandError, CommandOutcome, Phase, SessionEventKind};

async fn create_idle(
    mgr: &wasit_runtime::SessionManager,
    dir: &std::path::Path,
    driver: &str,
    sc: &Scenario,
) -> wasit_runtime::wire::SessionId {
    let r = mgr.create(create(dir, driver, sc)).await.expect("create");
    assert!(r.unsupported.is_empty());
    wait_phase(mgr, &r.id, "idle").await;
    r.id
}

async fn prompt_turn(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .model("m/one", "One")
        .turn(Turn::builder().say("hello from the fake").usage(10, 5))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    let out = mgr
        .command(&id, Command::Prompt { text: "hi".into() })
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let events = until(&mut s, is_turn_completed).await;
    assert!(
        assistant_text(&events).contains("hello from the fake"),
        "{events:#?}"
    );
    assert!(events.iter().any(|e| matches!(
        &e.kind,
        SessionEventKind::Input {
            command: Command::Prompt { .. },
            ..
        }
    )));
    assert_monotonic(&events);
    let snap = wait_phase(&mgr, &id, "idle").await;
    assert_eq!(snap.turns_completed, 1);
    assert_eq!(snap.title.as_deref(), Some("hi"));
    assert!(snap.native_session_id.is_some(), "{snap:#?}");
    assert!(snap.last_turn.is_some());
    mgr.stop(&id).await.unwrap();
    let snap = wait_snapshot(&mgr, &id, "exit", |s| !s.phase.is_live()).await;
    assert!(
        matches!(snap.phase, Phase::Exited { .. }),
        "{:?}",
        snap.phase
    );
}

#[tokio::test]
async fn claude_prompt_turn() {
    prompt_turn("claude").await
}
#[tokio::test]
async fn codex_prompt_turn() {
    prompt_turn("codex").await
}
#[tokio::test]
async fn opencode_prompt_turn() {
    prompt_turn("opencode").await
}
#[tokio::test]
async fn kilo_prompt_turn() {
    prompt_turn("kilo").await
}

async fn permission_gate(driver: &str, allow: bool) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .tool("Bash", json!({"command": "touch x"}))
                .needs_permission()
                .result_if_allowed("did it")
                .result_if_denied("refused")
                .done()
                .say("after the tool"),
        )
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    mgr.command(&id, Command::Prompt { text: "go".into() })
        .await
        .unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    assert_eq!(snap.pending_gates.len(), 1);
    let gate = &snap.pending_gates[0].gate;
    assert!(matches!(gate, GateRequest::Permission { .. }), "{gate:?}");
    let decision = if allow {
        match gate {
            GateRequest::Permission {
                elicitation_choices,
                ..
            } if !elicitation_choices.is_empty() => {
                // ACP: pick the "allow once" option explicitly.
                let c = elicitation_choices
                    .iter()
                    .find(|c| c.value.contains("allow") || c.label.to_lowercase().contains("allow"))
                    .unwrap_or(&elicitation_choices[0]);
                GateDecision::Approve {
                    updated_input: None,
                    permission_updates: None,
                    elicitation_content: Some(json!({c.key.clone(): c.value.clone()})),
                }
            }
            _ => GateDecision::approve(),
        }
    } else {
        GateDecision::deny(Some("no"))
    };
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    let out = mgr
        .command(
            &id,
            Command::AnswerGate {
                request_id: gate.request_id().to_string(),
                decision,
            },
        )
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let events = until(&mut s, is_turn_completed).await;
    let finished = events.iter().find_map(|e| match &e.kind {
        SessionEventKind::Agent {
            event: AgentEvent::ToolCallFinished { ok, .. },
        } => Some(*ok),
        _ => None,
    });
    assert_eq!(finished, Some(allow), "{events:#?}");
    let snap = wait_phase(&mgr, &id, "idle").await;
    assert!(snap.pending_gates.is_empty());
    // Answering it again is rejected by the protocol, typed.
    let again = mgr
        .command(
            &id,
            Command::AnswerGate {
                request_id: gate.request_id().to_string(),
                decision: GateDecision::approve(),
            },
        )
        .await;
    assert!(
        matches!(again, Err(CommandError::Rejected { .. })),
        "{again:?}"
    );
}

#[tokio::test]
async fn claude_gate_allow() {
    permission_gate("claude", true).await
}
#[tokio::test]
async fn claude_gate_deny() {
    permission_gate("claude", false).await
}
#[tokio::test]
async fn codex_gate_allow() {
    permission_gate("codex", true).await
}
#[tokio::test]
async fn codex_gate_deny() {
    permission_gate("codex", false).await
}
#[tokio::test]
async fn opencode_gate_allow() {
    permission_gate("opencode", true).await
}
#[tokio::test]
async fn opencode_gate_deny() {
    permission_gate("opencode", false).await
}
#[tokio::test]
async fn kilo_gate_allow() {
    permission_gate("kilo", true).await
}
#[tokio::test]
async fn kilo_gate_deny() {
    permission_gate("kilo", false).await
}

async fn question(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .ask_question("Which color?", &[("Red", "warm"), ("Blue", "cold")])
                .say("noted"),
        )
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    mgr.command(
        &id,
        Command::Prompt {
            text: "ask me".into(),
        },
    )
    .await
    .unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    let gate = snap.pending_gates[0].gate.clone();
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    let cmd = match &gate {
        GateRequest::Question {
            request_id,
            questions,
        } => Command::AnswerQuestion {
            request_id: request_id.clone(),
            answer: QuestionAnswer {
                answers: [(questions[0].question.clone(), "Blue".to_string())]
                    .into_iter()
                    .collect(),
            },
        },
        // ACP models questions as a permission request with options.
        GateRequest::Permission {
            request_id,
            elicitation_choices,
            ..
        } => {
            let c = elicitation_choices
                .iter()
                .find(|c| c.label == "Blue")
                .expect("Blue option");
            Command::AnswerGate {
                request_id: request_id.clone(),
                decision: GateDecision::Approve {
                    updated_input: None,
                    permission_updates: None,
                    elicitation_content: Some(json!({c.key.clone(): c.value.clone()})),
                },
            }
        }
        other => panic!("unexpected gate {other:?}"),
    };
    assert_eq!(mgr.command(&id, cmd).await.unwrap(), CommandOutcome::Sent);
    let events = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&events).contains("noted"), "{events:#?}");
    wait_phase(&mgr, &id, "idle").await;
}

#[tokio::test]
async fn claude_question() {
    question("claude").await
}
#[tokio::test]
async fn codex_question() {
    question("codex").await
}
#[tokio::test]
async fn opencode_question() {
    question("opencode").await
}
#[tokio::test]
async fn kilo_question() {
    question("kilo").await
}

async fn interrupt(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working").wait_for_interrupt())
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(
        &id,
        Command::Prompt {
            text: "long".into(),
        },
    )
    .await
    .unwrap();
    until(&mut s, |e| {
        is_agent(e, |a| matches!(a, AgentEvent::AssistantDelta { .. }))
    })
    .await;
    assert_eq!(wait_phase(&mgr, &id, "working").await.phase, Phase::Working);
    let out = mgr.command(&id, Command::Interrupt).await.unwrap();
    assert!(matches!(out, CommandOutcome::Sent | CommandOutcome::Queued));
    let events = until(&mut s, is_turn_completed).await;
    let interrupted = events.iter().any(|e| {
        is_agent(e, |a| {
            matches!(
                a,
                AgentEvent::TurnCompleted {
                    was_interrupted: true,
                    ..
                }
            )
        })
    });
    assert!(interrupted, "{events:#?}");
    let snap = wait_phase(&mgr, &id, "idle").await;
    assert!(snap.last_turn.unwrap().was_interrupted);
}

#[tokio::test]
async fn claude_interrupt() {
    interrupt("claude").await
}
#[tokio::test]
async fn codex_interrupt() {
    interrupt("codex").await
}
#[tokio::test]
async fn opencode_interrupt() {
    interrupt("opencode").await
}
#[tokio::test]
async fn kilo_interrupt() {
    interrupt("kilo").await
}

async fn model_and_mode(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .model("p/a", "A")
        .model("p/b", "B")
        .current_model("p/a")
        .mode("build", "Build")
        .mode("plan", "Plan")
        .current_mode("build")
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let caps = mgr.snapshot(&id).unwrap().capabilities;
    assert!(caps.set_model && caps.set_mode);
    mgr.command(
        &id,
        Command::SetModel {
            model: "p/b".into(),
        },
    )
    .await
    .unwrap();
    wait_snapshot(&mgr, &id, "model p/b", |s| {
        s.model.as_deref() == Some("p/b")
    })
    .await;
    let mode = if driver == "claude" {
        "acceptEdits"
    } else {
        "plan"
    };
    mgr.command(&id, Command::SetMode { mode: mode.into() })
        .await
        .unwrap();
    wait_snapshot(&mgr, &id, "mode", |s| s.mode.as_deref() == Some(mode)).await;
}

#[tokio::test]
async fn claude_model_and_mode() {
    model_and_mode("claude").await
}
#[tokio::test]
async fn codex_model_and_mode() {
    model_and_mode("codex").await
}
#[tokio::test]
async fn opencode_model_and_mode() {
    model_and_mode("opencode").await
}
#[tokio::test]
async fn kilo_model_and_mode() {
    model_and_mode("kilo").await
}

async fn slash(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .command("review", "Review the code")
        .turn(Turn::when("review").say("reviewed"))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let caps = mgr.snapshot(&id).unwrap().capabilities;
    let cmd = Command::RunSlash {
        name: "review".into(),
        args: "src".into(),
    };
    if !caps.slash_commands {
        let r = mgr.command(&id, cmd).await;
        assert!(matches!(r, Err(CommandError::Unsupported { .. })), "{r:?}");
        return;
    }
    let snap = wait_snapshot(&mgr, &id, "slash commands", |s| {
        s.slash_commands.iter().any(|c| c.name == "review")
    })
    .await;
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.command(&id, cmd).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&events).contains("reviewed"), "{events:#?}");
}

#[tokio::test]
async fn claude_slash() {
    slash("claude").await
}
#[tokio::test]
async fn codex_slash_is_unsupported() {
    slash("codex").await
}
#[tokio::test]
async fn opencode_slash() {
    slash("opencode").await
}
#[tokio::test]
async fn kilo_slash() {
    slash("kilo").await
}

async fn crash_mid_turn(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(
            Turn::builder()
                .say("about to die")
                .stderr("fatal: out of cheese")
                .exit(3),
        )
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    mgr.command(&id, Command::Prompt { text: "die".into() })
        .await
        .unwrap();
    let snap = wait_phase(&mgr, &id, "failed").await;
    let (reason, tail) = failed_reason(&snap.phase).unwrap();
    assert!(reason.contains("code 3"), "{reason}");
    assert!(tail.iter().any(|l| l.contains("out of cheese")), "{tail:?}");
    assert!(snap.turn.is_none() && snap.pending_gates.is_empty());
    assert_eq!(snap.exit.as_ref().unwrap().code, Some(3));
    let r = mgr.command(&id, Command::Prompt { text: "x".into() }).await;
    assert!(matches!(r, Err(CommandError::NotRunning { .. })), "{r:?}");
}

#[tokio::test]
async fn claude_crash_mid_turn() {
    crash_mid_turn("claude").await
}
#[tokio::test]
async fn codex_crash_mid_turn() {
    crash_mid_turn("codex").await
}
#[tokio::test]
async fn opencode_crash_mid_turn() {
    crash_mid_turn("opencode").await
}
#[tokio::test]
async fn kilo_crash_mid_turn() {
    crash_mid_turn("kilo").await
}

async fn clean_exit(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = create_idle(&mgr, d.path(), driver, &Scenario::default()).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.stop(&id).await.unwrap();
    let events = until(&mut s, |e| {
        matches!(e.kind, SessionEventKind::Exited { .. })
    })
    .await;
    assert!(events.iter().any(
        |e| matches!(&e.kind, SessionEventKind::Notice { message } if message.contains("stop"))
    ));
    let snap = wait_phase(&mgr, &id, "exited").await;
    assert!(snap.exit.unwrap().requested);
    // Stop of an exited session is a no-op.
    mgr.stop(&id).await.unwrap();
}

#[tokio::test]
async fn claude_clean_exit() {
    clean_exit("claude").await
}
#[tokio::test]
async fn codex_clean_exit() {
    clean_exit("codex").await
}
#[tokio::test]
async fn opencode_clean_exit() {
    clean_exit("opencode").await
}
#[tokio::test]
async fn kilo_clean_exit() {
    clean_exit("kilo").await
}

async fn handshake_hang(driver: &str) {
    let d = TempDir::new();
    let mut t = timeouts();
    t.handshake = std::time::Duration::from_millis(800);
    t.stop_grace = std::time::Duration::from_millis(300);
    let mgr = wasit_runtime::SessionManager::new(config(d.path(), t))
        .await
        .unwrap();
    let sc = Scenario::builder()
        .startup(Step::Stderr {
            text: "waiting for the network".into(),
        })
        .startup(Step::Hang)
        .build();
    let r = mgr.create(create(d.path(), driver, &sc)).await.unwrap();
    assert_eq!(mgr.snapshot(&r.id).unwrap().phase, Phase::Starting);
    let snap = wait_phase(&mgr, &r.id, "failed").await;
    let (reason, tail) = failed_reason(&snap.phase).unwrap();
    assert!(reason.contains("handshake"), "{reason}");
    assert!(tail.iter().any(|l| l.contains("network")), "{tail:?}");
}

#[tokio::test]
async fn claude_handshake_hang() {
    handshake_hang("claude").await
}
#[tokio::test]
async fn codex_handshake_hang() {
    handshake_hang("codex").await
}
#[tokio::test]
async fn opencode_handshake_hang() {
    handshake_hang("opencode").await
}
#[tokio::test]
async fn kilo_handshake_hang() {
    handshake_hang("kilo").await
}

async fn garbage_tolerated(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().garbage("{{{ not json").say("still fine"))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::Prompt { text: "x".into() })
        .await
        .unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(events
        .iter()
        .any(|e| is_agent(e, |a| matches!(a, AgentEvent::Unknown { .. }))));
    assert!(assistant_text(&events).contains("still fine"));
    wait_phase(&mgr, &id, "idle").await;
}

#[tokio::test]
async fn claude_garbage_tolerated() {
    garbage_tolerated("claude").await
}
#[tokio::test]
async fn codex_garbage_tolerated() {
    garbage_tolerated("codex").await
}
#[tokio::test]
async fn opencode_garbage_tolerated() {
    garbage_tolerated("opencode").await
}
#[tokio::test]
async fn kilo_garbage_tolerated() {
    garbage_tolerated("kilo").await
}

#[tokio::test]
async fn first_prompt_and_unknown_driver() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("first reply"))
        .build();
    let mut req = create(d.path(), "claude", &sc);
    req.first_prompt = Some("start here".into());
    let id = mgr.create(req).await.unwrap().id;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&events).contains("first reply"));
    assert_eq!(
        mgr.snapshot(&id).unwrap().title.as_deref(),
        Some("start here")
    );
    let r = mgr
        .create(wasit_runtime::wire::CreateSession::new("nope"))
        .await;
    assert!(matches!(
        r,
        Err(wasit_runtime::wire::RuntimeError::UnknownDriver(_))
    ));
}
