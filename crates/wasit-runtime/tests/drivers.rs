//! Per-driver session behaviour through the real stack: SessionManager → actor → real shim →
//! fake CLI speaking each driver's real wire protocol.

mod common;

use common::*;
use serde_json::json;
use wasit_core::{
    AgentEvent, Command, GateChoiceKind, GateDecision, GateRequest, InputId, QuestionAnswer,
};
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
    let out = mgr.command(&id, Command::prompt("hi")).await.unwrap();
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
    mgr.command(&id, Command::prompt("go")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "needs_you").await;
    assert_eq!(snap.pending_gates.len(), 1);
    let gate = &snap.pending_gates[0].gate;
    assert!(matches!(gate, GateRequest::Permission { .. }), "{gate:?}");
    let decision = if allow {
        GateDecision::by_kind(gate, &[GateChoiceKind::AllowOnce]).expect("an allow choice")
    } else {
        let c = gate
            .choice_of_kind(&[GateChoiceKind::RejectOnce, GateChoiceKind::Cancel])
            .expect("a reject choice");
        GateDecision::choose_with(&c.id, Some("no"))
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
                decision: GateDecision::by_kind(gate, &[GateChoiceKind::AllowOnce]).unwrap(),
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
    mgr.command(&id, Command::prompt("ask me")).await.unwrap();
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
                answers: [(questions[0].id.clone(), vec!["Blue".to_string()])]
                    .into_iter()
                    .collect(),
            },
        },
        // ACP models questions as a permission request with options.
        GateRequest::Permission {
            request_id,
            choices,
            ..
        } => {
            let c = choices
                .iter()
                .find(|c| c.label == "Blue")
                .expect("Blue option");
            Command::AnswerGate {
                request_id: request_id.clone(),
                decision: GateDecision::choose(&c.id),
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
    mgr.command(&id, Command::prompt("long")).await.unwrap();
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
/// codex runs `/compact` and `/review` itself and reports exactly those two; a name it does not run
/// is refused as unsupported, nothing is sent for it.
#[tokio::test]
async fn codex_slash_reports_compact_and_review_and_refuses_unknown_names() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = create_idle(&mgr, d.path(), "codex", &Scenario::default()).await;
    let snap = wait_snapshot(&mgr, &id, "slash commands", |s| {
        !s.slash_commands.is_empty()
    })
    .await;
    let names: Vec<&str> = snap
        .slash_commands
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["compact", "review"]);
    let r = mgr
        .command(
            &id,
            Command::RunSlash {
                name: "nope".into(),
                args: String::new(),
            },
        )
        .await;
    assert_eq!(
        r,
        Err(CommandError::Unsupported {
            operation: "run_slash /nope".into()
        })
    );
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
    mgr.command(&id, Command::prompt("die")).await.unwrap();
    let snap = wait_phase(&mgr, &id, "failed").await;
    let (reason, tail) = failed_reason(&snap.phase).unwrap();
    assert!(reason.contains("code 3"), "{reason}");
    assert!(tail.iter().any(|l| l.contains("out of cheese")), "{tail:?}");
    assert!(snap.turn.is_none() && snap.pending_gates.is_empty());
    assert_eq!(snap.exit.as_ref().unwrap().code, Some(3));
    // Only a prompt carries on an ended session (it continues it first); every other command
    // still says the session is not running.
    let r = mgr.command(&id, Command::Interrupt).await;
    assert!(matches!(r, Err(CommandError::NotRunning { .. })), "{r:?}");
    let r = mgr.command(&id, Command::prompt("x")).await;
    assert!(
        matches!(r, Ok(CommandOutcome::Queued) | Ok(CommandOutcome::Sent)),
        "{r:?}"
    );
    wait_snapshot(&mgr, &id, "the prompt continued the session", |s| {
        s.generation == 2
    })
    .await;
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
        |e| matches!(&e.kind, SessionEventKind::Notice { message, .. } if message.contains("stop"))
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
    mgr.command(&id, Command::prompt("x")).await.unwrap();
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

// ── inputs: the first prompt is an input; queued prompts leave the queue visibly ───────────

/// The `input` event of the prompt `text` (exactly one) and its id.
fn prompt_input(events: &[wasit_runtime::wire::SessionEvent], text: &str) -> (usize, InputId) {
    let found: Vec<(usize, InputId)> = events
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match &e.kind {
            SessionEventKind::Input {
                input,
                command: Command::Prompt { text: t, .. },
                ..
            } if t == text => Some((i, *input)),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "one input for {text:?}: {events:#?}");
    found[0]
}

/// The first prompt given at creation is an ordinary input: journaled with the start (its id
/// is the start note's), announced by `started`, its `input` event before the turn it starts,
/// which names it, written once.
async fn first_prompt_is_an_input(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("first reply"))
        .build();
    let mut req = create(d.path(), driver, &sc);
    req.first_prompt = Some("start here".into());
    let id = mgr.create(req).await.unwrap().id;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(assistant_text(&events).contains("first reply"));
    let (at, input) = prompt_input(&events, "start here");
    let started = events
        .iter()
        .position(|e| matches!(&e.kind, SessionEventKind::Started { first_input: Some(i), .. } if *i == input))
        .unwrap_or_else(|| panic!("started names the first input: {events:#?}"));
    let turn = events
        .iter()
        .position(|e| {
            is_agent(
                e,
                |a| matches!(a, AgentEvent::TurnStarted { input: Some(i), .. } if *i == input),
            )
        })
        .unwrap_or_else(|| panic!("a turn names the first input: {events:#?}"));
    assert!(started < at && at < turn, "{events:#?}");
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, SessionEventKind::Written { input: i, .. } if i == input))
            .count(),
        1
    );
    let snap = wait_phase(&mgr, &id, "idle").await;
    assert_eq!(snap.title.as_deref(), Some("start here"));
}

#[tokio::test]
async fn claude_first_prompt_is_an_input() {
    first_prompt_is_an_input("claude").await
}
#[tokio::test]
async fn codex_first_prompt_is_an_input() {
    first_prompt_is_an_input("codex").await
}
#[tokio::test]
async fn opencode_first_prompt_is_an_input() {
    first_prompt_is_an_input("opencode").await
}
#[tokio::test]
async fn kilo_first_prompt_is_an_input() {
    first_prompt_is_an_input("kilo").await
}

enum Ends {
    Interrupt,
    Stop,
}

/// A prompt the driver holds behind the running turn is reported dropped when an interrupt or
/// a stop clears it, and never starts a turn. Drivers that hold prompts say so
/// (`prompt_while_busy: queued_by_protocol`); for the others there is nothing to hold.
async fn queued_prompt_dropped(driver: &str, how: Ends) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working").wait_for_interrupt())
        .turn(Turn::builder().say("next"))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let caps = mgr.snapshot(&id).unwrap().capabilities;
    assert_eq!(
        caps.prompt_while_busy,
        wasit_core::BusyPrompt::QueuedByProtocol
    );
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("long")).await.unwrap();
    until(&mut s, |e| {
        is_agent(e, |a| matches!(a, AgentEvent::AssistantDelta { .. }))
    })
    .await;
    let out = mgr.command(&id, Command::prompt("later")).await.unwrap();
    assert_eq!(out, CommandOutcome::Queued);
    let events = until(&mut s, |e| {
        matches!(&e.kind, SessionEventKind::Input { command: Command::Prompt { text, .. }, .. } if text == "later")
    })
    .await;
    let (_, later) = prompt_input(&events[events.len() - 1..], "later");
    let reason = match how {
        Ends::Interrupt => {
            mgr.command(&id, Command::Interrupt).await.unwrap();
            wasit_core::InputDropReason::Interrupted
        }
        Ends::Stop => {
            mgr.stop(&id).await.unwrap();
            wasit_core::InputDropReason::Shutdown
        }
    };
    let events = until(&mut s, |e| match how {
        Ends::Interrupt => is_turn_completed(e),
        Ends::Stop => matches!(e.kind, SessionEventKind::Exited { .. }),
    })
    .await;
    let drops: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::InputDropped { input, reason, .. },
            } => Some((*input, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(drops, vec![(later, reason)], "{events:#?}");
    assert!(!events.iter().any(|e| is_agent(e, |a| matches!(
        a,
        AgentEvent::TurnStarted { input: Some(i), .. } | AgentEvent::InputSent { input: i } if *i == later
    ))));
    if let Ends::Interrupt = how {
        // The session stays; the next prompt gets its own turn.
        wait_phase(&mgr, &id, "idle").await;
        mgr.command(&id, Command::prompt("again")).await.unwrap();
        let events = until(&mut s, is_turn_completed).await;
        let (_, again) = prompt_input(&events, "again");
        assert!(events.iter().any(|e| is_agent(e, |a| matches!(
            a,
            AgentEvent::TurnStarted { input: Some(i), .. } if *i == again
        ))));
        assert!(
            !mgr.snapshot(&id)
                .unwrap()
                .last_turn
                .unwrap()
                .was_interrupted
        );
    }
}

#[tokio::test]
async fn claude_queued_prompt_dropped_by_interrupt() {
    queued_prompt_dropped("claude", Ends::Interrupt).await
}
#[tokio::test]
async fn claude_queued_prompt_dropped_by_stop() {
    queued_prompt_dropped("claude", Ends::Stop).await
}
#[tokio::test]
async fn opencode_queued_prompt_dropped_by_interrupt() {
    queued_prompt_dropped("opencode", Ends::Interrupt).await
}
#[tokio::test]
async fn opencode_queued_prompt_dropped_by_stop() {
    queued_prompt_dropped("opencode", Ends::Stop).await
}
#[tokio::test]
async fn kilo_queued_prompt_dropped_by_interrupt() {
    queued_prompt_dropped("kilo", Ends::Interrupt).await
}
#[tokio::test]
async fn kilo_queued_prompt_dropped_by_stop() {
    queued_prompt_dropped("kilo", Ends::Stop).await
}

/// claude acknowledges a prompt on the CLI's own report (the fake reports message lifecycles
/// as claude 2.1.293 does); nothing else acknowledges it.
#[tokio::test]
async fn claude_prompt_is_acknowledged_by_the_cli() {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder().turn(Turn::builder().say("hi")).build();
    let id = create_idle(&mgr, d.path(), "claude", &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("hello")).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    let (at, input) = prompt_input(&events, "hello");
    let pos = |f: &dyn Fn(&SessionEventKind) -> bool| {
        events
            .iter()
            .position(|e| f(&e.kind))
            .unwrap_or_else(|| panic!("{events:#?}"))
    };
    let written = pos(&|k| matches!(k, SessionEventKind::Written { input: i, .. } if *i == input));
    let acked = pos(
        &|k| matches!(k, SessionEventKind::Agent { event: AgentEvent::InputAcknowledged { input: i, steered: false } } if *i == input),
    );
    assert!(at < written && written < acked, "{events:#?}");
    assert_eq!(
        events
            .iter()
            .filter(|e| is_agent(e, |a| matches!(a, AgentEvent::InputAcknowledged { .. })))
            .count(),
        1
    );
}

fn input_events(
    events: &[wasit_runtime::wire::SessionEvent],
    f: impl Fn(&AgentEvent) -> bool,
) -> usize {
    events.iter().filter(|e| is_agent(e, &f)).count()
}

/// codex and the ACP agents (opencode, kilo) acknowledge a prompt on the CLI's own reaction:
/// codex's `turn/start` result, ACP's first update of the agent working on it. Once, after the
/// prompt was written, before the turn ends.
async fn prompt_is_acknowledged(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .turn(Turn::builder().say("hi"))
        .turn(Turn::builder().say("again"))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    for text in ["hello", "second"] {
        mgr.command(&id, Command::prompt(text)).await.unwrap();
        let events = until(&mut s, is_turn_completed).await;
        let (at, input) = prompt_input(&events, text);
        let pos = |f: &dyn Fn(&SessionEventKind) -> bool| {
            events
                .iter()
                .position(|e| f(&e.kind))
                .unwrap_or_else(|| panic!("{events:#?}"))
        };
        let written =
            pos(&|k| matches!(k, SessionEventKind::Written { input: i, .. } if *i == input));
        let acked = pos(
            &|k| matches!(k, SessionEventKind::Agent { event: AgentEvent::InputAcknowledged { input: i, steered: false } } if *i == input),
        );
        let ended = pos(&|k| {
            matches!(
                k,
                SessionEventKind::Agent {
                    event: AgentEvent::TurnCompleted { .. }
                }
            )
        });
        assert!(
            at < written && written < acked && acked < ended,
            "{events:#?}"
        );
        assert_eq!(
            input_events(&events, |a| matches!(
                a,
                AgentEvent::InputAcknowledged { .. }
            )),
            1,
            "{events:#?}"
        );
        assert_eq!(
            input_events(&events, |a| matches!(a, AgentEvent::InputDropped { .. })),
            0,
            "{events:#?}"
        );
        wait_phase(&mgr, &id, "idle").await;
    }
}

#[tokio::test]
async fn codex_prompt_is_acknowledged_by_the_cli() {
    prompt_is_acknowledged("codex").await
}
#[tokio::test]
async fn opencode_prompt_is_acknowledged_by_the_cli() {
    prompt_is_acknowledged("opencode").await
}
#[tokio::test]
async fn kilo_prompt_is_acknowledged_by_the_cli() {
    prompt_is_acknowledged("kilo").await
}

/// The fake codex running a turn that waits: `prompt("long")`, up to its first output.
async fn codex_running_turn(
    sc: &Scenario,
) -> (
    TempDir,
    wasit_runtime::SessionManager,
    wasit_runtime::wire::SessionId,
    wasit_runtime::SessionEventStream,
) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let id = create_idle(&mgr, d.path(), "codex", sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("long")).await.unwrap();
    until(&mut s, |e| {
        is_agent(e, |a| matches!(a, AgentEvent::AssistantDelta { .. }))
    })
    .await;
    (d, mgr, id, s)
}

/// A codex prompt sent during a running turn is steered into it: acknowledged as steered when
/// the CLI answers `turn/steer`, with no turn of its own.
#[tokio::test]
async fn codex_steered_prompt_is_acknowledged_as_steered() {
    let sc = Scenario::builder()
        .turn(Turn::builder().say("working").wait_for_interrupt())
        .build();
    let (_d, mgr, id, mut s) = codex_running_turn(&sc).await;
    let out = mgr.command(&id, Command::prompt("later")).await.unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let events = until(&mut s, |e| {
        is_agent(e, |a| matches!(a, AgentEvent::InputAcknowledged { .. }))
    })
    .await;
    let (_, later) = prompt_input(&events, "later");
    assert!(
        events.iter().any(|e| is_agent(
            e,
            |a| matches!(a, AgentEvent::InputAcknowledged { input: i, steered: true } if *i == later)
        )),
        "{events:#?}"
    );
    assert!(!events.iter().any(|e| is_agent(e, |a| matches!(
        a,
        AgentEvent::TurnStarted { input: Some(i), .. } if *i == later
    ))));
    // Still the one turn: an interrupt ends it, with nothing more said about the steered prompt.
    mgr.command(&id, Command::Interrupt).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert_eq!(
        input_events(&events, |a| matches!(
            a,
            AgentEvent::InputAcknowledged { .. } | AgentEvent::InputDropped { .. }
        )),
        0,
        "{events:#?}"
    );
    wait_phase(&mgr, &id, "idle").await;
}

/// A `turn/steer` the CLI refuses is reported dropped with the CLI's error; the running turn is
/// untouched and the prompt is not started as a turn behind its back.
#[tokio::test]
async fn codex_refused_steer_is_reported_dropped() {
    let sc = Scenario::builder()
        .refuse("^refuse me", "this turn cannot take input")
        .turn(Turn::builder().say("working").wait_for_interrupt())
        .turn(Turn::builder().say("next"))
        .build();
    let (_d, mgr, id, mut s) = codex_running_turn(&sc).await;
    let out = mgr
        .command(&id, Command::prompt("refuse me"))
        .await
        .unwrap();
    assert_eq!(out, CommandOutcome::Sent);
    let events = until(&mut s, |e| {
        is_agent(e, |a| matches!(a, AgentEvent::InputDropped { .. }))
    })
    .await;
    let (_, refused) = prompt_input(&events, "refuse me");
    let dropped = events
        .iter()
        .find_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event:
                    AgentEvent::InputDropped {
                        input,
                        reason,
                        error,
                    },
            } => Some((*input, *reason, error.clone())),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        dropped,
        (
            refused,
            wasit_core::InputDropReason::Refused,
            Some("this turn cannot take input".into())
        )
    );
    assert!(!events.iter().any(|e| is_agent(
        e,
        |a| matches!(a, AgentEvent::InputAcknowledged { input: i, .. } if *i == refused)
    )));
    // The turn runs on until it is interrupted; no turn was started for the refused prompt.
    mgr.command(&id, Command::Interrupt).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    assert!(!events.iter().any(|e| is_agent(e, |a| matches!(
        a,
        AgentEvent::TurnStarted { input: Some(i), .. } if *i == refused
    ))));
    wait_phase(&mgr, &id, "idle").await;
    assert_eq!(mgr.snapshot(&id).unwrap().turns_completed, 1);
}

/// A prompt the CLI refuses outright (codex: the `turn/start` is answered with an error; the
/// ACP agents: the `session/prompt` is, with no work done) is reported dropped with the CLI's
/// error, the turn it never ran ends as an error, and the session takes the next prompt.
async fn refused_prompt(driver: &str) {
    let d = TempDir::new();
    let mgr = manager(d.path()).await;
    let sc = Scenario::builder()
        .refuse("^refuse me", "You need to sign in to use this model.")
        .turn(Turn::when("^refuse me").fail("You need to sign in to use this model."))
        .turn(Turn::builder().say("fine"))
        .build();
    let id = create_idle(&mgr, d.path(), driver, &sc).await;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    mgr.command(&id, Command::prompt("refuse me"))
        .await
        .unwrap();
    let events = until(&mut s, is_turn_completed).await;
    let (_, refused) = prompt_input(&events, "refuse me");
    let dropped: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event:
                    AgentEvent::InputDropped {
                        input,
                        reason,
                        error,
                    },
            } => Some((*input, *reason, error.clone().unwrap_or_default())),
            _ => None,
        })
        .collect();
    assert_eq!(dropped.len(), 1, "{events:#?}");
    assert_eq!(
        (dropped[0].0, dropped[0].1),
        (refused, wasit_core::InputDropReason::Refused)
    );
    assert!(
        dropped[0]
            .2
            .contains("You need to sign in to use this model."),
        "{dropped:?}"
    );
    assert_eq!(
        input_events(&events, |a| matches!(
            a,
            AgentEvent::InputAcknowledged { .. }
        )),
        0,
        "{events:#?}"
    );
    assert!(events.iter().any(|e| is_agent(e, |a| matches!(
        a,
        AgentEvent::TurnCompleted {
            end_reason: Some(wasit_core::EndReason::ExecutionError),
            ..
        }
    ))));
    // The session carries on, and the next prompt is acknowledged as usual.
    wait_phase(&mgr, &id, "idle").await;
    mgr.command(&id, Command::prompt("go on")).await.unwrap();
    let events = until(&mut s, is_turn_completed).await;
    let (_, next) = prompt_input(&events, "go on");
    assert!(events.iter().any(|e| is_agent(
        e,
        |a| matches!(a, AgentEvent::InputAcknowledged { input: i, steered: false } if *i == next)
    )));
}

#[tokio::test]
async fn codex_refused_prompt_is_reported_dropped() {
    refused_prompt("codex").await
}
#[tokio::test]
async fn opencode_refused_prompt_is_reported_dropped() {
    refused_prompt("opencode").await
}
#[tokio::test]
async fn kilo_refused_prompt_is_reported_dropped() {
    refused_prompt("kilo").await
}
