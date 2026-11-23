use super::*;
use serde_json::json;
use wasit_core::{
    AgentEvent, Command, Encoded, EndReason, GateDecision, GateRequest, InputId, McpServerConfig,
    McpTransport, PluginRef, QuestionAnswer, SkillRef, Step, TurnTrigger,
};

/// `system/init`, printed at the start of every turn.
const TURN_INIT: &str = r#"{"type":"system","subtype":"init","session_id":"s1","model":"m","cwd":"/p","tools":[],"mcp_servers":[],"permissionMode":"default","claude_code_version":"2.1.293","slash_commands":[]}"#;

fn ready() -> (ClaudeProtocol, Step) {
    let mut p = ClaudeProtocol::new();
    let start = p.start(&SpawnSpec::default());
    let step = p.on_line(
        r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{"commands":[{"name":"review","description":"Review","argumentHint":"[pr]"}],"models":[{"value":"default","displayName":"Default","description":"d"},{"value":"haiku","displayName":"Haiku","description":"fast"}],"current_permission_mode":"default"}}}"#,
    );
    assert_eq!(start.write.len(), 1);
    (p, step)
}

fn sent(r: Result<Encoded, wasit_core::DriverError>) -> Step {
    match r.expect("encode ok") {
        Encoded::Sent(s) => s,
        Encoded::Queued => panic!("unexpectedly queued"),
    }
}

fn writes(step: &Step) -> Vec<Value> {
    step.write
        .iter()
        .map(|l| serde_json::from_str(&l.0).unwrap())
        .collect()
}

/// The first prompt is an ordinary input: encoded right after `start` (the protocol never
/// reads it from the spec), queued until the handshake, sent with its id.
#[test]
fn start_writes_initialize_and_the_first_prompt_waits_for_the_handshake() {
    let mut p = ClaudeProtocol::new();
    let spec = SpawnSpec {
        first_prompt: Some("hi".into()),
        ..SpawnSpec::default()
    };
    let step = p.start(&spec);
    assert_eq!(
        step.write[0].0,
        r#"{"request":{"hooks":null,"subtype":"initialize"},"request_id":"req_1","type":"control_request"}"#
    );
    assert_eq!(step.write.len(), 1);
    let init = r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#;
    // A spec's first prompt is not the protocol's business.
    assert!(p.on_line(init).write.is_empty());

    let mut p = ClaudeProtocol::new();
    let (spec, first) = spec.split_first_prompt();
    p.start(&spec);
    assert_eq!(
        p.encode(InputId(2), first.unwrap()).unwrap(),
        Encoded::Queued
    );
    // A prompt before the initialize response is queued.
    assert_eq!(
        p.encode(InputId(3), Command::prompt("x")).unwrap(),
        Encoded::Queued
    );
    let step = p.on_line(init);
    // Only the first prompt goes out; the second waits for the turn to end.
    assert_eq!(step.write.len(), 1);
    assert!(step.write[0].0.contains(r#""text":"hi""#));
    assert!(!step.write[0].0.contains("uuid"), "no session id, no uuid");
    // Writing the prompt is not the turn: the turn starts when the CLI says so.
    assert_eq!(
        step.events,
        vec![AgentEvent::InputSent { input: InputId(2) }]
    );
    // The second prompt waits for the first one's turn to start, run and end.
    p.on_line(TURN_INIT);
    let step = p.on_line(
        r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1}"#,
    );
    assert!(matches!(step.events[0], AgentEvent::TurnCompleted { .. }));
    assert_eq!(step.events[1], AgentEvent::InputSent { input: InputId(3) });
    assert_eq!(step.write.len(), 1);
    assert!(step.write[0].0.contains(r#""text":"x""#));
}

#[test]
fn init_response_reports_commands_models_and_mode() {
    let (_, step) = ready();
    assert_eq!(step.events.len(), 3);
    match &step.events[0] {
        AgentEvent::SlashCommandsUpdated { commands } => {
            assert_eq!(commands[0].name, "review");
            assert_eq!(commands[0].argument_hint, "[pr]");
        }
        e => panic!("{e:?}"),
    }
    match &step.events[1] {
        AgentEvent::ModelsUpdated { models, .. } => {
            assert_eq!(models.len(), 2);
            assert!(models[0].is_default);
            assert_eq!(models[1].id, "haiku");
        }
        e => panic!("{e:?}"),
    }
    assert_eq!(
        step.events[2],
        AgentEvent::ModesUpdated {
            available_modes: vec![],
            current_mode: Some("default".into())
        }
    );
}

#[test]
fn unknown_lines_before_init_are_tolerated() {
    let mut p = ClaudeProtocol::new();
    p.start(&SpawnSpec::default());
    let step = p.on_line(r#"{"type":"active_goal","value":null,"session_id":"s"}"#);
    assert!(matches!(step.events[0], AgentEvent::Unknown { .. }));
    let step = p.on_line("not json at all");
    assert!(matches!(step.events[0], AgentEvent::Unknown { .. }));
    // The handshake still completes afterwards.
    assert_eq!(
        p.encode(InputId(101), Command::prompt("x")).unwrap(),
        Encoded::Queued
    );
    let step = p.on_line(r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#);
    assert_eq!(step.write.len(), 1);
}

#[test]
fn session_init_only_once_and_model_change_reported() {
    let (mut p, _) = ready();
    let init = r#"{"type":"system","subtype":"init","session_id":"s1","model":"claude-haiku","cwd":"/p","tools":["Bash"],"mcp_servers":[{"name":"fs","status":"connected"}],"permissionMode":"default","claude_code_version":"2.1.326","slash_commands":["clear"]}"#;
    let step = p.on_line(init);
    match &step.events[0] {
        AgentEvent::SessionInit {
            native_session_id,
            model,
            tools,
            mcp_servers,
            slash_commands,
            current_mode,
            cli_version,
            ..
        } => {
            assert_eq!(native_session_id.as_deref(), Some("s1"));
            assert_eq!(model.as_deref(), Some("claude-haiku"));
            assert_eq!(tools, &vec!["Bash".to_string()]);
            assert_eq!(mcp_servers[0].status, "connected");
            assert_eq!(slash_commands[0].name, "clear");
            assert_eq!(current_mode, "default");
            assert_eq!(cli_version, "2.1.326");
        }
        e => panic!("{e:?}"),
    }
    assert!(p.on_line(init).events.is_empty());
    let step = p.on_line(&init.replace("claude-haiku", "claude-sonnet"));
    assert_eq!(
        step.events,
        vec![AgentEvent::ModelChanged {
            model: "claude-sonnet".into()
        }]
    );
}

#[test]
fn assistant_blocks_and_deduplicated_usage() {
    let (mut p, _) = ready();
    let a = r#"{"type":"assistant","message":{"id":"m1","content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"Hello"}],"usage":{"input_tokens":3,"output_tokens":2,"cache_read_input_tokens":5}}}"#;
    let ev = p.on_line(a).events;
    assert_eq!(ev.len(), 3);
    assert!(matches!(&ev[0], AgentEvent::ThinkingDelta { text, .. } if text == "hmm"));
    assert!(
        matches!(&ev[1], AgentEvent::AssistantDelta { text, message_id } if text == "Hello" && message_id.as_deref() == Some("m1"))
    );
    assert!(
        matches!(&ev[2], AgentEvent::UsageUpdate { usage, context_pct: None } if usage.cache_read_tokens == 5)
    );
    // Same message id again: no second usage.
    let ev = p.on_line(a).events;
    assert_eq!(ev.len(), 2);
}

#[test]
fn tool_use_and_result_correlate() {
    let (mut p, _) = ready();
    let ev = p.on_line(r#"{"type":"assistant","message":{"id":"m","content":[{"type":"tool_use","id":"t1","name":"Task","input":{"model":"haiku","description":"x"}}]}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::ToolCallStarted { name, is_subagent: true, subagent_model: Some(m), .. } if name == "Task" && m == "haiku"
    ));
    let ev = p.on_line(r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"done","is_error":true}]}}"#).events;
    assert_eq!(
        ev[0],
        AgentEvent::ToolCallFinished {
            call_id: "t1".into(),
            name: "Task".into(),
            ok: false,
            result: Some(json!("done")),
            parts: vec![]
        }
    );
}

#[test]
fn permission_gate_approve_echoes_input_and_deny_has_message() {
    let (mut p, _) = ready();
    let ev = p.on_line(r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"touch a","description":"Create a"},"permission_suggestions":[{"type":"addRules","rules":[{"toolName":"Bash","ruleContent":"touch a"}],"behavior":"allow","destination":"localSettings"},{"type":"setMode","mode":"acceptEdits","destination":"session"}]}}"#).events;
    match &ev[0] {
        AgentEvent::GateRequested {
            gate:
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    command,
                    explanation,
                    choices,
                    ..
                },
        } => {
            assert_eq!(request_id, "g1");
            assert_eq!(tool_name, "Bash");
            assert_eq!(command, "touch a");
            assert_eq!(explanation, "Create a");
            let ids: Vec<_> = choices.iter().map(|c| c.id.as_str()).collect();
            assert_eq!(ids, ["allow", "allow_always", "deny", "deny_stop"]);
            assert!(choices.iter().all(|c| c.neutral_label));
            assert_eq!(
                choices[1].description,
                "allow rule Bash(touch a) (localSettings); switch to mode acceptEdits (session)"
            );
            assert_eq!(choices[2].text, wasit_core::ChoiceText::Optional);
        }
        e => panic!("{e:?}"),
    }
    let step = sent(p.encode(
        InputId(102),
        Command::AnswerGate {
            request_id: "g1".into(),
            decision: GateDecision::choose("allow"),
        },
    ));
    assert_eq!(
        writes(&step)[0],
        json!({"type":"control_response","response":{"subtype":"success","request_id":"g1","response":{"behavior":"allow","updatedInput":{"command":"touch a","description":"Create a"}}}})
    );
    // Answered gates are gone.
    assert!(matches!(
        p.encode(
            InputId(103),
            Command::AnswerGate {
                request_id: "g1".into(),
                decision: GateDecision::choose("allow")
            }
        ),
        Err(wasit_core::DriverError::NotFound { .. })
    ));
    // "Allow and remember" echoes every suggestion as updatedPermissions.
    p.on_line(r#"{"type":"control_request","request_id":"g3","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"ls"},"permission_suggestions":[{"type":"addRules","rules":[{"toolName":"Bash","ruleContent":"ls"}],"behavior":"allow","destination":"session"}]}}"#);
    let step = sent(p.encode(
        InputId(104),
        Command::AnswerGate {
            request_id: "g3".into(),
            decision: GateDecision::choose("allow_always"),
        },
    ));
    assert_eq!(
        writes(&step)[0]["response"]["response"]["updatedPermissions"][0]["rules"][0]
            ["ruleContent"],
        "ls"
    );
    p.on_line(r#"{"type":"control_request","request_id":"g2","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm x"}}}"#);
    let step = sent(p.encode(
        InputId(105),
        Command::AnswerGate {
            request_id: "g2".into(),
            decision: GateDecision::choose_with("deny", Some("no")),
        },
    ));
    assert_eq!(
        writes(&step)[0]["response"]["response"],
        json!({"behavior":"deny","message":"no"})
    );
    // Without suggestions there is no "remember" choice; deny_stop interrupts.
    p.on_line(r#"{"type":"control_request","request_id":"g4","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm y"}}}"#);
    assert!(matches!(
        p.encode(
            InputId(106),
            Command::AnswerGate {
                request_id: "g4".into(),
                decision: GateDecision::choose("allow_always")
            }
        ),
        Err(wasit_core::DriverError::Invalid { .. })
    ));
    let step = sent(p.encode(
        InputId(107),
        Command::AnswerGate {
            request_id: "g4".into(),
            decision: GateDecision::choose("deny_stop"),
        },
    ));
    assert_eq!(
        writes(&step)[0]["response"]["response"],
        json!({"behavior":"deny","message":"denied by user","interrupt":true})
    );
}

#[test]
fn allow_always_lists_each_suggestion_as_an_effect() {
    let (mut p, _) = ready();
    let ev = p.on_line(r#"{"type":"control_request","request_id":"g5","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"npm test"},"permission_suggestions":[{"type":"addRules","rules":[{"toolName":"Bash","ruleContent":"npm test:*"}],"behavior":"allow","destination":"localSettings"},{"type":"addDirectories","directories":["/data/reports"],"destination":"session"},{"type":"setMode","mode":"acceptEdits","destination":"session"}]}}"#).events;
    match &ev[0] {
        AgentEvent::GateRequested {
            gate: GateRequest::Permission { choices, .. },
        } => {
            let by_id = |id: &str| choices.iter().find(|c| c.id == id).expect(id);
            assert_eq!(
                by_id("allow_always").effects,
                [
                    "allow rule Bash(npm test:*) (localSettings)",
                    "add directory /data/reports (session)",
                    "switch to mode acceptEdits (session)",
                ]
            );
            assert!(by_id("allow").effects.is_empty());
            assert!(by_id("deny").effects.is_empty());
            assert!(by_id("deny_stop").effects.is_empty());
        }
        e => panic!("{e:?}"),
    }
    // A choice from a document without the field still deserializes, with no effects.
    let old: wasit_core::GateChoice = serde_json::from_value(json!({
        "id": "allow",
        "label": "Allow",
        "kind": "allow_once"
    }))
    .unwrap();
    assert!(old.effects.is_empty());
}

#[test]
fn question_and_plan_and_elicitation_gates() {
    let (mut p, _) = ready();
    let ev = p.on_line(r#"{"type":"control_request","request_id":"q1","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","input":{"questions":[{"question":"Color?","header":"C","multiSelect":true,"options":[{"label":"Red","description":"r"}]}]}}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::GateRequested { gate: GateRequest::Question { questions, .. } } if questions[0].multi_select && questions[0].options[0].label == "Red"
    ));
    // Wrong command kind is rejected and the gate stays pending.
    assert!(matches!(
        p.encode(
            InputId(108),
            Command::AnswerGate {
                request_id: "q1".into(),
                decision: GateDecision::choose("allow")
            }
        ),
        Err(wasit_core::DriverError::Invalid { .. })
    ));
    let mut answer = QuestionAnswer::default();
    answer
        .answers
        .insert("Color?".into(), vec!["Red".into(), "my own".into()]);
    let step = sent(p.encode(
        InputId(109),
        Command::AnswerQuestion {
            request_id: "q1".into(),
            answer,
        },
    ));
    let w = writes(&step);
    assert_eq!(
        w[0]["response"]["response"]["updatedInput"]["answers"],
        json!({"Color?":"Red, my own"})
    );
    assert_eq!(
        w[0]["response"]["response"]["updatedInput"]["questions"][0]["question"],
        "Color?"
    );

    let ev = p.on_line(r#"{"type":"control_request","request_id":"p1","request":{"subtype":"can_use_tool","tool_name":"ExitPlanMode","input":{"plan":"1. do it"}}}"#).events;
    assert_eq!(
        ev[0],
        AgentEvent::GateRequested {
            gate: GateRequest::Plan {
                request_id: "p1".into(),
                summary: "1. do it".into(),
                choices: vec![
                    wasit_core::GateChoice::neutral(
                        "allow",
                        "Approve plan",
                        wasit_core::GateChoiceKind::AllowOnce
                    ),
                    wasit_core::GateChoice::neutral(
                        "deny",
                        "Keep planning",
                        wasit_core::GateChoiceKind::RejectOnce
                    )
                    .with_text(wasit_core::ChoiceText::Optional),
                    wasit_core::GateChoice::neutral(
                        "deny_stop",
                        "Deny and stop the turn",
                        wasit_core::GateChoiceKind::Cancel
                    )
                    .with_text(wasit_core::ChoiceText::Optional),
                ]
            }
        }
    );

    let ev = p.on_line(r#"{"type":"control_request","request_id":"e1","request":{"subtype":"elicitation","mcp_server_name":"hares","message":"Grant?","requested_schema":{"properties":{"grant":{"enum":["once","deny"]}}}}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::GateRequested { gate: GateRequest::Permission { tool_name, choices, .. } } if tool_name == "hares" && choices.len() == 4 && choices[0].id == "accept:once"
    ));
    let step = sent(p.encode(
        InputId(110),
        Command::AnswerGate {
            request_id: "e1".into(),
            decision: GateDecision::choose("accept:once"),
        },
    ));
    assert_eq!(
        writes(&step)[0]["response"]["response"],
        json!({"action":"accept","content":{"grant":"once"}})
    );
}

#[test]
fn unsupported_control_request_gets_error_reply() {
    let (mut p, _) = ready();
    let step = p.on_line(
        r#"{"type":"control_request","request_id":"h1","request":{"subtype":"hook_callback"}}"#,
    );
    assert_eq!(writes(&step)[0]["response"]["subtype"], "error");
    assert_eq!(writes(&step)[0]["response"]["request_id"], "h1");
}

#[test]
fn interrupt_declines_gates_then_interrupts_and_marks_cancel() {
    let (mut p, _) = ready();
    sent(p.encode(InputId(111), Command::prompt("go")));
    p.on_line(TURN_INIT);
    p.on_line(r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"x"}}}"#);
    let step = sent(p.encode(InputId(112), Command::Interrupt));
    let w = writes(&step);
    assert_eq!(w.len(), 2);
    assert_eq!(w[0]["response"]["response"]["behavior"], "deny");
    assert_eq!(
        w[1],
        json!({"type":"control_request","request_id":"req_2","request":{"subtype":"interrupt"}})
    );
    assert_eq!(
        step.events,
        vec![AgentEvent::GateResolved {
            request_id: "g1".into(),
            reason: None,
        }]
    );
    // A second interrupt during the same turn writes nothing.
    assert!(sent(p.encode(InputId(113), Command::Interrupt))
        .write
        .is_empty());
    let ev = p.on_line(r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_2","response":{"still_queued":[]}}}"#).events;
    assert!(ev.is_empty());
    let ev = p.on_line(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":2,"errors":["x"]}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            was_interrupted: true,
            reply: None,
            ..
        }
    ));
    // Idle interrupt: nothing to do.
    assert!(sent(p.encode(InputId(114), Command::Interrupt)).is_empty());
}

#[test]
fn result_variants_and_cumulative_usage() {
    let (mut p, _) = ready();
    p.on_line(TURN_INIT);
    let ev = p
        .on_line(r#"{"type":"result","subtype":"error_max_turns","is_error":true,"num_turns":9}"#)
        .events;
    assert!(matches!(
        &ev[0],
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::MaxTurns),
            ..
        }
    ));
    p.on_line(TURN_INIT);
    let ev = p.on_line(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"errors":["boom"]}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::TurnCompleted { end_reason: Some(EndReason::ExecutionError), reply: Some(r), .. } if r == "boom")
    );
    p.on_line(TURN_INIT);
    let ev = p.on_line(r#"{"type":"result","subtype":"success","is_error":false,"num_turns":1,"result":"hi","total_cost_usd":0.5,"modelUsage":{"a":{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4,"costUSD":0.2},"b":{"inputTokens":10,"outputTokens":20,"costUSD":0.3}}}"#).events;
    match &ev[0] {
        AgentEvent::TurnCompleted {
            end_reason: None,
            usage,
            per_model,
            reply,
            ..
        } => {
            assert_eq!(usage.input_tokens, 11);
            assert_eq!(usage.output_tokens, 22);
            assert_eq!(usage.cache_read_tokens, 3);
            assert_eq!(usage.cache_creation_tokens, 4);
            assert_eq!(usage.cost_usd, 0.5);
            assert_eq!(per_model.len(), 2);
            assert_eq!(reply.as_deref(), Some("hi"));
        }
        e => panic!("{e:?}"),
    }
}

#[test]
fn set_model_mode_and_rejections() {
    let (mut p, _) = ready();
    let step = sent(p.encode(
        InputId(115),
        Command::SetModel {
            model: "sonnet".into(),
        },
    ));
    assert_eq!(
        writes(&step)[0]["request"],
        json!({"subtype":"set_model","model":"sonnet"})
    );
    let ev = p.on_line(r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_2","response":{"model":"claude-sonnet-5-5"}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::ModelChanged {
            model: "claude-sonnet-5-5".into()
        }]
    );
    sent(p.encode(
        InputId(116),
        Command::SetMode {
            mode: "bogus".into(),
        },
    ));
    let ev = p.on_line(r#"{"type":"control_response","response":{"subtype":"error","request_id":"req_3","error":"Cannot set permission mode"}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::CommandRejected {
            command: "set_mode".into(),
            message: "Cannot set permission mode".into()
        }]
    );
    let ev = p
        .on_line(r#"{"type":"system","subtype":"status","status":null,"permissionMode":"plan"}"#)
        .events;
    assert!(
        matches!(&ev[0], AgentEvent::ModesUpdated { current_mode: Some(m), .. } if m == "plan")
    );
}

#[test]
fn slash_command_is_a_user_message() {
    let (mut p, _) = ready();
    let step = sent(p.encode(
        InputId(117),
        Command::RunSlash {
            name: "/context".into(),
            args: String::new(),
        },
    ));
    assert_eq!(
        writes(&step)[0]["message"]["content"][0]["text"],
        "/context"
    );
    p.on_line(TURN_INIT);
    let step = p.on_line(r#"{"type":"result","subtype":"success","is_error":false}"#);
    assert!(step.write.is_empty());
    let step = sent(p.encode(
        InputId(118),
        Command::RunSlash {
            name: "review".into(),
            args: " 12 ".into(),
        },
    ));
    assert_eq!(
        writes(&step)[0]["message"]["content"][0]["text"],
        "/review 12"
    );
}

#[test]
fn shutdown_closes_stdin_and_rejects_more() {
    let (mut p, _) = ready();
    assert!(sent(p.encode(InputId(119), Command::Shutdown)).close_stdin);
    assert_eq!(
        p.encode(InputId(120), Command::prompt("x")),
        Err(wasit_core::DriverError::Shutdown)
    );
}

#[test]
fn compaction_and_rate_limit() {
    let (mut p, _) = ready();
    let ev = p.on_line(r#"{"type":"system","subtype":"compact_boundary","compact_metadata":{"trigger":"manual","pre_tokens":5000}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::Compaction { info } if info.trigger == "manual" && info.pre_tokens == 5000)
    );
    let ev = p.on_line(r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","resetsAt":7,"rateLimitType":"seven_day","isUsingOverage":false}}"#).events;
    assert!(matches!(&ev[0], AgentEvent::RateLimitNotice { info } if info.resets_at == 7));
}

// ── descriptor ────────────────────────────────────────────────────────────────

#[test]
fn argv_shapes() {
    let d = ClaudeDescriptor;
    let base = d.argv(&SpawnSpec::default());
    assert_eq!(
        base,
        [
            "claude",
            "--print",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-prompt-tool",
            "stdio"
        ]
    );
    let argv = d.argv(&SpawnSpec {
        bin: Some("/opt/claude".into()),
        model: Some("haiku".into()),
        mode: "plan".into(),
        resume: ResumeMode::Resume("abc".into()),
        extra_args: vec!["--x".into()],
        first_prompt: Some("never on argv".into()),
        ..SpawnSpec::default()
    });
    assert_eq!(argv[0], "/opt/claude");
    let joined = argv.join(" ");
    assert!(joined.contains("--model haiku"));
    assert!(joined.contains("--permission-mode plan"));
    assert!(joined.contains("--resume abc"));
    assert!(joined.ends_with("--x"));
    assert!(!joined.contains("never on argv"));
    let argv = d.argv(&SpawnSpec {
        resume: ResumeMode::Continue,
        ..SpawnSpec::default()
    });
    assert!(argv.contains(&"--continue".to_string()));
}

/// A caller-chosen conversation id and a name go to the CLI only the way the real one takes them
/// (claude 2.1.293): `--session-id` starts a fresh conversation under the id, never together
/// with `--resume`; `--name` writes the title into claude's own transcript.
#[test]
fn argv_passes_a_chosen_id_and_a_name() {
    let d = ClaudeDescriptor;
    let id = "3b241101-e2bb-4255-8caf-4136c566a962";
    let argv = d.argv(&SpawnSpec {
        native_session_id: Some(id.into()),
        name: Some("My session".into()),
        ..SpawnSpec::default()
    });
    assert!(argv
        .windows(2)
        .any(|w| w[0] == "--session-id" && w[1] == id));
    assert!(argv
        .windows(2)
        .any(|w| w[0] == "--name" && w[1] == "My session"));
    // Resuming: the conversation has its id; a chosen id is not passed on top of it.
    let argv = d.argv(&SpawnSpec {
        resume: ResumeMode::Resume(id.into()),
        native_session_id: Some(id.into()),
        ..SpawnSpec::default()
    });
    assert!(argv.windows(2).any(|w| w[0] == "--resume" && w[1] == id));
    assert!(!argv.contains(&"--session-id".to_string()));
    assert!(!argv.contains(&"--name".to_string()));
    // Nothing chosen, nothing named: the argv of a session of an older build.
    let argv = d.argv(&SpawnSpec::default());
    assert!(!argv.contains(&"--session-id".to_string()));
    assert!(!argv.contains(&"--name".to_string()));
}

/// The id the runtime asks for is a version 4 UUID, which `--session-id` requires.
#[test]
fn chosen_ids_are_version_4_uuids_of_the_entropy_given() {
    let d = ClaudeDescriptor;
    let id = d.chosen_session_id([0xff; 16]).unwrap();
    assert_eq!(id, "ffffffff-ffff-4fff-bfff-ffffffffffff");
    let id = d.chosen_session_id([0; 16]).unwrap();
    assert_eq!(id, "00000000-0000-4000-8000-000000000000");
    let a = d.chosen_session_id([1; 16]);
    assert_eq!(a, d.chosen_session_id([1; 16]));
    assert_ne!(a, d.chosen_session_id([2; 16]));
}

#[test]
fn the_users_own_command_resumes_by_id() {
    assert_eq!(
        ClaudeDescriptor.user_resume_argv("abc").unwrap(),
        ["claude", "--resume", "abc"]
    );
    assert_eq!(
        ClaudeDescriptor.saves_conversation(),
        wasit_core::SavesConversation::WhenReported
    );
}

#[test]
fn render_config_all_items() {
    let d = ClaudeDescriptor;
    let mut spec = SpawnSpec::default();
    let cfg = AgentConfig {
        mcp_servers: vec![McpServerConfig {
            name: "wasl".into(),
            transport: McpTransport::Stdio {
                command: "wasl".into(),
                args: vec!["mcp".into()],
                env: Default::default(),
            },
            trusted: true,
        }],
        skills: vec![
            SkillRef {
                name: "a".into(),
                root: "/p/.claude/skills".into(),
            },
            SkillRef {
                name: "b".into(),
                root: "/elsewhere".into(),
            },
        ],
        plugins: vec![PluginRef {
            name: "pl".into(),
            path: "/pl".into(),
        }],
        extra_instructions: Some("Be brief.".into()),
        env: vec![("K".into(), "V".into())],
        tool_policy: wasit_core::ToolPolicy {
            disallowed_tools: vec!["Bash".into(), "Edit".into()],
        },
        disabled: vec![],
    };
    let unsupported = d.render_config(&cfg, &mut spec);
    assert_eq!(unsupported.len(), 1);
    assert_eq!(unsupported[0].item, "skill:b");
    assert_eq!(
        spec.extra_args,
        [
            "--mcp-config",
            r#"{"mcpServers":{"wasl":{"args":["mcp"],"command":"wasl","env":{},"type":"stdio"}}}"#,
            "--add-dir",
            "/p",
            "--plugin-dir",
            "/pl",
            "--append-system-prompt",
            "Be brief.",
            "--allowedTools",
            "mcp__wasl",
            "--disallowedTools",
            "Bash,Edit"
        ]
    );
    assert_eq!(d.env(&spec), vec![("K".to_string(), "V".to_string())]);
}

#[test]
fn discovery_parsers() {
    let help = ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: "  --permission-mode <mode>  Permission mode\n   (choices: \"acceptEdits\", \"auto\",\n   \"manual\", \"plan\")\n".into(),
    };
    match parse_permission_modes(&help).unwrap() {
        Discovered::Modes(m) => {
            // The CLI's default first (clients preselect it), with display words.
            assert_eq!(
                m.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
                ["default", "acceptEdits", "auto", "plan"]
            );
            assert_eq!(m[1].label, "Accept edits");
        }
        d => panic!("{d:?}"),
    }
    let files = ProbeOutput::Files {
        files: vec![
            wasit_core::FileHead {
                path: "/h/.claude/projects/-p/0b7c2b3e-1111-4222-8333-944445555666.jsonl".into(),
                modified_ms: 5,
                head: vec![
                    r#"{"type":"queue-operation"}"#.into(),
                    r#"{"type":"user","cwd":"/p","message":{"role":"user","content":[{"type":"text","text":"fix it"}]}}"#.into(),
                ],
                matching_lines: Some(7),
            },
            wasit_core::FileHead {
                path: "/h/.claude/projects/-p/agent-x.jsonl".into(),
                modified_ms: 1,
                head: vec![],
                matching_lines: None,
            },
        ],
    };
    match parse_sessions(&files).unwrap() {
        Discovered::Sessions(s) => {
            assert_eq!(s.len(), 1);
            assert_eq!(s[0].project.as_deref(), Some("/p"));
            assert_eq!(s[0].title, "fix it");
            assert_eq!(s[0].message_count, Some(7));
        }
        d => panic!("{d:?}"),
    }
    let ctx = DiscoveryContext {
        home: Some("/h".into()),
        project: Some("/a/b.c".into()),
        ..DiscoveryContext::default()
    };
    let probes: Vec<_> = ClaudeDescriptor
        .discovery(&ctx)
        .into_iter()
        .filter(|p| p.kind != wasit_core::DiscoveryKind::Config)
        .collect();
    assert_eq!(probes.len(), 4);
    assert!(
        matches!(&probes[3].action, ProbeAction::ListFiles { dir, recursive: false, .. } if dir == "/h/.claude/projects/-a-b-c")
    );
}

#[test]
fn windows_paths_list_the_same_sessions() {
    let uuid = "0b7c2b3e-1111-4222-8333-944445555666";
    let line = r#"{"type":"user","cwd":"C:\\Users\\me\\p","message":{"role":"user","content":[{"type":"text","text":"fix it"}]}}"#;
    let sessions = |path: String| {
        let out = ProbeOutput::Files {
            files: vec![wasit_core::FileHead {
                path,
                modified_ms: 5,
                head: vec![line.into()],
                matching_lines: Some(7),
            }],
        };
        match parse_sessions(&out).unwrap() {
            Discovered::Sessions(s) => s,
            d => panic!("{d:?}"),
        }
    };
    let posix = sessions(format!("/h/.claude/projects/-p/{uuid}.jsonl"));
    let windows = sessions(format!(
        r"C:\Users\me\.claude\projects\C--Users-me-p\{uuid}.jsonl"
    ));
    assert_eq!(posix.len(), 1);
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].id, uuid);
    assert_eq!(windows[0].id, posix[0].id);
    assert_eq!(windows[0].project, posix[0].project);
    assert_eq!(windows[0].title, "fix it");
    assert_eq!(windows[0].message_count, Some(7));
    // A subagent transcript is skipped in the Windows form too.
    assert!(sessions(r"C:\Users\me\.claude\projects\C--p\agent-x.jsonl".into()).is_empty());
}

#[test]
fn determinism_same_inputs_same_outputs() {
    let run = || {
        let mut p = ClaudeProtocol::new();
        let mut out = vec![format!(
            "{:?}",
            p.start(&SpawnSpec {
                session_id: Some("aaaaaaaaaaaa".into()),
                ..SpawnSpec::default()
            })
        )];
        out.push(format!("{:?}", p.encode(InputId(1), Command::prompt("a"))));
        for l in [
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#,
            r#"{"type":"control_request","request_id":"g","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"b":1,"a":2}}}"#,
        ] {
            out.push(format!("{:?}", p.on_line(l)));
        }
        out.push(format!(
            "{:?}",
            p.encode(
                InputId(121),
                Command::SetMode {
                    mode: "plan".into()
                }
            )
        ));
        out.push(format!("{:?}", p.encode(InputId(122), Command::Interrupt)));
        out
    };
    assert_eq!(run(), run());
}

#[test]
fn ready_only_after_the_initialize_response() {
    let mut p = ClaudeProtocol::new();
    assert!(!p.is_ready());
    p.start(&SpawnSpec::default());
    assert!(!p.is_ready());
    let (p2, _) = ready();
    assert!(p2.is_ready());
    p.on_line(r#"{"type":"control_response","response":{"subtype":"error","request_id":"req_1","error":"no"}}"#);
    assert!(p.is_ready(), "a failed initialize still ends the handshake");
}

// ── config discovery + disabling (shapes captured from claude 2.1.327) ──────

#[test]
fn state_file_yields_user_and_local_mcp_servers() {
    let text = r#"{"numStartups":3,"mcpServers":{"ustdio":{"type":"stdio","command":"echo","args":["hi"],"env":{}}},
        "projects":{"/w/proj":{"allowedTools":[],"mcpServers":{"lhttp":{"type":"http","url":"https://example.invalid/mcp"}}}}}"#;
    let out = wasit_core::ProbeOutput::File {
        path: "/h/.claude.json".into(),
        contents: Some(text.into()),
    };
    let Discovered::Config(items) = crate::parse_state_mcp(&out, Some("/w/proj")).unwrap() else {
        panic!()
    };
    assert_eq!(items.len(), 2);
    assert_eq!(
        (items[0].name.as_str(), items[0].source.scope.as_str()),
        ("ustdio", "user")
    );
    assert_eq!(
        items[0].mcp,
        Some(wasit_core::McpTransport::Stdio {
            command: "echo".into(),
            args: vec!["hi".into()],
            env: Default::default()
        })
    );
    assert_eq!(
        (items[1].name.as_str(), items[1].source.scope.as_str()),
        ("lhttp", "local")
    );
    let missing = wasit_core::ProbeOutput::File {
        path: "/nope".into(),
        contents: None,
    };
    assert_eq!(
        crate::parse_state_mcp(&missing, None).unwrap(),
        Discovered::Config(vec![])
    );
}

#[test]
fn project_mcp_and_plugin_list() {
    let mcp = wasit_core::ProbeOutput::File {
        path: "/w/proj/.mcp.json".into(),
        contents: Some(r#"{"mcpServers":{"pstdio":{"type":"stdio","command":"/bin/cat","args":["x"],"env":{"FOO":"bar"}}}}"#.into()),
    };
    let Discovered::Config(items) = crate::parse_project_mcp(&mcp).unwrap() else {
        panic!()
    };
    assert_eq!(items[0].source.scope, "project");
    let plugins = wasit_core::ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: r#"[{"id":"demo@localmkt","version":"1.0.0","scope":"user","enabled":true,"installPath":"/h/.claude/plugins/cache/localmkt/demo/1.0.0","projectEnabled":false}]"#.into(),
    };
    let Discovered::Config(items) = crate::parse_plugin_list(&plugins).unwrap() else {
        panic!()
    };
    assert_eq!(items[0].name, "demo@localmkt");
    assert!(items[0].enabled);
    assert!(crate::parse_plugin_list(&wasit_core::ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: "nope".into()
    })
    .is_err());
}

#[test]
fn config_probes_cover_files_plugins_and_directories() {
    let ctx = DiscoveryContext {
        binary: Some("claude".into()),
        home: Some("/h".into()),
        env: vec![],
        project: Some("/w/proj".into()),
    };
    let probes: Vec<_> = descriptor()
        .discovery(&ctx)
        .into_iter()
        .filter(|p| p.kind == wasit_core::DiscoveryKind::Config)
        .collect();
    // state file, .mcp.json, plugin list, 2 roots × (skills, agents, commands)
    assert_eq!(probes.len(), 9);
    assert!(probes.iter().any(|p| matches!(&p.action,
        wasit_core::ProbeAction::ReadFile { path, .. } if path == "/h/.claude.json")));
    assert!(probes.iter().any(|p| matches!(&p.action,
        wasit_core::ProbeAction::ListFiles { dir, .. } if dir == "/w/proj/.claude/skills")));
}

#[test]
fn disabled_items_render_as_one_settings_flag() {
    use wasit_core::{ConfigItemKind as K, DisabledItem};
    let d = |kind, name: &str| DisabledItem {
        kind,
        name: name.into(),
        path: None,
    };
    let cfg = AgentConfig {
        disabled: vec![
            d(K::Skill, "myskill"),
            d(K::Command, "hello"),
            d(K::McpServer, "pstdio"),
            d(K::Plugin, "demo@localmkt"),
            d(K::Agent, "reviewer"),
        ],
        ..AgentConfig::default()
    };
    let mut spec = SpawnSpec::default();
    let unsupported = descriptor().render_config(&cfg, &mut spec);
    assert_eq!(unsupported.len(), 1);
    assert_eq!(unsupported[0].item, "agent:reviewer");
    let at = spec
        .extra_args
        .iter()
        .position(|a| a == "--settings")
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&spec.extra_args[at + 1]).unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "skillOverrides": {"myskill": "off", "hello": "off"},
            "deniedMcpServers": [{"serverName": "pstdio"}],
            "enabledPlugins": {"demo@localmkt": false}
        })
    );
}

// ── inputs: queue, acknowledgement, drops (lifecycle frames as claude 2.1.293 prints them) ──

const INIT_OK: &str = r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#;
const RESULT_OK: &str =
    r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1}"#;

/// A ready protocol that sends message uuids (the runtime gave it a session id).
fn ready_with_ids() -> ClaudeProtocol {
    let mut p = ClaudeProtocol::new();
    p.start(&SpawnSpec {
        session_id: Some("abcdefghijkl".into()),
        ..SpawnSpec::default()
    });
    p.on_line(INIT_OK);
    p
}

fn uuid_of(step: &Step) -> String {
    let w = writes(step);
    let user = w
        .iter()
        .find(|v| v["type"] == "user")
        .expect("a user message");
    user["uuid"].as_str().expect("a uuid").to_string()
}

fn lifecycle(uuid: &str, state: &str) -> String {
    format!(
        r#"{{"type":"command_lifecycle","command_uuid":"{uuid}","state":"{state}","uuid":"e9cc3bf9-5691-43dc-9491-cf9459579ef0","session_id":"be6990f7-288c-4c6e-ace3-a5547598c189"}}"#
    )
}

fn drops(events: &[AgentEvent]) -> Vec<(InputId, wasit_core::InputDropReason)> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputDropped { input, reason, .. } => Some((*input, *reason)),
            _ => None,
        })
        .collect()
}

fn turn_starts(events: &[AgentEvent]) -> Vec<Option<InputId>> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::TurnStarted { input, .. } => Some(*input),
            _ => None,
        })
        .collect()
}

#[test]
fn message_uuids_are_deterministic_per_session_and_input() {
    let line = |session: &str, input: u64| {
        let mut p = ClaudeProtocol::new();
        p.start(&SpawnSpec {
            session_id: Some(session.into()),
            ..SpawnSpec::default()
        });
        p.on_line(INIT_OK);
        uuid_of(&sent(p.encode(InputId(input), Command::prompt("x"))))
    };
    let a = line("abcdefghijkl", 7);
    assert_eq!(a, line("abcdefghijkl", 7));
    assert_ne!(a, line("abcdefghijkl", 8));
    assert_ne!(a, line("mnopqrstuvwx", 7));
    // RFC 9562 shape: 8-4-4-4-12 hex, version 4, variant 10xx.
    let parts: Vec<&str> = a.split('-').collect();
    assert_eq!(
        parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
        [8, 4, 4, 4, 12]
    );
    assert!(a.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
    assert!(parts[2].starts_with('4'));
    assert!(matches!(parts[3].as_bytes()[0], b'8' | b'9' | b'a' | b'b'));
}

#[test]
fn two_queued_prompts_then_interrupt_are_dropped_and_never_start() {
    let mut p = ready_with_ids();
    let first = sent(p.encode(InputId(10), Command::prompt("long")));
    assert!(
        turn_starts(&first.events).is_empty(),
        "the CLI starts the turn"
    );
    p.on_line(TURN_INIT);
    assert_eq!(
        p.encode(InputId(11), Command::prompt("second")).unwrap(),
        Encoded::Queued
    );
    assert_eq!(
        p.encode(InputId(12), Command::prompt("third")).unwrap(),
        Encoded::Queued
    );
    let step = sent(p.encode(InputId(13), Command::Interrupt));
    assert_eq!(
        drops(&step.events),
        vec![
            (InputId(11), wasit_core::InputDropReason::Interrupted),
            (InputId(12), wasit_core::InputDropReason::Interrupted)
        ]
    );
    // No lifecycle seen: no cancellation is asked for, only the interrupt.
    let w = writes(&step);
    assert_eq!(w.len(), 1);
    assert_eq!(w[0]["request"]["subtype"], "interrupt");
    let step = p.on_line(
        r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":2}"#,
    );
    assert!(matches!(
        step.events[..],
        [AgentEvent::TurnCompleted {
            was_interrupted: true,
            ..
        }]
    ));
    assert!(step.write.is_empty(), "the dropped prompts are never sent");
    assert!(turn_starts(&step.events).is_empty());
}

#[test]
fn interrupt_before_ready_drops_the_queued_prompts() {
    let mut p = ClaudeProtocol::new();
    p.start(&SpawnSpec::default());
    assert_eq!(
        p.encode(InputId(2), Command::prompt("first")).unwrap(),
        Encoded::Queued
    );
    assert_eq!(
        p.encode(
            InputId(3),
            Command::SetModel {
                model: "haiku".into()
            }
        )
        .unwrap(),
        Encoded::Queued
    );
    let step = sent(p.encode(InputId(4), Command::Interrupt));
    assert!(step.write.is_empty());
    assert_eq!(
        drops(&step.events),
        vec![(InputId(2), wasit_core::InputDropReason::Interrupted)]
    );
    // The handshake sends what is left (the model switch), never the dropped prompt.
    let step = p.on_line(INIT_OK);
    assert_eq!(
        step.events,
        vec![AgentEvent::InputSent { input: InputId(3) }]
    );
    assert_eq!(writes(&step)[0]["request"]["subtype"], "set_model");
    assert!(turn_starts(&step.events).is_empty());
}

#[test]
fn shutdown_drops_everything_still_queued() {
    let mut p = ClaudeProtocol::new();
    p.start(&SpawnSpec::default());
    p.encode(InputId(2), Command::prompt("a")).unwrap();
    p.encode(
        InputId(3),
        Command::SetMode {
            mode: "plan".into(),
        },
    )
    .unwrap();
    let step = sent(p.encode(InputId(4), Command::Shutdown));
    assert!(step.close_stdin);
    assert_eq!(
        drops(&step.events),
        vec![
            (InputId(2), wasit_core::InputDropReason::Shutdown),
            (InputId(3), wasit_core::InputDropReason::Shutdown)
        ]
    );
    // Ready later: nothing goes out.
    assert!(p.on_line(INIT_OK).write.is_empty());
}

/// Acknowledged on the CLI's own report for this message (`started`), never on output the CLI
/// produces by itself: a CLI-initiated turn (`system/init` … `result` with an `origin`), a
/// background task's frames, lifecycle frames for messages that are not ours.
#[test]
fn acknowledgement_comes_from_the_message_lifecycle_only() {
    let mut p = ready_with_ids();
    let step = sent(p.encode(InputId(5), Command::prompt("go")));
    let uuid = uuid_of(&step);
    let unrelated = [
        r#"{"type":"system","subtype":"task_notification","task_id":"b1","tool_use_id":"t1","status":"completed","output_file":"","summary":"s"}"#.to_string(),
        r#"{"type":"system","subtype":"init","session_id":"s1","model":"claude-haiku","cwd":"/p","tools":[],"mcp_servers":[],"permissionMode":"default","claude_code_version":"2.1.293","slash_commands":[]}"#.to_string(),
        r#"{"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"on my own"}]}}"#.to_string(),
        r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"x"}]}}"#.to_string(),
        lifecycle("11111111-1111-4111-8111-111111111111", "started"),
        lifecycle(&uuid, "queued"),
    ];
    for l in &unrelated {
        let ev = p.on_line(l).events;
        assert!(
            !ev.iter()
                .any(|e| matches!(e, AgentEvent::InputAcknowledged { .. })),
            "{l} acknowledged: {ev:?}"
        );
    }
    // The `system/init` above opened a turn of the CLI's own: the message joined it.
    let ev = p.on_line(&lifecycle(&uuid, "started")).events;
    assert_eq!(
        ev,
        vec![AgentEvent::InputAcknowledged {
            input: InputId(5),
            steered: true
        }]
    );
    // Once.
    let ev = p.on_line(&format!(
        r#"{{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1,"user_message_uuids":["{uuid}"]}}"#
    ));
    assert!(matches!(ev.events[..], [AgentEvent::TurnCompleted { .. }]));
    assert!(p.on_line(&lifecycle(&uuid, "completed")).events.is_empty());
}

#[test]
fn a_result_listing_the_message_acknowledges_it_without_lifecycle_frames() {
    let mut p = ready_with_ids();
    let uuid = uuid_of(&sent(p.encode(InputId(5), Command::prompt("go"))));
    p.on_line(TURN_INIT);
    let ev = p
        .on_line(&format!(
            r#"{{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1,"user_message_uuids":["{uuid}"]}}"#
        ))
        .events;
    assert_eq!(
        ev[0],
        AgentEvent::InputAcknowledged {
            input: InputId(5),
            steered: false
        }
    );
    assert!(matches!(ev[1], AgentEvent::TurnCompleted { .. }));
}

/// A CLI that reports nothing about messages (or a session from before message ids) leaves
/// its prompts written but unacknowledged, whatever it prints.
#[test]
fn without_lifecycle_reports_nothing_is_acknowledged() {
    let (mut p, _) = ready();
    let step = sent(p.encode(InputId(5), Command::prompt("go")));
    assert!(writes(&step)[0].get("uuid").is_none());
    let mut events = vec![];
    for l in [
        r#"{"type":"system","subtype":"init","session_id":"s1","model":"m","cwd":"/p","tools":[],"mcp_servers":[],"permissionMode":"default","claude_code_version":"2.1.326","slash_commands":[]}"#,
        r#"{"type":"assistant","message":{"id":"m1","content":[{"type":"text","text":"hi"}]}}"#,
        RESULT_OK,
    ] {
        events.extend(p.on_line(l).events);
    }
    assert!(!events
        .iter()
        .any(|e| matches!(e, AgentEvent::InputAcknowledged { .. })));
    // An interrupt does not ask such a CLI to cancel anything.
    sent(p.encode(InputId(6), Command::prompt("again")));
    let w = writes(&sent(p.encode(InputId(7), Command::Interrupt)));
    assert_eq!(w.len(), 1);
    assert_eq!(w[0]["request"]["subtype"], "interrupt");
}

/// A message the CLI has read but not started (it sits in the CLI's own queue, behind a turn the
/// CLI started by itself) is cancelled before the interrupt; the CLI's `cancelled` report drops
/// it. No turn of ours ends with it: the turn that runs is the CLI's own, and its `result` ends it.
#[test]
fn interrupt_cancels_a_message_waiting_in_the_clis_queue() {
    let mut p = ready_with_ids();
    // An earlier message shows the CLI reports lifecycles.
    let u1 = uuid_of(&sent(p.encode(InputId(5), Command::prompt("one"))));
    p.on_line(&lifecycle(&u1, "queued"));
    p.on_line(&lifecycle(&u1, "started"));
    p.on_line(TURN_INIT);
    p.on_line(RESULT_OK);
    p.on_line(&lifecycle(&u1, "completed"));
    // The next one is read, and then the CLI starts a turn of its own before this one.
    let u2 = uuid_of(&sent(p.encode(InputId(6), Command::prompt("two"))));
    p.on_line(&lifecycle(&u2, "queued"));
    let ev = p.on_line(TURN_INIT).events;
    assert!(matches!(
        ev[..],
        [AgentEvent::TurnStarted {
            input: None,
            trigger: TurnTrigger::Agent,
            ..
        }]
    ));
    let step = sent(p.encode(InputId(7), Command::Interrupt));
    let w = writes(&step);
    assert_eq!(w.len(), 2, "{w:?}");
    assert_eq!(w[0]["request"]["subtype"], "cancel_async_message");
    assert_eq!(w[0]["request"]["message_uuid"], u2.as_str());
    assert_eq!(w[1]["request"]["subtype"], "interrupt");
    let cancel_id = w[0]["request_id"].as_str().unwrap().to_string();
    // A prompt sent meanwhile waits behind the CLI's turn.
    assert_eq!(
        p.encode(InputId(8), Command::prompt("three")).unwrap(),
        Encoded::Queued
    );
    // The CLI: `cancelled` frame first, then the answer (real order, 2.1.293).
    let step = p.on_line(&lifecycle(&u2, "cancelled"));
    assert_eq!(
        drops(&step.events),
        vec![(InputId(6), wasit_core::InputDropReason::Interrupted)]
    );
    assert_eq!(
        step.events.len(),
        1,
        "no turn of ours ends: {:?}",
        step.events
    );
    assert!(step.write.is_empty(), "the CLI's turn is still open");
    let ev = p.on_line(&format!(r#"{{"type":"control_response","response":{{"subtype":"success","request_id":"{cancel_id}","response":{{"cancelled":true}}}}}}"#)).events;
    assert!(ev.is_empty(), "dropped once: {ev:?}");
    // The CLI's turn ends (interrupted); the held prompt goes out.
    let step = p.on_line(
        r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1,"origin":{"kind":"task-notification","producer":"session-task","runId":"r1"}}"#,
    );
    assert!(matches!(
        &step.events[..],
        [
            AgentEvent::TurnCompleted {
                end_reason: Some(EndReason::Cancelled),
                was_interrupted: true,
                origin: Some(wasit_core::TurnOrigin { kind, .. }),
                ..
            },
            AgentEvent::InputSent { input: InputId(8) }
        ] if kind == "task-notification"
    ));
    assert_eq!(writes(&step).len(), 1);
}

/// Asked too late: the message had started (`cancelled:false`); it runs, is acknowledged, and the
/// interrupt that was meant for its turn goes out then; its interrupted turn ends with the
/// `result`.
#[test]
fn a_cancel_that_comes_too_late_interrupts_the_turn_that_started() {
    let mut p = ready_with_ids();
    let u1 = uuid_of(&sent(p.encode(InputId(5), Command::prompt("one"))));
    p.on_line(&lifecycle(&u1, "queued"));
    let w = writes(&sent(p.encode(InputId(6), Command::Interrupt)));
    assert_eq!(
        w.len(),
        1,
        "nothing runs yet: only the cancellation is asked for"
    );
    assert_eq!(w[0]["request"]["subtype"], "cancel_async_message");
    let cancel_id = w[0]["request_id"].as_str().unwrap().to_string();
    let ev = p.on_line(&lifecycle(&u1, "started")).events;
    assert!(matches!(
        ev[..],
        [AgentEvent::InputAcknowledged { steered: false, .. }]
    ));
    let step = p.on_line(&format!(r#"{{"type":"control_response","response":{{"subtype":"success","request_id":"{cancel_id}","response":{{"cancelled":false}}}}}}"#));
    assert!(step.events.is_empty());
    assert_eq!(writes(&step)[0]["request"]["subtype"], "interrupt");
    let ev = p.on_line(TURN_INIT).events;
    assert!(matches!(ev[0], AgentEvent::SessionInit { .. }));
    assert!(matches!(
        ev[1..],
        [AgentEvent::TurnStarted {
            input: Some(InputId(5)),
            trigger: TurnTrigger::Input,
            ..
        }]
    ));
    let ev = p
        .on_line(
            r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1}"#,
        )
        .events;
    assert!(matches!(
        ev[..],
        [AgentEvent::TurnCompleted {
            was_interrupted: true,
            ..
        }]
    ));
    // The `cancelled` frame for a message that ran is not a drop.
    assert!(p.on_line(&lifecycle(&u1, "cancelled")).events.is_empty());
}

#[test]
fn a_cancel_the_cli_refuses_is_reported() {
    let mut p = ready_with_ids();
    let u1 = uuid_of(&sent(p.encode(InputId(5), Command::prompt("one"))));
    p.on_line(&lifecycle(&u1, "queued"));
    let w = writes(&sent(p.encode(InputId(6), Command::Interrupt)));
    let cancel_id = w[0]["request_id"].as_str().unwrap().to_string();
    let ev = p.on_line(&format!(r#"{{"type":"control_response","response":{{"subtype":"error","request_id":"{cancel_id}","error":"unknown subtype"}}}}"#)).events;
    assert!(
        matches!(&ev[..], [AgentEvent::CommandRejected { command, .. }] if command == "interrupt")
    );
}

// ── turns come from the CLI; background work; subagents (frame shapes of claude 2.1.293) ──

/// `system/init` of a CLI that says it reports message lifecycles.
const TURN_INIT_CAPABLE: &str = r#"{"type":"system","subtype":"init","session_id":"s1","model":"m","cwd":"/p","tools":[],"mcp_servers":[],"permissionMode":"default","claude_code_version":"2.1.293","slash_commands":[],"capabilities":["interrupt_receipt_v1","msg_lifecycle_v1"]}"#;

const TASK_STARTED: &str = r#"{"type":"system","subtype":"task_started","task_id":"a1","run_id":"r1","tool_use_id":"toolu_A","description":"probe","subagent_type":"general-purpose","is_backgrounded":true,"spawn_depth":1,"task_type":"local_agent","prompt":"p"}"#;
const TASKS_LISTED: &str = r#"{"type":"system","subtype":"background_tasks_changed","tasks":[{"task_id":"a1","run_id":"r1","task_type":"local_agent","subagent_type":"general-purpose","description":"probe"}]}"#;
const TASKS_EMPTY: &str = r#"{"type":"system","subtype":"background_tasks_changed","tasks":[]}"#;
const TASK_ENDED: &str = r#"{"type":"system","subtype":"task_notification","task_id":"a1","run_id":"r1","tool_use_id":"toolu_A","status":"completed","output_file":"/o","summary":"hi","usage":{"total_tokens":60846,"tool_uses":1,"duration_ms":2533}}"#;

fn first_turn_start(events: &[AgentEvent]) -> Option<(Option<InputId>, TurnTrigger)> {
    events.iter().find_map(|e| match e {
        AgentEvent::TurnStarted { input, trigger, .. } => Some((*input, *trigger)),
        _ => None,
    })
}

/// The turn starts at `system/init`; which input it runs for is known only from the lifecycle
/// `started` frame that came just before. Three CLIs: one that reports lifecycles for our messages
/// (the turn is attributed; a turn with no `started` frame is the agent's own), one that said it
/// does but has not shown a frame yet (the agent's own turn), one that reports nothing (the
/// driver cannot tell and does not guess).
#[test]
fn a_turn_is_attributed_only_on_the_clis_own_report() {
    // Reports lifecycles.
    let mut p = ready_with_ids();
    let uuid = uuid_of(&sent(p.encode(InputId(5), Command::prompt("go"))));
    p.on_line(&lifecycle(&uuid, "queued"));
    p.on_line(&lifecycle(&uuid, "started"));
    let ev = p.on_line(TURN_INIT).events;
    assert_eq!(
        first_turn_start(&ev),
        Some((Some(InputId(5)), TurnTrigger::Input))
    );
    p.on_line(RESULT_OK);
    p.on_line(&lifecycle(&uuid, "completed"));
    // The next turn has no `started` frame before it: the CLI's own.
    let ev = p.on_line(TURN_INIT).events;
    assert_eq!(first_turn_start(&ev), Some((None, TurnTrigger::Agent)));

    // Said it reports (`capabilities`), no frame seen yet: the first turn of a session is ours,
    // but a turn the CLI starts for a message of ours it has not reported is still the agent's.
    let mut p = ready_with_ids();
    let ev = p.on_line(TURN_INIT_CAPABLE).events;
    assert_eq!(first_turn_start(&ev), Some((None, TurnTrigger::Agent)));

    // Reports nothing: our prompt, then `system/init`: whose turn it is cannot be told.
    let (mut p, _) = ready();
    sent(p.encode(InputId(5), Command::prompt("go")));
    let ev = p.on_line(TURN_INIT).events;
    assert_eq!(first_turn_start(&ev), Some((None, TurnTrigger::Unreported)));
    // The same for a CLI that lists the capability while the messages carry no uuid (no session
    // id was given): there is nothing it could report about.
    let (mut p, _) = ready();
    sent(p.encode(InputId(5), Command::prompt("go")));
    let ev = p.on_line(TURN_INIT_CAPABLE).events;
    assert_eq!(first_turn_start(&ev), Some((None, TurnTrigger::Unreported)));
}

/// A result with no `system/init` before it (a CLI that did not print one): the turn still has a
/// start, marked as unreported.
#[test]
fn a_turn_end_is_never_reported_without_its_start() {
    let (mut p, _) = ready();
    let ev = p.on_line(RESULT_OK).events;
    assert!(matches!(
        ev[..],
        [
            AgentEvent::TurnStarted {
                input: None,
                trigger: TurnTrigger::Unreported,
                ..
            },
            AgentEvent::TurnCompleted { .. }
        ]
    ));
}

/// A message the CLI takes into a turn that is already running joined it: acknowledged as
/// steered, and it starts no turn of its own. (The race: our prompt was written just before the
/// CLI began a turn by itself; it printed `queued`, then `system/init` for its own turn, and took
/// the message in at the next model call, as recorded with 2.1.293.)
#[test]
fn a_message_taken_into_a_turn_the_cli_started_is_acknowledged_as_steered() {
    let mut p = ready_with_ids();
    let u1 = uuid_of(&sent(p.encode(InputId(5), Command::prompt("one"))));
    p.on_line(&lifecycle(&u1, "queued"));
    let ev = p.on_line(TURN_INIT).events;
    assert_eq!(first_turn_start(&ev), Some((None, TurnTrigger::Agent)));
    // The CLI's turn goes on; our message is still waiting in its queue: nothing else is
    // written meanwhile.
    assert_eq!(
        p.encode(InputId(6), Command::prompt("two")).unwrap(),
        Encoded::Queued
    );
    let ev = p.on_line(&lifecycle(&u1, "started")).events;
    assert_eq!(
        ev,
        vec![AgentEvent::InputAcknowledged {
            input: InputId(5),
            steered: true
        }]
    );
    // It starts no turn of its own: the CLI's turn ends once, listing the message.
    p.on_line(&lifecycle(&u1, "completed"));
    let step = p.on_line(&format!(
        r#"{{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":2,"user_message_uuids":["{u1}"],"origin":{{"kind":"task-notification","producer":"session-task","runId":"r1"}}}}"#
    ));
    assert!(matches!(
        &step.events[..],
        [
            AgentEvent::TurnCompleted {
                origin: Some(_),
                ..
            },
            AgentEvent::InputSent { input: InputId(6) }
        ]
    ));
}

/// A prompt written while the CLI is in a turn of its own is held until that turn's `result`,
/// like one behind our own turn; and a prompt the CLI has queued (read, not started: it is
/// waiting behind a turn the CLI began first) holds the next ones back too, so no two of our
/// prompts are ever merged into one turn.
#[test]
fn prompts_wait_for_the_clis_turn_and_for_the_prompt_the_cli_has_queued() {
    let mut p = ready_with_ids();
    let u1 = uuid_of(&sent(p.encode(InputId(5), Command::prompt("one"))));
    p.on_line(&lifecycle(&u1, "queued"));
    p.on_line(&lifecycle(&u1, "started"));
    p.on_line(TURN_INIT);
    p.on_line(RESULT_OK);
    p.on_line(&lifecycle(&u1, "completed"));
    // The CLI begins a turn of its own: a prompt waits for it.
    p.on_line(TURN_INIT);
    assert_eq!(
        p.encode(InputId(6), Command::prompt("two")).unwrap(),
        Encoded::Queued
    );
    // Its turn ends: the prompt goes out.
    let step = p.on_line(RESULT_OK);
    let u2 = uuid_of(&step);
    assert_eq!(
        step.events.last(),
        Some(&AgentEvent::InputSent { input: InputId(6) })
    );
    // The CLI reads it but is busy with another turn of its own (`queued`, no `started`): the
    // next prompt keeps waiting although no turn of the CLI is open right now.
    p.on_line(&lifecycle(&u2, "queued"));
    assert_eq!(
        p.encode(InputId(7), Command::prompt("three")).unwrap(),
        Encoded::Queued
    );
    p.on_line(&lifecycle(&u2, "started"));
    p.on_line(TURN_INIT);
    let step = p.on_line(RESULT_OK);
    assert_eq!(
        step.events.last(),
        Some(&AgentEvent::InputSent { input: InputId(7) })
    );
}

/// Real frames of a background subagent (E4): the task is running from `task_started` until
/// `task_notification`, whatever happens to the tool call that spawned it.
#[test]
fn task_frames_become_task_events() {
    let (mut p, _) = ready();
    let ev = p.on_line(TASKS_LISTED).events;
    assert_eq!(
        ev,
        vec![AgentEvent::BackgroundTasksChanged {
            tasks: vec![wasit_core::BackgroundTask {
                task_id: "a1".into(),
                kind: "local_agent".into(),
                description: "probe".into(),
                subagent_type: Some("general-purpose".into()),
                run_id: Some("r1".into()),
            }]
        }]
    );
    let ev = p.on_line(TASK_STARTED).events;
    assert_eq!(
        ev,
        vec![AgentEvent::TaskStarted {
            task_id: "a1".into(),
            kind: "local_agent".into(),
            call_id: Some("toolu_A".into()),
            description: "probe".into(),
            subagent_type: Some("general-purpose".into()),
            background: true,
            run_id: Some("r1".into()),
        }]
    );
    let ev = p.on_line(r#"{"type":"system","subtype":"task_progress","task_id":"a1","run_id":"r1","tool_use_id":"toolu_A","description":"Running Echo","subagent_type":"general-purpose","usage":{"total_tokens":59844,"tool_uses":1,"duration_ms":1111},"last_tool_name":"Bash"}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::TaskProgress {
            task_id: "a1".into(),
            call_id: Some("toolu_A".into()),
            description: "Running Echo".into(),
            last_tool: Some("Bash".into()),
            usage: Some(wasit_core::TaskUsage {
                total_tokens: 59844,
                tool_uses: 1,
                duration_ms: 1111
            }),
        }]
    );
    let ev = p.on_line(r#"{"type":"system","subtype":"task_updated","task_id":"a1","run_id":"r1","patch":{"status":"completed","end_time":1795391266906}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::TaskUpdated {
            task_id: "a1".into(),
            status: Some("completed".into())
        }]
    );
    let ev = p.on_line(TASK_ENDED).events;
    assert_eq!(
        ev,
        vec![AgentEvent::TaskEnded {
            task_id: "a1".into(),
            call_id: Some("toolu_A".into()),
            status: "completed".into(),
            ok: true,
            summary: Some("hi".into()),
            usage: Some(wasit_core::TaskUsage {
                total_tokens: 60846,
                tool_uses: 1,
                duration_ms: 2533
            }),
        }]
    );
    // A stopped or failed task did not succeed.
    let ev = p
        .on_line(&TASK_ENDED.replace("completed", "stopped"))
        .events;
    assert!(
        matches!(&ev[0], AgentEvent::TaskEnded { ok: false, status, .. } if status == "stopped")
    );
    // A frame that names no task stays an unrecognised line.
    let ev = p
        .on_line(r#"{"type":"system","subtype":"task_started"}"#)
        .events;
    assert!(matches!(ev[0], AgentEvent::Unknown { .. }));
}

/// Every line the CLI tags with `parent_tool_use_id` is the subagent's own conversation.
#[test]
fn a_subagents_lines_are_scoped_to_it() {
    let (mut p, _) = ready();
    let scope = || wasit_core::Scope {
        call_id: "toolu_A".into(),
        agent_id: Some("a1".into()),
    };
    // Text, thinking and a tool call of the subagent, with its usage.
    let ev = p.on_line(r#"{"type":"assistant","parent_tool_use_id":"toolu_A","agent_id":"a1","subagent_type":"general-purpose","task_description":"probe","message":{"id":"m1","content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"hi"},{"type":"tool_use","id":"toolu_B","name":"Bash","input":{"command":"echo hi"}}],"usage":{"input_tokens":3,"output_tokens":2}}}"#).events;
    assert_eq!(ev.len(), 4);
    for e in &ev {
        match e {
            AgentEvent::Scoped { scope: s, .. } => assert_eq!(*s, scope()),
            other => panic!("not scoped: {other:?}"),
        }
    }
    assert!(matches!(
        &ev[0],
        AgentEvent::Scoped { event, .. } if matches!(&**event, AgentEvent::ThinkingDelta { text, .. } if text == "hmm")
    ));
    assert!(matches!(
        &ev[1],
        AgentEvent::Scoped { event, .. } if matches!(&**event, AgentEvent::AssistantDelta { text, .. } if text == "hi")
    ));
    assert!(matches!(
        &ev[2],
        AgentEvent::Scoped { event, .. } if matches!(&**event, AgentEvent::ToolCallStarted { name, .. } if name == "Bash")
    ));
    assert!(matches!(
        &ev[3],
        AgentEvent::Scoped { event, .. } if matches!(&**event, AgentEvent::UsageUpdate { context_pct: None, .. })
    ));
    // Its tool result, and a result a subagent's own text line is not mistaken for.
    let ev = p.on_line(r#"{"type":"user","parent_tool_use_id":"toolu_A","agent_id":"a1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_B","content":"hi"}]}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::Scoped { scope: s, event } if *s == scope() && matches!(&**event, AgentEvent::ToolCallFinished { call_id, name, ok: true, .. } if call_id == "toolu_B" && name == "Bash")
    ));
    // The session's own lines (`parent_tool_use_id` null) stay unscoped.
    let ev = p.on_line(r#"{"type":"assistant","parent_tool_use_id":null,"message":{"id":"m2","content":[{"type":"text","text":"mine"}]}}"#).events;
    assert!(matches!(&ev[0], AgentEvent::AssistantDelta { text, .. } if text == "mine"));
    // The agent's id is optional: the spawning call alone scopes a line.
    let ev = p.on_line(r#"{"type":"assistant","parent_tool_use_id":"toolu_A","message":{"id":"m3","content":[{"type":"text","text":"x"}]}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::Scoped { scope: wasit_core::Scope { call_id, agent_id: None }, .. } if call_id == "toolu_A"
    ));
}

/// What `interrupt` stops: the turn that runs (a turn the CLI started by itself too), else the
/// background tasks, which `interrupt` itself does not stop; `StopTask` stops one.
#[test]
fn interrupt_stops_the_turn_or_else_the_background_tasks_and_stop_task_one_task() {
    let mut p = ready_with_ids();
    p.on_line(TASKS_LISTED);
    p.on_line(TASK_STARTED);
    // No turn: the background task is what runs.
    let w = writes(&sent(p.encode(InputId(8), Command::Interrupt)));
    assert_eq!(w.len(), 1);
    assert_eq!(
        w[0]["request"],
        json!({"subtype":"stop_task","task_id":"a1"})
    );
    // A turn the CLI started by itself: interrupt stops the turn only.
    p.on_line(TURN_INIT);
    let w = writes(&sent(p.encode(InputId(9), Command::Interrupt)));
    assert_eq!(w.len(), 1);
    assert_eq!(w[0]["request"]["subtype"], "interrupt");
    let ev = p
        .on_line(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":1,"origin":{"kind":"task-notification","producer":"session-task","runId":"r1"}}"#)
        .events;
    assert!(matches!(
        ev[..],
        [AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            was_interrupted: true,
            ..
        }]
    ));
    // One task by id: the CLI answers success to any id, so an unknown one is refused here.
    let w = writes(&sent(p.encode(
        InputId(10),
        Command::StopTask {
            task_id: "a1".into(),
        },
    )));
    assert_eq!(
        w[0]["request"],
        json!({"subtype":"stop_task","task_id":"a1"})
    );
    assert!(matches!(
        p.encode(
            InputId(11),
            Command::StopTask {
                task_id: "nosuch".into()
            }
        ),
        Err(wasit_core::DriverError::NotFound { .. })
    ));
    // Once the agent reports it ended, it is no longer there to stop or interrupt.
    p.on_line(TASK_ENDED);
    p.on_line(TASKS_EMPTY);
    assert!(matches!(
        p.encode(
            InputId(12),
            Command::StopTask {
                task_id: "a1".into()
            }
        ),
        Err(wasit_core::DriverError::NotFound { .. })
    ));
    assert!(sent(p.encode(InputId(13), Command::Interrupt))
        .write
        .is_empty());
}

#[test]
fn a_stop_the_cli_refuses_is_reported() {
    let mut p = ready_with_ids();
    p.on_line(TASK_STARTED);
    let w = writes(&sent(p.encode(
        InputId(5),
        Command::StopTask {
            task_id: "a1".into(),
        },
    )));
    let id = w[0]["request_id"].as_str().unwrap().to_string();
    let ev = p.on_line(&format!(r#"{{"type":"control_response","response":{{"subtype":"error","request_id":"{id}","error":"stop_task: task_id must be a string"}}}}"#)).events;
    assert!(
        matches!(&ev[..], [AgentEvent::CommandRejected { command, .. }] if command == "stop_task")
    );
}
