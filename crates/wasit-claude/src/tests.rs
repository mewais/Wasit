use super::*;
use serde_json::json;
use wasit_core::{
    AgentEvent, Command, Encoded, EndReason, GateDecision, GateRequest, McpServerConfig,
    McpTransport, PluginRef, QuestionAnswer, SkillRef, Step,
};

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

#[test]
fn start_writes_initialize_and_queues_first_prompt() {
    let mut p = ClaudeProtocol::new();
    let step = p.start(&SpawnSpec {
        first_prompt: Some("hi".into()),
        ..SpawnSpec::default()
    });
    assert_eq!(
        step.write[0].0,
        r#"{"request":{"hooks":null,"subtype":"initialize"},"request_id":"req_1","type":"control_request"}"#
    );
    assert_eq!(step.write.len(), 1);
    // A prompt before the initialize response is queued.
    assert_eq!(
        p.encode(Command::Prompt { text: "x".into() }).unwrap(),
        Encoded::Queued
    );
    let step = p.on_line(r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#);
    // Only the first prompt goes out; the second waits for the turn to end.
    assert_eq!(step.write.len(), 1);
    assert!(step.write[0].0.contains(r#""text":"hi""#));
    assert!(matches!(step.events[0], AgentEvent::TurnStarted { .. }));
    let step = p.on_line(
        r#"{"type":"result","subtype":"success","is_error":false,"result":"ok","num_turns":1}"#,
    );
    assert!(matches!(step.events[0], AgentEvent::TurnCompleted { .. }));
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
        p.encode(Command::Prompt { text: "x".into() }).unwrap(),
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
            result: Some(json!("done"))
        }
    );
}

#[test]
fn permission_gate_approve_echoes_input_and_deny_has_message() {
    let (mut p, _) = ready();
    let ev = p.on_line(r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"touch a","description":"Create a"},"permission_suggestions":[{"type":"addRules"}]}}"#).events;
    match &ev[0] {
        AgentEvent::GateRequested {
            gate:
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    command,
                    explanation,
                    permission_suggestions,
                    ..
                },
        } => {
            assert_eq!(request_id, "g1");
            assert_eq!(tool_name, "Bash");
            assert_eq!(command, "touch a");
            assert_eq!(explanation, "Create a");
            assert_eq!(permission_suggestions.len(), 1);
        }
        e => panic!("{e:?}"),
    }
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "g1".into(),
        decision: GateDecision::approve(),
    }));
    assert_eq!(
        writes(&step)[0],
        json!({"type":"control_response","response":{"subtype":"success","request_id":"g1","response":{"behavior":"allow","updatedInput":{"command":"touch a","description":"Create a"}}}})
    );
    // Answered gates are gone.
    assert!(matches!(
        p.encode(Command::AnswerGate {
            request_id: "g1".into(),
            decision: GateDecision::approve()
        }),
        Err(wasit_core::DriverError::NotFound { .. })
    ));
    p.on_line(r#"{"type":"control_request","request_id":"g2","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"rm x"}}}"#);
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "g2".into(),
        decision: GateDecision::deny(Some("no")),
    }));
    assert_eq!(
        writes(&step)[0]["response"]["response"],
        json!({"behavior":"deny","message":"no"})
    );
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
        p.encode(Command::AnswerGate {
            request_id: "q1".into(),
            decision: GateDecision::approve()
        }),
        Err(wasit_core::DriverError::Invalid { .. })
    ));
    let mut answer = QuestionAnswer::default();
    answer.answers.insert("Color?".into(), "Red".into());
    let step = sent(p.encode(Command::AnswerQuestion {
        request_id: "q1".into(),
        answer,
    }));
    let w = writes(&step);
    assert_eq!(
        w[0]["response"]["response"]["updatedInput"]["answers"],
        json!({"Color?":"Red"})
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
                summary: "1. do it".into()
            }
        }
    );

    let ev = p.on_line(r#"{"type":"control_request","request_id":"e1","request":{"subtype":"elicitation","mcp_server_name":"hares","message":"Grant?","requested_schema":{"properties":{"grant":{"enum":["once","deny"]}}}}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::GateRequested { gate: GateRequest::Permission { is_elicitation: true, tool_name, elicitation_choices, .. } } if tool_name == "hares" && elicitation_choices.len() == 2
    ));
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "e1".into(),
        decision: GateDecision::Approve {
            updated_input: None,
            permission_updates: None,
            elicitation_content: Some(json!({"grant":"once"})),
        },
    }));
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
    sent(p.encode(Command::Prompt { text: "go".into() }));
    p.on_line(r#"{"type":"control_request","request_id":"g1","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"x"}}}"#);
    let step = sent(p.encode(Command::Interrupt));
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
            request_id: "g1".into()
        }]
    );
    // A second interrupt during the same turn writes nothing.
    assert!(sent(p.encode(Command::Interrupt)).write.is_empty());
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
    assert!(sent(p.encode(Command::Interrupt)).is_empty());
}

#[test]
fn result_variants_and_cumulative_usage() {
    let (mut p, _) = ready();
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
    let ev = p.on_line(r#"{"type":"result","subtype":"error_during_execution","is_error":true,"errors":["boom"]}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::TurnCompleted { end_reason: Some(EndReason::ExecutionError), reply: Some(r), .. } if r == "boom")
    );
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
    let step = sent(p.encode(Command::SetModel {
        model: "sonnet".into(),
    }));
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
    sent(p.encode(Command::SetMode {
        mode: "bogus".into(),
    }));
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
    let step = sent(p.encode(Command::RunSlash {
        name: "/context".into(),
        args: String::new(),
    }));
    assert_eq!(
        writes(&step)[0]["message"]["content"][0]["text"],
        "/context"
    );
    let step = p.on_line(r#"{"type":"result","subtype":"success","is_error":false}"#);
    assert!(step.write.is_empty());
    let step = sent(p.encode(Command::RunSlash {
        name: "review".into(),
        args: " 12 ".into(),
    }));
    assert_eq!(
        writes(&step)[0]["message"]["content"][0]["text"],
        "/review 12"
    );
}

#[test]
fn shutdown_closes_stdin_and_rejects_more() {
    let (mut p, _) = ready();
    assert!(sent(p.encode(Command::Shutdown)).close_stdin);
    assert_eq!(
        p.encode(Command::Prompt { text: "x".into() }),
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
        stdout: "  --permission-mode <mode>  Permission mode\n   (choices: \"acceptEdits\", \"auto\",\n   \"plan\")\n".into(),
    };
    match parse_permission_modes(&help).unwrap() {
        Discovered::Modes(m) => {
            assert_eq!(
                m.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
                ["acceptEdits", "auto", "plan"]
            )
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
            },
            wasit_core::FileHead {
                path: "/h/.claude/projects/-p/agent-x.jsonl".into(),
                modified_ms: 1,
                head: vec![],
            },
        ],
    };
    match parse_sessions(&files).unwrap() {
        Discovered::Sessions(s) => {
            assert_eq!(s.len(), 1);
            assert_eq!(s[0].project.as_deref(), Some("/p"));
            assert_eq!(s[0].title, "fix it");
        }
        d => panic!("{d:?}"),
    }
    let ctx = DiscoveryContext {
        home: Some("/h".into()),
        project: Some("/a/b.c".into()),
        ..DiscoveryContext::default()
    };
    let probes = ClaudeDescriptor.discovery(&ctx);
    assert_eq!(probes.len(), 4);
    assert!(
        matches!(&probes[3].action, ProbeAction::ListFiles { dir, recursive: false, .. } if dir == "/h/.claude/projects/-a-b-c")
    );
}

#[test]
fn determinism_same_inputs_same_outputs() {
    let run = || {
        let mut p = ClaudeProtocol::new();
        let mut out = vec![format!(
            "{:?}",
            p.start(&SpawnSpec {
                first_prompt: Some("a".into()),
                ..SpawnSpec::default()
            })
        )];
        for l in [
            r#"{"type":"control_response","response":{"subtype":"success","request_id":"req_1","response":{}}}"#,
            r#"{"type":"control_request","request_id":"g","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"b":1,"a":2}}}"#,
        ] {
            out.push(format!("{:?}", p.on_line(l)));
        }
        out.push(format!(
            "{:?}",
            p.encode(Command::SetMode {
                mode: "plan".into()
            })
        ));
        out.push(format!("{:?}", p.encode(Command::Interrupt)));
        out
    };
    assert_eq!(run(), run());
}
