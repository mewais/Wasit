use super::*;
use serde_json::json;
use wasit_core::{
    AgentEvent, Command, DriverError, Encoded, EndReason, GateDecision, GateRequest,
    InputDropReason, InputId, QuestionAnswer, ResumeMode, SkillRef, Step,
};

fn w(step: &Step) -> Vec<Value> {
    step.write
        .iter()
        .map(|l| serde_json::from_str(&l.0).unwrap())
        .collect()
}

fn sent(r: Result<Encoded, DriverError>) -> Step {
    match r.expect("encode ok") {
        Encoded::Sent(s) => s,
        Encoded::Queued => panic!("queued"),
    }
}

/// A protocol past the handshake on thread `t1`, model `m`.
fn ready() -> CodexProtocol {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec::default());
    p.on_line(r#"{"id":1,"result":{"userAgent":"x"}}"#);
    p.on_line(r#"{"id":3,"result":{"thread":{"id":"t1"},"model":"m","cwd":"/c"}}"#);
    p
}

#[test]
fn handshake_order_and_session_init() {
    let mut p = CodexProtocol::new();
    let (spec, first) = SpawnSpec {
        cwd: Some("/c".into()),
        model: Some("gpt".into()),
        first_prompt: Some("hi".into()),
        ..SpawnSpec::default()
    }
    .split_first_prompt();
    let step = p.start(&spec);
    // The first prompt is the first input, encoded right after start.
    assert_eq!(
        p.encode(InputId(1), first.unwrap()).unwrap(),
        Encoded::Queued
    );
    assert_eq!(w(&step)[0]["method"], "initialize");
    assert_eq!(
        w(&step)[0]["params"]["capabilities"]["experimentalApi"],
        true
    );
    // Commands wait for the handshake.
    assert_eq!(
        p.encode(InputId(101), Command::SetModel { model: "x".into() })
            .unwrap(),
        Encoded::Queued
    );
    let step = p.on_line(r#"{"id":1,"result":{}}"#);
    let lines = w(&step);
    assert_eq!(lines[0], json!({"jsonrpc":"2.0","method":"initialized"}));
    assert_eq!(lines[1]["method"], "model/list");
    assert_eq!(lines[2]["method"], "thread/start");
    assert_eq!(
        lines[2]["params"],
        json!({"cwd":"/c","model":"gpt","approvalPolicy":"on-request","sandbox":"workspace-write"})
    );
    let ev = p.on_line(r#"{"id":2,"result":{"data":[{"id":"a","displayName":"A","description":"d","isDefault":true,"hidden":false},{"id":"h","hidden":true}]}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::ModelsUpdated { models, .. } if models.len() == 1 && models[0].is_default)
    );
    let step = p.on_line(r#"{"id":3,"result":{"thread":{"id":"t1"},"model":"gpt","cwd":"/c"}}"#);
    assert!(
        matches!(&step.events[0], AgentEvent::SessionInit { native_session_id: Some(t), model: Some(m), .. } if t == "t1" && m == "gpt")
    );
    let lines = w(&step);
    assert_eq!(lines[0]["method"], "collaborationMode/list");
    // Queued commands in order: the first prompt, then set_model.
    assert_eq!(lines[1]["method"], "turn/start");
    assert_eq!(lines[1]["params"]["input"][0]["text"], "hi");
    assert_eq!(lines[2]["method"], "thread/settings/update");
    assert_eq!(lines[2]["params"], json!({"threadId":"t1","model":"x"}));
}

#[test]
fn continue_lists_latest_thread_then_resumes() {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec {
        cwd: Some("/c".into()),
        resume: ResumeMode::Continue,
        ..SpawnSpec::default()
    });
    let lines = w(&p.on_line(r#"{"id":1,"result":{}}"#));
    assert_eq!(
        lines[2],
        json!({"jsonrpc":"2.0","id":3,"method":"thread/list","params":{"cwd":"/c","limit":1}})
    );
    let lines = w(&p.on_line(r#"{"id":3,"result":{"data":[{"id":"old"}]}}"#));
    assert_eq!(lines[0]["method"], "thread/resume");
    assert_eq!(lines[0]["params"]["threadId"], "old");
    // No previous thread: start fresh.
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec {
        resume: ResumeMode::Continue,
        ..SpawnSpec::default()
    });
    p.on_line(r#"{"id":1,"result":{}}"#);
    let lines = w(&p.on_line(r#"{"id":3,"result":{"data":[]}}"#));
    assert_eq!(lines[0]["method"], "thread/start");
}

const PROMPT_ID: InputId = InputId(102);

#[test]
fn turn_lifecycle_usage_and_reply() {
    let mut p = ready();
    let s = sent(p.encode(InputId(102), Command::prompt("go")));
    let id = w(&s)[0]["id"].as_u64().unwrap();
    p.on_line(&format!(
        r#"{{"id":{id},"result":{{"turn":{{"id":"u1","status":"inProgress","items":[]}}}}}}"#
    ));
    let ev = p
        .on_line(r#"{"method":"turn/started","params":{"threadId":"t1","turn":{"id":"u1"}}}"#)
        .events;
    assert_eq!(
        ev,
        vec![AgentEvent::turn_started(Some("u1".into()), Some(PROMPT_ID))]
    );
    let ev = p
        .on_line(
            r#"{"method":"item/agentMessage/delta","params":{"itemId":"msg_1","delta":"Hel"}}"#,
        )
        .events;
    assert_eq!(
        ev,
        vec![AgentEvent::AssistantDelta {
            text: "Hel".into(),
            message_id: Some("msg_1".into())
        }]
    );
    let ev = p.on_line(r#"{"method":"thread/tokenUsage/updated","params":{"tokenUsage":{"total":{"totalTokens":107,"inputTokens":102,"cachedInputTokens":10,"cacheWriteInputTokens":0,"outputTokens":5},"last":{"totalTokens":100},"modelContextWindow":1000}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::UsageUpdate { usage, context_pct: Some(pct) } if usage.input_tokens == 102 && usage.cache_read_tokens == 10 && (*pct - 10.0).abs() < 1e-3)
    );
    let ev = p.on_line(r#"{"method":"turn/completed","params":{"turn":{"id":"u1","status":"completed","items":[{"type":"agentMessage","text":"Hello"}]}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::TurnCompleted { end_reason: None, usage, reply: Some(r), .. } if usage.output_tokens == 5 && r == "Hello")
    );
}

#[test]
fn prompt_during_turn_is_steered_with_turn_id() {
    let mut p = ready();
    sent(p.encode(InputId(103), Command::prompt("a")));
    // Turn id unknown yet: queued.
    assert_eq!(
        p.encode(InputId(104), Command::prompt("b")).unwrap(),
        Encoded::Queued
    );
    let step = p.on_line(r#"{"method":"turn/started","params":{"turn":{"id":"u9"}}}"#);
    let lines = w(&step);
    assert_eq!(lines[0]["method"], "turn/steer");
    assert_eq!(lines[0]["params"]["expectedTurnId"], "u9");
    assert_eq!(lines[0]["params"]["input"][0]["text"], "b");
}

#[test]
fn interrupt_uses_tracked_turn_and_declines_gates() {
    let mut p = ready();
    sent(p.encode(InputId(105), Command::prompt("a")));
    // Interrupt before the turn id is known waits for it.
    assert!(sent(p.encode(InputId(106), Command::Interrupt))
        .write
        .is_empty());
    let step = p.on_line(r#"{"method":"turn/started","params":{"turn":{"id":"u2"}}}"#);
    let lines = w(&step);
    assert_eq!(lines[0]["method"], "turn/interrupt");
    assert_eq!(lines[0]["params"], json!({"threadId":"t1","turnId":"u2"}));
    let ev = p.on_line(r#"{"method":"turn/completed","params":{"turn":{"id":"u2","status":"interrupted","items":[]}}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            was_interrupted: true,
            ..
        }
    ));

    // With a pending gate: decline first, then interrupt.
    sent(p.encode(InputId(107), Command::prompt("b")));
    p.on_line(r#"{"method":"turn/started","params":{"turn":{"id":"u3"}}}"#);
    p.on_line(r#"{"method":"item/commandExecution/requestApproval","id":0,"params":{"itemId":"call_1","command":"touch x","cwd":"/c"}}"#);
    let step = sent(p.encode(InputId(108), Command::Interrupt));
    let lines = w(&step);
    assert_eq!(
        lines[0],
        json!({"jsonrpc":"2.0","id":0,"result":{"decision":"decline"}})
    );
    assert_eq!(lines[1]["params"]["turnId"], "u3");
    assert_eq!(
        step.events,
        vec![AgentEvent::GateResolved {
            request_id: "call_1".into(),
            reason: None,
        }]
    );
}

#[test]
fn gates_route_by_item_id_and_encode_per_method() {
    let mut p = ready();
    let ev = p.on_line(r#"{"method":"item/commandExecution/requestApproval","id":0,"params":{"itemId":"call_3","reason":"needs it","command":"/bin/bash -lc 'touch a'","cwd":"/c"}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::GateRequested { gate: GateRequest::Permission { request_id, tool_name, command, explanation, .. } } if request_id == "call_3" && tool_name == "shell" && command.contains("touch a") && explanation == "needs it")
    );
    match &ev[0] {
        AgentEvent::GateRequested { gate } => {
            let ids: Vec<_> = gate.choices().iter().map(|c| c.id.as_str()).collect();
            assert_eq!(ids, ["accept", "acceptForSession", "decline", "cancel"]);
        }
        e => panic!("{e:?}"),
    }
    let step = sent(p.encode(
        InputId(109),
        Command::AnswerGate {
            request_id: "call_3".into(),
            decision: GateDecision::choose("accept"),
        },
    ));
    assert_eq!(
        w(&step)[0],
        json!({"jsonrpc":"2.0","id":0,"result":{"decision":"accept"}})
    );
    // Our own answer's acknowledgement is silent.
    assert!(p
        .on_line(r#"{"method":"serverRequest/resolved","params":{"requestId":0}}"#)
        .events
        .is_empty());

    // availableDecisions is the list the client may present, in codex's order.
    let ev = p.on_line(r#"{"method":"item/commandExecution/requestApproval","id":1,"params":{"itemId":"call_4","command":"curl x","availableDecisions":["accept",{"acceptWithExecpolicyAmendment":{"execpolicy_amendment":["curl"]}},{"applyNetworkPolicyAmendment":{"network_policy_amendment":{"host":"x.org","action":"allow"}}},"cancel"]}}"#).events;
    let AgentEvent::GateRequested { gate } = &ev[0] else {
        panic!("{ev:?}")
    };
    let ids: Vec<_> = gate.choices().iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids,
        [
            "accept",
            "acceptWithExecpolicyAmendment",
            "network:2",
            "cancel"
        ]
    );
    assert_eq!(
        gate.choices()[1].description,
        "commands starting with `curl`"
    );
    let step = sent(p.encode(
        InputId(110),
        Command::AnswerGate {
            request_id: "call_4".into(),
            decision: GateDecision::choose("acceptWithExecpolicyAmendment"),
        },
    ));
    assert_eq!(
        w(&step)[0]["result"],
        json!({"decision":{"acceptWithExecpolicyAmendment":{"execpolicy_amendment":["curl"]}}})
    );

    p.on_line(r#"{"method":"execCommandApproval","id":5,"params":{"command":["ls"]}}"#);
    let step = sent(p.encode(
        InputId(111),
        Command::AnswerGate {
            request_id: "codex:5".into(),
            decision: GateDecision::choose("approved_for_session"),
        },
    ));
    assert_eq!(
        w(&step)[0]["result"],
        json!({"decision":"approved_for_session"})
    );

    p.on_line(r#"{"method":"item/permissions/requestApproval","id":6,"params":{"itemId":"p1","permissions":{"network":{"enabled":true}}}}"#);
    let step = sent(p.encode(
        InputId(112),
        Command::AnswerGate {
            request_id: "p1".into(),
            decision: GateDecision::choose("accept"),
        },
    ));
    assert_eq!(
        w(&step)[0]["result"],
        json!({"permissions":{"network":{"enabled":true}},"scope":"turn"})
    );

    p.on_line(r#"{"method":"mcpServer/elicitation/request","id":7,"params":{"serverName":"hares","message":"ok?","requestedSchema":{"properties":{"grant":{"enum":["once"]}}}}}"#);
    let step = sent(p.encode(
        InputId(113),
        Command::AnswerGate {
            request_id: "codex:7".into(),
            decision: GateDecision::choose("decline"),
        },
    ));
    assert_eq!(w(&step)[0]["result"], json!({"action":"decline"}));

    // A gate resolved by codex itself.
    p.on_line(r#"{"method":"item/fileChange/requestApproval","id":8,"params":{"itemId":"f1","changes":{}}}"#);
    let ev = p
        .on_line(r#"{"method":"serverRequest/resolved","params":{"requestId":8}}"#)
        .events;
    assert_eq!(
        ev,
        vec![AgentEvent::GateResolved {
            request_id: "f1".into(),
            reason: None,
        }]
    );
}

#[test]
fn user_input_question_maps_text_to_ids() {
    let mut p = ready();
    let ev = p.on_line(r#"{"method":"item/tool/requestUserInput","id":9,"params":{"itemId":"q1","questions":[{"id":"color","header":"C","question":"Color?","options":[{"label":"Red","description":""}]}]}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::GateRequested { gate: GateRequest::Question { questions, .. } } if questions[0].options[0].label == "Red")
    );
    assert!(matches!(
        p.encode(
            InputId(114),
            Command::AnswerGate {
                request_id: "q1".into(),
                decision: GateDecision::choose("accept")
            }
        ),
        Err(DriverError::Invalid { .. })
    ));
    let mut a = QuestionAnswer::default();
    a.answers
        .insert("color".into(), vec!["Red".into(), "Blue".into()]);
    let step = sent(p.encode(
        InputId(115),
        Command::AnswerQuestion {
            request_id: "q1".into(),
            answer: a,
        },
    ));
    assert_eq!(
        w(&step)[0]["result"],
        json!({"answers":{"color":{"answers":["Red","Blue"]}}})
    );
}

#[test]
fn modes_model_and_settings_updates() {
    let mut p = ready();
    let ev = p.on_line(r#"{"id":4,"result":{"data":[{"name":"Plan","mode":"plan"},{"name":"Default","mode":"default"}]}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::ModesUpdated { available_modes, current_mode: None } if available_modes.len() == 2 && available_modes[0].label == "Plan")
    );
    let step = sent(p.encode(
        InputId(116),
        Command::SetMode {
            mode: "plan".into(),
        },
    ));
    assert_eq!(
        w(&step)[0]["params"],
        json!({"threadId":"t1","collaborationMode":{"mode":"plan","settings":{"model":"m","developer_instructions":null}}})
    );
    let ev = p.on_line(r#"{"method":"thread/settings/updated","params":{"threadSettings":{"model":"m","collaborationMode":{"mode":"plan"}}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::ModesUpdated {
            available_modes: vec![],
            current_mode: Some("plan".into())
        }]
    );
    let ev = p.on_line(r#"{"method":"thread/settings/updated","params":{"threadSettings":{"model":"m2","collaborationMode":{"mode":"plan"}}}}"#).events;
    assert_eq!(ev, vec![AgentEvent::ModelChanged { model: "m2".into() }]);
    let s = sent(p.encode(
        InputId(117),
        Command::SetModel {
            model: "bad".into(),
        },
    ));
    let id = w(&s)[0]["id"].as_u64().unwrap();
    let ev = p
        .on_line(&format!(
            r#"{{"id":{id},"error":{{"code":-32600,"message":"unknown model"}}}}"#
        ))
        .events;
    assert_eq!(
        ev,
        vec![AgentEvent::CommandRejected {
            command: "set_model".into(),
            message: "unknown model".into()
        }]
    );
}

#[test]
fn tool_items_and_unknown_server_requests() {
    let mut p = ready();
    let ev = p.on_line(r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"c1","command":"ls"}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::ToolCallStarted { name, call_id, .. } if name == "ls" && call_id == "c1")
    );
    let ev = p.on_line(r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"c1","status":"failed","exitCode":2,"aggregatedOutput":"no"}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::ToolCallFinished {
            call_id: "c1".into(),
            name: "ls".into(),
            ok: false,
            result: Some(json!("no")),
            parts: vec![]
        }]
    );
    let ev = p.on_line(r#"{"method":"item/started","params":{"item":{"type":"mcpToolCall","id":"m1","server":"fs","tool":"read","arguments":{"p":1}}}}"#).events;
    assert!(matches!(&ev[0], AgentEvent::ToolCallStarted { name, .. } if name == "fs.read"));
    let step = p.on_line(r#"{"method":"currentTime/read","id":44,"params":{}}"#);
    assert_eq!(w(&step)[0]["error"]["code"], -32601);
    assert!(p
        .encode(
            InputId(99),
            Command::RunSlash {
                name: "x".into(),
                args: String::new()
            }
        )
        .is_err());
}

#[test]
fn render_config_overrides_and_skill_roots() {
    let d = CodexDescriptor;
    let mut spec = SpawnSpec::default();
    let cfg = AgentConfig {
        mcp_servers: vec![
            McpServerConfig {
                name: "wasl".into(),
                transport: McpTransport::Stdio {
                    command: "wasl".into(),
                    args: vec!["mcp".into(), "--x".into()],
                    env: [("TOKEN".to_string(), "a\"b".to_string())]
                        .into_iter()
                        .collect(),
                },
                trusted: true,
            },
            McpServerConfig {
                name: "bad.name".into(),
                transport: McpTransport::Http {
                    url: "http://h".into(),
                    headers: Default::default(),
                },
                trusted: false,
            },
        ],
        skills: vec![SkillRef {
            name: "s".into(),
            root: "/sk/".into(),
        }],
        extra_instructions: Some("Be \"brief\".".into()),
        ..AgentConfig::default()
    };
    let unsupported = d.render_config(&cfg, &mut spec);
    assert_eq!(unsupported.len(), 1);
    assert_eq!(unsupported[0].item, "mcp:bad.name");
    assert_eq!(
        spec.extra_args,
        [
            "-c",
            r#"mcp_servers.wasl.command="wasl""#,
            "-c",
            r#"mcp_servers.wasl.args=["mcp", "--x"]"#,
            "-c",
            r#"mcp_servers.wasl.env.TOKEN="a\"b""#,
            "-c",
            r#"mcp_servers.wasl.default_tools_approval_mode="approve""#,
            "-c",
            r#"developer_instructions="Be \"brief\".""#,
        ]
    );
    assert_eq!(spec.protocol_options[SKILL_ROOTS_OPTION], json!(["/sk"]));
    assert_eq!(d.argv(&spec)[..3], ["codex", "app-server", "--stdio"]);
    // The protocol registers the roots before opening the thread.
    let mut p = CodexProtocol::new();
    p.start(&spec);
    let lines = w(&p.on_line(r#"{"id":1,"result":{}}"#));
    assert_eq!(
        lines[2],
        json!({"jsonrpc":"2.0","id":3,"method":"skills/extraRoots/set","params":{"extraRoots":["/sk"]}})
    );
    assert_eq!(lines[3]["method"], "thread/start");
}

#[test]
fn sessions_parser() {
    let out = ProbeOutput::Files {
        files: vec![wasit_core::FileHead {
            path: "/h/.codex/sessions/2026/11/17/rollout-2026-11-17T23-19-35-01a1ebcf-0528-7cd2-a32d-e89e91b6a844.jsonl".into(),
            modified_ms: 9,
            head: vec![
                r#"{"type":"session_meta","payload":{"id":"01a1ebcf-0528-7cd2-a32d-e89e91b6a844","cwd":"/p"}}"#.into(),
                r#"{"type":"response_item","payload":{"role":"user","content":[{"type":"input_text","text":"<environment_context>"}]}}"#.into(),
                r#"{"type":"response_item","payload":{"role":"user","content":[{"type":"input_text","text":"hello"}]}}"#.into(),
            ],
            matching_lines: Some(3),
        }],
    };
    match parse_sessions(&out).unwrap() {
        Discovered::Sessions(s) => {
            assert_eq!(s[0].id, "01a1ebcf-0528-7cd2-a32d-e89e91b6a844");
            assert_eq!(s[0].project.as_deref(), Some("/p"));
            assert_eq!(s[0].title, "hello");
            assert_eq!(s[0].message_count, Some(3));
        }
        d => panic!("{d:?}"),
    }
}

#[test]
fn windows_paths_list_the_same_sessions() {
    let head: Vec<String> = vec![
        r#"{"type":"session_meta","payload":{"id":"01a1ebcf-0528-7cd2-a32d-e89e91b6a844","cwd":"C:\\Users\\me\\p"}}"#.into(),
        r#"{"type":"response_item","payload":{"role":"user","content":[{"type":"input_text","text":"hello"}]}}"#.into(),
    ];
    let name = "rollout-2026-11-17T23-19-35-01a1ebcf-0528-7cd2-a32d-e89e91b6a844.jsonl";
    let sessions = |path: String| {
        let out = ProbeOutput::Files {
            files: vec![wasit_core::FileHead {
                path,
                modified_ms: 9,
                head: head.clone(),
                matching_lines: Some(3),
            }],
        };
        match parse_sessions(&out).unwrap() {
            Discovered::Sessions(s) => s,
            d => panic!("{d:?}"),
        }
    };
    let posix = sessions(format!("/h/.codex/sessions/2026/11/17/{name}"));
    let windows = sessions(format!(r"C:\Users\me\.codex\sessions\2026\11\17\{name}"));
    assert_eq!(posix.len(), 1);
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].id, "01a1ebcf-0528-7cd2-a32d-e89e91b6a844");
    assert_eq!(windows[0].id, posix[0].id);
    assert_eq!(windows[0].project, posix[0].project);
    assert_eq!(windows[0].title, "hello");
    assert_eq!(windows[0].message_count, Some(3));
}

#[test]
fn determinism_same_inputs_same_outputs() {
    let run = || {
        let mut p = CodexProtocol::new();
        let mut out = vec![format!("{:?}", p.start(&SpawnSpec::default()))];
        out.push(format!("{:?}", p.encode(InputId(1), Command::prompt("a"))));
        for l in [
            r#"{"id":1,"result":{}}"#,
            r#"{"id":3,"result":{"thread":{"id":"t"},"model":"m"}}"#,
            r#"{"method":"turn/started","params":{"turn":{"id":"u"}}}"#,
            r#"{"method":"item/commandExecution/requestApproval","id":0,"params":{"itemId":"c"}}"#,
        ] {
            out.push(format!("{:?}", p.on_line(l)));
        }
        out.push(format!("{:?}", p.encode(InputId(118), Command::Interrupt)));
        out
    };
    assert_eq!(run(), run());
}

#[test]
fn ready_only_after_the_thread_is_open() {
    let mut p = CodexProtocol::new();
    assert!(!p.is_ready());
    p.start(&SpawnSpec::default());
    p.on_line(r#"{"id":1,"result":{"userAgent":"x"}}"#);
    assert!(!p.is_ready());
    assert!(ready().is_ready());
}

// ── config discovery + disabling (shapes captured from codex-cli 0.169.1) ──

#[test]
fn mcp_list_json_parses() {
    let text = r#"WARNING: proceeding, even though we could not create PATH aliases
[{"name":"chttp","enabled":true,"disabled_reason":null,"transport":{"type":"streamable_http","url":"https://example.invalid/mcp","bearer_token_env_var":null,"http_headers":null},"auth_status":"unknown"},
 {"name":"cmcp","enabled":false,"disabled_reason":null,"transport":{"type":"stdio","command":"echo","args":["hi"],"env":null,"env_vars":[],"cwd":null},"auth_status":"unsupported"}]"#;
    let out = wasit_core::ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: text.into(),
    };
    let wasit_core::Discovered::Config(items) = crate::parse_mcp_list(&out).unwrap() else {
        panic!()
    };
    assert_eq!(items.len(), 2);
    assert!(
        matches!(&items[0].mcp, Some(McpTransport::Http { url, .. }) if url == "https://example.invalid/mcp")
    );
    assert!(!items[1].enabled);
    assert!(
        matches!(&items[1].mcp, Some(McpTransport::Stdio { command, args, .. }) if command == "echo" && args == &["hi".to_string()])
    );
}

#[test]
fn disabled_items_render_as_config_overrides() {
    use wasit_core::{ConfigItemKind as K, DisabledItem};
    let cfg = AgentConfig {
        disabled: vec![
            DisabledItem {
                kind: K::McpServer,
                name: "cmcp".into(),
                path: None,
            },
            DisabledItem {
                kind: K::Skill,
                name: "cs".into(),
                path: Some("/h/.codex/skills/cs/SKILL.md".into()),
            },
            DisabledItem {
                kind: K::Skill,
                name: "nopath".into(),
                path: None,
            },
            DisabledItem {
                kind: K::Plugin,
                name: "p".into(),
                path: None,
            },
        ],
        ..AgentConfig::default()
    };
    let mut spec = SpawnSpec::default();
    let unsupported = descriptor().render_config(&cfg, &mut spec);
    let items: Vec<&str> = unsupported.iter().map(|u| u.item.as_str()).collect();
    assert_eq!(items, ["skill:nopath", "plugin:p"]);
    assert_eq!(
        spec.extra_args,
        [
            "-c",
            "mcp_servers.cmcp.enabled=false",
            "-c",
            "skills.config=[{path=\"/h/.codex/skills/cs/SKILL.md\",enabled=false}]"
        ]
    );
}

#[test]
fn images_go_as_local_images_and_files_by_path() {
    let mut p = ready();
    let att = |name: &str, mime: &str| wasit_core::Attachment {
        name: name.into(),
        mime: mime.into(),
        size: 5,
        path: format!("/up/{name}"),
        data: None,
    };
    let s = sent(p.encode(
        InputId(119),
        Command::Prompt {
            text: "see".into(),
            attachments: vec![att("a.png", "image/png"), att("r.pdf", "application/pdf")],
        },
    ));
    let input = &w(&s)[0]["params"]["input"];
    assert_eq!(input[0]["type"], "text");
    assert!(input[0]["text"]
        .as_str()
        .unwrap()
        .contains("[Attached file: r.pdf"));
    assert_eq!(input[1], json!({"type":"localImage","path":"/up/a.png"}));
}

#[test]
fn image_view_items_carry_the_image_path() {
    let mut p = ready();
    p.on_line(r#"{"method":"item/started","params":{"item":{"type":"imageView","id":"v1","path":"/w/p.png"}}}"#);
    let ev = p
        .on_line(r#"{"method":"item/completed","params":{"item":{"type":"imageView","id":"v1","path":"/w/p.png"}}}"#)
        .events;
    let AgentEvent::ToolCallFinished { parts, .. } = &ev[0] else {
        panic!("{ev:?}")
    };
    assert!(
        matches!(&parts[0], wasit_core::ContentPart::Image { mime, source: wasit_core::MediaSource::Path { path }, .. } if mime == "image/png" && path == "/w/p.png")
    );
}

// ── queued prompts leave the queue visibly ──────────────────────────────────

fn drops(events: &[AgentEvent]) -> Vec<(InputId, InputDropReason)> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputDropped { input, reason, .. } => Some((*input, *reason)),
            _ => None,
        })
        .collect()
}

#[test]
fn interrupt_drops_prompts_waiting_for_the_turn_id() {
    let mut p = ready();
    // `turn/start` written, its turn id not known yet: later prompts wait.
    let start = sent(p.encode(InputId(10), Command::prompt("go")));
    let start_id = w(&start)[0]["id"].as_u64().unwrap();
    assert_eq!(
        p.encode(InputId(11), Command::prompt("a")).unwrap(),
        Encoded::Queued
    );
    assert_eq!(
        p.encode(InputId(12), Command::prompt("b")).unwrap(),
        Encoded::Queued
    );
    let step = sent(p.encode(InputId(13), Command::Interrupt));
    assert_eq!(
        drops(&step.events),
        vec![
            (InputId(11), InputDropReason::Interrupted),
            (InputId(12), InputDropReason::Interrupted)
        ]
    );
    // The turn id arrives: the deferred interrupt goes out, nothing else.
    let step = p.on_line(&format!(
        r#"{{"id":{start_id},"result":{{"turn":{{"id":"u1"}}}}}}"#
    ));
    let lines = w(&step);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["method"], "turn/interrupt");
}

#[test]
fn interrupt_before_ready_and_shutdown_drop_what_is_queued() {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec::default());
    p.encode(InputId(2), Command::prompt("a")).unwrap();
    p.encode(InputId(3), Command::SetModel { model: "x".into() })
        .unwrap();
    let step = sent(p.encode(InputId(4), Command::Interrupt));
    assert!(step.write.is_empty());
    assert_eq!(
        drops(&step.events),
        vec![(InputId(2), InputDropReason::Interrupted)]
    );
    p.encode(InputId(5), Command::prompt("b")).unwrap();
    let step = sent(p.encode(InputId(6), Command::Shutdown));
    assert!(step.close_stdin);
    assert_eq!(
        drops(&step.events),
        vec![
            (InputId(3), InputDropReason::Shutdown),
            (InputId(5), InputDropReason::Shutdown)
        ]
    );
}

#[test]
fn drained_commands_report_input_sent() {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec::default());
    p.encode(InputId(2), Command::prompt("a")).unwrap();
    p.on_line(r#"{"id":1,"result":{}}"#);
    let step = p.on_line(r#"{"id":3,"result":{"thread":{"id":"t1"},"model":"m"}}"#);
    assert!(step
        .events
        .contains(&AgentEvent::InputSent { input: InputId(2) }));
    // The turn it started names it.
    let ev = p
        .on_line(r#"{"method":"turn/started","params":{"threadId":"t1","turn":{"id":"u1"}}}"#)
        .events;
    assert_eq!(
        ev[0],
        AgentEvent::turn_started(Some("u1".into()), Some(InputId(2)))
    );
}

// ── acknowledgement and refusal of prompts ──────────────────────────────────

fn acks(events: &[AgentEvent]) -> Vec<(InputId, bool)> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputAcknowledged { input, steered } => Some((*input, *steered)),
            _ => None,
        })
        .collect()
}

fn refusals(events: &[AgentEvent]) -> Vec<(InputId, String)> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::InputDropped {
                input,
                reason: InputDropReason::Refused,
                error,
            } => Some((
                *input,
                error.clone().expect("a refusal carries the CLI's error"),
            )),
            _ => None,
        })
        .collect()
}

fn id_of(step: &Step, method: &str) -> u64 {
    w(step)
        .iter()
        .find(|l| l["method"] == method)
        .unwrap_or_else(|| panic!("no {method} written"))["id"]
        .as_u64()
        .unwrap()
}

fn result_line(id: u64, result: &str) -> String {
    format!(r#"{{"id":{id},"result":{result}}}"#)
}

const TURN_STARTED_U1: &str =
    r#"{"method":"turn/started","params":{"threadId":"t1","turn":{"id":"u1"}}}"#;

#[test]
fn turn_start_is_acknowledged_by_its_result_and_not_again_by_the_notification() {
    // The order both recorded CLIs (0.169.1, 0.161.0) use: result, then `turn/started`.
    let mut p = ready();
    let start = sent(p.encode(InputId(20), Command::prompt("go")));
    let id = id_of(&start, "turn/start");
    // Nothing is acknowledged by writing the line.
    assert!(acks(&start.events).is_empty());
    let step = p.on_line(&result_line(
        id,
        r#"{"turn":{"id":"u1","status":"inProgress"}}"#,
    ));
    assert_eq!(acks(&step.events), vec![(InputId(20), false)]);
    let step = p.on_line(TURN_STARTED_U1);
    assert_eq!(
        step.events,
        vec![AgentEvent::turn_started(
            Some("u1".into()),
            Some(InputId(20))
        )]
    );
    // The turn runs and ends: no further acknowledgement for the input.
    let step = p.on_line(
        r#"{"method":"turn/completed","params":{"turn":{"id":"u1","status":"completed","items":[]}}}"#,
    );
    assert!(acks(&step.events).is_empty());
}

#[test]
fn turn_start_is_acknowledged_by_the_notification_when_it_comes_first() {
    let mut p = ready();
    let start = sent(p.encode(InputId(21), Command::prompt("go")));
    let id = id_of(&start, "turn/start");
    let step = p.on_line(TURN_STARTED_U1);
    assert_eq!(
        step.events,
        vec![
            AgentEvent::turn_started(Some("u1".into()), Some(InputId(21))),
            AgentEvent::InputAcknowledged {
                input: InputId(21),
                steered: false
            }
        ]
    );
    // The late result says the same thing: no second acknowledgement.
    let step = p.on_line(&result_line(id, r#"{"turn":{"id":"u1"}}"#));
    assert!(step.events.is_empty() && step.write.is_empty(), "{step:?}");
}

#[test]
fn each_turn_start_is_acknowledged_once_for_its_own_input() {
    let mut p = ready();
    let mut all = Vec::new();
    for (n, turn) in [(30u64, "u1"), (31, "u2")] {
        let start = sent(p.encode(InputId(n), Command::prompt("go")));
        let id = id_of(&start, "turn/start");
        all.extend(acks(
            &p.on_line(&result_line(
                id,
                &format!(r#"{{"turn":{{"id":"{turn}"}}}}"#),
            ))
            .events,
        ));
        let started = format!(
            r#"{{"method":"turn/started","params":{{"threadId":"t1","turn":{{"id":"{turn}"}}}}}}"#
        );
        all.extend(acks(&p.on_line(&started).events));
        let done = format!(
            r#"{{"method":"turn/completed","params":{{"turn":{{"id":"{turn}","status":"completed","items":[]}}}}}}"#
        );
        all.extend(acks(&p.on_line(&done).events));
    }
    assert_eq!(all, vec![(InputId(30), false), (InputId(31), false)]);
}

#[test]
fn a_steered_prompt_is_acknowledged_as_steered_and_starts_no_turn() {
    let mut p = ready();
    let start = sent(p.encode(InputId(40), Command::prompt("a")));
    let start_id = id_of(&start, "turn/start");
    p.on_line(&result_line(start_id, r#"{"turn":{"id":"u1"}}"#));
    p.on_line(TURN_STARTED_U1);
    // A prompt during the running turn is steered.
    let steer = sent(p.encode(InputId(41), Command::prompt("b")));
    let steer_id = id_of(&steer, "turn/steer");
    assert_eq!(w(&steer)[0]["params"]["expectedTurnId"], "u1");
    assert!(acks(&steer.events).is_empty());
    // The shape codex-cli 0.161.0 answers with (wasit-codex/tests/fixtures/steer.ndjson).
    let step = p.on_line(&result_line(steer_id, r#"{"turnId":"u1"}"#));
    assert_eq!(
        step.events,
        vec![AgentEvent::InputAcknowledged {
            input: InputId(41),
            steered: true
        }]
    );
    assert!(step.write.is_empty());
    // It never gets a `TurnStarted` of its own: the turn that ends is the first prompt's.
    let step = p.on_line(
        r#"{"method":"turn/completed","params":{"turn":{"id":"u1","status":"completed","items":[]}}}"#,
    );
    assert!(!step
        .events
        .iter()
        .any(|e| matches!(e, AgentEvent::TurnStarted { .. })));
}

#[test]
fn a_prompt_queued_for_the_turn_id_is_sent_as_a_steer_and_acknowledged_as_one() {
    let mut p = ready();
    let start = sent(p.encode(InputId(50), Command::prompt("a")));
    let start_id = id_of(&start, "turn/start");
    // The turn id is not known yet: the second prompt waits.
    assert_eq!(
        p.encode(InputId(51), Command::prompt("b")).unwrap(),
        Encoded::Queued
    );
    let step = p.on_line(&result_line(start_id, r#"{"turn":{"id":"u1"}}"#));
    assert_eq!(acks(&step.events), vec![(InputId(50), false)]);
    assert!(step
        .events
        .contains(&AgentEvent::InputSent { input: InputId(51) }));
    let steer_id = id_of(&step, "turn/steer");
    let step = p.on_line(&result_line(steer_id, r#"{"turnId":"u1"}"#));
    assert_eq!(acks(&step.events), vec![(InputId(51), true)]);
    // `turn/started` then names the first prompt only.
    let step = p.on_line(TURN_STARTED_U1);
    assert_eq!(
        step.events,
        vec![AgentEvent::turn_started(
            Some("u1".into()),
            Some(InputId(50))
        )]
    );
}

#[test]
fn a_refused_steer_is_reported_dropped_and_not_sent_again() {
    let mut p = ready();
    let start = sent(p.encode(InputId(60), Command::prompt("a")));
    let start_id = id_of(&start, "turn/start");
    p.on_line(&result_line(start_id, r#"{"turn":{"id":"u1"}}"#));
    p.on_line(TURN_STARTED_U1);
    let steer = sent(p.encode(InputId(61), Command::prompt("b")));
    let steer_id = id_of(&steer, "turn/steer");
    // The error codex-cli 0.161.0 answers a steer with when the turn is gone or another one runs.
    let step = p.on_line(&format!(
        r#"{{"error":{{"code":-32600,"message":"no active turn to steer"}},"id":{steer_id}}}"#
    ));
    assert_eq!(
        step.events,
        vec![AgentEvent::InputDropped {
            input: InputId(61),
            reason: InputDropReason::Refused,
            error: Some("no active turn to steer".into())
        }]
    );
    // Not silently re-sent as a turn, and not also a bare `CommandRejected`.
    assert!(step.write.is_empty());
    assert!(acks(&step.events).is_empty());
    // The running turn is untouched: it completes as usual.
    let step = p.on_line(
        r#"{"method":"turn/completed","params":{"turn":{"id":"u1","status":"completed","items":[]}}}"#,
    );
    assert!(matches!(
        &step.events[0],
        AgentEvent::TurnCompleted {
            end_reason: None,
            ..
        }
    ));
    assert!(step.write.is_empty());
}

#[test]
fn a_refused_turn_start_is_reported_dropped_and_ends_the_turn() {
    let mut p = ready();
    let start = sent(p.encode(InputId(70), Command::prompt("a")));
    let start_id = id_of(&start, "turn/start");
    // A prompt waiting behind it goes out as its own turn once this one is gone.
    assert_eq!(
        p.encode(InputId(71), Command::prompt("b")).unwrap(),
        Encoded::Queued
    );
    // The error shape of reference/codex/lifecycle.jsonl (codex-cli 0.169.1).
    let step = p.on_line(&format!(
        r#"{{"error":{{"code":-32600,"message":"thread not found: t1"}},"id":{start_id}}}"#
    ));
    assert_eq!(
        refusals(&step.events),
        vec![(InputId(70), "thread not found: t1".to_string())]
    );
    assert!(acks(&step.events).is_empty());
    // The turn that never started ends, as before.
    assert!(step.events.iter().any(|e| matches!(
        e,
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::ExecutionError),
            reply: Some(r),
            ..
        } if r == "thread not found: t1"
    )));
    // The queued prompt follows, as a turn of its own, naming itself.
    assert!(step
        .events
        .contains(&AgentEvent::InputSent { input: InputId(71) }));
    let next_start = id_of(&step, "turn/start");
    let step = p.on_line(&result_line(next_start, r#"{"turn":{"id":"u2"}}"#));
    assert_eq!(acks(&step.events), vec![(InputId(71), false)]);
}

#[test]
fn an_error_after_the_turn_was_announced_is_not_a_refusal() {
    let mut p = ready();
    let start = sent(p.encode(InputId(80), Command::prompt("a")));
    let start_id = id_of(&start, "turn/start");
    p.on_line(TURN_STARTED_U1);
    let step = p.on_line(&format!(
        r#"{{"error":{{"code":-32603,"message":"late"}},"id":{start_id}}}"#
    ));
    assert!(refusals(&step.events).is_empty(), "{:?}", step.events);
}

#[test]
fn queued_prompts_dropped_by_an_interrupt_are_never_acknowledged() {
    let mut p = ready();
    let start = sent(p.encode(InputId(90), Command::prompt("go")));
    let start_id = id_of(&start, "turn/start");
    p.encode(InputId(91), Command::prompt("a")).unwrap();
    p.encode(InputId(92), Command::prompt("b")).unwrap();
    let step = sent(p.encode(InputId(93), Command::Interrupt));
    assert_eq!(
        drops(&step.events),
        vec![
            (InputId(91), InputDropReason::Interrupted),
            (InputId(92), InputDropReason::Interrupted)
        ]
    );
    // What the CLI reports afterwards concerns the written prompt only.
    let mut after = p
        .on_line(&result_line(start_id, r#"{"turn":{"id":"u1"}}"#))
        .events;
    after.extend(p.on_line(TURN_STARTED_U1).events);
    after.extend(
        p.on_line(
            r#"{"method":"turn/completed","params":{"turn":{"id":"u1","status":"interrupted","items":[]}}}"#,
        )
        .events,
    );
    assert_eq!(acks(&after), vec![(InputId(90), false)]);
    assert!(drops(&after).is_empty());
}

// ── earlier history of a resumed thread ─────────────────────────────────────────

fn history_events(step: &Step) -> Vec<(Option<u64>, AgentEvent)> {
    step.events
        .iter()
        .filter_map(|e| e.as_history().map(|(at, ev)| (at, ev.clone())))
        .collect()
}

/// A `thread/resume` result with every item kind the driver shows, plus kinds it does not, and
/// a turn whose items were not sent.
fn resume_result() -> String {
    json!({"id": 3, "result": {
        "model": "m", "cwd": "/c",
        "thread": {"id": "t9", "turns": [
            {"id": "u1", "startedAt": 1000, "itemsView": "full", "status": "completed", "items": [
                {"type": "userMessage", "id": "i1", "startedAtMs": 1000500,
                 "content": [{"type": "text", "text": "first"}, {"type": "text", "text": "second"},
                             {"type": "localImage", "path": "/w/a.png"}]},
                {"type": "reasoning", "id": "i2", "summary": ["thought"], "content": []},
                {"type": "commandExecution", "id": "i3", "command": "ls", "status": "completed",
                 "exitCode": 0, "aggregatedOutput": "out", "startedAtMs": 1000600, "completedAtMs": 1000700},
                {"type": "fileChange", "id": "i4", "status": "inProgress", "changes": [{"path": "x"}]},
                {"type": "agentMessage", "id": "i5", "text": "answer"},
                {"type": "contextCompaction", "id": "i6"},
                {"type": "plan", "id": "i7", "text": "p"},
                {"type": "somethingNew", "id": "i8"}]},
            {"id": "u2", "startedAt": 2000, "itemsView": "notLoaded", "status": "completed", "items": []}
        ]}}})
    .to_string()
}

#[test]
fn a_resumed_threads_turns_become_history_before_session_init() {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec {
        resume: ResumeMode::Resume("t9".into()),
        ..SpawnSpec::default()
    });
    p.on_line(r#"{"id":1,"result":{}}"#);
    let step = p.on_line(&resume_result());
    let last = step.events.last().unwrap();
    assert!(matches!(last, AgentEvent::SessionInit { .. }), "{last:?}");
    // Everything before it is history, in order.
    let h = history_events(&step);
    assert_eq!(h.len(), step.events.len() - 1);
    let kinds: Vec<&str> = h
        .iter()
        .map(|(_, e)| match e {
            AgentEvent::UserMessage { .. } => "user",
            AgentEvent::UserContent { .. } => "user_content",
            AgentEvent::ThinkingDelta { .. } => "think",
            AgentEvent::ToolCallStarted { .. } => "start",
            AgentEvent::ToolCallFinished { .. } => "finish",
            AgentEvent::AssistantDelta { .. } => "say",
            AgentEvent::Compaction { .. } => "compaction",
            AgentEvent::Notice { .. } => "notice",
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "user",
            "user_content",
            "think",
            "start",
            "finish",
            "start",
            "say",
            "compaction",
            // two items of kinds the driver does not show, said once for the turn; then the
            // second turn's items, which codex did not send
            "notice",
            "notice"
        ]
    );
    // Times: the item's own, else the turn's.
    assert_eq!(h[0].0, Some(1_000_500));
    assert_eq!(h[3].0, Some(1_000_600));
    assert_eq!(h[4].0, Some(1_000_700));
    assert_eq!(h[6].0, Some(1_000_000));
    assert_eq!(h[9].0, Some(2_000_000));
    assert_eq!(
        h[0].1,
        AgentEvent::UserMessage {
            text: "first\nsecond".into()
        }
    );
    // The file change never completed: no end is invented for it.
    assert!(!h.iter().any(
        |(_, e)| matches!(e, AgentEvent::ToolCallFinished { call_id, .. } if call_id == "i4")
    ));
    match &h[8].1 {
        AgentEvent::Notice { message } => assert!(message.starts_with("2 items"), "{message}"),
        other => panic!("{other:?}"),
    }
    // A fresh thread has no history.
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec::default());
    p.on_line(r#"{"id":1,"result":{}}"#);
    let step = p.on_line(&resume_result());
    assert!(history_events(&step).is_empty());
}

/// History is not live activity: a protocol that was handed a resumed thread's turns behaves, for
/// everything after, exactly like one that was not.
#[test]
fn history_changes_nothing_of_the_protocols_state() {
    let live = |opening: &str| {
        let mut p = CodexProtocol::new();
        p.start(&SpawnSpec {
            resume: ResumeMode::Resume("t9".into()),
            ..SpawnSpec::default()
        });
        p.on_line(r#"{"id":1,"result":{}}"#);
        let open = p.on_line(opening);
        let mut out = vec![];
        let s = sent(p.encode(InputId(7), Command::prompt("go")));
        out.extend(w(&s));
        let id = w(&s)[0]["id"].as_u64().unwrap();
        for line in [
            format!(r#"{{"id":{id},"result":{{"turn":{{"id":"n1","items":[]}}}}}}"#),
            r#"{"method":"turn/started","params":{"turn":{"id":"n1"}}}"#.to_string(),
            r#"{"method":"item/started","params":{"item":{"type":"commandExecution","id":"i3","command":"ls"}}}"#.to_string(),
            r#"{"method":"item/completed","params":{"item":{"type":"commandExecution","id":"i3","status":"completed","exitCode":0}}}"#.to_string(),
            r#"{"method":"turn/completed","params":{"turn":{"id":"n1","status":"completed","items":[]}}}"#.to_string(),
        ] {
            let step = p.on_line(&line);
            out.extend(w(&step));
            out.extend(step.events.iter().map(|e| json!(format!("{e:?}"))));
        }
        (open, out)
    };
    let with = live(&resume_result());
    let without = live(r#"{"id":3,"result":{"thread":{"id":"t9"},"model":"m","cwd":"/c"}}"#);
    let (open_with, rest_with) = with;
    let (open_without, rest_without) = without;
    let live_only: Vec<&AgentEvent> = open_with
        .events
        .iter()
        .filter(|e| e.as_history().is_none())
        .collect();
    assert_eq!(live_only, open_without.events.iter().collect::<Vec<_>>());
    assert_eq!(open_with.write, open_without.write);
    assert_eq!(rest_with, rest_without);
}

#[test]
fn history_items_without_ids_or_with_odd_shapes_do_not_panic() {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec {
        resume: ResumeMode::Resume("t9".into()),
        ..SpawnSpec::default()
    });
    p.on_line(r#"{"id":1,"result":{}}"#);
    let step = p.on_line(
        r#"{"id":3,"result":{"thread":{"id":"t9","turns":[7,{"items":"x"},{"items":[null,{"type":"commandExecution"},{"type":"userMessage","content":5}]}]}}}"#,
    );
    assert!(matches!(
        step.events.last(),
        Some(AgentEvent::SessionInit { .. })
    ));
}

#[test]
fn conversations_in_a_project_are_that_projects_rollouts_read_deep() {
    let desc = crate::descriptor();
    let ctx = |project: Option<&str>| wasit_core::DiscoveryContext {
        home: Some("/h".into()),
        project: project.map(str::to_string),
        ..Default::default()
    };
    let probe = desc.conversations_in_project(&ctx(Some("/p/"))).unwrap();
    assert_eq!(probe.kind, wasit_core::DiscoveryKind::Sessions);
    match &probe.action {
        wasit_core::ProbeAction::ListFiles {
            dir,
            max_files,
            count_containing,
            recursive,
            ..
        } => {
            assert_eq!(dir, "/h/.codex/sessions");
            assert_eq!(*max_files, 2000);
            assert!(count_containing.is_empty(), "no full-file read");
            assert!(*recursive);
        }
        other => panic!("{other:?}"),
    }
    let rollout = |id: &str, cwd: &str, ms: u64| wasit_core::FileHead {
        path: format!("/h/.codex/sessions/2026/11/17/rollout-2026-11-17T23-19-35-{id}.jsonl"),
        modified_ms: ms,
        head: vec![format!(
            r#"{{"type":"session_meta","payload":{{"id":"{id}","cwd":{}}}}}"#,
            serde_json::to_string(cwd).unwrap()
        )],
        matching_lines: None,
    };
    let out = ProbeOutput::Files {
        files: vec![
            rollout("01a1ebcf-0528-7cd2-a32d-e89e91b6a844", "/p", 9),
            rollout("01a1ebcf-0528-7cd2-a32d-e89e91b6a845", "/other", 8),
            rollout("01a1ebcf-0528-7cd2-a32d-e89e91b6a846", "/p", 7),
        ],
    };
    let ids = |probe: wasit_core::DiscoveryProbe| match (probe.parse)(&out).unwrap() {
        Discovered::Sessions(s) => s.into_iter().map(|s| s.id).collect::<Vec<_>>(),
        d => panic!("{d:?}"),
    };
    assert_eq!(
        ids(probe),
        [
            "01a1ebcf-0528-7cd2-a32d-e89e91b6a844",
            "01a1ebcf-0528-7cd2-a32d-e89e91b6a846"
        ]
    );
    // No project: everything.
    assert_eq!(
        ids(desc.conversations_in_project(&ctx(None)).unwrap()).len(),
        3
    );
    // Windows spelling of the same folder.
    let win = wasit_core::DiscoveryContext {
        home: Some("C:\\Users\\me".into()),
        project: Some("C:\\w\\p".into()),
        ..Default::default()
    };
    let out_win = ProbeOutput::Files {
        files: vec![rollout(
            "01a1ebcf-0528-7cd2-a32d-e89e91b6a847",
            "C:\\w\\p\\",
            1,
        )],
    };
    match (desc.conversations_in_project(&win).unwrap().parse)(&out_win).unwrap() {
        Discovered::Sessions(s) => assert_eq!(s.len(), 1),
        d => panic!("{d:?}"),
    }
    // No home, no CODEX_HOME: the CLI cannot say.
    assert!(desc
        .conversations_in_project(&wasit_core::DiscoveryContext::default())
        .is_none());
}

/// A resumed thread that has a first message but arrives without turns is not an empty
/// conversation: history says codex sent none.
#[test]
fn a_resumed_thread_without_turns_that_has_a_preview_says_nothing_was_sent() {
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec {
        resume: wasit_core::ResumeMode::Resume("t1".into()),
        ..SpawnSpec::default()
    });
    p.on_line(r#"{"id":1,"result":{}}"#);
    let step = p.on_line(
        r#"{"id":3,"result":{"thread":{"id":"t1","preview":"hello","turns":[]},"model":"m"}}"#,
    );
    let history: Vec<&AgentEvent> = step
        .events
        .iter()
        .filter_map(|e| e.as_history().map(|(_, e)| e))
        .collect();
    assert!(
        matches!(history.as_slice(), [AgentEvent::Notice { message }] if message.contains("did not send")),
        "{:?}",
        step.events
    );
    assert!(matches!(
        step.events.last(),
        Some(AgentEvent::SessionInit { .. })
    ));
    // A thread with no preview and no turns is just a new thread: nothing to say.
    let mut p = CodexProtocol::new();
    p.start(&SpawnSpec {
        resume: wasit_core::ResumeMode::Resume("t2".into()),
        ..SpawnSpec::default()
    });
    p.on_line(r#"{"id":1,"result":{}}"#);
    let step = p.on_line(r#"{"id":3,"result":{"thread":{"id":"t2","preview":"","turns":[]}}}"#);
    assert!(step.events.iter().all(|e| e.as_history().is_none()));
}

/// A rollout whose first prompt is megabytes of pasted text is listed with a title of at most
/// `TITLE_CHARS` characters ending with the cut marker; a long `cwd` is cut too. (Synthetic
/// text only.)
#[test]
fn a_huge_first_prompt_becomes_a_bounded_title() {
    use wasit_core::{limits, CUT};
    let huge = "€".repeat(2 * 1024 * 1024);
    let meta = json!({"type":"session_meta","payload":{
        "id":"01a1ebcf-0528-7cd2-a32d-e89e91b6a844","cwd":format!("/p/{}", "d".repeat(100_000))}});
    let prompt = json!({"type":"response_item","payload":{"role":"user",
        "content":[{"type":"input_text","text":huge}]}});
    let out = ProbeOutput::Files {
        files: vec![wasit_core::FileHead {
            path: "/h/.codex/sessions/2026/11/17/rollout-2026-11-17T23-19-35-01a1ebcf-0528-7cd2-a32d-e89e91b6a844.jsonl".into(),
            modified_ms: 9,
            head: vec![meta.to_string(), prompt.to_string()],
            matching_lines: Some(2),
        }],
    };
    let Discovered::Sessions(s) = parse_sessions(&out).unwrap() else {
        panic!("not sessions");
    };
    assert_eq!(s[0].title.chars().count(), limits::TITLE_CHARS);
    assert!(s[0].title.ends_with(CUT) && s[0].title.starts_with("€€"));
    assert_eq!(
        s[0].project.as_deref().unwrap().chars().count(),
        limits::PATH_CHARS
    );
}
