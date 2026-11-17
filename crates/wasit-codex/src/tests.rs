use super::*;
use serde_json::json;
use wasit_core::{
    AgentEvent, Command, DriverError, Encoded, EndReason, GateDecision, GateRequest,
    QuestionAnswer, ResumeMode, SkillRef, Step,
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
    let step = p.start(&SpawnSpec {
        cwd: Some("/c".into()),
        model: Some("gpt".into()),
        first_prompt: Some("hi".into()),
        ..SpawnSpec::default()
    });
    assert_eq!(w(&step)[0]["method"], "initialize");
    assert_eq!(
        w(&step)[0]["params"]["capabilities"]["experimentalApi"],
        true
    );
    // Commands wait for the handshake.
    assert_eq!(
        p.encode(Command::SetModel { model: "x".into() }).unwrap(),
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
    // Queued commands in order: the first prompt (queued by start), then
    // set_model.
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

#[test]
fn turn_lifecycle_usage_and_reply() {
    let mut p = ready();
    let s = sent(p.encode(Command::Prompt { text: "go".into() }));
    let id = w(&s)[0]["id"].as_u64().unwrap();
    p.on_line(&format!(
        r#"{{"id":{id},"result":{{"turn":{{"id":"u1","status":"inProgress","items":[]}}}}}}"#
    ));
    let ev = p
        .on_line(r#"{"method":"turn/started","params":{"threadId":"t1","turn":{"id":"u1"}}}"#)
        .events;
    assert_eq!(
        ev,
        vec![AgentEvent::TurnStarted {
            native_turn_id: Some("u1".into())
        }]
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
    sent(p.encode(Command::Prompt { text: "a".into() }));
    // Turn id unknown yet: queued.
    assert_eq!(
        p.encode(Command::Prompt { text: "b".into() }).unwrap(),
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
    sent(p.encode(Command::Prompt { text: "a".into() }));
    // Interrupt before the turn id is known waits for it.
    assert!(sent(p.encode(Command::Interrupt)).write.is_empty());
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
    sent(p.encode(Command::Prompt { text: "b".into() }));
    p.on_line(r#"{"method":"turn/started","params":{"turn":{"id":"u3"}}}"#);
    p.on_line(r#"{"method":"item/commandExecution/requestApproval","id":0,"params":{"itemId":"call_1","command":"touch x","cwd":"/c"}}"#);
    let step = sent(p.encode(Command::Interrupt));
    let lines = w(&step);
    assert_eq!(
        lines[0],
        json!({"jsonrpc":"2.0","id":0,"result":{"decision":"decline"}})
    );
    assert_eq!(lines[1]["params"]["turnId"], "u3");
    assert_eq!(
        step.events,
        vec![AgentEvent::GateResolved {
            request_id: "call_1".into()
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
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "call_3".into(),
        decision: GateDecision::approve(),
    }));
    assert_eq!(
        w(&step)[0],
        json!({"jsonrpc":"2.0","id":0,"result":{"decision":"accept"}})
    );
    // Our own answer's acknowledgement is silent.
    assert!(p
        .on_line(r#"{"method":"serverRequest/resolved","params":{"requestId":0}}"#)
        .events
        .is_empty());

    p.on_line(r#"{"method":"execCommandApproval","id":5,"params":{"command":["ls"]}}"#);
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "codex:5".into(),
        decision: GateDecision::Approve {
            updated_input: None,
            permission_updates: Some(json!(true)),
            elicitation_content: None,
        },
    }));
    assert_eq!(
        w(&step)[0]["result"],
        json!({"decision":"approved_for_session"})
    );

    p.on_line(r#"{"method":"item/permissions/requestApproval","id":6,"params":{"itemId":"p1","permissions":{"network":{"enabled":true}}}}"#);
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "p1".into(),
        decision: GateDecision::approve(),
    }));
    assert_eq!(
        w(&step)[0]["result"],
        json!({"permissions":{"network":{"enabled":true}},"scope":"turn"})
    );

    p.on_line(r#"{"method":"mcpServer/elicitation/request","id":7,"params":{"serverName":"hares","message":"ok?","requestedSchema":{"properties":{"grant":{"enum":["once"]}}}}}"#);
    let step = sent(p.encode(Command::AnswerGate {
        request_id: "codex:7".into(),
        decision: GateDecision::deny(None),
    }));
    assert_eq!(w(&step)[0]["result"], json!({"action":"decline"}));

    // A gate resolved by codex itself.
    p.on_line(r#"{"method":"item/fileChange/requestApproval","id":8,"params":{"itemId":"f1","changes":{}}}"#);
    let ev = p
        .on_line(r#"{"method":"serverRequest/resolved","params":{"requestId":8}}"#)
        .events;
    assert_eq!(
        ev,
        vec![AgentEvent::GateResolved {
            request_id: "f1".into()
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
        p.encode(Command::AnswerGate {
            request_id: "q1".into(),
            decision: GateDecision::approve()
        }),
        Err(DriverError::Invalid { .. })
    ));
    let mut a = QuestionAnswer::default();
    a.answers.insert("Color?".into(), "Red, Blue".into());
    let step = sent(p.encode(Command::AnswerQuestion {
        request_id: "q1".into(),
        answer: a,
    }));
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
    let step = sent(p.encode(Command::SetMode {
        mode: "plan".into(),
    }));
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
    let s = sent(p.encode(Command::SetModel {
        model: "bad".into(),
    }));
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
            result: Some(json!("no"))
        }]
    );
    let ev = p.on_line(r#"{"method":"item/started","params":{"item":{"type":"mcpToolCall","id":"m1","server":"fs","tool":"read","arguments":{"p":1}}}}"#).events;
    assert!(matches!(&ev[0], AgentEvent::ToolCallStarted { name, .. } if name == "fs.read"));
    let step = p.on_line(r#"{"method":"currentTime/read","id":44,"params":{}}"#);
    assert_eq!(w(&step)[0]["error"]["code"], -32601);
    assert!(p
        .encode(Command::RunSlash {
            name: "x".into(),
            args: String::new()
        })
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
            },
            McpServerConfig {
                name: "bad.name".into(),
                transport: McpTransport::Http {
                    url: "http://h".into(),
                    headers: Default::default(),
                },
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
        }],
    };
    match parse_sessions(&out).unwrap() {
        Discovered::Sessions(s) => {
            assert_eq!(s[0].id, "01a1ebcf-0528-7cd2-a32d-e89e91b6a844");
            assert_eq!(s[0].project.as_deref(), Some("/p"));
            assert_eq!(s[0].title, "hello");
        }
        d => panic!("{d:?}"),
    }
}

#[test]
fn determinism_same_inputs_same_outputs() {
    let run = || {
        let mut p = CodexProtocol::new();
        let mut out = vec![format!(
            "{:?}",
            p.start(&SpawnSpec {
                first_prompt: Some("a".into()),
                ..SpawnSpec::default()
            })
        )];
        for l in [
            r#"{"id":1,"result":{}}"#,
            r#"{"id":3,"result":{"thread":{"id":"t"},"model":"m"}}"#,
            r#"{"method":"turn/started","params":{"turn":{"id":"u"}}}"#,
            r#"{"method":"item/commandExecution/requestApproval","id":0,"params":{"itemId":"c"}}"#,
        ] {
            out.push(format!("{:?}", p.on_line(l)));
        }
        out.push(format!("{:?}", p.encode(Command::Interrupt)));
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
