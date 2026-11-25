use super::*;
use serde_json::json;
use wasit_core::{
    AgentEvent, Command, ContentPart, DriverError, Encoded, EndReason, GateDecision, GateRequest,
    InputDropReason, InputId, QuestionAnswer, ResumeMode, SkillRef, Step, Usage,
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

const SESSION_NEW: &str = r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_1","configOptions":[{"id":"model","currentValue":"opencode/big-pickle","options":[{"value":"opencode/big-pickle","name":"Big Pickle"},{"value":"opencode/free","name":"Free"}]},{"id":"mode","currentValue":"build","options":[{"value":"build","name":"build"},{"value":"plan","name":"plan","description":"read only"}]}]}}"#;

fn ready() -> AcpProtocol {
    let mut p = AcpProtocol::new("wasit-opencode");
    p.start(&SpawnSpec::default());
    p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"list":{}}}}}"#);
    p.on_line(SESSION_NEW);
    p
}

#[test]
fn handshake_applies_launch_model_and_mode_then_first_prompt() {
    let mut p = AcpProtocol::new("wasit-kilo");
    let (spec, first) = SpawnSpec {
        cwd: Some("/c".into()),
        model: Some("opencode/free".into()),
        mode: "plan".into(),
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
    assert_eq!(
        w(&step)[0],
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"wasit-kilo","version":env!("CARGO_PKG_VERSION")}}})
    );
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}"#);
    assert_eq!(
        w(&step)[0],
        json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/c","mcpServers":[]}})
    );
    let step = p.on_line(SESSION_NEW);
    match &step.events[0] {
        AgentEvent::SessionInit {
            native_session_id,
            model,
            available_modes,
            current_mode,
            ..
        } => {
            assert_eq!(native_session_id.as_deref(), Some("ses_1"));
            assert_eq!(model.as_deref(), Some("opencode/big-pickle"));
            assert_eq!(available_modes.len(), 2);
            assert_eq!(available_modes[1].description, "read only");
            assert_eq!(current_mode, "build");
        }
        e => panic!("{e:?}"),
    }
    assert!(
        matches!(&step.events[1], AgentEvent::ModelsUpdated { models, current: Some(c) } if models.len() == 2 && c == "opencode/big-pickle")
    );
    let lines = w(&step);
    assert_eq!(
        lines[0]["params"],
        json!({"sessionId":"ses_1","configId":"model","value":"opencode/free"})
    );
    assert_eq!(
        lines[1]["params"],
        json!({"sessionId":"ses_1","configId":"mode","value":"plan"})
    );
    assert_eq!(lines[2]["method"], "session/prompt");
    assert_eq!(
        step.events[step.events.len() - 2..],
        [
            AgentEvent::InputSent { input: InputId(1) },
            AgentEvent::turn_started(None, Some(InputId(1)))
        ]
    );
}

#[test]
fn resume_loads_and_turns_the_history_replay_into_history_events() {
    let mut p = AcpProtocol::new("x");
    p.start(&SpawnSpec {
        resume: ResumeMode::Resume("ses_old".into()),
        ..SpawnSpec::default()
    });
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
    assert_eq!(w(&step)[0]["method"], "session/load");
    assert_eq!(w(&step)[0]["params"]["sessionId"], "ses_old");
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"old"}}}}"#).events;
    // What `session/load` replays is the conversation's earlier history, not live activity.
    assert_eq!(
        ev,
        vec![AgentEvent::AssistantDelta {
            text: "old".into(),
            message_id: None
        }
        .earlier(None)]
    );
    let ev = p
        .on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#)
        .events;
    assert!(
        matches!(&ev[0], AgentEvent::SessionInit { native_session_id: Some(id), .. } if id == "ses_old")
    );
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"new"}}}}"#).events;
    assert_eq!(ev.len(), 1);
}

#[test]
fn continue_lists_then_loads_latest() {
    let mut p = AcpProtocol::new("x");
    p.start(&SpawnSpec {
        cwd: Some("/c".into()),
        resume: ResumeMode::Continue,
        ..SpawnSpec::default()
    });
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{"agentCapabilities":{"sessionCapabilities":{"list":{}}}}}"#);
    assert_eq!(w(&step)[0]["method"], "session/list");
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"sessions":[{"sessionId":"a","updatedAt":"2026-01-01"},{"sessionId":"b","updatedAt":"2026-02-01"}]}}"#);
    assert_eq!(w(&step)[0]["params"]["sessionId"], "b");
}

#[test]
fn prompt_turn_usage_and_queueing() {
    let mut p = ready();
    let step = sent(p.encode(InputId(101), Command::prompt("a")));
    assert_eq!(w(&step)[0]["params"]["prompt"][0]["text"], "a");
    assert_eq!(
        p.encode(InputId(102), Command::prompt("b")).unwrap(),
        Encoded::Queued
    );
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","messageId":"m","content":{"type":"text","text":"Hi"}}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::AssistantDelta {
            text: "Hi".into(),
            message_id: Some("m".into())
        }]
    );
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::UsageUpdate { usage, context_pct: Some(p) } if usage.cost_usd == 0.25 && (*p - 25.0).abs() < 1e-3)
    );
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","usage":{"inputTokens":136,"outputTokens":10,"cachedReadTokens":11136}}}"#);
    // (The updates above carry no session id, so they are not known to be ours: the result is
    // what acknowledges the prompt, ahead of the turn's end.)
    assert!(matches!(
        &step.events[0],
        AgentEvent::InputAcknowledged { .. }
    ));
    assert!(
        matches!(&step.events[1], AgentEvent::TurnCompleted { end_reason: None, usage, .. } if usage.input_tokens == 136 && usage.cache_read_tokens == 11136)
    );
    // The held prompt goes out now.
    assert_eq!(w(&step)[0]["params"]["prompt"][0]["text"], "b");
}

#[test]
fn permission_gate_options_and_answers() {
    let mut p = ready();
    p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"c1","title":"bash","kind":"execute","status":"pending","rawInput":{}}}}"#);
    let gate = r#"{"jsonrpc":"2.0","id":0,"method":"session/request_permission","params":{"sessionId":"ses_1","toolCall":{"toolCallId":"c1","title":"touch a","kind":"execute","rawInput":{"command":"touch a"}},"options":[{"optionId":"once","kind":"allow_once","name":"Allow once"},{"optionId":"always","kind":"allow_always","name":"Always allow"},{"optionId":"reject","kind":"reject_once","name":"Reject"}]}}"#;
    let ev = p.on_line(gate).events;
    match &ev[0] {
        AgentEvent::GateRequested {
            gate:
                GateRequest::Permission {
                    request_id,
                    tool_name,
                    command,
                    choices,
                    raw_input,
                    ..
                },
        } => {
            assert_eq!(request_id, "c1");
            assert_eq!(tool_name, "bash");
            assert_eq!(command, "touch a");
            assert_eq!(choices.len(), 3);
            assert_eq!(choices[0].id, "once");
            assert_eq!(choices[0].label, "Allow once");
            assert!(!choices[0].neutral_label);
            assert_eq!(choices[1].kind, wasit_core::GateChoiceKind::AllowAlways);
            assert_eq!(choices[2].kind, wasit_core::GateChoiceKind::RejectOnce);
            assert_eq!(raw_input["command"], "touch a");
        }
        e => panic!("{e:?}"),
    }
    let step = sent(p.encode(
        InputId(103),
        Command::AnswerGate {
            request_id: "c1".into(),
            decision: GateDecision::choose("once"),
        },
    ));
    assert_eq!(
        w(&step)[0],
        json!({"jsonrpc":"2.0","id":0,"result":{"outcome":{"outcome":"selected","optionId":"once"}}})
    );
    for id in ["reject", "always"] {
        p.on_line(gate);
        let step = sent(p.encode(
            InputId(104),
            Command::AnswerGate {
                request_id: "c1".into(),
                decision: GateDecision::choose(id),
            },
        ));
        assert_eq!(w(&step)[0]["result"]["outcome"]["optionId"], id);
    }
    // An id the agent did not offer is refused and the gate stays pending.
    p.on_line(gate);
    assert!(matches!(
        p.encode(
            InputId(105),
            Command::AnswerGate {
                request_id: "c1".into(),
                decision: GateDecision::choose("nope"),
            }
        ),
        Err(DriverError::Invalid { .. })
    ));
    let step = sent(p.encode(
        InputId(106),
        Command::AnswerGate {
            request_id: "c1".into(),
            decision: GateDecision::choose("once"),
        },
    ));
    assert_eq!(w(&step)[0]["result"]["outcome"]["optionId"], "once");
    // ACP has no question requests.
    p.on_line(gate);
    assert!(matches!(
        p.encode(
            InputId(107),
            Command::AnswerQuestion {
                request_id: "c1".into(),
                answer: QuestionAnswer::default()
            }
        ),
        Err(DriverError::Invalid { .. })
    ));
}

#[test]
fn interrupt_cancels_gates_then_sends_cancel_notification() {
    let mut p = ready();
    sent(p.encode(InputId(108), Command::prompt("a")));
    p.on_line(r#"{"jsonrpc":"2.0","id":7,"method":"session/request_permission","params":{"toolCall":{"toolCallId":"c2"},"options":[]}}"#);
    let step = sent(p.encode(InputId(109), Command::Interrupt));
    let lines = w(&step);
    assert_eq!(
        lines[0],
        json!({"jsonrpc":"2.0","id":7,"result":{"outcome":{"outcome":"cancelled"}}})
    );
    assert_eq!(
        lines[1],
        json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"ses_1"}})
    );
    assert_eq!(
        step.events,
        vec![AgentEvent::GateResolved {
            request_id: "c2".into(),
            reason: None,
        }]
    );
    let ev = p
        .on_line(r#"{"jsonrpc":"2.0","id":3,"result":{"stopReason":"cancelled"}}"#)
        .events;
    // The result acknowledges the prompt (nothing else did), then the turn ends.
    assert!(matches!(&ev[0], AgentEvent::InputAcknowledged { .. }));
    assert!(matches!(
        &ev[1],
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::Cancelled),
            was_interrupted: true,
            ..
        }
    ));
}

#[test]
fn config_updates_tools_subagents_and_slash() {
    let mut p = ready();
    let step = sent(p.encode(
        InputId(110),
        Command::SetMode {
            mode: "plan".into(),
        },
    ));
    assert_eq!(w(&step)[0]["method"], "session/set_config_option");
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"config_option_update","configOptions":[{"id":"mode","currentValue":"plan","options":[]}]}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::ModesUpdated {
            available_modes: vec![],
            current_mode: Some("plan".into())
        }]
    );
    // The response repeating the same value is not reported twice.
    assert!(p.on_line(r#"{"jsonrpc":"2.0","id":3,"result":{"configOptions":[{"id":"mode","currentValue":"plan"}]}}"#).events.is_empty());
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"t","title":"task","rawInput":{}}}}"#).events;
    assert!(ev.is_empty());
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"t","status":"in_progress","rawInput":{"description":"explore"}}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::ToolCallStarted { name, is_subagent: true, .. } if name == "explore")
    );
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"t","status":"completed","rawInput":{"description":"explore"},"content":[{"type":"content"}]}}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::ToolCallFinished { ok: true, .. }
    ));
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"available_commands_update","availableCommands":[{"name":"review","description":"r","input":{"hint":"[pr]"}}]}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::SlashCommandsUpdated { commands } if commands[0].argument_hint == "[pr]")
    );
    let step = sent(p.encode(
        InputId(111),
        Command::RunSlash {
            name: "review".into(),
            args: "12".into(),
        },
    ));
    assert_eq!(w(&step)[0]["params"]["prompt"][0]["text"], "/review 12");
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":5,"method":"fs/read_text_file","params":{}}"#);
    assert_eq!(w(&step)[0]["error"]["code"], -32601);
}

#[test]
fn render_config_env_json_per_flavor() {
    for (flavor, var) in [
        (Flavor::OpenCode, "OPENCODE_CONFIG_CONTENT"),
        (Flavor::Kilo, "KILO_CONFIG_CONTENT"),
    ] {
        let d = AcpDescriptor { flavor };
        let mut spec = SpawnSpec {
            session_dir: Some("/s".into()),
            ..SpawnSpec::default()
        };
        spec.env
            .push((var.into(), r#"{"permission":{"bash":"ask"}}"#.into()));
        let cfg = AgentConfig {
            mcp_servers: vec![
                McpServerConfig {
                    name: "wasl".into(),
                    transport: McpTransport::Stdio {
                        command: "wasl".into(),
                        args: vec!["mcp".into()],
                        env: Default::default(),
                    },
                    trusted: true,
                },
                McpServerConfig {
                    name: "web".into(),
                    transport: McpTransport::Http {
                        url: "http://h".into(),
                        headers: Default::default(),
                    },
                    trusted: false,
                },
            ],
            skills: vec![SkillRef {
                name: "s".into(),
                root: "/sk".into(),
            }],
            plugins: vec![wasit_core::PluginRef {
                name: "p".into(),
                path: "/p".into(),
            }],
            extra_instructions: Some("Be brief.".into()),
            ..AgentConfig::default()
        };
        assert!(d.render_config(&cfg, &mut spec).is_empty());
        let (k, v) = spec.env.iter().find(|(k, _)| k == var).unwrap();
        assert_eq!(k, var);
        let v: Value = serde_json::from_str(v).unwrap();
        assert_eq!(v["permission"]["bash"], "ask");
        assert_eq!(v["permission"]["wasl_*"], "allow");
        assert_eq!(
            v["mcp"]["wasl"],
            json!({"type":"local","command":["wasl","mcp"],"environment":{}})
        );
        assert_eq!(v["mcp"]["web"]["type"], "remote");
        assert_eq!(v["skills"], json!({"paths":["/sk/s"]}));
        assert_eq!(v["plugin"], json!(["/p"]));
        assert_eq!(v["instructions"], json!(["/s/wasit-instructions.md"]));
        assert_eq!(spec.files[0].contents, "Be brief.");
        // The descriptor's argv is the CLI's own; a wrapped flavor is launched as
        // `<host> <wrapper> -- <argv>` by the runtime (see `wrapper` and the runtime's launch test).
        assert_eq!(d.argv(&spec), [flavor.binary(), "acp"]);
        match flavor {
            Flavor::Kilo => assert_eq!(d.wrapper().map(|w| w.subcommand), Some("kilo-bridge")),
            Flavor::OpenCode => assert!(d.wrapper().is_none()),
        }
    }
    let d = AcpDescriptor {
        flavor: Flavor::Kilo,
    };
    let mut spec = SpawnSpec::default();
    let u = d.render_config(
        &AgentConfig {
            extra_instructions: Some("x".into()),
            ..AgentConfig::default()
        },
        &mut spec,
    );
    assert_eq!(u[0].item, "extra_instructions");
}

#[test]
fn discovery_parsers() {
    let out = ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: "opencode/a\nopencode/b\n\n".into(),
    };
    assert!(matches!(parse_models(&out).unwrap(), Discovered::Models(m) if m.len() == 2));
    let out = ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: r#"[{"id":"ses_1","title":"t","updated":5,"directory":"/p"},{"title":"no id"}]"#
            .into(),
    };
    match parse_sessions(&out).unwrap() {
        Discovered::Sessions(s) => {
            assert_eq!(s.len(), 1);
            assert_eq!(s[0].project.as_deref(), Some("/p"));
            assert_eq!(s[0].modified_ms, 5);
        }
        d => panic!("{d:?}"),
    }
    assert!(parse_sessions(&ProbeOutput::Exec {
        exit_code: Some(1),
        stdout: String::new()
    })
    .is_err());
    // No sessions: the CLI prints nothing.
    assert_eq!(
        parse_sessions(&ProbeOutput::Exec {
            exit_code: Some(0),
            stdout: "\n".into()
        })
        .unwrap(),
        Discovered::Sessions(vec![])
    );
    assert_eq!(opencode().id().as_str(), "opencode");
    assert_eq!(kilo().default_binary(), "kilo");
}

#[test]
fn determinism_same_inputs_same_outputs() {
    let run = || {
        let mut p = AcpProtocol::new("x");
        let mut out = vec![format!(
            "{:?}",
            p.start(&SpawnSpec {
                model: Some("opencode/free".into()),
                ..SpawnSpec::default()
            })
        )];
        out.push(format!("{:?}", p.encode(InputId(1), Command::prompt("a"))));
        for l in [r#"{"jsonrpc":"2.0","id":1,"result":{}}"#, SESSION_NEW] {
            out.push(format!("{:?}", p.on_line(l)));
        }
        out.push(format!("{:?}", p.encode(InputId(112), Command::Interrupt)));
        out
    };
    assert_eq!(run(), run());
}

#[test]
fn ready_only_after_the_session_is_open() {
    let mut p = AcpProtocol::new("wasit-opencode");
    assert!(!p.is_ready());
    p.start(&SpawnSpec::default());
    p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}"#);
    assert!(!p.is_ready());
    assert!(ready().is_ready());
}

// ── config discovery + disabling (shapes captured from opencode 1.19.6 / kilo 7.8.27) ──

#[test]
fn debug_config_and_skill_parse() {
    let cfg = r#"{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {"gmcp": {"type": "local", "command": ["echo", "hi"], "enabled": true, "environment": {"A": "1"}},
          "ghttp": {"type": "remote", "url": "https://example.invalid/mcp"},
          "off": {"type": "local", "command": ["cat"], "enabled": false}},
  "plugin": ["some-plugin", ["@kilocode/kilo-indexing", {}]],
  "plugin_origins": [{"spec": "some-plugin", "source": "/h/.config/opencode", "scope": "global"}],
  "agent": {"myagent": {"name": "myagent", "description": "my agent", "mode": "subagent", "prompt": "p", "options": {}, "permission": {}}},
  "mode": {}, "command": {"deploy": {"description": "ship it", "template": "x"}}, "username": "unknown"
}"#;
    let out = wasit_core::ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: cfg.into(),
    };
    let wasit_core::Discovered::Config(items) = crate::parse_debug_config(&out).unwrap() else {
        panic!()
    };
    let find = |n: &str| items.iter().find(|i| i.name == n).unwrap();
    assert_eq!(
        find("gmcp").mcp,
        Some(wasit_core::McpTransport::Stdio {
            command: "echo".into(),
            args: vec!["hi".into()],
            env: [("A".to_string(), "1".to_string())].into()
        })
    );
    assert!(!find("off").enabled);
    assert_eq!(find("some-plugin").source.scope, "global");
    assert_eq!(find("@kilocode/kilo-indexing").source.scope, "builtin");
    assert_eq!(find("myagent").kind, wasit_core::ConfigItemKind::Agent);
    assert_eq!(find("deploy").description, "ship it");

    let skills = r#"[{"name":"customize-opencode","description":"d","location":"<built-in>","content":"x"},
        {"name":"gskill","description":"global skill","location":"/h/.config/opencode/skills/gskill/SKILL.md","content":"b"},
        {"name":"pskill","description":"project skill","location":"/w/p/.opencode/skill/pskill/SKILL.md","content":"b"}]"#;
    let out = wasit_core::ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: skills.into(),
    };
    let wasit_core::Discovered::Config(items) =
        crate::parse_debug_skill(&out, Some("/w/p")).unwrap()
    else {
        panic!()
    };
    let scopes: Vec<&str> = items.iter().map(|i| i.source.scope.as_str()).collect();
    assert_eq!(scopes, ["builtin", "global", "project"]);
}

#[test]
fn disabled_items_merge_into_the_inline_config() {
    use wasit_core::{ConfigItemKind as K, DisabledItem};
    let d = |kind, name: &str| DisabledItem {
        kind,
        name: name.into(),
        path: None,
    };
    for desc in [crate::opencode(), crate::kilo()] {
        let cfg = AgentConfig {
            disabled: vec![
                d(K::McpServer, "gmcp"),
                d(K::Agent, "myagent"),
                d(K::Skill, "gskill"),
                d(K::Plugin, "some-plugin"),
                d(K::Command, "deploy"),
            ],
            ..AgentConfig::default()
        };
        let mut spec = SpawnSpec::default();
        let unsupported = desc.render_config(&cfg, &mut spec);
        let items: Vec<&str> = unsupported.iter().map(|u| u.item.as_str()).collect();
        assert_eq!(items, ["plugin:some-plugin", "command:deploy"]);
        let (_, v) = spec
            .env
            .iter()
            .find(|(k, _)| k.ends_with("_CONFIG_CONTENT"))
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(v).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "mcp": {"gmcp": {"enabled": false}},
                "agent": {"myagent": {"disable": true}},
                "permission": {"skill": {"gskill": "deny"}},
                "tools": {"question": false}
            })
        );
    }
}

#[test]
fn the_question_tool_is_always_turned_off() {
    // It never reaches ACP (the turn would hang); the model asks in plain text instead.
    for flavor in [crate::Flavor::OpenCode, crate::Flavor::Kilo] {
        let desc = crate::AcpDescriptor { flavor };
        let mut spec = SpawnSpec::default();
        spec.env.push((
            flavor.config_env().to_string(),
            r#"{"tools":{"bash":true}}"#.into(),
        ));
        desc.render_config(&AgentConfig::default(), &mut spec);
        let raw = &spec
            .env
            .iter()
            .rev()
            .find(|(k, _)| k == flavor.config_env())
            .unwrap()
            .1;
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(
            v["tools"],
            serde_json::json!({"bash": true, "question": false})
        );
    }
}

/// Discovery never opens a session in the project (the CLI would keep it and list it), and
/// lists the project's own sessions from inside it.
#[test]
fn discovery_keeps_throwaway_sessions_out_of_the_project() {
    for desc in [opencode(), kilo()] {
        let ctx = DiscoveryContext {
            binary: Some("cli".into()),
            home: None,
            env: vec![],
            project: Some("/work/app".into()),
        };
        for p in desc.discovery(&ctx) {
            match (&p.kind, &p.action) {
                (_, ProbeAction::Handshake { spec, .. }) => assert_eq!(spec.cwd, None),
                (DiscoveryKind::Sessions, ProbeAction::Exec { cwd, .. }) => {
                    assert_eq!(cwd.as_deref(), Some("/work/app"))
                }
                _ => {}
            }
        }
    }
}

/// opencode announces a shell call with only its `cwd` and sends the command in an update.
#[test]
fn a_tool_update_carries_the_arguments_that_came_late() {
    let mut p = ready();
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"b","title":"bash","kind":"execute","status":"pending","rawInput":{"cwd":"/w"}}}}"#).events;
    assert!(matches!(&ev[0], AgentEvent::ToolCallStarted { input, .. } if input["cwd"] == "/w"));
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"b","status":"in_progress","title":"ls","rawInput":{"command":"ls","cwd":"/w"}}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::ToolCallUpdated { input: Some(i), name, .. } if i["command"] == "ls" && name == "bash")
    );
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"b","status":"in_progress","rawInput":{}}}}"#).events;
    assert!(matches!(
        &ev[0],
        AgentEvent::ToolCallUpdated { input: None, .. }
    ));
}

#[test]
fn prompt_blocks_follow_the_agents_prompt_capabilities() {
    use crate::protocol::prompt_blocks;
    use wasit_core::Attachment;
    let img = Attachment {
        name: "a.png".into(),
        mime: "image/png".into(),
        size: 3,
        path: "/w/a b.png".into(),
        data: Some("AAEC".into()),
    };
    let txt = Attachment {
        name: "n.txt".into(),
        mime: "text/plain".into(),
        size: 2,
        path: "/w/n.txt".into(),
        data: Some(wasit_core::content::b64_encode(b"hi")),
    };
    let bin = Attachment {
        name: "x.bin".into(),
        mime: "application/octet-stream".into(),
        size: 9,
        path: "/w/x.bin".into(),
        data: None,
    };
    let all = [img.clone(), txt.clone(), bin.clone()];
    let b = prompt_blocks("look", &all, true, true);
    assert_eq!(b[0], json!({"type":"text","text":"look"}));
    assert_eq!(b[1]["type"], "image");
    assert_eq!(b[1]["data"], "AAEC");
    assert_eq!(b[1]["uri"], "file:///w/a%20b.png");
    assert_eq!(b[2]["type"], "resource");
    assert_eq!(b[2]["resource"]["text"], "hi");
    assert_eq!(b[3]["type"], "resource_link");
    assert_eq!(b[3]["name"], "x.bin");
    // An agent without image / embedded-context support gets links only.
    let b = prompt_blocks("look", &all, false, false);
    assert!(b[1..].iter().all(|x| x["type"] == "resource_link"), "{b:?}");
}

#[test]
fn agent_and_tool_media_become_parts() {
    use wasit_core::{ContentPart, MediaSource};
    let mut p = ready();
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","messageId":"m","content":{"type":"resource_link","uri":"file:///w/out.pdf","name":"out.pdf","mimeType":"application/pdf"}}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::AssistantContent {
            parts: vec![ContentPart::File {
                source: MediaSource::Path {
                    path: "/w/out.pdf".into()
                },
                name: Some("out.pdf".into()),
                mime: Some("application/pdf".into()),
                uri: Some("file:///w/out.pdf".into()),
            }],
            message_id: Some("m".into()),
        }]
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
fn interrupt_drops_the_prompts_queued_behind_the_turn() {
    let mut p = ready();
    let step = sent(p.encode(InputId(10), Command::prompt("go")));
    assert!(step
        .events
        .contains(&AgentEvent::turn_started(None, Some(InputId(10)))));
    assert_eq!(
        p.encode(InputId(11), Command::prompt("a")).unwrap(),
        Encoded::Queued
    );
    assert_eq!(
        p.encode(
            InputId(12),
            Command::RunSlash {
                name: "review".into(),
                args: String::new()
            }
        )
        .unwrap(),
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
    assert_eq!(w(&step)[0]["method"], "session/cancel");
}

#[test]
fn interrupt_before_ready_and_shutdown_drop_what_is_queued() {
    let mut p = AcpProtocol::new("x");
    p.start(&SpawnSpec::default());
    p.encode(InputId(2), Command::prompt("a")).unwrap();
    p.encode(
        InputId(3),
        Command::SetMode {
            mode: "plan".into(),
        },
    )
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

/// A `session/update` of session `ses_1` (the one `ready()` opens).
fn update(body: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"ses_1","update":{body}}}}}"#
    )
}

fn chunk(text: &str) -> String {
    update(&format!(
        r#"{{"sessionUpdate":"agent_message_chunk","messageId":"m","content":{{"type":"text","text":"{text}"}}}}"#
    ))
}

fn prompt_id(step: &Step) -> u64 {
    w(step)
        .iter()
        .find(|l| l["method"] == "session/prompt")
        .expect("session/prompt written")["id"]
        .as_u64()
        .unwrap()
}

fn prompt_result(id: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{{"stopReason":"end_turn"}}}}"#)
}

#[test]
fn the_first_sign_of_the_agent_working_acknowledges_the_prompt_once() {
    let mut p = ready();
    let step = sent(p.encode(InputId(20), Command::prompt("go")));
    let id = prompt_id(&step);
    // Writing it acknowledges nothing.
    assert!(acks(&step.events).is_empty());
    let ev = p.on_line(&chunk("Hi")).events;
    assert_eq!(
        ev,
        vec![
            AgentEvent::InputAcknowledged {
                input: InputId(20),
                steered: false
            },
            AgentEvent::AssistantDelta {
                text: "Hi".into(),
                message_id: Some("m".into())
            }
        ]
    );
    // More activity and the result add nothing.
    assert!(acks(&p.on_line(&chunk(" there")).events).is_empty());
    let ev = p.on_line(&prompt_result(id)).events;
    assert!(acks(&ev).is_empty());
    assert!(matches!(&ev[0], AgentEvent::TurnCompleted { .. }));
}

#[test]
fn every_kind_of_agent_work_acknowledges() {
    for body in [
        r#"{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"hm"}}"#,
        r#"{"sessionUpdate":"tool_call","toolCallId":"c1","title":"bash","kind":"execute","status":"pending"}"#,
        r#"{"sessionUpdate":"tool_call_update","toolCallId":"c1","status":"in_progress"}"#,
        r#"{"sessionUpdate":"plan","entries":[]}"#,
    ] {
        let mut p = ready();
        sent(p.encode(InputId(21), Command::prompt("go")));
        let ev = p.on_line(&update(body)).events;
        assert_eq!(acks(&ev), vec![(InputId(21), false)], "{body}");
    }
}

#[test]
fn the_result_acknowledges_a_prompt_that_streamed_nothing() {
    let mut p = ready();
    let step = sent(p.encode(InputId(22), Command::prompt("go")));
    let id = prompt_id(&step);
    let ev = p.on_line(&prompt_result(id)).events;
    // Acknowledged first, then the turn ends.
    assert_eq!(
        ev[0],
        AgentEvent::InputAcknowledged {
            input: InputId(22),
            steered: false
        }
    );
    assert!(matches!(&ev[1], AgentEvent::TurnCompleted { .. }));
    assert_eq!(acks(&ev).len(), 1);
}

#[test]
fn updates_that_are_not_the_agent_working_on_the_prompt_do_not_acknowledge() {
    let mut p = ready();
    sent(p.encode(InputId(23), Command::prompt("go")));
    // What the recordings show between `session/prompt` and the first real activity
    // (`available_commands_update`, `config_option_update`), usage that precedes an error, a
    // replayed user message, and activity of another session.
    for body in [
        r#"{"sessionUpdate":"available_commands_update","availableCommands":[]}"#,
        r#"{"sessionUpdate":"config_option_update","configOptions":[]}"#,
        r#"{"sessionUpdate":"current_mode_update","currentModeId":"plan"}"#,
        r#"{"sessionUpdate":"usage_update","used":1,"size":10}"#,
        r#"{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"go"}}"#,
    ] {
        let ev = p.on_line(&update(body)).events;
        assert!(acks(&ev).is_empty(), "{body}: {ev:?}");
    }
    let other = r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"ses_child","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"x"}}}}"#;
    assert!(acks(&p.on_line(other).events).is_empty());
    // An update with no session id is not known to be ours.
    let anon = r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"x"}}}}"#;
    assert!(acks(&p.on_line(anon).events).is_empty());
    // The real thing does.
    assert_eq!(
        acks(&p.on_line(&chunk("Hi")).events),
        vec![(InputId(23), false)]
    );
}

#[test]
fn history_replayed_by_session_load_is_never_an_acknowledgement() {
    let mut p = AcpProtocol::new("x");
    let (spec, first) = SpawnSpec {
        resume: ResumeMode::Resume("ses_old".into()),
        first_prompt: Some("go".into()),
        ..SpawnSpec::default()
    }
    .split_first_prompt();
    p.start(&spec);
    assert_eq!(
        p.encode(InputId(1), first.unwrap()).unwrap(),
        Encoded::Queued
    );
    p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
    // The replay of the stored conversation, as `reference/*/load.jsonl` and
    // `fixtures/resume.ndjson` show it: every kind of message, for the loaded session.
    let replay = |body: &str| {
        format!(
            r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"ses_old","update":{body}}}}}"#
        )
    };
    let mut replayed = Vec::new();
    for body in [
        r#"{"sessionUpdate":"user_message_chunk","messageId":"m1","content":{"type":"text","text":"old "}}"#,
        r#"{"sessionUpdate":"user_message_chunk","messageId":"m1","content":{"type":"text","text":"question"}}"#,
        r#"{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"old answer"}}"#,
        r#"{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"old thought"}}"#,
        r#"{"sessionUpdate":"tool_call","toolCallId":"c0","title":"bash","status":"pending"}"#,
        r#"{"sessionUpdate":"tool_call_update","toolCallId":"c0","status":"completed"}"#,
        r#"{"sessionUpdate":"usage_update","used":10,"size":100,"cost":{"amount":2.5,"currency":"USD"}}"#,
        r#"{"sessionUpdate":"plan","entries":[{"content":"x","priority":"high","status":"pending"}]}"#,
    ] {
        let step = p.on_line(&replay(body));
        assert!(acks(&step.events).is_empty(), "{body}");
        replayed.extend(step.events);
    }
    // All of it is history (the user message in one piece); none of it usage or plan.
    assert!(
        replayed.iter().all(|e| e.as_history().is_some()),
        "{replayed:?}"
    );
    let inner: Vec<&AgentEvent> = replayed
        .iter()
        .filter_map(|e| e.as_history().map(|(_, e)| e))
        .collect();
    assert_eq!(
        inner[0],
        &AgentEvent::UserMessage {
            text: "old question".into()
        }
    );
    assert!(matches!(inner[1], AgentEvent::AssistantDelta { text, .. } if text == "old answer"));
    assert!(matches!(inner[2], AgentEvent::ThinkingDelta { .. }));
    assert!(matches!(inner[3], AgentEvent::ToolCallStarted { name, .. } if name == "bash"));
    assert!(matches!(
        inner[4],
        AgentEvent::ToolCallFinished { ok: true, .. }
    ));
    assert_eq!(inner.len(), 5, "{inner:?}");
    // The load answers; the queued prompt goes out. Still nothing acknowledged.
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#);
    assert!(acks(&step.events).is_empty());
    assert!(step
        .events
        .contains(&AgentEvent::InputSent { input: InputId(1) }));
    let id = prompt_id(&step);
    // Only the agent's work on the new prompt acknowledges it.
    let ev = p
        .on_line(&replay(
            r#"{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"new"}}"#,
        ))
        .events;
    assert_eq!(acks(&ev), vec![(InputId(1), false)]);
    assert!(acks(&p.on_line(&prompt_result(id)).events).is_empty());
}

#[test]
fn each_prompt_is_acknowledged_once_for_its_own_input() {
    let mut p = ready();
    let first = sent(p.encode(InputId(30), Command::prompt("a")));
    let first_id = prompt_id(&first);
    assert_eq!(
        p.encode(InputId(31), Command::prompt("b")).unwrap(),
        Encoded::Queued
    );
    let mut all = acks(&p.on_line(&chunk("A")).events);
    let step = p.on_line(&prompt_result(first_id));
    all.extend(acks(&step.events));
    // The held prompt went out with the end of the turn; session-level noise does not count.
    let second_id = prompt_id(&step);
    all.extend(acks(
        &p.on_line(&update(
            r#"{"sessionUpdate":"available_commands_update","availableCommands":[]}"#,
        ))
        .events,
    ));
    assert_eq!(all, vec![(InputId(30), false)]);
    all.extend(acks(&p.on_line(&prompt_result(second_id)).events));
    assert_eq!(all, vec![(InputId(30), false), (InputId(31), false)]);
}

#[test]
fn a_refused_prompt_is_reported_dropped_and_ends_the_turn() {
    let mut p = ready();
    let step = sent(p.encode(InputId(40), Command::prompt("go")));
    let id = prompt_id(&step);
    assert_eq!(
        p.encode(InputId(41), Command::prompt("next")).unwrap(),
        Encoded::Queued
    );
    // What kilo 7.8.8 / 7.8.27 do without credentials: session-level updates and a usage update,
    // then this error (`fixtures/kilo-refused.ndjson`, `reference/kilo/error.jsonl`).
    p.on_line(&update(
        r#"{"sessionUpdate":"available_commands_update","availableCommands":[]}"#,
    ));
    p.on_line(&update(
        r#"{"sessionUpdate":"usage_update","used":0,"size":10}"#,
    ));
    let step = p.on_line(&format!(
        r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32603,"message":"Internal error: You need to sign in to use this model."}}}}"#
    ));
    assert_eq!(
        refusals(&step.events),
        vec![(
            InputId(40),
            "Internal error: You need to sign in to use this model.".to_string()
        )]
    );
    assert!(acks(&step.events).is_empty());
    // The turn ends as before, after the refusal.
    let dropped = step
        .events
        .iter()
        .position(|e| matches!(e, AgentEvent::InputDropped { .. }))
        .unwrap();
    let ended = step
        .events
        .iter()
        .position(|e| {
            matches!(
                e,
                AgentEvent::TurnCompleted {
                    end_reason: Some(EndReason::ExecutionError),
                    reply: Some(r),
                    ..
                } if r.contains("sign in")
            )
        })
        .unwrap();
    assert!(dropped < ended);
    // The held prompt follows and is acknowledged on its own.
    assert!(step
        .events
        .contains(&AgentEvent::InputSent { input: InputId(41) }));
    let next_id = prompt_id(&step);
    assert_eq!(
        acks(&p.on_line(&prompt_result(next_id)).events),
        vec![(InputId(41), false)]
    );
}

#[test]
fn an_error_after_the_agent_worked_is_not_a_refusal() {
    let mut p = ready();
    let step = sent(p.encode(InputId(50), Command::prompt("go")));
    let id = prompt_id(&step);
    assert_eq!(acks(&p.on_line(&chunk("partial")).events).len(), 1);
    let step = p.on_line(&format!(
        r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32603,"message":"Internal error: provider down"}}}}"#
    ));
    assert!(refusals(&step.events).is_empty(), "{:?}", step.events);
    assert!(acks(&step.events).is_empty());
    assert!(matches!(
        &step.events[0],
        AgentEvent::TurnCompleted {
            end_reason: Some(EndReason::ExecutionError),
            ..
        }
    ));
}

#[test]
fn queued_prompts_dropped_by_an_interrupt_are_never_acknowledged() {
    let mut p = ready();
    let step = sent(p.encode(InputId(60), Command::prompt("go")));
    let id = prompt_id(&step);
    p.encode(InputId(61), Command::prompt("a")).unwrap();
    p.encode(InputId(62), Command::prompt("b")).unwrap();
    let step = sent(p.encode(InputId(63), Command::Interrupt));
    assert_eq!(
        drops(&step.events),
        vec![
            (InputId(61), InputDropReason::Interrupted),
            (InputId(62), InputDropReason::Interrupted)
        ]
    );
    let mut after = p.on_line(&chunk("tail")).events;
    after.extend(
        p.on_line(&format!(
            r#"{{"jsonrpc":"2.0","id":{id},"result":{{"stopReason":"cancelled"}}}}"#
        ))
        .events,
    );
    assert_eq!(acks(&after), vec![(InputId(60), false)]);
    assert!(drops(&after).is_empty());
}

// ── earlier history of a loaded session ─────────────────────────────────────────

fn loading(spec: SpawnSpec) -> AcpProtocol {
    let mut p = AcpProtocol::new("x");
    p.start(&SpawnSpec {
        resume: ResumeMode::Resume("ses_old".into()),
        ..spec
    });
    p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
    p
}

fn replayed(body: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"ses_old","update":{body}}}}}"#
    )
}

#[test]
fn a_replayed_user_message_is_one_event_flushed_by_the_next_message_or_the_load_result() {
    let mut p = loading(SpawnSpec::default());
    let user = |id: &str, text: &str| {
        replayed(&format!(
            r#"{{"sessionUpdate":"user_message_chunk","messageId":"{id}","content":{{"type":"text","text":"{text}"}}}}"#
        ))
    };
    assert!(p.on_line(&user("m1", "a")).events.is_empty());
    assert!(p.on_line(&user("m1", "b")).events.is_empty());
    // A different message: the first is complete.
    let ev = p.on_line(&user("m2", "c")).events;
    assert_eq!(
        ev,
        vec![AgentEvent::UserMessage { text: "ab".into() }.earlier(None)]
    );
    // The load result flushes the last one, ahead of SessionInit.
    let ev = p
        .on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"configOptions":[]}}"#)
        .events;
    assert_eq!(
        ev[0],
        AgentEvent::UserMessage { text: "c".into() }.earlier(None)
    );
    assert!(matches!(ev[1], AgentEvent::SessionInit { .. }));
    // An image in a replayed user message is history content next to the text.
    let mut p = loading(SpawnSpec::default());
    let ev = p
        .on_line(&replayed(
            r#"{"sessionUpdate":"user_message_chunk","messageId":"m","content":{"type":"image","mimeType":"image/png","uri":"file:///w/a.png"}}"#,
        ))
        .events;
    assert!(matches!(
        ev.as_slice(),
        [AgentEvent::History { event, .. }] if matches!(**event, AgentEvent::UserContent { .. })
    ));
}

/// Nothing replayed leaves state behind: a call the history left open, a subagent it announced.
#[test]
fn replayed_calls_leave_no_state_behind() {
    let mut p = loading(SpawnSpec::default());
    for body in [
        r#"{"sessionUpdate":"tool_call","toolCallId":"c0","title":"replayed-name","status":"in_progress"}"#,
        r#"{"sessionUpdate":"tool_call","toolCallId":"t0","title":"task","kind":"other","status":"pending"}"#,
        r#"{"sessionUpdate":"tool_call_update","toolCallId":"t0","status":"in_progress","rawInput":{"description":"sub"}}"#,
    ] {
        p.on_line(&replayed(body));
    }
    p.on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_old","configOptions":[]}}"#);
    // The same ids, live, are strangers: names come from the update itself and the subagent
    // is not remembered.
    let live = |body: &str| {
        format!(
            r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"ses_old","update":{body}}}}}"#
        )
    };
    let ev = p
        .on_line(&live(
            r#"{"sessionUpdate":"tool_call_update","toolCallId":"c0","title":"live-name","status":"completed"}"#,
        ))
        .events;
    assert!(
        matches!(&ev[..], [AgentEvent::ToolCallFinished { name, .. }] if name == "live-name"),
        "{ev:?}"
    );
    let ev = p
        .on_line(&live(
            r#"{"sessionUpdate":"tool_call_update","toolCallId":"t0","status":"completed"}"#,
        ))
        .events;
    assert!(
        matches!(&ev[..], [AgentEvent::ToolCallFinished { name, .. }] if name != "subagent"),
        "{ev:?}"
    );
}

/// A live `usage_update` for the session, as kilo sends it before each prompt's result.
fn cost_update(p: &mut AcpProtocol, update: &str) {
    p.on_line(&format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"update":{update}}}}}"#
    ));
}

/// Sends `text` as input `input`, answers it with `answer` (a full JSON-RPC response body with
/// `ID` standing for the request id), and returns the cost of the `TurnCompleted` it produced.
fn finish_turn(p: &mut AcpProtocol, input: u64, text: &str, answer: &str) -> f64 {
    let step = sent(p.encode(InputId(input), Command::prompt(text)));
    let id = w(&step)[0]["id"].clone();
    let step = p.on_line(&answer.replace("ID", &id.to_string()));
    step.events
        .iter()
        .find_map(|e| match e {
            AgentEvent::TurnCompleted { usage, .. } => Some(usage.cost_usd),
            _ => None,
        })
        .expect("a TurnCompleted")
}

const END_TURN: &str = r#"{"jsonrpc":"2.0","id":ID,"result":{"stopReason":"end_turn","usage":{"inputTokens":10,"outputTokens":2}}}"#;

/// The session cost reported by `usage_update` reaches the turn's `TurnCompleted`: the prompt
/// result carries no cost of its own.
#[test]
fn usage_update_cost_reaches_turn_completed() {
    let mut p = ready();
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    assert_eq!(finish_turn(&mut p, 1, "a", END_TURN), 0.25);
}

/// The reported cost is cumulative for the session (kilo's `usage.ts` sums every assistant
/// message's cost), so a second turn reports the new total, not the turn's own share, and a
/// turn with no update of its own still reports the total.
#[test]
fn two_turns_report_the_cumulative_session_cost() {
    let mut p = ready();
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    assert_eq!(finish_turn(&mut p, 1, "a", END_TURN), 0.25);
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":80,"size":200,"cost":{"amount":0.4,"currency":"USD"}}"#,
    );
    assert_eq!(finish_turn(&mut p, 2, "b", END_TURN), 0.4);
    assert_eq!(finish_turn(&mut p, 3, "c", END_TURN), 0.4);
}

/// A zero, absent or non-USD cost does not erase the cost already known for the session.
#[test]
fn zero_or_absent_cost_keeps_the_known_cost() {
    let mut p = ready();
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    assert_eq!(finish_turn(&mut p, 1, "a", END_TURN), 0.25);
    for update in [
        r#"{"sessionUpdate":"usage_update","used":60,"size":200,"cost":{"amount":0,"currency":"USD"}}"#,
        r#"{"sessionUpdate":"usage_update","used":60,"size":200}"#,
        r#"{"sessionUpdate":"usage_update","used":60,"size":200,"cost":{"amount":9,"currency":"EUR"}}"#,
    ] {
        cost_update(&mut p, update);
        assert_eq!(finish_turn(&mut p, 2, "b", END_TURN), 0.25, "{update}");
    }
}

/// A turn that fails still reports the session's known cost, not zero.
#[test]
fn failed_turn_keeps_the_known_cost() {
    let mut p = ready();
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    let failed = r#"{"jsonrpc":"2.0","id":ID,"error":{"code":-32603,"message":"boom"}}"#;
    assert_eq!(finish_turn(&mut p, 1, "a", failed), 0.25);
}

/// A replayed usage report and plan are the past's: they are not shown, and cost nothing.
#[test]
fn replayed_usage_and_plan_are_not_shown() {
    let mut p = loading(SpawnSpec::default());
    for body in [
        r#"{"sessionUpdate":"usage_update","used":10,"size":100,"cost":{"amount":2.5,"currency":"USD"}}"#,
        r#"{"sessionUpdate":"plan","entries":[{"content":"x","priority":"high","status":"pending"}]}"#,
    ] {
        assert!(p.on_line(&replayed(body)).events.is_empty(), "{body}");
    }
}

/// A session list whose titles are megabytes long lists titles of at most `TITLE_CHARS`
/// characters ending with the cut marker. (Synthetic text only.)
#[test]
fn a_huge_session_title_is_bounded() {
    use wasit_core::{limits, CUT};
    let list = json!([
        {"id":"ses_1","title":"ü".repeat(1024 * 1024),"updated":5,"directory":"/p"},
        {"id":"ses_2","title":"short","updated":4,"directory":"/p"},
    ]);
    let out = ProbeOutput::Exec {
        exit_code: Some(0),
        stdout: list.to_string(),
    };
    let Discovered::Sessions(s) = parse_sessions(&out).unwrap() else {
        panic!("not sessions");
    };
    assert_eq!(s[0].title.chars().count(), limits::TITLE_CHARS);
    assert!(s[0].title.ends_with(CUT));
    assert_eq!(s[1].title, "short");
}

#[test]
fn compact_and_run_shell_are_unsupported() {
    let mut p = ready();
    assert_eq!(
        p.encode(InputId(130), Command::Compact),
        Err(DriverError::Unsupported {
            operation: "compact".into()
        })
    );
    assert_eq!(
        p.encode(
            InputId(131),
            Command::RunShell {
                line: "ls".into(),
                timeout_ms: None
            }
        ),
        Err(DriverError::Unsupported {
            operation: "run_shell".into()
        })
    );
}

/// An ACP `diff` content block is a diff part of the call's result; a failed call keeps none.
#[test]
fn acp_diff_blocks_become_diff_parts_of_the_result() {
    let mut p = ready();
    p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"d","title":"edit","kind":"edit","status":"pending","rawInput":{}}}}"#);
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"d","status":"completed","content":[{"type":"diff","path":"/w/a.rs","oldText":"x","newText":"y"},{"type":"diff","path":"/w/n.txt","oldText":null,"newText":"new\n"}]}}}"#).events;
    assert_eq!(
        ev,
        vec![AgentEvent::ToolCallFinished {
            call_id: "d".into(),
            name: "edit".into(),
            ok: true,
            result: Some(json!([
                {"type":"diff","path":"/w/a.rs","oldText":"x","newText":"y"},
                {"type":"diff","path":"/w/n.txt","oldText":null,"newText":"new\n"}
            ])),
            parts: vec![
                ContentPart::Diff {
                    path: "/w/a.rs".into(),
                    old_text: Some("x".into()),
                    new_text: "y".into(),
                    hunk: None,
                },
                ContentPart::Diff {
                    path: "/w/n.txt".into(),
                    old_text: None,
                    new_text: "new\n".into(),
                    hunk: None,
                },
            ],
        }]
    );
    p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"f","title":"edit","kind":"edit","status":"pending","rawInput":{}}}}"#);
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"f","status":"failed","content":[{"type":"diff","path":"/w/a.rs","oldText":"x","newText":"y"}]}}}"#).events;
    assert!(
        matches!(&ev[0], AgentEvent::ToolCallFinished { ok: false, parts, .. } if parts.is_empty())
    );
}

/// The `usage` and `turn_usage` of the `TurnCompleted` of a finished prompt.
fn finished(p: &mut AcpProtocol, input: u64, text: &str, answer: &str) -> (Usage, Option<Usage>) {
    let step = sent(p.encode(InputId(input), Command::prompt(text)));
    let id = w(&step)[0]["id"].clone();
    let step = p.on_line(&answer.replace("ID", &id.to_string()));
    match step
        .events
        .iter()
        .find(|e| matches!(e, AgentEvent::TurnCompleted { .. }))
    {
        Some(AgentEvent::TurnCompleted {
            usage, turn_usage, ..
        }) => (usage.clone(), turn_usage.clone()),
        _ => panic!("no TurnCompleted in {:?}", step.events),
    }
}

/// A prompt result with its own per-prompt tokens (kilo's `usage` of a prompt result is that
/// prompt's, not a running total).
fn prompt_tokens_result(input: u64, output: u64, cached: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":ID,"result":{{"stopReason":"end_turn","usage":{{"inputTokens":{input},"outputTokens":{output},"cachedReadTokens":{cached}}}}}}}"#
    )
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

/// Each prompt's tokens are its own and its cost is what the session cost grew by during it;
/// the cumulative `usage` is the sum of the prompts' tokens and the session's cost.
#[test]
fn turn_usage_is_each_prompts_own_and_sums_to_the_cumulative_usage() {
    let mut p = ready();
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    let (u1, t1) = finished(&mut p, 1, "a", &prompt_tokens_result(10, 2, 5));
    assert_eq!(
        (u1.input_tokens, u1.output_tokens, u1.cache_read_tokens),
        (10, 2, 5)
    );
    let t1 = t1.expect("turn 1 usage");
    assert_eq!(
        (t1.input_tokens, t1.output_tokens, t1.cache_read_tokens),
        (10, 2, 5)
    );
    assert!(close(t1.cost_usd, 0.25));

    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":80,"size":200,"cost":{"amount":0.4,"currency":"USD"}}"#,
    );
    let (u2, t2) = finished(&mut p, 2, "b", &prompt_tokens_result(20, 3, 7));
    let t2 = t2.expect("turn 2 usage");
    assert_eq!(
        (t2.input_tokens, t2.output_tokens, t2.cache_read_tokens),
        (20, 3, 7)
    );
    assert!(close(t2.cost_usd, 0.15), "{}", t2.cost_usd);
    assert_eq!(
        (u2.input_tokens, u2.output_tokens, u2.cache_read_tokens),
        (30, 5, 12)
    );
    assert!(close(u2.cost_usd, 0.4));
    assert_eq!(t1.input_tokens + t2.input_tokens, u2.input_tokens);
    assert_eq!(t1.output_tokens + t2.output_tokens, u2.output_tokens);
    assert!(close(t1.cost_usd + t2.cost_usd, u2.cost_usd));
}

/// A loaded session's cost before its first turn is not known from here, so that turn's cost,
/// and with it its usage, is unknown. The turn after it is counted from the cost then known.
#[test]
fn a_loaded_session_has_no_turn_usage_until_its_cost_baseline_is_known() {
    let mut p = loading(SpawnSpec::default());
    p.on_line(r#"{"jsonrpc":"2.0","id":2,"result":{"sessionId":"ses_old","configOptions":[]}}"#);
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    let (u1, t1) = finished(&mut p, 1, "a", &prompt_tokens_result(10, 2, 5));
    assert_eq!(t1, None);
    assert!(close(u1.cost_usd, 0.25));
    assert_eq!(u1.input_tokens, 10);

    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":80,"size":200,"cost":{"amount":0.4,"currency":"USD"}}"#,
    );
    let (_, t2) = finished(&mut p, 2, "b", &prompt_tokens_result(20, 3, 7));
    let t2 = t2.expect("counted from the known cost");
    assert_eq!(t2.input_tokens, 20);
    assert!(close(t2.cost_usd, 0.15));
}

/// A new session has spent nothing: its first turn's cost is what the session reported, and a
/// turn whose cost is never reported is free (0.0), not unknown.
#[test]
fn a_new_session_and_an_unpriced_turn_have_known_turn_usage() {
    let mut p = ready();
    let (_, t) = finished(&mut p, 1, "a", &prompt_tokens_result(10, 2, 5));
    let t = t.expect("no cost reported: 0.0 is the answer");
    assert_eq!(t.cost_usd, 0.0);
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.25,"currency":"USD"}}"#,
    );
    let (_, t) = finished(&mut p, 2, "b", &prompt_tokens_result(4, 1, 0));
    assert!(close(t.expect("known").cost_usd, 0.25));
}

/// No usage in the prompt result means no turn usage (the tokens are not known), and the
/// session's cumulative tokens do not move; the next turn still reports its own tokens.
#[test]
fn a_prompt_result_without_usage_has_no_turn_usage() {
    let mut p = ready();
    let (u, t) = finished(
        &mut p,
        1,
        "a",
        r#"{"jsonrpc":"2.0","id":ID,"result":{"stopReason":"end_turn"}}"#,
    );
    assert_eq!(t, None);
    assert_eq!(u.input_tokens, 0);
    let (u, t) = finished(&mut p, 2, "b", &prompt_tokens_result(4, 1, 0));
    assert_eq!(t.expect("own tokens").input_tokens, 4);
    assert_eq!(u.input_tokens, 4);
}

/// A session cost that falls is not a usage: the turn's cost is unknown, not negative.
#[test]
fn a_falling_session_cost_gives_no_negative_turn_usage() {
    let mut p = ready();
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":50,"size":200,"cost":{"amount":0.5,"currency":"USD"}}"#,
    );
    finished(&mut p, 1, "a", &prompt_tokens_result(10, 2, 0));
    cost_update(
        &mut p,
        r#"{"sessionUpdate":"usage_update","used":60,"size":200,"cost":{"amount":0.3,"currency":"USD"}}"#,
    );
    let (_, t) = finished(&mut p, 2, "b", &prompt_tokens_result(10, 2, 0));
    assert_eq!(t, None);
}
