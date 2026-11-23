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
            AgentEvent::TurnStarted {
                native_turn_id: None,
                input: Some(InputId(1))
            }
        ]
    );
}

#[test]
fn resume_loads_and_suppresses_history_replay() {
    let mut p = AcpProtocol::new("x");
    p.start(&SpawnSpec {
        resume: ResumeMode::Resume("ses_old".into()),
        ..SpawnSpec::default()
    });
    let step = p.on_line(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
    assert_eq!(w(&step)[0]["method"], "session/load");
    assert_eq!(w(&step)[0]["params"]["sessionId"], "ses_old");
    let ev = p.on_line(r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"old"}}}}"#).events;
    assert!(ev.is_empty());
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
        assert_eq!(d.argv(&spec), [flavor.binary(), "acp"]);
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
    assert!(step.events.contains(&AgentEvent::TurnStarted {
        native_turn_id: None,
        input: Some(InputId(10))
    }));
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
    for body in [
        r#"{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"old question"}}"#,
        r#"{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"old answer"}}"#,
        r#"{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"old thought"}}"#,
        r#"{"sessionUpdate":"tool_call","toolCallId":"c0","title":"bash","status":"pending"}"#,
        r#"{"sessionUpdate":"tool_call_update","toolCallId":"c0","status":"completed"}"#,
        r#"{"sessionUpdate":"plan","entries":[]}"#,
    ] {
        let step = p.on_line(&replay(body));
        assert!(step.events.is_empty(), "{body}: {:?}", step.events);
    }
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
