//! wasit-codex — the Codex driver (`codex app-server --stdio`, JSON-RPC).
//!
//! - [`descriptor()`] — the [`DriverDescriptor`] for id `"codex"`.
//! - [`CodexProtocol`] — the pure protocol state machine (see its module docs
//!   for the verified wire facts).
//!
//! Launch: `codex app-server --stdio <rendered -c overrides>`. Model, cwd and
//! approval policy are per-thread protocol parameters, not flags. The
//! process needs its stdin kept open (the runtime owns it).

#![forbid(unsafe_code)]

mod protocol;

use std::sync::Arc;

use serde_json::Value;
use wasit_core::{
    config_listing, exec_stdout, listed_files, models_from_handshake, modes_from_handshake,
    parse_named_version, skills_from_listing, AgentConfig, BusyPrompt, ConfigItem, ConfigItemKind,
    ConfigRenderer, ConfigSource, Discovered, DiscoveryContext, DiscoveryKind, DiscoveryProbe,
    DriverCapabilities, DriverDescriptor, DriverId, FileInput, McpServerConfig, McpTransport,
    ProbeAction, ProbeOutput, PromptInput, Protocol, ResumableSession, SavesConversation,
    SpawnSpec, Unsupported,
};

pub use protocol::{CodexProtocol, SKILL_ROOTS_OPTION};

/// The driver id.
pub const ID: &str = "codex";

/// The Codex descriptor.
#[derive(Debug, Clone, Copy, Default)]
pub struct CodexDescriptor;

/// The Codex descriptor, shareable.
pub fn descriptor() -> Arc<dyn DriverDescriptor> {
    Arc::new(CodexDescriptor)
}

impl DriverDescriptor for CodexDescriptor {
    fn id(&self) -> DriverId {
        DriverId::new(ID)
    }

    fn display_name(&self) -> &str {
        "Codex"
    }

    fn default_binary(&self) -> &str {
        "codex"
    }

    fn argv(&self, spec: &SpawnSpec) -> Vec<String> {
        let mut argv = vec![
            spec.binary_or(self.default_binary()).to_string(),
            "app-server".to_string(),
            "--stdio".to_string(),
        ];
        argv.extend(spec.extra_args.iter().cloned());
        argv
    }

    fn env(&self, spec: &SpawnSpec) -> Vec<(String, String)> {
        spec.env.clone()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            interrupt: true,
            set_model: true,
            set_mode: true,
            slash_commands: false,
            permission_gates: true,
            questions: true,
            plan_gate: false,
            usage: true,
            cost: false,
            resume: true,
            continue_latest: true,
            streaming_output: true,
            prompt_while_busy: BusyPrompt::Steered,
            stop_task: false,
        }
    }

    /// From the app-server schema (codex 0.169): `UserInput` has `localImage {path}`; other
    /// files are named by path. Images are uploaded to the session's host first.
    fn prompt_input(&self) -> PromptInput {
        PromptInput {
            embeds_bytes: false,
            ..PromptInput::images_and(FileInput::PathReference, 20 * 1024 * 1024)
        }
    }

    /// Verified with codex-cli 0.161.0: `thread/start` reports a thread id at once but writes no
    /// rollout until a turn runs, and `thread/resume` of such a thread fails with "no rollout
    /// found for thread id". (`thread/start` has no parameter for a caller-chosen id, so a
    /// thread cannot be started again under its id either.)
    fn saves_conversation(&self) -> SavesConversation {
        SavesConversation::OnFirstTurn
    }

    /// `codex resume <id>` (codex-cli 0.161.0: "Session id (UUID) or session name").
    fn user_resume_argv(&self, native_session_id: &str) -> Option<Vec<String>> {
        Some(vec![
            self.default_binary().to_string(),
            "resume".to_string(),
            native_session_id.to_string(),
        ])
    }

    fn new_protocol(&self) -> Box<dyn Protocol> {
        Box::new(CodexProtocol::new())
    }

    fn discovery(&self, ctx: &DiscoveryContext) -> Vec<DiscoveryProbe> {
        let bin = ctx
            .binary
            .clone()
            .unwrap_or_else(|| self.default_binary().to_string());
        let handshake = Box::new(SpawnSpec {
            bin: Some(bin.clone()),
            cwd: ctx.project.clone(),
            ..SpawnSpec::default()
        });
        let mut probes = vec![
            DiscoveryProbe {
                kind: DiscoveryKind::Version,
                action: ProbeAction::Exec {
                    argv: vec![bin, "--version".into()],
                    env: vec![],
                    cwd: None,
                },
                parse: std::sync::Arc::new(parse_named_version("codex-cli")),
            },
            DiscoveryProbe {
                kind: DiscoveryKind::Models,
                action: ProbeAction::Handshake {
                    spec: handshake.clone(),
                    until: DiscoveryKind::Models,
                },
                parse: std::sync::Arc::new(models_from_handshake),
            },
            DiscoveryProbe {
                kind: DiscoveryKind::Modes,
                action: ProbeAction::Handshake {
                    spec: handshake,
                    until: DiscoveryKind::Modes,
                },
                parse: std::sync::Arc::new(modes_from_handshake),
            },
        ];
        let home = ctx
            .var("CODEX_HOME")
            .map(|h| h.trim_end_matches('/').to_string())
            .or_else(|| {
                ctx.home
                    .as_deref()
                    .map(|h| format!("{}/.codex", h.trim_end_matches('/')))
            });
        probes.extend(config_probes(
            ctx.binary.as_deref().unwrap_or(self.default_binary()),
            home.as_deref(),
            ctx.project.clone(),
        ));
        if let Some(home) = home {
            probes.push(DiscoveryProbe {
                kind: DiscoveryKind::Sessions,
                action: ProbeAction::ListFiles {
                    dir: format!("{home}/sessions"),
                    suffix: ".jsonl".into(),
                    recursive: true,
                    max_files: 200,
                    head_lines: 40,
                    count_containing: vec![MESSAGE_RECORD.to_string()],
                },
                parse: std::sync::Arc::new(parse_sessions),
            });
        }
        probes
    }
}

/// Rollout records that are conversation messages (`response_item` payloads of type
/// `message`, any role). Their count is the session's message count.
pub const MESSAGE_RECORD: &str = "\"type\":\"message\"";

impl ConfigRenderer for CodexDescriptor {
    fn render_config(&self, config: &AgentConfig, spec: &mut SpawnSpec) -> Vec<Unsupported> {
        let mut unsupported = Vec::new();
        for server in &config.mcp_servers {
            match mcp_overrides(server) {
                Ok(args) => spec.extra_args.extend(args),
                Err(reason) => {
                    unsupported.push(Unsupported::new(format!("mcp:{}", server.name), reason))
                }
            }
        }
        if !config.skills.is_empty() {
            // Codex has no config key for extra skill roots (unknown fields
            // are rejected under --strict-config); the protocol registers
            // them with `skills/extraRoots/set` before opening the thread.
            let mut roots: Vec<Value> = Vec::new();
            for skill in &config.skills {
                let root = Value::String(skill.root.trim_end_matches('/').to_string());
                if !roots.contains(&root) {
                    roots.push(root);
                }
            }
            spec.protocol_options
                .insert(SKILL_ROOTS_OPTION.to_string(), Value::Array(roots));
        }
        for plugin in &config.plugins {
            unsupported.push(Unsupported::new(
                format!("plugin:{}", plugin.name),
                "codex only enables plugins installed from a marketplace; a local plugin path cannot be injected at launch",
            ));
        }
        if let Some(text) = config
            .extra_instructions
            .as_deref()
            .filter(|t| !t.is_empty())
        {
            spec.extra_args.extend([
                "-c".to_string(),
                format!("developer_instructions={}", toml_str(text)),
            ]);
        }
        if !config.tool_policy.disallowed_tools.is_empty() {
            unsupported.push(Unsupported::new(
                "tool_policy",
                "codex has no per-tool deny list; use its sandbox / approval policy",
            ));
        }
        render_disabled(config, spec, &mut unsupported);
        spec.env.extend(config.env.iter().cloned());
        unsupported
    }
}

/// Switch off inherited items with config overrides (verified against codex 0.169:
/// `-c mcp_servers.<name>.enabled=false` drops the server from `codex mcp list --json`, and
/// `-c skills.config=[{path="…/SKILL.md",enabled=false}]` removes the skill from the
/// model-visible prompt). Plugins, commands and agents have no verified off switch.
fn render_disabled(config: &AgentConfig, spec: &mut SpawnSpec, unsupported: &mut Vec<Unsupported>) {
    let mut skills: Vec<String> = Vec::new();
    for d in &config.disabled {
        match d.kind {
            ConfigItemKind::McpServer if toml_key_ok(&d.name) => spec.extra_args.extend([
                "-c".to_string(),
                format!("mcp_servers.{}.enabled=false", d.name),
            ]),
            ConfigItemKind::McpServer => unsupported.push(Unsupported::new(
                d.item_ref().key(),
                format!(
                    "server name {:?} must be [A-Za-z0-9_-]+ to be a codex config key",
                    d.name
                ),
            )),
            ConfigItemKind::Skill => match d.path.as_deref() {
                Some(p) => {
                    let file = if p.ends_with("SKILL.md") {
                        p.to_string()
                    } else {
                        format!("{}/SKILL.md", p.trim_end_matches('/'))
                    };
                    skills.push(format!("{{path={},enabled=false}}", toml_str(&file)));
                }
                None => unsupported.push(Unsupported::new(
                    d.item_ref().key(),
                    "codex disables a skill by its SKILL.md path, which is not known",
                )),
            },
            ConfigItemKind::Plugin | ConfigItemKind::Command | ConfigItemKind::Agent => unsupported
                .push(Unsupported::new(
                    d.item_ref().key(),
                    "codex has no verified launch-time switch for this item",
                )),
        }
    }
    if !skills.is_empty() {
        spec.extra_args.extend([
            "-c".to_string(),
            format!("skills.config=[{}]", skills.join(",")),
        ]);
    }
}

/// Probes for what codex itself has configured: `codex mcp list --json` (run in the project,
/// so project config applies) and the skill roots (`$CODEX_HOME/skills`, including the
/// bundled `.system` skills, and `<project>/.agents/skills`).
fn config_probes(bin: &str, home: Option<&str>, project: Option<String>) -> Vec<DiscoveryProbe> {
    let mut probes = vec![DiscoveryProbe::new(
        DiscoveryKind::Config,
        ProbeAction::Exec {
            argv: vec![
                bin.to_string(),
                "mcp".into(),
                "list".into(),
                "--json".into(),
            ],
            env: vec![],
            cwd: project.clone(),
        },
        parse_mcp_list,
    )];
    let mut roots: Vec<(String, &'static str)> = Vec::new();
    if let Some(h) = home {
        roots.push((format!("{h}/skills"), "user"));
    }
    if let Some(p) = &project {
        roots.push((
            format!("{}/.agents/skills", p.trim_end_matches('/')),
            "project",
        ));
    }
    for (root, scope) in roots {
        let r = root.clone();
        probes.push(DiscoveryProbe::new(
            DiscoveryKind::Config,
            config_listing(root, "SKILL.md", true),
            move |out| {
                Ok(Discovered::Config(skills_from_listing(
                    listed_files(out)?,
                    &r,
                    |p| {
                        if p.contains("/.system/") {
                            "system".to_string()
                        } else {
                            scope.to_string()
                        }
                    },
                )))
            },
        ));
    }
    probes
}

/// `codex mcp list --json`: `[{name, enabled, transport: {type: "stdio"|"streamable_http", …}}]`.
pub fn parse_mcp_list(out: &ProbeOutput) -> Result<Discovered, String> {
    let text = exec_stdout(out)?;
    let start = text
        .find('[')
        .ok_or("`mcp list --json` printed no JSON array")?;
    let v: Value = serde_json::from_str(&text[start..])
        .map_err(|e| format!("`mcp list --json` output is not JSON: {e}"))?;
    let list = v
        .as_array()
        .ok_or("`mcp list --json` did not return an array")?;
    let strs = |v: Option<&Value>| -> Vec<String> {
        v.and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let map = |v: Option<&Value>| -> std::collections::BTreeMap<String, String> {
        v.and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    };
    let items = list
        .iter()
        .filter_map(|s| {
            let name = s.get("name").and_then(Value::as_str)?;
            let t = s.get("transport");
            let mcp = match t.and_then(|t| t.get("type")).and_then(Value::as_str) {
                Some("stdio") => t
                    .and_then(|t| t.get("command"))
                    .and_then(Value::as_str)
                    .map(|c| McpTransport::Stdio {
                        command: c.to_string(),
                        args: strs(t.and_then(|t| t.get("args"))),
                        env: map(t.and_then(|t| t.get("env"))),
                    }),
                Some(_) => t
                    .and_then(|t| t.get("url"))
                    .and_then(Value::as_str)
                    .map(|u| McpTransport::Http {
                        url: u.to_string(),
                        headers: map(t.and_then(|t| t.get("http_headers"))),
                    }),
                None => None,
            };
            Some(ConfigItem {
                kind: ConfigItemKind::McpServer,
                name: name.to_string(),
                source: ConfigSource {
                    scope: "merged".into(),
                    path: None,
                },
                description: s
                    .get("disabled_reason")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                enabled: s.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                mcp,
                path: None,
            })
        })
        .collect();
    Ok(Discovered::Config(items))
}

/// A TOML basic string (JSON string syntax is valid TOML for the characters
/// serde_json escapes).
fn toml_str(s: &str) -> String {
    Value::String(s.to_string()).to_string()
}

fn toml_key_ok(k: &str) -> bool {
    !k.is_empty()
        && k.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// `-c mcp_servers.<name>.*` overrides for one server.
pub fn mcp_overrides(server: &McpServerConfig) -> Result<Vec<String>, String> {
    let name = &server.name;
    if !toml_key_ok(name) {
        return Err(format!(
            "server name {name:?} must be [A-Za-z0-9_-]+ to be a codex config key"
        ));
    }
    let mut out = Vec::new();
    let mut push = |k: String, v: String| {
        out.push("-c".to_string());
        out.push(format!("mcp_servers.{name}.{k}={v}"));
    };
    match &server.transport {
        McpTransport::Stdio { command, args, env } => {
            push("command".into(), toml_str(command));
            let args: Vec<String> = args.iter().map(|a| toml_str(a)).collect();
            push("args".into(), format!("[{}]", args.join(", ")));
            for (k, v) in env {
                if !toml_key_ok(k) {
                    return Err(format!("env key {k:?} is not a plain TOML key"));
                }
                push(format!("env.{k}"), toml_str(v));
            }
        }
        McpTransport::Http { url, headers } => {
            push("url".into(), toml_str(url));
            for (k, v) in headers {
                if !toml_key_ok(k) {
                    return Err(format!("header {k:?} is not a plain TOML key"));
                }
                push(format!("http_headers.{k}"), toml_str(v));
            }
        }
    }
    if server.trusted {
        // Run the server's tools without an approval prompt.
        push("default_tools_approval_mode".into(), toml_str("approve"));
    }
    Ok(out)
}

/// Parse rollout files (`rollout-<ts>-<uuid>.jsonl`) into resumable sessions.
pub fn parse_sessions(out: &ProbeOutput) -> Result<Discovered, String> {
    let mut sessions = Vec::new();
    for f in listed_files(out)? {
        let name = wasit_core::content::base_name(&f.path);
        let Some(stem) = name
            .strip_prefix("rollout-")
            .and_then(|s| s.strip_suffix(".jsonl"))
        else {
            continue;
        };
        if stem.len() < 36 {
            continue;
        }
        let mut id = stem[stem.len() - 36..].to_string();
        let mut project = None;
        let mut title = String::new();
        for line in &f.head {
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let payload = v.get("payload").cloned().unwrap_or(Value::Null);
            match v.get("type").and_then(Value::as_str) {
                Some("session_meta") => {
                    if let Some(i) = payload.get("id").and_then(Value::as_str) {
                        id = i.to_string();
                    }
                    project = payload
                        .get("cwd")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                Some("response_item")
                    if title.is_empty()
                        && payload.get("role").and_then(Value::as_str) == Some("user") =>
                {
                    let text = payload
                        .get("content")
                        .and_then(Value::as_array)
                        .and_then(|c| c.iter().find_map(|b| b.get("text").and_then(Value::as_str)))
                        .unwrap_or_default();
                    // Skip injected context blocks (`<environment_context>` …).
                    if !text.trim_start().starts_with('<') {
                        title = text.to_string();
                    }
                }
                _ => {}
            }
        }
        sessions.push(ResumableSession {
            id,
            project,
            title,
            modified_ms: f.modified_ms,
            message_count: f.matching_lines,
        });
    }
    Ok(Discovered::Sessions(sessions))
}

#[cfg(test)]
mod tests;
