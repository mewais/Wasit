//! wasit-claude — the Claude Code driver (`claude --print` stream-json with
//! the control protocol).
//!
//! - [`descriptor()`] — the [`DriverDescriptor`] for id `"claude"`.
//! - [`ClaudeProtocol`] — the pure protocol state machine (see its module docs
//!   for the verified wire facts).
//!
//! Launch: `claude --print --input-format stream-json --output-format
//! stream-json --verbose [--model M] [--permission-mode MODE]
//! --permission-prompt-tool stdio [--continue | --resume ID] <rendered args>`.
//! The prompt is never on the command line; it goes through stdin.

#![forbid(unsafe_code)]

mod protocol;

use std::sync::Arc;

use serde_json::{json, Value};
use wasit_core::{
    config_listing, exec_stdout, listed_files, markdown_items, models_from_handshake,
    parse_version_line, read_file, skills_from_listing, AgentConfig, BusyPrompt, ConfigItem,
    ConfigItemKind, ConfigRenderer, ConfigSource, Discovered, DiscoveryContext, DiscoveryKind,
    DiscoveryProbe, DriverCapabilities, DriverDescriptor, DriverId, McpServerConfig, McpTransport,
    ProbeAction, ProbeOutput, Protocol, ResumableSession, ResumeMode, SessionMode, SpawnSpec,
    Unsupported,
};

pub use protocol::{
    encode_allow, encode_deny, encode_elicitation_accept, encode_elicitation_decline,
    encode_question_answer, encode_user_message, ClaudeProtocol,
};

/// The driver id.
pub const ID: &str = "claude";

/// The Claude Code descriptor.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaudeDescriptor;

/// The Claude Code descriptor, shareable.
pub fn descriptor() -> Arc<dyn DriverDescriptor> {
    Arc::new(ClaudeDescriptor)
}

impl DriverDescriptor for ClaudeDescriptor {
    fn id(&self) -> DriverId {
        DriverId::new(ID)
    }

    fn display_name(&self) -> &str {
        "Claude Code"
    }

    fn default_binary(&self) -> &str {
        "claude"
    }

    fn argv(&self, spec: &SpawnSpec) -> Vec<String> {
        let mut argv: Vec<String> = [
            spec.binary_or(self.default_binary()),
            "--print",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if let Some(m) = spec.model.as_deref().filter(|m| !m.is_empty()) {
            argv.extend(["--model".to_string(), m.to_string()]);
        }
        if !spec.mode.is_empty() {
            argv.extend(["--permission-mode".to_string(), spec.mode.clone()]);
        }
        // Route permission prompts to us over the control protocol.
        argv.extend(["--permission-prompt-tool".to_string(), "stdio".to_string()]);
        match &spec.resume {
            ResumeMode::Fresh => {}
            ResumeMode::Continue => argv.push("--continue".to_string()),
            ResumeMode::Resume(id) => argv.extend(["--resume".to_string(), id.clone()]),
        }
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
            slash_commands: true,
            permission_gates: true,
            questions: true,
            plan_gate: true,
            usage: true,
            cost: true,
            resume: true,
            continue_latest: true,
            streaming_output: false,
            prompt_while_busy: BusyPrompt::QueuedByProtocol,
        }
    }

    fn new_protocol(&self) -> Box<dyn Protocol> {
        Box::new(ClaudeProtocol::new())
    }

    fn discovery(&self, ctx: &DiscoveryContext) -> Vec<DiscoveryProbe> {
        let bin = ctx
            .binary
            .clone()
            .unwrap_or_else(|| self.default_binary().to_string());
        let bin_for_config = bin.clone();
        let mut probes = vec![
            DiscoveryProbe {
                kind: DiscoveryKind::Version,
                action: ProbeAction::Exec {
                    argv: vec![bin.clone(), "--version".into()],
                    env: vec![],
                    cwd: None,
                },
                parse: std::sync::Arc::new(parse_version_line),
            },
            // The `initialize` control response carries the model catalog.
            DiscoveryProbe {
                kind: DiscoveryKind::Models,
                action: ProbeAction::Handshake {
                    spec: Box::new(SpawnSpec {
                        bin: Some(bin.clone()),
                        cwd: ctx.project.clone(),
                        ..SpawnSpec::default()
                    }),
                    until: DiscoveryKind::Models,
                },
                parse: std::sync::Arc::new(models_from_handshake),
            },
            // Claude reports no mode list over the protocol; the only live
            // source is the `--permission-mode` choices in `--help`.
            DiscoveryProbe {
                kind: DiscoveryKind::Modes,
                action: ProbeAction::Exec {
                    argv: vec![bin, "--help".into()],
                    env: vec![],
                    cwd: None,
                },
                parse: std::sync::Arc::new(parse_permission_modes),
            },
        ];
        probes.extend(config_probes(ctx, &bin_for_config));
        if let Some(dir) = sessions_dir(ctx) {
            probes.push(DiscoveryProbe {
                kind: DiscoveryKind::Sessions,
                action: ProbeAction::ListFiles {
                    recursive: ctx.project.is_none(),
                    dir,
                    suffix: ".jsonl".into(),
                    max_files: 200,
                    head_lines: 30,
                },
                parse: std::sync::Arc::new(parse_sessions),
            });
        }
        probes
    }
}

impl ConfigRenderer for ClaudeDescriptor {
    fn render_config(&self, config: &AgentConfig, spec: &mut SpawnSpec) -> Vec<Unsupported> {
        let mut unsupported = Vec::new();
        if !config.mcp_servers.is_empty() {
            spec.extra_args.push("--mcp-config".into());
            spec.extra_args
                .push(mcp_config_json(&config.mcp_servers).to_string());
        }
        // Claude discovers skills only under `<dir>/.claude/skills/`;
        // `--add-dir <dir>` makes it load them (verified: the skill appears
        // in the `initialize` command list).
        let mut add_dirs: Vec<String> = Vec::new();
        for skill in &config.skills {
            let root = skill.root.trim_end_matches('/');
            match root.strip_suffix("/.claude/skills") {
                Some(base) if !base.is_empty() => {
                    if !add_dirs.iter().any(|d| d == base) {
                        add_dirs.push(base.to_string());
                    }
                }
                _ => unsupported.push(Unsupported::new(
                    format!("skill:{}", skill.name),
                    "claude only loads skills from a `<dir>/.claude/skills` root (via --add-dir)",
                )),
            }
        }
        for d in add_dirs {
            spec.extra_args.extend(["--add-dir".to_string(), d]);
        }
        for plugin in &config.plugins {
            spec.extra_args
                .extend(["--plugin-dir".to_string(), plugin.path.clone()]);
        }
        if let Some(text) = config
            .extra_instructions
            .as_deref()
            .filter(|t| !t.is_empty())
        {
            spec.extra_args
                .extend(["--append-system-prompt".to_string(), text.to_string()]);
        }
        // `mcp__<server>` allows every tool of that server without asking (claude
        // permission-rule syntax).
        let trusted: Vec<String> = config
            .mcp_servers
            .iter()
            .filter(|s| s.trusted)
            .map(|s| format!("mcp__{}", s.name))
            .collect();
        if !trusted.is_empty() {
            spec.extra_args.push("--allowedTools".into());
            spec.extra_args.push(trusted.join(","));
        }
        if !config.tool_policy.disallowed_tools.is_empty() {
            spec.extra_args.push("--disallowedTools".into());
            spec.extra_args
                .push(config.tool_policy.disallowed_tools.join(","));
        }
        render_disabled(config, spec, &mut unsupported);
        spec.env.extend(config.env.iter().cloned());
        unsupported
    }
}

/// Switch off inherited items through one `--settings` JSON (verified against claude 2.1:
/// `skillOverrides: {name: "off"}` removes a skill or command from the session,
/// `deniedMcpServers: [{serverName}]` drops an MCP server of any scope, `enabledPlugins:
/// {id: false}` unloads a plugin). Claude has no setting that removes a custom agent.
fn render_disabled(config: &AgentConfig, spec: &mut SpawnSpec, unsupported: &mut Vec<Unsupported>) {
    let mut skills = serde_json::Map::new();
    let mut denied = Vec::new();
    let mut plugins = serde_json::Map::new();
    for d in &config.disabled {
        match d.kind {
            ConfigItemKind::Skill | ConfigItemKind::Command => {
                skills.insert(d.name.clone(), json!("off"));
            }
            ConfigItemKind::McpServer => denied.push(json!({ "serverName": d.name })),
            ConfigItemKind::Plugin => {
                plugins.insert(d.name.clone(), json!(false));
            }
            ConfigItemKind::Agent => unsupported.push(Unsupported::new(
                d.item_ref().key(),
                "claude has no launch-time setting that removes a custom agent",
            )),
        }
    }
    let mut settings = serde_json::Map::new();
    if !skills.is_empty() {
        settings.insert("skillOverrides".into(), Value::Object(skills));
    }
    if !denied.is_empty() {
        settings.insert("deniedMcpServers".into(), Value::Array(denied));
    }
    if !plugins.is_empty() {
        settings.insert("enabledPlugins".into(), Value::Object(plugins));
    }
    if !settings.is_empty() {
        spec.extra_args.extend([
            "--settings".to_string(),
            Value::Object(settings).to_string(),
        ]);
    }
}

fn config_dir(ctx: &DiscoveryContext) -> Option<String> {
    match ctx.var("CLAUDE_CONFIG_DIR") {
        Some(d) => Some(d.trim_end_matches('/').to_string()),
        None => Some(format!(
            "{}/.claude",
            ctx.home.as_deref()?.trim_end_matches('/')
        )),
    }
}

/// The global state file holding user- and local-scope MCP servers.
fn state_file(ctx: &DiscoveryContext) -> Option<String> {
    match ctx.var("CLAUDE_CONFIG_DIR") {
        Some(d) => Some(format!("{}/.claude.json", d.trim_end_matches('/'))),
        None => Some(format!(
            "{}/.claude.json",
            ctx.home.as_deref()?.trim_end_matches('/')
        )),
    }
}

/// Probes for what claude itself has configured: MCP servers from its own JSON files
/// (`~/.claude.json` user + local scope, `<project>/.mcp.json` project scope — claude's
/// `mcp list` only renders text), plugins from `claude plugin list --json`, skills, commands
/// and agents from their directories (user and project).
fn config_probes(ctx: &DiscoveryContext, bin: &str) -> Vec<DiscoveryProbe> {
    let mut probes = Vec::new();
    let project = ctx.project.clone();
    if let Some(state) = state_file(ctx) {
        let p = project.clone();
        probes.push(DiscoveryProbe::new(
            DiscoveryKind::Config,
            ProbeAction::ReadFile {
                path: state,
                max_bytes: 64 * 1024 * 1024,
            },
            move |out| parse_state_mcp(out, p.as_deref()),
        ));
    }
    if let Some(proj) = &project {
        probes.push(DiscoveryProbe::new(
            DiscoveryKind::Config,
            ProbeAction::ReadFile {
                path: format!("{}/.mcp.json", proj.trim_end_matches('/')),
                max_bytes: 4 * 1024 * 1024,
            },
            parse_project_mcp,
        ));
    }
    probes.push(DiscoveryProbe::new(
        DiscoveryKind::Config,
        ProbeAction::Exec {
            argv: vec![
                bin.to_string(),
                "plugin".into(),
                "list".into(),
                "--json".into(),
            ],
            env: vec![],
            cwd: project.clone(),
        },
        parse_plugin_list,
    ));
    let mut roots: Vec<(String, &'static str)> = Vec::new();
    if let Some(dir) = config_dir(ctx) {
        roots.push((dir, "user"));
    }
    if let Some(proj) = &project {
        roots.push((format!("{}/.claude", proj.trim_end_matches('/')), "project"));
    }
    for (base, scope) in roots {
        let skills = format!("{base}/skills");
        let root = skills.clone();
        probes.push(DiscoveryProbe::new(
            DiscoveryKind::Config,
            config_listing(skills, "SKILL.md", true),
            move |out| {
                Ok(Discovered::Config(skills_from_listing(
                    listed_files(out)?,
                    &root,
                    |_| scope.to_string(),
                )))
            },
        ));
        for (sub, kind, recursive) in [
            ("agents", ConfigItemKind::Agent, false),
            ("commands", ConfigItemKind::Command, true),
        ] {
            probes.push(DiscoveryProbe::new(
                DiscoveryKind::Config,
                config_listing(format!("{base}/{sub}"), ".md", recursive),
                move |out| {
                    Ok(Discovered::Config(markdown_items(
                        listed_files(out)?,
                        kind,
                        scope,
                    )))
                },
            ));
        }
    }
    probes
}

/// An MCP server definition in claude's JSON (`{"type": "stdio"|"http"|"sse", …}`).
pub fn mcp_transport_from_json(v: &Value) -> Option<McpTransport> {
    let str_map = |key: &str| -> std::collections::BTreeMap<String, String> {
        v.get(key)
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default()
    };
    if let Some(url) = v.get("url").and_then(Value::as_str) {
        return Some(McpTransport::Http {
            url: url.to_string(),
            headers: str_map("headers"),
        });
    }
    let command = v.get("command").and_then(Value::as_str)?;
    Some(McpTransport::Stdio {
        command: command.to_string(),
        args: v
            .get("args")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        env: str_map("env"),
    })
}

fn mcp_items(servers: Option<&Value>, scope: &str, path: &str) -> Vec<ConfigItem> {
    let Some(map) = servers.and_then(Value::as_object) else {
        return Vec::new();
    };
    map.iter()
        .map(|(name, def)| ConfigItem {
            kind: ConfigItemKind::McpServer,
            name: name.clone(),
            source: ConfigSource {
                scope: scope.to_string(),
                path: Some(path.to_string()),
            },
            description: String::new(),
            enabled: true,
            mcp: mcp_transport_from_json(def),
            path: None,
        })
        .collect()
}

fn file_path(out: &ProbeOutput) -> String {
    match out {
        ProbeOutput::File { path, .. } => path.clone(),
        _ => String::new(),
    }
}

/// User-scope (`mcpServers`) and local-scope (`projects[<project>].mcpServers`) servers from
/// `~/.claude.json`.
pub fn parse_state_mcp(out: &ProbeOutput, project: Option<&str>) -> Result<Discovered, String> {
    let Some(text) = read_file(out)? else {
        return Ok(Discovered::Config(Vec::new()));
    };
    let path = file_path(out);
    let v: Value = serde_json::from_str(text).map_err(|e| format!("{path}: {e}"))?;
    let mut items = mcp_items(v.get("mcpServers"), "user", &path);
    if let Some(p) = project {
        let local = v
            .get("projects")
            .and_then(|ps| ps.get(p).or_else(|| ps.get(p.trim_end_matches('/'))))
            .and_then(|e| e.get("mcpServers"));
        items.extend(mcp_items(local, "local", &path));
    }
    Ok(Discovered::Config(items))
}

/// Project-scope servers from `<project>/.mcp.json`.
pub fn parse_project_mcp(out: &ProbeOutput) -> Result<Discovered, String> {
    let Some(text) = read_file(out)? else {
        return Ok(Discovered::Config(Vec::new()));
    };
    let path = file_path(out);
    let v: Value = serde_json::from_str(text).map_err(|e| format!("{path}: {e}"))?;
    Ok(Discovered::Config(mcp_items(
        v.get("mcpServers"),
        "project",
        &path,
    )))
}

/// `claude plugin list --json`: `[{id, scope, enabled, installPath, …}]`.
pub fn parse_plugin_list(out: &ProbeOutput) -> Result<Discovered, String> {
    let text = exec_stdout(out)?;
    let v: Value = serde_json::from_str(text.trim())
        .map_err(|e| format!("`plugin list --json` output is not JSON: {e}"))?;
    let list = v
        .as_array()
        .ok_or("`plugin list --json` did not return an array")?;
    let items = list
        .iter()
        .filter_map(|p| {
            let id = p.get("id").and_then(Value::as_str)?;
            Some(ConfigItem {
                kind: ConfigItemKind::Plugin,
                name: id.to_string(),
                source: ConfigSource {
                    scope: p
                        .get("scope")
                        .and_then(Value::as_str)
                        .unwrap_or("user")
                        .to_string(),
                    path: p
                        .get("installPath")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                },
                description: String::new(),
                enabled: p.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                mcp: None,
                path: p
                    .get("installPath")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect();
    Ok(Discovered::Config(items))
}

/// The `--mcp-config` JSON (`{"mcpServers": {...}}`), deterministic.
pub fn mcp_config_json(servers: &[McpServerConfig]) -> Value {
    let mut map = serde_json::Map::new();
    for s in servers {
        let v = match &s.transport {
            McpTransport::Stdio { command, args, env } => {
                json!({"type": "stdio", "command": command, "args": args, "env": env})
            }
            McpTransport::Http { url, headers } => {
                json!({"type": "http", "url": url, "headers": headers})
            }
        };
        map.insert(s.name.clone(), v);
    }
    json!({ "mcpServers": map })
}

/// Claude's per-project transcript directory name: every non-alphanumeric
/// character of the absolute cwd becomes `-`.
pub fn encode_project_dir(cwd: &str) -> String {
    cwd.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn sessions_dir(ctx: &DiscoveryContext) -> Option<String> {
    let base = match ctx.var("CLAUDE_CONFIG_DIR") {
        Some(d) => d.trim_end_matches('/').to_string(),
        None => format!("{}/.claude", ctx.home.as_deref()?.trim_end_matches('/')),
    };
    Some(match &ctx.project {
        Some(p) => format!("{base}/projects/{}", encode_project_dir(p)),
        None => format!("{base}/projects"),
    })
}

/// Parse the `--permission-mode` choices out of `claude --help`.
pub fn parse_permission_modes(out: &ProbeOutput) -> Result<Discovered, String> {
    let help = exec_stdout(out)?;
    let start = help
        .find("--permission-mode")
        .ok_or_else(|| "`--permission-mode` not found in --help".to_string())?;
    let rest = &help[start..];
    let choices_at = rest
        .find("(choices:")
        .ok_or_else(|| "no choices listed for --permission-mode".to_string())?;
    let list = &rest[choices_at + "(choices:".len()..];
    let end = list
        .find(')')
        .ok_or_else(|| "unterminated choices list".to_string())?;
    let modes: Vec<SessionMode> = list[..end]
        .split(',')
        .map(|c| c.trim().trim_matches('"').to_string())
        .filter(|c| !c.is_empty())
        .map(|id| SessionMode {
            label: id.clone(),
            id,
            description: String::new(),
        })
        .collect();
    if modes.is_empty() {
        return Err("empty --permission-mode choices".into());
    }
    Ok(Discovered::Modes(modes))
}

/// Parse transcript files (`<uuid>.jsonl`) into resumable sessions.
pub fn parse_sessions(out: &ProbeOutput) -> Result<Discovered, String> {
    let files = listed_files(out)?;
    let mut sessions = Vec::new();
    for f in files {
        let name = f.path.rsplit('/').next().unwrap_or(&f.path);
        let Some(id) = name.strip_suffix(".jsonl") else {
            continue;
        };
        // Subagent transcripts live in nested directories; top-level
        // session files are named by a UUID.
        if id.len() != 36 || id.chars().filter(|c| *c == '-').count() != 4 {
            continue;
        }
        let mut project = None;
        let mut title = String::new();
        for line in &f.head {
            let Ok(v) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if v.get("type").and_then(Value::as_str) != Some("user") {
                continue;
            }
            if project.is_none() {
                project = v.get("cwd").and_then(Value::as_str).map(str::to_string);
            }
            if title.is_empty() {
                title = first_text(v.get("message").and_then(|m| m.get("content")));
            }
            if project.is_some() && !title.is_empty() {
                break;
            }
        }
        sessions.push(ResumableSession {
            id: id.to_string(),
            project,
            title,
            modified_ms: f.modified_ms,
        });
    }
    Ok(Discovered::Sessions(sessions))
}

fn first_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .find_map(|b| b.get("text").and_then(Value::as_str))
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests;
