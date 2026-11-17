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
    listed_files, models_from_handshake, modes_from_handshake, parse_version_line, AgentConfig,
    BusyPrompt, ConfigRenderer, Discovered, DiscoveryContext, DiscoveryKind, DiscoveryProbe,
    DriverCapabilities, DriverDescriptor, DriverId, McpServerConfig, McpTransport, ProbeAction,
    ProbeOutput, Protocol, ResumableSession, SpawnSpec, Unsupported,
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
        }
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
                },
                parse: parse_version_line,
            },
            DiscoveryProbe {
                kind: DiscoveryKind::Models,
                action: ProbeAction::Handshake {
                    spec: handshake.clone(),
                    until: DiscoveryKind::Models,
                },
                parse: models_from_handshake,
            },
            DiscoveryProbe {
                kind: DiscoveryKind::Modes,
                action: ProbeAction::Handshake {
                    spec: handshake,
                    until: DiscoveryKind::Modes,
                },
                parse: modes_from_handshake,
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
        if let Some(home) = home {
            probes.push(DiscoveryProbe {
                kind: DiscoveryKind::Sessions,
                action: ProbeAction::ListFiles {
                    dir: format!("{home}/sessions"),
                    suffix: ".jsonl".into(),
                    recursive: true,
                    max_files: 200,
                    head_lines: 40,
                },
                parse: parse_sessions,
            });
        }
        probes
    }
}

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
        spec.env.extend(config.env.iter().cloned());
        unsupported
    }
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
    Ok(out)
}

/// Parse rollout files (`rollout-<ts>-<uuid>.jsonl`) into resumable sessions.
pub fn parse_sessions(out: &ProbeOutput) -> Result<Discovered, String> {
    let mut sessions = Vec::new();
    for f in listed_files(out)? {
        let name = f.path.rsplit('/').next().unwrap_or(&f.path);
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
        });
    }
    Ok(Discovered::Sessions(sessions))
}

#[cfg(test)]
mod tests;
