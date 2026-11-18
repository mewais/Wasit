//! wasit-opencode — ACP drivers for OpenCode and Kilo Code.
//!
//! Both CLIs speak the same ACP dialect (`<bin> acp`), so one
//! [`AcpProtocol`] serves both; a [`Flavor`] carries what differs: driver id,
//! binary, the config-injection environment variable and the client name.
//! Mode ids and model ids are discovered live (OpenCode: `build`/`plan`,
//! models `opencode/…`; Kilo: `code`/`ask`/`debug`/`orchestrator`/`plan`, an
//! extra `effort` option, models `kilo/…`).
//!
//! - [`opencode()`] / [`kilo()`] — the two [`DriverDescriptor`]s.
//! - [`AcpProtocol`] — the pure protocol state machine (see its module docs).

#![forbid(unsafe_code)]

mod protocol;

use std::sync::Arc;

use serde_json::{json, Value};
use wasit_core::{
    exec_stdout, modes_from_handshake, parse_version_line, AgentConfig, BusyPrompt, ConfigItem,
    ConfigItemKind, ConfigRenderer, ConfigSource, Discovered, DiscoveryContext, DiscoveryKind,
    DiscoveryProbe, DriverCapabilities, DriverDescriptor, DriverId, GeneratedFile, McpServerConfig,
    McpTransport, ModelInfo, ProbeAction, ProbeOutput, Protocol, ResumableSession, SpawnSpec,
    Unsupported,
};

pub use protocol::{config_models, config_modes, AcpProtocol};

/// Which ACP CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    OpenCode,
    Kilo,
}

impl Flavor {
    /// Driver id.
    pub fn id(self) -> &'static str {
        match self {
            Flavor::OpenCode => "opencode",
            Flavor::Kilo => "kilo",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Flavor::OpenCode => "OpenCode",
            Flavor::Kilo => "Kilo Code",
        }
    }

    pub fn binary(self) -> &'static str {
        match self {
            Flavor::OpenCode => "opencode",
            Flavor::Kilo => "kilo",
        }
    }

    /// The environment variable holding inline JSON config that the CLI
    /// merges over its own config files (verified with `<bin> debug config`).
    pub fn config_env(self) -> &'static str {
        match self {
            Flavor::OpenCode => "OPENCODE_CONFIG_CONTENT",
            Flavor::Kilo => "KILO_CONFIG_CONTENT",
        }
    }

    /// `initialize.clientInfo.name`.
    pub fn client_name(self) -> &'static str {
        match self {
            Flavor::OpenCode => "wasit-opencode",
            Flavor::Kilo => "wasit-kilo",
        }
    }
}

/// An ACP driver descriptor for one flavor.
#[derive(Debug, Clone, Copy)]
pub struct AcpDescriptor {
    pub flavor: Flavor,
}

/// The OpenCode descriptor.
pub fn opencode() -> Arc<dyn DriverDescriptor> {
    Arc::new(AcpDescriptor {
        flavor: Flavor::OpenCode,
    })
}

/// The Kilo Code descriptor.
pub fn kilo() -> Arc<dyn DriverDescriptor> {
    Arc::new(AcpDescriptor {
        flavor: Flavor::Kilo,
    })
}

impl DriverDescriptor for AcpDescriptor {
    fn id(&self) -> DriverId {
        DriverId::new(self.flavor.id())
    }

    fn display_name(&self) -> &str {
        self.flavor.display_name()
    }

    fn default_binary(&self) -> &str {
        self.flavor.binary()
    }

    fn argv(&self, spec: &SpawnSpec) -> Vec<String> {
        let mut argv = vec![
            spec.binary_or(self.default_binary()).to_string(),
            "acp".to_string(),
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
            slash_commands: true,
            permission_gates: true,
            questions: false,
            plan_gate: false,
            usage: true,
            cost: true,
            resume: true,
            continue_latest: true,
            streaming_output: true,
            prompt_while_busy: BusyPrompt::QueuedByProtocol,
        }
    }

    fn new_protocol(&self) -> Box<dyn Protocol> {
        Box::new(AcpProtocol::new(self.flavor.client_name()))
    }

    fn discovery(&self, ctx: &DiscoveryContext) -> Vec<DiscoveryProbe> {
        let bin = ctx
            .binary
            .clone()
            .unwrap_or_else(|| self.default_binary().to_string());
        let mut probes = config_probes(&bin, ctx.project.clone());
        probes.splice(
            0..0,
            [
                DiscoveryProbe {
                    kind: DiscoveryKind::Version,
                    action: ProbeAction::Exec {
                        argv: vec![bin.clone(), "--version".into()],
                        env: vec![],
                        cwd: None,
                    },
                    parse: std::sync::Arc::new(parse_version_line),
                },
                DiscoveryProbe {
                    kind: DiscoveryKind::Models,
                    action: ProbeAction::Exec {
                        argv: vec![bin.clone(), "models".into()],
                        env: vec![],
                        cwd: None,
                    },
                    parse: std::sync::Arc::new(parse_models),
                },
                // Modes are user-configurable agents; only an ACP session reports them.
                DiscoveryProbe {
                    kind: DiscoveryKind::Modes,
                    action: ProbeAction::Handshake {
                        spec: Box::new(SpawnSpec {
                            bin: Some(bin.clone()),
                            cwd: ctx.project.clone(),
                            ..SpawnSpec::default()
                        }),
                        until: DiscoveryKind::Modes,
                    },
                    parse: std::sync::Arc::new(modes_from_handshake),
                },
                DiscoveryProbe {
                    kind: DiscoveryKind::Sessions,
                    action: ProbeAction::Exec {
                        argv: vec![
                            bin,
                            "session".into(),
                            "list".into(),
                            "--format".into(),
                            "json".into(),
                        ],
                        env: vec![],
                        cwd: None,
                    },
                    parse: std::sync::Arc::new(parse_sessions),
                },
            ],
        );
        probes
    }
}

impl ConfigRenderer for AcpDescriptor {
    fn render_config(&self, config: &AgentConfig, spec: &mut SpawnSpec) -> Vec<Unsupported> {
        let mut unsupported = Vec::new();
        let mut inline = serde_json::Map::new();
        if !config.mcp_servers.is_empty() {
            inline.insert("mcp".into(), mcp_json(&config.mcp_servers));
        }
        if !config.skills.is_empty() {
            // `skills.paths` takes skill directories (verified: the skill is
            // listed by `<bin> debug skill`).
            let paths: Vec<Value> = config
                .skills
                .iter()
                .map(|s| Value::String(s.dir()))
                .collect();
            inline.insert("skills".into(), json!({ "paths": paths }));
        }
        if !config.plugins.is_empty() {
            let specs: Vec<Value> = config
                .plugins
                .iter()
                .map(|p| Value::String(p.path.clone()))
                .collect();
            inline.insert("plugin".into(), Value::Array(specs));
        }
        if let Some(text) = config
            .extra_instructions
            .as_deref()
            .filter(|t| !t.is_empty())
        {
            // `instructions` takes file paths, so the text becomes a file in
            // the session directory.
            match spec.session_dir.clone() {
                Some(dir) => {
                    let rel = "wasit-instructions.md";
                    spec.files.push(GeneratedFile {
                        path: rel.into(),
                        contents: text.to_string(),
                    });
                    inline.insert(
                        "instructions".into(),
                        json!([format!("{}/{rel}", dir.trim_end_matches('/'))]),
                    );
                }
                None => unsupported.push(Unsupported::new(
                    "extra_instructions",
                    "needs a session directory to write the instructions file into",
                )),
            }
        }
        render_disabled(config, &mut inline, &mut unsupported);
        if !config.tool_policy.disallowed_tools.is_empty() {
            unsupported.push(Unsupported::new(
                "tool_policy",
                "per-tool deny lists are not rendered for this CLI yet (its `permission` config is not verified)",
            ));
        }
        if !inline.is_empty() {
            let var = self.flavor.config_env();
            // Merge over any inline config the caller already set.
            let mut merged = spec
                .env
                .iter()
                .rev()
                .find(|(k, _)| k == var)
                .and_then(|(_, v)| serde_json::from_str::<Value>(v).ok())
                .and_then(|v| v.as_object().cloned())
                .unwrap_or_default();
            merged.extend(inline);
            spec.env.retain(|(k, _)| k != var);
            spec.env
                .push((var.to_string(), Value::Object(merged).to_string()));
        }
        spec.env.extend(config.env.iter().cloned());
        unsupported
    }
}

/// Switch off inherited items in the inline config the CLI merges last (verified with
/// `<bin> debug config`): `mcp.<name>.enabled = false`, `agent.<name>.disable = true`, and a
/// `permission.skill.<name> = "deny"` rule for skills (the CLI's own per-skill switch).
/// Plugins and commands cannot be removed by a merge (plugin lists concatenate; commands have
/// no off switch).
fn render_disabled(
    config: &AgentConfig,
    inline: &mut serde_json::Map<String, Value>,
    unsupported: &mut Vec<Unsupported>,
) {
    fn obj<'a>(
        m: &'a mut serde_json::Map<String, Value>,
        k: &str,
    ) -> &'a mut serde_json::Map<String, Value> {
        let v = m.entry(k.to_string()).or_insert_with(|| json!({}));
        if !v.is_object() {
            *v = json!({});
        }
        match v {
            Value::Object(o) => o,
            _ => unreachable!("replaced by an object above"),
        }
    }
    for d in &config.disabled {
        match d.kind {
            ConfigItemKind::McpServer => {
                obj(obj(inline, "mcp"), &d.name).insert("enabled".into(), json!(false));
            }
            ConfigItemKind::Agent => {
                obj(obj(inline, "agent"), &d.name).insert("disable".into(), json!(true));
            }
            ConfigItemKind::Skill => {
                obj(obj(inline, "permission"), "skill").insert(d.name.clone(), json!("deny"));
            }
            ConfigItemKind::Plugin => unsupported.push(Unsupported::new(
                d.item_ref().key(),
                "plugin lists from the CLI's config files are concatenated; a launch-time config cannot remove one",
            )),
            ConfigItemKind::Command => unsupported.push(Unsupported::new(
                d.item_ref().key(),
                "the CLI has no switch that disables a configured command",
            )),
        }
    }
}

/// Probes for what the CLI itself has configured: `<bin> debug config` (resolved JSON config:
/// MCP servers, plugins with their origins, agents, commands) and `<bin> debug skill` (every
/// skill with its location), both run in the project directory.
fn config_probes(bin: &str, project: Option<String>) -> Vec<DiscoveryProbe> {
    let exec = |sub: &str| ProbeAction::Exec {
        argv: vec![bin.to_string(), "debug".into(), sub.into()],
        env: vec![],
        cwd: project.clone(),
    };
    let p = project.clone();
    vec![
        DiscoveryProbe::new(DiscoveryKind::Config, exec("config"), parse_debug_config),
        DiscoveryProbe::new(DiscoveryKind::Config, exec("skill"), move |out| {
            parse_debug_skill(out, p.as_deref())
        }),
    ]
}

fn json_stdout(out: &ProbeOutput, what: &str) -> Result<Value, String> {
    let text = exec_stdout(out)?;
    let start = text
        .find(['{', '['])
        .ok_or(format!("`{what}` printed no JSON"))?;
    serde_json::from_str(&text[start..]).map_err(|e| format!("`{what}` output is not JSON: {e}"))
}

fn str_map(v: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn merged(kind: ConfigItemKind, name: &str, description: &str, enabled: bool) -> ConfigItem {
    ConfigItem {
        kind,
        name: name.to_string(),
        source: ConfigSource {
            scope: "merged".into(),
            path: None,
        },
        description: description.to_string(),
        enabled,
        mcp: None,
        path: None,
    }
}

/// `<bin> debug config`: the resolved configuration.
pub fn parse_debug_config(out: &ProbeOutput) -> Result<Discovered, String> {
    let v = json_stdout(out, "debug config")?;
    let mut items = Vec::new();
    if let Some(mcp) = v.get("mcp").and_then(Value::as_object) {
        for (name, def) in mcp {
            let mut item = merged(
                ConfigItemKind::McpServer,
                name,
                "",
                def.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            );
            item.mcp = match def.get("type").and_then(Value::as_str) {
                Some("local") => {
                    let cmd: Vec<String> = def
                        .get("command")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    cmd.split_first().map(|(c, rest)| McpTransport::Stdio {
                        command: c.clone(),
                        args: rest.to_vec(),
                        env: str_map(def.get("environment")),
                    })
                }
                Some("remote") => {
                    def.get("url")
                        .and_then(Value::as_str)
                        .map(|u| McpTransport::Http {
                            url: u.to_string(),
                            headers: str_map(def.get("headers")),
                        })
                }
                _ => None,
            };
            items.push(item);
        }
    }
    let origins: Vec<&Value> = v
        .get("plugin_origins")
        .and_then(Value::as_array)
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    if let Some(plugins) = v.get("plugin").and_then(Value::as_array) {
        for p in plugins {
            let spec = match p {
                Value::String(s) => s.clone(),
                Value::Array(a) => match a.first().and_then(Value::as_str) {
                    Some(s) => s.to_string(),
                    None => continue,
                },
                _ => continue,
            };
            let mut item = merged(ConfigItemKind::Plugin, &spec, "", true);
            if let Some(o) = origins
                .iter()
                .find(|o| o.get("spec").and_then(Value::as_str) == Some(spec.as_str()))
            {
                item.source = ConfigSource {
                    scope: o
                        .get("scope")
                        .and_then(Value::as_str)
                        .unwrap_or("merged")
                        .to_string(),
                    path: o.get("source").and_then(Value::as_str).map(str::to_string),
                };
            } else {
                item.source.scope = "builtin".into();
            }
            items.push(item);
        }
    }
    for (key, kind) in [
        ("agent", ConfigItemKind::Agent),
        ("command", ConfigItemKind::Command),
    ] {
        if let Some(map) = v.get(key).and_then(Value::as_object) {
            for (name, def) in map {
                items.push(merged(
                    kind,
                    name,
                    def.get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    !def.get("disable").and_then(Value::as_bool).unwrap_or(false),
                ));
            }
        }
    }
    Ok(Discovered::Config(items))
}

/// `<bin> debug skill`: `[{name, description, location, content}]`; `location` is a
/// `SKILL.md` path, or a marker such as `<built-in>` / `builtin` for skills shipped inside
/// the CLI.
pub fn parse_debug_skill(out: &ProbeOutput, project: Option<&str>) -> Result<Discovered, String> {
    let v = json_stdout(out, "debug skill")?;
    let list = v
        .as_array()
        .ok_or("`debug skill` did not return an array")?;
    let items = list
        .iter()
        .filter_map(|s| {
            let name = s.get("name").and_then(Value::as_str)?;
            let location = s
                .get("location")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let is_file = location.ends_with(".md");
            let scope = if !is_file {
                "builtin"
            } else if project.is_some_and(|p| location.starts_with(p)) {
                "project"
            } else {
                "global"
            };
            Some(ConfigItem {
                kind: ConfigItemKind::Skill,
                name: name.to_string(),
                source: ConfigSource {
                    scope: scope.into(),
                    path: is_file.then(|| location.to_string()),
                },
                description: s
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                enabled: true,
                mcp: None,
                path: is_file.then(|| location.to_string()),
            })
        })
        .collect();
    Ok(Discovered::Config(items))
}

/// The `mcp` config object.
pub fn mcp_json(servers: &[McpServerConfig]) -> Value {
    let mut map = serde_json::Map::new();
    for s in servers {
        let v = match &s.transport {
            McpTransport::Stdio { command, args, env } => {
                let mut cmd = vec![command.clone()];
                cmd.extend(args.iter().cloned());
                json!({"type": "local", "command": cmd, "environment": env})
            }
            McpTransport::Http { url, headers } => {
                json!({"type": "remote", "url": url, "headers": headers})
            }
        };
        map.insert(s.name.clone(), v);
    }
    Value::Object(map)
}

/// Parse `<bin> models`: one `provider/model` per line.
pub fn parse_models(out: &ProbeOutput) -> Result<Discovered, String> {
    let models = exec_stdout(out)?
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && l.contains('/') && !l.contains(' '))
        .map(|id| ModelInfo {
            id: id.to_string(),
            display_name: id.to_string(),
            description: String::new(),
            is_default: false,
        })
        .collect();
    Ok(Discovered::Models(models))
}

/// Parse `<bin> session list --format json`.
pub fn parse_sessions(out: &ProbeOutput) -> Result<Discovered, String> {
    let stdout = exec_stdout(out)?;
    // With no sessions for the project the CLI prints nothing at all (verified with opencode
    // 1.19.6 and kilo 7.8.27).
    if stdout.trim().is_empty() {
        return Ok(Discovered::Sessions(Vec::new()));
    }
    let v: Value = serde_json::from_str(stdout.trim())
        .map_err(|e| format!("session list is not JSON: {e}"))?;
    let arr = v
        .as_array()
        .ok_or_else(|| "session list is not a JSON array".to_string())?;
    Ok(Discovered::Sessions(
        arr.iter()
            .filter_map(|e| {
                Some(ResumableSession {
                    id: e.get("id")?.as_str()?.to_string(),
                    project: e
                        .get("directory")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    title: e
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    modified_ms: e.get("updated").and_then(Value::as_u64).unwrap_or(0),
                })
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests;
