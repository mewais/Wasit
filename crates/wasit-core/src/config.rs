//! Agent configuration items and the [`ConfigRenderer`] hook (ARCHITECTURE §6).
//!
//! The effective [`AgentConfig`] (computed by the product from its layers) is
//! rendered by the driver into launch-time injection only: CLI arguments,
//! environment variables, generated files in the session directory, or
//! driver-private protocol options. The user's own CLI config files are never
//! touched. An item a driver cannot honour comes back as [`Unsupported`].

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::spec::SpawnSpec;

/// How to reach an MCP server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum McpTransport {
    /// A local process speaking MCP over stdio.
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    },
    /// A remote server over streamable HTTP.
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
}

/// One MCP server to attach to the agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct McpServerConfig {
    /// Server name as the agent will see it.
    pub name: String,
    #[serde(flatten)]
    pub transport: McpTransport,
}

/// A skill: the directory `<root>/<name>/` containing `SKILL.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct SkillRef {
    pub name: String,
    /// The skills root that contains the `<name>/` directory.
    pub root: String,
}

impl SkillRef {
    /// `<root>/<name>`.
    pub fn dir(&self) -> String {
        format!("{}/{}", self.root.trim_end_matches('/'), self.name)
    }
}

/// A plugin, as a local path the CLI can load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct PluginRef {
    pub name: String,
    pub path: String,
}

/// Tool restrictions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ToolPolicy {
    /// Tool names the agent must not use (CLI-native names).
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
}

/// The effective agent configuration for one launch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct AgentConfig {
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConfig>,
    #[serde(default)]
    pub skills: Vec<SkillRef>,
    #[serde(default)]
    pub plugins: Vec<PluginRef>,
    /// Text appended to the agent's instructions / system prompt.
    #[serde(default)]
    pub extra_instructions: Option<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default)]
    pub tool_policy: ToolPolicy,
    /// Items the CLI has configured itself (see [`ConfigItem`]) that this launch must not
    /// load. Rendered without touching the user's files; what a driver cannot switch off
    /// comes back as [`Unsupported`] with `item` = [`ConfigItemRef::key`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disabled: Vec<DisabledItem>,
}

/// Kind of a configurable item a CLI can have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum ConfigItemKind {
    McpServer,
    Skill,
    Plugin,
    Command,
    Agent,
}

impl ConfigItemKind {
    /// Prefix used in [`ConfigItemRef::key`] and [`Unsupported::item`].
    pub fn prefix(self) -> &'static str {
        match self {
            ConfigItemKind::McpServer => "mcp",
            ConfigItemKind::Skill => "skill",
            ConfigItemKind::Plugin => "plugin",
            ConfigItemKind::Command => "command",
            ConfigItemKind::Agent => "agent",
        }
    }
}

/// Names one item: kind + name (MCP server name, skill name, plugin id, …).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ConfigItemRef {
    pub kind: ConfigItemKind,
    pub name: String,
}

impl ConfigItemRef {
    pub fn new(kind: ConfigItemKind, name: impl Into<String>) -> Self {
        ConfigItemRef {
            kind,
            name: name.into(),
        }
    }

    /// `"<prefix>:<name>"`, e.g. `"mcp:github"`, `"skill:review"`.
    pub fn key(&self) -> String {
        format!("{}:{}", self.kind.prefix(), self.name)
    }
}

/// Where a CLI has an item from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ConfigSource {
    /// The CLI's own scope name (`"user"`, `"project"`, `"local"`, `"global"`, `"builtin"`,
    /// `"plugin"`, …), or `"merged"` when the CLI only reports its resolved configuration.
    pub scope: String,
    /// The file or directory the item is defined in, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// One item a CLI has configured on a host, as discovered from the CLI itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ConfigItem {
    pub kind: ConfigItemKind,
    pub name: String,
    pub source: ConfigSource,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// The CLI has the item switched on.
    pub enabled: bool,
    /// MCP servers: the definition, when the CLI exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<McpTransport>,
    /// Skills: the skill directory; plugins: the install path; commands / agents: the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl ConfigItem {
    pub fn item_ref(&self) -> ConfigItemRef {
        ConfigItemRef::new(self.kind, self.name.clone())
    }
}

/// An inherited item to switch off for one launch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct DisabledItem {
    pub kind: ConfigItemKind,
    pub name: String,
    /// The item's path as discovered (some CLIs address skills by `SKILL.md` path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl DisabledItem {
    pub fn item_ref(&self) -> ConfigItemRef {
        ConfigItemRef::new(self.kind, self.name.clone())
    }
}

/// An item the driver could not render, with the reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct Unsupported {
    /// Which item, e.g. `"skill:review"`, `"plugin:x"`, `"extra_instructions"`.
    pub item: String,
    pub reason: String,
}

impl Unsupported {
    pub fn new(item: impl Into<String>, reason: impl Into<String>) -> Self {
        Unsupported {
            item: item.into(),
            reason: reason.into(),
        }
    }
}

/// Renders an [`AgentConfig`] into launch-time injection on a [`SpawnSpec`].
pub trait ConfigRenderer {
    /// Mutate `spec` (args, env, files, protocol options) to honour `config`;
    /// return every item that could not be honoured. Never fails the launch.
    fn render_config(&self, config: &AgentConfig, spec: &mut SpawnSpec) -> Vec<Unsupported>;
}
