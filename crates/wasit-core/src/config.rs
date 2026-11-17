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
pub struct McpServerConfig {
    /// Server name as the agent will see it.
    pub name: String,
    #[serde(flatten)]
    pub transport: McpTransport,
}

/// A skill: the directory `<root>/<name>/` containing `SKILL.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
pub struct PluginRef {
    pub name: String,
    pub path: String,
}

/// Tool restrictions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ToolPolicy {
    /// Tool names the agent must not use (CLI-native names).
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
}

/// The effective agent configuration for one launch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
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
}

/// An item the driver could not render, with the reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
