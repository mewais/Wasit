//! `SpawnSpec` — everything needed to launch one agent session.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// How to resume or start a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ResumeMode {
    /// Brand-new conversation (the default).
    #[default]
    Fresh,
    /// Resume the most recent conversation in `cwd`.
    ///
    /// Only meaningful when the descriptor reports
    /// [`DriverCapabilities::continue_latest`](crate::DriverCapabilities::continue_latest).
    Continue,
    /// Resume a specific prior conversation by its native id (claude session
    /// UUID, codex thread id, ACP session id).
    Resume(String),
}

/// A file a [`ConfigRenderer`](crate::ConfigRenderer) wants written into the
/// session directory before the CLI starts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedFile {
    /// Path relative to [`SpawnSpec::session_dir`]. Never absolute, never `..`.
    pub path: String,
    /// UTF-8 file contents.
    pub contents: String,
}

/// The complete description of how to spawn one agent session.
///
/// The runtime turns this into a process with
/// [`DriverDescriptor::argv`](crate::DriverDescriptor::argv) /
/// [`DriverDescriptor::env`](crate::DriverDescriptor::env) and hands it to
/// [`Protocol::start`](crate::Protocol::start). A driver's
/// [`ConfigRenderer`](crate::ConfigRenderer) may add `extra_args`, `env`,
/// `files` and `protocol_options` before launch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct SpawnSpec {
    /// Working directory for the spawned process (the project).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,

    /// The session's private directory (where the shim keeps its journal).
    /// Config renderers that need to generate files reference them under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_dir: Option<String>,

    /// Model id, verbatim as the CLI names it. `None` = CLI default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Backend-native mode id (claude permission mode, codex collaboration
    /// mode, ACP `mode` config option). Empty = CLI default.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,

    /// Override the agent binary. `None` = the descriptor's default binary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bin: Option<String>,

    /// Extra environment variables for the process (these win over the
    /// driver's own variables).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<(String, String)>,

    /// Extra CLI arguments appended after the driver's own (filled by the
    /// config renderer).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,

    /// Files to write into `session_dir` before launch (filled by the config
    /// renderer).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<GeneratedFile>,

    /// Driver-private launch parameters, written by a driver's own config
    /// renderer and read by the same driver's protocol (e.g. codex skill
    /// roots, which are injected over the protocol). Opaque to everyone else.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub protocol_options: BTreeMap<String, serde_json::Value>,

    /// A shell command the runtime runs before the agent (e.g. `module load
    /// cuda`). Not interpreted by drivers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<String>,

    /// Fresh / continue / resume-by-native-id.
    #[serde(default)]
    pub resume: ResumeMode,

    /// Optional first prompt. The protocol sends it as soon as its handshake
    /// allows (it is queued inside the protocol until then).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_prompt: Option<String>,
}

impl SpawnSpec {
    /// The binary to execute: `bin` if set, else `default`.
    pub fn binary_or<'a>(&'a self, default: &'a str) -> &'a str {
        self.bin.as_deref().unwrap_or(default)
    }

    /// The working directory the protocol should announce to the CLI
    /// (`cwd`, else `"."`).
    pub fn cwd_or_dot(&self) -> &str {
        self.cwd.as_deref().unwrap_or(".")
    }
}
