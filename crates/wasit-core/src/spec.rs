//! `SpawnSpec` — everything needed to launch one agent session.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::protocol::Command;

/// How to resume or start a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
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
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
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
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
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

    /// Optional first prompt. It is the session's first input, applied like any later
    /// [`Command::Prompt`]: whoever starts the protocol takes it out with
    /// [`SpawnSpec::split_first_prompt`] and encodes it right after
    /// [`Protocol::start`](crate::Protocol::start). Protocols never read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_prompt: Option<String>,

    /// The native conversation id this session has, or will have, when it is known before the CLI
    /// reports one: the id the runtime chose for a fresh conversation
    /// ([`DriverDescriptor::chosen_session_id`](crate::DriverDescriptor::chosen_session_id); a
    /// driver passes it only with [`ResumeMode::Fresh`]: the conversation starts under that id).
    /// For [`ResumeMode::Resume`] the id is the one resumed and this stays `None`: read
    /// [`SpawnSpec::known_native_id`]. Journaled with the generation's `start` note, so a replay
    /// does not choose again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_session_id: Option<String>,

    /// The name the user gave the session. A driver whose CLI can name its own conversation when
    /// it starts one passes it then (claude `--name`); otherwise it is only Marsad's display
    /// name. Only set for a generation that starts the conversation, never for one that resumes
    /// it (the conversation already has its title, and the user may have renamed the session
    /// since).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The runtime's id of this session (random, unique per host), set by the runtime for the
    /// sessions it starts. Drivers derive ids that must never repeat across sessions from it
    /// (claude: the `uuid` of each user message). `None` in specs written before it existed:
    /// such a session keeps writing exactly what it wrote before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

impl SpawnSpec {
    /// The spec a protocol is started with (no first prompt) and the first prompt as the
    /// command to encode right after `start`, before anything else.
    pub fn split_first_prompt(&self) -> (SpawnSpec, Option<Command>) {
        let mut spec = self.clone();
        let first = spec
            .first_prompt
            .take()
            .filter(|p| !p.is_empty())
            .map(Command::prompt);
        (spec, first)
    }

    /// The native conversation id known before the CLI reports one: the one being resumed, else
    /// the one the runtime chose for a fresh conversation.
    pub fn known_native_id(&self) -> Option<&str> {
        match &self.resume {
            ResumeMode::Resume(id) => Some(id),
            _ => self.native_session_id.as_deref(),
        }
    }

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
