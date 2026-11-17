//! Live discovery (models, modes, resumable sessions, version) described
//! without doing any I/O.
//!
//! A descriptor returns [`DiscoveryProbe`]s; the runtime executes each probe's
//! [`ProbeAction`] under its own deadline and hands the captured
//! [`ProbeOutput`] to the probe's pure `parse` function.

use serde::{Deserialize, Serialize};

use crate::event::{AgentEvent, ModelInfo, SessionMode};
use crate::spec::SpawnSpec;

/// What the runtime knows about the host when asking for probes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DiscoveryContext {
    /// The binary to use (`None` = descriptor default).
    pub binary: Option<String>,
    /// The user's home directory on the host.
    pub home: Option<String>,
    /// Host environment variables relevant to discovery (e.g.
    /// `CLAUDE_CONFIG_DIR`, `CODEX_HOME`). Unlisted = unset.
    pub env: Vec<(String, String)>,
    /// Restrict session discovery to this project directory.
    pub project: Option<String>,
}

impl DiscoveryContext {
    /// Value of an environment variable, if set and non-empty.
    pub fn var(&self, key: &str) -> Option<&str> {
        self.env
            .iter()
            .find(|(k, v)| k == key && !v.is_empty())
            .map(|(_, v)| v.as_str())
    }
}

/// What a probe discovers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryKind {
    Version,
    Models,
    Modes,
    Sessions,
}

/// How the runtime gathers the raw material for a probe.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeAction {
    /// Run a command to completion and capture stdout.
    Exec {
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    /// Launch a throwaway session (argv/env from the descriptor for `spec`),
    /// drive `new_protocol()` with it, and collect its events until one
    /// satisfies [`DiscoveryKind::satisfied_by`] for `until` (or the
    /// deadline / process exit). Then kill it.
    Handshake {
        spec: Box<SpawnSpec>,
        until: DiscoveryKind,
    },
    /// List files ending in `suffix` under `dir` (recursively when asked),
    /// newest first, at most `max_files`, with the first `head_lines` lines
    /// of each.
    ListFiles {
        dir: String,
        suffix: String,
        recursive: bool,
        max_files: usize,
        head_lines: usize,
    },
}

/// One file returned for [`ProbeAction::ListFiles`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileHead {
    pub path: String,
    /// Modification time, milliseconds since the Unix epoch.
    pub modified_ms: u64,
    pub head: Vec<String>,
}

/// What the runtime captured for a probe.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeOutput {
    Exec {
        exit_code: Option<i32>,
        stdout: String,
    },
    Handshake {
        events: Vec<AgentEvent>,
    },
    Files {
        files: Vec<FileHead>,
    },
}

/// A session the CLI can resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumableSession {
    /// Native id for `ResumeMode::Resume`.
    pub id: String,
    /// Project directory, when known.
    pub project: Option<String>,
    /// Title or first user message, when known.
    pub title: String,
    /// Last modification, milliseconds since the Unix epoch (0 = unknown).
    pub modified_ms: u64,
}

/// A parsed probe result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Discovered {
    Version(String),
    Models(Vec<ModelInfo>),
    Modes(Vec<SessionMode>),
    Sessions(Vec<ResumableSession>),
}

/// One discovery probe.
#[derive(Debug, Clone)]
pub struct DiscoveryProbe {
    pub kind: DiscoveryKind,
    pub action: ProbeAction,
    /// Pure parser of the captured output. `Err` explains why the output
    /// could not be interpreted (shown to the user; never a silent empty).
    pub parse: fn(&ProbeOutput) -> Result<Discovered, String>,
}

/// Helper for parsers: the stdout of an `Exec` output, or an error.
pub fn exec_stdout(out: &ProbeOutput) -> Result<&str, String> {
    match out {
        ProbeOutput::Exec {
            exit_code: Some(0),
            stdout,
        } => Ok(stdout),
        ProbeOutput::Exec { exit_code, .. } => Err(format!("command failed: exit {exit_code:?}")),
        other => Err(format!("expected command output, got {other:?}")),
    }
}

/// Helper for parsers: the events of a `Handshake` output, or an error.
pub fn handshake_events(out: &ProbeOutput) -> Result<&[AgentEvent], String> {
    match out {
        ProbeOutput::Handshake { events } => Ok(events),
        other => Err(format!("expected handshake events, got {other:?}")),
    }
}

/// Helper for parsers: the files of a `Files` output, or an error.
pub fn listed_files(out: &ProbeOutput) -> Result<&[FileHead], String> {
    match out {
        ProbeOutput::Files { files } => Ok(files),
        other => Err(format!("expected a file listing, got {other:?}")),
    }
}

/// Generic handshake parsers shared by every driver whose protocol emits
/// `ModelsUpdated` / `ModesUpdated` during its handshake.
pub fn models_from_handshake(out: &ProbeOutput) -> Result<Discovered, String> {
    handshake_events(out)?
        .iter()
        .find_map(|e| match e {
            AgentEvent::ModelsUpdated { models, .. } => Some(Discovered::Models(models.clone())),
            _ => None,
        })
        .ok_or_else(|| "the handshake reported no models".to_string())
}

/// See [`models_from_handshake`].
pub fn modes_from_handshake(out: &ProbeOutput) -> Result<Discovered, String> {
    let events = handshake_events(out)?;
    for e in events {
        match e {
            AgentEvent::ModesUpdated {
                available_modes, ..
            } if !available_modes.is_empty() => {
                return Ok(Discovered::Modes(available_modes.clone()))
            }
            AgentEvent::SessionInit {
                available_modes, ..
            } if !available_modes.is_empty() => {
                return Ok(Discovered::Modes(available_modes.clone()))
            }
            _ => {}
        }
    }
    Err("the handshake reported no modes".to_string())
}

impl DiscoveryKind {
    /// Whether a handshake event completes a [`ProbeAction::Handshake`]
    /// probe waiting for this kind: `Models` ⇐ `ModelsUpdated`; `Modes` ⇐ a
    /// non-empty mode list in `ModesUpdated` or `SessionInit`; `Version` ⇐ a
    /// `SessionInit` with a CLI version; `Sessions` ⇐ never (sessions are not
    /// discovered by handshake).
    pub fn satisfied_by(self, e: &AgentEvent) -> bool {
        match (self, e) {
            (DiscoveryKind::Models, AgentEvent::ModelsUpdated { .. }) => true,
            (
                DiscoveryKind::Modes,
                AgentEvent::ModesUpdated {
                    available_modes, ..
                }
                | AgentEvent::SessionInit {
                    available_modes, ..
                },
            ) => !available_modes.is_empty(),
            (DiscoveryKind::Version, AgentEvent::SessionInit { cli_version, .. }) => {
                !cli_version.is_empty()
            }
            _ => false,
        }
    }
}

/// Parse a `--version` style stdout: the first whitespace token of the first
/// non-empty line that starts with a digit or `v`, else the whole first line.
pub fn parse_version_line(out: &ProbeOutput) -> Result<Discovered, String> {
    let stdout = exec_stdout(out)?;
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .ok_or_else(|| "empty version output".to_string())?;
    let token = line
        .split_whitespace()
        .find(|t| t.starts_with(|c: char| c.is_ascii_digit()))
        .unwrap_or(line);
    Ok(Discovered::Version(token.to_string()))
}
