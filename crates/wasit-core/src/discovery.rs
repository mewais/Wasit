//! Live discovery (models, modes, resumable sessions, version) described
//! without doing any I/O.
//!
//! A descriptor returns [`DiscoveryProbe`]s; the runtime executes each probe's
//! [`ProbeAction`] under its own deadline and hands the captured
//! [`ProbeOutput`] to the probe's pure `parse` function.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigItem, ConfigItemKind, ConfigSource};
use crate::event::{AgentEvent, ModelInfo, SessionMode};
use crate::spec::SpawnSpec;

/// What the runtime knows about the host when asking for probes.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
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
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryKind {
    Version,
    Models,
    Modes,
    Sessions,
    /// Configured items (MCP servers, skills, plugins, commands, agents) as the CLI itself
    /// has them on this host. Several probes of this kind add up.
    Config,
}

/// How the runtime gathers the raw material for a probe.
#[derive(Debug, Clone, PartialEq)]
pub enum ProbeAction {
    /// Run a command to completion and capture stdout.
    Exec {
        argv: Vec<String>,
        env: Vec<(String, String)>,
        /// Working directory (`None` = wherever the runtime runs; CLIs that resolve project
        /// configuration from their cwd need the project here).
        cwd: Option<String>,
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
    /// of each. With a non-empty `count_containing`, also count the lines of each file that
    /// contain any of those substrings ([`FileHead::matching_lines`]; e.g. a transcript's
    /// message records).
    ListFiles {
        dir: String,
        suffix: String,
        recursive: bool,
        max_files: usize,
        head_lines: usize,
        count_containing: Vec<String>,
    },
    /// Read one (small) file. A missing file is not an error: it yields `contents: None`.
    ReadFile { path: String, max_bytes: usize },
}

/// One file returned for [`ProbeAction::ListFiles`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct FileHead {
    pub path: String,
    /// Modification time, milliseconds since the Unix epoch.
    pub modified_ms: u64,
    pub head: Vec<String>,
    /// Lines containing one of the probe's `count_containing` substrings (`None` when the
    /// probe asked for no count or the file could not be read to the end).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matching_lines: Option<u64>,
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
    File {
        path: String,
        /// `None` = the file does not exist.
        contents: Option<String>,
    },
}

/// A session the CLI can resume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ResumableSession {
    /// Native id for `ResumeMode::Resume`.
    pub id: String,
    /// Project directory, when known.
    pub project: Option<String>,
    /// Title or first user message, when known.
    pub title: String,
    /// Last activity (last modification), milliseconds since the Unix epoch (0 = unknown).
    pub modified_ms: u64,
    /// Messages in the conversation, when the CLI's own records say (`None` = unknown).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<u64>,
}

/// A parsed probe result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Discovered {
    Version(String),
    Models(Vec<ModelInfo>),
    Modes(Vec<SessionMode>),
    Sessions(Vec<ResumableSession>),
    Config(Vec<ConfigItem>),
}

/// A pure parser of a probe's captured output. `Err` explains why the output could not be
/// interpreted (shown to the user; never a silent empty).
pub type ProbeParse = Arc<dyn Fn(&ProbeOutput) -> Result<Discovered, String> + Send + Sync>;

/// One discovery probe.
#[derive(Clone)]
pub struct DiscoveryProbe {
    pub kind: DiscoveryKind,
    pub action: ProbeAction,
    pub parse: ProbeParse,
}

impl std::fmt::Debug for DiscoveryProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryProbe")
            .field("kind", &self.kind)
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

impl DiscoveryProbe {
    pub fn new(
        kind: DiscoveryKind,
        action: ProbeAction,
        parse: impl Fn(&ProbeOutput) -> Result<Discovered, String> + Send + Sync + 'static,
    ) -> Self {
        DiscoveryProbe {
            kind,
            action,
            parse: Arc::new(parse),
        }
    }
}

/// Helper for parsers: the contents of a `ReadFile` output (`None` = no such file).
pub fn read_file(out: &ProbeOutput) -> Result<Option<&str>, String> {
    match out {
        ProbeOutput::File { contents, .. } => Ok(contents.as_deref()),
        other => Err(format!("expected file contents, got {other:?}")),
    }
}

/// The `key: value` pairs of a Markdown file's YAML front matter (`---` … `---` at the top),
/// read from its first lines. Only flat scalar keys are returned (quotes trimmed); anything
/// nested is ignored. Empty when the file has no front matter.
pub fn front_matter(head: &[String]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut lines = head.iter().map(|l| l.trim_end());
    if lines.next().map(str::trim) != Some("---") {
        return out;
    }
    for line in lines {
        if line.trim() == "---" {
            break;
        }
        if line.starts_with([' ', '\t']) {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim().trim_matches('"').trim_matches('\'');
            if !k.trim().is_empty() {
                out.insert(k.trim().to_string(), v.to_string());
            }
        }
    }
    out
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

/// Parse a `--version` stdout whose first non-empty line starts with the version
/// (`2.1.3 (Claude Code)`, `1.19.6`). Anything else is an error: the command is not the CLI
/// it stands in for (a per-host agent command such as `genie` is checked this way).
pub fn parse_version_line(out: &ProbeOutput) -> Result<Discovered, String> {
    version_after(out, None)
}

/// [`parse_version_line`] for a CLI that names itself first (`codex-cli 0.169.1`).
pub fn parse_named_version(
    name: &'static str,
) -> impl Fn(&ProbeOutput) -> Result<Discovered, String> {
    move |out| version_after(out, Some(name))
}

fn version_after(out: &ProbeOutput, name: Option<&str>) -> Result<Discovered, String> {
    let stdout = exec_stdout(out)?;
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .ok_or_else(|| "empty version output".to_string())?;
    let mut tokens = line.split_whitespace();
    if let Some(n) = name {
        if tokens.next() != Some(n) {
            return Err(format!("printed {line:?}, not a `{n}` version"));
        }
    }
    let is_version = |t: &str| {
        let t = t.strip_prefix('v').unwrap_or(t);
        t.starts_with(|c: char| c.is_ascii_digit())
            && t.chars()
                .all(|c| c.is_ascii_alphanumeric() || ".-+_".contains(c))
    };
    match tokens.next() {
        Some(t) if is_version(t) => Ok(Discovered::Version(t.to_string())),
        _ => Err(format!("printed {line:?}, not a version")),
    }
}

/// Skills from a `ListFiles` of a skills root (suffix `SKILL.md`, recursive): one item per
/// `<root>/<dir>/SKILL.md` (deeper `SKILL.md` files belong to a skill's own resources and are
/// skipped). Name and description come from the front matter (name falls back to `<dir>`);
/// `path` is the `SKILL.md` file. `scope_of` names the scope of each file.
pub fn skills_from_listing(
    files: &[FileHead],
    root: &str,
    scope_of: impl Fn(&str) -> String,
) -> Vec<ConfigItem> {
    let root = root.trim_end_matches(['/', '\\']);
    let mut out = Vec::new();
    for f in files {
        let rel = f.path.strip_prefix(root).unwrap_or(&f.path);
        let parts: Vec<&str> = rel.split(['/', '\\']).filter(|p| !p.is_empty()).collect();
        let dir = match parts.as_slice() {
            [dir, "SKILL.md"] => *dir,
            [".system", dir, "SKILL.md"] => *dir,
            _ => continue,
        };
        let fm = front_matter(&f.head);
        out.push(ConfigItem {
            kind: ConfigItemKind::Skill,
            name: fm
                .get("name")
                .filter(|n| !n.is_empty())
                .cloned()
                .unwrap_or_else(|| dir.to_string()),
            source: ConfigSource {
                scope: scope_of(&f.path),
                path: Some(f.path.clone()),
            },
            description: fm.get("description").cloned().unwrap_or_default(),
            enabled: true,
            mcp: None,
            path: Some(f.path.clone()),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Markdown-defined items (commands, agents) from a `ListFiles` (suffix `.md`): the name is the
/// front-matter `name` or the file stem; description from the front matter.
pub fn markdown_items(files: &[FileHead], kind: ConfigItemKind, scope: &str) -> Vec<ConfigItem> {
    let mut out: Vec<ConfigItem> = files
        .iter()
        .filter_map(|f| {
            let file = f.path.rsplit(['/', '\\']).next()?;
            let stem = file.strip_suffix(".md")?;
            let fm = front_matter(&f.head);
            Some(ConfigItem {
                kind,
                name: fm
                    .get("name")
                    .filter(|n| !n.is_empty())
                    .cloned()
                    .unwrap_or_else(|| stem.to_string()),
                source: ConfigSource {
                    scope: scope.to_string(),
                    path: Some(f.path.clone()),
                },
                description: fm.get("description").cloned().unwrap_or_default(),
                enabled: true,
                mcp: None,
                path: Some(f.path.clone()),
            })
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// A `ListFiles` probe for config discovery (bounded; newest first is irrelevant here).
pub fn config_listing(dir: String, suffix: &str, recursive: bool) -> ProbeAction {
    ProbeAction::ListFiles {
        dir,
        suffix: suffix.to_string(),
        recursive,
        max_files: 500,
        head_lines: 24,
        count_containing: vec![],
    }
}

#[cfg(test)]
mod config_discovery_tests {
    use super::*;

    fn fh(path: &str, head: &[&str]) -> FileHead {
        FileHead {
            path: path.into(),
            modified_ms: 0,
            head: head.iter().map(|s| s.to_string()).collect(),
            matching_lines: None,
        }
    }

    #[test]
    fn front_matter_reads_flat_keys() {
        let head: Vec<String> = [
            "---",
            "name: x",
            "description: \"a: b\"",
            "meta:",
            "  k: v",
            "---",
            "body",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let fm = front_matter(&head);
        assert_eq!(fm.get("name").map(String::as_str), Some("x"));
        assert_eq!(fm.get("description").map(String::as_str), Some("a: b"));
        assert!(!fm.contains_key("k"));
        assert!(front_matter(&["# no".to_string()]).is_empty());
    }

    #[test]
    fn skills_and_markdown_items() {
        let files = vec![
            fh(
                "/h/skills/a/SKILL.md",
                &["---", "name: alpha", "description: d", "---"],
            ),
            fh("/h/skills/b/SKILL.md", &["no front matter"]),
            fh("/h/skills/a/sub/SKILL.md", &[]),
            fh("/h/skills/.system/c/SKILL.md", &[]),
        ];
        let items = skills_from_listing(&files, "/h/skills/", |p| {
            if p.contains("/.system/") {
                "system".into()
            } else {
                "user".into()
            }
        });
        let names: Vec<(&str, &str)> = items
            .iter()
            .map(|i| (i.name.as_str(), i.source.scope.as_str()))
            .collect();
        assert_eq!(names, [("alpha", "user"), ("b", "user"), ("c", "system")]);
        assert_eq!(items[0].description, "d");
        assert_eq!(items[0].path.as_deref(), Some("/h/skills/a/SKILL.md"));

        let md = markdown_items(
            &[
                fh("/p/agents/rev.md", &["---", "description: reviews", "---"]),
                fh("/p/agents/x.txt", &[]),
            ],
            ConfigItemKind::Agent,
            "project",
        );
        assert_eq!(md.len(), 1);
        assert_eq!(
            (md[0].name.as_str(), md[0].description.as_str()),
            ("rev", "reviews")
        );
    }
}

#[cfg(test)]
mod version_tests {
    use super::*;

    fn out(s: &str) -> ProbeOutput {
        ProbeOutput::Exec {
            exit_code: Some(0),
            stdout: s.into(),
        }
    }

    fn v(r: Result<Discovered, String>) -> Result<String, String> {
        r.map(|d| match d {
            Discovered::Version(v) => v,
            other => panic!("{other:?}"),
        })
    }

    #[test]
    fn versions_of_the_cli_and_of_something_else() {
        assert_eq!(
            v(parse_version_line(&out("2.1.326 (Claude Code)\n"))),
            Ok("2.1.326".into())
        );
        assert_eq!(
            v(parse_version_line(&out("\n1.19.6\n"))),
            Ok("1.19.6".into())
        );
        assert_eq!(
            v(parse_version_line(&out("v0.3.0-beta.1"))),
            Ok("v0.3.0-beta.1".into())
        );
        let codex = parse_named_version("codex-cli");
        assert_eq!(v(codex(&out("codex-cli 0.169.1"))), Ok("0.169.1".into()));
        // A command standing in for the CLI that is something else.
        let e = v(parse_version_line(&out("ls (GNU coreutils) 9.4\n"))).unwrap_err();
        assert!(e.contains("not a version"), "{e}");
        assert!(v(parse_version_line(&out("Python 3.12.1"))).is_err());
        assert!(v(codex(&out("2.1.326 (Claude Code)"))).is_err());
        assert!(v(parse_version_line(&out(""))).is_err());
    }
}
