//! The recorded real-CLI exchanges (`reference/<persona>/*.jsonl`), embedded in the crate.
//!
//! Each line is `{"dir":"in"|"out","msg":{...}}`: `in` is client → CLI, `out` is CLI → client.
//! Personas use the recordings as templates for large static objects (claude `system/init`,
//! codex `thread/start` result, ACP `initialize` result) and tests use them as the shape oracle.

use serde_json::Value;
use std::sync::OnceLock;

macro_rules! refs {
    ($( $persona:literal / $name:literal ),* $(,)?) => {
        /// `(persona, recording name, raw jsonl)` for every embedded recording.
        pub const RECORDINGS: &[(&str, &str, &str)] = &[
            $( ($persona, $name, include_str!(concat!("../reference/", $persona, "/", $name, ".jsonl"))) ),*
        ];
    };
}

refs! {
    "claude"/"simple", "claude"/"tool_allow", "claude"/"tool_deny", "claude"/"ask_question",
    "claude"/"interrupt", "claude"/"resume", "claude"/"set_mode", "claude"/"turn_error",
    "claude"/"partial_messages", "claude"/"lifecycle",
    "codex"/"handshake", "codex"/"simple", "codex"/"tool_allow", "codex"/"tool_deny",
    "codex"/"interrupt", "codex"/"resume", "codex"/"mode", "codex"/"error", "codex"/"question",
    "codex"/"lifecycle",
    "opencode"/"handshake", "opencode"/"simple", "opencode"/"tool_allow", "opencode"/"tool_deny",
    "opencode"/"interrupt", "opencode"/"load", "opencode"/"mode", "opencode"/"error",
    "opencode"/"lifecycle", "opencode"/"plan",
    "kilo"/"handshake", "kilo"/"simple", "kilo"/"tool_allow", "kilo"/"tool_deny",
    "kilo"/"interrupt", "kilo"/"load", "kilo"/"mode", "kilo"/"error", "kilo"/"lifecycle",
}

/// Direction of a recorded line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    /// Client → CLI.
    In,
    /// CLI → client.
    Out,
}

/// One parsed recorded line.
#[derive(Debug, Clone)]
pub struct Line {
    pub dir: Dir,
    pub msg: Value,
}

/// Raw text of a recording.
pub fn raw(persona: &str, name: &str) -> Option<&'static str> {
    RECORDINGS
        .iter()
        .find(|(p, n, _)| *p == persona && *n == name)
        .map(|(_, _, t)| *t)
}

/// Parsed lines of a recording.
pub fn lines(persona: &str, name: &str) -> Vec<Line> {
    let Some(text) = raw(persona, name) else {
        return vec![];
    };
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| {
            let v: Value = serde_json::from_str(l).ok()?;
            let dir = if v["dir"] == "in" { Dir::In } else { Dir::Out };
            Some(Line {
                dir,
                msg: v["msg"].clone(),
            })
        })
        .collect()
}

/// First recorded CLI → client message in `persona/name` satisfying `pred` (a clone, ready to patch).
pub fn find_out(persona: &str, name: &str, pred: impl Fn(&Value) -> bool) -> Value {
    lines(persona, name)
        .into_iter()
        .filter(|l| l.dir == Dir::Out)
        .map(|l| l.msg)
        .find(|m| pred(m))
        .unwrap_or_else(|| panic!("no matching recorded message in reference/{persona}/{name}"))
}

/// Contents of `reference/<persona>/cli/version.txt`, trimmed.
pub fn recorded_version(persona: &str) -> String {
    static V: OnceLock<Vec<(&'static str, String)>> = OnceLock::new();
    let all = V.get_or_init(|| {
        vec![
            (
                "claude",
                include_str!("../reference/claude/cli/version.txt")
                    .trim()
                    .to_string(),
            ),
            (
                "codex",
                include_str!("../reference/codex/cli/version.txt")
                    .trim()
                    .to_string(),
            ),
            (
                "opencode",
                include_str!("../reference/opencode/cli/version.txt")
                    .trim()
                    .to_string(),
            ),
            (
                "kilo",
                include_str!("../reference/kilo/cli/version.txt")
                    .trim()
                    .to_string(),
            ),
        ]
    });
    all.iter()
        .find(|(p, _)| *p == persona)
        .map(|(_, v)| v.clone())
        .unwrap_or_default()
}
