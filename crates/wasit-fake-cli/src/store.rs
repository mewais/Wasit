//! The fake's "agent storage": what each real CLI keeps on disk between its runs (claude's
//! transcripts, codex's rollouts, an ACP agent's sessions), so a conversation can outlive the
//! process that had it and be resumed by the next one, and be refused the way the real CLI
//! refuses it when it was never stored.
//!
//! One directory, `WASIT_FAKE_STATE_DIR` (default: a per-user directory under the system temp
//! directory, so processes of different test binaries and different crates agree). A test that
//! wants its own isolated storage sets the variable; claude's transcripts live under
//! `CLAUDE_CONFIG_DIR` when that is set (where the real CLI keeps them, and where claude
//! discovery reads them), else under `<state>/claude-config`.

use serde::{de::DeserializeOwned, Serialize};
use std::path::{Path, PathBuf};

/// The storage root.
pub fn state_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("WASIT_FAKE_STATE_DIR") {
        return PathBuf::from(d);
    }
    #[cfg(unix)]
    let user = wasit_process::effective_uid().to_string();
    #[cfg(not(unix))]
    let user = std::env::var("USERNAME").unwrap_or_else(|_| "user".into());
    std::env::temp_dir().join(format!("wasit-fake-cli-state-{user}"))
}

fn kind_dir(kind: &str) -> PathBuf {
    state_dir().join(kind)
}

/// Write `<state>/<kind>/<id>.json` (atomically: another fake may be reading it).
pub fn put<T: Serialize>(kind: &str, id: &str, value: &T) {
    let dir = kind_dir(kind);
    let text = serde_json::to_vec_pretty(value).expect("serializable");
    // A write that fails shows up as a conversation the next process cannot find: say so.
    if let Err(e) = std::fs::create_dir_all(&dir).and_then(|_| write_atomically(&dir, id, &text)) {
        eprintln!("wasit-fake-cli: cannot store {kind}/{id}: {e}");
    }
}

fn write_atomically(dir: &Path, id: &str, text: &[u8]) -> std::io::Result<()> {
    let tmp = dir.join(format!("{id}.json.{}.tmp", std::process::id()));
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, dir.join(format!("{id}.json")))
}

/// Remove `<state>/<kind>/<id>.json`.
pub fn remove(kind: &str, id: &str) {
    match std::fs::remove_file(kind_dir(kind).join(format!("{id}.json"))) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => eprintln!("wasit-fake-cli: cannot remove {kind}/{id}: {e}"),
    }
}

/// Everything stored under `kind`, oldest id first.
pub fn list<T: DeserializeOwned>(kind: &str) -> Vec<T> {
    let Ok(rd) = std::fs::read_dir(kind_dir(kind)) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = rd
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    files
        .iter()
        .filter_map(|p| {
            let text = std::fs::read_to_string(p).ok()?;
            match serde_json::from_str(&text) {
                Ok(v) => Some(v),
                Err(e) => {
                    eprintln!("wasit-fake-cli: unreadable {}: {e}", p.display());
                    None
                }
            }
        })
        .collect()
}
