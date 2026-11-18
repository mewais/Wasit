//! Source scan behind each repository's "all process creation goes through wasit-process"
//! test: finds `Command::new(` (std or tokio) outside the allowed places.

use std::path::{Path, PathBuf};

/// Directories never scanned.
const SKIP_DIRS: &[&str] = &["target", "node_modules", ".git", "dist", "gen"];

/// Every `<file>:<line>: <text>` under `root` that creates a process directly, except in:
/// files below a `tests`/`benches`/`examples` directory, `build.rs`, the `#[cfg(test)]`
/// module at the end of a file, and paths ending with one of `allow` (relative, `/`-separated).
pub fn raw_spawns(root: &Path, allow: &[&str]) -> Vec<String> {
    let mut files = Vec::new();
    collect(root, &mut files);
    files.sort();
    let mut found = Vec::new();
    for f in files {
        let rel = f
            .strip_prefix(root)
            .unwrap_or(&f)
            .to_string_lossy()
            .replace('\\', "/");
        let exempt_dir = rel
            .split('/')
            .any(|c| matches!(c, "tests" | "benches" | "examples"));
        if exempt_dir || rel.ends_with("build.rs") || allow.iter().any(|a| rel.ends_with(a)) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&f) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            let t = line.trim_start();
            if t.starts_with("#[cfg(test)]") {
                break;
            }
            if t.starts_with("//") {
                continue;
            }
            if creates_process(line) {
                found.push(format!("{rel}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    found
}

/// `Command::new(` not preceded by an identifier character (so `CommandBuilder::new(` and
/// `ShimCommand::new(` do not count).
pub fn creates_process(line: &str) -> bool {
    let pat = "Command::new(";
    let mut from = 0;
    while let Some(i) = line[from..].find(pat) {
        let at = from + i;
        let prev = line[..at].chars().next_back();
        if !prev.is_some_and(|c| c.is_alphanumeric() || c == '_') {
            return true;
        }
        from = at + pat.len();
    }
    false
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name();
        let name = name.to_string_lossy();
        if p.is_dir() {
            if !SKIP_DIRS.contains(&name.as_ref()) && !name.starts_with('.') {
                collect(&p, out);
            }
        } else if name.ends_with(".rs") {
            out.push(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern() {
        assert!(creates_process(
            "let c = std::process::Command::new(\"git\");"
        ));
        assert!(creates_process("tokio::process::Command::new(exe)"));
        assert!(creates_process("    Command::new(prog)"));
        assert!(!creates_process("CommandBuilder::new(&shell)"));
        assert!(!creates_process("ShimCommand::new(x)"));
        assert!(!creates_process("wasit_process::command(prog)"));
    }
}
