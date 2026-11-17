//! Argv / environment handling: persona detection, `--version`, discovery commands, dispatch.

use crate::acp::{Acp, Flavor};
use crate::claude::{Claude, ClaudeArgs};
use crate::codex::Codex;
use crate::engine::{serve, Cursor};
use crate::io::Io;
use crate::reference::recorded_version;
use crate::scenario::Scenario;
use serde_json::json;

/// The CLI being impersonated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersonaKind {
    Claude,
    Codex,
    Opencode,
    Kilo,
}

impl PersonaKind {
    pub fn name(self) -> &'static str {
        match self {
            PersonaKind::Claude => "claude",
            PersonaKind::Codex => "codex",
            PersonaKind::Opencode => "opencode",
            PersonaKind::Kilo => "kilo",
        }
    }

    fn parse(s: &str) -> Option<PersonaKind> {
        match s {
            "claude" => Some(PersonaKind::Claude),
            "codex" => Some(PersonaKind::Codex),
            "opencode" => Some(PersonaKind::Opencode),
            "kilo" => Some(PersonaKind::Kilo),
            _ => None,
        }
    }
}

/// Pick the persona: `WASIT_FAKE_PERSONA`, else the argv shape, else the executable's name.
pub fn detect_persona(
    args: &[String],
    env_persona: Option<&str>,
    exe_name: &str,
) -> Option<PersonaKind> {
    if let Some(p) = env_persona.and_then(PersonaKind::parse) {
        return Some(p);
    }
    let has = |s: &str| args.iter().any(|a| a == s);
    let by_name = ["kilo", "opencode", "codex", "claude"]
        .iter()
        .find(|n| exe_name.contains(*n))
        .and_then(|n| PersonaKind::parse(n));
    if args
        .windows(2)
        .any(|w| w[0] == "--input-format" && w[1] == "stream-json")
    {
        Some(PersonaKind::Claude)
    } else if has("app-server") {
        Some(PersonaKind::Codex)
    } else if has("acp") {
        Some(match by_name {
            Some(PersonaKind::Kilo) => PersonaKind::Kilo,
            _ => PersonaKind::Opencode,
        })
    } else {
        by_name
    }
}

fn load_scenario() -> Result<Scenario, String> {
    match std::env::var_os("WASIT_FAKE_SCENARIO") {
        Some(p) => Scenario::from_file(p),
        None => Ok(Scenario::default()),
    }
}

fn version_of(kind: PersonaKind, scenario: &Scenario) -> String {
    match &scenario.version {
        Some(v) => match kind {
            PersonaKind::Claude => format!("{v} (Claude Code)"),
            PersonaKind::Codex => format!("codex-cli {v}"),
            _ => v.clone(),
        },
        None => recorded_version(kind.name()),
    }
}

/// Default model ids `models` lists when the scenario has none.
fn default_models(kind: PersonaKind) -> Vec<String> {
    match kind {
        PersonaKind::Kilo => vec!["kilo/kilo-auto/free".into()],
        _ => vec![
            "opencode/big-pickle".into(),
            "opencode/deepseek-v4.1-flash-free".into(),
        ],
    }
}

/// Handle `--version` and the discovery subcommands. Returns the process exit code when the argv
/// was one of those, `None` when the fake should serve a protocol session.
pub fn run_discovery(kind: PersonaKind, args: &[String], scenario: &Scenario) -> Option<i32> {
    let rest: Vec<&str> = args.iter().map(String::as_str).collect();
    if rest
        .iter()
        .any(|a| *a == "--version" || *a == "-v" || *a == "-V")
    {
        println!("{}", version_of(kind, scenario));
        return Some(0);
    }
    if !matches!(kind, PersonaKind::Opencode | PersonaKind::Kilo) {
        return None;
    }
    match rest.first().copied() {
        Some("models") => {
            let filter = rest.get(1).filter(|a| !a.starts_with('-')).copied();
            let verbose = rest.contains(&"--verbose");
            let ids: Vec<String> = if scenario.models.is_empty() {
                default_models(kind)
            } else {
                scenario.models.iter().map(|m| m.id.clone()).collect()
            };
            for id in ids {
                let (provider, model) = id.split_once('/').unwrap_or(("", &id));
                if filter.is_some_and(|f| f != provider) {
                    continue;
                }
                println!("{id}");
                if verbose {
                    let name = scenario
                        .models
                        .iter()
                        .find(|m| m.id == id)
                        .and_then(|m| m.name.clone())
                        .unwrap_or_else(|| model.to_string());
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "id": model, "providerID": provider, "name": name,
                            "status": "active", "capabilities": {"toolcall": true}
                        }))
                        .expect("serializable")
                    );
                }
            }
            Some(0)
        }
        Some("session") if rest.get(1) == Some(&"list") => {
            let json_format = rest
                .windows(2)
                .any(|w| w[0] == "--format" && w[1] == "json");
            let cwd = std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            if json_format {
                let list: Vec<_> = scenario
                    .sessions
                    .iter()
                    .map(|s| {
                        json!({"id": s.id, "title": s.title.clone().unwrap_or_default(),
                               "updated": crate::util::now_ms(), "created": crate::util::now_ms(),
                               "projectId": "global",
                               "directory": s.cwd.clone().unwrap_or_else(|| cwd.clone())})
                    })
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&list).expect("serializable")
                );
            } else {
                println!("{:<31} {:<40} Updated", "Session ID", "Title");
                println!("{}", "─".repeat(80));
                for s in &scenario.sessions {
                    println!(
                        "{:<31} {:<40} now",
                        s.id,
                        s.title.clone().unwrap_or_default()
                    );
                }
            }
            Some(0)
        }
        _ => None,
    }
}

/// Entry point used by `main`.
pub fn main_with(args: Vec<String>) -> i32 {
    let exe = std::env::args()
        .next()
        .map(|a| {
            std::path::Path::new(&a)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or(a)
        })
        .unwrap_or_default();
    let env_persona = std::env::var("WASIT_FAKE_PERSONA").ok();
    let scenario = match load_scenario() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("wasit-fake-cli: {e}");
            return 2;
        }
    };
    let Some(kind) = detect_persona(&args, env_persona.as_deref(), &exe) else {
        if args.iter().any(|a| a == "--version") {
            println!("wasit-fake-cli {}", env!("CARGO_PKG_VERSION"));
            return 0;
        }
        eprintln!(
            "wasit-fake-cli: cannot tell which CLI to impersonate; set WASIT_FAKE_PERSONA=claude|codex|opencode|kilo"
        );
        return 2;
    };
    if let Some(code) = run_discovery(kind, &args, &scenario) {
        return code;
    }
    let mut io = Io::stdio();
    let mut cursor = Cursor::new(scenario.clone());
    match kind {
        PersonaKind::Claude => {
            let mut p = Claude::new(&ClaudeArgs::parse(&args), &scenario);
            serve(&mut p, &mut io, &mut cursor);
        }
        PersonaKind::Codex => {
            let mut p = Codex::new(&scenario);
            serve(&mut p, &mut io, &mut cursor);
        }
        PersonaKind::Opencode | PersonaKind::Kilo => {
            let flavor = if kind == PersonaKind::Kilo {
                Flavor::Kilo
            } else {
                Flavor::Opencode
            };
            let mut p = Acp::new(flavor, &scenario);
            serve(&mut p, &mut io, &mut cursor);
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn persona_from_argv_shape() {
        let d = |args: &[&str], env: Option<&str>, exe: &str| detect_persona(&a(args), env, exe);
        assert_eq!(
            d(&["--print", "--input-format", "stream-json"], None, "x"),
            Some(PersonaKind::Claude)
        );
        assert_eq!(
            d(&["app-server", "--stdio"], None, "x"),
            Some(PersonaKind::Codex)
        );
        assert_eq!(d(&["acp"], None, "x"), Some(PersonaKind::Opencode));
        assert_eq!(d(&["acp"], None, "kilo"), Some(PersonaKind::Kilo));
        assert_eq!(d(&["acp"], Some("kilo"), "x"), Some(PersonaKind::Kilo));
        assert_eq!(d(&["--version"], None, "wasit-fake-cli"), None);
    }
}
