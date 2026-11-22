//! The user's real environment for everything a host starts (ARCHITECTURE.md §3.5).
//!
//! A daemon started over ssh, by `wsl.exe`, or by a GUI gets a bare, non-login environment:
//! tools installed under paths that the user's shell startup files add (nvm/fnm, npm-global,
//! `~/.local/bin`, `~/.bun/bin`, cargo, Homebrew, `module load` setups) are invisible to it.
//! [`HostEnv`] resolves the environment the user's login shell would give, once per daemon start
//! and on demand ([`HostEnv::refresh`]), and installs it as the base environment of every process
//! created through `wasit-process` (discovery probes, shims and so agent CLIs, terminals, git).
//!
//! Stages, in order (each a seam for the next):
//! 1. **captured** — Unix: `$SHELL -l -i -c '<print env between unique markers>'` with stdin
//!    closed and a deadline (output outside the markers, i.e. rc-file chatter, is ignored; the
//!    whole process group is killed at the deadline), falling back to `-l -c`, then to this
//!    process's environment. Windows: this process's environment plus the variables only the
//!    registry has (user over machine), and PATH = this process's entries, then registry
//!    entries it lacks (a GUI's environment is a snapshot from login; installs since then are
//!    only in the registry).
//! 2. *(later)* per-host setup commands, evaluated in the captured environment.
//! 3. *(later)* per-host variables, overlaid.
//!
//! Per-agent command overrides stay in `RuntimeConfig::overrides` / discovery.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;

use crate::wire::{EnvSource, HostEnvironment};

/// Variables that describe the capture shell itself, not the user's environment.
const DROP: &[&str] = &[
    "_",
    "SHLVL",
    "PWD",
    "OLDPWD",
    "PS1",
    "PS2",
    "PS3",
    "PS4",
    "PROMPT_COMMAND",
    "COLUMNS",
    "LINES",
];

/// How to capture.
#[derive(Debug, Clone)]
pub struct CaptureOptions {
    /// The shell (default: `$SHELL`, else the passwd entry, else `/bin/sh`).
    pub shell: Option<PathBuf>,
    /// Deadline of the login + interactive attempt.
    pub interactive_deadline: Duration,
    /// Deadline of the login-only attempt.
    pub login_deadline: Duration,
}

impl Default for CaptureOptions {
    fn default() -> Self {
        CaptureOptions {
            shell: None,
            interactive_deadline: Duration::from_secs(6),
            login_deadline: Duration::from_secs(4),
        }
    }
}

/// One capture: the variables and what to tell the user about them.
#[derive(Debug, Clone)]
pub struct Captured {
    pub vars: Vec<(String, String)>,
    pub report: HostEnvironment,
}

/// The host's environment stage: the current capture, installed as the base environment.
#[derive(Clone)]
pub struct HostEnv {
    opts: CaptureOptions,
    current: Arc<RwLock<Option<HostEnvironment>>>,
    refreshing: Arc<tokio::sync::Mutex<()>>,
}

impl HostEnv {
    pub fn new(opts: CaptureOptions) -> Self {
        HostEnv {
            opts,
            current: Arc::new(RwLock::new(None)),
            refreshing: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// The last report (`None` before the first capture).
    pub fn report(&self) -> Option<HostEnvironment> {
        self.current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Capture again and install the result as the base environment of new processes.
    /// Concurrent calls share one capture.
    pub async fn refresh(&self) -> HostEnvironment {
        let _g = self.refreshing.lock().await;
        let c = capture(&self.opts).await;
        wasit_process::set_base_env(Some(
            c.vars.iter().map(|(k, v)| (k.into(), v.into())).collect(),
        ));
        tracing::info!(
            source = ?c.report.source,
            ms = c.report.duration_ms,
            "captured the user's environment"
        );
        *self.current.write().unwrap_or_else(|e| e.into_inner()) = Some(c.report.clone());
        c.report
    }
}

/// Capture the user's environment (see the module docs). Never fails: the last resort is this
/// process's own environment, with the reasons in `report.notes`.
pub async fn capture(opts: &CaptureOptions) -> Captured {
    let started = Instant::now();
    let mut notes = Vec::new();
    #[cfg(unix)]
    let attempt = capture_unix(opts, &mut notes).await;
    #[cfg(windows)]
    let attempt = capture_windows(opts, &mut notes).await;
    let own: Vec<(String, String)> = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
        .collect();
    let (source, shell, vars) = match attempt {
        Some((src, sh, captured)) => (src, sh, merge_own(captured, &own)),
        None => (EnvSource::Process, None, own),
    };
    let path = vars
        .iter()
        .find(|(k, _)| {
            if cfg!(windows) {
                k.eq_ignore_ascii_case("PATH")
            } else {
                k == "PATH"
            }
        })
        .map(|(_, v)| {
            std::env::split_paths(v)
                .map(|p| p.display().to_string())
                .collect()
        })
        .unwrap_or_default();
    Captured {
        report: HostEnvironment {
            source,
            shell,
            captured_at_ms: crate::store::now_ms(),
            duration_ms: started.elapsed().as_millis() as u64,
            path,
            notes,
        },
        vars,
    }
}

/// Keep what the daemon could already reach: variables only this process has are added, and
/// `PATH` entries only this process has are appended (a login profile that resets `PATH` must
/// not hide a tool the session's own environment pointed at).
pub fn merge_own(
    mut captured: Vec<(String, String)>,
    own: &[(String, String)],
) -> Vec<(String, String)> {
    let same = |a: &str, b: &str| {
        if cfg!(windows) {
            a.eq_ignore_ascii_case(b)
        } else {
            a == b
        }
    };
    for (k, v) in own {
        if DROP.contains(&k.as_str()) {
            continue;
        }
        match captured.iter_mut().find(|(ck, _)| same(ck, k)) {
            None => captured.push((k.clone(), v.clone())),
            Some((_, cv)) if same(k, "PATH") => {
                let have: Vec<PathBuf> = std::env::split_paths(cv.as_str()).collect();
                let extra: Vec<PathBuf> = std::env::split_paths(v)
                    .filter(|p| !p.as_os_str().is_empty() && !have.contains(p))
                    .collect();
                if !extra.is_empty() {
                    if let Ok(joined) = std::env::join_paths(have.into_iter().chain(extra)) {
                        *cv = joined.to_string_lossy().into_owned();
                    }
                }
            }
            Some(_) => {}
        }
    }
    captured
}

/// Parse `env -0` (NUL-separated) or plain `env` output (one per line; a line without `=`
/// continues the previous value).
pub fn parse_env(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let nul = text.contains('\0');
    let items: Vec<&str> = if nul {
        text.split('\0').collect()
    } else {
        text.split('\n').collect()
    };
    for item in items {
        let item = if nul {
            item.trim_start_matches('\n')
        } else {
            item
        };
        let valid_key = |k: &str| {
            !k.is_empty()
                && !k.starts_with(|c: char| c.is_ascii_digit())
                && k.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '%' || c == '.')
        };
        match item.split_once('=') {
            Some((k, v)) if valid_key(k) => out.push((k.to_string(), v.to_string())),
            _ if !nul && !item.is_empty() => {
                if let Some(last) = out.last_mut() {
                    last.1.push('\n');
                    last.1.push_str(item);
                }
            }
            _ => {}
        }
    }
    out.retain(|(k, _)| !DROP.contains(&k.as_str()));
    out
}

/// The text between the `begin` and `end` marker lines, if both are present.
fn between<'a>(out: &'a str, begin: &str, end: &str) -> Option<&'a str> {
    let b = out.find(begin)? + begin.len();
    let rest = &out[b..];
    let e = rest.find(end)?;
    Some(rest[..e].trim_matches('\n'))
}

fn nonce() -> String {
    format!("{:016x}", fastrand::u64(..))
}

/// Run `program args…` with stdin closed and a deadline; stdout and the stderr tail.
async fn run_bounded(
    program: &std::path::Path,
    args: &[String],
    deadline: Duration,
) -> Result<(String, String), String> {
    use std::process::Stdio;
    let mut cmd = wasit_process::command_inheriting(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // A new session: no controlling terminal (an interactive shell in a background group of
    // one would be stopped by SIGTTOU), and a process group to kill at the deadline.
    #[cfg(unix)]
    wasit_process::new_session(&mut cmd);
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("cannot start {}: {e}", program.display()))?;
    let pid = child.id();
    let mut so = child.stdout.take();
    let mut se = child.stderr.take();
    let work = async {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let r1 = async {
            if let Some(s) = so.as_mut() {
                s.read_to_end(&mut out).await?;
            }
            Ok::<_, std::io::Error>(())
        };
        let r2 = async {
            if let Some(s) = se.as_mut() {
                s.read_to_end(&mut err).await?;
            }
            Ok::<_, std::io::Error>(())
        };
        let (a, b) = tokio::join!(r1, r2);
        a.and(b).map_err(|e| e.to_string())?;
        let st = child.wait().await.map_err(|e| e.to_string())?;
        Ok::<_, String>((st, out, err))
    };
    match tokio::time::timeout(deadline, work).await {
        Ok(Ok((_st, out, err))) => Ok((
            String::from_utf8_lossy(&out).into_owned(),
            tail(&String::from_utf8_lossy(&err)),
        )),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            #[cfg(unix)]
            if let Some(p) = pid {
                // The shell and whatever its startup files started (it is the group leader).
                wasit_process::kill_process_group(p);
            }
            #[cfg(not(unix))]
            let _ = pid;
            Err(format!(
                "did not finish within {} s",
                deadline.as_secs_f32()
            ))
        }
    }
}

/// The last few lines of `s` (for notes).
fn tail(s: &str) -> String {
    let lines: Vec<&str> = s.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = lines.len().saturating_sub(5);
    lines[start..].join("\n")
}

#[cfg(unix)]
fn login_shell() -> PathBuf {
    if let Some(s) = std::env::var_os("SHELL").filter(|s| !s.is_empty()) {
        return PathBuf::from(s);
    }
    let uid = wasit_process::effective_uid().to_string();
    std::fs::read_to_string("/etc/passwd")
        .ok()
        .and_then(|p| {
            p.lines().find_map(|l| {
                let f: Vec<&str> = l.split(':').collect();
                (f.len() >= 7 && f[2] == uid && !f[6].is_empty()).then(|| PathBuf::from(f[6]))
            })
        })
        .unwrap_or_else(|| PathBuf::from("/bin/sh"))
}

#[cfg(unix)]
async fn capture_unix(
    opts: &CaptureOptions,
    notes: &mut Vec<String>,
) -> Option<(EnvSource, Option<String>, Vec<(String, String)>)> {
    let shell = opts.shell.clone().unwrap_or_else(login_shell);
    let n = nonce();
    let (begin, end) = (
        format!("__WASIT_ENV_BEGIN_{n}__"),
        format!("__WASIT_ENV_END_{n}__"),
    );
    // POSIX shells and fish alike; `env -0` keeps multi-line values (exported functions).
    let script = format!(
        "printf '\\n%s\\n' '{begin}'; env -0 2>/dev/null || env; printf '\\n%s\\n' '{end}'"
    );
    let attempts = [
        (
            EnvSource::LoginInteractiveShell,
            vec!["-l".to_string(), "-i".into(), "-c".into(), script.clone()],
            opts.interactive_deadline,
        ),
        (
            EnvSource::LoginShell,
            vec!["-l".to_string(), "-c".into(), script.clone()],
            opts.login_deadline,
        ),
    ];
    for (source, args, deadline) in attempts {
        let what = format!("{} {}", shell.display(), args[..args.len() - 2].join(" "));
        match run_bounded(&shell, &args, deadline).await {
            Ok((out, err)) => match between(&out, &begin, &end).map(parse_env) {
                Some(vars) if vars.iter().any(|(k, _)| k == "PATH") => {
                    return Some((source, Some(shell.display().to_string()), vars));
                }
                _ => notes.push(format!(
                    "`{what}` printed no environment{}",
                    if err.is_empty() {
                        String::new()
                    } else {
                        format!(": {err}")
                    }
                )),
            },
            Err(e) => notes.push(format!("`{what}` {e}")),
        }
    }
    notes.push("using the daemon's own environment".into());
    None
}

#[cfg(windows)]
async fn capture_windows(
    opts: &CaptureOptions,
    notes: &mut Vec<String>,
) -> Option<(EnvSource, Option<String>, Vec<(String, String)>)> {
    let n = nonce();
    let (begin, end) = (
        format!("__WASIT_ENV_BEGIN_{n}__"),
        format!("__WASIT_ENV_END_{n}__"),
    );
    let script = format!(
        r#"$x=[Environment]::ExpandEnvironmentVariables
$r=@{{}}
foreach($t in 'User','Machine'){{foreach($e in [Environment]::GetEnvironmentVariables($t).GetEnumerator()){{if($e.Key -ne 'Path' -and -not $r.ContainsKey($e.Key)){{$r[$e.Key]=$x.Invoke($e.Value)}}}}}}
$p=@(); foreach($t in 'Machine','User'){{$v=[Environment]::GetEnvironmentVariable('Path',$t); if($v){{foreach($s in $v.Split(';')){{$s=$x.Invoke($s); if($s -and ($p -notcontains $s)){{$p+=$s}}}}}}}}
$r['Path']=$p -join ';'
$o=[Console]::Out
$o.Write("`n{begin}`n"); foreach($k in $r.Keys){{$o.Write("$k=$($r[$k])`0")}}; $o.Write("`n{end}`n")"#
    );
    let args = vec![
        "-NoProfile".to_string(),
        "-NonInteractive".into(),
        "-Command".into(),
        script,
    ];
    let ps = PathBuf::from("powershell.exe");
    match run_bounded(&ps, &args, opts.interactive_deadline).await {
        Ok((out, err)) => match between(&out, &begin, &end).map(parse_env) {
            Some(reg) if reg.iter().any(|(k, _)| k.eq_ignore_ascii_case("PATH")) => {
                // This process's variables win (PowerShell's own changes are not taken); the
                // registry adds what it lacks: variables set since login and PATH entries.
                let own: Vec<(String, String)> = std::env::vars_os()
                    .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                    .collect();
                return Some((EnvSource::Registry, None, merge_own(own, &reg)));
            }
            _ => notes.push(format!("the registry environment could not be read: {err}")),
        },
        Err(e) => notes.push(format!("powershell {e}")),
    }
    notes.push("using the daemon's own environment".into());
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_variables_and_path_entries_are_kept() {
        let sep = if cfg!(windows) { ";" } else { ":" };
        let captured = vec![
            ("PATH".into(), format!("/a{sep}/b")),
            ("X".into(), "1".into()),
        ];
        let own = vec![
            ("PATH".into(), format!("/b{sep}/fake")),
            ("X".into(), "0".into()),
            ("ONLY".into(), "o".into()),
            ("SHLVL".into(), "3".into()),
        ];
        let m = merge_own(captured, &own);
        assert_eq!(
            m,
            vec![
                ("PATH".into(), format!("/a{sep}/b{sep}/fake")),
                ("X".into(), "1".into()),
                ("ONLY".into(), "o".into()),
            ]
        );
    }

    #[test]
    fn parses_nul_and_line_output() {
        let v = parse_env("A=1\0PATH=/x:/y\0BASH_FUNC_m%%=() {\n echo hi\n}\0_=/bin/env\0");
        assert_eq!(
            v,
            vec![
                ("A".into(), "1".into()),
                ("PATH".into(), "/x:/y".into()),
                ("BASH_FUNC_m%%".into(), "() {\n echo hi\n}".into()),
            ]
        );
        let v = parse_env("A=1\nB=two\nlines\nSHLVL=2\n");
        assert_eq!(
            v,
            vec![("A".into(), "1".into()), ("B".into(), "two\nlines".into())]
        );
        assert_eq!(
            between("noise\n__B__\nA=1\n__E__\nmore", "__B__", "__E__"),
            Some("A=1")
        );
    }
}
