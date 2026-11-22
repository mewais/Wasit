//! The user's real environment for everything a host starts (ARCHITECTURE.md §3.5).
//!
//! A daemon started over ssh, by `wsl.exe`, or by a GUI gets a bare, non-login environment:
//! tools installed under paths that the user's shell startup files add (nvm/fnm, npm-global,
//! `~/.local/bin`, `~/.bun/bin`, cargo, Homebrew, `module load` setups) are invisible to it.
//! [`HostEnv`] resolves the environment the user's login shell would give, once per daemon start
//! and on demand ([`HostEnv::refresh`]), applies the host's configuration on top
//! ([`HostEnv::configure`]) and installs the result as the base environment of every process
//! created through `wasit-process` (discovery probes, shims and so agent CLIs, terminals, git).
//!
//! Stages, in order:
//! 1. **captured** — Unix: `$SHELL -l -i -c '<print env between unique markers>'` with stdin
//!    closed and a deadline (output outside the markers, i.e. rc-file chatter, is ignored; the
//!    whole process group is killed at the deadline), falling back to `-l -c`, then to this
//!    process's environment. Windows: this process's environment plus the variables only the
//!    registry has (user over machine), and PATH = this process's entries, then registry
//!    entries it lacks (a GUI's environment is a snapshot from login; installs since then are
//!    only in the registry).
//! 2. **setup commands** ([`HostEnvConfig::setup`]) — Unix: run by the same shell in the same
//!    mode the capture succeeded with (so functions such as `module` that startup files define
//!    exist), starting from the captured environment, stdin closed, between markers, with
//!    their own deadline; the environment they leave is taken (PATH entries and variables of
//!    the capture they dropped are kept, after theirs). POSIX shells and fish run the lines as
//!    written; another login shell (csh, …) runs them with `/bin/sh`. Windows: PowerShell
//!    (`-NoProfile`) in the captured environment; its `$env:` changes are taken. A setup that
//!    fails is reported ([`SetupReport`]); one that did not finish (deadline, ended the
//!    shell) leaves the captured environment.
//! 3. **variables** ([`HostEnvConfig::vars`]) — set as given (no expansion), over the above.
//! 4. **agent commands** ([`HostEnvConfig::commands`]) — installed in [`AgentCommands`], which
//!    discovery and session launches read.
//!
//! Secret values never appear in a report: every text that could echo them (setup output,
//! notes) is scrubbed, and [`crate::wire::EnvVar`]'s `Debug` hides them.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;

use crate::config::AgentCommands;
use crate::wire::{EnvSource, HostEnvConfig, HostEnvironment, SetupReport};

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

/// Lines of setup output kept in a report.
const SETUP_TAIL_LINES: usize = 40;
/// Bytes of setup output kept in a report.
const SETUP_TAIL_BYTES: usize = 8 * 1024;

/// How to capture.
#[derive(Debug, Clone)]
pub struct CaptureOptions {
    /// The shell (default: `$SHELL`, else the passwd entry, else `/bin/sh`).
    pub shell: Option<PathBuf>,
    /// Deadline of the login + interactive attempt.
    pub interactive_deadline: Duration,
    /// Deadline of the login-only attempt.
    pub login_deadline: Duration,
    /// Deadline of the setup commands (a `module load` on a busy cluster takes seconds).
    pub setup_deadline: Duration,
}

impl Default for CaptureOptions {
    fn default() -> Self {
        CaptureOptions {
            shell: None,
            interactive_deadline: Duration::from_secs(6),
            login_deadline: Duration::from_secs(4),
            setup_deadline: Duration::from_secs(30),
        }
    }
}

/// One capture: the variables and what to tell the user about them.
#[derive(Debug, Clone)]
pub struct Captured {
    pub vars: Vec<(String, String)>,
    pub report: HostEnvironment,
}

/// What the stage holds between runs.
#[derive(Default)]
struct Stage {
    captured: Option<Captured>,
    config: HostEnvConfig,
    revision: Option<String>,
    report: Option<HostEnvironment>,
}

/// The host's environment stage (see the module docs). Cheap to clone; runs are serialised.
#[derive(Clone)]
pub struct HostEnv {
    opts: CaptureOptions,
    stage: Arc<Mutex<Stage>>,
    running: Arc<tokio::sync::Mutex<()>>,
    commands: AgentCommands,
}

impl HostEnv {
    pub fn new(opts: CaptureOptions) -> Self {
        HostEnv {
            opts,
            stage: Arc::new(Mutex::new(Stage::default())),
            running: Arc::new(tokio::sync::Mutex::new(())),
            commands: AgentCommands::default(),
        }
    }

    /// The agent commands this stage installs; give them to the runtime configuration
    /// (`RuntimeConfig::commands`) so discovery and sessions use them.
    pub fn commands(&self) -> AgentCommands {
        self.commands.clone()
    }

    fn stage(&self) -> std::sync::MutexGuard<'_, Stage> {
        // Fields are replaced whole; a poisoned stage is still consistent.
        self.stage.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The last report (`None` before the first capture).
    pub fn report(&self) -> Option<HostEnvironment> {
        self.stage().report.clone()
    }

    /// Capture again, apply the configuration on top and install the result as the base
    /// environment of new processes. Concurrent calls run one after the other.
    pub async fn refresh(&self) -> HostEnvironment {
        let _g = self.running.lock().await;
        let c = capture(&self.opts).await;
        tracing::info!(
            source = ?c.report.source,
            ms = c.report.duration_ms,
            "captured the user's environment"
        );
        self.stage().captured = Some(c);
        self.apply().await
    }

    /// Apply `config` (setup commands, variables, agent commands) on top of the captured
    /// environment; captures first if nothing was captured yet. A configuration equal to the
    /// one in effect only records `revision`, unless `again` (the setup runs again: its
    /// outcome may differ, e.g. after a module was installed). Returns the report and whether
    /// the environment or the commands were applied anew (so agents should be detected again).
    pub async fn configure(
        &self,
        config: HostEnvConfig,
        revision: String,
        again: bool,
    ) -> (HostEnvironment, bool) {
        let _g = self.running.lock().await;
        let captured = {
            let mut guard = self.stage();
            let st = &mut *guard;
            if !again && st.config == config {
                if let Some(r) = st.report.as_mut() {
                    st.revision = Some(revision.clone());
                    r.revision = Some(revision);
                    return (r.clone(), false);
                }
            }
            st.config = config;
            st.revision = Some(revision);
            st.captured.is_some()
        };
        if !captured {
            let c = capture(&self.opts).await;
            self.stage().captured = Some(c);
        }
        (self.apply().await, true)
    }

    /// Run the configuration on the captured environment and install the result. The caller
    /// holds `running`.
    async fn apply(&self) -> HostEnvironment {
        let (captured, config, revision) = {
            let st = self.stage();
            let Some(c) = st.captured.clone() else {
                unreachable!("apply runs after a capture");
            };
            (c, st.config.clone(), st.revision.clone())
        };
        let secrets = config.secrets();
        let mut vars = captured.vars.clone();
        let mut report = captured.report.clone();
        if !config.setup.trim().is_empty() {
            let started = Instant::now();
            let (after, mut s) = run_setup(&self.opts, &captured, &config.setup).await;
            s.duration_ms = started.elapsed().as_millis() as u64;
            s.output = scrub(&s.output, &secrets);
            s.error = s.error.map(|e| scrub(&e, &secrets));
            if let Some(after) = after {
                vars = merge_own(after, &captured.vars);
            }
            tracing::info!(
                ok = s.ok,
                ms = s.duration_ms,
                "ran the host's setup commands"
            );
            report.setup = Some(s);
        }
        for v in &config.vars {
            match vars.iter_mut().find(|(k, _)| same_name(k, &v.name)) {
                Some((_, value)) => *value = v.value.clone(),
                None => vars.push((v.name.clone(), v.value.clone())),
            }
        }
        report.notes = report.notes.iter().map(|n| scrub(n, &secrets)).collect();
        report.path = path_of(&vars);
        report.variables = config.vars.iter().map(|v| v.name.clone()).collect();
        report.commands = config.commands.clone();
        report.revision = revision;
        wasit_process::set_base_env(Some(
            vars.iter().map(|(k, v)| (k.into(), v.into())).collect(),
        ));
        self.commands.set(config.commands.clone());
        self.stage().report = Some(report.clone());
        report
    }
}

fn same_name(a: &str, b: &str) -> bool {
    if cfg!(windows) {
        a.eq_ignore_ascii_case(b)
    } else {
        a == b
    }
}

/// `PATH` of `vars`, one entry per element.
fn path_of(vars: &[(String, String)]) -> Vec<String> {
    vars.iter()
        .find(|(k, _)| same_name(k, "PATH"))
        .map(|(_, v)| {
            std::env::split_paths(v)
                .map(|p| p.display().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// `text` with every secret value replaced.
pub fn scrub(text: &str, secrets: &[&str]) -> String {
    let mut out = text.to_string();
    for s in secrets {
        if !s.is_empty() {
            out = out.replace(s, "••••");
        }
    }
    out
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
    let path = path_of(&vars);
    Captured {
        report: HostEnvironment {
            source,
            shell,
            captured_at_ms: crate::store::now_ms(),
            duration_ms: started.elapsed().as_millis() as u64,
            path,
            notes,
            revision: None,
            setup: None,
            variables: Vec::new(),
            commands: Default::default(),
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
    let same = same_name;
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

/// What a bounded run produced (also when the deadline ended it).
struct Ran {
    out: String,
    err: String,
    /// Exit code (`None`: killed by a signal, or still running at the deadline).
    code: Option<i32>,
    /// The deadline passed and the process (group) was killed.
    timed_out: bool,
}

/// Run `program args…` with stdin closed and a deadline, in `env` (else this process's own
/// environment). On the deadline the process group (Unix) or process is killed and what it
/// printed so far is returned.
async fn run_bounded(
    program: &std::path::Path,
    args: &[String],
    env: Option<&[(String, String)]>,
    deadline: Duration,
) -> Result<Ran, String> {
    use std::process::Stdio;
    let mut cmd = wasit_process::command_inheriting(program);
    if let Some(env) = env {
        cmd.env_clear().envs(env.iter().map(|(k, v)| (k, v)));
    }
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
    let out = Arc::new(Mutex::new(Vec::new()));
    let err = Arc::new(Mutex::new(Vec::new()));
    let pump = |src: Option<Box<dyn tokio::io::AsyncRead + Unpin + Send>>,
                dst: Arc<Mutex<Vec<u8>>>| async move {
        let Some(mut src) = src else { return };
        let mut buf = [0u8; 8192];
        // A read error ends the stream like EOF; what arrived so far is kept.
        while let Ok(n) = src.read(&mut buf).await {
            if n == 0 {
                break;
            }
            dst.lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend_from_slice(&buf[..n]);
        }
    };
    let so = child
        .stdout
        .take()
        .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Unpin + Send>);
    let se = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Unpin + Send>);
    let readers = futures::future::join(pump(so, out.clone()), pump(se, err.clone()));
    let work = async {
        readers.await;
        child.wait().await
    };
    let (code, timed_out) = match tokio::time::timeout(deadline, work).await {
        Ok(Ok(st)) => (st.code(), false),
        Ok(Err(e)) => return Err(format!("waiting for {}: {e}", program.display())),
        Err(_) => {
            #[cfg(unix)]
            if let Some(p) = pid {
                // The shell and whatever its startup files started (it is the group leader).
                wasit_process::kill_process_group(p);
            }
            #[cfg(not(unix))]
            let _ = pid;
            // `kill_on_drop` ends the process itself when `child` goes out of scope.
            (None, true)
        }
    };
    let text = |b: &Arc<Mutex<Vec<u8>>>| {
        String::from_utf8_lossy(&b.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    };
    Ok(Ran {
        out: text(&out),
        err: text(&err),
        code,
        timed_out,
    })
}

/// The last few lines of `s` (for notes).
fn tail(s: &str) -> String {
    tail_lines(s, 5, SETUP_TAIL_BYTES)
}

/// The last `lines` non-blank lines of `s`, at most `bytes` long.
fn tail_lines(s: &str, lines: usize, bytes: usize) -> String {
    let all: Vec<&str> = s.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut t = all[all.len().saturating_sub(lines)..].join("\n");
    if t.len() > bytes {
        let mut cut = t.len() - bytes;
        while !t.is_char_boundary(cut) {
            cut += 1;
        }
        t = format!("…{}", &t[cut..]);
    }
    t
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
        match run_bounded(&shell, &args, None, deadline).await {
            Ok(r) if r.timed_out => notes.push(format!(
                "`{what}` did not finish within {} s",
                deadline.as_secs_f32()
            )),
            Ok(r) => match between(&r.out, &begin, &end).map(parse_env) {
                Some(vars) if vars.iter().any(|(k, _)| k == "PATH") => {
                    return Some((source, Some(shell.display().to_string()), vars));
                }
                _ => {
                    let err = tail(&r.err);
                    notes.push(format!(
                        "`{what}` printed no environment{}",
                        if err.is_empty() {
                            String::new()
                        } else {
                            format!(": {err}")
                        }
                    ))
                }
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
    match run_bounded(&ps, &args, None, opts.interactive_deadline).await {
        Ok(r) if r.timed_out => notes.push(format!(
            "powershell did not finish within {} s",
            opts.interactive_deadline.as_secs_f32()
        )),
        Ok(Ran { out, err, .. }) => match between(&out, &begin, &end).map(parse_env) {
            Some(reg) if reg.iter().any(|(k, _)| k.eq_ignore_ascii_case("PATH")) => {
                // This process's variables win (PowerShell's own changes are not taken); the
                // registry adds what it lacks: variables set since login and PATH entries.
                let own: Vec<(String, String)> = std::env::vars_os()
                    .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                    .collect();
                return Some((EnvSource::Registry, None, merge_own(own, &reg)));
            }
            _ => notes.push(format!(
                "the registry environment could not be read: {}",
                tail(&err)
            )),
        },
        Err(e) => notes.push(format!("powershell {e}")),
    }
    notes.push("using the daemon's own environment".into());
    None
}

// ── setup commands ─────────────────────────────────────────────────────────────────────────

/// Unique markers around the setup's output and the environment it leaves.
struct Markers {
    begin: String,
    end: String,
    env_end: String,
}

impl Markers {
    fn new() -> Self {
        let n = nonce();
        Markers {
            begin: format!("__WASIT_SETUP_BEGIN_{n}__"),
            end: format!("__WASIT_SETUP_END_{n}__"),
            env_end: format!("__WASIT_SETUP_ENV_{n}__"),
        }
    }

    /// Read a run: the environment the setup left (when it finished) and the report.
    fn read(
        &self,
        r: &Ran,
        shell: String,
        deadline: Duration,
    ) -> (Option<Vec<(String, String)>>, SetupReport) {
        let mut rep = SetupReport {
            ok: false,
            shell,
            exit_code: r.code,
            error: None,
            output: String::new(),
            applied: false,
            duration_ms: 0,
        };
        let err_tail = tail_lines(&r.err, SETUP_TAIL_LINES, SETUP_TAIL_BYTES);
        let unfinished = |what: String| {
            if r.timed_out {
                format!("did not finish within {} s", deadline.as_secs_f32())
            } else {
                what
            }
        };
        let Some(b) = r.out.find(&self.begin) else {
            rep.error = Some(unfinished(format!(
                "the shell did not run the commands (exit status {})",
                code_text(r.code)
            )));
            rep.output = err_tail;
            return (None, rep);
        };
        let after = &r.out[b + self.begin.len()..];
        let Some(e) = after.find(&self.end) else {
            rep.error = Some(unfinished(format!(
                "the commands ended the shell (exit status {})",
                code_text(r.code)
            )));
            rep.output = tail_lines(
                &format!("{after}\n{err_tail}"),
                SETUP_TAIL_LINES,
                SETUP_TAIL_BYTES,
            );
            return (None, rep);
        };
        rep.output = tail_lines(&after[..e], SETUP_TAIL_LINES, SETUP_TAIL_BYTES);
        let rest = &after[e + self.end.len()..];
        let rc: Option<i32> = rest.lines().next().and_then(|l| l.trim().parse().ok());
        rep.exit_code = rc;
        let vars = rest
            .find(&self.env_end)
            .map(|f| parse_env(rest[..f].split_once('\n').map(|x| x.1).unwrap_or("")))
            .filter(|v| v.iter().any(|(k, _)| same_name(k, "PATH")));
        match (&vars, rc) {
            (None, _) => {
                rep.error = Some(unfinished(
                    "the shell printed no environment after the commands".into(),
                ))
            }
            (Some(_), Some(0)) => rep.ok = true,
            (Some(_), rc) => {
                rep.error = Some(format!(
                    "the last command exited with status {}",
                    code_text(rc)
                ))
            }
        }
        rep.applied = vars.is_some();
        (vars, rep)
    }
}

fn code_text(c: Option<i32>) -> String {
    c.map(|c| c.to_string())
        .unwrap_or_else(|| "unknown: killed by a signal".into())
}

/// Run the setup commands in the captured environment (see the module docs, stage 2).
#[cfg(unix)]
async fn run_setup(
    opts: &CaptureOptions,
    captured: &Captured,
    setup: &str,
) -> (Option<Vec<(String, String)>>, SetupReport) {
    let login = opts.shell.clone().unwrap_or_else(login_shell);
    let name = login
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mode: &[&str] = match captured.report.source {
        EnvSource::LoginInteractiveShell => &["-l", "-i"],
        EnvSource::LoginShell => &["-l"],
        _ => &[],
    };
    let m = Markers::new();
    let (shell, flags, script) = match name.as_str() {
        "fish" => (
            login.clone(),
            mode,
            format!(
                "printf '\\n%s\\n' '{b}'\nbegin\n{setup}\nend </dev/null 2>&1\nset __wasit_rc $status\n\
                 printf '\\n%s %s\\n' '{e}' $__wasit_rc\nenv -0 2>/dev/null; or env\nprintf '\\n%s\\n' '{f}'\n",
                b = m.begin,
                e = m.end,
                f = m.env_end
            ),
        ),
        // Not POSIX shells: the lines run with `/bin/sh` (in the captured environment).
        "csh" | "tcsh" | "nu" | "elvish" | "xonsh" | "pwsh" => {
            (PathBuf::from("/bin/sh"), &[][..], posix_script(&m, setup))
        }
        _ => (login.clone(), mode, posix_script(&m, setup)),
    };
    let mut args: Vec<String> = flags.iter().map(|f| f.to_string()).collect();
    let what = std::iter::once(shell.display().to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ");
    args.push("-c".into());
    args.push(script);
    match run_bounded(&shell, &args, Some(&captured.vars), opts.setup_deadline).await {
        Ok(r) => m.read(&r, what, opts.setup_deadline),
        Err(e) => (None, start_failed(what, e)),
    }
}

#[cfg(unix)]
fn posix_script(m: &Markers, setup: &str) -> String {
    format!(
        "printf '\\n%s\\n' '{b}'\n{{\n{setup}\n}} </dev/null 2>&1\n__wasit_rc=$?\n\
         printf '\\n%s %s\\n' '{e}' \"$__wasit_rc\"\nenv -0 2>/dev/null || env\nprintf '\\n%s\\n' '{f}'\n",
        b = m.begin,
        e = m.end,
        f = m.env_end
    )
}

/// Run the setup commands with PowerShell in the captured environment.
#[cfg(windows)]
async fn run_setup(
    opts: &CaptureOptions,
    captured: &Captured,
    setup: &str,
) -> (Option<Vec<(String, String)>>, SetupReport) {
    let m = Markers::new();
    let script = format!(
        r#"$__o=[Console]::Out
$__o.Write("`n{b}`n")
$global:LASTEXITCODE=0
$__rc=0
try {{ & {{
{setup}
}} *>&1 | Out-String -Stream | ForEach-Object {{ $__o.WriteLine($_) }}; if ($LASTEXITCODE) {{ $__rc=$LASTEXITCODE }} }} catch {{ $__o.WriteLine(($_ | Out-String)); $__rc=1 }}
$__o.Write("`n{e} $__rc`n")
foreach($__v in [Environment]::GetEnvironmentVariables('Process').GetEnumerator()){{$__o.Write("$($__v.Key)=$($__v.Value)`0")}}
$__o.Write("`n{f}`n")"#,
        b = m.begin,
        e = m.end,
        f = m.env_end
    );
    let args = vec![
        "-NoProfile".to_string(),
        "-NonInteractive".into(),
        "-Command".into(),
        script,
    ];
    let what = "powershell -NoProfile".to_string();
    let ps = PathBuf::from("powershell.exe");
    match run_bounded(&ps, &args, Some(&captured.vars), opts.setup_deadline).await {
        Ok(r) => m.read(&r, what, opts.setup_deadline),
        Err(e) => (None, start_failed(what, e)),
    }
}

fn start_failed(shell: String, e: String) -> SetupReport {
    SetupReport {
        ok: false,
        shell,
        exit_code: None,
        error: Some(e),
        output: String::new(),
        applied: false,
        duration_ms: 0,
    }
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
