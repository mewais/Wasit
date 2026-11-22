//! Live discovery: runs each driver's [`DiscoveryProbe`]s (exec / throwaway handshake / file
//! listing) under their own deadlines and publishes a [`DriverInventory`] per driver (and
//! project directory, for session discovery).
//!
//! Results are cached until refreshed. Every probe failure is recorded with its reason in
//! [`DriverInventory::probes`]; a slow or failing probe never blocks another probe, another
//! driver, or anything else in the daemon (each refresh runs in its own task).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures::stream::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::watch;
use wasit_core::{
    AgentEvent, Discovered, DiscoveryContext, DiscoveryKind, DiscoveryProbe, DriverDescriptor,
    DriverId, DriverRegistry, FileHead, ProbeAction, ProbeOutput, SpawnSpec,
};

use crate::config::{DriverOverride, RuntimeConfig};
use crate::store::now_ms;
use crate::stream::{EventStream, StreamEnd};
use crate::wire::{DriverInventory, InventoryReply, ProbeReport};

/// Discovery tunables.
#[derive(Clone)]
pub struct DiscoveryConfig {
    pub registry: DriverRegistry,
    pub overrides: BTreeMap<DriverId, DriverOverride>,
    /// Deadline of an `Exec` probe.
    pub exec_timeout: Duration,
    /// Deadline of a `Handshake` probe.
    pub handshake_timeout: Duration,
    /// Deadline of a `ListFiles` probe.
    pub list_timeout: Duration,
    /// Captured stdout is truncated beyond this.
    pub max_output: usize,
    /// Probes running at once across all drivers and projects (each may start a CLI process;
    /// more wait their turn, and their deadline starts when they run).
    pub max_concurrent_probes: usize,
    /// Discovered modes are shared here with the sessions (see [`crate::config::ModeHints`]).
    pub mode_hints: crate::config::ModeHints,
}

impl DiscoveryConfig {
    pub fn new(registry: DriverRegistry) -> Self {
        DiscoveryConfig {
            registry,
            overrides: BTreeMap::new(),
            exec_timeout: Duration::from_secs(20),
            handshake_timeout: Duration::from_secs(45),
            list_timeout: Duration::from_secs(15),
            max_output: 4 * 1024 * 1024,
            max_concurrent_probes: 4,
            mode_hints: Default::default(),
        }
    }

    /// Registry and overrides of a runtime configuration.
    pub fn from_runtime(cfg: &RuntimeConfig) -> Self {
        let mut d = DiscoveryConfig::new(cfg.registry.clone());
        d.overrides = cfg.overrides.clone();
        d.mode_hints = cfg.mode_hints.clone();
        d
    }
}

type Key = (Option<String>, DriverId);

struct State {
    entries: BTreeMap<Key, DriverInventory>,
}

struct Inner {
    cfg: DiscoveryConfig,
    state: Mutex<State>,
    version: watch::Sender<u64>,
    epoch: String,
    /// Bounds probes running at once ([`DiscoveryConfig::max_concurrent_probes`]).
    probes: Arc<tokio::sync::Semaphore>,
}

/// See the module docs. Cheap to clone.
#[derive(Clone)]
pub struct Discovery {
    inner: Arc<Inner>,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    // Entries are replaced whole; a poisoned map is still consistent.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Discovery {
    pub fn new(cfg: DiscoveryConfig) -> Self {
        let cfg_probes = cfg.max_concurrent_probes.max(1);
        Discovery {
            inner: Arc::new(Inner {
                cfg,
                state: Mutex::new(State {
                    entries: BTreeMap::new(),
                }),
                version: watch::channel(0).0,
                epoch: format!("{:x}", fastrand::u64(..) >> 16),
                probes: Arc::new(tokio::sync::Semaphore::new(cfg_probes)),
            }),
        }
    }

    /// The cached inventory for `project` (every registered driver, registry order). Drivers
    /// never probed for this project start a refresh and are returned with `refreshing: true`.
    pub fn inventory(&self, project: Option<&str>) -> InventoryReply {
        let missing: Vec<DriverId> = {
            let st = lock(&self.inner.state);
            self.inner
                .cfg
                .registry
                .ids()
                .into_iter()
                .filter(|d| {
                    !st.entries
                        .contains_key(&(project.map(str::to_string), d.clone()))
                })
                .collect()
        };
        for d in missing {
            self.refresh_driver(&d, project);
        }
        self.current(project)
    }

    fn current(&self, project: Option<&str>) -> InventoryReply {
        let st = lock(&self.inner.state);
        let drivers = self
            .inner
            .cfg
            .registry
            .ids()
            .into_iter()
            .filter_map(|d| st.entries.get(&(project.map(str::to_string), d)).cloned())
            .collect();
        InventoryReply { drivers }
    }

    /// Re-run the probes of `driver` (all drivers when `None`) for `project`. Returns at once;
    /// results arrive via [`Discovery::changes`] / the subscription.
    pub fn refresh(&self, driver: Option<&DriverId>, project: Option<&str>) -> Result<(), String> {
        match driver {
            Some(d) => {
                self.inner.cfg.registry.get(d).map_err(|e| e.to_string())?;
                self.refresh_driver(d, project);
            }
            None => {
                for d in self.inner.cfg.registry.ids() {
                    self.refresh_driver(&d, project);
                }
            }
        }
        Ok(())
    }

    /// Bumps on every inventory change.
    pub fn changes(&self) -> watch::Receiver<u64> {
        self.inner.version.subscribe()
    }

    /// Every change of `project`'s inventory as a full [`InventoryReply`], starting with the
    /// current one unless `from` is the cursor of the current version. Cursor: `"<epoch>.<n>"`.
    pub fn subscribe(
        &self,
        project: Option<String>,
        from: Option<&str>,
    ) -> EventStream<(String, InventoryReply)> {
        let mut rx = self.inner.version.subscribe();
        let current = *rx.borrow_and_update();
        let epoch = self.inner.epoch.clone();
        let skip_first = from == Some(format!("{epoch}.{current}").as_str());
        if !skip_first {
            // Make the first poll yield the current inventory.
            rx.mark_changed();
        }
        // Make sure there is something to report.
        let _ = self.inventory(project.as_deref());
        let this = self.clone();
        let s = futures::stream::unfold(rx, move |mut rx| {
            let this = this.clone();
            let project = project.clone();
            let epoch = epoch.clone();
            async move {
                match rx.changed().await {
                    Ok(()) => {
                        let v = *rx.borrow_and_update();
                        let inv = this.current(project.as_deref());
                        Some((Ok::<_, StreamEnd>((format!("{epoch}.{v}"), inv)), rx))
                    }
                    Err(_) => None,
                }
            }
        });
        EventStream::new(s.boxed())
    }

    fn update(&self, key: &Key, f: impl FnOnce(&mut DriverInventory)) {
        {
            let mut st = lock(&self.inner.state);
            if let Some(e) = st.entries.get_mut(key) {
                f(e);
            }
        }
        self.inner.version.send_modify(|v| *v += 1);
    }

    fn refresh_driver(&self, driver: &DriverId, project: Option<&str>) {
        let Ok(desc) = self.inner.cfg.registry.get(driver) else {
            return;
        };
        let key: Key = (project.map(str::to_string), driver.clone());
        {
            let mut st = lock(&self.inner.state);
            let e = st
                .entries
                .entry(key.clone())
                .or_insert_with(|| DriverInventory {
                    driver: driver.clone(),
                    display_name: desc.display_name().to_string(),
                    capabilities: desc.capabilities(),
                    installed: false,
                    binary_path: None,
                    error: None,
                    version: None,
                    models: Vec::new(),
                    modes: Vec::new(),
                    sessions: Vec::new(),
                    config: Vec::new(),
                    project: project.map(str::to_string),
                    refreshing: false,
                    probes: Vec::new(),
                    refreshed_at_ms: 0,
                });
            if e.refreshing {
                return;
            }
            e.refreshing = true;
        }
        self.inner.version.send_modify(|v| *v += 1);
        let this = self.clone();
        tokio::spawn(async move { this.run_refresh(desc, key).await });
    }

    async fn run_refresh(&self, desc: Arc<dyn DriverDescriptor>, key: Key) {
        let cfg = &self.inner.cfg;
        let ov = cfg.overrides.get(&key.1).cloned().unwrap_or_default();
        let wanted = ov
            .binary
            .clone()
            .unwrap_or_else(|| desc.default_binary().to_string());
        let resolved = resolve_binary(&wanted).await;
        let Some(path) = resolved else {
            self.update(&key, |e| {
                e.installed = false;
                e.binary_path = None;
                e.error = Some(if Path::new(&wanted).components().count() > 1 {
                    format!("`{wanted}` does not exist or is not executable")
                } else {
                    let path = wasit_process::child_var("PATH")
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    format!("`{wanted}` was not found on PATH (searched: {path})")
                });
                e.refreshing = false;
                e.refreshed_at_ms = now_ms();
                e.probes = vec![];
                e.version = None;
                e.models.clear();
                e.modes.clear();
                e.sessions.clear();
                e.config.clear();
            });
            return;
        };
        let path_s = path.to_string_lossy().into_owned();
        // A folder that is not there (a half-typed path, a deleted project) has nothing to
        // discover: say so at once instead of starting the CLI's probes for it.
        if let Some(project) = &key.0 {
            let is_dir = tokio::fs::metadata(project)
                .await
                .map(|m| m.is_dir())
                .unwrap_or(false);
            if !is_dir {
                let error = format!("project folder {project:?} does not exist");
                // What the folder cannot change (version, models, modes) comes from the
                // host-wide inventory when it has one.
                let global = lock(&self.inner.state)
                    .entries
                    .get(&(None, key.1.clone()))
                    .cloned();
                self.update(&key, |e| {
                    e.installed = true;
                    e.binary_path = Some(path_s.clone());
                    e.error = Some(error);
                    e.refreshing = false;
                    e.refreshed_at_ms = now_ms();
                    e.probes = vec![];
                    e.version = global.as_ref().and_then(|g| g.version.clone());
                    e.models = global
                        .as_ref()
                        .map(|g| g.models.clone())
                        .unwrap_or_default();
                    e.modes = global.as_ref().map(|g| g.modes.clone()).unwrap_or_default();
                    e.sessions.clear();
                    e.config.clear();
                });
                return;
            }
        }
        self.update(&key, |e| {
            e.installed = true;
            e.error = None;
            e.binary_path = Some(path_s.clone());
        });
        let mut env: Vec<(String, String)> = std::env::vars().collect();
        env.extend(ov.env.iter().cloned());
        let ctx = DiscoveryContext {
            binary: Some(path_s),
            // A driver override may point the CLI at another home (tests, sandboxes).
            home: ov
                .env
                .iter()
                .rev()
                .find(|(k, _)| k == "HOME" || k == "USERPROFILE")
                .map(|(_, v)| v.clone())
                .or_else(|| {
                    std::env::var("HOME")
                        .or_else(|_| std::env::var("USERPROFILE"))
                        .ok()
                }),
            env,
            project: key.0.clone(),
        };
        let probes = desc.discovery(&ctx);
        let runs = probes.iter().map(|p| {
            let desc = desc.clone();
            let ov = ov.clone();
            let gate = self.inner.probes.clone();
            async move {
                // The semaphore is never closed, so acquiring only waits for a free slot.
                let _permit = gate.acquire_owned().await;
                let started = Instant::now();
                let r = run_probe(cfg, desc.as_ref(), &ov, p).await;
                (p.kind, r, started.elapsed())
            }
        });
        let results = futures::future::join_all(runs).await;
        self.update(&key, |e| {
            e.probes.clear();
            let mut config: Option<Vec<wasit_core::ConfigItem>> = None;
            for (kind, r, took) in results {
                let duration_ms = took.as_millis() as u64;
                match r {
                    Ok(d) => {
                        match d {
                            Discovered::Version(v) => e.version = Some(v),
                            Discovered::Models(m) => e.models = m,
                            Discovered::Modes(m) => {
                                cfg.mode_hints.set(&key.1, m.clone());
                                e.modes = m
                            }
                            Discovered::Sessions(s) => e.sessions = s,
                            Discovered::Config(c) => config.get_or_insert_with(Vec::new).extend(c),
                        }
                        e.probes.push(ProbeReport {
                            kind,
                            ok: true,
                            error: None,
                            duration_ms,
                        });
                    }
                    Err(err) => e.probes.push(ProbeReport {
                        kind,
                        ok: false,
                        error: Some(err),
                        duration_ms,
                    }),
                }
            }
            // Several probes contribute config items; a refresh replaces them as a whole.
            e.config = config.unwrap_or_default();
            e.refreshing = false;
            e.refreshed_at_ms = now_ms();
        });
    }
}

/// Find `bin` (a path, or a name looked up on `PATH`).
async fn resolve_binary(bin: &str) -> Option<PathBuf> {
    let bin = bin.to_string();
    tokio::task::spawn_blocking(move || {
        let p = Path::new(&bin);
        if p.components().count() > 1 {
            return is_executable(p).then(|| p.to_path_buf());
        }
        // The PATH children get (the user's captured environment), not this process's.
        let path = wasit_process::child_var("PATH")?;
        let exts: Vec<String> = if cfg!(windows) {
            wasit_process::child_var("PATHEXT")
                .and_then(|v| v.into_string().ok())
                .unwrap_or_else(|| ".EXE;.CMD;.BAT".into())
                .split(';')
                .map(str::to_string)
                .chain(std::iter::once(String::new()))
                .collect()
        } else {
            vec![String::new()]
        };
        std::env::split_paths(&path).find_map(|dir| {
            exts.iter().find_map(|ext| {
                let c = dir.join(format!("{bin}{ext}"));
                is_executable(&c).then_some(c)
            })
        })
    })
    .await
    .ok()
    .flatten()
}

fn is_executable(p: &Path) -> bool {
    match std::fs::metadata(p) {
        Ok(m) if m.is_file() => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                m.permissions().mode() & 0o111 != 0
            }
            #[cfg(not(unix))]
            {
                true
            }
        }
        _ => false,
    }
}

async fn run_probe(
    cfg: &DiscoveryConfig,
    desc: &dyn DriverDescriptor,
    ov: &DriverOverride,
    probe: &DiscoveryProbe,
) -> Result<Discovered, String> {
    let output = match &probe.action {
        ProbeAction::Exec { argv, env, cwd } => {
            let mut env = env.clone();
            env.extend(ov.env.iter().cloned());
            tokio::time::timeout(
                cfg.exec_timeout,
                exec(argv, &env, cwd.as_deref(), cfg.max_output),
            )
            .await
            .map_err(|_| {
                format!(
                    "`{}` did not finish within {:?}",
                    argv.join(" "),
                    cfg.exec_timeout
                )
            })??
        }
        ProbeAction::Handshake { spec, until } => {
            let mut spec = (**spec).clone();
            spec.env.extend(ov.env.iter().cloned());
            if spec.cwd.is_none() {
                // A throwaway session may leave traces in its project (e.g. a session record);
                // keep them out of whatever directory the daemon runs in.
                let scratch = std::env::temp_dir().join("wasit-discovery");
                tokio::fs::create_dir_all(&scratch)
                    .await
                    .map_err(|e| format!("cannot create {}: {e}", scratch.display()))?;
                spec.cwd = Some(scratch.to_string_lossy().into_owned());
            }
            handshake(desc, &spec, *until, cfg.handshake_timeout).await?
        }
        ProbeAction::ListFiles {
            dir,
            suffix,
            recursive,
            max_files,
            head_lines,
            count_containing,
        } => {
            let (dir, suffix, recursive, max_files, head_lines, count) = (
                dir.clone(),
                suffix.clone(),
                *recursive,
                *max_files,
                *head_lines,
                count_containing.clone(),
            );
            let job = tokio::task::spawn_blocking(move || {
                list_files(
                    Path::new(&dir),
                    &suffix,
                    recursive,
                    max_files,
                    head_lines,
                    &count,
                )
            });
            match tokio::time::timeout(cfg.list_timeout, job).await {
                Err(_) => {
                    return Err(format!(
                        "listing did not finish within {:?}",
                        cfg.list_timeout
                    ))
                }
                Ok(Err(e)) => return Err(format!("listing failed: {e}")),
                Ok(Ok(Err(e))) => {
                    return Err(format!("cannot list {}: {e}", dir_display(&probe.action)))
                }
                Ok(Ok(Ok(files))) => ProbeOutput::Files { files },
            }
        }
        ProbeAction::ReadFile { path, max_bytes } => {
            let read = read_small_file(path, *max_bytes);
            match tokio::time::timeout(cfg.list_timeout, read).await {
                Err(_) => {
                    return Err(format!(
                        "reading {path} did not finish within {:?}",
                        cfg.list_timeout
                    ))
                }
                Ok(r) => r?,
            }
        }
    };
    (probe.parse)(&output)
}

/// Read at most `max` bytes of `path`; a missing file yields `contents: None`, a larger one
/// is an error (a truncated JSON document would be misread).
async fn read_small_file(path: &str, max: usize) -> Result<ProbeOutput, String> {
    let file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProbeOutput::File {
                path: path.to_string(),
                contents: None,
            })
        }
        Err(e) => return Err(format!("cannot open {path}: {e}")),
    };
    let mut buf = Vec::new();
    file.take(max as u64 + 1)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("cannot read {path}: {e}"))?;
    if buf.len() > max {
        return Err(format!("{path} is larger than {max} bytes"));
    }
    Ok(ProbeOutput::File {
        path: path.to_string(),
        contents: Some(String::from_utf8_lossy(&buf).into_owned()),
    })
}

fn dir_display(a: &ProbeAction) -> String {
    match a {
        ProbeAction::ListFiles { dir, .. } => dir.clone(),
        _ => String::new(),
    }
}

async fn exec(
    argv: &[String],
    env: &[(String, String)],
    cwd: Option<&str>,
    max: usize,
) -> Result<ProbeOutput, String> {
    let (prog, args) = argv.split_first().ok_or("empty probe command")?;
    let mut cmd = wasit_process::command(prog);
    cmd.args(args).envs(env.iter().map(|(k, v)| (k, v)));
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
        // OpenCode / Kilo read their project from `$PWD` (e.g. `session list`).
        #[cfg(unix)]
        cmd.env("PWD", dir);
    }
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("cannot run {prog}: {e}"))?;
    let mut stdout = Vec::new();
    if let Some(out) = child.stdout.take() {
        out.take(max as u64)
            .read_to_end(&mut stdout)
            .await
            .map_err(|e| format!("reading {prog} output: {e}"))?;
    }
    let status = child
        .wait()
        .await
        .map_err(|e| format!("waiting for {prog}: {e}"))?;
    Ok(ProbeOutput::Exec {
        exit_code: status.code(),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
    })
}

/// Drive a throwaway session until an event satisfies `until`, the process exits, or the
/// deadline passes; then kill it.
async fn handshake(
    desc: &dyn DriverDescriptor,
    spec: &SpawnSpec,
    until: DiscoveryKind,
    deadline: Duration,
) -> Result<ProbeOutput, String> {
    let argv = desc.argv(spec);
    let (prog, args) = argv.split_first().ok_or("empty argv")?;
    let mut cmd = wasit_process::command(prog);
    cmd.args(args)
        .envs(desc.env(spec))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(cwd) = &spec.cwd {
        cmd.current_dir(cwd);
        #[cfg(unix)]
        cmd.env("PWD", cwd);
    }
    let mut child = cmd.spawn().map_err(|e| format!("cannot run {prog}: {e}"))?;
    let mut stdin = child.stdin.take();
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let mut lines = BufReader::new(stdout).lines();
    let mut p = desc.new_protocol();
    let mut events: Vec<AgentEvent> = Vec::new();
    let mut step = p.start(spec);
    let result = tokio::time::timeout(deadline, async {
        loop {
            let done = step.events.iter().any(|e| until.satisfied_by(e));
            events.append(&mut step.events);
            if done {
                return Ok(());
            }
            if let Some(w) = stdin.as_mut() {
                for l in &step.write {
                    let mut line = l.0.clone();
                    line.push('\n');
                    if w.write_all(line.as_bytes()).await.is_err() {
                        break;
                    }
                }
                // A failed flush shows up as EOF on stdout next.
                let _ = w.flush().await;
            }
            if step.close_stdin {
                stdin = None;
            }
            match lines.next_line().await {
                Ok(Some(l)) => step = p.on_line(&l),
                Ok(None) => return Ok(()),
                Err(e) => return Err(format!("reading {prog}: {e}")),
            }
        }
    })
    .await;
    // Throwaway process: kill it whatever happened (it may already be gone).
    let _ = child.kill().await;
    match result {
        Ok(Ok(())) => Ok(ProbeOutput::Handshake { events }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(format!(
            "the {} handshake reported no {until:?} within {deadline:?}",
            desc.display_name()
        )),
    }
}

/// The first `head_lines` lines of `p` and, when `count_containing` is non-empty, how many of
/// its lines contain one of those substrings (read to the end, line by line).
fn read_head(
    p: &Path,
    head_lines: usize,
    count_containing: &[String],
) -> (Vec<String>, Option<u64>) {
    use std::io::BufRead;
    let Ok(f) = std::fs::File::open(p) else {
        return (vec![], None);
    };
    let mut r = std::io::BufReader::new(f);
    let mut head = Vec::new();
    let mut count = 0u64;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        match r.read_until(b'\n', &mut buf) {
            Ok(0) => break,
            Ok(_) => {}
            // A file that cannot be read to the end has no trustworthy count.
            Err(_) => return (head, None),
        }
        if head.len() < head_lines {
            let line = String::from_utf8_lossy(&buf);
            head.push(line.trim_end_matches(['\n', '\r']).to_string());
        } else if count_containing.is_empty() {
            break;
        }
        if count_containing.iter().any(|n| memfind(&buf, n.as_bytes())) {
            count += 1;
        }
    }
    (head, (!count_containing.is_empty()).then_some(count))
}

fn memfind(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
}

fn list_files(
    dir: &Path,
    suffix: &str,
    recursive: bool,
    max_files: usize,
    head_lines: usize,
    count_containing: &[String],
) -> std::io::Result<Vec<FileHead>> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let rd = match std::fs::read_dir(&d) {
            Ok(rd) => rd,
            // A CLI that never ran has no session directory yet: nothing to list.
            Err(e) if d == dir && e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) if d == dir => return Err(e),
            Err(_) => continue,
        };
        for ent in rd.flatten() {
            let Ok(ft) = ent.file_type() else { continue };
            let p = ent.path();
            if ft.is_dir() {
                if recursive {
                    stack.push(p);
                }
            } else if p.to_string_lossy().ends_with(suffix) {
                let modified = ent
                    .metadata()
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                found.push((modified, p));
            }
        }
    }
    found.sort_by_key(|f| std::cmp::Reverse(f.0));
    found.truncate(max_files);
    Ok(found
        .into_iter()
        .map(|(modified_ms, p)| {
            let (head, matching_lines) = read_head(&p, head_lines, count_containing);
            FileHead {
                path: p.to_string_lossy().into_owned(),
                modified_ms,
                head,
                matching_lines,
            }
        })
        .collect())
}

#[cfg(all(test, unix))]
mod tests {
    /// A probe run in a directory sees it as `$PWD` too (OpenCode / Kilo take their project
    /// from it), whatever `$PWD` the daemon itself has.
    #[tokio::test]
    async fn exec_sets_pwd_to_the_probe_directory() {
        let dir = std::env::temp_dir();
        let dir = dir.to_string_lossy();
        let argv = ["sh", "-c", "printf %s \"$PWD\""].map(String::from);
        let env = [("PWD".to_string(), "/somewhere/else".to_string())];
        let out = super::exec(&argv, &env, Some(&dir), 4096).await.unwrap();
        match out {
            wasit_core::ProbeOutput::Exec { stdout, .. } => assert_eq!(stdout, dir),
            other => panic!("{other:?}"),
        }
    }
}
