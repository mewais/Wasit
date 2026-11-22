//! Runtime configuration: where data lives, how shims are launched, deadlines, and per-driver
//! overrides.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use wasit_core::{DriverId, DriverRegistry, SessionMode};
use wasit_shim::ShimOptions;

/// How to start a shim process.
///
/// The shim is not a separate binary in a product install: any executable whose entry point
/// routes `<program> <args…> <shim args…>` to [`wasit_shim::run`] works — e.g. the
/// `wasit-shim` binary (`args` empty), or a host binary with a `shim` subcommand
/// (`args = ["shim"]`; see [`ShimLauncher::subcommand`]). On Unix the shim re-executes its own
/// executable with the **same argv** (plus `WASIT_SHIM_ROLE=guard` in the environment) to start
/// its process-group guard, so the subcommand routing must also accept that re-exec — it does
/// automatically when `main` dispatches on the subcommand before doing anything else and hands
/// the remaining arguments to `wasit_shim::run`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShimLauncher {
    pub program: PathBuf,
    /// Arguments placed before the shim's own arguments.
    pub args: Vec<OsString>,
}

impl ShimLauncher {
    /// A dedicated shim binary.
    pub fn binary(program: impl Into<PathBuf>) -> Self {
        ShimLauncher {
            program: program.into(),
            args: Vec::new(),
        }
    }

    /// `<program> <subcommand> <shim args…>`.
    pub fn subcommand(program: impl Into<PathBuf>, subcommand: impl Into<OsString>) -> Self {
        ShimLauncher {
            program: program.into(),
            args: vec![subcommand.into()],
        }
    }

    /// The running executable with a subcommand (the usual choice for a host daemon).
    pub fn current_exe(subcommand: impl Into<OsString>) -> std::io::Result<Self> {
        Ok(Self::subcommand(std::env::current_exe()?, subcommand))
    }
}

/// Deadlines and graces owned by session actors and the manager.
#[derive(Debug, Clone)]
pub struct Timeouts {
    /// From the `start` of a process generation to a completed handshake; past it the process
    /// is killed and the session is `Failed`.
    pub handshake: Duration,
    /// From accepting a command to the shim journaling it; past it the requester gets
    /// `Timeout` (the command may still be applied later and then shows in the event stream).
    pub command: Duration,
    /// Graceful stop: after the `Shutdown` command, wait this long before closing stdin, then
    /// this long again before `TERM`, then again before `KILL`.
    pub stop_grace: Duration,
    /// Waiting for room in a session's mailbox.
    pub mailbox: Duration,
    /// `delete` waits at most this long for the process to exit.
    pub delete: Duration,
    /// Reconnect attempts to a live shim before the session is considered lost.
    pub reconnect_attempts: u32,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            handshake: Duration::from_secs(90),
            command: Duration::from_secs(30),
            stop_grace: Duration::from_secs(3),
            mailbox: Duration::from_secs(2),
            delete: Duration::from_secs(30),
            reconnect_attempts: 5,
        }
    }
}

/// Per-driver launch overrides (binary path and extra environment), applied to sessions and to
/// discovery alike.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriverOverride {
    pub binary: Option<String>,
    pub env: Vec<(String, String)>,
}

/// The per-host agent command of each driver (a name looked up on `PATH`, or a path), set by
/// the host's environment stage ([`crate::environment::HostEnv::configure`]) and read at use
/// time by discovery and session launches, so a change applies to the next probe and the next
/// session. It is the last stage of the host environment (ARCHITECTURE.md §3.5) and wins over
/// a [`DriverOverride::binary`]. The command stands in for the CLI: same driver, same
/// arguments.
#[derive(Clone, Default)]
pub struct AgentCommands(std::sync::Arc<std::sync::RwLock<BTreeMap<DriverId, String>>>);

impl AgentCommands {
    pub fn get(&self, driver: &DriverId) -> Option<String> {
        let m = self.0.read().unwrap_or_else(|e| e.into_inner());
        m.get(driver).cloned()
    }

    pub fn all(&self) -> BTreeMap<DriverId, String> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn set(&self, commands: BTreeMap<DriverId, String>) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = commands;
    }
}

/// Modes a driver's CLI offers as discovery found them, shared by [`crate::Discovery`] and the
/// sessions: a CLI whose protocol reports only the current mode (claude's permission modes)
/// still shows its choices.
#[derive(Clone, Default)]
pub struct ModeHints(std::sync::Arc<std::sync::RwLock<BTreeMap<DriverId, Vec<SessionMode>>>>);

impl ModeHints {
    pub fn get(&self, driver: &DriverId) -> Vec<SessionMode> {
        let m = self.0.read().unwrap_or_else(|e| e.into_inner());
        m.get(driver).cloned().unwrap_or_default()
    }

    pub fn set(&self, driver: &DriverId, modes: Vec<SessionMode>) {
        if modes.is_empty() {
            return;
        }
        let mut m = self.0.write().unwrap_or_else(|e| e.into_inner());
        m.insert(driver.clone(), modes);
    }
}

/// Everything a [`crate::SessionManager`] needs.
#[derive(Clone)]
pub struct RuntimeConfig {
    /// Root of the durable data layout (`<root>/sessions/<id>/…`: `session.json`, journal,
    /// blobs, attachments). May be on a filesystem shared between machines when `run_dir` is
    /// set.
    pub data_dir: PathBuf,
    /// Machine-local root for each session's `shim.lock` and control socket
    /// (`<run>/sessions/<id>/`); `None` keeps them in the session's data directory. Keep it
    /// short: Unix socket paths longer than ~104 bytes cannot be bound, and
    /// [`crate::SessionManager::new`] refuses a root that would exceed that.
    pub run_dir: Option<PathBuf>,
    pub launcher: ShimLauncher,
    pub registry: DriverRegistry,
    pub timeouts: Timeouts,
    pub shim: ShimOptions,
    pub overrides: BTreeMap<DriverId, DriverOverride>,
    /// Per-host agent commands (shared with discovery and the host environment stage).
    pub commands: AgentCommands,
    /// In-memory tail of events kept per session for subscribers and history.
    pub event_tail: usize,
    /// Per-subscriber queue; a subscriber that falls this far behind is dropped (`Lagged`).
    pub subscriber_queue: usize,
    /// Filled by a [`crate::Discovery`] made with [`crate::DiscoveryConfig::from_runtime`].
    pub mode_hints: ModeHints,
}

impl RuntimeConfig {
    pub fn new(data_dir: impl Into<PathBuf>, launcher: ShimLauncher) -> Self {
        RuntimeConfig {
            data_dir: data_dir.into(),
            run_dir: None,
            launcher,
            registry: crate::default_registry(),
            timeouts: Timeouts::default(),
            shim: ShimOptions::default(),
            overrides: BTreeMap::new(),
            commands: AgentCommands::default(),
            event_tail: 2048,
            subscriber_queue: 1024,
            mode_hints: ModeHints::default(),
        }
    }

    pub(crate) fn sessions_dir(&self) -> PathBuf {
        self.data_dir.join("sessions")
    }

    /// Root of the per-session runtime directories.
    pub(crate) fn run_sessions_dir(&self) -> PathBuf {
        self.run_dir
            .as_deref()
            .unwrap_or(&self.data_dir)
            .join("sessions")
    }

    /// Where new shims of the session whose data directory is `dir` keep their lock and
    /// endpoint.
    pub fn session_run_dir_for_spawn(&self, dir: &Path) -> PathBuf {
        match (&self.run_dir, dir.file_name()) {
            (Some(r), Some(id)) => r.join("sessions").join(id),
            _ => dir.to_path_buf(),
        }
    }

    /// The runtime directory (lock + endpoint) of the session whose data directory is `dir`:
    /// [`RuntimeConfig::session_run_dir_for_spawn`], except for a shim started by a build that
    /// kept its lock in the data directory (adopted from an older layout), until the session's
    /// next process generation.
    pub fn session_run_dir(&self, dir: &Path) -> PathBuf {
        let run = self.session_run_dir_for_spawn(dir);
        if run != dir
            && !run.join(wasit_shim::LOCK_FILE).exists()
            && dir.join(wasit_shim::LOCK_FILE).exists()
        {
            return dir.to_path_buf();
        }
        run
    }

    pub(crate) fn override_for(&self, id: &DriverId) -> DriverOverride {
        self.overrides.get(id).cloned().unwrap_or_default()
    }
}
