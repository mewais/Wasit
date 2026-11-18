//! Runtime configuration: where data lives, how shims are launched, deadlines, and per-driver
//! overrides.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use wasit_core::{DriverId, DriverRegistry};
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

/// Everything a [`crate::SessionManager`] needs.
#[derive(Clone)]
pub struct RuntimeConfig {
    /// Root of the data layout (`<root>/sessions/<id>/…`). Keep it short: on Unix the shim's
    /// control socket lives at `<root>/sessions/<id>/ctl.sock`, and socket paths longer than
    /// ~104 bytes cannot be bound; [`crate::SessionManager::new`] refuses a root that would
    /// exceed that.
    pub data_dir: PathBuf,
    pub launcher: ShimLauncher,
    pub registry: DriverRegistry,
    pub timeouts: Timeouts,
    pub shim: ShimOptions,
    pub overrides: BTreeMap<DriverId, DriverOverride>,
    /// In-memory tail of events kept per session for subscribers and history.
    pub event_tail: usize,
    /// Per-subscriber queue; a subscriber that falls this far behind is dropped (`Lagged`).
    pub subscriber_queue: usize,
}

impl RuntimeConfig {
    pub fn new(data_dir: impl Into<PathBuf>, launcher: ShimLauncher) -> Self {
        RuntimeConfig {
            data_dir: data_dir.into(),
            launcher,
            registry: crate::default_registry(),
            timeouts: Timeouts::default(),
            shim: ShimOptions::default(),
            overrides: BTreeMap::new(),
            event_tail: 2048,
            subscriber_queue: 1024,
        }
    }

    pub(crate) fn sessions_dir(&self) -> PathBuf {
        self.data_dir.join("sessions")
    }

    pub(crate) fn override_for(&self, id: &DriverId) -> DriverOverride {
        self.overrides.get(id).cloned().unwrap_or_default()
    }
}
