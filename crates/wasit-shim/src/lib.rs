//! `wasit-shim`: the per-session process holder (ARCHITECTURE.md §3.2).
//!
//! One shim process owns exactly one CLI child. It journals every stdout/stderr line, every
//! line written to stdin and the lifecycle of the child to `<dir>/journal.ndjson`, and serves a
//! single controller over a local endpoint (`<run>/ctl.sock` on Unix,
//! `\\.\pipe\wasit-<basename(run)>` on Windows) while holding `<run>/shim.lock`. `<run>` is
//! the machine-local runtime directory (`--run`), defaulting to `<dir>`; `<dir>` may be on a
//! filesystem shared between machines, `<run>` must not. The shim knows nothing about any CLI protocol.
//!
//! * [`run`] is the process entry point (used by the `wasit-shim` binary and by any host
//!   binary's `shim` subcommand).
//! * [`spawn_detached`] launches a fully detached shim and returns once it is ready.
//! * [`ShimClient`] is the controller side; [`read_journal`] reads a journal whose shim is gone;
//!   [`probe`] tells whether a shim is alive without blocking.

mod cli;
mod client;
mod diag;
mod endpoint;
mod error;
mod journal;
mod probe;
mod record;
mod server;
mod spawn;
pub mod wire;

#[cfg(unix)]
mod proc_unix;
#[cfg(unix)]
use proc_unix as platform;
#[cfg(windows)]
mod proc_windows;
#[cfg(windows)]
use proc_windows as platform;

pub use client::{read_journal, ClientOptions, RecordStream, ShimClient};
pub use error::Error;
pub use probe::{probe, probe_at, ShimState};
pub use record::{ExitInfo, Kind, Record};
pub use server::run;
pub use spawn::{spawn_detached, ShimHandle, ShimOptions, ShimSpec};
pub use wire::Sig;

/// File name of the journal inside a session directory.
pub const JOURNAL_FILE: &str = "journal.ndjson";
/// File name of the liveness lock inside a session directory.
pub const LOCK_FILE: &str = "shim.lock";
/// File name of the shim's own diagnostic log: NDJSON diagnostic records (`wasit-diag`) that
/// the shim process writes itself and rotates at 256 KiB, keeping `shim.log.1`.
pub const LOG_FILE: &str = "shim.log";
/// File name of the shim's stderr: raw crash output of the process, a few bytes at most, never
/// exported. [`spawn_detached`] opens it; a new shim cuts it to nothing when it is over 64 KiB.
pub const STDERR_FILE: &str = "shim.stderr";

/// Path of the control endpoint for a session directory: the Unix socket path, or the Windows
/// named-pipe name.
pub fn endpoint_name(dir: &std::path::Path) -> std::path::PathBuf {
    endpoint::name(dir)
}
