//! wasit-runtime — the session runtime (ARCHITECTURE.md §3.3).
//!
//! * [`default_registry`] — the one place that lists every driver.
//! * [`SessionManager`] — per-daemon owner of all sessions under a data directory: create,
//!   continue, stop, delete, list, subscribe, history, re-adoption on start.
//! * One `SessionActor` (tokio task) per session owns its shim connection and protocol state
//!   and derives a [`wire::SessionSnapshot`] and a [`wire::SessionEvent`] stream from the
//!   journal; see the `actor` and `core` module docs for the replay-exactness design.
//! * [`Discovery`] — runs the drivers' discovery probes with deadlines and publishes a
//!   [`wire::DriverInventory`] per driver.
//! * [`trail`] — the content-free projection of a session's events (`session.trail:<id>`,
//!   `session.trail.page`).
//! * [`DiagService`] — this process's own diagnostics (`diag.sources`, `diag.read`, topic
//!   `diag.tail`), mounted next to the runtime service on [`wire::DIAG_ROUTES`].
//! * [`RuntimeService`] — the [`wasit_control::Service`] serving all of the above; [`wire`]
//!   holds every payload type and [`RuntimeClient`] is the typed client.

#![forbid(unsafe_code)]

mod actor;
mod client;
mod config;
mod continuation;
mod core;
#[cfg(test)]
mod core_earlier_tests;
mod cwd;
mod diag;
mod diag_service;
mod discovery;
pub mod environment;
#[allow(dead_code)] // R2: event log not yet wired to actor/manager
mod eventlog;
mod export;
mod history;
mod host;
pub mod machine;
mod manager;
pub mod media;
mod note;
#[allow(dead_code)] // B5a: the export job (B5b) is not yet wired to the redactor
mod redact;
mod replay;
mod scrub;
mod search;
mod service;
#[doc(hidden)]
pub mod stats;
mod store;
mod stream;
mod subscribe;
pub mod trail;
pub mod wire;

pub use client::{ClientError, RuntimeClient, TypedSubscription};
pub use config::{AgentCommands, DriverOverride, ModeHints, RuntimeConfig, ShimLauncher, Timeouts};
pub use diag_service::DiagService;
pub use discovery::{Discovery, DiscoveryConfig};
pub use history::HistoryConfig;
pub use manager::{
    AdoptReport, HostEventStream, SessionEventStream, SessionManager, MAX_HISTORY_PAGE,
};
pub use service::RuntimeService;
pub use stream::{EventStream, StreamEnd};

use wasit_core::DriverRegistry;

/// Every driver this runtime ships, in a stable order: claude, codex, opencode, kilo. The only
/// place outside the driver crates that names drivers.
pub fn default_registry() -> DriverRegistry {
    let mut r = DriverRegistry::new();
    for d in [
        wasit_claude::descriptor(),
        wasit_codex::descriptor(),
        wasit_opencode::opencode(),
        wasit_opencode::kilo(),
    ] {
        if let Err(e) = r.register(d) {
            // Ids are distinct constants of the driver crates; a duplicate is a build bug.
            diag::registry_failed(&e);
        }
    }
    r
}

/// Runs the host subcommand `subcommand` if a driver of this runtime launches its CLI through it
/// (see [`wasit_core::Wrapper`]). `args` are the arguments after the subcommand; a leading `--` is
/// dropped and the rest is the CLI argv. Returns the wrapper's exit code, or `None` when no driver
/// has that subcommand, so the host goes on with its own. A host binary calls it first in `main`,
/// as it does for `shim`.
pub fn run_wrapper(subcommand: &str, args: &[String]) -> Option<std::process::ExitCode> {
    let wrapper = default_registry()
        .iter()
        .find_map(|d| d.wrapper().filter(|w| w.subcommand == subcommand))?;
    let cli = match args.first().map(String::as_str) {
        Some("--") => &args[1..],
        _ => args,
    };
    let code = (wrapper.main)(cli);
    Some(std::process::ExitCode::from(
        u8::try_from(code.clamp(0, 255)).unwrap_or(1),
    ))
}

/// Test support for the derived event log (not part of the API).
#[doc(hidden)]
pub mod testing {
    use std::path::Path;

    use crate::config::RuntimeConfig;
    use crate::eventlog::EventLog;

    /// Rebuilds the derived event log of the session in `session_dir` offline, into
    /// `out_dir/derived/`, keyed like the runtime keys its own logs (`cfg.derivation`). Returns
    /// once the log is sealed and its writer has finished.
    pub async fn build_event_log(
        cfg: &RuntimeConfig,
        session_dir: &Path,
        out_dir: &Path,
    ) -> Result<(), String> {
        let file = crate::store::read(session_dir).await?;
        let desc = cfg.registry.get(&file.driver).map_err(|e| e.to_string())?;
        let meta = file.meta();
        let hints = cfg.mode_hints.get(&meta.driver);
        let run_dir = cfg.session_run_dir(session_dir);
        let journal_len = tokio::fs::metadata(session_dir.join("journal.ndjson"))
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        let mut log = EventLog::open(out_dir, cfg.derivation.clone(), journal_len, None)
            .await
            .map_err(|e| e.to_string())?;
        let built = crate::replay::build(session_dir, &run_dir, desc, &meta, &hints, &log).await;
        // Dropping the log closes its queue; the writer then finishes every queued write.
        let writer = log.take_writer();
        drop(log);
        if let Some(w) = writer {
            w.await.map_err(|e| format!("log writer: {e}"))?;
        }
        built.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn registry_lists_every_driver() {
        let ids: Vec<String> = super::default_registry()
            .ids()
            .into_iter()
            .map(|i| i.0)
            .collect();
        assert_eq!(ids, ["claude", "codex", "opencode", "kilo"]);
    }
}
