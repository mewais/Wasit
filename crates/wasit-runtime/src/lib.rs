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
mod cwd;
mod diag;
mod diag_service;
mod discovery;
pub mod environment;
mod host;
pub mod machine;
mod manager;
pub mod media;
mod note;
mod replay;
mod scrub;
mod service;
mod store;
mod stream;
pub mod trail;
pub mod wire;

pub use client::{ClientError, RuntimeClient, TypedSubscription};
pub use config::{AgentCommands, DriverOverride, ModeHints, RuntimeConfig, ShimLauncher, Timeouts};
pub use diag_service::DiagService;
pub use discovery::{Discovery, DiscoveryConfig};
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
