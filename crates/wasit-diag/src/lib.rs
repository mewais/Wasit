//! Content-free diagnostics for every Wasit, Wasl and Marsad process.
//!
//! * **What a record may hold** is decided by types: an event is a variant of an enum declared
//!   with [`diag_events!`], and each field's type must implement [`DiagValue`] — numbers,
//!   [`Ms`], [`Bytes`], [`TimeMs`], [`Ident`], [`Code`], [`Fp`], [`ErrorInfo`], [`DiagText`],
//!   [`HomePath`], [`ProjectPath`], [`SrcLocation`], `Option`/`Vec` of those, enums declared with
//!   [`diag_enum!`], and validated newtypes registered with [`diag_value!`]. Strings, paths and
//!   `Debug` output are not values; free text exists only as [`DiagText`], made by
//!   [`DiagText::new`], which scrubs registered secrets and project paths ([`scrub`]).
//! * **What may be served over a wire** under the same rule: [`diag_data!`] declares an enum
//!   with data whose every field is a value type (a projection of something that carries
//!   content, like the session trail), with serde and TypeScript from the one declaration; the
//!   JSON of a value on a wire is the JSON it has in a log record.
//! * **Where records go:** one sink per process ([`install`]); [`emit`] serializes, keeps the
//!   record in a ring for live tails and hands it to one writer thread through a bounded queue.
//!   It never waits; when the queue is full the record is dropped and counted.
//! * **Where a file may be shared:** a short-lived process of a machine (`bridge`, `stop`,
//!   `askpass`, a daemon that lost the lock race) appends to one shared file with
//!   [`FileMode::SharedAppend`]; the daemon keeps it within bounds ([`RotateShared`]).
//! * **Who may read the files:** [`private`] creates and tightens directories and files so
//!   that only their owner can read them (Unix `0700` / `0600`); the sink's files, the shim's
//!   and the runtime's session files all go through it.
//! * The two decisions still open for the product owner live in `policy.rs`, one function
//!   each.
//!
//! The declaration is checked at compile time. A `String` field is refused:
//!
//! ```compile_fail,E0277
//! wasit_diag::diag_events! {
//!     pub enum E { #[diag(ev = "x.y")] V { text: String } }
//! }
//! ```
//!
//! and so is a path, a JSON value or a `Cow<str>`:
//!
//! ```compile_fail,E0277
//! wasit_diag::diag_events! {
//!     pub enum E { #[diag(ev = "x.y")] V { cwd: std::path::PathBuf } }
//! }
//! ```
//!
//! ```compile_fail,E0277
//! wasit_diag::diag_events! {
//!     pub enum E { #[diag(ev = "x.y")] V { raw: serde_json::Value } }
//! }
//! ```
//!
//! ```compile_fail,E0277
//! wasit_diag::diag_events! {
//!     pub enum E { #[diag(ev = "x.y")] V { s: std::borrow::Cow<'static, str> } }
//! }
//! ```
//!
//! A field named like a record header key (`b, seq, t, m, src, lvl, ev`) is refused:
//!
//! ```compile_fail,E0124
//! wasit_diag::diag_events! {
//!     pub enum E { #[diag(ev = "x.y")] V { seq: u64 } }
//! }
//! ```
//!
//! ```compile_fail,E0124
//! wasit_diag::diag_events! {
//!     pub enum E { #[diag(ev = "x.y")] V { session: wasit_diag::Ident, ev: wasit_diag::Code } }
//! }
//! ```
//!
//! while the same declarations with permitted types and names compile:
//!
//! ```
//! wasit_diag::diag_events! {
//!     pub enum E {
//!         #[diag(ev = "x.y")] V { text: wasit_diag::DiagText },
//!         #[diag(ev = "x.z")] W { cwd: wasit_diag::ProjectPath, seqs: u64, event: wasit_diag::Code },
//!     }
//! }
//! ```

#![forbid(unsafe_code)]

mod event;
mod internal;
mod json;
mod macros;
mod panic;
mod policy;
pub mod private;
pub mod reader;
mod ring;
pub mod scrub;
mod sink;
mod text;
#[cfg(feature = "tracing")]
mod tracing_bridge;
mod value;
mod wire;
mod writer;

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

pub use event::{DiagEvent, Level, DEFAULT_BUDGET};
pub use internal::SinkEvent;
pub use panic::{install_panic_hook, PANIC_FLUSH};
pub use reader::{Chunk, GenInfo, Source};
pub use ring::{Filter, Record, RING};
pub use sink::{FileMode, RotateShared, Sink, SinkConfig, Stats, Tail, QUEUE_ENV, WRITER_THREAD};
pub use text::{DiagText, Origin};
#[cfg(feature = "tracing")]
pub use tracing_bridge::{tracing_layer, CountingLayer};
pub use value::{
    Bytes, Code, DiagValue, ErrorInfo, Fp, HomePath, Ident, Ms, PathKind, PathTag, ProjectPath,
    SrcLocation, TimeMs, VEC_MAX,
};
pub use writer::{FsTarget, Target};

/// What the macros expand to. Not an API.
#[doc(hidden)]
pub mod __private {
    pub use crate::event::{valid_ev, Budget, Fields, SealedEvent};

    pub use crate::json::Json;
    pub use crate::value::Sealed;

    /// `a == b`, usable in a `const`.
    pub const fn same_str(a: &str, b: &str) -> bool {
        let (a, b) = (a.as_bytes(), b.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        let mut i = 0;
        while i < a.len() {
            if a[i] != b[i] {
                return false;
            }
            i += 1;
        }
        true
    }

    /// Writes a `diag_enum!` variant name as its `snake_case` code.
    pub fn write_variant(out: &mut Json, name: &'static str) {
        out.camel_as_snake(name)
    }

    /// Writes a `diag_data!` value as its serde form (every field of one is a value type, whose
    /// serde form is its log form).
    pub fn write_serde<T: serde::Serialize>(out: &mut Json, v: &T) {
        // A type made of value types serializes without failing; `null` is what a record shows
        // should that ever not hold.
        out.raw(&serde_json::to_string(v).unwrap_or_else(|_| "null".to_owned()));
    }
}

/// How long [`shutdown`] waits for the writer.
pub const SHUTDOWN_WAIT: Duration = Duration::from_secs(2);

static SINK: OnceLock<Sink> = OnceLock::new();
static INSTALL: Mutex<()> = Mutex::new(());

/// Why [`install`] did nothing.
#[derive(Debug)]
pub enum InstallError {
    /// This process already has its sink.
    AlreadyInstalled,
    /// The writer thread could not be started.
    Spawn(std::io::Error),
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstallError::AlreadyInstalled => f.write_str("the diagnostics sink is installed"),
            InstallError::Spawn(e) => write!(f, "cannot start the diagnostics writer: {e}"),
        }
    }
}

impl std::error::Error for InstallError {}

/// Installs this process's sink. Returns at once: the writer thread creates the directory and
/// opens the file. [`QUEUE_ENV`] overrides `config.queue`.
pub fn install(mut config: SinkConfig) -> Result<(), InstallError> {
    let _once = sink::lock(&INSTALL);
    if SINK.get().is_some() {
        return Err(InstallError::AlreadyInstalled);
    }
    if let Some(q) = std::env::var(QUEUE_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
    {
        config.queue = q;
    }
    let s = Sink::start(config).map_err(InstallError::Spawn)?;
    if SINK.set(s).is_err() {
        // Unreachable: `INSTALL` serializes installs and the sink was checked absent above.
        return Err(InstallError::AlreadyInstalled);
    }
    Ok(())
}

#[cfg(feature = "tracing")]
pub(crate) fn installed() -> Option<&'static Sink> {
    SINK.get()
}

/// Records `event` in this process's sink; nothing without one (libraries, unit tests).
pub fn emit<E: DiagEvent>(event: &E) {
    if let Some(s) = SINK.get() {
        s.emit(event);
    }
}

/// See [`Sink::announce_process`]; nothing without a sink.
pub fn announce_process() {
    if let Some(s) = SINK.get() {
        s.announce_process();
    }
}

/// See [`Sink::flush`]; `true` without a sink.
pub fn flush(timeout: Duration) -> bool {
    SINK.get().is_none_or(|s| s.flush(timeout))
}

/// See [`Sink::promote`]; nothing without a sink.
pub fn promote(file: &str) -> std::io::Result<()> {
    SINK.get().map_or(Ok(()), |s| s.promote(file))
}

/// Ends this process's sink (see [`Sink::shutdown`]), waiting at most [`SHUTDOWN_WAIT`].
/// Later records are ignored.
pub fn shutdown() -> bool {
    SINK.get().is_none_or(|s| s.shutdown(SHUTDOWN_WAIT))
}

/// See [`Sink::tail`].
pub fn tail() -> Option<Tail> {
    SINK.get().map(Sink::tail)
}

/// See [`Sink::sources`].
pub fn sources() -> Vec<Source> {
    SINK.get().map(Sink::sources).unwrap_or_default()
}

/// See [`Sink::read`].
pub fn read(source: &str, gen: u64, offset: u64, max: usize) -> std::io::Result<Chunk> {
    match SINK.get() {
        Some(s) => s.read(source, gen, offset, max),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no diagnostics sink",
        )),
    }
}

/// See [`Sink::stats`].
pub fn stats() -> Option<Stats> {
    SINK.get().map(Sink::stats)
}
