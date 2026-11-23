//! The shim's diagnostics (design S3, §12.2): `shim.log` is NDJSON written through
//! `wasit-diag`, and every record is a [`ShimEvent`] — ids, sizes, counts, timings, states and
//! reasons, never the journal's content (no line a child wrote or was sent, no argument or
//! environment value, no path of the project).
//!
//! The shim owns a CLI and its journal, so logging may never delay them: emitting is a budget
//! check, a serialization and a `try_send` (nothing awaits, nothing writes a file on the
//! caller's thread); one OS thread of the sink writes `shim.log`, rotates it at
//! [`LOG_MAX_BYTES`] keeping one older generation, and counts what a full queue drops.
//!
//! **Where stderr goes.** The sink owns `<dir>/shim.log` (the writer thread, inside the shim,
//! opens it, so there is exactly one writer and the spawner opens nothing for it). The shim's
//! stderr is a separate small file, `<dir>/shim.stderr` (see `spawn.rs`): raw crash output of
//! the process (an abort message, a runtime fatal error) that no event can describe, never
//! exported, cut to nothing when a new shim starts and it is over [`STDERR_MAX_BYTES`].
//! The panic hook writes a `diag.panic` record and prints nothing, unless `WASIT_LOG` is set (a
//! developer asked for the default hook's text on stderr).

use std::path::PathBuf;
use std::time::Duration;

use wasit_diag::{Bytes, Code, ErrorInfo, Ident, Ms, SinkConfig};

use crate::cli::Config;
use crate::Error;

/// `shim.log` is rotated before a record would take it past this size; one older generation
/// (`shim.log.1`) is kept.
pub(crate) const LOG_MAX_BYTES: u64 = 256 << 10;
/// A new shim cuts `shim.stderr` to nothing when it is larger than this.
pub(crate) const STDERR_MAX_BYTES: u64 = 64 << 10;
/// Records the sink's queue holds (a shim emits a handful per session; the bound is for a
/// storm of controller reconnects). Small: the queue is allocated up front, once per shim.
const QUEUE: usize = 256;
/// How long [`settle`] waits for the writer.
const SETTLE: Duration = Duration::from_secs(2);
/// An fsync slower than this is reported (`shim.fsync_slow`).
pub(crate) const FSYNC_SLOW: Duration = Duration::from_secs(1);
/// The developer override: the previous panic hook's text goes to stderr when it is set.
const DEV_ENV: &str = "WASIT_LOG";

wasit_diag::diag_enum! {
    /// What happened to a controller connection.
    pub(crate) enum ControllerEvent {
        /// It sent `attach`.
        Attached,
        /// A newer controller displaced it.
        Replaced,
        /// Its socket stayed unwritable for the write timeout; it was disconnected.
        SlowDropped,
        /// A write to it failed.
        WriteFailed,
        /// The connection ended.
        Disconnected,
    }
}

wasit_diag::diag_enum! {
    /// Which step of the journal's durability failed.
    pub(crate) enum JournalStage {
        /// Appending a batch to the file.
        Write,
        /// The periodic or requested fsync.
        Sync,
        /// The fsync of the exit sequence.
        FinalSync,
        /// The exit sequence's fsync did not finish in time.
        SyncTimeout,
        /// The flusher task was gone when the exit sequence asked for the fsync.
        FlusherGone,
    }
}

wasit_diag::diag_enum! {
    /// Who caused a signal.
    pub(crate) enum SignalBy {
        /// The attached controller asked for it.
        Controller,
        /// The shim itself (stopping the child, killing leftovers).
        Shim,
        /// The operating system or another process signalled the shim.
        External,
    }
}

wasit_diag::diag_enum! {
    /// Which step of the readiness handshake failed.
    pub(crate) enum ReadyStage {
        /// Writing the readiness line to stdout.
        WriteLine,
        /// Pointing stdout at the null device afterwards.
        DetachStdout,
    }
}

wasit_diag::diag_events! {
    /// Events of the shim process (`src` `shim`, file `shim.log`).
    pub(crate) enum ShimEvent {
        /// The child runs and `spawned` is journaled.
        #[diag(ev = "shim.start")]
        Start {
            session: Ident,
            pid: u32,
            child_pid: u32,
            spawned_seq: u64,
            separate_run_dir: bool,
        },
        /// The shim could not start; the readiness line carries the same reason.
        #[diag(ev = "shim.not_ready", lvl = error)]
        NotReady { error: ErrorInfo },
        /// The readiness handshake with whoever launched the shim failed.
        #[diag(ev = "shim.ready_failed", lvl = warn)]
        ReadyFailed { stage: ReadyStage, error: ErrorInfo },
        /// A controller connection event. `from_seq` with `attached`; `write_timeout_ms` with
        /// `slow_dropped`; `error` with `write_failed`.
        #[diag(ev = "shim.controller")]
        Controller {
            event: ControllerEvent,
            from_seq: Option<u64>,
            write_timeout_ms: Option<Ms>,
            error: Option<ErrorInfo>,
        },
        /// A stream reader waits for the journal flusher: `bytes` are unflushed.
        #[diag(ev = "shim.backlog_wait", lvl = warn, budget = 60)]
        BacklogWait { bytes: Bytes },
        /// An fsync of the journal took longer than a second.
        #[diag(ev = "shim.fsync_slow", lvl = warn, budget = 60)]
        FsyncSlow { ms: Ms },
        /// The journal could not be written or made durable. `error` is the I/O error when
        /// there is one.
        #[diag(ev = "shim.journal_failed", lvl = error)]
        JournalFailed { stage: JournalStage, error: Option<ErrorInfo> },
        /// A write to the child's stdin failed.
        #[diag(ev = "shim.stdin_failed", lvl = warn)]
        StdinFailed { error: ErrorInfo },
        /// The stdin writer was still busy when the child's stdin was to be closed at exit.
        #[diag(ev = "shim.stdin_busy", lvl = warn)]
        StdinBusy {},
        /// A signal reached or was sent to the child's process group; `error` when delivering
        /// it failed.
        #[diag(ev = "shim.signal")]
        Signal { sig: Code, by: SignalBy, error: Option<ErrorInfo> },
        /// A signal handler could not be installed.
        #[diag(ev = "shim.signal_handler_failed", lvl = warn)]
        SignalHandlerFailed { signal: Code, error: ErrorInfo },
        /// Accepting a controller connection failed (retried on a backoff schedule).
        #[diag(ev = "shim.accept_failed", lvl = warn, budget = 60)]
        AcceptFailed { error: ErrorInfo },
        /// The accept loop did not stop within a second at exit.
        #[diag(ev = "shim.accept_not_stopped", lvl = warn)]
        AcceptNotStopped {},
        /// The control endpoint could not be removed.
        #[diag(ev = "shim.endpoint_remove_failed", lvl = warn)]
        EndpointRemoveFailed { error: ErrorInfo },
        /// The liveness lock could not be released explicitly (it is released with the process).
        #[diag(ev = "shim.unlock_failed", lvl = warn)]
        UnlockFailed { error: ErrorInfo },
        /// Waiting for the child failed.
        #[diag(ev = "shim.wait_failed", lvl = error)]
        WaitFailed { error: ErrorInfo },
        /// A task reading the child's `stdout`/`stderr` ended abnormally.
        #[diag(ev = "shim.reader_failed", lvl = error)]
        ReaderFailed { stream: Code, error: ErrorInfo },
        /// The child's stdio was still open `ms` after it exited; leftovers are killed.
        #[diag(ev = "shim.drain_timeout", lvl = warn)]
        DrainTimeout { ms: Ms },
        /// The child was still running after SIGKILL and the grace period.
        #[diag(ev = "shim.kill_unconfirmed", lvl = error)]
        KillUnconfirmed {},
        /// The exit sequence is complete (the process exits right after). `synced`: the final
        /// `exited` record is durable; `linger_ms`: how long a controller was given to receive
        /// the tail.
        #[diag(ev = "shim.exit")]
        Exit {
            code: Option<i32>,
            signal: Option<i32>,
            synced: bool,
            linger_ms: Ms,
        },
    }
}

/// Records `event` (never waits).
pub(crate) fn emit(event: ShimEvent) {
    wasit_diag::emit(&event);
}

/// A failed I/O operation as a record field (kind and OS error number, never the message).
pub(crate) fn io(e: &std::io::Error) -> ErrorInfo {
    ErrorInfo::io(e)
}

/// The shim's own error as a record field. Its text is the `Display` of [`Error`], scrubbed:
/// argument errors name flags and positions and never a value (see `cli.rs`), the other
/// variants name an operation and the OS's reason. (The one `ErrorInfo::ours` call of the shim,
/// pinned in `wasit-diag/tests/value_sites.rs`.)
pub(crate) fn ours(e: &Error) -> ErrorInfo {
    ErrorInfo::ours(Code::st("not_ready"), e)
}

/// A task that ended abnormally, as a record field: only what kind of end it was (the panic
/// message of a task can hold anything) plus its length and fingerprint.
pub(crate) fn join(e: &tokio::task::JoinError) -> ErrorInfo {
    let code = if e.is_panic() {
        "task_panicked"
    } else {
        "task_cancelled"
    };
    ErrorInfo::foreign(Code::st(code), &e.to_string())
}

/// Installs this process's sink (`shim.log` in the session directory, role `shim`), the panic
/// hook, and records `process.start`. Returns at once: the writer thread creates the file. When
/// the sink cannot be installed the shim runs without diagnostics and says so on stderr (the
/// crash file), once.
pub(crate) fn start(dir: PathBuf) {
    let mut cfg = SinkConfig::new(
        Code::st("shim"),
        Ident::st(concat!("wasit-shim-", env!("CARGO_PKG_VERSION"))),
        dir,
        crate::LOG_FILE,
    );
    cfg.max_bytes = LOG_MAX_BYTES;
    cfg.generations = 1;
    cfg.queue = QUEUE;
    match wasit_diag::install(cfg) {
        Ok(()) => {
            wasit_diag::install_panic_hook(std::env::var_os(DEV_ENV).is_some());
            wasit_diag::announce_process();
        }
        // The only way to report it: stderr is the process's crash file.
        Err(e) => eprintln!("wasit-shim: no diagnostics: {e}"),
    }
}

/// The session id the shim's records name: the last component of its directory.
pub(crate) fn session(cfg: &Config) -> Ident {
    Ident::new(
        &cfg.dir
            .file_name()
            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
    )
}

/// Waits (bounded) until what was recorded so far is written: used when the shim cannot start,
/// so that whoever sees the readiness line saying why also finds the reason in `shim.log`. Never
/// on the path of a session that runs.
pub(crate) fn settle() {
    // A timeout or a writer that cannot write changes nothing for the caller.
    let _settled = wasit_diag::flush(SETTLE);
}

/// Gives the writer a bounded moment to write what is queued, at the very end of the process
/// (the journal, the lock and the endpoint are already settled; nothing that matters to the
/// session waits for this).
pub(crate) fn finish() {
    // Whether the writer finished in time changes nothing: the process is about to exit.
    let _finished = wasit_diag::shutdown();
}
