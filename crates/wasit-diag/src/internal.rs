//! The sink-internal events of every process (design §12.1).

use crate::text::DiagText;
use crate::value::{Code, ErrorInfo, Ident, SrcLocation};

crate::diag_events! {
    /// Events of the process and of the sink itself.
    pub enum SinkEvent {
        /// The process started (emitted by the process right after `install`).
        #[diag(ev = "process.start")]
        ProcessStart {
            role: Code,
            build: Ident,
            target: Ident,
            os: Code,
            arch: Code,
            os_release: Option<DiagText>,
            pid: u32,
        },
        /// The process is about to exit.
        #[diag(ev = "process.exit")]
        ProcessExit { reason: Code, code: Option<i32> },
        /// Records with seqs in `first_seq..=last_seq` were not written: `count` of them were
        /// dropped because the queue was full (or their write kept failing).
        #[diag(ev = "diag.dropped", lvl = warn)]
        Dropped { count: u64, first_seq: u64, last_seq: u64 },
        /// `count` records of `event` were over their per-minute budget.
        #[diag(ev = "diag.suppressed", lvl = warn)]
        Suppressed { event: Code, count: u64 },
        /// Writing the file failed (once per error kind until a write succeeds again).
        #[diag(ev = "diag.write_failed", lvl = error)]
        WriteFailed { error: ErrorInfo },
        /// A thread panicked. Only a static payload's text is kept.
        #[diag(ev = "diag.panic", lvl = error)]
        Panic {
            location: SrcLocation,
            thread: Ident,
            payload_kind: Code,
            len: u32,
            static_text: Option<DiagText>,
        },
        /// `n` `tracing` events of `target` at `level` in the last minute (no message, no field).
        #[diag(ev = "trace.counted")]
        TraceCounted { target: Code, level: Code, n: u64 },
    }
}
