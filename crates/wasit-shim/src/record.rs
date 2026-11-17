use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Kind of a journal record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// `{pid, argv, cwd, started_at}` — the child was started.
    Spawned,
    /// One line the child wrote to stdout (`data` is a string).
    Out,
    /// One line the child wrote to stderr (`data` is a string).
    Err,
    /// One line written to the child's stdin (`data` is a string).
    In,
    /// `{code}` or `{signal}` — the child exited. Always the last record of a run.
    Exited,
    /// Shim-level annotation (signal sent, stdin closed, drain timed out, ...).
    Note,
}

impl Kind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Kind::Spawned => "spawned",
            Kind::Out => "out",
            Kind::Err => "err",
            Kind::In => "in",
            Kind::Exited => "exited",
            Kind::Note => "note",
        }
    }
}

/// One journal record: `{"seq", "t", "kind", "data"[, "partial": true]}`.
///
/// Lines are stored without their terminating `\n` (a preceding `\r` is stripped too), decoded
/// as lossy UTF-8. A line longer than the record cap is split into several records of the same
/// kind; every piece but the last carries `partial: true`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// Strictly increasing per session directory, starting at 1.
    pub seq: u64,
    /// Unix time in milliseconds when the shim recorded it.
    pub t: u64,
    pub kind: Kind,
    pub data: Value,
    #[serde(default, skip_serializing_if = "is_false")]
    pub partial: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl Record {
    /// The line text of an `out`/`err`/`in` record.
    pub fn text(&self) -> Option<&str> {
        match self.kind {
            Kind::Out | Kind::Err | Kind::In => self.data.as_str(),
            _ => None,
        }
    }

    /// The exit status of an `exited` record.
    pub fn exit_info(&self) -> Option<ExitInfo> {
        if self.kind != Kind::Exited {
            return None;
        }
        serde_json::from_value(self.data.clone()).ok()
    }
}

/// How the child ended: exactly one of `code` / `signal` is set (both `None` only if the shim
/// could not learn the status).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExitInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
}

impl ExitInfo {
    pub(crate) fn from_status(status: std::process::ExitStatus) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            ExitInfo {
                code: status.code(),
                signal: status.signal(),
            }
        }
        #[cfg(not(unix))]
        {
            ExitInfo {
                code: status.code(),
                signal: None,
            }
        }
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
