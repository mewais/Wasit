//! Control-endpoint wire frames: newline-delimited JSON, one object per line, tagged by `type`.
//!
//! Client → shim: [`ClientFrame`]. Shim → client: [`ServerFrame`]. On connect the shim sends
//! `hello` first. Every request carrying an `id` is answered by exactly one `ack` or `error`
//! with the same `id`.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

use crate::record::Record;

/// Wire protocol version announced in `hello`.
pub const PROTOCOL: u32 = 1;

/// Signal delivered to the child's process group (Unix) / job (Windows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sig {
    Int,
    Term,
    Kill,
}

impl Sig {
    pub fn as_str(self) -> &'static str {
        match self {
            Sig::Int => "int",
            Sig::Term => "term",
            Sig::Kill => "kill",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// Stream journal records with `seq >= from_seq`, then live records, gap- and duplicate-free.
    Attach {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        from_seq: u64,
    },
    /// Write `line` + `\n` to the child's stdin, then journal it as an `in` record.
    Write { id: u64, line: String },
    /// Close the child's stdin.
    CloseStdin { id: u64 },
    /// Signal the child's process group / job.
    Signal { id: u64, sig: Sig },
    /// Journal `data` as a `note` record (a controller annotation, e.g. which command caused
    /// the writes that follow). Ordered with `write` / `close_stdin` frames of the same
    /// connection. Refused once the child's `exited` record has been journaled, so `exited`
    /// stays the last record of a run.
    Note { id: u64, data: serde_json::Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    Hello {
        proto: u32,
        shim_pid: u32,
        child_pid: u32,
    },
    Record {
        record: Record,
    },
    /// `seq` is the journal record the request produced (`in` for write, `note` otherwise).
    Ack {
        id: u64,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seq: Option<u64>,
    },
    Error {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<u64>,
        message: String,
    },
    /// A newer controller connected; this connection is closed right after.
    Replaced,
}

/// Serialize a frame followed by `\n`.
pub(crate) fn encode<T: Serialize>(frame: &T) -> String {
    // Serializing these plain enums cannot fail (no maps with non-string keys, no foreign
    // Serialize impls); fall back to an error frame rather than panicking.
    let mut s = serde_json::to_string(frame)
        .unwrap_or_else(|e| format!(r#"{{"type":"error","message":"encode failure: {e}"}}"#));
    s.push('\n');
    s
}

/// Read one `\n`-terminated frame into `buf` (without the terminator), refusing frames longer
/// than `cap`. Returns `Ok(false)` on clean EOF before any byte.
pub(crate) async fn read_frame<R: AsyncBufRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
    cap: usize,
) -> std::io::Result<bool> {
    buf.clear();
    loop {
        let avail = r.fill_buf().await?;
        if avail.is_empty() {
            if buf.is_empty() {
                return Ok(false);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed mid-frame",
            ));
        }
        if let Some(i) = avail.iter().position(|&b| b == b'\n') {
            buf.extend_from_slice(&avail[..i]);
            r.consume(i + 1);
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(true);
        }
        let n = avail.len();
        buf.extend_from_slice(avail);
        r.consume(n);
        if buf.len() > cap {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame longer than {cap} bytes"),
            ));
        }
    }
}
