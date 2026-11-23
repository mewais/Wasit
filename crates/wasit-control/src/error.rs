//! Typed errors surfaced to callers.

use std::fmt;

use serde::Serialize;

use crate::command::CommandFailureKind;
use crate::wire::{ProtocolVersion, RpcError};

/// Why establishing a connection failed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ConnectError {
    /// The peer speaks a different major protocol version.
    #[error("incompatible protocol: ours {ours}, theirs {theirs}")]
    Incompatible {
        ours: ProtocolVersion,
        theirs: ProtocolVersion,
    },
    /// The peer (or a bridge in front of it) said goodbye instead of hello.
    #[error("connection refused by peer: {0}")]
    Refused(RpcError),
    #[error("handshake timed out")]
    HandshakeTimeout,
    /// The peer went away before the handshake completed: its end of the stream was found
    /// closed by the read of its hello or by the write of ours ([`peer_gone`]), whichever came
    /// first.
    #[error("connection closed during handshake")]
    ClosedDuringHandshake,
    #[error("protocol error during handshake: {0}")]
    Protocol(String),
    #[error("i/o error: {0}")]
    Io(String),
    /// The local transport program could not be started (e.g. `ssh` missing locally).
    #[error("cannot start transport command: {0}")]
    Spawn(String),
    /// The transport command exited; classified from its exit status and stderr.
    #[error("transport command failed ({kind:?}, exit {exit_code:?}): {stderr}")]
    Command {
        kind: CommandFailureKind,
        exit_code: Option<i32>,
        stderr: String,
    },
    #[error("connect timed out")]
    Timeout,
}

impl ConnectError {
    pub(crate) fn io(e: impl fmt::Display) -> Self {
        ConnectError::Io(e.to_string())
    }

    /// What an I/O error on the link's stream during the handshake is: the peer having gone
    /// ([`peer_gone`]) is [`ConnectError::ClosedDuringHandshake`], whether the write of our
    /// hello or the read of theirs noticed it; anything else is [`ConnectError::Io`].
    pub(crate) fn stream_io(e: &std::io::Error) -> Self {
        if peer_gone(e) {
            ConnectError::ClosedDuringHandshake
        } else {
            ConnectError::io(e)
        }
    }
}

/// Whether an I/O error on a link's stream says that the peer is gone, that is, is the same
/// event as the stream ending (end-of-file): which of a read and a write notices first is a race
/// between two processes, and the answer must not depend on it. The set is chosen deliberately:
///
/// * `BrokenPipe`: writing to a pipe or socket whose reader is gone (`EPIPE`; Windows
///   `ERROR_BROKEN_PIPE` and `ERROR_NO_DATA`, which `std` maps to it).
/// * `ConnectionReset`, `ConnectionAborted`: the peer's end was torn down under us (`ECONNRESET`,
///   `ECONNABORTED`, `WSAECONNRESET`, `WSAECONNABORTED`), seen by a write or by a read.
/// * `NotConnected`: no peer is attached any more (`ENOTCONN`, `WSAENOTCONN`).
/// * `UnexpectedEof`: a reader's own report that the stream ended where more was due.
///
/// Out: `WriteZero` (a sink that takes no bytes says nothing about a peer), `TimedOut` (the
/// bounds have their own errors), `Interrupted`/`WouldBlock` (retried by the runtime) and every
/// error that is about us (permissions, invalid input, ...): those stay I/O errors. Not checked
/// on Windows (no machine to test on): `ERROR_PIPE_NOT_CONNECTED` is not given a kind by `std`
/// and so is not recognised.
pub fn peer_gone(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::{
        BrokenPipe, ConnectionAborted, ConnectionReset, NotConnected, UnexpectedEof,
    };
    matches!(
        e.kind(),
        BrokenPipe | ConnectionReset | ConnectionAborted | NotConnected | UnexpectedEof
    )
}

/// Why an established connection closed.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConnClose {
    /// The peer closed the stream: the read found its end, or a write found the peer gone
    /// ([`peer_gone`]), whichever came first.
    Eof,
    /// The peer said goodbye.
    Goodbye {
        reason: RpcError,
    },
    Io {
        message: String,
    },
    Protocol {
        message: String,
    },
    FrameTooLarge,
    /// Nothing arrived within the idle timeout (pings included).
    IdleTimeout,
    /// Closed locally (dropped, shut down).
    Local,
}

impl ConnClose {
    /// What an I/O error on the stream of an established connection is: the peer having gone
    /// ([`peer_gone`]) is [`ConnClose::Eof`], whether the writer or the reader noticed it first;
    /// anything else is [`ConnClose::Io`].
    pub(crate) fn stream_io(e: &std::io::Error) -> Self {
        if peer_gone(e) {
            ConnClose::Eof
        } else {
            ConnClose::Io {
                message: e.to_string(),
            }
        }
    }

    /// The kind as the word the `kind` tag serializes to (`eof`, `goodbye`, `io`, `protocol`,
    /// `frame_too_large`, `idle_timeout`, `local`): what diagnostics record of a close, never
    /// the message a peer or a parser produced.
    pub fn code(&self) -> &'static str {
        match self {
            ConnClose::Eof => "eof",
            ConnClose::Goodbye { .. } => "goodbye",
            ConnClose::Io { .. } => "io",
            ConnClose::Protocol { .. } => "protocol",
            ConnClose::FrameTooLarge => "frame_too_large",
            ConnClose::IdleTimeout => "idle_timeout",
            ConnClose::Local => "local",
        }
    }
}

impl fmt::Display for ConnClose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnClose::Eof => f.write_str("peer closed the connection"),
            ConnClose::Goodbye { reason } => write!(f, "peer said goodbye: {reason}"),
            ConnClose::Io { message } => write!(f, "i/o error: {message}"),
            ConnClose::Protocol { message } => write!(f, "protocol error: {message}"),
            ConnClose::FrameTooLarge => f.write_str("frame too large"),
            ConnClose::IdleTimeout => f.write_str("peer stopped responding"),
            ConnClose::Local => f.write_str("closed locally"),
        }
    }
}

/// Why a request (or subscribe) failed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CallError {
    /// The remote service answered with an error.
    #[error("{0}")]
    Rpc(RpcError),
    #[error("deadline exceeded")]
    Timeout,
    /// No connection is currently established (supervised client).
    #[error("not connected")]
    NotConnected,
    #[error("connection lost: {0}")]
    ConnectionLost(ConnClose),
    #[error("cannot encode params: {0}")]
    Encode(String),
    #[error("cannot decode result: {0}")]
    Decode(String),
}

impl CallError {
    /// Convert to an error suitable for returning from a [`crate::Service`] (e.g. when
    /// forwarding).
    pub fn into_rpc(self) -> RpcError {
        use crate::wire::codes;
        match self {
            CallError::Rpc(e) => e,
            CallError::Timeout => RpcError::new(codes::TIMEOUT, "deadline exceeded"),
            CallError::NotConnected => RpcError::new(codes::HOST_UNREACHABLE, "not connected"),
            CallError::ConnectionLost(c) => {
                RpcError::new(codes::HOST_UNREACHABLE, format!("connection lost: {c}"))
            }
            CallError::Encode(m) => RpcError::invalid_params(m),
            CallError::Decode(m) => RpcError::internal(m),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            CallError::Rpc(e) => e.is_retryable(),
            CallError::Timeout | CallError::NotConnected | CallError::ConnectionLost(_) => true,
            CallError::Encode(_) | CallError::Decode(_) => false,
        }
    }
}
