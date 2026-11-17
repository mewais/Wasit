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
}

/// Why an established connection closed.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConnClose {
    /// The peer closed the stream.
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
