use std::io;

/// Every error the shim library can return.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("timed out while {0}")]
    Timeout(&'static str),
    #[error("shim refused the request: {0}")]
    Remote(String),
    #[error("this controller was replaced by a newer connection")]
    Replaced,
    #[error("disconnected from the shim")]
    Disconnected,
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("shim failed to start: {0}")]
    Spawn(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("journal is corrupt: {0}")]
    Corrupt(String),
    #[error("another shim already holds the lock on {0}")]
    Locked(String),
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::Io(io::Error::other(e))
    }
}
