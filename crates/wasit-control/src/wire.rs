//! Wire types and newline-delimited JSON framing.
//!
//! Every frame is one JSON object on one line, tagged by `"type"`. Unknown frame types are
//! ignored (forward compatibility within a major version); anything that is not a JSON object
//! with a string `type`, or a known type with malformed fields, is a protocol error that closes
//! the connection.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// The protocol version this crate speaks.
pub const PROTOCOL: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };

/// Default maximum size of one frame (one line, excluding the newline).
pub const DEFAULT_MAX_FRAME: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Identifies a machine that hosts sessions (and runs a daemon).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct HostId(pub String);

impl HostId {
    pub fn new(s: impl Into<String>) -> Self {
        HostId(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for HostId {
    fn from(s: &str) -> Self {
        HostId(s.to_owned())
    }
}

impl From<String> for HostId {
    fn from(s: String) -> Self {
        HostId(s)
    }
}

/// Opaque position in a topic's event sequence. `from: Some(c)` means "events strictly after
/// `c`". The service that owns the topic defines the encoding; [`Cursor::from_u64`] is the
/// conventional encoding for sequence numbers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct Cursor(pub String);

impl Cursor {
    pub fn new(s: impl Into<String>) -> Self {
        Cursor(s.into())
    }
    pub fn from_u64(n: u64) -> Self {
        Cursor(n.to_string())
    }
    pub fn as_u64(&self) -> Option<u64> {
        self.0.parse().ok()
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Subscription id, allocated by the side that serves the subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(transparent)]
pub struct SubId(pub u64);

impl fmt::Display for SubId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// First frame each side sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct Hello {
    pub protocol: ProtocolVersion,
    /// Exact build identifier of the sender (used to decide provisioning).
    pub build: String,
    pub host_id: HostId,
    /// Free-form role, e.g. `"gui"`, `"daemon"`, `"hub"`.
    pub role: String,
    #[serde(default)]
    pub features: Vec<String>,
    /// Key of the machine the sender runs on (`wasit_runtime::machine::machine_key`), so a hub
    /// can tell two hosts apart even when they share a home directory or an alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
}

impl Hello {
    pub fn new(
        build: impl Into<String>,
        host_id: impl Into<HostId>,
        role: impl Into<String>,
    ) -> Self {
        Hello {
            protocol: PROTOCOL,
            build: build.into(),
            host_id: host_id.into(),
            role: role.into(),
            features: Vec::new(),
            machine: None,
        }
    }

    pub fn with_machine(mut self, machine: impl Into<String>) -> Self {
        self.machine = Some(machine.into());
        self
    }

    pub fn with_features<I, S>(mut self, features: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.features = features.into_iter().map(Into::into).collect();
        self
    }
}

/// Well-known error codes. Services may use their own codes as well.
pub mod codes {
    pub const METHOD_NOT_FOUND: &str = "method_not_found";
    pub const TOPIC_NOT_FOUND: &str = "topic_not_found";
    pub const INVALID_PARAMS: &str = "invalid_params";
    pub const INTERNAL: &str = "internal";
    pub const TIMEOUT: &str = "timeout";
    pub const OVERLOADED: &str = "overloaded";
    pub const SHUTTING_DOWN: &str = "shutting_down";
    pub const PROTOCOL: &str = "protocol_error";
    pub const FRAME_TOO_LARGE: &str = "frame_too_large";
    pub const INCOMPATIBLE: &str = "incompatible";
    pub const HOST_UNKNOWN: &str = "host_unknown";
    pub const HOST_UNREACHABLE: &str = "host_unreachable";
    pub const HOP_LIMIT: &str = "hop_limit";
    pub const BRIDGE_UNAVAILABLE: &str = "bridge_unavailable";
    pub const NOT_CONNECTED: &str = "not_connected";
}

/// Error carried in a `Response`, a `Goodbye` or a `SubscriptionClosed{Error}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct RpcError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        RpcError {
            code: code.into(),
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            codes::METHOD_NOT_FOUND,
            format!("unknown method `{method}`"),
        )
    }

    pub fn topic_not_found(topic: &str) -> Self {
        Self::new(codes::TOPIC_NOT_FOUND, format!("unknown topic `{topic}`"))
    }

    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self::new(codes::INVALID_PARAMS, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(codes::INTERNAL, msg)
    }

    /// Transient conditions a caller may retry (possibly after the link comes back).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.code.as_str(),
            codes::TIMEOUT
                | codes::OVERLOADED
                | codes::SHUTTING_DOWN
                | codes::HOST_UNREACHABLE
                | codes::NOT_CONNECTED
        )
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

/// Why a subscription ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CloseReason {
    /// The topic has no more events (or the service ended it).
    Ended,
    /// The consumer fell behind; resubscribe from the last cursor.
    Lagged,
    /// The host serving the topic (or the connection to it) was lost; resubscribe later.
    HostLost,
    /// The service failed the subscription.
    Error { error: RpcError },
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// One protocol frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Hello(Hello),
    Request {
        id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<HostId>,
        method: String,
        #[serde(default)]
        params: Value,
        /// Number of times this request has been forwarded by a router.
        #[serde(default, skip_serializing_if = "is_zero")]
        hops: u32,
    },
    Response {
        id: u64,
        #[serde(default)]
        result: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<RpcError>,
    },
    Subscribe {
        id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host: Option<HostId>,
        topic: String,
        #[serde(default, skip_serializing_if = "Value::is_null")]
        params: Value,
        #[serde(default)]
        from: Option<Cursor>,
        #[serde(default, skip_serializing_if = "is_zero")]
        hops: u32,
        /// Flow-control window: the server sends at most this many events beyond those
        /// acknowledged with `Credit`. `0` = no flow control (queue limits only).
        #[serde(default, skip_serializing_if = "is_zero")]
        window: u32,
    },
    Subscribed {
        id: u64,
        sub: SubId,
    },
    Event {
        sub: SubId,
        cursor: Cursor,
        payload: Value,
    },
    Unsubscribe {
        sub: SubId,
    },
    /// The subscriber consumed `n` more events of `sub` (refills the window).
    Credit {
        sub: SubId,
        n: u32,
    },
    SubscriptionClosed {
        sub: SubId,
        reason: CloseReason,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    /// Sent right before a side closes the connection on purpose (shutdown, protocol error,
    /// incompatible version, bridge failure).
    Goodbye {
        reason: RpcError,
    },
}

const KNOWN_TYPES: &[&str] = &[
    "hello",
    "request",
    "response",
    "subscribe",
    "subscribed",
    "event",
    "unsubscribe",
    "credit",
    "subscription_closed",
    "ping",
    "pong",
    "goodbye",
];

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("frame exceeds the maximum size of {limit} bytes")]
    TooLarge { limit: usize },
    #[error("malformed frame: {0}")]
    Malformed(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

/// Encode a frame as one line (with trailing newline). Fails if it would exceed `max`.
pub fn encode(frame: &Frame, max: usize) -> Result<Vec<u8>, RpcError> {
    let mut bytes = serde_json::to_vec(frame)
        .map_err(|e| RpcError::internal(format!("cannot encode frame: {e}")))?;
    if bytes.len() > max {
        return Err(RpcError::new(
            codes::FRAME_TOO_LARGE,
            format!("frame of {} bytes exceeds the limit of {max}", bytes.len()),
        ));
    }
    bytes.push(b'\n');
    Ok(bytes)
}

/// Decode one line. `Ok(None)` means a well-formed frame of an unknown type (ignored).
pub fn decode(line: &[u8]) -> Result<Option<Frame>, FrameError> {
    let value: Value =
        serde_json::from_slice(line).map_err(|e| FrameError::Malformed(e.to_string()))?;
    let ty = value.get("type").and_then(Value::as_str).ok_or_else(|| {
        FrameError::Malformed("frame is not an object with a string `type`".into())
    })?;
    if !KNOWN_TYPES.contains(&ty) {
        return Ok(None);
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| FrameError::Malformed(e.to_string()))
}

/// Reads newline-delimited frames with a hard size limit.
pub struct FrameReader<R> {
    inner: BufReader<R>,
    buf: Vec<u8>,
    max: usize,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(inner: R, max: usize) -> Self {
        FrameReader {
            inner: BufReader::new(inner),
            buf: Vec::new(),
            max,
        }
    }

    /// Next non-empty line (without the newline / trailing `\r`), or `None` at EOF.
    ///
    /// Cancel-safe: a partially read line stays buffered.
    pub async fn next_line(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        loop {
            let avail = self.inner.fill_buf().await?;
            if avail.is_empty() {
                if self.buf.iter().all(u8::is_ascii_whitespace) {
                    return Ok(None);
                }
                // A final line without newline: accept it as a frame.
                let line = std::mem::take(&mut self.buf);
                return Ok(Some(line));
            }
            match avail.iter().position(|b| *b == b'\n') {
                Some(i) => {
                    if self.buf.len() + i > self.max {
                        return Err(FrameError::TooLarge { limit: self.max });
                    }
                    self.buf.extend_from_slice(&avail[..i]);
                    self.inner.consume(i + 1);
                    let mut line = std::mem::take(&mut self.buf);
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    return Ok(Some(line));
                }
                None => {
                    let n = avail.len();
                    if self.buf.len() + n > self.max {
                        return Err(FrameError::TooLarge { limit: self.max });
                    }
                    self.buf.extend_from_slice(avail);
                    self.inner.consume(n);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frames_round_trip() {
        let frames = vec![
            Frame::Hello(Hello::new("b1", "h", "daemon").with_features(["x"])),
            Frame::Request {
                id: 1,
                host: Some("h2".into()),
                method: "m".into(),
                params: json!({"a": 1}),
                hops: 2,
            },
            Frame::Response {
                id: 1,
                result: Value::Null,
                error: None,
            },
            Frame::Response {
                id: 2,
                result: Value::Null,
                error: Some(RpcError::new("c", "m").with_data(json!(3))),
            },
            Frame::Subscribe {
                id: 3,
                host: None,
                topic: "t".into(),
                params: Value::Null,
                from: Some(Cursor::from_u64(9)),
                hops: 0,
                window: 16,
            },
            Frame::Subscribed {
                id: 3,
                sub: SubId(7),
            },
            Frame::Event {
                sub: SubId(7),
                cursor: Cursor::from_u64(10),
                payload: json!("p"),
            },
            Frame::Unsubscribe { sub: SubId(7) },
            Frame::Credit {
                sub: SubId(7),
                n: 8,
            },
            Frame::SubscriptionClosed {
                sub: SubId(7),
                reason: CloseReason::Error {
                    error: RpcError::new("x", "y"),
                },
            },
            Frame::SubscriptionClosed {
                sub: SubId(7),
                reason: CloseReason::Lagged,
            },
            Frame::Ping { nonce: 1 },
            Frame::Pong { nonce: 1 },
            Frame::Goodbye {
                reason: RpcError::new(codes::SHUTTING_DOWN, "bye"),
            },
        ];
        for f in frames {
            let bytes = encode(&f, DEFAULT_MAX_FRAME).unwrap();
            assert_eq!(bytes.last(), Some(&b'\n'));
            let back = decode(&bytes[..bytes.len() - 1]).unwrap().unwrap();
            assert_eq!(back, f);
        }
    }

    #[test]
    fn wire_shape_is_stable() {
        let bytes = encode(
            &Frame::Request {
                id: 5,
                host: None,
                method: "ping".into(),
                params: json!({}),
                hops: 0,
            },
            DEFAULT_MAX_FRAME,
        )
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "{\"type\":\"request\",\"id\":5,\"method\":\"ping\",\"params\":{}}\n"
        );
        let closed = serde_json::to_value(Frame::SubscriptionClosed {
            sub: SubId(1),
            reason: CloseReason::HostLost,
        })
        .unwrap();
        assert_eq!(
            closed,
            json!({"type": "subscription_closed", "sub": 1, "reason": {"kind": "host_lost"}})
        );
    }

    #[test]
    fn unknown_type_is_ignored_garbage_is_malformed() {
        assert!(decode(br#"{"type":"future_thing","x":1}"#)
            .unwrap()
            .is_none());
        assert!(matches!(decode(b"not json"), Err(FrameError::Malformed(_))));
        assert!(matches!(decode(b"[1,2]"), Err(FrameError::Malformed(_))));
        assert!(matches!(
            decode(br#"{"type":"request","id":"x"}"#),
            Err(FrameError::Malformed(_))
        ));
    }

    #[test]
    fn encode_enforces_limit() {
        let f = Frame::Event {
            sub: SubId(1),
            cursor: Cursor::from_u64(1),
            payload: json!("x".repeat(100)),
        };
        let err = encode(&f, 50).unwrap_err();
        assert_eq!(err.code, codes::FRAME_TOO_LARGE);
    }

    #[tokio::test]
    async fn reader_splits_lines_and_enforces_limit() {
        let data: &[u8] = b"\n{\"a\":1}\r\n\n  \n{\"b\":2}\n{\"c\"";
        let mut r = FrameReader::new(data, 100);
        assert_eq!(r.next_line().await.unwrap().unwrap(), b"{\"a\":1}");
        assert_eq!(r.next_line().await.unwrap().unwrap(), b"{\"b\":2}");
        assert_eq!(r.next_line().await.unwrap().unwrap(), b"{\"c\"");
        assert!(r.next_line().await.unwrap().is_none());

        let big = vec![b'x'; 1000];
        let mut r = FrameReader::new(&big[..], 100);
        assert!(matches!(
            r.next_line().await,
            Err(FrameError::TooLarge { limit: 100 })
        ));
        let mut big_line = vec![b'y'; 101];
        big_line.push(b'\n');
        let mut r = FrameReader::new(&big_line[..], 100);
        assert!(matches!(
            r.next_line().await,
            Err(FrameError::TooLarge { limit: 100 })
        ));
    }
}
