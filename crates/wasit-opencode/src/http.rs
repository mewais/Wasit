//! A minimal HTTP/1.1 client for Kilo's loopback server (channel B, see
//! [`crate::subagents`]).
//!
//! Std only: one connection per request (`Connection: close`), so a response body ends at
//! `Content-Length`, at the chunked terminator, or at EOF. Requests never retry; a failed request
//! is reported as an error for the caller to act on. The host is always `127.0.0.1`.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::Value;

/// Where the CLI's loopback server listens, and the Basic credentials it requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub port: u16,
    /// `(username, password)`; sent as `Authorization: Basic …` when set.
    pub basic: Option<(String, String)>,
}

impl Endpoint {
    fn authorization(&self) -> Option<String> {
        self.basic.as_ref().map(|(user, pass)| {
            format!(
                "Basic {}",
                wasit_core::content::b64_encode(format!("{user}:{pass}").as_bytes())
            )
        })
    }
}

/// A complete response (the body was read to its end).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Response {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The body parsed as JSON; `None` for an empty or non-JSON body.
    pub fn json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }
}

/// `true` for an identifier that may be placed in a URL path unescaped (Kilo's session, job and
/// permission ids are `[A-Za-z0-9_-]`). Anything else is refused rather than escaped.
pub fn is_path_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn connect(ep: &Endpoint, timeout: Option<Duration>) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(("127.0.0.1", ep.port))?;
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;
    Ok(stream)
}

fn write_head(
    stream: &mut TcpStream,
    ep: &Endpoint,
    method: &str,
    path: &str,
    body_len: Option<usize>,
    accept: &str,
) -> io::Result<()> {
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nAccept: {accept}\r\nConnection: close\r\n",
        ep.port
    );
    if let Some(auth) = ep.authorization() {
        head.push_str(&format!("Authorization: {auth}\r\n"));
    }
    if let Some(len) = body_len {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {len}\r\n"
        ));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())
}

/// Send one request and read its whole response. `body` is sent as JSON.
pub fn request(
    ep: &Endpoint,
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout: Duration,
) -> io::Result<Response> {
    let mut stream = connect(ep, Some(timeout))?;
    let payload = body.map(Value::to_string);
    write_head(
        &mut stream,
        ep,
        method,
        path,
        payload.as_ref().map(String::len).or(Some(0)),
        "application/json",
    )?;
    if let Some(p) = &payload {
        stream.write_all(p.as_bytes())?;
    }
    let mut reader = BufReader::new(stream);
    let (status, headers) = read_head(&mut reader)?;
    let mut body = BodyReader::new(status, &headers, reader);
    let mut out = Vec::new();
    body.read_to_end(&mut out)?;
    Ok(Response { status, body: out })
}

/// Open a server-sent-events stream (`GET path`, no read timeout). Fails unless the server
/// answers 200.
pub fn open_events(ep: &Endpoint, path: &str) -> io::Result<EventStream> {
    let mut stream = connect(ep, None)?;
    write_head(&mut stream, ep, "GET", path, None, "text/event-stream")?;
    let mut reader = BufReader::new(stream);
    let (status, headers) = read_head(&mut reader)?;
    if status != 200 {
        return Err(io::Error::other(format!("event stream answered {status}")));
    }
    Ok(EventStream {
        lines: BufReader::new(BodyReader::new(status, &headers, reader)),
    })
}

/// The `data:` payloads of a server-sent-events stream.
pub struct EventStream {
    lines: BufReader<BodyReader>,
}

impl EventStream {
    /// The next event's data (multi-line `data:` fields joined with `\n`). `Ok(None)` when the
    /// server closed the stream; an incomplete trailing event is dropped with it.
    pub fn next_data(&mut self) -> io::Result<Option<String>> {
        let mut data: Option<String> = None;
        loop {
            let mut line = String::new();
            if self.lines.read_line(&mut line)? == 0 {
                return Ok(None);
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if let Some(d) = data.take() {
                    return Ok(Some(d));
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("data:") {
                let rest = rest.strip_prefix(' ').unwrap_or(rest);
                match &mut data {
                    Some(d) => {
                        d.push('\n');
                        d.push_str(rest);
                    }
                    None => data = Some(rest.to_string()),
                }
            }
            // `event:`, `id:`, `retry:` and comments are not used by Kilo's stream.
        }
    }
}

type Headers = Vec<(String, String)>;

/// The status line and headers (names lower-cased).
fn read_head(reader: &mut impl BufRead) -> io::Result<(u16, Headers)> {
    let mut status_line = String::new();
    if reader.read_line(&mut status_line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "no HTTP status line",
        ));
    }
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed HTTP status line"))?;
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "HTTP headers ended early",
            ));
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            return Ok((status, headers));
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }
}

/// A response body: fixed length, chunked, or read until the server closes.
enum BodyReader {
    Length {
        reader: BufReader<TcpStream>,
        remaining: u64,
    },
    Chunked {
        reader: BufReader<TcpStream>,
        in_chunk: u64,
        done: bool,
    },
    Close(BufReader<TcpStream>),
}

impl BodyReader {
    fn new(status: u16, headers: &Headers, reader: BufReader<TcpStream>) -> Self {
        // 1xx, 204 and 304 never carry a body, whatever their headers say.
        if matches!(status, 100..=199 | 204 | 304) {
            return BodyReader::Length {
                reader,
                remaining: 0,
            };
        }
        let header = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.as_str())
        };
        if header("transfer-encoding").is_some_and(|v| v.to_ascii_lowercase().contains("chunked")) {
            return BodyReader::Chunked {
                reader,
                in_chunk: 0,
                done: false,
            };
        }
        match header("content-length").and_then(|v| v.parse::<u64>().ok()) {
            Some(remaining) => BodyReader::Length { reader, remaining },
            None => BodyReader::Close(reader),
        }
    }
}

fn read_line_crlf(reader: &mut BufReader<TcpStream>) -> io::Result<String> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "chunked body ended early",
        ));
    }
    Ok(line)
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        match self {
            BodyReader::Close(reader) => reader.read(buf),
            BodyReader::Length { reader, remaining } => {
                if *remaining == 0 {
                    return Ok(0);
                }
                let max = buf
                    .len()
                    .min(usize::try_from(*remaining).unwrap_or(usize::MAX));
                let n = reader.read(&mut buf[..max])?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "body shorter than Content-Length",
                    ));
                }
                *remaining -= n as u64;
                Ok(n)
            }
            BodyReader::Chunked {
                reader,
                in_chunk,
                done,
            } => {
                if *done {
                    return Ok(0);
                }
                if *in_chunk == 0 {
                    let line = read_line_crlf(reader)?;
                    let size_text = line.trim().split(';').next().unwrap_or("");
                    let size = u64::from_str_radix(size_text, 16).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "malformed chunk size")
                    })?;
                    if size == 0 {
                        // Trailer section, ended by an empty line.
                        loop {
                            let t = read_line_crlf(reader)?;
                            if t.trim_end_matches(['\r', '\n']).is_empty() {
                                break;
                            }
                        }
                        *done = true;
                        return Ok(0);
                    }
                    *in_chunk = size;
                }
                let max = buf
                    .len()
                    .min(usize::try_from(*in_chunk).unwrap_or(usize::MAX));
                let n = reader.read(&mut buf[..max])?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "chunk shorter than its size",
                    ));
                }
                *in_chunk -= n as u64;
                if *in_chunk == 0 {
                    let crlf = read_line_crlf(reader)?;
                    if !crlf.trim_end_matches(['\r', '\n']).is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "chunk not followed by CRLF",
                        ));
                    }
                }
                Ok(n)
            }
        }
    }
}
