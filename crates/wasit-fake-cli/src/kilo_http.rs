//! The loopback HTTP and SSE server of `kilo acp --port P --hostname H`, as the kilo-bridge uses it.
//!
//! Routes: `GET /global/event` (server-sent events) is answered here. Every other route is a
//! request the engine answers ([`crate::acp`] holds the answers): the bridge's `GET /session/status`,
//! `GET /session/{id}/children`, `GET /kilocode/background-jobs`, `GET /permission`, and its
//! `POST /session/{id}/abort`, `POST /session/{id}/prompt_async`, `POST /permission/{id}/reply`.
//! Anything else is 404. Basic auth is required when `KILO_SERVER_PASSWORD` is set (user
//! `KILO_SERVER_USERNAME`, default `kilo`), as the real server does.
//!
//! Server threads never touch the protocol or the session state. A request becomes a line on the
//! engine's input queue ([`crate::io::Injector`]), and the engine answers it through
//! [`Hub::answer`]. So the engine stays the only writer of stdout, and an HTTP request takes
//! effect at the same point in the turn as a line from the client would.
//!
//! The first event subscriber is waited for before the first event is published. The bridge
//! subscribes right after the `initialize` response, while the driver may already be sending
//! `session/new`; without that wait the session's first events could go to nobody. A process
//! that never gets a subscriber blocks at its first event, which the bridge's connection makes
//! impossible in practice.

use crate::io::Injector;
use crate::util::alnum;
use base64::Engine as _;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

/// The method of the input-queue lines that carry an HTTP request to the engine.
pub const REQUEST_METHOD: &str = "_fake/http";

/// Largest request body the server reads (the bridge's bodies are a few hundred bytes).
const MAX_BODY: usize = 1 << 20;

/// The engine's answer to one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    /// JSON text, or empty (a 204).
    pub body: String,
}

impl Answer {
    pub fn json(status: u16, value: &Value) -> Answer {
        Answer {
            status,
            body: value.to_string(),
        }
    }

    pub fn empty(status: u16) -> Answer {
        Answer {
            status,
            body: String::new(),
        }
    }
}

#[derive(Default)]
struct Subscribers {
    /// One channel per open event stream; the server thread of each writes what it receives.
    streams: Vec<Sender<String>>,
    /// A stream has been opened at least once.
    seen: bool,
}

/// What the engine and the server threads share.
pub struct Hub {
    directory: String,
    inject: Injector,
    subs: Mutex<Subscribers>,
    subs_changed: Condvar,
    pending: Mutex<HashMap<u64, Sender<Answer>>>,
    next_request: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Hub {
    pub fn new(directory: String, inject: Injector) -> Arc<Hub> {
        Arc::new(Hub {
            directory,
            inject,
            subs: Mutex::new(Subscribers::default()),
            subs_changed: Condvar::new(),
            pending: Mutex::new(HashMap::new()),
            next_request: AtomicU64::new(1),
        })
    }

    /// The SSE envelope of one event: `{directory, project, payload: {id, type, properties}}`.
    fn envelope(&self, payload_type: &str, properties: Value) -> String {
        json!({
            "directory": self.directory,
            "project": "global",
            "payload": {
                "id": format!("evt_{}", alnum(26)),
                "type": payload_type,
                "properties": properties,
            },
        })
        .to_string()
    }

    /// Send an event to every open event stream (waiting for the first one, see the module doc).
    pub fn publish(&self, payload_type: &str, properties: Value) {
        let data = self.envelope(payload_type, properties);
        let mut subs = lock(&self.subs);
        while !subs.seen {
            subs = self
                .subs_changed
                .wait(subs)
                .unwrap_or_else(|e| e.into_inner());
        }
        subs.streams.retain(|tx| tx.send(data.clone()).is_ok());
    }

    /// Open an event stream: its first event is `server.connected`. Registered under the same
    /// lock `publish` takes, so no event falls between the greeting and the stream.
    fn subscribe(&self) -> Receiver<String> {
        let (tx, rx) = mpsc::channel();
        let _ = tx.send(self.envelope("server.connected", json!({})));
        let mut subs = lock(&self.subs);
        subs.streams.push(tx);
        subs.seen = true;
        self.subs_changed.notify_all();
        rx
    }

    /// Hand the engine's answer to the request `id` to the server thread waiting for it.
    pub fn answer(&self, id: u64, answer: Answer) {
        if let Some(tx) = lock(&self.pending).remove(&id) {
            let _ = tx.send(answer);
        }
    }

    /// Ask the engine for an answer and wait for it. `None` when the input queue is closed.
    fn ask(&self, method: &str, target: &str, body: &str) -> Option<Answer> {
        let id = self.next_request.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel();
        lock(&self.pending).insert(id, tx);
        let line = json!({"jsonrpc": "2.0", "method": REQUEST_METHOD, "params": {
            "id": id, "method": method, "path": target, "body": body,
        }})
        .to_string();
        if !self.inject.inject(line) {
            lock(&self.pending).remove(&id);
            return None;
        }
        rx.recv().ok()
    }
}

/// Start the server `kilo acp` runs when given `--port`, from the CLI's argv and environment.
/// `None` when there is no `--port` (the CLI then serves ACP only).
pub fn start_from_argv(args: &[String], inject: Injector) -> Result<Option<Arc<Hub>>, String> {
    let Some(port) = args.iter().position(|a| a == "--port") else {
        return Ok(None);
    };
    let port: u16 = args
        .get(port + 1)
        .and_then(|p| p.parse().ok())
        .ok_or("--port needs a port number")?;
    let host = args
        .iter()
        .position(|a| a == "--hostname")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".into());
    let auth = std::env::var("KILO_SERVER_PASSWORD").ok().map(|password| {
        let user = std::env::var("KILO_SERVER_USERNAME").unwrap_or_else(|_| "kilo".into());
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"))
        )
    });
    let directory = std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "/".into());
    // Bound before anything is answered: the bridge connects as soon as it sees a response.
    let listener = TcpListener::bind((host.as_str(), port))
        .map_err(|e| format!("cannot listen on {host}:{port}: {e}"))?;
    let hub = Hub::new(directory, inject);
    let served = hub.clone();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (hub, auth) = (served.clone(), auth.clone());
            thread::spawn(move || {
                let _ = handle(stream, &hub, auth.as_deref());
            });
        }
    });
    Ok(Some(hub))
}

/// One request on one connection (`Connection: close`: a request per connection).
fn handle(stream: TcpStream, hub: &Hub, auth: Option<&str>) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut out = stream;
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(());
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let mut authorization = None;
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            match name.trim().to_ascii_lowercase().as_str() {
                "authorization" => authorization = Some(value.to_string()),
                "content-length" => length = value.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    if auth.is_some_and(|a| authorization.as_deref() != Some(a)) {
        return write_response(&mut out, &Answer::empty(401));
    }
    if method == "GET" && target.split('?').next() == Some("/global/event") {
        return event_stream(out, hub);
    }
    if length > MAX_BODY {
        return write_response(&mut out, &Answer::empty(413));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    let body = String::from_utf8_lossy(&body).into_owned();
    let answer = hub
        .ask(&method, &target, &body)
        .unwrap_or_else(|| Answer::empty(503));
    write_response(&mut out, &answer)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        401 => "Unauthorized",
        404 => "Not Found",
        413 => "Payload Too Large",
        _ => "Service Unavailable",
    }
}

fn write_response(out: &mut impl Write, a: &Answer) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        a.status,
        reason(a.status),
        a.body.len()
    );
    out.write_all(head.as_bytes())?;
    out.write_all(a.body.as_bytes())?;
    out.flush()
}

/// Serve the event stream until the client goes away. The body is not length-delimited: it ends
/// when the connection closes, which the bridge's reader accepts.
fn event_stream(mut out: TcpStream, hub: &Hub) -> io::Result<()> {
    out.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
    )?;
    let events = hub.subscribe();
    for data in events {
        out.write_all(format!("data: {data}\n\n").as_bytes())?;
        out.flush()?;
    }
    Ok(())
}
