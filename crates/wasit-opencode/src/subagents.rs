//! Channel B: Kilo's own loopback HTTP + SSE server, observed by the driver.
//!
//! # Design note: why a bridge process
//!
//! The driver's [`crate::AcpProtocol`] is a pure line reducer: it sees only the lines the shim
//! journals from the CLI's stdout, and it writes only lines to the CLI's stdin. Channel B
//! (`GET /global/event`, `GET /kilocode/background-jobs`, `POST …/abort`, …) needs real I/O, and
//! the shim is CLI-agnostic, so nothing in the existing pipeline can talk HTTP. A protocol cannot
//! do it either, since it must stay pure and replayable.
//!
//! The choice: the session's process is `<host> kilo-bridge -- kilo acp …` ([`SUBCOMMAND`]), where
//! the host is the daemon binary itself (the same binary that runs the shim), so a host ships one
//! helper binary. The runtime launches it through [`Wrapper`] and [`subcommand_main`] runs
//! [`run_bridge`].
//! - The bridge spawns `kilo acp` with `--port <free> --hostname 127.0.0.1` and a random
//!   `KILO_SERVER_PASSWORD`, and forwards ACP stdin and stdout unchanged.
//! - Its own observations go into the same stdout as JSON-RPC notifications with method
//!   [`METHOD`] (`_wasit/kilo`). The shim journals them like any other line, so a replay
//!   reproduces them and the protocol reduces them as it reduces ACP.
//! - Commands go the other way: the protocol writes lines with method [`METHOD`] to the CLI's
//!   stdin. The bridge runs them as HTTP calls and never forwards them to `kilo acp`. Each result
//!   comes back as a notification.
//!
//! Details, each chosen to avoid sleeps, polling and retries:
//! - **Port.** `kilo acp --port 0` does not announce its port, so the bridge reserves one by
//!   binding `127.0.0.1:0` and releasing it. There is a small race with other processes; a lost
//!   race shows up as `state_lost` with reason `connect`.
//! - **Readiness.** The SSE stream opens when the first JSON-RPC response from `kilo acp` goes by.
//!   The recorded fixtures show `server.connected` about 12 ms after the `initialize` reply.
//! - **Loss.** An SSE stream that closes or errors emits `state_lost`. The bridge never reconnects
//!   and never reports the state as idle. A stream that ends after the protocol closed the
//!   bridge's stdin is a normal shutdown: no `state_lost`.
//! - **Content-free.** Notifications carry ids, statuses, counts and token numbers. Prompts,
//!   message text, tool input, permission patterns, job titles and metadata never leave the
//!   bridge.
//! - **Background jobs** are fetched when a session event shows that a child of a session is
//!   changing, not on a timer.
//!
//! # Notification kinds (`params.kind`)
//!
//! `session.created {session, parent?}`, `session.status {session, status}`,
//! `session.idle {session}`, `tokens {session, input, output, reasoning, cache_read,
//! cache_write, cost}`, `permission.asked {id, session, permission}`,
//! `background_jobs {session, jobs:[{id,type,status}]}`, `background_jobs_failed {session}`,
//! `state_lost {reason}` (`connect` | `stream_closed` | `stream_error`), `command_result
//! {command, status}`, `command_failed {command, reason, status?}`.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};
use wasit_core::wire::rpc_notification;

use crate::http::{self, is_path_id, Endpoint};

/// The host subcommand that runs the bridge (`<host> kilo-bridge -- kilo acp …`).
pub const SUBCOMMAND: &str = "kilo-bridge";

/// [`SUBCOMMAND`]'s entry point: `args` is the CLI argv (`kilo acp …`). Returns the exit code.
pub fn subcommand_main(args: &[String]) -> i32 {
    match run_bridge(args) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("kilo-bridge: {e}");
            2
        }
    }
}

/// JSON-RPC method of every bridge notification and of every control line (in both directions).
pub const METHOD: &str = "_wasit/kilo";

/// The basic-auth user the bridge gives `kilo acp`.
const USERNAME: &str = "wasit";

/// Upper bound for one command's HTTP exchange. Not a retry: the command fails once.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Serialized, line-atomic writer for the bridge's stdout (shared by its threads).
pub struct Out<W: Write>(Mutex<W>);

impl<W: Write> Out<W> {
    pub fn new(writer: W) -> Self {
        Out(Mutex::new(writer))
    }

    /// Write one bridge notification with `params`.
    pub fn notify(&self, params: &Value) {
        let line = rpc_notification(METHOD, Some(params.clone()));
        self.raw(line.as_str());
    }

    /// Write one complete line (a newline is added when missing).
    pub fn raw(&self, line: &str) {
        let mut w = self.0.lock().unwrap_or_else(|e| e.into_inner());
        // A closed stdout means the shim is gone; the bridge then ends with the CLI.
        let _ = w.write_all(line.as_bytes());
        if !line.ends_with('\n') {
            let _ = w.write_all(b"\n");
        }
        let _ = w.flush();
    }

    /// Run `f` with the writer (used by tests to read what was written).
    pub fn with<R>(&self, f: impl FnOnce(&mut W) -> R) -> R {
        f(&mut self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

fn str_of(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

fn num_of(v: Option<&Value>, key: &str) -> Value {
    json!(v
        .and_then(|x| x.get(key))
        .and_then(Value::as_f64)
        .unwrap_or(0.0))
}

fn state_lost(reason: &str) -> Value {
    json!({"kind": "state_lost", "reason": reason})
}

/// What one SSE event turned into: notifications, and a session whose background jobs changed.
#[derive(Debug, Default, PartialEq)]
pub struct Mapped {
    pub notes: Vec<Value>,
    pub refresh_jobs_of: Option<String>,
}

/// Maps the SSE events to notifications. Remembers which session is a child of which, so that a
/// change in a child asks for its parent's background jobs.
#[derive(Debug, Default)]
pub struct Tracker {
    parent_of: HashMap<String, String>,
}

impl Tracker {
    /// `payload` is one SSE event's `payload` object: `{type, properties}`.
    pub fn on_event(&mut self, payload: &Value) -> Mapped {
        let kind = payload.get("type").and_then(Value::as_str).unwrap_or("");
        let props = payload.get("properties").unwrap_or(&Value::Null);
        let mut m = Mapped::default();
        match kind {
            "session.created" => {
                let info = props.get("info");
                let session =
                    str_of(props, "sessionID").or_else(|| info.and_then(|i| str_of(i, "id")));
                let Some(session) = session else { return m };
                let mut note = json!({"kind": "session.created", "session": session});
                if let Some(parent) = info.and_then(|i| str_of(i, "parentID")) {
                    self.parent_of.insert(session.clone(), parent.clone());
                    note["parent"] = json!(parent);
                    m.refresh_jobs_of = Some(parent);
                }
                m.notes.push(note);
            }
            "session.status" => {
                let Some(session) = str_of(props, "sessionID") else {
                    return m;
                };
                let status = props
                    .get("status")
                    .and_then(|s| str_of(s, "type"))
                    .unwrap_or_default();
                m.notes
                    .push(json!({"kind": "session.status", "session": session, "status": status}));
                m.refresh_jobs_of = self.parent_of.get(&session).cloned();
            }
            "session.idle" => {
                let Some(session) = str_of(props, "sessionID") else {
                    return m;
                };
                m.notes
                    .push(json!({"kind": "session.idle", "session": session}));
                m.refresh_jobs_of = self.parent_of.get(&session).cloned();
            }
            "message.updated" => {
                let Some(info) = props.get("info") else {
                    return m;
                };
                let (Some(tokens), Some(session)) = (
                    info.get("tokens"),
                    str_of(info, "sessionID").or_else(|| str_of(props, "sessionID")),
                ) else {
                    return m;
                };
                if info.get("role").and_then(Value::as_str) != Some("assistant") {
                    return m;
                }
                let cache = tokens.get("cache");
                m.notes.push(json!({
                    "kind": "tokens",
                    "session": session,
                    "input": num_of(Some(tokens), "input"),
                    "output": num_of(Some(tokens), "output"),
                    "reasoning": num_of(Some(tokens), "reasoning"),
                    "cache_read": num_of(cache, "read"),
                    "cache_write": num_of(cache, "write"),
                    "cost": num_of(Some(info), "cost"),
                }));
            }
            "permission.asked" => {
                let (Some(id), Some(session)) = (str_of(props, "id"), str_of(props, "sessionID"))
                else {
                    return m;
                };
                // Only the tool name: the patterns are the command or path itself.
                m.notes.push(json!({
                    "kind": "permission.asked",
                    "id": id,
                    "session": session,
                    "permission": str_of(props, "permission").unwrap_or_default(),
                }));
            }
            _ => {}
        }
        m
    }
}

/// The notification for a `GET /kilocode/background-jobs` answer: ids, types and statuses only.
pub fn jobs_note(parent: &str, body: &Value) -> Option<Value> {
    let jobs: Vec<Value> = body
        .as_array()?
        .iter()
        .filter_map(|j| {
            Some(json!({
                "id": str_of(j, "id")?,
                "type": str_of(j, "type").unwrap_or_default(),
                "status": str_of(j, "status").unwrap_or_default(),
            }))
        })
        .collect();
    Some(json!({"kind": "background_jobs", "session": parent, "jobs": jobs}))
}

/// Which children an abort reaches: `Session` only the named session; `Tree` it and its children.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Session,
    Tree,
}

/// The answer to a permission request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    Once,
    Always,
    Reject,
}

/// A control command from the protocol (params of a [`METHOD`] line on the bridge's stdin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Message a running child session; it takes the text at its next step.
    Steer { session: String, text: String },
    /// `POST /session/{session}/abort?scope=…`.
    Abort { session: String, scope: Scope },
    /// `POST /permission/{id}/reply`.
    ReplyPermission { id: String, reply: Reply },
}

impl Command {
    /// The name reported in `command_result` and `command_failed`.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Steer { .. } => "steer",
            Command::Abort { .. } => "abort",
            Command::ReplyPermission { .. } => "reply_permission",
        }
    }
}

/// One HTTP exchange a command performs.
#[derive(Debug, Clone, PartialEq)]
pub struct Exchange {
    pub method: &'static str,
    pub path: String,
    pub body: Option<Value>,
}

/// The HTTP call for a command. Ids that are not plain identifiers, and empty steer text, are
/// refused before anything is sent.
pub fn exchange_for(cmd: &Command) -> Result<Exchange, String> {
    match cmd {
        Command::Steer { session, text } => {
            check_id(session)?;
            if text.trim().is_empty() {
                return Err("steer text is empty".into());
            }
            // The part's metadata marks it as a steer, as Kilo's own UI does.
            Ok(Exchange {
                method: "POST",
                path: format!("/session/{session}/prompt_async"),
                body: Some(json!({"parts": [{
                    "type": "text",
                    "text": text,
                    "metadata": {"kind": "subagent_steer"},
                }]})),
            })
        }
        Command::Abort { session, scope } => {
            check_id(session)?;
            let scope = match scope {
                Scope::Session => "session",
                Scope::Tree => "tree",
            };
            Ok(Exchange {
                method: "POST",
                path: format!("/session/{session}/abort?scope={scope}"),
                body: None,
            })
        }
        Command::ReplyPermission { id, reply } => {
            check_id(id)?;
            let reply = match reply {
                Reply::Once => "once",
                Reply::Always => "always",
                Reply::Reject => "reject",
            };
            Ok(Exchange {
                method: "POST",
                path: format!("/permission/{id}/reply"),
                body: Some(json!({"reply": reply})),
            })
        }
    }
}

fn check_id(id: &str) -> Result<(), String> {
    if is_path_id(id) {
        Ok(())
    } else {
        Err("id is not a plain identifier".into())
    }
}

/// Parse a stdin line: `None` when it is not a control line (it goes to `kilo acp` unchanged),
/// `Some(Err)` when it is one but malformed. The error text is fixed, never the input.
pub fn parse_control(line: &str) -> Option<Result<Command, String>> {
    if !line.contains(METHOD) {
        return None;
    }
    let v: Value = serde_json::from_str(line).ok()?;
    if v.get("method").and_then(Value::as_str) != Some(METHOD) {
        return None;
    }
    let params = v.get("params").cloned().unwrap_or(Value::Null);
    Some(command_from_params(&params).ok_or_else(|| "invalid control command".into()))
}

/// `{"command": "steer" | "abort" | "reply_permission", …fields}`. Field values are never
/// echoed into errors.
fn command_from_params(p: &Value) -> Option<Command> {
    let field = |k: &str| p.get(k).and_then(Value::as_str).map(str::to_string);
    match p.get("command")?.as_str()? {
        "steer" => Some(Command::Steer {
            session: field("session")?,
            text: field("text")?,
        }),
        "abort" => Some(Command::Abort {
            session: field("session")?,
            scope: match field("scope")?.as_str() {
                "session" => Scope::Session,
                "tree" => Scope::Tree,
                _ => return None,
            },
        }),
        "reply_permission" => Some(Command::ReplyPermission {
            id: field("id")?,
            reply: match field("reply")?.as_str() {
                "once" => Reply::Once,
                "always" => Reply::Always,
                "reject" => Reply::Reject,
                _ => return None,
            },
        }),
        _ => None,
    }
}

/// Run a command: its HTTP exchange, reported as `command_result` (2xx) or `command_failed`.
pub fn execute<W: Write>(ep: &Endpoint, cmd: &Command, out: &Out<W>) {
    let name = cmd.name();
    let exchange = match exchange_for(cmd) {
        Ok(x) => x,
        Err(_) => {
            out.notify(&json!({"kind": "command_failed", "command": name, "reason": "invalid"}));
            return;
        }
    };
    match http::request(
        ep,
        exchange.method,
        &exchange.path,
        exchange.body.as_ref(),
        COMMAND_TIMEOUT,
    ) {
        Ok(r) if r.is_success() => out.notify(&json!({
            "kind": "command_result", "command": name, "status": r.status,
        })),
        Ok(r) => out.notify(&json!({
            "kind": "command_failed", "command": name, "reason": "http", "status": r.status,
        })),
        Err(_) => out.notify(&json!({"kind": "command_failed", "command": name, "reason": "io"})),
    }
}

/// Read the server's event stream until it ends, reporting every mapped event. Background jobs
/// are fetched for the parent whose child changed. A stream that ends or fails ends the loop with
/// `state_lost`, unless `closing` is set: it is set once the protocol has closed the bridge's
/// stdin, and the CLI's server going away after that is the normal shutdown. Nothing reconnects.
pub fn run_events<W: Write + Send>(ep: &Endpoint, out: &Out<W>, closing: &AtomicBool) {
    let lost = |reason: &str| {
        if !closing.load(Ordering::SeqCst) {
            out.notify(&state_lost(reason));
        }
    };
    let mut stream = match http::open_events(ep, "/global/event") {
        Ok(s) => s,
        Err(_) => return lost("connect"),
    };
    let mut tracker = Tracker::default();
    loop {
        match stream.next_data() {
            Ok(Some(data)) => {
                // Each SSE data is the envelope `{directory, project, payload}`.
                let Some(payload) = serde_json::from_str::<Value>(&data)
                    .ok()
                    .and_then(|v| v.get("payload").cloned())
                else {
                    continue;
                };
                let mapped = tracker.on_event(&payload);
                for note in &mapped.notes {
                    out.notify(note);
                }
                if let Some(parent) = mapped.refresh_jobs_of {
                    refresh_jobs(ep, &parent, out);
                }
            }
            Ok(None) => return lost("stream_closed"),
            Err(_) => return lost("stream_error"),
        }
    }
}

fn refresh_jobs<W: Write>(ep: &Endpoint, parent: &str, out: &Out<W>) {
    let failed = json!({"kind": "background_jobs_failed", "session": parent});
    if !is_path_id(parent) {
        return out.notify(&failed);
    }
    let path = format!("/kilocode/background-jobs?sessionID={parent}");
    match http::request(ep, "GET", &path, None, COMMAND_TIMEOUT) {
        Ok(r) if r.is_success() => match r.json().and_then(|b| jobs_note(parent, &b)) {
            Some(note) => out.notify(&note),
            None => out.notify(&failed),
        },
        _ => out.notify(&failed),
    }
}

/// The bridge process: `kilo_argv` is `[kilo, "acp", …]`. Returns the CLI's exit code.
pub fn run_bridge(kilo_argv: &[String]) -> io::Result<i32> {
    let (program, args) = kilo_argv
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no CLI command given"))?;
    let port = free_port()?;
    let password = random_password()?;
    let mut child = wasit_process::std_command(program)
        .args(args)
        .args(["--port", &port.to_string(), "--hostname", "127.0.0.1"])
        .env("KILO_SERVER_USERNAME", USERNAME)
        .env("KILO_SERVER_PASSWORD", &password)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut kilo_in = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("no stdin pipe to the CLI"))?;
    let kilo_out = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("no stdout pipe from the CLI"))?;

    let out = Arc::new(Out::new(io::stdout()));
    let ep = Endpoint {
        port,
        basic: Some((USERNAME.to_string(), password)),
    };
    let (cmd_tx, cmd_rx) = mpsc::channel::<Command>();
    // Both the event stream and the command worker wait for the CLI's first response, when its
    // HTTP server is known to be listening. A CLI that exits first closes these channels.
    let (events_ready_tx, events_ready_rx) = mpsc::channel::<()>();
    let (cmds_ready_tx, cmds_ready_rx) = mpsc::channel::<()>();
    // Set once the protocol closes stdin: from then on the CLI's exit is not a loss.
    let closing = Arc::new(AtomicBool::new(false));

    {
        let (ep, out, closing) = (ep.clone(), out.clone(), closing.clone());
        // Ends without events when the CLI never answered.
        thread::spawn(move || {
            if events_ready_rx.recv().is_ok() {
                run_events(&ep, &*out, &closing);
            }
        });
    }
    {
        let (ep, out) = (ep.clone(), out.clone());
        thread::spawn(move || {
            let running = cmds_ready_rx.recv().is_ok();
            for cmd in cmd_rx {
                if running {
                    execute(&ep, &cmd, &*out);
                } else {
                    out.notify(&json!({
                        "kind": "command_failed", "command": cmd.name(), "reason": "not_running",
                    }));
                }
            }
        });
    }
    let pump = {
        let out = out.clone();
        thread::spawn(move || forward_stdout(kilo_out, &*out, vec![events_ready_tx, cmds_ready_tx]))
    };
    {
        let (out, closing) = (out.clone(), closing.clone());
        // Not joined: it blocks on the bridge's own stdin until the shim closes it.
        thread::spawn(move || {
            forward_stdin(io::stdin().lock(), &mut kilo_in, &cmd_tx, &*out);
            // Before `kilo_in` drops, so the CLI can only exit after this is visible.
            closing.store(true, Ordering::SeqCst);
        });
    }
    let status = child.wait()?;
    let _ = pump.join();
    Ok(status.code().unwrap_or(1))
}

/// Copy the CLI's stdout to the bridge's, line by line. The first JSON-RPC response releases
/// `ready` (one signal per waiter).
fn forward_stdout<W: Write>(
    kilo_out: std::process::ChildStdout,
    out: &Out<W>,
    ready: Vec<mpsc::Sender<()>>,
) {
    let mut reader = BufReader::new(kilo_out);
    let mut ready = Some(ready);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if ready.is_some() && is_response(&line) {
            for tx in ready.take().unwrap_or_default() {
                let _ = tx.send(());
            }
        }
        out.raw(&line);
    }
}

fn is_response(line: &str) -> bool {
    serde_json::from_str::<Value>(line).is_ok_and(|v| {
        v.get("id").is_some()
            && v.get("method").is_none()
            && (v.get("result").is_some() || v.get("error").is_some())
    })
}

/// Copy the protocol's stdin to the CLI's, except control lines, which go to `cmds`.
fn forward_stdin<R: BufRead, W: Write, C: Write>(
    input: R,
    kilo_in: &mut C,
    cmds: &mpsc::Sender<Command>,
    out: &Out<W>,
) {
    for line in input.lines() {
        let Ok(line) = line else { break };
        match parse_control(&line) {
            Some(Ok(cmd)) => {
                let _ = cmds.send(cmd);
            }
            Some(Err(detail)) => out.notify(&json!({
                "kind": "command_failed", "command": "unknown", "reason": "invalid",
                "detail": detail,
            })),
            None => {
                if writeln!(kilo_in, "{line}")
                    .and_then(|()| kilo_in.flush())
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

fn free_port() -> io::Result<u16> {
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))?
        .local_addr()?
        .port())
}

/// 24 random bytes as hex, from the OS random source (`getrandom`: the same call on every
/// platform, including Windows).
fn random_password() -> io::Result<String> {
    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes).map_err(|e| io::Error::other(format!("random source: {e}")))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests;
