//! Process I/O for the fake: a stdin reader thread feeding a channel (so a turn can wait on
//! "a line arrived or the deadline passed"), line-flushed stdout, stderr, and an optional
//! transcript file (`WASIT_FAKE_TRANSCRIPT`) recording every line in both directions.

use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, Write};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Puts lines on the input queue from a thread of the fake's own (a server thread answering HTTP
/// requests for the engine). The queue closes when stdin does: after that [`Injector::inject`]
/// refuses, instead of keeping the queue open.
#[derive(Clone)]
pub struct Injector(Arc<Mutex<Option<Sender<String>>>>);

impl Injector {
    /// `false` when the queue is closed (stdin ended).
    pub fn inject(&self, line: String) -> bool {
        let slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        slot.as_ref().is_some_and(|tx| tx.send(line).is_ok())
    }
}

/// Result of a timed receive.
#[derive(Debug, PartialEq, Eq)]
pub enum Recv {
    Line(String),
    Timeout,
    Eof,
}

pub struct Io {
    rx: Receiver<String>,
    transcript: Option<File>,
    inject: Injector,
}

impl Io {
    /// Take over this process's stdin.
    pub fn stdio() -> Io {
        let (tx, rx) = mpsc::channel();
        // The reader's sender and the injector's share one slot: stdin's end clears it, so the
        // queue disconnects (EOF) once no sender is left.
        let slot = Arc::new(Mutex::new(Some(tx.clone())));
        let feed = slot.clone();
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            let mut lock = stdin.lock();
            let mut buf = String::new();
            loop {
                buf.clear();
                match lock.read_line(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let line = buf.trim_end_matches(['\n', '\r']).to_string();
                        if line.trim().is_empty() {
                            continue;
                        }
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
            // Dropping the senders is the EOF signal.
            *feed.lock().unwrap_or_else(|e| e.into_inner()) = None;
        });
        let transcript = std::env::var_os("WASIT_FAKE_TRANSCRIPT").and_then(|p| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .ok()
        });
        Io {
            rx,
            transcript,
            inject: Injector(slot),
        }
    }

    /// A handle that puts lines on this process's input queue (see [`Injector`]).
    pub fn injector(&self) -> Injector {
        self.inject.clone()
    }

    fn log(&mut self, dir: &str, line: &str) {
        if let Some(f) = self.transcript.as_mut() {
            let msg: Value =
                serde_json::from_str(line).unwrap_or_else(|_| serde_json::json!({ "_raw": line }));
            // The fake's own plumbing (see [`Injector`]) is not part of the conversation.
            if msg["method"]
                .as_str()
                .is_some_and(|m| m.starts_with("_fake/"))
            {
                return;
            }
            let entry = serde_json::json!({ "dir": dir, "msg": msg });
            // Transcript writes are best effort: a failing test log must not break the protocol.
            let _ = writeln!(f, "{entry}");
        }
    }

    /// Block for the next line; `None` on EOF.
    pub fn recv(&mut self) -> Option<String> {
        let line = self.rx.recv().ok()?;
        self.log("in", &line);
        Some(line)
    }

    pub fn recv_timeout(&mut self, d: Duration) -> Recv {
        match self.rx.recv_timeout(d) {
            Ok(line) => {
                self.log("in", &line);
                Recv::Line(line)
            }
            Err(RecvTimeoutError::Timeout) => Recv::Timeout,
            Err(RecvTimeoutError::Disconnected) => Recv::Eof,
        }
    }

    /// Write one JSON value as a line on stdout.
    pub fn send(&mut self, v: &Value) {
        let line = v.to_string();
        self.send_raw(&line);
    }

    /// Write one raw line on stdout (may be malformed on purpose).
    pub fn send_raw(&mut self, line: &str) {
        self.log("out", line);
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        // A closed stdout means the driver is gone; the next recv() sees EOF and we exit.
        let _ = writeln!(lock, "{line}");
        let _ = lock.flush();
    }

    pub fn stderr(&mut self, text: &str) {
        let mut e = std::io::stderr().lock();
        // Same reasoning as stdout: nothing useful to do if stderr is closed.
        let _ = e.write_all(text.as_bytes());
        if !text.ends_with('\n') {
            let _ = e.write_all(b"\n");
        }
        let _ = e.flush();
    }
}
