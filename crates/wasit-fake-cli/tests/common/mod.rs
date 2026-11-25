//! Shared test helpers: spawning the fake binary as a client would, and JSON shape comparison.
#![allow(dead_code)]

use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};
use wasit_fake_cli::scenario::Scenario;

pub const BIN: &str = env!("CARGO_BIN_EXE_wasit-fake-cli");
pub const WAIT: Duration = Duration::from_secs(15);

pub const CLAUDE_ARGV: [&str; 10] = [
    "--print",
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    "--verbose",
    "--model",
    "haiku",
    "--permission-prompt-tool",
    "stdio",
];

/// A running fake CLI, driven over its stdin/stdout.
pub struct Fake {
    child: Child,
    stdin: Option<ChildStdin>,
    rx: Receiver<String>,
    _dir: tempdir::Dir,
    /// Every JSON line received so far (raw lines that are not JSON are kept in `raw`).
    pub seen: Vec<Value>,
    pub raw: Vec<String>,
    stderr: std::sync::Arc<std::sync::Mutex<String>>,
    stderr_reader: Option<std::thread::JoinHandle<()>>,
}

mod tempdir {
    pub struct Dir(pub std::path::PathBuf);
    impl Dir {
        pub fn new() -> Dir {
            let p = std::env::temp_dir().join(format!(
                "wasit-fake-test-{}-{}",
                std::process::id(),
                wasit_fake_cli::util::hex(10)
            ));
            std::fs::create_dir_all(&p).expect("create temp dir");
            Dir(p)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            // Best-effort cleanup of a scratch directory.
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// A scratch directory for tests that run several fake processes over the same "agent storage".
pub fn scratch_dir() -> impl AsRef<std::path::Path> {
    struct Scratch(tempdir::Dir);
    impl AsRef<std::path::Path> for Scratch {
        fn as_ref(&self) -> &std::path::Path {
            &(self.0).0
        }
    }
    Scratch(tempdir::Dir::new())
}

impl Fake {
    /// A fake with storage of its own (nothing it stores is seen by another fake).
    pub fn spawn(args: &[&str], persona: Option<&str>, scenario: &Scenario) -> Fake {
        Fake::spawn_in(args, persona, scenario, None, None)
    }

    /// A fake over the storage `state` (several fakes over one `state` are successive runs of
    /// one CLI) running in `cwd`; `None`: its own.
    pub fn spawn_in(
        args: &[&str],
        persona: Option<&str>,
        scenario: &Scenario,
        state: Option<&std::path::Path>,
        cwd: Option<&std::path::Path>,
    ) -> Fake {
        let dir = tempdir::Dir::new();
        let scenario_path = dir.0.join("scenario.json");
        scenario.write_to(&scenario_path).expect("write scenario");
        let own_state = dir.0.join("state");
        let mut cmd = Command::new(BIN);
        cmd.args(args)
            .env("WASIT_FAKE_SCENARIO", &scenario_path)
            .env("WASIT_FAKE_STATE_DIR", state.unwrap_or(&own_state))
            .env_remove("CLAUDE_CONFIG_DIR")
            .current_dir(cwd.unwrap_or(&dir.0))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match persona {
            Some(p) => cmd.env("WASIT_FAKE_PERSONA", p),
            None => cmd.env_remove("WASIT_FAKE_PERSONA"),
        };
        let mut child = cmd.spawn().expect("spawn wasit-fake-cli");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout");
        let stderr = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let err_pipe = child.stderr.take().expect("stderr");
        let sink = stderr.clone();
        let stderr_reader = std::thread::spawn(move || {
            for line in BufReader::new(err_pipe).lines().map_while(Result::ok) {
                if let Ok(mut s) = sink.lock() {
                    s.push_str(&line);
                    s.push('\n');
                }
            }
        });
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        Fake {
            child,
            stdin,
            rx,
            _dir: dir,
            seen: vec![],
            raw: vec![],
            stderr,
            stderr_reader: Some(stderr_reader),
        }
    }

    /// Everything the fake has written to stderr so far.
    pub fn stderr(&self) -> String {
        self.stderr.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Everything the fake wrote to stderr: closes its stdin and reads stderr to its end (the
    /// reader thread lags behind stdout otherwise).
    pub fn stderr_to_end(&mut self) -> String {
        self.close_stdin();
        if let Some(t) = self.stderr_reader.take() {
            // The reader ends at EOF, when the fake exits; it does not panic.
            let _ = t.join();
        }
        self.stderr()
    }

    pub fn send(&mut self, v: &Value) {
        assert!(self.send_if_open(v), "the fake exited before reading {v}");
    }

    /// Sends `v`, unless the fake has already exited. The fake's stdin read end closes only when
    /// the process exits, so a write failing with `BrokenPipe` means it is gone; a scripted exit
    /// (a refused start) can happen before the test writes, so that case is reported, not
    /// panicked on. Any other write error is a real failure. Returns whether `v` was delivered.
    pub fn send_if_open(&mut self, v: &Value) -> bool {
        let result = {
            let stdin = self.stdin.as_mut().expect("stdin open");
            writeln!(stdin, "{v}").and_then(|()| stdin.flush())
        };
        match result {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                // The child has already exited; drop the handle so further writes are impossible.
                self.stdin = None;
                false
            }
            Err(e) => panic!("write to fake: {e}"),
        }
    }

    pub fn close_stdin(&mut self) {
        self.stdin = None;
    }

    /// Next JSON message within `timeout`; non-JSON lines are stored in `raw` and skipped.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Option<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.rx.recv_timeout(left) {
                Ok(line) => match serde_json::from_str::<Value>(&line) {
                    Ok(v) => {
                        self.seen.push(v.clone());
                        return Some(v);
                    }
                    Err(_) => self.raw.push(line),
                },
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => {
                    return None
                }
            }
        }
    }

    /// Next raw line (JSON or not) within `timeout`.
    pub fn recv_raw(&mut self, timeout: Duration) -> Option<String> {
        self.rx.recv_timeout(timeout).ok()
    }

    pub fn recv(&mut self) -> Value {
        self.recv_timeout(WAIT)
            .expect("timed out waiting for the fake")
    }

    /// Receive until `pred` matches; returns every message received, the match last.
    pub fn recv_until(&mut self, pred: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut out = vec![];
        loop {
            let m = self.recv_timeout(WAIT).unwrap_or_else(|| {
                panic!("timed out waiting for a matching message; got so far: {out:#?}")
            });
            let hit = pred(&m);
            out.push(m);
            if hit {
                return out;
            }
        }
    }

    /// Receive until nothing arrives for `quiet`.
    pub fn drain(&mut self, quiet: Duration) -> Vec<Value> {
        let mut out = vec![];
        while let Some(m) = self.recv_timeout(quiet) {
            out.push(m);
        }
        out
    }

    /// Wait for the process to exit.
    pub fn wait_exit(&mut self) -> Option<ExitStatus> {
        let deadline = Instant::now() + WAIT;
        while Instant::now() < deadline {
            if let Ok(Some(s)) = self.child.try_wait() {
                return Some(s);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn kill(&mut self) {
        // The child may already have exited; either way it is gone afterwards.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Keys whose value is opaque payload (tool inputs, per-id maps): only the JSON kind is compared.
const OPAQUE: &[&str] = &[
    "input",
    "rawInput",
    "rawOutput",
    "wire_tool_inputs",
    "modelUsage",
    "model_usage_since_last_result",
    "tool_use_result",
    "tool_result_meta",
    "answers",
    "updatedInput",
    "_meta",
    "metadata",
    "permission_suggestions",
    "capabilities",
    "settings",
    "threadSettings",
    "readStates",
    "startup_timing",
    "process_turn_states",
    "time_to_request_phases_ms",
    "process_stretches",
];

/// Keys the real CLI only sometimes includes; a fake may omit them.
const OPTIONAL: &[&str] = &["blocked_path", "cachedReadTokens", "thoughtTokens"];

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Differences between the structure of `got` and `want` (same keys, same JSON kinds, element
/// shape of arrays); values are ignored. `null` on either side matches anything (optional field).
pub fn shape_diff(got: &Value, want: &Value) -> Vec<String> {
    let mut diffs = vec![];
    walk(got, want, "$", &mut diffs);
    diffs
}

fn walk(got: &Value, want: &Value, path: &str, out: &mut Vec<String>) {
    if got.is_null() || want.is_null() {
        return;
    }
    if kind(got) != kind(want) {
        out.push(format!("{path}: kind {} != {}", kind(got), kind(want)));
        return;
    }
    match (got, want) {
        (Value::Object(g), Value::Object(w)) => {
            let leaf = path.rsplit('.').next().unwrap_or("");
            if OPAQUE.contains(&leaf) {
                return;
            }
            for k in w.keys() {
                if !g.contains_key(k) && !OPTIONAL.contains(&k.as_str()) {
                    out.push(format!("{path}.{k}: missing in fake output"));
                }
            }
            for k in g.keys() {
                if !w.contains_key(k) && !OPTIONAL.contains(&k.as_str()) {
                    out.push(format!("{path}.{k}: not in the real CLI output"));
                }
            }
            for (k, wv) in w {
                if let Some(gv) = g.get(k) {
                    walk(gv, wv, &format!("{path}.{k}"), out);
                }
            }
        }
        (Value::Array(g), Value::Array(w)) => {
            if let (Some(g0), Some(w0)) = (g.first(), w.first()) {
                walk(g0, w0, &format!("{path}[0]"), out);
            }
        }
        _ => {}
    }
}
