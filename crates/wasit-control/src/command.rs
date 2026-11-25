//! "A command whose stdio is the link": `ssh host marsad-agent bridge`, `wsl.exe -d X -- …`,
//! or any argv.

use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tokio::task::AbortHandle;

use wasit_diag::Ident;

use crate::attempt::Attempt;
use crate::client::{ClientOptions, ControlClient, Diagnostics, TransportFacts};
use crate::diag;
use crate::error::ConnectError;
use crate::transport::{join_duplex, BoxDuplex};

/// What went wrong with a transport command, as far as the UI needs to explain it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum CommandFailureKind {
    /// Authentication needs a human (password, missing key, agent).
    AuthNeeded,
    /// The host key is not known yet (needs confirming).
    HostKeyUnknown,
    /// The host key changed (possible MITM; needs a human).
    HostKeyChanged,
    /// Network-level failure (DNS, refused, timeout, no route).
    Unreachable,
    /// The remote helper (e.g. `marsad-agent`) is not installed.
    CommandNotFound,
    /// WSL has no distribution with the configured name.
    UnknownDistro,
    /// WSL itself is missing or has no distribution installed.
    WslUnavailable,
    /// A program could not be executed (not executable, access denied) — not a sign-in problem.
    PermissionDenied,
    /// There is no helper build for the host's OS/architecture.
    UnsupportedPlatform,
    /// The helper build for the host's platform is missing from this installation.
    HelperMissing,
    /// The host answered with something that is not the helper's protocol (a shell banner, an
    /// error page, …).
    UnexpectedOutput,
    /// The remote program the link runs (the helper, e.g. `marsad-agent bridge`) was started
    /// and then ended by itself, with a status that is its own and nothing else the classifier
    /// recognises, before the handshake completed: running the same command again cannot help.
    /// Only decided where the transport's exit-status contract says the status is the remote
    /// program's ([`TransportKind::remote_status`]).
    HelperFailed,
    Other,
}

impl CommandFailureKind {
    /// The kind as the word it serializes to (`auth_needed`, `host_key_unknown`, …): what
    /// diagnostics record of a failed transport.
    pub fn code(self) -> &'static str {
        match self {
            CommandFailureKind::AuthNeeded => "auth_needed",
            CommandFailureKind::HostKeyUnknown => "host_key_unknown",
            CommandFailureKind::HostKeyChanged => "host_key_changed",
            CommandFailureKind::Unreachable => "unreachable",
            CommandFailureKind::CommandNotFound => "command_not_found",
            CommandFailureKind::UnknownDistro => "unknown_distro",
            CommandFailureKind::WslUnavailable => "wsl_unavailable",
            CommandFailureKind::PermissionDenied => "permission_denied",
            CommandFailureKind::UnsupportedPlatform => "unsupported_platform",
            CommandFailureKind::HelperMissing => "helper_missing",
            CommandFailureKind::UnexpectedOutput => "unexpected_output",
            CommandFailureKind::HelperFailed => "helper_failed",
            CommandFailureKind::Other => "other",
        }
    }

    /// Needs a human action before retrying makes sense.
    pub fn needs_human(self) -> bool {
        matches!(
            self,
            CommandFailureKind::AuthNeeded
                | CommandFailureKind::HostKeyUnknown
                | CommandFailureKind::HostKeyChanged
        )
    }

    /// A configuration problem: retrying on a timer cannot fix it; the link fails until the
    /// user changes something and asks again.
    pub fn is_configuration(self) -> bool {
        matches!(
            self,
            CommandFailureKind::UnknownDistro
                | CommandFailureKind::WslUnavailable
                | CommandFailureKind::PermissionDenied
                | CommandFailureKind::UnsupportedPlatform
                | CommandFailureKind::HelperMissing
                | CommandFailureKind::UnexpectedOutput
                | CommandFailureKind::HelperFailed
        )
    }

    /// Worth retrying with backoff (the network or the peer may come back).
    pub fn is_transient(self) -> bool {
        matches!(
            self,
            CommandFailureKind::Unreachable | CommandFailureKind::Other
        )
    }
}

/// Classify a failed ssh/wsl/command invocation from its exit code and stderr (raw bytes may be
/// UTF-16LE, as `wsl.exe` writes them: decode with [`wasit_process::decode_console_output`]
/// first). Pure.
pub fn classify_command_failure(exit_code: Option<i32>, stderr: &str) -> CommandFailureKind {
    let s = stderr.to_ascii_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|n| s.contains(n));

    if has(&[
        "there is no distribution with the supplied name",
        "wsl_e_distro_not_found",
    ]) {
        return CommandFailureKind::UnknownDistro;
    }
    if has(&[
        "windows subsystem for linux has no installed distributions",
        "windows subsystem for linux is not installed",
        "wsl_e_default_distro_not_found",
        "the windows subsystem for linux optional component is not enabled",
        "wsl.exe: not found",
    ]) {
        return CommandFailureKind::WslUnavailable;
    }

    if has(&[
        "remote host identification has changed",
        "host key for",
        "offending ",
    ]) && has(&["changed", "offending"])
    {
        return CommandFailureKind::HostKeyChanged;
    }
    if has(&[
        "host key verification failed",
        "authenticity of host",
        "no ed25519 host key is known",
        "no rsa host key is known",
        "no ecdsa host key is known",
        "host key is known for",
    ]) {
        return CommandFailureKind::HostKeyUnknown;
    }
    if has(&[
        "permission denied (",
        "permission denied, please try again",
        "too many authentication failures",
        "authentication failed",
        "no supported authentication methods",
        "password:",
        "passphrase for key",
        "keyboard-interactive",
    ]) {
        return CommandFailureKind::AuthNeeded;
    }
    if has(&[
        "could not resolve hostname",
        "name or service not known",
        "temporary failure in name resolution",
        "connection refused",
        "connection timed out",
        "operation timed out",
        "no route to host",
        "network is unreachable",
        "connection reset",
        "connection closed by",
        "kex_exchange_identification",
        "timeout, server",
    ]) {
        return CommandFailureKind::Unreachable;
    }
    if exit_code == Some(126) || has(&["permission denied", "cannot execute", "access is denied"]) {
        return CommandFailureKind::PermissionDenied;
    }
    if exit_code == Some(127)
        || has(&[
            "command not found",
            ": not found",
            "no such file or directory",
            "is not recognized as an internal or external command",
            "the system cannot find the file specified",
        ])
    {
        return CommandFailureKind::CommandNotFound;
    }
    CommandFailureKind::Other
}

/// The program that carries a link, as far as the meaning of its exit status goes. Decided from
/// the program's file name ([`TransportKind::of_program`]); the one place that says what each
/// transport's status means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    /// `ssh`: exits with the remote command's own status, or with 255 when ssh itself failed
    /// (ssh(1), "EXIT STATUS").
    Ssh,
    /// `wsl.exe`: forwards the Linux program's status, but also exits with its own statuses
    /// (1 and HRESULT-derived values) for its own failures, whose messages are localised.
    /// Nothing here separates the two for sure and this could not be verified (no Windows
    /// available), so a status of `wsl.exe` is never taken to be the remote program's.
    Wsl,
    /// Any other command: its exit-status contract is unknown, so is never taken to be the
    /// remote program's.
    Other,
}

/// ssh's own failure status (see [`TransportKind::Ssh`]).
const SSH_OWN_FAILURE: i32 = 255;

impl TransportKind {
    /// The kind of transport `program` (the first element of a command's argv, a bare name or
    /// a path with `/` or `\` separators, with or without `.exe`) is.
    pub fn of_program(program: &str) -> TransportKind {
        let name = program.rsplit(['/', '\\']).next().unwrap_or(program);
        let name = name.to_ascii_lowercase();
        match name.strip_suffix(".exe").unwrap_or(&name) {
            "ssh" => TransportKind::Ssh,
            "wsl" => TransportKind::Wsl,
            _ => TransportKind::Other,
        }
    }

    /// Whether exit status `code` of this transport is, by the transport's contract, the status
    /// of the remote program it ran (and not of the transport itself).
    pub fn remote_status(self, code: i32) -> bool {
        match self {
            TransportKind::Ssh => (1..SSH_OWN_FAILURE).contains(&code),
            // Not verified, see the variants.
            TransportKind::Wsl | TransportKind::Other => false,
        }
    }
}

/// Classify the end of the transport command of a link whose handshake did not complete, the
/// remote program having been handed the connection. As [`classify_command_failure`] (what the
/// words say), and additionally [`CommandFailureKind::HelperFailed`] when nothing is recognised
/// but the child ended with a status that is the remote program's own
/// ([`TransportKind::remote_status`]). A signal, a clean exit, a status that is the
/// transport's own and any transport whose contract is unknown stay what the words make of
/// them (usually [`CommandFailureKind::Other`]: retried). Pure.
pub fn classify_helper_exit(
    transport: TransportKind,
    exit: Option<ExitInfo>,
    stderr: &str,
) -> CommandFailureKind {
    let code = exit.and_then(|e| e.code);
    let kind = classify_command_failure(code, stderr);
    if kind == CommandFailureKind::Other && code.is_some_and(|c| transport.remote_status(c)) {
        CommandFailureKind::HelperFailed
    } else {
        kind
    }
}

/// How to start the transport command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    /// Bytes of stderr kept (the tail) for diagnostics.
    #[serde(default = "default_stderr_limit")]
    pub stderr_limit: usize,
    /// The link attempt this command serves. With it, spawning and exiting are recorded
    /// (`transport.spawned`, `transport.exited`); without it nothing is. Never serialized.
    #[serde(skip)]
    pub trace: Option<TransportTrace>,
}

/// The link attempt a transport command belongs to (see [`CommandSpec::trace`]): the link's
/// diagnostics label, the host the link leads to (when it has one, as the attempt carries it)
/// and the attempt the supervisor passed to the connector, so the transport's records join the
/// supervisor's on `(link, n)` and, should the supervisor end the attempt, what the transport
/// had said is known to it ([`Attempt`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportTrace {
    pub link: Ident,
    pub host: Option<Ident>,
    pub attempt: Attempt,
}

fn default_stderr_limit() -> usize {
    16 * 1024
}

impl CommandSpec {
    pub fn new<I, S>(argv: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        CommandSpec {
            argv: argv.into_iter().map(Into::into).collect(),
            env: Vec::new(),
            stderr_limit: default_stderr_limit(),
            trace: None,
        }
    }

    /// Records this command's spawn and exit as belonging to `attempt` of `link`, with the host
    /// the attempt's link leads to (`SupervisedOptions::diag_host`), and makes the command known
    /// to the attempt (what it says, if the attempt is cut short).
    pub fn traced(mut self, link: Ident, attempt: &Attempt) -> Self {
        self.trace = Some(TransportTrace {
            link,
            host: attempt.host().cloned(),
            attempt: attempt.clone(),
        });
        self
    }
}

/// Exit of a transport child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitInfo {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl std::fmt::Display for ExitInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.code, self.signal) {
            (Some(c), _) => write!(f, "exit code {c}"),
            (None, Some(s)) => write!(f, "killed by signal {s}"),
            (None, None) => f.write_str("exited"),
        }
    }
}

struct Tail {
    buf: VecDeque<u8>,
    limit: usize,
    /// Bytes the child wrote to stderr in all (`buf` keeps only the last `limit`).
    seen: u64,
}

/// A read-only look at a transport child: its exit and its stderr tail. Does not keep the child
/// alive.
#[derive(Clone)]
pub(crate) struct ChildView {
    exit: watch::Receiver<Option<ExitInfo>>,
    stderr: Arc<Mutex<Tail>>,
}

impl ChildView {
    pub(crate) fn stderr_tail(&self) -> String {
        let t = self.stderr.lock().unwrap_or_else(|e| e.into_inner());
        let (a, b) = t.buf.as_slices();
        let mut v = Vec::with_capacity(a.len() + b.len());
        v.extend_from_slice(a);
        v.extend_from_slice(b);
        wasit_process::decode_console_output(&v).trim().to_owned()
    }

    pub(crate) fn exit(&self) -> Option<ExitInfo> {
        *self.exit.borrow()
    }
}

/// Owns the transport child. The child is killed when the last handle is dropped.
pub struct ChildHandle {
    pid: Option<u32>,
    transport: TransportKind,
    view: ChildView,
    waiter: AbortHandle,
}

impl Drop for ChildHandle {
    fn drop(&mut self) {
        // The waiter task owns the `Child` (spawned with kill_on_drop): aborting it kills it.
        self.waiter.abort();
    }
}

impl ChildHandle {
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// The kind of transport this child is (from its program).
    pub fn transport(&self) -> TransportKind {
        self.transport
    }

    /// The last bytes the child wrote to stderr (UTF-8, or UTF-16LE as `wsl.exe` writes it).
    pub fn stderr_tail(&self) -> String {
        self.view.stderr_tail()
    }

    pub fn exit(&self) -> Option<ExitInfo> {
        self.view.exit()
    }

    /// Wait at most `max` for the child to exit (stderr fully captured by then).
    pub async fn wait_exit(&self, max: Duration) -> Option<ExitInfo> {
        let mut rx = self.view.exit.clone();
        let res = match tokio::time::timeout(max, rx.wait_for(Option::is_some)).await {
            Ok(Ok(v)) => *v,
            _ => self.view.exit(),
        };
        res
    }

    /// Wait for the child to exit, however long that takes: for a command whose end is the
    /// answer itself (an ssh that detaches into the background once the user has signed in).
    /// The wait has no bound of its own; whoever cancels the future that awaits it (the link's
    /// connect bound, a stop) ends it, and the child dies with its handle.
    pub async fn wait_exit_unbounded(&self) -> Option<ExitInfo> {
        let mut rx = self.view.exit.clone();
        let res = match rx.wait_for(Option::is_some).await {
            Ok(v) => *v,
            Err(_) => self.view.exit(),
        };
        res
    }

    /// Explains a failed handshake (the hello exchange over this child's stdio) from the child:
    /// the one place that does, used by [`CommandTransport::connect`] and by any connector that
    /// talks to the child before the hello (the product's bootstrap) and then runs the hello
    /// itself.
    ///
    /// A peer that answered (an incompatible protocol, a goodbye) and an error that is not the
    /// handshake's are returned as they are. Otherwise the child's exit status and stderr tail
    /// are what the remote side did: when the stream closed the child is about to be reaped
    /// and is waited for (bounded), after a timeout or garbage it is alive and is not. A
    /// child that has neither ended nor written anything leaves the handshake's own error.
    /// Otherwise the result is [`ConnectError::Command`] with the kind
    /// [`classify_helper_exit`] decides, the child's exit code and a text for the user: the
    /// stderr tail, or the handshake's error with the exit when stderr is empty, or, for
    /// [`CommandFailureKind::HelperFailed`], both.
    pub async fn explain_handshake_failure(&self, e: ConnectError) -> ConnectError {
        let closed = match &e {
            ConnectError::ClosedDuringHandshake | ConnectError::Io(_) => true,
            ConnectError::HandshakeTimeout | ConnectError::Protocol(_) => false,
            _ => return e,
        };
        let exit = if closed {
            self.wait_exit(CLOSED_EXIT_WAIT).await
        } else {
            self.exit()
        };
        let stderr = self.stderr_tail();
        if exit.is_none() && stderr.is_empty() {
            return e;
        }
        let kind = classify_helper_exit(self.transport, exit, &stderr);
        let ended = match exit {
            Some(x) => format!("{e}; {x}"),
            None => e.to_string(),
        };
        let text = match (kind == CommandFailureKind::HelperFailed, stderr.is_empty()) {
            (true, true) => format!("the remote program ended on its own before the handshake completed ({ended})"),
            (true, false) => format!("the remote program ended on its own before the handshake completed ({ended}): {stderr}"),
            (false, true) => exit.map_or_else(|| e.to_string(), |x| format!("{e} ({x})")),
            (false, false) => stderr,
        };
        ConnectError::Command {
            kind,
            exit_code: exit.and_then(|x| x.code),
            stderr: text,
        }
    }
}

#[async_trait]
impl Diagnostics for ChildHandle {
    async fn facts(&self) -> Option<TransportFacts> {
        let exit = self.wait_exit(Duration::from_secs(1)).await;
        let stderr = self.stderr_tail();
        (exit.is_some() || !stderr.is_empty()).then_some(TransportFacts { exit, stderr })
    }
}

/// How long to wait for a transport child whose stdout closed: it must be about to exit, and
/// what it wrote to stderr is complete once it has.
pub const CLOSED_EXIT_WAIT: Duration = Duration::from_secs(5);

/// Spawns transport commands and connects over their stdio.
pub struct CommandTransport;

impl CommandTransport {
    /// Spawn the command; its stdout+stdin form the duplex.
    pub fn spawn(spec: &CommandSpec) -> Result<(BoxDuplex, ChildHandle), ConnectError> {
        let (program, args) = spec
            .argv
            .split_first()
            .ok_or_else(|| ConnectError::Spawn("empty argv".into()))?;
        let mut cmd = wasit_process::command(program);
        cmd.args(args)
            .envs(spec.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .map_err(|e| ConnectError::Spawn(format!("{program}: {e}")))?;
        let pid = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| ConnectError::Spawn("no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ConnectError::Spawn("no stdout".into()))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| ConnectError::Spawn("no stderr".into()))?;

        if let Some(t) = &spec.trace {
            diag::transport_spawned(t, program, spec.argv.len(), pid);
        }
        let tail = Arc::new(Mutex::new(Tail {
            buf: VecDeque::new(),
            limit: spec.stderr_limit.max(1),
            seen: 0,
        }));
        let (exit_tx, exit_rx) = watch::channel(None);
        let tail2 = tail.clone();
        let tail3 = tail.clone();
        let trace = spec.trace.clone();
        let waiter = tokio::spawn(async move {
            let mut stderr_reader = Box::pin(async move {
                let mut chunk = [0u8; 4096];
                loop {
                    match stderr.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let mut t = tail2.lock().unwrap_or_else(|e| e.into_inner());
                            t.seen = t.seen.saturating_add(n as u64);
                            t.buf.extend(&chunk[..n]);
                            let excess = t.buf.len().saturating_sub(t.limit);
                            t.buf.drain(..excess);
                        }
                    }
                }
            });
            let mut stderr_done = false;
            let status = tokio::select! {
                s = child.wait() => s,
                _ = &mut stderr_reader => {
                    stderr_done = true;
                    child.wait().await
                }
            };
            if !stderr_done {
                // stderr may still hold the final words; a grandchild could keep it open, so
                // bound the wait. Timing out only loses diagnostics.
                let _ = tokio::time::timeout(Duration::from_millis(500), stderr_reader).await;
            }
            let info = match status {
                Ok(st) => ExitInfo {
                    code: st.code(),
                    signal: exit_signal(&st),
                },
                Err(_) => ExitInfo {
                    code: None,
                    signal: None,
                },
            };
            if let Some(t) = &trace {
                let seen = tail3.lock().unwrap_or_else(|e| e.into_inner()).seen;
                diag::transport_exited(t, &info, seen);
            }
            let _ = exit_tx.send(Some(info)); // receivers may all be gone
        });
        let view = ChildView {
            exit: exit_rx,
            stderr: tail,
        };
        if let Some(t) = &spec.trace {
            t.attempt.watch(view.clone());
        }
        let handle = ChildHandle {
            pid,
            transport: TransportKind::of_program(program),
            view,
            waiter: waiter.abort_handle(),
        };
        Ok((join_duplex(stdout, stdin), handle))
    }

    /// Spawn and run the hello exchange. On a transport-level failure the child's exit status
    /// and stderr are classified into [`ConnectError::Command`]
    /// ([`ChildHandle::explain_handshake_failure`]).
    pub async fn connect(
        spec: &CommandSpec,
        opts: ClientOptions,
    ) -> Result<ControlClient, ConnectError> {
        let (io, child) = Self::spawn(spec)?;
        let child = Arc::new(child);
        // The program is a bridge (`… bridge`): it reports ready once it reaches the daemon,
        // however long that takes; the hello deadline starts then.
        let opts = ClientOptions {
            await_ready: true,
            ..opts
        };
        match ControlClient::connect(io, opts).await {
            Ok(c) => Ok(c.with_attachment(child.clone()).with_diagnostics(child)),
            Err(e) => Err(child.explain_handshake_failure(e).await),
        }
    }
}

#[cfg(unix)]
fn exit_signal(st: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    st.signal()
}

#[cfg(not(unix))]
fn exit_signal(_st: &std::process::ExitStatus) -> Option<i32> {
    None
}

#[cfg(test)]
mod tests {
    use super::CommandFailureKind as K;
    use super::*;

    #[test]
    fn classifies_well_known_ssh_failures() {
        let cases: &[(Option<i32>, &str, K)] = &[
            (Some(255), "user@h: Permission denied (publickey,password).", K::AuthNeeded),
            (Some(255), "Received disconnect from 1.2.3.4 port 22:2: Too many authentication failures", K::AuthNeeded),
            (Some(255), "Host key verification failed.", K::HostKeyUnknown),
            (Some(255), "No ED25519 host key is known for example.com and you have requested strict checking.\r\nHost key verification failed.", K::HostKeyUnknown),
            (Some(255), "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @\n@@@@\nOffending ECDSA key in /home/u/.ssh/known_hosts:3\nHost key verification failed.", K::HostKeyChanged),
            (Some(255), "ssh: Could not resolve hostname nope: Name or service not known", K::Unreachable),
            (Some(255), "ssh: connect to host 10.0.0.1 port 22: Connection refused", K::Unreachable),
            (Some(255), "ssh: connect to host 10.0.0.1 port 22: Connection timed out", K::Unreachable),
            (Some(255), "ssh: connect to host h port 22: No route to host", K::Unreachable),
            (Some(255), "kex_exchange_identification: read: Connection reset by peer", K::Unreachable),
            (Some(127), "bash: line 1: marsad-agent: command not found", K::CommandNotFound),
            (Some(127), "", K::CommandNotFound),
            (Some(1), "sh: 1: marsad-agent: not found", K::CommandNotFound),
            (Some(1), "'marsad-agent' is not recognized as an internal or external command,\r\noperable program or batch file.", K::CommandNotFound),
            (Some(1), "<3>WSL (123) ERROR: CreateProcessParseCommon:711: execvpe(marsad-agent) failed: No such file or directory", K::CommandNotFound),
            (Some(1), "something else broke", K::Other),
            (Some(1), "There is no distribution with the supplied name.\r\nError code: Wsl/Service/WSL_E_DISTRO_NOT_FOUND", K::UnknownDistro),
            (Some(1), "Windows Subsystem for Linux has no installed distributions.", K::WslUnavailable),
            (Some(126), "sh: 1: /home/u/.marsad/bin/marsad-agent: Permission denied", K::PermissionDenied),
            (Some(255), "u@h: Permission denied (publickey).", K::AuthNeeded),
            (Some(255), "Permission denied, please try again.", K::AuthNeeded),
            (None, "", K::Other),
        ];
        for (code, stderr, want) in cases {
            assert_eq!(
                classify_command_failure(*code, stderr),
                *want,
                "stderr: {stderr}"
            );
        }
        assert!(K::AuthNeeded.needs_human());
        assert!(K::HostKeyChanged.needs_human());
        assert!(!K::Unreachable.needs_human());
        for k in [
            K::UnknownDistro,
            K::WslUnavailable,
            K::PermissionDenied,
            K::UnsupportedPlatform,
            K::HelperMissing,
            K::UnexpectedOutput,
            K::HelperFailed,
        ] {
            assert!(
                k.is_configuration() && !k.is_transient() && !k.needs_human(),
                "{k:?}"
            );
        }
        assert!(K::Unreachable.is_transient() && !K::Unreachable.is_configuration());
    }

    fn exit(code: i32) -> Option<ExitInfo> {
        Some(ExitInfo {
            code: Some(code),
            signal: None,
        })
    }

    fn killed(signal: i32) -> Option<ExitInfo> {
        Some(ExitInfo {
            code: None,
            signal: Some(signal),
        })
    }

    #[test]
    fn the_transport_is_known_from_its_program() {
        for (program, want) in [
            ("ssh", TransportKind::Ssh),
            ("/usr/bin/ssh", TransportKind::Ssh),
            (
                "C:\\Windows\\System32\\OpenSSH\\ssh.exe",
                TransportKind::Ssh,
            ),
            ("SSH.EXE", TransportKind::Ssh),
            ("wsl", TransportKind::Wsl),
            ("C:\\Windows\\System32\\wsl.exe", TransportKind::Wsl),
            ("sshpass", TransportKind::Other),
            ("my-ssh", TransportKind::Other),
            ("wasit-control-demo", TransportKind::Other),
            ("", TransportKind::Other),
        ] {
            assert_eq!(TransportKind::of_program(program), want, "{program}");
        }
    }

    /// The one table of what an ended transport means when the remote program was handed the
    /// connection and the handshake did not complete.
    #[test]
    fn classifies_the_end_of_a_helper_per_transport() {
        use TransportKind::{Other as Any, Ssh, Wsl};
        let cases: &[(TransportKind, Option<ExitInfo>, &str, K)] = &[
            // ssh: a status of 1..=254 is the remote program's own, so retrying cannot help.
            (Ssh, exit(1), "", K::HelperFailed),
            (Ssh, exit(2), "bad argument", K::HelperFailed),
            (Ssh, exit(101), "thread 'main' panicked", K::HelperFailed),
            (Ssh, exit(254), "", K::HelperFailed),
            // ssh's own status, a signal, a clean exit and a child still running: transient.
            (
                Ssh,
                exit(255),
                "client_loop: send disconnect: Broken pipe",
                K::Other,
            ),
            (Ssh, exit(255), "", K::Other),
            (Ssh, killed(9), "", K::Other),
            (Ssh, exit(0), "", K::Other),
            (Ssh, None, "", K::Other),
            // What the words recognise keeps its kind, whatever the status.
            (Ssh, exit(126), "Permission denied", K::PermissionDenied),
            (Ssh, exit(126), "", K::PermissionDenied),
            (Ssh, exit(127), "", K::CommandNotFound),
            (
                Ssh,
                exit(1),
                "sh: 1: marsad-agent: not found",
                K::CommandNotFound,
            ),
            (Ssh, exit(255), "Connection reset by peer", K::Unreachable),
            (
                Ssh,
                exit(255),
                "Permission denied (publickey).",
                K::AuthNeeded,
            ),
            (
                Ssh,
                exit(255),
                "Host key verification failed.",
                K::HostKeyUnknown,
            ),
            (
                Ssh,
                exit(1),
                "no ed25519 host key is known for h",
                K::HostKeyUnknown,
            ),
            // wsl.exe: its own failures and the program's statuses cannot be told apart (not
            // verified), so nothing is claimed; the words still classify.
            (Wsl, exit(1), "bad argument", K::Other),
            (Wsl, exit(2), "", K::Other),
            (
                Wsl,
                exit(1),
                "There is no distribution with the supplied name.",
                K::UnknownDistro,
            ),
            (Wsl, exit(126), "", K::PermissionDenied),
            // Any other command: unknown contract, same.
            (Any, exit(2), "bad argument", K::Other),
            (Any, exit(1), "", K::Other),
            (Any, exit(127), "", K::CommandNotFound),
        ];
        for (transport, exit, stderr, want) in cases {
            assert_eq!(
                classify_helper_exit(*transport, *exit, stderr),
                *want,
                "{transport:?} {exit:?} {stderr:?}"
            );
        }
        // The new kind is a configuration problem: parked, not retried.
        assert!(K::HelperFailed.is_configuration() && !K::HelperFailed.is_transient());
        assert!(!K::HelperFailed.needs_human());
        assert_eq!(K::HelperFailed.code(), "helper_failed");
        assert_eq!(
            serde_json::to_string(&K::HelperFailed).unwrap(),
            "\"helper_failed\""
        );
    }

    #[test]
    fn classifies_utf16_wsl_output_after_decoding() {
        // The owner's report: wsl.exe wrote this as UTF-16LE.
        let raw: Vec<u8> = "There is no distribution with the supplied name.\r\n"
            .encode_utf16()
            .flat_map(|u| u.to_le_bytes())
            .collect();
        let text = wasit_process::decode_console_output(&raw);
        assert_eq!(classify_command_failure(Some(1), &text), K::UnknownDistro);
    }
}
