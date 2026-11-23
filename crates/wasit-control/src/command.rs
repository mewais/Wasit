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
/// diagnostics label and the attempt number the supervisor passed to the connector, so the
/// transport's records join the supervisor's on `(link, n)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportTrace {
    pub link: Ident,
    pub attempt: u64,
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

    /// Records this command's spawn and exit as belonging to attempt `attempt` of `link`.
    pub fn traced(mut self, link: Ident, attempt: u64) -> Self {
        self.trace = Some(TransportTrace { link, attempt });
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

/// Owns the transport child. The child is killed when the last handle is dropped.
pub struct ChildHandle {
    pid: Option<u32>,
    exit: watch::Receiver<Option<ExitInfo>>,
    stderr: Arc<Mutex<Tail>>,
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

    /// The last bytes the child wrote to stderr (UTF-8, or UTF-16LE as `wsl.exe` writes it).
    pub fn stderr_tail(&self) -> String {
        let t = self.stderr.lock().unwrap_or_else(|e| e.into_inner());
        let (a, b) = t.buf.as_slices();
        let mut v = Vec::with_capacity(a.len() + b.len());
        v.extend_from_slice(a);
        v.extend_from_slice(b);
        wasit_process::decode_console_output(&v).trim().to_owned()
    }

    pub fn exit(&self) -> Option<ExitInfo> {
        *self.exit.borrow()
    }

    /// Wait at most `max` for the child to exit (stderr fully captured by then).
    pub async fn wait_exit(&self, max: Duration) -> Option<ExitInfo> {
        let mut rx = self.exit.clone();
        let res = match tokio::time::timeout(max, rx.wait_for(Option::is_some)).await {
            Ok(Ok(v)) => *v,
            _ => *self.exit.borrow(),
        };
        res
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
        let handle = ChildHandle {
            pid,
            exit: exit_rx,
            stderr: tail,
            waiter: waiter.abort_handle(),
        };
        Ok((join_duplex(stdout, stdin), handle))
    }

    /// Spawn and run the hello exchange. On a transport-level failure the child's exit status
    /// and stderr are classified into [`ConnectError::Command`].
    pub async fn connect(
        spec: &CommandSpec,
        opts: ClientOptions,
    ) -> Result<ControlClient, ConnectError> {
        let (io, child) = Self::spawn(spec)?;
        let child = Arc::new(child);
        match ControlClient::connect(io, opts).await {
            Ok(c) => Ok(c.with_attachment(child.clone()).with_diagnostics(child)),
            Err(e @ (ConnectError::Incompatible { .. } | ConnectError::Refused(_))) => Err(e),
            Err(e) => match child.wait_exit(Duration::from_secs(2)).await {
                Some(exit) => {
                    let stderr = child.stderr_tail();
                    Err(ConnectError::Command {
                        kind: classify_command_failure(exit.code, &stderr),
                        exit_code: exit.code,
                        stderr: if stderr.is_empty() {
                            format!("{e} ({exit})")
                        } else {
                            stderr
                        },
                    })
                }
                None => Err(e),
            },
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
        ] {
            assert!(
                k.is_configuration() && !k.is_transient() && !k.needs_human(),
                "{k:?}"
            );
        }
        assert!(K::Unreachable.is_transient() && !K::Unreachable.is_configuration());
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
