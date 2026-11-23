//! Test hygiene: ending the processes a test started.
//!
//! A test of a daemon starts real processes that outlive their starter on purpose: a session's
//! shim (with its guard and the stand-in CLI) is detached so that it survives its daemon, and
//! a daemon started by a bridge or a client is detached from it. A test must therefore stop
//! them itself, also when it fails. [`stop_all_under`] does that for everything whose
//! single-instance lock lies below a directory the test owns (its temporary directory), found
//! through the lock files and stopped through the processes' own endpoints:
//!
//! * a shim (`shim.lock`): `signal kill` over its control endpoint, then its journal until
//!   `exited` is delivered (the shim ends when a controller has received it), then the exit of
//!   the shim process itself. The shim waits for its guard and the guard for the CLI, so their
//!   exit is covered by the shim's;
//! * a daemon (`<endpoint>.sock.lock`): `host.info` names its pid, which gets `SIGTERM` (on
//!   Windows it is killed), then its exit is awaited.
//!
//! Only processes that answer on an endpoint inside the directory are touched, and each is
//! addressed by the pid it reports itself, so no other process can be hit. Daemons are stopped
//! before shims, so nothing is left to start or hold a session while it is stopped.
//!
//! [`PidWatch`] is the strict "is this process gone" for tests (a pidfd on Linux: it cannot be
//! fooled by a recycled pid, and a zombie counts as gone).

use std::path::{Path, PathBuf};
use std::time::Duration;

use wasit_control::{local_endpoint, ClientOptions, ControlClient, Hello};
use wasit_shim::{ShimClient, ShimState, Sig};

/// Upper bound of each step (connect, request, process exit): it only turns a hang into a
/// reported leftover.
const BOUND: Duration = Duration::from_secs(20);

/// Stops every shim and daemon whose lock lies below `root` (see the module docs). Never
/// panics (it runs in `Drop`, possibly while unwinding); what it could not stop is printed to
/// stderr.
pub fn stop_all_under(root: &Path) {
    let mut found = Found::default();
    scan(root, &mut found);
    if found.shims.is_empty() && found.daemons.is_empty() {
        return;
    }
    // A thread and runtime of its own: callable from inside a test's runtime, from a plain
    // `Drop`, and while unwinding.
    let outcome = std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("no runtime to stop test processes: {e}"))?;
            Ok::<_, String>(rt.block_on(async {
                let mut problems = Vec::new();
                for sock in &found.daemons {
                    if let Err(e) = stop_daemon(sock).await {
                        problems.push(format!("daemon {}: {e}", sock.display()));
                    }
                }
                for dir in &found.shims {
                    if let Err(e) = stop_shim(dir).await {
                        problems.push(format!("shim {}: {e}", dir.display()));
                    }
                }
                problems
            }))
        })
        .join()
    });
    match outcome {
        Ok(Ok(problems)) => {
            for p in problems {
                report(&format!("test process left running: {p}"));
            }
        }
        Ok(Err(e)) => report(&e),
        Err(_) => report(&format!(
            "stopping test processes under {} panicked",
            root.display()
        )),
    }
}

/// To the real stderr: `eprintln!` would be captured with the output of a passing test, and
/// a leftover must show whatever the test's outcome.
fn report(line: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{line}");
}

#[derive(Default)]
struct Found {
    /// Runtime directories of shims (they hold `shim.lock`).
    shims: Vec<PathBuf>,
    /// Endpoint paths of daemons (their `<path>.lock` exists).
    daemons: Vec<PathBuf>,
}

/// The lock files below `dir` (symlinks are not followed: only what the test created).
fn scan(dir: &Path, out: &mut Found) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if kind.is_dir() {
            scan(&path, out);
        } else if kind.is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == wasit_shim::LOCK_FILE {
                out.shims.push(dir.to_path_buf());
            } else if let Some(sock) = name.strip_suffix(".sock.lock") {
                out.daemons.push(dir.join(format!("{sock}.sock")));
            }
        }
    }
}

async fn stop_daemon(sock: &Path) -> Result<(), String> {
    let client = match ControlClient::connect_endpoint(
        &local_endpoint(sock),
        ClientOptions::new(Hello::new("test", "cleanup", "cleanup")),
    )
    .await
    {
        Ok(c) => c,
        // Nobody serves it: either the lock file is a leftover of a dead daemon, or the daemon
        // is ending (it frees its endpoint before its lock).
        Err(e) => {
            return match released(wasit_control::lock_path_for(sock)).await {
                true => Ok(()),
                false => Err(format!("alive but not answering: {e}")),
            }
        }
    };
    let info: serde_json::Value = client
        .request("host.info", (), BOUND)
        .await
        .map_err(|e| format!("host.info: {e}"))?;
    let pid = info["pid"]
        .as_u64()
        .ok_or_else(|| format!("host.info without a pid: {info}"))? as u32;
    drop(client);
    terminate(pid).await
}

/// Waits, bounded, until nobody holds the lock `path` (the process that did has ended, or never
/// was: a missing file counts as free). A blocking `flock`, so it returns when the lock is
/// released, not on a schedule.
async fn released(path: PathBuf) -> bool {
    let wait = tokio::task::spawn_blocking(move || {
        match std::fs::File::options().read(true).write(true).open(&path) {
            Ok(f) => f.lock().is_ok(),
            Err(_) => true,
        }
    });
    matches!(tokio::time::timeout(BOUND, wait).await, Ok(Ok(true)))
}

async fn stop_shim(dir: &Path) -> Result<(), String> {
    if !shim_running(dir).await? {
        return Ok(());
    }
    match end_shim(dir).await {
        Ok(()) => Ok(()),
        // It ended on its own between the probe and the connection (it frees its endpoint
        // before its lock).
        Err(e) => match released(dir.join(wasit_shim::LOCK_FILE)).await {
            true => Ok(()),
            false => Err(e),
        },
    }
}

async fn shim_running(dir: &Path) -> Result<bool, String> {
    match wasit_shim::probe(dir).await {
        Ok(state) => Ok(state == ShimState::Running),
        Err(e) => Err(format!("probe: {e}")),
    }
}

async fn end_shim(dir: &Path) -> Result<(), String> {
    let client = ShimClient::connect(dir)
        .await
        .map_err(|e| format!("alive but not answering: {e}"))?;
    let pid = client.shim_pid();
    let watch = PidWatch::open(pid);
    // The CLI may have ended already (the shim then only lingers): the signal then fails and
    // the journal below already holds `exited`.
    let _already_gone = client.signal(Sig::Kill).await;
    let mut records = client.attach(0).await.map_err(|e| format!("attach: {e}"))?;
    // The shim ends once a controller has received `exited`: the stream then closes.
    let drained = tokio::time::timeout(BOUND, async {
        while let Some(r) = records.next().await {
            r.map_err(|e| format!("journal: {e}"))?;
        }
        Ok::<(), String>(())
    })
    .await;
    match drained {
        Ok(r) => r?,
        Err(_) => {
            terminate_hard(pid)?;
            return Err("did not end after its CLI was killed; killed it".into());
        }
    }
    match watch {
        Some(w) if !w.exited(BOUND).await => Err(format!("process {pid} did not exit")),
        _ => Ok(()),
    }
}

/// Asks the daemon `pid` to stop (`SIGTERM`; Windows has no polite signal for a detached
/// process) and waits for it; kills it when it does not end in time.
async fn terminate(pid: u32) -> Result<(), String> {
    let watch = PidWatch::open(pid);
    #[cfg(unix)]
    {
        // SAFETY: plain syscall on a pid the process reported about itself a moment ago.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    }
    #[cfg(windows)]
    terminate_hard(pid)?;
    match &watch {
        Some(w) if !w.exited(BOUND).await => {
            terminate_hard(pid)?;
            if w.exited(BOUND).await {
                Err(format!("process {pid} ignored SIGTERM and was killed"))
            } else {
                Err(format!("process {pid} did not exit"))
            }
        }
        _ => Ok(()),
    }
}

fn terminate_hard(pid: u32) -> Result<(), String> {
    #[cfg(unix)]
    {
        // SAFETY: plain syscall.
        if unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) } != 0 {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::ESRCH) {
                return Err(format!("kill {pid}: {e}"));
            }
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        let st = wasit_process::std_command_inheriting("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("taskkill {pid}: {e}"))?;
        // A process that is gone is fine.
        let _ = st;
        Ok(())
    }
}

/// A handle on one process that tells, without polling, when it has ended: a pidfd on Linux
/// (opened while the process is alive, so a recycled pid can never be mistaken for it; a
/// zombie that is not reaped yet counts as ended). On other platforms there is no such handle:
/// [`PidWatch::open`] gives `None` and callers rely on their other end-of-life signals.
#[cfg(target_os = "linux")]
pub struct PidWatch {
    fd: std::os::fd::OwnedFd,
}

#[cfg(target_os = "linux")]
impl PidWatch {
    /// A handle on `pid`; `None` when it is already gone.
    pub fn open(pid: u32) -> Option<PidWatch> {
        use std::os::fd::{FromRawFd, OwnedFd};
        // SAFETY: plain syscall.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
        if fd < 0 {
            return None;
        }
        // SAFETY: a fresh descriptor that nothing else owns.
        Some(PidWatch {
            fd: unsafe { OwnedFd::from_raw_fd(fd as i32) },
        })
    }

    /// Whether the process has ended.
    pub fn is_gone(&self) -> bool {
        Self::wait(&self.fd, 0)
    }

    /// Waits up to `bound` for the process to end; whether it did. The wait is a `poll(2)` on
    /// the pidfd, on the blocking pool: it returns when the process ends, not on a schedule.
    pub async fn exited(&self, bound: Duration) -> bool {
        let Ok(fd) = self.fd.try_clone() else {
            return self.is_gone();
        };
        let ms = bound.as_millis().min(i32::MAX as u128) as i32;
        tokio::task::spawn_blocking(move || Self::wait(&fd, ms))
            .await
            .unwrap_or(false)
    }

    fn wait(fd: &std::os::fd::OwnedFd, ms: i32) -> bool {
        use std::os::fd::AsRawFd;
        let mut p = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        loop {
            // SAFETY: one valid pollfd.
            let r = unsafe { libc::poll(&mut p, 1, ms) };
            if r >= 0 {
                return r > 0;
            }
            if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
                return false;
            }
        }
    }
}

/// See the Linux version: no process handle on this platform.
#[cfg(not(target_os = "linux"))]
pub struct PidWatch(());

#[cfg(not(target_os = "linux"))]
impl PidWatch {
    pub fn open(_pid: u32) -> Option<PidWatch> {
        None
    }

    pub fn is_gone(&self) -> bool {
        true
    }

    pub async fn exited(&self, _bound: Duration) -> bool {
        true
    }
}
