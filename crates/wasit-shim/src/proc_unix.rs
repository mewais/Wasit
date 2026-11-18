//! Unix process handling.
//!
//! The shim does not spawn the CLI directly. It re-executes itself as a tiny **guard**
//! (`WASIT_SHIM_ROLE=guard`, same argv) that becomes the leader of a new process group and
//! spawns the CLI inside that group with the shim's stdio pipes. The guard holds one end of a
//! socketpair whose other end lives only in the shim: when the shim dies for any reason
//! (including `kill -9`), the guard reads EOF and `SIGKILL`s the whole group. The guard mirrors
//! the CLI's exit status (code, or re-raised signal) so the shim learns it from `wait()`.
//! On Linux the CLI additionally gets `PR_SET_PDEATHSIG(SIGKILL)` tied to the guard.

use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;

use crate::cli::Config;
use crate::record::ExitInfo;
use crate::wire::Sig;
use crate::Error;

pub(crate) const ROLE_ENV: &str = "WASIT_SHIM_ROLE";
const GUARD_FD: i32 = 3;
const GUARD_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct Spawned {
    pub pid: u32,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
    pub exit: JoinHandle<io::Result<ExitInfo>>,
    pub signaler: Signaler,
}

pub(crate) struct Signaler {
    pgid: libc::pid_t,
}

fn signo(sig: Sig) -> libc::c_int {
    match sig {
        Sig::Int => libc::SIGINT,
        Sig::Term => libc::SIGTERM,
        Sig::Kill => libc::SIGKILL,
    }
}

impl Signaler {
    pub(crate) fn signal(&self, sig: Sig) -> io::Result<()> {
        // SAFETY: killpg has no memory-safety preconditions.
        if unsafe { libc::killpg(self.pgid, signo(sig)) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// SIGKILL whatever is left of the group. Only called while the group is known to still have
    /// members (e.g. stdio drain timed out), so the pgid cannot have been recycled.
    pub(crate) fn kill_all(&self) -> io::Result<()> {
        match self.signal(Sig::Kill) {
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
            r => r,
        }
    }
}

pub(crate) fn is_guard() -> bool {
    std::env::var_os(ROLE_ENV).is_some_and(|v| v == "guard")
}

fn self_exe() -> io::Result<std::path::PathBuf> {
    if cfg!(target_os = "linux") {
        // Works even if the binary was replaced on disk since we started.
        Ok("/proc/self/exe".into())
    } else {
        std::env::current_exe()
    }
}

pub(crate) async fn spawn(_cfg: &Config) -> Result<Spawned, Error> {
    let (shim_end, guard_end) = std::os::unix::net::UnixStream::pair()?;
    let guard_raw = guard_end.as_raw_fd();
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let mut cmd = wasit_process::command(self_exe()?);
    cmd.args(&args)
        .env(ROLE_ENV, "guard")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(false);
    // SAFETY: only async-signal-safe calls (dup2/fcntl) between fork and exec.
    unsafe {
        cmd.pre_exec(move || {
            if guard_raw == GUARD_FD {
                let fl = libc::fcntl(GUARD_FD, libc::F_GETFD);
                if fl < 0 || libc::fcntl(GUARD_FD, libc::F_SETFD, fl & !libc::FD_CLOEXEC) < 0 {
                    return Err(io::Error::last_os_error());
                }
            } else if libc::dup2(guard_raw, GUARD_FD) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Spawn(format!("cannot start guard process: {e}")))?;
    drop(guard_end);
    let guard_pid = child
        .id()
        .ok_or_else(|| Error::Spawn("guard exited immediately".into()))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Spawn("no stdin pipe".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Spawn("no stdout pipe".into()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::Spawn("no stderr pipe".into()))?;

    shim_end.set_nonblocking(true)?;
    let ctl = tokio::net::UnixStream::from_std(shim_end)?;
    let mut ctl = BufReader::new(ctl);
    let mut line = String::new();
    let n = tokio::time::timeout(GUARD_HANDSHAKE_TIMEOUT, ctl.read_line(&mut line))
        .await
        .map_err(|_| Error::Timeout("waiting for the guard to report the child pid"))??;
    let line = line.trim_end();
    let pid = if let Some(p) = line.strip_prefix("pid ") {
        p.parse::<u32>()
            .map_err(|_| Error::Spawn(format!("guard sent a bad pid line: {line:?}")))?
    } else if let Some(msg) = line.strip_prefix("err ") {
        // Reap the guard; its status carries no extra information.
        let _reaped = tokio::time::timeout(GUARD_HANDSHAKE_TIMEOUT, child.wait()).await;
        return Err(Error::Spawn(msg.to_string()));
    } else if n == 0 {
        let status = tokio::time::timeout(GUARD_HANDSHAKE_TIMEOUT, child.wait()).await;
        return Err(Error::Spawn(format!(
            "guard exited before reporting the child: {status:?}"
        )));
    } else {
        return Err(Error::Spawn(format!("guard sent {line:?}")));
    };

    let exit = tokio::spawn(async move {
        // Keep our end of the socketpair open until the guard is gone: closing it tells the
        // guard to kill the group.
        let _keep = ctl;
        let status = child.wait().await?;
        Ok(ExitInfo::from_status(status))
    });
    Ok(Spawned {
        pid,
        stdin,
        stdout,
        stderr,
        exit,
        signaler: Signaler {
            pgid: guard_pid as libc::pid_t,
        },
    })
}

/// Entry point of the guard role. Synchronous and single-purpose: it is its own process.
pub(crate) fn run_guard(cfg: &Config) -> i32 {
    // SAFETY: fd 3 was set up by the shim for exactly this purpose and nothing else owns it.
    let mut ctl = unsafe { std::fs::File::from_raw_fd(GUARD_FD) };
    // SAFETY: plain fcntl/signal calls on our own process.
    unsafe {
        libc::fcntl(GUARD_FD, libc::F_SETFD, libc::FD_CLOEXEC);
        for s in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT] {
            libc::signal(s, libc::SIG_IGN);
        }
    }
    let guard_pid = std::process::id() as libc::pid_t;
    let mut cmd = wasit_process::std_command(&cfg.program);
    cmd.args(&cfg.args).env_remove(ROLE_ENV);
    for (k, v) in &cfg.env {
        cmd.env(k, v);
    }
    if let Some(cwd) = &cfg.cwd {
        cmd.current_dir(cwd);
        // Some CLIs (OpenCode, Kilo) take their project from `$PWD`, which would otherwise
        // still name the directory this process was started in.
        cmd.env("PWD", cwd);
    }
    // SAFETY: only async-signal-safe calls between fork and exec.
    unsafe {
        use std::os::unix::process::CommandExt;
        cmd.pre_exec(move || {
            for s in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT] {
                libc::signal(s, libc::SIG_DFL);
            }
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != guard_pid {
                    // The guard died before the death signal was armed.
                    return Err(io::Error::other("guard died during spawn"));
                }
            }
            #[cfg(not(target_os = "linux"))]
            let _ = guard_pid; // only used on Linux
            Ok(())
        });
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            // If the shim is gone there is nobody to tell; exit code says it all.
            let _ignored = writeln!(ctl, "err cannot start {:?}: {e}", cfg.program);
            return 127;
        }
    };
    if writeln!(ctl, "pid {}", child.id()).is_err() {
        // Shim already gone: take the group down with us.
        // SAFETY: killpg on our own group.
        unsafe { libc::killpg(guard_pid, libc::SIGKILL) };
    }
    // Release our copies of the stdio pipes so EOF depends only on the child.
    if let Ok(null) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
    {
        for fd in 0..3 {
            // SAFETY: dup2 onto our own standard descriptors.
            unsafe { libc::dup2(null.as_raw_fd(), fd) };
        }
    }
    let watcher = ctl.try_clone();
    match watcher {
        Ok(mut w) => {
            std::thread::spawn(move || {
                let mut buf = [0u8; 64];
                loop {
                    match w.read(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(_) => break,
                    }
                }
                // The shim is gone: nobody owns this session any more.
                // SAFETY: killpg on our own group.
                unsafe { libc::killpg(guard_pid, libc::SIGKILL) };
            });
        }
        Err(_) => {
            // Cannot watch the shim; refuse to run an unowned child.
            // SAFETY: killpg on our own group.
            unsafe { libc::killpg(guard_pid, libc::SIGKILL) };
        }
    }
    let status = match child.wait() {
        Ok(s) => s,
        Err(_) => return 125,
    };
    let info = ExitInfo::from_status(status);
    if let Some(code) = info.code {
        return code;
    }
    if let Some(sig) = info.signal {
        // SAFETY: re-raise the child's fatal signal on ourselves with default disposition and
        // no core dump, so the shim's wait() sees the same signal.
        unsafe {
            let rl = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            libc::setrlimit(libc::RLIMIT_CORE, &rl);
            libc::signal(sig, libc::SIG_DFL);
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, sig);
            libc::sigprocmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
            libc::raise(sig);
        }
        return 128 + sig;
    }
    125
}

/// Configure a command so the started shim is fully detached: new session, reparented to init
/// via an intermediate fork (no zombie in the spawner, no controlling terminal).
pub(crate) fn configure_detached(cmd: &mut tokio::process::Command) {
    // SAFETY: setsid/fork/_exit are async-signal-safe.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            match libc::fork() {
                -1 => Err(io::Error::last_os_error()),
                0 => Ok(()),
                _ => libc::_exit(0),
            }
        });
    }
}

/// After the readiness line was written: point stdout at /dev/null so the spawner's pipe is
/// released.
pub(crate) fn detach_stdout() -> io::Result<()> {
    let null = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
    // SAFETY: dup2 onto our own stdout.
    if unsafe { libc::dup2(null.as_raw_fd(), 1) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
