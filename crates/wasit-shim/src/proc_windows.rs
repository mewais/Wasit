//! Windows process handling: the CLI is spawned directly in a new process group and assigned to
//! a job object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, so the whole tree dies with the
//! shim. Signals: `kill`/`term` terminate the job; `int` sends CTRL_BREAK to the child's group
//! (only effective when the child shares a console).

use std::io;
use std::process::Stdio;

use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::task::JoinHandle;
use windows_sys::Win32::Foundation::{
    CloseHandle, SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT,
};
use windows_sys::Win32::System::Console::{
    GenerateConsoleCtrlEvent, GetStdHandle, SetStdHandle, CTRL_BREAK_EVENT, STD_ERROR_HANDLE,
    STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Threading::{
    CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, DETACHED_PROCESS,
};

use crate::cli::Config;
use crate::record::ExitInfo;
use crate::wire::Sig;
use crate::Error;

pub(crate) struct Spawned {
    pub pid: u32,
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
    pub exit: JoinHandle<io::Result<ExitInfo>>,
    pub signaler: Signaler,
}

struct Job(HANDLE);
// SAFETY: a job handle is a kernel object handle usable from any thread.
unsafe impl Send for Job {}
// SAFETY: see above; the job APIs we call are thread-safe.
unsafe impl Sync for Job {}

impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: we own the handle.
        unsafe { CloseHandle(self.0) };
    }
}

pub(crate) struct Signaler {
    job: Job,
    pid: u32,
}

impl Signaler {
    pub(crate) fn signal(&self, sig: Sig) -> io::Result<()> {
        // SAFETY: valid job handle owned by self.
        let ok = unsafe {
            match sig {
                Sig::Int => GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, self.pid),
                Sig::Term | Sig::Kill => TerminateJobObject(self.job.0, 1),
            }
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn kill_all(&self) -> io::Result<()> {
        self.signal(Sig::Kill)
    }
}

pub(crate) fn is_guard() -> bool {
    false
}

pub(crate) fn run_guard(_cfg: &Config) -> i32 {
    2
}

fn new_job() -> io::Result<Job> {
    // SAFETY: plain Win32 calls with valid arguments; the struct is zero-initialisable.
    unsafe {
        let h = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if h.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Job(h);
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if SetInformationJobObject(
            job.0,
            JobObjectExtendedLimitInformation,
            &info as *const _ as *const core::ffi::c_void,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        ) == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }
}

/// Mark the shim's own standard handles non-inheritable. `CreateProcess` inherits every
/// inheritable handle, so without this the CLI child (and its descendants) would keep the
/// launcher's readiness pipe open and the launcher would never see EOF while the CLI runs.
fn uninherit_std_handles() {
    for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
        // SAFETY: plain Win32 calls on our own standard handles; failures (no such handle) are
        // harmless and ignored.
        unsafe {
            let h = GetStdHandle(which);
            if !h.is_null() && h as isize != -1 {
                SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0);
            }
        }
    }
}

pub(crate) async fn spawn(cfg: &Config) -> Result<Spawned, Error> {
    uninherit_std_handles();
    let job = new_job()?;
    let mut cmd = tokio::process::Command::new(&cfg.program);
    cmd.args(&cfg.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW)
        .kill_on_drop(false);
    for (k, v) in &cfg.env {
        cmd.env(k, v);
    }
    if let Some(cwd) = &cfg.cwd {
        cmd.current_dir(cwd);
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Spawn(format!("cannot start {:?}: {e}", cfg.program)))?;
    let pid = child
        .id()
        .ok_or_else(|| Error::Spawn("child exited immediately".into()))?;
    // Known gap: the child runs briefly before it is assigned to the job; a grandchild started
    // in that window escapes the job.
    if let Some(h) = child.raw_handle() {
        // SAFETY: both handles are valid for the duration of the call.
        if unsafe { AssignProcessToJobObject(job.0, h as HANDLE) } == 0 {
            let e = io::Error::last_os_error();
            // Best effort: the child must not outlive a failed setup.
            let _killed = child.start_kill();
            return Err(Error::Spawn(format!("cannot assign child to job: {e}")));
        }
    }
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
    let exit = tokio::spawn(async move {
        let status = child.wait().await?;
        Ok(ExitInfo::from_status(status))
    });
    Ok(Spawned {
        pid,
        stdin,
        stdout,
        stderr,
        exit,
        signaler: Signaler { job, pid },
    })
}

pub(crate) const DETACHED_FLAGS: u32 =
    DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB;
pub(crate) const DETACHED_FLAGS_NO_BREAKAWAY: u32 = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP;

/// After the readiness line was written: close the inherited stdout pipe.
pub(crate) fn detach_stdout() -> io::Result<()> {
    // SAFETY: we own our standard handles; replacing then closing the old one is sound.
    unsafe {
        let h = GetStdHandle(STD_OUTPUT_HANDLE);
        if SetStdHandle(STD_OUTPUT_HANDLE, std::ptr::null_mut()) == 0 {
            return Err(io::Error::last_os_error());
        }
        if !h.is_null() && h as isize != -1 {
            CloseHandle(h);
        }
    }
    Ok(())
}
