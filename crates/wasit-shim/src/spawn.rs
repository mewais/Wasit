use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::cli;
use crate::diag::STDERR_MAX_BYTES;
use crate::{Error, LOG_FILE, STDERR_FILE};

/// Tunables passed to the shim on its command line.
#[derive(Debug, Clone)]
pub struct ShimOptions {
    /// After `exited` is durable, keep serving until a controller has received it or no record
    /// was delivered to any controller for this long.
    pub linger: Duration,
    /// Disconnect a controller whose socket stays unwritable this long.
    pub controller_write_timeout: Duration,
    /// Cap on line payload bytes per record; longer lines are split (`partial: true`).
    pub max_record_bytes: usize,
    /// After the child exited, wait at most this long for stdout/stderr EOF.
    pub drain_timeout: Duration,
    /// [`spawn_detached`] fails if the shim has not signalled readiness within this time.
    pub ready_timeout: Duration,
}

impl Default for ShimOptions {
    fn default() -> Self {
        ShimOptions {
            linger: cli::DEFAULT_LINGER,
            controller_write_timeout: cli::DEFAULT_WRITE_TIMEOUT,
            max_record_bytes: cli::DEFAULT_MAX_RECORD,
            drain_timeout: cli::DEFAULT_DRAIN_TIMEOUT,
            ready_timeout: Duration::from_secs(10),
        }
    }
}

/// Everything needed to launch one shim.
#[derive(Debug, Clone)]
pub struct ShimSpec {
    /// Executable whose entry point calls [`crate::run`] (e.g. `wasit-shim`, or a host binary).
    pub shim_program: PathBuf,
    /// Arguments placed before the shim arguments (e.g. `["shim"]` for a host subcommand).
    pub shim_args: Vec<OsString>,
    /// Session directory (journal, log; also lock and endpoint unless `run_dir` is set).
    pub dir: PathBuf,
    /// Machine-local directory for `shim.lock` and the control endpoint. Set it when `dir` may
    /// be on a filesystem shared between machines (Unix sockets and advisory locks are
    /// per-kernel / unreliable there).
    pub run_dir: Option<PathBuf>,
    pub program: OsString,
    pub args: Vec<OsString>,
    /// Working directory of the child (default: inherited).
    pub cwd: Option<PathBuf>,
    /// Extra environment for the child.
    pub env: Vec<(OsString, OsString)>,
    pub options: ShimOptions,
}

impl ShimSpec {
    pub fn new(
        shim_program: impl Into<PathBuf>,
        dir: impl Into<PathBuf>,
        program: impl Into<OsString>,
    ) -> Self {
        ShimSpec {
            shim_program: shim_program.into(),
            shim_args: Vec::new(),
            dir: dir.into(),
            run_dir: None,
            program: program.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
            options: ShimOptions::default(),
        }
    }

    /// Directory of the lock and the control endpoint.
    pub fn endpoint_dir(&self) -> &std::path::Path {
        self.run_dir.as_deref().unwrap_or(&self.dir)
    }

    pub fn arg(mut self, a: impl Into<OsString>) -> Self {
        self.args.push(a.into());
        self
    }

    pub fn args<I: IntoIterator<Item = A>, A: Into<OsString>>(mut self, it: I) -> Self {
        self.args.extend(it.into_iter().map(Into::into));
        self
    }

    /// Build a spec from the shim command line (`--dir ... -- program args`), e.g. for a
    /// `launch` front-end that starts a detached shim of the same binary.
    pub fn from_cli_args<I: IntoIterator<Item = OsString>>(
        shim_program: impl Into<PathBuf>,
        args: I,
    ) -> Result<Self, Error> {
        let c = cli::parse(args)?;
        Ok(ShimSpec {
            shim_program: shim_program.into(),
            shim_args: Vec::new(),
            dir: c.dir,
            run_dir: c.run,
            program: c.program,
            args: c.args,
            cwd: c.cwd,
            env: c.env,
            options: ShimOptions {
                linger: c.linger,
                controller_write_timeout: c.write_timeout,
                max_record_bytes: c.max_record,
                drain_timeout: c.drain_timeout,
                ..ShimOptions::default()
            },
        })
    }

    /// The shim command line (without `shim_program` / `shim_args`).
    pub fn to_args(&self) -> Vec<OsString> {
        let mut v: Vec<OsString> = vec!["--dir".into(), self.dir.clone().into()];
        if let Some(r) = &self.run_dir {
            v.push("--run".into());
            v.push(r.clone().into());
        }
        if let Some(c) = &self.cwd {
            v.push("--cwd".into());
            v.push(c.clone().into());
        }
        for (k, val) in &self.env {
            let mut kv = k.clone();
            kv.push("=");
            kv.push(val);
            v.push("--env".into());
            v.push(kv);
        }
        let o = &self.options;
        for (flag, n) in [
            ("--linger-ms", o.linger.as_millis() as u64),
            (
                "--write-timeout-ms",
                o.controller_write_timeout.as_millis() as u64,
            ),
            ("--max-record-bytes", o.max_record_bytes as u64),
            ("--drain-timeout-ms", o.drain_timeout.as_millis() as u64),
        ] {
            v.push(flag.into());
            v.push(n.to_string().into());
        }
        v.push("--".into());
        v.push(self.program.clone());
        v.extend(self.args.iter().cloned());
        v
    }
}

/// A running, detached shim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShimHandle {
    pub dir: PathBuf,
    pub shim_pid: u32,
    pub child_pid: u32,
    /// Seq of the `spawned` record of this run.
    pub spawned_seq: u64,
}

#[derive(Deserialize)]
struct Ready {
    ready: bool,
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    child_pid: Option<u32>,
    #[serde(default)]
    seq: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

/// Start a fully detached shim and return once it has signalled readiness (journal open,
/// endpoint bound, child spawned and `spawned` journaled) over its inherited stdout pipe.
pub async fn spawn_detached(spec: &ShimSpec) -> Result<ShimHandle, Error> {
    if spec.env.iter().any(|(k, _)| k.is_empty()) {
        return Err(Error::InvalidArgument(
            "empty environment variable name".into(),
        ));
    }
    let dir = spec.dir.clone();
    tokio::task::spawn_blocking(move || wasit_diag::private::create_dir_all(&dir)).await??;
    // The shim writes `shim.log` itself (its diagnostics sink owns that file); its stderr is
    // the separate crash file.
    let stderr = open_stderr(&spec.dir).await?;
    let mut cmd = wasit_process::command(&spec.shim_program);
    cmd.args(&spec.shim_args)
        .args(spec.to_args())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr))
        .kill_on_drop(false);
    let mut child = launch(cmd).map_err(|e| {
        Error::Spawn(format!(
            "cannot execute {}: {e}",
            spec.shim_program.display()
        ))
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Spawn("no readiness pipe".into()))?;
    let mut line = String::new();
    let read = tokio::time::timeout(
        spec.options.ready_timeout,
        BufReader::new(stdout).read_line(&mut line),
    )
    .await;
    #[cfg(unix)]
    {
        // Reap the intermediate fork (it exits right after forking the shim).
        if tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .is_err()
        {
            return Err(Error::Timeout("reaping the intermediate launcher process"));
        }
    }
    #[cfg(windows)]
    drop(child); // the shim itself; detached, never waited for here
    match read {
        Err(_) => return Err(Error::Timeout("waiting for the shim to become ready")),
        Ok(Err(e)) => return Err(e.into()),
        Ok(Ok(0)) => {
            return Err(Error::Spawn(format!(
                "shim exited without signalling readiness; see {} and {} in {}",
                LOG_FILE,
                STDERR_FILE,
                spec.dir.display()
            )))
        }
        Ok(Ok(_)) => {}
    }
    let r: Ready = serde_json::from_str(line.trim_end())
        .map_err(|e| Error::Spawn(format!("bad readiness line {line:?}: {e}")))?;
    if !r.ready {
        return Err(Error::Spawn(
            r.error.unwrap_or_else(|| "shim reported not ready".into()),
        ));
    }
    match (r.pid, r.child_pid, r.seq) {
        (Some(shim_pid), Some(child_pid), Some(spawned_seq)) => Ok(ShimHandle {
            dir: spec.dir.clone(),
            shim_pid,
            child_pid,
            spawned_seq,
        }),
        _ => Err(Error::Spawn(format!("incomplete readiness line {line:?}"))),
    }
}

/// Opens the shim's stderr file for appending (private to its owner), cut to nothing first when
/// an earlier run left more than [`STDERR_MAX_BYTES`]: raw crash output is a few bytes, so
/// the file stays small without a rotation of its own.
async fn open_stderr(dir: &std::path::Path) -> std::io::Result<std::fs::File> {
    let path = dir.join(STDERR_FILE);
    let tighten = path.clone();
    tokio::task::spawn_blocking(move || wasit_diag::private::tighten_file(&tighten))
        .await
        .map_err(std::io::Error::other)??;
    let mut options = tokio::fs::OpenOptions::from(wasit_diag::private::file_options());
    options.append(true);
    let file = options.open(&path).await?;
    if file.metadata().await?.len() > STDERR_MAX_BYTES {
        file.set_len(0).await?;
    }
    Ok(file.into_std().await)
}

#[cfg(unix)]
fn launch(mut cmd: tokio::process::Command) -> std::io::Result<tokio::process::Child> {
    crate::platform::configure_detached(&mut cmd);
    cmd.spawn()
}

#[cfg(windows)]
fn launch(mut cmd: tokio::process::Command) -> std::io::Result<tokio::process::Child> {
    use crate::platform::{DETACHED_FLAGS, DETACHED_FLAGS_NO_BREAKAWAY};
    const ERROR_ACCESS_DENIED: i32 = 5;
    // CreateProcess inherits every inheritable handle: keep our own stdio (possibly a pipe a
    // caller is waiting on for EOF) out of the detached shim.
    crate::platform::uninherit_std_handles();
    cmd.creation_flags(DETACHED_FLAGS);
    match cmd.spawn() {
        // The spawner's job does not allow breakaway: stay in it.
        Err(e) if e.raw_os_error() == Some(ERROR_ACCESS_DENIED) => {
            cmd.creation_flags(DETACHED_FLAGS_NO_BREAKAWAY);
            cmd.spawn()
        }
        r => r,
    }
}
