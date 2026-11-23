#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use wasit_shim::{Kind, Record, RecordStream, ShimHandle, ShimSpec};

pub const T: Duration = Duration::from_secs(30);

/// Short temp dir (Unix socket paths are limited to ~104 bytes).
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        #[cfg(unix)]
        let root = PathBuf::from("/tmp");
        #[cfg(not(unix))]
        let root = std::env::temp_dir();
        let p = root.join(format!("wst-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn shim_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_wasit-shim"))
}

pub fn spec(dir: &Path, program: &str, args: &[&str]) -> ShimSpec {
    ShimSpec::new(shim_bin(), dir, program).args(args.iter().copied())
}

pub async fn start(dir: &Path, program: &str, args: &[&str]) -> ShimHandle {
    start_spec(spec(dir, program, args)).await
}

pub async fn start_spec(spec: ShimSpec) -> ShimHandle {
    tokio::time::timeout(T, wasit_shim::spawn_detached(&spec))
        .await
        .expect("spawn timed out")
        .expect("spawn failed")
}

/// The portable stand-in child (see its docs for the modes).
pub fn child_bin() -> String {
    env!("CARGO_BIN_EXE_wasit-shim-testchild").to_string()
}

/// Start the shim around the test child in `mode` with `args`.
pub async fn start_child(dir: &Path, mode: &str, args: &[&str]) -> ShimHandle {
    start_spec(child_spec(dir, mode, args)).await
}

pub fn child_spec(dir: &Path, mode: &str, args: &[&str]) -> ShimSpec {
    let mut a = vec![mode];
    a.extend_from_slice(args);
    spec(dir, &child_bin(), &a)
}

/// Kill a process outright (SIGKILL / TerminateProcess).
pub fn kill_hard(pid: u32) {
    #[cfg(unix)]
    {
        // SAFETY: plain syscall.
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) }, 0);
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, TerminateProcess, PROCESS_TERMINATE,
        };
        // SAFETY: plain Win32 calls; the handle is closed.
        unsafe {
            let h = OpenProcess(PROCESS_TERMINATE, 0, pid);
            assert!(
                !h.is_null(),
                "OpenProcess: {}",
                std::io::Error::last_os_error()
            );
            assert!(TerminateProcess(h, 1) != 0);
            CloseHandle(h);
        }
    }
}

/// A raw (unframed) connection to the shim endpoint: Unix socket or named pipe.
pub type RawRead = Box<dyn tokio::io::AsyncRead + Send + Unpin>;
pub type RawWrite = Box<dyn tokio::io::AsyncWrite + Send + Unpin>;

pub async fn raw_connect(dir: &Path) -> (RawRead, RawWrite) {
    let name = wasit_shim::endpoint_name(dir);
    #[cfg(unix)]
    {
        let s = tokio::net::UnixStream::connect(name).await.unwrap();
        let (r, w) = s.into_split();
        (Box::new(r), Box::new(w))
    }
    #[cfg(windows)]
    {
        let c = loop {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(&name) {
                Ok(c) => break c,
                Err(e) if e.raw_os_error() == Some(231) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(e) => panic!("open pipe: {e}"),
            }
        };
        let (r, w) = tokio::io::split(c);
        (Box::new(r), Box::new(w))
    }
}

/// A scripted peer endpoint standing in for a shim: accepts one connection.
pub struct RawListener {
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
    #[cfg(windows)]
    inner: tokio::net::windows::named_pipe::NamedPipeServer,
}

impl RawListener {
    pub fn bind(dir: &Path) -> Self {
        let name = wasit_shim::endpoint_name(dir);
        #[cfg(unix)]
        {
            RawListener {
                inner: tokio::net::UnixListener::bind(name).unwrap(),
            }
        }
        #[cfg(windows)]
        {
            RawListener {
                inner: tokio::net::windows::named_pipe::ServerOptions::new()
                    .first_pipe_instance(true)
                    .create(&name)
                    .unwrap(),
            }
        }
    }

    pub async fn accept(self) -> (RawRead, RawWrite) {
        #[cfg(unix)]
        {
            let (s, _) = self.inner.accept().await.unwrap();
            let (r, w) = s.into_split();
            (Box::new(r), Box::new(w))
        }
        #[cfg(windows)]
        {
            self.inner.connect().await.unwrap();
            let (r, w) = tokio::io::split(self.inner);
            (Box::new(r), Box::new(w))
        }
    }
}

pub async fn next(s: &mut RecordStream) -> Record {
    tokio::time::timeout(T, s.next())
        .await
        .expect("timed out waiting for a record")
        .expect("stream ended")
        .expect("stream error")
}

/// Collect records until (and including) the first one matching `pred`.
pub async fn until(s: &mut RecordStream, mut pred: impl FnMut(&Record) -> bool) -> Vec<Record> {
    let mut v = Vec::new();
    loop {
        let r = next(s).await;
        let done = pred(&r);
        v.push(r);
        if done {
            return v;
        }
    }
}

pub async fn until_exited(s: &mut RecordStream) -> Vec<Record> {
    until(s, |r| r.kind == Kind::Exited).await
}

/// Wait for the stream to end (the shim closed the connection).
pub async fn until_end(s: &mut RecordStream) {
    loop {
        match tokio::time::timeout(T, s.next())
            .await
            .expect("stream did not end")
        {
            None => return,
            Some(Ok(_)) => continue,
            Some(Err(e)) => panic!("stream error: {e}"),
        }
    }
}

pub fn assert_contiguous(recs: &[Record], from: u64) {
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(
            r.seq,
            from + i as u64,
            "gap or duplicate at index {i}: {r:?}"
        );
    }
}

pub fn texts(recs: &[Record], kind: Kind) -> Vec<String> {
    recs.iter()
        .filter(|r| r.kind == kind)
        .map(|r| r.text().unwrap().to_string())
        .collect()
}

/// Await the exit of an arbitrary process without polling (pidfd on Linux).
#[cfg(target_os = "linux")]
pub async fn await_pid_exit(pid: u32, timeout: Duration) -> bool {
    use std::os::fd::{FromRawFd, OwnedFd};
    use tokio::io::unix::AsyncFd;
    // SAFETY: plain syscall.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) };
    if fd < 0 {
        let e = std::io::Error::last_os_error();
        assert_eq!(e.raw_os_error(), Some(libc::ESRCH), "pidfd_open: {e}");
        return true;
    }
    // SAFETY: fresh fd we own.
    let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
    let afd = AsyncFd::new(fd).unwrap();
    tokio::time::timeout(timeout, afd.readable()).await.is_ok()
}

/// Fallback for other Unixes: backoff on kill(pid, 0).
#[cfg(all(unix, not(target_os = "linux")))]
pub async fn await_pid_exit(pid: u32, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut backoff = Duration::from_millis(5);
    loop {
        // SAFETY: plain syscall.
        if unsafe { libc::kill(pid as libc::pid_t, 0) } != 0 {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(100));
    }
}

/// Windows: wait for the process to be gone (or not exist).
#[cfg(windows)]
pub async fn await_pid_exit(pid: u32, timeout: Duration) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
    };
    // SAFETY: plain Win32 calls; the handle is closed.
    let h = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if h.is_null() {
        return true; // no such process
    }
    let h = h as usize;
    let r = tokio::task::spawn_blocking(move || {
        // SAFETY: handle is valid until closed below.
        let w = unsafe { WaitForSingleObject(h as _, timeout.as_millis() as u32) };
        unsafe { CloseHandle(h as _) };
        w == WAIT_OBJECT_0
    })
    .await
    .unwrap();
    r
}

/// Every line of `<dir>/shim.log` as JSON (panics on a line that is not one): the file header
/// first, then the diagnostic records.
pub fn shim_log(dir: &Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(dir.join(wasit_shim::LOG_FILE)).unwrap_or_default();
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect()
}

/// The records of `ev` in a parsed log.
pub fn events<'a>(log: &'a [serde_json::Value], ev: &str) -> Vec<&'a serde_json::Value> {
    log.iter().filter(|r| r["ev"] == ev).collect()
}

/// The shim's stderr file, empty when missing.
pub fn shim_stderr(dir: &Path) -> String {
    std::fs::read_to_string(dir.join(wasit_shim::STDERR_FILE)).unwrap_or_default()
}
