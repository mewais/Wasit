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
        let p = PathBuf::from(format!("/tmp/wst-{}-{n}", std::process::id()));
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

pub fn sh(script: &str) -> Vec<String> {
    vec!["-c".to_string(), script.to_string()]
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
