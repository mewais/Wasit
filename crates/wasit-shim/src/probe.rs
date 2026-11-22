use std::fs::{File, TryLockError};
use std::io;
use std::path::Path;

use crate::record::{ExitInfo, Kind};
use crate::{Error, JOURNAL_FILE, LOCK_FILE};

/// Liveness of the shim of a session directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShimState {
    /// Some process holds `shim.lock`.
    Running,
    /// No lock holder and the journal ends with an `exited` record.
    Exited(ExitInfo),
    /// No lock holder and no `exited` record: the shim died (or never started).
    Dead,
}

/// Probe a session directory without blocking: a non-blocking `try_lock` on `shim.lock`, then
/// (if free) a bounded read of the journal's last record.
pub async fn probe(dir: &Path) -> Result<ShimState, Error> {
    probe_at(dir, dir).await
}

/// [`probe`] for a session whose lock and endpoint live in a separate runtime directory `run`
/// (journal in `dir`).
pub async fn probe_at(dir: &Path, run: &Path) -> Result<ShimState, Error> {
    let dir = dir.to_path_buf();
    let run = run.to_path_buf();
    tokio::task::spawn_blocking(move || probe_sync(&dir, &run)).await?
}

fn probe_sync(dir: &Path, run: &Path) -> Result<ShimState, Error> {
    match File::options()
        .read(true)
        .write(true)
        .open(run.join(LOCK_FILE))
    {
        Ok(f) => match f.try_lock() {
            Ok(()) => {
                // We hold it only for the duration of this check; dropping `f` releases it.
                drop(f);
            }
            Err(TryLockError::WouldBlock) => return Ok(ShimState::Running),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    match crate::journal::last_record(&dir.join(JOURNAL_FILE))? {
        Some(r) if r.kind == Kind::Exited => {
            Ok(ShimState::Exited(r.exit_info().unwrap_or_default()))
        }
        _ => Ok(ShimState::Dead),
    }
}

/// Create the runtime directory of a session (owner-only on Unix) and, on Unix, take a shared
/// lock on it for the shim's lifetime (directory cleaners skip locked directories). The lock is
/// best effort: a filesystem without `flock` on directories just gets no guard.
pub(crate) async fn prepare_run_dir(run: &Path) -> Result<Option<File>, Error> {
    tokio::fs::create_dir_all(run).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(run, std::fs::Permissions::from_mode(0o700)).await?;
        let run = run.to_path_buf();
        let guard = tokio::task::spawn_blocking(move || {
            let f = File::open(&run).ok()?;
            f.lock_shared().ok()?;
            Some(f)
        })
        .await?;
        Ok(guard)
    }
    #[cfg(not(unix))]
    {
        Ok(None)
    }
}

/// Take the session lock for the shim's lifetime. Retries briefly (backoff) because a concurrent
/// [`probe`] may hold it for a moment.
pub(crate) async fn acquire(dir: &Path) -> Result<File, Error> {
    let path = dir.join(LOCK_FILE);
    let f = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    let mut backoff = std::time::Duration::from_millis(2);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match f.try_lock() {
            Ok(()) => return Ok(f),
            Err(TryLockError::WouldBlock) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(Error::Locked(path.display().to_string()));
                }
                // Backoff schedule while a probe briefly holds the lock.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(std::time::Duration::from_millis(100));
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
    }
}
