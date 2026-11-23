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

/// Probe a session directory without blocking: a non-blocking shared `try_lock` on `shim.lock`,
/// then (if free) a bounded read of the journal's last record.
///
/// The lock protocol (the one place it is written down):
///
/// * A shim holds `shim.lock` **exclusively** for as long as it lives; that is the liveness
///   signal.
/// * A probe asks with a **shared** try-lock, so probes never see each other as a shim (several
///   tasks probing one session at once is normal: the manager adopting, an actor, a snapshot
///   loader), and releases it at once with an explicit **unlock**, never by closing the
///   descriptor. `flock` locks belong to the open file *description*; a `fork` on another thread
///   copies the descriptor, the copy keeps the description open until the child's `exec`, and a
///   release by `close` would leave the lock held through that copy: the next probe would see a
///   shim that does not exist. `LOCK_UN` on any descriptor of the description releases the lock
///   whoever else still has a copy (Linux and the BSDs/macOS alike; Windows handles are not
///   inherited by process creation here and have no such copies).
/// * A starting shim waits for the probes' momentary shared holds ([`acquire`]).
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
        Ok(f) => match f.try_lock_shared() {
            Ok(()) => {
                // Held only for this check, and given back explicitly (see above): closing
                // `f` alone would not release it while a forked child still has a copy.
                f.unlock()?;
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

/// Take the session lock for the shim's lifetime.
///
/// Who can hold `shim.lock` when a shim starts: nobody (the usual case: taken at once); a probe,
/// shared, for the instant between its try-lock and its unlock; or another shim, exclusive, for
/// its whole life. The first two end by themselves, so the lock is waited for (a blocking
/// `flock`, on the blocking pool; no timer, no polling); the third is an error and is reported at
/// once. The two are told apart by asking for a shared lock: that fails only while an exclusive
/// holder exists.
///
/// (A second shim of the same session starting between that question and the blocking request
/// would be waited for instead of refused. The daemon starts one generation of a session at a
/// time, from that session's one actor, so this cannot happen in the running system.)
pub(crate) async fn acquire(dir: &Path) -> Result<File, Error> {
    let path = dir.join(LOCK_FILE);
    tokio::task::spawn_blocking(move || {
        let f = wasit_diag::private::open(&path, |o| {
            o.read(true).write(true).truncate(false);
        })?;
        match f.try_lock() {
            Ok(()) => return Ok(f),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        match f.try_lock_shared() {
            Ok(()) => f.unlock()?,
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Locked(path.display().to_string()));
            }
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        f.lock()?;
        Ok(f)
    })
    .await?
}
