//! Per-machine identity and the machine-local runtime directory.
//!
//! A host's home directory may be shared by several machines (NFS and similar). Everything
//! that is per-kernel — Unix sockets, advisory locks, pid/readiness files — must then live
//! outside it, and durable data must be keyed by the machine so two machines never adopt each
//! other's sessions. This module answers both questions:
//!
//! * [`machine_key`]: a short, stable, non-secret key for this machine, derived from the OS
//!   machine id (Linux `/etc/machine-id`, macOS `IOPlatformUUID`, Windows `MachineGuid`),
//!   falling back to the host name. The raw id is hashed (it is meant to stay confidential).
//! * [`runtime_dir`]: a machine-local, owner-only directory for sockets and locks.
//!
//! Runtime directory choice (Unix), from machine facts only — never from the calling session's
//! environment, so a desktop-started daemon and an ssh-started bridge agree: `/run/user/<uid>/<app>`
//! only when the user has systemd lingering enabled — otherwise `/run/user/<uid>` is removed when
//! the user's last login session ends (e.g. the ssh session that started a detached daemon),
//! which would delete the sockets of shims and daemons that keep running. Without lingering:
//! `/tmp/<app>-<uid>` (outlives logins; wiped at reboot, when every session is dead anyway).
//! macOS: `<DARWIN_USER_TEMP_DIR>/<app>` (`getconf`; the same per-user dir as a GUI's `$TMPDIR`).
//! The directory is created 0700 and refused when it is not a real directory
//! owned by us or is accessible by group/others. Processes holding sockets there keep a
//! shared `flock` on their directory: systemd-tmpfiles skips locked directories when aging.
//! Windows: `%LOCALAPPDATA%\<App>\run` (local, never roamed); named pipes are machine-local.

use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Characters allowed in an explicit machine key.
fn valid_key(k: &str) -> bool {
    !k.is_empty()
        && k.len() <= 40
        && k.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Hash a raw machine id into a key: 16 lowercase hex characters.
pub fn key_from_raw(raw: &str) -> String {
    let d = Sha256::digest(format!("wasit-machine:{}", raw.trim()).as_bytes());
    d.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// This machine's key. `explicit` (e.g. from an environment override) wins when it is a valid
/// key (`[A-Za-z0-9_-]{1,40}`); an invalid one is an error.
pub fn machine_key(explicit: Option<&str>) -> io::Result<String> {
    if let Some(k) = explicit.filter(|k| !k.is_empty()) {
        if valid_key(k) {
            return Ok(k.to_string());
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid machine key {k:?} (expected [A-Za-z0-9_-]{{1,40}})"),
        ));
    }
    match raw_machine_id() {
        Some(raw) => Ok(key_from_raw(&raw)),
        None => Ok(key_from_raw(&format!("host:{}", host_name()))),
    }
}

/// The OS machine id, if any.
fn raw_machine_id() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let out = wasit_process::std_command("ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout);
        text.lines()
            .find(|l| l.contains("IOPlatformUUID"))
            .and_then(|l| l.split('"').nth(3))
            .map(str::to_string)
            .filter(|s| !s.is_empty())
    }
    #[cfg(windows)]
    {
        let out = wasit_process::std_command("reg")
            .args([
                "query",
                r"HKLM\SOFTWARE\Microsoft\Cryptography",
                "/v",
                "MachineGuid",
            ])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout);
        text.lines()
            .find(|l| l.contains("MachineGuid"))
            .and_then(|l| l.split_whitespace().last())
            .map(str::to_string)
            .filter(|s| !s.is_empty())
    }
    #[cfg(not(any(target_os = "macos", windows)))]
    {
        ["/etc/machine-id", "/var/lib/dbus/machine-id"]
            .iter()
            .filter_map(|p| std::fs::read_to_string(p).ok())
            .map(|s| s.trim().to_string())
            .find(|s| !s.is_empty() && s.chars().any(|c| c != '0'))
    }
}

/// The host name (environment, `/etc/hostname`, else `localhost`).
pub fn host_name() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "localhost".into())
}

/// The machine-local runtime directory for application `app` (lowercase, e.g. `marsad`),
/// created and checked. `explicit` (an override) is used as is, with the same checks.
pub fn runtime_dir(app: &str, explicit: Option<&Path>) -> io::Result<PathBuf> {
    let dir = match explicit {
        Some(p) => p.to_path_buf(),
        None => default_runtime_dir(app)?,
    };
    ensure_private_dir(&dir)?;
    Ok(dir)
}

#[cfg(windows)]
fn default_runtime_dir(app: &str) -> io::Result<PathBuf> {
    let base = std::env::var_os("LOCALAPPDATA")
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "LOCALAPPDATA is not set"))?;
    let mut name = app.to_string();
    if let Some(f) = name.get_mut(0..1) {
        f.make_ascii_uppercase();
    }
    Ok(PathBuf::from(base).join(name).join("run"))
}

#[cfg(unix)]
fn default_runtime_dir(app: &str) -> io::Result<PathBuf> {
    // Derived from facts of the machine and user only, never from the calling session's
    // environment: a daemon started from a desktop session and a bridge started over ssh must
    // agree on one directory (and so on one single-instance lock).
    let uid = wasit_process::effective_uid();
    #[cfg(target_os = "macos")]
    if let Some(t) = darwin_user_temp_dir() {
        return Ok(t.join(app));
    }
    #[cfg(not(target_os = "macos"))]
    {
        let run = PathBuf::from(format!("/run/user/{uid}"));
        if lingering(uid) && owned_private(&run, uid) {
            return Ok(run.join(app));
        }
    }
    Ok(PathBuf::from(format!("/tmp/{app}-{uid}")))
}

/// The per-user temporary directory (`confstr(_CS_DARWIN_USER_TEMP_DIR)`), the same for every
/// session of the user.
#[cfg(target_os = "macos")]
fn darwin_user_temp_dir() -> Option<PathBuf> {
    let out = wasit_process::std_command_inheriting("getconf")
        .arg("DARWIN_USER_TEMP_DIR")
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty() && Path::new(&s).is_dir()).then(|| PathBuf::from(s))
}

/// Whether systemd keeps this user's runtime directory after their last session ends.
#[cfg(all(unix, not(target_os = "macos")))]
fn lingering(uid: u32) -> bool {
    let Some(name) = user_name(uid) else {
        return false;
    };
    Path::new("/var/lib/systemd/linger").join(name).exists()
}

#[cfg(all(unix, not(target_os = "macos")))]
fn user_name(uid: u32) -> Option<String> {
    let passwd = std::fs::read_to_string("/etc/passwd").ok()?;
    passwd.lines().find_map(|l| {
        let f: Vec<&str> = l.split(':').collect();
        (f.len() > 2 && f[2] == uid.to_string()).then(|| f[0].to_string())
    })
}

#[cfg(unix)]
fn owned_private(p: &Path, uid: u32) -> bool {
    use std::os::unix::fs::MetadataExt;
    std::fs::symlink_metadata(p)
        .map(|m| m.is_dir() && m.uid() == uid && m.mode() & 0o077 == 0)
        .unwrap_or(false)
}

/// Create `dir` (owner-only) if missing, then refuse it unless it is a real directory owned by
/// the current user and not accessible by group or others.
pub fn ensure_private_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e),
        }
        let m = std::fs::symlink_metadata(dir)?;
        let uid = wasit_process::effective_uid();
        if !m.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{} is not a directory", dir.display()),
            ));
        }
        if m.uid() != uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "{} is owned by uid {}, not by this user ({uid}); refusing to use it",
                    dir.display(),
                    m.uid()
                ),
            ));
        }
        if m.mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "{} is accessible by other users (mode {:o}); refusing to use it",
                    dir.display(),
                    m.mode() & 0o777
                ),
            ));
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_stable_short_and_validated() {
        let k = key_from_raw("abc\n");
        assert_eq!(k, key_from_raw("abc"));
        assert_eq!(k.len(), 16);
        assert_ne!(k, key_from_raw("abd"));
        assert_eq!(machine_key(Some("box-1")).unwrap(), "box-1");
        assert!(machine_key(Some("../x")).is_err());
        let own = machine_key(None).unwrap();
        assert_eq!(own, machine_key(None).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_dir_permissions_are_checked() {
        use std::os::unix::fs::PermissionsExt;
        let t = tempfile::tempdir().unwrap();
        let ok = t.path().join("run");
        assert_eq!(runtime_dir("x", Some(&ok)).unwrap(), ok);
        let mode = std::fs::metadata(&ok).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        // Group/world-accessible: refused.
        let open = t.path().join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let e = runtime_dir("x", Some(&open)).unwrap_err();
        assert!(e.to_string().contains("accessible by other users"), "{e}");
        // A symlink to a private directory: refused (not a real directory).
        let link = t.path().join("link");
        std::os::unix::fs::symlink(&ok, &link).unwrap();
        assert!(runtime_dir("x", Some(&link)).is_err());
        // Owned by someone else: refused (only checkable as root via chown).
        if wasit_process::effective_uid() == 0 {
            let other = t.path().join("other");
            std::fs::create_dir(&other).unwrap();
            std::fs::set_permissions(&other, std::fs::Permissions::from_mode(0o700)).unwrap();
            std::os::unix::fs::chown(&other, Some(65534), Some(65534)).unwrap();
            let e = runtime_dir("x", Some(&other)).unwrap_err();
            assert!(e.to_string().contains("owned by uid 65534"), "{e}");
        }
        // The default resolves somewhere private.
        let d = runtime_dir("wasit-test", None).unwrap();
        assert!(owned_private(&d, wasit_process::effective_uid()));
    }
}
