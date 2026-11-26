//! Per-machine identity and the machine-local runtime directory.
//!
//! A host's home directory may be shared by several machines (NFS and similar). Everything
//! that is per-kernel — Unix sockets, advisory locks, pid/readiness files — must then live
//! outside it, and durable data must be keyed by the machine so two machines never adopt each
//! other's sessions. This module answers both questions:
//!
//! * [`machine_key`]: a short, stable key for this machine: a hash of the OS machine id (Linux
//!   `/etc/machine-id`, macOS `IOPlatformUUID`, Windows `MachineGuid`). The raw id is hashed (it
//!   is meant to stay confidential). Only when there is no machine id (WSL distributions without
//!   one) is the key a hash of the kernel's host name instead. The host name is deliberately
//!   NOT mixed in when a machine id exists: host names change with the network (macOS), and a
//!   rename would orphan every session. No file is stored for this key either: a file under a
//!   temporary directory could be aged out, and the key would then change and orphan every
//!   session.
//!
//!   Cloned images share a machine id and so would share a key. Such a machine must be given its
//!   own key with the `MARSAD_MACHINE_KEY` override (`[A-Za-z0-9_-]{1,40}`): the caller reads
//!   that variable and passes it as `explicit` to [`machine_key`], which then uses it as is.
//! * [`runtime_dir`]: a machine-local, owner-only directory for sockets and locks.
//!
//! Runtime directory choice (Unix), from machine facts only — never from the calling session's
//! environment, so a desktop-started daemon and an ssh-started bridge agree: `/run/user/<uid>/<app>`
//! only when the user has systemd lingering enabled — otherwise `/run/user/<uid>` is removed when
//! the user's last login session ends (e.g. the ssh session that started a detached daemon),
//! which would delete the sockets of shims and daemons that keep running. Without lingering:
//! `/tmp/<app>-<uid>` (outlives logins; wiped at reboot, when every session is dead anyway).
//! macOS: `<DARWIN_USER_TEMP_DIR>/<app>` (`getconf`; the same per-user dir as a GUI's `$TMPDIR`).
//! The choice is recorded once per machine in the state directory and used from then on, so a
//! running daemon's directory does not move when linger is enabled or disabled later.
//! The directory is created 0700 and refused when it is not a real directory
//! owned by us or is accessible by group/others. A process holding sockets there keeps a
//! shared `flock` on its directory ([`hold_dir_guard`]): systemd-tmpfiles skips locked
//! directories when aging.
//! Windows: `%LOCALAPPDATA%\<App>\run` (local, never roamed); named pipes are machine-local.

use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The application whose state directory holds the recorded runtime directory choices. Fixed:
/// every application on the machine must find the same choices. (Unix only: Windows keeps its
/// runtime directory under `%LOCALAPPDATA%` without a recorded choice.)
#[cfg(unix)]
const STATE_APP: &str = "wasit";

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

/// This machine's key. `explicit` (the `MARSAD_MACHINE_KEY` override, read by the caller) wins
/// when it is a valid key (`[A-Za-z0-9_-]{1,40}`); an invalid one is an error. Otherwise the key
/// is derived from this machine's facts (see the module doc).
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
    Ok(key_from_machine_id(raw_machine_id().as_deref(), host_name))
}

/// The key for a machine whose OS machine id is `raw` (`None` when it has none). `host` is asked
/// for the kernel host name only when there is no id, so a machine with an id never runs the
/// host name lookup.
fn key_from_machine_id(raw: Option<&str>, host: impl FnOnce() -> String) -> String {
    match raw {
        Some(raw) => key_from_raw(raw),
        None => key_from_raw(&format!("host:{}", host())),
    }
}

/// The outcome of [`publish`].
#[cfg(unix)]
enum Published {
    /// The file was created with the content asked for.
    Created,
    /// The file already existed; it is left as it was.
    Existed,
}

/// Create `path` with `content` only if it does not exist yet. The content is written to a
/// private temporary file first and linked into place, so a reader never sees it half written
/// and a concurrent creator can not replace it.
#[cfg(unix)]
fn publish(path: &Path, content: &str) -> io::Result<Published> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    std::fs::write(&tmp, format!("{content}\n"))?;
    let linked = std::fs::hard_link(&tmp, path);
    let _ = std::fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(Published::Created),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(Published::Existed),
        Err(e) => Err(e),
    }
}

/// The machine-local state directory: where the recorded runtime directory choices live. It must
/// not be in a home directory that machines share. Windows: `%LOCALAPPDATA%\Wasit\state`; Unix:
/// `/var/tmp/wasit-<uid>`, created 0700 and owner-checked like the runtime directory. Nothing
/// the machine key depends on is stored here: the key is derived from machine facts only.
pub fn state_dir() -> io::Result<PathBuf> {
    #[cfg(windows)]
    {
        let base = std::env::var_os("LOCALAPPDATA")
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "LOCALAPPDATA is not set"))?;
        Ok(PathBuf::from(base).join("Wasit").join("state"))
    }
    #[cfg(unix)]
    {
        Ok(PathBuf::from(format!(
            "/var/tmp/{STATE_APP}-{}",
            wasit_process::effective_uid()
        )))
    }
}

/// Keeps `dir` from being aged away by a temporary-directory cleaner while the guard lives:
/// a shared `flock` on the directory (systemd-tmpfiles skips locked directories). Best effort:
/// a filesystem without directory locks gets no guard. Hold it for as long as the directory's
/// sockets or locks are in use.
pub fn hold_dir_guard(dir: &Path) -> Option<std::fs::File> {
    #[cfg(unix)]
    {
        let f = std::fs::File::open(dir).ok()?;
        f.lock_shared().ok()?;
        Some(f)
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        None
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

/// The host name (environment, `/etc/hostname`, else `localhost`). It is the input of the machine
/// key only when there is no machine id, and it must stay exactly as HEAD computed it (`$HOSTNAME`
/// first) so that no existing install's key changes.
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
    // The first choice on this machine is kept: lingering can change later, and a daemon running
    // in the old directory must still be the one every process finds.
    let state = state_dir()?;
    recorded_choice(&state, app, || unix_runtime_dir(app))
}

/// The runtime directory `candidate` chose, as recorded in `state` for `app` by the first call
/// on this machine (`candidate` is not called after that).
#[cfg(unix)]
fn recorded_choice(
    state: &Path,
    app: &str,
    candidate: impl FnOnce() -> PathBuf,
) -> io::Result<PathBuf> {
    ensure_private_dir(state)?;
    let file = state.join(format!("runtime-{app}"));
    let read = |p: &Path| -> io::Result<PathBuf> {
        let text = std::fs::read_to_string(p)?;
        let dir = PathBuf::from(text.trim());
        if dir.is_absolute() {
            Ok(dir)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} does not hold a directory path", p.display()),
            ))
        }
    };
    match read(&file) {
        Ok(dir) => return Ok(dir),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let chosen = candidate();
    match publish(&file, &chosen.to_string_lossy())? {
        Published::Created => Ok(chosen),
        Published::Existed => read(&file),
    }
}

#[cfg(unix)]
fn unix_runtime_dir(app: &str) -> PathBuf {
    let uid = wasit_process::effective_uid();
    #[cfg(target_os = "macos")]
    if let Some(t) = darwin_user_temp_dir() {
        return t.join(app);
    }
    #[cfg(not(target_os = "macos"))]
    {
        let run = PathBuf::from(format!("/run/user/{uid}"));
        if lingering(uid) && owned_private(&run, uid) {
            return run.join(app);
        }
    }
    PathBuf::from(format!("/tmp/{app}-{uid}"))
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

/// The owner-only scratch directory `name` inside the per-user runtime directory of `app`,
/// created and checked (see [`ensure_private_dir`]). For throwaway files a session may leave
/// behind: a predictable name in a shared temporary directory would let another local user
/// plant what the session reads (project config, plugins) before it runs.
pub fn private_scratch_dir(app: &str, name: &str) -> io::Result<PathBuf> {
    let dir = runtime_dir(app, None)?.join(name);
    ensure_private_dir(&dir)?;
    Ok(dir)
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

    #[test]
    fn the_same_machine_id_gives_the_same_key_whatever_the_host_name() {
        // Host names change (networks, renames), so they must not move a machine's key. The host
        // name is not even asked for when there is an id.
        let no_host = || -> String { panic!("the host name must not be consulted") };
        let k = key_from_machine_id(Some("machine-id"), no_host);
        assert_eq!(k, key_from_raw("machine-id"));
        // Pinned value (SHA-256 of "wasit-machine:machine-id", first 8 bytes): the key of every
        // install that predates the host name rule.
        assert_eq!(k, "38bc7a7a9ead30f8");
        // A cloned image: same id, another host name, same key (the MARSAD_MACHINE_KEY override
        // is what separates such machines).
        assert_eq!(
            k,
            key_from_machine_id(Some("machine-id"), || "box-b".to_string())
        );
        // Another id is another machine.
        assert_ne!(
            k,
            key_from_machine_id(Some("other-id"), || "box-a".to_string())
        );
        // A trailing newline in the raw id (as read from a file) does not change the key.
        assert_eq!(k, key_from_machine_id(Some("machine-id\n"), no_host));
    }

    #[test]
    fn without_a_machine_id_the_host_name_decides() {
        // WSL distributions without /etc/machine-id: the host name is the only fact.
        let a = key_from_machine_id(None, || "box-a".to_string());
        assert_eq!(a.len(), 16);
        assert_ne!(a, key_from_machine_id(None, || "box-b".to_string()));
        // Exactly HEAD's derivation: the host name is namespaced under "host:" before hashing,
        // so it can not collide with a machine id of the same text.
        assert_eq!(a, key_from_raw("host:box-a"));
        assert_ne!(a, key_from_raw("box-a"));
        // Pinned value (SHA-256 of "wasit-machine:host:box-a", first 8 bytes), so an
        // accidental change to the derivation fails here rather than orphaning installs.
        assert_eq!(a, "f89a0c029db098aa");
    }

    #[test]
    fn the_key_is_derived_from_this_machines_facts() {
        let expect = key_from_machine_id(raw_machine_id().as_deref(), host_name);
        assert_eq!(machine_key(None).unwrap(), expect);
        assert_eq!(machine_key(None).unwrap(), machine_key(None).unwrap());
    }

    #[test]
    fn an_explicit_key_overrides_the_derived_one() {
        assert_eq!(machine_key(Some("clone-2")).unwrap(), "clone-2");
        assert!(machine_key(Some("bad key")).is_err());
        // An empty override is no override.
        assert_eq!(machine_key(Some("")).unwrap(), machine_key(None).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_dir_choice_is_recorded_once() {
        // The first choice on the machine wins: a later call (linger enabled since, say) gets
        // the recorded directory and does not even ask for a new one.
        let t = tempfile::tempdir().unwrap();
        let state = t.path().join("state");
        let first = PathBuf::from("/tmp/app-first");
        let got = recorded_choice(&state, "app", || first.clone()).unwrap();
        assert_eq!(got, first);
        let again = recorded_choice(&state, "app", || PathBuf::from("/run/user/1/app")).unwrap();
        assert_eq!(again, first);
        // Recorded per application.
        let other = recorded_choice(&state, "other", || PathBuf::from("/run/user/1/other"));
        assert_eq!(other.unwrap(), PathBuf::from("/run/user/1/other"));
    }

    #[cfg(unix)]
    #[test]
    fn hold_dir_guard_locks_a_directory_shared() {
        let t = tempfile::tempdir().unwrap();
        let guard = hold_dir_guard(t.path()).expect("a directory lock");
        // A second shared holder is fine; the guard is released when dropped.
        let second = hold_dir_guard(t.path()).expect("shared with the first");
        drop(second);
        drop(guard);
    }

    #[cfg(unix)]
    #[test]
    fn private_scratch_dir_is_owned_and_private() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let dir = private_scratch_dir("wasit-test", "scratch-test").unwrap();
        assert!(dir.ends_with("scratch-test"));
        assert!(!dir.starts_with("/tmp/wasit-discovery"));
        let m = std::fs::symlink_metadata(&dir).unwrap();
        assert!(m.is_dir());
        assert_eq!(m.uid(), wasit_process::effective_uid());
        assert_eq!(m.permissions().mode() & 0o777, 0o700);
        // Reused on the next call, still private.
        assert_eq!(
            private_scratch_dir("wasit-test", "scratch-test").unwrap(),
            dir
        );
        // Someone else's (here: a world-readable) directory at that name is refused, not used.
        let base = runtime_dir("wasit-test", None).unwrap();
        let planted = base.join("planted-test");
        std::fs::create_dir_all(&planted).unwrap();
        std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o755)).unwrap();
        let e = private_scratch_dir("wasit-test", "planted-test").unwrap_err();
        assert!(e.to_string().contains("accessible by other users"), "{e}");
        // A symlink planted at that name is refused too.
        let link = base.join("link-test");
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        assert!(private_scratch_dir("wasit-test", "link-test").is_err());
        let _ = std::fs::remove_dir_all(&planted);
        let _ = std::fs::remove_file(&link);
    }
}
