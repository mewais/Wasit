//! Owner-only files and directories (design S23, leak L4): everything Marsad writes that may
//! hold session data or evidence about it — session directories, journals, `session.json`, the
//! shim's log, the diagnostics files — is readable by its owner only.
//!
//! On Unix directories are `0700` and files `0600`; what an older build created with wider
//! permissions is *tightened* (group and other bits cleared, nothing widened) the next time the
//! path is opened through here. Elsewhere every function only creates what is missing: there is
//! no ACL code (a Windows profile directory is already private to its user).
//!
//! This crate hosts the helpers because its own files need them and every process that writes
//! such files already depends on it (the shim, the runtime); there is exactly one definition of
//! "private". The functions are synchronous: asynchronous callers use
//! [`file_options`] with `tokio::fs::OpenOptions::from(..)` and run [`create_dir_all`] /
//! [`tighten_dir`] / [`tighten_file`] on the blocking pool.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;

/// Mode of a directory created here.
#[cfg(unix)]
pub const DIR_MODE: u32 = 0o700;
/// Mode of a file created here.
#[cfg(unix)]
pub const FILE_MODE: u32 = 0o600;

/// Creates `path` and every missing parent as a private directory, and tightens `path` itself
/// when it already existed. (Existing parents are left as they are.)
pub fn create_dir_all(path: &Path) -> io::Result<()> {
    let mut b = fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(DIR_MODE);
    }
    b.create(path)?;
    tighten_dir(path)
}

/// Clears the group and other permissions of the directory `path` (Unix); a missing path is
/// fine.
pub fn tighten_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    return tighten(path, DIR_MODE);
    #[cfg(not(unix))]
    {
        let _ = path; // nothing to tighten
        Ok(())
    }
}

/// Clears the group and other permissions of the file `path` (Unix); a missing path is fine.
pub fn tighten_file(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    return tighten(path, FILE_MODE);
    #[cfg(not(unix))]
    {
        let _ = path; // nothing to tighten
        Ok(())
    }
}

#[cfg(unix)]
fn tighten(path: &Path, keep: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mode = meta.permissions().mode() & 0o7777;
    let want = mode & keep;
    if want != mode {
        fs::set_permissions(path, fs::Permissions::from_mode(want))?;
    }
    Ok(())
}

/// Options that create a file private to its owner (`create(true)`, mode `0600` on Unix). The
/// caller adds `read`/`write`/`append`/`truncate`. The mode applies to a file created by the
/// open; an existing file is [`tighten_file`]d separately.
pub fn file_options() -> OpenOptions {
    let mut o = OpenOptions::new();
    o.create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(FILE_MODE);
    }
    o
}

/// Opens (creating when missing) the private file `path` with the options `configure` adds to
/// [`file_options`]; an existing file is tightened first (before the open, so an open that
/// blocks, as on a FIFO, does not hold the tightening back).
pub fn open(path: &Path, configure: impl FnOnce(&mut OpenOptions)) -> io::Result<File> {
    tighten_file(path)?;
    let mut o = file_options();
    configure(&mut o);
    o.open(path)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode(p: &Path) -> u32 {
        fs::metadata(p).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn creates_private_and_tightens_without_widening() {
        let t = tempfile::tempdir().unwrap();
        let deep = t.path().join("a/b/c");
        create_dir_all(&deep).unwrap();
        for p in [t.path().join("a"), t.path().join("a/b"), deep.clone()] {
            assert_eq!(mode(&p), 0o700, "{}", p.display());
        }

        let f = deep.join("f");
        drop(open(&f, |o| {
            o.write(true);
        }));
        assert_eq!(mode(&f), 0o600);

        fs::set_permissions(&deep, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&f, fs::Permissions::from_mode(0o664)).unwrap();
        create_dir_all(&deep).unwrap();
        drop(open(&f, |o| {
            o.append(true);
        }));
        assert_eq!(mode(&deep), 0o700);
        assert_eq!(mode(&f), 0o600);

        // Narrower than private stays as it is.
        fs::set_permissions(&f, fs::Permissions::from_mode(0o400)).unwrap();
        tighten_file(&f).unwrap();
        assert_eq!(mode(&f), 0o400);
        // A missing path is not an error.
        tighten_file(&t.path().join("none")).unwrap();
        tighten_dir(&t.path().join("none")).unwrap();
    }
}
