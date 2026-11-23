//! Working directories: the one normalisation, on the host where a session is created.
//!
//! A project folder reaches the host the way a person typed it (`~/src/x`) or a client built it.
//! The host is the only place that knows the home directory of the user the daemon runs as and
//! what its filesystem holds, so it normalises, once, and everything after that (the spawn,
//! `session.json`, the snapshot, discovery of resumable sessions) uses the one result:
//!
//! 1. a leading `~` (alone, or followed by a separator) is the home directory: `HOME` (Unix) or
//!    `USERPROFILE` (Windows) as the host's base environment has it
//!    ([`wasit_process::child_var`], else this process's), unless [`crate::RuntimeConfig::home`]
//!    names one;
//! 2. the result must be absolute (`~user`, `.`, `x/y` are refused: a relative path would mean
//!    the daemon's own working directory, which nobody typing a project folder means);
//! 3. it is resolved to the canonical path, which is the one the agent process sees as its
//!    working directory (symlinks resolved; on Windows without the `\\?\` prefix, see
//!    [`wasit_process::plain_path`]), and must be a directory.
//!
//! On a Windows host `~` is `%USERPROFILE%` (`~\x` and `~/x` both work), absolute means a drive
//! (`C:\…`) or UNC (`\\server\share\…`) path, and canonical is `canonicalize` without the
//! `\\?\` prefix. (Written against the Windows semantics of `std::fs::canonicalize`; not run on
//! a Windows host: the unit tests that name Windows paths only run there.)
//!
//! The agent files its own session storage under that path, so a session spawned in `~` and a
//! listing of resumable sessions for `~` must agree on it, which is why they share this code.

use std::path::{Path, PathBuf};

/// Why a working directory was refused. Every message names the path as given.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CwdError {
    #[error(
        "working directory {given:?} is not an absolute path (a leading ~ stands for the home \
         directory of the user the host runs as; ~user and relative paths are not accepted)"
    )]
    NotAbsolute { given: String },
    #[error(
        "working directory {given:?}: this host has no home directory for the user it runs as \
         (neither HOME nor USERPROFILE is set to an absolute path)"
    )]
    NoHome { given: String },
    #[error("working directory {given:?} does not exist (looked for {path:?})")]
    NotFound { given: String, path: String },
    #[error("working directory {given:?} is not a directory ({path:?})")]
    NotADirectory { given: String, path: String },
    #[error("working directory {given:?} cannot be used: {error}")]
    Unusable { given: String, error: String },
}

/// The home directory of the user the host runs as: `configured`, else the host's base
/// environment (`HOME`, on Windows `USERPROFILE`), else this process's. Must be absolute.
pub(crate) fn home_dir(configured: Option<&Path>) -> Option<PathBuf> {
    if let Some(h) = configured {
        return h.is_absolute().then(|| h.to_path_buf());
    }
    let name = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    let home = PathBuf::from(wasit_process::child_var(name)?);
    home.is_absolute().then_some(home)
}

/// Steps 1 and 2: the absolute path `given` names, before the filesystem is asked.
pub(crate) fn expand(given: &str, home: Option<&Path>) -> Result<PathBuf, CwdError> {
    let rest = match given.strip_prefix('~') {
        Some("") => Some(""),
        Some(r) if r.starts_with('/') || (cfg!(windows) && r.starts_with('\\')) => {
            Some(r.trim_start_matches(['/', '\\']))
        }
        _ => None,
    };
    let path = match rest {
        Some(rest) => {
            let home = home_dir(home).ok_or_else(|| CwdError::NoHome {
                given: given.to_string(),
            })?;
            if rest.is_empty() {
                home
            } else {
                home.join(rest)
            }
        }
        None => PathBuf::from(given),
    };
    if !path.is_absolute() {
        return Err(CwdError::NotAbsolute {
            given: given.to_string(),
        });
    }
    Ok(path)
}

/// Steps 1 to 3: the canonical directory `given` names, as text.
pub(crate) async fn resolve(given: &str, home: Option<&Path>) -> Result<String, CwdError> {
    let path = expand(given, home)?;
    let shown = path.to_string_lossy().into_owned();
    let canonical = match tokio::fs::canonicalize(&path).await {
        Ok(c) => wasit_process::plain_path(&c),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(CwdError::NotFound {
                given: given.to_string(),
                path: shown,
            })
        }
        Err(e) => {
            return Err(CwdError::Unusable {
                given: given.to_string(),
                error: e.to_string(),
            })
        }
    };
    match tokio::fs::metadata(&canonical).await {
        Ok(m) if m.is_dir() => {}
        Ok(_) => {
            return Err(CwdError::NotADirectory {
                given: given.to_string(),
                path: shown,
            })
        }
        Err(e) => {
            return Err(CwdError::Unusable {
                given: given.to_string(),
                error: e.to_string(),
            })
        }
    }
    canonical
        .into_os_string()
        .into_string()
        .map_err(|_| CwdError::Unusable {
            given: given.to_string(),
            error: "the path is not valid UTF-8".into(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("wcwd-{}-{}", std::process::id(), fastrand::u32(..)));
        std::fs::create_dir_all(&p).unwrap();
        wasit_process::plain_path(&std::fs::canonicalize(&p).unwrap())
    }

    #[test]
    fn a_leading_tilde_is_the_home_directory() {
        let home = PathBuf::from(if cfg!(windows) { r"C:\h" } else { "/h" });
        assert_eq!(expand("~", Some(&home)).unwrap(), home);
        assert_eq!(expand("~/", Some(&home)).unwrap(), home);
        assert_eq!(
            expand("~/a/b", Some(&home)).unwrap(),
            home.join("a").join("b")
        );
        // Only a leading tilde followed by a separator or nothing: not `~user`, not a tilde
        // inside a name.
        for bad in ["~user", "~user/x", "a/~", ""] {
            assert!(
                matches!(expand(bad, Some(&home)), Err(CwdError::NotAbsolute { .. })),
                "{bad:?}"
            );
        }
        let abs = if cfg!(windows) { r"C:\x\y" } else { "/x/y" };
        assert_eq!(expand(abs, Some(&home)).unwrap(), PathBuf::from(abs));
    }

    #[test]
    fn relative_paths_are_refused_not_resolved_against_the_daemon() {
        for bad in [".", "..", "x", "x/y", "./x"] {
            assert_eq!(
                expand(bad, None),
                Err(CwdError::NotAbsolute {
                    given: bad.to_string()
                })
            );
        }
    }

    #[test]
    fn a_tilde_without_a_home_is_its_own_error() {
        assert!(matches!(
            expand("~/x", Some(Path::new("relative"))),
            Err(CwdError::NoHome { .. })
        ));
    }

    #[tokio::test]
    async fn resolves_to_the_canonical_existing_directory() {
        let home = temp();
        std::fs::create_dir_all(home.join("sub")).unwrap();
        assert_eq!(
            resolve("~/sub", Some(&home)).await.unwrap(),
            home.join("sub").to_str().unwrap()
        );
        assert_eq!(
            resolve("~", Some(&home)).await.unwrap(),
            home.to_str().unwrap()
        );
        // `..` and symlinks are resolved: the agent sees the physical path.
        assert_eq!(
            resolve(&format!("{}/sub/../sub", home.display()), None)
                .await
                .unwrap(),
            home.join("sub").to_str().unwrap()
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(home.join("sub"), home.join("link")).unwrap();
            assert_eq!(
                resolve("~/link", Some(&home)).await.unwrap(),
                home.join("sub").to_str().unwrap()
            );
        }
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn a_missing_path_and_a_file_are_refused_naming_the_path_as_given() {
        let home = temp();
        std::fs::write(home.join("file"), b"x").unwrap();
        let e = resolve("~/missing", Some(&home)).await.unwrap_err();
        assert!(
            matches!(&e, CwdError::NotFound { given, .. } if given == "~/missing"),
            "{e:?}"
        );
        assert!(e.to_string().contains("\"~/missing\""));
        let e = resolve("~/file", Some(&home)).await.unwrap_err();
        assert!(
            matches!(&e, CwdError::NotADirectory { given, .. } if given == "~/file"),
            "{e:?}"
        );
        let _ = std::fs::remove_dir_all(&home);
    }
}
