//! Command line of the shim process:
//! `--dir <session_dir> [--run <runtime_dir>] [--cwd <dir>] [--env K=V]... [--linger-ms N] [--write-timeout-ms N]
//!  [--max-record-bytes N] [--drain-timeout-ms N] -- <program> <args...>`

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::time::Duration;

use crate::Error;

pub(crate) const DEFAULT_LINGER: Duration = Duration::from_millis(3000);
pub(crate) const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_millis(5000);
pub(crate) const DEFAULT_MAX_RECORD: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_millis(2000);

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub dir: PathBuf,
    /// Machine-local directory of the lock and the control endpoint (default: `dir`).
    pub run: Option<PathBuf>,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(OsString, OsString)>,
    pub program: OsString,
    pub args: Vec<OsString>,
    /// After `exited` is durable, keep serving until a controller has received it or no record
    /// was delivered for this long.
    pub linger: Duration,
    /// A frame write to the controller taking longer than this disconnects it.
    pub write_timeout: Duration,
    /// Maximum bytes of line payload per record.
    pub max_record: usize,
    /// How long to wait for stdout/stderr EOF after the child exited.
    pub drain_timeout: Duration,
}

fn value(it: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<OsString, Error> {
    it.next()
        .ok_or_else(|| Error::InvalidArgument(format!("{flag} needs a value")))
}

fn number(v: &OsStr, flag: &str) -> Result<u64, Error> {
    // The value is not echoed: an argument error is reported to the caller and logged, and
    // nothing on the command line may reach either (it can carry secrets).
    v.to_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::InvalidArgument(format!("{flag}: not a number")))
}

/// Splits one `--env K=V`. `n` is its position among the `--env` flags (1-based): the only
/// thing an error says about it. The variable's value (and, where it cannot be told from the
/// value, its name) is never echoed: the message goes to the readiness line, to `shim.log` and
/// back to the caller, and the value may be a secret (design leak L2).
pub(crate) fn split_env(kv: &OsStr, n: usize) -> Result<(OsString, OsString), Error> {
    let s = kv
        .to_str()
        .ok_or_else(|| Error::InvalidArgument(format!("--env #{n} is not UTF-8")))?;
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.into(), v.into())),
        _ => Err(Error::InvalidArgument(format!(
            "--env #{n} is not NAME=VALUE (no `=`, or an empty name)"
        ))),
    }
}

/// An unexpected argument as an error message shows it: a flag by name, anything else by its
/// size (it may be a value that landed in the wrong place).
fn describe(a: &OsStr) -> String {
    match a.to_str() {
        Some(s)
            if s.starts_with("--")
                && s.len() <= 40
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') =>
        {
            s.to_owned()
        }
        _ => format!("<{} bytes>", a.len()),
    }
}

/// The `--dir` of an argument list, without parsing the rest: lets a shim whose arguments are
/// invalid still write its `shim.not_ready` record where the session's log is.
pub(crate) fn dir_hint(args: &[OsString]) -> Option<PathBuf> {
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.to_str() {
            Some("--") => return None,
            Some("--dir") => return it.next().map(PathBuf::from),
            _ => {}
        }
    }
    None
}

pub(crate) fn parse<I: IntoIterator<Item = OsString>>(args: I) -> Result<Config, Error> {
    let mut it = args.into_iter();
    let mut dir = None;
    let mut cfg = Config {
        dir: PathBuf::new(),
        run: None,
        cwd: None,
        env: Vec::new(),
        program: OsString::new(),
        args: Vec::new(),
        linger: DEFAULT_LINGER,
        write_timeout: DEFAULT_WRITE_TIMEOUT,
        max_record: DEFAULT_MAX_RECORD,
        drain_timeout: DEFAULT_DRAIN_TIMEOUT,
    };
    let mut program = None;
    while let Some(a) = it.next() {
        match a.to_str() {
            Some("--") => {
                program = it.next();
                cfg.args = it.by_ref().collect();
                break;
            }
            Some("--dir") => dir = Some(PathBuf::from(value(&mut it, "--dir")?)),
            Some("--run") => cfg.run = Some(PathBuf::from(value(&mut it, "--run")?)),
            Some("--cwd") => cfg.cwd = Some(PathBuf::from(value(&mut it, "--cwd")?)),
            Some("--env") => {
                let kv = value(&mut it, "--env")?;
                cfg.env.push(split_env(&kv, cfg.env.len() + 1)?);
            }
            Some("--linger-ms") => {
                cfg.linger =
                    Duration::from_millis(number(&value(&mut it, "--linger-ms")?, "--linger-ms")?)
            }
            Some("--write-timeout-ms") => {
                cfg.write_timeout = Duration::from_millis(number(
                    &value(&mut it, "--write-timeout-ms")?,
                    "--write-timeout-ms",
                )?)
            }
            Some("--max-record-bytes") => {
                let n = number(&value(&mut it, "--max-record-bytes")?, "--max-record-bytes")?;
                if n < 16 {
                    return Err(Error::InvalidArgument(
                        "--max-record-bytes must be >= 16".into(),
                    ));
                }
                cfg.max_record = n as usize;
            }
            Some("--drain-timeout-ms") => {
                cfg.drain_timeout = Duration::from_millis(number(
                    &value(&mut it, "--drain-timeout-ms")?,
                    "--drain-timeout-ms",
                )?)
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "unexpected argument {}",
                    describe(&a)
                )))
            }
        }
    }
    cfg.dir = dir.ok_or_else(|| Error::InvalidArgument("--dir is required".into()))?;
    cfg.program =
        program.ok_or_else(|| Error::InvalidArgument("missing `-- <program> [args...]`".into()))?;
    Ok(cfg)
}

impl Config {
    /// Directory of `shim.lock` and the control endpoint.
    pub(crate) fn run_dir(&self) -> &std::path::Path {
        self.run.as_deref().unwrap_or(&self.dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(v: &[&str]) -> Vec<OsString> {
        v.iter().map(OsString::from).collect()
    }

    #[test]
    fn parses_full_command_line() {
        let c = parse(os(&[
            "--dir",
            "/d",
            "--run",
            "/r",
            "--cwd",
            "/w",
            "--env",
            "A=b=c",
            "--linger-ms",
            "5",
            "--",
            "prog",
            "--dir",
            "x",
        ]))
        .unwrap();
        assert_eq!(c.dir, PathBuf::from("/d"));
        assert_eq!(c.run_dir(), std::path::Path::new("/r"));
        assert_eq!(c.cwd, Some(PathBuf::from("/w")));
        assert_eq!(c.env, vec![("A".into(), "b=c".into())]);
        assert_eq!(c.linger, Duration::from_millis(5));
        assert_eq!(c.program, OsString::from("prog"));
        assert_eq!(c.args, os(&["--dir", "x"]));
    }

    #[test]
    fn rejects_missing_program_and_bad_env() {
        assert!(parse(os(&["--dir", "/d"])).is_err());
        assert!(parse(os(&["--dir", "/d", "--env", "noeq", "--", "p"])).is_err());
        assert!(parse(os(&["--", "p"])).is_err());
    }

    /// Leak L2: an argument error never echoes what the argument held.
    #[test]
    fn argument_errors_do_not_echo_values() {
        const MARK: &str = "SECRET-MARK-8d41";
        let bad_env = [
            // No `=`: the whole argument may be a value.
            MARK.to_string(),
            // An empty name: what follows `=` is the value.
            format!("={MARK}"),
        ];
        for kv in &bad_env {
            let e = parse(os(&["--dir", "/d", "--env", kv, "--", "p"])).unwrap_err();
            assert!(matches!(e, Error::InvalidArgument(_)));
            assert!(!e.to_string().contains(MARK), "{e}");
            assert!(e.to_string().contains("--env #1"), "{e}");
        }
        // The position counts the `--env` flags.
        let e = parse(os(&[
            "--dir", "/d", "--env", "A=1", "--env", MARK, "--", "p",
        ]))
        .unwrap_err();
        assert!(e.to_string().contains("--env #2"), "{e}");

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let mut raw = format!("K={MARK}").into_bytes();
            raw.push(0xff);
            let e = split_env(&OsString::from_vec(raw), 4).unwrap_err();
            assert!(!e.to_string().contains(MARK), "{e}");
            assert!(e.to_string().contains("--env #4"), "{e}");
        }

        let e = parse(os(&["--dir", "/d", "--linger-ms", MARK, "--", "p"])).unwrap_err();
        assert!(!e.to_string().contains(MARK), "{e}");
        let e = parse(os(&["--dir", "/d", MARK, "--", "p"])).unwrap_err();
        assert!(!e.to_string().contains(MARK), "{e}");
        let e = parse(os(&["--dir", "/d", "--surprise", "--", "p"])).unwrap_err();
        assert!(e.to_string().contains("--surprise"), "{e}");
    }

    #[test]
    fn dir_hint_reads_the_directory_before_the_program() {
        assert_eq!(
            dir_hint(&os(&["--env", "A=b", "--dir", "/d", "--", "p"])),
            Some(PathBuf::from("/d"))
        );
        assert_eq!(dir_hint(&os(&["--", "--dir", "/d"])), None);
        assert_eq!(dir_hint(&os(&["--dir"])), None);
    }
}
