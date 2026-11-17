//! Command line of the shim process:
//! `--dir <session_dir> [--cwd <dir>] [--env K=V]... [--linger-ms N] [--write-timeout-ms N]
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
    v.to_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::InvalidArgument(format!("{flag}: not a number: {v:?}")))
}

pub(crate) fn split_env(kv: &OsStr) -> Result<(OsString, OsString), Error> {
    let s = kv
        .to_str()
        .ok_or_else(|| Error::InvalidArgument(format!("--env is not UTF-8: {kv:?}")))?;
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.into(), v.into())),
        _ => Err(Error::InvalidArgument(format!(
            "--env expects K=V, got {s:?}"
        ))),
    }
}

pub(crate) fn parse<I: IntoIterator<Item = OsString>>(args: I) -> Result<Config, Error> {
    let mut it = args.into_iter();
    let mut dir = None;
    let mut cfg = Config {
        dir: PathBuf::new(),
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
            Some("--cwd") => cfg.cwd = Some(PathBuf::from(value(&mut it, "--cwd")?)),
            Some("--env") => cfg.env.push(split_env(&value(&mut it, "--env")?)?),
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
            _ => return Err(Error::InvalidArgument(format!("unexpected argument {a:?}"))),
        }
    }
    cfg.dir = dir.ok_or_else(|| Error::InvalidArgument("--dir is required".into()))?;
    cfg.program =
        program.ok_or_else(|| Error::InvalidArgument("missing `-- <program> [args...]`".into()))?;
    Ok(cfg)
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
}
