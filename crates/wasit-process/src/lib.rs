//! The one place child processes are created (Wasit, Wasl and Marsad all go through it).
//!
//! **Rule:** no code outside this crate calls `std::process::Command::new` or
//! `tokio::process::Command::new`; it calls [`command`], [`std_command`] or
//! [`detached_command`] instead. Each repository enforces that with a source scan test built
//! on [`scan::raw_spawns`].
//!
//! Why: on Windows a console program started by a process that has no console of its own (the
//! GUI app, or a daemon started detached) gets a **new, visible console window** unless it is
//! created with `CREATE_NO_WINDOW`. Every discovery probe, `git` call, `ssh`/`wsl.exe`
//! transport and agent CLI would flash a window. With `CREATE_NO_WINDOW` the child gets a
//! console that has no window; its own children inherit that invisible console, so even
//! processes started by third-party code further down the tree (ssh's askpass, an npm
//! `.cmd` shim's `node`) stay invisible. Piped stdio works unchanged.
//!
//! Detached long-lived processes (the daemon, wasit-hostd, the wasl node) are started with
//! [`detached_command`] + [`spawn_detached`]: on Unix a new process group; on Windows a new
//! process group, `CREATE_NO_WINDOW` (an invisible console of their own, not the parent's: closing
//! the parent's console does not reach them, and everything they start inherits the invisible
//! console) and `CREATE_BREAKAWAY_FROM_JOB` when the parent's job allows it (an OpenSSH session
//! job would otherwise kill the daemon when the session ends). `DETACHED_PROCESS` is not used
//! for them: a process without any console makes every console child it starts allocate a
//! new, visible one, which is exactly the window flashing this crate prevents.

use std::ffi::OsStr;

pub mod scan;
#[cfg(windows)]
pub mod winprobe;

/// Windows process-creation flags (values from the Windows SDK; usable on every platform so
/// callers need no `cfg` to name them).
pub mod flags {
    pub const DETACHED_PROCESS: u32 = 0x0000_0008;
    pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    pub const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
    pub const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    /// What [`super::command`] / [`super::std_command`] set.
    pub const NO_WINDOW: u32 = CREATE_NO_WINDOW;
    /// What [`super::spawn_detached`] tries first.
    pub const DETACHED: u32 =
        CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB;
    /// Fallback when the parent's job forbids breakaway.
    pub const DETACHED_NO_BREAKAWAY: u32 = CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP;
}

/// A `std::process::Command` that never opens a console window on Windows.
pub fn std_command<S: AsRef<OsStr>>(program: S) -> std::process::Command {
    #[cfg_attr(not(windows), allow(unused_mut))]
    let mut cmd = std::process::Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(flags::NO_WINDOW);
    }
    cmd
}

/// A `tokio::process::Command` that never opens a console window on Windows.
#[cfg(feature = "tokio")]
pub fn command<S: AsRef<OsStr>>(program: S) -> tokio::process::Command {
    tokio::process::Command::from(std_command(program))
}

/// A command for a long-lived detached process; start it with [`spawn_detached`].
/// On Unix it runs in its own process group (callers that need a new session add it).
#[cfg(feature = "tokio")]
pub fn detached_command<S: AsRef<OsStr>>(program: S) -> tokio::process::Command {
    let mut cmd = command(program);
    cmd.kill_on_drop(false);
    #[cfg(unix)]
    cmd.process_group(0);
    #[cfg(windows)]
    cmd.creation_flags(flags::DETACHED);
    cmd
}

/// Spawn a command built by [`detached_command`]. On Windows, retries without
/// `CREATE_BREAKAWAY_FROM_JOB` when the parent's job does not allow breaking away.
#[cfg(feature = "tokio")]
pub fn spawn_detached(cmd: &mut tokio::process::Command) -> std::io::Result<tokio::process::Child> {
    #[cfg(windows)]
    {
        const ERROR_ACCESS_DENIED: i32 = 5;
        match cmd.spawn() {
            Err(e) if e.raw_os_error() == Some(ERROR_ACCESS_DENIED) => {
                cmd.creation_flags(flags::DETACHED_NO_BREAKAWAY);
                cmd.spawn()
            }
            r => r,
        }
    }
    #[cfg(not(windows))]
    cmd.spawn()
}

/// Decode what a console program wrote. Windows tools such as `wsl.exe` write UTF-16LE (with
/// or without a BOM) even to a pipe; everything else is taken as UTF-8 (lossy). The result has
/// no BOM, no NULs and `\n` line ends.
pub fn decode_console_output(bytes: &[u8]) -> String {
    let s = if let Some(off) = utf16le_offset(bytes) {
        let units: Vec<u16> = bytes[off..]
            .chunks(2)
            .map(|c| u16::from_le_bytes([c[0], *c.get(1).unwrap_or(&0)]))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    };
    s.replace("\r\n", "\n")
        .chars()
        .filter(|&c| c != '\0' && c != '\u{feff}' && c != '\r')
        .collect()
}

/// UTF-16LE: a BOM, or mostly-ASCII text whose every other byte is NUL. Returns where the
/// first whole code unit starts.
fn utf16le_offset(b: &[u8]) -> Option<usize> {
    if b.starts_with(&[0xFF, 0xFE]) {
        return Some(0);
    }
    // A UTF-16LE fragment may start on an odd byte (e.g. a line split mid-unit): look at both
    // alignments.
    let pairs = b.len() / 2;
    if pairs == 0 {
        return None;
    }
    let zeros_at = |off: usize| {
        b.iter()
            .skip(off)
            .step_by(2)
            .take(pairs)
            .filter(|&&x| x == 0)
            .count()
    };
    let (even, odd) = (zeros_at(0), zeros_at(1));
    let (hi, lo, off) = if odd >= even {
        (odd, even, 0)
    } else {
        (even, odd, 1)
    };
    (hi * 10 >= pairs * 6 && lo * 10 <= pairs).then_some(off)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf16(s: &str, bom: bool) -> Vec<u8> {
        let mut v = if bom { vec![0xFF, 0xFE] } else { vec![] };
        v.extend(s.encode_utf16().flat_map(|u| u.to_le_bytes()));
        v
    }

    #[test]
    fn decodes_wsl_utf16_output() {
        let msg = "There is no distribution with the supplied name.\r\n";
        assert_eq!(
            decode_console_output(&utf16(msg, false)),
            "There is no distribution with the supplied name.\n"
        );
        assert_eq!(
            decode_console_output(&utf16("Ubuntu-24.04\r\nDebian\r\n", true)),
            "Ubuntu-24.04\nDebian\n"
        );
        // The exact bytes of the owner's report (one line, read up to `\n`, odd byte left over).
        let mut raw = utf16("There is no distribution with the supplied name.\r", false);
        raw.push(b'\n');
        assert_eq!(
            decode_console_output(&raw),
            "There is no distribution with the supplied name.\n"
        );
        // Starting on the high byte of a unit.
        let off = &utf16("Ubuntu\r\n", false)[1..];
        assert!(decode_console_output(off).contains("buntu"));
    }

    #[test]
    fn leaves_utf8_alone() {
        assert_eq!(
            decode_console_output(b"MARSAD-BOOT Linux x86_64 none\n"),
            "MARSAD-BOOT Linux x86_64 none\n"
        );
        assert_eq!(
            decode_console_output("héllo wörld\r\n".as_bytes()),
            "héllo wörld\n"
        );
        assert_eq!(decode_console_output(b""), "");
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn piped_stdio_works_through_the_helper() {
        let (prog, args): (&str, &[&str]) = if cfg!(windows) {
            ("cmd", &["/C", "echo hi"])
        } else {
            ("sh", &["-c", "echo hi"])
        };
        let out = command(prog)
            .args(args)
            .stdout(std::process::Stdio::piped())
            .output()
            .await
            .unwrap();
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hi");
    }
}
