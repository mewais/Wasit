//! Capture and setup commands with real fish, tcsh, csh and zsh login shells (installed by CI;
//! a missing shell fails the test, it is never skipped silently). Each shell gets its own HOME
//! with startup files: fish and zsh define a `module` function in interactive shells only (the
//! setup runs in that shell, so it exists); tcsh/csh set variables in `.tcshrc`/`.cshrc` and
//! `.login` (both captured; setup lines are POSIX and run with `/bin/sh`). Per shell: success
//! with non-ASCII output, a failing last command, a syntax error (nothing runs), `exit`.
//! One test function: the base environment is process-wide.
#![cfg(target_os = "linux")]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::*;
use wasit_runtime::environment::{capture, CaptureOptions, HostEnv};
use wasit_runtime::wire::{EnvSource, HostEnvConfig, SetupReport};

fn exe(p: &Path, text: &str) {
    std::fs::write(p, text).unwrap();
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn real(name: &str) -> PathBuf {
    ["/usr/bin", "/bin"]
        .iter()
        .map(|d| Path::new(d).join(name))
        .find(|p| p.exists())
        .unwrap_or_else(|| {
            panic!("{name} is not installed (CI installs it: apt-get install {name})")
        })
}

/// `<dir>/<name>/bin/<name>`: runs the real shell with HOME = `<dir>/<name>/home` and no
/// `XDG_CONFIG_HOME`/`ZDOTDIR` (CI runners set XDG_CONFIG_HOME, where fish would look instead);
/// the file name is what the environment stage recognises the shell by. Returns (shell, home).
fn shell(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
    let home = dir.join(name).join("home");
    let bin = dir.join(name).join("bin");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&bin).unwrap();
    let s = bin.join(name);
    exe(
        &s,
        &format!(
            "#!/bin/sh\nHOME='{}'; export HOME\nunset XDG_CONFIG_HOME ZDOTDIR\nexec '{}' \"$@\"\n",
            home.display(),
            real(name).display()
        ),
    );
    (s, home)
}

fn modules(dir: &Path) -> PathBuf {
    let m = dir.join("modules");
    std::fs::create_dir_all(m.join("tools/bin")).unwrap();
    m
}

fn cfg(setup: &str) -> HostEnvConfig {
    HostEnvConfig {
        setup: setup.into(),
        ..HostEnvConfig::default()
    }
}

fn var(name: &str) -> Option<String> {
    wasit_process::child_var(name).map(|v| v.to_string_lossy().into_owned())
}

fn first_path() -> String {
    var("PATH")
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string()
}

async fn setup(env: &HostEnv, lines: &str, rev: &str) -> SetupReport {
    let (r, _) = env.configure(cfg(lines), rev.into(), false).await;
    r.setup.expect("a setup report")
}

/// The cases every shell goes through, with lines in the language its setup runs in.
struct Lines {
    /// Puts `<modules>/tools/bin` first on PATH, sets FROM_SETUP="a b", prints `héllo ✓`.
    ok: String,
    failing: &'static str,
    /// Creates `marker` and then has a syntax error.
    syntax: String,
    exit: &'static str,
}

async fn exercise(name: &str, sh: &Path, lines: Lines, setup_shell: &str, dir: &Path) {
    let env = HostEnv::new(CaptureOptions {
        shell: Some(sh.to_path_buf()),
        ..CaptureOptions::default()
    });
    env.refresh().await;
    let tools = dir.join("modules/tools/bin").display().to_string();

    let s = setup(&env, &lines.ok, &format!("{name}-ok")).await;
    assert!(s.ok && s.applied, "{name}: {s:?}");
    assert!(s.shell.starts_with(setup_shell), "{name}: {s:?}");
    assert!(s.output.contains("héllo ✓"), "{name}: {s:?}");
    assert_eq!(var("FROM_SETUP").as_deref(), Some("a b"), "{name}");
    assert_eq!(first_path(), tools, "{name}");

    let s = setup(&env, lines.failing, &format!("{name}-fail")).await;
    assert!(!s.ok && s.applied, "{name}: {s:?}");
    assert_eq!(s.exit_code, Some(1), "{name}: {s:?}");
    assert!(
        s.error.as_deref() == Some("the last command exited with status 1"),
        "{name}: {s:?}"
    );

    let marker = dir.join(format!("{name}-ran"));
    let s = setup(&env, &lines.syntax, &format!("{name}-syntax")).await;
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert!(
        s.error
            .as_deref()
            .unwrap_or_default()
            .contains("syntax error"),
        "{name}: {s:?}"
    );
    assert!(!s.output.is_empty(), "{name}: the shell's message: {s:?}");
    assert!(
        !marker.exists(),
        "{name}: a line ran despite the syntax error"
    );
    assert_eq!(
        var("FROM_SETUP"),
        None,
        "{name}: the login environment is kept"
    );

    let s = setup(&env, lines.exit, &format!("{name}-exit")).await;
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert!(
        s.error
            .as_deref()
            .unwrap_or_default()
            .contains("ended the shell"),
        "{name}: {s:?}"
    );
    assert_eq!(s.exit_code, Some(3), "{name}: {s:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fish_tcsh_csh_and_zsh_login_shells() {
    let d = TempDir::new();
    let m = modules(d.path());
    let md = m.display();

    // fish: `module` only in interactive shells; setup lines are fish.
    let (fish, home) = shell(d.path(), "fish");
    std::fs::create_dir_all(home.join(".config/fish")).unwrap();
    std::fs::write(
        home.join(".config/fish/config.fish"),
        format!(
            "echo fish-noise\nset -gx RC_VAR fish-rc\n\
             if status is-interactive\n  function module\n    \
             if test \"$argv[1]\" = load -a -d '{md}'/$argv[2]\n      \
             set -gx PATH '{md}'/$argv[2]/bin $PATH; echo \"loaded $argv[2]\"\n    \
             else\n      echo \"module: $argv[2] not found\" >&2; return 1\n    end\n  end\nend\n"
        ),
    )
    .unwrap();
    let c = capture(&CaptureOptions {
        shell: Some(fish.clone()),
        ..CaptureOptions::default()
    })
    .await;
    assert_eq!(
        c.report.source,
        EnvSource::LoginInteractiveShell,
        "{:?}",
        c.report
    );
    assert!(c.vars.iter().any(|(k, v)| k == "RC_VAR" && v == "fish-rc"));
    let marker = d.path().join("fish-ran");
    exercise(
        "fish",
        &fish,
        Lines {
            ok: "module load tools\nset -gx FROM_SETUP 'a b'\necho 'héllo ✓'".into(),
            failing: "set -gx X 1\nfalse",
            syntax: format!("touch '{}'\nif true\n  echo never", marker.display()),
            exit: "echo bye\nexit 3",
        },
        &fish.display().to_string(),
        d.path(),
    )
    .await;

    // zsh: `.zprofile` (login) and `.zshrc` (interactive: `module`).
    let (zsh, home) = shell(d.path(), "zsh");
    std::fs::write(home.join(".zprofile"), "export LOGIN_VAR=zsh-login\n").unwrap();
    std::fs::write(
        home.join(".zshrc"),
        format!(
            "echo zsh-noise\nmodule() {{\n  if [ \"$1\" = load ] && [ -d '{md}'/\"$2\" ]; then\n    \
             PATH='{md}'/\"$2\"/bin:$PATH; export PATH; echo \"loaded $2\"\n  else\n    \
             echo \"module: $2 not found\" >&2; return 1\n  fi\n}}\n"
        ),
    )
    .unwrap();
    let c = capture(&CaptureOptions {
        shell: Some(zsh.clone()),
        ..CaptureOptions::default()
    })
    .await;
    assert_eq!(
        c.report.source,
        EnvSource::LoginInteractiveShell,
        "{:?}",
        c.report
    );
    assert!(c
        .vars
        .iter()
        .any(|(k, v)| k == "LOGIN_VAR" && v == "zsh-login"));
    let marker = d.path().join("zsh-ran");
    exercise(
        "zsh",
        &zsh,
        Lines {
            ok: "module load tools\nexport FROM_SETUP='a b'\necho 'héllo ✓'".into(),
            failing: "export X=1\nfalse",
            syntax: format!("touch '{}'\nif true; then\n  echo never", marker.display()),
            exit: "echo bye\nexit 3",
        },
        &zsh.display().to_string(),
        d.path(),
    )
    .await;

    // tcsh and csh: `-l` cannot be combined with `-c`; both rc and login files are captured.
    // Setup lines are POSIX (run with /bin/sh in the captured environment).
    for name in ["tcsh", "csh"] {
        let (sh, home) = shell(d.path(), name);
        let rc = if name == "tcsh" { ".tcshrc" } else { ".cshrc" };
        std::fs::write(
            home.join(rc),
            format!("setenv RC_VAR {name}-rc\nset path = ( '{md}/rc-bin' $path )\n"),
        )
        .unwrap();
        std::fs::write(
            home.join(".login"),
            format!("echo {name}-login-noise\nsetenv LOGIN_VAR {name}-login\n"),
        )
        .unwrap();
        let c = capture(&CaptureOptions {
            shell: Some(sh.clone()),
            ..CaptureOptions::default()
        })
        .await;
        assert_eq!(
            c.report.source,
            EnvSource::LoginShell,
            "{name}: {:?}",
            c.report
        );
        let get = |k: &str| c.vars.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
        assert_eq!(get("RC_VAR"), Some(format!("{name}-rc")), "{name}");
        assert_eq!(get("LOGIN_VAR"), Some(format!("{name}-login")), "{name}");
        assert!(
            get("PATH")
                .unwrap_or_default()
                .starts_with(&format!("{md}/rc-bin:")),
            "{name}: {:?}",
            get("PATH")
        );
        let marker = d.path().join(format!("{name}-ran"));
        exercise(
            name,
            &sh,
            Lines {
                ok: format!(
                    "PATH='{md}/tools/bin':$PATH; export PATH\nexport FROM_SETUP='a b'\n\
                     test \"$RC_VAR:$LOGIN_VAR\" = '{name}-rc:{name}-login' && echo 'héllo ✓'"
                ),
                failing: "export X=1\nfalse",
                syntax: format!("touch '{}'\nif true; then\n  echo never", marker.display()),
                exit: "echo bye\nexit 3",
            },
            "/bin/sh",
            d.path(),
        )
        .await;
    }
}
