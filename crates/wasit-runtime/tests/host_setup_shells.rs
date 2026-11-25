//! Capture and setup commands with real fish, tcsh, csh (bsd-csh) and zsh login shells
//! (installed by CI; a missing shell fails the test, it is never skipped silently; without root,
//! unpack the packages and name the directory of binaries in `WASIT_TEST_SHELL_DIR`). Each shell
//! gets its own HOME with startup files: fish and zsh define a `module` function in interactive
//! shells only (the setup runs in that shell, so it exists); tcsh/csh define a `module` alias in
//! `.tcshrc`/`.cshrc` and variables and another alias in `.login` (the setup is csh and runs in
//! that shell, from a private file). Per shell: success with non-ASCII output, a failing last
//! command, a syntax error, `exit`; for csh also: control structures, runtime errors (tcsh and
//! csh differ), the file's mode and removal, secrets, the deadline. One test function: the base
//! environment is process-wide.
#![cfg(target_os = "linux")]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::*;
use wasit_runtime::environment::{capture, CaptureOptions, EnvStage, HostEnv};
use wasit_runtime::wire::{EnvSource, HostEnvConfig, SetupReport};

fn exe(p: &Path, text: &str) {
    std::fs::write(p, text).unwrap();
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// `<dir>/<name>/bin/<name>`: runs the real shell (the first of `real_names` that is installed)
/// with HOME = `<dir>/<name>/home` and no `XDG_CONFIG_HOME`/`ZDOTDIR` (CI runners set
/// XDG_CONFIG_HOME, where fish would look instead); the file name is what the environment stage
/// recognises the shell by. Returns (shell, home).
fn shell_of(dir: &Path, name: &str, real_names: &[&str]) -> (PathBuf, PathBuf) {
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
            real_shell(real_names).display()
        ),
    );
    (s, home)
}

fn shell(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
    shell_of(dir, name, &[name])
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

/// The body is one long future (a debug build's frames for it do not fit the 2 MB of a test
/// thread), so it runs on a thread with a larger stack.
#[test]
fn fish_tcsh_csh_and_zsh_login_shells() {
    let t = std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(all_shells())
        })
        .unwrap();
    if let Err(e) = t.join() {
        std::panic::resume_unwind(e);
    }
}

async fn all_shells() {
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

    // tcsh, csh and bsd-csh (the Debian name of the real csh): `-l` cannot be combined with
    // `-c`; rc and login files are captured and the setup is csh, run by that shell.
    Box::pin(csh_family(d.path(), "tcsh", &["tcsh"], true)).await;
    Box::pin(csh_family(d.path(), "csh", &["bsd-csh"], false)).await;
    Box::pin(csh_family(d.path(), "bsd-csh", &["bsd-csh"], false)).await;

    // A login shell that is not POSIX, csh or fish (here a wrapper named like xonsh around bash,
    // which takes the same command line): captured like a POSIX shell, setup run by /bin/sh.
    let (xonsh, _) = shell_of(d.path(), "xonsh", &["bash"]);
    let env = HostEnv::new(CaptureOptions {
        shell: Some(xonsh),
        ..CaptureOptions::default()
    });
    env.refresh().await;
    let s = setup(&env, "FROM_SETUP='a b'; export FROM_SETUP", "foreign").await;
    assert!(s.ok && s.applied, "{s:?}");
    assert!(s.shell.starts_with("/bin/sh"), "{s:?}");
    assert_eq!(var("FROM_SETUP").as_deref(), Some("a b"));
}

const SECRET: &str = "s3cret-c5h-4d2a";

/// The lines every csh-family case below runs; `modcmd` is a POSIX script, as Environment
/// Modules' csh `module` alias is (`eval "`modulecmd csh ...`"`).
fn csh_home(home: &Path, rc: &str, name: &str, md: &Path) {
    let md = md.display();
    let modcmd = format!(
        "#!/bin/sh\nif [ \"$1\" = load ] && [ -d '{md}'/\"$2\" ]; then\n  \
         echo \"setenv PATH '{md}'/$2/bin:\\$PATH; echo loaded $2\"\nelse\n  \
         echo \"sh -c 'echo module: $2 not found >&2; exit 1'\"\nfi\n"
    );
    exe(&Path::new(&format!("{md}")).join("modcmd"), &modcmd);
    std::fs::write(
        home.join(rc),
        format!(
            "setenv RC_VAR {name}-rc\nset path = ( '{md}/rc-bin' $path )\n\
             alias module 'eval \"`{md}/modcmd \\!*`\"'\n"
        ),
    )
    .unwrap();
    std::fs::write(
        home.join(".login"),
        format!(
            "echo {name}-login-noise\nsetenv LOGIN_VAR {name}-login\nsetenv SITE_TOKEN {SECRET}\n\
             alias fromlogin 'echo login-alias-works'\n"
        ),
    )
    .unwrap();
}

fn files_in(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.starts_with("setup-"))
                .collect()
        })
        .unwrap_or_default()
}

/// One csh-family shell: `wrapper` is the file name the shell is recognised by, `tcsh` whether the
/// real binary is tcsh (csh's behaviour differs: no pre-check, a runtime error ends the shell).
async fn csh_family(dir: &Path, wrapper: &str, real_names: &[&str], tcsh: bool) {
    let name = wrapper;
    let m = modules(dir);
    let md = m.display();
    let (sh, home) = shell_of(dir, wrapper, real_names);
    let rc = if tcsh { ".tcshrc" } else { ".cshrc" };
    csh_home(&home, rc, name, &m);
    // bsd-csh uses the default private directory (the machine's runtime directory for `wasit`),
    // the others one of their own.
    let default_dir = wrapper == "bsd-csh";
    let scratch = if default_dir {
        wasit_runtime::machine::runtime_dir("wasit", None).unwrap()
    } else {
        dir.join(format!("{name}-run"))
    };
    let opts = CaptureOptions {
        shell: Some(sh.clone()),
        scratch_dir: (!default_dir).then(|| scratch.clone()),
        ..CaptureOptions::default()
    };
    let c = capture(&opts).await;
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

    let env = HostEnv::new(opts.clone());
    env.refresh().await;
    let tools = format!("{md}/tools/bin");
    let gone = |what: &str| {
        assert!(
            files_in(&scratch).is_empty(),
            "{name}: the setup file is left after {what}: {:?}",
            files_in(&scratch)
        )
    };

    // Success: the rc's alias (`module`), the login file's alias and variables, `setenv`, `if`,
    // `foreach`, non-ASCII output, the file private while it runs; the environment is taken.
    let ok = "module load tools\nsetenv FROM_SETUP \"a b\"\nfromlogin\n\
              if ( \"$RC_VAR\" == \"RCRC\" && \"$LOGIN_VAR\" == \"LOGINLOGIN\" ) then\n  echo 'héllo ✓'\nelse\n  \
              echo wrong-startup\nendif\n\
              set acc = ''\nforeach d ( one two )\n  set acc = \"$acc$d\"\nend\n\
              setenv FROM_LOOP $acc\n\
              set f = \"$__WASIT_SETUP_FILE\"\n\
              echo mode=`stat -c %a \"$f\"` dir=`stat -c %a \"$f:h\"`"
        .replace("RCRC", &format!("{name}-rc"))
    .replace("LOGINLOGIN", &format!("{name}-login"));
    let s = setup(&env, &ok, &format!("{name}-ok")).await;
    assert!(s.ok && s.applied, "{name}: {s:?}");
    assert_eq!(s.exit_code, Some(0), "{name}: {s:?}");
    assert_eq!(s.shell, sh.display().to_string(), "{name}: {s:?}");
    assert!(s.error.is_none(), "{name}: {s:?}");
    for want in [
        "loaded tools",
        "login-alias-works",
        "héllo ✓",
        "mode=600 dir=700",
    ] {
        assert!(s.output.contains(want), "{name}: {want}: {s:?}");
    }
    assert!(!s.output.contains("noise"), "{name}: {s:?}");
    assert_eq!(var("FROM_SETUP").as_deref(), Some("a b"), "{name}");
    assert_eq!(var("FROM_LOOP").as_deref(), Some("onetwo"), "{name}");
    assert_eq!(first_path(), tools, "{name}");
    for internal in ["__WASIT_SETUP_FILE", "__WASIT_SETUP_DONE", "__WASIT_SETUP"] {
        assert_eq!(var(internal), None, "{name}: {internal} leaked");
    }
    gone("success");
    // The environment reaches a process started through the runtime's process helper.
    let out = wasit_process::command("/bin/sh")
        .args(["-c", "echo \"$FROM_SETUP|$FROM_LOOP|$RC_VAR\""])
        .output()
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("a b|onetwo|{name}-rc"),
        "{name}"
    );

    // A failing last command: reported with its status; its environment is taken.
    let s = setup(
        &env,
        "setenv AFTER_FAIL 1\nmodule load nope",
        &format!("{name}-fail"),
    )
    .await;
    assert!(!s.ok && s.applied, "{name}: {s:?}");
    assert_eq!(s.exit_code, Some(1), "{name}: {s:?}");
    let e = s.error.as_deref().unwrap_or_default();
    assert!(
        e.starts_with("the last command exited with status 1"),
        "{name}: {s:?}"
    );
    assert!(e.contains("cannot check commands without running"), "{s:?}");
    assert!(s.output.contains("module: nope not found"), "{s:?}");
    assert_eq!(var("AFTER_FAIL").as_deref(), Some("1"), "{name}");
    assert_eq!(
        var("FROM_SETUP"),
        None,
        "{name}: the previous setup is gone"
    );
    gone("a failing command");

    // A runtime error in the middle (tcsh stops the file and goes on; csh ends the shell): either
    // way the commands did not finish, nothing after it ran and the login environment is kept.
    let s = setup(
        &env,
        "setenv HALF 1\necho $nosuchvariable\nsetenv AFTER_ERROR 1",
        &format!("{name}-undefined"),
    )
    .await;
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert_eq!(s.exit_code, Some(1), "{name}: {s:?}");
    let e = s.error.as_deref().unwrap_or_default();
    let stopped = if tcsh {
        "stopped before their end"
    } else {
        "ended the shell"
    };
    assert!(e.contains(stopped), "{name}: {s:?}");
    assert!(e.contains("cannot check commands without running"), "{s:?}");
    assert!(s.output.contains("Undefined variable"), "{name}: {s:?}");
    assert_eq!(var("HALF"), None, "{name}: the login environment is kept");
    assert_eq!(var("AFTER_ERROR"), None, "{name}");
    assert_eq!(var("RC_VAR").as_deref(), Some(&*format!("{name}-rc")));
    gone("a runtime error");

    // `exit` returns from the file: the commands did not finish.
    let s = setup(
        &env,
        "setenv HALF 1\necho bye\nexit 3\nsetenv AFTER_EXIT 1",
        &format!("{name}-exit"),
    )
    .await;
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert_eq!(s.exit_code, Some(3), "{name}: {s:?}");
    assert!(
        s.error
            .as_deref()
            .unwrap_or_default()
            .contains("stopped before their end"),
        "{name}: {s:?}"
    );
    assert_eq!(var("HALF"), None, "{name}");
    assert_eq!(var("AFTER_EXIT"), None, "{name}");
    gone("exit");

    // A block that is never closed, whose condition is false, swallows the rest of the file: it
    // does not look finished. (With a true condition csh/tcsh run the block's lines as they come
    // and end quietly at the end of the file: they report no error and neither do we.)
    let s = setup(
        &env,
        "echo opened\nif ( 0 ) then\n  setenv UNCLOSED 1",
        &format!("{name}-open"),
    )
    .await;
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert!(
        s.error.as_deref().unwrap_or_default().contains(stopped),
        "{name}: {s:?}"
    );
    assert_eq!(var("UNCLOSED"), None, "{name}");
    gone("an open block");

    // A syntax error. tcsh reads the file first (`-n`, lexical errors only): nothing runs. csh
    // has no such check, so what comes before the error runs, and the report says so.
    let marker = dir.join(format!("{name}-ran"));
    let s = setup(
        &env,
        &format!("touch '{}'\necho \"unterminated", marker.display()),
        &format!("{name}-syntax"),
    )
    .await;
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert!(!s.output.is_empty(), "{name}: the shell's message: {s:?}");
    let e = s.error.as_deref().unwrap_or_default();
    if tcsh {
        assert!(e.contains("nothing was run"), "{name}: {s:?}");
        assert!(s.shell.ends_with(" -n"), "{name}: {s:?}");
        assert!(!marker.exists(), "{name}: a line ran despite the check");
    } else {
        assert!(!e.contains("nothing was run"), "{name}: {s:?}");
        assert!(e.contains("cannot check commands without running"), "{s:?}");
        assert!(marker.exists(), "{name}: the line before the error ran");
    }
    assert_eq!(var("FROM_SETUP"), None, "{name}: the login environment");
    gone("a syntax error");

    // A secret the setup prints is scrubbed from the report; it is in the process environment
    // (set by the variables, after the setup).
    let mut config = cfg("echo \"token $SITE_TOKEN\"\nsetenv FROM_SETUP 1");
    config.vars = vec![wasit_runtime::wire::EnvVar {
        name: "API_TOKEN".into(),
        value: SECRET.into(),
        secret: true,
    }];
    let (r, _) = env.configure(config, format!("{name}-secret"), false).await;
    let s = r.setup.clone().expect("a setup report");
    assert!(s.ok && s.applied, "{name}: {s:?}");
    assert!(s.output.contains("token ••••"), "{name}: {s:?}");
    assert!(!format!("{r:?}").contains(SECRET), "{name}: {r:?}");
    assert_eq!(var("API_TOKEN").as_deref(), Some(SECRET), "{name}");
    assert_eq!(var("FROM_SETUP").as_deref(), Some("1"), "{name}");
    gone("the secret setup");

    // A hanging setup ends by the user's cancel: the report says so, the login environment is
    // kept and the setup file is gone. (The cancel comes once the setup stage has begun.)
    let quick = HostEnv::new(opts);
    quick.refresh().await;
    let mut stage = quick.progress();
    let runner = quick.clone();
    let hang_rev = format!("{name}-hang");
    let run = tokio::spawn(async move {
        runner
            .configure(cfg("setenv HUNG 1\nsleep 60"), hang_rev, false)
            .await
    });
    stage
        .wait_for(|s| *s == EnvStage::Setup)
        .await
        .expect("stage open");
    quick.cancel_capture();
    let (r, _) = run.await.unwrap();
    let s = r.setup.expect("a setup report");
    assert!(!s.ok && !s.applied, "{name}: {s:?}");
    assert!(
        s.error
            .as_deref()
            .unwrap_or_default()
            .contains("was cancelled by the user"),
        "{name}: {s:?}"
    );
    assert_eq!(var("HUNG"), None, "{name}");
    gone("the deadline");
}
