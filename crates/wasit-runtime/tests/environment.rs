//! The host environment stage with a fake login shell and rc files: a CLI only on a PATH the
//! rc file adds is discovered and launched; rc chatter is ignored; a hanging rc file is waited
//! for until the user cancels. One test function: the base environment is process-wide.
#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::*;
use wasit_core::DriverId;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::environment::{CaptureOptions, EnvStage, HostEnv};
use wasit_runtime::wire::{EnvSource, InventoryReply};
use wasit_runtime::{Discovery, DiscoveryConfig, SessionManager};

/// A "login shell" whose HOME is `home`: bash reading `home`'s startup files.
fn fake_shell(dir: &Path, home: &Path) -> std::path::PathBuf {
    let sh = dir.join(format!(
        "shell-{}",
        home.file_name().unwrap().to_string_lossy()
    ));
    std::fs::write(
        &sh,
        format!(
            "#!/bin/sh\nHOME='{}'; export HOME\nexec /bin/bash \"$@\"\n",
            home.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();
    sh
}

fn home_with(dir: &Path, name: &str, bashrc: &str) -> std::path::PathBuf {
    let home = dir.join(name);
    std::fs::create_dir_all(home.join(".nvm/bin")).unwrap();
    std::fs::write(
        home.join(".bash_profile"),
        "echo profile-noise\n[ -f ~/.bashrc ] && . ~/.bashrc\n",
    )
    .unwrap();
    std::fs::write(home.join(".bashrc"), bashrc).unwrap();
    home
}

/// An interactive rc that starts a `sleep` and hangs. It reports its own pid and the sleep's
/// through `fifo`: the write returns exactly when the rc has run (no polling needed).
fn hung_rc(fifo: &Path) -> String {
    format!(
        "case $- in *i*) sleep 300 & echo \"$$ $!\" > '{}'; wait;; esac\nexport PATH=\"$HOME/.nvm/bin:$PATH\"\n",
        fifo.display()
    )
}

fn mkfifo(path: &Path) {
    let st = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .unwrap();
    assert!(st.success(), "mkfifo {}", path.display());
}

/// The (shell, sleep) pids a [`hung_rc`] reported.
async fn hung_pids(fifo: PathBuf) -> (u32, u32) {
    let text = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || std::fs::read_to_string(&fifo)),
    )
    .await
    .expect("the hung rc ran")
    .unwrap()
    .unwrap();
    let mut pids = text.split_whitespace().map(|p| p.parse::<u32>().unwrap());
    (pids.next().unwrap(), pids.next().unwrap())
}

/// Whether `pid` is no longer running (gone, or a zombie that has already died). Only ever asked
/// about pids this test's own rc reported.
fn is_gone(pid: u32) -> bool {
    let out = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let stat = String::from_utf8_lossy(&out.stdout).trim().to_string();
    stat.is_empty() || stat.starts_with('Z')
}

async fn settled(disc: &Discovery) -> InventoryReply {
    let mut changes = disc.changes();
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let inv = disc.inventory(None);
            if !inv.drivers.is_empty() && inv.drivers.iter().all(|d| !d.refreshing) {
                return inv;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .expect("discovery settled")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_shell_environment_reaches_discovery_and_sessions() {
    let d = TempDir::new();
    // Interactive-only PATH addition (like nvm in .bashrc), plus chatter on both streams.
    let home = home_with(
        d.path(),
        "h1",
        "echo rc-noise; echo rc-err >&2\ncase $- in *i*) export PATH=\"$HOME/.nvm/bin:$PATH\";; esac\n",
    );
    std::os::unix::fs::symlink(fake(), home.join(".nvm/bin/claude")).unwrap();
    let env = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path(), &home)),
        ..CaptureOptions::default()
    });
    let r = env.refresh().await;
    assert_eq!(r.source, EnvSource::LoginInteractiveShell, "{r:?}");
    let nvm = home.join(".nvm/bin").display().to_string();
    assert_eq!(r.path.first(), Some(&nvm), "{r:?}");
    assert!(r.notes.is_empty(), "{r:?}");
    let base = wasit_process::base_env().unwrap();
    assert!(!base.iter().any(|(k, _)| k == "SHLVL" || k == "PS1"));

    // Discovery: `claude` (no configured path) is found on the captured PATH only.
    let data = d.path().join("data");
    let mut cfg = config(&data, timeouts());
    let claude = DriverId::new("claude");
    cfg.overrides.get_mut(&claude).unwrap().binary = None;
    let disc = Discovery::new(DiscoveryConfig::from_runtime(&cfg));
    disc.refresh(Some(&claude), None).unwrap();
    let inv = settled(&disc).await;
    let c = inv.drivers.iter().find(|x| x.driver == claude).unwrap();
    assert!(c.installed, "{:?}", c.error);
    assert_eq!(c.binary_path.as_deref(), Some(&*format!("{nvm}/claude")));

    // A session launches it (the shim and the CLI inherit the base environment).
    let mgr = SessionManager::new(cfg).await.unwrap();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("from the nvm dir"))
        .build();
    let mut req = create(&data, "claude", &sc);
    req.first_prompt = Some("hi".into());
    let id = mgr.create(req).await.unwrap().id;
    let s = wait_snapshot(&mgr, &id, "one turn", |s| s.turns_completed >= 1).await;
    assert_eq!(s.turns_completed, 1);
    mgr.delete(&id).await.unwrap();

    // A hanging interactive rc: no deadline ends the capture (a slow login is waited for). The
    // user's cancel does: the process group is killed and the report says the capture was
    // cancelled, instead of quietly falling back to a bare environment.
    let home2 = home_with(d.path(), "h2", &hung_rc(&d.path().join("h2.fifo")));
    mkfifo(&d.path().join("h2.fifo"));
    let env2 = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path(), &home2)),
        ..CaptureOptions::default()
    });
    let mut stage = env2.progress();
    let runner = env2.clone();
    let run = tokio::spawn(async move { runner.refresh().await });
    tokio::time::timeout(
        Duration::from_secs(20),
        stage.wait_for(|s| *s == EnvStage::Capturing),
    )
    .await
    .expect("capture reported")
    .expect("stage open");
    let (shell2, sleep2) = hung_pids(d.path().join("h2.fifo")).await;
    // Longer than the old login deadline: nothing ends the capture on its own.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(!run.is_finished(), "the capture ended without a cancel");
    let started = std::time::Instant::now();
    env2.cancel_capture();
    let r2 = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("cancel ends the capture")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "{r2:?}");
    assert_eq!(r2.source, EnvSource::Process, "{r2:?}");
    assert!(r2.notes[0].contains("cancelled"), "{r2:?}");
    assert_eq!(*stage.borrow(), EnvStage::Idle);
    // Nothing of the killed shell lingers: its own shell and the sleep it started.
    assert!(
        is_gone(shell2),
        "the cancelled login shell {shell2} survived"
    );
    assert!(
        is_gone(sleep2),
        "the cancelled rc's sleep {sleep2} survived"
    );

    // Dropping the capture (its caller went away) ends the shell's whole group the same way,
    // not only the shell itself.
    let home3 = home_with(d.path(), "h3", &hung_rc(&d.path().join("h3.fifo")));
    mkfifo(&d.path().join("h3.fifo"));
    let env3 = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path(), &home3)),
        ..CaptureOptions::default()
    });
    let runner3 = env3.clone();
    let run3 = tokio::spawn(async move { runner3.refresh().await });
    let (shell3, sleep3) = hung_pids(d.path().join("h3.fifo")).await;
    run3.abort();
    // Awaiting an aborted task returns once its future has been dropped.
    assert!(run3.await.unwrap_err().is_cancelled());
    assert!(is_gone(shell3), "the dropped login shell {shell3} survived");
    assert!(is_gone(sleep3), "the dropped rc's sleep {sleep3} survived");
    wasit_process::set_base_env(None);
}
