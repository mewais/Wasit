//! The host environment stage with a fake login shell and rc files: a CLI only on a PATH the
//! rc file adds is discovered and launched; rc chatter is ignored; a hanging rc file hits the
//! deadline and falls back. One test function: the base environment is process-wide.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::time::Duration;

use common::*;
use wasit_core::DriverId;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::environment::{CaptureOptions, HostEnv};
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

    // A hanging interactive rc: the deadline ends it (process group killed), login-only works.
    let home2 = home_with(
        d.path(),
        "h2",
        "case $- in *i*) sleep 300;; esac\nexport PATH=\"$HOME/.nvm/bin:$PATH\"\n",
    );
    let started = std::time::Instant::now();
    let env2 = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path(), &home2)),
        interactive_deadline: Duration::from_millis(800),
        login_deadline: Duration::from_secs(4),
        ..CaptureOptions::default()
    });
    let r2 = env2.refresh().await;
    assert_eq!(r2.source, EnvSource::LoginShell, "{r2:?}");
    assert!(r2.notes[0].contains("did not finish"), "{r2:?}");
    assert!(started.elapsed() < Duration::from_secs(4));
    // Nothing of the killed shell lingers.
    let ps = std::process::Command::new("pgrep")
        .args(["-f", "sleep 300"])
        .output()
        .unwrap();
    assert!(ps.stdout.is_empty(), "the hung rc's sleep survived");

    // Hanging in every mode: the daemon's own environment, with the reasons.
    let home3 = home_with(d.path(), "h3", "sleep 300\n");
    let env3 = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path(), &home3)),
        interactive_deadline: Duration::from_millis(500),
        login_deadline: Duration::from_millis(500),
        ..CaptureOptions::default()
    });
    let r3 = env3.refresh().await;
    assert_eq!(r3.source, EnvSource::Process, "{r3:?}");
    assert_eq!(r3.notes.len(), 3, "{r3:?}");
    wasit_process::set_base_env(None);
}
