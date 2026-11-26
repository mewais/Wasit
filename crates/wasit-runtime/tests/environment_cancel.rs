//! `host.environment.cancel` over the client: a setup that `host.environment.test` runs and that
//! hangs in the user's rc file is ended by the cancel call, the test call returns the report
//! saying so, and nothing of the cancelled shell survives. The test call carries no deadline of
//! its own: the client deadline below is far shorter than the setup, and the call still waits.
//! One test function: the base environment is process-wide.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use wasit_runtime::environment::{CaptureOptions, HostEnv};
use wasit_runtime::wire::{ConfigureEnvironment, HostEnvConfig};
use wasit_runtime::{Discovery, DiscoveryConfig, RuntimeService, SessionManager};

/// A "login shell" whose HOME is `home`: bash reading `home`'s startup files.
fn fake_shell(dir: &Path, home: &Path) -> std::path::PathBuf {
    let sh = dir.join("shell");
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

/// An interactive rc that starts a `sleep` and hangs, reporting its shell's and sleep's pids
/// through `fifo` (the write returns exactly when the rc has run).
fn hung_rc(fifo: &Path) -> String {
    format!(
        "case $- in *i*) sleep 300 & echo \"$$ $!\" > '{}'; wait;; esac\n",
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
async fn hung_pids(fifo: std::path::PathBuf) -> (u32, u32) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_call_ends_the_setup_that_test_environment_waits_for() {
    let d = TempDir::new();
    let fifo = d.path().join("rc.fifo");
    mkfifo(&fifo);
    let home = d.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join(".bash_profile"),
        "[ -f ~/.bashrc ] && . ~/.bashrc\n",
    )
    .unwrap();
    std::fs::write(home.join(".bashrc"), hung_rc(&fifo)).unwrap();
    let env = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path(), &home)),
        ..CaptureOptions::default()
    });
    let data = d.path().join("data");
    let cfg = config(&data, timeouts());
    let disc = Discovery::new(DiscoveryConfig::from_runtime(&cfg));
    let mgr = SessionManager::new(cfg).await.unwrap();
    let svc = RuntimeService::new(mgr, disc, "host", "test-build").with_environment(env);
    // A client deadline no setup can meet: a call that carried it would fail before the rc ran.
    let client =
        loopback(Arc::new(svc)).with_deadlines(Duration::from_millis(1), Duration::from_secs(10));

    let req = ConfigureEnvironment {
        config: HostEnvConfig::default(),
        revision: "r1".into(),
    };
    let run = {
        let client = client.clone();
        let req = req.clone();
        tokio::spawn(async move { client.test_environment(&req).await })
    };
    let (shell, sleep) = hung_pids(fifo).await;
    assert!(!run.is_finished(), "the test call ended without a cancel");

    client.cancel_environment().await.expect("cancel answered");
    let test = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("cancel ends the test call")
        .unwrap()
        .expect("the test call answered");
    assert!(
        test.environment
            .notes
            .first()
            .is_some_and(|n| n.contains("cancelled")),
        "{test:?}"
    );
    assert!(is_gone(shell), "the cancelled login shell {shell} survived");
    assert!(is_gone(sleep), "the cancelled rc's sleep {sleep} survived");
    wasit_process::set_base_env(None);
}
