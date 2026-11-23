//! The guard of the test hygiene: a test that fails leaves nothing behind. The processes a
//! daemon test starts are detached on purpose (a session's shim, its guard and the CLI survive
//! their daemon; a bridge's daemon survives the bridge), so the harness ends them itself
//! (`TempDir`'s drop, which runs while a failing test unwinds). This test runs a scenario with
//! the real processes of both kinds — a daemon and its session, and a daemon started by a
//! bridge and its session, under data directories of their own — and panics in the middle; then
//! it asserts that every process the scenario started has ended and its directory is gone.
//! It checks only the processes it started itself, named by the processes' own reports.
#![cfg(target_os = "linux")]

mod common;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use common::*;
use wasit_control::{ClientOptions, ControlClient, Hello};
use wasit_fake_cli::harness::PidWatch;
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::HostInfo;

/// What the failing scenario started, noted as it goes.
#[derive(Default)]
struct Started {
    root: PathBuf,
    /// A handle on each process, opened while it was alive.
    watches: Vec<(String, PidWatch)>,
}

impl Started {
    fn note(&mut self, what: impl Into<String>, pid: u32) {
        let what = what.into();
        let watch = PidWatch::open(pid).unwrap_or_else(|| panic!("{what} ({pid}) is not alive"));
        self.watches.push((what, watch));
    }
}

/// The parent of `pid` (the guard is the parent of the CLI).
fn parent_of(pid: u32) -> u32 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    // `pid (comm) state ppid …`; the name may hold spaces and parentheses.
    let after = &stat[stat.rfind(')').unwrap() + 2..];
    after.split(' ').nth(1).unwrap().parse().unwrap()
}

/// A session on the daemon of `data`, idle, and the processes it started noted: the daemon
/// itself, the session's shim, its guard and its CLI.
async fn session_on(started: &Mutex<Started>, label: &str, root: &Path, data: &Path) {
    let c = client(data);
    let daemon = c.host_info().await.unwrap().pid;
    started
        .lock()
        .unwrap()
        .note(format!("{label} daemon"), daemon);
    let id = c
        .create(create(root, "claude", &Scenario::default()))
        .await
        .unwrap()
        .id;
    let mut sub = c.subscribe_session(&id, None);
    loop {
        let e = tokio::time::timeout(T, sub.next())
            .await
            .expect("an event")
            .expect("the stream")
            .expect("an event");
        if is_phase(&e.1, "idle") {
            break;
        }
    }
    let shim = wasit_shim::ShimClient::connect(&data.join("sessions").join(id.as_str()))
        .await
        .unwrap();
    let (shim_pid, cli) = (shim.shim_pid(), shim.child_pid());
    let mut s = started.lock().unwrap();
    s.note(format!("{label} shim"), shim_pid);
    s.note(format!("{label} guard"), parent_of(cli));
    s.note(format!("{label} CLI"), cli);
}

async fn failing_scenario(started: Arc<Mutex<Started>>) {
    let d = TempDir::new();
    started.lock().unwrap().root = d.path().to_owned();

    // A daemon that is a child of the test, with a session.
    let data = d.path().join("d");
    let _daemon = start_daemon(&data).await;
    session_on(&started, "direct", d.path(), &data).await;

    // A daemon that a bridge starts, detached from it, with a session.
    let data = d.path().join("b");
    let mut bridge = tokio::process::Command::new(hostd())
        .arg("bridge")
        .args(daemon_args(&data))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let io =
        wasit_control::join_duplex(bridge.stdout.take().unwrap(), bridge.stdin.take().unwrap());
    let link = ControlClient::connect(io, ClientOptions::new(Hello::new("t", "hub", "hub")))
        .await
        .unwrap();
    let _: HostInfo = link
        .request("host.info", (), std::time::Duration::from_secs(10))
        .await
        .unwrap();
    session_on(&started, "bridged", d.path(), &data).await;

    panic!("the test fails in the middle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_test_leaves_no_process_and_no_directory_behind() {
    let started = Arc::new(Mutex::new(Started::default()));
    // On a task of its own, so that its panic unwinds its locals (the harness's drop runs)
    // while this test goes on.
    let outcome = tokio::spawn(failing_scenario(started.clone())).await;
    assert!(outcome.unwrap_err().is_panic(), "the scenario must panic");

    let started = started.lock().unwrap();
    assert_eq!(started.watches.len(), 8, "the scenario started 8 processes");
    for (what, watch) in &started.watches {
        assert!(
            watch.is_gone(),
            "the {what} is still running when its test has failed"
        );
    }
    assert!(
        !started.root.exists(),
        "{} was not removed",
        started.root.display()
    );
}
