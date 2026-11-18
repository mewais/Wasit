//! Windows: nothing the runtime starts opens a visible console window, even when the daemon
//! itself has no console (as when a GUI app starts it). The fake CLI reports, from inside each
//! process, whether it got a visible console window (`WASIT_FAKE_CONSOLE_REPORT`).
#![cfg(windows)]

mod common;

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use common::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use wasit_control::Hello;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::RuntimeClient;

fn report_lines(p: &Path) -> Vec<String> {
    std::fs::read_to_string(p)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

async fn wait_for(p: &Path, what: &str, pred: impl Fn(&[String]) -> bool) -> Vec<String> {
    let r = tokio::time::timeout(T, async {
        loop {
            let l = report_lines(p);
            if pred(&l) {
                return l;
            }
            // Test-only polling of a file written by other processes.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    r.unwrap_or_else(|_| panic!("no report for {what}: {:?}", report_lines(p)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn probes_and_sessions_open_no_console_window_from_a_consoleless_daemon() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let report = d.path().join("console.txt");

    // The daemon gets NO console at all (DETACHED_PROCESS), like one started from the GUI.
    let mut args = vec![
        "serve".to_string(),
        "--data".into(),
        data.to_string_lossy().into(),
    ];
    for drv in DRIVERS {
        args.push("--driver-bin".into());
        args.push(format!("{drv}={}", fake().display()));
        args.push("--driver-env".into());
        args.push(format!("{drv}=WASIT_FAKE_PERSONA={drv}"));
    }
    let mut daemon = tokio::process::Command::new(hostd())
        .args(&args)
        .env("WASIT_FAKE_CONSOLE_REPORT", &report)
        .creation_flags(wasit_process::flags::DETACHED_PROCESS)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut line = String::new();
    let out = daemon.stdout.take().unwrap();
    tokio::time::timeout(T, BufReader::new(out).read_line(&mut line))
        .await
        .expect("daemon readiness")
        .unwrap();
    assert!(line.contains("\"ready\":true"), "{line:?}");

    let c = RuntimeClient::connect_endpoint(
        wasit_control::local_endpoint(&data.join("hostd.sock")),
        Hello::new("test", "gui-test", "gui"),
    );
    // Discovery probes (`--version` and friends) of every driver.
    c.inventory(None).await.unwrap();
    wait_for(&report, "discovery probes", |l| {
        l.iter().any(|x| x.contains("--version"))
    })
    .await;

    // A shim-launched session.
    let sc = Scenario::builder()
        .turn(Turn::builder().say("hello"))
        .build();
    let before = report_lines(&report).len();
    c.create(create(d.path(), "claude", &sc)).await.unwrap();
    let lines = wait_for(&report, "the session", |l| l.len() > before).await;

    // Control: a console program started by a console-less parent WITHOUT wasit-process
    // (here: cmd.exe started with DETACHED_PROCESS starting the fake) — shows what the check
    // detects on this machine.
    let control = d.path().join("control.txt");
    let st = tokio::process::Command::new("cmd")
        .args(["/C", &format!("\"{}\" --version", fake().display())])
        .env("WASIT_FAKE_CONSOLE_REPORT", &control)
        .creation_flags(wasit_process::flags::DETACHED_PROCESS)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .unwrap();
    eprintln!(
        "control (no wasit-process, exit {st}): {:?}",
        report_lines(&control)
    );
    eprintln!("reports: {lines:#?}");

    let visible: Vec<_> = lines.iter().filter(|l| l.contains(" visible")).collect();
    assert!(
        visible.is_empty(),
        "processes with a visible console window: {visible:?}"
    );
    let _ = daemon.start_kill();
}
