//! A panic in `wasit-hostd` leaves a `diag.panic` record with its location and the kind and
//! length of its payload, and nothing of the message: not in the diagnostics file, nowhere on
//! disk, and nothing on stderr (design §6.3, leak L1).
//!
//! The panic is real: the test build of `wasit-hostd` (the `panic-test` feature, which only this
//! package's own dev-dependency turns on) panics with a formatted message holding a marker once
//! its diagnostics are installed.

mod common;

use std::path::Path;

use common::*;

const MARKER: &str = "PANIC-CONTENT-MARKER-6d2b";

/// Every file under `dir`, with its bytes.
fn files(dir: &Path, out: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            files(&p, out);
        } else if let Ok(b) = std::fs::read(&p) {
            out.push((p, b));
        }
    }
}

fn records(data: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(data.join("diag/daemon.ndjson"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect()
}

#[tokio::test]
async fn a_panic_is_recorded_without_its_message() {
    let d = TempDir::new();
    let data = d.path().join("d");
    // The process ends on its own: waiting for it is the signal.
    let out = tokio::time::timeout(
        T,
        daemon_command(&data)
            .arg("--panic-test")
            .arg(MARKER)
            .stderr(std::process::Stdio::piped())
            .output(),
    )
    .await
    .expect("the daemon ended")
    .unwrap();
    assert_eq!(out.status.code(), Some(101), "{out:?}");
    assert!(
        out.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());

    let log = records(&data);
    let panics: Vec<_> = log.iter().filter(|r| r["ev"] == "diag.panic").collect();
    assert_eq!(panics.len(), 1, "{log:?}");
    let p = panics[0];
    assert_eq!(p["lvl"], "error");
    assert_eq!(p["thread"], "main");
    assert_eq!(p["payload_kind"], "string");
    assert_eq!(
        p["len"],
        format!("hostd panic test: {MARKER}").chars().count() as u64
    );
    assert!(p.get("static_text").is_none(), "{p}");
    let location = p["location"].as_str().unwrap();
    assert!(location.contains("wasit-hostd.rs:"), "{location}");
    // The panic comes after the daemon started: its start is in the same file.
    assert!(log.iter().any(|r| r["ev"] == "hostd.start"));

    let mut all = Vec::new();
    files(&data, &mut all);
    assert!(all.iter().any(|(p, _)| p.ends_with("daemon.ndjson")));
    for (path, bytes) in all {
        assert!(
            !String::from_utf8_lossy(&bytes).contains(MARKER),
            "marker in {}",
            path.display()
        );
    }
}

/// The developer override: with `WASIT_LOG` set the usual panic message also goes to stderr
/// (which is the daemon's crash file, never exported); the diagnostics file is the same.
#[tokio::test]
async fn the_developer_override_prints_the_message_to_stderr_only() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let out = tokio::time::timeout(
        T,
        daemon_command(&data)
            .env("WASIT_LOG", "info")
            .arg("--panic-test")
            .arg(MARKER)
            .stderr(std::process::Stdio::piped())
            .output(),
    )
    .await
    .expect("the daemon ended")
    .unwrap();
    assert_eq!(out.status.code(), Some(101));
    assert!(String::from_utf8_lossy(&out.stderr).contains(MARKER));
    let text = std::fs::read_to_string(data.join("diag/daemon.ndjson")).unwrap();
    assert!(!text.contains(MARKER));
    assert!(text.contains("\"ev\":\"diag.panic\""));
}
