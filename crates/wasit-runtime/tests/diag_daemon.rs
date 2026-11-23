//! `wasit-hostd` records typed, content-free events in `<data>/diag/daemon.ndjson` (design S3):
//! start, adoption, every control request with its outcome, shutdown; and a prompt, a cwd, a
//! session name and an error message never reach the file.
#![cfg(unix)]

mod common;

use common::*;
use wasit_core::Command;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::wire::SessionEvent;

const MARKER: &str = "DAEMON-LOG-MARKER-0be7";

async fn next_event(sub: &mut wasit_runtime::TypedSubscription<SessionEvent>) -> SessionEvent {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok((_, e)))) => e,
        other => panic!(
            "no event: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_daemon_writes_ndjson_events_and_no_content() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let pid = daemon.id().unwrap();

    let c = client(&data);
    c.host_info().await.unwrap();
    let sc = Scenario::builder()
        .turn(Turn::builder().say(format!("reply {MARKER}")))
        .build();
    let mut req = create(d.path(), "claude", &sc);
    req.cwd = Some(
        d.path()
            .join(format!("project-{MARKER}"))
            .to_string_lossy()
            .into_owned(),
    );
    std::fs::create_dir_all(req.cwd.as_ref().unwrap()).unwrap();
    let id = c.create(req).await.unwrap().id;
    let mut sub = c.subscribe_session(&id, None);
    loop {
        if is_phase(&next_event(&mut sub).await, "idle") {
            break;
        }
    }
    c.command(&id, Command::prompt(format!("prompt {MARKER}")))
        .await
        .unwrap();
    loop {
        if is_turn_completed(&next_event(&mut sub).await) {
            break;
        }
    }
    // A refused request: its code is recorded, its message is not.
    let err = c
        .create(wasit_runtime::wire::CreateSession::new(
            wasit_core::DriverId::new(format!("nope-{MARKER}")),
        ))
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("nope"), "{err:?}");

    // SIGTERM ends the daemon cleanly: it writes what is queued before it exits.
    kill_with("TERM", pid);
    let status = tokio::time::timeout(T, daemon.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status:?}");

    let text = std::fs::read_to_string(data.join("diag/daemon.ndjson")).unwrap();
    assert!(!text.contains(MARKER), "{text}");
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect();
    let header = &lines[0];
    assert_eq!(
        (header["diag_file"].as_u64(), header["src"].as_str()),
        (Some(1), Some("daemon"))
    );
    assert_eq!(header["pid"], pid);
    let ev = |name: &str| -> Vec<&serde_json::Value> {
        lines.iter().filter(|r| r["ev"] == name).collect()
    };
    assert_eq!(ev("process.start").len(), 1);
    assert_eq!(ev("hostd.start").len(), 1);
    let adopted = ev("hostd.adopted");
    assert_eq!(
        (
            adopted[0]["running"].as_u64(),
            adopted[0]["finished"].as_u64(),
            adopted[0]["errors"].as_u64()
        ),
        (Some(0), Some(0), Some(0))
    );
    assert_eq!(ev("hostd.ready").len(), 1);
    let ok = |method: &str| {
        ev("hostd.request")
            .into_iter()
            .any(|r| r["method"] == method && r["ok"] == true)
    };
    assert!(ok("host.info") && ok("sessions.create") && ok("session.command"));
    let refused: Vec<_> = ev("hostd.request")
        .into_iter()
        .filter(|r| r["ok"] == false)
        .collect();
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert_eq!(refused[0]["method"], "sessions.create");
    assert!(refused[0]["code"].is_string());
    assert!(ev("hostd.subscribe").iter().any(|r| r["ok"] == true));
    let down = ev("hostd.shutdown");
    assert_eq!(down.len(), 1);
    assert_eq!(down[0]["reason"], "SIGTERM");
    // Only the sink's per-minute `tracing` counts, written when it ends, follow it.
    let after: Vec<_> = lines
        .iter()
        .skip_while(|r| r["ev"] != "hostd.shutdown")
        .skip(1)
        .collect();
    assert!(
        after.iter().all(|r| r["ev"] == "trace.counted"),
        "{after:?}"
    );
    // Strictly increasing, no gap: nothing was dropped.
    let seqs: Vec<u64> = lines[1..]
        .iter()
        .map(|r| r["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    assert!(ev("diag.dropped").is_empty());

    // The session's own log is diagnostic records too, and as free of content.
    let shim_log =
        std::fs::read_to_string(data.join("sessions").join(id.as_str()).join("shim.log")).unwrap();
    assert!(!shim_log.contains(MARKER), "{shim_log}");
    assert!(shim_log
        .lines()
        .all(|l| serde_json::from_str::<serde_json::Value>(l).is_ok()));
    assert!(shim_log.contains("\"ev\":\"shim.start\""));
}
