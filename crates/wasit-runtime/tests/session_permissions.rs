//! Design S23 (leak L4): the session directory is `0700`, and `session.json`, `journal.ndjson`,
//! `shim.log`, the shim's stderr file, uploads and the daemon's `diag/` directory and file are
//! `0600`/`0700` — created so, and tightened when the manager adopts a session an older build
//! left wider.
#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use common::*;
use wasit_core::content::b64_encode;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::media::AttachmentPut;

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p)
        .unwrap_or_else(|e| panic!("{}: {e}", p.display()))
        .permissions()
        .mode()
        & 0o777
}

fn set_mode(p: &Path, m: u32) {
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(m)).unwrap();
}

/// The files of a session directory that exist after one session ran, relative to it.
const FILES: &[&str] = &[
    "session.json",
    "journal.ndjson",
    "shim.log",
    "shim.stderr",
    "shim.lock",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_is_private_and_an_old_one_is_tightened_on_adoption() {
    let d = TempDir::new();
    let sc = Scenario::builder().turn(Turn::builder().say("ok")).build();
    let (id, dir, upload) = {
        let mgr = manager(d.path()).await;
        let id = mgr
            .create(create(d.path(), "claude", &sc))
            .await
            .unwrap()
            .id;
        wait_phase(&mgr, &id, "idle").await;
        let up = mgr
            .attachment_put(&AttachmentPut {
                id: id.clone(),
                upload: "up1abcde".into(),
                name: "notes.bin".into(),
                size: 3,
                offset: 0,
                data: b64_encode(b"\0\x01\x02"),
                last: true,
            })
            .await
            .unwrap()
            .attachment
            .unwrap();
        mgr.stop(&id).await.unwrap();
        (id.clone(), d.path().join("sessions").join(id.as_str()), up)
    };
    let sessions = d.path().join("sessions");
    let uploads = dir.join("attachments");
    assert_eq!(mode(&sessions), 0o700);
    assert_eq!(mode(&dir), 0o700);
    for f in FILES {
        assert_eq!(mode(&dir.join(f)), 0o600, "{f}");
    }
    assert_eq!(mode(&uploads), 0o700);
    assert_eq!(mode(&uploads.join("up1abcde")), 0o700);
    assert_eq!(mode(Path::new(&upload.path)), 0o600);

    // What an older build left: everything wider.
    for p in [&sessions, &dir, &uploads, &uploads.join("up1abcde")] {
        set_mode(p, 0o755);
    }
    for f in FILES {
        set_mode(&dir.join(f), 0o644);
    }
    set_mode(Path::new(&upload.path), 0o644);

    let mgr = manager(d.path()).await;
    assert_eq!(mode(&sessions), 0o700, "tightened by the manager");
    let report = mgr.adopt_all().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        report.running.contains(&id) || report.finished.contains(&id),
        "{report:?}"
    );
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&uploads), 0o700);
    for f in FILES {
        assert_eq!(mode(&dir.join(f)), 0o600, "{f}");
    }
    // Inside a tightened directory a file is only reachable through it (the adoption does not
    // walk it): the directory is what protects it.
    assert_eq!(mode(&uploads.join("up1abcde")), 0o755);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_daemons_diagnostics_directory_and_file_are_private() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let pid = daemon.id().unwrap();
    client(&data).host_info().await.unwrap();
    // SIGTERM: the daemon writes its records (and so creates its file) before it exits.
    kill_with("TERM", pid);
    tokio::time::timeout(T, daemon.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mode(&data), 0o700);
    assert_eq!(mode(&data.join("diag")), 0o700);
    assert_eq!(mode(&data.join("diag/daemon.ndjson")), 0o600);

    // A directory and file of an older build, wider: the next daemon tightens them.
    set_mode(&data.join("diag"), 0o755);
    set_mode(&data.join("diag/daemon.ndjson"), 0o644);
    let mut daemon = start_daemon(&data).await;
    let pid = daemon.id().unwrap();
    kill_with("TERM", pid);
    tokio::time::timeout(T, daemon.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mode(&data.join("diag")), 0o700);
    assert_eq!(mode(&data.join("diag/daemon.ndjson")), 0o600);
}
