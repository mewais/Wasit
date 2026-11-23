//! A session another process created is registered with the scrubber when this process adopts
//! it: its environment (as `session.json` keeps it) and its directory. The session is created by
//! a real `wasit-hostd`, which is then stopped; the test process, which never saw the values,
//! adopts the session with a manager of its own.
#![cfg(unix)]

mod common;

use common::*;
use wasit_diag::{DiagText, Origin};
use wasit_fake_cli::scenario::{Scenario, Turn};

const ENV_SECRET: &str = "adopted-Env-Secret-3a7f";
const SHORT: &str = "ab12";
const DIR: &str = "adopted-dir-Marker-90be";

fn shown(raw: &str) -> String {
    DiagText::new(Origin::HostShell, raw).as_str().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopting_a_session_registers_its_environment_and_directory() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let c = client(&data);
    let sc = Scenario::builder().turn(Turn::builder().say("ok")).build();
    let mut req = create(d.path(), "claude", &sc);
    let project = d.path().join(DIR);
    std::fs::create_dir_all(&project).unwrap();
    let cwd = project.to_string_lossy().into_owned();
    req.cwd = Some(cwd.clone());
    req.config.env.push(("A_TOKEN".into(), ENV_SECRET.into()));
    req.config.env.push(("SHORT".into(), SHORT.into()));
    let id = c.create(req).await.unwrap().id;
    kill_with("TERM", daemon.id().unwrap());
    tokio::time::timeout(T, daemon.wait())
        .await
        .unwrap()
        .unwrap();

    // This process knows none of it.
    let text = format!("env {ENV_SECRET} short {SHORT} cwd {cwd}");
    let before = shown(&text);
    assert!(
        before.contains(ENV_SECRET) && !before.contains("<p:"),
        "{before}"
    );

    let mgr = manager(&data).await;
    let report = mgr.adopt_all().await.unwrap();
    assert_eq!(report.errors, [], "{report:?}");
    assert!(report.running.contains(&id) || report.finished.contains(&id));
    let after = shown(&text);
    assert!(
        !after.contains(ENV_SECRET) && !after.contains(DIR),
        "{after}"
    );
    assert!(after.contains(SHORT) && after.contains("<p:"), "{after}");
    mgr.delete(&id).await.unwrap();
}
