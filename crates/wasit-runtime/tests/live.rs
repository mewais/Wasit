//! Live tests against the real CLIs, through the whole runtime (manager → actor → real shim →
//! real CLI). Opt-in: `cargo test -p wasit-runtime --test live -- --ignored`. A missing binary
//! or prerequisite fails the test loudly; nothing passes silently.
//!
//! * claude: authenticated `claude`; uses the cheapest model (`haiku`) and tiny prompts.
//! * opencode / kilo: free models (`opencode/deepseek-v4.1-flash-free`, `kilo/kilo-auto/free`).
//! * codex: a throwaway `CODEX_HOME` whose provider is the local fake Responses server
//!   (`wasit-fake-cli/reference/record/fake-responses.mjs`, started here with `node` on
//!   127.0.0.1:18080).

mod common;

use std::time::Duration;

use common::*;
use wasit_core::{AgentEvent, Command, DriverId};
use wasit_runtime::wire::{CreateSession, Phase, SessionEventKind, SessionId};
use wasit_runtime::{default_registry, RuntimeConfig, SessionManager, ShimLauncher};

const LIVE: Duration = Duration::from_secs(240);

fn require(bin: &str) {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let ok = std::env::split_paths(&path).any(|dir| {
        ["", ".exe", ".cmd", ".bat"]
            .iter()
            .any(|ext| dir.join(format!("{bin}{ext}")).is_file())
    });
    assert!(ok, "live test needs `{bin}` on PATH");
}

async fn live_manager(dir: &std::path::Path) -> SessionManager {
    let mut cfg = RuntimeConfig::new(dir, ShimLauncher::subcommand(hostd(), "shim"));
    cfg.timeouts.handshake = Duration::from_secs(120);
    cfg.timeouts.command = Duration::from_secs(60);
    cfg.timeouts.stop_grace = Duration::from_secs(3);
    SessionManager::new(cfg).await.unwrap()
}

/// Create → Idle → one tiny turn → re-adopt in a new manager → identical snapshot → stop.
async fn live_turn(driver: &str, model: Option<&str>, env: Vec<(String, String)>, prompt: &str) {
    let d = TempDir::new();
    let work = d.path().join("w");
    std::fs::create_dir_all(&work).unwrap();
    let (id, before) = {
        let mgr = live_manager(d.path()).await;
        let mut req = CreateSession::new(driver);
        req.cwd = Some(work.to_string_lossy().into_owned());
        req.model = model.map(str::to_string);
        req.config.env = env;
        let id = mgr.create(req).await.unwrap().id;
        let snap = wait_snapshot_within(&mgr, &id, "idle", LIVE, |s| {
            s.phase == Phase::Idle || !s.phase.is_live()
        })
        .await;
        assert_eq!(snap.phase, Phase::Idle, "{snap:#?}");
        let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
        mgr.command(&id, Command::prompt(prompt)).await.unwrap();
        let events = tokio::time::timeout(
            LIVE,
            until(&mut s, |e| {
                is_turn_completed(e) || matches!(e.kind, SessionEventKind::Exited { .. })
            }),
        )
        .await
        .expect("turn completed");
        let text = assistant_text(&events);
        let done = events.iter().find_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::TurnCompleted { end_reason, .. },
            } => Some(*end_reason),
            _ => None,
        });
        assert_eq!(done, Some(None), "turn did not succeed: {events:#?}");
        let snap =
            wait_snapshot_within(&mgr, &id, "idle again", LIVE, |s| s.phase == Phase::Idle).await;
        eprintln!(
            "[{driver}] reply {text:?}; model {:?}; native id {:?}; usage {:?}; context {:?}",
            snap.model, snap.native_session_id, snap.usage, snap.context_pct
        );
        assert!(snap.native_session_id.is_some());
        (id, snap)
    };
    // Re-adopt: the replayed snapshot equals the live one.
    let mgr = live_manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let after = wait_snapshot_within(&mgr, &id, "replayed", LIVE, |s| {
        s.loaded && s.cursor == before.cursor
    })
    .await;
    let mut a = after.clone();
    a.loaded = before.loaded;
    assert_eq!(a, before);
    stop(&mgr, &id).await;
}

async fn stop(mgr: &SessionManager, id: &SessionId) {
    mgr.stop(id).await.unwrap();
    let snap = wait_snapshot_within(mgr, id, "exit", LIVE, |s| !s.phase.is_live()).await;
    assert!(
        matches!(snap.phase, Phase::Exited { .. }),
        "{:?}",
        snap.phase
    );
}

#[tokio::test]
#[ignore = "live: needs an authenticated claude CLI"]
async fn live_claude() {
    require("claude");
    live_turn(
        "claude",
        Some("haiku"),
        vec![],
        "Reply with exactly the word: ok",
    )
    .await;
}

#[tokio::test]
#[ignore = "live: needs the opencode CLI and network"]
async fn live_opencode() {
    require("opencode");
    live_turn(
        "opencode",
        Some("opencode/deepseek-v4.1-flash-free"),
        vec![],
        "Reply with exactly the word: ok",
    )
    .await;
}

#[tokio::test]
#[ignore = "live: needs the kilo CLI and network"]
async fn live_kilo() {
    require("kilo");
    live_turn(
        "kilo",
        Some("kilo/kilo-auto/free"),
        vec![],
        "Reply with exactly the word: ok",
    )
    .await;
}

#[tokio::test]
#[ignore = "live: needs the codex CLI and node (local fake Responses server)"]
async fn live_codex() {
    require("codex");
    require("node");
    let d = TempDir::new();
    let home = d.path().join("codexhome");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.toml"),
        "model = \"fake\"\nmodel_provider = \"fake\"\n[model_providers.fake]\nname = \"fake\"\nbase_url = \"http://127.0.0.1:18080/v1\"\nwire_api = \"responses\"\nrequires_openai_auth = false\n",
    )
    .unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../wasit-fake-cli/reference/record/fake-responses.mjs");
    let mut server = tokio::process::Command::new("node")
        .arg(&script)
        .kill_on_drop(true)
        .spawn()
        .expect("start the fake Responses server");
    // Readiness: the port accepts connections (bounded backoff while node starts).
    let mut backoff = Duration::from_millis(50);
    let mut up = false;
    for _ in 0..20 {
        if tokio::net::TcpStream::connect("127.0.0.1:18080")
            .await
            .is_ok()
        {
            up = true;
            break;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
    assert!(up, "fake Responses server did not start");
    live_turn(
        "codex",
        None,
        vec![("CODEX_HOME".into(), home.to_string_lossy().into_owned())],
        "hello",
    )
    .await;
    let _ = server.start_kill(); // already exiting or gone is fine
}

#[tokio::test]
#[ignore = "live: probes every installed CLI"]
async fn live_discovery() {
    let disc =
        wasit_runtime::Discovery::new(wasit_runtime::DiscoveryConfig::new(default_registry()));
    let mut changes = disc.changes();
    let inv = tokio::time::timeout(LIVE, async {
        loop {
            let inv = disc.inventory(None);
            if inv.drivers.len() == 4 && inv.drivers.iter().all(|d| !d.refreshing) {
                return inv;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    for e in &inv.drivers {
        eprintln!(
            "[{}] installed={} version={:?} models={} modes={} sessions={} probes={:?}",
            e.driver,
            e.installed,
            e.version,
            e.models.len(),
            e.modes.len(),
            e.sessions.len(),
            e.probes
                .iter()
                .map(|p| (p.kind, p.ok, p.error.clone(), p.duration_ms))
                .collect::<Vec<_>>()
        );
        assert!(e.installed, "{} not installed", e.driver);
        assert!(e.version.is_some(), "{e:#?}");
    }
    let _ = DriverId::new("claude");
}
