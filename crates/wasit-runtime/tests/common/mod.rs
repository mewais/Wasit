#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use wasit_core::{AgentEvent, DriverId};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::{
    CreateSession, Phase, SessionEvent, SessionEventKind, SessionId, SessionSnapshot,
};
use wasit_runtime::{
    DriverOverride, RuntimeConfig, SessionEventStream, SessionManager, ShimLauncher, Timeouts,
};

pub const T: Duration = Duration::from_secs(30);
pub const DRIVERS: [&str; 4] = ["claude", "codex", "opencode", "kilo"];

/// Short temp dir (Unix socket paths are limited to ~104 bytes).
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = PathBuf::from(format!("/tmp/wrt-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Kill CLI processes still running (their shims then exit) before removing the dir.
        if let Ok(rd) = std::fs::read_dir(self.0.join("sessions")) {
            for ent in rd.flatten() {
                let Ok(text) = std::fs::read_to_string(ent.path().join("journal.ndjson")) else {
                    continue;
                };
                for line in text.lines() {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                        continue;
                    };
                    if v["kind"] == "spawned" {
                        if let Some(pid) = v["data"]["pid"].as_u64() {
                            let _ = std::process::Command::new("kill")
                                .args(["-9", &pid.to_string()])
                                .stderr(std::process::Stdio::null())
                                .status();
                        }
                    }
                }
            }
        }
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn hostd() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_wasit-hostd"))
}

/// The fake CLI binary, building it once if this test run did not.
pub fn fake() -> PathBuf {
    static P: OnceLock<PathBuf> = OnceLock::new();
    P.get_or_init(|| {
        if let Some(p) = wasit_fake_cli::fake_cli_path() {
            return p;
        }
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let st = std::process::Command::new(cargo)
            .args([
                "build",
                "-q",
                "-p",
                "wasit-fake-cli",
                "--bin",
                "wasit-fake-cli",
            ])
            .status()
            .expect("run cargo build for the fake CLI");
        assert!(st.success(), "building wasit-fake-cli failed");
        wasit_fake_cli::fake_cli_path().expect("fake CLI built")
    })
    .clone()
}

pub fn timeouts() -> Timeouts {
    Timeouts {
        handshake: Duration::from_secs(20),
        command: Duration::from_secs(20),
        stop_grace: Duration::from_millis(500),
        mailbox: Duration::from_secs(5),
        delete: Duration::from_secs(20),
        reconnect_attempts: 5,
    }
}

pub fn config(dir: &Path, timeouts: Timeouts) -> RuntimeConfig {
    let mut cfg = RuntimeConfig::new(dir, ShimLauncher::subcommand(hostd(), "shim"));
    cfg.timeouts = timeouts;
    for d in DRIVERS {
        cfg.overrides.insert(
            DriverId::new(d),
            DriverOverride {
                binary: Some(fake().to_string_lossy().into_owned()),
                env: vec![("WASIT_FAKE_PERSONA".into(), d.into())],
            },
        );
    }
    cfg
}

pub async fn manager(dir: &Path) -> SessionManager {
    SessionManager::new(config(dir, timeouts())).await.unwrap()
}

/// A create request for `driver` running `scenario` (written next to the data dir).
pub fn create(dir: &Path, driver: &str, scenario: &Scenario) -> CreateSession {
    static N: AtomicU64 = AtomicU64::new(0);
    let path = dir.join(format!(
        "scenario-{}.json",
        N.fetch_add(1, Ordering::Relaxed)
    ));
    scenario.write_to(&path).unwrap();
    let mut req = CreateSession::new(driver);
    req.cwd = Some(dir.to_string_lossy().into_owned());
    req.config.env = vec![(
        "WASIT_FAKE_SCENARIO".into(),
        path.to_string_lossy().into_owned(),
    )];
    req
}

/// Wait (bounded, signalled by the snapshot watch) until `pred` holds.
pub async fn wait_snapshot(
    mgr: &SessionManager,
    id: &SessionId,
    what: &str,
    pred: impl Fn(&SessionSnapshot) -> bool,
) -> SessionSnapshot {
    let mut w = mgr.watch(id).unwrap();
    let r = tokio::time::timeout(T, w.wait_for(|s| pred(s))).await;
    match r {
        Ok(Ok(s)) => (**s).clone(),
        _ => panic!(
            "timed out waiting for {what}; snapshot: {:#?}",
            mgr.snapshot(id).ok()
        ),
    }
}

pub async fn wait_phase(mgr: &SessionManager, id: &SessionId, name: &str) -> SessionSnapshot {
    wait_snapshot(mgr, id, &format!("phase {name}"), |s| {
        s.phase.name() == name
    })
    .await
}

/// Next event of a stream (bounded).
pub async fn next(s: &mut SessionEventStream) -> SessionEvent {
    match tokio::time::timeout(T, s.next()).await {
        Ok(Some(Ok(e))) => e,
        Ok(Some(Err(e))) => panic!("stream error: {e}"),
        Ok(None) => panic!("stream ended"),
        Err(_) => panic!("timed out waiting for an event"),
    }
}

/// Collect events until (and including) the first matching `pred`.
pub async fn until(
    s: &mut SessionEventStream,
    pred: impl Fn(&SessionEvent) -> bool,
) -> Vec<SessionEvent> {
    let mut v = Vec::new();
    loop {
        let e = next(s).await;
        let hit = pred(&e);
        v.push(e);
        if hit {
            return v;
        }
    }
}

pub fn is_agent(e: &SessionEvent, f: impl Fn(&AgentEvent) -> bool) -> bool {
    matches!(&e.kind, SessionEventKind::Agent { event } if f(event))
}

pub fn is_turn_completed(e: &SessionEvent) -> bool {
    is_agent(e, |a| matches!(a, AgentEvent::TurnCompleted { .. }))
}

pub fn is_phase(e: &SessionEvent, name: &str) -> bool {
    matches!(&e.kind, SessionEventKind::Phase { phase } if phase.name() == name)
}

pub fn assistant_text(events: &[SessionEvent]) -> String {
    events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::AssistantDelta { text, .. },
            } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

pub fn assert_monotonic(events: &[SessionEvent]) {
    for w in events.windows(2) {
        assert!(
            w[0].cursor < w[1].cursor,
            "cursors not increasing: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
}

pub fn failed_reason(p: &Phase) -> Option<(&str, &[String])> {
    match p {
        Phase::Failed {
            reason,
            stderr_tail,
        } => Some((reason, stderr_tail)),
        _ => None,
    }
}
