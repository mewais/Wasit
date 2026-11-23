//! A host whose agents' own stores hold a great deal of text still publishes an inventory that
//! reaches a subscriber: the inventory is one event, a frame has a size limit, and what is in it
//! comes from the user's data (a conversation's first prompt can be a pasted file).
//!
//! The stores are generated here (synthetic text only) in the fake CLIs' homes; discovery reads
//! them as it reads the real ones, and the inventory is followed over a real control connection.

mod common;

use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime};

use common::*;
use wasit_core::{limits, DriverId, CUT};
use wasit_fake_cli::scenario::{Scenario, SessionInfo};
use wasit_runtime::wire::InventoryReply;
use wasit_runtime::{Discovery, DiscoveryConfig, RuntimeService, SessionManager};

const MIB: usize = 1024 * 1024;

fn uuid(n: usize) -> String {
    format!("{n:08x}-1111-4222-8333-{n:012x}")
}

/// A claude transcript of `prompt_bytes` bytes of first prompt, `age_s` seconds old. Two-byte
/// characters, so a cut inside one would show.
fn claude_conversation(dir: &Path, n: usize, prompt_bytes: usize, age_s: u64) -> usize {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(format!("{}.jsonl", uuid(n)));
    let prompt = "é".repeat(prompt_bytes / 2);
    let user = serde_json::json!({"type": "user", "cwd": "/w/big", "message": {"role": "user", "content": prompt}});
    let assistant = serde_json::json!({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "ok"}]}});
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "{user}").unwrap();
    writeln!(f, "{assistant}").unwrap();
    f.set_modified(SystemTime::now() - Duration::from_secs(age_s))
        .unwrap();
    prompt.len()
}

/// The listing of conversations itself worked (the fake CLIs answer some other probes only as far
/// as other tests need).
fn sessions_probe_ok(d: &wasit_runtime::wire::DriverInventory) -> bool {
    d.probes
        .iter()
        .any(|p| p.kind == wasit_core::DiscoveryKind::Sessions && p.ok)
}

#[tokio::test]
async fn a_host_with_a_great_deal_of_conversation_text_still_publishes_its_inventory() {
    let d = TempDir::new();
    let claude = d.path().join("claude");
    let codex = d.path().join("codex");

    // claude: ten conversations with a 3 MiB first prompt each (30 MiB of text: over the frame
    // limit by itself), the newest; 260 small older ones; and, newer than all, 50 transcripts
    // of subagents, which are not conversations and must not take places in the listing.
    let mut text = 0;
    let projects = claude.join("projects");
    for n in 0..10 {
        text += claude_conversation(&projects.join("-w-big"), n, 3 * MIB, 100 + n as u64);
    }
    for n in 0..260 {
        claude_conversation(&projects.join("-w-small"), 1000 + n, 40, 10_000 + n as u64);
    }
    for n in 0..50 {
        let sub = projects.join(format!("-w-big/{}/subagents", uuid(n)));
        claude_conversation(&sub, 5000 + n, 1000, 1);
    }
    assert!(
        text > wasit_control::wire::DEFAULT_MAX_FRAME,
        "the store is not over the frame limit: nothing is proved"
    );

    // codex: one rollout with a 3 MiB first prompt.
    let rollouts = codex.join("sessions/2026/11/17");
    std::fs::create_dir_all(&rollouts).unwrap();
    let id = "01a1ebcf-0528-7cd2-a32d-e89e91b6a844";
    let meta = serde_json::json!({"type": "session_meta", "payload": {"id": id, "cwd": "/w/big"}});
    let prompt = serde_json::json!({"type": "response_item", "payload": {"role": "user",
        "content": [{"type": "input_text", "text": "€".repeat(MIB)}]}});
    std::fs::write(
        rollouts.join(format!("rollout-2026-11-17T23-19-35-{id}.jsonl")),
        format!("{meta}\n{prompt}\n"),
    )
    .unwrap();

    // kilo / opencode: 260 sessions with 8 KB titles (their `session list` is capped by the
    // runtime's own output limit, under which this stays).
    let scenario = Scenario {
        sessions: (0..260)
            .map(|n| SessionInfo {
                id: format!("ses_{n:04}"),
                title: Some("k".repeat(8000)),
                ..SessionInfo::default()
            })
            .collect(),
        ..Scenario::default()
    };
    let scenario_path = d.path().join("sessions.json");
    scenario.write_to(&scenario_path).unwrap();

    let mut cfg = config(d.path(), timeouts());
    for (driver, vars) in [
        ("claude", vec![("CLAUDE_CONFIG_DIR", &claude)]),
        ("codex", vec![("CODEX_HOME", &codex)]),
    ] {
        let o = cfg.overrides.get_mut(&DriverId::new(driver)).unwrap();
        for (k, v) in vars {
            o.env.push((k.into(), v.to_string_lossy().into_owned()));
        }
    }
    for driver in ["opencode", "kilo"] {
        cfg.overrides
            .get_mut(&DriverId::new(driver))
            .unwrap()
            .env
            .push((
                "WASIT_FAKE_SCENARIO".into(),
                scenario_path.to_string_lossy().into_owned(),
            ));
    }
    let mgr = SessionManager::new(cfg.clone()).await.unwrap();
    let service = std::sync::Arc::new(RuntimeService::new(
        mgr,
        Discovery::new(DiscoveryConfig::from_runtime(&cfg)),
        "h",
        "test",
    ));
    let client = loopback(service);

    // The subscriber gets the settled inventory: a frame that was too large would end the
    // subscription with an error instead.
    let mut sub = client.subscribe_inventory(None);
    let inv: InventoryReply = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let (_, inv) = sub
                .next()
                .await
                .expect("the subscription stays open")
                .expect("the inventory event arrives");
            if inv.drivers.len() == 4 && inv.drivers.iter().all(|d| !d.refreshing) {
                return inv;
            }
        }
    })
    .await
    .expect("the inventory settles");
    let size = serde_json::to_vec(&inv).unwrap().len();
    assert!(size < MIB, "the inventory is {size} bytes");

    let by = |id: &str| {
        inv.drivers
            .iter()
            .find(|d| d.driver.as_str() == id)
            .unwrap()
    };
    // claude: the newest 200 conversations (the ten big ones are the newest), every title
    // within its limit, the big ones cut with the marker, the rest said to be left out.
    let c = by("claude");
    assert!(sessions_probe_ok(c), "{:?}", c.probes);
    assert_eq!(c.sessions.len(), 200);
    assert!(
        c.sessions
            .iter()
            .all(|s| s.title.chars().count() <= limits::TITLE_CHARS),
        "a title is over its limit"
    );
    let cut = c.sessions.iter().filter(|s| s.title.ends_with(CUT)).count();
    assert_eq!(cut, 10);
    assert!(
        c.sessions.iter().all(|s| !s.id.starts_with("00001388")),
        "a subagent transcript is listed"
    );
    // 270 conversations, 200 listed.
    assert_eq!(c.not_listed.map(|n| n.sessions), Some(70));
    // codex: the one rollout, its title cut.
    let x = by("codex");
    assert_eq!(x.sessions.len(), 1);
    assert_eq!(x.sessions[0].title.chars().count(), limits::TITLE_CHARS);
    assert!(x.sessions[0].title.ends_with(CUT));
    assert!(x.not_listed.is_none());
    // kilo and opencode: 200 of 260, titles cut, the rest counted.
    for id in ["opencode", "kilo"] {
        let k = by(id);
        assert!(sessions_probe_ok(k), "{id}: {:?}", k.probes);
        assert_eq!(k.sessions.len(), 200, "{id}");
        assert!(k
            .sessions
            .iter()
            .all(|s| s.title.chars().count() == limits::TITLE_CHARS && s.title.ends_with(CUT)));
        assert_eq!(k.not_listed.map(|n| n.sessions), Some(60), "{id}");
    }
}
