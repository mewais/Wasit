//! Discovery probes executed against the fake CLI (exec, throwaway handshake, file listing).

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::*;
use wasit_core::{DiscoveryKind, DriverId};
use wasit_fake_cli::scenario::Scenario;
use wasit_runtime::wire::InventoryReply;
use wasit_runtime::{default_registry, Discovery, DiscoveryConfig, DriverOverride};

async fn settled(disc: &Discovery, project: Option<&str>) -> InventoryReply {
    let mut changes = disc.changes();
    let r = tokio::time::timeout(T, async {
        loop {
            let inv = disc.inventory(project);
            if inv.drivers.len() == 4 && inv.drivers.iter().all(|d| !d.refreshing) {
                return inv;
            }
            changes.changed().await.unwrap();
        }
    })
    .await;
    r.expect("discovery settled")
}

#[tokio::test]
async fn inventory_from_the_fake_clis() {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .version("9.9.9")
        .model("p/alpha", "Alpha")
        .model("p/beta", "Beta")
        .mode("build", "Build")
        .mode("plan", "Plan")
        .build();
    let path = d.path().join("sc.json");
    sc.write_to(&path).unwrap();
    let mut cfg = DiscoveryConfig::new(default_registry());
    cfg.handshake_timeout = Duration::from_secs(10);
    let mut overrides = BTreeMap::new();
    for drv in DRIVERS {
        overrides.insert(
            DriverId::new(drv),
            DriverOverride {
                binary: Some(fake().to_string_lossy().into_owned()),
                env: vec![
                    ("WASIT_FAKE_PERSONA".into(), drv.into()),
                    ("WASIT_FAKE_SCENARIO".into(), path.to_string_lossy().into()),
                    // Point session discovery at an empty place.
                    (
                        "CODEX_HOME".into(),
                        d.path().join("codex").to_string_lossy().into(),
                    ),
                    (
                        "CLAUDE_CONFIG_DIR".into(),
                        d.path().join("claude").to_string_lossy().into(),
                    ),
                ],
            },
        );
    }
    cfg.overrides = overrides;
    let disc = Discovery::new(cfg);
    let mut sub = disc.subscribe(None, None);
    let inv = settled(&disc, None).await;
    for e in &inv.drivers {
        assert!(e.installed, "{e:?}");
        assert_eq!(e.binary_path.as_deref(), Some(fake().to_str().unwrap()));
        let version = e
            .probes
            .iter()
            .find(|p| p.kind == DiscoveryKind::Version)
            .unwrap();
        assert!(version.ok, "{e:#?}");
        assert!(e.version.as_deref().unwrap().contains("9.9.9"), "{e:#?}");
        // Every probe ran and was reported, with a reason when it failed.
        assert!(!e.probes.is_empty());
        for p in &e.probes {
            assert!(p.ok || p.error.is_some(), "{p:?}");
        }
        assert!(e.refreshed_at_ms > 0);
    }
    let by = |id: &str| {
        inv.drivers
            .iter()
            .find(|d| d.driver.as_str() == id)
            .unwrap()
    };
    // claude: models from the initialize handshake; codex: model/list in its handshake;
    // ACP: `models` command.
    for id in ["claude", "codex", "opencode", "kilo"] {
        let ids: Vec<&str> = by(id).models.iter().map(|m| m.id.as_str()).collect();
        assert!(!ids.is_empty(), "{id}: {:#?}", by(id));
    }
    assert!(by("opencode").models.iter().any(|m| m.id == "p/alpha"));
    assert!(
        by("codex").modes.iter().any(|m| m.id == "plan"),
        "{:#?}",
        by("codex")
    );
    // The subscription delivered the inventory (first event = current state).
    let (cursor, first) = tokio::time::timeout(T, sub.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(cursor.contains('.'));
    assert_eq!(first.drivers.len(), 4);

    // A refresh re-runs the probes without blocking the caller.
    disc.refresh(Some(&DriverId::new("kilo")), None).unwrap();
    assert!(disc.refresh(Some(&DriverId::new("nope")), None).is_err());
    settled(&disc, None).await;
}

#[tokio::test]
async fn missing_binary_is_reported_not_installed() {
    let mut cfg = DiscoveryConfig::new(default_registry());
    for drv in DRIVERS {
        cfg.overrides.insert(
            DriverId::new(drv),
            DriverOverride {
                binary: Some(format!("/nonexistent/{drv}")),
                env: vec![],
            },
        );
    }
    let disc = Discovery::new(cfg);
    let inv = settled(&disc, Some("/tmp")).await;
    for e in &inv.drivers {
        assert!(!e.installed);
        assert!(
            e.error.as_deref().unwrap().contains("/nonexistent/"),
            "{e:?}"
        );
        assert_eq!(e.project.as_deref(), Some("/tmp"));
    }
}

#[tokio::test]
async fn a_folder_that_does_not_exist_runs_no_probes() {
    let d = TempDir::new();
    let sc = Scenario::builder()
        .version("9.9.9")
        .model("p/alpha", "Alpha")
        .build();
    let path = d.path().join("sc.json");
    sc.write_to(&path).unwrap();
    let mut cfg = DiscoveryConfig::new(default_registry());
    for drv in DRIVERS {
        cfg.overrides.insert(
            DriverId::new(drv),
            DriverOverride {
                binary: Some(fake().to_string_lossy().into_owned()),
                env: vec![
                    ("WASIT_FAKE_PERSONA".into(), drv.into()),
                    ("WASIT_FAKE_SCENARIO".into(), path.to_string_lossy().into()),
                ],
            },
        );
    }
    let disc = Discovery::new(cfg);
    settled(&disc, None).await;
    // A half-typed path: answered at once, the host-wide models kept, nothing started.
    let gone = d.path().join("no-such-fold");
    let gone = gone.to_string_lossy();
    let inv = settled(&disc, Some(&gone)).await;
    for e in &inv.drivers {
        assert!(e.installed, "{e:?}");
        assert!(e.probes.is_empty(), "{e:?}");
        assert!(
            e.error.as_deref().unwrap().contains("does not exist"),
            "{e:?}"
        );
        assert!(e.models.iter().any(|m| m.id == "p/alpha"), "{e:?}");
    }
}
