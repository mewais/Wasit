//! The `tracing` bridge records only counts per target and level, never a message or a field
//! (design §4.5, §10 "Tracing bridge"). One test: the sink is process-global.

#![cfg(feature = "tracing")]

mod common;

use common::{ev, file_records};
use tracing_subscriber::layer::SubscriberExt;
use wasit_diag::{Code, Ident, SinkConfig};

const MARKER: &str = "TRACING-CONTENT-MARKER-77ab";

#[test]
fn only_counts_reach_the_sink() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = SinkConfig::new(Code::st("test"), Ident::st("b1"), dir.path(), "t.ndjson");
    wasit_diag::install(cfg).unwrap();

    let subscriber = tracing_subscriber::registry().with(wasit_diag::tracing_layer());
    tracing::subscriber::with_default(subscriber, || {
        tracing::warn!(secret = MARKER, "{}", MARKER);
        tracing::info!(target: "custom::area", path = %MARKER, "one");
        tracing::info!(target: "custom::area", "two {}", MARKER);
    });
    // The per-minute summary is written at the latest at shutdown.
    assert!(wasit_diag::shutdown());

    let text = std::fs::read_to_string(dir.path().join("t.ndjson")).unwrap();
    assert!(!text.contains(MARKER), "{text}");
    let recs = file_records(dir.path(), "t.ndjson");
    assert!(recs.iter().all(|r| ev(r) == "trace.counted"), "{text}");
    let find = |target: &str, level: &str| {
        recs.iter()
            .find(|r| r["target"] == target && r["level"] == level)
            .map(|r| r["n"].as_u64().unwrap())
    };
    assert_eq!(find("tracing_bridge", "warn"), Some(1), "{text}");
    assert_eq!(find("custom::area", "info"), Some(2), "{text}");
    assert_eq!(recs.len(), 2);
}
