//! The panic hook records location, thread and payload kind, and the text of a static payload
//! only (design §6.3). One test: the hook and the sink are process-global.

mod common;

use common::{ev, file_records};
use wasit_diag::{Code, Ident, SinkConfig};

const MARKER: &str = "PANIC-CONTENT-MARKER-5c1e";

#[test]
fn panic_records_hold_no_formatted_payload() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = SinkConfig::new(Code::st("test"), Ident::st("b1"), dir.path(), "p.ndjson");
    wasit_diag::install(cfg).unwrap();
    wasit_diag::install_panic_hook(false);

    let formatted = std::thread::Builder::new()
        .name("panicker".into())
        .spawn(|| {
            let content = String::from(MARKER);
            panic!("the prompt was {content}");
        })
        .unwrap()
        .join();
    assert!(formatted.is_err());
    let fixed = std::thread::Builder::new()
        .name("fixed".into())
        .spawn(|| panic!("static boom"))
        .unwrap()
        .join();
    assert!(fixed.is_err());
    assert!(wasit_diag::shutdown());

    let text = std::fs::read_to_string(dir.path().join("p.ndjson")).unwrap();
    assert!(!text.contains(MARKER), "{text}");
    let panics: Vec<_> = file_records(dir.path(), "p.ndjson")
        .into_iter()
        .filter(|r| ev(r) == "diag.panic")
        .collect();
    assert_eq!(panics.len(), 2, "{text}");

    let p = &panics[0];
    assert_eq!(p["thread"], "panicker");
    assert_eq!(p["payload_kind"], "string");
    assert_eq!(
        p["len"],
        format!("the prompt was {MARKER}").chars().count() as u64
    );
    assert!(p.get("static_text").is_none());
    let loc = p["location"].as_str().unwrap();
    assert!(loc.contains("panic_hook.rs:"), "{loc}");

    let s = &panics[1];
    assert_eq!(s["thread"], "fixed");
    assert_eq!(s["payload_kind"], "static_str");
    assert_eq!(s["static_text"], "static boom");
    assert_eq!(s["len"], 11);
}
