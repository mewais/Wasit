//! `diag_data!`: a projection declared from value types serves one JSON form, on a wire and in a
//! log record, and reads back.

mod common;

use std::time::Duration;

use common::{parse, Mem};
use wasit_diag::{Bytes, Code, DiagEvent, ErrorInfo, Ident, Ms, SinkConfig};

wasit_diag::diag_data! {
    #[diag(tag = "kind")]
    /// Why something happened.
    pub enum Cause {
        #[diag(ev = "started")]
        Started { generation: u32 ; input: u64 },
        #[diag(ev = "ended")]
        Ended { requested: bool ; code: i32, signal: i32 },
        #[diag(ev = "unknown")]
        Unknown {},
    }
}

wasit_diag::diag_data! {
    #[diag(tag = "ev")]
    /// A projected entry.
    pub enum Entry {
        #[diag(ev = "phase.changed")]
        Phase { to: Code, cause: Cause ; reason: wasit_diag::DiagText },
        #[diag(ev = "input.dropped")]
        Dropped { input: u64, reason: Code, size: Bytes, took: Ms, tags: Vec<Code> ; error: ErrorInfo, who: Ident },
    }
}

wasit_diag::diag_events! {
    pub enum Logged {
        #[diag(ev = "entry.seen")]
        Seen { session: Ident, entry: Entry },
    }
}

fn samples() -> Vec<Entry> {
    vec![
        Entry::Phase {
            to: Code::st("idle"),
            cause: Cause::Started {
                generation: 2,
                input: Some(9),
            },
            reason: None,
        },
        Entry::Phase {
            to: Code::st("exited"),
            cause: Cause::Ended {
                requested: true,
                code: Some(0),
                signal: None,
            },
            reason: Some(wasit_diag::DiagText::new(
                wasit_diag::Origin::Own,
                "the CLI exited",
            )),
        },
        Entry::Phase {
            to: Code::st("lost"),
            cause: Cause::Unknown {},
            reason: None,
        },
        Entry::Dropped {
            input: 7,
            reason: Code::st("refused"),
            size: Bytes(3),
            took: Ms(4),
            tags: vec![Code::st("a"), Code::new("not a word")],
            error: Some(ErrorInfo::foreign(Code::st("refused"), "SECRET-TEXT")),
            who: Some(Ident::st("sess-1")),
        },
    ]
}

#[test]
fn the_wire_form_is_tagged_leaves_optional_fields_out_and_reads_back() {
    let v = serde_json::to_value(&samples()[0]).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"ev":"phase.changed","to":"idle","cause":{"kind":"started","generation":2,"input":9}})
    );
    let v = serde_json::to_value(&samples()[2]).unwrap();
    assert_eq!(
        v,
        serde_json::json!({"ev":"phase.changed","to":"lost","cause":{"kind":"unknown"}})
    );
    for e in samples() {
        let text = serde_json::to_string(&e).unwrap();
        assert!(
            !text.contains("SECRET-TEXT") && !text.contains("not a word"),
            "{text}"
        );
        assert_eq!(serde_json::from_str::<Entry>(&text).unwrap(), e, "{text}");
    }
    // An absent optional field reads back as absent, an unknown variant is an error.
    let e: Entry = serde_json::from_str(
        r#"{"ev":"input.dropped","input":1,"reason":"x","size":2,"took":3,"tags":[]}"#,
    )
    .unwrap();
    assert!(matches!(
        e,
        Entry::Dropped {
            error: None,
            who: None,
            ..
        }
    ));
    assert!(serde_json::from_str::<Entry>(r#"{"ev":"nope"}"#).is_err());
}

#[test]
fn declared_lists_every_variant_with_its_fields_and_matches_what_is_written() {
    assert_eq!(
        Cause::DECLARED,
        &[
            ("started", &["generation", "input"][..]),
            ("ended", &["requested", "code", "signal"][..]),
            ("unknown", &[][..]),
        ]
    );
    // Every variant, all fields present, writes exactly its declared keys.
    let full = [
        serde_json::to_value(Entry::Phase {
            to: Code::st("idle"),
            cause: Cause::Unknown {},
            reason: Some(wasit_diag::DiagText::new(wasit_diag::Origin::Own, "r")),
        })
        .unwrap(),
        serde_json::to_value(&samples()[3]).unwrap(),
    ];
    for v in full {
        let ev = v["ev"].as_str().unwrap();
        let declared = Entry::DECLARED.iter().find(|(n, _)| *n == ev).unwrap().1;
        let mut keys: Vec<&str> = v
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .filter(|k| *k != "ev")
            .collect();
        let mut want = declared.to_vec();
        keys.sort_unstable();
        want.sort_unstable();
        assert_eq!(keys, want, "{v}");
    }
    assert_eq!(samples()[1].ev(), "phase.changed");
}

#[test]
fn a_projection_is_a_value_in_a_log_record_with_the_same_json() {
    let mem = Mem::default();
    let cfg = SinkConfig::new(Code::st("hub"), Ident::st("b1"), "unused", "e.ndjson");
    let sink = wasit_diag::Sink::start_with(cfg, mem.clone()).unwrap();
    let entry = samples().remove(3);
    let e = Logged::Seen {
        session: Ident::st("s1"),
        entry: entry.clone(),
    };
    assert_eq!(e.ev(), "entry.seen");
    sink.emit(&e);
    assert!(sink.shutdown(Duration::from_secs(30)));
    let lines = parse(&mem.text("e.ndjson"));
    assert_eq!(lines[1]["entry"], serde_json::to_value(&entry).unwrap());
    assert!(!lines[1].to_string().contains("SECRET-TEXT"));
}
