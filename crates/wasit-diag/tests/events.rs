//! The serialized form of every sink-internal event (design §12.1): the record header and the
//! declared fields, nothing else.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::{parse, Mem};
use wasit_diag::{Code, DiagEvent, DiagText, ErrorInfo, Ident, Origin, SinkConfig, SinkEvent};

const HEADER: [&str; 7] = ["b", "seq", "t", "m", "src", "lvl", "ev"];

fn line_of(e: &SinkEvent) -> serde_json::Value {
    let mem = Mem::default();
    let cfg = SinkConfig::new(Code::st("hub"), Ident::st("b1"), "unused", "e.ndjson");
    let sink = wasit_diag::Sink::start_with(cfg, mem.clone()).unwrap();
    sink.emit(e);
    assert!(sink.shutdown(Duration::from_secs(30)));
    let lines = parse(&mem.text("e.ndjson"));
    assert_eq!(lines.len(), 2, "header and the record");
    lines[1].clone()
}

fn check(e: SinkEvent, ev: &str, lvl: &str, fields: &[&str]) -> serde_json::Value {
    assert_eq!(e.ev(), ev);
    let v = line_of(&e);
    let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    let want: BTreeSet<&str> = HEADER.iter().chain(fields).copied().collect();
    assert_eq!(keys, want, "{v}");
    assert_eq!(v["ev"], ev);
    assert_eq!(v["lvl"], lvl);
    assert_eq!(v["src"], "hub");
    assert_eq!(v["b"].as_str().unwrap().len(), 8);
    assert_eq!(v["seq"], 1);
    v
}

#[test]
fn process_start() {
    let v = check(
        SinkEvent::ProcessStart {
            role: Code::st("daemon"),
            build: Ident::st("0.4.2+abc"),
            target: Ident::st("x86_64-unknown-linux-gnu"),
            os: Code::st("linux"),
            arch: Code::st("x86_64"),
            os_release: Some(DiagText::new(
                Origin::System,
                "6.6.87.2-microsoft-standard-WSL2",
            )),
            pid: 4242,
        },
        "process.start",
        "info",
        &["role", "build", "target", "os", "arch", "os_release", "pid"],
    );
    assert_eq!(v["pid"], 4242);
    assert_eq!(v["build"], "0.4.2+abc");
    check(
        SinkEvent::ProcessStart {
            role: Code::st("daemon"),
            build: Ident::st("b"),
            target: Ident::st("t"),
            os: Code::st("linux"),
            arch: Code::st("x86_64"),
            os_release: None,
            pid: 1,
        },
        "process.start",
        "info",
        &["role", "build", "target", "os", "arch", "pid"],
    );
}

#[test]
fn process_exit() {
    let v = check(
        SinkEvent::ProcessExit {
            reason: Code::st("signal"),
            code: Some(-2),
        },
        "process.exit",
        "info",
        &["reason", "code"],
    );
    assert_eq!(v["code"], -2);
    check(
        SinkEvent::ProcessExit {
            reason: Code::st("request"),
            code: None,
        },
        "process.exit",
        "info",
        &["reason"],
    );
}

#[test]
fn dropped_suppressed_counted() {
    let v = check(
        SinkEvent::Dropped {
            count: 3,
            first_seq: 10,
            last_seq: 12,
        },
        "diag.dropped",
        "warn",
        &["count", "first_seq", "last_seq"],
    );
    assert_eq!(v["last_seq"], 12);
    let v = check(
        SinkEvent::Suppressed {
            event: Code::st("link.attempt_end"),
            count: 9,
        },
        "diag.suppressed",
        "warn",
        &["event", "count"],
    );
    assert_eq!(v["event"], "link.attempt_end");
    let v = check(
        SinkEvent::TraceCounted {
            target: Code::st("wasit_control::connection"),
            level: Code::st("warn"),
            n: 17,
        },
        "trace.counted",
        "info",
        &["target", "level", "n"],
    );
    assert_eq!(v["target"], "wasit_control::connection");
}

#[test]
fn write_failed_carries_only_kind_and_os_code() {
    let err = std::io::Error::other("/home/someone/secret-project: disk full");
    let v = check(
        SinkEvent::WriteFailed {
            error: ErrorInfo::io(&err),
        },
        "diag.write_failed",
        "error",
        &["error"],
    );
    assert_eq!(v["error"], serde_json::json!({"code": "other"}));
    assert!(!v.to_string().contains("secret-project"));
}

#[test]
fn panic_record() {
    let v = check(
        SinkEvent::Panic {
            location: wasit_diag::SrcLocation::of(std::panic::Location::caller()),
            thread: Ident::st("tokio-runtime-worker"),
            payload_kind: Code::st("static_str"),
            len: 4,
            static_text: Some(DiagText::new(Origin::Static, "boom")),
        },
        "diag.panic",
        "error",
        &["location", "thread", "payload_kind", "len", "static_text"],
    );
    assert!(v["location"].as_str().unwrap().contains("events.rs:"));
    check(
        SinkEvent::Panic {
            location: wasit_diag::SrcLocation::unknown(),
            thread: Ident::st("main"),
            payload_kind: Code::st("string"),
            len: 99,
            static_text: None,
        },
        "diag.panic",
        "error",
        &["location", "thread", "payload_kind", "len"],
    );
}
