//! The process-global sink: a no-op before `install`, the queue override, promotion, the reader
//! API and shutdown. One test: the sink is process-global.

mod common;

use common::{ev, file_records};
use wasit_diag::{Code, Ident, SinkConfig, SinkEvent};

fn exit(reason: &'static str) -> SinkEvent {
    SinkEvent::ProcessExit {
        reason: Code::st(reason),
        code: None,
    }
}

#[test]
fn global_sink_lifecycle() {
    // Without a sink: nothing happens, nothing waits.
    wasit_diag::emit(&exit("before"));
    assert!(wasit_diag::flush(std::time::Duration::from_secs(1)));
    assert!(wasit_diag::stats().is_none());
    assert!(wasit_diag::sources().is_empty());

    // The documented dev/test override of the queue size. Set before any thread of this test
    // binary reads the environment.
    std::env::set_var(wasit_diag::QUEUE_ENV, "7");
    let dir = tempfile::tempdir().unwrap();
    let cfg = SinkConfig::new(
        Code::st("daemon"),
        Ident::st("b1"),
        dir.path(),
        "aux.ndjson",
    );
    wasit_diag::install(cfg.clone()).unwrap();
    assert!(matches!(
        wasit_diag::install(cfg),
        Err(wasit_diag::InstallError::AlreadyInstalled)
    ));
    assert_eq!(wasit_diag::stats().unwrap().queue, 7);

    wasit_diag::emit(&exit("in_aux"));
    wasit_diag::promote("daemon.ndjson").unwrap();
    wasit_diag::emit(&exit("in_daemon"));
    assert!(wasit_diag::flush(std::time::Duration::from_secs(30)));

    let sources = wasit_diag::sources();
    let names: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["aux", "daemon"]);
    let gen = sources[1].gens[0].gen;
    let chunk = wasit_diag::read("daemon", gen, 0, 1 << 20).unwrap();
    assert!(chunk.data.contains("\"reason\":\"in_daemon\""));
    assert!(wasit_diag::read("nope", 1, 0, 10).is_err());

    assert!(wasit_diag::shutdown());
    wasit_diag::emit(&exit("after"));
    let s = wasit_diag::stats().unwrap();
    assert_eq!(s.emitted, 2);
    assert_eq!(s.written, 2);

    let aux = file_records(dir.path(), "aux.ndjson");
    let daemon = file_records(dir.path(), "daemon.ndjson");
    assert_eq!(
        aux.iter().map(|r| r["reason"].clone()).collect::<Vec<_>>(),
        ["in_aux"]
    );
    assert_eq!(
        daemon
            .iter()
            .map(|r| r["reason"].clone())
            .collect::<Vec<_>>(),
        ["in_daemon"]
    );
    assert!(aux.iter().chain(&daemon).all(|r| ev(r) == "process.exit"));
    assert_eq!(aux[0]["src"], "daemon");
    assert!(daemon[0]["seq"].as_u64() > aux[0]["seq"].as_u64());
}
