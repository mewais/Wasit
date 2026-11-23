//! The sink's files are private to their owner (design S23, leak L4): `diag/` is `0700`, its
//! files `0600`, and what an older build left wider is tightened when the sink opens it.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use wasit_diag::{Code, Ident, Sink, SinkConfig};

wasit_diag::diag_events! {
    enum T { #[diag(ev = "test.n")] N { i: u64 } }
}

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

fn config(dir: &Path, file: &str) -> SinkConfig {
    SinkConfig::new(Code::st("test"), Ident::st("b1"), dir, file)
}

fn write_one(cfg: SinkConfig) {
    let sink = Sink::start(cfg).unwrap();
    sink.emit(&T::N { i: 1 });
    assert!(sink.shutdown(Duration::from_secs(30)));
}

#[test]
fn the_sink_creates_private_files_and_directories() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("machine/diag");
    write_one(config(&dir, "daemon.ndjson"));
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&dir.join("daemon.ndjson")), 0o600);

    // The shared file as well, and the file a rotation creates.
    let mut cfg = SinkConfig::shared(
        Code::st("test"),
        Ident::st("b1"),
        &dir,
        "aux.ndjson",
        1 << 20,
    );
    cfg.queue = 8;
    write_one(cfg);
    assert_eq!(mode(&dir.join("aux.ndjson")), 0o600);
    let mut cfg = config(&dir, "rot.ndjson");
    cfg.max_bytes = 1;
    let sink = Sink::start(cfg).unwrap();
    for i in 0..3 {
        sink.emit(&T::N { i });
    }
    assert!(sink.shutdown(Duration::from_secs(30)));
    assert_eq!(mode(&dir.join("rot.ndjson")), 0o600);
    assert_eq!(mode(&dir.join("rot.ndjson.1")), 0o600);
}

#[test]
fn files_of_an_older_build_are_tightened_when_the_sink_opens_them() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("diag");
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let file = dir.join("daemon.ndjson");
    std::fs::write(&file, "").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!((mode(&dir), mode(&file)), (0o755, 0o644));

    write_one(config(&dir, "daemon.ndjson"));
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&file), 0o600);
}
