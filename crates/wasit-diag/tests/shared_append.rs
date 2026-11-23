//! The shared append file (`FileMode::SharedAppend`, design §6.1 `aux.ndjson`): short-lived
//! processes of one machine append to one file. Real processes: the test binary starts itself
//! as the appenders (`child_appender`), released together over their stdin.
//!
//! What is relied on: each record (with, for the first writer, the file header) is one `write`
//! on a file opened for append. POSIX `O_APPEND` positions and writes as one step, and a local
//! file system serialises the writes of one file; on Windows std opens an append file with
//! `FILE_APPEND_DATA` alone, whose writes always go to the end of file. Not verified here:
//! Windows, macOS, and NFS (where `O_APPEND` is not atomic between machines — the shared file
//! is per machine).

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::Stdio;

use common::parse;
use wasit_diag::{Code, DiagText, FileMode, Ident, Origin, RotateShared, Sink, SinkConfig};

const WAIT: std::time::Duration = std::time::Duration::from_secs(60);
const DIR_ENV: &str = "WASIT_DIAG_SHARED_DIR";
const CAP_ENV: &str = "WASIT_DIAG_SHARED_CAP";
const WORKER_ENV: &str = "WASIT_DIAG_SHARED_WORKER";
const RECORDS: u64 = 100;
const PARTS: usize = 32;
const PART_LEN: usize = 250;

wasit_diag::diag_events! {
    enum T {
        /// A record of about 8 KiB, every character of which is the worker's letter: a write
        /// torn or interleaved with another process's shows as a mixed letter or invalid JSON.
        #[diag(ev = "test.blob", budget = u32::MAX)]
        Blob { worker: u32, i: u64, parts: Vec<DiagText> },
        #[diag(ev = "test.small", budget = u32::MAX)]
        Small { i: u64 },
    }
}

fn blob(worker: u32, i: u64) -> T {
    let letter = char::from(b'a' + (worker % 26) as u8);
    let part = DiagText::new(Origin::Static, &letter.to_string().repeat(PART_LEN));
    T::Blob {
        worker,
        i,
        parts: vec![part; PARTS],
    }
}

fn config(dir: &std::path::Path, cap: u64) -> SinkConfig {
    SinkConfig::shared(Code::st("test"), Ident::st("b1"), dir, "aux.ndjson", cap)
}

/// Not a test: the appender the tests below start as a child process (it does nothing unless
/// the environment says so).
#[test]
fn child_appender() {
    let (Ok(dir), Ok(cap), Ok(worker)) = (
        std::env::var(DIR_ENV),
        std::env::var(CAP_ENV),
        std::env::var(WORKER_ENV),
    ) else {
        return;
    };
    let worker: u32 = worker.parse().unwrap();
    let sink = Sink::start(config(std::path::Path::new(&dir), cap.parse().unwrap())).unwrap();
    // Wait for the release the parent gives all children together.
    let mut go = String::new();
    std::io::stdin().read_line(&mut go).unwrap();
    for i in 0..RECORDS {
        sink.emit(&blob(worker, i));
    }
    assert!(sink.shutdown(WAIT));
    let s = sink.stats();
    println!(
        "\nSTATS {}",
        serde_json::json!({"emitted": s.emitted, "written": s.written, "dropped": s.dropped})
    );
}

struct Done {
    emitted: u64,
    written: u64,
    dropped: u64,
}

/// Runs `workers` appenders at once against `dir`.
fn run_children(dir: &std::path::Path, cap: u64, workers: u32) -> Vec<Done> {
    let exe = std::env::current_exe().unwrap();
    let mut children: Vec<_> = (0..workers)
        .map(|w| {
            wasit_process::std_command(&exe)
                .args([
                    "child_appender",
                    "--exact",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(DIR_ENV, dir)
                .env(CAP_ENV, cap.to_string())
                .env(WORKER_ENV, w.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for c in &mut children {
        let mut stdin = c.stdin.take().unwrap();
        stdin.write_all(b"go\n").unwrap();
    }
    children
        .into_iter()
        .map(|c| {
            let out = c.wait_with_output().unwrap();
            assert!(out.status.success(), "{out:?}");
            let line = BufReader::new(&out.stdout[..])
                .lines()
                .map(Result::unwrap)
                .find_map(|l| l.strip_prefix("STATS ").map(str::to_owned))
                .unwrap_or_else(|| panic!("no stats line: {out:?}"));
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            Done {
                emitted: v["emitted"].as_u64().unwrap(),
                written: v["written"].as_u64().unwrap(),
                dropped: v["dropped"].as_u64().unwrap(),
            }
        })
        .collect()
}

fn blob_records(lines: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    lines.iter().filter(|r| r["ev"] == "test.blob").collect()
}

/// Every line of the file is a whole record (or a header), none torn and none interleaved.
fn check_whole(lines: &[serde_json::Value]) {
    for r in blob_records(lines) {
        let letter = char::from(b'a' + (r["worker"].as_u64().unwrap() % 26) as u8);
        let parts = r["parts"].as_array().unwrap();
        assert_eq!(parts.len(), PARTS);
        for p in parts {
            let p = p.as_str().unwrap();
            assert_eq!(p.chars().count(), PART_LEN);
            assert!(p.chars().all(|c| c == letter), "mixed record: {r}");
        }
    }
}

#[test]
fn many_processes_append_whole_records_to_one_file() {
    const WORKERS: u32 = 6;
    let dir = tempfile::tempdir().unwrap();
    let done = run_children(dir.path(), 1 << 30, WORKERS);
    assert!(done
        .iter()
        .all(|d| d.emitted == RECORDS && d.written == RECORDS && d.dropped == 0));

    let text = std::fs::read_to_string(dir.path().join("aux.ndjson")).unwrap();
    // `parse` fails on any line that is not JSON: a torn or interleaved write would be one.
    let lines = parse(&text);
    check_whole(&lines);
    assert!(text.lines().next().unwrap().contains("\"diag_file\""));
    let blobs = blob_records(&lines);
    assert_eq!(blobs.len() as u64, u64::from(WORKERS) * RECORDS);
    for w in 0..WORKERS {
        let own: Vec<u64> = blobs
            .iter()
            .filter(|r| r["worker"] == w)
            .map(|r| r["i"].as_u64().unwrap())
            .collect();
        assert_eq!(own, (0..RECORDS).collect::<Vec<_>>(), "worker {w}");
        // Per process, records are in order: its writer thread appends them one by one.
        let seqs: Vec<u64> = blobs
            .iter()
            .filter(|r| r["worker"] == w)
            .map(|r| r["seq"].as_u64().unwrap())
            .collect();
        assert!(seqs.windows(2).all(|p| p[0] < p[1]));
    }
    // One boot id per process.
    let boots: std::collections::BTreeSet<_> = blobs.iter().map(|r| r["b"].as_str()).collect();
    assert_eq!(boots.len(), WORKERS as usize);
    // Headers: one per process that found the file empty, at least the first line's.
    let headers = lines
        .iter()
        .filter(|l| l.get("diag_file").is_some())
        .count();
    assert!((1..=WORKERS as usize).contains(&headers), "{headers}");
}

#[test]
fn writers_skip_and_count_when_the_file_is_over_its_cap() {
    const WORKERS: u32 = 4;
    const CAP: u64 = 100 << 10;
    let dir = tempfile::tempdir().unwrap();
    let done = run_children(dir.path(), CAP, WORKERS);
    for d in &done {
        assert_eq!(d.emitted, RECORDS);
        assert_eq!(d.written + d.dropped, d.emitted);
    }
    let dropped: u64 = done.iter().map(|d| d.dropped).sum();
    let written: u64 = done.iter().map(|d| d.written).sum();
    assert!(dropped > 0, "the cap was never reached");

    let path = dir.path().join("aux.ndjson");
    let size = std::fs::metadata(&path).unwrap().len();
    // Each writer may have started one record (plus a header) before seeing the file full.
    let one = (PARTS * PART_LEN) as u64 + 2048;
    assert!(
        size >= CAP && size <= CAP + u64::from(WORKERS) * one,
        "{size} bytes"
    );
    let lines = parse(&std::fs::read_to_string(&path).unwrap());
    check_whole(&lines);
    assert_eq!(blob_records(&lines).len() as u64, written);
}

/// One process: the daemon rotates the shared file when it is over its limit (when its writer
/// starts), writers find a new file afterwards and give it a header, and the generation
/// numbers continue.
#[test]
fn the_daemon_rotates_the_shared_file_and_writers_follow() {
    let dir = tempfile::tempdir().unwrap();
    let aux = dir.path().join("aux.ndjson");

    let first = Sink::start(config(dir.path(), 1 << 30)).unwrap();
    for i in 0..40 {
        first.emit(&blob(1, i));
    }
    assert!(first.shutdown(WAIT));
    let before = std::fs::metadata(&aux).unwrap().len();
    assert!(before > 40 * 8000);

    // The daemon's sink: its own file, the shared file under its care. Its writer rotates
    // `aux.ndjson` before it takes a record, so once this record is flushed the rotation is done.
    let mut cfg = SinkConfig::new(
        Code::st("daemon"),
        Ident::st("b1"),
        dir.path(),
        "daemon.ndjson",
    );
    cfg.rotate_shared = Some(RotateShared {
        file: "aux.ndjson".into(),
        max_bytes: 100_000,
        generations: 1,
    });
    let daemon = Sink::start(cfg).unwrap();
    daemon.emit(&T::Small { i: 0 });
    assert!(daemon.flush(WAIT));
    assert!(!aux.exists(), "rotated away");
    assert_eq!(
        std::fs::metadata(dir.path().join("aux.ndjson.1"))
            .unwrap()
            .len(),
        before
    );

    // A short-lived process now creates the file again, with its own header, one generation on.
    let second = Sink::start(config(dir.path(), 1 << 30)).unwrap();
    second.emit(&T::Small { i: 1 });
    assert!(second.shutdown(WAIT));
    let lines = parse(&std::fs::read_to_string(&aux).unwrap());
    assert_eq!(lines[0]["diag_file"], 1);
    assert_eq!(lines[0]["gen"], 2);
    assert_eq!(lines.len(), 2);
    let old = parse(&std::fs::read_to_string(dir.path().join("aux.ndjson.1")).unwrap());
    assert_eq!(old[0]["gen"], 1);

    // Under its limit the shared file is left alone.
    let third = Sink::start(config(dir.path(), 1 << 30)).unwrap();
    third.emit(&T::Small { i: 2 });
    assert!(third.shutdown(WAIT));
    let mut cfg = SinkConfig::new(
        Code::st("daemon"),
        Ident::st("b2"),
        dir.path(),
        "daemon.ndjson",
    );
    cfg.rotate_shared = Some(RotateShared {
        file: "aux.ndjson".into(),
        max_bytes: 100_000,
        generations: 1,
    });
    let again = Sink::start(cfg).unwrap();
    again.emit(&T::Small { i: 3 });
    assert!(again.flush(WAIT));
    assert!(aux.exists());
    assert!(daemon.shutdown(WAIT) && again.shutdown(WAIT));

    // The reader sees the generations of the shared file.
    let sources = wasit_diag::reader::source(dir.path(), "aux.ndjson");
    assert_eq!(
        sources.gens.iter().map(|g| g.gen).collect::<Vec<_>>(),
        [1, 2]
    );
}

#[test]
fn promote_leaves_the_shared_mode() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Sink::start(config(dir.path(), 1 << 30)).unwrap();
    sink.emit(&T::Small { i: 1 });
    sink.promote("daemon.ndjson").unwrap();
    sink.emit(&T::Small { i: 2 });
    sink.emit(&T::Small { i: 3 });
    assert!(sink.shutdown(WAIT));
    assert_eq!(common::file_records(dir.path(), "aux.ndjson").len(), 1);
    let daemon = common::file_records(dir.path(), "daemon.ndjson");
    assert_eq!(
        daemon
            .iter()
            .map(|r| r["i"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [2, 3]
    );
    // The promoted file is owned: it has its header and is written in batches.
    let text = std::fs::read_to_string(dir.path().join("daemon.ndjson")).unwrap();
    assert!(text.lines().next().unwrap().contains("diag_file"));
    assert_eq!(
        SinkConfig::shared(Code::st("t"), Ident::st("b"), ".", "x.ndjson", 5).mode,
        FileMode::SharedAppend { cap: 5 }
    );
}
