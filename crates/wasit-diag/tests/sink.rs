//! The sink with its real writer: never blocking, rotation under load, drops, budgets,
//! promotion, the ring and the reader (design §10). Runs on Linux, Windows and macOS.

mod common;

use std::collections::BTreeSet;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use common::{ev, file_records, parse, records, Mem};
use wasit_diag::{Code, Filter, Ident, Sink, SinkConfig};

const WAIT: Duration = Duration::from_secs(60);

wasit_diag::diag_events! {
    enum T {
        #[diag(ev = "test.n", budget = u32::MAX)]
        N { worker: u32, i: u64 },
        #[diag(ev = "test.limited", budget = 5)]
        Limited { i: u64 },
        #[diag(ev = "test.keyed", budget = u32::MAX)]
        Keyed { host: Ident, session: Option<Ident>, trace: Option<Ident>, i: u64 },
    }
}

fn config(dir: &std::path::Path, file: &str) -> SinkConfig {
    SinkConfig::new(Code::st("test"), Ident::st("b1"), dir, file)
}

#[test]
fn emit_never_waits_for_a_blocked_writer() {
    const QUEUE: usize = 1000;
    const EMITTED: u64 = 100_000;
    let (release, gate) = mpsc::channel();
    let mem = Mem::gated(gate);
    let mut cfg = config(std::path::Path::new("unused"), "t.ndjson");
    cfg.queue = QUEUE;
    let sink = Sink::start_with(cfg, mem.clone()).unwrap();

    // The writer is held in its first write (the file header) until `release`: every emit
    // below must still return.
    for i in 0..EMITTED {
        sink.emit(&T::N { worker: 0, i });
    }
    let s = sink.stats();
    assert_eq!(s.emitted, EMITTED);
    assert_eq!(s.dropped, EMITTED - QUEUE as u64);
    assert_eq!(s.written, 0);

    release.send(()).unwrap();
    assert!(sink.flush(WAIT));
    let lines = parse(&mem.text("t.ndjson"));
    assert!(lines[0].get("diag_file").is_some(), "{}", lines[0]);
    let recs = &lines[1..];
    assert_eq!(
        recs.len(),
        QUEUE + 1,
        "the queued records and one drop count"
    );
    for (k, r) in recs[..QUEUE].iter().enumerate() {
        assert_eq!(ev(r), "test.n");
        assert_eq!(r["seq"], k as u64 + 1);
        assert_eq!(r["i"], k as u64);
    }
    let d = &recs[QUEUE];
    assert_eq!(ev(d), "diag.dropped");
    assert_eq!(d["lvl"], "warn");
    assert_eq!(d["count"], EMITTED - QUEUE as u64);
    assert_eq!(d["first_seq"], QUEUE as u64 + 1);
    assert_eq!(d["last_seq"], EMITTED);
    let s = sink.stats();
    assert_eq!(s.written, QUEUE as u64);
    assert_eq!(s.written + s.dropped, s.emitted);
    assert!(sink.shutdown(WAIT));
}

#[test]
fn rotation_under_load_keeps_bounds_order_and_counts() {
    const THREADS: u32 = 8;
    const PER: u64 = 25_000;
    const MAX: u64 = 64 << 10;
    const GENS: u32 = 3;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), "load.ndjson");
    cfg.max_bytes = MAX;
    cfg.generations = GENS;
    // Small enough that the emitters outrun the writer and the drop path runs too.
    cfg.queue = 64;
    let sink = Arc::new(Sink::start(cfg).unwrap());
    let workers: Vec<_> = (0..THREADS)
        .map(|w| {
            let sink = sink.clone();
            std::thread::spawn(move || {
                for i in 0..PER {
                    sink.emit(&T::N { worker: w, i });
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    assert!(sink.shutdown(WAIT));

    let s = sink.stats();
    assert_eq!(s.emitted, u64::from(THREADS) * PER);
    assert_eq!(s.written + s.dropped, s.emitted, "{s:?}");

    let mut names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert!(names.len() <= GENS as usize + 1, "{names:?}");
    assert!(names.contains(&"load.ndjson".to_owned()));

    // Oldest generation first.
    let mut order: Vec<String> = (1..=GENS)
        .rev()
        .map(|k| format!("load.ndjson.{k}"))
        .collect();
    order.push("load.ndjson".to_owned());
    let mut seqs = Vec::new();
    let mut drops = Vec::new();
    let mut gens = Vec::new();
    let mut longest = 0;
    for name in order.iter().filter(|n| names.contains(n)) {
        let text = std::fs::read_to_string(dir.path().join(name)).unwrap();
        for l in text.lines() {
            longest = longest.max(l.len() as u64 + 1);
        }
        let size = std::fs::metadata(dir.path().join(name)).unwrap().len();
        let lines = parse(&text);
        assert!(
            lines[0].get("diag_file").is_some(),
            "{name} starts with its header"
        );
        gens.push(lines[0]["gen"].as_u64().unwrap());
        for r in records(&text) {
            seqs.push(r["seq"].as_u64().unwrap());
            if ev(&r) == "diag.dropped" {
                drops.push((
                    r["first_seq"].as_u64().unwrap(),
                    r["last_seq"].as_u64().unwrap(),
                    r["count"].as_u64().unwrap(),
                ));
            }
        }
        assert!(size <= MAX + longest, "{name}: {size} bytes");
    }
    assert!(gens.windows(2).all(|w| w[0] < w[1]), "generations {gens:?}");
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "seqs increase across generations"
    );

    // Every seq missing between the first and the last surviving record is explained by a
    // `diag.dropped` range, and no written record lies inside one.
    let present: BTreeSet<u64> = seqs.iter().copied().collect();
    let (lo, hi) = (seqs[0], *seqs.last().unwrap());
    for &(first, last, count) in &drops {
        assert_eq!(count, last - first + 1, "drop ranges are contiguous");
        assert!((first..=last).all(|q| !present.contains(&q)));
    }
    for q in lo..=hi {
        if !present.contains(&q) {
            assert!(
                drops.iter().any(|&(f, l, _)| (f..=l).contains(&q)),
                "seq {q} missing without a drop record"
            );
        }
    }
}

#[test]
fn reader_returns_whole_lines_and_follows_a_generation_through_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), "r.ndjson");
    cfg.max_bytes = 2000;
    cfg.generations = 2;
    let sink = Sink::start(cfg).unwrap();
    for i in 0..3 {
        sink.emit(&T::N { worker: 1, i });
    }
    assert!(sink.flush(WAIT));
    let src = sink.sources();
    assert_eq!(src.len(), 1);
    assert_eq!(src[0].name, "r");
    assert_eq!(src[0].gens.len(), 1);
    let g = src[0].gens[0].clone();
    assert!(g.first_t.is_some() && g.last_t >= g.first_t);

    let all = sink.read("r", g.gen, 0, 1 << 20).unwrap();
    assert!(!all.gone && all.eof);
    assert_eq!(records(&all.data).len(), 3);
    assert_eq!(all.next, g.bytes);

    // A chunk that ends inside a line stops at the last complete line.
    let first_len = all.data.find('\n').unwrap() + 1;
    let part = sink.read("r", g.gen, 0, first_len + 10).unwrap();
    assert_eq!(part.data.len(), first_len);
    assert_eq!(part.next, first_len as u64);
    assert!(!part.eof);

    // A partial last line (a write in progress) is left for the next read.
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("r.ndjson"))
            .unwrap();
        f.write_all(b"{\"partial\":").unwrap();
    }
    let rest = sink.read("r", g.gen, all.next, 1 << 20).unwrap();
    assert_eq!(rest.data, "");
    assert_eq!(rest.next, all.next);
    assert!(!rest.eof);
    assert!(sink.shutdown(WAIT));

    // A fresh sink on the same directory: rotate the first generation to `.1`, then away.
    let dir2 = tempfile::tempdir().unwrap();
    let mut cfg = config(dir2.path(), "r.ndjson");
    cfg.max_bytes = 600;
    cfg.generations = 1;
    let sink = Sink::start(cfg).unwrap();
    sink.emit(&T::N { worker: 2, i: 0 });
    assert!(sink.flush(WAIT));
    let g0 = sink.sources()[0].gens[0].gen;
    let c0 = sink.read("r", g0, 0, 1 << 20).unwrap();
    let mut i = 1;
    while sink.sources()[0].gens.last().unwrap().gen == g0 {
        sink.emit(&T::N { worker: 2, i });
        i += 1;
        assert!(sink.flush(WAIT));
    }
    // Rotated to `.1`, still the same generation: reading on from where we were works.
    let c1 = sink.read("r", g0, c0.next, 1 << 20).unwrap();
    assert!(!c1.gone);
    while sink.sources()[0].gens[0].gen == g0 {
        sink.emit(&T::N { worker: 2, i });
        i += 1;
        assert!(sink.flush(WAIT));
    }
    let gone = sink.read("r", g0, 0, 1 << 20).unwrap();
    assert!(gone.gone && gone.data.is_empty());
    assert!(sink.sources()[0].gens.len() <= 2);
    assert!(sink.read("../x", 1, 0, 10).is_err());
    assert!(sink.shutdown(WAIT));
}

#[test]
fn budget_suppresses_and_summarizes() {
    const EMITTED: u64 = 50;
    let mem = Mem::default();
    let sink = Sink::start_with(
        config(std::path::Path::new("unused"), "b.ndjson"),
        mem.clone(),
    )
    .unwrap();
    for i in 0..EMITTED {
        sink.emit(&T::Limited { i });
    }
    assert!(sink.shutdown(WAIT));
    let recs = records(&mem.text("b.ndjson"));
    let written = recs.iter().filter(|r| ev(r) == "test.limited").count() as u64;
    let summarized: u64 = recs
        .iter()
        .filter(|r| ev(r) == "diag.suppressed")
        .map(|r| {
            assert_eq!(r["event"], "test.limited");
            r["count"].as_u64().unwrap()
        })
        .sum();
    // At most 5 per minute; a minute boundary during the test may admit 5 more.
    assert!((5..=10).contains(&written), "{written}");
    assert_eq!(written + summarized, EMITTED);
    assert_eq!(sink.stats().suppressed, summarized);
}

#[test]
fn promote_switches_files_at_a_record_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let sink = Sink::start(config(dir.path(), "aux.ndjson")).unwrap();
    sink.emit(&T::N { worker: 0, i: 1 });
    sink.promote("daemon.ndjson").unwrap();
    assert!(sink.promote("../escape.ndjson").is_err());
    sink.emit(&T::N { worker: 0, i: 2 });
    assert!(sink.shutdown(WAIT));
    let aux = file_records(dir.path(), "aux.ndjson");
    let daemon = file_records(dir.path(), "daemon.ndjson");
    assert_eq!(aux.len(), 1);
    assert_eq!(aux[0]["i"], 1);
    assert_eq!(daemon.len(), 1);
    assert_eq!(daemon[0]["i"], 2);
    let names: Vec<String> = sink.sources().into_iter().map(|s| s.name).collect();
    assert_eq!(names, ["aux", "daemon"]);
}

#[test]
fn ring_and_tail_carry_filter_keys() {
    let sink = Sink::start_with(
        config(std::path::Path::new("unused"), "k.ndjson"),
        Mem::default(),
    )
    .unwrap();
    sink.emit(&T::Keyed {
        host: Ident::st("h1"),
        session: Some(Ident::st("s1")),
        trace: None,
        i: 1,
    });
    let (ring, mut rx) = sink.tail();
    assert_eq!(ring.len(), 1);
    sink.emit(&T::Keyed {
        host: Ident::st("h2"),
        session: None,
        trace: Some(Ident::st("u-0011223344556677")),
        i: 2,
    });
    let next = rx.try_recv().unwrap();
    assert_eq!(next.seq, ring[0].seq + 1);
    assert_eq!(next.host.as_deref(), Some("h2"));
    assert_eq!(next.trace.as_deref(), Some("u-0011223344556677"));

    let host1 = Filter {
        host: Some("h1".into()),
        ..Filter::default()
    };
    assert!(host1.matches(&ring[0]) && !host1.matches(&next));
    let traced = Filter {
        traces: vec!["u-0011223344556677".into()],
        ..Filter::default()
    };
    assert!(traced.matches(&next) && !traced.matches(&ring[0]));
    let prefix = Filter {
        ev_prefix: Some("test.".into()),
        session: Some("s1".into()),
        ..Filter::default()
    };
    assert!(prefix.matches(&ring[0]) && !prefix.matches(&next));
    assert!(sink.shutdown(WAIT));
}

#[test]
fn write_failures_are_reported_once_and_records_kept() {
    let mem = Mem::failing_opens(2);
    let sink = Sink::start_with(
        config(std::path::Path::new("unused"), "f.ndjson"),
        mem.clone(),
    )
    .unwrap();
    sink.emit(&T::N { worker: 0, i: 7 });
    assert!(sink.shutdown(WAIT));
    let recs = records(&mem.text("f.ndjson"));
    let failed: Vec<_> = recs
        .iter()
        .filter(|r| ev(r) == "diag.write_failed")
        .collect();
    assert_eq!(failed.len(), 1, "{recs:?}");
    assert_eq!(failed[0]["error"]["code"], "permission_denied");
    assert_eq!(failed[0]["lvl"], "error");
    assert!(recs.iter().any(|r| ev(r) == "test.n" && r["i"] == 7));
    assert_eq!(sink.stats().written, 1);
}

#[test]
fn after_shutdown_emit_is_ignored() {
    let mem = Mem::default();
    let sink = Sink::start_with(
        config(std::path::Path::new("unused"), "s.ndjson"),
        mem.clone(),
    )
    .unwrap();
    sink.emit(&T::N { worker: 0, i: 0 });
    assert!(sink.shutdown(WAIT));
    sink.emit(&T::N { worker: 0, i: 1 });
    assert_eq!(sink.stats().emitted, 1);
    assert_eq!(records(&mem.text("s.ndjson")).len(), 1);
}
