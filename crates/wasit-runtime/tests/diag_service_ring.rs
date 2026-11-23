//! `DiagService` over a sink the test owns (the service's process is the test's own, so the
//! sink can be small): a generation rotated away is `gone`, a cursor the ring no longer holds
//! starts with a reset (and the one just inside it resumes), a subscriber that lags is closed
//! and resumes without gap or duplicate. One test function: the sink is process-wide.
#![cfg(unix)]

mod common;

use std::sync::Arc;

use common::*;
use wasit_control::{Cursor, ServiceMux};
use wasit_diag::{Code, Ident, SinkConfig};
use wasit_runtime::wire::{DiagTailEvent, DiagTailParams, DIAG_ROUTES};
use wasit_runtime::{DiagService, RuntimeClient, TypedSubscription};

wasit_diag::diag_events! {
    enum Ev {
        #[diag(ev = "t.tick", budget = 1000000)]
        Tick { n: u64, session: Ident },
    }
}

const FILE: &str = "ring.ndjson";
/// Small, so that a few hundred records rotate the file many times.
const MAX_BYTES: u64 = 8192;
const GENERATIONS: u32 = 2;

fn tick(n: u64, session: &str) {
    wasit_diag::emit(&Ev::Tick {
        n,
        session: Ident::new(session),
    });
}

async fn next(sub: &mut TypedSubscription<DiagTailEvent>) -> (Cursor, DiagTailEvent) {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok(x))) => x,
        other => panic!(
            "no tail event: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

fn seq_of(line: &str) -> u64 {
    serde_json::from_str::<serde_json::Value>(line).unwrap()["seq"]
        .as_u64()
        .unwrap()
}

fn reset(ev: DiagTailEvent) -> Vec<String> {
    match ev {
        DiagTailEvent::Reset { records } => records,
        other => panic!("not a reset: {other:?}"),
    }
}

fn record(ev: DiagTailEvent) -> String {
    match ev {
        DiagTailEvent::Record { line } => line,
        other => panic!("not a record: {other:?}"),
    }
}

async fn service(dir: &std::path::Path) -> RuntimeClient {
    let svc = DiagService::new(manager(dir).await);
    let mux = ServiceMux::new()
        .with(DIAG_ROUTES.iter().copied(), Arc::new(svc))
        .unwrap();
    loopback(Arc::new(mux))
}

#[tokio::test(flavor = "current_thread")]
async fn rotation_the_ring_and_lag() {
    let d = TempDir::new();
    let mut cfg = SinkConfig::new(
        Code::st("daemon"),
        Ident::st("test"),
        d.path().join("diag"),
        FILE,
    );
    cfg.max_bytes = MAX_BYTES;
    cfg.generations = GENERATIONS;
    wasit_diag::install(cfg).unwrap();
    let c = service(d.path()).await;
    let boot = wasit_diag::boot().unwrap();

    // ── rotation ────────────────────────────────────────────────────────────
    tick(0, "rotation");
    assert!(wasit_diag::flush(T));
    let first = c.diag_sources().await.unwrap();
    assert_eq!(first.boot, boot);
    let name = FILE.strip_suffix(".ndjson").unwrap();
    let gens = &first.sources.iter().find(|s| s.name == name).unwrap().gens;
    assert_eq!(gens.len(), 1);
    let oldest = gens[0].gen;
    let text = c.diag_read(name, oldest, 0, 1 << 20).await.unwrap();
    assert!(!text.gone && text.data.contains("\"ev\":\"t.tick\""));

    for n in 1..=400 {
        tick(n, "rotation");
    }
    assert!(wasit_diag::flush(T));
    let s = c.diag_sources().await.unwrap();
    let gens = &s.sources.iter().find(|s| s.name == name).unwrap().gens;
    // The newest file and the older ones kept, oldest first; every file within its bound.
    assert_eq!(gens.len(), GENERATIONS as usize + 1, "{gens:?}");
    assert!(
        gens.windows(2).all(|w| w[0].gen + 1 == w[1].gen),
        "{gens:?}"
    );
    assert!(
        gens[0].gen > oldest + 1,
        "the file was not rotated away: {gens:?}"
    );
    assert!(
        gens.iter()
            .all(|g| g.bytes <= MAX_BYTES && g.first_t <= g.last_t),
        "{gens:?}"
    );
    // The generation read before is gone; the ones listed are there, whole lines, in order.
    let gone = c.diag_read(name, oldest, 0, 1 << 20).await.unwrap();
    assert!(
        gone.gone && gone.eof && gone.data.is_empty() && gone.next == 0,
        "{gone:?}"
    );
    let mut last_n = None;
    for g in gens {
        let mut offset = 0;
        loop {
            let chunk = c.diag_read(name, g.gen, offset, 2048).await.unwrap();
            assert!(!chunk.gone);
            for l in chunk.data.lines() {
                let v: serde_json::Value = serde_json::from_str(l).unwrap();
                if v["ev"] == "t.tick" {
                    let n = v["n"].as_u64().unwrap();
                    assert!(last_n.is_none_or(|p| n == p + 1), "{n} after {last_n:?}");
                    last_n = Some(n);
                }
            }
            offset = chunk.next;
            if chunk.eof {
                break;
            }
        }
    }
    assert_eq!(
        last_n,
        Some(400),
        "the newest generation ends with the last record"
    );

    // ── the ring ────────────────────────────────────────────────────────────
    // More records than the ring holds. A cursor at the edge of it resumes; one past it resets.
    for n in 0..5000 {
        tick(n, "ring");
    }
    let params = DiagTailParams::default();
    let mut sub = c.subscribe_diag_tail(&params, None);
    let (cursor, ev) = next(&mut sub).await;
    let records = reset(ev);
    assert_eq!(records.len(), wasit_diag::RING);
    let (first_seq, last_seq) = (seq_of(&records[0]), seq_of(records.last().unwrap()));
    assert_eq!(last_seq - first_seq + 1, wasit_diag::RING as u64);
    assert_eq!(cursor.as_str(), format!("{boot}.{last_seq}"));
    drop(sub);

    let at_edge = Cursor::new(format!("{boot}.{}", first_seq - 1));
    let mut sub = c.subscribe_diag_tail(&params, Some(at_edge));
    let (cursor, ev) = next(&mut sub).await;
    assert_eq!(
        seq_of(&record(ev)),
        first_seq,
        "the ring still holds everything after the cursor"
    );
    assert_eq!(cursor.as_str(), format!("{boot}.{first_seq}"));
    for want in first_seq + 1..=last_seq {
        assert_eq!(seq_of(&record(next(&mut sub).await.1)), want);
    }
    drop(sub);

    let expired = Cursor::new(format!("{boot}.{}", first_seq - 2));
    let mut sub = c.subscribe_diag_tail(&params, Some(expired));
    let records = reset(next(&mut sub).await.1);
    assert_eq!(seq_of(&records[0]), first_seq);
    assert_eq!(records.len(), wasit_diag::RING);
    drop(sub);

    // A reset holds the matching records of the ring only: one record of this session among
    // thousands.
    let params_ring = DiagTailParams {
        session: Some(wasit_runtime::wire::SessionId("aaaaaaaaaaaa".into())),
        ..DiagTailParams::default()
    };
    tick(0, "aaaaaaaaaaaa");
    let mut sub = c.subscribe_diag_tail(&params_ring, None);
    let records = reset(next(&mut sub).await.1);
    assert_eq!(records.len(), 1, "{records:?}");
    drop(sub);

    // ── lag ─────────────────────────────────────────────────────────────────
    // On a single thread the service's stream cannot run while this loop emits: its receiver
    // falls further behind than the tail's queue holds, so it is closed as lagged and the
    // client's durable subscription resumes from its last cursor. Everything arrives once.
    let mut sub = c.subscribe_diag_tail(&params, None);
    let mut want = seq_of(reset(next(&mut sub).await.1).last().unwrap()) + 1;
    assert_eq!(sub.subscribe_count(), 1);
    const BURST: u64 = 3000;
    for n in 0..BURST {
        tick(n, "burst");
    }
    let end = want + BURST;
    while want < end {
        let line = record(next(&mut sub).await.1);
        assert_eq!(seq_of(&line), want, "a gap or a duplicate after a lag");
        want += 1;
    }
    assert!(sub.subscribe_count() >= 2, "the subscriber never lagged");
}
