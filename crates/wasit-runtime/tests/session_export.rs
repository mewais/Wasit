//! `session.export` (B5b): the conversation export of a session's derived event log.
//!
//! Sessions are recorded with the fake CLI; their derived logs are then extended with crafted
//! records (images, long tool outputs, secrets, subagent events) that the export must treat as
//! specified. Exports run through a fresh manager, so the log is read as a reopened session's
//! would be. The work counters are process-wide, so the tests of this file run one at a time.

mod common;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use base64::Engine;
use common::*;
use serde_json::{json, Value};
use wasit_core::Command;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::stats::counters;
use wasit_runtime::wire::{
    ExportChunk, ExportEvent, ExportFile, ExportFormat, ExportReadRequest, ExportRequest,
    SessionEvent, SessionId, MAX_EXPORT_READ,
};
use wasit_runtime::{EventStream, SessionManager, StreamEnd};

/// Serializes the tests of this file (see the module docs).
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Records one turn per prompt, then closes the session's log; returns the session's id.
async fn record(d: &Path, prompts: &[&str]) -> SessionId {
    let mgr = manager(d).await;
    let mut scenario = Scenario::builder();
    for _ in prompts {
        scenario = scenario.turn(Turn::builder().say("answer"));
    }
    let sc = scenario.build();
    let id = mgr.create(create(d, "codex", &sc)).await.unwrap().id;
    wait_phase(&mgr, &id, "idle").await;
    for (i, prompt) in prompts.iter().enumerate() {
        mgr.command(&id, Command::prompt(*prompt)).await.unwrap();
        wait_snapshot(&mgr, &id, "the turn", |s| {
            s.turns_completed as usize == i + 1
        })
        .await;
    }
    mgr.stop(&id).await.unwrap();
    mgr.close_log(&id).await.unwrap();
    id
}

/// A fresh manager over the directory, with its sessions adopted (none is started).
async fn reopened(d: &Path) -> SessionManager {
    let mgr = manager(d).await;
    mgr.adopt_all().await.unwrap();
    mgr
}

fn session_dir(d: &Path, id: &SessionId) -> PathBuf {
    d.join("sessions").join(id.as_str())
}

fn derived(d: &Path, id: &SessionId, file: &str) -> PathBuf {
    session_dir(d, id).join("derived").join(file)
}

fn exports_dir(d: &Path, id: &SessionId) -> PathBuf {
    session_dir(d, id).join("exports")
}

/// The non-empty lines of a file, parsed as JSON.
fn json_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Appends raw record lines to a derived log.
fn append_lines(path: &Path, lines: &[Value]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    for v in lines {
        let mut line = serde_json::to_vec(v).unwrap();
        line.push(b'\n');
        f.write_all(&line).unwrap();
    }
}

/// The cursor of the last record of a derived log.
fn last_cursor(path: &Path) -> u64 {
    json_lines(path).last().unwrap()["cursor"].as_u64().unwrap()
}

/// An agent record after the log's last cursor, numbered `n`-th after it.
fn agent_at(path: &Path, n: u64, event: Value) -> Value {
    let cursor = last_cursor(path) + n;
    json!({"cursor": cursor, "t": cursor * 10, "kind": "agent", "event": event})
}

fn request(id: &SessionId, formats: &[ExportFormat]) -> ExportRequest {
    ExportRequest {
        session: id.clone(),
        formats: formats.to_vec(),
        include_subagents: false,
        include_earlier: true,
        tool_output_cap: 64 << 10,
    }
}

/// Every event of an export, through its terminal one.
async fn collect(mgr: &SessionManager, export_id: &str) -> Vec<ExportEvent> {
    let mut s = mgr.export_subscribe(export_id, None).unwrap();
    let mut out = Vec::new();
    loop {
        let (_, ev) = next_item(&mut s).await;
        let terminal = matches!(ev, ExportEvent::Done { .. } | ExportEvent::Failed { .. });
        out.push(ev);
        if terminal {
            return out;
        }
    }
}

async fn next_item(s: &mut EventStream<(u64, ExportEvent)>) -> (u64, ExportEvent) {
    tokio::time::timeout(T, s.next())
        .await
        .expect("the export timed out")
        .expect("the progress topic ended before a terminal event")
        .unwrap_or_else(|e: StreamEnd| panic!("the progress topic failed: {e}"))
}

/// Starts an export and waits for it to finish; returns its id and every event.
async fn export(mgr: &SessionManager, req: ExportRequest) -> (String, Vec<ExportEvent>) {
    let started = mgr.export_start(req).await.unwrap();
    let events = collect(mgr, &started.export_id).await;
    (started.export_id, events)
}

/// The files of a finished export, and its redaction counts.
fn outcome(events: &[ExportEvent]) -> (Vec<ExportFile>, BTreeMap<String, u64>) {
    match events.last().unwrap() {
        ExportEvent::Done { files, redactions } => (files.clone(), redactions.clone()),
        other => panic!("the export did not finish: {other:?}"),
    }
}

/// The path of the file of `format` in a finished export.
fn file_of(events: &[ExportEvent], format: ExportFormat) -> PathBuf {
    let (files, _) = outcome(events);
    let f = files
        .iter()
        .find(|f| f.format == format)
        .unwrap_or_else(|| panic!("no {format:?} file in {files:?}"));
    PathBuf::from(&f.path)
}

#[tokio::test]
async fn json_export_reparses_to_the_derived_events() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["first prompt", "second prompt"]).await;
    let mgr = reopened(d.path()).await;
    let (_, events) = export(&mgr, request(&id, &[ExportFormat::Json])).await;

    let file = file_of(&events, ExportFormat::Json);
    assert_eq!(file.parent().unwrap(), exports_dir(d.path(), &id));
    assert_eq!(
        file.extension().unwrap(),
        "ndjson",
        "the JSON file is NDJSON"
    );
    let lines = json_lines(&file);
    assert_eq!(lines[0]["format"], 1);
    assert_eq!(lines[0]["redaction"], 1);
    assert_eq!(lines[0]["session"]["id"], id.as_str());
    assert_eq!(lines[0]["session"]["driver"], "codex");
    assert!(lines[0]["session"]["created"].is_u64());

    // Every line after the manifest is one of the derived log's records, in the same order,
    // and each parses back as a session event.
    // The export writes each record as the runtime's event type serializes it, so the
    // comparison is with the record parsed as that type and serialized again (a float field
    // such as `context_pct` is written in that canonical form, not as the log has it).
    let derived_lines = json_lines(&derived(d.path(), &id, "events.ndjson"));
    assert!(
        derived_lines.len() >= 4,
        "the recorded turns are in the log"
    );
    let canonical: Vec<Value> = derived_lines
        .iter()
        .map(|l| {
            let e: SessionEvent = serde_json::from_value(l.clone()).unwrap();
            serde_json::to_value(e).unwrap()
        })
        .collect();
    assert_eq!(&lines[1..], &canonical[..]);
    for l in &lines[1..] {
        serde_json::from_value::<SessionEvent>(l.clone()).unwrap();
    }

    let (files, redactions) = outcome(&events);
    assert!(redactions.is_empty(), "nothing to redact: {redactions:?}");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].bytes, std::fs::metadata(&file).unwrap().len());
    assert_no_temporaries(&exports_dir(d.path(), &id));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the export is owner-only");
    }
}

fn assert_no_temporaries(dir: &Path) {
    let names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".tmp"))
        .collect();
    assert!(names.is_empty(), "temporary files left: {names:?}");
}

#[tokio::test]
async fn markdown_export_has_every_prompt_once_in_order() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let prompts = ["alpha prompt", "beta prompt", "gamma prompt"];
    let id = record(d.path(), &prompts).await;
    let mgr = reopened(d.path()).await;
    let (_, events) = export(&mgr, request(&id, &[ExportFormat::Markdown])).await;

    let text = std::fs::read_to_string(file_of(&events, ExportFormat::Markdown)).unwrap();
    // Each prompt is one `## User` block; the session title (taken from the first prompt) also
    // names it in the header, so the blocks are what is counted.
    let positions: Vec<usize> = prompts
        .iter()
        .map(|p| {
            let block = format!("## User\n\n{p}\n\n");
            assert_eq!(text.matches(&block).count(), 1, "{p} is one block: {text}");
            text.find(&block).unwrap()
        })
        .collect();
    assert!(
        positions.windows(2).all(|w| w[0] < w[1]),
        "prompts in order: {positions:?}"
    );
    assert!(text.contains("best-effort"), "the header notes redaction");
    assert!(
        text.contains("conversation"),
        "the header notes the content"
    );
}

#[tokio::test]
async fn markdown_covers_reasoning_content_and_earlier_history() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["live prompt"]).await;
    let log = derived(d.path(), &id, "events.ndjson");
    // The earlier history of the conversation precedes the session's own events.
    let earlier = derived(d.path(), &id, "earlier.ndjson");
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&earlier)
        .unwrap();
    append_lines(
        &earlier,
        &[json!({"cursor": 1, "t": 10, "kind": "agent", "event": {
            "type": "history", "event": {"type": "user_message", "text": "earlier prompt"}}})],
    );
    append_lines(
        &log,
        &[
            agent_at(
                &log,
                1,
                json!({"type": "thinking_delta", "text": "pondering <b>this</b>"}),
            ),
            agent_at(
                &log,
                2,
                json!({"type": "assistant_content", "parts": [
                    {"type": "text", "text": "standalone reply"}]}),
            ),
        ],
    );

    let mgr = reopened(d.path()).await;
    let (_, events) = export(&mgr, request(&id, &[ExportFormat::Markdown])).await;
    let text = std::fs::read_to_string(file_of(&events, ExportFormat::Markdown)).unwrap();
    let earlier_at = text
        .find("## User\n\nearlier prompt\n\n")
        .expect("earlier history");
    let live_at = text
        .find("## User\n\nlive prompt\n\n")
        .expect("live prompt");
    assert!(earlier_at < live_at, "earlier history comes first");
    assert!(text.contains("<details><summary>Thinking</summary>"));
    assert!(
        text.contains("pondering &lt;b>this&lt;/b>"),
        "tags in prose are escaped"
    );
    assert_eq!(
        text.matches("standalone reply").count(),
        1,
        "the final message is written once"
    );

    let mut without = request(&id, &[ExportFormat::Markdown]);
    without.include_earlier = false;
    let (_, live_only) = export(&mgr, without).await;
    let text = std::fs::read_to_string(file_of(&live_only, ExportFormat::Markdown)).unwrap();
    assert!(!text.contains("earlier prompt"), "earlier history left out");
    assert!(text.contains("live prompt"));
}

#[tokio::test]
async fn images_are_omitted_and_tool_output_is_capped() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["look"]).await;
    let log = derived(d.path(), &id, "events.ndjson");
    let long = "z".repeat(100_000);
    append_lines(
        &log,
        &[
            agent_at(
                &log,
                1,
                json!({"type": "user_content", "parts": [
                    {"type": "text", "text": "see the picture"},
                    {"type": "image", "mime": "image/png",
                     "source": {"source": "blob", "id": "ab", "size": 3}}
                ]}),
            ),
            agent_at(
                &log,
                2,
                json!({"type": "tool_call_finished", "call_id": "c1", "name": "bash",
                       "ok": true, "result": long}),
            ),
        ],
    );
    let mgr = reopened(d.path()).await;
    let (_, events) = export(&mgr, request(&id, &[ExportFormat::Json])).await;
    let lines = json_lines(&file_of(&events, ExportFormat::Json));

    let user = lines
        .iter()
        .find(|l| l["event"]["type"] == "user_content")
        .expect("the user content is exported");
    assert_eq!(
        user["event"]["parts"][1],
        json!({"type": "image", "mime": "image/png", "omitted": true})
    );
    assert_eq!(user["event"]["parts"][0]["text"], "see the picture");

    let finished = lines
        .iter()
        .find(|l| l["event"]["type"] == "tool_call_finished")
        .expect("the tool result is exported");
    let result = finished["event"]["result"].as_str().unwrap();
    assert!(result.starts_with(&"z".repeat(65_536)));
    assert!(!result.contains(&"z".repeat(65_537)));
    assert!(
        result.contains("showing 65536 of 100000 bytes"),
        "{result:.80}"
    );
}

#[tokio::test]
async fn redaction_counts_are_reported_and_secrets_are_absent() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["hello"]).await;
    let log = derived(d.path(), &id, "events.ndjson");
    append_lines(
        &log,
        &[
            agent_at(
                &log,
                1,
                json!({"type": "assistant_delta",
                       "text": "key sk-proj-SECRETONE0123456789abcdefghij end"}),
            ),
            json!({"cursor": last_cursor(&log) + 2, "t": 1, "kind": "notice",
                   "message": "password: hunter2hunter2"}),
            agent_at(
                &log,
                3,
                json!({"type": "tool_call_finished", "call_id": "c2", "name": "curl",
                       "ok": true, "result": "Bearer SECRETTWO0123456789abcdefghij"}),
            ),
        ],
    );
    let mgr = reopened(d.path()).await;
    let (_, events) = export(
        &mgr,
        request(&id, &[ExportFormat::Json, ExportFormat::Markdown]),
    )
    .await;

    for format in [ExportFormat::Json, ExportFormat::Markdown] {
        let text = std::fs::read_to_string(file_of(&events, format)).unwrap();
        for secret in ["SECRETONE", "SECRETTWO", "hunter2hunter2"] {
            assert!(!text.contains(secret), "{secret} leaked into {format:?}");
        }
        assert!(text.contains("<redacted:"), "markers stand in for secrets");
    }
    let (_, redactions) = outcome(&events);
    assert!(
        redactions.get("sk_key").copied().unwrap_or(0) >= 1,
        "{redactions:?}"
    );
    assert!(
        redactions.get("bearer").copied().unwrap_or(0) >= 1,
        "{redactions:?}"
    );
    assert!(
        redactions.get("credential").copied().unwrap_or(0) >= 1,
        "{redactions:?}"
    );
}

#[tokio::test]
async fn subagent_events_are_left_out_unless_asked_for() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["hello"]).await;
    let log = derived(d.path(), &id, "events.ndjson");
    append_lines(
        &log,
        &[agent_at(
            &log,
            1,
            json!({"type": "scoped", "scope": {"call_id": "c9"},
                   "event": {"type": "assistant_delta", "text": "subagent words"}}),
        )],
    );
    let mgr = reopened(d.path()).await;
    let (_, main) = export(&mgr, request(&id, &[ExportFormat::Json])).await;
    let text = std::fs::read_to_string(file_of(&main, ExportFormat::Json)).unwrap();
    assert!(!text.contains("subagent words"));

    let mut with = request(&id, &[ExportFormat::Json]);
    with.include_subagents = true;
    let (_, all) = export(&mgr, with).await;
    let text = std::fs::read_to_string(file_of(&all, ExportFormat::Json)).unwrap();
    assert!(text.contains("subagent words"));
}

#[tokio::test]
async fn cancel_leaves_no_file_and_one_export_runs_per_session() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["hello"]).await;
    let log = derived(d.path(), &id, "events.ndjson");
    // Enough records that the export is still running when it is cancelled.
    let first = last_cursor(&log) + 1;
    let mut text = String::new();
    for c in first..first + 200_000 {
        text.push_str(&format!(
            "{{\"cursor\":{c},\"t\":{c},\"kind\":\"notice\",\"message\":\"filler {c}\"}}\n"
        ));
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(&log)
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();

    let mgr = reopened(d.path()).await;
    let started = mgr
        .export_start(request(&id, &[ExportFormat::Json]))
        .await
        .unwrap();
    let mut s = mgr.export_subscribe(&started.export_id, None).unwrap();
    loop {
        if let (_, ExportEvent::Progress { .. }) = next_item(&mut s).await {
            break;
        }
    }
    let busy = mgr
        .export_start(request(&id, &[ExportFormat::Json]))
        .await
        .unwrap_err();
    assert_eq!(
        busy.code(),
        "busy",
        "a second export waits its turn: {busy}"
    );

    mgr.export_cancel(&started.export_id).unwrap();
    let last = loop {
        let (_, ev) = next_item(&mut s).await;
        if !matches!(ev, ExportEvent::Progress { .. }) {
            break ev;
        }
    };
    match last {
        ExportEvent::Failed { reason } => assert_eq!(reason, "cancelled"),
        other => panic!("the cancelled export finished instead: {other:?}"),
    }
    let dir = exports_dir(d.path(), &id);
    let left: Vec<String> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    assert!(left.is_empty(), "a cancelled export left files: {left:?}");
}

#[tokio::test]
async fn export_starts_no_actor_and_reads_no_journal() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["hello", "again"]).await;
    let mgr = reopened(d.path()).await;
    let before = counters();
    let (_, events) = export(
        &mgr,
        request(&id, &[ExportFormat::Json, ExportFormat::Markdown]),
    )
    .await;
    let now = counters();
    outcome(&events);
    assert_eq!(
        now.actors_started, before.actors_started,
        "an actor started"
    );
    assert_eq!(
        now.journal_records_replayed, before.journal_records_replayed,
        "the journal was replayed"
    );
}

#[tokio::test]
async fn read_returns_the_file_bytes_in_chunks() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["hello", "again"]).await;
    let mgr = reopened(d.path()).await;
    let (export_id, events) = export(
        &mgr,
        request(&id, &[ExportFormat::Json, ExportFormat::Markdown]),
    )
    .await;

    for format in [ExportFormat::Json, ExportFormat::Markdown] {
        let want = std::fs::read(file_of(&events, format)).unwrap();
        let mut got = Vec::new();
        loop {
            let c: ExportChunk = mgr
                .export_read(ExportReadRequest {
                    export_id: export_id.clone(),
                    format: Some(format),
                    offset: got.len() as u64,
                    len: 7,
                })
                .await
                .unwrap();
            assert_eq!(c.total, want.len() as u64);
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&c.data)
                .unwrap();
            assert!(bytes.len() <= 7);
            got.extend_from_slice(&bytes);
            if c.eof {
                break;
            }
            assert_eq!(bytes.len(), 7, "a chunk before the end is full");
        }
        assert_eq!(
            got, want,
            "the chunks are the file, byte for byte ({format:?})"
        );
    }

    let total = std::fs::metadata(file_of(&events, ExportFormat::Json))
        .unwrap()
        .len();
    let read = |offset, len, format| ExportReadRequest {
        export_id: export_id.clone(),
        format,
        offset,
        len,
    };
    let at_end = mgr
        .export_read(read(total, 10, Some(ExportFormat::Json)))
        .await
        .unwrap();
    assert!(at_end.eof && at_end.data.is_empty());
    assert!(mgr
        .export_read(read(total + 1, 10, Some(ExportFormat::Json)))
        .await
        .is_err());
    assert!(mgr
        .export_read(read(0, 0, Some(ExportFormat::Json)))
        .await
        .is_err());
    assert!(mgr
        .export_read(read(0, MAX_EXPORT_READ + 1, Some(ExportFormat::Json)))
        .await
        .is_err());
    assert!(
        mgr.export_read(read(0, 10, None)).await.is_err(),
        "two files: the format is needed"
    );

    // Cancelling a finished export changes nothing.
    mgr.export_cancel(&export_id).unwrap();
    let still = std::fs::metadata(file_of(&events, ExportFormat::Json)).unwrap();
    assert_eq!(still.len(), total);
}

#[tokio::test]
async fn invalid_requests_and_unknown_sessions_are_refused() {
    let _serial = SERIAL.lock().await;
    let d = TempDir::new();
    let id = record(d.path(), &["hello"]).await;
    let mgr = reopened(d.path()).await;
    let refused = |req: ExportRequest| {
        let mgr = mgr.clone();
        async move { mgr.export_start(req).await.is_err() }
    };
    assert!(refused(request(&id, &[])).await, "no format");
    assert!(
        refused(request(&id, &[ExportFormat::Json, ExportFormat::Json])).await,
        "a format twice"
    );
    let mut zero = request(&id, &[ExportFormat::Json]);
    zero.tool_output_cap = 0;
    assert!(refused(zero).await, "a zero cap");
    let mut huge = request(&id, &[ExportFormat::Json]);
    huge.tool_output_cap = u32::MAX;
    assert!(refused(huge).await, "a cap past the maximum");
    let stranger = SessionId::parse(&"z".repeat(SessionId::LEN)).unwrap();
    assert!(refused(request(&stranger, &[ExportFormat::Json])).await);
    assert!(mgr.export_subscribe("no-such-export", None).is_err());
}
