//! Channel B against the recorded Kilo 7.8.8 fixtures (`wasit-fake-cli/reference/kilo`) and a
//! scripted fake HTTP/SSE server. The server answers one connection per scripted reply, in order;
//! the tests use no sleeps and no retries.

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use serde_json::{json, Value};

use super::*;

fn fixture(name: &str) -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../wasit-fake-cli/reference/kilo/"
    );
    std::fs::read_to_string(format!("{path}{name}"))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

/// The SSE `payload` objects of a recording, in order.
fn sse_payloads(scenario: &str) -> Vec<Value> {
    fixture(&format!("{scenario}.sse.jsonl"))
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["dir"] == "sse")
        .filter_map(|v| v["msg"].get("payload").cloned())
        .collect()
}

fn payload_of_type<'a>(payloads: &'a [Value], kind: &str) -> Vec<&'a Value> {
    payloads.iter().filter(|p| p["type"] == kind).collect()
}

/// The recorded HTTP exchanges of a scenario whose path starts with `prefix`.
fn http_bodies(scenario: &str, prefix: &str) -> Vec<Value> {
    let v: Value = serde_json::from_str(&fixture(&format!("{scenario}.http.json"))).unwrap();
    v["http"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|h| h["path"].as_str().is_some_and(|p| p.starts_with(prefix)))
        .map(|h| h["body"].clone())
        .collect()
}

fn parent_of_child(payloads: &[Value]) -> (String, String) {
    let created = payload_of_type(payloads, "session.created")
        .into_iter()
        .find(|p| p["properties"]["info"].get("parentID").is_some())
        .expect("fixture has a child session.created");
    let info = &created["properties"]["info"];
    (
        info["id"].as_str().unwrap().to_string(),
        info["parentID"].as_str().unwrap().to_string(),
    )
}

/// Keys a notification may carry. Anything else would be content leaking out of the bridge.
const ALLOWED_KEYS: &[&str] = &[
    "kind",
    "session",
    "parent",
    "status",
    "input",
    "output",
    "reasoning",
    "cache_read",
    "cache_write",
    "cost",
    // The assistant message a token report belongs to (an id, not its content).
    "message",
    "id",
    "permission",
    "jobs",
    "reason",
    "command",
    "detail",
];

fn assert_content_free(note: &Value) {
    let obj = note.as_object().expect("notification is an object");
    for k in obj.keys() {
        assert!(
            ALLOWED_KEYS.contains(&k.as_str()),
            "unexpected key {k} in {note}"
        );
    }
    let text = note.to_string();
    for needle in ["patterns", "title", "metadata", "[[", "$PROBE"] {
        assert!(!text.contains(needle), "{needle} leaked into {note}");
    }
}

fn kinds(notes: &[Value]) -> Vec<String> {
    notes
        .iter()
        .map(|n| n["kind"].as_str().unwrap().to_string())
        .collect()
}

fn read_out(out: &Out<Vec<u8>>) -> Vec<Value> {
    let text = out.with(|w| String::from_utf8(w.clone()).unwrap());
    text.lines()
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("bridge line is JSON");
            assert_eq!(v["method"], METHOD);
            v["params"].clone()
        })
        .collect()
}

// ── pure mapping over the recordings ─────────────────────────────────────────

#[test]
fn bg_recording_maps_to_content_free_notifications() {
    let payloads = sse_payloads("subagent_bg");
    let (child, parent) = parent_of_child(&payloads);
    let mut tracker = Tracker::default();
    let mut notes = Vec::new();
    let mut refreshes = Vec::new();
    for p in &payloads {
        let m = tracker.on_event(p);
        notes.extend(m.notes);
        refreshes.extend(m.refresh_jobs_of);
    }
    for n in &notes {
        assert_content_free(n);
    }
    let created = notes
        .iter()
        .find(|n| n["kind"] == "session.created" && n.get("parent").is_some())
        .expect("child creation is reported");
    assert_eq!(created["session"], child.as_str());
    assert_eq!(created["parent"], parent.as_str());

    let child_statuses: Vec<&str> = notes
        .iter()
        .filter(|n| n["kind"] == "session.status" && n["session"] == child.as_str())
        .map(|n| n["status"].as_str().unwrap())
        .collect();
    assert_eq!(child_statuses.first(), Some(&"busy"));
    assert_eq!(child_statuses.last(), Some(&"idle"));
    assert!(notes
        .iter()
        .any(|n| n["kind"] == "session.idle" && n["session"] == child.as_str()));

    let tokens: Vec<&Value> = notes.iter().filter(|n| n["kind"] == "tokens").collect();
    assert!(
        !tokens.is_empty(),
        "assistant message.updated reports tokens"
    );
    assert!(tokens
        .iter()
        .all(|t| t["input"].is_number() && t["cost"].is_number()));
    // Job state is refreshed for the parent whenever the child changes, never otherwise.
    assert!(!refreshes.is_empty());
    assert!(refreshes.iter().all(|r| *r == parent));
}

#[test]
fn unloaded_permission_recording_reports_id_and_tool_only() {
    let payloads = sse_payloads("subagent_permission_unloaded");
    let asked = payload_of_type(&payloads, "permission.asked");
    assert_eq!(asked.len(), 1);
    let pattern = asked[0]["properties"]["patterns"][0]
        .as_str()
        .unwrap()
        .to_string();
    assert!(!pattern.is_empty());
    let note = Tracker::default().on_event(asked[0]).notes.remove(0);
    assert_eq!(note["kind"], "permission.asked");
    assert_eq!(note["id"], asked[0]["properties"]["id"]);
    assert_eq!(note["session"], asked[0]["properties"]["sessionID"]);
    assert_eq!(note["permission"], "bash");
    assert_content_free(&note);
    assert!(!note.to_string().contains(&pattern));
}

#[test]
fn background_jobs_answer_keeps_only_id_type_and_status() {
    let bodies = http_bodies("subagent_bg", "/kilocode/background-jobs");
    let body = bodies
        .iter()
        .find(|b| b.as_array().is_some_and(|a| !a.is_empty()))
        .expect("a recorded job list");
    let note = jobs_note("ses_parent", body).expect("array body");
    assert_eq!(note["kind"], "background_jobs");
    let jobs = note["jobs"].as_array().unwrap();
    assert!(!jobs.is_empty());
    for j in jobs {
        let keys: BTreeSet<&str> = j.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, BTreeSet::from(["id", "status", "type"]));
    }
    assert!(jobs_note("p", &json!({"not": "an array"})).is_none());
}

#[test]
fn only_events_of_a_known_child_refresh_jobs() {
    let mut t = Tracker::default();
    let status = |s: &str| json!({"type": "session.status", "properties": {"sessionID": s, "status": {"type": "busy"}}});
    assert_eq!(t.on_event(&status("ses_unknown")).refresh_jobs_of, None);
    let created = json!({"type": "session.created", "properties": {"sessionID": "ses_c", "info": {"id": "ses_c", "parentID": "ses_p"}}});
    let m = t.on_event(&created);
    assert_eq!(m.refresh_jobs_of.as_deref(), Some("ses_p"));
    assert_eq!(m.notes[0]["parent"], "ses_p");
    assert_eq!(
        t.on_event(&status("ses_c")).refresh_jobs_of.as_deref(),
        Some("ses_p")
    );
}

#[test]
fn exchanges_match_the_recorded_calls() {
    let steer = exchange_for(&Command::Steer {
        session: "ses_c".into(),
        text: "use the other file".into(),
    })
    .unwrap();
    assert_eq!(steer.method, "POST");
    assert_eq!(steer.path, "/session/ses_c/prompt_async");
    assert_eq!(
        steer.body.unwrap(),
        json!({"parts": [{"type": "text", "text": "use the other file",
                          "metadata": {"kind": "subagent_steer"}}]})
    );

    let abort = |scope| {
        exchange_for(&Command::Abort {
            session: "ses_p".into(),
            scope,
        })
        .unwrap()
    };
    assert_eq!(
        abort(Scope::Session).path,
        "/session/ses_p/abort?scope=session"
    );
    assert_eq!(abort(Scope::Tree).path, "/session/ses_p/abort?scope=tree");
    assert_eq!(abort(Scope::Session).body, None);

    let reply = exchange_for(&Command::ReplyPermission {
        id: "per_211105187001vqfRUkWefI6Gtl".into(),
        reply: Reply::Once,
    })
    .unwrap();
    assert_eq!(
        reply.path,
        "/permission/per_211105187001vqfRUkWefI6Gtl/reply"
    );
    assert_eq!(reply.body.unwrap(), json!({"reply": "once"}));
}

#[test]
fn exchanges_refuse_unsafe_ids_and_empty_steers() {
    let bad_id = Command::Abort {
        session: "ses_x/../../etc".into(),
        scope: Scope::Session,
    };
    assert!(exchange_for(&bad_id).is_err());
    let empty = Command::Steer {
        session: "ses_c".into(),
        text: "   ".into(),
    };
    assert!(exchange_for(&empty).is_err());
}

#[test]
fn control_lines_are_told_apart_from_acp_lines() {
    let acp = r#"{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{}}"#;
    assert!(parse_control(acp).is_none());
    let steer = r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"steer","session":"ses_c","text":"hi"}}"#;
    assert_eq!(
        parse_control(steer).unwrap().unwrap(),
        Command::Steer {
            session: "ses_c".into(),
            text: "hi".into()
        }
    );
    let abort = r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"abort","session":"ses_c","scope":"session"}}"#;
    assert_eq!(
        parse_control(abort).unwrap().unwrap(),
        Command::Abort {
            session: "ses_c".into(),
            scope: Scope::Session
        }
    );
    let reply = r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"reply_permission","id":"per_1","reply":"reject"}}"#;
    assert_eq!(
        parse_control(reply).unwrap().unwrap(),
        Command::ReplyPermission {
            id: "per_1".into(),
            reply: Reply::Reject
        }
    );
    // Malformed: rejected with a fixed message that never echoes the text.
    let bad =
        r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"steer","text":"SECRET"}}"#;
    let err = parse_control(bad).unwrap().unwrap_err();
    assert_eq!(err, "invalid control command");
    assert!(!err.contains("SECRET"));
    let unknown = r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"take_over"}}"#;
    assert!(parse_control(unknown).unwrap().is_err());
}

#[test]
fn stdin_forwards_acp_and_takes_control_lines_out() {
    let input = concat!(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"abort","session":"ses_p","scope":"session"}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"command":"nope"}}"#,
        "\n",
    );
    let mut kilo_in: Vec<u8> = Vec::new();
    let (tx, rx) = mpsc::channel();
    let out = Out::new(Vec::new());
    forward_stdin(input.as_bytes(), &mut kilo_in, &tx, &out);
    drop(tx);
    assert_eq!(
        String::from_utf8(kilo_in).unwrap(),
        format!("{}\n", input.lines().next().unwrap())
    );
    let cmds: Vec<Command> = rx.iter().collect();
    assert_eq!(
        cmds,
        vec![Command::Abort {
            session: "ses_p".into(),
            scope: Scope::Session
        }]
    );
    let notes = read_out(&out);
    assert_eq!(kinds(&notes), ["command_failed"]);
    assert_eq!(notes[0]["reason"], "invalid");
}

#[test]
fn only_jsonrpc_responses_open_the_event_stream() {
    assert!(is_response(
        r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1}}"#
    ));
    assert!(is_response(
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1}}"#
    ));
    assert!(!is_response(
        r#"{"jsonrpc":"2.0","method":"session/update","params":{}}"#
    ));
    assert!(!is_response("not json"));
}

// ── against a fake HTTP/SSE server ───────────────────────────────────────────

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    auth: Option<String>,
    body: String,
}

enum Served {
    /// Status and a JSON body with a `Content-Length`.
    Json(u16, String),
    /// A 200 event stream: one chunk per envelope. `clean_end` writes the chunked terminator;
    /// otherwise the server drops the connection mid-body.
    Sse { events: Vec<Value>, clean_end: bool },
}

fn read_request(stream: std::net::TcpStream) -> (Recorded, std::net::TcpStream) {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).unwrap();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap().to_string();
    let path = parts.next().unwrap().to_string();
    let (mut auth, mut len) = (None, 0usize);
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            match k.trim().to_ascii_lowercase().as_str() {
                "authorization" => auth = Some(v.trim().to_string()),
                "content-length" => len = v.trim().parse().unwrap(),
                _ => {}
            }
        }
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).unwrap();
    let stream = reader.into_inner();
    (
        Recorded {
            method,
            path,
            auth,
            body: String::from_utf8(body).unwrap(),
        },
        stream,
    )
}

/// Serve one connection per reply, in order, and return what was requested.
fn serve(replies: Vec<Served>) -> (Endpoint, JoinHandle<Vec<Recorded>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let mut seen = Vec::new();
        for reply in replies {
            let (stream, _) = listener.accept().unwrap();
            let (rec, mut stream) = read_request(stream);
            seen.push(rec);
            match reply {
                Served::Json(status, body) => write!(
                    stream.try_clone().unwrap(),
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap(),
                Served::Sse { events, clean_end } => {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                        )
                        .unwrap();
                    for ev in events {
                        let data = format!("data: {}\n\n", json!({"directory": "d", "project": "global", "payload": ev}));
                        write!(stream, "{:x}\r\n{data}\r\n", data.len()).unwrap();
                    }
                    if clean_end {
                        stream.write_all(b"0\r\n\r\n").unwrap();
                    }
                }
            }
        }
        seen
    });
    (
        Endpoint {
            port,
            basic: Some(("wasit".into(), "pw".into())),
        },
        handle,
    )
}

fn basic(user: &str, pass: &str) -> String {
    format!(
        "Basic {}",
        wasit_core::content::b64_encode(format!("{user}:{pass}").as_bytes())
    )
}

#[test]
fn event_stream_reports_mapped_events_refreshes_jobs_and_reports_loss_on_close() {
    let payloads = sse_payloads("subagent_bg");
    let (child, parent) = parent_of_child(&payloads);
    let child_events: Vec<Value> = payloads
        .iter()
        .filter(|p| {
            p["properties"]["sessionID"] == child.as_str()
                || p["properties"]["info"]["id"] == child.as_str()
        })
        .filter(|p| matches!(p["type"].as_str(), Some("session.created" | "session.idle")))
        .cloned()
        .collect();
    assert_eq!(child_events.len(), 2);
    let jobs = http_bodies("subagent_bg", "/kilocode/background-jobs")
        .into_iter()
        .find(|b| b.as_array().is_some_and(|a| !a.is_empty()))
        .unwrap();

    let (ep, server) = serve(vec![
        Served::Sse {
            events: child_events,
            clean_end: true,
        },
        Served::Json(200, jobs.to_string()),
        Served::Json(200, jobs.to_string()),
    ]);
    let out = Out::new(Vec::new());
    run_events(&ep, &out, &AtomicBool::new(false));
    let seen = server.join().unwrap();

    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/global/event");
    assert_eq!(seen[0].auth, Some(basic("wasit", "pw")));
    assert_eq!(
        seen[1].path,
        format!("/kilocode/background-jobs?sessionID={parent}")
    );
    assert_eq!(seen[2].path, seen[1].path);
    assert!(seen.iter().all(|r| r.auth == Some(basic("wasit", "pw"))));

    let notes = read_out(&out);
    assert_eq!(
        kinds(&notes),
        [
            "session.created",
            "background_jobs",
            "session.idle",
            "background_jobs",
            "state_lost"
        ]
    );
    assert_eq!(notes[4]["reason"], "stream_closed");
    assert_eq!(notes[1]["jobs"][0]["status"], "running");
    for n in &notes {
        assert_content_free(n);
    }
}

#[test]
fn dropped_event_stream_is_reported_as_lost_not_idle() {
    let permission = payload_of_type(
        &sse_payloads("subagent_permission_unloaded"),
        "permission.asked",
    )
    .into_iter()
    .cloned()
    .collect();
    let (ep, server) = serve(vec![Served::Sse {
        events: permission,
        clean_end: false,
    }]);
    let out = Out::new(Vec::new());
    run_events(&ep, &out, &AtomicBool::new(false));
    server.join().unwrap();
    let notes = read_out(&out);
    assert_eq!(kinds(&notes), ["permission.asked", "state_lost"]);
    assert_eq!(notes[1]["reason"], "stream_error");
}

#[test]
fn stream_ending_after_stdin_closed_is_a_normal_shutdown() {
    // The CLI's server goes away once the shim has closed the bridge's stdin: no loss.
    let (ep, server) = serve(vec![Served::Sse {
        events: Vec::new(),
        clean_end: false,
    }]);
    let out = Out::new(Vec::new());
    run_events(&ep, &out, &AtomicBool::new(true));
    server.join().unwrap();
    assert!(read_out(&out).is_empty());
}

#[test]
fn refused_event_stream_is_reported_as_lost_at_connect() {
    let (ep, server) = serve(vec![Served::Json(500, String::new())]);
    let out = Out::new(Vec::new());
    run_events(&ep, &out, &AtomicBool::new(false));
    server.join().unwrap();
    assert_eq!(kinds(&read_out(&out)), ["state_lost"]);
    assert_eq!(read_out(&out)[0]["reason"], "connect");
}

#[test]
fn steer_is_one_prompt_async_with_the_steer_marker() {
    let (ep, server) = serve(vec![Served::Json(204, String::new())]);
    let out = Out::new(Vec::new());
    execute(
        &ep,
        &Command::Steer {
            session: "ses_c".into(),
            text: "look at src/lib.rs".into(),
        },
        &out,
    );
    let seen = server.join().unwrap();
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/session/ses_c/prompt_async");
    let body: Value = serde_json::from_str(&seen[0].body).unwrap();
    assert_eq!(body["parts"][0]["metadata"]["kind"], "subagent_steer");
    assert_eq!(body["parts"][0]["text"], "look at src/lib.rs");
    let notes = read_out(&out);
    assert_eq!(
        notes,
        vec![json!({"kind": "command_result", "command": "steer", "status": 204})]
    );
}

#[test]
fn abort_parent_keeps_its_children_by_scope() {
    let (ep, server) = serve(vec![Served::Json(200, "true".into())]);
    let out = Out::new(Vec::new());
    execute(
        &ep,
        &Command::Abort {
            session: "ses_p".into(),
            scope: Scope::Session,
        },
        &out,
    );
    let seen = server.join().unwrap();
    assert_eq!(seen[0].method, "POST");
    assert_eq!(seen[0].path, "/session/ses_p/abort?scope=session");
    assert_eq!(seen[0].body, "");
    assert_eq!(read_out(&out)[0]["command"], "abort");
}

#[test]
fn abort_child_posts_to_the_child() {
    let (ep, server) = serve(vec![Served::Json(200, "true".into())]);
    let out = Out::new(Vec::new());
    execute(
        &ep,
        &Command::Abort {
            session: "ses_child".into(),
            scope: Scope::Session,
        },
        &out,
    );
    assert_eq!(
        server.join().unwrap()[0].path,
        "/session/ses_child/abort?scope=session"
    );
}

#[test]
fn permission_reply_posts_the_answer() {
    let (ep, server) = serve(vec![Served::Json(200, "true".into())]);
    let out = Out::new(Vec::new());
    execute(
        &ep,
        &Command::ReplyPermission {
            id: "per_1".into(),
            reply: Reply::Once,
        },
        &out,
    );
    let seen = server.join().unwrap();
    assert_eq!(seen[0].path, "/permission/per_1/reply");
    assert_eq!(
        serde_json::from_str::<Value>(&seen[0].body).unwrap(),
        json!({"reply": "once"})
    );
    assert_eq!(read_out(&out)[0]["kind"], "command_result");
}

#[test]
fn failed_command_reports_status_without_the_server_body() {
    let (ep, server) = serve(vec![Served::Json(409, r#"{"error":"SECRET_TEXT"}"#.into())]);
    let out = Out::new(Vec::new());
    execute(
        &ep,
        &Command::ReplyPermission {
            id: "per_1".into(),
            reply: Reply::Reject,
        },
        &out,
    );
    server.join().unwrap();
    let raw = out.with(|w| String::from_utf8(w.clone()).unwrap());
    assert!(!raw.contains("SECRET_TEXT"));
    let notes = read_out(&out);
    assert_eq!(
        notes,
        vec![
            json!({"kind": "command_failed", "command": "reply_permission", "reason": "http", "status": 409})
        ]
    );
}

#[test]
fn command_with_no_listener_reports_io_failure() {
    let port = TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let ep = Endpoint { port, basic: None };
    let out = Out::new(Vec::new());
    execute(
        &ep,
        &Command::Abort {
            session: "ses_p".into(),
            scope: Scope::Tree,
        },
        &out,
    );
    assert_eq!(
        read_out(&out),
        vec![json!({"kind": "command_failed", "command": "abort", "reason": "io"})]
    );
}

#[test]
fn http_reader_decodes_length_and_chunked_bodies() {
    let (ep, server) = serve(vec![Served::Json(200, r#"{"a":1}"#.into())]);
    let r = http::request(&ep, "GET", "/x", None, std::time::Duration::from_secs(5)).unwrap();
    server.join().unwrap();
    assert!(r.is_success());
    assert_eq!(r.json().unwrap(), json!({"a": 1}));

    // Chunked body split across chunks, with a chunk extension and trailers.
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let (_, mut stream) = read_request(stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4;ext=1\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Trailer: y\r\n\r\n")
            .unwrap();
    });
    let ep = Endpoint { port, basic: None };
    let r = http::request(
        &ep,
        "POST",
        "/c",
        Some(&json!({})),
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    server.join().unwrap();
    assert_eq!(r.body, b"Wikipedia");
}

#[test]
fn event_reader_joins_multiline_data_and_ends_on_eof() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let (_, mut stream) = read_request(stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n: comment\r\ndata: one\r\ndata: two\r\n\r\nevent: x\r\ndata: three\r\n\r\n")
            .unwrap();
    });
    let ep = Endpoint { port, basic: None };
    let mut s = http::open_events(&ep, "/global/event").unwrap();
    assert_eq!(s.next_data().unwrap().as_deref(), Some("one\ntwo"));
    assert_eq!(s.next_data().unwrap().as_deref(), Some("three"));
    assert_eq!(s.next_data().unwrap(), None);
    server.join().unwrap();
}

#[test]
fn path_ids_are_plain_identifiers() {
    assert!(http::is_path_id("ses_deef41713ffemDqz84K4hP7xlK"));
    assert!(http::is_path_id("per_211105187001vqfRUkWefI6Gtl"));
    assert!(!http::is_path_id(""));
    assert!(!http::is_path_id("a/b"));
    assert!(!http::is_path_id("a?b"));
}

#[test]
fn protocol_consumes_bridge_notifications_without_events() {
    use crate::AcpProtocol;
    use wasit_core::Protocol;
    let mut p = AcpProtocol::new("test");
    let note = r#"{"jsonrpc":"2.0","method":"_wasit/kilo","params":{"kind":"state_lost","reason":"stream_error"}}"#;
    assert!(p.on_line(note).is_empty());
    assert!(p.on_line(&format!("{note}\n")).is_empty());
}

#[test]
fn control_lines_are_the_inverse_of_their_parse() {
    use super::{control_line, parse_control, Reply, Scope};
    let cases = [
        Command::Steer {
            session: "ses_c".into(),
            text: "use the other file".into(),
        },
        Command::Abort {
            session: "ses_p".into(),
            scope: Scope::Session,
        },
        Command::Abort {
            session: "ses_c".into(),
            scope: Scope::Tree,
        },
        Command::ReplyPermission {
            id: "per_1".into(),
            reply: Reply::Always,
        },
    ];
    for cmd in cases {
        let line = control_line(&cmd).0;
        let parsed = parse_control(&line)
            .expect("a control line")
            .expect("parses");
        assert_eq!(parsed, cmd, "{line}");
    }
}

#[test]
fn bridge_notes_are_read_by_kind_and_unknown_ones_are_none() {
    use super::{parse_note, Job, Note};
    let note = |v: Value| parse_note(&v);
    assert_eq!(
        note(json!({"kind": "session.created", "session": "ses_c", "parent": "ses_p"})),
        Some(Note::SessionCreated {
            session: "ses_c".into(),
            parent: Some("ses_p".into())
        })
    );
    assert_eq!(
        note(
            json!({"kind": "tokens", "session": "ses_c", "message": "msg_1", "input": 100.0,
                    "output": 20.0, "reasoning": 0.0, "cache_read": 0.0, "cache_write": 0.0,
                    "cost": 0.25})
        ),
        Some(Note::Tokens {
            session: "ses_c".into(),
            message: "msg_1".into(),
            input: 100,
            output: 20,
            reasoning: 0,
            cache_read: 0,
            cache_write: 0,
            cost: 0.25
        })
    );
    assert_eq!(
        note(json!({"kind": "background_jobs", "session": "ses_p",
                    "jobs": [{"id": "ses_c", "type": "task", "status": "cancelled"}]})),
        Some(Note::BackgroundJobs {
            session: "ses_p".into(),
            jobs: vec![Job {
                id: "ses_c".into(),
                kind: "task".into(),
                status: "cancelled".into()
            }]
        })
    );
    assert_eq!(
        note(
            json!({"kind": "command_failed", "command": "steer", "reason": "http", "status": 404})
        ),
        Some(Note::CommandFailed {
            command: "steer".into(),
            reason: "http".into(),
            status: Some(404)
        })
    );
    // A kind the bridge does not send, and a note without its session, are not notes.
    assert_eq!(note(json!({"kind": "something_new"})), None);
    assert_eq!(note(json!({"kind": "session.idle"})), None);
}
