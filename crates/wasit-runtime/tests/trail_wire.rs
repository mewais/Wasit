//! The session trail over the control protocol, against a real `wasit-hostd` (real shims, the
//! fake CLI): the trace id on the wire and in the daemon's own log, `session.trail:<id>` resuming
//! exactly across a daemon kill, `session.trail.page`, a command that times out and is applied
//! later, and the content test: every content channel the fake CLI can script carries a marker,
//! and no marker is in anything the trail serves.

mod common;

use std::path::Path;

use common::*;
use serde_json::{json, Value};
use wasit_control::{ClientOptions, ControlClient, Hello, TraceId};
use wasit_core::{
    BusyPrompt, Command, FileInput, GateChoiceKind, GateDecision, GateRequest, QuestionAnswer,
};
use wasit_fake_cli::scenario::{Scenario, Turn};
#[cfg(unix)]
use wasit_runtime::wire::SessionEventKind;
use wasit_runtime::wire::{topics, SessionId, TrailItem};
use wasit_runtime::{ClientError, TypedSubscription};

fn trace(s: &str) -> TraceId {
    TraceId::new(s).unwrap()
}

/// The next entry of a typed trail subscription.
async fn next_item(sub: &mut TypedSubscription<TrailItem>) -> TrailItem {
    match tokio::time::timeout(T, sub.next()).await {
        Ok(Some(Ok((c, item)))) => {
            assert_eq!(c.as_u64(), Some(item.cursor), "the cursor is the event's");
            item
        }
        other => panic!(
            "no trail entry: {:?}",
            other.map(|o| o.map(|r| r.map(|(c, _)| c)))
        ),
    }
}

#[cfg(unix)]
fn ev(item: &TrailItem) -> String {
    item.entry.ev().to_owned()
}

/// Reads entries until one satisfies `pred` (a JSON view of it); returns all read.
async fn read_until(
    sub: &mut TypedSubscription<TrailItem>,
    pred: impl Fn(&Value) -> bool,
) -> Vec<TrailItem> {
    let mut seen = Vec::new();
    loop {
        let item = next_item(sub).await;
        let hit = pred(&serde_json::to_value(&item).unwrap());
        seen.push(item);
        if hit {
            return seen;
        }
    }
}

fn gate_scenario() -> Scenario {
    Scenario::builder()
        .turn(
            Turn::builder()
                .say("let me run something")
                .tool("Bash", json!({"command": "make"}))
                .needs_permission()
                .result_if_allowed("built")
                .done()
                .say("all done"),
        )
        .build()
}

fn is_ev(name: &'static str) -> impl Fn(&Value) -> bool {
    move |v| v["ev"] == name
}

// ── the trace id and the daemon's own records ───────────────────────────────

/// What the daemon's diagnostics file holds, read after a clean shutdown.
#[cfg(unix)]
async fn daemon_records(daemon: &mut tokio::process::Child, data: &Path) -> Vec<Value> {
    kill_with("TERM", daemon.id().unwrap());
    let status = tokio::time::timeout(T, daemon.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status:?}");
    std::fs::read_to_string(data.join("diag/daemon.ndjson"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// A trace id sent by a client reaches the session (the first prompt of a created session and a
/// later command), shows in the history and on the trail the daemon serves, and the daemon
/// records a request that carries one (`rpc.traced`) — and only those — and a command it refused
/// before journaling (`session.command_refused`), each with the trace.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trace_sent_by_a_client_reaches_the_journal_the_trail_and_the_daemons_log() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let c = client(&data);
    let sc = Scenario::builder()
        .turn(Turn::builder().say("one"))
        .turn(Turn::builder().say("two"))
        .build();
    let mut req = create(d.path(), "claude", &sc);
    req.first_prompt = Some("first".into());
    let id = c
        .clone()
        .with_trace(trace("u-create-0000001"))
        .create(req)
        .await
        .unwrap()
        .id;
    let mut sub = c.subscribe_trail(&id, None);
    read_until(&mut sub, is_ev("turn.completed")).await;
    let ct = c.clone().with_trace(trace("u-second-000001"));
    ct.command(&id, Command::prompt("second")).await.unwrap();
    // An untraced command, and a refused one that carries a trace.
    c.command(&id, Command::prompt("third")).await.unwrap();
    let err = ct.command(&id, Command::Shutdown).await.unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Command(wasit_runtime::wire::CommandError::Rejected { .. })
        ),
        "{err:?}"
    );
    let err = c
        .clone()
        .with_trace(trace("u-nosess-000001"))
        .command(&SessionId::random(), Command::Interrupt)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Command(wasit_runtime::wire::CommandError::SessionNotFound { .. })
        ),
        "{err:?}"
    );
    let seen = read_until(&mut sub, |v| v["ev"] == "turn.completed").await;
    assert!(!seen.is_empty());
    let history = c.history(&id, None, 2000).await.unwrap().events;
    let trace_of = |text: &str| {
        history.iter().find_map(|e| match &e.kind {
            SessionEventKind::Input {
                command: Command::Prompt { text: t, .. },
                trace,
                ..
            } if t == text => Some(trace.as_ref().map(|t| t.as_str().to_owned())),
            _ => None,
        })
    };
    assert_eq!(trace_of("first"), Some(Some("u-create-0000001".into())));
    assert_eq!(trace_of("second"), Some(Some("u-second-000001".into())));
    assert_eq!(trace_of("third"), Some(None));
    let page = c.trail_page(&id, None, 2000).await.unwrap();
    let traces: Vec<Option<String>> = page
        .entries
        .iter()
        .filter(|i| ev(i) == "input.applied")
        .map(|i| {
            serde_json::to_value(i).unwrap()["trace"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(
        traces,
        [
            Some("u-create-0000001".to_string()),
            Some("u-second-000001".to_string()),
            None
        ]
    );

    let log = daemon_records(&mut daemon, &data).await;
    let of = |ev: &str| -> Vec<&Value> { log.iter().filter(|r| r["ev"] == ev).collect() };
    // `rpc.traced`: the create, the second prompt, the refused shutdown, the unknown session —
    // each once, each with its outcome; nothing for the untraced requests (host.info, subscribe,
    // the third prompt, history, trail pages).
    let traced = of("rpc.traced");
    let summary: Vec<(String, String, Option<String>, String)> = traced
        .iter()
        .map(|r| {
            (
                r["trace"].as_str().unwrap().to_owned(),
                r["method"].as_str().unwrap().to_owned(),
                r["code"].as_str().map(str::to_owned),
                r["outcome"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("u-create-0000001", "sessions.create", None, "ok"),
            ("u-second-000001", "session.command", None, "ok"),
            (
                "u-second-000001",
                "session.command",
                Some("rejected"),
                "error"
            ),
            (
                "u-nosess-000001",
                "session.command",
                Some("session_not_found"),
                "error"
            ),
        ]
        .map(|(t, m, c, o)| (
            t.to_owned(),
            m.to_owned(),
            c.map(str::to_owned),
            o.to_owned()
        )),
        "{traced:#?}"
    );
    assert!(traced
        .iter()
        .all(|r| r["hops"] == 0 && r.get("host").is_none() && r["ms"].is_u64()));
    // `session.command_refused`: before journaling, with the trace and the code the requester got.
    let refused: Vec<(Option<&str>, &str, &str, bool)> = of("session.command_refused")
        .iter()
        .map(|r| {
            (
                r["trace"].as_str(),
                r["kind"].as_str().unwrap(),
                r["code"].as_str().unwrap(),
                r["session"] == id.as_str(),
            )
        })
        .collect();
    assert_eq!(
        refused,
        [
            (Some("u-second-000001"), "shutdown", "rejected", true),
            (
                Some("u-nosess-000001"),
                "interrupt",
                "session_not_found",
                false
            ),
        ]
    );
    // The records name ids and codes only.
    let text = std::fs::read_to_string(data.join("diag/daemon.ndjson")).unwrap();
    for word in ["use sessions.stop", "no such session"] {
        assert!(!text.contains(word), "{word}");
    }
}

/// Pids of the processes whose command line names `dir` (a session's shim).
#[cfg(unix)]
fn shim_pids(dir: &Path) -> Vec<u32> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", &dir.display().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

/// Continues the stopped processes when dropped, so a failing test leaves none stopped.
#[cfg(unix)]
struct Continue(Vec<u32>);

#[cfg(unix)]
impl Drop for Continue {
    fn drop(&mut self) {
        for p in &self.0 {
            // A process that is gone is fine.
            let _ = std::process::Command::new("kill")
                .arg("-CONT")
                .arg(p.to_string())
                .status();
        }
    }
}

/// A command that is not journaled in time gets `timeout`; the daemon records it
/// (`session.command_timeout`, with the trace) and the command is applied later all the same:
/// the trail shows it with its trace, and its turn runs. The shim is stopped (SIGSTOP) while the
/// command waits, so nothing can journal it before its deadline, and continued after.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_command_that_times_out_is_recorded_and_still_applied_later() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = {
        let mut child = daemon_command(&data)
            .args(["--command-timeout-ms", "500"])
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        await_ready(&mut child).await;
        child
    };
    let c = client(&data);
    let sc = Scenario::builder()
        .turn(Turn::builder().say("late reply"))
        .build();
    let id = c.create(create(d.path(), "claude", &sc)).await.unwrap().id;
    let mut sub = c.subscribe_trail(&id, None);
    read_until(&mut sub, |v| v["ev"] == "phase" && v["to"] == "idle").await;

    let pids = shim_pids(&data.join("sessions").join(id.as_str()));
    assert!(!pids.is_empty(), "no shim process found");
    let resume = Continue(pids.clone());
    for p in &pids {
        kill_with("STOP", *p);
    }
    let err = c
        .clone()
        .with_trace(trace("u-late-00000001"))
        .command(&id, Command::prompt("slow"))
        .await
        .unwrap_err();
    drop(resume);
    assert_eq!(
        err,
        ClientError::Command(wasit_runtime::wire::CommandError::Timeout)
    );
    // It was applied after the requester gave up: its trail entry, with the trace, and its turn.
    let seen = read_until(&mut sub, |v| v["ev"] == "turn.completed").await;
    let applied = seen
        .iter()
        .map(|i| serde_json::to_value(i).unwrap())
        .find(|v| v["ev"] == "input.applied")
        .expect("the command was applied after all");
    assert_eq!(applied["trace"], "u-late-00000001");
    assert_eq!(applied["kind"], "prompt");

    let log = daemon_records(&mut daemon, &data).await;
    let timed_out: Vec<&Value> = log
        .iter()
        .filter(|r| r["ev"] == "session.command_timeout")
        .collect();
    assert_eq!(timed_out.len(), 1, "{timed_out:#?}");
    assert_eq!(timed_out[0]["trace"], "u-late-00000001");
    assert_eq!(timed_out[0]["kind"], "prompt");
    assert_eq!(timed_out[0]["session"], id.as_str());
    assert!(log.iter().all(|r| r["ev"] != "session.command_refused"));
}

// ── resume across a daemon kill ─────────────────────────────────────────────

/// `session.trail:<id>` resumes from its last cursor through a `kill -9` of the daemon and a
/// restart: the entries read before and after join the page served by the new daemon with no
/// gap and no duplicate, and the page can be read back `limit` at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_trail_topic_resumes_exactly_across_a_daemon_kill_and_pages_back() {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let c = client(&data);
    let id = c
        .create(create(d.path(), "claude", &gate_scenario()))
        .await
        .unwrap()
        .id;
    let mut sub = c.subscribe_trail(&id, None);
    let mut seen = read_until(&mut sub, |v| v["ev"] == "phase" && v["to"] == "idle").await;
    c.clone()
        .with_trace(trace("u-build-0000001"))
        .command(&id, Command::prompt("build it"))
        .await
        .unwrap();
    let raised = read_until(&mut sub, is_ev("gate.raised")).await;
    let request_id =
        match serde_json::to_value(raised.last().unwrap()).unwrap()["request_id"].as_str() {
            Some(r) => r.to_owned(),
            None => panic!("a gate without a request id"),
        };
    seen.extend(raised);
    assert_eq!(sub.subscribe_count(), 1);

    // kill -9; the session, held by its shim, keeps waiting on the gate.
    daemon.start_kill().unwrap();
    daemon.wait().await.unwrap();
    let mut daemon = start_daemon(&data).await;

    c.command(
        &id,
        Command::AnswerGate {
            request_id: request_id.clone(),
            decision: GateDecision::choose("allow"),
        },
    )
    .await
    .unwrap();
    seen.extend(read_until(&mut sub, is_ev("turn.completed")).await);
    assert!(
        sub.subscribe_count() >= 2,
        "the durable subscription resubscribed"
    );

    // Exactly the session's trail: no gap, no duplicate across the restart.
    let page = c.trail_page(&id, None, 2000).await.unwrap();
    assert!(!page.has_more);
    assert_eq!(&page.entries[..seen.len()], &seen[..]);
    let cursors: Vec<u64> = page.entries.iter().map(|i| i.cursor).collect();
    assert!(cursors.windows(2).all(|w| w[0] < w[1]), "{cursors:?}");
    let t = serde_json::to_value(&page.entries).unwrap();
    let t = t.as_array().unwrap();
    assert!(t.iter().any(|v| v["ev"] == "input.applied"
        && v["kind"] == "answer_gate"
        && v["target"] == request_id.as_str()
        && v["choice"] == "allow"));
    assert!(t.iter().any(|v| v["ev"] == "phase"
        && v["to"] == "idle"
        && v["cause"]["kind"] == "turn_completed"));

    // Resuming from any cursor yields exactly the entries after it.
    let mid = page.entries[page.entries.len() / 2].cursor;
    let mut resumed = c.subscribe_trail(&id, Some(mid));
    let after: Vec<TrailItem> = page
        .entries
        .iter()
        .filter(|i| i.cursor > mid)
        .cloned()
        .collect();
    for want in &after {
        assert_eq!(&next_item(&mut resumed).await, want);
    }

    // Paging: the last `limit` before a cursor, oldest first, `has_more` while older exist.
    let mut walked: Vec<TrailItem> = Vec::new();
    let mut before = None;
    loop {
        let p = c.trail_page(&id, before, 4).await.unwrap();
        assert!(p.entries.len() <= 4);
        before = p.entries.first().map(|i| i.cursor);
        let mut older = p.entries;
        older.extend(walked);
        walked = older;
        if !p.has_more {
            break;
        }
    }
    // (The CLI may still write a line or two after its turn: what was added since the whole page
    // was read comes last.)
    assert!(walked.starts_with(&page.entries), "{walked:#?}");
    // A page of a session that does not exist is a typed error, as for history.
    let err = c
        .trail_page(&SessionId::random(), None, 5)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Runtime(wasit_runtime::wire::RuntimeError::SessionNotFound(_))
        ),
        "{err:?}"
    );
    daemon.start_kill().unwrap();
    daemon.wait().await.unwrap();
}

// ── the content test ────────────────────────────────────────────────────────

/// One marker per content channel. Text channels carry a quote and a backslash, so that a leak
/// of the JSON-escaped form is as findable as one of the raw form; names the file system holds
/// (the attachment's name, the working directory's) are plain.
struct Markers {
    prompt: String,
    queued: String,
    att_name: String,
    att_data: String,
    say: String,
    think: String,
    tool_in: String,
    tool_in2: String,
    tool_res: String,
    title: String,
    deny: String,
    answer: String,
    question: String,
    opt1: String,
    opt2: String,
    stderr: String,
    fail: String,
    garbage: String,
    cwd: String,
}

impl Markers {
    fn new(driver: &str) -> Markers {
        let text = |ch: &str| format!("MKLEAK{driver}{ch}\"q\"\\z");
        let plain = |ch: &str| format!("MKLEAK{driver}{ch}");
        Markers {
            prompt: text("prompt"),
            queued: text("queued"),
            att_name: plain("attname"),
            att_data: plain("attdata"),
            say: text("say"),
            think: text("think"),
            tool_in: text("toolin"),
            tool_in2: text("toolintwo"),
            tool_res: text("toolres"),
            title: text("title"),
            deny: text("deny"),
            answer: text("answer"),
            question: text("question"),
            opt1: text("optone"),
            opt2: text("opttwo"),
            stderr: text("stderr"),
            fail: text("fail"),
            garbage: text("garbage"),
            cwd: plain("cwddir"),
        }
    }

    fn all(&self) -> Vec<(&'static str, &str)> {
        vec![
            ("prompt", &self.prompt),
            ("queued", &self.queued),
            ("attachment name", &self.att_name),
            ("attachment content", &self.att_data),
            ("assistant text", &self.say),
            ("thinking", &self.think),
            ("tool input", &self.tool_in),
            ("tool input two", &self.tool_in2),
            ("tool result", &self.tool_res),
            ("tool title", &self.title),
            ("deny text", &self.deny),
            ("question answer", &self.answer),
            ("question text", &self.question),
            ("option one", &self.opt1),
            ("option two", &self.opt2),
            ("stderr", &self.stderr),
            ("cli failure", &self.fail),
            ("garbage line", &self.garbage),
            ("cwd name", &self.cwd),
        ]
    }
}

/// `m` as it appears inside a JSON string.
fn escaped(m: &str) -> String {
    let s = serde_json::to_string(m).unwrap();
    s[1..s.len() - 1].to_owned()
}

fn raw_endpoint(data: &Path) -> wasit_control::Endpoint {
    wasit_control::local_endpoint(&data.join("hostd.sock"))
}

async fn content_never_reaches_the_trail(driver: &str) {
    let d = TempDir::new();
    let data = d.path().join("d");
    let mut daemon = start_daemon(&data).await;
    let c = client(&data);
    let raw = ControlClient::connect_endpoint(
        &raw_endpoint(&data),
        ClientOptions::new(Hello::new("test", "raw-test", "gui")),
    )
    .await
    .unwrap();
    let m = Markers::new(driver);

    let sc = Scenario::builder()
        .turn(
            Turn::when("TURN-ONE")
                .think(&m.think)
                .say(&m.say)
                .tool("Bash", json!({"command": m.tool_in}))
                .result_if_allowed(&m.tool_res)
                .done()
                .tool("Edit", json!({"path": m.tool_in2}))
                .needs_permission()
                .title(&m.title)
                .result_if_allowed("ok")
                .result_if_denied("refused")
                .done()
                .stderr(&m.stderr)
                .garbage(&m.garbage)
                .ask_question(&m.question, &[(&m.opt1, "first"), (&m.opt2, "second")])
                .say("after"),
        )
        .turn(Turn::when("TURN-TWO").fail(&m.fail))
        .build();
    let mut req = create(d.path(), driver, &sc);
    let cwd = d.path().join(&m.cwd);
    std::fs::create_dir_all(&cwd).unwrap();
    req.cwd = Some(cwd.to_string_lossy().into_owned());
    let id = c
        .clone()
        .with_trace(trace("u-content-000001"))
        .create(req)
        .await
        .unwrap()
        .id;

    let mut topic = raw
        .subscribe(&topics::session_trail(id.as_str()), None)
        .await
        .unwrap();
    // The JSON of everything the trail served: every topic payload, every page.
    let mut served: Vec<String> = Vec::new();
    let mut entries: Vec<Value> = Vec::new();
    macro_rules! read_until {
        ($pred:expr) => {{
            loop {
                let (cursor, payload) = tokio::time::timeout(T, topic.next())
                    .await
                    .expect("a trail entry")
                    .expect("the topic ended");
                assert_eq!(payload["cursor"].as_u64(), cursor.as_u64());
                served.push(payload.to_string());
                let hit = ($pred)(&payload);
                entries.push(payload);
                if hit {
                    break;
                }
            }
        }};
    }
    read_until!(|v: &Value| v["ev"] == "phase" && v["to"] == "idle");

    let snap = c.snapshot(&id).await.unwrap();
    assert_eq!(snap.cwd.as_deref(), Some(cwd.to_str().unwrap()));
    let busy = snap.capabilities.prompt_while_busy;
    let input = &snap.prompt_input;
    let mut attachments = Vec::new();
    if input.files != FileInput::None {
        attachments.push(
            c.upload(
                &id,
                "upload01ab",
                &format!("{}.txt", m.att_name),
                m.att_data.as_bytes(),
            )
            .await
            .unwrap(),
        );
    }
    // The attachment's channel is real: its bytes are on the host, under the session.
    for a in &attachments {
        assert_eq!(std::fs::read_to_string(&a.path).unwrap(), m.att_data);
        assert!(a.name.contains(&m.att_name));
    }
    let first_text = format!("TURN-ONE {}", m.prompt);
    c.clone()
        .with_trace(trace("u-content-000002"))
        .command(
            &id,
            Command::Prompt {
                text: first_text.clone(),
                attachments: attachments.clone(),
            },
        )
        .await
        .unwrap();

    // The permission gate is answered with a deny and a text.
    read_until!(is_ev("gate.raised"));
    let rid = entries.last().unwrap()["request_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let gate = c.snapshot(&id).await.unwrap().pending_gates[0].gate.clone();
    assert_eq!(gate.request_id(), rid);
    let reject = gate
        .choice_of_kind(&[GateChoiceKind::RejectOnce, GateChoiceKind::Cancel])
        .expect("a reject choice")
        .id
        .clone();
    c.command(
        &id,
        Command::AnswerGate {
            request_id: rid.clone(),
            decision: GateDecision::choose_with(&reject, Some(&m.deny)),
        },
    )
    .await
    .unwrap();

    // Then the question; a prompt waits behind the turn where the driver holds prompts.
    read_until!(|v: &Value| v["ev"] == "gate.raised" && v["request_id"] != rid.as_str());
    let qid = entries.last().unwrap()["request_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_text = format!("TURN-TWO {}", m.queued);
    let queued = busy == BusyPrompt::QueuedByProtocol;
    if queued {
        let out = c
            .command(&id, Command::prompt(second_text.clone()))
            .await
            .unwrap();
        assert_eq!(out, wasit_runtime::wire::CommandOutcome::Queued);
    }
    let gate = c
        .snapshot(&id)
        .await
        .unwrap()
        .pending_gates
        .into_iter()
        .map(|g| g.gate)
        .find(|g| g.request_id() == qid)
        .expect("the question is pending");
    let answer_is_text = matches!(gate, GateRequest::Question { .. });
    let cmd = match &gate {
        GateRequest::Question {
            request_id,
            questions,
        } => Command::AnswerQuestion {
            request_id: request_id.clone(),
            answer: QuestionAnswer {
                answers: [(questions[0].id.clone(), vec![m.answer.clone()])]
                    .into_iter()
                    .collect(),
            },
        },
        // ACP models a question as a permission request whose options are the answers.
        GateRequest::Permission {
            request_id,
            choices,
            ..
        } => Command::AnswerGate {
            request_id: request_id.clone(),
            decision: GateDecision::choose(
                &choices
                    .iter()
                    .find(|c| c.label == m.opt1)
                    .expect("the first option")
                    .id,
            ),
        },
        other => panic!("unexpected gate {other:?}"),
    };
    c.command(&id, cmd).await.unwrap();
    read_until!(is_ev("turn.completed"));
    if !queued {
        c.command(&id, Command::prompt(second_text.clone()))
            .await
            .unwrap();
    }
    read_until!(|v: &Value| v["ev"] == "turn.completed"
        && v["interrupted"] == false
        && entries
            .iter()
            .filter(|e| e["ev"] == "turn.completed")
            .count()
            >= 1);
    // (the second turn's completion is the second `turn.completed` read)
    assert_eq!(
        entries
            .iter()
            .filter(|e| e["ev"] == "turn.completed")
            .count(),
        2
    );

    // Pages, walked back three at a time and in one call, as the JSON on the wire.
    let mut paged: Vec<Value> = Vec::new();
    let mut before: Option<u64> = None;
    loop {
        let page: Value = raw
            .request(
                "session.trail.page",
                json!({"id": id, "before": before, "limit": 3}),
                T,
            )
            .await
            .unwrap();
        served.push(page.to_string());
        let es = page["entries"].as_array().unwrap().clone();
        before = es.first().map(|e| e["cursor"].as_u64().unwrap());
        paged.splice(0..0, es);
        if page["has_more"] == false {
            break;
        }
    }
    let whole: Value = raw
        .request("session.trail.page", json!({"id": id, "limit": 2000}), T)
        .await
        .unwrap();
    served.push(whole.to_string());
    let whole_entries = whole["entries"].as_array().unwrap().clone();
    // (A CLI may still write a line or two after its turn: what arrived between the reads
    // comes last.)
    assert!(
        whole_entries.starts_with(&paged),
        "pages walked back are the whole page"
    );
    assert!(
        whole_entries.starts_with(&entries),
        "the topic and the page serve the same entries"
    );

    // ── the channels were really driven: the markers are in the session's own events ──
    let history = serde_json::to_string(&c.history(&id, None, 2000).await.unwrap().events).unwrap();
    let in_events: Vec<&str> = m
        .all()
        .into_iter()
        .filter(|(_, mk)| {
            history.contains(&escaped(mk)) || snap.cwd.as_deref().is_some_and(|c| c.contains(mk))
        })
        .map(|(name, _)| name)
        .collect();
    // The channels every driver of the fake carries; codex shows no title with a permission
    // request.
    let must_flow = [
        "prompt",
        "queued",
        "attachment name",
        "assistant text",
        "thinking",
        "tool input",
        "tool result",
        "tool title",
        "deny text",
        "question text",
        "stderr",
        "cli failure",
        "garbage line",
        "cwd name",
    ];
    for name in must_flow {
        if driver == "codex" && name == "tool title" {
            continue;
        }
        assert!(
            in_events.contains(&name),
            "{driver}: the {name} marker never reached the events: {in_events:?}"
        );
    }
    if answer_is_text {
        assert!(in_events.contains(&"question answer"), "{in_events:?}");
    }

    // ── …and in nothing the trail served ──
    for (what, mk) in m.all() {
        // Raw, JSON-escaped once, and the letters and digits the marker starts with (what is
        // left of it however often a leak was escaped).
        let stem: String = mk
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        assert!(stem.len() >= 10, "{stem}");
        for form in [mk.to_owned(), escaped(mk), stem] {
            for p in &served {
                assert!(
                    !p.contains(&form),
                    "{driver}: the {what} marker is in a served trail payload: {p}"
                );
            }
        }
    }

    // ── what the trail does say: sizes, counts and ids ──
    let find = |f: &dyn Fn(&Value) -> bool| -> &Value {
        entries
            .iter()
            .find(|v| f(v))
            .unwrap_or_else(|| panic!("{driver}: not in {entries:#?}"))
    };
    let applied = |kind: &str| -> Vec<&Value> {
        entries
            .iter()
            .filter(|v| v["ev"] == "input.applied" && v["kind"] == kind)
            .collect()
    };
    let first = find(&|v| {
        v["ev"] == "input.applied"
            && v["kind"] == "prompt"
            && v["chars"] == first_text.chars().count()
    });
    assert_eq!(first["trace"], "u-content-000002");
    assert_eq!(first["attachments"], attachments.len());
    assert_eq!(
        first["bytes"],
        if attachments.is_empty() {
            0
        } else {
            m.att_data.len()
        }
    );
    let second = find(&|v| {
        v["ev"] == "input.applied" && v["kind"] == "prompt" && v["input"] != first["input"]
    });
    assert_eq!(second["chars"], second_text.chars().count());
    assert_eq!(second["status"], if queued { "queued" } else { "sent" });
    // (ACP answers its question with a gate decision too.)
    let deny: Vec<&Value> = applied("answer_gate")
        .into_iter()
        .filter(|v| v["target"] == rid.as_str())
        .collect();
    assert_eq!(deny.len(), 1);
    assert_eq!(deny[0]["chars"], m.deny.chars().count());
    assert_eq!(deny[0]["choice"], reject.as_str());
    assert_eq!(deny[0]["target"], rid.as_str());
    if answer_is_text {
        let a = applied("answer_question");
        assert_eq!(a.len(), 1);
        assert_eq!(a[0]["chars"], m.answer.chars().count());
        assert_eq!(a[0]["target"], qid.as_str());
    }
    // Tool names are words: codex names its shell call by the command line, which is not one.
    let names: Vec<&str> = entries
        .iter()
        .filter(|v| v["ev"] == "tool.started")
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    let edit = if driver == "codex" {
        "apply_patch"
    } else {
        "Edit"
    };
    assert!(names.len() >= 2 && names.contains(&edit), "{names:?}");
    if driver == "codex" {
        assert!(names.iter().any(|n| n.starts_with("<other:")), "{names:?}");
    } else {
        assert!(names.contains(&"Bash"), "{names:?}");
    }
    let permission = find(&|v| v["ev"] == "gate.raised" && v["request_id"] == rid.as_str());
    assert_eq!(permission["gate_type"], "permission");
    assert_eq!(permission["tool"], edit);
    find(&|v| v["ev"] == "process.stderr" && v["chars"] == m.stderr.chars().count());
    find(&|v| v["ev"] == "agent.unknown" && v["raw_type"] == "line");
    find(&|v| v["ev"] == "process.started" && v["trace"] == "u-content-000001");
    find(&|v| v["ev"] == "agent.init");
    // The failing turn reports the CLI's words as a length and a fingerprint at most.
    let last = entries
        .iter()
        .rev()
        .find(|v| v["ev"] == "turn.completed")
        .unwrap();
    if last["end_reason"] == "execution_error" {
        // (A driver may word the failure around the CLI's message.)
        assert!(last["error"]["len"].as_u64().unwrap() >= m.fail.chars().count() as u64);
        assert!(last["error"].get("text").is_none());
        assert_eq!(last["error"]["fp"].as_str().unwrap().len(), 8);
    }
    c.stop(&id).await.unwrap();
    daemon.start_kill().unwrap();
    daemon.wait().await.unwrap();
}

macro_rules! per_driver {
    ($f:ident => $($name:ident: $driver:literal),+ $(,)?) => {
        $( #[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $name() { $f($driver).await } )+
    };
}

per_driver!(content_never_reaches_the_trail =>
    claude_content_never_reaches_the_trail: "claude",
    codex_content_never_reaches_the_trail: "codex",
    opencode_content_never_reaches_the_trail: "opencode",
    kilo_content_never_reaches_the_trail: "kilo",
);
