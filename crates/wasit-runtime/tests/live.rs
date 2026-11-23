//! Live tests against the real CLIs, through the whole runtime (manager → actor → real shim →
//! real CLI). Opt-in: `cargo test -p wasit-runtime --test live -- --ignored`. A missing binary
//! or prerequisite fails the test loudly; nothing passes silently.
//!
//! * claude: authenticated `claude`; uses the cheapest model (`haiku`) and tiny prompts.
//! * opencode / kilo: free models (`opencode/deepseek-v4.1-flash-free`, `kilo/kilo-auto/free`).
//! * codex: a throwaway `CODEX_HOME` whose provider is the local fake Responses server
//!   (`wasit-fake-cli/reference/record/fake-responses.mjs`, started here with `node` on
//!   127.0.0.1:18080 / 18081).

mod common;

use std::time::Duration;

use common::*;
use wasit_core::{AgentEvent, Command, DriverId};
use wasit_runtime::wire::{CreateSession, Phase, SessionEventKind, SessionId, SessionSnapshot};
use wasit_runtime::{default_registry, RuntimeConfig, SessionManager, ShimLauncher};

const LIVE: Duration = Duration::from_secs(240);

fn require(bin: &str) {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let ok = std::env::split_paths(&path).any(|dir| {
        ["", ".exe", ".cmd", ".bat"]
            .iter()
            .any(|ext| dir.join(format!("{bin}{ext}")).is_file())
    });
    assert!(ok, "live test needs `{bin}` on PATH");
}

async fn live_manager(dir: &std::path::Path) -> SessionManager {
    let mut cfg = RuntimeConfig::new(dir, ShimLauncher::subcommand(hostd(), "shim"));
    cfg.timeouts.handshake = Duration::from_secs(120);
    cfg.timeouts.command = Duration::from_secs(60);
    cfg.timeouts.stop_grace = Duration::from_secs(3);
    SessionManager::new(cfg).await.unwrap()
}

/// Create → Idle → one tiny turn → re-adopt in a new manager → identical snapshot → stop.
async fn live_turn(driver: &str, model: Option<&str>, env: Vec<(String, String)>, prompt: &str) {
    let d = TempDir::new();
    let work = d.path().join("w");
    std::fs::create_dir_all(&work).unwrap();
    let (id, before) = {
        let mgr = live_manager(d.path()).await;
        let mut req = CreateSession::new(driver);
        req.cwd = Some(work.to_string_lossy().into_owned());
        req.model = model.map(str::to_string);
        req.config.env = env;
        let id = mgr.create(req).await.unwrap().id;
        let snap = wait_snapshot_within(&mgr, &id, "idle", LIVE, |s| {
            s.phase == Phase::Idle || !s.phase.is_live()
        })
        .await;
        assert_eq!(snap.phase, Phase::Idle, "{snap:#?}");
        let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
        mgr.command(&id, Command::prompt(prompt)).await.unwrap();
        let events = tokio::time::timeout(
            LIVE,
            until(&mut s, |e| {
                is_turn_completed(e) || matches!(e.kind, SessionEventKind::Exited { .. })
            }),
        )
        .await
        .expect("turn completed");
        let text = assistant_text(&events);
        let done = events.iter().find_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::TurnCompleted { end_reason, .. },
            } => Some(*end_reason),
            _ => None,
        });
        assert_eq!(done, Some(None), "turn did not succeed: {events:#?}");
        let snap =
            wait_snapshot_within(&mgr, &id, "idle again", LIVE, |s| s.phase == Phase::Idle).await;
        eprintln!(
            "[{driver}] reply {text:?}; model {:?}; native id {:?}; usage {:?}; context {:?}",
            snap.model, snap.native_session_id, snap.usage, snap.context_pct
        );
        assert!(snap.native_session_id.is_some());
        // Compared at the exit: the CLI may print lines that change the snapshot without an
        // event after the turn's `result` (claude: `command_lifecycle completed`), so a
        // snapshot read while it runs can be a record behind the replay at the same cursor.
        let snap = stop(&mgr, &id).await;
        (id, snap)
    };
    // Re-adopt: the replayed snapshot equals the live one.
    let mgr = live_manager(d.path()).await;
    mgr.adopt_all().await.unwrap();
    let after = wait_snapshot_within(&mgr, &id, "replayed", LIVE, |s| {
        s.loaded && s.cursor == before.cursor
    })
    .await;
    let mut a = after.clone();
    a.loaded = before.loaded;
    assert_eq!(a, before);
}

/// The owner's report against the real CLI: a prompt that makes claude start a background
/// subagent. The session must never read `idle` between the start of the background task and the
/// CLI's report that it ended; the subagent's tool calls are attributed to the call that spawned
/// it (and not part of the session's own conversation); the subagent runs until its task ended;
/// and the final state is `idle` with nothing running.
#[tokio::test]
#[ignore = "live: needs an authenticated claude CLI"]
async fn live_claude_background_subagent() {
    require("claude");
    let d = TempDir::new();
    let work = d.path().join("w");
    std::fs::create_dir_all(&work).unwrap();
    let mgr = live_manager(d.path()).await;
    let mut req = CreateSession::new("claude");
    req.cwd = Some(work.to_string_lossy().into_owned());
    req.model = Some("haiku".into());
    let id = mgr.create(req).await.unwrap().id;
    wait_snapshot_within(&mgr, &id, "idle", LIVE, |s| s.phase == Phase::Idle).await;
    mgr.command(
        &id,
        Command::prompt(
            "Use the Agent tool exactly once with run_in_background set to true, subagent_type \
             general-purpose, description probe, instruction: run the Bash command `echo hi` and \
             report its output in one word. Then reply with just the word STARTED and end your \
             turn. When you are later told the agent finished, reply with just the word OK.",
        ),
    )
    .await
    .unwrap();
    // Everything the CLI does for this prompt, to the moment the agent's own turn(s) ended.
    let end = wait_snapshot_within(
        &mgr,
        &id,
        "the background work done and the session idle",
        LIVE,
        |s| {
            s.phase == Phase::Idle
                && s.turns_completed >= 2
                && !s.tasks.is_empty()
                && s.tasks.iter().all(|t| !t.running)
        },
    )
    .await;
    let all = mgr.history(&id, None, 5000).await.unwrap().events;
    let agent_events: Vec<&AgentEvent> = all
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent { event } => Some(event),
            _ => None,
        })
        .collect();
    eprintln!(
        "[claude] {} events; tasks {:?}; subagents {:?}; turns {}",
        all.len(),
        end.tasks
            .iter()
            .map(|t| (&t.kind, t.background, &t.status))
            .collect::<Vec<_>>(),
        end.subagents
            .iter()
            .map(|s| (&s.subagent_type, s.tool_calls, s.running, s.ok))
            .collect::<Vec<_>>(),
        end.turns_completed
    );
    // The CLI backgrounded it, as asked.
    let started = all
        .iter()
        .position(|e| {
            matches!(
                &e.kind,
                SessionEventKind::Agent {
                    event: AgentEvent::TaskStarted {
                        background: true,
                        ..
                    }
                }
            )
        })
        .expect("the CLI started a background task");
    let ended = all
        .iter()
        .position(|e| {
            matches!(
                &e.kind,
                SessionEventKind::Agent {
                    event: AgentEvent::TaskEnded { .. }
                }
            )
        })
        .expect("the CLI reported the task ended");
    assert!(started < ended);
    // Never idle between the task's start and the CLI's report that it ended — although the turn
    // that started it ended long before.
    let first_turn_end = all.iter().position(is_turn_completed).unwrap();
    assert!(
        started < first_turn_end && first_turn_end < ended,
        "the turn ended while the task ran"
    );
    for e in &all[started..ended] {
        assert!(
            !is_phase(e, "idle"),
            "idle while the background task ran: {e:?}"
        );
    }
    // The subagent's own tool calls are scoped to the call that spawned it.
    let spawn = agent_events
        .iter()
        .find_map(|e| match e {
            AgentEvent::ToolCallStarted {
                call_id,
                is_subagent: true,
                ..
            } => Some(call_id.clone()),
            _ => None,
        })
        .expect("the Agent call");
    let scoped: Vec<&AgentEvent> = agent_events
        .iter()
        .copied()
        .filter(|e| matches!(e, AgentEvent::Scoped { .. }))
        .collect();
    assert!(!scoped.is_empty(), "the subagent's lines were attributed");
    assert!(scoped
        .iter()
        .all(|e| matches!(e, AgentEvent::Scoped { scope, .. } if scope.call_id == spawn)));
    assert!(scoped.iter().any(|e| matches!(e, AgentEvent::Scoped { event, .. } if matches!(&**event, AgentEvent::ToolCallStarted { .. }))));
    // The session's own transcript has none of them.
    let main = mgr
        .history_scoped(&id, None, 5000, wasit_runtime::wire::HistoryScope::Main)
        .await
        .unwrap();
    assert!(!main
        .events
        .iter()
        .any(|e| is_agent(e, |a| matches!(a, AgentEvent::Scoped { .. }))));
    // The subagent ran until the agent said its task ended; the final state is idle, nothing running.
    assert_eq!(end.phase, Phase::Idle);
    assert_eq!(end.background_running, 0);
    assert_eq!(end.subagents.len(), 1);
    assert!(
        !end.subagents[0].running && end.subagents[0].ok == Some(true),
        "{:?}",
        end.subagents[0]
    );
    assert!(end.subagents[0].tool_calls >= 1);
    assert!(
        !all.iter()
            .any(|e| matches!(&e.kind, SessionEventKind::Anomaly { .. })),
        "an activity the model could not explain was reported"
    );
    stop(&mgr, &id).await;
}

async fn stop(mgr: &SessionManager, id: &SessionId) -> SessionSnapshot {
    mgr.stop(id).await.unwrap();
    let snap = wait_snapshot_within(mgr, id, "exit", LIVE, |s| !s.phase.is_live()).await;
    assert!(
        matches!(snap.phase, Phase::Exited { .. }),
        "{:?}",
        snap.phase
    );
    snap
}

#[tokio::test]
#[ignore = "live: needs an authenticated claude CLI"]
async fn live_claude() {
    require("claude");
    live_turn(
        "claude",
        Some("haiku"),
        vec![],
        "Reply with exactly the word: ok",
    )
    .await;
}

/// The real claude through the whole runtime: the first prompt given at creation is an
/// `input` (journaled with the start) before the turn that names it; the CLI's own lifecycle
/// report acknowledges it; two prompts held behind the running turn are dropped by an
/// interrupt and never start; the next prompt is acknowledged and gets its own turn.
#[tokio::test]
#[ignore = "live: needs an authenticated claude CLI"]
async fn live_claude_inputs() {
    use wasit_core::{InputDropReason, InputId};
    use wasit_runtime::wire::{CommandOutcome, SessionEvent};
    require("claude");
    let d = TempDir::new();
    let work = d.path().join("w");
    std::fs::create_dir_all(&work).unwrap();
    let mgr = live_manager(d.path()).await;
    let mut req = CreateSession::new("claude");
    req.cwd = Some(work.to_string_lossy().into_owned());
    req.model = Some("haiku".into());
    req.first_prompt =
        Some("Use the Bash tool to run: sleep 15 . Then reply with exactly the word: done".into());
    let id = mgr.create(req).await.unwrap().id;
    let mut s = mgr.subscribe(&id, None).await.unwrap();
    let mut events = tokio::time::timeout(
        LIVE,
        until(&mut s, |e| {
            is_agent(e, |a| matches!(a, AgentEvent::ToolCallStarted { .. }))
        }),
    )
    .await
    .expect("the tool started");
    for text in [
        "Reply with exactly the word: second",
        "Reply with exactly the word: third",
    ] {
        let out = mgr.command(&id, Command::prompt(text)).await.unwrap();
        assert_eq!(out, CommandOutcome::Queued);
    }
    mgr.command(&id, Command::Interrupt).await.unwrap();
    events.extend(
        tokio::time::timeout(LIVE, until(&mut s, is_turn_completed))
            .await
            .expect("the turn ended"),
    );
    mgr.command(&id, Command::prompt("Reply with exactly the word: after"))
        .await
        .unwrap();
    events.extend(
        tokio::time::timeout(LIVE, until(&mut s, is_turn_completed))
            .await
            .expect("the next turn ended"),
    );
    let input_of = |text: &str| {
        events
            .iter()
            .find_map(|e| match &e.kind {
                SessionEventKind::Input {
                    input,
                    command: Command::Prompt { text: t, .. },
                    ..
                } if t.ends_with(text) => Some(*input),
                _ => None,
            })
            .unwrap_or_else(|| panic!("an input for {text:?}"))
    };
    let pos = |f: &dyn Fn(&SessionEvent) -> bool| events.iter().position(f);
    let agent = |e: &SessionEvent, f: &dyn Fn(&AgentEvent) -> bool| is_agent(e, f);
    let first = input_of("word: done");
    let (second, third, after) = (input_of("second"), input_of("third"), input_of("after"));
    let input_at =
        pos(&|e| matches!(&e.kind, SessionEventKind::Input { input, .. } if *input == first));
    let turn_at = pos(&|e| {
        agent(
            e,
            &|a| matches!(a, AgentEvent::TurnStarted { input: Some(i), .. } if *i == first),
        )
    });
    assert!(input_at.is_some() && input_at < turn_at, "{events:#?}");
    for i in [first, after] {
        assert!(
            pos(&|e| agent(
                e,
                &|a| matches!(a, AgentEvent::InputAcknowledged { input, .. } if *input == i)
            ))
            .is_some(),
            "{i} acknowledged: {events:#?}"
        );
        assert!(
            pos(&|e| matches!(e.kind, SessionEventKind::Written { input, .. } if input == i))
                .is_some()
        );
    }
    let drops: Vec<(InputId, InputDropReason)> = events
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Agent {
                event: AgentEvent::InputDropped { input, reason, .. },
            } => Some((*input, *reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        drops,
        [
            (second, InputDropReason::Interrupted),
            (third, InputDropReason::Interrupted)
        ]
    );
    for i in [second, third] {
        assert!(pos(&|e| agent(
            e,
            &|a| matches!(a, AgentEvent::TurnStarted { input: Some(x), .. } if *x == i)
        ))
        .is_none());
    }
    let summary: Vec<String> = events
        .iter()
        .filter_map(|e| {
            let v = serde_json::to_value(&e.kind).ok()?;
            let kind = v["kind"].as_str()?.to_string();
            let name = match kind.as_str() {
                "agent" => v["event"]["type"].as_str()?.to_string(),
                _ => kind,
            };
            let input = v["input"].as_u64().or_else(|| v["event"]["input"].as_u64());
            (!matches!(
                name.as_str(),
                "assistant_delta" | "usage_update" | "unknown"
            ))
            .then(|| match input {
                Some(i) => format!("{name}:{i}"),
                None => name,
            })
        })
        .collect();
    eprintln!("[claude] inputs {first} {second} {third} {after}; events {summary:?}");
    stop(&mgr, &id).await;
}

#[tokio::test]
#[ignore = "live: needs the opencode CLI and network"]
async fn live_opencode() {
    require("opencode");
    live_turn(
        "opencode",
        Some("opencode/deepseek-v4.1-flash-free"),
        vec![],
        "Reply with exactly the word: ok",
    )
    .await;
}

#[tokio::test]
#[ignore = "live: needs the kilo CLI and network"]
async fn live_kilo() {
    require("kilo");
    live_turn(
        "kilo",
        Some("kilo/kilo-auto/free"),
        vec![],
        "Reply with exactly the word: ok",
    )
    .await;
}

/// A throwaway `CODEX_HOME` whose provider is the local fake Responses server, started here
/// (requests appended to `<dir>/responses.log`). The server is killed when the guard drops.
async fn fake_responses(
    dir: &std::path::Path,
    port: u16,
) -> (std::path::PathBuf, tokio::process::Child) {
    require("codex");
    require("node");
    let home = dir.join("codexhome");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.toml"),
        format!("model = \"fake\"\nmodel_provider = \"fake\"\n[model_providers.fake]\nname = \"fake\"\nbase_url = \"http://127.0.0.1:{port}/v1\"\nwire_api = \"responses\"\nrequires_openai_auth = false\n"),
    )
    .unwrap();
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../wasit-fake-cli/reference/record/fake-responses.mjs");
    let server = tokio::process::Command::new("node")
        .arg(&script)
        .env("FAKE_RESPONSES_LOG", dir.join("responses.log"))
        .env("FAKE_RESPONSES_PORT", port.to_string())
        .kill_on_drop(true)
        .spawn()
        .expect("start the fake Responses server");
    // Readiness: the port accepts connections (bounded backoff while node starts).
    let mut backoff = Duration::from_millis(50);
    let mut up = false;
    for _ in 0..20 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            up = true;
            break;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
    assert!(up, "fake Responses server did not start");
    (home, server)
}

#[tokio::test]
#[ignore = "live: needs the codex CLI and node (local fake Responses server)"]
async fn live_codex() {
    let d = TempDir::new();
    let (home, _server) = fake_responses(d.path(), 18080).await;
    live_turn(
        "codex",
        None,
        vec![("CODEX_HOME".into(), home.to_string_lossy().into_owned())],
        "hello",
    )
    .await;
}

/// Image input through the real codex app-server: a PNG uploaded to the session goes out as
/// `localImage {path}` (the descriptor's prompt input), codex reads it and sends it to the model
/// as an `input_image` data URL with the same bytes; the fake model names what it got.
#[tokio::test]
#[ignore = "live: needs the codex CLI and node (local fake Responses server)"]
async fn live_codex_image() {
    use wasit_core::content::b64_encode;
    use wasit_runtime::media::AttachmentPut;
    let d = TempDir::new();
    let (home, _server) = fake_responses(d.path(), 18081).await;
    let work = d.path().join("w");
    std::fs::create_dir_all(&work).unwrap();
    let mgr = live_manager(d.path()).await;
    let mut req = CreateSession::new("codex");
    req.cwd = Some(work.to_string_lossy().into_owned());
    req.config.env = vec![("CODEX_HOME".into(), home.to_string_lossy().into_owned())];
    let id = mgr.create(req).await.unwrap().id;
    let snap = wait_snapshot_within(&mgr, &id, "idle", LIVE, |s| {
        s.phase == Phase::Idle || !s.phase.is_live()
    })
    .await;
    assert_eq!(snap.phase, Phase::Idle, "{snap:#?}");
    assert!(
        snap.prompt_input
            .image_mimes
            .iter()
            .any(|m| m == "image/png"),
        "{:?}",
        snap.prompt_input
    );
    let bytes = png();
    let att = mgr
        .attachment_put(&AttachmentPut {
            id: id.clone(),
            upload: "cximage1".into(),
            name: "chart.png".into(),
            size: bytes.len() as u64,
            offset: 0,
            data: b64_encode(&bytes),
            last: true,
        })
        .await
        .unwrap()
        .attachment
        .expect("upload complete");
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.command(
        &id,
        Command::Prompt {
            text: "What is in this picture?".into(),
            attachments: vec![att],
        },
    )
    .await
    .unwrap();
    let events = tokio::time::timeout(
        LIVE,
        until(&mut s, |e| {
            is_turn_completed(e) || matches!(e.kind, SessionEventKind::Exited { .. })
        }),
    )
    .await
    .expect("turn completed");
    let text = assistant_text(&events);
    let want = format!("I see a image/png image of {} bytes", bytes.len());
    assert!(text.contains(&want), "reply {text:?}, wanted {want:?}");
    // The model request carried the very bytes, as a data URL.
    let log = std::fs::read_to_string(d.path().join("responses.log")).unwrap();
    assert!(log.contains("\"input_image\""), "no input_image in {log}");
    assert!(
        log.contains(&format!("data:image/png;base64,{}", b64_encode(&bytes))),
        "the image bytes did not reach the model"
    );
    // What the runtime wrote to codex: a localImage item with the upload's path.
    let journal = std::fs::read_to_string(
        d.path()
            .join("sessions")
            .join(id.as_str())
            .join("journal.ndjson"),
    )
    .unwrap();
    assert!(
        journal.contains("localImage"),
        "no localImage in the journal"
    );
    stop(&mgr, &id).await;
}

#[tokio::test]
#[ignore = "live: probes every installed CLI"]
async fn live_discovery() {
    let disc =
        wasit_runtime::Discovery::new(wasit_runtime::DiscoveryConfig::new(default_registry()));
    let mut changes = disc.changes();
    let inv = tokio::time::timeout(LIVE, async {
        loop {
            let inv = disc.inventory(None);
            if inv.drivers.len() == 4 && inv.drivers.iter().all(|d| !d.refreshing) {
                return inv;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    for e in &inv.drivers {
        eprintln!(
            "[{}] installed={} version={:?} models={} modes={} sessions={} probes={:?}",
            e.driver,
            e.installed,
            e.version,
            e.models.len(),
            e.modes.len(),
            e.sessions.len(),
            e.probes
                .iter()
                .map(|p| (p.kind, p.ok, p.error.clone(), p.duration_ms))
                .collect::<Vec<_>>()
        );
        assert!(e.installed, "{} not installed", e.driver);
        assert!(e.version.is_some(), "{e:#?}");
    }
    let _ = DriverId::new("claude");
}

/// The real claude's own storage for a scratch project: `<config dir>/projects/<cwd with every
/// non-alphanumeric character as `-`>`. Only names and the fields `type`, `sessionId`,
/// `customTitle` are ever read, in a directory this test made.
fn claude_project_dir(cwd: &std::path::Path) -> std::path::PathBuf {
    let config = match std::env::var_os("CLAUDE_CONFIG_DIR") {
        Some(c) => std::path::PathBuf::from(c),
        None => std::path::PathBuf::from(std::env::var_os("HOME").expect("HOME")).join(".claude"),
    };
    let enc: String = cwd
        .to_string_lossy()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    config.join("projects").join(enc)
}

/// The conversation lifecycle against the real claude, through the whole runtime: created under
/// an id the runtime chose and a name; ended before any prompt (nothing stored yet) and
/// continued (the conversation starts under the same id: a resume would fail); a prompt; ended
/// and continued by sending a prompt to the ended session (now it resumes). The conversation id
/// never changes and claude's own storage holds one transcript for it, titled with the name.
///
/// Opt-in like the other live tests (`-- --ignored`); it sends two tiny prompts to haiku.
#[tokio::test]
#[ignore = "live: needs an authenticated claude CLI (two tiny haiku prompts)"]
async fn live_claude_conversation_lifecycle() {
    require("claude");
    let d = TempDir::new();
    let scratch = std::path::PathBuf::from("/tmp/marsad-lifecycle").join(format!(
        "rt-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    let work = scratch.join("proj");
    std::fs::create_dir_all(&work).unwrap();
    let work = std::fs::canonicalize(&work).unwrap();
    let project = claude_project_dir(&work);
    let mgr = live_manager(d.path()).await;
    let mut req = CreateSession::new("claude");
    req.cwd = Some(work.to_string_lossy().into_owned());
    req.model = Some("haiku".into());
    req.name = Some("Lifecycle live test".into());
    let id = mgr.create(req).await.unwrap().id;

    // The conversation id is known from creation; the name is the display name at once.
    let snap = mgr.snapshot(&id).unwrap();
    let native = snap.native_session_id.clone().expect("an id from creation");
    assert_eq!(native.len(), 36, "{native}");
    assert_eq!(snap.display_name.as_deref(), Some("Lifecycle live test"));
    assert_eq!(snap.cwd.as_deref(), Some(work.to_str().unwrap()));
    let mut seen_ids = vec![native.clone()];
    let idle = |g: u32| move |s: &SessionSnapshot| s.generation == g && s.phase == Phase::Idle;
    let snap = wait_snapshot_within(&mgr, &id, "idle", LIVE, idle(1)).await;
    seen_ids.push(snap.native_session_id.clone().unwrap());

    // Ended before any prompt: claude stored nothing, and the session can still be continued.
    let ended = stop(&mgr, &id).await;
    assert!(ended.continuable, "{:?}", ended.not_continuable);
    assert!(
        !project.join(format!("{native}.jsonl")).exists(),
        "claude stored a conversation nobody spoke in"
    );
    mgr.continue_session(&id).await.unwrap();
    let snap = wait_snapshot_within(&mgr, &id, "generation 2 idle", LIVE, idle(2)).await;
    seen_ids.push(snap.native_session_id.clone().unwrap());

    // The first prompt stores the conversation, titled with the name.
    let mut s = mgr.subscribe(&id, Some(snap.cursor)).await.unwrap();
    mgr.command(&id, Command::prompt("Reply with exactly the word: ok"))
        .await
        .unwrap();
    let evs = tokio::time::timeout(LIVE, until(&mut s, is_turn_completed))
        .await
        .expect("first turn");
    assert!(!assistant_text(&evs).is_empty(), "{evs:#?}");
    let transcript = project.join(format!("{native}.jsonl"));
    assert!(transcript.is_file(), "{}", transcript.display());
    let (mut titles, mut ids) = (Vec::new(), std::collections::BTreeSet::new());
    for line in std::fs::read_to_string(&transcript).unwrap().lines() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        if v["type"] == "custom-title" {
            titles.push(v["customTitle"].as_str().unwrap().to_string());
        }
        if let Some(sid) = v["sessionId"].as_str() {
            ids.insert(sid.to_string());
        }
    }
    assert!(
        titles.iter().any(|t| t == "Lifecycle live test"),
        "{titles:?}"
    );
    assert_eq!(ids.len(), 1, "{ids:?}");
    assert!(ids.contains(&native), "{ids:?}");
    let snap = wait_snapshot_within(&mgr, &id, "idle again", LIVE, idle(2)).await;
    seen_ids.push(snap.native_session_id.clone().unwrap());

    // Ended after a turn: the user's own command for it is offered, and a prompt sent to the
    // ended session continues it (resuming now) and is answered.
    let ended = stop(&mgr, &id).await;
    let cmd = ended.resume_command.clone().expect("a resume command");
    assert_eq!(cmd.argv, ["claude", "--resume", native.as_str()]);
    assert_eq!(cmd.cwd, work.to_str().unwrap());
    let mut s = mgr.subscribe(&id, Some(ended.cursor)).await.unwrap();
    mgr.command(&id, Command::prompt("Reply with exactly the word: ok"))
        .await
        .unwrap();
    let evs = tokio::time::timeout(LIVE, until(&mut s, is_turn_completed))
        .await
        .expect("second turn");
    assert!(!assistant_text(&evs).is_empty(), "{evs:#?}");
    let snap = wait_snapshot_within(&mgr, &id, "generation 3 idle", LIVE, idle(3)).await;
    seen_ids.push(snap.native_session_id.clone().unwrap());
    let ended = stop(&mgr, &id).await;
    seen_ids.push(ended.native_session_id.clone().unwrap());

    // How each process was started, from the journal.
    let history = mgr.history(&id, None, 2000).await.unwrap().events;
    let argvs: Vec<Vec<String>> = history
        .iter()
        .filter_map(|e| match &e.kind {
            SessionEventKind::Spawned { argv, .. } => Some(argv.clone()),
            _ => None,
        })
        .collect();
    let has = |a: &[String], f: &str| a.windows(2).any(|w| w[0] == f && w[1] == native);
    assert!(has(&argvs[0], "--session-id"), "{:?}", argvs[0]);
    assert!(has(&argvs[1], "--session-id"), "{:?}", argvs[1]);
    assert!(!argvs[1].contains(&"--resume".to_string()));
    assert!(has(&argvs[2], "--resume"), "{:?}", argvs[2]);
    assert!(argvs[0].iter().any(|a| a == "Lifecycle live test"));
    assert!(argvs[1].iter().any(|a| a == "Lifecycle live test"));
    assert!(!argvs[2].contains(&"--name".to_string()));

    // One conversation: the id never changed, and the project holds exactly one transcript.
    assert!(seen_ids.iter().all(|i| *i == native), "{seen_ids:?}");
    let mut files: Vec<String> = std::fs::read_dir(&project)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "jsonl"))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    files.sort();
    eprintln!(
        "[claude] conversation {native}; project {}; transcripts {files:?}; argv[1] {:?}; argv[2] {:?}",
        project.display(),
        argvs[1],
        argvs[2]
    );
    assert_eq!(files, [format!("{native}.jsonl")]);
}
