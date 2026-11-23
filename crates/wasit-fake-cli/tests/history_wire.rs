//! What the codex and ACP personas keep of a conversation and hand back when it is resumed:
//! codex answers `thread/resume` with the turns (the shape of `reference/codex/resume.jsonl`) and
//! writes the rollout discovery lists; an ACP agent replays the conversation as `session/update`
//! notifications before the `session/load` result (`reference/{kilo,opencode}/load.jsonl`) and
//! `session list` / `session/list` show the stored sessions of the project.

mod common;

use common::{scratch_dir, Fake};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use wasit_fake_cli::scenario::{HistoryMessage, Scenario, SessionInfo, Turn};

fn scenario() -> Scenario {
    Scenario::builder()
        .turn(
            Turn::builder()
                .think("considering the request")
                .tool("Bash", json!({"command": "ls"}))
                .result_if_allowed("TOOL-OUT")
                .done()
                .say("first answer"),
        )
        .turn(Turn::builder().say("second answer"))
        .build()
}

fn project(state: &Path, name: &str) -> PathBuf {
    let p = state.join(name);
    std::fs::create_dir_all(&p).unwrap();
    p
}

// ── codex ───────────────────────────────────────────────────────────────────

fn codex(state: &Path, cwd: &Path, scenario: &Scenario) -> Fake {
    let mut f = Fake::spawn_in(
        &["app-server", "--stdio"],
        Some("codex"),
        scenario,
        Some(state),
        Some(cwd),
    );
    f.send(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "clientInfo":{"name":"history-test","version":"0"},"capabilities":{"experimentalApi":true}}}));
    f.recv_until(|m| m["id"] == 1);
    f.send(&json!({"jsonrpc":"2.0","method":"initialized"}));
    f
}

fn rollouts(state: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "jsonl") {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    walk(&state.join("codex-home/sessions"), &mut out);
    out.sort();
    out
}

fn lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn codex_resume_hands_back_the_turns_and_the_rollout_is_listable() {
    let state = scratch_dir();
    let state = state.as_ref();
    let cwd = project(state, "proj");
    let cwd_s = cwd.to_str().unwrap().to_string();

    let mut f = codex(state, &cwd, &scenario());
    f.send(&json!({"jsonrpc":"2.0","id":2,"method":"thread/start","params":{"cwd":cwd_s}}));
    let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
    let thread = r["result"]["thread"]["id"].as_str().unwrap().to_string();
    // A thread opened with `thread/start` has no turns.
    assert_eq!(r["result"]["thread"]["turns"], json!([]));
    for (n, text) in ["first question", "second question"].iter().enumerate() {
        f.send(
            &json!({"jsonrpc":"2.0","id":10 + n,"method":"turn/start","params":{
            "threadId":thread,"input":[{"type":"text","text":text}]}}),
        );
        f.recv_until(|m| m["method"] == "turn/completed");
    }
    f.close_stdin();

    // The rollout, as discovery reads it: `rollout-<stamp>-<id>.jsonl`, `session_meta` first, then
    // one `response_item` message per message.
    let files = rollouts(state);
    assert_eq!(files.len(), 1, "{files:?}");
    let name = files[0].file_name().unwrap().to_str().unwrap();
    assert!(
        name.starts_with("rollout-") && name.ends_with(&format!("-{thread}.jsonl")),
        "{name}"
    );
    let rows = lines(&files[0]);
    assert_eq!(rows[0]["type"], "session_meta");
    assert_eq!(rows[0]["payload"]["id"], thread);
    assert_eq!(rows[0]["payload"]["cwd"], cwd_s);
    let messages: Vec<(String, String, String)> = rows[1..]
        .iter()
        .filter(|r| r["type"] == "response_item" && r["payload"]["type"] == "message")
        .map(|r| {
            let p = &r["payload"];
            (
                p["role"].as_str().unwrap().to_string(),
                p["content"][0]["type"].as_str().unwrap().to_string(),
                p["content"][0]["text"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let want = [
        ("user", "input_text", "first question"),
        ("assistant", "output_text", "first answer"),
        ("user", "input_text", "second question"),
        ("assistant", "output_text", "second answer"),
    ];
    assert_eq!(
        messages,
        want.map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string()))
    );

    // Another process resumes the thread: the earlier turns come back in the result, after the
    // deprecation notice 0.169.1 prints for a paginated thread, as the recording shows.
    let mut g = codex(state, &cwd, &Scenario::default());
    g.send(
        &json!({"jsonrpc":"2.0","id":2,"method":"thread/resume","params":{
        "threadId":thread,"cwd":cwd_s}}),
    );
    let seen = g.recv_until(|m| m["id"] == 2);
    let methods: Vec<&str> = seen.iter().filter_map(|m| m["method"].as_str()).collect();
    assert!(methods.contains(&"deprecationNotice"), "{methods:?}");
    let result = &seen.last().unwrap()["result"];
    let th = &result["thread"];
    assert_eq!(th["id"], thread);
    assert_eq!(th["historyMode"], "paginated");
    assert!(result["turnsBackwardsCursor"].is_string());
    assert!(result["itemsBackwardsCursor"].is_string());
    let turns = th["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    let shape = |turn: &Value| -> Vec<String> {
        turn["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["type"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(
        shape(&turns[0]),
        [
            "userMessage",
            "reasoning",
            "commandExecution",
            "agentMessage"
        ]
    );
    assert_eq!(shape(&turns[1]), ["userMessage", "agentMessage"]);
    for t in turns {
        assert_eq!(t["itemsView"], "full");
        assert_eq!(t["status"], "completed");
        assert_eq!(t["rootTurnId"], t["id"]);
        assert!(t["startedAt"].is_u64() && t["completedAt"].is_u64());
        assert!(t["durationMs"].is_u64());
    }
    let items = turns[0]["items"].as_array().unwrap();
    assert_eq!(
        items[0]["content"],
        json!([{"type":"text","text":"first question","text_elements":[]}])
    );
    assert_eq!(items[1]["summary"], json!(["considering the request"]));
    assert_eq!(items[2]["status"], "completed");
    assert_eq!(items[2]["aggregatedOutput"], "TOOL-OUT");
    assert_eq!(items[2]["exitCode"], 0);
    assert_eq!(items[3]["text"], "first answer");
    assert!(items[3]["completedAtMs"].is_u64());
    assert_eq!(turns[1]["items"][1]["text"], "second answer");
}

#[test]
fn codex_resume_of_a_scenario_conversation_pairs_its_messages_into_turns() {
    let state = scratch_dir();
    let state = state.as_ref();
    let cwd = project(state, "proj");
    let id = "01a1ebc4-6d1c-7502-97a3-4dd474957024";
    let hm = |role: &str, text: &str| HistoryMessage {
        role: role.into(),
        text: text.into(),
    };
    let sc = Scenario::builder()
        .session(SessionInfo {
            id: id.into(),
            cwd: Some(cwd.to_str().unwrap().into()),
            history: vec![
                hm("user", "q1"),
                hm("assistant", "a1"),
                hm("assistant", "a1b"),
                hm("user", "q2"),
                hm("assistant", "a2"),
            ],
            ..SessionInfo::default()
        })
        .build();
    let mut f = codex(state, &cwd, &sc);
    f.send(
        &json!({"jsonrpc":"2.0","id":2,"method":"thread/resume","params":{
        "threadId":id,"cwd":cwd.to_str().unwrap()}}),
    );
    let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
    let turns = r["result"]["thread"]["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    let texts = |t: &Value| -> Vec<String> {
        t["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| {
                i["text"]
                    .as_str()
                    .or_else(|| i["content"][0]["text"].as_str())
                    .unwrap()
                    .to_string()
            })
            .collect()
    };
    assert_eq!(texts(&turns[0]), ["q1", "a1", "a1b"]);
    assert_eq!(texts(&turns[1]), ["q2", "a2"]);
    // The conversation exists in the agent's storage too: its rollout is there for discovery.
    let files = rollouts(state);
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(lines(&files[0])[0]["payload"]["id"], id);
}

#[test]
fn codex_thread_list_is_per_folder_and_newest_first() {
    let state = scratch_dir();
    let state = state.as_ref();
    let (a, b) = (project(state, "a"), project(state, "b"));
    let mut ids = Vec::new();
    for dir in [&a, &b, &a] {
        let mut f = codex(state, dir, &Scenario::default());
        f.send(
            &json!({"jsonrpc":"2.0","id":2,"method":"thread/start","params":{
            "cwd":dir.to_str().unwrap()}}),
        );
        let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
        let t = r["result"]["thread"]["id"].as_str().unwrap().to_string();
        f.send(
            &json!({"jsonrpc":"2.0","id":3,"method":"turn/start","params":{
            "threadId":t,"input":[{"type":"text","text":"hi"}]}}),
        );
        f.recv_until(|m| m["method"] == "turn/completed");
        ids.push(t);
    }
    let mut f = codex(state, &a, &Scenario::default());
    f.send(
        &json!({"jsonrpc":"2.0","id":2,"method":"thread/list","params":{
        "cwd":a.to_str().unwrap(),"limit":1}}),
    );
    let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
    let listed: Vec<&str> = r["result"]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(listed.len(), 2, "only the folder's threads: {listed:?}");
    assert!(!listed.contains(&ids[1].as_str()));
    assert!(listed.contains(&ids[0].as_str()) && listed.contains(&ids[2].as_str()));
}

// ── ACP (opencode, kilo) ────────────────────────────────────────────────────

fn acp(persona: &str, state: &Path, cwd: &Path, scenario: &Scenario) -> Fake {
    let mut f = Fake::spawn_in(&["acp"], Some(persona), scenario, Some(state), Some(cwd));
    f.send(
        &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,
        "clientCapabilities":{}}}),
    );
    f.recv_until(|m| m["id"] == 1);
    f
}

fn kinds(msgs: &[Value]) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| m["params"]["update"]["sessionUpdate"].as_str())
        .map(str::to_string)
        .collect()
}

fn each_acp(f: impl Fn(&'static str)) {
    for persona in ["opencode", "kilo"] {
        f(persona);
    }
}

#[test]
fn acp_load_replays_what_the_turns_sent_before_the_result() {
    each_acp(|persona| {
        let state = scratch_dir();
        let state = state.as_ref();
        let cwd = project(state, "proj");
        let cwd_s = cwd.to_str().unwrap();
        let mut f = acp(persona, state, &cwd, &scenario());
        f.send(
            &json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{
            "cwd":cwd_s,"mcpServers":[]}}),
        );
        let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
        let sid = r["result"]["sessionId"].as_str().unwrap().to_string();
        for (n, text) in ["first question", "second question"].iter().enumerate() {
            f.send(
                &json!({"jsonrpc":"2.0","id":10 + n,"method":"session/prompt","params":{
                "sessionId":sid,"prompt":[{"type":"text","text":text}]}}),
            );
            f.recv_until(|m| m["id"] == 10 + n);
        }
        f.close_stdin();

        let mut g = acp(persona, state, &cwd, &Scenario::default());
        g.send(
            &json!({"jsonrpc":"2.0","id":2,"method":"session/load","params":{
            "sessionId":sid,"cwd":cwd_s,"mcpServers":[]}}),
        );
        let seen = g.recv_until(|m| m["id"] == 2 && m["method"].is_null());
        assert!(seen.last().unwrap()["error"].is_null(), "{persona}");
        // The replay is the conversation in order, then the result; every update is for the
        // loaded session.
        let replay = &seen[..seen.len() - 1];
        assert!(replay
            .iter()
            .all(|m| m["params"]["sessionId"] == sid.as_str()));
        let all = kinds(replay);
        // The available-commands update the agent sends after the load is not part of it.
        assert_eq!(
            all,
            [
                "user_message_chunk",
                "agent_thought_chunk",
                "tool_call",
                "tool_call_update",
                "agent_message_chunk",
                "usage_update",
                "user_message_chunk",
                "agent_message_chunk",
                "usage_update",
            ],
            "{persona}"
        );
        let up = |i: usize| &replay[i]["params"]["update"];
        assert_eq!(up(0)["content"]["text"], "first question");
        assert!(up(0)["messageId"].as_str().unwrap().starts_with("msg_"));
        assert_eq!(up(1)["content"]["text"], "considering the request");
        assert_eq!(up(2)["status"], "pending");
        assert_eq!(up(2)["rawInput"]["command"], "ls");
        assert_eq!(up(3)["toolCallId"], up(2)["toolCallId"]);
        assert_eq!(up(3)["status"], "completed");
        assert_eq!(up(3)["content"][0]["content"]["text"], "TOOL-OUT");
        assert_eq!(up(4)["content"]["text"], "first answer");
        assert_eq!(up(6)["content"]["text"], "second question");
        assert_eq!(up(7)["content"]["text"], "second answer");
    });
}

#[test]
fn acp_lists_the_stored_sessions_of_the_folder_newest_first() {
    each_acp(|persona| {
        let state = scratch_dir();
        let state = state.as_ref();
        let (a, b) = (project(state, "a"), project(state, "b"));
        let mut made = Vec::new();
        for dir in [&a, &b, &a] {
            let mut f = acp(persona, state, dir, &Scenario::default());
            f.send(
                &json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{
                "cwd":dir.to_str().unwrap(),"mcpServers":[]}}),
            );
            let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
            let sid = r["result"]["sessionId"].as_str().unwrap().to_string();
            made.push(sid);
        }
        let mut f = acp(persona, state, &a, &Scenario::default());
        f.send(
            &json!({"jsonrpc":"2.0","id":3,"method":"session/list","params":{
            "cwd":a.to_str().unwrap()}}),
        );
        let r = f.recv_until(|m| m["id"] == 3).pop().unwrap();
        let ids: Vec<&str> = r["result"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["sessionId"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), 2, "{persona}: {ids:?}");
        assert!(ids.contains(&made[0].as_str()) && ids.contains(&made[2].as_str()));
        assert!(!ids.contains(&made[1].as_str()), "{persona}: other folder");
        // Newest first: the one created last.
        assert_eq!(ids[0], made[2], "{persona}");
        let stamps: Vec<&str> = r["result"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["updatedAt"].as_str().unwrap())
            .collect();
        assert!(stamps[0] >= stamps[1], "{stamps:?}");
    });
}

fn session_list_json(persona: &str, state: &Path, cwd: &Path) -> String {
    let out = std::process::Command::new(common::BIN)
        .args(["session", "list", "--format", "json"])
        .env("WASIT_FAKE_PERSONA", persona)
        .env("WASIT_FAKE_STATE_DIR", state)
        .env_remove("WASIT_FAKE_SCENARIO")
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn session_list_command_shows_the_stored_sessions_of_the_folder_it_runs_in() {
    each_acp(|persona| {
        let state = scratch_dir();
        let state = state.as_ref();
        let (a, b) = (project(state, "a"), project(state, "b"));
        let mut made = Vec::new();
        for dir in [&a, &b] {
            let mut f = acp(persona, state, dir, &Scenario::default());
            f.send(
                &json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{
                "cwd":dir.to_str().unwrap(),"mcpServers":[]}}),
            );
            let r = f.recv_until(|m| m["id"] == 2).pop().unwrap();
            made.push(r["result"]["sessionId"].as_str().unwrap().to_string());
        }
        let here: Value = serde_json::from_str(&session_list_json(persona, state, &a)).unwrap();
        let here = here.as_array().unwrap();
        assert_eq!(here.len(), 1, "{persona}: {here:?}");
        assert_eq!(here[0]["id"], made[0].as_str());
        assert_eq!(here[0]["directory"], a.to_str().unwrap());
        // The fields the driver's `parse_sessions` reads, with times that mean something.
        assert!(here[0]["updated"].as_u64().unwrap() > 1_700_000_000_000);
        assert!(here[0]["title"].is_string());
        // A folder with none: nothing at all is printed, as the real CLIs do.
        let empty = project(state, "empty");
        assert_eq!(session_list_json(persona, state, &empty), "");
    });
}
