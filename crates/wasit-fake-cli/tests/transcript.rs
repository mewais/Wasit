//! The claude transcript the fake writes has the real file's shape (claude 2.1.293): chained
//! `parentUuid`s, one `assistant` record per content block, tool results with `toolUseResult`,
//! subagents in their own files, compaction boundaries, bookkeeping records; and a resumed run
//! appends to the same file, chained to the leaf the previous run left.

mod common;

use common::{Fake, CLAUDE_ARGV};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use wasit_fake_cli::scenario::{
    BackgroundStep, HistoryMessage, Scenario, SessionInfo, Step, ToolStep, Turn,
};
use wasit_fake_cli::transcript::{
    encode_project_dir, subagent_path, transcript_path, Content, SubagentSpec, Transcript, Writer,
};

const ID: &str = "3b241101-e2bb-4255-8caf-4136c566a962";

fn records(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{l}: {e}")))
        .collect()
}

fn types(rs: &[Value]) -> BTreeSet<String> {
    rs.iter()
        .map(|r| r["type"].as_str().unwrap().to_string())
        .collect()
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` to milliseconds since the epoch (days-from-civil, no dependency).
fn parse_ts(ts: &str) -> u64 {
    let b = ts.as_bytes();
    assert_eq!(b.len(), 24, "{ts}");
    assert!(b[4] == b'-' && b[7] == b'-' && b[10] == b'T' && b[13] == b':' && b[16] == b':');
    assert!(b[19] == b'.' && b[23] == b'Z', "{ts}");
    let n = |r: std::ops::Range<usize>| ts[r].parse::<i64>().unwrap();
    let (y, m, d) = (n(0..4), n(5..7), n(8..10));
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + n(11..13) * 3600 + n(14..16) * 60 + n(17..19);
    (secs * 1000 + n(20..23)) as u64
}

/// Every record with a `uuid` has a `parentUuid` that is null or names an earlier record
/// (or, for a compaction boundary, a `logicalParentUuid` that does), and every message record
/// has the fields the real ones carry. Returns uuid -> parent.
fn check_chain(rs: &[Value]) -> HashMap<String, Option<String>> {
    let mut seen: HashMap<String, Option<String>> = HashMap::new();
    for r in rs {
        let Some(uuid) = r["uuid"].as_str() else {
            assert!(
                r["sessionId"].is_string(),
                "a bookkeeping record carries the session id: {r}"
            );
            continue;
        };
        for k in [
            "isSidechain",
            "timestamp",
            "userType",
            "entrypoint",
            "cwd",
            "sessionId",
            "version",
            "gitBranch",
        ] {
            assert!(!r[k].is_null(), "{k} missing in {r}");
        }
        parse_ts(r["timestamp"].as_str().unwrap());
        match r["parentUuid"].as_str() {
            Some(p) => assert!(
                seen.contains_key(p),
                "parent {p} of {uuid} not written before"
            ),
            None => assert!(r["parentUuid"].is_null(), "{r}"),
        }
        if let Some(l) = r["logicalParentUuid"].as_str() {
            assert!(r["parentUuid"].is_null());
            assert!(seen.contains_key(l));
        }
        assert!(
            seen.insert(
                uuid.to_string(),
                r["parentUuid"].as_str().map(str::to_string)
            )
            .is_none(),
            "uuid {uuid} twice"
        );
    }
    seen
}

fn leaf(rs: &[Value]) -> String {
    rs.iter()
        .rev()
        .find_map(|r| r["uuid"].as_str())
        .unwrap()
        .to_string()
}

fn scratch(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "wasit-transcript-{name}-{}-{}",
        std::process::id(),
        wasit_fake_cli::util::hex(8)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

struct Clean(PathBuf);

impl Drop for Clean {
    fn drop(&mut self) {
        // Best-effort removal of a scratch directory.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn everything() -> Transcript {
    Transcript::deterministic(11)
        .titles("a named conversation")
        .user("first question")
        .thinking()
        .assistant("first answer")
        .user_typed("second question")
        .tool(
            "Bash",
            json!({"command":"ls","description":"list"}),
            "a b c",
        )
        .tool_error("Bash", json!({"command":"false"}), "exit 1")
        .tool_image(
            "Read",
            json!({"file_path":"/p/x.png"}),
            "image/png",
            "iVBORw0KGgo=",
        )
        .subagent(
            SubagentSpec::new("probe the thing", "look into the thing")
                .content(Content::text("looking"))
                .content(Content::tool("Grep", json!({"pattern":"x"}), "match"))
                .answer("found it"),
        )
        .subagent(SubagentSpec::new("background probe", "do it later").background())
        .assistant("second answer")
        .compaction()
        .user_with_image("third question", "image/png", "iVBORw0KGgo=")
        .assistant("third answer")
        .rewind(1)
        .user("third question, edited")
        .assistant("edited answer")
        .unknown_record("future-record-kind")
        .ai_title("a title")
        .mode("normal")
}

#[test]
fn a_written_transcript_has_the_shape_of_the_real_file() {
    let dir = scratch("shape");
    let _c = Clean(dir.clone());
    let path = everything().write(&dir, "/work/some proj", ID);
    assert_eq!(
        path,
        dir.join("projects")
            .join(encode_project_dir("/work/some proj"))
            .join(format!("{ID}.jsonl"))
    );
    assert_eq!(path, transcript_path(&dir, "/work/some proj", ID));
    let rs = records(&path);
    let chain = check_chain(&rs);
    let t = types(&rs);
    for want in [
        "queue-operation",
        "user",
        "assistant",
        "attachment",
        "system",
        "last-prompt",
        "cost-state",
        "mode",
        "atis-latch",
        "ai-title",
        "custom-title",
        "agent-name",
        "future-record-kind",
    ] {
        assert!(t.contains(want), "no {want} record in {t:?}");
    }

    // A stream-json prompt is an array of blocks, a typed one a string; both carry the prompt
    // fields.
    let users: Vec<&Value> = rs
        .iter()
        .filter(|r| r["type"] == "user" && r["turnPosition"].is_object())
        .collect();
    assert!(users[0]["message"]["content"].is_array());
    assert!(users[1]["message"]["content"].is_string());
    for u in &users {
        for k in ["promptId", "permissionMode", "promptSource", "turnOrigin"] {
            assert!(u[k].is_string(), "{k} in {u}");
        }
        assert_eq!(u["message"]["role"], "user");
    }
    assert_eq!(users[0]["parentUuid"], Value::Null);
    let turn_indexes: Vec<u64> = users
        .iter()
        .map(|u| u["turnPosition"]["turnIndex"].as_u64().unwrap())
        .collect();
    assert_eq!(turn_indexes, (1..=users.len() as u64).collect::<Vec<_>>());

    // The first prompt of the conversation comes with its attachments.
    let kinds: BTreeSet<&str> = rs
        .iter()
        .filter(|r| r["type"] == "attachment")
        .map(|r| r["attachment"]["type"].as_str().unwrap())
        .collect();
    assert!(
        kinds.contains("environment") && kinds.contains("date"),
        "{kinds:?}"
    );

    // One assistant record per block; a thinking block has no text and a signature; every record
    // is a message of its own here.
    let thinking = rs
        .iter()
        .find(|r| r["type"] == "assistant" && r["message"]["content"][0]["type"] == "thinking")
        .unwrap();
    assert_eq!(thinking["message"]["content"][0]["thinking"], "");
    assert!(thinking["message"]["content"][0]["signature"].is_string());
    for a in rs.iter().filter(|r| r["type"] == "assistant") {
        assert_eq!(a["message"]["content"].as_array().unwrap().len(), 1);
        assert_eq!(a["message"]["role"], "assistant");
        assert!(a["message"]["id"].is_string() && a["requestId"].is_string());
        assert!(a["apiBlockIndex"].is_number());
    }

    // A tool result: the block, the structured result and the assistant record it answers.
    let uses: HashMap<&str, &Value> = rs
        .iter()
        .filter(|r| r["type"] == "assistant" && r["message"]["content"][0]["type"] == "tool_use")
        .map(|r| (r["message"]["content"][0]["id"].as_str().unwrap(), r))
        .collect();
    let results: Vec<&Value> = rs
        .iter()
        .filter(|r| r["message"]["content"][0]["type"] == "tool_result")
        .collect();
    assert_eq!(results.len(), uses.len());
    for r in &results {
        let block = &r["message"]["content"][0];
        let call = block["tool_use_id"].as_str().unwrap();
        let used = uses[call];
        assert_eq!(r["sourceToolAssistantUUID"], used["uuid"]);
        assert!(!r["toolUseResult"].is_null());
        assert_eq!(r["parentUuid"], used["uuid"], "a result follows its call");
    }
    assert!(results
        .iter()
        .any(|r| r["message"]["content"][0]["is_error"] == true));
    let image = results
        .iter()
        .find(|r| r["message"]["content"][0]["content"][0]["type"] == "image")
        .unwrap();
    assert_eq!(image["toolUseResult"]["type"], "image");
    assert!(image["toolUseResult"]["file"]["base64"].is_string());

    // Subagents are files of their own, linked by `agentId`, with their own chain.
    let spawns: Vec<&Value> = rs
        .iter()
        .filter(|r| r["toolUseResult"]["agentId"].is_string())
        .collect();
    assert_eq!(spawns.len(), 2);
    assert_eq!(spawns[0]["toolUseResult"]["status"], "completed");
    assert_eq!(spawns[1]["toolUseResult"]["status"], "async_launched");
    for s in &spawns {
        let agent = s["toolUseResult"]["agentId"].as_str().unwrap();
        let file = subagent_path(&path, agent);
        let sub = records(&file);
        check_chain(&sub);
        assert_eq!(sub[0]["parentUuid"], Value::Null);
        assert_eq!(sub[0]["type"], "user");
        assert!(sub[0]["message"]["content"].is_string(), "the prompt");
        assert!(sub
            .iter()
            .all(|r| r["isSidechain"] == true && r["agentId"] == agent));
        let meta: Value = serde_json::from_str(
            &std::fs::read_to_string(file.with_extension("meta.json")).unwrap(),
        )
        .unwrap();
        let call = s["message"]["content"][0]["tool_use_id"].as_str().unwrap();
        assert_eq!(meta["toolUseId"], call);
        for k in ["agentType", "description", "spawnDepth", "model"] {
            assert!(!meta[k].is_null(), "{k} in {meta}");
        }
        assert!(uses[call]["message"]["content"][0]["name"] == "Agent");
    }
    let first_sub = records(&subagent_path(
        &path,
        spawns[0]["toolUseResult"]["agentId"].as_str().unwrap(),
    ));
    assert!(types(&first_sub).contains("assistant"));
    assert!(first_sub
        .iter()
        .any(|r| r["message"]["content"][0]["name"] == "Grep"));

    // Compaction: a boundary with no parent that points at the leaf before it, then the summary.
    let b = rs
        .iter()
        .position(|r| r["subtype"] == "compact_boundary")
        .unwrap();
    assert_eq!(rs[b]["parentUuid"], Value::Null);
    assert_eq!(rs[b]["logicalParentUuid"].as_str().unwrap(), {
        let before = rs[..b].iter().rev().find_map(|r| r["uuid"].as_str());
        before.unwrap()
    });
    assert!(rs[b]["compactMetadata"]["preTokens"].is_number());
    assert_eq!(rs[b + 1]["isCompactSummary"], true);
    assert_eq!(rs[b + 1]["isVisibleInTranscriptOnly"], true);
    assert_eq!(rs[b + 1]["parentUuid"], rs[b]["uuid"]);

    // A branch: the edited prompt is a second child of the node the replaced prompt hangs from,
    // and it was written later in the file.
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for (u, p) in &chain {
        if let Some(p) = p {
            children.entry(p).or_default().push(u);
        }
    }
    let branch_points: Vec<_> = children.iter().filter(|(_, c)| c.len() > 1).collect();
    assert_eq!(branch_points.len(), 1, "{branch_points:?}");
    let order: Vec<&str> = rs.iter().filter_map(|r| r["uuid"].as_str()).collect();
    let (_, kids) = branch_points[0];
    let pos = |u: &str| order.iter().position(|x| *x == u).unwrap();
    let (old, new) = (
        kids.iter().min_by_key(|u| pos(u)).unwrap(),
        kids.iter().max_by_key(|u| pos(u)).unwrap(),
    );
    // The old branch's records sit between the two in the file: file order is not chain order.
    assert!(pos(new) > pos(old) + 1);
    // The leaf is on the new branch.
    let mut cursor = Some(leaf(&rs));
    let mut on_leaf_chain = BTreeSet::new();
    while let Some(c) = cursor {
        on_leaf_chain.insert(c.clone());
        cursor = chain[&c].clone();
    }
    assert!(on_leaf_chain.contains(*new) && !on_leaf_chain.contains(*old));
}

#[test]
fn a_deterministic_transcript_is_the_same_bytes_every_time_and_times_increase() {
    let (a, b) = (scratch("det-a"), scratch("det-b"));
    let _c = (Clean(a.clone()), Clean(b.clone()));
    let pa = everything().write(&a, "/w", ID);
    let pb = everything().write(&b, "/w", ID);
    let (ta, tb) = (
        std::fs::read_to_string(&pa).unwrap(),
        std::fs::read_to_string(&pb).unwrap(),
    );
    // Ids derived from the seed and counter: identical, apart from what is random by nature
    // (message and request ids, the signature of a thinking block).
    let norm = |t: &str| -> Vec<Value> {
        t.lines()
            .map(|l| {
                let mut v: Value = serde_json::from_str(l).unwrap();
                for k in ["requestId", "toolUseResult"] {
                    if v.get(k).is_some() {
                        v[k] = Value::Null;
                    }
                }
                if let Some(m) = v.get_mut("message") {
                    m["id"] = Value::Null;
                    if let Some(b) = m["content"].get_mut(0) {
                        for k in ["signature", "id", "tool_use_id"] {
                            if b.get(k).is_some() {
                                b[k] = Value::Null;
                            }
                        }
                    }
                }
                v
            })
            .collect()
    };
    assert_eq!(norm(&ta), norm(&tb));
    let ts: Vec<u64> = records(&pa)
        .iter()
        .filter_map(|r| r["timestamp"].as_str().map(parse_ts))
        .collect();
    assert!(ts.windows(2).all(|w| w[0] < w[1]), "{ts:?}");
}

#[test]
fn a_cut_last_line_is_left_cut_and_an_append_starts_a_new_line() {
    let dir = scratch("cut");
    let _c = Clean(dir.clone());
    let path = Transcript::deterministic(3)
        .user("q")
        .assistant("a")
        .truncated_tail()
        .write(&dir, "/w", ID);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.ends_with('\n'));
    let (whole, cut) = text.rsplit_once('\n').unwrap();
    assert!(serde_json::from_str::<Value>(cut).is_err(), "{cut}");
    let good = whole.lines().count();
    // Appending (a resumed run) goes on after the cut line, chained to the last whole record.
    let last = leaf(
        &whole
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect::<Vec<_>>(),
    );
    Transcript::deterministic(4)
        .user("again")
        .append(&dir, "/w", ID);
    let after = std::fs::read_to_string(&path).unwrap();
    assert!(
        after.starts_with(&format!("{text}\n")),
        "the cut line stays, ended"
    );
    assert!(after.lines().count() > good + 1);
    let parsed: Vec<Value> = after
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let again = parsed
        .iter()
        .find(|r| r["message"]["content"][0]["text"] == "again")
        .unwrap();
    assert_eq!(again["parentUuid"].as_str().unwrap(), last);
}

#[test]
fn filler_makes_a_big_transcript_of_about_the_size_asked_for() {
    let dir = scratch("filler");
    let _c = Clean(dir.clone());
    let path = Transcript::deterministic(5)
        .filler(500, 4096)
        .write(&dir, "/w", ID);
    let size = std::fs::metadata(&path).unwrap().len();
    // Each turn is its records (with their fixed fields) around about `bytes_per_turn` of text.
    assert!(size > 500 * 4096, "{size}");
    assert!(size < 500 * 4096 * 3, "{size}");
    let rs = records(&path);
    check_chain(&rs);
    assert_eq!(
        rs.iter().filter(|r| r["turnPosition"].is_object()).count(),
        500
    );
}

#[test]
fn a_writer_records_one_assistant_record_per_block_with_one_message_id() {
    let dir = scratch("blocks");
    let _c = Clean(dir.clone());
    let path = transcript_path(&dir, "/w", ID);
    let mut w = Writer::open(path.clone(), ID, "/w");
    w.begin_turn(json!("q"));
    w.assistant_message(
        vec![
            json!({"type":"text","text":"one"}),
            json!({"type":"tool_use","id":"toolu_x","name":"Bash","input":{}}),
        ],
        "tool_use",
    );
    w.end_turn();
    w.flush().unwrap();
    let rs = records(&path);
    let a: Vec<&Value> = rs.iter().filter(|r| r["type"] == "assistant").collect();
    assert_eq!(a.len(), 2);
    assert_eq!(a[0]["message"]["id"], a[1]["message"]["id"]);
    assert_eq!(
        (
            a[0]["apiBlockIndex"].as_u64(),
            a[1]["apiBlockIndex"].as_u64()
        ),
        (Some(0), Some(1))
    );
    assert_eq!(a[0]["message"]["stop_reason"], Value::Null);
    assert_eq!(a[1]["message"]["stop_reason"], "tool_use");
    assert_eq!(a[1]["parentUuid"], a[0]["uuid"]);
}

// ── the persona ─────────────────────────────────────────────────────────────

fn claude_run(
    state: &Path,
    cwd: &Path,
    scenario: &Scenario,
    extra: &[&str],
    prompt: Option<&str>,
) -> Fake {
    let mut fake = Fake::spawn_in(
        &[CLAUDE_ARGV.as_slice(), extra].concat(),
        Some("claude"),
        scenario,
        Some(state),
        Some(cwd),
    );
    fake.send(&json!({"type":"control_request","request_id":"init","request":{"subtype":"initialize","hooks":null}}));
    fake.recv_until(|m| m["type"] == "control_response");
    if let Some(p) = prompt {
        fake.send(
            &json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":p}]}}),
        );
        fake.recv_until(|m| m["type"] == "result");
    }
    fake.close_stdin();
    fake
}

fn stored(state: &Path, cwd: &Path) -> PathBuf {
    transcript_path(&state.join("claude-config"), &cwd.to_string_lossy(), ID)
}

#[test]
fn a_run_leaves_a_chained_transcript_and_a_resumed_run_appends_to_it() {
    let (state, cwd) = (common::scratch_dir(), common::scratch_dir());
    let (state, cwd) = (state.as_ref(), cwd.as_ref());
    let scenario = Scenario::builder()
        .turn(Turn::builder().say("first reply"))
        .build();
    let scenario2 = Scenario::builder()
        .turn(Turn::builder().say("second reply"))
        .build();
    let mut f = claude_run(
        state,
        cwd,
        &scenario,
        &["--session-id", ID],
        Some("first prompt"),
    );
    f.wait_exit().expect("exit");
    let path = stored(state, cwd);
    let rs = records(&path);
    check_chain(&rs);
    let t = types(&rs);
    for want in [
        "queue-operation",
        "user",
        "attachment",
        "assistant",
        "last-prompt",
        "cost-state",
    ] {
        assert!(t.contains(want), "{want} in {t:?}");
    }
    assert!(rs.iter().all(|r| r["sessionId"] == ID));
    let user = rs.iter().find(|r| r["type"] == "user").unwrap();
    assert_eq!(user["message"]["content"][0]["text"], "first prompt");
    assert_eq!(user["parentUuid"], Value::Null);
    assert_eq!(user["entrypoint"], "sdk-cli");
    assert_eq!(user["cwd"], cwd.to_string_lossy().as_ref());
    let a = rs.iter().find(|r| r["type"] == "assistant").unwrap();
    assert_eq!(a["message"]["content"][0]["text"], "first reply");
    // The bookkeeping that follows the last message names the leaf.
    let lp = rs
        .iter()
        .rev()
        .find(|r| r["type"] == "last-prompt")
        .unwrap();
    assert_eq!(lp["lastPrompt"], "first prompt");
    assert_eq!(lp["leafUuid"].as_str().unwrap(), leaf(&rs));
    let before = leaf(&rs);
    let count = rs.len();

    // A second run, resuming it, appends to the same file; its first record chains to the leaf.
    let mut f = claude_run(
        state,
        cwd,
        &scenario2,
        &["--resume", ID],
        Some("second prompt"),
    );
    f.wait_exit().expect("exit");
    let rs2 = records(&path);
    check_chain(&rs2);
    assert_eq!(&rs2[..count], &rs[..]);
    let first_new = rs2[count..].iter().find(|r| r["uuid"].is_string()).unwrap();
    assert_eq!(first_new["type"], "user");
    assert_eq!(first_new["parentUuid"].as_str().unwrap(), before);
    assert_eq!(first_new["turnPosition"]["turnIndex"], 2);
    assert!(rs2[count..].iter().any(|r| r["type"] == "queue-operation"));
    assert!(rs2
        .iter()
        .any(|r| r["type"] == "assistant" && r["message"]["content"][0]["text"] == "second reply"));
}

#[test]
fn a_tool_turn_leaves_a_tool_use_and_its_result() {
    let (state, cwd) = (common::scratch_dir(), common::scratch_dir());
    let (state, cwd) = (state.as_ref(), cwd.as_ref());
    let scenario = Scenario::builder()
        .turn(
            Turn::builder()
                .think("hm")
                .tool("Bash", json!({"command":"echo hi","description":"Say hi"}))
                .result_if_allowed("hi")
                .done()
                .say("said hi"),
        )
        .build();
    let mut f = claude_run(state, cwd, &scenario, &["--session-id", ID], Some("do it"));
    f.wait_exit().expect("exit");
    let rs = records(&stored(state, cwd));
    check_chain(&rs);
    let msgs: Vec<&Value> = rs
        .iter()
        .filter(|r| r["type"] == "assistant" || r["message"]["content"][0]["type"] == "tool_result")
        .collect();
    let kinds: Vec<String> = msgs
        .iter()
        .map(|r| {
            r["message"]["content"][0]["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(kinds, ["thinking", "tool_use", "tool_result", "text"]);
    let (used, result) = (msgs[1], msgs[2]);
    assert_eq!(used["message"]["content"][0]["name"], "Bash");
    assert_eq!(used["message"]["content"][0]["input"]["command"], "echo hi");
    assert_eq!(
        result["message"]["content"][0]["tool_use_id"],
        used["message"]["content"][0]["id"]
    );
    assert_eq!(result["message"]["content"][0]["content"], "hi");
    assert_eq!(result["sourceToolAssistantUUID"], used["uuid"]);
    assert_eq!(result["toolUseResult"]["stdout"], "hi");
    assert_eq!(result["parentUuid"], used["uuid"]);
}

#[test]
fn a_denied_tool_call_is_recorded_as_an_error_result_and_an_interrupt_as_the_cli_writes_it() {
    let (state, cwd) = (common::scratch_dir(), common::scratch_dir());
    let (state, cwd) = (state.as_ref(), cwd.as_ref());
    let scenario = Scenario::builder()
        .turn(
            Turn::builder()
                .tool("Bash", json!({"command":"rm x"}))
                .needs_permission()
                .result_if_denied("no")
                .done()
                .say("ok then"),
        )
        .build();
    let mut f = Fake::spawn_in(
        &[CLAUDE_ARGV.as_slice(), &["--session-id", ID]].concat(),
        Some("claude"),
        &scenario,
        Some(state),
        Some(cwd),
    );
    f.send(&json!({"type":"control_request","request_id":"init","request":{"subtype":"initialize","hooks":null}}));
    f.recv_until(|m| m["type"] == "control_response");
    f.send(
        &json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"go"}]}}),
    );
    let ask = f
        .recv_until(|m| m["type"] == "control_request")
        .pop()
        .unwrap();
    f.send(
        &json!({"type":"control_response","response":{"subtype":"success",
        "request_id":ask["request_id"],"response":{"behavior":"deny","message":"not allowed"}}}),
    );
    f.recv_until(|m| m["type"] == "result");
    f.close_stdin();
    f.wait_exit().expect("exit");
    let rs = records(&stored(state, cwd));
    check_chain(&rs);
    let result = rs
        .iter()
        .find(|r| r["message"]["content"][0]["type"] == "tool_result")
        .unwrap();
    assert_eq!(result["message"]["content"][0]["is_error"], true);
    assert_eq!(result["message"]["content"][0]["content"], "not allowed");
}

#[test]
fn a_named_conversation_starts_with_its_titles() {
    let (state, cwd) = (common::scratch_dir(), common::scratch_dir());
    let (state, cwd) = (state.as_ref(), cwd.as_ref());
    let mut f = claude_run(
        state,
        cwd,
        &Scenario::default(),
        &["--session-id", ID, "--name", "my work"],
        Some("hello"),
    );
    f.wait_exit().expect("exit");
    let rs = records(&stored(state, cwd));
    assert_eq!(rs[0]["type"], "custom-title");
    assert_eq!(rs[0]["customTitle"], "my work");
    assert_eq!(rs[1]["type"], "agent-name");
}

#[test]
fn a_scenario_session_is_a_real_transcript_when_it_is_resumed() {
    let (state, cwd) = (common::scratch_dir(), common::scratch_dir());
    let (state, cwd) = (state.as_ref(), cwd.as_ref());
    let scenario = Scenario::builder()
        .session(SessionInfo {
            id: ID.into(),
            title: Some("an old one".into()),
            cwd: None,
            history: vec![
                HistoryMessage {
                    role: "user".into(),
                    text: "old question".into(),
                },
                HistoryMessage {
                    role: "assistant".into(),
                    text: "old answer".into(),
                },
            ],
        })
        .turn(Turn::builder().say("new answer"))
        .build();
    let mut f = claude_run(
        state,
        cwd,
        &scenario,
        &["--resume", ID],
        Some("new question"),
    );
    f.wait_exit().expect("exit");
    let rs = records(&stored(state, cwd));
    check_chain(&rs);
    let texts: Vec<&str> = rs
        .iter()
        .filter_map(|r| {
            r["message"]["content"][0]["text"]
                .as_str()
                .filter(|_| r["type"] == "user" || r["type"] == "assistant")
        })
        .collect();
    assert_eq!(
        texts,
        ["old question", "old answer", "new question", "new answer"]
    );
    // The new prompt chains to the history's leaf, and the title is the stored one.
    let new_q = rs
        .iter()
        .find(|r| r["message"]["content"][0]["text"] == "new question")
        .unwrap();
    let old_a = rs
        .iter()
        .find(|r| r["message"]["content"][0]["text"] == "old answer")
        .unwrap();
    assert_eq!(new_q["parentUuid"], old_a["uuid"]);
    assert!(rs.iter().any(|r| r["customTitle"] == "an old one"));
}

#[test]
fn a_background_subagent_is_written_to_its_own_file_linked_from_the_call() {
    let (state, cwd) = (common::scratch_dir(), common::scratch_dir());
    let (state, cwd) = (state.as_ref(), cwd.as_ref());
    let scenario = Scenario::builder()
        .turn(Turn::builder().background(BackgroundStep {
            kind: "local_agent".into(),
            description: "probe".into(),
            subagent_type: Some("general-purpose".into()),
            after_ms: 10,
            steps: vec![
                Step::Tool(ToolStep {
                    name: "Bash".into(),
                    input: json!({"command":"echo hi","description":"Say hi"}),
                    result_if_allowed: Some("hi".into()),
                    ..Default::default()
                }),
                Step::Say {
                    text: "agent says hi".into(),
                    chunks: None,
                },
            ],
            ..BackgroundStep::default()
        }))
        .build();
    let mut f = Fake::spawn_in(
        &[CLAUDE_ARGV.as_slice(), &["--session-id", ID]].concat(),
        Some("claude"),
        &scenario,
        Some(state),
        Some(cwd),
    );
    f.send(&json!({"type":"control_request","request_id":"init","request":{"subtype":"initialize","hooks":null}}));
    f.recv_until(|m| m["type"] == "control_response");
    f.send(&json!({"type":"user","message":{"role":"user","content":[{"type":"text","text":"spawn"}]}}));
    f.recv_until(|m| m["type"] == "result");
    // The task runs while the session is idle; its end is announced.
    f.recv_until(|m| m["type"] == "system" && m["subtype"] == "task_notification");
    f.close_stdin();
    f.wait_exit().expect("exit");

    let main = stored(state, cwd);
    let rs = records(&main);
    check_chain(&rs);
    let launched = rs
        .iter()
        .find(|r| r["toolUseResult"]["agentId"].is_string())
        .expect("the spawning call's result links to the subagent");
    assert_eq!(launched["toolUseResult"]["status"], "async_launched");
    let agent = launched["toolUseResult"]["agentId"].as_str().unwrap();
    let call = launched["message"]["content"][0]["tool_use_id"]
        .as_str()
        .unwrap();
    // The live stream names the same subagent.
    assert!(f.seen.iter().any(|m| m["agent_id"] == agent));
    let sub = records(&subagent_path(&main, agent));
    check_chain(&sub);
    assert_eq!(sub[0]["type"], "user");
    assert_eq!(sub[0]["parentUuid"], Value::Null);
    assert!(sub
        .iter()
        .all(|r| r["isSidechain"] == true && r["agentId"] == agent));
    let kinds: Vec<&str> = sub
        .iter()
        .skip(1)
        .map(|r| r["message"]["content"][0]["type"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["tool_use", "tool_result", "text"]);
    let meta: Value = serde_json::from_str(
        &std::fs::read_to_string(subagent_path(&main, agent).with_extension("meta.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["toolUseId"], call);
    assert_eq!(meta["agentType"], "general-purpose");
    // None of the subagent's conversation is in the main chain.
    assert!(!rs
        .iter()
        .any(|r| r["message"]["content"][0]["text"] == "agent says hi"));
}

/// A 20 MB transcript is quick to make (tests of an import that must not hold a session up).
#[test]
fn filler_makes_twenty_megabytes_quickly_and_a_resume_chains_to_its_leaf() {
    let dir = scratch("twenty");
    let _c = Clean(dir.clone());
    let path = Transcript::deterministic(9)
        .filler(4000, 5000)
        .write(&dir, "/w", ID);
    let size = std::fs::metadata(&path).unwrap().len();
    assert!(size >= 20 * 1024 * 1024, "{size}");
    // Appending to it finds the leaf without parsing the whole file.
    let last_uuid_line = |text: &str, needle: &str| -> Value {
        let l = text
            .lines()
            .rev()
            .find(|l| l.contains(needle))
            .expect("record");
        serde_json::from_str(l).unwrap()
    };
    let before = last_uuid_line(&std::fs::read_to_string(&path).unwrap(), "\"uuid\":\"")["uuid"]
        .as_str()
        .unwrap()
        .to_string();
    Transcript::deterministic(10)
        .user("more")
        .append(&dir, "/w", ID);
    let more = last_uuid_line(
        &std::fs::read_to_string(&path).unwrap(),
        "\"text\":\"more\"",
    );
    assert_eq!(more["parentUuid"].as_str().unwrap(), before);
}
