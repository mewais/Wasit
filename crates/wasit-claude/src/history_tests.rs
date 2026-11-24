//! The history parser over sanitised fixtures (`tests/fixtures/history`: invented content, the
//! structure of real claude 2.1.293 transcripts) and generated transcripts.

use serde_json::{json, Value};
use wasit_core::{
    AgentEvent, ContentPart, DiscoveryContext, FileProblem, HistoryFailure, HistoryLimits,
    HistoryParse, HistoryParser, MediaSource,
};

use crate::history::ClaudeHistory;

fn fixture(name: &str) -> String {
    format!(
        "{}/tests/fixtures/history/{name}",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// What the runtime does with a parser: read the main file, then every file the parser wants,
/// line by line; a file that is not there is reported.
fn run_with(parser: Box<dyn HistoryParser>, main: &str) -> Result<HistoryParse, HistoryFailure> {
    let mut p = parser;
    let mut queue = vec![main.to_string()];
    let mut done = 0;
    while done < queue.len() {
        let file = queue[done].clone();
        done += 1;
        match std::fs::read(&file) {
            Ok(bytes) => {
                p.begin(&file, bytes.len() as u64);
                feed(&mut *p, &bytes);
                p.end();
            }
            Err(_) => p.unavailable(&file, FileProblem::Missing),
        }
        queue.extend(p.wants());
    }
    p.finish()
}

fn feed(p: &mut dyn HistoryParser, bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    let ends = text.ends_with('\n');
    let lines: Vec<&str> = text.split('\n').collect();
    let n = lines.len();
    for (i, line) in lines.into_iter().enumerate() {
        if i + 1 == n {
            if !line.is_empty() {
                p.push(line, ends);
            }
        } else {
            p.push(line, true);
        }
    }
}

fn parse(path: &str) -> HistoryParse {
    parse_with(path, HistoryLimits::default())
}

fn parse_with(path: &str, limits: HistoryLimits) -> HistoryParse {
    run_with(Box::new(ClaudeHistory::new(limits)), path).expect("understood")
}

fn inner(e: &AgentEvent) -> (&AgentEvent, Option<u64>) {
    match e {
        AgentEvent::History { at_ms, event } => (event, *at_ms),
        other => panic!("not history: {other:?}"),
    }
}

/// A one-line summary of each event, attributed events prefixed by their agent.
fn summary(events: &[AgentEvent]) -> Vec<String> {
    events
        .iter()
        .map(|e| {
            let (ev, _) = inner(e);
            let (scope, ev) = ev.unscoped();
            let tag = scope
                .map(|s| format!("[{}:{}] ", s.call_id, s.agent_id.as_deref().unwrap_or("-")))
                .unwrap_or_default();
            let body = match ev {
                AgentEvent::UserMessage { text } => format!("user: {text}"),
                AgentEvent::UserContent { parts } => format!("user-parts: {}", parts.len()),
                AgentEvent::AssistantDelta { text, .. } => format!("assistant: {text}"),
                AgentEvent::ThinkingDelta { text, .. } => format!("thinking: {text}"),
                AgentEvent::ToolCallStarted {
                    name,
                    call_id,
                    is_subagent,
                    ..
                } => format!("call {name} {call_id} sub={is_subagent}"),
                AgentEvent::ToolCallFinished {
                    name,
                    call_id,
                    ok,
                    parts,
                    ..
                } => format!("result {name} {call_id} ok={ok} parts={}", parts.len()),
                AgentEvent::Compaction { info } => {
                    format!(
                        "compaction {} {}",
                        info.trigger.as_deref().unwrap_or(""),
                        info.pre_tokens.unwrap_or(0)
                    )
                }
                other => panic!("unexpected {other:?}"),
            };
            format!("{tag}{body}")
        })
        .collect()
}

fn no_live_events(events: &[AgentEvent]) {
    for e in events {
        let (ev, _) = inner(e);
        let (_, ev) = ev.unscoped();
        assert!(
            !matches!(
                ev,
                AgentEvent::UsageUpdate { .. }
                    | AgentEvent::TurnStarted { .. }
                    | AgentEvent::TurnCompleted { .. }
                    | AgentEvent::TaskStarted { .. }
                    | AgentEvent::TaskProgress { .. }
                    | AgentEvent::TaskUpdated { .. }
                    | AgentEvent::TaskEnded { .. }
                    | AgentEvent::BackgroundTasksChanged { .. }
                    | AgentEvent::GateRequested { .. }
                    | AgentEvent::GateResolved { .. }
                    | AgentEvent::SessionInit { .. }
            ),
            "{ev:?}"
        );
    }
}

#[test]
fn the_conversation_is_the_chain_from_the_last_record_in_file_order() {
    let r = parse(&fixture("session1.jsonl"));
    no_live_events(&r.events);
    let got = summary(&r.events);
    let expect = [
        "user: Please list the files.",
        "assistant: Let me look.",
        "call Bash toolu_1 sub=false",
        "result Bash toolu_1 ok=true parts=0",
        "thinking: Two files, report them.",
        "assistant: Two files.",
        // The edited branch, not the abandoned one (which comes first in the file).
        "user: Edited second question",
        "call Read toolu_2 sub=false",
        "result Read toolu_2 ok=false parts=0",
        "user: Look at this",
        "user-parts: 1",
        "assistant: I see an image.",
        "call Agent toolu_3 sub=true",
        "[toolu_3:agent1abc] user: Count the files",
        "[toolu_3:agent1abc] call Bash toolu_s1 sub=false",
        "[toolu_3:agent1abc] result Bash toolu_s1 ok=true parts=0",
        "[toolu_3:agent1abc] assistant: There are two files.",
        "result Agent toolu_3 ok=true parts=0",
        "call Bash toolu_4 sub=false",
        "result Bash toolu_4 ok=true parts=1",
        // The compaction boundary continues the chain through `logicalParentUuid`; its summary
        // record is not shown.
        "compaction auto 900000",
        "user: After compaction question",
        "assistant: Final answer.",
    ];
    assert_eq!(got, expect, "{got:#?}");
    // What was seen and not shown is counted, never silently dropped.
    let rep = &r.report;
    assert_eq!(rep.events, expect.len() as u64);
    assert_eq!(rep.unrecognised, 1);
    assert_eq!(rep.unrecognised_kinds, ["future-thing"]);
    assert_eq!(rep.unreadable, 1, "the malformed middle line");
    assert!(rep.truncated_tail, "the cut last line");
    assert_eq!(rep.missing_files, 0);
    assert_eq!(rep.older_omitted, 0);
    // Bookkeeping (queue-operation, mode, file-history-snapshot) and records that are not part
    // of the conversation (attachment, turn_duration, isMeta, the compact summary).
    assert!(rep.ignored >= 7, "{}", rep.ignored);
    assert!(
        !rep.complete(),
        "an unrecognised record and a cut line are gaps"
    );
    assert!(rep.messages > 0);
}

#[test]
fn tool_results_name_their_tool_and_images_become_parts() {
    let r = parse(&fixture("session1.jsonl"));
    let mut seen_prompt_image = false;
    let mut seen_result_image = false;
    for e in &r.events {
        let (ev, _) = inner(e);
        match ev {
            AgentEvent::UserContent { parts } => {
                seen_prompt_image = matches!(
                    &parts[0],
                    ContentPart::Image {
                        mime,
                        source: MediaSource::Inline { data },
                        ..
                    } if mime == "image/png" && !data.is_empty()
                );
            }
            AgentEvent::ToolCallFinished {
                call_id,
                parts,
                result,
                ..
            } if call_id == "toolu_4" => {
                seen_result_image = matches!(&parts[0], ContentPart::Image { .. });
                // The text of the result stays in `result` (the blocks as the agent wrote them).
                assert!(result.as_ref().unwrap().to_string().contains("shot"));
            }
            _ => {}
        }
    }
    assert!(seen_prompt_image && seen_result_image);
}

#[test]
fn times_are_the_records_own_and_events_keep_their_order_in_time() {
    let r = parse(&fixture("session1.jsonl"));
    let times: Vec<Option<u64>> = r.events.iter().map(|e| inner(e).1).collect();
    assert!(times.iter().all(Option::is_some));
    // The first prompt: 2026-11-22T10:00:01.037Z.
    assert_eq!(times[0], Some(1_795_341_601_037));
    // The main chain is chronological (the subagent's lines are placed after their spawning
    // call, with their own times).
    let main: Vec<u64> = r
        .events
        .iter()
        .filter(|e| inner(e).0.unscoped().0.is_none())
        .map(|e| inner(e).1.unwrap())
        .collect();
    assert!(main.windows(2).all(|w| w[0] <= w[1]), "{main:?}");
}

#[test]
fn a_subagent_file_that_is_missing_is_counted_and_the_rest_is_shown() {
    // The main file alone: its subagent file is not next to it.
    let dir = std::env::temp_dir().join(format!("wch-{}-missing", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::copy(fixture("session1.jsonl"), dir.join("session1.jsonl")).unwrap();
    let r = parse(dir.join("session1.jsonl").to_str().unwrap());
    assert_eq!(r.report.missing_files, 1);
    let got = summary(&r.events);
    assert!(got.iter().any(|l| l == "call Agent toolu_3 sub=true"));
    assert!(!got.iter().any(|l| l.starts_with("[toolu_3")));
    assert!(got.iter().any(|l| l == "assistant: Final answer."));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_parser_asks_for_the_files_of_the_subagents_the_chain_started() {
    let mut p = ClaudeHistory::new(HistoryLimits::default());
    let path = fixture("session1.jsonl");
    let bytes = std::fs::read(&path).unwrap();
    p.begin(&path, bytes.len() as u64);
    feed(&mut p, &bytes);
    p.end();
    let wanted = p.wants();
    assert_eq!(
        wanted,
        [fixture("session1/subagents/agent-agent1abc.jsonl")]
    );
    // Asked once.
    assert!(p.wants().is_empty());
}

#[test]
fn in_file_sidechain_records_are_a_subagents_lines() {
    let r = parse(&fixture("inline_sidechain.jsonl"));
    no_live_events(&r.events);
    assert_eq!(
        summary(&r.events),
        [
            "user: Delegate this",
            "call Task toolu_9 sub=true",
            "[toolu_9:inl1] user: Inner prompt",
            "[toolu_9:inl1] assistant: Inner reply.",
            "result Task toolu_9 ok=true parts=0",
            "assistant: All done.",
        ]
    );
    // The sidechain record nothing links to is counted, not shown.
    assert_eq!(r.report.ignored, 1);
    assert_eq!(r.report.missing_files, 0);
}

fn line(v: Value) -> String {
    v.to_string()
}

fn user(uuid: &str, parent: Option<&str>, text: &str, n: u64) -> String {
    line(
        json!({"type":"user","uuid":uuid,"parentUuid":parent,"isSidechain":false,
        "timestamp":format!("2026-11-22T10:00:{:02}.000Z", n % 60),
        "message":{"role":"user","content":text}}),
    )
}

fn assistant(uuid: &str, parent: &str, text: &str, n: u64) -> String {
    line(
        json!({"type":"assistant","uuid":uuid,"parentUuid":parent,"isSidechain":false,
        "timestamp":format!("2026-11-22T10:00:{:02}.000Z", n % 60),
        "message":{"id":format!("m-{uuid}"),"role":"assistant","content":[{"type":"text","text":text}],
                   "usage":{"input_tokens":1,"output_tokens":2}}}),
    )
}

fn parse_lines(lines: &[String], limits: HistoryLimits) -> Result<HistoryParse, HistoryFailure> {
    let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
    let mut p = ClaudeHistory::new(limits);
    p.begin("/x/main.jsonl", body.len() as u64);
    feed(&mut p, body.as_bytes());
    p.end();
    assert!(p.wants().is_empty());
    Box::new(p).finish()
}

#[test]
fn the_window_leaves_out_what_is_older_and_starts_at_a_prompt() {
    // Eight turns, each a prompt and a reply, 8 messages... 16 records.
    let mut lines = Vec::new();
    let mut parent: Option<String> = None;
    for t in 0..8u64 {
        let (u, a) = (format!("u{t}"), format!("a{t}"));
        lines.push(user(&u, parent.as_deref(), &format!("question {t}"), t * 2));
        lines.push(assistant(&a, &u, &format!("answer {t}"), t * 2 + 1));
        parent = Some(a);
    }
    let size: usize = lines.iter().map(|l| l.len() + 1).sum();
    // The window covers the last 3.5 turns or so: it starts inside a reply, so the parser
    // drops down to the next prompt.
    let each = size / 16;
    let limits = HistoryLimits {
        window_bytes: (each * 7) as u64,
        ..HistoryLimits::default()
    };
    let r = parse_lines(&lines, limits).unwrap();
    let got = summary(&r.events);
    assert_eq!(got.first().unwrap(), "user: question 5", "{got:?}");
    assert_eq!(got.last().unwrap(), "assistant: answer 7");
    assert_eq!(got.len(), 6);
    assert_eq!(r.report.older_omitted, 10, "{:?}", r.report);
    assert_eq!(r.report.messages, 6);
    assert!(!r.report.complete());
    // The same transcript with a window that holds it all omits nothing.
    let all = parse_lines(&lines, HistoryLimits::default()).unwrap();
    assert_eq!(all.events.len(), 16);
    assert_eq!(all.report.older_omitted, 0);
    assert!(all.report.complete());
}

#[test]
fn long_text_and_big_media_are_bounded_and_counted() {
    let big_text = "x".repeat(10_000);
    let big_image = "A".repeat(2_000);
    let lines = vec![
        user("u1", None, "go", 0),
        line(
            json!({"type":"assistant","uuid":"a1","parentUuid":"u1","isSidechain":false,
            "message":{"id":"m1","role":"assistant","content":[
                {"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/p","note":big_text}}]}}),
        ),
        line(
            json!({"type":"user","uuid":"r1","parentUuid":"a1","isSidechain":false,
            "message":{"role":"user","content":[
                {"type":"tool_result","tool_use_id":"t1","content":[
                    {"type":"text","text":big_text},
                    {"type":"image","source":{"type":"base64","media_type":"image/png","data":big_image}}]}]}}),
        ),
    ];
    let limits = HistoryLimits {
        item_bytes: 100,
        ..HistoryLimits::default()
    };
    let r = parse_lines(&lines, limits).unwrap();
    assert_eq!(r.report.shortened, 3, "{:?}", r.report);
    for e in &r.events {
        let (ev, _) = inner(e);
        let size = serde_json::to_string(ev).unwrap().len();
        assert!(size < 1_200, "{size}: {ev:?}");
    }
    let (ev, _) = inner(&r.events[2]);
    let AgentEvent::ToolCallFinished { parts, result, .. } = ev else {
        panic!("{ev:?}")
    };
    assert!(
        matches!(&parts[0], ContentPart::Text { text } if text.contains("image omitted: 2000"))
    );
    let shown = result.as_ref().unwrap().to_string();
    assert!(shown.contains("more bytes not shown"), "{shown}");
    // A cut never splits a character.
    let multi = vec![user("u1", None, &"é".repeat(300), 0)];
    let r = parse_lines(&multi, limits).unwrap();
    let AgentEvent::UserMessage { text } = inner(&r.events[0]).0 else {
        panic!()
    };
    assert!(text.starts_with(&"é".repeat(50)) && text.contains("more bytes"));
}

#[test]
fn nothing_understood_and_nothing_at_all() {
    let junk = vec![
        "hello".to_string(),
        "{\"x\":1}".to_string(),
        "[1,2]".to_string(),
    ];
    assert_eq!(
        parse_lines(&junk, HistoryLimits::default()).err(),
        Some(HistoryFailure::FormatNotUnderstood { record: 1 })
    );
    // Only records of unknown kinds is not understood either.
    let foreign = vec![line(json!({"type":"from-another-tool"}))];
    assert!(matches!(
        parse_lines(&foreign, HistoryLimits::default()),
        Err(HistoryFailure::FormatNotUnderstood { record: 1 })
    ));
    // An empty file is a conversation without messages.
    let r = parse_lines(&[], HistoryLimits::default()).unwrap();
    assert!(r.events.is_empty() && r.report.records == 0);
    // Bookkeeping only: understood, nothing to show.
    let r = parse_lines(
        &[line(
            json!({"type":"queue-operation","operation":"enqueue"}),
        )],
        HistoryLimits::default(),
    )
    .unwrap();
    assert!(r.events.is_empty());
    assert_eq!(r.report.ignored, 1);
}

#[test]
fn the_same_lines_give_the_same_history() {
    let a = parse(&fixture("session1.jsonl"));
    let b = parse(&fixture("session1.jsonl"));
    assert_eq!(a, b);
}

#[test]
fn a_cut_last_line_is_not_a_malformed_one() {
    let lines = [user("u1", None, "one", 0)];
    let body = format!(
        "{}\n{{\"type\":\"assistant\",\"uuid\":\"a1\",\"mess",
        lines[0]
    );
    let mut p = ClaudeHistory::new(HistoryLimits::default());
    p.begin("/x/m.jsonl", body.len() as u64);
    feed(&mut p, body.as_bytes());
    let r = Box::new(p).finish().unwrap();
    assert!(r.report.truncated_tail);
    assert_eq!(r.report.unreadable, 0);
    assert_eq!(r.events.len(), 1);
}

#[test]
fn garbage_never_panics() {
    let nasty: Vec<String> = vec![
        "".into(),
        "null".into(),
        "\"just a string\"".into(),
        "{".into(),
        "{\"type\":5}".into(),
        "{\"type\":\"user\"}".into(),
        "{\"type\":\"user\",\"uuid\":\"x\",\"message\":5}".into(),
        "{\"type\":\"user\",\"uuid\":\"x\",\"parentUuid\":\"x\",\"message\":{\"content\":[1,null,{\"type\":\"tool_result\"}]}}".into(),
        "{\"type\":\"assistant\",\"uuid\":\"y\",\"parentUuid\":\"z\",\"message\":{\"content\":\"plain\"}}".into(),
        "{\"type\":\"assistant\",\"uuid\":\"z\",\"parentUuid\":\"y\",\"message\":{\"content\":[{\"type\":\"tool_use\"}]}}".into(),
        "{\"type\":\"system\",\"uuid\":\"s\",\"subtype\":\"compact_boundary\",\"compactMetadata\":7}".into(),
        "{\"type\":\"user\",\"uuid\":\"q\",\"isSidechain\":true,\"agentId\":\"../../etc\",\"message\":{\"content\":\"x\"}}".into(),
        format!("{{\"type\":\"user\",\"uuid\":\"{}\",\"timestamp\":\"9999-99-99T99:99:99Z\",\"message\":{{\"content\":\"id\"}}}}", "i".repeat(100_000)),
        format!("{}1{}", "[".repeat(1000), "]".repeat(1000)),
        "{\"type\":\"user\",\"uuid\":\"n\",\"parentUuid\":\"n\",\"message\":{\"content\":\"self parent\"}}".into(),
        "\u{0}\u{1}\u{2}".into(),
    ];
    // Each alone and all together; cycles (y <-> z, n -> n) end.
    for l in &nasty {
        let _ = parse_lines(std::slice::from_ref(l), HistoryLimits::default());
    }
    let r = parse_lines(&nasty, HistoryLimits::default());
    assert!(r.is_ok());
}

#[test]
fn the_storage_path_is_derived_like_the_listing_and_ids_cannot_escape() {
    let desc = crate::descriptor();
    let ctx = DiscoveryContext {
        home: Some("/home/u/".into()),
        env: vec![],
        project: Some("/work/my proj".into()),
        binary: None,
    };
    let id = "3b241101-e2bb-4255-8caf-4136c566a962";
    assert_eq!(
        desc.history_storage(&ctx, id).as_deref(),
        Some("/home/u/.claude/projects/-work-my-proj/3b241101-e2bb-4255-8caf-4136c566a962.jsonl")
    );
    let configured = DiscoveryContext {
        env: vec![("CLAUDE_CONFIG_DIR".into(), "/cfg/".into())],
        ..ctx.clone()
    };
    assert_eq!(
        desc.history_storage(&configured, id).as_deref(),
        Some("/cfg/projects/-work-my-proj/3b241101-e2bb-4255-8caf-4136c566a962.jsonl")
    );
    for bad in [
        "../../etc/passwd",
        "x",
        "3b241101-e2bb-4255-8caf-4136c566a96/",
        "",
    ] {
        assert!(desc.history_storage(&ctx, bad).is_none(), "{bad}");
    }
    let no_project = DiscoveryContext {
        project: None,
        ..ctx
    };
    assert!(desc.history_storage(&no_project, id).is_none());
    assert!(desc
        .history_parser("/x.jsonl", &HistoryLimits::default())
        .is_some());
}

/// A generated transcript of about 20 MB: the parser takes it line by line and gives the whole
/// conversation, in a memory of about the file's size.
#[test]
fn a_twenty_megabyte_transcript_is_taken_whole() {
    let mut p = ClaudeHistory::new(HistoryLimits::default());
    let filler = "lorem ipsum dolor sit amet ".repeat(150);
    let mut total = 0usize;
    let mut prev: Option<String> = None;
    let mut turns = 0u64;
    let mut batch = Vec::new();
    while total < 20 * 1024 * 1024 {
        let (u, a) = (format!("u{turns}"), format!("a{turns}"));
        batch.push(user(
            &u,
            prev.as_deref(),
            &format!("q{turns} {filler}"),
            turns,
        ));
        batch.push(assistant(&a, &u, &format!("r{turns} {filler}"), turns));
        total += batch[batch.len() - 2].len() + batch[batch.len() - 1].len() + 2;
        prev = Some(a);
        turns += 1;
    }
    p.begin("/x/big.jsonl", total as u64);
    for l in &batch {
        p.push(l, true);
    }
    p.end();
    let r = Box::new(p).finish().unwrap();
    assert_eq!(r.events.len() as u64, turns * 2);
    assert_eq!(r.report.messages, turns * 2);
    assert!(r.report.complete());
}

/// The import window is one budget for the main file and the subagents' files together: a
/// conversation with big subagents is bounded as a whole, and what the window leaves out of any
/// of them is counted, not dropped silently.
#[test]
fn the_window_is_one_budget_for_the_main_file_and_the_subagents() {
    let path = fixture("session1.jsonl");
    let all = parse(&path);
    let with_sub = |p: &HistoryParse| {
        p.events
            .iter()
            .filter(|e| inner(e).0.unscoped().0.is_some())
            .count()
    };
    assert!(with_sub(&all) > 0, "the fixture has a subagent in it");
    assert_eq!(all.report.older_omitted, 0);
    // A window as big as the main file: the main file takes all of it, and the subagent's own
    // file gets nothing.
    let main_len = std::fs::metadata(&path).unwrap().len();
    let tight = parse_with(
        &path,
        HistoryLimits {
            window_bytes: main_len,
            ..HistoryLimits::default()
        },
    );
    assert_eq!(with_sub(&tight), 0);
    assert!(
        tight.report.older_omitted > 0,
        "the subagent's messages that did not fit are counted: {:?}",
        tight.report
    );
    // The part of the conversation in the main file is whole.
    assert_eq!(
        summary(&tight.events),
        summary(
            &all.events
                .iter()
                .filter(|e| inner(e).0.unscoped().0.is_none())
                .cloned()
                .collect::<Vec<_>>()
        )
    );
}

/// An edit made in a resumed transcript: the diff is on the result the user record carries, and
/// a failed one has none.
#[test]
fn edits_in_a_transcript_carry_their_diffs_on_the_result() {
    // Each record is the child of the one before it: the chain is the conversation.
    let call = |id: &str, parent: Option<&str>, name: &str, input: Value| {
        line(
            json!({"type":"assistant","uuid":format!("a-{id}"),"parentUuid":parent,"isSidechain":false,
            "timestamp":"2026-11-22T10:00:01.000Z",
            "message":{"id":format!("m-{id}"),"role":"assistant","content":[
                {"type":"tool_use","id":id,"name":name,"input":input}]}}),
        )
    };
    let result = |id: &str, parent: &str, is_error: bool| {
        line(
            json!({"type":"user","uuid":format!("u-{id}"),"parentUuid":parent,"isSidechain":false,
            "timestamp":"2026-11-22T10:00:02.000Z",
            "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":id,
                "content":"done","is_error":is_error}]}}),
        )
    };
    let lines = vec![
        call(
            "e1",
            None,
            "Edit",
            json!({"file_path":"/w/a.rs","old_string":"x","new_string":"y"}),
        ),
        result("e1", "a-e1", false),
        call(
            "e2",
            Some("u-e1"),
            "Edit",
            json!({"file_path":"/w/a.rs","old_string":"p","new_string":"q"}),
        ),
        result("e2", "a-e2", true),
        call(
            "w1",
            Some("u-e2"),
            "Write",
            json!({"file_path":"/w/n.txt","content":"new\n"}),
        ),
        result("w1", "a-w1", false),
    ];
    let r = parse_lines(&lines, HistoryLimits::default()).unwrap();
    let parts_of = |want: &str| -> Vec<ContentPart> {
        r.events
            .iter()
            .find_map(|e| match inner(e).0 {
                AgentEvent::ToolCallFinished { call_id, parts, .. } if call_id == want => {
                    Some(parts.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("no result for {want}"))
    };
    assert_eq!(
        parts_of("e1"),
        vec![ContentPart::Diff {
            path: "/w/a.rs".into(),
            old_text: Some("x".into()),
            new_text: "y".into(),
            hunk: None,
        }]
    );
    assert!(parts_of("e2").is_empty());
    assert_eq!(
        parts_of("w1"),
        vec![ContentPart::Diff {
            path: "/w/n.txt".into(),
            old_text: None,
            new_text: "new\n".into(),
            hunk: None,
        }]
    );
}
