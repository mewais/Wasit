//! Claude's earlier history: a parser of the conversation transcript the CLI keeps in its own
//! storage, `<config>/projects/<cwd with every non-alphanumeric character as "-">/<id>.jsonl`.
//!
//! Format facts (claude 2.1.293, read from real transcripts; this module only ever sees lines):
//!
//! * one JSON object per line. Records of the conversation are `user`, `assistant`, `system` and
//!   `attachment`; each carries `uuid`, `parentUuid`, `isSidechain`, `timestamp` (RFC 3339, UTC)
//!   and, for the first two, an Anthropic-shaped `message`. Everything else (`queue-operation`,
//!   `file-history-snapshot`, `mode`, `last-prompt`, `ai-title`, `cost-state`, …) is bookkeeping.
//! * the conversation is a tree: a rewind or an edit gives a message a second child, and a
//!   transcript has several leaves. The conversation is the chain from the **last** record in
//!   file order that has a `uuid` back to the root along `parentUuid`; file order is not chain
//!   order. A compaction starts a new root (`system/compact_boundary`, `parentUuid` null) whose
//!   `logicalParentUuid` continues the chain.
//! * an `assistant` record holds ONE content block (text, thinking, tool_use) of a message; a
//!   `user` record is a typed prompt (string or text blocks), images, or `tool_result` blocks
//!   (with a sibling `toolUseResult` that duplicates the result, and for a subagent call carries
//!   its `agentId`).
//! * a subagent's own conversation is a separate file,
//!   `<main file without .jsonl>/subagents/agent-<agentId>.jsonl` (records `isSidechain: true`,
//!   with `agentId`); older versions wrote such records into the main file.
//!
//! The parser is pure: the runtime reads the files off the async path and pushes their lines
//! ([`HistoryParser`]). It produces [`AgentEvent::History`] events only: no usage, no turn, task or
//! gate event is ever made from history, so nothing in it can change a session's live state.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde_json::{json, Value};
use wasit_core::wire::{opt_s, s};
use wasit_core::{
    AgentEvent, CompactionInfo, ContentPart, FileProblem, HistoryFailure, HistoryLimits,
    HistoryParse, HistoryParser, HistoryReport, Scope,
};

use crate::edit::edit_diffs;
use crate::protocol::claude_part;

/// Record types that are bookkeeping, not conversation.
const BOOKKEEPING: &[&str] = &[
    "queue-operation",
    "file-history-snapshot",
    "file-history-delta",
    "mode",
    "permission-mode",
    "last-prompt",
    "ai-title",
    "custom-title",
    "agent-name",
    "atis-latch",
    "cost-state",
    "summary",
    "progress",
];

/// How deep subagents of subagents are followed.
const MAX_NEST: usize = 8;
/// Media above this many times the item bound is left out.
const MEDIA_FACTOR: usize = 16;
/// Most distinct subagent files asked for.
const MAX_AGENTS: usize = 4096;

/// A parser of claude's transcript files. See the module docs.
pub struct ClaudeHistory {
    limits: HistoryLimits,
    report: HistoryReport,
    /// The main file without `.jsonl`: the subagent directory hangs under it.
    base: Option<String>,
    /// The agent whose file is being read (`None`: the main file).
    reading_agent: Option<String>,
    /// Byte offset of the next line in the file being read, and where its window starts.
    offset: u64,
    window_start: u64,
    /// What is left of the import window ([`HistoryLimits::window_bytes`]) for the files still
    /// to come: the window is one budget for the main file and the subagents' files together,
    /// so a conversation with many big subagents is bounded as a whole.
    budget_left: u64,
    /// 1-based number of the line being read, over all files, and of the first non-blank one.
    line_no: u64,
    first_line: Option<u64>,
    understood: bool,
    main: Stream,
    agents: BTreeMap<String, Stream>,
    requested: HashSet<String>,
    started: bool,
}

/// What a record is to the conversation.
struct Rec {
    /// The record the chain continues from: `parentUuid`, else `logicalParentUuid`.
    up: Option<String>,
    /// A user or assistant record of the conversation (counted as a message).
    message: bool,
    /// A typed user prompt (a turn starts here).
    prompt: bool,
    /// Read in full (inside the import window): `events` is meaningful.
    in_window: bool,
    events: Vec<AgentEvent>,
    /// `(agent id, spawning tool call id)` this record's tool result names.
    links: Vec<(String, String)>,
}

#[derive(Default)]
struct Stream {
    recs: Vec<Rec>,
    index: HashMap<String, usize>,
    leaf: Option<usize>,
    /// The chain, kept until a record is added.
    chain: Option<Vec<usize>>,
}

impl Stream {
    fn add(&mut self, uuid: Option<String>, rec: Rec) {
        let i = self.recs.len();
        self.recs.push(rec);
        if let Some(u) = uuid {
            self.index.insert(u, i);
            self.leaf = Some(i);
        }
        self.chain = None;
    }

    /// The chain from the root to the leaf, oldest first. Iterative, guarded against cycles.
    fn chain(&mut self) -> &[usize] {
        if self.chain.is_none() {
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            let mut cur = self.leaf;
            while let Some(i) = cur {
                if !seen.insert(i) {
                    break;
                }
                out.push(i);
                cur = self
                    .recs
                    .get(i)
                    .and_then(|r| r.up.as_deref())
                    .and_then(|u| self.index.get(u).copied());
            }
            out.reverse();
            self.chain = Some(out);
        }
        self.chain.as_deref().unwrap_or(&[])
    }

    /// The part of the chain inside the import window, cut to start at a typed prompt when
    /// something older was left out: `(chain, first included position)`.
    fn included(&mut self) -> (Vec<usize>, usize) {
        let chain = self.chain().to_vec();
        let mut start = chain.len();
        while start > 0 && self.recs[chain[start - 1]].in_window {
            start -= 1;
        }
        if start > 0 {
            if let Some(p) = (start..chain.len()).find(|&i| self.recs[chain[i]].prompt) {
                start = p;
            }
        }
        (chain, start)
    }
}

impl ClaudeHistory {
    pub fn new(limits: HistoryLimits) -> Self {
        ClaudeHistory {
            limits,
            report: HistoryReport::default(),
            base: None,
            reading_agent: None,
            offset: 0,
            window_start: 0,
            budget_left: limits.window_bytes,
            line_no: 0,
            first_line: None,
            understood: false,
            main: Stream::default(),
            agents: BTreeMap::new(),
            requested: HashSet::new(),
            started: false,
        }
    }

    fn media_limit(&self) -> usize {
        self.limits.item_bytes.saturating_mul(MEDIA_FACTOR)
    }

    /// Route and convert one record.
    fn record(&mut self, v: &Value, shape: Shape, in_window: bool) {
        let uuid = opt_s(v, "uuid");
        let sidechain = v.get("isSidechain").and_then(Value::as_bool) == Some(true);
        let agent = match &self.reading_agent {
            Some(a) => Some(a.clone()),
            None if sidechain => match opt_s(v, "agentId").filter(|a| valid_id(a)) {
                Some(a) => Some(a),
                None => {
                    // A subagent record with nothing to attribute it to.
                    self.report.ignored += 1;
                    return;
                }
            },
            None => None,
        };
        if uuid.is_none() {
            self.report
                .unrecognised_kind(&format!("{} without uuid", shape.name()));
            return;
        }
        let at = opt_s(v, "timestamp").and_then(|t| parse_time_ms(&t));
        let conv = convert(
            v,
            shape,
            at,
            in_window,
            self.limits.item_bytes,
            self.media_limit(),
        );
        self.report.shortened += conv.shortened;
        if conv.ignored {
            self.report.ignored += 1;
        }
        let up = opt_s(v, "parentUuid").or_else(|| opt_s(v, "logicalParentUuid"));
        let rec = Rec {
            up,
            message: conv.message,
            prompt: conv.prompt,
            in_window,
            events: conv.events,
            links: conv.links,
        };
        match agent {
            Some(a) => self.agents.entry(a).or_default().add(uuid, rec),
            None => self.main.add(uuid, rec),
        }
    }

    fn stream_mut(&mut self, key: Option<&str>) -> Option<&mut Stream> {
        match key {
            None => Some(&mut self.main),
            Some(k) => self.agents.get_mut(k),
        }
    }

    /// The events of stream `key` (`None`: the main one), oldest first, each an
    /// `AgentEvent::History`; subagents the chain starts are inserted after their spawning call
    /// as `Scoped` events.
    fn assemble(
        &mut self,
        key: Option<&str>,
        depth: usize,
        active: &mut Vec<String>,
    ) -> Vec<AgentEvent> {
        let Some(st) = self.stream_mut(key) else {
            return Vec::new();
        };
        let (chain, start) = st.included();
        let count =
            |st: &Stream, r: &[usize]| r.iter().filter(|&&i| st.recs[i].message).count() as u64;
        let (omitted, shown) = (count(st, &chain[..start]), count(st, &chain[start..]));
        // Tool calls that start a subagent: call id -> agent id.
        let mut spawned: HashMap<String, String> = HashMap::new();
        for &i in &chain[start..] {
            for (agent, call) in &st.recs[i].links {
                spawned.entry(call.clone()).or_insert_with(|| agent.clone());
            }
        }
        self.report.older_omitted += omitted;
        self.report.messages += shown;
        let mut out = Vec::new();
        let mut names: HashMap<String, String> = HashMap::new();
        // The file edits of the calls seen so far, attached to their results.
        let mut edits: HashMap<String, Vec<ContentPart>> = HashMap::new();
        for &i in &chain[start..] {
            let Some(st) = self.stream_mut(key) else {
                return out;
            };
            let events = std::mem::take(&mut st.recs[i].events);
            for mut ev in events {
                let mut call = None;
                if let AgentEvent::History { event, .. } = &mut ev {
                    match event.as_mut() {
                        AgentEvent::ToolCallStarted {
                            name,
                            call_id,
                            input,
                            ..
                        } => {
                            names.insert(call_id.clone(), name.clone());
                            let diffs = edit_diffs(name.as_str(), input);
                            if !diffs.is_empty() {
                                edits.insert(call_id.clone(), diffs);
                            }
                            call = Some(call_id.clone());
                        }
                        AgentEvent::ToolCallFinished {
                            name,
                            call_id,
                            ok,
                            parts,
                            ..
                        } => {
                            if name.is_empty() {
                                if let Some(n) = names.get(call_id) {
                                    name.clone_from(n);
                                }
                            }
                            // A failed edit changed nothing: its diff is not shown.
                            let diffs = edits.remove(call_id).unwrap_or_default();
                            if *ok {
                                parts.extend(diffs);
                            }
                        }
                        _ => {}
                    }
                }
                out.push(ev);
                let Some(call) = call else { continue };
                let Some(agent) = spawned.get(&call).cloned() else {
                    continue;
                };
                if depth >= MAX_NEST || active.contains(&agent) || !self.agents.contains_key(&agent)
                {
                    continue;
                }
                active.push(agent.clone());
                let inner = self.assemble(Some(&agent), depth + 1, active);
                active.pop();
                let scope = Scope {
                    call_id: call,
                    agent_id: Some(agent),
                };
                for e in inner {
                    out.push(match e {
                        AgentEvent::History { at_ms, event } => match *event {
                            // Already attributed to a deeper subagent: flat, its own scope.
                            scoped @ AgentEvent::Scoped { .. } => scoped.earlier(at_ms),
                            plain => plain.scoped(Some(scope.clone())).earlier(at_ms),
                        },
                        other => other,
                    });
                }
            }
        }
        out
    }
}

impl HistoryParser for ClaudeHistory {
    fn begin(&mut self, file: &str, len: u64) {
        self.offset = 0;
        self.window_start = len.saturating_sub(self.budget_left);
        if !self.started {
            self.started = true;
            self.base = Some(file.strip_suffix(".jsonl").unwrap_or(file).to_string());
            self.reading_agent = None;
        } else {
            let name = file.rsplit(['/', '\\']).next().unwrap_or(file);
            self.reading_agent = name
                .strip_prefix("agent-")
                .and_then(|n| n.strip_suffix(".jsonl"))
                .filter(|a| valid_id(a))
                .map(str::to_string);
        }
    }

    fn push(&mut self, line: &str, terminated: bool) {
        let start = self.offset;
        self.offset += line.len() as u64 + 1;
        if start >= self.window_start {
            // Every line of the window counts against the one budget, records or not.
            self.budget_left = self.budget_left.saturating_sub(line.len() as u64 + 1);
        }
        self.line_no += 1;
        if line.trim().is_empty() {
            return;
        }
        self.first_line.get_or_insert(self.line_no);
        self.report.records += 1;
        let v: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) if !terminated => {
                self.report.truncated_tail = true;
                return;
            }
            Err(_) => {
                self.report.unreadable += 1;
                return;
            }
        };
        let Some(kind) = v.get("type").and_then(Value::as_str) else {
            self.report.unreadable += 1;
            return;
        };
        let shape = match kind {
            "user" => Shape::User,
            "assistant" => Shape::Assistant,
            "system" => Shape::System,
            "attachment" => Shape::Attachment,
            k if BOOKKEEPING.contains(&k) => {
                self.understood = true;
                self.report.ignored += 1;
                return;
            }
            other => {
                self.report.unrecognised_kind(other);
                return;
            }
        };
        self.understood = true;
        let in_window = start >= self.window_start;
        self.record(&v, shape, in_window);
    }

    fn end(&mut self) {}

    fn wants(&mut self) -> Vec<String> {
        let Some(base) = self.base.clone() else {
            return Vec::new();
        };
        let mut ids: BTreeSet<String> = BTreeSet::new();
        let mut collect = |st: &mut Stream| {
            let (chain, start) = st.included();
            for &i in &chain[start..] {
                for (agent, _) in &st.recs[i].links {
                    ids.insert(agent.clone());
                }
            }
        };
        collect(&mut self.main);
        for st in self.agents.values_mut() {
            collect(st);
        }
        let mut paths = Vec::new();
        for id in ids {
            if !valid_id(&id)
                || self.requested.contains(&id)
                || self.agents.contains_key(&id)
                || self.requested.len() >= MAX_AGENTS
            {
                continue;
            }
            self.requested.insert(id.clone());
            paths.push(format!("{base}/subagents/agent-{id}.jsonl"));
        }
        paths
    }

    fn unavailable(&mut self, _file: &str, why: FileProblem) {
        match why {
            FileProblem::Missing | FileProblem::Unreadable => self.report.missing_files += 1,
            FileProblem::LineTooLong => self.report.unreadable += 1,
        }
    }

    fn finish(mut self: Box<Self>) -> Result<HistoryParse, HistoryFailure> {
        if self.report.records > 0 && !self.understood {
            return Err(HistoryFailure::FormatNotUnderstood {
                record: self.first_line.unwrap_or(1),
            });
        }
        let events = self.assemble(None, 0, &mut Vec::new());
        let mut report = std::mem::take(&mut self.report);
        report.events = events.len() as u64;
        Ok(HistoryParse { events, report })
    }
}

#[derive(Clone, Copy)]
enum Shape {
    User,
    Assistant,
    System,
    Attachment,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::User => "user",
            Shape::Assistant => "assistant",
            Shape::System => "system",
            Shape::Attachment => "attachment",
        }
    }
}

/// A subagent id that is safe to put in a file name.
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

struct Conv {
    events: Vec<AgentEvent>,
    message: bool,
    prompt: bool,
    links: Vec<(String, String)>,
    /// A record that is no part of the conversation.
    ignored: bool,
    shortened: u64,
}

fn convert(
    v: &Value,
    shape: Shape,
    at: Option<u64>,
    build: bool,
    item: usize,
    media: usize,
) -> Conv {
    let mut c = Conv {
        events: Vec::new(),
        message: false,
        prompt: false,
        links: Vec::new(),
        ignored: false,
        shortened: 0,
    };
    let push = |c: &mut Conv, e: AgentEvent| {
        if build {
            c.events.push(e.earlier(at));
        }
    };
    let flag = |k: &str| v.get(k).and_then(Value::as_bool) == Some(true);
    match shape {
        Shape::Attachment => c.ignored = true,
        Shape::System => match v.get("subtype").and_then(Value::as_str) {
            Some("compact_boundary") => {
                let m = v.get("compactMetadata").cloned().unwrap_or(Value::Null);
                let n = |k: &str| m.get(k).and_then(Value::as_u64).unwrap_or(0);
                push(
                    &mut c,
                    AgentEvent::Compaction {
                        info: CompactionInfo {
                            trigger: Some(s(&m, "trigger").to_string()),
                            pre_tokens: Some(n("preTokens")),
                            post_tokens: Some(n("postTokens")),
                            cumulative_dropped_tokens: Some(n("cumulativeDroppedTokens")),
                            duration_ms: Some(n("durationMs")),
                        },
                    },
                );
            }
            _ => c.ignored = true,
        },
        Shape::User => {
            if flag("isMeta") || flag("isCompactSummary") || flag("isVisibleInTranscriptOnly") {
                c.ignored = true;
                return c;
            }
            c.message = true;
            let content = v.get("message").and_then(|m| m.get("content"));
            match content {
                Some(Value::String(t)) => {
                    if !t.is_empty() {
                        c.prompt = true;
                        let mut t = t.clone();
                        c.shortened += cut(&mut t, item);
                        push(&mut c, AgentEvent::UserMessage { text: t });
                    }
                }
                Some(Value::Array(blocks)) => {
                    user_blocks(&mut c, v, blocks, build, at, item, media)
                }
                _ => {}
            }
        }
        Shape::Assistant => {
            c.message = true;
            let msg = v.get("message");
            let message_id = msg.and_then(|m| opt_s(m, "id"));
            let blocks: Vec<Value> = match msg.and_then(|m| m.get("content")) {
                Some(Value::Array(b)) => b.clone(),
                Some(Value::String(t)) => vec![json!({"type": "text", "text": t})],
                _ => Vec::new(),
            };
            for b in &blocks {
                let ev = match s(b, "type") {
                    "text" => {
                        let mut text = s(b, "text").to_string();
                        if text.is_empty() {
                            continue;
                        }
                        c.shortened += cut(&mut text, item);
                        AgentEvent::AssistantDelta {
                            text,
                            message_id: message_id.clone(),
                        }
                    }
                    "thinking" => {
                        let mut text = s(b, "thinking").to_string();
                        if text.is_empty() {
                            continue;
                        }
                        c.shortened += cut(&mut text, item);
                        AgentEvent::ThinkingDelta {
                            text,
                            message_id: message_id.clone(),
                        }
                    }
                    "tool_use" => {
                        let name = s(b, "name").to_string();
                        let mut input = b.get("input").cloned().unwrap_or(Value::Null);
                        c.shortened += bound_value(&mut input, item, 0);
                        let is_subagent = name == "Task" || name == "Agent";
                        let subagent_model = if is_subagent {
                            opt_s(&input, "model")
                        } else {
                            None
                        };
                        AgentEvent::ToolCallStarted {
                            name,
                            call_id: s(b, "id").to_string(),
                            input,
                            is_subagent,
                            subagent_model,
                        }
                    }
                    _ => continue,
                };
                push(&mut c, ev);
            }
        }
    }
    c
}

fn user_blocks(
    c: &mut Conv,
    v: &Value,
    blocks: &[Value],
    build: bool,
    at: Option<u64>,
    item: usize,
    media: usize,
) {
    let agent_id = v
        .get("toolUseResult")
        .filter(|t| t.is_object())
        .and_then(|t| opt_s(t, "agentId"));
    let mut texts: Vec<String> = Vec::new();
    let mut parts: Vec<ContentPart> = Vec::new();
    let mut events: Vec<AgentEvent> = Vec::new();
    for b in blocks {
        match s(b, "type") {
            "text" => {
                let t = s(b, "text");
                if !t.is_empty() {
                    texts.push(t.to_string());
                }
            }
            "tool_result" => {
                let call_id = s(b, "tool_use_id").to_string();
                if let Some(a) = &agent_id {
                    c.links.push((a.clone(), call_id.clone()));
                }
                if !build {
                    continue;
                }
                let mut result = b.get("content").cloned();
                let mut result_parts = Vec::new();
                match result.as_mut() {
                    Some(Value::String(t)) => c.shortened += cut(t, item),
                    Some(Value::Array(items)) => {
                        for blk in items.iter_mut() {
                            match s(blk, "type") {
                                "image" | "document" => {
                                    if let Some(p) = media_part(blk, media, &mut c.shortened) {
                                        result_parts.push(p);
                                    }
                                }
                                _ => {
                                    c.shortened += bound_value(blk, item, 0);
                                }
                            }
                        }
                    }
                    Some(other) => c.shortened += bound_value(other, item, 0),
                    None => {}
                }
                events.push(AgentEvent::ToolCallFinished {
                    call_id,
                    name: String::new(),
                    ok: !b.get("is_error").and_then(Value::as_bool).unwrap_or(false),
                    result,
                    parts: result_parts,
                });
            }
            "image" | "document" if build => {
                let mut blk = b.clone();
                if let Some(p) = media_part(&mut blk, media, &mut c.shortened) {
                    parts.push(p);
                }
            }
            _ => {}
        }
    }
    if texts.iter().any(|t| !t.is_empty()) {
        c.prompt = true;
    }
    if !build {
        return;
    }
    if !texts.is_empty() {
        let mut text = texts.join("\n");
        c.shortened += cut(&mut text, item);
        c.events.push(AgentEvent::UserMessage { text }.earlier(at));
    }
    if !parts.is_empty() {
        c.events.push(AgentEvent::UserContent { parts }.earlier(at));
    }
    c.events.extend(events.into_iter().map(|e| e.earlier(at)));
}

/// The part an image / document block makes; its inline data is removed from the block when it
/// is too large (then the part is a note saying so).
fn media_part(block: &mut Value, media: usize, shortened: &mut u64) -> Option<ContentPart> {
    let size = block
        .get("source")
        .filter(|src| s(src, "type") == "base64")
        .map(|src| s(src, "data").len())
        .unwrap_or(0);
    if size > media {
        *shortened += 1;
        if let Some(data) = block.get_mut("source").and_then(|src| src.get_mut("data")) {
            *data = Value::String(String::new());
        }
        return Some(ContentPart::Text {
            text: format!("[image omitted: {size} bytes]"),
        });
    }
    claude_part(block)
}

/// Cut `t` to about `max` bytes at a character boundary, with a note of what was left out.
/// Returns 1 when it cut.
fn cut(t: &mut String, max: usize) -> u64 {
    if t.len() <= max {
        return 0;
    }
    let mut end = max;
    while end > 0 && !t.is_char_boundary(end) {
        end -= 1;
    }
    let left = t.len() - end;
    t.truncate(end);
    t.push_str(&format!("\n… [{left} more bytes not shown]"));
    1
}

/// [`cut`] every string leaf of `v`; the count of cuts.
fn bound_value(v: &mut Value, max: usize, depth: usize) -> u64 {
    if depth > 64 {
        return 0;
    }
    match v {
        Value::String(t) => cut(t, max),
        Value::Array(a) => a.iter_mut().map(|x| bound_value(x, max, depth + 1)).sum(),
        Value::Object(o) => o.values_mut().map(|x| bound_value(x, max, depth + 1)).sum(),
        _ => 0,
    }
}

/// `2026-11-22T16:42:09.123Z` (or with a numeric offset) as milliseconds since the Unix epoch.
pub(crate) fn parse_time_ms(t: &str) -> Option<u64> {
    let b = t.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !(b[10] == b'T' || b[10] == b' ') {
        return None;
    }
    let num = |a: usize, z: usize| -> Option<i64> { t.get(a..z)?.parse().ok() };
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, sec) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let mut i = 19;
    let mut ms = 0i64;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let mut digits = 0;
        while let Some(c) = b.get(i).filter(|c| c.is_ascii_digit()) {
            if digits < 3 {
                ms = ms * 10 + i64::from(c - b'0');
                digits += 1;
            }
            i += 1;
        }
        while digits < 3 {
            ms *= 10;
            digits += 1;
        }
    }
    let offset_min = match b.get(i) {
        Some(b'Z') | Some(b'z') if i + 1 == b.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            let (oh, om) = (num(i + 1, i + 3)?, num(i + 4, i + 6)?);
            let m = oh * 60 + om;
            if *sign == b'+' {
                m
            } else {
                -m
            }
        }
        _ => return None,
    };
    // Days since 1970-01-01 (proleptic Gregorian, Howard Hinnant's algorithm).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec - offset_min * 60;
    u64::try_from(secs)
        .ok()?
        .checked_mul(1000)?
        .checked_add(ms as u64)
}

#[cfg(test)]
mod time_tests {
    use super::parse_time_ms;

    #[test]
    fn rfc3339_times() {
        assert_eq!(parse_time_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_time_ms("2026-11-22T16:42:09.123Z"),
            Some(1_795_365_729_123)
        );
        assert_eq!(
            parse_time_ms("2026-11-22T16:42:09Z"),
            Some(1_795_365_729_000)
        );
        assert_eq!(
            parse_time_ms("2026-11-22T16:42:09.5Z"),
            Some(1_795_365_729_500)
        );
        assert_eq!(
            parse_time_ms("2026-11-22T11:42:09.123-05:00"),
            Some(1_795_365_729_123)
        );
        assert_eq!(
            parse_time_ms("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000)
        );
        assert_eq!(parse_time_ms("nonsense"), None);
        assert_eq!(parse_time_ms("2026-13-22T16:42:09Z"), None);
        assert_eq!(parse_time_ms("1969-12-31T23:59:59Z"), None);
    }
}
