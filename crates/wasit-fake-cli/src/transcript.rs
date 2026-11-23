//! Claude's transcript file, written the way the real CLI writes it: the ONE writer of that shape,
//! used by the claude persona (what a run leaves behind) and by tests (an "agent-only"
//! conversation: one that exists in the agent's storage and in no Marsad journal).
//!
//! # The shape (claude 2.1.293)
//!
//! Taken from real files (`~/.claude/projects/<cwd with every non-alphanumeric character as
//! "-">/<session id>.jsonl`, a one-prompt stream-json run and larger interactive ones; only their
//! structure was used, no content). One JSON object per line, `type` tells what it is:
//!
//! * message records, chained through `parentUuid` (`null` for the first): `user`, `assistant`,
//!   `attachment`, `system`. Every one carries `uuid`, `parentUuid`, `isSidechain`, `timestamp`
//!   (RFC 3339, milliseconds, `Z`), `userType`, `entrypoint`, `cwd`, `sessionId`, `version`,
//!   `gitBranch`.
//!   * `user`: `message {role:"user", content}` (an array of blocks for a stream-json prompt, a
//!     string for a typed one), `promptId`, `permissionMode`, `promptSource`, `turnOrigin`,
//!     `turnPosition {promptIndex, turnIndex}`. A tool result is a `user` record whose content is
//!     `[{type:"tool_result", tool_use_id, content, is_error?}]` with `toolUseResult` (the tool's
//!     own structured result; for a subagent `{agentId, status, …}`) and `sourceToolAssistantUUID`.
//!   * `assistant`: ONE RECORD PER CONTENT BLOCK of an API message (`message {id, model, role,
//!     type:"message", content:[block], stop_reason, usage, …}`, the same `message.id` on all
//!     blocks of a message, `apiBlockIndex`, `requestId`, `effort`, `perTurnEffort`). Blocks:
//!     `text`, `thinking` (empty text plus a `signature`, as the real files hold it), `tool_use`.
//!   * `attachment`: `attachment {type, …}` (`environment`, `date`, `model`, …), optionally
//!     `rendered` / `renderedRole`.
//!   * `system`: `subtype` `turn_duration`, `local_command`, `away_summary`, or
//!     `compact_boundary` (`parentUuid` null, `logicalParentUuid` the leaf before the
//!     compaction, `compactMetadata`), the latter followed by the `user` record
//!     `isCompactSummary` / `isVisibleInTranscriptOnly`.
//! * bookkeeping records with no `uuid` (not part of the chain): `queue-operation` (`operation`
//!   enqueue / dequeue, `timestamp`), `last-prompt` (`lastPrompt`, `leafUuid`), `cost-state`,
//!   `mode`, `permission-mode`, `atis-latch`, `ai-title`, and for a named conversation
//!   `custom-title` / `agent-name`; each carries `sessionId`.
//! * a resumed run APPENDS to the same file; its first record chains to the file's last leaf. An
//!   edited or rewound message is a second child of an earlier node, so file order is not chain
//!   order.
//! * a subagent's conversation is a SEPARATE file,
//!   `<main path minus .jsonl>/subagents/agent-<agentId>.jsonl` (records `isSidechain: true`,
//!   `agentId`, an own chain whose first `user` record, `parentUuid` null, holds the prompt), with
//!   `agent-<agentId>.meta.json` `{agentType, description, toolUseId, spawnDepth, requestShape,
//!   requestNonInteractive, model}`; the main file links to it by `toolUseResult.agentId`.
//!
//! Not written: `file-history-snapshot` / `file-history-delta`, `slug`, `session_id`,
//! `queueTranscriptOnly`, `classifier*`, `permissionDecision`, `system/turn_duration` for
//! non-interactive runs (the real files of stream-json runs have none), the large
//! `prompt_snapshot` attachments, thinking text (the real files hold it empty), tool-result
//! persistence under `<id>/tool-results/`.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

/// Claude's per-project transcript directory name: every non-alphanumeric character of the
/// absolute working directory becomes `-`.
pub fn encode_project_dir(cwd: &str) -> String {
    cwd.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// `<config>/projects/<project>/<session id>.jsonl`.
pub fn transcript_path(config_dir: &Path, cwd: &str, session_id: &str) -> PathBuf {
    config_dir
        .join("projects")
        .join(encode_project_dir(cwd))
        .join(format!("{session_id}.jsonl"))
}

/// `<main path minus .jsonl>/subagents/agent-<agent id>.jsonl`.
pub fn subagent_path(main: &Path, agent_id: &str) -> PathBuf {
    main.with_extension("")
        .join("subagents")
        .join(format!("agent-{agent_id}.jsonl"))
}

/// What the records say about the CLI that wrote them.
#[derive(Debug, Clone, PartialEq)]
pub struct Style {
    pub version: String,
    pub entrypoint: String,
    pub model: String,
    pub git_branch: String,
    pub permission_mode: String,
    /// `promptSource` / `turnOrigin` of a prompt (`sdk` for stream-json, `typed` in the TUI).
    pub prompt_source: String,
}

impl Default for Style {
    fn default() -> Self {
        Style {
            version: "2.1.293".into(),
            entrypoint: "sdk-cli".into(),
            model: "claude-haiku-5-5".into(),
            git_branch: "main".into(),
            permission_mode: "default".into(),
            prompt_source: "sdk".into(),
        }
    }
}

/// Where ids and times come from: the real ones, or a deterministic sequence (fixtures).
#[derive(Debug, Clone)]
enum Source {
    Random,
    Seeded { seed: u64, n: u64 },
}

/// First timestamp of a deterministic transcript (2026-11-20T00:00:00Z) and the step between two.
const SEEDED_T0_MS: u64 = 1_795_132_800_000;
const SEEDED_STEP_MS: u64 = 250;

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl Source {
    fn hex(&mut self, len: usize) -> String {
        match self {
            Source::Random => crate::util::hex(len),
            Source::Seeded { seed, n } => {
                *n += 1;
                let mut s = String::new();
                let mut k = 0u64;
                while s.len() < len {
                    s.push_str(&format!("{:016x}", mix(*seed ^ mix(*n * 31 + k))));
                    k += 1;
                }
                s.truncate(len);
                s
            }
        }
    }

    fn uuid(&mut self) -> String {
        let h = self.hex(32);
        format!(
            "{}-{}-4{}-a{}-{}",
            &h[0..8],
            &h[8..12],
            &h[13..16],
            &h[17..20],
            &h[20..32]
        )
    }

    fn now_ms(&mut self) -> u64 {
        match self {
            Source::Random => crate::util::now_ms(),
            Source::Seeded { n, .. } => {
                *n += 1;
                SEEDED_T0_MS + *n * SEEDED_STEP_MS
            }
        }
    }

    /// Continue after the draws another copy of this source made.
    fn catch_up(&mut self, other: &Source) {
        if let (Source::Seeded { n, .. }, Source::Seeded { n: m, .. }) = (self, other) {
            *n = (*n).max(*m);
        }
    }

    fn timestamp(&mut self) -> String {
        let ms = self.now_ms();
        crate::util::iso_from_ms(ms)
    }
}

/// Identifies a subagent's own file: its records carry `isSidechain` and the agent's id.
#[derive(Debug, Clone)]
struct Sidechain {
    agent_id: String,
}

/// The incremental writer of one transcript file (a main one or a subagent's). Records are
/// buffered and written by [`Writer::flush`] (appending, creating the file and its directories).
pub struct Writer {
    path: PathBuf,
    session_id: String,
    cwd: String,
    style: Style,
    src: Source,
    side: Option<Sidechain>,
    /// `uuid` of the last record of the chain (what the next record's `parentUuid` is).
    leaf: Option<String>,
    /// Prompts written (their `uuid` and `parentUuid`), for [`Writer::rewind`].
    prompts: Vec<(String, Option<String>)>,
    turn_index: u64,
    turn_open: bool,
    last_prompt: String,
    api_calls: u64,
    pending: Vec<String>,
    /// The file did not end in a line break when it was opened (a cut line): the first write
    /// starts a new line.
    needs_break: bool,
    /// The file was not there when the writer opened it.
    fresh: bool,
}

impl Writer {
    /// A writer appending to `path`, continuing the chain of the file when it exists (its last
    /// record with a `uuid` is the leaf).
    pub fn open(path: PathBuf, session_id: &str, cwd: &str) -> Writer {
        Writer::open_with(
            path,
            session_id,
            cwd,
            Style::default(),
            Source::Random,
            None,
        )
    }

    fn open_with(
        path: PathBuf,
        session_id: &str,
        cwd: &str,
        style: Style,
        src: Source,
        side: Option<Sidechain>,
    ) -> Writer {
        let mut w = Writer {
            path,
            session_id: session_id.to_string(),
            cwd: cwd.to_string(),
            style,
            src,
            side,
            leaf: None,
            prompts: Vec::new(),
            turn_index: 0,
            turn_open: false,
            last_prompt: String::new(),
            api_calls: 0,
            pending: Vec::new(),
            needs_break: false,
            fresh: true,
        };
        w.scan_existing();
        w
    }

    /// Learn the chain's leaf and the turn count from a file that is already there, by looking
    /// at its lines without parsing them all (a transcript can be tens of megabytes).
    fn scan_existing(&mut self) {
        let Ok(f) = std::fs::File::open(&self.path) else {
            return;
        };
        self.fresh = false;
        let mut r = std::io::BufReader::with_capacity(1 << 20, f);
        let mut buf = Vec::new();
        let mut last_uuid_line: Option<Vec<u8>> = None;
        let mut turns = 0u64;
        loop {
            buf.clear();
            match r.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            self.needs_break = !buf.ends_with(b"\n");
            if contains(&buf, b"\"turnPosition\"") {
                turns += 1;
            }
            if contains(&buf, b"\"uuid\":\"") && buf.ends_with(b"\n") {
                last_uuid_line = Some(buf.clone());
            }
        }
        self.turn_index = turns;
        if let Some(line) = last_uuid_line {
            if let Ok(v) = serde_json::from_slice::<Value>(&line) {
                self.leaf = v["uuid"].as_str().map(str::to_string);
            }
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The file did not exist when the writer opened it.
    pub fn is_fresh(&self) -> bool {
        self.fresh
    }

    /// The `uuid` the next chained record gets as its `parentUuid`.
    pub fn leaf(&self) -> Option<&str> {
        self.leaf.as_deref()
    }

    /// Make `uuid` the leaf: the next record chains from it (a branch).
    pub fn set_leaf(&mut self, uuid: Option<String>) {
        self.leaf = uuid;
    }

    /// Bytes buffered and not yet written.
    pub fn pending_bytes(&self) -> usize {
        self.pending.iter().map(|l| l.len() + 1).sum()
    }

    // ── plumbing ────────────────────────────────────────────────────────────

    fn line(&mut self, v: &Value) {
        self.pending.push(v.to_string());
    }

    /// Write what is buffered.
    pub fn flush(&mut self) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        if std::mem::take(&mut self.needs_break) {
            f.write_all(b"\n")?;
        }
        let mut out = String::with_capacity(self.pending_bytes());
        for l in self.pending.drain(..) {
            out.push_str(&l);
            out.push('\n');
        }
        f.write_all(out.as_bytes())
    }

    /// [`Writer::flush`], saying so on stderr when it fails.
    pub fn flush_logged(&mut self) {
        // A transcript that cannot be written is a conversation the next process cannot resume:
        // say so.
        if let Err(e) = self.flush() {
            eprintln!("wasit-fake-cli: cannot write {}: {e}", self.path.display());
        }
    }

    /// The fields every chained record carries; sets the record's `uuid`, `parentUuid` and
    /// `timestamp`, and makes it the leaf. Returns its uuid.
    fn chain(&mut self, mut rec: Value) -> String {
        let uuid = self.src.uuid();
        let ts = self.src.timestamp();
        let side = self.side.is_some();
        let obj = rec.as_object_mut().expect("records are objects");
        obj.insert("parentUuid".into(), json!(self.leaf));
        obj.insert("isSidechain".into(), json!(side));
        if let Some(s) = &self.side {
            obj.insert("agentId".into(), json!(s.agent_id));
        }
        obj.insert("uuid".into(), json!(uuid));
        obj.insert("timestamp".into(), json!(ts));
        obj.insert("userType".into(), json!("external"));
        obj.insert("entrypoint".into(), json!(self.style.entrypoint));
        obj.insert("cwd".into(), json!(self.cwd));
        obj.insert("sessionId".into(), json!(self.session_id));
        obj.insert("version".into(), json!(self.style.version));
        obj.insert("gitBranch".into(), json!(self.style.git_branch));
        self.line(&rec);
        self.leaf = Some(uuid.clone());
        uuid
    }

    fn bookkeeping(&mut self, mut rec: Value) {
        rec["sessionId"] = json!(self.session_id);
        self.line(&rec);
    }

    // ── bookkeeping records (no uuid, not in the chain) ─────────────────────

    /// `queue-operation` enqueue then dequeue: what a stream-json run writes before a prompt.
    pub fn queue_operations(&mut self) {
        for op in ["enqueue", "dequeue"] {
            let ts = self.src.timestamp();
            self.bookkeeping(json!({"type":"queue-operation","operation":op,"timestamp":ts}));
        }
    }

    pub fn atis_latch(&mut self) {
        self.bookkeeping(json!({"type":"atis-latch","atis":""}));
    }

    pub fn mode(&mut self, mode: &str) {
        self.bookkeeping(json!({"type":"mode","mode":mode}));
    }

    pub fn permission_mode(&mut self, mode: &str) {
        self.bookkeeping(json!({"type":"permission-mode","permissionMode":mode}));
    }

    pub fn ai_title(&mut self, title: &str) {
        self.bookkeeping(json!({"type":"ai-title","aiTitle":title}));
    }

    /// `custom-title` and `agent-name`, as `--name` writes them.
    pub fn titles(&mut self, name: &str) {
        self.bookkeeping(json!({"type":"custom-title","customTitle":name}));
        self.bookkeeping(json!({"type":"agent-name","agentName":name}));
    }

    pub fn last_prompt(&mut self, text: &str) {
        let leaf = self.leaf.clone();
        self.bookkeeping(json!({"type":"last-prompt","lastPrompt":text,"leafUuid":leaf}));
    }

    pub fn cost_state(&mut self) {
        let calls = self.api_calls;
        let now = self.src.now_ms();
        self.bookkeeping(json!({
            "type":"cost-state","totalCostUSD":0.0001 * calls as f64,
            "totalAPIDuration":300 * calls,"totalAPIDurationWithoutRetries":290 * calls,
            "totalToolDuration":0,"totalLinesAdded":0,"totalLinesRemoved":0,
            "totalDuration":600 * calls,"startTime":now,
            "modelUsage":{self.style.model.clone():{
                "inputTokens":10 * calls,"outputTokens":3 * calls,"thinkingTokens":0,
                "cacheReadInputTokens":0,"cacheCreationInputTokens":0,"webSearchRequests":0,
                "costUSD":0.0001 * calls as f64}},
            "hasUnknownModelCost":false}));
    }

    /// A record of a type nobody knows: a reader must count it, not choke on it.
    pub fn unknown(&mut self, kind: &str) {
        self.bookkeeping(json!({"type":kind,"payload":{"n":1}}));
    }

    /// A line cut in the middle, with no end of line: what a file looks like when read while the
    /// CLI is writing it. Must be the last thing written.
    pub fn truncated_tail(&mut self) {
        let mut v = json!({"type":"user","uuid":self.src.uuid(),"parentUuid":self.leaf,
                           "message":{"role":"user","content":[{"type":"text","text":"cut"}]}})
        .to_string();
        v.truncate(v.len() / 2);
        self.flush_logged();
        if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(&self.path) {
            // (Test helper: a failing write leaves the file without its cut line, which the
            // test that asked for it notices.)
            let _ = f.write_all(v.as_bytes());
        }
    }

    // ── chained records ─────────────────────────────────────────────────────

    /// The attachments the CLI writes with the first prompt of a conversation, plus the state
    /// markers: a few small ones, in the real shape.
    pub fn first_turn_attachments(&mut self) {
        let cwd = self.cwd.clone();
        self.attachment(
            json!({"type":"environment","snapshot":{"workingDirectory":cwd,"isWorktree":false,
                   "isGitRepo":false,"additionalWorkingDirectories":[],"platform":"linux",
                   "shell":"bash","osVersion":"Linux"}}),
            Some(("system", "environment")),
        );
        self.attachment(
            json!({"type":"model","identity":{"modelId":self.style.model,
                   "marketingName":null,"knowledgeCutoff":null},"text":"model"}),
            Some(("system", "model")),
        );
        self.atis_latch();
        self.attachment(json!({"type":"session_context","context":{}}), None);
        self.attachment(
            json!({"type":"date","date":"2026-11-20"}),
            Some(("system", "date")),
        );
    }

    /// An `attachment` record; `rendered` is the `(role, text)` the model was given for it.
    pub fn attachment(&mut self, attachment: Value, rendered: Option<(&str, &str)>) -> String {
        let mut rec = json!({"type":"attachment","attachment":attachment});
        if let Some((role, text)) = rendered {
            rec["rendered"] = json!([{"content":text}]);
            rec["renderedRole"] = json!(role);
        }
        self.chain(rec)
    }

    /// Start a turn: `queue-operation`s, the user record (`content`: a string or blocks), and
    /// on a fresh file the attachments of a first prompt. Returns the prompt's uuid.
    pub fn begin_turn(&mut self, content: Value) -> String {
        self.end_turn();
        let text = text_of(&content);
        let first = self.leaf.is_none();
        if self.side.is_none() {
            self.queue_operations();
        }
        self.turn_index += 1;
        let prompt_id = self.src.uuid();
        let rec = json!({"type":"user","promptId":prompt_id,
            "message":{"role":"user","content":content},
            "permissionMode":self.style.permission_mode,
            "promptSource":self.style.prompt_source,"turnOrigin":self.style.prompt_source,
            "turnPosition":{"promptIndex":0,"turnIndex":self.turn_index}});
        let parent = self.leaf.clone();
        let uuid = self.chain(rec);
        self.prompts.push((uuid.clone(), parent));
        self.last_prompt = text;
        self.turn_open = true;
        if first && self.side.is_none() {
            self.first_turn_attachments();
        }
        uuid
    }

    /// The `user` record of a subagent's own conversation (its prompt).
    fn sidechain_prompt(&mut self, prompt: &str) -> String {
        let rec = json!({"type":"user","promptId":self.src.uuid(),
            "message":{"role":"user","content":prompt}});
        self.chain(rec)
    }

    /// End the turn that is open: the `last-prompt` and `cost-state` records the CLI writes
    /// after its last message. Does nothing when no turn is open.
    pub fn end_turn(&mut self) {
        if !std::mem::take(&mut self.turn_open) || self.side.is_some() {
            return;
        }
        let text = self.last_prompt.clone();
        self.last_prompt(&text);
        self.cost_state();
    }

    /// One API message as the CLI records it: ONE `assistant` record per content block, all with
    /// the same `message.id`; every block but the last has no `stop_reason`, the last has
    /// `stop` (`end_turn`, `tool_use`). Returns the uuids of the records.
    pub fn assistant_message(&mut self, blocks: Vec<Value>, stop: &str) -> Vec<String> {
        self.api_calls += 1;
        let msg_id = format!("msg_{}", crate::util::alnum(24));
        let request_id = format!("req_{}", crate::util::alnum(24));
        let n = blocks.len();
        let usage = json!({"input_tokens":10,"cache_creation_input_tokens":0,
            "cache_read_input_tokens":0,"output_tokens":3,
            "output_tokens_details":{"thinking_tokens":0},
            "server_tool_use":{"web_search_requests":0,"web_fetch_requests":0},
            "service_tier":"standard","cache_creation":{"ephemeral_1h_input_tokens":0,
            "ephemeral_5m_input_tokens":0},"inference_geo":"","iterations":[],
            "speed":"standard","fallback_credit":null});
        blocks
            .into_iter()
            .enumerate()
            .map(|(i, block)| {
                let last = i + 1 == n;
                let rec = json!({"type":"assistant","message":{
                    "model":self.style.model,"id":msg_id,"type":"message","role":"assistant",
                    "content":[block],"container":null,
                    "stop_reason":if last { json!(stop) } else { Value::Null },
                    "stop_sequence":null,"stop_details":null,"usage":usage,"diagnostics":null},
                    "apiBlockIndex":i,"requestId":request_id,"effort":"high",
                    "perTurnEffort":null});
                self.chain(rec)
            })
            .collect()
    }

    /// An assistant message holding one text block.
    pub fn assistant_text(&mut self, text: &str) -> String {
        self.assistant_message(vec![json!({"type":"text","text":text})], "end_turn")
            .remove(0)
    }

    /// A thinking block as the real files hold it: no text, a signature.
    pub fn assistant_thinking(&mut self) -> String {
        let sig = self.src.hex(24);
        self.assistant_message(
            vec![json!({"type":"thinking","thinking":"","signature":format!("sig-{sig}")})],
            "end_turn",
        )
        .remove(0)
    }

    /// What the CLI records when the user interrupts a turn.
    pub fn interrupted(&mut self) -> String {
        let rec = json!({"type":"user","promptId":self.src.uuid(),
            "message":{"role":"user","content":[{"type":"text","text":"[Request interrupted by user]"}]}});
        self.chain(rec)
    }

    /// The error the CLI records as an assistant message when a turn fails.
    pub fn assistant_error(&mut self, text: &str) -> String {
        let rec = json!({"type":"assistant","isApiErrorMessage":true,"message":{
            "model":"<synthetic>","id":format!("msg_{}", crate::util::alnum(24)),
            "type":"message","role":"assistant","content":[{"type":"text","text":text}],
            "stop_reason":"stop_sequence","stop_sequence":"","usage":{"input_tokens":0,"output_tokens":0}}});
        self.chain(rec)
    }

    /// A tool call: the `tool_use` block of an assistant message. Returns `(call id, record
    /// uuid)`.
    pub fn tool_use(&mut self, name: &str, input: &Value) -> (String, String) {
        let call = format!("toolu_{}", crate::util::alnum(24));
        let uuid = self.tool_use_with_id(&call, name, input);
        (call, uuid)
    }

    pub fn tool_use_with_id(&mut self, call: &str, name: &str, input: &Value) -> String {
        self.assistant_message(
            vec![
                json!({"type":"tool_use","id":call,"name":name,"input":input,
                        "caller":{"type":"direct"}}),
            ],
            "tool_use",
        )
        .remove(0)
    }

    /// The result of tool call `call`: a `user` record with a `tool_result` block (`content`:
    /// a string or blocks) and the tool's structured `tool_use_result`.
    pub fn tool_result(
        &mut self,
        call: &str,
        content: Value,
        is_error: bool,
        tool_use_result: Value,
        assistant_uuid: &str,
    ) -> String {
        let rec = json!({"type":"user","promptId":self.src.uuid(),
            "message":{"role":"user","content":[
                {"tool_use_id":call,"type":"tool_result","content":content,"is_error":is_error}]},
            "toolUseResult":tool_use_result,"sourceToolAssistantUUID":assistant_uuid});
        self.chain(rec)
    }

    /// Compaction: the boundary (`parentUuid` null, `logicalParentUuid` the leaf before it) and
    /// the summary record the CLI writes after it.
    pub fn compaction(&mut self) {
        let logical = self.leaf.clone();
        self.leaf = None;
        let boundary = json!({"type":"system","subtype":"compact_boundary","content":"Conversation compacted",
            "level":"info","logicalParentUuid":logical,
            "compactMetadata":{"trigger":"manual","preTokens":12000,"postTokens":2000,
                "cumulativeDroppedTokens":10000,"durationMs":900}});
        self.chain(boundary);
        let summary = json!({"type":"user","promptId":self.src.uuid(),
            "message":{"role":"user","content":"This session is being continued from a previous conversation. Summary: earlier work."},
            "isCompactSummary":true,"isVisibleInTranscriptOnly":true});
        self.chain(summary);
    }

    /// Branch: the next record chains from the parent of the `n`-th last user prompt (`n` = 1:
    /// the last prompt is replaced by an edited one), so file order stops being chain order.
    pub fn rewind(&mut self, prompts_back: usize) {
        let n = prompts_back.max(1);
        if self.prompts.len() >= n {
            let (_, parent) = self.prompts[self.prompts.len() - n].clone();
            self.leaf = parent;
        }
    }

    // ── a subagent ──────────────────────────────────────────────────────────

    /// A subagent (a `Task`/`Agent` tool call, the subagent's own file, the call's result) and
    /// its outcome in the main chain. See [`SubagentSpec`].
    pub fn subagent(&mut self, spec: &SubagentSpec) -> SubagentRun {
        let input = json!({"description":spec.description,"prompt":spec.prompt,
            "subagent_type":spec.subagent_type,"model":"haiku"});
        let (call, assistant) = self.tool_use("Agent", &input);
        let run = self.subagent_files(spec, &call);
        let (content, result) = if spec.background {
            (
                json!([{"type":"text","text":"Async agent launched successfully."}]),
                json!({"status":"async_launched","agentId":run.agent_id,"description":spec.description,
                       "prompt":spec.prompt,"outputFile":format!("/tmp/claude-fake/{}/tasks/{}.output", self.session_id, run.agent_id),
                       "canReadOutputFile":true,"isAsync":true,"resolvedModel":"haiku"}),
            )
        } else {
            let answer = spec.answer.clone();
            (
                json!([{"type":"text","text":answer}]),
                json!({"status":"completed","agentId":run.agent_id,"agentType":spec.subagent_type,
                       "content":[{"type":"text","text":answer}],"prompt":spec.prompt,
                       "resolvedModel":"haiku","totalDurationMs":1200,"totalTokens":900,
                       "totalToolUseCount":run.tool_calls,
                       "toolStats":{"readCount":0,"searchCount":0,"bashCount":0,"editFileCount":0,
                                    "linesAdded":0,"linesRemoved":0,"otherToolCount":run.tool_calls},
                       "usage":{"input_tokens":10,"output_tokens":3}}),
            )
        };
        self.tool_result(&call, content, false, result, &assistant);
        run
    }

    /// Start a subagent's own file: its meta and its first record (the prompt). Returns the
    /// writer of the file, which the caller appends the subagent's conversation to; `call` is the
    /// spawning tool call's id. The main file links to it by `agent_id`.
    pub fn begin_subagent(
        &mut self,
        agent_id: &str,
        call: &str,
        description: &str,
        subagent_type: &str,
        prompt: &str,
    ) -> Writer {
        let path = subagent_path(&self.path, agent_id);
        let mut sub = Writer::open_with(
            path.clone(),
            &self.session_id,
            &self.cwd,
            self.style.clone(),
            self.src.clone(),
            Some(Sidechain {
                agent_id: agent_id.to_string(),
            }),
        );
        sub.sidechain_prompt(prompt);
        sub.flush_logged();
        // The sub-writer draws from a copy of the source: move ours past what it used so ids
        // stay distinct.
        self.src.catch_up(&sub.src);
        let meta = json!({"agentType":subagent_type,"description":description,
            "toolUseId":call,"spawnDepth":1,"requestShape":"standard",
            "requestNonInteractive":true,"model":"haiku"});
        let meta_path = path.with_extension("meta.json");
        if let Err(e) = std::fs::write(&meta_path, meta.to_string()) {
            eprintln!("wasit-fake-cli: cannot write {}: {e}", meta_path.display());
        }
        sub
    }

    /// Write the subagent's file and meta; `call` is the spawning call's id.
    pub fn subagent_files(&mut self, spec: &SubagentSpec, call: &str) -> SubagentRun {
        let agent_id = format!("a{}", self.src.hex(16));
        let mut sub = self.begin_subagent(
            &agent_id,
            call,
            &spec.description,
            &spec.subagent_type,
            &spec.prompt,
        );
        let tool_calls = sub.content(&spec.content);
        sub.flush_logged();
        self.src.catch_up(&sub.src);
        SubagentRun {
            agent_id,
            call_id: call.to_string(),
            path: sub.path.clone(),
            tool_calls,
        }
    }

    /// Write one piece of content of a conversation (an assistant text, a thinking block, a tool
    /// call with its result). Returns how many tool calls it made.
    pub fn content(&mut self, items: &[Content]) -> u64 {
        let mut tools = 0;
        for c in items {
            match c {
                Content::Text(t) => {
                    self.assistant_text(t);
                }
                Content::Thinking => {
                    self.assistant_thinking();
                }
                Content::Tool {
                    name,
                    input,
                    output,
                } => {
                    tools += 1;
                    let (call, a) = self.tool_use(name, input);
                    match output {
                        Output::Text(text) => {
                            self.tool_result(
                                &call,
                                json!(text),
                                false,
                                json!({"stdout":text,"stderr":"","interrupted":false,"isImage":false,"noOutputExpected":false}),
                                &a,
                            );
                        }
                        Output::Error(text) => {
                            self.tool_result(
                                &call,
                                json!(text),
                                true,
                                json!(format!("Error: {text}")),
                                &a,
                            );
                        }
                        Output::Image { mime, base64 } => {
                            self.tool_result(
                                &call,
                                json!([{"type":"image","source":{"type":"base64",
                                        "media_type":mime,"data":base64}}]),
                                false,
                                json!({"type":"image","file":{"base64":base64,"type":mime,
                                       "originalSize":base64.len(),
                                       "dimensions":{"originalWidth":40,"originalHeight":30,
                                                     "displayWidth":40,"displayHeight":30}}}),
                                &a,
                            );
                        }
                    }
                }
            }
        }
        tools
    }
}

/// `needle` is in `hay` (both text; `str::contains` is the fast search even in a debug build).
fn contains(hay: &[u8], needle: &[u8]) -> bool {
    match (std::str::from_utf8(hay), std::str::from_utf8(needle)) {
        (Ok(h), Ok(n)) => h.contains(n),
        _ => hay.windows(needle.len()).any(|w| w == needle),
    }
}

/// The text of a user message content (string, or the text blocks of an array).
fn text_of(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter(|b| b["type"] == "text")
            .filter_map(|b| b["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

// ── the builder ─────────────────────────────────────────────────────────────

/// What a tool call returned.
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Text(String),
    /// `is_error: true`.
    Error(String),
    /// The result is an image block (a `Read` of an image file); `base64` is the image.
    Image {
        mime: String,
        base64: String,
    },
}

/// One piece of what the assistant did.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Text(String),
    Thinking,
    Tool {
        name: String,
        input: Value,
        output: Output,
    },
}

impl Content {
    pub fn text(t: impl Into<String>) -> Content {
        Content::Text(t.into())
    }

    pub fn tool(name: impl Into<String>, input: Value, output: impl Into<String>) -> Content {
        Content::Tool {
            name: name.into(),
            input,
            output: Output::Text(output.into()),
        }
    }
}

/// A subagent the assistant spawned.
#[derive(Debug, Clone, PartialEq)]
pub struct SubagentSpec {
    pub description: String,
    pub subagent_type: String,
    /// The prompt the subagent was given (the first record of its file).
    pub prompt: String,
    /// What the subagent did.
    pub content: Vec<Content>,
    /// Its final answer (the spawning call's result, for a subagent that is not in the
    /// background).
    pub answer: String,
    /// Launched in the background: the call's result is the "launched" notice.
    pub background: bool,
}

impl SubagentSpec {
    pub fn new(description: impl Into<String>, prompt: impl Into<String>) -> SubagentSpec {
        SubagentSpec {
            description: description.into(),
            subagent_type: "general-purpose".into(),
            prompt: prompt.into(),
            content: Vec::new(),
            answer: "done".into(),
            background: false,
        }
    }

    pub fn content(mut self, c: Content) -> SubagentSpec {
        self.content.push(c);
        self
    }

    pub fn answer(mut self, a: impl Into<String>) -> SubagentSpec {
        self.answer = a.into();
        self
    }

    pub fn background(mut self) -> SubagentSpec {
        self.background = true;
        self
    }
}

/// A subagent that was written.
#[derive(Debug, Clone, PartialEq)]
pub struct SubagentRun {
    pub agent_id: String,
    /// The tool call that spawned it (`toolUseId` of its meta file).
    pub call_id: String,
    pub path: PathBuf,
    pub tool_calls: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Item {
    User(Value),
    Content(Content),
    Subagent(SubagentSpec),
    Compaction,
    Rewind(usize),
    EndTurn,
    QueueOperations,
    LastPrompt(String),
    CostState,
    Mode(String),
    AiTitle(String),
    Titles(String),
    Attachment(Value),
    Unknown(String),
    Filler { turns: usize, bytes_per_turn: usize },
    TruncatedTail,
}

/// A conversation described step by step, written as claude would have written it. Methods
/// consume and return the builder.
///
/// ```
/// use wasit_fake_cli::transcript::Transcript;
/// let t = Transcript::deterministic(7).user("hello").assistant("hi there");
/// let dir = std::env::temp_dir().join(format!("wasit-transcript-doc-{}", std::process::id()));
/// let path = t.write(&dir, "/work/proj", "3b241101-e2bb-4255-8caf-4136c566a962");
/// assert!(path.is_file());
/// # std::fs::remove_dir_all(dir).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct Transcript {
    style: Style,
    seed: Option<u64>,
    items: Vec<Item>,
}

impl Default for Transcript {
    fn default() -> Self {
        Transcript::new()
    }
}

impl Transcript {
    /// Real ids and times.
    pub fn new() -> Transcript {
        Transcript {
            style: Style::default(),
            seed: None,
            items: Vec::new(),
        }
    }

    /// Ids and times derived from `seed` and a counter: the same builder gives the same file.
    pub fn deterministic(seed: u64) -> Transcript {
        Transcript {
            seed: Some(seed),
            ..Transcript::new()
        }
    }

    pub fn style(mut self, style: Style) -> Transcript {
        self.style = style;
        self
    }

    /// A user prompt, as a stream-json run writes it (an array of blocks).
    pub fn user(mut self, text: &str) -> Transcript {
        self.items
            .push(Item::User(json!([{"type":"text","text":text}])));
        self
    }

    /// A prompt typed in the TUI (the content is a string).
    pub fn user_typed(mut self, text: &str) -> Transcript {
        self.items.push(Item::User(json!(text)));
        self
    }

    /// A prompt with an image (base64) after its text.
    pub fn user_with_image(mut self, text: &str, mime: &str, base64: &str) -> Transcript {
        self.items.push(Item::User(json!([
            {"type":"text","text":text},
            {"type":"image","source":{"type":"base64","media_type":mime,"data":base64}}])));
        self
    }

    pub fn assistant(self, text: &str) -> Transcript {
        self.content(Content::text(text))
    }

    pub fn thinking(self) -> Transcript {
        self.content(Content::Thinking)
    }

    /// A tool call and its text result.
    pub fn tool(self, name: &str, input: Value, output: &str) -> Transcript {
        self.content(Content::tool(name, input, output))
    }

    pub fn tool_error(self, name: &str, input: Value, error: &str) -> Transcript {
        self.content(Content::Tool {
            name: name.into(),
            input,
            output: Output::Error(error.into()),
        })
    }

    pub fn tool_image(self, name: &str, input: Value, mime: &str, base64: &str) -> Transcript {
        self.content(Content::Tool {
            name: name.into(),
            input,
            output: Output::Image {
                mime: mime.into(),
                base64: base64.into(),
            },
        })
    }

    pub fn content(mut self, c: Content) -> Transcript {
        self.items.push(Item::Content(c));
        self
    }

    pub fn subagent(mut self, s: SubagentSpec) -> Transcript {
        self.items.push(Item::Subagent(s));
        self
    }

    /// Compaction (boundary and summary).
    pub fn compaction(mut self) -> Transcript {
        self.items.push(Item::Compaction);
        self
    }

    /// The next record chains from the parent of the `n`-th last user prompt (an edited
    /// message).
    pub fn rewind(mut self, prompts_back: usize) -> Transcript {
        self.items.push(Item::Rewind(prompts_back));
        self
    }

    /// End the turn now (`last-prompt`, `cost-state`); a turn is also ended by the next prompt
    /// and at the end of the builder.
    pub fn end_turn(mut self) -> Transcript {
        self.items.push(Item::EndTurn);
        self
    }

    pub fn queue_operations(mut self) -> Transcript {
        self.items.push(Item::QueueOperations);
        self
    }

    pub fn last_prompt(mut self, text: &str) -> Transcript {
        self.items.push(Item::LastPrompt(text.into()));
        self
    }

    pub fn cost_state(mut self) -> Transcript {
        self.items.push(Item::CostState);
        self
    }

    pub fn mode(mut self, mode: &str) -> Transcript {
        self.items.push(Item::Mode(mode.into()));
        self
    }

    pub fn ai_title(mut self, title: &str) -> Transcript {
        self.items.push(Item::AiTitle(title.into()));
        self
    }

    /// `custom-title` and `agent-name`.
    pub fn titles(mut self, name: &str) -> Transcript {
        self.items.push(Item::Titles(name.into()));
        self
    }

    /// An `attachment` record with this `attachment` object.
    pub fn attachment(mut self, attachment: Value) -> Transcript {
        self.items.push(Item::Attachment(attachment));
        self
    }

    /// A record of a type nobody knows.
    pub fn unknown_record(mut self, kind: &str) -> Transcript {
        self.items.push(Item::Unknown(kind.into()));
        self
    }

    /// `turns` turns (a prompt, a tool call with a result and a reply) of about `bytes_per_turn`
    /// bytes each: a big transcript fast.
    pub fn filler(mut self, turns: usize, bytes_per_turn: usize) -> Transcript {
        self.items.push(Item::Filler {
            turns,
            bytes_per_turn,
        });
        self
    }

    /// A last line cut in half, with no end of line. Must be the last item.
    pub fn truncated_tail(mut self) -> Transcript {
        self.items.push(Item::TruncatedTail);
        self
    }

    /// Write a new file (replacing one that is there, and the subagent files next to it).
    pub fn write(&self, config_dir: &Path, cwd: &str, session_id: &str) -> PathBuf {
        let path = transcript_path(config_dir, cwd, session_id);
        // Replacing a conversation: its old subagent files go with it (a missing path is fine).
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(path.with_extension(""));
        self.append(config_dir, cwd, session_id)
    }

    /// Append to the conversation (creating it when it is not there), chaining from its last
    /// record as a resumed run does.
    pub fn append(&self, config_dir: &Path, cwd: &str, session_id: &str) -> PathBuf {
        let path = transcript_path(config_dir, cwd, session_id);
        let src = match self.seed {
            Some(seed) => Source::Seeded { seed, n: 0 },
            None => Source::Random,
        };
        let mut w = Writer::open_with(path.clone(), session_id, cwd, self.style.clone(), src, None);
        self.run(&mut w);
        path
    }

    /// Play the items into `w`.
    pub fn run(&self, w: &mut Writer) {
        for item in &self.items {
            match item {
                Item::User(content) => {
                    w.begin_turn(content.clone());
                }
                Item::Content(c) => {
                    w.content(std::slice::from_ref(c));
                }
                Item::Subagent(s) => {
                    w.subagent(s);
                }
                Item::Compaction => w.compaction(),
                Item::Rewind(n) => w.rewind(*n),
                Item::EndTurn => w.end_turn(),
                Item::QueueOperations => w.queue_operations(),
                Item::LastPrompt(t) => w.last_prompt(t),
                Item::CostState => w.cost_state(),
                Item::Mode(m) => w.mode(m),
                Item::AiTitle(t) => w.ai_title(t),
                Item::Titles(t) => w.titles(t),
                Item::Attachment(a) => {
                    w.attachment(a.clone(), None);
                }
                Item::Unknown(k) => w.unknown(k),
                Item::Filler {
                    turns,
                    bytes_per_turn,
                } => {
                    for i in 0..*turns {
                        let body = filler_text(*bytes_per_turn / 2, i);
                        w.begin_turn(json!([{"type":"text","text":format!("question {i}")}]));
                        w.content(&[Content::tool(
                            "Bash",
                            json!({"command":format!("echo {i}"),"description":"filler"}),
                            body.clone(),
                        )]);
                        w.assistant_text(&format!(
                            "answer {i}: {}",
                            filler_text(*bytes_per_turn / 2, i + 1)
                        ));
                        if w.pending_bytes() > (1 << 20) {
                            w.flush_logged();
                        }
                    }
                }
                Item::TruncatedTail => {
                    w.end_turn();
                    w.truncated_tail();
                }
            }
        }
        w.end_turn();
        w.flush_logged();
    }
}

fn filler_text(len: usize, salt: usize) -> String {
    const WORDS: [&str; 8] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    ];
    let mut s = String::with_capacity(len + 8);
    let mut i = salt;
    while s.len() < len {
        s.push_str(WORDS[i % WORDS.len()]);
        s.push(' ');
        i = i.wrapping_mul(31).wrapping_add(7);
    }
    s.truncate(len);
    s
}

/// Write the scenario's pre-existing conversations (`sessions[]` with a `history`) as real
/// transcripts for the project they belong to (the session's `cwd`, else `default_cwd`). One
/// that is already stored is left alone. Returns the paths written.
pub fn materialise_sessions(
    config_dir: &Path,
    default_cwd: &str,
    sessions: &[crate::scenario::SessionInfo],
) -> Vec<PathBuf> {
    sessions
        .iter()
        .filter_map(|s| materialise_session(config_dir, default_cwd, s))
        .collect()
}

/// [`materialise_sessions`] for one session; `None` when it is already stored.
pub fn materialise_session(
    config_dir: &Path,
    default_cwd: &str,
    s: &crate::scenario::SessionInfo,
) -> Option<PathBuf> {
    let cwd = s.cwd.as_deref().unwrap_or(default_cwd);
    if transcript_path(config_dir, cwd, &s.id).is_file() {
        return None;
    }
    let mut t = Transcript::new();
    if let Some(title) = &s.title {
        t = t.titles(title);
    }
    for m in &s.history {
        t = match m.role.as_str() {
            "user" => t.user(&m.text),
            _ => t.assistant(&m.text),
        };
    }
    Some(t.write(config_dir, cwd, &s.id))
}
