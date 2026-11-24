//! Find over a session's events. [`searchable`] lists the text one [`SessionEvent`] contributes
//! to find, each piece tagged with the [`Field`] it came from; [`find_all`] matches a query in
//! one piece and [`hits_in`] cuts the text around each match. The app mirrors the rules of
//! [`searchable`] in TypeScript, and the shared fixture `tests/fixtures/searchable.json` pins
//! both sides to the same events and pieces.
//!
//! [`run`] is the scan behind `session.search` (B6b): it walks records of one log file in the
//! request's direction, stops at a hit limit or at [`SCAN_BUDGET`] bytes of records, and returns
//! a [`Resume`] token that continues the walk without repeating or skipping a hit.

use std::io;

use wasit_core::{AgentEvent, Command, ContentPart};

use crate::wire::{HistoryScope, SessionEvent, SessionEventKind};

/// Most hits one search page returns.
pub(crate) const MAX_HITS: usize = 50;
/// Bytes of event-log records one search may examine before it returns a resume token. The
/// record that crosses the budget is examined whole, so a page may exceed it by one record.
pub(crate) const SCAN_BUDGET: u64 = 8 << 20;
/// Characters of context shown on each side of a hit.
const RADIUS: usize = 40;

/// The part of a session event a piece of find text came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    /// A prompt the user sent, or a user message of the earlier history, or user text parts.
    UserText,
    /// Assistant prose and assistant text parts.
    AssistantText,
    /// Reasoning text.
    Thinking,
    /// The string values of a tool call's input.
    ToolInput,
    /// The string values of a tool call's result or of its progress.
    ToolResult,
    /// A notice the runtime or the CLI reported.
    Notice,
}

const FIELDS: [Field; 6] = [
    Field::UserText,
    Field::AssistantText,
    Field::Thinking,
    Field::ToolInput,
    Field::ToolResult,
    Field::Notice,
];

impl Field {
    /// The name of the field in the shared fixture, in the app, and on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Field::UserText => "user_text",
            Field::AssistantText => "assistant_text",
            Field::Thinking => "thinking",
            Field::ToolInput => "tool_input",
            Field::ToolResult => "tool_result",
            Field::Notice => "notice",
        }
    }

    /// The field with this name, if there is one.
    pub fn from_name(name: &str) -> Option<Field> {
        FIELDS.into_iter().find(|f| f.as_str() == name)
    }
}

/// The text of `event` that find searches, in the order it appears in the event. Empty texts
/// are not listed. Covered: user prompts and user messages, assistant prose, reasoning, the
/// string values of tool inputs and of tool results, and notices. Object keys are not text, and
/// neither are the discriminator values under a `"type"` key (`{"type":"text"}` labels its
/// content, it is not content). Events of the earlier history and of a subagent's conversation
/// are searched as the events they wrap. Anything else (phases, spawns, stderr, usage, …) has
/// no find text.
pub fn searchable(event: &SessionEvent) -> Vec<(Field, String)> {
    let mut out = Vec::new();
    match &event.kind {
        SessionEventKind::Agent { event } => agent(event, &mut out),
        SessionEventKind::Input {
            command: Command::Prompt { text, .. },
            ..
        } => push(&mut out, Field::UserText, text),
        SessionEventKind::Notice { message, .. } => push(&mut out, Field::Notice, message),
        _ => {}
    }
    out
}

fn agent(event: &AgentEvent, out: &mut Vec<(Field, String)>) {
    match event {
        AgentEvent::UserMessage { text } => push(out, Field::UserText, text),
        AgentEvent::UserContent { parts } => parts_text(out, Field::UserText, parts),
        AgentEvent::AssistantDelta { text, .. } => push(out, Field::AssistantText, text),
        AgentEvent::AssistantContent { parts, .. } => parts_text(out, Field::AssistantText, parts),
        AgentEvent::ThinkingDelta { text, .. } => push(out, Field::Thinking, text),
        AgentEvent::ToolCallStarted { input, .. } => json_strings(out, Field::ToolInput, input),
        AgentEvent::ToolCallUpdated {
            content,
            input,
            parts,
            ..
        } => {
            if let Some(input) = input {
                json_strings(out, Field::ToolInput, input);
            }
            for block in content {
                json_strings(out, Field::ToolResult, block);
            }
            parts_text(out, Field::ToolResult, parts);
        }
        AgentEvent::ToolCallFinished { result, parts, .. } => {
            if let Some(result) = result {
                json_strings(out, Field::ToolResult, result);
            }
            parts_text(out, Field::ToolResult, parts);
        }
        AgentEvent::Notice { message } => push(out, Field::Notice, message),
        AgentEvent::History { event, .. } | AgentEvent::Scoped { event, .. } => agent(event, out),
        _ => {}
    }
}

fn push(out: &mut Vec<(Field, String)>, field: Field, text: &str) {
    if !text.is_empty() {
        out.push((field, text.to_owned()));
    }
}

fn parts_text(out: &mut Vec<(Field, String)>, field: Field, parts: &[ContentPart]) {
    for part in parts {
        if let ContentPart::Text { text } = part {
            push(out, field, text);
        }
    }
}

/// Every string value inside `value`, depth first. Keys are not text, and a string under a
/// `"type"` key is a structural label, not text.
fn json_strings(out: &mut Vec<(Field, String)>, field: Field, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(s) => push(out, field, s),
        serde_json::Value::Array(items) => {
            for item in items {
                json_strings(out, field, item);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, item) in map {
                if key == "type" && item.is_string() {
                    continue;
                }
                json_strings(out, field, item);
            }
        }
        _ => {}
    }
}

/// A match of a query: character offsets `[start, end)` into the text (not bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

/// The matches of `query` in `text`, left to right, not overlapping. The comparison is
/// case-insensitive and character-wise: each character is folded to its single lowercase form
/// (a character whose lowercase is more than one character compares as itself). An empty query
/// matches nothing.
pub fn find_all(text: &str, query: &str) -> Vec<Span> {
    let query: Vec<char> = query.chars().map(fold).collect();
    if query.is_empty() {
        return Vec::new();
    }
    let text: Vec<char> = text.chars().map(fold).collect();
    let mut spans = Vec::new();
    let mut i = 0;
    while i + query.len() <= text.len() {
        if text[i..i + query.len()] == query[..] {
            spans.push(Span {
                start: i,
                end: i + query.len(),
            });
            i += query.len();
        } else {
            i += 1;
        }
    }
    spans
}

fn fold(c: char) -> char {
    let mut lower = c.to_lowercase();
    match (lower.next(), lower.next()) {
        (Some(one), None) => one,
        _ => c,
    }
}

/// A window of a text around a match. `start` and `end` are the match's character offsets
/// within `text`, so the app can highlight it without a second search. Line breaks and other
/// control characters are shown as spaces, one for one, so the offsets stay valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snippet {
    pub text: String,
    pub start: usize,
    pub end: usize,
}

/// One snippet per match of `query` in `text` (see [`find_all`] for the matching), each
/// [`RADIUS`] characters of context wide. The characters are collected once per text.
pub fn hits_in(text: &str, query: &str) -> Vec<Snippet> {
    let spans = find_all(text, query);
    if spans.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = text.chars().collect();
    spans
        .into_iter()
        .map(|span| window(&chars, span, RADIUS))
        .collect()
}

/// The characters of `chars` within `radius` characters of `span`, clipped to the text.
fn window(chars: &[char], span: Span, radius: usize) -> Snippet {
    let end = span.end.min(chars.len());
    let start = span.start.min(end);
    let from = start.saturating_sub(radius);
    let to = (end + radius).min(chars.len());
    let text: String = chars[from..to]
        .iter()
        .map(|&c| if c.is_control() { ' ' } else { c })
        .collect();
    Snippet {
        text,
        start: start - from,
        end: end - from,
    }
}

/// A search: the text to find, the events it may come from (`scope`), and the fields it may
/// match (`kinds`, every field when `None`).
#[derive(Debug, Clone)]
pub(crate) struct Query {
    text: String,
    scope: HistoryScope,
    kinds: Option<Vec<Field>>,
}

impl Query {
    /// Validates a request: the text must not be empty, and `kinds`, when given, names at least
    /// one known field.
    pub(crate) fn new(
        text: &str,
        scope: HistoryScope,
        kinds: Option<&[String]>,
    ) -> Result<Query, String> {
        if text.is_empty() {
            return Err("the query is empty".into());
        }
        let kinds = match kinds {
            None => None,
            Some([]) => return Err("kinds names no field".into()),
            Some(names) => Some(
                names
                    .iter()
                    .map(|n| Field::from_name(n).ok_or_else(|| format!("unknown kind {n}")))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        };
        Ok(Query {
            text: text.to_owned(),
            scope,
            kinds,
        })
    }
}

/// Where a search page stopped: the record with `cursor` and the number of its hits already
/// returned, so the next page starts at hit `skip` of that record. Encoded as `"cursor.skip"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Resume {
    pub cursor: u64,
    pub skip: usize,
}

impl Resume {
    pub(crate) fn encode(self) -> String {
        format!("{}.{}", self.cursor, self.skip)
    }

    pub(crate) fn parse(token: &str) -> Option<Resume> {
        let (cursor, skip) = token.split_once('.')?;
        Some(Resume {
            cursor: cursor.parse().ok()?,
            skip: skip.parse().ok()?,
        })
    }
}

/// A hit found in one event: the event's cursor and time, the field, and the snippet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Found {
    pub cursor: u64,
    pub at: u64,
    pub field: Field,
    pub snippet: Snippet,
}

/// The result of one scan: the hits in scan order, the token that continues the scan (`None`
/// when the scan reached the end of the records), and the bytes of records it examined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanPage {
    pub hits: Vec<Found>,
    pub next: Option<Resume>,
    pub scanned_bytes: u64,
}

/// The hits of `query` in one event, in the order of the event's searchable pieces. An event
/// outside the query's scope has none.
fn matches_in(event: &SessionEvent, query: &Query) -> Vec<Found> {
    if !query.scope.admits(event) {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (field, text) in searchable(event) {
        if query.kinds.as_ref().is_some_and(|k| !k.contains(&field)) {
            continue;
        }
        for snippet in hits_in(&text, &query.text) {
            out.push(Found {
                cursor: event.cursor,
                at: event.t,
                field,
                snippet,
            });
        }
    }
    out
}

/// Scans `records` (each with the bytes it occupies in its log) for `query`, starting with the
/// first record given. Stops when `limit` hits are held, or before a record once
/// [`SCAN_BUDGET`] bytes were examined; the token then names the first hit not returned. When
/// `resume` is given, its record's first `skip` hits were returned by an earlier page and are
/// not returned again. A page holds at most `limit` hits.
pub(crate) fn run(
    records: impl Iterator<Item = io::Result<(SessionEvent, u64)>>,
    query: &Query,
    limit: usize,
    resume: Option<Resume>,
) -> io::Result<ScanPage> {
    let mut records = records;
    let mut hits = Vec::new();
    let mut scanned = 0u64;
    // Where the last examined record leaves the scan: every one of its hits is returned.
    let mut last: Option<Resume> = None;
    loop {
        if hits.len() >= limit || scanned >= SCAN_BUDGET {
            return Ok(ScanPage {
                hits,
                next: last,
                scanned_bytes: scanned,
            });
        }
        let Some(item) = records.next() else {
            return Ok(ScanPage {
                hits,
                next: None,
                scanned_bytes: scanned,
            });
        };
        let (event, bytes) = item?;
        scanned += bytes;
        let cursor = event.cursor;
        let found = matches_in(&event, query);
        let skip = resume.filter(|r| r.cursor == cursor).map_or(0, |r| r.skip);
        let total = found.len();
        for (i, hit) in found.into_iter().enumerate().skip(skip) {
            if hits.len() >= limit {
                return Ok(ScanPage {
                    hits,
                    next: Some(Resume { cursor, skip: i }),
                    scanned_bytes: scanned,
                });
            }
            hits.push(hit);
        }
        last = Some(Resume {
            cursor,
            skip: total,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use wasit_core::Scope;

    use crate::wire::NoticeCode;

    /// `tests/fixtures/searchable.json`: `{"cases": [{"name", "event", "expect"}]}` where `event`
    /// is a [`SessionEvent`] and `expect` the `(field, text)` pieces [`searchable`] must list.
    const FIXTURE: &str = include_str!("../tests/fixtures/searchable.json");

    #[derive(Deserialize)]
    struct Fixture {
        cases: Vec<Case>,
    }

    #[derive(Deserialize)]
    struct Case {
        name: String,
        event: SessionEvent,
        expect: Vec<Piece>,
    }

    #[derive(Deserialize, Debug, PartialEq)]
    struct Piece {
        field: String,
        text: String,
    }

    #[test]
    fn fixture_pieces_match_searchable() {
        let fixture: Fixture = serde_json::from_str(FIXTURE).expect("fixture parses");
        assert!(!fixture.cases.is_empty());
        for case in &fixture.cases {
            let actual: Vec<Piece> = searchable(&case.event)
                .into_iter()
                .map(|(field, text)| Piece {
                    field: field.as_str().to_owned(),
                    text,
                })
                .collect();
            let expect: Vec<Piece> = case
                .expect
                .iter()
                .map(|p| Piece {
                    field: p.field.clone(),
                    text: p.text.clone(),
                })
                .collect();
            assert_eq!(actual, expect, "case {}", case.name);
        }
    }

    fn spans(text: &str, query: &str) -> Vec<(usize, usize)> {
        find_all(text, query)
            .into_iter()
            .map(|s| (s.start, s.end))
            .collect()
    }

    #[test]
    fn match_is_case_insensitive_with_char_offsets() {
        // `É` is one character: offsets count characters, not bytes.
        assert_eq!(spans("Un café CAFÉ", "café"), [(3, 7), (8, 12)]);
        assert_eq!(
            spans("Hello HELLO hello", "hello"),
            [(0, 5), (6, 11), (12, 17)]
        );
    }

    #[test]
    fn matches_do_not_overlap_and_empty_query_matches_nothing() {
        assert_eq!(spans("aaaa", "aa"), [(0, 2), (2, 4)]);
        assert_eq!(spans("abc", ""), []);
        assert_eq!(spans("abc", "abcd"), []);
    }

    #[test]
    fn snippet_keeps_offsets_and_shows_control_characters_as_spaces() {
        let text = "first line\nthe needle here\tend";
        let snip = hits_in(text, "NEEDLE").remove(0);
        // The needle is at characters 15..21 and the context is wide enough for the whole line.
        assert_eq!(snip.text, "first line the needle here end");
        assert_eq!((snip.start, snip.end), (15, 21));
        let chars: Vec<char> = snip.text.chars().collect();
        let hit: String = chars[snip.start..snip.end].iter().collect();
        assert_eq!(hit, "needle");
    }

    #[test]
    fn snippet_offsets_count_characters_after_multibyte_text() {
        // Bytes and characters differ before the match: `Ä`, `Ö`, `Ü` are two bytes each.
        let text = "ÄÖÜ CAFÉ ünd";
        let snip = hits_in(text, "café").remove(0);
        assert_eq!(snip.text, "ÄÖÜ CAFÉ ünd");
        assert_eq!((snip.start, snip.end), (4, 8));
        let chars: Vec<char> = snip.text.chars().collect();
        let hit: String = chars[snip.start..snip.end].iter().collect();
        assert_eq!(hit, "CAFÉ");
    }

    #[test]
    fn snippet_clips_to_the_text() {
        let snips = hits_in("abc", "abc");
        assert_eq!(
            snips,
            [Snippet {
                text: "abc".into(),
                start: 0,
                end: 3
            }]
        );
    }

    #[test]
    fn json_labels_are_not_searched() {
        let event = SessionEvent {
            cursor: 1,
            t: 1,
            kind: SessionEventKind::Agent {
                event: AgentEvent::ToolCallFinished {
                    call_id: "c".into(),
                    name: "Read".into(),
                    ok: true,
                    result: Some(serde_json::json!({"type": "text", "text": "body"})),
                    parts: vec![],
                },
            },
        };
        let pieces = searchable(&event);
        assert_eq!(pieces, [(Field::ToolResult, "body".to_owned())]);
    }

    fn notice(cursor: u64, message: &str) -> SessionEvent {
        SessionEvent {
            cursor,
            t: cursor * 10,
            kind: SessionEventKind::Notice {
                message: message.into(),
                code: NoticeCode::default(),
            },
        }
    }

    fn sub_text(cursor: u64, call_id: &str, text: &str) -> SessionEvent {
        SessionEvent {
            cursor,
            t: cursor * 10,
            kind: SessionEventKind::Agent {
                event: AgentEvent::Scoped {
                    scope: Scope {
                        call_id: call_id.into(),
                        agent_id: None,
                    },
                    event: Box::new(AgentEvent::AssistantDelta {
                        text: text.into(),
                        message_id: None,
                    }),
                },
            },
        }
    }

    fn query(text: &str) -> Query {
        Query::new(text, HistoryScope::All, None).unwrap()
    }

    fn sample() -> Vec<SessionEvent> {
        vec![
            notice(1, "needle one"),
            notice(2, "nothing here"),
            notice(3, "needle two and needle three"),
            sub_text(4, "c1", "needle in the subagent"),
            notice(5, "NEEDLE five"),
            notice(6, "needle six"),
            notice(7, "plain"),
        ]
    }

    /// The records of `events` from `resume` (or the start/end when none) in the direction.
    fn source(
        events: &[SessionEvent],
        backward: bool,
        resume: Option<Resume>,
    ) -> Box<dyn Iterator<Item = io::Result<(SessionEvent, u64)>>> {
        let from = resume.map(|r| r.cursor);
        let picked: Vec<SessionEvent> = if backward {
            events
                .iter()
                .rev()
                .filter(|e| from.is_none_or(|c| e.cursor <= c))
                .cloned()
                .collect()
        } else {
            events
                .iter()
                .filter(|e| from.is_none_or(|c| e.cursor >= c))
                .cloned()
                .collect()
        };
        Box::new(picked.into_iter().map(|e| Ok((e, 10))))
    }

    fn all_hits(page: &ScanPage) -> Vec<(u64, Field, usize)> {
        page.hits
            .iter()
            .map(|h| (h.cursor, h.field, h.snippet.start))
            .collect()
    }

    #[test]
    fn pages_resumed_by_token_repeat_nothing_and_skip_nothing() {
        let events = sample();
        for backward in [false, true] {
            let whole = run(
                source(&events, backward, None),
                &query("needle"),
                1000,
                None,
            )
            .unwrap();
            assert!(whole.next.is_none());
            assert_eq!(whole.hits.len(), 6, "backward {backward}");
            for limit in 1..=7 {
                let mut got = Vec::new();
                let mut resume = None;
                let mut pages = 0;
                loop {
                    pages += 1;
                    assert!(pages < 100, "the walk does not end");
                    let page = run(
                        source(&events, backward, resume),
                        &query("needle"),
                        limit,
                        resume,
                    )
                    .unwrap();
                    assert!(page.hits.len() <= limit);
                    got.extend(all_hits(&page));
                    match page.next {
                        Some(next) => resume = Some(next),
                        None => break,
                    }
                }
                assert_eq!(got, all_hits(&whole), "limit {limit} backward {backward}");
            }
        }
    }

    #[test]
    fn the_budget_ends_a_page_with_a_token() {
        // 20 records of 1 MiB; the hit is in the last one examined only by the second page.
        let mut events: Vec<SessionEvent> = (1..=20).map(|c| notice(c, "filler")).collect();
        events[19] = notice(20, "needle");
        let big = |records: Vec<SessionEvent>| -> Box<dyn Iterator<Item = io::Result<(SessionEvent, u64)>>> {
            Box::new(records.into_iter().map(|e| Ok((e, 1 << 20))))
        };
        let first = run(big(events.clone()), &query("needle"), 50, None).unwrap();
        assert!(first.hits.is_empty());
        assert_eq!(first.scanned_bytes, SCAN_BUDGET);
        let next = first.next.expect("the budget leaves a token");
        assert_eq!(next.cursor, 8, "the last examined record");
        // Each page examines 8 records of 1 MiB: the hit in record 20 comes on the third page
        // overall, the second one of this loop.
        let mut resume = Some(next);
        let mut found = Vec::new();
        let mut pages = 0;
        while let Some(token) = resume {
            pages += 1;
            assert!(pages < 10, "the walk does not end");
            let rest: Vec<SessionEvent> = events
                .iter()
                .filter(|e| e.cursor >= token.cursor)
                .cloned()
                .collect();
            let page = run(big(rest), &query("needle"), 50, Some(token)).unwrap();
            assert!(page.scanned_bytes <= SCAN_BUDGET + (1 << 20));
            found.extend(all_hits(&page));
            resume = page.next;
        }
        assert_eq!(pages, 2);
        assert_eq!(found, [(20, Field::Notice, 0)]);
    }

    #[test]
    fn scope_and_kinds_filter_the_hits() {
        let events = sample();
        let main = Query::new("needle", HistoryScope::Main, None).unwrap();
        let sub = Query::new(
            "needle",
            HistoryScope::Subagent {
                call_id: "c1".into(),
            },
            None,
        )
        .unwrap();
        let in_main = run(source(&events, false, None), &main, 50, None).unwrap();
        assert!(in_main.hits.iter().all(|h| h.cursor != 4));
        assert_eq!(in_main.hits.len(), 5);
        let in_sub = run(source(&events, false, None), &sub, 50, None).unwrap();
        assert_eq!(all_hits(&in_sub), [(4, Field::AssistantText, 0)]);
        let kinds = vec!["thinking".to_owned()];
        let none = Query::new("needle", HistoryScope::All, Some(&kinds)).unwrap();
        let page = run(source(&events, false, None), &none, 50, None).unwrap();
        assert!(page.hits.is_empty());
        assert!(Query::new("x", HistoryScope::All, Some(&["nope".to_owned()])).is_err());
        assert!(Query::new("", HistoryScope::All, None).is_err());
    }

    #[test]
    fn stale_token_skips_only_its_own_record() {
        let events = sample();
        let stale = Resume {
            cursor: 99,
            skip: 4,
        };
        let page = run(
            source(&events, false, None),
            &query("needle"),
            50,
            Some(stale),
        )
        .unwrap();
        assert_eq!(page.hits.len(), 6);
    }

    #[test]
    fn token_round_trips() {
        let r = Resume {
            cursor: 1_234_567,
            skip: 3,
        };
        assert_eq!(Resume::parse(&r.encode()), Some(r));
        assert_eq!(Resume::parse("x.1"), None);
        assert_eq!(Resume::parse("12"), None);
    }
}
