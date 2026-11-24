//! Find over a session's events: the matcher only (B6a). Reading the journal and the index that
//! serves far hits are later steps (B6b).
//!
//! [`searchable`] lists the text one [`SessionEvent`] contributes to find, each piece tagged with
//! the [`Field`] it came from; [`find_all`] matches a query in one piece and [`snippet`] cuts the
//! text around a match. The app mirrors the rules of [`searchable`] in TypeScript, and the shared
//! fixture `tests/fixtures/searchable.json` pins both sides to the same events and pieces.

use wasit_core::{AgentEvent, Command, ContentPart};

use crate::wire::{SessionEvent, SessionEventKind};

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

impl Field {
    /// The name of the field in the shared fixture and in the app.
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
}

/// The text of `event` that find searches, in the order it appears in the event. Empty texts
/// are not listed. Covered: user prompts and user messages, assistant prose, reasoning, the
/// string values of tool inputs and of tool results (object keys are not text), and notices.
/// Events of the earlier history and of a subagent's conversation are searched as the events
/// they wrap. Anything else (phases, spawns, stderr, usage, …) has no find text.
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

/// Every string value inside `value`, depth first. Keys are not text.
fn json_strings(out: &mut Vec<(Field, String)>, field: Field, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(s) => push(out, field, s),
        serde_json::Value::Array(items) => {
            for item in items {
                json_strings(out, field, item);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
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

/// The characters of `text` within `radius` characters of `span`, clipped to the text.
pub fn snippet(text: &str, span: Span, radius: usize) -> Snippet {
    let chars: Vec<char> = text.chars().collect();
    let end = span.end.min(chars.len());
    let start = span.start.min(end);
    let from = start.saturating_sub(radius);
    let to = (end + radius).min(chars.len());
    let window: String = chars[from..to]
        .iter()
        .map(|&c| if c.is_control() { ' ' } else { c })
        .collect();
    Snippet {
        text: window,
        start: start - from,
        end: end - from,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

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
        let found = find_all(text, "NEEDLE")[0];
        // The match is at characters 15..21; six characters of context on each side, the line
        // break and the tab shown as spaces.
        let snip = snippet(text, found, 6);
        assert_eq!(snip.text, "e the needle here ");
        assert_eq!((snip.start, snip.end), (6, 12));
        let chars: Vec<char> = snip.text.chars().collect();
        let hit: String = chars[snip.start..snip.end].iter().collect();
        assert_eq!(hit, "needle");
    }

    #[test]
    fn snippet_clips_to_the_text() {
        let snip = snippet("abc", Span { start: 0, end: 3 }, 10);
        assert_eq!(
            snip,
            Snippet {
                text: "abc".into(),
                start: 0,
                end: 3
            }
        );
    }
}
