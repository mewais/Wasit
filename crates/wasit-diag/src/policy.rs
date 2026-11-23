//! The two policy points that wait for the product owner's decision (design §16.1 and §16.3).
//!
//! Each policy is exactly one function. Call sites and event declarations never see the
//! decision: they construct a [`ProjectPath`](crate::ProjectPath) or call
//! [`ErrorInfo::foreign`](crate::ErrorInfo::foreign), and what reaches a log line is decided here.
//! Changing a policy is a change of one function body (plus its tests); nothing else moves.

use crate::text::DiagText;
use crate::value::{Fp, PathTag};

/// What a project path (a session's cwd, a launch project, a file a tool touched) becomes in
/// diagnostics, both as a field value and where a registered project path is found inside a
/// free text by the scrubber.
///
/// **POLICY POINT 1** (design §16.1, "Project paths"). Current behaviour: hashed, as a
/// [`PathTag`] `{kind, depth, hash}` — no names, equal paths give equal tags on every machine.
/// The alternative under discussion is the path in clear.
pub(crate) fn project_path(raw: &str) -> PathTag {
    PathTag::of(raw)
}

/// What is kept of a text that came from outside our code (a CLI's error message, a JSON-RPC
/// peer's error, anything a user may have typed).
///
/// **POLICY POINT 2** (design §16.3, "Agent CLI error text"). Current behaviour: no text, only
/// the length in characters and a fingerprint ([`Fp`]). The alternative under discussion keeps a
/// scrubbed text behind an export option.
pub(crate) fn foreign_text(text: &str) -> Foreign {
    Foreign {
        text: None,
        len: Some(u32::try_from(text.chars().count()).unwrap_or(u32::MAX)),
        fp: Some(Fp::of(text)),
    }
}

/// The parts of a foreign text that [`foreign_text`] lets through.
pub(crate) struct Foreign {
    pub(crate) text: Option<DiagText>,
    pub(crate) len: Option<u32>,
    pub(crate) fp: Option<Fp>,
}
