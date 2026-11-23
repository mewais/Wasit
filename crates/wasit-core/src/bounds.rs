//! The bounds of what discovery reports.
//!
//! An inventory is one event on a topic (and one reply), and a frame has a size limit. What the
//! inventory holds comes from the user's own data (a conversation's first prompt, a skill's
//! description, a server's arguments), which no limit of the protocol constrains. So every
//! free-text field that comes from such data has a stated maximum here, in characters, and every
//! list that can grow with the data has a maximum number of items. Text over its maximum is cut
//! on a character boundary and ends with [`CUT`]; items over their list's maximum are left out
//! and counted ([`NotListed`]), so a client can say so instead of showing a shorter list as if
//! it were the whole.
//!
//! A driver applies the text limits where it produces the text (a title is cut as it is read
//! from the transcript, not after a multi-megabyte prompt was copied around); the runtime
//! applies [`Discovered::bounded`] to whatever any probe returns, so a driver that forgot one
//! cannot put an unbounded field on the wire.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigItem, McpTransport};
use crate::discovery::{Discovered, ResumableSession};
use crate::event::{ModelInfo, SessionMode};

/// What ends a text that was cut.
pub const CUT: char = '…';

/// The maxima, in characters (text) and items (lists).
pub mod limits {
    /// A conversation's title or first prompt.
    pub const TITLE_CHARS: usize = 240;
    /// A native session id.
    pub const ID_CHARS: usize = 128;
    /// A folder or file path.
    pub const PATH_CHARS: usize = 1024;
    /// The name of an item (a skill, a server, a model, a mode).
    pub const NAME_CHARS: usize = 200;
    /// A description.
    pub const DESCRIPTION_CHARS: usize = 1000;
    /// A scope word.
    pub const SCOPE_CHARS: usize = 64;
    /// A version, as the CLI prints it.
    pub const VERSION_CHARS: usize = 64;
    /// Why a driver or one of its probes failed.
    pub const ERROR_CHARS: usize = 500;
    /// An MCP server's command or URL.
    pub const MCP_TARGET_CHARS: usize = 2048;
    /// One argument, environment value or header value of an MCP server.
    pub const MCP_VALUE_CHARS: usize = 1024;
    /// The name of an environment variable or header of an MCP server.
    pub const MCP_KEY_CHARS: usize = 256;
    /// Arguments, environment variables or headers of one MCP server.
    pub const MCP_ENTRIES: usize = 64;

    /// Resumable sessions of one driver (the drivers list the newest first).
    pub const SESSIONS: usize = 200;
    /// Configured items of one driver.
    pub const CONFIG_ITEMS: usize = 1000;
    /// Models of one driver.
    pub const MODELS: usize = 500;
    /// Modes of one driver.
    pub const MODES: usize = 64;
}

/// `s` cut to at most `max` characters, the last of them [`CUT`] when anything was cut.
pub fn clip(s: &str, max: usize) -> String {
    let mut out = s.to_string();
    clip_in_place(&mut out, max);
    out
}

/// [`clip`] on a string already owned (no copy of what is cut off).
pub fn clip_in_place(s: &mut String, max: usize) {
    // A string of `max` bytes or fewer has at most `max` characters.
    if s.len() <= max {
        return;
    }
    // The byte offset of the first character that does not fit; none = it fits as it is.
    let Some((over, _)) = s.char_indices().nth(max) else {
        return;
    };
    let keep = s[..over]
        .char_indices()
        .nth(max.saturating_sub(1))
        .map(|(i, _)| i)
        .unwrap_or(0);
    s.truncate(keep);
    if max > 0 {
        s.push(CUT);
    }
}

/// What a list left out because it is longer than its maximum (or than what fits in the
/// inventory's size budget): how many items of each list are not listed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct NotListed {
    /// Resumable sessions (the oldest are the ones left out).
    #[serde(default)]
    pub sessions: u64,
    /// Configured items.
    #[serde(default)]
    pub config: u64,
    #[serde(default)]
    pub models: u64,
    #[serde(default)]
    pub modes: u64,
}

impl NotListed {
    /// Nothing was left out.
    pub fn is_empty(&self) -> bool {
        *self == NotListed::default()
    }

    /// The counts of `other` on top of these.
    pub fn add(&mut self, other: NotListed) {
        self.sessions += other.sessions;
        self.config += other.config;
        self.models += other.models;
        self.modes += other.modes;
    }
}

fn cap<T>(list: &mut Vec<T>, max: usize) -> u64 {
    let over = list.len().saturating_sub(max);
    list.truncate(max);
    over as u64
}

/// A session with its text within [`limits`].
pub fn bound_session(s: &mut ResumableSession) {
    clip_in_place(&mut s.id, limits::ID_CHARS);
    if let Some(p) = &mut s.project {
        clip_in_place(p, limits::PATH_CHARS);
    }
    clip_in_place(&mut s.title, limits::TITLE_CHARS);
}

/// A model with its text within [`limits`].
pub fn bound_model(m: &mut ModelInfo) {
    clip_in_place(&mut m.id, limits::NAME_CHARS);
    clip_in_place(&mut m.display_name, limits::NAME_CHARS);
    clip_in_place(&mut m.description, limits::DESCRIPTION_CHARS);
}

/// A mode with its text within [`limits`].
pub fn bound_mode(m: &mut SessionMode) {
    clip_in_place(&mut m.id, limits::NAME_CHARS);
    clip_in_place(&mut m.label, limits::NAME_CHARS);
    clip_in_place(&mut m.description, limits::DESCRIPTION_CHARS);
}

/// A configured item with its text within [`limits`].
pub fn bound_config_item(c: &mut ConfigItem) {
    clip_in_place(&mut c.name, limits::NAME_CHARS);
    clip_in_place(&mut c.description, limits::DESCRIPTION_CHARS);
    clip_in_place(&mut c.source.scope, limits::SCOPE_CHARS);
    for p in [&mut c.source.path, &mut c.path].into_iter().flatten() {
        clip_in_place(p, limits::PATH_CHARS);
    }
    match &mut c.mcp {
        Some(McpTransport::Stdio { command, args, env }) => {
            clip_in_place(command, limits::MCP_TARGET_CHARS);
            let over = args.len().saturating_sub(limits::MCP_ENTRIES);
            if over > 0 {
                args.truncate(limits::MCP_ENTRIES - 1);
                args.push(format!("{CUT} {} more arguments not listed", over + 1));
            }
            for a in args {
                clip_in_place(a, limits::MCP_VALUE_CHARS);
            }
            bound_map(env);
        }
        Some(McpTransport::Http { url, headers }) => {
            clip_in_place(url, limits::MCP_TARGET_CHARS);
            bound_map(headers);
        }
        None => {}
    }
}

/// The environment variables or headers of a server: at most [`limits::MCP_ENTRIES`], the last
/// being a note of how many more there were.
fn bound_map(m: &mut BTreeMap<String, String>) {
    let over = m.len().saturating_sub(limits::MCP_ENTRIES);
    let mut out = BTreeMap::new();
    for (mut k, mut v) in std::mem::take(m)
        .into_iter()
        .take(limits::MCP_ENTRIES - usize::from(over > 0))
    {
        clip_in_place(&mut k, limits::MCP_KEY_CHARS);
        clip_in_place(&mut v, limits::MCP_VALUE_CHARS);
        out.insert(k, v);
    }
    if over > 0 {
        out.insert(CUT.to_string(), format!("{} more not listed", over + 1));
    }
    *m = out;
}

impl Discovered {
    /// This result within [`limits`]: text cut, lists capped, and what the lists left out
    /// counted.
    pub fn bounded(self) -> (Discovered, NotListed) {
        let mut cut = NotListed::default();
        let d = match self {
            Discovered::Version(mut v) => {
                clip_in_place(&mut v, limits::VERSION_CHARS);
                Discovered::Version(v)
            }
            Discovered::Models(mut l) => {
                cut.models = cap(&mut l, limits::MODELS);
                l.iter_mut().for_each(bound_model);
                Discovered::Models(l)
            }
            Discovered::Modes(mut l) => {
                cut.modes = cap(&mut l, limits::MODES);
                l.iter_mut().for_each(bound_mode);
                Discovered::Modes(l)
            }
            Discovered::Sessions(mut l) => {
                cut.sessions = cap(&mut l, limits::SESSIONS);
                l.iter_mut().for_each(bound_session);
                Discovered::Sessions(l)
            }
            Discovered::Config(mut l) => {
                cut.config = cap(&mut l, limits::CONFIG_ITEMS);
                l.iter_mut().for_each(bound_config_item);
                Discovered::Config(l)
            }
        };
        (d, cut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clip_cuts_on_a_character_boundary_and_marks_the_cut() {
        assert_eq!(clip("abc", 3), "abc");
        assert_eq!(clip("abcd", 3), "ab…");
        assert_eq!(clip("", 3), "");
        // Two-, three- and four-byte characters are never split.
        let s = "é€𝄞é€𝄞é€𝄞";
        for max in 1..=s.chars().count() {
            let c = clip(s, max);
            assert!(c.chars().count() <= max, "{max}");
            if max < s.chars().count() {
                assert!(c.ends_with(CUT) && c.chars().count() == max, "{max}: {c}");
            } else {
                assert_eq!(c, s);
            }
        }
        // A string of at most `max` characters is untouched however many bytes it has.
        assert_eq!(clip("𝄞𝄞𝄞", 3), "𝄞𝄞𝄞");
        assert_eq!(clip("x", 0), "");
    }

    fn session(i: usize, title: String) -> ResumableSession {
        ResumableSession {
            id: format!("id-{i}"),
            project: Some("/p".into()),
            title,
            modified_ms: i as u64,
            message_count: None,
        }
    }

    /// A list over its maximum keeps the first items (the drivers list the newest first) and
    /// counts the rest; text over its maximum is cut with the marker; what fits is untouched.
    #[test]
    fn a_result_is_bounded_and_what_was_left_out_is_counted() {
        let sessions: Vec<_> = (0..limits::SESSIONS + 7)
            .map(|i| session(i, "t".repeat(limits::TITLE_CHARS + 1)))
            .collect();
        let (d, cut) = Discovered::Sessions(sessions).bounded();
        let Discovered::Sessions(s) = d else { panic!() };
        assert_eq!(s.len(), limits::SESSIONS);
        assert_eq!(s[0].id, "id-0");
        assert!(s
            .iter()
            .all(|x| x.title.chars().count() == limits::TITLE_CHARS && x.title.ends_with(CUT)));
        assert_eq!(
            cut,
            NotListed {
                sessions: 7,
                ..NotListed::default()
            }
        );

        let (d, cut) = Discovered::Sessions(vec![session(1, "fits".into())]).bounded();
        assert!(cut.is_empty());
        assert_eq!(d, Discovered::Sessions(vec![session(1, "fits".into())]));

        let models: Vec<_> = (0..limits::MODELS + 1)
            .map(|i| ModelInfo {
                id: i.to_string(),
                display_name: String::new(),
                description: "d".repeat(limits::DESCRIPTION_CHARS + 1),
                is_default: false,
            })
            .collect();
        let (d, cut) = Discovered::Models(models).bounded();
        let Discovered::Models(m) = d else { panic!() };
        assert_eq!((m.len(), cut.models), (limits::MODELS, 1));
        assert!(m[0].description.ends_with(CUT));

        let (d, _) = Discovered::Version("1".repeat(1000)).bounded();
        assert!(matches!(d, Discovered::Version(v) if v.chars().count() == limits::VERSION_CHARS));
    }

    /// A configured item's text is bounded wherever it is: description, paths, an MCP server's
    /// command, arguments (the last one says how many more there were), environment.
    #[test]
    fn a_configured_item_is_bounded_in_every_field() {
        use crate::config::{ConfigItemKind, ConfigSource};
        let big = "x".repeat(100_000);
        let item = ConfigItem {
            kind: ConfigItemKind::McpServer,
            name: big.clone(),
            source: ConfigSource {
                scope: big.clone(),
                path: Some(big.clone()),
            },
            description: big.clone(),
            enabled: true,
            mcp: Some(McpTransport::Stdio {
                command: big.clone(),
                args: (0..limits::MCP_ENTRIES + 10).map(|_| big.clone()).collect(),
                env: (0..limits::MCP_ENTRIES + 10)
                    .map(|i| (format!("K{i:04}"), big.clone()))
                    .collect(),
            }),
            path: Some(big.clone()),
        };
        let (d, cut) = Discovered::Config(vec![item.clone(), item]).bounded();
        assert!(cut.is_empty());
        let Discovered::Config(c) = d else { panic!() };
        let c = &c[0];
        assert_eq!(c.name.chars().count(), limits::NAME_CHARS);
        assert_eq!(c.description.chars().count(), limits::DESCRIPTION_CHARS);
        assert_eq!(c.source.scope.chars().count(), limits::SCOPE_CHARS);
        assert_eq!(
            c.path.as_deref().unwrap().chars().count(),
            limits::PATH_CHARS
        );
        let Some(McpTransport::Stdio { command, args, env }) = &c.mcp else {
            panic!()
        };
        assert_eq!(command.chars().count(), limits::MCP_TARGET_CHARS);
        assert_eq!(
            (args.len(), env.len()),
            (limits::MCP_ENTRIES, limits::MCP_ENTRIES)
        );
        assert!(args[..limits::MCP_ENTRIES - 1]
            .iter()
            .all(|a| a.chars().count() == limits::MCP_VALUE_CHARS));
        assert_eq!(
            args.last().unwrap(),
            &format!("{CUT} 11 more arguments not listed")
        );
        assert_eq!(env.get("…").map(String::as_str), Some("11 more not listed"));
    }

    /// The text of a skill or command read from its front matter is bounded where it is made.
    #[test]
    fn front_matter_text_is_bounded_where_it_is_read() {
        use crate::config::ConfigItemKind;
        use crate::discovery::{markdown_items, skills_from_listing, FileHead};
        let big = "d".repeat(50_000);
        let file = |path: &str| FileHead {
            path: path.into(),
            modified_ms: 0,
            head: vec![
                "---".into(),
                format!("name: {big}"),
                format!("description: {big}"),
                "---".into(),
            ],
            matching_lines: None,
        };
        let skills = skills_from_listing(&[file("/s/a/SKILL.md")], "/s", |_| "user".into());
        let cmds = markdown_items(&[file("/c/x.md")], ConfigItemKind::Command, "user");
        for i in skills.iter().chain(&cmds) {
            assert_eq!(i.name.chars().count(), limits::NAME_CHARS);
            assert_eq!(i.description.chars().count(), limits::DESCRIPTION_CHARS);
        }
    }
}
