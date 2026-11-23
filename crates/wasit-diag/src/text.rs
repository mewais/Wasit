//! Free text that is not session content: [`DiagText`] and its one constructor (design §4.3).

use std::fmt::Write as _;

use crate::json::Json;
use crate::scrub;
use crate::value::{DiagValue, Sealed};

/// Where a free text comes from. It decides the length bound and, for [`Origin::HostShell`],
/// that only the first line is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// `Display` of our own error types.
    Own,
    /// ssh / wsl / bridge stderr and the bootstrap preamble: kept up to
    /// [`DiagText::MAX_TRANSPORT`] characters, since it is the evidence for "the host would not
    /// connect".
    Transport,
    /// Environment capture notes, setup errors (never setup output): first line only.
    HostShell,
    /// Probe errors of discovery.
    Discovery,
    /// The OS release string.
    System,
    /// A text fixed in the source.
    Static,
}

/// Bounded, scrubbed free text. The only constructor is [`DiagText::new`], whose call sites are
/// pinned by a source-scan test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagText(String);

impl DiagText {
    /// Characters kept of a text of every origin but [`Origin::Transport`]: head 200, `…`,
    /// tail 99.
    pub const MAX: usize = 300;
    /// Characters kept of an [`Origin::Transport`] text: head 200, `…`, tail 1847.
    pub const MAX_TRANSPORT: usize = 2048;
    /// Runs of token characters at least this long, with letters and digits, are redacted.
    pub const TOKEN_MIN: usize = 24;

    /// The one way to make a free text a field value. In this order:
    /// 1. every value registered with the scrubber is replaced: a secret by `••••`, a project
    ///    path by its policy text form (`<p:1a2b3c4d5e>`);
    /// 2. the home directory prefix is replaced by `~`;
    /// 3. token-shaped runs (≥ 24 characters of `[A-Za-z0-9+/=_-]` with both letters and
    ///    digits) become `<redacted:N>`;
    /// 4. control characters are escaped (a line break shows as `⏎`) and the text is cut to its
    ///    bound (head 200 + `…` + the tail).
    ///
    /// [`Origin::HostShell`] texts are first reduced to their first line.
    pub fn new(origin: Origin, s: &str) -> DiagText {
        let s = match origin {
            Origin::HostShell => s.lines().next().unwrap_or(""),
            _ => s,
        };
        let snap = scrub::snapshot();
        let mut t = s.to_owned();
        for (pattern, replacement) in &snap.patterns {
            if t.contains(&**pattern) {
                t = t.replace(&**pattern, replacement);
            }
        }
        for h in &snap.home {
            t = replace_home(&t, h);
        }
        let t = redact_tokens(&t);
        let max = match origin {
            Origin::Transport => Self::MAX_TRANSPORT,
            _ => Self::MAX,
        };
        DiagText(cut(&escape_controls(&t), max))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A text read back from the wire: it was scrubbed and bounded where it was made, so it is
    /// kept as it is (see [`crate::wire`]).
    pub(crate) fn from_wire(s: String) -> DiagText {
        DiagText(s)
    }
}

impl Sealed for DiagText {}
impl DiagValue for DiagText {
    fn __write(&self, out: &mut Json) {
        out.str(&self.0)
    }
}

/// `s` with the home directory prefix written as `~`.
pub(crate) fn home_relative(s: &str) -> String {
    match scrub::home() {
        Some(h) => replace_home(s, &h),
        None => s.to_owned(),
    }
}

/// Every occurrence of `home` that ends at a path boundary becomes `~`.
fn replace_home(s: &str, home: &str) -> String {
    if home.is_empty() || !s.contains(home) {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(home) {
        let after = &rest[i + home.len()..];
        let boundary = after
            .chars()
            .next()
            .is_none_or(|c| !(c.is_alphanumeric() || matches!(c, '_' | '-' | '.')));
        out.push_str(&rest[..i]);
        out.push_str(if boundary { "~" } else { home });
        rest = after;
    }
    out.push_str(rest);
    out
}

fn token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=' | '_' | '-')
}

fn redact_tokens(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut run = String::new();
    let flush = |run: &mut String, out: &mut String| {
        let n = run.chars().count();
        let letters = run.chars().any(|c| c.is_ascii_alphabetic());
        let digits = run.chars().any(|c| c.is_ascii_digit());
        if n >= DiagText::TOKEN_MIN && letters && digits {
            // Writing into a String cannot fail.
            let _ = write!(out, "<redacted:{n}>");
        } else {
            out.push_str(run);
        }
        run.clear();
    };
    for c in s.chars() {
        if token_char(c) {
            run.push(c);
        } else {
            flush(&mut run, &mut out);
            out.push(c);
        }
    }
    flush(&mut run, &mut out);
    out
}

/// Control characters escaped; `\r\n`, `\n` and `\r` shown as `⏎`.
pub(crate) fn escape_controls(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                out.push('⏎');
            }
            '\n' => out.push('⏎'),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                // Writing into a String cannot fail.
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// At most `max` characters: the head, `…`, the tail.
pub(crate) fn cut(s: &str, max: usize) -> String {
    let n = s.chars().count();
    if n <= max {
        return s.to_owned();
    }
    let head = (max * 2 / 3).min(200);
    let tail = max - head - 1;
    let mut out: String = s.chars().take(head).collect();
    out.push('…');
    out.extend(s.chars().skip(n - tail));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;

    /// The one home directory every test of this binary uses (the registry is process-global).
    pub(crate) const HOME: &str = "/home/diagtester";

    fn t(s: &str) -> String {
        DiagText::new(Origin::Own, s).as_str().to_owned()
    }

    #[test]
    fn registered_secret_is_masked() {
        assert!(crate::scrub::register_secret("hunter2-SECRET-one"));
        assert!(!crate::scrub::register_secret("abc"));
        assert_eq!(
            t("auth failed with hunter2-SECRET-one!"),
            "auth failed with ••••!"
        );
        // A secret inside a JSON document a tool printed.
        assert_eq!(t(r#"{"k":"hunter2-SECRET-one"}"#), r#"{"k":"••••"}"#);
    }

    #[test]
    fn secret_containing_another_is_replaced_whole() {
        crate::scrub::register_secrets(["inner-key", "outer-inner-key-x"]);
        assert_eq!(t("v=outer-inner-key-x;"), "v=••••;");
    }

    #[test]
    fn registered_project_path_becomes_its_tag() {
        crate::scrub::register_project_path("/srv/work/clientproj");
        let tag = crate::policy::project_path("/srv/work/clientproj").text_form();
        assert_eq!(
            t("cd /srv/work/clientproj/src: denied"),
            format!("cd {tag}/src: denied")
        );
        assert!(tag.starts_with("<p:") && tag.len() == 14, "{tag}");
        crate::scrub::register_project_path("C:\\Users\\Ann\\proj");
        let out = t(r#"{"cwd":"C:\\Users\\Ann\\proj"}"#);
        assert!(!out.contains("Ann"), "{out}");
    }

    #[test]
    fn home_prefix_becomes_tilde() {
        crate::scrub::set_home(HOME);
        assert_eq!(
            t("open /home/diagtester/.marsad/x failed"),
            "open ~/.marsad/x failed"
        );
        assert_eq!(t("/home/diagtesterz/x"), "/home/diagtesterz/x");
        assert_eq!(t("at /home/diagtester"), "at ~");
    }

    #[test]
    fn token_shaped_runs_are_redacted() {
        assert_eq!(
            t("key sk-ant-api03-AbCdEf0123456789xyz done"),
            "key <redacted:32> done"
        );
        // Long words without digits, and short mixed runs, stay.
        assert_eq!(
            t("unrecognised_configuration_option_name x1y2"),
            "unrecognised_configuration_option_name x1y2"
        );
    }

    #[test]
    fn control_characters_are_escaped() {
        assert_eq!(t("a\r\nb\nc\rd\te\u{1b}[31mf"), "a⏎b⏎c⏎d\\te\\x1b[31mf");
        assert_eq!(
            DiagText::new(Origin::HostShell, "first\nsecond").as_str(),
            "first"
        );
    }

    #[test]
    fn length_is_cut_head_and_tail() {
        let s: String = (0..1000)
            .map(|i| char::from(b'a' + (i % 26) as u8))
            .collect();
        let out = t(&s);
        assert_eq!(out.chars().count(), DiagText::MAX);
        assert!(out.starts_with(&s[..200]));
        assert!(out.ends_with(&s[1000 - 99..]));
        assert_eq!(out.chars().nth(200), Some('…'));
        let long = "x ".repeat(3000);
        let tr = DiagText::new(Origin::Transport, &long);
        assert_eq!(tr.as_str().chars().count(), DiagText::MAX_TRANSPORT);
    }

    /// xorshift64*: deterministic inputs without a dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    #[test]
    fn registered_marker_never_survives() {
        crate::scrub::set_home(HOME);
        let markers = ["MRK7q2Zp9", "mk\"quote\\back", "Zz9-marker_x"];
        crate::scrub::register_secrets(markers);
        crate::scrub::register_project_path("/data/MRKPROJ9x");
        let pieces = [
            " ",
            "\n",
            "\r\n",
            "\t",
            "\u{1b}",
            "\"",
            "\\",
            "/",
            "=",
            "-",
            "_",
            "é",
            "…",
            "x",
            "Q",
            "7",
            "abc",
            "/home/diagtester",
            "/home/diagtester/",
            "aaaaaaaaaaaaaaaaaaaa",
            "0123456789",
            "\u{0}",
            "MRK",
            "7q2",
            "{",
            "}",
        ];
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for round in 0..3000 {
            let mut input = String::new();
            let target = rng.below(markers.len() + 1);
            for _ in 0..rng.below(60) {
                input.push_str(pieces[rng.below(pieces.len())]);
                if rng.below(4) == 0 {
                    let m = if target == markers.len() {
                        "/data/MRKPROJ9x"
                    } else {
                        markers[target]
                    };
                    match rng.below(3) {
                        0 => input.push_str(m),
                        1 => input.push_str(&json::quoted(m)[1..json::quoted(m).len() - 1]),
                        _ => input.push_str(&format!("{m}{m}")),
                    }
                }
            }
            if rng.below(5) == 0 {
                input.push_str(&"y".repeat(rng.below(3000)));
            }
            let origin = [Origin::Own, Origin::Transport, Origin::HostShell][rng.below(3)];
            let out = DiagText::new(origin, &input);
            let mut j = Json::new();
            out.__write(&mut j);
            for m in markers
                .iter()
                .copied()
                .chain(["/data/MRKPROJ9x", "MRKPROJ9x"])
            {
                let esc = json::quoted(m);
                let esc = &esc[1..esc.len() - 1];
                for (form, text) in [("raw", out.as_str()), ("json", j.buf.as_str())] {
                    assert!(
                        !text.contains(m) && !text.contains(esc),
                        "round {round}: {m:?} survived ({form}) in {text:?} from {input:?}"
                    );
                }
            }
        }
    }
}
