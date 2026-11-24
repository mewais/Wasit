//! Best-effort secret redaction for session content that leaves the runtime (the conversation
//! export, B5b).
//!
//! [`redact_json`] replaces secret-shaped substrings of every string value of a JSON value with
//! `<redacted:KIND>`; object keys are left as they are. Both functions are pure: the output
//! depends only on the input, never on the process's secret registry or environment, so an
//! export is reproducible from its events.
//!
//! The passes run in this order, each on the output of the one before:
//!
//! 1. PEM private-key blocks (their base64 body would otherwise look like a token run);
//! 2. provider key formats (OpenAI/Anthropic `sk-`, AWS `AKIA`, GitHub `ghp_`/`github_pat_`,
//!    Slack `xox[baprs]-`, Google `AIza`);
//! 3. `Bearer <token>`;
//! 4. URL credentials `://user:pass@`;
//! 5. credential assignments `password|secret|token|api[_-]?key` followed by `:`/`=` and a value;
//! 6. token-shaped runs (the rule of `wasit_diag`'s free-text redactor: at least
//!    [`TOKEN_MIN`] token characters with both letters and digits), except UUIDs and hex object
//!    ids of git (40 and 64 characters), which are identifiers and not secrets.
//!
//! It is best effort: a secret in a shape none of these recognise is left as it is.

use std::collections::BTreeMap;

use serde_json::Value;

/// Token-shaped runs at least this long are redacted (as in `wasit_diag::DiagText::TOKEN_MIN`).
const TOKEN_MIN: usize = 24;

/// A credential value shorter than this is not a secret worth hiding (`password: see` in prose).
const CREDENTIAL_MIN_CHARS: usize = 6;

/// What a redaction replaced. The text of the marker is `<redacted:` + [`RedactKind::as_str`] +
/// `>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RedactKind {
    PrivateKey,
    SkKey,
    AwsKey,
    GithubToken,
    SlackToken,
    GoogleKey,
    Bearer,
    UrlCredentials,
    Credential,
    Token,
}

impl RedactKind {
    /// The word inside the `<redacted:…>` marker.
    pub fn as_str(self) -> &'static str {
        match self {
            RedactKind::PrivateKey => "private_key",
            RedactKind::SkKey => "sk_key",
            RedactKind::AwsKey => "aws_key",
            RedactKind::GithubToken => "github_token",
            RedactKind::SlackToken => "slack_token",
            RedactKind::GoogleKey => "google_key",
            RedactKind::Bearer => "bearer",
            RedactKind::UrlCredentials => "url_credentials",
            RedactKind::Credential => "credential",
            RedactKind::Token => "token",
        }
    }
}

/// How many replacements of each kind a redaction made.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedactionCounts(BTreeMap<RedactKind, u64>);

impl RedactionCounts {
    /// Replacements of `kind`.
    pub fn get(&self, kind: RedactKind) -> u64 {
        self.0.get(&kind).copied().unwrap_or(0)
    }

    /// All replacements, of every kind.
    pub fn total(&self) -> u64 {
        self.0.values().sum()
    }

    /// `(kind, count)` for each kind that was replaced at least once, in [`RedactKind`] order.
    pub fn iter(&self) -> impl Iterator<Item = (RedactKind, u64)> + '_ {
        self.0.iter().map(|(k, n)| (*k, *n))
    }

    fn add(&mut self, kind: RedactKind) {
        *self.0.entry(kind).or_insert(0) += 1;
    }
}

/// `value` with every secret-shaped substring of its string values replaced, and the counts.
/// Object keys, numbers, booleans and nulls are kept as they are.
pub fn redact_json(value: &Value) -> (Value, RedactionCounts) {
    let mut counts = RedactionCounts::default();
    let out = walk(value, &mut counts);
    (out, counts)
}

/// `text` with every secret-shaped substring replaced, and the counts.
pub fn redact_text(text: &str) -> (String, RedactionCounts) {
    let mut counts = RedactionCounts::default();
    let out = redact_str(text, &mut counts);
    (out, counts)
}

fn walk(value: &Value, counts: &mut RedactionCounts) -> Value {
    match value {
        Value::String(s) => Value::String(redact_str(s, counts)),
        Value::Array(items) => Value::Array(items.iter().map(|v| walk(v, counts)).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), walk(v, counts)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn redact_str(text: &str, counts: &mut RedactionCounts) -> String {
    let passes: [fn(&str, usize) -> Option<Hit>; 6] = [
        private_key_at,
        provider_key_at,
        bearer_at,
        url_credentials_at,
        credential_at,
        token_at,
    ];
    passes
        .iter()
        .fold(text.to_owned(), |t, find| scan(&t, counts, *find))
}

/// One span to replace: `text[start..end]` becomes the marker of `kind`.
struct Hit {
    start: usize,
    end: usize,
    kind: RedactKind,
}

/// Replaces every hit of `find`, which is asked at each character position `i` and may return a
/// hit that starts at or after `i`. Every hit is non-empty and both ends are character
/// boundaries (the matchers only stop at ASCII bytes or at the end of the text).
fn scan(text: &str, counts: &mut RedactionCounts, find: fn(&str, usize) -> Option<Hit>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    let mut i = 0;
    while i < text.len() {
        match find(text, i) {
            Some(hit) => {
                debug_assert!(i <= hit.start && hit.start < hit.end);
                out.push_str(&text[copied..hit.start]);
                out.push_str("<redacted:");
                out.push_str(hit.kind.as_str());
                out.push('>');
                counts.add(hit.kind);
                copied = hit.end;
                i = hit.end;
            }
            None => i += text[i..].chars().next().map_or(1, char::len_utf8),
        }
    }
    out.push_str(&text[copied..]);
    out
}

/// `b[at..]` starts with `pat`.
fn at(b: &[u8], i: usize, pat: &[u8]) -> bool {
    b.get(i..i + pat.len()) == Some(pat)
}

/// `b[at..]` starts with `pat`, ASCII case-insensitively.
fn at_ci(b: &[u8], i: usize, pat: &[u8]) -> bool {
    b.get(i..i + pat.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(pat))
}

/// The length of the run of bytes that satisfy `keep` starting at `from`.
fn run_len(b: &[u8], from: usize, keep: impl Fn(u8) -> bool) -> usize {
    b.get(from..)
        .unwrap_or(&[])
        .iter()
        .take_while(|&&c| keep(c))
        .count()
}

/// `-----BEGIN <label> PRIVATE KEY-----` up to its `-----END …-----` (or to the end of the text
/// when the footer is missing: a truncated key is still a key).
fn private_key_at(text: &str, i: usize) -> Option<Hit> {
    const BEGIN: &str = "-----BEGIN ";
    const END: &str = "-----END ";
    let rest = text.get(i..)?.strip_prefix(BEGIN)?;
    let label_len = rest.find("-----")?;
    if !rest[..label_len].ends_with("PRIVATE KEY") {
        return None;
    }
    let body = i + BEGIN.len() + label_len + "-----".len();
    let end = match text[body..].find(END) {
        Some(p) => {
            let after = body + p + END.len();
            text[after..]
                .find("-----")
                .map_or(text.len(), |q| after + q + 5)
        }
        None => text.len(),
    };
    Some(Hit {
        start: i,
        end,
        kind: RedactKind::PrivateKey,
    })
}

/// A provider key of a known format, at a word start (the prefix must not follow a letter or
/// digit, so `task-manager-service-configuration` is not an `sk-` key).
fn provider_key_at(text: &str, i: usize) -> Option<Hit> {
    let b = text.as_bytes();
    if i > 0 && b[i - 1].is_ascii_alphanumeric() {
        return None;
    }
    let (kind, body_len) = if at(b, i, b"sk-") {
        let n = run_len(b, i + 3, |c| {
            c.is_ascii_alphanumeric() || c == b'_' || c == b'-'
        });
        (RedactKind::SkKey, (n >= 20).then_some(3 + n))
    } else if at(b, i, b"AKIA") {
        // AWS access key ids are exactly 20 characters: `AKIA` and 16 of `[0-9A-Z]`.
        let n = run_len(b, i + 4, |c| c.is_ascii_uppercase() || c.is_ascii_digit());
        (RedactKind::AwsKey, (n == 16).then_some(4 + n))
    } else if at(b, i, b"ghp_") {
        let n = run_len(b, i + 4, |c| c.is_ascii_alphanumeric());
        (RedactKind::GithubToken, (n >= 36).then_some(4 + n))
    } else if at(b, i, b"github_pat_") {
        let n = run_len(b, i + 11, |c| c.is_ascii_alphanumeric() || c == b'_');
        (RedactKind::GithubToken, (n >= 22).then_some(11 + n))
    } else if at(b, i, b"xoxb-")
        || at(b, i, b"xoxa-")
        || at(b, i, b"xoxp-")
        || at(b, i, b"xoxr-")
        || at(b, i, b"xoxs-")
    {
        let n = run_len(b, i + 5, |c| c.is_ascii_alphanumeric() || c == b'-');
        (RedactKind::SlackToken, (n >= 10).then_some(5 + n))
    } else if at(b, i, b"AIza") {
        let n = run_len(b, i + 4, |c| {
            c.is_ascii_alphanumeric() || c == b'_' || c == b'-'
        });
        (RedactKind::GoogleKey, (n >= 35).then_some(4 + n))
    } else {
        return None;
    };
    body_len.map(|len| Hit {
        start: i,
        end: i + len,
        kind,
    })
}

/// `Bearer <token>`: the token is replaced, the word `Bearer` kept. A token must be at least 16
/// characters and carry a digit or a dot (a JWT), so prose like `bearer authentication` is kept.
fn bearer_at(text: &str, i: usize) -> Option<Hit> {
    let b = text.as_bytes();
    if !at_ci(b, i, b"bearer") || (i > 0 && b[i - 1].is_ascii_alphanumeric()) {
        return None;
    }
    let mut j = i + "bearer".len();
    let spaces = run_len(b, j, |c| c == b' ' || c == b'\t');
    if spaces == 0 {
        return None;
    }
    j += spaces;
    let n = run_len(b, j, |c| {
        c.is_ascii_alphanumeric() || b"._~+/=-".contains(&c)
    });
    let token = &text[j..j + n];
    if n < 16 || !(token.bytes().any(|c| c.is_ascii_digit()) || token.contains('.')) {
        return None;
    }
    Some(Hit {
        start: j,
        end: j + n,
        kind: RedactKind::Bearer,
    })
}

/// The `user:pass` of `scheme://user:pass@host`, replaced; the scheme and host are kept. A
/// password is required, so `https://host:8443/a@b` is not a credential.
fn url_credentials_at(text: &str, i: usize) -> Option<Hit> {
    let b = text.as_bytes();
    if !at(b, i, b"://") {
        return None;
    }
    let user_start = i + 3;
    let userinfo = |c: u8| !c.is_ascii_whitespace() && !b"/@:\"'<>\\".contains(&c);
    let user_len = run_len(b, user_start, userinfo);
    let colon = user_start + user_len;
    if b.get(colon) != Some(&b':') {
        return None;
    }
    let pass_start = colon + 1;
    let pass_len = run_len(b, pass_start, |c| {
        !c.is_ascii_whitespace() && !b"/@\"'<>\\".contains(&c)
    });
    let at_sign = pass_start + pass_len;
    if pass_len == 0 || b.get(at_sign) != Some(&b'@') {
        return None;
    }
    Some(Hit {
        start: user_start,
        end: at_sign,
        kind: RedactKind::UrlCredentials,
    })
}

/// The keyword of a credential assignment at `i` (`password`, `secret`, `token`, `api_key`,
/// `api-key`, `apikey`), with its length.
fn credential_keyword_at(b: &[u8], i: usize) -> Option<usize> {
    let keywords: [&[u8]; 3] = [b"password", b"secret", b"token"];
    for kw in keywords {
        if at_ci(b, i, kw) {
            return Some(kw.len());
        }
    }
    if at_ci(b, i, b"api") {
        let sep = usize::from(matches!(b.get(i + 3), Some(b'_' | b'-')));
        if at_ci(b, i + 3 + sep, b"key") {
            return Some(3 + sep + 3);
        }
    }
    None
}

/// The value of `password|secret|token|api[_-]?key` followed by `:` or `=` (optionally with the
/// key quoted, as in JSON text, and the value quoted). `max_tokens=4096` and `tokens: 3` are
/// not keywords: the keyword must end right before the separator.
fn credential_at(text: &str, i: usize) -> Option<Hit> {
    let b = text.as_bytes();
    let kw_len = credential_keyword_at(b, i)?;
    let skip_spaces = |mut j: usize| {
        while matches!(b.get(j), Some(b' ' | b'\t')) {
            j += 1;
        }
        j
    };
    let is_quote = |c: Option<&u8>| matches!(c, Some(b'"' | b'\''));
    let mut j = i + kw_len;
    if is_quote(b.get(j)) {
        j += 1;
    }
    j = skip_spaces(j);
    if !matches!(b.get(j), Some(b':' | b'=')) {
        return None;
    }
    j = skip_spaces(j + 1);
    if is_quote(b.get(j)) {
        j += 1;
    }
    let n = run_len(b, j, |c| {
        !c.is_ascii_whitespace() && !b"\"',;&)}]".contains(&c)
    });
    let value = &text[j..j + n];
    if value.chars().count() < CREDENTIAL_MIN_CHARS || value.starts_with("<redacted:") {
        return None;
    }
    Some(Hit {
        start: j,
        end: j + n,
        kind: RedactKind::Credential,
    })
}

fn is_token_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || b"+/=_-".contains(&c)
}

/// A run of token bytes that is long enough and has letters and digits, and is not an
/// identifier (see [`is_identifier`]).
fn token_at(text: &str, i: usize) -> Option<Hit> {
    let b = text.as_bytes();
    if !is_token_byte(b[i]) || (i > 0 && is_token_byte(b[i - 1])) {
        return None;
    }
    let n = run_len(b, i, is_token_byte);
    let run = &text[i..i + n];
    let letters = run.bytes().any(|c| c.is_ascii_alphabetic());
    let digits = run.bytes().any(|c| c.is_ascii_digit());
    if n < TOKEN_MIN || !letters || !digits || is_identifier(run) {
        return None;
    }
    Some(Hit {
        start: i,
        end: i + n,
        kind: RedactKind::Token,
    })
}

/// A UUID (8-4-4-4-12 hex) or a git object id (40 or 64 hex digits, SHA-1 or SHA-256). These
/// are identifiers that appear in every session's text; hiding them would damage it.
fn is_identifier(run: &str) -> bool {
    let uuid = run.len() == 36
        && run.bytes().enumerate().all(|(k, c)| {
            if matches!(k, 8 | 13 | 18 | 23) {
                c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        });
    let object_id = matches!(run.len(), 40 | 64) && run.bytes().all(|c| c.is_ascii_hexdigit());
    uuid || object_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// `text` redacted, with the counts checked: each `(kind, n)` is replaced `n` times and
    /// nothing else is.
    fn check(text: &str, expected: &str, kinds: &[(RedactKind, u64)]) {
        let (out, counts) = redact_text(text);
        assert_eq!(out, expected, "input: {text:?}");
        for &(kind, n) in kinds {
            assert_eq!(counts.get(kind), n, "{kind:?} in {text:?}");
        }
        let total: u64 = kinds.iter().map(|&(_, n)| n).sum();
        assert_eq!(counts.total(), total, "total in {text:?}");
        // Redacting the output again finds nothing: markers are never secrets.
        let (again, recount) = redact_text(&out);
        assert_eq!(again, out, "not idempotent: {text:?}");
        assert_eq!(recount.total(), 0, "second pass in {text:?}");
    }

    #[test]
    fn provider_keys_are_replaced() {
        check(
            "key sk-proj-abcdefghijklmnopqrstuvwxyz0123 end",
            "key <redacted:sk_key> end",
            &[(RedactKind::SkKey, 1)],
        );
        check(
            "AWS AKIAIOSFODNN7EXAMPLE.",
            "AWS <redacted:aws_key>.",
            &[(RedactKind::AwsKey, 1)],
        );
        check(
            "gh ghp_0123456789abcdefghijklmnopqrstuvwxyz",
            "gh <redacted:github_token>",
            &[(RedactKind::GithubToken, 1)],
        );
        check(
            "pat github_pat_11ABCDEFG0123456789_abcdefghijk",
            "pat <redacted:github_token>",
            &[(RedactKind::GithubToken, 1)],
        );
        check(
            "slack xoxb-1234567890-abcdefghij",
            "slack <redacted:slack_token>",
            &[(RedactKind::SlackToken, 1)],
        );
        check(
            "g AIzaSyA1234567890abcdefghijklmnopqrstuv",
            "g <redacted:google_key>",
            &[(RedactKind::GoogleKey, 1)],
        );
    }

    #[test]
    fn provider_key_false_positives_are_kept() {
        // `sk-` inside a word, a short body, an AKIA prefix of a longer uppercase run.
        check(
            "task-manager-service-configuration risk-based",
            "task-manager-service-configuration risk-based",
            &[],
        );
        check("sk-approach", "sk-approach", &[]);
        check("AKIAS3BUCKETNAMELONGX", "AKIAS3BUCKETNAMELONGX", &[]);
        check("ghp_short", "ghp_short", &[]);
    }

    #[test]
    fn pem_private_key_blocks_are_replaced_whole() {
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA7vQ9abc\nxyz123==\n-----END RSA PRIVATE KEY-----";
        check(
            &format!("before\n{pem}\nafter"),
            "before\n<redacted:private_key>\nafter",
            &[(RedactKind::PrivateKey, 1)],
        );
        // Without a footer the rest of the text is the key.
        check(
            "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADAN",
            "<redacted:private_key>",
            &[(RedactKind::PrivateKey, 1)],
        );
    }

    #[test]
    fn certificates_are_kept() {
        let cert = "-----BEGIN CERTIFICATE-----\nMIIDdzCCAl+gAwIBAgIE\n-----END CERTIFICATE-----";
        check(cert, cert, &[]);
    }

    #[test]
    fn bearer_tokens_are_replaced_and_prose_is_kept() {
        check(
            "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig",
            "Authorization: Bearer <redacted:bearer>",
            &[(RedactKind::Bearer, 1)],
        );
        check(
            "BEARER abcdefgh1234567890XYZ",
            "BEARER <redacted:bearer>",
            &[(RedactKind::Bearer, 1)],
        );
        check(
            "bearer authentication is required",
            "bearer authentication is required",
            &[],
        );
        check("the bearer tokens rotate", "the bearer tokens rotate", &[]);
    }

    #[test]
    fn url_credentials_keep_scheme_and_host() {
        check(
            "postgres://admin:hunter22@db.example.com:5432/app",
            "postgres://<redacted:url_credentials>@db.example.com:5432/app",
            &[(RedactKind::UrlCredentials, 1)],
        );
        check(
            "https://example.com:8443/a@b",
            "https://example.com:8443/a@b",
            &[],
        );
        check(
            "git@github.com:org/repo.git",
            "git@github.com:org/repo.git",
            &[],
        );
        check("https://example.com/x", "https://example.com/x", &[]);
    }

    #[test]
    fn credential_assignments_keep_the_key() {
        check(
            "password=hunter22 user=bob",
            "password=<redacted:credential> user=bob",
            &[(RedactKind::Credential, 1)],
        );
        check(
            r#"{"api_key": "abcd1234efgh"}"#,
            r#"{"api_key": "<redacted:credential>"}"#,
            &[(RedactKind::Credential, 1)],
        );
        check(
            "GITHUB_TOKEN: 'zz99yy88xx77'",
            "GITHUB_TOKEN: '<redacted:credential>'",
            &[(RedactKind::Credential, 1)],
        );
        check(
            "api-key=abc123def456",
            "api-key=<redacted:credential>",
            &[(RedactKind::Credential, 1)],
        );
    }

    #[test]
    fn credential_false_positives_are_kept() {
        check("max_tokens: 4096", "max_tokens: 4096", &[]);
        check("tokens=1234567", "tokens=1234567", &[]);
        check("token count: 5", "token count: 5", &[]);
        check("password: see below", "password: see below", &[]);
        check("secret_name=abcdefgh", "secret_name=abcdefgh", &[]);
    }

    #[test]
    fn uuids_and_git_ids_are_kept() {
        check(
            "session 3f2a9c1e-7b4d-4e8f-9a1b-2c3d4e5f6a7b",
            "session 3f2a9c1e-7b4d-4e8f-9a1b-2c3d4e5f6a7b",
            &[],
        );
        check(
            "commit 9fceb02d0ae598e95dc970b74767f19372d61af8",
            "commit 9fceb02d0ae598e95dc970b74767f19372d61af8",
            &[],
        );
        let sha256 = "a3f5c7e9b1d2f4a6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c4e6b8d0f2a4";
        check(
            &format!("object {sha256}"),
            &format!("object {sha256}"),
            &[],
        );
    }

    #[test]
    fn token_runs_are_replaced() {
        check(
            "value Zx9kQ2mV8pLw3RtY7uHn4BcE6sDf1GjA0iOy5Xq end",
            "value <redacted:token> end",
            &[(RedactKind::Token, 1)],
        );
        // Known limitation, pinned: a 32-hex digest (md5) is token-shaped and is not exempted,
        // since only UUIDs and git object ids are identifiers here.
        check(
            "md5 d41d8cd98f00b204e9800998ecf8427e",
            "md5 <redacted:token>",
            &[(RedactKind::Token, 1)],
        );
    }

    #[test]
    fn paths_and_words_are_kept() {
        for text in [
            "/home/mewais/Marsad/Wasit/crates/wasit-runtime/src/redact.rs",
            "crates/wasit-runtime/tests/fixtures/searchable.json",
            "the quick brown fox jumps over the lazy dog",
            "internationalization and abcdefghijklmnopqrstuvwxyzabcdef",
        ] {
            check(text, text, &[]);
        }
    }

    #[test]
    fn non_ascii_text_keeps_its_characters() {
        check(
            "héllo sk-proj-abcdefghijklmnopqrstuvwxyz0123 wörld ✓",
            "héllo <redacted:sk_key> wörld ✓",
            &[(RedactKind::SkKey, 1)],
        );
    }

    #[test]
    fn several_kinds_in_one_text_are_counted_per_kind() {
        let text =
            "a sk-proj-abcdefghijklmnopqrstuvwxyz0123 b sk-proj-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123 \
                    c password=hunter22 d Bearer eyJhbGciOiJIUzI1NiJ9.sig";
        let (out, counts) = redact_text(text);
        assert_eq!(
            out,
            "a <redacted:sk_key> b <redacted:sk_key> c password=<redacted:credential> d Bearer <redacted:bearer>"
        );
        assert_eq!(counts.get(RedactKind::SkKey), 2);
        assert_eq!(counts.get(RedactKind::Credential), 1);
        assert_eq!(counts.get(RedactKind::Bearer), 1);
        assert_eq!(counts.get(RedactKind::Token), 0);
        assert_eq!(counts.total(), 4);
        let kinds: Vec<&str> = counts.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, ["sk_key", "bearer", "credential"]);
    }

    #[test]
    fn json_keys_are_untouched_and_non_strings_pass_through() {
        let value = json!({
            "sk-proj-abcdefghijklmnopqrstuvwxyz0123": "sk-proj-abcdefghijklmnopqrstuvwxyz0123",
            "n": 3,
            "ok": true,
            "none": null,
            "list": ["Bearer eyJhbGciOiJIUzI1NiJ9.sig", null, 7, {"msg": "plain"}],
        });
        let (out, counts) = redact_json(&value);
        assert_eq!(
            out,
            json!({
                "sk-proj-abcdefghijklmnopqrstuvwxyz0123": "<redacted:sk_key>",
                "n": 3,
                "ok": true,
                "none": null,
                "list": ["Bearer <redacted:bearer>", null, 7, {"msg": "plain"}],
            })
        );
        assert_eq!(counts.get(RedactKind::SkKey), 1);
        assert_eq!(counts.get(RedactKind::Bearer), 1);
        assert_eq!(counts.total(), 2);
    }

    #[test]
    fn json_without_secrets_is_returned_equal() {
        let value =
            json!({"path": "/work/src/lib.rs", "sha": "9fceb02d0ae598e95dc970b74767f19372d61af8"});
        let (out, counts) = redact_json(&value);
        assert_eq!(out, value);
        assert_eq!(counts.total(), 0);
    }
}
