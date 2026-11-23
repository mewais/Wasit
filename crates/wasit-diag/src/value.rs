//! The value types an event field may have (design §4.2): the privacy boundary.
//!
//! [`DiagValue`] is sealed. Its implementations are this file, [`DiagText`](crate::DiagText),
//! enums declared with [`diag_enum!`](crate::diag_enum), and validated newtypes of other Wasit
//! crates registered with [`diag_value!`](crate::diag_value) (which render through one of these
//! types). Deliberately without an implementation: `String`, `&str`, `Cow<str>`,
//! `serde_json::Value`, `Path`/`PathBuf`, `OsString`, and every wire or content type.

use std::borrow::Cow;
use std::fmt;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::json::Json;
use crate::policy;
use crate::text::{self, DiagText, Origin};

/// Seal of [`DiagValue`]; implemented only in this crate and by the registration macros.
pub trait Sealed {}

/// A type that may be the type of an event field. Sealed: see the module documentation.
pub trait DiagValue: Sealed {
    #[doc(hidden)]
    fn __write(&self, out: &mut Json);
    /// `false` for an absent optional value: the field is left out of the record.
    #[doc(hidden)]
    fn __present(&self) -> bool {
        true
    }
}

// ── numbers ─────────────────────────────────────────────────────────────────

macro_rules! unsigned {
    ($($t:ty),*) => {$(
        impl Sealed for $t {}
        impl DiagValue for $t {
            fn __write(&self, out: &mut Json) { out.uint(u64::from(*self)) }
        }
    )*};
}
macro_rules! signed {
    ($($t:ty),*) => {$(
        impl Sealed for $t {}
        impl DiagValue for $t {
            fn __write(&self, out: &mut Json) { out.int(i64::from(*self)) }
        }
    )*};
}
unsigned!(u8, u16, u32, u64);
signed!(i32, i64);

impl Sealed for bool {}
impl DiagValue for bool {
    fn __write(&self, out: &mut Json) {
        out.bool(*self)
    }
}

impl Sealed for f32 {}
impl DiagValue for f32 {
    /// Non-finite values are written as `null`.
    fn __write(&self, out: &mut Json) {
        out.float(*self)
    }
}

macro_rules! unit_newtype {
    ($($(#[$m:meta])* $name:ident),*) => {$(
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub u64);
        impl Sealed for $name {}
        impl DiagValue for $name {
            fn __write(&self, out: &mut Json) { out.uint(self.0) }
        }
    )*};
}
unit_newtype!(
    /// A duration in milliseconds.
    Ms,
    /// A size in bytes.
    Bytes,
    /// A wall-clock time: milliseconds since the Unix epoch.
    TimeMs
);

impl Ms {
    /// A duration, saturating at `u64::MAX` milliseconds.
    pub fn of(d: std::time::Duration) -> Ms {
        Ms(u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

impl TimeMs {
    /// The current wall-clock time.
    pub fn now() -> TimeMs {
        TimeMs(crate::sink::wall_ms())
    }
}

// ── validated words ─────────────────────────────────────────────────────────

/// A validated word, or the character count of what failed validation (the text itself is
/// not kept).
#[derive(Clone, PartialEq, Eq, Hash)]
enum Word {
    Ok(Cow<'static, str>),
    Other(usize),
}

impl Word {
    fn check(s: Cow<'static, str>, max: usize, ok: fn(u8) -> bool) -> Word {
        if !s.is_empty() && s.len() <= max && s.bytes().all(ok) {
            Word::Ok(s)
        } else {
            Word::Other(s.chars().count())
        }
    }

    /// Reads back what [`Word::write`] wrote: `<other:N>` is a word that failed validation on
    /// the writing side (its length is all that is known), anything else is validated again.
    fn from_wire(s: &str, max: usize, ok: fn(u8) -> bool) -> Word {
        if let Some(n) = s
            .strip_prefix("<other:")
            .and_then(|r| r.strip_suffix('>'))
            .and_then(|n| n.parse::<usize>().ok())
        {
            return Word::Other(n);
        }
        Word::check(Cow::Owned(s.to_owned()), max, ok)
    }

    fn write(&self, out: &mut Json) {
        match self {
            // Validated: no character that needs escaping.
            Word::Ok(s) => out.str(s),
            Word::Other(n) => out.other(*n),
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Word::Ok(s) => Some(s),
            Word::Other(_) => None,
        }
    }
}

impl fmt::Debug for Word {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Word::Ok(s) => write!(f, "{s:?}"),
            Word::Other(n) => write!(f, "<other:{n}>"),
        }
    }
}

fn ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'@' | b'+' | b'-')
}

fn code_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-')
}

/// An identifier: session, host, driver, model, mode, native session/turn, gate request, build
/// ids, machine keys. `[A-Za-z0-9._:@+-]{1,128}`; anything else renders as `"<other:N>"`
/// (N = character count).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(Word);

impl Ident {
    pub const MAX: usize = 128;

    pub fn new(s: &str) -> Ident {
        Ident(Word::check(Cow::Owned(s.to_owned()), Self::MAX, ident_byte))
    }

    /// From a static string, without allocating.
    pub fn st(s: &'static str) -> Ident {
        Ident(Word::check(Cow::Borrowed(s), Self::MAX, ident_byte))
    }

    /// The identifier, `None` when it failed validation.
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_str()
    }

    /// Reads an identifier back from the wire (see [`crate::wire`]).
    pub(crate) fn from_wire(s: &str) -> Ident {
        Ident(Word::from_wire(s, Self::MAX, ident_byte))
    }
}

/// A machine word: error codes, enum names, CLI result subtypes, JSON-RPC method names, tool
/// names, flag names. `[A-Za-z0-9_.:-]{1,64}`; anything else renders as `"<other:N>"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Code(Word);

impl Code {
    pub const MAX: usize = 64;

    pub fn new(s: &str) -> Code {
        Code(Word::check(Cow::Owned(s.to_owned()), Self::MAX, code_byte))
    }

    /// From a static string, without allocating.
    pub fn st(s: &'static str) -> Code {
        Code(Word::check(Cow::Borrowed(s), Self::MAX, code_byte))
    }

    /// The word, `None` when it failed validation.
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_str()
    }

    /// Reads a word back from the wire (see [`crate::wire`]).
    pub(crate) fn from_wire(s: &str) -> Code {
        Code(Word::from_wire(s, Self::MAX, code_byte))
    }
}

impl Sealed for Ident {}
impl DiagValue for Ident {
    fn __write(&self, out: &mut Json) {
        self.0.write(out)
    }
}

impl Sealed for Code {}
impl DiagValue for Code {
    fn __write(&self, out: &mut Json) {
        self.0.write(out)
    }
}

// ── fingerprints ────────────────────────────────────────────────────────────

/// Fingerprint of a foreign text: the first 8 hex of SHA-256 over the text with hex runs of 8
/// or more characters replaced by `x`, then digits by `0`, whitespace runs collapsed to one
/// space. Groups identical errors across sessions without containing them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fp([u8; 4]);

impl Fp {
    pub fn of(text: &str) -> Fp {
        let digest = Sha256::digest(Self::normalize(text).as_bytes());
        Fp([digest[0], digest[1], digest[2], digest[3]])
    }

    fn normalize(text: &str) -> String {
        let chars: Vec<char> = text.chars().collect();
        let mut out = String::with_capacity(text.len());
        let mut i = 0;
        let mut space = false;
        while i < chars.len() {
            let c = chars[i];
            if c.is_ascii_hexdigit() {
                let end = chars[i..]
                    .iter()
                    .position(|c| !c.is_ascii_hexdigit())
                    .map_or(chars.len(), |p| i + p);
                if end - i >= 8 {
                    out.push('x');
                } else {
                    out.extend(
                        chars[i..end]
                            .iter()
                            .map(|c| if c.is_ascii_digit() { '0' } else { *c }),
                    );
                }
                i = end;
                space = false;
                continue;
            }
            if c.is_whitespace() {
                if !space && !out.is_empty() {
                    out.push(' ');
                }
                space = true;
            } else {
                out.push(if c.is_numeric() { '0' } else { c });
                space = false;
            }
            i += 1;
        }
        if out.ends_with(' ') {
            out.pop();
        }
        out
    }
}

impl Sealed for Fp {}
impl DiagValue for Fp {
    fn __write(&self, out: &mut Json) {
        out.str(&hex(&self.0));
    }
}

impl Fp {
    /// Reads a fingerprint back from the wire: exactly eight hex digits.
    pub(crate) fn from_hex(s: &str) -> Option<Fp> {
        let b = s.as_bytes();
        if b.len() != 8 {
            return None;
        }
        let nibble = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        let mut out = [0u8; 4];
        for (i, pair) in b.chunks(2).enumerate() {
            out[i] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
        }
        Some(Fp(out))
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[usize::from(b >> 4)] as char);
        s.push(H[usize::from(b & 15)] as char);
    }
    s
}

// ── errors ──────────────────────────────────────────────────────────────────

/// An error: `{code, text?, len?, fp?, os?}`, built by exactly three constructors.
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorInfo {
    code: Code,
    text: Option<DiagText>,
    len: Option<u32>,
    fp: Option<Fp>,
    os: Option<i32>,
}

impl ErrorInfo {
    /// One of our own error types: its `Display`, scrubbed ([`Origin::Own`]).
    pub fn ours(code: Code, err: &impl fmt::Display) -> ErrorInfo {
        ErrorInfo {
            code,
            text: Some(DiagText::new(Origin::Own, &err.to_string())),
            len: None,
            fp: None,
            os: None,
        }
    }

    /// An I/O error: its kind as the code and the OS error number. Never the message (it can
    /// name a path).
    pub fn io(err: &std::io::Error) -> ErrorInfo {
        ErrorInfo {
            code: Code::new(&snake(&format!("{:?}", err.kind()))),
            text: None,
            len: None,
            fp: None,
            os: err.raw_os_error(),
        }
    }

    /// A text from outside our code (a CLI, a JSON-RPC peer, a user). What is kept of it is
    /// [`policy::foreign_text`]'s decision (currently length and fingerprint, no text).
    pub fn foreign(code: Code, text: &str) -> ErrorInfo {
        let kept = policy::foreign_text(text);
        ErrorInfo {
            code,
            text: kept.text,
            len: kept.len,
            fp: kept.fp,
            os: None,
        }
    }
}

impl Sealed for ErrorInfo {}
impl DiagValue for ErrorInfo {
    fn __write(&self, out: &mut Json) {
        out.raw("{\"code\":");
        self.code.__write(out);
        if let Some(t) = &self.text {
            out.raw(",\"text\":");
            t.__write(out);
        }
        if let Some(n) = self.len {
            out.raw(",\"len\":");
            out.uint(u64::from(n));
        }
        if let Some(fp) = &self.fp {
            out.raw(",\"fp\":");
            fp.__write(out);
        }
        if let Some(os) = self.os {
            out.raw(",\"os\":");
            out.int(i64::from(os));
        }
        out.raw("}");
    }
}

impl ErrorInfo {
    /// Reads an error back from the wire (see [`crate::wire`]); what was written is what is read.
    pub(crate) fn from_wire(
        code: Code,
        text: Option<DiagText>,
        len: Option<u32>,
        fp: Option<Fp>,
        os: Option<i32>,
    ) -> ErrorInfo {
        ErrorInfo {
            code,
            text,
            len,
            fp,
            os,
        }
    }
}

/// `NotFound` → `not_found`.
fn snake(camel: &str) -> String {
    let mut s = String::with_capacity(camel.len() + 4);
    for (i, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                s.push('_');
            }
            s.push(c.to_ascii_lowercase());
        } else {
            s.push(c);
        }
    }
    s
}

// ── paths ───────────────────────────────────────────────────────────────────

/// One of Marsad's own paths (data root, runtime directory, helper binary, socket), only for
/// paths the code derived itself from its layout. The home directory prefix is written as `~`;
/// at most 200 characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomePath(String);

impl HomePath {
    pub const MAX: usize = 200;

    pub fn new(path: &Path) -> HomePath {
        let raw = path.to_string_lossy();
        let shown = text::home_relative(&raw);
        HomePath(text::cut(&text::escape_controls(&shown), Self::MAX))
    }
}

impl Sealed for HomePath {}
impl DiagValue for HomePath {
    fn __write(&self, out: &mut Json) {
        out.str(&self.0)
    }
}

/// A path that may name the user's project (a session's cwd, a launch project, a file a tool
/// touched). How it is shown is [`policy::project_path`]'s decision (currently a [`PathTag`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectPath(PathTag);

impl ProjectPath {
    pub fn new(path: impl AsRef<Path>) -> ProjectPath {
        ProjectPath(policy::project_path(&path.as_ref().to_string_lossy()))
    }

    /// A path that arrived as text (wire types carry paths as strings).
    pub fn from_text(path: &str) -> ProjectPath {
        ProjectPath(policy::project_path(path))
    }

    /// Reads a tag back from the wire (see [`crate::wire`]).
    pub(crate) fn from_tag(tag: PathTag) -> ProjectPath {
        ProjectPath(tag)
    }
}

impl Sealed for ProjectPath {}
impl DiagValue for ProjectPath {
    fn __write(&self, out: &mut Json) {
        self.0.write_json(out)
    }
}

/// The hashed form of a path: `{kind, depth, hash}`, `hash` = the first 10 hex of
/// SHA-256(`"p1:" + normalized path`), normalized = trailing separators trimmed; Windows forms
/// lowercased with `/` read as `\`. Inside free text it renders as `<p:hash>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTag {
    kind: PathKind,
    depth: u32,
    hash: String,
}

/// The shape of a path, without its names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    Posix,
    Win,
    Unc,
    WslUnc,
    WslMnt,
    Tilde,
    Relative,
    Empty,
}

impl PathKind {
    fn code(self) -> &'static str {
        match self {
            PathKind::Posix => "posix",
            PathKind::Win => "win",
            PathKind::Unc => "unc",
            PathKind::WslUnc => "wsl_unc",
            PathKind::WslMnt => "wsl_mnt",
            PathKind::Tilde => "tilde",
            PathKind::Relative => "relative",
            PathKind::Empty => "empty",
        }
    }

    /// The kind written as `code`.
    pub(crate) fn from_code(code: &str) -> Option<PathKind> {
        Some(match code {
            "posix" => PathKind::Posix,
            "win" => PathKind::Win,
            "unc" => PathKind::Unc,
            "wsl_unc" => PathKind::WslUnc,
            "wsl_mnt" => PathKind::WslMnt,
            "tilde" => PathKind::Tilde,
            "relative" => PathKind::Relative,
            "empty" => PathKind::Empty,
            _ => return None,
        })
    }

    fn windows(self) -> bool {
        matches!(self, PathKind::Win | PathKind::Unc | PathKind::WslUnc)
    }

    /// Leading components that form the root (not counted in `depth`).
    fn root_components(self) -> usize {
        match self {
            PathKind::Posix | PathKind::Relative | PathKind::Empty => 0,
            PathKind::Win | PathKind::Tilde => 1,
            PathKind::Unc | PathKind::WslUnc | PathKind::WslMnt => 2,
        }
    }
}

impl PathTag {
    /// A tag read back from the wire; the hash is the ten hex digits [`PathTag::of`] makes.
    pub(crate) fn from_parts(kind: PathKind, depth: u32, hash: &str) -> Option<PathTag> {
        let ok = hash.len() == 10 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        ok.then(|| PathTag {
            kind,
            depth,
            hash: hash.to_owned(),
        })
    }

    pub(crate) fn of(raw: &str) -> PathTag {
        let kind = Self::classify(raw);
        let mut norm = if kind.windows() {
            raw.replace('/', "\\").to_lowercase()
        } else {
            raw.to_owned()
        };
        let sep: &[char] = if kind.windows() { &['\\'] } else { &['/'] };
        let min = match kind {
            PathKind::Win => 3, // keep `c:\`
            _ => 1,             // keep `/`
        };
        while norm.len() > min && norm.ends_with(sep) {
            norm.pop();
        }
        let depth = norm
            .split(['/', '\\'])
            .filter(|c| !c.is_empty())
            .count()
            .saturating_sub(kind.root_components());
        let digest = Sha256::digest(format!("p1:{norm}").as_bytes());
        PathTag {
            kind,
            depth: u32::try_from(depth).unwrap_or(u32::MAX),
            hash: hex(&digest[..5]),
        }
    }

    fn classify(raw: &str) -> PathKind {
        let b = raw.as_bytes();
        let lower = raw.to_ascii_lowercase().replace('/', "\\");
        if raw.is_empty() {
            PathKind::Empty
        } else if lower.starts_with("\\\\wsl$\\") || lower.starts_with("\\\\wsl.localhost\\") {
            PathKind::WslUnc
        } else if raw.starts_with("\\\\") || (raw.starts_with("//") && !raw.starts_with("///")) {
            PathKind::Unc
        } else if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
            PathKind::Win
        } else if b.len() >= 6
            && raw.starts_with("/mnt/")
            && b[5].is_ascii_lowercase()
            && (b.len() == 6 || b[6] == b'/')
        {
            PathKind::WslMnt
        } else if raw.starts_with('/') {
            PathKind::Posix
        } else if raw == "~" || raw.starts_with("~/") || raw.starts_with("~\\") {
            PathKind::Tilde
        } else {
            PathKind::Relative
        }
    }

    fn write_json(&self, out: &mut Json) {
        out.raw("{\"kind\":");
        out.str(self.kind.code());
        out.raw(",\"depth\":");
        out.uint(u64::from(self.depth));
        out.raw(",\"hash\":");
        out.str(&self.hash);
        out.raw("}");
    }

    /// The form used inside free text: `<p:hash>`.
    pub(crate) fn text_form(&self) -> String {
        format!("<p:{}>", self.hash)
    }
}

/// A source location (`file:line:col`) made by the compiler (`std::panic::Location` has no
/// public constructor), never runtime data: the panic hook's `location`. The home directory
/// prefix is written as `~`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SrcLocation(String);

impl SrcLocation {
    pub fn of(loc: &std::panic::Location<'_>) -> SrcLocation {
        let s = format!("{}:{}:{}", loc.file(), loc.line(), loc.column());
        SrcLocation(text::cut(
            &text::escape_controls(&text::home_relative(&s)),
            HomePath::MAX,
        ))
    }

    /// For a panic without a location.
    pub fn unknown() -> SrcLocation {
        SrcLocation("unknown".to_owned())
    }
}

impl Sealed for SrcLocation {}
impl DiagValue for SrcLocation {
    fn __write(&self, out: &mut Json) {
        out.str(&self.0)
    }
}

// ── containers ──────────────────────────────────────────────────────────────

impl<T: DiagValue> Sealed for Option<T> {}
impl<T: DiagValue> DiagValue for Option<T> {
    /// `None` as a field leaves the field out; nested (inside a `Vec`) it is `null`.
    fn __write(&self, out: &mut Json) {
        match self {
            Some(v) => v.__write(out),
            None => out.null(),
        }
    }
    fn __present(&self) -> bool {
        self.is_some()
    }
}

/// Elements of a `Vec` written before the rest is summarized as `"…+N"`.
pub const VEC_MAX: usize = 32;

impl<T: DiagValue> Sealed for Vec<T> {}
impl<T: DiagValue> DiagValue for Vec<T> {
    fn __write(&self, out: &mut Json) {
        out.raw("[");
        for (i, v) in self.iter().take(VEC_MAX).enumerate() {
            if i > 0 {
                out.raw(",");
            }
            v.__write(out);
        }
        if self.len() > VEC_MAX {
            out.raw(",");
            out.str(&format!("…+{}", self.len() - VEC_MAX));
        }
        out.raw("]");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json<V: DiagValue>(v: &V) -> String {
        let mut j = Json::new();
        v.__write(&mut j);
        j.buf
    }

    #[test]
    fn ident_and_code_validate_and_never_keep_rejected_text() {
        assert_eq!(
            json(&Ident::new("sess-01.a:b@c+d_e")),
            "\"sess-01.a:b@c+d_e\""
        );
        assert_eq!(json(&Ident::new("has space")), "\"<other:9>\"");
        assert_eq!(json(&Ident::new("a/b")), "\"<other:3>\"");
        assert_eq!(json(&Ident::new("")), "\"<other:0>\"");
        assert_eq!(json(&Ident::new(&"x".repeat(129))), "\"<other:129>\"");
        assert_eq!(json(&Ident::new("héllo")), "\"<other:5>\"");
        assert_eq!(json(&Code::st("not_found")), "\"not_found\"");
        assert_eq!(json(&Code::new("a@b")), "\"<other:3>\"");
        assert_eq!(json(&Code::new(&"c".repeat(65))), "\"<other:65>\"");
        assert!(!format!("{:?}", Code::new("secret words")).contains("secret"));
    }

    #[test]
    fn numbers_and_containers() {
        assert_eq!(json(&Ms(5)), "5");
        assert_eq!(json(&-3i32), "-3");
        assert_eq!(json(&f32::NAN), "null");
        assert_eq!(json(&1.5f32), "1.5");
        assert_eq!(json(&Some(Bytes(7))), "7");
        assert!(!None::<u8>.__present());
        assert_eq!(json(&vec![Some(1u8), None]), "[1,null]");
        let long: Vec<u32> = (0..40).collect();
        let s = json(&long);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        let a = v.as_array().unwrap();
        assert_eq!(a.len(), VEC_MAX + 1);
        assert_eq!(a[VEC_MAX], "…+8");
    }

    #[test]
    fn fingerprint_groups_variants_of_one_error() {
        let a = Fp::of("connection 12 refused  at\tdeadbeef0123 port 22");
        let b = Fp::of("connection 97 refused at 0123456789abcdef port 80");
        assert_eq!(a, b);
        assert_ne!(a, Fp::of("permission denied"));
        assert_eq!(json(&a).len(), 10);
    }

    #[test]
    fn error_info_constructors() {
        let e = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert_eq!(json(&ErrorInfo::io(&e)), "{\"code\":\"not_found\"}");
        let e = std::io::Error::from_raw_os_error(2);
        let v: serde_json::Value = serde_json::from_str(&json(&ErrorInfo::io(&e))).unwrap();
        assert_eq!(v["os"], 2);
        assert!(v.get("text").is_none());

        let f = ErrorInfo::foreign(Code::st("execution_error"), "the prompt said FOREIGN-MARK");
        let s = json(&f);
        assert!(!s.contains("FOREIGN-MARK"), "{s}");
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["len"], 28);
        assert_eq!(v["fp"].as_str().unwrap().len(), 8);

        let o = ErrorInfo::ours(Code::st("spawn"), &"could not start: no such file");
        let v: serde_json::Value = serde_json::from_str(&json(&o)).unwrap();
        assert_eq!(v["text"], "could not start: no such file");
    }

    #[test]
    fn path_tags() {
        let t = |s: &str| {
            let v: serde_json::Value =
                serde_json::from_str(&json(&ProjectPath::from_text(s))).unwrap();
            (
                v["kind"].as_str().unwrap().to_owned(),
                v["depth"].as_u64().unwrap(),
                v["hash"].as_str().unwrap().to_owned(),
            )
        };
        assert_eq!(t("/home/u/proj").0, "posix");
        assert_eq!(t("/home/u/proj").1, 3);
        assert_eq!(t("/home/u/proj/").2, t("/home/u/proj").2);
        assert_ne!(t("/home/u/proj").2, t("/home/u/other").2);
        assert_eq!(t("/home/u/proj").2.len(), 10);
        assert_eq!(t("C:\\Users\\U\\Proj").0, "win");
        assert_eq!(t("C:\\Users\\U\\Proj").1, 3);
        assert_eq!(t("c:/users/u/proj/").2, t("C:\\Users\\U\\Proj").2);
        assert_eq!(t("\\\\server\\share\\x").0, "unc");
        assert_eq!(t("\\\\server\\share\\x").1, 1);
        assert_eq!(t("\\\\wsl$\\Ubuntu\\home\\u").0, "wsl_unc");
        assert_eq!(t("\\\\wsl.localhost\\Ubuntu\\home").0, "wsl_unc");
        assert_eq!(t("/mnt/c/work").0, "wsl_mnt");
        assert_eq!(t("/mnt/c/work").1, 1);
        assert_eq!(t("/mnt/data").0, "posix");
        assert_eq!(t("~/proj").0, "tilde");
        assert_eq!(t("src/x.rs").0, "relative");
        assert_eq!(t("").0, "empty");
        assert_eq!(t("/").1, 0);
        assert!(!json(&ProjectPath::from_text("/home/u/secretproj")).contains("secretproj"));
    }
}
