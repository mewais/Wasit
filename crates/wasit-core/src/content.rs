//! Non-text content: images and files that appear in a transcript ([`ContentPart`]) and files
//! attached to a prompt ([`Attachment`]), plus what a driver accepts in a prompt
//! ([`PromptInput`]).
//!
//! Bytes never travel in events past the runtime: a protocol reports inline data as
//! [`MediaSource::Inline`]; the runtime stores it once per session (content-addressed blob)
//! and replaces it with [`MediaSource::Blob`] before an event is published. Clients fetch blob
//! bytes through the runtime's `session.file.read` method.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Where the bytes of an image / file are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum MediaSource {
    /// Base64 data, as the CLI wrote it. Only between a protocol and the runtime; published
    /// events never carry it.
    Inline { data: String },
    /// Stored by the runtime in the session's blob store: `id` is the lowercase hex SHA-256 of
    /// the bytes, `size` their length.
    Blob { id: String, size: u64 },
    /// A remote URL (never fetched by clients without the user asking).
    Url { url: String },
    /// A file on the session's host (absolute, or relative to the session's cwd).
    Path { path: String },
}

/// One part of a message or tool result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text {
        text: String,
    },
    Image {
        mime: String,
        source: MediaSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
        /// Alt text / file name, when the CLI gives one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alt: Option<String>,
    },
    /// A file or resource: an ACP `resource` / `resource_link`, a codex image path, a file a
    /// tool reports.
    File {
        source: MediaSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime: Option<String>,
        /// The resource URI as the CLI gave it (`file:///…`, `https://…`), when it has one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        uri: Option<String>,
    },
}

impl ContentPart {
    /// An image part with inline base64 data.
    pub fn inline_image(mime: impl Into<String>, data: impl Into<String>) -> Self {
        ContentPart::Image {
            mime: mime.into(),
            source: MediaSource::Inline { data: data.into() },
            width: None,
            height: None,
            alt: None,
        }
    }

    /// `true` for text parts.
    pub fn is_text(&self) -> bool {
        matches!(self, ContentPart::Text { .. })
    }
}

/// A file attached to a prompt. The client uploads it first (`session.attachment.put`, which
/// answers with this value) and sends it with [`Command::Prompt`](crate::Command::Prompt).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct Attachment {
    /// File name shown to the user.
    pub name: String,
    pub mime: String,
    pub size: u64,
    /// Absolute path of the uploaded copy on the session's host.
    pub path: String,
    /// Base64 bytes, filled in by the runtime (from `path`) for the protocol to embed; never
    /// published (events carry the attachment without it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts", ts(skip))]
    pub data: Option<String>,
}

/// How a driver passes an arbitrary (non-image) file with a prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
#[serde(rename_all = "snake_case")]
pub enum FileInput {
    /// Files cannot be attached.
    #[default]
    None,
    /// The file is uploaded to the session's host and its path is named in the prompt text
    /// (the agent reads it with its own tools).
    PathReference,
    /// Text files are embedded in the prompt (ACP `resource`); other files are passed as a
    /// link to the uploaded copy (ACP `resource_link`).
    Embedded,
}

/// What a driver accepts in a prompt besides text. A `false` / empty value is honest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct PromptInput {
    /// Image MIME types sent as images (empty: no images).
    pub image_mimes: Vec<String>,
    /// Largest image accepted, in bytes.
    pub max_image_bytes: u64,
    pub files: FileInput,
    /// Largest non-image file accepted, in bytes.
    pub max_file_bytes: u64,
    /// Most attachments in one prompt.
    pub max_attachments: u32,
    /// The protocol embeds the bytes of images (and, with [`FileInput::Embedded`], text
    /// files) in the prompt, so the runtime loads them; otherwise it passes paths only.
    pub embeds_bytes: bool,
}

/// The image MIME types every current model API accepts.
pub const COMMON_IMAGE_MIMES: &[&str] = &["image/png", "image/jpeg", "image/gif", "image/webp"];

/// Embedded-bytes budget per prompt: one prompt is one journal line, capped well below the
/// journal's record limit.
pub const MAX_EMBEDDED_BYTES: u64 = 5 * 1024 * 1024 + 512 * 1024;

impl PromptInput {
    /// Images of the common MIME types up to `max_image_bytes`, files as `files`.
    pub fn images_and(files: FileInput, max_image_bytes: u64) -> Self {
        PromptInput {
            image_mimes: COMMON_IMAGE_MIMES.iter().map(|s| s.to_string()).collect(),
            max_image_bytes,
            files,
            max_file_bytes: 50 * 1024 * 1024,
            max_attachments: 10,
            embeds_bytes: true,
        }
    }

    /// `mime` is sent as an image.
    pub fn accepts_image(&self, mime: &str) -> bool {
        self.image_mimes.iter().any(|m| m == mime)
    }
}

// ── Pure helpers ────────────────────────────────────────────────────────────

/// Standard base64 of `bytes`.
pub fn b64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Bytes of standard (padded or not) base64, ignoring ASCII whitespace.
pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    let clean: String = s.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(clean.as_bytes())
        .or_else(|_| {
            base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(clean.trim_end_matches('=').as_bytes())
        })
        .ok()
}

/// The last path component of a path or URI.
pub fn base_name(p: &str) -> String {
    p.trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(p)
        .to_string()
}

/// Width and height of a PNG, GIF, JPEG or WebP image from its first bytes, when cheaply
/// readable.
pub fn image_dimensions(b: &[u8]) -> Option<(u32, u32)> {
    let be16 = |i: usize| Some(u16::from_be_bytes([*b.get(i)?, *b.get(i + 1)?]) as u32);
    let le16 = |i: usize| Some(u16::from_le_bytes([*b.get(i)?, *b.get(i + 1)?]) as u32);
    let be32 = |i: usize| {
        Some(u32::from_be_bytes([
            *b.get(i)?,
            *b.get(i + 1)?,
            *b.get(i + 2)?,
            *b.get(i + 3)?,
        ]))
    };
    if b.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some((be32(16)?, be32(20)?));
    }
    if b.starts_with(b"GIF8") {
        return Some((le16(6)?, le16(8)?));
    }
    if b.len() > 30 && &b[0..4] == b"RIFF" && &b[8..12] == b"WEBP" {
        return match &b[12..16] {
            b"VP8 " => Some((le16(26)? & 0x3fff, le16(28)? & 0x3fff)),
            b"VP8L" => {
                let v = u32::from_le_bytes([b[21], b[22], b[23], b[24]]);
                Some(((v & 0x3fff) + 1, ((v >> 14) & 0x3fff) + 1))
            }
            b"VP8X" => {
                let w = u32::from_le_bytes([b[24], b[25], b[26], 0]) + 1;
                let h = u32::from_le_bytes([b[27], b[28], b[29], 0]) + 1;
                Some((w, h))
            }
            _ => None,
        };
    }
    if b.starts_with(&[0xff, 0xd8]) {
        let mut i = 2;
        while i + 9 < b.len() {
            if b[i] != 0xff {
                return None;
            }
            let marker = b[i + 1];
            let len = be16(i + 2)? as usize;
            if (0xc0..=0xcf).contains(&marker) && ![0xc4, 0xc8, 0xcc].contains(&marker) {
                return Some((be16(i + 7)?, be16(i + 5)?));
            }
            i += 2 + len;
        }
    }
    None
}

/// The MIME type of a file from its first bytes, falling back to its name's extension.
/// Unknown binary data is `application/octet-stream`; unknown text is `text/plain`.
pub fn sniff_mime(head: &[u8], name: &str) -> String {
    let magic: &[(&[u8], &str)] = &[
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
        (b"%PDF-", "application/pdf"),
        (b"PK\x03\x04", "application/zip"),
        (b"\x1f\x8b", "application/gzip"),
        (b"OggS", "audio/ogg"),
        (b"ID3", "audio/mpeg"),
        (b"fLaC", "audio/flac"),
    ];
    for (m, t) in magic {
        if head.starts_with(m) {
            return (*t).to_string();
        }
    }
    if head.len() >= 12 && &head[0..4] == b"RIFF" {
        match &head[8..12] {
            b"WEBP" => return "image/webp".into(),
            b"WAVE" => return "audio/wav".into(),
            _ => {}
        }
    }
    if head.len() >= 12 && &head[4..8] == b"ftyp" {
        return "video/mp4".into();
    }
    if head.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]) {
        return "video/webm".into();
    }
    let ext = name
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    let by_ext = match ext.as_str() {
        "svg" => Some("image/svg+xml"),
        "html" | "htm" => Some("text/html"),
        "md" | "markdown" => Some("text/markdown"),
        "json" => Some("application/json"),
        "csv" => Some("text/csv"),
        "tsv" => Some("text/tab-separated-values"),
        "xml" => Some("application/xml"),
        "mp3" => Some("audio/mpeg"),
        "mp4" | "m4v" => Some("video/mp4"),
        "webm" => Some("video/webm"),
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "pdf" => Some("application/pdf"),
        _ => None,
    };
    let text = looks_textual(head);
    match by_ext {
        // A binary payload never passes as text because of its name.
        Some(t)
            if t.starts_with("text/")
                || t.ends_with("json")
                || t.ends_with("xml")
                || t.ends_with("svg+xml") =>
        {
            if text {
                t.to_string()
            } else {
                "application/octet-stream".into()
            }
        }
        Some(t) => t.to_string(),
        None if text => {
            let h = String::from_utf8_lossy(&head[..head.len().min(512)]).to_ascii_lowercase();
            let h = h.trim_start();
            if h.starts_with("<svg") || (h.starts_with("<?xml") && h.contains("<svg")) {
                "image/svg+xml".into()
            } else if h.starts_with("<!doctype html") || h.starts_with("<html") {
                "text/html".into()
            } else {
                "text/plain".into()
            }
        }
        None => "application/octet-stream".into(),
    }
}

/// `true` when `head` looks like UTF-8 text (no NUL bytes; valid up to a cut character).
pub fn looks_textual(head: &[u8]) -> bool {
    if head.contains(&0) {
        return false;
    }
    match std::str::from_utf8(head) {
        Ok(_) => true,
        // A multi-byte character cut by the end of the sample is still text.
        Err(e) => e.error_len().is_none() && head.len() - e.valid_up_to() < 4,
    }
}

/// `true` for MIME types whose bytes are text.
pub fn is_text_mime(mime: &str) -> bool {
    mime.starts_with("text/")
        || matches!(
            mime,
            "application/json" | "application/xml" | "image/svg+xml" | "application/javascript"
        )
}

/// Longest string kept verbatim inside raw JSON carried by events (tool inputs/results).
pub const MAX_RAW_STRING: usize = 64 * 1024;

/// Replace inline media inside raw JSON (base64 `data` / data URLs / very long strings) by a
/// short placeholder, so raw values attached to events stay small. Returns `true` when
/// something was replaced. Deterministic.
pub fn scrub_raw(v: &mut Value) -> bool {
    match v {
        Value::String(s) => {
            let media = s.starts_with("data:") && s.len() > 256;
            if media || s.len() > MAX_RAW_STRING {
                *s = format!("[{} bytes omitted]", s.len());
                true
            } else {
                false
            }
        }
        Value::Array(a) => a.iter_mut().fold(false, |acc, x| scrub_raw(x) | acc),
        Value::Object(o) => {
            let mut any = false;
            for (k, x) in o.iter_mut() {
                let b64 = matches!(k.as_str(), "data" | "base64" | "blob")
                    && x.as_str().is_some_and(|s| s.len() > 256);
                if b64 {
                    let n = x.as_str().map_or(0, str::len);
                    *x = Value::String(format!("[{n} bytes omitted]"));
                    any = true;
                } else {
                    any |= scrub_raw(x);
                }
            }
            any
        }
        _ => false,
    }
}

/// A `file://` URI's path (percent-decoded), or `None` for other URIs.
pub fn file_uri_path(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("file://")?;
    // `file://host/path` is not ours; `file:///path` and `file:///C:/x` are.
    let rest = rest.strip_prefix("localhost").unwrap_or(rest);
    if !rest.starts_with('/') {
        return None;
    }
    let bytes = rest.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(h) = u8::from_str_radix(&rest[i + 1..i + 3], 16) {
                out.push(h);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    let p = String::from_utf8_lossy(&out).into_owned();
    // `/C:/x` → `C:/x` (Windows drive paths).
    let b = p.as_bytes();
    if b.len() > 3 && b[2] == b':' && b[1].is_ascii_alphabetic() {
        return Some(p[1..].to_string());
    }
    Some(p)
}

/// A `file://` URI for an absolute path.
pub fn path_file_uri(path: &str) -> String {
    let p = path.replace('\\', "/");
    let mut out = String::from("file://");
    if !p.starts_with('/') {
        out.push('/');
    }
    for c in p.chars() {
        match c {
            ' ' => out.push_str("%20"),
            '%' => out.push_str("%25"),
            '#' => out.push_str("%23"),
            '?' => out.push_str("%3F"),
            c => out.push(c),
        }
    }
    out
}

/// The text a prompt carries for files passed by path (drivers whose [`FileInput`] is
/// [`FileInput::PathReference`], and files a driver cannot embed).
pub fn path_reference_text(files: &[&Attachment]) -> String {
    let mut s = String::new();
    for a in files {
        s.push_str(&format!(
            "\n[Attached file: {} ({}, {} bytes) at {}]",
            a.name, a.mime, a.size, a.path
        ));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn png_gif_jpeg_webp_dimensions() {
        let mut png = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        png.extend(16u32.to_be_bytes());
        png.extend(9u32.to_be_bytes());
        assert_eq!(image_dimensions(&png), Some((16, 9)));
        let gif = b"GIF89a\x20\x00\x10\x00rest";
        assert_eq!(image_dimensions(gif), Some((32, 16)));
        let jpeg = [
            0xff, 0xd8, 0xff, 0xe0, 0, 4, 0, 0, 0xff, 0xc0, 0, 17, 8, 0, 20, 0, 40, 3, 0, 0, 0, 0,
        ];
        assert_eq!(image_dimensions(&jpeg), Some((40, 20)));
        assert_eq!(image_dimensions(b"nope"), None);
    }

    #[test]
    fn sniffing_prefers_bytes_and_never_calls_binary_text() {
        assert_eq!(sniff_mime(b"\x89PNG\r\n\x1a\nxxxx", "a.txt"), "image/png");
        assert_eq!(sniff_mime(b"%PDF-1.7", "x"), "application/pdf");
        assert_eq!(sniff_mime(b"<svg xmlns=", "pic"), "image/svg+xml");
        assert_eq!(sniff_mime(b"<svg/>", "pic.svg"), "image/svg+xml");
        assert_eq!(
            sniff_mime(b"\0\x01\x02", "pic.svg"),
            "application/octet-stream"
        );
        assert_eq!(sniff_mime(b"a,b\n1,2", "t.csv"), "text/csv");
        assert_eq!(sniff_mime(b"<!DOCTYPE html><p>", "page"), "text/html");
        assert_eq!(sniff_mime(b"hello", "notes"), "text/plain");
    }

    #[test]
    fn scrub_replaces_media_and_keeps_small_values() {
        let big = "A".repeat(300);
        let mut v = serde_json::json!({
            "type": "image", "source": {"data": big, "media_type": "image/png"},
            "url": format!("data:image/png;base64,{big}"), "ok": "short"
        });
        assert!(scrub_raw(&mut v));
        assert_eq!(v["source"]["data"], "[300 bytes omitted]");
        assert!(v["url"].as_str().unwrap().ends_with("bytes omitted]"));
        assert_eq!(v["ok"], "short");
        assert!(!scrub_raw(&mut serde_json::json!({"data": "small"})));
    }

    #[test]
    fn file_uris_round_trip() {
        assert_eq!(
            file_uri_path("file:///a%20b/c.txt").as_deref(),
            Some("/a b/c.txt")
        );
        assert_eq!(file_uri_path("file:///C:/x/y").as_deref(), Some("C:/x/y"));
        assert_eq!(file_uri_path("https://x/y"), None);
        assert_eq!(path_file_uri("/a b/c#1"), "file:///a%20b/c%231");
        assert_eq!(
            file_uri_path(&path_file_uri("/a b/c#1")).as_deref(),
            Some("/a b/c#1")
        );
    }

    #[test]
    fn parts_serialize_flat_and_tagged() {
        let p = ContentPart::Image {
            mime: "image/png".into(),
            source: MediaSource::Blob {
                id: "ab".into(),
                size: 3,
            },
            width: Some(1),
            height: None,
            alt: None,
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type":"image","mime":"image/png","source":{"source":"blob","id":"ab","size":3},"width":1})
        );
        assert_eq!(serde_json::from_value::<ContentPart>(v).unwrap(), p);
    }
}
