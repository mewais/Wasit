//! Images and files: the per-session blob store, attachment uploads and file reads.
//!
//! * **Blobs.** A protocol reports inline media as [`MediaSource::Inline`]. The pure core
//!   ([`externalize`]) decodes it, names it by the SHA-256 of its bytes and replaces it with
//!   [`MediaSource::Blob`] before the event is published, so events, snapshots and history pages
//!   never hold the bytes. The actor writes each blob once to `<session dir>/blobs/<id>`
//!   ([`write_blob`]). The journal stays the source of truth: a replay derives the same ids,
//!   and the actor's re-read of the journal after a restart rewrites any missing blob.
//! * **Attachments.** `session.attachment.put` uploads a file in chunks to
//!   `<session dir>/attachments/<upload>/<name>`; the answer is the [`Attachment`] a prompt
//!   carries. Before a prompt is journaled the runtime loads the bytes a driver embeds.
//! * **Reads.** `session.file.read` / `session.files.stat` serve blobs, attachments and files
//!   under the session's cwd (anything else only with `outside: true`, which the client sets
//!   when the user explicitly opens another path), with size limits and ranges. Paths are
//!   canonicalised (symlinks resolved) before the root check, so `..` and symlinks cannot
//!   leave the allowed roots.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use wasit_core::content::{b64_decode, b64_encode, image_dimensions, scrub_raw, sniff_mime};
use wasit_core::{AgentEvent, Attachment, ContentPart, GateRequest, MediaSource, PromptInput};

use crate::store;
use crate::wire::{RuntimeError, SessionId};

/// Largest blob kept (a larger inline payload becomes a text note).
pub const MAX_BLOB_BYTES: usize = 32 * 1024 * 1024;
/// Largest `session.file.read` chunk.
pub const MAX_READ_CHUNK: u64 = 4 * 1024 * 1024;
/// Largest `session.attachment.put` chunk (decoded).
pub const MAX_UPLOAD_CHUNK: usize = 1024 * 1024;
/// Largest attachment.
pub const MAX_ATTACHMENT_BYTES: u64 = 50 * 1024 * 1024;
/// Most paths in one `session.files.stat`.
pub const MAX_STAT_PATHS: usize = 200;

// ── wire types ──────────────────────────────────────────────────────────────

/// `session.attachment.put`: one chunk of an upload. Chunks are sent in order; `offset` is
/// where this chunk starts (0 starts the upload, replacing an unfinished one with the same
/// `upload` id) and `last` finishes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct AttachmentPut {
    pub id: SessionId,
    /// Client-chosen upload id: 8–32 characters `[a-z0-9]`.
    pub upload: String,
    pub name: String,
    /// Total size of the file in bytes.
    pub size: u64,
    pub offset: u64,
    /// Base64 bytes of this chunk (at most 1 MiB decoded).
    pub data: String,
    #[serde(default)]
    pub last: bool,
}

/// Result of `session.attachment.put`: the attachment once `last` was written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct AttachmentPutReply {
    /// Bytes received so far.
    pub received: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment: Option<Attachment>,
}

/// `session.file.read`: a byte range of a blob or a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct FileReadRequest {
    pub id: SessionId,
    /// A blob id (see [`MediaSource::Blob`]); or
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    /// a path (absolute, or relative to the session's cwd).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub offset: u64,
    /// At most 4 MiB per call (default: 4 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    /// The user explicitly opened a path outside the session's roots.
    #[serde(default)]
    pub outside: bool,
}

/// Result of `session.file.read`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct FileChunk {
    /// The canonical path read (the blob's path for blobs).
    pub path: String,
    /// Sniffed on the host from the first bytes and the name.
    pub mime: String,
    /// Total size of the file.
    pub size: u64,
    pub offset: u64,
    /// Base64 bytes of the range.
    pub data: String,
    /// The range reaches the end of the file.
    pub eof: bool,
}

/// `session.files.stat`: which of these paths exist (for linking paths mentioned in a
/// transcript). Paths outside the session's roots are reported as missing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct FilesStatRequest {
    pub id: SessionId,
    pub paths: Vec<String>,
}

/// One file of [`FilesStatReply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct FileStat {
    /// The path as asked.
    pub path: String,
    /// Canonical absolute path.
    pub resolved: String,
    pub size: u64,
    pub mime: String,
    pub is_dir: bool,
    /// Modification time (ms since the Unix epoch), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_ms: Option<u64>,
}

/// Result of `session.files.stat`: the paths that exist inside the roots, in request order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts", derive(ts_rs::TS))]
pub struct FilesStatReply {
    pub files: Vec<FileStat>,
}

// ── blobs (pure part) ───────────────────────────────────────────────────────

/// Lowercase hex SHA-256.
pub(crate) fn blob_id(bytes: &[u8]) -> String {
    let d = Sha256::digest(bytes);
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn valid_blob_id(id: &str) -> bool {
    id.len() == 64
        && id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn externalize_part(p: &mut ContentPart, blobs: &mut Vec<(String, Vec<u8>)>) {
    let (source, dims) = match p {
        ContentPart::Image {
            source,
            width,
            height,
            ..
        } => (source, Some((width, height))),
        ContentPart::File { source, .. } => (source, None),
        ContentPart::Text { .. } => return,
    };
    let MediaSource::Inline { data } = source else {
        return;
    };
    let Some(bytes) = b64_decode(data).filter(|b| b.len() <= MAX_BLOB_BYTES) else {
        *p = ContentPart::Text {
            text: format!(
                "[media omitted: {} bytes of invalid or oversized data]",
                data.len()
            ),
        };
        return;
    };
    if let Some((w, h)) = dims {
        if let Some((iw, ih)) = image_dimensions(&bytes) {
            *w = Some(iw);
            *h = Some(ih);
        }
    }
    let id = blob_id(&bytes);
    *source = MediaSource::Blob {
        id: id.clone(),
        size: bytes.len() as u64,
    };
    if !blobs.iter().any(|(b, _)| *b == id) {
        blobs.push((id, bytes));
    }
}

/// Move inline media of `ev` into `blobs` (by content id) and scrub raw JSON values, so the
/// published event is small. Pure and deterministic.
pub(crate) fn externalize(ev: &mut AgentEvent, blobs: &mut Vec<(String, Vec<u8>)>) {
    match ev {
        AgentEvent::AssistantContent { parts, .. } | AgentEvent::UserContent { parts } => {
            parts.iter_mut().for_each(|p| externalize_part(p, blobs));
        }
        AgentEvent::ToolCallStarted { input, .. } => {
            scrub_raw(input);
        }
        AgentEvent::ToolCallUpdated {
            parts,
            content,
            input,
            ..
        } => {
            parts.iter_mut().for_each(|p| externalize_part(p, blobs));
            content.iter_mut().for_each(|c| {
                scrub_raw(c);
            });
            if let Some(i) = input {
                scrub_raw(i);
            }
        }
        AgentEvent::ToolCallFinished { parts, result, .. } => {
            parts.iter_mut().for_each(|p| externalize_part(p, blobs));
            if let Some(r) = result {
                scrub_raw(r);
            }
        }
        AgentEvent::GateRequested {
            gate: GateRequest::Permission { raw_input, .. },
        } => {
            scrub_raw(raw_input);
        }
        AgentEvent::Unknown { raw } => {
            scrub_raw(raw);
        }
        _ => {}
    }
}

/// The command as published: attachment bytes removed.
pub(crate) fn without_bytes(mut cmd: wasit_core::Command) -> wasit_core::Command {
    if let wasit_core::Command::Prompt { attachments, .. } = &mut cmd {
        for a in attachments {
            a.data = None;
        }
    }
    cmd
}

// ── blobs (I/O) ─────────────────────────────────────────────────────────────

type Inflight = Mutex<HashMap<PathBuf, watch::Receiver<bool>>>;

fn inflight() -> &'static Inflight {
    static M: OnceLock<Inflight> = OnceLock::new();
    M.get_or_init(Default::default)
}

fn inflight_lock() -> std::sync::MutexGuard<'static, HashMap<PathBuf, watch::Receiver<bool>>> {
    // Entries are inserted/removed whole; a poisoned map is still consistent.
    inflight().lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn blobs_dir(session_dir: &Path) -> PathBuf {
    session_dir.join("blobs")
}

pub(crate) fn attachments_dir(session_dir: &Path) -> PathBuf {
    session_dir.join("attachments")
}

/// Write a blob in the background (once; an existing blob is left alone). Readers of a blob
/// that is still being written wait for it.
pub(crate) fn write_blob(session_dir: &Path, id: String, bytes: Vec<u8>) {
    let path = blobs_dir(session_dir).join(&id);
    let (tx, rx) = watch::channel(false);
    {
        let mut m = inflight_lock();
        if m.contains_key(&path) {
            return;
        }
        m.insert(path.clone(), rx);
    }
    tokio::spawn(async move {
        if let Err(e) = store_blob(&path, &bytes).await {
            crate::diag::blob_write_failed(&id, &e);
        }
        inflight_lock().remove(&path);
        // Waiting readers may have gone away.
        let _ = tx.send(true);
    });
}

async fn store_blob(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let dir = path.parent().unwrap_or(Path::new("."));
    store::private_dir_all(dir).await?;
    // Unique temporary name: a restore and the actor may store the same blob at once.
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = path.with_extension(format!("{}-{n}.tmp", std::process::id()));
    let mut f = store::open_private(&tmp, |o| {
        o.write(true).truncate(true);
    })
    .await?;
    f.write_all(bytes).await?;
    f.sync_all().await?;
    drop(f);
    match tokio::fs::rename(&tmp, path).await {
        Ok(()) => Ok(()),
        // Content-addressed: whoever stored it first stored the same bytes.
        Err(_) if tokio::fs::try_exists(path).await.unwrap_or(false) => {
            // Best effort: a leftover temporary file is harmless.
            let _ = tokio::fs::remove_file(&tmp).await;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

async fn wait_blob(path: &Path) {
    let rx = inflight_lock().get(path).cloned();
    if let Some(mut rx) = rx {
        // A bounded wait: a stuck writer must not hold a reader forever.
        let wait = rx.wait_for(|done| *done);
        if tokio::time::timeout(std::time::Duration::from_secs(10), wait)
            .await
            .is_err()
        {
            crate::diag::blob_wait_timeout(path);
        }
    }
}

// ── roots and path checks ───────────────────────────────────────────────────

fn io(what: &str, e: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Io(format!("{what}: {e}"))
}

fn invalid(m: impl Into<String>) -> RuntimeError {
    RuntimeError::InvalidRequest(m.into())
}

/// The roots a session's reads are confined to: its cwd, its attachments and its blobs.
pub(crate) struct Roots {
    pub session_dir: PathBuf,
    pub cwd: Option<PathBuf>,
}

impl Roots {
    async fn canonical_roots(&self) -> Vec<PathBuf> {
        let mut v = Vec::new();
        for p in [
            self.cwd.clone(),
            Some(attachments_dir(&self.session_dir)),
            Some(blobs_dir(&self.session_dir)),
        ]
        .into_iter()
        .flatten()
        {
            if let Ok(c) = tokio::fs::canonicalize(&p).await {
                v.push(c);
            }
        }
        v
    }

    /// The canonical path of `p` (relative paths against the cwd) if it exists and lies inside
    /// a root (or `outside` is set). Symlinks are resolved first, so a link pointing out of a
    /// root is outside.
    pub(crate) async fn resolve(&self, p: &str, outside: bool) -> Result<PathBuf, RuntimeError> {
        if p.is_empty() || p.contains('\0') {
            return Err(invalid("empty or invalid path"));
        }
        let raw = PathBuf::from(p);
        let joined = if raw.is_absolute() {
            raw
        } else {
            match &self.cwd {
                Some(c) => c.join(raw),
                None => return Err(invalid("the session has no working directory")),
            }
        };
        let canon = tokio::fs::canonicalize(&joined).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                RuntimeError::InvalidRequest(format!("no such file: {p}"))
            } else {
                io(&format!("resolve {p}"), e)
            }
        })?;
        if outside {
            return Ok(canon);
        }
        if self
            .canonical_roots()
            .await
            .iter()
            .any(|r| canon.starts_with(r))
        {
            Ok(canon)
        } else {
            Err(RuntimeError::InvalidRequest(format!(
                "{p} is outside the session's folder (open it explicitly to read it)"
            )))
        }
    }
}

/// The path of blob `id` if it is stored (waiting for a write in progress); an invalid id is
/// an error.
pub(crate) async fn stored_blob(
    session_dir: &Path,
    id: &str,
) -> Result<Option<PathBuf>, RuntimeError> {
    if !valid_blob_id(id) {
        return Err(invalid("bad blob id"));
    }
    let p = blobs_dir(session_dir).join(id);
    wait_blob(&p).await;
    Ok(tokio::fs::try_exists(&p)
        .await
        .unwrap_or(false)
        .then_some(p))
}

/// Store a blob found again in the journal (awaited, unlike [`write_blob`]).
pub(crate) async fn restore_blob(
    session_dir: &Path,
    id: &str,
    bytes: &[u8],
) -> Result<(), RuntimeError> {
    if blob_id(bytes) != id {
        return Err(RuntimeError::Io(
            "the journal's blob does not match its id".into(),
        ));
    }
    store_blob(&blobs_dir(session_dir).join(id), bytes)
        .await
        .map_err(|e| io("store blob", e))
}

/// Read a range of a blob or file.
pub(crate) async fn read(roots: &Roots, r: &FileReadRequest) -> Result<FileChunk, RuntimeError> {
    let path = match (&r.blob, &r.path) {
        (Some(b), None) => {
            if !valid_blob_id(b) {
                return Err(invalid("bad blob id"));
            }
            let p = blobs_dir(&roots.session_dir).join(b);
            wait_blob(&p).await;
            if !tokio::fs::try_exists(&p).await.unwrap_or(false) {
                return Err(invalid(format!("no such blob: {b}")));
            }
            p
        }
        (None, Some(p)) => roots.resolve(p, r.outside).await?,
        _ => return Err(invalid("give exactly one of blob, path")),
    };
    let mut f = tokio::fs::File::open(&path)
        .await
        .map_err(|e| io(&format!("open {}", path.display()), e))?;
    let meta = f.metadata().await.map_err(|e| io("stat", e))?;
    if meta.is_dir() {
        return Err(invalid(format!("{} is a directory", path.display())));
    }
    let size = meta.len();
    let mut head = vec![0u8; 512.min(size as usize)];
    f.read_exact(&mut head).await.map_err(|e| io("read", e))?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mime = sniff_mime(&head, &name);
    let offset = r.offset.min(size);
    let len = r
        .length
        .unwrap_or(MAX_READ_CHUNK)
        .min(MAX_READ_CHUNK)
        .min(size - offset);
    f.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| io("seek", e))?;
    let mut buf = vec![0u8; len as usize];
    f.read_exact(&mut buf).await.map_err(|e| io("read", e))?;
    Ok(FileChunk {
        path: path.to_string_lossy().into_owned(),
        mime,
        size,
        offset,
        data: b64_encode(&buf),
        eof: offset + len >= size,
    })
}

/// Stat the paths that exist inside the roots.
pub(crate) async fn stat(roots: &Roots, paths: &[String]) -> Result<FilesStatReply, RuntimeError> {
    if paths.len() > MAX_STAT_PATHS {
        return Err(invalid(format!("at most {MAX_STAT_PATHS} paths")));
    }
    let mut files = Vec::new();
    for p in paths {
        let Ok(canon) = roots.resolve(p, false).await else {
            continue;
        };
        let Ok(meta) = tokio::fs::metadata(&canon).await else {
            continue;
        };
        let mime = if meta.is_dir() {
            "inode/directory".to_string()
        } else {
            let mut head = vec![0u8; 512.min(meta.len() as usize)];
            match tokio::fs::File::open(&canon).await {
                Ok(mut f) => {
                    if f.read_exact(&mut head).await.is_err() {
                        head.clear();
                    }
                }
                Err(_) => head.clear(),
            }
            let name = canon
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            sniff_mime(&head, &name)
        };
        files.push(FileStat {
            path: p.clone(),
            resolved: canon.to_string_lossy().into_owned(),
            size: meta.len(),
            mime,
            is_dir: meta.is_dir(),
            modified_ms: meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64),
        });
    }
    Ok(FilesStatReply { files })
}

// ── uploads ─────────────────────────────────────────────────────────────────

fn safe_name(name: &str) -> Result<String, RuntimeError> {
    let base = wasit_core::content::base_name(name);
    let clean: String = base
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let clean = clean.trim().trim_start_matches('.').to_string();
    if clean.is_empty() || clean.len() > 200 {
        return Err(invalid("bad file name"));
    }
    Ok(clean)
}

/// Write one upload chunk; the attachment once the last chunk is in.
pub(crate) async fn put(
    session_dir: &Path,
    r: &AttachmentPut,
) -> Result<AttachmentPutReply, RuntimeError> {
    if !(8..=32).contains(&r.upload.len())
        || !r
            .upload
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err(invalid("bad upload id"));
    }
    if r.size > MAX_ATTACHMENT_BYTES {
        return Err(invalid(format!(
            "the file is larger than {} MiB",
            MAX_ATTACHMENT_BYTES / 1024 / 1024
        )));
    }
    let name = safe_name(&r.name)?;
    let bytes = b64_decode(&r.data).ok_or_else(|| invalid("chunk is not base64"))?;
    if bytes.len() > MAX_UPLOAD_CHUNK {
        return Err(invalid("chunk larger than 1 MiB"));
    }
    if r.offset + bytes.len() as u64 > r.size {
        return Err(invalid("chunk goes past the declared size"));
    }
    let dir = attachments_dir(session_dir).join(&r.upload);
    let part = dir.join(".part");
    if r.offset == 0 {
        store::private_dir_all(&dir)
            .await
            .map_err(|e| io("create attachments dir", e))?;
    }
    let mut f = store::open_private(&part, |o| {
        o.create(r.offset == 0).write(true).truncate(r.offset == 0);
    })
    .await
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            invalid("unknown upload (start at offset 0)")
        } else {
            io("open upload", e)
        }
    })?;
    let have = f.metadata().await.map_err(|e| io("stat upload", e))?.len();
    if have != r.offset {
        return Err(invalid(format!("expected offset {have}, got {}", r.offset)));
    }
    f.seek(std::io::SeekFrom::Start(r.offset))
        .await
        .map_err(|e| io("seek", e))?;
    f.write_all(&bytes)
        .await
        .map_err(|e| io("write upload", e))?;
    let received = r.offset + bytes.len() as u64;
    if !r.last {
        f.flush().await.map_err(|e| io("write upload", e))?;
        return Ok(AttachmentPutReply {
            received,
            attachment: None,
        });
    }
    if received != r.size {
        return Err(invalid(format!(
            "upload ended at {received} of {} bytes",
            r.size
        )));
    }
    f.sync_all().await.map_err(|e| io("sync upload", e))?;
    drop(f);
    let dest = dir.join(&name);
    tokio::fs::rename(&part, &dest)
        .await
        .map_err(|e| io("finish upload", e))?;
    let mut head = vec![0u8; 512.min(r.size as usize)];
    let mut rf = tokio::fs::File::open(&dest)
        .await
        .map_err(|e| io("open upload", e))?;
    rf.read_exact(&mut head)
        .await
        .map_err(|e| io("read upload", e))?;
    Ok(AttachmentPutReply {
        received,
        attachment: Some(Attachment {
            mime: sniff_mime(&head, &name),
            name,
            size: r.size,
            path: dest.to_string_lossy().into_owned(),
            data: None,
        }),
    })
}

/// Fill in the bytes the driver embeds (images it takes; text files when it embeds files),
/// after checking every attachment is an upload of this session and within the driver's
/// limits.
pub(crate) async fn load_attachments(
    session_dir: &Path,
    input: &PromptInput,
    attachments: &mut [Attachment],
) -> Result<(), RuntimeError> {
    if attachments.is_empty() {
        return Ok(());
    }
    if input.max_attachments == 0 {
        return Err(RuntimeError::Unsupported(
            "this agent takes no attachments".into(),
        ));
    }
    if attachments.len() > input.max_attachments as usize {
        return Err(invalid(format!(
            "at most {} attachments",
            input.max_attachments
        )));
    }
    let root = tokio::fs::canonicalize(attachments_dir(session_dir))
        .await
        .map_err(|_| invalid("this session has no uploads"))?;
    let mut budget = wasit_core::content::MAX_EMBEDDED_BYTES;
    for a in attachments.iter_mut() {
        let canon = tokio::fs::canonicalize(&a.path)
            .await
            .map_err(|_| invalid(format!("no such upload: {}", a.name)))?;
        if !canon.starts_with(&root) {
            return Err(invalid(format!(
                "{} is not an upload of this session",
                a.name
            )));
        }
        let size = tokio::fs::metadata(&canon)
            .await
            .map_err(|e| io("stat upload", e))?
            .len();
        a.size = size;
        a.path = canon.to_string_lossy().into_owned();
        let image = a.mime.starts_with("image/");
        if image && !input.accepts_image(&a.mime) {
            if input.files == wasit_core::FileInput::None {
                return Err(RuntimeError::Unsupported(format!(
                    "this agent does not take {} images",
                    a.mime
                )));
            }
        } else if image && size > input.max_image_bytes {
            return Err(invalid(format!(
                "{} is larger than the {} MiB this agent takes",
                a.name,
                input.max_image_bytes / 1024 / 1024
            )));
        }
        if !image || !input.accepts_image(&a.mime) {
            if input.files == wasit_core::FileInput::None {
                return Err(RuntimeError::Unsupported(
                    "this agent takes images only".into(),
                ));
            }
            if size > input.max_file_bytes {
                return Err(invalid(format!("{} is too large", a.name)));
            }
        }
        let embed = input.embeds_bytes
            && ((image && input.accepts_image(&a.mime))
                || (input.files == wasit_core::FileInput::Embedded
                    && wasit_core::content::is_text_mime(&a.mime)
                    && size <= 256 * 1024));
        if embed {
            if size > budget {
                return Err(invalid(
                    "the attachments are too large to send in one prompt",
                ));
            }
            budget -= size;
            let bytes = tokio::fs::read(&canon)
                .await
                .map_err(|e| io("read upload", e))?;
            a.data = Some(b64_encode(&bytes));
        } else {
            a.data = None;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn externalize_is_deterministic_and_strips_bytes() {
        let png = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR\0\0\0\x02\0\0\0\x03rest";
        let data = b64_encode(png);
        let mk = || AgentEvent::ToolCallFinished {
            call_id: "c".into(),
            name: "Read".into(),
            ok: true,
            result: Some(serde_json::json!([{"type":"image","source":{"data": "x".repeat(400)}}])),
            parts: vec![ContentPart::inline_image("image/png", data.clone())],
        };
        let (mut a, mut b) = (mk(), mk());
        let (mut ba, mut bb) = (Vec::new(), Vec::new());
        externalize(&mut a, &mut ba);
        externalize(&mut b, &mut bb);
        assert_eq!(a, b);
        assert_eq!(ba, bb);
        assert_eq!(ba.len(), 1);
        assert_eq!(ba[0].1, png);
        let AgentEvent::ToolCallFinished { parts, result, .. } = a else {
            unreachable!()
        };
        assert_eq!(
            parts[0],
            ContentPart::Image {
                mime: "image/png".into(),
                source: MediaSource::Blob {
                    id: blob_id(png),
                    size: png.len() as u64
                },
                width: Some(2),
                height: Some(3),
                alt: None
            }
        );
        assert_eq!(result.unwrap()[0]["source"]["data"], "[400 bytes omitted]");
    }

    #[test]
    fn invalid_inline_data_becomes_a_note() {
        let mut ev = AgentEvent::AssistantContent {
            parts: vec![ContentPart::inline_image("image/png", "!!not base64!!")],
            message_id: None,
        };
        let mut blobs = Vec::new();
        externalize(&mut ev, &mut blobs);
        assert!(blobs.is_empty());
        let AgentEvent::AssistantContent { parts, .. } = ev else {
            unreachable!()
        };
        assert!(matches!(&parts[0], ContentPart::Text { text } if text.contains("omitted")));
    }

    #[test]
    fn upload_names_are_sanitised() {
        assert_eq!(safe_name("../../etc/passwd").unwrap(), "passwd");
        assert_eq!(safe_name("a:b?.png").unwrap(), "a_b_.png");
        assert!(safe_name("..").is_err());
        assert!(safe_name("").is_err());
    }
}
