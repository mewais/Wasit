//! The conversation export (B5b): a session's derived event log written forward into one file
//! per format under the session's `exports/` directory, with best-effort redaction counted per
//! kind.
//!
//! The records come from the derived log only (never the journal), through a [`LogView`], so no
//! actor is started. Each file is written under a temporary name and renamed into place once every
//! record is written: a file under its final name is complete. A cancelled or failed export
//! removes what it wrote ([`Exporter`]'s drop). Files are owner-only.
//!
//! A job (one export) is kept in memory with its progress and outcome: its progress topic
//! replays them, and `session.export.read` reads its files. The most recent [`MAX_JOBS`] finished
//! jobs are kept, and a session runs one export at a time.
//!
//! JSON: line 1 is the manifest `{format, redaction, session}`; each further line is one
//! [`SessionEvent`] with images replaced by `{"type":"image","mime":…,"omitted":true}`, tool outputs
//! longer than the cap replaced by their prefix and a marker, and every string redacted. Redaction
//! runs before truncation, so a secret is never cut into a fragment the redactor cannot see.
//!
//! Markdown: the prompts, the assistant's prose, the reasoning (in `<details>`), and each tool call
//! with its input and its capped output. Prose and reasoning are escaped (`<` becomes `&lt;`) so
//! the transcript's HTML-looking text does not render as markup. Streamed deltas are the prose. A
//! final `assistant_content` repeats its streamed message, so it is skipped when its `message_id`
//! streamed (or, without an id, when no other event came between the deltas and it). Tool updates
//! are not written: the finished call carries the output.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use base64::Engine;
use futures::stream::{BoxStream, StreamExt};
use serde_json::{json, Value};
use tokio::sync::watch;

use wasit_core::{AgentEvent, Command, ContentPart};

use crate::eventlog::{LogFile, LogView, Records, Scan};
use crate::redact::{self, RedactKind, RedactionCounts};
use crate::stream::StreamEnd;
use crate::wire::{
    ExportChunk, ExportEvent, ExportFile, ExportFormat, ExportRequest, HistoryScope, RuntimeError,
    SessionEvent, SessionEventKind, SessionId, SessionSnapshot, MAX_EXPORT_READ,
    MAX_TOOL_OUTPUT_CAP,
};

/// The format and redaction versions of the JSON manifest.
const FORMAT_VERSION: u32 = 1;
const REDACTION_VERSION: u32 = 1;
/// Records between two progress events.
const PROGRESS_EVERY: u64 = 1000;
/// Finished jobs kept for their files and topics; older ones are forgotten.
const MAX_JOBS: usize = 64;
/// The directory under the session's directory that holds the exports.
const EXPORTS_DIR: &str = "exports";

/// Why an export stopped short.
#[derive(Debug)]
pub(crate) enum Failure {
    Cancelled,
    Failed(String),
}

impl From<io::Error> for Failure {
    fn from(e: io::Error) -> Self {
        Failure::Failed(e.to_string())
    }
}

impl From<serde_json::Error> for Failure {
    fn from(e: serde_json::Error) -> Self {
        Failure::Failed(e.to_string())
    }
}

impl Failure {
    /// The reason a `failed` event carries.
    fn reason(&self) -> String {
        match self {
            Failure::Cancelled => "cancelled".to_owned(),
            Failure::Failed(why) => why.clone(),
        }
    }
}

/// The header of a session as the export writes it.
#[derive(Debug, Clone)]
pub(crate) struct Header {
    pub(crate) id: String,
    pub(crate) driver: String,
    pub(crate) model: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) created: u64,
}

impl Header {
    pub(crate) fn of(s: &SessionSnapshot) -> Header {
        Header {
            id: s.id.to_string(),
            driver: s.driver.to_string(),
            model: s.model.clone(),
            title: s.title.clone(),
            created: s.created_at_ms,
        }
    }
}

/// What an export writes: its formats, which events, and the tool-output cap.
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    formats: Vec<ExportFormat>,
    scope: HistoryScope,
    include_earlier: bool,
    cap: usize,
}

impl Plan {
    /// The plan of a request, or why the request is invalid.
    pub(crate) fn new(r: &ExportRequest) -> Result<Plan, RuntimeError> {
        let invalid = |why: &str| RuntimeError::InvalidRequest(why.to_owned());
        if r.formats.is_empty() {
            return Err(invalid("give at least one export format"));
        }
        for (i, f) in r.formats.iter().enumerate() {
            if r.formats[..i].contains(f) {
                return Err(invalid("each export format may be given once"));
            }
        }
        if r.tool_output_cap == 0 || r.tool_output_cap > MAX_TOOL_OUTPUT_CAP {
            return Err(RuntimeError::InvalidRequest(format!(
                "tool_output_cap must be 1..={MAX_TOOL_OUTPUT_CAP}"
            )));
        }
        Ok(Plan {
            formats: r.formats.clone(),
            scope: if r.include_subagents {
                HistoryScope::All
            } else {
                HistoryScope::Main
            },
            include_earlier: r.include_earlier,
            cap: r.tool_output_cap as usize,
        })
    }
}

/// Redactions made by an export, summed over the records it wrote.
#[derive(Debug, Default)]
pub(crate) struct Tally(BTreeMap<RedactKind, u64>);

impl Tally {
    fn add(&mut self, counts: &RedactionCounts) {
        for (kind, n) in counts.iter() {
            *self.0.entry(kind).or_insert(0) += n;
        }
    }

    fn wire(&self) -> BTreeMap<String, u64> {
        self.0
            .iter()
            .map(|(kind, n)| (kind.as_str().to_owned(), *n))
            .collect()
    }
}

fn redact_str(text: &str, tally: &mut Tally) -> String {
    let (out, counts) = redact::redact_text(text);
    tally.add(&counts);
    out
}

/// Prose as Markdown: escaped, then redacted (the redaction markers keep their `<`).
fn prose(text: &str, tally: &mut Tally) -> String {
    redact_str(&text.replace('<', "&lt;"), tally)
}

/// `text` cut to at most `cap` bytes on a character boundary, with a marker of what was cut.
fn cap_text(text: String, cap: usize) -> String {
    if text.len() <= cap {
        return text;
    }
    let mut cut = cap;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    format!(
        "{}\n[truncated: showing {cut} of {} bytes]",
        &text[..cut],
        text.len()
    )
}

/// A code fence longer than any backtick run in `body`, so the body cannot close it.
fn fence(body: &str) -> String {
    let (mut longest, mut run) = (0, 0);
    for c in body.chars() {
        run = if c == '`' { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    "`".repeat((longest + 1).max(3))
}

/// The NDJSON manifest line (its strings redacted).
fn manifest(h: &Header, tally: &mut Tally) -> Value {
    let v = json!({
        "format": FORMAT_VERSION,
        "redaction": REDACTION_VERSION,
        "session": {
            "id": h.id,
            "driver": h.driver,
            "model": h.model,
            "title": h.title,
            "created": h.created,
        },
    });
    let (v, counts) = redact::redact_json(&v);
    tally.add(&counts);
    v
}

/// The JSON line of one event: images omitted, every string redacted, tool output capped.
fn event_json(event: &SessionEvent, cap: usize, tally: &mut Tally) -> serde_json::Result<Value> {
    let mut v = serde_json::to_value(event)?;
    omit_images(&mut v);
    let (mut v, counts) = redact::redact_json(&v);
    tally.add(&counts);
    cap_tool_output(&mut v, cap);
    Ok(v)
}

fn is_image(v: &Value) -> bool {
    v.get("type") == Some(&json!("image")) && v.get("source").is_some()
}

/// Every image part of `v` becomes `{"type":"image","mime":…,"omitted":true}`.
fn omit_images(v: &mut Value) {
    if is_image(v) {
        let mime = v.get("mime").cloned().unwrap_or(Value::Null);
        *v = json!({"type": "image", "mime": mime, "omitted": true});
        return;
    }
    match v {
        Value::Array(items) => items.iter_mut().for_each(omit_images),
        Value::Object(map) => map.values_mut().for_each(omit_images),
        _ => {}
    }
}

/// The agent event of a `kind: agent` record, looking through history and subagent wrappers.
fn agent_node(v: &mut Value) -> Option<&mut Value> {
    if v.get("kind")?.as_str()? != "agent" {
        return None;
    }
    let mut node = v.get_mut("event")?;
    while matches!(
        node.get("type").and_then(Value::as_str),
        Some("history" | "scoped")
    ) {
        node = node.get_mut("event")?;
    }
    Some(node)
}

/// Caps the outputs of a tool call: a finished call's `result` and each updated call's `content`
/// block.
fn cap_tool_output(v: &mut Value, cap: usize) {
    let Some(node) = agent_node(v) else { return };
    match node.get("type").and_then(Value::as_str) {
        Some("tool_call_finished") => {
            if let Some(result) = node.get_mut("result") {
                cap_value(result, cap);
            }
        }
        Some("tool_call_updated") => {
            if let Some(Value::Array(blocks)) = node.get_mut("content") {
                blocks.iter_mut().for_each(|b| cap_value(b, cap));
            }
        }
        _ => {}
    }
}

fn cap_value(v: &mut Value, cap: usize) {
    let text = match &*v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if text.len() > cap {
        *v = Value::String(cap_text(text, cap));
    }
}

/// The Markdown projection of the events, written in order.
#[derive(Debug)]
pub(crate) struct Markdown {
    cap: usize,
    run: Run,
    /// Whether the last event was an assistant delta (an id-less `assistant_content` after it
    /// repeats the streamed message).
    streamed_now: bool,
    /// The message ids that streamed and have no `assistant_content` yet.
    streamed_ids: BTreeSet<String>,
    pub(crate) tally: Tally,
}

/// The text being collected into one block.
#[derive(Debug)]
enum Run {
    Idle,
    Prose(String),
    Thinking(String),
}

impl Markdown {
    pub(crate) fn new(cap: usize) -> Markdown {
        Markdown {
            cap,
            run: Run::Idle,
            streamed_now: false,
            streamed_ids: BTreeSet::new(),
            tally: Tally::default(),
        }
    }

    /// The header: the session, and a note that the file holds conversation content and that
    /// redaction is best effort.
    pub(crate) fn header(&mut self, h: &Header, out: &mut String) {
        let title = h.title.as_deref().unwrap_or(&h.id).replace('\n', " ");
        out.push_str(&format!("# {}\n\n", prose(&title, &mut self.tally)));
        out.push_str(&format!("- Session: `{}`\n", h.id));
        out.push_str(&format!("- Driver: {}\n", h.driver));
        let model = h.model.as_deref().unwrap_or("unknown");
        out.push_str(&format!("- Model: {}\n", prose(model, &mut self.tally)));
        out.push_str(&format!(
            "- Created: {} (ms since the Unix epoch)\n\n",
            h.created
        ));
        out.push_str(
            "> This file contains the conversation itself. Secrets are redacted on a best-effort \
             basis only: review it before sharing.\n\n",
        );
    }

    /// Writes what `event` contributes to `out`.
    pub(crate) fn event(&mut self, event: &SessionEvent, out: &mut String) {
        match &event.kind {
            SessionEventKind::Input {
                command: Command::Prompt { text, .. },
                ..
            } => {
                self.flush(out);
                self.user(text, out);
            }
            SessionEventKind::Agent { event } => self.agent(event, out),
            _ => {}
        }
    }

    /// Writes the pending block, if any. Call once after the last event.
    pub(crate) fn end(&mut self, out: &mut String) {
        self.flush(out);
    }

    fn agent(&mut self, event: &AgentEvent, out: &mut String) {
        match event {
            AgentEvent::History { event, .. } | AgentEvent::Scoped { event, .. } => {
                self.agent(event, out)
            }
            AgentEvent::AssistantDelta { text, message_id } => {
                self.streamed_now = true;
                if let Some(id) = message_id {
                    self.streamed_ids.insert(id.clone());
                }
                self.append(text, out, true);
            }
            AgentEvent::AssistantContent { parts, message_id } => {
                let streamed = match message_id {
                    Some(id) => self.streamed_ids.remove(id),
                    None => self.streamed_now,
                };
                self.streamed_now = false;
                self.flush(out);
                if let (false, Some(text)) = (streamed, text_of(parts)) {
                    self.assistant(&text, out);
                }
            }
            other => {
                self.streamed_now = false;
                self.other(other, out);
            }
        }
    }

    /// The events other than assistant prose and its history wrappers.
    fn other(&mut self, event: &AgentEvent, out: &mut String) {
        match event {
            AgentEvent::UserMessage { text } => {
                self.flush(out);
                self.user(text, out);
            }
            AgentEvent::UserContent { parts } => {
                self.flush(out);
                if let Some(text) = text_of(parts) {
                    self.user(&text, out);
                }
            }
            AgentEvent::ThinkingDelta { text, .. } => self.append(text, out, false),
            AgentEvent::ToolCallStarted { name, input, .. } => {
                self.flush(out);
                let body = serde_json::to_string_pretty(input).unwrap_or_default();
                let body = redact_str(&body, &mut self.tally);
                let f = fence(&body);
                out.push_str(&format!(
                    "### Tool: {}\n\nInput:\n\n{f}json\n{body}\n{f}\n\n",
                    redact_str(name, &mut self.tally)
                ));
            }
            AgentEvent::ToolCallFinished {
                name, ok, result, ..
            } => {
                self.flush(out);
                let text = match result {
                    Some(Value::String(s)) => s.clone(),
                    Some(other) => serde_json::to_string_pretty(other).unwrap_or_default(),
                    None => String::new(),
                };
                let text = cap_text(redact_str(&text, &mut self.tally), self.cap);
                let status = if *ok { "ok" } else { "failed" };
                let f = fence(&text);
                out.push_str(&format!(
                    "Output of `{}` ({status}):\n\n{f}\n{text}\n{f}\n\n",
                    redact_str(name, &mut self.tally)
                ));
            }
            _ => {}
        }
    }

    /// Adds `text` to the pending run of the kind (prose or reasoning), starting a new run when
    /// the pending one is of the other kind.
    fn append(&mut self, text: &str, out: &mut String, prose_run: bool) {
        let same = matches!(
            (&self.run, prose_run),
            (Run::Prose(_), true) | (Run::Thinking(_), false)
        );
        if !same {
            self.flush(out);
            self.run = if prose_run {
                Run::Prose(String::new())
            } else {
                Run::Thinking(String::new())
            };
        }
        match &mut self.run {
            Run::Prose(s) | Run::Thinking(s) => s.push_str(text),
            Run::Idle => {}
        }
    }

    fn flush(&mut self, out: &mut String) {
        match std::mem::replace(&mut self.run, Run::Idle) {
            Run::Idle => {}
            Run::Prose(text) => self.assistant(&text, out),
            Run::Thinking(text) => {
                let body = prose(&text, &mut self.tally);
                out.push_str(&format!(
                    "<details><summary>Thinking</summary>\n\n{body}\n\n</details>\n\n"
                ));
            }
        }
    }

    fn user(&mut self, text: &str, out: &mut String) {
        let body = prose(text, &mut self.tally);
        out.push_str(&format!("## User\n\n{body}\n\n"));
    }

    fn assistant(&mut self, text: &str, out: &mut String) {
        let body = prose(text, &mut self.tally);
        out.push_str(&format!("## Assistant\n\n{body}\n\n"));
    }
}

/// The text parts of a message joined, `None` when it has none.
fn text_of(parts: &[ContentPart]) -> Option<String> {
    let texts: Vec<&str> = parts
        .iter()
        .filter_map(|p| match p {
            ContentPart::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    (!texts.is_empty()).then(|| texts.join("\n\n"))
}

/// One output file: its temporary name while it is written, its final name once committed.
#[derive(Debug)]
struct Target {
    format: ExportFormat,
    tmp: PathBuf,
    dest: PathBuf,
    out: Option<BufWriter<File>>,
    bytes: u64,
}

impl Target {
    fn create(dir: &Path, id: &str, format: ExportFormat) -> io::Result<Target> {
        let ext = match format {
            ExportFormat::Json => "ndjson",
            ExportFormat::Markdown => "md",
        };
        let tmp = dir.join(format!(".export-{id}.{ext}.tmp"));
        let dest = dir.join(format!("export-{id}.{ext}"));
        let file = create_private(&tmp)?;
        Ok(Target {
            format,
            tmp,
            dest,
            out: Some(BufWriter::new(file)),
            bytes: 0,
        })
    }

    fn write(&mut self, text: &str) -> io::Result<()> {
        let out = self
            .out
            .as_mut()
            .ok_or_else(|| io::Error::other("the export file is already closed"))?;
        out.write_all(text.as_bytes())?;
        self.bytes += text.len() as u64;
        Ok(())
    }
}

/// Creates a new file readable and writable by its owner only.
fn create_private(path: &Path) -> io::Result<File> {
    let mut o = OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    o.open(path)
}

fn create_private_dir(dir: &Path) -> io::Result<()> {
    let mut b = fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    b.create(dir)
}

/// The writer of one export: every record of the log goes through [`Exporter::record`] into each
/// requested file. Dropped before a commit (a cancel, a failure), it removes its files.
pub(crate) struct Exporter {
    job: Arc<Job>,
    scope: HistoryScope,
    cap: usize,
    json: Option<Target>,
    json_tally: Tally,
    markdown: Option<(Target, Markdown)>,
    events_done: u64,
    committed: bool,
}

impl Exporter {
    /// Creates the files of `plan` under `dir` and writes their headers.
    fn begin(job: Arc<Job>, plan: &Plan, header: &Header, dir: &Path) -> io::Result<Exporter> {
        create_private_dir(dir)?;
        let mut ex = Exporter {
            job,
            scope: plan.scope.clone(),
            cap: plan.cap,
            json: None,
            json_tally: Tally::default(),
            markdown: None,
            events_done: 0,
            committed: false,
        };
        let id = ex.job.id.clone();
        for format in &plan.formats {
            match format {
                ExportFormat::Json => {
                    let line = manifest(header, &mut ex.json_tally);
                    let target = ex
                        .json
                        .insert(Target::create(dir, &id, ExportFormat::Json)?);
                    let text = serde_json::to_string(&line).map_err(io::Error::other)?;
                    target.write(&format!("{text}\n"))?;
                }
                ExportFormat::Markdown => {
                    let (target, md) = ex.markdown.insert((
                        Target::create(dir, &id, ExportFormat::Markdown)?,
                        Markdown::new(plan.cap),
                    ));
                    let mut text = String::new();
                    md.header(header, &mut text);
                    target.write(&text)?;
                }
            }
        }
        Ok(ex)
    }

    /// Writes one record (it is left out of the files when the scope does not admit it).
    fn record(&mut self, item: io::Result<(SessionEvent, u64)>) -> Result<(), Failure> {
        let (event, _) = item?;
        if self.job.cancelled() {
            return Err(Failure::Cancelled);
        }
        self.events_done += 1;
        if self.scope.admits(&event) {
            if let Some(t) = self.json.as_mut() {
                let line = event_json(&event, self.cap, &mut self.json_tally)?;
                t.write(&format!("{}\n", serde_json::to_string(&line)?))?;
            }
            if let Some((t, md)) = self.markdown.as_mut() {
                let mut text = String::new();
                md.event(&event, &mut text);
                t.write(&text)?;
            }
        }
        if self.events_done.is_multiple_of(PROGRESS_EVERY) {
            self.job.publish(ExportEvent::Progress {
                events_done: self.events_done,
                bytes: self.bytes(),
            });
        }
        Ok(())
    }

    fn bytes(&self) -> u64 {
        self.json.as_ref().map_or(0, |t| t.bytes)
            + self.markdown.as_ref().map_or(0, |(t, _)| t.bytes)
    }

    /// The redactions reported: the JSON pass's when JSON was written, else the Markdown's.
    fn redactions(&self) -> BTreeMap<String, u64> {
        match (&self.json, &self.markdown) {
            (Some(_), _) => self.json_tally.wire(),
            (None, Some((_, md))) => md.tally.wire(),
            (None, None) => BTreeMap::new(),
        }
    }

    /// Completes the files and renames them into place. A cancel that arrived before this point
    /// wins: nothing is committed.
    fn commit(mut self) -> Result<Committed, Failure> {
        if let Some((t, md)) = self.markdown.as_mut() {
            let mut text = String::new();
            md.end(&mut text);
            t.write(&text)?;
        }
        if self.job.cancelled() {
            return Err(Failure::Cancelled);
        }
        let redactions = self.redactions();
        let mut files = Vec::new();
        let targets = self
            .json
            .iter_mut()
            .chain(self.markdown.iter_mut().map(|(t, _)| t));
        for t in targets {
            if let Some(w) = t.out.take() {
                let file = w.into_inner().map_err(|e| e.into_error())?;
                file.sync_all()?;
            }
            fs::rename(&t.tmp, &t.dest)?;
            files.push(ExportFile {
                format: t.format,
                path: t.dest.display().to_string(),
                bytes: t.bytes,
            });
        }
        self.committed = true;
        Ok(Committed { files, redactions })
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let targets = self.json.iter().chain(self.markdown.iter().map(|(t, _)| t));
        for t in targets {
            let _ = fs::remove_file(&t.tmp);
            let _ = fs::remove_file(&t.dest);
        }
    }
}

/// A committed export: its files and the redactions made.
#[derive(Debug)]
pub(crate) struct Committed {
    files: Vec<ExportFile>,
    redactions: BTreeMap<String, u64>,
}

/// Feeds the records of one file of the log to the exporter; hands the exporter back.
fn consume(mut ex: Exporter, records: Records) -> Result<Exporter, Failure> {
    for record in records {
        ex.record(record)?;
    }
    Ok(ex)
}

/// One pass over one file of the log, forward from its start.
async fn pass(view: &LogView, file: LogFile, ex: Exporter) -> Result<Exporter, Failure> {
    view.scan_records(file, Scan::Forward { from: None }, move |records| {
        Ok(consume(ex, records))
    })
    .await
    .map_err(|e| Failure::Failed(format!("read the event log: {e}")))?
}

/// Runs an export to completion (or until it is cancelled or fails). The log is settled first.
pub(crate) async fn run(
    job: Arc<Job>,
    view: LogView,
    plan: Plan,
    header: Header,
    dir: PathBuf,
) -> Result<Committed, Failure> {
    view.settled()
        .await
        .map_err(|e| Failure::Failed(format!("read the event log: {e}")))?;
    let exports = dir.join(EXPORTS_DIR);
    let mut ex = Exporter::begin(job, &plan, &header, &exports)
        .map_err(|e| Failure::Failed(format!("create the export files: {e}")))?;
    if plan.include_earlier {
        ex = pass(&view, LogFile::Earlier, ex).await?;
    }
    ex = pass(&view, LogFile::Events, ex).await?;
    tokio::task::spawn_blocking(move || ex.commit())
        .await
        .expect("spawn_blocking panicked")
}

/// One export as the registry keeps it: its id, its session, its progress and outcome.
pub(crate) struct Job {
    pub(crate) id: String,
    session: SessionId,
    cancel: AtomicBool,
    state: Mutex<State>,
    /// The number of events published so far; a subscriber waits on it for more.
    ticks: watch::Sender<u64>,
}

#[derive(Default)]
struct State {
    events: Vec<ExportEvent>,
    files: Vec<ExportFile>,
    finished: bool,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // The state is only ever extended whole; a poisoned lock still holds consistent data.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Job {
    fn new(id: String, session: SessionId) -> Arc<Job> {
        Arc::new(Job {
            id,
            session,
            cancel: AtomicBool::new(false),
            state: Mutex::new(State::default()),
            ticks: watch::channel(0).0,
        })
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    fn publish(&self, event: ExportEvent) {
        let n = {
            let mut st = lock(&self.state);
            st.events.push(event);
            st.events.len() as u64
        };
        self.ticks.send_replace(n);
    }

    /// Records the outcome: the terminal event, and the files a read can serve.
    pub(crate) fn finish(&self, outcome: Result<Committed, Failure>) {
        let n = {
            let mut st = lock(&self.state);
            let event = match outcome {
                Ok(c) => {
                    st.files = c.files.clone();
                    ExportEvent::Done {
                        files: c.files,
                        redactions: c.redactions,
                    }
                }
                Err(f) => ExportEvent::Failed { reason: f.reason() },
            };
            st.events.push(event);
            st.finished = true;
            st.events.len() as u64
        };
        self.ticks.send_replace(n);
    }

    fn finished(&self) -> bool {
        lock(&self.state).finished
    }

    /// The file a read is served from: the one of `format`, or the only file.
    pub(crate) fn file(&self, format: Option<ExportFormat>) -> Result<ExportFile, RuntimeError> {
        let st = lock(&self.state);
        if !st.finished {
            return Err(RuntimeError::InvalidState(format!(
                "export {} is not finished",
                self.id
            )));
        }
        if st.files.is_empty() {
            return Err(RuntimeError::InvalidState(format!(
                "export {} wrote no files",
                self.id
            )));
        }
        match format {
            Some(f) => st
                .files
                .iter()
                .find(|x| x.format == f)
                .cloned()
                .ok_or_else(|| {
                    RuntimeError::InvalidRequest(format!("export {} wrote no {f:?} file", self.id))
                }),
            None if st.files.len() == 1 => Ok(st.files[0].clone()),
            None => Err(RuntimeError::InvalidRequest(format!(
                "export {} wrote {} files: give the format",
                self.id,
                st.files.len()
            ))),
        }
    }

    /// Asks the export to stop at its next record (a finished job ignores it).
    pub(crate) fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    pub(crate) fn session(&self) -> &SessionId {
        &self.session
    }
}

/// The exports of a manager: their jobs, by id.
#[derive(Default)]
pub(crate) struct Exports {
    jobs: Mutex<BTreeMap<String, Arc<Job>>>,
}

impl Exports {
    /// A new job for `session`, unless one of its exports is still running.
    pub(crate) fn start(&self, session: &SessionId) -> Result<Arc<Job>, RuntimeError> {
        let mut jobs = lock(&self.jobs);
        for job in jobs.values() {
            if job.session() == session && !job.finished() {
                return Err(RuntimeError::Busy(format!(
                    "session {session} already has an export running"
                )));
            }
        }
        let excess = (jobs.len() + 1).saturating_sub(MAX_JOBS);
        let finished: Vec<String> = jobs
            .iter()
            .filter(|(_, j)| j.finished())
            .map(|(id, _)| id.clone())
            .take(excess)
            .collect();
        for id in finished {
            jobs.remove(&id);
        }
        let id = format!("{:016x}", fastrand::u64(..));
        let job = Job::new(id.clone(), session.clone());
        jobs.insert(id, job.clone());
        Ok(job)
    }

    pub(crate) fn get(&self, id: &str) -> Option<Arc<Job>> {
        lock(&self.jobs).get(id).cloned()
    }
}

/// The events of a job from `from` (exclusive; from the start when `None`), each with its index
/// as cursor. Ends after the terminal event.
pub(crate) fn stream(
    job: Arc<Job>,
    from: Option<u64>,
) -> BoxStream<'static, Result<(u64, ExportEvent), StreamEnd>> {
    let rx = job.ticks.subscribe();
    let next = from.map_or(0, |c| c.saturating_add(1));
    futures::stream::unfold((job, next, rx), |(job, idx, mut rx)| async move {
        loop {
            let (event, finished) = {
                let st = lock(&job.state);
                (st.events.get(idx as usize).cloned(), st.finished)
            };
            if let Some(event) = event {
                return Some((Ok((idx, event)), (job, idx + 1, rx)));
            }
            if finished || rx.changed().await.is_err() {
                return None;
            }
        }
    })
    .boxed()
}

/// `len` bytes of `path` from `offset`, base64. Reads at most [`MAX_EXPORT_READ`] bytes.
pub(crate) fn read_chunk(path: &Path, offset: u64, len: u32) -> Result<ExportChunk, RuntimeError> {
    let io_err = |e: io::Error| RuntimeError::Io(format!("read {}: {e}", path.display()));
    if len == 0 || len > MAX_EXPORT_READ {
        return Err(RuntimeError::InvalidRequest(format!(
            "len must be 1..={MAX_EXPORT_READ}"
        )));
    }
    let mut file = File::open(path).map_err(io_err)?;
    let total = file.metadata().map_err(io_err)?.len();
    if offset > total {
        return Err(RuntimeError::InvalidRequest(format!(
            "offset {offset} is past the end of the export ({total} bytes)"
        )));
    }
    let n = (total - offset).min(u64::from(len)) as usize;
    file.seek(SeekFrom::Start(offset)).map_err(io_err)?;
    let mut buf = vec![0u8; n];
    file.read_exact(&mut buf).map_err(io_err)?;
    Ok(ExportChunk {
        offset,
        total,
        eof: offset + n as u64 >= total,
        data: base64::engine::general_purpose::STANDARD.encode(buf),
    })
}
