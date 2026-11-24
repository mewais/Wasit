use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, watch};

use wasit_shim::journal::sorted_ndjson;

#[cfg(test)]
use crate::core::{blank_snapshot, Conversation, SessionMeta};
#[cfg(test)]
use crate::wire::{event_cursor, NoticeCode, Phase, SessionEventKind, SessionId};
use crate::wire::{SessionEvent, SessionSnapshot};

const FORMAT: u32 = 1;
const META_FILE: &str = "meta.json";
const EVENTS_FILE: &str = "events.ndjson";
const EARLIER_FILE: &str = "earlier.ndjson";
const CURSOR_PREFIX: &[u8] = b"{\"cursor\":";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogState {
    Building,
    Ready,
    Failed(String),
}

pub struct EventLog {
    paths: Paths,
    tx: mpsc::UnboundedSender<LogMsg>,
    state: watch::Receiver<LogState>,
    written: watch::Receiver<u64>,
    submitted: Arc<AtomicU64>,
}

#[derive(Debug)]
enum LogMsg {
    Append {
        events: Vec<SessionEvent>,
        earlier: Vec<SessionEvent>,
    },
    Seal {
        snapshot: Box<SessionSnapshot>,
        journal_len: u64,
        last_seq: u64,
    },
    Unseal,
    Reset,
}

#[derive(Clone)]
struct Paths {
    dir: PathBuf,
    events: PathBuf,
    earlier: PathBuf,
    meta: PathBuf,
}

impl Paths {
    fn new(dir: &Path) -> Self {
        let dir = dir.join("derived");
        Paths {
            events: dir.join(EVENTS_FILE),
            earlier: dir.join(EARLIER_FILE),
            meta: dir.join(META_FILE),
            dir,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Meta {
    format: u32,
    build: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal: Option<Seal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Seal {
    journal_len: u64,
    last_seq: u64,
    snapshot: Box<SessionSnapshot>,
}

#[derive(Debug)]
struct RecoverOutcome {
    meta: Meta,
    state: LogState,
    written: u64,
}

impl EventLog {
    pub async fn open(
        session_dir: &Path,
        build: impl Into<String>,
        journal_len: u64,
        last_seq: Option<u64>,
    ) -> io::Result<Self> {
        let build = build.into();
        let paths = Paths::new(session_dir);
        let outcome = tokio::task::spawn_blocking({
            let paths = paths.clone();
            let build = build.clone();
            move || recover(&paths, &build, journal_len, last_seq)
        })
        .await
        .expect("spawn_blocking panicked")?;

        let submitted = Arc::new(AtomicU64::new(outcome.written));
        let (state_tx, state) = watch::channel(outcome.state.clone());
        let (written_tx, written) = watch::channel(outcome.written);
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = LogWriter {
            paths: paths.clone(),
            meta: outcome.meta,
            state: state_tx,
            written: written_tx,
            written_count: outcome.written,
            submitted: submitted.clone(),
        };
        tokio::spawn(writer.run(rx));

        Ok(EventLog {
            paths,
            tx,
            state,
            written,
            submitted,
        })
    }

    pub fn state(&self) -> watch::Receiver<LogState> {
        self.state.clone()
    }

    pub fn written(&self) -> watch::Receiver<u64> {
        self.written.clone()
    }

    pub fn submitted(&self) -> u64 {
        self.submitted.load(Ordering::Relaxed)
    }

    pub fn append(&self, events: Vec<SessionEvent>, earlier: Vec<SessionEvent>) -> io::Result<()> {
        if events.is_empty() && earlier.is_empty() {
            return Ok(());
        }
        let added = (events.len() + earlier.len()) as u64;
        self.submitted.fetch_add(added, Ordering::Relaxed);
        self.send(LogMsg::Append { events, earlier })
    }

    pub fn seal(
        &self,
        snapshot: SessionSnapshot,
        journal_len: u64,
        last_seq: u64,
    ) -> io::Result<()> {
        self.send(LogMsg::Seal {
            snapshot: Box::new(snapshot),
            journal_len,
            last_seq,
        })
    }

    pub fn unseal(&self) -> io::Result<()> {
        self.send(LogMsg::Unseal)
    }

    pub fn reset(&self) -> io::Result<()> {
        self.submitted.store(0, Ordering::Relaxed);
        self.send(LogMsg::Reset)
    }

    pub async fn page_back<F>(
        &self,
        before: Option<u64>,
        limit: usize,
        max_bytes: Option<u64>,
        filter: F,
    ) -> io::Result<(Vec<SessionEvent>, bool)>
    where
        F: Fn(&SessionEvent) -> bool + Send + Sync + 'static,
    {
        if let LogState::Failed(reason) = self.state.borrow().clone() {
            return Err(io::Error::other(reason));
        }
        let path = self.paths.events.clone();
        let max_bytes = max_bytes.unwrap_or(u64::MAX);
        tokio::task::spawn_blocking(move || {
            page_back_blocking(path, before, limit, max_bytes, filter)
        })
        .await
        .expect("spawn_blocking panicked")
    }

    pub fn stream_between(
        &self,
        after: Option<u64>,
        below: u64,
    ) -> mpsc::Receiver<io::Result<SessionEvent>> {
        let path = self.paths.events.clone();
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            let start = tokio::task::spawn_blocking({
                let path = path.clone();
                move || stream_start(&path, after)
            })
            .await;
            let (start, good) = match start {
                Ok(Ok(bounds)) => bounds,
                Ok(Err(e)) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(io::Error::other(e.to_string()))).await;
                    return;
                }
            };
            let mut file = match OpenOptions::new().read(true).open(&path).await {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            if let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await {
                let _ = tx.send(Err(e)).await;
                return;
            }
            let mut reader = BufReader::new(file);
            let mut buf = String::new();
            let mut offset = start;
            loop {
                buf.clear();
                let n = match reader.read_line(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n as u64,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                offset += n;
                if offset > good {
                    break;
                }
                if buf.is_empty() {
                    continue;
                }
                let event: SessionEvent =
                    match serde_json::from_slice(buf.trim_end_matches('\n').as_bytes()) {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = tx.send(Err(json_err(e))).await;
                            return;
                        }
                    };
                if event.cursor >= below {
                    break;
                }
                if after.is_none_or(|a| event.cursor > a) && tx.send(Ok(event)).await.is_err() {
                    return;
                }
            }
        });
        rx
    }

    fn send(&self, msg: LogMsg) -> io::Result<()> {
        self.tx
            .send(msg)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "log writer stopped"))
    }
}

struct LogWriter {
    paths: Paths,
    meta: Meta,
    state: watch::Sender<LogState>,
    written: watch::Sender<u64>,
    written_count: u64,
    submitted: Arc<AtomicU64>,
}

impl LogWriter {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<LogMsg>) {
        while let Some(msg) = rx.recv().await {
            let res = match msg {
                LogMsg::Append { events, earlier } => self.append(events, earlier).await,
                LogMsg::Seal {
                    snapshot,
                    journal_len,
                    last_seq,
                } => self.seal(*snapshot, journal_len, last_seq).await,
                LogMsg::Unseal => self.unseal().await,
                LogMsg::Reset => self.reset().await,
            };
            if let Err(e) = res {
                let _ = self.state.send_replace(LogState::Failed(e.to_string()));
                break;
            }
        }
    }

    async fn append(
        &mut self,
        events: Vec<SessionEvent>,
        earlier: Vec<SessionEvent>,
    ) -> io::Result<()> {
        let mut wrote = 0u64;
        if !events.is_empty() {
            append_lines(&self.paths.events, &events).await?;
            wrote += events.len() as u64;
        }
        if !earlier.is_empty() {
            append_lines(&self.paths.earlier, &earlier).await?;
            wrote += earlier.len() as u64;
        }
        if wrote > 0 {
            self.written_count += wrote;
            let _ = self.written.send_replace(self.written_count);
        }
        Ok(())
    }

    async fn seal(
        &mut self,
        snapshot: SessionSnapshot,
        journal_len: u64,
        last_seq: u64,
    ) -> io::Result<()> {
        self.sync_file(&self.paths.events).await?;
        self.sync_file(&self.paths.earlier).await?;
        self.meta.seal = Some(Seal {
            journal_len,
            last_seq,
            snapshot: Box::new(snapshot),
        });
        tokio::task::spawn_blocking({
            let meta_path = self.paths.meta.clone();
            let meta = self.meta.clone();
            move || write_meta(&meta_path, &meta)
        })
        .await
        .expect("spawn_blocking panicked")?;
        let _ = self.state.send_replace(LogState::Ready);
        Ok(())
    }

    async fn unseal(&mut self) -> io::Result<()> {
        if self.meta.seal.is_some() {
            self.meta.seal = None;
            tokio::task::spawn_blocking({
                let meta_path = self.paths.meta.clone();
                let meta = self.meta.clone();
                move || write_meta(&meta_path, &meta)
            })
            .await
            .expect("spawn_blocking panicked")?;
        }
        let _ = self.state.send_replace(LogState::Ready);
        Ok(())
    }

    async fn reset(&mut self) -> io::Result<()> {
        self.meta = Meta {
            format: FORMAT,
            build: self.meta.build.clone(),
            seal: None,
        };
        truncate(&self.paths.events).await?;
        truncate(&self.paths.earlier).await?;
        tokio::task::spawn_blocking({
            let meta_path = self.paths.meta.clone();
            let meta = self.meta.clone();
            move || write_meta(&meta_path, &meta)
        })
        .await
        .expect("spawn_blocking panicked")?;
        self.written_count = 0;
        self.submitted.store(0, Ordering::Relaxed);
        let _ = self.written.send_replace(0);
        let _ = self.state.send_replace(LogState::Building);
        Ok(())
    }

    async fn sync_file(&self, path: &Path) -> io::Result<()> {
        if let Ok(f) = OpenOptions::new().read(true).open(path).await {
            f.sync_all().await
        } else {
            Ok(())
        }
    }
}

fn recover(
    paths: &Paths,
    build: &str,
    journal_len: u64,
    last_seq: Option<u64>,
) -> io::Result<RecoverOutcome> {
    std::fs::create_dir_all(&paths.dir)?;
    let mut state = LogState::Ready;
    let mut wrote_meta = false;
    let mut meta = match read_meta(&paths.meta)? {
        Some(m) => m,
        None => {
            state = LogState::Building;
            wrote_meta = true;
            Meta {
                format: FORMAT,
                build: build.to_string(),
                seal: None,
            }
        }
    };
    if meta.format != FORMAT || meta.build != build {
        reset_files(paths)?;
        meta = Meta {
            format: FORMAT,
            build: build.to_string(),
            seal: None,
        };
        state = LogState::Building;
        wrote_meta = true;
    }
    let events_written = recover_file(&paths.events, last_seq)?;
    let earlier_written = recover_file(&paths.earlier, last_seq)?;
    if let Some(seal) = meta.seal.as_ref() {
        if seal.journal_len != journal_len {
            state = LogState::Building;
            meta.seal = None;
            wrote_meta = true;
        }
    }
    if wrote_meta {
        write_meta(&paths.meta, &meta)?;
    }
    Ok(RecoverOutcome {
        meta,
        state,
        written: events_written + earlier_written,
    })
}

fn recover_file(path: &Path, last_seq: Option<u64>) -> io::Result<u64> {
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let len = f.metadata()?.len();
    let mut good = sorted_ndjson::complete_len(&mut f, len)?;
    if good != len {
        f.set_len(good)?;
        f.sync_all()?;
    }
    if let Some(max_seq) = last_seq {
        if max_seq > 0 && good > 0 {
            let cutoff =
                sorted_ndjson::seek_sorted(path, CURSOR_PREFIX, (max_seq + 1) << 16, good)?;
            if cutoff < good {
                f.set_len(cutoff)?;
                f.sync_all()?;
                good = cutoff;
            }
        }
    }
    drop(f);
    count_lines(path, good)
}

fn reset_files(paths: &Paths) -> io::Result<()> {
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.events)?;
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.earlier)?;
    Ok(())
}

fn read_meta(path: &Path) -> io::Result<Option<Meta>> {
    match std::fs::File::open(path) {
        Ok(f) => Ok(Some(serde_json::from_reader(f).map_err(json_err)?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_meta(path: &Path, meta: &Meta) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        serde_json::to_writer(&mut f, meta).map_err(json_err)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
    }
    Ok(())
}

fn count_lines(path: &Path, limit: u64) -> io::Result<u64> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    if limit == 0 {
        return Ok(0);
    }
    let mut reader = std::io::BufReader::new(&mut f);
    let mut buf = String::new();
    let mut count = 0u64;
    let mut bytes = 0u64;
    while bytes < limit {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        if bytes <= limit {
            count += 1;
        }
    }
    Ok(count)
}

async fn truncate(path: &Path) -> io::Result<()> {
    fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await?
        .sync_all()
        .await
}

async fn append_lines(path: &Path, events: &[SessionEvent]) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    for event in events {
        let line = serde_json::to_vec(event).map_err(json_err)?;
        f.write_all(&line).await?;
        f.write_all(b"\n").await?;
    }
    f.flush().await
}

fn page_back_blocking<F>(
    path: PathBuf,
    before: Option<u64>,
    limit: usize,
    max_bytes: u64,
    filter: F,
) -> io::Result<(Vec<SessionEvent>, bool)>
where
    F: Fn(&SessionEvent) -> bool,
{
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), false)),
        Err(e) => return Err(e),
    };
    let len = file.metadata()?.len();
    let good = sorted_ndjson::complete_len(&mut file, len)?;
    let mut end = match before {
        Some(b) => sorted_ndjson::seek_sorted(&path, CURSOR_PREFIX, b, good)?,
        None => good,
    };
    let mut items = Vec::new();
    let mut bytes = 0u64;
    while end > 0 {
        let start = sorted_ndjson::line_start_before(&mut file, end)?;
        let line = sorted_ndjson::read_range(&mut file, start, end - 1)?;
        end = start;
        bytes += (line.len() + 1) as u64;
        let event: SessionEvent = serde_json::from_slice(&line).map_err(json_err)?;
        if filter(&event) {
            items.push(event);
            if items.len() >= limit || (bytes >= max_bytes && !items.is_empty()) {
                break;
            }
        }
        if bytes >= max_bytes && !items.is_empty() {
            break;
        }
    }
    items.reverse();
    Ok((items, end > 0))
}

fn stream_start(path: &Path, after: Option<u64>) -> io::Result<(u64, u64)> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e),
    };
    let len = f.metadata()?.len();
    let good = sorted_ndjson::complete_len(&mut f, len)?;
    let start = match after {
        Some(a) => sorted_ndjson::seek_sorted(path, CURSOR_PREFIX, a + 1, good)?,
        None => 0,
    };
    Ok((start, good))
}

fn json_err(e: serde_json::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("bad record ({:?} at column {})", e.classify(), e.column()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp() -> TempDir {
        tempfile::tempdir().unwrap()
    }

    fn dummy_snapshot() -> SessionSnapshot {
        let desc = wasit_claude::descriptor();
        let meta = SessionMeta {
            id: SessionId("aaaaaaaaaaaa".into()),
            driver: desc.id().clone(),
            cwd: None,
            labels: Default::default(),
            created_at_ms: 0,
            display_name: None,
            command: None,
            conversation: Conversation::Unknown,
        };
        blank_snapshot(&meta, desc.as_ref(), Phase::Starting, true)
    }

    fn ev(cursor: u64, message: &str) -> SessionEvent {
        SessionEvent {
            cursor,
            t: 1,
            kind: SessionEventKind::Notice {
                message: message.to_string(),
                code: NoticeCode::default(),
            },
        }
    }

    async fn wait_written(log: &EventLog, target: u64) {
        let mut w = log.written();
        while *w.borrow() < target {
            w.changed().await.unwrap();
        }
    }

    #[tokio::test]
    async fn torn_tail_truncated_on_recover() {
        let dir = temp();
        let paths = Paths::new(dir.path());
        fs::create_dir_all(&paths.dir).await.unwrap();
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&paths.events)
            .await
            .unwrap();
        let full = [ev(1, "a"), ev(2, "b"), ev(3, "c")];
        for e in full.iter().take(2) {
            let line = serde_json::to_vec(e).unwrap();
            f.write_all(&line).await.unwrap();
            f.write_all(b"\n").await.unwrap();
        }
        let partial = serde_json::to_vec(&full[2]).unwrap();
        f.write_all(&partial[..partial.len() / 2]).await.unwrap();
        drop(f);

        let log = EventLog::open(dir.path(), "build", 0, Some(3))
            .await
            .unwrap();
        let content = std::fs::read_to_string(&paths.events).unwrap();
        assert!(content.contains("\"cursor\":1"));
        assert!(content.contains("\"cursor\":2"));
        assert!(!content.contains("\"cursor\":3"));
        assert_eq!(*log.written.borrow(), 2);
    }

    #[tokio::test]
    async fn truncation_at_every_offset_preserves_prefix() {
        let full_lines: Vec<_> = (1..=4)
            .map(|i| serde_json::to_string(&ev(i, "x")).unwrap() + "\n")
            .collect();
        let full: String = full_lines.clone().into_iter().collect();
        for cut in 0..=full.len() {
            let dir = temp();
            let paths = Paths::new(dir.path());
            fs::create_dir_all(&paths.dir).await.unwrap();
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&paths.events)
                .await
                .unwrap();
            f.write_all(full.as_bytes()).await.unwrap();
            f.set_len(cut as u64).await.unwrap();
            drop(f);
            let _ = EventLog::open(dir.path(), "build", 0, Some(4))
                .await
                .unwrap();
            let recovered = std::fs::read_to_string(&paths.events).unwrap();
            let prefix = &full[..cut.min(full.len())];
            assert!(prefix.starts_with(&recovered));
        }
    }

    #[tokio::test]
    async fn key_mismatch_resets_log() {
        let dir = temp();
        let paths = Paths::new(dir.path());
        fs::create_dir_all(&paths.dir).await.unwrap();
        write_meta(
            &paths.meta,
            &Meta {
                format: FORMAT,
                build: "old".into(),
                seal: None,
            },
        )
        .unwrap();
        std::fs::write(
            &paths.events,
            serde_json::to_string(&ev(1, "a")).unwrap() + "\n",
        )
        .unwrap();
        let log = EventLog::open(dir.path(), "new", 0, Some(1)).await.unwrap();
        assert!(matches!(*log.state.borrow(), LogState::Building));
        assert_eq!(std::fs::read_to_string(&paths.events).unwrap(), "");
    }

    #[tokio::test]
    async fn seal_validity_checks_journal_len() {
        let dir = temp();
        let paths = Paths::new(dir.path());
        fs::create_dir_all(&paths.dir).await.unwrap();
        let snapshot = dummy_snapshot();
        write_meta(
            &paths.meta,
            &Meta {
                format: FORMAT,
                build: "b".into(),
                seal: Some(Seal {
                    journal_len: 10,
                    last_seq: 2,
                    snapshot: Box::new(snapshot),
                }),
            },
        )
        .unwrap();
        let log = EventLog::open(dir.path(), "b", 11, Some(2)).await.unwrap();
        assert!(matches!(*log.state.borrow(), LogState::Building));
        let meta = read_meta(&paths.meta).unwrap().unwrap();
        assert!(meta.seal.is_none());
    }

    #[tokio::test]
    async fn truncate_back_when_journal_shorter() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(2)).await.unwrap();
        log.append(
            vec![ev(event_cursor(1, 0), "a"), ev(event_cursor(3, 0), "b")],
            Vec::new(),
        )
        .unwrap();
        wait_written(&log, 2).await;
        drop(log);
        let reopened = EventLog::open(dir.path(), "b", 0, Some(2)).await.unwrap();
        let content = std::fs::read_to_string(Paths::new(dir.path()).events).unwrap();
        assert!(content.contains(&format!("{}", event_cursor(1, 0))));
        assert!(!content.contains(&format!("{}", event_cursor(3, 0))));
        assert_eq!(*reopened.written.borrow(), 1);
    }

    #[tokio::test]
    async fn page_back_honours_limits_and_bytes() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(10)).await.unwrap();
        let events: Vec<_> = (1..=6)
            .map(|i| ev(event_cursor(i, 0), &format!("e{i}")))
            .collect();
        log.append(events.clone(), Vec::new()).unwrap();
        wait_written(&log, events.len() as u64).await;
        let (page, has_more) = log
            .page_back(None, 3, Some(200), |e| e.cursor >= event_cursor(2, 0))
            .await
            .unwrap();
        assert_eq!(page.len(), 3);
        assert!(has_more);
        let (page_bytes, has_more_bytes) = log
            .page_back(
                None,
                10,
                Some((page[0].cursor.to_string().len() as u64) + 5),
                |_| true,
            )
            .await
            .unwrap();
        assert!(!page_bytes.is_empty());
        assert!(has_more_bytes);
    }

    #[tokio::test]
    async fn stream_between_respects_bounds() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(10)).await.unwrap();
        let events: Vec<_> = (1..=5)
            .map(|i| ev(event_cursor(i, 0), &format!("e{i}")))
            .collect();
        log.append(events.clone(), Vec::new()).unwrap();
        wait_written(&log, events.len() as u64).await;
        let mut rx = log.stream_between(Some(event_cursor(2, 0)), event_cursor(5, 0));
        let mut got = Vec::new();
        while let Some(item) = rx.recv().await {
            got.push(item.unwrap().cursor);
        }
        assert_eq!(got, vec![event_cursor(3, 0), event_cursor(4, 0)]);
    }

    #[tokio::test]
    async fn lines_start_with_cursor_prefix() {
        let dir = temp();
        let log = EventLog::open(dir.path(), "b", 0, Some(1)).await.unwrap();
        log.append(vec![ev(5, "x")], Vec::new()).unwrap();
        wait_written(&log, 1).await;
        let contents = std::fs::read_to_string(Paths::new(dir.path()).events).unwrap();
        assert!(contents.trim_start().starts_with("{\"cursor\":"));
    }
}
