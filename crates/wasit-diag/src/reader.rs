//! Reading a sink's files (design §6.3 reader API, used by the diagnostics service and the
//! exporter). A generation is named by the `gen` of its header line, which stays with the file
//! through every rename, so a reader never confuses two files that held the same name.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// The header line is at most this long.
const HEADER_MAX: u64 = 4096;
/// Bytes read from either end of a file to find its first and last record time.
const PROBE: u64 = 16 << 10;

/// A source and its generations on disk, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    /// The file name without `.ndjson` (`daemon`, `aux`, …).
    pub name: String,
    pub gens: Vec<GenInfo>,
}

/// One file of a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenInfo {
    pub gen: u64,
    pub bytes: u64,
    /// The `t` of its first and last record, when it has records.
    pub first_t: Option<u64>,
    pub last_t: Option<u64>,
}

/// Whole lines read from a generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Complete lines (each ending in `\n`); a trailing partial line is left for the next read.
    pub data: String,
    /// The offset to read from next.
    pub next: u64,
    /// `next` is the end of the file as it was when read.
    pub eof: bool,
    /// The generation no longer exists (rotated away): nothing was read.
    pub gone: bool,
}

pub(crate) fn source_name(file: &str) -> &str {
    file.strip_suffix(".ndjson").unwrap_or(file)
}

/// The generations of `file` in `dir`, oldest first.
pub fn source(dir: &Path, file: &str) -> Source {
    let mut gens: Vec<GenInfo> = candidates(dir, file)
        .into_iter()
        .filter_map(|p| gen_info(&p).ok().flatten())
        .collect();
    gens.sort_by_key(|g| g.gen);
    Source {
        name: source_name(file).to_owned(),
        gens,
    }
}

/// Reads at most `max` bytes of whole lines of generation `gen` of `file` in `dir`, from
/// `offset`. The generation is found by its header, and the bytes are read through the handle
/// whose header was checked, so a rotation between the two cannot mix files.
pub fn read(dir: &Path, file: &str, gen: u64, offset: u64, max: usize) -> io::Result<Chunk> {
    if file.contains(['/', '\\']) || file.starts_with('.') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a diagnostics source is a plain file name",
        ));
    }
    for path in candidates(dir, file) {
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        if header_gen(&first_line(&mut f)?) != Some(gen) {
            continue;
        }
        let len = f.metadata()?.len();
        f.seek(SeekFrom::Start(offset))?;
        let mut bytes = Vec::with_capacity(max.min(1 << 20));
        f.take(max as u64).read_to_end(&mut bytes)?;
        let whole = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        if whole == 0 && bytes.len() == max && max > 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a line is longer than the requested chunk",
            ));
        }
        bytes.truncate(whole);
        let next = offset + whole as u64;
        return Ok(Chunk {
            data: String::from_utf8_lossy(&bytes).into_owned(),
            next,
            eof: next >= len,
            gone: false,
        });
    }
    Ok(Chunk {
        data: String::new(),
        next: offset,
        eof: true,
        gone: true,
    })
}

/// `file` and `file.<n>` in `dir`, newest first.
fn candidates(dir: &Path, file: &str) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<(u64, PathBuf)> = rd
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?;
            let k = if name == file {
                0
            } else {
                name.strip_prefix(file)?.strip_prefix('.')?.parse().ok()?
            };
            Some((k, e.path()))
        })
        .collect();
    found.sort_by_key(|(k, _)| *k);
    found.into_iter().map(|(_, p)| p).collect()
}

fn gen_info(path: &Path) -> io::Result<Option<GenInfo>> {
    let mut f = File::open(path)?;
    let Some(gen) = header_gen(&first_line(&mut f)?) else {
        return Ok(None);
    };
    let bytes = f.metadata()?.len();
    f.seek(SeekFrom::Start(0))?;
    let mut head = Vec::new();
    (&mut f).take(PROBE).read_to_end(&mut head)?;
    let first_t = complete_lines(&head).find_map(record_t);
    let from = bytes.saturating_sub(PROBE);
    f.seek(SeekFrom::Start(from))?;
    let mut tail = Vec::new();
    f.read_to_end(&mut tail)?;
    let mut lines: Vec<&[u8]> = complete_lines(&tail).collect();
    if from > 0 && !lines.is_empty() {
        lines.remove(0); // may start mid-line
    }
    let last_t = lines.into_iter().rev().find_map(record_t);
    Ok(Some(GenInfo {
        gen,
        bytes,
        first_t,
        last_t,
    }))
}

/// Lines ending in `\n`, without it.
fn complete_lines(b: &[u8]) -> impl Iterator<Item = &[u8]> {
    let whole = b.iter().rposition(|c| *c == b'\n').map_or(0, |i| i + 1);
    b[..whole].split(|c| *c == b'\n').filter(|l| !l.is_empty())
}

fn record_t(line: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(line).ok()?;
    v.get("seq")?;
    v.get("t")?.as_u64()
}

/// The first line of `f` (at most [`HEADER_MAX`] bytes, without the newline), read from the
/// start.
pub(crate) fn first_line(f: &mut File) -> io::Result<Vec<u8>> {
    f.seek(SeekFrom::Start(0))?;
    let mut b = Vec::new();
    f.take(HEADER_MAX).read_to_end(&mut b)?;
    if let Some(i) = b.iter().position(|c| *c == b'\n') {
        b.truncate(i);
    }
    Ok(b)
}

/// The `gen` of a file header line.
pub(crate) fn header_gen(line: &[u8]) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_slice(line).ok()?;
    v.get("diag_file")?;
    v.get("gen")?.as_u64()
}
