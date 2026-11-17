//! Synchronous journal file helpers. Callers run them on the blocking pool.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::record::Record;

const CHUNK: usize = 64 * 1024;

/// Extract `seq` from one journal line. Fast path for the shim's own layout (`{"seq":N,`),
/// full parse otherwise.
pub(crate) fn parse_seq(line: &[u8]) -> io::Result<u64> {
    const PREFIX: &[u8] = b"{\"seq\":";
    if let Some(rest) = line.strip_prefix(PREFIX) {
        let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
        if digits > 0 && rest.get(digits) == Some(&b',') {
            if let Some(n) = std::str::from_utf8(&rest[..digits])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
            {
                return Ok(n);
            }
        }
    }
    #[derive(serde::Deserialize)]
    struct SeqOnly {
        seq: u64,
    }
    serde_json::from_slice::<SeqOnly>(line)
        .map(|s| s.seq)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad record: {e}")))
}

/// Offset of the start of the line that ends at `end` (exclusive; `end - 1` is that line's
/// `\n` or the end of a torn line). Scans backwards.
fn line_start_before(f: &mut File, end: u64) -> io::Result<u64> {
    let mut pos = end.saturating_sub(1); // skip the line's own terminator
    let mut buf = vec![0u8; CHUNK];
    while pos > 0 {
        let start = pos.saturating_sub(CHUNK as u64);
        let len = (pos - start) as usize;
        f.seek(SeekFrom::Start(start))?;
        f.read_exact(&mut buf[..len])?;
        if let Some(i) = buf[..len].iter().rposition(|&b| b == b'\n') {
            return Ok(start + i as u64 + 1);
        }
        pos = start;
    }
    Ok(0)
}

fn read_range(f: &mut File, start: u64, end: u64) -> io::Result<Vec<u8>> {
    let mut v = vec![0u8; (end - start) as usize];
    f.seek(SeekFrom::Start(start))?;
    f.read_exact(&mut v)?;
    Ok(v)
}

/// Position just after the last `\n` in the first `len` bytes (0 if none).
fn complete_len(f: &mut File, len: u64) -> io::Result<u64> {
    if len == 0 {
        return Ok(0);
    }
    let mut last = [0u8; 1];
    f.seek(SeekFrom::Start(len - 1))?;
    f.read_exact(&mut last)?;
    if last[0] == b'\n' {
        return Ok(len);
    }
    // Torn tail: the start of the torn line is the complete length.
    line_start_before(f, len + 1)
}

/// Prepare an existing journal for appending: truncate a torn final line and return
/// `(next_seq, length)`. A missing journal is created empty.
pub(crate) fn recover(path: &Path) -> Result<(u64, u64), crate::Error> {
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let len = f.metadata()?.len();
    let good = complete_len(&mut f, len)?;
    if good != len {
        f.set_len(good)?;
        f.sync_all()?;
    }
    if good == 0 {
        return Ok((1, 0));
    }
    let start = line_start_before(&mut f, good)?;
    let line = read_range(&mut f, start, good - 1)?;
    let seq = parse_seq(&line)
        .map_err(|e| crate::Error::Corrupt(format!("last record of {}: {e}", path.display())))?;
    Ok((seq + 1, good))
}

/// The last complete record of a journal, if any (a torn tail is ignored).
pub(crate) fn last_record(path: &Path) -> io::Result<Option<Record>> {
    let mut f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let len = f.metadata()?.len();
    let good = complete_len(&mut f, len)?;
    if good == 0 {
        return Ok(None);
    }
    let start = line_start_before(&mut f, good)?;
    let line = read_range(&mut f, start, good - 1)?;
    serde_json::from_slice(&line)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad record: {e}")))
}

/// Read the complete line starting at `start`; returns (line without `\n`, end offset).
fn line_at(r: &mut BufReader<File>, start: u64) -> io::Result<(Vec<u8>, u64)> {
    r.seek(SeekFrom::Start(start))?;
    let mut line = Vec::new();
    let n = r.read_until(b'\n', &mut line)?;
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    Ok((line, start + n as u64))
}

/// First line start at or after `pos` (`pos` itself if it is 0).
fn next_line_start(r: &mut BufReader<File>, pos: u64) -> io::Result<u64> {
    if pos == 0 {
        return Ok(0);
    }
    r.seek(SeekFrom::Start(pos - 1))?;
    let mut skipped = Vec::new();
    let n = r.read_until(b'\n', &mut skipped)?;
    Ok(pos - 1 + n as u64)
}

/// Byte offset of the first record with `seq >= from_seq` within the first `limit` bytes
/// (which must end on a line boundary), by binary search over line starts. Returns `limit`
/// when every record is older. Callers still filter by seq while reading forward.
pub(crate) fn seek_to_seq(path: &Path, from_seq: u64, limit: u64) -> io::Result<u64> {
    if from_seq <= 1 || limit == 0 {
        return Ok(0);
    }
    let mut r = BufReader::with_capacity(CHUNK, File::open(path)?);
    // Invariant: every record before `lo` has seq < from_seq; `hi` is a line start whose record
    // has seq >= from_seq, or `limit`.
    let (mut lo, mut hi) = (0u64, limit);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let s = next_line_start(&mut r, mid)?;
        let probe = if s >= hi { lo } else { s };
        let (line, end) = line_at(&mut r, probe)?;
        if end == probe {
            break; // EOF inside the limit: file shorter than claimed
        }
        if parse_seq(&line)? < from_seq {
            lo = end;
        } else {
            hi = probe;
        }
    }
    Ok(lo.min(limit))
}
