//! Synchronous journal file helpers. Callers run them on the blocking pool.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::record::Record;

const CHUNK: usize = 64 * 1024;

/// Extract `seq` from one journal line. Fast path for the shim's own layout (`{"seq":N,`),
/// full parse otherwise.
pub(crate) fn parse_seq(line: &[u8]) -> io::Result<u64> {
    parse_numeric_key(line, b"{\"seq\":")
}

/// Parse the leading integer after `prefix`, falling back to a full JSON parse when the prefix
/// is not at the start (journals written by older builds).
fn parse_numeric_key(line: &[u8], prefix: &[u8]) -> io::Result<u64> {
    if let Some(n) = parse_key_fast(line, prefix) {
        return Ok(n);
    }
    let key = key_name(prefix)?;
    let v: serde_json::Value = serde_json::from_slice(line).map_err(json_err)?;
    v.get(&key).and_then(|v| v.as_u64()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("record has no {key} field"),
        )
    })
}

fn json_err(e: serde_json::Error) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("bad record ({:?} at column {})", e.classify(), e.column()),
    )
}

/// Parse a leading unsigned integer right after `prefix`. Returns `None` when the prefix is
/// missing, there are no digits, or the first non-digit is not `,`.
fn parse_key_fast(line: &[u8], prefix: &[u8]) -> Option<u64> {
    let rest = line.strip_prefix(prefix)?;
    let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 || rest.get(digits) != Some(&b',') {
        return None;
    }
    std::str::from_utf8(&rest[..digits])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
}

fn key_name(prefix: &[u8]) -> io::Result<String> {
    let rest = prefix.strip_prefix(b"{\"").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "key prefix must start with {\"",
        )
    })?;
    let end = rest.iter().position(|&b| b == b'"').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "key prefix missing closing quote",
        )
    })?;
    if rest.get(end + 1) != Some(&b':') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "key prefix must end with \"<key>:\"",
        ));
    }
    Ok(std::str::from_utf8(&rest[..end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key prefix not UTF-8"))?
        .to_string())
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
    // Private to the owner (design leak L4); a journal an older build left wider is tightened.
    let mut f = wasit_diag::private::open(path, |o| {
        o.read(true).write(true).truncate(false);
    })?;
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
    sorted_ndjson::seek_sorted(path, b"{\"seq\":", from_seq, limit)
}

/// General helpers for seeking inside an NDJSON file whose lines start with a numeric key.
///
/// The file is treated as a sequence of newline-terminated lines; each line is expected to
/// begin with `key_prefix` followed by a decimal integer and a comma (e.g. `{"seq":12,` or
/// `{"cursor":34,`). Seeking is by binary search over line starts, so it is O(log lines) I/O.
pub mod sorted_ndjson {
    use std::fs::File;
    use std::io::{self, BufReader};
    use std::path::Path;

    use super::{line_at, next_line_start, CHUNK};

    /// Parse a leading unsigned integer right after `prefix`. Fast path for the common ordering,
    /// full JSON parse otherwise.
    pub fn parse_key(line: &[u8], prefix: &[u8]) -> io::Result<u64> {
        super::parse_numeric_key(line, prefix)
    }

    /// Byte offset of the first line whose key is `>= from_key` within the first `limit` bytes
    /// (which must end on a line boundary), by binary search over line starts. Returns `limit`
    /// when every line's key is smaller. Callers still filter by key while reading forward.
    ///
    /// `key_prefix` is the literal bytes that begin every line, e.g. `b"{\"seq\":"`.
    pub fn seek_sorted(
        path: &Path,
        key_prefix: &[u8],
        from_key: u64,
        limit: u64,
    ) -> io::Result<u64> {
        if from_key <= 1 || limit == 0 {
            return Ok(0);
        }
        let mut r = BufReader::with_capacity(CHUNK, File::open(path)?);
        // Invariant: every line before `lo` has key < from_key; `hi` is a line start whose key
        // is >= from_key, or `limit`.
        let (mut lo, mut hi) = (0u64, limit);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let s = next_line_start(&mut r, mid)?;
            let probe = if s >= hi { lo } else { s };
            let (line, end) = line_at(&mut r, probe)?;
            if end == probe {
                break; // EOF inside the limit: file shorter than claimed
            }
            if parse_key(&line, key_prefix)? < from_key {
                lo = end;
            } else {
                hi = probe;
            }
        }
        Ok(lo.min(limit))
    }

    /// Position just after the last `\n` in the first `len` bytes (0 if none). Exported so
    /// callers can compute the searchable prefix of a file that may have a torn tail.
    #[allow(dead_code)]
    pub fn complete_len(f: &mut File, len: u64) -> io::Result<u64> {
        super::complete_len(f, len)
    }

    /// Offset of the start of the line that ends at `end` (exclusive). Scans backwards.
    #[allow(dead_code)]
    pub fn line_start_before(f: &mut File, end: u64) -> io::Result<u64> {
        super::line_start_before(f, end)
    }

    /// Read `end - start` bytes from `f` starting at `start`.
    #[allow(dead_code)]
    pub fn read_range(f: &mut File, start: u64, end: u64) -> io::Result<Vec<u8>> {
        super::read_range(f, start, end)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Write;

        fn write_lines(path: &Path, lines: &[&str]) {
            let mut f = File::create(path).unwrap();
            for line in lines {
                writeln!(f, "{line}").unwrap();
            }
        }

        fn temp_dir() -> (std::path::PathBuf, std::path::PathBuf) {
            static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("wst-sorted-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("log.ndjson");
            (dir, path)
        }

        #[test]
        fn seek_sorted_finds_cursor_prefix() {
            let (dir, path) = temp_dir();
            write_lines(
                &path,
                &[
                    r#"{"cursor":1,"t":1,"kind":"a"}"#,
                    r#"{"cursor":2,"t":2,"kind":"b"}"#,
                    r#"{"cursor":5,"t":5,"kind":"c"}"#,
                    r#"{"cursor":6,"t":6,"kind":"d"}"#,
                ],
            );
            let len = std::fs::metadata(&path).unwrap().len();
            let start = seek_sorted(&path, b"{\"cursor\":", 2, len).unwrap();
            let rest = std::fs::read_to_string(&path).unwrap();
            assert_eq!(
                &rest[start as usize..],
                r#"{"cursor":2,"t":2,"kind":"b"}
{"cursor":5,"t":5,"kind":"c"}
{"cursor":6,"t":6,"kind":"d"}
"#
            );
            assert_eq!(seek_sorted(&path, b"{\"cursor\":", 7, len).unwrap(), len);
            assert_eq!(seek_sorted(&path, b"{\"cursor\":", 1, len).unwrap(), 0);
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn seek_sorted_with_seq_prefix_still_works() {
            let (dir, path) = temp_dir();
            write_lines(
                &path,
                &[
                    r#"{"seq":1,"t":1,"kind":"a"}"#,
                    r#"{"seq":10,"t":10,"kind":"b"}"#,
                    r#"{"seq":20,"t":20,"kind":"c"}"#,
                ],
            );
            let len = std::fs::metadata(&path).unwrap().len();
            let start = seek_sorted(&path, b"{\"seq\":", 11, len).unwrap();
            let rest = std::fs::read_to_string(&path).unwrap();
            assert_eq!(
                &rest[start as usize..],
                r#"{"seq":20,"t":20,"kind":"c"}
"#
            );
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn parse_key_rejects_bad_lines() {
            assert_eq!(parse_key(b"{\"cursor\":12,}", b"{\"cursor\":").unwrap(), 12);
            // With the fallback, reordered keys parse too.
            assert_eq!(parse_key(b"{\"cursor\":12}", b"{\"cursor\":").unwrap(), 12);
            assert!(parse_key(b"{\"seq\":3,}", b"{\"cursor\":").is_err());
            assert!(parse_key(b"{\"cursor\":abc,}", b"{\"cursor\":").is_err());
        }

        #[test]
        fn parse_key_reads_reordered_keys() {
            assert_eq!(
                parse_key(b"{\"t\":1,\"cursor\":9}", b"{\"cursor\":").unwrap(),
                9
            );
            assert_eq!(
                parse_key(b"{\"kind\":\"a\",\"seq\":42,\"t\":1}", b"{\"seq\":").unwrap(),
                42
            );
        }
    }
}
