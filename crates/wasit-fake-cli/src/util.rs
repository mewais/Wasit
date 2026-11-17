//! Small helpers: ids and timestamps (no external crates).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(1);

fn entropy() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut x =
        nanos ^ (u64::from(std::process::id()) << 32) ^ c.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    // splitmix64 finaliser
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// `n` lowercase hex characters.
pub fn hex(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&format!("{:016x}", entropy()));
    }
    s.truncate(n);
    s
}

/// `n` base62 characters.
pub fn alnum(n: usize) -> String {
    const A: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut s = String::new();
    while s.len() < n {
        let mut e = entropy();
        for _ in 0..10 {
            s.push(A[(e % 62) as usize] as char);
            e /= 62;
        }
    }
    s.truncate(n);
    s
}

/// A random v4-shaped UUID.
pub fn uuid() -> String {
    let h = hex(32);
    format!(
        "{}-{}-4{}-a{}-{}",
        &h[0..8],
        &h[8..12],
        &h[13..16],
        &h[17..20],
        &h[20..32]
    )
}

/// A v7-shaped UUID (codex thread ids): millisecond timestamp prefix.
pub fn uuid7() -> String {
    let ms = now_ms();
    let h = hex(20);
    format!(
        "{:08x}-{:04x}-7{}-a{}-{}",
        (ms >> 16) & 0xffff_ffff,
        ms & 0xffff,
        &h[0..3],
        &h[3..6],
        &h[6..18]
    )
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn now_secs() -> u64 {
    now_ms() / 1000
}

/// Current time as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub fn iso_now() -> String {
    iso_from_ms(now_ms())
}

pub fn iso_from_ms(ms: u64) -> String {
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // civil-from-days (Howard Hinnant)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        ms % 1000
    )
}

/// Split `text` into at most `n` roughly equal chunks on char boundaries (never empty chunks).
pub fn chunk_text(text: &str, n: usize) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let n = n.max(1).min(chars.len().max(1));
    let size = chars.len().div_ceil(n).max(1);
    let out: Vec<String> = chars.chunks(size).map(|c| c.iter().collect()).collect();
    if out.is_empty() {
        vec![String::new()]
    } else {
        out
    }
}

/// Set `value` at dotted `path` inside a JSON object, creating objects on the way.
pub fn set(v: &mut serde_json::Value, path: &str, value: serde_json::Value) {
    let mut cur = v;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if !cur.is_object() {
            *cur = serde_json::Value::Object(Default::default());
        }
        let map = cur.as_object_mut().expect("just made an object");
        if parts.peek().is_none() {
            map.insert(part.to_string(), value);
            return;
        }
        cur = map
            .entry(part.to_string())
            .or_insert_with(|| serde_json::Value::Object(Default::default()));
    }
}

/// Remove a top-level or dotted key.
pub fn unset(v: &mut serde_json::Value, path: &str) {
    let (parent, key) = match path.rsplit_once('.') {
        Some((p, k)) => (v.pointer_mut(&format!("/{}", p.replace('.', "/"))), k),
        None => (Some(v), path),
    };
    if let Some(map) = parent.and_then(|p| p.as_object_mut()) {
        map.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_epoch() {
        assert_eq!(iso_from_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso_from_ms(1_794_956_692_758), "2026-11-17T23:04:52.758Z");
    }

    #[test]
    fn chunking_keeps_text() {
        let c = chunk_text("hello world", 3);
        assert_eq!(c.concat(), "hello world");
        assert!(c.len() <= 3);
        assert_eq!(chunk_text("", 3), vec![String::new()]);
    }

    #[test]
    fn uuid_shape() {
        let u = uuid();
        assert_eq!(u.len(), 36);
        assert_eq!(uuid7().len(), 36);
    }
}
