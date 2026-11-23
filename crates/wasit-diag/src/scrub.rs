//! The process-global scrubber registry (design §4.3): values that must never appear in a
//! [`DiagText`](crate::DiagText) — secrets and project paths — and the home directory.
//!
//! Registration happens where the values become known (cold paths: configuration, launch,
//! adoption). It takes one mutex that only registrations contend on, rebuilds an immutable
//! snapshot and swaps it in under a write lock held for one pointer store. `DiagText::new`
//! reads the current snapshot under a read lock held for one `Arc` clone. Emitters never touch
//! the registry.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, LazyLock, Mutex, RwLock};

use crate::json;
use crate::policy;

/// Values kept; the least recently registered is forgotten first.
pub const CAPACITY: usize = 4096;

/// Shorter secrets are refused: replacing every occurrence of a two-letter value would destroy
/// every text without protecting anything.
pub const MIN_SECRET_CHARS: usize = 4;

/// What a secret is replaced by.
pub const SECRET_MASK: &str = "••••";

struct Entry {
    replacement: Arc<str>,
    stamp: u64,
}

#[derive(Default)]
struct Registry {
    entries: HashMap<Arc<str>, Entry>,
    clock: u64,
    home: Option<Arc<str>>,
}

/// An immutable view of the registry, as `DiagText::new` uses it.
#[derive(Default)]
pub(crate) struct Snapshot {
    /// `(pattern, replacement)`, longest pattern first so a value containing another is
    /// replaced whole.
    pub(crate) patterns: Vec<(Arc<str>, Arc<str>)>,
    /// The home directory forms (as is, JSON-escaped), longest first.
    pub(crate) home: Vec<Arc<str>>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| {
    Mutex::new(Registry {
        home: default_home().map(Arc::from),
        ..Registry::default()
    })
});

static SNAPSHOT: LazyLock<RwLock<Arc<Snapshot>>> = LazyLock::new(|| {
    let reg = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    RwLock::new(Arc::new(build(&reg)))
});

fn default_home() -> Option<String> {
    let var = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    std::env::var(var)
        .ok()
        .or_else(|| std::env::var("HOME").ok())
        .filter(|h| h.len() > 1)
}

/// Registers a secret value (a host secret variable, an MCP env/header value, an askpass
/// answer). Returns `false` when refused as too short (fewer than [`MIN_SECRET_CHARS`]).
pub fn register_secret(value: &str) -> bool {
    register_secrets([value]) == 1
}

/// Registers several secrets with one snapshot rebuild; returns how many were accepted.
pub fn register_secrets<'a>(values: impl IntoIterator<Item = &'a str>) -> usize {
    let mask: Arc<str> = Arc::from(SECRET_MASK);
    update(|reg| {
        let mut n = 0;
        for v in values {
            if v.chars().count() >= MIN_SECRET_CHARS {
                insert(reg, v, mask.clone());
                n += 1;
            }
        }
        n
    })
}

/// Registers a project path (a session's cwd, a launch project): inside a free text it is
/// replaced by its [`policy::project_path`] text form.
pub fn register_project_path(path: impl AsRef<Path>) {
    let raw = path.as_ref().to_string_lossy().into_owned();
    if raw.chars().count() < 2 {
        return;
    }
    let shown: Arc<str> = Arc::from(policy::project_path(&raw).text_form());
    update(|reg| {
        let trimmed = raw.trim_end_matches(['/', '\\']);
        if trimmed.chars().count() >= 2 && trimmed != raw {
            insert(reg, trimmed, shown.clone());
        }
        insert(reg, &raw, shown);
    })
}

/// Sets the home directory whose prefix free text shows as `~` (default: `HOME`, on Windows
/// `USERPROFILE`, read once).
pub fn set_home(path: impl AsRef<Path>) {
    let home = path.as_ref().to_string_lossy();
    let home = home.trim_end_matches(['/', '\\']).to_owned();
    update(|reg| {
        reg.home = (home.len() > 1).then(|| Arc::from(home.as_str()));
    })
}

/// The home directory free text and paths are made relative to.
pub(crate) fn home() -> Option<Arc<str>> {
    snapshot().home.last().cloned()
}

pub(crate) fn snapshot() -> Arc<Snapshot> {
    SNAPSHOT.read().unwrap_or_else(|e| e.into_inner()).clone()
}

fn insert(reg: &mut Registry, value: &str, replacement: Arc<str>) {
    reg.clock += 1;
    let stamp = reg.clock;
    if let Some(e) = reg.entries.get_mut(value) {
        e.stamp = stamp;
        e.replacement = replacement;
        return;
    }
    if reg.entries.len() >= CAPACITY {
        // Cold path: a linear scan over at most CAPACITY entries.
        if let Some(oldest) = reg
            .entries
            .iter()
            .min_by_key(|(_, e)| e.stamp)
            .map(|(k, _)| k.clone())
        {
            reg.entries.remove(&oldest);
        }
    }
    reg.entries
        .insert(Arc::from(value), Entry { replacement, stamp });
}

fn update<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    // Make sure the snapshot exists before taking the registry lock (its initializer takes it).
    LazyLock::force(&SNAPSHOT);
    let mut reg = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    let r = f(&mut reg);
    let snap = Arc::new(build(&reg));
    // Swapped in while the registry is still locked: two updates cannot publish their snapshots
    // out of order (the older one overwriting the newer, losing a registration). Readers take
    // only the snapshot lock, and its initializer (forced above) is not waiting for this one.
    *SNAPSHOT.write().unwrap_or_else(|e| e.into_inner()) = snap;
    drop(reg);
    r
}

/// Every value in the form it has as is and JSON-escaped (a secret inside a JSON document a
/// tool printed), longest first.
fn build(reg: &Registry) -> Snapshot {
    let mut patterns: Vec<(Arc<str>, Arc<str>)> = Vec::with_capacity(reg.entries.len() * 2);
    for (value, e) in &reg.entries {
        patterns.push((value.clone(), e.replacement.clone()));
        let escaped = escaped_inner(value);
        if escaped != **value {
            patterns.push((Arc::from(escaped), e.replacement.clone()));
        }
    }
    patterns.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
    let mut home = Vec::new();
    if let Some(h) = &reg.home {
        let escaped = escaped_inner(h);
        if escaped != **h {
            home.push(Arc::from(escaped));
        }
        home.push(h.clone());
    }
    Snapshot { patterns, home }
}

/// `s` JSON-escaped, without the quotes.
fn escaped_inner(s: &str) -> String {
    let q = json::quoted(s);
    q[1..q.len() - 1].to_owned()
}
