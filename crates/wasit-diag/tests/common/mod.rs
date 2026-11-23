//! Shared by the sink tests: an in-memory [`Target`] (optionally holding its first write until
//! the test releases it, or failing its first opens), and line helpers.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};

use wasit_diag::Target;

type Files = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Files in memory. The writer reaches them through the same [`Target`] calls as real files.
#[derive(Clone, Default)]
pub struct Mem {
    pub files: Files,
    /// The first write waits for a message (or the sender's drop) on this receiver.
    gate: Arc<Mutex<Option<Receiver<()>>>>,
    /// Opens that fail before opens succeed.
    failing_opens: Arc<Mutex<u32>>,
}

impl Mem {
    pub fn gated(release: Receiver<()>) -> Mem {
        Mem {
            gate: Arc::new(Mutex::new(Some(release))),
            ..Mem::default()
        }
    }

    pub fn failing_opens(n: u32) -> Mem {
        Mem {
            failing_opens: Arc::new(Mutex::new(n)),
            ..Mem::default()
        }
    }

    pub fn text(&self, name: &str) -> String {
        let files = self.files.lock().unwrap();
        String::from_utf8(files.get(name).cloned().unwrap_or_default()).unwrap()
    }
}

pub struct MemFile {
    name: String,
    files: Files,
    gate: Arc<Mutex<Option<Receiver<()>>>>,
}

impl Write for MemFile {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(release) = gate {
            // Blocks the writer thread until the test sends or drops the sender.
            let _ = release.recv();
        }
        self.files
            .lock()
            .unwrap()
            .entry(self.name.clone())
            .or_default()
            .extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Target for Mem {
    type File = MemFile;

    fn open(&mut self, name: &str) -> io::Result<(MemFile, u64)> {
        {
            let mut n = self.failing_opens.lock().unwrap();
            if *n > 0 {
                *n -= 1;
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
        }
        let len = self
            .files
            .lock()
            .unwrap()
            .entry(name.to_owned())
            .or_default()
            .len() as u64;
        Ok((
            MemFile {
                name: name.to_owned(),
                files: self.files.clone(),
                gate: self.gate.clone(),
            },
            len,
        ))
    }

    fn len_of(&mut self, name: &str) -> io::Result<Option<u64>> {
        Ok(self.files.lock().unwrap().get(name).map(|b| b.len() as u64))
    }

    fn first_line(&mut self, name: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(self
            .files
            .lock()
            .unwrap()
            .get(name)
            .map(|b| b.split(|c| *c == b'\n').next().unwrap_or_default().to_vec()))
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        let mut files = self.files.lock().unwrap();
        let b = files
            .remove(from)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        files.insert(to.to_owned(), b);
        Ok(())
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        self.files
            .lock()
            .unwrap()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    }

    fn sync(&mut self, _file: &mut MemFile) -> io::Result<()> {
        Ok(())
    }
}

/// Every line of `text` parsed as JSON (panics on a line that is not).
pub fn parse(text: &str) -> Vec<serde_json::Value> {
    text.lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l:?}")))
        .collect()
}

/// Records only (no file headers).
pub fn records(text: &str) -> Vec<serde_json::Value> {
    parse(text)
        .into_iter()
        .filter(|v| v.get("diag_file").is_none())
        .collect()
}

/// The records of a file in `dir`.
pub fn file_records(dir: &Path, name: &str) -> Vec<serde_json::Value> {
    records(&std::fs::read_to_string(dir.join(name)).unwrap_or_default())
}

pub fn ev(v: &serde_json::Value) -> &str {
    v["ev"].as_str().unwrap()
}
