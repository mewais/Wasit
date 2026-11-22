//! `session.json` (beside the journal) and the shim launch spec.
//!
//! Data layout under the configured root:
//!
//! ```text
//! <root>/sessions/<id>/session.json    this file (spec, driver, native id, labels)
//! <root>/sessions/<id>/journal.ndjson  the shim's journal = the transcript
//! <run>/sessions/<id>/shim.lock        held by the live shim
//! <run>/sessions/<id>/ctl.sock         the shim's control socket (Unix)
//! <root>/sessions/<id>/shim.log        the shim's own diagnostics
//! <root>/sessions/<id>/<generated>     files written by the driver's config renderer
//! ```
//!
//! `<run>` is [`RuntimeConfig::run_dir`] (machine-local), or `<root>` when unset.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use wasit_core::{DriverDescriptor, DriverId, SpawnSpec, Unsupported};
use wasit_shim::ShimSpec;

use crate::config::RuntimeConfig;
use crate::core::SessionMeta;
use crate::wire::{RuntimeError, SessionId};

pub(crate) const SESSION_FILE: &str = "session.json";
const VERSION: u32 = 1;

/// Contents of `session.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SessionFile {
    pub version: u32,
    pub id: SessionId,
    pub driver: DriverId,
    pub created_at_ms: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
    /// Spec of the latest process generation (used when its `start` note is still missing).
    pub spec: SpawnSpec,
    #[serde(default)]
    pub native_session_id: Option<String>,
    #[serde(default)]
    pub unsupported: Vec<Unsupported>,
    /// User-given name (`sessions.rename`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

impl SessionFile {
    pub(crate) fn new(
        id: SessionId,
        driver: DriverId,
        created_at_ms: u64,
        labels: BTreeMap<String, String>,
        spec: SpawnSpec,
        unsupported: Vec<Unsupported>,
    ) -> Self {
        SessionFile {
            version: VERSION,
            id,
            driver,
            created_at_ms,
            labels,
            cwd: spec.cwd.clone(),
            spec,
            native_session_id: None,
            unsupported,
            display_name: None,
        }
    }

    pub(crate) fn meta(&self) -> SessionMeta {
        SessionMeta {
            id: self.id.clone(),
            driver: self.driver.clone(),
            cwd: self.cwd.clone(),
            labels: self.labels.clone(),
            created_at_ms: self.created_at_ms,
            display_name: self.display_name.clone(),
        }
    }
}

pub(crate) async fn read(dir: &Path) -> Result<SessionFile, String> {
    let text = tokio::fs::read_to_string(dir.join(SESSION_FILE))
        .await
        .map_err(|e| format!("read {SESSION_FILE}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("parse {SESSION_FILE}: {e}"))
}

/// Write atomically (temp file + rename).
pub(crate) async fn write(dir: &Path, file: &SessionFile) -> std::io::Result<()> {
    let text = serde_json::to_vec_pretty(file).map_err(std::io::Error::other)?;
    let tmp = dir.join(format!("{SESSION_FILE}.tmp"));
    tokio::fs::write(&tmp, text).await?;
    tokio::fs::rename(&tmp, dir.join(SESSION_FILE)).await
}

/// Write the renderer's generated files into the session directory.
pub(crate) async fn write_generated(dir: &Path, spec: &SpawnSpec) -> Result<(), RuntimeError> {
    for f in &spec.files {
        let rel = Path::new(&f.path);
        if rel.is_absolute()
            || rel
                .components()
                .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
        {
            return Err(RuntimeError::InvalidRequest(format!(
                "generated file path {:?} must be relative and stay inside the session directory",
                f.path
            )));
        }
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| RuntimeError::Io(format!("create {}: {e}", parent.display())))?;
        }
        tokio::fs::write(&path, &f.contents)
            .await
            .map_err(|e| RuntimeError::Io(format!("write {}: {e}", path.display())))?;
    }
    Ok(())
}

/// The shim launch spec for one process generation of `spec`.
pub(crate) fn shim_spec(
    cfg: &RuntimeConfig,
    desc: &dyn DriverDescriptor,
    dir: &Path,
    spec: &SpawnSpec,
) -> Result<ShimSpec, RuntimeError> {
    let argv = desc.argv(spec);
    let Some((program, args)) = argv.split_first() else {
        return Err(RuntimeError::Spawn(
            "the driver produced an empty argv".into(),
        ));
    };
    let env: Vec<(OsString, OsString)> = desc
        .env(spec)
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect();
    Ok(ShimSpec {
        shim_program: cfg.launcher.program.clone(),
        shim_args: cfg.launcher.args.clone(),
        dir: dir.to_path_buf(),
        run_dir: cfg
            .run_dir
            .as_ref()
            .map(|_| cfg.session_run_dir_for_spawn(dir)),
        program: program.into(),
        args: args.iter().map(OsString::from).collect(),
        cwd: spec.cwd.as_ref().map(PathBuf::from),
        env,
        options: cfg.shim.clone(),
    })
}

/// Apply the host's agent command, else the configured per-driver override (binary), and the
/// override's extra env to a spec.
pub(crate) fn apply_override(cfg: &RuntimeConfig, driver: &DriverId, spec: &mut SpawnSpec) {
    let o = cfg.override_for(driver);
    if spec.bin.is_none() {
        spec.bin = cfg.commands.get(driver).or(o.binary);
    }
    spec.env.extend(o.env);
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
