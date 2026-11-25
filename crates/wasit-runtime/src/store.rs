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
//!
//! Everything in a session directory is private to the owner (design S23, leak L4): the
//! directories are `0700` and the files `0600` on Unix (nothing is done on Windows), created so
//! and, for what an older build left wider, tightened when the manager adopts the session
//! ([`tighten_session`]). The helpers here are the only way the runtime creates such
//! directories and files.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use wasit_core::{AgentConfig, DriverDescriptor, DriverId, SpawnSpec, Unsupported};
use wasit_shim::ShimSpec;

use crate::config::RuntimeConfig;
use crate::core::{Conversation, SessionMeta};
use crate::wire::{ForkedFrom, RuntimeError, SessionId};

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
    /// The agent's own id of the conversation: the one chosen or resumed at create, else the one
    /// the agent reported.
    #[serde(default)]
    pub native_session_id: Option<String>,
    #[serde(default)]
    pub unsupported: Vec<Unsupported>,
    /// User-given name (`sessions.rename`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// How the earlier history of the conversation this session resumed reaches its journal,
    /// decided at create (see [`crate::note::EarlierPlan`]); read by the actor until it has
    /// journaled the first `start` note, which carries it from then on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earlier: Option<crate::note::EarlierPlan>,
    /// The agent configuration the session was created with. A fork starts with it.
    #[serde(default, skip_serializing_if = "is_default_config")]
    pub config: AgentConfig,
    /// The session and conversation this one forked from (`sessions.fork`), set at creation and
    /// never changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from: Option<ForkedFrom>,
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
            native_session_id: spec.known_native_id().map(str::to_string),
            spec,
            unsupported,
            display_name: None,
            earlier: None,
            config: AgentConfig::default(),
            forked_from: None,
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
            command: self.spec.bin.clone(),
            conversation: Conversation::of_spec(&self.spec),
            forked_from: self.forked_from.clone(),
        }
    }
}

/// A configuration that is the default one: not written (a session file without it reads the
/// same).
fn is_default_config(c: &AgentConfig) -> bool {
    *c == AgentConfig::default()
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
    write_private(&tmp, &text).await?;
    tokio::fs::rename(&tmp, dir.join(SESSION_FILE)).await
}

/// Creates `path` and its missing parents as private directories (`0700`); an existing `path`
/// is tightened.
pub(crate) async fn private_dir_all(path: &Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || wasit_diag::private::create_dir_all(&path))
        .await
        .map_err(io::Error::other)?
}

/// Opens a file private to its owner (`0600` when created; one that exists is tightened first),
/// with the options `configure` sets on top of `create(true)`.
pub(crate) async fn open_private(
    path: &Path,
    configure: impl FnOnce(&mut tokio::fs::OpenOptions),
) -> io::Result<tokio::fs::File> {
    let tighten = path.to_path_buf();
    tokio::task::spawn_blocking(move || wasit_diag::private::tighten_file(&tighten))
        .await
        .map_err(io::Error::other)??;
    let mut options = tokio::fs::OpenOptions::from(wasit_diag::private::file_options());
    configure(&mut options);
    options.open(path).await
}

/// Writes `bytes` to the private file `path`, replacing its contents.
pub(crate) async fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut f = open_private(path, |o| {
        o.write(true).truncate(true);
    })
    .await?;
    f.write_all(bytes).await?;
    f.flush().await
}

/// Tightens a session directory an older build created: the directory itself, and the files and
/// directories directly in it (`journal.ndjson`, `session.json`, `shim.log`, `blobs/`, …). What
/// is inside those directories is reachable only through them, so it is not walked. The first
/// failure is returned after the rest was tried.
pub(crate) async fn tighten_session(dir: &Path) -> io::Result<()> {
    use wasit_diag::private::{tighten_dir, tighten_file};
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut first = tighten_dir(&dir).err();
        for ent in std::fs::read_dir(&dir)? {
            let ent = ent?;
            let kind = ent.file_type()?;
            let done = if kind.is_dir() {
                tighten_dir(&ent.path())
            } else if kind.is_file() {
                tighten_file(&ent.path())
            } else {
                Ok(())
            };
            if let Err(e) = done {
                first.get_or_insert(e);
            }
        }
        first.map_or(Ok(()), Err)
    })
    .await
    .map_err(io::Error::other)?
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
            private_dir_all(parent)
                .await
                .map_err(|e| RuntimeError::Io(format!("create {}: {e}", parent.display())))?;
        }
        write_private(&path, f.contents.as_bytes())
            .await
            .map_err(|e| RuntimeError::Io(format!("write {}: {e}", path.display())))?;
    }
    Ok(())
}

/// The argv a session's process is started with. A driver with a [`wasit_core::Wrapper`] is
/// started as `<host> <subcommand> -- <driver argv…>`, where the host is the program the shim is
/// started with and the subcommand replaces the shim's (the last element of the launcher's
/// arguments, by convention `shim`). The host then runs the wrapper itself, so no executable path
/// is known to the driver.
pub(crate) fn launch_argv(
    launcher: &crate::config::ShimLauncher,
    desc: &dyn DriverDescriptor,
    spec: &SpawnSpec,
) -> Result<Vec<String>, RuntimeError> {
    let argv = desc.argv(spec);
    let Some(wrapper) = desc.wrapper() else {
        return Ok(argv);
    };
    let host = launcher
        .program
        .to_str()
        .ok_or_else(|| RuntimeError::Spawn("the host program path is not UTF-8".into()))?;
    let Some((_shim_subcommand, before)) = launcher.args.split_last() else {
        return Err(RuntimeError::Spawn(format!(
            "the host launcher has no subcommand to run `{}` with",
            wrapper.subcommand
        )));
    };
    let mut out = vec![host.to_string()];
    for arg in before {
        let arg = arg
            .to_str()
            .ok_or_else(|| RuntimeError::Spawn("a launcher argument is not UTF-8".into()))?;
        out.push(arg.to_string());
    }
    out.push(wrapper.subcommand.to_string());
    out.push("--".to_string());
    out.extend(argv);
    Ok(out)
}

/// The shim launch spec for one process generation of `spec`.
pub(crate) fn shim_spec(
    cfg: &RuntimeConfig,
    desc: &dyn DriverDescriptor,
    dir: &Path,
    spec: &SpawnSpec,
) -> Result<ShimSpec, RuntimeError> {
    let argv = launch_argv(&cfg.launcher, desc, spec)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShimLauncher;

    #[test]
    fn a_wrapped_driver_is_launched_through_its_host_subcommand() {
        let spec = SpawnSpec::default();
        let launcher = ShimLauncher::subcommand("/opt/host/wasit-hostd", "shim");
        let kilo = wasit_opencode::kilo();
        assert_eq!(
            launch_argv(&launcher, kilo.as_ref(), &spec).unwrap(),
            ["/opt/host/wasit-hostd", "kilo-bridge", "--", "kilo", "acp"]
        );
        // A driver without a wrapper is launched as it always was.
        let claude = wasit_claude::descriptor();
        assert_eq!(
            launch_argv(&launcher, claude.as_ref(), &spec).unwrap(),
            claude.argv(&spec)
        );
    }

    #[test]
    fn a_wrapper_needs_a_subcommand_in_the_launcher() {
        let launcher = ShimLauncher::binary("/opt/host/wasit-shim");
        let err = launch_argv(
            &launcher,
            wasit_opencode::kilo().as_ref(),
            &SpawnSpec::default(),
        );
        assert!(matches!(err, Err(RuntimeError::Spawn(_))));
    }
}
