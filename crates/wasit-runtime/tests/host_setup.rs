//! The host environment configuration with real shells and the fake CLI: setup commands that
//! use a shell function the interactive startup files define (a fake module system) put an
//! agent on PATH; variables override the setup; an agent command (`genie`, a wrapper) stands in
//! for claude in discovery and in a session; a command that is not the agent is warned about;
//! failing, exiting, hanging and malformed setups are reported; a hanging setup ends by the
//! user's cancel, its processes with it.
//! Secrets never appear in a report. One test function: the base environment is process-wide.
#![cfg(unix)]

mod common;

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::*;
use wasit_core::DriverId;
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::environment::{CaptureOptions, HostEnv};
use wasit_runtime::wire::{EnvSource, EnvVar, HostEnvConfig, HostEnvironment, InventoryReply};
use wasit_runtime::{Discovery, DiscoveryConfig, SessionManager};

fn exe(p: &Path, text: &str) {
    std::fs::write(p, text).unwrap();
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// A login shell (bash) with its own HOME whose `.bashrc` defines `module` only in
/// interactive shells, like an HPC site's profile: `module load <m>` puts `<modules>/<m>/bin`
/// first on PATH, an unknown module fails.
fn site(dir: &Path) -> (PathBuf, PathBuf) {
    let home = dir.join("home");
    let modules = dir.join("modules");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join(".bash_profile"),
        format!(
            "echo profile-noise\nexport SITE_TOKEN={SECRET}\n[ -f ~/.bashrc ] && . ~/.bashrc\n"
        ),
    )
    .unwrap();
    std::fs::write(
        home.join(".bashrc"),
        format!(
            "case $- in *i*) module() {{\n  if [ \"$1\" = load ] && [ -d '{m}'/\"$2\" ]; then\n    \
             PATH='{m}'/\"$2\"/bin:$PATH; export PATH; echo \"loaded $2\"\n  else\n    \
             echo \"module: $2 not found\" >&2; return 1\n  fi\n}};; esac\n",
            m = modules.display()
        ),
    )
    .unwrap();
    let shell = dir.join("login-shell");
    exe(
        &shell,
        &format!(
            "#!/bin/sh\nHOME='{}'; export HOME\nexec /bin/bash \"$@\"\n",
            home.display()
        ),
    );
    // Module `tools`: `kilo-site` (the fake CLI, persona by name; a name no machine has) and
    // `genie`, a wrapper that stands
    // in for claude and leaves its environment behind.
    let bin = modules.join("tools/bin");
    std::fs::create_dir_all(&bin).unwrap();
    std::os::unix::fs::symlink(fake(), bin.join("kilo-site")).unwrap();
    exe(
        &bin.join("genie"),
        &format!(
            "#!/bin/sh\nenv > '{}/genie-env'\nWASIT_FAKE_PERSONA=claude exec '{}' \"$@\"\n",
            dir.display(),
            fake().display()
        ),
    );
    // Something that is not an agent.
    exe(
        &bin.join("lister"),
        "#!/bin/sh\necho 'lister (GNU tools) 9.4'\n",
    );
    (shell, modules)
}

async fn settled(disc: &Discovery) -> InventoryReply {
    let (inv, complete) = disc.settled(None, Duration::from_secs(60)).await;
    assert!(complete, "discovery did not settle: {inv:?}");
    inv
}

fn base(name: &str) -> Option<String> {
    wasit_process::child_var(name).map(|v| v.to_string_lossy().into_owned())
}

fn cfg(setup: &str) -> HostEnvConfig {
    HostEnvConfig {
        setup: setup.into(),
        ..HostEnvConfig::default()
    }
}

const SECRET: &str = "s3cret-7f1e9a";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_variables_and_agent_commands() {
    let d = TempDir::new();
    let (shell, modules) = site(d.path());
    let opts = CaptureOptions {
        shell: Some(shell),
        ..CaptureOptions::default()
    };
    let env = HostEnv::new(opts.clone());
    let r = env.refresh().await;
    assert_eq!(r.source, EnvSource::LoginInteractiveShell, "{r:?}");
    assert!(r.setup.is_none() && r.revision.is_none());

    let data = d.path().join("data");
    let mut rcfg = config(&data, timeouts());
    rcfg.commands = env.commands();
    let kilo = DriverId::new("kilo");
    let claude = DriverId::new("claude");
    let codex = DriverId::new("codex");
    rcfg.overrides.get_mut(&kilo).unwrap().binary = Some("kilo-site".into());
    let disc = Discovery::new(DiscoveryConfig::from_runtime(&rcfg));
    let inv = settled(&disc).await;
    let k = inv.drivers.iter().find(|x| x.driver == kilo).unwrap();
    assert!(!k.installed, "kilo is only in the module: {k:?}");

    // Setup (with the interactive shell's `module` function), variables, agent commands.
    let tools = modules.join("tools/bin").display().to_string();
    let mut config =
        cfg("module load tools\nexport ORDER=setup FROM_SETUP=yes\necho \"token $SITE_TOKEN\"");
    config.vars = vec![
        EnvVar {
            name: "ORDER".into(),
            value: "variable".into(),
            secret: false,
        },
        EnvVar {
            name: "API_TOKEN".into(),
            value: SECRET.into(),
            secret: true,
        },
    ];
    config.commands = BTreeMap::from([
        (claude.clone(), "genie".to_string()),
        (codex.clone(), "lister".to_string()),
    ]);
    assert!(
        !format!("{config:?}").contains(SECRET),
        "Debug shows a secret"
    );
    let (r, changed) = env.configure(config.clone(), "rev-1".into(), false).await;
    assert!(changed);
    let s = r.setup.clone().expect("setup report");
    assert!(s.ok && s.applied, "{s:?}");
    assert_eq!(s.exit_code, Some(0));
    assert!(s.shell.ends_with("login-shell -l -i"), "{s:?}");
    assert!(s.output.contains("loaded tools"), "{s:?}");
    assert!(
        !s.output.contains("profile-noise"),
        "startup chatter is not setup output: {s:?}"
    );
    assert!(
        s.output.contains("token ••••"),
        "secret not scrubbed: {s:?}"
    );
    assert_eq!(r.revision.as_deref(), Some("rev-1"));
    assert_eq!(r.path.first(), Some(&tools), "{r:?}");
    assert_eq!(r.variables, ["ORDER", "API_TOKEN"]);
    assert_eq!(r.commands.get(&claude).map(String::as_str), Some("genie"));
    assert_no_secret(&r);
    // Order: login → setup → variables.
    assert_eq!(base("ORDER").as_deref(), Some("variable"));
    assert_eq!(base("FROM_SETUP").as_deref(), Some("yes"));
    assert_eq!(base("API_TOKEN").as_deref(), Some(SECRET));
    // The capture's PATH is kept after the setup's entries.
    assert!(r.path.len() > 1);

    // Discovery: kilo on the setup's PATH; claude is `genie`; codex is not an agent.
    disc.refresh_all();
    let inv = settled(&disc).await;
    let k = inv.drivers.iter().find(|x| x.driver == kilo).unwrap();
    assert!(k.installed, "{k:?}");
    assert_eq!(
        k.binary_path.as_deref(),
        Some(&*format!("{tools}/kilo-site"))
    );
    assert!(k.command.is_none() && k.warning.is_none(), "{k:?}");
    let c = inv.drivers.iter().find(|x| x.driver == claude).unwrap();
    assert_eq!(c.command.as_deref(), Some("genie"));
    assert_eq!(c.binary_path.as_deref(), Some(&*format!("{tools}/genie")));
    assert!(c.version.is_some() && c.warning.is_none(), "{c:?}");
    let x = inv.drivers.iter().find(|x| x.driver == codex).unwrap();
    assert!(x.installed, "{x:?}");
    let w = x
        .warning
        .as_deref()
        .expect("a warning for a command that is not codex");
    assert!(
        w.contains("`lister` does not answer like") && w.contains("lister (GNU tools) 9.4"),
        "{w}"
    );

    // A session of claude runs `genie`, with the variables (the secret included).
    let mgr = SessionManager::new(rcfg.clone()).await.unwrap();
    let sc = Scenario::builder()
        .turn(Turn::builder().say("via genie"))
        .build();
    let mut req = create(&data, "claude", &sc);
    req.first_prompt = Some("hi".into());
    let id = mgr.create(req).await.unwrap().id;
    let snap = wait_snapshot(&mgr, &id, "one turn", |s| s.turns_completed >= 1).await;
    assert_eq!(snap.turns_completed, 1);
    assert_eq!(
        snap.command.as_deref(),
        Some("genie"),
        "the snapshot names the command"
    );
    let genie_env = std::fs::read_to_string(d.path().join("genie-env")).unwrap();
    assert!(genie_env.contains("ORDER=variable"), "{genie_env}");
    assert!(genie_env.contains(&format!("API_TOKEN={SECRET}")));
    // The secret is in the process's environment, not in what the session recorded.
    let dir = data.join("sessions").join(id.as_str());
    for f in ["session.json", "journal.ndjson"] {
        let text = std::fs::read_to_string(dir.join(f)).unwrap_or_default();
        assert!(!text.contains(SECRET), "{f} holds the secret");
    }
    mgr.delete(&id).await.unwrap();

    // The same configuration again: nothing re-runs; `again` re-runs the setup.
    let (r2, changed) = env.configure(config.clone(), "rev-2".into(), false).await;
    assert!(!changed);
    assert_eq!(r2.revision.as_deref(), Some("rev-2"));
    assert_eq!(r2.setup, r.setup);
    let (r3, changed) = env.configure(config.clone(), "rev-2".into(), true).await;
    assert!(changed);
    assert_ne!(r3.setup.unwrap().duration_ms, u64::MAX);

    // A failing last command: reported with its status and stderr; its environment is taken.
    let (r, _) = env
        .configure(
            cfg("export AFTER_FAIL=1\nmodule load nope"),
            "f".into(),
            false,
        )
        .await;
    let s = r.setup.unwrap();
    assert!(!s.ok && s.applied, "{s:?}");
    assert_eq!(s.exit_code, Some(1));
    assert!(s.output.contains("module: nope not found"), "{s:?}");
    assert!(s.error.as_deref().unwrap_or("").contains("status 1"));
    assert_eq!(base("AFTER_FAIL").as_deref(), Some("1"));
    // The agent commands and variables of the previous configuration are gone.
    assert!(env.commands().all().is_empty());
    assert_eq!(base("API_TOKEN"), None);

    // A setup that ends the shell: the login environment, not the setup's.
    let (r, _) = env
        .configure(cfg("export HALF=1\nexit 3"), "x".into(), false)
        .await;
    let s = r.setup.unwrap();
    assert!(!s.ok && !s.applied, "{s:?}");
    assert_eq!(s.exit_code, Some(3));
    assert!(
        s.error.as_deref().unwrap_or("").contains("ended the shell"),
        "{s:?}"
    );
    assert_eq!(base("HALF"), None);

    // Malformed: a failing command whose output is the shell's own message.
    let (r, _) = env.configure(cfg("if then fi ("), "m".into(), false).await;
    let s = r.setup.unwrap();
    assert!(!s.ok && s.error.is_some(), "{s:?}");
    // Found by the syntax check before anything runs, whatever the shell version.
    assert_ne!(s.exit_code, Some(0), "{s:?}");
    assert!(s.output.contains("syntax error"), "{s:?}");

    // Hanging: nothing bounds a setup (a module load can take minutes). The user's cancel ends
    // it: the whole group is killed, the report says so, the login environment is kept. The
    // setup reports its background sleep's pid through a FIFO, so the cancel comes once it runs.
    let fifo = d.path().join("hang.fifo");
    mkfifo(&fifo);
    let runner = env.clone();
    let hang = cfg(&format!(
        "echo before\nsleep 300 & echo \"$! \" > '{}'; wait",
        fifo.display()
    ));
    let run = tokio::spawn(async move { runner.configure(hang, "h".into(), false).await });
    let sleep_pid = read_pid(fifo).await;
    env.cancel_capture();
    let (r, _) = run.await.unwrap();
    let s = r.setup.unwrap();
    assert!(!s.ok && !s.applied, "{s:?}");
    assert!(
        s.error
            .as_deref()
            .unwrap_or("")
            .contains("was cancelled by the user"),
        "{s:?}"
    );
    assert!(s.output.contains("before"), "{s:?}");
    assert!(
        is_gone(sleep_pid),
        "the cancelled setup's sleep {sleep_pid} survived"
    );
    wasit_process::set_base_env(None);
}

/// A FIFO a hung setup reports its pid through: the write returns exactly when the setup runs.
fn mkfifo(path: &Path) {
    let st = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .unwrap();
    assert!(st.success(), "mkfifo {}", path.display());
}

async fn read_pid(fifo: PathBuf) -> u32 {
    let text = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || std::fs::read_to_string(&fifo)),
    )
    .await
    .expect("the hung setup ran")
    .unwrap()
    .unwrap();
    text.trim().parse().unwrap()
}

/// Whether `pid` is no longer running (gone, or a zombie that has already died). Only ever asked
/// about a pid a test's own setup reported.
fn is_gone(pid: u32) -> bool {
    let out = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    let stat = String::from_utf8_lossy(&out.stdout).trim().to_string();
    stat.is_empty() || stat.starts_with('Z')
}

fn assert_no_secret(r: &HostEnvironment) {
    let json = serde_json::to_string(r).unwrap();
    assert!(
        !json.contains(SECRET),
        "the report holds the secret: {json}"
    );
}
