//! What the runtime registers with the process's scrubber, checked in the test's own process
//! (the scrubber is process-wide, and so is the sink the records go to): host secret variables
//! when the environment is configured through the real method, a session's environment, MCP
//! server and directory when it is created through the real method, and the secrets again when
//! the scrubber has forgotten them. Records on disk and what `diag.read` serves carry the mask,
//! never the value. One test function: the scrubber and the sink are process-wide.
#![cfg(unix)]

mod common;

use std::sync::Arc;

use common::*;
use wasit_control::ServiceMux;
use wasit_core::{McpServerConfig, McpTransport};
use wasit_diag::{Code, DiagText, Ident, Origin, SinkConfig};
use wasit_fake_cli::scenario::{Scenario, Turn};
use wasit_runtime::environment::{CaptureOptions, HostEnv};
use wasit_runtime::wire::{ConfigureEnvironment, EnvVar, HostEnvConfig, DIAG_ROUTES, ROUTES};
use wasit_runtime::{DiagService, Discovery, DiscoveryConfig, RuntimeService};

wasit_diag::diag_events! {
    enum Ev {
        #[diag(ev = "t.text", budget = 1000000)]
        Text { what: Code, text: DiagText },
    }
}

const HOST_SECRET: &str = "s3cr3t-Host-Value";
const PLAIN: &str = "plain-visible-value";
const ENV_SECRET: &str = "env-Secret-4b1d90";
const SHORT: &str = "abc12";
const MCP_ENV: &str = "mcp-Env-Secret-77c2";
const MCP_HEADER: &str = "Bearer mcp-Header-9d03";
const DIR: &str = "project-dir-Marker-51e8";

/// The text a record holds for `raw` as free text from the host's shell.
fn shown(raw: &str) -> String {
    DiagText::new(Origin::HostShell, raw).as_str().to_owned()
}

/// `raw` recorded as an event: the line the sink wrote for it.
fn recorded(what: &'static str, raw: &str) {
    wasit_diag::emit(&Ev::Text {
        what: Code::st(what),
        text: DiagText::new(Origin::HostShell, raw),
    });
}

fn fake_shell(dir: &std::path::Path) -> std::path::PathBuf {
    let home = dir.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let sh = dir.join("shell");
    std::fs::write(
        &sh,
        format!(
            "#!/bin/sh\nHOME='{}'; export HOME\nexec /bin/bash \"$@\"\n",
            home.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&sh, std::fs::Permissions::from_mode(0o755)).unwrap();
    sh
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secrets_and_directories_are_masked_once_the_runtime_knows_them() {
    let d = TempDir::new();
    let diag = d.path().join("diag");
    wasit_diag::install(SinkConfig::new(
        Code::st("daemon"),
        Ident::st("test"),
        &diag,
        "daemon.ndjson",
    ))
    .unwrap();
    let data = d.path().join("data");
    let cfg = config(&data, timeouts());
    let manager = wasit_runtime::SessionManager::new(cfg.clone())
        .await
        .unwrap();
    let env = HostEnv::new(CaptureOptions {
        shell: Some(fake_shell(d.path())),
        ..CaptureOptions::default()
    });
    let runtime = RuntimeService::new(
        manager.clone(),
        Discovery::new(DiscoveryConfig::from_runtime(&cfg)),
        "scrub-test",
        "test",
    )
    .with_environment(env);
    let mux = ServiceMux::new()
        .with(ROUTES.iter().copied(), Arc::new(runtime))
        .and_then(|m| {
            m.with(
                DIAG_ROUTES.iter().copied(),
                Arc::new(DiagService::new(manager)),
            )
        })
        .unwrap();
    let c = loopback(Arc::new(mux));

    // Nothing is registered yet: the values show.
    let text = format!("the shell said {HOST_SECRET} and {PLAIN}");
    assert_eq!(shown(&text), text);

    // A host secret configured through the real method. A setup failure's text is the shell's
    // words; it holds the value as the user typed it into the command.
    let configure = |secret: &str| ConfigureEnvironment {
        config: HostEnvConfig {
            setup: format!("echo {secret}; false"),
            vars: vec![
                EnvVar {
                    name: "HOST_TOKEN".into(),
                    value: secret.into(),
                    secret: true,
                },
                EnvVar {
                    name: "HOST_PLAIN".into(),
                    value: PLAIN.into(),
                    secret: false,
                },
            ],
            commands: Default::default(),
        },
        revision: "r1".into(),
    };
    let report = c
        .configure_environment(&configure(HOST_SECRET))
        .await
        .unwrap();
    let setup = report.setup.unwrap();
    assert!(
        !setup.ok && !setup.output.contains(HOST_SECRET),
        "{setup:?}"
    );
    assert_eq!(shown(&text), "the shell said •••• and plain-visible-value");
    recorded("host", &text);
    recorded(
        "setup",
        &format!("{} {}", setup.error.unwrap_or_default(), setup.output),
    );

    // Values of a session: its environment, an MCP server's environment and headers, its
    // directory. Values shorter than six characters are not registered.
    let project = d.path().join(DIR);
    std::fs::create_dir_all(&project).unwrap();
    let cwd = project.to_string_lossy().into_owned();
    let about = format!(
        "env {ENV_SECRET}, short {SHORT}, mcp {MCP_ENV}, header {MCP_HEADER}, cwd {cwd}, child {cwd}/src"
    );
    // (A long path is token-shaped, so it is redacted as such before it is registered; what it
    // becomes once registered is its tag.)
    let before = shown(&about);
    assert!(before.contains(ENV_SECRET) && before.contains(MCP_ENV) && before.contains(MCP_HEADER));
    assert!(!before.contains("<p:"), "{before}");
    let sc = Scenario::builder().turn(Turn::builder().say("ok")).build();
    let mut req = create(d.path(), "claude", &sc);
    req.cwd = Some(cwd.clone());
    req.config.env.push(("A_TOKEN".into(), ENV_SECRET.into()));
    req.config.env.push(("SHORT".into(), SHORT.into()));
    req.config.mcp_servers = vec![
        McpServerConfig {
            name: "local".into(),
            transport: McpTransport::Stdio {
                command: "true".into(),
                args: vec![],
                env: [("API_TOKEN".to_string(), MCP_ENV.to_string())].into(),
            },
            trusted: false,
        },
        McpServerConfig {
            name: "remote".into(),
            transport: McpTransport::Http {
                url: "http://127.0.0.1:9/mcp".into(),
                headers: [("Authorization".to_string(), MCP_HEADER.to_string())].into(),
            },
            trusted: false,
        },
    ];
    let id = c.create(req).await.unwrap().id;
    let after = shown(&about);
    assert!(!after.contains(ENV_SECRET), "{after}");
    assert!(
        !after.contains(MCP_ENV) && !after.contains(MCP_HEADER),
        "{after}"
    );
    assert!(!after.contains(DIR), "{after}");
    assert!(
        after.contains(SHORT),
        "a five-character value is not registered: {after}"
    );
    assert!(
        after.contains("<p:"),
        "the directory is shown as its tag: {after}"
    );
    recorded("session", &about);

    // The scrubber forgets its oldest values beyond its capacity. The host's secrets are
    // registered again when a session is created, so they outlive any number of sessions.
    let flood: Vec<String> = (0..wasit_diag::scrub::CAPACITY + 10)
        .map(|i| format!("flood-value-{i:06}"))
        .collect();
    wasit_diag::scrub::register_secrets(flood.iter().map(String::as_str));
    assert_eq!(
        shown(&text),
        text,
        "the host secret should have been forgotten by now"
    );
    let mut req = create(d.path(), "claude", &sc);
    req.cwd = Some(d.path().to_string_lossy().into_owned());
    let id2 = c.create(req).await.unwrap().id;
    assert_eq!(shown(&text), "the shell said •••• and plain-visible-value");
    c.delete(&id).await.unwrap();
    c.delete(&id2).await.unwrap();

    // What was recorded, as the files hold it and as `diag.read` serves it.
    assert!(wasit_diag::flush(T));
    let s = c.diag_sources().await.unwrap();
    let g = s.sources[0].gens[0].gen;
    let served = c.diag_read("daemon", g, 0, 1 << 20).await.unwrap();
    let on_disk = std::fs::read_to_string(diag.join("daemon.ndjson")).unwrap();
    for text in [&served.data, &on_disk] {
        for line in ["host", "setup", "session"] {
            assert!(
                text.contains(&format!("\"what\":\"{line}\"")),
                "{line} was not recorded"
            );
        }
        for value in [HOST_SECRET, ENV_SECRET, MCP_ENV, MCP_HEADER, DIR] {
            assert!(!text.contains(value), "{value} is in a record");
        }
        assert!(text.contains("the shell said ••••"), "{text}");
        assert!(text.contains(PLAIN) && text.contains(SHORT));
    }
    wasit_process::set_base_env(None);
}
