//! Live tests of config discovery and of disabling inherited items, against the real CLIs in
//! a throwaway `HOME` (the user's own configuration is never read or written). Opt-in:
//! `cargo test -p wasit-runtime --test live_config -- --ignored`. A missing binary fails the
//! test loudly.
//!
//! Each test configures items in the CLI's own files, checks that [`Discovery`] reports them
//! with their source, then renders `AgentConfig::disabled` with the driver's renderer and asks
//! the real CLI (through the same structured command it offers) whether the item is gone.

mod common;

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use common::TempDir;
use serde_json::Value;
use wasit_core::{AgentConfig, ConfigItem, ConfigItemKind, DisabledItem, DriverId, SpawnSpec};
use wasit_runtime::{default_registry, Discovery, DiscoveryConfig, DriverOverride};

fn require(bin: &str) {
    let path = std::env::var_os("PATH").unwrap_or_default();
    assert!(
        std::env::split_paths(&path).any(|d| d.join(bin).is_file()),
        "live test needs `{bin}` on PATH"
    );
}

fn write(p: &Path, text: &str) {
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, text).unwrap();
}

async fn discover(driver: &str, home: &Path, project: &Path) -> Vec<ConfigItem> {
    let mut cfg = DiscoveryConfig::new(default_registry());
    cfg.exec_timeout = Duration::from_secs(90);
    cfg.overrides.insert(
        DriverId::new(driver),
        DriverOverride {
            binary: None,
            env: vec![("HOME".into(), home.to_string_lossy().into_owned())],
        },
    );
    let d = Discovery::new(cfg);
    let project = project.to_string_lossy().into_owned();
    let mut changes = d.changes();
    d.refresh(Some(&DriverId::new(driver)), Some(&project))
        .unwrap();
    loop {
        let inv = d.inventory(Some(&project));
        let e = inv
            .drivers
            .iter()
            .find(|e| e.driver.as_str() == driver)
            .unwrap();
        if !e.refreshing && e.refreshed_at_ms > 0 {
            // Only the config probes are under test here (model/session probes may fail in an
            // empty home without credentials).
            for p in e
                .probes
                .iter()
                .filter(|p| p.kind == wasit_core::DiscoveryKind::Config)
            {
                assert!(p.ok, "config probe failed: {p:?}");
            }
            return e.config.clone();
        }
        tokio::time::timeout(Duration::from_secs(120), changes.changed())
            .await
            .expect("discovery finished")
            .unwrap();
    }
}

fn has(items: &[ConfigItem], kind: ConfigItemKind, name: &str, scope: &str) -> bool {
    items
        .iter()
        .any(|i| i.kind == kind && i.name == name && i.source.scope == scope)
}

fn render(driver: &str, disabled: Vec<DisabledItem>) -> SpawnSpec {
    let desc = default_registry().get(&DriverId::new(driver)).unwrap();
    let mut spec = SpawnSpec::default();
    let unsupported = desc.render_config(
        &AgentConfig {
            disabled,
            ..AgentConfig::default()
        },
        &mut spec,
    );
    assert!(unsupported.is_empty(), "{unsupported:?}");
    spec
}

fn dis(kind: ConfigItemKind, name: &str, path: Option<String>) -> DisabledItem {
    DisabledItem {
        kind,
        name: name.into(),
        path,
    }
}

async fn run(argv: &[String], env: &[(String, String)], cwd: &Path, stdin: &str) -> String {
    use tokio::io::AsyncWriteExt;
    let mut c = tokio::process::Command::new(&argv[0])
        .args(&argv[1..])
        .envs(env.iter().cloned())
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut i = c.stdin.take().unwrap();
    i.write_all(stdin.as_bytes()).await.unwrap();
    // Keep stdin open (claude would exit on EOF before answering); the reader below stops at
    // the answers it needs.
    let out = c.stdout.take().unwrap();
    let text = tokio::time::timeout(
        Duration::from_secs(120),
        read_until_answers(out, stdin.lines().count()),
    )
    .await
    .expect("CLI answered");
    drop(i);
    text
}

/// Reads stdout until `n` control responses arrived (claude), or EOF (one-shot commands).
async fn read_until_answers(out: tokio::process::ChildStdout, n: usize) -> String {
    use tokio::io::AsyncBufReadExt;
    let mut lines = tokio::io::BufReader::new(out).lines();
    let mut all = String::new();
    let mut answers = 0;
    while let Ok(Some(l)) = lines.next_line().await {
        if l.contains("\"control_response\"") {
            answers += 1;
        }
        all.push_str(&l);
        all.push('\n');
        if n > 0 && answers >= n {
            break;
        }
    }
    all
}

#[tokio::test]
#[ignore = "live: needs the real `claude` binary"]
async fn claude_config_discovery_and_disable() {
    require("claude");
    let t = TempDir::new();
    let home = t.path().join("h");
    let proj = t.path().join("p");
    write(
        &home.join(".claude.json"),
        r#"{"mcpServers":{"umcp":{"type":"stdio","command":"echo","args":["x"]}}}"#,
    );
    write(
        &proj.join(".mcp.json"),
        r#"{"mcpServers":{"pmcp":{"type":"stdio","command":"cat"}}}"#,
    );
    write(
        &home.join(".claude/skills/myskill/SKILL.md"),
        "---\nname: myskill\ndescription: a test skill\n---\nbody\n",
    );
    write(
        &home.join(".claude/commands/hello.md"),
        "---\ndescription: say hi\n---\nhi\n",
    );
    write(
        &home.join(".claude/agents/rev.md"),
        "---\nname: rev\ndescription: reviews\n---\nx\n",
    );
    let items = discover("claude", &home, &proj).await;
    assert!(
        has(&items, ConfigItemKind::McpServer, "umcp", "user"),
        "{items:#?}"
    );
    assert!(has(&items, ConfigItemKind::McpServer, "pmcp", "project"));
    assert!(has(&items, ConfigItemKind::Skill, "myskill", "user"));
    assert!(has(&items, ConfigItemKind::Command, "hello", "user"));
    assert!(has(&items, ConfigItemKind::Agent, "rev", "user"));

    let spec = render(
        "claude",
        vec![
            dis(ConfigItemKind::McpServer, "umcp", None),
            dis(ConfigItemKind::Skill, "myskill", None),
            dis(ConfigItemKind::Command, "hello", None),
        ],
    );
    let mut argv: Vec<String> = [
        "claude",
        "--print",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    argv.extend(spec.extra_args);
    let input = concat!(
        r#"{"type":"control_request","request_id":"1","request":{"subtype":"initialize","hooks":null}}"#,
        "\n",
        r#"{"type":"control_request","request_id":"2","request":{"subtype":"mcp_status"}}"#,
        "\n"
    );
    let env = vec![("HOME".to_string(), home.to_string_lossy().into_owned())];
    let out = run(&argv, &env, &proj, input).await;
    let mut commands = Vec::new();
    let mut servers = Vec::new();
    for l in out.lines() {
        let Ok(v) = serde_json::from_str::<Value>(l) else {
            continue;
        };
        let r = &v["response"]["response"];
        if let Some(c) = r["commands"].as_array() {
            commands = c
                .iter()
                .filter_map(|c| c["name"].as_str().map(str::to_string))
                .collect();
        }
        if let Some(s) = r["mcpServers"].as_array() {
            servers = s
                .iter()
                .filter_map(|c| c["name"].as_str().map(str::to_string))
                .collect();
        }
    }
    assert!(servers.contains(&"pmcp".to_string()), "{out}");
    assert!(!servers.contains(&"umcp".to_string()), "{servers:?}");
    assert!(
        !commands.iter().any(|c| c == "myskill" || c == "hello"),
        "{commands:?}"
    );
}

async fn acp_config_discovery_and_disable(driver: &str, config_rel: &str, skills_rel: &str) {
    require(driver);
    let t = TempDir::new();
    let home = t.path().join("h");
    let proj = t.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write(
        &home.join(config_rel),
        r#"{"mcp":{"gmcp":{"type":"local","command":["echo","hi"]}},"agent":{"helper":{"description":"helps","mode":"subagent","prompt":"p"}}}"#,
    );
    write(
        &home.join(skills_rel).join("gskill/SKILL.md"),
        "---\nname: gskill\ndescription: global skill\n---\nbody\n",
    );
    let items = discover(driver, &home, &proj).await;
    assert!(
        has(&items, ConfigItemKind::McpServer, "gmcp", "merged"),
        "{items:#?}"
    );
    assert!(has(&items, ConfigItemKind::Agent, "helper", "merged"));
    assert!(has(&items, ConfigItemKind::Skill, "gskill", "global"));

    let spec = render(
        driver,
        vec![
            dis(ConfigItemKind::McpServer, "gmcp", None),
            dis(ConfigItemKind::Agent, "helper", None),
            dis(ConfigItemKind::Skill, "gskill", None),
        ],
    );
    let mut env = vec![("HOME".to_string(), home.to_string_lossy().into_owned())];
    env.extend(spec.env);
    let out = run(
        &[driver.to_string(), "debug".into(), "config".into()],
        &env,
        &proj,
        "",
    )
    .await;
    let v: Value = serde_json::from_str(&out[out.find('{').unwrap()..]).unwrap();
    assert_eq!(v["mcp"]["gmcp"]["enabled"], Value::Bool(false), "{v:#}");
    assert_eq!(v["agent"]["helper"]["disable"], Value::Bool(true));
    assert_eq!(
        v["permission"]["skill"]["gskill"],
        Value::String("deny".into())
    );
}

#[tokio::test]
#[ignore = "live: needs the real `opencode` binary"]
async fn opencode_config_discovery_and_disable() {
    acp_config_discovery_and_disable(
        "opencode",
        ".config/opencode/opencode.json",
        ".config/opencode/skills",
    )
    .await;
}

#[tokio::test]
#[ignore = "live: needs the real `kilo` binary"]
async fn kilo_config_discovery_and_disable() {
    acp_config_discovery_and_disable("kilo", ".config/kilo/kilo.json", ".config/kilo/skills").await;
}

#[tokio::test]
#[ignore = "live: needs the real `codex` binary"]
async fn codex_config_discovery_and_disable() {
    require("codex");
    let t = TempDir::new();
    // codex refuses some setup under /tmp but works; keep its home inside the temp dir.
    let home = t.path().join("h");
    let proj = t.path().join("p");
    std::fs::create_dir_all(&proj).unwrap();
    write(
        &home.join(".codex/config.toml"),
        "[mcp_servers.cmcp]\ncommand = \"echo\"\nargs = [\"hi\"]\n",
    );
    write(
        &home.join(".codex/skills/cs/SKILL.md"),
        "---\nname: cs\ndescription: codex test skill\n---\nbody\n",
    );
    let items = discover("codex", &home, &proj).await;
    assert!(
        has(&items, ConfigItemKind::McpServer, "cmcp", "merged"),
        "{items:#?}"
    );
    let skill = items
        .iter()
        .find(|i| i.kind == ConfigItemKind::Skill && i.name == "cs")
        .expect("skill discovered");
    assert_eq!(skill.source.scope, "user");

    let spec = render(
        "codex",
        vec![
            dis(ConfigItemKind::McpServer, "cmcp", None),
            dis(ConfigItemKind::Skill, "cs", skill.path.clone()),
        ],
    );
    let env = vec![("HOME".to_string(), home.to_string_lossy().into_owned())];
    let mut argv: Vec<String> = vec!["codex".into(), "mcp".into(), "list".into(), "--json".into()];
    argv.extend(spec.extra_args.clone());
    let out = run(&argv, &env, &proj, "").await;
    let v: Value = serde_json::from_str(&out[out.find('[').unwrap()..]).unwrap();
    let cmcp = v
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "cmcp")
        .unwrap();
    assert_eq!(cmcp["enabled"], Value::Bool(false));

    let mut argv: Vec<String> = vec!["codex".into(), "debug".into(), "prompt-input".into()];
    let before = run(&argv, &env, &proj, "").await;
    assert!(
        before.contains("codex test skill"),
        "the skill is visible without the override"
    );
    argv.extend(spec.extra_args);
    let after = run(&argv, &env, &proj, "").await;
    assert!(
        !after.contains("codex test skill"),
        "the skill is gone with the override"
    );
}
