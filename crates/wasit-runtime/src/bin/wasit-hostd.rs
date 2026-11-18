//! `wasit-hostd`: a minimal session-host daemon over the Wasit runtime — enough to run the
//! whole stack by hand and in tests. (The product daemon embeds the same pieces and adds its
//! own services next to [`RuntimeService`] in a [`ServiceMux`].)
//!
//! ```text
//! wasit-hostd serve  --data <dir> [--socket <path>] [--host-id <id>] [options]
//! wasit-hostd bridge --data <dir> [--socket <path>] [--host-id <id>] [options]
//! wasit-hostd shim   <wasit-shim arguments>
//! ```
//!
//! * `serve` takes the single-instance lock next to the socket (`<socket>.lock`), re-adopts the
//!   sessions under `<dir>/sessions/`, prints one JSON readiness line on stdout and serves the
//!   runtime's control methods until SIGTERM / SIGINT. Sessions keep running when it exits.
//!   The socket defaults to `<dir>/hostd.sock` (Windows: a named pipe derived from `<dir>`).
//! * `bridge` pipes stdio to the daemon's socket (starting a detached `serve` with the same
//!   options if none is running) — the command a remote hub runs over ssh.
//! * `shim` is the per-session process holder entry (also used for the shim's own guard
//!   re-exec), so one binary is all a host needs.
//!
//! Options: `--driver-bin <driver>=<path>` (repeatable) overrides a driver's binary,
//! `--driver-env <driver>=<KEY>=<VALUE>` (repeatable) adds environment for it,
//! `--handshake-timeout-ms N`, `--stop-grace-ms N`, `--command-timeout-ms N`.
//!
//! Keep `<dir>` short: session sockets live at `<dir>/sessions/<12 chars>/ctl.sock` and Unix
//! socket paths are limited to ~104 bytes.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use wasit_control::{ConnConfig, ControlServer, Endpoint, Hello, ServiceMux};
use wasit_core::DriverId;
use wasit_runtime::wire::ROUTES;
use wasit_runtime::{
    Discovery, DiscoveryConfig, RuntimeConfig, RuntimeService, SessionManager, ShimLauncher,
};

const BUILD: &str = concat!("wasit-hostd/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
struct Args {
    data: PathBuf,
    socket: Option<PathBuf>,
    host_id: Option<String>,
    driver_bin: Vec<(String, String)>,
    driver_env: Vec<(String, String, String)>,
    handshake_ms: Option<u64>,
    stop_grace_ms: Option<u64>,
    command_ms: Option<u64>,
    /// The options exactly as given (passed on when `bridge` starts `serve`).
    raw: Vec<String>,
}

fn parse(args: &[String]) -> Result<Args, String> {
    let mut a = Args {
        data: PathBuf::new(),
        socket: None,
        host_id: None,
        driver_bin: Vec::new(),
        driver_env: Vec::new(),
        handshake_ms: None,
        stop_grace_ms: None,
        command_ms: None,
        raw: args.to_vec(),
    };
    let mut data = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let mut val = || it.next().cloned().ok_or(format!("{flag} needs a value"));
        let num = |v: String| v.parse::<u64>().map_err(|e| format!("{flag}: {e}"));
        match flag.as_str() {
            "--data" => data = Some(PathBuf::from(val()?)),
            "--socket" => a.socket = Some(PathBuf::from(val()?)),
            "--host-id" => a.host_id = Some(val()?),
            "--driver-bin" => {
                let v = val()?;
                let (d, p) = v
                    .split_once('=')
                    .ok_or(format!("--driver-bin expects DRIVER=PATH, got {v:?}"))?;
                a.driver_bin.push((d.into(), p.into()));
            }
            "--driver-env" => {
                let v = val()?;
                let (d, kv) = v
                    .split_once('=')
                    .ok_or(format!("--driver-env expects DRIVER=KEY=VALUE, got {v:?}"))?;
                let (k, val) = kv
                    .split_once('=')
                    .ok_or(format!("--driver-env expects DRIVER=KEY=VALUE, got {v:?}"))?;
                a.driver_env.push((d.into(), k.into(), val.into()));
            }
            "--handshake-timeout-ms" => a.handshake_ms = Some(num(val()?)?),
            "--stop-grace-ms" => a.stop_grace_ms = Some(num(val()?)?),
            "--command-timeout-ms" => a.command_ms = Some(num(val()?)?),
            other => return Err(format!("unknown option {other}")),
        }
    }
    a.data = data.ok_or("--data <dir> is required")?;
    Ok(a)
}

fn socket_path(a: &Args) -> PathBuf {
    a.socket
        .clone()
        .unwrap_or_else(|| a.data.join("hostd.sock"))
}

fn endpoint(a: &Args) -> Endpoint {
    wasit_control::local_endpoint(&socket_path(a))
}

fn runtime_config(a: &Args) -> Result<RuntimeConfig, String> {
    let launcher = ShimLauncher::current_exe("shim").map_err(|e| e.to_string())?;
    let mut cfg = RuntimeConfig::new(a.data.clone(), launcher);
    for (d, p) in &a.driver_bin {
        let id = DriverId::new(d.clone());
        cfg.registry.get(&id).map_err(|e| e.to_string())?;
        cfg.overrides.entry(id).or_default().binary = Some(p.clone());
    }
    for (d, k, v) in &a.driver_env {
        let id = DriverId::new(d.clone());
        cfg.registry.get(&id).map_err(|e| e.to_string())?;
        cfg.overrides
            .entry(id)
            .or_default()
            .env
            .push((k.clone(), v.clone()));
    }
    if let Some(ms) = a.handshake_ms {
        cfg.timeouts.handshake = Duration::from_millis(ms);
    }
    if let Some(ms) = a.stop_grace_ms {
        cfg.timeouts.stop_grace = Duration::from_millis(ms);
    }
    if let Some(ms) = a.command_ms {
        cfg.timeouts.command = Duration::from_millis(ms);
    }
    Ok(cfg)
}

async fn serve(a: Args) -> ExitCode {
    let ep = endpoint(&a);
    if let Err(e) = tokio::fs::create_dir_all(&a.data).await {
        eprintln!("wasit-hostd: cannot create {}: {e}", a.data.display());
        return ExitCode::from(1);
    }
    let cfg = match runtime_config(&a) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("wasit-hostd: {e}");
            return ExitCode::from(2);
        }
    };
    let listener = match wasit_control::bind_local_exclusive(socket_path(&a)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("wasit-hostd: cannot listen: {e}");
            return ExitCode::from(3);
        }
    };
    let discovery = Discovery::new(DiscoveryConfig::from_runtime(&cfg));
    let manager = match SessionManager::new(cfg).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("wasit-hostd: {e}");
            return ExitCode::from(1);
        }
    };
    match manager.adopt_all().await {
        Ok(r) => {
            tracing::info!(
                running = r.running.len(),
                finished = r.finished.len(),
                errors = r.errors.len(),
                "adopted sessions"
            );
            for (dir, e) in r.errors {
                tracing::warn!(dir = %dir.display(), error = %e, "session not adopted");
            }
        }
        Err(e) => tracing::error!(error = %e, "cannot scan sessions"),
    }
    let host_id = a.host_id.clone().unwrap_or_else(default_host_id);
    let service = RuntimeService::new(manager, discovery, host_id.clone(), BUILD);
    let mux = match ServiceMux::new().with(ROUTES.iter().copied(), Arc::new(service)) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("wasit-hostd: {e}");
            return ExitCode::from(1);
        }
    };
    let server = ControlServer::new(
        Arc::new(mux),
        Hello::new(BUILD, host_id, "daemon"),
        ConnConfig::default(),
    );
    println!(
        "{}",
        serde_json::json!({"ready": true, "pid": std::process::id(), "endpoint": format!("{ep:?}")})
    );
    tokio::select! {
        r = server.serve(listener) => {
            if let Err(e) = r {
                eprintln!("wasit-hostd: serve failed: {e}");
                return ExitCode::from(1);
            }
        }
        what = termination() => {
            tracing::info!("{what}: shutting down (sessions keep running)");
            server.shutdown(Duration::from_secs(2)).await;
        }
    }
    ExitCode::SUCCESS
}

fn default_host_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "localhost".into())
}

#[cfg(unix)]
async fn termination() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(mut t), Ok(mut i)) => tokio::select! {
            _ = t.recv() => "SIGTERM",
            _ = i.recv() => "SIGINT",
        },
        _ => {
            tracing::error!("cannot install signal handlers");
            std::future::pending().await
        }
    }
}

#[cfg(windows)]
async fn termination() -> &'static str {
    match tokio::signal::ctrl_c().await {
        Ok(()) => "ctrl-c",
        Err(_) => std::future::pending().await,
    }
}

async fn bridge(a: Args) -> ExitCode {
    let ep = endpoint(&a);
    let raw = a.raw.clone();
    let data = a.data.clone();
    let start = move || start_daemon(raw, data);
    match wasit_control::bridge(
        tokio::io::stdin(),
        tokio::io::stdout(),
        &ep,
        start,
        wasit_control::BridgeOptions::default(),
    )
    .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("wasit-hostd bridge: {e}");
            ExitCode::from(1)
        }
    }
}

/// Start `serve` detached with the same options and wait (bounded by the bridge) for its
/// readiness line.
async fn start_daemon(raw: Vec<String>, data: PathBuf) -> std::io::Result<()> {
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt;
    tokio::fs::create_dir_all(&data).await?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(data.join("hostd.log"))?;
    let exe = std::env::current_exe()?;
    let mut cmd = wasit_process::detached_command(exe);
    cmd.arg("serve")
        .args(&raw)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log));
    let mut child = wasit_process::spawn_detached(&mut cmd)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("no stdout"))?;
    let mut line = String::new();
    tokio::io::BufReader::new(stdout)
        .read_line(&mut line)
        .await?;
    // EOF without a readiness line usually means another instance won the lock; the bridge's
    // connect retries decide.
    drop(child);
    Ok(())
}

fn usage() -> ExitCode {
    eprintln!("usage: wasit-hostd serve|bridge --data <dir> [--socket <path>] [options] | wasit-hostd shim <args>");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let Some(cmd) = args.next() else {
        return usage();
    };
    if cmd == "shim" {
        // Before anything else: the shim builds its own runtime, and its guard re-exec comes
        // back through here with the same argv.
        let rest: Vec<OsString> = args.collect();
        let code = wasit_shim::run(rest);
        return ExitCode::from(u8::try_from(code).unwrap_or(1));
    }
    let rest: Vec<String> = match args.map(|a| a.into_string()).collect() {
        Ok(v) => v,
        Err(bad) => {
            eprintln!("wasit-hostd: argument is not UTF-8: {bad:?}");
            return ExitCode::from(2);
        }
    };
    let parsed = match parse(&rest) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("wasit-hostd: {e}");
            return usage();
        }
    };
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("WASIT_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init(); // only fails if a subscriber is already set
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("wasit-hostd: cannot start the runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match cmd.to_str() {
        Some("serve") => rt.block_on(serve(parsed)),
        Some("bridge") => rt.block_on(bridge(parsed)),
        _ => usage(),
    }
}
