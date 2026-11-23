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
//!
//! Diagnostics (`wasit-diag`): once `serve` holds its lock it records typed, content-free events
//! in `<dir>/diag/daemon.ndjson` (8 MiB, three older generations; the directory is `0700`, the
//! file `0600`) and serves them (`diag.sources`, `diag.read`, topic `diag.tail`, see
//! [`DiagService`]) next to the runtime's methods, and a panic leaves a `diag.panic` record there instead of text on stderr.
//! `tracing` output is counted per minute, not written. Developer overrides: `WASIT_LOG=<filter>`
//! prints plain `tracing` text (no colour) on stderr and lets a panic print its usual message
//! too; `WASIT_DIAG_QUEUE=<n>` sets the size of the record queue.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use tracing_subscriber::prelude::*;
use wasit_control::{
    CallContext, ConnConfig, ControlServer, Cursor, Endpoint, EventStream, Hello, RpcError,
    Service, ServiceMux,
};
use wasit_core::DriverId;
use wasit_diag::{Code, ErrorInfo, HomePath, Ident, Ms, SinkConfig};
use wasit_runtime::wire::{DIAG_ROUTES, ROUTES};
use wasit_runtime::{
    DiagService, Discovery, DiscoveryConfig, RuntimeConfig, RuntimeService, SessionManager,
    ShimLauncher,
};

const BUILD: &str = concat!("wasit-hostd/", env!("CARGO_PKG_VERSION"));
/// The build as a diagnostics identifier (which has no `/`).
const BUILD_ID: &str = concat!("wasit-hostd-", env!("CARGO_PKG_VERSION"));
/// The developer override: plain `tracing` text on stderr, and the usual panic message.
const DEV_ENV: &str = "WASIT_LOG";

wasit_diag::diag_events! {
    /// Events of the dev/test daemon (`src` `daemon`, file `diag/daemon.ndjson`).
    enum HostdEvent {
        /// `serve` holds its lock and its diagnostics are installed.
        #[diag(ev = "hostd.start")]
        Start { host: Ident, data: HomePath, socket: HomePath },
        /// The sessions found under the data directory were adopted.
        #[diag(ev = "hostd.adopted")]
        Adopted { running: u32, finished: u32, errors: u32 },
        /// One session could not be adopted (the reason is from outside this function: only its
        /// size and fingerprint are kept).
        #[diag(ev = "hostd.adopt_error", lvl = warn)]
        AdoptError { session: Ident, error: ErrorInfo },
        /// The sessions directory could not be scanned.
        #[diag(ev = "hostd.scan_failed", lvl = error)]
        ScanFailed { error: ErrorInfo },
        /// The readiness line was printed.
        #[diag(ev = "hostd.ready")]
        Ready { pid: u32 },
        /// The daemon stops serving (sessions keep running).
        #[diag(ev = "hostd.shutdown")]
        Shutdown { reason: Code },
        /// A control request was answered; `code` is the error code of a failure (never its
        /// message).
        #[diag(ev = "hostd.request")]
        Request { method: Code, ok: bool, code: Option<Code>, ms: Ms },
        /// A subscription was opened or refused.
        #[diag(ev = "hostd.subscribe")]
        Subscribe { topic: Code, ok: bool, code: Option<Code> },
    }
}

fn emit(event: HostdEvent) {
    wasit_diag::emit(&event);
}

/// Records each control request and subscription of the wrapped service: the method, whether it
/// succeeded, the error code and the time it took.
struct Observed<S>(S);

#[async_trait]
impl<S: Service> Service for Observed<S> {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        let began = Instant::now();
        let r = self.0.call(ctx, method, params).await;
        emit(HostdEvent::Request {
            method: Code::new(method),
            ok: r.is_ok(),
            code: r.as_ref().err().map(|e| Code::new(&e.code)),
            ms: Ms::of(began.elapsed()),
        });
        r
    }

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        let r = self.0.subscribe(ctx, topic, params, from).await;
        emit(HostdEvent::Subscribe {
            topic: Code::new(topic),
            ok: r.is_ok(),
            code: r.as_ref().err().map(|e| Code::new(&e.code)),
        });
        r
    }
}

/// Installs the process's diagnostics sink under `<data>/diag/`, the panic hook, and records
/// `process.start`. Called once the daemon holds its lock (a second instance must not write
/// the first one's file).
fn install_diagnostics(data: &std::path::Path) {
    let cfg = SinkConfig::new(
        Code::st("daemon"),
        Ident::st(BUILD_ID),
        data.join("diag"),
        "daemon.ndjson",
    );
    match wasit_diag::install(cfg) {
        Ok(()) => {
            wasit_diag::install_panic_hook(std::env::var_os(DEV_ENV).is_some());
            wasit_diag::announce_process();
        }
        // Nothing else can say it: this is the daemon's stderr (`hostd.log` when detached).
        Err(e) => eprintln!("wasit-hostd: no diagnostics: {e}"),
    }
}

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
    /// `--panic-test <marker>`: panic, once the diagnostics are installed, with a message that
    /// holds the marker (the `diag_panic` test; only in builds with the `panic-test` feature,
    /// which only the test build of this package enables).
    #[cfg(feature = "panic-test")]
    panic_test: Option<String>,
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
        #[cfg(feature = "panic-test")]
        panic_test: None,
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
            #[cfg(feature = "panic-test")]
            "--panic-test" => a.panic_test = Some(val()?),
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
    let data = a.data.clone();
    let created = tokio::task::spawn_blocking(move || wasit_diag::private::create_dir_all(&data))
        .await
        .map_err(std::io::Error::other)
        .and_then(|r| r);
    if let Err(e) = created {
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
    let host_id = a.host_id.clone().unwrap_or_else(default_host_id);
    install_diagnostics(&a.data);
    emit(HostdEvent::Start {
        host: Ident::new(&host_id),
        data: HomePath::new(&a.data),
        socket: HomePath::new(&socket_path(&a)),
    });
    #[cfg(feature = "panic-test")]
    if let Some(marker) = &a.panic_test {
        panic!("hostd panic test: {marker}");
    }
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
            emit(HostdEvent::Adopted {
                running: u32::try_from(r.running.len()).unwrap_or(u32::MAX),
                finished: u32::try_from(r.finished.len()).unwrap_or(u32::MAX),
                errors: u32::try_from(r.errors.len()).unwrap_or(u32::MAX),
            });
            for (dir, e) in r.errors {
                emit(HostdEvent::AdoptError {
                    session: Ident::new(
                        &dir.file_name()
                            .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
                    ),
                    error: ErrorInfo::foreign(Code::st("adopt"), &e),
                });
            }
        }
        Err(e) => emit(HostdEvent::ScanFailed {
            error: ErrorInfo::ours(Code::st("scan"), &e),
        }),
    }
    // Not wrapped in `Observed`: reading the log must not write to it.
    let diag = DiagService::new(manager.clone());
    let service = Observed(RuntimeService::new(
        manager,
        discovery,
        host_id.clone(),
        BUILD,
    ));
    let mux = match ServiceMux::new()
        .with(ROUTES.iter().copied(), Arc::new(service))
        .and_then(|m| m.with(DIAG_ROUTES.iter().copied(), Arc::new(diag)))
    {
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
    emit(HostdEvent::Ready {
        pid: std::process::id(),
    });
    let code = tokio::select! {
        r = server.serve(listener) => match r {
            Ok(()) => {
                emit(HostdEvent::Shutdown { reason: Code::st("serve_ended") });
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("wasit-hostd: serve failed: {e}");
                emit(HostdEvent::Shutdown { reason: Code::st("serve_failed") });
                ExitCode::from(1)
            }
        },
        what = termination() => {
            emit(HostdEvent::Shutdown { reason: Code::new(what) });
            server.shutdown(Duration::from_secs(2)).await;
            ExitCode::SUCCESS
        }
    };
    // Waits (bounded) for the writer to write what was recorded; the process is ending.
    let _written = wasit_diag::shutdown();
    code
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
            eprintln!("wasit-hostd: cannot install signal handlers");
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
    // The daemon's stderr: crash output and developer (`WASIT_LOG`) text, never diagnostics.
    let log = wasit_diag::private::open(&data.join("hostd.log"), |o| {
        o.append(true);
    })?;
    if log.metadata()?.len() > (1 << 20) {
        log.set_len(0)?;
    }
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
    // `tracing` is counted into the diagnostics sink; its text goes nowhere unless a developer
    // sets `WASIT_LOG` (then to stderr, without colour).
    let text = std::env::var_os(DEV_ENV).map(|_| {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .with_filter(
                tracing_subscriber::EnvFilter::try_from_env(DEV_ENV)
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
    });
    let _ = tracing_subscriber::registry()
        .with(wasit_diag::tracing_layer())
        .with(text)
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
