//! Demo daemon for the control protocol, also used by the crate's tests.
//!
//! ```text
//! wasit-control-demo serve-stdio [--host ID] [--build B] [--major N] [--exit-after-events N]
//! wasit-control-demo serve-unix PATH [--host ID] [--build B]   # prints "ready" when listening
//! wasit-control-demo bridge PATH [--host ID] [--build B]       # stdio <-> PATH, starting serve-unix if absent
//! ```

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use tokio::sync::Notify;
use wasit_control::demo::DemoService;
use wasit_control::{
    CallContext, ConnConfig, ControlServer, Cursor, EventStream, Hello, RpcError, Service,
    StdioListener,
};

struct Args {
    mode: String,
    path: Option<String>,
    host: String,
    build: String,
    major: Option<u16>,
    exit_after_events: Option<u64>,
}

fn parse() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mode = it.next().ok_or("missing mode")?;
    let mut a = Args {
        mode,
        path: None,
        host: "demo".into(),
        build: "demo-build".into(),
        major: None,
        exit_after_events: None,
    };
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--host" => a.host = val()?,
            "--build" => a.build = val()?,
            "--major" => a.major = Some(val()?.parse().map_err(|e| format!("{e}"))?),
            "--exit-after-events" => {
                a.exit_after_events = Some(val()?.parse().map_err(|e| format!("{e}"))?)
            }
            _ if a.path.is_none() && !arg.starts_with("--") => a.path = Some(arg),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(a)
}

/// Demo service plus `shutdown` and an optional crash after N `count` events.
struct BinService {
    demo: DemoService,
    shutdown: Arc<Notify>,
    exit_after_events: Option<u64>,
}

#[async_trait]
impl Service for BinService {
    async fn call(&self, ctx: CallContext, method: &str, params: Value) -> Result<Value, RpcError> {
        if method == "shutdown" {
            self.shutdown.notify_one();
            return Ok(json!("bye"));
        }
        self.demo.call(ctx, method, params).await
    }

    async fn subscribe(
        &self,
        ctx: CallContext,
        topic: &str,
        params: Value,
        from: Option<Cursor>,
    ) -> Result<EventStream, RpcError> {
        let s = self.demo.subscribe(ctx, topic, params, from).await?;
        let Some(limit) = self.exit_after_events else {
            return Ok(s);
        };
        let mut sent = 0u64;
        Ok(s.map(move |item| {
            if sent >= limit {
                // Simulated crash mid-stream.
                std::process::exit(3);
            }
            sent += 1;
            item
        })
        .boxed())
    }
}

fn hello(a: &Args) -> Hello {
    let mut h = Hello::new(a.build.clone(), a.host.clone(), "daemon").with_features(["demo"]);
    if let Some(m) = a.major {
        h.protocol.major = m;
    }
    h
}

fn server(a: &Args, shutdown: Arc<Notify>) -> ControlServer {
    let svc = BinService {
        demo: DemoService::new(a.host.clone()),
        shutdown,
        exit_after_events: a.exit_after_events,
    };
    ControlServer::new(Arc::new(svc), hello(a), ConnConfig::default())
}

#[tokio::main]
async fn main() -> ExitCode {
    let a = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("wasit-control-demo: {e}");
            return ExitCode::from(2);
        }
    };
    match a.mode.as_str() {
        "serve-stdio" => {
            let shutdown = Arc::new(Notify::new());
            let srv = server(&a, shutdown.clone());
            tokio::select! {
                r = srv.serve(StdioListener::process()) => {
                    if let Err(e) = r {
                        eprintln!("serve failed: {e}");
                        return ExitCode::from(1);
                    }
                }
                _ = shutdown.notified() => srv.shutdown(Duration::from_secs(1)).await,
            }
            ExitCode::SUCCESS
        }
        #[cfg(unix)]
        "serve-unix" => {
            use tokio::io::AsyncWriteExt;
            let Some(path) = a.path.clone() else {
                eprintln!("serve-unix needs a PATH");
                return ExitCode::from(2);
            };
            let listener = match wasit_control::UnixSocketListener::bind_exclusive(&path).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("cannot listen: {e}");
                    return ExitCode::from(3);
                }
            };
            let mut out = tokio::io::stdout();
            // Readiness signal for whoever started us; they may already be gone.
            let _ = out.write_all(b"ready\n").await;
            let _ = out.flush().await;
            drop(out);
            let shutdown = Arc::new(Notify::new());
            let srv = server(&a, shutdown.clone());
            tokio::select! {
                r = srv.serve(listener) => {
                    if let Err(e) = r {
                        eprintln!("serve failed: {e}");
                        return ExitCode::from(1);
                    }
                }
                _ = shutdown.notified() => srv.shutdown(Duration::from_secs(1)).await,
            }
            ExitCode::SUCCESS
        }
        #[cfg(unix)]
        "bridge" => {
            let Some(path) = a.path.clone() else {
                eprintln!("bridge needs a PATH");
                return ExitCode::from(2);
            };
            let endpoint = wasit_control::Endpoint::Unix(path.clone().into());
            let start = || start_daemon(path.clone(), a.host.clone(), a.build.clone());
            match wasit_control::bridge(
                tokio::io::stdin(),
                tokio::io::stdout(),
                &endpoint,
                start,
                wasit_control::BridgeOptions::default(),
            )
            .await
            {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("bridge: {e}");
                    ExitCode::from(1)
                }
            }
        }
        other => {
            eprintln!("wasit-control-demo: unknown mode {other}");
            ExitCode::from(2)
        }
    }
}

/// Start `serve-unix PATH` detached and wait (bounded by the bridge) for its "ready" line.
#[cfg(unix)]
async fn start_daemon(path: String, host: String, build: String) -> std::io::Result<()> {
    use std::process::Stdio;
    use tokio::io::AsyncBufReadExt;
    let exe = std::env::current_exe()?;
    let mut child = tokio::process::Command::new(exe)
        .args(["serve-unix", &path, "--host", &host, "--build", &build])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .process_group(0)
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("no stdout"))?;
    let mut line = String::new();
    tokio::io::BufReader::new(stdout)
        .read_line(&mut line)
        .await?;
    // EOF without "ready" usually means another instance won the lock; the bridge's connect
    // retries decide.
    if line.trim() == "ready" || line.is_empty() {
        // Detached daemon: we do not wait for it; tokio reaps it if it exits while we run.
        drop(child);
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "daemon did not report ready (got {line:?})"
        )))
    }
}
