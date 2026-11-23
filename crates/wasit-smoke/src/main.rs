//! wasit-smoke — a tiny manual harness: run a real agent CLI through its
//! Wasit `Protocol`, print the events, answer gates from stdin.
//!
//! ```text
//! wasit-smoke <claude|codex|opencode|kilo> [options] [prompt...]
//!   --model M  --mode X  --cwd DIR  --bin PATH  --env K=V (repeatable)
//!   --resume ID | --continue      --config agent-config.json
//!   --record FILE    write a replayable transcript (wasit_core::transcript)
//!   --script FILE    drive the session from a JSON step list (see below)
//!   --timeout SECS   per-wait deadline in script mode (default 120)
//! ```
//!
//! Interactive commands (one per line on stdin): `y`/`n`/`a` approve, deny or
//! always-allow the latest gate; `q ANSWER` answer the latest question gate;
//! `i` interrupt; `m MODEL`; `mode ID`; `/name args` slash command; anything
//! else is a prompt; `.quit` or EOF shuts the session down.
//!
//! Script steps: `{"cmd": <Command JSON>}` (`"task_id": "@running"` names the latest task the
//! agent started), `{"wait": "<event type>",
//! "contains": "<substring>"?}`, `{"approve": true}` / `{"deny": true}` /
//! `{"always": true}` (latest gate), `{"answer": "<label>"}` (latest question
//! gate).

use std::time::Duration;

use wasit_core::{AgentConfig, ResumeMode, SpawnSpec};
use wasit_smoke::{run, RunOptions};

fn usage() -> String {
    "usage: wasit-smoke <claude|codex|opencode|kilo> [--model M] [--mode X] [--cwd D] [--bin B] \
     [--env K=V] [--resume ID|--continue] [--config FILE] [--record FILE] [--script FILE] \
     [--timeout SECS] [prompt...]"
        .into()
}

fn parse_args() -> Result<RunOptions, String> {
    let mut it = std::env::args().skip(1);
    let driver = it.next().ok_or_else(usage)?;
    let mut spec = SpawnSpec::default();
    let mut opts = RunOptions {
        driver,
        spec: SpawnSpec::default(),
        config: None,
        record: None,
        script: None,
        wait: Duration::from_secs(120),
        echo: true,
    };
    let mut prompt = Vec::new();
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--model" => spec.model = Some(val()?),
            "--mode" => spec.mode = val()?,
            "--cwd" => spec.cwd = Some(val()?),
            "--bin" => spec.bin = Some(val()?),
            "--resume" => spec.resume = ResumeMode::Resume(val()?),
            "--continue" => spec.resume = ResumeMode::Continue,
            "--env" => {
                let kv = val()?;
                let (k, v) = kv.split_once('=').ok_or("--env takes K=V")?;
                spec.env.push((k.into(), v.into()));
            }
            "--config" => {
                let path = val()?;
                let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
                let cfg: AgentConfig =
                    serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
                opts.config = Some(cfg);
            }
            "--record" => opts.record = Some(val()?),
            "--script" => {
                let path = val()?;
                let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
                opts.script =
                    Some(serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))?);
            }
            "--timeout" => {
                opts.wait =
                    Duration::from_secs(val()?.parse().map_err(|e| format!("--timeout: {e}"))?)
            }
            "--help" | "-h" => return Err(usage()),
            _ => prompt.push(arg),
        }
    }
    if !prompt.is_empty() {
        spec.first_prompt = Some(prompt.join(" "));
    }
    if spec.cwd.is_none() {
        spec.cwd = std::env::current_dir()
            .ok()
            .map(|p| p.to_string_lossy().into_owned());
    }
    opts.spec = spec;
    Ok(opts)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = match parse_args() {
        Ok(opts) => run(opts).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(outcome) => {
            println!("exit: {:?}", outcome.exit_code);
            if outcome.script_error.is_some() {
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("smoke: {e}");
            std::process::exit(1);
        }
    }
}
