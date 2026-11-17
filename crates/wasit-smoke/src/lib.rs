//! wasit-smoke library: run a real agent CLI through its Wasit protocol.
//! Used by the `wasit-smoke` binary and by the drivers' live (`#[ignore]`)
//! tests. This is the only place in the driver layer that does process I/O.

use std::io::Write as _;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command as Proc};
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};
use wasit_core::transcript::{to_line, Record};
use wasit_core::{
    AgentConfig, AgentEvent, Command, DriverDescriptor, DriverId, DriverRegistry, Encoded,
    GateDecision, GateRequest, Protocol, QuestionAnswer, SpawnSpec, Step,
};

/// Every driver this workspace ships, in a stable order.
pub fn registry() -> DriverRegistry {
    let mut r = DriverRegistry::new();
    for d in [
        wasit_claude::descriptor(),
        wasit_codex::descriptor(),
        wasit_opencode::opencode(),
        wasit_opencode::kilo(),
    ] {
        r.register(d).expect("driver ids are unique");
    }
    r
}

/// What to run.
pub struct RunOptions {
    pub driver: String,
    pub spec: SpawnSpec,
    pub config: Option<AgentConfig>,
    /// Write the transcript here as it happens.
    pub record: Option<String>,
    /// Script steps (see the binary's docs); `None` = interactive stdin.
    pub script: Option<Vec<Value>>,
    /// Deadline for each `wait` step.
    pub wait: Duration,
    /// Echo events to stdout.
    pub echo: bool,
}

/// What happened.
#[derive(Debug, Default)]
pub struct Outcome {
    pub events: Vec<AgentEvent>,
    pub transcript: Vec<Record>,
    pub exit_code: Option<i32>,
    /// Set when the script failed (timeout, CLI exit, missing gate).
    pub script_error: Option<String>,
}

/// One live session: the child, its protocol and the recorder.
pub(crate) struct Live {
    protocol: Box<dyn Protocol>,
    stdin: Option<ChildStdin>,
    record: Option<std::fs::File>,
    last_gate: Option<GateRequest>,
    echo: bool,
    outcome: Outcome,
}

impl Live {
    fn rec(&mut self, r: &Record) {
        self.outcome.transcript.push(r.clone());
        if let Some(f) = self.record.as_mut() {
            if let Err(e) = writeln!(f, "{}", to_line(r)) {
                eprintln!("smoke: cannot write transcript: {e}");
            }
        }
    }

    async fn apply(&mut self, step: Step) -> Vec<AgentEvent> {
        for ev in &step.events {
            self.outcome.events.push(ev.clone());
            if self.echo {
                println!("event: {}", serde_json::to_string(ev).unwrap_or_default());
            }
            match ev {
                AgentEvent::GateRequested { gate } => self.last_gate = Some(gate.clone()),
                AgentEvent::GateResolved { request_id }
                    if self.last_gate.as_ref().map(|g| g.request_id()) == Some(request_id) =>
                {
                    self.last_gate = None
                }
                _ => {}
            }
        }
        for line in &step.write {
            self.rec(&Record::In(line.0.clone()));
            if let Some(stdin) = self.stdin.as_mut() {
                let res = async {
                    stdin.write_all(line.0.as_bytes()).await?;
                    stdin.write_all(b"\n").await?;
                    stdin.flush().await
                };
                if let Err(e) = res.await {
                    eprintln!("smoke: write to CLI failed: {e}");
                }
            }
        }
        if step.close_stdin {
            self.rec(&Record::CloseStdin);
            // Dropping the handle closes the pipe: the CLI sees EOF.
            self.stdin = None;
        }
        step.events
    }

    async fn command(&mut self, cmd: Command) -> Vec<AgentEvent> {
        self.rec(&Record::Cmd(cmd.clone()));
        let name = cmd.name();
        match self.protocol.encode(cmd) {
            Ok(Encoded::Sent(step)) => self.apply(step).await,
            Ok(Encoded::Queued) => {
                println!("note: {name} queued until the protocol is ready");
                vec![]
            }
            Err(e) => {
                println!("error: {name}: {e}");
                vec![]
            }
        }
    }

    async fn line(&mut self, line: &str) -> Vec<AgentEvent> {
        self.rec(&Record::Out(line.to_string()));
        let step = self.protocol.on_line(line);
        self.apply(step).await
    }

    fn gate_answer(&self, decision: GateDecision) -> Option<Command> {
        let gate = self.last_gate.as_ref()?;
        Some(Command::AnswerGate {
            request_id: gate.request_id().to_string(),
            decision,
        })
    }

    fn question_answer(&self, answer: &str) -> Option<Command> {
        let gate = self.last_gate.as_ref()?;
        let mut a = QuestionAnswer::default();
        match gate {
            GateRequest::Question { questions, .. } => {
                for q in questions {
                    a.answers.insert(q.question.clone(), answer.to_string());
                }
            }
            _ => {
                a.answers.insert(String::new(), answer.to_string());
            }
        }
        Some(Command::AnswerQuestion {
            request_id: gate.request_id().to_string(),
            answer: a,
        })
    }
}

/// Run one session to completion (script) or until the user quits
/// (interactive), then shut it down with deadlines.
pub async fn run(mut opts: RunOptions) -> Result<Outcome, String> {
    let desc: Arc<dyn DriverDescriptor> = registry()
        .get(&DriverId::new(opts.driver.clone()))
        .map_err(|e| e.to_string())?;
    if let Some(cfg) = &opts.config {
        if opts.spec.session_dir.is_none() {
            opts.spec.session_dir = Some(std::env::temp_dir().to_string_lossy().into_owned());
        }
        for u in desc.render_config(cfg, &mut opts.spec) {
            println!("unsupported: {} — {}", u.item, u.reason);
        }
        write_generated_files(&opts.spec)?;
    }
    let mut child = spawn(desc.as_ref(), &opts.spec)?;
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let stderr = child.stderr.take().ok_or("no stderr")?;
    let (tx, mut rx) = mpsc::channel::<String>(1024);
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            if tx.send(l).await.is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            eprintln!("cli-stderr: {l}");
        }
    });

    let mut live = Live {
        protocol: desc.new_protocol(),
        stdin: child.stdin.take(),
        record: match &opts.record {
            Some(p) => Some(std::fs::File::create(p).map_err(|e| format!("{p}: {e}"))?),
            None => None,
        },
        last_gate: None,
        echo: opts.echo,
        outcome: Outcome::default(),
    };
    live.rec(&Record::Start(opts.spec.clone()));
    let step = live.protocol.start(&opts.spec);
    live.apply(step).await;

    match &opts.script {
        Some(steps) => {
            if let Err(e) = run_script(&mut live, &mut rx, steps, opts.wait).await {
                eprintln!("smoke: script failed: {e}");
                live.outcome.script_error = Some(e);
            }
        }
        None => run_interactive(&mut live, &mut rx).await,
    }

    // Shut down and drain what the CLI still prints, with a deadline.
    if live.stdin.is_some() {
        live.command(Command::Shutdown).await;
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match timeout(
            deadline.saturating_duration_since(Instant::now()),
            rx.recv(),
        )
        .await
        {
            Ok(Some(l)) => {
                live.line(&l).await;
            }
            Ok(None) => break,
            Err(_) => {
                eprintln!("smoke: CLI did not exit in time; killing it");
                if let Err(e) = child.kill().await {
                    eprintln!("smoke: kill failed: {e}");
                }
                break;
            }
        }
    }
    match timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(status)) => live.outcome.exit_code = status.code(),
        Ok(Err(e)) => eprintln!("smoke: wait failed: {e}"),
        Err(_) => eprintln!("smoke: CLI still running after kill"),
    }
    Ok(live.outcome)
}

fn spawn(desc: &dyn DriverDescriptor, spec: &SpawnSpec) -> Result<Child, String> {
    let argv = desc.argv(spec);
    eprintln!("smoke: spawning {argv:?}");
    let mut cmd = Proc::new(&argv[0]);
    cmd.args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = &spec.cwd {
        cmd.current_dir(cwd);
    }
    for (k, v) in desc.env(spec) {
        cmd.env(k, v);
    }
    cmd.spawn().map_err(|e| format!("spawn {}: {e}", argv[0]))
}

fn write_generated_files(spec: &SpawnSpec) -> Result<(), String> {
    if spec.files.is_empty() {
        return Ok(());
    }
    let dir = spec
        .session_dir
        .as_deref()
        .ok_or("config generated files but no session directory is set")?;
    for f in &spec.files {
        let path = std::path::Path::new(dir).join(&f.path);
        std::fs::write(&path, &f.contents).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    Ok(())
}

pub(crate) async fn run_interactive(live: &mut Live, rx: &mut mpsc::Receiver<String>) {
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    loop {
        tokio::select! {
            line = rx.recv() => match line {
                Some(l) => { live.line(&l).await; }
                None => { println!("note: CLI closed its stdout"); return; }
            },
            user = input.next_line() => {
                let text = match user {
                    Ok(Some(t)) => t,
                    Ok(None) => return,
                    Err(e) => { eprintln!("smoke: stdin: {e}"); return; }
                };
                let text = text.trim();
                let cmd = match text {
                    "" => continue,
                    ".quit" => return,
                    "y" => live.gate_answer(GateDecision::approve()),
                    "n" => live.gate_answer(GateDecision::deny(Some("denied in wasit-smoke"))),
                    "a" => live.gate_answer(GateDecision::Approve { updated_input: None, permission_updates: Some(Value::Bool(true)), elicitation_content: None }),
                    "i" => Some(Command::Interrupt),
                    t if t.starts_with("q ") => live.question_answer(&t[2..]),
                    t if t.starts_with("m ") => Some(Command::SetModel { model: t[2..].trim().into() }),
                    t if t.starts_with("mode ") => Some(Command::SetMode { mode: t[5..].trim().into() }),
                    t if t.starts_with('/') => {
                        let (name, rest) = t[1..].split_once(' ').unwrap_or((&t[1..], ""));
                        Some(Command::RunSlash { name: name.into(), args: rest.into() })
                    }
                    t => Some(Command::Prompt { text: t.into() }),
                };
                match cmd {
                    Some(c) => { live.command(c).await; }
                    None => println!("note: no pending gate"),
                }
            }
        }
    }
}

async fn run_script(
    live: &mut Live,
    rx: &mut mpsc::Receiver<String>,
    steps: &[Value],
    wait: Duration,
) -> Result<(), String> {
    for st in steps {
        let cmd = if let Some(c) = st.get("cmd") {
            Some(
                serde_json::from_value::<Command>(c.clone())
                    .map_err(|e| format!("bad cmd {c}: {e}"))?,
            )
        } else if st.get("approve").is_some() {
            live.gate_answer(GateDecision::approve())
        } else if st.get("deny").is_some() {
            live.gate_answer(GateDecision::deny(Some("denied in wasit-smoke")))
        } else if st.get("always").is_some() {
            live.gate_answer(GateDecision::Approve {
                updated_input: None,
                permission_updates: Some(Value::Bool(true)),
                elicitation_content: None,
            })
        } else if let Some(a) = st.get("answer").and_then(Value::as_str) {
            live.question_answer(a)
        } else if let Some(kind) = st.get("wait").and_then(Value::as_str) {
            let contains = st.get("contains").and_then(Value::as_str).unwrap_or("");
            wait_for(live, rx, kind, contains, wait).await?;
            continue;
        } else {
            return Err(format!("unknown script step {st}"));
        };
        match cmd {
            Some(c) => {
                live.command(c).await;
            }
            None => return Err(format!("step {st}: no pending gate")),
        }
    }
    Ok(())
}

async fn wait_for(
    live: &mut Live,
    rx: &mut mpsc::Receiver<String>,
    kind: &str,
    contains: &str,
    wait: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + wait;
    loop {
        let line = match timeout(
            deadline.saturating_duration_since(Instant::now()),
            rx.recv(),
        )
        .await
        {
            Ok(Some(l)) => l,
            Ok(None) => return Err(format!("CLI exited while waiting for {kind}")),
            Err(_) => return Err(format!("timed out waiting for {kind}")),
        };
        for ev in live.line(&line).await {
            let json = serde_json::to_string(&ev).unwrap_or_default();
            let ty = serde_json::to_value(&ev)
                .ok()
                .and_then(|v| v.get("type").and_then(Value::as_str).map(str::to_string));
            if ty.as_deref() == Some(kind) && json.contains(contains) {
                return Ok(());
            }
        }
    }
}
