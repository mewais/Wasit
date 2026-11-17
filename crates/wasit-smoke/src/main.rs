//! wasit-smoke — manual live-verification tool for Wasit drivers.
//!
//! NOT a unit test or CI job. This binary requires a REAL CLI on PATH (or
//! specified via `--bin`). Run it by hand to confirm that a driver actually
//! works end-to-end: spawns the process, drives the handshake, streams events,
//! and answers gates.
//!
//! # Usage
//!
//!   wasit-smoke --driver <claude|codex|opencode> \
//!               --prompt <text> \
//!               [--cwd <path>] \
//!               [--model <id>] \
//!               [--mode <id>] \
//!               [--bin <path>] \
//!               [--auto-approve]
//!
//! # Example invocations
//!
//! ## Claude (needs `claude` on PATH or ANTHROPIC_API_KEY set)
//!
//!   cargo run -p wasit-smoke -- \
//!     --driver claude \
//!     --prompt "Say hello in one sentence." \
//!     --model claude-haiku-4
//!
//! ## Codex (needs `codex` on PATH)
//!
//!   cargo run -p wasit-smoke -- \
//!     --driver codex \
//!     --prompt "Echo 'wasit codex smoke ok' and stop." \
//!     --auto-approve
//!
//! ## Opencode (needs `opencode` on PATH)
//!
//!   cargo run -p wasit-smoke -- \
//!     --driver opencode \
//!     --prompt "Say 'wasit opencode smoke ok' and nothing else." \
//!     --model claude-haiku-4
//!
//! # Gate behaviour
//!
//! Without `--auto-approve`, each gate pauses and prints:
//!   [GATE] <JSON>
//!   Approve? [y/N]:
//! Type `y` + Enter to approve (one-time, no permission update), anything else to deny.
//!
//! With `--auto-approve`, every gate is approved immediately with no input.
//!
//! # Overall timeout
//!
//! The run is wrapped in a 300-second wall-clock timeout. If the CLI hangs,
//! the binary prints a TIMEOUT line and exits non-zero.

use std::collections::HashMap;
use std::process;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::time::{timeout, Duration};
use wasit_core::{AgentDriver, AgentEvent, GateDecision, GateRequest, QuestionAnswer, SpawnSpec};

// ============================================================================
// Arg parsing (manual — no clap dependency)
// ============================================================================

struct Args {
    driver: String,
    cwd: Option<String>,
    model: Option<String>,
    mode: String,
    bin: Option<String>,
    prompt: String,
    auto_approve: bool,
}

fn usage() -> ! {
    eprintln!(
        "\
wasit-smoke — manual live-smoke for Wasit drivers

USAGE:
  wasit-smoke --driver <claude|codex|opencode> --prompt <text> [OPTIONS]

OPTIONS:
  --driver <claude|codex|opencode>   (required) which CLI backend to use
  --prompt <text>                    (required) first prompt to send
  --cwd <path>                       working directory (default: current dir)
  --model <id>                       model override (e.g. claude-haiku-4)
  --mode <id>                        permission/collaboration mode id
  --bin <path>                       override CLI binary path
  --auto-approve                     auto-approve every gate (no stdin prompt)

EXAMPLES:
  # Claude
  cargo run -p wasit-smoke -- --driver claude --prompt 'Hello in one sentence.' --model claude-haiku-4

  # Codex
  cargo run -p wasit-smoke -- --driver codex --prompt \"Echo 'smoke ok'\" --auto-approve

  # Opencode
  cargo run -p wasit-smoke -- --driver opencode --prompt 'Say smoke ok.' --model claude-haiku-4
"
    );
    process::exit(1);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut driver = String::new();
    let mut cwd: Option<String> = None;
    let mut model: Option<String> = None;
    let mut mode = String::new();
    let mut bin: Option<String> = None;
    let mut prompt = String::new();
    let mut auto_approve = false;

    let mut i = 0usize;
    while i < raw.len() {
        match raw[i].as_str() {
            "--driver" => {
                i += 1;
                driver = raw.get(i).cloned().unwrap_or_else(|| usage());
            }
            "--cwd" => {
                i += 1;
                cwd = Some(raw.get(i).cloned().unwrap_or_else(|| usage()));
            }
            "--model" => {
                i += 1;
                model = Some(raw.get(i).cloned().unwrap_or_else(|| usage()));
            }
            "--mode" => {
                i += 1;
                mode = raw.get(i).cloned().unwrap_or_else(|| usage());
            }
            "--bin" => {
                i += 1;
                bin = Some(raw.get(i).cloned().unwrap_or_else(|| usage()));
            }
            "--prompt" => {
                i += 1;
                prompt = raw.get(i).cloned().unwrap_or_else(|| usage());
            }
            "--auto-approve" => {
                auto_approve = true;
            }
            flag => {
                eprintln!("unknown flag: {flag}");
                usage();
            }
        }
        i += 1;
    }

    if driver.is_empty() {
        eprintln!("error: --driver is required");
        usage();
    }
    if prompt.is_empty() {
        eprintln!("error: --prompt is required");
        usage();
    }
    if !matches!(driver.as_str(), "claude" | "codex" | "opencode") {
        eprintln!("error: --driver must be one of: claude, codex, opencode");
        usage();
    }

    Args {
        driver,
        cwd,
        model,
        mode,
        bin,
        prompt,
        auto_approve,
    }
}

// ============================================================================
// Gate answering
// ============================================================================

/// Print the gate and return the decision.
/// If `auto_approve` is true, always approve with no updates.
/// Otherwise, read y/N from stdin.
async fn decide_gate(gate: &GateRequest, auto_approve: bool) -> GateDecision {
    let gate_json = serde_json::to_string(gate).unwrap_or_else(|_| "<serialize error>".into());
    println!("[GATE] {gate_json}");

    if auto_approve {
        println!("[AUTO-APPROVE] approving gate");
        return GateDecision::Approve {
            updated_input: None,
            permission_updates: None,
            elicitation_content: None,
        };
    }

    // Prompt human on stdin.
    print!("Approve? [y/N]: ");
    // Flush stdout (no newline above).
    use std::io::Write;
    let _ = std::io::stdout().flush();

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    let _ = reader.read_line(&mut line).await;
    let trimmed = line.trim().to_lowercase();

    if trimmed == "y" || trimmed == "yes" {
        GateDecision::Approve {
            updated_input: None,
            permission_updates: None,
            elicitation_content: None,
        }
    } else {
        GateDecision::Deny {
            reason: Some("user denied".to_string()),
        }
    }
}

// ============================================================================
// Event loop
// ============================================================================

async fn run_inner(args: Args) -> i32 {
    // Build SpawnSpec.
    let spec = SpawnSpec {
        cwd: args.cwd.or_else(|| {
            std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(|s| s.to_string()))
        }),
        model: args.model,
        mode: args.mode,
        bin: args.bin,
        first_prompt: Some(args.prompt),
        ..SpawnSpec::default()
    };

    // Pick and spawn the driver.
    let session_result: Result<Box<dyn wasit_core::Session>, wasit_core::DriverError> =
        match args.driver.as_str() {
            "claude" => {
                let driver = wasit_claude::ClaudeDriver::from_path();
                driver.spawn(spec).await
            }
            "codex" => {
                let driver = wasit_codex::CodexDriver::from_path();
                driver.spawn(spec).await
            }
            "opencode" => {
                let driver = wasit_opencode::OpencodeDriver::from_path();
                driver.spawn(spec).await
            }
            other => {
                eprintln!("internal error: unknown driver {other}");
                return 2;
            }
        };

    let mut session = match session_result {
        Ok(s) => s,
        Err(e) => {
            eprintln!("SPAWN FAILED: {e}");
            return 2;
        }
    };

    println!("» [smoke] session spawned with driver={}", args.driver);

    // Event loop.
    loop {
        let maybe_event = session.next_event().await;

        match maybe_event {
            None => {
                println!("» [smoke] stream ended (None)");
                return 0;
            }
            Some(event) => {
                // Pretty-print every event as one-line JSON with » prefix.
                let event_json =
                    serde_json::to_string(&event).unwrap_or_else(|_| "<serialize error>".into());
                println!("» {event_json}");

                match &event {
                    AgentEvent::GateRequested { gate } => {
                        let request_id = match gate {
                            GateRequest::Permission { request_id, .. } => request_id.clone(),
                            GateRequest::Plan { request_id, .. } => request_id.clone(),
                            GateRequest::Question { request_id, .. } => request_id.clone(),
                        };

                        match gate {
                            GateRequest::Question { .. } => {
                                // Auto-answer questions with empty (skip).
                                println!("[GATE/Question] auto-skipping with empty QuestionAnswer");
                                let answer = QuestionAnswer {
                                    answers: HashMap::new(),
                                };
                                if let Err(e) = session.answer_question(&request_id, answer).await {
                                    eprintln!("answer_question error: {e}");
                                }
                            }
                            _ => {
                                let decision = decide_gate(gate, args.auto_approve).await;
                                if let Err(e) = session.answer_gate(&request_id, decision).await {
                                    eprintln!("answer_gate error: {e}");
                                }
                            }
                        }
                    }

                    AgentEvent::TurnCompleted { .. } => {
                        println!("» [smoke] TurnCompleted — exiting 0");
                        return 0;
                    }

                    AgentEvent::ProcessExited { code } => {
                        let c = code.unwrap_or(-1);
                        println!("» [smoke] ProcessExited code={c}");
                        return if c == 0 { 0 } else { 1 };
                    }

                    _ => {
                        // All other events already printed above; nothing extra to do.
                    }
                }
            }
        }
    }
}

// ============================================================================
// Entry point
// ============================================================================

#[tokio::main]
async fn main() {
    let args = parse_args();
    let driver_name = args.driver.clone();

    let overall = timeout(Duration::from_secs(300), run_inner(args));
    match overall.await {
        Ok(exit_code) => {
            process::exit(exit_code);
        }
        Err(_elapsed) => {
            eprintln!(
                "TIMEOUT: wasit-smoke [{driver_name}] did not complete within 300 seconds. \
                 The CLI may be hung. Check that the CLI binary is on PATH and that your \
                 API key is configured."
            );
            process::exit(124);
        }
    }
}
