//! `wasit-kilo-bridge <kilo> acp [args…]`: the CLI command for Kilo sessions. Runs `kilo acp`
//! and adds its loopback HTTP channel as `_wasit/kilo` notifications on stdout. See
//! `wasit_opencode::subagents` for the design.

use std::process::ExitCode;

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match wasit_opencode::subagents::run_bridge(&argv) {
        Ok(code) => ExitCode::from(u8::try_from(code.clamp(0, 255)).unwrap_or(1)),
        Err(e) => {
            eprintln!("wasit-kilo-bridge: {e}");
            ExitCode::from(2)
        }
    }
}
