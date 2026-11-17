//! `wasit-shim` binary.
//!
//! * `wasit-shim --dir <session_dir> [--cwd D] [--env K=V]... -- <program> [args...]` runs the
//!   shim in the foreground (normally started detached by `spawn_detached`).
//! * `wasit-shim launch <same arguments>` starts a detached shim of this binary, prints its
//!   readiness as one JSON line and exits.

use std::ffi::OsString;

fn launch(args: Vec<OsString>) -> i32 {
    let exe = match std::env::current_exe() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("wasit-shim launch: cannot locate own executable: {e}");
            return 1;
        }
    };
    let spec = match wasit_shim::ShimSpec::from_cli_args(exe, args) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("wasit-shim launch: {e}");
            return 2;
        }
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("wasit-shim launch: {e}");
            return 1;
        }
    };
    match rt.block_on(wasit_shim::spawn_detached(&spec)) {
        Ok(h) => {
            println!(
                "{}",
                serde_json::json!({
                    "ready": true,
                    "pid": h.shim_pid,
                    "child_pid": h.child_pid,
                    "seq": h.spawned_seq,
                })
            );
            0
        }
        Err(e) => {
            eprintln!("wasit-shim launch: {e}");
            1
        }
    }
}

fn main() {
    let mut args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let code = if args.first().is_some_and(|a| a == "launch") {
        args.remove(0);
        launch(args)
    } else {
        wasit_shim::run(args)
    };
    std::process::exit(code);
}
