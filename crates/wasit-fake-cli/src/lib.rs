//! `wasit-fake-cli`: a scriptable fake of the real coding-agent CLIs (claude, codex, opencode,
//! kilo) that speaks their real wire protocols over stdin/stdout, driven by a JSON scenario so
//! tests are deterministic and need no network or model.
//!
//! * The binary is `wasit-fake-cli`; see the crate README for the scenario format.
//! * [`scenario`] has the serde types and a builder for writing scenarios from tests.
//! * [`fake_cli_path`] locates the built binary from another crate's tests.
//!
//! # Using the binary from another crate
//!
//! `env!("CARGO_BIN_EXE_wasit-fake-cli")` only exists for this crate's own integration tests.
//! Dependents build the binary first (`cargo build -p wasit-fake-cli`, which writes it to
//! `target/<profile>/wasit-fake-cli`) and then call [`fake_cli_path`], which finds it next to the
//! running test executable (`target/<profile>/deps/<test>` → `target/<profile>/wasit-fake-cli`),
//! or at `$WASIT_FAKE_CLI` when set.

pub mod acp;
pub mod claude;
pub mod cli;
pub mod codex;
pub mod engine;
pub mod io;
pub mod reference;
pub mod scenario;
pub mod util;

use std::path::PathBuf;

/// Environment variable that overrides [`fake_cli_path`].
pub const ENV_BIN: &str = "WASIT_FAKE_CLI";

/// Locate the `wasit-fake-cli` binary: `$WASIT_FAKE_CLI`, else the executable named
/// `wasit-fake-cli` in the cargo target directory of the currently running binary.
/// Returns `None` if it has not been built (run `cargo build -p wasit-fake-cli`).
pub fn fake_cli_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os(ENV_BIN) {
        let p = PathBuf::from(p);
        return p.is_file().then_some(p);
    }
    let name = format!("wasit-fake-cli{}", std::env::consts::EXE_SUFFIX);
    let exe = std::env::current_exe().ok()?;
    // target/<profile>/deps/<test-exe>  or  target/<profile>/<exe>
    exe.ancestors()
        .skip(1)
        .take(3)
        .map(|dir| dir.join(&name))
        .find(|p| p.is_file())
}
