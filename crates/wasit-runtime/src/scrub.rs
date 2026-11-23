//! What this host registers with the process's scrubber (`wasit_diag::scrub`, design §4.3) so
//! that no free text recorded here ever holds it: the host's secret variable values, the secrets
//! a session is launched with, and the directories sessions work in.
//!
//! Each function is called where the values first reach this process and before anything
//! made from them can be recorded (see the call sites): registration is a few hash inserts and
//! one snapshot swap, a cold path, and it never fails.

use wasit_core::{AgentConfig, McpTransport, SpawnSpec};

use crate::wire::HostEnvConfig;

/// A session's env or MCP value is registered only from this many characters: shorter ones
/// (`1`, `on`, `/`) are not secrets and replacing them would only damage texts.
const MIN_SESSION_SECRET_CHARS: usize = 6;

/// The secret variables of a host environment configuration.
pub(crate) fn host_secrets(config: &HostEnvConfig) {
    wasit_diag::scrub::register_secrets(config.secrets());
}

/// Every environment value of an agent configuration and every value of an MCP server's
/// environment (stdio) or headers (http): tokens travel in them.
pub(crate) fn agent_config(config: &AgentConfig) {
    let mcp = config.mcp_servers.iter().flat_map(|s| match &s.transport {
        McpTransport::Stdio { env, .. } => env.values(),
        McpTransport::Http { headers, .. } => headers.values(),
    });
    let values = config
        .env
        .iter()
        .map(|(_, v)| v)
        .chain(mcp)
        .map(String::as_str)
        .filter(|v| v.chars().count() >= MIN_SESSION_SECRET_CHARS);
    wasit_diag::scrub::register_secrets(values);
}

/// The environment a session's process was launched with, as `session.json` keeps it (what a
/// restarted daemon knows of the values `agent_config` saw).
pub(crate) fn spawn_env(spec: &SpawnSpec) {
    let values = spec
        .env
        .iter()
        .map(|(_, v)| v.as_str())
        .filter(|v| v.chars().count() >= MIN_SESSION_SECRET_CHARS);
    wasit_diag::scrub::register_secrets(values);
}

/// A session's working directory: a project path, shown in free text as its tag.
pub(crate) fn session_cwd(cwd: Option<&str>) {
    if let Some(cwd) = cwd {
        wasit_diag::scrub::register_project_path(cwd);
    }
}
