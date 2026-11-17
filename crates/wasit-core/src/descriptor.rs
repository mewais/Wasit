//! [`DriverDescriptor`]: everything the runtime needs to know about one CLI,
//! and [`DriverRegistry`]: the ordered list of descriptors.

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::ConfigRenderer;
use crate::discovery::{DiscoveryContext, DiscoveryProbe};
use crate::protocol::Protocol;
use crate::spec::SpawnSpec;

/// A driver id (`"claude"`, `"codex"`, `"opencode"`, `"kilo"`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DriverId(pub String);

impl DriverId {
    pub fn new(id: impl Into<String>) -> Self {
        DriverId(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DriverId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for DriverId {
    fn from(s: &str) -> Self {
        DriverId(s.to_string())
    }
}

/// What happens to a `Command::Prompt` encoded while a turn is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusyPrompt {
    /// The prompt is written immediately and the CLI itself queues it for
    /// after the running turn (claude stream-json).
    QueuedByCli,
    /// The prompt is injected into the running turn (codex `turn/steer`).
    Steered,
    /// The protocol holds the prompt and sends it when the running turn
    /// completes (ACP allows one `session/prompt` at a time).
    QueuedByProtocol,
}

/// Honest capability report. A `false` is honest; a rounded-up `true` is a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverCapabilities {
    /// `Command::Interrupt` stops the running turn.
    pub interrupt: bool,
    /// `Command::SetModel` works without restarting.
    pub set_model: bool,
    /// `Command::SetMode` works without restarting.
    pub set_mode: bool,
    /// `Command::RunSlash` runs CLI slash commands / skills.
    pub slash_commands: bool,
    /// The CLI raises tool-permission gates (`GateRequest::Permission`).
    pub permission_gates: bool,
    /// The CLI raises question gates answered with `Command::AnswerQuestion`.
    pub questions: bool,
    /// The CLI raises plan-approval gates (`GateRequest::Plan`).
    pub plan_gate: bool,
    /// The CLI reports token usage.
    pub usage: bool,
    /// The CLI reports cost in USD.
    pub cost: bool,
    /// `ResumeMode::Resume(id)` works.
    pub resume: bool,
    /// `ResumeMode::Continue` (most recent session in cwd) works.
    pub continue_latest: bool,
    /// Assistant text arrives as incremental deltas (else whole blocks).
    pub streaming_output: bool,
    /// Semantics of a prompt sent during a running turn.
    pub prompt_while_busy: BusyPrompt,
}

/// Everything the runtime needs to know about one CLI. Implemented by each
/// driver crate; nothing outside a driver crate and the registry that lists
/// drivers ever matches on a driver id.
pub trait DriverDescriptor: ConfigRenderer + Send + Sync {
    /// Stable id.
    fn id(&self) -> DriverId;
    /// Human name, e.g. `"Claude Code"`.
    fn display_name(&self) -> &str;
    /// Binary looked up on `PATH` when the spec does not override it.
    fn default_binary(&self) -> &str;
    /// Full argv (element 0 is the binary) for `spec`.
    fn argv(&self, spec: &SpawnSpec) -> Vec<String>;
    /// Environment variables to set for `spec` (driver variables first, then
    /// `spec.env`; later entries win).
    fn env(&self, spec: &SpawnSpec) -> Vec<(String, String)>;
    /// Honest capabilities.
    fn capabilities(&self) -> DriverCapabilities;
    /// A fresh protocol state machine for one session.
    fn new_protocol(&self) -> Box<dyn Protocol>;
    /// Discovery probes for this host.
    fn discovery(&self, ctx: &DiscoveryContext) -> Vec<DiscoveryProbe>;
}

/// Errors from the registry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("unknown driver id: {0}")]
    UnknownDriver(DriverId),
    #[error("duplicate driver id: {0}")]
    DuplicateDriver(DriverId),
}

/// An ordered list of descriptors. Lookup of an unknown id is an error, never
/// a fallback.
#[derive(Clone, Default)]
pub struct DriverRegistry {
    drivers: Vec<Arc<dyn DriverDescriptor>>,
}

impl DriverRegistry {
    pub fn new() -> Self {
        DriverRegistry::default()
    }

    /// Append a descriptor. Fails on a duplicate id.
    pub fn register(&mut self, d: Arc<dyn DriverDescriptor>) -> Result<(), RegistryError> {
        let id = d.id();
        if self.drivers.iter().any(|x| x.id() == id) {
            return Err(RegistryError::DuplicateDriver(id));
        }
        self.drivers.push(d);
        Ok(())
    }

    /// Look up a descriptor by id.
    pub fn get(&self, id: &DriverId) -> Result<Arc<dyn DriverDescriptor>, RegistryError> {
        self.drivers
            .iter()
            .find(|d| &d.id() == id)
            .cloned()
            .ok_or_else(|| RegistryError::UnknownDriver(id.clone()))
    }

    /// All descriptors in registration order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn DriverDescriptor>> {
        self.drivers.iter()
    }

    /// All ids in registration order.
    pub fn ids(&self) -> Vec<DriverId> {
        self.drivers.iter().map(|d| d.id()).collect()
    }
}

impl std::fmt::Debug for DriverRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.ids()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AgentConfig, Unsupported};
    use crate::discovery::DiscoveryProbe;
    use crate::protocol::{Command, DriverError, Encoded, Step};

    struct Fake(&'static str);
    struct Nop;
    impl Protocol for Nop {
        fn start(&mut self, _: &SpawnSpec) -> Step {
            Step::empty()
        }
        fn on_line(&mut self, _: &str) -> Step {
            Step::empty()
        }
        fn encode(&mut self, _: Command) -> Result<Encoded, DriverError> {
            Ok(Encoded::Queued)
        }
        fn is_ready(&self) -> bool {
            false
        }
    }
    impl ConfigRenderer for Fake {
        fn render_config(&self, _: &AgentConfig, _: &mut SpawnSpec) -> Vec<Unsupported> {
            vec![]
        }
    }
    impl DriverDescriptor for Fake {
        fn id(&self) -> DriverId {
            DriverId::new(self.0)
        }
        fn display_name(&self) -> &str {
            self.0
        }
        fn default_binary(&self) -> &str {
            self.0
        }
        fn argv(&self, spec: &SpawnSpec) -> Vec<String> {
            vec![spec.binary_or(self.0).to_string()]
        }
        fn env(&self, spec: &SpawnSpec) -> Vec<(String, String)> {
            spec.env.clone()
        }
        fn capabilities(&self) -> DriverCapabilities {
            DriverCapabilities {
                interrupt: false,
                set_model: false,
                set_mode: false,
                slash_commands: false,
                permission_gates: false,
                questions: false,
                plan_gate: false,
                usage: false,
                cost: false,
                resume: false,
                continue_latest: false,
                streaming_output: false,
                prompt_while_busy: BusyPrompt::QueuedByProtocol,
            }
        }
        fn new_protocol(&self) -> Box<dyn Protocol> {
            Box::new(Nop)
        }
        fn discovery(&self, _: &DiscoveryContext) -> Vec<DiscoveryProbe> {
            vec![]
        }
    }

    #[test]
    fn registry_order_lookup_and_errors() {
        let mut r = DriverRegistry::new();
        r.register(Arc::new(Fake("a"))).unwrap();
        r.register(Arc::new(Fake("b"))).unwrap();
        assert_eq!(r.ids(), vec![DriverId::new("a"), DriverId::new("b")]);
        assert_eq!(r.get(&"b".into()).unwrap().display_name(), "b");
        assert_eq!(
            r.get(&"zzz".into()).err(),
            Some(RegistryError::UnknownDriver("zzz".into()))
        );
        assert_eq!(
            r.register(Arc::new(Fake("a"))),
            Err(RegistryError::DuplicateDriver("a".into()))
        );
    }
}
