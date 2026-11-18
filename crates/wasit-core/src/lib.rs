//! wasit-core — the contract every Wasit driver implements and every consumer
//! depends on. No I/O happens in this crate.
//!
//! - [`AgentEvent`] and its value types: the one event model all CLIs map to.
//! - [`Protocol`]: a pure, deterministic per-session state machine. The
//!   runtime feeds it CLI stdout lines and user [`Command`]s; it returns
//!   events and stdin lines ([`Step`]).
//! - [`DriverDescriptor`]: id, binary, argv/env, honest
//!   [`DriverCapabilities`], a protocol factory, discovery probes
//!   ([`DiscoveryProbe`]) and a [`ConfigRenderer`].
//! - [`DriverRegistry`]: ordered descriptors, lookup by [`DriverId`]; an
//!   unknown id is an error.
//! - [`wire`]: tiny JSON-RPC / JSON helpers shared by the driver crates.
//! - [`transcript`]: record/replay format for protocol fixture tests.
//!
//! Driver crates: `wasit-claude` (stream-json control protocol),
//! `wasit-codex` (app-server JSON-RPC) and `wasit-opencode` (ACP; OpenCode and
//! Kilo Code flavors).

#![forbid(unsafe_code)]

mod config;
mod descriptor;
mod discovery;
mod event;
mod protocol;
mod spec;
pub mod transcript;
pub mod wire;

pub use config::{
    AgentConfig, ConfigItem, ConfigItemKind, ConfigItemRef, ConfigRenderer, ConfigSource,
    DisabledItem, McpServerConfig, McpTransport, PluginRef, SkillRef, ToolPolicy, Unsupported,
};
pub use descriptor::{
    BusyPrompt, DriverCapabilities, DriverDescriptor, DriverId, DriverRegistry, RegistryError,
};
pub use discovery::{
    config_listing, exec_stdout, front_matter, handshake_events, listed_files, markdown_items,
    models_from_handshake, modes_from_handshake, parse_version_line, read_file,
    skills_from_listing, Discovered, DiscoveryContext, DiscoveryKind, DiscoveryProbe, FileHead,
    ProbeAction, ProbeOutput, ProbeParse, ResumableSession,
};
pub use event::{
    AgentEvent, CompactionInfo, ElicitationChoice, EndReason, GateDecision, GateRequest,
    McpServerInfo, ModelInfo, ModelUsage, PlanEntry, QuestionAnswer, QuestionOption,
    QuestionPrompt, RateLimitInfo, SessionMode, SlashCommandInfo, Usage,
};
pub use protocol::{Command, DriverError, Encoded, OutLine, Protocol, Step};
pub use spec::{GeneratedFile, ResumeMode, SpawnSpec};
