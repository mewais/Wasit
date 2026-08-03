//! wasit-opencode — opencode ACP (Agent Communication Protocol) JSON-RPC driver for Wasit.
//!
//! Implements `AgentDriver` + `Session` for the `opencode` CLI using the ACP
//! JSON-RPC-over-stdio protocol.
//!
//! ## Wire protocol overview
//!
//! Opencode communicates over stdin/stdout as NDJSON JSON-RPC 2.0 lines.
//! Handshake: `initialize` → `session/new` (or `session/load` for resume).
//! The init response carries `configOptions` with mode and model catalogs.
//! Per-turn: `session/prompt` request → `session/update` notifications
//! (agent_message_chunk, tool_call, tool_call_update, plan, usage_update,
//! current_mode_update, available_commands_update, context_compaction, …)
//! → `session/prompt` result (stopReason + usage).
//! Interrupt: `session/interrupt` request (or `session/stop`).
//! Live config change: `session/set_config_option` (model, mode).
//! Permission gate: `session/request_permission` JSON-RPC request (must be
//! answered with `session/permission_response` before the turn can proceed).
//!
//! ## Capability report (honest)
//! - `interruptible`: true — `session/interrupt` request.
//! - `model_switchable`: true — `session/set_config_option` with `id="model"`.
//! - `mode_switchable`: true — `session/set_config_option` with `id="mode"`.
//! - `persistent_conversation`: true — session context persists across turns.
//! - `resumable`: true — `session/load` with a known sessionId.
//! - `streaming_output`: true — `agent_message_chunk` updates.
//! - `reports_usage`: true — `usage_update` and `session/prompt` result usage.
//! - `supports_gates`: true — `session/request_permission` method.
//!
//! ## Gaps vs. claude richness
//! - No per-message `usage` (usage comes as periodic `usage_update` + turn-end
//!   usage in the result; no dedup by message id).
//! - No `AskUserQuestion` gate type (opencode's equivalent is the raw
//!   `session/request_permission` which carries `options`; mapped to a
//!   `GateRequest::Permission` with elicitation_choices).
//! - No `ExitPlanMode`/plan gate — opencode's plan mode surfaces via
//!   `PlanUpdated` events but does not block on an explicit gate.
//! - No `compact_boundary` metadata (only a bare compaction notice).
//! - No `rate_limit_event`.
//! - No `per_model` breakdown in `TurnCompleted` (no `modelUsage` in ACP).
//! - Mode list only available at init and after `set_config_option` — not pushed
//!   spontaneously.
//! - `num_turns` in `TurnCompleted` is always 1 (ACP does not report it).
//!
//! All bodies are `todo!()`.

#![forbid(unsafe_code)]
#![allow(dead_code, unused_variables)]

use async_trait::async_trait;
use wasit_core::{
    AgentDriver, AgentEvent, DriverCapabilities, DriverError, GateDecision, QuestionAnswer,
    Session, SpawnSpec,
};

// ============================================================================
// Public driver struct
// ============================================================================

/// The opencode ACP JSON-RPC driver.
pub struct OpencodeDriver {
    bin: Option<std::path::PathBuf>,
}

impl OpencodeDriver {
    pub fn new(bin: impl Into<std::path::PathBuf>) -> Self {
        OpencodeDriver {
            bin: Some(bin.into()),
        }
    }

    pub fn from_path() -> Self {
        OpencodeDriver { bin: None }
    }
}

#[async_trait]
impl AgentDriver for OpencodeDriver {
    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            interruptible: true,
            model_switchable: true,
            mode_switchable: true,
            persistent_conversation: true,
            resumable: true,
            streaming_output: true,
            reports_usage: true,
            supports_gates: true,
        }
    }

    async fn spawn(&self, spec: SpawnSpec) -> Result<Box<dyn Session>, DriverError> {
        todo!("OpencodeDriver::spawn — Phase 2 logic")
    }
}

// ============================================================================
// Session impl (stub)
// ============================================================================

pub struct OpencodeSession {
    _phantom: std::marker::PhantomData<()>,
}

#[async_trait]
impl Session for OpencodeSession {
    async fn next_event(&mut self) -> Option<AgentEvent> {
        todo!("OpencodeSession::next_event")
    }

    async fn send_prompt(&mut self, text: &str) -> Result<(), DriverError> {
        todo!("OpencodeSession::send_prompt — send session/prompt JSON-RPC")
    }

    async fn answer_gate(
        &mut self,
        request_id: &str,
        decision: GateDecision,
    ) -> Result<(), DriverError> {
        todo!("OpencodeSession::answer_gate — send session/permission_response JSON-RPC")
    }

    async fn answer_question(
        &mut self,
        request_id: &str,
        answer: QuestionAnswer,
    ) -> Result<(), DriverError> {
        // opencode does not emit a distinct Question gate type — permission gates
        // with options are mapped to GateRequest::Permission with elicitation_choices.
        // If a question gate were to arrive, it would already be routed through
        // answer_gate. Return Unsupported for now.
        Err(DriverError::Unsupported {
            operation: "answer_question (opencode uses answer_gate with elicitation_choices for multi-option gates)".to_string(),
        })
    }

    async fn interrupt(&mut self) -> Result<(), DriverError> {
        todo!("OpencodeSession::interrupt — send session/interrupt JSON-RPC")
    }

    async fn set_model(&mut self, model: &str) -> Result<(), DriverError> {
        todo!("OpencodeSession::set_model — send session/set_config_option id=model")
    }

    async fn set_mode(&mut self, mode_id: &str) -> Result<(), DriverError> {
        todo!("OpencodeSession::set_mode — send session/set_config_option id=mode")
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        todo!("OpencodeSession::shutdown")
    }
}
